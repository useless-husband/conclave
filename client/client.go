// Package client is the Go client for a conclave cluster.
//
// A Client finds the leader by itself, follows leader hints and retries
// requests on other servers. Writes carry a client session and a sequence
// number, so a write retried after a timeout or a leader change takes
// effect at most once. When the client cannot tell whether a write took
// effect (the context expired after an attempt that may have reached a
// log, or the session expired), the method returns an error that matches
// ErrUnknown with errors.Is.
//
// A Client serializes its writes, because a session has one write in
// flight at a time; use several Clients for concurrent writes. Reads are
// linearizable (ReadIndex on the leader) and may run concurrently.
package client

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
	"net/http"
	"net/url"
	"strconv"
	"sync"
	"time"
)

var (
	// ErrUnknown means a write may or may not have taken effect.
	ErrUnknown = errors.New("conclave: outcome unknown")
	// ErrSessionExpired means the cluster evicted the client's session
	// while a write was being retried. The write's outcome is unknown; the
	// error also matches ErrUnknown. The next write opens a new session.
	ErrSessionExpired = fmt.Errorf("%w: session expired", ErrUnknown)
	// ErrNotFound is returned by Get for a key that does not exist.
	ErrNotFound = errors.New("conclave: key not found")
)

// Options tunes a Client.
type Options struct {
	// AttemptTimeout bounds one HTTP attempt. Zero means 2 s.
	AttemptTimeout time.Duration
	// HTTPClient is used for all requests. Zero means a client with
	// keep-alive connections.
	HTTPClient *http.Client
}

// Client talks to a cluster. Its methods are safe for concurrent use.
type Client struct {
	addrs []string
	opt   Options
	hc    *http.Client

	mu     sync.Mutex // guards leader
	leader string

	wmu     sync.Mutex // one write at a time
	session uint64
	seq     uint64
}

// New returns a client for the servers whose API addresses (host:port) are
// given. One reachable address is enough: the others are learned from
// leader hints.
func New(addrs []string, opt Options) *Client {
	if opt.AttemptTimeout == 0 {
		opt.AttemptTimeout = 2 * time.Second
	}
	hc := opt.HTTPClient
	if hc == nil {
		hc = &http.Client{Transport: &http.Transport{MaxIdleConnsPerHost: 64, IdleConnTimeout: 30 * time.Second}}
	}
	return &Client{addrs: append([]string(nil), addrs...), opt: opt, hc: hc}
}

// Session returns the client's current session ID (0 before the first
// write).
func (c *Client) Session() uint64 {
	c.wmu.Lock()
	defer c.wmu.Unlock()
	return c.session
}

type errorBody struct {
	Code      string `json:"code"`
	Error     string `json:"error"`
	LeaderAPI string `json:"leader_api"`
}

type kvBody struct {
	Found   bool   `json:"found"`
	Value   string `json:"value"`
	Swapped *bool  `json:"swapped"`
	Index   uint64 `json:"index"`
	Session uint64 `json:"session"`
}

// outcome classifies one attempt.
type outcome int

const (
	done       outcome = iota // the request was executed; body is valid
	refused                   // definitely not executed; try elsewhere
	maybe                     // may or may not have been executed
	expired                   // session expired
	stale                     // session already applied a later write
	badRequest                // will never succeed
)

type attempt struct {
	out    outcome
	status int
	body   kvBody
	hint   string // leader API address named by a refusal
	err    error
}

func (c *Client) target() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.leader != "" {
		return c.leader
	}
	return c.addrs[rand.IntN(len(c.addrs))]
}

func (c *Client) setLeader(addr string) {
	c.mu.Lock()
	c.leader = addr
	c.mu.Unlock()
}

func (c *Client) once(ctx context.Context, method, path string, body []byte, hdr http.Header) attempt {
	addr := c.target()
	actx, cancel := context.WithTimeout(ctx, c.opt.AttemptTimeout)
	defer cancel()
	req, err := http.NewRequestWithContext(actx, method, "http://"+addr+path, bytes.NewReader(body))
	if err != nil {
		return attempt{out: badRequest, err: err}
	}
	for k, v := range hdr {
		req.Header[k] = v
	}
	resp, err := c.hc.Do(req)
	if err != nil {
		c.setLeader("")
		// The request may have reached the server before the failure.
		return attempt{out: maybe, err: err}
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(io.LimitReader(resp.Body, 4<<20))
	if err != nil {
		c.setLeader("")
		return attempt{out: maybe, err: err}
	}
	var eb errorBody
	if resp.StatusCode >= 400 && resp.StatusCode != http.StatusNotFound && resp.StatusCode != http.StatusConflict {
		json.Unmarshal(raw, &eb)
		c.setLeader(eb.LeaderAPI)
		err := fmt.Errorf("conclave: %s: %s", eb.Code, eb.Error)
		switch eb.Code {
		case "not-leader", "retry":
			return attempt{out: refused, status: resp.StatusCode, hint: eb.LeaderAPI, err: err}
		case "session-expired":
			return attempt{out: expired, status: resp.StatusCode, err: err}
		case "stale":
			return attempt{out: stale, status: resp.StatusCode, err: err}
		case "bad-request":
			return attempt{out: badRequest, status: resp.StatusCode, err: err}
		}
		return attempt{out: maybe, status: resp.StatusCode, err: err}
	}
	var kb kvBody
	if err := json.Unmarshal(raw, &kb); err != nil {
		return attempt{out: maybe, err: fmt.Errorf("conclave: bad response: %w", err)}
	}
	c.setLeader(addr)
	return attempt{out: done, status: resp.StatusCode, body: kb}
}

// call retries a request until it is executed, refused for good, or ctx
// ends. write tells whether attempts that may have executed matter.
func (c *Client) call(ctx context.Context, method, path string, body []byte, hdr http.Header, write bool) (attempt, error) {
	uncertain := false
	backoff := 5 * time.Millisecond
	follows := 0
	for {
		a := c.once(ctx, method, path, body, hdr)
		switch a.out {
		case done:
			return a, nil
		case badRequest:
			return a, a.err
		case expired:
			return a, ErrSessionExpired
		case stale:
			return a, fmt.Errorf("%w: %v", ErrUnknown, a.err)
		case maybe:
			uncertain = true
		}
		if ctx.Err() != nil {
			if write && uncertain {
				return a, fmt.Errorf("%w: %v", ErrUnknown, ctx.Err())
			}
			return a, ctx.Err()
		}
		if a.out == refused && a.status == http.StatusMisdirectedRequest && a.hint != "" && follows < 3 {
			follows++
			continue // follow the leader hint at once
		}
		follows = 0
		select {
		case <-time.After(backoff + time.Duration(rand.Int64N(int64(backoff)))):
		case <-ctx.Done():
			if write && uncertain {
				return a, fmt.Errorf("%w: %v", ErrUnknown, ctx.Err())
			}
			return a, ctx.Err()
		}
		backoff = min(2*backoff, 200*time.Millisecond)
	}
}

func keyPath(prefix, key string) string { return prefix + url.PathEscape(key) }

// Get returns the value of key. It returns ErrNotFound if the key does not
// exist.
func (c *Client) Get(ctx context.Context, key string) (string, error) {
	a, err := c.call(ctx, http.MethodGet, keyPath("/v1/kv/", key), nil, nil, false)
	if err != nil {
		return "", err
	}
	if !a.body.Found {
		return "", ErrNotFound
	}
	return a.body.Value, nil
}

// write runs one de-duplicated write.
func (c *Client) write(ctx context.Context, method, path string, body []byte) (attempt, error) {
	c.wmu.Lock()
	defer c.wmu.Unlock()
	if c.session == 0 {
		a, err := c.call(ctx, http.MethodPost, "/v1/sessions", nil, nil, false)
		if err != nil {
			return a, fmt.Errorf("conclave: open session: %w", err)
		}
		c.session, c.seq = a.body.Session, 0
	}
	c.seq++
	hdr := http.Header{}
	hdr.Set("Conclave-Session", strconv.FormatUint(c.session, 10))
	hdr.Set("Conclave-Seq", strconv.FormatUint(c.seq, 10))
	a, err := c.call(ctx, method, path, body, hdr, true)
	if errors.Is(err, ErrSessionExpired) {
		c.session = 0
	}
	return a, err
}

// Put sets key to value.
func (c *Client) Put(ctx context.Context, key, value string) error {
	_, err := c.write(ctx, http.MethodPut, keyPath("/v1/kv/", key), []byte(value))
	return err
}

// Delete removes key and reports the value it had, if any.
func (c *Client) Delete(ctx context.Context, key string) (old string, existed bool, err error) {
	a, err := c.write(ctx, http.MethodDelete, keyPath("/v1/kv/", key), nil)
	if err != nil {
		return "", false, err
	}
	return a.body.Value, a.body.Found, nil
}

// CAS sets key to value if its current value is expect, or, with
// expectAbsent, if it does not exist. If the swap fails it reports the
// current value.
func (c *Client) CAS(ctx context.Context, key, expect string, expectAbsent bool, value string) (swapped bool, cur string, found bool, err error) {
	body, _ := json.Marshal(map[string]any{"expect": expect, "expect_absent": expectAbsent, "value": value})
	a, err := c.write(ctx, http.MethodPost, keyPath("/v1/cas/", key), body)
	if err != nil {
		return false, "", false, err
	}
	return a.body.Swapped != nil && *a.body.Swapped, a.body.Value, a.body.Found, nil
}

// Status returns the status document of one server.
func (c *Client) Status(ctx context.Context, addr string) (map[string]any, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://"+addr+"/v1/status", nil)
	if err != nil {
		return nil, err
	}
	resp, err := c.hc.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	var out map[string]any
	return out, json.NewDecoder(resp.Body).Decode(&out)
}

// Admin sends an administrative request (membership or transfer) to the
// leader, following hints. method and path are as in the HTTP API.
func (c *Client) Admin(ctx context.Context, method, path string, body any) error {
	var b []byte
	if body != nil {
		b, _ = json.Marshal(body)
	}
	_, err := c.call(ctx, method, path, b, nil, true)
	return err
}

// Close releases idle connections.
func (c *Client) Close() { c.hc.CloseIdleConnections() }
