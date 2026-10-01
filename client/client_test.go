package client

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

// fake is a scripted server: each request is answered by the next handler
// in its list (the last one repeats).
type fake struct {
	mu    sync.Mutex
	steps []http.HandlerFunc
	seen  []*http.Request
	srv   *httptest.Server
}

func newFake(t *testing.T, steps ...http.HandlerFunc) *fake {
	f := &fake{steps: steps}
	f.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		f.mu.Lock()
		f.seen = append(f.seen, r.Clone(context.Background()))
		h := f.steps[0]
		if len(f.steps) > 1 {
			f.steps = f.steps[1:]
		}
		f.mu.Unlock()
		h(w, r)
	}))
	t.Cleanup(f.srv.Close)
	return f
}

func (f *fake) addr() string { return strings.TrimPrefix(f.srv.URL, "http://") }

func reply(status int, body any) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		json.NewEncoder(w).Encode(body)
	}
}

func session(id uint64) http.HandlerFunc { return reply(200, map[string]any{"session": id}) }

func TestFollowsLeaderHint(t *testing.T) {
	leader := newFake(t, reply(200, map[string]any{"found": true, "value": "v", "index": 3}))
	follower := newFake(t, reply(421, map[string]any{"code": "not-leader", "leader_api": leader.addr()}))
	c := New([]string{follower.addr()}, Options{})
	v, err := c.Get(context.Background(), "k")
	if err != nil || v != "v" {
		t.Fatalf("%q %v", v, err)
	}
}

func TestRetriedWriteKeepsSequenceNumber(t *testing.T) {
	srv := newFake(t,
		session(7),
		reply(503, map[string]any{"code": "unknown"}),
		reply(200, map[string]any{"index": 9}),
		reply(200, map[string]any{"index": 10}),
	)
	c := New([]string{srv.addr()}, Options{})
	ctx := context.Background()
	if err := c.Put(ctx, "k", "a"); err != nil {
		t.Fatal(err)
	}
	if err := c.Put(ctx, "k", "b"); err != nil {
		t.Fatal(err)
	}
	var seqs []string
	for _, r := range srv.seen[1:] {
		if r.Header.Get("Conclave-Session") != "7" {
			t.Fatalf("session header %q", r.Header.Get("Conclave-Session"))
		}
		seqs = append(seqs, r.Header.Get("Conclave-Seq"))
	}
	if strings.Join(seqs, ",") != "1,1,2" {
		t.Fatalf("sequence numbers %v, want the retry to reuse 1", seqs)
	}
}

func TestUnknownOutcome(t *testing.T) {
	srv := newFake(t, session(1), reply(503, map[string]any{"code": "unknown"}))
	c := New([]string{srv.addr()}, Options{})
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	if err := c.Put(ctx, "k", "v"); !errors.Is(err, ErrUnknown) {
		t.Fatalf("err %v, want ErrUnknown", err)
	}
}

func TestRefusedIsNotUnknown(t *testing.T) {
	srv := newFake(t, session(1), reply(503, map[string]any{"code": "retry"}))
	c := New([]string{srv.addr()}, Options{})
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	err := c.Put(ctx, "k", "v")
	if err == nil || errors.Is(err, ErrUnknown) {
		t.Fatalf("err %v: a write refused every time did not execute", err)
	}
}

func TestSessionExpiry(t *testing.T) {
	srv := newFake(t, session(1), reply(410, map[string]any{"code": "session-expired"}), session(2), reply(200, map[string]any{}))
	c := New([]string{srv.addr()}, Options{})
	ctx := context.Background()
	if err := c.Put(ctx, "k", "v"); !errors.Is(err, ErrSessionExpired) || !errors.Is(err, ErrUnknown) {
		t.Fatalf("err %v", err)
	}
	if err := c.Put(ctx, "k", "w"); err != nil {
		t.Fatal(err)
	}
	if c.Session() != 2 {
		t.Fatalf("session %d, want a new one", c.Session())
	}
}

func TestCASAndDeleteResults(t *testing.T) {
	srv := newFake(t, session(1),
		reply(409, map[string]any{"swapped": false, "found": true, "value": "cur"}),
		reply(404, map[string]any{"found": false}),
	)
	c := New([]string{srv.addr()}, Options{})
	ok, cur, found, err := c.CAS(context.Background(), "k", "x", false, "y")
	if err != nil || ok || !found || cur != "cur" {
		t.Fatal(ok, cur, found, err)
	}
	_, existed, err := c.Delete(context.Background(), "k")
	if err != nil || existed {
		t.Fatal(existed, err)
	}
}
