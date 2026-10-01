// Package server runs one conclave node for real: TCP to the other
// servers, a write-ahead log on disk, a wall clock, and an HTTP API for
// clients.
//
// All consensus and state-machine work happens in internal/node, driven by
// a single goroutine (run). Everything that reaches the node, a tick, a
// peer message or a client request, arrives on a channel, and the loop
// drains whatever else is waiting before it calls Ready, so that one fsync
// covers a whole batch of requests (group commit).
package server

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"math/rand/v2"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/useless-husband/conclave/internal/node"
	"github.com/useless-husband/conclave/internal/raft"
	"github.com/useless-husband/conclave/internal/transport"
	"github.com/useless-husband/conclave/internal/vfs"
	"github.com/useless-husband/conclave/internal/wal"
)

// Config configures a server.
type Config struct {
	ID      raft.NodeID
	DataDir string
	// RaftAddr and APIAddr are the listen addresses. An empty address or
	// port 0 reuses the addresses of the previous run, recorded in the
	// data directory, or else picks a free port. The membership stores
	// the addresses, so a restarted server must come back on them.
	RaftAddr string
	APIAddr  string
	// Bootstrap creates a new single-server cluster if the data
	// directory is empty. Other servers start empty and are added.
	Bootstrap      bool
	Tick           time.Duration
	ElectionTicks  int
	HeartbeatTicks int
	SnapshotEvery  uint64
	Fsync          vfs.SyncMode
	// RequestTimeout bounds how long the API waits for an answer.
	RequestTimeout time.Duration
	Logger         *log.Logger
}

func (c *Config) defaults() {
	if c.Tick == 0 {
		c.Tick = 10 * time.Millisecond
	}
	if c.ElectionTicks == 0 {
		c.ElectionTicks = 10
	}
	if c.HeartbeatTicks == 0 {
		c.HeartbeatTicks = 2
	}
	if c.RequestTimeout == 0 {
		c.RequestTimeout = 5 * time.Second
	}
	if c.Logger == nil {
		c.Logger = log.New(io.Discard, "", 0)
	}
}

// Addresses is what a server records about itself in its data directory.
type Addresses struct {
	ID   uint64 `json:"id"`
	Raft string `json:"raft"`
	API  string `json:"api"`
	PID  int    `json:"pid"`
}

// EncodeMeta stores a member's addresses in the membership.
func EncodeMeta(raftAddr, apiAddr string) []byte {
	return []byte("raft=" + raftAddr + " api=" + apiAddr)
}

// ParseMeta reverses EncodeMeta.
func ParseMeta(meta []byte) (raftAddr, apiAddr string) {
	for _, f := range strings.Fields(string(meta)) {
		if v, ok := strings.CutPrefix(f, "raft="); ok {
			raftAddr = v
		} else if v, ok := strings.CutPrefix(f, "api="); ok {
			apiAddr = v
		}
	}
	return raftAddr, apiAddr
}

type call struct {
	req  node.Request
	resp chan node.Response
}

type pendingCall struct {
	ch    chan node.Response
	since time.Time
}

// Server is a running node.
type Server struct {
	cfg    Config
	log    *log.Logger
	tr     *transport.Transport
	apiLn  net.Listener
	httpd  *http.Server
	w      *wal.WAL
	nd     *node.Node
	reqs   chan call
	stop   chan struct{}
	done   chan struct{}
	nextID atomic.Uint64
	status atomic.Pointer[Status]
	closed sync.Once
}

// Member describes one member in Status.
type Member struct {
	ID   uint64 `json:"id"`
	Raft string `json:"raft"`
	API  string `json:"api"`
}

// Status is a server's view of the cluster, served at /v1/status.
type Status struct {
	ID        uint64   `json:"id"`
	Role      string   `json:"role"`
	Term      uint64   `json:"term"`
	Leader    uint64   `json:"leader"`
	LeaderAPI string   `json:"leader_api,omitempty"`
	Commit    uint64   `json:"commit"`
	Applied   uint64   `json:"applied"`
	LastIndex uint64   `json:"last_index"`
	SnapIndex uint64   `json:"snapshot_index"`
	Keys      int      `json:"keys"`
	Sessions  int      `json:"sessions"`
	Members   []Member `json:"members"`
	RaftAddr  string   `json:"raft_addr"`
	APIAddr   string   `json:"api_addr"`
}

func addrFile(dir string) string { return filepath.Join(dir, "addresses.json") }

// ReadAddresses reads the addresses a server recorded in its data
// directory.
func ReadAddresses(dir string) (Addresses, error) {
	var a Addresses
	b, err := os.ReadFile(addrFile(dir))
	if err != nil {
		return a, err
	}
	return a, json.Unmarshal(b, &a)
}

func portZero(addr string) bool {
	if addr == "" {
		return true
	}
	_, port, err := net.SplitHostPort(addr)
	return err == nil && port == "0"
}

// Start opens the data directory, binds the listeners and starts serving.
func Start(cfg Config) (*Server, error) {
	cfg.defaults()
	if cfg.ID == raft.None {
		return nil, errors.New("server: -id must be a positive number")
	}
	if err := os.MkdirAll(cfg.DataDir, 0o755); err != nil {
		return nil, err
	}
	prev, err := ReadAddresses(cfg.DataDir)
	switch {
	case err == nil && prev.ID != uint64(cfg.ID):
		return nil, fmt.Errorf("server: data directory %s belongs to server %d, not %d", cfg.DataDir, prev.ID, cfg.ID)
	case err == nil:
		if portZero(cfg.RaftAddr) {
			cfg.RaftAddr = prev.Raft
		}
		if portZero(cfg.APIAddr) {
			cfg.APIAddr = prev.API
		}
	case !errors.Is(err, os.ErrNotExist):
		return nil, fmt.Errorf("server: %w", err)
	}
	if cfg.RaftAddr == "" {
		cfg.RaftAddr = "127.0.0.1:0"
	}
	if cfg.APIAddr == "" {
		cfg.APIAddr = "127.0.0.1:0"
	}
	s := &Server{cfg: cfg, log: cfg.Logger, reqs: make(chan call, 1024), stop: make(chan struct{}), done: make(chan struct{})}
	s.tr, err = transport.Listen(cfg.ID, cfg.RaftAddr, cfg.Logger)
	if err != nil {
		return nil, fmt.Errorf("server: raft listener: %w", err)
	}
	s.apiLn, err = net.Listen("tcp", cfg.APIAddr)
	if err != nil {
		s.tr.Close()
		return nil, fmt.Errorf("server: api listener: %w", err)
	}
	if err := s.open(); err != nil {
		s.tr.Close()
		s.apiLn.Close()
		return nil, err
	}
	addrs, _ := json.Marshal(Addresses{ID: uint64(cfg.ID), Raft: s.tr.Addr(), API: s.apiLn.Addr().String(), PID: os.Getpid()})
	if err := writeFileAtomic(addrFile(cfg.DataDir), addrs); err != nil {
		s.Close()
		return nil, err
	}
	s.publish()
	go s.run()
	s.httpd = &http.Server{Handler: s.routes(), ReadHeaderTimeout: 10 * time.Second}
	go s.httpd.Serve(s.apiLn)
	s.log.Printf("n%d serving: raft %s, api http://%s, data %s", cfg.ID, s.tr.Addr(), s.apiLn.Addr(), cfg.DataDir)
	return s, nil
}

func writeFileAtomic(path string, b []byte) error {
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, b, 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}

// open recovers the write-ahead log and creates the node.
func (s *Server) open() error {
	fs, err := vfs.NewOS(filepath.Join(s.cfg.DataDir, "wal"), s.cfg.Fsync)
	if err != nil {
		return err
	}
	s.w, err = wal.Open(fs, wal.Options{})
	if err != nil {
		return fmt.Errorf("server: recover: %w", err)
	}
	if st := s.w.Stats(); st.TornTail {
		s.log.Printf("recovery cut a torn write of %d bytes off the log", st.TornBytes)
	}
	if s.w.Empty() && s.cfg.Bootstrap {
		conf := raft.NewMembership(raft.Member{ID: s.cfg.ID, Meta: EncodeMeta(s.tr.Addr(), s.apiLn.Addr().String())})
		if err := s.w.Bootstrap(conf); err != nil {
			return err
		}
		s.log.Printf("bootstrapped a new cluster with n%d as its only member", s.cfg.ID)
	}
	s.nd, err = node.New(node.Config{
		ID:            s.cfg.ID,
		ElectionTick:  s.cfg.ElectionTicks,
		HeartbeatTick: s.cfg.HeartbeatTicks,
		SnapshotEvery: s.cfg.SnapshotEvery,
		Trace:         func(format string, args ...any) { s.log.Printf(format, args...) },
	}, s.w, s.tr, jitter{rand.New(rand.NewPCG(rand.Uint64(), uint64(s.cfg.ID)))})
	if err != nil {
		return err
	}
	s.updateAddrs()
	return nil
}

// jitter supplies election timeout randomness. Only the run goroutine uses
// it.
type jitter struct{ r *rand.Rand }

func (j jitter) Intn(n int) int { return j.r.IntN(n) }

func (s *Server) updateAddrs() {
	addrs := map[raft.NodeID]string{}
	for _, m := range s.nd.Raft().Membership().Members {
		if r, _ := ParseMeta(m.Meta); r != "" {
			addrs[m.ID] = r
		}
	}
	s.tr.SetAddrs(addrs)
}

// RaftAddr and APIAddr return the bound addresses.
func (s *Server) RaftAddr() string { return s.tr.Addr() }
func (s *Server) APIAddr() string  { return s.apiLn.Addr().String() }

// Status returns the latest published status.
func (s *Server) Status() Status { return *s.status.Load() }

// Close stops the server. Pending requests fail; everything acknowledged
// is already durable.
func (s *Server) Close() error {
	var err error
	s.closed.Do(func() {
		if s.httpd != nil {
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			s.httpd.Shutdown(ctx)
			cancel()
		} else {
			s.apiLn.Close()
		}
		close(s.stop)
		if s.nd != nil {
			<-s.done
		}
		s.tr.Close()
		if s.w != nil {
			err = s.w.Close()
		}
	})
	return err
}

// run is the only goroutine that touches the node.
func (s *Server) run() {
	defer close(s.done)
	ticker := time.NewTicker(s.cfg.Tick)
	defer ticker.Stop()
	sweep := time.NewTicker(10 * time.Second)
	defer sweep.Stop()
	pending := map[uint64]pendingCall{}
	confIndex := s.nd.Raft().Status().ConfIndex
	handle := func(c call) {
		pending[c.req.ID] = pendingCall{ch: c.resp, since: time.Now()}
		s.nd.Submit(c.req)
	}
	for {
		select {
		case <-s.stop:
			for id, p := range pending {
				p.ch <- node.Response{ID: id, Status: node.StatusUnknown}
			}
			return
		case <-ticker.C:
			s.nd.Tick()
		case m := <-s.tr.Inbox():
			s.nd.Step(m)
		case c := <-s.reqs:
			handle(c)
		case now := <-sweep.C:
			for id, p := range pending {
				if now.Sub(p.since) > time.Minute {
					delete(pending, id)
				}
			}
		}
		// Take whatever else is already waiting, so that one fsync
		// covers the whole batch.
	drain:
		for i := 0; i < 1024; i++ {
			select {
			case m := <-s.tr.Inbox():
				s.nd.Step(m)
			case c := <-s.reqs:
				handle(c)
			default:
				break drain
			}
		}
		s.nd.Ready(func(r node.Response) {
			if p, ok := pending[r.ID]; ok {
				p.ch <- r
				delete(pending, r.ID)
			}
		})
		if ci := s.nd.Raft().Status().ConfIndex; ci != confIndex {
			confIndex = ci
			s.updateAddrs()
		}
		s.publish()
	}
}

func (s *Server) publish() {
	r := s.nd.Raft()
	st := r.Status()
	out := &Status{
		ID: uint64(st.ID), Role: st.Role.String(), Term: st.Term, Leader: uint64(st.Leader),
		Commit: st.Commit, Applied: s.nd.Applied(), LastIndex: st.LastIndex, SnapIndex: st.SnapIndex,
		Keys: s.nd.Store().Len(), Sessions: s.nd.Store().Sessions(),
		RaftAddr: s.tr.Addr(), APIAddr: s.apiLn.Addr().String(),
	}
	for _, m := range st.Conf.Members {
		ra, aa := ParseMeta(m.Meta)
		out.Members = append(out.Members, Member{ID: uint64(m.ID), Raft: ra, API: aa})
		if m.ID == st.Leader {
			out.LeaderAPI = aa
		}
	}
	s.status.Store(out)
}

var errStopped = errors.New("server is shutting down")

// do submits a request to the node and waits for its answer.
func (s *Server) do(ctx context.Context, req node.Request) (node.Response, error) {
	req.ID = s.nextID.Add(1)
	ch := make(chan node.Response, 1)
	select {
	case s.reqs <- call{req: req, resp: ch}:
	case <-ctx.Done():
		return node.Response{}, ctx.Err()
	case <-s.stop:
		return node.Response{}, errStopped
	}
	select {
	case r := <-ch:
		return r, nil
	case <-ctx.Done():
		return node.Response{}, ctx.Err()
	}
}

// memberAPI returns the API address of a member.
func (s *Server) memberAPI(id raft.NodeID) string {
	for _, m := range s.Status().Members {
		if m.ID == uint64(id) {
			return m.API
		}
	}
	return ""
}
