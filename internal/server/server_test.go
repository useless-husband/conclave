package server

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/useless-husband/conclave/client"
	"github.com/useless-husband/conclave/internal/raft"
	"github.com/useless-husband/conclave/internal/vfs"
)

func start(t *testing.T, id raft.NodeID, dir string, bootstrap bool) *Server {
	t.Helper()
	s, err := Start(Config{ID: id, DataDir: dir, Bootstrap: bootstrap, Fsync: vfs.SyncNone, Tick: 5 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

func TestThreeServers(t *testing.T) {
	dirs := []string{t.TempDir(), t.TempDir(), t.TempDir()}
	srv := []*Server{start(t, 1, dirs[0], true), start(t, 2, dirs[1], false), start(t, 3, dirs[2], false)}
	defer func() {
		for _, s := range srv {
			if s != nil {
				s.Close()
			}
		}
	}()
	waitFor(t, "n1 to lead", func() bool { return srv[0].Status().Role == "leader" })

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	admin := client.New([]string{srv[0].APIAddr()}, client.Options{})
	for _, s := range srv[1:] {
		st := s.Status()
		if err := admin.Admin(ctx, "POST", "/v1/members", MemberRequest{ID: st.ID, Raft: st.RaftAddr, API: st.APIAddr}); err != nil {
			t.Fatalf("add n%d: %v", st.ID, err)
		}
	}
	waitFor(t, "all three to see three members", func() bool {
		for _, s := range srv {
			if len(s.Status().Members) != 3 {
				return false
			}
		}
		return true
	})

	c := client.New([]string{srv[2].APIAddr()}, client.Options{})
	if err := c.Put(ctx, "greeting", "hello"); err != nil {
		t.Fatal(err)
	}
	if v, err := c.Get(ctx, "greeting"); err != nil || v != "hello" {
		t.Fatalf("get: %q %v", v, err)
	}
	if ok, cur, found, err := c.CAS(ctx, "greeting", "nope", false, "x"); err != nil || ok || !found || cur != "hello" {
		t.Fatalf("failed cas: %v %q %v %v", ok, cur, found, err)
	}
	if ok, _, _, err := c.CAS(ctx, "greeting", "hello", false, "bonjour"); err != nil || !ok {
		t.Fatalf("cas: %v %v", ok, err)
	}
	if old, existed, err := c.Delete(ctx, "greeting"); err != nil || !existed || old != "bonjour" {
		t.Fatalf("delete: %q %v %v", old, existed, err)
	}
	if _, err := c.Get(ctx, "greeting"); !errors.Is(err, client.ErrNotFound) {
		t.Fatalf("get after delete: %v", err)
	}
	odd := "a key/with spaces?and=%2F"
	if err := c.Put(ctx, odd, "v"); err != nil {
		t.Fatal(err)
	}
	if v, err := c.Get(ctx, odd); err != nil || v != "v" {
		t.Fatalf("odd key: %q %v", v, err)
	}

	// Stop the leader; the others elect a new one and keep serving.
	leader := 0
	for i, s := range srv {
		if s.Status().Role == "leader" {
			leader = i
		}
	}
	addr := srv[leader].APIAddr()
	srv[leader].Close()
	srv[leader] = nil
	for i := 0; i < 20; i++ {
		if err := c.Put(ctx, fmt.Sprintf("k%d", i), "v"); err != nil {
			t.Fatalf("put %d after the leader stopped: %v", i, err)
		}
	}
	// It comes back on the same addresses, with its data.
	srv[leader] = start(t, raft.NodeID(leader+1), dirs[leader], false)
	if srv[leader].APIAddr() != addr {
		t.Fatalf("restarted on %s, was %s", srv[leader].APIAddr(), addr)
	}
	waitFor(t, "the restarted server to catch up", func() bool {
		return srv[leader].Status().Keys == 21 // k0..k19 and the odd key
	})
}

func TestRefusesForeignDataDir(t *testing.T) {
	dir := t.TempDir()
	s := start(t, 1, dir, true)
	s.Close()
	if _, err := Start(Config{ID: 2, DataDir: dir}); err == nil {
		t.Fatal("server 2 started on server 1's data")
	}
}

func TestMeta(t *testing.T) {
	r, a := ParseMeta(EncodeMeta("127.0.0.1:1", "127.0.0.1:2"))
	if r != "127.0.0.1:1" || a != "127.0.0.1:2" {
		t.Fatal(r, a)
	}
}
