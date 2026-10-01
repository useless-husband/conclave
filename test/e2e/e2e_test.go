// Package e2e runs real conclave processes and checks what clients saw.
//
// TestKillRestartUnderLoad builds the conclave binary, starts a 3-server
// cluster on 127.0.0.1 with ephemeral ports, runs concurrent clients
// against it while servers are killed with SIGKILL (by PID) and restarted
// on their data directories, and checks the recorded history with the same
// linearizability checker the simulator uses.
package e2e

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"math/rand/v2"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/useless-husband/conclave/client"
	"github.com/useless-husband/conclave/internal/kv"
	"github.com/useless-husband/conclave/internal/lincheck"
)

var (
	flagDuration = flag.Duration("e2e.duration", 20*time.Second, "length of the faulty phase")
	flagSeed     = flag.Uint64("e2e.seed", 1, "seed for the fault schedule and the workload")
)

type proc struct {
	id   int
	dir  string
	cmd  *exec.Cmd
	log  *os.File
	api  string
	raft string
}

type cluster struct {
	t    *testing.T
	bin  string
	root string
	mu   sync.Mutex
	ps   []*proc
}

func buildBinary(t *testing.T) string {
	t.Helper()
	bin := filepath.Join(t.TempDir(), "conclave")
	if runtime.GOOS == "windows" {
		bin += ".exe"
	}
	cmd := exec.Command("go", "build", "-o", bin, "../../cmd/conclave")
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		t.Fatalf("build: %v", err)
	}
	return bin
}

func readAddrs(path string) (map[string]any, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var m map[string]any
	return m, json.Unmarshal(b, &m)
}

// start launches server id and waits until it has written its addresses.
func (c *cluster) start(id int, bootstrap bool) {
	c.t.Helper()
	p := c.ps[id-1]
	args := []string{"serve", "-id", fmt.Sprint(id), "-data", p.dir, "-fsync", "full", "-q"}
	if bootstrap {
		args = append(args, "-bootstrap")
	}
	cmd := exec.Command(c.bin, args...)
	cmd.Stdout, cmd.Stderr = p.log, p.log
	if err := cmd.Start(); err != nil {
		c.t.Fatal(err)
	}
	c.mu.Lock()
	p.cmd = cmd
	c.mu.Unlock()
	go cmd.Wait()
	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		m, err := readAddrs(filepath.Join(p.dir, "addresses.json"))
		if err == nil && int(m["pid"].(float64)) == cmd.Process.Pid {
			p.api, p.raft = m["api"].(string), m["raft"].(string)
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	c.t.Fatalf("n%d did not come up", id)
}

// kill sends SIGKILL to server id's process, by PID.
func (c *cluster) kill(id int) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	p := c.ps[id-1]
	if p.cmd == nil {
		return 0
	}
	pid := p.cmd.Process.Pid
	p.cmd.Process.Kill()
	p.cmd = nil
	return pid
}

func (c *cluster) stopAll() {
	for i := range c.ps {
		c.kill(i + 1)
	}
}

func newCluster(t *testing.T) *cluster {
	c := &cluster{t: t, bin: buildBinary(t), root: t.TempDir()}
	for i := 1; i <= 3; i++ {
		dir := filepath.Join(c.root, fmt.Sprintf("n%d", i))
		os.MkdirAll(dir, 0o755)
		f, err := os.Create(filepath.Join(c.root, fmt.Sprintf("n%d.log", i)))
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { f.Close() })
		c.ps = append(c.ps, &proc{id: i, dir: dir, log: f})
	}
	t.Cleanup(c.stopAll)
	c.start(1, true)
	c.start(2, false)
	c.start(3, false)
	admin := client.New([]string{c.ps[0].api}, client.Options{})
	defer admin.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	for _, p := range c.ps[1:] {
		body := map[string]any{"id": p.id, "raft": p.raft, "api": p.api}
		if err := admin.Admin(ctx, "POST", "/v1/members", body); err != nil {
			t.Fatalf("add n%d: %v", p.id, err)
		}
	}
	return c
}

func (c *cluster) addrs() []string {
	var out []string
	for _, p := range c.ps {
		out = append(out, p.api)
	}
	return out
}

// recorder collects the history. Times are nanoseconds on one monotonic
// clock, read before a request is sent and after its answer arrives.
type recorder struct {
	mu   sync.Mutex
	t0   time.Time
	hist []lincheck.Op
}

func (r *recorder) now() int64 { return time.Since(r.t0).Nanoseconds() + 1 }

func (r *recorder) add(op lincheck.Op) int {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.hist = append(r.hist, op)
	return len(r.hist) - 1
}

func (r *recorder) bound(idx []int, at int64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, i := range idx {
		r.hist[i].Return = at
	}
}

type counts struct {
	ok, unknown, failed int
}

// worker runs random operations with its own session until stop closes.
func worker(id int, addrs []string, rec *recorder, seed uint64, stop <-chan struct{}, out *counts) {
	c := client.New(addrs, client.Options{AttemptTimeout: time.Second})
	defer c.Close()
	rng := rand.New(rand.NewPCG(seed, uint64(id)))
	abandoned := []int{} // unknown writes of the current session
	seen := map[string]string{}
	for n := 0; ; n++ {
		select {
		case <-stop:
			return
		default:
		}
		key := fmt.Sprintf("k%d", rng.IntN(4))
		val := fmt.Sprintf("w%d.%d", id, n)
		op := lincheck.Op{Client: id}
		// Odd workers give up quickly, so that some writes end with an
		// unknown outcome while a leader is being replaced.
		timeout := 5 * time.Second
		if id%2 == 1 {
			timeout = 300 * time.Millisecond
		}
		ctx, cancel := context.WithTimeout(context.Background(), timeout)
		op.Call = rec.now()
		var err error
		switch r := rng.IntN(10); {
		case r < 4:
			op.Cmd = kv.Command{Kind: kv.Get, Key: key}
			var v string
			v, err = c.Get(ctx, key)
			switch {
			case err == nil:
				op.Result = kv.Result{Code: kv.OK, Found: true, Value: v}
				seen[key] = v
			case errors.Is(err, client.ErrNotFound):
				op.Result, err = kv.Result{Code: kv.NotFound}, nil
			}
		case r < 7:
			op.Cmd = kv.Command{Kind: kv.Put, Key: key, Value: val}
			err = c.Put(ctx, key, val)
			op.Result = kv.Result{Code: kv.OK}
		case r < 9:
			op.Cmd = kv.Command{Kind: kv.CAS, Key: key, Value: val}
			if prev, ok := seen[key]; ok && rng.IntN(4) != 0 {
				op.Cmd.Expect = prev
			} else {
				op.Cmd.ExpectAbsent = true
			}
			var ok, found bool
			var cur string
			ok, cur, found, err = c.CAS(ctx, key, op.Cmd.Expect, op.Cmd.ExpectAbsent, val)
			if ok {
				op.Result = kv.Result{Code: kv.OK}
			} else {
				op.Result = kv.Result{Code: kv.CASFailed, Found: found, Value: cur}
			}
		default:
			op.Cmd = kv.Command{Kind: kv.Delete, Key: key}
			var old string
			var existed bool
			old, existed, err = c.Delete(ctx, key)
			if existed {
				op.Result = kv.Result{Code: kv.OK, Found: true, Value: old}
			} else {
				op.Result = kv.Result{Code: kv.NotFound}
			}
		}
		cancel()
		ret := rec.now()
		write := op.Cmd.Kind != kv.Get
		switch {
		case err == nil:
			op.Return = ret
			rec.add(op)
			out.ok++
			if write {
				// A later write of the session has been applied, so the
				// abandoned ones took effect before now or never will.
				rec.bound(abandoned, ret)
				abandoned = abandoned[:0]
			}
		case write && errors.Is(err, client.ErrUnknown):
			op.Unknown, op.Result = true, kv.Result{}
			i := rec.add(op)
			out.unknown++
			if errors.Is(err, client.ErrSessionExpired) {
				rec.bound(append(abandoned, i), ret)
				abandoned = abandoned[:0]
			} else {
				abandoned = append(abandoned, i)
			}
		default:
			out.failed++ // not executed (or a read): no effect on the history
		}
	}
}

func TestKillRestartUnderLoad(t *testing.T) {
	dur := *flagDuration
	if testing.Short() {
		dur = 6 * time.Second
	}
	c := newCluster(t)
	rec := &recorder{t0: time.Now()}
	stop := make(chan struct{})
	const workers = 6
	cnt := make([]counts, workers)
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			worker(i, c.addrs(), rec, *flagSeed, stop, &cnt[i])
		}(i)
	}

	// Faults: every 1-2.5 s kill one server (half the time the leader),
	// and bring it back 0.3-1.5 s later on the same data directory.
	rng := rand.New(rand.NewPCG(*flagSeed, 99))
	end := time.Now().Add(dur)
	kills := 0
	for time.Now().Before(end) {
		time.Sleep(time.Duration(1000+rng.IntN(1500)) * time.Millisecond)
		victim := 1 + rng.IntN(3)
		if rng.IntN(2) == 0 {
			if l := leader(c); l != 0 {
				victim = l
			}
		}
		pid := c.kill(victim)
		kills++
		t.Logf("%v: SIGKILL n%d (pid %d)", time.Since(rec.t0).Round(time.Millisecond), victim, pid)
		time.Sleep(time.Duration(300+rng.IntN(1200)) * time.Millisecond)
		c.start(victim, false)
	}
	// Fault-free tail: the cluster must make progress again.
	healOK := 0
	for i := range cnt {
		healOK -= cnt[i].ok
	}
	time.Sleep(3 * time.Second)
	close(stop)
	wg.Wait()
	var tot counts
	for i := range cnt {
		tot.ok += cnt[i].ok
		tot.unknown += cnt[i].unknown
		tot.failed += cnt[i].failed
		healOK += cnt[i].ok
	}
	t.Logf("%d kills; %d operations completed, %d writes with unknown outcome, %d requests not executed", kills, tot.ok, tot.unknown, tot.failed)
	if healOK == 0 {
		t.Fatal("no operation completed after the faults stopped")
	}
	start := time.Now()
	rep := lincheck.CheckKV(rec.hist, 50_000_000)
	t.Logf("linearizability: %v (%d operations checked, %d dropped as unobservable) in %v", rep.Verdict, rep.Ops, rep.Dropped, time.Since(start).Round(time.Millisecond))
	if rep.Verdict != lincheck.Linearizable {
		t.Fatalf("%v", rep)
	}
	// The same history with one read changed to a value nobody wrote must
	// be rejected: the check is looking at this history, not passing it
	// by default.
	bad := append([]lincheck.Op(nil), rec.hist...)
	for i := len(bad) / 2; i < len(bad); i++ {
		if bad[i].Cmd.Kind == kv.Get && bad[i].Result.Code == kv.OK {
			bad[i].Result.Value = "never-written"
			break
		}
	}
	if r := lincheck.CheckKV(bad, 50_000_000); r.Verdict != lincheck.Violation {
		t.Fatalf("a corrupted copy of the history was judged %v", r.Verdict)
	}
	checkConverged(t, c)
}

// leader returns the ID of a server that reports itself leader, or 0.
func leader(c *cluster) int {
	cl := client.New(c.addrs(), client.Options{})
	defer cl.Close()
	for _, p := range c.ps {
		ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
		st, err := cl.Status(ctx, p.api)
		cancel()
		if err == nil && st["role"] == "leader" {
			return p.id
		}
	}
	return 0
}

// checkConverged waits until all three servers have applied the same log
// prefix and hold the same number of keys.
func checkConverged(t *testing.T, c *cluster) {
	cl := client.New(c.addrs(), client.Options{})
	defer cl.Close()
	deadline := time.Now().Add(20 * time.Second)
	var last []string
	for time.Now().Before(deadline) {
		last = last[:0]
		var applied, keys []float64
		for _, p := range c.ps {
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			st, err := cl.Status(ctx, p.api)
			cancel()
			if err != nil {
				break
			}
			applied = append(applied, st["applied"].(float64))
			keys = append(keys, st["keys"].(float64))
			last = append(last, fmt.Sprintf("n%d applied=%v keys=%v role=%v", p.id, st["applied"], st["keys"], st["role"]))
		}
		if len(applied) == 3 && applied[0] == applied[1] && applied[1] == applied[2] && keys[0] == keys[1] && keys[1] == keys[2] {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("servers did not converge: %v", last)
}
