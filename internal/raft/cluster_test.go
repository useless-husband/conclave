package raft

import (
	"fmt"
	"sort"
	"testing"

	"github.com/useless-husband/conclave/internal/mutation"
)

// fixedRand makes election timeouts predictable: node i always waits
// ElectionTick+i-1 ticks, so tests can count ticks and the lowest live ID
// times out first.
type fixedRand int

func (f fixedRand) Intn(n int) int { return int(f) % n }

const (
	testElectionTick  = 10
	testHeartbeatTick = 2
)

// testNet is a tiny synchronous network for unit tests: messages queue up
// and are delivered in FIFO order when the test says so.
type testNet struct {
	t       *testing.T
	ids     []NodeID
	nodes   map[NodeID]*Raft
	stores  map[NodeID]*MemStorage
	queue   []Message
	drop    func(Message) bool
	applied map[NodeID][]Entry
	reads   map[NodeID][]ReadState
	aborted map[NodeID][]uint64
	snaps   map[NodeID][]Snapshot
	sent    []Message // every message handed to the transport, in order
	cfgHook func(*Config)
}

type netTransport struct{ n *testNet }

func (tr netTransport) Send(m Message) {
	// Round-trip through the codec so nodes never share memory.
	d, err := DecodeMessage(EncodeMessage(nil, m))
	if err != nil {
		tr.n.t.Fatalf("message does not round-trip: %v (%+v)", err, m)
	}
	tr.n.sent = append(tr.n.sent, d)
	if tr.n.drop != nil && tr.n.drop(d) {
		return
	}
	tr.n.queue = append(tr.n.queue, d)
}

func newTestNet(t *testing.T, n int, hook func(*Config)) *testNet {
	t.Helper()
	net := &testNet{
		t:       t,
		nodes:   map[NodeID]*Raft{},
		stores:  map[NodeID]*MemStorage{},
		applied: map[NodeID][]Entry{},
		reads:   map[NodeID][]ReadState{},
		aborted: map[NodeID][]uint64{},
		snaps:   map[NodeID][]Snapshot{},
		cfgHook: hook,
	}
	var members []Member
	for i := 1; i <= n; i++ {
		members = append(members, Member{ID: NodeID(i)})
	}
	conf := NewMembership(members...)
	for i := 1; i <= n; i++ {
		id := NodeID(i)
		net.ids = append(net.ids, id)
		net.stores[id] = NewMemStorage(conf)
		net.start(id)
	}
	return net
}

// addEmpty starts a node with no membership, as a server about to join.
func (n *testNet) addEmpty(id NodeID) {
	n.ids = append(n.ids, id)
	sort.Slice(n.ids, func(i, j int) bool { return n.ids[i] < n.ids[j] })
	n.stores[id] = NewMemStorage(Membership{})
	n.start(id)
}

// start (re)creates node id from its storage, as after a process restart.
func (n *testNet) start(id NodeID) {
	n.t.Helper()
	cfg := Config{ID: id, ElectionTick: testElectionTick, HeartbeatTick: testHeartbeatTick}
	if n.cfgHook != nil {
		n.cfgHook(&cfg)
	}
	r, err := New(cfg, n.stores[id], netTransport{n}, fixedRand(id-1))
	if err != nil {
		n.t.Fatalf("start n%d: %v", id, err)
	}
	n.nodes[id] = r
}

// restart discards a node's volatile state. MemStorage keeps everything it
// was given, so this models a crash in which no write was lost.
func (n *testNet) restart(id NodeID) {
	n.applied[id] = nil
	n.start(id)
}

func (n *testNet) flush(id NodeID) {
	out := n.nodes[id].Flush()
	if out.Snapshot != nil {
		n.snaps[id] = append(n.snaps[id], *out.Snapshot)
		n.applied[id] = nil
	}
	n.applied[id] = append(n.applied[id], out.Committed...)
	n.reads[id] = append(n.reads[id], out.ReadStates...)
	n.aborted[id] = append(n.aborted[id], out.ReadsAborted...)
}

func (n *testNet) flushAll() {
	for _, id := range n.ids {
		n.flush(id)
	}
}

// run delivers messages until the network is quiet.
func (n *testNet) run() {
	n.t.Helper()
	for steps := 0; ; steps++ {
		n.flushAll()
		if len(n.queue) == 0 {
			return
		}
		if steps > 10000 {
			for _, m := range n.queue {
				n.t.Logf("queued: %v %d->%d term=%d index=%d logterm=%d commit=%d reject=%v ci=%d ct=%d ents=%d",
					m.Type, m.From, m.To, m.Term, m.Index, m.LogTerm, m.Commit, m.Reject, m.ConflictIndex, m.ConflictTerm, len(m.Entries))
			}
			n.t.Fatal("network does not quiesce")
		}
		q := n.queue
		n.queue = nil
		for _, m := range q {
			if r := n.nodes[m.To]; r != nil {
				r.Step(m)
			}
		}
	}
}

func (n *testNet) tick(id NodeID, times int) {
	for i := 0; i < times; i++ {
		n.nodes[id].Tick()
		n.run()
	}
}

func (n *testNet) tickAll(times int) {
	for i := 0; i < times; i++ {
		for _, id := range n.ids {
			n.nodes[id].Tick()
		}
		n.run()
	}
}

// elect makes id time out and win an election. It only works while the
// other nodes are not under a leader's lease.
func (n *testNet) elect(id NodeID) {
	n.t.Helper()
	for i := 0; i < 3*testElectionTick && n.nodes[id].Role() != Leader; i++ {
		n.tick(id, 1)
	}
	if got := n.nodes[id].Role(); got != Leader {
		n.t.Fatalf("n%d is %v after election timeout, want leader", id, got)
	}
}

// force makes id start a real election immediately, bypassing pre-vote and
// leader stickiness, the way a leadership transfer does. Scripted scenarios
// use it to decide exactly who campaigns when.
func (n *testNet) force(id NodeID) {
	n.nodes[id].campaign(campaignTransfer)
	n.run()
}

func (n *testNet) propose(id NodeID, data string) uint64 {
	n.t.Helper()
	idx, _, err := n.nodes[id].Propose([]byte(data))
	if err != nil {
		n.t.Fatalf("propose on n%d: %v", id, err)
	}
	n.run()
	return idx
}

// isolate drops every message to or from the given nodes.
func isolate(ids ...NodeID) func(Message) bool {
	set := map[NodeID]bool{}
	for _, id := range ids {
		set[id] = true
	}
	return func(m Message) bool { return set[m.From] || set[m.To] }
}

func (n *testNet) leaders() []NodeID {
	var out []NodeID
	for _, id := range n.ids {
		if n.nodes[id].Role() == Leader {
			out = append(out, id)
		}
	}
	return out
}

func (n *testNet) normalData(id NodeID) []string {
	var out []string
	for _, e := range n.applied[id] {
		if e.Type == EntryNormal {
			out = append(out, string(e.Data))
		}
	}
	return out
}

func (n *testNet) requireApplied(id NodeID, want ...string) {
	n.t.Helper()
	got := n.normalData(id)
	if fmt.Sprint(got) != fmt.Sprint(want) {
		n.t.Fatalf("n%d applied %v, want %v", id, got, want)
	}
}

func mutate(t *testing.T, ms ...mutation.Mutation) func(*Config) {
	set := mutation.ForTest(t, ms...)
	return func(c *Config) { c.Mutations = set }
}
