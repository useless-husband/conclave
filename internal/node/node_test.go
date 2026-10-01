package node

import (
	"testing"

	"github.com/useless-husband/conclave/internal/kv"
	"github.com/useless-husband/conclave/internal/raft"
)

type rnd int

func (r rnd) Intn(n int) int { return int(r) % n }

// cluster is a synchronous in-memory network of Nodes.
type cluster struct {
	t       *testing.T
	nodes   map[raft.NodeID]*Node
	stores  map[raft.NodeID]*raft.MemStorage
	queue   []raft.Message
	cut     map[raft.NodeID]bool
	resps   map[uint64]Response
	applied map[raft.NodeID][]raft.Entry
	nextID  uint64
}

type tr struct{ c *cluster }

func (t tr) Send(m raft.Message) {
	if t.c.cut[m.From] || t.c.cut[m.To] {
		return
	}
	t.c.queue = append(t.c.queue, m)
}

func newCluster(t *testing.T, n int) *cluster {
	c := &cluster{
		t: t, nodes: map[raft.NodeID]*Node{}, stores: map[raft.NodeID]*raft.MemStorage{},
		cut: map[raft.NodeID]bool{}, resps: map[uint64]Response{}, applied: map[raft.NodeID][]raft.Entry{},
	}
	var members []raft.Member
	for i := 1; i <= n; i++ {
		members = append(members, raft.Member{ID: raft.NodeID(i)})
	}
	conf := raft.NewMembership(members...)
	for i := 1; i <= n; i++ {
		id := raft.NodeID(i)
		c.stores[id] = raft.NewMemStorage(conf)
		c.start(id, 0)
	}
	return c
}

func (c *cluster) start(id raft.NodeID, snapshotEvery uint64) {
	c.t.Helper()
	nd, err := New(Config{
		ID: id, ElectionTick: 10, HeartbeatTick: 2, SnapshotEvery: snapshotEvery,
		OnApply: func(e raft.Entry) { c.applied[id] = append(c.applied[id], e) },
	}, c.stores[id], tr{c}, rnd(id))
	if err != nil {
		c.t.Fatal(err)
	}
	c.nodes[id] = nd
}

func (c *cluster) ready() {
	for id := raft.NodeID(1); int(id) <= len(c.nodes); id++ {
		for _, r := range c.nodes[id].Ready() {
			if _, dup := c.resps[r.ID]; dup {
				c.t.Fatalf("request %d answered twice", r.ID)
			}
			c.resps[r.ID] = r
		}
	}
}

func (c *cluster) run() {
	for i := 0; i < 1000; i++ {
		c.ready()
		if len(c.queue) == 0 {
			return
		}
		q := c.queue
		c.queue = nil
		for _, m := range q {
			c.nodes[m.To].Step(m)
		}
	}
	c.t.Fatal("no quiescence")
}

func (c *cluster) tick(n int) {
	for i := 0; i < n; i++ {
		for id := raft.NodeID(1); int(id) <= len(c.nodes); id++ {
			c.nodes[id].Tick()
		}
		c.run()
	}
}

func (c *cluster) leader() raft.NodeID {
	for id, n := range c.nodes {
		if n.Raft().Role() == raft.Leader && !c.cut[id] {
			return id
		}
	}
	return raft.None
}

func (c *cluster) submit(to raft.NodeID, req Request) Response {
	c.t.Helper()
	c.nextID++
	req.ID = c.nextID
	c.nodes[to].Submit(req)
	c.run()
	r, ok := c.resps[req.ID]
	if !ok {
		c.t.Fatalf("no response to %+v", req)
	}
	return r
}

func cmd(c kv.Command) Request { return Request{Op: OpCommand, Cmd: c} }

func TestWritesReadsAndRedirect(t *testing.T) {
	c := newCluster(t, 3)
	c.tick(30)
	l := c.leader()
	if l == raft.None {
		t.Fatal("no leader")
	}
	if r := c.submit(l, cmd(kv.Command{Kind: kv.Put, Key: "a", Value: "1"})); r.Status != StatusOK || r.Result.Code != kv.OK || r.Index == 0 {
		t.Fatalf("put: %+v", r)
	}
	if r := c.submit(l, cmd(kv.Command{Kind: kv.Get, Key: "a"})); r.Status != StatusOK || r.Result.Value != "1" {
		t.Fatalf("get: %+v", r)
	}
	if r := c.submit(l, cmd(kv.Command{Kind: kv.Get, Key: "zz"})); r.Status != StatusOK || r.Result.Code != kv.NotFound {
		t.Fatalf("get missing: %+v", r)
	}
	f := l%3 + 1
	if r := c.submit(f, cmd(kv.Command{Kind: kv.Put, Key: "a", Value: "2"})); r.Status != StatusNotLeader || r.Leader != l {
		t.Fatalf("follower: %+v", r)
	}
	if r := c.submit(f, cmd(kv.Command{Kind: kv.Get, Key: "a"})); r.Status != StatusNotLeader || r.Leader != l {
		t.Fatalf("follower read: %+v", r)
	}
}

func TestDeposedLeaderAnswersUnknown(t *testing.T) {
	c := newCluster(t, 3)
	c.tick(30)
	l := c.leader()
	c.cut[l] = true
	c.nextID++
	put := Request{ID: c.nextID, Op: OpCommand, Cmd: kv.Command{Kind: kv.Put, Key: "a", Value: "lost?"}}
	c.nodes[l].Submit(put)
	c.nextID++
	get := Request{ID: c.nextID, Op: OpCommand, Cmd: kv.Command{Kind: kv.Get, Key: "a"}}
	c.nodes[l].Submit(get)
	c.tick(40) // the others elect a new leader; the old one steps down
	if r := c.resps[put.ID]; r.Status != StatusUnknown {
		t.Fatalf("pending write on deposed leader: %+v", r)
	}
	if r := c.resps[get.ID]; r.Status != StatusNotLeader {
		t.Fatalf("pending read on deposed leader: %+v", r)
	}
}

func TestSessionRetryAcrossLeaders(t *testing.T) {
	c := newCluster(t, 3)
	c.tick(30)
	l := c.leader()
	sess := c.submit(l, cmd(kv.Command{Kind: kv.Register})).Result.Session
	w := kv.Command{Kind: kv.CAS, Session: sess, Seq: 1, Key: "k", ExpectAbsent: true, Value: "v"}
	if r := c.submit(l, cmd(w)); r.Result.Code != kv.OK {
		t.Fatalf("first: %+v", r)
	}
	// The client did not see the answer, the leader changes, and the
	// client retries on the new leader.
	c.nodes[l].Raft().TransferLeadership(l%3 + 1)
	c.tick(5)
	nl := c.leader()
	if nl == l || nl == raft.None {
		t.Fatalf("leader %d after transfer from %d", nl, l)
	}
	if r := c.submit(nl, cmd(w)); r.Status != StatusOK || r.Result.Code != kv.OK {
		t.Fatalf("retry: %+v (a second application would fail the CAS)", r)
	}
}

func TestMembershipThroughRequests(t *testing.T) {
	c := newCluster(t, 3)
	c.tick(30)
	l := c.leader()
	c.stores[4] = raft.NewMemStorage(raft.Membership{})
	c.start(4, 0)
	if r := c.submit(l, Request{Op: OpAddMember, Member: 4, Meta: []byte("addr")}); r.Status != StatusOK {
		t.Fatalf("add: %+v", r)
	}
	c.tick(5)
	if ids := c.nodes[4].Raft().Membership().IDs(); len(ids) != 4 {
		t.Fatalf("new member sees %v", ids)
	}
	if r := c.submit(l, Request{Op: OpAddMember, Member: 4}); r.Status != StatusRejected {
		t.Fatalf("re-add: %+v", r)
	}
	if r := c.submit(l, Request{Op: OpRemoveMember, Member: 4}); r.Status != StatusOK {
		t.Fatalf("remove: %+v", r)
	}
}

func TestSnapshotCompactionAndRestart(t *testing.T) {
	c := newCluster(t, 3)
	for id := raft.NodeID(1); id <= 3; id++ {
		c.start(id, 5)
	}
	c.tick(30)
	l := c.leader()
	for i := 0; i < 23; i++ {
		c.submit(l, cmd(kv.Command{Kind: kv.Put, Key: "k", Value: string(rune('a' + i))}))
	}
	if st := c.nodes[l].Raft().Status(); st.SnapIndex == 0 {
		t.Fatalf("no compaction: %+v", st)
	}
	// Restart a follower from its storage: it restores the snapshot and
	// replays the rest.
	f := l%3 + 1
	c.start(f, 5)
	c.tick(5)
	if v, _ := c.nodes[f].Store().Read("k"); v != string(rune('a'+22)) {
		t.Fatalf("restarted follower reads %q", v)
	}
	if a, b := c.nodes[f].Store().Digest(), c.nodes[l].Store().Digest(); a != b {
		t.Fatal("replicas differ after restart")
	}
}
