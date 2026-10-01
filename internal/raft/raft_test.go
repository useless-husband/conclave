package raft

import (
	"errors"
	"fmt"
	"reflect"
	"testing"

	"github.com/useless-husband/conclave/internal/mutation"
)

func TestElectionAndReplication(t *testing.T) {
	n := newTestNet(t, 3, nil)
	n.elect(1)
	if got := n.leaders(); !reflect.DeepEqual(got, []NodeID{1}) {
		t.Fatalf("leaders = %v, want [1]", got)
	}
	for _, id := range n.ids {
		st := n.nodes[id].Status()
		if st.Term != 1 || st.Leader != 1 {
			t.Fatalf("n%d: term=%d leader=%d, want term 1 leader 1", id, st.Term, st.Leader)
		}
	}
	n.propose(1, "a")
	n.propose(1, "b")
	n.propose(1, "c")
	for _, id := range n.ids {
		n.requireApplied(id, "a", "b", "c")
		if e := n.applied[id][0]; e.Type != EntryNoop || e.Index != 1 {
			t.Fatalf("n%d: first applied entry %+v, want the leader's no-op at index 1", id, e)
		}
	}
	if _, _, err := n.nodes[2].Propose([]byte("x")); !errors.Is(err, ErrNotLeader) {
		t.Fatalf("propose on follower: err=%v, want ErrNotLeader", err)
	}
}

func TestSingleNodeCommitsAlone(t *testing.T) {
	n := newTestNet(t, 1, nil)
	n.elect(1)
	n.propose(1, "solo")
	n.requireApplied(1, "solo")
}

func TestPreVoteKeepsTermStable(t *testing.T) {
	n := newTestNet(t, 3, nil)
	n.elect(1)
	n.drop = isolate(3)
	// n3 times out again and again but never gets a pre-vote quorum.
	n.tickAll(8 * testElectionTick)
	if st := n.nodes[3].Status(); st.Term != 1 || st.Role != PreCandidate {
		t.Fatalf("isolated n3: term=%d role=%v, want term 1 pre-candidate", st.Term, st.Role)
	}
	n.drop = nil
	n.tickAll(3 * testElectionTick)
	if got := n.leaders(); !reflect.DeepEqual(got, []NodeID{1}) {
		t.Fatalf("after heal leaders = %v, want [1]", got)
	}
	for _, id := range n.ids {
		if term := n.nodes[id].Term(); term != 1 {
			t.Fatalf("n%d term = %d after heal, want 1: the partitioned node disturbed the cluster", id, term)
		}
	}
}

func TestLeaderStickiness(t *testing.T) {
	n := newTestNet(t, 3, nil)
	n.elect(1)
	req := Message{Type: MsgVote, From: 3, To: 2, Term: 9, Index: 100, LogTerm: 9}
	n.nodes[2].Step(req)
	n.run()
	if term := n.nodes[2].Term(); term != 1 {
		t.Fatalf("follower in contact with its leader adopted term %d from a vote request", term)
	}
	req.Transfer = true
	n.nodes[2].Step(req)
	n.flush(2)
	if st := n.nodes[2].Status(); st.Term != 9 || st.Vote != 3 {
		t.Fatalf("transfer vote request: term=%d vote=%d, want term 9 vote 3", st.Term, st.Vote)
	}
}

func TestCheckQuorumStepsDown(t *testing.T) {
	n := newTestNet(t, 3, nil)
	n.elect(1)
	n.drop = isolate(1)
	n.tick(1, 2*testElectionTick)
	if st := n.nodes[1].Status(); st.Role != Follower || st.Term != 1 {
		t.Fatalf("isolated leader: role=%v term=%d, want follower in term 1", st.Role, st.Term)
	}
}

func TestStaleLeaderLearnsNewTerm(t *testing.T) {
	n := newTestNet(t, 3, nil)
	n.elect(1)
	n.drop = isolate(1)
	n.force(2)
	if n.nodes[2].Role() != Leader || n.nodes[1].Role() != Leader {
		t.Fatalf("want both n1 (stale) and n2 to believe they lead; roles %v %v", n.nodes[1].Role(), n.nodes[2].Role())
	}
	n.drop = nil
	n.tick(1, testHeartbeatTick) // stale heartbeat goes out, answers carry term 2
	if st := n.nodes[1].Status(); st.Role != Follower || st.Term != 2 {
		t.Fatalf("stale leader: role=%v term=%d, want follower in term 2", st.Role, st.Term)
	}
}

// TestFastBacktracking checks that a follower with a long divergent suffix
// is repaired in a constant number of round trips, not one per entry.
func TestFastBacktracking(t *testing.T) {
	n := newTestNet(t, 3, nil)
	n.elect(1)
	// n1 is cut off and piles up 100 entries of term 1 nobody else has.
	n.drop = isolate(1)
	for i := 0; i < 100; i++ {
		if _, _, err := n.nodes[1].Propose([]byte("lost")); err != nil {
			t.Fatal(err)
		}
	}
	n.run()
	// n2 leads term 2 and commits 50 entries, then n3 takes over in term
	// 3, so n3 starts out probing n1 far above the point of divergence.
	n.force(2)
	for i := 0; i < 50; i++ {
		n.propose(2, fmt.Sprintf("kept%d", i))
	}
	n.force(3)
	if n.nodes[3].Role() != Leader {
		t.Fatalf("n3 role %v, want leader", n.nodes[3].Role())
	}
	n.drop = nil
	n.sent = nil
	n.tickAll(3 * testHeartbeatTick)

	rejects := 0
	for _, m := range n.sent {
		if m.Type == MsgAppResp && m.From == 1 && m.Reject && m.Term == 3 {
			rejects++
		}
	}
	if rejects == 0 || rejects > 2 {
		t.Fatalf("follower with 100 divergent entries needed %d rejections, want 1 or 2", rejects)
	}
	if got, want := n.nodes[1].Status().LastIndex, n.nodes[3].Status().LastIndex; got != want {
		t.Fatalf("n1 last index %d, leader %d", got, want)
	}
	if got := n.normalData(1); len(got) != 50 || got[0] != "kept0" || got[49] != "kept49" {
		t.Fatalf("n1 applied %d commands, want the 50 kept ones", len(got))
	}
}

// figure8 drives five servers into the situation of Raft Figure 8(c): an
// entry from an old term is stored on a majority while the current leader's
// own entry is not. It returns the net and the index of the old entry.
func figure8(t *testing.T, hook func(*Config)) (*testNet, uint64) {
	n := newTestNet(t, 5, func(c *Config) {
		c.MaxEntriesPerMsg = 1
		if hook != nil {
			hook(c)
		}
	})
	n.elect(1)
	// (a) n1 replicates index 2 only to n2.
	n.drop = isolate(3, 4, 5)
	old := n.propose(1, "old")
	// (b) n5 wins term 2 with n3 and n4; its entry at index 2 stays local.
	cut := isolate(1, 2)
	n.drop = func(m Message) bool { return cut(m) || (m.From == 5 && m.Type == MsgApp) }
	n.force(5)
	if n.nodes[5].Role() != Leader {
		t.Fatalf("n5 role %v, want leader", n.nodes[5].Role())
	}
	// (c) n1 wins term 3 and copies the old entry to n3 and n4, but its
	// own term-3 no-op reaches nobody.
	n.drop = func(m Message) bool {
		if m.From == 5 || m.To == 5 {
			return true
		}
		// Let the old entry through; once the target holds it, lose
		// every append that would carry the new leader's own entry.
		if m.Type == MsgApp && m.From == 1 && n.nodes[m.To].log.lastIndex() >= old {
			for _, e := range m.Entries {
				if e.Term == 3 {
					return true
				}
			}
		}
		return false
	}
	n.force(1) // term 2: n3 already voted for n5 in this term
	n.force(1) // term 3
	if st := n.nodes[1].Status(); st.Role != Leader || st.Term != 3 {
		t.Fatalf("n1 role=%v term=%d, want leader of term 3", st.Role, st.Term)
	}
	for _, id := range []NodeID{1, 2, 3, 4} {
		if !n.nodes[id].log.matches(old, 1) {
			t.Fatalf("n%d does not hold the term-1 entry at index %d", id, old)
		}
	}
	return n, old
}

func TestFigure8OldTermEntryIsNotCommittedByCount(t *testing.T) {
	n, old := figure8(t, nil)
	if c := n.nodes[1].Commit(); c >= old {
		t.Fatalf("leader committed index %d: an entry of an earlier term was committed by counting replicas", c)
	}
	// Once an entry of the leader's own term reaches a majority, the old
	// entry commits with it.
	n.drop = isolate(4, 5)
	n.tick(1, 4*testHeartbeatTick)
	if c := n.nodes[1].Commit(); c < old+1 {
		t.Fatalf("commit = %d after current-term entry replicated, want >= %d", c, old+1)
	}
}

func TestFigure8Mutant(t *testing.T) {
	n, old := figure8(t, mutate(t, mutation.CommitPriorTermByCount))
	if c := n.nodes[1].Commit(); c != old {
		t.Fatalf("mutant commit = %d, want %d (the bug should be observable here)", c, old)
	}
	// (d) n5 now wins term 4 with n3 and n4 and replaces the entry that
	// n1 has already applied: two servers apply different commands at the
	// same index.
	n.drop = isolate(1, 2)
	n.force(5) // term 3: n3 and n4 already voted for n1 in this term
	n.force(5) // term 4
	if n.nodes[5].Role() != Leader {
		t.Fatalf("n5 role %v, want leader: its log ends in term 2, later than the others' term 1", n.nodes[5].Role())
	}
	at := func(id NodeID) Entry {
		for _, e := range n.applied[id] {
			if e.Index == old {
				return e
			}
		}
		t.Fatalf("n%d has not applied index %d", id, old)
		return Entry{}
	}
	if a, b := at(1), at(5); a.Term == b.Term {
		t.Fatalf("n1 and n5 applied the same entry at index %d; expected divergence", old)
	}
}

func TestVoteSurvivesRestart(t *testing.T) {
	run := func(hook func(*Config)) (granted bool) {
		n := newTestNet(t, 3, hook)
		n.nodes[2].Step(Message{Type: MsgVote, From: 1, To: 2, Term: 1})
		n.flush(2)
		n.restart(2)
		n.sent = nil
		n.nodes[2].Step(Message{Type: MsgVote, From: 3, To: 2, Term: 1})
		n.flush(2)
		for _, m := range n.sent {
			if m.Type == MsgVoteResp && m.To == 3 {
				return !m.Reject
			}
		}
		t.Fatal("no vote response")
		return false
	}
	if run(nil) {
		t.Fatal("n2 voted for two candidates in the same term across a restart")
	}
	if !run(mutate(t, mutation.VoteNotPersisted)) {
		t.Fatal("mutant did not vote twice; the mutation is not observable")
	}
}

func TestVoteRequiresUpToDateLog(t *testing.T) {
	run := func(hook func(*Config)) (granted bool) {
		n := newTestNet(t, 3, hook)
		n.elect(1)
		n.propose(1, "a")
		n.sent = nil
		// A candidate whose log is empty asks n2, which holds two entries.
		n.nodes[2].Step(Message{Type: MsgVote, From: 3, To: 2, Term: 5, Transfer: true})
		n.flush(2)
		for _, m := range n.sent {
			if m.Type == MsgVoteResp && m.To == 3 {
				return !m.Reject
			}
		}
		t.Fatal("no vote response")
		return false
	}
	if run(nil) {
		t.Fatal("vote granted to a candidate with a shorter log")
	}
	if !run(mutate(t, mutation.VoteWithoutLogCheck)) {
		t.Fatal("mutant refused the vote; the mutation is not observable")
	}
}

// recorder logs the order in which a node touches storage and network.
type recorder struct {
	*MemStorage
	events *[]string
	onSync func()
}

func (r recorder) SaveHardState(hs HardState) error {
	*r.events = append(*r.events, "hardstate")
	return r.MemStorage.SaveHardState(hs)
}

func (r recorder) Append(e []Entry) error {
	*r.events = append(*r.events, "append")
	return r.MemStorage.Append(e)
}

func (r recorder) Sync() error {
	if r.onSync != nil {
		r.onSync()
	}
	*r.events = append(*r.events, "sync")
	return r.MemStorage.Sync()
}

type recTransport struct{ events *[]string }

func (r recTransport) Send(m Message) { *r.events = append(*r.events, "send:"+m.Type.String()) }

func newRecorded(t *testing.T, id NodeID, voters int, hook func(*Config)) (*Raft, *[]string, *recorder) {
	t.Helper()
	var members []Member
	for i := 1; i <= voters; i++ {
		members = append(members, Member{ID: NodeID(i)})
	}
	events := &[]string{}
	st := &recorder{MemStorage: NewMemStorage(NewMembership(members...)), events: events}
	cfg := Config{ID: id, ElectionTick: testElectionTick, HeartbeatTick: testHeartbeatTick}
	if hook != nil {
		hook(&cfg)
	}
	r, err := New(cfg, st, recTransport{events}, fixedRand(0))
	if err != nil {
		t.Fatal(err)
	}
	return r, events, st
}

func TestPromisesLeaveOnlyAfterSync(t *testing.T) {
	t.Run("vote", func(t *testing.T) {
		r, events, _ := newRecorded(t, 2, 3, nil)
		r.Step(Message{Type: MsgVote, From: 1, To: 2, Term: 1})
		r.Flush()
		want := []string{"hardstate", "hardstate", "sync", "send:VoteResp"}
		if !reflect.DeepEqual(*events, want) {
			t.Fatalf("events %v, want %v", *events, want)
		}
	})
	t.Run("append", func(t *testing.T) {
		r, events, _ := newRecorded(t, 2, 3, nil)
		r.Step(Message{Type: MsgApp, From: 1, To: 2, Term: 1, Entries: []Entry{{Index: 1, Term: 1}}})
		r.Flush()
		want := []string{"hardstate", "append", "sync", "send:AppResp"}
		if !reflect.DeepEqual(*events, want) {
			t.Fatalf("events %v, want %v", *events, want)
		}
	})
	t.Run("append mutant", func(t *testing.T) {
		r, events, _ := newRecorded(t, 2, 3, mutate(t, mutation.AckBeforeFsync))
		r.Step(Message{Type: MsgApp, From: 1, To: 2, Term: 1, Entries: []Entry{{Index: 1, Term: 1}}})
		r.Flush()
		want := []string{"hardstate", "append", "send:AppResp", "sync"}
		if !reflect.DeepEqual(*events, want) {
			t.Fatalf("events %v, want %v", *events, want)
		}
	})
	t.Run("leader sends before its own sync but does not count itself", func(t *testing.T) {
		r, events, st := newRecorded(t, 1, 1, nil)
		for i := 0; i < testElectionTick; i++ {
			r.Tick()
		}
		if r.Role() != Leader {
			t.Fatalf("role %v", r.Role())
		}
		r.Flush()
		*events = nil
		idx, _, err := r.Propose([]byte("x"))
		if err != nil {
			t.Fatal(err)
		}
		st.onSync = func() {
			if r.Commit() >= idx {
				t.Errorf("leader committed index %d before its own fsync", idx)
			}
		}
		out := r.Flush()
		if len(out.Committed) != 1 || out.Committed[0].Index != idx {
			t.Fatalf("committed %+v, want entry %d", out.Committed, idx)
		}
		if want := []string{"append", "sync"}; !reflect.DeepEqual(*events, want) {
			t.Fatalf("events %v, want %v", *events, want)
		}
	})
	t.Run("leader appends go out before sync", func(t *testing.T) {
		n := newTestNet(t, 3, nil)
		n.elect(1)
		events := &[]string{}
		n.nodes[1].st = &recorder{MemStorage: n.stores[1], events: events}
		n.nodes[1].tr = recTransport{events}
		if _, _, err := n.nodes[1].Propose([]byte("x")); err != nil {
			t.Fatal(err)
		}
		n.nodes[1].Flush()
		want := []string{"append", "send:App", "send:App", "sync"}
		if !reflect.DeepEqual(*events, want) {
			t.Fatalf("events %v, want %v", *events, want)
		}
	})
}

func TestReadIndex(t *testing.T) {
	n := newTestNet(t, 3, nil)
	n.elect(1)
	n.propose(1, "a")
	if err := n.nodes[1].ReadIndex(7); err != nil {
		t.Fatal(err)
	}
	if len(n.reads[1]) != 0 {
		t.Fatal("read confirmed before any follower answered")
	}
	n.run()
	if want := []ReadState{{Context: 7, Index: 2}}; !reflect.DeepEqual(n.reads[1], want) {
		t.Fatalf("read states %+v, want %+v", n.reads[1], want)
	}
	if err := n.nodes[2].ReadIndex(1); !errors.Is(err, ErrNotLeader) {
		t.Fatalf("ReadIndex on follower: %v", err)
	}

	// A deposed leader must never confirm a read.
	n.reads[1] = nil
	n.drop = isolate(1)
	n.force(2)
	n.propose(2, "b")
	if err := n.nodes[1].ReadIndex(8); err != nil {
		t.Fatal(err)
	}
	n.tick(1, 2*testElectionTick)
	if len(n.reads[1]) != 0 {
		t.Fatalf("deposed leader confirmed a read: %+v", n.reads[1])
	}
	if !reflect.DeepEqual(n.aborted[1], []uint64{8}) {
		t.Fatalf("aborted reads %v, want [8]", n.aborted[1])
	}
}

func TestReadIndexMutantServesStale(t *testing.T) {
	n := newTestNet(t, 3, mutate(t, mutation.ReadWithoutQuorum))
	n.elect(1)
	n.propose(1, "a")
	n.drop = isolate(1)
	n.force(2)
	n.propose(2, "b")
	if err := n.nodes[1].ReadIndex(8); err != nil {
		t.Fatal(err)
	}
	n.flush(1)
	if len(n.reads[1]) != 1 || n.reads[1][0].Index >= n.nodes[2].Commit() {
		t.Fatalf("mutant read states %+v with new leader commit %d: expected a stale confirmation",
			n.reads[1], n.nodes[2].Commit())
	}
}

func TestReadIndexWaitsForTermCommit(t *testing.T) {
	n := newTestNet(t, 3, nil)
	n.elect(1)
	n.drop = func(m Message) bool { return m.Type == MsgApp }
	n.force(2)
	if n.nodes[2].Role() != Leader {
		t.Fatal("n2 not leader")
	}
	if err := n.nodes[2].ReadIndex(1); !errors.Is(err, ErrNotReady) {
		t.Fatalf("ReadIndex before the no-op committed: %v, want ErrNotReady", err)
	}
	if _, _, err := n.nodes[2].ProposeConfChange(ConfChange{Type: RemoveNode, Node: 3}); !errors.Is(err, ErrNotReady) {
		t.Fatalf("conf change before the no-op committed: %v, want ErrNotReady", err)
	}
}

func TestMembershipChange(t *testing.T) {
	n := newTestNet(t, 3, nil)
	n.elect(1)
	n.propose(1, "a")
	n.addEmpty(4)
	if _, _, err := n.nodes[1].ProposeConfChange(ConfChange{Type: AddNode, Node: 4, Meta: []byte("addr4")}); err != nil {
		t.Fatal(err)
	}
	if _, _, err := n.nodes[1].ProposeConfChange(ConfChange{Type: RemoveNode, Node: 2}); !errors.Is(err, ErrConfChangePending) {
		t.Fatalf("second change while first uncommitted: %v, want ErrConfChangePending", err)
	}
	n.run()
	n.tickAll(2 * testHeartbeatTick)
	for _, id := range n.ids {
		conf := n.nodes[id].Membership()
		if got := conf.IDs(); !reflect.DeepEqual(got, []NodeID{1, 2, 3, 4}) {
			t.Fatalf("n%d members %v, want [1 2 3 4]", id, got)
		}
		if string(conf.Members[3].Meta) != "addr4" {
			t.Fatalf("n%d lost member metadata: %q", id, conf.Members[3].Meta)
		}
	}
	n.requireApplied(4, "a")

	if _, _, err := n.nodes[1].ProposeConfChange(ConfChange{Type: AddNode, Node: 4}); !errors.Is(err, ErrBadConfChange) {
		t.Fatalf("adding an existing member: %v", err)
	}
	if _, _, err := n.nodes[1].ProposeConfChange(ConfChange{Type: RemoveNode, Node: 9}); !errors.Is(err, ErrBadConfChange) {
		t.Fatalf("removing a non-member: %v", err)
	}

	// The leader removes itself, keeps leading until the change commits,
	// then steps down and hands off.
	if _, _, err := n.nodes[1].ProposeConfChange(ConfChange{Type: RemoveNode, Node: 1}); err != nil {
		t.Fatal(err)
	}
	n.run()
	if n.nodes[1].Role() == Leader {
		t.Fatal("removed leader still leads after its removal committed")
	}
	leaders := n.leaders()
	if len(leaders) != 1 {
		t.Fatalf("leaders after hand-off: %v, want exactly one", leaders)
	}
	n.propose(leaders[0], "b")
	for _, id := range []NodeID{2, 3, 4} {
		n.requireApplied(id, "a", "b")
		if got := n.nodes[id].Membership().IDs(); !reflect.DeepEqual(got, []NodeID{2, 3, 4}) {
			t.Fatalf("n%d members %v, want [2 3 4]", id, got)
		}
	}
	// The removed server no longer campaigns.
	term := n.nodes[leaders[0]].Term()
	n.tick(1, 5*testElectionTick)
	if n.nodes[1].Role() != Follower || n.nodes[leaders[0]].Term() != term {
		t.Fatalf("removed server disturbed the cluster: role=%v term %d -> %d",
			n.nodes[1].Role(), term, n.nodes[leaders[0]].Term())
	}
}

func TestLastVoterCannotBeRemoved(t *testing.T) {
	n := newTestNet(t, 1, nil)
	n.elect(1)
	n.run()
	if _, _, err := n.nodes[1].ProposeConfChange(ConfChange{Type: RemoveNode, Node: 1}); !errors.Is(err, ErrBadConfChange) {
		t.Fatalf("removing the last voter: %v, want ErrBadConfChange", err)
	}
}

func TestUncommittedConfChangeIsRolledBack(t *testing.T) {
	n := newTestNet(t, 3, nil)
	n.elect(1)
	n.propose(1, "a")
	n.drop = isolate(1)
	if _, _, err := n.nodes[1].ProposeConfChange(ConfChange{Type: AddNode, Node: 4}); err != nil {
		t.Fatal(err)
	}
	n.run()
	if got := n.nodes[1].Membership().IDs(); len(got) != 4 {
		t.Fatalf("n1 members %v: a server uses the latest configuration in its log, committed or not", got)
	}
	n.force(2)
	n.propose(2, "b")
	n.drop = nil
	n.tickAll(3 * testHeartbeatTick)
	if got := n.nodes[1].Membership().IDs(); !reflect.DeepEqual(got, []NodeID{1, 2, 3}) {
		t.Fatalf("n1 members %v after its uncommitted change was overwritten, want [1 2 3]", got)
	}
	n.requireApplied(1, "a", "b")
}

func TestLeadershipTransfer(t *testing.T) {
	n := newTestNet(t, 3, nil)
	n.elect(1)
	n.propose(1, "a")
	if err := n.nodes[1].TransferLeadership(2); err != nil {
		t.Fatal(err)
	}
	if _, _, err := n.nodes[1].Propose([]byte("x")); !errors.Is(err, ErrTransferring) {
		t.Fatalf("propose during transfer: %v, want ErrTransferring", err)
	}
	n.run()
	if got := n.leaders(); !reflect.DeepEqual(got, []NodeID{2}) {
		t.Fatalf("leaders %v, want [2]", got)
	}
	if term := n.nodes[2].Term(); term != 2 {
		t.Fatalf("term %d, want 2", term)
	}
	n.propose(2, "b")
	n.requireApplied(3, "a", "b")

	// A transfer whose TimeoutNow is lost is abandoned after an election
	// timeout and the leader serves again.
	n.drop = func(m Message) bool { return m.Type == MsgTimeoutNow }
	if err := n.nodes[2].TransferLeadership(3); err != nil {
		t.Fatal(err)
	}
	n.tickAll(testElectionTick + 1)
	if n.nodes[2].Role() != Leader {
		t.Fatalf("n2 role %v, want leader", n.nodes[2].Role())
	}
	n.propose(2, "c")
	n.requireApplied(1, "a", "b", "c")
}

func TestSnapshotCatchUp(t *testing.T) {
	n := newTestNet(t, 3, nil)
	n.elect(1)
	n.drop = isolate(3)
	for i := 0; i < 20; i++ {
		n.propose(1, fmt.Sprintf("c%d", i))
	}
	at := n.nodes[1].Status().Applied
	if err := n.nodes[1].Compact(at, []byte("image")); err != nil {
		t.Fatal(err)
	}
	if first := n.nodes[1].Status().FirstIndex; first != at+1 {
		t.Fatalf("first index %d after compaction at %d", first, at)
	}
	n.propose(1, "after")
	n.drop = nil
	n.tickAll(3 * testHeartbeatTick)

	if len(n.snaps[3]) != 1 {
		t.Fatalf("n3 received %d snapshots, want 1", len(n.snaps[3]))
	}
	s := n.snaps[3][0]
	if s.Index != at || string(s.Data) != "image" || len(s.Conf.Members) != 3 {
		t.Fatalf("snapshot index=%d data=%q members=%d", s.Index, s.Data, len(s.Conf.Members))
	}
	n.requireApplied(3, "after")
	if got, want := n.nodes[3].Commit(), n.nodes[1].Commit(); got != want {
		t.Fatalf("n3 commit %d, leader %d", got, want)
	}
}

func TestSnapshotTrailingKeepsEntries(t *testing.T) {
	n := newTestNet(t, 3, func(c *Config) { c.SnapshotTrailing = 5 })
	n.elect(1)
	n.drop = isolate(3)
	for i := 0; i < 10; i++ {
		n.propose(1, fmt.Sprintf("c%d", i))
	}
	at := n.nodes[1].Status().Applied
	if err := n.nodes[1].Compact(at, []byte("image")); err != nil {
		t.Fatal(err)
	}
	if first := n.nodes[1].Status().FirstIndex; first != at-5+1 {
		t.Fatalf("first index %d, want %d", first, at-5+1)
	}
}

func TestRestartKeepsLogAndTerm(t *testing.T) {
	n := newTestNet(t, 3, nil)
	n.elect(1)
	n.propose(1, "a")
	n.propose(1, "b")
	before := n.nodes[2].Status()
	n.restart(2)
	after := n.nodes[2].Status()
	if after.Term != before.Term || after.LastIndex != before.LastIndex || after.Vote != before.Vote {
		t.Fatalf("restart changed durable state: %+v -> %+v", before, after)
	}
	if after.Commit != 0 {
		t.Fatalf("commit %d after restart: nothing is committed until a leader says so", after.Commit)
	}
	n.tickAll(testHeartbeatTick)
	n.requireApplied(2, "a", "b")
}

func TestLogHelpers(t *testing.T) {
	ents := []Entry{{Index: 4, Term: 2}, {Index: 5, Term: 2}, {Index: 6, Term: 3}, {Index: 7, Term: 5}}
	l, err := newLog(3, 1, ents)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := newLog(3, 1, ents[1:]); err == nil {
		t.Fatal("gap after compaction point accepted")
	}
	if l.firstIndex() != 4 || l.lastIndex() != 7 || l.lastTerm() != 5 {
		t.Fatalf("bounds %d %d %d", l.firstIndex(), l.lastIndex(), l.lastTerm())
	}
	for _, c := range []struct {
		i    uint64
		term uint64
		ok   bool
	}{{2, 0, false}, {3, 1, true}, {5, 2, true}, {7, 5, true}, {8, 0, false}} {
		if term, ok := l.term(c.i); term != c.term || ok != c.ok {
			t.Errorf("term(%d) = %d,%v want %d,%v", c.i, term, ok, c.term, c.ok)
		}
	}
	if got := l.firstIndexOfTerm(5, 2); got != 4 {
		t.Errorf("firstIndexOfTerm(5,2) = %d", got)
	}
	if got := l.lastIndexOfTerm(7, 2); got != 5 {
		t.Errorf("lastIndexOfTerm(7,2) = %d", got)
	}
	if got := l.lastIndexOfTerm(7, 4); got != 0 {
		t.Errorf("lastIndexOfTerm(7,4) = %d, want 0", got)
	}
	if !l.isUpToDate(7, 5) || !l.isUpToDate(1, 6) || l.isUpToDate(6, 5) || l.isUpToDate(100, 4) {
		t.Error("isUpToDate")
	}
	held := l.slice(6, 7)
	l.truncateAndAppend([]Entry{{Index: 6, Term: 6}})
	if held[0].Term != 3 || held[1].Term != 5 {
		t.Errorf("slice taken before truncation changed: %+v", held)
	}
	if l.lastIndex() != 6 || l.lastTerm() != 6 || l.stable != 5 {
		t.Errorf("after truncate: last=%d term=%d stable=%d", l.lastIndex(), l.lastTerm(), l.stable)
	}
	l.compactTo(5)
	if l.prevIndex != 5 || l.prevTerm != 2 || len(l.entries) != 1 {
		t.Errorf("after compact: prev=%d/%d len=%d", l.prevIndex, l.prevTerm, len(l.entries))
	}
}

func TestMembershipApply(t *testing.T) {
	m := NewMembership(Member{ID: 3}, Member{ID: 1})
	m = m.apply(ConfChange{Type: AddNode, Node: 2, Meta: []byte("x")})
	m = m.apply(ConfChange{Type: AddNode, Node: 9})
	if got := m.IDs(); !reflect.DeepEqual(got, []NodeID{1, 2, 3, 9}) {
		t.Fatalf("ids %v", got)
	}
	m = m.apply(ConfChange{Type: RemoveNode, Node: 1})
	m = m.apply(ConfChange{Type: RemoveNode, Node: 7})
	if got := m.IDs(); !reflect.DeepEqual(got, []NodeID{2, 3, 9}) {
		t.Fatalf("ids %v", got)
	}
	if !m.Contains(9) || m.Contains(1) || m.Contains(4) {
		t.Fatal("Contains")
	}
}

// ongaro2015 replays the single-server membership bug reported by Diego
// Ongaro on raft-dev in 2015. In C0 = {1,2,3,4}, n1 starts adding n5 but the
// new configuration C1 reaches only n5. n2 wins term 2 and, before
// committing anything of its own term, removes n1 (C2 = {2,3,4}) and commits
// that with n3. n1 then wins term 3 with n4 and n5, a majority of C1, and
// commits C1. Two different entries are committed at the same index.
//
// With the guard, n2 cannot propose C2 until it has committed an entry of
// its term, which would need n4, whose vote n1 then could not get.
func ongaro2015(t *testing.T, hook func(*Config)) (*testNet, error) {
	n := newTestNet(t, 4, hook)
	n.elect(1)
	n.addEmpty(5)
	n.drop = func(m Message) bool { return m.From == 1 && m.Type == MsgApp && m.To != 5 }
	if _, _, err := n.nodes[1].ProposeConfChange(ConfChange{Type: AddNode, Node: 5}); err != nil {
		t.Fatal(err)
	}
	n.run()
	if got := len(n.nodes[5].Membership().Members); got != 5 {
		t.Fatalf("n5 sees %d members, want C1 with 5", got)
	}

	// n2 wins term 2 with n3 and n4; n4 gets no entries from it.
	cut := isolate(1, 5)
	n.drop = func(m Message) bool { return cut(m) || m.From == 2 && m.To == 4 && m.Type == MsgApp }
	n.force(2)
	if n.nodes[2].Role() != Leader {
		t.Fatal("n2 did not win term 2")
	}
	_, _, err := n.nodes[2].ProposeConfChange(ConfChange{Type: RemoveNode, Node: 1})
	if err != nil {
		return n, err
	}
	n.run()

	// n1 wins term 3 with n4 and n5 and commits C1.
	n.drop = isolate(2, 3)
	n.force(1) // term 2: n4 has already voted for n2
	n.force(1) // term 3
	if n.nodes[1].Role() != Leader {
		t.Fatalf("n1 role %v, want leader of term 3", n.nodes[1].Role())
	}
	n.tick(1, 2*testHeartbeatTick)
	return n, nil
}

func TestOngaro2015MembershipBug(t *testing.T) {
	if _, err := ongaro2015(t, nil); !errors.Is(err, ErrNotReady) {
		t.Fatalf("n2 proposed a membership change before committing in its term: %v", err)
	}
	n, err := ongaro2015(t, mutate(t, mutation.ConfChangeBeforeTermCommit))
	if err != nil {
		t.Fatal(err)
	}
	at := func(id NodeID, index uint64) (Entry, bool) {
		for _, e := range n.applied[id] {
			if e.Index == index {
				return e, true
			}
		}
		return Entry{}, false
	}
	c2, ok2 := at(3, 3)
	c1, ok1 := at(4, 2)
	other, okOther := at(3, 2)
	if !ok1 || !ok2 || !okOther {
		t.Fatalf("expected n3 to apply indexes 2-3 and n4 index 2; n3 %v, n4 %v", n.applied[3], n.applied[4])
	}
	if c2.Type != EntryConfChange || c1.Type != EntryConfChange || other.Term == c1.Term {
		t.Fatalf("mutant did not diverge: n4 applied %+v at 2, n3 applied %+v at 2", c1, other)
	}
}
