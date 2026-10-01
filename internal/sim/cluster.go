package sim

import (
	"fmt"
	"hash/fnv"
	"runtime/debug"
	"strings"

	"github.com/useless-husband/conclave/internal/node"
	"github.com/useless-husband/conclave/internal/prng"
	"github.com/useless-husband/conclave/internal/raft"
	"github.com/useless-husband/conclave/internal/simdisk"
	"github.com/useless-husband/conclave/internal/wal"
)

// Timing of the simulated servers, in the units the production server uses
// by default: a 10 ms tick, heartbeats every 2 ticks, elections after 10-20.
const (
	tickInterval  = 10 * Millisecond
	electionTicks = 10
	heartbeatTick = 2
)

type simNode struct {
	id   raft.NodeID
	disk *simdisk.Disk
	nd   *node.Node // nil while down
	inc  uint64     // incarnation, bumped by every crash
	rng  *prng.Rand // forked per incarnation for raft's election jitter

	tick        Time // this node's tick period (clock skew)
	pausedUntil Time
	retired     bool // removed from the cluster and shut down for good
	joined      bool // part of the initial cluster or added successfully
	broken      bool // cannot recover its storage; stays down
	bounce      bool // the current crash is followed by a quick restart

	// Last observed role, term, vote and commit index, to notice
	// transitions.
	lastRole   raft.Role
	lastTerm   uint64
	lastVote   raft.NodeID
	lastCommit uint64

	// outbox holds the messages sent within the last sendWindow; a crash
	// loses some of them.
	outbox []*event
}

func (n *simNode) up() bool { return n.nd != nil }

type transport struct {
	s    *Sim
	from raft.NodeID
}

func (t transport) Send(m raft.Message) { t.s.sendRaft(t.from, m) }

// addNode creates the next server. Initial servers share a bootstrapped
// membership; later ones start empty and wait to be added.
func (s *Sim) addNode(initial bool) *simNode {
	id := raft.NodeID(len(s.nodes) + 1)
	disk := simdisk.New(s.rng.Fork(100 + uint64(id)))
	disk.TornPercent = s.prof.TornPercent
	n := &simNode{id: id, disk: disk, rng: s.rng.Fork(200 + uint64(id)), joined: initial}
	skew := 0
	if s.prof.SkewPercent > 0 {
		skew = s.nemRng.Intn(2*s.prof.SkewPercent+1) - s.prof.SkewPercent
	}
	n.tick = tickInterval * Time(100+skew) / 100
	s.nodes = append(s.nodes, n)
	if initial {
		w, err := wal.Open(disk, s.walOptions())
		if err != nil {
			panic(fmt.Sprintf("sim: open fresh wal: %v", err))
		}
		var members []raft.Member
		for i := 1; i <= s.prof.Nodes; i++ {
			members = append(members, raft.Member{ID: raft.NodeID(i)})
		}
		if err := w.Bootstrap(raft.NewMembership(members...)); err != nil {
			panic(fmt.Sprintf("sim: bootstrap: %v", err))
		}
		if err := w.Close(); err != nil {
			panic(err)
		}
		// Bootstrap is done by an operator before the server first runs;
		// make it durable regardless of the crash schedule.
		if err := disk.SyncDir(); err != nil {
			panic(err)
		}
	}
	s.start(n)
	return n
}

func (s *Sim) walOptions() wal.Options {
	return wal.Options{SegmentSize: s.prof.SegmentSize, Mutations: s.opt.Mutations}
}

// start boots a server from its disk.
func (s *Sim) start(n *simNode) {
	w, err := wal.Open(n.disk, s.walOptions())
	if err != nil {
		s.violate("recovery", "n%d cannot recover its write-ahead log after a crash: %v", n.id, err)
		n.broken = true
		return
	}
	if st := w.Stats(); st.TornTail {
		s.tracef("n%d recovery cut a torn tail of %d bytes", n.id, st.TornBytes)
	}
	id := n.id
	inc := n.inc
	nd, err := node.New(node.Config{
		ID:               n.id,
		ElectionTick:     electionTicks,
		HeartbeatTick:    heartbeatTick,
		MaxEntriesPerMsg: s.prof.MaxEntriesPerMsg,
		SnapshotEvery:    s.prof.SnapshotEvery,
		SnapshotTrailing: s.prof.SnapshotEvery / 2,
		MaxSessions:      s.prof.MaxSessions,
		Mutations:        s.opt.Mutations,
		Trace: func(format string, args ...any) {
			s.tracef(format, args...)
		},
		OnApply: func(e raft.Entry) { s.onApply(id, e) },
		OnRestore: func(index, term uint64) {
			if inc == n.inc && n.nd != nil {
				s.stats.SnapshotsRecvd++
			}
			s.tracef("n%d restored snapshot index=%d term=%d", id, index, term)
		},
	}, w, transport{s, n.id}, n.rng.Fork(n.inc))
	if err != nil {
		s.violate("recovery", "n%d cannot start from its recovered state: %v", n.id, err)
		n.broken = true
		return
	}
	n.nd = nd
	r := nd.Raft()
	n.lastRole, n.lastTerm, n.lastVote, n.lastCommit = r.Role(), r.Term(), r.Vote(), r.Commit()
	s.at(Time(s.nemRng.Intn(int(n.tick)))+1, &event{kind: evTick, node: n.id, gen: n.inc})
}

func (s *Sim) node(id raft.NodeID) *simNode {
	if id == raft.None || int(id) > len(s.nodes) {
		return nil
	}
	return s.nodes[id-1]
}

// onNode runs f against a live server and then completes the batch with
// Ready, delivering its responses. A simulated crash inside either is the
// server dying mid-operation; any other panic is an assertion in the server
// code firing, which is reported and also kills the server.
func (s *Sim) onNode(n *simNode, f func(*node.Node)) {
	defer func() {
		r := recover()
		if r == nil {
			return
		}
		if _, ok := r.(simdisk.Crashed); ok {
			s.stats.MidIOCrashes++
			s.crash(n, "crashed in the middle of disk I/O")
			return
		}
		stack := string(debug.Stack())
		if i := strings.Index(stack, "panic("); i >= 0 {
			stack = stack[i:]
		}
		s.violate("assertion", "n%d panicked: %v\n%s", n.id, r, stack)
		s.crash(n, "panicked")
	}()
	f(n.nd)
	for _, resp := range n.nd.Ready() {
		s.routeResponse(n, resp)
	}
	s.observe(n)
}

// observe checks election safety after every step of a server, and
// injects the faults that are tied to protocol transitions.
func (s *Sim) observe(n *simNode) {
	if n.nd == nil {
		return
	}
	r := n.nd.Raft()
	role, t, vote := r.Role(), r.Term(), r.Vote()
	if t > s.stats.MaxTerm {
		s.stats.MaxTerm = t
	}
	n.trimOutbox(s.now)
	commit := r.Commit()
	voted := vote != raft.None && vote != n.id && (vote != n.lastVote || t != n.lastTerm)
	elected := role == raft.Leader && (n.lastRole != raft.Leader || t != n.lastTerm)
	committed := role == raft.Leader && commit > n.lastCommit
	n.lastRole, n.lastTerm, n.lastVote, n.lastCommit = role, t, vote, commit
	if s.chaos && (voted || elected || committed) {
		p := s.prof.TransitionCrashPercent
		if committed && !elected {
			// A leader commits all the time; crash at a fraction of
			// those moments.
			p /= 8
		}
		if p > 0 && s.nemRng.Intn(100) < p {
			// Usually at once, before what was just sent has left the
			// machine; sometimes a little later.
			d := Time(0)
			if s.nemRng.Chance(1, 2) {
				d = Time(s.nemRng.Intn(int(3 * Millisecond)))
			}
			s.at(d, &event{kind: evCrash, node: n.id, gen: n.inc})
		}
		if elected && s.prof.ReconfigureOnElection && s.prof.MembershipEvery > 0 && s.nemRng.Chance(1, 3) {
			s.at(Time(s.nemRng.Intn(int(2*Millisecond))), &event{kind: evNemesis, nem: nemReconfigure})
		}
	}
	if role != raft.Leader {
		return
	}
	switch prev, ok := s.leaders[t]; {
	case !ok:
		s.leaders[t] = n.id
	case prev != n.id:
		s.violate("election-safety", "n%d and n%d are both leader of term %d", prev, n.id, t)
		s.leaders[t] = n.id
	}
}

// onApply checks state machine safety: every server applies the same entry
// at the same index.
func (s *Sim) onApply(id raft.NodeID, e raft.Entry) {
	h := fnv.New64a()
	h.Write([]byte{byte(e.Type)})
	h.Write(e.Data)
	got := entryID{term: e.Term, sum: h.Sum64(), by: id}
	want, ok := s.committed[e.Index]
	if !ok {
		s.committed[e.Index] = got
		return
	}
	if want.term != got.term || want.sum != got.sum {
		s.violate("state-machine-safety", "n%d applied entry %d of term %d (sum %x) but n%d applied entry %d of term %d (sum %x)",
			id, e.Index, e.Term, got.sum, want.by, e.Index, want.term, want.sum)
	}
}

func (s *Sim) onTick(e *event) {
	n := s.node(e.node)
	if n.nd == nil || e.gen != n.inc {
		return
	}
	if s.now < n.pausedUntil {
		// A paused process does not see its clock advance; the ticks it
		// missed are not replayed, like a stopped VM.
		s.at(n.pausedUntil-s.now+n.tick, &event{kind: evTick, node: n.id, gen: n.inc})
		return
	}
	s.onNode(n, func(nd *node.Node) { nd.Tick() })
	if n.nd != nil && e.gen == n.inc {
		s.at(n.tick, &event{kind: evTick, node: n.id, gen: n.inc})
	}
}

func (s *Sim) onDeliver(e *event) {
	n := s.node(e.node)
	if n == nil || n.nd == nil {
		return
	}
	if s.now < n.pausedUntil {
		e.at = n.pausedUntil
		s.schedule(e)
		return
	}
	m, err := raft.DecodeMessage(e.msg)
	if err != nil {
		panic(fmt.Sprintf("sim: undecodable message: %v", err))
	}
	s.onNode(n, func(nd *node.Node) { nd.Step(m) })
}

func (s *Sim) crash(n *simNode, why string) {
	if n.nd == nil {
		return
	}
	s.stats.Crashes++
	n.nd = nil
	n.inc++
	n.disk.Crash()
	if lost := s.loseSendBuffer(n, Time(s.nemRng.Intn(int(sendWindow)+1))); lost > 0 {
		s.tracef("n%d lost %d unsent messages", n.id, lost)
	}
	n.pausedUntil = 0
	s.tracef("n%d CRASH (%s)", n.id, why)
	if !n.retired && !n.broken {
		s.scheduleRestart(n)
	}
}

// onCrash is a crash scheduled at a protocol transition: the server dies a
// moment later, possibly in the middle of its next disk write, and comes
// back quickly.
func (s *Sim) onCrash(e *event) {
	n := s.node(e.node)
	if n.nd == nil || e.gen != n.inc || !s.chaos {
		return
	}
	n.bounce = true
	if s.nemRng.Intn(100) < s.prof.MidIOPercent {
		if !n.disk.Armed() {
			n.disk.Arm(1 + s.nemRng.Intn(3))
		}
		return
	}
	s.crash(n, "killed right after a transition")
}

func (s *Sim) onRestart(e *event) {
	n := s.node(e.node)
	if n.nd != nil || n.retired || n.broken || e.gen != n.inc {
		return
	}
	s.stats.Restarts++
	s.tracef("n%d restart", n.id)
	s.start(n)
	s.observe(n)
}

// leader returns a live server that believes it leads the highest term, or
// nil.
func (s *Sim) leader() *simNode {
	var best *simNode
	for _, n := range s.nodes {
		if n.nd == nil || n.nd.Raft().Role() != raft.Leader {
			continue
		}
		if best == nil || n.nd.Raft().Term() > best.nd.Raft().Term() {
			best = n
		}
	}
	return best
}
