// Package node joins the Raft core, the write-ahead log and the key-value
// state machine into one server, and turns client requests into proposals,
// ReadIndex rounds and responses.
//
// Like the core, a Node is a single-threaded state machine without
// goroutines, clocks or I/O of its own: the driver calls Tick, Step and
// Submit, then Ready, which makes the batch durable, sends the messages,
// applies what committed and returns the responses that are now decided.
// The production server (internal/server) drives it from one goroutine with
// a real clock, TCP and the file system; the simulator (internal/sim)
// drives exactly the same code from its event loop. Everything between a
// request arriving and its response leaving is therefore covered by the
// simulator's checks.
package node

import (
	"errors"
	"fmt"
	"sort"

	"github.com/useless-husband/conclave/internal/kv"
	"github.com/useless-husband/conclave/internal/mutation"
	"github.com/useless-husband/conclave/internal/raft"
)

// Config holds the parameters of one server.
type Config struct {
	ID               raft.NodeID
	ElectionTick     int
	HeartbeatTick    int
	MaxEntriesPerMsg int
	MaxInflight      int
	// SnapshotEvery is the number of applied entries after which the
	// state machine is snapshotted and the log compacted. Zero means
	// 10000.
	SnapshotEvery uint64
	// SnapshotTrailing is passed to raft.Config.
	SnapshotTrailing uint64
	// MaxSessions bounds the client sessions kept by the state machine.
	MaxSessions int
	// Mutations selects injected bugs. It is always empty outside tests.
	Mutations mutation.Set
	// Trace receives the Raft core's role and term changes.
	Trace func(format string, args ...any)
	// OnApply, if set, observes every entry applied to the state machine,
	// and OnRestore every snapshot restored. The simulator uses them to
	// check that all replicas apply the same entries.
	OnApply   func(e raft.Entry)
	OnRestore func(index, term uint64)
}

// Op is the kind of a request.
type Op uint8

const (
	// OpCommand runs a key-value command: Get through ReadIndex, anything
	// else through the log.
	OpCommand Op = iota
	// OpAddMember adds Member (with Meta) as a voter.
	OpAddMember
	// OpRemoveMember removes Member.
	OpRemoveMember
	// OpTransfer hands leadership to Member.
	OpTransfer
)

// Request is one client or administrative request.
type Request struct {
	// ID is chosen by the driver and echoed in the Response. IDs of
	// requests in flight on one node must be distinct.
	ID     uint64
	Op     Op
	Cmd    kv.Command
	Member raft.NodeID
	Meta   []byte
}

// Status says whether and how a request was executed.
type Status uint8

const (
	// StatusOK: the request was executed; Result holds its outcome.
	StatusOK Status = iota
	// StatusNotLeader: not executed; Leader names the leader if known.
	StatusNotLeader
	// StatusRetry: not executed; the leader cannot serve it yet (it has
	// not committed an entry of its term, or a membership change or
	// leadership transfer is in progress).
	StatusRetry
	// StatusUnknown: this server stopped being leader while the request
	// was in its log. It may still take effect. A client retries a write
	// with the same session and sequence number, which makes it take
	// effect at most once.
	StatusUnknown
	// StatusRejected: not executed and never will be.
	StatusRejected
)

var statusNames = [...]string{"ok", "not-leader", "retry", "unknown", "rejected"}

func (s Status) String() string {
	if int(s) >= len(statusNames) {
		return fmt.Sprintf("status(%d)", uint8(s))
	}
	return statusNames[s]
}

// Response answers one Request.
type Response struct {
	ID     uint64
	Status Status
	Leader raft.NodeID
	Result kv.Result
	// Index is the log index at which a write took effect, or the read
	// index a read was served at.
	Index uint64
}

type proposal struct {
	id   uint64
	term uint64
}

type waitingRead struct {
	id    uint64
	key   string
	index uint64
}

// Node is one server. It is not safe for concurrent use.
type Node struct {
	cfg Config
	r   *raft.Raft
	st  raft.Storage
	sm  *kv.Store

	applied   uint64
	snapIndex uint64

	// Proposals made while leader of term propTerm, by log index.
	propTerm  uint64
	proposals map[uint64]proposal
	// Reads registered with ReadIndex and not yet confirmed, by ID.
	reads map[uint64]string
	// Reads confirmed but waiting for the state machine to catch up.
	waiting []waitingRead

	out []Response
}

// New starts a server from the state in st.
func New(cfg Config, st raft.Storage, tr raft.Transport, rng raft.Rand) (*Node, error) {
	if cfg.SnapshotEvery == 0 {
		cfg.SnapshotEvery = 10000
	}
	r, err := raft.New(raft.Config{
		ID:               cfg.ID,
		ElectionTick:     cfg.ElectionTick,
		HeartbeatTick:    cfg.HeartbeatTick,
		MaxEntriesPerMsg: cfg.MaxEntriesPerMsg,
		MaxInflight:      cfg.MaxInflight,
		SnapshotTrailing: cfg.SnapshotTrailing,
		Mutations:        cfg.Mutations,
		Trace:            cfg.Trace,
	}, st, tr, rng)
	if err != nil {
		return nil, err
	}
	snap, err := st.Snapshot()
	if err != nil {
		return nil, fmt.Errorf("node: load snapshot: %w", err)
	}
	sm := kv.New(cfg.MaxSessions, cfg.Mutations)
	if err := sm.Restore(snap.Data); err != nil {
		return nil, fmt.Errorf("node: restore snapshot %d: %w", snap.Index, err)
	}
	if got := r.Status().Applied; got != snap.Index {
		return nil, fmt.Errorf("node: snapshot at %d but log starts after %d", snap.Index, got)
	}
	if cfg.OnRestore != nil && snap.Index > 0 {
		cfg.OnRestore(snap.Index, snap.Term)
	}
	return &Node{
		cfg: cfg, r: r, st: st, sm: sm,
		applied: snap.Index, snapIndex: snap.Index,
		proposals: map[uint64]proposal{},
		reads:     map[uint64]string{},
	}, nil
}

// Tick advances the logical clock by one tick.
func (n *Node) Tick() { n.r.Tick() }

// Step delivers a message from another server.
func (n *Node) Step(m raft.Message) { n.r.Step(m) }

// Raft exposes the consensus state for status reporting.
func (n *Node) Raft() *raft.Raft { return n.r }

// Store exposes the state machine for status reporting and tests. It must
// not be modified.
func (n *Node) Store() *kv.Store { return n.sm }

// Applied returns the index of the last entry applied to the state machine.
func (n *Node) Applied() uint64 { return n.applied }

// Pending returns the number of requests waiting for an outcome.
func (n *Node) Pending() int { return len(n.proposals) + len(n.reads) + len(n.waiting) }

func (n *Node) respond(r Response) { n.out = append(n.out, r) }

func (n *Node) refuse(id uint64, err error) {
	switch {
	case errors.Is(err, raft.ErrNotLeader):
		n.respond(Response{ID: id, Status: StatusNotLeader, Leader: n.r.Leader()})
	case errors.Is(err, raft.ErrBadConfChange):
		n.respond(Response{ID: id, Status: StatusRejected})
	default:
		n.respond(Response{ID: id, Status: StatusRetry, Leader: n.r.Leader()})
	}
}

// Submit accepts a request. Its response is returned by a later Ready (or
// the next one, if it can be decided at once).
func (n *Node) Submit(req Request) {
	n.checkLeadership()
	switch req.Op {
	case OpTransfer:
		if err := n.r.TransferLeadership(req.Member); err != nil {
			n.refuse(req.ID, err)
			return
		}
		n.respond(Response{ID: req.ID, Status: StatusOK})
	case OpAddMember, OpRemoveMember:
		cc := raft.ConfChange{Type: raft.AddNode, Node: req.Member, Meta: req.Meta}
		if req.Op == OpRemoveMember {
			cc.Type = raft.RemoveNode
		}
		index, term, err := n.r.ProposeConfChange(cc)
		if err != nil {
			n.refuse(req.ID, err)
			return
		}
		n.track(req.ID, index, term)
	case OpCommand:
		if req.Cmd.Kind == kv.Get {
			if err := n.r.ReadIndex(req.ID); err != nil {
				n.refuse(req.ID, err)
				return
			}
			n.reads[req.ID] = req.Cmd.Key
			return
		}
		index, term, err := n.r.Propose(req.Cmd.Encode())
		if err != nil {
			n.refuse(req.ID, err)
			return
		}
		n.track(req.ID, index, term)
	default:
		n.respond(Response{ID: req.ID, Status: StatusRejected})
	}
}

func (n *Node) track(id, index, term uint64) {
	n.propTerm = term
	n.proposals[index] = proposal{id: id, term: term}
}

// checkLeadership answers every proposal made in a term in which this node
// is no longer leader. Their entries may still commit under a new leader,
// so the outcome is unknown.
func (n *Node) checkLeadership() {
	if len(n.proposals) == 0 {
		return
	}
	if n.r.Role() == raft.Leader && n.r.Term() == n.propTerm {
		return
	}
	idx := make([]uint64, 0, len(n.proposals))
	for i := range n.proposals {
		idx = append(idx, i)
	}
	sort.Slice(idx, func(a, b int) bool { return idx[a] < idx[b] })
	for _, i := range idx {
		n.respond(Response{ID: n.proposals[i].id, Status: StatusUnknown, Leader: n.r.Leader()})
	}
	clear(n.proposals)
}

// Ready completes a batch: it makes it durable, sends messages, applies
// newly committed entries and passes every response decided so far to
// emit. Responses that do not depend on this batch being durable (writes
// already committed by a majority, confirmed reads) are emitted before the
// batch's fsync, so that a client's answer does not wait for the disk
// write of requests that arrived after it.
func (n *Node) Ready(emit func(Response)) {
	n.handle(n.r.Early())
	n.flushOut(emit)
	n.handle(n.r.Flush())
	n.checkLeadership()
	if n.applied-n.snapIndex >= n.cfg.SnapshotEvery {
		if err := n.r.Compact(n.applied, n.sm.Snapshot()); err != nil {
			panic(fmt.Sprintf("node n%d: compact at %d: %v", n.cfg.ID, n.applied, err))
		}
		n.snapIndex = n.applied
	}
	n.flushOut(emit)
}

func (n *Node) flushOut(emit func(Response)) {
	for _, r := range n.out {
		emit(r)
	}
	clear(n.out)
	n.out = n.out[:0]
}

func (n *Node) handle(out raft.Output) {
	if s := out.Snapshot; s != nil {
		if err := n.sm.Restore(s.Data); err != nil {
			// The leader sent a snapshot this server cannot read. Going
			// on would serve a wrong state; stopping is the only safe
			// choice.
			panic(fmt.Sprintf("node n%d: restore snapshot %d from leader: %v", n.cfg.ID, s.Index, err))
		}
		n.applied, n.snapIndex = s.Index, s.Index
		if n.cfg.OnRestore != nil {
			n.cfg.OnRestore(s.Index, s.Term)
		}
	}
	for _, e := range out.Committed {
		n.apply(e)
	}
	for _, rs := range out.ReadStates {
		key, ok := n.reads[rs.Context]
		if !ok {
			continue
		}
		delete(n.reads, rs.Context)
		n.waiting = append(n.waiting, waitingRead{id: rs.Context, key: key, index: rs.Index})
	}
	for _, ctx := range out.ReadsAborted {
		if _, ok := n.reads[ctx]; ok {
			delete(n.reads, ctx)
			n.respond(Response{ID: ctx, Status: StatusNotLeader, Leader: n.r.Leader()})
		}
	}
	n.serveReads()
}

func (n *Node) apply(e raft.Entry) {
	var res kv.Result
	if e.Type == raft.EntryNormal {
		res = n.sm.Apply(e.Index, e.Data)
	}
	n.applied = e.Index
	if n.cfg.OnApply != nil {
		n.cfg.OnApply(e)
	}
	p, ok := n.proposals[e.Index]
	if !ok {
		return
	}
	delete(n.proposals, e.Index)
	if p.term != e.Term {
		// A different entry committed at this index, so this proposal
		// can never commit.
		n.respond(Response{ID: p.id, Status: StatusNotLeader, Leader: n.r.Leader()})
		return
	}
	n.respond(Response{ID: p.id, Status: StatusOK, Result: res, Index: e.Index})
}

// serveReads answers the confirmed reads whose index has been applied.
func (n *Node) serveReads() {
	keep := n.waiting[:0]
	for _, w := range n.waiting {
		if w.index > n.applied {
			keep = append(keep, w)
			continue
		}
		res := kv.Result{Code: kv.NotFound}
		if v, ok := n.sm.Read(w.key); ok {
			res = kv.Result{Code: kv.OK, Found: true, Value: v}
		}
		n.respond(Response{ID: w.id, Status: StatusOK, Result: res, Index: w.index})
	}
	n.waiting = keep
}
