// Package sim is a deterministic discrete-event simulator for a conclave
// cluster.
//
// It runs the real server code (internal/node over internal/raft and the
// internal/wal write-ahead log) for N servers and a set of clients inside a
// single goroutine on virtual time. The network between them delays, drops,
// duplicates and reorders messages and can be partitioned, symmetrically or
// not; every server writes to a simulated disk (internal/simdisk) that loses
// unsynced data and can tear the last write when the server crashes; servers
// crash, restart, pause, join and leave. Every one of those decisions is
// drawn from one seed, so a seed is a complete, replayable description of a
// run.
//
// While it runs, the simulator checks that no two servers lead the same term
// and that every server applies the same entry at each log index. At the
// end it heals every fault, requires the cluster to make progress again,
// compares the replicas' state machines and checks the clients' history for
// linearizability with internal/lincheck.
package sim

import (
	"container/heap"
	"fmt"
	"hash"
	"hash/fnv"
	"sort"
	"strings"

	"github.com/useless-husband/conclave/internal/lincheck"
	"github.com/useless-husband/conclave/internal/mutation"
	"github.com/useless-husband/conclave/internal/node"
	"github.com/useless-husband/conclave/internal/prng"
	"github.com/useless-husband/conclave/internal/raft"
)

// Options configures one run.
type Options struct {
	Seed uint64
	// Profile, if non-nil, replaces the profile drawn from the seed.
	Profile *Profile
	// Mutations selects injected bugs (tests only).
	Mutations mutation.Set
	// Duration is the length of the faulty phase; Heal the length of the
	// fault-free phase after it. Zero means 20 s and 5 s of virtual time.
	Duration Time
	Heal     Time
	// TraceLines is the number of trace lines kept for the failure report
	// (the most recent ones). Zero means 400. Negative keeps all of them.
	TraceLines int
	// LinBudget bounds the linearizability search per key (steps). Zero
	// means 5,000,000.
	LinBudget int
}

// Violation is one broken safety or liveness property.
type Violation struct {
	At     Time
	Kind   string // "election-safety", "state-machine-safety", "linearizability", ...
	Detail string
}

// Stats counts what happened during a run.
type Stats struct {
	Events          int
	Messages        int
	Dropped         int
	Ops             int // completed client operations
	Unknown         int // writes whose outcome the client never learned
	Crashes         int
	MidIOCrashes    int
	TornWrites      int
	Restarts        int
	Partitions      int
	Pauses          int
	Elections       int // distinct (term, leader) pairs
	MaxTerm         uint64
	MembershipAdds  int
	MembershipRems  int
	Transfers       int
	SnapshotsRecvd  int
	MaxCommit       uint64
	LinSteps        int
	LinInconclusive bool
}

// Result is the outcome of one run.
type Result struct {
	Seed       uint64
	Profile    Profile
	Mutations  mutation.Set
	SimTime    Time
	Stats      Stats
	Violations []Violation
	// Digest is a hash of every event of the run; two runs with the same
	// seed and options must have equal digests.
	Digest uint64
	Trace  []string
	// TraceNote says which part of the run Trace covers.
	TraceNote string
	// History is the clients' history as given to the checker.
	History []lincheck.Op
	Lin     lincheck.Report
}

// Failed reports whether any property was violated.
func (r *Result) Failed() bool { return len(r.Violations) > 0 }

// SafetyViolated reports whether a safety property (anything but liveness)
// was violated.
func (r *Result) SafetyViolated() bool {
	for _, v := range r.Violations {
		if v.Kind != "liveness" {
			return true
		}
	}
	return false
}

// Kinds lists the distinct violation kinds.
func (r *Result) Kinds() []string {
	seen := map[string]bool{}
	var out []string
	for _, v := range r.Violations {
		if !seen[v.Kind] {
			seen[v.Kind] = true
			out = append(out, v.Kind)
		}
	}
	return out
}

// Report renders a failure report: seed, profile, violations, the end of
// the trace and how to replay the run.
func (r *Result) Report() string {
	var b strings.Builder
	fmt.Fprintf(&b, "seed %d: ", r.Seed)
	if !r.Failed() {
		fmt.Fprintf(&b, "ok (%v simulated, %d ops)\n", r.SimTime, r.Stats.Ops)
		return b.String()
	}
	fmt.Fprintf(&b, "FAILED with %d violation(s)\n", len(r.Violations))
	fmt.Fprintf(&b, "profile: %v\n", r.Profile)
	if !r.Mutations.Empty() {
		// Mutations cannot be selected from the binary, only from tests.
		fmt.Fprintf(&b, "mutations: %v\n", r.Mutations)
		fmt.Fprintf(&b, "replay:  go test ./internal/sim -run 'TestMutationsAreDetected/%v' -sim.report -v\n", r.Mutations)
	} else {
		fmt.Fprintf(&b, "replay:  go run ./cmd/conclave sim -seed %d -trace\n", r.Seed)
	}
	for i, v := range r.Violations {
		if i == 10 {
			fmt.Fprintf(&b, "... and %d more\n", len(r.Violations)-10)
			break
		}
		fmt.Fprintf(&b, "\n[%v] %s: %s\n", v.At, v.Kind, v.Detail)
	}
	if len(r.Trace) > 0 {
		if r.TraceNote != "" {
			fmt.Fprintf(&b, "\n%s:\n", r.TraceNote)
		} else {
			fmt.Fprintf(&b, "\ntrace (last %d lines):\n", len(r.Trace))
		}
		for _, l := range r.Trace {
			b.WriteString(l)
			b.WriteByte('\n')
		}
	}
	return b.String()
}

type evKind uint8

const (
	evTick evKind = iota + 1
	evDeliver
	evRequest
	evResponse
	evClient
	evNemesis
	evRestart
	evHeal
	evCrash
)

type event struct {
	at    Time
	seq   uint64
	kind  evKind
	node  raft.NodeID
	gen   uint64 // node incarnation or client timer generation
	cl    int    // client index
	msg   []byte // encoded raft message
	req   node.Request
	resp  node.Response
	nem   nemesis
	retry int
	// sent and cancelled: a message is cancelled when its sender crashes
	// before it left the sender's buffers.
	sent      Time
	cancelled bool
}

type eventQueue []*event

func (q eventQueue) Len() int { return len(q) }
func (q eventQueue) Less(i, j int) bool {
	if q[i].at != q[j].at {
		return q[i].at < q[j].at
	}
	return q[i].seq < q[j].seq
}
func (q eventQueue) Swap(i, j int) { q[i], q[j] = q[j], q[i] }
func (q *eventQueue) Push(x any)   { *q = append(*q, x.(*event)) }
func (q *eventQueue) Pop() any {
	old := *q
	e := old[len(old)-1]
	old[len(old)-1] = nil
	*q = old[:len(old)-1]
	return e
}

// Sim is one simulated cluster. It is single-threaded.
type Sim struct {
	opt  Options
	prof Profile
	now  Time
	seq  uint64
	q    eventQueue

	rng     *prng.Rand // root, only forked
	netRng  *prng.Rand
	nemRng  *prng.Rand
	nodes   []*simNode // by ID order; index i holds ID i+1
	clients []*client
	nextReq uint64
	routes  map[uint64]route // request ID -> who is waiting

	blocked map[[2]raft.NodeID]bool // directed links that drop everything
	chaos   bool                    // faulty phase
	healed  Time                    // when the healing phase started

	// Safety bookkeeping.
	leaders   map[uint64]raft.NodeID // term -> leader
	committed map[uint64]entryID     // index -> what was applied there

	hist    []lincheck.Op
	stats   Stats
	viol    []Violation
	trace   *tracer
	digest  hash.Hash64
	scratch []byte

	healOps   int // operations completed during the healing phase
	linReport lincheck.Report
	// The stretch of virtual time a linearizability violation is about.
	linFrom, linTo Time
}

type entryID struct {
	term uint64
	sum  uint64
	by   raft.NodeID
}

type route struct {
	client  int
	op      uint64 // client operation number
	admin   bool
	adminOp node.Op
	member  raft.NodeID
}

// Run executes one simulation.
func Run(opt Options) *Result {
	s := newSim(opt)
	s.run()
	r := s.result()
	if s.linFrom < s.linTo && opt.TraceLines >= 0 {
		// The checker only fails at the end of the run, long after the
		// events that matter left the trace buffer. Runs are
		// deterministic, so run the seed again with the whole trace and
		// keep the stretch around the operations the checker could not
		// place.
		o := opt
		o.TraceLines = -1
		again := newSim(o)
		again.run()
		if again.digest.Sum64() == r.Digest {
			r.Trace = again.trace.window(s.linFrom-300*Millisecond, s.linTo+20*Millisecond, 600)
			r.TraceNote = fmt.Sprintf("trace from %v to %v, around the operations the linearizability checker could not place",
				s.linFrom-300*Millisecond, s.linTo+20*Millisecond)
		}
	}
	return r
}

func newSim(opt Options) *Sim {
	if opt.Duration == 0 {
		opt.Duration = 20 * Second
	}
	if opt.Heal == 0 {
		opt.Heal = 5 * Second
	}
	if opt.TraceLines == 0 {
		opt.TraceLines = 400
	}
	if opt.LinBudget == 0 {
		opt.LinBudget = 5_000_000
	}
	root := prng.New(opt.Seed)
	s := &Sim{
		opt:       opt,
		rng:       root,
		netRng:    root.Fork(1),
		nemRng:    root.Fork(2),
		routes:    map[uint64]route{},
		blocked:   map[[2]raft.NodeID]bool{},
		leaders:   map[uint64]raft.NodeID{},
		committed: map[uint64]entryID{},
		digest:    fnv.New64a(),
		chaos:     true,
	}
	if opt.Profile != nil {
		s.prof = *opt.Profile
	} else {
		s.prof = RandomProfile(root.Fork(3))
	}
	s.trace = newTracer(opt.TraceLines)
	return s
}

func (s *Sim) schedule(e *event) {
	s.seq++
	e.seq = s.seq
	heap.Push(&s.q, e)
}

func (s *Sim) at(d Time, e *event) {
	e.at = s.now + d
	s.schedule(e)
}

func (s *Sim) tracef(format string, args ...any) {
	s.trace.add(s.now, fmt.Sprintf(format, args...))
}

func (s *Sim) violate(kind, format string, args ...any) {
	v := Violation{At: s.now, Kind: kind, Detail: fmt.Sprintf(format, args...)}
	s.viol = append(s.viol, v)
	s.tracef("VIOLATION %s: %s", kind, firstLine(v.Detail))
}

func firstLine(s string) string {
	if i := strings.IndexByte(s, '\n'); i >= 0 {
		return s[:i] + " ..."
	}
	return s
}

func (s *Sim) hashEvent(e *event) {
	b := s.scratch[:0]
	b = fmt.Appendf(b, "%d %d %d %d %d|", e.at, e.kind, e.node, e.cl, e.gen)
	b = append(b, e.msg...)
	if e.kind == evRequest {
		b = fmt.Appendf(b, "%d %d %s", e.req.ID, e.req.Op, e.req.Cmd.Encode())
	}
	if e.kind == evResponse {
		b = fmt.Appendf(b, "%d %d %d %d %q %d", e.resp.ID, e.resp.Status, e.resp.Leader, e.resp.Result.Code, e.resp.Result.Value, e.resp.Index)
	}
	s.digest.Write(b)
	s.scratch = b
}

func (s *Sim) run() {
	for i := 0; i < s.prof.Nodes; i++ {
		s.addNode(true)
	}
	for i := 0; i < s.prof.Clients; i++ {
		s.addClient(i)
	}
	s.startNemesis()
	s.at(s.opt.Duration, &event{kind: evHeal})
	end := s.opt.Duration + s.opt.Heal
	for s.q.Len() > 0 {
		e := heap.Pop(&s.q).(*event)
		if e.at > end {
			break
		}
		s.now = e.at
		if e.cancelled {
			continue
		}
		s.stats.Events++
		s.hashEvent(e)
		s.dispatch(e)
	}
	s.now = end
	s.finish()
}

func (s *Sim) dispatch(e *event) {
	switch e.kind {
	case evTick:
		s.onTick(e)
	case evDeliver:
		s.onDeliver(e)
	case evRequest:
		s.onRequest(e)
	case evResponse:
		s.onResponse(e)
	case evClient:
		s.onClientTimer(e)
	case evNemesis:
		s.onNemesis(e)
	case evRestart:
		s.onRestart(e)
	case evHeal:
		s.heal()
	case evCrash:
		s.onCrash(e)
	}
}

// finish stops the clients, checks the end state and the history.
func (s *Sim) finish() {
	s.tracef("end of run")
	for _, c := range s.clients {
		c.abandon(s)
	}
	if s.healOps == 0 {
		s.violate("liveness", "no client operation completed during the %v after all faults were healed", s.opt.Heal)
	}
	s.checkReplicas()
	rep := lincheck.CheckKV(s.hist, s.opt.LinBudget)
	for _, k := range rep.Keys {
		s.stats.LinSteps += k.Steps
	}
	s.stats.LinInconclusive = rep.Verdict == lincheck.Inconclusive
	if rep.Verdict == lincheck.Violation {
		var b strings.Builder
		for i, k := range rep.Failed() {
			if i == 0 || Time(k.From) < s.linFrom {
				s.linFrom = Time(k.From)
			}
			if Time(k.To) > s.linTo {
				s.linTo = Time(k.To)
			}
			fmt.Fprintf(&b, "key %q (%d operations) is not linearizable\n%s", k.Key, k.Ops, k.Explanation)
		}
		s.violate("linearizability", "%s", strings.TrimSuffix(b.String(), "\n"))
	}
	s.linReport = rep
}

// checkReplicas compares the state machines of all running members that
// have applied the same prefix of the log.
func (s *Sim) checkReplicas() {
	type snap struct {
		id     raft.NodeID
		digest uint64
	}
	byIndex := map[uint64][]snap{}
	for _, n := range s.nodes {
		if n.nd == nil {
			continue
		}
		a := n.nd.Applied()
		byIndex[a] = append(byIndex[a], snap{n.id, n.nd.Store().Digest()})
		if a > s.stats.MaxCommit {
			s.stats.MaxCommit = a
		}
	}
	idx := make([]uint64, 0, len(byIndex))
	for i := range byIndex {
		idx = append(idx, i)
	}
	sort.Slice(idx, func(a, b int) bool { return idx[a] < idx[b] })
	for _, i := range idx {
		g := byIndex[i]
		for _, x := range g[1:] {
			if x.digest != g[0].digest {
				s.violate("replica-divergence", "n%d and n%d have both applied up to index %d but their state machines differ",
					g[0].id, x.id, i)
			}
		}
	}
}

func (s *Sim) result() *Result {
	s.stats.Elections = 0
	for range s.leaders {
		s.stats.Elections++
	}
	for _, n := range s.nodes {
		s.stats.TornWrites += n.disk.TornWrites
	}
	return &Result{
		Seed:       s.opt.Seed,
		Profile:    s.prof,
		Mutations:  s.opt.Mutations,
		SimTime:    s.now,
		Stats:      s.stats,
		Violations: s.viol,
		Digest:     s.digest.Sum64(),
		Trace:      s.trace.lines(),
		History:    s.hist,
		Lin:        s.linReport,
	}
}
