package raft

import (
	"fmt"
	"sort"

	"github.com/useless-husband/conclave/internal/mutation"
)

// Config holds the parameters of one Raft server.
type Config struct {
	// ID is this server's identity. It must be non-zero and must never be
	// reused by a server with different stable storage.
	ID NodeID
	// ElectionTick is the number of ticks a follower waits without hearing
	// from a leader before it starts an election. The actual timeout is
	// drawn uniformly from [ElectionTick, 2*ElectionTick).
	ElectionTick int
	// HeartbeatTick is the number of ticks between leader heartbeats. It
	// must be smaller than ElectionTick.
	HeartbeatTick int
	// MaxEntriesPerMsg bounds the entries carried by one append message.
	MaxEntriesPerMsg int
	// MaxBytesPerMsg bounds the entry payload of one append message. At
	// least one entry is always sent.
	MaxBytesPerMsg int
	// MaxInflight bounds the number of unacknowledged append messages the
	// leader keeps in flight to one follower.
	MaxInflight int
	// SnapshotTrailing is the number of already-snapshotted entries kept
	// in memory so that a slightly lagging follower can still be caught up
	// from the log instead of by snapshot.
	SnapshotTrailing uint64
	// Mutations selects injected bugs. It is always empty outside tests.
	Mutations mutation.Set
	// Trace, if set, receives a line for every role or term change. It is
	// used by the simulator to build readable failure traces.
	Trace func(format string, args ...any)
}

func (c *Config) validate() error {
	switch {
	case c.ID == None:
		return fmt.Errorf("raft: ID must be non-zero")
	case c.HeartbeatTick <= 0:
		return fmt.Errorf("raft: HeartbeatTick must be positive")
	case c.ElectionTick <= c.HeartbeatTick:
		return fmt.Errorf("raft: ElectionTick must exceed HeartbeatTick")
	}
	if c.MaxEntriesPerMsg <= 0 {
		c.MaxEntriesPerMsg = 256
	}
	if c.MaxBytesPerMsg <= 0 {
		c.MaxBytesPerMsg = 1 << 20
	}
	if c.MaxInflight <= 0 {
		c.MaxInflight = 64
	}
	return nil
}

// ReadState reports that a read registered with ReadIndex is linearizable
// once the state machine has applied the log up to Index.
type ReadState struct {
	Context uint64
	Index   uint64
}

// Output is what one Flush hands back to the driver.
type Output struct {
	// Snapshot, if non-nil, replaces the state machine before Committed is
	// applied.
	Snapshot *Snapshot
	// Committed holds newly committed entries in log order. Each entry is
	// delivered exactly once per process lifetime.
	Committed []Entry
	// ReadStates lists reads that have been confirmed.
	ReadStates []ReadState
	// ReadsAborted lists reads that will never be confirmed because this
	// server stopped being leader.
	ReadsAborted []uint64
}

// Status is a point-in-time view of a server, for monitoring and tests.
type Status struct {
	ID         NodeID
	Role       Role
	Term       uint64
	Vote       NodeID
	Leader     NodeID
	Commit     uint64
	Applied    uint64
	FirstIndex uint64
	LastIndex  uint64
	LastTerm   uint64
	SnapIndex  uint64
	Conf       Membership
	ConfIndex  uint64
}

type progressState uint8

const (
	// stateProbe: the follower's log position is unknown; send one append
	// at a time and wait for the answer.
	stateProbe progressState = iota
	// stateReplicate: the follower is following; appends are pipelined.
	stateReplicate
	// stateSnapshot: a snapshot is on its way; appends are paused.
	stateSnapshot
)

// progress is the leader's view of one follower.
type progress struct {
	match uint64 // highest index known to be durably stored on the follower
	next  uint64 // next index to send
	state progressState

	probeSent       bool   // stateProbe: an append is outstanding
	inflight        int    // stateReplicate: unacknowledged appends
	pendingSnapshot uint64 // stateSnapshot: index of the snapshot sent
	snapshotElapsed int    // stateSnapshot: ticks since it was sent

	recentActive bool   // heard from since the last quorum check
	readAck      uint64 // highest ReadIndex sequence acknowledged
}

func (p *progress) becomeProbe() {
	p.state = stateProbe
	p.probeSent = false
	p.inflight = 0
	p.pendingSnapshot = 0
	p.next = p.match + 1
}

func (p *progress) becomeReplicate() {
	p.state = stateReplicate
	p.probeSent = false
	p.inflight = 0
	p.pendingSnapshot = 0
	p.next = p.match + 1
}

type confAt struct {
	index uint64
	conf  Membership
}

type pendingRead struct {
	seq   uint64
	ctx   uint64
	index uint64
}

// Raft is one server's consensus state. It is not safe for concurrent use;
// the driver must serialize all calls.
type Raft struct {
	cfg Config
	id  NodeID
	st  Storage
	tr  Transport
	rng Rand
	mut mutation.Set

	// Persistent state (Raft Figure 2), mirrored from storage.
	term uint64
	vote NodeID
	log  *raftLog

	commit    uint64
	applied   uint64 // highest index handed out in Output.Committed
	snapIndex uint64 // index of the latest snapshot in storage
	snapTerm  uint64

	// confs[0] is the membership at the compaction point; each later
	// element is a membership change still present in the log. The last
	// element is the membership in force: a server uses the latest
	// configuration in its log whether or not it is committed.
	confs  []confAt
	voters []NodeID

	role Role
	lead NodeID

	electionElapsed  int
	heartbeatElapsed int
	randTimeout      int

	votes map[NodeID]bool

	// Leader state.
	prs             map[NodeID]*progress
	transferee      NodeID
	transferElapsed int
	readSeq         uint64
	reads           []pendingRead
	bcastPending    bool

	// Effects accumulated since the last Flush.
	needSync     bool
	pre          []Message // may be sent before the local log is durable
	post         []Message // must wait for the local log to be durable
	readStates   []ReadState
	readsAborted []uint64
	pendingSnap  *Snapshot

	scratch []uint64
}

// New creates a server from the state in st. A server with empty storage
// and no membership stays passive until a leader contacts it.
func New(cfg Config, st Storage, tr Transport, rng Rand) (*Raft, error) {
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	hs, snap, ents, err := st.InitialState()
	if err != nil {
		return nil, fmt.Errorf("raft: load state: %w", err)
	}
	log, err := newLog(snap.Index, snap.Term, ents)
	if err != nil {
		return nil, err
	}
	r := &Raft{
		cfg: cfg, id: cfg.ID, st: st, tr: tr, rng: rng, mut: cfg.Mutations,
		term: hs.Term, vote: hs.Vote, log: log,
		commit: snap.Index, applied: snap.Index,
		snapIndex: snap.Index, snapTerm: snap.Term,
		confs: []confAt{{index: snap.Index, conf: snap.Conf.Clone()}},
	}
	for _, e := range ents {
		if e.Type == EntryConfChange {
			r.applyConfEntry(e)
		}
	}
	r.setConf()
	r.resetVolatile()
	r.role = Follower
	return r, nil
}

func (r *Raft) tracef(format string, args ...any) {
	if r.cfg.Trace != nil {
		r.cfg.Trace(format, args...)
	}
}

func (r *Raft) must(err error) {
	if err != nil {
		panic(&StorageError{Err: err})
	}
}

// Status returns a snapshot of the server's state.
func (r *Raft) Status() Status {
	return Status{
		ID: r.id, Role: r.role, Term: r.term, Vote: r.vote, Leader: r.lead,
		Commit: r.commit, Applied: r.applied,
		FirstIndex: r.log.firstIndex(), LastIndex: r.log.lastIndex(), LastTerm: r.log.lastTerm(),
		SnapIndex: r.snapIndex,
		Conf:      r.conf().Clone(),
		ConfIndex: r.confs[len(r.confs)-1].index,
	}
}

// Role returns the current role.
func (r *Raft) Role() Role { return r.role }

// Term returns the current term.
func (r *Raft) Term() uint64 { return r.term }

// Leader returns the server this one believes is leader, or None.
func (r *Raft) Leader() NodeID { return r.lead }

// Vote returns the candidate this server voted for in the current term.
func (r *Raft) Vote() NodeID { return r.vote }

// Commit returns the commit index.
func (r *Raft) Commit() uint64 { return r.commit }

// Membership returns the membership currently in force.
func (r *Raft) Membership() Membership { return r.conf() }

// ---------------------------------------------------------------------------
// Membership

func (r *Raft) conf() Membership { return r.confs[len(r.confs)-1].conf }

func (r *Raft) isVoter(id NodeID) bool { return r.conf().Contains(id) }

func (r *Raft) quorum() int { return len(r.voters)/2 + 1 }

// promotable reports whether this server may stand for election: only
// members of the latest configuration in the server's own log campaign.
func (r *Raft) promotable() bool { return r.isVoter(r.id) }

// confAtIndex returns the membership in force at log index i.
func (r *Raft) confAtIndex(i uint64) Membership {
	c := r.confs[0].conf
	for _, ca := range r.confs[1:] {
		if ca.index > i {
			break
		}
		c = ca.conf
	}
	return c
}

func (r *Raft) applyConfEntry(e Entry) {
	conf, err := DecodeConf(e.Data)
	if err != nil {
		// A malformed change is ignored identically on every server.
		return
	}
	r.confs = append(r.confs, confAt{index: e.Index, conf: conf})
}

// truncateConfs undoes membership changes at or after index, which are
// about to be removed from the log.
func (r *Raft) truncateConfs(index uint64) {
	n := len(r.confs)
	for n > 1 && r.confs[n-1].index >= index {
		n--
	}
	if n != len(r.confs) {
		r.confs = r.confs[:n]
		r.setConf()
	}
}

// foldConfs merges membership changes at or before index into confs[0].
func (r *Raft) foldConfs(index uint64) {
	n := 0
	for n+1 < len(r.confs) && r.confs[n+1].index <= index {
		n++
	}
	if n > 0 {
		r.confs = append([]confAt(nil), r.confs[n:]...)
	}
}

// setConf recomputes everything derived from the membership in force.
func (r *Raft) setConf() {
	r.voters = r.conf().IDs()
	if r.role != Leader {
		return
	}
	for _, id := range r.voters {
		if r.prs[id] == nil {
			r.prs[id] = &progress{next: r.log.lastIndex() + 1, recentActive: true}
		}
	}
	for id := range r.prs {
		if !r.isVoter(id) {
			delete(r.prs, id)
		}
	}
	if r.transferee != None && !r.isVoter(r.transferee) {
		r.transferee = None
	}
}

// ---------------------------------------------------------------------------
// Role changes

func (r *Raft) persistHardState() {
	r.must(r.st.SaveHardState(HardState{Term: r.term, Vote: r.vote}))
	r.needSync = true
}

func (r *Raft) resetVolatile() {
	r.electionElapsed = 0
	r.heartbeatElapsed = 0
	r.randTimeout = r.cfg.ElectionTick + r.rng.Intn(r.cfg.ElectionTick)
	r.votes = nil
	r.transferee = None
	r.lead = None
}

func (r *Raft) becomeFollower(term uint64, lead NodeID) {
	if r.role == Leader {
		for _, rd := range r.reads {
			r.readsAborted = append(r.readsAborted, rd.ctx)
		}
		r.reads = nil
		r.prs = nil
	}
	if term != r.term {
		r.term = term
		r.vote = None
		r.persistHardState()
	}
	r.resetVolatile()
	r.role = Follower
	r.lead = lead
	r.tracef("n%d follower term=%d lead=n%d", r.id, r.term, lead)
}

func (r *Raft) becomePreCandidate() {
	// A pre-candidate changes neither its term nor its vote: pre-vote
	// exists so that a server that cannot win does not disturb the others.
	r.resetVolatile()
	r.role = PreCandidate
	r.votes = map[NodeID]bool{r.id: true}
	r.tracef("n%d pre-candidate term=%d", r.id, r.term)
}

func (r *Raft) becomeCandidate() {
	r.resetVolatile()
	r.term++
	r.vote = r.id
	r.persistHardState()
	r.role = Candidate
	r.votes = map[NodeID]bool{r.id: true}
	r.tracef("n%d candidate term=%d", r.id, r.term)
}

func (r *Raft) becomeLeader() {
	r.resetVolatile()
	r.role = Leader
	r.lead = r.id
	r.prs = make(map[NodeID]*progress, len(r.voters))
	last := r.log.lastIndex()
	for _, id := range r.voters {
		r.prs[id] = &progress{next: last + 1}
	}
	if pr := r.prs[r.id]; pr != nil {
		pr.match = r.log.stable
		pr.recentActive = true
	}
	r.readSeq = 0
	r.reads = nil
	r.tracef("n%d leader term=%d last=%d", r.id, r.term, last)
	// A new leader cannot tell which of its entries from earlier terms are
	// committed until it commits one of its own (Raft §5.4.2), so it
	// appends a no-op at once.
	r.appendLeader(Entry{Type: EntryNoop})
	r.bcastAppend()
}

type campaignKind uint8

const (
	campaignPreVote campaignKind = iota
	campaignElection
	campaignTransfer
)

func (r *Raft) campaign(kind campaignKind) {
	var (
		typ  MsgType
		term uint64
	)
	if kind == campaignPreVote {
		r.becomePreCandidate()
		typ, term = MsgPreVote, r.term+1
	} else {
		r.becomeCandidate()
		typ, term = MsgVote, r.term
	}
	if granted, _ := r.tally(); granted >= r.quorum() {
		// Single-voter cluster.
		if kind == campaignPreVote {
			r.campaign(campaignElection)
		} else {
			r.becomeLeader()
		}
		return
	}
	for _, id := range r.voters {
		if id == r.id {
			continue
		}
		r.send(Message{
			Type: typ, To: id, Term: term,
			Index: r.log.lastIndex(), LogTerm: r.log.lastTerm(),
			Transfer: kind == campaignTransfer,
		})
	}
}

// tally counts the responses of the current (pre-)election among voters.
func (r *Raft) tally() (granted, rejected int) {
	for _, id := range r.voters {
		v, ok := r.votes[id]
		switch {
		case !ok:
		case v:
			granted++
		default:
			rejected++
		}
	}
	return granted, rejected
}

// ---------------------------------------------------------------------------
// Time

// Tick advances the server's logical clock by one tick.
func (r *Raft) Tick() {
	if r.role == Leader {
		r.tickLeader()
		return
	}
	r.electionElapsed++
	if r.electionElapsed >= r.randTimeout && r.promotable() {
		r.campaign(campaignPreVote)
	}
}

func (r *Raft) tickLeader() {
	r.heartbeatElapsed++
	r.electionElapsed++
	if r.transferee != None {
		r.transferElapsed++
		if r.transferElapsed >= r.cfg.ElectionTick {
			r.tracef("n%d abort transfer to n%d", r.id, r.transferee)
			r.transferee = None
		}
	}
	if r.electionElapsed >= r.cfg.ElectionTick {
		r.electionElapsed = 0
		// Check quorum: a leader that has not heard from a majority for a
		// whole election timeout steps down. Followers refuse to vote
		// while they hear from a leader, so without this rule a leader
		// that can send but not receive would block elections forever.
		active := 0
		for _, id := range r.voters {
			pr := r.prs[id]
			if id == r.id || pr.recentActive {
				active++
			}
			if id != r.id {
				pr.recentActive = false
			}
		}
		if active < r.quorum() {
			r.tracef("n%d lost quorum contact, stepping down", r.id)
			r.becomeFollower(r.term, None)
			return
		}
	}
	for _, id := range r.voters {
		pr := r.prs[id]
		if pr.state == stateSnapshot {
			pr.snapshotElapsed++
			if pr.snapshotElapsed >= 2*r.cfg.ElectionTick {
				// The snapshot or its acknowledgement was lost.
				pr.becomeProbe()
			}
		}
	}
	if r.heartbeatElapsed >= r.cfg.HeartbeatTick {
		r.heartbeatElapsed = 0
		r.bcastHeartbeat()
	}
}

// ---------------------------------------------------------------------------
// Messages

func (r *Raft) send(m Message) {
	m.From = r.id
	switch m.Type {
	case MsgApp, MsgHeartbeat, MsgSnap, MsgTimeoutNow:
		// Leader-to-follower traffic does not depend on anything this
		// server has yet to make durable: the leader counts its own copy
		// of an entry only after its own fsync (see Flush).
		r.pre = append(r.pre, m)
	default:
		// Votes and acknowledgements are promises. They leave only after
		// the state they promise is on disk.
		r.post = append(r.post, m)
	}
}

// Step feeds one incoming message to the server.
func (r *Raft) Step(m Message) {
	if m.To != r.id || m.From == r.id || m.From == None {
		return
	}
	switch {
	case m.Term > r.term:
		if m.Type == MsgVote || m.Type == MsgPreVote {
			// Leader stickiness: a server that has heard from a leader
			// within the minimum election timeout ignores vote requests,
			// so a server that was removed or partitioned away cannot
			// depose a working leader (Raft thesis §4.2.3). A transfer
			// initiated by the leader itself overrides it.
			if !m.Transfer && r.lead != None && r.electionElapsed < r.cfg.ElectionTick {
				return
			}
		}
		switch {
		case m.Type == MsgPreVote:
			// A pre-vote request never changes the receiver's term.
		case m.Type == MsgPreVoteResp && !m.Reject:
			// A granted pre-vote carries the term we asked about, which
			// is one ahead of ours until we actually campaign.
		case m.Type == MsgApp || m.Type == MsgHeartbeat || m.Type == MsgSnap:
			r.becomeFollower(m.Term, m.From)
		default:
			r.becomeFollower(m.Term, None)
		}
	case m.Term < r.term:
		switch m.Type {
		case MsgApp, MsgHeartbeat, MsgSnap:
			// Tell a stale leader about the newer term so it steps down.
			r.send(Message{Type: MsgAppResp, To: m.From, Term: r.term, Reject: true, Index: m.Index})
		case MsgPreVote:
			r.send(Message{Type: MsgPreVoteResp, To: m.From, Term: r.term, Reject: true})
		}
		return
	}

	switch m.Type {
	case MsgPreVote, MsgVote:
		r.handleVoteRequest(m)
		return
	}
	switch r.role {
	case Leader:
		r.stepLeader(m)
	case Candidate, PreCandidate:
		r.stepCandidate(m)
	default:
		r.stepFollower(m)
	}
}

func (r *Raft) handleVoteRequest(m Message) {
	respType := MsgVoteResp
	var canVote bool
	if m.Type == MsgPreVote {
		respType = MsgPreVoteResp
		canVote = m.Term > r.term
	} else {
		// m.Term == r.term here. One vote per term, first come first
		// served; repeating a vote already given is fine.
		canVote = r.vote == m.From || (r.vote == None && r.lead == None)
	}
	upToDate := r.log.isUpToDate(m.Index, m.LogTerm)
	if r.mut.Has(mutation.VoteWithoutLogCheck) {
		upToDate = true
	}
	if !canVote || !upToDate {
		r.send(Message{Type: respType, To: m.From, Term: r.term, Reject: true})
		return
	}
	if m.Type == MsgVote {
		r.vote = m.From
		r.electionElapsed = 0
		if !r.mut.Has(mutation.VoteNotPersisted) {
			r.persistHardState()
		}
		r.tracef("n%d votes for n%d term=%d", r.id, m.From, r.term)
	}
	r.send(Message{Type: respType, To: m.From, Term: m.Term})
}

func (r *Raft) stepFollower(m Message) {
	switch m.Type {
	case MsgApp:
		r.electionElapsed = 0
		r.lead = m.From
		r.handleAppend(m)
	case MsgHeartbeat:
		r.electionElapsed = 0
		r.lead = m.From
		r.handleHeartbeat(m)
	case MsgSnap:
		r.electionElapsed = 0
		r.lead = m.From
		r.handleSnapshot(m)
	case MsgTimeoutNow:
		if r.promotable() {
			r.tracef("n%d received TimeoutNow from n%d", r.id, m.From)
			r.campaign(campaignTransfer)
		}
	}
}

func (r *Raft) stepCandidate(m Message) {
	switch m.Type {
	case MsgApp, MsgHeartbeat, MsgSnap:
		// Someone else won this term.
		r.becomeFollower(m.Term, m.From)
		r.stepFollower(m)
	case MsgVoteResp:
		if r.role != Candidate {
			return
		}
		r.recordVote(m.From, !m.Reject)
	case MsgPreVoteResp:
		if r.role != PreCandidate {
			return
		}
		// Only answers to the pre-vote in progress count: a grant echoes
		// the term we proposed, a rejection carries the voter's term.
		if !m.Reject && m.Term != r.term+1 {
			return
		}
		r.recordVote(m.From, !m.Reject)
	}
}

func (r *Raft) recordVote(from NodeID, granted bool) {
	if _, dup := r.votes[from]; !dup {
		r.votes[from] = granted
	}
	g, rej := r.tally()
	switch {
	case g >= r.quorum():
		if r.role == PreCandidate {
			r.campaign(campaignElection)
		} else {
			r.becomeLeader()
		}
	case rej > len(r.voters)-r.quorum():
		r.becomeFollower(r.term, None)
	}
}

func (r *Raft) handleAppend(m Message) {
	if m.Index < r.commit {
		// Everything up to the commit index is known to match the leader.
		r.send(Message{Type: MsgAppResp, To: m.From, Term: r.term, Index: r.commit})
		return
	}
	if !r.log.matches(m.Index, m.LogTerm) {
		resp := Message{Type: MsgAppResp, To: m.From, Term: r.term, Reject: true, Index: m.Index}
		if m.Index > r.log.lastIndex() {
			resp.ConflictIndex = r.log.lastIndex() + 1
		} else {
			// Report the whole run of the conflicting term so the leader
			// can skip it in one step instead of one entry per round trip.
			t, _ := r.log.term(m.Index)
			resp.ConflictTerm = t
			resp.ConflictIndex = r.log.firstIndexOfTerm(m.Index, t)
		}
		r.send(resp)
		return
	}
	for i, e := range m.Entries {
		if r.log.matches(e.Index, e.Term) {
			continue
		}
		if e.Index <= r.commit {
			panic(fmt.Sprintf("raft: n%d asked to overwrite committed entry %d (commit %d) by n%d in term %d",
				r.id, e.Index, r.commit, m.From, m.Term))
		}
		r.appendFollower(m.Entries[i:])
		break
	}
	last := m.Index + uint64(len(m.Entries))
	// Only entries verified to match the leader may be committed here: the
	// log may still hold a stale suffix beyond `last`.
	r.commitTo(min(m.Commit, last))
	r.send(Message{Type: MsgAppResp, To: m.From, Term: r.term, Index: last})
}

func (r *Raft) appendFollower(ents []Entry) {
	first := ents[0].Index
	if first <= r.log.lastIndex() {
		r.truncateConfs(first)
	}
	r.log.truncateAndAppend(ents)
	r.must(r.st.Append(ents))
	r.needSync = true
	changed := false
	for _, e := range ents {
		if e.Type == EntryConfChange {
			r.applyConfEntry(e)
			changed = true
		}
	}
	if changed {
		r.setConf()
	}
}

func (r *Raft) commitTo(i uint64) {
	if i > r.commit {
		if i > r.log.lastIndex() {
			panic(fmt.Sprintf("raft: n%d commit %d beyond last index %d", r.id, i, r.log.lastIndex()))
		}
		r.commit = i
	}
}

func (r *Raft) handleHeartbeat(m Message) {
	// The leader caps Commit at what it knows this follower stores.
	r.commitTo(min(m.Commit, r.log.lastIndex()))
	r.send(Message{Type: MsgHeartbeatResp, To: m.From, Term: r.term, Context: m.Context})
}

func (r *Raft) handleSnapshot(m Message) {
	s := m.Snapshot
	if s == nil {
		return
	}
	if s.Index <= r.commit {
		r.send(Message{Type: MsgAppResp, To: m.From, Term: r.term, Index: r.commit})
		return
	}
	if r.log.matches(s.Index, s.Term) {
		// The log already contains everything the snapshot covers.
		r.commitTo(s.Index)
		r.send(Message{Type: MsgAppResp, To: m.From, Term: r.term, Index: s.Index})
		return
	}
	r.must(r.st.InstallSnapshot(*s))
	r.log.reset(s.Index, s.Term)
	r.commit, r.applied = s.Index, s.Index
	r.snapIndex, r.snapTerm = s.Index, s.Term
	r.confs = []confAt{{index: s.Index, conf: s.Conf.Clone()}}
	r.setConf()
	r.pendingSnap = s
	r.tracef("n%d installed snapshot index=%d term=%d", r.id, s.Index, s.Term)
	r.send(Message{Type: MsgAppResp, To: m.From, Term: r.term, Index: s.Index})
}

// ---------------------------------------------------------------------------
// Leader

func (r *Raft) stepLeader(m Message) {
	pr := r.prs[m.From]
	if pr == nil {
		return
	}
	switch m.Type {
	case MsgAppResp:
		pr.recentActive = true
		r.handleAppendResp(m, pr)
	case MsgHeartbeatResp:
		pr.recentActive = true
		switch pr.state {
		case stateProbe:
			pr.probeSent = false
		case stateReplicate:
			// A lost append would otherwise hold its window slot forever.
			if pr.inflight >= r.cfg.MaxInflight {
				pr.inflight--
			}
		}
		if m.Context > pr.readAck {
			pr.readAck = m.Context
			r.advanceReads()
		}
		if pr.match < r.log.lastIndex() {
			r.sendAppend(m.From)
		}
	}
}

func (r *Raft) handleAppendResp(m Message, pr *progress) {
	if m.Reject {
		switch pr.state {
		case stateReplicate:
			if m.Index <= pr.match {
				return // stale
			}
		case stateProbe:
			if m.Index != pr.next-1 {
				return // stale
			}
		default:
			return
		}
		// Fast backtracking (Raft §5.3, last paragraphs): skip the whole
		// conflicting term instead of retrying one index at a time.
		next := m.ConflictIndex
		if m.ConflictTerm != 0 {
			if i := r.log.lastIndexOfTerm(m.Index, m.ConflictTerm); i != 0 {
				next = i + 1
			}
		}
		pr.becomeProbe()
		pr.next = max(min(next, m.Index), pr.match+1)
		r.sendAppend(m.From)
		return
	}

	if m.Index > r.log.lastIndex() {
		return // cannot acknowledge what was never sent
	}
	if m.Index > pr.match {
		pr.match = m.Index
	}
	if pr.next < pr.match+1 {
		pr.next = pr.match + 1
	}
	switch pr.state {
	case stateProbe:
		pr.becomeReplicate()
	case stateSnapshot:
		if pr.match >= pr.pendingSnapshot {
			pr.becomeReplicate()
		}
	case stateReplicate:
		if pr.inflight > 0 {
			pr.inflight--
		}
	}
	if r.maybeCommit() {
		r.bcastAppend()
	} else {
		for pr.next <= r.log.lastIndex() && r.sendAppend(m.From) {
		}
	}
	if r.transferee == m.From && pr.match == r.log.lastIndex() {
		r.send(Message{Type: MsgTimeoutNow, To: m.From, Term: r.term})
	}
}

// sendAppend sends the next append (or a snapshot) to a follower and
// reports whether anything was sent.
func (r *Raft) sendAppend(to NodeID) bool {
	pr := r.prs[to]
	switch pr.state {
	case stateProbe:
		if pr.probeSent {
			return false
		}
	case stateReplicate:
		if pr.inflight >= r.cfg.MaxInflight {
			return false
		}
	case stateSnapshot:
		return false
	}
	prevTerm, ok := r.log.term(pr.next - 1)
	if !ok {
		r.sendSnapshot(to, pr)
		return true
	}
	hi := min(r.log.lastIndex(), pr.next+uint64(r.cfg.MaxEntriesPerMsg)-1)
	ents := r.log.slice(pr.next, hi)
	size := 0
	for i, e := range ents {
		size += len(e.Data) + 16
		if size > r.cfg.MaxBytesPerMsg && i > 0 {
			ents = ents[:i:i]
			break
		}
	}
	r.send(Message{
		Type: MsgApp, To: to, Term: r.term,
		Index: pr.next - 1, LogTerm: prevTerm,
		Entries: ents, Commit: r.commit,
	})
	switch pr.state {
	case stateReplicate:
		if len(ents) > 0 {
			pr.next += uint64(len(ents))
			pr.inflight++
		}
	case stateProbe:
		pr.probeSent = true
	}
	return true
}

func (r *Raft) sendSnapshot(to NodeID, pr *progress) {
	snap, err := r.st.Snapshot()
	r.must(err)
	if snap.Index < r.log.prevIndex {
		panic(fmt.Sprintf("raft: n%d snapshot %d older than compaction point %d", r.id, snap.Index, r.log.prevIndex))
	}
	pr.state = stateSnapshot
	pr.pendingSnapshot = snap.Index
	pr.snapshotElapsed = 0
	pr.probeSent = false
	pr.inflight = 0
	r.send(Message{Type: MsgSnap, To: to, Term: r.term, Snapshot: &snap})
}

func (r *Raft) bcastAppend() {
	for _, id := range r.voters {
		if id != r.id {
			r.sendAppend(id)
		}
	}
}

func (r *Raft) bcastHeartbeat() {
	for _, id := range r.voters {
		if id == r.id {
			continue
		}
		r.send(Message{
			Type: MsgHeartbeat, To: id, Term: r.term,
			// Never tell a follower to commit past what it is known to hold.
			Commit:  min(r.prs[id].match, r.commit),
			Context: r.readSeq,
		})
	}
}

// maybeCommit advances the commit index to the highest entry stored on a
// majority, and reports whether it moved.
func (r *Raft) maybeCommit() bool {
	if len(r.voters) == 0 {
		return false
	}
	ms := r.scratch[:0]
	for _, id := range r.voters {
		ms = append(ms, r.prs[id].match)
	}
	sort.Slice(ms, func(i, j int) bool { return ms[i] > ms[j] })
	q := ms[r.quorum()-1]
	r.scratch = ms
	if q <= r.commit {
		return false
	}
	t, ok := r.log.term(q)
	if !ok {
		return false
	}
	// Raft §5.4.2: a leader only counts replicas for entries of its own
	// term. An entry from an earlier term stored on a majority can still
	// be overwritten (Figure 8); it becomes committed indirectly when a
	// current-term entry after it commits.
	if t != r.term && !r.mut.Has(mutation.CommitPriorTermByCount) {
		return false
	}
	r.commit = q
	return true
}

// committedInTerm reports whether this leader has committed an entry of its
// own term, i.e. whether its commit index is known to be current.
func (r *Raft) committedInTerm() bool {
	t, ok := r.log.term(r.commit)
	return ok && t == r.term
}

func (r *Raft) appendLeader(e Entry) Entry {
	e.Index = r.log.lastIndex() + 1
	e.Term = r.term
	r.log.append(e)
	r.must(r.st.Append([]Entry{e}))
	r.needSync = true
	if e.Type == EntryConfChange {
		r.applyConfEntry(e)
		r.setConf()
	}
	r.bcastPending = true
	return e
}

// Propose appends a command to the log if this server is the leader. The
// command is committed if and only if an entry with the returned index and
// term is later delivered in Output.Committed.
func (r *Raft) Propose(data []byte) (index, term uint64, err error) {
	if r.role != Leader {
		return 0, 0, ErrNotLeader
	}
	if r.transferee != None {
		return 0, 0, ErrTransferring
	}
	e := r.appendLeader(Entry{Type: EntryNormal, Data: data})
	return e.Index, e.Term, nil
}

// ProposeConfChange appends a single-server membership change.
func (r *Raft) ProposeConfChange(cc ConfChange) (index, term uint64, err error) {
	if r.role != Leader {
		return 0, 0, ErrNotLeader
	}
	if r.transferee != None {
		return 0, 0, ErrTransferring
	}
	// One change at a time: the safety argument for single-server changes
	// needs the previous change to be committed first.
	if r.confs[len(r.confs)-1].index > r.commit {
		return 0, 0, ErrConfChangePending
	}
	// ...and needs the leader to have committed an entry of its own term,
	// otherwise two leaders can each commit a different change on
	// non-overlapping majorities (Ongaro, raft-dev, 2015).
	if !r.committedInTerm() && !r.mut.Has(mutation.ConfChangeBeforeTermCommit) {
		return 0, 0, ErrNotReady
	}
	cur := r.conf()
	switch cc.Type {
	case AddNode:
		if cc.Node == None || cur.Contains(cc.Node) {
			return 0, 0, ErrBadConfChange
		}
	case RemoveNode:
		if !cur.Contains(cc.Node) || len(cur.Members) == 1 {
			return 0, 0, ErrBadConfChange
		}
	default:
		return 0, 0, ErrBadConfChange
	}
	e := r.appendLeader(Entry{Type: EntryConfChange, Data: EncodeConf(cur.apply(cc))})
	r.tracef("n%d proposes conf change %v n%d at %d", r.id, cc.Type, cc.Node, e.Index)
	return e.Index, e.Term, nil
}

// ReadIndex registers a linearizable read. A ReadState carrying ctx is
// delivered by a later Flush once a majority has confirmed that this server
// was still leader after the call; the read may then be served from the
// state machine as soon as it has applied ReadState.Index.
func (r *Raft) ReadIndex(ctx uint64) error {
	if r.role != Leader {
		return ErrNotLeader
	}
	if r.mut.Has(mutation.ReadWithoutQuorum) {
		r.readStates = append(r.readStates, ReadState{Context: ctx, Index: r.commit})
		return nil
	}
	// Until the leader commits in its own term its commit index may lag
	// behind writes acknowledged by a previous leader.
	if !r.committedInTerm() {
		return ErrNotReady
	}
	r.readSeq++
	r.reads = append(r.reads, pendingRead{seq: r.readSeq, ctx: ctx, index: r.commit})
	if r.isVoter(r.id) && r.quorum() == 1 {
		r.advanceReads()
		return nil
	}
	r.bcastHeartbeat()
	return nil
}

// advanceReads releases every pending read whose heartbeat round has been
// acknowledged by a majority.
func (r *Raft) advanceReads() {
	if len(r.reads) == 0 || len(r.voters) == 0 {
		return
	}
	acks := r.scratch[:0]
	for _, id := range r.voters {
		if id == r.id {
			acks = append(acks, r.readSeq)
		} else {
			acks = append(acks, r.prs[id].readAck)
		}
	}
	sort.Slice(acks, func(i, j int) bool { return acks[i] > acks[j] })
	q := acks[r.quorum()-1]
	r.scratch = acks
	n := 0
	for n < len(r.reads) && r.reads[n].seq <= q {
		r.readStates = append(r.readStates, ReadState{Context: r.reads[n].ctx, Index: r.reads[n].index})
		n++
	}
	r.reads = r.reads[n:]
}

// TransferLeadership asks the leader to hand over to another voter. The
// leader stops accepting proposals, brings the target up to date and tells
// it to start an election immediately. If the target has not taken over
// within an election timeout the transfer is abandoned.
func (r *Raft) TransferLeadership(to NodeID) error {
	if r.role != Leader {
		return ErrNotLeader
	}
	if to == r.id || !r.isVoter(to) {
		return ErrBadConfChange
	}
	if r.transferee == to {
		return nil
	}
	r.transferee = to
	r.transferElapsed = 0
	r.tracef("n%d transferring leadership to n%d", r.id, to)
	if pr := r.prs[to]; pr.match == r.log.lastIndex() {
		r.send(Message{Type: MsgTimeoutNow, To: to, Term: r.term})
	} else {
		r.sendAppend(to)
	}
	return nil
}

// Compact records a snapshot of the state machine taken after applying the
// log up to index, and lets the log before it be discarded. data must be the
// state machine's image at exactly that point.
func (r *Raft) Compact(index uint64, data []byte) error {
	if index <= r.snapIndex {
		return nil
	}
	if index > r.applied {
		return fmt.Errorf("raft: compact index %d beyond applied %d", index, r.applied)
	}
	term, ok := r.log.term(index)
	if !ok {
		return fmt.Errorf("raft: compact index %d not in log", index)
	}
	snap := Snapshot{Index: index, Term: term, Conf: r.confAtIndex(index).Clone(), Data: data}
	r.must(r.st.SaveSnapshot(snap))
	r.snapIndex, r.snapTerm = index, term
	if index > r.cfg.SnapshotTrailing {
		if to := index - r.cfg.SnapshotTrailing; to > r.log.prevIndex {
			r.log.compactTo(to)
			r.foldConfs(to)
		}
	}
	return nil
}

// ---------------------------------------------------------------------------
// Flush

func (r *Raft) deliver(msgs *[]Message) {
	for i := range *msgs {
		r.tr.Send((*msgs)[i])
		(*msgs)[i] = Message{}
	}
	*msgs = (*msgs)[:0]
}

// afterDurable runs the effects that are only allowed once the local log
// is on disk: the leader counts its own copy, and promises go out.
func (r *Raft) afterDurable() {
	if r.role == Leader {
		if pr := r.prs[r.id]; pr != nil && pr.match < r.log.lastIndex() {
			pr.match = r.log.lastIndex()
		}
		if r.maybeCommit() {
			r.bcastAppend()
		}
	}
	r.deliver(&r.post)
	r.deliver(&r.pre)
}

// Flush completes a batch of Tick/Step/Propose calls. It sends what may be
// sent early, makes the batch durable, sends the rest, and returns what the
// driver must now apply. The ordering inside this function is the
// durability contract of the whole system:
//
//  1. append messages leave (the leader does not wait for its own disk);
//  2. one fsync covers every log and hard-state write of the batch;
//  3. only then do votes and acknowledgements leave, and only then does a
//     leader count itself towards a majority.
func (r *Raft) Flush() Output {
	if r.role == Leader && r.bcastPending {
		r.bcastAppend()
	}
	r.bcastPending = false

	r.deliver(&r.pre)
	early := r.mut.Has(mutation.AckBeforeFsync)
	if early {
		r.afterDurable()
	}
	if r.needSync {
		r.must(r.st.Sync())
		r.needSync = false
	}
	r.log.stable = r.log.lastIndex()
	if !early {
		r.afterDurable()
	}

	// A leader that has committed its own removal has nothing left to do.
	if r.role == Leader && !r.isVoter(r.id) && r.confs[len(r.confs)-1].index <= r.commit {
		r.handOff()
		r.becomeFollower(r.term, None)
		r.deliver(&r.pre)
	}

	var out Output
	out.Snapshot, r.pendingSnap = r.pendingSnap, nil
	if r.commit > r.applied {
		out.Committed = r.log.slice(r.applied+1, r.commit)
		r.applied = r.commit
	}
	out.ReadStates, r.readStates = r.readStates, nil
	out.ReadsAborted, r.readsAborted = r.readsAborted, nil
	return out
}

// handOff tells the most up-to-date voter to start an election at once, so
// that a leader leaving the cluster does not cost an election timeout.
func (r *Raft) handOff() {
	var best NodeID
	var bestMatch uint64
	for _, id := range r.voters {
		if pr := r.prs[id]; id != r.id && (best == None || pr.match > bestMatch) {
			best, bestMatch = id, pr.match
		}
	}
	if best != None && bestMatch == r.log.lastIndex() {
		r.send(Message{Type: MsgTimeoutNow, To: best, Term: r.term})
	}
}
