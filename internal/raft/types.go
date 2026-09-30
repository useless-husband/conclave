// Package raft implements the Raft consensus algorithm as a deterministic
// state machine.
//
// The core in this package starts no goroutines, reads no clock and performs
// no I/O of its own. Everything that touches the outside world arrives
// through an injected interface:
//
//   - time is a sequence of Tick calls made by the driver;
//   - the network is a Transport that the core hands outgoing messages to,
//     and Step calls for incoming ones;
//   - stable storage is a Storage;
//   - randomness (election timeout jitter) is a Rand.
//
// Given the same sequence of calls and the same injected behaviour, a Raft
// value goes through exactly the same states. That property is what lets the
// simulator in internal/sim replay a failing schedule from its seed, and it
// is why the production server and the simulator can run the same code.
package raft

import (
	"errors"
	"fmt"
	"sort"
)

// NodeID identifies a server. Zero is reserved.
type NodeID uint64

// None is the absent NodeID.
const None NodeID = 0

// EntryType distinguishes what a log entry carries.
type EntryType uint8

const (
	// EntryNormal carries an opaque command for the replicated state machine.
	EntryNormal EntryType = iota
	// EntryNoop is appended by each new leader so that it can commit
	// entries from earlier terms and learn its commit index.
	EntryNoop
	// EntryConfChange carries the complete membership that takes effect
	// at this entry (see EncodeConf). Storing the whole membership rather
	// than the delta means a joining server learns the full configuration
	// from the one entry that adds it.
	EntryConfChange
)

// Entry is one slot of the replicated log.
type Entry struct {
	Index uint64
	Term  uint64
	Type  EntryType
	Data  []byte
}

// ConfChangeType is the kind of a single-server membership change.
type ConfChangeType uint8

const (
	// AddNode adds a voting member (or replaces the metadata of an
	// existing one).
	AddNode ConfChangeType = iota
	// RemoveNode removes a voting member.
	RemoveNode
)

// ConfChange adds or removes exactly one member. Changing one server at a
// time guarantees that any majority of the old configuration intersects any
// majority of the new one, which is what makes it safe without joint
// consensus.
type ConfChange struct {
	Type ConfChangeType
	Node NodeID
	// Meta is opaque to the core. The server stores the member's network
	// addresses here so that they replicate with the membership.
	Meta []byte
}

// Member is one voting member of the cluster.
type Member struct {
	ID   NodeID
	Meta []byte
}

// Membership is the set of voting members, sorted by ID.
type Membership struct {
	Members []Member
}

// Clone returns a deep copy.
func (m Membership) Clone() Membership {
	out := Membership{Members: make([]Member, len(m.Members))}
	for i, mem := range m.Members {
		out.Members[i] = Member{ID: mem.ID, Meta: append([]byte(nil), mem.Meta...)}
	}
	return out
}

// Contains reports whether id is a member.
func (m Membership) Contains(id NodeID) bool {
	i := sort.Search(len(m.Members), func(i int) bool { return m.Members[i].ID >= id })
	return i < len(m.Members) && m.Members[i].ID == id
}

// IDs returns the member IDs in ascending order.
func (m Membership) IDs() []NodeID {
	out := make([]NodeID, len(m.Members))
	for i, mem := range m.Members {
		out[i] = mem.ID
	}
	return out
}

// apply returns the membership after cc. The receiver is not modified.
func (m Membership) apply(cc ConfChange) Membership {
	out := Membership{Members: make([]Member, 0, len(m.Members)+1)}
	switch cc.Type {
	case AddNode:
		placed := false
		for _, mem := range m.Members {
			if !placed && cc.Node <= mem.ID {
				out.Members = append(out.Members, Member{ID: cc.Node, Meta: cc.Meta})
				placed = true
				if cc.Node == mem.ID {
					continue
				}
			}
			out.Members = append(out.Members, mem)
		}
		if !placed {
			out.Members = append(out.Members, Member{ID: cc.Node, Meta: cc.Meta})
		}
	case RemoveNode:
		for _, mem := range m.Members {
			if mem.ID != cc.Node {
				out.Members = append(out.Members, mem)
			}
		}
	default:
		out.Members = append(out.Members, m.Members...)
	}
	return out
}

// NewMembership builds a Membership from members in any order.
func NewMembership(members ...Member) Membership {
	out := Membership{Members: append([]Member(nil), members...)}
	sort.Slice(out.Members, func(i, j int) bool { return out.Members[i].ID < out.Members[j].ID })
	return out
}

// Snapshot is a point-in-time image of the state machine together with the
// log position and membership it corresponds to.
type Snapshot struct {
	Index uint64
	Term  uint64
	Conf  Membership
	Data  []byte
}

// HardState is the part of a server's state that must survive a crash
// before the server may act on it.
type HardState struct {
	Term uint64
	Vote NodeID
}

// MsgType is the type of a Raft message.
type MsgType uint8

const (
	MsgPreVote MsgType = iota + 1
	MsgPreVoteResp
	MsgVote
	MsgVoteResp
	MsgApp
	MsgAppResp
	MsgHeartbeat
	MsgHeartbeatResp
	MsgSnap
	MsgTimeoutNow

	numMsgTypes
)

var msgNames = [...]string{
	MsgPreVote:       "PreVote",
	MsgPreVoteResp:   "PreVoteResp",
	MsgVote:          "Vote",
	MsgVoteResp:      "VoteResp",
	MsgApp:           "App",
	MsgAppResp:       "AppResp",
	MsgHeartbeat:     "Heartbeat",
	MsgHeartbeatResp: "HeartbeatResp",
	MsgSnap:          "Snap",
	MsgTimeoutNow:    "TimeoutNow",
}

func (t MsgType) String() string {
	if t == 0 || int(t) >= len(msgNames) {
		return fmt.Sprintf("Msg(%d)", uint8(t))
	}
	return msgNames[t]
}

// Message is the single wire message of the protocol. Which fields are
// meaningful depends on Type:
//
//	PreVote, Vote     Index/LogTerm = candidate's last log position;
//	                  Transfer marks an election started by TimeoutNow.
//	App               Index/LogTerm = position preceding Entries; Commit.
//	AppResp           on success Index = highest index known to match;
//	                  on Reject Index = the rejected preceding index and
//	                  ConflictIndex/ConflictTerm guide the leader's backoff.
//	Heartbeat(Resp)   Commit; Context carries the ReadIndex sequence.
//	Snap              Snapshot.
type Message struct {
	Type MsgType
	From NodeID
	To   NodeID
	Term uint64

	Index   uint64
	LogTerm uint64
	Commit  uint64
	Entries []Entry

	Reject        bool
	ConflictIndex uint64
	ConflictTerm  uint64

	Context  uint64
	Transfer bool
	Snapshot *Snapshot
}

// Role is a server's current role.
type Role uint8

const (
	Follower Role = iota
	PreCandidate
	Candidate
	Leader
)

func (r Role) String() string {
	switch r {
	case Follower:
		return "follower"
	case PreCandidate:
		return "pre-candidate"
	case Candidate:
		return "candidate"
	case Leader:
		return "leader"
	}
	return "unknown"
}

// Errors returned by the proposal and read entry points.
var (
	// ErrNotLeader means the request must be retried on another node.
	ErrNotLeader = errors.New("raft: not the leader")
	// ErrNotReady means this node is the leader but has not yet committed
	// an entry of its own term, so it cannot yet serve the request.
	ErrNotReady = errors.New("raft: leader has not committed an entry in its term")
	// ErrTransferring means a leadership transfer is in progress.
	ErrTransferring = errors.New("raft: leadership transfer in progress")
	// ErrConfChangePending means an earlier membership change is not yet
	// committed.
	ErrConfChangePending = errors.New("raft: a membership change is already in progress")
	// ErrBadConfChange means the change would have no effect or would
	// leave the cluster without voters.
	ErrBadConfChange = errors.New("raft: membership change is not applicable")
)

// Storage is the stable storage the core writes through. Append and
// SaveHardState may buffer; nothing is assumed durable until Sync returns.
// SaveSnapshot and InstallSnapshot are durable when they return.
//
// A Storage error is fatal: the core panics with a *StorageError rather
// than continue on storage whose state it no longer knows.
type Storage interface {
	// InitialState returns the state recovered at startup: the hard
	// state, the latest snapshot (without requiring Data) and the log
	// entries that follow it.
	InitialState() (HardState, Snapshot, []Entry, error)
	// SaveHardState records the term and vote.
	SaveHardState(HardState) error
	// Append records entries. If the first entry's index is not past the
	// end of the stored log, the stored suffix from that index on is
	// replaced.
	Append([]Entry) error
	// Sync makes everything recorded so far durable.
	Sync() error
	// SaveSnapshot durably records a snapshot of the local state machine
	// and allows the log up to its index to be discarded.
	SaveSnapshot(Snapshot) error
	// InstallSnapshot durably replaces the snapshot and discards the
	// entire log.
	InstallSnapshot(Snapshot) error
	// Snapshot returns the latest snapshot including its data.
	Snapshot() (Snapshot, error)
}

// Transport carries messages to other servers. Send must not block and may
// drop, delay, duplicate or reorder messages.
type Transport interface {
	Send(Message)
}

// Rand supplies the randomness for election timeouts.
type Rand interface {
	// Intn returns a uniform integer in [0, n).
	Intn(n int) int
}

// StorageError is the panic value raised when Storage fails.
type StorageError struct{ Err error }

func (e *StorageError) Error() string { return "raft: storage failure: " + e.Err.Error() }
func (e *StorageError) Unwrap() error { return e.Err }
