// Package mutation defines deliberately injected bugs.
//
// Each Mutation switches one safety-critical line of the real node code to a
// plausible wrong version. They exist to measure the test harness: a
// simulator that cannot tell the mutated code from the correct code is not
// evidence of anything. No flag, environment variable or configuration file
// selects a mutation; the only constructor of a non-empty Set refuses to run
// outside a Go test binary.
package mutation

import (
	"flag"
	"fmt"
)

// Mutation identifies one injected bug.
type Mutation uint8

const (
	// VoteWithoutLogCheck grants votes (and pre-votes) without checking
	// that the candidate's log is at least as up to date as the voter's
	// (Raft §5.4.1).
	VoteWithoutLogCheck Mutation = iota + 1
	// CommitPriorTermByCount lets a leader advance its commit index to an
	// entry from an earlier term as soon as a majority stores it, instead
	// of only committing entries of its own term (Raft §5.4.2, Figure 8).
	CommitPriorTermByCount
	// VoteNotPersisted answers a vote request without first writing the
	// vote to stable storage, so a restarted node can vote twice in one
	// term.
	VoteNotPersisted
	// AckBeforeFsync sends acknowledgements (and counts the leader's own
	// copy) before the log write has been fsynced.
	AckBeforeFsync
	// ReadWithoutQuorum serves a linearizable read from whatever node
	// believes it is leader, without the ReadIndex heartbeat round that
	// proves it has not been deposed.
	ReadWithoutQuorum
	// DuplicateApply disables session de-duplication in the state machine,
	// so a retried write is applied every time it is committed.
	DuplicateApply
	// SkipWALChecksum makes WAL recovery accept records without verifying
	// their checksum, so a torn final record is replayed as if it were
	// valid.
	SkipWALChecksum
	// SkipDirSync omits the directory fsync after creating a WAL segment,
	// so a crash can lose a whole segment of acknowledged entries.
	SkipDirSync
	// TruncateWithoutMarker installs a snapshot received from the leader
	// without writing the WAL reset marker that voids the old log, so a
	// stale uncommitted suffix can reappear after a restart.
	TruncateWithoutMarker
	// ConfChangeBeforeTermCommit lets a new leader propose a membership
	// change before it has committed an entry of its own term, the bug in
	// single-server membership change found by Ongaro in 2015.
	ConfChangeBeforeTermCommit

	numMutations
)

var names = [...]string{
	VoteWithoutLogCheck:        "vote-without-log-check",
	CommitPriorTermByCount:     "commit-prior-term-by-count",
	VoteNotPersisted:           "vote-not-persisted",
	AckBeforeFsync:             "ack-before-fsync",
	ReadWithoutQuorum:          "read-without-quorum",
	DuplicateApply:             "duplicate-apply",
	SkipWALChecksum:            "skip-wal-checksum",
	SkipDirSync:                "skip-dir-sync",
	TruncateWithoutMarker:      "truncate-without-marker",
	ConfChangeBeforeTermCommit: "conf-change-before-term-commit",
}

// String returns the stable name used in test output and documentation.
func (m Mutation) String() string {
	if m == 0 || int(m) >= len(names) {
		return fmt.Sprintf("mutation(%d)", uint8(m))
	}
	return names[m]
}

// All returns every defined mutation in declaration order.
func All() []Mutation {
	out := make([]Mutation, 0, numMutations-1)
	for m := Mutation(1); m < numMutations; m++ {
		out = append(out, m)
	}
	return out
}

// Set is an immutable set of mutations. The zero value is the empty set,
// which is the only value production code can construct.
type Set struct{ bits uint32 }

// Has reports whether m is in the set.
func (s Set) Has(m Mutation) bool { return s.bits&(1<<m) != 0 }

// Empty reports whether no mutation is selected.
func (s Set) Empty() bool { return s.bits == 0 }

// String lists the selected mutations.
func (s Set) String() string {
	if s.bits == 0 {
		return "none"
	}
	out := ""
	for _, m := range All() {
		if s.Has(m) {
			if out != "" {
				out += ","
			}
			out += m.String()
		}
	}
	return out
}

// TB is the part of testing.TB that ForTest needs. Declaring it here keeps
// the testing package out of the production binary.
type TB interface {
	Helper()
	Name() string
}

// ForTest returns a Set containing ms. It panics unless the process is a Go
// test binary, which is what makes mutations unreachable from the conclave
// executable.
func ForTest(tb TB, ms ...Mutation) Set {
	tb.Helper()
	if flag.Lookup("test.v") == nil {
		panic("mutation: ForTest called outside a test binary")
	}
	var s Set
	for _, m := range ms {
		if m == 0 || m >= numMutations {
			panic(fmt.Sprintf("mutation: unknown mutation %d", m))
		}
		s.bits |= 1 << m
	}
	return s
}
