package raft

import "fmt"

// raftLog is the in-memory mirror of the durable log: every entry after the
// compaction point. Storage holds the same entries; this copy exists so that
// the core never has to read storage while running.
type raftLog struct {
	// entries[i] has index prevIndex+1+i.
	entries []Entry
	// prevIndex/prevTerm describe the entry immediately before entries[0].
	// Everything at or before prevIndex has been compacted away.
	prevIndex uint64
	prevTerm  uint64
	// stable is the highest index known to be durable on this server.
	stable uint64
}

func newLog(prevIndex, prevTerm uint64, ents []Entry) (*raftLog, error) {
	l := &raftLog{prevIndex: prevIndex, prevTerm: prevTerm}
	for i, e := range ents {
		if e.Index != prevIndex+1+uint64(i) {
			return nil, fmt.Errorf("raft: recovered log has a gap: entry %d at position %d after index %d",
				e.Index, i, prevIndex)
		}
	}
	l.entries = append(l.entries, ents...)
	l.stable = l.lastIndex()
	return l, nil
}

func (l *raftLog) firstIndex() uint64 { return l.prevIndex + 1 }
func (l *raftLog) lastIndex() uint64  { return l.prevIndex + uint64(len(l.entries)) }

func (l *raftLog) lastTerm() uint64 {
	if n := len(l.entries); n > 0 {
		return l.entries[n-1].Term
	}
	return l.prevTerm
}

// term returns the term of the entry at index i. ok is false when i has been
// compacted away or lies beyond the end of the log.
func (l *raftLog) term(i uint64) (term uint64, ok bool) {
	switch {
	case i == l.prevIndex:
		return l.prevTerm, true
	case i < l.prevIndex || i > l.lastIndex():
		return 0, false
	}
	return l.entries[i-l.prevIndex-1].Term, true
}

// matches reports whether the log has an entry at index i with term t.
func (l *raftLog) matches(i, t uint64) bool {
	got, ok := l.term(i)
	return ok && got == t
}

// slice returns the entries with indices in [lo, hi]. The result shares
// storage with the log but has no spare capacity, so an append to it cannot
// scribble on the log.
func (l *raftLog) slice(lo, hi uint64) []Entry {
	if lo > hi {
		return nil
	}
	if lo <= l.prevIndex || hi > l.lastIndex() {
		panic(fmt.Sprintf("raft: slice [%d,%d] outside log (%d,%d]", lo, hi, l.prevIndex, l.lastIndex()))
	}
	a, b := lo-l.prevIndex-1, hi-l.prevIndex
	return l.entries[a:b:b]
}

// isUpToDate implements the election restriction of Raft §5.4.1: a
// candidate's log is acceptable if its last entry has a later term, or the
// same term and an index at least as large.
func (l *raftLog) isUpToDate(lastIndex, lastTerm uint64) bool {
	mine := l.lastTerm()
	return lastTerm > mine || (lastTerm == mine && lastIndex >= l.lastIndex())
}

// append adds entries that start exactly one past the current end.
func (l *raftLog) append(ents ...Entry) {
	if len(ents) == 0 {
		return
	}
	if ents[0].Index != l.lastIndex()+1 {
		panic(fmt.Sprintf("raft: append at %d but log ends at %d", ents[0].Index, l.lastIndex()))
	}
	l.entries = append(l.entries, ents...)
}

// truncateAndAppend replaces the suffix starting at ents[0].Index.
func (l *raftLog) truncateAndAppend(ents []Entry) {
	first := ents[0].Index
	if first <= l.prevIndex || first > l.lastIndex()+1 {
		panic(fmt.Sprintf("raft: truncate at %d outside log (%d,%d]", first, l.prevIndex, l.lastIndex()))
	}
	keep := first - l.prevIndex - 1
	// The three-index slice forces a reallocation, so slices handed out
	// earlier (messages not yet sent, entries not yet applied) keep seeing
	// the entries they were created with.
	l.entries = append(l.entries[:keep:keep], ents...)
	if l.stable > first-1 {
		l.stable = first - 1
	}
}

// compactTo discards entries at or before index.
func (l *raftLog) compactTo(index uint64) {
	if index <= l.prevIndex {
		return
	}
	t, ok := l.term(index)
	if !ok {
		panic(fmt.Sprintf("raft: compact to %d beyond log end %d", index, l.lastIndex()))
	}
	rest := l.entries[index-l.prevIndex:]
	l.entries = append(make([]Entry, 0, len(rest)), rest...)
	l.prevIndex, l.prevTerm = index, t
}

// reset discards the whole log and restarts it after (index, term).
func (l *raftLog) reset(index, term uint64) {
	l.entries = nil
	l.prevIndex, l.prevTerm = index, term
	l.stable = index
}

// firstIndexOfTerm returns the lowest index i such that every entry in
// [i, from] has term t. from must hold an entry of term t.
func (l *raftLog) firstIndexOfTerm(from, t uint64) uint64 {
	i := from
	for i > l.firstIndex() {
		prev, ok := l.term(i - 1)
		if !ok || prev != t {
			break
		}
		i--
	}
	return i
}

// lastIndexOfTerm returns the highest index at or below from whose entry has
// term t, or 0 if the log holds no such entry. Terms never decrease along
// the log, so the scan can stop at the first smaller term.
func (l *raftLog) lastIndexOfTerm(from, t uint64) uint64 {
	if from > l.lastIndex() {
		from = l.lastIndex()
	}
	for i := from; i > l.prevIndex; i-- {
		got := l.entries[i-l.prevIndex-1].Term
		if got == t {
			return i
		}
		if got < t {
			return 0
		}
	}
	return 0
}
