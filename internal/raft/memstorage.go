package raft

import "fmt"

// MemStorage is a Storage that keeps everything in memory. It is used by
// unit tests and benchmarks that do not need durability; the server and the
// simulator use the write-ahead log in internal/wal instead.
type MemStorage struct {
	hs   HardState
	snap Snapshot
	ents []Entry

	// Syncs counts Sync calls, for tests.
	Syncs int
}

// NewMemStorage returns storage whose initial snapshot (at index 0) carries
// the given membership.
func NewMemStorage(conf Membership) *MemStorage {
	return &MemStorage{snap: Snapshot{Conf: conf.Clone()}}
}

func (s *MemStorage) InitialState() (HardState, Snapshot, []Entry, error) {
	return s.hs, s.snap, append([]Entry(nil), s.ents...), nil
}

func (s *MemStorage) SaveHardState(hs HardState) error {
	s.hs = hs
	return nil
}

func (s *MemStorage) Append(ents []Entry) error {
	if len(ents) == 0 {
		return nil
	}
	first := ents[0].Index
	last := s.snap.Index + uint64(len(s.ents))
	if first <= s.snap.Index || first > last+1 {
		return fmt.Errorf("memstorage: append at %d outside (%d,%d]", first, s.snap.Index, last+1)
	}
	keep := first - s.snap.Index - 1
	s.ents = append(s.ents[:keep:keep], ents...)
	return nil
}

func (s *MemStorage) Sync() error {
	s.Syncs++
	return nil
}

func (s *MemStorage) SaveSnapshot(snap Snapshot) error {
	if snap.Index <= s.snap.Index {
		return nil
	}
	last := s.snap.Index + uint64(len(s.ents))
	if snap.Index > last {
		return fmt.Errorf("memstorage: snapshot %d beyond log end %d", snap.Index, last)
	}
	s.ents = append([]Entry(nil), s.ents[snap.Index-s.snap.Index:]...)
	s.snap = snap
	return nil
}

func (s *MemStorage) InstallSnapshot(snap Snapshot) error {
	s.ents = nil
	s.snap = snap
	return nil
}

func (s *MemStorage) Snapshot() (Snapshot, error) { return s.snap, nil }
