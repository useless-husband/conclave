// Package wal is the durable half of a Raft server: a checksummed,
// segmented write-ahead log for the term, vote and log entries, plus
// atomically replaced snapshot files. It implements raft.Storage.
//
// # Layout
//
// A data directory holds segment files wal-<seq>.log and at most a couple of
// snapshot files snap-<index>-<term>.snap. A segment is a sequence of
// records:
//
//	| length uint32 | crc32c uint32 | type byte | payload |
//
// length counts type and payload. The checksum covers the segment's sequence
// number, the length, the type and the payload, so a record copied to the
// wrong place or cut short does not verify. Every segment starts with a
// header record and a copy of the hard state, which makes any suffix of the
// segment list self-contained and lets older segments be deleted once a
// snapshot covers their entries.
//
// # Recovery
//
// Records are replayed in order. An entry record whose index is not past the
// end of the replayed log replaces the suffix from that index, which is how
// a follower's truncation is represented in an append-only file. A reset
// record voids everything before it (it is written when a snapshot received
// from the leader replaces the log).
//
// The first record of the last segment that is incomplete or fails its
// checksum is taken to be a write torn by a crash: it and everything after
// it are cut off. Such bytes were never synced, so nothing acknowledged is
// lost. The same damage in any earlier segment is reported as corruption and
// the server refuses to start.
package wal

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"sort"
	"strings"

	"github.com/useless-husband/conclave/internal/mutation"
	"github.com/useless-husband/conclave/internal/raft"
	"github.com/useless-husband/conclave/internal/vfs"
)

// ErrCorrupt is returned by Open when the log is damaged in a way a crash
// cannot explain.
var ErrCorrupt = errors.New("wal: corrupt")

const (
	recHeader    byte = 1
	recHardState byte = 2
	recEntries   byte = 3
	recReset     byte = 4

	frameSize     = 8
	maxRecordSize = 256 << 20

	segMagic  = "conclave-wal\x01"
	snapMagic = "conclave-snp\x01"

	defaultSegmentSize = 16 << 20
)

var castagnoli = crc32.MakeTable(crc32.Castagnoli)

// Options configures a WAL.
type Options struct {
	// SegmentSize is the size after which the active segment is closed and
	// a new one started. Zero means 16 MiB.
	SegmentSize int64
	// Mutations selects injected bugs. It is always empty outside tests.
	Mutations mutation.Set
}

// Stats counts notable recovery events, for tests and the simulator.
type Stats struct {
	// TornTail is set when Open cut an incomplete or damaged record off
	// the end of the last segment. TornBytes is how much was cut.
	TornTail  bool
	TornBytes int
	// StaleSuffixDropped is set when Open discarded log entries that
	// contradicted the snapshot (a crash between installing a snapshot
	// and recording that the old log is void).
	StaleSuffixDropped bool
	Segments           int
}

type segment struct {
	seq uint64
	// maxIndex is the highest entry index ever written to this segment
	// and not since voided by a reset.
	maxIndex uint64
}

// WAL is the stable storage of one server. It is not safe for concurrent
// use.
type WAL struct {
	fs  vfs.FS
	opt Options

	hs        raft.HardState
	snap      raft.Snapshot // Data is not retained
	ents      []raft.Entry  // recovered entries, handed out once
	lastIndex uint64
	hasState  bool
	hasSnap   bool // a snapshot file exists

	segs []segment // oldest first; the last one is active
	f    vfs.File
	size int64
	buf  []byte

	stats Stats
}

func segName(seq uint64) string { return fmt.Sprintf("wal-%016x.log", seq) }

func snapName(index, term uint64) string { return fmt.Sprintf("snap-%016x-%016x.snap", index, term) }

func parseSegName(name string) (uint64, bool) {
	var seq uint64
	if len(name) != len("wal-0000000000000000.log") {
		return 0, false
	}
	if _, err := fmt.Sscanf(name, "wal-%016x.log", &seq); err != nil {
		return 0, false
	}
	return seq, true
}

func parseSnapName(name string) (index, term uint64, ok bool) {
	if len(name) != len("snap-0000000000000000-0000000000000000.snap") {
		return 0, 0, false
	}
	if _, err := fmt.Sscanf(name, "snap-%016x-%016x.snap", &index, &term); err != nil {
		return 0, 0, false
	}
	return index, term, true
}

func checksum(seq uint64, length uint32, body []byte) uint32 {
	var pre [12]byte
	binary.LittleEndian.PutUint64(pre[:8], seq)
	binary.LittleEndian.PutUint32(pre[8:], length)
	return crc32.Update(crc32.Update(0, castagnoli, pre[:]), castagnoli, body)
}

// appendRecord frames one record for the segment with the given sequence.
func appendRecord(buf []byte, seq uint64, typ byte, payload []byte) []byte {
	length := uint32(1 + len(payload))
	start := len(buf)
	buf = append(buf, 0, 0, 0, 0, 0, 0, 0, 0, typ)
	buf = append(buf, payload...)
	binary.LittleEndian.PutUint32(buf[start:], length)
	binary.LittleEndian.PutUint32(buf[start+4:], checksum(seq, length, buf[start+frameSize:]))
	return buf
}

// Open recovers the state in fs, repairing a torn tail, and returns a WAL
// ready for appending.
func Open(fs vfs.FS, opt Options) (*WAL, error) {
	if opt.SegmentSize <= 0 {
		opt.SegmentSize = defaultSegmentSize
	}
	w := &WAL{fs: fs, opt: opt}
	names, err := fs.List()
	if err != nil {
		return nil, fmt.Errorf("wal: list: %w", err)
	}

	var seqs []uint64
	type snapFile struct {
		index, term uint64
		name        string
	}
	var snaps []snapFile
	for _, name := range names {
		if strings.HasSuffix(name, ".tmp") {
			// Left behind by a crash before the rename; never referenced.
			if err := fs.Remove(name); err != nil {
				return nil, fmt.Errorf("wal: remove %s: %w", name, err)
			}
			continue
		}
		if seq, ok := parseSegName(name); ok {
			seqs = append(seqs, seq)
		} else if index, term, ok := parseSnapName(name); ok {
			snaps = append(snaps, snapFile{index, term, name})
		}
	}

	// Newest snapshot wins. It was fsynced before it was renamed into
	// place, so if it does not verify the disk is damaged.
	sort.Slice(snaps, func(i, j int) bool { return snaps[i].index > snaps[j].index })
	if len(snaps) > 0 {
		s, err := readSnapshot(fs, snaps[0].name)
		if err != nil {
			return nil, err
		}
		if s.Index != snaps[0].index || s.Term != snaps[0].term {
			return nil, fmt.Errorf("%w: snapshot %s describes index %d term %d", ErrCorrupt, snaps[0].name, s.Index, s.Term)
		}
		s.Data = nil
		w.snap = s
		w.hasState = true
		w.hasSnap = true
		for _, old := range snaps[1:] {
			if err := fs.Remove(old.name); err != nil {
				return nil, fmt.Errorf("wal: remove %s: %w", old.name, err)
			}
		}
	}

	sort.Slice(seqs, func(i, j int) bool { return seqs[i] < seqs[j] })
	var (
		ents     []raft.Entry
		validLen = -1 // valid length of the last segment; -1: unusable
	)
	for i, seq := range seqs {
		if i > 0 && seq != seqs[i-1]+1 {
			return nil, fmt.Errorf("%w: segment %d follows %d", ErrCorrupt, seq, seqs[i-1])
		}
		last := i == len(seqs)-1
		data, err := fs.ReadFile(segName(seq))
		if err != nil {
			return nil, fmt.Errorf("wal: read %s: %w", segName(seq), err)
		}
		w.segs = append(w.segs, segment{seq: seq})
		n, err := w.replay(seq, data, &ents)
		if err != nil {
			return nil, err
		}
		if n < len(data) {
			if !last {
				return nil, fmt.Errorf("%w: %s is damaged at offset %d of %d and is not the last segment",
					ErrCorrupt, segName(seq), n, len(data))
			}
			w.stats.TornTail = true
			w.stats.TornBytes = len(data) - n
		}
		if last {
			validLen = n
		}
	}

	// Reconcile the replayed entries with the snapshot.
	needReset := false
	if len(ents) > 0 {
		first, lastIdx := ents[0].Index, ents[len(ents)-1].Index
		s := w.snap.Index
		switch {
		case first > s+1:
			return nil, fmt.Errorf("%w: log starts at %d but snapshot ends at %d", ErrCorrupt, first, s)
		case lastIdx < s:
			// The snapshot is newer than the whole log, which only an
			// installed snapshot can be. The crash came before the reset
			// record that voids the old log.
			ents = nil
			needReset = true
		case first <= s && ents[s-first].Term != w.snap.Term:
			// The log disagrees with the snapshot at the snapshot's own
			// index: the snapshot was installed over a divergent log and
			// the crash came before the reset record. The whole log is
			// the stale branch.
			ents = nil
			needReset = true
			w.stats.StaleSuffixDropped = true
		case first <= s:
			ents = ents[s-first+1:]
		}
	}
	w.ents = ents
	w.lastIndex = w.snap.Index + uint64(len(ents))
	if len(ents) > 0 || w.hs != (raft.HardState{}) {
		w.hasState = true
	}

	// Make the last segment appendable again, or start a fresh one.
	if len(w.segs) > 0 && validLen <= 0 {
		// Not even a header survived: the crash hit segment creation.
		dead := w.segs[len(w.segs)-1]
		w.segs = w.segs[:len(w.segs)-1]
		if err := fs.Remove(segName(dead.seq)); err != nil {
			return nil, fmt.Errorf("wal: remove %s: %w", segName(dead.seq), err)
		}
		if err := w.newSegment(dead.seq); err != nil {
			return nil, err
		}
	} else if len(w.segs) == 0 {
		if err := w.newSegment(1); err != nil {
			return nil, err
		}
	} else {
		active := w.segs[len(w.segs)-1]
		name := segName(active.seq)
		if w.stats.TornTail {
			if err := fs.Truncate(name, int64(validLen)); err != nil {
				return nil, fmt.Errorf("wal: truncate %s: %w", name, err)
			}
		}
		f, err := fs.OpenAppend(name)
		if err != nil {
			return nil, fmt.Errorf("wal: open %s: %w", name, err)
		}
		if w.stats.TornTail {
			if err := f.Sync(); err != nil {
				return nil, fmt.Errorf("wal: sync %s: %w", name, err)
			}
		}
		w.f = f
		w.size = int64(validLen)
	}
	if needReset {
		// Finish what the interrupted InstallSnapshot started. Without
		// the reset record the stale entries stay on disk in front of
		// everything appended from now on: the next recovery would see a
		// gap, or would take the stale branch for the log again and drop
		// the new, acknowledged entries with it.
		if err := w.writeReset(w.snap.Index, w.snap.Term); err != nil {
			return nil, err
		}
	}
	w.stats.Segments = len(w.segs)
	return w, nil
}

// writeReset durably records that the log before (index, term) is void.
func (w *WAL) writeReset(index, term uint64) error {
	var enc raft.Encoder
	enc.Uvarint(index)
	enc.Uvarint(term)
	w.buf = appendRecord(w.buf, w.active().seq, recReset, enc.B)
	if err := w.Sync(); err != nil {
		return err
	}
	for i := range w.segs {
		w.segs[i].maxIndex = 0
	}
	return nil
}

// replay applies the records of one segment and returns the length of its
// valid prefix. It returns an error only for damage that cannot be a torn
// write.
func (w *WAL) replay(seq uint64, data []byte, ents *[]raft.Entry) (int, error) {
	seg := &w.segs[len(w.segs)-1]
	off := 0
	first := true
	for off < len(data) {
		if len(data)-off < frameSize {
			return off, nil
		}
		length := binary.LittleEndian.Uint32(data[off:])
		sum := binary.LittleEndian.Uint32(data[off+4:])
		if length == 0 || length > maxRecordSize || int(length) > len(data)-off-frameSize {
			return off, nil
		}
		body := data[off+frameSize : off+frameSize+int(length)]
		if !w.opt.Mutations.Has(mutation.SkipWALChecksum) && checksum(seq, length, body) != sum {
			return off, nil
		}
		typ, payload := body[0], body[1:]
		if first != (typ == recHeader) {
			if w.opt.Mutations.Has(mutation.SkipWALChecksum) {
				return off, nil
			}
			return 0, fmt.Errorf("%w: %s: header record misplaced", ErrCorrupt, segName(seq))
		}
		first = false
		d := raft.NewDecoder(payload)
		switch typ {
		case recHeader:
			if len(payload) < len(segMagic) || string(payload[:len(segMagic)]) != segMagic {
				return 0, fmt.Errorf("%w: %s: bad magic", ErrCorrupt, segName(seq))
			}
			d = raft.NewDecoder(payload[len(segMagic):])
			if got := d.Uvarint(); d.Err() != nil || got != seq {
				return 0, fmt.Errorf("%w: %s: header names segment %d", ErrCorrupt, segName(seq), got)
			}
		case recHardState:
			hs := raft.HardState{Term: d.Uvarint(), Vote: raft.NodeID(d.Uvarint())}
			if d.Err() == nil {
				w.hs = hs
			}
		case recEntries:
			batch := raft.DecodeEntries(d)
			if d.Err() != nil {
				break
			}
			for _, e := range batch {
				cur := *ents
				if len(cur) > 0 {
					base := cur[0].Index
					switch {
					case e.Index < base:
						cur = cur[:0]
					case e.Index <= base+uint64(len(cur)):
						cur = cur[:e.Index-base]
					default:
						if w.opt.Mutations.Has(mutation.SkipWALChecksum) {
							return off, nil
						}
						return 0, fmt.Errorf("%w: %s: entry %d follows %d", ErrCorrupt, segName(seq), e.Index, base+uint64(len(cur))-1)
					}
				}
				*ents = append(cur, e)
				if e.Index > seg.maxIndex {
					seg.maxIndex = e.Index
				}
			}
		case recReset:
			d.Uvarint()
			d.Uvarint()
			if d.Err() == nil {
				*ents = (*ents)[:0]
				for i := range w.segs {
					w.segs[i].maxIndex = 0
				}
			}
		default:
			if w.opt.Mutations.Has(mutation.SkipWALChecksum) {
				return off, nil
			}
			return 0, fmt.Errorf("%w: %s: unknown record type %d", ErrCorrupt, segName(seq), typ)
		}
		if d.Err() != nil {
			if w.opt.Mutations.Has(mutation.SkipWALChecksum) {
				return off, nil
			}
			return 0, fmt.Errorf("%w: %s: record at offset %d passes its checksum but does not decode", ErrCorrupt, segName(seq), off)
		}
		off += frameSize + int(length)
	}
	return off, nil
}

// newSegment creates segment seq, makes its existence durable and makes it
// the active segment.
func (w *WAL) newSegment(seq uint64) error {
	name := segName(seq)
	f, err := w.fs.Create(name)
	if err != nil {
		return fmt.Errorf("wal: create %s: %w", name, err)
	}
	var enc raft.Encoder
	enc.B = append(enc.B, segMagic...)
	enc.Uvarint(seq)
	buf := appendRecord(nil, seq, recHeader, enc.B)
	buf = appendRecord(buf, seq, recHardState, encodeHardState(w.hs))
	if _, err := f.Write(buf); err != nil {
		return fmt.Errorf("wal: write %s: %w", name, err)
	}
	if err := f.Sync(); err != nil {
		return fmt.Errorf("wal: sync %s: %w", name, err)
	}
	// Without this the file's name may not survive a crash even though
	// its contents were synced, taking every later record with it.
	if !w.opt.Mutations.Has(mutation.SkipDirSync) {
		if err := w.fs.SyncDir(); err != nil {
			return fmt.Errorf("wal: sync directory: %w", err)
		}
	}
	w.f = f
	w.size = int64(len(buf))
	w.segs = append(w.segs, segment{seq: seq})
	return nil
}

func encodeHardState(hs raft.HardState) []byte {
	var enc raft.Encoder
	enc.Uvarint(hs.Term)
	enc.Uvarint(uint64(hs.Vote))
	return enc.B
}

func (w *WAL) active() *segment { return &w.segs[len(w.segs)-1] }

// Stats returns what recovery found.
func (w *WAL) Stats() Stats { return w.stats }

// Empty reports whether the directory held no Raft state at all when it was
// opened, i.e. whether this server has never run.
func (w *WAL) Empty() bool { return !w.hasState }

// Bootstrap records the initial membership of a brand-new cluster as a
// snapshot at index 0. It is refused once the server has any state.
func (w *WAL) Bootstrap(conf raft.Membership) error {
	if w.hasState {
		return errors.New("wal: refusing to bootstrap a server that already has state")
	}
	return w.writeSnapshot(raft.Snapshot{Conf: conf})
}

// InitialState implements raft.Storage.
func (w *WAL) InitialState() (raft.HardState, raft.Snapshot, []raft.Entry, error) {
	ents := w.ents
	w.ents = nil
	return w.hs, w.snap, ents, nil
}

// SaveHardState implements raft.Storage.
func (w *WAL) SaveHardState(hs raft.HardState) error {
	w.hs = hs
	w.hasState = true
	w.buf = appendRecord(w.buf, w.active().seq, recHardState, encodeHardState(hs))
	return nil
}

// Append implements raft.Storage.
func (w *WAL) Append(ents []raft.Entry) error {
	if len(ents) == 0 {
		return nil
	}
	first := ents[0].Index
	if first <= w.snap.Index || first > w.lastIndex+1 {
		return fmt.Errorf("wal: append at %d outside (%d,%d]", first, w.snap.Index, w.lastIndex+1)
	}
	for i, e := range ents {
		if e.Index != first+uint64(i) {
			return fmt.Errorf("wal: append has a gap at %d", e.Index)
		}
	}
	var enc raft.Encoder
	raft.EncodeEntries(&enc, ents)
	if len(enc.B)+1 > maxRecordSize {
		return fmt.Errorf("wal: batch of %d bytes exceeds the record limit", len(enc.B))
	}
	seg := w.active()
	w.buf = appendRecord(w.buf, seg.seq, recEntries, enc.B)
	w.lastIndex = ents[len(ents)-1].Index
	if w.lastIndex > seg.maxIndex {
		seg.maxIndex = w.lastIndex
	}
	w.hasState = true
	return nil
}

// Sync implements raft.Storage: it writes out everything buffered since the
// last call and fsyncs the active segment.
func (w *WAL) Sync() error {
	if len(w.buf) == 0 {
		return nil
	}
	if _, err := w.f.Write(w.buf); err != nil {
		return fmt.Errorf("wal: write: %w", err)
	}
	w.size += int64(len(w.buf))
	w.buf = w.buf[:0]
	if err := w.f.Sync(); err != nil {
		return fmt.Errorf("wal: sync: %w", err)
	}
	if w.size >= w.opt.SegmentSize {
		return w.rotate()
	}
	return nil
}

func (w *WAL) rotate() error {
	if err := w.f.Close(); err != nil {
		return fmt.Errorf("wal: close segment: %w", err)
	}
	if err := w.newSegment(w.active().seq + 1); err != nil {
		return err
	}
	return w.purge()
}

// purge deletes leading segments whose every entry is covered by the
// snapshot. Only a prefix of the segment list is ever removed, so a reset
// record is never deleted before the stale entries it voids.
func (w *WAL) purge() error {
	removed := false
	for len(w.segs) > 1 && w.segs[0].maxIndex <= w.snap.Index {
		if err := w.fs.Remove(segName(w.segs[0].seq)); err != nil {
			return fmt.Errorf("wal: remove segment: %w", err)
		}
		w.segs = w.segs[1:]
		removed = true
	}
	if removed {
		if err := w.fs.SyncDir(); err != nil {
			return fmt.Errorf("wal: sync directory: %w", err)
		}
	}
	return nil
}

// writeSnapshot durably replaces the snapshot file: write to a temporary
// name, fsync, rename over the final name, fsync the directory.
func (w *WAL) writeSnapshot(s raft.Snapshot) error {
	enc := raft.Encoder{B: []byte(snapMagic)}
	raft.EncodeSnapshot(&enc, s)
	sum := crc32.Checksum(enc.B, castagnoli)
	enc.B = binary.LittleEndian.AppendUint32(enc.B, sum)

	name := snapName(s.Index, s.Term)
	tmp := name + ".tmp"
	f, err := w.fs.Create(tmp)
	if err != nil {
		return fmt.Errorf("wal: create %s: %w", tmp, err)
	}
	if _, err := f.Write(enc.B); err != nil {
		return fmt.Errorf("wal: write %s: %w", tmp, err)
	}
	if err := f.Sync(); err != nil {
		return fmt.Errorf("wal: sync %s: %w", tmp, err)
	}
	if err := f.Close(); err != nil {
		return fmt.Errorf("wal: close %s: %w", tmp, err)
	}
	if err := w.fs.Rename(tmp, name); err != nil {
		return fmt.Errorf("wal: rename %s: %w", tmp, err)
	}
	if err := w.fs.SyncDir(); err != nil {
		return fmt.Errorf("wal: sync directory: %w", err)
	}
	old, hadSnap := w.snap, w.hasSnap
	w.snap = raft.Snapshot{Index: s.Index, Term: s.Term, Conf: s.Conf.Clone()}
	w.hasState, w.hasSnap = true, true
	if hadSnap && (old.Index != s.Index || old.Term != s.Term) {
		// Not synced: if the removal is lost, Open removes the file.
		if err := w.fs.Remove(snapName(old.Index, old.Term)); err != nil {
			return fmt.Errorf("wal: remove old snapshot: %w", err)
		}
	}
	return nil
}

func readSnapshot(fs vfs.FS, name string) (raft.Snapshot, error) {
	data, err := fs.ReadFile(name)
	if err != nil {
		return raft.Snapshot{}, fmt.Errorf("wal: read %s: %w", name, err)
	}
	if len(data) < len(snapMagic)+4 || string(data[:len(snapMagic)]) != snapMagic {
		return raft.Snapshot{}, fmt.Errorf("%w: %s: bad magic", ErrCorrupt, name)
	}
	body, sum := data[:len(data)-4], binary.LittleEndian.Uint32(data[len(data)-4:])
	if crc32.Checksum(body, castagnoli) != sum {
		return raft.Snapshot{}, fmt.Errorf("%w: %s: checksum mismatch", ErrCorrupt, name)
	}
	d := raft.NewDecoder(body[len(snapMagic):])
	s := raft.DecodeSnapshot(d)
	if d.Err() != nil || d.Remaining() != 0 {
		return raft.Snapshot{}, fmt.Errorf("%w: %s: does not decode", ErrCorrupt, name)
	}
	return s, nil
}

// SaveSnapshot implements raft.Storage: it records a snapshot of the local
// state machine and releases the log it covers.
func (w *WAL) SaveSnapshot(s raft.Snapshot) error {
	if s.Index <= w.snap.Index {
		return nil
	}
	if s.Index > w.lastIndex {
		return fmt.Errorf("wal: snapshot at %d beyond log end %d", s.Index, w.lastIndex)
	}
	if err := w.writeSnapshot(s); err != nil {
		return err
	}
	return w.purge()
}

// InstallSnapshot implements raft.Storage: the snapshot replaces the entire
// log. The order matters. The snapshot file goes first, because once the
// reset record is durable the old log is gone and the snapshot is all that
// is left; a crash in between is repaired by Open, which notices that the
// log contradicts the snapshot.
func (w *WAL) InstallSnapshot(s raft.Snapshot) error {
	if err := w.writeSnapshot(s); err != nil {
		return err
	}
	if w.opt.Mutations.Has(mutation.TruncateWithoutMarker) {
		if err := w.Sync(); err != nil {
			return err
		}
	} else if err := w.writeReset(s.Index, s.Term); err != nil {
		return err
	}
	w.lastIndex = s.Index
	return w.purge()
}

// Snapshot implements raft.Storage.
func (w *WAL) Snapshot() (raft.Snapshot, error) {
	if !w.hasSnap {
		// A server that joined without bootstrapping has no snapshot yet.
		return raft.Snapshot{}, nil
	}
	return readSnapshot(w.fs, snapName(w.snap.Index, w.snap.Term))
}

// Close flushes buffered records and closes the active segment.
func (w *WAL) Close() error {
	if w.f == nil {
		return nil
	}
	err := w.Sync()
	if cerr := w.f.Close(); err == nil {
		err = cerr
	}
	w.f = nil
	return err
}
