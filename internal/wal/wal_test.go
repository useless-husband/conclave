package wal

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/useless-husband/conclave/internal/prng"
	"github.com/useless-husband/conclave/internal/raft"
	"github.com/useless-husband/conclave/internal/simdisk"
	"github.com/useless-husband/conclave/internal/vfs"
)

func ents(from, to, term uint64) []raft.Entry {
	var out []raft.Entry
	for i := from; i <= to; i++ {
		out = append(out, raft.Entry{Index: i, Term: term, Data: []byte(fmt.Sprintf("e%d.%d", i, term))})
	}
	return out
}

func mustOpen(t *testing.T, fs vfs.FS, opt Options) *WAL {
	t.Helper()
	w, err := Open(fs, opt)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	return w
}

func must(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}

type state struct {
	HS    raft.HardState
	SnapI uint64
	SnapT uint64
	Ents  []raft.Entry
}

func recovered(t *testing.T, w *WAL) state {
	t.Helper()
	hs, snap, es, err := w.InitialState()
	must(t, err)
	return state{HS: hs, SnapI: snap.Index, SnapT: snap.Term, Ents: es}
}

func sameEntries(a, b []raft.Entry) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i].Index != b[i].Index || a[i].Term != b[i].Term || a[i].Type != b[i].Type || !bytes.Equal(a[i].Data, b[i].Data) {
			return false
		}
	}
	return true
}

var conf3 = raft.NewMembership(raft.Member{ID: 1, Meta: []byte("a")}, raft.Member{ID: 2}, raft.Member{ID: 3})

func TestRoundTrip(t *testing.T) {
	disk := simdisk.New(prng.New(1))
	w := mustOpen(t, disk, Options{})
	if !w.Empty() {
		t.Fatal("fresh WAL is not empty")
	}
	must(t, w.Bootstrap(conf3))
	if err := w.Bootstrap(conf3); err == nil {
		t.Fatal("second bootstrap accepted")
	}
	must(t, w.SaveHardState(raft.HardState{Term: 3, Vote: 2}))
	must(t, w.Append(ents(1, 5, 3)))
	must(t, w.Sync())
	must(t, w.Close())

	w = mustOpen(t, disk, Options{})
	if w.Empty() {
		t.Fatal("reopened WAL claims to be empty")
	}
	hs, snap, es, err := w.InitialState()
	must(t, err)
	if hs != (raft.HardState{Term: 3, Vote: 2}) {
		t.Fatalf("hard state %+v", hs)
	}
	if snap.Index != 0 || !reflect.DeepEqual(snap.Conf, conf3) {
		t.Fatalf("snapshot %+v", snap)
	}
	if !sameEntries(es, ents(1, 5, 3)) {
		t.Fatalf("entries %+v", es)
	}
	if st := w.Stats(); st.TornTail {
		t.Fatalf("clean reopen reported a torn tail: %+v", st)
	}
}

func TestSuffixOverwrite(t *testing.T) {
	disk := simdisk.New(prng.New(1))
	w := mustOpen(t, disk, Options{})
	must(t, w.Append(ents(1, 6, 1)))
	must(t, w.Append(ents(4, 5, 2))) // replaces 4..6 with 4..5
	must(t, w.Append(ents(6, 6, 3)))
	must(t, w.Sync())
	if err := w.Append(ents(9, 9, 3)); err == nil {
		t.Fatal("append leaving a gap accepted")
	}
	must(t, w.Close())
	got := recovered(t, mustOpen(t, disk, Options{}))
	want := append(append(ents(1, 3, 1), ents(4, 5, 2)...), ents(6, 6, 3)...)
	if !sameEntries(got.Ents, want) {
		t.Fatalf("entries %+v", got.Ents)
	}
}

func TestUnsyncedWritesMayBeLostButSyncedOnesAreNot(t *testing.T) {
	for seed := uint64(1); seed <= 50; seed++ {
		disk := simdisk.New(prng.New(seed))
		w := mustOpen(t, disk, Options{})
		must(t, w.SaveHardState(raft.HardState{Term: 1, Vote: 1}))
		must(t, w.Append(ents(1, 3, 1)))
		must(t, w.Sync())
		must(t, w.SaveHardState(raft.HardState{Term: 2, Vote: 3}))
		must(t, w.Append(ents(4, 9, 2)))
		// No Sync: the process dies with these still in its buffer.
		disk.Crash()
		got := recovered(t, mustOpen(t, disk, Options{}))
		if got.HS != (raft.HardState{Term: 1, Vote: 1}) || !sameEntries(got.Ents, ents(1, 3, 1)) {
			t.Fatalf("seed %d: recovered %+v", seed, got)
		}
	}
}

// TestTornTail damages the end of the last segment in the ways a crash can
// and checks that recovery cuts it off and keeps everything before it.
func TestTornTail(t *testing.T) {
	record := appendRecord(nil, 1, recEntries, func() []byte {
		var enc raft.Encoder
		raft.EncodeEntries(&enc, ents(4, 4, 1))
		return enc.B
	}())
	flipped := append([]byte(nil), record...)
	flipped[len(flipped)-1] ^= 0x01
	cases := map[string][]byte{
		"partial header":       record[:3],
		"header only":          record[:frameSize],
		"partial payload":      record[:len(record)-2],
		"bit flip in payload":  flipped,
		"zero length":          make([]byte, 16),
		"absurd length":        {0xff, 0xff, 0xff, 0x7f, 1, 2, 3, 4, 5},
		"garbage after record": append(append([]byte(nil), record...), 0xde, 0xad, 0xbe, 0xef, 1, 2, 3, 4, 5, 6),
	}
	for name, tail := range cases {
		t.Run(name, func(t *testing.T) {
			disk := simdisk.New(prng.New(1))
			w := mustOpen(t, disk, Options{})
			must(t, w.SaveHardState(raft.HardState{Term: 1, Vote: 2}))
			must(t, w.Append(ents(1, 3, 1)))
			must(t, w.Sync())
			must(t, w.Close())
			f, err := disk.OpenAppend(segName(1))
			must(t, err)
			_, err = f.Write(tail)
			must(t, err)
			must(t, f.Sync())

			w = mustOpen(t, disk, Options{})
			st := w.Stats()
			if !st.TornTail || st.TornBytes == 0 {
				t.Fatalf("torn tail not detected: %+v", st)
			}
			got := recovered(t, w)
			want := ents(1, 3, 1)
			if name == "garbage after record" {
				want = ents(1, 4, 1)
			}
			if got.HS != (raft.HardState{Term: 1, Vote: 2}) || !sameEntries(got.Ents, want) {
				t.Fatalf("recovered %+v", got)
			}
			// The log must be usable and clean afterwards.
			next := want[len(want)-1].Index + 1
			must(t, w.Append(ents(next, next, 2)))
			must(t, w.Sync())
			must(t, w.Close())
			w = mustOpen(t, disk, Options{})
			if w.Stats().TornTail {
				t.Fatal("tail still torn after repair")
			}
			if got := recovered(t, w); !sameEntries(got.Ents, append(want, ents(next, next, 2)...)) {
				t.Fatalf("after repair: %+v", got.Ents)
			}
		})
	}
}

func TestDamageInOlderSegmentIsFatal(t *testing.T) {
	disk := simdisk.New(prng.New(1))
	w := mustOpen(t, disk, Options{SegmentSize: 200})
	for i := uint64(1); i <= 30; i++ {
		must(t, w.Append(ents(i, i, 1)))
		must(t, w.Sync())
	}
	must(t, w.Close())
	names, _ := disk.List()
	if len(names) < 3 {
		t.Fatalf("expected several segments, have %v", names)
	}
	must(t, disk.Corrupt(names[0], 40))
	_, err := Open(disk, Options{SegmentSize: 200})
	if !errors.Is(err, ErrCorrupt) {
		t.Fatalf("open after corrupting %s: %v, want ErrCorrupt", names[0], err)
	}
}

func TestMissingSegmentIsFatal(t *testing.T) {
	disk := simdisk.New(prng.New(1))
	w := mustOpen(t, disk, Options{SegmentSize: 200})
	for i := uint64(1); i <= 30; i++ {
		must(t, w.Append(ents(i, i, 1)))
		must(t, w.Sync())
	}
	must(t, w.Close())
	must(t, disk.Remove(segName(2)))
	if _, err := Open(disk, Options{SegmentSize: 200}); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("open with a missing middle segment: %v, want ErrCorrupt", err)
	}
}

func TestCorruptSnapshotIsFatal(t *testing.T) {
	disk := simdisk.New(prng.New(1))
	w := mustOpen(t, disk, Options{})
	must(t, w.Bootstrap(conf3))
	must(t, w.Close())
	must(t, disk.Corrupt(snapName(0, 0), 20))
	if _, err := Open(disk, Options{}); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("open with a damaged snapshot: %v, want ErrCorrupt", err)
	}
}

func TestRotationAndPurge(t *testing.T) {
	disk := simdisk.New(prng.New(1))
	opt := Options{SegmentSize: 256}
	w := mustOpen(t, disk, opt)
	must(t, w.Bootstrap(conf3))
	must(t, w.SaveHardState(raft.HardState{Term: 4, Vote: 1}))
	for i := uint64(1); i <= 60; i++ {
		must(t, w.Append(ents(i, i, 4)))
		must(t, w.Sync())
	}
	before, _ := disk.List()
	must(t, w.SaveSnapshot(raft.Snapshot{Index: 50, Term: 4, Conf: conf3, Data: []byte("image")}))
	after, _ := disk.List()
	if len(after) >= len(before) {
		t.Fatalf("snapshot did not release segments: %d files before, %d after", len(before), len(after))
	}
	for _, name := range after {
		if name == snapName(0, 0) {
			t.Fatal("old snapshot file kept")
		}
	}
	snap, err := w.Snapshot()
	must(t, err)
	if snap.Index != 50 || string(snap.Data) != "image" {
		t.Fatalf("snapshot %+v", snap)
	}
	must(t, w.Close())

	w = mustOpen(t, disk, opt)
	got := recovered(t, w)
	if got.SnapI != 50 || got.SnapT != 4 || got.HS != (raft.HardState{Term: 4, Vote: 1}) {
		t.Fatalf("recovered %+v", got)
	}
	if !sameEntries(got.Ents, ents(51, 60, 4)) {
		t.Fatalf("entries after snapshot: %d..%d", got.Ents[0].Index, got.Ents[len(got.Ents)-1].Index)
	}
	// The hard state survives even though the segment it was written to
	// is gone: every segment begins with a copy.
	if err := w.Append(ents(50, 50, 9)); err == nil {
		t.Fatal("append below the snapshot accepted")
	}
}

func TestInstallSnapshotVoidsOldLog(t *testing.T) {
	disk := simdisk.New(prng.New(1))
	w := mustOpen(t, disk, Options{})
	must(t, w.Bootstrap(conf3))
	must(t, w.Append(ents(1, 150, 1))) // a branch the leader does not share
	must(t, w.Sync())
	must(t, w.InstallSnapshot(raft.Snapshot{Index: 100, Term: 5, Conf: conf3, Data: []byte("s")}))
	must(t, w.Append(ents(101, 103, 5)))
	must(t, w.Sync())
	disk.Crash()

	got := recovered(t, mustOpen(t, disk, Options{}))
	if got.SnapI != 100 || got.SnapT != 5 || !sameEntries(got.Ents, ents(101, 103, 5)) {
		t.Fatalf("recovered snap=%d/%d entries=%d", got.SnapI, got.SnapT, len(got.Ents))
	}
}

// TestCrashDuringInstallSnapshot interrupts InstallSnapshot at every one of
// its I/O steps. Whatever survives must be either the old state or the new
// one, never the new snapshot with the old, contradicting log after it.
func TestCrashDuringInstallSnapshot(t *testing.T) {
	sawDropped := false
	for seed := uint64(1); seed <= 40; seed++ {
		for step := 1; step <= 10; step++ {
			disk := simdisk.New(prng.New(seed))
			w := mustOpen(t, disk, Options{})
			must(t, w.Bootstrap(conf3))
			must(t, w.SaveHardState(raft.HardState{Term: 1, Vote: 1}))
			must(t, w.Append(ents(1, 150, 1)))
			must(t, w.Sync())

			disk.Arm(step)
			crashed := func() (crashed bool) {
				defer func() {
					if r := recover(); r != nil {
						if _, ok := r.(simdisk.Crashed); !ok {
							panic(r)
						}
						crashed = true
					}
				}()
				must(t, w.InstallSnapshot(raft.Snapshot{Index: 100, Term: 5, Conf: conf3}))
				return false
			}()
			disk.Crash()
			w, err := Open(disk, Options{})
			if err != nil {
				t.Fatalf("seed %d step %d: reopen: %v", seed, step, err)
			}
			got := recovered(t, w)
			sawDropped = sawDropped || w.Stats().StaleSuffixDropped
			oldState := got.SnapI == 0 && sameEntries(got.Ents, ents(1, 150, 1))
			newState := got.SnapI == 100 && got.SnapT == 5 && len(got.Ents) == 0
			if !oldState && !newState {
				t.Fatalf("seed %d step %d: recovered snap=%d/%d with %d entries", seed, step, got.SnapI, got.SnapT, len(got.Ents))
			}
			if !crashed && !newState {
				t.Fatalf("seed %d step %d: InstallSnapshot returned but the old state came back", seed, step)
			}
		}
	}
	if !sawDropped {
		t.Fatal("no run exercised the crash window between the snapshot file and the reset record")
	}
}

func TestOSBackedTornTail(t *testing.T) {
	dir := t.TempDir()
	fs, err := vfs.NewOS(dir, vfs.SyncFull)
	must(t, err)
	w := mustOpen(t, fs, Options{})
	must(t, w.Bootstrap(conf3))
	must(t, w.SaveHardState(raft.HardState{Term: 2, Vote: 1}))
	must(t, w.Append(ents(1, 10, 2)))
	must(t, w.Sync())
	must(t, w.Append(ents(11, 11, 2)))
	must(t, w.Sync())
	must(t, w.Close())

	// Cut the file in the middle of its last record, as a crash during
	// the final write would.
	path := filepath.Join(dir, segName(1))
	info, err := os.Stat(path)
	must(t, err)
	must(t, os.Truncate(path, info.Size()-5))

	w = mustOpen(t, fs, Options{})
	if !w.Stats().TornTail {
		t.Fatal("torn tail not detected on a real file")
	}
	got := recovered(t, w)
	if got.HS != (raft.HardState{Term: 2, Vote: 1}) || !sameEntries(got.Ents, ents(1, 10, 2)) {
		t.Fatalf("recovered %+v", got)
	}
	must(t, w.Append(ents(11, 12, 3)))
	must(t, w.Sync())
	must(t, w.Close())
	got = recovered(t, mustOpen(t, fs, Options{}))
	if !sameEntries(got.Ents, append(ents(1, 10, 2), ents(11, 12, 3)...)) {
		t.Fatalf("after repair %+v", got.Ents)
	}
}

func TestJoinerHasNoSnapshot(t *testing.T) {
	disk := simdisk.New(prng.New(1))
	w := mustOpen(t, disk, Options{})
	snap, err := w.Snapshot()
	must(t, err)
	if snap.Index != 0 || len(snap.Conf.Members) != 0 {
		t.Fatalf("snapshot of an empty server: %+v", snap)
	}
}
