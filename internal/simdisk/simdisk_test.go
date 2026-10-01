package simdisk

import (
	"bytes"
	"errors"
	"io/fs"
	"testing"

	"github.com/useless-husband/conclave/internal/prng"
)

func write(t *testing.T, d *Disk, name string, data string, sync bool) {
	t.Helper()
	f, err := d.Create(name)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.Write([]byte(data)); err != nil {
		t.Fatal(err)
	}
	if sync {
		if err := f.Sync(); err != nil {
			t.Fatal(err)
		}
	}
}

func read(t *testing.T, d *Disk, name string) (string, bool) {
	t.Helper()
	b, err := d.ReadFile(name)
	if errors.Is(err, fs.ErrNotExist) {
		return "", false
	}
	if err != nil {
		t.Fatal(err)
	}
	return string(b), true
}

func TestUnsyncedNameIsLost(t *testing.T) {
	d := New(prng.New(1))
	write(t, d, "a", "hello", true)
	d.Crash()
	if _, ok := read(t, d, "a"); ok {
		t.Fatal("a file whose directory entry was never synced survived a crash")
	}
	write(t, d, "b", "hello", true)
	if err := d.SyncDir(); err != nil {
		t.Fatal(err)
	}
	d.Crash()
	if got, ok := read(t, d, "b"); !ok || got != "hello" {
		t.Fatalf("synced file after crash: %q %v", got, ok)
	}
}

func TestSyncedBytesSurviveAndUnsyncedTailIsAPrefix(t *testing.T) {
	sawLoss, sawTorn, sawFull := false, false, false
	for seed := uint64(1); seed <= 200; seed++ {
		d := New(prng.New(seed))
		write(t, d, "log", "durable|", true)
		if err := d.SyncDir(); err != nil {
			t.Fatal(err)
		}
		f, err := d.OpenAppend("log")
		if err != nil {
			t.Fatal(err)
		}
		tail := "volatile-tail-bytes"
		if _, err := f.Write([]byte(tail)); err != nil {
			t.Fatal(err)
		}
		torn := d.TornWrites
		d.Crash()
		got, ok := read(t, d, "log")
		if !ok || len(got) < len("durable|") || got[:len("durable|")] != "durable|" {
			t.Fatalf("seed %d: synced prefix damaged: %q", seed, got)
		}
		rest := got[len("durable|"):]
		if len(rest) > len(tail) {
			t.Fatalf("seed %d: crash invented bytes: %q", seed, rest)
		}
		if d.TornWrites == torn && rest != tail[:len(rest)] {
			t.Fatalf("seed %d: surviving tail %q is not a prefix of %q", seed, rest, tail)
		}
		sawLoss = sawLoss || len(rest) < len(tail)
		sawTorn = sawTorn || d.TornWrites > torn
		sawFull = sawFull || rest == tail
	}
	if !sawLoss || !sawTorn || !sawFull {
		t.Fatalf("200 seeds did not cover loss=%v torn=%v full=%v", sawLoss, sawTorn, sawFull)
	}
}

func TestRenameIsNotDurableUntilSyncDir(t *testing.T) {
	d := New(prng.New(3))
	write(t, d, "old", "v1", true)
	if err := d.SyncDir(); err != nil {
		t.Fatal(err)
	}
	write(t, d, "tmp", "v2", true)
	if err := d.Rename("tmp", "old"); err != nil {
		t.Fatal(err)
	}
	d.Crash()
	if got, _ := read(t, d, "old"); got != "v1" {
		t.Fatalf("rename survived without a directory sync: %q", got)
	}
	if d.LostRenames == 0 {
		t.Fatal("lost rename not counted")
	}
}

func TestArmedCrashPanicsAtTheRightStep(t *testing.T) {
	d := New(prng.New(4))
	f, err := d.Create("x")
	if err != nil {
		t.Fatal(err)
	}
	d.Arm(2)
	if _, err := f.Write([]byte("one")); err != nil {
		t.Fatal(err)
	}
	func() {
		defer func() {
			if _, ok := recover().(Crashed); !ok {
				t.Fatal("second operation did not crash")
			}
		}()
		_ = f.Sync()
	}()
	if d.Armed() {
		t.Fatal("still armed after striking")
	}
}

func TestTruncateMayOrMayNotSurvive(t *testing.T) {
	seen := map[string]bool{}
	for seed := uint64(1); seed <= 64; seed++ {
		d := New(prng.New(seed))
		write(t, d, "f", "0123456789", true)
		if err := d.SyncDir(); err != nil {
			t.Fatal(err)
		}
		if err := d.Truncate("f", 4); err != nil {
			t.Fatal(err)
		}
		d.Crash()
		got, _ := read(t, d, "f")
		if got != "0123" && got != "0123456789" {
			t.Fatalf("seed %d: unsynced truncate produced %q", seed, got)
		}
		seen[got] = true
	}
	if len(seen) != 2 {
		t.Fatalf("outcomes %v: want both", seen)
	}
}

func TestCorruptFlipsDurableBit(t *testing.T) {
	d := New(prng.New(5))
	write(t, d, "f", "abcd", true)
	if err := d.SyncDir(); err != nil {
		t.Fatal(err)
	}
	if err := d.Corrupt("f", 1); err != nil {
		t.Fatal(err)
	}
	d.Crash()
	got, _ := read(t, d, "f")
	if bytes.Equal([]byte(got), []byte("abcd")) {
		t.Fatal("corruption did not persist")
	}
}
