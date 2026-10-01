// Package simdisk is an in-memory vfs.FS that fails the way real disks do
// when the power goes out.
//
// It keeps two views of every file: what the running process sees, and
// what is actually durable. File.Sync promotes a file's contents to
// durable; FS.SyncDir promotes the directory (which names exist and what
// they point to). Crash throws away the process's view and rebuilds it from
// the durable one, with two twists taken from real hardware:
//
//   - bytes appended since the last sync may partly survive (any prefix of
//     them), and
//   - the end of that surviving prefix may be garbage (a torn write).
//
// A crash can also be armed to strike in the middle of a specific write,
// sync or rename, which is how the simulator interrupts the write-ahead log
// between any two of its steps.
package simdisk

import (
	"errors"
	"fmt"
	"io/fs"
	"sort"

	"github.com/useless-husband/conclave/internal/prng"
	"github.com/useless-husband/conclave/internal/vfs"
)

// Crashed is the panic value raised by an armed crash. The code under test
// is abandoned mid-operation, exactly as a killed process would be; the
// owner of the Disk recovers the panic and calls Crash.
type Crashed struct{}

func (Crashed) Error() string { return "simdisk: simulated crash" }

type inode struct {
	data   []byte // contents as the running process sees them
	synced []byte // contents as of the last Sync
	// appendOnly records that every change since the last Sync was an
	// append, so the durable contents are a prefix of data.
	appendOnly bool
}

// Disk is one simulated storage device holding one directory.
type Disk struct {
	rng     *prng.Rand
	files   map[string]*inode // what the process sees
	durable map[string]*inode // what survives a crash

	// TornPercent is the probability, in percent, that the surviving part
	// of an unsynced write ends in garbage.
	TornPercent int

	armed     bool
	countdown int

	// Counters, for tests and simulator statistics.
	Crashes     int
	LostBytes   int
	TornWrites  int
	LostRenames int
}

// New returns an empty disk drawing its randomness from rng.
func New(rng *prng.Rand) *Disk {
	return &Disk{
		rng:         rng,
		files:       map[string]*inode{},
		durable:     map[string]*inode{},
		TornPercent: 50,
	}
}

var _ vfs.FS = (*Disk)(nil)

// Arm schedules a crash to strike during the n-th mutating operation from
// now (n >= 1). The operation may be cut short or may complete; either way
// it then panics with Crashed.
func (d *Disk) Arm(n int) {
	if n < 1 {
		n = 1
	}
	d.armed = true
	d.countdown = n
}

// Armed reports whether a crash is pending.
func (d *Disk) Armed() bool { return d.armed }

// Disarm cancels a pending armed crash.
func (d *Disk) Disarm() { d.armed = false }

// step is called at the start of every mutating operation. It reports
// whether the crash strikes now, and if so whether the operation should
// still take effect before the panic.
func (d *Disk) step() (crash, apply bool) {
	if !d.armed {
		return false, true
	}
	d.countdown--
	if d.countdown > 0 {
		return false, true
	}
	d.armed = false
	return true, d.rng.Intn(2) == 0
}

// Crash simulates power loss: the visible state is replaced by what was
// durable, plus whatever part of the unsynced writes the dice let through.
func (d *Disk) Crash() {
	d.Crashes++
	d.armed = false
	names := make([]string, 0, len(d.durable))
	for name := range d.durable {
		names = append(names, name)
	}
	sort.Strings(names)
	for name, ino := range d.files {
		if d.durable[name] != ino {
			d.LostRenames++
		}
	}
	seen := map[*inode]bool{}
	files := make(map[string]*inode, len(names))
	for _, name := range names {
		ino := d.durable[name]
		files[name] = ino
		if seen[ino] {
			continue
		}
		seen[ino] = true
		content := append([]byte(nil), ino.synced...)
		switch {
		case ino.appendOnly && len(ino.data) > len(ino.synced):
			tail := ino.data[len(ino.synced):]
			keep := d.rng.Intn(len(tail) + 1)
			d.LostBytes += len(tail) - keep
			content = append(content, tail[:keep]...)
			if keep > 0 && d.rng.Intn(100) < d.TornPercent {
				// The device was part-way through these bytes.
				n := 1 + d.rng.Intn(min(keep, 48))
				for i := len(content) - n; i < len(content); i++ {
					content[i] = byte(d.rng.Uint64())
				}
				d.TornWrites++
			}
		case !ino.appendOnly:
			// A truncation that was never synced either happened or not.
			if d.rng.Intn(2) == 0 {
				content = append([]byte(nil), ino.data...)
			}
		}
		ino.data = content
		ino.synced = append([]byte(nil), content...)
		ino.appendOnly = true
	}
	d.files = files
	d.durable = make(map[string]*inode, len(files))
	for name, ino := range files {
		d.durable[name] = ino
	}
}

func notExist(op, name string) error {
	return &fs.PathError{Op: op, Path: name, Err: fs.ErrNotExist}
}

func (d *Disk) Create(name string) (vfs.File, error) {
	crash, apply := d.step()
	var ino *inode
	if apply {
		ino = &inode{appendOnly: true}
		d.files[name] = ino
	}
	if crash {
		panic(Crashed{})
	}
	return &file{d: d, ino: ino}, nil
}

func (d *Disk) OpenAppend(name string) (vfs.File, error) {
	ino := d.files[name]
	if ino == nil {
		return nil, notExist("open", name)
	}
	return &file{d: d, ino: ino}, nil
}

func (d *Disk) ReadFile(name string) ([]byte, error) {
	ino := d.files[name]
	if ino == nil {
		return nil, notExist("read", name)
	}
	return append([]byte(nil), ino.data...), nil
}

func (d *Disk) Remove(name string) error {
	if d.files[name] == nil {
		return notExist("remove", name)
	}
	crash, apply := d.step()
	if apply {
		delete(d.files, name)
	}
	if crash {
		panic(Crashed{})
	}
	return nil
}

func (d *Disk) Rename(oldname, newname string) error {
	ino := d.files[oldname]
	if ino == nil {
		return notExist("rename", oldname)
	}
	crash, apply := d.step()
	if apply {
		delete(d.files, oldname)
		d.files[newname] = ino
	}
	if crash {
		panic(Crashed{})
	}
	return nil
}

func (d *Disk) Truncate(name string, size int64) error {
	ino := d.files[name]
	if ino == nil {
		return notExist("truncate", name)
	}
	if size < 0 || size > int64(len(ino.data)) {
		return fmt.Errorf("simdisk: truncate %s to %d, size is %d", name, size, len(ino.data))
	}
	crash, apply := d.step()
	if apply {
		ino.data = ino.data[:size:size]
		if int(size) < len(ino.synced) {
			ino.appendOnly = false
		}
	}
	if crash {
		panic(Crashed{})
	}
	return nil
}

func (d *Disk) List() ([]string, error) {
	names := make([]string, 0, len(d.files))
	for name := range d.files {
		names = append(names, name)
	}
	sort.Strings(names)
	return names, nil
}

func (d *Disk) SyncDir() error {
	crash, apply := d.step()
	if apply {
		d.durable = make(map[string]*inode, len(d.files))
		for name, ino := range d.files {
			d.durable[name] = ino
		}
	}
	if crash {
		panic(Crashed{})
	}
	return nil
}

// DurableSize returns the number of bytes of name guaranteed to survive a
// crash, or -1 if the name itself would not survive.
func (d *Disk) DurableSize(name string) int {
	ino := d.durable[name]
	if ino == nil {
		return -1
	}
	return len(ino.synced)
}

// Corrupt flips one bit of the named file, both in the visible and in the
// durable view, to model silent media corruption.
func (d *Disk) Corrupt(name string, offset int) error {
	ino := d.files[name]
	if ino == nil {
		return notExist("corrupt", name)
	}
	if offset < 0 || offset >= len(ino.data) {
		return fmt.Errorf("simdisk: corrupt %s at %d, size is %d", name, offset, len(ino.data))
	}
	ino.data[offset] ^= 0x10
	if offset < len(ino.synced) {
		ino.synced[offset] ^= 0x10
	}
	return nil
}

type file struct {
	d      *Disk
	ino    *inode
	closed bool
}

var errClosed = errors.New("simdisk: file already closed")

func (f *file) Write(p []byte) (int, error) {
	if f.closed {
		return 0, errClosed
	}
	crash, apply := f.d.step()
	if crash {
		// The write is interrupted: any prefix of it may have reached
		// the file.
		n := 0
		if apply {
			n = f.d.rng.Intn(len(p) + 1)
		}
		f.ino.data = append(f.ino.data, p[:n]...)
		panic(Crashed{})
	}
	f.ino.data = append(f.ino.data, p...)
	return len(p), nil
}

func (f *file) Sync() error {
	if f.closed {
		return errClosed
	}
	crash, apply := f.d.step()
	if apply {
		ino := f.ino
		if ino.appendOnly && len(ino.data) >= len(ino.synced) {
			ino.synced = append(ino.synced, ino.data[len(ino.synced):]...)
		} else {
			ino.synced = append([]byte(nil), ino.data...)
		}
		ino.appendOnly = true
	}
	if crash {
		panic(Crashed{})
	}
	return nil
}

func (f *file) Close() error {
	if f.closed {
		return errClosed
	}
	f.closed = true
	return nil
}
