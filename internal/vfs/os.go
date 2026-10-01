package vfs

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
)

// SyncMode selects how OS flushes data to the device.
type SyncMode int

const (
	// SyncFull asks the device to flush its write cache: F_FULLFSYNC on
	// macOS, fsync elsewhere. This is the only mode that survives power
	// loss and it is the default.
	SyncFull SyncMode = iota
	// SyncFsync uses plain fsync everywhere. On macOS that hands the data
	// to the drive but does not flush the drive's cache, so it survives a
	// process or kernel crash but not necessarily power loss.
	SyncFsync
	// SyncNone does not sync at all. For benchmarks and throwaway
	// clusters only: a crash can lose acknowledged writes.
	SyncNone
)

// ParseSyncMode parses the -fsync flag value.
func ParseSyncMode(s string) (SyncMode, error) {
	switch s {
	case "full":
		return SyncFull, nil
	case "fsync":
		return SyncFsync, nil
	case "none":
		return SyncNone, nil
	}
	return 0, fmt.Errorf("unknown fsync mode %q (want full, fsync or none)", s)
}

func (m SyncMode) String() string {
	switch m {
	case SyncFull:
		return "full"
	case SyncFsync:
		return "fsync"
	case SyncNone:
		return "none"
	}
	return "unknown"
}

// OS is an FS backed by one directory of the real file system.
type OS struct {
	dir  string
	mode SyncMode
}

// NewOS returns an FS rooted at dir, creating the directory if needed.
func NewOS(dir string, mode SyncMode) (*OS, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	return &OS{dir: dir, mode: mode}, nil
}

func (o *OS) path(name string) string { return filepath.Join(o.dir, filepath.Base(name)) }

func (o *OS) Create(name string) (File, error) {
	f, err := os.OpenFile(o.path(name), os.O_WRONLY|os.O_CREATE|os.O_TRUNC|os.O_APPEND, 0o644)
	if err != nil {
		return nil, err
	}
	return &osFile{f: f, mode: o.mode}, nil
}

func (o *OS) OpenAppend(name string) (File, error) {
	f, err := os.OpenFile(o.path(name), os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return nil, err
	}
	return &osFile{f: f, mode: o.mode}, nil
}

func (o *OS) ReadFile(name string) ([]byte, error) { return os.ReadFile(o.path(name)) }
func (o *OS) Remove(name string) error             { return os.Remove(o.path(name)) }
func (o *OS) Rename(oldname, newname string) error {
	return os.Rename(o.path(oldname), o.path(newname))
}
func (o *OS) Truncate(name string, size int64) error { return os.Truncate(o.path(name), size) }

func (o *OS) List() ([]string, error) {
	ents, err := os.ReadDir(o.dir)
	if err != nil {
		return nil, err
	}
	names := make([]string, 0, len(ents))
	for _, e := range ents {
		if !e.IsDir() {
			names = append(names, e.Name())
		}
	}
	sort.Strings(names)
	return names, nil
}

func (o *OS) SyncDir() error {
	if o.mode == SyncNone {
		return nil
	}
	d, err := os.Open(o.dir)
	if err != nil {
		return err
	}
	defer d.Close()
	return syncDir(d, o.mode)
}

type osFile struct {
	f    *os.File
	mode SyncMode
}

func (f *osFile) Write(p []byte) (int, error) { return f.f.Write(p) }
func (f *osFile) Close() error                { return f.f.Close() }

func (f *osFile) Sync() error {
	switch f.mode {
	case SyncNone:
		return nil
	case SyncFsync:
		return plainSync(f.f)
	}
	return fullSync(f.f)
}
