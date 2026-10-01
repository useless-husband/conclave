//go:build darwin

package vfs

import (
	"os"
	"syscall"
)

// fullSync issues fcntl(F_FULLFSYNC). On macOS fsync(2) only moves data to
// the drive; the drive may keep it in a volatile cache and even reorder it.
// F_FULLFSYNC asks the drive to flush that cache, which is what a
// write-ahead log means by "durable". (Go's os.File.Sync happens to do the
// same on darwin; calling fcntl directly makes the guarantee explicit and
// independent of that implementation detail.)
func fullSync(f *os.File) error {
	conn, err := f.SyscallConn()
	if err != nil {
		return err
	}
	var serr error
	if err := conn.Control(func(fd uintptr) {
		for {
			_, _, e := syscall.Syscall(syscall.SYS_FCNTL, fd, uintptr(syscall.F_FULLFSYNC), 0)
			if e == syscall.EINTR {
				continue
			}
			if e != 0 {
				serr = e
			}
			return
		}
	}); err != nil {
		return err
	}
	if serr != nil {
		return &os.PathError{Op: "fcntl(F_FULLFSYNC)", Path: f.Name(), Err: serr}
	}
	return nil
}

func plainSync(f *os.File) error {
	conn, err := f.SyscallConn()
	if err != nil {
		return err
	}
	var serr error
	if err := conn.Control(func(fd uintptr) {
		for {
			serr = syscall.Fsync(int(fd))
			if serr != syscall.EINTR {
				return
			}
		}
	}); err != nil {
		return err
	}
	if serr != nil {
		return &os.PathError{Op: "fsync", Path: f.Name(), Err: serr}
	}
	return nil
}

func syncDir(d *os.File, mode SyncMode) error {
	if mode == SyncFull {
		if err := fullSync(d); err == nil {
			return nil
		}
		// Some file systems reject F_FULLFSYNC on a directory.
	}
	return plainSync(d)
}
