//go:build !darwin

package vfs

import (
	"os"
	"runtime"
)

// On Linux and the BSDs fsync(2) already asks the device to flush its cache.
func fullSync(f *os.File) error  { return f.Sync() }
func plainSync(f *os.File) error { return f.Sync() }

func syncDir(d *os.File, _ SyncMode) error {
	err := d.Sync()
	if err != nil && runtime.GOOS == "windows" {
		// Windows cannot fsync a directory handle; NTFS journals metadata.
		return nil
	}
	return err
}
