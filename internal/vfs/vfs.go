// Package vfs is the narrow file-system interface the write-ahead log is
// written against. Production uses the operating system (OS); the simulator
// substitutes a disk that can lose unsynced writes and tear the last one.
//
// The interface models exactly the durability rules the log relies on:
// bytes written to a File are not durable until File.Sync, and creating,
// renaming or removing a name is not durable until FS.SyncDir.
package vfs

import "io"

// FS is a single flat directory of files.
type FS interface {
	// Create creates the named file, truncating it if it exists, and
	// opens it for appending.
	Create(name string) (File, error)
	// OpenAppend opens an existing file for appending.
	OpenAppend(name string) (File, error)
	// ReadFile returns the whole content of a file.
	ReadFile(name string) ([]byte, error)
	// Remove deletes a file.
	Remove(name string) error
	// Rename atomically replaces newname with oldname.
	Rename(oldname, newname string) error
	// Truncate cuts a file to size bytes.
	Truncate(name string, size int64) error
	// List returns the names of all files, sorted.
	List() ([]string, error)
	// SyncDir makes creations, renames and removals durable.
	SyncDir() error
}

// File is an append-only handle.
type File interface {
	io.Writer
	// Sync makes everything written so far durable.
	Sync() error
	Close() error
}
