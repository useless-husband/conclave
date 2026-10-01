package vfs

import (
	"bytes"
	"reflect"
	"testing"
)

func TestOS(t *testing.T) {
	for _, mode := range []SyncMode{SyncFull, SyncFsync, SyncNone} {
		t.Run(mode.String(), func(t *testing.T) {
			fs, err := NewOS(t.TempDir(), mode)
			if err != nil {
				t.Fatal(err)
			}
			f, err := fs.Create("a.tmp")
			if err != nil {
				t.Fatal(err)
			}
			if _, err := f.Write([]byte("hello ")); err != nil {
				t.Fatal(err)
			}
			if err := f.Sync(); err != nil {
				t.Fatalf("file sync: %v", err)
			}
			if err := f.Close(); err != nil {
				t.Fatal(err)
			}
			if err := fs.Rename("a.tmp", "a"); err != nil {
				t.Fatal(err)
			}
			if err := fs.SyncDir(); err != nil {
				t.Fatalf("dir sync: %v", err)
			}
			f, err = fs.OpenAppend("a")
			if err != nil {
				t.Fatal(err)
			}
			if _, err := f.Write([]byte("world!!")); err != nil {
				t.Fatal(err)
			}
			if err := f.Close(); err != nil {
				t.Fatal(err)
			}
			if err := fs.Truncate("a", 11); err != nil {
				t.Fatal(err)
			}
			got, err := fs.ReadFile("a")
			if err != nil || !bytes.Equal(got, []byte("hello world")) {
				t.Fatalf("content %q, %v", got, err)
			}
			if _, err := fs.Create("b"); err != nil {
				t.Fatal(err)
			}
			names, err := fs.List()
			if err != nil || !reflect.DeepEqual(names, []string{"a", "b"}) {
				t.Fatalf("list %v, %v", names, err)
			}
			if err := fs.Remove("b"); err != nil {
				t.Fatal(err)
			}
			if _, err := fs.OpenAppend("b"); err == nil {
				t.Fatal("opened a removed file")
			}
		})
	}
}

func TestParseSyncMode(t *testing.T) {
	for _, m := range []SyncMode{SyncFull, SyncFsync, SyncNone} {
		got, err := ParseSyncMode(m.String())
		if err != nil || got != m {
			t.Fatalf("%v: got %v, %v", m, got, err)
		}
	}
	if _, err := ParseSyncMode("maybe"); err == nil {
		t.Fatal("bad mode accepted")
	}
}
