// Copyright 2026 Jeremy Hahn
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package fsutil

import (
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"
)

// TestSyncDirChainWith_WalksToEveryRoot is the regression test for
// SyncDirChainWith silently degrading to a LEAF-ONLY fsync.
//
// Deciding containment with a string prefix over leaf+sep and stopAt+sep
// breaks for exactly the two values DeepestExistingAncestor returns when
// nothing above the leaf pre-exists: stopAt "/" builds the prefix "//" and
// stopAt "." builds "./", and neither prefixes a CLEANED leaf. Both fell
// through to syncing the leaf alone, skipping every ancestor os.MkdirAll had
// just created — so a crash could roll back the directory entries of the tree
// a snapshot or WAL segment had just been fsynced into.
//
// The `stopAt == ""` test also has to run BEFORE Clean, because
// filepath.Clean("") is ".", which is a legitimate stop point (the working
// directory) rather than "no stop point at all".
func TestSyncDirChainWith_WalksToEveryRoot(t *testing.T) {
	cases := []struct {
		name   string
		leaf   string
		stopAt string
		want   []string
	}{
		{"absolute filesystem root", "/a/b/c", "/", []string{"/a/b/c", "/a/b", "/a", "/"}},
		{"relative working directory", "a/b/c", ".", []string{"a/b/c", "a/b", "a", "."}},
		{"empty stopAt means no stop point: leaf only", "/a/b/c", "", []string{"/a/b/c"}},
		{"stopAt equal to leaf syncs just it", "/a/b/c", "/a/b/c", []string{"/a/b/c"}},
		{"ordinary nested stop point", "/a/b/c", "/a", []string{"/a/b/c", "/a/b", "/a"}},
		{"leaf not under stopAt: leaf only", "/a/b", "/x", []string{"/a/b"}},
		{"sibling prefix must not count as containment", "/ab/c", "/a", []string{"/ab/c"}},
		{"uncleaned inputs are normalised", "/a//b/c/", "/a/", []string{"/a/b/c", "/a/b", "/a"}},
		{"relative leaf with absolute stopAt is not comparable: leaf only", "a/b", "/x", []string{"a/b"}},
		{"relative chain under a relative stop point", "data/shard-1/replica-1", "data",
			[]string{"data/shard-1/replica-1", "data/shard-1", "data"}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			leaf := filepath.FromSlash(tc.leaf)
			stopAt := filepath.FromSlash(tc.stopAt)
			var synced []string
			err := SyncDirChainWith(func(dir string) error {
				synced = append(synced, filepath.ToSlash(dir))
				return nil
			}, leaf, stopAt)
			if err != nil {
				t.Fatalf("SyncDirChainWith returned %v", err)
			}
			if !reflect.DeepEqual(synced, tc.want) {
				t.Fatalf("synced %v, want %v: every directory MkdirAll may have "+
					"created needs its own fsync or its parent's entry is not durable",
					synced, tc.want)
			}
		})
	}
}

// TestSyncDirChainWith_PropagatesError verifies the walk stops at the first
// failing directory and returns its error rather than continuing up the chain.
func TestSyncDirChainWith_PropagatesError(t *testing.T) {
	boom := errors.New("fsync failed")
	var synced []string
	err := SyncDirChainWith(func(dir string) error {
		synced = append(synced, dir)
		if dir == "/a/b" {
			return boom
		}
		return nil
	}, "/a/b/c", "/")
	if !errors.Is(err, boom) {
		t.Fatalf("expected the syncDir error to propagate, got %v", err)
	}
	if want := []string{"/a/b/c", "/a/b"}; !reflect.DeepEqual(synced, want) {
		t.Fatalf("synced %v, want %v: the walk must stop at the first failure", synced, want)
	}
}

// fakeInfo is the minimal fs.FileInfo a stat seam returns for a present path.
type fakeInfo struct{ name string }

func (i fakeInfo) Name() string       { return i.name }
func (i fakeInfo) Size() int64        { return 0 }
func (i fakeInfo) Mode() fs.FileMode  { return fs.ModeDir | 0o750 }
func (i fakeInfo) ModTime() time.Time { return time.Time{} }
func (i fakeInfo) IsDir() bool        { return true }
func (i fakeInfo) Sys() any           { return nil }

// statOver returns a stat seam answering present for the listed paths, the
// given error for paths in errs, and fs.ErrNotExist for everything else.
func statOver(present []string, errs map[string]error) func(string) (fs.FileInfo, error) {
	set := make(map[string]bool, len(present))
	for _, p := range present {
		set[p] = true
	}
	return func(name string) (fs.FileInfo, error) {
		if err, ok := errs[name]; ok {
			return nil, err
		}
		if set[name] {
			return fakeInfo{name: filepath.Base(name)}, nil
		}
		return nil, fs.ErrNotExist
	}
}

// TestDeepestExistingAncestorWith covers the walk that chooses the stopAt for
// the chain sync following a MkdirAll.
//
// A stat error other than fs.ErrNotExist leaves the level's existence unknown
// and the walk continues upward. That makes the chain sync cover MORE
// directories, never fewer: fsyncing a directory that did exist costs one
// fsync, while skipping one that MkdirAll created loses its dirent on a crash.
func TestDeepestExistingAncestorWith(t *testing.T) {
	cases := []struct {
		name    string
		present []string
		errs    map[string]error
		path    string
		want    string
	}{
		{"path exists", []string{"/a", "/a/b", "/a/b/c"}, nil, "/a/b/c", "/a/b/c"},
		{"walks to deepest present ancestor", []string{"/a"}, nil, "/a/b/c", "/a"},
		{"nothing present stops at root", nil, nil, "/a/b/c", "/"},
		{"relative tree stops at the working directory", nil, nil, "a/b", "."},
		{"unknown existence keeps walking", []string{"/a"},
			map[string]error{"/a/b": fs.ErrPermission}, "/a/b/c", "/a"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			present := make([]string, len(tc.present))
			for i, p := range tc.present {
				present[i] = filepath.FromSlash(p)
			}
			errs := make(map[string]error, len(tc.errs))
			for p, err := range tc.errs {
				errs[filepath.FromSlash(p)] = err
			}
			got := DeepestExistingAncestorWith(statOver(present, errs), filepath.FromSlash(tc.path))
			if want := filepath.FromSlash(tc.want); got != want {
				t.Fatalf("DeepestExistingAncestorWith(%q) = %q, want %q", tc.path, got, want)
			}
		})
	}
}

// recordingDir is a directory handle that records which flush it received.
type recordingDir struct {
	log      *[]string
	syncErr  error
	closeErr error
}

func (d *recordingDir) Sync() error  { *d.log = append(*d.log, "sync"); return d.syncErr }
func (d *recordingDir) Close() error { *d.log = append(*d.log, "close"); return d.closeErr }

// metadataDir is a handle whose Sync omits metadata and which therefore
// exposes SyncMetadata, as waldb's Linux file does.
type metadataDir struct{ recordingDir }

func (d *metadataDir) SyncMetadata() error {
	*d.log = append(*d.log, "syncmeta")
	return d.syncErr
}

// TestSyncDirWith covers the open/flush/close sequence shared by every
// directory fsync.
func TestSyncDirWith(t *testing.T) {
	errSync := errors.New("sync failed")
	errClose := errors.New("close failed")
	errOpen := errors.New("open failed")

	t.Run("plain handle uses Sync then Close", func(t *testing.T) {
		var log []string
		err := SyncDirWith(func(string) (*recordingDir, error) {
			return &recordingDir{log: &log}, nil
		}, "/d")
		if err != nil {
			t.Fatalf("SyncDirWith: %v", err)
		}
		if want := []string{"sync", "close"}; !reflect.DeepEqual(log, want) {
			t.Fatalf("calls %v, want %v", log, want)
		}
	})

	t.Run("metadata-omitting handle uses SyncMetadata, never Sync", func(t *testing.T) {
		var log []string
		err := SyncDirWith(func(string) (*metadataDir, error) {
			return &metadataDir{recordingDir{log: &log}}, nil
		}, "/d")
		if err != nil {
			t.Fatalf("SyncDirWith: %v", err)
		}
		if want := []string{"syncmeta", "close"}; !reflect.DeepEqual(log, want) {
			t.Fatalf("calls %v, want %v: a directory's dirents are metadata, "+
				"so a data-only Sync does not make them durable", log, want)
		}
	})

	t.Run("sync error wins and the handle is still closed", func(t *testing.T) {
		var log []string
		err := SyncDirWith(func(string) (*recordingDir, error) {
			return &recordingDir{log: &log, syncErr: errSync, closeErr: errClose}, nil
		}, "/d")
		if !errors.Is(err, errSync) {
			t.Fatalf("got %v, want the sync error", err)
		}
		if want := []string{"sync", "close"}; !reflect.DeepEqual(log, want) {
			t.Fatalf("calls %v, want %v", log, want)
		}
	})

	t.Run("close error surfaces", func(t *testing.T) {
		var log []string
		err := SyncDirWith(func(string) (*recordingDir, error) {
			return &recordingDir{log: &log, closeErr: errClose}, nil
		}, "/d")
		if !errors.Is(err, errClose) {
			t.Fatalf("got %v, want the close error", err)
		}
	})

	t.Run("open error surfaces", func(t *testing.T) {
		var opened string
		err := SyncDirWith(func(name string) (*recordingDir, error) {
			opened = name
			return nil, errOpen
		}, "/d")
		if !errors.Is(err, errOpen) {
			t.Fatalf("got %v, want the open error", err)
		}
		if opened != "/d" {
			t.Fatalf("opened %q, want /d", opened)
		}
	})
}

// seamFS is a filesystem seam for MkdirAllSyncedWith: Stat answers from a set
// of present directories, MkdirAll adds the whole chain to it, and every call
// is logged in order.
type seamFS struct {
	present  map[string]bool
	log      []string
	mkdirErr error
	perm     fs.FileMode
}

func newSeamFS(present ...string) *seamFS {
	s := &seamFS{present: make(map[string]bool)}
	for _, p := range present {
		s.present[filepath.FromSlash(p)] = true
	}
	return s
}

func (s *seamFS) Stat(name string) (fs.FileInfo, error) {
	if s.present[name] {
		return fakeInfo{name: filepath.Base(name)}, nil
	}
	return nil, fs.ErrNotExist
}

func (s *seamFS) MkdirAll(path string, perm fs.FileMode) error {
	s.log = append(s.log, "mkdir "+filepath.ToSlash(path))
	if s.mkdirErr != nil {
		return s.mkdirErr
	}
	s.perm = perm
	for p := path; !s.present[p]; p = filepath.Dir(p) {
		s.present[p] = true
	}
	return nil
}

func (s *seamFS) syncDir(dir string) error {
	s.log = append(s.log, "sync "+filepath.ToSlash(dir))
	return nil
}

// TestMkdirAllSyncedWith covers the composition every durable directory
// creation needs: record the deepest pre-existing ancestor, create, then fsync
// each created directory (deepest first) and finally the pre-existing parent
// whose entry for the topmost new directory must become durable.
func TestMkdirAllSyncedWith(t *testing.T) {
	t.Run("new chain is synced leaf first up to the pre-existing parent", func(t *testing.T) {
		s := newSeamFS("/", "/data")
		if err := MkdirAllSyncedWith(s, s.syncDir, filepath.FromSlash("/data/a/b"), 0o750); err != nil {
			t.Fatalf("MkdirAllSyncedWith: %v", err)
		}
		want := []string{"mkdir /data/a/b", "sync /data/a/b", "sync /data/a", "sync /data"}
		if !reflect.DeepEqual(s.log, want) {
			t.Fatalf("calls %v, want %v", s.log, want)
		}
		if s.perm != 0o750 {
			t.Fatalf("perm %v, want 0750", s.perm)
		}
	})

	t.Run("existing directory syncs only itself", func(t *testing.T) {
		s := newSeamFS("/", "/data", "/data/a")
		if err := MkdirAllSyncedWith(s, s.syncDir, filepath.FromSlash("/data/a"), 0o700); err != nil {
			t.Fatalf("MkdirAllSyncedWith: %v", err)
		}
		want := []string{"mkdir /data/a", "sync /data/a"}
		if !reflect.DeepEqual(s.log, want) {
			t.Fatalf("calls %v, want %v", s.log, want)
		}
	})

	t.Run("mkdir error propagates and nothing is synced", func(t *testing.T) {
		boom := errors.New("mkdir failed")
		s := newSeamFS("/")
		s.mkdirErr = boom
		if err := MkdirAllSyncedWith(s, s.syncDir, filepath.FromSlash("/a"), 0o700); !errors.Is(err, boom) {
			t.Fatalf("got %v, want the mkdir error", err)
		}
		if want := []string{"mkdir /a"}; !reflect.DeepEqual(s.log, want) {
			t.Fatalf("calls %v, want %v", s.log, want)
		}
	})

	t.Run("sync error propagates", func(t *testing.T) {
		boom := errors.New("fsync failed")
		s := newSeamFS("/")
		err := MkdirAllSyncedWith(s, func(string) error { return boom }, filepath.FromSlash("/a"), 0o700)
		if !errors.Is(err, boom) {
			t.Fatalf("got %v, want the sync error", err)
		}
	})
}

// TestMkdirAllSynced_RealFilesystem exercises the os-backed entry points end
// to end: the tree is created with the requested mode and every directory in
// it can be fsynced.
func TestMkdirAllSynced_RealFilesystem(t *testing.T) {
	root := t.TempDir()
	leaf := filepath.Join(root, "a", "b", "c")
	if err := MkdirAllSynced(leaf, 0o750); err != nil {
		t.Fatalf("MkdirAllSynced: %v", err)
	}
	info, err := os.Stat(leaf)
	if err != nil {
		t.Fatalf("stat leaf: %v", err)
	}
	if !info.IsDir() {
		t.Fatalf("%s is not a directory", leaf)
	}
	if got := DeepestExistingAncestor(filepath.Join(leaf, "x", "y")); got != leaf {
		t.Fatalf("DeepestExistingAncestor = %q, want %q", got, leaf)
	}
	if err := MkdirAllSynced(leaf, 0o750); err != nil {
		t.Fatalf("MkdirAllSynced on an existing directory: %v", err)
	}
	if err := SyncDirChain(leaf, root); err != nil {
		t.Fatalf("SyncDirChain: %v", err)
	}
}

// TestSyncDir_MissingDirectory verifies the os-backed SyncDir reports a
// missing directory rather than succeeding.
func TestSyncDir_MissingDirectory(t *testing.T) {
	err := SyncDir(filepath.Join(t.TempDir(), "missing"))
	if !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("got %v, want fs.ErrNotExist", err)
	}
}
