package engine

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// TestMkdirStageDir pins MkdirStageDir's contract on its own: a fresh
// directory directly under parent, named prefix + decimal digits + suffix,
// created 0750 under the umask, unique per call, reclaimable by exactly the
// stage matcher of its pattern and invisible to listSnapshotDirs; a missing
// parent is reported, not retried.
func TestMkdirStageDir(t *testing.T) {
	t.Parallel()

	allDigits := func(s string) bool {
		if s == "" {
			return false
		}
		for _, r := range s {
			if r < '0' || r > '9' {
				return false
			}
		}
		return true
	}

	patterns := []struct {
		name    string
		pattern string
		prefix  string
		own     func(string) bool
		other   func(string) bool
	}{
		{"SaveStageDirPattern", SaveStageDirPattern, SaveStageDirPrefix, IsSnapshotSaveStage, IsSnapshotRecvStage},
		{"RecvStageDirPattern", RecvStageDirPattern, RecvStageDirPrefix, IsSnapshotRecvStage, IsSnapshotSaveStage},
	}
	for _, tc := range patterns {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			parent := t.TempDir()
			dir, err := MkdirStageDir(parent, tc.pattern)
			if err != nil {
				t.Fatalf("MkdirStageDir: %v", err)
			}
			if got := filepath.Dir(dir); got != parent {
				t.Fatalf("stage dir parent = %q, want %q", got, parent)
			}
			fi, err := os.Stat(dir)
			if err != nil {
				t.Fatalf("stat stage dir: %v", err)
			}
			if !fi.IsDir() {
				t.Fatalf("%s is not a directory", dir)
			}
			base := filepath.Base(dir)
			if !strings.HasPrefix(base, tc.prefix) || !allDigits(strings.TrimPrefix(base, tc.prefix)) {
				t.Fatalf("stage dir name %q, want %q + decimal digits", base, tc.prefix)
			}
			if !tc.own(base) {
				t.Fatalf("%q is not reclaimed by its own matcher", base)
			}
			if tc.other(base) {
				t.Fatalf("%q is claimed by the other stage's matcher", base)
			}
		})
	}

	t.Run("SuffixKept", func(t *testing.T) {
		t.Parallel()
		dir, err := MkdirStageDir(t.TempDir(), "x-*.tmp")
		if err != nil {
			t.Fatalf("MkdirStageDir: %v", err)
		}
		base := filepath.Base(dir)
		if !strings.HasPrefix(base, "x-") || !strings.HasSuffix(base, ".tmp") {
			t.Fatalf("stage dir name %q, want x-<digits>.tmp", base)
		}
		if mid := strings.TrimSuffix(strings.TrimPrefix(base, "x-"), ".tmp"); !allDigits(mid) {
			t.Fatalf("stage dir name %q: random part %q is not decimal", base, mid)
		}
	})

	t.Run("Mode0750UnderUmask", func(t *testing.T) {
		t.Parallel()
		parent := t.TempDir()
		dir, err := MkdirStageDir(parent, SaveStageDirPattern)
		if err != nil {
			t.Fatalf("MkdirStageDir: %v", err)
		}
		fi, err := os.Stat(dir)
		if err != nil {
			t.Fatalf("stat stage dir: %v", err)
		}
		// "0750 under the umask": compare with a sibling made the same way
		// under the same umask, not a literal 0750 a restrictive umask breaks.
		ref := filepath.Join(parent, "reference")
		if err := os.Mkdir(ref, 0o750); err != nil {
			t.Fatalf("mkdir reference: %v", err)
		}
		want, err := os.Stat(ref)
		if err != nil {
			t.Fatalf("stat reference: %v", err)
		}
		if got := fi.Mode().Perm(); got != want.Mode().Perm() {
			t.Fatalf("stage dir mode = %o, want %o (0750 under the umask)", got, want.Mode().Perm())
		}
		if fi.Mode().Perm() == 0o700 && want.Mode().Perm() != 0o700 {
			t.Fatal("stage dir has os.MkdirTemp's 0700")
		}
	})

	t.Run("UniquePerCall", func(t *testing.T) {
		t.Parallel()
		parent := t.TempDir()
		const n = 200
		seen := make(map[string]struct{}, n)
		for i := range n {
			dir, err := MkdirStageDir(parent, RecvStageDirPattern)
			if err != nil {
				t.Fatalf("call %d: MkdirStageDir: %v", i, err)
			}
			if _, dup := seen[dir]; dup {
				t.Fatalf("call %d returned %q twice", i, dir)
			}
			seen[dir] = struct{}{}
		}
		entries, err := os.ReadDir(parent)
		if err != nil {
			t.Fatalf("read parent: %v", err)
		}
		if len(entries) != n {
			t.Fatalf("parent holds %d entries, want %d", len(entries), n)
		}
		for dir := range seen {
			if fi, err := os.Stat(dir); err != nil || !fi.IsDir() {
				t.Fatalf("returned dir %q does not exist as a directory: %v", dir, err)
			}
		}
	})

	t.Run("MissingParent", func(t *testing.T) {
		t.Parallel()
		root := t.TempDir()
		parent := filepath.Join(root, "missing")
		dir, err := MkdirStageDir(parent, SaveStageDirPattern)
		if err == nil {
			t.Fatalf("MkdirStageDir under a missing parent returned %q, want an error", dir)
		}
		if !os.IsNotExist(err) {
			t.Fatalf("error %v: os.IsNotExist = false, want true", err)
		}
		if dir != "" {
			t.Fatalf("returned dir %q with an error, want empty", dir)
		}
		if _, statErr := os.Stat(parent); !os.IsNotExist(statErr) {
			t.Fatalf("missing parent was created: stat err = %v", statErr)
		}
		entries, err := os.ReadDir(root)
		if err != nil {
			t.Fatalf("read root: %v", err)
		}
		if len(entries) != 0 {
			t.Fatalf("root holds %d entries after a failed call, want 0", len(entries))
		}
	})

	t.Run("InvisibleToListSnapshotDirs", func(t *testing.T) {
		t.Parallel()
		baseDir := t.TempDir()
		const shardID, replicaID = 7, 3
		base := snapshotBaseDir(baseDir, shardID, replicaID)
		if err := os.MkdirAll(base, 0o750); err != nil {
			t.Fatalf("mkdir snapshot base: %v", err)
		}
		for _, pattern := range []string{SaveStageDirPattern, RecvStageDirPattern} {
			for range 3 {
				if _, err := MkdirStageDir(base, pattern); err != nil {
					t.Fatalf("MkdirStageDir(%q): %v", pattern, err)
				}
			}
		}
		dirs, err := listSnapshotDirs(baseDir, shardID, replicaID)
		if err != nil {
			t.Fatalf("listSnapshotDirs: %v", err)
		}
		if len(dirs) != 0 {
			t.Fatalf("listSnapshotDirs listed %d stage dirs, want none: %+v", len(dirs), dirs)
		}
	})
}
