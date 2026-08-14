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

package engine

import (
	"os"
	"path/filepath"
	"strings"
)

// SyncDir fsyncs a directory so metadata changes within it (file creates,
// deletes, renames) are durable. Without this, a crash on Linux ext4 can
// lose a new file's directory entry even though the file's own data was
// fsynced — leaving e.g. a durably-recorded LogDB snapshot pointing at a
// directory entry that no longer exists.
func SyncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	syncErr := d.Sync()
	closeErr := d.Close()
	if syncErr != nil {
		return syncErr
	}
	return closeErr
}

// SyncDirChain fsyncs leaf and each ancestor directory up to and
// including stopAt. Use after os.MkdirAll(leaf): every directory the call
// may have created needs its parent's entry made durable, so the whole
// chain from the deepest new directory to the pre-existing root must be
// synced. When stopAt is empty or leaf is not under stopAt, only leaf is
// synced.
func SyncDirChain(leaf, stopAt string) error {
	return SyncDirChainWith(SyncDir, leaf, stopAt)
}

// SyncDirChainWith performs SyncDirChain's walk, fsyncing each directory with
// the supplied function instead of SyncDir. It exists so a caller that routes
// its file I/O through its own filesystem abstraction syncs directories
// through that same abstraction; the walk itself must not be duplicated.
//
// Containment is decided by filepath.Rel, NOT by a string prefix. The prefix
// form broke for exactly the two values deepestExistingAncestor returns when
// nothing above the leaf pre-exists: stopAt "/" builds the prefix "//" and
// stopAt "." builds "./", and neither prefixes a CLEANED leaf. Both silently
// degraded to a leaf-only fsync, skipping every ancestor MkdirAll had just
// created — reachable with a relative NodeHostDir (only DataDir must be
// absolute) or with an absolute path whose entire tree is new. Rel handles both
// roots correctly and reports non-containment as a ".." prefix.
//
// The emptiness test runs BEFORE Clean because filepath.Clean("") is ".", which
// is a legitimate stopAt (the working directory) rather than "no stop point".
func SyncDirChainWith(syncDir func(string) error, leaf, stopAt string) error {
	if stopAt == "" {
		return syncDir(filepath.Clean(leaf))
	}
	leaf = filepath.Clean(leaf)
	stopAt = filepath.Clean(stopAt)
	rel, err := filepath.Rel(stopAt, leaf)
	if err != nil || rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
		// Not under stopAt (or the two are not comparable, e.g. one absolute
		// and one relative): sync the leaf alone, which is all that is known
		// to exist along a chain this function cannot walk.
		return syncDir(leaf)
	}
	for dir := leaf; ; dir = filepath.Dir(dir) {
		if err := syncDir(dir); err != nil {
			return err
		}
		if dir == stopAt {
			return nil
		}
	}
}
