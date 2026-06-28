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
	leaf = filepath.Clean(leaf)
	stopAt = filepath.Clean(stopAt)
	sep := string(filepath.Separator)
	if stopAt == "" || !strings.HasPrefix(leaf+sep, stopAt+sep) {
		return SyncDir(leaf)
	}
	for dir := leaf; ; dir = filepath.Dir(dir) {
		if err := SyncDir(dir); err != nil {
			return err
		}
		if dir == stopAt {
			return nil
		}
	}
}
