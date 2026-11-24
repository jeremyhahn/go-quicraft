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

package waldb

import (
	"path/filepath"
	"testing"
)

// TestCreateWALRoot_EntirelyNewTreeSurvivesCrash is the regression test for
// the WAL root's chain sync degrading to a LEAF-ONLY fsync when nothing above
// the root pre-exists but the filesystem root itself.
//
// The deepest pre-existing ancestor is then "/", and a string-prefix
// containment test over leaf+sep and stopAt+sep builds the prefix "//", which
// no cleaned leaf carries. The walk fell through to syncing the leaf alone, so
// the dirents of every directory MkdirAll had just created were never made
// durable and a crash took the whole WAL tree with it.
func TestCreateWALRoot_EntirelyNewTreeSurvivesCrash(t *testing.T) {
	root := string(filepath.Separator)
	dir := filepath.Join(root, "wal", "a", "b")

	cfs := newCrashFS(root)
	if err := createWALRoot(dbOptions{fs: cfs}, dir); err != nil {
		t.Fatalf("createWALRoot: %v", err)
	}
	cfs.Crash()

	for p := dir; p != root; p = filepath.Dir(p) {
		if _, err := cfs.Stat(p); err != nil {
			t.Fatalf("%s lost in the crash (%v): every directory createWALRoot "+
				"created needs its parent fsynced", p, err)
		}
	}
}
