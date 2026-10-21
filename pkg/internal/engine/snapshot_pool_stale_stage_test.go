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
	"testing"
)

func TestIsSnapshotRecvStage(t *testing.T) {
	for name, want := range map[string]bool{
		`.recv-0123456789abcdef`: true,
		"snapshot-1":             false,
		".recv":                  false,
	} {
		if got := IsSnapshotRecvStage(name); got != want {
			t.Errorf("IsSnapshotRecvStage(%q) = %v, want %v", name, got, want)
		}
	}
}

// TestSweepStaleRecvStageDirs removes the orphaned receive staging directory
// and leaves everything else in the shard's snapshot tree in place.
func TestSweepStaleRecvStageDirs(t *testing.T) {
	base := t.TempDir()
	shardDir := filepath.Join(base, "shard-1", "replica-1")
	if err := os.MkdirAll(filepath.Join(shardDir, ".recv-deadbeef"), 0o750); err != nil {
		t.Fatalf("MkdirAll: %v", err)
	}
	if err := os.WriteFile(filepath.Join(shardDir, ".recv-deadbeef", "stale.bin"), []byte("x"), 0o600); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}
	if err := os.MkdirAll(filepath.Join(shardDir, "snapshot-0000000000000001"), 0o750); err != nil {
		t.Fatalf("MkdirAll: %v", err)
	}
	if err := os.WriteFile(filepath.Join(shardDir, "snapshot.meta"), []byte("m"), 0o600); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	SweepStaleRecvStageDirs(base, 1, 1)

	if _, err := os.Stat(filepath.Join(shardDir, ".recv-deadbeef")); !os.IsNotExist(err) {
		t.Fatalf(".recv-deadbeef survived the sweep (err %v)", err)
	}
	for _, keep := range []string{"snapshot-0000000000000001", "snapshot.meta"} {
		if _, err := os.Stat(filepath.Join(shardDir, keep)); err != nil {
			t.Errorf("%s was swept but must be kept: %v", keep, err)
		}
	}
}

// TestSweepStageDirsReadFails tolerates a snapshot tree it cannot read: a
// non-directory on the path yields a path error that is not ErrNotExist, so
// the sweep must note the failure and stop, not panic.
func TestSweepStageDirsReadFails(t *testing.T) {
	base := t.TempDir()
	if err := os.WriteFile(filepath.Join(base, "shard-1"), []byte("not a dir"), 0o600); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}
	SweepStaleRecvStageDirs(base, 1, 1)
}

// TestMkdirStageDirMissingParent reports the path error instead of looping:
// the parent never exists, so no random name can succeed.
func TestMkdirStageDirMissingParent(t *testing.T) {
	base := t.TempDir()
	_, err := MkdirStageDir(filepath.Join(base, "does-not-exist"), ".recv-*")
	if err == nil || os.IsExist(err) {
		t.Fatalf("MkdirStageDir with a missing parent err = %v, want a non-Exist error", err)
	}
}

func TestMinNonZero(t *testing.T) {
	cases := [][3]uint64{
		{0, 0, 0},
		{7, 0, 7},
		{0, 9, 9},
		{3, 5, 3},
		{5, 3, 3},
	}
	for _, c := range cases {
		if got := minNonZero(c[0], c[1]); got != c[2] {
			t.Errorf("minNonZero(%d, %d) = %d, want %d", c[0], c[1], got, c[2])
		}
	}
}
