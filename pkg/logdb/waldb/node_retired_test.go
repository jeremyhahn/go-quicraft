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

package waldb_test

import (
	"path/filepath"
	"testing"

	"github.com/jeremyhahn/go-quicraft/pkg/logdb"
	"github.com/jeremyhahn/go-quicraft/pkg/logdb/waldb"
)

// assertRetired fails unless db reports exactly the given retirements for
// (shard 1, replica 1) and (shard 2, replica 1).
func assertRetired(t *testing.T, db *waldb.DB, want1, want2 bool) {
	t.Helper()
	for _, tc := range []struct {
		shard uint64
		want  bool
	}{{1, want1}, {2, want2}} {
		got, err := db.NodeRemoved(tc.shard, 1)
		if err != nil {
			t.Fatalf("NodeRemoved(%d, 1): %v", tc.shard, err)
		}
		if got != tc.want {
			t.Fatalf("NodeRemoved(%d, 1) = %v, want %v", tc.shard, got, tc.want)
		}
	}
}

// TestNodeRemoved_RetirementIsDurableAndMigrates verifies RemoveNodeData
// retires the replica ID for good: NodeRemoved reports it after a reopen, and
// after the re-encryption migration, which must carry the tombstone across or
// hand the retired ID back. A replica that is never removed is not reported.
func TestNodeRemoved_RetirementIsDurableAndMigrates(t *testing.T) {
	root := t.TempDir()
	srcDir := filepath.Join(root, "src")
	dstDir := filepath.Join(root, "dst")

	db, err := waldb.Open(srcDir, waldb.WithNoSync(true))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	for _, shard := range []uint64{1, 2} {
		if err := db.SaveState([]logdb.Update{{ShardID: shard, ReplicaID: 1,
			State: logdb.State{Term: 3, Vote: 1, Commit: 0, VoteWindow: 7}}}); err != nil {
			t.Fatalf("SaveState(%d): %v", shard, err)
		}
	}
	assertRetired(t, db, false, false)
	if err := db.RemoveNodeData(1, 1); err != nil {
		t.Fatalf("RemoveNodeData: %v", err)
	}
	assertRetired(t, db, true, false)
	if err := db.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	db, err = waldb.Open(srcDir, waldb.WithNoSync(true))
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	assertRetired(t, db, true, false)
	if err := db.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	if _, err := waldb.Reencrypt(srcDir, nil, dstDir, nil, nil); err != nil {
		t.Fatalf("Reencrypt: %v", err)
	}
	dst, err := waldb.Open(dstDir)
	if err != nil {
		t.Fatalf("Open destination: %v", err)
	}
	defer func() { _ = dst.Close() }()
	assertRetired(t, dst, true, false)
}
