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

package quicraft

import (
	"context"
	"errors"
	"testing"

	"github.com/jeremyhahn/go-quicraft/pkg/proto"
)

// These pin the lifetime of the per-shard import lock records. A record was
// installed on the first StartShard, ImportSnapshot or RemoveData of a shard
// ID and never removed, so a long-lived host held one for every shard ID it
// had ever seen — including starts that failed.

// importLockCount reports how many per-shard import lock records the host
// holds.
func importLockCount(h *Host) int {
	return h.importLocks.count()
}

// TestImportLocks_ShardChurnLeavesNoRecords drives every operation that takes
// the import lock — a start, a stop, RemoveData, a start refused because the
// replica is retired — across many shard IDs, and requires the host to hold
// no lock record once they have all returned.
func TestImportLocks_ShardChurnLeavesNoRecords(t *testing.T) {
	h, err := NewHost(testHostConfig(t), WithMemoryLogDB(), WithoutTransport(), WithoutEncryption())
	if err != nil {
		t.Fatalf("NewHost: %v", err)
	}
	defer h.Close()

	const shards = 16
	members := map[uint64]string{1: "127.0.0.1:5000"}
	for shardID := uint64(1); shardID <= shards; shardID++ {
		if err := h.StartShard(members, false, testCreateFunc, testShardConfig(shardID, 1)); err != nil {
			t.Fatalf("StartShard(%d): %v", shardID, err)
		}
		if err := h.StopShard(shardID); err != nil {
			t.Fatalf("StopShard(%d): %v", shardID, err)
		}
		if err := h.RemoveData(shardID, 1); err != nil {
			t.Fatalf("RemoveData(%d): %v", shardID, err)
		}
		// The retired replica is refused under the lock: a failed start.
		err := h.StartShard(members, false, testCreateFunc, testShardConfig(shardID, 1))
		var retired *ReplicaRetiredError
		if !errors.As(err, &retired) {
			t.Fatalf("StartShard(%d) of a retired replica = %v, want ReplicaRetiredError", shardID, err)
		}
	}
	if n := importLockCount(h); n != 0 {
		t.Fatalf("host holds %d import lock records after the churn, want 0", n)
	}
}

// TestImportLocks_FailedImportLeavesNoRecord: an import that took the lock and
// then failed releases its record with the lock.
func TestImportLocks_FailedImportLeavesNoRecord(t *testing.T) {
	f := newImportFixture(t)

	payload := []byte("payload")
	artifact := buildSnapshotArtifact(t, 42, 1, 0, proto.Membership{}, payload)
	// Deliver the header and membership, which are read before the lock, and
	// fail inside the payload, which is read under it.
	r := &failAtReader{data: artifact, limit: len(artifact) - len(payload)}
	err := f.host.ImportSnapshot(context.Background(), f.shardID, r, WithImportDataDir(""))
	if !errors.Is(err, errTestArtifactStream) {
		t.Fatalf("ImportSnapshot = %v, want the stream error", err)
	}
	if n := importLockCount(f.host); n != 0 {
		t.Fatalf("host holds %d import lock records after a failed import, want 0", n)
	}
}

// failAtReader delivers the first limit bytes of data and then fails with
// errTestArtifactStream.
type failAtReader struct {
	data   []byte
	limit  int
	offset int
}

func (r *failAtReader) Read(p []byte) (int, error) {
	if r.offset >= r.limit {
		return 0, errTestArtifactStream
	}
	n := copy(p, r.data[r.offset:r.limit])
	r.offset += n
	return n, nil
}
