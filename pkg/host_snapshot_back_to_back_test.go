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
	"testing"
	"time"
)

// TestSyncRequestSnapshot_BackToBackIsNeverInProgress: once SyncRequestSnapshot
// has returned, the snapshot it waited for is over, so a request issued the
// moment it returns must be admitted. It was refused with
// ErrSnapshotInProgress whenever the caller won the race against the pool
// worker, which resolved the caller before releasing the shard's snapshot
// slots. The loop issues requests back to back with nothing in between, so the
// caller wakes into that window as often as the scheduler allows.
func TestSyncRequestSnapshot_BackToBackIsNeverInProgress(t *testing.T) {
	h, err := NewHost(testHostConfig(t), WithMemoryLogDB(), WithoutTransport(), WithoutEncryption())
	if err != nil {
		t.Fatalf("NewHost: %v", err)
	}
	defer h.Close()

	if startErr := h.StartShard(map[uint64]string{1: "127.0.0.1:5000"}, false,
		testCreateFunc, testShardConfig(1, 1)); startErr != nil {
		t.Fatalf("StartShard: %v", startErr)
	}
	waitForLeader(t, h, 1)

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	if _, proposeErr := h.SyncPropose(ctx, 1, []byte("snapshot-me")); proposeErr != nil {
		t.Fatalf("SyncPropose: %v", proposeErr)
	}
	// Every request after the first is a no-op save at an index already
	// captured: the cheapest completion the pool has, which leaves the caller
	// the most room to win the race.
	for i := range 500 {
		if _, snapErr := h.SyncRequestSnapshot(ctx, 1, SnapshotOption{}); snapErr != nil {
			t.Fatalf("SyncRequestSnapshot #%d issued after the previous one returned: %v", i, snapErr)
		}
	}
}
