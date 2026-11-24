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
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/jeremyhahn/go-quicraft/pkg/logdb"
)

// StartShard seeds the state machine's applied counter on restart; an unseeded counter re-walks capped session expiry and deletes live sessions.

// startShardRecoveringFromSnapshot seeds a LogDB with a snapshot record at
// index and the matching on-disk artifact, then starts the shard on it. The
// artifact is the minimum RecoverFromSnapshot accepts: a four-byte
// little-endian session count of zero, followed by the (empty) user SM bytes.
func startShardRecoveringFromSnapshot(t *testing.T, h *Host, index uint64) {
	t.Helper()

	snapDir := t.TempDir()
	if err := os.WriteFile(filepath.Join(snapDir, "snapshot.dat"),
		[]byte{0, 0, 0, 0}, 0o600); err != nil {
		t.Fatalf("writing the snapshot artifact: %v", err)
	}
	stampTestSnapshotMeta(t, snapDir, index, 1)
	if err := h.logdb.SaveSnapshot(1, 1, logdb.Snapshot{
		Index: index,
		Term:  1,
		Membership: logdb.Membership{
			ConfigChangeID: 1,
			Addresses:      map[uint64]string{1: "127.0.0.1:5000"},
		},
		Filepath: snapDir,
	}); err != nil {
		t.Fatalf("SaveSnapshot: %v", err)
	}
	if err := h.logdb.SaveState([]logdb.Update{{
		ShardID:   1,
		ReplicaID: 1,
		State:     logdb.State{Term: 1, Vote: 1, Commit: index},
	}}); err != nil {
		t.Fatalf("SaveState: %v", err)
	}
	if err := h.StartShard(map[uint64]string{1: "127.0.0.1:5000"}, false,
		testCreateFunc, testShardConfig(1, 1)); err != nil {
		t.Fatalf("StartShard on a recorded snapshot at index %d: %v", index, err)
	}
}

// TestStartShard_SeedsStateMachineAppliedIndexFromSnapshot checks the state machine's applied counter equals the recovered snapshot index.
func TestStartShard_SeedsStateMachineAppliedIndexFromSnapshot(t *testing.T) {
	const snapshotIndex uint64 = 40

	h := newTestHost(t)
	startShardRecoveringFromSnapshot(t, h, snapshotIndex)
	defer func() { _ = h.StopShard(1) }()

	ns := testLoadNode(h, 1)
	if got := ns.rsm.LastApplied(); got != snapshotIndex {
		t.Fatalf("state machine lastApplied = %d, want %d. The state machine's own "+
			"counter is the lower bound of its session expiry checkpoint walk, so a "+
			"replica resuming with it at %d re-runs every checkpoint in (%d, %d] "+
			"that its peers already ran; expiry is capped per checkpoint, so the "+
			"re-run deletes sessions the shard still holds and this replica then "+
			"rejects entries its peers apply",
			got, snapshotIndex, got, got, snapshotIndex)
	}

	// The two counters must agree, not merely be non-zero.
	engNode := h.engine.GetNode(1)
	if engNode == nil {
		t.Fatal("shard is not loaded in the engine")
	}
	if got := engNode.LastApplied(); got != ns.rsm.LastApplied() {
		t.Fatalf("engine node lastApplied = %d but state machine lastApplied = %d; "+
			"the two markers name different entries, so the first batch the worker "+
			"offers cannot be contiguous for both", got, ns.rsm.LastApplied())
	}
}

// TestStartShard_RestartedShardAppliesAfterSnapshotRecovery checks the restarted shard applies its term-opening no-op.
func TestStartShard_RestartedShardAppliesAfterSnapshotRecovery(t *testing.T) {
	const snapshotIndex uint64 = 40

	h := newTestHost(t)
	startShardRecoveringFromSnapshot(t, h, snapshotIndex)
	defer func() { _ = h.StopShard(1) }()

	waitForLeader(t, h, 1)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if _, err := h.SyncPropose(ctx, 1, []byte("after-restart")); err != nil {
		t.Fatalf("SyncPropose after restarting from a snapshot at index %d = %v; "+
			"the shard cannot apply anything after the restart", snapshotIndex, err)
	}

	ns := testLoadNode(h, 1)
	if got := ns.rsm.LastApplied(); got <= snapshotIndex {
		t.Fatalf("state machine lastApplied = %d after a completed proposal, want "+
			"above the snapshot index %d", got, snapshotIndex)
	}
}
