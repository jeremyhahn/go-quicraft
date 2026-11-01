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

//go:build integration

package host_test

import (
	"context"
	"fmt"
	"testing"

	quicraft "github.com/jeremyhahn/go-quicraft/pkg"
)

// TestHostAPI_SyncRead_ThreeNode writes via leader then reads back with
// linearizable SyncRead.
func TestHostAPI_SyncRead_ThreeNode(t *testing.T) {
	cluster := newTestCluster(t, 3)
	defer cluster.close()

	trackers := cluster.startShard(1)
	leaderIdx := cluster.waitForClusterLeader(1, leaderElectionTimeout)

	proposeToLeader(t, cluster, 1, encodeKVPut("sr-key", "sr-value"), defaultTimeout)

	leaderSM := trackers[leaderIdx].Get(1, uint64(leaderIdx+1))
	waitForCondition(t, defaultTimeout, "sr-key applied on leader", func() bool {
		_, ok := leaderSM.Get("sr-key")
		return ok
	})

	result := readFromLeader(t, cluster, 1, "sr-key", defaultTimeout)
	val, ok := result.(string)
	if !ok {
		t.Fatalf("SyncRead returned unexpected type: %T", result)
	}
	if val != "sr-value" {
		t.Errorf("SyncRead = %q, want %q", val, "sr-value")
	}
}

// TestHostAPI_QueryLocalNode_ThreeNode does a local query after replication.
func TestHostAPI_QueryLocalNode_ThreeNode(t *testing.T) {
	cluster := newTestCluster(t, 3)
	defer cluster.close()

	trackers := cluster.startShard(1)
	leaderIdx := cluster.waitForClusterLeader(1, leaderElectionTimeout)
	leaderHost := cluster.hosts[leaderIdx]

	ctx, cancel := context.WithTimeout(context.Background(), defaultTimeout)
	defer cancel()

	proposeToLeader(t, cluster, 1, encodeKVPut("local-key", "local-val"), defaultTimeout)

	leaderSM := trackers[leaderIdx].Get(1, uint64(leaderIdx+1))
	waitForCondition(t, defaultTimeout, "local-key applied", func() bool {
		_, ok := leaderSM.Get("local-key")
		return ok
	})

	result, err := leaderHost.QueryLocalNode(ctx, 1, "local-key")
	if err != nil {
		t.Fatalf("QueryLocalNode failed: %v", err)
	}
	if val, ok := result.(string); !ok || val != "local-val" {
		t.Errorf("QueryLocalNode = %v, want 'local-val'", result)
	}
}

// TestHostAPI_Session_Lifecycle_ThreeNode creates a session on leader, uses it, closes it.
func TestHostAPI_Session_Lifecycle_ThreeNode(t *testing.T) {
	cluster := newTestCluster(t, 3)
	defer cluster.close()

	cluster.startShard(1)
	leaderIdx := cluster.waitForClusterLeader(1, leaderElectionTimeout)
	leaderHost := cluster.hosts[leaderIdx]

	ctx, cancel := context.WithTimeout(context.Background(), defaultTimeout)
	defer cancel()

	session, err := leaderHost.NewSession(ctx, 1)
	if err != nil {
		t.Fatalf("NewSession failed: %v", err)
	}
	if session.ClientID() == 0 {
		t.Error("session ClientID is 0")
	}

	rs, err := leaderHost.CloseSession(ctx, 1, session)
	if err != nil {
		t.Fatalf("CloseSession failed: %v", err)
	}
	rs.Release()
}

// TestHostAPI_Snapshot_LeaderRequest requests a snapshot on the leader and
// verifies a snapshot actually happened: the request resolves with a non-zero
// index, a snapshot artifact for that index exists on disk, and a second
// request after more writes reports a HIGHER index.
//
// The previous version released the RequestState without ever reading its
// result. RequestSnapshot is asynchronous, so the test neither waited for the
// snapshot nor noticed a failed or skipped one — an engine that never wrote an
// artifact passed.
func TestHostAPI_Snapshot_LeaderRequest(t *testing.T) {
	cluster := newTestCluster(t, 3)
	defer cluster.close()

	trackers := cluster.startShard(1)
	leaderIdx := cluster.waitForClusterLeader(1, leaderElectionTimeout)
	leaderHost := cluster.hosts[leaderIdx]

	// Write some data first.
	for i := 0; i < 5; i++ {
		proposeToLeader(t, cluster, 1, encodeKVPut(
			fmt.Sprintf("snap-key-%d", i),
			fmt.Sprintf("snap-val-%d", i),
		), defaultTimeout)
	}

	leaderSM := trackers[leaderIdx].Get(1, uint64(leaderIdx+1))
	waitForCondition(t, defaultTimeout, "5 entries applied", func() bool {
		return leaderSM.Len() >= 5
	})

	// This shard uses testShardConfig, whose SnapshotEntries stays 0 —
	// config.SetDefaults never assigns it — so the engine takes NO automatic
	// snapshot here and every artifact below comes from an explicit request.
	snapshotDir := cluster.nodeHostDirs[leaderIdx]
	firstIndex := requestSnapshotIndex(t, leaderHost, 1)
	assertSnapshotArtifact(t, snapshotDir, firstIndex, defaultTimeout)
	t.Logf("first snapshot taken at index %d", firstIndex)

	// Write more entries and snapshot again: the second snapshot must cover
	// the new entries, which is only true if a snapshot really happened.
	for i := 5; i < 10; i++ {
		proposeToLeader(t, cluster, 1, encodeKVPut(
			fmt.Sprintf("snap-key-%d", i),
			fmt.Sprintf("snap-val-%d", i),
		), defaultTimeout)
	}
	waitForCondition(t, defaultTimeout, "10 entries applied", func() bool {
		return leaderSM.Len() >= 10
	})

	secondIndex := requestSnapshotIndex(t, leaderHost, 1)
	if secondIndex <= firstIndex {
		t.Fatalf("second snapshot index = %d, want > %d (the snapshot did not advance)",
			secondIndex, firstIndex)
	}
	assertSnapshotArtifact(t, snapshotDir, secondIndex, defaultTimeout)
	t.Logf("second snapshot taken at index %d", secondIndex)
}

// TestHostAPI_ReadIndex_ThreeNode verifies that a ReadIndex issued to the
// leader resolves to a confirmed index.
//
// The previous version submitted the ReadIndex to a pinned host and released
// the RequestState without reading it, which asserted only that a request was
// accepted: a not-leader outcome, or a read index that never resolved, would
// have surfaced in the result it never read.
//
// The warmup write is not there for readiness. A leader refuses ReadIndex with
// ErrReadIndexNotReady until it commits an entry in its own term, but
// readIndexFromLeader goes through proposer.ReadIndex, which retries that
// refusal (go-quicraft#275). The write gives the index == 0 assertion a floor
// the test committed itself: a confirmed index must cover it.
func TestHostAPI_ReadIndex_ThreeNode(t *testing.T) {
	cluster := newTestCluster(t, 3)
	defer cluster.close()

	cluster.startShard(1)
	cluster.waitForClusterLeader(1, leaderElectionTimeout)
	proposeToLeader(t, cluster, 1, encodeKVPut("ri-warmup", "ok"), defaultTimeout)

	index := readIndexFromLeader(t, cluster, 1, defaultTimeout)
	if index == 0 {
		t.Fatal("ReadIndex resolved to index 0 after a committed write; the " +
			"confirmed index must be at least the warmup entry's")
	}
}

// TestHostAPI_LeaderTransfer_ThreeNode requests leadership transfer to a
// specific target and verifies that leadership actually moved to THAT target.
//
// The previous version only checked that RequestLeaderTransfer returned nil
// and that ShardInfo still reported ShardID == 1 — a value the test itself
// passed in, so it could never be wrong. An engine that ignored the requested
// target, or never transferred at all, passed.
func TestHostAPI_LeaderTransfer_ThreeNode(t *testing.T) {
	cluster := newTestCluster(t, 3)
	defer cluster.close()

	trackers := cluster.startShard(1)
	leaderIdx := cluster.waitForClusterLeader(1, leaderElectionTimeout)
	leaderHost := cluster.hosts[leaderIdx]

	// Wait for a follower to discover the leader before transferring.
	targetIdx := cluster.waitForFollower(1, leaderElectionTimeout)
	targetReplicaID := uint64(targetIdx + 1)

	// A transfer target must have an up-to-date log before the leader will
	// hand over, so replicate one entry everywhere first.
	proposeToLeader(t, cluster, 1, encodeKVPut("lt-warmup", "ok"), defaultTimeout)
	waitForReplication(t, cluster, trackers, 1, "lt-warmup", "ok", defaultTimeout)

	transferLeadershipTo(t, cluster, 1, targetIdx, failoverTimeout)

	// Leadership must be held by the REQUESTED replica, not merely by
	// someone.
	targetInfo, err := cluster.hosts[targetIdx].ShardInfo(1)
	if err != nil {
		t.Fatalf("ShardInfo on transfer target node[%d] failed: %v", targetIdx, err)
	}
	if !targetInfo.IsLeader {
		t.Fatalf("transfer target node[%d] is not the leader after transfer", targetIdx)
	}
	if targetInfo.ReplicaID != targetReplicaID {
		t.Fatalf("transfer target node[%d] ReplicaID = %d, want %d",
			targetIdx, targetInfo.ReplicaID, targetReplicaID)
	}

	// Every live node must agree that the requested replica leads the shard.
	waitForCondition(t, failoverTimeout,
		fmt.Sprintf("all nodes report replica %d as leader", targetReplicaID),
		func() bool {
			for _, i := range cluster.liveNodeIndexes() {
				leaderID, known, leaderErr := cluster.hosts[i].LeaderID(1)
				if leaderErr != nil || !known || leaderID != targetReplicaID {
					return false
				}
			}
			return true
		},
	)

	// The original leader is still queryable and still reports the shard.
	info, err := leaderHost.ShardInfo(1)
	if err != nil {
		t.Fatalf("ShardInfo after transfer request failed: %v", err)
	}
	if info.ShardID != 1 {
		t.Errorf("ShardID = %d, want 1", info.ShardID)
	}
}

// TestHostAPI_GetShardInfo_AllNodes verifies all nodes return correct shard info.
func TestHostAPI_GetShardInfo_AllNodes(t *testing.T) {
	cluster := newTestCluster(t, 3)
	defer cluster.close()

	cluster.startShard(1)
	cluster.waitForClusterLeader(1, leaderElectionTimeout)

	// Wait for all nodes to learn the term (followers may lag behind the
	// leader's heartbeat). This avoids races where a follower still reports
	// Term=0 right after the leader is elected.
	for i, h := range cluster.hosts {
		idx := i
		host := h
		waitForCondition(t, leaderElectionTimeout,
			fmt.Sprintf("node[%d] learns term", idx),
			func() bool {
				info, err := host.ShardInfo(1)
				return err == nil && info.Term > 0
			},
		)
	}

	for i, h := range cluster.hosts {
		info, err := h.ShardInfo(1)
		if err != nil {
			t.Fatalf("node[%d] ShardInfo failed: %v", i, err)
		}
		if info.ShardID != 1 {
			t.Errorf("node[%d] ShardID = %d, want 1", i, info.ShardID)
		}
		expectedReplicaID := uint64(i + 1)
		if info.ReplicaID != expectedReplicaID {
			t.Errorf("node[%d] ReplicaID = %d, want %d", i, info.ReplicaID, expectedReplicaID)
		}
		if info.Term == 0 {
			t.Errorf("node[%d] Term is 0", i)
		}
	}
}

// TestHostAPI_GetNodeHostInfo_AllNodes verifies all nodes return host info.
func TestHostAPI_GetNodeHostInfo_AllNodes(t *testing.T) {
	cluster := newTestCluster(t, 3)
	defer cluster.close()

	cluster.startShard(1)
	cluster.waitForClusterLeader(1, leaderElectionTimeout)

	for i, h := range cluster.hosts {
		info := h.NodeHostInfo(quicraft.NodeHostInfoOption{SkipLogInfo: true})
		if info == nil {
			t.Fatalf("node[%d] NodeHostInfo returned nil", i)
		}
		shardCount := len(info.ShardInfoList)
		if shardCount != 1 {
			t.Errorf("node[%d] ShardCount = %d, want 1", i, shardCount)
		}
		if info.RaftAddress != cluster.addrs[i] {
			t.Errorf("node[%d] RaftAddress = %q, want %q", i, info.RaftAddress, cluster.addrs[i])
		}
	}
}

// TestHostAPI_MultipleShards_ThreeNode runs 3 independent shards on the cluster.
func TestHostAPI_MultipleShards_ThreeNode(t *testing.T) {
	cluster := newTestCluster(t, 3)
	defer cluster.close()

	// Start 3 shards and collect trackers per shard. After each shard
	// elects a leader, perform a warmup proposal to ensure all QUIC
	// connections are fully established before starting the next shard.
	shardTrackers := make(map[uint64][]*smTracker)
	for shardID := uint64(1); shardID <= 3; shardID++ {
		trackers := cluster.startShard(shardID)
		shardTrackers[shardID] = trackers
		cluster.waitForClusterLeader(shardID, leaderElectionTimeout)

		// Warmup: propose and verify replication to establish QUIC
		// connections between all nodes for this shard.
		warmupKey := fmt.Sprintf("warmup-shard%d", shardID)
		proposeToLeader(t, cluster, shardID,
			encodeKVPut(warmupKey, "warmup"), defaultTimeout)
		waitForReplication(t, cluster, trackers, shardID, warmupKey, "warmup", defaultTimeout)
	}

	// Write to each shard via its leader.
	for shardID := uint64(1); shardID <= 3; shardID++ {
		key := fmt.Sprintf("shard%d-key", shardID)
		val := fmt.Sprintf("shard%d-val", shardID)
		proposeToLeader(t, cluster, shardID, encodeKVPut(key, val), defaultTimeout)
	}

	// Verify host info shows all 3 shards.
	for i, h := range cluster.hosts {
		info := h.NodeHostInfo(quicraft.NodeHostInfoOption{SkipLogInfo: true})
		if info == nil {
			t.Fatalf("node[%d] NodeHostInfo returned nil", i)
		}
		shardCount := len(info.ShardInfoList)
		if shardCount != 3 {
			t.Errorf("node[%d] ShardCount = %d, want 3", i, shardCount)
		}
	}
}

// TestHostAPI_ProposeToFollower_Forwarded verifies that proposing to a
// follower succeeds via automatic leader forwarding.
func TestHostAPI_ProposeToFollower_Forwarded(t *testing.T) {
	cluster := newTestCluster(t, 3)
	defer cluster.close()

	trackers := cluster.startShard(1)
	cluster.waitForClusterLeader(1, leaderElectionTimeout)

	// Wait for a follower that knows the leader so it can forward proposals.
	followerIdx := cluster.waitForFollower(1, leaderElectionTimeout)

	ctx, cancel := context.WithTimeout(context.Background(), defaultTimeout)
	defer cancel()

	// Propose via follower — this should succeed via leader forwarding.
	//quicraft:direct-propose the FOLLOWER is the subject: this asserts leader forwarding
	_, err := cluster.hosts[followerIdx].SyncPropose(ctx, 1, encodeKVPut("fwd-key", "fwd-val"))
	if err != nil {
		t.Fatalf("SyncPropose via follower failed: %v", err)
	}

	// Verify the value was committed and replicated.
	waitForReplication(t, cluster, trackers, 1, "fwd-key", "fwd-val", defaultTimeout)
}
