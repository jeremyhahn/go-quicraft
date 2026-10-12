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
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	quicraft "github.com/jeremyhahn/go-quicraft/pkg"
)

// TestHostAPI_SyncPropose_ThreeNode proposes to the leader and verifies
// the value is replicated to all nodes.
func TestHostAPI_SyncPropose_ThreeNode(t *testing.T) {
	cluster := newTestCluster(t, 3)
	defer cluster.close()

	trackers := cluster.startShard(1)
	cluster.waitForClusterLeader(1, leaderElectionTimeout)

	result := proposeToLeader(t, cluster, 1, encodeKVPut("sp-key", "sp-value"), defaultTimeout)
	if result.Value == 0 {
		t.Error("SyncPropose result.Value is 0")
	}

	waitForReplication(t, cluster, trackers, 1, "sp-key", "sp-value", defaultTimeout)
}

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

// TestHostAPI_StaleRead_FromFollower writes on leader, stale-reads from follower.
func TestHostAPI_StaleRead_FromFollower(t *testing.T) {
	cluster := newTestCluster(t, 3)
	defer cluster.close()

	trackers := cluster.startShard(1)
	cluster.waitForClusterLeader(1, leaderElectionTimeout)

	ctx, cancel := context.WithTimeout(context.Background(), defaultTimeout)
	defer cancel()

	proposeToLeader(t, cluster, 1, encodeKVPut("stale-key", "stale-val"), defaultTimeout)

	followerIdx := cluster.findFollower(1)
	if followerIdx < 0 {
		t.Fatal("no follower found")
	}

	followerSM := trackers[followerIdx].Get(1, uint64(followerIdx+1))
	waitForCondition(t, defaultTimeout, "follower replicates stale-key", func() bool {
		_, ok := followerSM.Get("stale-key")
		return ok
	})

	result, err := cluster.hosts[followerIdx].StaleRead(ctx, 1, "stale-key")
	if err != nil {
		t.Fatalf("StaleRead failed: %v", err)
	}
	if val, ok := result.(string); !ok || val != "stale-val" {
		t.Errorf("StaleRead = %v, want 'stale-val'", result)
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

	session, err := leaderHost.GetNewSession(ctx, 1)
	if err != nil {
		t.Fatalf("GetNewSession failed: %v", err)
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
// and that GetShardInfo still reported ShardID == 1 — a value the test itself
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
	targetInfo, err := cluster.hosts[targetIdx].GetShardInfo(1)
	if err != nil {
		t.Fatalf("GetShardInfo on transfer target node[%d] failed: %v", targetIdx, err)
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
				leaderID, known, leaderErr := cluster.hosts[i].GetLeaderID(1)
				if leaderErr != nil || !known || leaderID != targetReplicaID {
					return false
				}
			}
			return true
		},
	)

	// The original leader is still queryable and still reports the shard.
	info, err := leaderHost.GetShardInfo(1)
	if err != nil {
		t.Fatalf("GetShardInfo after transfer request failed: %v", err)
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
				info, err := host.GetShardInfo(1)
				return err == nil && info.Term > 0
			},
		)
	}

	for i, h := range cluster.hosts {
		info, err := h.GetShardInfo(1)
		if err != nil {
			t.Fatalf("node[%d] GetShardInfo failed: %v", i, err)
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
		info := h.GetNodeHostInfo(quicraft.NodeHostInfoOption{SkipLogInfo: true})
		if info == nil {
			t.Fatalf("node[%d] GetNodeHostInfo returned nil", i)
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
		info := h.GetNodeHostInfo(quicraft.NodeHostInfoOption{SkipLogInfo: true})
		if info == nil {
			t.Fatalf("node[%d] GetNodeHostInfo returned nil", i)
		}
		shardCount := len(info.ShardInfoList)
		if shardCount != 3 {
			t.Errorf("node[%d] ShardCount = %d, want 3", i, shardCount)
		}
	}
}

// TestHostAPI_ConcurrentProposals_ThreeNode sends proposals from 5 goroutines
// and verifies that nearly all of them succeed and that every value that was
// accepted is present on every node.
//
// The previous version's only floor was "at least one proposal succeeded" and
// its only replication check was Len() >= successCount, so a run in which 99
// of 100 proposals failed passed: one success, and every node's Len() is >= 1.
func TestHostAPI_ConcurrentProposals_ThreeNode(t *testing.T) {
	cluster := newTestCluster(t, 3)
	defer cluster.close()

	trackers := cluster.startShard(1)
	leaderIdx := cluster.waitForClusterLeader(1, leaderElectionTimeout)

	numGoroutines := 5
	proposalsPerGoroutine := 20
	totalProposals := numGoroutines * proposalsPerGoroutine
	// A stable 3-node cluster with a live leader must accept essentially
	// every proposal; the 10% allowance covers a leadership change under a
	// loaded CI host, not a broken propose path.
	minSuccess := int64(totalProposals * 9 / 10)

	var wg sync.WaitGroup
	var successCount atomic.Int64
	var writtenMu sync.Mutex
	written := make(map[string]string, totalProposals)
	var proposeErrs []error

	for g := 0; g < numGoroutines; g++ {
		wg.Add(1)
		go func(gID int) {
			defer wg.Done()
			for p := 0; p < proposalsPerGoroutine; p++ {
				key := fmt.Sprintf("conc-g%d-p%d", gID, p)
				val := fmt.Sprintf("v-%d-%d", gID, p)
				_, err := tryProposeToLeader(cluster, 1, encodeKVPut(key, val), defaultTimeout)
				if err != nil {
					// Collected, not asserted here: this runs on a
					// non-test goroutine, where t.Fatal would only stop
					// this goroutine and let the test carry on.
					writtenMu.Lock()
					proposeErrs = append(proposeErrs, fmt.Errorf("propose %q: %w", key, err))
					writtenMu.Unlock()
					continue
				}
				successCount.Add(1)
				writtenMu.Lock()
				written[key] = val
				writtenMu.Unlock()
			}
		}(g)
	}

	wg.Wait()
	t.Logf("concurrent proposals: %d/%d succeeded", successCount.Load(), totalProposals)

	for _, err := range proposeErrs {
		t.Logf("proposal error: %v", err)
	}
	if got := successCount.Load(); got < minSuccess {
		t.Fatalf("only %d/%d proposals succeeded, want at least %d",
			got, totalProposals, minSuccess)
	}

	// Verify replication to all followers.
	leaderSM := trackers[leaderIdx].Get(1, uint64(leaderIdx+1))
	waitForCondition(t, defaultTimeout, "leader SM apply all", func() bool {
		return int64(leaderSM.Len()) >= successCount.Load()
	})

	// Every accepted key must carry its own value on every node — a count of
	// applied entries says nothing about WHICH entries arrived.
	verifyAllData(t, cluster, trackers, 1, written)
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

// TestHostAPI_LargeValue_Replication replicates a 64KB value to all nodes.
func TestHostAPI_LargeValue_Replication(t *testing.T) {
	cluster := newTestCluster(t, 3)
	defer cluster.close()

	trackers := cluster.startShard(1)
	cluster.waitForClusterLeader(1, leaderElectionTimeout)

	// Create a 64KB value.
	largeVal := strings.Repeat("X", 64*1024)
	proposeToLeader(t, cluster, 1, encodeKVPut("large-key", largeVal), defaultTimeout)

	// Verify replication. A node whose state machine has not registered is
	// waited for, never skipped: skipping it drops it from the assertion.
	for _, i := range cluster.liveNodeIndexes() {
		nodeSM := waitForStateMachine(t, cluster, trackers, 1, i, defaultTimeout)
		waitForCondition(t, defaultTimeout,
			fmt.Sprintf("node[%d] replicates large value", i),
			func() bool {
				val, ok := nodeSM.Get("large-key")
				return ok && len(val) == 64*1024
			},
		)
	}
}

// ---------------------------------------------------------------------------
// 5-Node Cluster Tests
// ---------------------------------------------------------------------------

// TestHostAPI_SyncPropose_FiveNode proposes to the leader of a 5-node cluster
// and verifies the value is replicated to all nodes.
func TestHostAPI_SyncPropose_FiveNode(t *testing.T) {
	cluster := newTestCluster(t, 5)
	defer cluster.close()

	trackers := cluster.startShard(1)
	cluster.waitForClusterLeader(1, leaderElectionTimeout)

	result := proposeToLeader(t, cluster, 1, encodeKVPut("sp5-key", "sp5-value"), defaultTimeout)
	if result.Value == 0 {
		t.Error("SyncPropose result.Value is 0")
	}

	waitForReplication(t, cluster, trackers, 1, "sp5-key", "sp5-value", defaultTimeout)
}

// TestHostAPI_SyncRead_FiveNode writes via leader then reads back with
// linearizable SyncRead on a 5-node cluster.
func TestHostAPI_SyncRead_FiveNode(t *testing.T) {
	cluster := newTestCluster(t, 5)
	defer cluster.close()

	trackers := cluster.startShard(1)
	leaderIdx := cluster.waitForClusterLeader(1, leaderElectionTimeout)

	proposeToLeader(t, cluster, 1, encodeKVPut("sr5-key", "sr5-value"), defaultTimeout)

	leaderSM := trackers[leaderIdx].Get(1, uint64(leaderIdx+1))
	waitForCondition(t, defaultTimeout, "sr5-key applied on leader", func() bool {
		_, ok := leaderSM.Get("sr5-key")
		return ok
	})

	result := readFromLeader(t, cluster, 1, "sr5-key", defaultTimeout)
	val, ok := result.(string)
	if !ok {
		t.Fatalf("SyncRead returned unexpected type: %T", result)
	}
	if val != "sr5-value" {
		t.Errorf("SyncRead = %q, want %q", val, "sr5-value")
	}
}

// TestHostAPI_StaleRead_FiveNode writes on leader, stale-reads from a follower
// in a 5-node cluster.
func TestHostAPI_StaleRead_FiveNode(t *testing.T) {
	cluster := newTestCluster(t, 5)
	defer cluster.close()

	trackers := cluster.startShard(1)
	cluster.waitForClusterLeader(1, leaderElectionTimeout)

	ctx, cancel := context.WithTimeout(context.Background(), defaultTimeout)
	defer cancel()

	proposeToLeader(t, cluster, 1, encodeKVPut("stale5-key", "stale5-val"), defaultTimeout)

	followerIdx := cluster.findFollower(1)
	if followerIdx < 0 {
		t.Fatal("no follower found")
	}

	followerSM := trackers[followerIdx].Get(1, uint64(followerIdx+1))
	waitForCondition(t, defaultTimeout, "follower replicates stale5-key", func() bool {
		_, ok := followerSM.Get("stale5-key")
		return ok
	})

	result, err := cluster.hosts[followerIdx].StaleRead(ctx, 1, "stale5-key")
	if err != nil {
		t.Fatalf("StaleRead failed: %v", err)
	}
	if val, ok := result.(string); !ok || val != "stale5-val" {
		t.Errorf("StaleRead = %v, want 'stale5-val'", result)
	}
}

// TestHostAPI_QueryLocalNode_FiveNode does a local query on the leader after
// replication in a 5-node cluster.
func TestHostAPI_QueryLocalNode_FiveNode(t *testing.T) {
	cluster := newTestCluster(t, 5)
	defer cluster.close()

	trackers := cluster.startShard(1)
	leaderIdx := cluster.waitForClusterLeader(1, leaderElectionTimeout)
	leaderHost := cluster.hosts[leaderIdx]

	ctx, cancel := context.WithTimeout(context.Background(), defaultTimeout)
	defer cancel()

	proposeToLeader(t, cluster, 1, encodeKVPut("local5-key", "local5-val"), defaultTimeout)

	leaderSM := trackers[leaderIdx].Get(1, uint64(leaderIdx+1))
	waitForCondition(t, defaultTimeout, "local5-key applied", func() bool {
		_, ok := leaderSM.Get("local5-key")
		return ok
	})

	result, err := leaderHost.QueryLocalNode(ctx, 1, "local5-key")
	if err != nil {
		t.Fatalf("QueryLocalNode failed: %v", err)
	}
	if val, ok := result.(string); !ok || val != "local5-val" {
		t.Errorf("QueryLocalNode = %v, want 'local5-val'", result)
	}
}

// TestHostAPI_Session_Lifecycle_FiveNode creates a session on the leader,
// uses it, and closes it in a 5-node cluster.
func TestHostAPI_Session_Lifecycle_FiveNode(t *testing.T) {
	cluster := newTestCluster(t, 5)
	defer cluster.close()

	cluster.startShard(1)
	leaderIdx := cluster.waitForClusterLeader(1, leaderElectionTimeout)
	leaderHost := cluster.hosts[leaderIdx]

	ctx, cancel := context.WithTimeout(context.Background(), defaultTimeout)
	defer cancel()

	session, err := leaderHost.GetNewSession(ctx, 1)
	if err != nil {
		t.Fatalf("GetNewSession failed: %v", err)
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

// TestHostAPI_Snapshot_FiveNode requests a snapshot on the leader of a 5-node
// cluster and verifies the snapshot actually happened: a non-zero index, an
// artifact on disk, and a higher index on a second request after more writes.
// See TestHostAPI_Snapshot_LeaderRequest for what the previous version missed.
func TestHostAPI_Snapshot_FiveNode(t *testing.T) {
	cluster := newTestCluster(t, 5)
	defer cluster.close()

	trackers := cluster.startShard(1)
	leaderIdx := cluster.waitForClusterLeader(1, leaderElectionTimeout)
	leaderHost := cluster.hosts[leaderIdx]

	// Write data so the snapshot has content.
	for i := 0; i < 5; i++ {
		proposeToLeader(t, cluster, 1, encodeKVPut(
			fmt.Sprintf("snap5-key-%d", i),
			fmt.Sprintf("snap5-val-%d", i),
		), defaultTimeout)
	}

	leaderSM := trackers[leaderIdx].Get(1, uint64(leaderIdx+1))
	waitForCondition(t, defaultTimeout, "5 entries applied", func() bool {
		return leaderSM.Len() >= 5
	})

	// SnapshotEntries stays 0 for this shard (see
	// TestHostAPI_Snapshot_LeaderRequest), so no automatic snapshot competes
	// with the explicit requests below.
	snapshotDir := cluster.nodeHostDirs[leaderIdx]
	firstIndex := requestSnapshotIndex(t, leaderHost, 1)
	assertSnapshotArtifact(t, snapshotDir, firstIndex, defaultTimeout)
	t.Logf("first snapshot taken at index %d", firstIndex)

	for i := 5; i < 10; i++ {
		proposeToLeader(t, cluster, 1, encodeKVPut(
			fmt.Sprintf("snap5-key-%d", i),
			fmt.Sprintf("snap5-val-%d", i),
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

// TestHostAPI_ReadIndex_FiveNode verifies that a ReadIndex issued to the
// leader of a 5-node cluster resolves to a confirmed index. See
// TestHostAPI_ReadIndex_ThreeNode for why the result is read and why the
// warmup write precedes it.
func TestHostAPI_ReadIndex_FiveNode(t *testing.T) {
	cluster := newTestCluster(t, 5)
	defer cluster.close()

	cluster.startShard(1)
	cluster.waitForClusterLeader(1, leaderElectionTimeout)
	proposeToLeader(t, cluster, 1, encodeKVPut("ri5-warmup", "ok"), defaultTimeout)

	index := readIndexFromLeader(t, cluster, 1, defaultTimeout)
	if index == 0 {
		t.Fatal("ReadIndex resolved to index 0 after a committed write; the " +
			"confirmed index must be at least the warmup entry's")
	}
}

// TestHostAPI_LeaderTransfer_FiveNode requests leadership transfer to a
// specific follower in a 5-node cluster and verifies that leadership actually
// moved to THAT follower. See TestHostAPI_LeaderTransfer_ThreeNode for why the
// previous ShardID check proved nothing.
func TestHostAPI_LeaderTransfer_FiveNode(t *testing.T) {
	cluster := newTestCluster(t, 5)
	defer cluster.close()

	trackers := cluster.startShard(1)
	leaderIdx := cluster.waitForClusterLeader(1, leaderElectionTimeout)
	leaderHost := cluster.hosts[leaderIdx]

	// Wait for a follower to discover the leader before transferring.
	targetIdx := cluster.waitForFollower(1, leaderElectionTimeout)
	targetReplicaID := uint64(targetIdx + 1)

	// A transfer target must have an up-to-date log before the leader will
	// hand over, so replicate one entry everywhere first.
	proposeToLeader(t, cluster, 1, encodeKVPut("lt5-warmup", "ok"), defaultTimeout)
	waitForReplication(t, cluster, trackers, 1, "lt5-warmup", "ok", defaultTimeout)

	transferLeadershipTo(t, cluster, 1, targetIdx, failoverTimeout)

	targetInfo, err := cluster.hosts[targetIdx].GetShardInfo(1)
	if err != nil {
		t.Fatalf("GetShardInfo on transfer target node[%d] failed: %v", targetIdx, err)
	}
	if !targetInfo.IsLeader {
		t.Fatalf("transfer target node[%d] is not the leader after transfer", targetIdx)
	}
	if targetInfo.ReplicaID != targetReplicaID {
		t.Fatalf("transfer target node[%d] ReplicaID = %d, want %d",
			targetIdx, targetInfo.ReplicaID, targetReplicaID)
	}

	waitForCondition(t, failoverTimeout,
		fmt.Sprintf("all nodes report replica %d as leader", targetReplicaID),
		func() bool {
			for _, i := range cluster.liveNodeIndexes() {
				leaderID, known, leaderErr := cluster.hosts[i].GetLeaderID(1)
				if leaderErr != nil || !known || leaderID != targetReplicaID {
					return false
				}
			}
			return true
		},
	)

	info, err := leaderHost.GetShardInfo(1)
	if err != nil {
		t.Fatalf("GetShardInfo after transfer request failed: %v", err)
	}
	if info.ShardID != 1 {
		t.Errorf("ShardID = %d, want 1", info.ShardID)
	}
}

// TestHostAPI_GetShardInfo_FiveNode verifies all 5 nodes return correct
// shard info.
func TestHostAPI_GetShardInfo_FiveNode(t *testing.T) {
	cluster := newTestCluster(t, 5)
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
				info, err := host.GetShardInfo(1)
				return err == nil && info.Term > 0
			},
		)
	}

	for i, h := range cluster.hosts {
		info, err := h.GetShardInfo(1)
		if err != nil {
			t.Fatalf("node[%d] GetShardInfo failed: %v", i, err)
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

	// Wait for exactly one leader. During election transitions, multiple
	// nodes may momentarily report IsLeader before convergence.
	waitForCondition(t, leaderElectionTimeout, "exactly 1 leader", func() bool {
		count := 0
		for _, h := range cluster.hosts {
			info, err := h.GetShardInfo(1)
			if err != nil {
				return false
			}
			if info.IsLeader {
				count++
			}
		}
		return count == 1
	})
}

// TestHostAPI_GetNodeHostInfo_FiveNode verifies all 5 nodes return host info.
func TestHostAPI_GetNodeHostInfo_FiveNode(t *testing.T) {
	cluster := newTestCluster(t, 5)
	defer cluster.close()

	cluster.startShard(1)
	cluster.waitForClusterLeader(1, leaderElectionTimeout)

	for i, h := range cluster.hosts {
		info := h.GetNodeHostInfo(quicraft.NodeHostInfoOption{SkipLogInfo: true})
		if info == nil {
			t.Fatalf("node[%d] GetNodeHostInfo returned nil", i)
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

// TestHostAPI_MultipleShards_FiveNode runs 3 independent shards on the 5-node
// cluster and verifies each shard has an independent leader and can replicate.
func TestHostAPI_MultipleShards_FiveNode(t *testing.T) {
	cluster := newTestCluster(t, 5)
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
		// connections between all 5 nodes for this shard.
		warmupKey := fmt.Sprintf("warmup-shard%d", shardID)
		proposeToLeader(t, cluster, shardID,
			encodeKVPut(warmupKey, "warmup"), defaultTimeout)
		waitForReplication(t, cluster, trackers, shardID, warmupKey, "warmup", defaultTimeout)
	}

	// Write to each shard via its leader.
	for shardID := uint64(1); shardID <= 3; shardID++ {
		key := fmt.Sprintf("ms5-shard%d-key", shardID)
		val := fmt.Sprintf("ms5-shard%d-val", shardID)
		proposeToLeader(t, cluster, shardID, encodeKVPut(key, val), defaultTimeout)
	}

	// Verify replication for each shard.
	for shardID := uint64(1); shardID <= 3; shardID++ {
		key := fmt.Sprintf("ms5-shard%d-key", shardID)
		val := fmt.Sprintf("ms5-shard%d-val", shardID)
		waitForReplication(t, cluster, shardTrackers[shardID], shardID, key, val, defaultTimeout)
	}

	// Verify host info shows all 3 shards.
	for i, h := range cluster.hosts {
		info := h.GetNodeHostInfo(quicraft.NodeHostInfoOption{SkipLogInfo: true})
		if info == nil {
			t.Fatalf("node[%d] GetNodeHostInfo returned nil", i)
		}
		shardCount := len(info.ShardInfoList)
		if shardCount != 3 {
			t.Errorf("node[%d] ShardCount = %d, want 3", i, shardCount)
		}
	}
}

// TestHostAPI_ConcurrentProposals_FiveNode sends proposals from 5 goroutines
// (20 each) to the leader of a 5-node cluster and verifies that nearly all of
// them succeed and that every accepted value is present on every node. See
// TestHostAPI_ConcurrentProposals_ThreeNode for what the previous floor missed.
func TestHostAPI_ConcurrentProposals_FiveNode(t *testing.T) {
	cluster := newTestCluster(t, 5)
	defer cluster.close()

	trackers := cluster.startShard(1)
	leaderIdx := cluster.waitForClusterLeader(1, leaderElectionTimeout)

	numGoroutines := 5
	proposalsPerGoroutine := 20
	totalProposals := numGoroutines * proposalsPerGoroutine
	minSuccess := int64(totalProposals * 9 / 10)

	var wg sync.WaitGroup
	var successCount atomic.Int64
	var writtenMu sync.Mutex
	written := make(map[string]string, totalProposals)
	var proposeErrs []error

	for g := 0; g < numGoroutines; g++ {
		wg.Add(1)
		go func(gID int) {
			defer wg.Done()
			for p := 0; p < proposalsPerGoroutine; p++ {
				key := fmt.Sprintf("conc5-g%d-p%d", gID, p)
				val := fmt.Sprintf("v5-%d-%d", gID, p)
				_, err := tryProposeToLeader(cluster, 1, encodeKVPut(key, val), defaultTimeout)
				if err != nil {
					// Collected, not asserted here: this runs on a
					// non-test goroutine, where t.Fatal would only stop
					// this goroutine and let the test carry on.
					writtenMu.Lock()
					proposeErrs = append(proposeErrs, fmt.Errorf("propose %q: %w", key, err))
					writtenMu.Unlock()
					continue
				}
				successCount.Add(1)
				writtenMu.Lock()
				written[key] = val
				writtenMu.Unlock()
			}
		}(g)
	}

	wg.Wait()
	t.Logf("concurrent proposals (5-node): %d/%d succeeded", successCount.Load(), totalProposals)

	for _, err := range proposeErrs {
		t.Logf("proposal error: %v", err)
	}
	if got := successCount.Load(); got < minSuccess {
		t.Fatalf("only %d/%d proposals succeeded, want at least %d",
			got, totalProposals, minSuccess)
	}

	// Verify replication to all nodes.
	leaderSM := trackers[leaderIdx].Get(1, uint64(leaderIdx+1))
	waitForCondition(t, defaultTimeout, "leader SM apply all", func() bool {
		return int64(leaderSM.Len()) >= successCount.Load()
	})

	verifyAllData(t, cluster, trackers, 1, written)
}

// TestHostAPI_ProposeToFollower_FiveNode verifies that proposing to a follower
// in a 5-node cluster succeeds via automatic leader forwarding.
func TestHostAPI_ProposeToFollower_FiveNode(t *testing.T) {
	cluster := newTestCluster(t, 5)
	defer cluster.close()

	trackers := cluster.startShard(1)
	cluster.waitForClusterLeader(1, leaderElectionTimeout)

	// Wait for a follower that knows the leader so it can forward proposals.
	followerIdx := cluster.waitForFollower(1, leaderElectionTimeout)

	ctx, cancel := context.WithTimeout(context.Background(), defaultTimeout)
	defer cancel()

	// Propose via follower — this should succeed via leader forwarding.
	//quicraft:direct-propose the FOLLOWER is the subject: this asserts leader forwarding
	_, err := cluster.hosts[followerIdx].SyncPropose(ctx, 1, encodeKVPut("fwd5-key", "fwd5-val"))
	if err != nil {
		t.Fatalf("SyncPropose via follower failed: %v", err)
	}

	// Verify the value was committed and replicated.
	waitForReplication(t, cluster, trackers, 1, "fwd5-key", "fwd5-val", defaultTimeout)
}

// TestHostAPI_LargeValue_FiveNode replicates a 64KB value to all 5 nodes.
func TestHostAPI_LargeValue_FiveNode(t *testing.T) {
	cluster := newTestCluster(t, 5)
	defer cluster.close()

	trackers := cluster.startShard(1)
	cluster.waitForClusterLeader(1, leaderElectionTimeout)

	// Create a 64KB value.
	largeVal := strings.Repeat("Y", 64*1024)
	proposeToLeader(t, cluster, 1, encodeKVPut("large5-key", largeVal), defaultTimeout)

	// Verify replication to all 5 nodes. A node whose state machine has not
	// registered is waited for, never skipped.
	for _, i := range cluster.liveNodeIndexes() {
		nodeSM := waitForStateMachine(t, cluster, trackers, 1, i, defaultTimeout)
		waitForCondition(t, defaultTimeout,
			fmt.Sprintf("node[%d] replicates large value (5-node)", i),
			func() bool {
				val, ok := nodeSM.Get("large5-key")
				return ok && len(val) == 64*1024
			},
		)
	}
}
