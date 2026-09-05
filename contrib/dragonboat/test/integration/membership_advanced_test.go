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

package integration_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	dragonboat "github.com/jeremyhahn/go-quicraft/contrib/dragonboat"
	dbClient "github.com/jeremyhahn/go-quicraft/contrib/dragonboat/client"
	dbConfig "github.com/jeremyhahn/go-quicraft/contrib/dragonboat/config"
)

// TestIntegration_DB_OrderedMembershipChange verifies that membership
// changes must use the current ConfigChangeID. A stale ConfigChangeID
// causes the change to be rejected. Matches dragonboat's
// TestOrderedMembershipChange.
func TestIntegration_DB_OrderedMembershipChange(t *testing.T) {
	cluster := newDBTestCluster(t, 3)
	defer cluster.close()

	// OrderedConfigChange must be true so stale ConfigChangeIDs are rejected.
	orderedCfg := func(shardID, replicaID uint64) dbConfig.Config {
		return dbConfig.Config{
			ShardID:             shardID,
			ReplicaID:           replicaID,
			ElectionRTT:         testElectionRTT,
			HeartbeatRTT:        1,
			CheckQuorum:         true,
			OrderedConfigChange: true,
		}
	}
	trackers := cluster.startShardWithConfig(1, orderedCfg)
	leaderIdx := cluster.waitForClusterLeader(1, leaderElectionTimeout)
	t.Logf("leader elected at node[%d]", leaderIdx)

	// Warmup entry.
	proposeAndWait(t, cluster, trackers, 1, "warmup", "ok")
	waitForReplication(t, cluster, trackers, 1, "warmup", "ok", defaultTimeout)

	leaderHost := cluster.hosts[leaderIdx]

	// Get the current ConfigChangeID.
	ctx, cancel := context.WithTimeout(context.Background(), defaultTimeout)
	defer cancel()
	membership, err := leaderHost.SyncGetShardMembership(ctx, 1)
	if err != nil {
		t.Fatalf("SyncGetShardMembership failed: %v", err)
	}
	currentID := membership.ConfigChangeID

	// Use the correct ConfigChangeID to add a replica — should succeed.
	addCtx, addCancel := context.WithTimeout(context.Background(), defaultTimeout)
	defer addCancel()
	if addErr := leaderHost.SyncRequestAddReplica(addCtx, 1, 10, "127.10.0.1:59999", currentID); addErr != nil {
		t.Fatalf("SyncRequestAddReplica with correct ConfigChangeID failed: %v", addErr)
	}
	t.Logf("SyncRequestAddReplica with ConfigChangeID=%d succeeded", currentID)

	// Re-read membership to get the updated (non-zero) ConfigChangeID
	// after the first config change was applied.
	updCtx, updCancel := context.WithTimeout(context.Background(), defaultTimeout)
	defer updCancel()
	updatedMembership, updErr := leaderHost.SyncGetShardMembership(updCtx, 1)
	if updErr != nil {
		t.Fatalf("SyncGetShardMembership after add failed: %v", updErr)
	}
	freshID := updatedMembership.ConfigChangeID
	t.Logf("fresh ConfigChangeID after first add: %d", freshID)

	// Using the OLD ConfigChangeID (which is now stale) should fail.
	staleCtx, staleCancel := context.WithTimeout(context.Background(), defaultTimeout)
	defer staleCancel()
	staleErr := leaderHost.SyncRequestAddReplica(staleCtx, 1, 11, "127.10.0.1:59998", currentID)
	if staleErr == nil {
		// If the initial ConfigChangeID was 0, the staleness check is skipped
		// (0 means "don't check"). Use the freshID instead and verify that
		// a truly stale (non-zero) ID is rejected.
		if currentID == 0 {
			t.Logf("initial ConfigChangeID was 0 (skip check); retrying with stale freshID=%d", freshID)
			// The second add succeeded with currentID=0, so re-read again.
			rereadCtx, rereadCancel := context.WithTimeout(context.Background(), defaultTimeout)
			defer rereadCancel()
			reMembership, reErr := leaderHost.SyncGetShardMembership(rereadCtx, 1)
			if reErr != nil {
				t.Fatalf("SyncGetShardMembership re-read failed: %v", reErr)
			}
			_ = reMembership // new ID after second add
			// Now use freshID (which is stale after the second add).
			stale2Ctx, stale2Cancel := context.WithTimeout(context.Background(), defaultTimeout)
			defer stale2Cancel()
			stale2Err := leaderHost.SyncRequestAddReplica(stale2Ctx, 1, 12, "127.10.0.1:59997", freshID)
			if stale2Err == nil {
				t.Fatal("SyncRequestAddReplica with stale non-zero ConfigChangeID should return error")
			}
			t.Logf("SyncRequestAddReplica with stale ConfigChangeID=%d correctly returned: %v", freshID, stale2Err)
		} else {
			t.Fatal("SyncRequestAddReplica with stale ConfigChangeID should return error")
		}
	} else {
		t.Logf("SyncRequestAddReplica with stale ConfigChangeID correctly returned: %v", staleErr)
	}
}

// TestIntegration_DB_SyncRequestDeleteReplica verifies that a replica
// can be removed from the cluster via SyncRequestDeleteReplica. Matches
// dragonboat's TestSyncRequestDeleteReplica.
func TestIntegration_DB_SyncRequestDeleteReplica(t *testing.T) {
	cluster := newDBTestCluster(t, 3)
	defer cluster.close()

	trackers := cluster.startShard(1)
	leaderIdx := cluster.waitForClusterLeader(1, leaderElectionTimeout)
	t.Logf("leader elected at node[%d]", leaderIdx)

	// Warmup entry.
	proposeAndWait(t, cluster, trackers, 1, "warmup", "ok")
	waitForReplication(t, cluster, trackers, 1, "warmup", "ok", defaultTimeout)

	leaderHost := cluster.hosts[leaderIdx]

	// Get current membership.
	ctx, cancel := context.WithTimeout(context.Background(), defaultTimeout)
	defer cancel()
	membership, err := leaderHost.SyncGetShardMembership(ctx, 1)
	if err != nil {
		t.Fatalf("SyncGetShardMembership failed: %v", err)
	}
	if len(membership.Nodes) != 3 {
		t.Fatalf("expected 3 nodes, got %d", len(membership.Nodes))
	}

	// Pick a follower to remove.
	var targetReplicaID uint64
	for i := range cluster.hosts {
		if !cluster.stopped[i].Load() && i != leaderIdx {
			targetReplicaID = uint64(i + 1)
			break
		}
	}

	// Delete the follower.
	delCtx, delCancel := context.WithTimeout(context.Background(), defaultTimeout)
	defer delCancel()
	if delErr := leaderHost.SyncRequestDeleteReplica(delCtx, 1, targetReplicaID, membership.ConfigChangeID); delErr != nil {
		t.Fatalf("SyncRequestDeleteReplica failed: %v", delErr)
	}
	t.Logf("SyncRequestDeleteReplica removed replica %d", targetReplicaID)

	// Verify the membership no longer includes the removed replica.
	verifyCtx, verifyCancel := context.WithTimeout(context.Background(), defaultTimeout)
	defer verifyCancel()
	updatedMembership, mErr := leaderHost.SyncGetShardMembership(verifyCtx, 1)
	if mErr != nil {
		t.Fatalf("SyncGetShardMembership after delete failed: %v", mErr)
	}
	if _, exists := updatedMembership.Nodes[targetReplicaID]; exists {
		t.Fatalf("replica %d should not be in membership after delete", targetReplicaID)
	}
	t.Logf("membership after delete: %d nodes (replica %d removed)",
		len(updatedMembership.Nodes), targetReplicaID)
}

// TestIntegration_DB_SyncRequestAddReplica verifies that a new replica
// can be added to the cluster via SyncRequestAddReplica. Matches
// dragonboat's TestSyncRequestAddReplica.
func TestIntegration_DB_SyncRequestAddReplica(t *testing.T) {
	cluster := newDBTestCluster(t, 3)
	defer cluster.close()

	trackers := cluster.startShard(1)
	leaderIdx := cluster.waitForClusterLeader(1, leaderElectionTimeout)
	t.Logf("leader elected at node[%d]", leaderIdx)

	// Warmup entry.
	proposeAndWait(t, cluster, trackers, 1, "warmup", "ok")

	leaderHost := cluster.hosts[leaderIdx]

	// Get current membership.
	ctx, cancel := context.WithTimeout(context.Background(), defaultTimeout)
	defer cancel()
	membership, err := leaderHost.SyncGetShardMembership(ctx, 1)
	if err != nil {
		t.Fatalf("SyncGetShardMembership failed: %v", err)
	}

	// Add replica 10 at a dummy address.
	addCtx, addCancel := context.WithTimeout(context.Background(), defaultTimeout)
	defer addCancel()
	if addErr := leaderHost.SyncRequestAddReplica(addCtx, 1, 10, "127.10.0.1:59999", membership.ConfigChangeID); addErr != nil {
		t.Fatalf("SyncRequestAddReplica failed: %v", addErr)
	}

	// Verify the membership now includes replica 10.
	verifyCtx, verifyCancel := context.WithTimeout(context.Background(), defaultTimeout)
	defer verifyCancel()
	updated, mErr := leaderHost.SyncGetShardMembership(verifyCtx, 1)
	if mErr != nil {
		t.Fatalf("SyncGetShardMembership after add failed: %v", mErr)
	}
	if _, exists := updated.Nodes[10]; !exists {
		t.Fatalf("replica 10 not found in membership after add")
	}
	t.Logf("SyncRequestAddReplica succeeded: %d nodes (replica 10 added)", len(updated.Nodes))
}

// TestIntegration_DB_SyncRequestAddNonVoting verifies that a non-voting
// observer can be added via SyncRequestAddNonVoting. Matches
// dragonboat's TestSyncRequestAddNonVoting.
func TestIntegration_DB_SyncRequestAddNonVoting(t *testing.T) {
	cluster := newDBTestCluster(t, 3)
	defer cluster.close()

	trackers := cluster.startShard(1)
	leaderIdx := cluster.waitForClusterLeader(1, leaderElectionTimeout)
	t.Logf("leader elected at node[%d]", leaderIdx)

	// Warmup entry.
	proposeAndWait(t, cluster, trackers, 1, "warmup", "ok")

	leaderHost := cluster.hosts[leaderIdx]

	// Get current membership.
	ctx, cancel := context.WithTimeout(context.Background(), defaultTimeout)
	defer cancel()
	membership, err := leaderHost.SyncGetShardMembership(ctx, 1)
	if err != nil {
		t.Fatalf("SyncGetShardMembership failed: %v", err)
	}

	// Add non-voting replica 20 at a dummy address.
	addCtx, addCancel := context.WithTimeout(context.Background(), defaultTimeout)
	defer addCancel()
	if addErr := leaderHost.SyncRequestAddNonVoting(addCtx, 1, 20, "127.10.0.1:59998", membership.ConfigChangeID); addErr != nil {
		t.Fatalf("SyncRequestAddNonVoting failed: %v", addErr)
	}

	// Verify the membership now includes non-voting replica 20.
	verifyCtx, verifyCancel := context.WithTimeout(context.Background(), defaultTimeout)
	defer verifyCancel()
	updated, mErr := leaderHost.SyncGetShardMembership(verifyCtx, 1)
	if mErr != nil {
		t.Fatalf("SyncGetShardMembership after add non-voting failed: %v", mErr)
	}
	if _, exists := updated.NonVotings[20]; !exists {
		t.Fatalf("non-voting replica 20 not found in membership after add")
	}
	t.Logf("SyncRequestAddNonVoting succeeded: NonVotings=%d", len(updated.NonVotings))
}

// TestIntegration_DB_ConcurrentConfigChangeRejected verifies that only ONE
// config change may be in flight at a time (PhD thesis 4.1): a second config
// change submitted while the first is still unapplied is REJECTED, the first
// still completes, and a config change issued sequentially afterwards
// succeeds.
//
// The overlap is made deterministic rather than raced for. The Raft layer's
// latch is `log.confChangeIndex > log.processed`, and processed only advances
// as the state machine drains committed entries, so the window normally lasts
// the couple of milliseconds a config change takes to commit and apply. The
// shard therefore runs gated state machines: a proposal whose key carries
// gateKeyPrefix parks every replica's apply loop until the test releases it,
// which pins processed below the config-change entry for as long as the test
// needs.
//
// Three assertions, none of which can be satisfied by everything failing:
//
//	NEGATIVE  the overlapping second change must come back Rejected.
//	FIRST     the first change must Complete once the gate opens, and
//	          replica 10 must be in the membership while replica 11 is not.
//	POSITIVE  a control config change issued AFTER the first one applied must
//	          succeed and must land replica 11 in the membership. Without it a
//	          shard that rejected every config change would pass.
func TestIntegration_DB_ConcurrentConfigChangeRejected(t *testing.T) {
	cluster := newDBTestCluster(t, 3)
	defer cluster.close()

	// release opens the apply gate. It is closed exactly once, either by the
	// test body at the point the overlap is no longer needed or by the defer
	// on any earlier failure — a parked apply loop would otherwise wedge
	// cluster.close().
	release := make(chan struct{})
	var releaseOnce sync.Once
	openGate := func() { releaseOnce.Do(func() { close(release) }) }
	defer openGate()

	// OrderedConfigChange must be true so the Raft layer enforces
	// one-at-a-time config changes.
	orderedCfg := func(shardID, replicaID uint64) dbConfig.Config {
		return dbConfig.Config{
			ShardID:             shardID,
			ReplicaID:           replicaID,
			ElectionRTT:         testElectionRTT,
			HeartbeatRTT:        1,
			CheckQuorum:         true,
			OrderedConfigChange: true,
		}
	}
	trackers, gated := cluster.startGatedShard(1, release, orderedCfg)
	leaderIdx := cluster.waitForClusterLeader(1, leaderElectionTimeout)
	t.Logf("leader elected at node[%d]", leaderIdx)

	// Warmup entry required after election (PhD 6.4). "warmup" carries no gate
	// prefix, so it applies normally.
	proposeAndWait(t, cluster, trackers, 1, "warmup", "ok")
	waitForReplication(t, cluster, trackers, 1, "warmup", "ok", defaultTimeout)

	leaderHost := cluster.hosts[leaderIdx]
	leaderReplicaID := uint64(leaderIdx + 1)

	// Get the current ConfigChangeID. Both overlapping changes use it: apply
	// is about to be parked, so the shard's configChangeIndex cannot advance
	// and the Host-level staleness check passes for both. That is deliberate —
	// it forces the rejection to come from the pending-config-change rule and
	// not from staleness, which TestIntegration_DB_OrderedMembershipChange
	// already covers.
	ctx, cancel := context.WithTimeout(context.Background(), defaultTimeout)
	defer cancel()
	membership, err := leaderHost.SyncGetShardMembership(ctx, 1)
	if err != nil {
		t.Fatalf("SyncGetShardMembership failed: %v", err)
	}
	configChangeID := membership.ConfigChangeID

	// Park the leader's apply loop on a gate entry. Propose is asynchronous;
	// the entry commits and then blocks inside the state machine's Update.
	gateSession := dbClient.NewNoOPSession(1, nil)
	//quicraft:direct-propose the gate entry must park THIS leader's apply loop --
	// the one whose gated state machine is waited on below and whose
	// RequestAddReplica calls the rest of the test makes. A re-resolving helper
	// could park a different replica, leaving the pending-config-change latch this
	// test exists to exercise unarmed.
	gateRS, gateErr := leaderHost.Propose(gateSession, encodeKVPut("gate-hold", "1"), defaultTimeout)
	if gateErr != nil {
		t.Fatalf("Propose(gate entry) failed: %v", gateErr)
	}
	defer gateRS.Release()

	var leaderGate *dbGatedKVStateMachine
	waitForCondition(t, defaultTimeout,
		fmt.Sprintf("leader node[%d] registers a gated state machine", leaderIdx),
		func() bool {
			leaderGate = gated[leaderIdx].Get(1, leaderReplicaID)
			return leaderGate != nil
		},
	)
	select {
	case <-leaderGate.entered:
		t.Logf("leader node[%d] apply loop parked on the gate entry", leaderIdx)
	case <-time.After(defaultTimeout):
		t.Fatalf("leader node[%d] apply loop never reached the gate entry within %v",
			leaderIdx, defaultTimeout)
	}

	// FIRST config change. It appends to the leader's log immediately, which
	// raises log.confChangeIndex; because processed is pinned below it by the
	// gate, the pending-config-change latch now stays armed indefinitely.
	rs1, err := leaderHost.RequestAddReplica(1, 10, "127.10.0.1:59990", configChangeID, defaultTimeout)
	if err != nil {
		t.Fatalf("first RequestAddReplica failed: %v", err)
	}
	defer rs1.Release()
	t.Logf("first config change submitted (add replica 10) with ConfigChangeID=%d", configChangeID)

	// Let the step worker drain the first change in its own Propose batch.
	// Two config changes coalesced into ONE batch are both rejected (the
	// foundCC pass in handleLeaderPropose), which is a different code path
	// from the one under test. The step worker turns a batch around in well
	// under a millisecond; this window is three orders of magnitude of
	// margin, and the gate means there is no opposing race to lose — the
	// latch cannot clear while apply is parked, however long the wait.
	settle := time.NewTimer(500 * time.Millisecond)
	<-settle.C
	settle.Stop()

	// SECOND config change, overlapping the first. This must be rejected.
	rs2, secondErr := leaderHost.RequestAddReplica(1, 11, "127.10.0.1:59991", configChangeID, defaultTimeout)
	if secondErr != nil {
		// The submission path may reject synchronously; it must still be a
		// rejection and not some unrelated failure.
		if !errors.Is(secondErr, dragonboat.ErrRejected) {
			t.Fatalf("overlapping second config change failed at submission with %v, want ErrRejected", secondErr)
		}
		t.Logf("second config change rejected at submission: %v", secondErr)
	} else {
		defer rs2.Release()
		select {
		case result := <-rs2.ResultC():
			if !result.Rejected() {
				t.Fatalf("overlapping second config change was not rejected: "+
					"completed=%v timeout=%v rejected=%v dropped=%v aborted=%v terminated=%v",
					result.Completed(), result.Timeout(), result.Rejected(),
					result.Dropped(), result.Aborted(), result.Terminated())
			}
			t.Logf("second config change correctly Rejected while the first was still unapplied")
		case <-time.After(defaultTimeout):
			t.Fatalf("overlapping second config change produced no result within %v; "+
				"a pending config change must be rejected promptly, not left to time out",
				defaultTimeout)
		}
	}

	// Open the gate so the parked entry — and behind it the first config
	// change — can apply.
	openGate()

	select {
	case result := <-rs1.ResultC():
		if !result.Completed() {
			t.Fatalf("first config change did not complete: "+
				"timeout=%v rejected=%v dropped=%v aborted=%v terminated=%v",
				result.Timeout(), result.Rejected(), result.Dropped(),
				result.Aborted(), result.Terminated())
		}
		t.Logf("first config change completed")
	case <-time.After(defaultTimeout):
		t.Fatalf("first config change produced no result within %v after the gate opened", defaultTimeout)
	}

	// The first change landed and the rejected one did not.
	verifyCtx, verifyCancel := context.WithTimeout(context.Background(), defaultTimeout)
	defer verifyCancel()
	updatedMembership, mErr := leaderHost.SyncGetShardMembership(verifyCtx, 1)
	if mErr != nil {
		t.Fatalf("SyncGetShardMembership after config changes failed: %v", mErr)
	}
	if _, exists := updatedMembership.Nodes[10]; !exists {
		t.Fatalf("replica 10 not found in membership after the first config change: %v",
			updatedMembership.Nodes)
	}
	if _, exists := updatedMembership.Nodes[11]; exists {
		t.Fatalf("replica 11 IS in the membership; the rejected config change must not have "+
			"entered the log: %v", updatedMembership.Nodes)
	}

	// POSITIVE CONTROL. The very change that was rejected while overlapping
	// must now succeed when issued sequentially. A shard that rejects every
	// config change, or one whose latch stuck armed after the rejection,
	// fails here.
	seqCtx, seqCancel := context.WithTimeout(context.Background(), defaultTimeout)
	defer seqCancel()
	if addErr := leaderHost.SyncRequestAddReplica(seqCtx, 1, 11, "127.10.0.1:59991",
		updatedMembership.ConfigChangeID); addErr != nil {
		t.Fatalf("sequential config change after the first applied must succeed, got: %v", addErr)
	}

	finalCtx, finalCancel := context.WithTimeout(context.Background(), defaultTimeout)
	defer finalCancel()
	finalMembership, fErr := leaderHost.SyncGetShardMembership(finalCtx, 1)
	if fErr != nil {
		t.Fatalf("SyncGetShardMembership after the sequential config change failed: %v", fErr)
	}
	if _, exists := finalMembership.Nodes[11]; !exists {
		t.Fatalf("replica 11 not found in membership after the sequential config change: %v",
			finalMembership.Nodes)
	}
	t.Logf("concurrent config change rejection verified: overlapping change rejected, "+
		"first completed, sequential control succeeded (%d voters)", len(finalMembership.Nodes))
}
