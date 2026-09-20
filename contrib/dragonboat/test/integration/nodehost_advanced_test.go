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
	"fmt"
	"testing"
	"time"

	dragonboat "github.com/jeremyhahn/go-quicraft/contrib/dragonboat"
	dbClient "github.com/jeremyhahn/go-quicraft/contrib/dragonboat/client"
	dbConfig "github.com/jeremyhahn/go-quicraft/contrib/dragonboat/config"
	quicraft "github.com/jeremyhahn/go-quicraft/pkg"
)

// TestIntegration_DB_SyncIOAPIs verifies that the sync propose/read
// round-trip works through the adapter. Matches dragonboat's
// TestNodeHostSyncIOAPIs.
func TestIntegration_DB_SyncIOAPIs(t *testing.T) {
	cluster := newDBTestCluster(t, 3)
	defer cluster.close()

	trackers := cluster.startShard(1)
	cluster.waitForClusterLeader(1, leaderElectionTimeout)

	// Warmup entry.
	proposeAndWait(t, cluster, trackers, 1, "warmup", "ok")
	waitForReplication(t, cluster, trackers, 1, "warmup", "ok", defaultTimeout)

	session := dbClient.NewNoOPSession(1, nil)

	// Propose. The read below is the other half of the round trip; it is
	// linearizable, so it re-resolves rather than pinning the replica the
	// proposal happened to use.
	cmd := encodeKVPut("sync-key", "sync-value")
	result := cluster.proposeToLeader(t, session, cmd, defaultTimeout).Result
	if result.Value != uint64(len("sync-value")) {
		t.Errorf("result.Value = %d, want %d", result.Value, len("sync-value"))
	}

	// Read.
	readResult := cluster.readFromLeader(t, 1, encodeKVGet("sync-key"), defaultTimeout)
	val, ok := readResult.(string)
	if !ok {
		t.Fatalf("SyncRead returned type %T, want string", readResult)
	}
	if val != "sync-value" {
		t.Fatalf("SyncRead = %q, want %q", val, "sync-value")
	}
	t.Logf("sync IO round-trip verified: propose + read")
}

// TestIntegration_DB_ConcurrentSMLookup verifies that a concurrent
// state machine correctly handles concurrent reads during writes.
// Matches dragonboat's TestConcurrentStateMachineLookup.
func TestIntegration_DB_ConcurrentSMLookup(t *testing.T) {
	cluster := newDBTestCluster(t, 3)
	defer cluster.close()

	concTrackers := cluster.startConcurrentShard(1)
	leaderIdx := cluster.waitForClusterLeader(1, leaderElectionTimeout)
	t.Logf("concurrent SM leader at node[%d]", leaderIdx)

	session := dbClient.NewNoOPSession(1, nil)

	// Warmup entry. The read at the end re-resolves, so the accepting replica
	// is not kept.
	cluster.proposeToLeader(t, session, encodeKVPut("warmup", "ok"), defaultTimeout)
	for i := range cluster.hosts {
		if cluster.stopped[i].Load() || concTrackers[i] == nil {
			continue
		}
		sm := concTrackers[i].Get(1, uint64(i+1))
		if sm == nil {
			continue
		}
		waitForCondition(t, defaultTimeout, fmt.Sprintf("node[%d] warmup", i), func() bool {
			_, ok := sm.Get("warmup")
			return ok
		})
	}

	// Propose 10 entries.
	for i := range 10 {
		key := fmt.Sprintf("conc-lookup-%03d", i)
		value := fmt.Sprintf("val-%03d", i)
		cmd := encodeKVPut(key, value)
		cluster.proposeToLeader(t, session, cmd, defaultTimeout)
	}

	// Verify with a linearizable read (exercises the Lookup path on the
	// concurrent SM).
	result := cluster.readFromLeader(t, 1, encodeKVGet("conc-lookup-005"), defaultTimeout)
	val, ok := result.(string)
	if !ok {
		t.Fatalf("SyncRead returned type %T, want string", result)
	}
	if val != "val-005" {
		t.Fatalf("SyncRead = %q, want %q", val, "val-005")
	}
	t.Logf("concurrent SM lookup verified")
}

// TestIntegration_DB_LeaderInfoIsReported verifies that GetNodeHostInfo
// correctly reports the leader for each shard. Matches dragonboat's
// TestLeaderInfoIsReported.
func TestIntegration_DB_LeaderInfoIsReported(t *testing.T) {
	cluster := newDBTestCluster(t, 3)
	defer cluster.close()

	trackers := cluster.startShard(1)
	leaderIdx := cluster.waitForClusterLeader(1, leaderElectionTimeout)

	// Warmup entry.
	proposeAndWait(t, cluster, trackers, 1, "warmup", "ok")

	leaderHost := cluster.hosts[leaderIdx]
	info := leaderHost.GetNodeHostInfo(dragonboat.NodeHostInfoOption{})

	if len(info.ShardInfoList) == 0 {
		t.Fatal("ShardInfoList should not be empty")
	}

	foundShard := false
	for _, si := range info.ShardInfoList {
		if si.ShardID == 1 {
			foundShard = true
			if si.LeaderID == 0 {
				t.Error("LeaderID should be non-zero for an active shard")
			}
			if !si.IsLeader {
				t.Error("IsLeader should be true on the leader node")
			}
			t.Logf("shard 1 info: LeaderID=%d, IsLeader=%v, Term=%d",
				si.LeaderID, si.IsLeader, si.Term)
			break
		}
	}
	if !foundShard {
		t.Fatal("shard 1 not found in ShardInfoList")
	}
}

// TestIntegration_DB_DroppedRequestsOnInvalidShard verifies that
// proposals and reads on shards that aren't ready return errors.
// Matches dragonboat's TestDroppedRequestsAreReported.
func TestIntegration_DB_DroppedRequestsOnInvalidShard(t *testing.T) {
	pki := dbTestPKIFor(t)
	addr := freePort(t)
	nhConfig := dbTestNodeHostConfig(t, addr, pki.leaf(t))

	nh, err := dragonboat.NewNodeHostWithOptions(nhConfig, quicraft.WithMemoryLogDB())
	if err != nil {
		t.Fatalf("NewNodeHostWithOptions failed: %v", err)
	}
	defer nh.Close()

	ctx, cancel := context.WithTimeout(context.Background(), defaultTimeout)
	defer cancel()

	// Propose on shard that doesn't exist.
	session := dbClient.NewNoOPSession(999, nil)
	//quicraft:direct-propose the NON-EXISTENT shard is the subject. There is no
	// cluster and no shard 999 to resolve a leader for; the rejection is the
	// assertion.
	_, propErr := nh.SyncPropose(ctx, session, encodeKVPut("k", "v"))
	if propErr == nil {
		t.Fatal("SyncPropose on non-existent shard should fail")
	}
	t.Logf("SyncPropose on non-existent shard: %v", propErr)

	// Read on shard that doesn't exist.
	//quicraft:direct-read the NON-EXISTENT shard is the subject, as for SyncPropose above:
	// there is no cluster and no shard 999 to resolve a leader for
	_, readErr := nh.SyncRead(ctx, 999, encodeKVGet("k"))
	if readErr == nil {
		t.Fatal("SyncRead on non-existent shard should fail")
	}
	t.Logf("SyncRead on non-existent shard: %v", readErr)
}

// TestIntegration_DB_TimeoutCanBeReturned verifies that operations
// with a very short timeout return a timeout error. Matches
// dragonboat's TestTimeoutCanBeReturned.
func TestIntegration_DB_TimeoutCanBeReturned(t *testing.T) {
	cluster := newDBTestCluster(t, 3)
	defer cluster.close()

	trackers := cluster.startShard(1)
	cluster.waitForClusterLeader(1, leaderElectionTimeout)

	// Warmup entry.
	proposeAndWait(t, cluster, trackers, 1, "warmup", "ok")

	// waitForLeaderHost, not cluster.hosts[cluster.findLeader(1)]: findLeader
	// returns -1 between two leaders, and indexing hosts with it panics.
	leaderHost := cluster.waitForLeaderHost(1, leaderElectionTimeout)

	// Use an already-expired context.
	expiredCtx, expiredCancel := context.WithTimeout(context.Background(), 1*time.Nanosecond)
	// Ensure the context is truly expired before proceeding.
	expireTimer := time.NewTimer(1 * time.Millisecond)
	<-expireTimer.C
	expireTimer.Stop()
	defer expiredCancel()

	session := dbClient.NewNoOPSession(1, nil)
	//quicraft:direct-propose the EXPIRED context is the subject. SyncPropose
	// rejects it before the proposal is routed, and proposeToLeader takes a
	// timeout rather than a context so it cannot express this input.
	_, propErr := leaderHost.SyncPropose(expiredCtx, session, encodeKVPut("k", "v"))
	if propErr == nil {
		t.Fatal("SyncPropose with expired context should return error")
	}
	t.Logf("SyncPropose with expired context returned: %v", propErr)
}

// TestIntegration_DB_RegisterSessionTwice verifies ProposeSession behavior
// when called on an already-registered session.
//
// In quicraft, after SyncGetSession the session's SeriesID is
// SeriesIDFirstProposal (normal proposal range), not SeriesIDForRegister.
// The adapter's ProposeSession routes based on SeriesID:
//   - SeriesIDForRegister   -> register the session (client ID preserved)
//   - SeriesIDForUnregister -> close/unregister the session
//   - any other value       -> rejected with ErrInvalidSession
//
// To simulate "register twice", we manually prepare the session for
// registration. The register path is asynchronous: the duplicate
// registration is reported through the RequestState's ResultC as a
// non-completed result rather than as an immediate error.
//
// Matches dragonboat's TestRegisterASessionTwiceWillBeReported in spirit:
// both verify that ProposeSession on an already-registered session is
// reported and does not produce undefined behavior.
func TestIntegration_DB_RegisterSessionTwice(t *testing.T) {
	cluster := newDBTestCluster(t, 3)
	defer cluster.close()

	trackers := cluster.startShard(1)
	leaderIdx := cluster.waitForClusterLeader(1, leaderElectionTimeout)

	// Warmup entry.
	proposeAndWait(t, cluster, trackers, 1, "warmup", "ok")

	leaderHost := cluster.hosts[leaderIdx]

	// Register a tracked session.
	ctx, cancel := context.WithTimeout(context.Background(), defaultTimeout)
	defer cancel()
	session, err := leaderHost.SyncGetSession(ctx, 1)
	if err != nil {
		t.Fatalf("SyncGetSession failed: %v", err)
	}
	originalClientID := session.ClientID()
	t.Logf("original session clientID=%d seriesID=%d", session.ClientID(), session.SeriesID())

	// Manually prepare the session for registration so ProposeSession
	// takes the register path. After SyncGetSession the SeriesID is in
	// the normal proposal range; PrepareForRegister sets it to
	// SeriesIDForRegister.
	session.Inner().PrepareForRegister()

	// The register path runs asynchronously through RegisterSession,
	// which preserves the client ID. A duplicate registration is applied
	// as an empty result by the state machine and surfaces here as a
	// non-completed RequestResult.
	rs, propErr := leaderHost.ProposeSession(session, defaultTimeout)
	if propErr != nil {
		// A direct error is acceptable -- some implementations may reject
		// the operation eagerly.
		t.Logf("ProposeSession returned error: %v", propErr)
		return
	}
	defer rs.Release()

	select {
	case result := <-rs.ResultC():
		if !result.Completed() {
			// The duplicate registration was reported -- this is the
			// expected outcome (dragonboat: "will be reported").
			t.Logf("ProposeSession for already-registered session was reported: completed=%v rejected=%v aborted=%v",
				result.Completed(), result.Rejected(), result.Aborted())
			return
		}

		// If the engine treated the re-registration as a success, the
		// client ID must be preserved: RegisterSession never mints a
		// new client ID.
		newClientID := session.ClientID()
		t.Logf("session clientID after re-register=%d (original=%d)", newClientID, originalClientID)
		if newClientID != originalClientID {
			t.Fatal("ProposeSession must preserve the session's client ID")
		}
	case <-ctx.Done():
		t.Fatal("ProposeSession timed out waiting for result")
	}
}

// TestIntegration_DB_GetNodeHostRegistry verifies that
// GetNodeHostRegistry returns nil/false since quicraft doesn't support gossip.
func TestIntegration_DB_GetNodeHostRegistry(t *testing.T) {
	pki := dbTestPKIFor(t)
	addr := freePort(t)
	nhConfig := dbTestNodeHostConfig(t, addr, pki.leaf(t))

	nh, err := dragonboat.NewNodeHostWithOptions(nhConfig, quicraft.WithMemoryLogDB())
	if err != nil {
		t.Fatalf("NewNodeHostWithOptions failed: %v", err)
	}
	defer nh.Close()

	registry, ok := nh.GetNodeHostRegistry()
	if ok {
		t.Fatal("GetNodeHostRegistry should return false (gossip not supported)")
	}
	if registry != nil {
		t.Fatal("GetNodeHostRegistry should return nil registry")
	}
	t.Logf("GetNodeHostRegistry correctly returns nil, false")
}

// TestIntegration_DB_GetNodeUser verifies GetNodeUser returns a
// functional INodeUser for proposal and read operations.
func TestIntegration_DB_GetNodeUser(t *testing.T) {
	pki := dbTestPKIFor(t)
	addr := freePort(t)
	nhConfig := dbTestNodeHostConfig(t, addr, pki.leaf(t))

	nh, err := dragonboat.NewNodeHostWithOptions(nhConfig, quicraft.WithMemoryLogDB())
	if err != nil {
		t.Fatalf("NewNodeHostWithOptions failed: %v", err)
	}
	defer nh.Close()

	// GetNodeUser on non-existent shard should fail.
	_, userErr := nh.GetNodeUser(99999)
	if userErr == nil {
		t.Fatal("GetNodeUser on non-existent shard should return error")
	}
	t.Logf("GetNodeUser on non-existent shard: %v", userErr)

	// Start a single-node shard.
	members := map[uint64]dragonboat.Target{1: addr}
	createFn, _ := dbKVCreateFunc()
	cfg := dbTestShardConfig(1, 1)
	cfg.MemberIdentities = map[uint64]string{1: nh.NodeIdentity()}
	if startErr := nh.StartReplica(members, false, createFn, cfg); startErr != nil {
		t.Fatalf("StartReplica failed: %v", startErr)
	}

	// Wait for leader election.
	waitForCondition(t, leaderElectionTimeout, "single-node leader", func() bool {
		_, _, valid, lErr := nh.GetLeaderID(1)
		return lErr == nil && valid
	})

	// GetNodeUser should succeed now.
	user, userErr2 := nh.GetNodeUser(1)
	if userErr2 != nil {
		t.Fatalf("GetNodeUser failed: %v", userErr2)
	}
	if user.ShardID() != 1 {
		t.Fatalf("user.ShardID() = %d, want 1", user.ShardID())
	}
	t.Logf("GetNodeUser succeeded: ShardID=%d, ReplicaID=%d",
		user.ShardID(), user.ReplicaID())
}

// TestIntegration_DB_SnapshotRecoveryAfterRestart verifies that a node
// that restarts after a snapshot was taken recovers its state correctly.
// This exercises the full snapshot lifecycle: propose, snapshot, stop,
// restart, verify data intact.
func TestIntegration_DB_SnapshotRecoveryAfterRestart(t *testing.T) {
	cluster := newDBTestCluster(t, 3)
	defer cluster.close()

	snapshotCfg := func(shardID, replicaID uint64) dbConfig.Config {
		return dbTestShardConfigWithSnapshot(shardID, replicaID, 15, 5)
	}
	trackers := cluster.startShardWithConfig(1, snapshotCfg)

	leaderIdx := cluster.waitForClusterLeader(1, leaderElectionTimeout)
	t.Logf("leader elected at node[%d]", leaderIdx)

	// Warmup entry.
	proposeAndWait(t, cluster, trackers, 1, "warmup", "ok")
	waitForReplication(t, cluster, trackers, 1, "warmup", "ok", defaultTimeout)

	// Propose enough entries to trigger auto-snapshots.
	expected := proposeNEntries(t, cluster, trackers, 1, 30, "snap-recover")
	verifyAllData(t, cluster, trackers, 1, expected)

	// Stop a follower.
	followerIdx := -1
	for i := range cluster.hosts {
		if !cluster.stopped[i].Load() && i != leaderIdx {
			followerIdx = i
			break
		}
	}
	if followerIdx < 0 {
		t.Fatal("no follower found")
	}

	followerHost := cluster.hosts[followerIdx]
	if err := followerHost.StopShard(1); err != nil {
		t.Fatalf("StopShard on follower failed: %v", err)
	}
	t.Logf("stopped shard on follower node[%d]", followerIdx)

	// Restart the follower's shard.
	replicaID := uint64(followerIdx + 1)
	createFn, newTracker := dbKVCreateFunc()
	cfg := snapshotCfg(1, replicaID)
	cfg.MemberIdentities = cluster.identities()
	if err := followerHost.StartReplica(nil, true, createFn, cfg); err != nil {
		t.Fatalf("StartReplica (rejoin) failed: %v", err)
	}
	trackers[followerIdx] = newTracker

	// Verify the restarted follower catches up: its own snapshot and log
	// first, then whatever tail the leader has to ship it. One wait for the
	// whole recovery, bounded by catchUpTimeout (see helpers_test.go for what
	// it bounds and why defaultTimeout was the wrong unit), and then every
	// key checked exactly.
	sm := newTracker.Get(1, replicaID)
	if sm == nil {
		t.Fatal("restarted follower has no state machine")
	}
	recoveryStarted := time.Now()
	waitForCondition(t, catchUpTimeout,
		fmt.Sprintf("restarted follower node[%d] holds all %d entries", followerIdx, len(expected)),
		func() bool {
			for key, val := range expected {
				if got, ok := sm.Get(key); !ok || got != val {
					return false
				}
			}
			return true
		},
	)
	for key, val := range expected {
		if got, ok := sm.Get(key); !ok || got != val {
			t.Fatalf("restarted follower has %q=%q (present=%v), want %q", key, got, ok, val)
		}
	}
	t.Logf("follower recovered all %d entries %v after snapshot-based restart",
		len(expected), time.Since(recoveryStarted).Round(time.Millisecond))
}

// TestIntegration_DB_SnapshotIndex_RawValueOnRegularResult verifies the
// adapter's documented SnapshotIndex() behavior: quicraft delivers the
// snapshot index in the result's Value field, so SnapshotIndex() returns
// the raw result value. It is only meaningful for snapshot-related
// results; on a regular proposal it simply mirrors GetResult().Value
// (dragonboat panics on such misuse, the adapter does not).
func TestIntegration_DB_SnapshotIndex_RawValueOnRegularResult(t *testing.T) {
	cluster := newDBTestCluster(t, 3)
	defer cluster.close()

	trackers := cluster.startShard(1)
	leaderIdx := cluster.waitForClusterLeader(1, leaderElectionTimeout)
	t.Logf("leader elected at node[%d]", leaderIdx)

	// Warmup entry.
	proposeAndWait(t, cluster, trackers, 1, "warmup", "ok")
	waitForReplication(t, cluster, trackers, 1, "warmup", "ok", defaultTimeout)

	session := dbClient.NewNoOPSession(1, nil)

	key := "snap-idx-key"
	value := "snap-idx-value"
	cmd := encodeKVPut(key, value)

	rs := cluster.proposeAsyncToLeader(t, session, cmd, defaultTimeout).RS

	select {
	case result := <-rs.ResultC():
		if !result.Completed() {
			t.Fatalf("Propose did not complete: timeout=%v, rejected=%v",
				result.Timeout(), result.Rejected())
		}
		smResult := result.GetResult()
		if smResult.Value != uint64(len(value)) {
			t.Fatalf("GetResult().Value = %d, want %d", smResult.Value, len(value))
		}
		// SnapshotIndex mirrors the raw result value on non-snapshot
		// results (documented adapter behavior).
		if result.SnapshotIndex() != smResult.Value {
			t.Fatalf("SnapshotIndex() = %d, want raw result value %d",
				result.SnapshotIndex(), smResult.Value)
		}
		t.Logf("regular proposal result: SnapshotIndex=0, Value=%d", smResult.Value)
	case <-time.After(defaultTimeout):
		t.Fatal("Propose timed out waiting for ResultC")
	}
	rs.Release()
}
