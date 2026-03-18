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
	"testing"
	"time"

	dragonboat "github.com/jeremyhahn/go-quicraft/contrib/dragonboat"
	dbClient "github.com/jeremyhahn/go-quicraft/contrib/dragonboat/client"
)

// TestIntegration_DB_NoOPSession verifies that creating a no-op session
// via dbClient.NewNoOPSession and using it for proposals works correctly
// through the dragonboat adapter in a real multi-node cluster.
func TestIntegration_DB_NoOPSession(t *testing.T) {
	cluster := newDBTestCluster(t, 3)
	defer cluster.close()

	trackers := cluster.startShard(1)
	leaderIdx := cluster.waitForClusterLeader(1, leaderElectionTimeout)
	t.Logf("leader elected at node[%d]", leaderIdx)

	// Warmup entry required after election (PhD 6.4).
	proposeAndWait(t, cluster, trackers, 1, "warmup", "ok")
	waitForReplication(t, cluster, trackers, 1, "warmup", "ok", defaultTimeout)

	// Create a no-op session using the dragonboat client API directly.
	session := dbClient.NewNoOPSession(1, nil)

	if !session.IsNoOPSession() {
		t.Fatal("expected no-op session, got tracked session")
	}
	if session.ShardID != 1 {
		t.Fatalf("session.ShardID = %d, want 1", session.ShardID)
	}

	// Use the session for SyncPropose through the dragonboat adapter.
	leaderHost := cluster.hosts[leaderIdx]
	ctx, cancel := context.WithTimeout(context.Background(), defaultTimeout)
	defer cancel()

	cmd := encodeKVPut("session-key", "session-value")
	result, err := leaderHost.SyncPropose(ctx, session, cmd)
	if err != nil {
		t.Fatalf("SyncPropose with NoOPSession failed: %v", err)
	}
	if result.Value != uint64(len("session-value")) {
		t.Errorf("result.Value = %d, want %d", result.Value, len("session-value"))
	}

	// Verify the entry replicated to all nodes.
	waitForReplication(t, cluster, trackers, 1, "session-key", "session-value", defaultTimeout)

	// Verify via SyncRead on the leader.
	readResult, err := leaderHost.SyncRead(ctx, 1, encodeKVGet("session-key"))
	if err != nil {
		t.Fatalf("SyncRead failed: %v", err)
	}
	val, ok := readResult.(string)
	if !ok {
		t.Fatalf("SyncRead returned type %T, want string", readResult)
	}
	if val != "session-value" {
		t.Fatalf("SyncRead = %q, want %q", val, "session-value")
	}
	t.Logf("NoOPSession proposal and read verified")
}

// TestIntegration_DB_GetNoOPSession verifies that the NodeHost.GetNoOPSession
// convenience method creates a valid no-op session that works for
// proposals in a real multi-node cluster.
func TestIntegration_DB_GetNoOPSession(t *testing.T) {
	cluster := newDBTestCluster(t, 3)
	defer cluster.close()

	trackers := cluster.startShard(1)
	leaderIdx := cluster.waitForClusterLeader(1, leaderElectionTimeout)
	t.Logf("leader elected at node[%d]", leaderIdx)

	// Warmup entry required after election (PhD 6.4).
	proposeAndWait(t, cluster, trackers, 1, "warmup", "ok")
	waitForReplication(t, cluster, trackers, 1, "warmup", "ok", defaultTimeout)

	// Get a no-op session from the NodeHost convenience method.
	leaderHost := cluster.hosts[leaderIdx]
	session := leaderHost.GetNoOPSession(1)

	if !session.IsNoOPSession() {
		t.Fatal("expected no-op session from GetNoOPSession")
	}
	if session.ShardID != 1 {
		t.Fatalf("session.ShardID = %d, want 1", session.ShardID)
	}

	// Use it for multiple proposals.
	ctx, cancel := context.WithTimeout(context.Background(), defaultTimeout)
	defer cancel()

	expected := make(map[string]string, 5)
	for i := range 5 {
		key := "getnoop-" + string(rune('a'+i))
		value := "val-" + key
		expected[key] = value

		cmd := encodeKVPut(key, value)
		result, err := leaderHost.SyncPropose(ctx, session, cmd)
		if err != nil {
			t.Fatalf("SyncPropose(%q) failed: %v", key, err)
		}
		if result.Value != uint64(len(value)) {
			t.Errorf("result.Value = %d, want %d", result.Value, len(value))
		}
	}

	// Verify all entries replicated.
	verifyAllData(t, cluster, trackers, 1, expected)
	t.Logf("GetNoOPSession: 5 proposals verified on all 3 nodes")
}

// TestIntegration_DB_TrackedSession_Lifecycle verifies the full lifecycle
// of a tracked client session: SyncGetSession to register, verify it is
// not a no-op session, use it for SyncPropose, then SyncCloseSession to
// unregister.
func TestIntegration_DB_TrackedSession_Lifecycle(t *testing.T) {
	cluster := newDBTestCluster(t, 3)
	defer cluster.close()

	trackers := cluster.startShard(1)
	leaderIdx := cluster.waitForClusterLeader(1, leaderElectionTimeout)
	t.Logf("leader elected at node[%d]", leaderIdx)

	// Warmup entry required after election (PhD 6.4).
	proposeAndWait(t, cluster, trackers, 1, "warmup", "ok")
	waitForReplication(t, cluster, trackers, 1, "warmup", "ok", defaultTimeout)

	leaderHost := cluster.hosts[leaderIdx]

	// Register a tracked session via SyncGetSession.
	getCtx, getCancel := context.WithTimeout(context.Background(), defaultTimeout)
	defer getCancel()

	session, err := leaderHost.SyncGetSession(getCtx, 1)
	if err != nil {
		t.Fatalf("SyncGetSession failed: %v", err)
	}
	t.Logf("tracked session registered: ShardID=%d, ClientID=%d",
		session.ShardID, session.ClientID())

	// Verify it is NOT a no-op session.
	if session.IsNoOPSession() {
		t.Fatal("expected tracked session, got no-op session")
	}

	// Use the tracked session for a proposal.
	propCtx, propCancel := context.WithTimeout(context.Background(), defaultTimeout)
	defer propCancel()

	cmd := encodeKVPut("tracked-key", "tracked-value")
	result, propErr := leaderHost.SyncPropose(propCtx, session, cmd)
	if propErr != nil {
		t.Fatalf("SyncPropose with tracked session failed: %v", propErr)
	}
	if result.Value != uint64(len("tracked-value")) {
		t.Errorf("result.Value = %d, want %d", result.Value, len("tracked-value"))
	}
	session.ProposalCompleted()

	// Verify the entry replicated.
	waitForReplication(t, cluster, trackers, 1, "tracked-key", "tracked-value", defaultTimeout)

	// Close the tracked session.
	closeCtx, closeCancel := context.WithTimeout(context.Background(), defaultTimeout)
	defer closeCancel()

	if closeErr := leaderHost.SyncCloseSession(closeCtx, session); closeErr != nil {
		t.Fatalf("SyncCloseSession failed: %v", closeErr)
	}
	t.Logf("tracked session closed successfully")
}

// TestIntegration_DB_ProposeSession verifies the async ProposeSession
// pattern that uses ProposeSession to register/unregister a session
// and waits on ResultC for completion.
func TestIntegration_DB_ProposeSession(t *testing.T) {
	cluster := newDBTestCluster(t, 3)
	defer cluster.close()

	trackers := cluster.startShard(1)
	leaderIdx := cluster.waitForClusterLeader(1, leaderElectionTimeout)
	t.Logf("leader elected at node[%d]", leaderIdx)

	// Warmup entry required after election (PhD 6.4).
	proposeAndWait(t, cluster, trackers, 1, "warmup", "ok")
	waitForReplication(t, cluster, trackers, 1, "warmup", "ok", defaultTimeout)

	leaderHost := cluster.hosts[leaderIdx]

	// Register a tracked session first.
	getCtx, getCancel := context.WithTimeout(context.Background(), defaultTimeout)
	defer getCancel()

	session, err := leaderHost.SyncGetSession(getCtx, 1)
	if err != nil {
		t.Fatalf("SyncGetSession failed: %v", err)
	}
	t.Logf("session registered: ClientID=%d", session.ClientID())

	// Use ProposeSession (async) to close the session. In the adapter,
	// ProposeSession delegates to CloseSession.
	rs, propErr := leaderHost.ProposeSession(session, defaultTimeout)
	if propErr != nil {
		t.Fatalf("ProposeSession failed: %v", propErr)
	}
	defer rs.Release()

	// Wait for the result via ResultC.
	select {
	case result := <-rs.ResultC():
		if !result.Completed() {
			t.Fatalf("ProposeSession did not complete successfully")
		}
		t.Logf("ProposeSession completed via ResultC")
	case <-time.After(defaultTimeout):
		t.Fatalf("ProposeSession timed out waiting for ResultC")
	}
}

// TestIntegration_DB_DoubleCloseSession_Rejected verifies that closing an
// already-closed tracked session returns an error. In dragonboat's behavior
// this manifests as ErrRejected because the session is no longer registered
// with the state machine.
func TestIntegration_DB_DoubleCloseSession_Rejected(t *testing.T) {
	cluster := newDBTestCluster(t, 3)
	defer cluster.close()

	trackers := cluster.startShard(1)
	leaderIdx := cluster.waitForClusterLeader(1, leaderElectionTimeout)
	t.Logf("leader elected at node[%d]", leaderIdx)

	// Warmup entry required after election (PhD 6.4).
	proposeAndWait(t, cluster, trackers, 1, "warmup", "ok")

	leaderHost := cluster.hosts[leaderIdx]

	// Register a tracked session.
	getCtx, getCancel := context.WithTimeout(context.Background(), defaultTimeout)
	defer getCancel()

	session, err := leaderHost.SyncGetSession(getCtx, 1)
	if err != nil {
		t.Fatalf("SyncGetSession failed: %v", err)
	}
	t.Logf("tracked session registered: ShardID=%d, ClientID=%d",
		session.ShardID, session.ClientID())

	// First close should succeed.
	closeCtx1, closeCancel1 := context.WithTimeout(context.Background(), defaultTimeout)
	defer closeCancel1()

	if closeErr := leaderHost.SyncCloseSession(closeCtx1, session); closeErr != nil {
		t.Fatalf("first SyncCloseSession failed: %v", closeErr)
	}
	t.Logf("first SyncCloseSession succeeded")

	// Second close on the same session should be rejected.
	closeCtx2, closeCancel2 := context.WithTimeout(context.Background(), defaultTimeout)
	defer closeCancel2()

	doubleCloseErr := leaderHost.SyncCloseSession(closeCtx2, session)
	if doubleCloseErr == nil {
		t.Fatal("expected error when closing an already-closed session, got nil")
	}
	t.Logf("double close returned expected error: %v", doubleCloseErr)

	// Verify the error is ErrRejected, which is the dragonboat behavior
	// when an unregistered session is closed.
	if !errors.Is(doubleCloseErr, dragonboat.ErrRejected) {
		t.Errorf("expected ErrRejected, got: %v", doubleCloseErr)
	}
}

// TestIntegration_DB_OnDiskStateMachine_RejectsTrackedSession verifies
// that on-disk state machines only support NoOP sessions. Attempting to
// register a tracked session via SyncGetSession on a shard backed by an
// IOnDiskStateMachine must return an error (ErrRejected or similar),
// because on-disk SMs manage their own persistence and do not support
// the at-most-once delivery semantics of tracked sessions.
func TestIntegration_DB_OnDiskStateMachine_RejectsTrackedSession(t *testing.T) {
	cluster := newDBTestCluster(t, 3)
	defer cluster.close()

	// Start the shard with an on-disk state machine.
	cluster.startOnDiskShard(1)
	leaderIdx := cluster.waitForClusterLeader(1, leaderElectionTimeout)
	t.Logf("on-disk SM cluster leader at node[%d]", leaderIdx)

	leaderHost := cluster.hosts[leaderIdx]

	// First, verify that NoOP sessions work correctly with the
	// on-disk state machine.
	session := dbClient.NewNoOPSession(1, nil)
	warmupCtx, warmupCancel := context.WithTimeout(context.Background(), defaultTimeout)
	defer warmupCancel()

	warmupCmd := encodeKVPut("ondisk-warmup", "ok")
	_, warmupErr := leaderHost.SyncPropose(warmupCtx, session, warmupCmd)
	if warmupErr != nil {
		t.Fatalf("SyncPropose with NoOPSession on on-disk SM failed: %v", warmupErr)
	}
	t.Logf("NoOP session proposal succeeded on on-disk SM")

	// Attempt to register a tracked session on the on-disk SM shard.
	// On-disk state machines should reject tracked session registration
	// because they manage their own persistence and do not support the
	// session tracking machinery.
	getCtx, getCancel := context.WithTimeout(context.Background(), defaultTimeout)
	defer getCancel()

	_, sessionErr := leaderHost.SyncGetSession(getCtx, 1)
	if sessionErr == nil {
		t.Fatal("SyncGetSession should fail on an on-disk state machine shard; tracked sessions are not supported")
	}

	// The error should indicate rejection. In dragonboat semantics,
	// attempting to register a tracked session on an on-disk SM
	// returns ErrRejected.
	if !errors.Is(sessionErr, dragonboat.ErrRejected) {
		t.Errorf("expected ErrRejected for tracked session on on-disk SM, got: %v", sessionErr)
	}
	t.Logf("SyncGetSession on on-disk SM correctly rejected: %v", sessionErr)
}
