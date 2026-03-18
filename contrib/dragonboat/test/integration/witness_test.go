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
	"testing"
	"time"
)

// TestIntegration_DB_SyncRequestAddWitness verifies that a witness
// replica can be added to the cluster via SyncRequestAddWitness.
// Witnesses participate in voting but do not maintain a full log.
// Matches dragonboat's TestSyncRequestAddWitness.
func TestIntegration_DB_SyncRequestAddWitness(t *testing.T) {
	cluster := newDBTestCluster(t, 3)
	defer cluster.close()

	trackers := cluster.startShard(1)
	leaderIdx := cluster.waitForClusterLeader(1, leaderElectionTimeout)
	t.Logf("leader elected at node[%d]", leaderIdx)

	// Warmup entry required after election (PhD 6.4).
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
	if len(membership.Witnesses) != 0 {
		t.Fatalf("expected 0 witnesses initially, got %d", len(membership.Witnesses))
	}

	// Add witness replica 30 at a dummy address.
	addCtx, addCancel := context.WithTimeout(context.Background(), defaultTimeout)
	defer addCancel()
	if addErr := leaderHost.SyncRequestAddWitness(addCtx, 1, 30, "127.0.0.1:59990", membership.ConfigChangeID); addErr != nil {
		t.Fatalf("SyncRequestAddWitness failed: %v", addErr)
	}

	// Re-read membership and verify witness replica 30 is present.
	verifyCtx, verifyCancel := context.WithTimeout(context.Background(), defaultTimeout)
	defer verifyCancel()
	updated, mErr := leaderHost.SyncGetShardMembership(verifyCtx, 1)
	if mErr != nil {
		t.Fatalf("SyncGetShardMembership after add witness failed: %v", mErr)
	}
	if _, exists := updated.Witnesses[30]; !exists {
		t.Fatalf("witness replica 30 not found in membership.Witnesses after add; Witnesses=%v", updated.Witnesses)
	}
	t.Logf("SyncRequestAddWitness succeeded: Witnesses=%d (replica 30 added)", len(updated.Witnesses))
}

// TestIntegration_DB_RequestAddWitness_Async verifies the async
// RequestAddWitness variant that returns a RequestState with a
// ResultC channel for completion notification.
func TestIntegration_DB_RequestAddWitness_Async(t *testing.T) {
	cluster := newDBTestCluster(t, 3)
	defer cluster.close()

	trackers := cluster.startShard(1)
	leaderIdx := cluster.waitForClusterLeader(1, leaderElectionTimeout)
	t.Logf("leader elected at node[%d]", leaderIdx)

	// Warmup entry required after election (PhD 6.4).
	proposeAndWait(t, cluster, trackers, 1, "warmup", "ok")
	waitForReplication(t, cluster, trackers, 1, "warmup", "ok", defaultTimeout)

	leaderHost := cluster.hosts[leaderIdx]

	// Get current membership for ConfigChangeID.
	ctx, cancel := context.WithTimeout(context.Background(), defaultTimeout)
	defer cancel()
	membership, err := leaderHost.SyncGetShardMembership(ctx, 1)
	if err != nil {
		t.Fatalf("SyncGetShardMembership failed: %v", err)
	}

	// Async add witness replica 31.
	rs, addErr := leaderHost.RequestAddWitness(1, 31, "127.0.0.1:59991", membership.ConfigChangeID, defaultTimeout)
	if addErr != nil {
		t.Fatalf("RequestAddWitness failed: %v", addErr)
	}
	defer rs.Release()

	// Wait for the result via ResultC.
	select {
	case result := <-rs.ResultC():
		if !result.Completed() {
			t.Fatalf("RequestAddWitness did not complete successfully; rejected=%v", result.Rejected())
		}
		t.Logf("RequestAddWitness completed via ResultC")
	case <-time.After(defaultTimeout):
		t.Fatal("RequestAddWitness timed out waiting for ResultC")
	}

	// Verify witness 31 appears in membership.
	verifyCtx, verifyCancel := context.WithTimeout(context.Background(), defaultTimeout)
	defer verifyCancel()
	updated, mErr := leaderHost.SyncGetShardMembership(verifyCtx, 1)
	if mErr != nil {
		t.Fatalf("SyncGetShardMembership after async add witness failed: %v", mErr)
	}
	if _, exists := updated.Witnesses[31]; !exists {
		t.Fatalf("witness replica 31 not found in membership.Witnesses after async add; Witnesses=%v", updated.Witnesses)
	}
	t.Logf("RequestAddWitness async: Witnesses=%d (replica 31 added)", len(updated.Witnesses))
}
