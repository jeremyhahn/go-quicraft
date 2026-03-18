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

	dbClient "github.com/jeremyhahn/go-quicraft/contrib/dragonboat/client"
)

// TestIntegration_DB_TermMonotonicity verifies that Raft terms are strictly
// monotonically increasing across leader elections per Raft Section 5.2. After
// a leader failure and new election, the new term must be strictly greater than
// the old term.
func TestIntegration_DB_TermMonotonicity(t *testing.T) {
	cluster := newDBTestCluster(t, 3)
	defer cluster.close()

	trackers := cluster.startShard(1)
	leaderIdx := cluster.waitForClusterLeader(1, leaderElectionTimeout)
	t.Logf("initial leader at node[%d]", leaderIdx)

	// Warmup entry required after election (PhD 6.4).
	proposeAndWait(t, cluster, trackers, 1, "warmup", "ok")
	waitForReplication(t, cluster, trackers, 1, "warmup", "ok", defaultTimeout)

	// Record the current term from the leader.
	leaderHost := cluster.hosts[leaderIdx]
	_, oldTerm, valid, err := leaderHost.GetLeaderID(1)
	if err != nil {
		t.Fatalf("GetLeaderID failed: %v", err)
	}
	if !valid {
		t.Fatal("GetLeaderID returned valid=false on elected leader")
	}
	if oldTerm == 0 {
		t.Fatal("oldTerm should be > 0 after election")
	}
	t.Logf("old leader term = %d", oldTerm)

	// Stop the leader to force a new election.
	oldLeaderIdx := leaderIdx
	cluster.stopNode(oldLeaderIdx)
	t.Logf("stopped leader node[%d]", oldLeaderIdx)

	// Wait for a new leader among survivors.
	newLeaderIdx := cluster.waitForClusterLeader(1, failoverTimeout)
	if newLeaderIdx == oldLeaderIdx {
		t.Fatal("new leader must not be the stopped node")
	}
	t.Logf("new leader at node[%d]", newLeaderIdx)

	// Get the new term from the new leader.
	newLeaderHost := cluster.hosts[newLeaderIdx]
	_, newTerm, newValid, newErr := newLeaderHost.GetLeaderID(1)
	if newErr != nil {
		t.Fatalf("GetLeaderID on new leader failed: %v", newErr)
	}
	if !newValid {
		t.Fatal("GetLeaderID returned valid=false on new leader")
	}

	// Assert strict monotonicity: newTerm > oldTerm (Raft Section 5.2).
	if newTerm <= oldTerm {
		t.Fatalf("term monotonicity violated: newTerm=%d <= oldTerm=%d", newTerm, oldTerm)
	}
	t.Logf("term monotonicity verified: oldTerm=%d < newTerm=%d", oldTerm, newTerm)
}

// TestIntegration_DB_PreVoteElection verifies that a cluster configured with
// the PreVote protocol extension operates correctly. PreVote prevents
// disruptive elections from partitioned nodes while still allowing normal
// propose and read operations.
func TestIntegration_DB_PreVoteElection(t *testing.T) {
	cluster := newDBTestCluster(t, 3)
	defer cluster.close()

	// Default config has PreVote enabled; use it directly.
	trackers := cluster.startShard(1)
	leaderIdx := cluster.waitForClusterLeader(1, leaderElectionTimeout)
	t.Logf("PreVote cluster leader at node[%d]", leaderIdx)

	// Warmup entry required after election (PhD 6.4).
	proposeAndWait(t, cluster, trackers, 1, "warmup", "ok")
	waitForReplication(t, cluster, trackers, 1, "warmup", "ok", defaultTimeout)

	// Propose data through the leader.
	proposeAndWait(t, cluster, trackers, 1, "prevote-key", "prevote-value")
	waitForReplication(t, cluster, trackers, 1, "prevote-key", "prevote-value", defaultTimeout)

	// Verify linearizable read round-trip works with PreVote enabled.
	leaderHost := cluster.hosts[leaderIdx]
	ctx, cancel := context.WithTimeout(context.Background(), defaultTimeout)
	defer cancel()

	result, err := leaderHost.SyncRead(ctx, 1, encodeKVGet("prevote-key"))
	if err != nil {
		t.Fatalf("SyncRead with PreVote enabled failed: %v", err)
	}
	val, ok := result.(string)
	if !ok {
		t.Fatalf("SyncRead returned type %T, want string", result)
	}
	if val != "prevote-value" {
		t.Fatalf("SyncRead = %q, want %q", val, "prevote-value")
	}
	t.Logf("PreVote cluster propose + read verified: %q = %q", "prevote-key", val)

	// Propose multiple entries to ensure sustained operation.
	expected := proposeNEntries(t, cluster, trackers, 1, 10, "prevote-batch")
	verifyAllData(t, cluster, trackers, 1, expected)
	t.Logf("PreVote cluster replicated %d entries to all nodes", len(expected))

	// Verify async propose also works with PreVote.
	session := dbClient.NewNoOPSession(1, nil)
	propCtx, propCancel := context.WithTimeout(context.Background(), defaultTimeout)
	defer propCancel()

	cmd := encodeKVPut("prevote-async", "async-value")
	syncResult, syncErr := leaderHost.SyncPropose(propCtx, session, cmd)
	if syncErr != nil {
		t.Fatalf("SyncPropose with PreVote enabled failed: %v", syncErr)
	}
	if syncResult.Value != uint64(len("async-value")) {
		t.Fatalf("SyncPropose result.Value = %d, want %d", syncResult.Value, len("async-value"))
	}
	t.Logf("PreVote cluster async propose verified")
}
