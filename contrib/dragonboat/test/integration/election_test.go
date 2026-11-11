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
	"testing"
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
	oldLeaderID, oldTerm, valid, err := leaderHost.GetLeaderID(1)
	if err != nil {
		t.Fatalf("GetLeaderID failed: %v", err)
	}
	if !valid {
		t.Fatal("GetLeaderID returned valid=false on elected leader")
	}
	if oldLeaderID != uint64(leaderIdx+1) {
		t.Fatalf("GetLeaderID leaderID = %d, want %d", oldLeaderID, leaderIdx+1)
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
