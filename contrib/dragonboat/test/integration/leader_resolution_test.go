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

	dbClient "github.com/jeremyhahn/go-quicraft/contrib/dragonboat/client"
)

// TestIntegration_DB_ProposeSurvivesLeaderLoss proposes between a leader's death and the next election; tryProposeToLeader must re-resolve the leader each attempt.
func TestIntegration_DB_ProposeSurvivesLeaderLoss(t *testing.T) {
	cluster := newDBTestCluster(t, 3)
	defer cluster.close()

	trackers := cluster.startShard(1)
	leaderIdx := cluster.waitForClusterLeader(1, leaderElectionTimeout)

	// Warmup entry required after election (PhD 6.4).
	proposeAndWait(t, cluster, trackers, 1, "warmup", "ok")
	waitForReplication(t, cluster, trackers, 1, "warmup", "ok", defaultTimeout)

	// Stop the incumbent and propose at once, without waiting for a successor.
	cluster.stopNode(leaderIdx)
	t.Logf("stopped incumbent node[%d]; proposing without waiting for a successor", leaderIdx)

	session := dbClient.NewNoOPSession(1, nil)
	proposal := cluster.proposeToLeader(t, session, encodeKVPut("post-loss", "committed"), failoverTimeout)
	if proposal.Index == leaderIdx {
		t.Fatalf("entry was accepted by the stopped node[%d]", leaderIdx)
	}
	if proposal.Result.Value != uint64(len("committed")) {
		t.Fatalf("result.Value = %d, want %d", proposal.Result.Value, len("committed"))
	}
	t.Logf("entry committed on the successor, node[%d]", proposal.Index)

	// And it must be a real commit, visible on every surviving replica.
	waitForReplication(t, cluster, trackers, 1, "post-loss", "committed", failoverTimeout)
}
