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

// TestIntegration_DB_ProposeSurvivesLeaderLoss proposes into the gap between
// one leader dying and the next being elected, and requires the entry to land.
//
// It guards the RE-RESOLVE inside tryProposeToLeader, which nothing else can.
// The build gate in the parent module (test/buildgates) enforces that proposals
// go through these helpers; it cannot tell whether the helper still resolves
// the leader on every attempt. Hoist that resolve out of the retry loop and
// every other test in this suite stays green while this one fails.
//
// TestIntegration_DB_LeaderFailover covers the neighbouring case and
// deliberately does not cover this one: it waits for the successor with
// waitForClusterLeader BEFORE proposing, so it never enters the window.
//
// The window is real and wide. Measured on this suite, a host observed while
// the cluster had a leader refused 1595 consecutive proposals after the
// incumbent was stopped -- every one of them the not-leader rejection, which
// this adapter's translateError reports as the bare ErrShardNotReady sentinel
// with no mention of leadership at all.
func TestIntegration_DB_ProposeSurvivesLeaderLoss(t *testing.T) {
	cluster := newDBTestCluster(t, 3)
	defer cluster.close()

	trackers := cluster.startShard(1)
	leaderIdx := cluster.waitForClusterLeader(1, leaderElectionTimeout)

	// Warmup entry required after election (PhD 6.4).
	proposeAndWait(t, cluster, trackers, 1, "warmup", "ok")
	waitForReplication(t, cluster, trackers, 1, "warmup", "ok", defaultTimeout)

	// Stop the incumbent and propose at once, WITHOUT waiting for a successor.
	// Every live replica is now a follower whose leader has gone, and will
	// refuse the proposal until the election completes.
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
