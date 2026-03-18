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

	dbClient "github.com/jeremyhahn/go-quicraft/contrib/dragonboat/client"
)

// TestIntegration_DB_QuorumLoss_ProposalFails verifies that when a 3-node
// cluster loses quorum (2 of 3 nodes stopped), proposals fail because the
// leader cannot commit entries without a majority. This exercises the
// CheckQuorum behavior per Raft specification.
func TestIntegration_DB_QuorumLoss_ProposalFails(t *testing.T) {
	cluster := newDBTestCluster(t, 3)
	defer cluster.close()

	trackers := cluster.startShard(1)
	leaderIdx := cluster.waitForClusterLeader(1, leaderElectionTimeout)
	t.Logf("leader elected at node[%d]", leaderIdx)

	// Warmup entry required after election (PhD 6.4).
	proposeAndWait(t, cluster, trackers, 1, "warmup", "ok")
	waitForReplication(t, cluster, trackers, 1, "warmup", "ok", defaultTimeout)

	// Verify the cluster is healthy with a successful proposal before quorum loss.
	proposeAndWait(t, cluster, trackers, 1, "pre-quorum-loss", "healthy")
	t.Logf("pre-quorum-loss proposal succeeded")

	// Identify the two follower nodes and stop them.
	followersStopped := 0
	for i := range cluster.hosts {
		if cluster.stopped[i].Load() || i == leaderIdx {
			continue
		}
		cluster.stopNode(i)
		followersStopped++
		t.Logf("stopped follower node[%d]", i)
	}
	if followersStopped != 2 {
		t.Fatalf("expected to stop 2 followers, stopped %d", followersStopped)
	}

	// Attempt a proposal with a short timeout. Without quorum, this must fail
	// because the leader cannot get acknowledgment from a majority of nodes.
	leaderHost := cluster.hosts[leaderIdx]
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	session := dbClient.NewNoOPSession(1, nil)
	cmd := encodeKVPut("quorum-loss-key", "should-fail")
	_, err := leaderHost.SyncPropose(ctx, session, cmd)
	if err == nil {
		t.Fatal("SyncPropose should fail when quorum is lost, but it succeeded")
	}
	t.Logf("SyncPropose correctly failed under quorum loss: %v", err)

	// Also verify SyncRead fails without quorum (linearizable read requires
	// leader confirmation which depends on quorum).
	readCtx, readCancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer readCancel()

	_, readErr := leaderHost.SyncRead(readCtx, 1, encodeKVGet("warmup"))
	if readErr == nil {
		t.Fatal("SyncRead should fail when quorum is lost, but it succeeded")
	}
	t.Logf("SyncRead correctly failed under quorum loss: %v", readErr)
}
