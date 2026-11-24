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

//go:build integration && quicraft_netfault

// Fixture for proposing while the deposed leader does not know its successor, built by network isolation. Run with: go test -tags 'integration quicraft_netfault' ./test/integration/e2e/...
package e2e_test

import (
	"testing"
	"time"

	"github.com/jeremyhahn/go-quicraft/test/proposer"
)

// TestNetPartition_ProposeToLeader_ResolvesPastAnIsolatedIncumbent checks a proposal issued right after isolating the leader commits, while a cached-leader propose does not.
func TestNetPartition_ProposeToLeader_ResolvesPastAnIsolatedIncumbent(t *testing.T) {
	cluster := newTestCluster(t, 3)
	defer cluster.close()

	const shardID uint64 = 1
	trackers := cluster.startShard(shardID)
	cluster.waitForClusterLeader(shardID, leaderElectionTimeout)

	proposeToLeader(t, cluster, shardID, encodeKVPut("ptl-pre", "ok"), defaultTimeout)
	waitForReplication(t, cluster, trackers, shardID, "ptl-pre", "ok", defaultTimeout)

	// The observation a test would cache and then treat as fixed.
	staleIdx := cluster.findLeader(shardID)
	if staleIdx < 0 {
		t.Fatalf("no leader for shard %d after the warmup write", shardID)
	}

	cluster.isolate(staleIdx)
	requireFaultInjection(t, 1) // one isolated replica, both directions
	t.Logf("node[%d] isolated while it still believes it leads shard %d",
		staleIdx, shardID)

	// Propose at once so the first resolution hits the incumbent and the second finds the new leader.
	result := proposeToLeader(t, cluster, shardID,
		encodeKVPut("ptl-moved", "committed"), failoverTimeout)
	if result.Value != uint64(len("committed")) {
		t.Errorf("result.Value = %d, want %d", result.Value, len("committed"))
	}

	// The incumbent has lost the shard; a single cached-index Propose must fail.
	waitForCondition(t, failoverTimeout, "the isolated incumbent steps down", func() bool {
		return !isLeaderAt(cluster, staleIdx, shardID)
	})
	//quicraft:direct-propose the deposed incumbent IS the subject of this assertion
	_, err := proposer.Once(cluster.hosts[staleIdx], shardID,
		encodeKVPut("ptl-stale", "rejected"), 3*time.Second)
	if err == nil {
		t.Fatalf("isolated node[%d] reported a proposal as committed while cut off "+
			"from both of its peers; a majority of 3 is 2 and it can reach neither",
			staleIdx)
	}
	if !proposer.IsNotLeaderRejection(err) {
		t.Fatalf("isolated node[%d] refused the proposal with %v; this fixture needs "+
			"the not-leader rejection, so either the isolation did not take effect "+
			"or the refusal came from an unrelated fault", staleIdx, err)
	}
	t.Logf("cached-leader pattern refused by the isolated incumbent: %v", err)

	// Check the majority only; the isolated node is unreachable.
	for i := range cluster.hosts {
		if i == staleIdx {
			continue
		}
		waitForCondition(t, defaultTimeout,
			"the majority applies the committed entry",
			func() bool {
				v, ok := valueAt(cluster, trackers, shardID, i, "ptl-moved")
				return ok && v == "committed"
			},
		)
	}

	// The refused proposal must not apply anywhere.
	for i := range cluster.hosts {
		if v, ok := valueAt(cluster, trackers, shardID, i, "ptl-stale"); ok {
			t.Errorf("node[%d] applied ptl-stale=%q, a value the isolated node[%d] "+
				"was told it could not commit", i, v, staleIdx)
		}
	}

	// After the heal the incumbent adopts the majority's log, and the value it
	// was refused must not survive that.
	cluster.rejoin(staleIdx)
	waitForSingleLeader(t, cluster, shardID, failoverTimeout)
	waitForCondition(t, defaultTimeout, "the rejoined node adopts the majority's entry",
		func() bool {
			v, ok := valueAt(cluster, trackers, shardID, staleIdx, "ptl-moved")
			return ok && v == "committed"
		},
	)
	if v, ok := valueAt(cluster, trackers, shardID, staleIdx, "ptl-stale"); ok {
		t.Errorf("node[%d] applied ptl-stale=%q after rejoining; the majority's log "+
			"never held that entry", staleIdx, v)
	}
}
