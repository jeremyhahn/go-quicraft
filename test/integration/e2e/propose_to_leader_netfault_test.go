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

// The regression fixture for the cached-leader-index flake.
//
// It lives in the netfault suite because the failure needs a live replica that
// has lost leadership AND does not yet know who took over. Host.Propose only
// refuses outright in that second condition: when the local replica is not
// leader but a leader IS known, the engine forwards the proposal and it
// commits. That is why twenty call sites could cache a leader index for months
// and pass — an ordinary re-election leaves the deposed replica knowing its
// successor within a heartbeat, and the propose gets forwarded.
//
// The window where it does not is the one CI hit: run 10735 reported
// "not leader for shard 1, LEADER UNKNOWN". A process kill cannot construct
// that state, because a killed node answers nothing at all. Isolation can: the
// node keeps running, steps down when check-quorum fires, and hears nothing
// further, so leaderID stays 0 for as long as the isolation holds.
//
// Run with:
//
//	go test -tags 'integration quicraft_netfault' ./test/integration/e2e/...
package e2e_test

import (
	"testing"
	"time"

	"github.com/jeremyhahn/go-quicraft/test/proposer"
)

// TestNetPartition_ProposeToLeader_ResolvesPastAnIsolatedIncumbent asserts
// that a proposal issued while leadership is moving out from under it still
// commits, and that the pre-fix pattern in the same state does not.
//
// ORDERING IS THE WHOLE TEST. The write is issued IMMEDIATELY after the
// incumbent is cut off, while it still believes it leads and findLeader still
// returns it. The helper's first resolution therefore lands on the node that is
// about to lose the shard, and only a resolution taken again on the next
// attempt can find the majority's new leader.
//
// An earlier draft waited for the new leader to be elected BEFORE proposing.
// That version passed with the resolve hoisted out of the retry loop — by the
// time it ran, one lookup already returned the right node, so the property it
// claimed to cover was never exercised. Measured: hoisted resolve, 1.63s,
// green. Do not reintroduce that ordering.
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

	// No wait here, deliberately. The proposal races the step-down: the first
	// resolution gets the incumbent, that attempt fails not-leader when check
	// quorum takes the shard away from it, and the SECOND resolution — the one
	// a cached index cannot make — finds the majority's leader.
	result := proposeToLeader(t, cluster, shardID,
		encodeKVPut("ptl-moved", "committed"), failoverTimeout)
	if result.Value != uint64(len("committed")) {
		t.Errorf("result.Value = %d, want %d", result.Value, len("committed"))
	}

	// The entry committed, so the incumbent has necessarily lost the shard by
	// now. Assert the pre-fix pattern against it: one cached index, one Propose,
	// no retry. This is what the converted call sites used to do, and it doubles
	// as proof that the isolation produced the leader-unknown state rather than
	// a healthy cluster that forwarded the proposal on.
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
	t.Logf("pre-fix pattern refused by the isolated incumbent: %v", err)

	// Assert on the majority only: the isolated node is running but unreachable,
	// so waitForReplication — which requires every node that is not STOPPED —
	// would be asserting that a partition heals itself.
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

	// The refused proposal must not APPLY anywhere. This reads state machines,
	// so it does not speak to what may sit unapplied in the isolated replica's
	// own log; that entry cannot commit without a quorum it cannot reach, and
	// the heal below is where it would have to disappear.
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
