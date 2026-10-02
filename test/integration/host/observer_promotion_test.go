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

package host_test

import (
	"context"
	"testing"

	"github.com/jeremyhahn/go-quicraft/pkg/config"
	"github.com/jeremyhahn/go-quicraft/test/membership"
)

// TestHostAPI_ObserverPromotedInPlace is the end-to-end red/green for #218.
//
// A 3-voter cluster adds a 4th replica as an observer, starts it as one
// (Config.IsObserver), and promotes it with an AddNode for the same replica
// ID, address and identity. Without a restart, the promoted replica must then:
//
//   - report itself a voter (ShardInfo.IsObserver false);
//   - serve linearizable reads (a follower ReadIndex forwarded to the leader);
//   - vote: with one original voter stopped, the three survivors are exactly
//     a quorum of four, so electing a leader and committing need its vote and
//     its ack; and
//   - be elected leader, and serve reads as leader.
//
// Before #218 the committed promotion latched a role mismatch on the replica
// (configured observer, listed voter): it refused every read with
// ErrWitnessRoleMismatch and every campaign, including a leadership transfer,
// until its shard was stopped and restarted with IsObserver=false.
//
// It runs under the default configuration and under LeaseRead, where the
// promotion also starts the replica promising vote windows in its heartbeat
// acks.
func TestHostAPI_ObserverPromotedInPlace(t *testing.T) {
	tests := []struct {
		name  string
		cfgFn func(shardID, replicaID uint64) config.Config
	}{
		{name: "default", cfgFn: testShardConfig},
		{name: "lease read", cfgFn: testLeaseReadShardConfig},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			observerPromotedInPlace(t, tt.cfgFn)
		})
	}
}

func observerPromotedInPlace(t *testing.T, cfgFn func(shardID, replicaID uint64) config.Config) {
	cluster := newTestCluster(t, 3)
	t.Cleanup(func() { cluster.close() })

	const (
		shardID    uint64 = 1
		observerID uint64 = 4
	)

	trackers := cluster.startShardWithConfig(shardID, cfgFn)
	cluster.waitForClusterLeader(shardID, leaderElectionTimeout)
	data := proposeNEntries(t, cluster, trackers, shardID, 5, "pre-observer")

	// Phase 1: add replica 4 as an observer and start it as one.
	joined := addObserverToCluster(t, cluster, shardID, observerID, cfgFn)
	waitForMembershipConverged(t, cluster, shardID, []uint64{1, 2, 3}, nil, membershipTimeout)
	waitForNodeData(t, joined.tracker, shardID, observerID, data, membershipTimeout)
	if info, err := joined.host.GetShardInfo(shardID); err != nil || !info.IsObserver {
		t.Fatalf("phase 1: the joined replica's ShardInfo = %+v (err %v), want IsObserver", info, err)
	}
	// The state machine the replica runs now is the one it must still be
	// running after the promotion: the promotion happens in place.
	smBefore := joined.tracker.Get(shardID, observerID)

	// Phase 2: promote it — AddNode, same replica ID, address and identity.
	membership.PromoteNode(t, cluster.leaderHost, shardID, observerID,
		joined.addr, joined.host.NodeIdentity(), membershipTimeout)
	waitForMembershipConverged(t, cluster, shardID, []uint64{1, 2, 3, observerID}, nil, membershipTimeout)

	waitForCondition(t, membershipTimeout, "the promoted replica reports itself a voter", func() bool {
		info, err := joined.host.GetShardInfo(shardID)
		return err == nil && !info.IsObserver
	})

	// Phase 3: it serves linearizable reads as a follower.
	waitForPromotedRead(t, cluster, joined, shardID, "pre-observer-000", data["pre-observer-000"])

	// Phase 4: it votes. Stop one original voter other than the leader: the
	// three survivors, the promoted replica among them, are exactly a quorum
	// of the four voters, so a write commits only with the promoted replica's
	// ack and any election held from here needs its vote.
	leaderIdx := cluster.waitForClusterLeader(shardID, leaderElectionTimeout)
	stopIdx := (leaderIdx + 1) % 3
	if stopIdx == joined.idx {
		t.Fatalf("fixture: picked the promoted replica (node[%d]) to stop", stopIdx)
	}
	cluster.stopNode(stopIdx)
	t.Logf("phase 4: stopped node[%d]; live voters are exactly a quorum", stopIdx)

	postStop := proposeNEntries(t, cluster, trackers, shardID, 3, "post-stop")
	waitForNodeData(t, joined.tracker, shardID, observerID, postStop, membershipTimeout)

	// Phase 5: it can be elected leader, and serves reads as leader.
	transferLeadershipTo(t, cluster, shardID, joined.idx, failoverTimeout)
	waitForPromotedRead(t, cluster, joined, shardID, "post-stop-002", postStop["post-stop-002"])

	asLeader := proposeNEntries(t, cluster, trackers, shardID, 3, "as-leader")
	waitForNodeData(t, joined.tracker, shardID, observerID, asLeader, membershipTimeout)

	if smAfter := joined.tracker.Get(shardID, observerID); smAfter != smBefore {
		t.Fatal("the promoted replica is running a different state machine than it " +
			"joined with: the shard was restarted, not promoted in place")
	}
}

// waitForPromotedRead waits for a linearizable read ON the promoted replica to
// return want for key.
func waitForPromotedRead(t *testing.T, cluster *testCluster, joined addNodeResult, shardID uint64, key, want string) {
	t.Helper()
	var lastErr error
	ok := waitForConditionNoFatal(membershipTimeout, func() bool {
		ctx, cancel := context.WithTimeout(context.Background(), defaultTimeout)
		defer cancel()
		//quicraft:direct-read the promoted replica IS the subject: its own ReadIndex must be served, not a leader's
		got, err := joined.host.SyncRead(ctx, shardID, key)
		if err != nil {
			lastErr = err
			return false
		}
		val, isString := got.(string)
		return isString && val == want
	})
	if !ok {
		t.Fatalf("the promoted replica node[%d] never served a linearizable read of %q "+
			"(last error: %v)%s", joined.idx, key, lastErr, shardStateDump(cluster, shardID))
	}
	t.Logf("node[%d] served a linearizable read of %q", joined.idx, key)
}
