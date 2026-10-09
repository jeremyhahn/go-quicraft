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
	"testing"

	quicraft "github.com/jeremyhahn/go-quicraft/pkg"
	"github.com/jeremyhahn/go-quicraft/test/membership"
)

// replicaProgressOf returns the leader's ShardInfo.Replicas entry for
// replicaID, and the leader's LastApplied, or ok=false when there is no leader
// yet or the entry is absent.
func replicaProgressOf(cluster *testCluster, shardID, replicaID uint64) (rp quicraft.ReplicaProgress, applied uint64, ok bool) {
	leader := cluster.leaderHost(shardID)
	if leader == nil {
		return rp, 0, false
	}
	info, err := leader.GetShardInfo(shardID)
	if err != nil {
		return rp, 0, false
	}
	for i := range info.Replicas {
		if info.Replicas[i].ReplicaID == replicaID {
			return info.Replicas[i], info.LastApplied, true
		}
	}
	return rp, info.LastApplied, false
}

// TestHostAPI_ReplicaProgressReportsObserverThenVoter pins that the leader's
// ShardInfo.Replicas covers an observer with its role and match index, so an
// embedder doing learner-then-promote replacement can see the learner catch up,
// and that the entry's role changes to voter once the observer is promoted.
func TestHostAPI_ReplicaProgressReportsObserverThenVoter(t *testing.T) {
	cluster := newTestCluster(t, 3)
	t.Cleanup(func() { cluster.close() })

	const (
		shardID    uint64 = 1
		observerID uint64 = 4
	)

	trackers := cluster.startShardWithConfig(shardID, testShardConfig)
	cluster.waitForClusterLeader(shardID, leaderElectionTimeout)
	data := proposeNEntries(t, cluster, trackers, shardID, 5, "pre-observer")

	joined := addObserverToCluster(t, cluster, shardID, observerID, testShardConfig)
	waitForMembershipConverged(t, cluster, shardID, []uint64{1, 2, 3}, nil, membershipTimeout)
	waitForNodeData(t, joined.tracker, shardID, observerID, data, membershipTimeout)

	// The observer appears in the leader's Replicas with its role, and its
	// Match reaches the leader's applied index. The first GetShardInfo enables
	// publication, so the entry shows up after the next tick.
	waitForCondition(t, membershipTimeout, "the observer's Match reaches the leader's applied index", func() bool {
		rp, applied, ok := replicaProgressOf(cluster, shardID, observerID)
		return ok && rp.Role == quicraft.ReplicaRoleObserver && applied > 0 && rp.Match >= applied
	})

	// The leader's other two voters are reported too, with the voter role.
	waitForCondition(t, membershipTimeout, "the leader reports its two voters and the observer", func() bool {
		leader := cluster.leaderHost(shardID)
		if leader == nil {
			return false
		}
		info, err := leader.GetShardInfo(shardID)
		if err != nil {
			return false
		}
		voters, observers := 0, 0
		for i := range info.Replicas {
			switch info.Replicas[i].Role {
			case quicraft.ReplicaRoleVoter:
				voters++
			case quicraft.ReplicaRoleObserver:
				observers++
			}
		}
		return voters == 2 && observers == 1
	})

	membership.PromoteNode(t, cluster.leaderHost, shardID, observerID,
		joined.addr, joined.host.NodeIdentity(), membershipTimeout)
	waitForMembershipConverged(t, cluster, shardID, []uint64{1, 2, 3, observerID}, nil, membershipTimeout)

	waitForCondition(t, membershipTimeout, "the promoted replica's role is voter", func() bool {
		rp, _, ok := replicaProgressOf(cluster, shardID, observerID)
		return ok && rp.Role == quicraft.ReplicaRoleVoter
	})
}
