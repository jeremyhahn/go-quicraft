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

// Regression fixture for reads through a cached leader: an isolated incumbent steps down without learning its successor.

package host_test

import (
	"context"
	"testing"
	"time"

	"github.com/jeremyhahn/go-quicraft/test/netfault"
	"github.com/jeremyhahn/go-quicraft/test/proposer"
)

// TestNetPartition_ReadFromLeader_ResolvesPastAnIsolatedIncumbent checks readFromLeader re-resolves the leader between attempts.
func TestNetPartition_ReadFromLeader_ResolvesPastAnIsolatedIncumbent(t *testing.T) {
	cluster := newTestCluster(t, 3)
	t.Cleanup(func() { cluster.close() })

	const shardID uint64 = 1
	trackers := cluster.startShard(shardID)
	cluster.waitForClusterLeader(shardID, leaderElectionTimeout)

	proposeToLeader(t, cluster, shardID, encodeKVPut("rfl-pre", "ok"), defaultTimeout)
	waitForReplication(t, cluster, trackers, shardID, "rfl-pre", "ok", defaultTimeout)

	// The observation a test would cache and then treat as fixed.
	staleIdx := cluster.findLeader(shardID)
	if staleIdx < 0 {
		t.Fatalf("no leader for shard %d after the warmup write", shardID)
	}
	// startShard numbers replicas idx+1; every state machine lookup in this
	// suite relies on the same mapping.
	staleReplica := uint64(staleIdx + 1)

	// Registered after the cluster's cleanup so it runs first and heals the partition.
	t.Cleanup(netfault.Heal)
	netfault.Isolate(staleReplica)
	// This guard is the point. Built without quicraft_netfault every netfault
	// call is inert and this test would exercise a healthy cluster while
	// reporting success.
	if got := netfault.BlockedCount(); got < 1 {
		t.Fatalf("fault injection is not in effect: BlockedCount()=%d, want >=1. "+
			"The cluster is healthy and this test proves nothing.", got)
	}
	t.Logf("node[%d] (replica %d) isolated while it still believes it leads shard %d",
		staleIdx, staleReplica, shardID)

	// No wait: the read races the step-down, so only a second resolution finds the new leader.
	got := readFromLeader(t, cluster, shardID, "rfl-pre", failoverTimeout)
	if val, ok := got.(string); !ok || val != "ok" {
		t.Fatalf("read through the leader = %v (%T), want %q", got, got, "ok")
	}

	// Assert a single cached-index SyncRead is refused, proving the isolation produced the leader-unknown refusal.
	waitForCondition(t, failoverTimeout, "the isolated incumbent steps down", func() bool {
		info, err := cluster.hosts[staleIdx].ShardInfo(shardID)
		return err == nil && !info.IsLeader
	})
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	//quicraft:direct-read the deposed incumbent IS the subject of this assertion
	_, err := cluster.hosts[staleIdx].SyncRead(ctx, shardID, "rfl-pre")
	if err == nil {
		t.Fatalf("isolated node[%d] answered a linearizable read while cut off from "+
			"both of its peers; a ReadIndex quorum of 3 is 2 and it can reach neither",
			staleIdx)
	}
	if !proposer.IsNotLeaderRejection(err) {
		t.Fatalf("isolated node[%d] refused the read with %v; this fixture needs the "+
			"not-leader rejection, so either the isolation did not take effect or "+
			"the refusal came from an unrelated fault", staleIdx, err)
	}
	t.Logf("cached-leader pattern refused by the isolated incumbent: %v", err)

	// After the heal the incumbent rejoins as a follower and the read path is
	// whole again: a read through the leader still answers, and the rejoined
	// node forwards rather than refuses.
	netfault.Rejoin(staleReplica)
	cluster.waitForClusterLeader(shardID, failoverTimeout)
	got = readFromLeader(t, cluster, shardID, "rfl-pre", failoverTimeout)
	if val, ok := got.(string); !ok || val != "ok" {
		t.Fatalf("read through the leader after the heal = %v (%T), want %q", got, got, "ok")
	}
	waitForCondition(t, failoverTimeout, "the rejoined node forwards a linearizable read", func() bool {
		rctx, rcancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer rcancel()
		//quicraft:direct-read the rejoined replica IS the subject: its forwarding, not the value, is asserted
		v, rerr := cluster.hosts[staleIdx].SyncRead(rctx, shardID, "rfl-pre")
		return rerr == nil && v == "ok"
	})
}
