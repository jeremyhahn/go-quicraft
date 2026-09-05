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

// The regression fixture for the READ half of the cached-leader class (#147).
//
// It is the read-path twin of the e2e suite's
// TestNetPartition_ProposeToLeader_ResolvesPastAnIsolatedIncumbent (#98), and
// it lives in the netfault build for the same reason: the failure needs a live
// replica that has lost leadership AND does not yet know who took over.
// handleFollowerReadIndex forwards a ReadIndex whenever the follower knows the
// leader, and refuses it with "not leader ... leader unknown" only when it does
// not. A process kill cannot construct that state — a killed node answers
// ErrClosed, not not-leader — but isolation can: the node keeps running, steps
// down when check-quorum fires, and hears nothing about its successor.
//
// The CI failure this guards (#147, membership_change_test.go:210) was the
// same refusal on a healthy cluster: a leader observed once above a loop of
// thirty reads, deposed by the election storm that followed a membership
// change, refusing the eleventh read correctly.

package host_test

import (
	"context"
	"testing"
	"time"

	"github.com/jeremyhahn/go-quicraft/test/netfault"
	"github.com/jeremyhahn/go-quicraft/test/proposer"
)

// TestNetPartition_ReadFromLeader_ResolvesPastAnIsolatedIncumbent asserts
// that readFromLeader re-resolves the leader between attempts, by issuing a
// read the instant the incumbent is cut off — while the resolver still names
// it — and requiring the read to be answered by the majority's leader.
//
// Mutation: hoist the resolve out of the retry loop in test/proposer's drive
// and this test fails with the not-leader rejection; the pinned read below
// proves that is the rejection the isolated incumbent produces.
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

	// Registered AFTER the cluster's own cleanup so it runs BEFORE it: the
	// injector is process-global, and a partition left standing leaks into
	// the next test.
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

	// No wait here, deliberately. The read races the step-down: the first
	// resolution gets the incumbent, whose ReadIndex heartbeat can reach no
	// quorum, so that attempt is failed not-leader when check-quorum takes the
	// shard away from it — and the SECOND resolution, the one a cached index
	// cannot make, finds the majority's leader.
	got := readFromLeader(t, cluster, shardID, "rfl-pre", failoverTimeout)
	if val, ok := got.(string); !ok || val != "ok" {
		t.Fatalf("read through the leader = %v (%T), want %q", got, got, "ok")
	}

	// The read was answered, so the majority elected past the incumbent.
	// Assert the pre-fix pattern against it once it has stepped down: one
	// cached index, one SyncRead, no retry. This is what the converted sites
	// used to do, and it doubles as proof that the isolation produced the
	// leader-unknown refusal rather than a healthy cluster that forwarded the
	// ReadIndex on.
	waitForCondition(t, failoverTimeout, "the isolated incumbent steps down", func() bool {
		info, err := cluster.hosts[staleIdx].GetShardInfo(shardID)
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
	t.Logf("pre-fix pattern refused by the isolated incumbent: %v", err)

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
