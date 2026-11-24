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

// Real network partitions that keep nodes running. Run with go test -tags 'integration quicraft_netfault' ./test/integration/e2e/...
package e2e_test

import (
	"context"
	"testing"
	"time"

	"github.com/jeremyhahn/go-quicraft/test/netfault"
)

// replicaOf maps a cluster index to the replica ID startShard assigns it.
// Kept next to the partition helpers so the two cannot drift apart: if
// startShard's numbering ever changes, every partition below would silently
// cut the wrong links and the tests would quietly pass against a healthy
// cluster.
func replicaOf(idx int) uint64 { return uint64(idx + 1) }

// isLeaderAt reports whether the node at idx believes it leads the shard.
func isLeaderAt(c *testCluster, idx int, shardID uint64) bool {
	c.hostsMu.RLock()
	h := c.hosts[idx]
	c.hostsMu.RUnlock()
	info, err := h.ShardInfo(shardID)
	if err != nil {
		return false
	}
	return info.IsLeader
}

// valueAt reads a key directly from the state machine of one specific node,
// with no consensus round trip, so a test can assert what a given replica
// actually holds rather than what the cluster would answer.
func valueAt(c *testCluster, trackers []*smTracker, shardID uint64, idx int, key string) (string, bool) {
	machine := resolveStateMachine(c, trackers, shardID, idx)
	if machine == nil {
		return "", false
	}
	return machine.Get(key)
}

// isolate cuts one node off from every other in both directions without stopping it.
func (c *testCluster) isolate(idx int) {
	c.t.Helper()
	netfault.Isolate(replicaOf(idx))
	c.t.Logf("isolated node[%d] (replica %d) — still running", idx, replicaOf(idx))
}

// rejoin restores a node isolated by isolate.
func (c *testCluster) rejoin(idx int) {
	c.t.Helper()
	netfault.Rejoin(replicaOf(idx))
	c.t.Logf("rejoined node[%d] (replica %d)", idx, replicaOf(idx))
}

// partition splits the cluster along a cut. Traffic inside each side is
// untouched, so each side stays internally connected and elects among itself if
// it holds quorum.
func (c *testCluster) partition(groupA, groupB []int) {
	c.t.Helper()
	a := make([]uint64, len(groupA))
	for i, idx := range groupA {
		a[i] = replicaOf(idx)
	}
	b := make([]uint64, len(groupB))
	for i, idx := range groupB {
		b[i] = replicaOf(idx)
	}
	netfault.Partition(a, b)
	c.t.Logf("partitioned %v | %v", groupA, groupB)
}

// healNetwork removes every injected fault. Registered with t.Cleanup by
// requireFaultInjection, because the injector state is process-global and a
// partition left standing leaks into the next test.
func (c *testCluster) healNetwork() {
	netfault.Heal()
}

// requireFaultInjection asserts the installed faults are in effect and removes them afterwards.
func requireFaultInjection(t *testing.T, wantAtLeast int) {
	t.Helper()
	t.Cleanup(netfault.Heal)
	if got := netfault.BlockedCount(); got < wantAtLeast {
		t.Fatalf("fault injection is not in effect: BlockedCount()=%d, want >=%d. "+
			"The cluster is healthy and this test proves nothing.", got, wantAtLeast)
	}
}

// TestNetPartition_TwoOfFourCannotElect verifies a 2|2 split of four nodes elects no leader on either side.
func TestNetPartition_TwoOfFourCannotElect(t *testing.T) {
	cluster := newTestCluster(t, 4)
	defer cluster.close()

	trackers := cluster.startShard(1)
	leaderIdx := cluster.waitForClusterLeader(1, leaderElectionTimeout)
	proposeAndWait(t, cluster, trackers, 1, "pre-split", "ok")
	waitForReplication(t, cluster, trackers, 1, "pre-split", "ok", defaultTimeout)
	t.Logf("initial leader node[%d]", leaderIdx)

	// Split into two pairs, with the incumbent on the left.
	left := []int{leaderIdx, (leaderIdx + 1) % 4}
	var right []int
	for i := 0; i < 4; i++ {
		if i != left[0] && i != left[1] {
			right = append(right, i)
		}
	}
	cluster.partition(left, right)
	requireFaultInjection(t, 8) // 2*2 pairs, both directions

	// Give both sides several election timeouts to try and fail.
	deadline := time.Now().Add(failoverTimeout)
	for time.Now().Before(deadline) {
		for _, side := range [][]int{left, right} {
			for _, idx := range side {
				if isLeaderAt(cluster, idx, 1) {
					// A leader here is a violation only if it can still commit.
					ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
					//quicraft:direct-propose a minority-side incumbent must not be able to commit
					_, err := cluster.hosts[idx].SyncPropose(ctx, 1,
						encodeKVPut("split-brain", "committed"))
					cancel()
					if err == nil {
						t.Fatalf("node[%d] committed a proposal while holding only "+
							"2 of 4 replicas; a majority of 4 is 3. Both halves of "+
							"this partition can now accept writes.", idx)
					}
				}
			}
		}
		// Sampling interval across the held partition: the partition is
		// held for failoverTimeout by design, not until a condition.
		time.Sleep(100 * time.Millisecond)
	}

	// Heal, and the cluster must converge on exactly one leader again.
	cluster.healNetwork()
	healed := waitForSingleLeader(t, cluster, 1, failoverTimeout)
	t.Logf("after heal, single leader node[%d]", healed)
}

// TestNetPartition_DeposedLeaderStaysAliveAndCannotCommit verifies an isolated live leader commits nothing and adopts the new leader's log on heal.
func TestNetPartition_DeposedLeaderStaysAliveAndCannotCommit(t *testing.T) {
	cluster := newTestCluster(t, 5)
	defer cluster.close()

	trackers := cluster.startShard(1)
	oldLeader := cluster.waitForClusterLeader(1, leaderElectionTimeout)
	proposeAndWait(t, cluster, trackers, 1, "pre-partition", "before")
	waitForReplication(t, cluster, trackers, 1, "pre-partition", "before", defaultTimeout)
	t.Logf("initial leader node[%d]", oldLeader)

	cluster.isolate(oldLeader)
	requireFaultInjection(t, 1)

	// The isolated node is still running.
	if cluster.stopped[oldLeader].Load() {
		t.Fatal("the deposed leader was stopped, not partitioned; this test is " +
			"back to being a process-kill test and proves nothing new")
	}
	if cluster.liveNodes() != 5 {
		t.Fatalf("liveNodes()=%d, want 5 — every node must remain alive",
			cluster.liveNodes())
	}

	// The majority must elect a new leader from the other four.
	var newLeader int
	waitForCondition(t, failoverTimeout, "majority elects a new leader", func() bool {
		for i := range cluster.hosts {
			if i == oldLeader {
				continue
			}
			if isLeaderAt(cluster, i, 1) {
				newLeader = i
				return true
			}
		}
		return false
	})
	t.Logf("majority elected node[%d]", newLeader)

	// The isolated old leader must not be able to commit. It may still report
	// itself leader for up to an election timeout; committing is the violation.
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	//quicraft:direct-propose the isolated deposed leader IS the subject: it must not commit
	_, err := cluster.hosts[oldLeader].SyncPropose(ctx, 1,
		encodeKVPut("ghost-write", "from-deposed-leader"))
	cancel()
	if err == nil {
		t.Fatal("the isolated leader committed a proposal without a quorum; a " +
			"client would have been told this write was durable")
	}
	t.Logf("deposed leader correctly refused the write: %v", err)

	// The majority side keeps working.
	if err := proposeUntilCommitted(cluster, 1, "during-partition", "majority",
		defaultTimeout); err != nil {
		t.Fatalf("the majority could not commit during the partition: %v", err)
	}

	// Heal. The old leader must step down and pick up the majority's data.
	cluster.rejoin(oldLeader)
	waitForCondition(t, failoverTimeout, "old leader steps down", func() bool {
		return !isLeaderAt(cluster, oldLeader, 1)
	})
	waitForCondition(t, defaultTimeout, "old leader adopts the majority's log", func() bool {
		v, ok := valueAt(cluster, trackers, 1, oldLeader, "during-partition")
		return ok && v == "majority"
	})

	// And its solo write must never appear anywhere.
	for i := range cluster.hosts {
		if v, ok := valueAt(cluster, trackers, 1, i, "ghost-write"); ok {
			t.Errorf("node[%d] holds ghost-write=%q, a value the deposed leader "+
				"was told it could not commit", i, v)
		}
	}
}

// TestNetPartition_AsymmetricLeaderCannotBeHeard verifies a leader that cannot hear acks steps down through check-quorum.
func TestNetPartition_AsymmetricLeaderCannotBeHeard(t *testing.T) {
	cluster := newTestCluster(t, 3)
	defer cluster.close()

	trackers := cluster.startShard(1)
	leaderIdx := cluster.waitForClusterLeader(1, leaderElectionTimeout)
	proposeAndWait(t, cluster, trackers, 1, "pre-asym", "ok")
	waitForReplication(t, cluster, trackers, 1, "pre-asym", "ok", defaultTimeout)

	// Block only follower -> leader. The leader can still transmit.
	blocked := 0
	for i := range cluster.hosts {
		if i == leaderIdx {
			continue
		}
		netfault.BlockLink(replicaOf(i), replicaOf(leaderIdx))
		blocked++
	}
	requireFaultInjection(t, blocked)
	t.Logf("node[%d] can send but not receive (%d inbound links blocked)",
		leaderIdx, blocked)

	// It must lose leadership on its own initiative — nothing else can tell
	// it to, because from the followers' point of view it is still heartbeating.
	waitForCondition(t, failoverTimeout,
		"leader that cannot hear a quorum steps down", func() bool {
			return !isLeaderAt(cluster, leaderIdx, 1)
		})

	// And it must not have committed anything in the meantime.
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	//quicraft:direct-propose the leader that cannot hear a quorum IS the subject
	_, err := cluster.hosts[leaderIdx].SyncPropose(ctx, 1,
		encodeKVPut("asym-write", "unheard"))
	cancel()
	if err == nil {
		t.Fatal("a leader that cannot receive acknowledgements committed a write")
	}

	cluster.healNetwork()
	waitForSingleLeader(t, cluster, 1, failoverTimeout)
}
