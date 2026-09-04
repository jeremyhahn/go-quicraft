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

// Real network partitions, as opposed to the process kills the rest of the
// suite calls partitions.
//
// Every other "partition" test in this package calls stopNode, which is a
// process kill. A killed node holds no leadership belief, answers nothing, and
// returns with cold in-memory state. It cannot be a LIVE leader that a quorum
// has moved on without — which is the single most dangerous configuration in
// Raft and the one these tests exist to construct.
//
// The cost of not having this was measured: with the suite as it stood,
// weakening the election quorum from a strict majority to n/2 passed 18 of 19
// linearizability tests. TestNetPartition_TwoOfFourCannotElect below is the
// test that kills that mutant outright.
//
// Run with:
//
//	go test -tags 'integration quicraft_netfault' ./test/integration/e2e/...
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

// isLeaderAt reports whether the node at idx currently believes it leads the
// shard. Asked per-node rather than via findLeader, because these tests need to
// catch the case where MORE THAN ONE node answers yes — which findLeader, by
// returning the first match, cannot express.
func isLeaderAt(c *testCluster, idx int, shardID uint64) bool {
	c.hostsMu.RLock()
	h := c.hosts[idx]
	c.hostsMu.RUnlock()
	info, err := h.GetShardInfo(shardID)
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

// isolate cuts one node off from every other node in both directions, WITHOUT
// stopping it. It keeps its term, its role, its leadership belief and its
// ticker.
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

// requireFaultInjection asserts that the faults a test just installed are
// actually in effect, and arranges for them to be removed afterwards.
//
// This guard is the point. These tests are compiled only under
// quicraft_netfault; built without it every netfault call is inert and the
// whole suite would exercise a perfectly healthy cluster while reporting
// success. That is precisely the failure this file was written to correct, so
// it is not left to convention.
func requireFaultInjection(t *testing.T, wantAtLeast int) {
	t.Helper()
	t.Cleanup(netfault.Heal)
	if got := netfault.BlockedCount(); got < wantAtLeast {
		t.Fatalf("fault injection is not in effect: BlockedCount()=%d, want >=%d. "+
			"The cluster is healthy and this test proves nothing.", got, wantAtLeast)
	}
}

// TestNetPartition_TwoOfFourCannotElect is the strict-majority test.
//
// A 4-node shard split 2|2 must produce NO leader on either side: a majority of
// 4 is 3. If the election quorum is computed as n/2 rather than n/2+1, each
// side has exactly 2 and BOTH sides elect — two leaders in the same term, in a
// cluster that is entirely alive and will reconverge into split-brain when the
// partition heals.
//
// n=4 is chosen deliberately. It is the smallest size where n/2 and n/2+1 give
// different answers for an even split, so the mutation is visible here and
// nowhere else. Killing nodes cannot express this at all: stopping 2 of 4
// leaves 2 alive that legitimately cannot form a quorum for a different reason.
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
					// A leader here is only a violation if it can still
					// COMMIT — an incumbent that has not yet noticed the
					// partition is expected and harmless. Prove it cannot.
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
		time.Sleep(100 * time.Millisecond)
	}

	// Heal, and the cluster must converge on exactly one leader again.
	cluster.healNetwork()
	healed := waitForSingleLeader(t, cluster, 1, failoverTimeout)
	t.Logf("after heal, single leader node[%d]", healed)
}

// TestNetPartition_DeposedLeaderStaysAliveAndCannotCommit builds the
// configuration stopNode cannot: a leader that is still running, still
// believes it leads, and has been superseded by a quorum it cannot reach.
//
// The isolated leader must not commit anything. The majority must elect and
// make progress. On heal, the old leader must step down and adopt the new
// leader's log — including discarding anything it appended alone.
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

	// The isolated node is STILL RUNNING. That is the whole point: assert it,
	// so a regression that turns this back into a process kill is caught.
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

// TestNetPartition_AsymmetricLeaderCannotBeHeard covers the half-open failure
// that a process kill cannot produce at all: a leader whose outbound traffic
// still arrives, so followers keep receiving heartbeats and never time out,
// but whose inbound traffic is dropped, so it never hears an acknowledgement.
//
// Such a leader is invisible to the followers' election timers, and without a
// quorum check on the leader's own side it will hold leadership indefinitely
// while committing nothing — a cluster that looks healthy and accepts no
// writes. Check-quorum exists to make it step down; this asserts it does.
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
