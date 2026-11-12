// Copyright 2026 Jeremy Hahn
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//go:build integration

package e2e_test

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	quicraft "github.com/jeremyhahn/go-quicraft/pkg"
	"github.com/jeremyhahn/go-quicraft/test/proposer"
)

// growShrinkBudget bounds the whole grow-and-shrink run.
const growShrinkBudget = 2 * time.Minute

// growShrinkCallTimeout bounds one proposal or membership request.
const growShrinkCallTimeout = 5 * time.Second

// TestE2E_GrowShrink_OneToFiveToOne grows a shard from one host to five by
// successive single-replica adds, each naming the new member's identity and
// the current ConfigChangeID, then shrinks it back to one by successive
// removes, removing the leader itself twice. After every change exactly one
// leader must exist and a write must succeed; at the end the last host must
// read back every acknowledged write and list only itself as a voter, with
// every removed replica in Removed.
func TestE2E_GrowShrink_OneToFiveToOne(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), growShrinkBudget)
	defer cancel()

	const shardID uint64 = 1
	const maxReplicas = 5

	cluster := newTestCluster(t, 1)
	defer cluster.close()
	cluster.startShard(shardID)
	waitForSingleLeader(t, cluster, shardID, leaderElectionTimeout)

	acked := make(map[string]string)
	growShrinkWrite(t, cluster, shardID, "grow-1", 5, acked)

	// (a) Grow: one host to five, one replica at a time.
	for rid := uint64(2); rid <= maxReplicas; rid++ {
		tracker := growShrinkAdd(t, cluster, shardID, rid)
		waitForNodeData(t, tracker, shardID, rid, acked, membershipTimeout)
		waitForSingleLeader(t, cluster, shardID, failoverTimeout)
		want := make([]uint64, 0, rid)
		for id := uint64(1); id <= rid; id++ {
			want = append(want, id)
		}
		waitForMembershipConverged(t, cluster, shardID, want, nil, membershipTimeout)
		growShrinkWrite(t, cluster, shardID, fmt.Sprintf("grow-%d", rid), 5, acked)
		t.Logf("grew to %d voters; %d acknowledged writes", rid, len(acked))
	}

	// (b) Shrink: remove five to two, one at a time. Leadership is moved to
	// replicas five and three before their removal, so the shard removes
	// its own leader twice and a follower twice.
	var removed []uint64
	for rid := uint64(maxReplicas); rid >= 2; rid-- {
		idx := int(rid - 1)
		if rid%2 == 1 {
			growShrinkTransferLeader(ctx, t, cluster, shardID, rid)
		}
		growShrinkRemove(ctx, t, cluster, shardID, rid, idx)
		cluster.stopNode(idx)
		removed = append(removed, rid)

		leaderIdx := waitForSingleLeader(t, cluster, shardID, failoverTimeout)
		if leaderIdx == idx {
			t.Fatalf("removed replica %d still leads shard %d", rid, shardID)
		}
		want := make([]uint64, 0, rid-1)
		for id := uint64(1); id < rid; id++ {
			want = append(want, id)
		}
		waitForMembershipConverged(t, cluster, shardID, want, removed, membershipTimeout)
		growShrinkWrite(t, cluster, shardID, fmt.Sprintf("shrink-%d", rid), 3, acked)
		t.Logf("shrank to %d voters; %d acknowledged writes", rid-1, len(acked))
	}

	// (c) The last host holds every acknowledged write and only itself.
	for key, want := range acked {
		got := proposer.Read(t, cluster.leaderHost, shardID, key, growShrinkCallTimeout)
		if got != want {
			t.Fatalf("Read(%q) through the last host = %v, want %q", key, got, want)
		}
	}
	last := cluster.hosts[0]
	if lh := cluster.leaderHost(shardID); lh != last {
		t.Fatalf("shard %d is not led by the last host", shardID)
	}
	m, err := last.ShardMembership(shardID)
	if err != nil {
		t.Fatalf("ShardMembership on the last host: %v", err)
	}
	if len(m.Nodes) != 1 {
		t.Fatalf("last host lists voters %v, want only replica 1", m.Nodes)
	}
	if _, ok := m.Nodes[1]; !ok {
		t.Fatalf("last host lists voters %v, want only replica 1", m.Nodes)
	}
	for _, rid := range removed {
		if _, ok := m.Removed[rid]; !ok {
			t.Fatalf("replica %d missing from Removed %v", rid, m.Removed)
		}
	}
	if len(m.Removed) != len(removed) {
		t.Fatalf("Removed = %v, want exactly %v", m.Removed, removed)
	}
	t.Logf("1 -> 5 -> 1 complete with all %d acknowledged writes intact", len(acked))
}

// growShrinkWrite proposes n puts with keys prefix-000.. through the current
// leader, retrying a leadership change, and records each acknowledged put in
// acked.
func growShrinkWrite(t *testing.T, c *testCluster, shardID uint64,
	prefix string, n int, acked map[string]string) {
	t.Helper()
	for i := range n {
		key := fmt.Sprintf("%s-%03d", prefix, i)
		value := "val-" + key
		proposeToLeader(t, c, shardID, encodeKVPut(key, value), failoverTimeout)
		acked[key] = value
	}
}

// growShrinkAdd builds a host for replicaID, adds it as a voter with its node
// identity and the leader's current ConfigChangeID, starts the shard on it in
// join mode, and appends it to c. It returns the new replica's tracker.
func growShrinkAdd(t *testing.T, c *testCluster, shardID, replicaID uint64) *smTracker {
	t.Helper()
	leaf := c.ca.Leaf(t)
	hostCfg := testHostConfig(t, kernelAddr, leaf)
	h, err := quicraft.NewHost(hostCfg, quicraft.WithoutEncryption())
	if err != nil {
		t.Fatalf("NewHost for replica %d: %v", replicaID, err)
	}
	started := false
	defer func() {
		if !started {
			h.Close()
		}
	}()
	addr, id := h.RaftAddress(), h.NodeIdentity()

	syncAddNode(t, c, shardID, replicaID, addr, id, membershipTimeout)
	m, err := c.leaderHost(shardID).ShardMembership(shardID)
	if err != nil {
		t.Fatalf("ShardMembership after adding replica %d: %v", replicaID, err)
	}
	if _, ok := m.Nodes[replicaID]; !ok {
		t.Fatalf("replica %d is not a voter after its add: %v", replicaID, m.Nodes)
	}
	if m.Identities[replicaID] != id {
		t.Fatalf("replica %d committed identity %q, want %q", replicaID, m.Identities[replicaID], id)
	}

	createFn, tracker := kvCreateFunc()
	scfg := testShardConfig(shardID, replicaID)
	scfg.MemberIdentities = c.identities()
	if err := h.StartShard(nil, true, createFn, scfg); err != nil {
		t.Fatalf("StartShard(join) for replica %d: %v", replicaID, err)
	}
	started = true

	c.hostsMu.Lock()
	c.hosts = append(c.hosts, h)
	c.addrs = append(c.addrs, addr)
	c.configs = append(c.configs, hostCfg)
	c.trackers = append(c.trackers, tracker)
	c.stopped = append(c.stopped, atomic.Bool{})
	c.leaves = append(c.leaves, leaf)
	c.ids = append(c.ids, id)
	c.hostsMu.Unlock()
	if got := len(c.hosts); uint64(got) != replicaID {
		t.Fatalf("cluster holds %d hosts after adding replica %d", got, replicaID)
	}
	return tracker
}

// growShrinkTransferLeader moves leadership of shardID to target and waits
// until target is the single leader.
func growShrinkTransferLeader(ctx context.Context, t *testing.T, c *testCluster, shardID, target uint64) {
	t.Helper()
	for {
		leaderIdx := waitForSingleLeader(t, c, shardID, failoverTimeout)
		if uint64(leaderIdx+1) == target {
			return
		}
		rctx, rcancel := context.WithTimeout(ctx, growShrinkCallTimeout)
		err := c.hosts[leaderIdx].RequestLeaderTransfer(rctx, shardID, target)
		rcancel()
		if err != nil && !isTransientLeadershipError(err) {
			t.Fatalf("RequestLeaderTransfer to replica %d: %v", target, err)
		}
		growShrinkPause(ctx, t, fmt.Sprintf("leadership to replica %d", target))
	}
}

// growShrinkRemove removes replicaID (hosted at c.hosts[idx]) with the
// leader's current ConfigChangeID and returns once a live host other than
// the removed one has applied the removal.
func growShrinkRemove(ctx context.Context, t *testing.T, c *testCluster, shardID, replicaID uint64, idx int) {
	t.Helper()
	syncDeleteNode(t, c, shardID, replicaID, membershipTimeout)
	for {
		for i, h := range c.hosts {
			if i == idx || c.stopped[i].Load() {
				continue
			}
			if m, err := h.ShardMembership(shardID); err == nil {
				if _, ok := m.Removed[replicaID]; ok {
					return
				}
			}
		}
		growShrinkPause(ctx, t, fmt.Sprintf("removal of replica %d to apply", replicaID))
	}
}

// growShrinkPause waits briefly before a retry, failing the test with what
// it was waiting for once ctx expires.
func growShrinkPause(ctx context.Context, t *testing.T, what string) {
	t.Helper()
	timer := time.NewTimer(100 * time.Millisecond)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		t.Fatalf("grow/shrink budget %v exhausted waiting for %s", growShrinkBudget, what)
	case <-timer.C:
	}
}
