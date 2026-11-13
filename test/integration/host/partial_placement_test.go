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
	"errors"
	"slices"
	"testing"
	"time"

	quicraft "github.com/jeremyhahn/go-quicraft/pkg"
)

// Shard IDs for the partial-placement test. Shard A lives on hosts 1-3
// (indexes 0-2), shard B on hosts 2-4 (indexes 1-3): the replica sets
// overlap on hosts 2 and 3 and differ on hosts 1 and 4.
const (
	partialShardA uint64 = 96701
	partialShardB uint64 = 96702
)

var (
	partialHostsA = []int{0, 1, 2}
	partialHostsB = []int{1, 2, 3}
)

// startShardOnHosts starts shardID on the hosts at idxs only. Each member's
// replica ID is its host index plus one, as in startShard, but the member
// map and MemberIdentities name only the chosen hosts, so the hosts outside
// idxs neither run nor know the shard. The returned trackers are indexed by
// host and nil for a host outside idxs.
func startShardOnHosts(ctx context.Context, t *testing.T, c *testCluster, shardID uint64, idxs []int) []*smTracker {
	t.Helper()
	if err := ctx.Err(); err != nil {
		t.Fatalf("startShardOnHosts shard=%d: %v", shardID, err)
	}
	all := c.identities()
	members := make(map[uint64]string, len(idxs))
	ids := make(map[uint64]string, len(idxs))
	for _, i := range idxs {
		replicaID := uint64(i + 1)
		members[replicaID] = c.addrs[i]
		ids[replicaID] = all[replicaID]
	}
	trackers := make([]*smTracker, len(c.hosts))
	for _, i := range idxs {
		replicaID := uint64(i + 1)
		createFn, tracker := kvCreateFunc()
		trackers[i] = tracker
		scfg := testShardConfig(shardID, replicaID)
		scfg.MemberIdentities = ids
		if err := c.hosts[i].StartShard(members, false, createFn, scfg); err != nil {
			t.Fatalf("StartShard host=%d shard=%d: %v", i+1, shardID, err)
		}
	}
	return trackers
}

// waitForAgreedLeader waits until a live host in idxs leads shardID and every
// live host in idxs names that leader, and returns the leader's host index
// and its ShardInfo.
func waitForAgreedLeader(ctx context.Context, t *testing.T, c *testCluster, shardID uint64, idxs []int) (int, *quicraft.ShardInfo) {
	t.Helper()
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		if idx, info := agreedLeader(c, shardID, idxs); idx >= 0 {
			return idx, info
		}
		select {
		case <-ctx.Done():
			t.Fatalf("shard %d: no leader agreed by hosts %v: %v", shardID, hostNumbers(idxs), ctx.Err())
			return -1, nil
		case <-ticker.C:
		}
	}
}

// agreedLeader is one poll of waitForAgreedLeader: -1 when no leader is
// agreed yet.
func agreedLeader(c *testCluster, shardID uint64, idxs []int) (int, *quicraft.ShardInfo) {
	c.hostsMu.RLock()
	defer c.hostsMu.RUnlock()
	leaderIdx := -1
	var leaderInfo *quicraft.ShardInfo
	var leaderID uint64
	for _, i := range idxs {
		if c.stopped[i].Load() {
			continue
		}
		info, err := c.hosts[i].ShardInfo(shardID)
		if err != nil || info.LeaderID == 0 {
			return -1, nil
		}
		if leaderID != 0 && info.LeaderID != leaderID {
			return -1, nil
		}
		leaderID = info.LeaderID
		if info.IsLeader {
			leaderIdx, leaderInfo = i, info
		}
	}
	if leaderIdx < 0 || leaderInfo.LeaderID != uint64(leaderIdx+1) {
		return -1, nil
	}
	return leaderIdx, leaderInfo
}

// commitOnHosts proposes key=value to shardID's leader and waits until every
// live host in idxs has applied it.
func commitOnHosts(ctx context.Context, t *testing.T, c *testCluster, trackers []*smTracker, shardID uint64, idxs []int, key, value string) {
	t.Helper()
	proposeAndWait(t, c, trackers, shardID, key, value)
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		missing := -1
		for _, i := range idxs {
			if c.stopped[i].Load() {
				continue
			}
			machine := trackers[i].Get(shardID, uint64(i+1))
			if machine == nil {
				missing = i
				break
			}
			if v, ok := machine.Get(key); !ok || v != value {
				missing = i
				break
			}
		}
		if missing < 0 {
			return
		}
		select {
		case <-ctx.Done():
			t.Fatalf("shard %d: host %d did not apply %q=%q: %v", shardID, missing+1, key, value, ctx.Err())
			return
		case <-ticker.C:
		}
	}
}

// hostNumbers turns host indexes into the 1-based host numbers the test
// names them by.
func hostNumbers(idxs []int) []int {
	out := make([]int, len(idxs))
	for k, i := range idxs {
		out[k] = i + 1
	}
	return out
}

// TestPartialPlacement_OverlappingShardsAreIndependent places shard A on
// hosts 1-3 and shard B on hosts 2-4 of a four-host cluster. Stopping host 1
// must leave shard B (which host 1 does not host) undisturbed — same leader,
// same term, still committing — while shard A fails over within hosts 2-3.
// Host 4, which never hosted shard A, must refuse shard A with
// ErrShardNotFound and leave it out of its NodeHostInfo.
func TestPartialPlacement_OverlappingShardsAreIndependent(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	cluster := newTestCluster(t, 4)
	defer cluster.close()

	// (a) Both shards elect a leader and commit.
	trackersA := startShardOnHosts(ctx, t, cluster, partialShardA, partialHostsA)
	trackersB := startShardOnHosts(ctx, t, cluster, partialShardB, partialHostsB)

	electCtx, electCancel := context.WithTimeout(ctx, leaderElectionTimeout)
	leaderA, _ := waitForAgreedLeader(electCtx, t, cluster, partialShardA, partialHostsA)
	leaderB, _ := waitForAgreedLeader(electCtx, t, cluster, partialShardB, partialHostsB)
	electCancel()
	if !slices.Contains(partialHostsA, leaderA) {
		t.Fatalf("shard A leader is host %d, outside its replica set %v", leaderA+1, hostNumbers(partialHostsA))
	}
	if !slices.Contains(partialHostsB, leaderB) {
		t.Fatalf("shard B leader is host %d, outside its replica set %v", leaderB+1, hostNumbers(partialHostsB))
	}
	commitOnHosts(ctx, t, cluster, trackersA, partialShardA, partialHostsA, "a-before", "1")
	commitOnHosts(ctx, t, cluster, trackersB, partialShardB, partialHostsB, "b-before", "1")

	// Re-read B's leader after its first commit: that is the steady state
	// stopping host 1 must not disturb.
	leaderB, infoB := waitForAgreedLeader(ctx, t, cluster, partialShardB, partialHostsB)
	termB := infoB.Term
	t.Logf("before stop: shard A leader host %d, shard B leader host %d term %d", leaderA+1, leaderB+1, termB)

	// (b) Stop host 1.
	cluster.stopNode(0)

	failCtx, failCancel := context.WithTimeout(ctx, failoverTimeout)
	newLeaderA, _ := waitForAgreedLeader(failCtx, t, cluster, partialShardA, partialHostsA)
	failCancel()
	if newLeaderA != 1 && newLeaderA != 2 {
		t.Fatalf("shard A leader after stopping host 1 is host %d, want host 2 or 3", newLeaderA+1)
	}
	commitOnHosts(ctx, t, cluster, trackersA, partialShardA, partialHostsA, "a-after", "2")
	commitOnHosts(ctx, t, cluster, trackersB, partialShardB, partialHostsB, "b-after", "2")

	gotLeaderB, gotInfoB := waitForAgreedLeader(ctx, t, cluster, partialShardB, partialHostsB)
	if gotLeaderB != leaderB {
		t.Errorf("shard B leader moved from host %d to host %d when host 1 stopped", leaderB+1, gotLeaderB+1)
	}
	if gotInfoB.Term != termB {
		t.Errorf("shard B term moved from %d to %d when host 1 stopped", termB, gotInfoB.Term)
	}
	for _, i := range partialHostsB {
		machine := trackersB[i].Get(partialShardB, uint64(i+1))
		if v, ok := machine.Get("b-before"); !ok || v != "1" {
			t.Errorf("shard B on host %d lost b-before: got %q, %v", i+1, v, ok)
		}
	}

	// (c) Host 4 never hosted shard A.
	host4 := cluster.hosts[3]
	opCtx, opCancel := context.WithTimeout(ctx, 3*time.Second)
	defer opCancel()
	if _, err := host4.SyncRead(opCtx, partialShardA, "a-after"); !errors.Is(err, quicraft.ErrShardNotFound) {
		t.Errorf("host 4 SyncRead(shard A) = %v, want ErrShardNotFound", err)
	}
	//quicraft:direct-propose the test is about host 4, which does not host shard A, refusing it
	if _, err := host4.SyncPropose(opCtx, partialShardA, encodeKVPut("a-host4", "x")); !errors.Is(err, quicraft.ErrShardNotFound) {
		t.Errorf("host 4 SyncPropose(shard A) = %v, want ErrShardNotFound", err)
	}
	if _, err := host4.ShardInfo(partialShardA); !errors.Is(err, quicraft.ErrShardNotFound) {
		t.Errorf("host 4 ShardInfo(shard A) = %v, want ErrShardNotFound", err)
	}
	listed := listedShards(host4)
	if slices.Contains(listed, partialShardA) {
		t.Errorf("host 4 NodeHostInfo lists shard A: %v", listed)
	}
	if !slices.Contains(listed, partialShardB) {
		t.Errorf("host 4 NodeHostInfo does not list shard B: %v", listed)
	}
	for _, i := range []int{1, 2} {
		if v, ok := trackersA[i].Get(partialShardA, uint64(i+1)).Get("a-host4"); ok {
			t.Errorf("shard A on host %d applied host 4's refused proposal: %q", i+1, v)
		}
	}
}

// listedShards returns the shard IDs h's NodeHostInfo reports.
func listedShards(h *quicraft.Host) []uint64 {
	info := h.NodeHostInfo(quicraft.NodeHostInfoOption{SkipLogInfo: true})
	out := make([]uint64, 0, len(info.ShardInfoList))
	for _, si := range info.ShardInfoList {
		out = append(out, si.ShardID)
	}
	return out
}
