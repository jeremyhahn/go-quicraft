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
	"fmt"
	"testing"
	"time"

	"github.com/jeremyhahn/go-quicraft/pkg/config"
)

// TestHostAPI_FullClusterRestart_KeepsAcknowledgedWrites closes every host
// of a three-replica shard at once and reopens all of them on the same
// directories. Unlike a rolling restart, no replica keeps the shard's state
// in memory across the gap: the persisted log, hard state and snapshots are
// all there is. The shard must come back with exactly one leader, every
// acknowledged write on every host, no entry applied twice, and it must
// commit new writes.
func TestHostAPI_FullClusterRestart_KeepsAcknowledgedWrites(t *testing.T) {
	// Any applied index that fails to advance on any state machine (a
	// duplicate or replayed apply) fails the test at cleanup.
	assertApplyOrder(t)

	cluster, nodeConfigs := newTestClusterWithRestart(t, 3)
	t.Cleanup(func() { cluster.close() })

	const (
		shardID  uint64 = 1
		nEntries        = 25
	)

	// Snapshot every 10 entries so the reopened replicas recover from a
	// snapshot plus the log tail after it, not from the log alone.
	shardCfgFn := func(sid, rid uint64) config.Config {
		return testShardConfigWithSnapshot(sid, rid, 10, 5)
	}

	// (a) Start the shard and write a known set of acknowledged entries.
	trackers := cluster.startShardWithConfig(shardID, shardCfgFn)
	cluster.waitForClusterLeader(shardID, leaderElectionTimeout)

	acked := proposeNEntries(t, cluster, trackers, shardID, nEntries, "cold")
	if len(acked) != nEntries {
		t.Fatalf("acknowledged %d entries, want %d", len(acked), nEntries)
	}
	verifyAllData(t, cluster, trackers, shardID, acked)

	// The highest index any replica applied before the shutdown. Every
	// reopened replica must get back at least this far.
	var appliedBefore uint64
	for _, i := range cluster.liveNodeIndexes() {
		h := cluster.hosts[i]
		info, err := h.ShardInfo(shardID)
		if err != nil {
			t.Fatalf("node[%d] ShardInfo before close: %v", i, err)
		}
		appliedBefore = max(appliedBefore, info.LastApplied)
	}
	if appliedBefore == 0 {
		t.Fatal("no replica reports an applied index before close")
	}

	// (b) Close all three hosts.
	for i := range nodeConfigs {
		cluster.stopNode(i)
	}
	if live := cluster.liveNodeIndexes(); len(live) != 0 {
		t.Fatalf("hosts %v still live after closing every host", live)
	}

	// (c) Reopen all three on the same directories and wait for exactly one
	// leader that every replica agrees on.
	for i, nc := range nodeConfigs {
		restartNode(t, cluster, i, nc, shardID, shardCfgFn)
	}
	ctx, cancel := context.WithTimeout(context.Background(), failoverTimeout)
	defer cancel()
	leaderIdx := waitForSingleLeader(ctx, t, cluster, shardID)
	t.Logf("node[%d] leads after the full-cluster restart", leaderIdx)

	// (d) Every acknowledged entry is on every host, nothing else is, and
	// every replica has applied at least as far as before the shutdown.
	verifyAllData(t, cluster, cluster.trackers, shardID, acked)
	assertExactData(ctx, t, cluster, shardID, acked)
	for _, i := range cluster.liveNodeIndexes() {
		h := cluster.hosts[i]
		waitForConditionDiag(t, membershipTimeout,
			fmt.Sprintf("node[%d] applied index >= %d after restart", i, appliedBefore),
			func() bool {
				info, err := h.ShardInfo(shardID)
				return err == nil && info.LastApplied >= appliedBefore
			},
			func() string { return shardStateDump(cluster, shardID) },
		)
	}

	// (e) A new write after the restart commits and reaches all three.
	const newKey, newValue = "cold-after-restart", "val-cold-after-restart"
	proposeAndWait(t, cluster, cluster.trackers, shardID, newKey, newValue)
	acked[newKey] = newValue
	verifyAllData(t, cluster, cluster.trackers, shardID, acked)
	assertExactData(ctx, t, cluster, shardID, acked)
}

// waitForSingleLeader waits until exactly one live host reports itself
// leader of shardID and every live host names that same replica as leader.
// It returns the leader's index and fails the test when ctx ends first.
func waitForSingleLeader(ctx context.Context, t *testing.T, cluster *testCluster, shardID uint64) int {
	t.Helper()
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	last := "no poll yet"
	for {
		idx, state := singleLeader(cluster, shardID)
		if idx >= 0 {
			return idx
		}
		last = state
		select {
		case <-ctx.Done():
			t.Fatalf("no single agreed leader for shard %d: %s\n%s",
				shardID, last, shardStateDump(cluster, shardID))
			return -1
		case <-ticker.C:
		}
	}
}

// singleLeader returns the index of the one live host leading shardID when
// all live hosts agree on it, else -1 and a description of the disagreement.
func singleLeader(cluster *testCluster, shardID uint64) (int, string) {
	live := cluster.liveNodeIndexes()
	if len(live) != len(cluster.addrs) {
		return -1, fmt.Sprintf("live hosts %v, want %d", live, len(cluster.addrs))
	}
	leaderIdx := -1
	leaders := 0
	views := make([]uint64, 0, len(live))
	for _, i := range cluster.liveNodeIndexes() {
		h := cluster.hosts[i]
		info, err := h.ShardInfo(shardID)
		if err != nil {
			return -1, fmt.Sprintf("node[%d] ShardInfo: %v", i, err)
		}
		views = append(views, info.LeaderID)
		if info.IsLeader {
			leaders++
			leaderIdx = i
		}
	}
	if leaders != 1 {
		return -1, fmt.Sprintf("%d hosts report leadership, leader views %v", leaders, views)
	}
	for _, v := range views {
		if v != uint64(leaderIdx+1) {
			return -1, fmt.Sprintf("leader views %v disagree with node[%d]", views, leaderIdx)
		}
	}
	return leaderIdx, ""
}

// assertExactData checks every live replica's state machine holds exactly
// the keys in want: an extra key would be a write that was never
// acknowledged surfacing from disk.
func assertExactData(ctx context.Context, t *testing.T, cluster *testCluster, shardID uint64, want map[string]string) {
	t.Helper()
	timeout := membershipTimeout
	if dl, ok := ctx.Deadline(); ok {
		timeout = max(time.Until(dl), time.Second)
	}
	for _, i := range cluster.liveNodeIndexes() {
		machine := waitForStateMachine(t, cluster, cluster.trackers, shardID, i, timeout)
		got := machine.Snapshot()
		if len(got) != len(want) {
			t.Fatalf("node[%d] holds %d keys, want %d", i, len(got), len(want))
		}
		for k, v := range want {
			if got[k] != v {
				t.Fatalf("node[%d] key %q = %q, want %q", i, k, got[k], v)
			}
		}
	}
}
