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

package integration_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	dragonboat "github.com/jeremyhahn/go-quicraft/contrib/dragonboat"
	dbClient "github.com/jeremyhahn/go-quicraft/contrib/dragonboat/client"
	dbConfig "github.com/jeremyhahn/go-quicraft/contrib/dragonboat/config"
	"github.com/jeremyhahn/go-quicraft/contrib/dragonboat/raftio"
	quicraft "github.com/jeremyhahn/go-quicraft/pkg"
)

// snapshotEventsBudget bounds each SnapshotEvents test end to end.
const snapshotEventsBudget = 2 * time.Minute

// snapEventKind names the snapshot and compaction callbacks a
// snapshotEventListener records.
type snapEventKind string

const (
	snapEvCreated     snapEventKind = "SnapshotCreated"
	snapEvRecovered   snapEventKind = "SnapshotRecovered"
	snapEvLogCompact  snapEventKind = "LogCompacted"
	snapEvLogDBCompct snapEventKind = "LogDBCompacted"
)

// snapEvent is one recorded callback, in delivery order.
type snapEvent struct {
	kind      snapEventKind
	shardID   uint64
	replicaID uint64
	index     uint64
}

// snapshotEventListener implements raftio.ISystemEventListener and records
// the snapshot and compaction callbacks in the order they are delivered.
// With panics set, each recorded callback panics after recording.
type snapshotEventListener struct {
	mu     sync.Mutex
	events []snapEvent
	panics atomic.Bool
}

func (l *snapshotEventListener) record(ev snapEvent) {
	l.mu.Lock()
	l.events = append(l.events, ev)
	l.mu.Unlock()
	if l.panics.Load() {
		panic(fmt.Sprintf("snapshotEventListener: deliberate panic in %s", ev.kind))
	}
}

// Events returns a copy of the recorded events.
func (l *snapshotEventListener) Events() []snapEvent {
	l.mu.Lock()
	defer l.mu.Unlock()
	cp := make([]snapEvent, len(l.events))
	copy(cp, l.events)
	return cp
}

func (l *snapshotEventListener) NodeHostShuttingDown()                         {}
func (l *snapshotEventListener) NodeReady(_ raftio.NodeInfo)                   {}
func (l *snapshotEventListener) NodeUnloaded(_ raftio.NodeInfo)                {}
func (l *snapshotEventListener) NodeDeleted(_ raftio.NodeInfo)                 {}
func (l *snapshotEventListener) MembershipChanged(_ raftio.NodeInfo)           {}
func (l *snapshotEventListener) ConnectionEstablished(_ raftio.ConnectionInfo) {}
func (l *snapshotEventListener) ConnectionFailed(_ raftio.ConnectionInfo)      {}
func (l *snapshotEventListener) SendSnapshotStarted(_ raftio.SnapshotInfo)     {}
func (l *snapshotEventListener) SendSnapshotCompleted(_ raftio.SnapshotInfo)   {}
func (l *snapshotEventListener) SendSnapshotAborted(_ raftio.SnapshotInfo)     {}
func (l *snapshotEventListener) SnapshotReceived(_ raftio.SnapshotInfo)        {}
func (l *snapshotEventListener) SnapshotCompacted(_ raftio.SnapshotInfo)       {}

// SnapshotRecovered records a restore from a snapshot.
func (l *snapshotEventListener) SnapshotRecovered(info raftio.SnapshotInfo) {
	l.record(snapEvent{kind: snapEvRecovered, shardID: info.ShardID, replicaID: info.ReplicaID, index: info.Index})
}

// SnapshotCreated records a completed snapshot save.
func (l *snapshotEventListener) SnapshotCreated(info raftio.SnapshotInfo) {
	l.record(snapEvent{kind: snapEvCreated, shardID: info.ShardID, replicaID: info.ReplicaID, index: info.Index})
}

// LogCompacted records an in-memory log compaction.
func (l *snapshotEventListener) LogCompacted(info raftio.EntryInfo) {
	l.record(snapEvent{kind: snapEvLogCompact, shardID: info.ShardID, replicaID: info.ReplicaID, index: info.Index})
}

// LogDBCompacted records a LogDB compaction.
func (l *snapshotEventListener) LogDBCompacted(info raftio.EntryInfo) {
	l.record(snapEvent{kind: snapEvLogDBCompct, shardID: info.ShardID, replicaID: info.ReplicaID, index: info.Index})
}

var _ raftio.ISystemEventListener = (*snapshotEventListener)(nil)

// firstSnapEvent returns the position and value of the first event of kind
// for shardID whose index is at least minIndex, or -1.
func firstSnapEvent(events []snapEvent, kind snapEventKind, shardID, minIndex uint64) (int, snapEvent) {
	for i, ev := range events {
		if ev.kind == kind && ev.shardID == shardID && ev.index >= minIndex {
			return i, ev
		}
	}
	return -1, snapEvent{}
}

// waitForSnapEvent polls l until an event of kind for shardID with an index
// at least minIndex is recorded, failing t when ctx ends first.
func waitForSnapEvent(ctx context.Context, t *testing.T, l *snapshotEventListener, kind snapEventKind, shardID, minIndex uint64) snapEvent {
	t.Helper()
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		if i, ev := firstSnapEvent(l.Events(), kind, shardID, minIndex); i >= 0 {
			return ev
		}
		select {
		case <-ctx.Done():
			t.Fatalf("no %s for shard %d with index >= %d before %v; recorded %+v",
				kind, shardID, minIndex, ctx.Err(), l.Events())
		case <-ticker.C:
		}
	}
}

// startListenedSingleNode starts a single-replica shard 1 on a NodeHost
// whose SystemEventListener is l, waits for its leader and returns it.
func startListenedSingleNode(ctx context.Context, t *testing.T, l *snapshotEventListener) *dragonboat.NodeHost {
	t.Helper()
	pki := dbTestPKIFor(t)
	nhConfig := dbTestNodeHostConfig(t, kernelAddr, pki.leaf(t))
	nhConfig.SystemEventListener = l
	nh, err := dragonboat.NewNodeHostWithOptions(nhConfig, quicraft.WithMemoryLogDB(), quicraft.WithoutEncryption())
	if err != nil {
		t.Fatalf("NewNodeHostWithOptions failed: %v", err)
	}
	t.Cleanup(nh.Close)

	createFn, _ := dbKVCreateFunc()
	cfg := dbTestShardConfig(1, 1)
	cfg.MemberIdentities = map[uint64]string{1: nh.NodeIdentity()}
	members := map[uint64]dragonboat.Target{1: nh.RaftAddress()}
	if err := nh.StartReplica(members, false, createFn, cfg); err != nil {
		t.Fatalf("StartReplica failed: %v", err)
	}
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		if _, _, valid, lerr := nh.GetLeaderID(1); lerr == nil && valid {
			return nh
		}
		select {
		case <-ctx.Done():
			t.Fatalf("shard 1 elected no leader before %v", ctx.Err())
		case <-ticker.C:
		}
	}
}

// proposeSnapshotEventKeys writes n keys to shard 1 through nh, each
// confirmed by SyncPropose under ctx.
func proposeSnapshotEventKeys(ctx context.Context, t *testing.T, nh *dragonboat.NodeHost, prefix string, n int) {
	t.Helper()
	session := dbClient.NewNoOPSession(1, nil)
	for i := range n {
		pctx, cancel := context.WithTimeout(ctx, defaultTimeout)
		//quicraft:direct-propose single-replica shard: every caller passes the startListenedSingleNode NodeHost, the shard's only member and so its leader
		_, err := nh.SyncPropose(pctx, session, encodeKVPut(fmt.Sprintf("%s-%d", prefix, i), "v"))
		cancel()
		if err != nil {
			t.Fatalf("SyncPropose %s-%d failed: %v", prefix, i, err)
		}
	}
}

// requestCompactingSnapshot saves a snapshot of shardID on nh with the
// compaction overhead overridden to 1, so the save compacts the log, retrying
// while another snapshot is in progress. It returns the snapshot index.
func requestCompactingSnapshot(ctx context.Context, t *testing.T, nh *dragonboat.NodeHost, shardID uint64) uint64 {
	t.Helper()
	opt := dragonboat.SnapshotOption{OverrideCompactionOverhead: true, CompactionOverhead: 1}
	for {
		sctx, cancel := context.WithTimeout(ctx, defaultTimeout)
		idx, err := nh.SyncRequestSnapshot(sctx, shardID, opt)
		cancel()
		if err == nil {
			if idx == 0 {
				t.Fatal("SyncRequestSnapshot returned index 0")
			}
			return idx
		}
		if !errors.Is(err, quicraft.ErrSnapshotInProgress) {
			t.Fatalf("SyncRequestSnapshot failed: %v", err)
		}
		select {
		case <-ctx.Done():
			t.Fatalf("SyncRequestSnapshot still in progress at %v", ctx.Err())
		case <-time.After(100 * time.Millisecond):
		}
	}
}

// newListenedDBTestCluster builds a dbTestCluster whose host i reports to
// listeners[i].
func newListenedDBTestCluster(ctx context.Context, t *testing.T, listeners []*snapshotEventListener) *dbTestCluster {
	t.Helper()
	if err := ctx.Err(); err != nil {
		t.Fatalf("context ended before the cluster started: %v", err)
	}
	pki := dbTestPKIFor(t)
	n := len(listeners)
	hosts := make([]*dragonboat.NodeHost, n)
	addrs := make([]string, n)
	for i, l := range listeners {
		nhConfig := dbTestNodeHostConfig(t, kernelAddr, pki.leaf(t))
		nhConfig.SystemEventListener = l
		nh, err := dragonboat.NewNodeHostWithOptions(nhConfig, quicraft.WithMemoryLogDB(), quicraft.WithoutEncryption())
		if err != nil {
			for j := range i {
				hosts[j].Close()
			}
			t.Fatalf("NewNodeHostWithOptions[%d] failed: %v", i, err)
		}
		hosts[i] = nh
		addrs[i] = nh.RaftAddress()
	}
	return &dbTestCluster{
		hosts:   hosts,
		addrs:   addrs,
		stopped: make([]atomic.Bool, n),
		pki:     pki,
		t:       t,
	}
}

// TestIntegration_DB_SnapshotEvents_CreatedThenCompacted checks (a) and (b):
// a saved snapshot that compacts fires SnapshotCreated with the shard,
// replica and the snapshot's index, then LogCompacted for the same shard,
// then LogDBCompacted with the same compaction index.
func TestIntegration_DB_SnapshotEvents_CreatedThenCompacted(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), snapshotEventsBudget)
	defer cancel()

	l := &snapshotEventListener{}
	nh := startListenedSingleNode(ctx, t, l)
	proposeSnapshotEventKeys(ctx, t, nh, "a", 20)
	snapIdx := requestCompactingSnapshot(ctx, t, nh, 1)

	created := waitForSnapEvent(ctx, t, l, snapEvCreated, 1, snapIdx)
	if created.replicaID != 1 || created.index != snapIdx {
		t.Fatalf("SnapshotCreated = %+v, want shard 1 replica 1 index %d", created, snapIdx)
	}
	compacted := waitForSnapEvent(ctx, t, l, snapEvLogCompact, 1, 1)
	if compacted.replicaID != 1 || compacted.index == 0 || compacted.index > snapIdx {
		t.Fatalf("LogCompacted = %+v, want shard 1 replica 1 index in (0, %d]", compacted, snapIdx)
	}
	logdb := waitForSnapEvent(ctx, t, l, snapEvLogDBCompct, 1, 1)
	if logdb.replicaID != 1 || logdb.index != compacted.index {
		t.Fatalf("LogDBCompacted = %+v, want shard 1 replica 1 index %d", logdb, compacted.index)
	}

	events := l.Events()
	ci, _ := firstSnapEvent(events, snapEvCreated, 1, snapIdx)
	li, _ := firstSnapEvent(events, snapEvLogCompact, 1, 1)
	di, _ := firstSnapEvent(events, snapEvLogDBCompct, 1, 1)
	if ci >= li || li >= di {
		t.Fatalf("event order SnapshotCreated@%d LogCompacted@%d LogDBCompacted@%d, want increasing; events %+v",
			ci, li, di, events)
	}
}

// TestIntegration_DB_SnapshotEvents_RecoveredOnLaggingReplica checks (c): a
// replica added after the shard snapshotted and compacted is sent the
// snapshot, and its host fires SnapshotRecovered for that shard and replica
// with an index at or above the snapshot's.
func TestIntegration_DB_SnapshotEvents_RecoveredOnLaggingReplica(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), snapshotEventsBudget)
	defer cancel()

	listeners := []*snapshotEventListener{{}, {}, {}}
	cluster := newListenedDBTestCluster(ctx, t, listeners)
	defer cluster.close()

	trackers := cluster.startShardWithConfig(1, func(shardID, replicaID uint64) dbConfig.Config {
		return dbTestShardConfigWithSnapshot(shardID, replicaID, 10, 5)
	})
	leaderIdx := cluster.waitForClusterLeader(1, leaderElectionTimeout)
	proposeAndWait(t, cluster, trackers, 1, "warmup", "ok")
	waitForReplication(t, cluster, trackers, 1, "warmup", "ok", defaultTimeout)
	expected := proposeNEntries(t, cluster, trackers, 1, 30, "lag")
	verifyAllData(t, cluster, trackers, 1, expected)

	snapIdx := requestCompactingSnapshot(ctx, t, cluster.hosts[leaderIdx], 1)
	waitForSnapEvent(ctx, t, listeners[leaderIdx], snapEvLogCompact, 1, 1)

	membership := cluster.membershipFromLeader(t, 1, membershipTimeout)
	newListener := &snapshotEventListener{}
	nhConfig := dbTestNodeHostConfig(t, kernelAddr, cluster.pki.leaf(t))
	nhConfig.SystemEventListener = newListener
	newHost, err := dragonboat.NewNodeHostWithOptions(nhConfig, quicraft.WithMemoryLogDB(), quicraft.WithoutEncryption())
	if err != nil {
		t.Fatalf("NewNodeHostWithOptions for the 4th host failed: %v", err)
	}
	defer newHost.Close()

	const newReplicaID = 4
	cluster.addReplicaToLeader(t, 1, newReplicaID, newHost.RaftAddress(), newHost.NodeIdentity(),
		membership.ConfigChangeID, membershipTimeout)
	createFn, _ := dbKVCreateFunc()
	cfg := dbTestShardConfigWithSnapshot(1, newReplicaID, 10, 5)
	cfg.MemberIdentities = cluster.identities()
	if err := newHost.StartReplica(nil, true, createFn, cfg); err != nil {
		t.Fatalf("StartReplica on the 4th host failed: %v", err)
	}

	recovered := waitForSnapEvent(ctx, t, newListener, snapEvRecovered, 1, snapIdx)
	if recovered.replicaID != newReplicaID {
		t.Fatalf("SnapshotRecovered = %+v, want replica %d", recovered, newReplicaID)
	}
}

// TestIntegration_DB_SnapshotEvents_PanickingListenerKeepsShard checks (d):
// a listener that panics in SnapshotCreated, LogCompacted and LogDBCompacted
// does not stop the shard; later writes and a later snapshot still succeed,
// and the later snapshot's events are still delivered.
func TestIntegration_DB_SnapshotEvents_PanickingListenerKeepsShard(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), snapshotEventsBudget)
	defer cancel()

	l := &snapshotEventListener{}
	l.panics.Store(true)
	nh := startListenedSingleNode(ctx, t, l)
	proposeSnapshotEventKeys(ctx, t, nh, "d1", 20)
	first := requestCompactingSnapshot(ctx, t, nh, 1)
	waitForSnapEvent(ctx, t, l, snapEvCreated, 1, first)
	firstCompact := waitForSnapEvent(ctx, t, l, snapEvLogCompact, 1, 1)
	waitForSnapEvent(ctx, t, l, snapEvLogDBCompct, 1, firstCompact.index)

	proposeSnapshotEventKeys(ctx, t, nh, "d2", 20)
	second := requestCompactingSnapshot(ctx, t, nh, 1)
	if second <= first {
		t.Fatalf("second snapshot index %d, want above the first %d", second, first)
	}
	waitForSnapEvent(ctx, t, l, snapEvCreated, 1, second)
	waitForSnapEvent(ctx, t, l, snapEvLogCompact, 1, firstCompact.index+1)
	waitForSnapEvent(ctx, t, l, snapEvLogDBCompct, 1, firstCompact.index+1)
	proposeSnapshotEventKeys(ctx, t, nh, "d3", 5)
}
