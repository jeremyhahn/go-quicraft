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
	"sync"
	"sync/atomic"
	"testing"
	"time"

	quicraft "github.com/jeremyhahn/go-quicraft/pkg"
	"github.com/jeremyhahn/go-quicraft/pkg/config"
	"github.com/jeremyhahn/go-quicraft/test/membership"
	"github.com/jeremyhahn/go-quicraft/test/testpki"
)

// Snapshot-transfer event kinds the recorder captures.
const (
	snapEvSendStarted = "OnSnapshotSendStarted"
	snapEvSent        = "OnSnapshotSent"
	snapEvSendAborted = "OnSnapshotSendAborted"
	snapEvReceived    = "OnSnapshotReceived"
)

// recordedSnapshotEvent is one snapshot-transfer callback a host delivered,
// in delivery order (seq).
type recordedSnapshotEvent struct {
	seq  int
	kind string
	info config.SnapshotInfo
}

// snapshotEventRecorder records every snapshot-transfer callback of one
// host's EventListener.
type snapshotEventRecorder struct {
	mu     sync.Mutex
	events []recordedSnapshotEvent
}

func (r *snapshotEventRecorder) record(kind string) func(config.SnapshotInfo) {
	return func(info config.SnapshotInfo) {
		r.mu.Lock()
		defer r.mu.Unlock()
		r.events = append(r.events, recordedSnapshotEvent{seq: len(r.events), kind: kind, info: info})
	}
}

func (r *snapshotEventRecorder) listener() *config.EventListener {
	return &config.EventListener{
		OnSnapshotSendStarted: r.record(snapEvSendStarted),
		OnSnapshotSent:        r.record(snapEvSent),
		OnSnapshotSendAborted: r.record(snapEvSendAborted),
		OnSnapshotReceived:    r.record(snapEvReceived),
	}
}

func (r *snapshotEventRecorder) snapshot() []recordedSnapshotEvent {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]recordedSnapshotEvent(nil), r.events...)
}

// firstOf returns the first recorded event of kind for shardID.
func firstOf(events []recordedSnapshotEvent, kind string, shardID uint64) (recordedSnapshotEvent, bool) {
	for _, ev := range events {
		if ev.kind == kind && ev.info.ShardID == shardID {
			return ev, true
		}
	}
	return recordedSnapshotEvent{}, false
}

// waitForSnapshotEvent polls rec until an event of kind for shardID is
// recorded, failing the test at the deadline.
func waitForSnapshotEvent(t *testing.T, rec *snapshotEventRecorder, kind string, shardID uint64, timeout time.Duration) recordedSnapshotEvent {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		if ev, ok := firstOf(rec.snapshot(), kind, shardID); ok {
			return ev
		}
		if time.Now().After(deadline) {
			t.Fatalf("no %s for shard %d within %v; recorded: %+v", kind, shardID, timeout, rec.snapshot())
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// TestHostAPI_SnapshotTransferEvents proves the snapshot-transfer callbacks
// config.EventListener documents on a real snapshot catch-up over QUIC: a
// one-voter shard compacts its log, a new replica joins and must catch up by
// snapshot, and the leader reports OnSnapshotSendStarted then
// OnSnapshotSent (never OnSnapshotSendAborted) while the joiner reports
// OnSnapshotReceived, all naming the same shard and snapshot index, and the
// joiner's event names the leader as its sender (SnapshotInfo.From).
func TestHostAPI_SnapshotTransferEvents(t *testing.T) {
	const (
		shardID         uint64 = 1
		leaderReplicaID uint64 = 1
		joinerReplicaID uint64 = 2
		entries                = 150
	)
	snapshotCfg := func(sid, rid uint64) config.Config {
		return testShardConfigWithSnapshot(sid, rid, 100, 10)
	}

	// A one-host cluster whose host records its snapshot-transfer events.
	assertApplyOrder(t)
	ca := testpki.NewCA(t)
	leaves := ca.Leaves(t, 1)
	leaderRec := &snapshotEventRecorder{}
	leaderCfg := testHostConfig(t, kernelAddr, leaves[0])
	leaderCfg.EventListener = leaderRec.listener()
	leaderHost, err := quicraft.NewHost(leaderCfg, quicraft.WithoutEncryption())
	if err != nil {
		t.Fatalf("NewHost (leader) failed: %v", err)
	}
	cluster := &testCluster{
		hosts:        []*quicraft.Host{leaderHost},
		addrs:        []string{leaderHost.RaftAddress()},
		stopped:      make([]atomic.Bool, 1),
		nodeHostDirs: []string{leaderCfg.NodeHostDir},
		ca:           ca,
		leaves:       leaves,
		ids:          nodeIdentities([]*quicraft.Host{leaderHost}),
		t:            t,
	}
	t.Cleanup(func() { cluster.close() })

	trackers := cluster.startShardWithConfig(shardID, snapshotCfg)
	cluster.waitForClusterLeader(shardID, leaderElectionTimeout)

	// Enough entries to save a snapshot and compact the log behind it, so
	// the joiner cannot catch up by log replay.
	data := proposeNEntries(t, cluster, trackers, shardID, entries, "snap-ev")
	verifyAllData(t, cluster, trackers, shardID, data)

	// The joiner: its own host and leaf, with its own recorder.
	joinerRec := &snapshotEventRecorder{}
	leaf := ca.Leaf(t)
	joinerCfg := testHostConfig(t, kernelAddr, leaf)
	joinerCfg.EventListener = joinerRec.listener()
	joinerHost, err := quicraft.NewHost(joinerCfg, quicraft.WithoutEncryption())
	if err != nil {
		t.Fatalf("NewHost (joiner) failed: %v", err)
	}
	joinerAddr := joinerHost.RaftAddress()
	if err := membership.TryAddNode(cluster.leaderHost, shardID, joinerReplicaID,
		joinerAddr, joinerHost.NodeIdentity(), membershipTimeout); err != nil {
		joinerHost.Close()
		t.Fatal(err)
	}
	createFn, joinerTracker := kvCreateFunc()
	scfg := snapshotCfg(shardID, joinerReplicaID)
	scfg.MemberIdentities = cluster.identities()
	if err := joinerHost.StartShard(nil, true, createFn, scfg); err != nil {
		joinerHost.Close()
		t.Fatalf("StartShard(join=true) for replica %d failed: %v", joinerReplicaID, err)
	}
	cluster.hostsMu.Lock()
	cluster.hosts = append(cluster.hosts, joinerHost)
	cluster.addrs = append(cluster.addrs, joinerAddr)
	cluster.trackers = append(cluster.trackers, joinerTracker)
	cluster.stopped = append(cluster.stopped, atomic.Bool{})
	cluster.nodeHostDirs = append(cluster.nodeHostDirs, joinerCfg.NodeHostDir)
	cluster.leaves = append(cluster.leaves, leaf)
	cluster.ids = append(cluster.ids, joinerHost.NodeIdentity())
	cluster.hostsMu.Unlock()

	// (c) The joiner applies every entry, and it got them from a snapshot.
	waitForNodeData(t, joinerTracker, shardID, joinerReplicaID, data, membershipTimeout)
	joinerSM := joinerTracker.Get(shardID, joinerReplicaID)
	if joinerSM == nil {
		t.Fatalf("joiner (replica %d) has no state machine after catch-up", joinerReplicaID)
	}
	if joinerSM.Recoveries() == 0 {
		t.Fatalf("joiner (replica %d) never restored from a snapshot: it caught up by "+
			"log replay, so no snapshot transfer was exercised", joinerReplicaID)
	}

	// (a) The leader reported the send starting, then completing, at one
	// index, and never aborting.
	sent := waitForSnapshotEvent(t, leaderRec, snapEvSent, shardID, membershipTimeout)
	started, ok := firstOf(leaderRec.snapshot(), snapEvSendStarted, shardID)
	if !ok {
		t.Fatalf("leader reported %s without %s: %+v", snapEvSent, snapEvSendStarted, leaderRec.snapshot())
	}
	if started.seq > sent.seq {
		t.Fatalf("leader reported %s (seq %d) before %s (seq %d)",
			snapEvSent, sent.seq, snapEvSendStarted, started.seq)
	}
	if sent.info.Index == 0 || started.info.Index != sent.info.Index {
		t.Fatalf("leader send events disagree on the snapshot index: started %+v, sent %+v",
			started.info, sent.info)
	}
	if aborted, ok := firstOf(leaderRec.snapshot(), snapEvSendAborted, shardID); ok {
		t.Fatalf("leader reported %s on a completed catch-up: %+v", snapEvSendAborted, aborted.info)
	}

	// (b) The joiner reported receiving that snapshot from the leader.
	recv := waitForSnapshotEvent(t, joinerRec, snapEvReceived, shardID, membershipTimeout)
	if recv.info.Index != sent.info.Index {
		t.Fatalf("joiner received index %d, leader sent index %d", recv.info.Index, sent.info.Index)
	}
	if recv.info.From != leaderReplicaID {
		t.Fatalf("joiner %s From = %d, want the leader's replica ID %d (info %+v)",
			snapEvReceived, recv.info.From, leaderReplicaID, recv.info)
	}
	t.Logf("snapshot catch-up events: started %+v, sent %+v, received %+v; %d restores",
		started.info, sent.info, recv.info, joinerSM.Recoveries())
}
