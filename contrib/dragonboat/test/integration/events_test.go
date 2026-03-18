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
	"sync"
	"testing"

	dragonboat "github.com/jeremyhahn/go-quicraft/contrib/dragonboat"
	"github.com/jeremyhahn/go-quicraft/contrib/dragonboat/raftio"
	quicraft "github.com/jeremyhahn/go-quicraft/pkg"
)

// ---------------------------------------------------------------------------
// Test Raft Event Listener
// ---------------------------------------------------------------------------

// testRaftEventListener implements raftio.IRaftEventListener for capturing
// LeaderUpdated events during integration tests.
type testRaftEventListener struct {
	mu     sync.Mutex
	events []raftio.LeaderInfo
}

// LeaderUpdated records a leader change event.
func (l *testRaftEventListener) LeaderUpdated(info raftio.LeaderInfo) {
	l.mu.Lock()
	l.events = append(l.events, info)
	l.mu.Unlock()
}

// Events returns a snapshot copy of all recorded leader events.
func (l *testRaftEventListener) Events() []raftio.LeaderInfo {
	l.mu.Lock()
	defer l.mu.Unlock()
	cp := make([]raftio.LeaderInfo, len(l.events))
	copy(cp, l.events)
	return cp
}

// Compile-time interface assertion.
var _ raftio.IRaftEventListener = (*testRaftEventListener)(nil)

// ---------------------------------------------------------------------------
// Test System Event Listener
// ---------------------------------------------------------------------------

// testSystemEventListener implements raftio.ISystemEventListener for capturing
// system-level events during integration tests. Most methods are no-ops; only
// NodeReady, NodeUnloaded, and MembershipChanged are tracked.
type testSystemEventListener struct {
	mu                sync.Mutex
	nodeReady         []raftio.NodeInfo
	nodeUnloaded      []raftio.NodeInfo
	membershipChanged []raftio.NodeInfo
}

func (l *testSystemEventListener) NodeHostShuttingDown()                         {}
func (l *testSystemEventListener) NodeDeleted(_ raftio.NodeInfo)                 {}
func (l *testSystemEventListener) ConnectionEstablished(_ raftio.ConnectionInfo) {}
func (l *testSystemEventListener) ConnectionFailed(_ raftio.ConnectionInfo)      {}
func (l *testSystemEventListener) SendSnapshotStarted(_ raftio.SnapshotInfo)     {}
func (l *testSystemEventListener) SendSnapshotCompleted(_ raftio.SnapshotInfo)   {}
func (l *testSystemEventListener) SendSnapshotAborted(_ raftio.SnapshotInfo)     {}
func (l *testSystemEventListener) SnapshotReceived(_ raftio.SnapshotInfo)        {}
func (l *testSystemEventListener) SnapshotRecovered(_ raftio.SnapshotInfo)       {}
func (l *testSystemEventListener) SnapshotCreated(_ raftio.SnapshotInfo)         {}
func (l *testSystemEventListener) SnapshotCompacted(_ raftio.SnapshotInfo)       {}
func (l *testSystemEventListener) LogCompacted(_ raftio.EntryInfo)               {}
func (l *testSystemEventListener) LogDBCompacted(_ raftio.EntryInfo)             {}

// NodeReady records that a shard/replica became ready.
func (l *testSystemEventListener) NodeReady(info raftio.NodeInfo) {
	l.mu.Lock()
	l.nodeReady = append(l.nodeReady, info)
	l.mu.Unlock()
}

// NodeUnloaded records that a shard/replica was unloaded.
func (l *testSystemEventListener) NodeUnloaded(info raftio.NodeInfo) {
	l.mu.Lock()
	l.nodeUnloaded = append(l.nodeUnloaded, info)
	l.mu.Unlock()
}

// MembershipChanged records that a shard/replica membership changed.
func (l *testSystemEventListener) MembershipChanged(info raftio.NodeInfo) {
	l.mu.Lock()
	l.membershipChanged = append(l.membershipChanged, info)
	l.mu.Unlock()
}

// NodeReadyEvents returns a snapshot copy of all NodeReady events.
func (l *testSystemEventListener) NodeReadyEvents() []raftio.NodeInfo {
	l.mu.Lock()
	defer l.mu.Unlock()
	cp := make([]raftio.NodeInfo, len(l.nodeReady))
	copy(cp, l.nodeReady)
	return cp
}

// NodeUnloadedEvents returns a snapshot copy of all NodeUnloaded events.
func (l *testSystemEventListener) NodeUnloadedEvents() []raftio.NodeInfo {
	l.mu.Lock()
	defer l.mu.Unlock()
	cp := make([]raftio.NodeInfo, len(l.nodeUnloaded))
	copy(cp, l.nodeUnloaded)
	return cp
}

// MembershipChangedEvents returns a snapshot copy of all MembershipChanged events.
func (l *testSystemEventListener) MembershipChangedEvents() []raftio.NodeInfo {
	l.mu.Lock()
	defer l.mu.Unlock()
	cp := make([]raftio.NodeInfo, len(l.membershipChanged))
	copy(cp, l.membershipChanged)
	return cp
}

// Compile-time interface assertion.
var _ raftio.ISystemEventListener = (*testSystemEventListener)(nil)

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

// TestIntegration_DB_RaftEventListener_LeaderUpdated verifies that a
// RaftEventListener receives LeaderUpdated callbacks during leader election
// on a single-node cluster.
func TestIntegration_DB_RaftEventListener_LeaderUpdated(t *testing.T) {
	listener := &testRaftEventListener{}
	mtls := dbTestMTLSFiles(t)
	addr := freePort(t)

	nhConfig := dbTestNodeHostConfig(t, addr, mtls)
	nhConfig.RaftEventListener = listener

	nh, err := dragonboat.NewNodeHostWithOptions(nhConfig, quicraft.WithMemoryLogDB())
	if err != nil {
		t.Fatalf("NewNodeHostWithOptions failed: %v", err)
	}
	defer nh.Close()

	// Start a single-node shard.
	members := map[uint64]dragonboat.Target{
		1: addr,
	}
	createFn, _ := dbKVCreateFunc()
	cfg := dbTestShardConfig(1, 1)
	if err := nh.StartReplica(members, false, createFn, cfg); err != nil {
		t.Fatalf("StartReplica failed: %v", err)
	}

	// Wait for leader election to produce at least one LeaderUpdated event
	// with a non-zero LeaderID.
	waitForCondition(t, leaderElectionTimeout,
		"RaftEventListener receives LeaderUpdated with LeaderID != 0",
		func() bool {
			for _, ev := range listener.Events() {
				if ev.LeaderID != 0 {
					return true
				}
			}
			return false
		},
	)

	// Verify at least one event has the expected shard and leader.
	events := listener.Events()
	t.Logf("received %d LeaderUpdated events", len(events))
	found := false
	for i, ev := range events {
		t.Logf("  event[%d]: ShardID=%d ReplicaID=%d Term=%d LeaderID=%d",
			i, ev.ShardID, ev.ReplicaID, ev.Term, ev.LeaderID)
		if ev.ShardID == 1 && ev.LeaderID == 1 {
			found = true
		}
	}
	if !found {
		t.Fatal("expected at least one LeaderUpdated event with ShardID=1 and LeaderID=1")
	}
}

// TestIntegration_DB_SystemEventListener_NodeReady verifies that a
// SystemEventListener receives NodeReady callbacks when a shard becomes
// ready on a single-node cluster.
func TestIntegration_DB_SystemEventListener_NodeReady(t *testing.T) {
	sysListener := &testSystemEventListener{}
	mtls := dbTestMTLSFiles(t)
	addr := freePort(t)

	nhConfig := dbTestNodeHostConfig(t, addr, mtls)
	nhConfig.SystemEventListener = sysListener

	nh, err := dragonboat.NewNodeHostWithOptions(nhConfig, quicraft.WithMemoryLogDB())
	if err != nil {
		t.Fatalf("NewNodeHostWithOptions failed: %v", err)
	}
	defer nh.Close()

	// Start a single-node shard.
	members := map[uint64]dragonboat.Target{
		1: addr,
	}
	createFn, _ := dbKVCreateFunc()
	cfg := dbTestShardConfig(1, 1)
	if err := nh.StartReplica(members, false, createFn, cfg); err != nil {
		t.Fatalf("StartReplica failed: %v", err)
	}

	// Wait for the shard to become ready (leader elected implies node ready).
	waitForCondition(t, leaderElectionTimeout,
		"SystemEventListener receives NodeReady event",
		func() bool {
			return len(sysListener.NodeReadyEvents()) > 0
		},
	)

	// Verify the NodeReady event has the expected ShardID.
	events := sysListener.NodeReadyEvents()
	t.Logf("received %d NodeReady events", len(events))
	found := false
	for i, ev := range events {
		t.Logf("  event[%d]: ShardID=%d ReplicaID=%d", i, ev.ShardID, ev.ReplicaID)
		if ev.ShardID == 1 {
			found = true
		}
	}
	if !found {
		t.Fatal("expected at least one NodeReady event with ShardID=1")
	}
}

// TestIntegration_DB_SystemEventListener_NodeUnloaded verifies that a
// SystemEventListener receives NodeUnloaded callbacks when a shard is
// stopped on a single-node cluster.
func TestIntegration_DB_SystemEventListener_NodeUnloaded(t *testing.T) {
	sysListener := &testSystemEventListener{}
	mtls := dbTestMTLSFiles(t)
	addr := freePort(t)

	nhConfig := dbTestNodeHostConfig(t, addr, mtls)
	nhConfig.SystemEventListener = sysListener

	nh, err := dragonboat.NewNodeHostWithOptions(nhConfig, quicraft.WithMemoryLogDB())
	if err != nil {
		t.Fatalf("NewNodeHostWithOptions failed: %v", err)
	}
	defer nh.Close()

	// Start a single-node shard.
	members := map[uint64]dragonboat.Target{
		1: addr,
	}
	createFn, _ := dbKVCreateFunc()
	cfg := dbTestShardConfig(1, 1)
	if err := nh.StartReplica(members, false, createFn, cfg); err != nil {
		t.Fatalf("StartReplica failed: %v", err)
	}

	// Wait for the shard to become ready before stopping it.
	waitForCondition(t, leaderElectionTimeout,
		"shard becomes ready before stop",
		func() bool {
			_, _, valid, getErr := nh.GetLeaderID(1)
			return getErr == nil && valid
		},
	)

	// Stop the shard to trigger NodeUnloaded.
	if err := nh.StopShard(1); err != nil {
		t.Fatalf("StopShard failed: %v", err)
	}

	// Wait for the NodeUnloaded event.
	waitForCondition(t, defaultTimeout,
		"SystemEventListener receives NodeUnloaded event",
		func() bool {
			return len(sysListener.NodeUnloadedEvents()) > 0
		},
	)

	// Verify the NodeUnloaded event has the expected ShardID.
	events := sysListener.NodeUnloadedEvents()
	t.Logf("received %d NodeUnloaded events", len(events))
	found := false
	for i, ev := range events {
		t.Logf("  event[%d]: ShardID=%d ReplicaID=%d", i, ev.ShardID, ev.ReplicaID)
		if ev.ShardID == 1 {
			found = true
		}
	}
	if !found {
		t.Fatal("expected at least one NodeUnloaded event with ShardID=1")
	}
}
