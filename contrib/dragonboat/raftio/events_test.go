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

package raftio

import (
	"testing"

	qcConfig "github.com/jeremyhahn/go-quicraft/pkg/config"
)

// testRaftListener is a test implementation of IRaftEventListener.
type testRaftListener struct {
	leaderInfos []LeaderInfo
}

func (l *testRaftListener) LeaderUpdated(info LeaderInfo) {
	l.leaderInfos = append(l.leaderInfos, info)
}

// testSystemListener is a test implementation of ISystemEventListener.
type testSystemListener struct {
	nodeReadyCalls         []NodeInfo
	nodeUnloadedCalls      []NodeInfo
	nodeDeletedCalls       []NodeInfo
	membershipChangedCalls []NodeInfo
	connEstablishedCalls   []ConnectionInfo
	connFailedCalls        []ConnectionInfo
	snapshotSentCalls      []SnapshotInfo
	snapshotReceivedCalls  []SnapshotInfo
	snapshotCreatedCalls   []SnapshotInfo
	logCompactedCalls      []EntryInfo
	logDBCompactedCalls    []EntryInfo
	shuttingDownCalled     bool
	sendSnapshotStarted    []SnapshotInfo
	sendSnapshotAborted    []SnapshotInfo
	snapshotRecoveredCalls []SnapshotInfo
	snapshotCompactedCalls []SnapshotInfo
}

func (l *testSystemListener) NodeHostShuttingDown() { l.shuttingDownCalled = true }
func (l *testSystemListener) NodeUnloaded(info NodeInfo) {
	l.nodeUnloadedCalls = append(l.nodeUnloadedCalls, info)
}
func (l *testSystemListener) NodeDeleted(info NodeInfo) {
	l.nodeDeletedCalls = append(l.nodeDeletedCalls, info)
}
func (l *testSystemListener) NodeReady(info NodeInfo) {
	l.nodeReadyCalls = append(l.nodeReadyCalls, info)
}
func (l *testSystemListener) MembershipChanged(info NodeInfo) {
	l.membershipChangedCalls = append(l.membershipChangedCalls, info)
}
func (l *testSystemListener) ConnectionEstablished(info ConnectionInfo) {
	l.connEstablishedCalls = append(l.connEstablishedCalls, info)
}
func (l *testSystemListener) ConnectionFailed(info ConnectionInfo) {
	l.connFailedCalls = append(l.connFailedCalls, info)
}
func (l *testSystemListener) SendSnapshotStarted(info SnapshotInfo) {
	l.sendSnapshotStarted = append(l.sendSnapshotStarted, info)
}
func (l *testSystemListener) SendSnapshotCompleted(info SnapshotInfo) {
	l.snapshotSentCalls = append(l.snapshotSentCalls, info)
}
func (l *testSystemListener) SendSnapshotAborted(info SnapshotInfo) {
	l.sendSnapshotAborted = append(l.sendSnapshotAborted, info)
}
func (l *testSystemListener) SnapshotReceived(info SnapshotInfo) {
	l.snapshotReceivedCalls = append(l.snapshotReceivedCalls, info)
}
func (l *testSystemListener) SnapshotRecovered(info SnapshotInfo) {
	l.snapshotRecoveredCalls = append(l.snapshotRecoveredCalls, info)
}
func (l *testSystemListener) SnapshotCreated(info SnapshotInfo) {
	l.snapshotCreatedCalls = append(l.snapshotCreatedCalls, info)
}
func (l *testSystemListener) SnapshotCompacted(info SnapshotInfo) {
	l.snapshotCompactedCalls = append(l.snapshotCompactedCalls, info)
}
func (l *testSystemListener) LogCompacted(info EntryInfo) {
	l.logCompactedCalls = append(l.logCompactedCalls, info)
}
func (l *testSystemListener) LogDBCompacted(info EntryInfo) {
	l.logDBCompactedCalls = append(l.logDBCompactedCalls, info)
}

func TestConvertEventListeners_LeaderUpdated(t *testing.T) {
	rl := &testRaftListener{}
	el := ConvertEventListeners(rl, nil)

	if el.OnLeaderUpdated == nil {
		t.Fatal("OnLeaderUpdated callback must not be nil when raft listener is provided")
	}

	el.OnLeaderUpdated(qcConfig.LeaderInfo{
		ShardID:   1,
		ReplicaID: 2,
		Term:      3,
		LeaderID:  4,
	})

	if len(rl.leaderInfos) != 1 {
		t.Fatalf("expected 1 LeaderUpdated call, got %d", len(rl.leaderInfos))
	}
	info := rl.leaderInfos[0]
	if info.ShardID != 1 {
		t.Errorf("ShardID = %d, want 1", info.ShardID)
	}
	if info.ReplicaID != 2 {
		t.Errorf("ReplicaID = %d, want 2", info.ReplicaID)
	}
	if info.Term != 3 {
		t.Errorf("Term = %d, want 3", info.Term)
	}
	if info.LeaderID != 4 {
		t.Errorf("LeaderID = %d, want 4", info.LeaderID)
	}
}

func TestConvertEventListeners_NilListeners(t *testing.T) {
	el := ConvertEventListeners(nil, nil)

	if el.OnLeaderUpdated != nil {
		t.Error("OnLeaderUpdated should be nil when raft listener is nil")
	}
	if el.OnNodeReady != nil {
		t.Error("OnNodeReady should be nil when system listener is nil")
	}
	if el.OnNodeUnloaded != nil {
		t.Error("OnNodeUnloaded should be nil when system listener is nil")
	}
	if el.OnMembershipChanged != nil {
		t.Error("OnMembershipChanged should be nil when system listener is nil")
	}
	if el.OnConnectionEstablished != nil {
		t.Error("OnConnectionEstablished should be nil when system listener is nil")
	}
	if el.OnConnectionFailed != nil {
		t.Error("OnConnectionFailed should be nil when system listener is nil")
	}
	if el.OnSnapshotSent != nil {
		t.Error("OnSnapshotSent should be nil when system listener is nil")
	}
	if el.OnSnapshotReceived != nil {
		t.Error("OnSnapshotReceived should be nil when system listener is nil")
	}
	if el.OnSnapshotCreated != nil {
		t.Error("OnSnapshotCreated should be nil when system listener is nil")
	}
	if el.OnLogCompacted != nil {
		t.Error("OnLogCompacted should be nil when system listener is nil")
	}
	if el.OnNodeHostShuttingDown != nil {
		t.Error("OnNodeHostShuttingDown should be nil when system listener is nil")
	}
	if el.OnNodeDeleted != nil {
		t.Error("OnNodeDeleted should be nil when system listener is nil")
	}
	if el.OnSnapshotSendStarted != nil {
		t.Error("OnSnapshotSendStarted should be nil when system listener is nil")
	}
	if el.OnSnapshotSendAborted != nil {
		t.Error("OnSnapshotSendAborted should be nil when system listener is nil")
	}
	if el.OnSnapshotRecovered != nil {
		t.Error("OnSnapshotRecovered should be nil when system listener is nil")
	}
	if el.OnSnapshotCompacted != nil {
		t.Error("OnSnapshotCompacted should be nil when system listener is nil")
	}
	if el.OnLogDBCompacted != nil {
		t.Error("OnLogDBCompacted should be nil when system listener is nil")
	}
}

func TestConvertEventListeners_SystemEvents(t *testing.T) {
	sl := &testSystemListener{}
	el := ConvertEventListeners(nil, sl)

	// NodeReady
	el.OnNodeReady(qcConfig.EventShardInfo{ShardID: 10, ReplicaID: 20})
	if len(sl.nodeReadyCalls) != 1 {
		t.Fatalf("expected 1 NodeReady call, got %d", len(sl.nodeReadyCalls))
	}
	if sl.nodeReadyCalls[0].ShardID != 10 || sl.nodeReadyCalls[0].ReplicaID != 20 {
		t.Errorf("NodeReady info mismatch: %+v", sl.nodeReadyCalls[0])
	}

	// NodeUnloaded
	el.OnNodeUnloaded(qcConfig.EventShardInfo{ShardID: 11, ReplicaID: 21})
	if len(sl.nodeUnloadedCalls) != 1 {
		t.Fatalf("expected 1 NodeUnloaded call, got %d", len(sl.nodeUnloadedCalls))
	}
	if sl.nodeUnloadedCalls[0].ShardID != 11 || sl.nodeUnloadedCalls[0].ReplicaID != 21 {
		t.Errorf("NodeUnloaded info mismatch: %+v", sl.nodeUnloadedCalls[0])
	}

	// MembershipChanged
	el.OnMembershipChanged(qcConfig.EventShardInfo{ShardID: 12, ReplicaID: 22})
	if len(sl.membershipChangedCalls) != 1 {
		t.Fatalf("expected 1 MembershipChanged call, got %d", len(sl.membershipChangedCalls))
	}

	// ConnectionEstablished
	el.OnConnectionEstablished(qcConfig.ConnectionInfo{Address: "10.0.0.1:5000"})
	if len(sl.connEstablishedCalls) != 1 {
		t.Fatalf("expected 1 ConnectionEstablished call, got %d", len(sl.connEstablishedCalls))
	}
	if sl.connEstablishedCalls[0].Address != "10.0.0.1:5000" {
		t.Errorf("ConnectionEstablished address = %q, want %q", sl.connEstablishedCalls[0].Address, "10.0.0.1:5000")
	}

	// ConnectionFailed
	el.OnConnectionFailed(qcConfig.ConnectionInfo{Address: "10.0.0.2:5000"})
	if len(sl.connFailedCalls) != 1 {
		t.Fatalf("expected 1 ConnectionFailed call, got %d", len(sl.connFailedCalls))
	}

	// SnapshotSent -> SendSnapshotCompleted
	el.OnSnapshotSent(qcConfig.SnapshotInfo{ShardID: 13, ReplicaID: 23, From: 1, Index: 100})
	if len(sl.snapshotSentCalls) != 1 {
		t.Fatalf("expected 1 SendSnapshotCompleted call, got %d", len(sl.snapshotSentCalls))
	}
	if sl.snapshotSentCalls[0].Index != 100 {
		t.Errorf("SnapshotSent Index = %d, want 100", sl.snapshotSentCalls[0].Index)
	}

	// SnapshotReceived
	el.OnSnapshotReceived(qcConfig.SnapshotInfo{ShardID: 14, ReplicaID: 24, From: 2, Index: 200})
	if len(sl.snapshotReceivedCalls) != 1 {
		t.Fatalf("expected 1 SnapshotReceived call, got %d", len(sl.snapshotReceivedCalls))
	}

	// SnapshotCreated
	el.OnSnapshotCreated(qcConfig.SnapshotInfo{ShardID: 15, ReplicaID: 25, Index: 300})
	if len(sl.snapshotCreatedCalls) != 1 {
		t.Fatalf("expected 1 SnapshotCreated call, got %d", len(sl.snapshotCreatedCalls))
	}

	// LogCompacted
	el.OnLogCompacted(qcConfig.EventShardInfo{ShardID: 16, ReplicaID: 26})
	if len(sl.logCompactedCalls) != 1 {
		t.Fatalf("expected 1 LogCompacted call, got %d", len(sl.logCompactedCalls))
	}

	// NodeHostShuttingDown
	el.OnNodeHostShuttingDown()
	if !sl.shuttingDownCalled {
		t.Error("NodeHostShuttingDown was not called")
	}

	// NodeDeleted
	el.OnNodeDeleted(qcConfig.EventShardInfo{ShardID: 17, ReplicaID: 27})
	if len(sl.nodeDeletedCalls) != 1 {
		t.Fatalf("expected 1 NodeDeleted call, got %d", len(sl.nodeDeletedCalls))
	}
	if sl.nodeDeletedCalls[0].ShardID != 17 || sl.nodeDeletedCalls[0].ReplicaID != 27 {
		t.Errorf("NodeDeleted info mismatch: %+v", sl.nodeDeletedCalls[0])
	}

	// SendSnapshotStarted
	el.OnSnapshotSendStarted(qcConfig.SnapshotInfo{ShardID: 18, ReplicaID: 28, From: 3, Index: 400})
	if len(sl.sendSnapshotStarted) != 1 {
		t.Fatalf("expected 1 SendSnapshotStarted call, got %d", len(sl.sendSnapshotStarted))
	}
	if sl.sendSnapshotStarted[0].Index != 400 {
		t.Errorf("SendSnapshotStarted Index = %d, want 400", sl.sendSnapshotStarted[0].Index)
	}

	// SendSnapshotAborted
	el.OnSnapshotSendAborted(qcConfig.SnapshotInfo{ShardID: 19, ReplicaID: 29, From: 4, Index: 500})
	if len(sl.sendSnapshotAborted) != 1 {
		t.Fatalf("expected 1 SendSnapshotAborted call, got %d", len(sl.sendSnapshotAborted))
	}
	if sl.sendSnapshotAborted[0].Index != 500 {
		t.Errorf("SendSnapshotAborted Index = %d, want 500", sl.sendSnapshotAborted[0].Index)
	}

	// SnapshotRecovered
	el.OnSnapshotRecovered(qcConfig.SnapshotInfo{ShardID: 20, ReplicaID: 30, From: 5, Index: 600})
	if len(sl.snapshotRecoveredCalls) != 1 {
		t.Fatalf("expected 1 SnapshotRecovered call, got %d", len(sl.snapshotRecoveredCalls))
	}
	if sl.snapshotRecoveredCalls[0].Index != 600 {
		t.Errorf("SnapshotRecovered Index = %d, want 600", sl.snapshotRecoveredCalls[0].Index)
	}

	// SnapshotCompacted
	el.OnSnapshotCompacted(qcConfig.SnapshotInfo{ShardID: 21, ReplicaID: 31, From: 6, Index: 700})
	if len(sl.snapshotCompactedCalls) != 1 {
		t.Fatalf("expected 1 SnapshotCompacted call, got %d", len(sl.snapshotCompactedCalls))
	}
	if sl.snapshotCompactedCalls[0].Index != 700 {
		t.Errorf("SnapshotCompacted Index = %d, want 700", sl.snapshotCompactedCalls[0].Index)
	}

	// LogDBCompacted
	el.OnLogDBCompacted(qcConfig.EventShardInfo{ShardID: 22, ReplicaID: 32})
	if len(sl.logDBCompactedCalls) != 1 {
		t.Fatalf("expected 1 LogDBCompacted call, got %d", len(sl.logDBCompactedCalls))
	}
	if sl.logDBCompactedCalls[0].ShardID != 22 || sl.logDBCompactedCalls[0].ReplicaID != 32 {
		t.Errorf("LogDBCompacted info mismatch: %+v", sl.logDBCompactedCalls[0])
	}
}

func TestConvertEventListeners_NilRaftOnlySystemProvided(t *testing.T) {
	sl := &testSystemListener{}
	el := ConvertEventListeners(nil, sl)

	if el.OnLeaderUpdated != nil {
		t.Error("OnLeaderUpdated should be nil when raft listener is nil")
	}
	if el.OnNodeReady == nil {
		t.Error("OnNodeReady should not be nil when system listener is provided")
	}
}

func TestConvertEventListeners_NilSystemOnlyRaftProvided(t *testing.T) {
	rl := &testRaftListener{}
	el := ConvertEventListeners(rl, nil)

	if el.OnLeaderUpdated == nil {
		t.Error("OnLeaderUpdated should not be nil when raft listener is provided")
	}
	if el.OnNodeReady != nil {
		t.Error("OnNodeReady should be nil when system listener is nil")
	}
}

func TestNoLeaderConstant(t *testing.T) {
	if NoLeader != 0 {
		t.Errorf("NoLeader = %d, want 0", NoLeader)
	}
}

func TestGetNodeInfo(t *testing.T) {
	ni := GetNodeInfo(42, 7)
	if ni.ShardID != 42 {
		t.Errorf("ShardID = %d, want 42", ni.ShardID)
	}
	if ni.ReplicaID != 7 {
		t.Errorf("ReplicaID = %d, want 7", ni.ReplicaID)
	}
}

func TestGetNodeInfo_ZeroValues(t *testing.T) {
	ni := GetNodeInfo(0, 0)
	if ni.ShardID != 0 {
		t.Errorf("ShardID = %d, want 0", ni.ShardID)
	}
	if ni.ReplicaID != 0 {
		t.Errorf("ReplicaID = %d, want 0", ni.ReplicaID)
	}
}
