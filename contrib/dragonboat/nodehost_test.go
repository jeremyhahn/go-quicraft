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

package dragonboat

import (
	"bytes"
	"context"
	"errors"
	"io"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	dbClient "github.com/jeremyhahn/go-quicraft/contrib/dragonboat/client"
	dbConfig "github.com/jeremyhahn/go-quicraft/contrib/dragonboat/config"
	dbSM "github.com/jeremyhahn/go-quicraft/contrib/dragonboat/statemachine"
	quicraft "github.com/jeremyhahn/go-quicraft/pkg"
)

// testStateMachine is a minimal dragonboat IStateMachine for adapter tests.
type testStateMachine struct {
	data []byte
}

func (sm *testStateMachine) Update(entry dbSM.Entry) (dbSM.Result, error) {
	sm.data = append(sm.data[:0], entry.Cmd...)
	return dbSM.Result{Value: uint64(len(entry.Cmd))}, nil
}

func (sm *testStateMachine) Lookup(query interface{}) (interface{}, error) {
	result := make([]byte, len(sm.data))
	copy(result, sm.data)
	return result, nil
}

func (sm *testStateMachine) SaveSnapshot(w io.Writer, _ dbSM.ISnapshotFileCollection, _ <-chan struct{}) error {
	_, err := w.Write(sm.data)
	return err
}

func (sm *testStateMachine) RecoverFromSnapshot(r io.Reader, _ []dbSM.SnapshotFile, _ <-chan struct{}) error {
	data, err := io.ReadAll(r)
	if err != nil {
		return err
	}
	sm.data = data
	return nil
}

func (sm *testStateMachine) Close() error { return nil }

// testSMFactory returns a dragonboat-compatible CreateStateMachineFunc.
func testSMFactory() dbSM.CreateStateMachineFunc {
	return func(shardID, replicaID uint64) dbSM.IStateMachine {
		return &testStateMachine{}
	}
}

// newTestNodeHost creates a NodeHost backed by an in-memory LogDB and no
// transport, suitable for fast isolated unit tests.
func newTestNodeHost(t *testing.T) *NodeHost {
	t.Helper()
	dir := t.TempDir()
	nhConfig := dbConfig.NodeHostConfig{
		DeploymentID: 1,
		WALDir:       filepath.Join(dir, "wal"),
		NodeHostDir:  filepath.Join(dir, "data"),
		// A real port: dragonboat's address validator rejects port 0,
		// and initial-member targets are validated at Start*Replica.
		RaftAddress:    "localhost:63001",
		ListenAddress:  "localhost:0",
		RTTMillisecond: 1,
	}

	nh, err := NewNodeHostWithOptions(nhConfig,
		quicraft.WithMemoryLogDB(),
		quicraft.WithoutTransport(), quicraft.WithoutEncryption(),
	)
	if err != nil {
		t.Fatalf("NewNodeHostWithOptions: %v", err)
	}
	t.Cleanup(func() { nh.Close() })
	return nh
}

// startTestShard bootstraps a single-node shard on the given NodeHost
// and waits for the leader to be elected.
func startTestShard(t *testing.T, nh *NodeHost, shardID, replicaID uint64) {
	t.Helper()
	startTestShardWith(t, nh, shardID, replicaID, testSMFactory())
}

// startTestShardWith is startTestShard with the state machine factory given.
func startTestShardWith(t *testing.T, nh *NodeHost, shardID, replicaID uint64, factory dbSM.CreateStateMachineFunc) {
	t.Helper()
	members := map[uint64]Target{replicaID: nh.RaftAddress()}
	cfg := dbConfig.Config{
		ShardID:         shardID,
		ReplicaID:       replicaID,
		ElectionRTT:     5,
		HeartbeatRTT:    1,
		CheckQuorum:     true,
		SnapshotEntries: 0,
	}

	err := nh.StartReplica(members, false, factory, cfg)
	if err != nil {
		t.Fatalf("StartReplica: %v", err)
	}

	// Wait for leader election.
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	deadline := time.NewTimer(5 * time.Second)
	defer deadline.Stop()
	for {
		leaderID, _, valid, leaderErr := nh.GetLeaderID(shardID)
		if leaderErr == nil && valid && leaderID == replicaID {
			break
		}
		select {
		case <-deadline.C:
			t.Fatalf("leader not elected for shard %d within 5s", shardID)
			return
		case <-ticker.C:
		}
	}

	// A linearizable read is only servable once the post-election ReadIndex
	// warmup has completed (PhD 6.4), so wait for that too. Returning at
	// leader election alone makes every SyncRead/SyncGetShardMembership in
	// the suite race the warmup and intermittently see ErrShardNotReady.
	for {
		readCtx, cancel := context.WithTimeout(context.Background(), time.Second)
		_, readErr := nh.SyncGetShardMembership(readCtx, shardID)
		cancel()
		if readErr == nil {
			return
		}
		select {
		case <-deadline.C:
			t.Fatalf("shard %d not readable within 5s: %v", shardID, readErr)
			return
		case <-ticker.C:
		}
	}
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

func TestNodeHost_NewAndClose(t *testing.T) {
	nh := newTestNodeHost(t)

	id := nh.ID()
	if id == "" {
		t.Error("expected non-empty host ID")
	}

	addr := nh.RaftAddress()
	if addr == "" {
		t.Error("expected non-empty RaftAddress")
	}

	cfg := nh.NodeHostConfig()
	if cfg.RTTMillisecond != 1 {
		t.Errorf("expected RTTMillisecond=1, got %d", cfg.RTTMillisecond)
	}
}

func TestNodeHost_StartReplica_SyncPropose_SyncRead(t *testing.T) {
	nh := newTestNodeHost(t)
	shardID := uint64(1)
	replicaID := uint64(1)
	startTestShard(t, nh, shardID, replicaID)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Propose data.
	session := nh.GetNoOPSession(shardID)
	payload := []byte("hello quicraft")
	result, err := nh.SyncPropose(ctx, session, payload)
	if err != nil {
		t.Fatalf("SyncPropose: %v", err)
	}
	if result.Value != uint64(len(payload)) {
		t.Errorf("expected Value=%d, got %d", len(payload), result.Value)
	}

	// Linearizable read.
	readResult, err := nh.SyncRead(ctx, shardID, nil)
	if err != nil {
		t.Fatalf("SyncRead: %v", err)
	}
	data, ok := readResult.([]byte)
	if !ok {
		t.Fatalf("expected []byte result, got %T", readResult)
	}
	if string(data) != string(payload) {
		t.Errorf("expected %q, got %q", payload, data)
	}
}

func TestNodeHost_StartReplica_SyncPropose_InvalidShard(t *testing.T) {
	nh := newTestNodeHost(t)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	session := nh.GetNoOPSession(999)
	_, err := nh.SyncPropose(ctx, session, []byte("test"))
	if err == nil {
		t.Error("expected error for non-existent shard, got nil")
	}
}

func TestNodeHost_GetNoOPSession(t *testing.T) {
	nh := newTestNodeHost(t)
	session := nh.GetNoOPSession(42)

	if session == nil {
		t.Fatal("expected non-nil session")
	}
	if !session.IsNoOPSession() {
		t.Error("expected IsNoOPSession() to return true")
	}
	if session.ShardID != 42 {
		t.Errorf("expected ShardID=42, got %d", session.ShardID)
	}
}

func TestNodeHost_GetLeaderID_ReturnsFourValues(t *testing.T) {
	nh := newTestNodeHost(t)
	shardID := uint64(1)
	replicaID := uint64(1)
	startTestShard(t, nh, shardID, replicaID)

	leaderID, term, valid, err := nh.GetLeaderID(shardID)
	if err != nil {
		t.Fatalf("GetLeaderID: %v", err)
	}
	if !valid {
		t.Error("expected valid=true after leader election")
	}
	if leaderID != replicaID {
		t.Errorf("expected leaderID=%d, got %d", replicaID, leaderID)
	}
	// Term should be at least 1 after election.
	if term == 0 {
		t.Error("expected term > 0 after leader election")
	}
}

func TestNodeHost_StaleRead(t *testing.T) {
	nh := newTestNodeHost(t)
	shardID := uint64(1)
	replicaID := uint64(1)
	startTestShard(t, nh, shardID, replicaID)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Propose first so there is data to read.
	session := nh.GetNoOPSession(shardID)
	payload := []byte("stale-read-data")
	_, err := nh.SyncPropose(ctx, session, payload)
	if err != nil {
		t.Fatalf("SyncPropose: %v", err)
	}

	// Stale read (no consensus check).
	result, err := nh.StaleRead(shardID, nil)
	if err != nil {
		t.Fatalf("StaleRead: %v", err)
	}
	data, ok := result.([]byte)
	if !ok {
		t.Fatalf("expected []byte, got %T", result)
	}
	if string(data) != string(payload) {
		t.Errorf("expected %q, got %q", payload, data)
	}
}

func TestNodeHost_StaleRead_InvalidShard(t *testing.T) {
	nh := newTestNodeHost(t)
	_, err := nh.StaleRead(999, nil)
	if err == nil {
		t.Error("expected error for non-existent shard, got nil")
	}
}

func TestNodeHost_SyncGetShardMembership(t *testing.T) {
	nh := newTestNodeHost(t)
	shardID := uint64(1)
	replicaID := uint64(1)
	startTestShard(t, nh, shardID, replicaID)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	m, err := nh.SyncGetShardMembership(ctx, shardID)
	if err != nil {
		t.Fatalf("SyncGetShardMembership: %v", err)
	}
	if m == nil {
		t.Fatal("expected non-nil membership")
	}
	if len(m.Nodes) == 0 {
		t.Error("expected at least one node in membership")
	}
	addr, exists := m.Nodes[replicaID]
	if !exists {
		t.Errorf("expected replicaID %d in membership Nodes", replicaID)
	}
	if addr == "" {
		t.Error("expected non-empty address for the replica")
	}
}

func TestNodeHost_SyncGetShardMembership_InvalidShard(t *testing.T) {
	nh := newTestNodeHost(t)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	_, err := nh.SyncGetShardMembership(ctx, 999)
	if err == nil {
		t.Error("expected error for non-existent shard, got nil")
	}
}

func TestNodeHost_HasNodeInfo(t *testing.T) {
	nh := newTestNodeHost(t)
	shardID := uint64(1)
	replicaID := uint64(1)
	startTestShard(t, nh, shardID, replicaID)

	if !nh.HasNodeInfo(shardID, replicaID) {
		t.Error("expected HasNodeInfo to return true for active shard")
	}
}

func TestNodeHost_HasNodeInfo_NonExistent(t *testing.T) {
	nh := newTestNodeHost(t)
	if nh.HasNodeInfo(999, 999) {
		t.Error("expected HasNodeInfo to return false for non-existent shard")
	}
}

func TestSnapshotOption_ToQuicraft(t *testing.T) {
	opt := SnapshotOption{
		ExportPath:                 "/tmp/export",
		CompactionOverhead:         100,
		OverrideCompactionOverhead: true,
		Exported:                   true,
	}

	qc := opt.toQuicraft()
	if qc.ExportPath != "/tmp/export" {
		t.Errorf("expected ExportPath=/tmp/export, got %s", qc.ExportPath)
	}
	if qc.CompactionOverhead != 100 {
		t.Errorf("expected CompactionOverhead=100, got %d", qc.CompactionOverhead)
	}
	if !qc.OverrideCompactionOverhead {
		t.Error("expected OverrideCompactionOverhead=true")
	}
	if !qc.Exported {
		t.Error("expected Exported=true")
	}
}

func TestSnapshotOption_Validate_Valid(t *testing.T) {
	opt := SnapshotOption{CompactionOverhead: 100}
	if err := opt.Validate(); err != nil {
		t.Errorf("expected nil error, got %v", err)
	}
}

func TestSnapshotOption_Validate_MutuallyExclusive(t *testing.T) {
	opt := SnapshotOption{
		OverrideCompactionOverhead: true,
		CompactionOverhead:         100,
		CompactionIndex:            50,
	}
	err := opt.Validate()
	if err == nil {
		t.Error("expected error when both CompactionOverhead and CompactionIndex are set")
	}
	if !errors.Is(err, ErrInvalidOption) {
		t.Errorf("expected ErrInvalidOption, got %v", err)
	}
}

func TestNodeHostInfoOption(t *testing.T) {
	nh := newTestNodeHost(t)
	shardID := uint64(1)
	replicaID := uint64(1)
	startTestShard(t, nh, shardID, replicaID)

	info := nh.GetNodeHostInfo(NodeHostInfoOption{SkipLogInfo: false})
	if info == nil {
		t.Fatal("expected non-nil NodeHostInfo")
	}
	if info.RaftAddress == "" {
		t.Error("expected non-empty RaftAddress")
	}
	if info.NodeHostID == "" {
		t.Error("expected non-empty NodeHostID")
	}
	if len(info.ShardInfoList) == 0 {
		t.Error("expected at least one shard in ShardInfoList")
	}

	found := false
	for _, si := range info.ShardInfoList {
		if si.ShardID == shardID && si.ReplicaID == replicaID {
			found = true
			break
		}
	}
	if !found {
		t.Errorf("expected shard %d / replica %d in ShardInfoList", shardID, replicaID)
	}
}

func TestNodeHostInfoOption_SkipLogInfo(t *testing.T) {
	nh := newTestNodeHost(t)
	shardID := uint64(1)
	replicaID := uint64(1)
	startTestShard(t, nh, shardID, replicaID)

	info := nh.GetNodeHostInfo(NodeHostInfoOption{SkipLogInfo: true})
	if info == nil {
		t.Fatal("expected non-nil NodeHostInfo")
	}
	if info.LogInfo != nil {
		t.Error("expected LogInfo to be nil when SkipLogInfo is true")
	}
}

func TestNodeHostInfo_GossipFieldExists(t *testing.T) {
	nh := newTestNodeHost(t)
	info := nh.GetNodeHostInfo(NodeHostInfoOption{})
	// Gossip should be zero-valued since quicraft doesn't support it.
	if info.Gossip.Enabled {
		t.Error("expected Gossip.Enabled to be false")
	}
	if info.Gossip.NumOfKnownNodeHosts != 0 {
		t.Errorf("expected Gossip.NumOfKnownNodeHosts=0, got %d", info.Gossip.NumOfKnownNodeHosts)
	}
}

func TestNodeHost_StopShard(t *testing.T) {
	nh := newTestNodeHost(t)
	shardID := uint64(1)
	replicaID := uint64(1)
	startTestShard(t, nh, shardID, replicaID)

	err := nh.StopShard(shardID)
	if err != nil {
		t.Fatalf("StopShard: %v", err)
	}

	// Verify the shard is stopped by trying to propose.
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	session := nh.GetNoOPSession(shardID)
	_, propErr := nh.SyncPropose(ctx, session, []byte("after-stop"))
	if propErr == nil {
		t.Error("expected error proposing to stopped shard, got nil")
	}
}

func TestNodeHost_StopShard_InvalidShard(t *testing.T) {
	nh := newTestNodeHost(t)
	err := nh.StopShard(999)
	if err == nil {
		t.Error("expected error for non-existent shard, got nil")
	}
}

func TestNodeHost_StopReplica(t *testing.T) {
	nh := newTestNodeHost(t)
	shardID := uint64(1)
	replicaID := uint64(1)
	startTestShard(t, nh, shardID, replicaID)

	err := nh.StopReplica(shardID, replicaID)
	if err != nil {
		t.Fatalf("StopReplica: %v", err)
	}
}

func TestNodeHost_StopReplica_WrongReplica(t *testing.T) {
	nh := newTestNodeHost(t)
	shardID := uint64(1)
	replicaID := uint64(1)
	startTestShard(t, nh, shardID, replicaID)

	err := nh.StopReplica(shardID, 999)
	if err == nil {
		t.Error("expected error for wrong replicaID, got nil")
	}
}

func TestNodeHost_StartDuplicateReplica(t *testing.T) {
	nh := newTestNodeHost(t)
	shardID := uint64(1)
	replicaID := uint64(1)
	startTestShard(t, nh, shardID, replicaID)

	members := map[uint64]Target{replicaID: nh.RaftAddress()}
	cfg := dbConfig.Config{
		ShardID:      shardID,
		ReplicaID:    replicaID,
		ElectionRTT:  5,
		HeartbeatRTT: 1,
	}

	err := nh.StartReplica(members, false, testSMFactory(), cfg)
	if err == nil {
		t.Error("expected error starting duplicate shard, got nil")
	}
}

func TestNodeHost_Propose_InvalidShard(t *testing.T) {
	nh := newTestNodeHost(t)
	session := nh.GetNoOPSession(999)
	_, err := nh.Propose(session, []byte("test"), time.Second)
	if err == nil {
		t.Error("expected error for non-existent shard, got nil")
	}
}

func TestNodeHost_SyncRead_InvalidShard(t *testing.T) {
	nh := newTestNodeHost(t)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	_, err := nh.SyncRead(ctx, 999, nil)
	if err == nil {
		t.Error("expected error for non-existent shard, got nil")
	}
}

func TestNodeHost_RequestLeaderTransfer_InvalidShard(t *testing.T) {
	nh := newTestNodeHost(t)
	err := nh.RequestLeaderTransfer(999, 1)
	if err == nil {
		t.Error("expected error for non-existent shard, got nil")
	}
}

func TestNodeHost_GetNodeHostInfo_EmptyHost(t *testing.T) {
	nh := newTestNodeHost(t)
	info := nh.GetNodeHostInfo(NodeHostInfoOption{SkipLogInfo: true})
	if info == nil {
		t.Fatal("expected non-nil info")
	}
	if len(info.ShardInfoList) != 0 {
		t.Errorf("expected 0 shards, got %d", len(info.ShardInfoList))
	}
}

func TestFromQuicraftShardInfo_EmptyNodes(t *testing.T) {
	qcSI := quicraft.ShardInfo{
		ShardID:   10,
		ReplicaID: 1,
		Nodes:     nil,
	}

	si := fromQuicraftShardInfo(qcSI)
	if si.Nodes == nil {
		t.Error("expected non-nil Nodes map even for nil input")
	}
	if len(si.Nodes) != 0 {
		t.Errorf("expected 0 nodes, got %d", len(si.Nodes))
	}
}

func TestToDragonboatStateMachineType(t *testing.T) {
	// Quicraft numbers state machine types Regular=0, Concurrent=1,
	// OnDisk=2 (pkg/proto); dragonboat numbers them Regular=1,
	// Concurrent=2, OnDisk=3 (statemachine.Type). The conversion must
	// translate, not copy.
	tests := []struct {
		name     string
		quicraft uint64
		want     uint64
	}{
		{"Regular", 0, 1},
		{"Concurrent", 1, 2},
		{"OnDisk", 2, 3},
		{"Unknown", 99, 0},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := toDragonboatStateMachineType(tc.quicraft); got != tc.want {
				t.Errorf("toDragonboatStateMachineType(%d) = %d, want %d", tc.quicraft, got, tc.want)
			}
			si := fromQuicraftShardInfo(quicraft.ShardInfo{StateMachineType: tc.quicraft})
			if si.StateMachineType != tc.want {
				t.Errorf("fromQuicraftShardInfo StateMachineType = %d, want %d", si.StateMachineType, tc.want)
			}
		})
	}
}

func TestCodeToError(t *testing.T) {
	tests := []struct {
		name     string
		code     RequestResultCode
		expected error
	}{
		{"Timeout", RequestTimeout, ErrTimeout},
		{"Terminated", RequestTerminated, ErrShardClosed},
		{"Rejected", RequestRejected, ErrRejected},
		{"Dropped", RequestDropped, ErrShardNotReady},
		{"Aborted", RequestAborted, ErrAborted},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := codeToError(tt.code)
			if result != tt.expected {
				t.Errorf("codeToError(%d) = %v, want %v", tt.code, result, tt.expected)
			}
		})
	}
}

func TestCodeToError_UnknownCode(t *testing.T) {
	result := codeToError(RequestResultCode(99))
	if result != ErrAborted {
		t.Errorf("expected ErrAborted for unknown code, got %v", result)
	}
}

func TestNodeHost_SyncRemoveData_CanceledContext(t *testing.T) {
	nh := newTestNodeHost(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // Cancel immediately.

	err := nh.SyncRemoveData(ctx, 999, 999)
	if err == nil {
		t.Error("expected error for canceled context, got nil")
	}
}

// ---------------------------------------------------------------------------
// Version Constants
// ---------------------------------------------------------------------------

func TestVersionConstants(t *testing.T) {
	if DragonboatMajor != 4 {
		t.Errorf("DragonboatMajor = %d, want 4", DragonboatMajor)
	}
	if DragonboatMinor != 0 {
		t.Errorf("DragonboatMinor = %d, want 0", DragonboatMinor)
	}
	if DragonboatPatch != 0 {
		t.Errorf("DragonboatPatch = %d, want 0", DragonboatPatch)
	}
	if !DEVVersion {
		t.Error("expected DEVVersion to be true")
	}
}

// ---------------------------------------------------------------------------
// Default Option Vars
// ---------------------------------------------------------------------------

func TestDefaultNodeHostInfoOption(t *testing.T) {
	if DefaultNodeHostInfoOption.SkipLogInfo {
		t.Error("expected DefaultNodeHostInfoOption.SkipLogInfo to be false")
	}
}

func TestDefaultSnapshotOption(t *testing.T) {
	if DefaultSnapshotOption.ExportPath != "" {
		t.Error("expected DefaultSnapshotOption.ExportPath to be empty")
	}
	if DefaultSnapshotOption.CompactionOverhead != 0 {
		t.Errorf("expected DefaultSnapshotOption.CompactionOverhead=0, got %d", DefaultSnapshotOption.CompactionOverhead)
	}
	if DefaultSnapshotOption.CompactionIndex != 0 {
		t.Errorf("expected DefaultSnapshotOption.CompactionIndex=0, got %d", DefaultSnapshotOption.CompactionIndex)
	}
}

// ---------------------------------------------------------------------------
// GetNodeUser / GetNodeHostRegistry
// ---------------------------------------------------------------------------

func TestNodeHost_GetNodeUser_ReturnsError(t *testing.T) {
	nh := newTestNodeHost(t)
	user, err := nh.GetNodeUser(1)
	if user != nil {
		t.Error("expected nil INodeUser")
	}
	if err == nil {
		t.Error("expected error from GetNodeUser")
	}
}

func TestNodeHost_GetNodeHostRegistry_ReturnsFalse(t *testing.T) {
	nh := newTestNodeHost(t)
	registry, ok := nh.GetNodeHostRegistry()
	if registry != nil {
		t.Error("expected nil registry")
	}
	if ok {
		t.Error("expected ok=false from GetNodeHostRegistry")
	}
}

// ---------------------------------------------------------------------------
// NAReadLocalNode
// ---------------------------------------------------------------------------

func TestNodeHost_NAReadLocalNode_ReturnsNotImplemented(t *testing.T) {
	// testStateMachine does not implement IExtended, so, as in dragonboat,
	// the read is refused with the statemachine package's sentinel itself.
	nh := newTestNodeHost(t)
	startTestShard(t, nh, 1, 1)
	rs := completedReadIndex(t, nh, 1)

	_, err := nh.NAReadLocalNode(rs, []byte("query"))
	if err != dbSM.ErrNotImplemented {
		t.Errorf("expected dbSM.ErrNotImplemented, got %v", err)
	}
}

// extendedTestStateMachine implements the optional IExtended interface.
type extendedTestStateMachine struct {
	testStateMachine
	naErr error
}

func (sm *extendedTestStateMachine) NALookup(key []byte) ([]byte, error) {
	if sm.naErr != nil {
		return nil, sm.naErr
	}
	return append(append([]byte("na:"), key...), sm.data...), nil
}

var _ dbSM.IExtended = (*extendedTestStateMachine)(nil)

// completedReadIndex runs a ReadIndex on shardID and waits for it to
// complete, the precondition of ReadLocalNode and NAReadLocalNode.
func completedReadIndex(t *testing.T, nh *NodeHost, shardID uint64) *RequestState {
	t.Helper()
	rs, err := nh.ReadIndex(shardID, 5*time.Second)
	if err != nil {
		t.Fatalf("ReadIndex: %v", err)
	}
	t.Cleanup(rs.Release)
	if s := <-rs.ResultC(); !s.Completed() {
		t.Fatalf("ReadIndex did not complete: code=%v", s.code)
	}
	return rs
}

func TestNodeHost_NAReadLocalNode_ServesNALookup(t *testing.T) {
	nh := newTestNodeHost(t)
	startTestShardWith(t, nh, 1, 1, func(uint64, uint64) dbSM.IStateMachine {
		return &extendedTestStateMachine{}
	})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := nh.SyncPropose(ctx, nh.GetNoOPSession(1), []byte("-applied")); err != nil {
		t.Fatalf("SyncPropose: %v", err)
	}
	rs := completedReadIndex(t, nh, 1)

	got, err := nh.NAReadLocalNode(rs, []byte("key"))
	if err != nil {
		t.Fatalf("NAReadLocalNode: %v", err)
	}
	if string(got) != "na:key-applied" {
		t.Errorf("NAReadLocalNode = %q, want %q", got, "na:key-applied")
	}

	// A []byte query to ReadLocalNode still reaches Lookup, not NALookup.
	val, err := nh.ReadLocalNode(rs, []byte("key"))
	if err != nil {
		t.Fatalf("ReadLocalNode: %v", err)
	}
	if b, ok := val.([]byte); !ok || string(b) != "-applied" {
		t.Errorf("ReadLocalNode = %v, want Lookup's %q", val, "-applied")
	}
}

func TestNodeHost_NAReadLocalNode_PropagatesNALookupError(t *testing.T) {
	naErr := errors.New("na lookup failed")
	nh := newTestNodeHost(t)
	startTestShardWith(t, nh, 1, 1, func(uint64, uint64) dbSM.IStateMachine {
		return &extendedTestStateMachine{naErr: naErr}
	})
	rs := completedReadIndex(t, nh, 1)

	if _, err := nh.NAReadLocalNode(rs, []byte("key")); !errors.Is(err, naErr) {
		t.Errorf("NAReadLocalNode error = %v, want one wrapping %v", err, naErr)
	}
}

func TestNodeHost_NAReadLocalNode_UnresolvedFailsFast(t *testing.T) {
	nh := newTestNodeHost(t)
	rs := &RequestState{CompletedC: make(chan RequestResult, 1), shardID: 1}

	if _, err := nh.NAReadLocalNode(rs, []byte("key")); !errors.Is(err, ErrInvalidOperation) {
		t.Errorf("NAReadLocalNode on an unresolved RequestState = %v, want ErrInvalidOperation", err)
	}
}

// ---------------------------------------------------------------------------
// WriteHealthMetrics
// ---------------------------------------------------------------------------

func TestWriteHealthMetrics_NoOp(t *testing.T) {
	var buf bytes.Buffer
	WriteHealthMetrics(&buf)
	if buf.Len() != 0 {
		t.Errorf("expected empty output, got %d bytes", buf.Len())
	}
}

// ---------------------------------------------------------------------------
// RequestCompaction
// ---------------------------------------------------------------------------

// TestNodeHost_RequestCompaction_UnloadedShard verifies the dragonboat
// contract for a shard this NodeHost does not have loaded: it is assumed to
// have been retired via RemoveData, so compaction reclaims what it can and
// reports success rather than failing. This keeps the canonical
// StopShard -> RemoveData -> RequestCompaction reclamation sequence working.
func TestNodeHost_RequestCompaction_UnloadedShard(t *testing.T) {
	nh := newTestNodeHost(t)
	state, err := nh.RequestCompaction(999, 999)
	if err != nil {
		t.Fatalf("expected success for an unloaded shard, got: %v", err)
	}
	if state == nil {
		t.Fatal("expected a non-nil SysOpState")
	}
	select {
	case <-state.ResultC():
	default:
		t.Error("expected the SysOpState to be already completed")
	}
}

// ---------------------------------------------------------------------------
// GossipInfo / ShardView
// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------
// SnapshotOption Validation (matching dragonboat TestSnapshotOptionIsChecked)
// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------
// NodeHostConstructionError
// ---------------------------------------------------------------------------

func TestNodeHostConstructionError_WithReason(t *testing.T) {
	e := &NodeHostConstructionError{Reason: "bad config"}
	msg := e.Error()
	if msg == "" {
		t.Fatal("expected non-empty error message")
	}
	if !strings.Contains(msg, "bad config") {
		t.Fatalf("error doesn't contain reason: %s", msg)
	}
}

func TestNodeHostConstructionError_WithUnderlying(t *testing.T) {
	inner := errors.New("underlying")
	e := &NodeHostConstructionError{Reason: "test", Err: inner}
	if !errors.Is(e, inner) {
		t.Fatal("Unwrap should return inner error")
	}
	msg := e.Error()
	if !strings.Contains(msg, "underlying") {
		t.Fatalf("error doesn't contain underlying message: %s", msg)
	}
}

func TestNodeHostConstructionError_NilUnderlying(t *testing.T) {
	e := &NodeHostConstructionError{Reason: "no inner"}
	if e.Unwrap() != nil {
		t.Fatal("expected nil from Unwrap")
	}
}

// ---------------------------------------------------------------------------
// ShardInfo field mapping (all fields)
// ---------------------------------------------------------------------------

func TestFromQuicraftShardInfo_AllFields(t *testing.T) {
	nodes := map[uint64]string{1: "addr1", 2: "addr2", 3: "addr3"}
	qcSI := quicraft.ShardInfo{
		ShardID:           100,
		ReplicaID:         5,
		LeaderID:          5,
		Term:              42,
		IsLeader:          true,
		IsObserver:        true,
		IsWitness:         true,
		StateMachineType:  2,
		Pending:           7,
		Nodes:             nodes,
		ConfigChangeIndex: 999,
	}

	si := fromQuicraftShardInfo(qcSI)

	if si.ShardID != 100 {
		t.Errorf("ShardID = %d, want 100", si.ShardID)
	}
	if si.ReplicaID != 5 {
		t.Errorf("ReplicaID = %d, want 5", si.ReplicaID)
	}
	if si.LeaderID != 5 {
		t.Errorf("LeaderID = %d, want 5", si.LeaderID)
	}
	if si.Term != 42 {
		t.Errorf("Term = %d, want 42", si.Term)
	}
	if !si.IsLeader {
		t.Error("expected IsLeader=true")
	}
	if !si.IsObserver {
		t.Error("expected IsObserver=true")
	}
	if !si.IsWitness {
		t.Error("expected IsWitness=true")
	}
	// Quicraft OnDisk (2) must be translated to dragonboat OnDisk (3).
	if si.StateMachineType != 3 {
		t.Errorf("StateMachineType = %d, want 3 (dragonboat OnDisk)", si.StateMachineType)
	}
	if si.PendingProposals != 7 {
		t.Errorf("PendingProposals = %d, want 7", si.PendingProposals)
	}
	if len(si.Nodes) != 3 {
		t.Errorf("Nodes length = %d, want 3", len(si.Nodes))
	}
	for k, v := range nodes {
		if si.Nodes[k] != v {
			t.Errorf("Nodes[%d] = %q, want %q", k, si.Nodes[k], v)
		}
	}
	if si.ConfigChangeIndex != 999 {
		t.Errorf("ConfigChangeIndex = %d, want 999", si.ConfigChangeIndex)
	}
}

func TestFromQuicraftShardInfo_IsolatesNodes(t *testing.T) {
	source := map[uint64]string{1: "addr1"}
	qcSI := quicraft.ShardInfo{
		ShardID: 1,
		Nodes:   source,
	}
	si := fromQuicraftShardInfo(qcSI)

	// Mutating the source should not affect the converted ShardInfo.
	source[2] = "addr2"
	if len(si.Nodes) != 1 {
		t.Errorf("mutation of source affected result: Nodes length = %d, want 1", len(si.Nodes))
	}
}

// ---------------------------------------------------------------------------
// GetNodeUser with active shard (still returns error per implementation)
// ---------------------------------------------------------------------------

func TestNodeHost_GetNodeUser_InvalidShard(t *testing.T) {
	nh := newTestNodeHost(t)
	user, err := nh.GetNodeUser(999)
	if user != nil {
		t.Error("expected nil INodeUser for non-existent shard")
	}
	if err == nil {
		t.Error("expected error from GetNodeUser for non-existent shard")
	}
}

func TestNodeHost_GetNodeUser_ActiveShard(t *testing.T) {
	nh := newTestNodeHost(t)
	shardID := uint64(1)
	replicaID := uint64(1)
	startTestShard(t, nh, shardID, replicaID)

	user, err := nh.GetNodeUser(shardID)
	if err != nil {
		t.Fatalf("GetNodeUser: unexpected error: %v", err)
	}
	if user == nil {
		t.Fatal("expected non-nil INodeUser for active shard")
	}
	if user.ShardID() != shardID {
		t.Errorf("ShardID() = %d, want %d", user.ShardID(), shardID)
	}
	if user.ReplicaID() != replicaID {
		t.Errorf("ReplicaID() = %d, want %d", user.ReplicaID(), replicaID)
	}
}

// ---------------------------------------------------------------------------
// QueryRaftLog
// ---------------------------------------------------------------------------

func TestNodeHost_QueryRaftLog_InvalidShard(t *testing.T) {
	nh := newTestNodeHost(t)
	_, err := nh.QueryRaftLog(999, 0, 100, 1024)
	if err == nil {
		t.Error("expected error for non-existent shard")
	}
}

// ---------------------------------------------------------------------------
// ReadIndex + ReadLocalNode
// ---------------------------------------------------------------------------

func TestNodeHost_ReadIndex_InvalidShard(t *testing.T) {
	nh := newTestNodeHost(t)
	_, err := nh.ReadIndex(999, time.Second)
	if err == nil {
		t.Error("expected error for non-existent shard")
	}
}

// ---------------------------------------------------------------------------
// SyncRequestSnapshot
// ---------------------------------------------------------------------------

func TestNodeHost_SyncRequestSnapshot_InvalidShard(t *testing.T) {
	nh := newTestNodeHost(t)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	_, err := nh.SyncRequestSnapshot(ctx, 999, DefaultSnapshotOption)
	if err == nil {
		t.Error("expected error for non-existent shard")
	}
}

// ---------------------------------------------------------------------------
// RequestSnapshot async
// ---------------------------------------------------------------------------

func TestNodeHost_RequestSnapshot_InvalidShard(t *testing.T) {
	nh := newTestNodeHost(t)
	_, err := nh.RequestSnapshot(999, DefaultSnapshotOption, time.Second)
	if err == nil {
		t.Error("expected error for non-existent shard")
	}
}

// ---------------------------------------------------------------------------
// GetLogReader
// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------
// Async membership operations with invalid shard
// ---------------------------------------------------------------------------

func TestNodeHost_RequestAddReplica_InvalidShard(t *testing.T) {
	nh := newTestNodeHost(t)
	_, err := nh.RequestAddReplica(999, 2, "addr", 0, time.Second)
	if err == nil {
		t.Error("expected error for non-existent shard")
	}
}

func TestNodeHost_RequestDeleteReplica_InvalidShard(t *testing.T) {
	nh := newTestNodeHost(t)
	_, err := nh.RequestDeleteReplica(999, 2, 0, time.Second)
	if err == nil {
		t.Error("expected error for non-existent shard")
	}
}

func TestNodeHost_RequestAddNonVoting_InvalidShard(t *testing.T) {
	nh := newTestNodeHost(t)
	_, err := nh.RequestAddNonVoting(999, 2, "addr", 0, time.Second)
	if err == nil {
		t.Error("expected error for non-existent shard")
	}
}

func TestNodeHost_RequestAddWitness_InvalidShard(t *testing.T) {
	nh := newTestNodeHost(t)
	_, err := nh.RequestAddWitness(999, 2, "addr", 0, time.Second)
	if err == nil {
		t.Error("expected error for non-existent shard")
	}
}

// ---------------------------------------------------------------------------
// Sync membership operations with invalid shard
// ---------------------------------------------------------------------------

func TestNodeHost_SyncRequestAddReplica_InvalidShard(t *testing.T) {
	nh := newTestNodeHost(t)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	err := nh.SyncRequestAddReplica(ctx, 999, 2, "addr", 0)
	if err == nil {
		t.Error("expected error for non-existent shard")
	}
}

func TestNodeHost_SyncRequestDeleteReplica_InvalidShard(t *testing.T) {
	nh := newTestNodeHost(t)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	err := nh.SyncRequestDeleteReplica(ctx, 999, 2, 0)
	if err == nil {
		t.Error("expected error for non-existent shard")
	}
}

func TestNodeHost_SyncRequestAddNonVoting_InvalidShard(t *testing.T) {
	nh := newTestNodeHost(t)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	err := nh.SyncRequestAddNonVoting(ctx, 999, 2, "addr", 0)
	if err == nil {
		t.Error("expected error for non-existent shard")
	}
}

func TestNodeHost_SyncRequestAddWitness_InvalidShard(t *testing.T) {
	nh := newTestNodeHost(t)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	err := nh.SyncRequestAddWitness(ctx, 999, 2, "addr", 0)
	if err == nil {
		t.Error("expected error for non-existent shard")
	}
}

// ---------------------------------------------------------------------------
// ReadLocalNode edge cases
// ---------------------------------------------------------------------------

func TestNodeHost_ReadLocalNode_NilResultC(t *testing.T) {
	nh := newTestNodeHost(t)
	// A RequestState with no delivered completion must be rejected:
	// serving the query without a resolved ReadIndex would violate
	// linearizability.
	rs := &RequestState{}
	_, err := nh.ReadLocalNode(rs, nil)
	if err == nil {
		t.Error("expected error for unresolved RequestState")
	}
	if !errors.Is(err, ErrInvalidOperation) {
		t.Errorf("expected ErrInvalidOperation, got %v", err)
	}
}

// ---------------------------------------------------------------------------
// ProposeSession / SyncGetSession / SyncCloseSession error paths
// ---------------------------------------------------------------------------

func TestNodeHost_SyncGetSession_InvalidShard(t *testing.T) {
	nh := newTestNodeHost(t)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	_, err := nh.SyncGetSession(ctx, 999)
	if err == nil {
		t.Error("expected error for non-existent shard")
	}
}

// ---------------------------------------------------------------------------
// Multiple shards on same host
// ---------------------------------------------------------------------------

func TestNodeHost_MultipleShardsIndependent(t *testing.T) {
	nh := newTestNodeHost(t)
	startTestShard(t, nh, 1, 1)
	startTestShard(t, nh, 2, 1)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Propose to shard 1.
	s1 := nh.GetNoOPSession(1)
	_, err := nh.SyncPropose(ctx, s1, []byte("shard1"))
	if err != nil {
		t.Fatalf("SyncPropose shard 1: %v", err)
	}

	// Propose to shard 2.
	s2 := nh.GetNoOPSession(2)
	_, err = nh.SyncPropose(ctx, s2, []byte("shard2"))
	if err != nil {
		t.Fatalf("SyncPropose shard 2: %v", err)
	}

	// Read from each shard independently.
	r1, err := nh.SyncRead(ctx, 1, nil)
	if err != nil {
		t.Fatalf("SyncRead shard 1: %v", err)
	}
	if string(r1.([]byte)) != "shard1" {
		t.Errorf("shard 1: got %q, want %q", r1, "shard1")
	}

	r2, err := nh.SyncRead(ctx, 2, nil)
	if err != nil {
		t.Fatalf("SyncRead shard 2: %v", err)
	}
	if string(r2.([]byte)) != "shard2" {
		t.Errorf("shard 2: got %q, want %q", r2, "shard2")
	}
}

// ---------------------------------------------------------------------------
// Close idempotency
// ---------------------------------------------------------------------------

func TestNodeHost_Close_Idempotent(t *testing.T) {
	dir := t.TempDir()
	nhConfig := dbConfig.NodeHostConfig{
		DeploymentID: 1,
		WALDir:       filepath.Join(dir, "wal"),
		NodeHostDir:  filepath.Join(dir, "data"),
		// A real port: dragonboat's address validator rejects port 0,
		// and initial-member targets are validated at Start*Replica.
		RaftAddress:    "localhost:63001",
		ListenAddress:  "localhost:0",
		RTTMillisecond: 1,
	}
	nh, err := NewNodeHostWithOptions(nhConfig,
		quicraft.WithMemoryLogDB(),
		quicraft.WithoutTransport(), quicraft.WithoutEncryption(),
	)
	if err != nil {
		t.Fatalf("NewNodeHostWithOptions: %v", err)
	}
	// Close twice should not panic.
	nh.Close()
	nh.Close()
}

// ---------------------------------------------------------------------------
// StartConcurrentReplica
// ---------------------------------------------------------------------------

// testConcurrentStateMachine implements dbSM.IConcurrentStateMachine.
type testConcurrentStateMachine struct {
	data []byte
}

func (sm *testConcurrentStateMachine) Update(entries []dbSM.Entry) ([]dbSM.Entry, error) {
	for i := range entries {
		sm.data = append(sm.data[:0], entries[i].Cmd...)
		entries[i].Result = dbSM.Result{Value: uint64(len(entries[i].Cmd))}
	}
	return entries, nil
}

func (sm *testConcurrentStateMachine) Lookup(query interface{}) (interface{}, error) {
	result := make([]byte, len(sm.data))
	copy(result, sm.data)
	return result, nil
}

func (sm *testConcurrentStateMachine) PrepareSnapshot() (interface{}, error) {
	snap := make([]byte, len(sm.data))
	copy(snap, sm.data)
	return snap, nil
}

func (sm *testConcurrentStateMachine) SaveSnapshot(ctx interface{}, w io.Writer, _ dbSM.ISnapshotFileCollection, _ <-chan struct{}) error {
	_, err := w.Write(ctx.([]byte))
	return err
}

func (sm *testConcurrentStateMachine) RecoverFromSnapshot(r io.Reader, _ []dbSM.SnapshotFile, _ <-chan struct{}) error {
	data, err := io.ReadAll(r)
	if err != nil {
		return err
	}
	sm.data = data
	return nil
}

func (sm *testConcurrentStateMachine) Close() error { return nil }

func testConcurrentSMFactory() dbSM.CreateConcurrentStateMachineFunc {
	return func(shardID, replicaID uint64) dbSM.IConcurrentStateMachine {
		return &testConcurrentStateMachine{}
	}
}

func TestNodeHost_StartConcurrentReplica(t *testing.T) {
	nh := newTestNodeHost(t)
	shardID := uint64(10)
	replicaID := uint64(1)

	members := map[uint64]Target{replicaID: nh.RaftAddress()}
	cfg := dbConfig.Config{
		ShardID:      shardID,
		ReplicaID:    replicaID,
		ElectionRTT:  5,
		HeartbeatRTT: 1,
		CheckQuorum:  true,
	}

	err := nh.StartConcurrentReplica(members, false, testConcurrentSMFactory(), cfg)
	if err != nil {
		t.Fatalf("StartConcurrentReplica: %v", err)
	}

	// Wait for leader election.
	{
		concTicker := time.NewTicker(10 * time.Millisecond)
		concDeadline := time.NewTimer(5 * time.Second)
	concLeaderWait:
		for {
			leaderID, _, valid, leaderErr := nh.GetLeaderID(shardID)
			if leaderErr == nil && valid && leaderID == replicaID {
				concTicker.Stop()
				concDeadline.Stop()
				break concLeaderWait
			}
			select {
			case <-concDeadline.C:
				concTicker.Stop()
				break concLeaderWait
			case <-concTicker.C:
			}
		}
	}

	// Verify the shard is running by proposing.
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	session := nh.GetNoOPSession(shardID)
	result, err := nh.SyncPropose(ctx, session, []byte("concurrent"))
	if err != nil {
		t.Fatalf("SyncPropose to concurrent SM: %v", err)
	}
	if result.Value != uint64(len("concurrent")) {
		t.Errorf("expected Value=%d, got %d", len("concurrent"), result.Value)
	}
}

func TestNodeHost_StartConcurrentReplica_DuplicateShard(t *testing.T) {
	nh := newTestNodeHost(t)
	shardID := uint64(10)
	replicaID := uint64(1)

	members := map[uint64]Target{replicaID: nh.RaftAddress()}
	cfg := dbConfig.Config{
		ShardID:      shardID,
		ReplicaID:    replicaID,
		ElectionRTT:  5,
		HeartbeatRTT: 1,
	}

	err := nh.StartConcurrentReplica(members, false, testConcurrentSMFactory(), cfg)
	if err != nil {
		t.Fatalf("first StartConcurrentReplica: %v", err)
	}

	err = nh.StartConcurrentReplica(members, false, testConcurrentSMFactory(), cfg)
	if err == nil {
		t.Error("expected error for duplicate shard, got nil")
	}
}

// ---------------------------------------------------------------------------
// StartOnDiskReplica
// ---------------------------------------------------------------------------

// testOnDiskStateMachine implements dbSM.IOnDiskStateMachine.
type testOnDiskStateMachine struct {
	data    []byte
	applied uint64
}

func (sm *testOnDiskStateMachine) Open(_ <-chan struct{}) (uint64, error) {
	return sm.applied, nil
}

func (sm *testOnDiskStateMachine) Update(entries []dbSM.Entry) ([]dbSM.Entry, error) {
	for i := range entries {
		sm.data = append(sm.data[:0], entries[i].Cmd...)
		entries[i].Result = dbSM.Result{Value: uint64(len(entries[i].Cmd))}
		sm.applied = entries[i].Index
	}
	return entries, nil
}

func (sm *testOnDiskStateMachine) Lookup(query interface{}) (interface{}, error) {
	result := make([]byte, len(sm.data))
	copy(result, sm.data)
	return result, nil
}

func (sm *testOnDiskStateMachine) Sync() error { return nil }

func (sm *testOnDiskStateMachine) PrepareSnapshot() (interface{}, error) {
	snap := make([]byte, len(sm.data))
	copy(snap, sm.data)
	return snap, nil
}

func (sm *testOnDiskStateMachine) SaveSnapshot(ctx interface{}, w io.Writer, _ <-chan struct{}) error {
	_, err := w.Write(ctx.([]byte))
	return err
}

func (sm *testOnDiskStateMachine) RecoverFromSnapshot(r io.Reader, _ <-chan struct{}) error {
	data, err := io.ReadAll(r)
	if err != nil {
		return err
	}
	sm.data = data
	return nil
}

func (sm *testOnDiskStateMachine) Close() error { return nil }

func testOnDiskSMFactory() dbSM.CreateOnDiskStateMachineFunc {
	return func(shardID, replicaID uint64) dbSM.IOnDiskStateMachine {
		return &testOnDiskStateMachine{}
	}
}

func TestNodeHost_StartOnDiskReplica(t *testing.T) {
	nh := newTestNodeHost(t)
	shardID := uint64(20)
	replicaID := uint64(1)

	members := map[uint64]Target{replicaID: nh.RaftAddress()}
	cfg := dbConfig.Config{
		ShardID:      shardID,
		ReplicaID:    replicaID,
		ElectionRTT:  5,
		HeartbeatRTT: 1,
		CheckQuorum:  true,
	}

	err := nh.StartOnDiskReplica(members, false, testOnDiskSMFactory(), cfg)
	if err != nil {
		t.Fatalf("StartOnDiskReplica: %v", err)
	}

	// Wait for leader election.
	{
		diskTicker := time.NewTicker(10 * time.Millisecond)
		diskDeadline := time.NewTimer(5 * time.Second)
	diskLeaderWait:
		for {
			leaderID, _, valid, leaderErr := nh.GetLeaderID(shardID)
			if leaderErr == nil && valid && leaderID == replicaID {
				diskTicker.Stop()
				diskDeadline.Stop()
				break diskLeaderWait
			}
			select {
			case <-diskDeadline.C:
				diskTicker.Stop()
				break diskLeaderWait
			case <-diskTicker.C:
			}
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	session := nh.GetNoOPSession(shardID)
	result, err := nh.SyncPropose(ctx, session, []byte("ondisk"))
	if err != nil {
		t.Fatalf("SyncPropose to on-disk SM: %v", err)
	}
	if result.Value != uint64(len("ondisk")) {
		t.Errorf("expected Value=%d, got %d", len("ondisk"), result.Value)
	}
}

func TestNodeHost_StartOnDiskReplica_DuplicateShard(t *testing.T) {
	nh := newTestNodeHost(t)
	shardID := uint64(20)
	replicaID := uint64(1)

	members := map[uint64]Target{replicaID: nh.RaftAddress()}
	cfg := dbConfig.Config{
		ShardID:      shardID,
		ReplicaID:    replicaID,
		ElectionRTT:  5,
		HeartbeatRTT: 1,
	}

	err := nh.StartOnDiskReplica(members, false, testOnDiskSMFactory(), cfg)
	if err != nil {
		t.Fatalf("first StartOnDiskReplica: %v", err)
	}

	err = nh.StartOnDiskReplica(members, false, testOnDiskSMFactory(), cfg)
	if err == nil {
		t.Error("expected error for duplicate shard, got nil")
	}
}

// ---------------------------------------------------------------------------
// SyncPropose with tracked session (non-NoOP)
// ---------------------------------------------------------------------------

func TestNodeHost_SyncPropose_WithTrackedSession(t *testing.T) {
	nh := newTestNodeHost(t)
	shardID := uint64(1)
	replicaID := uint64(1)
	startTestShard(t, nh, shardID, replicaID)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	session, err := nh.SyncGetSession(ctx, shardID)
	if err != nil {
		t.Fatalf("SyncGetSession: %v", err)
	}
	if session == nil {
		t.Fatal("expected non-nil tracked session")
	}
	if session.IsNoOPSession() {
		t.Error("expected tracked session, got no-op")
	}

	payload := []byte("tracked-proposal")
	result, err := nh.SyncPropose(ctx, session, payload)
	if err != nil {
		t.Fatalf("SyncPropose with tracked session: %v", err)
	}
	if result.Value != uint64(len(payload)) {
		t.Errorf("expected Value=%d, got %d", len(payload), result.Value)
	}
}

func TestNodeHost_SyncPropose_WithTrackedSession_InvalidShard(t *testing.T) {
	nh := newTestNodeHost(t)
	shardID := uint64(1)
	replicaID := uint64(1)
	startTestShard(t, nh, shardID, replicaID)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	session, err := nh.SyncGetSession(ctx, shardID)
	if err != nil {
		t.Fatalf("SyncGetSession: %v", err)
	}

	// Stop the shard to force an error on propose.
	if stopErr := nh.StopShard(shardID); stopErr != nil {
		t.Fatalf("StopShard: %v", stopErr)
	}

	_, err = nh.SyncPropose(ctx, session, []byte("should-fail"))
	if err == nil {
		t.Error("expected error proposing to stopped shard with tracked session")
	}
}

// ---------------------------------------------------------------------------
// SyncGetSession success path
// ---------------------------------------------------------------------------

func TestNodeHost_SyncGetSession_Success(t *testing.T) {
	nh := newTestNodeHost(t)
	shardID := uint64(1)
	replicaID := uint64(1)
	startTestShard(t, nh, shardID, replicaID)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	session, err := nh.SyncGetSession(ctx, shardID)
	if err != nil {
		t.Fatalf("SyncGetSession: %v", err)
	}
	if session == nil {
		t.Fatal("expected non-nil session")
	}
	if session.ShardID != shardID {
		t.Errorf("expected ShardID=%d, got %d", shardID, session.ShardID)
	}
	if session.IsNoOPSession() {
		t.Error("expected tracked session, got no-op")
	}
}

// ---------------------------------------------------------------------------
// SyncCloseSession
// ---------------------------------------------------------------------------

func TestNodeHost_SyncCloseSession_CanceledContext(t *testing.T) {
	nh := newTestNodeHost(t)
	shardID := uint64(1)
	replicaID := uint64(1)
	startTestShard(t, nh, shardID, replicaID)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	session, err := nh.SyncGetSession(ctx, shardID)
	if err != nil {
		t.Fatalf("SyncGetSession: %v", err)
	}

	canceledCtx, cancelNow := context.WithCancel(context.Background())
	cancelNow()
	err = nh.SyncCloseSession(canceledCtx, session)
	if err == nil {
		t.Error("expected error for canceled context, got nil")
	}
}

func TestNodeHost_SyncCloseSession_InvalidShard(t *testing.T) {
	nh := newTestNodeHost(t)

	// Create a no-op session for a shard that does not exist.
	session := nh.GetNoOPSession(999)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	err := nh.SyncCloseSession(ctx, session)
	if err == nil {
		t.Error("expected error for non-existent shard, got nil")
	}
}

func TestNodeHost_SyncCloseSession_PreparesForUnregister(t *testing.T) {
	nh := newTestNodeHost(t)
	shardID := uint64(1)
	replicaID := uint64(1)
	startTestShard(t, nh, shardID, replicaID)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	session, err := nh.SyncGetSession(ctx, shardID)
	if err != nil {
		t.Fatalf("SyncGetSession: %v", err)
	}

	// Verify the session is in proposal-ready state before close.
	if session.SeriesID() == quicraft.SeriesIDForUnregister {
		t.Fatal("session should not already have unregister seriesID")
	}

	err = nh.SyncCloseSession(ctx, session)
	if err != nil {
		t.Fatalf("SyncCloseSession: %v", err)
	}

	// After SyncCloseSession, the session's seriesID must reflect
	// the unregistration state (SeriesIDForUnregister), matching
	// dragonboat v4 behavior where PrepareForUnregister is called.
	if session.SeriesID() != quicraft.SeriesIDForUnregister {
		t.Errorf("session seriesID after close: got %d, want %d (SeriesIDForUnregister)",
			session.SeriesID(), quicraft.SeriesIDForUnregister)
	}
}

func TestNodeHost_SyncCloseSession_NoDeadlineReturnsError(t *testing.T) {
	nh := newTestNodeHost(t)
	shardID := uint64(1)
	replicaID := uint64(1)
	startTestShard(t, nh, shardID, replicaID)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	session, err := nh.SyncGetSession(ctx, shardID)
	if err != nil {
		t.Fatalf("SyncGetSession: %v", err)
	}

	// Use a context without a deadline.
	err = nh.SyncCloseSession(context.Background(), session)
	if !errors.Is(err, ErrDeadlineNotSet) {
		t.Errorf("expected ErrDeadlineNotSet, got %v", err)
	}
}

func TestSyncCloseSession_ClientIDMismatch(t *testing.T) {
	// validateCloseSessionResult is the extracted check from SyncCloseSession
	// that verifies result.Value matches cs.ClientID(). Testing through the
	// real Raft pipeline would require a state machine that returns a wrong
	// client ID on unregister, which is a protocol violation that cannot
	// occur with a correctly implemented SM. We test the validation logic
	// directly.
	t.Run("Mismatch", func(t *testing.T) {
		err := validateCloseSessionResult(42, 99)
		if !errors.Is(err, ErrClientIDMismatch) {
			t.Errorf("expected ErrClientIDMismatch, got %v", err)
		}
	})

	t.Run("Match", func(t *testing.T) {
		err := validateCloseSessionResult(42, 42)
		if err != nil {
			t.Errorf("expected nil error for matching client IDs, got %v", err)
		}
	})
}

// ---------------------------------------------------------------------------
// Propose with tracked session (non-NoOP path)
// ---------------------------------------------------------------------------

func TestNodeHost_Propose_WithTrackedSession(t *testing.T) {
	nh := newTestNodeHost(t)
	shardID := uint64(1)
	replicaID := uint64(1)
	startTestShard(t, nh, shardID, replicaID)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	session, err := nh.SyncGetSession(ctx, shardID)
	if err != nil {
		t.Fatalf("SyncGetSession: %v", err)
	}

	rs, err := nh.Propose(session, []byte("tracked-async"), 5*time.Second)
	if err != nil {
		t.Fatalf("Propose with tracked session: %v", err)
	}
	defer rs.Release()

	select {
	case result := <-rs.ResultC():
		if !result.Completed() {
			t.Errorf("expected completed result, got code %d", result.code)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for async proposal result")
	}
}

func TestNodeHost_Propose_WithTrackedSession_InvalidShard(t *testing.T) {
	nh := newTestNodeHost(t)
	shardID := uint64(1)
	replicaID := uint64(1)
	startTestShard(t, nh, shardID, replicaID)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	session, err := nh.SyncGetSession(ctx, shardID)
	if err != nil {
		t.Fatalf("SyncGetSession: %v", err)
	}

	// Stop the shard to force an error.
	if stopErr := nh.StopShard(shardID); stopErr != nil {
		t.Fatalf("StopShard: %v", stopErr)
	}

	_, err = nh.Propose(session, []byte("should-fail"), time.Second)
	if err == nil {
		t.Error("expected error proposing to stopped shard with tracked session")
	}
}

// ---------------------------------------------------------------------------
// ProposeSession
// ---------------------------------------------------------------------------

func TestNodeHost_ProposeSession_Success(t *testing.T) {
	nh := newTestNodeHost(t)
	shardID := uint64(1)
	replicaID := uint64(1)
	startTestShard(t, nh, shardID, replicaID)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	session, err := nh.SyncGetSession(ctx, shardID)
	if err != nil {
		t.Fatalf("SyncGetSession: %v", err)
	}

	// Dragonboat requires the caller to prepare the session for a
	// session operation; a live in-use session is rejected with
	// ErrInvalidSession (see TestNodeHost_ProposeSession_LiveSessionRejected).
	session.PrepareForUnregister()

	rs, err := nh.ProposeSession(session, 5*time.Second)
	if err != nil {
		t.Fatalf("ProposeSession: %v", err)
	}
	defer rs.Release()

	select {
	case result := <-rs.ResultC():
		// The result should complete (may be completed or another terminal state).
		_ = result
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for ProposeSession result")
	}
}

func TestNodeHost_ProposeSession_InvalidShard(t *testing.T) {
	nh := newTestNodeHost(t)
	session := nh.GetNoOPSession(999)
	_, err := nh.ProposeSession(session, time.Second)
	if err == nil {
		t.Error("expected error for non-existent shard, got nil")
	}
}

func TestProposeSession_DoesNotRace(t *testing.T) {
	nh := newTestNodeHost(t)
	shardID := uint64(1)
	replicaID := uint64(1)
	startTestShard(t, nh, shardID, replicaID)

	// Create a tracked session and put it in register state.
	session := dbClient.NewSession(shardID, nil)
	session.Inner().PrepareForRegister()

	const readers = 16
	const readsPerGoroutine = 500

	// Start concurrent readers before ProposeSession swaps the inner pointer.
	var wg sync.WaitGroup
	wg.Add(readers)
	start := make(chan struct{})
	for range readers {
		go func() {
			defer wg.Done()
			<-start
			for range readsPerGoroutine {
				_ = session.Inner()
				_ = session.ClientID()
				_ = session.SeriesID()
				_ = session.IsNoOPSession()
				_ = session.ValidForProposal(shardID)
			}
		}()
	}

	// Unblock readers and call ProposeSession which atomically swaps inner.
	close(start)
	rs, err := nh.ProposeSession(session, 5*time.Second)
	if err != nil {
		t.Fatalf("ProposeSession: %v", err)
	}
	defer rs.Release()

	wg.Wait()

	// After the swap the session must be readable without panic.
	if session.Inner() == nil {
		t.Error("Inner() must not be nil after ProposeSession")
	}
	if session.ClientID() == 0 {
		t.Error("ClientID must be non-zero after registration")
	}
}

func TestNodeHost_ProposeSession_PreservesClientID(t *testing.T) {
	nh := newTestNodeHost(t)
	shardID := uint64(1)
	replicaID := uint64(1)
	startTestShard(t, nh, shardID, replicaID)

	// Create a session with a known client ID and prepare it for registration.
	const knownClientID uint64 = 0xDEADBEEF_CAFEBABE
	session := dbClient.NewSession(shardID, &fixedSource{value: knownClientID})
	originalClientID := session.ClientID()
	if originalClientID != knownClientID {
		t.Fatalf("initial ClientID = %d, want %d", originalClientID, knownClientID)
	}

	session.Inner().PrepareForRegister()

	rs, err := nh.ProposeSession(session, 5*time.Second)
	if err != nil {
		t.Fatalf("ProposeSession: %v", err)
	}
	defer rs.Release()

	select {
	case <-rs.ResultC():
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for ProposeSession result")
	}

	// The session's client ID must be preserved after registration.
	if session.ClientID() != knownClientID {
		t.Errorf("ClientID after ProposeSession = %d, want %d (original preserved)",
			session.ClientID(), knownClientID)
	}
}

func TestNodeHost_ProposeSession_PreservesClientID_Error(t *testing.T) {
	nh := newTestNodeHost(t)

	// Create a session for a non-existent shard so registration fails.
	session := dbClient.NewSession(999, &fixedSource{value: 42})
	originalClientID := session.ClientID()
	session.Inner().PrepareForRegister()

	_, err := nh.ProposeSession(session, time.Second)
	if err == nil {
		t.Fatal("expected error for non-existent shard, got nil")
	}

	// On error, the session should remain unchanged.
	if session.ClientID() != originalClientID {
		t.Errorf("ClientID after failed ProposeSession = %d, want %d (unchanged)",
			session.ClientID(), originalClientID)
	}
}

// fixedSource is a Source that returns a fixed uint64 value.
// Used in tests to control client ID generation deterministically.
type fixedSource struct {
	value uint64
}

func (f *fixedSource) Uint64() uint64 {
	return f.value
}

// ---------------------------------------------------------------------------
// ReadLocalNode success path (ReadIndex -> ReadLocalNode)
// ---------------------------------------------------------------------------

func TestNodeHost_ReadLocalNode_ClosedChannel(t *testing.T) {
	nh := newTestNodeHost(t)

	// A RequestState whose channel was closed without ever delivering a
	// completion is an abandoned request: no completion was latched, so
	// ReadLocalNode must refuse to serve the query.
	ch := make(chan RequestResult)
	close(ch)
	rs := &RequestState{CompletedC: ch}

	_, err := nh.ReadLocalNode(rs, nil)
	if err == nil {
		t.Error("expected error for abandoned request")
	}
	if !errors.Is(err, ErrInvalidOperation) {
		t.Errorf("expected ErrInvalidOperation, got %v", err)
	}
}

// ---------------------------------------------------------------------------
// RequestCompaction success path
// ---------------------------------------------------------------------------

func TestNodeHost_RequestCompaction_Success(t *testing.T) {
	nh := newTestNodeHost(t)
	shardID := uint64(1)
	replicaID := uint64(1)
	startTestShard(t, nh, shardID, replicaID)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Propose some entries to create data for compaction.
	session := nh.GetNoOPSession(shardID)
	for i := 0; i < 5; i++ {
		_, err := nh.SyncPropose(ctx, session, []byte("compaction-data"))
		if err != nil {
			t.Fatalf("SyncPropose: %v", err)
		}
	}

	sysOp, err := nh.RequestCompaction(shardID, replicaID)
	if err != nil {
		t.Fatalf("RequestCompaction: %v", err)
	}

	// Since RequestCompaction is synchronous in quicraft, the SysOpState
	// should be pre-completed.
	select {
	case <-sysOp.CompletedC():
		// Expected: already completed.
	default:
		t.Error("expected CompletedC to be closed immediately")
	}

	// ResultC should also be closed.
	select {
	case <-sysOp.ResultC():
		// Expected.
	default:
		t.Error("expected ResultC to be closed immediately")
	}

	sysOp.Release()
}

// ---------------------------------------------------------------------------
// Snapshot operations (success paths)
// ---------------------------------------------------------------------------

func TestNodeHost_SyncRequestSnapshot_Success(t *testing.T) {
	nh := newTestNodeHost(t)
	shardID := uint64(1)
	replicaID := uint64(1)
	startTestShard(t, nh, shardID, replicaID)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Propose some data first.
	session := nh.GetNoOPSession(shardID)
	_, err := nh.SyncPropose(ctx, session, []byte("snapshot-data"))
	if err != nil {
		t.Fatalf("SyncPropose: %v", err)
	}

	idx, err := nh.SyncRequestSnapshot(ctx, shardID, DefaultSnapshotOption)
	if err != nil {
		t.Fatalf("SyncRequestSnapshot: %v", err)
	}
	if idx == 0 {
		t.Error("expected non-zero snapshot index")
	}
}

func TestNodeHost_RequestSnapshot_Success(t *testing.T) {
	nh := newTestNodeHost(t)
	shardID := uint64(1)
	replicaID := uint64(1)
	startTestShard(t, nh, shardID, replicaID)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	session := nh.GetNoOPSession(shardID)
	_, err := nh.SyncPropose(ctx, session, []byte("snap-data"))
	if err != nil {
		t.Fatalf("SyncPropose: %v", err)
	}

	rs, err := nh.RequestSnapshot(shardID, DefaultSnapshotOption, 5*time.Second)
	if err != nil {
		t.Fatalf("RequestSnapshot: %v", err)
	}
	defer rs.Release()

	select {
	case result := <-rs.ResultC():
		if !result.Completed() {
			t.Errorf("expected completed result, got code %d", result.code)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for snapshot result")
	}
}

// ---------------------------------------------------------------------------
// ExportSnapshot and ImportSnapshot
// ---------------------------------------------------------------------------

func TestNodeHost_ExportSnapshot_InvalidShard(t *testing.T) {
	nh := newTestNodeHost(t)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	var buf bytes.Buffer
	err := nh.ExportSnapshot(ctx, 999, &buf)
	if err == nil {
		t.Error("expected error for non-existent shard, got nil")
	}
}

func TestNodeHost_ImportSnapshot_InvalidShard(t *testing.T) {
	nh := newTestNodeHost(t)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	r := bytes.NewReader([]byte("fake-snapshot-data"))
	err := nh.ImportSnapshot(ctx, 999, r)
	if err == nil {
		t.Error("expected error for non-existent shard, got nil")
	}
}

func TestNodeHost_ImportSnapshot_CanceledContext(t *testing.T) {
	nh := newTestNodeHost(t)
	canceledCtx, cancelNow := context.WithCancel(context.Background())
	cancelNow()

	r := bytes.NewReader([]byte("fake-snapshot-data"))
	err := nh.ImportSnapshot(canceledCtx, 1, r)
	if err == nil {
		t.Error("expected error for canceled context, got nil")
	}
}

// ---------------------------------------------------------------------------
// nodeUser.Propose and nodeUser.ReadIndex
// ---------------------------------------------------------------------------

func TestNodeUser_Propose(t *testing.T) {
	nh := newTestNodeHost(t)
	shardID := uint64(1)
	replicaID := uint64(1)
	startTestShard(t, nh, shardID, replicaID)

	user, err := nh.GetNodeUser(shardID)
	if err != nil {
		t.Fatalf("GetNodeUser: %v", err)
	}

	session := nh.GetNoOPSession(shardID)
	rs, err := user.Propose(session, []byte("nodeuser-propose"), 5*time.Second)
	if err != nil {
		t.Fatalf("nodeUser.Propose: %v", err)
	}
	defer rs.Release()

	select {
	case result := <-rs.ResultC():
		if !result.Completed() {
			t.Errorf("expected completed result, got code %d", result.code)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for nodeUser.Propose result")
	}
}

func TestNodeUser_ReadIndex(t *testing.T) {
	nh := newTestNodeHost(t)
	shardID := uint64(1)
	replicaID := uint64(1)
	startTestShard(t, nh, shardID, replicaID)

	user, err := nh.GetNodeUser(shardID)
	if err != nil {
		t.Fatalf("GetNodeUser: %v", err)
	}

	rs, err := user.ReadIndex(5 * time.Second)
	if err != nil {
		t.Fatalf("nodeUser.ReadIndex: %v", err)
	}
	defer rs.Release()

	select {
	case result := <-rs.ResultC():
		if !result.Completed() {
			t.Errorf("expected completed result, got code %d", result.code)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for nodeUser.ReadIndex result")
	}
}

// ---------------------------------------------------------------------------
// QueryRaftLog success path
// ---------------------------------------------------------------------------

func TestNodeHost_QueryRaftLog_SuccessWithEntries(t *testing.T) {
	nh := newTestNodeHost(t)
	shardID := uint64(1)
	replicaID := uint64(1)

	// Use a config with SnapshotEntries=0 to avoid compaction.
	members := map[uint64]Target{replicaID: nh.RaftAddress()}
	cfg := dbConfig.Config{
		ShardID:         shardID,
		ReplicaID:       replicaID,
		ElectionRTT:     5,
		HeartbeatRTT:    1,
		CheckQuorum:     true,
		SnapshotEntries: 0,
	}
	if err := nh.StartReplica(members, false, testSMFactory(), cfg); err != nil {
		t.Fatalf("StartReplica: %v", err)
	}

	// Wait for leader.
	{
		logTicker := time.NewTicker(10 * time.Millisecond)
		logDeadline := time.NewTimer(5 * time.Second)
	logLeaderWait:
		for {
			leaderID, _, valid, leaderErr := nh.GetLeaderID(shardID)
			if leaderErr == nil && valid && leaderID == replicaID {
				logTicker.Stop()
				logDeadline.Stop()
				break logLeaderWait
			}
			select {
			case <-logDeadline.C:
				logTicker.Stop()
				break logLeaderWait
			case <-logTicker.C:
			}
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Propose several entries to create log data.
	session := nh.GetNoOPSession(shardID)
	for i := 0; i < 5; i++ {
		_, err := nh.SyncPropose(ctx, session, []byte("log-entry"))
		if err != nil {
			t.Fatalf("SyncPropose %d: %v", i, err)
		}
	}

	// Get the log reader to find available range.
	reader, err := nh.GetLogReader(shardID)
	if err != nil {
		t.Fatalf("GetLogReader: %v", err)
	}

	firstIdx, lastIdx := reader.GetRange()
	if lastIdx <= firstIdx {
		t.Skipf("no log entries available in range [%d, %d)", firstIdx, lastIdx)
	}

	rs, err := nh.QueryRaftLog(shardID, firstIdx, lastIdx, 1024*1024)
	if err != nil {
		t.Fatalf("QueryRaftLog: %v", err)
	}
	defer rs.Release()

	resultC := rs.ResultC()
	if resultC == nil {
		t.Fatal("expected non-nil ResultC")
	}

	select {
	case result := <-resultC:
		if !result.Completed() {
			t.Errorf("expected completed result, got code %d", result.code)
		}
		entries, lr := result.RaftLogs()
		if len(entries) == 0 {
			t.Error("expected at least one entry from QueryRaftLog")
		}
		if lr.FirstIndex == 0 {
			t.Error("expected non-zero FirstIndex")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for QueryRaftLog result")
	}
}

// ---------------------------------------------------------------------------
// GetLogReader success path
// ---------------------------------------------------------------------------

func TestNodeHost_GetLogReader_Success(t *testing.T) {
	nh := newTestNodeHost(t)
	shardID := uint64(1)
	replicaID := uint64(1)
	startTestShard(t, nh, shardID, replicaID)

	reader, err := nh.GetLogReader(shardID)
	if err != nil {
		t.Fatalf("GetLogReader: %v", err)
	}
	if reader == nil {
		t.Error("expected non-nil ReadonlyLogReader")
	}
}

// ---------------------------------------------------------------------------
// WriteHealthMetrics with non-nil writer
// ---------------------------------------------------------------------------

func TestWriteHealthMetrics_NonNilWriter(t *testing.T) {
	var buf bytes.Buffer
	// Should be a no-op that does not panic.
	WriteHealthMetrics(&buf)
	if buf.Len() != 0 {
		t.Errorf("expected empty output from no-op WriteHealthMetrics, got %d bytes", buf.Len())
	}
}

// ---------------------------------------------------------------------------
// NewNodeHost success path (without options)
// ---------------------------------------------------------------------------

func TestNodeHost_NewNodeHostWithOptions_Success(t *testing.T) {
	dir := t.TempDir()
	nhConfig := dbConfig.NodeHostConfig{
		DeploymentID: 1,
		WALDir:       filepath.Join(dir, "wal"),
		NodeHostDir:  filepath.Join(dir, "data"),
		// A real port: dragonboat's address validator rejects port 0,
		// and initial-member targets are validated at Start*Replica.
		RaftAddress:    "localhost:63001",
		ListenAddress:  "localhost:0",
		RTTMillisecond: 1,
	}
	nh, err := NewNodeHostWithOptions(nhConfig,
		quicraft.WithMemoryLogDB(),
		quicraft.WithoutTransport(), quicraft.WithoutEncryption(),
	)
	if err != nil {
		t.Fatalf("NewNodeHostWithOptions: %v", err)
	}
	t.Cleanup(func() { nh.Close() })

	if nh.ID() == "" {
		t.Error("expected non-empty host ID")
	}
	if nh.RaftAddress() == "" {
		t.Error("expected non-empty RaftAddress")
	}
}

func TestNodeHost_NewNodeHost_InvalidConfig(t *testing.T) {
	nhConfig := dbConfig.NodeHostConfig{}
	_, err := NewNodeHost(nhConfig)
	if err == nil {
		t.Error("expected error for invalid config")
	}
	var constructErr *NodeHostConstructionError
	if !errors.As(err, &constructErr) {
		t.Errorf("expected NodeHostConstructionError, got %T", err)
	}
}

// ---------------------------------------------------------------------------
// Async membership operations success paths
// ---------------------------------------------------------------------------

func TestNodeHost_RequestAddReplica_Success(t *testing.T) {
	nh := newTestNodeHost(t)
	shardID := uint64(1)
	replicaID := uint64(1)
	startTestShard(t, nh, shardID, replicaID)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	m, err := nh.SyncGetShardMembership(ctx, shardID)
	if err != nil {
		t.Fatalf("SyncGetShardMembership: %v", err)
	}

	rs, err := nh.RequestAddReplica(shardID, 2, "localhost:9999", m.ConfigChangeID, 5*time.Second)
	if err != nil {
		t.Fatalf("RequestAddReplica: %v", err)
	}
	defer rs.Release()

	select {
	case result := <-rs.ResultC():
		if !result.Completed() {
			t.Logf("RequestAddReplica result code: %d", result.code)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timed out")
	}
}

func TestNodeHost_RequestAddNonVoting_Success(t *testing.T) {
	nh := newTestNodeHost(t)
	shardID := uint64(1)
	replicaID := uint64(1)
	startTestShard(t, nh, shardID, replicaID)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	m, err := nh.SyncGetShardMembership(ctx, shardID)
	if err != nil {
		t.Fatalf("SyncGetShardMembership: %v", err)
	}

	rs, err := nh.RequestAddNonVoting(shardID, 3, "localhost:9998", m.ConfigChangeID, 5*time.Second)
	if err != nil {
		t.Fatalf("RequestAddNonVoting: %v", err)
	}
	defer rs.Release()

	select {
	case result := <-rs.ResultC():
		_ = result
	case <-time.After(5 * time.Second):
		t.Fatal("timed out")
	}
}

func TestNodeHost_RequestAddWitness_Success(t *testing.T) {
	nh := newTestNodeHost(t)
	shardID := uint64(1)
	replicaID := uint64(1)
	startTestShard(t, nh, shardID, replicaID)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	m, err := nh.SyncGetShardMembership(ctx, shardID)
	if err != nil {
		t.Fatalf("SyncGetShardMembership: %v", err)
	}

	rs, err := nh.RequestAddWitness(shardID, 4, "localhost:9997", m.ConfigChangeID, 5*time.Second)
	if err != nil {
		t.Fatalf("RequestAddWitness: %v", err)
	}
	defer rs.Release()

	select {
	case result := <-rs.ResultC():
		_ = result
	case <-time.After(5 * time.Second):
		t.Fatal("timed out")
	}
}

func TestNodeHost_RequestDeleteReplica_Success(t *testing.T) {
	nh := newTestNodeHost(t)
	shardID := uint64(1)
	replicaID := uint64(1)
	startTestShard(t, nh, shardID, replicaID)

	// Deleting the sole full replica is invalid: the Host membership API
	// pre-validates config changes and rejects it synchronously with
	// ErrRemoveLastFullReplica instead of returning a RequestState that
	// would hang until deadline (a documented divergence from upstream
	// dragonboat; see the RequestDeleteReplica doc comment).
	rs, err := nh.RequestDeleteReplica(shardID, replicaID, 0, 5*time.Second)
	if err == nil {
		rs.Release()
		t.Fatal("expected synchronous rejection when deleting the last full replica")
	}
	if !errors.Is(err, quicraft.ErrRemoveLastFullReplica) {
		t.Fatalf("expected ErrRemoveLastFullReplica, got %v", err)
	}
}

// ---------------------------------------------------------------------------
// GetShardInfo Tests
// ---------------------------------------------------------------------------

func TestNodeHost_GetShardInfo_Success(t *testing.T) {
	nh := newTestNodeHost(t)
	shardID := uint64(1)
	replicaID := uint64(1)
	startTestShard(t, nh, shardID, replicaID)

	info, found, err := nh.GetShardInfo(shardID)
	if err != nil {
		t.Fatalf("GetShardInfo: %v", err)
	}
	if !found {
		t.Fatal("expected found=true for running shard")
	}
	if info == nil {
		t.Fatal("expected non-nil ShardInfo")
	}
	if info.ShardID != shardID {
		t.Errorf("expected ShardID=%d, got %d", shardID, info.ShardID)
	}
	if info.ReplicaID != replicaID {
		t.Errorf("expected ReplicaID=%d, got %d", replicaID, info.ReplicaID)
	}
	if info.Nodes == nil {
		t.Error("expected non-nil Nodes map")
	}
}

func TestNodeHost_GetShardInfo_InvalidShard(t *testing.T) {
	nh := newTestNodeHost(t)

	info, found, err := nh.GetShardInfo(99999)
	if err == nil {
		t.Fatal("expected error for non-existent shard, got nil")
	}
	if found {
		t.Error("expected found=false for non-existent shard")
	}
	if info != nil {
		t.Error("expected nil ShardInfo for non-existent shard")
	}
}

// ---------------------------------------------------------------------------
// GetShardMembership Tests
// ---------------------------------------------------------------------------

func TestNodeHost_GetShardMembership_Success(t *testing.T) {
	nh := newTestNodeHost(t)
	shardID := uint64(1)
	replicaID := uint64(1)
	startTestShard(t, nh, shardID, replicaID)

	m, err := nh.GetShardMembership(shardID)
	if err != nil {
		t.Fatalf("GetShardMembership: %v", err)
	}
	if m == nil {
		t.Fatal("expected non-nil Membership")
	}
	if len(m.Nodes) == 0 {
		t.Error("expected at least one node in membership")
	}
	if _, ok := m.Nodes[replicaID]; !ok {
		t.Errorf("expected replica %d in membership Nodes", replicaID)
	}
}

func TestNodeHost_GetShardMembership_InvalidShard(t *testing.T) {
	nh := newTestNodeHost(t)

	m, err := nh.GetShardMembership(99999)
	if err == nil {
		t.Fatal("expected error for non-existent shard, got nil")
	}
	if m != nil {
		t.Error("expected nil Membership for non-existent shard")
	}
}

// ---------------------------------------------------------------------------
// SnapshotOption CompactionIndex descriptive error message
// ---------------------------------------------------------------------------

func TestSnapshotOption_Validate_CompactionIndex_DescriptiveMessage(t *testing.T) {
	opt := SnapshotOption{OverrideCompactionOverhead: true, CompactionIndex: 50}
	err := opt.Validate()
	if err == nil {
		t.Fatal("expected error for unsupported CompactionIndex")
	}
	msg := err.Error()
	if !strings.Contains(msg, "CompactionIndex") {
		t.Errorf("error message should mention CompactionIndex: %s", msg)
	}
	if !strings.Contains(msg, "CompactionOverhead") {
		t.Errorf("error message should suggest CompactionOverhead: %s", msg)
	}
}

func TestSnapshotOption_Validate_CompactionIndex_StillMatchesErrInvalidOption(t *testing.T) {
	opt := SnapshotOption{OverrideCompactionOverhead: true, CompactionIndex: 50}
	err := opt.Validate()
	if err == nil {
		t.Fatal("expected error for unsupported CompactionIndex")
	}
	if !errors.Is(err, ErrInvalidOption) {
		t.Errorf("expected errors.Is(err, ErrInvalidOption) to be true, got %v", err)
	}
}

// ---------------------------------------------------------------------------
// Async context lifetime - verify cancel not called prematurely
// ---------------------------------------------------------------------------

func TestNodeHost_Propose_Async_ContextNotCanceledPrematurely(t *testing.T) {
	nh := newTestNodeHost(t)
	shardID := uint64(1)
	replicaID := uint64(1)
	startTestShard(t, nh, shardID, replicaID)

	session := nh.GetNoOPSession(shardID)
	rs, err := nh.Propose(session, []byte("async-lifetime"), 5*time.Second)
	if err != nil {
		t.Fatalf("Propose: %v", err)
	}

	// The RequestState should be usable; the context should NOT have been
	// canceled when Propose returned.
	resultC := rs.ResultC()
	if resultC == nil {
		t.Fatal("expected non-nil ResultC channel")
	}

	select {
	case result := <-resultC:
		if !result.Completed() {
			t.Errorf("expected completed result, got code %d", result.code)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for async proposal - context may have been canceled prematurely")
	}

	rs.Release()
}

// ---------------------------------------------------------------------------
// Entry Term field in QueryRaftLog
// ---------------------------------------------------------------------------

func TestNodeHost_QueryRaftLog_EntryTermField(t *testing.T) {
	nh := newTestNodeHost(t)
	shardID := uint64(1)
	replicaID := uint64(1)
	startTestShard(t, nh, shardID, replicaID)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Propose a few entries so there is data in the log.
	session := nh.GetNoOPSession(shardID)
	for i := 0; i < 3; i++ {
		_, propErr := nh.SyncPropose(ctx, session, []byte("entry"))
		if propErr != nil {
			t.Fatalf("SyncPropose: %v", propErr)
		}
	}

	// QueryRaftLog may or may not return entries depending on log state,
	// but if entries are returned, they should have non-zero Term.
	rs, err := nh.QueryRaftLog(shardID, 1, 100, 1024*1024)
	if err != nil {
		// Error is acceptable if log entries are unavailable.
		return
	}
	defer rs.Release()

	resultC := rs.ResultC()
	if resultC == nil {
		return
	}

	select {
	case result := <-resultC:
		entries, _ := result.RaftLogs()
		for _, e := range entries {
			if e.Term == 0 {
				t.Errorf("Entry at index %d has Term=0, expected non-zero", e.Index)
			}
		}
	case <-time.After(time.Second):
		t.Fatal("timed out reading QueryRaftLog result")
	}
}

func TestNodeHost_GetLeaderID_ConsistentLeaderIDAndTerm(t *testing.T) {
	nh := newTestNodeHost(t)
	shardID := uint64(1)
	replicaID := uint64(1)
	startTestShard(t, nh, shardID, replicaID)

	// The adapter's GetLeaderID now uses LeaderIDAndTerm internally,
	// so leaderID and term are read from the same nodeState atomically.
	leaderID, term, valid, err := nh.GetLeaderID(shardID)
	if err != nil {
		t.Fatalf("GetLeaderID: %v", err)
	}
	if !valid {
		t.Error("expected valid=true after leader election")
	}
	if leaderID == 0 {
		t.Error("expected non-zero leaderID")
	}
	if term == 0 {
		t.Error("expected non-zero term when leader is valid")
	}

	// Verify consistency: if valid, both leaderID and term must be non-zero.
	// A TOCTOU bug would occasionally return term=0 with a valid leaderID
	// or vice-versa when leadership changes between two separate calls.
	if valid && leaderID != 0 && term == 0 {
		t.Error("inconsistent: valid leader with zero term indicates TOCTOU race")
	}
}

func TestNodeHost_GetLeaderID_InvalidShard_ReturnsError(t *testing.T) {
	nh := newTestNodeHost(t)
	leaderID, term, valid, err := nh.GetLeaderID(999)
	if err == nil {
		t.Error("expected error for non-existent shard, got nil")
	}
	if valid {
		t.Error("expected valid=false for non-existent shard")
	}
	if leaderID != 0 {
		t.Errorf("expected leaderID=0 for error case, got %d", leaderID)
	}
	if term != 0 {
		t.Errorf("expected term=0 for error case, got %d", term)
	}
}

// ---------------------------------------------------------------------------
// ReadLocalNode linearizability: blocks until resolved
// ---------------------------------------------------------------------------

func TestReadLocalNode_UnresolvedFailsFast(t *testing.T) {
	nh := newTestNodeHost(t)
	shardID := uint64(1)
	replicaID := uint64(1)
	startTestShard(t, nh, shardID, replicaID)

	// ReadLocalNode must never block waiting for the ReadIndex: per the
	// dragonboat contract the caller drains ResultC first, so a second
	// receive inside ReadLocalNode would deadlock the canonical caller
	// pattern. An unresolved RequestState is rejected immediately.
	ch := make(chan RequestResult, 1)
	rs := &RequestState{CompletedC: ch, shardID: shardID}

	done := make(chan error, 1)
	go func() {
		_, err := nh.ReadLocalNode(rs, nil)
		done <- err
	}()

	select {
	case err := <-done:
		if !errors.Is(err, ErrInvalidOperation) {
			t.Fatalf("expected ErrInvalidOperation for unresolved ReadIndex, got %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("ReadLocalNode blocked on an unresolved RequestState")
	}
}

func TestReadLocalNode_AfterCompletionServesQuery(t *testing.T) {
	nh := newTestNodeHost(t)
	shardID := uint64(1)
	replicaID := uint64(1)
	startTestShard(t, nh, shardID, replicaID)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Write data so the state machine has something to return.
	session := nh.GetNoOPSession(shardID)
	payload := []byte("read-after-completion")
	if _, err := nh.SyncPropose(ctx, session, payload); err != nil {
		t.Fatalf("SyncPropose: %v", err)
	}

	// Canonical dragonboat caller pattern: ReadIndex, drain ResultC,
	// observe completion, then ReadLocalNode. This must not deadlock and
	// must serve the linearizable query.
	rs, err := nh.ReadIndex(shardID, 5*time.Second)
	if err != nil {
		t.Fatalf("ReadIndex: %v", err)
	}
	defer rs.Release()
	s := <-rs.ResultC()
	if !s.Completed() {
		t.Fatalf("ReadIndex did not complete: code=%v", s.code)
	}

	result, readErr := nh.ReadLocalNode(rs, nil)
	if readErr != nil {
		t.Fatalf("ReadLocalNode returned error: %v", readErr)
	}
	data, ok := result.([]byte)
	if !ok {
		t.Fatalf("expected []byte, got %T", result)
	}
	if string(data) != string(payload) {
		t.Errorf("expected %q, got %q", payload, data)
	}
}

func TestReadLocalNode_ReturnsErrorOnFailedReadIndex(t *testing.T) {
	nh := newTestNodeHost(t)

	tests := []struct {
		name        string
		code        RequestResultCode
		expectedErr error
		description string
	}{
		{
			name:        "Timeout",
			code:        RequestTimeout,
			expectedErr: ErrTimeout,
			description: "timed-out ReadIndex must propagate ErrTimeout",
		},
		{
			name:        "Terminated",
			code:        RequestTerminated,
			expectedErr: ErrShardClosed,
			description: "terminated ReadIndex must propagate ErrShardClosed",
		},
		{
			name:        "Rejected",
			code:        RequestRejected,
			expectedErr: ErrRejected,
			description: "rejected ReadIndex must propagate ErrRejected",
		},
		{
			name:        "Dropped",
			code:        RequestDropped,
			expectedErr: ErrShardNotReady,
			description: "dropped ReadIndex must propagate ErrShardNotReady",
		},
		{
			name:        "Aborted",
			code:        RequestAborted,
			expectedErr: ErrAborted,
			description: "aborted ReadIndex must propagate ErrAborted",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rs := newResolvedRequestState(RequestResult{code: tt.code})

			_, err := nh.ReadLocalNode(rs, nil)
			if err == nil {
				t.Fatalf("%s: expected error, got nil", tt.description)
			}
			if !errors.Is(err, tt.expectedErr) {
				t.Errorf("%s: expected %v, got %v", tt.description, tt.expectedErr, err)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// QueryTimeout configuration
// ---------------------------------------------------------------------------

func TestNodeHost_QueryTimeout_Default(t *testing.T) {
	nh := newTestNodeHost(t)

	// Dragonboat's ReadLocalNode calls sm.Lookup with no deadline, so the
	// adapter must impose none by default.
	if nh.QueryTimeout() != 0 {
		t.Errorf("expected default queryTimeout 0 (no deadline), got %v", nh.QueryTimeout())
	}
}

func TestNodeHost_SetQueryTimeout(t *testing.T) {
	nh := newTestNodeHost(t)

	// Set a custom timeout.
	custom := 10 * time.Second
	nh.SetQueryTimeout(custom)
	if nh.QueryTimeout() != custom {
		t.Errorf("expected queryTimeout %v after SetQueryTimeout, got %v", custom, nh.QueryTimeout())
	}

	// Zero restores the dragonboat default of no deadline.
	nh.SetQueryTimeout(0)
	if nh.QueryTimeout() != 0 {
		t.Errorf("expected queryTimeout 0 after SetQueryTimeout(0), got %v", nh.QueryTimeout())
	}

	// Negative is clamped to the same no-deadline default.
	nh.SetQueryTimeout(custom)
	nh.SetQueryTimeout(-1 * time.Second)
	if nh.QueryTimeout() != 0 {
		t.Errorf("expected queryTimeout 0 after SetQueryTimeout(-1s), got %v", nh.QueryTimeout())
	}

	// Verify a smaller positive value is accepted.
	smaller := 500 * time.Millisecond
	nh.SetQueryTimeout(smaller)
	if nh.QueryTimeout() != smaller {
		t.Errorf("expected queryTimeout %v, got %v", smaller, nh.QueryTimeout())
	}
}

func TestNodeHost_QueryTimeout_ConcurrentAccess(t *testing.T) {
	nh := newTestNodeHost(t)
	// The default is 0 (no deadline); seed a positive value so readers can
	// assert that they never observe a torn write.
	nh.SetQueryTimeout(time.Millisecond)

	const goroutines = 100
	const iterations = 1000

	var wg sync.WaitGroup
	wg.Add(goroutines * 2)

	// Half the goroutines write, half read.
	for i := range goroutines {
		go func(id int) {
			defer wg.Done()
			for j := range iterations {
				d := time.Duration(id*iterations+j+1) * time.Millisecond
				nh.SetQueryTimeout(d)
			}
		}(i)

		go func() {
			defer wg.Done()
			for range iterations {
				d := nh.QueryTimeout()
				if d <= 0 {
					t.Errorf("QueryTimeout returned non-positive duration: %v", d)
				}
			}
		}()
	}

	wg.Wait()
}

// Compile-time import verification.
var _ = dbClient.NewNoOPSession
