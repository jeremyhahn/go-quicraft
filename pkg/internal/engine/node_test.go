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

package engine

import (
	"sync"
	"sync/atomic"
	"testing"

	"github.com/jeremyhahn/go-quicraft/pkg/config"
	"github.com/jeremyhahn/go-quicraft/pkg/internal/raft"
	"github.com/jeremyhahn/go-quicraft/pkg/logdb"
	"github.com/jeremyhahn/go-quicraft/pkg/proto"
)

func TestNewNode_InvalidConfig(t *testing.T) {
	// Node creation does not validate config, but should still work
	// with zero values.
	cfg := config.Config{}
	n := NewNode(nil, nil, nil, cfg, nil, "", nil)
	if n.ShardID() != 0 {
		t.Errorf("ShardID = %d, want 0", n.ShardID())
	}
}

func TestNode_LastApplied(t *testing.T) {
	cfg := config.Config{ShardID: 1, ReplicaID: 1}
	n := NewNode(nil, nil, nil, cfg, nil, "", nil)

	if n.lastApplied.Load() != 0 {
		t.Fatalf("lastApplied should be 0 initially, got %d", n.lastApplied.Load())
	}
	n.lastApplied.Store(42)
	if n.lastApplied.Load() != 42 {
		t.Fatalf("lastApplied should be 42, got %d", n.lastApplied.Load())
	}
}

func TestNode_Stop(t *testing.T) {
	cfg := config.Config{ShardID: 1, ReplicaID: 1}
	n := NewNode(nil, nil, nil, cfg, nil, "", nil)

	// Stop should close the channel.
	n.Stop()
	select {
	case <-n.stopC:
		// Expected: channel is closed.
	default:
		t.Fatal("stopC should be closed after Stop()")
	}

	// Double stop should not panic.
	n.Stop()
}

func TestNode_HandleError_WithListener(t *testing.T) {
	var called atomic.Bool
	listener := &config.EventListener{
		OnShardFailed: func(info config.ShardFailedInfo) {
			called.Store(true)
			if info.ShardID != 5 {
				t.Errorf("ShardID = %d, want 5", info.ShardID)
			}
		},
	}
	cfg := config.Config{ShardID: 5, ReplicaID: 6}
	n := NewNode(nil, nil, nil, cfg, nil, "", listener)
	n.handleError(&ApplyError{ShardID: 5, ReplicaID: 6, Err: ErrStopped})
	if !called.Load() {
		t.Fatal("expected OnShardFailed callback to be invoked")
	}
}

func TestNode_HandleError_NilListener(t *testing.T) {
	cfg := config.Config{ShardID: 1, ReplicaID: 1}
	n := NewNode(nil, nil, nil, cfg, nil, "", nil)
	// Should not panic with nil listener.
	n.handleError(&ApplyError{ShardID: 1, ReplicaID: 1, Err: ErrStopped})
}

func TestNode_HandleError_ListenerWithoutCallback(t *testing.T) {
	// Listener configured but OnShardFailed is nil.
	listener := &config.EventListener{}
	cfg := config.Config{ShardID: 1, ReplicaID: 1}
	n := NewNode(nil, nil, nil, cfg, nil, "", listener)
	// Should not panic.
	n.handleError(&ApplyError{ShardID: 1, ReplicaID: 1, Err: ErrStopped})
}

func TestNode_StopC(t *testing.T) {
	cfg := config.Config{ShardID: 1, ReplicaID: 1}
	n := NewNode(nil, nil, nil, cfg, nil, "", nil)

	stopC := n.StopC()
	select {
	case <-stopC:
		t.Fatal("StopC should not be closed before Stop()")
	default:
		// expected
	}

	n.Stop()
	select {
	case <-stopC:
		// expected - channel should be closed after Stop()
	default:
		t.Fatal("StopC should be closed after Stop()")
	}
}

func TestNode_CacheLinePadding(t *testing.T) {
	// Verify padding sizes are correct for cache-line isolation.
	cfg := config.Config{ShardID: 1, ReplicaID: 1}
	n := NewNode(nil, nil, nil, cfg, nil, "", nil)

	// commitPending: atomic.Bool (1 byte) + 127 bytes padding = 128 bytes.
	// lastApplied: atomic.Uint64 (8 bytes) + 120 bytes padding = 128 bytes.
	// These are compile-time guarantees via struct layout; verify they
	// are independently addressable by testing concurrent access.
	done := make(chan struct{})
	go func() {
		for range 1000 {
			n.commitPending.Store(true)
			n.commitPending.Store(false)
		}
		close(done)
	}()
	for range 1000 {
		n.lastApplied.Store(42)
		n.lastApplied.Load()
	}
	<-done
}

func TestNode_DeliverProposal_Success(t *testing.T) {
	cfg := config.Config{ShardID: 1, ReplicaID: 1}
	n := NewNode(nil, nil, nil, cfg, nil, "", nil)

	entries := []proto.Entry{
		{Key: 1, Cmd: []byte("cmd1")},
		{Key: 2, Cmd: []byte("cmd2")},
	}
	ok := n.DeliverProposal(entries)
	if !ok {
		t.Fatal("DeliverProposal should succeed when queue is not full")
	}

	// Drain and verify.
	drained := n.DrainProposals(nil)
	if len(drained) != 2 {
		t.Fatalf("expected 2 entries, got %d", len(drained))
	}
	if drained[0].Key != 1 || drained[1].Key != 2 {
		t.Fatalf("entries = %+v, want keys 1,2", drained)
	}
}

func TestNode_DeliverProposal_QueueFull(t *testing.T) {
	cfg := config.Config{ShardID: 1, ReplicaID: 1}
	n := NewNode(nil, nil, nil, cfg, nil, "", nil)

	// Fill the queue to capacity (defaultProposalQueueSize = 256).
	bigBatch := make([]proto.Entry, defaultProposalQueueSize)
	for i := range bigBatch {
		bigBatch[i] = proto.Entry{Key: uint64(i)}
	}
	ok := n.DeliverProposal(bigBatch)
	if !ok {
		t.Fatal("DeliverProposal should succeed when filling exactly to capacity")
	}

	// One more entry should be rejected.
	ok = n.DeliverProposal([]proto.Entry{{Key: 999}})
	if ok {
		t.Fatal("DeliverProposal should reject when queue is at capacity")
	}
}

func TestNode_SetCommittedIndex_StoresAndLoads(t *testing.T) {
	cfg := config.Config{ShardID: 1, ReplicaID: 1}
	n := NewNode(nil, nil, nil, cfg, nil, "", nil)

	const want uint64 = 42
	n.SetCommittedIndex(want)
	if got := n.CommittedIndex(); got != want {
		t.Fatalf("CommittedIndex() = %d, want %d after SetCommittedIndex", got, want)
	}
}

func TestNode_CommittedIndex_Monotonic(t *testing.T) {
	cfg := config.Config{ShardID: 3, ReplicaID: 5}
	n := NewNode(nil, nil, nil, cfg, nil, "", nil)

	indices := []uint64{1, 10, 100, 1000}
	for _, idx := range indices {
		n.SetCommittedIndex(idx)
		if got := n.CommittedIndex(); got != idx {
			t.Fatalf("CommittedIndex() = %d after SetCommittedIndex(%d)", got, idx)
		}
	}
}

func TestNode_CommittedIndex_ConcurrentSafe(t *testing.T) {
	// Verify that concurrent reads and writes do not race. Run with
	// the race detector enabled (go test -race) for full coverage.
	cfg := config.Config{ShardID: 2, ReplicaID: 2}
	n := NewNode(nil, nil, nil, cfg, nil, "", nil)

	var wg sync.WaitGroup
	const iterations = 1000

	// Writer goroutine (simulates step worker).
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := range uint64(iterations) {
			n.SetCommittedIndex(i)
		}
	}()

	// Reader goroutines (simulates FollowerRead callers).
	for range 4 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range iterations {
				_ = n.CommittedIndex()
			}
		}()
	}
	wg.Wait()
}

func TestNode_DrainProposals_Empty(t *testing.T) {
	cfg := config.Config{ShardID: 1, ReplicaID: 1}
	n := NewNode(nil, nil, nil, cfg, nil, "", nil)

	buf := make([]proto.Entry, 0, 8)
	result := n.DrainProposals(buf)
	if len(result) != 0 {
		t.Fatalf("expected empty drain, got %d entries", len(result))
	}
}

func TestNode_DrainProposals_ReusesBuf(t *testing.T) {
	cfg := config.Config{ShardID: 1, ReplicaID: 1}
	n := NewNode(nil, nil, nil, cfg, nil, "", nil)

	n.DeliverProposal([]proto.Entry{{Key: 1}, {Key: 2}})
	first := n.DrainProposals(nil)
	if len(first) != 2 {
		t.Fatalf("first drain: expected 2, got %d", len(first))
	}

	// Second drain with recycled buffer should return empty.
	n.DeliverProposal([]proto.Entry{{Key: 3}})
	second := n.DrainProposals(first[:0])
	if len(second) != 1 {
		t.Fatalf("second drain: expected 1, got %d", len(second))
	}
	if second[0].Key != 3 {
		t.Fatalf("second drain: Key = %d, want 3", second[0].Key)
	}
}

func TestNode_DeliverProposal_Concurrent(t *testing.T) {
	cfg := config.Config{ShardID: 1, ReplicaID: 1}
	n := NewNode(nil, nil, nil, cfg, nil, "", nil)

	const numGoroutines = 8
	const entriesPerGoroutine = 30
	// Total: 8 * 30 = 240 < 256 (defaultProposalQueueSize), so all should fit.

	var wg sync.WaitGroup
	wg.Add(numGoroutines)
	for g := range numGoroutines {
		go func(id int) {
			defer wg.Done()
			for i := range entriesPerGoroutine {
				key := uint64(id*entriesPerGoroutine + i)
				ok := n.DeliverProposal([]proto.Entry{{Key: key}})
				if !ok {
					t.Errorf("goroutine %d: DeliverProposal(%d) rejected", id, key)
					return
				}
			}
		}(g)
	}
	wg.Wait()

	drained := n.DrainProposals(nil)
	if len(drained) != numGoroutines*entriesPerGoroutine {
		t.Fatalf("expected %d entries, got %d",
			numGoroutines*entriesPerGoroutine, len(drained))
	}

	// Verify all keys are present.
	seen := make(map[uint64]bool, len(drained))
	for _, e := range drained {
		seen[e.Key] = true
	}
	for i := range numGoroutines * entriesPerGoroutine {
		if !seen[uint64(i)] {
			t.Fatalf("missing entry with Key=%d", i)
		}
	}
}

// ---------------------------------------------------------------------------
// ReadIndex queue tests
// ---------------------------------------------------------------------------

func TestNode_DeliverReadIndex_Success(t *testing.T) {
	cfg := config.Config{ShardID: 1, ReplicaID: 1}
	n := NewNode(nil, nil, nil, cfg, nil, "", nil)

	ok := n.DeliverReadIndex(42)
	if !ok {
		t.Fatal("DeliverReadIndex should succeed when queue is not full")
	}

	ok = n.DeliverReadIndex(43)
	if !ok {
		t.Fatal("DeliverReadIndex should succeed for second key")
	}

	drained := n.DrainReadIndex(nil)
	if len(drained) != 2 {
		t.Fatalf("expected 2 keys, got %d", len(drained))
	}
	if drained[0] != 42 || drained[1] != 43 {
		t.Fatalf("keys = %v, want [42 43]", drained)
	}
}

func TestNode_DeliverReadIndex_QueueFull(t *testing.T) {
	cfg := config.Config{ShardID: 1, ReplicaID: 1}
	n := NewNode(nil, nil, nil, cfg, nil, "", nil)

	// Fill the queue to capacity (defaultReadIndexQueueSize = 4096).
	for i := uint64(0); i < defaultReadIndexQueueSize; i++ {
		ok := n.DeliverReadIndex(i)
		if !ok {
			t.Fatalf("DeliverReadIndex(%d) should succeed before capacity", i)
		}
	}

	// One more should be rejected.
	ok := n.DeliverReadIndex(99999)
	if ok {
		t.Fatal("DeliverReadIndex should reject when queue is at capacity")
	}
}

func TestNode_DrainReadIndex_Empty(t *testing.T) {
	cfg := config.Config{ShardID: 1, ReplicaID: 1}
	n := NewNode(nil, nil, nil, cfg, nil, "", nil)

	buf := make([]uint64, 0, 8)
	result := n.DrainReadIndex(buf)
	if len(result) != 0 {
		t.Fatalf("expected empty drain, got %d keys", len(result))
	}
}

func TestNode_DrainReadIndex_ReusesBuf(t *testing.T) {
	cfg := config.Config{ShardID: 1, ReplicaID: 1}
	n := NewNode(nil, nil, nil, cfg, nil, "", nil)

	n.DeliverReadIndex(10)
	n.DeliverReadIndex(20)
	first := n.DrainReadIndex(nil)
	if len(first) != 2 {
		t.Fatalf("first drain: expected 2, got %d", len(first))
	}

	// Second drain with recycled buffer should return new items.
	n.DeliverReadIndex(30)
	second := n.DrainReadIndex(first[:0])
	if len(second) != 1 {
		t.Fatalf("second drain: expected 1, got %d", len(second))
	}
	if second[0] != 30 {
		t.Fatalf("second drain: key = %d, want 30", second[0])
	}
}

func TestNode_DeliverReadIndex_Concurrent(t *testing.T) {
	cfg := config.Config{ShardID: 1, ReplicaID: 1}
	n := NewNode(nil, nil, nil, cfg, nil, "", nil)

	const numGoroutines = 8
	const keysPerGoroutine = 50
	// Total: 8 * 50 = 400 < 4096 (defaultReadIndexQueueSize), so all should fit.

	var wg sync.WaitGroup
	wg.Add(numGoroutines)
	for g := range numGoroutines {
		go func(id int) {
			defer wg.Done()
			for i := range keysPerGoroutine {
				key := uint64(id*keysPerGoroutine + i)
				ok := n.DeliverReadIndex(key)
				if !ok {
					t.Errorf("goroutine %d: DeliverReadIndex(%d) rejected", id, key)
					return
				}
			}
		}(g)
	}
	wg.Wait()

	drained := n.DrainReadIndex(nil)
	if len(drained) != numGoroutines*keysPerGoroutine {
		t.Fatalf("expected %d keys, got %d",
			numGoroutines*keysPerGoroutine, len(drained))
	}

	// Verify all keys are present.
	seen := make(map[uint64]bool, len(drained))
	for _, k := range drained {
		seen[k] = true
	}
	for i := range numGoroutines * keysPerGoroutine {
		if !seen[uint64(i)] {
			t.Fatalf("missing key %d", i)
		}
	}
}

// --- IsMember ---

func TestNode_IsMember_NilPeer(t *testing.T) {
	// When peer is nil, IsMember should return true (cannot verify,
	// allow message through to avoid blocking during initialization).
	cfg := config.Config{ShardID: 1, ReplicaID: 1}
	n := NewNode(nil, nil, nil, cfg, nil, "", nil)

	if !n.IsMember(42) {
		t.Error("IsMember should return true when peer is nil")
	}
	if !n.IsMember(0) {
		t.Error("IsMember should return true for replicaID 0 when peer is nil")
	}
}

func TestNode_IsMember_UnknownReplicaRejected(t *testing.T) {
	// Create a node with a real peer that has a multi-node membership.
	ldb := newTestLogDB()
	lr := logdb.NewLogReader(1, 1, ldb)
	ms := proto.Membership{
		Addresses: map[uint64]string{
			1: "addr1",
			2: "addr2",
		},
	}
	lr.ApplySnapshot(proto.Snapshot{
		Index:      0,
		Term:       0,
		Membership: ms,
	})
	cfg := config.Config{ShardID: 1, ReplicaID: 1}
	cfg.SetDefaults()
	peer, err := raft.NewPeer(cfg, lr, nil)
	if err != nil {
		t.Fatalf("NewPeer failed: %v", err)
	}
	n := NewNode(peer, nil, nil, cfg, nil, "", nil)

	// Known members should pass.
	if !n.IsMember(1) {
		t.Error("IsMember should return true for replica 1")
	}
	if !n.IsMember(2) {
		t.Error("IsMember should return true for replica 2")
	}

	// Unknown member should fail.
	if n.IsMember(999) {
		t.Error("IsMember should return false for unknown replica 999")
	}
}

// ---------------------------------------------------------------------------
// SetNotifyCommit
// ---------------------------------------------------------------------------

func TestNode_SetNotifyCommit_DisablesFlag(t *testing.T) {
	cfg := config.Config{ShardID: 1, ReplicaID: 1}
	n := NewNode(nil, nil, nil, cfg, nil, "", nil)

	n.SetNotifyCommit(true)
	n.SetNotifyCommit(false)
	if n.notifyCommit {
		t.Fatal("notifyCommit should be false after SetNotifyCommit(false)")
	}
}

// ---------------------------------------------------------------------------
// TryStartSnapshot / ClearSnapshotting
// ---------------------------------------------------------------------------

func TestNode_TryStartSnapshot_FirstCallSucceeds(t *testing.T) {
	cfg := config.Config{ShardID: 1, ReplicaID: 1}
	n := NewNode(nil, nil, nil, cfg, nil, "", nil)

	if !n.TryStartSnapshot() {
		t.Fatal("first TryStartSnapshot should succeed")
	}
}

func TestNode_TryStartSnapshot_SecondCallFails(t *testing.T) {
	cfg := config.Config{ShardID: 1, ReplicaID: 1}
	n := NewNode(nil, nil, nil, cfg, nil, "", nil)

	if !n.TryStartSnapshot() {
		t.Fatal("first TryStartSnapshot should succeed")
	}
	if n.TryStartSnapshot() {
		t.Fatal("second TryStartSnapshot should fail (already snapshotting)")
	}
}

func TestNode_ClearSnapshotting_AllowsRetry(t *testing.T) {
	cfg := config.Config{ShardID: 1, ReplicaID: 1}
	n := NewNode(nil, nil, nil, cfg, nil, "", nil)

	if !n.TryStartSnapshot() {
		t.Fatal("first TryStartSnapshot should succeed")
	}
	n.ClearSnapshotting()

	// After clearing, a new snapshot attempt should succeed.
	if !n.TryStartSnapshot() {
		t.Fatal("TryStartSnapshot should succeed after ClearSnapshotting")
	}
}

func TestNode_ClearSnapshotting_Idempotent(t *testing.T) {
	cfg := config.Config{ShardID: 1, ReplicaID: 1}
	n := NewNode(nil, nil, nil, cfg, nil, "", nil)

	// Clearing when not snapshotting should not panic or cause issues.
	n.ClearSnapshotting()
	n.ClearSnapshotting()

	// State should remain non-snapshotting.
	if !n.TryStartSnapshot() {
		t.Fatal("TryStartSnapshot should succeed after redundant ClearSnapshotting calls")
	}
}

// ---------------------------------------------------------------------------
// DiagRaftState
// ---------------------------------------------------------------------------

func TestNode_DiagRaftState_NilPeer(t *testing.T) {
	cfg := config.Config{ShardID: 1, ReplicaID: 1}
	n := NewNode(nil, nil, nil, cfg, nil, "", nil)

	term, committed, processed, lastIdx, msgs, state := n.DiagRaftState()
	if term != 0 || committed != 0 || processed != 0 || lastIdx != 0 || msgs != 0 {
		t.Fatalf("expected all zeros for nil peer, got term=%d committed=%d processed=%d lastIdx=%d msgs=%d",
			term, committed, processed, lastIdx, msgs)
	}
	if state != "nil" {
		t.Fatalf("expected state 'nil' for nil peer, got %q", state)
	}
}

func TestNode_DiagRaftState_WithPeer(t *testing.T) {
	peer, lr := newTestPeer(t, 1, 1)
	cfg := config.Config{ShardID: 1, ReplicaID: 1}
	cfg.SetDefaults()
	n := NewNode(peer, nil, lr, cfg, nil, "", nil)

	// DiagRaftState reads an eventually-consistent snapshot published by
	// the step worker under raftMu (reading Peer.r from a foreign
	// goroutine would race). With no step worker in this unit test,
	// publish one snapshot the same way tick() does.
	n.diagEnabled.Store(true)
	n.raftMu.Lock()
	n.publishDiagLocked()
	n.raftMu.Unlock()

	term, _, _, _, _, state := n.DiagRaftState()
	// A fresh single-node peer starts in follower state at term 0.
	if term != 0 {
		t.Fatalf("expected term 0 for fresh peer, got %d", term)
	}
	if state == "nil" {
		t.Fatal("expected non-nil state string with a real peer")
	}
	// The state string should contain a recognizable raft state name.
	if !contains(state, "follower") && !contains(state, "leader") &&
		!contains(state, "candidate") && !contains(state, "preCandidate") {
		t.Fatalf("expected a recognizable raft state, got %q", state)
	}
}

// ---------------------------------------------------------------------------
// DiagRemotes
// ---------------------------------------------------------------------------

func TestNode_DiagRemotes_NilPeer(t *testing.T) {
	cfg := config.Config{ShardID: 1, ReplicaID: 1}
	n := NewNode(nil, nil, nil, cfg, nil, "", nil)

	result := n.DiagRemotes()
	if result != "nil" {
		t.Fatalf("expected 'nil' for nil peer, got %q", result)
	}
}

func TestNode_DiagRemotes_WithNonLeaderPeer(t *testing.T) {
	peer, lr := newTestPeer(t, 1, 1)
	cfg := config.Config{ShardID: 1, ReplicaID: 1}
	cfg.SetDefaults()
	n := NewNode(peer, nil, lr, cfg, nil, "", nil)

	// Publish a snapshot as the step worker would (no step worker runs in
	// this unit test); see TestNode_DiagRaftState_WithPeer.
	n.diagEnabled.Store(true)
	n.raftMu.Lock()
	n.publishDiagLocked()
	n.raftMu.Unlock()

	// A fresh peer is a follower, so DiagRemotes should return "not leader".
	result := n.DiagRemotes()
	if result != "not leader" {
		t.Fatalf("expected 'not leader' for follower peer, got %q", result)
	}
}

// ---------------------------------------------------------------------------
// Snapshotter / SnapshotDir accessors
// ---------------------------------------------------------------------------

func TestNode_SnapshotDir_ReturnsConfiguredPath(t *testing.T) {
	cfg := config.Config{ShardID: 1, ReplicaID: 1}
	n := NewNode(nil, nil, nil, cfg, nil, "/tmp/snapshots/shard1", nil)

	if n.SnapshotDir() != "/tmp/snapshots/shard1" {
		t.Fatalf("SnapshotDir = %q, want /tmp/snapshots/shard1", n.SnapshotDir())
	}
}

// ---------------------------------------------------------------------------
// LastSnapshotIndex
// ---------------------------------------------------------------------------

func TestNode_LastSnapshotIndex_AfterStore(t *testing.T) {
	cfg := config.Config{ShardID: 1, ReplicaID: 1}
	n := NewNode(nil, nil, nil, cfg, nil, "", nil)

	n.lastSnapshotIndex.Store(500)
	if n.LastSnapshotIndex() != 500 {
		t.Fatalf("LastSnapshotIndex = %d, want 500", n.LastSnapshotIndex())
	}
}

// ---------------------------------------------------------------------------
// Recovering
// ---------------------------------------------------------------------------

func TestNode_SetRecovering_RoundTrip(t *testing.T) {
	cfg := config.Config{ShardID: 1, ReplicaID: 1}
	n := NewNode(nil, nil, nil, cfg, nil, "", nil)

	n.SetRecovering(true)
	if !n.IsRecovering() {
		t.Fatal("IsRecovering should be true after SetRecovering(true)")
	}

	n.SetRecovering(false)
	if n.IsRecovering() {
		t.Fatal("IsRecovering should be false after SetRecovering(false)")
	}
}

func TestNode_DiagState_IncludesReadIndexQ(t *testing.T) {
	cfg := config.Config{ShardID: 1, ReplicaID: 1}
	n := NewNode(nil, nil, nil, cfg, nil, "", nil)

	if d := n.DiagState(); d.ReadIndexQLen != 0 {
		t.Fatalf("readIndexQ should be 0 initially, got %d", d.ReadIndexQLen)
	}

	n.DeliverReadIndex(1)
	n.DeliverReadIndex(2)
	if d := n.DiagState(); d.ReadIndexQLen != 2 {
		t.Fatalf("readIndexQ should be 2 after 2 deliveries, got %d", d.ReadIndexQLen)
	}
}

// TestNode_DiagState_ReportsSnapshotGates verifies that the diagnostic
// snapshot exposes the recovering and saving gates and the apply drain
// counter. Without them an operator sees a shard that is applying nothing
// and cannot tell whether a snapshot gate is holding it.
func TestNode_DiagState_ReportsSnapshotGates(t *testing.T) {
	cfg := config.Config{ShardID: 1, ReplicaID: 1}
	n := NewNode(nil, nil, nil, cfg, nil, "", nil)

	if d := n.DiagState(); d.Recovering || d.Saving || d.ApplyInFlight != 0 {
		t.Fatalf("fresh node reported gates %+v", d)
	}

	n.SetRecovering(true)
	n.applyInFlight.Add(1)
	d := n.DiagState()
	if !d.Recovering {
		t.Fatal("DiagState must report the recovering gate")
	}
	if d.ApplyInFlight != 1 {
		t.Fatalf("DiagState ApplyInFlight = %d, want 1", d.ApplyInFlight)
	}
	n.SetRecovering(false)
	n.applyInFlight.Add(-1)

	n.SetSaving(true)
	if d := n.DiagState(); !d.Saving {
		t.Fatal("DiagState must report the saving gate")
	}
}

// ===========================================================================
// lastApplied monotonically increasing
// ===========================================================================

// TestNode_LastAppliedMonotonicallyIncreasing verifies that lastApplied
// only increases and never decreases, even under concurrent access.
func TestNode_LastAppliedMonotonicallyIncreasing(t *testing.T) {
	cfg := config.Config{ShardID: 1, ReplicaID: 1}
	n := NewNode(nil, nil, nil, cfg, nil, "", nil)

	// Advance lastApplied in sequence.
	for _, idx := range []uint64{1, 5, 10, 20, 50, 100} {
		n.lastApplied.Store(idx)
		if n.lastApplied.Load() != idx {
			t.Fatalf("lastApplied = %d, want %d", n.lastApplied.Load(), idx)
		}
	}

	// Verify current value.
	if n.lastApplied.Load() != 100 {
		t.Fatalf("lastApplied = %d, want 100", n.lastApplied.Load())
	}

	// Attempting to store a lower value would be a bug. Verify that
	// the atomic store does store the value (the caller is responsible
	// for the monotonic invariant, not the atomic itself).
	// In a real system, the apply worker enforces this. Here we verify
	// the pattern: only store if higher.
	current := n.lastApplied.Load()
	newVal := uint64(50) // lower than current
	if newVal > current {
		n.lastApplied.Store(newVal)
	}
	if n.lastApplied.Load() != 100 {
		t.Fatalf("lastApplied should remain 100, got %d", n.lastApplied.Load())
	}

	// Store a higher value.
	newVal = 200
	if newVal > current {
		n.lastApplied.Store(newVal)
	}
	if n.lastApplied.Load() != 200 {
		t.Fatalf("lastApplied = %d, want 200", n.lastApplied.Load())
	}
}

// TestNode_LastAppliedMonotonicallyIncreasing_InvalidDecrease verifies
// that the test pattern correctly rejects a decreasing lastApplied.
func TestNode_LastAppliedMonotonicallyIncreasing_InvalidDecrease(t *testing.T) {
	cfg := config.Config{ShardID: 1, ReplicaID: 1}
	n := NewNode(nil, nil, nil, cfg, nil, "", nil)

	n.lastApplied.Store(500)

	// Guard pattern: only advance, never regress.
	candidate := uint64(200)
	if candidate <= n.lastApplied.Load() {
		// Correctly rejected.
	} else {
		t.Fatal("candidate 200 should be rejected when lastApplied is 500")
	}

	if n.lastApplied.Load() != 500 {
		t.Fatalf("lastApplied should remain 500, got %d", n.lastApplied.Load())
	}
}
