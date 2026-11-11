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
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jeremyhahn/go-quicraft/pkg/config"
	"github.com/jeremyhahn/go-quicraft/pkg/internal/raft"
	"github.com/jeremyhahn/go-quicraft/pkg/logdb"
	"github.com/jeremyhahn/go-quicraft/pkg/proto"
)

// TestSendMessages verifies that sendMessages routes every InstallSnapshot
// through SendSnapshot and every other message, in order, through Send: the
// fast path (no snapshot), a batch of snapshots only, and a mixed batch.
func TestSendMessages(t *testing.T) {
	tests := []struct {
		name        string
		msgs        []proto.Message
		wantRegular []uint64 // message types sent through Send, in order
		wantSnapTo  []uint64 // targets sent through SendSnapshot, in order
	}{
		{
			name: "no snapshots",
			msgs: []proto.Message{
				{Type: proto.Heartbeat, ShardID: 1, From: 1, To: 2},
				{Type: proto.Replicate, ShardID: 1, From: 1, To: 3},
				{Type: proto.HeartbeatResp, ShardID: 1, From: 2, To: 1},
			},
			wantRegular: []uint64{proto.Heartbeat, proto.Replicate, proto.HeartbeatResp},
		},
		{
			name: "only snapshots",
			msgs: []proto.Message{
				{Type: proto.InstallSnapshot, Origin: proto.OriginLocal, ShardID: 1, From: 1, To: 2},
				{Type: proto.InstallSnapshot, ShardID: 1, From: 1, To: 3},
			},
			wantSnapTo: []uint64{2, 3},
		},
		{
			name: "mixed batch",
			msgs: []proto.Message{
				{Type: proto.Heartbeat, ShardID: 1, From: 1, To: 2},
				{Type: proto.InstallSnapshot, ShardID: 1, From: 1, To: 3},
				{Type: proto.Replicate, ShardID: 1, From: 1, To: 4},
				{Type: proto.InstallSnapshot, ShardID: 2, From: 1, To: 5},
				{Type: proto.HeartbeatResp, ShardID: 1, From: 2, To: 1},
			},
			wantRegular: []uint64{proto.Heartbeat, proto.Replicate, proto.HeartbeatResp},
			wantSnapTo:  []uint64{3, 5},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sender := newTestSender()
			w := newStepWorker(0, time.Millisecond, sender, NewWorkSignal(1),
				make(chan *commitItem, 1), nil, make(chan struct{}), nil)

			w.sendMessages(tt.msgs)

			regular := sender.sentMessages()
			if len(regular) != len(tt.wantRegular) {
				t.Fatalf("sent %d regular messages, want %d", len(regular), len(tt.wantRegular))
			}
			for i, msg := range regular {
				if msg.Type != tt.wantRegular[i] {
					t.Errorf("regular[%d].Type = %v, want %v", i, msg.Type, tt.wantRegular[i])
				}
			}
			snaps := sender.sentSnapshots()
			if len(snaps) != len(tt.wantSnapTo) {
				t.Fatalf("sent %d snapshot messages, want %d", len(snaps), len(tt.wantSnapTo))
			}
			for i, snap := range snaps {
				if snap.To != tt.wantSnapTo[i] {
					t.Errorf("snapshot[%d].To = %d, want %d", i, snap.To, tt.wantSnapTo[i])
				}
			}
		})
	}
}

// TestStepWorker_ProposalBatching verifies that multiple Propose messages
// in the inbox are batched into a single Handle call, while non-proposal
// messages (e.g. Heartbeat) are handled individually. This reduces
// broadcastAppend calls from N to 1 when N concurrent proposals arrive.
func TestStepWorker_ProposalBatching(t *testing.T) {
	sender := newTestSender()
	ws := NewWorkSignal(1)
	commitC := make(chan *commitItem, 16)
	stopC := make(chan struct{})

	cb := newTestCallback()
	sw := newStepWorker(0, time.Millisecond, sender, ws, commitC, cb, stopC, nil)

	peer, lr := newTestPeer(t, 1, 1)
	cfg := config.Config{ShardID: 1, ReplicaID: 1}
	cfg.SetDefaults()
	testSm := newTestSM()
	rsmSM := newTestApplier(testSm)
	node := NewNode(peer, rsmSM, lr, cfg, nil, "", nil)
	sw.loadNode(node)

	// Tick the peer until it becomes leader in this single-node cluster.
	tickPeerToLeader(peer)

	// Drain the initial election update so processReady starts clean.
	ws.Notify(1)
	sw.processReady()
	select {
	case item := <-commitC:
		if err := peer.Commit(item.update); err != nil {
			t.Fatalf("initial commit failed: %v", err)
		}
		node.commitPending.Store(false)
	default:
	}

	// Deliver 4 proposals via the MPSC queue and 1 Heartbeat via inbox.
	// Each DeliverProposal simulates a concurrent goroutine proposing
	// independently. The Heartbeat is a transport message and uses the
	// channel-based inbox.
	for i := range 4 {
		node.DeliverProposal([]proto.Entry{{Cmd: []byte{byte(i + 1)}}})
	}
	node.Deliver(proto.Message{
		Type:    proto.Heartbeat,
		From:    1,
		To:      1,
		ShardID: 1,
	})

	// processReady should batch all 4 proposals into a single Handle
	// call (one log append + one broadcastAppend) and handle the
	// heartbeat individually.
	ws.Notify(1)
	sw.processReady()

	// Verify a commit item was produced with all 4 proposal entries.
	select {
	case item := <-commitC:
		if item.node != node {
			t.Fatal("commit item should reference the correct node")
		}
		// The update should contain entries from all 4 batched proposals.
		// In a single-node cluster, proposed entries are immediately
		// committed, so they appear in EntriesToSave.
		totalEntries := len(item.update.EntriesToSave)
		if totalEntries < 4 {
			t.Fatalf("expected at least 4 entries from batched proposals, got %d",
				totalEntries)
		}
	case <-time.After(time.Second):
		t.Fatal("expected a commit item in commitC from batched proposals")
	}

	// Both the proposal queue and inbox should be fully drained.
	remainingProposals := node.DrainProposals(nil)
	if len(remainingProposals) != 0 {
		t.Fatalf("proposal queue should be empty after processReady, got %d entries", len(remainingProposals))
	}
	remaining := node.DrainInbox(nil)
	if len(remaining) != 0 {
		t.Fatalf("inbox should be empty after processReady, got %d messages", len(remaining))
	}
}

// TestStepWorker_MemoryMode_BypassesCommitC verifies that when
// memoryMode=true, processReady calls Peer.Commit inline and forwards
// the update to applyC, completely bypassing commitC. This is the
// memory-mode fast path that eliminates the commit worker goroutine
// hop, reducing the pipeline from 3 hops to 2.
func TestStepWorker_MemoryMode_BypassesCommitC(t *testing.T) {
	sender := newTestSender()
	ws := NewWorkSignal(1)
	commitC := make(chan *commitItem, 16)
	applyC := make(chan *applyItem, 16)
	applyPool := &sync.Pool{New: func() interface{} { return &applyItem{} }}
	stopC := make(chan struct{})

	cb := newTestCallback()
	sw := newStepWorker(0, time.Millisecond, sender, ws, commitC, cb, stopC, nil)

	// Configure memory-mode fast path fields.
	sw.memoryMode = true
	sw.applyC = applyC
	sw.applyPool = applyPool

	peer, lr := newTestPeer(t, 1, 1)
	cfg := config.Config{ShardID: 1, ReplicaID: 1}
	cfg.SetDefaults()
	testSm := newTestSM()
	rsmSM := newTestApplier(testSm)
	node := NewNode(peer, rsmSM, lr, cfg, nil, "", nil)
	sw.loadNode(node)

	// Tick the peer until it becomes leader in this single-node cluster.
	tickPeerToLeader(peer)

	// Propose an entry to generate a non-read-only update.
	_, err := peer.Propose([]proto.Entry{{Cmd: []byte("memory-mode")}})
	if err != nil {
		t.Fatalf("propose failed: %v", err)
	}

	ws.Notify(1)
	sw.processReady()

	// The update should appear in applyC (memory-mode fast path).
	select {
	case ai := <-applyC:
		if ai.node != node {
			t.Fatal("apply item should reference the correct node")
		}
		if ai.update.ShardID != 1 {
			t.Errorf("apply update.ShardID = %d, want 1", ai.update.ShardID)
		}
	case <-time.After(time.Second):
		t.Fatal("expected an apply item in applyC via memory-mode fast path")
	}

	// commitC must remain empty. The memory-mode path bypasses it entirely.
	select {
	case <-commitC:
		t.Fatal("commitC should be empty in memory mode")
	default:
		// Expected: nothing in commitC.
	}

	// commitPending should be set to enforce the single-owner invariant
	// so the step worker skips this shard on the next tick.
	if !node.commitPending.Load() {
		t.Fatal("commitPending should be true after memory-mode processReady")
	}
}

// TestStepWorker_MemoryMode_StopC_ClearsState verifies that when the
// step worker is in memory mode and stopC fires during the applyC send,
// commitPending is cleared and the applyItem is returned to the pool.
func TestStepWorker_MemoryMode_StopC_ClearsState(t *testing.T) {
	sender := newTestSender()
	ws := NewWorkSignal(1)
	commitC := make(chan *commitItem, 16)
	// Use an unbuffered applyC so the send blocks until stopC fires.
	applyC := make(chan *applyItem)
	applyPool := &sync.Pool{New: func() interface{} { return &applyItem{} }}
	stopC := make(chan struct{})

	sw := newStepWorker(0, time.Millisecond, sender, ws, commitC, nil, stopC, nil)
	sw.memoryMode = true
	sw.applyC = applyC
	sw.applyPool = applyPool

	peer, lr := newTestPeer(t, 1, 1)
	cfg := config.Config{ShardID: 1, ReplicaID: 1}
	cfg.SetDefaults()
	node := NewNode(peer, nil, lr, cfg, nil, "", nil)
	sw.loadNode(node)

	tickPeerToLeader(peer)

	_, err := peer.Propose([]proto.Entry{{Cmd: []byte("stop-test")}})
	if err != nil {
		t.Fatalf("propose failed: %v", err)
	}

	ws.Notify(1)

	// processReady will block on the unbuffered applyC send. Close
	// stopC to unblock it.
	done := make(chan struct{})
	go func() {
		sw.processReady()
		close(done)
	}()

	// Wait until commitPending is set, which means processReady has
	// reached the applyC send and is now blocking on the select.
	waitFor(t, 5*time.Second, time.Millisecond, "timed out waiting for processReady to block on applyC send",
		node.commitPending.Load)
	close(stopC)

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("processReady did not unblock after stopC closed")
	}

	// commitPending should be cleared by the stopC branch.
	if node.commitPending.Load() {
		t.Fatal("commitPending should be false after stopC fired in memory mode")
	}
}

// newMultiNodeLeaderTestPeer creates a 3-node cluster peer that is fully
// elected as leader with the no-op entry committed. This is needed for
// multi-node ReadIndex tests where the leader must have committed an
// entry in its current term (PhD 6.4).
func newMultiNodeLeaderTestPeer(t *testing.T, shardID, replicaID uint64) (*raft.Peer, *logdb.LogReader) {
	t.Helper()
	ldb := newTestLogDB()
	lr := logdb.NewLogReader(shardID, replicaID, ldb)
	ms := proto.Membership{
		Addresses: map[uint64]string{
			1: "localhost:10001",
			2: "localhost:10002",
			3: "localhost:10003",
		},
	}
	lr.ApplySnapshot(proto.Snapshot{
		Index:      0,
		Term:       0,
		Membership: ms,
	})
	cfg := config.Config{
		ShardID:   shardID,
		ReplicaID: replicaID,
		PreVote:   config.BoolPtr(false), // disable PreVote for simpler test election
	}
	cfg.SetDefaults()
	peer, err := raft.NewPeer(cfg, lr, nil)
	if err != nil {
		t.Fatalf("failed to create peer: %v", err)
	}

	// Tick past election timeout to start election.
	for range 30 {
		peer.Tick()
	}
	// Drain election messages.
	_ = peer.DrainMessages()

	// Grant votes. Network-originated message types must carry the term:
	// raft rejects a zero term on them, because Term==0 is reserved for
	// locally-injected control messages and would otherwise let a peer
	// bypass every term check.
	campaignTerm, _, _, _, _, _ := peer.DiagRaftState()
	_ = peer.Handle(proto.Message{Type: proto.RequestVoteResp, From: 2, Term: campaignTerm})
	_ = peer.Handle(proto.Message{Type: proto.RequestVoteResp, From: 3, Term: campaignTerm})

	if !peer.IsLeader() {
		t.Fatal("peer must be leader")
	}

	// Drain becomeLeader messages (no-op append, broadcastAppend).
	_ = peer.DrainMessages()

	// Ack the no-op entry from followers to commit it.
	leaderTerm, _, _, lastIdx, _, _ := peer.DiagRaftState()
	_ = peer.Handle(proto.Message{
		Type: proto.ReplicateResp, From: 2, LogIndex: lastIdx, Term: leaderTerm,
	})
	_ = peer.Handle(proto.Message{
		Type: proto.ReplicateResp, From: 3, LogIndex: lastIdx, Term: leaderTerm,
	})

	// Drain commit-advance messages.
	_ = peer.DrainMessages()

	return peer, lr
}

// TestStepWorker_CommitPendingInbox_ReadIndexResolution verifies that
// the commitPending fast path drains inbox messages and delivers
// ReadIndex results via the OnReadyToRead callback, even while the
// commit/apply pipeline is in-flight. This is the core optimization:
// HeartbeatResp messages that satisfy ReadIndex quorum are processed
// without waiting for commitPending to clear.
func TestStepWorker_CommitPendingInbox_ReadIndexResolution(t *testing.T) {
	sender := newTestSender()
	ws := NewWorkSignal(1)
	commitC := make(chan *commitItem, 16)
	stopC := make(chan struct{})

	cb := newTestCallback()
	sw := newStepWorker(0, time.Millisecond, sender, ws, commitC, cb, stopC, nil)

	// Create a fully-elected 3-node leader peer with the no-op committed.
	peer, lr := newMultiNodeLeaderTestPeer(t, 1, 1)
	cfg := config.Config{ShardID: 1, ReplicaID: 1}
	cfg.SetDefaults()
	testSm := newTestSM()
	rsmSM := newTestApplier(testSm)
	node := NewNode(peer, rsmSM, lr, cfg, nil, "", nil)
	sw.loadNode(node)

	// Drain any residual state from the election through processReady
	// to clear the peer's pending state.
	ws.Notify(1)
	sw.processReady()
	select {
	case item := <-commitC:
		if err := peer.Commit(item.update); err != nil {
			t.Fatalf("initial commit failed: %v", err)
		}
		node.commitPending.Store(false)
	default:
	}

	// Issue a ReadIndex request via the MPSC ReadIndex queue. For a
	// 3-node cluster without LeaseRead, this requires heartbeat quorum
	// confirmation.
	node.DeliverReadIndex(42)

	// Process the ReadIndex: FlushReadIndex broadcasts heartbeats.
	ws.Notify(1)
	sw.processReady()

	// Drain any update (ReadOnly with heartbeat messages).
	select {
	case item := <-commitC:
		if err := peer.Commit(item.update); err != nil {
			t.Fatalf("commit failed: %v", err)
		}
		node.commitPending.Store(false)
	default:
	}

	// Now set commitPending=true to simulate a concurrent Propose
	// in the commit pipeline.
	node.commitPending.Store(true)

	initialReadyCount := cb.readyCount.Load()

	// Deliver HeartbeatResp from both followers while commitPending=true.
	// These carry the ReadIndex ctx and should satisfy quorum when
	// processed by processCommitPendingInbox.
	// HeartbeatResp is network-originated, so it must carry the term.
	hbTerm, _, _, _, _, _ := node.Peer().DiagRaftState()
	node.Deliver(proto.Message{
		Type: proto.HeartbeatResp,
		From: 2,
		Term: hbTerm,
		// The context the leader ACTUALLY issued. Exactly one ReadIndex was
		// delivered above, and handleLeaderReadIndex assigns contexts by
		// incrementing from zero at becomeLeader, so it is 1. A larger value is a
		// message no honest follower could send, and the leader now refuses it.
		Hint: 1,
	})
	node.Deliver(proto.Message{
		Type: proto.HeartbeatResp,
		From: 3,
		Term: hbTerm,
		Hint: 1,
	})

	// Process. commitPending is true, so processCommitPendingInbox runs.
	ws.Notify(1)
	sw.processReady()

	// The HeartbeatResp should have satisfied ReadIndex quorum and
	// produced readStates that are delivered via OnReadyToRead.
	if cb.readyCount.Load() <= initialReadyCount {
		t.Fatal("expected OnReadyToRead to fire during commitPending inbox processing")
	}

	// Verify the inbox was drained.
	remaining := node.DrainInbox(nil)
	if len(remaining) != 0 {
		t.Fatalf("inbox should be empty after commitPending drain, got %d", len(remaining))
	}
}

// TestStepWorker_CommitPendingInbox_NilPeerSkipped verifies that
// processReady during commitPending does not call processCommitPendingInbox
// when the peer is nil.
func TestStepWorker_CommitPendingInbox_NilPeerSkipped(t *testing.T) {
	sender := newTestSender()
	ws := NewWorkSignal(1)
	commitC := make(chan *commitItem, 16)
	stopC := make(chan struct{})

	sw := newStepWorker(0, time.Millisecond, sender, ws, commitC, nil, stopC, nil)

	cfg := config.Config{ShardID: 1, ReplicaID: 1}
	node := NewNode(nil, nil, nil, cfg, nil, "", nil)
	node.commitPending.Store(true)
	sw.loadNode(node)

	// Should not panic even with nil peer and commitPending=true.
	ws.Notify(1)
	sw.processReady()
}

// TestStepWorker_ReadIndexBatching_SingleKey verifies that a single
// ReadIndex key is submitted through the MPSC queue and processed by
// the step worker without triggering OnReadIndexBatched (no aliases).
func TestStepWorker_ReadIndexBatching_SingleKey(t *testing.T) {
	sender := newTestSender()
	ws := NewWorkSignal(1)
	commitC := make(chan *commitItem, 16)
	stopC := make(chan struct{})

	cb := newTestCallback()
	sw := newStepWorker(0, time.Millisecond, sender, ws, commitC, cb, stopC, nil)

	peer, lr := newTestPeer(t, 1, 1)
	cfg := config.Config{ShardID: 1, ReplicaID: 1}
	cfg.SetDefaults()
	node := NewNode(peer, nil, lr, cfg, nil, "", nil)
	sw.loadNode(node)

	tickPeerToLeader(peer)

	// Drain the election update.
	ws.Notify(1)
	sw.processReady()
	select {
	case item := <-commitC:
		if err := peer.Commit(item.update); err != nil {
			t.Fatalf("initial commit failed: %v", err)
		}
		node.commitPending.Store(false)
	default:
	}

	// Deliver a single ReadIndex key via the MPSC queue.
	if !node.DeliverReadIndex(42) {
		t.Fatal("DeliverReadIndex should succeed")
	}

	ws.Notify(1)
	sw.processReady()

	// Single key: no aliases, so OnReadIndexBatched should NOT be called.
	if cb.batchCount.Load() != 0 {
		t.Fatalf("expected 0 OnReadIndexBatched calls for single key, got %d",
			cb.batchCount.Load())
	}

	// The ReadIndex queue should be drained.
	if remaining := node.DrainReadIndex(nil); len(remaining) != 0 {
		t.Fatalf("readIndex queue should be empty, got %d", len(remaining))
	}
}

// TestStepWorker_ReadIndexBatching_NilCallback verifies that ReadIndex
// batch coalescing works without panicking when the callback is nil.
func TestStepWorker_ReadIndexBatching_NilCallback(t *testing.T) {
	sender := newTestSender()
	ws := NewWorkSignal(1)
	commitC := make(chan *commitItem, 16)
	stopC := make(chan struct{})

	// nil callback should not panic.
	sw := newStepWorker(0, time.Millisecond, sender, ws, commitC, nil, stopC, nil)

	peer, lr := newTestPeer(t, 1, 1)
	cfg := config.Config{ShardID: 1, ReplicaID: 1}
	cfg.SetDefaults()
	node := NewNode(peer, nil, lr, cfg, nil, "", nil)
	sw.loadNode(node)

	tickPeerToLeader(peer)

	// Drain the election update.
	ws.Notify(1)
	sw.processReady()
	select {
	case item := <-commitC:
		if err := peer.Commit(item.update); err != nil {
			t.Fatalf("initial commit failed: %v", err)
		}
		node.commitPending.Store(false)
	default:
	}

	// Deliver multiple ReadIndex keys with nil callback.
	for i := uint64(1); i <= 3; i++ {
		node.DeliverReadIndex(i)
	}

	// Should not panic.
	ws.Notify(1)
	sw.processReady()

	// Queue should be drained.
	if remaining := node.DrainReadIndex(nil); len(remaining) != 0 {
		t.Fatalf("readIndex queue should be empty, got %d", len(remaining))
	}
}

// orderingCallback records the order in which OnReadIndexBatched and
// OnReadyToRead fire, allowing tests to verify that aliases are
// registered BEFORE the raft layer produces readStates. Each call
// appends an event to a shared sequence slice under a mutex.
type orderingCallback struct {
	testCallback
	seqMu  sync.Mutex
	events []orderingEvent
}

type orderingEventKind int

const (
	eventBatched     orderingEventKind = 1
	eventReadyToRead orderingEventKind = 2
)

type orderingEvent struct {
	kind     orderingEventKind
	shardID  uint64
	batchKey uint64
	keys     []uint64 // aliasKeys for batched, ReadyToRead keys for readyToRead
}

func newOrderingCallback() *orderingCallback {
	return &orderingCallback{
		testCallback: *newTestCallback(),
	}
}

func (o *orderingCallback) OnReadIndexBatched(shardID uint64, batchKey uint64, aliasKeys []uint64) {
	o.testCallback.OnReadIndexBatched(shardID, batchKey, aliasKeys)
	cp := make([]uint64, len(aliasKeys))
	copy(cp, aliasKeys)
	o.seqMu.Lock()
	o.events = append(o.events, orderingEvent{
		kind:     eventBatched,
		shardID:  shardID,
		batchKey: batchKey,
		keys:     cp,
	})
	o.seqMu.Unlock()
}

func (o *orderingCallback) OnReadyToRead(shardID uint64, readyToRead []proto.ReadyToRead) {
	o.testCallback.OnReadyToRead(shardID, readyToRead)
	keys := make([]uint64, len(readyToRead))
	for i, rtr := range readyToRead {
		keys[i] = rtr.Key
	}
	o.seqMu.Lock()
	o.events = append(o.events, orderingEvent{
		kind:    eventReadyToRead,
		shardID: shardID,
		keys:    keys,
	})
	o.seqMu.Unlock()
}

func (o *orderingCallback) getEvents() []orderingEvent {
	o.seqMu.Lock()
	defer o.seqMu.Unlock()
	result := make([]orderingEvent, len(o.events))
	copy(result, o.events)
	return result
}

var _ Callback = (*orderingCallback)(nil)

// TestStepWorker_ReadIndexBatching_AliasesRegisteredBeforeHandle verifies
// that OnReadIndexBatched fires BEFORE OnReadyToRead in the normal
// processReady path. In a single-node cluster, Handle(ReadIndex)
// resolves immediately, so GetUpdate returns ReadyToRead in the same
// cycle. If aliases were registered after Handle, OnReadyToRead would
// complete the batchKey without resolving the aliases, leaving alias
// callers blocked until context deadline.
func TestStepWorker_ReadIndexBatching_AliasesRegisteredBeforeHandle(t *testing.T) {
	sender := newTestSender()
	ws := NewWorkSignal(1)
	commitC := make(chan *commitItem, 16)
	stopC := make(chan struct{})

	cb := newOrderingCallback()
	sw := newStepWorker(0, time.Millisecond, sender, ws, commitC, cb, stopC, nil)

	// Single-node peer: ReadIndex resolves immediately (no quorum needed).
	peer, lr := newTestPeer(t, 1, 1)
	cfg := config.Config{ShardID: 1, ReplicaID: 1}
	cfg.SetDefaults()
	node := NewNode(peer, nil, lr, cfg, nil, "", nil)
	sw.loadNode(node)

	tickPeerToLeader(peer)

	// Drain the election update.
	ws.Notify(1)
	sw.processReady()
	select {
	case item := <-commitC:
		if err := peer.Commit(item.update); err != nil {
			t.Fatalf("initial commit failed: %v", err)
		}
		node.commitPending.Store(false)
	default:
	}

	// Clear events from election processing.
	cb.seqMu.Lock()
	cb.events = nil
	cb.seqMu.Unlock()

	// Deliver multiple ReadIndex keys.
	for i := uint64(500); i < 505; i++ {
		if !node.DeliverReadIndex(i) {
			t.Fatalf("DeliverReadIndex(%d) should succeed", i)
		}
	}

	ws.Notify(1)
	sw.processReady()

	// Drain any commit item (ReadOnly updates skip the pipeline, but
	// verify the channel is empty).
	select {
	case item := <-commitC:
		if err := peer.Commit(item.update); err != nil {
			t.Fatalf("commit failed: %v", err)
		}
		node.commitPending.Store(false)
	default:
	}

	events := cb.getEvents()

	// One batch, however many callers: the four aliases ride on one key.
	if n := cb.batchCount.Load(); n != 1 {
		t.Fatalf("expected 1 OnReadIndexBatched call, got %d", n)
	}
	if remaining := node.DrainReadIndex(nil); len(remaining) != 0 {
		t.Fatalf("readIndex queue should be empty, got %d", len(remaining))
	}

	// There must be at least one OnReadIndexBatched and one OnReadyToRead.
	var batchedIdx, readyIdx int
	batchedIdx = -1
	readyIdx = -1
	for i, ev := range events {
		if ev.kind == eventBatched && batchedIdx == -1 {
			batchedIdx = i
		}
		if ev.kind == eventReadyToRead && readyIdx == -1 {
			readyIdx = i
		}
	}

	if batchedIdx == -1 {
		t.Fatal("OnReadIndexBatched was never called")
	}
	if readyIdx == -1 {
		t.Fatal("OnReadyToRead was never called")
	}

	// The critical ordering guarantee: OnReadIndexBatched MUST fire
	// before OnReadyToRead. This ensures aliases are in place before
	// the batchKey's ReadIndex result is delivered.
	if batchedIdx >= readyIdx {
		t.Fatalf("OnReadIndexBatched (event %d) must fire BEFORE "+
			"OnReadyToRead (event %d); aliases would be orphaned",
			batchedIdx, readyIdx)
	}

	// Verify the batch content is correct.
	batchEvent := events[batchedIdx]
	if batchEvent.batchKey != 500 {
		t.Errorf("batchKey = %d, want 500", batchEvent.batchKey)
	}
	if len(batchEvent.keys) != 4 {
		t.Fatalf("expected 4 alias keys, got %d", len(batchEvent.keys))
	}
	for i, want := range []uint64{501, 502, 503, 504} {
		if batchEvent.keys[i] != want {
			t.Errorf("aliasKeys[%d] = %d, want %d", i, batchEvent.keys[i], want)
		}
	}

	// Verify the ReadyToRead event contains the batchKey.
	readyEvent := events[readyIdx]
	found := false
	for _, key := range readyEvent.keys {
		if key == 500 {
			found = true
			break
		}
	}
	if !found {
		t.Errorf("OnReadyToRead did not contain batchKey 500; keys: %v",
			readyEvent.keys)
	}
}

// TestStepWorker_ReadIndexBatching_AliasesBeforeHandle_CommitPending
// verifies the same ordering guarantee as the test above, but for the
// commitPending fast path (processCommitPendingInbox). When the
// commit pipeline is in-flight, ReadIndex keys are still drained and
// submitted under raftMu. The aliases must be registered before
// Handle(ReadIndex) to prevent the race.
func TestStepWorker_ReadIndexBatching_AliasesBeforeHandle_CommitPending(t *testing.T) {
	sender := newTestSender()
	ws := NewWorkSignal(1)
	commitC := make(chan *commitItem, 16)
	stopC := make(chan struct{})

	cb := newOrderingCallback()
	sw := newStepWorker(0, time.Millisecond, sender, ws, commitC, cb, stopC, nil)

	// Use a 3-node cluster leader for testing the commitPending path.
	peer, lr := newMultiNodeLeaderTestPeer(t, 1, 1)
	cfg := config.Config{ShardID: 1, ReplicaID: 1}
	cfg.SetDefaults()
	testSm := newTestSM()
	rsmSM := newTestApplier(testSm)
	node := NewNode(peer, rsmSM, lr, cfg, nil, "", nil)
	sw.loadNode(node)

	// Drain election state.
	ws.Notify(1)
	sw.processReady()
	select {
	case item := <-commitC:
		if err := peer.Commit(item.update); err != nil {
			t.Fatalf("initial commit failed: %v", err)
		}
		node.commitPending.Store(false)
	default:
	}

	// Force commitPending to enter the fast path.
	node.commitPending.Store(true)

	// Clear events.
	cb.seqMu.Lock()
	cb.events = nil
	cb.seqMu.Unlock()

	// Deliver multiple ReadIndex keys while commitPending is true.
	for i := uint64(600); i < 604; i++ {
		if !node.DeliverReadIndex(i) {
			t.Fatalf("DeliverReadIndex(%d) should succeed", i)
		}
	}

	ws.Notify(1)
	sw.processReady()

	events := cb.getEvents()

	// In the commitPending path with a multi-node cluster, ReadIndex
	// may not resolve immediately (needs heartbeat quorum). But the
	// OnReadIndexBatched call must still happen, and if OnReadyToRead
	// fires in the same cycle, it must come after.
	batchedIdx := -1
	for i, ev := range events {
		if ev.kind == eventBatched {
			batchedIdx = i
			break
		}
	}
	if batchedIdx == -1 {
		t.Fatal("OnReadIndexBatched was not called during commitPending path")
	}

	// If OnReadyToRead also fired (single-node or quorum already
	// satisfied), verify ordering.
	for i, ev := range events {
		if ev.kind == eventReadyToRead {
			if i <= batchedIdx {
				t.Fatalf("OnReadyToRead (event %d) fired before "+
					"OnReadIndexBatched (event %d) during commitPending",
					i, batchedIdx)
			}
			break
		}
	}

	// Verify batch content.
	batchEvent := events[batchedIdx]
	if batchEvent.batchKey != 600 {
		t.Errorf("batchKey = %d, want 600", batchEvent.batchKey)
	}
	if len(batchEvent.keys) != 3 {
		t.Fatalf("expected 3 alias keys, got %d", len(batchEvent.keys))
	}
	for i, want := range []uint64{601, 602, 603} {
		if batchEvent.keys[i] != want {
			t.Errorf("aliasKeys[%d] = %d, want %d", i, batchEvent.keys[i], want)
		}
	}

	if remaining := node.DrainReadIndex(nil); len(remaining) != 0 {
		t.Fatalf("readIndex queue should be empty, got %d", len(remaining))
	}
}

// --- GAP-1: Message Ordering Relative to WAL Persistence Tests ---

// TestProcessReady_PanicInReadyToRead_ReclaimsDurabilityClaim pins the window
// that Peer.InvalidatePersistedState exists to close.
//
// OnReadyToRead is invoked inline on the step worker, before the update reaches
// any persistence path, and GetUpdate has ALREADY advanced prevState by then. A
// panic there destroys the update while the peer still believes its hard state
// is durable — after which every subsequent update is classified ReadOnly and
// skips SaveState, so a granted vote is answered but never written.
//
// The engine's restart loop calls discardExtractedUpdate on the unwind; this
// asserts the worker has recorded which node needs that treatment, and that
// applying it re-arms the peer.
func TestProcessReady_PanicInReadyToRead_ReclaimsDurabilityClaim(t *testing.T) {
	sender := newTestSender()
	ws := NewWorkSignal(1)
	commitC := make(chan *commitItem, 16)
	stopC := make(chan struct{})
	cb := newTestCallback()
	sw := newStepWorker(0, time.Millisecond, sender, ws, commitC, cb, stopC, nil)

	peer, lr := newMultiNodeLeaderTestPeer(t, 1, 1)
	cfg := config.Config{ShardID: 1, ReplicaID: 1}
	cfg.SetDefaults()
	node := NewNode(peer, newTestApplier(newTestSM()), lr, cfg, nil, "", nil)
	sw.loadNode(node)

	ws.Notify(1)
	sw.processReady()
	select {
	case item := <-commitC:
		if err := peer.Commit(item.update); err != nil {
			t.Fatalf("initial commit failed: %v", err)
		}
		node.commitPending.Store(false)
	default:
	}

	// Produce an update carrying ReadyToRead: a ReadIndex confirmed by
	// heartbeat quorum.
	node.DeliverReadIndex(42)
	ws.Notify(1)
	sw.processReady()
	select {
	case item := <-commitC:
		if err := peer.Commit(item.update); err != nil {
			t.Fatalf("commit failed: %v", err)
		}
		node.commitPending.Store(false)
	default:
	}
	hbTerm, _, _, _, _, _ := node.Peer().DiagRaftState()
	node.Deliver(proto.Message{Type: proto.HeartbeatResp, From: 2, Term: hbTerm, Hint: 1})
	node.Deliver(proto.Message{Type: proto.HeartbeatResp, From: 3, Term: hbTerm, Hint: 1})

	cb.panicOnReadyToRead = true

	panicked := func() (p bool) {
		defer func() {
			if r := recover(); r != nil {
				p = true
			}
		}()
		ws.Notify(1)
		sw.processReady()
		return false
	}()
	if !panicked {
		t.Fatal("OnReadyToRead did not fire, so this test never entered the " +
			"window it is meant to cover")
	}

	// The update is gone. The worker must know whose durability claim went
	// with it — enqueuingNode is NOT set here, because the panic unwound
	// before the hand-off ever began.
	if sw.extractedNode != node {
		t.Fatalf("extractedNode = %v, want the panicking node; the recovery "+
			"has no way to know the hard state was never persisted",
			sw.extractedNode)
	}

	// What the engine's restart loop does.
	sw.discardExtractedUpdate()

	if !node.peer.HasUpdate(node.lastApplied.Load()) {
		t.Error("the peer reports no work after losing an unpersisted update, " +
			"so the step worker never re-extracts and the hard state is " +
			"never written")
	}
	next, err := node.peer.GetUpdate(true, node.lastApplied.Load())
	if err != nil {
		t.Fatalf("GetUpdate after recovery: %v", err)
	}
	if next.ReadOnly {
		t.Error("the update after the lost one is ReadOnly, so it bypasses " +
			"SaveState and the hard state never reaches the WAL")
	}
}

// persistBeforeRespondFixture is the message set both split sites are driven
// with below. The classification is written out as LITERAL message types on
// purpose.
//
// Every pre-existing test of this property partitioned its observations with
// proto.IsFreeOrderMessage — the same predicate the production code branches
// on — so the test's expectation moved in lockstep with the behaviour it was
// meant to pin. Inverting the split in enqueueForCommit sent every
// RequestVoteResp before its fsync and the entire package still passed. That
// mutation is two-leaders-in-one-term: a follower that answers a vote request
// before the grant is durable can crash, forget the vote, and grant it again
// to a different candidate in the same term.
//
// Nothing in this file may ask proto.IsFreeOrderMessage what the answer is.
var persistBeforeRespondFixture = struct {
	messages []proto.Message
	// freeOrder may go out before the state behind them is durable.
	freeOrder []uint64
	// ordered MUST NOT be transmitted until SaveState/Commit has returned.
	ordered []uint64
}{
	messages: []proto.Message{
		{Type: proto.Replicate, From: 1, To: 2},
		{Type: proto.RequestVoteResp, From: 1, To: 3},
		{Type: proto.Heartbeat, From: 1, To: 4},
		{Type: proto.HeartbeatResp, From: 1, To: 5},
		{Type: proto.ReplicateResp, From: 1, To: 6},
		{Type: proto.Quiesce, From: 1, To: 7},
	},
	freeOrder: []uint64{proto.Replicate, proto.Heartbeat, proto.Quiesce},
	ordered: []uint64{
		proto.RequestVoteResp, proto.HeartbeatResp, proto.ReplicateResp,
	},
}

// typeNames renders a message slice as its type names, for failure output.
func typeNames(msgs []proto.Message) []string {
	out := make([]string, len(msgs))
	for i := range msgs {
		out[i] = proto.MessageTypeName(msgs[i].Type)
	}
	return out
}

// containsType reports whether msgs holds a message of type t.
func containsType(msgs []proto.Message, t uint64) bool {
	for i := range msgs {
		if msgs[i].Type == t {
			return true
		}
	}
	return false
}

// TestEnqueueForCommit_OrderedMessagesWithheldUntilPersist drives the real
// WAL-mode split site with a synthetic update carrying both classes, and
// asserts the outcome against the literal fixture above.
//
// This is the test that fails when the split predicate in enqueueForCommit is
// inverted, and it is the only one in the package that does.
func TestEnqueueForCommit_OrderedMessagesWithheldUntilPersist(t *testing.T) {
	sender := newTestSender()
	ws := NewWorkSignal(1)
	// Buffered by one: enqueueForCommit hands the item over and returns, so
	// everything observed on the sender afterwards was transmitted BEFORE the
	// commit worker could possibly have called SaveState.
	commitC := make(chan *commitItem, 1)
	stopC := make(chan struct{})
	cb := newTestCallback()
	sw := newStepWorker(0, time.Millisecond, sender, ws, commitC, cb, stopC, nil)

	peer, lr := newTestPeer(t, 1, 1)
	cfg := config.Config{ShardID: 1, ReplicaID: 1}
	cfg.SetDefaults()
	node := NewNode(peer, newTestApplier(newTestSM()), lr, cfg, nil, "", nil)

	update := proto.Update{ShardID: 1}
	update.Messages = append(update.Messages, persistBeforeRespondFixture.messages...)

	if sw.enqueueForCommit(1, node, update) {
		t.Fatal("enqueueForCommit reported shutdown on a live stopC")
	}

	var item *commitItem
	select {
	case item = <-commitC:
	default:
		t.Fatal("enqueueForCommit did not queue a commitItem")
	}

	sent := sender.sentMessages()

	// The safety property: nothing ordered may have left the node yet.
	for _, want := range persistBeforeRespondFixture.ordered {
		if containsType(sent, want) {
			t.Errorf("%s was transmitted BEFORE the state behind it was persisted; "+
				"a crash here re-grants the same term. sent=%v",
				proto.MessageTypeName(want), typeNames(sent))
		}
		if !containsType(item.orderedMsgs, want) {
			t.Errorf("%s missing from commitItem.orderedMsgs, so it is never sent at all; "+
				"orderedMsgs=%v", proto.MessageTypeName(want), typeNames(item.orderedMsgs))
		}
	}

	// The liveness property: free-order traffic must NOT be held behind fsync,
	// or replication stalls for a disk round trip on every update.
	for _, want := range persistBeforeRespondFixture.freeOrder {
		if !containsType(sent, want) {
			t.Errorf("%s was withheld until after persist; replication now waits on "+
				"fsync. sent=%v", proto.MessageTypeName(want), typeNames(sent))
		}
		if containsType(item.orderedMsgs, want) {
			t.Errorf("%s was deferred into orderedMsgs; it belongs on the fast path. "+
				"orderedMsgs=%v", proto.MessageTypeName(want), typeNames(item.orderedMsgs))
		}
	}
}

// TestCommitMemoryMode_OrderedMessagesSentAfterCommit is the memory-mode twin.
//
// Memory mode has no fsync, so the split is observable as two distinct SEND
// BATCHES either side of peer.Commit: batch 0 before, batch 1 after. The
// existing coverage of this site checked batch membership with
// proto.IsFreeOrderMessage, so inverting the split moved test and code
// together; this compares each batch against the literal fixture instead.
func TestCommitMemoryMode_OrderedMessagesSentAfterCommit(t *testing.T) {
	sender := newSequenceSender()
	ws := NewWorkSignal(1)
	commitC := make(chan *commitItem, 16)
	applyC := make(chan *applyItem, 16)
	stopC := make(chan struct{})

	sw := newStepWorker(0, time.Millisecond, sender, ws, commitC, nil, stopC, nil)
	sw.memoryMode = true
	sw.applyC = applyC
	sw.applyPool = &sync.Pool{New: func() interface{} { return &applyItem{} }}

	peer, lr := newTestPeer(t, 1, 1)
	cfg := config.Config{ShardID: 1, ReplicaID: 1}
	cfg.SetDefaults()
	node := NewNode(peer, newTestApplier(newTestSM()), lr, cfg, nil, "", nil)
	sw.loadNode(node)

	tickPeerToLeader(peer)

	ws.Notify(1)
	sw.processReady()
	select {
	case <-applyC:
		node.commitPending.Store(false)
	default:
	}
	sender.mu.Lock()
	sender.events = sender.events[:0]
	sender.seq.Store(0)
	sender.mu.Unlock()

	// A real update from the peer, so Commit succeeds; the messages on it are
	// then replaced with the fixture.
	node.DeliverProposal([]proto.Entry{{Cmd: []byte("persist-order")}})
	ws.Notify(1)
	_ = ws.Swap(0)
	if buf := node.DrainProposals(nil); len(buf) > 0 {
		_ = peer.Handle(proto.Message{
			Type: proto.Propose, Origin: proto.OriginLocal, Entries: buf,
		})
	}
	peer.FlushReadIndex()
	if !peer.HasUpdate(node.lastApplied.Load()) {
		t.Fatal("peer should have an update after propose")
	}
	update, err := peer.GetUpdate(true, node.lastApplied.Load())
	if err != nil {
		t.Fatalf("GetUpdate failed: %v", err)
	}
	update.Messages = append(update.Messages[:0],
		persistBeforeRespondFixture.messages...)

	if sw.commitMemoryMode(node, update) {
		t.Fatal("commitMemoryMode reported shutdown on a live stopC")
	}

	events := sender.getEvents()
	if len(events) != 2 {
		t.Fatalf("expected 2 send batches (free-order before Commit, ordered "+
			"after), got %d", len(events))
	}

	// Batch 0 goes out before peer.Commit: it must hold the free-order set and
	// nothing else. An ordered message here has outrun the state it reports.
	for _, want := range persistBeforeRespondFixture.ordered {
		if containsType(events[0].msgs, want) {
			t.Errorf("%s was sent BEFORE peer.Commit applied the state it "+
				"acknowledges; pre-commit batch=%v",
				proto.MessageTypeName(want), typeNames(events[0].msgs))
		}
		if !containsType(events[1].msgs, want) {
			t.Errorf("%s never reached the post-commit batch=%v",
				proto.MessageTypeName(want), typeNames(events[1].msgs))
		}
	}
	for _, want := range persistBeforeRespondFixture.freeOrder {
		if !containsType(events[0].msgs, want) {
			t.Errorf("%s was held back until after Commit; pre-commit batch=%v",
				proto.MessageTypeName(want), typeNames(events[0].msgs))
		}
		if containsType(events[1].msgs, want) {
			t.Errorf("%s was deferred to the post-commit batch=%v",
				proto.MessageTypeName(want), typeNames(events[1].msgs))
		}
	}
}

// TestProcessReady_WALMode_EmptyMessages verifies that when an update
// has no messages, the orderedMsgs slice is properly reset and no
// messages are sent.
func TestProcessReady_WALMode_EmptyMessages(t *testing.T) {
	sender := newTestSender()
	ws := NewWorkSignal(1)
	commitC := make(chan *commitItem, 16)
	stopC := make(chan struct{})

	cb := newTestCallback()
	sw := newStepWorker(0, time.Millisecond, sender, ws, commitC, cb, stopC, nil)

	peer, lr := newTestPeer(t, 1, 1)
	cfg := config.Config{ShardID: 1, ReplicaID: 1}
	cfg.SetDefaults()
	testSm := newTestSM()
	rsmSM := newTestApplier(testSm)
	node := NewNode(peer, rsmSM, lr, cfg, nil, "", nil)
	sw.loadNode(node)

	tickPeerToLeader(peer)

	// Drain the election update.
	ws.Notify(1)
	sw.processReady()
	select {
	case item := <-commitC:
		if err := peer.Commit(item.update); err != nil {
			t.Fatalf("initial commit failed: %v", err)
		}
		node.commitPending.Store(false)
	default:
	}

	// Pre-populate the orderedMsgs in the commit slot to verify it
	// gets cleared when the update has no messages.
	slot := sw.getCommitSlot(1)
	slot.orderedMsgs = append(slot.orderedMsgs, proto.Message{Type: proto.RequestVoteResp})

	// Propose to create an update.
	node.DeliverProposal([]proto.Entry{{Cmd: []byte("empty-msgs-test")}})
	ws.Notify(1)
	sw.processReady()

	select {
	case item := <-commitC:
		// orderedMsgs should be empty (reset from prior cycle).
		if len(item.orderedMsgs) != 0 {
			t.Fatalf("expected empty orderedMsgs, got %d messages",
				len(item.orderedMsgs))
		}
	case <-time.After(time.Second):
		t.Fatal("expected a commit item")
	}
}

// TestTick_CommitPending_FiltersFreeOrderOnly verifies that during
// commitPending, tick() only sends free-order messages and filters out
// any non-free-order messages as defense-in-depth.
func TestTick_CommitPending_FiltersFreeOrderOnly(t *testing.T) {
	sender := newTestSender()
	ws := NewWorkSignal(1)
	commitC := make(chan *commitItem, 16)
	stopC := make(chan struct{})

	sw := newStepWorker(0, time.Millisecond, sender, ws, commitC, nil, stopC, nil)

	// A 3-node leader, so its ticks produce heartbeats to send: on a
	// single-node shard tick() sends nothing and the check below is empty.
	peer, lr := newMultiNodeLeaderTestPeer(t, 1, 1)
	cfg := config.Config{ShardID: 1, ReplicaID: 1}
	cfg.SetDefaults()
	node := NewNode(peer, nil, lr, cfg, nil, "", nil)
	sw.loadNode(node)

	// commitPending: processReady skips the shard, so tick() is the only
	// sender of the heartbeats it produces.
	node.commitPending.Store(true)
	for range 30 {
		sw.tick()
	}

	sent := sender.sentMessages()
	if !containsType(sent, proto.Heartbeat) {
		t.Fatalf("tick() during commitPending sent no heartbeat; sent=%v", typeNames(sent))
	}
	for _, ordered := range persistBeforeRespondFixture.ordered {
		if containsType(sent, ordered) {
			t.Errorf("%s sent by tick() during commitPending, before its state was persisted; sent=%v",
				proto.MessageTypeName(ordered), typeNames(sent))
		}
	}
}

// --- Memory-Mode Pipeline Collapse Tests ---

// sequenceSender records each Send/SendSnapshot call as a numbered event,
// allowing tests to verify the temporal ordering of message sends relative
// to other operations (e.g., Peer.Commit).
type sequenceSender struct {
	mu     sync.Mutex
	events []sendEvent
	seq    atomic.Int64
}

type sendEvent struct {
	seqNum int64
	msgs   []proto.Message
}

func newSequenceSender() *sequenceSender {
	return &sequenceSender{}
}

func (s *sequenceSender) Send(msgs []proto.Message) {
	cp := make([]proto.Message, len(msgs))
	copy(cp, msgs)
	n := s.seq.Add(1)
	s.mu.Lock()
	s.events = append(s.events, sendEvent{seqNum: n, msgs: cp})
	s.mu.Unlock()
}

func (s *sequenceSender) SendSnapshot(msg proto.Message) {
	n := s.seq.Add(1)
	s.mu.Lock()
	s.events = append(s.events, sendEvent{seqNum: n, msgs: []proto.Message{msg}})
	s.mu.Unlock()
}

func (s *sequenceSender) getEvents() []sendEvent {
	s.mu.Lock()
	defer s.mu.Unlock()
	result := make([]sendEvent, len(s.events))
	copy(result, s.events)
	return result
}

var _ MessageSender = (*sequenceSender)(nil)

// TestCommitMemoryMode_NoMessages verifies that commitMemoryMode does not
// panic or send any messages when the update has an empty Messages slice.
func TestCommitMemoryMode_NoMessages(t *testing.T) {
	sender := newSequenceSender()
	ws := NewWorkSignal(1)
	commitC := make(chan *commitItem, 16)
	applyC := make(chan *applyItem, 16)
	applyPool := &sync.Pool{New: func() interface{} { return &applyItem{} }}
	stopC := make(chan struct{})

	sw := newStepWorker(0, time.Millisecond, sender, ws, commitC, nil, stopC, nil)
	sw.memoryMode = true
	sw.applyC = applyC
	sw.applyPool = applyPool

	peer, lr := newTestPeer(t, 1, 1)
	cfg := config.Config{ShardID: 1, ReplicaID: 1}
	cfg.SetDefaults()
	testSm := newTestSM()
	rsmSM := newTestApplier(testSm)
	node := NewNode(peer, rsmSM, lr, cfg, nil, "", nil)
	sw.loadNode(node)

	tickPeerToLeader(peer)

	// Drain the election update.
	ws.Notify(1)
	sw.processReady()
	select {
	case <-applyC:
		node.commitPending.Store(false)
	default:
	}
	sender.mu.Lock()
	sender.events = sender.events[:0]
	sender.seq.Store(0)
	sender.mu.Unlock()

	// Propose to get a valid update.
	node.DeliverProposal([]proto.Entry{{Cmd: []byte("no-msgs-test")}})
	buf := node.DrainProposals(nil)
	if len(buf) > 0 {
		_ = peer.Handle(proto.Message{
			Type: proto.Propose, Origin: proto.OriginLocal,
			Entries: buf,
		})
	}
	peer.FlushReadIndex()

	update, err := peer.GetUpdate(true, node.lastApplied.Load())
	if err != nil {
		t.Fatalf("GetUpdate failed: %v", err)
	}

	// Ensure no messages in the update.
	update.Messages = nil

	stopped := sw.commitMemoryMode(node, update)
	if stopped {
		t.Fatal("commitMemoryMode should not return stopped")
	}

	// No messages should have been sent.
	events := sender.getEvents()
	if len(events) != 0 {
		t.Fatalf("expected 0 send events, got %d", len(events))
	}

	// Verify the update was still forwarded to applyC.
	select {
	case ai := <-applyC:
		if ai.node != node {
			t.Fatal("apply item should reference the correct node")
		}
	default:
		t.Fatal("expected an apply item in applyC")
	}
}

// TestCommitMemoryMode_StoppingStepTierStillHandsOff: with a free apply slot
// and the step tier stopping, the hand-off select chose at random between the
// send and the stop, dropping a committed update about half the time although
// the apply tier would still have applied it. The send is tried first; every
// update of twenty reaches applyC.
func TestCommitMemoryMode_StoppingStepTierStillHandsOff(t *testing.T) {
	sender := newSequenceSender()
	ws := NewWorkSignal(1)
	commitC := make(chan *commitItem, 16)
	applyC := make(chan *applyItem, 16)
	applyPool := &sync.Pool{New: func() interface{} { return &applyItem{} }}
	stopC := make(chan struct{})

	sw := newStepWorker(0, time.Millisecond, sender, ws, commitC, nil, stopC, nil)
	sw.memoryMode = true
	sw.applyC = applyC
	sw.applyPool = applyPool

	peer, lr := newTestPeer(t, 1, 1)
	cfg := config.Config{ShardID: 1, ReplicaID: 1}
	cfg.SetDefaults()
	node := NewNode(peer, newTestApplier(newTestSM()), lr, cfg, nil, "", nil)
	sw.loadNode(node)
	tickPeerToLeader(peer)
	ws.Notify(1)
	sw.processReady()
	select {
	case <-applyC:
		node.commitPending.Store(false)
	default:
	}

	close(stopC) // the step tier is stopping; the apply tier is still live
	for i := range 20 {
		node.DeliverProposal([]proto.Entry{{Cmd: []byte("stopping")}})
		if buf := node.DrainProposals(nil); len(buf) > 0 {
			_ = peer.Handle(proto.Message{Type: proto.Propose, Origin: proto.OriginLocal, Entries: buf})
		}
		peer.FlushReadIndex()
		update, err := peer.GetUpdate(true, node.lastApplied.Load())
		if err != nil {
			t.Fatalf("GetUpdate %d: %v", i, err)
		}
		update.Messages = nil
		sw.commitMemoryMode(node, update)
		select {
		case <-applyC:
			node.commitPending.Store(false)
		default:
			t.Fatalf("update %d was dropped although applyC had room: the stopping step tier won the select", i)
		}
	}
}

// TestCommitMemoryMode_OnlyFreeOrderMessages verifies that when all
// messages are free-order, they are all sent before Commit and no second
// send event occurs.
func TestCommitMemoryMode_OnlyFreeOrderMessages(t *testing.T) {
	sender := newSequenceSender()
	ws := NewWorkSignal(1)
	commitC := make(chan *commitItem, 16)
	applyC := make(chan *applyItem, 16)
	applyPool := &sync.Pool{New: func() interface{} { return &applyItem{} }}
	stopC := make(chan struct{})

	sw := newStepWorker(0, time.Millisecond, sender, ws, commitC, nil, stopC, nil)
	sw.memoryMode = true
	sw.applyC = applyC
	sw.applyPool = applyPool

	peer, lr := newTestPeer(t, 1, 1)
	cfg := config.Config{ShardID: 1, ReplicaID: 1}
	cfg.SetDefaults()
	testSm := newTestSM()
	rsmSM := newTestApplier(testSm)
	node := NewNode(peer, rsmSM, lr, cfg, nil, "", nil)
	sw.loadNode(node)

	tickPeerToLeader(peer)

	// Drain election.
	ws.Notify(1)
	sw.processReady()
	select {
	case <-applyC:
		node.commitPending.Store(false)
	default:
	}
	sender.mu.Lock()
	sender.events = sender.events[:0]
	sender.seq.Store(0)
	sender.mu.Unlock()

	// Get a valid update.
	node.DeliverProposal([]proto.Entry{{Cmd: []byte("all-free-test")}})
	buf := node.DrainProposals(nil)
	_ = peer.Handle(proto.Message{
		Type: proto.Propose, Origin: proto.OriginLocal,
		Entries: buf,
	})
	peer.FlushReadIndex()

	update, err := peer.GetUpdate(true, node.lastApplied.Load())
	if err != nil {
		t.Fatalf("GetUpdate failed: %v", err)
	}

	// Only free-order messages.
	update.Messages = []proto.Message{
		{Type: proto.Replicate, ShardID: 1, From: 1, To: 2},
		{Type: proto.Heartbeat, ShardID: 1, From: 1, To: 3},
		{Type: proto.Ping, ShardID: 1, From: 1, To: 4},
	}

	stopped := sw.commitMemoryMode(node, update)
	if stopped {
		t.Fatal("commitMemoryMode should not return stopped")
	}

	events := sender.getEvents()
	// Only 1 send event (all free-order, no ordered to send after Commit).
	if len(events) != 1 {
		t.Fatalf("expected 1 send event (all free-order), got %d", len(events))
	}
	if len(events[0].msgs) != 3 {
		t.Fatalf("expected 3 free-order messages, got %d", len(events[0].msgs))
	}
	for _, msg := range events[0].msgs {
		if !proto.IsFreeOrderMessage(msg.Type) {
			t.Errorf("unexpected ordered message %s in free-order-only batch",
				proto.MessageTypeName(msg.Type))
		}
	}

	// Verify applyC received the item.
	select {
	case <-applyC:
	default:
		t.Fatal("expected an apply item in applyC")
	}
}

// TestCommitMemoryMode_OnlyOrderedMessages verifies that when all messages
// are ordered, no messages are sent before Commit and all are sent after.
func TestCommitMemoryMode_OnlyOrderedMessages(t *testing.T) {
	sender := newSequenceSender()
	ws := NewWorkSignal(1)
	commitC := make(chan *commitItem, 16)
	applyC := make(chan *applyItem, 16)
	applyPool := &sync.Pool{New: func() interface{} { return &applyItem{} }}
	stopC := make(chan struct{})

	sw := newStepWorker(0, time.Millisecond, sender, ws, commitC, nil, stopC, nil)
	sw.memoryMode = true
	sw.applyC = applyC
	sw.applyPool = applyPool

	peer, lr := newTestPeer(t, 1, 1)
	cfg := config.Config{ShardID: 1, ReplicaID: 1}
	cfg.SetDefaults()
	testSm := newTestSM()
	rsmSM := newTestApplier(testSm)
	node := NewNode(peer, rsmSM, lr, cfg, nil, "", nil)
	sw.loadNode(node)

	tickPeerToLeader(peer)

	// Drain election.
	ws.Notify(1)
	sw.processReady()
	select {
	case <-applyC:
		node.commitPending.Store(false)
	default:
	}
	sender.mu.Lock()
	sender.events = sender.events[:0]
	sender.seq.Store(0)
	sender.mu.Unlock()

	// Get a valid update.
	node.DeliverProposal([]proto.Entry{{Cmd: []byte("all-ordered-test")}})
	buf := node.DrainProposals(nil)
	_ = peer.Handle(proto.Message{
		Type: proto.Propose, Origin: proto.OriginLocal,
		Entries: buf,
	})
	peer.FlushReadIndex()

	update, err := peer.GetUpdate(true, node.lastApplied.Load())
	if err != nil {
		t.Fatalf("GetUpdate failed: %v", err)
	}

	// Only ordered messages.
	update.Messages = []proto.Message{
		{Type: proto.RequestVoteResp, ShardID: 1, From: 1, To: 2},
		{Type: proto.HeartbeatResp, ShardID: 1, From: 1, To: 3},
		{Type: proto.ReplicateResp, ShardID: 1, From: 1, To: 4},
	}

	stopped := sw.commitMemoryMode(node, update)
	if stopped {
		t.Fatal("commitMemoryMode should not return stopped")
	}

	events := sender.getEvents()
	// Only 1 send event (all ordered, sent after Commit).
	if len(events) != 1 {
		t.Fatalf("expected 1 send event (all ordered after Commit), got %d", len(events))
	}
	if len(events[0].msgs) != 3 {
		t.Fatalf("expected 3 ordered messages, got %d", len(events[0].msgs))
	}
	for _, msg := range events[0].msgs {
		if proto.IsFreeOrderMessage(msg.Type) {
			t.Errorf("unexpected free-order message %s in ordered-only batch",
				proto.MessageTypeName(msg.Type))
		}
	}

	// Verify applyC received the item.
	select {
	case <-applyC:
	default:
		t.Fatal("expected an apply item in applyC")
	}
}

// ---------------------------------------------------------------------------
// InstallSnapshot deferral during commitPending tests
// ---------------------------------------------------------------------------

// TestStepWorker_CommitPendingInbox_DefersInstallSnapshotButProcessesOthers
// verifies that when the inbox contains a mix of InstallSnapshot and other
// message types during commitPending, only InstallSnapshot is deferred.
// Other messages (HeartbeatResp, ReadIndexResp, etc.) are processed normally
// to maintain ReadIndex quorum resolution without delay.
func TestStepWorker_CommitPendingInbox_DefersInstallSnapshotButProcessesOthers(t *testing.T) {
	sender := newTestSender()
	ws := NewWorkSignal(1)
	commitC := make(chan *commitItem, 16)
	stopC := make(chan struct{})

	cb := newTestCallback()
	sw := newStepWorker(0, time.Millisecond, sender, ws, commitC, cb, stopC, nil)

	peer, lr := newMultiNodeLeaderTestPeer(t, 1, 1)
	cfg := config.Config{ShardID: 1, ReplicaID: 1}
	cfg.SetDefaults()
	testSm := newTestSM()
	rsmSM := newTestApplier(testSm)
	node := NewNode(peer, rsmSM, lr, cfg, nil, "", nil)
	sw.loadNode(node)

	// Drain election state.
	ws.Notify(1)
	sw.processReady()
	select {
	case item := <-commitC:
		if err := peer.Commit(item.update); err != nil {
			t.Fatalf("initial commit failed: %v", err)
		}
		node.commitPending.Store(false)
	default:
	}

	// Issue a ReadIndex so we can observe quorum resolution.
	node.DeliverReadIndex(77)
	ws.Notify(1)
	sw.processReady()
	select {
	case item := <-commitC:
		if err := peer.Commit(item.update); err != nil {
			t.Fatalf("readindex commit failed: %v", err)
		}
		node.commitPending.Store(false)
	default:
	}

	// Now set commitPending and deliver a mix of messages.
	node.commitPending.Store(true)
	initialReadyCount := cb.readyCount.Load()

	// Stamp the leader's current term: raft rejects network messages that
	// arrive with Term 0, so an unstamped HeartbeatResp never reaches the
	// ReadIndex quorum logic this test is exercising.
	curTerm, _, _, _, _, _ := peer.DiagRaftState()

	// HeartbeatResp from both followers to satisfy ReadIndex quorum.
	node.Deliver(proto.Message{
		Type: proto.HeartbeatResp,
		From: 2,
		Term: curTerm,
		Hint: 1,
	})
	node.DeliverLocal(proto.Message{
		Type: proto.InstallSnapshot, Origin: proto.OriginLocal,
		From:    2,
		To:      1,
		ShardID: 1,
		Term:    curTerm,
		Snapshot: proto.Snapshot{
			Index: 500,
			Term:  10,
		},
	})
	node.Deliver(proto.Message{
		Type: proto.HeartbeatResp,
		From: 3,
		Term: curTerm,
		Hint: 1,
	})

	ws.Notify(1)
	sw.processReady()

	// HeartbeatResp messages should have been processed (ReadIndex quorum).
	if cb.readyCount.Load() <= initialReadyCount {
		t.Fatal("expected OnReadyToRead to fire from HeartbeatResp during commitPending")
	}

	// The InstallSnapshot should be buffered in deferredSnapshots.
	remaining := node.DrainInbox(nil)
	if len(remaining) != 0 {
		t.Fatalf("expected 0 messages in inbox (InstallSnapshot buffered), got %d", len(remaining))
	}
	deferred := sw.deferredSnapshots[1]
	if len(deferred) != 1 {
		t.Fatalf("expected 1 deferred snapshot, got %d", len(deferred))
	}
	if deferred[0].Type != proto.InstallSnapshot {
		t.Fatalf("deferred message type = %s, want InstallSnapshot",
			proto.MessageTypeName(deferred[0].Type))
	}
	if deferred[0].Snapshot.Index != 500 {
		t.Errorf("deferred snapshot index = %d, want 500", deferred[0].Snapshot.Index)
	}
}

// TestStepWorker_RedeliverDeferredSnapshots_RetainsUndelivered verifies
// that redeliverDeferredSnapshots only drops messages that were
// successfully delivered. When the node's inbox is full (Deliver returns
// false), the deferred InstallSnapshot must be retained for a later retry
// rather than silently dropped.
func TestStepWorker_RedeliverDeferredSnapshots_RetainsUndelivered(t *testing.T) {
	sender := newTestSender()
	ws := NewWorkSignal(1)
	commitC := make(chan *commitItem, 16)
	stopC := make(chan struct{})
	sw := newStepWorker(0, time.Millisecond, sender, ws, commitC, nil, stopC, nil)

	cfg := config.Config{ShardID: 1, ReplicaID: 1}
	cfg.SetDefaults()
	node := NewNode(nil, nil, nil, cfg, nil, "", nil)
	sw.loadNode(node)

	// Fill the inbox to capacity so the next Deliver returns false.
	full := false
	for i := 0; i < defaultInboxQueueSize+1; i++ {
		if !node.Deliver(proto.Message{Type: proto.Heartbeat}) {
			full = true
			break
		}
	}
	if !full {
		t.Fatal("failed to saturate inbox queue")
	}

	// Seed a deferred InstallSnapshot for the shard.
	snap := proto.Message{
		Type: proto.InstallSnapshot, Origin: proto.OriginLocal,
		From:     2,
		To:       1,
		ShardID:  1,
		Snapshot: proto.Snapshot{Index: 500, Term: 10},
	}
	sw.deferredSnapshots[1] = []proto.Message{snap}

	// Redeliver: the inbox is full, so the snapshot must be retained.
	sw.redeliverDeferredSnapshots(1, node)
	if got := len(sw.deferredSnapshots[1]); got != 1 {
		t.Fatalf("deferred snapshot dropped while inbox full: len = %d, want 1", got)
	}
	if sw.deferredSnapshots[1][0].Snapshot.Index != 500 {
		t.Fatalf("retained snapshot index = %d, want 500",
			sw.deferredSnapshots[1][0].Snapshot.Index)
	}

	// Drain the inbox so there is room, then redeliver succeeds and the
	// deferred buffer is cleared.
	_ = node.DrainInbox(nil)
	sw.redeliverDeferredSnapshots(1, node)
	if got := len(sw.deferredSnapshots[1]); got != 0 {
		t.Fatalf("deferred snapshot not cleared after successful redelivery: len = %d, want 0", got)
	}
	delivered := node.DrainInbox(nil)
	foundSnap := false
	for i := range delivered {
		if delivered[i].Type == proto.InstallSnapshot && delivered[i].Snapshot.Index == 500 {
			foundSnap = true
		}
	}
	if !foundSnap {
		t.Fatal("InstallSnapshot was not redelivered to the inbox")
	}
}

// TestStepWorker_CommitPendingInbox_OnlyInstallSnapshotDefersAll verifies
// that when the inbox contains ONLY InstallSnapshot messages during
// commitPending, they are deferred and processCommitPendingInbox returns
// early without acquiring raftMu. Only the highest-index snapshot is kept:
// older ones are strictly superseded, and retaining every leader
// retransmission would grow the buffer without bound.
func TestStepWorker_CommitPendingInbox_OnlyInstallSnapshotDefersAll(t *testing.T) {
	sender := newTestSender()
	ws := NewWorkSignal(1)
	commitC := make(chan *commitItem, 16)
	stopC := make(chan struct{})

	sw := newStepWorker(0, time.Millisecond, sender, ws, commitC, nil, stopC, nil)

	peer, lr := newTestPeer(t, 1, 1)
	cfg := config.Config{ShardID: 1, ReplicaID: 1}
	cfg.SetDefaults()
	node := NewNode(peer, nil, lr, cfg, nil, "", nil)
	node.commitPending.Store(true)
	sw.loadNode(node)

	tickPeerToLeader(peer)

	// Deliver two InstallSnapshot messages (process-local, as the snapshot
	// receive path builds them).
	node.DeliverLocal(proto.Message{
		Type: proto.InstallSnapshot, Origin: proto.OriginLocal,
		From:     2,
		To:       1,
		ShardID:  1,
		Snapshot: proto.Snapshot{Index: 50, Term: 3},
	})
	node.DeliverLocal(proto.Message{
		Type: proto.InstallSnapshot, Origin: proto.OriginLocal,
		From:     3,
		To:       1,
		ShardID:  1,
		Snapshot: proto.Snapshot{Index: 75, Term: 4},
	})

	// Process with commitPending true.
	ws.Notify(1)
	sw.processReady()

	// Both InstallSnapshot messages leave the inbox; the buffer retains only
	// the highest-index one.
	remaining := node.DrainInbox(nil)
	if len(remaining) != 0 {
		t.Fatalf("expected 0 messages in inbox (InstallSnapshot buffered), got %d", len(remaining))
	}
	deferred := sw.deferredSnapshots[1]
	if len(deferred) != 1 {
		t.Fatalf("expected 1 deferred snapshot (superseded), got %d", len(deferred))
	}
	if deferred[0].Snapshot.Index != 75 {
		t.Fatalf("deferred snapshot index = %d, want 75 (highest)", deferred[0].Snapshot.Index)
	}
	for _, msg := range deferred {
		if msg.Type != proto.InstallSnapshot {
			t.Errorf("deferred message type = %s, want InstallSnapshot",
				proto.MessageTypeName(msg.Type))
		}
	}
}

// TestStepWorker_InstallSnapshot_ProcessedDuringNormalProcessReady verifies
// that InstallSnapshot messages ARE processed during the normal processReady
// path when commitPending is false. This is the complementary test to
// TestStepWorker_CommitPendingInbox_DefersInstallSnapshot: it confirms that
// the normal path handles snapshot messages without deferral.
func TestStepWorker_InstallSnapshot_ProcessedDuringNormalProcessReady(t *testing.T) {
	sender := newTestSender()
	ws := NewWorkSignal(1)
	commitC := make(chan *commitItem, 16)
	stopC := make(chan struct{})

	sw := newStepWorker(0, time.Millisecond, sender, ws, commitC, nil, stopC, nil)

	peer, lr := newMultiNodeTestPeer(t, 1, 1)
	cfg := config.Config{ShardID: 1, ReplicaID: 1}
	cfg.SetDefaults()
	node := NewNode(peer, nil, lr, cfg, nil, "", nil)
	sw.loadNode(node)

	// The peer starts as follower in a multi-node cluster.

	// Deliver an InstallSnapshot while commitPending is false (normal path).
	node.Deliver(proto.Message{
		Type: proto.InstallSnapshot, Origin: proto.OriginLocal,
		From:    2,
		To:      1,
		ShardID: 1,
		Term:    5,
		Snapshot: proto.Snapshot{
			Index: 100,
			Term:  5,
			Membership: proto.Membership{
				Addresses: map[uint64]string{
					1: "localhost:10001",
					2: "localhost:10002",
					3: "localhost:10003",
				},
			},
		},
	})

	// Process via normal processReady (commitPending is false).
	ws.Notify(1)
	sw.processReady()

	// The inbox should be fully drained (message was processed, not deferred).
	remaining := node.DrainInbox(nil)
	if len(remaining) != 0 {
		t.Fatalf("expected inbox to be empty after normal processReady, got %d messages", len(remaining))
	}
	// commitPending is false, so the snapshot must not be deferred.
	if n := len(sw.deferredSnapshots[1]); n != 0 {
		t.Fatalf("InstallSnapshot deferred on the normal path: %d deferred", n)
	}

	// A commit item should be produced if the snapshot was accepted
	// (which generates a state change). Drain it to verify the normal
	// pipeline processed the snapshot.
	select {
	case <-commitC:
		// Expected: snapshot processing produced an update.
	default:
		// Also acceptable: the peer might reject the snapshot if terms
		// don't match or it's stale. The important assertion is that
		// the inbox was drained (message was handled, not deferred).
	}
}

// ---------------------------------------------------------------------------
// Failed ReadIndex completes pending reads via OnReadIndexFailed
// ---------------------------------------------------------------------------

// TestStepWorker_ReadIndexFailed_CompletesAllKeys verifies that when
// peer.Handle(ReadIndex) fails, OnReadIndexFailed is called with all
// keys in the batch (batch key + aliases). This prevents orphaned
// pending reads that would otherwise block until context deadline.
func TestStepWorker_ReadIndexFailed_CompletesAllKeys(t *testing.T) {
	sender := newTestSender()
	ws := NewWorkSignal(1)
	commitC := make(chan *commitItem, 16)
	stopC := make(chan struct{})

	cb := newTestCallback()
	sw := newStepWorker(0, time.Millisecond, sender, ws, commitC, cb, stopC, nil)

	// Use a multi-node peer that is a follower with no known leader.
	// Handle(ReadIndex) on such a peer returns NotLeaderError.
	peer, lr := newMultiNodeTestPeer(t, 1, 1)
	cfg := config.Config{ShardID: 1, ReplicaID: 1}
	cfg.SetDefaults()
	node := NewNode(peer, nil, lr, cfg, nil, "", nil)
	sw.loadNode(node)

	// Deliver 4 ReadIndex keys. The first is the batch key, the rest
	// become aliases via OnReadIndexBatched.
	for i := uint64(300); i < 304; i++ {
		if !node.DeliverReadIndex(i) {
			t.Fatalf("DeliverReadIndex(%d) should succeed", i)
		}
	}

	ws.Notify(1)
	sw.processReady()

	// Drain any commit items produced by the election state.
	select {
	case <-commitC:
	default:
	}

	// OnReadIndexBatched should have been called for the aliases.
	if cb.batchCount.Load() != 1 {
		t.Fatalf("expected 1 OnReadIndexBatched call, got %d", cb.batchCount.Load())
	}

	// OnReadIndexFailed should have been called with ALL 4 keys.
	if cb.readIndexFailedCount.Load() != 1 {
		t.Fatalf("expected 1 OnReadIndexFailed call, got %d", cb.readIndexFailedCount.Load())
	}
	failedCalls := cb.getReadIndexFailedCalls()
	if len(failedCalls) != 1 {
		t.Fatalf("expected 1 failed call, got %d", len(failedCalls))
	}
	fc := failedCalls[0]
	if fc.shardID != 1 {
		t.Errorf("shardID = %d, want 1", fc.shardID)
	}
	if len(fc.keys) != 4 {
		t.Fatalf("expected 4 keys in failed call, got %d", len(fc.keys))
	}
	for i, want := range []uint64{300, 301, 302, 303} {
		if fc.keys[i] != want {
			t.Errorf("keys[%d] = %d, want %d", i, fc.keys[i], want)
		}
	}
	// The error should be a NotLeaderError since the peer has no leader.
	var nle *raft.NotLeaderError
	if !errors.As(fc.err, &nle) {
		t.Fatalf("expected NotLeaderError, got %T: %v", fc.err, fc.err)
	}
}

// TestStepWorker_ReadIndexFailed_SingleKey verifies the error path works
// for a single ReadIndex key (no aliases registered).
func TestStepWorker_ReadIndexFailed_SingleKey(t *testing.T) {
	sender := newTestSender()
	ws := NewWorkSignal(1)
	commitC := make(chan *commitItem, 16)
	stopC := make(chan struct{})

	cb := newTestCallback()
	sw := newStepWorker(0, time.Millisecond, sender, ws, commitC, cb, stopC, nil)

	peer, lr := newMultiNodeTestPeer(t, 1, 1)
	cfg := config.Config{ShardID: 1, ReplicaID: 1}
	cfg.SetDefaults()
	node := NewNode(peer, nil, lr, cfg, nil, "", nil)
	sw.loadNode(node)

	// Deliver a single ReadIndex key.
	if !node.DeliverReadIndex(42) {
		t.Fatal("DeliverReadIndex(42) should succeed")
	}

	ws.Notify(1)
	sw.processReady()

	select {
	case <-commitC:
	default:
	}

	// No aliases registered for a single key.
	if cb.batchCount.Load() != 0 {
		t.Fatalf("expected 0 OnReadIndexBatched calls for single key, got %d", cb.batchCount.Load())
	}

	// OnReadIndexFailed should have been called with the single key.
	if cb.readIndexFailedCount.Load() != 1 {
		t.Fatalf("expected 1 OnReadIndexFailed call, got %d", cb.readIndexFailedCount.Load())
	}
	fc := cb.getReadIndexFailedCalls()[0]
	if len(fc.keys) != 1 {
		t.Fatalf("expected 1 key, got %d", len(fc.keys))
	}
	if fc.keys[0] != 42 {
		t.Errorf("key = %d, want 42", fc.keys[0])
	}
}

// TestStepWorker_ReadIndexFailed_NilCallback verifies that ReadIndex
// failure with a nil callback does not panic.
func TestStepWorker_ReadIndexFailed_NilCallback(t *testing.T) {
	sender := newTestSender()
	ws := NewWorkSignal(1)
	commitC := make(chan *commitItem, 16)
	stopC := make(chan struct{})

	// nil callback must not panic.
	sw := newStepWorker(0, time.Millisecond, sender, ws, commitC, nil, stopC, nil)

	peer, lr := newMultiNodeTestPeer(t, 1, 1)
	cfg := config.Config{ShardID: 1, ReplicaID: 1}
	cfg.SetDefaults()
	node := NewNode(peer, nil, lr, cfg, nil, "", nil)
	sw.loadNode(node)

	for i := uint64(1); i <= 3; i++ {
		node.DeliverReadIndex(i)
	}

	// Must not panic.
	ws.Notify(1)
	sw.processReady()

	select {
	case <-commitC:
	default:
	}
}

// TestStepWorker_ReadIndexFailed_CommitPending_CompletesAllKeys verifies
// that the commitPending fast path also calls OnReadIndexFailed when
// ReadIndex submission to the peer fails.
func TestStepWorker_ReadIndexFailed_CommitPending_CompletesAllKeys(t *testing.T) {
	sender := newTestSender()
	ws := NewWorkSignal(1)
	commitC := make(chan *commitItem, 16)
	stopC := make(chan struct{})

	cb := newTestCallback()
	sw := newStepWorker(0, time.Millisecond, sender, ws, commitC, cb, stopC, nil)

	// Use a multi-node follower with no leader (ReadIndex will fail).
	peer, lr := newMultiNodeTestPeer(t, 1, 1)
	cfg := config.Config{ShardID: 1, ReplicaID: 1}
	cfg.SetDefaults()
	testSm := newTestSM()
	rsmSM := newTestApplier(testSm)
	node := NewNode(peer, rsmSM, lr, cfg, nil, "", nil)
	sw.loadNode(node)

	// Set commitPending to enter the fast path.
	node.commitPending.Store(true)

	// Deliver 3 ReadIndex keys while commitPending is true.
	for i := uint64(500); i < 503; i++ {
		if !node.DeliverReadIndex(i) {
			t.Fatalf("DeliverReadIndex(%d) should succeed", i)
		}
	}

	ws.Notify(1)
	sw.processReady()

	// OnReadIndexBatched should have been called for the 2 alias keys.
	if cb.batchCount.Load() != 1 {
		t.Fatalf("expected 1 OnReadIndexBatched call, got %d", cb.batchCount.Load())
	}

	// OnReadIndexFailed should have been called with all 3 keys.
	if cb.readIndexFailedCount.Load() != 1 {
		t.Fatalf("expected 1 OnReadIndexFailed call, got %d", cb.readIndexFailedCount.Load())
	}
	fc := cb.getReadIndexFailedCalls()[0]
	if fc.shardID != 1 {
		t.Errorf("shardID = %d, want 1", fc.shardID)
	}
	if len(fc.keys) != 3 {
		t.Fatalf("expected 3 keys, got %d", len(fc.keys))
	}
	for i, want := range []uint64{500, 501, 502} {
		if fc.keys[i] != want {
			t.Errorf("keys[%d] = %d, want %d", i, fc.keys[i], want)
		}
	}
	var nle *raft.NotLeaderError
	if !errors.As(fc.err, &nle) {
		t.Fatalf("expected NotLeaderError, got %T: %v", fc.err, fc.err)
	}
}

// ---------------------------------------------------------------------------
// Metrics helpers (merged from coverage_step_test.go)
// ---------------------------------------------------------------------------

// metricsElectionCount returns the election count for a shard from testRaftMetrics.
func metricsElectionCount(m *testRaftMetrics, shardID uint64) int64 {
	v, ok := m.elections.Load(shardID)
	if !ok {
		return 0
	}
	return v.(*atomic.Int64).Load()
}

// metricsBatchSizeCount returns the number of batch size observations for a shard.
func metricsBatchSizeCount(m *testRaftMetrics, shardID uint64) int {
	v, ok := m.proposalBatchSizes.Load(shardID)
	if !ok {
		return 0
	}
	return len(*v.(*[]int))
}

// ---------------------------------------------------------------------------
// drainAndHandleProposals coverage (merged from coverage_step_test.go)
// ---------------------------------------------------------------------------

// TestDrainAndHandleProposals_WithMetrics verifies that the metrics
// ObserveProposalBatchSize is called when metrics are enabled and
// proposals are batched.
func TestDrainAndHandleProposals_WithMetrics(t *testing.T) {
	sender := newTestSender()
	ws := NewWorkSignal(1)
	commitC := make(chan *commitItem, 16)
	stopC := make(chan struct{})
	cb := newTestCallback()
	metrics := newTestRaftMetrics()

	sw := newStepWorker(0, time.Millisecond, sender, ws, commitC, cb, stopC, metrics)

	peer, lr := newTestPeer(t, 1, 1)
	cfg := config.Config{ShardID: 1, ReplicaID: 1}
	cfg.SetDefaults()
	node := NewNode(peer, nil, lr, cfg, nil, "", nil)
	sw.loadNode(node)

	tickPeerToLeader(peer)

	// Drain election update.
	ws.Notify(1)
	sw.processReady()
	select {
	case item := <-commitC:
		if err := peer.Commit(item.update); err != nil {
			t.Fatalf("commit failed: %v", err)
		}
		node.commitPending.Store(false)
	default:
	}

	// Deliver 3 proposals.
	for i := range 3 {
		node.DeliverProposal([]proto.Entry{{Cmd: []byte{byte(i + 10)}}})
	}

	sw.drainAndHandleProposals(1, node)

	if metricsBatchSizeCount(metrics, 1) != 1 {
		t.Fatalf("expected 1 batch size observation, got %d", metricsBatchSizeCount(metrics, 1))
	}
}

// TestDrainAndHandleProposals_ErrorCallback verifies that when Handle
// returns an error for the batched proposal, OnProposalFailed is called
// on the callback. This exercises the error path in drainAndHandleProposals.
func TestDrainAndHandleProposals_ErrorCallback(t *testing.T) {
	sender := newTestSender()
	ws := NewWorkSignal(1)
	commitC := make(chan *commitItem, 16)
	stopC := make(chan struct{})
	cb := newTestCallback()

	sw := newStepWorker(0, time.Millisecond, sender, ws, commitC, cb, stopC, nil)

	// Create a peer but do NOT tick it to leader. As a follower, it will
	// reject Propose messages (no leader known). This triggers the error
	// and callback path.
	peer, lr := newTestPeer(t, 1, 1)
	cfg := config.Config{ShardID: 1, ReplicaID: 1}
	cfg.SetDefaults()
	node := NewNode(peer, nil, lr, cfg, nil, "", nil)
	sw.loadNode(node)

	// Deliver a proposal to a follower peer (no leader known).
	node.DeliverProposal([]proto.Entry{{Cmd: []byte("will-fail")}})

	sw.drainAndHandleProposals(1, node)

	// Raft refused the batch before appending it: a definite rejection,
	// reported through OnProposalRejected and never as a post-hand-off failure.
	if cb.proposalRejectedCount.Load() == 0 {
		t.Fatal("expected OnProposalRejected to be called for follower rejection")
	}
	if got := cb.proposalFailedCount.Load(); got != 0 {
		t.Fatalf("OnProposalFailed fired %d times for a batch raft never appended; "+
			"its callers would be told the outcome is unknown", got)
	}
}

// ---------------------------------------------------------------------------
// processCallbacks campaign detection (merged from coverage_step_test.go)
// ---------------------------------------------------------------------------

// TestProcessCallbacks_CampaignDetection verifies that processCallbacks
// treats an outbound RequestVote or RequestPreVote as a campaign launch,
// firing OnCampaignLaunched and IncElection, and that other message types
// fire neither.
func TestProcessCallbacks_CampaignDetection(t *testing.T) {
	tests := []struct {
		name     string
		msgs     []proto.Message
		campaign bool
	}{
		{"RequestVote", []proto.Message{
			{Type: proto.RequestVote, Term: 5, From: 1, To: 2},
		}, true},
		{"RequestPreVote", []proto.Message{
			{Type: proto.RequestPreVote, Term: 3, From: 1, To: 2},
		}, true},
		{"NonElection", []proto.Message{
			{Type: proto.Heartbeat, From: 1, To: 2},
			{Type: proto.Replicate, From: 1, To: 3},
		}, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cb := newTestCallback()
			metrics := newTestRaftMetrics()
			sw := newStepWorker(0, time.Millisecond, newTestSender(), NewWorkSignal(1),
				make(chan *commitItem, 16), cb, make(chan struct{}), metrics)

			peer, lr := newTestPeer(t, 1, 1)
			cfg := config.Config{ShardID: 1, ReplicaID: 1}
			cfg.SetDefaults()
			node := NewNode(peer, nil, lr, cfg, nil, "", nil)
			sw.loadNode(node)

			sw.processCallbacks(1, node, proto.Update{
				ShardID: 1, ReplicaID: 1, Messages: tt.msgs,
			})

			if got := cb.campaignCount.Load() != 0; got != tt.campaign {
				t.Fatalf("OnCampaignLaunched fired = %v, want %v", got, tt.campaign)
			}
			if got := metricsElectionCount(metrics, 1) != 0; got != tt.campaign {
				t.Fatalf("IncElection called = %v, want %v", got, tt.campaign)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// processReady keeps recovering nodes in the protocol
// ---------------------------------------------------------------------------

// TestProcessReady_RecoveringNodeDrainsInbox verifies that processReady
// still drains and steps the inbox of a recovering node. Raft message
// handling touches only raft state and the log, never the state machine
// the snapshot pool is replacing, so a recovering replica must keep
// answering RequestVote and Heartbeat. When it does not, a shard with two
// replicas recovering at once loses quorum with every node still up.
func TestProcessReady_RecoveringNodeDrainsInbox(t *testing.T) {
	sender := newTestSender()
	ws := NewWorkSignal(1)
	commitC := make(chan *commitItem, 16)
	stopC := make(chan struct{})

	sw := newStepWorker(0, time.Millisecond, sender, ws, commitC, nil, stopC, nil)

	peer, lr := newTestPeer(t, 1, 1)
	cfg := config.Config{ShardID: 1, ReplicaID: 1}
	cfg.SetDefaults()
	node := NewNode(peer, nil, lr, cfg, nil, "", nil)

	// Mark node as recovering.
	node.recovering.Store(true)
	sw.loadNode(node)

	tickPeerToLeader(peer)
	priorTerm := peer.Term()

	// A higher-term heartbeat must be stepped, not left queued.
	if !node.Deliver(proto.Message{
		Type:    proto.Heartbeat,
		ShardID: 1,
		From:    2,
		To:      1,
		Term:    priorTerm + 3,
	}) {
		t.Fatal("failed to deliver heartbeat to node inbox")
	}

	ws.Notify(1)
	sw.processReady()

	if d := node.DiagState(); d.InboxLen != 0 {
		t.Fatalf("recovering node left %d messages in its inbox; it must be drained", d.InboxLen)
	}
	if got := peer.Term(); got <= priorTerm {
		t.Fatalf("recovering node did not step the heartbeat: term %d -> %d", priorTerm, got)
	}
}

// ---------------------------------------------------------------------------
// drainAndProcess coverage (merged from coverage_step_test.go)
// ---------------------------------------------------------------------------

// TestDrainAndProcess_WithMetrics verifies that drainAndProcess records
// commit times in the metrics map when metrics are enabled.
func TestDrainAndProcess_WithMetrics(t *testing.T) {
	ldb := newTestLogDB()
	commitC := make(chan *commitItem, 16)
	applyC := make(chan *applyItem, 16)
	applyPool := &sync.Pool{New: func() interface{} { return &applyItem{} }}
	stopC := make(chan struct{})
	metrics := newTestRaftMetrics()

	cw := newCommitWorker(0, ldb, nil, commitC, applyC, applyPool, NewWorkSignal(1), nil, stopC, metrics)
	cw.noRetryDelay = true

	peer, lr := newTestPeer(t, 5, 1)
	tickPeerToLeader(peer)

	cfg := config.Config{ShardID: 5, ReplicaID: 1}
	cfg.SetDefaults()
	node := NewNode(peer, nil, lr, cfg, nil, "", nil)
	node.commitPending.Store(true)

	// Drain the initial election update.
	if peer.HasUpdate(0) {
		electionUpdate, gErr := peer.GetUpdate(true, 0)
		if gErr != nil {
			t.Fatalf("GetUpdate (election) failed: %v", gErr)
		}
		if cErr := peer.Commit(electionUpdate); cErr != nil {
			t.Fatalf("Commit (election) failed: %v", cErr)
		}
	}

	// Propose to generate a real update.
	if handleErr := peer.Handle(proto.Message{
		Type: proto.Propose, Origin: proto.OriginLocal, ShardID: 5, From: 1,
		Entries: []proto.Entry{{Cmd: []byte("metrics")}},
	}); handleErr != nil {
		t.Fatalf("Handle propose failed: %v", handleErr)
	}
	if !peer.HasUpdate(0) {
		t.Fatal("expected update after propose")
	}
	update, err := peer.GetUpdate(true, 0)
	if err != nil {
		t.Fatalf("GetUpdate failed: %v", err)
	}

	now := time.Now()
	commitC <- &commitItem{
		update:    update,
		node:      node,
		createdAt: now,
	}

	cw.drainAndProcess()

	// Verify that commit latency was observed via metrics.
	if _, ok := metrics.commitLatencies.Load(uint64(5)); !ok {
		t.Fatal("expected ObserveCommitLatency to be called for shardID 5")
	}
}

// TestDrainAndProcess_WithOrderedMessages verifies that ordered messages
// captured during drainAndProcess are sent after SaveState.
func TestDrainAndProcess_WithOrderedMessages(t *testing.T) {
	ldb := newTestLogDB()
	commitC := make(chan *commitItem, 16)
	applyC := make(chan *applyItem, 16)
	applyPool := &sync.Pool{New: func() interface{} { return &applyItem{} }}
	stopC := make(chan struct{})

	sender := newTestSender()
	cw := newCommitWorker(0, ldb, sender, commitC, applyC, applyPool, NewWorkSignal(1), nil, stopC, nil)
	cw.noRetryDelay = true

	peer, lr := newTestPeer(t, 3, 1)
	tickPeerToLeader(peer)

	cfg := config.Config{ShardID: 3, ReplicaID: 1}
	cfg.SetDefaults()
	node := NewNode(peer, nil, lr, cfg, nil, "", nil)
	node.commitPending.Store(true)

	// Drain election update.
	if peer.HasUpdate(0) {
		eu, gErr := peer.GetUpdate(true, 0)
		if gErr != nil {
			t.Fatalf("GetUpdate (election) failed: %v", gErr)
		}
		if cErr := peer.Commit(eu); cErr != nil {
			t.Fatalf("Commit (election) failed: %v", cErr)
		}
	}

	// Propose to generate a real update.
	if handleErr := peer.Handle(proto.Message{
		Type: proto.Propose, Origin: proto.OriginLocal, ShardID: 3, From: 1,
		Entries: []proto.Entry{{Cmd: []byte("ordered")}},
	}); handleErr != nil {
		t.Fatalf("Handle propose failed: %v", handleErr)
	}
	if !peer.HasUpdate(0) {
		t.Fatal("expected update after propose")
	}
	update, err := peer.GetUpdate(true, 0)
	if err != nil {
		t.Fatalf("GetUpdate failed: %v", err)
	}

	commitC <- &commitItem{
		update: update,
		node:   node,
		orderedMsgs: []proto.Message{
			{Type: proto.RequestVoteResp, ShardID: 3, From: 1, To: 2, Term: 1},
		},
	}

	cw.drainAndProcess()

	if ldb.saveCount.Load() == 0 {
		t.Fatal("expected SaveState to be called")
	}
	// Ordered messages should have been sent via sender.
	if sender.sentCount() == 0 {
		t.Fatal("expected ordered messages to be sent after SaveState")
	}
}

// ---------------------------------------------------------------------------
// executePiggyback coverage (merged from coverage_step_test.go)
// ---------------------------------------------------------------------------

// TestExecutePiggyback_WithFreeOrderMessages verifies that executePiggyback
// separates free-order messages from ordered messages and sends free-order
// ones immediately.
func TestExecutePiggyback_WithFreeOrderMessages(t *testing.T) {
	ldb := newTestLogDB()
	commitC := make(chan *commitItem, 16)
	applyC := make(chan *applyItem, 16)
	applyPool := &sync.Pool{New: func() interface{} { return &applyItem{} }}
	stopC := make(chan struct{})
	sender := newTestSender()
	metrics := newTestRaftMetrics()

	cw := newCommitWorker(0, ldb, sender, commitC, applyC, applyPool, NewWorkSignal(1), nil, stopC, metrics)
	cw.noRetryDelay = true

	peer, lr := newTestPeer(t, 1, 1)
	tickPeerToLeader(peer)

	cfg := config.Config{ShardID: 1, ReplicaID: 1}
	cfg.SetDefaults()
	node := NewNode(peer, nil, lr, cfg, nil, "", nil)
	node.commitPending.Store(true)

	// Persist the term-opening no-op through a real GetUpdate/Commit
	// cycle. tryCommit counts the leader's own match at its durable
	// index, so the single-node commit index only advances at the
	// persistence edge inside Peer.Commit — the manufactured piggyback
	// below claims entry 1 is committed, which must be true by then.
	initial, err := peer.GetUpdate(true, 0)
	if err != nil {
		t.Fatalf("GetUpdate: %v", err)
	}
	if err := peer.Commit(initial); err != nil {
		t.Fatalf("Commit(initial): %v", err)
	}

	piggyback := proto.Update{
		ShardID:   1,
		ReplicaID: 1,
		State:     proto.State{Term: 1, Commit: 1},
		Messages: []proto.Message{
			{Type: proto.Heartbeat, ShardID: 1, From: 1, To: 2},
			{Type: proto.RequestVoteResp, ShardID: 1, From: 1, To: 2, Term: 1},
		},
		CommittedEntries: []proto.Entry{
			{Index: 1, Term: 1, Cmd: []byte("committed")},
		},
	}

	cw.executePiggyback(node, piggyback)

	// Free-order (Heartbeat) should have been sent via sender.
	if sender.sentCount() == 0 {
		t.Fatal("expected sender.Send to be called for free-order messages")
	}

	// Apply item should have been forwarded.
	select {
	case ai := <-applyC:
		if ai.update.ShardID != 1 {
			t.Fatalf("expected shardID 1 in apply item, got %d", ai.update.ShardID)
		}
	case <-time.After(time.Second):
		t.Fatal("expected apply item within timeout")
	}
}

// ---------------------------------------------------------------------------
// processReady with commitPending inbox processing (merged from coverage_step_test.go)
// ---------------------------------------------------------------------------

// TestStepWorker_CommitPendingInbox_InstallSnapshotNoRedeliveryLoop verifies
// that an InstallSnapshot message deferred during commitPending is NOT
// re-delivered on every subsequent processReady cycle. The step worker
// buffers deferred InstallSnapshot messages in deferredSnapshots and
// re-injects them into the inbox when commitPending clears, preventing
// the CPU-burning drain-redeliver loop.
func TestStepWorker_CommitPendingInbox_InstallSnapshotNoRedeliveryLoop(t *testing.T) {
	sender := newTestSender()
	ws := NewWorkSignal(1)
	commitC := make(chan *commitItem, 16)
	stopC := make(chan struct{})

	cb := newTestCallback()
	sw := newStepWorker(0, time.Millisecond, sender, ws, commitC, cb, stopC, nil)

	peer, lr := newMultiNodeLeaderTestPeer(t, 1, 1)
	cfg := config.Config{ShardID: 1, ReplicaID: 1}
	cfg.SetDefaults()
	testSm := newTestSM()
	rsmSM := newTestApplier(testSm)
	node := NewNode(peer, rsmSM, lr, cfg, nil, "", nil)
	sw.loadNode(node)

	// Drain election state.
	ws.Notify(1)
	sw.processReady()
	select {
	case item := <-commitC:
		if err := peer.Commit(item.update); err != nil {
			t.Fatalf("initial commit failed: %v", err)
		}
		node.commitPending.Store(false)
	default:
	}

	// Set commitPending and deliver an InstallSnapshot.
	node.commitPending.Store(true)
	snapMsg := proto.Message{
		Type: proto.InstallSnapshot, Origin: proto.OriginLocal,
		From:    2,
		To:      1,
		ShardID: 1,
		Snapshot: proto.Snapshot{
			Index: 100,
			Term:  5,
		},
	}
	node.DeliverLocal(snapMsg)

	// First processReady cycle: InstallSnapshot is buffered in
	// deferredSnapshots, not re-delivered to inbox.
	ws.Notify(1)
	sw.processReady()

	// The inbox should be empty (InstallSnapshot was buffered, not re-delivered).
	remaining := node.DrainInbox(nil)
	if len(remaining) != 0 {
		t.Fatalf("expected 0 messages in inbox after deferral, got %d", len(remaining))
	}

	// The deferred buffer should hold the InstallSnapshot.
	deferred := sw.deferredSnapshots[1]
	if len(deferred) != 1 {
		t.Fatalf("expected 1 deferred snapshot, got %d", len(deferred))
	}
	if deferred[0].Snapshot.Index != 100 {
		t.Errorf("deferred snapshot index = %d, want 100", deferred[0].Snapshot.Index)
	}

	// Second processReady cycle: no InstallSnapshot in inbox, so no
	// drain-redeliver loop. Other messages (proposals, ReadIndex)
	// still process normally.
	ws.Notify(1)
	sw.processReady()

	// Deferred buffer should still hold the same message.
	deferred = sw.deferredSnapshots[1]
	if len(deferred) != 1 {
		t.Fatalf("deferred snapshot count should remain 1, got %d", len(deferred))
	}
}

// TestStepWorker_CommitPendingInbox_DeferredSnapshotReinjected verifies
// that deferred InstallSnapshot messages are re-injected into the inbox
// and processed when commitPending transitions to false.
func TestStepWorker_CommitPendingInbox_DeferredSnapshotReinjected(t *testing.T) {
	sender := newTestSender()
	ws := NewWorkSignal(1)
	commitC := make(chan *commitItem, 16)
	stopC := make(chan struct{})

	cb := newTestCallback()
	sw := newStepWorker(0, time.Millisecond, sender, ws, commitC, cb, stopC, nil)

	peer, lr := newMultiNodeLeaderTestPeer(t, 1, 1)
	cfg := config.Config{ShardID: 1, ReplicaID: 1}
	cfg.SetDefaults()
	testSm := newTestSM()
	rsmSM := newTestApplier(testSm)
	node := NewNode(peer, rsmSM, lr, cfg, nil, "", nil)
	sw.loadNode(node)

	// Drain election state.
	ws.Notify(1)
	sw.processReady()
	select {
	case item := <-commitC:
		if err := peer.Commit(item.update); err != nil {
			t.Fatalf("initial commit failed: %v", err)
		}
		node.commitPending.Store(false)
	default:
	}

	// Manually add a deferred snapshot to simulate the deferral.
	sw.deferredSnapshots[1] = []proto.Message{{
		Type: proto.InstallSnapshot, Origin: proto.OriginLocal,
		From:     2,
		To:       1,
		ShardID:  1,
		Snapshot: proto.Snapshot{Index: 200, Term: 8},
	}}

	// commitPending is false, so normal processReady runs.
	// It should re-inject the deferred snapshot into the inbox.
	ws.Notify(1)
	sw.processReady()

	// After processReady, the deferred buffer should be empty.
	deferred := sw.deferredSnapshots[1]
	if len(deferred) != 0 {
		t.Fatalf("deferred snapshots should be empty after re-injection, got %d", len(deferred))
	}
}

// TestCommitMemoryMode_CommitError_FailsShard verifies that the memory-mode
// inline commit path treats a Peer.Commit error as terminal: Commit has
// already advanced the processed marker, so the committed entries can never
// be re-offered and the shard must go offline rather than keep serving a
// state machine that is missing them.
func TestCommitMemoryMode_CommitError_FailsShard(t *testing.T) {
	sender := newSequenceSender()
	ws := NewWorkSignal(1)
	commitC := make(chan *commitItem, 16)
	applyC := make(chan *applyItem, 16)
	applyPool := &sync.Pool{New: func() interface{} { return &applyItem{} }}
	stopC := make(chan struct{})
	cb := newTestCallback()

	sw := newStepWorker(0, time.Millisecond, sender, ws, commitC, cb, stopC, nil)
	sw.memoryMode = true
	sw.applyC = applyC
	sw.applyPool = applyPool

	peer, lr, update := newCommitFailurePeer(t, 1, 1)
	cfg := config.Config{ShardID: 1, ReplicaID: 1}
	cfg.SetDefaults()
	node := NewNode(peer, newTestApplier(newTestSM()), lr, cfg, nil, "", nil)
	sw.loadNode(node)

	if stopped := sw.commitMemoryMode(node, update); stopped {
		t.Fatal("commitMemoryMode should not report stopped")
	}

	if !node.IsFailed() {
		t.Fatal("node must be marked failed after a Peer.Commit error")
	}
	if node.commitPending.Load() {
		t.Fatal("commitPending must be cleared after a Peer.Commit error")
	}
	if len(applyC) != 0 {
		t.Fatalf("no apply item may be forwarded after a Commit error, got %d", len(applyC))
	}
	if cb.proposalFailedCount.Load() != 1 {
		t.Fatalf("expected 1 OnProposalFailed call, got %d", cb.proposalFailedCount.Load())
	}
}

// ---------------------------------------------------------------------------
// commitC backpressure must not silence the shard
// ---------------------------------------------------------------------------

// TestProcessReady_CommitCFull_FlushesFreeOrderMessages verifies that the
// commitC-full backpressure skip still flushes the peer's free-order
// messages (heartbeats, Replicate). GetUpdate is the only other drain of
// those messages on this path and commitPending is false here, so tick()'s
// drain does not run either: skipping without a flush leaves a leader's
// heartbeats sitting in the peer for as long as the commit worker is
// behind. A single SaveState retry backoff (up to ~1.76s) is enough for
// followers to time out and campaign against a live leader.
func TestProcessReady_CommitCFull_FlushesFreeOrderMessages(t *testing.T) {
	sender := newTestSender()
	ws := NewWorkSignal(1)
	// Capacity 1 and already full: the backpressure guard trips.
	commitC := make(chan *commitItem, 1)
	commitC <- &commitItem{}
	stopC := make(chan struct{})
	defer close(stopC)

	sw := newStepWorker(0, time.Millisecond, sender, ws, commitC, nil, stopC, nil)

	peer, lr := newMultiNodeLeaderTestPeer(t, 1, 1)
	cfg := config.Config{ShardID: 1, ReplicaID: 1}
	cfg.SetDefaults()
	node := NewNode(peer, nil, lr, cfg, nil, "", nil)
	sw.loadNode(node)

	// Tick the leader so heartbeats accumulate in the peer. commitPending
	// is false, so tick() deliberately does not drain them.
	for range 30 {
		sw.tick()
	}
	if sender.sentCount() != 0 {
		t.Fatalf("tick() must not send while commitPending is false, sent %d", sender.sentCount())
	}

	ws.Notify(1)
	sw.processReady()

	sent := sender.sentMessages()
	if len(sent) == 0 {
		t.Fatal("commitC-full skip sent nothing: the shard's heartbeats are stuck in the peer")
	}
	for i := range sent {
		if !proto.IsFreeOrderMessage(sent[i].Type) {
			t.Fatalf("ordered message %s sent before its state was persisted",
				proto.MessageTypeName(sent[i].Type))
		}
	}

	// The guard must still hold back the update itself: nothing new may be
	// queued while commitC is full.
	if len(commitC) != cap(commitC) {
		t.Fatal("the backpressure guard must not enqueue while commitC is full")
	}
	if node.commitPending.Load() {
		t.Fatal("no update was extracted, so commitPending must stay false")
	}
}
