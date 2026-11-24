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
	"testing"
	"time"

	"github.com/jeremyhahn/go-quicraft/pkg/internal/raft"
	"github.com/jeremyhahn/go-quicraft/pkg/proto"
)

// Between a snapshot install and its recovery, raft offers entries from snapshotIndex+1 while lastApplied lags; extracting them fails the shard with ErrApplyGap.

const (
	adoptSnapIndex = uint64(100)
	adoptSnapTerm  = uint64(7)
)

// installAndCommitNext delivers an InstallSnapshot and the Replicate committing the next entry to node's inbox.
func installAndCommitNext(t *testing.T, node *Node) {
	t.Helper()
	if !node.DeliverLocal(installSnapshotMessage(node, adoptSnapIndex, adoptSnapTerm)) {
		t.Fatal("fixture: inbox rejected the InstallSnapshot")
	}
	next := adoptSnapIndex + 1
	if !node.Deliver(proto.Message{
		Type:     proto.Replicate,
		From:     2,
		To:       node.ReplicaID(),
		ShardID:  node.ShardID(),
		Term:     adoptSnapTerm,
		LogIndex: adoptSnapIndex,
		LogTerm:  adoptSnapTerm,
		Commit:   next,
		Entries: []proto.Entry{{
			Index: next,
			Term:  adoptSnapTerm,
			Cmd:   []byte("post-transfer"),
		}},
	}) {
		t.Fatal("fixture: inbox rejected the Replicate")
	}
}

// assertLogIsAheadOfStateMachine fails the test if the fixture did not
// reproduce the window: the raft log holding the snapshot and the entry above
// it, with the state machine still at its pre-snapshot lastApplied. Without
// this the assertions below would pass against a log that never installed
// anything.
func assertLogIsAheadOfStateMachine(t *testing.T, node *Node, peer *raft.Peer) {
	t.Helper()
	_, committed, processed, _, _, _ := peer.DiagRaftState()
	if processed != adoptSnapIndex {
		t.Fatalf("fixture: raft processed marker = %d, want %d: the install did not land",
			processed, adoptSnapIndex)
	}
	if committed != adoptSnapIndex+1 {
		t.Fatalf("fixture: raft committed = %d, want %d: the entry above the snapshot is not committed",
			committed, adoptSnapIndex+1)
	}
	if la := node.LastApplied(); la != 0 {
		t.Fatalf("fixture: lastApplied = %d, want 0: the state machine must not have adopted the snapshot yet", la)
	}
}

// TestProcessReady_WithholdsCommittedEntriesUntilSnapshotAdopted covers the step worker's extraction gate.
func TestProcessReady_WithholdsCommittedEntriesUntilSnapshotAdopted(t *testing.T) {
	commitC := make(chan *commitItem, 16)
	stopC := make(chan struct{})
	defer close(stopC)
	ws := NewWorkSignal(1)
	sw := newStepWorker(0, time.Millisecond, newTestSender(), ws, commitC,
		newTestCallback(), stopC, nil)

	node, peer := newRecoveryTestNode(t, 1, 1, newSnapshotTestSnapshotter(0))
	sw.loadNode(node)

	installAndCommitNext(t, node)
	ws.Notify(1)
	sw.processReady()
	assertLogIsAheadOfStateMachine(t, node, peer)

	item := <-commitC
	if n := len(item.update.CommittedEntries); n != 0 {
		t.Fatalf("step worker extracted %d committed entries at index %d while the "+
			"state machine is still at lastApplied=%d: the apply worker fails the "+
			"shard with ErrApplyGap and the replica never applies again",
			n, item.update.CommittedEntries[0].Index, node.LastApplied())
	}
	if item.update.Snapshot.Index != adoptSnapIndex {
		t.Fatalf("update.Snapshot.Index = %d, want %d: the snapshot must still reach "+
			"the commit worker, only the entries above it are withheld",
			item.update.Snapshot.Index, adoptSnapIndex)
	}

	// The commit worker commits the update; the snapshot pool then finishes
	// RecoverFromSnapshot and seeds the markers (snapshot_pool.recoverSnapshot).
	if err := peer.Commit(item.update); err != nil {
		t.Fatalf("commit the snapshot update: %v", err)
	}
	node.commitPending.Store(false)
	node.SeedLastApplied(adoptSnapIndex, adoptSnapTerm)

	// The withheld entry is offered again, now continuing lastApplied exactly.
	ws.Notify(1)
	sw.processReady()
	item = <-commitC
	if len(item.update.CommittedEntries) != 1 {
		t.Fatalf("after the snapshot was adopted the step worker extracted %d entries, want 1: "+
			"the withheld entry was never re-offered and the replica is stuck",
			len(item.update.CommittedEntries))
	}
	if got := item.update.CommittedEntries[0].Index; got != adoptSnapIndex+1 {
		t.Fatalf("re-offered batch starts at index %d, want %d", got, adoptSnapIndex+1)
	}
}

// TestCommitUpdateLocked_WithholdsPiggybackUntilSnapshotAdopted covers the commit worker's piggyback half of the gate.
func TestCommitUpdateLocked_WithholdsPiggybackUntilSnapshotAdopted(t *testing.T) {
	node, peer := newRecoveryTestNode(t, 1, 1, newSnapshotTestSnapshotter(0))

	installAndCommitNext(t, node)
	// Step the inbox exactly as drainAndHandleInbox does, without the step
	// worker: this test drives the commit worker's entry point directly.
	for _, msg := range node.DrainInbox(nil) {
		if err := peer.Handle(msg); err != nil {
			t.Fatalf("handle %s: %v", proto.MessageTypeName(msg.Type), err)
		}
	}
	assertLogIsAheadOfStateMachine(t, node, peer)

	update, err := peer.GetUpdate(false, node.LastApplied())
	if err != nil {
		t.Fatalf("GetUpdate: %v", err)
	}
	if len(update.CommittedEntries) != 0 {
		t.Fatalf("fixture: the update carries %d committed entries; the piggyback "+
			"branch under test only runs for an update that carries none",
			len(update.CommittedEntries))
	}

	var piggyback proto.Update
	status, err := commitUpdateLocked(node, &update, &piggyback)
	if err != nil {
		t.Fatalf("commitUpdateLocked: %v", err)
	}
	if n := len(piggyback.CommittedEntries); n != 0 {
		t.Fatalf("piggyback extracted %d committed entries at index %d while the state "+
			"machine is still at lastApplied=%d: same ErrApplyGap, one goroutine over",
			n, piggyback.CommittedEntries[0].Index, node.LastApplied())
	}
	if status != commitRecovering {
		t.Fatalf("commitUpdateLocked status = %d, want commitRecovering (%d): the commit "+
			"worker must clear commitPending and re-notify so the entries are re-offered",
			status, commitRecovering)
	}
}
