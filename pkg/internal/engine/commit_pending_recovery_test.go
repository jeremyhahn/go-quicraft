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

	"github.com/jeremyhahn/go-quicraft/pkg/config"
	"github.com/jeremyhahn/go-quicraft/pkg/internal/raft"
	"github.com/jeremyhahn/go-quicraft/pkg/logdb"
	"github.com/jeremyhahn/go-quicraft/pkg/proto"
)

func newCommitPendingLeader(t *testing.T, sw *stepWorker, shardID, replicaID uint64) *Node {
	t.Helper()
	peer, lr := newTestVoterPeer(t, shardID, replicaID, 3)
	cfg := config.Config{ShardID: shardID, ReplicaID: replicaID}
	cfg.SetDefaults()
	node := NewNode(peer, newTestApplier(newTestSM()), lr, cfg, nil, "", nil)
	sw.loadNode(node)
	granter := uint64(2)
	if replicaID == 2 {
		granter = 1
	}
	driveToLeader(t, peer, replicaID, granter)
	return node
}

func peerLastIndex(n *Node) uint64 {
	_, _, _, lastIdx, _, _ := n.peer.DiagRaftState()
	return lastIdx
}

// zzR14: the else-branch truncation in processCommitPendingInbox is the ONLY
// thing that stops a stale commitProposeBuf from being replayed into the peer.
// Without it, the SAME client proposal is appended to the raft log again on
// every commitPending cycle for as long as the recovery gate is up.
func TestCommitPendingInbox_RecoveringDoesNotReplayStaleProposals(t *testing.T) {
	sender := newTestSender()
	ws := NewWorkSignal(1)
	commitC := make(chan *commitItem, 64)
	stopC := make(chan struct{})
	defer close(stopC)

	sw := newStepWorker(0, time.Millisecond, sender, ws, commitC, newTestCallback(), stopC, nil)

	node := newCommitPendingLeader(t, sw, 1, 1)
	node.commitPending.Store(true)

	if !node.DeliverProposal([]proto.Entry{{Key: 0xAAA, Cmd: []byte("p1")}}) {
		t.Fatal("fixture: proposal queue rejected the entry")
	}
	sw.processCommitPendingInbox(1, node)
	afterFirst := peerLastIndex(node)

	// The recovery gate goes up. Nothing new is queued.
	node.SetRecovering(true)
	sw.processCommitPendingInbox(1, node)
	sw.processCommitPendingInbox(1, node)

	if got := peerLastIndex(node); got != afterFirst {
		t.Fatalf("raft log grew from %d to %d with nothing new queued: the stale "+
			"commitProposeBuf was re-handled, appending duplicate copies of client "+
			"proposal 0xAAA on every commitPending cycle of the recovery",
			afterFirst, got)
	}
}

// zzR14: commitProposeBuf/readKeyBuf are stepWorker-wide, not per-shard. If the
// recovering branch does not clear them, the shard processed just before a
// recovering shard leaks its client proposals INTO the recovering shard's raft
// log.
func TestCommitPendingInbox_RecoveringDoesNotInjectOtherShardsProposals(t *testing.T) {
	sender := newTestSender()
	ws := NewWorkSignal(1)
	commitC := make(chan *commitItem, 64)
	stopC := make(chan struct{})
	defer close(stopC)

	sw := newStepWorker(0, time.Millisecond, sender, ws, commitC, newTestCallback(), stopC, nil)

	nodeA := newCommitPendingLeader(t, sw, 1, 1)
	nodeB := newCommitPendingLeader(t, sw, 2, 1)
	nodeA.commitPending.Store(true)
	nodeB.commitPending.Store(true)
	nodeB.SetRecovering(true)

	beforeB := peerLastIndex(nodeB)

	// Shard A (healthy) is processed first and leaves its entries in the
	// worker-wide buffer.
	if !nodeA.DeliverProposal([]proto.Entry{{Key: 0xA1, Cmd: []byte("shardA")}}) {
		t.Fatal("fixture: shard A proposal queue rejected the entry")
	}
	sw.processCommitPendingInbox(1, nodeA)

	// Shard B (recovering) is processed next with nothing of its own queued.
	sw.processCommitPendingInbox(2, nodeB)

	if got := peerLastIndex(nodeB); got != beforeB {
		t.Fatalf("shard 2's raft log grew from %d to %d with nothing proposed to it: "+
			"shard 1's client proposal was appended to shard 2's log", beforeB, got)
	}
}

// TestProcessReady_RecoveringLeavesClientWorkQueued is the processReady half of
// the recovery backpressure contract. Its commitPending sibling is covered by
// TestStepWorker_CommitPendingInbox_LeavesClientWorkQueuedWhileRecovering; this
// gate is older and was never covered at all — flipping it to always drain
// during a recovery passed the entire suite.
//
// Beyond holding client work, the gate also suppresses redeliverDeferredSnapshots:
// re-injecting a deferred InstallSnapshot while a recovery owns the state
// machine restarts the drain/redeliver churn that deferring it exists to stop.
func TestProcessReady_RecoveringLeavesClientWorkQueued(t *testing.T) {
	sender := newTestSender()
	ws := NewWorkSignal(1)
	commitC := make(chan *commitItem, 64)
	stopC := make(chan struct{})
	defer close(stopC)

	cb := newTestCallback()
	sw := newStepWorker(0, time.Millisecond, sender, ws, commitC, cb, stopC, nil)
	node := newCommitPendingLeader(t, sw, 1, 1)
	drainCommitC(commitC)

	logBefore := peerLastIndex(node)

	// A recovery owns the state machine. commitPending is CLEAR, so processReady
	// (not processCommitPendingInbox) is the path taken.
	node.SetRecovering(true)
	if node.commitPending.Load() {
		t.Fatal("fixture: commitPending must be clear to exercise processReady")
	}

	if !node.DeliverProposal([]proto.Entry{{Term: 1, Key: 0xBEEF, Cmd: []byte("x")}}) {
		t.Fatal("fixture: proposal queue rejected the entry")
	}

	sw.processReady()
	drainCommitC(commitC)

	if got := peerLastIndex(node); got != logBefore {
		t.Fatalf("raft log grew from %d to %d during a recovery: the proposal was "+
			"appended while the apply pipeline is stopped, so it cannot complete, "+
			"and it was taken off the queue that would otherwise answer ErrSystemBusy",
			logBefore, got)
	}

	// It must still be on the queue for a post-recovery drain to find.
	node.SetRecovering(false)
	drained := node.DrainProposals(nil)
	if len(drained) != 1 || drained[0].Key != 0xBEEF {
		t.Fatalf("the proposal was consumed during recovery (drained %d entries)",
			len(drained))
	}
}

// TestEngine_UnloadNodeReleasesGlobalRateLimit pins the WIRING of the shared
// in-memory limiter's teardown release, through the real Engine.UnloadNode.
//
// The raft layer can release a shard's share, but the leak only closes if the
// engine actually calls it. Without this, every StartShard/StopShard cycle
// leaves the stopped shard's bytes in the host-wide counter until the
// accumulated orphans cross MaxTotalInMemLogSize and every propose on the host
// fails ErrSystemBusy with no memory actually held.
//
// It goes through Engine.UnloadNode rather than stepWorker.unloadNode on
// purpose: the release must happen only after the commit worker has been told
// to skip Peer.Commit and the apply goroutine has exited, because an in-flight
// Commit reaches the very same accounting through appliedTo -> subGlobalSize.
func TestEngine_UnloadNodeReleasesGlobalRateLimit(t *testing.T) {
	cfg := newTestHostConfig()
	e := NewEngine(cfg, newTestLogDB(), newTestSender(), nil, nil, nil)
	global := e.GlobalRateLimiter()
	if global == nil {
		t.Skip("host config has no global in-memory limit configured")
	}

	const shardID = 11
	nodeCfg := config.Config{ShardID: shardID, ReplicaID: 1}
	nodeCfg.SetDefaults()

	ldb := newTestLogDB()
	lr := logdb.NewLogReader(shardID, 1, ldb)
	if err := lr.ApplySnapshot(proto.Snapshot{
		Membership: proto.Membership{Addresses: map[uint64]string{
			1: "localhost:0", 2: "localhost:0", 3: "localhost:0",
		}},
	}); err != nil {
		t.Fatalf("ApplySnapshot: %v", err)
	}
	peer, err := raft.NewPeer(nodeCfg, lr, global)
	if err != nil {
		t.Fatalf("NewPeer: %v", err)
	}
	node := NewNode(peer, newTestApplier(newTestSM()), lr, nodeCfg, nil, "", nil)
	e.LoadNode(node)

	// Charge the limiter: only a leader appends proposals to its in-memory log.
	driveToLeader(t, peer, 1, 2)
	if _, err := peer.Propose([]proto.Entry{{Term: 1, Key: 1, Cmd: make([]byte, 4096)}}); err != nil {
		t.Fatalf("Propose: %v", err)
	}
	if global.Get() == 0 {
		t.Fatal("fixture: the shard never contributed to the global limiter, so " +
			"the release below would prove nothing")
	}

	e.UnloadNode(shardID)

	if got := global.Get(); got != 0 {
		t.Fatalf("global limiter still holds %d bytes after Engine.UnloadNode: the "+
			"stopped shard's share is counted against a shard that no longer "+
			"exists, and every start/stop cycle leaks more", got)
	}
}
