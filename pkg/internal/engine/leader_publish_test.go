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

// newTestVoterPeer builds a peer for a shard with `voters` voting members,
// all of which are unreachable in these tests. A multi-voter membership is
// what makes CheckQuorum observable: a single-node shard is always its own
// quorum and can never step down.
func newTestVoterPeer(t testing.TB, shardID, replicaID uint64, voters int) (*raft.Peer, *logdb.LogReader) {
	t.Helper()
	ldb := newTestLogDB()
	lr := logdb.NewLogReader(shardID, replicaID, ldb)
	addrs := make(map[uint64]string, voters)
	for i := 1; i <= voters; i++ {
		addrs[uint64(i)] = "localhost:0"
	}
	if err := lr.ApplySnapshot(proto.Snapshot{
		Index:      0,
		Term:       0,
		Membership: proto.Membership{Addresses: addrs},
	}); err != nil {
		t.Fatalf("ApplySnapshot failed: %v", err)
	}
	cfg := config.Config{ShardID: shardID, ReplicaID: replicaID}
	cfg.SetDefaults()
	peer, err := raft.NewPeer(cfg, lr, nil)
	if err != nil {
		t.Fatalf("NewPeer failed: %v", err)
	}
	return peer, lr
}

// driveToLeader campaigns the peer and grants it the votes of `granter`,
// carrying it through the pre-vote and vote rounds to leadership. The peer
// must be a voter in a three-member shard so that one grant plus its own
// vote is a quorum.
func driveToLeader(t *testing.T, peer *raft.Peer, self, granter uint64) {
	t.Helper()
	// Tick past the (randomised) election timeout to launch the pre-vote.
	for range 200 {
		if peer.LeaderID() != 0 {
			break
		}
		peer.Tick()
		term := peer.Term()
		// Grant the pre-vote at the prospective term, then the real vote at
		// whatever term the candidate reached.
		if err := peer.Handle(proto.Message{
			Type: proto.RequestPreVoteResp,
			From: granter,
			To:   self,
			Term: term + 1,
		}); err != nil {
			t.Fatalf("RequestPreVoteResp handle failed: %v", err)
		}
		if vt := peer.Term(); vt > 0 {
			if err := peer.Handle(proto.Message{
				Type: proto.RequestVoteResp,
				From: granter,
				To:   self,
				Term: vt,
			}); err != nil {
				t.Fatalf("RequestVoteResp handle failed: %v", err)
			}
		}
	}
	if peer.LeaderID() != self {
		t.Fatalf("peer did not become leader (leaderID=%d, term=%d)",
			peer.LeaderID(), peer.Term())
	}
}

// TestStepWorker_Tick_PublishesCheckQuorumStepDown verifies a CheckQuorum step-down inside Tick is published to the Host.
func TestStepWorker_Tick_PublishesCheckQuorumStepDown(t *testing.T) {
	sender := newTestSender()
	ws := NewWorkSignal(1)
	commitC := make(chan *commitItem, 64)
	stopC := make(chan struct{})
	defer close(stopC)

	cb := newTestCallback()
	sw := newStepWorker(0, time.Millisecond, sender, ws, commitC, cb, stopC, nil)

	peer, lr := newTestVoterPeer(t, 1, 1, 3)
	cfg := config.Config{ShardID: 1, ReplicaID: 1}
	cfg.SetDefaults()
	node := NewNode(peer, newTestApplier(newTestSM()), lr, cfg, nil, "", nil)
	sw.loadNode(node)

	driveToLeader(t, peer, 1, 2)

	// Publish the "became leader" transition through the ordinary path so
	// prevLeaders is primed exactly as it would be in production.
	sw.processReady()
	drainCommitC(commitC)
	if got := lastLeader(cb); got != 1 {
		t.Fatalf("expected leadership to be published as replica 1, got %d", got)
	}

	// No peer ever answers a heartbeat, so the next CheckQuorum window finds
	// no active remotes and the leader steps down inside Tick().
	for range 400 {
		sw.tick()
		sw.processReady()
		drainCommitC(commitC)
		if peer.LeaderID() != 1 {
			break
		}
	}
	if peer.LeaderID() == 1 {
		t.Fatal("raft did not step down under CheckQuorum; test setup is wrong")
	}

	if got := lastLeader(cb); got == 1 {
		t.Fatalf("the engine still advertises replica 1 as leader after a "+
			"CheckQuorum step-down (raft leaderID=%d): Host.ShardInfo().IsLeader "+
			"and every !isLeader write guard read this stale value until the node "+
			"campaigned, ~one election timeout later", peer.LeaderID())
	}
}

// TestStepWorker_CommitPendingInbox_PublishesLeaderChange covers the publish site inside processCommitPendingInbox.
func TestStepWorker_CommitPendingInbox_PublishesLeaderChange(t *testing.T) {
	sender := newTestSender()
	ws := NewWorkSignal(1)
	commitC := make(chan *commitItem, 64)
	stopC := make(chan struct{})
	defer close(stopC)

	cb := newTestCallback()
	sw := newStepWorker(0, time.Millisecond, sender, ws, commitC, cb, stopC, nil)

	peer, lr := newTestVoterPeer(t, 1, 1, 3)
	cfg := config.Config{ShardID: 1, ReplicaID: 1}
	cfg.SetDefaults()
	node := NewNode(peer, newTestApplier(newTestSM()), lr, cfg, nil, "", nil)
	sw.loadNode(node)

	// Campaign without the winning vote, via the tick path only.
	for range 400 {
		peer.Tick()
		if err := peer.Handle(proto.Message{
			Type: proto.RequestPreVoteResp,
			From: 2,
			To:   1,
			Term: peer.Term() + 1,
		}); err != nil {
			t.Fatalf("RequestPreVoteResp handle failed: %v", err)
		}
		if peer.Term() > 1 {
			break
		}
	}
	if peer.LeaderID() == 1 {
		t.Fatal("peer became leader before the deciding vote was delivered")
	}

	// The pipeline is mid-commit: the step worker takes the commitPending path
	// only, which is where the deciding RequestVoteResp is stepped.
	node.commitPending.Store(true)
	if !node.Deliver(proto.Message{
		Type: proto.RequestVoteResp,
		From: 2,
		To:   1,
		Term: peer.Term(),
	}) {
		t.Fatal("inbox rejected the vote response")
	}
	sw.processCommitPendingInbox(1, node)
	if peer.LeaderID() != 1 {
		t.Fatalf("peer did not win its election from the commitPending path "+
			"(leaderID=%d)", peer.LeaderID())
	}

	// The cycle that won the election must publish it; the vote response is stepped in phase 2.
	if got := lastLeader(cb); got != 1 {
		t.Fatalf("leadership won under commitPending was never published "+
			"(last published leader=%d, raft leaderID=%d): the Host reports this "+
			"real leader as a follower and rejects every write against it",
			got, peer.LeaderID())
	}
}

// lastLeader returns the leaderID of the most recent OnLeaderUpdated call, or
// the sentinel ^uint64(0) when the callback has never fired.
func lastLeader(cb *testCallback) uint64 {
	calls := cb.getLeaderCalls()
	if len(calls) == 0 {
		return ^uint64(0)
	}
	return calls[len(calls)-1].leaderID
}

// drainCommitC empties the commit channel so a test that never runs a commit
// worker cannot wedge the step worker on its backpressure guard.
func drainCommitC(commitC chan *commitItem) {
	for {
		select {
		case <-commitC:
		default:
			return
		}
	}
}

// lockProbeCallback records whether OnLeaderUpdated was invoked while the
// node's raftMu was held, by trying to take the lock from inside the callback.
type lockProbeCallback struct {
	Callback
	node       *Node
	calls      int
	underLock  bool
	lastLeader uint64
}

func (c *lockProbeCallback) OnLeaderUpdated(shardID, leaderID, term uint64) {
	c.calls++
	c.lastLeader = leaderID
	// raftMu is not reentrant. If the caller still holds it, TryLock fails.
	if c.node.raftMu.TryLock() {
		c.node.raftMu.Unlock()
	} else {
		c.underLock = true
	}
}

// TestStepWorker_CommitPendingInbox_PublishesOutsideRaftMu pins that OnLeaderUpdated never runs under the node lock.
func TestStepWorker_CommitPendingInbox_PublishesOutsideRaftMu(t *testing.T) {
	sender := newTestSender()
	ws := NewWorkSignal(1)
	commitC := make(chan *commitItem, 64)
	stopC := make(chan struct{})
	defer close(stopC)

	peer, lr := newTestVoterPeer(t, 1, 1, 3)
	cfg := config.Config{ShardID: 1, ReplicaID: 1}
	cfg.SetDefaults()
	node := NewNode(peer, newTestApplier(newTestSM()), lr, cfg, nil, "", nil)

	cb := &lockProbeCallback{Callback: newTestCallback(), node: node}
	sw := newStepWorker(0, time.Millisecond, sender, ws, commitC, cb, stopC, nil)
	sw.loadNode(node)

	// Drive the node to leadership through the commitPending path.
	for range 400 {
		peer.Tick()
		if err := peer.Handle(proto.Message{
			Type: proto.RequestPreVoteResp, From: 2, To: 1, Term: peer.Term() + 1,
		}); err != nil {
			t.Fatalf("RequestPreVoteResp handle failed: %v", err)
		}
		if peer.Term() > 1 {
			break
		}
	}
	node.commitPending.Store(true)
	if !node.Deliver(proto.Message{
		Type: proto.RequestVoteResp, From: 2, To: 1, Term: peer.Term(),
	}) {
		t.Fatal("inbox rejected the vote response")
	}
	sw.processCommitPendingInbox(1, node)

	if cb.calls == 0 {
		t.Fatal("fixture: OnLeaderUpdated never fired, so the lock probe proved nothing")
	}
	if cb.underLock {
		t.Fatalf("OnLeaderUpdated (leader=%d) ran while raftMu was held: arbitrary "+
			"user listener code now stalls the commit worker for every shard it owns",
			cb.lastLeader)
	}
}

// TestStepWorker_CommitPendingInbox_Phase1PublishesOutsideRaftMu pins the phase-1 publish runs outside raftMu.
func TestStepWorker_CommitPendingInbox_Phase1PublishesOutsideRaftMu(t *testing.T) {
	sender := newTestSender()
	ws := NewWorkSignal(1)
	commitC := make(chan *commitItem, 64)
	stopC := make(chan struct{})
	defer close(stopC)

	peer, lr := newTestVoterPeer(t, 1, 1, 3)
	cfg := config.Config{ShardID: 1, ReplicaID: 1}
	cfg.SetDefaults()
	node := NewNode(peer, newTestApplier(newTestSM()), lr, cfg, nil, "", nil)

	cb := &lockProbeCallback{Callback: newTestCallback(), node: node}
	sw := newStepWorker(0, time.Millisecond, sender, ws, commitC, cb, stopC, nil)
	sw.loadNode(node)

	driveToLeader(t, peer, 1, 2)
	sw.processReady()
	drainCommitC(commitC)
	callsBefore := cb.calls

	node.commitPending.Store(true)
	if !node.Deliver(proto.Message{
		Type: proto.HeartbeatResp, From: 2, To: 1, Term: peer.Term() + 5,
	}) {
		t.Fatal("inbox rejected the heartbeat response")
	}
	sw.processCommitPendingInbox(1, node)

	if cb.calls == callsBefore {
		t.Fatal("fixture: the phase-1 step-down published nothing, so the lock " +
			"probe proved nothing about the phase-1 site")
	}
	if cb.underLock {
		t.Fatalf("the PHASE 1 OnLeaderUpdated (leader=%d) ran while raftMu was held",
			cb.lastLeader)
	}
	if cb.lastLeader == 1 {
		t.Fatalf("step-down in PHASE 1 was never published (last published leader=%d, "+
			"raft leaderID=%d): Host.ShardInfo().IsLeader keeps reporting this node "+
			"as leader", cb.lastLeader, peer.LeaderID())
	}
}

// TestStepWorker_CommitPendingInbox_LeavesClientWorkQueuedWhileRecovering pins that client work stays queued during recovery while Raft traffic flows.
func TestStepWorker_CommitPendingInbox_LeavesClientWorkQueuedWhileRecovering(t *testing.T) {
	sender := newTestSender()
	ws := NewWorkSignal(1)
	commitC := make(chan *commitItem, 64)
	stopC := make(chan struct{})
	defer close(stopC)

	cb := newTestCallback()
	sw := newStepWorker(0, time.Millisecond, sender, ws, commitC, cb, stopC, nil)

	peer, lr := newTestVoterPeer(t, 1, 1, 3)
	cfg := config.Config{ShardID: 1, ReplicaID: 1}
	cfg.SetDefaults()
	node := NewNode(peer, newTestApplier(newTestSM()), lr, cfg, nil, "", nil)
	sw.loadNode(node)

	node.commitPending.Store(true)
	node.SetRecovering(true)

	if !node.DeliverProposal([]proto.Entry{{Term: 1, Key: 0x1234, Cmd: []byte("x")}}) {
		t.Fatal("fixture: proposal queue rejected the entry")
	}

	sw.processCommitPendingInbox(1, node)

	// The proposal must still be on the queue: a later drain has to find it.
	node.SetRecovering(false)
	drained := node.DrainProposals(nil)
	if len(drained) != 1 || drained[0].Key != 0x1234 {
		t.Fatalf("the proposal was consumed during recovery (drained %d entries): "+
			"it can never complete while the apply pipeline is stopped, and taking "+
			"it off the queue denies the caller the ErrSystemBusy backpressure the "+
			"queue would otherwise apply", len(drained))
	}
}
