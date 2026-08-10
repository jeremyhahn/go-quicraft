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
func newTestVoterPeer(t *testing.T, shardID, replicaID uint64, voters int) (*raft.Peer, *logdb.LogReader) {
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

// TestStepWorker_Tick_PublishesCheckQuorumStepDown is the regression test for
// a leader that stepped down but kept advertising itself as leader.
//
// A CheckQuorum step-down happens ENTIRELY inside Tick(): tickLeader sees no
// quorum of active remotes and calls becomeFollower(r.term, noLeader). That
// changes neither term, vote nor commit index and produces no message, so
// Peer.HasUpdate() is false and processReady skips the shard without ever
// reaching a publish site. The engine's only writer of the shard's leader
// flag — OnLeaderUpdated — was reachable from just two places, both of which
// require an update or a non-empty inbox, so the step-down was invisible:
// Host.GetShardInfo().IsLeader kept reporting true, and every `!isLeader`
// guard in the Host kept admitting writes to a replica that had already
// relinquished leadership. The stale value survived until the node campaigned,
// roughly a full election timeout later.
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
	node := NewNode(peer, newTestApplier(newTestSM()), lr, cfg, 100, nil, "", nil)
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
			"CheckQuorum step-down (raft leaderID=%d): Host.GetShardInfo().IsLeader "+
			"and every !isLeader write guard read this stale value until the node "+
			"campaigned, ~one election timeout later", peer.LeaderID())
	}
}

// TestStepWorker_CommitPendingInbox_PublishesLeaderChange covers the publish
// site inside processCommitPendingInbox.
//
// While commitPending is set the step worker never calls GetUpdate, so
// processCallbacks — the other publish site — cannot run at all. A node that
// wins its election from inside this path (the vote response is stepped in
// phase 2) would therefore be a real Raft leader that the Host reports as a
// follower, rejecting every write with NotLeaderError, for as long as the
// write load keeps commitPending set.
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
	node := NewNode(peer, newTestApplier(newTestSM()), lr, cfg, 100, nil, "", nil)
	sw.loadNode(node)

	// Campaign to pre-candidate/candidate WITHOUT the winning vote, using only
	// the tick path so nothing is published yet.
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

	// The SAME cycle that won the election must publish it. RequestVoteResp is
	// not a HeartbeatResp, so it is sorted into otherMsgs and stepped in phase
	// 2 — after the phase 1 publish has already run. With publishing done only
	// at the end of phase 1, this node stayed a real Raft leader that the Host
	// reported as a follower until some later cycle happened to publish, and
	// under sustained write load commitPending keeps that cycle from coming.
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

// TestStepWorker_CommitPendingInbox_PublishesOutsideRaftMu pins the contract
// publishLeaderChange documents for itself: the callback never executes under
// the node lock.
//
// OnLeaderUpdated runs arbitrary user listener code (and the Host's
// failPendingRequests). Invoking it under raftMu blocks the commit worker's
// node.raftMu.Lock() for EVERY shard that worker owns, for as long as the
// user's listener takes — a head-of-line stall across unrelated shards, gated
// on third-party code with no time bound.
func TestStepWorker_CommitPendingInbox_PublishesOutsideRaftMu(t *testing.T) {
	sender := newTestSender()
	ws := NewWorkSignal(1)
	commitC := make(chan *commitItem, 64)
	stopC := make(chan struct{})
	defer close(stopC)

	peer, lr := newTestVoterPeer(t, 1, 1, 3)
	cfg := config.Config{ShardID: 1, ReplicaID: 1}
	cfg.SetDefaults()
	node := NewNode(peer, newTestApplier(newTestSM()), lr, cfg, 100, nil, "", nil)

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
