// Copyright 2026 Jeremy Hahn
// SPDX-License-Identifier: Apache-2.0

package engine

import (
	"testing"
	"time"

	"github.com/jeremyhahn/go-quicraft/pkg/config"
	"github.com/jeremyhahn/go-quicraft/pkg/internal/raft"
	"github.com/jeremyhahn/go-quicraft/pkg/logdb"
	"github.com/jeremyhahn/go-quicraft/pkg/proto"
)

// stalledFollower is a follower Node on its own step worker whose WAL write
// is in flight: commitPending stays set for the whole test, so the step
// worker steps the inbox and ticks but sends only the free-order drain.
type stalledFollower struct {
	node   *Node
	sw     *stepWorker
	sender *testSender
	seen   int
}

func newStalledFollower(t *testing.T, replicaID, term uint64, stopC chan struct{}) *stalledFollower {
	t.Helper()
	lr := logdb.NewLogReader(1, replicaID, newTestLogDB())
	if err := lr.ApplySnapshot(proto.Snapshot{
		Membership: proto.Membership{Addresses: map[uint64]string{
			1: "localhost:0", 2: "localhost:0", 3: "localhost:0",
		}},
	}); err != nil {
		t.Fatalf("ApplySnapshot: %v", err)
	}
	// The leader's term and the vote for it are already durable, as they
	// are on a follower that elected this leader before the stall began.
	lr.SetState(proto.State{Term: term, Vote: 1})
	cfg := config.Config{ShardID: 1, ReplicaID: replicaID}
	cfg.SetDefaults()
	peer, err := raft.NewPeer(cfg, lr, nil)
	if err != nil {
		t.Fatalf("NewPeer: %v", err)
	}
	sender := newTestSender()
	sw := newStepWorker(0, time.Millisecond, sender, NewWorkSignal(1), make(chan *commitItem, 64), nil, stopC, nil)
	node := NewNode(peer, newTestApplier(newTestSM()), lr, cfg, nil, "", nil)
	sw.loadNode(node)
	node.commitPending.Store(true)
	return &stalledFollower{node: node, sw: sw, sender: sender}
}

// sent returns what the follower's step worker put on the wire since the
// last call.
func (f *stalledFollower) sent() []proto.Message {
	all := f.sender.sentMessages()
	out := all[f.seen:]
	f.seen = len(all)
	return out
}

// TestStepWorker_LeaderSurvivesQuorumFollowerFsyncStall holds a quorum of
// followers in a bounded WAL fsync stall (commitPending set, the in-flight
// update never committed) for several election timeouts. The followers
// keep answering heartbeats from the free-order drain, so CheckQuorum keeps
// the leader in office, and no reply acknowledges the entry that the stall
// keeps from being durable (#390).
func TestStepWorker_LeaderSurvivesQuorumFollowerFsyncStall(t *testing.T) {
	stopC := make(chan struct{})
	defer close(stopC)

	leader, _ := newTestVoterPeer(t, 1, 1, 3)
	driveToLeader(t, leader, 1, 2)
	term := leader.Term()

	followers := map[uint64]*stalledFollower{
		2: newStalledFollower(t, 2, term, stopC),
		3: newStalledFollower(t, 3, term, stopC),
	}

	// The leader's first batch carries its no-op entry: the followers
	// append it in memory, and its WAL write is the one that stalls.
	route := func() {
		for _, m := range leader.DrainMessages() {
			if f, ok := followers[m.To]; ok {
				f.node.Deliver(m)
			}
		}
		for id, f := range followers {
			f.sw.processCommitPendingInbox(1, f.node)
			for _, m := range f.sent() {
				if m.Type == proto.ReplicateResp {
					t.Fatalf("replica %d acknowledged a log position during its fsync stall", id)
				}
				if m.To == 1 {
					if err := leader.Handle(m); err != nil {
						t.Fatalf("leader Handle(%d): %v", m.Type, err)
					}
				}
			}
		}
	}
	route()

	cfg := config.Config{ShardID: 1, ReplicaID: 1}
	cfg.SetDefaults()
	for i := range 5 * int(cfg.ElectionRTT) {
		leader.Tick()
		for _, f := range followers {
			f.sw.tick()
		}
		route()
		if !leader.IsLeader() {
			t.Fatalf("leader deposed by CheckQuorum %d ticks into a quorum fsync stall (term %d -> %d)",
				i+1, term, leader.Term())
		}
	}
	for id, f := range followers {
		if got := f.node.peer.Term(); got != term {
			t.Fatalf("replica %d moved to term %d during the stall, want %d", id, got, term)
		}
	}
}
