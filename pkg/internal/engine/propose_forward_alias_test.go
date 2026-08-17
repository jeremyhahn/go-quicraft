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

// newTestFollowerPeer builds a peer in a 2-replica shard that is a FOLLOWER
// with a known leader, so Propose is FORWARDED (not appended locally).
func newTestFollowerPeer(t *testing.T, shardID, replicaID, leaderID uint64) (*raft.Peer, *logdb.LogReader) {
	t.Helper()
	ldb := newTestLogDB()
	lr := logdb.NewLogReader(shardID, replicaID, ldb)
	ms := proto.Membership{
		Addresses: map[uint64]string{
			replicaID: "localhost:0",
			leaderID:  "localhost:1",
		},
	}
	lr.ApplySnapshot(proto.Snapshot{Index: 0, Term: 0, Membership: ms})
	cfg := config.Config{ShardID: shardID, ReplicaID: replicaID}
	cfg.SetDefaults()
	p, err := raft.NewPeer(cfg, lr, nil)
	if err != nil {
		t.Fatalf("newPeer: %v", err)
	}
	if err := p.Handle(proto.Message{
		Type:    proto.Heartbeat,
		From:    leaderID,
		To:      replicaID,
		ShardID: shardID,
		Term:    1,
		Origin:  proto.OriginRemote,
	}); err != nil {
		t.Fatalf("handle heartbeat: %v", err)
	}
	return p, lr
}

// TestStepWorker_CommitPendingProposeBufRecycledAcrossIterations shows that
// commitProposeBuf is handed back to the proposal queue on the NEXT
// commit-pending drain while a FORWARDED Propose message still aliases it.
//
// A follower forwards Propose to the leader. Propose is NOT a free-order
// message, so the forwarded message stays in the peer's outbound buffer until
// the next GetUpdate -- which cannot run while commitPending is true. Two
// consecutive commit-pending drains therefore recycle the buffer under a live
// message, and the next client proposal overwrites the entry it carries.
func TestStepWorker_CommitPendingProposeBufRecycledAcrossIterations(t *testing.T) {
	sender := newTestSender()
	ws := NewWorkSignal(1)
	commitC := make(chan *commitItem, 16)
	stopC := make(chan struct{})
	defer close(stopC)

	sw := newStepWorker(0, time.Millisecond, sender, ws, commitC, nil, stopC, nil)

	peer, lr := newTestFollowerPeer(t, 1, 1, 2)
	cfg := config.Config{ShardID: 1, ReplicaID: 1}
	cfg.SetDefaults()
	node := NewNode(peer, nil, lr, cfg, nil, "", nil)
	sw.loadNode(node)

	// The commit worker owns the peer; the step worker takes the
	// commit-pending pipelining path.
	node.commitPending.Store(true)

	// Iteration 1: proposal A is drained into commitProposeBuf and forwarded.
	node.DeliverProposal([]proto.Entry{{Cmd: []byte("A")}})
	ws.Notify(1)
	sw.processReady()

	// Iteration 2: proposal B is drained; commitProposeBuf (still aliased by
	// the forwarded A message) is handed back to the queue as its write buffer.
	node.DeliverProposal([]proto.Entry{{Cmd: []byte("B")}})
	ws.Notify(1)
	sw.processReady()

	// A client proposes C. Add() appends into the recycled buffer, on top of
	// the entry the pending forwarded message still points at.
	node.DeliverProposal([]proto.Entry{{Cmd: []byte("C")}})

	// Drain what the follower would actually put on the wire.
	upd, err := peer.GetUpdate(true, 0)
	if err != nil {
		t.Fatalf("GetUpdate: %v", err)
	}
	var forwarded []string
	collect := func(msgs []proto.Message) {
		for i := range msgs {
			if msgs[i].Type != proto.Propose {
				continue
			}
			for j := range msgs[i].Entries {
				forwarded = append(forwarded, string(msgs[i].Entries[j].Cmd))
			}
		}
	}
	collect(upd.Messages)

	// C is still sitting in the proposal queue (it was appended AFTER the
	// second Swap), so the next drain forwards it a SECOND time.
	ws.Notify(1)
	sw.processReady()
	upd2, err := peer.GetUpdate(true, 0)
	if err != nil {
		t.Fatalf("GetUpdate 2: %v", err)
	}
	collect(upd2.Messages)
	t.Logf("forwarded proposals on the wire: %v", forwarded)

	want := map[string]bool{"A": true, "B": true, "C": true}
	got := map[string]int{}
	for _, c := range forwarded {
		got[c]++
	}
	for k := range want {
		if got[k] != 1 {
			t.Errorf("proposal %q: forwarded %d times, want exactly 1 (wire carries %v)", k, got[k], forwarded)
		}
	}
	for k, n := range got {
		if !want[k] && n > 0 {
			t.Errorf("proposal %q was forwarded %d times but was never drained into this batch (wire carries %v)", k, n, forwarded)
		}
	}
}
