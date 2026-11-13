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

	"github.com/jeremyhahn/go-quicraft/pkg/config"
	"github.com/jeremyhahn/go-quicraft/pkg/proto"
)

// newHandOffTestNode builds a fresh leader Node for the proposal hand-off
// tests, as shard_fail_once_test.go does.
func newHandOffTestNode(t *testing.T) *Node {
	t.Helper()
	peer, lr := newTestPeer(t, 1, 1)
	tickPeerToLeader(peer)
	nodeCfg := config.Config{ShardID: 1, ReplicaID: 1}
	nodeCfg.SetDefaults()
	return NewNode(peer, nil, lr, nodeCfg, nil, "", nil)
}

// assertProposalTaken fails the test unless ProposalTaken(at) is (taken, final).
func assertProposalTaken(t *testing.T, node *Node, at *uint64, taken, final bool, when string) {
	t.Helper()
	gotTaken, gotFinal := node.ProposalTaken(at)
	if gotTaken != taken || gotFinal != final {
		t.Fatalf("%s: ProposalTaken = (%v, %v), want (%v, %v)", when, gotTaken, gotFinal, taken, final)
	}
}

// deliverOne delivers a one-entry tracked proposal and returns its record.
func deliverOne(t *testing.T, node *Node, key uint64) *uint64 {
	t.Helper()
	var at uint64
	if !node.DeliverTrackedProposal([]proto.Entry{{Key: key, Cmd: []byte("x")}}, &at) {
		t.Fatalf("DeliverTrackedProposal(key %d) = false on an open node", key)
	}
	if at == 0 {
		t.Fatalf("DeliverTrackedProposal(key %d) left *at zero", key)
	}
	return &at
}

// drainOne drains the queue and requires exactly one entry with the key.
func drainOne(t *testing.T, node *Node, key uint64) uint64 {
	t.Helper()
	got, id := node.DrainProposalBatch(nil)
	if len(got) != 1 || got[0].Key != key {
		t.Fatalf("DrainProposalBatch = %v, want one entry with key %d", got, key)
	}
	if id == 0 {
		t.Fatal("DrainProposalBatch took a batch but returned id 0")
	}
	return id
}

// assertFenced checks the closed-queue contract shared by Stop and
// markFailed: the batch drained before the fence (drainedAt, drainedID) can
// no longer be handed off and is final and untaken, the proposal still queued
// (queuedAt) is final and untaken, a drain takes nothing and a delivery is
// refused.
func assertFenced(t *testing.T, node *Node, drainedAt *uint64, drainedID uint64, queuedAt *uint64) {
	t.Helper()
	if node.HandOffProposals(drainedID) {
		t.Fatal("HandOffProposals of a batch drained before the fence = true, want false")
	}
	assertProposalTaken(t, node, drainedAt, false, true, "drained batch after the fence")
	assertProposalTaken(t, node, queuedAt, false, true, "queued proposal after the fence")
	buf := make([]proto.Entry, 0, 4)
	got, id := node.DrainProposalBatch(buf)
	if len(got) != 0 || id != 0 {
		t.Fatalf("DrainProposalBatch after the fence = (%v, %d), want nothing and id 0", got, id)
	}
	var at uint64
	if node.DeliverTrackedProposal([]proto.Entry{{Key: 9, Cmd: []byte("x")}}, &at) {
		t.Fatal("DeliverTrackedProposal after the fence = true, want false")
	}
	if at != 0 {
		t.Fatalf("refused DeliverTrackedProposal wrote *at = %d, want 0", at)
	}
	assertProposalTaken(t, node, queuedAt, false, true, "queued proposal after a refused delivery")
}

func TestNodeProposalHandOff(t *testing.T) {
	t.Run("empty queue drains nothing", func(t *testing.T) {
		node := newHandOffTestNode(t)
		buf := make([]proto.Entry, 3, 8)
		got, id := node.DrainProposalBatch(buf)
		if len(got) != 0 || id != 0 {
			t.Fatalf("DrainProposalBatch on an empty queue = (%v, %d), want (buf[:0], 0)", got, id)
		}
		if cap(got) != cap(buf) || &got[:1][0] != &buf[0] {
			t.Fatal("DrainProposalBatch on an empty queue did not return the caller's buffer")
		}
	})

	t.Run("taken only after hand-off", func(t *testing.T) {
		node := newHandOffTestNode(t)
		at := deliverOne(t, node, 5)
		assertProposalTaken(t, node, at, false, false, "queued")
		id := drainOne(t, node, 5)
		assertProposalTaken(t, node, at, false, false, "drained, not handed off")
		if !node.HandOffProposals(id) {
			t.Fatal("HandOffProposals on an open node = false, want true")
		}
		assertProposalTaken(t, node, at, true, false, "handed off")
	})

	t.Run("second batch has a larger id and its own hand-off", func(t *testing.T) {
		node := newHandOffTestNode(t)
		first := deliverOne(t, node, 5)
		firstID := drainOne(t, node, 5)
		if !node.HandOffProposals(firstID) {
			t.Fatal("HandOffProposals(first) = false, want true")
		}
		second := deliverOne(t, node, 6)
		assertProposalTaken(t, node, second, false, false, "second queued")
		secondID := drainOne(t, node, 6)
		if secondID <= firstID {
			t.Fatalf("second batch id %d, want greater than first %d", secondID, firstID)
		}
		assertProposalTaken(t, node, second, false, false, "second drained, not handed off")
		assertProposalTaken(t, node, first, true, false, "first while second is in flight")
		if !node.HandOffProposals(secondID) {
			t.Fatal("HandOffProposals(second) = false, want true")
		}
		assertProposalTaken(t, node, second, true, false, "second handed off")
		assertProposalTaken(t, node, first, true, false, "first after second handed off")
	})

	for _, fence := range []struct {
		name  string
		apply func(*Node)
	}{
		{name: "Stop", apply: func(n *Node) { n.Stop() }},
		{name: "markFailed", apply: func(n *Node) {
			if !n.markFailed() {
				panic("markFailed on a fresh node = false")
			}
		}},
	} {
		t.Run(fence.name+" hands nothing further to raft", func(t *testing.T) {
			node := newHandOffTestNode(t)
			drainedAt := deliverOne(t, node, 5)
			drainedID := drainOne(t, node, 5)
			queuedAt := deliverOne(t, node, 6)
			fence.apply(node)
			assertFenced(t, node, drainedAt, drainedID, queuedAt)
		})
	}

	t.Run("never delivered is not taken", func(t *testing.T) {
		node := newHandOffTestNode(t)
		var at uint64
		assertProposalTaken(t, node, &at, false, false, "never delivered")
		// A handed-off batch does not make an undelivered proposal taken.
		deliverOne(t, node, 7)
		id := drainOne(t, node, 7)
		if !node.HandOffProposals(id) {
			t.Fatal("HandOffProposals = false, want true")
		}
		assertProposalTaken(t, node, &at, false, false, "never delivered, after another batch's hand-off")
	})
}
