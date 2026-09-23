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

package quicraft

import (
	"errors"
	"fmt"
	"os"
	"sync"
	"testing"

	"github.com/jeremyhahn/go-quicraft/pkg/internal/engine"
	"github.com/jeremyhahn/go-quicraft/pkg/logdb"
	"github.com/jeremyhahn/go-quicraft/pkg/proto"
)

// ---------------------------------------------------------------------------
// H2: HandleSnapshot must validate the TARGET replica, not just the sender
// ---------------------------------------------------------------------------

// TestHandleSnapshot_RejectsForeignTargetReplica is the H2 regression test.
// The target ReplicaID travels in-band and was previously trusted verbatim,
// yet the whole receive keys off it: a foreign value splits snapshotRecvKey
// (bypassing the in-flight serialization guard while snapshotDir still uses
// the LOCAL replica ID), defeats the LogDB dedup lookup, writes a durable
// LogDB record under a replica ID this host does not own, and reaches raft
// through DeliverMessage, which performs no destination check of its own.
func TestHandleSnapshot_RejectsForeignTargetReplica(t *testing.T) {
	eng, h, handler := newMedlowHandlerFixture(t)

	const shardID, localReplicaID, fromReplicaID = uint64(40), uint64(1), uint64(2)
	const foreignReplicaID = uint64(7)
	const snapIndex = uint64(600)
	authorizeSnapshotPath(t, eng, h, shardID, localReplicaID, fromReplicaID)

	chunks := []proto.SnapshotChunk{
		{
			ShardID:    shardID,
			ReplicaID:  foreignReplicaID, // not this host's replica
			From:       fromReplicaID,    // an authorized shard member
			Index:      snapIndex,
			Term:       3,
			ChunkID:    0,
			ChunkCount: 1,
			Data:       []byte("poison"),
		},
	}

	err := handler.HandleSnapshot(chunks, authorizeAllSenders)
	if err == nil {
		t.Fatal("HandleSnapshot accepted a snapshot addressed to a different replica")
	}
	var unauthorized *UnauthorizedMessageError
	if !errors.As(err, &unauthorized) {
		t.Fatalf("error = %T (%v), want *UnauthorizedMessageError", err, err)
	}

	// Nothing durable may have been written under either replica ID.
	if _, getErr := h.logdb.GetSnapshot(shardID, foreignReplicaID); getErr == nil {
		t.Error("a LogDB record was written under the foreign replica ID")
	}
	if _, getErr := h.logdb.GetSnapshot(shardID, localReplicaID); getErr == nil {
		t.Error("a LogDB record was written under the local replica ID")
	}
	node := eng.GetNode(shardID)
	if node == nil {
		t.Fatal("engine node missing")
	}
	if _, statErr := os.Stat(node.SnapshotSaveDir(snapIndex)); statErr == nil {
		t.Error("the snapshot directory was created for a rejected snapshot")
	}

	// The in-flight claim must not have been taken under either key, so an
	// honest receive is not blocked by the rejected one.
	for _, rid := range []uint64{localReplicaID, foreignReplicaID} {
		key := snapshotRecvKey{shardID: shardID, replicaID: rid, index: snapIndex}
		if _, loaded := handler.inflightSnapshotRecv.Load(key); loaded {
			t.Errorf("rejected snapshot left an in-flight claim for replica %d", rid)
		}
	}
}

// ---------------------------------------------------------------------------
// H3: a failed InstallSnapshot delivery is a FAILED receive
// ---------------------------------------------------------------------------

// refusingInstallDeliverer refuses to deliver InstallSnapshot control
// messages while forwarding everything else, simulating a persistently full
// inbox on the receiving shard.
type refusingInstallDeliverer struct {
	engineDeliverer

	mu               sync.Mutex
	recoveryRequests int
}

func (d *refusingInstallDeliverer) DeliverMessage(shardID uint64, msg proto.Message) bool {
	if msg.Type == proto.InstallSnapshot {
		return false
	}
	return d.engineDeliverer.DeliverMessage(shardID, msg)
}

// DeliverLocal refuses the InstallSnapshot on the in-process injection path
// too: that is the path deliverSnapshotControl actually uses.
func (d *refusingInstallDeliverer) DeliverLocal(shardID uint64, msg proto.Message) bool {
	if msg.Type == proto.InstallSnapshot {
		return false
	}
	return d.engineDeliverer.DeliverLocal(shardID, msg)
}

// DeliverLocalTo refuses the InstallSnapshot on the node-bound path as well:
// it is the one the receive now injects its awaited install through.
func (d *refusingInstallDeliverer) DeliverLocalTo(node *engine.Node, msg proto.Message) bool {
	if msg.Type == proto.InstallSnapshot {
		return false
	}
	return d.engineDeliverer.DeliverLocalTo(node, msg)
}

func (d *refusingInstallDeliverer) RequestSnapshotRecovery(shardID, replicaID uint64, node *engine.Node) error {
	d.mu.Lock()
	d.recoveryRequests++
	d.mu.Unlock()
	return d.engineDeliverer.RequestSnapshotRecovery(shardID, replicaID, node)
}

// TestHandleSnapshot_FailedInstallDeliveryFailsReceive is the H3 regression
// test. A failed deliverSnapshotControl used to be logged as a warning while
// the receive still reported SUCCESS and queued state-machine recovery. Raft
// never advanced to the snapshot index, so recoverSnapshot refused to move
// the node markers (SnapshotRecoveryBehindError) — and because the sender was
// told the transfer succeeded, the leader never re-sent and the follower
// never recovered. The receive must fail instead.
//
// Under the two-phase receive (whole-project review, M1) nothing is recorded
// until raft ADOPTS the install, so a failed delivery leaves no LogDB record
// and no artifact: the leader's retry is received afresh rather than answered
// from a record of a snapshot raft never saw.
func TestHandleSnapshot_FailedInstallDeliveryFailsReceive(t *testing.T) {
	eng, h, _ := newMedlowHandlerFixture(t)

	const shardID, localReplicaID, fromReplicaID = uint64(41), uint64(1), uint64(2)
	const snapIndex = uint64(700)
	authorizeSnapshotPath(t, eng, h, shardID, localReplicaID, fromReplicaID)

	refusing := &refusingInstallDeliverer{engineDeliverer: eng}
	handler := &hostMessageHandler{engine: refusing, host: h}

	payload := []byte("snapshot-body-that-must-survive")
	chunks := []proto.SnapshotChunk{
		{
			ShardID:    shardID,
			ReplicaID:  localReplicaID,
			From:       fromReplicaID,
			Index:      snapIndex,
			Term:       4,
			ChunkID:    0,
			ChunkCount: 1,
			Data:       payload,
		},
	}

	err := handler.HandleSnapshot(chunks, authorizeAllSenders)
	if err == nil {
		t.Fatal("HandleSnapshot reported success despite a failed InstallSnapshot delivery")
	}
	var recvErr *SnapshotReceiveError
	if !errors.As(err, &recvErr) {
		t.Fatalf("error = %T (%v), want *SnapshotReceiveError", err, err)
	}
	if recvErr.Op != "deliver_install" {
		t.Errorf("SnapshotReceiveError.Op = %q, want %q", recvErr.Op, "deliver_install")
	}

	// Recovery must NOT have been queued: raft never installed the snapshot,
	// so recovery could only fail on the behind-index check.
	refusing.mu.Lock()
	requests := refusing.recoveryRequests
	refusing.mu.Unlock()
	if requests != 0 {
		t.Errorf("queued %d recovery requests after a failed install; want 0", requests)
	}

	// Nothing may be durable: raft never saw the install.
	if ss, getErr := h.logdb.GetSnapshot(shardID, localReplicaID); !errors.Is(getErr, logdb.ErrNoSnapshot) {
		t.Fatalf("GetSnapshot after a failed install = (%+v, %v), want ErrNoSnapshot: "+
			"a snapshot raft never saw was recorded", ss, getErr)
	}
	node := eng.GetNode(shardID)
	if _, statErr := os.Stat(node.SnapshotSaveDir(snapIndex)); !os.IsNotExist(statErr) {
		t.Fatalf("snapshot directory left behind after a failed install (stat err %v)", statErr)
	}
}

// countingRecoveryDeliverer forwards everything and counts recovery requests.
type countingRecoveryDeliverer struct {
	engineDeliverer

	mu               sync.Mutex
	recoveryRequests int
}

func (d *countingRecoveryDeliverer) RequestSnapshotRecovery(shardID, replicaID uint64, node *engine.Node) error {
	d.mu.Lock()
	d.recoveryRequests++
	d.mu.Unlock()
	return d.engineDeliverer.RequestSnapshotRecovery(shardID, replicaID, node)
}

// TestHandleSnapshot_RaftRefusalLeavesNothingDurable is the whole-project
// review finding M1. The receive path used to record the snapshot in LogDB,
// raise the log floor to it and queue state-machine recovery BEFORE raft had
// decided on the install — and raft can still refuse one that passed every
// host check (a sender deposed mid-stream; here, a membership naming more
// voters than raft's ReadIndex ack word can address, which only raft checks).
// A refused install then left a recorded snapshot, a raised floor and a queued
// recovery the log could never reach, failing the shard. Under the two-phase
// receive nothing is durable until raft adopts the install.
func TestHandleSnapshot_RaftRefusalLeavesNothingDurable(t *testing.T) {
	eng, h, _ := newMedlowHandlerFixture(t)
	const shardID, localReplicaID, fromReplicaID = uint64(43), uint64(1), uint64(2)
	const snapIndex = uint64(900)
	authorizeSnapshotPath(t, eng, h, shardID, localReplicaID, fromReplicaID)

	counting := &countingRecoveryDeliverer{engineDeliverer: eng}
	handler := &hostMessageHandler{engine: counting, host: h}

	// One voter more than raft can address; this replica and the sender among them.
	voters := make(map[uint64]string, 65)
	for id := uint64(1); id <= 65; id++ {
		voters[id] = fmt.Sprintf("127.0.0.%d:5000", id)
	}
	voters[localReplicaID] = snapshotPathLocalAddr
	voters[fromReplicaID] = snapshotPathSenderAddr
	chunks := []proto.SnapshotChunk{{
		ShardID: shardID, ReplicaID: localReplicaID, From: fromReplicaID,
		Index: snapIndex, Term: 4, ChunkID: 0, ChunkCount: 1,
		Data:       []byte("a snapshot raft will refuse"),
		Membership: proto.Membership{Addresses: voters},
	}}

	err := handler.HandleSnapshot(chunks, authorizeAllSenders)
	var recvErr *SnapshotReceiveError
	if !errors.As(err, &recvErr) || recvErr.Op != "install" {
		t.Fatalf("HandleSnapshot = %v, want a SnapshotReceiveError with Op \"install\" (raft's refusal)", err)
	}

	if ss, getErr := h.logdb.GetSnapshot(shardID, localReplicaID); !errors.Is(getErr, logdb.ErrNoSnapshot) {
		t.Fatalf("GetSnapshot = (%+v, %v), want ErrNoSnapshot: a snapshot raft refused was recorded", ss, getErr)
	}
	node := eng.GetNode(shardID)
	if _, statErr := os.Stat(node.SnapshotSaveDir(snapIndex)); !os.IsNotExist(statErr) {
		t.Fatalf("snapshot directory left behind after raft refused it (stat err %v)", statErr)
	}
	counting.mu.Lock()
	requests := counting.recoveryRequests
	counting.mu.Unlock()
	if requests != 0 {
		t.Fatalf("queued %d recovery requests for a snapshot raft refused; want 0", requests)
	}
	if node.IsFailed() {
		t.Fatal("the shard failed over a snapshot raft simply refused")
	}
}
