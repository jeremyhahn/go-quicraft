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
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/jeremyhahn/go-quicraft/pkg/internal/engine"
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
// never recovered. The receive must fail instead, while KEEPING the persisted
// artifacts so the leader's retry is answered from LogDB by the dedup branch.
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

	// The artifacts must survive: LogDB already references the directory and
	// the leader's re-send is answered from it by the dedup branch.
	ss, getErr := h.logdb.GetSnapshot(shardID, localReplicaID)
	if getErr != nil {
		t.Fatalf("LogDB record missing after a failed install: %v", getErr)
	}
	if ss.Index != snapIndex {
		t.Fatalf("persisted snapshot index = %d, want %d", ss.Index, snapIndex)
	}
	node := eng.GetNode(shardID)
	dataPath := filepath.Join(node.SnapshotSaveDir(snapIndex), "snapshot.dat")
	if _, statErr := os.Stat(dataPath); statErr != nil {
		t.Fatalf("snapshot.dat removed after a failed install; LogDB now points at nothing: %v", statErr)
	}
}
