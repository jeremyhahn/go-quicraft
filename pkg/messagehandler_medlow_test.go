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
	"bytes"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/jeremyhahn/go-quicraft/pkg/config"
	"github.com/jeremyhahn/go-quicraft/pkg/internal/engine"
	"github.com/jeremyhahn/go-quicraft/pkg/internal/registry"
	"github.com/jeremyhahn/go-quicraft/pkg/logdb/memdb"
	"github.com/jeremyhahn/go-quicraft/pkg/proto"
)

func newMedlowHandlerFixture(t *testing.T) (*engine.Engine, *Host, *hostMessageHandler) {
	t.Helper()
	cfg := config.HostConfig{
		WALDir:                t.TempDir(),
		NodeHostDir:           t.TempDir(),
		RaftAddress:           "127.0.0.1:5000",
		ListenAddress:         "127.0.0.1:5000",
		DeploymentID:          1,
		AllowZeroDeploymentID: false,
	}
	cfg.SetDefaults()

	ldb := memdb.New()
	eng := engine.NewEngine(cfg, ldb, &noopSender{}, nil, nil, nil)
	eng.Start()
	t.Cleanup(eng.Stop)

	h := &Host{cfg: cfg, logdb: ldb, registry: registry.NewRegistry()}
	handler := &hostMessageHandler{engine: eng, host: h}
	return eng, h, handler
}

// ---------------------------------------------------------------------------
// L15: HandleMessage must drop messages addressed to a different replica
// ---------------------------------------------------------------------------

// TestHandleMessage_DropsMisroutedTo is the L15 regression test: a message
// whose To field names a different replica of a locally-loaded shard must
// be dropped via the unauthorized-message path, not stepped into the local
// raft state. To == 0 (broadcast/control) and To == local replica pass.
func TestHandleMessage_DropsMisroutedTo(t *testing.T) {
	eng, h, _ := newMedlowHandlerFixture(t)

	const shardID, localReplicaID, peerReplicaID = uint64(7), uint64(1), uint64(2)
	node := newEngineNodeWithMembership(t, shardID, localReplicaID, map[uint64]string{
		localReplicaID: "127.0.0.1:6001",
		peerReplicaID:  "127.0.0.1:6002",
	})
	eng.LoadNode(node)
	defer eng.UnloadNode(shardID)

	capEng := &capturingEngineDeliverer{engineDeliverer: eng}
	handler := &hostMessageHandler{engine: capEng, host: h}

	batch := proto.MessageBatch{
		Requests: []proto.Message{
			{ShardID: shardID, From: peerReplicaID, To: 99, Type: proto.Heartbeat},              // misrouted: dropped
			{ShardID: shardID, From: peerReplicaID, To: localReplicaID, Type: proto.Heartbeat},  // addressed here: delivered
			{ShardID: shardID, From: peerReplicaID, To: 0, Type: proto.Heartbeat},               // broadcast/control: delivered
			{ShardID: shardID, From: peerReplicaID, To: peerReplicaID, Type: proto.RequestVote}, // misrouted: dropped
		},
	}
	if err := handler.HandleMessage(batch, authorizeAllSenders); err != nil {
		t.Fatalf("HandleMessage: %v", err)
	}

	capEng.mu.Lock()
	delivered := append([]proto.Message(nil), capEng.delivered...)
	capEng.mu.Unlock()

	if len(delivered) != 2 {
		t.Fatalf("delivered %d messages, want 2 (misrouted To must be dropped)", len(delivered))
	}
	for _, m := range delivered {
		if m.To != 0 && m.To != localReplicaID {
			t.Errorf("message with To=%d was delivered; only To==0 or To==%d may pass", m.To, localReplicaID)
		}
	}
}

// ---------------------------------------------------------------------------
// M16: concurrent same-index snapshot receives must be serialized
// ---------------------------------------------------------------------------

// TestHandleSnapshot_DuplicateInFlightTakesEarlyReturn is the deterministic
// half of the M16 regression: while a receive for (shard, replica, index)
// is claimed, a second HandleSnapshot for the same key must take the same
// early-return path as the LogDB dedup — return nil WITHOUT touching the
// snapshot directory or LogDB. After the claim is released, the receive
// proceeds normally.
func TestHandleSnapshot_DuplicateInFlightTakesEarlyReturn(t *testing.T) {
	eng, h, handler := newMedlowHandlerFixture(t)

	const shardID, localReplicaID, fromReplicaID = uint64(30), uint64(1), uint64(2)
	const snapIndex = uint64(400)
	authorizeSnapshotPath(t, eng, h, shardID, localReplicaID, fromReplicaID)

	chunks := []proto.SnapshotChunk{
		{
			ShardID:    shardID,
			ReplicaID:  localReplicaID,
			From:       fromReplicaID,
			Index:      snapIndex,
			Term:       9,
			ChunkID:    0,
			ChunkCount: 1,
			Data:       []byte("snapshot-body"),
		},
	}

	// Simulate a concurrent receive holding the claim.
	key := snapshotRecvKey{shardID: shardID, replicaID: localReplicaID, index: snapIndex}
	handler.inflightSnapshotRecv.Store(key, struct{}{})

	if err := handler.HandleSnapshot(chunks, authorizeAllSenders); err != nil {
		t.Fatalf("duplicate HandleSnapshot: %v, want nil (dedup-style early return)", err)
	}
	if _, err := h.logdb.GetSnapshot(shardID, localReplicaID); err == nil {
		t.Fatal("duplicate receive must not write LogDB")
	}
	node := eng.GetNode(shardID)
	if node == nil {
		t.Fatal("engine node missing")
	}
	if _, err := os.Stat(node.SnapshotSaveDir(snapIndex)); err == nil {
		t.Fatal("duplicate receive must not create the snapshot directory")
	}

	// Release the claim: the receive now completes and persists.
	handler.inflightSnapshotRecv.Delete(key)
	if err := handler.HandleSnapshot(chunks, authorizeAllSenders); err != nil {
		t.Fatalf("HandleSnapshot after release: %v", err)
	}
	ss, err := h.logdb.GetSnapshot(shardID, localReplicaID)
	if err != nil {
		t.Fatalf("GetSnapshot: %v", err)
	}
	if ss.Index != snapIndex {
		t.Fatalf("persisted snapshot index = %d, want %d", ss.Index, snapIndex)
	}

	// The claim must have been released on exit: a fresh receive of a NEWER
	// index for the same replica must not be blocked.
	if _, loaded := handler.inflightSnapshotRecv.Load(key); loaded {
		t.Fatal("in-flight claim was not released after HandleSnapshot returned")
	}
}

// TestHandleSnapshot_ConcurrentSameIndexReceives is the concurrency half of
// the M16 regression: many simultaneous deliveries of the same snapshot
// must produce exactly one intact on-disk snapshot and a consistent LogDB
// record — no interleaved snapshot.dat writes, and no loser cleanup
// removing the directory the winner recorded in LogDB.
func TestHandleSnapshot_ConcurrentSameIndexReceives(t *testing.T) {
	eng, h, handler := newMedlowHandlerFixture(t)

	const shardID, localReplicaID, fromReplicaID = uint64(31), uint64(1), uint64(2)
	const snapIndex = uint64(500)
	authorizeSnapshotPath(t, eng, h, shardID, localReplicaID, fromReplicaID)

	// Seed the applied marker to the snapshot index so late duplicates that
	// land in the LogDB dedup branch do not contend for the recovery slot
	// the winner's own recovery request holds — the test's subject is the
	// receive serialization, not recovery retry timing.
	if node := eng.GetNode(shardID); node != nil {
		node.SeedLastApplied(snapIndex, 11)
	}

	payload := bytes.Repeat([]byte("concurrent-snapshot-payload-"), 1024)
	chunks := []proto.SnapshotChunk{
		{
			ShardID:    shardID,
			ReplicaID:  localReplicaID,
			From:       fromReplicaID,
			Index:      snapIndex,
			Term:       11,
			ChunkID:    0,
			ChunkCount: 1,
			Data:       payload,
		},
	}

	const receivers = 8
	var wg sync.WaitGroup
	errs := make([]error, receivers)
	for i := 0; i < receivers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			// Each goroutine gets its own chunk slice (the handler indexes
			// into it); Data aliases the shared payload, which is read-only.
			own := make([]proto.SnapshotChunk, len(chunks))
			copy(own, chunks)
			errs[i] = handler.HandleSnapshot(own, authorizeAllSenders)
		}()
	}
	wg.Wait()

	for i, err := range errs {
		if err != nil {
			t.Errorf("receiver %d: %v", i, err)
		}
	}

	ss, err := h.logdb.GetSnapshot(shardID, localReplicaID)
	if err != nil {
		t.Fatalf("GetSnapshot: %v", err)
	}
	if ss.Index != snapIndex {
		t.Fatalf("persisted snapshot index = %d, want %d", ss.Index, snapIndex)
	}

	node := eng.GetNode(shardID)
	dataPath := filepath.Join(node.SnapshotSaveDir(snapIndex), "snapshot.dat")
	got, err := os.ReadFile(dataPath)
	if err != nil {
		t.Fatalf("snapshot.dat missing after concurrent receives (loser cleanup removed the winner's directory?): %v", err)
	}
	if !bytes.Equal(got, payload) {
		t.Fatalf("snapshot.dat corrupted: %d bytes, want %d", len(got), len(payload))
	}
}
