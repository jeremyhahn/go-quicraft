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
	"errors"
	"log/slog"
	"maps"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

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

// dropCountRecorder counts unauthorized-message drops.
type dropCountRecorder struct {
	NoOpMetricsCollector
	drops atomic.Int64
}

func (r *dropCountRecorder) IncUnauthorizedMessageDropped(uint64, uint64, string) {
	r.drops.Add(1)
}

// TestHandleMessage_DropWarningsAreRateLimited: a batch carries up to
// proto.MaxMessagesPerBatch messages, so a Warn line per drop in the
// authorization path would let one frame from a peer the shard does not admit
// write thousands of synchronous lines. The warning is sampled; the counter
// still records every drop.
func TestHandleMessage_DropWarningsAreRateLimited(t *testing.T) {
	eng, h, _ := newMedlowHandlerFixture(t)
	const shardID, localReplicaID, peerReplicaID = uint64(7), uint64(1), uint64(2)
	node := newEngineNodeWithMembership(t, shardID, localReplicaID, map[uint64]string{
		localReplicaID: "127.0.0.1:6001",
		peerReplicaID:  "127.0.0.1:6002",
	})
	eng.LoadNode(node)
	defer eng.UnloadNode(shardID)
	rec := &dropCountRecorder{}
	h.metrics = rec
	handler := &hostMessageHandler{engine: eng, host: h}

	var logs bytes.Buffer
	prev := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&logs, nil)))
	defer slog.SetDefault(prev)

	const n = 5000
	batch := proto.MessageBatch{Requests: make([]proto.Message, n)}
	for i := range batch.Requests {
		batch.Requests[i] = proto.Message{ShardID: shardID, From: peerReplicaID, To: 99, Type: proto.Heartbeat} // misrouted
	}
	if err := handler.HandleMessage(batch, authorizeAllSenders); err != nil {
		t.Fatalf("HandleMessage: %v", err)
	}

	if lines := strings.Count(logs.String(), "rejected misrouted message"); lines != 1 {
		t.Fatalf("%d warning lines for %d dropped messages, want 1", lines, n)
	}
	if got := rec.drops.Load(); got != n {
		t.Fatalf("counted %d drops, want every one of %d", got, n)
	}
}

// TestReplicaIdentity_IsTheIdentityASenderIsHeldTo: the transport writes a
// message, or a snapshot, only to a connection whose peer presents the
// identity replicaIdentity returns for its destination. It must be the
// identity membership binds to the replica — what an inbound sender from it is
// held to — and "" for a shard not loaded here, so nothing is sent there.
func TestReplicaIdentity_IsTheIdentityASenderIsHeldTo(t *testing.T) {
	eng, h, _ := newMedlowHandlerFixture(t)
	const shardID, localReplicaID, peerReplicaID = uint64(7), uint64(1), uint64(2)
	node := newEngineNodeWithMembership(t, shardID, localReplicaID, map[uint64]string{
		localReplicaID: "127.0.0.1:6001",
		peerReplicaID:  "127.0.0.1:6002",
	})
	eng.LoadNode(node)
	defer eng.UnloadNode(shardID)
	handler := &hostMessageHandler{engine: eng, host: h}

	if got := handler.replicaIdentity(shardID, peerReplicaID); got != fixtureIdentity {
		t.Fatalf("replicaIdentity(%d, %d) = %q, want the bound identity %q", shardID, peerReplicaID, got, fixtureIdentity)
	}
	if got := handler.replicaIdentity(shardID+1, peerReplicaID); got != "" {
		t.Fatalf("replicaIdentity of a shard not loaded = %q, want none", got)
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
			// The shard's own committed configuration, as a real sender stamps
			// it: the receive that completes below delivers this to raft, which
			// adopts it wholesale. See snapshotPathMembership.
			Membership: snapshotPathMembership(localReplicaID, fromReplicaID),
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
//
// Every receiver must return nil. Authorization runs BEFORE the serialization
// claim, so a loser is refused only if the winner changed the committed
// membership out from under it — and these eight deliveries carry ONE
// membership, the shard's own, so there is nothing for the restore to change.
// See snapshotPathMembership for why the chunk must carry it, and
// TestHandleSnapshot_RestoredMembershipGovernsLaterReceives for what a snapshot
// that genuinely drops the sender does instead.
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
	membership := snapshotPathMembership(localReplicaID, fromReplicaID)
	chunks := []proto.SnapshotChunk{
		{
			ShardID:    shardID,
			ReplicaID:  localReplicaID,
			From:       fromReplicaID,
			Index:      snapIndex,
			Term:       11,
			ChunkID:    0,
			ChunkCount: 1,
			// The shard's own committed configuration, exactly as a real
			// sender stamps it. Leaving it empty is what made this test flaky:
			// the winner's InstallSnapshot is adopted by raft on the engine
			// worker, asynchronously, and an empty membership erases the very
			// binding the losers are still being authorized against.
			Membership: membership,
			Data:       payload,
		},
	}

	const receivers = 8
	var wg sync.WaitGroup
	errs := make([]error, receivers)
	// `i` is captured directly, with no `i := i` shadow. That is correct here
	// and deliberately not "fixed": since Go 1.22 the for-clause variable is
	// PER-ITERATION, and this module declares go 1.27.0, so each goroutine
	// closes over its own copy. Under pre-1.22 semantics the same code writes
	// through a shared variable and can panic on errs[8]; that is the footgun
	// the language change removed, and it is why a reader out of version
	// context may flag it.
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
	// Guard the fixture itself, deterministically. The membership the chunks
	// carry is what raft adopts, and dropping it reintroduces the flake as a
	// scheduling-dependent failure that no single run reliably shows. Asserting
	// it on the durable record turns that omission into a hard failure here.
	if !maps.Equal(ss.Membership.Addresses, membership.Addresses) {
		t.Fatalf("persisted snapshot membership = %v, want %v (the chunks must carry the "+
			"shard's committed configuration; an empty one unauthorizes the sender)",
			ss.Membership.Addresses, membership.Addresses)
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

// TestHandleSnapshot_RestoredMembershipGovernsLaterReceives pins the contract
// the test above depends on, and states the half of it that is NOT a bug: a
// snapshot REPLACES the committed membership, and the replacement — not the
// configuration that authorized the snapshot carrying it — is what every later
// receive is judged against.
//
// So a sender that ships a configuration no longer naming itself has, by that
// act, given up its own admission. Refusing it afterwards is the documented
// fail-closed rule (committedSenderBinding: a replica the configuration does
// not name at all is a drop), not an ordering defect: the snapshot channel is
// the one inbound path that rewrites the authorization source, which is exactly
// why HandleSnapshot re-reads committed membership per receive instead of
// caching a verdict.
//
// The adoption is asynchronous — HandleSnapshot queues the InstallSnapshot to
// the engine worker and returns — so this waits for it rather than assuming it
// has landed. That asynchrony is the flake's timing element: a concurrent
// receive authorized before the adoption passes and one authorized after does
// not, which is why a same-snapshot fixture must not change the configuration
// at all.
func TestHandleSnapshot_RestoredMembershipGovernsLaterReceives(t *testing.T) {
	eng, h, handler := newMedlowHandlerFixture(t)

	const shardID, localReplicaID, fromReplicaID = uint64(32), uint64(1), uint64(2)
	authorizeSnapshotPath(t, eng, h, shardID, localReplicaID, fromReplicaID)

	node := eng.GetNode(shardID)
	if node == nil {
		t.Fatal("engine node missing")
	}
	if _, bound := node.MemberBinding(fromReplicaID); !bound {
		t.Fatalf("precondition: replica %d must be a committed member before the receive", fromReplicaID)
	}

	// A configuration that keeps this replica and DROPS the sender — the shape
	// a snapshot taken after the sender was removed carries.
	withoutSender := proto.Membership{
		Addresses: map[uint64]string{localReplicaID: snapshotPathLocalAddr},
	}
	chunk := func(index uint64, ms proto.Membership) []proto.SnapshotChunk {
		return []proto.SnapshotChunk{{
			ShardID:    shardID,
			ReplicaID:  localReplicaID,
			From:       fromReplicaID,
			Index:      index,
			Term:       12,
			ChunkID:    0,
			ChunkCount: 1,
			Membership: ms,
			Data:       []byte("membership-rewriting-snapshot"),
		}}
	}

	// Authorized against the configuration that still names the sender.
	if err := handler.HandleSnapshot(chunk(600, withoutSender), authorizeAllSenders); err != nil {
		t.Fatalf("first receive: %v", err)
	}
	if !waitUntil(5*time.Second, func() bool {
		_, bound := node.MemberBinding(fromReplicaID)
		return !bound
	}) {
		t.Fatalf("raft never adopted the snapshot's membership: replica %d is still bound", fromReplicaID)
	}

	// The sender is now a replica the committed configuration does not name, so
	// its next snapshot is refused — on the membership IT installed.
	err := handler.HandleSnapshot(chunk(601, withoutSender), authorizeAllSenders)
	var unauthorized *UnauthorizedMessageError
	if !errors.As(err, &unauthorized) {
		t.Fatalf("second receive err = %v, want UnauthorizedMessageError", err)
	}
	if unauthorized.From != fromReplicaID || unauthorized.ShardID != shardID {
		t.Fatalf("refusal names shard %d from %d, want shard %d from %d",
			unauthorized.ShardID, unauthorized.From, shardID, fromReplicaID)
	}
	// Refused BEFORE anything is persisted: the durable record still names the
	// snapshot that was authorized.
	ss, err := h.logdb.GetSnapshot(shardID, localReplicaID)
	if err != nil {
		t.Fatalf("GetSnapshot: %v", err)
	}
	if ss.Index != 600 {
		t.Fatalf("persisted snapshot index = %d, want 600 (the refused receive must not persist)", ss.Index)
	}
}
