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
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/jeremyhahn/go-quicraft/pkg/config"
	"github.com/jeremyhahn/go-quicraft/pkg/internal/engine"
	"github.com/jeremyhahn/go-quicraft/pkg/internal/raft"
	"github.com/jeremyhahn/go-quicraft/pkg/internal/transport"
	"github.com/jeremyhahn/go-quicraft/pkg/logdb"
	"github.com/jeremyhahn/go-quicraft/pkg/logdb/memdb"
	"github.com/jeremyhahn/go-quicraft/pkg/proto"
)

// These pin the confirmation review's findings against the two-phase receive
// (HIGH-1, MEDIUM-1): a verdict of "abandoned" never licenses deleting an
// artifact raft may have adopted, an install never crosses to a reloaded
// shard's new node, and a failed shard's receive does not wait forever.

func verdictTestChunks(shardID, localReplicaID, fromReplicaID, index uint64) []proto.SnapshotChunk {
	return []proto.SnapshotChunk{{
		ShardID: shardID, ReplicaID: localReplicaID, From: fromReplicaID,
		Index: index, Term: 4, ChunkID: 0, ChunkCount: 1,
		Data:       []byte("payload"),
		Membership: snapshotPathMembership(localReplicaID, fromReplicaID),
	}}
}

// stopAfterDeliver injects the install, then stops the node — the first half
// of UnloadNode — before the step worker necessarily steps it.
type stopAfterDeliver struct {
	engineDeliverer
	node *engine.Node
}

func (d *stopAfterDeliver) DeliverLocalTo(node *engine.Node, msg proto.Message) bool {
	ok := d.engineDeliverer.DeliverLocalTo(node, msg)
	if msg.Type == proto.InstallSnapshot {
		d.node.Stop()
	}
	return ok
}

// TestHandleSnapshot_StopDuringVerdictAdoptsNothingAndKeepsTheArtifact: a
// node stopped after the install is delivered must not be stepped and adopt it
// while the receive, told "abandoned", deletes the artifact raft would then
// depend on. A stopped node adopts nothing, and "abandoned" keeps the
// artifact.
func TestHandleSnapshot_StopDuringVerdictAdoptsNothingAndKeepsTheArtifact(t *testing.T) {
	eng, h, _ := newMedlowHandlerFixture(t)
	const shardID, localReplicaID, fromReplicaID = uint64(43), uint64(1), uint64(2)
	const snapIndex = uint64(900)
	authorizeSnapshotPath(t, eng, h, shardID, localReplicaID, fromReplicaID)
	node := eng.GetNode(shardID)
	handler := &hostMessageHandler{engine: &stopAfterDeliver{engineDeliverer: eng, node: node}, host: h}

	err := handler.HandleSnapshot(verdictTestChunks(shardID, localReplicaID, fromReplicaID, snapIndex), authorizeAllSenders)
	var recvErr *SnapshotReceiveError
	if !errors.As(err, &recvErr) || recvErr.Op != "install" {
		t.Fatalf("HandleSnapshot = %v, want the abandoned install reported", err)
	}
	// Give a stray step every chance to happen.
	time.Sleep(300 * time.Millisecond)
	if got := node.CommittedIndex(); got >= snapIndex {
		t.Fatalf("a stopped node adopted snapshot %d (committed %d)", snapIndex, got)
	}
	if _, statErr := os.Stat(node.SnapshotSaveDir(snapIndex)); statErr != nil {
		t.Fatalf("the artifact was deleted on an abandoned verdict: %v", statErr)
	}
}

// swappingDeliverer reloads the shard — unload the node, load a fresh one on
// the same snapshot base — just before the install is injected, as a
// StopShard/StartShard racing a long receive would.
type swappingDeliverer struct {
	engineDeliverer
	eng     *engine.Engine
	newNode *engine.Node
	once    sync.Once
}

func (d *swappingDeliverer) DeliverLocalTo(node *engine.Node, msg proto.Message) bool {
	if msg.Type == proto.InstallSnapshot {
		d.once.Do(func() {
			d.eng.UnloadNode(msg.ShardID)
			d.eng.LoadNode(d.newNode)
		})
	}
	return d.engineDeliverer.DeliverLocalTo(node, msg)
}

// TestHandleSnapshot_ReloadDuringReceiveNeverReachesTheNewNode: delivery by
// shard ID handed the install to the reloaded shard's NEW node, which adopted
// it while the receive — waiting on the old node — deleted the artifact. The
// install now goes to exactly the node its verdict is awaited on, and a
// stopped node accepts nothing, so the new node never sees it.
func TestHandleSnapshot_ReloadDuringReceiveNeverReachesTheNewNode(t *testing.T) {
	eng, h, _ := newMedlowHandlerFixture(t)
	const shardID, localReplicaID, fromReplicaID = uint64(43), uint64(1), uint64(2)
	const snapIndex = uint64(900)
	authorizeSnapshotPath(t, eng, h, shardID, localReplicaID, fromReplicaID)
	old := eng.GetNode(shardID)

	voters := snapshotPathMembership(localReplicaID, fromReplicaID).Addresses
	lr := logdb.NewLogReader(shardID, localReplicaID, memdb.New())
	if err := lr.ApplySnapshot(proto.Snapshot{Membership: proto.Membership{Addresses: voters, Identities: fixtureIdentities(voters)}}); err != nil {
		t.Fatalf("ApplySnapshot: %v", err)
	}
	cfg := config.Config{ShardID: shardID, ReplicaID: localReplicaID}
	cfg.SetDefaults()
	peer, err := raft.NewPeer(cfg, lr, nil)
	if err != nil {
		t.Fatalf("NewPeer: %v", err)
	}
	fresh := engine.NewNode(peer, nil, lr, cfg, nil, old.SnapshotDir(), nil)

	handler := &hostMessageHandler{engine: &swappingDeliverer{engineDeliverer: eng, eng: eng, newNode: fresh}, host: h}
	if err := handler.HandleSnapshot(verdictTestChunks(shardID, localReplicaID, fromReplicaID, snapIndex), authorizeAllSenders); err == nil {
		t.Fatal("HandleSnapshot succeeded although its node was unloaded before the install could reach it")
	}
	time.Sleep(300 * time.Millisecond)
	if got := fresh.CommittedIndex(); got >= snapIndex {
		t.Fatalf("the reloaded shard's new node adopted snapshot %d (committed %d) from a receive awaiting the old node", snapIndex, got)
	}
}

// TestHandleSnapshot_FailedShardReturnsAtOnce: a failed shard steps nothing,
// so its install never got a verdict and the receive blocked — holding a
// host-wide transport receive slot and its memory budget — until an operator
// reloaded the shard.
func TestHandleSnapshot_FailedShardReturnsAtOnce(t *testing.T) {
	eng, h, _ := newMedlowHandlerFixture(t)
	const shardID, localReplicaID, fromReplicaID = uint64(43), uint64(1), uint64(2)
	authorizeSnapshotPath(t, eng, h, shardID, localReplicaID, fromReplicaID)
	handler := &hostMessageHandler{engine: eng, host: h}
	eng.GetNode(shardID).Fail(errors.New("latched for the test"))

	done := make(chan error, 1)
	go func() {
		done <- handler.HandleSnapshot(verdictTestChunks(shardID, localReplicaID, fromReplicaID, 900), authorizeAllSenders)
	}()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("HandleSnapshot accepted a snapshot for a failed shard")
		}
		// An ordinary shard state, not an authorization failure: a leader
		// retransmits to a failed replica as a matter of course.
		var unauthorized *UnauthorizedMessageError
		if errors.As(err, &unauthorized) || !errors.Is(err, ErrShardFailed) {
			t.Fatalf("HandleSnapshot = %v, want ErrShardFailed and not an authorization failure", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("HandleSnapshot still blocked 2s after a snapshot arrived for a failed shard")
	}
}

// TestHandleSnapshot_StoppingShardStagesNothing: a stopped incarnation may
// still hold an adopted-but-unrecorded install in its commit pipeline, and
// that install's artifact is retained, unrecorded, at the canonical path. A
// same-index retry against the stopping node must not reach the publish step,
// which reclaims any unrecorded directory there as an orphan — moments before
// the old pipeline's record naming it lands. The receive is refused before
// staging anything; the retry waits for the next incarnation, which UnloadNode
// releases only once the old one can no longer write LogDB.
func TestHandleSnapshot_StoppingShardStagesNothing(t *testing.T) {
	eng, h, _ := newMedlowHandlerFixture(t)
	const shardID, localReplicaID, fromReplicaID = uint64(43), uint64(1), uint64(2)
	const snapIndex = uint64(900)
	authorizeSnapshotPath(t, eng, h, shardID, localReplicaID, fromReplicaID)
	node := eng.GetNode(shardID)
	handler := &hostMessageHandler{engine: eng, host: h}

	// The first receive's retained artifact.
	retained := node.SnapshotSaveDir(snapIndex)
	if err := os.MkdirAll(retained, 0o700); err != nil {
		t.Fatalf("MkdirAll: %v", err)
	}
	marker := filepath.Join(retained, "adopted")
	if err := os.WriteFile(marker, []byte("adopted, record in flight"), 0o600); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}
	node.Stop()

	err := handler.HandleSnapshot(verdictTestChunks(shardID, localReplicaID, fromReplicaID, snapIndex), authorizeAllSenders)
	// Refused as an ordinary shard state, not an authorization failure: a
	// leader retransmits to a restarting replica as a matter of course.
	var unauthorized *UnauthorizedMessageError
	if errors.As(err, &unauthorized) || !errors.Is(err, ErrShardNotReady) {
		t.Fatalf("HandleSnapshot = %v, want ErrShardNotReady and not an authorization failure", err)
	}
	if _, statErr := os.Stat(marker); statErr != nil {
		t.Fatalf("the retained artifact was reclaimed by a receive on a stopping shard: %v", statErr)
	}
}

// TestHandleSnapshot_AuthorizationIsCheckedBeforeShardState: the failed and
// stopping refusals are reported as ordinary shard states, so they must not
// run ahead of the authorization gates — an unauthorized snapshot that lands
// while the shard is failed or stopping is still refused, and counted, as
// unauthorized.
func TestHandleSnapshot_AuthorizationIsCheckedBeforeShardState(t *testing.T) {
	for _, tc := range []struct {
		name  string
		state func(*engine.Node)
	}{
		{"failed", func(n *engine.Node) { n.Fail(errors.New("latched for the test")) }},
		{"stopping", func(n *engine.Node) { n.Stop() }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			eng, h, _ := newMedlowHandlerFixture(t)
			const shardID, localReplicaID, fromReplicaID = uint64(43), uint64(1), uint64(2)
			authorizeSnapshotPath(t, eng, h, shardID, localReplicaID, fromReplicaID)
			handler := &hostMessageHandler{engine: eng, host: h}
			tc.state(eng.GetNode(shardID))

			// Addressed to a replica this host does not own.
			err := handler.HandleSnapshot(verdictTestChunks(shardID, localReplicaID+7, fromReplicaID, 900), authorizeAllSenders)
			var unauthorized *UnauthorizedMessageError
			if !errors.As(err, &unauthorized) {
				t.Fatalf("HandleSnapshot = %v, want the misaddressed snapshot refused as unauthorized "+
					"whatever the shard's state", err)
			}
		})
	}
}

// snapshotDropRecorder records the shard label of each unauthorized-snapshot
// drop.
type snapshotDropRecorder struct {
	NoOpMetricsCollector
	mu     sync.Mutex
	shards []uint64
}

func (r *snapshotDropRecorder) IncUnauthorizedSnapshotDropped(shardID, _ uint64) {
	r.mu.Lock()
	r.shards = append(r.shards, shardID)
	r.mu.Unlock()
}

// TestHandleSnapshot_UnloadedShardIsNotAMetricLabel: a snapshot naming a shard
// this host has not loaded carries a shard ID nothing has verified. Counting
// the drop under it let any cluster-certificate holder mint an unbounded
// number of metric series by cycling shard IDs; it is counted under shard 0.
// A loaded shard's drop keeps its own label.
func TestHandleSnapshot_UnloadedShardIsNotAMetricLabel(t *testing.T) {
	eng, h, _ := newMedlowHandlerFixture(t)
	const shardID, localReplicaID, fromReplicaID = uint64(43), uint64(1), uint64(2)
	authorizeSnapshotPath(t, eng, h, shardID, localReplicaID, fromReplicaID)
	rec := &snapshotDropRecorder{}
	h.metrics = rec
	handler := &hostMessageHandler{engine: eng, host: h}

	for _, unloaded := range []uint64{1_000_001, 1_000_002} {
		_ = handler.HandleSnapshot(verdictTestChunks(unloaded, localReplicaID, fromReplicaID, 900), authorizeAllSenders)
	}
	// A loaded shard, misaddressed: still an authorization failure, labelled.
	_ = handler.HandleSnapshot(verdictTestChunks(shardID, localReplicaID+7, fromReplicaID, 900), authorizeAllSenders)

	rec.mu.Lock()
	defer rec.mu.Unlock()
	want := []uint64{0, 0, shardID}
	if !slices.Equal(rec.shards, want) {
		t.Fatalf("unauthorized-snapshot drops counted under shards %v, want %v", rec.shards, want)
	}
}

// TestAuthorizeSnapshot_AdmitsFromTheHeaderAlone: the transport asks the host
// to authorize a snapshot's sender from the stream header, before it takes a
// receive slot or any memory. The host applies HandleSnapshot's own gates
// there, so a sender the shard admits proceeds and one it does not — a
// certificate holder with the wrong identity, a misaddressed transfer, a shard
// not loaded — is refused before the transfer costs the receiver anything.
func TestAuthorizeSnapshot_AdmitsFromTheHeaderAlone(t *testing.T) {
	eng, h, _ := newMedlowHandlerFixture(t)
	const shardID, localReplicaID, fromReplicaID = uint64(43), uint64(1), uint64(2)
	authorizeSnapshotPath(t, eng, h, shardID, localReplicaID, fromReplicaID)
	handler := &hostMessageHandler{engine: eng, host: h}

	if err := handler.AuthorizeSnapshot(shardID, localReplicaID, fromReplicaID, 900, authorizeAllSenders); err != nil {
		t.Fatalf("AuthorizeSnapshot refused the committed sender: %v", err)
	}

	// A shard that is merely failed is an ordinary state: marked so the
	// transport reports it quietly, apart from unauthorized transfers.
	eng.GetNode(shardID).Fail(errors.New("latched for the test"))
	if err := handler.AuthorizeSnapshot(shardID, localReplicaID, fromReplicaID, 900, authorizeAllSenders); !errors.Is(err, transport.ErrSnapshotNotAccepting) {
		t.Fatalf("AuthorizeSnapshot on a failed shard = %v, want ErrSnapshotNotAccepting", err)
	}

	stranger := transport.PeerAuth{
		Identity:          "spki:5555555555555555555555555555555555555555555555555555555555555555",
		AuthorizesAddress: func(string) bool { return true },
	}
	for _, tc := range []struct {
		name                   string
		shard, replica, sender uint64
		peer                   transport.PeerAuth
	}{
		{"a certificate holder the shard does not admit", shardID, localReplicaID, fromReplicaID, stranger},
		{"a transfer addressed to another replica", shardID, localReplicaID + 7, fromReplicaID, authorizeAllSenders},
		{"a shard not loaded", shardID + 1, localReplicaID, fromReplicaID, authorizeAllSenders},
	} {
		var unauthorized *UnauthorizedMessageError
		if err := handler.AuthorizeSnapshot(tc.shard, tc.replica, tc.sender, 900, tc.peer); !errors.As(err, &unauthorized) {
			t.Fatalf("%s: AuthorizeSnapshot = %v, want an authorization refusal", tc.name, err)
		}
	}
}

// stopThenRefuse stops the node the moment the install is offered and refuses
// the delivery: the shape of a StopShard landing between a receive's publish
// and its injection.
type stopThenRefuse struct {
	engineDeliverer
}

func (d *stopThenRefuse) DeliverLocalTo(node *engine.Node, msg proto.Message) bool {
	if msg.Type == proto.InstallSnapshot {
		node.Stop()
		return false
	}
	return d.engineDeliverer.DeliverLocalTo(node, msg)
}

// TestHandleSnapshot_StoppedShardKeepsThePublishedDirectory: a receive that
// published and then failed must not discard the canonical directory
// unconditionally. On a node stopped in between, that directory may be named
// by a record the stopped incarnation's commit pipeline is still writing — its
// own adopted install at this index, which this receive's publish replaced —
// so the discard takes the write lease, and a stopped node keeps it.
func TestHandleSnapshot_StoppedShardKeepsThePublishedDirectory(t *testing.T) {
	eng, h, _ := newMedlowHandlerFixture(t)
	const shardID, localReplicaID, fromReplicaID = uint64(43), uint64(1), uint64(2)
	const snapIndex = uint64(900)
	authorizeSnapshotPath(t, eng, h, shardID, localReplicaID, fromReplicaID)
	node := eng.GetNode(shardID)
	handler := &hostMessageHandler{engine: &stopThenRefuse{engineDeliverer: eng}, host: h}

	err := handler.HandleSnapshot(verdictTestChunks(shardID, localReplicaID, fromReplicaID, snapIndex), authorizeAllSenders)
	var recvErr *SnapshotReceiveError
	if !errors.As(err, &recvErr) || recvErr.Op != "deliver_install" {
		t.Fatalf("HandleSnapshot = %v, want the refused delivery reported", err)
	}
	if _, statErr := os.Stat(node.SnapshotSaveDir(snapIndex)); statErr != nil {
		t.Fatalf("the published directory was discarded on a stopped node: %v", statErr)
	}
}

// recordThenRefuse stands in for a local save that publishes and records the
// receive's index while the receive is in flight, after which the install is
// refused.
type recordThenRefuse struct {
	engineDeliverer
	record func(dir string)
	dir    string
}

func (d *recordThenRefuse) DeliverLocalTo(node *engine.Node, msg proto.Message) bool {
	if msg.Type == proto.InstallSnapshot {
		d.record(d.dir)
		return false
	}
	return d.engineDeliverer.DeliverLocalTo(node, msg)
}

// TestHandleSnapshot_FailedReceiveKeepsADirectoryLogDBRecords: the write lease
// is shared, so a local save can publish and record the very index a receive
// published while the receive awaited its verdict. The failed receive's
// discard then deleted the artifact LogDB named, with the log below it already
// compacted. The discard now runs under the snapshot-directory lock and keeps
// a directory the record names.
func TestHandleSnapshot_FailedReceiveKeepsADirectoryLogDBRecords(t *testing.T) {
	eng, h, _ := newMedlowHandlerFixture(t)
	const shardID, localReplicaID, fromReplicaID = uint64(43), uint64(1), uint64(2)
	const snapIndex = uint64(900)
	authorizeSnapshotPath(t, eng, h, shardID, localReplicaID, fromReplicaID)
	node := eng.GetNode(shardID)
	dir := node.SnapshotSaveDir(snapIndex)

	var once sync.Once // the delivery is retried; the racing save records once
	d := &recordThenRefuse{engineDeliverer: eng, dir: dir, record: func(dir string) {
		once.Do(func() {
			if err := h.logdb.SaveSnapshot(shardID, localReplicaID, logdb.Snapshot{
				Index: snapIndex, Term: 4, Filepath: dir,
			}); err != nil {
				t.Errorf("recording the racing save: %v", err)
			}
		})
	}}
	handler := &hostMessageHandler{engine: d, host: h}

	err := handler.HandleSnapshot(verdictTestChunks(shardID, localReplicaID, fromReplicaID, snapIndex), authorizeAllSenders)
	var recvErr *SnapshotReceiveError
	if !errors.As(err, &recvErr) || recvErr.Op != "deliver_install" {
		t.Fatalf("HandleSnapshot = %v, want the refused delivery reported", err)
	}
	if _, statErr := os.Stat(dir); statErr != nil {
		t.Fatalf("DATA LOSS: the failed receive discarded the directory LogDB records: %v", statErr)
	}
}

// claimProbe records, when the install is offered to raft, whether the
// receive holds its claim on the index — the claim must span the wait for
// raft's verdict, during which the published directory is unrecorded.
type claimProbe struct {
	engineDeliverer
	index   uint64
	claimed bool
	checked bool
}

func (d *claimProbe) DeliverLocalTo(node *engine.Node, msg proto.Message) bool {
	if msg.Type == proto.InstallSnapshot && !d.checked {
		d.checked = true
		node.LockSnapshotDir()
		d.claimed = node.IsSnapshotDirClaimed(d.index)
		node.UnlockSnapshotDir()
	}
	return d.engineDeliverer.DeliverLocalTo(node, msg)
}

// TestHandleSnapshot_ClaimsItsIndexUntilSettled: a receive's published
// directory is unrecorded until raft's verdict, and a local save at the same
// index treats an unclaimed, unrecorded directory as an orphan to replace. The
// receive claims its index from the publish until it returns.
func TestHandleSnapshot_ClaimsItsIndexUntilSettled(t *testing.T) {
	eng, h, _ := newMedlowHandlerFixture(t)
	const shardID, localReplicaID, fromReplicaID = uint64(43), uint64(1), uint64(2)
	const snapIndex = uint64(900)
	authorizeSnapshotPath(t, eng, h, shardID, localReplicaID, fromReplicaID)
	node := eng.GetNode(shardID)
	probe := &claimProbe{engineDeliverer: eng, index: snapIndex}
	handler := &hostMessageHandler{engine: probe, host: h}

	_ = handler.HandleSnapshot(verdictTestChunks(shardID, localReplicaID, fromReplicaID, snapIndex), authorizeAllSenders)
	if !probe.checked {
		t.Fatal("the install was never offered to raft; the probe proves nothing")
	}
	if !probe.claimed {
		t.Fatal("the receive awaited raft's verdict without claiming its index: a local save could replace its published directory")
	}
	node.LockSnapshotDir()
	defer node.UnlockSnapshotDir()
	if node.IsSnapshotDirClaimed(snapIndex) {
		t.Fatal("the receive left its claim behind: saves at that index would be refused for the incarnation's life")
	}
}
