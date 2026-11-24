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
	"context"
	"encoding/binary"
	"errors"
	"io"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/jeremyhahn/go-quicraft/pkg/config"
	"github.com/jeremyhahn/go-quicraft/pkg/internal/raft"
	"github.com/jeremyhahn/go-quicraft/pkg/logdb"
	"github.com/jeremyhahn/go-quicraft/pkg/proto"
)

// assertNoSnapshotArtifact fails if any snapshot data file exists anywhere
// under base. A save that fails closed must leave nothing on disk — least of
// all the plaintext data file it wrote before the failure was detected.
func assertNoSnapshotArtifact(t *testing.T, base string) {
	t.Helper()
	err := filepath.Walk(base, func(path string, info os.FileInfo, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if !info.IsDir() && info.Name() == snapshotDataFile {
			t.Errorf("snapshot artifact left on disk at %s", path)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("walk %s: %v", base, err)
	}
}

// seedRecoverableSnapshot writes a snapshotTestSnapshotter artifact holding
// state and records it in ldb at index/term, returning the snapshot directory.
func seedRecoverableSnapshot(t *testing.T, ldb *snapshotTestLogDB,
	shardID, replicaID, index, term, state uint64) string {
	t.Helper()
	dir := t.TempDir()
	var buf [8]byte
	binary.LittleEndian.PutUint64(buf[:], state)
	if err := os.WriteFile(filepath.Join(dir, snapshotDataFile), buf[:], 0o600); err != nil {
		t.Fatalf("write snapshot artifact: %v", err)
	}
	if err := writeTestSnapshotMetadata(dir, index, term, shardID, replicaID, 0,
		config.NoCompression); err != nil {
		t.Fatalf("write snapshot metadata: %v", err)
	}
	if err := ldb.SaveSnapshot(shardID, replicaID, logdb.Snapshot{
		Index:    index,
		Term:     term,
		Filepath: dir,
	}); err != nil {
		t.Fatalf("SaveSnapshot: %v", err)
	}
	return dir
}

// newRecoveryTestNode builds a node backed by a real Peer whose raft log is
// empty, so its committed and processed markers both start at 0.
func newRecoveryTestNode(t *testing.T, shardID, replicaID uint64,
	ss Snapshotter) (*Node, *raft.Peer) {
	t.Helper()
	lr := logdb.NewLogReader(shardID, replicaID, newTestLogDB())
	ms := proto.Membership{Addresses: map[uint64]string{replicaID: "127.0.0.1:0", 2: "127.0.0.1:1"}}
	lr.ApplySnapshot(proto.Snapshot{Membership: ms})
	cfg := config.Config{ShardID: shardID, ReplicaID: replicaID, CompactionOverhead: 50}
	cfg.SetDefaults()
	peer, err := raft.NewPeer(cfg, lr, nil)
	if err != nil {
		t.Fatalf("NewPeer: %v", err)
	}
	node := NewNode(peer, nil, lr, cfg, ss, t.TempDir(), nil)
	// StartShard creates the replica's snapshot directory, where a recovery
	// decrypts its scratch.
	if err := os.MkdirAll(node.ReplicaSnapshotDir(), 0o750); err != nil {
		t.Fatalf("mkdir replica snapshot dir: %v", err)
	}
	return node, peer
}

// installSnapshotIntoLog advances the peer's committed AND processed markers
// to index by delivering the InstallSnapshot the step worker would deliver.
func installSnapshotIntoLog(t *testing.T, node *Node, peer *raft.Peer, index, term uint64) {
	t.Helper()
	msg := proto.Message{
		Type: proto.InstallSnapshot, Origin: proto.OriginLocal,
		From:    2,
		To:      node.ReplicaID(),
		ShardID: node.ShardID(),
		Term:    term,
		Snapshot: proto.Snapshot{
			Index:      index,
			Term:       term,
			Membership: proto.Membership{Addresses: map[uint64]string{node.ReplicaID(): "127.0.0.1:0", 2: "127.0.0.1:1"}},
		},
	}
	node.raftMu.Lock()
	defer node.raftMu.Unlock()
	if err := peer.Handle(msg); err != nil {
		t.Fatalf("install snapshot into raft log: %v", err)
	}
	// The step worker publishes the log markers after touching the Peer; the
	// snapshot pool reads only what is published, never the Peer itself.
	node.publishLogMarkers()
}

// TestSnapshotPool_RecoverSnapshot_RefusesSnapshotBehindApplyFrontier pins the
// lower bound. A leader whose remote.next was driven below the follower's real
// progress (leader change, rejection walk-down, observer promotion) ships an
// InstallSnapshot at an index the follower is already past. Raft rejects it;
// recovering it anyway would REPLACE the state machine with older state and
// regress lastApplied, so the next committed batch trips ErrApplyGap and fails
// the shard permanently.
func TestSnapshotPool_RecoverSnapshot_RefusesSnapshotBehindApplyFrontier(t *testing.T) {
	stopC := make(chan struct{})
	defer close(stopC)
	ldb := newSnapshotTestLogDB()
	p := newSnapshotPool(2, ldb, nil, nil, nil, stopC, nil)

	seedRecoverableSnapshot(t, ldb, 1, 1, 500, 4, 500)

	ss := newSnapshotTestSnapshotter(900)
	node, peer := newRecoveryTestNode(t, 1, 1, ss)
	node.SeedLastApplied(900, 6)
	installSnapshotIntoLog(t, node, peer, 900, 6)

	err := p.recoverSnapshot(snapshotRequest{shardID: 1, replicaID: 1, node: node})

	var stale *SnapshotRecoveryStaleError
	if !errors.As(err, &stale) {
		t.Fatalf("recoverSnapshot error = %T (%v), want *SnapshotRecoveryStaleError", err, err)
	}
	if stale.SnapshotIndex != 500 || stale.LastApplied != 900 {
		t.Errorf("stale error = %+v, want SnapshotIndex 500 / LastApplied 900", stale)
	}
	if got := ss.state.Load(); got != 900 {
		t.Errorf("state machine rolled back to %d; it must be untouched at 900", got)
	}
	if got := node.LastApplied(); got != 900 {
		t.Errorf("node.LastApplied regressed to %d, want 900", got)
	}
	if got := node.lastSnapshotIndex.Load(); got != 900 {
		t.Errorf("node.lastSnapshotIndex regressed to %d, want 900", got)
	}
}

// TestSnapshotPool_RecoverSnapshot_ChecksLogBeforeTouchingStateMachine pins the
// ORDER of the upper-bound check. Were recoverSnapshot to call
// RecoverFromSnapshot first and read the committed index afterwards, the state
// machine would be replaced while the markers were refused, and nothing
// would retry.
func TestSnapshotPool_RecoverSnapshot_ChecksLogBeforeTouchingStateMachine(t *testing.T) {
	stopC := make(chan struct{})
	defer close(stopC)
	ldb := newSnapshotTestLogDB()
	p := newSnapshotPool(2, ldb, nil, nil, nil, stopC, nil)

	seedRecoverableSnapshot(t, ldb, 1, 1, 100, 7, 100)

	ss := newSnapshotTestSnapshotter(0)
	node, _ := newRecoveryTestNode(t, 1, 1, ss)

	err := p.recoverSnapshot(snapshotRequest{shardID: 1, replicaID: 1, node: node})

	var behind *SnapshotRecoveryBehindError
	if !errors.As(err, &behind) {
		t.Fatalf("recoverSnapshot error = %T (%v), want *SnapshotRecoveryBehindError", err, err)
	}
	if !behind.Retryable() {
		t.Error("SnapshotRecoveryBehindError must report itself retryable")
	}
	if got := ss.state.Load(); got != 0 {
		t.Errorf("state machine was recovered to %d before the log check; it must be untouched", got)
	}
	if got := node.LastApplied(); got != 0 {
		t.Errorf("node.LastApplied = %d, want 0 (markers must not move)", got)
	}

	// Once the step worker installs the snapshot, the SAME request succeeds.
	installSnapshotIntoLog(t, node, node.Peer(), 100, 7)
	if retryErr := p.recoverSnapshot(snapshotRequest{shardID: 1, replicaID: 1, node: node}); retryErr != nil {
		t.Fatalf("retry after install failed: %v", retryErr)
	}
	if got := ss.state.Load(); got != 100 {
		t.Errorf("state machine state = %d after retry, want 100", got)
	}
	if got := node.LastApplied(); got != 100 {
		t.Errorf("node.LastApplied = %d after retry, want 100", got)
	}
}

// signallingSnapshotter reports entry into RecoverFromSnapshot, so a test can
// observe that the state machine was touched rather than infer it.
type signallingSnapshotter struct {
	*snapshotTestSnapshotter
	started chan struct{}
	once    sync.Once
}

func newSignallingSnapshotter(state uint64) *signallingSnapshotter {
	return &signallingSnapshotter{
		snapshotTestSnapshotter: newSnapshotTestSnapshotter(state),
		started:                 make(chan struct{}),
	}
}

func (s *signallingSnapshotter) RecoverFromSnapshot(ctx context.Context, r io.Reader, stopper <-chan struct{}) error {
	s.once.Do(func() { close(s.started) })
	return s.snapshotTestSnapshotter.RecoverFromSnapshot(ctx, r, stopper)
}

// TestSnapshotPool_Recovery_WaitsForInstallBeforeGatingStepWorker pins the
// gate ordering end to end. handleRequest raises the recovering flag on
// dequeue, and the step worker skips a recovering node ENTIRELY — including
// its inbox drain — so the InstallSnapshot that satisfies the committed check
// could never be processed while a recovery was pending. The pool must
// therefore wait for the log to reach the snapshot index BEFORE gating and
// before the state machine is touched.
//
// The first phase publishes no log markers at all, so a worker that consults
// the log before recovering keeps waiting, while a worker that recovers first
// reaches the state machine and signals it.
func TestSnapshotPool_Recovery_WaitsForInstallBeforeGatingStepWorker(t *testing.T) {
	stopC := make(chan struct{})
	defer close(stopC)
	ldb := newSnapshotTestLogDB()
	p := newSnapshotPool(2, ldb, nil, nil, nil, stopC, nil)
	var wg sync.WaitGroup
	p.start(&wg)

	seedRecoverableSnapshot(t, ldb, 1, 1, 100, 7, 100)

	ss := newSignallingSnapshotter(0)
	node, peer := newRecoveryTestNode(t, 1, 1, ss)

	node.raftMu.Lock()
	if err := p.requestSnapshot(snapshotRequest{shardID: 1, replicaID: 1, node: node}); err != nil {
		node.raftMu.Unlock()
		t.Fatalf("requestSnapshot: %v", err)
	}
	select {
	case <-ss.started:
		node.raftMu.Unlock()
		t.Fatal("state machine recovery started before the raft log was consulted")
	case <-time.After(250 * time.Millisecond):
	}
	if node.IsRecovering() {
		node.raftMu.Unlock()
		t.Fatal("recovering gate raised before the raft log installed the snapshot; the step worker can never install it")
	}

	// Install the snapshot the step worker would have delivered, then let the
	// waiting recovery observe it.
	installMsg := proto.Message{
		Type: proto.InstallSnapshot, Origin: proto.OriginLocal,
		From:    2,
		To:      node.ReplicaID(),
		ShardID: node.ShardID(),
		Term:    7,
		Snapshot: proto.Snapshot{
			Index:      100,
			Term:       7,
			Membership: proto.Membership{Addresses: map[uint64]string{node.ReplicaID(): "127.0.0.1:0", 2: "127.0.0.1:1"}},
		},
	}
	if err := peer.Handle(installMsg); err != nil {
		node.raftMu.Unlock()
		t.Fatalf("install snapshot into raft log: %v", err)
	}
	node.publishLogMarkers()
	node.raftMu.Unlock()

	res := awaitSnapshotResult(t, p, "recovery after install")
	if res.err != nil {
		t.Fatalf("recovery failed after install: %v", res.err)
	}
	if got := ss.state.Load(); got != 100 {
		t.Errorf("state machine state = %d, want 100", got)
	}
	if got := node.LastApplied(); got != 100 {
		t.Errorf("node.LastApplied = %d, want 100", got)
	}
}
