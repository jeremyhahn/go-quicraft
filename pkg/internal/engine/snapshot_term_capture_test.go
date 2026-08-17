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
	"io"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jeremyhahn/go-quicraft/pkg/config"
)

// termShiftSnapshotter simulates a leader change that applies entries at a
// higher term while the snapshot save is still streaming AFTER afterCapture
// released the apply gate. The (index, term) recorded by the save must be the
// pair captured under the gate, not the post-release values.
type termShiftSnapshotter struct {
	lastApplied atomic.Uint64
	node        atomic.Pointer[Node]
	shiftIndex  uint64 // node.lastApplied value stored after the gate releases
	shiftTerm   uint64 // node.lastAppliedTerm value stored after the gate releases
}

func (s *termShiftSnapshotter) SaveSnapshot(ctx context.Context, w io.Writer, stopper <-chan struct{}) error {
	return s.SaveSnapshotGated(ctx, w, stopper, nil)
}

func (s *termShiftSnapshotter) SaveSnapshotGated(_ context.Context, w io.Writer, _ <-chan struct{}, afterCapture func()) error {
	var buf [8]byte
	binary.LittleEndian.PutUint64(buf[:], s.lastApplied.Load())
	if _, err := w.Write(buf[:]); err != nil {
		return err
	}
	if afterCapture != nil {
		afterCapture()
	}
	// The gate is released now: simulate applies at a NEWER term advancing
	// the node's applied markers while flush/fsync/encryption still run.
	if n := s.node.Load(); n != nil {
		n.lastApplied.Store(s.shiftIndex)
		n.lastAppliedTerm.Store(s.shiftTerm)
	}
	return nil
}

func (s *termShiftSnapshotter) RecoverFromSnapshot(_ context.Context, r io.Reader, _ <-chan struct{}) error {
	var buf [8]byte
	_, err := io.ReadFull(r, buf[:])
	return err
}

func (s *termShiftSnapshotter) LastApplied() uint64 {
	return s.lastApplied.Load()
}

var _ Snapshotter = (*termShiftSnapshotter)(nil)
var _ gatedSnapshotter = (*termShiftSnapshotter)(nil)

// TestSnapshotPool_SaveSnapshot_TermCapturedUnderGate is the H5 regression
// test: the snapshot's term must be captured together with its index INSIDE
// the apply gate. A term read after afterCapture releases the gate can observe
// applies from a new leader at term T+1, recording (index@T, term=T+1) — a
// Log Matching violation for any follower that installs the snapshot.
func TestSnapshotPool_SaveSnapshot_TermCapturedUnderGate(t *testing.T) {
	tmpDir := t.TempDir()
	stopC := make(chan struct{})
	defer close(stopC)

	ldb := newSnapshotTestLogDB()
	p := newSnapshotPool(2, ldb, nil, nil, nil, stopC, nil)
	var wg sync.WaitGroup
	p.start(&wg)

	ss := &termShiftSnapshotter{shiftIndex: 105, shiftTerm: 7}
	ss.lastApplied.Store(100)
	cfg := config.Config{ShardID: 21, ReplicaID: 1, CompactionOverhead: 50}
	node := NewNode(nil, nil, nil, cfg, ss, tmpDir, nil)
	// Applied frontier at capture time: (index=100, term=3).
	node.SeedLastApplied(100, 3)
	ss.node.Store(node)

	if err := p.requestSnapshot(snapshotRequest{
		shardID:   21,
		replicaID: 1,
		node:      node,
		save:      true,
	}); err != nil {
		t.Fatalf("requestSnapshot failed: %v", err)
	}

	select {
	case result := <-p.CompletedC():
		if result.err != nil {
			t.Fatalf("unexpected save error: %v", result.err)
		}
		if result.index != 100 {
			t.Fatalf("result.index = %d, want 100", result.index)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("did not receive snapshot result within timeout")
	}

	// The LogDB record must carry the term captured under the gate (3), not
	// the post-release term (7).
	snap, err := ldb.GetSnapshot(21, 1)
	if err != nil {
		t.Fatalf("GetSnapshot failed: %v", err)
	}
	if snap.Index != 100 || snap.Term != 3 {
		t.Fatalf("logdb snapshot = (index=%d, term=%d), want (100, 3): "+
			"term was read after the gate released — Log Matching violation", snap.Index, snap.Term)
	}

	// The metadata file must record the same captured term.
	metaPath := filepath.Join(snapshotSaveDir(tmpDir, 21, 1, 100), snapshotMetadataFile)
	data, err := os.ReadFile(metaPath)
	if err != nil {
		t.Fatalf("read metadata: %v", err)
	}
	if len(data) < snapshotMetaSize {
		t.Fatalf("metadata too short: %d bytes", len(data))
	}
	meta := mustParseSnapshotMeta(t, data)
	if meta.Index != 100 {
		t.Fatalf("metadata index = %d, want 100", meta.Index)
	}
	if meta.Term != 3 {
		t.Fatalf("metadata term = %d, want 3: term was read after the gate released", meta.Term)
	}
}

// rsmLikeSnapshotter mirrors rsm.StateMachine's applied-counter behavior: its
// RecoverFromSnapshot restores state but does NOT advance its own applied
// counter — the snapshot pool must seed it via SetLastApplied (H8).
type rsmLikeSnapshotter struct {
	lastApplied atomic.Uint64
	state       atomic.Uint64
}

func (s *rsmLikeSnapshotter) SaveSnapshot(ctx context.Context, w io.Writer, stopper <-chan struct{}) error {
	return s.SaveSnapshotGated(ctx, w, stopper, nil)
}

func (s *rsmLikeSnapshotter) SaveSnapshotGated(_ context.Context, w io.Writer, _ <-chan struct{}, afterCapture func()) error {
	var buf [8]byte
	binary.LittleEndian.PutUint64(buf[:], s.state.Load())
	if _, err := w.Write(buf[:]); err != nil {
		return err
	}
	if afterCapture != nil {
		afterCapture()
	}
	return nil
}

func (s *rsmLikeSnapshotter) RecoverFromSnapshot(_ context.Context, r io.Reader, _ <-chan struct{}) error {
	var buf [8]byte
	if _, err := io.ReadFull(r, buf[:]); err != nil {
		return err
	}
	// State is restored, but the applied counter is deliberately NOT touched,
	// matching rsm.StateMachine (whose counter only advances in Open/Apply).
	s.state.Store(binary.LittleEndian.Uint64(buf[:]))
	return nil
}

func (s *rsmLikeSnapshotter) SetLastApplied(idx uint64) {
	s.lastApplied.Store(idx)
}

func (s *rsmLikeSnapshotter) LastApplied() uint64 {
	return s.lastApplied.Load()
}

var _ Snapshotter = (*rsmLikeSnapshotter)(nil)
var _ appliedSetter = (*rsmLikeSnapshotter)(nil)

// TestSnapshotPool_RecoverSeedsSnapshotterLastApplied is the H8 regression
// test: after recovering to snapshot index N, the snapshotter's own applied
// counter (the INDEX source for the next save) must equal N. Without the
// seeding, a snapshot requested right after recovery records a stale
// pre-recovery index against post-recovery state — a payload/metadata
// mismatch that regresses lastSnapshotIndex and double-applies on replay.
func TestSnapshotPool_RecoverSeedsSnapshotterLastApplied(t *testing.T) {
	tmpDir := t.TempDir()
	stopC := make(chan struct{})
	defer close(stopC)

	ldb := newSnapshotTestLogDB()
	p := newSnapshotPool(2, ldb, nil, nil, nil, stopC, nil)
	var wg sync.WaitGroup
	p.start(&wg)

	cfg := config.Config{ShardID: 33, ReplicaID: 1, CompactionOverhead: 50}

	// Phase 1: produce a durable snapshot at index 100.
	saver := &rsmLikeSnapshotter{}
	saver.lastApplied.Store(100)
	saver.state.Store(100)
	saveNode := NewNode(nil, nil, nil, cfg, saver, tmpDir, nil)
	saveNode.SeedLastApplied(100, 2)
	if err := p.requestSnapshot(snapshotRequest{
		shardID: 33, replicaID: 1, node: saveNode, save: true,
	}); err != nil {
		t.Fatalf("requestSnapshot(save) failed: %v", err)
	}
	select {
	case result := <-p.CompletedC():
		if result.err != nil || result.index != 100 {
			t.Fatalf("save result = (index=%d, err=%v), want (100, nil)", result.index, result.err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("save did not complete within timeout")
	}

	// Phase 2: a fresh snapshotter (stale counter at 40) recovers the
	// snapshot. Its counter must be seeded to the recovered index.
	recoverer := &rsmLikeSnapshotter{}
	recoverer.lastApplied.Store(40)
	recoverer.state.Store(40)
	node := NewNode(nil, nil, nil, cfg, recoverer, tmpDir, nil)
	node.SeedLastApplied(40, 1)
	if err := p.requestSnapshot(snapshotRequest{
		shardID: 33, replicaID: 1, node: node, save: false,
	}); err != nil {
		t.Fatalf("requestSnapshot(recover) failed: %v", err)
	}
	select {
	case result := <-p.CompletedC():
		if result.err != nil {
			t.Fatalf("recover failed: %v", result.err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("recover did not complete within timeout")
	}

	if got := recoverer.LastApplied(); got != 100 {
		t.Fatalf("snapshotter.LastApplied() = %d after recovery, want 100: "+
			"recoverSnapshot did not seed the snapshotter's applied counter", got)
	}
	if got := node.LastApplied(); got != 100 {
		t.Fatalf("node.LastApplied() = %d after recovery, want 100", got)
	}

	// Phase 3: a save requested right after recovery, before anything is
	// applied, is a NO-OP — the recovered snapshot at index 100 is the very
	// artifact a re-save would truncate. It must leave the record untouched.
	if err := p.requestSnapshot(snapshotRequest{
		shardID: 33, replicaID: 1, node: node, save: true,
	}); err != nil {
		t.Fatalf("requestSnapshot(post-recovery save) failed: %v", err)
	}
	select {
	case result := <-p.CompletedC():
		if result.err != nil {
			t.Fatalf("post-recovery save failed: %v", result.err)
		}
		if result.index != 0 {
			t.Fatalf("save at the recovered index = %d, want 0 (no-op): re-saving "+
				"truncates the artifact the recovery just read", result.index)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("post-recovery save did not complete within timeout")
	}

	// Phase 4: once applies advance past the recovered index, the save records
	// the ADVANCED index against recovered state — never the stale
	// pre-recovery counter (40), which is what H8 guards.
	recoverer.lastApplied.Store(150)
	recoverer.state.Store(150)
	if err := p.requestSnapshot(snapshotRequest{
		shardID: 33, replicaID: 1, node: node, save: true,
	}); err != nil {
		t.Fatalf("requestSnapshot(advanced save) failed: %v", err)
	}
	select {
	case result := <-p.CompletedC():
		if result.err != nil {
			t.Fatalf("advanced save failed: %v", result.err)
		}
		if result.index != 150 {
			t.Fatalf("advanced save index = %d, want 150: snapshot "+
				"records a stale index against recovered state", result.index)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("advanced save did not complete within timeout")
	}

	snap, err := ldb.GetSnapshot(33, 1)
	if err != nil {
		t.Fatalf("GetSnapshot failed: %v", err)
	}
	if snap.Index != 150 || snap.Term != 2 {
		t.Fatalf("post-recovery logdb snapshot = (index=%d, term=%d), want (150, 2)", snap.Index, snap.Term)
	}
}
