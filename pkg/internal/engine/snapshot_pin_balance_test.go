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
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"testing"

	"github.com/jeremyhahn/go-quicraft/pkg/config"
)

// pinCountBarrier wraps a SnapshotBarrier and records the RAW net in-flight
// reference count per epoch, INCLUDING negatives.
//
// The production Barrier (and the engine's testBarrier) delete an epoch's entry
// once its count drops to <= 0, which makes a double-free indistinguishable
// from a balanced release. That erasure is exactly what a leak/double-free
// audit must not inherit: an epoch released once too often can drop a count
// another concurrent writer still needs to 1 -> 0, letting PurgeEpochsBefore
// drop a live DEK. This wrapper therefore keeps the running sum and the LOWEST
// value each epoch ever reached, so both directions are provable:
//
//	net[e] > 0 after a full cycle  => LEAK (the DEK can never be purged)
//	low[e] < 0 at any point        => DOUBLE-FREE / under-count
type pinCountBarrier struct {
	SnapshotBarrier
	mu     sync.Mutex
	net    map[uint64]int
	low    map[uint64]int
	regs   int
	deregs int
}

func newPinCountBarrier(inner SnapshotBarrier) *pinCountBarrier {
	return &pinCountBarrier{
		SnapshotBarrier: inner,
		net:             make(map[uint64]int),
		low:             make(map[uint64]int),
	}
}

func (b *pinCountBarrier) RegisterInFlightEpoch(epoch uint64) {
	b.mu.Lock()
	b.net[epoch]++
	b.regs++
	b.mu.Unlock()
	b.SnapshotBarrier.RegisterInFlightEpoch(epoch)
}

func (b *pinCountBarrier) DeregisterInFlightEpoch(epoch uint64) {
	b.mu.Lock()
	b.net[epoch]--
	if b.net[epoch] < b.low[epoch] {
		b.low[epoch] = b.net[epoch]
	}
	b.deregs++
	b.mu.Unlock()
	b.SnapshotBarrier.DeregisterInFlightEpoch(epoch)
}

// held returns the current net in-flight count for an epoch.
func (b *pinCountBarrier) held(epoch uint64) int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.net[epoch]
}

// snapshot returns a stable rendering of the net counts for failure messages.
func (b *pinCountBarrier) snapshotCounts() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	epochs := make([]uint64, 0, len(b.net))
	for e := range b.net {
		epochs = append(epochs, e)
	}
	sort.Slice(epochs, func(i, j int) bool { return epochs[i] < epochs[j] })
	out := fmt.Sprintf("regs=%d deregs=%d", b.regs, b.deregs)
	for _, e := range epochs {
		out += fmt.Sprintf(" [epoch %d net=%d low=%d]", e, b.net[e], b.low[e])
	}
	return out
}

// assertBalanced asserts the net in-flight count for EVERY epoch is exactly
// zero and that no epoch ever went negative.
func (b *pinCountBarrier) assertBalanced(t *testing.T, what string) {
	t.Helper()
	b.mu.Lock()
	defer b.mu.Unlock()
	for e, n := range b.net {
		if n > 0 {
			t.Errorf("%s: epoch %d LEAKED %d in-flight pin(s); its DEK can never be purged (%s)",
				what, e, n, b.snapshotCountsLocked())
		}
		if n < 0 {
			t.Errorf("%s: epoch %d was DEREGISTERED %d time(s) more than registered; an "+
				"under-count lets a purge drop a DEK another holder still needs (%s)",
				what, e, -n, b.snapshotCountsLocked())
		}
	}
	for e, l := range b.low {
		if l < 0 {
			t.Errorf("%s: epoch %d transiently reached net=%d (double-free) (%s)",
				what, e, l, b.snapshotCountsLocked())
		}
	}
}

func (b *pinCountBarrier) snapshotCountsLocked() string {
	epochs := make([]uint64, 0, len(b.net))
	for e := range b.net {
		epochs = append(epochs, e)
	}
	sort.Slice(epochs, func(i, j int) bool { return epochs[i] < epochs[j] })
	out := fmt.Sprintf("regs=%d deregs=%d", b.regs, b.deregs)
	for _, e := range epochs {
		out += fmt.Sprintf(" [epoch %d net=%d low=%d]", e, b.net[e], b.low[e])
	}
	return out
}

var _ SnapshotBarrier = (*pinCountBarrier)(nil)

// writeMultiChunkPlaintext writes a plaintext file spanning several encrypt
// chunks so the frame loop runs more than once.
func writeMultiChunkPlaintext(t *testing.T, path string, chunks int) {
	t.Helper()
	data := make([]byte, snapshotEncryptChunkSize*chunks+321)
	for i := range data {
		data[i] = byte(i*11 + 5)
	}
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatalf("write plaintext: %v", err)
	}
}

// TestPinBalance_EncryptSnapshotFile_SuccessHandsOffExactlyOnePinPerEpoch
// proves the SUCCESS contract of the encrypt loop: every epoch it sealed a
// frame under is held with a net count of exactly ONE when the call returns
// (not zero — the caller must be able to rely on the pin surviving until its
// durable record exists — and not two, which the caller's single release would
// leave leaked forever).
func TestPinBalance_EncryptSnapshotFile_SuccessHandsOffExactlyOnePinPerEpoch(t *testing.T) {
	dir := t.TempDir()
	// Rotate after the first frame so the file spans TWO epochs; this is the
	// mixed-epoch case the commit exists to protect.
	inner := &rotateAfterNBarrier{testBarrier: newTestBarrier(5), rotateAfter: 1}
	barrier := newPinCountBarrier(inner)
	stopC := make(chan struct{})
	p := newSnapshotPool(2, nil, barrier, nil, nil, stopC, nil)

	dataPath := filepath.Join(dir, snapshotDataFile)
	writeMultiChunkPlaintext(t, dataPath, 3)

	minEpoch, pins, err := p.encryptSnapshotFile(dataPath, 2, 1, 42, barrier)
	if err != nil {
		t.Fatalf("encryptSnapshotFile: %v", err)
	}
	if minEpoch != 5 {
		t.Fatalf("min epoch = %d, want 5", minEpoch)
	}
	if len(pins) < 2 {
		t.Fatalf("fixture: %d pins returned, want the multi-epoch path (>=2): %s",
			len(pins), barrier.snapshotCounts())
	}

	// Exactly one pin per returned epoch, and no duplicates in the set.
	seen := map[uint64]bool{}
	for _, e := range pins {
		if seen[e] {
			t.Fatalf("epoch %d returned twice in the pin set; the caller releases once "+
				"per element, so a duplicate is a permanent leak: %v", e, pins)
		}
		seen[e] = true
		if got := barrier.held(e); got != 1 {
			t.Fatalf("epoch %d held %d times after a successful encrypt, want exactly 1: %s",
				e, got, barrier.snapshotCounts())
		}
	}

	// The caller's release must bring every epoch back to zero.
	for _, e := range pins {
		barrier.DeregisterInFlightEpoch(e)
	}
	barrier.assertBalanced(t, "encryptSnapshotFile success + caller release")
}

// TestPinBalance_EncryptSnapshotFile_ErrorReleasesEverythingAndReturnsNoPins
// covers the whole error family of the engine encrypt loop. Every in-loop and
// post-loop failure (encrypt, write_frame, write_data, read, sync, close,
// rename, sync_dir) routes through the single encErrOf helper, so proving the
// helper releases and returns a nil set proves the family: the caller receives
// nothing to release, so there is neither a leak nor a double-free.
func TestPinBalance_EncryptSnapshotFile_ErrorReleasesEverythingAndReturnsNoPins(t *testing.T) {
	dir := t.TempDir()
	// Seal one frame successfully (pinning its epoch), then fail: the loop is
	// holding a pin at the moment the error occurs.
	inner := &failAfterNBarrier{testBarrier: newTestBarrier(7), failAfter: 1}
	barrier := newPinCountBarrier(inner)
	stopC := make(chan struct{})
	p := newSnapshotPool(2, nil, barrier, nil, nil, stopC, nil)

	dataPath := filepath.Join(dir, snapshotDataFile)
	writeMultiChunkPlaintext(t, dataPath, 3)

	_, pins, err := p.encryptSnapshotFile(dataPath, 2, 1, 42, barrier)
	if err == nil {
		t.Fatal("encryptSnapshotFile: expected the injected encrypt failure")
	}
	if pins != nil {
		t.Errorf("error return handed back %v; a non-nil set on error is a DOUBLE-FREE, "+
			"since the loop already released them", pins)
	}
	if barrier.regs == 0 {
		t.Fatalf("fixture: nothing was ever pinned, so the release is untested: %s",
			barrier.snapshotCounts())
	}
	barrier.assertBalanced(t, "encryptSnapshotFile error")
}

// bulkSnapshotter writes enough bytes to span several encrypt chunks, so a
// mid-save rotation genuinely produces frames at more than one epoch.
type bulkSnapshotter struct {
	snapshotTestSnapshotter
	bytes int
}

func (s *bulkSnapshotter) SaveSnapshot(ctx context.Context, w io.Writer, stopper <-chan struct{}) error {
	return s.SaveSnapshotGated(ctx, w, stopper, nil)
}

func (s *bulkSnapshotter) SaveSnapshotGated(_ context.Context, w io.Writer, _ <-chan struct{}, afterCapture func()) error {
	payload := make([]byte, s.bytes)
	for i := range payload {
		payload[i] = byte(i * 13)
	}
	if _, err := w.Write(payload); err != nil {
		return err
	}
	if afterCapture != nil {
		afterCapture()
	}
	return nil
}

// TestPinBalance_SaveSnapshot_FullCycleReturnsToZero drives a complete save —
// encrypt, metadata file, directory fsync chain and the LogDB snapshot record —
// and asserts the barrier's in-flight accounting is back to zero afterwards.
// A leak here permanently blocks DEK purge for the shard's barrier.
func TestPinBalance_SaveSnapshot_FullCycleReturnsToZero(t *testing.T) {
	for _, tc := range []struct {
		name        string
		rotateAfter int
		wantEpochs  int
	}{
		{"single_epoch", 0, 1},
		{"rotation_mid_save", 1, 2},
	} {
		t.Run(tc.name, func(t *testing.T) {
			snapDir := t.TempDir()
			cfg := config.Config{ShardID: 10, ReplicaID: 1, CompactionOverhead: 50}
			cfg.SetDefaults()

			snapshotter := &bulkSnapshotter{bytes: snapshotEncryptChunkSize*3 + 77}
			snapshotter.lastApplied.Store(200)
			snapshotter.state.Store(42)
			node := NewNode(nil, nil, nil, cfg, 100, snapshotter, snapDir, nil)

			inner := &rotateAfterNBarrier{testBarrier: newTestBarrier(3), rotateAfter: tc.rotateAfter}
			barrier := newPinCountBarrier(inner)
			ldb := newSnapshotTestLogDB()
			stopC := make(chan struct{})
			pool := newSnapshotPool(1, ldb, barrier, nil, nil, stopC, nil)

			if _, err := pool.saveSnapshot(snapshotRequest{
				shardID:   10,
				replicaID: 1,
				node:      node,
				save:      true,
			}); err != nil {
				t.Fatalf("saveSnapshot: %v", err)
			}
			if barrier.regs < tc.wantEpochs {
				t.Fatalf("fixture: save pinned %d epoch(s), want at least %d so the "+
					"multi-epoch release is actually exercised: %s",
					barrier.regs, tc.wantEpochs, barrier.snapshotCounts())
			}
			barrier.assertBalanced(t, "saveSnapshot full cycle")
		})
	}
}

// TestPinBalance_SaveSnapshot_EncryptFailureReturnsToZero is the error twin:
// the encrypt fails after a frame was already pinned, saveSnapshot discards the
// save directory and returns, and the accounting must still be flat. Both the
// loop's own release and the caller's deferred release run on this path, so it
// is where a double-free would surface.
func TestPinBalance_SaveSnapshot_EncryptFailureReturnsToZero(t *testing.T) {
	snapDir := t.TempDir()
	cfg := config.Config{ShardID: 11, ReplicaID: 1, CompactionOverhead: 50}
	cfg.SetDefaults()

	snapshotter := &snapshotTestSnapshotter{}
	snapshotter.lastApplied.Store(200)
	snapshotter.state.Store(42)
	node := NewNode(nil, nil, nil, cfg, 100, snapshotter, snapDir, nil)

	inner := &failAfterNBarrier{testBarrier: newTestBarrier(4), failAfter: 0}
	barrier := newPinCountBarrier(inner)
	ldb := newSnapshotTestLogDB()
	stopC := make(chan struct{})
	pool := newSnapshotPool(1, ldb, barrier, nil, nil, stopC, nil)

	_, err := pool.saveSnapshot(snapshotRequest{
		shardID:   11,
		replicaID: 1,
		node:      node,
		save:      true,
	})
	if err == nil {
		t.Fatal("saveSnapshot: expected the injected encrypt failure")
	}
	var snapErr *SnapshotError
	if !errors.As(err, &snapErr) {
		t.Fatalf("error = %T (%v), want *SnapshotError", err, err)
	}
	barrier.assertBalanced(t, "saveSnapshot encrypt failure")
}
