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
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jeremyhahn/go-quicraft/pkg/sm"
)

// errInjectedSync is the error syncOrderDiskSM.Sync returns while failSync is set.
var errInjectedSync = errors.New("injected disk sync failure")

// syncEvent is one recorded call on syncOrderDiskSM. For "update" index is
// the entry index; for "sync", "prepare" and "save" it is the state
// machine's applied index at the time of the call.
type syncEvent struct {
	kind  string
	index uint64
}

// syncOrderDiskSM is a DiskStateMachine that records Update, Sync,
// PrepareSnapshot and SaveSnapshot calls in order, and fails Sync on demand.
type syncOrderDiskSM struct {
	mu       sync.Mutex
	events   []syncEvent
	applied  uint64
	failSync atomic.Bool
}

func (d *syncOrderDiskSM) record(kind string, index uint64) {
	d.events = append(d.events, syncEvent{kind: kind, index: index})
}

func (d *syncOrderDiskSM) Open(_ context.Context, _ string, _ <-chan struct{}) (uint64, error) {
	return 0, nil
}

func (d *syncOrderDiskSM) Update(_ context.Context, entries []sm.Entry, results []sm.Result) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	for i := range entries {
		d.applied = entries[i].Index
		d.record("update", entries[i].Index)
		results[i].Value = entries[i].Index
	}
	return nil
}

func (d *syncOrderDiskSM) Lookup(_ context.Context, _ interface{}) (interface{}, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.applied, nil
}

func (d *syncOrderDiskSM) Sync() error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.failSync.Load() {
		d.record("sync-failed", d.applied)
		return errInjectedSync
	}
	d.record("sync", d.applied)
	return nil
}

func (d *syncOrderDiskSM) PrepareSnapshot() (interface{}, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.record("prepare", d.applied)
	return d.applied, nil
}

func (d *syncOrderDiskSM) SaveSnapshot(_ context.Context, snapCtx interface{}, w io.Writer, _ <-chan struct{}) error {
	applied, ok := snapCtx.(uint64)
	if !ok {
		return fmt.Errorf("snapshot context is %T, want uint64", snapCtx)
	}
	d.mu.Lock()
	d.record("save", applied)
	d.mu.Unlock()
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], applied)
	_, err := w.Write(buf[:])
	return err
}

func (d *syncOrderDiskSM) RecoverFromSnapshot(_ context.Context, r io.Reader, _ <-chan struct{}) error {
	var buf [8]byte
	if _, err := io.ReadFull(r, buf[:]); err != nil {
		return err
	}
	d.mu.Lock()
	d.applied = binary.BigEndian.Uint64(buf[:])
	d.mu.Unlock()
	return nil
}

func (d *syncOrderDiskSM) Close(_ context.Context) error { return nil }

var _ sm.DiskStateMachine = (*syncOrderDiskSM)(nil)

// snapshot returns a copy of the recorded events.
func (d *syncOrderDiskSM) snapshot() []syncEvent {
	d.mu.Lock()
	defer d.mu.Unlock()
	return append([]syncEvent(nil), d.events...)
}

// startOnDiskSyncSnapshotHost starts a single-replica shard 1 backed by diskSM.
func startOnDiskSyncSnapshotHost(t *testing.T, diskSM *syncOrderDiskSM) *Host {
	t.Helper()
	h, err := NewHost(testHostConfig(t), WithMemoryLogDB(), WithoutTransport(), WithoutEncryption())
	if err != nil {
		t.Fatalf("NewHost: %v", err)
	}
	create := func(_, _ uint64) interface{} { return diskSM }
	if startErr := h.StartShard(map[uint64]string{1: "127.0.0.1:5000"}, false,
		create, testShardConfig(1, 1)); startErr != nil {
		h.Close()
		t.Fatalf("StartShard: %v", startErr)
	}
	waitForLeader(t, h, 1)
	return h
}

// proposeOnDiskSync proposes n commands to shard 1 and returns the index the
// last one applied at.
func proposeOnDiskSync(ctx context.Context, t *testing.T, h *Host, n int) uint64 {
	t.Helper()
	var last uint64
	for i := range n {
		res, err := h.SyncPropose(ctx, 1, []byte(fmt.Sprintf("cmd-%d", i)))
		if err != nil {
			t.Fatalf("SyncPropose #%d: %v", i, err)
		}
		last = res.Value
	}
	return last
}

// assertSyncBeforeCapture checks that the first successful Sync at or after
// position from follows the last Update at or below snapIdx, and is followed by a
// PrepareSnapshot and SaveSnapshot at snapIdx, in that order. It returns the
// position after the SaveSnapshot.
func assertSyncBeforeCapture(t *testing.T, events []syncEvent, from int, snapIdx uint64) int {
	t.Helper()
	lastUpdate, syncAt, prepareAt, saveAt := -1, -1, -1, -1
	for i := from; i < len(events); i++ {
		e := events[i]
		switch {
		case e.kind == "sync" && syncAt < 0:
			syncAt = i
		case e.kind == "prepare" && syncAt >= 0 && prepareAt < 0:
			prepareAt = i
		case e.kind == "save" && prepareAt >= 0 && saveAt < 0:
			saveAt = i
		}
	}
	// The last Update at or below snapIdx may precede from (the proposals can
	// come before the caller's mark); it must precede the Sync.
	for i := 0; i < syncAt; i++ {
		if e := events[i]; e.kind == "update" && e.index <= snapIdx {
			lastUpdate = i
		}
	}
	if lastUpdate < 0 || syncAt < 0 || prepareAt < 0 || saveAt < 0 {
		t.Fatalf("events %v from %d: want update(<=%d), sync, prepare, save in order; got update=%d sync=%d prepare=%d save=%d",
			events, from, snapIdx, lastUpdate, syncAt, prepareAt, saveAt)
	}
	for i := syncAt + 1; i < len(events); i++ {
		if e := events[i]; e.kind == "update" && e.index <= snapIdx {
			t.Fatalf("update at index %d (<= snapshot %d) recorded after the Sync: events %v", e.index, snapIdx, events)
		}
	}
	if events[lastUpdate].index != snapIdx {
		t.Errorf("last Update before the Sync applied index %d, want the snapshot index %d", events[lastUpdate].index, snapIdx)
	}
	if events[syncAt].index != snapIdx {
		t.Errorf("Sync ran at applied index %d, want the snapshot index %d", events[syncAt].index, snapIdx)
	}
	if events[prepareAt].index != snapIdx || events[saveAt].index != snapIdx {
		t.Errorf("capture at %d / save at %d, want the snapshot index %d",
			events[prepareAt].index, events[saveAt].index, snapIdx)
	}
	return saveAt + 1
}

// TestOnDiskSyncSnapshot_SyncPrecedesEachCapture checks, through
// Host.SyncRequestSnapshot, that a disk state machine's Sync runs after the
// last applied Update and before the capture, for each of two snapshots.
func TestOnDiskSyncSnapshot_SyncPrecedesEachCapture(t *testing.T) {
	diskSM := &syncOrderDiskSM{}
	h := startOnDiskSyncSnapshotHost(t, diskSM)
	defer h.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// (a) the first snapshot.
	last := proposeOnDiskSync(ctx, t, h, 5)
	first, err := h.SyncRequestSnapshot(ctx, 1, SnapshotOption{})
	if err != nil || first == 0 {
		t.Fatalf("first SyncRequestSnapshot = (%d, %v), want an index", first, err)
	}
	if first < last {
		t.Fatalf("first snapshot index %d is below the last proposal's index %d", first, last)
	}
	next := assertSyncBeforeCapture(t, diskSM.snapshot(), 0, first)

	// (b) a second snapshot after more proposals has its own Sync.
	last = proposeOnDiskSync(ctx, t, h, 3)
	second, err := h.SyncRequestSnapshot(ctx, 1, SnapshotOption{})
	if err != nil || second <= first {
		t.Fatalf("second SyncRequestSnapshot = (%d, %v), want an index above %d", second, err, first)
	}
	if second < last {
		t.Fatalf("second snapshot index %d is below the last proposal's index %d", second, last)
	}
	assertSyncBeforeCapture(t, diskSM.snapshot(), next, second)
}

// TestOnDiskSyncSnapshot_SyncErrorFailsOnlyThatSnapshot checks that a Sync
// error fails that one snapshot with an error wrapping the state machine's,
// captures nothing, leaves the previous snapshot the latest, and leaves the
// shard serving proposals and reads; the next snapshot after Sync recovers
// succeeds.
func TestOnDiskSyncSnapshot_SyncErrorFailsOnlyThatSnapshot(t *testing.T) {
	diskSM := &syncOrderDiskSM{}
	h := startOnDiskSyncSnapshotHost(t, diskSM)
	defer h.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	proposeOnDiskSync(ctx, t, h, 4)
	first, err := h.SyncRequestSnapshot(ctx, 1, SnapshotOption{})
	if err != nil || first == 0 {
		t.Fatalf("baseline SyncRequestSnapshot = (%d, %v), want an index", first, err)
	}
	engNode := h.engine.GetNode(1)
	if engNode == nil {
		t.Fatal("engine node not found")
	}
	if got := engNode.LastSnapshotIndex(); got != first {
		t.Fatalf("LastSnapshotIndex = %d after the baseline snapshot, want %d", got, first)
	}

	// (c) Sync fails: the snapshot fails, nothing is captured.
	proposeOnDiskSync(ctx, t, h, 3)
	mark := len(diskSM.snapshot())
	diskSM.failSync.Store(true)
	failedIdx, err := h.SyncRequestSnapshot(ctx, 1, SnapshotOption{})
	if err == nil {
		t.Fatalf("SyncRequestSnapshot with a failing Sync = (%d, nil), want an error", failedIdx)
	}
	if !errors.Is(err, errInjectedSync) {
		t.Fatalf("SyncRequestSnapshot error = %v, want it to wrap %v", err, errInjectedSync)
	}
	sawFailedSync := false
	for _, e := range diskSM.snapshot()[mark:] {
		switch e.kind {
		case "sync-failed":
			sawFailedSync = true
		case "prepare", "save":
			t.Fatalf("%s recorded after a failed Sync: the capture must not run", e.kind)
		}
	}
	if !sawFailedSync {
		t.Fatal("no failed Sync recorded: the snapshot error did not come from Sync")
	}
	if got := engNode.LastSnapshotIndex(); got != first {
		t.Fatalf("LastSnapshotIndex = %d after the failed snapshot, want the previous %d", got, first)
	}

	// The shard still accepts proposals and serves reads.
	last := proposeOnDiskSync(ctx, t, h, 2)
	v, err := h.SyncRead(ctx, 1, nil)
	if err != nil {
		t.Fatalf("SyncRead after the failed snapshot: %v", err)
	}
	if got, ok := v.(uint64); !ok || got < last {
		t.Fatalf("SyncRead after the failed snapshot = %v, want an applied index >= %d", v, last)
	}

	// (d) Sync recovers: the next snapshot succeeds.
	diskSM.failSync.Store(false)
	mark = len(diskSM.snapshot())
	recovered, err := h.SyncRequestSnapshot(ctx, 1, SnapshotOption{})
	if err != nil || recovered <= first {
		t.Fatalf("SyncRequestSnapshot after Sync recovered = (%d, %v), want an index above %d", recovered, err, first)
	}
	assertSyncBeforeCapture(t, diskSM.snapshot(), mark, recovered)
	if got := engNode.LastSnapshotIndex(); got != recovered {
		t.Fatalf("LastSnapshotIndex = %d after the recovered snapshot, want %d", got, recovered)
	}
}
