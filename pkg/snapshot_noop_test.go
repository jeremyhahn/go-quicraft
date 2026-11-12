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
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jeremyhahn/go-quicraft/pkg/config"
)

// startSnapshotNoopHost starts a single-replica in-memory shard 1 with the
// given event listener and waits for it to elect itself leader.
func startSnapshotNoopHost(t *testing.T, el *config.EventListener) *Host {
	t.Helper()
	cfg := testHostConfig(t)
	cfg.EventListener = el
	h, err := NewHost(cfg, WithMemoryLogDB(), WithoutTransport(), WithoutEncryption())
	if err != nil {
		t.Fatalf("NewHost: %v", err)
	}
	if startErr := h.StartShard(map[uint64]string{1: "127.0.0.1:5000"}, false,
		testCreateFunc, testShardConfig(1, 1)); startErr != nil {
		h.Close()
		t.Fatalf("StartShard: %v", startErr)
	}
	waitForLeader(t, h, 1)
	return h
}

// requestSnapshotValue issues RequestSnapshot and waits for its result.
func requestSnapshotValue(h *Host) (uint64, error) {
	rs, err := h.RequestSnapshot(context.Background(), 1, SnapshotOption{})
	if err != nil {
		return 0, err
	}
	defer rs.Release()
	res, err := rs.Result()
	return res.Value, err
}

// TestSnapshotNoop_AsyncRepeatResolvesZero pins the documented result of an
// asynchronous RequestSnapshot issued when nothing was applied since the last
// snapshot: the request is accepted and resolves with Value 0 and no error.
// The synchronous form is covered by TestSnapshotNoOpSave_FiresNoSnapshotEvents.
func TestSnapshotNoop_AsyncRepeatResolvesZero(t *testing.T) {
	h := startSnapshotNoopHost(t, nil)
	defer h.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	for _, cmd := range []string{"a", "b", "c"} {
		if _, err := h.SyncPropose(ctx, 1, []byte(cmd)); err != nil {
			t.Fatalf("SyncPropose: %v", err)
		}
	}

	first, err := requestSnapshotValue(h)
	if err != nil || first == 0 {
		t.Fatalf("first RequestSnapshot = (%d, %v), want a snapshot index", first, err)
	}
	for i := range 2 {
		again, againErr := requestSnapshotValue(h)
		if againErr != nil || again != 0 {
			t.Fatalf("repeat #%d RequestSnapshot with nothing new = (%d, %v), want the no-op (0, nil)",
				i, again, againErr)
		}
	}
}

// TestSnapshotNoop_ConcurrentPairYieldsOneSnapshot issues two RequestSnapshot
// calls at once over the same applied state. Each call either is refused with
// ErrSnapshotInProgress or resolves (with the snapshot index, or 0 when the
// other call's snapshot already covers the index); exactly one snapshot is
// created between them.
func TestSnapshotNoop_ConcurrentPairYieldsOneSnapshot(t *testing.T) {
	var created atomic.Int32
	h := startSnapshotNoopHost(t, &config.EventListener{
		OnSnapshotCreated: func(config.SnapshotInfo) { created.Add(1) },
	})
	closed := false
	defer func() {
		if !closed {
			h.Close()
		}
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	for _, cmd := range []string{"a", "b", "c"} {
		if _, err := h.SyncPropose(ctx, 1, []byte(cmd)); err != nil {
			t.Fatalf("SyncPropose: %v", err)
		}
	}

	var (
		wg     sync.WaitGroup
		start  = make(chan struct{})
		values [2]uint64
		errs   [2]error
	)
	for i := range 2 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			values[i], errs[i] = requestSnapshotValue(h)
		}()
	}
	close(start)
	wg.Wait()

	snapshots := 0
	for i := range 2 {
		switch {
		case errors.Is(errs[i], ErrSnapshotInProgress):
		case errs[i] != nil:
			t.Fatalf("concurrent RequestSnapshot #%d: %v, want nil or ErrSnapshotInProgress", i, errs[i])
		case values[i] != 0:
			snapshots++
		}
	}
	if snapshots != 1 {
		t.Fatalf("concurrent pair resolved %d snapshot indexes (values %v, errs %v), want exactly 1",
			snapshots, values, errs)
	}

	// Close drains the listener dispatcher, so every OnSnapshotCreated the
	// pair produced has been delivered.
	h.Close()
	closed = true
	if n := created.Load(); n != 1 {
		t.Fatalf("OnSnapshotCreated fired %d times for the concurrent pair, want 1", n)
	}
}
