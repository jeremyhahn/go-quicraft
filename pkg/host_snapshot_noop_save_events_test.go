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
	"sync/atomic"
	"testing"
	"time"

	"github.com/jeremyhahn/go-quicraft/pkg/config"
)

// TestSnapshotNoOpSave_FiresNoSnapshotEvents: a save that has nothing new to
// snapshot — here, a second request at an index already captured — completes
// without error and writes nothing. It must not be reported as a snapshot
// created, and it is certainly not a recovery: OnSnapshotRecovered tells an
// embedder the state machine was replaced from a snapshot, which never
// happened.
func TestSnapshotNoOpSave_FiresNoSnapshotEvents(t *testing.T) {
	var created, compacted, recovered atomic.Int32
	cfg := testHostConfig(t)
	cfg.EventListener = &config.EventListener{
		OnSnapshotCreated:   func(config.SnapshotInfo) { created.Add(1) },
		OnSnapshotCompacted: func(config.SnapshotInfo) { compacted.Add(1) },
		OnSnapshotRecovered: func(config.SnapshotInfo) { recovered.Add(1) },
	}
	h, err := NewHost(cfg, WithMemoryLogDB(), WithoutTransport(), WithoutEncryption())
	if err != nil {
		t.Fatalf("NewHost: %v", err)
	}
	closed := false
	defer func() {
		if !closed {
			h.Close()
		}
	}()

	if startErr := h.StartShard(map[uint64]string{1: "127.0.0.1:5000"}, false,
		testCreateFunc, testShardConfig(1, 1)); startErr != nil {
		t.Fatalf("StartShard: %v", startErr)
	}
	waitForLeader(t, h, 1)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if _, proposeErr := h.SyncPropose(ctx, 1, []byte("snapshot-me")); proposeErr != nil {
		t.Fatalf("SyncPropose: %v", proposeErr)
	}
	first, err := h.SyncRequestSnapshot(ctx, 1, SnapshotOption{})
	if err != nil || first == 0 {
		t.Fatalf("first SyncRequestSnapshot = (%d, %v), want a snapshot index", first, err)
	}
	second, err := h.SyncRequestSnapshot(ctx, 1, SnapshotOption{})
	if err != nil || second != 0 {
		t.Fatalf("second SyncRequestSnapshot = (%d, %v), want the no-op (0, nil)", second, err)
	}

	// Close stops every notification producer and then drains the listener
	// dispatcher, so every event both saves produced has been delivered.
	h.Close()
	closed = true
	if n := recovered.Load(); n != 0 {
		t.Fatalf("OnSnapshotRecovered fired %d times for two saves and no recovery, want 0", n)
	}
	if n := created.Load(); n != 1 {
		t.Fatalf("OnSnapshotCreated fired %d times, want 1: the no-op save created nothing", n)
	}
	if n := compacted.Load(); n != 1 {
		t.Fatalf("OnSnapshotCompacted fired %d times, want 1", n)
	}
}
