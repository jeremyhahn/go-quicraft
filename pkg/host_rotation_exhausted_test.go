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
	"strings"
	"testing"
	"time"

	"github.com/jeremyhahn/go-quicraft/pkg/logdb/waldb"
	"github.com/jeremyhahn/go-quicraft/pkg/seal"
	"github.com/jeremyhahn/go-quicraft/pkg/sm/kv"
)

// exhaustedHostBarrier models a barrier restarted with a full known-epoch
// set: Rotate fails with seal.ErrKnownEpochsExhausted until a purge frees
// at least one epoch.
type exhaustedHostBarrier struct {
	*testHostBarrier
	full      bool
	purgeFree int
}

func (b *exhaustedHostBarrier) Rotate(ctx context.Context) (uint64, error) {
	b.mu.Lock()
	full := b.full
	b.mu.Unlock()
	if full {
		b.rotateCount.Add(1)
		return 0, seal.ErrKnownEpochsExhausted
	}
	return b.testHostBarrier.Rotate(ctx)
}

func (b *exhaustedHostBarrier) PurgeEpochsBefore(ctx context.Context, minEpoch uint64) (int, error) {
	b.lastPurgeAt.Store(minEpoch)
	b.purgeCount.Add(1)
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.purgeFree > 0 {
		b.full = false
	}
	return b.purgeFree, nil
}

func newExhaustedHostBarrier(purgeFree int) *exhaustedHostBarrier {
	return &exhaustedHostBarrier{testHostBarrier: newTestHostBarrier(9), full: true, purgeFree: purgeFree}
}

func TestRotateBarrierKey_ExhaustedPurgesAndRetries(t *testing.T) {
	rb := newExhaustedHostBarrier(1)
	h, logs := newRotationLogHost(t, rb, true)

	h.rotateBarrierKey(context.Background())

	if got := rb.CurrentEpoch(); got != 10 {
		t.Fatalf("epoch = %d after recovery, want 10; logs:\n%s", got, logs.String())
	}
	if got := rb.rotateCount.Load(); got != 2 {
		t.Fatalf("rotate calls = %d, want 2 (refused, then one retry)", got)
	}
	if rb.purgeCount.Load() < 1 {
		t.Fatal("exhausted rotation did not purge")
	}
	// The purge floor is clamped to the global safe floor (epoch 2 from the
	// retained encrypted snapshot), never past it.
	if got := rb.lastPurgeAt.Load(); got > 2 {
		t.Fatalf("purge floor = %d, exceeds the global safe floor 2", got)
	}
	if strings.Contains(logs.String(), "at-rest key rotation is DISABLED") {
		t.Fatalf("recovered rotation logged the misconfiguration error:\n%s", logs.String())
	}

	// The tick after recovery rotates again: the host is writable under new
	// epochs, not stuck.
	h.rotateBarrierKey(context.Background())
	if got := rb.CurrentEpoch(); got != 11 {
		t.Fatalf("epoch = %d after second tick, want 11", got)
	}
}

func TestRotateBarrierKey_ExhaustedWithoutRetentionStaysRefused(t *testing.T) {
	rb := newExhaustedHostBarrier(1)
	h, _ := newRotationLogHost(t, rb, true)
	h.cfg.MaxRetainedEpochs = 0

	h.rotateBarrierKey(context.Background())

	if got := rb.CurrentEpoch(); got != 9 {
		t.Fatalf("epoch = %d, want 9 (rotation stays refused)", got)
	}
	if got := rb.rotateCount.Load(); got != 1 {
		t.Fatalf("rotate calls = %d, want 1 (no retry)", got)
	}
	if got := rb.purgeCount.Load(); got != 0 {
		t.Fatalf("purge calls = %d, want 0 without retention", got)
	}
}

func TestRotateBarrierKey_ExhaustedNothingPurgeableNoRetry(t *testing.T) {
	rb := newExhaustedHostBarrier(0)
	h, _ := newRotationLogHost(t, rb, true)

	h.rotateBarrierKey(context.Background())

	if got := rb.rotateCount.Load(); got != 1 {
		t.Fatalf("rotate calls = %d, want 1 (nothing freed, no retry)", got)
	}
	if got := rb.CurrentEpoch(); got != 9 {
		t.Fatalf("epoch = %d, want 9", got)
	}
}

func TestRotateBarrierKey_ExhaustedNoSafeFloorNoPurge(t *testing.T) {
	rb := newExhaustedHostBarrier(1)
	h, _ := newRotationLogHost(t, rb, false)

	h.rotateBarrierKey(context.Background())

	if got := rb.purgeCount.Load(); got != 0 {
		t.Fatalf("purge calls = %d, want 0 when the safe floor is unknown", got)
	}
	if got := rb.rotateCount.Load(); got != 1 {
		t.Fatalf("rotate calls = %d, want 1", got)
	}
}

// startExhaustedRestartHost starts the live-rotation shard on a real WAL with
// retention 2 and a rotation interval far beyond the test's lifetime, so no
// rotation tick (scheduled or threshold) can run before the first proposal.
func startExhaustedRestartHost(t *testing.T, walDir, nodeDir string, b *seal.Barrier) *Host {
	t.Helper()
	cfg := testHostConfig(t)
	cfg.WALDir, cfg.NodeHostDir = walDir, nodeDir
	cfg.KeyRotationInterval = time.Hour
	cfg.MaxRetainedEpochs = 2
	db, err := waldb.Open(walDir, waldb.WithBarrier(b), waldb.WithBlockSize(4096),
		waldb.WithMaxFileSize(keyRotationLiveSegmentSize), waldb.WithNoSync(true))
	if err != nil {
		t.Fatalf("waldb.Open: %v", err)
	}
	h, err := NewHost(cfg, WithoutTransport(), WithBarrier(b), WithLogDB(db))
	if err != nil {
		_ = db.Close()
		t.Fatalf("NewHost: %v", err)
	}
	members := map[uint64]string{keyRotationLiveReplicaID: "127.0.0.1:5000"}
	if err := h.StartShard(members, false, kv.NewMemoryCreateFunc(),
		testShardConfig(keyRotationLiveShardID, keyRotationLiveReplicaID)); err != nil {
		mustCloseHost(t, h)
		t.Fatalf("StartShard: %v", err)
	}
	waitForLeader(t, h, keyRotationLiveShardID)
	return h
}

// TestNewHost_RestartAtMaxKnownEpochsRecoversBeforeFirstWrite pins the
// startup recovery for a full keyring. A host restarted with MaxKnownEpochs
// known epochs is refused rotation by Unseal, which seeds the invocation
// counter at its limit, so every WAL write fails ErrInvocationLimitExceeded
// until a purge-and-rotate runs. That recovery must run while the host is
// built, not on the first rotation tick (30 s away by default): a proposal
// made at once must commit, and the shard must keep serving.
func TestNewHost_RestartAtMaxKnownEpochsRecoversBeforeFirstWrite(t *testing.T) {
	ctx := context.Background()
	store := &liveSealedStore{}
	walDir, nodeDir := t.TempDir(), t.TempDir()

	// First life: fill the known-epoch set to the bound, then write WAL
	// records under the last epoch so the WAL floor sits at the top.
	b1 := newLiveRotationBarrier(ctx, t, store, nil)
	for i := 1; i < seal.MaxKnownEpochs; i++ {
		if _, err := b1.Rotate(ctx); err != nil {
			t.Fatalf("fixture rotation %d: %v", i, err)
		}
	}
	top := b1.CurrentEpoch()
	if _, err := b1.Rotate(ctx); !errors.Is(err, seal.ErrKnownEpochsExhausted) {
		t.Fatalf("fixture: rotation at the bound = %v, want ErrKnownEpochsExhausted", err)
	}
	h1 := startExhaustedRestartHost(t, walDir, nodeDir, b1)
	propCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	proposeLiveValue(propCtx, t, h1, "before", "restart")
	cancel()
	mustCloseHost(t, h1)
	if !b1.IsSealed() {
		if err := b1.Seal(); err != nil {
			t.Fatalf("seal first-life barrier: %v", err)
		}
	}

	// Second life: Unseal refuses the rotation at the bound and seeds the
	// invocation limit. Propose at once, long before any rotation tick.
	b2 := newLiveRotationBarrier(ctx, t, store, store.load())
	h2 := startExhaustedRestartHost(t, walDir, nodeDir, b2)
	defer mustCloseHost(t, h2)
	propCtx, cancel = context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	proposeLiveValue(propCtx, t, h2, "after", "restart")

	if got := b2.CurrentEpoch(); got <= top {
		t.Fatalf("epoch = %d after restart, want > %d (recovered by rotation)", got, top)
	}
	if b2.ShouldRotateKey() {
		t.Fatal("barrier still reports its invocation budget exhausted after startup recovery")
	}
}
