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
	"strings"
	"testing"

	"github.com/jeremyhahn/go-quicraft/pkg/seal"
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
