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
	"slices"
	"testing"
)

// sealEpochPinnerAt sets the inner barrier's epoch, seals one frame through
// the pinner and asserts the seal succeeded under that epoch.
func sealEpochPinnerAt(ctx context.Context, t *testing.T, inner *testBarrier, p *epochPinner, epoch uint64) {
	t.Helper()
	if err := ctx.Err(); err != nil {
		t.Fatalf("context done before seal at epoch %d: %v", epoch, err)
	}
	inner.SetEpoch(epoch)
	ct, got, err := p.sealFrame([]byte("frame"), nil)
	if err != nil {
		t.Fatalf("sealFrame at epoch %d: %v", epoch, err)
	}
	if got != epoch {
		t.Fatalf("sealFrame returned epoch %d, want %d", got, epoch)
	}
	if len(ct) == 0 {
		t.Fatalf("sealFrame at epoch %d returned an empty ciphertext", epoch)
	}
}

// newEpochPinnerFixture builds a pinner over a counting barrier over a
// test barrier at epoch 5, and seals at epochs 5, 5, 6, 5.
func newEpochPinnerFixture(ctx context.Context, t *testing.T) (*testBarrier, *pinCountBarrier, *epochPinner) {
	t.Helper()
	inner := newTestBarrier(5)
	b := newPinCountBarrier(inner)
	p := newEpochPinner(b)
	for _, e := range []uint64{5, 5, 6, 5} {
		sealEpochPinnerAt(ctx, t, inner, p, e)
	}
	return inner, b, p
}

func TestEpochPinner_DistinctEpochsFirstSeenOrderOnePinEach(t *testing.T) {
	_, b, p := newEpochPinnerFixture(context.Background(), t)

	if got, want := p.epochs(), []uint64{5, 6}; !slices.Equal(got, want) {
		t.Fatalf("epochs() = %v, want %v", got, want)
	}
	if b.held(5) != 1 || b.held(6) != 1 {
		t.Fatalf("held(5)=%d held(6)=%d, want 1 and 1 (%s)", b.held(5), b.held(6), b.snapshotCounts())
	}
	if b.regs != 4 || b.deregs != 2 {
		t.Fatalf("regs=%d deregs=%d, want 4 and 2: duplicates must be released at once", b.regs, b.deregs)
	}
}

func TestEpochPinner_TrackEpochZeroIsIgnored(t *testing.T) {
	b := newPinCountBarrier(newTestBarrier(5))
	p := newEpochPinner(b)

	p.track(0)

	if n := len(p.epochs()); n != 0 {
		t.Fatalf("epochs() has %d entries after track(0), want 0", n)
	}
	if b.deregs != 0 {
		t.Fatalf("deregs = %d after track(0), want 0", b.deregs)
	}
	p.releaseAll()
	if b.deregs != 0 {
		t.Fatalf("deregs = %d after releaseAll following track(0), want 0", b.deregs)
	}
}

func TestEpochPinner_FailedSealPinsNothing(t *testing.T) {
	inner := newTestBarrier(5)
	b := newPinCountBarrier(inner)
	p := newEpochPinner(b)
	inner.sealed = true

	ct, epoch, err := p.sealFrame([]byte("x"), nil)
	if err == nil {
		t.Fatal("sealFrame on a sealed barrier returned a nil error")
	}
	if ct != nil {
		t.Fatalf("sealFrame on error returned ciphertext %x, want nil", ct)
	}
	if epoch != 0 {
		t.Fatalf("sealFrame on error returned epoch %d, want 0", epoch)
	}
	if n := len(p.epochs()); n != 0 {
		t.Fatalf("epochs() has %d entries after a failed seal, want 0", n)
	}
	if b.regs != 0 {
		t.Fatalf("regs = %d after a failed seal, want 0", b.regs)
	}
}

func TestEpochPinner_ReleaseAllDropsEveryPinOnceAndIsIdempotent(t *testing.T) {
	_, b, p := newEpochPinnerFixture(context.Background(), t)

	p.releaseAll()

	if b.held(5) != 0 || b.held(6) != 0 {
		t.Fatalf("held(5)=%d held(6)=%d after releaseAll, want 0 and 0 (%s)", b.held(5), b.held(6), b.snapshotCounts())
	}
	b.mu.Lock()
	low5, low6 := b.low[5], b.low[6]
	b.mu.Unlock()
	if low5 < 0 || low6 < 0 {
		t.Fatalf("pin count went negative: low(5)=%d low(6)=%d", low5, low6)
	}
	if n := len(p.epochs()); n != 0 {
		t.Fatalf("epochs() has %d entries after releaseAll, want 0", n)
	}

	deregs := b.deregs
	p.releaseAll()
	if b.deregs != deregs {
		t.Fatalf("second releaseAll released %d more pins, want 0", b.deregs-deregs)
	}
}

func TestEpochPinner_ReusableAfterReleaseAll(t *testing.T) {
	ctx := context.Background()
	inner, b, p := newEpochPinnerFixture(ctx, t)
	p.releaseAll()

	sealEpochPinnerAt(ctx, t, inner, p, 5)

	if got, want := p.epochs(), []uint64{5}; !slices.Equal(got, want) {
		t.Fatalf("epochs() after reuse = %v, want %v", got, want)
	}
	if b.held(5) != 1 {
		t.Fatalf("held(5) after reuse = %d, want 1 (%s)", b.held(5), b.snapshotCounts())
	}
}
