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

package seal

import (
	"context"
	"errors"
	"testing"
)

// newInvocationConfigBarrier returns an initialized barrier whose invocation
// counter is built by NewBarrier from limit and threshold (no injected
// counter).
func newInvocationConfigBarrier(ctx context.Context, t *testing.T, limit int64, threshold float64) *Barrier {
	t.Helper()
	cfg := testBarrierConfig(1)
	cfg.InvocationCounter = nil
	cfg.InvocationLimit = limit
	cfg.RotationThreshold = threshold
	b := MustNewBarrier(cfg)
	if err := b.Initialize(ctx, testStrategy(t), Credentials{}); err != nil {
		t.Fatalf("Initialize failed: %v", err)
	}
	return b
}

// encryptN performs n successful encrypts on b.
func encryptN(ctx context.Context, t *testing.T, b *Barrier, n int) {
	t.Helper()
	_ = ctx
	for i := 0; i < n; i++ {
		if _, err := b.Encrypt(nil, []byte("payload")); err != nil {
			t.Fatalf("Encrypt %d failed: %v", i+1, err)
		}
	}
}

func TestBarrierInvocationConfig_RotationThresholdHalf(t *testing.T) {
	ctx := context.Background()
	const limit = 8
	b := newInvocationConfigBarrier(ctx, t, limit, 50)
	for i := 0; i < limit/2; i++ {
		if b.ShouldRotateKey() {
			t.Fatalf("ShouldRotateKey true after %d of %d encrypts at threshold 50", i, limit)
		}
		encryptN(ctx, t, b, 1)
	}
	if !b.ShouldRotateKey() {
		t.Fatalf("ShouldRotateKey false after %d of %d encrypts at threshold 50", limit/2, limit)
	}
	encryptN(ctx, t, b, 1)
	if !b.ShouldRotateKey() {
		t.Fatalf("ShouldRotateKey false after %d of %d encrypts at threshold 50", limit/2+1, limit)
	}
}

func TestBarrierInvocationConfig_DefaultRotationThreshold(t *testing.T) {
	ctx := context.Background()
	const limit = 20
	b := newInvocationConfigBarrier(ctx, t, limit, 0)
	ic, ok := b.InvocationCounter().(*InMemoryInvocationCounter)
	if !ok {
		t.Fatalf("InvocationCounter is %T, want *InMemoryInvocationCounter", b.InvocationCounter())
	}
	if got := ic.GetRotationThreshold(); got != 95 {
		t.Fatalf("rotation threshold = %v, want the documented default 95", got)
	}
	encryptN(ctx, t, b, 18) // 90%
	if b.ShouldRotateKey() {
		t.Fatal("ShouldRotateKey true at 90% with the default threshold")
	}
	encryptN(ctx, t, b, 1) // 95%
	if !b.ShouldRotateKey() {
		t.Fatal("ShouldRotateKey false at 95% with the default threshold")
	}
}

func TestBarrierInvocationConfig_LimitRefusesEncrypt(t *testing.T) {
	ctx := context.Background()
	const limit = 8
	b := newInvocationConfigBarrier(ctx, t, limit, 50)
	encryptN(ctx, t, b, limit)
	ct, err := b.Encrypt(nil, []byte("payload"))
	if !errors.Is(err, ErrInvocationLimitExceeded) {
		t.Fatalf("Encrypt past the limit: err = %v, want ErrInvocationLimitExceeded", err)
	}
	if ct != nil {
		t.Fatalf("Encrypt past the limit returned %d bytes of ciphertext, want none", len(ct))
	}
	// The refusal does not consume budget: it still refuses.
	if _, err := b.Encrypt(nil, []byte("payload")); !errors.Is(err, ErrInvocationLimitExceeded) {
		t.Fatalf("second Encrypt past the limit: err = %v, want ErrInvocationLimitExceeded", err)
	}
}

func TestBarrierInvocationConfig_RotateResetsCount(t *testing.T) {
	ctx := context.Background()
	const limit = 8
	b := newInvocationConfigBarrier(ctx, t, limit, 50)
	encryptN(ctx, t, b, limit)
	if !b.ShouldRotateKey() {
		t.Fatal("ShouldRotateKey false at the limit")
	}
	if _, err := b.Rotate(ctx); err != nil {
		t.Fatalf("Rotate failed: %v", err)
	}
	if b.ShouldRotateKey() {
		t.Fatal("ShouldRotateKey true right after Rotate")
	}
	if got := b.InvocationCounter().Count(); got != 0 {
		t.Fatalf("invocation count after Rotate = %d, want 0", got)
	}
	ct, err := b.Encrypt(nil, []byte("payload"))
	if err != nil {
		t.Fatalf("Encrypt after Rotate failed: %v", err)
	}
	if len(ct) == 0 {
		t.Fatal("Encrypt after Rotate returned no ciphertext")
	}
	// The new epoch has the full budget again.
	encryptN(ctx, t, b, limit-1)
	if _, err := b.Encrypt(nil, []byte("payload")); !errors.Is(err, ErrInvocationLimitExceeded) {
		t.Fatalf("Encrypt past the new epoch's limit: err = %v, want ErrInvocationLimitExceeded", err)
	}
}

func TestBarrierInvocationConfig_LimitDisabled(t *testing.T) {
	ctx := context.Background()
	b := newInvocationConfigBarrier(ctx, t, -1, 50)
	encryptN(ctx, t, b, 1000)
	if b.ShouldRotateKey() {
		t.Fatal("ShouldRotateKey true with InvocationLimit -1")
	}
}
