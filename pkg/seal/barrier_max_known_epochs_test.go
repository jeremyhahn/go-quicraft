package seal

import (
	"context"
	"errors"
	"testing"
)

// fillKnownEpochsToBound pads b.knownEpochs with fake epoch numbers until it
// names exactly MaxKnownEpochs entries. Only the set's size matters to the
// rotation guard; no DEK or enclave is created per entry.
func fillKnownEpochsToBound(t *testing.T, b *Barrier) {
	t.Helper()
	b.mu.Lock()
	defer b.mu.Unlock()
	for e := uint64(1 << 32); len(b.knownEpochs) < MaxKnownEpochs; e++ {
		b.knownEpochs[e] = struct{}{}
	}
}

// TestBarrier_RotateRefusedAtMaxKnownEpochs pins that Rotate refuses, before
// deriving or persisting anything, once the known-epoch set has reached
// MaxKnownEpochs: one more epoch would persist a blob SetSealedRootKey
// refuses to load.
func TestBarrier_RotateRefusedAtMaxKnownEpochs(t *testing.T) {
	ctx := context.Background()
	cfg := DefaultBarrierConfig(1)
	rotations := 0
	cfg.StatePersister = func(blob *SealedRootKey) error {
		if blob != nil && blob.Epoch > 1 {
			rotations++
		}
		return nil
	}
	b := MustNewBarrier(cfg)
	s := testStrategy(t)
	t.Cleanup(func() { _ = s.Close() })
	if err := b.Initialize(ctx, s, Credentials{}); err != nil {
		t.Fatalf("Initialize: %v", err)
	}
	fillKnownEpochsToBound(t, b)

	epoch, err := b.Rotate(ctx)
	if !errors.Is(err, ErrKnownEpochsExhausted) {
		t.Fatalf("Rotate at the bound = (%d, %v), want ErrKnownEpochsExhausted", epoch, err)
	}
	if epoch != 0 {
		t.Fatalf("Rotate at the bound returned epoch %d, want 0", epoch)
	}
	if rotations != 0 {
		t.Fatalf("persister called %d times for a refused rotation, want 0", rotations)
	}
	if got := b.CurrentEpoch(); got != 1 {
		t.Fatalf("CurrentEpoch after refused rotation = %d, want 1", got)
	}
}

// TestBarrier_RestartBudgetAtMaxKnownEpochsSeedsInsteadOfRotating pins the
// restart path: with the known-epoch set at the bound, the forced
// restart rotation is refused, nothing is persisted, and the barrier falls
// back to refusing encryption while decryption keeps working.
func TestBarrier_RestartBudgetAtMaxKnownEpochsSeedsInsteadOfRotating(t *testing.T) {
	ctx := context.Background()
	sealed, s, ct := restartBudgetFirstLife(ctx, t, testBarrierConfig(1))

	// Restore with no persister so Unseal does not rotate, then arm the
	// state resolveInvocationBudget sees on a real restart at the bound.
	b := restartBudgetRestore(ctx, t, DefaultBarrierConfig(1), sealed, s)
	persists := 0
	fillKnownEpochsToBound(t, b)

	b.mu.Lock()
	b.config.StatePersister = func(*SealedRootKey) error {
		persists++
		return nil
	}
	b.invocationsAuthoritative = false
	if err := b.invocations.Reset(); err != nil {
		b.mu.Unlock()
		t.Fatalf("invocation counter Reset: %v", err)
	}
	b.resolveInvocationBudget(ctx)
	b.mu.Unlock()

	if persists != 0 {
		t.Fatalf("persister called %d times at the bound, want 0", persists)
	}
	restartBudgetAssertRefusesWrites(ctx, t, b, ct)
}
