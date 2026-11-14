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
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"log/slog"
	"testing"
)

var errRestartBudgetPersist = errors.New("restart budget test: persist refused")

// restartBudgetFirstLife initializes a barrier with cfg, encrypts one
// plaintext on epoch 1, and returns the sealed blob, the strategy and the
// ciphertext, which a second barrier then restores as a process restart.
func restartBudgetFirstLife(ctx context.Context, t *testing.T, cfg BarrierConfig) (*SealedRootKey, *SoftwareStrategy, []byte) {
	t.Helper()
	b := MustNewBarrier(cfg)
	s := testStrategy(t)
	t.Cleanup(func() { _ = s.Close() })
	if err := b.Initialize(ctx, s, Credentials{}); err != nil {
		t.Fatalf("Initialize: %v", err)
	}
	ct, err := b.Encrypt(nil, []byte("written before the restart"))
	if err != nil {
		t.Fatalf("Encrypt before the restart: %v", err)
	}
	sealed := b.SealedRootKeyData()
	if sealed.Epoch != 1 {
		t.Fatalf("pre-restart epoch = %d, want 1", sealed.Epoch)
	}
	return sealed, s, ct
}

// restartBudgetRestore restores sealed into a fresh barrier built from cfg and
// unseals it, failing the test if Unseal does not return nil.
func restartBudgetRestore(ctx context.Context, t *testing.T, cfg BarrierConfig, sealed *SealedRootKey, s *SoftwareStrategy) *Barrier {
	t.Helper()
	b := MustNewBarrier(cfg)
	if err := b.SetSealedRootKey(sealed); err != nil {
		t.Fatalf("SetSealedRootKey: %v", err)
	}
	if err := b.Unseal(ctx, s, Credentials{}); err != nil {
		t.Fatalf("Unseal after restart = %v, want nil", err)
	}
	return b
}

// restartBudgetCauseRecords counts the captured records whose cause attr is
// ErrInvocationBudgetUnknown, and how many of those are at error level.
func restartBudgetCauseRecords(_ context.Context, t *testing.T, h *recordingHandler) (all, atError int) {
	t.Helper()
	want := ErrInvocationBudgetUnknown.Error()
	h.mu.Lock()
	defer h.mu.Unlock()
	for _, r := range h.records {
		found := false
		r.Attrs(func(a slog.Attr) bool {
			if a.Key == "cause" && a.Value.String() == want {
				found = true
				return false
			}
			return true
		})
		if !found {
			continue
		}
		all++
		if r.Level == slog.LevelError {
			atError++
		}
	}
	return all, atError
}

// restartBudgetAssertRefusesWrites asserts the unprovable-budget state: epoch
// 1, the count seeded to the full limit, Encrypt refused, Decrypt unaffected.
func restartBudgetAssertRefusesWrites(_ context.Context, t *testing.T, b *Barrier, ct []byte) {
	t.Helper()
	if got := b.CurrentEpoch(); got != 1 {
		t.Fatalf("CurrentEpoch after restart = %d, want 1 (the rotation did not persist)", got)
	}
	ic := b.InvocationCounter()
	if ic == nil || !ic.IsEnabled() {
		t.Fatal("invocation counter must be enabled by default")
	}
	if got, limit := ic.Count(), ic.Limit(); got != limit {
		t.Fatalf("seeded count = %d, want the full limit %d", got, limit)
	}
	if _, err := b.Encrypt(nil, []byte("must not be written")); !errors.Is(err, ErrInvocationLimitExceeded) {
		t.Fatalf("Encrypt after unprovable-budget restart = %v, want ErrInvocationLimitExceeded", err)
	}
	got, err := b.Decrypt(nil, ct)
	if err != nil {
		t.Fatalf("Decrypt of a pre-restart ciphertext: %v", err)
	}
	if !bytes.Equal(got, []byte("written before the restart")) {
		t.Fatal("decrypted plaintext does not match")
	}
}

// TestBarrier_RestartBudgetRotationPersistFails restores a barrier whose
// StatePersister refuses the rotated blob: the forced restart rotation fails,
// so the barrier stays on epoch 1, refuses Encrypt, keeps Decrypt, and logs
// exactly one error record whose cause is ErrInvocationBudgetUnknown.
func TestBarrier_RestartBudgetRotationPersistFails(t *testing.T) {
	ctx := context.Background()
	sealed, s, ct := restartBudgetFirstLife(ctx, t, testBarrierConfig(1))

	cfg := DefaultBarrierConfig(1)
	rotationAttempts := 0
	cfg.StatePersister = func(blob *SealedRootKey) error {
		if blob != nil && blob.Epoch > 1 {
			rotationAttempts++
			return errRestartBudgetPersist
		}
		return nil
	}
	h := captureLogs(t)
	b := restartBudgetRestore(ctx, t, cfg, sealed, s)

	if rotationAttempts == 0 {
		t.Fatal("Unseal never tried to persist a rotated blob; the restart rotation was not attempted")
	}
	restartBudgetAssertRefusesWrites(ctx, t, b, ct)

	all, atError := restartBudgetCauseRecords(ctx, t, h)
	if all != 1 || atError != 1 {
		t.Fatalf("records with cause %q: %d (%d at error level), want exactly 1 at error level",
			ErrInvocationBudgetUnknown.Error(), all, atError)
	}
}

// TestBarrier_RestartBudgetNoPersisterLogsCause restores a barrier with no
// StatePersister: it cannot rotate, so exactly one error record carries the
// ErrInvocationBudgetUnknown cause.
func TestBarrier_RestartBudgetNoPersisterLogsCause(t *testing.T) {
	ctx := context.Background()
	sealed, s, ct := restartBudgetFirstLife(ctx, t, DefaultBarrierConfig(1))

	h := captureLogs(t)
	b := restartBudgetRestore(ctx, t, DefaultBarrierConfig(1), sealed, s)
	restartBudgetAssertRefusesWrites(ctx, t, b, ct)

	all, atError := restartBudgetCauseRecords(ctx, t, h)
	if all != 1 || atError != 1 {
		t.Fatalf("records with cause %q: %d (%d at error level), want exactly 1 at error level",
			ErrInvocationBudgetUnknown.Error(), all, atError)
	}
}

// TestBarrier_RestartBudgetCountingOffSkipsRotation restarts a barrier whose
// InvocationLimit is -1 (counting off) with no StatePersister: the restart
// neither rotates nor seeds the counter, Encrypt keeps writing epoch 1, and
// no record carries the ErrInvocationBudgetUnknown cause.
func TestBarrier_RestartBudgetCountingOffSkipsRotation(t *testing.T) {
	ctx := context.Background()
	cfg := DefaultBarrierConfig(1)
	cfg.InvocationLimit = -1
	sealed, s, ct := restartBudgetFirstLife(ctx, t, cfg)

	h := captureLogs(t)
	b := restartBudgetRestore(ctx, t, cfg, sealed, s)

	if got := b.CurrentEpoch(); got != 1 {
		t.Fatalf("CurrentEpoch after restart with counting off = %d, want 1", got)
	}
	ct2, err := b.Encrypt(nil, []byte("written after the restart"))
	if err != nil {
		t.Fatalf("Encrypt after restart with counting off: %v", err)
	}
	if epoch := binary.BigEndian.Uint64(ct2[:8]); epoch != 1 {
		t.Fatalf("ciphertext header epoch = %d, want 1", epoch)
	}
	if ic := b.InvocationCounter(); ic != nil {
		if ic.IsEnabled() {
			t.Fatal("invocation counter must be disabled with InvocationLimit -1")
		}
		if got := ic.Count(); got != 0 {
			t.Fatalf("invocation count with counting off = %d, want 0", got)
		}
	}
	if _, err := b.Decrypt(nil, ct); err != nil {
		t.Fatalf("Decrypt of a pre-restart ciphertext: %v", err)
	}

	if all, _ := restartBudgetCauseRecords(ctx, t, h); all != 0 {
		t.Fatalf("records with cause %q: %d, want 0 with counting off",
			ErrInvocationBudgetUnknown.Error(), all)
	}
}
