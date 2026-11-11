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
	"slices"
	"sync/atomic"
	"testing"

	"github.com/jeremyhahn/go-enclave"
)

// cancelDuringUnsealStrategy is a SoftwareStrategy whose UnsealRootKey ends the
// caller's context and then runs the real unseal under it, so the failure the
// barrier sees is exactly the one go-enclave produces for an open whose caller
// stopped waiting: ErrOpenFailed joined with context.Canceled.
type cancelDuringUnsealStrategy struct {
	*SoftwareStrategy
	cancel context.CancelFunc
}

func (s *cancelDuringUnsealStrategy) UnsealRootKey(ctx context.Context, sealed *SealedRootKey, creds Credentials) ([]byte, error) {
	s.cancel()
	return s.SoftwareStrategy.UnsealRootKey(ctx, sealed, creds)
}

// authFailureAfterCancelStrategy reports an authentication verdict alongside a
// cancellation, the shape go-enclave gives a provider that rejected the material
// as the caller's context ended. The verdict must not be laundered by the
// cancellation it arrived with.
type authFailureAfterCancelStrategy struct {
	*SoftwareStrategy
	cancel context.CancelFunc
}

func (s *authFailureAfterCancelStrategy) UnsealRootKey(ctx context.Context, _ *SealedRootKey, _ Credentials) ([]byte, error) {
	s.cancel()
	return nil, &UnsealError{
		Strategy: passphraseStrategyName,
		Err:      errors.Join(enclave.ErrOpenFailed, ctx.Err(), enclave.ErrSealedAuthentication),
	}
}

// sealedBarrierWithPersister initializes and seals a barrier whose persister
// counts its calls, returning both.
func sealedBarrierWithPersister(t *testing.T) (*Barrier, *SoftwareStrategy, *atomic.Int64) {
	t.Helper()
	var persisted atomic.Int64
	cfg := DefaultBarrierConfig(1)
	cfg.StatePersister = func(*SealedRootKey) error {
		persisted.Add(1)
		return nil
	}
	b := MustNewBarrier(cfg)
	s := testStrategy(t)
	t.Cleanup(func() { _ = s.Close() })
	if err := b.Initialize(context.Background(), s, Credentials{}); err != nil {
		t.Fatalf("Initialize: %v", err)
	}
	if err := b.Seal(); err != nil {
		t.Fatalf("Seal: %v", err)
	}
	persisted.Store(0)
	return b, s, &persisted
}

// assertBackoffUntouched fails the test if an Unseal moved the brute-force
// counter, its timestamp, or the copy of either in the sealed state.
func assertBackoffUntouched(t *testing.T, b *Barrier, persisted *atomic.Int64) {
	t.Helper()
	if b.failedAttempts != 0 || !b.lastFailedAt.IsZero() {
		t.Fatalf("failedAttempts=%d lastFailedAt=%v, want an untouched counter",
			b.failedAttempts, b.lastFailedAt)
	}
	if b.sealed.FailedAttempts != 0 || b.sealed.LastFailedUnixNano != 0 {
		t.Fatalf("sealed state carries FailedAttempts=%d LastFailedUnixNano=%d, want nothing persisted",
			b.sealed.FailedAttempts, b.sealed.LastFailedUnixNano)
	}
	if n := persisted.Load(); n != 0 {
		t.Fatalf("StatePersister called %d times, want 0", n)
	}
}

// TestUnseal_CancelledBeforeCallIsNotAFailedAttempt pins that a caller whose
// context has already ended never reaches the strategy and is not charged a
// brute-force attempt: nothing was tried, so nothing failed.
func TestUnseal_CancelledBeforeCallIsNotAFailedAttempt(t *testing.T) {
	b, s, persisted := sealedBarrierWithPersister(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := b.Unseal(ctx, s, Credentials{})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Unseal = %v, want an error matching context.Canceled", err)
	}
	assertBackoffUntouched(t, b, persisted)
	if !b.IsSealed() {
		t.Fatal("a cancelled Unseal left the barrier unsealed")
	}
}

// TestUnseal_CancelledDuringStrategyIsNotAFailedAttempt pins the in-flight
// case: the strategy fails because the caller's context ended while it ran.
// That says nothing about the credential, so it must not drive the backoff a
// later, honest attempt would then be locked out by.
func TestUnseal_CancelledDuringStrategyIsNotAFailedAttempt(t *testing.T) {
	b, s, persisted := sealedBarrierWithPersister(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	err := b.Unseal(ctx, &cancelDuringUnsealStrategy{SoftwareStrategy: s, cancel: cancel}, Credentials{})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Unseal = %v, want an error matching context.Canceled", err)
	}
	assertBackoffUntouched(t, b, persisted)

	// The value is intact: the same strategy with a live context unseals.
	if err := b.Unseal(context.Background(), s, Credentials{}); err != nil {
		t.Fatalf("Unseal after a cancelled attempt: %v", err)
	}
}

// TestUnseal_AuthFailureJoinedWithCancelStillCounts pins the qualification:
// an authentication verdict that arrives joined with the cancellation is still
// a failed attempt. Cancelling the context must not be a way to probe
// credentials for free.
func TestUnseal_AuthFailureJoinedWithCancelStillCounts(t *testing.T) {
	b, s, _ := sealedBarrierWithPersister(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	err := b.Unseal(ctx, &authFailureAfterCancelStrategy{SoftwareStrategy: s, cancel: cancel}, Credentials{})
	if !errors.Is(err, enclave.ErrSealedAuthentication) {
		t.Fatalf("Unseal = %v, want the authentication verdict visible", err)
	}
	if b.failedAttempts != 1 {
		t.Fatalf("failedAttempts = %d, want 1", b.failedAttempts)
	}
	if b.sealed.FailedAttempts != 1 {
		t.Fatalf("sealed FailedAttempts = %d, want 1 persisted", b.sealed.FailedAttempts)
	}
}

// rotatedBarrierWithPersister returns an unsealed barrier at epoch 3 with every
// epoch's DEK still held, and a counter of persister calls made after setup.
func rotatedBarrierWithPersister(t *testing.T) (*Barrier, *atomic.Int64) {
	t.Helper()
	var persisted atomic.Int64
	cfg := DefaultBarrierConfig(1)
	cfg.StatePersister = func(*SealedRootKey) error {
		persisted.Add(1)
		return nil
	}
	b := MustNewBarrier(cfg)
	s := testStrategy(t)
	t.Cleanup(func() { _ = s.Close() })
	if err := b.Initialize(context.Background(), s, Credentials{}); err != nil {
		t.Fatalf("Initialize: %v", err)
	}
	for b.CurrentEpoch() < 3 {
		if _, err := b.Rotate(context.Background()); err != nil {
			t.Fatalf("Rotate: %v", err)
		}
	}
	persisted.Store(0)
	return b, &persisted
}

// barrierSnapshot is the persisted and in-memory state a cancelled call must
// leave exactly as it found it.
type barrierSnapshot struct {
	epoch       uint64
	knownEpochs []uint64
	deks        int
	stateMAC    []byte
}

func snapshotBarrier(b *Barrier) barrierSnapshot {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return barrierSnapshot{
		epoch:       b.epoch,
		knownEpochs: slices.Clone(b.sealed.KnownEpochs),
		deks:        len(b.deks),
		stateMAC:    slices.Clone(b.sealed.StateMAC),
	}
}

func assertBarrierUnchanged(t *testing.T, b *Barrier, before barrierSnapshot, persisted *atomic.Int64) {
	t.Helper()
	after := snapshotBarrier(b)
	if after.epoch != before.epoch || after.deks != before.deks ||
		!slices.Equal(after.knownEpochs, before.knownEpochs) ||
		!slices.Equal(after.stateMAC, before.stateMAC) {
		t.Fatalf("state changed: before %+v, after %+v", before, after)
	}
	if n := persisted.Load(); n != 0 {
		t.Fatalf("StatePersister called %d times, want 0", n)
	}
}

// TestRotate_CancelledContextChangesNothing pins that a rotation whose context
// has ended aborts before anything is written: same epoch, same keyring, same
// persisted state, no persister call.
func TestRotate_CancelledContextChangesNothing(t *testing.T) {
	b, persisted := rotatedBarrierWithPersister(t)
	before := snapshotBarrier(b)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	epoch, err := b.Rotate(ctx)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Rotate = (%d, %v), want an error matching context.Canceled", epoch, err)
	}
	if epoch != 0 {
		t.Fatalf("Rotate returned epoch %d alongside its error, want 0", epoch)
	}
	assertBarrierUnchanged(t, b, before, persisted)
}

// TestPurgeEpochsBefore_CancelledContextChangesNothing pins the same for a
// purge: no DEK dropped, KnownEpochs unchanged, nothing persisted.
func TestPurgeEpochsBefore_CancelledContextChangesNothing(t *testing.T) {
	b, persisted := rotatedBarrierWithPersister(t)
	before := snapshotBarrier(b)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	n, err := b.PurgeEpochsBefore(ctx, 3)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("PurgeEpochsBefore = (%d, %v), want an error matching context.Canceled", n, err)
	}
	if n != 0 {
		t.Fatalf("PurgeEpochsBefore reported %d purged alongside its error, want 0", n)
	}
	assertBarrierUnchanged(t, b, before, persisted)
}

// TestNewCredentials_EndedContextWipesSecret pins NewCredentials' ownership
// contract on the refusal path: go-enclave's NewSealed wipes its input when it
// refuses an ended context, so the caller's plaintext does not outlive a call
// that produced nothing.
func TestNewCredentials_EndedContextWipesSecret(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	secret := []byte("correct horse battery staple")

	creds, err := NewCredentials(ctx, secret)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("NewCredentials = %v, want an error matching context.Canceled", err)
	}
	if !errors.Is(err, enclave.ErrSealFailed) {
		t.Fatalf("NewCredentials = %v, want an error matching enclave.ErrSealFailed", err)
	}
	if creds.Secret != nil {
		t.Fatal("NewCredentials returned a secret alongside its error")
	}
	for i, c := range secret {
		if c != 0 {
			t.Fatalf("secret[%d] = %#x after a refused NewCredentials, want it wiped", i, c)
		}
	}
}

// TestClosedIfEnclaveGone_DistinguishesCloseFromSweep pins that only a
// passphrase enclave that is actually gone reports ErrStrategyClosed. Since
// go-enclave v0.7 an Open can also fail with ErrEnclaveDestroyed because a
// concurrent WipeAll destroyed one of its PER-CALL enclaves while the
// passphrase itself is intact; naming the strategy closed there would send an
// operator after a Close that never happened. Both answers fail the call.
func TestClosedIfEnclaveGone_DistinguishesCloseFromSweep(t *testing.T) {
	passphrase, err := enclave.NewSealed(context.Background(), []byte("passphrase"))
	if err != nil {
		t.Fatalf("NewSealed: %v", err)
	}
	defer passphrase.Destroy()

	swept := errors.Join(enclave.ErrOpenFailed, enclave.ErrEnclaveDestroyed)
	if got := closedIfEnclaveGone(passphrase, swept); errors.Is(got, ErrStrategyClosed) || !errors.Is(got, enclave.ErrEnclaveDestroyed) {
		t.Fatalf("live passphrase: closedIfEnclaveGone = %v, want ErrEnclaveDestroyed kept", got)
	}

	passphrase.Destroy()
	if got := closedIfEnclaveGone(passphrase, enclave.ErrEnclaveDestroyed); !errors.Is(got, ErrStrategyClosed) {
		t.Fatalf("destroyed passphrase: closedIfEnclaveGone = %v, want ErrStrategyClosed", got)
	}

	other := errors.New("unrelated")
	if got := closedIfEnclaveGone(passphrase, other); got != other {
		t.Fatalf("unrelated error: closedIfEnclaveGone = %v, want it unchanged", got)
	}
}
