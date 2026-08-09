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
	"errors"
	"slices"
	"sync"
	"testing"
)

// TestEncryptWithEpochPinned_SurvivesConcurrentRotatePurge is the TOCTOU
// regression: EncryptWithEpoch releases b.mu before returning, so a Rotate +
// PurgeEpochsBefore (both taking b.mu exclusively) can complete in the gap
// before the caller pins the returned epoch — dropping the DEK of a frame that
// is already sealed and making the artifact permanently undecryptable.
// EncryptWithEpochPinned performs the seal and the pin under one lock, so the
// ciphertext it returns must ALWAYS remain decryptable no matter how many
// rotations and purges run immediately afterward.
func TestEncryptWithEpochPinned_SurvivesConcurrentRotatePurge(t *testing.T) {
	b, _ := testInitializedBarrier(t) // epoch 1
	plaintext := []byte("frame that must stay decryptable")

	ct, epoch, err := b.EncryptWithEpochPinned(nil, plaintext)
	if err != nil {
		t.Fatalf("EncryptWithEpochPinned: %v", err)
	}
	defer b.DeregisterInFlightEpoch(epoch)

	// The exact interleaving that used to orphan the frame: rotate twice past
	// the sealed epoch, then purge everything below the new current epoch.
	if _, err := b.Rotate(); err != nil {
		t.Fatalf("Rotate: %v", err)
	}
	if _, err := b.Rotate(); err != nil {
		t.Fatalf("Rotate: %v", err)
	}
	if _, err := b.PurgeEpochsBefore(b.CurrentEpoch()); err != nil {
		t.Fatalf("PurgeEpochsBefore: %v", err)
	}

	pt, err := b.DecryptForEpoch(nil, ct, epoch)
	if err != nil {
		t.Fatalf("frame sealed under epoch %d is UNDECRYPTABLE after a concurrent "+
			"rotate+purge (%v): the atomic seal-and-pin did not protect its DEK", epoch, err)
	}
	if !bytes.Equal(pt, plaintext) {
		t.Fatal("decrypted frame does not match the original plaintext")
	}
}

// TestEncryptWithEpochPinned_BalancesRefcount asserts the pin it takes is a
// single, releasable reference: after the caller deregisters, the epoch is no
// longer protected and a purge may reclaim it.
func TestEncryptWithEpochPinned_BalancesRefcount(t *testing.T) {
	b, _ := testInitializedBarrier(t)
	_, epoch, err := b.EncryptWithEpochPinned(nil, []byte("x"))
	if err != nil {
		t.Fatalf("EncryptWithEpochPinned: %v", err)
	}

	b.inFlightMu.Lock()
	held := b.inFlightEpochs[epoch]
	b.inFlightMu.Unlock()
	if held != 1 {
		t.Fatalf("in-flight refcount for epoch %d = %d, want exactly 1", epoch, held)
	}

	b.DeregisterInFlightEpoch(epoch)
	b.inFlightMu.Lock()
	_, stillHeld := b.inFlightEpochs[epoch]
	b.inFlightMu.Unlock()
	if stillHeld {
		t.Fatalf("epoch %d still pinned after Deregister: the pin leaks and would "+
			"block every future DEK purge for this barrier", epoch)
	}
}

// TestPurgeEpochsBefore_SyncsSealedKnownEpochs is the regression test for
// PurgeEpochsBefore not rebuilding the persisted sealed state: without the
// sync, SealedRootKeyData() kept returning the purged epochs and a restart
// resurrected them into knownEpochs, so Unseal re-derived DEKs the purge
// floor had retired.
func TestPurgeEpochsBefore_SyncsSealedKnownEpochs(t *testing.T) {
	b, _ := testInitializedBarrier(t)

	if _, err := b.Rotate(); err != nil { // epoch 2
		t.Fatalf("Rotate: %v", err)
	}
	if _, err := b.Rotate(); err != nil { // epoch 3
		t.Fatalf("Rotate: %v", err)
	}

	purged, err := b.PurgeEpochsBefore(3)
	if err != nil {
		t.Fatalf("PurgeEpochsBefore: %v", err)
	}
	if purged != 2 {
		t.Fatalf("purged = %d, want 2", purged)
	}

	sealed := b.SealedRootKeyData()
	if sealed == nil {
		t.Fatal("SealedRootKeyData returned nil")
	}
	got := make(map[uint64]bool, len(sealed.KnownEpochs))
	for _, ep := range sealed.KnownEpochs {
		got[ep] = true
	}
	if got[1] || got[2] {
		t.Fatalf("SealedRootKeyData still returns purged epochs: %v", sealed.KnownEpochs)
	}
	if !got[3] {
		t.Fatalf("SealedRootKeyData lost the current epoch: %v", sealed.KnownEpochs)
	}

	// Restart simulation: a fresh barrier restored from the persisted
	// sealed state must not resurrect the purged epochs after Unseal.
	strategy2 := testStrategy(t)
	b2 := NewBarrier(DefaultBarrierConfig(1))
	if err := b2.SetSealedRootKey(sealed); err != nil {
		t.Fatalf("SetSealedRootKey: %v", err)
	}
	if err := b2.Unseal(context.Background(), strategy2, Credentials{}); err != nil {
		t.Fatalf("Unseal: %v", err)
	}
	b2.mu.RLock()
	_, has1 := b2.knownEpochs[1]
	_, has2 := b2.knownEpochs[2]
	_, has3 := b2.knownEpochs[3]
	b2.mu.RUnlock()
	if has1 || has2 {
		t.Fatal("restart resurrected purged epochs into knownEpochs")
	}
	if !has3 {
		t.Fatal("restart lost the current epoch")
	}
}

// TestUnseal_MultiEpochRederivation guards the Unseal loop that skips the
// current epoch (avoiding an unwiped duplicate current-epoch DEK enclave):
// after seal/unseal, data encrypted under every historical epoch AND the
// current epoch must still decrypt.
func TestUnseal_MultiEpochRederivation(t *testing.T) {
	b, s := testInitializedBarrier(t)

	ct1, epoch1, err := b.EncryptWithEpoch(nil, []byte("epoch-one-data"))
	if err != nil {
		t.Fatalf("Encrypt epoch 1: %v", err)
	}
	if _, err := b.Rotate(); err != nil {
		t.Fatalf("Rotate: %v", err)
	}
	ct2, epoch2, err := b.EncryptWithEpoch(nil, []byte("epoch-two-data"))
	if err != nil {
		t.Fatalf("Encrypt epoch 2: %v", err)
	}

	if err := b.Seal(); err != nil {
		t.Fatalf("Seal: %v", err)
	}
	if err := b.Unseal(context.Background(), s, Credentials{}); err != nil {
		t.Fatalf("Unseal: %v", err)
	}

	pt1, err := b.DecryptForEpoch(nil, ct1, epoch1)
	if err != nil {
		t.Fatalf("DecryptForEpoch(%d): %v", epoch1, err)
	}
	if string(pt1) != "epoch-one-data" {
		t.Fatalf("epoch %d decrypt = %q", epoch1, pt1)
	}
	pt2, err := b.DecryptForEpoch(nil, ct2, epoch2)
	if err != nil {
		t.Fatalf("DecryptForEpoch(%d): %v", epoch2, err)
	}
	if string(pt2) != "epoch-two-data" {
		t.Fatalf("epoch %d decrypt = %q", epoch2, pt2)
	}

	// Exactly one DEK enclave per known epoch after unseal.
	b.mu.RLock()
	deks, known := len(b.deks), len(b.knownEpochs)
	b.mu.RUnlock()
	if deks != known {
		t.Fatalf("deks=%d knownEpochs=%d, want equal", deks, known)
	}
}

// TestSoftwareStrategy_UseAfterCloseReturnsTypedError is the regression
// test for the nil-passphrase panic: Close() nils the passphrase enclave,
// and a subsequent seal/unseal must fail with ErrStrategyClosed instead of
// panicking on a nil *ProtectedKey dereference.
func TestSoftwareStrategy_UseAfterCloseReturnsTypedError(t *testing.T) {
	s := testStrategy(t)

	rootKey := make([]byte, 32)
	sealed, err := s.SealRootKey(context.Background(), rootKey, Credentials{})
	if err != nil {
		t.Fatalf("SealRootKey: %v", err)
	}

	if err := s.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if err := s.Close(); err != nil { // documented idempotent
		t.Fatalf("second Close: %v", err)
	}

	if _, err := s.SealRootKey(context.Background(), rootKey, Credentials{}); !errors.Is(err, ErrStrategyClosed) {
		t.Fatalf("SealRootKey after Close: got %v, want ErrStrategyClosed", err)
	}
	if _, err := s.UnsealRootKey(context.Background(), sealed, Credentials{}); !errors.Is(err, ErrStrategyClosed) {
		t.Fatalf("UnsealRootKey after Close: got %v, want ErrStrategyClosed", err)
	}
}

// --- Rotation durability: persist-then-activate ---

// recordingPersister is a StatePersister that records what it was asked to
// persist and can be made to fail.
type recordingPersister struct {
	mu    sync.Mutex
	last  *SealedRootKey
	calls int
	err   error
}

func (p *recordingPersister) persist(sealed *SealedRootKey) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.calls++
	if p.err != nil {
		return p.err
	}
	p.last = sealed
	return nil
}

func (p *recordingPersister) snapshot() (*SealedRootKey, int) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.last, p.calls
}

// testPersistingBarrier returns an initialized barrier whose sealed state is
// captured by the returned persister, seeded with the Initialize output.
func testPersistingBarrier(t *testing.T) (*Barrier, *SoftwareStrategy, *recordingPersister) {
	t.Helper()
	p := &recordingPersister{}
	cfg := DefaultBarrierConfig(1)
	cfg.StatePersister = p.persist
	b := NewBarrier(cfg)
	s := testStrategy(t)
	if err := b.Initialize(context.Background(), s, Credentials{}); err != nil {
		t.Fatalf("Initialize: %v", err)
	}
	// Initialize does not call the persister; the embedder persists the
	// bootstrap blob itself.
	p.mu.Lock()
	p.last = b.SealedRootKeyData()
	p.mu.Unlock()
	return b, s, p
}

// TestRotate_PersistsNewEpochBeforeActivating is the regression test for the
// unpersisted-rotation data-loss bug: Rotate committed a new epoch in memory
// only, the write path immediately stamped it into WAL/snapshot headers, and
// after a restart the reloaded blob still named the OLD epoch — so every
// segment written since the rotation failed to decrypt.
func TestRotate_PersistsNewEpochBeforeActivating(t *testing.T) {
	b, s, p := testPersistingBarrier(t)
	defer s.Close()

	newEpoch, err := b.Rotate()
	if err != nil {
		t.Fatalf("Rotate: %v", err)
	}
	if newEpoch != 2 {
		t.Fatalf("Rotate = %d, want 2", newEpoch)
	}
	if b.CurrentEpoch() != newEpoch {
		t.Fatalf("CurrentEpoch = %d, want %d", b.CurrentEpoch(), newEpoch)
	}

	persisted, calls := p.snapshot()
	if calls != 1 {
		t.Fatalf("persister calls = %d, want 1", calls)
	}
	if persisted.Epoch != newEpoch {
		t.Fatalf("persisted Epoch = %d, want %d", persisted.Epoch, newEpoch)
	}
	if !slices.Equal(persisted.KnownEpochs, []uint64{1, 2}) {
		t.Fatalf("persisted KnownEpochs = %v, want [1 2] (ascending)", persisted.KnownEpochs)
	}
}

// TestRotate_PersisterFailureAbortsRotation verifies the abort path: a failed
// persist must leave the OLD epoch fully active with no partial state.
func TestRotate_PersisterFailureAbortsRotation(t *testing.T) {
	b, s, p := testPersistingBarrier(t)
	defer s.Close()

	sentinel := errors.New("disk full")
	p.mu.Lock()
	p.err = sentinel
	p.mu.Unlock()

	before := b.CurrentEpoch()
	countBefore := b.invocations.Count()

	newEpoch, err := b.Rotate()
	if !errors.Is(err, sentinel) {
		t.Fatalf("Rotate error = %v, want wrapped %v", err, sentinel)
	}
	var perr *StatePersistError
	if !errors.As(err, &perr) {
		t.Fatalf("Rotate error = %T, want *StatePersistError", err)
	}
	if perr.Op != "rotate" || perr.Epoch != before+1 {
		t.Fatalf("StatePersistError = %+v, want op=rotate epoch=%d", perr, before+1)
	}
	if newEpoch != 0 {
		t.Fatalf("Rotate returned epoch %d on failure, want 0", newEpoch)
	}

	// Old epoch still active and usable.
	if b.CurrentEpoch() != before {
		t.Fatalf("CurrentEpoch = %d after failed rotate, want %d", b.CurrentEpoch(), before)
	}
	ct, err := b.Encrypt(nil, []byte("still-works"))
	if err != nil {
		t.Fatalf("Encrypt after failed rotate: %v", err)
	}
	pt, err := b.Decrypt(nil, ct)
	if err != nil || string(pt) != "still-works" {
		t.Fatalf("Decrypt after failed rotate: %q, %v", pt, err)
	}

	// No partial state: the aborted epoch must not appear anywhere.
	b.mu.RLock()
	_, leakedKnown := b.knownEpochs[before+1]
	_, leakedDEK := b.deks[before+1]
	_, leakedCipher := b.ciphers[before+1]
	sealedEpoch := b.sealed.Epoch
	sealedKnown := slices.Clone(b.sealed.KnownEpochs)
	b.mu.RUnlock()
	if leakedKnown || leakedDEK || leakedCipher {
		t.Fatal("aborted rotation leaked the new epoch into barrier state")
	}
	if sealedEpoch != before {
		t.Fatalf("sealed.Epoch = %d after failed rotate, want %d", sealedEpoch, before)
	}
	if slices.Contains(sealedKnown, before+1) {
		t.Fatalf("aborted rotation leaked the new epoch into sealed.KnownEpochs: %v", sealedKnown)
	}
	// The invocation counter must not be reset by an aborted rotation.
	if b.invocations.Count() < countBefore {
		t.Fatalf("invocation count fell from %d to %d across an aborted rotation",
			countBefore, b.invocations.Count())
	}
}

// TestRotate_WithoutPersisterFailsClosed verifies that an unconfigured
// deployment gets a loud recurring error instead of silent data loss.
func TestRotate_WithoutPersisterFailsClosed(t *testing.T) {
	b := NewBarrier(DefaultBarrierConfig(1)) // no StatePersister
	s := testStrategy(t)
	defer s.Close()
	if err := b.Initialize(context.Background(), s, Credentials{}); err != nil {
		t.Fatalf("Initialize: %v", err)
	}

	if _, err := b.Rotate(); !errors.Is(err, ErrNoStatePersister) {
		t.Fatalf("Rotate = %v, want ErrNoStatePersister", err)
	}
	if b.CurrentEpoch() != 1 {
		t.Fatalf("CurrentEpoch = %d, want 1 (unchanged)", b.CurrentEpoch())
	}
	b.mu.RLock()
	nKnown, nDEKs := len(b.knownEpochs), len(b.deks)
	b.mu.RUnlock()
	if nKnown != 1 || nDEKs != 1 {
		t.Fatalf("state mutated: knownEpochs=%d deks=%d, want 1/1", nKnown, nDEKs)
	}
	// Encryption continues under the current epoch.
	if _, err := b.Encrypt(nil, []byte("x")); err != nil {
		t.Fatalf("Encrypt after refused rotation: %v", err)
	}
}

// TestRotate_DataSurvivesRestartFromPersistedBlob is the end-to-end proof: data
// encrypted AFTER a rotation must decrypt on a fresh barrier restored solely
// from the persisted blob.
func TestRotate_DataSurvivesRestartFromPersistedBlob(t *testing.T) {
	b, s, p := testPersistingBarrier(t)
	defer s.Close()

	if _, err := b.Rotate(); err != nil {
		t.Fatalf("Rotate: %v", err)
	}
	ct, epoch, err := b.EncryptWithEpoch(nil, []byte("post-rotation-payload"))
	if err != nil {
		t.Fatalf("EncryptWithEpoch: %v", err)
	}
	if epoch != 2 {
		t.Fatalf("EncryptWithEpoch epoch = %d, want 2", epoch)
	}

	// Restart: only the persisted blob crosses the process boundary.
	persisted, _ := p.snapshot()
	b2 := NewBarrier(DefaultBarrierConfig(1))
	if err := b2.SetSealedRootKey(persisted); err != nil {
		t.Fatalf("SetSealedRootKey: %v", err)
	}
	s2 := testStrategy(t)
	defer s2.Close()
	if err := b2.Unseal(context.Background(), s2, Credentials{}); err != nil {
		t.Fatalf("Unseal: %v", err)
	}

	pt, err := b2.DecryptForEpoch(nil, ct, epoch)
	if err != nil {
		t.Fatalf("DecryptForEpoch(%d) after restart: %v", epoch, err)
	}
	if string(pt) != "post-rotation-payload" {
		t.Fatalf("decrypted %q after restart", pt)
	}
}

// --- Purge durability ---

// TestPurgeEpochsBefore_WithoutPersisterFailsClosed verifies that a purge that
// would shrink the persisted KnownEpochs refuses to run unpersisted, while a
// purge with nothing to drop needs no persister.
func TestPurgeEpochsBefore_WithoutPersisterFailsClosed(t *testing.T) {
	b, s, p := testPersistingBarrier(t)
	defer s.Close()

	if _, err := b.Rotate(); err != nil {
		t.Fatalf("Rotate: %v", err)
	}

	// Drop the persister, emulating an embedder that never configured one.
	b.mu.Lock()
	b.config.StatePersister = nil
	b.mu.Unlock()

	if _, err := b.PurgeEpochsBefore(2); !errors.Is(err, ErrNoStatePersister) {
		t.Fatalf("PurgeEpochsBefore = %v, want ErrNoStatePersister", err)
	}
	b.mu.RLock()
	_, stillThere := b.deks[1]
	b.mu.RUnlock()
	if !stillThere {
		t.Fatal("refused purge dropped the DEK anyway")
	}

	// A no-op purge is allowed without a persister.
	purged, err := b.PurgeEpochsBefore(1)
	if err != nil || purged != 0 {
		t.Fatalf("no-op PurgeEpochsBefore = (%d, %v), want (0, nil)", purged, err)
	}

	// Restore the persister: the purge now succeeds and persists the
	// reduced, sorted epoch set.
	b.mu.Lock()
	b.config.StatePersister = p.persist
	b.mu.Unlock()
	purged, err = b.PurgeEpochsBefore(2)
	if err != nil || purged != 1 {
		t.Fatalf("PurgeEpochsBefore = (%d, %v), want (1, nil)", purged, err)
	}
	persisted, _ := p.snapshot()
	if !slices.Equal(persisted.KnownEpochs, []uint64{2}) {
		t.Fatalf("persisted KnownEpochs = %v, want [2]", persisted.KnownEpochs)
	}
}

// TestPurgeEpochsBefore_PersisterFailureRetainsDEKs verifies that a failed
// persist aborts the purge with every DEK still usable.
func TestPurgeEpochsBefore_PersisterFailureRetainsDEKs(t *testing.T) {
	b, s, p := testPersistingBarrier(t)
	defer s.Close()

	ct1, epoch1, err := b.EncryptWithEpoch(nil, []byte("epoch-one"))
	if err != nil {
		t.Fatalf("EncryptWithEpoch: %v", err)
	}
	if _, err := b.Rotate(); err != nil {
		t.Fatalf("Rotate: %v", err)
	}

	sentinel := errors.New("persist rejected")
	p.mu.Lock()
	p.err = sentinel
	p.mu.Unlock()

	purged, err := b.PurgeEpochsBefore(2)
	if !errors.Is(err, sentinel) {
		t.Fatalf("PurgeEpochsBefore error = %v, want wrapped %v", err, sentinel)
	}
	if purged != 0 {
		t.Fatalf("purged = %d on failure, want 0", purged)
	}
	pt, err := b.DecryptForEpoch(nil, ct1, epoch1)
	if err != nil || string(pt) != "epoch-one" {
		t.Fatalf("epoch %d unreadable after aborted purge: %q, %v", epoch1, pt, err)
	}
}

// --- Strategy downgrade ---

// TestUnseal_StrategyFieldDowngradeRejected is the regression test for the
// unauthenticated SealedRootKey.Strategy field: flipping it to "none" used to
// steer Unseal into the no-encryption branch, producing a "successfully
// unsealed" barrier with no cipher whose first Encrypt panicked.
func TestUnseal_StrategyFieldDowngradeRejected(t *testing.T) {
	b, s, _ := testPersistingBarrier(t)
	defer s.Close()
	sealed := b.SealedRootKeyData()

	// Attacker flips the plaintext strategy field.
	sealed.Strategy = StrategyNone

	b2 := NewBarrier(DefaultBarrierConfig(1))
	if err := b2.SetSealedRootKey(sealed); err != nil {
		t.Fatalf("SetSealedRootKey: %v", err)
	}
	s2 := testStrategy(t)
	defer s2.Close()

	err := b2.Unseal(context.Background(), s2, Credentials{})
	if !errors.Is(err, ErrStrategyMismatch) {
		t.Fatalf("Unseal with downgraded strategy field = %v, want ErrStrategyMismatch", err)
	}
	if !b2.IsSealed() {
		t.Fatal("barrier reported unsealed after a rejected downgrade")
	}
	// Must fail closed, never panic on a nil cipher.
	if _, err := b2.Encrypt(nil, []byte("x")); !errors.Is(err, ErrBarrierSealed) {
		t.Fatalf("Encrypt after rejected downgrade = %v, want ErrBarrierSealed", err)
	}
}

// noEncryptorNoneStrategy claims StrategyNone but supplies no encryptor.
type noEncryptorNoneStrategy struct{ *NoneStrategy }

func (noEncryptorNoneStrategy) BarrierEncryptor(_ context.Context, _ []byte) (SymmetricEncrypter, error) {
	return nil, nil
}

// TestUnseal_NoneStrategyWithoutEncryptorFailsClosed verifies the second half
// of the downgrade fix: the no-encryption branch must not mark the barrier
// unsealed when the strategy hands back a nil encryptor.
func TestUnseal_NoneStrategyWithoutEncryptorFailsClosed(t *testing.T) {
	sealed, err := (&NoneStrategy{}).SealRootKey(context.Background(), nil, Credentials{})
	if err != nil {
		t.Fatalf("SealRootKey: %v", err)
	}

	b := NewBarrier(DefaultBarrierConfig(1))
	if err := b.SetSealedRootKey(sealed); err != nil {
		t.Fatalf("SetSealedRootKey: %v", err)
	}
	if err := b.Unseal(context.Background(), noEncryptorNoneStrategy{NewNoneStrategy()}, Credentials{}); !errors.Is(err, ErrNoBarrierEncryptor) {
		t.Fatalf("Unseal = %v, want ErrNoBarrierEncryptor", err)
	}
	if !b.IsSealed() {
		t.Fatal("barrier reported unsealed with no encryptor installed")
	}
	if _, err := b.Encrypt(nil, []byte("x")); !errors.Is(err, ErrBarrierSealed) {
		t.Fatalf("Encrypt = %v, want ErrBarrierSealed", err)
	}
}

// --- HKDF info domain separation ---

// TestDeriveDEK_V2SeparatesAlgorithmAndKeySize verifies that the v2 info binds
// the algorithm and key length: the AES-128 DEK is no longer the first half of
// the AES-256 DEK for the same (deployment, epoch), as it is under v1.
func TestDeriveDEK_V2SeparatesAlgorithmAndKeySize(t *testing.T) {
	rootKey := make([]byte, rootKeySize)
	salt := make([]byte, hkdfSaltSize)
	for i := range rootKey {
		rootKey[i] = byte(i)
	}

	v1Short, err := deriveDEK(rootKey, salt, 7, 3, AlgAES128GCM, HKDFV1)
	if err != nil {
		t.Fatalf("deriveDEK v1/128: %v", err)
	}
	v1Long, err := deriveDEK(rootKey, salt, 7, 3, AlgAES256GCM, HKDFV1)
	if err != nil {
		t.Fatalf("deriveDEK v1/256: %v", err)
	}
	if !bytes.Equal(v1Short, v1Long[:len(v1Short)]) {
		t.Fatal("v1 layout was expected to produce prefix-related keys; test premise is stale")
	}

	v2Short, err := deriveDEK(rootKey, salt, 7, 3, AlgAES128GCM, HKDFV2)
	if err != nil {
		t.Fatalf("deriveDEK v2/128: %v", err)
	}
	v2Long, err := deriveDEK(rootKey, salt, 7, 3, AlgAES256GCM, HKDFV2)
	if err != nil {
		t.Fatalf("deriveDEK v2/256: %v", err)
	}
	if bytes.Equal(v2Short, v2Long[:len(v2Short)]) {
		t.Fatal("v2 layout still derives the short key as a prefix of the long key")
	}
	if bytes.Equal(v2Long, v1Long) {
		t.Fatal("v2 layout derived the same key as v1")
	}

	// ChaCha20 and AES-256 share a key size but must not share a key.
	v2ChaCha, err := deriveDEK(rootKey, salt, 7, 3, AlgChaCha20Poly1305, HKDFV2)
	if err != nil {
		t.Fatalf("deriveDEK v2/chacha: %v", err)
	}
	if bytes.Equal(v2ChaCha, v2Long) {
		t.Fatal("v2 layout derived the same key for two different algorithms")
	}

	if _, err := deriveDEK(rootKey, salt, 7, 3, AlgAES256GCM, HKDFVersion(9)); !errors.Is(err, ErrUnsupportedHKDFVersion) {
		t.Fatalf("deriveDEK(unknown version) = %v, want ErrUnsupportedHKDFVersion", err)
	}
}

// TestSetSealedRootKey_LegacyBlobDerivesV1 verifies the version gate: a blob
// with no HKDFVersion (persisted before the field existed) must keep deriving
// DEKs with the v1 info layout, and an unknown version is rejected up front.
func TestSetSealedRootKey_LegacyBlobDerivesV1(t *testing.T) {
	b, s, _ := testPersistingBarrier(t)
	defer s.Close()
	sealed := b.SealedRootKeyData()
	if sealed.HKDFVersion != currentHKDFVersion {
		t.Fatalf("Initialize stamped HKDFVersion %d, want %d", sealed.HKDFVersion, currentHKDFVersion)
	}
	rootKey, err := b.RootKey()
	if err != nil {
		t.Fatalf("RootKey: %v", err)
	}

	legacy := b.SealedRootKeyData()
	legacy.HKDFVersion = 0 // pre-versioning blob

	b2 := NewBarrier(DefaultBarrierConfig(1))
	if err := b2.SetSealedRootKey(legacy); err != nil {
		t.Fatalf("SetSealedRootKey(legacy): %v", err)
	}
	s2 := testStrategy(t)
	defer s2.Close()
	if err := b2.Unseal(context.Background(), s2, Credentials{}); err != nil {
		t.Fatalf("Unseal(legacy): %v", err)
	}

	wantV1, err := deriveDEK(rootKey, legacy.Salt, 1, legacy.Epoch, DefaultBarrierConfig(1).Algorithm, HKDFV1)
	if err != nil {
		t.Fatalf("deriveDEK v1: %v", err)
	}
	b2.mu.RLock()
	dek := b2.deks[legacy.Epoch]
	b2.mu.RUnlock()
	buf, err := dek.Open()
	if err != nil {
		t.Fatalf("open DEK: %v", err)
	}
	defer buf.Destroy()
	if !bytes.Equal(buf.Bytes(), wantV1) {
		t.Fatal("legacy blob did not derive its DEK with the v1 info layout")
	}

	unknown := b.SealedRootKeyData()
	unknown.HKDFVersion = currentHKDFVersion + 1
	b3 := NewBarrier(DefaultBarrierConfig(1))
	if err := b3.SetSealedRootKey(unknown); !errors.Is(err, ErrUnsupportedHKDFVersion) {
		t.Fatalf("SetSealedRootKey(unknown version) = %v, want ErrUnsupportedHKDFVersion", err)
	}
}

// --- SoftwareStrategy.Close concurrency ---

// TestSoftwareStrategy_CloseRacesSealUnseal is the regression test for the
// plain `s.passphrase = nil` field write, which raced the reads in
// seal/unseal (a TOCTOU that -race flagged). Every operation must return
// either a valid result or ErrStrategyClosed — never panic, never race.
func TestSoftwareStrategy_CloseRacesSealUnseal(t *testing.T) {
	s := testStrategy(t)
	rootKey := make([]byte, 32)
	sealed, err := s.SealRootKey(context.Background(), rootKey, Credentials{})
	if err != nil {
		t.Fatalf("SealRootKey: %v", err)
	}

	var wg sync.WaitGroup
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, err := s.UnsealRootKey(context.Background(), sealed, Credentials{}); err != nil &&
				!errors.Is(err, ErrStrategyClosed) {
				t.Errorf("UnsealRootKey = %v, want nil or ErrStrategyClosed", err)
			}
		}()
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, err := s.SealRootKey(context.Background(), rootKey, Credentials{}); err != nil &&
				!errors.Is(err, ErrStrategyClosed) {
				t.Errorf("SealRootKey = %v, want nil or ErrStrategyClosed", err)
			}
		}()
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		_ = s.Close()
	}()
	wg.Wait()
}
