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

	if _, err := b.Rotate(context.Background()); err != nil { // epoch 2
		t.Fatalf("Rotate: %v", err)
	}
	if _, err := b.Rotate(context.Background()); err != nil { // epoch 3
		t.Fatalf("Rotate: %v", err)
	}

	purged, err := b.PurgeEpochsBefore(context.Background(), 3)
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
	b2 := MustNewBarrier(DefaultBarrierConfig(1))
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
	if _, err := b.Rotate(context.Background()); err != nil {
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
	b := MustNewBarrier(cfg)
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

	newEpoch, err := b.Rotate(context.Background())
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

	newEpoch, err := b.Rotate(context.Background())
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
	b := MustNewBarrier(DefaultBarrierConfig(1)) // no StatePersister
	s := testStrategy(t)
	defer s.Close()
	if err := b.Initialize(context.Background(), s, Credentials{}); err != nil {
		t.Fatalf("Initialize: %v", err)
	}

	if _, err := b.Rotate(context.Background()); !errors.Is(err, ErrNoStatePersister) {
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

	if _, err := b.Rotate(context.Background()); err != nil {
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
	b2 := MustNewBarrier(DefaultBarrierConfig(1))
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

	if _, err := b.Rotate(context.Background()); err != nil {
		t.Fatalf("Rotate: %v", err)
	}

	// Drop the persister, emulating an embedder that never configured one.
	b.mu.Lock()
	b.config.StatePersister = nil
	b.mu.Unlock()

	if _, err := b.PurgeEpochsBefore(context.Background(), 2); !errors.Is(err, ErrNoStatePersister) {
		t.Fatalf("PurgeEpochsBefore = %v, want ErrNoStatePersister", err)
	}
	b.mu.RLock()
	_, stillThere := b.deks[1]
	b.mu.RUnlock()
	if !stillThere {
		t.Fatal("refused purge dropped the DEK anyway")
	}

	// A no-op purge is allowed without a persister.
	purged, err := b.PurgeEpochsBefore(context.Background(), 1)
	if err != nil || purged != 0 {
		t.Fatalf("no-op PurgeEpochsBefore = (%d, %v), want (0, nil)", purged, err)
	}

	// Restore the persister: the purge now succeeds and persists the
	// reduced, sorted epoch set.
	b.mu.Lock()
	b.config.StatePersister = p.persist
	b.mu.Unlock()
	purged, err = b.PurgeEpochsBefore(context.Background(), 2)
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
	if _, err := b.Rotate(context.Background()); err != nil {
		t.Fatalf("Rotate: %v", err)
	}

	sentinel := errors.New("persist rejected")
	p.mu.Lock()
	p.err = sentinel
	p.mu.Unlock()

	purged, err := b.PurgeEpochsBefore(context.Background(), 2)
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
// SealedRootKey.Strategy field: flipping it to "none" must not steer Unseal
// into the no-encryption branch and produce a "successfully unsealed" barrier
// with no cipher whose first Encrypt panics.
func TestUnseal_StrategyFieldDowngradeRejected(t *testing.T) {
	b, s, _ := testPersistingBarrier(t)
	defer s.Close()
	sealed := b.SealedRootKeyData()

	// Attacker flips the plaintext strategy field.
	sealed.Strategy = StrategyNone

	b2 := MustNewBarrier(DefaultBarrierConfig(1))
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

	b := MustNewBarrier(DefaultBarrierConfig(1))
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

	// Two algorithms that share a key size must not share a key. Which pair
	// that is — and whether one exists at all — depends on the build; see
	// assertV2SeparatesSameKeySizeAlgorithms.
	assertV2SeparatesSameKeySizeAlgorithms(t, rootKey, salt, v2Long)

	// 0 (no version recorded) and 1 (the retired v1 layout) are refused like
	// any version this build does not implement.
	for _, v := range []HKDFVersion{0, 1, 9} {
		if _, err := deriveDEK(rootKey, salt, 7, 3, AlgAES256GCM, v); !errors.Is(err, ErrUnsupportedHKDFVersion) {
			t.Fatalf("deriveDEK(version %d) = %v, want ErrUnsupportedHKDFVersion", v, err)
		}
	}
}

// TestSetSealedRootKey_RefusesRetiredHKDFVersions verifies the version gate: a
// blob with no HKDFVersion (persisted before the field existed), one naming
// the retired v1 layout, and one naming a version this build does not know are
// all refused up front with the standard ErrUnsupportedHKDFVersion.
func TestSetSealedRootKey_RefusesRetiredHKDFVersions(t *testing.T) {
	b, s, _ := testPersistingBarrier(t)
	defer s.Close()
	sealed := b.SealedRootKeyData()
	if sealed.HKDFVersion != currentHKDFVersion {
		t.Fatalf("Initialize stamped HKDFVersion %d, want %d", sealed.HKDFVersion, currentHKDFVersion)
	}

	for _, v := range []HKDFVersion{0, 1, currentHKDFVersion + 1} {
		blob := b.SealedRootKeyData()
		blob.HKDFVersion = v
		b2 := MustNewBarrier(DefaultBarrierConfig(1))
		if err := b2.SetSealedRootKey(blob); !errors.Is(err, ErrUnsupportedHKDFVersion) {
			t.Fatalf("SetSealedRootKey(HKDFVersion %d) = %v, want ErrUnsupportedHKDFVersion", v, err)
		}
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

// --- context-binding capability is unanswerable before it exists ---

// TestBarrier_ContextBinding_UnresolvedBeforeUnseal is the regression for a
// barrier that ANSWERED a question it had no basis to answer: the capability
// was classified from b.hwEncryptor alone, and seal() sets that to nil, so an
// uninitialized or sealed barrier reported the SOFTWARE binding and
// SupportsContextAAD returned true.
//
// That mattered because the capability is latched by consumers. A node may
// start sealed and be unsealed later, so a WAL opened at start would pin
// "records are positionally bound" against a barrier that had not yet chosen a
// strategy — and then be unsealed with a delegated encryptor that fails every
// context-bearing write with ErrHardwareMode, with the deliberate fail-closed
// refusal at open having been a no-op because the capability lied.
//
// The fix makes the pre-unseal state its own answer: Binds() is false for it,
// AND it is distinguishable from an unsealed barrier that genuinely cannot bind.
func TestBarrier_ContextBinding_UnresolvedBeforeUnseal(t *testing.T) {
	// (a) Uninitialized: no strategy has been chosen, so there is no answer.
	fresh := MustNewBarrier(testBarrierConfig(1))
	if got := fresh.ContextBinding(); got != ContextBindingUnresolved {
		t.Errorf("uninitialized barrier reports binding %v, want %v", got, ContextBindingUnresolved)
	}
	if fresh.SupportsContextAAD() {
		t.Error("uninitialized barrier claims it can authenticate caller context")
	}
	if fresh.ContextBindingResolved() {
		t.Error("uninitialized barrier reports its capability as resolved")
	}

	// (b) Sealed: the encryptor is gone, and the capability with it.
	b, s := testInitializedBarrier(t)
	if got := b.ContextBinding(); got != ContextBindingBarrier {
		t.Fatalf("test premise broken: unsealed software barrier reports %v, want %v",
			got, ContextBindingBarrier)
	}
	if err := b.Seal(); err != nil {
		t.Fatalf("Seal: %v", err)
	}
	if got := b.ContextBinding(); got != ContextBindingUnresolved {
		t.Errorf("sealed barrier reports binding %v, want %v", got, ContextBindingUnresolved)
	}
	if b.SupportsContextAAD() {
		t.Error("sealed barrier claims it can authenticate caller context")
	}
	if b.ContextBindingResolved() {
		t.Error("sealed barrier reports its capability as resolved")
	}

	// (c) Unsealed but incapable: a real answer of NO, which a consumer must be
	// able to tell apart from (a)/(b)'s absence of an answer.
	incapable, err := hwBarrier(t, testBarrierConfig(1), &mockHWEncryptor{prefix: []byte("HW:")})
	if err != nil {
		t.Fatalf("Initialize (incapable hw): %v", err)
	}
	if got := incapable.ContextBinding(); got != ContextBindingUnsupported {
		t.Errorf("incapable delegated barrier reports binding %v, want %v",
			got, ContextBindingUnsupported)
	}
	if !incapable.ContextBindingResolved() {
		t.Error("unsealed delegated barrier reports its capability as unresolved")
	}
	if incapable.ContextBinding() == b.ContextBinding() {
		t.Fatal("\"cannot bind\" is indistinguishable from \"cannot answer yet\": " +
			"a consumer that latches the capability cannot fail closed")
	}
	if incapable.SupportsContextAAD() != b.SupportsContextAAD() {
		t.Fatal("test premise broken: the bare predicate already separates the two states")
	}

	// (d) Unsealing restores a real answer, from the strategy it is given.
	if err := b.Unseal(context.Background(), s, Credentials{}); err != nil {
		t.Fatalf("Unseal: %v", err)
	}
	if got := b.ContextBinding(); got != ContextBindingBarrier {
		t.Errorf("re-unsealed software barrier reports binding %v, want %v",
			got, ContextBindingBarrier)
	}
	if !b.ContextBindingResolved() {
		t.Error("re-unsealed barrier reports its capability as unresolved")
	}
}

// --- SetSealedRootKey structural bounds ---

// TestSetSealedRootKey_BoundsKnownEpochs is the DoS regression: KnownEpochs is
// plaintext, unauthenticated metadata, and Unseal derives one HKDF DEK, one
// AEAD and one mlock'd enclave PER ENTRY. An attacker with write access to the
// persisted blob — the same attacker the sealing strategies already defend
// against — could rewrite it to millions of epochs and hang or OOM the node on
// every start, with no passphrase required.
func TestSetSealedRootKey_BoundsKnownEpochs(t *testing.T) {
	src, _ := testInitializedBarrier(t)
	base := src.SealedRootKeyData()
	if base == nil {
		t.Fatal("SealedRootKeyData returned nil")
	}

	epochsUpTo := func(n uint64) []uint64 {
		eps := make([]uint64, 0, n)
		for ep := uint64(1); ep <= n; ep++ {
			eps = append(eps, ep)
		}
		return eps
	}

	for _, tc := range []struct {
		name        string
		epoch       uint64
		knownEpochs []uint64
		want        error
	}{
		{
			name:        "at the bound",
			epoch:       MaxKnownEpochs,
			knownEpochs: epochsUpTo(MaxKnownEpochs),
			want:        nil,
		},
		{
			name:        "one over the bound",
			epoch:       MaxKnownEpochs + 1,
			knownEpochs: epochsUpTo(MaxKnownEpochs + 1),
			want:        ErrTooManyKnownEpochs,
		},
		{
			// The forged shape from the report: a small current epoch padded
			// with millions of derivations. Rejected on size before a single
			// DEK is derived.
			name:        "inflated set on epoch 1",
			epoch:       1,
			knownEpochs: epochsUpTo(MaxKnownEpochs + 5000),
			want:        ErrTooManyKnownEpochs,
		},
		{
			// Epochs only advance and Epoch is the highest reached, so an
			// entry above it is corruption or padding, never history.
			name:        "epoch above current",
			epoch:       2,
			knownEpochs: []uint64{1, 2, 3},
			want:        ErrKnownEpochAboveCurrent,
		},
		{
			name:        "current epoch missing",
			epoch:       7,
			knownEpochs: []uint64{1, 2},
			want:        ErrInvalidSealedEpochs,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			blob := cloneSealedRootKey(base)
			blob.Epoch = tc.epoch
			blob.KnownEpochs = tc.knownEpochs

			b := MustNewBarrier(testBarrierConfig(1))
			err := b.SetSealedRootKey(blob)
			if !errors.Is(err, tc.want) {
				t.Fatalf("SetSealedRootKey = %v, want %v", err, tc.want)
			}
			if tc.want == nil {
				return
			}
			// A rejected blob must leave nothing behind for a later Unseal to
			// derive from.
			if b.CurrentEpoch() != 0 {
				t.Errorf("rejected blob still set epoch %d", b.CurrentEpoch())
			}
			if n := len(b.knownEpochs); n != 0 {
				t.Errorf("rejected blob still installed %d known epochs", n)
			}
		})
	}
}

// TestSetSealedRootKey_RejectsAlgorithmMismatch pins that a blob written under
// one data-path AEAD is refused by a barrier configured for another, naming
// both. Without the check the mismatch surfaces only as the deliberately
// generic ErrDecryptionFailed on every read — indistinguishable from tampering,
// with nothing on disk naming the algorithm that would work. This is the
// restore-onto-a-host-without-AES-NI case, which SelectOptimalAlgorithm makes
// silent by default.
func TestSetSealedRootKey_RejectsAlgorithmMismatch(t *testing.T) {
	writerCfg := testBarrierConfig(1)
	writerCfg.Algorithm = AlgAES256GCM
	writer := MustNewBarrier(writerCfg)
	strategy := testStrategy(t)
	if err := writer.Initialize(context.Background(), strategy, Credentials{}); err != nil {
		t.Fatalf("Initialize: %v", err)
	}
	blob := writer.SealedRootKeyData()
	if blob.Algorithm != AlgAES256GCM {
		t.Fatalf("Initialize did not record the data-path algorithm: got %v", blob.Algorithm)
	}

	readerCfg := testBarrierConfig(1)
	readerCfg.Algorithm = AlgChaCha20Poly1305
	if err := readerCfg.Validate(); err != nil {
		t.Skipf("ChaCha20-Poly1305 unavailable in this build: %v", err)
	}
	reader := MustNewBarrier(readerCfg)
	err := reader.SetSealedRootKey(cloneSealedRootKey(blob))
	var mismatch *AlgorithmMismatchError
	if !errors.As(err, &mismatch) {
		t.Fatalf("SetSealedRootKey = %v, want *AlgorithmMismatchError", err)
	}
	if mismatch.Sealed != AlgAES256GCM || mismatch.Configured != AlgChaCha20Poly1305 {
		t.Errorf("mismatch = {sealed:%v configured:%v}, want {AES-256-GCM ChaCha20-Poly1305}",
			mismatch.Sealed, mismatch.Configured)
	}

	// Back-compatibility: a blob persisted before the field existed carries no
	// algorithm and must still load, under whatever the barrier is configured
	// with. Dev clusters hold exactly such blobs.
	legacy := cloneSealedRootKey(blob)
	legacy.Algorithm = 0
	if err := MustNewBarrier(readerCfg).SetSealedRootKey(legacy); err != nil {
		t.Fatalf("unrecorded-algorithm blob rejected: %v", err)
	}
}

// TestSetSealedRootKey_ClonesCallerBlob pins that the barrier neither aliases
// nor mutates the caller's *SealedRootKey. If b.salt aliased the caller's
// slice, an embedder reusing its decode buffer would silently change the HKDF
// salt and every derived DEK with it — surfacing as ErrDecryptionFailed on data
// that was written correctly.
func TestSetSealedRootKey_ClonesCallerBlob(t *testing.T) {
	src, strategy := testInitializedBarrier(t)
	ciphertext, err := src.Encrypt(nil, []byte("data written before the restart"))
	if err != nil {
		t.Fatalf("Encrypt: %v", err)
	}

	blob := src.SealedRootKeyData()
	blob.FailedAttempts = 3 // below bruteForceThreshold: no backoff, but observable

	b := MustNewBarrier(testBarrierConfig(1))
	if err := b.SetSealedRootKey(blob); err != nil {
		t.Fatalf("SetSealedRootKey: %v", err)
	}

	// The embedder reuses its decode buffer, as it is entitled to.
	for i := range blob.Salt {
		blob.Salt[i] ^= 0xFF
	}

	if err := b.Unseal(context.Background(), strategy, Credentials{}); err != nil {
		t.Fatalf("Unseal after caller reused its buffer: %v", err)
	}
	plaintext, err := b.Decrypt(nil, ciphertext)
	if err != nil {
		t.Fatalf("Decrypt after caller reused its buffer: %v "+
			"(the barrier derived DEKs from the caller's mutated salt)", err)
	}
	if !bytes.Equal(plaintext, []byte("data written before the restart")) {
		t.Fatal("round-trip mismatch after restore")
	}

	// The other direction: a successful Unseal resets the backoff counters
	// through persistBackoffState, which must not reach into the caller's
	// struct.
	if blob.FailedAttempts != 3 {
		t.Errorf("barrier mutated the caller's blob: FailedAttempts = %d, want 3",
			blob.FailedAttempts)
	}
}
