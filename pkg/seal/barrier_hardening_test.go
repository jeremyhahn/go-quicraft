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
	"crypto/cipher"
	"crypto/rand"
	"errors"
	"io"
	"testing"
)

// --- Persisted metadata is authenticated, and epochs cannot roll back ---

// epochFiveBarrier returns a barrier that has been rotated to epoch 5, together
// with the strategy that seals it and every sealed blob the rotations persisted
// (blobs[i] is the state at epoch i+1). It is the setup the epoch-rollback probe
// used: a live deployment several rotations in, whose persisted state an
// attacker can read and rewrite.
func epochFiveBarrier(t *testing.T) (*Barrier, *SoftwareStrategy, []*SealedRootKey) {
	t.Helper()
	var blobs []*SealedRootKey
	cfg := DefaultBarrierConfig(1)
	cfg.StatePersister = func(s *SealedRootKey) error {
		blobs = append(blobs, s)
		return nil
	}
	b := MustNewBarrier(cfg)
	s := testStrategy(t)
	t.Cleanup(func() { _ = s.Close() })
	if err := b.Initialize(context.Background(), s, Credentials{}); err != nil {
		t.Fatalf("Initialize: %v", err)
	}
	blobs = append(blobs, b.SealedRootKeyData())
	for b.CurrentEpoch() < 5 {
		if _, err := b.Encrypt(nil, []byte("epoch traffic")); err != nil {
			t.Fatalf("Encrypt: %v", err)
		}
		if _, err := b.Rotate(context.Background()); err != nil {
			t.Fatalf("Rotate: %v", err)
		}
	}
	if got := b.CurrentEpoch(); got != 5 {
		t.Fatalf("setup epoch = %d, want 5", got)
	}
	return b, s, blobs
}

// TestSealedStateMAC_RejectsEpochRewrite pins the epoch-rewrite rejection: a
// barrier live at epoch 5 with KnownEpochs [1..5] has its persisted blob
// rewritten to Epoch 2 / KnownEpochs [1,2] — touching nothing the root-key AEAD
// covers, since its AAD is the constant strategy name. Were the doctored blob
// to load and unseal cleanly, the barrier would resume encrypting under epoch 2's
// already-spent DEK with a counter freshly seeded from zero and a nonce tracker
// with no memory of the first lifetime's nonces — two independent random-nonce
// sequences under one key, which is precisely what the invocation cap bounds.
func TestSealedStateMAC_RejectsEpochRewrite(t *testing.T) {
	b, s, _ := epochFiveBarrier(t)
	genuine := b.SealedRootKeyData()
	if len(genuine.StateMAC) == 0 {
		t.Fatal("Rotate must stamp a metadata MAC into the persisted blob")
	}

	// The forgery: rewrite ONLY plaintext metadata, leave Ciphertext and
	// StateMAC byte-identical.
	forged := cloneSealedRootKey(genuine)
	forged.Epoch = 2
	forged.KnownEpochs = []uint64{1, 2}
	if !bytes.Equal(forged.Ciphertext, genuine.Ciphertext) {
		t.Fatal("the forgery must not touch the root-key ciphertext")
	}

	b2 := MustNewBarrier(DefaultBarrierConfig(1))
	// The structural validators accept it: KnownEpochs contains Epoch, names
	// nothing above it, the salt is the right length. Nothing structural is
	// wrong with a rolled-back epoch, which is why authentication is needed.
	if err := b2.SetSealedRootKey(forged); err != nil {
		t.Fatalf("SetSealedRootKey(forged) = %v; the forgery is structurally valid, "+
			"so this must fail at authentication in Unseal, not here", err)
	}
	err := b2.Unseal(context.Background(), s, Credentials{})
	if !errors.Is(err, ErrSealedStateUnauthenticated) {
		t.Fatalf("Unseal(forged epoch rewrite) = %v, want ErrSealedStateUnauthenticated", err)
	}
	// Assert the ACTUAL consequence the probe measured, not just the error: the
	// barrier must not be usable at the rolled-back epoch.
	if !b2.IsSealed() {
		t.Fatal("barrier must remain sealed after a failed metadata authentication")
	}
	if _, err := b2.Encrypt(nil, []byte("must never be written under epoch 2")); !errors.Is(err, ErrBarrierSealed) {
		t.Fatalf("Encrypt after refused unseal = %v, want ErrBarrierSealed", err)
	}

	// Control: the untouched blob still loads and unseals, so the test is not
	// passing because everything fails.
	b3 := MustNewBarrier(DefaultBarrierConfig(1))
	if err := b3.SetSealedRootKey(genuine); err != nil {
		t.Fatalf("SetSealedRootKey(genuine): %v", err)
	}
	if err := b3.Unseal(context.Background(), s, Credentials{}); err != nil {
		t.Fatalf("Unseal(genuine): %v", err)
	}
	if got := b3.CurrentEpoch(); got != 5 {
		t.Fatalf("genuine blob unsealed at epoch %d, want 5", got)
	}
}

// TestSealedStateMAC_CoversEveryField walks the metadata the MAC is documented
// to cover and requires each single-field edit to be rejected. A MAC that
// silently omitted a field would otherwise leave exactly that field forgeable,
// and the omission would be invisible.
func TestSealedStateMAC_CoversEveryField(t *testing.T) {
	b, s, _ := epochFiveBarrier(t)
	genuine := b.SealedRootKeyData()

	tamper := map[string]func(*SealedRootKey){
		"epoch":        func(s *SealedRootKey) { s.Epoch = 4 },
		"known_epochs": func(s *SealedRootKey) { s.KnownEpochs = []uint64{5} },
		"salt":         func(s *SealedRootKey) { s.Salt[0] ^= 0xff },
		"hkdf_version": func(s *SealedRootKey) { s.HKDFVersion = 1 },
		"strategy":     func(s *SealedRootKey) { s.Strategy = StrategyNone },
		"version":      func(s *SealedRootKey) { s.Version++ },
		"ciphertext":   func(s *SealedRootKey) { s.Ciphertext[0] ^= 0xff },
		// Appending rather than substituting a specific KDF byte: the
		// strategy's own KDF is build-dependent, and substituting the value it
		// already holds would be a no-op tamper that the MAC is right to accept.
		"metadata":         func(s *SealedRootKey) { s.Metadata = append(bytes.Clone(s.Metadata), 0xFF) },
		"nonce":            func(s *SealedRootKey) { s.Nonce = []byte{1, 2, 3} },
		"hardware_payload": func(s *SealedRootKey) { s.HardwarePayload = []byte{9} },
		"mac":              func(s *SealedRootKey) { s.StateMAC[0] ^= 0xff },
	}
	for name, mutate := range tamper {
		t.Run(name, func(t *testing.T) {
			edited := cloneSealedRootKey(genuine)
			mutate(edited)

			cfg := DefaultBarrierConfig(1)
			// The strategy field is separately gated by the unseal strategy, and
			// a mangled ciphertext fails the AEAD first; both are correct
			// refusals, so accept either that or the MAC error for those cases.
			b2 := MustNewBarrier(cfg)
			if err := b2.SetSealedRootKey(edited); err != nil {
				// Structural rejection is also a refusal; nothing loaded.
				return
			}
			if err := b2.Unseal(context.Background(), s, Credentials{}); err == nil {
				t.Fatalf("Unseal accepted a blob with a tampered %s field", name)
			}
		})
	}

	// The DeploymentID is not a field of the blob at all, but it IS bound into
	// the MAC: moving a blob to a barrier configured for a different deployment
	// must not authenticate.
	other := MustNewBarrier(DefaultBarrierConfig(2))
	if err := other.SetSealedRootKey(cloneSealedRootKey(genuine)); err != nil {
		t.Fatalf("SetSealedRootKey(other deployment): %v", err)
	}
	if err := other.Unseal(context.Background(), s, Credentials{}); !errors.Is(err, ErrSealedStateUnauthenticated) {
		t.Fatalf("Unseal under a different DeploymentID = %v, want ErrSealedStateUnauthenticated", err)
	}
}

// TestSealedStateMAC_AbsentTagFailsClosed pins the downgrade direction: a MAC
// an attacker can simply DELETE is not a MAC. An absent tag is always refused,
// whether the metadata was edited or not: a blob written before the MAC
// existed has no migration path and is wiped and resealed.
func TestSealedStateMAC_AbsentTagFailsClosed(t *testing.T) {
	b, s, _ := epochFiveBarrier(t)
	stripped := b.SealedRootKeyData()
	stripped.StateMAC = nil
	stripped.Epoch = 2
	stripped.KnownEpochs = []uint64{1, 2}

	b2 := MustNewBarrier(DefaultBarrierConfig(1))
	if err := b2.SetSealedRootKey(cloneSealedRootKey(stripped)); err != nil {
		t.Fatalf("SetSealedRootKey: %v", err)
	}
	if err := b2.Unseal(context.Background(), s, Credentials{}); !errors.Is(err, ErrSealedStateUnauthenticated) {
		t.Fatalf("Unseal(stripped MAC) = %v, want ErrSealedStateUnauthenticated", err)
	}

	// The same blob with only its tag removed, metadata untouched, is what an
	// older build left behind. It is refused the same way.
	untagged := b.SealedRootKeyData()
	untagged.StateMAC = nil
	b3 := MustNewBarrier(DefaultBarrierConfig(1))
	if err := b3.SetSealedRootKey(untagged); err != nil {
		t.Fatalf("SetSealedRootKey(untagged): %v", err)
	}
	if err := b3.Unseal(context.Background(), s, Credentials{}); !errors.Is(err, ErrSealedStateUnauthenticated) {
		t.Fatalf("Unseal(untagged) = %v, want ErrSealedStateUnauthenticated", err)
	}
}

// TestSealedEpochRollback_ReplayFloor covers what a MAC provably cannot: the
// replay of an OLDER, genuinely authenticated blob. The attacker keeps the
// epoch-2 state the barrier itself persisted and restores it over epoch 5; its
// MAC verifies, because the barrier really did write it. Only a floor the
// embedder derives from state the attacker would have to roll back in the same
// step — the highest epoch a live WAL segment or retained snapshot still
// references — can reject it.
func TestSealedEpochRollback_ReplayFloor(t *testing.T) {
	b, s, blobs := epochFiveBarrier(t)
	current := b.SealedRootKeyData()

	var epoch2 *SealedRootKey
	for _, blob := range blobs {
		if blob.Epoch == 2 {
			epoch2 = cloneSealedRootKey(blob)
		}
	}
	if epoch2 == nil {
		t.Fatal("setup did not capture a genuine epoch-2 blob")
	}

	// Without a floor the replay is indistinguishable from a legitimate load —
	// which is the honest statement of what authentication alone buys.
	noFloor := MustNewBarrier(DefaultBarrierConfig(1))
	if err := noFloor.SetSealedRootKey(cloneSealedRootKey(epoch2)); err != nil {
		t.Fatalf("SetSealedRootKey(replayed, no floor): %v", err)
	}

	// With the floor the embedder can supply, it is refused.
	cfg := DefaultBarrierConfig(1)
	cfg.MinEpoch = 5
	withFloor := MustNewBarrier(cfg)
	if err := withFloor.SetSealedRootKey(cloneSealedRootKey(epoch2)); !errors.Is(err, ErrSealedEpochRollback) {
		t.Fatalf("SetSealedRootKey(replayed, floor=5) = %v, want ErrSealedEpochRollback", err)
	}
	// The floor must not reject the state that is actually current.
	if err := withFloor.SetSealedRootKey(cloneSealedRootKey(current)); err != nil {
		t.Fatalf("SetSealedRootKey(current, floor=5): %v", err)
	}
	if err := withFloor.Unseal(context.Background(), s, Credentials{}); err != nil {
		t.Fatalf("Unseal(current, floor=5): %v", err)
	}

	// In-process monotonicity needs no configuration: a barrier already holding
	// epoch 5 must refuse to be walked backwards to epoch 2 underneath itself.
	if err := b.SetSealedRootKey(cloneSealedRootKey(epoch2)); !errors.Is(err, ErrSealedEpochRollback) {
		t.Fatalf("SetSealedRootKey(epoch 2) on a live epoch-5 barrier = %v, want ErrSealedEpochRollback", err)
	}
	if got := b.CurrentEpoch(); got != 5 {
		t.Fatalf("epoch after refused rollback = %d, want 5 (nothing may have been adopted)", got)
	}
}

// --- NonceTracking's zero value must not disable detection ---

// TestNonceTracking_ZeroValueDefaultsOn pins that the hand-built config an
// embedder writes when it pins Algorithm and DeploymentID explicitly —
// seal.NewBarrier(seal.BarrierConfig{Algorithm: ..., DeploymentID: 7}) —
// produces a barrier WITH a nonce tracker. Were the field a plain bool
// documented as "Default: true", that config would pass Validate and produce a
// barrier with nonceTracker == nil, since only DefaultBarrierConfig writes
// true.
//
// It asserts the ACTUAL tracker, not a proxy for it, and then that the tracker
// actually rejects a repeat.
func TestNonceTracking_ZeroValueDefaultsOn(t *testing.T) {
	b, err := NewBarrier(BarrierConfig{Algorithm: AlgAES256GCM, DeploymentID: 7})
	if err != nil {
		t.Fatalf("NewBarrier: %v", err)
	}
	nt := b.NonceTracker()
	if nt == nil {
		t.Fatal("a config that never mentions NonceTracking must still get a nonce tracker")
	}
	if !nt.IsEnabled() {
		t.Fatal("the defaulted nonce tracker must be enabled")
	}

	// And it must be wired into the encrypt path, not merely present.
	s := testStrategy(t)
	defer func() { _ = s.Close() }()
	if err := b.Initialize(context.Background(), s, Credentials{}); err != nil {
		t.Fatalf("Initialize: %v", err)
	}
	if _, err := b.Encrypt(nil, []byte("first")); err != nil {
		t.Fatalf("Encrypt: %v", err)
	}
	if nt.Count() != 1 {
		t.Fatalf("tracked nonces after one encrypt = %d, want 1", nt.Count())
	}

	// An explicit false is still honoured.
	off, err := NewBarrier(BarrierConfig{
		Algorithm:     AlgAES256GCM,
		DeploymentID:  7,
		NonceTracking: BoolPtr(false),
	})
	if err != nil {
		t.Fatalf("NewBarrier(tracking off): %v", err)
	}
	if off.NonceTracker() != nil {
		t.Fatal("NonceTracking=BoolPtr(false) must yield no tracker")
	}
}

// TestNonceTracking_CeilingIsDocumentedAndAdjustable pins the second half of
// the nonce-tracking contract: detection is NOT unconditional for the life of
// the epoch. Past the ceiling the tracker saturates — it admits nothing
// further — and the only lever is SetMaxTrackedNonces.
//
// Saturation semantics: the tracker does not clear its enabled flag at the
// ceiling, because that would conflate an operator's switch with
// a memory bound and make a saturated tracker report "off, zero tracked". What
// it stops is ADMISSION; the nonces it already holds stay checked.
func TestNonceTracking_CeilingIsDocumentedAndAdjustable(t *testing.T) {
	nt := NewNonceTracker(true)
	if got := nt.MaxTrackedNonces(); got != DefaultMaxTrackedNonces {
		t.Fatalf("default ceiling = %d, want %d", got, DefaultMaxTrackedNonces)
	}
	nt.SetMaxTrackedNonces(2)
	for i := 0; i < 3; i++ {
		if err := nt.CheckAndRecordNonce([]byte{byte(i), 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}); err != nil {
			t.Fatalf("CheckAndRecordNonce(%d): %v", i, err)
		}
	}
	if !nt.Saturated() {
		t.Fatal("the tracker must saturate once its ceiling is crossed")
	}
	if got := nt.Count(); got != 2 {
		t.Fatalf("tracked count = %d past the ceiling, want it pinned at 2", got)
	}
	// The documented consequence: a nonce OUTSIDE the covered prefix is no
	// longer recorded, so its later repeat cannot be detected.
	uncovered := []byte{9, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}
	if err := nt.CheckAndRecordNonce(uncovered); err != nil {
		t.Fatalf("post-ceiling CheckAndRecordNonce = %v, want nil (outside the covered prefix)", err)
	}
	if err := nt.CheckAndRecordNonce(uncovered); err != nil {
		t.Fatalf("repeat of an uncovered nonce = %v, want nil (it was never admitted)", err)
	}
	// Raising the ceiling and clearing (what a rotation does) restores full
	// coverage.
	nt.SetMaxTrackedNonces(DefaultMaxTrackedNonces)
	if err := nt.Clear(); err != nil {
		t.Fatalf("Clear: %v", err)
	}
	if nt.Saturated() {
		t.Fatal("Clear must lift saturation on a tracker that hit its ceiling")
	}
	if !nt.IsEnabled() {
		t.Fatal("Clear must leave an enabled tracker enabled")
	}
}

// --- The context-AAD capability must be measured, not declared ---

// lyingAADEncryptor implements AADSymmetricEncrypter and IGNORES the aad,
// exactly what that interface's doc comment begs implementers not to do — and
// exactly what a structural type assertion cannot detect, because its method
// set is identical to an honest implementation's.
type lyingAADEncryptor struct{ aead cipher.AEAD }

func newLyingAADEncryptor(t *testing.T) *lyingAADEncryptor {
	t.Helper()
	key := make([]byte, 32)
	if _, err := io.ReadFull(rand.Reader, key); err != nil {
		t.Fatalf("key: %v", err)
	}
	// newAESGCM, not cipher.NewGCM — see newMockAADEncryptor. The framing in
	// this mock is expressed via aead.NonceSize() and therefore works for both
	// the caller-nonce and module-nonce constructions.
	aead, err := newAESGCM(key)
	if err != nil {
		t.Fatalf("gcm: %v", err)
	}
	return &lyingAADEncryptor{aead: aead}
}

func (e *lyingAADEncryptor) Encrypt(plaintext []byte) ([]byte, error) {
	return e.EncryptWithAAD(plaintext, nil)
}

func (e *lyingAADEncryptor) Decrypt(ciphertext []byte) ([]byte, error) {
	return e.DecryptWithAAD(ciphertext, nil)
}

// EncryptWithAAD accepts the aad and drops it.
func (e *lyingAADEncryptor) EncryptWithAAD(plaintext, _ []byte) ([]byte, error) {
	nonce := make([]byte, e.aead.NonceSize())
	if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
		return nil, err
	}
	return e.aead.Seal(nonce, nonce, plaintext, nil), nil
}

// DecryptWithAAD likewise ignores the aad, so any aad opens any ciphertext.
func (e *lyingAADEncryptor) DecryptWithAAD(ciphertext, _ []byte) ([]byte, error) {
	n := e.aead.NonceSize()
	if len(ciphertext) < n {
		return nil, ErrDecryptionFailed
	}
	return e.aead.Open(nil, ciphertext[:n], ciphertext[n:], nil)
}

// honestAADEncryptor authenticates the aad for real.
type honestAADEncryptor struct{ aead cipher.AEAD }

func newHonestAADEncryptor(t *testing.T) *honestAADEncryptor {
	t.Helper()
	l := newLyingAADEncryptor(t)
	return &honestAADEncryptor{aead: l.aead}
}

func (e *honestAADEncryptor) Encrypt(plaintext []byte) ([]byte, error) {
	return e.EncryptWithAAD(plaintext, nil)
}

func (e *honestAADEncryptor) Decrypt(ciphertext []byte) ([]byte, error) {
	return e.DecryptWithAAD(ciphertext, nil)
}

func (e *honestAADEncryptor) EncryptWithAAD(plaintext, aad []byte) ([]byte, error) {
	nonce := make([]byte, e.aead.NonceSize())
	if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
		return nil, err
	}
	return e.aead.Seal(nonce, nonce, plaintext, aad), nil
}

func (e *honestAADEncryptor) DecryptWithAAD(ciphertext, aad []byte) ([]byte, error) {
	n := e.aead.NonceSize()
	if len(ciphertext) < n {
		return nil, ErrDecryptionFailed
	}
	return e.aead.Open(nil, ciphertext[:n], ciphertext[n:], aad)
}

// TestContextBinding_ProbeCatchesLyingEncryptor pins that context binding is
// established by a behavioural probe, not a Go METHOD SET. Were a method set
// enough, an encryptor that accepted the aad and dropped it would report
// ContextBindingDelegated, callers would stop suppressing their positional
// context, and RequireContextAAD — which is documented as refusing a
// misconfigured deployment "before it writes a single unbound byte" — would
// pass at startup. The implementations live in another
// repository, so the interface's warning comment was the entire enforcement.
func TestContextBinding_ProbeCatchesLyingEncryptor(t *testing.T) {
	ctx := context.Background()

	t.Run("lying encryptor is classified unsupported", func(t *testing.T) {
		b := MustNewBarrier(DefaultBarrierConfig(1))
		s := &hwStrategy{hwEnc: newLyingAADEncryptor(t)}
		if err := b.Initialize(ctx, s, Credentials{}); err != nil {
			t.Fatalf("Initialize: %v", err)
		}
		if got := b.ContextBinding(); got != ContextBindingUnsupported {
			t.Fatalf("ContextBinding = %v, want unsupported: the encryptor implements "+
				"AADSymmetricEncrypter but does not authenticate the AAD", got)
		}
		if b.SupportsContextAAD() {
			t.Fatal("SupportsContextAAD must be false for an encryptor that drops the AAD")
		}
		// The caller's context is refused rather than silently unbound.
		if _, err := b.EncryptWithContext(nil, []byte("frame"), []byte("shard/1/index/9")); !errors.Is(err, ErrHardwareMode) {
			t.Fatalf("EncryptWithContext = %v, want ErrHardwareMode", err)
		}
	})

	t.Run("honest encryptor is classified delegated", func(t *testing.T) {
		b := MustNewBarrier(DefaultBarrierConfig(1))
		s := &hwStrategy{hwEnc: newHonestAADEncryptor(t)}
		if err := b.Initialize(ctx, s, Credentials{}); err != nil {
			t.Fatalf("Initialize: %v", err)
		}
		if got := b.ContextBinding(); got != ContextBindingDelegated {
			t.Fatalf("ContextBinding = %v, want delegated", got)
		}
		aad := []byte("shard/1/index/9")
		ct, err := b.EncryptWithContext(nil, []byte("frame"), aad)
		if err != nil {
			t.Fatalf("EncryptWithContext: %v", err)
		}
		if got, err := b.DecryptWithContext(nil, ct, aad); err != nil || !bytes.Equal(got, []byte("frame")) {
			t.Fatalf("DecryptWithContext = (%q, %v), want (\"frame\", nil)", got, err)
		}
		if _, err := b.DecryptWithContext(nil, ct, []byte("shard/1/index/8")); err == nil {
			t.Fatal("a different context must not open the frame")
		}
	})

	t.Run("RequireContextAAD refuses a lying encryptor at setup", func(t *testing.T) {
		cfg := DefaultBarrierConfig(1)
		cfg.RequireContextAAD = true
		b := MustNewBarrier(cfg)
		s := &hwStrategy{hwEnc: newLyingAADEncryptor(t)}
		if err := b.Initialize(ctx, s, Credentials{}); !errors.Is(err, ErrContextAADUnsupported) {
			t.Fatalf("Initialize = %v, want ErrContextAADUnsupported", err)
		}
		if b.IsInitialized() {
			t.Fatal("a refused setup must leave the barrier uninitialized")
		}
	})

	t.Run("RequireContextAAD accepts an honest encryptor", func(t *testing.T) {
		cfg := DefaultBarrierConfig(1)
		cfg.RequireContextAAD = true
		b := MustNewBarrier(cfg)
		s := &hwStrategy{hwEnc: newHonestAADEncryptor(t)}
		if err := b.Initialize(ctx, s, Credentials{}); err != nil {
			t.Fatalf("Initialize with an honest AAD encryptor: %v", err)
		}
	})
}

// TestVerifyAADBinding_Stages exercises the probe directly, so a future change
// that made it vacuous (always returning nil) would be caught.
func TestVerifyAADBinding_Stages(t *testing.T) {
	if err := verifyAADBinding(newHonestAADEncryptor(t), rand.Reader); err != nil {
		t.Fatalf("honest encryptor failed the probe: %v", err)
	}
	err := verifyAADBinding(newLyingAADEncryptor(t), rand.Reader)
	if !errors.Is(err, ErrAADProbeNotBound) {
		t.Fatalf("lying encryptor probe = %v, want ErrAADProbeNotBound", err)
	}
	var probeErr *AADBindingProbeError
	if !errors.As(err, &probeErr) || probeErr.Stage != "decrypt-wrong-aad" {
		t.Fatalf("probe error = %#v, want stage decrypt-wrong-aad", err)
	}
}

// --- FIPS mode must constrain the KDF, and must be requestable ---

// TestFIPS_ConstrainsKDF pins that FIPS mode constrains BOTH the KDF
// (PBKDF2-SHA256 instead of Argon2id) and the data-path cipher, as fips.go
// states. If DeriveKey ignored FIPS, or NewSoftwareStrategyWithParams hardcoded
// Argon2id, a FIPS-140 deployment would wrap its root key under a non-approved
// KDF with no error and no warning.
func TestFIPS_ConstrainsKDF(t *testing.T) {
	salt := make([]byte, 32)
	pass := []byte("a-passphrase-for-the-kdf")

	// Baseline before FIPS is requested. What "baseline" means is
	// build-dependent: a normal build derives with Argon2id here, and a build
	// with -tags fips has no Argon2id at all — see assertArgon2idBaseline.
	restore := AllowWeakKDFParams()
	defer restore()
	assertArgon2idBaseline(t, pass, salt)

	t.Setenv(envGOFIPS140, "on")
	if !FIPSEnabled() {
		t.Fatal("FIPSEnabled must report true with GOFIPS140 set")
	}
	assertArgon2idRefusedUnderFIPS(t, pass, salt)
	// The approved KDF still works, so the rule is about approval, not breakage.
	if _, err := DeriveKey(pass, salt, KDFPbkdf2SHA256, DefaultFIPSKDFParams()); err != nil {
		t.Fatalf("DeriveKey(PBKDF2) under FIPS: %v", err)
	}
	// The auto-selecting constructor picks the approved KDF and succeeds.
	s, err := NewSoftwareStrategy(context.Background(), pass)
	if err != nil {
		t.Fatalf("NewSoftwareStrategy under FIPS: %v", err)
	}
	defer func() { _ = s.Close() }()
	if s.KDFVersion() != KDFPbkdf2SHA256 {
		t.Fatalf("NewSoftwareStrategy under FIPS selected KDF %d, want PBKDF2-SHA256", s.KDFVersion())
	}
}

// TestRequireFIPS_FailsClosedWhenUnavailable pins the other half: without a
// RequireFIPS flag, "FIPS was not requested" and "FIPS was requested but this
// binary has no module" are indistinguishable, and the second silently falls
// back to Argon2id + whatever cipher was auto-selected.
func TestRequireFIPS_FailsClosedWhenUnavailable(t *testing.T) {
	cfg := DefaultBarrierConfig(1)
	cfg.Algorithm = AlgAES256GCM
	cfg.RequireFIPS = true

	if FIPSEnabled() {
		t.Skip("this binary has the FIPS module active; the unavailable case cannot be constructed here")
	}
	if _, err := NewBarrier(cfg); !errors.Is(err, ErrFIPSUnavailable) {
		t.Fatalf("NewBarrier(RequireFIPS, no module) = %v, want ErrFIPSUnavailable", err)
	}
	if err := cfg.Validate(); !errors.Is(err, ErrFIPSUnavailable) {
		t.Fatalf("Validate(RequireFIPS, no module) = %v, want ErrFIPSUnavailable", err)
	}

	// With the module signalled, the same config constructs.
	t.Setenv(envGOFIPS140, "on")
	if _, err := NewBarrier(cfg); err != nil {
		t.Fatalf("NewBarrier(RequireFIPS, module present): %v", err)
	}
	// And RequireFIPS off is unaffected either way.
	cfg.RequireFIPS = false
	if _, err := NewBarrier(cfg); err != nil {
		t.Fatalf("NewBarrier(RequireFIPS=false): %v", err)
	}
}

// --- What positional binding does and does not deliver ---

// TestContextAAD_ResistsReorderAndSpliceButNotTruncation is a CHARACTERIZATION
// test. The positional AAD prevents an at-rest attacker from reordering or
// splicing same-epoch frames, but not from TRUNCATING them, and nothing in this
// package can make it, because nothing in the AAD names the number of frames
// or the total length. Truncation resistance is a design the frame owner must
// implement — and this test is what keeps EncryptWithContext's doc comment
// honest: it asserts each half, including the negative one, so a future reader
// can see the gap rather than infer protection from a sentence.
func TestContextAAD_ResistsReorderAndSpliceButNotTruncation(t *testing.T) {
	b, _ := testInitializedBarrier(t)

	// A four-frame artifact, each frame bound to its own ordinal.
	frameAAD := func(artifact uint64, ordinal int) []byte {
		return []byte{byte(artifact), byte(ordinal)}
	}
	const frames = 4
	artifact := make([][]byte, frames)
	for i := 0; i < frames; i++ {
		ct, err := b.EncryptWithContext(nil, []byte{byte('a' + i)}, frameAAD(7, i))
		if err != nil {
			t.Fatalf("EncryptWithContext(%d): %v", i, err)
		}
		artifact[i] = ct
	}

	// REORDER is caught: frame 2 does not open at position 1.
	if _, err := b.DecryptWithContext(nil, artifact[2], frameAAD(7, 1)); !errors.Is(err, ErrDecryptionFailed) {
		t.Fatalf("reordered frame = %v, want ErrDecryptionFailed", err)
	}
	// SPLICE from another artifact is caught.
	foreign, err := b.EncryptWithContext(nil, []byte("x"), frameAAD(9, 1))
	if err != nil {
		t.Fatalf("EncryptWithContext(foreign): %v", err)
	}
	if _, err := b.DecryptWithContext(nil, foreign, frameAAD(7, 1)); !errors.Is(err, ErrDecryptionFailed) {
		t.Fatalf("spliced frame = %v, want ErrDecryptionFailed", err)
	}

	// TRUNCATION is NOT caught. Drop the tail and every surviving frame still
	// authenticates perfectly at its own position; a reader that stops at EOF
	// sees a complete, shorter artifact. The doc comment makes no claim of
	// truncation resistance.
	truncated := artifact[:2]
	for i, ct := range truncated {
		got, err := b.DecryptWithContext(nil, ct, frameAAD(7, i))
		if err != nil {
			t.Fatalf("truncated artifact frame %d: %v", i, err)
		}
		if !bytes.Equal(got, []byte{byte('a' + i)}) {
			t.Fatalf("truncated artifact frame %d = %q", i, got)
		}
	}

	// And the shape of the fix the caller must supply: fold the frame COUNT
	// into every frame's context, and a truncated artifact stops
	// authenticating, because the count the reader is told to expect no longer
	// matches the one the writer bound.
	countedAAD := func(artifact uint64, ordinal, total int) []byte {
		return []byte{byte(artifact), byte(ordinal), byte(total)}
	}
	counted, err := b.EncryptWithContext(nil, []byte("a"), countedAAD(7, 0, frames))
	if err != nil {
		t.Fatalf("EncryptWithContext(counted): %v", err)
	}
	if _, err := b.DecryptWithContext(nil, counted, countedAAD(7, 0, 2)); !errors.Is(err, ErrDecryptionFailed) {
		t.Fatalf("count-bound frame read as part of a 2-frame artifact = %v, want ErrDecryptionFailed", err)
	}
}

// --- The single-writer contract on persisted barrier state ---

// TestTwoLiveBarriersOverOneBlobGoStale states, as an executable contract, that
// a second live Barrier over the SAME persisted state is not a supported
// configuration: once either one rotates, the other holds an epoch set that no
// longer covers the data being written, and its reads fail with the generic
// ErrDecryptionFailed.
//
// This is not new with the restart-rotation, and the test proves it by rotating
// EXPLICITLY: it fails identically on a barrier that never restarts. What
// changed is only how easy the condition is to reach — restoring persisted
// state now rotates, so any holder of a pre-restore barrier object goes stale
// immediately rather than at the embedder's next scheduled rotation.
//
// The correct pattern is one barrier per process over one blob, re-loaded from
// that blob after any other process may have written it. Deriving an unknown
// epoch's DEK on demand would paper over this and must not be done: the epoch
// in a ciphertext header is unauthenticated, so on-demand derivation is an
// unbounded HKDF/AEAD/mlock amplifier driven by attacker bytes, and it would
// silently undo PurgeEpochsBefore's capability revocation by making every
// purged epoch decryptable again.
func TestTwoLiveBarriersOverOneBlobGoStale(t *testing.T) {
	cfg := testBarrierConfig(1)
	first := MustNewBarrier(cfg)
	s := testStrategy(t)
	defer func() { _ = s.Close() }()
	if err := first.Initialize(context.Background(), s, Credentials{}); err != nil {
		t.Fatalf("Initialize: %v", err)
	}

	second := MustNewBarrier(cfg)
	if err := second.SetSealedRootKey(first.SealedRootKeyData()); err != nil {
		t.Fatalf("SetSealedRootKey: %v", err)
	}
	if err := second.Unseal(context.Background(), s, Credentials{}); err != nil {
		t.Fatalf("Unseal: %v", err)
	}
	// Rotate explicitly, so the contract is demonstrated independently of the
	// restart-rotation that already moved this barrier to epoch 2.
	if _, err := second.Rotate(context.Background()); err != nil {
		t.Fatalf("Rotate: %v", err)
	}

	payload := []byte("written by the second barrier")
	ct, err := second.Encrypt(nil, payload)
	if err != nil {
		t.Fatalf("Encrypt: %v", err)
	}
	if first.CurrentEpoch() == second.CurrentEpoch() {
		t.Fatalf("both barriers report epoch %d; the premise of this test is gone", first.CurrentEpoch())
	}
	if _, err := first.Decrypt(nil, ct); !errors.Is(err, ErrDecryptionFailed) {
		t.Fatalf("stale barrier Decrypt = %v, want ErrDecryptionFailed", err)
	}

	// Re-loading the blob is what makes the first barrier current again.
	reloaded := MustNewBarrier(cfg)
	if err := reloaded.SetSealedRootKey(second.SealedRootKeyData()); err != nil {
		t.Fatalf("SetSealedRootKey(reload): %v", err)
	}
	if err := reloaded.Unseal(context.Background(), s, Credentials{}); err != nil {
		t.Fatalf("Unseal(reload): %v", err)
	}
	got, err := reloaded.Decrypt(nil, ct)
	if err != nil {
		t.Fatalf("reloaded barrier Decrypt: %v", err)
	}
	if !bytes.Equal(got, payload) {
		t.Fatalf("reloaded barrier decrypted %q, want %q", got, payload)
	}
}
