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

// mockAADEncryptor is a delegated encryptor that DOES authenticate
// caller-supplied AAD, so it implements AADSymmetricEncrypter. It stands in for
// a hardware/KMS encryptor whose API carries AAD end to end. The AEAD is real
// (AES-256-GCM under a fixed test key) precisely so that a wrong AAD fails the
// way a real one would, instead of being detected by a bespoke comparison.
type mockAADEncryptor struct {
	aead cipher.AEAD
}

func newMockAADEncryptor(t *testing.T) *mockAADEncryptor {
	t.Helper()
	key := make([]byte, 32)
	if _, err := io.ReadFull(rand.Reader, key); err != nil {
		t.Fatalf("key generation failed: %v", err)
	}
	// newAESGCM, not cipher.NewGCM: the mock stands in for a real hardware
	// encryptor, and a real one has to be FIPS-approved. Its framing below is
	// written in terms of aead.NonceSize(), so it is correct for a module-nonce
	// AEAD (which reports 0 and carries the nonce in its own output) without
	// any special-casing.
	aead, err := newAESGCM(key)
	if err != nil {
		t.Fatalf("newAESGCM failed: %v", err)
	}
	return &mockAADEncryptor{aead: aead}
}

func (m *mockAADEncryptor) Encrypt(plaintext []byte) ([]byte, error) {
	return m.EncryptWithAAD(plaintext, nil)
}

func (m *mockAADEncryptor) Decrypt(ciphertext []byte) ([]byte, error) {
	return m.DecryptWithAAD(ciphertext, nil)
}

func (m *mockAADEncryptor) EncryptWithAAD(plaintext, aad []byte) ([]byte, error) {
	nonce := make([]byte, m.aead.NonceSize())
	if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
		return nil, err
	}
	return m.aead.Seal(nonce, nonce, plaintext, aad), nil
}

func (m *mockAADEncryptor) DecryptWithAAD(ciphertext, aad []byte) ([]byte, error) {
	ns := m.aead.NonceSize()
	if len(ciphertext) < ns {
		return nil, ErrCiphertextTooShort
	}
	pt, err := m.aead.Open(nil, ciphertext[:ns], ciphertext[ns:], aad)
	if err != nil {
		return nil, ErrDecryptionFailed
	}
	return pt, nil
}

// Compile-time proof that the capable mock declares the capability and the
// legacy mock deliberately does NOT.
var (
	_ AADSymmetricEncrypter = (*mockAADEncryptor)(nil)
	_ SymmetricEncrypter    = (*mockHWEncryptor)(nil)
)

// hwBarrier initializes a barrier in delegated mode with enc installed.
func hwBarrier(t *testing.T, cfg BarrierConfig, enc SymmetricEncrypter) (*Barrier, error) {
	t.Helper()
	b := MustNewBarrier(cfg)
	return b, b.Initialize(context.Background(), &hwStrategy{hwEnc: enc}, Credentials{})
}

// --- (a) a capable delegated encryptor carries the context end to end ---

// TestBarrier_HardwareAAD_RoundTripAndMismatch is the point of the whole
// change: a hardware barrier whose encryptor implements AADSymmetricEncrypter
// gets the SAME positional binding a software barrier gets. A frame sealed under
// one context must not open under another, which is what makes moving, splicing
// or truncating a frame detectable.
func TestBarrier_HardwareAAD_RoundTripAndMismatch(t *testing.T) {
	b, err := hwBarrier(t, testBarrierConfig(1), newMockAADEncryptor(t))
	if err != nil {
		t.Fatalf("Initialize (capable hw) failed: %v", err)
	}

	plaintext := []byte("hardware-sealed snapshot frame")
	ctxA := []byte("QRSNAP1|shard=1|replica=2|index=7|chunk=0")
	ctxB := []byte("QRSNAP1|shard=1|replica=2|index=7|chunk=1")

	ct, err := b.EncryptWithContext(nil, plaintext, ctxA)
	if err != nil {
		t.Fatalf("EncryptWithContext (capable hw) failed: %v", err)
	}
	got, err := b.DecryptWithContext(nil, ct, ctxA)
	if err != nil {
		t.Fatalf("DecryptWithContext with matching context failed: %v", err)
	}
	if !bytes.Equal(got, plaintext) {
		t.Fatalf("round-trip mismatch: got %q want %q", got, plaintext)
	}

	// Replayed at another ordinal: authentication must fail.
	if _, err := b.DecryptWithContext(nil, ct, ctxB); !errors.Is(err, ErrDecryptionFailed) {
		t.Fatalf("DecryptWithContext with wrong context: got %v want ErrDecryptionFailed", err)
	}
	// Stripped of its context entirely: also fails. An empty context takes the
	// no-AAD delegated path, so this is a genuine authentication failure and
	// not an accidental match.
	if _, err := b.DecryptWithContext(nil, ct, nil); err == nil {
		t.Fatal("DecryptWithContext with no context opened a context-bound frame")
	}
	if _, err := b.Decrypt(nil, ct); err == nil {
		t.Fatal("Decrypt opened a context-bound frame sealed by the delegated encryptor")
	}
}

// TestBarrier_HardwareAAD_EpochVariantsCarryContext covers the epoch-returning
// and pinning variants, which snapshot and WAL callers actually use.
func TestBarrier_HardwareAAD_EpochVariantsCarryContext(t *testing.T) {
	b, err := hwBarrier(t, testBarrierConfig(1), newMockAADEncryptor(t))
	if err != nil {
		t.Fatalf("Initialize (capable hw) failed: %v", err)
	}

	plaintext := []byte("wal record payload")
	recordCtx := []byte("QRWAL1|shard=3|segment=7|seq=11")
	otherCtx := []byte("QRWAL1|shard=3|segment=7|seq=12")

	ct, epoch, err := b.EncryptWithEpochContext(nil, plaintext, recordCtx)
	if err != nil {
		t.Fatalf("EncryptWithEpochContext (capable hw) failed: %v", err)
	}
	if epoch != b.CurrentEpoch() {
		t.Fatalf("epoch = %d, want current epoch %d", epoch, b.CurrentEpoch())
	}
	got, err := b.DecryptForEpochContext(nil, ct, epoch, recordCtx)
	if err != nil {
		t.Fatalf("DecryptForEpochContext with matching context failed: %v", err)
	}
	if !bytes.Equal(got, plaintext) {
		t.Fatalf("round-trip mismatch: got %q want %q", got, plaintext)
	}
	if _, err := b.DecryptForEpochContext(nil, ct, epoch, otherCtx); !errors.Is(err, ErrDecryptionFailed) {
		t.Fatalf("DecryptForEpochContext with wrong context: got %v want ErrDecryptionFailed", err)
	}

	pinnedCT, pinnedEpoch, err := b.EncryptWithEpochPinnedContext(nil, plaintext, recordCtx)
	if err != nil {
		t.Fatalf("EncryptWithEpochPinnedContext (capable hw) failed: %v", err)
	}
	defer b.DeregisterInFlightEpoch(pinnedEpoch)
	if _, err := b.DecryptWithContext(nil, pinnedCT, recordCtx); err != nil {
		t.Fatalf("DecryptWithContext on pinned ciphertext failed: %v", err)
	}
	b.inFlightMu.Lock()
	pins := b.inFlightEpochs[pinnedEpoch]
	b.inFlightMu.Unlock()
	if pins != 1 {
		t.Fatalf("in-flight pin count for epoch %d = %d, want 1", pinnedEpoch, pins)
	}
}

// TestBarrier_HardwareAAD_EmptyContextStaysCompatible pins the deliberate
// carve-out: with no context supplied there is nothing to bind, so the call
// takes the plain delegated path and stays interchangeable with Encrypt. If this
// ever diverged, ciphertext written through one method would stop opening
// through the other.
func TestBarrier_HardwareAAD_EmptyContextStaysCompatible(t *testing.T) {
	b, err := hwBarrier(t, testBarrierConfig(1), newMockAADEncryptor(t))
	if err != nil {
		t.Fatalf("Initialize (capable hw) failed: %v", err)
	}

	plaintext := []byte("context-free frame")
	ct, err := b.EncryptWithContext(nil, plaintext, nil)
	if err != nil {
		t.Fatalf("EncryptWithContext(nil) failed: %v", err)
	}
	got, err := b.Decrypt(nil, ct)
	if err != nil {
		t.Fatalf("Decrypt on context-free delegated ciphertext failed: %v", err)
	}
	if !bytes.Equal(got, plaintext) {
		t.Fatalf("round-trip mismatch: got %q want %q", got, plaintext)
	}

	ct2, err := b.Encrypt(nil, plaintext)
	if err != nil {
		t.Fatalf("Encrypt failed: %v", err)
	}
	if _, err := b.DecryptWithContext(nil, ct2, nil); err != nil {
		t.Fatalf("DecryptWithContext(nil) on Encrypt output failed: %v", err)
	}
}

// --- (b) an incapable delegated encryptor still refuses, never drops ---

// TestBarrier_HardwareNoAAD_RefusesContext is the fail-closed half: an encryptor
// that cannot authenticate AAD must reject a context outright. Silently dropping
// it would hand the caller a frame it believes is positionally bound and is not.
func TestBarrier_HardwareNoAAD_RefusesContext(t *testing.T) {
	b, err := hwBarrier(t, testBarrierConfig(1), &mockHWEncryptor{prefix: []byte("HW:")})
	if err != nil {
		t.Fatalf("Initialize (incapable hw) failed: %v", err)
	}

	plaintext := []byte("frame the hardware cannot bind")
	ctx := []byte("QRSNAP1|shard=1|replica=2|index=7|chunk=0")

	if _, err := b.EncryptWithContext(nil, plaintext, ctx); !errors.Is(err, ErrHardwareMode) {
		t.Fatalf("EncryptWithContext: got %v want ErrHardwareMode", err)
	}
	if _, _, err := b.EncryptWithEpochContext(nil, plaintext, ctx); !errors.Is(err, ErrHardwareMode) {
		t.Fatalf("EncryptWithEpochContext: got %v want ErrHardwareMode", err)
	}
	if _, _, err := b.EncryptWithEpochPinnedContext(nil, plaintext, ctx); !errors.Is(err, ErrHardwareMode) {
		t.Fatalf("EncryptWithEpochPinnedContext: got %v want ErrHardwareMode", err)
	}
	if _, err := b.DecryptWithContext(nil, []byte("HW:whatever"), ctx); !errors.Is(err, ErrHardwareMode) {
		t.Fatalf("DecryptWithContext: got %v want ErrHardwareMode", err)
	}
	if _, err := b.DecryptForEpochContext(nil, []byte("HW:whatever"), 1, ctx); !errors.Is(err, ErrHardwareMode) {
		t.Fatalf("DecryptForEpochContext: got %v want ErrHardwareMode", err)
	}

	// A refused encrypt must not have pinned an epoch on the way out.
	b.inFlightMu.Lock()
	pins := len(b.inFlightEpochs)
	b.inFlightMu.Unlock()
	if pins != 0 {
		t.Fatalf("refused encrypt left %d in-flight epoch pins", pins)
	}

	// Context-free operation is unchanged.
	ct, err := b.EncryptWithContext(nil, plaintext, nil)
	if err != nil {
		t.Fatalf("EncryptWithContext(nil) on incapable hw failed: %v", err)
	}
	got, err := b.DecryptWithContext(nil, ct, nil)
	if err != nil {
		t.Fatalf("DecryptWithContext(nil) on incapable hw failed: %v", err)
	}
	if !bytes.Equal(got, plaintext) {
		t.Fatalf("round-trip mismatch: got %q want %q", got, plaintext)
	}
}

// --- (d) and (e): capability reporting and the None-vs-hardware distinction ---

// TestBarrier_SupportsContextAAD_PerMode pins what each mode reports, including
// the distinction IsHardwareMode cannot express: no-encryption mode and an
// incapable hardware encryptor both fail to bind context, but only one of them
// is a security limitation.
func TestBarrier_SupportsContextAAD_PerMode(t *testing.T) {
	software, _ := testInitializedBarrier(t)

	capable, err := hwBarrier(t, testBarrierConfig(1), newMockAADEncryptor(t))
	if err != nil {
		t.Fatalf("Initialize (capable hw) failed: %v", err)
	}
	incapable, err := hwBarrier(t, testBarrierConfig(1), &mockHWEncryptor{prefix: []byte("HW:")})
	if err != nil {
		t.Fatalf("Initialize (incapable hw) failed: %v", err)
	}

	none := MustNewBarrier(testBarrierConfig(1))
	if err := none.Initialize(context.Background(), NewNoneStrategy(), Credentials{}); err != nil {
		t.Fatalf("Initialize (none) failed: %v", err)
	}

	for _, tc := range []struct {
		name      string
		b         *Barrier
		supports  bool
		hardware  bool
		plaintext bool
		binding   ContextBinding
	}{
		{"software", software, true, false, false, ContextBindingBarrier},
		{"capable hardware", capable, true, true, false, ContextBindingDelegated},
		{"incapable hardware", incapable, false, true, false, ContextBindingUnsupported},
		{"no encryption", none, false, true, true, ContextBindingPlaintext},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := tc.b.SupportsContextAAD(); got != tc.supports {
				t.Errorf("SupportsContextAAD = %v, want %v", got, tc.supports)
			}
			if got := tc.b.IsHardwareMode(); got != tc.hardware {
				t.Errorf("IsHardwareMode = %v, want %v", got, tc.hardware)
			}
			if got := tc.b.IsPlaintextMode(); got != tc.plaintext {
				t.Errorf("IsPlaintextMode = %v, want %v", got, tc.plaintext)
			}
			if got := tc.b.ContextBinding(); got != tc.binding {
				t.Errorf("ContextBinding = %v, want %v", got, tc.binding)
			}
			if got := tc.b.ContextBinding().Binds(); got != tc.supports {
				t.Errorf("ContextBinding().Binds() = %v, want %v", got, tc.supports)
			}
		})
	}

	// The distinction the enum exists for: both non-binding modes report the
	// same IsHardwareMode, and only IsPlaintextMode/ContextBinding separate
	// "nothing to bind" from "binding unavailable".
	if none.IsHardwareMode() != incapable.IsHardwareMode() {
		t.Fatal("test premise broken: IsHardwareMode already distinguishes the two modes")
	}
	if none.ContextBinding() == incapable.ContextBinding() {
		t.Fatal("no-encryption mode is indistinguishable from an incapable hardware encryptor")
	}
}

// TestBarrier_ContextBinding_ClearedAfterSeal pins that the capability follows
// the encryptor: sealing drops it, and a later unseal re-derives it from the
// strategy it is given rather than from a stale field.
func TestBarrier_ContextBinding_ClearedAfterSeal(t *testing.T) {
	strategy := &hwStrategy{hwEnc: &mockHWEncryptor{prefix: []byte("HW:")}}
	b := MustNewBarrier(testBarrierConfig(1))
	if err := b.Initialize(context.Background(), strategy, Credentials{}); err != nil {
		t.Fatalf("Initialize failed: %v", err)
	}
	if b.SupportsContextAAD() {
		t.Fatal("incapable hardware barrier reports it can bind context")
	}
	if err := b.Seal(); err != nil {
		t.Fatalf("Seal failed: %v", err)
	}
	// A sealed barrier has NO encryptor, so it has no capability to report --
	// not the software one. See TestBarrier_ContextBinding_UnresolvedUntilUnsealed.
	if b.ContextBinding() != ContextBindingUnresolved {
		t.Fatalf("sealed barrier reports binding %v, want %v",
			b.ContextBinding(), ContextBindingUnresolved)
	}
	if b.IsPlaintextMode() {
		t.Fatal("sealed barrier still reports plaintext mode")
	}
	if err := b.Unseal(context.Background(), strategy, Credentials{}); err != nil {
		t.Fatalf("Unseal failed: %v", err)
	}
	if b.ContextBinding() != ContextBindingUnsupported {
		t.Fatalf("re-unsealed barrier reports binding %v, want %v",
			b.ContextBinding(), ContextBindingUnsupported)
	}
}

// TestContextBinding_String keeps the operator-facing names stable; they appear
// in the setup logs an operator reads to decide whether a deployment has the
// binding it thinks it has.
func TestContextBinding_String(t *testing.T) {
	for binding, want := range map[ContextBinding]string{
		ContextBindingUnresolved:  "unresolved",
		ContextBindingBarrier:     "barrier",
		ContextBindingDelegated:   "delegated",
		ContextBindingUnsupported: "unsupported",
		ContextBindingPlaintext:   "plaintext",
		ContextBinding(200):       "unknown",
	} {
		if got := binding.String(); got != want {
			t.Errorf("ContextBinding(%d).String() = %q, want %q", binding, got, want)
		}
	}
}

// --- (c) RequireContextAAD fails closed at setup, not at first write ---

// TestBarrier_RequireContextAAD_Initialize covers every mode at Initialize. The
// whole value of the flag is that the refusal happens HERE, before a single
// unbound byte is written.
func TestBarrier_RequireContextAAD_Initialize(t *testing.T) {
	requiring := func() BarrierConfig {
		cfg := testBarrierConfig(1)
		cfg.RequireContextAAD = true
		return cfg
	}

	t.Run("software satisfies", func(t *testing.T) {
		b := MustNewBarrier(requiring())
		s := testStrategy(t)
		defer func() { _ = s.Close() }()
		if err := b.Initialize(context.Background(), s, Credentials{}); err != nil {
			t.Fatalf("Initialize (software) failed: %v", err)
		}
		if !b.SupportsContextAAD() {
			t.Fatal("software barrier accepted RequireContextAAD but cannot bind context")
		}
	})

	t.Run("capable hardware satisfies", func(t *testing.T) {
		b, err := hwBarrier(t, requiring(), newMockAADEncryptor(t))
		if err != nil {
			t.Fatalf("Initialize (capable hw) failed: %v", err)
		}
		if !b.SupportsContextAAD() {
			t.Fatal("capable hardware barrier accepted RequireContextAAD but cannot bind context")
		}
	})

	t.Run("incapable hardware refused", func(t *testing.T) {
		b, err := hwBarrier(t, requiring(), &mockHWEncryptor{prefix: []byte("HW:")})
		if !errors.Is(err, ErrContextAADUnsupported) {
			t.Fatalf("Initialize (incapable hw): got %v want ErrContextAADUnsupported", err)
		}
		if !b.IsSealed() || b.IsInitialized() {
			t.Fatal("refused Initialize left the barrier usable")
		}
	})

	t.Run("no-encryption refused", func(t *testing.T) {
		b := MustNewBarrier(requiring())
		err := b.Initialize(context.Background(), NewNoneStrategy(), Credentials{})
		if !errors.Is(err, ErrContextAADUnsupported) {
			t.Fatalf("Initialize (none): got %v want ErrContextAADUnsupported", err)
		}
		if !b.IsSealed() || b.IsInitialized() {
			t.Fatal("refused Initialize left the barrier usable")
		}
	})
}

// TestBarrier_RequireContextAAD_Unseal covers the restart path: a deployment
// that changes its sealing strategy (or its firmware) between runs must be
// refused at the next unseal, not silently downgraded.
func TestBarrier_RequireContextAAD_Unseal(t *testing.T) {
	t.Run("incapable hardware refused", func(t *testing.T) {
		strategy := &hwStrategy{hwEnc: &mockHWEncryptor{prefix: []byte("HW:")}}
		// Initialize WITHOUT the requirement so the barrier exists, then seal
		// and unseal a barrier that does require it — the restart shape.
		b := MustNewBarrier(testBarrierConfig(1))
		if err := b.Initialize(context.Background(), strategy, Credentials{}); err != nil {
			t.Fatalf("Initialize failed: %v", err)
		}
		sealed := b.SealedRootKeyData()
		if err := b.Seal(); err != nil {
			t.Fatalf("Seal failed: %v", err)
		}

		cfg := testBarrierConfig(1)
		cfg.RequireContextAAD = true
		restarted := MustNewBarrier(cfg)
		restarted.sealed = sealed
		restarted.state = barrierSealed
		if err := restarted.Unseal(context.Background(), strategy, Credentials{}); !errors.Is(err, ErrContextAADUnsupported) {
			t.Fatalf("Unseal (incapable hw): got %v want ErrContextAADUnsupported", err)
		}
		if !restarted.IsSealed() {
			t.Fatal("refused Unseal left the barrier unsealed")
		}
	})

	t.Run("no-encryption refused", func(t *testing.T) {
		none := NewNoneStrategy()
		b := MustNewBarrier(testBarrierConfig(1))
		if err := b.Initialize(context.Background(), none, Credentials{}); err != nil {
			t.Fatalf("Initialize (none) failed: %v", err)
		}
		sealedKey := b.SealedRootKeyData()
		if err := b.Seal(); err != nil {
			t.Fatalf("Seal failed: %v", err)
		}

		cfg := testBarrierConfig(1)
		cfg.RequireContextAAD = true
		restarted := MustNewBarrier(cfg)
		if err := restarted.SetSealedRootKey(sealedKey); err != nil {
			t.Fatalf("SetSealedRootKey failed: %v", err)
		}
		if err := restarted.Unseal(context.Background(), none, Credentials{}); !errors.Is(err, ErrContextAADUnsupported) {
			t.Fatalf("Unseal (none): got %v want ErrContextAADUnsupported", err)
		}
		if !restarted.IsSealed() {
			t.Fatal("refused Unseal left the barrier unsealed")
		}
	})

	t.Run("capable hardware satisfies", func(t *testing.T) {
		strategy := &hwStrategy{hwEnc: newMockAADEncryptor(t)}
		cfg := testBarrierConfig(1)
		cfg.RequireContextAAD = true
		b := MustNewBarrier(cfg)
		if err := b.Initialize(context.Background(), strategy, Credentials{}); err != nil {
			t.Fatalf("Initialize (capable hw) failed: %v", err)
		}
		if err := b.Seal(); err != nil {
			t.Fatalf("Seal failed: %v", err)
		}
		if err := b.Unseal(context.Background(), strategy, Credentials{}); err != nil {
			t.Fatalf("Unseal (capable hw) failed: %v", err)
		}
		if !b.SupportsContextAAD() {
			t.Fatal("capable hardware barrier lost its capability across seal/unseal")
		}
	})

	t.Run("software satisfies", func(t *testing.T) {
		s := testStrategy(t)
		defer func() { _ = s.Close() }()
		cfg := testBarrierConfig(1)
		cfg.RequireContextAAD = true
		b := MustNewBarrier(cfg)
		if err := b.Initialize(context.Background(), s, Credentials{}); err != nil {
			t.Fatalf("Initialize (software) failed: %v", err)
		}
		if err := b.Seal(); err != nil {
			t.Fatalf("Seal failed: %v", err)
		}
		if err := b.Unseal(context.Background(), s, Credentials{}); err != nil {
			t.Fatalf("Unseal (software) failed: %v", err)
		}
	})
}

// TestBarrier_RequireContextAAD_DefaultOff pins backward compatibility: the
// zero-value config keeps constructing the barriers it always did, and only the
// per-call ErrHardwareMode marks the missing capability.
func TestBarrier_RequireContextAAD_DefaultOff(t *testing.T) {
	if testBarrierConfig(1).RequireContextAAD {
		t.Fatal("RequireContextAAD defaults to true; existing deployments would break")
	}
	if _, err := hwBarrier(t, testBarrierConfig(1), &mockHWEncryptor{prefix: []byte("HW:")}); err != nil {
		t.Fatalf("Initialize with RequireContextAAD unset failed: %v", err)
	}
	none := MustNewBarrier(testBarrierConfig(1))
	if err := none.Initialize(context.Background(), NewNoneStrategy(), Credentials{}); err != nil {
		t.Fatalf("Initialize (none) with RequireContextAAD unset failed: %v", err)
	}
}
