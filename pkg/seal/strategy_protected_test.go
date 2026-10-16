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
	"crypto/rand"
	"errors"
	"testing"

	"github.com/jeremyhahn/go-enclave"
)

// protectedRootKey returns a 32-byte root key already in locked memory, plus
// the plaintext copy the test compares against. The comparison copy is the
// TEST's own material, deliberately outside the enclave: a yardstick read back
// out of the thing under test would move with it.
func protectedRootKey(t *testing.T) (enclave.ByteAccess, []byte) {
	t.Helper()
	want := make([]byte, rootKeySize)
	if _, err := rand.Read(want); err != nil {
		t.Fatalf("rand.Read: %v", err)
	}
	// Frozen by construction: NewMemory freezes as the material moves in,
	// which is the shape a CALLER of the protected pair would use for a key it
	// already has. (A key it is about to produce belongs in a sized enclave that
	// is filled and then Frozen; both end read-only.) want is copied first
	// because this constructor wipes its source.
	enc := mustFromBytes(t, append([]byte(nil), want...))
	destroyOnCleanup(t, enc)
	return enc, want
}

// assertEnclaveHolds compares an enclave's contents INSIDE the borrow, so the
// assertion never makes its own unprotected copy of the key it is checking.
func assertEnclaveHolds(t *testing.T, enc enclave.ByteAccess, want []byte) {
	t.Helper()
	if err := enc.WithBytes(func(got []byte) error {
		if !bytes.Equal(got, want) {
			return errors.New("enclave contents do not match the expected root key")
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func TestSoftwareStrategy_ProtectedSealUnsealRoundTrip(t *testing.T) {
	s, err := newTestStrategy([]byte("protected-round-trip-passphrase"))
	if err != nil {
		t.Fatalf("newTestStrategy: %v", err)
	}
	defer s.Close()

	rootKey, want := protectedRootKey(t)

	sealed, err := s.SealRootKeyProtected(context.Background(), rootKey, Credentials{})
	if err != nil {
		t.Fatalf("SealRootKeyProtected: %v", err)
	}
	if sealed.Strategy != StrategyPassphrase {
		t.Fatalf("expected strategy=%q, got %q", StrategyPassphrase, sealed.Strategy)
	}

	// Sealing BORROWS the caller's key; it does not consume it. A strategy that
	// destroyed the enclave would break a caller that seals under two strategies
	// in turn, which is exactly what a rekey does.
	assertEnclaveHolds(t, rootKey, want)

	recovered, err := s.UnsealRootKeyProtected(context.Background(), sealed, Credentials{})
	if err != nil {
		t.Fatalf("UnsealRootKeyProtected: %v", err)
	}
	defer recovered.Destroy()

	if recovered.Size() != rootKeySize {
		t.Fatalf("expected a %d-byte key, got %d", rootKeySize, recovered.Size())
	}
	assertEnclaveHolds(t, recovered, want)
}

// The protected pair and the string-typed pair must produce and consume the
// same blob. If they did not, adopting the protected path would silently orphan
// every sealed root key already on disk, and a consumer could not migrate one
// call site at a time.
func TestSoftwareStrategy_ProtectedAndStringPathsInteroperate(t *testing.T) {
	ctx := context.Background()

	t.Run("protected seal opens with the string unseal", func(t *testing.T) {
		s, err := newTestStrategy([]byte("interop-passphrase-one"))
		if err != nil {
			t.Fatalf("newTestStrategy: %v", err)
		}
		defer s.Close()

		rootKey, want := protectedRootKey(t)
		sealed, err := s.SealRootKeyProtected(ctx, rootKey, Credentials{})
		if err != nil {
			t.Fatalf("SealRootKeyProtected: %v", err)
		}

		got, err := s.UnsealRootKey(ctx, sealed, Credentials{})
		if err != nil {
			t.Fatalf("UnsealRootKey: %v", err)
		}
		defer enclave.Wipe(got)
		if !bytes.Equal(got, want) {
			t.Fatal("string unseal did not recover the key the protected seal wrapped")
		}
	})

	t.Run("string seal opens with the protected unseal", func(t *testing.T) {
		s, err := newTestStrategy([]byte("interop-passphrase-two"))
		if err != nil {
			t.Fatalf("newTestStrategy: %v", err)
		}
		defer s.Close()

		want := make([]byte, rootKeySize)
		if _, err := rand.Read(want); err != nil {
			t.Fatalf("rand.Read: %v", err)
		}
		key := make([]byte, rootKeySize)
		copy(key, want)

		sealed, err := s.SealRootKey(ctx, key, Credentials{})
		if err != nil {
			t.Fatalf("SealRootKey: %v", err)
		}
		enclave.Wipe(key)

		recovered, err := s.UnsealRootKeyProtected(ctx, sealed, Credentials{})
		if err != nil {
			t.Fatalf("UnsealRootKeyProtected: %v", err)
		}
		defer recovered.Destroy()
		assertEnclaveHolds(t, recovered, want)
	})
}

// Destroy is the caller's obligation and the only thing that releases the
// locked page, so it has to actually wipe rather than drop a reference.
func TestSoftwareStrategy_ProtectedUnsealKeyIsDestroyable(t *testing.T) {
	s, err := newTestStrategy([]byte("destroyable-passphrase"))
	if err != nil {
		t.Fatalf("newTestStrategy: %v", err)
	}
	defer s.Close()

	rootKey, _ := protectedRootKey(t)
	sealed, err := s.SealRootKeyProtected(context.Background(), rootKey, Credentials{})
	if err != nil {
		t.Fatalf("SealRootKeyProtected: %v", err)
	}
	recovered, err := s.UnsealRootKeyProtected(context.Background(), sealed, Credentials{})
	if err != nil {
		t.Fatalf("UnsealRootKeyProtected: %v", err)
	}

	recovered.Destroy()

	if encCopy(recovered) != nil {
		t.Fatal("a destroyed root-key enclave must report no bytes")
	}
	if err := recovered.WithBytes(func([]byte) error { return nil }); !errors.Is(err, enclave.ErrEnclaveDestroyed) {
		t.Fatalf("WithBytes after Destroy = %v, want ErrEnclaveDestroyed", err)
	}
}

// The plaintext length is a function of a blob nobody has authenticated yet, and
// the enclave allocation is sized from it, so an oversized blob is a remote way
// to drain a process-wide resource unless the length is refused BEFORE anything
// is locked.
//
// The ORDERING is what this test pins, not merely the refusal: the payload here
// is zeroes, so a check placed after the decrypt would report
// ErrDecryptionFailed. Reading ErrInvalidSealedKey instead is the evidence that
// the length was judged first — and the allocation sits between the two.
func TestSoftwareStrategy_ProtectedUnsealRefusesAnUnauthenticatedLength(t *testing.T) {
	ctx := context.Background()
	s, err := newTestStrategy([]byte("oversized-blob-passphrase"))
	if err != nil {
		t.Fatalf("newTestStrategy: %v", err)
	}
	defer s.Close()

	frame := kdfSaltLen + wrappingGCMNonceSize + wrappingGCMTagSize
	for _, tc := range []struct {
		name         string
		plaintextLen int
	}{
		{"one byte over", rootKeySize + 1},
		{"one byte under", rootKeySize - 1},
		{"a megabyte of locked memory", 1 << 20},
	} {
		t.Run(tc.name, func(t *testing.T) {
			blob := &SealedRootKey{
				Strategy:   StrategyPassphrase,
				Metadata:   []byte{byte(testKDFVersion)},
				Ciphertext: make([]byte, frame+tc.plaintextLen),
			}
			got, err := s.UnsealRootKeyProtected(ctx, blob, Credentials{})
			if !errors.Is(err, ErrInvalidSealedKey) {
				t.Fatalf("UnsealRootKeyProtected = %v, want ErrInvalidSealedKey", err)
			}
			if got != nil {
				got.Destroy()
				t.Fatal("a refused blob must not hand back an enclave")
			}
		})
	}
}

func TestSoftwareStrategy_SealRootKeyProtectedErrors(t *testing.T) {
	ctx := context.Background()

	destroyed := mustSized(t, rootKeySize)
	destroyed.Destroy()

	wrongSize := mustSized(t, rootKeySize-1)
	destroyOnCleanup(t, wrongSize)

	tests := []struct {
		name    string
		rootKey enclave.ByteAccess
		want    error
	}{
		{"nil enclave", nil, ErrInvalidRootKeySize},
		{"destroyed enclave", destroyed, ErrInvalidRootKeySize},
		{"wrong size", wrongSize, ErrInvalidRootKeySize},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			s, err := newTestStrategy([]byte("seal-error-passphrase"))
			if err != nil {
				t.Fatalf("newTestStrategy: %v", err)
			}
			defer s.Close()

			if _, err := s.SealRootKeyProtected(ctx, tc.rootKey, Credentials{}); !errors.Is(err, tc.want) {
				t.Fatalf("SealRootKeyProtected = %v, want %v", err, tc.want)
			}
		})
	}

	t.Run("closed strategy", func(t *testing.T) {
		s, err := newTestStrategy([]byte("seal-error-passphrase"))
		if err != nil {
			t.Fatalf("newTestStrategy: %v", err)
		}
		if err := s.Close(); err != nil {
			t.Fatalf("Close: %v", err)
		}
		rootKey, _ := protectedRootKey(t)
		// Reported BEFORE the size check, matching the string-typed path: a
		// closed strategy cannot seal anything, whatever it is handed.
		if _, err := s.SealRootKeyProtected(ctx, rootKey, Credentials{}); !errors.Is(err, ErrStrategyClosed) {
			t.Fatalf("SealRootKeyProtected on a closed strategy = %v, want ErrStrategyClosed", err)
		}
	})
}

func TestSoftwareStrategy_UnsealRootKeyProtectedErrors(t *testing.T) {
	ctx := context.Background()

	s, err := newTestStrategy([]byte("unseal-error-passphrase"))
	if err != nil {
		t.Fatalf("newTestStrategy: %v", err)
	}
	defer s.Close()

	rootKey, _ := protectedRootKey(t)
	good, err := s.SealRootKeyProtected(ctx, rootKey, Credentials{})
	if err != nil {
		t.Fatalf("SealRootKeyProtected: %v", err)
	}

	tampered := &SealedRootKey{Strategy: good.Strategy, Metadata: good.Metadata}
	tampered.Ciphertext = append([]byte(nil), good.Ciphertext...)
	tampered.Ciphertext[len(tampered.Ciphertext)-1] ^= 0xFF

	tests := []struct {
		name   string
		sealed *SealedRootKey
		want   error
	}{
		{"nil sealed key", nil, ErrBarrierNotInit},
		{"strategy mismatch", &SealedRootKey{Strategy: StrategyNone, Ciphertext: good.Ciphertext}, ErrStrategyMismatch},
		{"ciphertext too short", &SealedRootKey{Strategy: StrategyPassphrase, Ciphertext: make([]byte, 10)}, ErrCiphertextTooShort},
		{"authentication failure", tampered, ErrDecryptionFailed},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := s.UnsealRootKeyProtected(ctx, tc.sealed, Credentials{})
			if !errors.Is(err, tc.want) {
				t.Fatalf("UnsealRootKeyProtected = %v, want %v", err, tc.want)
			}
			if got != nil {
				got.Destroy()
				t.Fatal("a failed unseal must not hand back an enclave")
			}
		})
	}

	t.Run("wrong passphrase", func(t *testing.T) {
		other, err := newTestStrategy([]byte("a-different-passphrase"))
		if err != nil {
			t.Fatalf("newTestStrategy: %v", err)
		}
		defer other.Close()

		got, err := other.UnsealRootKeyProtected(ctx, good, Credentials{})
		if !errors.Is(err, ErrDecryptionFailed) {
			t.Fatalf("UnsealRootKeyProtected with the wrong passphrase = %v, want ErrDecryptionFailed", err)
		}
		if got != nil {
			got.Destroy()
			t.Fatal("a failed unseal must not hand back an enclave")
		}
	})

	t.Run("closed strategy", func(t *testing.T) {
		closed, err := newTestStrategy([]byte("closing-passphrase"))
		if err != nil {
			t.Fatalf("newTestStrategy: %v", err)
		}
		if err := closed.Close(); err != nil {
			t.Fatalf("Close: %v", err)
		}
		if _, err := closed.UnsealRootKeyProtected(ctx, good, Credentials{}); !errors.Is(err, ErrStrategyClosed) {
			t.Fatalf("UnsealRootKeyProtected on a closed strategy = %v, want ErrStrategyClosed", err)
		}
	})
}

// The deferred strategy takes its passphrase at call time, and both protected
// methods must route through the same temporary inner strategy the string-typed
// pair uses — including refusing empty credentials rather than deriving from
// nothing.
func TestDeferredSoftwareStrategy_ProtectedPathUsesCallTimeCredentials(t *testing.T) {
	ctx := context.Background()
	s := NewDeferredSoftwareStrategy()
	defer s.Close()
	// The cheap KDF the rest of this package's tests use; the deferred
	// constructor takes production work factors, which would make this test a
	// multi-second one for no added coverage.
	s.kdfVersion = testKDFVersion
	s.kdfParams = testKDFParams()

	creds, err := NewCredentials(context.Background(), []byte("a-deferred-protected-passphrase"))
	if err != nil {
		t.Fatalf("NewCredentials: %v", err)
	}
	defer creds.Secret.Destroy()

	rootKey, want := protectedRootKey(t)

	sealed, err := s.SealRootKeyProtected(ctx, rootKey, creds)
	if err != nil {
		t.Fatalf("SealRootKeyProtected: %v", err)
	}

	recovered, err := s.UnsealRootKeyProtected(ctx, sealed, creds)
	if err != nil {
		t.Fatalf("UnsealRootKeyProtected: %v", err)
	}
	defer recovered.Destroy()
	assertEnclaveHolds(t, recovered, want)

	// The credentials are the CALLER's: many calls, one enclave.Sealed, released
	// by the caller's own Clear and never by the strategy.
	again, err := s.UnsealRootKeyProtected(ctx, sealed, creds)
	if err != nil {
		t.Fatalf("second UnsealRootKeyProtected: %v", err)
	}
	defer again.Destroy()
	assertEnclaveHolds(t, again, want)

	if _, err := s.SealRootKeyProtected(ctx, rootKey, Credentials{}); !errors.Is(err, ErrEmptyCredentials) {
		t.Fatalf("SealRootKeyProtected with no credentials = %v, want ErrEmptyCredentials", err)
	}
	if _, err := s.UnsealRootKeyProtected(ctx, sealed, Credentials{}); !errors.Is(err, ErrEmptyCredentials) {
		t.Fatalf("UnsealRootKeyProtected with no credentials = %v, want ErrEmptyCredentials", err)
	}

	wrong, err := NewCredentials(context.Background(), []byte("the-wrong-passphrase-entirely"))
	if err != nil {
		t.Fatalf("NewCredentials: %v", err)
	}
	defer wrong.Secret.Destroy()
	if _, err := s.UnsealRootKeyProtected(ctx, sealed, wrong); !errors.Is(err, ErrDecryptionFailed) {
		t.Fatalf("UnsealRootKeyProtected with the wrong credentials = %v, want ErrDecryptionFailed", err)
	}
}

// NoneStrategy seals nothing and therefore has nothing to hand back protected.
// The asymmetry with its string-typed sibling — which returns an empty slice —
// is deliberate and is the interesting half of this type's conformance.
func TestNoneStrategy_ProtectedPair(t *testing.T) {
	ctx := context.Background()
	s := NewNoneStrategy()
	rootKey, want := protectedRootKey(t)

	sealed, err := s.SealRootKeyProtected(ctx, rootKey, Credentials{})
	if err != nil {
		t.Fatalf("SealRootKeyProtected: %v", err)
	}
	if sealed.Strategy != StrategyNone {
		t.Fatalf("expected strategy=%q, got %q", StrategyNone, sealed.Strategy)
	}
	if len(sealed.Ciphertext) != 0 {
		t.Fatal("no-encryption mode must not produce ciphertext")
	}
	// The caller's key is untouched: None reads nothing and owns nothing.
	assertEnclaveHolds(t, rootKey, want)

	got, err := s.UnsealRootKeyProtected(ctx, sealed, Credentials{})
	if !errors.Is(err, ErrNoRootKeyAvailable) {
		t.Fatalf("UnsealRootKeyProtected = %v, want ErrNoRootKeyAvailable", err)
	}
	if got != nil {
		got.Destroy()
		t.Fatal("no-encryption mode must not hand back an enclave")
	}

	// The enclave is validated even though its contents are never read: a
	// caller that destroyed or lost its root key before sealing must learn so
	// here, not at an unseal it can no longer perform.
	t.Run("refuses a root key it cannot vouch for", func(t *testing.T) {
		destroyed := mustSized(t, rootKeySize)
		destroyed.Destroy()
		wrongSize := mustSized(t, rootKeySize-1)
		destroyOnCleanup(t, wrongSize)

		for name, enc := range map[string]enclave.ByteAccess{
			"nil":        nil,
			"destroyed":  destroyed,
			"undersized": wrongSize,
		} {
			got, err := s.SealRootKeyProtected(ctx, enc, Credentials{})
			if !errors.Is(err, ErrInvalidRootKeySize) {
				t.Fatalf("SealRootKeyProtected(%s) = %v, want ErrInvalidRootKeySize", name, err)
			}
			if got != nil {
				t.Fatalf("SealRootKeyProtected(%s) must not hand back a marker", name)
			}
		}
	})

	// The blob is still validated first, so a caller learns that its sealed
	// state is wrong rather than that no key exists.
	if _, err := s.UnsealRootKeyProtected(ctx, nil, Credentials{}); !errors.Is(err, ErrInvalidSealedKey) {
		t.Fatalf("UnsealRootKeyProtected(nil) = %v, want ErrInvalidSealedKey", err)
	}
	mismatched := &SealedRootKey{Strategy: StrategyPassphrase}
	if _, err := s.UnsealRootKeyProtected(ctx, mismatched, Credentials{}); !errors.Is(err, ErrStrategyMismatch) {
		t.Fatalf("UnsealRootKeyProtected(mismatched) = %v, want ErrStrategyMismatch", err)
	}
}
