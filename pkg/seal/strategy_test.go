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

func TestNewSoftwareStrategy_EmptyPassphrase(t *testing.T) {
	for _, tc := range []struct {
		name string
		ctor func() (*SoftwareStrategy, error)
	}{
		{"NewSoftwareStrategy empty", func() (*SoftwareStrategy, error) {
			return NewSoftwareStrategy(context.Background(), []byte{})
		}},
		{"NewSoftwareStrategyFIPS nil", func() (*SoftwareStrategy, error) {
			return NewSoftwareStrategyFIPS(context.Background(), nil, 1000)
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := tc.ctor(); !errors.Is(err, ErrInvalidPassphrase) {
				t.Fatalf("expected ErrInvalidPassphrase, got %v", err)
			}
		})
	}
}

func TestSoftwareStrategy_PassphraseIsCopied(t *testing.T) {
	original := []byte("my-secret-passphrase")
	passphrase := make([]byte, len(original))
	copy(passphrase, original)

	s, err := NewSoftwareStrategy(context.Background(), passphrase)
	if err != nil {
		t.Fatalf("NewSoftwareStrategy returned unexpected error: %v", err)
	}

	// Zero the original slice.
	enclave.Wipe(passphrase)

	// Strategy should still work since it made a copy.
	rootKey := make([]byte, 32)
	if _, err := rand.Read(rootKey); err != nil {
		t.Fatalf("rand.Read failed: %v", err)
	}

	sealed, err := s.SealRootKey(context.Background(), rootKey, Credentials{})
	if err != nil {
		t.Fatalf("Seal failed: %v", err)
	}

	// Create a fresh strategy with the original passphrase to unseal.
	s2, err := NewSoftwareStrategy(context.Background(), original)
	if err != nil {
		t.Fatalf("NewSoftwareStrategy returned unexpected error: %v", err)
	}

	decrypted, err := s2.UnsealRootKey(context.Background(), sealed, Credentials{})
	if err != nil {
		t.Fatalf("Unseal failed: %v", err)
	}
	if !bytes.Equal(rootKey, decrypted) {
		t.Fatal("round-trip failed: decrypted root key does not match original")
	}

	enclave.Wipe(decrypted)
	s.Close()
	s2.Close()
}

func TestSoftwareStrategy_SealUnsealRoundTrip(t *testing.T) {
	passphrase := []byte("correct-horse-battery-staple")
	// Use low parameters for fast tests.
	s, err := newTestStrategy(passphrase)
	if err != nil {
		t.Fatalf("newTestStrategy returned unexpected error: %v", err)
	}
	defer s.Close()

	rootKey := make([]byte, 32)
	if _, err := rand.Read(rootKey); err != nil {
		t.Fatalf("rand.Read failed: %v", err)
	}

	sealed, err := s.SealRootKey(context.Background(), rootKey, Credentials{})
	if err != nil {
		t.Fatalf("Seal failed: %v", err)
	}

	if sealed.Strategy != StrategyPassphrase {
		t.Fatalf("expected strategy=%q, got %q", passphraseStrategyName, sealed.Strategy)
	}
	// The Salt field is nil because the software strategy stores its
	// KDF salt inside the ciphertext (not in the SealedRootKey.Salt
	// field, which is reserved for the barrier's HKDF salt).
	if sealed.Salt != nil {
		t.Fatalf("expected nil Salt from strategy, got %d bytes", len(sealed.Salt))
	}
	// Metadata must contain the KDF version byte.
	if len(sealed.Metadata) != metadataKDFVersionLen {
		t.Fatalf("expected %d-byte metadata, got %d bytes", metadataKDFVersionLen, len(sealed.Metadata))
	}
	if KDFVersion(sealed.Metadata[0]) != testKDFVersion {
		t.Fatalf("expected KDF version %d in metadata, got %d", testKDFVersion, sealed.Metadata[0])
	}
	// Ciphertext must be at least: kdfSalt(32) + nonce(12) + rootKey(32) + tag(16) = 92
	minExpected := kdfSaltLen + 12 + 32 + 16
	if len(sealed.Ciphertext) < minExpected {
		t.Fatalf("ciphertext too short: got %d, want >= %d", len(sealed.Ciphertext), minExpected)
	}

	decrypted, err := s.UnsealRootKey(context.Background(), sealed, Credentials{})
	if err != nil {
		t.Fatalf("Unseal failed: %v", err)
	}
	defer enclave.Wipe(decrypted)

	if !bytes.Equal(rootKey, decrypted) {
		t.Fatal("round-trip failed: decrypted root key does not match original")
	}
}

func TestSoftwareStrategy_SealInvalidRootKeySize(t *testing.T) {
	s, err := newTestStrategy([]byte("test"))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	defer s.Close()

	// Too short.
	_, err = s.SealRootKey(context.Background(), []byte("short"), Credentials{})
	if !errors.Is(err, ErrInvalidRootKeySize) {
		t.Fatalf("expected ErrInvalidRootKeySize for short key, got %v", err)
	}

	// Too long.
	_, err = s.SealRootKey(context.Background(), make([]byte, 64), Credentials{})
	if !errors.Is(err, ErrInvalidRootKeySize) {
		t.Fatalf("expected ErrInvalidRootKeySize for long key, got %v", err)
	}
}

func TestSoftwareStrategy_UnsealWrongPassphrase(t *testing.T) {
	for _, tc := range []struct {
		name string
		ctor func(passphrase []byte) (*SoftwareStrategy, error)
	}{
		{"test KDF", newTestStrategy},
		{"PBKDF2", func(passphrase []byte) (*SoftwareStrategy, error) {
			return NewSoftwareStrategyFIPS(context.Background(), passphrase, 1000)
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			correct, err := tc.ctor([]byte("correct"))
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			defer correct.Close()

			rootKey := make([]byte, 32)
			if _, err := rand.Read(rootKey); err != nil {
				t.Fatalf("rand.Read failed: %v", err)
			}

			sealed, err := correct.SealRootKey(context.Background(), rootKey, Credentials{})
			if err != nil {
				t.Fatalf("Seal failed: %v", err)
			}

			wrong, err := tc.ctor([]byte("wrong"))
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			defer wrong.Close()

			_, err = wrong.UnsealRootKey(context.Background(), sealed, Credentials{})
			if !errors.Is(err, ErrDecryptionFailed) {
				t.Fatalf("expected ErrDecryptionFailed for wrong passphrase, got %v", err)
			}
			var unsealErr *UnsealError
			if !errors.As(err, &unsealErr) {
				t.Fatalf("expected *UnsealError, got %T: %v", err, err)
			}
		})
	}
}

func TestSoftwareStrategy_UnsealRefusesMalformedSealedKey(t *testing.T) {
	s, err := newTestStrategy([]byte("test"))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	defer s.Close()

	rootKey := make([]byte, 32)
	if _, err := rand.Read(rootKey); err != nil {
		t.Fatalf("rand.Read failed: %v", err)
	}
	corrupt, err := s.SealRootKey(context.Background(), rootKey, Credentials{})
	if err != nil {
		t.Fatalf("Seal failed: %v", err)
	}
	corrupt.Ciphertext[len(corrupt.Ciphertext)-1] ^= 0xFF

	for _, tc := range []struct {
		name   string
		sealed *SealedRootKey
		want   error
	}{
		{"nil sealed key", nil, ErrBarrierNotInit},
		{"truncated ciphertext", &SealedRootKey{
			Strategy:   passphraseStrategyName,
			Salt:       make([]byte, kdfSaltLen),
			Ciphertext: []byte{0x01}, // Too short to contain a nonce.
		}, ErrCiphertextTooShort},
		{"corrupt ciphertext", corrupt, ErrDecryptionFailed},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := s.UnsealRootKey(context.Background(), tc.sealed, Credentials{})
			if !errors.Is(err, tc.want) {
				t.Fatalf("UnsealRootKey = %v, want %v", err, tc.want)
			}
			if got != nil {
				t.Fatal("a failed unseal returned key material")
			}
		})
	}
}

func TestSoftwareStrategy_CloseIdempotent(t *testing.T) {
	s, err := newTestStrategy([]byte("test"))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if err := s.Close(); err != nil {
		t.Fatalf("first Close returned error: %v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("second Close returned error: %v", err)
	}
}

func TestSoftwareStrategy_CloseNilsEnclave(t *testing.T) {
	s, err := newTestStrategy([]byte("secret-passphrase"))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Passphrase should be accessible before Close.
	if s.passphrase.Load() == nil {
		t.Fatal("passphrase enclave should not be nil before Close")
	}

	s.Close()

	// Passphrase enclave reference should be nil after Close.
	if s.passphrase.Load() != nil {
		t.Fatal("passphrase enclave should be nil after Close")
	}
}

func TestSoftwareStrategy_PassphraseInEnclave(t *testing.T) {
	original := []byte("enclave-test-passphrase")
	s, err := newTestStrategy(original)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	defer s.Close()

	// The passphrase should be stored in a sealed enclave.
	buf, openErr := (*s.passphrase.Load()).Open(context.Background())
	if openErr != nil {
		t.Fatalf("passphrase.Open() failed: %v", openErr)
	}
	defer buf.Destroy()

	got := encCopy(buf)
	if string(got) != "enclave-test-passphrase" {
		t.Fatalf("passphrase mismatch: got %q", got)
	}
}

func TestSoftwareStrategy_MultipleSealsDifferentCiphertexts(t *testing.T) {
	s, err := newTestStrategy([]byte("test"))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	defer s.Close()

	rootKey := make([]byte, 32)
	if _, err := rand.Read(rootKey); err != nil {
		t.Fatalf("rand.Read failed: %v", err)
	}

	sealed1, err := s.SealRootKey(context.Background(), rootKey, Credentials{})
	if err != nil {
		t.Fatalf("first Seal failed: %v", err)
	}

	sealed2, err := s.SealRootKey(context.Background(), rootKey, Credentials{})
	if err != nil {
		t.Fatalf("second Seal failed: %v", err)
	}

	// Different salts should produce different ciphertexts.
	if bytes.Equal(sealed1.Ciphertext, sealed2.Ciphertext) {
		t.Fatal("two seals of the same key produced identical ciphertexts")
	}

	// Both should unseal to the same root key.
	dec1, err := s.UnsealRootKey(context.Background(), sealed1, Credentials{})
	if err != nil {
		t.Fatalf("Unseal sealed1 failed: %v", err)
	}
	defer enclave.Wipe(dec1)

	dec2, err := s.UnsealRootKey(context.Background(), sealed2, Credentials{})
	if err != nil {
		t.Fatalf("Unseal sealed2 failed: %v", err)
	}
	defer enclave.Wipe(dec2)

	if !bytes.Equal(dec1, dec2) {
		t.Fatal("two unseals produced different root keys")
	}
}

// --- SoftwareStrategy rand failure paths ---

// strategyErrReader is a test io.Reader that fails after n bytes.
type strategyErrReader struct {
	remaining int
}

func (r *strategyErrReader) Read(p []byte) (int, error) {
	if r.remaining <= 0 {
		return 0, errors.New("injected rand failure")
	}
	n := len(p)
	if n > r.remaining {
		n = r.remaining
	}
	for i := 0; i < n; i++ {
		p[i] = 0xBB
	}
	r.remaining -= n
	return n, nil
}

func TestSoftwareStrategy_SealRandSaltFails(t *testing.T) {
	s, err := newTestStrategy([]byte("test"))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	defer s.Close()

	// Inject a failing reader: fail immediately so argon2 salt generation fails.
	s.randReader = &strategyErrReader{remaining: 0}

	rootKey := make([]byte, 32)
	_, err = s.SealRootKey(context.Background(), rootKey, Credentials{})
	if err == nil {
		t.Fatal("expected error from Seal with failing rand for salt")
	}
	var sealErr *Error
	if !errors.As(err, &sealErr) {
		t.Fatalf("expected *Error, got %T: %v", err, err)
	}
}

// TestSoftwareStrategy_SealDoesNotDrawTheNonceFromRandReader pins that the
// wrapping AEAD generates its own nonce.
//
// Drawing the AES-GCM nonce from s.randReader and handing it to Seal would be a
// caller-supplied IV, which SP 800-38D forbids. The wrapping AEAD is built by
// newAESGCM (cipher.NewGCMWithRandomNonce) and generates the nonce itself, so s.randReader supplies the KDF salt and nothing else. A reader that
// yields exactly the salt and then fails must therefore produce a COMPLETE
// sealed blob; if it does not, a caller-supplied IV has come back.
func TestSoftwareStrategy_SealDoesNotDrawTheNonceFromRandReader(t *testing.T) {
	s, err := newTestStrategy([]byte("test-passphrase"))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	defer s.Close()

	// Yields the salt (32 bytes) and fails on any further read.
	s.randReader = &strategyErrReader{remaining: kdfSaltLen}

	rootKey := make([]byte, 32)
	sealed, err := s.SealRootKey(context.Background(), rootKey, Credentials{})
	if err != nil {
		t.Fatalf("SealRootKey consumed randomness beyond the KDF salt; the "+
			"wrapping nonce must be generated inside the module: %v", err)
	}
	wantLen := kdfSaltLen + wrappingGCMNonceSize + len(rootKey) + wrappingGCMTagSize
	if len(sealed.Ciphertext) != wantLen {
		t.Fatalf("sealed ciphertext is %d bytes, want %d "+
			"([KDFSalt:32 | Nonce:12 | EncryptedRootKey:32 | Tag:16])",
			len(sealed.Ciphertext), wantLen)
	}

	got, err := s.UnsealRootKey(context.Background(), sealed, Credentials{})
	if err != nil {
		t.Fatalf("UnsealRootKey: %v", err)
	}
	if !bytes.Equal(got, rootKey) {
		t.Fatalf("root key roundtrip mismatch: got %x, want %x", got, rootKey)
	}
}

// --- FIPS KDF integration tests ---

func TestSoftwareStrategyFIPS_SealUnsealRoundTrip(t *testing.T) {
	passphrase := []byte("fips-round-trip-passphrase")
	s, err := NewSoftwareStrategyFIPS(context.Background(), passphrase, 1000)
	if err != nil {
		t.Fatalf("NewSoftwareStrategyFIPS returned unexpected error: %v", err)
	}
	defer s.Close()

	rootKey := make([]byte, 32)
	if _, err := rand.Read(rootKey); err != nil {
		t.Fatalf("rand.Read failed: %v", err)
	}

	sealed, err := s.SealRootKey(context.Background(), rootKey, Credentials{})
	if err != nil {
		t.Fatalf("Seal failed: %v", err)
	}

	// Verify metadata stores PBKDF2 version.
	if len(sealed.Metadata) != metadataKDFVersionLen {
		t.Fatalf("expected %d-byte metadata, got %d bytes", metadataKDFVersionLen, len(sealed.Metadata))
	}
	if KDFVersion(sealed.Metadata[0]) != KDFPbkdf2SHA256 {
		t.Fatalf("expected KDFPbkdf2SHA256 in metadata, got %d", sealed.Metadata[0])
	}

	decrypted, err := s.UnsealRootKey(context.Background(), sealed, Credentials{})
	if err != nil {
		t.Fatalf("Unseal failed: %v", err)
	}
	defer enclave.Wipe(decrypted)

	if !bytes.Equal(rootKey, decrypted) {
		t.Fatal("FIPS round-trip failed: decrypted root key does not match original")
	}
}

func TestSoftwareStrategy_UnsealRefusesMissingOrUnknownKDFMetadata(t *testing.T) {
	passphrase := []byte("kdf-metadata-required")
	s, err := newTestStrategy(passphrase)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	defer s.Close()

	rootKey := make([]byte, 32)
	if _, err := rand.Read(rootKey); err != nil {
		t.Fatalf("rand.Read failed: %v", err)
	}

	cases := []struct {
		name     string
		metadata []byte
	}{
		{"nil", nil},
		{"empty", []byte{}},
		{"unknown version 0x00", []byte{0x00}},
		{"unknown version 0x03", []byte{0x03}},
		{"unknown version 0xFF", []byte{0xFF}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			// Seal normally (which writes metadata), then replace it.
			sealed, err := s.SealRootKey(context.Background(), rootKey, Credentials{})
			if err != nil {
				t.Fatalf("Seal failed: %v", err)
			}
			sealed.Metadata = tc.metadata

			decrypted, err := s.UnsealRootKey(context.Background(), sealed, Credentials{})
			if err == nil {
				enclave.Wipe(decrypted)
				t.Fatal("UnsealRootKey accepted a sealed root key without a known KDF version")
			}
			if !errors.Is(err, ErrKDFUnsupportedVersion) {
				t.Fatalf("UnsealRootKey: expected ErrKDFUnsupportedVersion, got %v", err)
			}
			var ue *UnsealError
			if !errors.As(err, &ue) {
				t.Fatalf("UnsealRootKey: expected *UnsealError, got %T", err)
			}

			protected, err := s.UnsealRootKeyProtected(context.Background(), sealed, Credentials{})
			if err == nil {
				protected.Destroy()
				t.Fatal("UnsealRootKeyProtected accepted a sealed root key without a known KDF version")
			}
			if !errors.Is(err, ErrKDFUnsupportedVersion) {
				t.Fatalf("UnsealRootKeyProtected: expected ErrKDFUnsupportedVersion, got %v", err)
			}
		})
	}
}

func TestSoftwareStrategy_IDAvailableHardwareBacked(t *testing.T) {
	s, err := newTestStrategy([]byte("test"))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	defer s.Close()

	if s.ID() != StrategyPassphrase {
		t.Fatalf("expected ID=%q, got %q", StrategyPassphrase, s.ID())
	}
	if !s.Available() {
		t.Fatal("expected Available() to return true")
	}
	if s.HardwareBacked() {
		t.Fatal("expected HardwareBacked() to return false")
	}
}

// --- BarrierEncryptor tests ---

func TestSoftwareStrategy_BarrierEncryptor_ReturnsNilNil(t *testing.T) {
	s, err := newTestStrategy([]byte("test"))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	defer s.Close()

	// The software strategy returns (nil, nil) whatever the root key,
	// including a nil one.
	for _, rootKey := range [][]byte{make([]byte, 32), nil} {
		enc, err := s.BarrierEncryptor(context.Background(), rootKey)
		if enc != nil {
			t.Fatalf("rootKey len %d: expected nil SymmetricEncrypter, got %v", len(rootKey), enc)
		}
		if err != nil {
			t.Fatalf("rootKey len %d: expected nil error, got %v", len(rootKey), err)
		}
	}
}

func TestSoftwareStrategy_UnsealKDFVersionAndParams_FIPSToArgon2Defaults(t *testing.T) {
	t.Parallel()

	// Create a FIPS strategy but present Argon2id metadata.
	s, err := NewSoftwareStrategyFIPS(context.Background(), []byte("test"), 1000)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	defer s.Close()

	// Metadata says KDFArgon2id but strategy is KDFPbkdf2SHA256 (version mismatch).
	metadata := []byte{byte(KDFArgon2id)}
	version, params, err := s.unsealKDFVersionAndParams(metadata)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if version != KDFArgon2id {
		t.Fatalf("expected KDFArgon2id, got %d", version)
	}
	// Should use default Argon2id params.
	defaultArgon := DefaultKDFParams()
	if params.Time != defaultArgon.Time {
		t.Fatalf("expected default Argon2id Time=%d, got %d", defaultArgon.Time, params.Time)
	}
	if params.Memory != defaultArgon.Memory {
		t.Fatalf("expected default Argon2id Memory=%d, got %d", defaultArgon.Memory, params.Memory)
	}
}

// --- newSoftwareStrategy with invalid KDF version ---

func TestNewSoftwareStrategy_InvalidKDFVersion(t *testing.T) {
	t.Parallel()

	// Call the internal constructor with an invalid KDF version.
	_, err := newSoftwareStrategy(context.Background(), []byte("test"), KDFVersion(0xFF), KDFParams{KeyLen: 32, SaltLen: 32})
	if !errors.Is(err, ErrKDFUnsupportedVersion) {
		t.Fatalf("expected ErrKDFUnsupportedVersion, got %v", err)
	}
}
