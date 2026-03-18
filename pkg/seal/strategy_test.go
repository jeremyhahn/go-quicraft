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
)

func TestNewSoftwareStrategy_ValidPassphrase(t *testing.T) {
	s, err := NewSoftwareStrategy([]byte("test-passphrase"))
	if err != nil {
		t.Fatalf("NewSoftwareStrategy returned unexpected error: %v", err)
	}
	if s == nil {
		t.Fatal("NewSoftwareStrategy returned nil strategy")
	}
	if err := s.Close(); err != nil {
		t.Fatalf("Close returned unexpected error: %v", err)
	}
}

func TestNewSoftwareStrategy_EmptyPassphrase(t *testing.T) {
	_, err := NewSoftwareStrategy([]byte{})
	if !errors.Is(err, ErrInvalidPassphrase) {
		t.Fatalf("expected ErrInvalidPassphrase, got %v", err)
	}
}

func TestNewSoftwareStrategy_NilPassphrase(t *testing.T) {
	_, err := NewSoftwareStrategy(nil)
	if !errors.Is(err, ErrInvalidPassphrase) {
		t.Fatalf("expected ErrInvalidPassphrase, got %v", err)
	}
}

func TestNewSoftwareStrategyWithParams_Valid(t *testing.T) {
	s, err := NewSoftwareStrategyWithParams([]byte("test"), 1, 64*1024, 2)
	if err != nil {
		t.Fatalf("NewSoftwareStrategyWithParams returned unexpected error: %v", err)
	}
	if s.kdfVersion != KDFArgon2id {
		t.Fatalf("expected KDFArgon2id, got %d", s.kdfVersion)
	}
	if s.kdfParams.Time != 1 || s.kdfParams.Memory != 64*1024 || s.kdfParams.Threads != 2 {
		t.Fatalf("unexpected params: time=%d, memory=%d, threads=%d",
			s.kdfParams.Time, s.kdfParams.Memory, s.kdfParams.Threads)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("Close returned unexpected error: %v", err)
	}
}

func TestNewSoftwareStrategyWithParams_EmptyPassphrase(t *testing.T) {
	_, err := NewSoftwareStrategyWithParams(nil, 1, 64*1024, 2)
	if !errors.Is(err, ErrInvalidPassphrase) {
		t.Fatalf("expected ErrInvalidPassphrase, got %v", err)
	}
}

func TestSoftwareStrategy_PassphraseIsCopied(t *testing.T) {
	original := []byte("my-secret-passphrase")
	passphrase := make([]byte, len(original))
	copy(passphrase, original)

	s, err := NewSoftwareStrategy(passphrase)
	if err != nil {
		t.Fatalf("NewSoftwareStrategy returned unexpected error: %v", err)
	}

	// Zero the original slice.
	wipeBytes(passphrase)

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
	s2, err := NewSoftwareStrategy(original)
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

	wipeBytes(decrypted)
	s.Close()
	s2.Close()
}

func TestSoftwareStrategy_SealUnsealRoundTrip(t *testing.T) {
	passphrase := []byte("correct-horse-battery-staple")
	// Use low parameters for fast tests.
	s, err := NewSoftwareStrategyWithParams(passphrase, 1, 64*1024, 1)
	if err != nil {
		t.Fatalf("NewSoftwareStrategyWithParams returned unexpected error: %v", err)
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

	if sealed.Strategy != StrategySoftware {
		t.Fatalf("expected strategy=%q, got %q", softwareStrategyName, sealed.Strategy)
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
	if KDFVersion(sealed.Metadata[0]) != KDFArgon2id {
		t.Fatalf("expected KDFArgon2id in metadata, got %d", sealed.Metadata[0])
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
	defer wipeBytes(decrypted)

	if !bytes.Equal(rootKey, decrypted) {
		t.Fatal("round-trip failed: decrypted root key does not match original")
	}
}

func TestSoftwareStrategy_SealInvalidRootKeySize(t *testing.T) {
	s, err := NewSoftwareStrategyWithParams([]byte("test"), 1, 64*1024, 1)
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
	correct, err := NewSoftwareStrategyWithParams([]byte("correct"), 1, 64*1024, 1)
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

	wrong, err := NewSoftwareStrategyWithParams([]byte("wrong"), 1, 64*1024, 1)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	defer wrong.Close()

	_, err = wrong.UnsealRootKey(context.Background(), sealed, Credentials{})
	if err == nil {
		t.Fatal("expected error for wrong passphrase, got nil")
	}
	var unsealErr *UnsealError
	if !errors.As(err, &unsealErr) {
		t.Fatalf("expected *UnsealError, got %T: %v", err, err)
	}
}

func TestSoftwareStrategy_UnsealNilSealed(t *testing.T) {
	s, err := NewSoftwareStrategyWithParams([]byte("test"), 1, 64*1024, 1)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	defer s.Close()

	_, err = s.UnsealRootKey(context.Background(), nil, Credentials{})
	if err == nil {
		t.Fatal("expected error for nil sealed, got nil")
	}
}

func TestSoftwareStrategy_UnsealTruncatedCiphertext(t *testing.T) {
	s, err := NewSoftwareStrategyWithParams([]byte("test"), 1, 64*1024, 1)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	defer s.Close()

	sealed := &SealedRootKey{
		Strategy:   softwareStrategyName,
		Salt:       make([]byte, kdfSaltLen),
		Ciphertext: []byte{0x01}, // Too short to contain a nonce.
	}

	_, err = s.UnsealRootKey(context.Background(), sealed, Credentials{})
	if err == nil {
		t.Fatal("expected error for truncated ciphertext, got nil")
	}
}

func TestSoftwareStrategy_UnsealCorruptCiphertext(t *testing.T) {
	s, err := NewSoftwareStrategyWithParams([]byte("test"), 1, 64*1024, 1)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
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

	// Corrupt the ciphertext.
	sealed.Ciphertext[len(sealed.Ciphertext)-1] ^= 0xFF

	_, err = s.UnsealRootKey(context.Background(), sealed, Credentials{})
	if err == nil {
		t.Fatal("expected error for corrupt ciphertext, got nil")
	}
}

func TestSoftwareStrategy_CloseIdempotent(t *testing.T) {
	s, err := NewSoftwareStrategyWithParams([]byte("test"), 1, 64*1024, 1)
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
	s, err := NewSoftwareStrategyWithParams([]byte("secret-passphrase"), 1, 64*1024, 1)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Passphrase should be accessible before Close.
	if s.passphrase == nil {
		t.Fatal("passphrase enclave should not be nil before Close")
	}

	s.Close()

	// Passphrase enclave reference should be nil after Close.
	if s.passphrase != nil {
		t.Fatal("passphrase enclave should be nil after Close")
	}
}

func TestSoftwareStrategy_PassphraseInEnclave(t *testing.T) {
	original := []byte("enclave-test-passphrase")
	s, err := NewSoftwareStrategyWithParams(original, 1, 64*1024, 1)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	defer s.Close()

	// The passphrase should be stored in a memguard enclave.
	buf, openErr := s.passphrase.Open()
	if openErr != nil {
		t.Fatalf("passphrase.Open() failed: %v", openErr)
	}
	defer buf.Destroy()

	got := buf.Bytes()
	if string(got) != "enclave-test-passphrase" {
		t.Fatalf("passphrase mismatch: got %q", got)
	}
}

func TestSoftwareStrategy_MultipleSealsDifferentCiphertexts(t *testing.T) {
	s, err := NewSoftwareStrategyWithParams([]byte("test"), 1, 64*1024, 1)
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
	defer wipeBytes(dec1)

	dec2, err := s.UnsealRootKey(context.Background(), sealed2, Credentials{})
	if err != nil {
		t.Fatalf("Unseal sealed2 failed: %v", err)
	}
	defer wipeBytes(dec2)

	if !bytes.Equal(dec1, dec2) {
		t.Fatal("two unseals produced different root keys")
	}
}

func TestWipeBytes_AllZeroed(t *testing.T) {
	data := []byte{1, 2, 3, 4, 5, 6, 7, 8}
	wipeBytes(data)
	for i, b := range data {
		if b != 0 {
			t.Fatalf("byte at index %d is %d, expected 0", i, b)
		}
	}
}

func TestWipeBytes_EmptyAndNilSlice(t *testing.T) {
	// Should not panic.
	wipeBytes(nil)
	wipeBytes([]byte{})
}

func TestWipeBytes_LargeSlice(t *testing.T) {
	data := make([]byte, 1024)
	for i := range data {
		data[i] = 0xFF
	}
	wipeBytes(data)
	for i, b := range data {
		if b != 0 {
			t.Fatalf("byte at index %d is %d, expected 0", i, b)
		}
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
	s, err := NewSoftwareStrategyWithParams([]byte("test"), 1, 64*1024, 1)
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

func TestSoftwareStrategy_SealRandNonceFails(t *testing.T) {
	s, err := NewSoftwareStrategyWithParams([]byte("test"), 1, 64*1024, 1)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	defer s.Close()

	// Inject a failing reader: allow salt (32 bytes) to succeed but fail on nonce.
	s.randReader = &strategyErrReader{remaining: kdfSaltLen}

	rootKey := make([]byte, 32)
	_, err = s.SealRootKey(context.Background(), rootKey, Credentials{})
	if err == nil {
		t.Fatal("expected error from Seal with failing rand for nonce")
	}
	var sealErr *Error
	if !errors.As(err, &sealErr) {
		t.Fatalf("expected *Error, got %T: %v", err, err)
	}
}

// --- FIPS KDF integration tests ---

func TestNewSoftwareStrategyFIPS_Valid(t *testing.T) {
	s, err := NewSoftwareStrategyFIPS([]byte("fips-passphrase"), 1000)
	if err != nil {
		t.Fatalf("NewSoftwareStrategyFIPS returned unexpected error: %v", err)
	}
	defer s.Close()

	if s.kdfVersion != KDFPbkdf2SHA256 {
		t.Fatalf("expected KDFPbkdf2SHA256, got %d", s.kdfVersion)
	}
	if s.kdfParams.Iterations != 1000 {
		t.Fatalf("expected 1000 iterations, got %d", s.kdfParams.Iterations)
	}
}

func TestNewSoftwareStrategyFIPS_EmptyPassphrase(t *testing.T) {
	_, err := NewSoftwareStrategyFIPS(nil, 1000)
	if !errors.Is(err, ErrInvalidPassphrase) {
		t.Fatalf("expected ErrInvalidPassphrase, got %v", err)
	}
}

func TestSoftwareStrategyFIPS_SealUnsealRoundTrip(t *testing.T) {
	passphrase := []byte("fips-round-trip-passphrase")
	s, err := NewSoftwareStrategyFIPS(passphrase, 1000)
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
	defer wipeBytes(decrypted)

	if !bytes.Equal(rootKey, decrypted) {
		t.Fatal("FIPS round-trip failed: decrypted root key does not match original")
	}
}

func TestSoftwareStrategy_CrossKDFUnseal_MatchingParams(t *testing.T) {
	passphrase := []byte("cross-kdf-passphrase")
	rootKey := make([]byte, 32)
	if _, err := rand.Read(rootKey); err != nil {
		t.Fatalf("rand.Read failed: %v", err)
	}

	// Seal with Argon2id using custom params (time=1, memory=64KB, threads=1).
	argon2Strategy, err := NewSoftwareStrategyWithParams(passphrase, 1, 64*1024, 1)
	if err != nil {
		t.Fatalf("NewSoftwareStrategyWithParams failed: %v", err)
	}

	sealed, err := argon2Strategy.SealRootKey(context.Background(), rootKey, Credentials{})
	if err != nil {
		t.Fatalf("Seal with Argon2id failed: %v", err)
	}
	argon2Strategy.Close()

	// Unseal with a second Argon2id strategy using the same params.
	argon2Strategy2, err := NewSoftwareStrategyWithParams(passphrase, 1, 64*1024, 1)
	if err != nil {
		t.Fatalf("NewSoftwareStrategyWithParams failed: %v", err)
	}
	defer argon2Strategy2.Close()

	decrypted, err := argon2Strategy2.UnsealRootKey(context.Background(), sealed, Credentials{})
	if err != nil {
		t.Fatalf("Unseal with matching Argon2id params failed: %v", err)
	}
	defer wipeBytes(decrypted)

	if !bytes.Equal(rootKey, decrypted) {
		t.Fatal("cross-KDF unseal: decrypted key does not match original")
	}
}

func TestSoftwareStrategy_CrossKDFUnseal_FIPSMismatch(t *testing.T) {
	passphrase := []byte("cross-kdf-mismatch")
	rootKey := make([]byte, 32)
	if _, err := rand.Read(rootKey); err != nil {
		t.Fatalf("rand.Read failed: %v", err)
	}

	// Seal with Argon2id (custom params: time=1, memory=64KB, threads=1).
	argon2Strategy, err := NewSoftwareStrategyWithParams(passphrase, 1, 64*1024, 1)
	if err != nil {
		t.Fatalf("NewSoftwareStrategyWithParams failed: %v", err)
	}

	sealed, err := argon2Strategy.SealRootKey(context.Background(), rootKey, Credentials{})
	if err != nil {
		t.Fatalf("Seal with Argon2id failed: %v", err)
	}
	argon2Strategy.Close()

	// Unseal with a FIPS strategy. The metadata says KDFArgon2id and the
	// FIPS strategy's version is KDFPbkdf2SHA256 (mismatch), so defaults
	// for Argon2id are used (time=4, memory=128KB). Since the original
	// was sealed with time=1, memory=64KB, the derived key differs and
	// the AES-GCM authentication fails.
	fipsStrategy, err := NewSoftwareStrategyFIPS(passphrase, 1000)
	if err != nil {
		t.Fatalf("NewSoftwareStrategyFIPS failed: %v", err)
	}
	defer fipsStrategy.Close()

	_, err = fipsStrategy.UnsealRootKey(context.Background(), sealed, Credentials{})
	if err == nil {
		t.Fatal("expected unseal to fail: FIPS strategy uses default Argon2id params, not original custom params")
	}
	var unsealErr *UnsealError
	if !errors.As(err, &unsealErr) {
		t.Fatalf("expected *UnsealError, got %T: %v", err, err)
	}
}

func TestSoftwareStrategy_BackwardCompatibility_NoMetadata(t *testing.T) {
	passphrase := []byte("backward-compat")
	// Create a strategy with known params.
	s, err := NewSoftwareStrategyWithParams(passphrase, 1, 64*1024, 1)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	defer s.Close()

	rootKey := make([]byte, 32)
	if _, err := rand.Read(rootKey); err != nil {
		t.Fatalf("rand.Read failed: %v", err)
	}

	// Seal normally (which writes metadata).
	sealed, err := s.SealRootKey(context.Background(), rootKey, Credentials{})
	if err != nil {
		t.Fatalf("Seal failed: %v", err)
	}

	// Simulate pre-FIPS sealed data by clearing metadata.
	sealed.Metadata = nil

	// Unseal should still work, defaulting to KDFArgon2id with the
	// strategy's configured params.
	decrypted, err := s.UnsealRootKey(context.Background(), sealed, Credentials{})
	if err != nil {
		t.Fatalf("Unseal with nil metadata (backward compat) failed: %v", err)
	}
	defer wipeBytes(decrypted)

	if !bytes.Equal(rootKey, decrypted) {
		t.Fatal("backward compatibility unseal: decrypted key does not match original")
	}
}

func TestSoftwareStrategy_KDFVersion(t *testing.T) {
	argon2, err := NewSoftwareStrategyWithParams([]byte("test"), 1, 64*1024, 1)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	defer argon2.Close()
	if argon2.KDFVersion() != KDFArgon2id {
		t.Fatalf("expected KDFArgon2id, got %d", argon2.KDFVersion())
	}

	fips, err := NewSoftwareStrategyFIPS([]byte("test"), 1000)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	defer fips.Close()
	if fips.KDFVersion() != KDFPbkdf2SHA256 {
		t.Fatalf("expected KDFPbkdf2SHA256, got %d", fips.KDFVersion())
	}
}

func TestSoftwareStrategy_IDAvailableHardwareBacked(t *testing.T) {
	s, err := NewSoftwareStrategyWithParams([]byte("test"), 1, 64*1024, 1)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	defer s.Close()

	if s.ID() != StrategySoftware {
		t.Fatalf("expected ID=%q, got %q", "software", s.ID())
	}
	if !s.Available() {
		t.Fatal("expected Available() to return true")
	}
	if s.HardwareBacked() {
		t.Fatal("expected HardwareBacked() to return false")
	}
}

func TestSoftwareStrategyFIPS_UnsealWrongPassphrase(t *testing.T) {
	correct, err := NewSoftwareStrategyFIPS([]byte("correct-fips"), 1000)
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

	wrong, err := NewSoftwareStrategyFIPS([]byte("wrong-fips"), 1000)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	defer wrong.Close()

	_, err = wrong.UnsealRootKey(context.Background(), sealed, Credentials{})
	if err == nil {
		t.Fatal("expected error for wrong passphrase, got nil")
	}
	var unsealErr *UnsealError
	if !errors.As(err, &unsealErr) {
		t.Fatalf("expected *UnsealError, got %T: %v", err, err)
	}
}

// --- BarrierEncryptor tests ---

func TestSoftwareStrategy_BarrierEncryptor_ReturnsNilNil(t *testing.T) {
	s, err := NewSoftwareStrategyWithParams([]byte("test"), 1, 64*1024, 1)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	defer s.Close()

	enc, err := s.BarrierEncryptor(context.Background(), make([]byte, 32))
	if enc != nil {
		t.Fatalf("expected nil SymmetricEncrypter, got %v", enc)
	}
	if err != nil {
		t.Fatalf("expected nil error, got %v", err)
	}
}

func TestSoftwareStrategy_BarrierEncryptor_NilRootKey(t *testing.T) {
	s, err := NewSoftwareStrategyWithParams([]byte("test"), 1, 64*1024, 1)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	defer s.Close()

	// Even with nil rootKey, software strategy returns (nil, nil).
	enc, err := s.BarrierEncryptor(context.Background(), nil)
	if enc != nil {
		t.Fatalf("expected nil SymmetricEncrypter, got %v", enc)
	}
	if err != nil {
		t.Fatalf("expected nil error, got %v", err)
	}
}

// --- unsealKDFVersionAndParams unknown version ---

func TestSoftwareStrategy_UnsealKDFVersionAndParams_UnknownVersion(t *testing.T) {
	t.Parallel()

	s, err := NewSoftwareStrategyWithParams([]byte("test"), 1, 64*1024, 1)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	defer s.Close()

	// Metadata with an unknown KDF version byte (0xFF).
	metadata := []byte{0xFF}
	version, params := s.unsealKDFVersionAndParams(metadata)

	// Unknown version falls back to the strategy's configured KDF.
	if version != s.kdfVersion {
		t.Fatalf("expected fallback version=%d, got %d", s.kdfVersion, version)
	}
	if params.KeyLen != s.kdfParams.KeyLen {
		t.Fatalf("expected fallback params KeyLen=%d, got %d", s.kdfParams.KeyLen, params.KeyLen)
	}
}

func TestSoftwareStrategy_UnsealKDFVersionAndParams_EmptyMetadata(t *testing.T) {
	t.Parallel()

	s, err := NewSoftwareStrategyWithParams([]byte("test"), 1, 64*1024, 1)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	defer s.Close()

	// Empty metadata: backward compatibility path.
	version, params := s.unsealKDFVersionAndParams([]byte{})

	if version != s.kdfVersion {
		t.Fatalf("expected strategy version=%d, got %d", s.kdfVersion, version)
	}
	if params.Time != s.kdfParams.Time {
		t.Fatalf("expected strategy params Time=%d, got %d", s.kdfParams.Time, params.Time)
	}
}

func TestSoftwareStrategy_UnsealKDFVersionAndParams_CrossVersionDefaults(t *testing.T) {
	t.Parallel()

	// Create an Argon2id strategy but present PBKDF2 metadata.
	s, err := NewSoftwareStrategyWithParams([]byte("test"), 1, 64*1024, 1)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	defer s.Close()

	// Metadata says KDFPbkdf2SHA256 but strategy is KDFArgon2id (version mismatch).
	metadata := []byte{byte(KDFPbkdf2SHA256)}
	version, params := s.unsealKDFVersionAndParams(metadata)

	if version != KDFPbkdf2SHA256 {
		t.Fatalf("expected KDFPbkdf2SHA256, got %d", version)
	}
	// Should use default FIPS params, not the strategy's Argon2id params.
	defaultFIPS := DefaultFIPSKDFParams()
	if params.Iterations != defaultFIPS.Iterations {
		t.Fatalf("expected default FIPS iterations=%d, got %d", defaultFIPS.Iterations, params.Iterations)
	}
}

func TestSoftwareStrategy_UnsealKDFVersionAndParams_FIPSToArgon2Defaults(t *testing.T) {
	t.Parallel()

	// Create a FIPS strategy but present Argon2id metadata.
	s, err := NewSoftwareStrategyFIPS([]byte("test"), 1000)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	defer s.Close()

	// Metadata says KDFArgon2id but strategy is KDFPbkdf2SHA256 (version mismatch).
	metadata := []byte{byte(KDFArgon2id)}
	version, params := s.unsealKDFVersionAndParams(metadata)

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
	_, err := newSoftwareStrategy([]byte("test"), KDFVersion(0xFF), KDFParams{KeyLen: 32, SaltLen: 32})
	if !errors.Is(err, ErrKDFUnsupportedVersion) {
		t.Fatalf("expected ErrKDFUnsupportedVersion, got %v", err)
	}
}
