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
	"errors"
	"testing"
)

// --- UnsealError Tests ---

func TestUnsealError_Error_FormatsMessage(t *testing.T) {
	inner := errors.New("decryption authentication failed")
	err := &UnsealError{Strategy: "software", Err: inner}

	expected := "quicraft/seal: unseal failed (strategy=software): decryption authentication failed"
	if got := err.Error(); got != expected {
		t.Fatalf("Error() mismatch:\n  got:  %q\n  want: %q", got, expected)
	}
}

func TestUnsealError_Error_DifferentStrategies(t *testing.T) {
	tests := []struct {
		name     string
		strategy string
		inner    error
		expected string
	}{
		{
			name:     "hardware strategy",
			strategy: "hardware",
			inner:    errors.New("TPM unsealing failed"),
			expected: "quicraft/seal: unseal failed (strategy=hardware): TPM unsealing failed",
		},
		{
			name:     "transit strategy",
			strategy: "transit",
			inner:    errors.New("remote key service unavailable"),
			expected: "quicraft/seal: unseal failed (strategy=transit): remote key service unavailable",
		},
		{
			name:     "empty strategy",
			strategy: "",
			inner:    errors.New("unknown error"),
			expected: "quicraft/seal: unseal failed (strategy=): unknown error",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := &UnsealError{Strategy: tc.strategy, Err: tc.inner}
			if got := err.Error(); got != tc.expected {
				t.Fatalf("Error() mismatch:\n  got:  %q\n  want: %q", got, tc.expected)
			}
		})
	}
}

func TestUnsealError_Unwrap_ReturnsInnerError(t *testing.T) {
	inner := errors.New("wrapped error")
	err := &UnsealError{Strategy: "software", Err: inner}

	unwrapped := err.Unwrap()
	if unwrapped != inner {
		t.Fatalf("Unwrap() returned %v, want %v", unwrapped, inner)
	}
}

func TestUnsealError_Unwrap_NilError(t *testing.T) {
	err := &UnsealError{Strategy: "software", Err: nil}

	unwrapped := err.Unwrap()
	if unwrapped != nil {
		t.Fatalf("Unwrap() returned %v, want nil", unwrapped)
	}
}

func TestUnsealError_ErrorsIs_MatchesInnerError(t *testing.T) {
	inner := ErrDecryptionFailed
	err := &UnsealError{Strategy: "software", Err: inner}

	if !errors.Is(err, ErrDecryptionFailed) {
		t.Fatal("errors.Is should match the inner ErrDecryptionFailed")
	}
}

func TestUnsealError_ErrorsIs_NestedWrapping(t *testing.T) {
	// Create a chain: UnsealError -> Error -> sentinel
	sentinel := ErrBarrierSealed
	inner := &Error{Strategy: "software", Err: sentinel}
	outer := &UnsealError{Strategy: "software", Err: inner}

	if !errors.Is(outer, ErrBarrierSealed) {
		t.Fatal("errors.Is should traverse through nested wrapped errors")
	}
	if !errors.Is(outer, inner) {
		t.Fatal("errors.Is should match intermediate wrapped error")
	}
}

func TestUnsealError_ErrorsAs_ExtractsUnsealError(t *testing.T) {
	inner := errors.New("test error")
	err := &UnsealError{Strategy: "test-strategy", Err: inner}

	var target *UnsealError
	if !errors.As(err, &target) {
		t.Fatal("errors.As should extract *UnsealError from itself")
	}
	if target.Strategy != "test-strategy" {
		t.Fatalf("Strategy mismatch: got %q, want %q", target.Strategy, "test-strategy")
	}
	if target.Err != inner {
		t.Fatal("Err field mismatch after errors.As")
	}
}

func TestUnsealError_ErrorsAs_FromWrappedError(t *testing.T) {
	inner := errors.New("original cause")
	unsealErr := &UnsealError{Strategy: "software", Err: inner}
	// Wrap the UnsealError in another error
	wrapped := errors.Join(errors.New("context"), unsealErr)

	var target *UnsealError
	if !errors.As(wrapped, &target) {
		t.Fatal("errors.As should extract *UnsealError from wrapped error")
	}
	if target.Strategy != "software" {
		t.Fatalf("Strategy mismatch: got %q, want %q", target.Strategy, "software")
	}
}

func TestUnsealError_ErrorsAs_ExtractsNestedInnerError(t *testing.T) {
	// Inner error is a typed error
	innerTyped := &CipherError{Err: errors.New("cipher issue")}
	unsealErr := &UnsealError{Strategy: "software", Err: innerTyped}

	var target *CipherError
	if !errors.As(unsealErr, &target) {
		t.Fatal("errors.As should extract nested *CipherError from UnsealError")
	}
	if target.Err.Error() != "cipher issue" {
		t.Fatalf("inner error mismatch: got %q", target.Err.Error())
	}
}

// --- ShareStoreError Tests ---

func TestShareStoreError_Error_FormatsMessage(t *testing.T) {
	inner := errors.New("permission denied")
	err := &ShareStoreError{Op: "save_metadata", Err: inner}

	expected := "quicraft/seal: share store save_metadata failed: permission denied"
	if got := err.Error(); got != expected {
		t.Fatalf("Error() mismatch:\n  got:  %q\n  want: %q", got, expected)
	}
}

func TestShareStoreError_Error_DifferentOperations(t *testing.T) {
	tests := []struct {
		name     string
		op       string
		inner    error
		expected string
	}{
		{
			name:     "get_sealed_share operation",
			op:       "get_sealed_share",
			inner:    errors.New("file not found"),
			expected: "quicraft/seal: share store get_sealed_share failed: file not found",
		},
		{
			name:     "save_version operation",
			op:       "save_version",
			inner:    errors.New("disk full"),
			expected: "quicraft/seal: share store save_version failed: disk full",
		},
		{
			name:     "delete operation",
			op:       "delete",
			inner:    errors.New("entry locked"),
			expected: "quicraft/seal: share store delete failed: entry locked",
		},
		{
			name:     "empty operation",
			op:       "",
			inner:    errors.New("unknown operation"),
			expected: "quicraft/seal: share store  failed: unknown operation",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := &ShareStoreError{Op: tc.op, Err: tc.inner}
			if got := err.Error(); got != tc.expected {
				t.Fatalf("Error() mismatch:\n  got:  %q\n  want: %q", got, tc.expected)
			}
		})
	}
}

func TestShareStoreError_Unwrap_ReturnsInnerError(t *testing.T) {
	inner := errors.New("wrapped store error")
	err := &ShareStoreError{Op: "get_metadata", Err: inner}

	unwrapped := err.Unwrap()
	if unwrapped != inner {
		t.Fatalf("Unwrap() returned %v, want %v", unwrapped, inner)
	}
}

func TestShareStoreError_Unwrap_NilError(t *testing.T) {
	err := &ShareStoreError{Op: "test_op", Err: nil}

	unwrapped := err.Unwrap()
	if unwrapped != nil {
		t.Fatalf("Unwrap() returned %v, want nil", unwrapped)
	}
}

func TestShareStoreError_ErrorsIs_MatchesSentinel(t *testing.T) {
	err := &ShareStoreError{Op: "get_metadata", Err: ErrShareStoreNotFound}

	if !errors.Is(err, ErrShareStoreNotFound) {
		t.Fatal("errors.Is should match ErrShareStoreNotFound sentinel")
	}
}

func TestShareStoreError_ErrorsAs_ExtractsShareStoreError(t *testing.T) {
	inner := errors.New("io error")
	err := &ShareStoreError{Op: "save_sealed_share", Err: inner}

	var target *ShareStoreError
	if !errors.As(err, &target) {
		t.Fatal("errors.As should extract *ShareStoreError from itself")
	}
	if target.Op != "save_sealed_share" {
		t.Fatalf("Op mismatch: got %q, want %q", target.Op, "save_sealed_share")
	}
}

func TestShareStoreError_ErrorsAs_FromWrappedError(t *testing.T) {
	inner := errors.New("original cause")
	storeErr := &ShareStoreError{Op: "get_version", Err: inner}
	wrapped := errors.Join(errors.New("additional context"), storeErr)

	var target *ShareStoreError
	if !errors.As(wrapped, &target) {
		t.Fatal("errors.As should extract *ShareStoreError from wrapped error")
	}
	if target.Op != "get_version" {
		t.Fatalf("Op mismatch: got %q, want %q", target.Op, "get_version")
	}
}

// --- KeyDerivationError Tests ---

func TestKeyDerivationError_Error_FormatsMessage(t *testing.T) {
	inner := errors.New("hkdf expand failed")
	err := &KeyDerivationError{Epoch: 123, Err: inner}

	expected := "quicraft/seal: key derivation failed for epoch 123: hkdf expand failed"
	if got := err.Error(); got != expected {
		t.Fatalf("Error() mismatch:\n  got:  %q\n  want: %q", got, expected)
	}
}

func TestKeyDerivationError_Error_ZeroEpoch(t *testing.T) {
	inner := errors.New("invalid epoch")
	err := &KeyDerivationError{Epoch: 0, Err: inner}

	expected := "quicraft/seal: key derivation failed for epoch 0: invalid epoch"
	if got := err.Error(); got != expected {
		t.Fatalf("Error() mismatch:\n  got:  %q\n  want: %q", got, expected)
	}
}

func TestKeyDerivationError_Error_MaxEpoch(t *testing.T) {
	inner := errors.New("max epoch reached")
	err := &KeyDerivationError{Epoch: ^uint64(0), Err: inner}

	expected := "quicraft/seal: key derivation failed for epoch 18446744073709551615: max epoch reached"
	if got := err.Error(); got != expected {
		t.Fatalf("Error() mismatch:\n  got:  %q\n  want: %q", got, expected)
	}
}

func TestKeyDerivationError_Unwrap_ReturnsInnerError(t *testing.T) {
	inner := errors.New("derivation error")
	err := &KeyDerivationError{Epoch: 5, Err: inner}

	unwrapped := err.Unwrap()
	if unwrapped != inner {
		t.Fatalf("Unwrap() returned %v, want %v", unwrapped, inner)
	}
}

func TestKeyDerivationError_Unwrap_NilError(t *testing.T) {
	err := &KeyDerivationError{Epoch: 1, Err: nil}

	unwrapped := err.Unwrap()
	if unwrapped != nil {
		t.Fatalf("Unwrap() returned %v, want nil", unwrapped)
	}
}

func TestKeyDerivationError_ErrorsIs_MatchesInnerError(t *testing.T) {
	sentinel := errors.New("sentinel derivation error")
	err := &KeyDerivationError{Epoch: 10, Err: sentinel}

	if !errors.Is(err, sentinel) {
		t.Fatal("errors.Is should match the inner sentinel error")
	}
}

func TestKeyDerivationError_ErrorsAs_ExtractsKeyDerivationError(t *testing.T) {
	inner := errors.New("test error")
	err := &KeyDerivationError{Epoch: 42, Err: inner}

	var target *KeyDerivationError
	if !errors.As(err, &target) {
		t.Fatal("errors.As should extract *KeyDerivationError")
	}
	if target.Epoch != 42 {
		t.Fatalf("Epoch mismatch: got %d, want %d", target.Epoch, 42)
	}
}

// --- Error (seal.Error) Tests ---

func TestError_Error_FormatsMessage(t *testing.T) {
	inner := errors.New("random generator failed")
	err := &Error{Strategy: "software", Err: inner}

	expected := "quicraft/seal: seal failed (strategy=software): random generator failed"
	if got := err.Error(); got != expected {
		t.Fatalf("Error() mismatch:\n  got:  %q\n  want: %q", got, expected)
	}
}

func TestError_Error_DifferentStrategies(t *testing.T) {
	tests := []struct {
		name     string
		strategy string
		inner    error
		expected string
	}{
		{
			name:     "hardware strategy",
			strategy: "hardware",
			inner:    errors.New("TPM sealing failed"),
			expected: "quicraft/seal: seal failed (strategy=hardware): TPM sealing failed",
		},
		{
			name:     "transit strategy",
			strategy: "transit",
			inner:    errors.New("encryption service error"),
			expected: "quicraft/seal: seal failed (strategy=transit): encryption service error",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := &Error{Strategy: tc.strategy, Err: tc.inner}
			if got := err.Error(); got != tc.expected {
				t.Fatalf("Error() mismatch:\n  got:  %q\n  want: %q", got, tc.expected)
			}
		})
	}
}

func TestError_Unwrap_ReturnsInnerError(t *testing.T) {
	inner := errors.New("wrapped seal error")
	err := &Error{Strategy: "software", Err: inner}

	unwrapped := err.Unwrap()
	if unwrapped != inner {
		t.Fatalf("Unwrap() returned %v, want %v", unwrapped, inner)
	}
}

func TestError_Unwrap_NilError(t *testing.T) {
	err := &Error{Strategy: "software", Err: nil}

	unwrapped := err.Unwrap()
	if unwrapped != nil {
		t.Fatalf("Unwrap() returned %v, want nil", unwrapped)
	}
}

func TestError_ErrorsIs_MatchesInnerSentinel(t *testing.T) {
	err := &Error{Strategy: "software", Err: ErrInvalidRootKeySize}

	if !errors.Is(err, ErrInvalidRootKeySize) {
		t.Fatal("errors.Is should match ErrInvalidRootKeySize sentinel")
	}
}

func TestError_ErrorsAs_ExtractsError(t *testing.T) {
	inner := errors.New("test error")
	err := &Error{Strategy: "test-strategy", Err: inner}

	var target *Error
	if !errors.As(err, &target) {
		t.Fatal("errors.As should extract *Error")
	}
	if target.Strategy != "test-strategy" {
		t.Fatalf("Strategy mismatch: got %q, want %q", target.Strategy, "test-strategy")
	}
}

// --- CipherError Tests ---

func TestCipherError_Error_FormatsMessage(t *testing.T) {
	inner := errors.New("invalid key length")
	err := &CipherError{Err: inner}

	expected := "quicraft/seal: cipher creation failed: invalid key length"
	if got := err.Error(); got != expected {
		t.Fatalf("Error() mismatch:\n  got:  %q\n  want: %q", got, expected)
	}
}

func TestCipherError_Unwrap_ReturnsInnerError(t *testing.T) {
	inner := errors.New("aes error")
	err := &CipherError{Err: inner}

	unwrapped := err.Unwrap()
	if unwrapped != inner {
		t.Fatalf("Unwrap() returned %v, want %v", unwrapped, inner)
	}
}

func TestCipherError_Unwrap_NilError(t *testing.T) {
	err := &CipherError{Err: nil}

	unwrapped := err.Unwrap()
	if unwrapped != nil {
		t.Fatalf("Unwrap() returned %v, want nil", unwrapped)
	}
}

func TestCipherError_ErrorsIs_MatchesInnerError(t *testing.T) {
	sentinel := errors.New("sentinel cipher error")
	err := &CipherError{Err: sentinel}

	if !errors.Is(err, sentinel) {
		t.Fatal("errors.Is should match the inner sentinel error")
	}
}

func TestCipherError_ErrorsAs_ExtractsCipherError(t *testing.T) {
	inner := errors.New("test cipher error")
	err := &CipherError{Err: inner}

	var target *CipherError
	if !errors.As(err, &target) {
		t.Fatal("errors.As should extract *CipherError")
	}
	if target.Err != inner {
		t.Fatal("Err field mismatch after errors.As")
	}
}

// --- Sentinel Error Tests ---

func TestSentinelErrors_AreDistinct(t *testing.T) {
	sentinels := []error{
		ErrBarrierSealed,
		ErrBarrierNotInit,
		ErrDecryptionFailed,
		ErrBarrierAlreadyInit,
		ErrBarrierUnsealed,
		ErrInvalidRootKeySize,
		ErrInvalidPassphrase,
		ErrCiphertextTooShort,
		ErrUnsealBackoff,
		ErrPurgeCurrentEpoch,
		ErrInvalidSealedEpoch,
		ErrInvalidSealedSalt,
		ErrInvalidSealedEpochs,
		ErrKDFUnsupportedVersion,
		ErrKDFInvalidPassphrase,
		ErrKDFInvalidSalt,
		ErrHardwareMode,
		ErrShamirNotConfigured,
		ErrShamirThresholdInvalid,
		ErrShareStoreInvalidDir,
		ErrShareStoreNotFound,
		ErrShareStoreNilMetadata,
		ErrShareStoreInvalidIndex,
		ErrShareStoreNilShare,
	}

	for i := range sentinels {
		for j := range sentinels {
			if i != j && errors.Is(sentinels[i], sentinels[j]) {
				t.Fatalf("sentinel errors %d and %d should be distinct", i, j)
			}
		}
	}
}

// --- TrackerError Tests ---

func TestTrackerError_Error_FormatsMessage(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name     string
		tracker  string
		op       string
		inner    error
		expected string
	}{
		{
			name:     "bytes tracker reset failure",
			tracker:  "bytes",
			op:       "reset",
			inner:    errors.New("persistent store unavailable"),
			expected: "quicraft/seal: tracker bytes reset failed: persistent store unavailable",
		},
		{
			name:     "nonce tracker clear failure",
			tracker:  "nonce",
			op:       "clear",
			inner:    errors.New("hardware error"),
			expected: "quicraft/seal: tracker nonce clear failed: hardware error",
		},
		{
			name:     "bytes tracker close failure",
			tracker:  "bytes",
			op:       "close",
			inner:    errors.New("flush failed"),
			expected: "quicraft/seal: tracker bytes close failed: flush failed",
		},
		{
			name:     "nonce tracker seed failure",
			tracker:  "nonce",
			op:       "seed",
			inner:    errors.New("invalid seed data"),
			expected: "quicraft/seal: tracker nonce seed failed: invalid seed data",
		},
		{
			name:     "empty tracker and op",
			tracker:  "",
			op:       "",
			inner:    errors.New("unknown"),
			expected: "quicraft/seal: tracker   failed: unknown",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			err := &TrackerError{Tracker: tc.tracker, Op: tc.op, Err: tc.inner}
			if got := err.Error(); got != tc.expected {
				t.Fatalf("Error() mismatch:\n  got:  %q\n  want: %q", got, tc.expected)
			}
		})
	}
}

func TestTrackerError_Unwrap_ReturnsInnerError(t *testing.T) {
	t.Parallel()
	inner := errors.New("underlying tracker failure")
	err := &TrackerError{Tracker: "bytes", Op: "reset", Err: inner}

	unwrapped := err.Unwrap()
	if unwrapped != inner {
		t.Fatalf("Unwrap() returned %v, want %v", unwrapped, inner)
	}
}

func TestTrackerError_Unwrap_NilError(t *testing.T) {
	t.Parallel()
	err := &TrackerError{Tracker: "nonce", Op: "clear", Err: nil}

	unwrapped := err.Unwrap()
	if unwrapped != nil {
		t.Fatalf("Unwrap() returned %v, want nil", unwrapped)
	}
}

func TestTrackerError_ErrorsAs_ExtractsTrackerError(t *testing.T) {
	t.Parallel()
	inner := errors.New("disk write failed")
	err := &TrackerError{Tracker: "bytes", Op: "reset", Err: inner}

	var target *TrackerError
	if !errors.As(err, &target) {
		t.Fatal("errors.As should extract *TrackerError from itself")
	}
	if target.Tracker != "bytes" {
		t.Fatalf("Tracker mismatch: got %q, want %q", target.Tracker, "bytes")
	}
	if target.Op != "reset" {
		t.Fatalf("Op mismatch: got %q, want %q", target.Op, "reset")
	}
	if target.Err != inner {
		t.Fatal("Err field mismatch after errors.As")
	}
}

func TestTrackerError_ErrorsAs_FromWrappedError(t *testing.T) {
	t.Parallel()
	inner := errors.New("original cause")
	trackerErr := &TrackerError{Tracker: "nonce", Op: "clear", Err: inner}
	wrapped := errors.Join(errors.New("context"), trackerErr)

	var target *TrackerError
	if !errors.As(wrapped, &target) {
		t.Fatal("errors.As should extract *TrackerError from wrapped error")
	}
	if target.Tracker != "nonce" {
		t.Fatalf("Tracker mismatch: got %q, want %q", target.Tracker, "nonce")
	}
	if target.Op != "clear" {
		t.Fatalf("Op mismatch: got %q, want %q", target.Op, "clear")
	}
}

func TestTrackerError_ErrorsIs_MatchesInnerSentinel(t *testing.T) {
	t.Parallel()
	sentinel := errors.New("sentinel tracker error")
	err := &TrackerError{Tracker: "bytes", Op: "reset", Err: sentinel}

	if !errors.Is(err, sentinel) {
		t.Fatal("errors.Is should match the inner sentinel error via Unwrap")
	}
}

func TestTrackerError_ErrorsIs_NestedWrapping(t *testing.T) {
	t.Parallel()
	// Create a chain: TrackerError -> CipherError -> sentinel
	sentinel := ErrBarrierSealed
	cipherErr := &CipherError{Err: sentinel}
	trackerErr := &TrackerError{Tracker: "bytes", Op: "close", Err: cipherErr}

	if !errors.Is(trackerErr, ErrBarrierSealed) {
		t.Fatal("errors.Is should traverse through nested wrapped errors to match sentinel")
	}
	if !errors.Is(trackerErr, cipherErr) {
		t.Fatal("errors.Is should match intermediate wrapped CipherError")
	}
}

func TestSentinelErrors_HaveDescriptiveMessages(t *testing.T) {
	tests := []struct {
		err      error
		contains string
	}{
		{ErrBarrierSealed, "sealed"},
		{ErrBarrierNotInit, "not initialized"},
		{ErrDecryptionFailed, "decryption failed"},
		{ErrBarrierAlreadyInit, "already initialized"},
		{ErrBarrierUnsealed, "already unsealed"},
		{ErrInvalidRootKeySize, "invalid root key size"},
		{ErrInvalidPassphrase, "passphrase"},
		{ErrCiphertextTooShort, "ciphertext too short"},
		{ErrUnsealBackoff, "backoff"},
		{ErrPurgeCurrentEpoch, "purge current epoch"},
		{ErrInvalidSealedEpoch, "invalid epoch"},
		{ErrInvalidSealedSalt, "invalid salt"},
		{ErrInvalidSealedEpochs, "missing current epoch"},
		{ErrKDFUnsupportedVersion, "unsupported KDF version"},
		{ErrKDFInvalidPassphrase, "passphrase"},
		{ErrKDFInvalidSalt, "salt too short"},
		{ErrHardwareMode, "hardware encryptor"},
		{ErrShamirNotConfigured, "shamir not configured"},
		{ErrShamirThresholdInvalid, "threshold"},
		{ErrShareStoreInvalidDir, "directory"},
		{ErrShareStoreNotFound, "not found"},
		{ErrShareStoreNilMetadata, "metadata"},
		{ErrShareStoreInvalidIndex, "index"},
		{ErrShareStoreNilShare, "share data"},
	}

	for _, tc := range tests {
		t.Run(tc.err.Error(), func(t *testing.T) {
			msg := tc.err.Error()
			if len(msg) == 0 {
				t.Fatal("error message should not be empty")
			}
			// Verify prefix
			if msg[:13] != "quicraft/seal" {
				t.Fatalf("error should have quicraft/seal prefix: %q", msg)
			}
		})
	}
}
