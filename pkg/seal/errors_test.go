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
	err := &UnsealError{Strategy: "passphrase", Err: inner}

	expected := "quicraft/seal: unseal failed (strategy=passphrase): decryption authentication failed"
	if got := err.Error(); got != expected {
		t.Fatalf("Error() mismatch:\n  got:  %q\n  want: %q", got, expected)
	}
}

func TestUnsealError_Unwrap_ReturnsInnerError(t *testing.T) {
	inner := errors.New("wrapped error")
	err := &UnsealError{Strategy: "passphrase", Err: inner}

	unwrapped := err.Unwrap()
	if unwrapped != inner {
		t.Fatalf("Unwrap() returned %v, want %v", unwrapped, inner)
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

func TestKeyDerivationError_Unwrap_ReturnsInnerError(t *testing.T) {
	inner := errors.New("derivation error")
	err := &KeyDerivationError{Epoch: 5, Err: inner}

	unwrapped := err.Unwrap()
	if unwrapped != inner {
		t.Fatalf("Unwrap() returned %v, want %v", unwrapped, inner)
	}
}

// --- Error (seal.Error) Tests ---

func TestError_Error_FormatsMessage(t *testing.T) {
	inner := errors.New("random generator failed")
	err := &Error{Strategy: "passphrase", Err: inner}

	expected := "quicraft/seal: seal failed (strategy=passphrase): random generator failed"
	if got := err.Error(); got != expected {
		t.Fatalf("Error() mismatch:\n  got:  %q\n  want: %q", got, expected)
	}
}

func TestError_Unwrap_ReturnsInnerError(t *testing.T) {
	inner := errors.New("wrapped seal error")
	err := &Error{Strategy: "passphrase", Err: inner}

	unwrapped := err.Unwrap()
	if unwrapped != inner {
		t.Fatalf("Unwrap() returned %v, want %v", unwrapped, inner)
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
		ErrEpochsInFlight,
		ErrResetNotFirstBoot,
		ErrInvalidSealedEpoch,
		ErrInvalidSealedSalt,
		ErrInvalidSealedEpochs,
		ErrKDFUnsupportedVersion,
		ErrKDFInvalidPassphrase,
		ErrKDFInvalidSalt,
		ErrHardwareMode,
		ErrShamirThresholdInvalid,
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
		{ErrEpochsInFlight, "pinned in flight"},
		{ErrResetNotFirstBoot, "uncompleted first-boot"},
		{ErrInvalidSealedEpoch, "invalid epoch"},
		{ErrInvalidSealedSalt, "invalid salt"},
		{ErrInvalidSealedEpochs, "missing current epoch"},
		{ErrKDFUnsupportedVersion, "unsupported KDF version"},
		{ErrKDFInvalidPassphrase, "passphrase"},
		{ErrKDFInvalidSalt, "salt too short"},
		{ErrHardwareMode, "hardware encryptor"},
		{ErrShamirThresholdInvalid, "threshold"},
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
