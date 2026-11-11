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
	"strings"
	"testing"
)

// TestErrorTypes_FormatAndUnwrap pins the message of each wrapping error type
// and that Unwrap returns the wrapped error.
func TestErrorTypes_FormatAndUnwrap(t *testing.T) {
	t.Parallel()
	inner := errors.New("inner failure")
	tests := []struct {
		name string
		err  interface {
			error
			Unwrap() error
		}
		inner    error
		expected string
	}{
		{"UnsealError", &UnsealError{Strategy: "passphrase", Err: errors.New("decryption authentication failed")}, nil,
			"quicraft/seal: unseal failed (strategy=passphrase): decryption authentication failed"},
		{"KeyDerivationError", &KeyDerivationError{Epoch: 123, Err: errors.New("hkdf expand failed")}, nil,
			"quicraft/seal: key derivation failed for epoch 123: hkdf expand failed"},
		{"Error", &Error{Strategy: "passphrase", Err: errors.New("random generator failed")}, nil,
			"quicraft/seal: seal failed (strategy=passphrase): random generator failed"},
		{"CipherError", &CipherError{Err: errors.New("invalid key length")}, nil,
			"quicraft/seal: cipher creation failed: invalid key length"},
		{"TrackerError bytes reset", &TrackerError{Tracker: "bytes", Op: "reset", Err: errors.New("persistent store unavailable")}, nil,
			"quicraft/seal: tracker bytes reset failed: persistent store unavailable"},
		{"TrackerError nonce clear", &TrackerError{Tracker: "nonce", Op: "clear", Err: errors.New("hardware error")}, nil,
			"quicraft/seal: tracker nonce clear failed: hardware error"},
		{"TrackerError bytes close", &TrackerError{Tracker: "bytes", Op: "close", Err: errors.New("flush failed")}, nil,
			"quicraft/seal: tracker bytes close failed: flush failed"},
		{"TrackerError nonce seed", &TrackerError{Tracker: "nonce", Op: "seed", Err: errors.New("invalid seed data")}, nil,
			"quicraft/seal: tracker nonce seed failed: invalid seed data"},
		{"TrackerError empty tracker and op", &TrackerError{Err: errors.New("unknown")}, nil,
			"quicraft/seal: tracker   failed: unknown"},
		{"UnsealError unwrap", &UnsealError{Strategy: "passphrase", Err: inner}, inner, ""},
		{"KeyDerivationError unwrap", &KeyDerivationError{Epoch: 5, Err: inner}, inner, ""},
		{"Error unwrap", &Error{Strategy: "passphrase", Err: inner}, inner, ""},
		{"CipherError unwrap", &CipherError{Err: inner}, inner, ""},
		{"TrackerError unwrap", &TrackerError{Tracker: "bytes", Op: "reset", Err: inner}, inner, ""},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if tc.expected != "" {
				if got := tc.err.Error(); got != tc.expected {
					t.Fatalf("Error() mismatch:\n  got:  %q\n  want: %q", got, tc.expected)
				}
			}
			if tc.inner != nil {
				if got := tc.err.Unwrap(); got != tc.inner {
					t.Fatalf("Unwrap() returned %v, want %v", got, tc.inner)
				}
			}
		})
	}
}

// TestSentinelErrors verifies that the sentinel errors are distinct from each
// other and carry the quicraft/seal prefix and a descriptive message.
func TestSentinelErrors(t *testing.T) {
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

	for i := range tests {
		for j := range tests {
			if i != j && errors.Is(tests[i].err, tests[j].err) {
				t.Fatalf("sentinel errors %q and %q should be distinct", tests[i].err, tests[j].err)
			}
		}
	}
	for _, tc := range tests {
		t.Run(tc.err.Error(), func(t *testing.T) {
			msg := tc.err.Error()
			if !strings.HasPrefix(msg, "quicraft/seal") {
				t.Fatalf("error should have quicraft/seal prefix: %q", msg)
			}
			if !strings.Contains(msg, tc.contains) {
				t.Fatalf("error %q should mention %q", msg, tc.contains)
			}
		})
	}
}
