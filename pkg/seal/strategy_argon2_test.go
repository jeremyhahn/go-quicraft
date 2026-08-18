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

//go:build !fips

package seal

import (
	"context"
	"crypto/rand"
	"errors"
	"testing"
)

// Tests for the Argon2id-only surface of SoftwareStrategy. They live behind
// //go:build !fips because they need Argon2id to actually DERIVE, and its
// deriver is not compiled into a FIPS build (kdf_argon2.go); the cross-KDF cases
// additionally need two different KDFs to cross between. The constructor itself
// exists in both builds — the FIPS-side contract is pinned by
// strategy_params_api_fips_test.go.

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

func TestSoftwareStrategy_UnsealKDFVersionAndParams_CrossVersionDefaults(t *testing.T) {
	t.Parallel()

	// Create an Argon2id strategy but present PBKDF2 metadata.
	s, err := NewSoftwareStrategyWithParams([]byte("test"), 1, 64*1024, 2)
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
