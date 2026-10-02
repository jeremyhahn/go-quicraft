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
	"bytes"
	"context"
	"errors"
	"testing"
)

// newTestStrategy returns a SoftwareStrategy wrapped with the cheapest KDF this
// build supports, so tests that need *a* working strategy do not each hard-code
// one. Argon2id here; PBKDF2-SHA256 in a build with -tags fips, which has no
// Argon2id compiled in (see kdf_argon2.go).
//
// The work factors are far below the production floor and rely on the bypass
// TestMain installs. Tests that are ABOUT a specific KDF must name it directly
// rather than go through this helper.
func newTestStrategy(passphrase []byte) (*SoftwareStrategy, error) {
	return NewSoftwareStrategyWithParams(context.Background(), passphrase, 1, 64*1024, 1)
}

// testKDFVersion is the KDF version newTestStrategy seals with, for tests that
// assert on the version stamped into SealedRootKey.Metadata. It is also the KDF
// that build-neutral derivation tests exercise.
const testKDFVersion = KDFArgon2id

// testKDFParams are cheap parameters for testKDFVersion.
func testKDFParams() KDFParams {
	return KDFParams{Time: 1, Memory: 64 * 1024, Threads: 1, KeyLen: 32, SaltLen: 32}
}

// assertDefaultKDFVersionOutsideFIPS pins what DefaultKDFVersion answers with
// GOFIPS140 unset: Argon2id, the standard-mode KDF this build compiles in.
func assertDefaultKDFVersionOutsideFIPS(t *testing.T) {
	t.Helper()
	if v := DefaultKDFVersion(); v != KDFArgon2id {
		t.Fatalf("expected KDFArgon2id (%d), got %d", KDFArgon2id, v)
	}
}

// assertArgon2idBaseline pins what this build does with Argon2id when FIPS is
// NOT requested: it derives normally, because the deriver is compiled in.
func assertArgon2idBaseline(t *testing.T, pass, salt []byte) {
	t.Helper()
	if _, err := DeriveKey(pass, salt, KDFArgon2id, DefaultKDFParams()); err != nil {
		t.Fatalf("DeriveKey(Argon2id) outside FIPS: %v", err)
	}
}

// assertArgon2idRefusedUnderFIPS pins what this build does with Argon2id when
// FIPS mode IS requested at runtime: DeriveKey refuses it, and so does the
// constructor that names it explicitly — at construction, which is the earliest
// point an operator can act on.
func assertArgon2idRefusedUnderFIPS(t *testing.T, pass, salt []byte) {
	t.Helper()
	if _, err := DeriveKey(pass, salt, KDFArgon2id, DefaultKDFParams()); !errors.Is(err, ErrKDFNotFIPSApproved) {
		t.Fatalf("DeriveKey(Argon2id) under FIPS = %v, want ErrKDFNotFIPSApproved", err)
	}
	if _, err := NewSoftwareStrategyWithParams(context.Background(), pass, 4, 128*1024, 4); !errors.Is(err, ErrKDFNotFIPSApproved) {
		t.Fatalf("NewSoftwareStrategyWithParams under FIPS = %v, want ErrKDFNotFIPSApproved", err)
	}
}

// assertV2SeparatesSameKeySizeAlgorithms pins that the HKDFV2 info string
// separates two algorithms of the SAME key length. ChaCha20-Poly1305 and
// AES-256-GCM are both 32-byte keys, so under v1 — whose info omits the
// algorithm — they would derive the identical DEK and be used as keys for two
// different primitives.
func assertV2SeparatesSameKeySizeAlgorithms(t *testing.T, rootKey, salt, aes256DEK []byte) {
	t.Helper()
	chacha, err := deriveDEK(rootKey, salt, 7, 3, AlgChaCha20Poly1305, HKDFV2)
	if err != nil {
		t.Fatalf("deriveDEK v2/chacha: %v", err)
	}
	if bytes.Equal(chacha, aes256DEK) {
		t.Fatal("v2 layout derived the same key for two different algorithms")
	}
}

// assertChaChaRefusedUnderFIPS pins how a normal build refuses the ChaCha20
// family once FIPS mode is requested at runtime: the algorithms are compiled
// in and registered, so the refusal is a POLICY decision reported as
// ErrAlgorithmNotFIPSApproved.
func assertChaChaRefusedUnderFIPS(t *testing.T) {
	t.Helper()
	for _, alg := range []SymmetricAlgorithm{AlgChaCha20Poly1305, AlgXChaCha20Poly1305} {
		cfg := BarrierConfig{Algorithm: alg, DeploymentID: 1}
		if err := cfg.Validate(); !errors.Is(err, ErrAlgorithmNotFIPSApproved) {
			t.Fatalf("validate(%v) under FIPS = %v, want ErrAlgorithmNotFIPSApproved", alg, err)
		}
	}
}

// assertChaChaOutsideFIPS pins that a normal build still offers ChaCha20 when
// FIPS is not requested. The FIPS constraint is about approval, not about
// removing the algorithm from a non-FIPS deployment.
func assertChaChaOutsideFIPS(t *testing.T) {
	t.Helper()
	cfg := BarrierConfig{Algorithm: AlgChaCha20Poly1305, DeploymentID: 1}
	if err := cfg.Validate(); err != nil {
		t.Fatalf("validate(ChaCha20) outside FIPS = %v, want nil", err)
	}
}
