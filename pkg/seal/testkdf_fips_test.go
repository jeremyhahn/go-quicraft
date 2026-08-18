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

//go:build fips

package seal

import (
	"errors"
	"testing"
)

// newTestStrategy returns a SoftwareStrategy wrapped with the cheapest KDF this
// build supports, so tests that need *a* working strategy do not each hard-code
// one. PBKDF2-SHA256 here; Argon2id in a build without -tags fips. This build has no
// Argon2id compiled in at all (see kdf_argon2.go).
//
// The work factors are far below the production floor and rely on the bypass
// TestMain installs. Tests that are ABOUT a specific KDF must name it directly
// rather than go through this helper.
func newTestStrategy(passphrase []byte) (*SoftwareStrategy, error) {
	return NewSoftwareStrategyFIPS(passphrase, 1000)
}

// testKDFVersion is the KDF version newTestStrategy seals with, for tests that
// assert on the version stamped into SealedRootKey.Metadata.
const testKDFVersion = KDFPbkdf2SHA256

// testKDFParams are cheap parameters for testKDFVersion.
func testKDFParams() KDFParams {
	return KDFParams{Iterations: 1000, KeyLen: 32, SaltLen: 32}
}

// assertDefaultKDFVersionOutsideFIPS pins what DefaultKDFVersion answers with
// GOFIPS140 unset in a -tags fips build: PBKDF2-SHA256, because there is no
// "standard mode" here — the non-approved KDF is not compiled in, so clearing
// the environment cannot select it.
func assertDefaultKDFVersionOutsideFIPS(t *testing.T) {
	t.Helper()
	if v := DefaultKDFVersion(); v != KDFPbkdf2SHA256 {
		t.Fatalf("expected KDFPbkdf2SHA256 (%d), got %d", KDFPbkdf2SHA256, v)
	}
}

// assertArgon2idBaseline pins what this build does with Argon2id when FIPS is
// not requested, which in a -tags fips build is the same as when it is: there
// is no Argon2id deriver in the binary, so DeriveKey cannot reach one. The
// registry assertion is the stronger of the two claims — it says the algorithm
// is ABSENT, not merely declined.
func assertArgon2idBaseline(t *testing.T, pass, salt []byte) {
	t.Helper()
	if _, ok := kdfDispatchers[KDFArgon2id]; ok {
		t.Fatal("a FIPS build registered an Argon2id deriver; the non-approved " +
			"KDF must not be compiled in at all")
	}
	if _, err := DeriveKey(pass, salt, KDFArgon2id, DefaultKDFParams()); !errors.Is(err, ErrKDFUnsupportedVersion) {
		t.Fatalf("DeriveKey(Argon2id) in a FIPS build = %v, want ErrKDFUnsupportedVersion", err)
	}
}

// assertArgon2idRefusedUnderFIPS is assertArgon2idBaseline in a FIPS build:
// requesting FIPS at runtime cannot make an absent algorithm any more absent.
func assertArgon2idRefusedUnderFIPS(t *testing.T, pass, salt []byte) {
	t.Helper()
	assertArgon2idBaseline(t, pass, salt)
}

// assertV2SeparatesSameKeySizeAlgorithms has no same-key-size pair to compare
// in a FIPS build: the only 32-byte algorithm left is AES-256-GCM, because the
// ChaCha20 family is not compiled in. It asserts that absence instead — a DEK
// cannot be derived for an unregistered algorithm at all, which is a stronger
// separation than deriving a different key for it.
func assertV2SeparatesSameKeySizeAlgorithms(t *testing.T, rootKey, salt, _ []byte) {
	t.Helper()
	if _, err := deriveDEK(rootKey, salt, 7, 3, AlgChaCha20Poly1305, HKDFV2); err == nil {
		t.Fatal("a FIPS build derived a DEK for ChaCha20-Poly1305; the algorithm " +
			"must not be reachable from the registry at all")
	}
}

// assertChaChaRefusedUnderFIPS pins how a -tags fips build refuses the ChaCha20
// family: not as unapproved policy but as an UNSUPPORTED identifier, because
// algorithm_chacha.go is not compiled and the registry has no entry. That
// difference is the point of the build tag — the refusal does not depend on a
// runtime check that could be misconfigured, because there is nothing to run.
func assertChaChaRefusedUnderFIPS(t *testing.T) {
	t.Helper()
	for _, alg := range []SymmetricAlgorithm{AlgChaCha20Poly1305, AlgXChaCha20Poly1305} {
		cfg := BarrierConfig{Algorithm: alg, DeploymentID: 1}
		if err := cfg.Validate(); !errors.Is(err, ErrUnsupportedAlgorithm) {
			t.Fatalf("validate(%v) in a FIPS build = %v, want ErrUnsupportedAlgorithm", alg, err)
		}
	}
}

// assertChaChaOutsideFIPS pins that clearing GOFIPS140 does not bring ChaCha20
// back in a -tags fips build. A runtime-gated implementation would reappear
// here; a compiled-out one cannot.
func assertChaChaOutsideFIPS(t *testing.T) {
	t.Helper()
	cfg := BarrierConfig{Algorithm: AlgChaCha20Poly1305, DeploymentID: 1}
	if err := cfg.Validate(); !errors.Is(err, ErrUnsupportedAlgorithm) {
		t.Fatalf("validate(ChaCha20) with GOFIPS140 cleared in a FIPS build = %v, "+
			"want ErrUnsupportedAlgorithm", err)
	}
}
