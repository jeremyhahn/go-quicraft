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
	"crypto/fips140"
	"os"
)

const (
	// envGOFIPS140 is the environment variable that controls FIPS 140 mode.
	envGOFIPS140 = "GOFIPS140"
)

// FIPSEnabled reports whether FIPS 140 mode is in effect. Detection is
// keyed primarily off the Go runtime's FIPS 140-3 module state
// (crypto/fips140.Enabled), with the GOFIPS140 environment variable as a
// fallback for deployments that signal FIPS intent via the environment;
// the explicit value "off" is honored as disabled rather than being
// misread as enabled.
//
// When enabled, FIPS mode constrains BOTH the KDF and the data-path cipher:
//
//   - KDF: DefaultKDFVersion selects PBKDF2-SHA256, and DeriveKey (plus the
//     software-strategy constructors) REFUSE Argon2id with
//     ErrKDFNotFIPSApproved — including the explicitly-Argon2id constructor
//     NewSoftwareStrategyWithParams. See kdf.go.
//   - Cipher: SelectOptimalAlgorithm returns AES-256-GCM unconditionally, and
//     BarrierConfig.Validate rejects the ChaCha20/XChaCha20 algorithms, which
//     are not FIPS-approved.
//
// Detection is ENVIRONMENTAL, so a false return means either "FIPS was not
// requested" or "FIPS was requested but this binary has no module" — and the
// second would otherwise fail open onto Argon2id and ChaCha20-Poly1305. A
// deployment that requires FIPS must say so with BarrierConfig.RequireFIPS,
// which turns that ambiguity into a refusal at NewBarrier.
//
// THIS FUNCTION IS THE RUNTIME POLICY, NOT THE STRONGEST FORM. Everything it
// governs is a check standing between a caller and an algorithm that is still
// compiled into the binary. A build with -tags fips removes the algorithms
// instead: Argon2id (kdf_argon2.go) and the ChaCha20 family
// (algorithm_chacha.go) are excluded from compilation, so neither this
// detection nor any configuration can reach them, and the refusals become
// ErrKDFUnsupportedVersion and ErrUnsupportedAlgorithm. The EXPORTED SURFACE
// does not move with the tag — NewSoftwareStrategyWithParams still exists in a
// FIPS build and still compiles at every call site; it simply cannot succeed,
// because the deriver it names is not there. The AES-GCM data path is FIPS-approved in EVERY
// build — newAESGCM uses cipher.NewGCMWithRandomNonce unconditionally, because
// a module-generated IV is required (SP 800-38D) and costs a non-FIPS
// deployment nothing. See docs/security/encryption.md ("FIPS builds").
func FIPSEnabled() bool {
	if fips140.Enabled() {
		return true
	}
	v := os.Getenv(envGOFIPS140)
	return v != "" && v != "off"
}
