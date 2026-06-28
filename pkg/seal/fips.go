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
// When enabled, FIPS mode constrains BOTH the KDF (PBKDF2-SHA256 instead
// of Argon2id, see kdf.go) and the data-path cipher: SelectOptimalAlgorithm
// returns AES-256-GCM unconditionally, and BarrierConfig.validate rejects
// the ChaCha20/XChaCha20 algorithms, which are not FIPS-approved.
func FIPSEnabled() bool {
	if fips140.Enabled() {
		return true
	}
	v := os.Getenv(envGOFIPS140)
	return v != "" && v != "off"
}
