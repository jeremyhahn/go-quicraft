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

import "os"

const (
	// envGOFIPS140 is the environment variable that controls FIPS 140 mode.
	envGOFIPS140 = "GOFIPS140"
)

// FIPSEnabled returns true when the GOFIPS140 environment variable is set
// to a non-empty value. When enabled, FIPS-compliant algorithms (PBKDF2-SHA256)
// are used instead of Argon2id for key derivation.
func FIPSEnabled() bool {
	return os.Getenv(envGOFIPS140) != ""
}
