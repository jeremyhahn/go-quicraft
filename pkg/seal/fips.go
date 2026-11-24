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

// FIPSEnabled reports whether FIPS 140 mode is in effect (crypto/fips140, else GOFIPS140 other than "off"); it selects PBKDF2 and AES-256-GCM. Use BarrierConfig.RequireFIPS to require it, or -tags fips to remove the other algorithms.
func FIPSEnabled() bool {
	if fips140.Enabled() {
		return true
	}
	v := os.Getenv(envGOFIPS140)
	return v != "" && v != "off"
}
