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

package waldb

import "github.com/jeremyhahn/go-quicraft/pkg/seal"

// newTestSealStrategy returns a passphrase sealing strategy with the cheapest
// KDF this build supports, so tests do not each hard-code one that a FIPS build
// cannot compile.
//
// Argon2id here. A binary built with -tags fips has no Argon2id DERIVER — it is
// behind //go:build !fips in pkg/seal — so this constructor, which exists in
// every build, cannot succeed there and the fips variant of this file uses
// PBKDF2-SHA256 instead. Callers must have enabled seal.AllowWeakKDFParams.
func newTestSealStrategy(passphrase []byte) (*seal.SoftwareStrategy, error) {
	return seal.NewSoftwareStrategyWithParams(passphrase, 1, 64*1024, 1)
}
