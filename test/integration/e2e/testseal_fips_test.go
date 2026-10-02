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

//go:build integration && fips

package e2e_test

import (
	"context"

	"github.com/jeremyhahn/go-quicraft/pkg/seal"
)

// newTestSealStrategy returns a passphrase sealing strategy with the cheapest
// KDF this build supports. See the !fips variant of this file: Argon2id is not
// compiled into a -tags fips build, so tests seal with PBKDF2-SHA256 there.
// Callers must have enabled seal.AllowWeakKDFParams.
func newTestSealStrategy(passphrase []byte) (*seal.SoftwareStrategy, error) {
	return seal.NewSoftwareStrategyFIPS(context.Background(), passphrase, 1000)
}
