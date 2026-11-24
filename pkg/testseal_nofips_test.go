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

package quicraft

import (
	"context"

	"github.com/jeremyhahn/go-quicraft/pkg/seal"
)

// newTestSealStrategy returns a passphrase strategy with this build's cheapest KDF (Argon2id; the fips variant uses PBKDF2-SHA256); callers must enable seal.AllowWeakKDFParams.
func newTestSealStrategy(passphrase []byte) (*seal.SoftwareStrategy, error) {
	return seal.NewSoftwareStrategyWithParams(context.Background(), passphrase, 1, 64*1024, 1)
}
