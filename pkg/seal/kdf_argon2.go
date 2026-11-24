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

// Argon2id is not FIPS-approved, so this file and its dependency are excluded from -tags fips builds.

package seal

import "golang.org/x/crypto/argon2"

const (
	// Production floors for the Argon2id work factors. See the floor block in
	// kdf.go for why a caller-supplied near-trivial cost must be refused.
	minArgon2Time      = 2
	minArgon2MemoryKiB = 32 * 1024 // 32 MiB
	minArgon2Threads   = 1
)

// Register Argon2id as a deriver and its work-factor floor. This runs after
// kdfDispatchers and kdfFloorChecks are initialised, because package-level
// variables are initialised before init functions.
func init() {
	kdfDispatchers[KDFArgon2id] = deriveArgon2id
	kdfFloorChecks[KDFArgon2id] = func(params KDFParams) bool {
		return params.Time < minArgon2Time ||
			params.Memory < minArgon2MemoryKiB ||
			params.Threads < minArgon2Threads
	}
}

// deriveArgon2id derives a key using the Argon2id algorithm.
func deriveArgon2id(passphrase, salt []byte, params KDFParams) ([]byte, error) {
	key := argon2.IDKey(passphrase, salt, params.Time, params.Memory, params.Threads, params.KeyLen)
	return key, nil
}

// defaultStandardKDFVersion is the KDF DefaultKDFVersion selects when FIPS mode
// is not in effect.
func defaultStandardKDFVersion() KDFVersion { return KDFArgon2id }
