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

// The ChaCha20-Poly1305 family is not FIPS-approved, so this file and its dependency are excluded under -tags fips.

package seal

import "golang.org/x/crypto/chacha20poly1305"

const (
	// maxPlaintextChaCha is x/crypto's ~256 GiB plaintext limit for both nonce sizes.
	maxPlaintextChaCha uint64 = (1 << 38) - 64
)

// Register the ChaCha20 family. This runs after algorithmRegistry's own
// initialisation (package-level variables are initialised before init
// functions), so the map already holds the AES entries.
func init() {
	algorithmRegistry[AlgChaCha20Poly1305] = algorithmParams{
		keySize:      chacha20poly1305.KeySize,
		nonceSize:    chacha20poly1305.NonceSize,
		maxPlaintext: maxPlaintextChaCha,
		name:         "ChaCha20-Poly1305",
		newAEAD:      chacha20poly1305.New,
	}
	algorithmRegistry[AlgXChaCha20Poly1305] = algorithmParams{
		keySize:      chacha20poly1305.KeySize,
		nonceSize:    chacha20poly1305.NonceSizeX,
		maxPlaintext: maxPlaintextChaCha,
		name:         "XChaCha20-Poly1305",
		newAEAD:      chacha20poly1305.NewX,
	}
}

// softwareFallbackAlgorithm returns the algorithm SelectOptimalAlgorithm picks
// for a software-only path on a CPU without AES-NI, where ChaCha20 is both
// constant-time and considerably faster than a table-driven AES.
func softwareFallbackAlgorithm() SymmetricAlgorithm { return AlgChaCha20Poly1305 }
