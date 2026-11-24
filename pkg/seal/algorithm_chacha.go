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

// The ChaCha20-Poly1305 family is not FIPS-approved, so this file — and with it
// the golang.org/x/crypto/chacha20poly1305 dependency — is excluded from a
// binary built with -tags fips. Nothing about that exclusion is advisory: the
// algorithms are absent from algorithmRegistry, which is the only place a
// SymmetricAlgorithm becomes an executable cipher, so a FIPS build CANNOT
// perform ChaCha20-Poly1305 rather than merely declining to. That is the whole
// point of the tag over a runtime check — a runtime check leaves the primitive
// in the shipped artefact and asks an auditor to trust a branch.

package seal

import "golang.org/x/crypto/chacha20poly1305"

const (
	// maxPlaintextChaCha is the ChaCha20-Poly1305 ceiling used by x/crypto for
	// BOTH the 12-byte-nonce and the extended-nonce (X) construction, ~256 GiB.
	// x/crypto panics with "chacha20poly1305: plaintext too large" beyond it.
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
