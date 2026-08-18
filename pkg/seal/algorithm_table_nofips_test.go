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

package seal

import "golang.org/x/crypto/chacha20poly1305"

// knownAlgorithms enumerates every SymmetricAlgorithm a non-FIPS build
// registers — all five — so table-driven subtests can iterate over them
// without repetition.
var knownAlgorithms = []algorithmCase{
	{alg: AlgAES128GCM, name: "AES-128-GCM", keySize: 16, nonceSize: 12, maxPlaintext: maxPlaintextGCM, moduleNonce: true},
	{alg: AlgAES192GCM, name: "AES-192-GCM", keySize: 24, nonceSize: 12, maxPlaintext: maxPlaintextGCM, moduleNonce: true},
	{alg: AlgAES256GCM, name: "AES-256-GCM", keySize: 32, nonceSize: 12, maxPlaintext: maxPlaintextGCM, moduleNonce: true},
	{alg: AlgChaCha20Poly1305, name: "ChaCha20-Poly1305",
		keySize: chacha20poly1305.KeySize, nonceSize: chacha20poly1305.NonceSize,
		maxPlaintext: maxPlaintextChaCha},
	{alg: AlgXChaCha20Poly1305, name: "XChaCha20-Poly1305",
		keySize: chacha20poly1305.KeySize, nonceSize: chacha20poly1305.NonceSizeX,
		maxPlaintext: maxPlaintextChaCha},
}

// unregisteredAlgorithms are identifiers this build does NOT register. A
// non-FIPS build registers everything it defines, so the set is empty; the
// variable exists so the shared tests can assert the FIPS build's exclusions
// without a build-tag branch of their own.
var unregisteredAlgorithms []SymmetricAlgorithm
