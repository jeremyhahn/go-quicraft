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

//go:build fips

package seal

// knownAlgorithms enumerates every SymmetricAlgorithm a FIPS build registers.
// The ChaCha20 family is absent because algorithm_chacha.go is not compiled
// here; that absence is asserted directly by unregisteredAlgorithms.
var knownAlgorithms = []algorithmCase{
	{alg: AlgAES128GCM, name: "AES-128-GCM", keySize: 16, nonceSize: 12, maxPlaintext: maxPlaintextGCM, moduleNonce: true},
	{alg: AlgAES192GCM, name: "AES-192-GCM", keySize: 24, nonceSize: 12, maxPlaintext: maxPlaintextGCM, moduleNonce: true},
	{alg: AlgAES256GCM, name: "AES-256-GCM", keySize: 32, nonceSize: 12, maxPlaintext: maxPlaintextGCM, moduleNonce: true},
}

// unregisteredAlgorithms are the non-approved identifiers a FIPS build defines
// as wire values but refuses to construct a cipher for.
var unregisteredAlgorithms = []SymmetricAlgorithm{
	AlgChaCha20Poly1305,
	AlgXChaCha20Poly1305,
}
