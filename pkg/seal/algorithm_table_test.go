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

import "testing"

// aeadTagSize is the authentication tag every algorithm in this package uses:
// 16 bytes, GCM and Poly1305 alike.
const aeadTagSize = 16

// algorithmCase describes one registered SymmetricAlgorithm for the
// table-driven algorithm tests. The set of cases is build-tag dependent — see
// algorithm_table_nofips_test.go and algorithm_table_fips_test.go — because a
// FIPS build does not register the ChaCha20 family at all.
type algorithmCase struct {
	alg  SymmetricAlgorithm
	name string

	keySize int

	// nonceSize is the WIRE nonce width, i.e. what SymmetricAlgorithm.NonceSize
	// reports. It is not always the cipher.AEAD's NonceSize; see moduleNonce.
	nonceSize int

	// maxPlaintext is the algorithm's single-invocation plaintext ceiling, the
	// value SymmetricAlgorithm.MaxPlaintextSize reports.
	maxPlaintext uint64

	// moduleNonce marks an algorithm whose AEAD generates its own nonce inside
	// the FIPS module boundary and prepends it to its own Seal output
	// (cipher.NewGCMWithRandomNonce). Such an AEAD reports NonceSize() == 0,
	// counts the nonce in Overhead(), and PANICS if handed a non-empty nonce.
	moduleNonce bool
}

// wantAEADNonceSize is the cipher.AEAD.NonceSize() this case's AEAD must report.
func (tc algorithmCase) wantAEADNonceSize() int {
	if tc.moduleNonce {
		return 0
	}
	return tc.nonceSize
}

// wantAEADOverhead is the cipher.AEAD.Overhead() this case's AEAD must report:
// the tag alone, plus the nonce when the AEAD carries its own.
func (tc algorithmCase) wantAEADOverhead() int {
	if tc.moduleNonce {
		return tc.nonceSize + aeadTagSize
	}
	return aeadTagSize
}

// sealNonce returns the nonce to pass to Seal/Open for this case: nil for a
// module-nonce AEAD, which requires an empty nonce, and a fresh random nonce of
// the algorithm's width otherwise.
func (tc algorithmCase) sealNonce(t *testing.T) []byte {
	t.Helper()
	if tc.moduleNonce {
		return nil
	}
	return randKey(t, tc.nonceSize)
}
