// Copyright 2026 Jeremy Hahn
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package seal

import "crypto/cipher"

// Compile-time interface compliance check.
var _ AEADProvider = (*SoftwareAEADProvider)(nil)

// SoftwareAEADProvider creates cipher.AEAD instances using the standard Go
// crypto library. It delegates to the algorithm registry (newAEADForAlgorithm)
// which selects the appropriate implementation for each SymmetricAlgorithm.
//
// This is the default AEADProvider when no hardware-backed provider is
// configured in BarrierConfig.
type SoftwareAEADProvider struct{}

// NewSoftwareAEADProvider returns a new SoftwareAEADProvider.
func NewSoftwareAEADProvider() *SoftwareAEADProvider {
	return &SoftwareAEADProvider{}
}

// NewAEAD creates a cipher.AEAD for the given algorithm and key using the
// standard Go crypto library. Returns ErrUnsupportedAlgorithm if alg is not
// registered, or ErrInvalidKeySize if the key length does not match the
// algorithm's requirements.
func (p *SoftwareAEADProvider) NewAEAD(alg SymmetricAlgorithm, key []byte) (cipher.AEAD, error) {
	return newAEADForAlgorithm(alg, key)
}
