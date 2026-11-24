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

import (
	"context"
	"time"

	"github.com/jeremyhahn/go-enclave"
)

// NoneStrategy disables barrier encryption, for storage already encrypted at rest; it is selected explicitly with --barrier-strategy=none.
type NoneStrategy struct{}

// NewNoneStrategy returns a NoneStrategy. It takes no parameters because the
// strategy has no key material, no credentials, and no external dependencies.
func NewNoneStrategy() *NoneStrategy { return &NoneStrategy{} }

// ID returns StrategyNone.
func (s *NoneStrategy) ID() StrategyID { return StrategyNone }

// Available always returns true: NoneStrategy has no dependencies that can
// fail.
func (s *NoneStrategy) Available() bool { return true }

// HardwareBacked returns false: NoneStrategy uses no hardware (and no
// encryption at all).
func (s *NoneStrategy) HardwareBacked() bool { return false }

// BarrierEncryptor returns a non-aliasing identity encrypter that writes an 8-byte zero epoch prefix.
func (s *NoneStrategy) BarrierEncryptor(_ context.Context, _ []byte) (SymmetricEncrypter, error) {
	return noneEncrypter{}, nil
}

// SealRootKey returns a StrategyNone marker; rootKey is not stored.
func (s *NoneStrategy) SealRootKey(_ context.Context, _ []byte, _ Credentials) (*SealedRootKey, error) {
	// Fill Epoch, Salt and KnownEpochs so the marker passes SetSealedRootKey's validation.
	return &SealedRootKey{
		Strategy:    StrategyNone,
		Version:     1,
		CreatedAt:   time.Now().UTC(),
		Epoch:       1, // matches Barrier.Initialize's initialEpoch
		Salt:        make([]byte, hkdfSaltSize),
		KnownEpochs: []uint64{1},
		HKDFVersion: currentHKDFVersion,
	}, nil
}

// UnsealRootKey returns an empty key; sealed must be tagged StrategyNone.
func (s *NoneStrategy) UnsealRootKey(_ context.Context, sealed *SealedRootKey, _ Credentials) ([]byte, error) {
	if sealed == nil {
		return nil, ErrInvalidSealedKey
	}
	if sealed.Strategy != StrategyNone {
		return nil, ErrStrategyMismatch
	}
	return []byte{}, nil
}

// SealRootKeyProtected returns the SealRootKey marker after checking rootKey's size, without reading or destroying it.
func (s *NoneStrategy) SealRootKeyProtected(ctx context.Context, rootKey enclave.ByteAccess, _ Credentials) (*SealedRootKey, error) {
	if rootKey == nil || rootKey.Size() != rootKeySize {
		return nil, ErrInvalidRootKeySize
	}
	// Credentials are ignored in either form: this strategy wraps nothing, so it
	// derives nothing and has no use for a passphrase.
	return s.SealRootKey(ctx, nil, Credentials{})
}

// UnsealRootKeyProtected validates sealed, then returns ErrNoRootKeyAvailable.
func (s *NoneStrategy) UnsealRootKeyProtected(ctx context.Context, sealed *SealedRootKey, _ Credentials) (enclave.ByteAccess, error) {
	if _, err := s.UnsealRootKey(ctx, sealed, Credentials{}); err != nil {
		return nil, err
	}
	return nil, ErrNoRootKeyAvailable
}

// Close is a no-op for NoneStrategy.
func (s *NoneStrategy) Close() error { return nil }

// noneEpochHeaderSize is the 8-byte zero epoch prefix that keeps the barrier wire format.
const noneEpochHeaderSize = 8

// noneEncrypter copies data into fresh buffers behind an 8-byte zero epoch prefix.
type noneEncrypter struct{}

// Encrypt returns a new slice: an 8-byte zero epoch prefix followed by a copy
// of plaintext. It never aliases or retains the input.
func (noneEncrypter) Encrypt(plaintext []byte) ([]byte, error) {
	out := make([]byte, noneEpochHeaderSize+len(plaintext))
	// out[:noneEpochHeaderSize] stays zero == sentinel epoch 0 (big-endian).
	copy(out[noneEpochHeaderSize:], plaintext)
	return out, nil
}

// Decrypt strips the epoch prefix and returns a copy, failing on a non-zero epoch written by a real strategy.
func (noneEncrypter) Decrypt(ciphertext []byte) ([]byte, error) {
	if len(ciphertext) < noneEpochHeaderSize {
		return nil, ErrCiphertextTooShort
	}
	for _, b := range ciphertext[:noneEpochHeaderSize] {
		if b != 0 {
			return nil, ErrNoneStrategyEncryptedData
		}
	}
	payload := ciphertext[noneEpochHeaderSize:]
	out := make([]byte, len(payload))
	copy(out, payload)
	return out, nil
}

// Compile-time interface assertions.
var (
	_ SealingStrategy    = (*NoneStrategy)(nil)
	_ SymmetricEncrypter = noneEncrypter{}
)
