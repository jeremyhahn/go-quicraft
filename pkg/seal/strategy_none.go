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

	"github.com/jeremyhahn/go-quicraft/pkg/enclave"
)

// NoneStrategy disables barrier encryption: SealRootKey records a marker
// SealedRootKey with no ciphertext, UnsealRootKey returns an empty key, and
// BarrierEncryptor returns an identity SymmetricEncrypter that passes bytes
// through unchanged. Operators select this strategy explicitly via
// --barrier-strategy=none; it is never auto-selected.
//
// Use cases: development, integration testing, and deployments where the
// underlying storage already provides at-rest encryption (OS full-disk
// encryption, encrypted block devices, encrypted object stores). In those
// cases barrier-level encryption is redundant.
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

// BarrierEncryptor returns a no-encryption SymmetricEncrypter. The rootKey
// argument is ignored because no key material is needed. The returned
// encrypter does NOT alias caller buffers and preserves the barrier wire-format
// contract (an 8-byte epoch prefix), so downstream readers that parse the
// leading epoch (WAL, snapshots) see a well-formed sentinel epoch of 0.
func (s *NoneStrategy) BarrierEncryptor(_ context.Context, _ []byte) (SymmetricEncrypter, error) {
	return noneEncrypter{}, nil
}

// SealRootKey returns a SealedRootKey marker tagged with StrategyNone. No
// encryption is performed; the rootKey is not stored. Callers persist the
// marker so that subsequent Unseal calls know the system was configured for
// no-encryption mode.
func (s *NoneStrategy) SealRootKey(_ context.Context, _ []byte, _ Credentials) (*SealedRootKey, error) {
	// Emit a self-consistent marker that satisfies SetSealedRootKey's structural
	// validation (Epoch > 0, 32-byte Salt, KnownEpochs contains Epoch) so the
	// marker round-trips through persistence even when a caller stores this raw
	// output directly rather than via Barrier.Initialize (which would otherwise
	// overwrite these fields). None mode ignores the epoch and salt entirely —
	// the identity encrypter uses no key material — so they exist only so the
	// generic sealed-key loader accepts the blob.
	return &SealedRootKey{
		Strategy:    StrategyNone,
		Version:     1,
		CreatedAt:   time.Now().UTC(),
		Epoch:       1, // matches Barrier.Initialize's initialEpoch
		Salt:        make([]byte, hkdfSaltSize),
		KnownEpochs: []uint64{1},
	}, nil
}

// UnsealRootKey returns an empty byte slice. The barrier never invokes the
// returned key for cryptographic operations because BarrierEncryptor
// supplies an identity encrypter that ignores keys entirely. The strategy
// argument on the SealedRootKey must match StrategyNone.
func (s *NoneStrategy) UnsealRootKey(_ context.Context, sealed *SealedRootKey, _ Credentials) ([]byte, error) {
	if sealed == nil {
		return nil, ErrInvalidSealedKey
	}
	if sealed.Strategy != StrategyNone {
		return nil, ErrStrategyMismatch
	}
	return []byte{}, nil
}

// SealRootKeyProtected records the same marker SealRootKey does. The enclave's
// CONTENTS are never read — no-encryption mode wraps nothing — and it is NOT
// destroyed either: ownership of a protected root key stays with the caller in
// this interface, and a strategy that silently released one would break a caller
// that seals with several strategies in turn.
//
// It is still VALIDATED, and that is a deliberate asymmetry with SealRootKey,
// which accepts any slice including nil. The protected contract says a nil or
// destroyed enclave is an error rather than an empty key, and a contract the
// reference implementation ignores is not a contract. The failure this catches
// is real: a caller that destroyed or lost its root key before sealing would
// otherwise be handed a valid marker and a success, and would learn nothing
// until an unseal it can no longer perform. Size alone answers all three cases
// — nil, destroyed and wrong-sized all report a size that is not rootKeySize —
// without reading a byte of the key.
func (s *NoneStrategy) SealRootKeyProtected(ctx context.Context, rootKey *enclave.MemoryEnclave, _ ProtectedCredentials) (*SealedRootKey, error) {
	if rootKey.Size() != rootKeySize {
		return nil, ErrInvalidRootKeySize
	}
	// Credentials are ignored in either form: this strategy wraps nothing, so it
	// derives nothing and has no use for a passphrase.
	return s.SealRootKey(ctx, nil, Credentials{})
}

// UnsealRootKeyProtected returns ErrNoRootKeyAvailable. This is the one place
// the protected pair deliberately does NOT mirror its string-typed sibling:
// UnsealRootKey returns an empty []byte because the barrier's no-encryption
// branch wants a length-zero key it can carry through the same code path, and
// there is no such thing as a protected buffer holding nothing — memguard
// refuses a zero-length allocation by terminating the process, and an enclave
// that "held" an empty key would be a protected wrapper around a fiction.
//
// So the honest answer to "give me the root key, protected" in no-encryption
// mode is that there is no root key, which is exactly what ErrNoRootKeyAvailable
// says and what Barrier.RootKey already returns in this mode. A caller reaching
// for the protected path in None mode has a bug — it is about to use a key that
// does not exist — and a typed error at that point is worth more than a
// zero-length success it would carry two layers further.
//
// The sealed blob is still validated first, so a mismatched or nil blob reports
// the same error it does through UnsealRootKey rather than being masked by this
// one.
func (s *NoneStrategy) UnsealRootKeyProtected(ctx context.Context, sealed *SealedRootKey, _ ProtectedCredentials) (*enclave.MemoryEnclave, error) {
	if _, err := s.UnsealRootKey(ctx, sealed, Credentials{}); err != nil {
		return nil, err
	}
	return nil, ErrNoRootKeyAvailable
}

// Close is a no-op for NoneStrategy.
func (s *NoneStrategy) Close() error { return nil }

// noneEpochHeaderSize is the size of the epoch prefix the no-encryption
// encrypter prepends so its output matches the barrier wire-format contract,
// which begins with an 8-byte big-endian epoch. Downstream readers (the WAL
// and snapshot epoch parsers) read these leading bytes; NoneStrategy always
// writes 0, signalling "no DEK / no epoch tracking". This keeps the format
// invariant intact instead of forcing mode checks throughout storage.
const noneEpochHeaderSize = 8

// noneEncrypter is the no-encryption barrier encrypter. It performs no
// cryptography but, unlike a raw passthrough, it (a) returns freshly allocated
// buffers — never aliasing the caller's input, matching the ownership contract
// every other SymmetricEncrypter honors — and (b) prepends an 8-byte zero
// epoch so ciphertext is wire-format compatible with epoch-aware readers.
type noneEncrypter struct{}

// Encrypt returns a new slice: an 8-byte zero epoch prefix followed by a copy
// of plaintext. It never aliases or retains the input.
func (noneEncrypter) Encrypt(plaintext []byte) ([]byte, error) {
	out := make([]byte, noneEpochHeaderSize+len(plaintext))
	// out[:noneEpochHeaderSize] stays zero == sentinel epoch 0 (big-endian).
	copy(out[noneEpochHeaderSize:], plaintext)
	return out, nil
}

// Decrypt strips the epoch prefix and returns a fresh copy of the payload.
//
// A non-zero epoch prefix means the data was written by a REAL sealing
// strategy (the epoch is a live DEK epoch), not by NoneStrategy. Decoding
// it as plaintext would silently return ciphertext-as-plaintext garbage —
// there is no AEAD tag in None mode to fail on. This happens when a
// barrier is misconfigured to None after previously running encrypted, so
// fail loudly instead of corrupting reads.
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
