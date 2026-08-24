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

	"github.com/jeremyhahn/go-quicraft/pkg/enclave"
)

// ProtectedSealingStrategy is the protected-memory counterpart of the root-key
// half of SealingStrategy: the same two operations, with the key on the
// protected side of the boundary in both directions.
//
// It exists because SealingStrategy's root-key signatures force an
// implementation to hold the most sensitive value in the system — the barrier
// root key — in an ordinary Go allocation, and no amount of care on the
// implementer's side can avoid that while the signature demands a []byte.
// go-qrdb reported the concrete shape (go-qrdb PR #76, Codex round 4): its
// recovery strategy reconstructs the root key into an enclave and then has to
// copy it into a plain []byte field purely to be able to return it from
// UnsealRootKey, so the key sits in collectable, swappable memory for the whole
// barrier-unseal operation and is wiped only at Close. The review's own
// conclusion was that the fix belongs here rather than there — "if
// go-quicraft's SealingStrategy interface necessarily returns an unprotected
// key, extend/fix that lower-layer interface rather than asserting end-to-end
// protected custody" — and this interface is that extension.
//
// It is a SIBLING, not a replacement, and deliberately so. SealingStrategy
// stays exactly as it is: every strategy in the portfolio (go-xkms's TPM2,
// PKCS#11 and cloud-KMS strategies, go-qrdb's shamir and recovery strategies,
// and the two in this package) keeps compiling unchanged, and an implementation
// adopts the protected pair when it has something to gain from it. A caller
// that wants the protected path type-asserts for this interface; there is no
// silent fallback, because a fallback would be the un-protected copy wearing
// the protected API's name.
//
// BOTH SIDES OF THE BOUNDARY ARE PROTECTED, which is why these methods take
// ProtectedCredentials rather than Credentials. A protected root-key API that
// still accepted the passphrase in an ordinary []byte would protect the key it
// derives while leaving the material that derives it collectable and swappable
// — and for a deferred-passphrase strategy the passphrase IS the root key's
// protection. There is deliberately no overload taking Credentials: an
// implementation that converted one to the other would reintroduce exactly the
// plain copy this signature exists to remove.
//
// # What this does NOT change
//
// The Barrier still calls SealingStrategy.SealRootKey and UnsealRootKey. It
// needs the raw key bytes for HKDF epoch-DEK derivation and for the sealed-state
// HMAC, wipes them on every path (success and failure alike), and then holds the
// key in an enclave.ProtectedKey for its lifetime. Switching the barrier itself
// onto the protected pair is a larger change than this interface — the barrier's
// own root-key handling is []byte end to end, and an enclave frozen read-only
// would fault under the wipeBytes calls that discipline depends on — so it is
// tracked separately (#39) rather than half-done here. What this interface buys
// today is that an implementer no longer has to RETAIN an unprotected copy
// between calls, which is the defect that was actually reported.
type ProtectedSealingStrategy interface {
	SealingStrategy

	// SealRootKeyProtected encrypts a root key held in protected memory and
	// returns the SealedRootKey to persist. It is SealRootKey with the input
	// on the protected side: the strategy borrows the enclave for exactly as
	// long as the encryption takes and never copies the key out of it.
	//
	// The caller retains ownership of rootKey — this method neither destroys
	// it nor takes it over. Passing a nil or destroyed enclave is an error,
	// not an empty key.
	SealRootKeyProtected(ctx context.Context, rootKey *enclave.MemoryEnclave, creds ProtectedCredentials) (*SealedRootKey, error)

	// UnsealRootKeyProtected decrypts the root key into locked memory and
	// returns the enclave holding it. It is UnsealRootKey with the output on
	// the protected side: the plaintext key is written into the enclave by the
	// AEAD itself, so it never exists as an ordinary heap allocation at all.
	//
	// The caller OWNS the returned enclave and MUST Destroy it — on every
	// path, including the error paths of whatever it does next. Locked memory
	// is a finite kernel resource whose exhaustion memguard answers by
	// terminating the process. The enclave is returned frozen: its contents
	// are final, and Destroy is what releases them.
	UnsealRootKeyProtected(ctx context.Context, sealed *SealedRootKey, creds ProtectedCredentials) (*enclave.MemoryEnclave, error)
}

// Compile-time assertions that both strategies this package implements satisfy
// the protected pair. A strategy that implements SealingStrategy but not this
// interface is legitimate — the interface is optional by design — but these two
// are the reference implementations consumers read, so drift in either is a
// build failure here rather than a surprise in go-qrdb.
var (
	_ ProtectedSealingStrategy = (*SoftwareStrategy)(nil)
	_ ProtectedSealingStrategy = (*NoneStrategy)(nil)
)

// ProtectedCredentials is Credentials with the secret in protected memory: the
// authentication material a deferred-passphrase strategy needs at call time,
// held in an enclave.ProtectedKey — memguard-encrypted at rest, decrypted into
// a locked, guard-paged buffer only for the moment a KDF reads it.
//
// It exists because Credentials.Secret is an ordinary []byte, so the passphrase
// that protects the root key lives in collectable, swappable memory that this
// package can only ask the caller to wipe. For a deferred strategy that
// passphrase is not a lesser secret than the key it wraps — it IS the key's
// protection — so a protected root-key API that accepted an unprotected
// passphrase would be protecting the wrong half (Codex review of PR #40).
//
// The zero value is valid and means "no credentials", which is what a
// passphrase-bound strategy expects: it uses the passphrase given at
// construction and ignores this entirely.
//
// The CALLER owns Secret. This package opens it, derives from it and destroys
// the opened copy; it never clears the ProtectedKey itself, so one credential
// can be used for many calls and is released by the caller's own Clear.
type ProtectedCredentials struct {
	// Secret is the passphrase, encrypted at rest in memory. Nil means the
	// caller supplied none.
	Secret *enclave.ProtectedKey // #nosec G117 -- this IS a secret field by design
}

// NewProtectedCredentials moves a secret into protected memory and returns the
// credentials holding it. The source slice is WIPED — that is the whole point,
// and it is why this constructor exists rather than leaving callers to build the
// struct: material that reaches here already existed unprotected, and the only
// thing left worth doing about it is not leaving it there.
//
// An empty secret is refused rather than stored. A zero-length passphrase is not
// a passphrase, and memguard answers a zero-length allocation by terminating the
// process.
func NewProtectedCredentials(secret []byte) (ProtectedCredentials, error) {
	if len(secret) == 0 {
		return ProtectedCredentials{}, ErrInvalidPassphrase
	}
	// The recommendation is warned about HERE, at construction, exactly as
	// newSoftwareStrategy warns for a bound passphrase: the earliest point an
	// operator can act on, and the only point at which the length is known
	// without opening the enclave again.
	warnIfPassphraseWeak(len(secret))
	return ProtectedCredentials{Secret: enclave.NewProtectedKey(secret)}, nil
}
