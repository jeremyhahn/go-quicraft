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

	"github.com/jeremyhahn/go-enclave"
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
// BOTH SIDES OF THE BOUNDARY ARE PROTECTED. A protected root-key API that still
// accepted the passphrase in an ordinary []byte would protect the key it derives
// while leaving the material that derives it collectable and swappable — and for
// a deferred-passphrase strategy the passphrase IS the root key's protection.
//
// That used to require a SECOND credential type, because Credentials.Secret was
// a []byte and these two methods were the only ones that could afford to break.
// #41 retyped Credentials.Secret itself, so there is now exactly ONE credential
// type in this package and these methods take it. ProtectedCredentials survives
// only as a deprecated alias for the implementations that still name it; an
// interface carrying two credential types was the defect #41 was filed against,
// not a shape worth keeping.
//
// # What this does NOT change
//
// The Barrier still calls SealingStrategy.SealRootKey and UnsealRootKey. It
// needs the raw key bytes for HKDF epoch-DEK derivation and for the sealed-state
// HMAC, holds them in a locked enclave for the whole of Initialize and Unseal
// (#39), and then seals them into an enclave.SealedEnclave for the barrier's
// lifetime. Switching the barrier itself onto the protected PAIR is a further
// change and is deliberately not made here: the enclave it holds is filled in
// place and its last act is to be consumed by enclave.NewSealed, which wipes what
// it is handed — so those pages must stay WRITABLE, where this interface's
// contract hands back a frozen one. What this interface buys today is that an
// implementer no longer has to RETAIN an unprotected copy between calls, which is
// the defect that was actually reported.
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
	SealRootKeyProtected(ctx context.Context, rootKey *enclave.MemoryEnclave, creds Credentials) (*SealedRootKey, error)

	// UnsealRootKeyProtected decrypts the root key into locked memory and
	// returns the enclave holding it. It is UnsealRootKey with the output on
	// the protected side: the plaintext key is written into the enclave by the
	// AEAD itself, so it never exists as an ordinary heap allocation at all.
	//
	// The caller OWNS the returned enclave and MUST Destroy it — on every
	// path, including the error paths of whatever it does next. Locked memory
	// is a finite PROCESS-WIDE resource: an enclave that is never destroyed
	// holds a whole locked page against RLIMIT_MEMLOCK for the life of the
	// process, and the next caller to reach the ceiling is refused with
	// ErrLockedMemoryUnavailable. Under memguard the bill was steeper still —
	// a refused lock terminated the process — so the duty is less dire than it
	// was and no less real. The enclave is returned FROZEN: its contents are
	// final, a stray write through a retained slice faults rather than
	// corrupting the root key, and Destroy is what releases them.
	UnsealRootKeyProtected(ctx context.Context, sealed *SealedRootKey, creds Credentials) (*enclave.MemoryEnclave, error)
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

// ProtectedCredentials is [Credentials].
//
// Deprecated: use Credentials. The two were separate types only while
// Credentials.Secret was an ordinary []byte — this one existed so the protected
// root-key pair above could refuse a passphrase in collectable memory while the
// []byte siblings kept compiling. #41 retyped Credentials.Secret onto the same
// [enclave.SealedEnclave], so the distinction has nothing left to express.
//
// It is an ALIAS, not a defined type, so an implementation whose method
// signatures still say ProtectedCredentials satisfies ProtectedSealingStrategy
// unchanged. It is kept for exactly that reason and goes at the next breaking
// release; new code names Credentials.
type ProtectedCredentials = Credentials

// NewProtectedCredentials calls [NewCredentials], including its contract:
// the caller's slice is CONSUMED.
//
// Deprecated: use NewCredentials. Same function, and the name no longer
// distinguishes anything now that there is one credential type.
//
// It is a FUNCTION, not `var NewProtectedCredentials = NewCredentials`. A
// deprecation shim is the one place where the wrapper costs nothing, and an
// exported function VARIABLE in a crypto package is writable by every package
// that can import this one: an init() anywhere in a consumer's transitive graph
// could reassign it and route every passphrase still reaching the deprecated
// name through its own code, with no call site changed to notice (#95). That
// this was continuity from the NewProtectedKey/SealProtectedBuffer vars in the
// deleted memprotect.go is the reason it was written, not a reason to keep it.
// staticcheck's SA1019 flags a deprecated func exactly as it flags a deprecated
// var, so the deprecation still lands on consumers.
func NewProtectedCredentials(secret []byte) (Credentials, error) {
	return NewCredentials(secret)
}
