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

// Package enclave provides userspace memory-protected containers for sensitive
// cryptographic key material, built on memguard. ProtectedKey keeps a key
// encrypted at rest in ordinary process memory via memguard.Enclave;
// MemoryEnclave is the region of locked memory itself — an mlock'd,
// guard-paged buffer for the window in which the plaintext key must actually
// be used.
//
// # Vocabulary
//
// MemoryEnclave is the portfolio-wide name for "a region of locked, zeroized
// memory" (owner ruling, 2026-08-18). It replaces this package's original
// ProtectedBuffer, which survives only as a deprecated alias so that consumers
// compiled against the old name keep building until the coordinated
// bottom-up rename wave. Do not introduce Secret, SecureString, SecureBytes or
// another local synonym for this concept: one name across the stack is what
// makes the protected-memory audit a grep rather than a re-reading of nine
// codebases.
//
// # The name is not a hardware enclave
//
// "Enclave" here is memguard's term for an encrypted in-memory container. It is
// NOT a TEE, not SGX/SEV/TrustZone, and not a TPM or HSM boundary: everything
// this package protects lives in the address space of this process, protected
// by this process, and any code running with the ability to read this process's
// memory — a debugger, a live core read, another goroutine with a stray slice —
// defeats it. Hardware key custody in this stack belongs to go-xkms, where the
// key never leaves the device. Nothing here should be read as offering that
// property.
//
// # What is actually guaranteed
//
//   - MemoryEnclave: memory is mlock'd (excluded from swap), fenced by guard
//     pages, and canary-checked, and Destroy WIPES it before unmapping. That
//     wipe is real and is the strongest guarantee in this package.
//   - ProtectedKey: the key material is AEAD-encrypted under a per-process
//     session key that memguard itself keeps in a locked buffer. A memory
//     disclosure that captures only the enclave's ciphertext yields nothing
//     without that session key, which is never written out and dies with the
//     process.
//   - Plaintext exposure is bounded to Open/Destroy windows, during which the
//     key lives only in a MemoryEnclave.
//
// # What is NOT guaranteed
//
//   - Clear does NOT wipe. It drops the enclave reference so the key can no
//     longer be opened; the enclave's ciphertext is an ordinary heap slice that
//     memguard exposes no API to zero, and it registers no finalizer, so those
//     bytes persist until the garbage collector reclaims them. See
//     ProtectedKey.Clear.
//   - Nothing here survives SIGKILL, a hypervisor snapshot, or a cold-boot
//     attack on the machine.
//   - Derived copies made by consumers are outside this boundary. A cipher.AEAD
//     built from an opened key holds the key in its own expanded key schedule,
//     and neither the standard library nor x/crypto offers a way to wipe it.
package enclave

import (
	"fmt"
	"sync/atomic"

	"github.com/awnumar/memguard"
)

// ErrEnclaveNil is returned when Open is called on a ProtectedKey whose
// enclave has not been initialized or has been explicitly cleared.
var ErrEnclaveNil = &NilError{}

// NilError is the typed error for nil enclave access attempts.
// It is returned when Open is called on a ProtectedKey whose enclave
// has not been initialized or has been explicitly cleared.
type NilError struct{}

// Error implements the error interface.
func (e *NilError) Error() string {
	return "memprotect: enclave is nil"
}

// OpenError is returned when memguard fails to open an enclave.
type OpenError struct {
	Err error
}

// Error returns a human-readable description of the open failure.
func (e *OpenError) Error() string {
	return fmt.Sprintf("memprotect: failed to open enclave: %v", e.Err)
}

// Unwrap returns the underlying error.
func (e *OpenError) Unwrap() error {
	return e.Err
}

// MemoryEnclave is a region of locked, zeroized memory: it wraps
// memguard.LockedBuffer for consistent memory protection across all platforms,
// providing guard pages, core dump prevention, and guaranteed zeroing on
// destruction. It is where a key, passphrase, token, share or other sensitive
// material lives for its whole lifetime instead of an ordinary []byte or an
// un-wipeable string.
//
// OWNERSHIP: a MemoryEnclave's CONTENTS are single-owner. memguard's
// LockedBuffer is not safe for concurrent Destroy/read, so Bytes and Size must
// not run concurrently with Destroy, and the slice returned by Bytes must not
// outlive the buffer (Destroy zeroes and unmaps that memory).
//
// The locked-buffer REFERENCE is held in an atomic.Pointer, mirroring
// ProtectedKey. That makes Destroy single-shot and idempotent under
// concurrency — exactly one caller reaches memguard, the rest are no-ops — and
// removes this type's own field race; it does not make the contents shareable.
//
// RESOURCE: every MemoryEnclave is one mlock'd allocation, and locked memory is
// a finite kernel resource (RLIMIT_MEMLOCK). memguard terminates the process
// when a lock fails, so code that allocates enclaves proportionally to
// untrusted or operator-supplied input must bound the count and Destroy as it
// goes rather than at the end.
type MemoryEnclave struct {
	buf atomic.Pointer[memguard.LockedBuffer]
}

// ProtectedBuffer is the former name of MemoryEnclave.
//
// Deprecated: renamed to MemoryEnclave, which says what the type is — a region
// of locked, zeroized memory — and matches the name used across the stack. It
// is an ALIAS, not a distinct type, so existing consumers keep compiling and
// *ProtectedBuffer and *MemoryEnclave are interchangeable everywhere; the old
// name is removed in the coordinated bottom-up rename, not before.
type ProtectedBuffer = MemoryEnclave

// newMemoryEnclave wraps an existing locked buffer.
func newMemoryEnclave(buf *memguard.LockedBuffer) *MemoryEnclave {
	p := &MemoryEnclave{}
	p.buf.Store(buf)
	return p
}

// NewMemoryEnclave allocates a region of locked memory of the given size,
// guard-paged on both sides. The contents are MUTABLE — the caller writes the
// sensitive material directly into Bytes and should call Freeze once it is
// final — which is what lets a producer build its output inside the enclave
// instead of assembling it on the ordinary heap and copying it in.
func NewMemoryEnclave(size int) *MemoryEnclave {
	return newMemoryEnclave(memguard.NewBuffer(size))
}

// NewMemoryEnclaveFromBytes moves existing bytes into a region of locked
// memory. The source slice is WIPED after the copy, and the resulting enclave
// is frozen read-only.
//
// Prefer NewMemoryEnclave and write into it where the material can be produced
// in place: bytes that reach this constructor already existed unprotected.
func NewMemoryEnclaveFromBytes(data []byte) *MemoryEnclave {
	return newMemoryEnclave(memguard.NewBufferFromBytes(data))
}

// NewProtectedBuffer allocates a memory-protected buffer of the given size.
//
// Deprecated: use NewMemoryEnclave. Identical behaviour and identical return
// type; only the name changed.
func NewProtectedBuffer(size int) *MemoryEnclave {
	return NewMemoryEnclave(size)
}

// NewProtectedBufferFromBytes creates a memory-protected buffer from existing
// bytes, wiping the source.
//
// Deprecated: use NewMemoryEnclaveFromBytes. Identical behaviour and identical
// return type; only the name changed.
func NewProtectedBufferFromBytes(data []byte) *MemoryEnclave {
	return NewMemoryEnclaveFromBytes(data)
}

// Bytes returns the underlying byte slice, or nil once the buffer has been
// destroyed. The caller must NOT store this reference beyond the lifetime of
// the ProtectedBuffer.
func (p *MemoryEnclave) Bytes() []byte {
	buf := p.buf.Load()
	if buf == nil {
		return nil
	}
	return buf.Bytes()
}

// Size returns the size of the protected buffer, or 0 once destroyed.
func (p *MemoryEnclave) Size() int {
	buf := p.buf.Load()
	if buf == nil {
		return 0
	}
	return buf.Size()
}

// Freeze makes the enclave's memory read-only. It is the second half of the
// NewMemoryEnclave pattern: allocate, write the material in place, freeze. A
// write after Freeze faults, which is the point — the material is final and
// nothing should be able to mutate it in place. Destroy still works on a
// frozen enclave (it melts the pages before wiping them). Freeze is a no-op on
// a nil or already-destroyed enclave.
func (p *MemoryEnclave) Freeze() {
	if buf := p.buf.Load(); buf != nil {
		buf.Freeze()
	}
}

// Destroy zeroes and releases the protected memory. Safe to call multiple
// times and concurrently with Bytes/Size; only the caller that wins the swap
// destroys the buffer.
func (p *MemoryEnclave) Destroy() {
	if buf := p.buf.Swap(nil); buf != nil {
		buf.Destroy()
	}
}

// ProtectedKey wraps memguard.Enclave for encrypting key material at rest in memory.
// The key is only decrypted when needed and immediately re-encrypted.
//
// LIFECYCLE, STATED HONESTLY: a ProtectedKey can be made unusable (Clear) but
// cannot be erased. Clear revokes the ABILITY to open the key; the enclave's
// ciphertext remains on the heap until the GC reclaims it, because memguard
// offers no way to zero it. Callers that need erasure of the plaintext have it
// already — the plaintext exists only inside the MemoryEnclave that Open
// returns, and MemoryEnclave.Destroy genuinely wipes. Callers that need
// erasure of the ciphertext cannot get it from this type; treat the key as
// GC-bounded, not wiped. See the package doc.
//
// ProtectedKey is safe for concurrent use: the enclave reference is held in
// an atomic.Pointer, so Open and Clear may race without a torn read. An
// atomic pointer (rather than a mutex) is used because Open sits on hot
// decrypt paths (per-frame DEK access in the seal barrier) and the only
// shared state is the single reference — a lock would add contention for no
// additional invariant.
type ProtectedKey struct {
	enclave atomic.Pointer[memguard.Enclave]
}

// NewProtectedKey creates an encrypted-at-rest key from a byte slice.
// The source slice is wiped after sealing into the enclave.
func NewProtectedKey(key []byte) *ProtectedKey {
	p := &ProtectedKey{}
	p.enclave.Store(memguard.NewEnclave(key))
	return p
}

// Open decrypts the key material into a MemoryEnclave.
// The caller must call Destroy() on the returned enclave when done.
func (p *ProtectedKey) Open() (*MemoryEnclave, error) {
	enclave := p.enclave.Load()
	if enclave == nil {
		return nil, ErrEnclaveNil
	}
	buf, err := enclave.Open()
	if err != nil {
		return nil, &OpenError{Err: err}
	}
	return newMemoryEnclave(buf), nil
}

// Clear drops the enclave reference so the key can no longer be opened.
// After Clear, Open returns ErrEnclaveNil. Safe to call multiple times and
// concurrently with Open.
//
// Clear deliberately does NOT open the enclave to destroy the returned
// buffer: memguard.Enclave has no API to zero its internal ciphertext, so an
// open-then-destroy would only wipe the freshly DECRYPTED copy — briefly
// re-materializing the plaintext in memory for no benefit — while leaving the
// enclave's own ciphertext untouched either way.
//
// What remains after Clear is therefore the enclave's ciphertext, on the
// ordinary heap, until the garbage collector reclaims it. Nothing wipes it:
// memguard v0.23.0 registers NO finalizer on Enclave (only its own tests do),
// so an earlier claim that "the finalizer reclaims it" was wrong. The security
// argument that survives is the other one — the ciphertext is useless without
// memguard's per-process session coffer key, which lives in a locked buffer
// this process never writes out.
func (p *ProtectedKey) Clear() {
	p.enclave.Store(nil)
}

// SealMemoryEnclave creates a new ProtectedKey from a MemoryEnclave,
// CONSUMING the enclave: ownership transfers to the returned key and the
// argument is left empty (Bytes returns nil, Size returns 0). Returns nil if
// the enclave is nil or already consumed/destroyed. Only the caller that wins
// the swap seals; a concurrent second call gets nil.
func SealMemoryEnclave(buf *MemoryEnclave) *ProtectedKey {
	if buf == nil {
		return nil
	}
	locked := buf.buf.Swap(nil)
	if locked == nil {
		return nil
	}
	p := &ProtectedKey{}
	p.enclave.Store(locked.Seal())
	return p
}

// SealProtectedBuffer creates a new ProtectedKey from a MemoryEnclave,
// consuming it.
//
// Deprecated: use SealMemoryEnclave. Identical behaviour; only the name
// changed.
func SealProtectedBuffer(buf *MemoryEnclave) *ProtectedKey {
	return SealMemoryEnclave(buf)
}

// WipeBytes securely zeroes a byte slice using memguard's wiping mechanism.
// This provides compiler-optimization-resistant zeroing.
func WipeBytes(b []byte) {
	memguard.WipeBytes(b)
}

// NOTE — there is deliberately no InitMemguard here.
//
// An earlier InitMemguard called memguard.CatchSignal(SIGTERM, SIGINT). It had
// no callers, and wiring it up would have been actively harmful in an embedded
// consensus engine:
//
//   - memguard.CatchSignal calls signal.Reset() with NO arguments, which
//     detaches EVERY signal handler in the process — including the embedding
//     application's own, and including handlers this library knows nothing
//     about.
//   - Its handler then calls core.Exit(1). SIGTERM stops being a graceful
//     shutdown and becomes an immediate exit: no engine quiesce, no Host.Close,
//     no barrier seal, no WAL flush. Trading a durable shutdown for a memory
//     wipe is the wrong trade for a database's consensus layer, where the WAL
//     is the thing that must survive.
//   - It would not have bought much: memguard already wipes LockedBuffers on
//     Destroy, and the enclave ciphertext it cannot wipe is useless without the
//     session coffer key, which dies with the process either way. Nothing
//     catches SIGKILL, so the protection could never have been complete.
//
// An embedder that wants signal-triggered wiping must install it in its own
// main, alongside — not instead of — its graceful shutdown path.
