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

// Package enclave provides memory-protected containers for sensitive
// cryptographic key material using memguard. ProtectedKey encrypts keys
// at rest in memory via memguard.Enclave, while ProtectedBuffer provides
// mlock'd guard-page-protected buffers for temporary access.
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

// ProtectedBuffer wraps memguard.LockedBuffer for consistent memory protection
// across all platforms. It provides guard pages, core dump prevention, and
// guaranteed zeroing on destruction.
//
// OWNERSHIP: a ProtectedBuffer's CONTENTS are single-owner. memguard's
// LockedBuffer is not safe for concurrent Destroy/read, so Bytes and Size must
// not run concurrently with Destroy, and the slice returned by Bytes must not
// outlive the buffer (Destroy zeroes and unmaps that memory).
//
// The locked-buffer REFERENCE is held in an atomic.Pointer, mirroring
// ProtectedKey. That makes Destroy single-shot and idempotent under
// concurrency — exactly one caller reaches memguard, the rest are no-ops — and
// removes this type's own field race; it does not make the contents shareable.
type ProtectedBuffer struct {
	buf atomic.Pointer[memguard.LockedBuffer]
}

// newProtectedBuffer wraps an existing locked buffer.
func newProtectedBuffer(buf *memguard.LockedBuffer) *ProtectedBuffer {
	p := &ProtectedBuffer{}
	p.buf.Store(buf)
	return p
}

// NewProtectedBuffer allocates a memory-protected buffer of the given size.
// The buffer is locked in memory with guard pages on both sides.
func NewProtectedBuffer(size int) *ProtectedBuffer {
	return newProtectedBuffer(memguard.NewBuffer(size))
}

// NewProtectedBufferFromBytes creates a memory-protected buffer from existing bytes.
// The source slice is wiped after copying into the protected buffer.
func NewProtectedBufferFromBytes(data []byte) *ProtectedBuffer {
	return newProtectedBuffer(memguard.NewBufferFromBytes(data))
}

// Bytes returns the underlying byte slice, or nil once the buffer has been
// destroyed. The caller must NOT store this reference beyond the lifetime of
// the ProtectedBuffer.
func (p *ProtectedBuffer) Bytes() []byte {
	buf := p.buf.Load()
	if buf == nil {
		return nil
	}
	return buf.Bytes()
}

// Size returns the size of the protected buffer, or 0 once destroyed.
func (p *ProtectedBuffer) Size() int {
	buf := p.buf.Load()
	if buf == nil {
		return 0
	}
	return buf.Size()
}

// Destroy zeroes and releases the protected memory. Safe to call multiple
// times and concurrently with Bytes/Size; only the caller that wins the swap
// destroys the buffer.
func (p *ProtectedBuffer) Destroy() {
	if buf := p.buf.Swap(nil); buf != nil {
		buf.Destroy()
	}
}

// ProtectedKey wraps memguard.Enclave for encrypting key material at rest in memory.
// The key is only decrypted when needed and immediately re-encrypted.
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

// Open decrypts the key material and returns a ProtectedBuffer.
// The caller must call Destroy() on the returned buffer when done.
func (p *ProtectedKey) Open() (*ProtectedBuffer, error) {
	enclave := p.enclave.Load()
	if enclave == nil {
		return nil, ErrEnclaveNil
	}
	buf, err := enclave.Open()
	if err != nil {
		return nil, &OpenError{Err: err}
	}
	return newProtectedBuffer(buf), nil
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

// SealProtectedBuffer creates a new ProtectedKey from a ProtectedBuffer,
// CONSUMING the buffer: ownership transfers to the returned key and the
// argument is left empty (Bytes returns nil, Size returns 0). Returns nil if
// the buffer is nil or already consumed/destroyed. Only the caller that wins
// the swap seals; a concurrent second call gets nil.
func SealProtectedBuffer(buf *ProtectedBuffer) *ProtectedKey {
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
