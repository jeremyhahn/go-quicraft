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
	"os"
	"syscall"

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
type ProtectedBuffer struct {
	buf *memguard.LockedBuffer
}

// NewProtectedBuffer allocates a memory-protected buffer of the given size.
// The buffer is locked in memory with guard pages on both sides.
func NewProtectedBuffer(size int) *ProtectedBuffer {
	return &ProtectedBuffer{buf: memguard.NewBuffer(size)}
}

// NewProtectedBufferFromBytes creates a memory-protected buffer from existing bytes.
// The source slice is wiped after copying into the protected buffer.
func NewProtectedBufferFromBytes(data []byte) *ProtectedBuffer {
	return &ProtectedBuffer{buf: memguard.NewBufferFromBytes(data)}
}

// Bytes returns the underlying byte slice. The caller must NOT store this reference
// beyond the lifetime of the ProtectedBuffer.
func (p *ProtectedBuffer) Bytes() []byte {
	if p.buf == nil {
		return nil
	}
	return p.buf.Bytes()
}

// Size returns the size of the protected buffer.
func (p *ProtectedBuffer) Size() int {
	if p.buf == nil {
		return 0
	}
	return p.buf.Size()
}

// Destroy zeroes and releases the protected memory. Safe to call multiple times.
func (p *ProtectedBuffer) Destroy() {
	if p.buf != nil {
		p.buf.Destroy()
		p.buf = nil
	}
}

// ProtectedKey wraps memguard.Enclave for encrypting key material at rest in memory.
// The key is only decrypted when needed and immediately re-encrypted.
type ProtectedKey struct {
	enclave *memguard.Enclave
}

// NewProtectedKey creates an encrypted-at-rest key from a byte slice.
// The source slice is wiped after sealing into the enclave.
func NewProtectedKey(key []byte) *ProtectedKey {
	return &ProtectedKey{enclave: memguard.NewEnclave(key)}
}

// Open decrypts the key material and returns a ProtectedBuffer.
// The caller must call Destroy() on the returned buffer when done.
func (p *ProtectedKey) Open() (*ProtectedBuffer, error) {
	if p.enclave == nil {
		return nil, ErrEnclaveNil
	}
	buf, err := p.enclave.Open()
	if err != nil {
		return nil, &OpenError{Err: err}
	}
	return &ProtectedBuffer{buf: buf}, nil
}

// Clear actively zeroes the key material in the enclave by opening it
// into a LockedBuffer and destroying it, then nils the reference. This
// ensures the sensitive data is wiped immediately rather than relying on
// the garbage collector and memguard's finalizer. After Clear, Open
// returns ErrEnclaveNil. Safe to call multiple times.
func (p *ProtectedKey) Clear() {
	if p.enclave == nil {
		return
	}
	// Open the enclave to get a LockedBuffer, then destroy it to
	// securely zero the memory. If Open fails (e.g., enclave was
	// already destroyed internally), we still nil the reference.
	buf, err := p.enclave.Open()
	if err == nil && buf != nil {
		buf.Destroy()
	}
	p.enclave = nil
}

// SealProtectedBuffer creates a new ProtectedKey from a ProtectedBuffer,
// consuming the buffer. The buffer is destroyed after sealing.
func SealProtectedBuffer(buf *ProtectedBuffer) *ProtectedKey {
	if buf == nil || buf.buf == nil {
		return nil
	}
	enclave := buf.buf.Seal()
	buf.buf = nil
	return &ProtectedKey{enclave: enclave}
}

// WipeBytes securely zeroes a byte slice using memguard's wiping mechanism.
// This provides compiler-optimization-resistant zeroing.
func WipeBytes(b []byte) {
	memguard.WipeBytes(b)
}

// InitMemguard initializes the memguard library. Should be called once at startup.
// This sets up signal handlers for secure cleanup on SIGTERM/SIGINT. The callback
// receives the signal for logging; memguard internally wipes all protected memory
// before the process terminates.
func InitMemguard() {
	memguard.CatchSignal(func(_ os.Signal) {}, syscall.SIGTERM, syscall.SIGINT)
}
