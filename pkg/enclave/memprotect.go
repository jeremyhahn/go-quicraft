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

// Package enclave provides ProtectedKey, a container that keeps cryptographic
// key material encrypted at rest in ordinary process memory and decrypts it
// only into locked memory.
//
// # Locked memory belongs to go-enclave
//
// This package no longer implements locked memory. MemoryEnclave is an ALIAS
// for github.com/jeremyhahn/go-enclave's type of the same name — the portfolio
// locked-memory primitive (docs/00 §R), which every Layer 0–3 library consumes
// and no project reimplements. Allocate one with go-enclave's constructors
// directly:
//
//	e, err := goenclave.NewMemoryEnclaveSized(32) // fill in place
//	e, err := goenclave.NewMemoryEnclave(data)    // move existing bytes in
//
// The alias exists so that a consumer naming enclave.MemoryEnclave keeps
// compiling and so that this package's own ProtectedKey has a return type to
// name. It is not a wrapper: there is one implementation in the portfolio and
// it is go-enclave's.
//
// What this package still owns is ProtectedKey, whose posture go-enclave has no
// released counterpart for: material held as CIPHERTEXT at rest, readable only
// inside an Open window, where a MemoryEnclave holds plaintext present-but-locked
// continuously. That gap is tracked upstream (go-enclave #22, the pluggable
// sealing seam) and closes this package when it lands.
//
// # The name is not a hardware enclave
//
// "Enclave" here means an encrypted in-memory container. It is NOT a TEE, not
// SGX/SEV/TrustZone, and not a TPM or HSM boundary: everything protected here
// lives in the address space of this process, protected by this process, and any
// code able to read this process's memory — a debugger, a live core read, another
// goroutine with a stray slice — defeats it. Hardware key custody in this stack
// belongs to go-xkms, where the key never leaves the device.
//
// # What is actually guaranteed
//
//   - MemoryEnclave (go-enclave): memory is mlock'd (excluded from swap), fenced
//     by guard pages, canary-checked and excluded from core dumps, and Destroy
//     WIPES it before unmapping. That wipe is real and is the strongest
//     guarantee available here.
//   - ProtectedKey: the key material is AEAD-encrypted under a per-process
//     session key that memguard keeps in a locked buffer. A memory disclosure
//     capturing only the ciphertext yields nothing without that session key,
//     which is never written out and dies with the process.
//   - Plaintext exposure is bounded to Open/Destroy windows, during which the key
//     lives only in a MemoryEnclave — never on the ordinary Go heap. Open copies
//     locked-to-locked precisely so that no intermediate heap buffer exists.
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
	"sync"
	"sync/atomic"

	"github.com/awnumar/memguard"
	goenclave "github.com/jeremyhahn/go-enclave"
)

// MemoryEnclave is a region of locked, guard-paged, zeroized memory.
//
// It is an ALIAS for go-enclave's type, not a wrapper: the portfolio has one
// locked-memory implementation and this is not it. Construct one with
// goenclave.NewMemoryEnclave or goenclave.NewMemoryEnclaveSized, both of which
// return an error — locking memory is a syscall that can be refused, and the
// bare-pointer constructors this package used to export could not say so.
type MemoryEnclave = goenclave.MemoryEnclave

// ProtectedBuffer is the former name of MemoryEnclave.
//
// Deprecated: use MemoryEnclave, which says what the type is — a region of
// locked, zeroized memory — and matches the name used across the stack. It is an
// ALIAS, so *ProtectedBuffer and *MemoryEnclave remain interchangeable.
type ProtectedBuffer = goenclave.MemoryEnclave

// ErrEnclaveDestroyed reports access to a MemoryEnclave whose memory has already
// been released.
//
// It is go-enclave's sentinel, re-exported rather than redefined: the errors a
// caller sees now come from the implementation that produces them, so
// errors.Is(err, enclave.ErrEnclaveDestroyed) matches whether the error arrived
// through this package or straight from go-enclave. It is deliberately distinct
// from a zero-length secret — a caller that cannot tell "destroyed" from "empty"
// will eventually treat one as the other.
var ErrEnclaveDestroyed = goenclave.ErrEnclaveDestroyed

// ErrEnclaveNil is returned when Open is called on a ProtectedKey whose enclave
// has not been initialized or has been explicitly cleared.
var ErrEnclaveNil = &NilError{}

// NilError is the typed error for nil enclave access attempts. It is returned
// when Open is called on a ProtectedKey whose enclave has not been initialized
// or has been explicitly cleared.
type NilError struct{}

// Error implements the error interface.
func (e *NilError) Error() string {
	return "memprotect: enclave is nil"
}

// OpenError is returned when a ProtectedKey cannot be opened into locked memory.
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

// purgeMu serializes the memguard operations that can PURGE THE PROCESS against
// each other. It is what remains of a much larger lock, and the shrinkage is the
// point of adopting go-enclave rather than a side effect of it.
//
// memguard's failure path is process-wide, not per-buffer: an operation that
// fails calls core.Panic, which calls core.Purge FIRST (v0.23.0
// core/exit.go:84-87), and Purge destroys every live memguard buffer — the pages
// are UNMAPPED, not merely wiped. Purge holds only memguard's own registry and
// key locks, so it cannot see, and does not wait for, anything else.
//
// WHAT CHANGED. This lock used to be held SHARED ACROSS EVERY BORROW of every
// enclave in the process, because those borrows aliased memguard pages a purge
// could unmap underneath them. go-enclave maps its own memory and nothing can
// purge it, so borrows no longer touch memguard at all and no longer take this
// lock. It now covers only the three operations below that genuinely call into
// memguard, each of which is a brief, self-contained window inside this file.
//
// WHY THAT MATTERS BEYOND TIDINESS. Go's RWMutex blocks a new RLock once a writer
// is queued, so while borrows took this lock a borrow could not enclose an
// allocation — across DIFFERENT enclaves, because the lock is process-wide. That
// made a nested borrow a self-deadlock rather than merely a hazard (see #39, and
// the note wrappingCipher used to carry). No borrow takes this lock now, so the
// restriction is gone with it.
//
// It disappears entirely when ProtectedKey moves off memguard (go-enclave #22).
var purgeMu sync.Mutex

// ProtectedKey holds key material encrypted at rest in ordinary memory. The key
// is decrypted only for the duration of an Open, and into locked memory.
//
// LIFECYCLE, STATED HONESTLY: a ProtectedKey can be made unusable (Clear) but
// cannot be erased. Clear revokes the ABILITY to open the key; the enclave's
// ciphertext remains on the heap until the GC reclaims it, because memguard
// offers no way to zero it. Callers that need erasure of the plaintext have it
// already — the plaintext exists only inside the MemoryEnclave that Open
// returns, and MemoryEnclave.Destroy genuinely wipes. Callers that need erasure
// of the ciphertext cannot get it from this type; treat the key as GC-bounded,
// not wiped.
//
// ProtectedKey is safe for concurrent use: the enclave reference is held in an
// atomic.Pointer, so Open and Clear may race without a torn read.
type ProtectedKey struct {
	enclave atomic.Pointer[memguard.Enclave]
}

// NewProtectedKey creates an encrypted-at-rest key from a byte slice.
// The source slice is wiped after sealing into the enclave.
func NewProtectedKey(key []byte) *ProtectedKey {
	// EXCLUSIVE: core/enclave.go:62 shows NewEnclave on the Panic path.
	purgeMu.Lock()
	defer purgeMu.Unlock()
	p := &ProtectedKey{}
	p.enclave.Store(memguard.NewEnclave(key))
	return p
}

// Open decrypts the key material into a MemoryEnclave. The caller OWNS the
// returned enclave and must Destroy it when done.
//
// The returned enclave is FROZEN read-only, so a stray write through a retained
// slice faults instead of silently corrupting the key.
//
// The plaintext never reaches the ordinary Go heap. memguard decrypts into a
// locked buffer of its own; this copies from those locked pages straight into
// go-enclave's, wipes memguard's copy and hands its pages back. Both endpoints of
// the copy are mlock'd, so there is no window in which the key is swappable or
// core-dumpable.
func (p *ProtectedKey) Open() (*MemoryEnclave, error) {
	e := p.enclave.Load()
	if e == nil {
		return nil, ErrEnclaveNil
	}

	// HELD ACROSS THE WHOLE BRIDGE, not around each memguard call in turn, and
	// this is a correctness requirement rather than a simplification.
	//
	// memguard's failure path is process-wide: core.Panic calls core.Purge first
	// (v0.23.0 core/exit.go:84-87), and Purge destroys every live memguard buffer
	// in the process — the pages are UNMAPPED, not merely wiped. buf is one of
	// those buffers for its whole lifetime here. So any window in which buf is
	// alive and this lock is NOT held is a window in which an unrelated
	// goroutine's failed memguard allocation can unmap the pages this one is
	// reading from, and the read faults.
	//
	// A first version of this took the lock only around e.Open and buf.Melt,
	// which left the copy out of buf (inside NewMemoryEnclave, which reads AND
	// wipes it) and buf.Destroy itself unprotected — the two longest windows.
	// Codex caught it. Everything touching buf is inside the one section now.
	//
	// The cost is bounded: since the go-enclave adoption no BORROW takes this
	// lock, so the only contention is between concurrent ProtectedKey operations,
	// which are setup, rekey and rotation paths rather than steady state.
	//
	// No lock cycle results from holding it across NewMemoryEnclave. That reaches
	// go-enclave's registryMu (via register), and nothing that holds registryMu
	// ever waits on this lock or on an enclave's own lock: WipeAll snapshots the
	// live set under registryMu and releases it BEFORE destroying anything
	// (go-enclave registry.go, "Snapshot under the lock, then release it").
	//
	// DEFERRED, not unlocked inline, and this is load-bearing rather than style.
	// The failure this lock exists for is core.Panic, which PANICS after purging —
	// and memguard documents that panic as recoverable ("creates a new key so it
	// is safe to recover from this panic"). An inline Unlock is skipped when the
	// stack unwinds, so a recovered purge would leave purgeMu held forever and
	// wedge every ProtectedKey in the process.
	purgeMu.Lock()
	defer purgeMu.Unlock()

	// Open ALLOCATES — core.Open builds a Buffer through NewBuffer
	// (core/enclave.go:109) — and core/enclave.go:111 is on the Panic path.
	buf, err := e.Open()
	if err != nil {
		return nil, &OpenError{Err: err}
	}
	// Runs BEFORE the deferred Unlock above (defers are LIFO), so the destroy —
	// itself purge-capable, core/buffer.go:182 — is inside the section too.
	defer buf.Destroy()

	// memguard.Enclave.Open FREEZES the buffer it returns (enclave.go:63), and
	// goenclave.NewMemoryEnclave wipes its source on every return path. Wiping
	// mprotect'd read-only pages faults the process, so the buffer is melted
	// first. Melt is purge-capable too (core/buffer.go:141).
	//
	// Destroy above still runs on the melted buffer, which is what memguard's own
	// Destroy does anyway — it melts before wiping.
	buf.Melt()

	me, err := goenclave.NewMemoryEnclave(buf.Bytes())
	if err != nil {
		return nil, &OpenError{Err: err}
	}
	return me, nil
}

// Clear drops the enclave reference so the key can no longer be opened.
// After Clear, Open returns ErrEnclaveNil. Safe to call multiple times and
// concurrently with Open.
//
// Clear deliberately does NOT open the enclave to destroy the returned buffer:
// memguard.Enclave has no API to zero its internal ciphertext, so an
// open-then-destroy would only wipe the freshly DECRYPTED copy — briefly
// re-materializing the plaintext in memory for no benefit — while leaving the
// enclave's own ciphertext untouched either way.
//
// What remains after Clear is therefore the enclave's ciphertext, on the ordinary
// heap, until the garbage collector reclaims it. Nothing wipes it: memguard
// v0.23.0 registers NO finalizer on Enclave. The security argument that survives
// is the other one — the ciphertext is useless without memguard's per-process
// session coffer key, which lives in a locked buffer this process never writes
// out.
func (p *ProtectedKey) Clear() {
	p.enclave.Store(nil)
}

// SealMemoryEnclave creates a new ProtectedKey from a MemoryEnclave, CONSUMING
// the enclave: ownership transfers to the returned key and the argument is
// Destroyed. Returns nil if the enclave is nil or already destroyed.
//
// As in Open, the material is copied locked-to-locked and never touches the
// ordinary heap: memguard's buffer is locked memory, and the source is the
// go-enclave region.
func SealMemoryEnclave(buf *MemoryEnclave) *ProtectedKey {
	if buf == nil {
		return nil
	}

	// CONSUME-ONCE IS ENFORCED BY THIS LOCK, not by the borrow, and the
	// distinction is the whole correctness of this function.
	//
	// The old implementation took the source's mutator lock (lockIdle) and
	// swapped its buffer pointer out under it, so exactly one caller could ever
	// observe live material and a concurrent second call got nil. WithBytes is a
	// READ-side borrow: it admits concurrent callers by design. A first version
	// of this took only that borrow and destroyed afterwards, which let two
	// goroutines both copy the plaintext and both return a distinct non-nil key —
	// duplicating key material and breaking the documented contract. Codex caught
	// it on PR #69.
	//
	// go-enclave exposes no take-ownership primitive to rebuild the old handoff
	// with, so the exclusion lives here: every seal serializes on purgeMu, and the
	// liveness check below is inside the same section as the Destroy that answers
	// it. The loser therefore sees a destroyed enclave, which is exactly the state
	// the winner left.
	//
	// LOCK ORDER, uniform across this file: purgeMu is always outermost. Open
	// takes it before go-enclave's registryMu; this takes it before the source's
	// own read and write locks. Nothing takes an enclave lock and then this one,
	// so the two orders cannot interleave into a cycle.
	purgeMu.Lock()
	defer purgeMu.Unlock()

	// The liveness gate. Checked under the lock so it cannot go stale before the
	// copy: a concurrent seal either has not started or has already finished.
	if !buf.IsAlive() {
		return nil
	}

	var sealed *memguard.Enclave
	if err := buf.WithBytes(func(b []byte) error {
		// memguard's buffer is locked memory and the source is go-enclave's, so
		// the copy is locked-to-locked and the material never reaches the
		// ordinary heap. Seal encrypts and destroys lb.
		lb := memguard.NewBuffer(len(b))
		lb.Copy(b)
		sealed = lb.Seal()
		return nil
	}); err != nil {
		return nil
	}
	buf.Destroy()
	if sealed == nil {
		return nil
	}
	p := &ProtectedKey{}
	p.enclave.Store(sealed)
	return p
}

// SealProtectedBuffer creates a new ProtectedKey from a MemoryEnclave,
// consuming it.
//
// Deprecated: use SealMemoryEnclave. Identical behaviour; only the name changed.
func SealProtectedBuffer(buf *MemoryEnclave) *ProtectedKey {
	return SealMemoryEnclave(buf)
}

// WipeBytes securely zeroes a byte slice. It is go-enclave's Wipe, re-exported
// so callers have one wiping primitive rather than two.
func WipeBytes(b []byte) {
	goenclave.Wipe(b)
}
