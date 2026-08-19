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
	"sync"
	"sync/atomic"
	"testing"

	"github.com/awnumar/memguard"
)

// ErrEnclaveNil is returned when Open is called on a ProtectedKey whose
// enclave has not been initialized or has been explicitly cleared.
var ErrEnclaveNil = &NilError{}

// ErrEnclaveDestroyed is returned by WithBytes when the enclave is nil or its
// memory has already been released. It is deliberately distinct from a
// zero-length secret: a caller that cannot tell "destroyed" from "empty" will
// eventually treat one as the other.
var ErrEnclaveDestroyed = &DestroyedError{}

// DestroyedError is the typed error for access to a destroyed enclave.
type DestroyedError struct{}

// Error implements the error interface.
func (e *DestroyedError) Error() string {
	return "memprotect: enclave has been destroyed"
}

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
// CONCURRENCY: every method is safe to call concurrently with every other,
// including Destroy. The methods split by what they do to the region. Bytes,
// Size and WithBytes READ it and register a borrow; Destroy, Freeze and
// SealMemoryEnclave MUTATE it — releasing the memory, or changing its page
// protection — and wait for every borrow to end first. A reader therefore
// observes either a live buffer or a destroyed one, never a buffer whose memory
// is being unmapped or re-protected underneath it.
//
// "Live" does NOT imply "writable", and the difference is a real hazard rather
// than pedantry. A frozen enclave is live and read-only. The borrow orders a
// callback against a Freeze it RACES — whichever acquires first goes first —
// but it cannot order a callback against a Freeze that already won, and a
// callback that then writes faults on read-only pages. Writing inside WithBytes
// is therefore only sound while the enclave is still mutable, which is the
// producer's own allocate/fill/Freeze sequence. If some other goroutine may
// Freeze concurrently, the caller must order that externally; this type does
// not, and cannot, do it for them.
//
// Mutators take priority: once one is queued, no new borrow is admitted until
// it has finished. That fairness rule is part of the fix rather than a polish
// on it. A counter that admits readers whenever the region is merely non-empty
// lets a steady stream of overlapping callbacks hold the count above zero
// forever, and a Destroy parked behind them NEVER WIPES THE SECRET — unbounded
// starvation of the wipe is a security defect in a type whose entire purpose is
// that the wipe happens.
//
// The price is reentrancy, and it is worth stating plainly because an earlier
// version of this type advertised the opposite. A callback MUST NOT call Bytes,
// Size or WithBytes on the SAME enclave: if a mutator queued while the callback
// was running, the nested borrow waits for that mutator and the mutator waits
// for the borrow the callback is holding. That is the same restriction
// sync.RWMutex imposes, for the same reason, and it is a deliberate trade —
// a deadlock is loud, reproducible and fixable at the call site, whereas
// starvation is silent and unbounded. Nothing in this repository nests: the
// reads inside CombineProtected's and MarshalJSON's callbacks are on a
// DIFFERENT enclave, which has its own lock. A callback also must not Destroy
// or Freeze its own enclave, which would wait on a borrow it is itself
// holding.
//
// This field was previously an atomic.Pointer, which was NOT sufficient, and
// the reason is worth stating because the mistake is an easy one to repeat. An
// atomic makes access to the POINTER race-free and says nothing about the
// LIFETIME of what the pointer addresses: Destroy could swap the pointer out
// and unmap the region between a reader's load and its dereference. memguard
// v0.23.0 makes that concrete — `core.Buffer` embeds a mutex whose own author
// notes it "does not protect 'data' field", `Buffer.Data()` reads that field
// holding nothing, and `Buffer.destroy()` writes it and then `memcall.Free`s
// the region. Because the memory is genuinely unmapped rather than merely
// zeroed, losing that race is a SIGSEGV rather than a stale read. See issue #15
// and memprotect_race_test.go.
//
// THE ONE THING THE LOCK CANNOT DO is govern a slice the caller has already
// been handed. Bytes returns a slice ALIASING the protected region, and that
// slice outlives the call, so no amount of internal locking makes
//
//	b := enc.Bytes()   // returns, lock released
//	use(b)             // Destroy may run here
//
// safe. Use WithBytes for anything that may run alongside teardown: it holds
// the borrow across the callback, which is the only construction in which the
// aliasing slice is guaranteed valid for the whole span it is used.
//
// RESOURCE: every MemoryEnclave is one mlock'd allocation, and locked memory is
// a finite kernel resource (RLIMIT_MEMLOCK). memguard terminates the process
// when a lock fails, so code that allocates enclaves proportionally to
// untrusted or operator-supplied input must bound the count and Destroy as it
// goes rather than at the end.
type MemoryEnclave struct {
	mu       sync.Mutex
	idle     *sync.Cond // signalled when borrows hits 0; woken by release
	readable *sync.Cond // signalled when waiting hits 0; woken by a finishing mutator
	buf      *memguard.LockedBuffer
	borrows  int // accessors currently reading the region
	waiting  int // mutators queued or running — see borrow's writer preference
}

// borrow registers a reader and returns the live buffer, or false once the
// enclave has been destroyed. Every borrow must be matched by a release.
//
// The counter is kept instead of an RWMutex read lock for ONE reason, and it is
// no longer the reason it was introduced for: the mutator's park is an
// observable point, which is what lets the concurrency probes assert the
// guarantee without using elapsed time as evidence. Its original justification
// — that a counter permits a callback to borrow again where an RWMutex would
// deadlock — did not survive writer preference, because a nested borrow is a
// new borrow and the loop below turns it away exactly as an RWMutex would. See
// the type doc: callbacks must not re-enter.
func (p *MemoryEnclave) borrow() (*memguard.LockedBuffer, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	// WRITER PREFERENCE. Once a mutator is queued, no new borrow is admitted
	// until it has finished. Without this the counter admits readers for as
	// long as they keep arriving, so a steady stream of overlapping WithBytes
	// callbacks can hold the count above zero forever and a parked Destroy
	// NEVER WIPES THE SECRET. Unbounded starvation of the wipe is a security
	// defect in a type whose entire purpose is that the wipe happens, which is
	// why the fairness rule is not a refinement here but part of the fix.
	//
	// This is the same policy sync.RWMutex applies, and it costs the same
	// thing: a borrow taken from INSIDE a callback that already holds one
	// deadlocks if a mutator queued in between. See the type doc — callbacks
	// must not re-enter.
	for p.waiting > 0 {
		if p.readable == nil {
			p.readable = sync.NewCond(&p.mu)
		}
		notifyReaderParked(p)
		p.readable.Wait()
	}
	if p.buf == nil {
		return nil, false
	}
	p.borrows++
	return p.buf, true
}

// release ends a borrow and wakes any mutator waiting for the region to fall
// idle.
func (p *MemoryEnclave) release() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.borrows--
	if p.borrows == 0 && p.idle != nil {
		p.idle.Broadcast()
	}
}

// lockIdle acquires mu and holds it once no reader is borrowing the region. The
// caller must unlock mu. It is how Destroy, Freeze and SealMemoryEnclave get
// exclusive use: they change or release the underlying memory, so they must not
// run while anything is reading it.
func (p *MemoryEnclave) lockIdle() {
	p.mu.Lock()
	// Registered BEFORE the wait, so a reader arriving while this mutator is
	// parked sees the intent and queues behind it rather than renewing the
	// borrow count this mutator is waiting to see fall.
	p.waiting++

	// lockIdle returns HOLDING mu, so its caller — not it — owns the unlock,
	// via a deferred unlockMutated the caller installs on the next line. That
	// leaves a window: if this function exits ABNORMALLY the caller never
	// reaches that line, and mu stays locked with waiting inflated, wedging the
	// enclave permanently. Abnormal exit is reachable through the park hook: a
	// panic in it, or a t.Fatal from the mutator's own goroutine, which is a
	// runtime.Goexit and unwinds defers exactly like a panic.
	//
	// So the abnormal path is cleaned up here, and only the abnormal path —
	// `handedOff` disarms this on the normal return, leaving mu held for the
	// caller as the contract requires.
	handedOff := false
	defer func() {
		if handedOff {
			return
		}
		p.waiting--
		if p.waiting == 0 && p.readable != nil {
			p.readable.Broadcast()
		}
		p.mu.Unlock()
	}()

	for p.borrows > 0 {
		if p.idle == nil {
			p.idle = sync.NewCond(&p.mu)
		}
		notifyMutatorParked(p)
		p.idle.Wait()
	}
	handedOff = true
}

// unlockMutated ends the exclusive section lockIdle began, releasing mu and
// readmitting readers once no mutator remains queued. Every lockIdle must be
// paired with exactly one of these.
func (p *MemoryEnclave) unlockMutated() {
	p.waiting--
	if p.waiting == 0 && p.readable != nil {
		p.readable.Broadcast()
	}
	p.mu.Unlock()
}

// mutatorParked and readerParked hold the test-only observation hooks. They are
// atomic.Pointers rather than plain variables because lockIdle and borrow read
// them from whichever goroutine is blocking while a test installs them from
// another, and an unsynchronized global would be a data race in the very tests
// that exist to find data races.
//
// Both pass the enclave to the callback. That is not decoration: the hooks are
// PACKAGE-GLOBAL, so a mutator or reader parking on an UNRELATED enclave fires
// the hook a test installed for its own. A probe that treated any notification
// as its own could conclude its target had parked when it had not — and would
// then pass against a broken implementation whose mutator merely ran late. Every
// caller must compare the argument against the enclave it is testing and ignore
// the rest.
var (
	mutatorParked atomic.Pointer[func(*MemoryEnclave)]
	readerParked  atomic.Pointer[func(*MemoryEnclave)]
)

// notifyMutatorParked runs the installed hook, if any. Nil in production, where
// this is one atomic load on a path only reached when a mutator actually has to
// wait — never on an uncontended Destroy.
func notifyMutatorParked(p *MemoryEnclave) {
	if fn := mutatorParked.Load(); fn != nil {
		(*fn)(p)
	}
}

// notifyReaderParked runs the installed hook, if any, when a reader is about to
// block behind a queued mutator. Nil in production, and reached only under the
// writer-preference wait.
func notifyReaderParked(p *MemoryEnclave) {
	if fn := readerParked.Load(); fn != nil {
		(*fn)(p)
	}
}

// TestOnlyOnMutatorParked installs a hook invoked each time a mutator (Destroy,
// Freeze, SealMemoryEnclave) is about to block waiting for outstanding borrows
// to end, and returns a function restoring the previous hook. It PANICS outside
// a test binary, following testOnlyRandReader in pkg/crypto/shamir.
//
// It exists because this package's central guarantee — that a mutator cannot
// COMPLETE while a reader holds a borrow — can otherwise only be tested by
// waiting a while and concluding the mutator is probably still blocked. This
// repository's testing convention forbids exactly that: a sleep is never a
// synchronization primitive, because a loaded runner turns "give it 100ms" into
// a hang or a vacuous pass. With this hook a test learns the mutator IS parked
// and can then assert, deterministically, that it has not finished.
//
// It is exported rather than package-private because pkg/crypto/shamir needs it
// for the same assertions on ProtectedShare, which is the surface through which
// the defect this guards was actually reached.
//
// The hook is called WITH the enclave's mutex held, and receives the enclave
// that parked. It must not touch that enclave — signalling a channel is the
// intended use — and it MUST check the argument, because notifications arrive
// for every enclave in the process, not only the one under test.
//
// LIFETIMES DO NOT NEST ACROSS GOROUTINES. Restore puts back whatever was
// installed when this call ran, so overlapping installs restore incorrectly: if
// A installs, B installs, A restores, then B restores, B is silently disabled
// from A's restore onward and A is resurrected by B's. A resurrected hook can
// then close an already-closed channel and panic inside lockIdle. Install and
// restore from ONE goroutine, and do not run hook-using tests in parallel.
func TestOnlyOnMutatorParked(fn func(*MemoryEnclave)) func() {
	if !testing.Testing() {
		panic("enclave.TestOnlyOnMutatorParked: test-only hook installed outside a test binary")
	}
	prev := mutatorParked.Swap(&fn)
	return func() { mutatorParked.Store(prev) }
}

// TestOnlyOnReaderParked installs a hook invoked each time a reader is about to
// block because a mutator is queued ahead of it, and returns a function
// restoring the previous hook. It PANICS outside a test binary.
//
// It is the counterpart to TestOnlyOnMutatorParked and exists for the half of
// writer preference the mutator hook cannot reach: that a readmitting Broadcast
// actually happens. A test that releases its borrow before the late readers have
// parked never exercises the wakeup at all, and passes against an
// implementation that queues readers correctly and then forgets to wake them.
//
// Same contract: called with the mutex held, receives the enclave that parked,
// and the caller MUST filter on it.
func TestOnlyOnReaderParked(fn func(*MemoryEnclave)) func() {
	if !testing.Testing() {
		panic("enclave.TestOnlyOnReaderParked: test-only hook installed outside a test binary")
	}
	prev := readerParked.Swap(&fn)
	return func() { readerParked.Store(prev) }
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
	return &MemoryEnclave{buf: buf}
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

// Bytes returns the underlying byte slice, or nil once the enclave has been
// destroyed.
//
// The returned slice ALIASES the protected region and its validity ends when
// the enclave is destroyed. This call cannot protect the caller's USE of it:
// the read lock is released on return, so a Destroy racing the caller's own
// dereference unmaps memory the caller is still reading, which segfaults. That
// is safe only where the caller can show Destroy cannot run concurrently —
// typically single-goroutine ownership, the `defer enc.Destroy()` shape used
// throughout the seal barrier.
//
// Where teardown may be concurrent, use WithBytes instead. It is the same
// access under a lock held across the callback, and it is the only form that
// closes the window.
func (p *MemoryEnclave) Bytes() []byte {
	if p == nil {
		return nil
	}
	buf, ok := p.borrow()
	if !ok {
		return nil
	}
	defer p.release()
	return buf.Bytes()
}

// WithBytes calls fn with the enclave's protected bytes, holding the read lock
// for the whole call so the region cannot be destroyed underneath it. It
// returns whatever fn returns, or ErrEnclaveDestroyed without calling fn if
// the enclave is nil or already destroyed — a destroyed enclave is reported
// rather than passed to fn as a nil slice, which fn could otherwise mistake for
// an empty secret.
//
// This is the accessor to reach for whenever teardown may be concurrent with
// use. Bytes hands out a slice whose validity ends at an unpredictable moment;
// WithBytes makes the valid span exactly the callback.
//
// fn MUST NOT retain the slice beyond its own return, and MUST NOT RE-ENTER the
// same enclave by ANY method — not Bytes, Size or WithBytes, and not Destroy,
// Freeze or SealMemoryEnclave.
//
// The read half of that is the one worth explaining, because an earlier version
// of this comment promised the opposite. Mutators take priority: once one is
// queued, borrow admits no new reader, and a nested borrow is a new reader. So
// a nested read blocks on a mutator that queued after fn started, while that
// mutator waits for fn's own borrow to end — neither can proceed. The mutator
// half deadlocks for the plainer reason that it waits on a borrow fn is
// holding.
//
// Reading a DIFFERENT enclave from inside fn is fine and is what this package's
// own callbacks do. Destroying this enclave AFTER WithBytes returns is correct
// and ordinary.
func (p *MemoryEnclave) WithBytes(fn func([]byte) error) error {
	if p == nil {
		return ErrEnclaveDestroyed
	}
	buf, ok := p.borrow()
	if !ok {
		return ErrEnclaveDestroyed
	}
	defer p.release()
	return fn(buf.Bytes())
}

// Size returns the size of the protected buffer, or 0 once destroyed.
func (p *MemoryEnclave) Size() int {
	if p == nil {
		return 0
	}
	buf, ok := p.borrow()
	if !ok {
		return 0
	}
	defer p.release()
	return buf.Size()
}

// Freeze makes the enclave's memory read-only. It is the second half of the
// NewMemoryEnclave pattern: allocate, write the material in place, freeze. A
// write after Freeze faults, which is the point — the material is final and
// nothing should be able to mutate it in place. Destroy still works on a
// frozen enclave (it melts the pages before wiping them). Freeze is a no-op on
// a nil or already-destroyed enclave.
//
// It waits for every in-flight borrow to end, rather than running alongside
// readers, because it is a mutation of the region's page protection rather than
// a read of its contents. A fresh NewMemoryEnclave is deliberately mutable and
// WithBytes hands its callback a writable slice, so a Freeze running under a
// read lock could flip the pages to read-only midway through a callback that is
// still filling the buffer — and the callback's next write would fault and take
// the process down.
//
// WHAT THAT DOES NOT BUY, because an earlier version of this comment claimed it
// did: it does not make "allocate, write in place, freeze" safe across
// goroutines on its own. Waiting for borrows orders Freeze behind a callback
// that ALREADY HOLDS one; it cannot order Freeze behind a callback that has not
// started. If Freeze wins the race, the next WithBytes gets read-only pages and
// its write faults just the same. A producer that fills the buffer from one
// goroutine while another may Freeze must order the two itself — this type
// serializes them, it does not sequence them.
//
// As with Destroy, do not call this from inside a WithBytes callback: it waits
// for every borrow to end, and the callback is holding one.
func (p *MemoryEnclave) Freeze() {
	if p == nil {
		return
	}
	p.lockIdle()
	defer p.unlockMutated()
	if p.buf != nil {
		p.buf.Freeze()
	}
}

// Destroy zeroes and releases the protected memory. It is safe on a nil
// receiver, safe to call more than once, and safe to call concurrently with
// Bytes, Size, Freeze and WithBytes: it takes the write lock, so it cannot
// interleave with a reader that is inside one of those calls, and only the
// first caller reaches memguard.
//
// It does NOT make safe a slice a caller obtained from Bytes and is still
// using: that borrow ended when Bytes returned. See WithBytes.
func (p *MemoryEnclave) Destroy() {
	if p == nil {
		return
	}
	p.lockIdle()
	defer p.unlockMutated()
	if p.buf != nil {
		p.buf.Destroy()
		p.buf = nil
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
	// Exclusive, for the same reason Destroy is: sealing consumes the locked
	// buffer, so it must not run while anything is reading it.
	buf.lockIdle()
	locked := buf.buf
	buf.buf = nil
	buf.unlockMutated()
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
