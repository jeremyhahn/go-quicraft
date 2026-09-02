# Memory Enclave — Architecture

> **Status: NORMATIVE** · Scope: go-quicraft `pkg/enclave` · Last verified: 2026-09-02
> Conflicts with this document are defects. Fix them here first.

## The problem

Every key this library handles — the barrier root key, each epoch DEK, the
passphrase held by `SoftwareStrategy` — spends its life in ordinary Go heap
memory. Ordinary heap memory is swappable, appears in core dumps, is copied by
the garbage collector, and is never zeroed on release. A `[]byte` holding a
32-byte AES key therefore leaves an unbounded number of copies at unpredictable
addresses for the remaining life of the process.

`pkg/enclave` is the single answer to that in this repository. Nothing in
go-quicraft holds raw key bytes in a plain `[]byte` or `string` for longer than
the window between allocating one and handing it to this package.

## The shape chosen

Two types, and the split between them is the whole design:

| Type | Holds | Guarantee |
|---|---|---|
| `ProtectedKey` | key material **encrypted at rest** in process memory (a `memguard.Enclave`). Owned by this package. | ciphertext is useless without memguard's per-process session key |
| `MemoryEnclave` (`ProtectedBuffer` is the deprecated spelling) | key material **in plaintext**, `mlock`'d, guard-paged, canary-checked. **Owned by go-enclave**; this package only ALIASES the type. | `Destroy` genuinely wipes the bytes before unmapping |

**The second row is not this repository's code.** `MemoryEnclave` is
`github.com/jeremyhahn/go-enclave`'s type, per `docs/00 §R` — *"No project
hand-rolls its own locked-memory handling."* Sections below that describe borrow
counters, `purgeMu` and process-wide purge coordination are describing machinery
this repository **no longer has**; go-enclave maps its own memory, nothing can
purge it, and a borrow takes only that enclave's own read lock. Those sections
are stale and are being rewritten (#68).

A key lives as a `ProtectedKey` and is materialized into a `ProtectedBuffer`
only for the window in which it must actually be used, then destroyed. Plaintext
exposure is bounded to `Open` → `Destroy`.

```
NewProtectedKey(key []byte)          key []byte is WIPED by the constructor
        |
        v
  ProtectedKey  ──── Open() ────►  ProtectedBuffer  ──── Bytes() ──►  cipher.AEAD
   (ciphertext)                      (plaintext,           |
        |                             mlock'd)             |
        |                                 |                v
     Clear()                          Destroy()        (use, then discard)
   revokes access                   WIPES + unmaps
   (does NOT wipe)
```

`SealProtectedBuffer` runs the arrow in reverse and **consumes** the buffer:
ownership transfers to the returned key and the argument is left empty.

## The name is not a hardware enclave

"Enclave" is memguard's term for an encrypted in-memory container. It is not a
TEE, not SGX/SEV/TrustZone, and not a TPM or HSM boundary. Everything this
package protects lives in the address space of this process, protected by this
process. Any code that can read this process's memory — a debugger, a live core
read, another goroutine holding a stray slice — defeats it.

Hardware key custody in this stack belongs to **go-xkms**, where the key never
leaves the device. Nothing here should be read as offering that property, and a
consumer that needs it consumes go-xkms rather than reaching for this package.

## Trust boundary

The boundary is the process. Inside it, this package narrows the *window* and
the *number of copies*; it does not create an isolation domain. Stated as the
package doc states it:

**Guaranteed**

- `MemoryEnclave` memory is `mlock`'d (excluded from swap), fenced by guard
  pages on both sides, canary-checked, excluded from core dumps, and wiped by
  `Destroy` before unmapping. That wipe is real and is the strongest guarantee
  available here. It is go-enclave's guarantee, consumed rather than restated.
- `ProtectedKey` material is AEAD-encrypted under a per-process session key that
  memguard itself keeps in a locked buffer, never writes out, and drops on exit.
  A memory disclosure capturing only the enclave's ciphertext yields nothing.
- Plaintext exposure is bounded to `Open`/`Destroy` windows.

**Not guaranteed**

- `Clear` does **not** wipe. See the design decision below.
- Nothing survives `SIGKILL`, a hypervisor snapshot, or a cold-boot attack.
- Derived copies made by consumers are outside the boundary. A `cipher.AEAD`
  built from an opened key holds that key in its own expanded key schedule, and
  neither the standard library nor `x/crypto` offers a way to wipe it.

## Design decisions, and what was rejected

### `Clear` revokes rather than erases

`ProtectedKey.Clear` drops the enclave reference so the key can no longer be
opened; the enclave's ciphertext remains an ordinary heap slice until the
garbage collector reclaims it.

*Rejected: open-then-destroy inside `Clear`.* `memguard.Enclave` exposes no API
to zero its internal ciphertext, so opening the enclave in order to destroy the
result would wipe only the freshly **decrypted** copy — briefly re-materializing
the plaintext in memory for no benefit — and leave the ciphertext untouched
either way. The operation is strictly worse than doing nothing.

*Rejected: "the finalizer reclaims it."* memguard v0.23.0 registers no finalizer
on `Enclave` (only its own tests do). An earlier version of this claim was wrong
and is recorded here so it is not re-derived. The security argument that
survives is the other one: the ciphertext is useless without the session key.

### An `atomic.Pointer` for `ProtectedKey`, a borrow counter for `MemoryEnclave`

`ProtectedKey` holds its `memguard.Enclave` in an `atomic.Pointer`. `Open` sits
on hot decrypt paths (per-frame DEK access in the seal barrier), the only shared
state is a single reference, and `Open` hands memguard's own `Enclave.Open` a
pointer it then owns — so a lock would add contention and protect no additional
invariant.

`MemoryEnclave` used the same design and it was **wrong** (issue #15). An atomic
makes the *pointer* race-free and says nothing about how long the thing it
points at stays *mapped*: `Destroy` could unmap between an accessor's load and
its dereference, and `memcall.Free` is a real `munmap`, so the consequence was a
segfault rather than a stale read. memguard's own `core.Buffer` mutex carries the
comment "TODO: this does not protect 'data' field" — the field `destroy()` nils
— so the read side was unsynchronised however carefully the pointer was loaded.

The general shape of the mistake, worth keeping: **an atomic on the handle is
not a lifetime discipline for the thing it points at.** The fix has to make
`Destroy` *wait*, which is what a lock does and what an atomic swap cannot.

Methods therefore split by what they do to the region. `Bytes`, `Size` and
`WithBytes` READ it and register a **borrow**; `Destroy`, `Freeze` and
`SealMemoryEnclave` MUTATE it — releasing the memory or changing its page
protection — and wait for every borrow to end first.

**Mutators take priority.** Once one is queued, no new borrow is admitted until
it finishes. This is not a polish on the fix, it is part of it: a counter that
admits a reader whenever the region is merely non-empty lets a steady stream of
overlapping callbacks hold the count above zero forever, and a `Destroy` parked
behind them never wipes the secret. Unbounded starvation of the wipe is a
security defect in a type whose entire purpose is that the wipe happens. Raised
by Codex review of PR #17, after two rounds in which the counter had no fairness
rule at all.

*The price is reentrancy, and an earlier revision of this design advertised the
opposite.* The counter was originally chosen over an `RWMutex` precisely because
Go's `RWMutex` forbids recursive read locking, and it looked as though callbacks
would need to re-enter. Two things turned out to be true. First, nothing in this
repository nests: the reads inside `CombineProtected`'s and `MarshalJSON`'s
callbacks are on a **different** enclave (the decode scratch buffer), which has
its own lock. Second, the only admission policy that prevents starvation is
precisely the one that forbids nesting — a queued mutator must block new
borrows, and a nested borrow is a new borrow. The two properties are mutually
exclusive, so the choice is which failure to have, and a deadlock is loud,
reproducible and fixable at the call site where starvation is silent and
unbounded.

The counter is kept over an `RWMutex` (which would give the same policy) only
because the mutator's park is an observable point, and the concurrency tests
need it to assert the guarantee without using elapsed time as evidence.

A callback must therefore not call `Bytes`, `Size` or `WithBytes` on the SAME
enclave, and must not `Destroy` or `Freeze` it either.

*Why `Freeze` is a mutator.* It changes page protection. `NewMemoryEnclave`
returns deliberately mutable memory whose documented use is "allocate, write in
place, freeze", so a `Freeze` that only borrowed could re-protect the pages
midway through a callback still filling the buffer — and that callback's next
write would fault, killing the process rather than returning an error.

Measured cost of the borrow on the accessor, back to back on one machine:
2.40 ns/op unsynchronised → 21.6 ns/op with the borrow, uncontended, against a
`Barrier.Decrypt` 64B floor of ~123 ns/op. An `RWMutex` read lock would be
cheaper (~9.6 ns/op when that variant was measured), because a borrow takes a
full mutex twice where an `RLock` takes one; the difference buys the nesting
property, which is what the internal call sites need.

One asymmetry survives, deliberately, and is a caller obligation: the borrow
bounds the buffer's lifetime against the accessor **call**, not against a slice
the caller kept. `Bytes` returns a slice that aliases protected memory and is
valid only until `Destroy`. `WithBytes(func([]byte) error) error` holds the
borrow across the caller's own use and is the accessor for anywhere teardown can
race a reader.

### Constructors wipe their input

`NewProtectedKey` and `NewProtectedBufferFromBytes` wipe the source slice after
copying. A caller that hands over key bytes does not then hold a second copy it
has to remember to clear, because the most common way to leak a key is to forget
the copy you made on the way in.

## Component boundaries

- **`pkg/enclave`** owns the types and is the only place in this repository that
  calls memguard. `pkg/seal/memprotect.go` re-exports `ProtectedBuffer`,
  `ProtectedKey` and the four constructors as aliases for `pkg/seal` consumers;
  it adds no behavior.
- **Consumers inside this repo**: `pkg/seal` (`Barrier.rootKey`,
  `Barrier.currentDEK`, the per-epoch `deks` map, `SoftwareStrategy.passphrase`).
- **Consumers above**: Layer 0–3 library code that must touch raw key bytes uses
  this package (or memguard directly). Higher layers use the stack's owned
  abstractions instead — the go-quicbaas KMS, go-qrdb credentials, go-xkms
  password storage — and must not code raw memguard where such an abstraction
  exists.

Capability rows in `docs/00 §R` touched: the go-quicraft rows for
`Barrier`/`SealingStrategy` and for the `none`/`passphrase` strategies, whose
key material this package holds. `pkg/enclave` has no §R row of its own.

## What this feature deliberately does not do

- **No hardware custody.** No TPM, PKCS#11, HSM, TEE, or cloud KMS. That is
  go-xkms.
- **No key derivation, encryption, or algorithm choice.** This package moves and
  protects bytes; `pkg/seal` decides what they mean.
- **No persistence.** Nothing here writes to disk. A key that must survive the
  process is sealed by a `SealingStrategy`, not by this package.
- **No cross-process sharing.** There is no shared-memory mode and no handle
  that another process can open.
- **No erasure of `ProtectedKey` ciphertext.** Callers that need erasure of
  plaintext already have it via `ProtectedBuffer.Destroy`; callers that need
  erasure of ciphertext cannot get it from this type and must treat the key as
  GC-bounded.
