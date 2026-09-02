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
hand-rolls its own locked-memory handling."* This package declares the alias and
nothing else about it: the `mlock`, the guard pages, the canary, the wipe and the
locking discipline are upstream, documented upstream, and tested upstream. What
this repository still owns is `ProtectedKey` and the bridge between the two
containers.

A key lives as a `ProtectedKey` and is materialized into a `MemoryEnclave` only
for the window in which it must actually be used, then destroyed. Plaintext
exposure is bounded to `Open` → `Destroy`.

```
NewProtectedKey(key []byte)          key []byte is WIPED by the constructor
        |
        v
  ProtectedKey  ─── Open() ───►  MemoryEnclave  ── WithBytes() ──►  cipher.AEAD
   (ciphertext)                    (plaintext,           |
        |                           mlock'd)             |
        |                               |                v
     Clear()                        Destroy()        (use, then discard)
   revokes access                 WIPES + unmaps
   (does NOT wipe)
```

`SealMemoryEnclave` runs the arrow in reverse and **consumes** the enclave:
ownership transfers to the returned key and the argument is destroyed.

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

### An `atomic.Pointer` for `ProtectedKey`, and whose lock guards the rest

`ProtectedKey` holds its `memguard.Enclave` in an `atomic.Pointer`. The reason
used to be stated as throughput — *"`Open` sits on hot decrypt paths (per-frame
DEK access in the seal barrier)"* — and the go-enclave adoption (#54) measured
that claim **false**. Nothing in steady state opens a `ProtectedKey` at all.
`Barrier.Encrypt` and `Barrier.Decrypt` run off a `cipher.AEAD` cached per epoch
in `b.ciphers`; the per-epoch DEK `ProtectedKey`s in `b.deks` exist so a DEK can
be re-derived and are never opened. Every `ProtectedKey.Open` call site in the
repository is a root-key path — rotation, export, unseal — plus the passphrase in
`SoftwareStrategy`. Those run at setup, rotation and rekey rates, not per frame.

The smaller argument survives and is sufficient on its own: the only shared state
is a single reference, `Open` hands memguard's `Enclave.Open` a pointer that call
then owns, and `Clear` is a store of `nil`. There is no invariant spanning two
fields for a mutex to protect, so a mutex would serialize the calls and buy no
guarantee. The retired reason is recorded rather than quietly deleted, because it
is exactly the kind of justification that gets reused for an atomic somewhere it
*is* wrong — as it was here, once:

`MemoryEnclave` used the same design and it was **wrong** (issue #15). An atomic
makes the *pointer* race-free and says nothing about how long the thing it
points at stays *mapped*: `Destroy` could unmap between an accessor's load and
its dereference, and the free is a real `munmap`, so the consequence was a
segfault rather than a stale read.

The general shape of the mistake, worth keeping: **an atomic on the handle is
not a lifetime discipline for the thing it points at.** The fix has to make
`Destroy` *wait*, which is what a lock does and what an atomic swap cannot.

**That fix no longer lives here.** This repository's answer was a borrow counter
with mutator preference; it went with the implementation it guarded. go-enclave
gives each enclave its own `sync.RWMutex` and reaches the same property by the
ordinary route: `WithBytes`, `OpenString`, `EqualTo`, `Size` and `IsAlive` hold
the read lock for the whole call, and `Destroy` takes the write lock, so it
blocks until every in-flight reader has returned rather than unmapping pages
underneath one.

Two consequences of the swap are worth stating, because they reverse what this
document used to say:

- **Nesting is legal.** A borrow of one enclave may construct, borrow and destroy
  *other* enclaves. Under the old counter — and under the process-wide lock
  described below — that was a deadlock, and code was shaped around it.
  `Barrier.Initialize` now holds the root key in a locked enclave for its whole
  body and allocates a `ProtectedKey` inside that borrow (`e694607`), which could
  not have been written before #54. Re-entering the *same* enclave from inside
  its own borrow is still a deadlock, and always will be.
- **The starvation rule went with the counter.** Mutator preference existed
  because a counter admitting a reader whenever the region was non-empty let
  overlapping callbacks park a `Destroy` forever, and an unbounded delay on the
  wipe is a security defect in a type whose purpose is that the wipe happens
  (Codex, PR #17). `sync.RWMutex` already blocks new readers once a writer is
  queued, so the policy is now the standard library's rather than this
  repository's. The counter was kept over an `RWMutex` only because its park was
  an observable point the concurrency tests could assert on; those tests are gone
  too, and the property is upstream's to pin.

One asymmetry survives, deliberately, and is a caller obligation: a borrow bounds
the enclave's lifetime against the accessor **call**, not against a slice the
caller kept. `MemoryEnclave.Open` returns a slice that aliases the locked pages
and is covered by no lock once it returns; a concurrent `Destroy` unmaps it and
the next read faults. It is the accessor for code that owns the enclave outright
— the `defer e.Destroy()` shape used throughout the seal barrier. `WithBytes`
holds the read lock across the caller's own use and is the accessor for anywhere
teardown can race a reader.

### `purgeMu`, and what is left of it

`pkg/enclave` still holds one package-level lock, and its size is the point.
`purgeMu` is a plain `sync.Mutex` covering exactly the three places this
repository calls memguard: `NewProtectedKey`, the whole body of
`ProtectedKey.Open`, and `SealMemoryEnclave`. **No borrow takes it**, and no
go-enclave operation takes it.

It exists because memguard's failure path is process-wide where go-enclave's is
not. A memguard operation that fails calls `core.Panic`, which calls `core.Purge`
first (v0.23.0 `core/exit.go`), and `Purge` UNMAPS every live memguard buffer in
the process while holding none of this package's locks. The memguard buffer that
`Open` decrypts into is one of those for its whole life inside that function, so
any window in which that buffer is alive and this lock is not held is a window in
which an unrelated goroutine's failed memguard allocation unmaps the pages this
one is copying out of. That is why the lock spans the entire bridge rather than
each memguard call in turn.

In `SealMemoryEnclave` it carries a second duty unrelated to purges: it is what
makes consumption happen exactly once. go-enclave exposes no take-ownership
primitive to rebuild the old pointer-swap handoff with, and `WithBytes` is the
READ side — it admits concurrent callers by design — so a seal built on the
borrow alone let two goroutines both copy the plaintext and both return a
distinct non-`nil` key, duplicating key material. The liveness check and the
`Destroy` that answers it are inside one `purgeMu` section instead.

**What this lock used to be**, because a reader who remembers the old layering
needs to know it changed: an `RWMutex` held SHARED ACROSS EVERY BORROW of every
enclave in the process, because those borrows aliased memguard pages a purge
could unmap. Go's `RWMutex` refuses a new `RLock` once a writer is queued, so
while that was true a borrow could not enclose an allocation — across *different*
enclaves, since the lock was process-wide — which is what made nesting a
self-deadlock rather than a documented hazard. go-enclave maps its own memory,
nothing can purge it, and borrows no longer touch memguard, so the layering and
the restriction are gone together. The lock disappears entirely when
`ProtectedKey` moves off memguard (go-enclave #22), which retires the last
memguard call in the repository.

**Lock order, one rule:** `purgeMu` is outermost. `Open` takes it before
go-enclave's registry lock; `SealMemoryEnclave` takes it before the source
enclave's own lock. Nothing takes an enclave lock and then this one, so the two
orders cannot interleave into a cycle.

### Locked memory is charged by the page, and the ceiling is process-wide

`mlock` works on pages, so the kernel charges a whole page per enclave: a 32-byte
key costs 4096 bytes on a 4 KiB-page host, and go-enclave's `LockedSize` reports
the real figure. Guard and canary pages are mapped `PROT_NONE` and never locked,
so they cost address space and VMAs rather than `RLIMIT_MEMLOCK`. Budget in
pages, never in the sizes of the things being protected.

Reaching the ceiling now fails exactly the allocation that reached it, with a
typed `ErrLockedMemoryUnavailable`, and touches no other enclave. Under memguard
it purged every live buffer in the process and aborted. That is a change of kind
rather than degree, and it is the one an operator plans capacity against.

go-enclave offers an `Allocator` to bound one path's share of the ceiling, with a
budget fixed at construction. **This repository does not use one**, deliberately:
nothing here allocates locked memory on behalf of an unauthenticated peer. The
allocations are the root-key enclave that `Initialize` and `Unseal` hold, the
enclaves `ProtectedKey.Open` produces, and the passphrase — each one 32 bytes or
so, made on setup, rotation and rekey paths, and all of them the process's own. A consumer that *does* buffer key material before authenticating a peer
should admit those allocations through an `Allocator` at its own layer, which is
the shape that turns a remote exhaustion attack into a local typed error. This
repository also never calls `WipeAll`: it is a shutdown sweep for whoever owns
the process, and a library that called it would destroy enclaves its embedder
still holds.

### Constructors wipe their input

`NewProtectedKey` wipes the source slice after sealing it, and go-enclave's
`NewMemoryEnclave` wipes `data` on every return path including its failures. A
caller that hands over key bytes does not then hold a second copy it has to
remember to clear, because the most common way to leak a key is to forget the
copy you made on the way in. The sized constructor has nothing to wipe: material
is produced directly into the locked pages and never exists anywhere else.

## Component boundaries

- **`pkg/enclave`** owns `ProtectedKey` and is the only place in this repository
  that calls memguard. `pkg/seal/memprotect.go` re-exports two types
  (`ProtectedBuffer`, `ProtectedKey`) and two functions (`NewProtectedKey`,
  `SealProtectedBuffer`) as aliases for `pkg/seal` consumers; it adds no
  behavior. `seal.NewProtectedBuffer` and `seal.NewProtectedBufferFromBytes` are
  gone rather than renamed, because go-enclave's constructors return an error and
  a bare-pointer alias cannot express one.
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
  plaintext already have it via `MemoryEnclave.Destroy`; callers that need
  erasure of ciphertext cannot get it from this type and must treat the key as
  GC-bounded.
