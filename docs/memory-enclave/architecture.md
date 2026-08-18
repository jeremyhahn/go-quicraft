# Memory Enclave — Architecture

> **Status: NORMATIVE** · Scope: go-quicraft `pkg/enclave` · Last verified: 2026-08-19
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
| `ProtectedKey` | key material **encrypted at rest** in process memory (a `memguard.Enclave`) | ciphertext is useless without memguard's per-process session key |
| `ProtectedBuffer` | key material **in plaintext**, `mlock`'d, guard-paged, canary-checked (a `memguard.LockedBuffer`) | `Destroy` genuinely wipes the bytes before unmapping |

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

- `ProtectedBuffer` memory is `mlock`'d (excluded from swap), fenced by guard
  pages on both sides, canary-checked, and wiped by `Destroy` before unmapping.
  That wipe is real and is the strongest guarantee in this package.
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

### An `atomic.Pointer` reference, not a mutex

Both types hold their memguard object in an `atomic.Pointer`. This makes
`Destroy` and `Clear` single-shot and idempotent under concurrency — exactly one
caller reaches memguard, the rest are no-ops.

*Rejected: a `sync.Mutex` per key.* `Open` sits on hot decrypt paths (per-frame
DEK access in the seal barrier). The only shared state is a single reference, so
a lock would add contention and protect no additional invariant.

Note the asymmetry this leaves, which is deliberate and is a caller obligation:
the **reference** is race-safe, the **contents** are not. `ProtectedBuffer` is
single-owner — `Bytes` and `Size` must not run concurrently with `Destroy`, and
the slice `Bytes` returns must not outlive the buffer.

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
