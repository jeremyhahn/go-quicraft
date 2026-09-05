# Memory Enclave — Architecture

> **Status: NORMATIVE** · Scope: go-quicraft's use of go-enclave protected memory · Last verified: 2026-09-03
> Conflicts with this document are defects. Fix them here first.

## The problem

Every key this library handles — the barrier root key, each epoch DEK, the
passphrase held by `SoftwareStrategy`, the credential a caller unseals with —
would otherwise spend its life in ordinary Go heap memory. Ordinary heap memory
is swappable, appears in core dumps, is copied by the garbage collector, and is
never zeroed on release. A `[]byte` holding a 32-byte AES key therefore leaves an
unbounded number of copies at unpredictable addresses for the remaining life of
the process.

## The shape chosen: this repository owns none of it

**There is no `pkg/enclave` any more.** It was deleted in #54, and its deletion is
the architecture: `docs/00 §R` is normative that **no project hand-rolls its own
protected-memory handling**, and every type below belongs to
`github.com/jeremyhahn/go-enclave`. go-quicraft declares no protected-memory type,
no alias for one, and no wrapper around one. It consumes two:

| Type | Holds | Guarantee |
|---|---|---|
| `enclave.MemoryEnclave` | key material **in plaintext**, `mlock`'d, guard-paged, canary-checked, excluded from core dumps | `Destroy` genuinely wipes the bytes before unmapping |
| `enclave.SealedEnclave` | key material **encrypted at rest** on the ordinary heap; plaintext only inside an `Open` | `Destroy` wipes the ciphertext and the wrapped-key blob |

The split is the design. A `MemoryEnclave` holds plaintext locked for its whole
lifetime — unswappable and undumpable, but *present*. A `SealedEnclave` holds
ciphertext, so between `Open`s the material is not readable at all. Material in
continuous use belongs in the first; material consulted occasionally and held for
a long time belongs in the second, which is why every long-lived key in this
repository is a `SealedEnclave` and every working copy is a `MemoryEnclave`.

```
enclave.NewSealed(key []byte)        key []byte is WIPED by the constructor
        |
        v
  SealedEnclave  ─── Open() ───►  MemoryEnclave  ── WithBytes() ──►  cipher.AEAD
   (ciphertext,                     (plaintext,           |
    heap)                            mlock'd)             |
        |                               |                 v
    Destroy()                       Destroy()        (use, then discard)
  WIPES ciphertext                WIPES + unmaps
```

Material this repository *generates* never travels that arrow at all. It is
produced directly into locked pages with `NewMemoryEnclaveSized`, filled inside a
borrow, and `Freeze`d — see "Fill, freeze, hand over" below.

## Where the two types are used

| Site | Type | Why |
|---|---|---|
| `Barrier.rootKey` | `*SealedEnclave` | opened on rotation, export, unseal and MAC restamp — not per frame |
| `Barrier.currentDEK`, `Barrier.deks[epoch]` | `*SealedEnclave` | never opened at all; kept so a DEK can be re-derived |
| `SoftwareStrategy.passphrase` | `*SealedEnclave` | read once per seal or unseal, held for the life of the strategy |
| `seal.Credentials.Secret` | `*SealedEnclave` | the caller's passphrase, protected at the API boundary (#41) |
| the root key inside `Initialize`/`Unseal` | `*MemoryEnclave` | live for the whole body, in continuous use |
| `UnsealRootKeyProtected`'s result | `*MemoryEnclave` | the AEAD decrypts straight into it |
| `Barrier.RootKeyProtected`'s result | `*MemoryEnclave` | opened from the sealed root key and frozen before it is handed over (#95) |
| a `ProtectedShare`'s value, a reconstructed secret | `*MemoryEnclave` | read in place, frozen, caller-destroyed |

`Barrier.Encrypt` and `Barrier.Decrypt` open **nothing**. They run off a
`cipher.AEAD` cached per epoch in `b.ciphers`. An older claim that
`ProtectedKey.Open` sat on a per-frame DEK path was measured false during #54 and
is recorded here so it is not re-derived.

## The name is not a hardware enclave

"Enclave" here means an encrypted or locked in-memory container. It is not a TEE,
not SGX/SEV/TrustZone, and not a TPM or HSM boundary. Everything protected lives
in the address space of this process, protected by this process. Any code that can
read this process's memory — a debugger, a live core read, another goroutine
holding a stray slice — defeats it.

Hardware key custody in this stack belongs to **go-xkms**, where the key never
leaves the device. Nothing here should be read as offering that property.

## Trust boundary

The boundary is the process. Inside it, protected memory narrows the *window* and
the *number of copies*; it does not create an isolation domain.

**Guaranteed**

- `MemoryEnclave` memory is `mlock`'d (excluded from swap), fenced by guard pages
  on both sides, canary-checked, marked `MADV_DONTDUMP`, and wiped by `Destroy`
  before unmapping. That wipe is real and is the strongest guarantee available
  here. It is go-enclave's guarantee, consumed rather than restated.
- `SealedEnclave` material is AES-256-GCM ciphertext under a per-value data key
  that is itself wrapped by the process sealer's in-memory key hierarchy. A
  memory disclosure capturing only the envelope yields nothing without that
  hierarchy, which is held in locked memory and dies with the process.
- Plaintext exposure is bounded to `Open`/`Destroy` windows.
- **`Destroy` erases.** Both types wipe what they hold before releasing it. This
  is new: `pkg/enclave.ProtectedKey.Clear` could only revoke access, because
  memguard exposed no way to zero an `Enclave`'s ciphertext and registered no
  finalizer that would. Every "GC-bounded ciphertext" caveat that used to appear
  in this repository's key-teardown paths is gone with it.

**Not guaranteed**

- Nothing survives `SIGKILL`, a hypervisor snapshot, or a cold-boot attack.
- Derived copies made by consumers are outside the boundary. A `cipher.AEAD`
  built from an opened key holds that key in its own expanded key schedule, and
  neither the standard library nor `x/crypto` offers a way to wipe it. That is
  the one un-erasable copy this repository still has, and it is why
  `PurgeEpochsBefore` is a capability revocation rather than crypto-shredding.
- A `SealedEnclave` open transits its per-value DEK's key schedule through the
  ordinary heap, as any pure-Go AEAD requires. go-enclave audits that exposure
  byte-exactly on `withAEAD`; it is accepted portfolio-wide by
  `software/docs/22-crypto-sealing-architecture.md`.

## Design decisions, and what was rejected

### Fill, freeze, hand over

Material this repository generates is produced **into** locked pages rather than
moved into them: `NewMemoryEnclaveSized` returns writable locked memory, the
caller fills it inside a borrow, and `MemoryEnclave.Freeze` mprotects it read-only
once the material is final. A stray write through a retained slice then faults
instead of silently corrupting a key.

*Rejected: the fill-then-MOVE shape, which is what this repository shipped
between #54 stage one and stage two.* go-enclave had no post-fill freeze, so the
only way to get a frozen result was to build a second, from-bytes enclave out of
the first — two locked regions live at once, a copy between them, and the source
wiped on the way. It was paid at three sites: the unsealed root key, a Shamir
share value, and a reconstructed secret. go-enclave v0.5.0-alpha's `Freeze`
(go-enclave #6) collapsed all three. In the Shamir split the saving is structural
rather than cosmetic: the shared encoding scratch was a term in
`splitLockedBytes`, so removing it moved the `MaxShareSetBytes` boundary.

A FOURTH site joined them in #95, and it never had the move shape because it had
no freeze at all: `Barrier.RootKeyProtected` returned `SealedEnclave.Open`'s
result unchanged. `Open` allocates a *sized* enclave, so those pages stayed
writable for the enclave's whole life — the exact window `Freeze` exists to
close — while the method's sibling `UnsealRootKeyProtected` had frozen since #40.
Both hand a caller a root key it owns, and they now agree. Only the caller's copy
was ever exposed: `SealedEnclave.Open` decrypts into a fresh, independent enclave
per call.

*Not frozen, deliberately:* the root-key enclave that `Barrier.Initialize` and
`Barrier.Unseal` hold. Its final act is to be **consumed** by `enclave.NewSealed`,
which wipes what it is handed — and a wipe of frozen pages faults the process.
Freezing there would trade a hypothetical stray-write fault for a certain one.

### `SealedEnclave` for anything long-lived, `MemoryEnclave` for anything in use

A `MemoryEnclave` holding a key that is read twice a day costs a locked page for
the whole process lifetime and keeps the plaintext present. A `SealedEnclave`
costs heap and produces plaintext for the length of one operation. Locked memory
is a fixed, process-wide resource (`RLIMIT_MEMLOCK`), so spending it on material
nobody is reading starves the paths that are.

The measured consequence: a `Barrier` holds **zero resident locked pages**. Its
root key and every epoch DEK are sealed envelopes on the ordinary heap, and
locked pages exist only inside a borrow, each of which is `defer`-destroyed or
handed to a caller with a documented obligation.
`seal.TestInitializeAndUnseal_ReleaseTheirRootKeyEnclave` asserts that against
the kernel's own `VmLck` accounting rather than against the code's intentions.

### No process-wide lock, and none is needed

This repository once held a package-level `purgeMu` taken shared across every
borrow, because memguard's failure path called `core.Purge` — which UNMAPS every
live memguard buffer in the process — while holding none of this package's locks.
Go's `RWMutex` refuses a new `RLock` once a writer is queued, so while that lock
existed a borrow could not enclose an allocation, **across different enclaves**,
and nesting was a self-deadlock rather than a documented hazard.

go-enclave maps its own memory and nothing can purge it. `purgeMu` went with the
last memguard call site in #54 stage two, and with it the last reason a borrow
could not enclose one. `Barrier.Initialize` now holds the root key in a locked
enclave for its whole body and seals other enclaves inside that borrow. Re-entering
the *same* enclave from inside its own borrow is still a deadlock, and always
will be.

One asymmetry survives, deliberately, and is a caller obligation: a borrow bounds
the enclave's lifetime against the accessor **call**, not against a slice the
caller kept. `MemoryEnclave.Open` returns a slice that aliases the locked pages
and is covered by no lock once it returns; a concurrent `Destroy` unmaps it and
the next read faults. It is the accessor for code that owns the enclave outright —
the `defer e.Destroy()` shape used throughout the seal barrier. `WithBytes` holds
the read lock across the caller's own use and is the accessor for anywhere
teardown can race a reader.

### Locked memory is charged by the page, and the ceiling is process-wide

`mlock` works on pages, so the kernel charges a whole page per enclave: a 32-byte
key costs 4096 bytes on a 4 KiB-page host, and go-enclave's `LockedSize` reports
the real figure. Guard and canary pages are mapped `PROT_NONE` and never locked,
so they cost address space and VMAs rather than `RLIMIT_MEMLOCK`. Budget in
pages, never in the sizes of the things being protected.

Reaching the ceiling fails exactly the allocation that reached it, with a typed
`ErrLockedMemoryUnavailable`, and touches no other enclave.

go-enclave offers an `Allocator` to bound one path's share of the ceiling.
**This repository does not use one**, deliberately: nothing here allocates locked
memory on behalf of an unauthenticated peer. What it uses instead, where the
arithmetic can multiply out of hand, is a priced refusal — `MaxShareSetBytes`
refuses an over-budget Shamir split before taking a single page. A consumer that
*does* buffer key material before authenticating a peer should admit those
allocations through an `Allocator` at its own layer.

This repository also never calls `WipeAll`: it is a shutdown sweep for whoever
owns the process, and a library that called it would destroy enclaves its embedder
still holds.

### Constructors consume their input

`enclave.NewSealed` and `enclave.NewMemoryEnclave` both wipe their argument on
every return path, failures included. A caller that hands over key bytes does not
then hold a second copy it has to remember to clear, because the most common way
to leak a key is to forget the copy you made on the way in. `seal.NewCredentials`
inherits that and is the whole of #41's fix: the passphrase a caller supplies is
gone from the caller's slice by the time the constructor returns.

The two exceptions are named rather than implied. `NewMemoryEnclaveCopy` protects
a copy and leaves the source alone, for callers whose source genuinely has to
survive. `newSoftwareStrategy` copies before sealing, because its public
constructors take a passphrase the caller may still be using.

## Component boundaries

- **go-enclave (Layer 0)** owns every protected-memory type, the `mlock`, the
  guard pages, the canary, the wipe, the sealing hierarchy and the locking
  discipline. It is documented and tested there.
- **`pkg/seal`** holds `Barrier.rootKey`, `Barrier.currentDEK`, the per-epoch
  `deks` map, `SoftwareStrategy.passphrase` and `Credentials.Secret`.
- **`pkg/crypto/shamir`** holds a `ProtectedShare`'s value and the secret
  `CombineProtected` reconstructs, both frozen `MemoryEnclave`s the caller owns.
- **Consumers above** — go-qrdb, go-xkms, go-quicbaas, go-trusted-platform —
  consume go-enclave directly, never through go-quicraft. This repository
  re-exports none of it, which is the point of deleting the aliases that used to.

Capability rows in `docs/00 §R` touched: the go-quicraft rows for
`Barrier`/`SealingStrategy` and for the `none`/`passphrase` strategies, whose key
material these types hold.

## What this deliberately does not do

- **No hardware custody.** No TPM, PKCS#11, HSM, TEE, or cloud KMS. That is
  go-xkms.
- **No protected-memory implementation.** This repository allocates, borrows,
  freezes and destroys; it does not map, lock, or encrypt. That is go-enclave.
- **No persistence.** Nothing here writes protected material to disk. A key that
  must survive the process is sealed by a `SealingStrategy`.
- **No cross-process sharing.** There is no shared-memory mode and no handle
  another process can open.
