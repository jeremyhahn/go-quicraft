# Memory enclave: architecture

> **Status: NORMATIVE** · Scope: go-quicraft's use of go-enclave protected memory · Last verified: 2026-11-12
> Conflicts with this document are defects. Fix them here first.

## The problem

Every key this library handles (the barrier root key, each epoch DEK, the
passphrase held by `SoftwareStrategy`, the credential a caller unseals with)
would otherwise live in ordinary Go heap memory. Heap memory is swappable,
appears in core dumps, is copied by the garbage collector and is never zeroed on
release. A `[]byte` holding a 32-byte AES key leaves an unbounded number of
copies at unpredictable addresses for the rest of the process's life.

## The shape: this repository owns none of it

go-quicraft has no `pkg/enclave` and declares no protected-memory type, no alias
for one and no wrapper around one. Protected memory is a single portfolio-wide
primitive, `github.com/jeremyhahn/go-enclave` (Layer 0), so that no project
hand-rolls its own locked-memory handling. `github.com/awnumar/memguard` is not
in the dependency graph. go-quicraft consumes two types:

| Type | Holds | Guarantee |
|---|---|---|
| `enclave.ByteAccess` | key material in plaintext, `mlock`'d, guard-paged, canary-checked, excluded from core dumps | `Destroy` wipes the bytes before unmapping |
| `enclave.Sealed` | key material encrypted at rest on the ordinary heap; plaintext only inside an `Open` | `Destroy` wipes the ciphertext and the wrapped-key blob |

A `ByteAccess` keeps plaintext locked for its whole lifetime: unswappable and
undumpable, but present. A `Sealed` keeps ciphertext, so between `Open`s
the material is not readable at all. Material in continuous use goes in the
first; material consulted occasionally and held for a long time goes in the
second. That is why every long-lived key in this repository is a
`Sealed` and every working copy is a `ByteAccess`.

```
enclave.NewSealed(ctx, key []byte)   key []byte is WIPED by the constructor
        |
        v
  Sealed  ─ Open(ctx) ─►  ByteAccess  ── WithBytes() ──►  cipher.AEAD
   (ciphertext,                         (plaintext,           |
    heap)                                mlock'd, frozen)     |
        |                                   |                 v
    Destroy()                           Destroy()        (use, then discard)
  WIPES ciphertext                    WIPES + unmaps
```

Material this repository generates never takes that path. It is produced
directly into locked pages with `NewMemorySized`, filled inside a borrow
and frozen (see "Fill, freeze, hand over").

## Where the two types are used

| Site | Type | Why |
|---|---|---|
| `Barrier.rootKey` | `Sealed` | opened on rotation, export, unseal and MAC restamp, not per frame |
| `Barrier.currentDEK`, `Barrier.deks[epoch]` | `Sealed` | never opened; kept so a DEK can be re-derived |
| `SoftwareStrategy.passphrase` | `Sealed` | read once per seal or unseal, held for the life of the strategy |
| `seal.Credentials.Secret` | `Sealed` | the caller's passphrase, protected at the API boundary |
| the root key inside `Initialize`/`Unseal` | `ByteAccess` | live for the whole body, in continuous use |
| `UnsealRootKeyProtected`'s result | `ByteAccess` | the AEAD decrypts straight into it |
| `Barrier.RootKeyProtected`'s result | `ByteAccess` | opened from the sealed root key; go-enclave returns it frozen |
| a `ProtectedShare`'s value, a reconstructed secret | `ByteAccess` | read in place, frozen, caller-destroyed |

`Barrier.Encrypt` and `Barrier.Decrypt` open nothing. They run off a
`cipher.AEAD` cached per epoch in `Barrier.ciphers`, so protected memory is not
on the per-frame path.

## "Enclave" is not a hardware enclave

Here "enclave" means an encrypted or locked in-memory container. It is not a
TEE, not SGX/SEV/TrustZone, and not a TPM or HSM boundary. Everything protected
lives in this process's address space, protected by this process. Anything that
can read the process's memory (a debugger, a live core read, another goroutine
holding a stray slice) defeats it.

Hardware key custody in this stack belongs to go-xkms, where the key never
leaves the device. Nothing here offers that property.

## Trust boundary

The boundary is the process. Inside it, protected memory narrows the window of
exposure and the number of copies; it does not create an isolation domain.

Guaranteed:

- `ByteAccess` memory is `mlock`'d (excluded from swap), fenced by guard pages
  on both sides, canary-checked, marked `MADV_DONTDUMP`, and wiped by `Destroy`
  before unmapping. This is go-enclave's guarantee, consumed rather than
  restated.
- `Sealed` material is AES-256-GCM ciphertext under a per-value data key,
  itself wrapped by the process sealer. This repository installs no sealer, so
  unless the embedder calls `enclave.SetSealer` that is go-enclave's default, a
  `SoftwareProvider` in `ModeRootKey`, whose key hierarchy is in-memory. A
  memory disclosure that captures only the envelope yields nothing without that
  hierarchy, which is held in locked memory and dies with the process.
- Plaintext exposure is bounded to `Open`/`Destroy` windows.
- `Destroy` erases on both types: each wipes what it holds before releasing it.
  A `ByteAccess`'s `Destroy` reports a release the kernel refused, or one that ran
  without a wipe because `mprotect` was denied; see "Releases are reported".

Not guaranteed:

- Nothing survives `SIGKILL`, a hypervisor snapshot or a cold-boot attack.
- Derived copies made by consumers are outside the boundary. A `cipher.AEAD`
  built from an opened key holds that key in its own expanded key schedule, and
  neither the standard library nor `x/crypto` can wipe it. That is the one
  un-erasable copy this repository has, and it is why `PurgeEpochsBefore` is a
  capability revocation rather than crypto-shredding.
- A `Sealed` open passes its per-value DEK's key schedule through the
  ordinary heap, as any pure-Go AEAD requires. go-enclave documents that
  exposure on its internal `withAEAD`; it is an accepted, bounded cost of
  encrypting at rest in pure Go.

## Design decisions

### Fill, freeze, hand over

Material this repository generates is produced into locked pages rather than
moved into them. `NewMemorySized` returns writable locked memory, the
caller fills it inside a borrow, and `Freezer.Freeze` (the capability reached
with `enclave.As`) mprotects it
read-only once the material is final. A stray write through a retained slice
then faults instead of silently corrupting a key.

Three sites use it: the unsealed root key (`UnsealRootKeyProtected`), a Shamir
share value, and a reconstructed secret. The root key `Barrier.RootKeyProtected`
hands out is frozen too, but not by this repository: `Sealed.Open`
freezes the enclave it returns and fails the open closed when the kernel refuses
the freeze, so the accessor returns `Open`'s result as it is. Each `Open`
decrypts into a fresh, independent enclave, so a caller's frozen copy affects
nothing else. Code that needs a writable copy of opened material allocates its
own sized enclave and copies into it inside a borrow; writing into the opened
enclave faults.

*Rejected: fill-then-move* (build a second, from-bytes enclave out of the
first). Without a post-fill freeze it was the only way to get a frozen result,
and it costs two locked regions live at once plus a copy between them. In the
Shamir split the difference is structural: the shared encoding scratch would be
a term in `splitLockedBytes` and move the `MaxShareSetBytes` boundary.

*Not frozen, deliberately:* the root-key enclave that `Barrier.Initialize` and
`Barrier.Unseal` hold. Its last act is to be consumed by `enclave.NewSealed`,
which wipes what it is handed, and a wipe of frozen pages faults the process.
Freezing there would trade a hypothetical stray-write fault for a certain one.

### `Sealed` for anything long-lived, `ByteAccess` for anything in use

A `ByteAccess` holding a key read twice a day costs a locked page for the
whole process lifetime and keeps the plaintext present. A `Sealed` costs
heap and produces plaintext for the length of one operation. Locked memory is a
fixed, process-wide resource (`RLIMIT_MEMLOCK`), so spending it on material
nobody is reading starves the paths that are.

The consequence: a `Barrier` holds zero resident locked pages. Its root key and
every epoch DEK are sealed envelopes on the heap, and locked pages exist only
inside a borrow, each of which is `defer`-destroyed or handed to a caller with a
documented obligation. `seal.TestInitializeAndUnseal_ReleaseTheirRootKeyEnclave`
checks this against the kernel's own `VmLck` accounting.

### No process-wide lock

go-enclave maps its own memory and nothing can purge it from outside, so this
repository holds no process-wide lock around borrows. A borrow may enclose an
allocation or a borrow of a different enclave: `Barrier.Initialize` holds the
root key in a locked enclave for its whole body and seals other enclaves inside
that borrow. Re-entering the same enclave from inside its own borrow is a
deadlock.

*Rejected: a package-level lock shared across every borrow.* A memory library
whose failure path unmaps every live buffer in the process needs one, and with
it Go's `RWMutex` writer preference turns any nested borrow of a second enclave
into a self-deadlock. go-enclave has no such path.

A borrow bounds the enclave's lifetime against the callback, not against a slice
the caller kept, and that remains a caller obligation. `WithBytes` is the only
path that hands out the bytes (`EqualTo` compares in place and returns only a
verdict): it holds the read lock across the caller's use, so a concurrent
`Destroy` or `Freeze` waits for the callback to return. Each borrow is scoped to
the code that uses the bytes, and nothing is copied out of it.

*Rejected: an accessor that returns the aliasing slice.* Such a slice is covered
by no lock once the accessor returns, so its safety rests on the caller showing
that no concurrent teardown exists, which is a property of the whole program
rather than of the call site. go-enclave offers no such accessor.

### Locked memory is charged by the page, and the ceiling is process-wide

`mlock` works on pages, so the kernel charges a whole page per enclave: a 32-byte
key costs 4096 bytes on a 4 KiB-page host, and go-enclave's `LockedSize` reports
the real figure. Guard and canary pages are mapped `PROT_NONE` and never locked,
so they cost address space and VMAs, not `RLIMIT_MEMLOCK`. Budget in pages, never
in the sizes of the things being protected.

Reaching the ceiling fails exactly the allocation that reached it, with a typed
`ErrLockedMemoryUnavailable`, and touches no other enclave.

go-enclave offers an `Allocator` to bound one path's share of the ceiling. This
repository does not construct or install one, because nothing here allocates
locked memory on behalf of an unauthenticated peer. Where the arithmetic can
multiply out of hand it uses a priced refusal instead: `MaxShareSetBytes` refuses
an over-budget Shamir split before taking a single page. A consumer that does
buffer key material before authenticating a peer should admit those allocations
through an `Allocator` at its own layer, and can extend that budget to share
material: `SplitProtectedWith`, `CombineProtectedWith`, `NewProtectedShareWith`\nand `UnmarshalShareSetWith` charge every region they lock to
the `Allocator` the caller passes.

This repository never calls `WipeAll`. It is a shutdown sweep for whoever owns
the process, and a library that called it would destroy enclaves its embedder
still holds.

### Releases are reported

A `ByteAccess`'s `Destroy` returns an error. `ErrLockedMemoryUnavailable` means the
kernel refused the release and the pages are still held; go-enclave keeps them
on its `WipeAll` ledger and a later `Destroy` retries. `ErrReleasedWithoutWipe`
means a denied `mprotect` forced a bare unmap: the pages are gone, and the
kernel zeroes them before reuse. The two are disjoint. A third,
`ErrEnclaveCorrupted`, means the canary or guard pages were overwritten; the
pages are still wiped and released.

`Sealed.Destroy` returns an error too. `pkg/seal` logs a non-nil one at WARN through its own helper
(`releaseSealed`), and `SoftwareStrategy.Close` returns it.

Every `ByteAccess` this repository destroys is a cleanup step whose operation
already has its result, so the report never replaces that result. `pkg/seal`
logs it at WARN through one helper; `pkg/crypto/shamir`, a primitive with no
logger, drops it through one helper whose comment says why nothing is lost.
`ProtectedShare.Destroy` returns nothing and so reports nothing; a caller that
must observe a refused release holds the enclave itself.

### Contexts reach the sealer

Every go-enclave call that can reach the process sealer's provider takes a
`context.Context`: `NewSealed`, `Sealed.Open` and `OpenIn`. A provider
can block for as long as its device does, so this repository passes the
caller's context wherever it has one: `seal.NewCredentials`, the bound
`NewSoftwareStrategy*` constructors, the `SealingStrategy` methods,
`Barrier.Initialize`, `Unseal`, `Rotate`, `PurgeEpochsBefore`, `RootKey`,
`RootKeyProtected` and `StampSealedStateMAC`. The host's key-rotation loop and
the snapshot pool, which rotate and purge in the background, pass a context
cancelled when they stop. The forced rotation `Unseal` runs when an epoch's
invocation budget cannot be recovered uses `context.WithoutCancel` of the
unseal's context: the barrier is already unsealed by then, and a rotation cut
short would only leave encryption refused.

### Constructors consume their input

`enclave.NewSealed` and `enclave.NewMemory` wipe their argument on every
return path, failures included. A caller that hands over key bytes then holds no
second copy it has to remember to clear, which is the most common way a key
leaks. `seal.NewCredentials` inherits this: the passphrase a caller supplies is
gone from the caller's slice by the time the constructor returns.

Two exceptions, named rather than implied. `enclave.NewMemoryCopy`
protects a copy and leaves the source alone, for callers whose source has to
survive. `newSoftwareStrategy` copies before sealing, because its public
constructors take a passphrase the caller may still be using.

## Component boundaries

- **go-enclave (Layer 0)** owns every protected-memory type, the `mlock`, the
  guard pages, the canary, the wipe, the sealing hierarchy and the locking
  discipline. It is documented and tested there.
- **`pkg/seal`** holds `Barrier.rootKey`, `Barrier.currentDEK`, the per-epoch
  `deks` map, `SoftwareStrategy.passphrase` and `Credentials.Secret`.
- **`pkg/crypto/shamir`** holds a `ProtectedShare`'s value and the secret
  `CombineProtected` reconstructs, both frozen `ByteAccess` enclaves the caller owns.
- **Consumers above** (go-qrdb, go-xkms, go-quicbaas, go-trusted-platform) import
  go-enclave directly, never through go-quicraft. This repository re-exports none
  of it.

## What this deliberately does not do

- **No hardware custody.** No TPM, PKCS#11, HSM, TEE or cloud KMS. That is
  go-xkms.
- **No protected-memory implementation.** This repository allocates, borrows,
  freezes and destroys; it does not map, lock or encrypt. That is go-enclave.
- **No persistence.** Nothing here writes protected material to disk. A key that
  must survive the process is sealed by a `SealingStrategy`.
- **No cross-process sharing.** There is no shared-memory mode and no handle
  another process can open.
