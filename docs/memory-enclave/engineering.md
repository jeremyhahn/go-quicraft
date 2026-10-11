# Memory enclave: engineering contract

> **Status: NORMATIVE** · Scope: go-quicraft's use of go-enclave protected memory · Last verified: 2026-10-11
> Conflicts with this document are defects. Fix them here first.

Import path: `github.com/jeremyhahn/go-enclave` (v0.8.0-alpha in `go.mod`).

go-quicraft implements none of this. There is no `pkg/enclave` and no
protected-memory type, alias or wrapper in this repository, and
`github.com/awnumar/memguard` is not in its dependency graph. This page is the
contract this repository codes against and the obligations it accepts.

## 1. Surface used

```go
// Locked plaintext
func enclave.NewMemoryEnclaveSized(n int) (*MemoryEnclave, error)      // writable; fill in place
func enclave.NewMemoryEnclave(data []byte) (*MemoryEnclave, error)     // moves data in; FROZEN
func enclave.NewMemoryEnclaveCopy(src []byte) (*MemoryEnclave, error)  // copies; FROZEN; src untouched
func (e *MemoryEnclave) WithBytes(fn func([]byte) error) error // the only read path
func (e *MemoryEnclave) Freeze() error               // post-fill mprotect; irreversible
func (e *MemoryEnclave) EqualTo(other []byte) (bool, error)
func (e *MemoryEnclave) Size() int
func (e *MemoryEnclave) IsAlive() bool
func (e *MemoryEnclave) Destroy() error              // reports a refused or unwiped release

// Ciphertext at rest
func enclave.NewSealed(ctx context.Context, data []byte) (*SealedEnclave, error)
func (s *SealedEnclave) Open(ctx context.Context) (*MemoryEnclave, error) // caller OWNS the result; FROZEN
func (s *SealedEnclave) Openable() error
func (s *SealedEnclave) IsAlive() bool
func (s *SealedEnclave) Destroy() error                  // WIPES the ciphertext; always nil today

// Utility
func enclave.Wipe(b []byte)
func enclave.LockedSize(n int) int64

// Sentinels
var ErrEnclaveDestroyed, ErrEmptyEnclave, ErrInvalidSize,
    ErrLockedMemoryUnavailable, ErrReleasedWithoutWipe, ErrSealFailed,
    ErrOpenFailed, ErrSealedAuthentication, ErrSealerWiped error
```

## 2. Invariants

**Call safety versus slice safety.** A `MemoryEnclave` guards its mapping with
its own `sync.RWMutex` and a `SealedEnclave` its envelope with another, both
inside go-enclave. Readers hold the read lock for the call and `Destroy` takes
the write lock, so teardown waits out in-flight readers. Every call is safe under
concurrency. A slice that outlives the call is not, and since v0.8.0 go-enclave
hands out no such slice: `WithBytes` is the only read path, and the view it
passes is valid for the callback alone.

| Operation | Concurrency | Idempotent |
|---|---|---|
| `SealedEnclave.Open` | safe with `Open`, `Destroy` | n/a; each call returns a new enclave |
| `SealedEnclave.Destroy` | does not wait for an `Open` in flight: that open completes against the envelope it pinned, a later one returns `ErrEnclaveDestroyed`, and the last open out wipes the ciphertext | yes; a `nil` receiver is a no-op |
| `MemoryEnclave.WithBytes` / `EqualTo` / `Size` / `IsAlive` | readers; hold the read lock for the whole call, so a concurrent `Destroy` blocks | n/a |
| `MemoryEnclave.Freeze` | writer; waits out every in-flight borrow | yes; irreversible |
| `MemoryEnclave.Destroy` | writer; blocks until every in-flight reader returns | yes; a `nil` receiver and every call after a successful release return `nil`; a refused release is retried by calling it again |

- **The lock bounds the callback, not the slice.** `Destroy` wipes and unmaps
  the memory, so a view retained past its callback is a use-after-free that
  faults rather than reading zeros. Every borrow is scoped to the code that uses
  the bytes: derive, compare or copy into another enclave inside the callback,
  and return. Nothing is copied out to the heap to work on later.
  `ProtectedShare.WithValue` is the same borrow on a share.
- **A callback must not re-enter the same enclave**: no `WithBytes`, `Size`,
  `IsAlive`, `EqualTo`, `Freeze` or `Destroy` on the enclave it is borrowing.
  The read lock is held for the call, so a nested writer deadlocks, and a nested
  reader deadlocks whenever a writer is already queued.
- **A callback may use a different enclave.** `Barrier.Initialize` and
  `Barrier.Unseal` rely on this: they hold the root key in a locked enclave
  across their whole body and seal other enclaves inside that borrow. There is no
  process-wide lock that would make that a self-deadlock.
- **There is no process-wide purge.** go-enclave maps its own memory, and an
  allocation that fails, fails alone.
- **`WipeAll` is the one other thing that can destroy an enclave.** go-enclave's
  shutdown sweep destroys every live `MemoryEnclave` in the process, including a
  `Sealer`'s key hierarchy, after which a `SealedEnclave` still reports `IsAlive`
  while `Open` fails with `ErrSealerWiped`. This repository never calls it: it
  belongs to whoever owns the process.
- **Every `Open` is paired with a `Destroy`.** `SealedEnclave.Open` materializes
  plaintext into a fresh `MemoryEnclave` the caller owns. A leaked enclave keeps
  plaintext resident for the life of the process and holds a whole page of
  `RLIMIT_MEMLOCK`, which makes later locks fail with `ErrLockedMemoryUnavailable`.
- **Constructors consume their input.** `NewSealed(data)` and
  `NewMemoryEnclave(data)` wipe `data` on every return path, failures included.
  Two named exceptions: `NewMemoryEnclaveCopy` leaves its source alone, and
  `newSoftwareStrategy` copies before sealing because its public constructors take
  a passphrase the caller may still be using.
- **`Destroy` erases, on both types.** A destroyed `SealedEnclave` has its
  ciphertext and wrapped-key blob zeroed, not merely dereferenced, so
  `Barrier.seal()`, `PurgeEpochsBefore` and `SoftwareStrategy.Close` erase what
  they release. The one un-erasable copy is the DEK inside a cached
  `cipher.AEAD`'s key schedule, which no stdlib or `x/crypto` API can wipe.
- **A sized enclave is writable until frozen.** This repository freezes at every
  site where the material it generates is final: the share value, the
  reconstructed secret and the unsealed root key (`UnsealRootKeyProtected`). It
  deliberately does not freeze the root-key enclave `Initialize`/`Unseal` hold,
  whose last act is to be consumed by `NewSealed`; a wipe of frozen pages faults
  the process.
- **`SealedEnclave.Open` returns a FROZEN enclave.** go-enclave freezes the
  destination before returning it and fails the open closed when the kernel
  refuses the freeze, so `Barrier.RootKeyProtected` returns `Open`'s result as it
  is. Code must never write into an opened enclave; a caller that needs a
  writable copy allocates its own sized enclave and copies into it inside a
  borrow.
- **Every call that can reach a provider takes a context.** `NewSealed`, `Open`
  and `OpenWith` are bounded by the context they are given, and this repository
  passes the caller's wherever it has one. A context that has ended fails a seal
  with an error matching `ErrSealFailed` and `ctx.Err()`, and an open with one
  matching `ErrOpenFailed` and `ctx.Err()`; neither matches
  `ErrSealedAuthentication`. `Rotate` and `PurgeEpochsBefore` take one; the host's
  rotation loop and the snapshot pool pass a context cancelled when they stop,
  and the forced rotation inside `Unseal` passes
  `context.WithoutCancel` of the unseal's context.
- **`MemoryEnclave.Destroy` reports its release.** It returns
  `ErrLockedMemoryUnavailable` when the kernel refused the release (the pages are
  still held and on go-enclave's `WipeAll` ledger; calling `Destroy` again
  retries) and `ErrReleasedWithoutWipe` when a denied `mprotect` forced a bare
  unmap (the pages are gone). The two are disjoint. Every site here is a cleanup
  step whose operation already has its result: `pkg/seal` logs the report at
  WARN through `releaseEnclave`, and `pkg/crypto/shamir` drops it through
  `release`, whose comment records why nothing is lost.
- **A refused `Freeze` leaves the enclave alive and writable.** Every call site
  here destroys it rather than return material whose protection was not
  established.
- **`Destroy` panics on a canary failure**, the one panic in the path. A
  mismatch means something wrote into memory it did not own, which the caller
  cannot act on because the state it would act on is the state that is wrong. The
  material is wiped and the pages released before the panic, so nothing leaks.
- **A `nil` `*MemoryEnclave` or `*SealedEnclave` is a valid receiver.** It
  reports itself not alive, reads with `ErrEnclaveDestroyed` and destroys as a
  no-op, which is why `Barrier.seal()` destroys `b.rootKey` and `b.currentDEK`
  without nil checks.
- **No signal handler is installed.** Neither go-enclave nor this repository
  calls `signal.Reset` or registers a handler: a library does not get to detach
  the embedding application's handlers.

## 3. Error semantics

| Error | Type | Returned when | Fail-closed? |
|---|---|---|---|
| `ErrEnclaveDestroyed` | sentinel | any read on a destroyed or nil enclave, either type | Yes; the callback is not called |
| `ErrLockedMemoryUnavailable` | `*LockedMemoryError` | the kernel refuses to map, lock or mprotect the pages, usually at `RLIMIT_MEMLOCK` | Yes; that allocation alone fails |
| `ErrEmptyEnclave` / `ErrInvalidSize` | sentinels | an enclave over no material, or a size no enclave can have | Yes; refused before anything is mapped |
| `ErrSealFailed` | sentinel | the AEAD, the CSPRNG or the provider failed while sealing, or the seal's context ended | Yes |
| `ErrOpenFailed` | sentinel | an open's context ended, or a `ModePerOperation` provider's infrastructure failed | Yes; the sealed value stays intact and can be opened again |
| `ErrSealedAuthentication` | sentinel | a sealed envelope did not authenticate | Yes; deliberately indistinguishable between causes |
| `ErrSealerWiped` | sentinel | a shutdown sweep destroyed the sealer's root or current KEK | Yes; a per-call key destroyed by a sweep is `ErrEnclaveDestroyed` instead |
| `ErrReleasedWithoutWipe` | sentinel | `MemoryEnclave.Destroy` unmapped the pages without a wipe because `mprotect` was denied | Not a failure to protect: the pages are gone and the kernel zeroes them before reuse |

How `pkg/seal` and `pkg/crypto/shamir` translate them:

- A destroyed passphrase enclave becomes `ErrStrategyClosed`
  (`closedIfEnclaveGone`). `Close` destroys the `SealedEnclave`, so a `Close`
  that lands after a seal or unseal has loaded the pointer surfaces as
  `ErrEnclaveDestroyed` from `Open`. That is the same condition as "used after
  Close" and reports the same error. The mapping applies only when the
  passphrase enclave itself is no longer alive: a concurrent `WipeAll` that
  destroys one of the open's per-call enclaves while the passphrase is intact
  stays `ErrEnclaveDestroyed`, since no `Close` happened. Both fail the call.
- An `Unseal` whose context ended is not a failed brute-force attempt: an
  already-ended context is refused before the strategy runs, and a strategy
  error matching `ctx.Err()` is returned without touching the backoff counter
  — unless it also matches `enclave.ErrSealedAuthentication` or
  `seal.ErrDecryptionFailed`, which still count.
- A refused allocation on a barrier path becomes `*seal.Error{Strategy: ...}` or
  `*seal.UnsealError`, and the barrier stays uninitialized or sealed, never
  half-installed. Every sealing call in `Initialize`, `Unseal` and `Rotate` runs
  before the first state assignment for that reason.
- An over-budget Shamir split is a `*SplitError` (a rejected request); a refused
  lock is a `*ProtectedMemoryError` (an exhausted host). The first is the
  caller's arithmetic, the second the machine's, and `MaxShareSetBytes` is priced
  so the first is decided before a single page is taken.

There is no error path that yields a partially-protected key. Either the
material is inside a protected container or the call failed.

## 4. Configuration

None: no configuration keys, environment variables or files.

The one operational interaction is `RLIMIT_MEMLOCK`. The kernel charges a whole
page per allocation (a 32-byte key costs 4096 bytes on a 4 KiB-page host). A
process or container with a low limit sees allocations fail with
`ErrLockedMemoryUnavailable` once the limit is reached; only the allocation that
reached the ceiling fails. Raise the limit rather than reduce the number of
protected keys, and size it in pages: `enclave.LockedSize` reports what one
enclave costs. Guard and canary pages are mapped `PROT_NONE` and never locked, so
they consume address space rather than the limit.

go-enclave's `Allocator` bounds what one code path may hold against that
ceiling. This repository constructs none, because nothing here allocates locked
memory for an unauthenticated peer. Where legal arguments can multiply into an
illegal footprint it prices and refuses the whole operation up front instead
(`MaxShareSetBytes` against `splitLockedBytes`). A consumer that buffers key
material before authenticating a peer should admit those allocations through an
`Allocator` at its own layer, and can pass the same allocator to
`shamir.SplitProtectedWith`, `CombineProtectedWith`, `NewProtectedShareWith`,
`UnmarshalShareSetWith` and `Share.ProtectWith` so its budget covers share
material too. A nil allocator is refused there, never treated as unbudgeted.
`ProtectedShare.UnmarshalJSON` is the one share constructor that cannot take an
allocator; a budgeted consumer decodes through `UnmarshalShareSetWith` instead.

## 5. Persistence

None. Nothing protected here is written to disk or survives the process. Key
material that must outlive the process is sealed by a `SealingStrategy`; see
[`../seal-barrier/engineering.md`](../seal-barrier/engineering.md).

## 6. Test strategy

This feature has no test target of its own. Every property of the protected
memory itself belongs to go-enclave and is tested there; re-asserting it here
would test a dependency through its consumer.

This repository tests its own use of the contract, in the packages that hold key
material:

| Test | Pins |
|---|---|
| `seal.TestInitializeAndUnseal_ReleaseTheirRootKeyEnclave` | the borrow discipline, against the kernel's own `VmLck` accounting: a missed `Destroy` leaks a page per cycle, which no functional assertion would catch |
| `seal.TestNewCredentials_*` | that a caller's passphrase slice is wiped by the constructor |
| `seal.TestSoftwareStrategy_*Protected*` | that the protected pair never copies the root key or the passphrase out of its enclave |
| `shamir.TestSplitProtected_BudgetIsATrueUpperBound` | that `splitLockedBytes` prices every region live at the peak, by checking that every region the allocator seam handed out is still alive when the split returns |
| `shamir.TestSplitProtected_AllocationFailureDestroysPartialShares` | that a mid-loop refusal tears down the shares already built, which the caller never receives and could never `Destroy` |
| `shamir.TestNewProtectedShare_AllocationFailureIsNotValidation` | the ownership contract the two error types encode: a refused lock consumed the source, a validation failure did not |
| `shamir.TestBorrow_*`, `shamir.Test{Split,Combine}ProtectedWith_Release*` | that a borrow ends with its callback, so `Freeze` and `Destroy` go through straight after it, and that every scratch split and combine borrow is released by the time they return |

**Fault-injection seams.** The failure that matters, `RLIMIT_MEMLOCK`
exhaustion, cannot be produced honestly in a shared test binary: reaching it for
real fails allocations in every unrelated test. So each package that must prove
its refusal path is clean owns a `testing.Testing()`-gated seam at its own
allocation site:

- `seal.Barrier.lockRootKey`: a field, not a package variable, so it cannot be
  reached from another test or from production code that did not ask for it.
  `barrier_borrow_test.go` sets it to a function that refuses.
- `shamir.newProtectedMemory` and `shamir.newSizedProtectedMemory`: package
  variables behind `testOnlyProtectedMemory` / `testOnlySizedProtectedMemory`,
  which panic outside a test binary. They are separate because the two
  constructors have different callers: a share value is produced through the
  sized seam and frozen in place, so an injection on the from-bytes seam would
  refuse nothing and the test would pass without exercising anything.

## 7. Observability

No logging, metrics or audit records for protected memory itself. Every event it
could report is about a key, and the useful fields (identity, size, lifetime) are
exactly the ones that should not reach a log. `pkg/seal` logs barrier lifecycle
events without naming key material; see
[`../observability/engineering.md`](../observability/engineering.md).
