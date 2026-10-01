# Memory Enclave — Engineering Contract

> **Status: NORMATIVE** · Scope: go-quicraft's use of go-enclave protected memory · Last verified: 2026-09-03
> Conflicts with this document are defects. Fix them here first.

Import path: `github.com/jeremyhahn/go-enclave`.

**go-quicraft implements none of this.** `pkg/enclave` was deleted in #54 and
nothing replaced it: there is no protected-memory type, alias, or wrapper in this
repository, and `github.com/awnumar/memguard` is not in its dependency graph. What
this page documents is the CONTRACT this repository codes against and the
obligations it accepts, not an implementation it owns.

## 1. Surface used

```go
// Locked plaintext
func enclave.NewMemoryEnclaveSized(n int) (*MemoryEnclave, error)      // writable; fill in place
func enclave.NewMemoryEnclave(data []byte) (*MemoryEnclave, error)     // moves data in; FROZEN
func enclave.NewMemoryEnclaveCopy(src []byte) (*MemoryEnclave, error)  // copies; FROZEN; src untouched
func (e *MemoryEnclave) WithBytes(fn func([]byte) error) error
func (e *MemoryEnclave) Open() ([]byte, error)       // UNSAFE: the slice outlives the lock
func (e *MemoryEnclave) Freeze() error               // post-fill mprotect; irreversible
func (e *MemoryEnclave) EqualTo(other []byte) bool
func (e *MemoryEnclave) Size() int
func (e *MemoryEnclave) IsAlive() bool
func (e *MemoryEnclave) Destroy()

// Ciphertext at rest
func enclave.NewSealed(data []byte) (*SealedEnclave, error)
func (s *SealedEnclave) Open() (*MemoryEnclave, error)   // caller OWNS the result
func (s *SealedEnclave) Openable() error
func (s *SealedEnclave) IsAlive() bool
func (s *SealedEnclave) Destroy()                        // WIPES the ciphertext

// Utility
func enclave.Wipe(b []byte)
func enclave.LockedSize(n int) int64

// Sentinels
var ErrEnclaveDestroyed, ErrEmptyEnclave, ErrInvalidSize,
    ErrLockedMemoryUnavailable, ErrSealFailed, ErrSealedAuthentication error
```

**What is gone from this repository rather than renamed**, because a reader
coming from an older revision of this page will look for it: `pkg/enclave`
entirely — `ProtectedKey`, `NewProtectedKey`, `SealMemoryEnclave`,
`SealProtectedBuffer`, `WipeBytes`, `MemoryEnclave`/`ProtectedBuffer` as aliases,
`ErrEnclaveNil`, `NilError`, `OpenError`, `purgeMu` — and `pkg/seal`'s four
re-exports of them. `ProtectedKey`'s replacement is `enclave.SealedEnclave`,
which does the same job and additionally WIPES on teardown. The full translation
table is in [`api.md`](api.md).

## 2. Invariants

**Call safety vs. slice safety.** A `MemoryEnclave` guards its mapping with its
own `sync.RWMutex` and a `SealedEnclave` its envelope with another, both inside
go-enclave. Readers hold the read lock for the duration of the call and `Destroy`
takes the write lock, so teardown waits out in-flight readers instead of pulling
memory out from under them. Every *call* is therefore safe under concurrency. A
*slice* that outlives the call is not — and `MemoryEnclave.Open` hands back
exactly such a slice, on purpose.

| Operation | Concurrency | Idempotent |
|---|---|---|
| `SealedEnclave.Open` | safe with `Open`, `Destroy` | n/a — each call returns a new enclave |
| `SealedEnclave.Destroy` | writer — waits out an `Open` in flight | yes; a `nil` receiver is a no-op |
| `MemoryEnclave.WithBytes` / `OpenString` / `EqualTo` / `Size` / `IsAlive` | readers — hold the read lock for the whole call; a concurrent `Destroy` blocks | n/a |
| `MemoryEnclave.Freeze` | writer — waits out every in-flight borrow | yes; irreversible |
| `MemoryEnclave.Destroy` | writer — blocks until every in-flight reader returns | yes; a `nil` receiver is a no-op |
| `MemoryEnclave.Open` | returns a slice covered by **no** lock once it returns | n/a |

- **The lock bounds the CALL, not the slice.** `Destroy` wipes and unmaps the
  memory, so a slice retained past the accessor that returned it is a
  use-after-free that FAULTS rather than reading zeros. Where teardown can race a
  reader — barrier and custody ceremony paths — use `WithBytes` /
  `ProtectedShare.WithValue`, which hold the read lock across the callback.
  `Open` is for code that owns the enclave outright and can show no concurrent
  teardown exists.
- **A callback must not re-enter the SAME enclave** — no `WithBytes`, `Size`,
  `IsAlive`, `EqualTo`, `Freeze` or `Destroy` on the enclave it is borrowing. The
  read lock is held for the call, so a nested writer deadlocks and a nested
  reader deadlocks whenever a writer is already queued.
- **A callback MAY touch a DIFFERENT enclave.** This REVERSES what this document
  said before #54, and the reversal is load-bearing rather than incidental:
  `Barrier.Initialize` and `Barrier.Unseal` hold the root key in a locked enclave
  across their whole body and seal other enclaves inside that borrow. Under the
  old process-wide `purgeMu` — held shared across every borrow, against Go's
  `RWMutex` writer preference — that was a self-deadlock (#18, #39). `purgeMu`
  went with the last memguard call site in #54 stage two.
- **There is no process-wide purge.** go-enclave maps its own memory, and an
  allocation that fails fails alone. The old hazard — a failed memguard
  allocation in one goroutine unmapping pages another was reading — has no
  remaining call site in this repository.
- **`WipeAll` is the one other thing that can destroy an enclave.** go-enclave's
  shutdown sweep destroys every live `MemoryEnclave` in the process, including a
  `Sealer`'s key hierarchy — after which a `SealedEnclave` still reports
  `IsAlive` while `Open` fails with `ErrSealerWiped`. This repository never calls
  it: it belongs to whoever owns the process, and a library calling it would
  destroy enclaves its embedder still holds.
- **Every `Open` is paired with a `Destroy`.** `SealedEnclave.Open` materializes
  plaintext into a fresh `MemoryEnclave`; the caller owns it. A leaked enclave
  keeps plaintext resident for the life of the process and holds a whole page of
  `RLIMIT_MEMLOCK`, which then makes later locks fail with a typed
  `ErrLockedMemoryUnavailable`.
- **Constructors consume their input.** `NewSealed(data)` and
  `NewMemoryEnclave(data)` wipe `data` on every return path including their
  failures. Callers must not rely on the argument being readable afterwards. The
  two exceptions are named: `NewMemoryEnclaveCopy` leaves its source alone, and
  `newSoftwareStrategy` copies before sealing because its public constructors take
  a passphrase the caller may still be using.
- **`Destroy` erases, on both types.** A destroyed `SealedEnclave` has had its
  ciphertext and wrapped-key blob zeroed, not merely dereferenced. Every
  "GC-bounded ciphertext" caveat that used to qualify `Barrier.seal()`,
  `PurgeEpochsBefore` and `SoftwareStrategy.Close` is retired; the one un-erasable
  copy that remains is the DEK inside a cached `cipher.AEAD`'s key schedule,
  which no stdlib or `x/crypto` API can wipe.
- **A sized enclave is WRITABLE until frozen.** `Freeze` is what makes a stray
  write fault instead of silently corrupting key material, and this repository
  freezes at every site where the material is final: the share value, the
  reconstructed secret, the unsealed root key. It deliberately does NOT freeze the
  root-key enclave `Initialize`/`Unseal` hold, whose last act is to be consumed by
  `NewSealed` — a wipe of frozen pages faults the process.
- **A refused `Freeze` leaves the enclave alive and writable.** Every call site
  here destroys it rather than returning material whose promised protection was
  not established.
- **`Destroy` panics on a canary failure**, and this is the one panic in the
  path. go-enclave verifies the canary ahead of the data when it releases the
  pages; a mismatch means something wrote into memory it did not own, which is
  not a condition the caller can act on, because the state it would act on is the
  state that is wrong. The material is wiped and the pages are released before the
  panic, so nothing leaks either way.
- **A `nil` `*MemoryEnclave` or `*SealedEnclave` is a valid receiver.** It reports
  itself not alive, reads with `ErrEnclaveDestroyed`, and destroys as a no-op, so
  an optional enclave needs no guard at every use — which is why `Barrier.seal()`
  destroys `b.rootKey` and `b.currentDEK` without nil checks.
- **No signal handler is installed.** Neither go-enclave nor this repository calls
  `signal.Reset`. memguard's `CatchSignal`/`CatchInterrupt` detached **every**
  handler in the process, including the embedding application's; a library does
  not get to make that decision for its embedder.

## 3. Error semantics

| Error | Type | Returned when | Fail-closed? |
|---|---|---|---|
| `ErrEnclaveDestroyed` | sentinel | any read on a destroyed or nil enclave, either type | Yes — the callback is not called |
| `ErrLockedMemoryUnavailable` | `*LockedMemoryError` | the kernel refuses to map, lock or mprotect the pages, usually at `RLIMIT_MEMLOCK` | Yes — that allocation alone fails; nothing else is touched |
| `ErrEmptyEnclave` / `ErrInvalidSize` | sentinels | an enclave over no material, or a size no enclave can have | Yes — refused before anything is mapped |
| `ErrSealFailed` | sentinel | the AEAD, the CSPRNG or the provider failed while sealing | Yes |
| `ErrSealedAuthentication` | sentinel | a sealed envelope did not authenticate | Yes — deliberately indistinguishable between causes |

How `pkg/seal` translates them at its own boundary:

- A destroyed passphrase enclave becomes `ErrStrategyClosed`
  (`closedIfEnclaveGone`). `Close` destroys the `SealedEnclave`, so a `Close`
  landing after a seal or unseal has loaded the pointer surfaces as
  `ErrEnclaveDestroyed` from `Open`, which is the same condition as "used after
  Close" and must report the same error.
- A refused allocation on a barrier path becomes `*seal.Error{Strategy: ...}` or
  `*seal.UnsealError`, and the barrier stays uninitialized or sealed — never
  half-installed. Every sealing call in `Initialize`, `Unseal` and `Rotate` runs
  BEFORE the first state assignment for exactly that reason.
- An over-budget Shamir split is a `*SplitError` (a rejected REQUEST), while a
  refused lock is a `*ProtectedMemoryError` (an exhausted HOST). The distinction
  is the contract: the first is the caller's arithmetic, the second is the
  machine's, and `MaxShareSetBytes` is priced so the first is decided before a
  single page is taken.

**There is no error path that yields a partially-protected key.** Either the
material is inside a protected container or the call failed.

## 4. Configuration

**None.** No configuration keys, no environment variables, no files.

The one environment interaction is operational rather than configured: locked
memory consumes `RLIMIT_MEMLOCK`, and the kernel charges a whole PAGE per
allocation — a 32-byte key costs 4096 bytes on a 4 KiB-page host. A process (or
container) with a low limit sees allocations fail with a typed
`ErrLockedMemoryUnavailable` once the limit is reached; that fails exactly the
allocation which reached the ceiling and touches no other enclave. Raise the
limit rather than reducing the number of protected keys, and size it in PAGES
rather than in key sizes — `enclave.LockedSize` reports what one enclave actually
costs. Guard and canary pages are mapped `PROT_NONE` and never locked, so they
consume address space rather than the limit.

go-enclave's `Allocator` bounds what one code path may hold against that ceiling.
**This repository constructs none**: nothing here allocates locked memory for an
unauthenticated peer, so there is no path to bound. What it does instead, where
legal arguments can multiply into an illegal footprint, is price and refuse the
whole operation up front — `MaxShareSetBytes` against `splitLockedBytes`. A
consumer that buffers key material before authenticating a peer should admit
those allocations through an `Allocator` at its own layer, and can pass that
same allocator to `shamir.SplitProtectedWith`, `CombineProtectedWith`,
`NewProtectedShareWith` (issue #204), `UnmarshalShareSetWith` and
`Share.ProtectWith` (issue #211) so its budget covers share material as well.
A nil allocator is refused there, never treated as unbudgeted.
`ProtectedShare.UnmarshalJSON` is the one share constructor that cannot take an
allocator; a budgeted consumer decodes through `UnmarshalShareSetWith` instead.

## 5. Persistence

**None.** Nothing protected here is written to disk, and nothing it holds
survives the process. Key material that must outlive the process is sealed by a
`SealingStrategy` — see
[`../seal-barrier/engineering.md`](../seal-barrier/engineering.md).

## 6. Test strategy

There is no test target for this feature any more, and that is the correct
outcome rather than a coverage regression: `make test-enclave` and
`make coverage-enclave` were removed with `pkg/enclave/`. Every property they
asserted belongs to go-enclave and is pinned there — `enclave_freeze_*_test.go`,
`enclave_lockfail_linux_test.go`, `sealed_*_test.go`, `registry_linux_test.go`,
`admission_accounting_linux_test.go`. Re-asserting them here would be testing a
dependency through its consumer.

What this repository tests is its own USE of the contract, in the packages that
hold key material:

| Test | Pins |
|---|---|
| `seal.TestInitializeAndUnseal_ReleaseTheirRootKeyEnclave` | the borrow discipline, against the kernel's own `VmLck` accounting — a missed `Destroy` leaks a page per cycle and shows up as growth no functional assertion would catch |
| `seal.TestNewCredentials_*` | that a caller's passphrase slice is wiped by the constructor (#41's verification criterion) |
| `seal.TestSoftwareStrategy_*Protected*` | that the protected pair never copies the root key or the passphrase out of its enclave |
| `shamir.TestSplitProtected_BudgetIsATrueUpperBound` | that `splitLockedBytes` prices every region live at the peak — enforced by checking that every region the allocator seam handed out is still alive when the split returns, which is what "there is no scratch" means operationally |
| `shamir.TestSplitProtected_AllocationFailureDestroysPartialShares` | that a mid-loop refusal tears down the shares already built, which the caller never receives and could never `Destroy` |
| `shamir.TestNewProtectedShare_AllocationFailureIsNotValidation` | the ownership contract the two error types encode: a refused lock consumed the source, a validation failure did not |

**Fault-injection seams.** The failure that matters — `RLIMIT_MEMLOCK`
exhaustion — cannot be produced honestly in a shared test binary: reaching it for
real fails allocations in every unrelated test in the process. So each package
that must prove its refusal path is clean owns a `testing.Testing()`-gated seam at
its own allocation site rather than reaching into the dependency:

- `seal.Barrier.lockRootKey` — a FIELD, not a package variable, so it cannot be
  reached from another test or from production code that did not ask for it.
  `barrier_borrow_test.go` sets it to a function that refuses.
- `shamir.newProtectedMemory` and `shamir.newSizedProtectedMemory` — package
  variables behind `testOnlyProtectedMemory` / `testOnlySizedProtectedMemory`,
  which PANIC outside a test binary. They are separate because the two
  constructors have different callers: a share value is now produced through the
  SIZED seam and frozen in place, so an injection on the from-bytes seam would
  refuse nothing and the test would pass by exercising nothing.

## 7. Observability

**No logging, no metrics, no audit records** for protected memory itself.
Deliberately: every event it could report is about a key, and the useful fields
(identity, size, lifetime) are exactly the fields that should not reach a log.
`pkg/seal` logs barrier lifecycle events without naming key material. See
[`../observability/engineering.md`](../observability/engineering.md).
