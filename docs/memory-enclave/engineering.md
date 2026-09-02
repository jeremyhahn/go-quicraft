# Memory Enclave — Engineering Contract

> **Status: NORMATIVE** · Scope: go-quicraft `pkg/enclave` · Last verified: 2026-09-02
> Conflicts with this document are defects. Fix them here first.

Import path: `github.com/jeremyhahn/go-quicraft/pkg/enclave`
Locked memory is `github.com/jeremyhahn/go-enclave`; `ProtectedKey`'s
encrypted-at-rest container is still `github.com/awnumar/memguard`.

## 1. Exported surface

```go
// Types. Locked memory is go-enclave's; these are ALIASES, not definitions.
type MemoryEnclave   = goenclave.MemoryEnclave
type ProtectedBuffer = goenclave.MemoryEnclave // Deprecated: spelling only
type ProtectedKey    struct{ /* unexported */ }

// Owned here
func NewProtectedKey(key []byte) *ProtectedKey
func SealMemoryEnclave(buf *MemoryEnclave) *ProtectedKey
func SealProtectedBuffer(buf *MemoryEnclave) *ProtectedKey // Deprecated: spelling only
func (p *ProtectedKey) Open() (*MemoryEnclave, error)
func (p *ProtectedKey) Clear()
func WipeBytes(b []byte) // go-enclave's Wipe, re-exported

// Locked memory is allocated with go-enclave's constructors, by name
func goenclave.NewMemoryEnclaveSized(n int) (*MemoryEnclave, error)      // writable; fill in place
func goenclave.NewMemoryEnclave(data []byte) (*MemoryEnclave, error)     // moves data in; FROZEN
func goenclave.NewMemoryEnclaveFromString(s string) (*MemoryEnclave, error)

// MemoryEnclave methods, all go-enclave's
func (e *MemoryEnclave) WithBytes(fn func([]byte) error) error
func (e *MemoryEnclave) Open() ([]byte, error)       // UNSAFE: the slice outlives the lock
func (e *MemoryEnclave) OpenString() (string, error) // unprotected copy; boundary use only
func (e *MemoryEnclave) EqualTo(other []byte) bool
func (e *MemoryEnclave) Size() int
func (e *MemoryEnclave) IsAlive() bool
func (e *MemoryEnclave) Destroy()

// Errors
var ErrEnclaveNil       = &NilError{}                   // this package's
var ErrEnclaveDestroyed = goenclave.ErrEnclaveDestroyed // re-exported, not redefined
type NilError  struct{}
type OpenError struct{ Err error }
```

**What is gone rather than renamed**, because a reader coming from an older
revision of this page will look for it: `enclave.NewMemoryEnclave(size int)`,
`NewMemoryEnclaveFromBytes`, `MemoryEnclave.Bytes()`, `MemoryEnclave.Freeze()`
and the `DestroyedError` type. Locking pages is a syscall the kernel can decline,
so the constructors return an error and a bare-pointer alias could not express
one; `Bytes` became go-enclave's `Open`, which reports a destroyed enclave as an
error instead of as a `nil` slice; and there is no post-fill freeze at all
(go-enclave #6), so the fill-then-freeze shape is a locked-to-locked move from a
sized enclave into `NewMemoryEnclave`. `ErrEnclaveDestroyed` is now go-enclave's
sentinel re-exported, so `errors.Is` matches whichever package the error came
from — two sentinels meaning the same thing is how a caller ends up handling one
and not the other.

`pkg/seal/memprotect.go` re-exports two types (`ProtectedBuffer`,
`ProtectedKey`) and two functions (`NewProtectedKey`, `SealProtectedBuffer`) as
aliases. They are the same types and the same functions; there is no second
implementation. `seal.NewProtectedBuffer` and `seal.NewProtectedBufferFromBytes`
are gone for the reason above, and `WipeBytes` is not re-exported there — `pkg/seal`
keeps an unexported `wipeBytes` wrapper for its own use.

## 2. Invariants

**Call safety vs. slice safety.** `ProtectedKey` holds its enclave in an
`atomic.Pointer`; a `MemoryEnclave` guards its mapping with its own
`sync.RWMutex`, inside go-enclave. Readers hold that read lock for the duration
of the call and `Destroy` takes the write lock, so teardown waits out in-flight
readers instead of unmapping pages underneath them. Every *call* is therefore
safe under concurrency. A *slice* that outlives the call is not — and
`MemoryEnclave.Open` hands back exactly such a slice, on purpose.

| Operation | Concurrency | Idempotent |
|---|---|---|
| `ProtectedKey.Open` | safe with `Open`, `Clear`; serialized against this package's other memguard calls by `purgeMu` | n/a — each call returns a new enclave |
| `ProtectedKey.Clear` | safe with `Open`, `Clear` | yes; it is a store of `nil` |
| `SealMemoryEnclave` | safe; serialized on `purgeMu`, so exactly one concurrent caller consumes the enclave | yes — after the first, the source is destroyed and later calls return `nil` |
| `MemoryEnclave.WithBytes` / `OpenString` / `EqualTo` / `Size` / `IsAlive` | readers — hold the enclave's read lock for the whole call; a concurrent `Destroy` blocks | n/a |
| `MemoryEnclave.Destroy` | writer — blocks until every in-flight reader returns | yes; a `nil` receiver is a no-op |
| `MemoryEnclave.Open` | returns a slice covered by **no** lock once it returns | n/a |

- **The lock bounds the CALL, not the slice.** `Destroy` wipes and unmaps the
  memory, so a slice retained past the accessor that returned it is a
  use-after-free that FAULTS rather than reading zeros. Where teardown can race a
  reader — barrier and custody ceremony paths — use `WithBytes` /
  `ProtectedShare.WithValue`, which hold the read lock across the callback.
  `WithBytes` returns `ErrEnclaveDestroyed` if the enclave is already gone, and
  the callback's own error unchanged otherwise. `Open` is for code that owns the
  enclave outright and can show no concurrent teardown exists.
- **A callback must not re-enter the SAME enclave** — no `WithBytes`, `Size`,
  `IsAlive`, `EqualTo` or `Destroy` on the enclave it is borrowing. The read lock
  is held for the call, so a nested `Destroy` deadlocks and a nested read
  deadlocks whenever a `Destroy` is already queued for the write lock.
- **A callback MAY touch a DIFFERENT enclave**, and this REVERSES what this
  document said before the go-enclave adoption. Constructing, borrowing and
  destroying other enclaves inside a live borrow is ordinary now. It used to be a
  self-deadlock: a process-wide `purgeMu` was held shared across every borrow, and
  Go's `RWMutex` refuses a new `RLock` once a writer is queued, so any
  purge-capable operation anywhere in the process could park between the outer
  borrow and the inner one (#18, #39). Call sites shaped around that restriction
  are being unwound as they are touched; `Barrier.Initialize` already holds the
  root key in a locked enclave across its whole body (`e694607`).
- **There is no process-wide purge.** go-enclave maps its own memory, and an
  allocation that fails fails alone. The old invariant — that a failed memguard
  allocation in one goroutine could unmap pages another was reading, so per-buffer
  locking could not be sufficient — applied to memguard-backed buffers and no
  longer applies to any enclave. What remains of that hazard is confined to
  `ProtectedKey`'s three memguard call sites and is covered by `purgeMu`; see
  [`architecture.md`](architecture.md).
- **`WipeAll` is the one other thing that can destroy an enclave.** go-enclave's
  shutdown sweep destroys every live enclave in the process. This repository never
  calls it, and an embedder that does must have stopped the goroutines holding
  borrows first: the sweep skips a held enclave and reports it in its count rather
  than waiting, and a constructor racing a sweep can return an already-destroyed
  enclave.
- **Every `Open` is paired with a `Destroy`.** `ProtectedKey.Open` materializes
  plaintext into a fresh `MemoryEnclave`; the caller owns it. A leaked enclave
  keeps plaintext resident for the life of the process and holds a whole page of
  `RLIMIT_MEMLOCK`, which then makes later locks fail with a typed
  `ErrLockedMemoryUnavailable` — it no longer aborts the process.
- **Constructors wipe their input.** `NewProtectedKey(key)` wipes `key`, and
  `goenclave.NewMemoryEnclave(data)` wipes `data` on every return path including
  its failures. Callers must not rely on the argument being readable afterwards.
- **`SealMemoryEnclave` consumes its argument.** Ownership transfers to the
  returned key and the source is destroyed: afterwards `Size()` is `0`,
  `IsAlive()` is false, and `WithBytes` returns `ErrEnclaveDestroyed`. A
  concurrent second call gets `nil`, and exactly one caller can ever win.
- **`Clear` revokes, it does not erase.** After `Clear`, `Open` returns
  `ErrEnclaveNil`. The enclave's ciphertext persists on the ordinary heap until
  the GC reclaims it. Treat a cleared key as GC-bounded, not wiped.
- **`Destroy` panics on a canary failure**, and this is the one panic in the
  path. go-enclave verifies the canary ahead of the data when it releases the
  pages; a mismatch means something wrote into memory it did not own, which is
  not a condition the caller can act on, because the state it would act on is the
  state that is wrong. The material is wiped and the pages are released before the
  panic, so nothing leaks either way.
- **A `nil` `*MemoryEnclave` is a valid receiver.** It reports itself not alive,
  reads with `ErrEnclaveDestroyed`, and destroys as a no-op, so an optional
  enclave needs no guard at every use.
- **No signal handler is installed.** This package never calls
  `memguard.CatchSignal` or `memguard.CatchInterrupt`. Those call
  `signal.Reset()` with no arguments, which detaches **every** signal handler in
  the process, including the embedding application's. A library does not get to
  make that decision for its embedder. An embedder that wants signal-triggered
  wiping installs it itself. This is pinned behaviourally by
  `TestNoSignalHandlerInstalled`, not by grep.

## 3. Error semantics

| Error | Type | Returned when | Fail-closed? |
|---|---|---|---|
| `ErrEnclaveNil` | `*NilError` | `Open` on a `ProtectedKey` whose enclave was never initialized or has been `Clear`ed | Yes — no key material is produced |
| `ErrEnclaveDestroyed` | sentinel (`errors.New`), go-enclave's | any read on a `MemoryEnclave` that has been destroyed or consumed by `SealMemoryEnclave` | Yes — the callback is not called |
| `OpenError{Err}` | `*OpenError` | memguard fails to open the enclave | Yes — returns `(nil, err)` |
| `ErrLockedMemoryUnavailable` | `*goenclave.LockedMemoryError` | the kernel refuses to map or lock the pages, usually at `RLIMIT_MEMLOCK` | Yes — that allocation alone fails; nothing else is touched |
| `ErrEmptyEnclave` / `ErrInvalidSize` | sentinels, go-enclave's | an enclave over no material, or a size no enclave can have | Yes — refused before anything is mapped |

- `ErrEnclaveNil` is a package-level sentinel of concrete type `*NilError`;
  compare with `errors.Is(err, enclave.ErrEnclaveNil)`. `pkg/seal` does exactly
  this in `strategy_software.go` to distinguish "the passphrase was cleared"
  from a real failure.
- `ErrEnclaveDestroyed` has no type of its own any more. `DestroyedError` is
  gone: the sentinel is go-enclave's, re-exported, so `errors.Is(err,
  enclave.ErrEnclaveDestroyed)` matches whether the error arrived through this
  package or straight from go-enclave. The two remaining sentinels stay distinct
  on purpose — `NilError` says a `ProtectedKey` was never given an enclave or has
  been `Clear`ed; `ErrEnclaveDestroyed` says locked memory existed and has been
  wiped and unmapped.
- `OpenError.Unwrap()` returns the underlying memguard error.
- Messages are lowercase and origin-prefixed: `ErrEnclaveNil.Error()` is
  `"memprotect: enclave is nil"`.
- Locked-memory constructors DO return errors, and this reverses what this
  section said before the go-enclave adoption. `goenclave.NewMemoryEnclave` and
  `NewMemoryEnclaveSized` return `(*MemoryEnclave, error)`: locking pages is a
  syscall the kernel can decline, and a refusal is now a reportable condition
  rather than a process abort. `NewProtectedKey` still returns a bare pointer.
  `SealMemoryEnclave` returns `nil` (not an error) for a `nil`, consumed, or
  destroyed enclave.

**There is no error path that yields a partially-protected key.** Either the
material is inside a protected container or the call failed.

## 4. Configuration

**None.** This package reads no configuration keys, no environment variables,
and no files. Its behavior is fixed at compile time.

The one environment interaction is operational rather than configured: locked
memory consumes `RLIMIT_MEMLOCK`, and the kernel charges a whole PAGE per
allocation — a 32-byte key costs 4096 bytes on a 4 KiB-page host. A process (or
container) with a low limit sees allocations fail with a typed
`ErrLockedMemoryUnavailable` once the limit is reached; since the go-enclave
adoption this fails exactly the allocation that reached the ceiling and touches
no other enclave, where memguard previously purged every live buffer and aborted.
Raise the limit rather than reducing the number of protected keys, and size it in
PAGES rather than in key sizes — `goenclave.LockedSize` reports what one enclave
actually costs. Guard and canary pages are mapped `PROT_NONE` and never locked,
so they consume address space rather than the limit.

go-enclave's `Allocator` bounds what one code path may hold against that ceiling,
with a budget fixed at construction and returned enclave by enclave as they are
destroyed. **This repository constructs none**: nothing here allocates locked
memory for an unauthenticated peer, so there is no path to bound. A consumer that
buffers key material before authenticating one should admit those allocations
through an `Allocator` at its own layer.

## 5. Persistence

**None.** Nothing in this package writes to disk, and nothing it holds survives
the process. Key material that must outlive the process is sealed by a
`SealingStrategy` — see [`../seal-barrier/engineering.md`](../seal-barrier/engineering.md).

## 6. Test strategy

All unit, in `pkg/enclave/memprotect_test.go`. Target: `make test-enclave`.

What the suite pins, beyond happy-path lifecycle:

| Test | Pins |
|---|---|
| `TestMemoryEnclaveIsGoEnclaves`, `TestErrEnclaveDestroyedIsGoEnclaves` | that the type and the sentinel ARE go-enclave's — the property that makes every other guarantee upstream's to keep |
| `TestNewProtectedKey_WipesSource` | the constructor's wipe of its argument |
| `TestProtectedKey_OpenSeal`, `_MultipleOpens` | the round trip, and that each `Open` yields an independent enclave holding the same key |
| `TestProtectedKey_OpenNilEnclave`, `_ClearDropsEnclaveReference`, `_ClearAlreadyClearedNoPanic` | `ErrEnclaveNil`, `Clear` → `Open`, and `Clear` idempotence |
| `TestProtectedKey_ConcurrentOpenClear` | the `atomic.Pointer` invariant |
| `TestProtectedKey_BridgeUnderConcurrentMemguardWork` | the memguard-to-go-enclave bridge under concurrent memguard traffic, under `-race` |
| `TestSealMemoryEnclave`, `_NilAndDestroyed`, `_ConsumedEnclaveReturnsNil`, `TestSealMemoryEnclaveAndDeprecatedAliasAgree` | consumption semantics, the `nil` returns, and that the deprecated spelling is the same function |
| `TestSealMemoryEnclave_ConcurrentConsumeExactlyOnce` | that exactly one of four concurrent callers consumes the enclave — a regression test for a real defect, not a hypothetical |
| `TestNoSignalHandlerInstalled` | that no code path resets process signal handlers |
| `TestWipeBytes` | `WipeBytes`, including the empty slice |
| `TestErrEnclaveNil_Message`, `TestNilError_TypedError`, `TestOpenError_FormatAndUnwrap` | the error vocabulary |

**What was deleted with the implementation, and why it is not a coverage
regression.** `TestProtectedBuffer_Lifecycle`, `_NilAfterDestroy`, `_FromBytes`
and `_ConcurrentDestroy`, `TestMemoryEnclave_WriteInPlaceThenFreeze`,
`_FreezeIsSafeOnDestroyed` and `_FromBytesWipesSourceAndFreezes`, plus the whole
of `memprotect_race_test.go` and `memprotect_purge_test.go`, went with the code
they covered. Every one exercised this repository's own `mlock`, guard-page,
canary, borrow and purge machinery, and that machinery no longer exists here.
Re-asserting those properties would be testing a dependency through its consumer;
they are pinned upstream instead — `enclave_lockfail_linux_test.go`,
`registry_linux_test.go` and `admission_accounting_linux_test.go` in go-enclave.
The issue-#15 accessor-vs-`Destroy` probes and their mutation verification belong
to that set: the defect they pinned was in an implementation this repository no
longer has.

**Fault-injection seam: none in this package.** memguard is called directly and
go-enclave's constructors are called by name, so there is nothing to substitute.
The failure that matters — `RLIMIT_MEMLOCK` exhaustion — is driven upstream under
a lowered limit in a subprocess, which is where it can be done without taking the
test binary down with it. Consumers that need to see their own refusal path
inject at their allocation site: `pkg/seal`'s `Barrier.lockRootKey` is a field
defaulting to `goenclave.NewMemoryEnclaveSized` for exactly that reason, and
`barrier_borrow_test.go` sets it to a function that refuses.

`TestProtectedKey_BridgeUnderConcurrentMemguardWork` is the one to run under
`-race`, and its own comment states the limit it cannot cross: it proves the
bridge is race-clean against every other memguard caller in this package, not
that a process-wide memguard purge cannot unmap the buffer mid-copy. Reaching a
purge means driving the process to exhaustion, and memguard answers that by
panicking after purging — so the purge invariant is carried by the argument at
`purgeMu` rather than by a test. That gap closes with memguard itself, when
`ProtectedKey` moves to go-enclave's sealing seam (go-enclave #22).

## 7. Observability

**No logging, no metrics, no audit records.** Deliberately: every event this
package could report is about a key, and the useful fields (identity, size,
lifetime) are exactly the fields that should not reach a log. Consumers that
need visibility instrument at their own layer — `pkg/seal` logs barrier
lifecycle events without naming key material. See
[`../observability/engineering.md`](../observability/engineering.md).
