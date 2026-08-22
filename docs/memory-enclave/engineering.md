# Memory Enclave — Engineering Contract

> **Status: NORMATIVE** · Scope: go-quicraft `pkg/enclave` · Last verified: 2026-08-19
> Conflicts with this document are defects. Fix them here first.

Import path: `github.com/jeremyhahn/go-quicraft/pkg/enclave`
Backed by `github.com/awnumar/memguard`.

## 1. Exported surface

```go
// Types
type MemoryEnclave struct{ /* unexported */ }
type ProtectedBuffer = MemoryEnclave // Deprecated: alias, not a distinct type
type ProtectedKey    struct{ /* unexported */ }

// Constructors
func NewMemoryEnclave(size int) *MemoryEnclave
func NewMemoryEnclaveFromBytes(data []byte) *MemoryEnclave
func NewProtectedKey(key []byte) *ProtectedKey
func SealMemoryEnclave(buf *MemoryEnclave) *ProtectedKey
// Deprecated aliases: NewProtectedBuffer, NewProtectedBufferFromBytes,
// SealProtectedBuffer — identical behaviour and return types.

// MemoryEnclave methods
func (p *MemoryEnclave) Bytes() []byte
func (p *MemoryEnclave) WithBytes(fn func([]byte) error) error
func (p *MemoryEnclave) Size() int
func (p *MemoryEnclave) Freeze()
func (p *MemoryEnclave) Destroy()

// ProtectedKey methods
func (p *ProtectedKey) Open() (*MemoryEnclave, error)
func (p *ProtectedKey) Clear()

// Free function
func WipeBytes(b []byte)

// Errors
var ErrEnclaveNil       = &NilError{}
var ErrEnclaveDestroyed = &DestroyedError{}
type NilError       struct{}
type DestroyedError struct{}
type OpenError      struct{ Err error }
```

`pkg/seal/memprotect.go` re-exports `ProtectedBuffer`, `ProtectedKey`,
`NewProtectedBuffer`, `NewProtectedBufferFromBytes`, `NewProtectedKey`,
`SealProtectedBuffer` and `WipeBytes` as aliases. They are the same types; there
is no second implementation.

## 2. Invariants

**Call safety vs. slice safety.** `ProtectedKey` holds its enclave in an
`atomic.Pointer`; `MemoryEnclave` guards its locked buffer with a borrow counter
(see the architecture note — the atomic was a defect here, issue #15). Readers
register a borrow; mutators wait for the count to reach zero. Every *call* is
safe under concurrency. A *slice* a caller retained after the call returned is
not.

| Operation | Concurrency | Idempotent |
|---|---|---|
| `ProtectedKey.Open` | safe with `Open`, `Clear` | n/a — each call returns a new buffer |
| `ProtectedKey.Clear` | safe with `Open`, `Clear` | yes; only the winner of the pointer swap acts |
| `MemoryEnclave.Destroy` / `Freeze` / `SealMemoryEnclave` | mutators — safe with everything, and BLOCK until every borrow ends | `Destroy` yes; only the first caller reaches memguard |
| `MemoryEnclave.Bytes` / `Size` | readers — safe with the mutators | n/a |
| `MemoryEnclave.WithBytes` | reader — `Destroy` and `Freeze` cannot complete while the callback runs | n/a |

- **The lock bounds the CALL, not the slice.** `Destroy` zeroes and unmaps the
  memory, so a slice retained past the accessor that returned it is a
  use-after-free. Where teardown can race a reader — barrier and custody
  ceremony paths — use `WithBytes` / `ProtectedShare.WithValue`, which hold the
  read lock across the callback; `Destroy` then blocks until it returns.
  `WithBytes` returns `ErrEnclaveDestroyed` (`*DestroyedError`) if the enclave
  is already gone, and the callback's own error unchanged otherwise.
- **Mutators take priority over readers.** A queued `Destroy`, `Freeze` or
  `SealMemoryEnclave` blocks every new borrow until it completes, so readers
  cannot starve the wipe. This is the same policy `sync.RWMutex` applies.
- **A callback must NOT touch ANY enclave** — not its own and not a different
  one. No `Bytes`, `Size`, `WithBytes`, `Destroy`, `Freeze` or
  `SealMemoryEnclave` from inside a borrow.

  For its own enclave the reason is the per-enclave rule: a nested borrow queues
  behind any mutator that arrived meanwhile, and that mutator is waiting on the
  borrow the callback holds.

  For a DIFFERENT enclave the reason is the process-wide one, and this is a
  TIGHTENING of what this document previously said (issue #18). Borrowing now
  takes the package-level `purgeMu` shared, and Go's `RWMutex` refuses a new
  `RLock` once a writer is queued — so a nested borrow of a second enclave
  deadlocks against any purge-capable operation (an allocation, `Destroy`,
  `Freeze`) that queued after the outer borrow began, anywhere in the process.
  Materialize what the callback needs BEFORE borrowing; `CombineProtected` does
  exactly that with its decode buffer.
- **A process-wide purge is coordinated against every borrow.** memguard's
  failure path is not per-buffer: `core.Panic` calls `core.Purge` first, which
  unmaps *every* live buffer in the process while holding none of this package's
  locks. The per-enclave borrow counter therefore cannot make a borrow safe on
  its own — a failed allocation in one goroutine could unmap pages another was
  reading, with no `Destroy` involved. `purgeMu` is the outer layer that closes
  it: borrows take it shared, purge-capable operations take it exclusively, and
  the order is always `purgeMu` then the enclave's own lock.
- **Package-internal readers go through the accessor too.**
  `CombineProtected` and `ProtectedShare.MarshalJSON` read share values inside
  `WithValue`, not from a released alias. Both obtain the slices themselves, so
  no caller-side discipline could have made them safe.
- **Every `Open` is paired with a `Destroy`.** `Open` materializes plaintext
  into a fresh `ProtectedBuffer`; the caller owns it. A leaked buffer keeps
  plaintext resident for the life of the process and consumes `RLIMIT_MEMLOCK`,
  which memguard answers by aborting.
- **Constructors wipe their input.** `NewProtectedKey(key)` and
  `NewProtectedBufferFromBytes(data)` wipe the source slice after copying.
  Callers must not rely on the argument being readable afterwards.
- **`SealProtectedBuffer` consumes its argument.** Ownership transfers to the
  returned key; the argument is left empty (`Bytes()` returns `nil`, `Size()`
  returns `0`). A concurrent second call gets `nil`.
- **`Clear` revokes, it does not erase.** After `Clear`, `Open` returns
  `ErrEnclaveNil`. The enclave's ciphertext persists on the ordinary heap until
  the GC reclaims it. Treat a cleared key as GC-bounded, not wiped.
- **After `Destroy`**, `Bytes()` returns `nil` and `Size()` returns `0` rather
  than panicking.
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
| `ErrEnclaveDestroyed` | `*DestroyedError` | `WithBytes` on a `MemoryEnclave` that has been destroyed or consumed by `SealMemoryEnclave` | Yes — the callback is not called |
| `OpenError{Err}` | `*OpenError` | memguard fails to open the enclave | Yes — returns `(nil, err)` |

- `ErrEnclaveNil` is a package-level sentinel of concrete type `*NilError`;
  compare with `errors.Is(err, enclave.ErrEnclaveNil)`. `pkg/seal` does exactly
  this in `strategy_software.go` to distinguish "the passphrase was cleared"
  from a real failure.
- `OpenError.Unwrap()` returns the underlying memguard error.
- Messages are lowercase and origin-prefixed: `ErrEnclaveNil.Error()` is
  `"memprotect: enclave is nil"`.
- Constructors do **not** return errors. `NewProtectedBuffer` and
  `NewProtectedKey` return a usable value or memguard aborts the process — an
  allocation failure of locked memory is not a condition this library can carry
  on from. `SealProtectedBuffer` returns `nil` (not an error) for a `nil`,
  consumed, or destroyed buffer.

**There is no error path that yields a partially-protected key.** Either the
material is inside a protected container or the call failed.

## 4. Configuration

**None.** This package reads no configuration keys, no environment variables,
and no files. Its behavior is fixed at compile time.

The one environment interaction is operational rather than configured: locked
buffers consume `RLIMIT_MEMLOCK`. A process (or container) with a low limit will
see memguard abort once the limit is reached. Raise the limit rather than
reducing the number of protected keys.

## 5. Persistence

**None.** Nothing in this package writes to disk, and nothing it holds survives
the process. Key material that must outlive the process is sealed by a
`SealingStrategy` — see [`../seal-barrier/engineering.md`](../seal-barrier/engineering.md).

## 6. Test strategy

All unit, in `pkg/enclave/memprotect_test.go`. Target: `make test-enclave`.

What the suite pins, beyond happy-path lifecycle:

| Test | Pins |
|---|---|
| `TestNewProtectedKey_WipesSource` | the constructor's wipe of its argument |
| `TestProtectedKey_ClearDropsEnclaveReference` | `Clear` → `Open` returns `ErrEnclaveNil` |
| `TestProtectedKey_ClearAlreadyClearedNoPanic` | `Clear` idempotence |
| `TestProtectedKey_ConcurrentOpenClear` | the `atomic.Pointer` invariant |
| `TestProtectedBuffer_ConcurrentDestroy` | single-shot `Destroy` under race |
| `TestMemoryEnclave_AccessorsAreSafeAgainstConcurrentDestroy` | issue #15: `Bytes`/`Size` vs `Destroy` — 200 unsynchronised pairs |
| `TestMemoryEnclave_WithBytesIsSafeAgainstConcurrentDestroy` | the same for the scoped accessor |
| `TestMemoryEnclave_WithBytesHoldsTheEnclaveOpen` | that `Destroy` cannot COMPLETE during the callback — the property an atomic could not give |
| `TestMemoryEnclave_WithBytesOnDestroyed` / `_OnSealed` / `_PropagatesCallbackError` | the refusal reason, and that the callback's error is not wrapped |
| `TestProtectedShare_ValueIsSafeAgainstConcurrentDestroy` and the `WithValue` set (`pkg/crypto/shamir`) | the same properties on the share surface that exposed the defect |
| `TestProtectedBuffer_NilAfterDestroy` | post-`Destroy` accessors return nil/0 |
| `TestSealProtectedBuffer_ConsumedBufferReturnsNil` | consumption semantics |
| `TestSealProtectedBuffer_DestroyedBuffer` / `_NilBuffer` | `nil` return, not panic |
| `TestNoSignalHandlerInstalled` | that no code path resets process signal handlers |
| `TestWipeBytes`, `TestWipeBytes_EmptySlice` | `WipeBytes` including the empty case |
| `TestOpenError_Format`, `TestOpenError_Unwrap`, `TestNilError_TypedError`, `TestErrEnclaveNil_Message` | the error vocabulary |

**Fault-injection seam:** none. There is no injectable allocator or FS here —
memguard is called directly. Failure modes that matter (`RLIMIT_MEMLOCK`
exhaustion) abort the process by design and are not simulated in unit tests.

The concurrency tests are the ones to run under `-race`; they are the reason the
locking design is asserted rather than assumed. The issue-#15 probes were
mutation-verified: with the borrow discipline stripped from `memprotect.go`,
five enclave tests and two share tests fail under `-race`, and four of them fail
on their OWN reason assertion — "Destroy completed while the callback held the
enclave", "Freeze completed while a callback still held the enclave's bytes" —
without `-race` at all. The `MarshalJSON` probe is the exception and says so in
its own comment: it is probabilistic (measured 10/10 under `-race`, 0/10
without), because `MarshalJSON` runs no caller-supplied code inside its borrow
and so offers no hook a deterministic probe could use.

## 7. Observability

**No logging, no metrics, no audit records.** Deliberately: every event this
package could report is about a key, and the useful fields (identity, size,
lifetime) are exactly the fields that should not reach a log. Consumers that
need visibility instrument at their own layer — `pkg/seal` logs barrier
lifecycle events without naming key material. See
[`../observability/engineering.md`](../observability/engineering.md).
