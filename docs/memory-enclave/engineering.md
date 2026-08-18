# Memory Enclave — Engineering Contract

> **Status: NORMATIVE** · Scope: go-quicraft `pkg/enclave` · Last verified: 2026-08-19
> Conflicts with this document are defects. Fix them here first.

Import path: `github.com/jeremyhahn/go-quicraft/pkg/enclave`
Backed by `github.com/awnumar/memguard`.

## 1. Exported surface

```go
// Types
type ProtectedBuffer struct{ /* unexported */ }
type ProtectedKey    struct{ /* unexported */ }

// Constructors
func NewProtectedBuffer(size int) *ProtectedBuffer
func NewProtectedBufferFromBytes(data []byte) *ProtectedBuffer
func NewProtectedKey(key []byte) *ProtectedKey
func SealProtectedBuffer(buf *ProtectedBuffer) *ProtectedKey

// ProtectedBuffer methods
func (p *ProtectedBuffer) Bytes() []byte
func (p *ProtectedBuffer) Size() int
func (p *ProtectedBuffer) Destroy()

// ProtectedKey methods
func (p *ProtectedKey) Open() (*ProtectedBuffer, error)
func (p *ProtectedKey) Clear()

// Free function
func WipeBytes(b []byte)

// Errors
var ErrEnclaveNil = &NilError{}
type NilError struct{}
type OpenError struct{ Err error }
```

`pkg/seal/memprotect.go` re-exports `ProtectedBuffer`, `ProtectedKey`,
`NewProtectedBuffer`, `NewProtectedBufferFromBytes`, `NewProtectedKey`,
`SealProtectedBuffer` and `WipeBytes` as aliases. They are the same types; there
is no second implementation.

## 2. Invariants

**Reference safety vs. content safety.** Both types hold their memguard object
in an `atomic.Pointer`, so the *reference* is safe under concurrency. The
*contents* are not.

| Operation | Concurrency | Idempotent |
|---|---|---|
| `ProtectedKey.Open` | safe with `Open`, `Clear` | n/a — each call returns a new buffer |
| `ProtectedKey.Clear` | safe with `Open`, `Clear` | yes; only the winner of the pointer swap acts |
| `ProtectedBuffer.Destroy` | safe with `Bytes`, `Size`, `Destroy` | yes; only the winner of the swap destroys |
| `ProtectedBuffer.Bytes` / `Size` | **NOT** safe with `Destroy` | n/a |

- **`ProtectedBuffer` contents are single-owner.** memguard's `LockedBuffer` is
  not safe for concurrent `Destroy`/read. The slice returned by `Bytes` must not
  outlive the buffer: `Destroy` zeroes and unmaps that memory, so a retained
  slice is a use-after-free.
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
`atomic.Pointer` design is asserted rather than assumed.

## 7. Observability

**No logging, no metrics, no audit records.** Deliberately: every event this
package could report is about a key, and the useful fields (identity, size,
lifetime) are exactly the fields that should not reach a log. Consumers that
need visibility instrument at their own layer — `pkg/seal` logs barrier
lifecycle events without naming key material. See
[`../observability/engineering.md`](../observability/engineering.md).
