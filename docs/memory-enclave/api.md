# Memory Enclave — API

> **Status: NORMATIVE** · Scope: go-quicraft `pkg/enclave` · Last verified: 2026-09-02
> Conflicts with this document are defects. Fix them here first.

## Transport coverage

go-quicraft is a library. **The Go API is the API** — there is no server, no
route table, no RPC service, and no MCP tool in this repository. Of the six
transports the portfolio's API conventions enumerate, exactly one applies here:

| Transport | This feature |
|---|---|
| **Embedded (Go call)** | The whole surface below. This is the transport. |
| Unix socket | n/a — this repository serves no socket |
| REST | n/a — this repository exposes no HTTP handler |
| gRPC | n/a — this repository defines no gRPC service |
| QUIC | n/a for this feature. The repository's QUIC listener carries Raft messages only (see [`../quic-transport/api.md`](../quic-transport/api.md)); key material never crosses it. |
| MCP | n/a — this repository registers no MCP tools |

These are **not** missing-capability rows. This feature has no row in the
portfolio capability register carrying a transport tier; it is consumed by
importing the package. Exposing key material over a transport is the thing this
feature exists to prevent, so a row here would be a defect, not a gap.

## Go API

Import: `github.com/jeremyhahn/go-quicraft/pkg/enclave`, and — for locked memory
itself — `github.com/jeremyhahn/go-enclave`.

### Where the surface lives now

**Locked memory is go-enclave's.** `pkg/enclave` declares no locked-memory type
of its own: `MemoryEnclave` (and its deprecated spelling `ProtectedBuffer`) are
type ALIASES for `goenclave.MemoryEnclave`, so the three names are one type and
assignment between them compiles. This is `docs/00 §R` — *"No project hand-rolls
its own locked-memory handling"* — and it is why the constructors below are
go-enclave's rather than this package's.

What `pkg/enclave` still owns is `ProtectedKey`: material held as CIPHERTEXT at
rest, readable only inside an `Open` window. go-enclave has no released
counterpart for that posture (tracked at go-enclave #22); when it does,
`pkg/enclave` goes away entirely.

### Capability: allocate locked memory

```go
func goenclave.NewMemoryEnclaveSized(n int) (*MemoryEnclave, error)  // writable, fill in place
func goenclave.NewMemoryEnclave(data []byte) (*MemoryEnclave, error) // moves data in, FROZEN
```

| | |
|---|---|
| Request | `n int`, or `data []byte` (**wiped** on every return path, including failure) |
| Response | `*MemoryEnclave` — `mlock`'d, guard-paged, canary-checked, excluded from core dumps |
| Errors | `ErrInvalidSize`, `ErrEmptyEnclave`, and `ErrLockedMemoryUnavailable` when the kernel refuses the lock |
| Permission | none — in-process call, no authorization boundary |

**These return an error, and that is the point.** The bare-pointer constructors
`enclave.NewMemoryEnclave(size)` and `enclave.NewMemoryEnclaveFromBytes(data)`
are GONE, not renamed: locking pages is a syscall the kernel can decline, and a
constructor that cannot say so leaves the caller asserting against a nil.

Mutability differs between them and it matters. A **sized** enclave stays
WRITABLE for its whole life — that is what makes filling it possible — so a
slice retained past a borrow can silently corrupt the material. One built by
`NewMemoryEnclave` is frozen read-only, where the same mistake faults. There is
no post-fill seal yet (go-enclave #6), so the fill-then-freeze shape is a move:

```go
scratch, err := goenclave.NewMemoryEnclaveSized(n)
defer scratch.Destroy()
err = scratch.WithBytes(func(dst []byte) error { /* produce material here */ })
final, err := goenclave.NewMemoryEnclave(dst) // locked-to-locked, freezes, wipes dst
```

Both endpoints are `mlock`'d, so the material never reaches the ordinary heap.

### Capability: hold a key encrypted at rest in process memory

```go
func NewProtectedKey(key []byte) *ProtectedKey
```

| | |
|---|---|
| Request | `key []byte` — the raw key material. **Wiped by the constructor.** |
| Response | `*ProtectedKey`. Never `nil`. |
| Errors | none returned |
| Permission | none |

### Capability: materialize the plaintext for a bounded window

```go
func (p *ProtectedKey) Open() (*MemoryEnclave, error)
```

| | |
|---|---|
| Request | none |
| Response | `*MemoryEnclave` holding the plaintext, `mlock`'d, guard-paged and **frozen read-only**. The caller **owns** it and must `Destroy` it. |
| Errors | `ErrEnclaveNil` (`*NilError`) if the key was never initialized or has been `Clear`ed; `*OpenError` wrapping the underlying failure otherwise. Both fail closed — no material is returned. |
| Permission | none |

The plaintext never reaches the ordinary Go heap: memguard decrypts into a locked
buffer of its own, and `Open` copies from those locked pages straight into
go-enclave's, wipes memguard's copy and hands its pages back.

### Capability: read protected bytes with a bounded lifetime

```go
func (e *MemoryEnclave) WithBytes(fn func([]byte) error) error
func (e *MemoryEnclave) Open() ([]byte, error)
func (e *MemoryEnclave) Size() int
func (e *MemoryEnclave) IsAlive() bool
func (e *MemoryEnclave) EqualTo(other []byte) bool
func (e *MemoryEnclave) Destroy()
```

| | |
|---|---|
| Request | `WithBytes`: `fn`, called with a slice aliasing the protected region. `Open`: none. |
| Response | `WithBytes`: `fn`'s error, unchanged. `Open`: the aliasing slice. |
| Errors | Both return `ErrEnclaveDestroyed` — go-enclave's sentinel, re-exported as `enclave.ErrEnclaveDestroyed` — if the enclave is destroyed. `WithBytes` does not call `fn`. |
| Permission | none |

`MemoryEnclave.Open` REPLACES the old `Bytes()` accessor, and the rename carries
a contract change: it reports a destroyed enclave as an **error** where `Bytes`
reported it as a `nil` slice.

`Open` is otherwise what `Bytes` was and carries the same warning: the slice
aliases the locked pages and no lock is held once it returns, so a concurrent
`Destroy` unmaps memory the caller is still reading and the read FAULTS. Use it
only where the structure shows no concurrent teardown is possible — single
goroutine ownership, the `defer e.Destroy()` shape used throughout the seal
barrier. Everywhere teardown can race a reader, use `WithBytes`, which holds the
read lock across `fn`.

`fn` must not escape the slice, and must not re-enter THIS enclave. **`fn` MAY
touch a different enclave** — a relaxation of what this page previously said.
Under this repository's former borrow machinery a process-wide `purgeMu` was held
shared across every borrow, so a nested borrow of any second enclave deadlocked
against a purge-capable operation queued anywhere in the process. go-enclave
holds only the borrowed enclave's own read lock, so that hazard is gone with the
lock that created it.

`shamir.ProtectedShare` mirrors the pair as `Value` / `WithValue`.

### Capability: promote an enclave to an encrypted-at-rest key

```go
func SealMemoryEnclave(buf *MemoryEnclave) *ProtectedKey
func SealProtectedBuffer(buf *MemoryEnclave) *ProtectedKey // deprecated spelling
```

| | |
|---|---|
| Request | `buf` — **consumed**; ownership transfers to the returned key |
| Response | `*ProtectedKey`, or `nil` if `buf` was `nil`, already consumed, or destroyed |
| Errors | none returned; `nil` is the failure signal |
| Permission | none |

### Capability: revoke access to a key

```go
func (p *ProtectedKey) Clear()
```

| | |
|---|---|
| Request | none |
| Response | none |
| Errors | none. Idempotent and safe concurrently with `Open`. |
| Permission | none |

After `Clear`, `Open` returns `ErrEnclaveNil`. **This does not erase the
ciphertext** — see [`engineering.md`](engineering.md) §2.

### Capability: wipe an ordinary slice

```go
func WipeBytes(b []byte)
```

Zeroes `b` in place. Handles the empty slice. It is go-enclave's `Wipe`,
re-exported so callers have one wiping primitive rather than two.

### Error vocabulary

```go
var ErrEnclaveNil       = &NilError{}                  // "memprotect: enclave is nil"
var ErrEnclaveDestroyed = goenclave.ErrEnclaveDestroyed // go-enclave's sentinel, re-exported

type NilError struct{}
func (e *NilError) Error() string

type OpenError struct{ Err error }
func (e *OpenError) Error() string
func (e *OpenError) Unwrap() error
```

Match with `errors.Is(err, enclave.ErrEnclaveNil)`, `errors.Is(err,
enclave.ErrEnclaveDestroyed)` and `errors.As(err, &openErr)`.

`DestroyedError` is GONE as a type. `ErrEnclaveDestroyed` is now go-enclave's
sentinel re-exported rather than a local one redefined, so `errors.Is` matches
whether the error arrived through this package or straight from go-enclave —
two sentinels meaning the same thing is how a caller ends up handling one and
not the other.

The two remaining sentinels are still distinct on purpose: `NilError` says a
`ProtectedKey` was never given an enclave or has been `Clear`ed;
`ErrEnclaveDestroyed` says locked memory existed and has been wiped and unmapped.

## Re-exports

`pkg/seal` re-exports what remains as type aliases and function variables:

```go
type ProtectedBuffer = enclave.MemoryEnclave
type ProtectedKey    = enclave.ProtectedKey

var NewProtectedKey     = enclave.NewProtectedKey
var SealProtectedBuffer = enclave.SealMemoryEnclave
```

`seal.NewProtectedBuffer` and `seal.NewProtectedBufferFromBytes` are GONE for the
reason given above — call go-enclave's constructors and handle the error. A local
wrapper preserving the old bare-pointer shape would be the removed-API shim
`conventions/dependency-resolution.md` §0 forbids.

These are aliases, not wrappers. There is no second implementation and no
behavioral difference.

## Versioning and deprecation

Nothing in this package is deprecated. The surface is pre-1.0 and may change;
see the repository's compatibility policy in
[`../wire-protocol/architecture.md`](../wire-protocol/architecture.md) for what
"pre-1.0" means here. Because the types are re-exported by `pkg/seal`, a
signature change breaks both import paths at once, in the same module version.
