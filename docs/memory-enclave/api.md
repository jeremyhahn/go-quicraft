# Memory enclave: API

> **Status: NORMATIVE** · Scope: go-quicraft's use of go-enclave protected memory · Last verified: 2026-10-13
> Conflicts with this document are defects. Fix them here first.

## Transport coverage

go-quicraft is a library, and the Go API is the only API. There is no server,
route table, RPC service or MCP tool in this repository.

| Transport | This feature |
|---|---|
| Embedded (Go call) | The whole surface below. |
| Unix socket | n/a: this repository serves no socket |
| REST | n/a: no HTTP handler |
| gRPC | n/a: no gRPC service |
| QUIC | n/a. The QUIC listener carries Raft messages only (see [`../quic-transport/api.md`](../quic-transport/api.md)); key material never crosses it. |
| MCP | n/a: no MCP tools |

These rows are not gaps. Exposing key material over a transport is what this
feature exists to prevent.

## Go API

Import: `github.com/jeremyhahn/go-enclave` (go-quicraft requires v0.9.0-alpha).

All of the surface belongs to go-enclave. go-quicraft declares no
protected-memory type, alias or wrapper, and re-exports nothing: consumers
import go-enclave and call it directly. The only go-quicraft identifiers on this
page are the ones in `pkg/seal` and `pkg/crypto/shamir` that take or return
go-enclave types.

### Allocate locked memory

```go
func NewMemorySized(n int) (ByteAccess, error)  // writable, fill in place
func NewMemory(data []byte) (ByteAccess, error) // moves data in, FROZEN
func NewMemoryCopy(src []byte) (ByteAccess, error) // copies, FROZEN, src untouched
```

| | |
|---|---|
| Request | `n int`, or `data []byte` (wiped on every return path, failure included) |
| Response | a `ByteAccess` handle to a memory enclave: `mlock`'d, guard-paged, canary-checked, excluded from core dumps. The enclave's type is internal to go-enclave; a failed call returns a nil interface. |
| Errors | `ErrInvalidSize`, `ErrEmptyEnclave`, and `ErrLockedMemoryUnavailable` when the kernel refuses the lock |
| Permission | none; in-process call |

They return an error because locking pages is a syscall the kernel can decline.

Mutability differs. A sized enclave stays writable until it is frozen, which is
what makes filling it possible, so a slice retained past a borrow can silently
corrupt the material. One built by `NewMemory` or `NewMemoryCopy`
is frozen read-only, where the same mistake faults.

### Freeze material generated in place

```go
type Freezer interface {
	Handle
	Freeze() error
}
```

`Freeze` is the `Freezer` capability, reached from a `ByteAccess` with
`enclave.As`. A handle without it is refused with a `*CapabilityError`
(`CapabilityFreeze`), never handed back writable.

| | |
|---|---|
| Request | none |
| Response | none |
| Errors | `ErrEnclaveDestroyed` on a nil or destroyed enclave; `ErrLockedMemoryUnavailable` when the kernel refuses the `mprotect` |
| Permission | none |

Idempotent and irreversible; there is no way to unfreeze. It takes the write
lock, so it waits out every in-flight borrow and must never be called from inside
one on the same enclave. A refused freeze leaves the enclave alive and writable:
destroy it rather than return material whose protection was not established.

```go
e, err := enclave.NewMemorySized(n)
if err != nil {
	return nil, err
}
if err := e.WithBytes(func(dst []byte) error {
	_, err := io.ReadFull(rand.Reader, dst) // produce material here
	return err
}); err != nil {
	_ = e.Destroy()
	return nil, err
}
var f enclave.Freezer
if !enclave.As(e, &f) {
	_ = e.Destroy()
	return nil, &enclave.CapabilityError{Backend: e.Backend().Name(), Capability: enclave.CapabilityFreeze}
}
if err := f.Freeze(); err != nil { // a stray write faults from here
	_ = e.Destroy() // refused: still writable, so never hand it out
	return nil, err
}
return e, nil
```

go-quicraft uses this shape for the unsealed root key, a Shamir share value and a
reconstructed secret. `Sealed.Open` freezes its own result, so
`Barrier.RootKeyProtected` needs no `Freeze` of its own. Do not freeze an enclave whose last act is to be consumed
by `NewSealed` or `NewMemory`: those wipe what they are handed, and a wipe
of frozen pages faults the process.

### Hold a key encrypted at rest in process memory

```go
func NewSealed(ctx context.Context, data []byte) (Sealed, error)
```

| | |
|---|---|
| Request | `ctx`, which bounds the seal and reaches the process sealer's provider; `data []byte`, the raw key material. `data` is wiped on every return path, failure and cancellation included. |
| Response | `Sealed`, an interface: ciphertext on the ordinary heap, holding no locked pages; a nil interface on failure |
| Errors | `ErrEmptyEnclave`, `ErrMaterialTooLarge`, `ErrSealFailed`, `ErrLockedMemoryUnavailable`, `ErrSealerWiped`, `ErrSealerDestroyed` (after `Sealer.Destroy` on the process sealer). A context that has ended fails the seal with an error matching both `ErrSealFailed` and `ctx.Err()`. |
| Permission | none |

### Materialize the plaintext for a bounded window

```go
// Sealed interface methods
Open(ctx context.Context) (ByteAccess, error)
OpenIn(ctx context.Context, b Backend) (Handle, error)
Openable() error
IsAlive() bool
Destroy() error // always nil today
```

| | |
|---|---|
| Request | `ctx`, which bounds the open (`OpenIn` also takes the `Backend` to open into; an `Allocator` charges the destination against its budget) |
| Response | a new memory enclave, as a `ByteAccess`, holding the plaintext, `mlock`'d and FROZEN read-only. The caller owns it and must `Destroy` it. Each call returns an independent enclave. `OpenIn` returns a `Handle`; into locked memory it is a `ByteAccess`. |
| Errors | `ErrEnclaveDestroyed`, `ErrSealedAuthentication`, `ErrLockedMemoryUnavailable`, `ErrSealerWiped`, `ErrSealerDestroyed` (after `Sealer.Destroy`), `ErrOpenFailed`; `OpenIn` adds `ErrInvalidBackend`, a `*CapabilityError`, and, into an `Allocator`, `ErrNilAllocator` and `ErrBudgetExhausted`. A context that has ended fails the open with an error matching both `ErrOpenFailed` and `ctx.Err()`, and leaves the sealed value intact. |
| Permission | none |

The plaintext never reaches the ordinary Go heap: the destination is allocated
first and the AEAD decrypts into it in place, and the destination is frozen
before it is returned. A write through the result faults, so code that needs a
writable copy allocates its own sized enclave and copies into it inside a
borrow. A refused freeze fails the open closed: no enclave is returned and the
destination is wiped.

`ErrSealerWiped` means the sealer's hierarchy is dead (its root or current KEK
was destroyed by a sweep). A per-call key destroyed by a concurrent sweep
surfaces as `ErrEnclaveDestroyed` instead.

`Destroy` wipes the ciphertext and the wrapped-key blob and does not affect
enclaves already handed out. It does not wait for an `Open` in flight: that open
completes against the envelope it pinned, an open that starts after `Destroy`
returns `ErrEnclaveDestroyed`, and the last open out wipes the ciphertext. `IsAlive` reports on this handle's ciphertext. `Openable` answers a
different question, whether an `Open` could still succeed, which a shutdown
`WipeAll` can change without touching this value.

### Read protected bytes with a bounded lifetime

```go
// ByteAccess interface methods (Size, IsAlive and Destroy come from Handle)
WithBytes(fn func([]byte) error) error
Size() int
IsAlive() bool
EqualTo(other []byte) (bool, error)
Destroy() error
```

| | |
|---|---|
| Request | `WithBytes`: `fn`, called with a slice aliasing the protected region. `EqualTo`: the bytes to compare against; its timing reveals whether the lengths differ, never the contents. |
| Response | `WithBytes`: `fn`'s error, unchanged. `EqualTo`: whether the contents match. |
| Errors | Both return `ErrEnclaveDestroyed` if the enclave is destroyed; `WithBytes` then does not call `fn`. |
| Permission | none |

`WithBytes` is the only way to read the bytes. The slice is valid for `fn`'s
call and no longer: the read lock is held across `fn`, so a concurrent `Destroy`
or `Freeze` waits for it, and once `fn` returns nothing covers the slice. Scope
each borrow to the code that uses the bytes, and copy nothing out of it.
No accessor returns the slice outside a borrow, and none returns the material
as a string.

`Destroy` returns `nil` for a successful release, for a nil enclave and for
every call after a successful release. It returns `ErrReleasedWithoutWipe` when
a denied `mprotect` on a frozen enclave forced a bare unmap (the pages are
gone; a writable enclave was wiped before the `mprotect` and returns `nil`), and
`ErrLockedMemoryUnavailable` when the kernel refused the release (the pages are
still held, and calling `Destroy` again retries). The two are disjoint. It
returns `ErrEnclaveCorrupted` when the canary or guard pages were overwritten;
the pages are still wiped and released, and `Destroy` never panics. A bare
`defer e.Destroy()` still compiles and discards the report; go-quicraft's own
cleanup paths log it (`pkg/seal`) or document why they drop it
(`pkg/crypto/shamir`).

`fn` must not let the slice escape and must not re-enter the same enclave. It may
use a different enclave: go-enclave holds only the borrowed enclave's own read
lock, and there is no process-wide lock.

`shamir.ProtectedShare` mirrors it as `WithValue`, the share value's only
accessor.

### Wipe an ordinary slice

```go
func Wipe(b []byte)
```

Zeroes `b` in place and handles the empty slice. Use it for the copy a
third-party API forced you to make; it is not a substitute for an enclave.

### Error vocabulary

```go
var ErrEnclaveDestroyed        = errors.New("enclave: enclave has been destroyed")
var ErrEmptyEnclave            = errors.New("enclave: refusing to create an enclave over empty material")
var ErrInvalidSize             = errors.New("enclave: enclave size must be positive and small enough to map")
var ErrLockedMemoryUnavailable = errors.New("enclave: could not allocate locked memory")
var ErrReleasedWithoutWipe     = errors.New("enclave: pages unmapped without an in-process wipe; mprotect was denied")
var ErrEnclaveCorrupted        = errors.New("enclave: canary verification failed; memory ahead of the enclave was overwritten")
var ErrSealFailed              = errors.New("enclave: could not seal material")
var ErrOpenFailed              = errors.New("enclave: could not open sealed material")
var ErrSealedAuthentication    = errors.New("enclave: sealed material failed authentication")
```

Match with `errors.Is`. `LockedMemoryError` and `BudgetError` carry diagnostic
detail and match their sentinels, so callers never need the types.

A destroyed enclave reports `ErrEnclaveDestroyed`; one sentinel meaning
"the material is gone" is the whole contract. `pkg/seal` translates it at its own
boundary: a destroyed strategy passphrase surfaces as `ErrStrategyClosed`.

## Versioning and deprecation

The protected-memory surface is versioned by go-enclave; this repository pins a
released tag.

`seal.Credentials`, built by `seal.NewCredentials`, is the only credentials
type. `Credentials.Secret` is an `enclave.Sealed`, so there is no
separate protected form; implementations name `Credentials`.
