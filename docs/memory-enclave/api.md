# Memory enclave: API

> **Status: NORMATIVE** · Scope: go-quicraft's use of go-enclave protected memory · Last verified: 2026-10-02
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

Import: `github.com/jeremyhahn/go-enclave` (go-quicraft requires v0.6.2-alpha).

All of the surface belongs to go-enclave. go-quicraft declares no
protected-memory type, alias or wrapper, and re-exports nothing: consumers
import go-enclave and call it directly. The only go-quicraft identifiers on this
page are the ones in `pkg/seal` and `pkg/crypto/shamir` that take or return
go-enclave types.

### Allocate locked memory

```go
func NewMemoryEnclaveSized(n int) (*MemoryEnclave, error)  // writable, fill in place
func NewMemoryEnclave(data []byte) (*MemoryEnclave, error) // moves data in, FROZEN
func NewMemoryEnclaveCopy(src []byte) (*MemoryEnclave, error) // copies, FROZEN, src untouched
```

| | |
|---|---|
| Request | `n int`, or `data []byte` (wiped on every return path, failure included) |
| Response | `*MemoryEnclave`: `mlock`'d, guard-paged, canary-checked, excluded from core dumps |
| Errors | `ErrInvalidSize`, `ErrEmptyEnclave`, and `ErrLockedMemoryUnavailable` when the kernel refuses the lock |
| Permission | none; in-process call |

They return an error because locking pages is a syscall the kernel can decline.

Mutability differs. A sized enclave stays writable until it is frozen, which is
what makes filling it possible, so a slice retained past a borrow can silently
corrupt the material. One built by `NewMemoryEnclave` or `NewMemoryEnclaveCopy`
is frozen read-only, where the same mistake faults.

### Freeze material generated in place

```go
func (e *MemoryEnclave) Freeze() error
```

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
e, err := enclave.NewMemoryEnclaveSized(n)
err = e.WithBytes(func(dst []byte) error { /* produce material here */ })
err = e.Freeze() // a stray write faults from here
```

go-quicraft uses this shape for the unsealed root key, a Shamir share value and a
reconstructed secret. Do not freeze an enclave whose last act is to be consumed
by `NewSealed` or `NewMemoryEnclave`: those wipe what they are handed, and a wipe
of frozen pages faults the process.

### Hold a key encrypted at rest in process memory

```go
func NewSealed(data []byte) (*SealedEnclave, error)
```

| | |
|---|---|
| Request | `data []byte`, the raw key material. Wiped on every return path, failure included. |
| Response | `*SealedEnclave`: ciphertext on the ordinary heap, holding no locked pages |
| Errors | `ErrEmptyEnclave`, `ErrMaterialTooLarge`, `ErrSealFailed`, `ErrLockedMemoryUnavailable`, `ErrSealerWiped` |
| Permission | none |

### Materialize the plaintext for a bounded window

```go
func (s *SealedEnclave) Open() (*MemoryEnclave, error)
func (s *SealedEnclave) OpenWith(a *Allocator) (*MemoryEnclave, error)
func (s *SealedEnclave) Openable() error
func (s *SealedEnclave) IsAlive() bool
func (s *SealedEnclave) Destroy()
```

| | |
|---|---|
| Request | none (`OpenWith`: the `Allocator` to charge the destination against) |
| Response | a new `*MemoryEnclave` holding the plaintext, `mlock`'d and writable (not frozen). The caller owns it and must `Destroy` it. Each call returns an independent enclave. |
| Errors | `ErrEnclaveDestroyed`, `ErrSealedAuthentication`, `ErrLockedMemoryUnavailable`, `ErrSealerWiped`; `OpenWith` adds `ErrNilAllocator` and `ErrBudgetExhausted` |
| Permission | none |

The plaintext never reaches the ordinary Go heap: the destination is allocated
first and the AEAD decrypts into it in place. Freeze the result yourself if you
hand it on; `Barrier.RootKeyProtected` does.

`Destroy` wipes the ciphertext and the wrapped-key blob, takes the write lock so
it waits out an `Open` in flight, and does not affect enclaves already handed
out. `IsAlive` reports on this handle's ciphertext. `Openable` answers a
different question, whether an `Open` could still succeed, which a shutdown
`WipeAll` can change without touching this value.

### Read protected bytes with a bounded lifetime

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
| Errors | Both return `ErrEnclaveDestroyed` if the enclave is destroyed; `WithBytes` then does not call `fn`. |
| Permission | none |

`Open`'s slice aliases the locked pages and no lock is held once it returns, so
a concurrent `Destroy` unmaps memory the caller is still reading and the read
faults. Use it only where no concurrent teardown is possible, such as
single-goroutine ownership with `defer e.Destroy()`. Wherever teardown can race
a reader, use `WithBytes`, which holds the read lock across `fn`.

`fn` must not let the slice escape and must not re-enter the same enclave. It may
use a different enclave: go-enclave holds only the borrowed enclave's own read
lock, and there is no process-wide lock.

`shamir.ProtectedShare` mirrors the pair as `Value` / `WithValue`.

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
var ErrSealFailed              = errors.New("enclave: could not seal material")
var ErrSealedAuthentication    = errors.New("enclave: sealed material failed authentication")
```

Match with `errors.Is`. `LockedMemoryError` and `BudgetError` carry diagnostic
detail and match their sentinels, so callers never need the types.

A destroyed or nil enclave reports `ErrEnclaveDestroyed`; one sentinel meaning
"the material is gone" is the whole contract. `pkg/seal` translates it at its own
boundary: a destroyed strategy passphrase surfaces as `ErrStrategyClosed`.

## Versioning and deprecation

The protected-memory surface is versioned by go-enclave; this repository pins a
released tag.

`seal.ProtectedCredentials` and `seal.NewProtectedCredentials` are deprecated
aliases for `seal.Credentials` and `seal.NewCredentials`; `Credentials.Secret` is
a `*enclave.SealedEnclave`, so the two are one type. The aliases remain so
implementations whose method signatures name `ProtectedCredentials` still satisfy
`ProtectedSealingStrategy`, and are removed at the next breaking release.
