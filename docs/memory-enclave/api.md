# Memory Enclave — API

> **Status: NORMATIVE** · Scope: go-quicraft's use of go-enclave protected memory · Last verified: 2026-09-03
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

Import: `github.com/jeremyhahn/go-enclave`.

### Where the surface lives

**All of it is go-enclave's.** go-quicraft declares no protected-memory type, no
alias for one, and no wrapper around one: `pkg/enclave` was deleted in #54 and
`pkg/seal`'s re-exports (`ProtectedBuffer`, `ProtectedKey`, `NewProtectedKey`,
`SealProtectedBuffer`) went with it. This is `docs/00 §R` — *"No project
hand-rolls its own locked-memory handling"* — and a re-export is a second name for
a type that should have one.

Consumers that held those names import go-enclave and call it directly. The
translation is mechanical and total:

| was | is now |
|---|---|
| `enclave.MemoryEnclave`, `enclave.ProtectedBuffer`, `seal.ProtectedBuffer` | `enclave.MemoryEnclave` (go-enclave) |
| `enclave.ProtectedKey`, `seal.ProtectedKey` | `enclave.SealedEnclave` |
| `enclave.NewProtectedKey(b)` | `enclave.NewSealed(b)` — now returns an error |
| `pk.Clear()` | `pk.Destroy()` — and it now WIPES |
| `enclave.SealMemoryEnclave(buf)` | fill-and-`Freeze` a sized enclave, or `enclave.NewSealed` |
| `enclave.WipeBytes(b)` | `enclave.Wipe(b)` |
| `enclave.ErrEnclaveNil`, `*NilError`, `*OpenError` | `enclave.ErrEnclaveDestroyed` |
| `seal.Credentials{Secret: []byte(pw)}` | `seal.NewCredentials([]byte(pw))` |

### Capability: allocate locked memory

```go
func enclave.NewMemoryEnclaveSized(n int) (*MemoryEnclave, error)  // writable, fill in place
func enclave.NewMemoryEnclave(data []byte) (*MemoryEnclave, error) // moves data in, FROZEN
func enclave.NewMemoryEnclaveCopy(src []byte) (*MemoryEnclave, error) // copies, FROZEN, src untouched
```

| | |
|---|---|
| Request | `n int`, or `data []byte` (**wiped** on every return path, including failure) |
| Response | `*MemoryEnclave` — `mlock`'d, guard-paged, canary-checked, excluded from core dumps |
| Errors | `ErrInvalidSize`, `ErrEmptyEnclave`, and `ErrLockedMemoryUnavailable` when the kernel refuses the lock |
| Permission | none — in-process call, no authorization boundary |

**These return an error, and that is the point.** Locking pages is a syscall the
kernel can decline, and a constructor that cannot say so leaves the caller
asserting against a nil.

Mutability differs between them and it matters. A **sized** enclave stays
WRITABLE until it is frozen — that is what makes filling it possible — so a slice
retained past a borrow can silently corrupt the material. One built by
`NewMemoryEnclave` or `NewMemoryEnclaveCopy` is frozen read-only, where the same
mistake faults.

### Capability: freeze material generated in place

```go
func (e *MemoryEnclave) Freeze() error
```

| | |
|---|---|
| Request | none |
| Response | none |
| Errors | `ErrEnclaveDestroyed` on a nil or destroyed enclave; `ErrLockedMemoryUnavailable` when the kernel refuses the `mprotect` |
| Permission | none |

Idempotent, irreversible, and there is deliberately no `Melt`. It takes the write
lock, so it waits out every in-flight borrow and must never be called from inside
one on the same enclave. A refused freeze leaves the enclave ALIVE and WRITABLE —
destroy it rather than returning material whose protection was not established.

```go
e, err := enclave.NewMemoryEnclaveSized(n)
err = e.WithBytes(func(dst []byte) error { /* produce material here */ })
err = e.Freeze() // a stray write faults from here
```

That shape REPLACED a fill-then-move at three sites in this repository — the
unsealed root key, a Shamir share value and a reconstructed secret — each of
which previously built a second from-bytes enclave out of the first, paying two
locked regions and a copy. Do not freeze an enclave whose last act is to be
consumed by `NewSealed` or `NewMemoryEnclave`: those wipe what they are handed,
and a wipe of frozen pages faults the process.

### Capability: hold a key encrypted at rest in process memory

```go
func enclave.NewSealed(data []byte) (*SealedEnclave, error)
```

| | |
|---|---|
| Request | `data []byte` — the raw key material. **Wiped on every return path, failure included.** |
| Response | `*SealedEnclave` — ciphertext on the ordinary heap; it holds no locked pages |
| Errors | `ErrEmptyEnclave`, `ErrMaterialTooLarge`, `ErrSealFailed`, `ErrLockedMemoryUnavailable`, `ErrSealerWiped` |
| Permission | none |

### Capability: materialize the plaintext for a bounded window

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
| Response | a NEW `*MemoryEnclave` holding the plaintext, `mlock`'d and frozen. The caller **owns** it and must `Destroy` it. Each call returns an independent enclave. |
| Errors | `ErrEnclaveDestroyed`, `ErrSealedAuthentication`, `ErrLockedMemoryUnavailable`, `ErrSealerWiped` |
| Permission | none |

The plaintext never reaches the ordinary Go heap: the destination is allocated
first and the AEAD decrypts into it in place, with a capacity exactly the
plaintext length so it cannot reallocate.

`Destroy` wipes the ciphertext and the wrapped-key blob, takes the write lock so
it waits out an `Open` in flight, and does **not** affect enclaves already handed
out. `IsAlive` reports on this handle's ciphertext; `Openable` is the different
question of whether an `Open` could still succeed, which a shutdown `WipeAll` can
change without touching this value.

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
| Errors | Both return `ErrEnclaveDestroyed` if the enclave is destroyed. `WithBytes` does not call `fn`. |
| Permission | none |

`Open`'s slice aliases the locked pages and no lock is held once it returns, so a
concurrent `Destroy` unmaps memory the caller is still reading and the read
FAULTS. Use it only where the structure shows no concurrent teardown is possible
— single-goroutine ownership, the `defer e.Destroy()` shape used throughout the
seal barrier. Everywhere teardown can race a reader, use `WithBytes`, which holds
the read lock across `fn`.

`fn` must not escape the slice, and must not re-enter THIS enclave. **`fn` MAY
touch a different enclave** — a relaxation of what this page previously said.
Under this repository's former borrow machinery a process-wide `purgeMu` was held
shared across every borrow, so a nested borrow of any second enclave deadlocked
against a purge-capable operation queued anywhere in the process. go-enclave
holds only the borrowed enclave's own read lock, so that hazard is gone with the
lock that created it — and `purgeMu` went with the last memguard call site.

`shamir.ProtectedShare` mirrors the pair as `Value` / `WithValue`.

### Capability: wipe an ordinary slice

```go
func enclave.Wipe(b []byte)
```

Zeroes `b` in place. Handles the empty slice. Use it for the copy a third-party
API forced you to make; it is not a substitute for an enclave.

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
detail and report themselves as their sentinels, so callers match the sentinel
and never have to know about the types.

`ErrEnclaveNil`, `*NilError` and `*OpenError` are GONE with `pkg/enclave`. Their
job is done by `ErrEnclaveDestroyed`: a `SealedEnclave` that has been destroyed —
or is nil — reports exactly that, and one sentinel meaning "the material is gone"
is the whole contract. `pkg/seal` still translates it at its own boundary, where
a destroyed strategy passphrase surfaces as `ErrStrategyClosed`.

## Versioning and deprecation

The protected-memory surface is go-enclave's and is versioned there; this
repository pins a released tag (`conventions/dependency-resolution.md` §2).

`seal.ProtectedCredentials` and `seal.NewProtectedCredentials` are **deprecated
aliases** for `seal.Credentials` and `seal.NewCredentials`. They existed only
while `Credentials.Secret` was a `[]byte`; #41 retyped it onto the same
`*enclave.SealedEnclave`, so the two are one type. The aliases are kept so
implementations whose method signatures name `ProtectedCredentials` still satisfy
`ProtectedSealingStrategy`, and go at the next breaking release.
