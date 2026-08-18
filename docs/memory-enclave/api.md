# Memory Enclave — API

> **Status: NORMATIVE** · Scope: go-quicraft `pkg/enclave` · Last verified: 2026-08-19
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

Import: `github.com/jeremyhahn/go-quicraft/pkg/enclave`

### Capability: hold a key encrypted at rest in process memory

```go
func NewProtectedKey(key []byte) *ProtectedKey
```

| | |
|---|---|
| Request | `key []byte` — the raw key material. **Wiped by the constructor.** |
| Response | `*ProtectedKey`. Never `nil` on success; memguard aborts the process on locked-memory exhaustion rather than returning an error. |
| Errors | none returned |
| Permission | none — in-process call, no authorization boundary |

### Capability: materialize the plaintext for a bounded window

```go
func (p *ProtectedKey) Open() (*ProtectedBuffer, error)
func (p *ProtectedBuffer) Bytes() []byte
func (p *ProtectedBuffer) Size() int
func (p *ProtectedBuffer) Destroy()
```

| | |
|---|---|
| Request | none |
| Response | `*ProtectedBuffer` holding the plaintext, `mlock`'d and guard-paged. The caller **owns** it and must `Destroy` it. |
| Errors | `ErrEnclaveNil` (`*NilError`) if the key was never initialized or has been `Clear`ed; `*OpenError` wrapping the memguard failure otherwise. Both fail closed — no material is returned. |
| Permission | none |

`Bytes()` returns `nil` and `Size()` returns `0` after `Destroy`. The returned
slice must not outlive the buffer.

### Capability: allocate protected memory directly

```go
func NewProtectedBuffer(size int) *ProtectedBuffer
func NewProtectedBufferFromBytes(data []byte) *ProtectedBuffer
```

| | |
|---|---|
| Request | `size int`, or `data []byte` (**wiped** after copying) |
| Response | `*ProtectedBuffer` |
| Errors | none returned |
| Permission | none |

### Capability: promote a buffer to an encrypted-at-rest key

```go
func SealProtectedBuffer(buf *ProtectedBuffer) *ProtectedKey
```

| | |
|---|---|
| Request | `buf` — **consumed**; ownership transfers to the returned key |
| Response | `*ProtectedKey`, or `nil` if `buf` was `nil`, already consumed, or destroyed. A concurrent second call gets `nil`. |
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

Zeroes `b` in place. Handles the empty slice. Use it on any transient copy of
key material that could not be avoided.

### Error vocabulary

```go
var ErrEnclaveNil = &NilError{}   // "memprotect: enclave is nil"

type NilError struct{}
func (e *NilError) Error() string

type OpenError struct{ Err error }
func (e *OpenError) Error() string
func (e *OpenError) Unwrap() error
```

Match with `errors.Is(err, enclave.ErrEnclaveNil)` and `errors.As(err,
&openErr)`.

## Re-exports

`pkg/seal` re-exports this surface as type aliases and function variables so
`pkg/seal` consumers need not import `pkg/enclave` separately:

```go
type ProtectedBuffer = enclave.ProtectedBuffer
type ProtectedKey    = enclave.ProtectedKey

var NewProtectedBuffer          = enclave.NewProtectedBuffer
var NewProtectedBufferFromBytes = enclave.NewProtectedBufferFromBytes
var NewProtectedKey             = enclave.NewProtectedKey
var SealProtectedBuffer         = enclave.SealProtectedBuffer
```

These are aliases, not wrappers. There is no second implementation and no
behavioral difference.

## Versioning and deprecation

Nothing in this package is deprecated. The surface is pre-1.0 and may change;
see the repository's compatibility policy in
[`../wire-protocol/architecture.md`](../wire-protocol/architecture.md) for what
"pre-1.0" means here. Because the types are re-exported by `pkg/seal`, a
signature change breaks both import paths at once, in the same module version.
