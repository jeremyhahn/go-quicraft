# Shamir Primitive — API

> **Status: NORMATIVE** · Scope: go-quicraft `pkg/crypto/shamir`, plus the
> share-custody interfaces in `pkg/seal` · Last verified: 2026-09-02
> Conflicts with this document are defects. Fix them here first.

## Transport coverage

go-quicraft is a library. **The Go API is the API.**

| Transport | This feature |
|---|---|
| **Embedded (Go call)** | The whole surface below. This is the transport. |
| Unix socket | n/a — this repository serves no socket |
| REST | n/a — this repository exposes no HTTP handler |
| gRPC | n/a — this repository defines no gRPC service |
| QUIC | n/a. The repository's QUIC listener carries Raft messages only (see [`../quic-transport/api.md`](../quic-transport/api.md)); no share ever crosses it. |
| MCP | n/a — this repository registers no MCP tools |

These are not missing-capability rows. In the portfolio capability register the
**primitive** is this repository's, re-exported one layer up as a type surface;
the **custody** capability — which is what a remote transport would carry — is
owned by go-qrdb and exposed on go-qrdb's transports. A share-bearing endpoint
in this repository would be the defect, not the fix.

## Go API — the primitive

Import: `github.com/jeremyhahn/go-quicraft/pkg/crypto/shamir`

### Capability: split a secret into N shares (protected form — preferred)

```go
func SplitProtected(secret []byte, threshold, total int) ([]*ProtectedShare, error)
```

| | |
|---|---|
| Request | `secret` — 1..`MaxSecretBytes` (65536) bytes; `threshold` — 2..255; `total` — `threshold`..255 |
| Response | `[]*ProtectedShare` in `Index` order. Each share's value lives in an `enclave.MemoryEnclave` — mlock'd, guard-paged, and genuinely wiped by `Destroy`. |
| Errors | `*SplitError` on any bound violation or RNG failure, **including** a whole-set footprint above `MaxShareSetBytes` |
| Permission | none — in-process call, no authorization boundary |
| Caller obligation | **`Destroy` every share**, ideally as each is consumed rather than at the end of the ceremony |

Native, not a wrapper: coefficients are read into locked memory and each share
value is encoded directly into the locked buffer that share will own, so no
secret-derived byte reaches an ordinary heap buffer.

### Capability: split a secret into N shares (bytes form)

```go
func SplitBytes(secret []byte, threshold, total int) ([][]byte, error)
```

| | |
|---|---|
| Request | `secret` — 1..`MaxSecretBytes` (65536) bytes; `threshold` — 2..255; `total` — `threshold`..255 |
| Response | `[][]byte` in `Index` order: `values[i]` is the value of share `i+1`. Each is the **base64 encoding** of `[version:1][x:1][y...]` — byte-for-byte what `Share.Value` holds, not the pre-encoding payload. |
| Errors | `*SplitError` on any bound violation or RNG failure |
| Permission | none — in-process call, no authorization boundary |
| Caller obligation | **wipe the returned buffers** once distributed |

### Capability: split a secret into N shares (struct form)

```go
func Split(secret []byte, threshold, total int) ([]*Share, error)
```

Same parameters and errors. Returns `[]*Share` with `Index` 1..N.

`Share.Value` is a Go **string** and cannot be zeroed; it stays readable in the
heap until GC. A ceremony holds all N at once, so this is the point of widest
exposure in the scheme. Prefer `SplitProtected` in new custody code, or
`SplitBytes` if the caller manages the wipeable buffers itself.

### Capability: reconstruct a secret from M shares (protected form — preferred)

```go
func CombineProtected(shares []*ProtectedShare) (*enclave.MemoryEnclave, error)
```

| | |
|---|---|
| Request | `shares` — at least `threshold` shares of one split; `Threshold` is taken from `shares[0]` and is unauthenticated |
| Response | the reconstructed secret in a `*enclave.MemoryEnclave` |
| Errors | `*CombineError` on any bound violation, duplicate/zero x-coordinate, or ragged share set |
| Permission | none — in-process call, no authorization boundary |
| Caller obligation | **`Destroy` the returned enclave**, and the input shares |

Holds only **one decoded share at a time**, walking the set twice against a
single reused buffer: peak locked memory is a property of the share size, not
of how many shares were submitted.

### Capability: decode a share set that arrived from outside the process

```go
func UnmarshalShareSet(data []byte) ([]*ProtectedShare, error)
func NewProtectedShare(index, threshold, total int, value []byte) (*ProtectedShare, error)
```

Use `UnmarshalShareSet` — **never** `json.Unmarshal` into a
`[]*ProtectedShare`. Shares are unauthenticated by construction, so a recovery
endpoint decodes whatever it is handed; the generic decoder allocates one locked
buffer per element as it visits them, which lets the submission rather than the
service choose how much locked memory to take, exhausting a process-wide
resource every other caller depends on. `UnmarshalShareSet` bounds the element
count (255) and the document size (`MaxShareSetBytes`) *before* allocating, and
destroys the accepted prefix if a later element is rejected.

`NewProtectedShare` builds one share at a time, and where it wipes the source
buffer follows the error type exactly:

- **`*ValidationError`** (bad header, empty value, non-standard base64, oversized
  value) — the source is **left untouched** and the caller must wipe it. Every
  one of these checks runs before any locked memory is allocated, which is also
  why an unauthenticated submission cannot size that allocation.
- **Success, or `*ProtectedMemoryError`** — the source is **WIPED**. On success
  ownership of the share material transfers to the returned share; on a refused
  `mlock` the value is gone anyway, because the locked-memory constructor wipes
  its source before reporting the refusal. Do not retry that error with the same
  buffer.

That split is the reason the two error types exist; see §Error vocabulary.
A ceremony that legitimately needs more than the budget does its own accounting
and decodes share by share with it.

### Capability: reconstruct a secret from M shares (bytes form)

```go
func CombineBytes(values [][]byte, threshold int) ([]byte, error)
```

| | |
|---|---|
| Request | `values` — M..255 base64-encoded share values (what `SplitBytes` returned); `threshold` — the true M, **from a trusted source** |
| Response | the reconstructed secret |
| Errors | `*CombineError` for structural violations: too few, too many, wrong format version, zero or duplicate x, mismatched payload length, oversized payload |
| Permission | none |
| Caller obligation | authenticate the result out of band; wipe the input buffers |

Every decoded buffer is wiped on every return path, including partially decoded
and validation-rejected shares.

### Capability: reconstruct a secret from M shares (struct form)

```go
func Combine(shares []*Share) ([]byte, error)
func CombineUnverified(shares []*Share) ([]byte, error)
```

Identical implementations; both names exist so a call site can state intent.
`threshold` is taken from `shares[0].Threshold`, which is **unauthenticated**.

**Neither verifies the result.** With fewer than threshold genuine shares, a
corrupted share, a share from another split, or a mislabelled `Threshold`, the
return is `(indistinguishable garbage, nil)`. The canonical out-of-band check is
to use the result as a key in an authenticated decrypt that fails on a wrong key
— which is exactly what `Barrier.Unseal` does.

### Capability: cross-check one share against others

```go
func VerifyShare(share *Share, otherShares []*Share) error
```

| | |
|---|---|
| Request | one share and a set to compare it against |
| Response | `nil` if consistent |
| Errors | `*ValidationError` for a nil share (its position named), a `Threshold`/`Total` mismatch, or a duplicate `Index` |
| Permission | none |

This checks **metadata consistency**, not cryptographic validity. It cannot tell
you a share is genuine.

### Capability: inspect and serialize a share

```go
type Share struct {
    Index     int               `json:"index"`
    Threshold int               `json:"threshold"`
    Total     int               `json:"total"`
    Value     string            `json:"value"`
    Metadata  map[string]string `json:"metadata,omitempty"`
}

func (s *Share) Bytes() ([]byte, error)        // raw decoded value
func (s *Share) Validate() error               // nil-receiver safe
func (s *Share) String() string                // value REDACTED
func (s *Share) LogValue() slog.Value          // value REDACTED
func (s *Share) MarshalJSON() ([]byte, error)  // value INCLUDED
func (s *Share) UnmarshalJSON([]byte) error
```

`Threshold` and `Total` are unauthenticated caller metadata. `Metadata` carries
no cryptographic meaning.

The redaction asymmetry is deliberate: `String` and `LogValue` are the paths
that reach humans and logs, `MarshalJSON` is the path that reaches storage and
distribution. See [`engineering.md`](engineering.md) §7.

### Error vocabulary

```go
type SplitError           struct{ Reason string }
type CombineError         struct{ Reason string }
type ValidationError      struct{ Field, Reason string }
type ProtectedMemoryError struct{ Op string; Err error }  // Unwrap() error
type IntegrityError       struct{}   // DEPRECATED — never returned
```

Match with `errors.As`. Never branch on `*IntegrityError`: shares carry no
digest, so no code path constructs one.

**`ProtectedMemoryError` vs `ValidationError` is an OWNERSHIP contract, not a
wording choice, and `NewProtectedShare` is where it bites.** A `*ValidationError`
means the value never reached protected memory and is **left untouched** — every
syntactic check runs before any locked memory is allocated. A
`*ProtectedMemoryError` means the kernel refused the `mlock`, and by then the
value has **already been wiped**: go-enclave's constructor wipes its source
before it reports a refusal. Do not retry with the same buffer on that error; it
is zeroes. It is also the one an operator can act on — it says the host or
container is out of locked memory, not that the input was wrong. It unwraps to
the allocator's own error, so `errors.Is` reaches go-enclave's sentinels.

### Constants

```go
const MaxSecretBytes   = 64 << 10   // 65536 — one secret
const MaxShareSetBytes = 1 << 20    // 1 MiB — whole-set locked-memory budget
```

The share-set maximum is 255, from the GF(2^8) x-coordinate space.

`MaxShareSetBytes` exists because individually legal arguments multiply into a
footprint no host can lock — a 64 KiB secret split 255 ways needs ~16 MiB of
coefficients alone, and a failed lock is a dead process rather than an error. A
full 255-share ceremony over a 32-byte root key needs ~24 KiB of the budget;
sharing a 4 KiB *payload* 255 ways does not fit and should be enveloped under a
key that does.

## Go API — the custody interfaces

Import: `github.com/jeremyhahn/go-quicraft/pkg/seal`

**Declared here, implemented above.** This repository ships no implementation of
either interface.

```go
type ProtectedShareSplitter interface {
    SplitProtected(secret []byte, threshold, total int) ([]*shamir.ProtectedShare, error)
}

type ProtectedShareAccumulator interface {
    AddProtectedShare(ctx context.Context, share *shamir.ProtectedShare) (*QuorumProgress, error)
    GetProgress() *QuorumProgress
    ReconstructProtected(ctx context.Context) (*enclave.MemoryEnclave, error)
    Reset()
    Expired() bool
}
```

**These are the only custody interfaces.** The string-typed `ShareSplitter` and
`ShareAccumulator` existed through `v0.8.0-alpha` and were removed in
`v0.9.0-alpha` (#37, owner-ruled).
A Go string cannot be zeroed: an implementation that split into locked memory
still had to copy each share back out into an immortal heap string to satisfy
`Split`'s return type, and `AddShare` received a share that was un-wipeable
before any protected buffer could exist — so a consumer could migrate to the
protected primitives, silence every `SA1019`, and leave the exposure exactly
where it was. Keeping the string forms as deprecated siblings preserved a shape
whose only use was to undo the guarantee.

The protected forms close both ends: `AddProtectedShare` **takes ownership** of
the share it is given (and destroys it whether or not it is accepted, and whether
or not the quorum completes), and `ReconstructProtected` returns the secret in an
enclave the caller owns and must `Destroy`.

| Type | Role |
|---|---|
| `ShamirConfig{Threshold, TotalShares, QuorumTTL}` | configuration shape for an M-of-N setup; `QuorumTTL` defaults to `DefaultQuorumTTL` (5 minutes) when zero |
| `ShamirInitResult{Shares, Threshold, TotalShares}` | output of a Shamir-based initialization. `Shares` is `[]*shamir.ProtectedShare`; the result OWNS them and `Destroy()` releases them all |
| `QuorumProgress{Required, Submitted, Complete}` | accumulator state |

`ShamirInitResult` is where a ceremony holds all N shares at once. Its JSON
`"shares"` is an array of share OBJECTS — the same encoding `shamir.Share` and
`shamir.ProtectedShare` produce — not the array of bare values the `[]string`
field emitted before #37.

Implementer obligations: `ProtectedShareAccumulator` handles deduplication,
ordering and TTL expiration, and is **not** required to be concurrency-safe —
the `Barrier` serializes calls through its own mutex.

## Go API — the protected sealing pair

`SealingStrategy` keeps its `[]byte` root-key methods. Alongside them, a strategy
MAY implement the protected pair (#37):

```go
type ProtectedSealingStrategy interface {
    SealingStrategy

    SealRootKeyProtected(ctx context.Context, rootKey *enclave.MemoryEnclave, creds ProtectedCredentials) (*SealedRootKey, error)
    UnsealRootKeyProtected(ctx context.Context, sealed *SealedRootKey, creds ProtectedCredentials) (*enclave.MemoryEnclave, error)
}

// The passphrase is protected too. Built by NewProtectedCredentials, which
// wipes the caller's slice; the caller owns it and releases it with Clear.
type ProtectedCredentials struct {
    Secret *enclave.ProtectedKey
}
```

It exists because the `[]byte` signatures force an implementation to RETAIN the
root key in ordinary memory: go-qrdb's recovery strategy reconstructs the key
into an enclave and then copies it into a plain field solely to be able to return
it (go-qrdb PR #76). Sealing borrows the caller's enclave and never copies the
key out of it; unsealing decrypts straight into a fresh enclave the caller owns
and must `Destroy`.

Optional by design — a caller type-asserts for it, and there is no fallback,
because a fallback is the unprotected copy wearing the protected API's name.
`SoftwareStrategy` implements both; `NoneStrategy` implements
`SealRootKeyProtected` and answers `UnsealRootKeyProtected` with
`ErrNoRootKeyAvailable`, since no-encryption mode has no root key and an enclave
cannot hold nothing.

## Versioning and deprecation

- **Share format**: `version` byte is `0x01`, gated on exact equality. See the
  repository-wide pre-1.0 pinning rule in
  [`../wire-protocol/architecture.md`](../wire-protocol/architecture.md): until
  the first production release, a layout change is made in place at version 1
  and consuming repositories are updated in the same change.
- **`IntegrityError`**: deprecated, retained for API/SDK compatibility, never
  returned.
- **`CombineUnverified`**: not deprecated. It is an intent-naming alias for
  `Combine` and both are supported.
- A signature change to `ProtectedShareSplitter`, `ProtectedShareAccumulator` or
  `ProtectedSealingStrategy` breaks go-qrdb's implementations. Sequence such
  changes bottom-up.
