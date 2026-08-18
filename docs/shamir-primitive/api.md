# Shamir Primitive — API

> **Status: NORMATIVE** · Scope: go-quicraft `pkg/crypto/shamir`, plus the
> share-custody interfaces in `pkg/seal` · Last verified: 2026-08-19
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

### Capability: split a secret into N shares (bytes form — preferred)

```go
func SplitBytes(secret []byte, threshold, total int) ([][]byte, error)
```

| | |
|---|---|
| Request | `secret` — 1..`MaxSecretBytes` (65536) bytes; `threshold` — 2..255; `total` — `threshold`..255 |
| Response | `[][]byte` in `Index` order: `values[i]` is the value of share `i+1`. Each is the raw `[version:1][x:1][y...]` payload — the same bytes `Share.Value` base64-encodes. |
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
exposure in the scheme. Prefer `SplitBytes` in new custody code.

### Capability: reconstruct a secret from M shares (bytes form — preferred)

```go
func CombineBytes(values [][]byte, threshold int) ([]byte, error)
```

| | |
|---|---|
| Request | `values` — M..255 raw share payloads; `threshold` — the true M, **from a trusted source** |
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
type SplitError      struct{ Reason string }
type CombineError    struct{ Reason string }
type ValidationError struct{ Field, Reason string }
type IntegrityError  struct{}   // DEPRECATED — never returned
```

Match with `errors.As`. Never branch on `*IntegrityError`: shares carry no
digest, so no code path constructs one.

### Constants

```go
const MaxSecretBytes = 64 << 10   // 65536
```

The share-set maximum is 255, from the GF(2^8) x-coordinate space.

## Go API — the custody interfaces

Import: `github.com/jeremyhahn/go-quicraft/pkg/seal`

**Declared here, implemented above.** This repository ships no implementation of
either interface.

```go
type ShareSplitter interface {
    Split(secret []byte, threshold, total int) ([]string, error)
}

type ShareAccumulator interface {
    AddShare(ctx context.Context, share string) (*QuorumProgress, error)
    GetProgress() *QuorumProgress
    Reconstruct(ctx context.Context) ([]byte, error)
    Reset()
    Expired() bool
}
```

| Type | Role |
|---|---|
| `ShamirConfig{Threshold, TotalShares, QuorumTTL}` | configuration shape for an M-of-N setup; `QuorumTTL` defaults to `DefaultQuorumTTL` (5 minutes) when zero |
| `ShamirInitResult{Shares, Threshold, TotalShares}` | output of a Shamir-based initialization |
| `QuorumProgress{Required, Submitted, Complete}` | accumulator state |

Implementer obligations: `ShareAccumulator` handles deduplication, ordering and
TTL expiration, and is **not** required to be concurrency-safe — the `Barrier`
serializes calls through its own mutex.

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
- A signature change to `ShareSplitter` or `ShareAccumulator` breaks go-qrdb's
  implementations. Sequence such changes bottom-up.
