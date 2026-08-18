# Shamir Primitive — Engineering Contract

> **Status: NORMATIVE** · Scope: go-quicraft `pkg/crypto/shamir`, plus the
> share-custody interfaces in `pkg/seal` · Last verified: 2026-08-19
> Conflicts with this document are defects. Fix them here first.

Import paths:
`github.com/jeremyhahn/go-quicraft/pkg/crypto/shamir` (the primitive)
`github.com/jeremyhahn/go-quicraft/pkg/seal` (the custody interfaces)

## 1. Exported surface

### `pkg/crypto/shamir`

```go
const MaxSecretBytes = 64 << 10   // 65536

// Split / combine — struct form
func Split(secret []byte, threshold, total int) ([]*Share, error)
func Combine(shares []*Share) ([]byte, error)
func CombineUnverified(shares []*Share) ([]byte, error)

// Split / combine — wipeable bytes form
func SplitBytes(secret []byte, threshold, total int) ([][]byte, error)
func CombineBytes(values [][]byte, threshold int) ([]byte, error)

// Cross-checking
func VerifyShare(share *Share, otherShares []*Share) error

type Share struct {
    Index     int               `json:"index"`
    Threshold int               `json:"threshold"`
    Total     int               `json:"total"`
    Value     string            `json:"value"`
    Metadata  map[string]string `json:"metadata,omitempty"`
}

func (s *Share) Bytes() ([]byte, error)
func (s *Share) Validate() error
func (s *Share) String() string
func (s *Share) LogValue() slog.Value
func (s *Share) MarshalJSON() ([]byte, error)
func (s *Share) UnmarshalJSON(data []byte) error

// Errors
type SplitError      struct{ Reason string }
type CombineError    struct{ Reason string }
type ValidationError struct{ Field, Reason string }
type IntegrityError  struct{}   // DEPRECATED — never returned
```

`Combine` and `CombineUnverified` are **identical**. Both names are retained so
a caller can express intent at the call site; neither verifies anything.

### `pkg/seal` — the custody interfaces this repository declares but does not implement

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

type ShamirConfig struct {
    Threshold   int           `json:"threshold"`
    TotalShares int           `json:"total_shares"`
    QuorumTTL   time.Duration `json:"quorum_ttl,omitempty"`
}

type ShamirInitResult struct {
    Shares      []string `json:"shares"`
    Threshold   int      `json:"threshold"`
    TotalShares int      `json:"total_shares"`
}

type QuorumProgress struct {
    Required  int  `json:"required"`
    Submitted int  `json:"submitted"`
    Complete  bool `json:"complete"`
}

const DefaultQuorumTTL = 5 * time.Minute
```

`ShareAccumulator` implementations handle deduplication, ordering and TTL
expiration, and are **not** required to be safe for concurrent use — the
`Barrier` provides that synchronization through its own mutex.

## 2. Wire format of a share

`Share.Value` is the base64 encoding of:

```
[version:1][x:1][y:len(secret)]
```

| Field | Value |
|---|---|
| `version` | `0x01` (`shareFormatVersion`). Decoders gate on **exact** equality; anything else is rejected as "legacy or foreign". |
| `x` | the field coordinate, `1..255`, equal to `Share.Index`. Never `0`. |
| `y` | one byte per secret byte |

Overhead is 2 bytes (`rawShareOverhead`). The `x` in the value — not
`Share.Index` — is what reconstruction uses, so a share string is
self-contained.

`parseRawShare` is the single copy of these rules, shared by both the struct and
the bytes reconstruction path, so the two can never diverge on which shares they
accept.

## 3. Invariants

**Correctness**

- Any `threshold` genuine shares of one split reconstruct the secret exactly.
- Any `threshold - 1` shares reveal nothing about the secret — information
  theoretically, not computationally.
- `Split` is randomized: two splits of the same secret share no values.
- `Split(secret, M, N)` returns shares in `Index` order, so `shares[i].Index ==
  i+1`; `SplitBytes` likewise returns `values[i]` as the value of share `i+1`.

**Bounds, enforced at every entry point**

| Rule | Enforced in | Error |
|---|---|---|
| `threshold >= 2` | `Split`, `CombineBytes`, `Share.Validate` | `SplitError` / `CombineError` / `ValidationError` |
| `total >= threshold` | `Split`, `Share.Validate` | as above |
| `threshold <= 255`, `total <= 255` | `Split` | `SplitError` |
| `len(secret) > 0` | `Split` | `SplitError` |
| `len(secret) <= MaxSecretBytes` | `Split` | `SplitError` |
| `len(shares) <= 255` (`maxShares`) | `Combine`, `CombineBytes` | `CombineError` |
| payload `<= MaxSecretBytes` | `parseRawShare` | `CombineError` |
| `x != 0` | `parseRawShare` | `CombineError` |
| no duplicate `x` | `parseRawShare` | `CombineError` |
| all `y` the same length as share 0's | `parseRawShare` | `CombineError` |
| all `Threshold`/`Total` equal to share 0's | `combine` | `CombineError` |

The payload bound is checked on the **first** share, before the rest of the set
is decoded — shares at a recovery ceremony are unauthenticated by construction,
so an oversized submission must be rejected immediately rather than after 255 of
them have been decoded and copied.

**Nil safety**

- `Share.Validate()` is safe on a `nil` receiver and reports
  `ValidationError{Field: "share", Reason: "share is nil"}`. A JSON `null` in a
  submitted share list decodes to a `nil *Share`, and operator input must not
  panic the process.
- A `nil` element of `shares` in `Combine`, or of `otherShares` in
  `VerifyShare`, is reported as an error naming its position and is never
  dereferenced.

**Memory**

- `SplitBytes` returns buffers the **caller owns and must wipe**.
- `Split` wipes the byte buffer it encoded from; the returned `Share.Value`
  strings cannot be wiped and persist until GC.
- `CombineBytes` wipes every decoded buffer on every return path — including
  partially decoded and validation-rejected shares. The caller still owns and
  must wipe the input buffers.

**Constant time**

`gfMul` and the GF(2^8) primitives in `gf256.go` are constant-time with respect
to secret operands: no table lookups indexed by secret data, no branches on
secret data, all data-dependent selection by branchless mask, using the AES
reduction polynomial `x^8 + x^4 + x^3 + x + 1` (0x11B). This is a requirement,
not an optimization detail — the operands are root-key material. A change here
that introduces a lookup table or a data-dependent branch is a defect regardless
of its benchmark result.

**The invariant that is NOT provided**

`Combine` performs no self-verification. Fewer than threshold genuine shares, a
corrupted share, a share from a different split, or a mislabelled `Threshold`
all produce **indistinguishable garbage and a `nil` error**. `Share.Threshold`
and `Share.Total` are unauthenticated caller-supplied metadata.

## 4. Error semantics

| Type | Fields | Raised by |
|---|---|---|
| `*SplitError` | `Reason` | `Split`, `SplitBytes` — parameter and size violations, and RNG failure |
| `*CombineError` | `Reason` | `Combine`, `CombineUnverified`, `CombineBytes` — set-level and per-share structural violations |
| `*ValidationError` | `Field`, `Reason` | `Share.Validate`, `VerifyShare` — per-field violations, positionally named |
| `*IntegrityError` | — | **never**. Deprecated; retained for API/SDK compatibility only. |

- All four are concrete pointer types; match with `errors.As`.
- Error strings name the offending share by its position in the input slice
  (`"share 3 has zero x-coordinate"`), not by custodian identity — this package
  has no notion of one.
- **Never** does an error message contain share bytes or secret bytes.
- **Fail-closed paths**: every validation listed in §3 returns before any field
  arithmetic runs. There is no partial reconstruction and no best-effort mode.
  `Split` fails closed on RNG error rather than proceeding with weak
  coefficients.
- **The path that does not fail closed, by design**: a semantically wrong but
  structurally valid share set. It cannot be detected here (see §3) and must be
  caught by the caller's out-of-band authentication.

## 5. Configuration

**None** in `pkg/crypto/shamir` — no keys, no environment variables, no files.
`threshold` and `total` are call arguments.

`seal.ShamirConfig` is the configuration *shape* for implementations above:

| Field | Type | Default | Meaning |
|---|---|---|---|
| `Threshold` | `int` | none — required | M of the M-of-N scheme |
| `TotalShares` | `int` | none — required | N of the M-of-N scheme |
| `QuorumTTL` | `time.Duration` | `DefaultQuorumTTL` (5 minutes) when zero | how long an accumulator session may collect shares |

The defaulting of `QuorumTTL` is a documented contract for implementers; this
repository ships no code that applies it.

## 6. Persistence

**Nothing in this repository persists a share.** No WAL record, no snapshot
section, no config file, no log line. `Share` carries JSON tags and a
`MarshalJSON` because the custody layer above serializes shares — that
serialization is go-qrdb's, written to go-qrdb's store.

`MarshalJSON` is deliberately **not redacted**: it is the serialization path,
and a custody layer that persists or distributes shares as JSON needs the real
value back. Redaction lives on the human-facing paths instead (§7).

## 7. Observability

**No logging, no metrics, no audit records** are emitted by this package. What
it does instead is make a share safe to *reach* a logger:

| Method | Behavior | Why |
|---|---|---|
| `String()` | never includes `Value` — not even a prefix | `String` is what reaches `%v`/`%s` and logs; a share prefix still leaks share bytes |
| `LogValue()` | `slog.Value` with the value redacted | `String` alone was not enough: `slog` resolves a `LogValuer` **first**, but with none present `slog.JSONHandler` encodes through `json.Marshaler` — so logging a `*Share` through a JSON handler wrote the full share material and never called `String` at all |
| `MarshalJSON()` | full value | serialization path; see §6 |

A single leaked share is a permanent, unrevocable disclosure, which is why the
redaction has to hold on **every** handler rather than on the one that happens
to use `fmt`.

Metrics for quorum progress belong to the accumulator implementation above;
`QuorumProgress` is the shape it reports.

## 8. Test strategy

Unit only. Target: `make test-crypto` (the package is under `pkg/crypto/...`).

| File | Covers |
|---|---|
| `shamir_test.go` | split/combine round trips, every bound in §3, error paths, nil shares, duplicate x, mismatched sets, the bytes API and its wiping |
| `gf256_test.go` | the field arithmetic against known values |
| `zerox_test.go` | rejection of a zero x-coordinate — the share that would leak the secret directly |
| `hardening_round14_test.go` | the hardening findings, kept as regression tests |

**What a test here must always assert**: that `Combine` on a bad-but-structural
share set returns `(garbage, nil)` rather than an error. That is the documented
contract, and a test that expects an error would encode the removed digest
design.

**Fault-injection seams**: none, deliberately. The only external dependency is
`crypto/rand`, and the RNG-failure path is exercised by parameter validation
ordering rather than by injecting a failing reader.

**Fixtures**: shares are generated per test rather than checked in. A checked-in
share is a real share of a real secret and does not belong in a repository.
