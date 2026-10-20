# Shamir primitive: API

> **Status: NORMATIVE** · Scope: go-quicraft `pkg/crypto/shamir`, plus the
> share-custody interfaces in `pkg/seal` · Last verified: 2026-10-13
> Conflicts with this document are defects. Fix them here first.

## Transport coverage

go-quicraft is a library, and the Go API is the only API.

| Transport | This feature |
|---|---|
| Embedded (Go call) | The whole surface below. |
| Unix socket | n/a: this repository serves no socket |
| REST | n/a: no HTTP handler |
| gRPC | n/a: no gRPC service |
| QUIC | n/a. The QUIC listener carries Raft messages only (see [`../quic-transport/api.md`](../quic-transport/api.md)); no share ever crosses it. |
| MCP | n/a: no MCP tools |

These rows are not gaps. This repository owns the primitive; the custody
capability, which is what a remote transport would carry, is owned by go-qrdb and
exposed on go-qrdb's transports. A share-bearing endpoint here would be a defect.

## Go API: the primitive

Import: `github.com/jeremyhahn/go-quicraft/pkg/crypto/shamir`

### Split a secret into N shares (protected form, preferred)

```go
func SplitProtected(secret []byte, threshold, total int) ([]*ProtectedShare, error)
```

| | |
|---|---|
| Request | `secret`: 1..`MaxSecretBytes` (65536) bytes; `threshold`: 2..255; `total`: `threshold`..255 |
| Response | `[]*ProtectedShare` in `Index` order. Each share's value lives in a frozen `enclave.ByteAccess`: mlock'd, guard-paged, and wiped by `Destroy`. |
| Errors | `*SplitError` on any bound violation or RNG failure, including a whole-set footprint above `MaxShareSetBytes`; `*ProtectedMemoryError` when a lock is refused |
| Permission | none; in-process call |
| Caller obligation | `Destroy` every share, ideally as each is consumed rather than at the end of the ceremony |

Native, not a wrapper: coefficients are read into locked memory and each share
value is encoded directly into the locked buffer that share will own, then
frozen, so no secret-derived byte reaches an ordinary heap buffer.

### Split a secret into N shares (bytes form)

```go
func SplitBytes(secret []byte, threshold, total int) ([][]byte, error)
```

| | |
|---|---|
| Request | `secret`: 1..`MaxSecretBytes` (65536) bytes; `threshold`: 2..255; `total`: `threshold`..255 |
| Response | `[][]byte` in `Index` order: `values[i]` is the value of share `i+1`. Each is the base64 encoding of `[version:1][x:1][y...]`, byte-for-byte what a `ProtectedShare` holds and what its JSON `"value"` carries, not the pre-encoding payload. |
| Errors | `*SplitError` on any bound violation or RNG failure |
| Permission | none; in-process call |
| Caller obligation | wipe the returned buffers once distributed |

### Reconstruct a secret from M shares (protected form, preferred)

```go
func CombineProtected(shares []*ProtectedShare) (enclave.ByteAccess, error)
```

| | |
|---|---|
| Request | `shares`: at least `threshold` shares of one split; `Threshold` is taken from `shares[0]` and is unauthenticated |
| Response | the reconstructed secret in a frozen `enclave.ByteAccess` |
| Errors | `*CombineError` on any bound violation, duplicate/zero x-coordinate, ragged share set, or a share destroyed mid-reconstruction; `*ProtectedMemoryError` when a lock is refused |
| Permission | none; in-process call |
| Caller obligation | `Destroy` the returned enclave, and the input shares |

Holds only one decoded share at a time, walking the set twice against a single
reused buffer, so peak locked memory depends on the share size, not on how many
shares were submitted.

### Decode a share set that arrived from outside the process

```go
func UnmarshalShareSet(data []byte) ([]*ProtectedShare, error)
func NewProtectedShare(index, threshold, total int, value []byte) (*ProtectedShare, error)
```

Use `UnmarshalShareSet`, never `json.Unmarshal` into a `[]*ProtectedShare`.
Shares are unauthenticated by construction, so a recovery endpoint decodes
whatever it is handed. The generic decoder allocates one locked buffer per
element as it visits them, which lets the submission rather than the service
choose how much locked memory to take, exhausting a process-wide resource every
other caller depends on. `UnmarshalShareSet` bounds the element count (255) and
the page-rounded locked cost of the share values (`MaxShareSetBytes`) before
allocating, and destroys the accepted prefix if a later element is rejected. A
share whose locked memory is refused fails the set with a `*ProtectedMemoryError`
naming the element, not a `*ValidationError`: the document was well formed, the
host is out of locked memory. A document that is not a JSON array is returned as
`encoding/json`'s own error, unwrapped. The caller's `data` buffer still holds share
material afterwards and is the caller's to wipe. A consumer that bounds its
locked memory with an allocator uses `UnmarshalShareSetWith` (see "Charge share
memory to a locked-memory budget").

`ProtectedShare.UnmarshalJSON` decodes one share and is unbudgeted: it
implements `json.Unmarshaler`, whose signature has no room for an allocator.
Under a budget, decode one share as a one-element set with
`UnmarshalShareSetWith` (wrap the object in `[ ]`), or build it with
`NewProtectedShareWith` when the value bytes are already in hand.

`NewProtectedShare` builds one share at a time, and whether it wipes the source
buffer follows the error type exactly:

- **`*ValidationError`** (bad header, empty value, non-standard base64, oversized
  value): the source is left untouched and the caller must wipe it. Every one of
  these checks runs before any locked memory is allocated, which is also why an
  unauthenticated submission cannot size that allocation.
- **Success, or `*ProtectedMemoryError`**: the source is wiped. On success,
  ownership of the share material transfers to the returned share; on a refused
  lock the value is gone anyway, because the locked-memory constructor wipes its
  source before reporting the refusal. Do not retry that error with the same
  buffer.

That split is the reason the two error types exist (see "Error vocabulary"). A
ceremony that legitimately needs more than the budget does its own accounting
and decodes share by share with `NewProtectedShare`.

### Charge share memory to a locked-memory budget

```go
func SplitProtectedWith(a enclave.Allocator, secret []byte, threshold, total int) ([]*ProtectedShare, error)
func CombineProtectedWith(a enclave.Allocator, shares []*ProtectedShare) (enclave.ByteAccess, error)
func NewProtectedShareWith(a enclave.Allocator, index, threshold, total int, value []byte) (*ProtectedShare, error)
func UnmarshalShareSetWith(a enclave.Allocator, data []byte) ([]*ProtectedShare, error)
```

| | |
|---|---|
| Request | as the allocator-less form, plus `a`, a go-enclave `*Allocator`, normally `enclave.ProcessAllocator()` |
| Response | as the allocator-less form; every locked region the call makes is charged to `a` until it is `Destroy`ed |
| Errors | as the allocator-less form, plus `enclave.ErrNilAllocator` for a nil `a` (decided before the input is read: nothing allocated, `value`/`data` untouched) and a `*ProtectedMemoryError` matching `enclave.ErrBudgetExhausted` under `errors.Is` when an allocation would exceed `a`'s budget |
| Permission | none; in-process call |
| Caller obligation | as the allocator-less form |

The allocator-less `SplitProtected`, `CombineProtected`, `NewProtectedShare`,
`UnmarshalShareSet` and `ProtectedShare.UnmarshalJSON` lock
memory through go-enclave's unbudgeted constructors, which no `Allocator` sees,
so a consumer that bounds its locked memory with a process allocator would have
every share escape that bound. The `...With` forms charge every region they lock
to the allocator they are handed: for a split, the coefficient pool, the
coefficient vector, the payload scratch and each share's value; for a combine,
the decode buffer and the returned secret; for a set decode, each share's
value. Both forms share one implementation. `UnmarshalJSON` alone has no
`...With` form, because `json.Unmarshaler` fixes its signature; see "Decode a
share set" for the budgeted route.

go-enclave's own unbudgeted constructors do not consult
`enclave.ProcessAllocator()`, so neither does this package implicitly: the
budget is opted into by passing it, and a nil allocator is refused rather than
treated as "unbudgeted", exactly as `Sealed.OpenIn` refuses a typed-nil `*Allocator`.

A budget refusal is recoverable and local to the call:

- It is decided before the refused region's locked memory is requested.
- Everything the call had already allocated is destroyed before it returns, so
  a split refused partway through its share loop, or a set decode refused
  partway through its elements, gives its whole reservation back to `a`. Shares
  returned by earlier calls, and the input shares to a combine, are untouched.
- `NewProtectedShareWith` keeps `NewProtectedShare`'s ownership contract: a
  `*ProtectedMemoryError`, budget refusal included, means `value` has already
  been wiped.

`MaxShareSetBytes` still bounds a single split, and a single set decode, up
front (`UnmarshalShareSetWith` runs the 255-share and `MaxShareSetBytes` checks
before it admits anything). An allocator bounds everything the process holds
across calls, which no per-call figure can.

### Reconstruct a secret from M shares (bytes form)

```go
func CombineBytes(values [][]byte, threshold int) ([]byte, error)
```

| | |
|---|---|
| Request | `values`: M..255 base64-encoded share values (what `SplitBytes` returned); `threshold`: the true M, from a trusted source |
| Response | the reconstructed secret |
| Errors | `*CombineError` for structural violations: `threshold` below 2, no values, too few, too many, a value that is not standard base64, a share too short to carry a header, wrong format version, zero or duplicate x, mismatched payload length, oversized payload |
| Permission | none |
| Caller obligation | authenticate the result out of band; wipe the input buffers |

Every decoded buffer is wiped on every return path, including partially decoded
and validation-rejected shares.

**No combine form verifies the result.** With fewer than threshold genuine shares, a
corrupted share, a share from another split, or a mislabelled `Threshold`, the
return is `(indistinguishable garbage, nil)`. The canonical out-of-band check is
to use the result as a key in an authenticated decrypt that fails on a wrong
key, which is what `Barrier.Unseal` does. `CombineProtected` takes `threshold`
from `shares[0].Threshold`, which is unauthenticated, so the same holds there.

### Cross-check one share against others

```go
func VerifyShare(share *ProtectedShare, otherShares []*ProtectedShare) error
```

| | |
|---|---|
| Request | one share and a set to compare it against |
| Response | `nil` if consistent |
| Errors | `*ValidationError` when `share` itself fails `Validate` (nil, bad header or empty value), for a nil element of `otherShares` (its position named), a `Threshold`/`Total` mismatch, or a duplicate `Index` |
| Permission | none |

This checks metadata consistency, not cryptographic validity. It cannot tell you
a share is genuine.

### Inspect and serialize a share

```go
type ProtectedShare struct {
    Index     int               // JSON "index"
    Threshold int               // JSON "threshold"
    Total     int               // JSON "total"
    Metadata  map[string]string // JSON "metadata", omitted when empty
    // value: unexported, base64 [version][x][y...] in locked memory; JSON "value"
}

func (s *ProtectedShare) WithValue(fn func([]byte) error) error // borrow the value
func (s *ProtectedShare) Destroy()                              // wipe and unlock
func (s *ProtectedShare) Validate() error                       // nil-receiver safe
func (s *ProtectedShare) String() string                        // value REDACTED
func (s *ProtectedShare) LogValue() slog.Value                  // value REDACTED
func (s *ProtectedShare) MarshalJSON() ([]byte, error)          // value INCLUDED
func (s *ProtectedShare) UnmarshalJSON([]byte) error            // unbudgeted
```

`Threshold` and `Total` are unauthenticated caller metadata. `Metadata` carries
no cryptographic meaning.

The redaction asymmetry is deliberate: `String` and `LogValue` reach humans and
logs, `MarshalJSON` reaches storage and distribution.
See [`engineering.md`](engineering.md) §7.

### Error vocabulary

```go
type SplitError           struct{ Reason string }
type CombineError         struct{ Reason string }
type ValidationError      struct{ Field, Reason string }
type ProtectedMemoryError struct{ Op string; Err error }  // Unwrap() error
```

Match with `errors.As`. There is no integrity error: shares carry no digest, so
a wrong reconstruction is never reported as one.

**`ProtectedMemoryError` versus `ValidationError` is an ownership contract**, and
`NewProtectedShare` is where it matters. A `*ValidationError` means the value
never reached protected memory and is left untouched: every syntactic check runs
before any locked memory is allocated. A `*ProtectedMemoryError` means the lock
was refused, and by then the value has already been wiped, because go-enclave's
constructor wipes its source before it reports a refusal. Do not retry with the
same buffer; it is zeroes. It is also the error an operator can act on: the host
or container is out of locked memory, the input was fine. It unwraps to the
allocator's own error, so `errors.Is` reaches go-enclave's sentinels:
`enclave.ErrLockedMemoryUnavailable` for a kernel refusal and
`enclave.ErrBudgetExhausted` when a `...With` call's allocator refused the
admission.

### Constants

```go
const MaxSecretBytes = 64 << 10  // 65536: one secret
var MaxShareSetBytes             // 512 pages: whole-set locked-memory budget
```

`MaxShareSetBytes` is a `var`, not a `const`, because its byte value depends on
the host: 512 pages, derived from the page size at init. On a 4 KiB-page host
that is 2 MiB; on the 64 KiB-page arm64 kernels RHEL and CentOS Stream ship,
32 MiB. It cannot appear in a constant expression.

The share-set maximum is 255, from the GF(2^8) x-coordinate space.

`MaxShareSetBytes` exists because individually legal arguments multiply into a
footprint no host can lock: a 64 KiB secret split 255 ways needs about 16 MiB of
coefficients alone.

It is counted in the pages the kernel charges, not the bytes a caller asks for.
Every region is its own enclave and a 32-byte enclave locks a full page, so a
figure summed from requested sizes cannot be compared to `RLIMIT_MEMLOCK` at
all. A full 255-share ceremony over a 32-byte root key costs 259 pages, which is
why the budget is 512 pages rather than 256. 512 pages admits every split whose
encoded share fits one page, meaning secrets up to 3,070 bytes at the full 255
ways; sharing a 4 KiB payload 255 ways does not fit and should be enveloped under
a key that does.

## Go API: the custody interfaces

Import: `github.com/jeremyhahn/go-quicraft/pkg/seal`

Declared here, implemented above. This repository ships no implementation of
either interface and never calls them.

```go
type ProtectedShareSplitter interface {
    SplitProtected(secret []byte, threshold, total int) ([]*shamir.ProtectedShare, error)
}

type ProtectedShareAccumulator interface {
    AddProtectedShare(ctx context.Context, share *shamir.ProtectedShare) (*QuorumProgress, error)
    GetProgress() *QuorumProgress
    ReconstructProtected(ctx context.Context) (enclave.ByteAccess, error)
    Reset()
    Expired() bool
}
```

These are the only custody interfaces; there is no string-typed sibling. A Go
string cannot be zeroed, so a string-valued splitter would force an
implementation that split into locked memory to copy every share back out into an
immortal heap string, and a string-valued accumulator would receive a share that
was un-wipeable before any protected buffer existed. A consumer could then adopt
the protected primitives and leave the exposure exactly where it was.

The protected forms close both ends: `AddProtectedShare` takes ownership of the
share it is given (and destroys it whether or not it is accepted, and whether or
not the quorum completes), and `ReconstructProtected` returns the secret in an
enclave the caller owns and must `Destroy`.

| Type | Role |
|---|---|
| `ShamirConfig{Threshold, TotalShares, QuorumTTL}` | configuration shape for an M-of-N setup; implementations treat a zero `QuorumTTL` as `DefaultQuorumTTL` (5 minutes). This repository applies no default itself. |
| `ShamirInitResult{Shares, Threshold, TotalShares}` | output of a Shamir-based initialization. `Shares` is `[]*shamir.ProtectedShare`; the result owns them and `Destroy()` releases them all |
| `QuorumProgress{Required, Submitted, Complete}` | accumulator state |

`ShamirInitResult` is where a ceremony holds all N shares at once. Its JSON
`"shares"` is an array of share objects, the same encoding
`shamir.ProtectedShare` produces. Its `UnmarshalJSON` decodes them through
`shamir.UnmarshalShareSet`, so it inherits the 255-share and `MaxShareSetBytes`
bounds. It is not charged to an allocator (`json.Unmarshaler` fixes its
signature); a caller under a budget decodes the `"shares"` member itself, as a
`json.RawMessage`, through `shamir.UnmarshalShareSetWith`. A rejected document
leaves the receiver unchanged, and a reused receiver's old shares are destroyed
before the new set replaces them.

Implementer obligations: `ProtectedShareAccumulator` handles deduplication,
ordering and TTL expiration, and is not required to be safe for concurrent use.
The caller serializes access; nothing in this repository calls it.

## Go API: the protected sealing pair

`SealingStrategy` keeps its `[]byte` root-key methods. Alongside them, a strategy
may implement the protected pair:

```go
type ProtectedSealingStrategy interface {
    SealingStrategy

    SealRootKeyProtected(ctx context.Context, rootKey enclave.ByteAccess, creds Credentials) (*SealedRootKey, error)
    UnsealRootKeyProtected(ctx context.Context, sealed *SealedRootKey, creds Credentials) (enclave.ByteAccess, error)
}

// The passphrase is protected too: the same Credentials every strategy takes.
// Built by NewCredentials, which wipes the caller's slice; the caller owns it
// and releases it with Secret.Destroy.
type Credentials struct {
    Secret enclave.Sealed
}

```

It exists because the `[]byte` signatures force an implementation to retain the
root key in ordinary memory: a recovery strategy that reconstructs the key into
an enclave would have to copy it into a plain field solely to be able to return
it. Sealing borrows the caller's enclave and never copies the key out of it;
unsealing decrypts straight into a fresh enclave the caller owns and must
`Destroy`.

Optional by design: a caller type-asserts for it, and there is no fallback,
because a fallback is the unprotected copy wearing the protected API's name.
`SoftwareStrategy` implements both; `NoneStrategy` implements
`SealRootKeyProtected` and answers `UnsealRootKeyProtected` with
`ErrNoRootKeyAvailable`, since no-encryption mode has no root key and an enclave
cannot hold nothing.

## Versioning and deprecation

- **Share format**: the `version` byte is `0x01`, gated on exact equality. Until
  the first production release a layout change is made in place at version 1 and
  consumers are updated in the same change (see
  [`../wire-protocol/architecture.md`](../wire-protocol/architecture.md)).
- **No string-custody form.** The deprecated struct-and-string share API was
  removed before v1.0.0; `CHANGELOG.md` lists each removed name with its
  replacement. The share JSON encoding is unchanged. `seal.ProtectedCredentials` and `seal.NewProtectedCredentials`
  were removed in 0.17.0; use `seal.Credentials` and `seal.NewCredentials`.
- A signature change to `ProtectedShareSplitter`, `ProtectedShareAccumulator` or
  `ProtectedSealingStrategy` breaks go-qrdb's implementations. Sequence such
  changes bottom-up.
