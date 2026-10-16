# Shamir primitive: engineering contract

> **Status: NORMATIVE** · Scope: go-quicraft `pkg/crypto/shamir`, plus the
> share-custody interfaces in `pkg/seal` · Last verified: 2026-10-13
> Conflicts with this document are defects. Fix them here first.

Import paths:
`github.com/jeremyhahn/go-quicraft/pkg/crypto/shamir` (the primitive)
`github.com/jeremyhahn/go-quicraft/pkg/seal` (the custody interfaces)

## 1. Exported surface

### `pkg/crypto/shamir`

```go
const MaxSecretBytes = 64 << 10                 // 65536
var MaxShareSetBytes = 512 * <page size>        // whole-set locked-memory budget

// Split / combine: struct form (DEPRECATED: string custody)
func Split(secret []byte, threshold, total int) ([]*Share, error)
func Combine(shares []*Share) ([]byte, error)
func CombineUnverified(shares []*Share) ([]byte, error)

// Split / combine: protected-memory form
func SplitProtected(secret []byte, threshold, total int) ([]*ProtectedShare, error)
func CombineProtected(shares []*ProtectedShare) (enclave.ByteAccess, error)
func NewProtectedShare(index, threshold, total int, value []byte) (*ProtectedShare, error)
func UnmarshalShareSet(data []byte) ([]*ProtectedShare, error)

// The same, with every locked region charged to a go-enclave Allocator;
// a nil allocator is refused with enclave.ErrNilAllocator
func SplitProtectedWith(a enclave.Allocator, secret []byte, threshold, total int) ([]*ProtectedShare, error)
func CombineProtectedWith(a enclave.Allocator, shares []*ProtectedShare) (enclave.ByteAccess, error)
func NewProtectedShareWith(a enclave.Allocator, index, threshold, total int, value []byte) (*ProtectedShare, error)
func UnmarshalShareSetWith(a enclave.Allocator, data []byte) ([]*ProtectedShare, error)

// Split / combine: wipeable bytes form
func SplitBytes(secret []byte, threshold, total int) ([][]byte, error)
func CombineBytes(values [][]byte, threshold int) ([]byte, error)

// Cross-checking
func VerifyShare(share *Share, otherShares []*Share) error

type Share struct {
    Index     int               `json:"index"`
    Threshold int               `json:"threshold"`
    Total     int               `json:"total"`
    Value     string            `json:"value"`   // DEPRECATED
    Metadata  map[string]string `json:"metadata,omitempty"`
}

func (s *Share) Bytes() ([]byte, error)
func (s *Share) Protect() (*ProtectedShare, error)
func (s *Share) ProtectWith(a enclave.Allocator) (*ProtectedShare, error)
func (s *Share) Validate() error
func (s *Share) String() string
func (s *Share) LogValue() slog.Value
func (s *Share) MarshalJSON() ([]byte, error)
func (s *Share) UnmarshalJSON(data []byte) error

type ProtectedShare struct {
    Index     int
    Threshold int
    Total     int
    Metadata  map[string]string   // NOT protected memory: no secrets here
    // value is unexported: it lives in an enclave.ByteAccess
}

func (s *ProtectedShare) WithValue(fn func([]byte) error) error
func (s *ProtectedShare) Destroy()
func (s *ProtectedShare) Validate() error
func (s *ProtectedShare) String() string
func (s *ProtectedShare) LogValue() slog.Value
func (s *ProtectedShare) MarshalJSON() ([]byte, error)
func (s *ProtectedShare) UnmarshalJSON(data []byte) error

// Errors
type SplitError           struct{ Reason string }
type CombineError         struct{ Reason string }
type ValidationError      struct{ Field, Reason string }
type ProtectedMemoryError struct{ Op string; Err error }  // Unwrap() error
type IntegrityError       struct{}   // DEPRECATED: never returned
```

Every locked-memory refusal in this package reports as `*ProtectedMemoryError`.
It is distinct from `*ValidationError` because the two say different things about
who owns the caller's buffer afterwards (see `api.md`, "Error vocabulary").
`SplitError` and `CombineError` cover logic failures, including the
`MaxShareSetBytes` budget refusal, which is a rejected request rather than an
exhausted host.

`Combine` and `CombineUnverified` are identical and both deprecated; neither
verifies anything.

### Share values in protected memory

`Share.Value` is a Go `string`, and a string cannot be zeroed: every share stays
readable in the heap until the collector reclaims it, and a ceremony holding all
N shares at once is the widest exposure in the scheme. That field, and the
`Split` / `Combine` / `CombineUnverified` functions that hand out and consume it,
are deprecated.

The replacement is `ProtectedShare`, whose value lives in a frozen
`enclave.ByteAccess`: mlock'd, guard-paged, canary-checked, and wiped by
`Destroy`.

| Deprecated (string custody) | Protected replacement |
|---|---|
| `Split` | `SplitProtected` |
| `Combine`, `CombineUnverified` | `CombineProtected` (returns the secret in an `enclave.ByteAccess`) |
| `&Share{…, Value: v}` | `NewProtectedShare(index, threshold, total, value)`: wipes the source on success and on `*ProtectedMemoryError`; leaves it untouched on `*ValidationError` |
| an existing `*Share` | `(*Share).Protect()`, or `(*Share).ProtectWith(a)` under a locked-memory budget |

Both protected paths are native, not wrappers. `SplitProtected` reads the
polynomial coefficients into locked memory and encodes each share value directly
into a sized enclave that share will own, then freezes it. `CombineProtected`
decodes each share value in place and interpolates directly into the locked
buffer it returns, then freezes that. Neither routes through
`SplitBytes`/`CombineBytes`, which return ordinary heap buffers by contract, so no
secret-derived byte is ever written to unprotected memory, not even to one wiped
afterwards. The share x-coordinates and the Lagrange basis derived from them stay
on the ordinary heap: an x-coordinate is a published share index, not secret
material.

`CombineProtected` holds only one decoded share at a time, walking the set twice
against a single reused buffer, so the peak locked memory of a reconstruction
depends on the share size, not on how many shares were submitted.

`SplitBytes` and `CombineBytes` remain the right path for a caller that manages
its own wipeable buffers; `ProtectedShare` is for the caller that wants the
buffer managed for it.

The JSON a `ProtectedShare` marshals is byte-for-byte what a `Share` marshals, so
the two are interchangeable on the wire and a consumer can migrate one call site
at a time.

### `pkg/seal`: custody interfaces declared, not implemented

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

type ShamirConfig struct {
    Threshold   int           `json:"threshold"`
    TotalShares int           `json:"total_shares"`
    QuorumTTL   time.Duration `json:"quorum_ttl,omitempty"`
}

type ShamirInitResult struct {
    Shares      []*shamir.ProtectedShare `json:"shares"`
    Threshold   int                      `json:"threshold"`
    TotalShares int                      `json:"total_shares"`
}

// Destroy wipes and releases every share the result holds. Safe on a nil
// receiver and safe to call twice, so a ceremony can defer it from the line
// the result exists.
func (r *ShamirInitResult) Destroy()

type QuorumProgress struct {
    Required  int  `json:"required"`
    Submitted int  `json:"submitted"`
    Complete  bool `json:"complete"`
}

const DefaultQuorumTTL = 5 * time.Minute
```

Nothing in this module implements or calls these interfaces.
`ProtectedShareAccumulator` implementations handle deduplication, ordering and
TTL expiration, and are not required to be safe for concurrent use; the caller
provides that synchronization.

There is no string-typed custody surface. A string-typed splitter would force an
implementation that split into locked memory to copy every share back out into an
immortal heap string to satisfy the signature, and a string-typed accumulator
would receive a share already un-wipeable before any protected buffer existed. A
consumer could then migrate to `SplitProtected`/`CombineProtected`, silence every
`SA1019`, and leave the exposure exactly where it was.

Both ends of the protected family are protected deliberately: `AddProtectedShare`
takes ownership of a share already in locked memory (destroying it whether or not
it is accepted and whether or not the quorum completes), and
`ReconstructProtected` returns the secret in an `enclave.ByteAccess` rather
than an ordinary `[]byte`. Protecting one end only would leave the exposure
intact at the other.

`ShamirInitResult` is the moment a ceremony holds all N shares at once, and it
owns them: `Destroy()` releases every one, and an abandoned result holds N locked
allocations against `RLIMIT_MEMLOCK` for the life of the process. Its
`"shares"` JSON is an array of share objects, the encoding every other share
surface uses, and `ShamirInitResult.UnmarshalJSON` decodes it through
`shamir.UnmarshalShareSet`, so it is bounded like any other ingestion (255 shares,
`MaxShareSetBytes`). It is not charged to an allocator, because
`json.Unmarshaler` fixes its signature; a budgeted caller decodes the `"shares"`
member itself through `shamir.UnmarshalShareSetWith`.

### The protected sealing pair

`SealingStrategy.SealRootKey`/`UnsealRootKey` take and return `[]byte`. An
optional sibling interface sits beside them:

```go
type ProtectedSealingStrategy interface {
    SealingStrategy

    SealRootKeyProtected(ctx context.Context, rootKey enclave.ByteAccess, creds Credentials) (*SealedRootKey, error)
    UnsealRootKeyProtected(ctx context.Context, sealed *SealedRootKey, creds Credentials) (enclave.ByteAccess, error)
}

type Credentials struct {
    Secret enclave.Sealed
}

func NewCredentials(ctx context.Context, secret []byte) (Credentials, error) // moves and WIPES secret

```

It answers a defect no implementer can avoid while the signature demands a
`[]byte`: a recovery strategy that reconstructs the root key into an enclave must
copy it into a plain `[]byte` field purely to have something to return from
`UnsealRootKey`, so the most sensitive value in the system sits in collectable,
swappable memory for the whole unseal. The fix belongs at the layer that owns the
signature.

Implementation notes not obvious from the signatures:

- **Order of operations.** `SoftwareStrategy` derives the wrapping key before it
  borrows the root-key enclave. Nesting the two is legal, but the order keeps the
  key-touching window as short as possible, and the AEAD it produces holds only
  an expanded key schedule in ordinary memory, which a borrow may touch freely.
- **The plaintext never exists on the heap.** GCM's plaintext length is known
  before decryption (payload minus nonce minus tag), so the enclave is allocated
  at exactly that size and `aead.Open` is handed the enclave's own storage as its
  destination. The result is frozen before it is returned.
- **That length is refused unless it is the root-key size, before anything is
  locked.** It is derived from a blob the AEAD has not yet authenticated, and
  locked memory is a finite process-wide resource. Sizing an `mlock`'d allocation
  from it would let whoever can write the blob exhaust that resource for every
  other caller before the tag that rejects it is checked. The refusal is
  `*UnsealError` wrapping `ErrInvalidSealedKey`.

`NoneStrategy` implements the pair too. `UnsealRootKeyProtected` returns
`ErrNoRootKeyAvailable`: no-encryption mode has no root key, a zero-length
allocation is refused (`ErrEmptyEnclave`), and an enclave "holding" an empty key
would be a protected wrapper around a fiction. Its `SealRootKeyProtected` checks
the enclave's size even though it never reads the bytes, because a contract the
reference implementation ignores is not a contract.

Both sides of the boundary are protected. For a deferred-passphrase strategy the
passphrase is that key's protection, not a lesser secret, so accepting it in a
plain `[]byte` would protect the wrong half. `Credentials.Secret` is a
`enclave.Sealed` for every strategy method.

Every root-key method derives straight from the credential's sealed passphrase
rather than building a temporary inner strategy from a plain slice, so a
FIPS-unapproved KDF is refused at `DeriveKey`.

The `Barrier` calls the `[]byte` root-key pair. It needs raw bytes for HKDF
epoch-DEK derivation and the sealed-state HMAC, holds them in a locked enclave for
the whole of `Initialize`/`Unseal`, and then seals them into a `Sealed`.
That root-key enclave is deliberately left writable so `enclave.NewSealed` can
consume it; a wipe of frozen pages faults the process.

## 2. Wire format of a share

A share value is the base64 encoding of:

```
[version:1][x:1][y:len(secret)]
```

| Field | Value |
|---|---|
| `version` | `0x01` (`shareFormatVersion`). Decoders gate on exact equality; anything else is rejected as "legacy or foreign". |
| `x` | the field coordinate, `1..255`, equal to the share's `Index`. Never `0`. |
| `y` | one byte per secret byte |

Overhead is 2 bytes (`rawShareOverhead`). The `x` in the value, not `Index`, is
what reconstruction uses, so a share string is self-contained.

`parseRawShare` is the single copy of these rules, shared by every
reconstruction path, so they can never diverge on which shares they accept.

## 3. Invariants

**Correctness**

- Any `threshold` genuine shares of one split reconstruct the secret exactly.
- Any `threshold - 1` shares reveal nothing about the secret, information
  theoretically, not just computationally.
- Splitting is randomized: two splits of the same secret share no values.
- Every split form returns shares in `Index` order: `shares[i].Index == i+1`, and
  `SplitBytes` returns `values[i]` as the value of share `i+1`.

**Bounds, enforced at every entry point**

| Rule | Enforced in | Error |
|---|---|---|
| `threshold >= 2` | `Split`, `SplitProtected`, `CombineBytes`, `Share.Validate`, `ProtectedShare.Validate` | `SplitError` / `CombineError` / `ValidationError` |
| `total >= threshold` | `Split`, `SplitProtected`, `Share.Validate`, `ProtectedShare.Validate` | as above |
| `threshold <= 255`, `total <= 255` | `Split`, `SplitProtected` | `SplitError` |
| `len(secret) > 0` | `Split`, `SplitProtected` | `SplitError` |
| `len(secret) <= MaxSecretBytes` | `Split`, `SplitProtected` | `SplitError` |
| page-rounded whole-set footprint `<= MaxShareSetBytes` | `SplitProtected` | `SplitError` |
| page-rounded sum of share-value lengths `<= MaxShareSetBytes` | `UnmarshalShareSet`, `UnmarshalShareSetWith` | `ValidationError` |
| `len(shares) <= 255` | `UnmarshalShareSet`, `UnmarshalShareSetWith` | `ValidationError` |
| `len(shares) <= 255` (`maxShares`) | `Combine`, `CombineBytes`, `CombineProtected` | `CombineError` |
| payload `<= MaxSecretBytes` | `parseRawShare` | `CombineError` |
| `x != 0` | `parseRawShare` | `CombineError` |
| no duplicate `x` | `parseRawShare` | `CombineError` |
| all `y` the same length as share 0's | `parseRawShare` | `CombineError` |
| all `Threshold`/`Total` equal to share 0's | `Combine`, `CombineProtected` | `CombineError` |

The payload bound is checked on the first share, before the rest of the set is
decoded: shares at a recovery ceremony are unauthenticated by construction, so an
oversized submission must be rejected immediately rather than after 255 of them
have been decoded and copied.

**Nil safety**

- `Share.Validate()` and `ProtectedShare.Validate()` are safe on a `nil` receiver
  and report `ValidationError{Field: "share", Reason: "share is nil"}`. A JSON
  `null` in a submitted share list decodes to a `nil` share, and operator input
  must not panic the process.
- A `nil` element of `shares` in a combine, or of `otherShares` in `VerifyShare`,
  is reported as an error naming its position and is never dereferenced.

**Memory**

- `SplitBytes` returns buffers the caller owns and must wipe.
- `Split` wipes the byte buffer it encoded from; the returned `Share.Value`
  strings cannot be wiped and persist until GC.
- `CombineBytes` wipes every decoded buffer on every return path, including
  partially decoded and validation-rejected shares. The caller still owns and
  must wipe the input buffers.

**Protected-memory obligations**

Three obligations, and one limit, come with `ProtectedShare`:

- **Every `ProtectedShare` must be `Destroy`ed, including one produced by
  `json.Unmarshal`**: decoding is a construction. An abandoned share holds a
  locked allocation for the life of the process.
- **Locked memory is a finite kernel resource** (`RLIMIT_MEMLOCK`), and each
  share is one locked allocation. Destroy shares as they are consumed rather than
  at the end of a ceremony.
- **`Destroy` does not report a refused release.** go-enclave's
  `Memory.Destroy` returns an error, and `ProtectedShare.Destroy` returns
  nothing: a release the kernel refuses stays on go-enclave's `WipeAll` ledger
  for a later retry, and a release that ran without a wipe (a denied
  `mprotect`) has already returned the pages, which the kernel zeroes before
  reuse. The package's internal cleanup paths drop the report the same way,
  through one helper (`release`) whose comment records why nothing is lost.
- **A share's value is read only inside `WithValue`.** The view it passes is
  valid for the callback alone: it holds the share open for the whole callback,
  so a concurrent `Destroy` blocks until it returns, and `Destroy` unmaps the
  pages, so a view kept past the callback faults when read. Scope each borrow to
  the code that uses the value; the callback must not call methods on the same
  share. There is no accessor that returns the slice.

Because of the second obligation, the two calls that allocate a whole set price
it first.

`SplitProtected` rejects parameters whose peak footprint exceeds
`MaxShareSetBytes` (512 pages), since individually legal arguments multiply into
one no host can lock (a 64 KiB secret split 255 ways needs about 16 MiB of
coefficients alone). `splitLockedBytes` prices every region live at the peak: the
random coefficients, the per-share coefficient vector, the reused raw payload,
and one frozen value per returned share. There is no separate encoding scratch to
price, because each share is encoded directly into its own enclave and frozen in
place.

Every term is priced in whole pages, through go-enclave's `LockedSize`. That is
the unit the kernel charges: each region is its own enclave and a 32-byte enclave
locks a full page, so a figure summed from requested sizes cannot be compared to
`RLIMIT_MEMLOCK`, the only comparison the budget exists to make. The byte figures
in the rest of this section are for a 4 KiB-page host. A byte-summed price of a
255-of-255 split of a one-byte secret is 1,532 bytes; the measured `VmLck` delta
is 1,044,480, with the per-share rounding accounting for nearly all of it.

The budget is sized for the regime the primitive serves. A full 255-share
ceremony over a 32-byte root key costs 259 pages (1,060,864 bytes), so a
256-page cap would refuse the operation the package exists for. 512 pages admits
every split whose encoded share fits one page: secrets up to 3,070 bytes at the
full 255 ways, peaking at 1,835,008. One byte past that, every share needs a
second page and the footprint jumps to 2,879,488, which is refused, as is sharing
a 4 KiB payload 255 ways (3,141,632). Envelope large payloads under a key that
fits. Against the 8 MiB `RLIMIT_MEMLOCK` a Linux host commonly defaults to, one
maximal set is a quarter of the ceiling. That bounds one call and is deliberately
not a concurrency budget.

The concurrency budget is a go-enclave `Allocator`, owned by whoever owns the
process. The `...With` variants of `SplitProtected`, `CombineProtected`,
`NewProtectedShare`, `UnmarshalShareSet` and `Share.Protect` charge every region
they lock to the one they are handed, and a refusal partway through a split or a
set decode destroys everything that call had allocated. The one locking entry
point without a `...With` form is `ProtectedShare.UnmarshalJSON`, because
`json.Unmarshaler` fixes its signature; it is documented as unbudgeted, and the
budgeted route for one share is a one-element `UnmarshalShareSetWith`.

**Shares arriving from outside the process are decoded with
`UnmarshalShareSet`, not `json.Unmarshal` into a `[]*ProtectedShare`.** Shares
are unauthenticated by construction (nothing about one can be checked until the
set is combined), so a recovery endpoint necessarily decodes whatever it is
handed. The generic decoder allocates one locked buffer per element as it visits
them, so the submission, not the service, would choose how much locked memory to
take. `UnmarshalShareSet` bounds the element count (255, the scheme's own limit)
and the page-rounded sum of the share value lengths against `MaxShareSetBytes`
before it allocates any of it, and destroys the accepted prefix if a later
element is rejected.

- **The sum is taken over the elements, not from `len(data)`**, so whitespace
  cannot evade it.
- **Each value is rounded up to a page**, which makes it a bound on the resource:
  one element is one enclave and costs a whole page however small the share. The
  full 255-share set of a 32-byte root key is about 25 KiB of JSON and 1,044,480
  bytes of locked memory; bounding the raw length would price it at a fortieth of
  its cost, on the path whose submitter cannot be authenticated.
- **The value is priced, not the whole element**, which keeps this bound and
  `splitLockedBytes` comparable. An element carries about 48 bytes of JSON header,
  and when the encoded value lands on an exact page multiple that header would
  push each element into a further page: a 171-share set of a 6142-byte secret
  splits at 1,421,312 and, priced per element, would decode at 2,101,248,
  admitted by one guard and refused by the other on a set the library itself
  produced. Priced per value, the decode sum is exactly `splitLockedBytes`'
  `total*lockedBytes(encoded)` term, to which that function adds further positive
  terms, so anything the split admits the ingest path admits by construction.

A ceremony that legitimately needs more than the budget does its own accounting
and decodes share by share with `NewProtectedShare`.

**Constant time**

`gfMul` and the GF(2^8) primitives in `gf256.go` are constant-time with respect
to secret operands: no table lookups indexed by secret data, no branches on
secret data, all data-dependent selection by branchless mask, using the AES
reduction polynomial `x^8 + x^4 + x^3 + x + 1` (0x11B). This is a requirement,
not an optimization detail, because the operands are root-key material. A change
that introduces a lookup table or a data-dependent branch is a defect regardless
of its benchmark result.

**The invariant that is not provided**

Combining performs no self-verification. Fewer than threshold genuine shares, a
corrupted share, a share from a different split, or a mislabelled `Threshold` all
produce indistinguishable garbage and a `nil` error. `Threshold` and `Total` are
unauthenticated caller-supplied metadata.

## 4. Error semantics

| Type | Fields | Raised by |
|---|---|---|
| `*SplitError` | `Reason` | `Split`, `SplitBytes`, `SplitProtected`: parameter and size violations, RNG failure, and (protected only) a whole-set footprint above `MaxShareSetBytes` |
| `*CombineError` | `Reason` | `Combine`, `CombineUnverified`, `CombineBytes`, `CombineProtected`: set-level and per-share structural violations |
| `*ValidationError` | `Field`, `Reason` | `Share.Validate`, `ProtectedShare.Validate`, `VerifyShare`, `NewProtectedShare`, `UnmarshalShareSet`: per-field violations, positionally named |
| `*ProtectedMemoryError` | `Op`, `Err` | every protected path and its `...With` form: a locked allocation the kernel (`enclave.ErrLockedMemoryUnavailable`) or the caller's allocator (`enclave.ErrBudgetExhausted`) refused; `UnmarshalShareSet` names the element in `Op` |
| `*IntegrityError` | none | never. Deprecated; retained so existing callers compile. |

- All five are concrete pointer types; match with `errors.As`.
- `UnmarshalShareSet` returns a document that is not a JSON array as
  `encoding/json`'s own error, unwrapped and outside the five; the bounds and the
  per-element checks run only once the array has decoded.
- Error strings name the offending share by its position in the input slice
  (`"share 3 has zero x-coordinate"`), not by custodian identity; this package has
  no notion of one.
- No error message ever contains share bytes or secret bytes.
- **Fail-closed paths**: every validation in §3 returns before any field
  arithmetic runs. There is no partial reconstruction and no best-effort mode.
  Splitting fails closed on RNG error rather than proceeding with weak
  coefficients.
- **The path that does not fail closed, by design**: a semantically wrong but
  structurally valid share set. It cannot be detected here (see §3) and must be
  caught by the caller's out-of-band authentication.

## 5. Configuration

None in `pkg/crypto/shamir`: no keys, environment variables or files.
`threshold` and `total` are call arguments.

`seal.ShamirConfig` is the configuration shape for implementations above:

| Field | Type | Default | Meaning |
|---|---|---|---|
| `Threshold` | `int` | none; required | M of the M-of-N scheme |
| `TotalShares` | `int` | none; required | N of the M-of-N scheme |
| `QuorumTTL` | `time.Duration` | `DefaultQuorumTTL` (5 minutes) when zero | how long an accumulator session may collect shares |

The defaulting of `QuorumTTL` is a contract for implementers; this repository
ships no code that applies it.

## 6. Persistence

Nothing in this repository persists a share: no WAL record, snapshot section,
config file or log line. `Share` carries JSON tags and a `MarshalJSON` because the
custody layer above serializes shares; that serialization is go-qrdb's, written
to go-qrdb's store.

`MarshalJSON` is deliberately not redacted: it is the serialization path, and a
custody layer that persists or distributes shares as JSON needs the real value
back. Redaction lives on the human-facing paths instead (§7).

## 7. Observability

This package emits no logs, metrics or audit records. It makes a share safe to
reach a logger instead:

| Method | Behavior | Why |
|---|---|---|
| `String()` | never includes the value, not even a prefix | `String` is what reaches `%v`/`%s` and logs; a share prefix still leaks share bytes |
| `LogValue()` | `slog.Value` with the value redacted | `slog` resolves a `LogValuer` first; without one, `slog.JSONHandler` encodes through `json.Marshaler`, which would write the full share and never call `String` |
| `MarshalJSON()` | full value | serialization path; see §6 |

`Share` and `ProtectedShare` behave the same. A single leaked share is a
permanent, unrevocable disclosure, so the redaction has to hold on every handler,
not only the one that happens to use `fmt`.

Metrics for quorum progress belong to the accumulator implementation above;
`QuorumProgress` is the shape it reports.

## 8. Test strategy

Unit only. Target: `make test-crypto` (runs `./pkg/crypto/...`).

| File | Covers |
|---|---|
| `shamir_test.go` | split/combine round trips, every bound in §3, error paths, nil shares, duplicate x, mismatched sets, the bytes API and its wiping |
| `gf256_test.go` | the field arithmetic against known values |
| `zerox_test.go` | rejection of a zero x-coordinate, the share that would leak the secret directly |
| `share_hardening_test.go` | hardening regression tests |
| `protected_share_test.go` | the protected split/combine/decode paths, ownership of the source buffer, redaction |
| `protected_share_allocator_test.go` | the `...With` forms: every region charged, nil allocator refused, refusals give the reservation back |
| `protected_memory_error_test.go` | locked-memory refusals reported as `*ProtectedMemoryError`, partial sets torn down |
| `protected_share_lockedmem_linux_test.go` | `splitLockedBytes` and the ingestion bound against the kernel's `VmLck` accounting |
| `protected_freeze_linux_test.go` | share values and reconstructed secrets are read-only after return |
| `protected_share_race_test.go` | concurrent `WithValue`/`Destroy` |
| `protected_borrow_test.go` | a borrow ends with its callback; split and combine release every scratch they borrow |
| `enclave_testhelp_test.go` | helpers only: copying a share value or an enclave out for comparison, building an enclave from bytes |

**What a test here must always assert**: that combining a bad-but-structural
share set returns `(garbage, nil)` rather than an error. That is the documented
contract, and a test that expects an error would encode the rejected digest
design.

**Fault-injection seams**, each `testing.Testing()`-gated and panicking outside a
test binary:

- `testOnlyRandReader` replaces the coefficient source, to drive the RNG-failure
  path.
- `testOnlyProtectedMemory` / `testOnlySizedProtectedMemory` replace the
  from-bytes and sized locked-memory constructors, to drive refusal paths without
  exhausting `RLIMIT_MEMLOCK` for the whole test binary.

**Fixtures**: shares are generated per test rather than checked in. A checked-in
share is a real share of a real secret and does not belong in a repository.
