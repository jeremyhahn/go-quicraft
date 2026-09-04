# Shamir Primitive — Engineering Contract

> **Status: NORMATIVE** · Scope: go-quicraft `pkg/crypto/shamir`, plus the
> share-custody interfaces in `pkg/seal` · Last verified: 2026-09-02
> Conflicts with this document are defects. Fix them here first.

Import paths:
`github.com/jeremyhahn/go-quicraft/pkg/crypto/shamir` (the primitive)
`github.com/jeremyhahn/go-quicraft/pkg/seal` (the custody interfaces)

## 1. Exported surface

### `pkg/crypto/shamir`

```go
const MaxSecretBytes    = 64 << 10   // 65536
const MaxShareSetBytes  = 1 << 20    // 1 MiB — whole-set footprint budget

// Split / combine — struct form (DEPRECATED: string custody)
func Split(secret []byte, threshold, total int) ([]*Share, error)
func Combine(shares []*Share) ([]byte, error)
func CombineUnverified(shares []*Share) ([]byte, error)

// Split / combine — protected-memory form
func SplitProtected(secret []byte, threshold, total int) ([]*ProtectedShare, error)
func CombineProtected(shares []*ProtectedShare) (*enclave.MemoryEnclave, error)
func NewProtectedShare(index, threshold, total int, value []byte) (*ProtectedShare, error)
func UnmarshalShareSet(data []byte) ([]*ProtectedShare, error)

// Split / combine — wipeable bytes form
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
func (s *Share) Validate() error
func (s *Share) String() string
func (s *Share) LogValue() slog.Value
func (s *Share) MarshalJSON() ([]byte, error)
func (s *Share) UnmarshalJSON(data []byte) error

type ProtectedShare struct {
    Index     int
    Threshold int
    Total     int
    Metadata  map[string]string   // NOT protected memory — no secrets here
    // value is unexported: it lives in an enclave.MemoryEnclave
}

func (s *ProtectedShare) Value() []byte
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
type IntegrityError       struct{}   // DEPRECATED — never returned
```

Every locked-memory refusal in this package reports as `*ProtectedMemoryError`.
It is deliberately distinct from `*ValidationError` because the two say different
things about who owns the caller's buffer afterwards — see `api.md` §Error
vocabulary. `SplitError` and `CombineError` remain for logic failures, including
the `MaxShareSetBytes` budget refusal, which is a rejected request rather than an
exhausted host.

`Combine` and `CombineUnverified` are **identical**. Both names are retained so
a caller can express intent at the call site; neither verifies anything.

### Share values in protected memory

`Share.Value` is a Go `string`, and a string is immortal: it cannot be zeroed,
so every share stays readable in the heap until the collector reclaims it. A
ceremony holding all N shares at once is the widest exposure in the scheme.
That field, and the `Split` / `Combine` / `CombineUnverified` functions that
hand out and consume it, are **deprecated**.

The replacement is `ProtectedShare`, whose value lives in an
`enclave.MemoryEnclave` — mlock'd, guard-paged, canary-checked, and genuinely
wiped by `Destroy`:

| Deprecated (string custody) | Protected replacement |
|---|---|
| `Split` | `SplitProtected` |
| `Combine`, `CombineUnverified` | `CombineProtected` (returns the secret in a `*enclave.MemoryEnclave`) |
| `&Share{…, Value: v}` | `NewProtectedShare(index, threshold, total, value)` — wipes the source **on success only**; the caller wipes it on every error return |
| an existing `*Share` | `(*Share).Protect()` |

Both protected paths are **native**, not wrappers: `SplitProtected` reads the
polynomial coefficients into locked memory and encodes each share value
directly into the locked buffer that share will own, and `CombineProtected`
decodes each share value in place and interpolates directly into the locked
buffer it returns. Neither routes through `SplitBytes`/`CombineBytes`, which by
contract return ordinary heap buffers, so no secret-derived byte is ever
written to unprotected memory — not even to one that is wiped afterwards. The
share x-coordinates and the Lagrange basis derived from them are the exception
and stay on the ordinary heap: an x-coordinate is a published share index, not
secret material.

`CombineProtected` also holds only **one decoded share at a time**, walking the
set twice against a single reused buffer rather than keeping every decoded
payload: the peak locked memory of a reconstruction is a property of the share
size, not of how many shares were submitted.

`SplitBytes` and `CombineBytes` are unchanged and are still the right path for
a caller that manages its own wipeable buffers; `ProtectedShare` is for the
caller that wants the buffer managed for it.

Nothing was removed and no signature changed: the JSON a `ProtectedShare`
marshals is byte-for-byte what a `Share` marshals, so the two are
interchangeable on the wire and a consumer can migrate one call site at a time.

### `pkg/seal` — the custody interfaces this repository declares but does not implement

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
// receiver and safe to call twice, so a ceremony can `defer` it from the line
// the result exists.
func (r *ShamirInitResult) Destroy()

type QuorumProgress struct {
    Required  int  `json:"required"`
    Submitted int  `json:"submitted"`
    Complete  bool `json:"complete"`
}

const DefaultQuorumTTL = 5 * time.Minute
```

`ProtectedShareAccumulator` implementations handle deduplication, ordering and
TTL expiration, and are **not** required to be safe for concurrent use — the
`Barrier` provides that synchronization through its own mutex.

**There is no string-typed custody surface left.** `ShareSplitter`,
`ShareAccumulator` and `ShamirInitResult.Shares []string` were removed in #37 on
the owner's ruling, one release after #35 added the protected siblings. Both
were typed in `string`, and a Go string cannot be zeroed — so an implementation
that split into locked memory had to copy every share straight back out into an
immortal heap string to satisfy the signature, and an accumulator received a
share that was already un-wipeable before any protected buffer existed. Keeping
them as deprecated siblings let a consumer migrate to
`SplitProtected`/`CombineProtected`, silence every `SA1019`, and leave the
exposure exactly where it was — which is worse than not migrating, because it
removes the honest signposts.

Both ENDS of the protected family are protected deliberately:
`AddProtectedShare` **takes ownership** of a share already in locked memory
(destroying it whether or not the share is accepted and whether or not the
quorum completes), and `ReconstructProtected` returns the secret in an
`enclave.MemoryEnclave` rather than an ordinary `[]byte`. Protecting one end only
would leave the exposure intact at the other.

`ShamirInitResult` is the moment a ceremony holds all N shares at once. It OWNS
them: `Destroy()` releases every one, and an abandoned result holds N locked
allocations against `RLIMIT_MEMLOCK` for the life of the process, so later locks
anywhere in it start failing. Its `"shares"` JSON changed shape with the retype —
from an array of bare values to the array of share OBJECTS every other share
surface in the stack already uses.

### The protected sealing pair

`SealingStrategy.SealRootKey`/`UnsealRootKey` stay as they are, and #37 added an
optional sibling interface beside them:

```go
type ProtectedSealingStrategy interface {
    SealingStrategy

    SealRootKeyProtected(ctx context.Context, rootKey *enclave.MemoryEnclave, creds Credentials) (*SealedRootKey, error)
    UnsealRootKeyProtected(ctx context.Context, sealed *SealedRootKey, creds Credentials) (*enclave.MemoryEnclave, error)
}

type Credentials struct {
    Secret *enclave.SealedEnclave
}

func NewCredentials(secret []byte) (Credentials, error) // moves and WIPES secret

// Deprecated: aliases kept so implementations naming them still compile.
type ProtectedCredentials = Credentials
var  NewProtectedCredentials = NewCredentials
```

The reported defect it answers is go-qrdb's recovery strategy (go-qrdb PR #76,
Codex round 4): it reconstructs the root key into an enclave and then copies it
into a plain `[]byte` field purely to have something to return from
`UnsealRootKey`, so the most sensitive value in the system sits in collectable,
swappable memory for the whole barrier-unseal and is wiped only at `Close`. No
care on the implementer's side can avoid that while the signature demands a
`[]byte`; the fix belongs at the layer that owns the signature.

Two implementation notes that are not obvious from the signatures:

- **Order of operations.** `SoftwareStrategy` derives the wrapping key BEFORE it
  borrows the root-key enclave. It had to: until the go-enclave adoption, this
  repository's own `pkg/enclave` (since deleted) held a process-wide `purgeMu`
  shared across every borrow while allocating locked memory took it exclusively,
  so opening the passphrase enclave inside a root-key borrow was a self-deadlock
  rather than a documented hazard.
  That lock is gone and the nesting is now legal, but the ordering stays: it keeps
  the key-touching window as short as it can be, and the AEAD it produces holds
  only an expanded key schedule in ordinary memory, which a borrow may touch
  freely.
- **The plaintext never exists on the heap.** GCM's plaintext length is known
  before the decryption — payload minus nonce minus tag — so the enclave is
  allocated at exactly that size and `aead.Open` is handed the enclave's own
  storage as its append destination.
- **That length is refused unless it is the root-key size, before anything is
  locked.** It is derived from a blob the AEAD has not authenticated yet, and
  locked memory is a finite process-wide resource — so sizing an `mlock`'d
  allocation from it would let whoever can write the blob exhaust that resource
  for every other caller, before the tag that rejects it is checked. Under
  memguard this was worse still: a refused lock TERMINATED THE PROCESS. Since the
  go-enclave adoption it fails exactly the allocation that hit the ceiling, which
  lowers the stakes without removing the reason for the check.

`NoneStrategy` implements the pair too, and `UnsealRootKeyProtected` returns
`ErrNoRootKeyAvailable`: no-encryption mode has no root key, a zero-length
allocation is refused (`ErrEmptyEnclave`), and an enclave "holding" an empty key
would be a protected wrapper around a fiction. Its
`SealRootKeyProtected` validates the enclave even though it never reads it —
a contract the reference implementation ignores is not a contract.

**Both sides of the boundary are protected.** For a deferred-passphrase strategy
the passphrase is not a lesser secret than the root key it wraps — it IS that
key's protection — so accepting it in a plain `[]byte` would protect the wrong
half. That used to require a second credential TYPE, because `Credentials.Secret`
was a `[]byte` and only these two methods could afford to break. #41 retyped
`Credentials.Secret` itself onto `*enclave.SealedEnclave`, so there is now one
credential type and `ProtectedCredentials` is a deprecated alias for it.

One consequence is worth knowing: every root-key method now derives straight from
the credential's sealed passphrase instead of building a temporary inner strategy
from a plain slice, so a FIPS-unapproved KDF is refused at `DeriveKey` rather than
at that construction. The protected pair has behaved that way since PR #40; #41
made the `[]byte` siblings agree with it rather than changing them alone.

The `Barrier` still calls the `[]byte` root-key pair. It needs raw bytes for HKDF
epoch-DEK derivation and the sealed-state HMAC, holds them in a locked enclave for
the whole of `Initialize`/`Unseal`, and then seals them. Moving the barrier itself
onto the protected pair is tracked separately (#39): the root-key enclave is
deliberately left WRITABLE so `enclave.NewSealed` can consume it, and a wipe of
frozen pages faults the process.

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
| `threshold >= 2` | `Split`, `SplitProtected`, `CombineBytes`, `Share.Validate`, `ProtectedShare.Validate` | `SplitError` / `CombineError` / `ValidationError` |
| `total >= threshold` | `Split`, `SplitProtected`, `Share.Validate`, `ProtectedShare.Validate` | as above |
| `threshold <= 255`, `total <= 255` | `Split`, `SplitProtected` | `SplitError` |
| `len(secret) > 0` | `Split`, `SplitProtected` | `SplitError` |
| `len(secret) <= MaxSecretBytes` | `Split`, `SplitProtected` | `SplitError` |
| whole-set footprint `<= MaxShareSetBytes` | `SplitProtected` | `SplitError` |
| summed element length `<= MaxShareSetBytes` | `UnmarshalShareSet` | `ValidationError` |
| `len(shares) <= 255` | `UnmarshalShareSet` | `ValidationError` |
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

**Protected-memory obligations**

Three obligations come with `ProtectedShare`:

- **Every `ProtectedShare` must be `Destroy`ed, including one produced by
  `json.Unmarshal`** — decoding is a construction. An abandoned share holds a
  locked allocation for the life of the process.
- **Locked memory is a finite kernel resource** (`RLIMIT_MEMLOCK`), and each
  share is one locked allocation. Destroy shares as they are consumed rather
  than at the end of a ceremony.
- **A slice from `Value` does not outlive the share.** `Value` and `Destroy`
  are safe to CALL concurrently (since `0.8.0-alpha` — before that they raced,
  and because memguard genuinely unmaps, the consequence was a segfault: issue
  #15). What no lock can bound is a slice a caller kept after `Value` returned.
  Where one goroutine tears shares down while another still reads them, which
  is a ceremony's normal shape, use `WithValue(func([]byte) error) error`: it
  holds the share open for the whole callback, so a concurrent `Destroy` blocks
  until the callback returns.

Because of the second one, the two calls that allocate a whole set price it
first. `SplitProtected` rejects parameters whose footprint — coefficients,
the reused scratch, the reused ENCODING scratch, and every returned share value —
exceeds `MaxShareSetBytes` (1 MiB), since individually legal arguments multiply
into one no host can lock (a 64 KiB secret split 255 ways needs ~16 MiB of
coefficients alone). The encoding scratch is counted because it is live at the
peak: go-enclave has no post-fill freeze, so a share's frozen value is built by
copying out of the scratch and both exist during the copy. Omitting it made the
cap stop being an upper bound, which is a defect rather than an approximation —
`secretLen=1757` at 255-of-255 priced under the cap and exceeded it in fact. A full 255-share ceremony over a 32-byte
root key needs ~24 KiB of that budget; sharing a 4 KiB *payload* 255 ways does
not fit and should be enveloped under a key that does.

And **shares arriving from outside the process are decoded with
`UnmarshalShareSet`, not `json.Unmarshal` into a `[]*ProtectedShare`.** Shares
are unauthenticated by construction — nothing about one can be checked until
the set is combined — so a recovery endpoint necessarily decodes whatever it is
handed, and the generic decoder allocates one locked buffer per element as it
visits them: the submission, not the service, would choose how much locked
memory to take, exhausting a process-wide resource every other caller depends
on. `UnmarshalShareSet` bounds the element count (255, the scheme's own limit) and
the **summed length of the raw JSON elements** against `MaxShareSetBytes`
before it allocates any of it, and destroys the accepted prefix if a later
element is rejected. The sum is taken over the elements rather than from
`len(data)` deliberately: it is the same bound, but it cannot be evaded by
whitespace accounting, and each element's length is an upper bound on the
locked buffer it will produce. For scale, the full 255-share set of a 32-byte
root key is ~24 KiB against the 1 MiB budget. A ceremony that legitimately
needs more than the budget does its own accounting and decodes share by share
with `NewProtectedShare`.

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
| `*SplitError` | `Reason` | `Split`, `SplitBytes`, `SplitProtected` — parameter and size violations, RNG failure, and (protected only) a whole-set footprint above `MaxShareSetBytes` |
| `*CombineError` | `Reason` | `Combine`, `CombineUnverified`, `CombineBytes`, `CombineProtected` — set-level and per-share structural violations |
| `*ValidationError` | `Field`, `Reason` | `Share.Validate`, `ProtectedShare.Validate`, `VerifyShare`, `NewProtectedShare`, `UnmarshalShareSet` — per-field violations, positionally named |
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
