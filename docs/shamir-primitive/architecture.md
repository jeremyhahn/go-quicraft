# Shamir primitive: architecture

> **Status: NORMATIVE** · Scope: go-quicraft `pkg/crypto/shamir`, plus the
> share-custody interfaces in `pkg/seal` · Last verified: 2026-10-11
> Conflicts with this document are defects. Fix them here first.

## The problem

A barrier root key held by one person, one process or one file is a single point
of both loss and compromise. M-of-N secret sharing removes that: the key is split
into N shares such that any M reconstruct it and any M-1 reveal nothing at all.
That is information-theoretic, not merely impractical to break.

This repository owns the mathematics of that split and nothing else about it.

## The scope fence comes first

This feature is the one most likely to be misread, so the boundary comes before
the design:

| | Owner |
|---|---|
| GF(2^8) split and combine: the polynomial arithmetic | **go-quicraft** (`pkg/crypto/shamir`) |
| The `ProtectedShareSplitter` / `ProtectedShareAccumulator` interfaces, `ShamirConfig`, `ShamirInitResult`, `QuorumProgress` | **go-quicraft** (`pkg/seal`), declared only |
| The `shamir` sealing strategy, share custody, distribution, M-of-N quorum sessions, recovery, rekey, root tokens, the Security-Officer ceremony | **go-qrdb** |

go-quicraft can split a secret and combine shares. It never holds a share. No
share is written to a WAL, a snapshot, a config file or any other artifact this
repository produces. The types in the middle row are contracts an implementation
elsewhere satisfies; `pkg/seal` declares them and ships no implementation, and
nothing in this module calls them.

`pkg/seal` itself is the Layer-1 encryptor only: the `EpochBarrier`, the `None`
and `Software` (passphrase) sealing strategies, and the `SealingStrategy` and
`ProtectedSealingStrategy` interfaces (see
[`../seal-barrier/architecture.md`](../seal-barrier/architecture.md)). The
`shamir` strategy ID belongs to go-qrdb. Anything in this repository that
started to look like custody would be a boundary violation, not a feature.

## The shape chosen

Classic Shamir over GF(2^8), one independent polynomial per secret byte.

- Each share value is `[version:1][x:1][y...]`, base64-encoded. `y` has one byte
  per secret byte.
- `x` is the share's field coordinate and equals the share's `Index` (1-based).
- The `x` embedded in the value is authoritative for reconstruction, so a share
  string is self-contained: an operator can hand over one opaque string and
  nothing else is needed to place it in the interpolation.
- Combining is Lagrange interpolation at `x = 0` over the same field.

### Rejected: a digest of the secret inside each share

Embedding `sha256(secret)` in every share would let combine self-check its
output, but it is an offline brute-force oracle. A single shareholder, or anyone
who reads one persisted share, could test guessed secrets against the digest
without reaching the threshold. That destroys Shamir's information-theoretic
secrecy exactly where it matters most: low-entropy secrets.

Shares carry no digest. `IntegrityError` remains in the package, deprecated and
never returned, so existing callers keep compiling. Do not write code that waits
for it.

The consequence runs through every document about this feature: with fewer than
threshold genuine shares, or a corrupted or mismatched share, reconstruction
yields indistinguishable garbage, not an error. Integrity is the caller's,
established out of band.

### Rejected: authenticating `Threshold` and `Total`

`Threshold` and `Total` on a share are caller-supplied metadata carried for
convenience. Combining takes the threshold from `shares[0].Threshold`, so
relabelling two shares of a real 5-of-9 split as `Threshold: 2` returns garbage
rather than an error.

Binding them cryptographically needs a commitment scheme (VSS/Feldman), a
different primitive with a different security model and a dependency this
Layer-1 library does not take. The fields are documented as unauthenticated, and
the true threshold must come from a trusted source.

### Rejected: unbounded secret size

`MaxSecretBytes` is 64 KiB. Shamir here is a key-custody primitive: the secret is
a 32-byte root key. A larger payload costs O(shares × len) field operations with
roughly 1.75× the input resident while combining, and shares cannot be
authenticated before they are combined. Without the bound, an attacker submitting
shares to a recovery ceremony could hand combine 255 one-megabyte shares: about
340 MB of input and seconds of CPU, from unauthenticated material.

64 KiB is four orders of magnitude above any legitimate key and caps the work at
a few megabytes and milliseconds. Large payloads are enveloped under a key that
does fit; the payload itself is never shared.

## Three APIs

```
SplitProtected(secret, M, N) []*ProtectedShare   SplitBytes(secret, M, N) [][]byte   Split(secret, M, N) []*Share
        |                                                |                                   |
  value in an enclave.Memory             raw wipeable []byte buffers        Share.Value is a Go STRING
        |                                                |                                   |
  mlock'd; Destroy wipes                        caller wipes when distributed       immortal until GC
        |                                                |                                   |
CombineProtected([]*ProtectedShare) *Memory   CombineBytes([][]byte, M) []byte   Combine([]*Share) []byte
```

- **Protected (preferred).** `SplitProtected` / `CombineProtected` hold each
  share value in an `enclave.Memory` (see
  [`../memory-enclave/architecture.md`](../memory-enclave/architecture.md)).
  Both are native, not wrappers over the bytes API: they encode and interpolate
  directly into locked memory, so no secret-derived byte is ever written to an
  ordinary heap buffer, not even one wiped afterwards. Only the share
  x-coordinates and the Lagrange basis derived from them stay on the heap,
  because a published share index is not secret material.
- **Bytes.** `SplitBytes` / `CombineBytes` are for callers that keep share
  material in wipeable buffers they manage themselves. `CombineBytes` wipes every
  decoded buffer on every return path, including partially decoded and rejected
  shares; the caller still owns and must wipe the inputs.
- **Struct (deprecated).** `Share.Value` is a Go `string` and cannot be zeroed,
  so every share stays readable in the heap until the garbage collector reclaims
  it. `Split` wipes the buffer it encoded from, but the string it returns is the
  one copy it cannot take back, and a ceremony holds all N shares at once: the
  widest exposure in the scheme. `Split`, `Combine`, `CombineUnverified` and the
  `Share.Value` field are deprecated for that reason.

The three are interchangeable on the wire: a `ProtectedShare` marshals
byte-for-byte what a `Share` does, so a consumer migrates one call site at a
time. The full contract, including the obligations that come with locked memory,
is in [`engineering.md`](engineering.md#share-values-in-protected-memory).

## Data flow, and where it stops

```
       go-qrdb ceremony                        go-quicraft
  ─────────────────────────────────   ──────────────────────────────
  root key (from the barrier)  ──────►  SplitProtected / SplitBytes
  distribute shares to custodians ◄─────  N share values
  ... shares live in go-qrdb custody, never here ...
  collect M shares from custodians ─────►  UnmarshalShareSet + CombineProtected
  reconstructed key  ◄──────────────────   secret in locked memory
  authenticate it: unseal the barrier
  (an authenticated decrypt that fails on a wrong key)
```

Nothing crosses a process or host boundary inside this repository. Every function
is pure, synchronous, in-process and stateless. Share transport, custodian
identity, session TTL and quorum accounting all happen above.

## Trust boundary

- **Inputs are untrusted.** Shares arriving at combine came from operators.
  Every entry point validates before use; `Validate` is safe on a `nil`
  receiver, and a `nil` element in a slice is a validation error naming its
  position, never a dereference. A JSON `null` in a submitted share list decodes
  to a `nil` share, and that must not take down the process.
- **Ingestion is budgeted.** A share set from outside the process is decoded with
  `UnmarshalShareSet`, which prices the set's locked memory against
  `MaxShareSetBytes` before allocating any of it. The submitter, not the service,
  would otherwise choose how much of a process-wide resource to take.
- **Output is unauthenticated.** The reconstructed secret carries no proof. The
  caller authenticates it, canonically by using it as the key in an
  authenticated decrypt that fails on a wrong key. See
  [`../seal-barrier/architecture.md`](../seal-barrier/architecture.md).
- **Shares are permanent disclosures.** A leaked share cannot be revoked, only
  outrun by a rekey. That is why `String()` never includes the value and
  `LogValue()` redacts on every structured-log handler (see
  [`engineering.md`](engineering.md) §7).

## Component boundaries

- **`pkg/crypto/shamir`**: the primitive. Its only dependency is go-enclave; it
  imports nothing else from this repository.
- **`pkg/seal/shamir.go`**: `ProtectedShareSplitter`,
  `ProtectedShareAccumulator`, `ShamirConfig`, `ShamirInitResult`,
  `QuorumProgress`, `DefaultQuorumTTL`. Interfaces and value types, no
  implementation. Every share-valued surface is protected memory; there is no
  string-typed sibling.
- **Consumers**: go-qrdb implements the interfaces and owns the `shamir`
  strategy. go-xkms's `xkey` acts as a custodian client. Each repository
  documents its own half.

## What this feature deliberately does not do

- **No integrity check on combine.** By design, permanently. See above.
- **No verifiable secret sharing.** No Feldman/Pedersen commitments, no proof
  that a dealer split honestly.
- **No share storage, transport or custodian identity.**
- **No quorum session state.** `ProtectedShareAccumulator` is an interface this
  repository declares, not one it implements.
- **No refresh or proactive resharing.** Rotating shares without changing the
  secret is a go-qrdb ceremony.
- **No secrets larger than `MaxSecretBytes`.** Envelope them instead.
