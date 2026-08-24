# Shamir Primitive — Architecture

> **Status: NORMATIVE** · Scope: go-quicraft `pkg/crypto/shamir`, plus the
> share-custody interfaces in `pkg/seal` · Last verified: 2026-08-19
> Conflicts with this document are defects. Fix them here first.

## The problem

A barrier root key held by one person, one process, or one file is a single
point of both loss and compromise. M-of-N secret sharing removes that: the key
is split into N shares such that any M reconstruct it and any M-1 reveal
**nothing at all** — not "nothing practical", but information-theoretically
nothing.

This repository owns the mathematics of that split, and nothing else about it.

## The scope fence comes first

This is the feature most likely to be misread, so the boundary is stated before
the design:

| | Owner |
|---|---|
| GF(2^8) `Split` / `Combine` — the polynomial arithmetic | **go-quicraft** (this feature) |
| The `ProtectedShareSplitter` / `ProtectedShareAccumulator` **interfaces**, `ShamirConfig`, `ShamirInitResult`, `QuorumProgress` | **go-quicraft** (`pkg/seal`) |
| The `shamir` **sealing strategy**, share custody, distribution, M-of-N quorum sessions, recovery, rekey, root tokens, the Security-Officer ceremony | **go-qrdb** |

go-quicraft can split a secret and combine shares. It **never holds** a share.
No share is written to a WAL, a snapshot, a config file, or any other artifact
this repository produces. The types listed in the middle row are contracts an
implementation elsewhere satisfies; `pkg/seal` declares them and calls them, and
ships no implementation of either.

The capability register (`docs/00 §R`) carries a row for the *primitive* only,
with the custody row assigned to go-qrdb. Anything in this repository that
started to look like custody would be a boundary violation, not a feature.

## The shape chosen

Classic Shamir over GF(2^8), one independent polynomial per secret byte.

- Each share is `[version:1][x:1][y...]`, base64-encoded into `Share.Value`.
  `y` has one byte per secret byte.
- `x` is the share's field coordinate and equals `Share.Index` (1-based).
- The `x` embedded in the value is **authoritative** for reconstruction, so a
  share string is self-contained: an operator can hand over one opaque string
  and nothing else is needed to place it in the interpolation.
- `Combine` is Lagrange interpolation at `x = 0` over the same field.

### Rejected: a digest of the secret inside each share

An earlier design embedded `sha256(secret)` in every share so `Combine` could
self-check its own output. **That is an offline brute-force oracle.** A single
shareholder — or anyone who reads one persisted share — can test guessed secrets
against the digest without ever reaching the threshold, which destroys Shamir's
information-theoretic secrecy exactly where it is most needed: low-entropy
secrets.

The digest was removed. `IntegrityError` remains in the package, deprecated and
never returned, because removing a type from a published API is a separate
break. **Do not write code that waits for it.**

The consequence is load-bearing and appears in every document about this
feature: with fewer than threshold genuine shares, or a corrupted or mismatched
share, reconstruction yields **indistinguishable garbage, not an error**.
Integrity is the caller's, established out of band.

### Rejected: authenticating `Threshold` and `Total`

`Share.Threshold` and `Share.Total` are caller-supplied metadata carried for
convenience. `Combine` takes the threshold from `shares[0].Threshold`, so
relabelling two shares of a real 5-of-9 split as `Threshold: 2` makes `Combine`
return garbage rather than an error.

Binding them cryptographically would require a commitment scheme — which is a
different primitive (VSS/Feldman), with a different security model and a
dependency this Layer-1 library does not take. The honest position is the one
taken: the fields are documented as unauthenticated, and the true threshold must
come from a trusted source.

### Rejected: unbounded secret size

`MaxSecretBytes` is 64 KiB. Shamir here is a **key**-custody primitive: the
secret is a 32-byte root key. Every larger payload costs O(shares × len) field
operations with roughly 1.75× the input resident while combining, and shares
cannot be authenticated *before* they are combined — so an attacker submitting
shares to a recovery ceremony could hand `Combine` 255 shares of a megabyte
each: ~340 MB of input and seconds of CPU, from unauthenticated material.

64 KiB is four orders of magnitude above any legitimate key and caps the work at
a few megabytes and milliseconds. Large payloads are enveloped under a key that
*is* this size; the payload itself is never shared.

## Three APIs, and why there are three

```
Split(secret, M, N) []*Share    SplitBytes(secret, M, N) [][]byte   SplitProtected(secret, M, N) []*ProtectedShare
        |                                |                                      |
  Share.Value is a Go STRING     raw wipeable []byte buffers          value in an enclave.MemoryEnclave
        |                                |                                      |
   immortal until GC             caller wipes when distributed        mlock'd; Destroy genuinely wipes
        |                                |                                      |
Combine([]*Share) []byte      CombineBytes([][]byte, M) []byte    CombineProtected([]*ProtectedShare) *MemoryEnclave
```

`Share.Value` is a Go `string` and therefore cannot be zeroed: every share stays
readable in the heap until the garbage collector reclaims it. `Split` wipes the
byte buffer it encoded from, but the string it returns is the one copy it cannot
take away — and a ceremony holds all N shares at once, which makes this the
point of widest exposure in the whole scheme.

`SplitBytes`/`CombineBytes` exist so callers that can keep share material in
wipeable buffers (memory-locked enclaves — see
[`../memory-enclave/architecture.md`](../memory-enclave/architecture.md)) never
have to materialize it as a string. `CombineBytes` wipes every decoded buffer on
every return path, including partially-decoded and validation-rejected shares;
the caller still owns and must wipe the inputs.

`SplitProtected`/`CombineProtected` go one step further and manage the wipeable
buffer for the caller, holding each share value in an `enclave.MemoryEnclave`.
Both are **native**, not wrappers over the bytes API: they encode and
interpolate directly into locked memory, so no secret-derived byte is ever
written to an ordinary heap buffer — not even one that is wiped afterwards.
Only the share x-coordinates and the Lagrange basis derived from them stay on
the ordinary heap, because a published share index is not secret material.

**The struct API is the deprecated compatibility surface. `ProtectedShare` is
the one to prefer in new custody code; the bytes API remains right for a caller
that already manages its own wipeable buffers.** The three are interchangeable
on the wire — a `ProtectedShare` marshals byte-for-byte what a `Share` does — so
a consumer migrates one call site at a time. The full contract, including the
three obligations that come with locked memory, is in
[`engineering.md`](engineering.md#share-values-in-protected-memory).

## Data flow, and where it stops

```
       go-qrdb ceremony                        go-quicraft
  ─────────────────────────────────   ──────────────────────────────
  root key (from the barrier)  ──────►  SplitBytes / Split
  distribute shares to custodians ◄─────  N share values
  ... shares live in go-qrdb custody, never here ...
  collect M shares from custodians ─────►  CombineBytes / Combine
  reconstructed key  ◄──────────────────   secret bytes
  authenticate it: unseal the barrier
  (an authenticated decrypt that fails on a wrong key)
```

Nothing crosses a process or host boundary inside this repository. Both
functions are pure, synchronous, in-process, and stateless. Share transport,
custodian identity, session TTL and quorum accounting all happen above.

## Trust boundary

- **Inputs are untrusted.** Shares arriving at `Combine` came from operators.
  Every entry point validates before use; `Validate` is safe on a `nil`
  receiver, and a `nil` element in a slice is a validation error naming its
  position, never a dereference. A JSON `null` in a submitted share list decodes
  to a `nil *Share`, and that must not take down the process.
- **Output is unauthenticated.** The reconstructed secret carries no proof. The
  caller authenticates it — canonically, by using it as the key in an
  authenticated decrypt that fails on a wrong key. See
  [`../seal-barrier/architecture.md`](../seal-barrier/architecture.md).
- **Shares are permanent disclosures.** A leaked share cannot be revoked, only
  outrun by a rekey. This is why `Share.String()` never includes the value and
  `Share.LogValue()` redacts on every structured-log handler — see
  [`engineering.md`](engineering.md) §7.

## Component boundaries

- **`pkg/crypto/shamir`** — the primitive. No dependency on the rest of the
  repository.
- **`pkg/seal/shamir.go`** — `ProtectedShareSplitter`,
  `ProtectedShareAccumulator`, `ShamirConfig`, `ShamirInitResult`,
  `QuorumProgress`. Interfaces and value types; **no implementation**. Every
  share-valued surface here is protected memory (#37); there is no string-typed
  sibling for a consumer to fall back to.
- **Consumers**: go-qrdb implements the interfaces and owns the `shamir`
  strategy. go-xkms's `xkey` acts as a custodian client. Neither is documented
  here; each repository documents its own half.

## What this feature deliberately does not do

- **No integrity check on `Combine`.** By design, permanently. See above.
- **No verifiable secret sharing.** No Feldman/Pedersen commitments, no proof
  that a dealer split honestly.
- **No share storage, transport, or custodian identity.**
- **No quorum session state.** `ProtectedShareAccumulator` is an interface
  this repository declares, not one it implements.
- **No refresh or proactive resharing.** Rotating shares without changing the
  secret is a go-qrdb ceremony.
- **No secrets larger than `MaxSecretBytes`.** Envelope them instead.
