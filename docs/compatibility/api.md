# Compatibility and upgrades: API

> **Status: NORMATIVE** · Scope: go-quicraft, all persisted and wire formats ·
> Last verified: 2026-10-13
> Conflicts with this document are defects. Fix them here first.

## Transport coverage

go-quicraft is a library, and the Go API is the only API. Compatibility is a
policy, not a capability: it adds no call of its own. This file lists what the
policy requires of an embedder's own types, and the errors each version check
returns.

| Transport | This feature |
|---|---|
| Embedded Go | the interface requirements and error values below |
| REST, gRPC, QUIC method, MCP, Unix socket | none: this library serves no request API. The QUIC transport carries Raft traffic only, versioned as in [engineering.md](engineering.md#version-discriminators). |

## Interface requirements for embedders

These are satisfied structurally, so a type check alone will not catch a type
that misses them.

### `waldb.FS` requires `Rename`

Any `waldb.FS` (test doubles, instrumented or fault-injecting filesystems)
provides `OpenFile`, `Open`, `MkdirAll`, `ReadDir`, `Stat`, `Remove` and
`Rename` (`pkg/logdb/waldb/fs.go`). `Rename` is required because the
`WALFORMAT` marker is published by writing a temporary file, fsyncing it and
renaming it into place; an in-place fallback would restore the torn state the
rename removes. Delegate to `os.Rename` or model its atomicity.

### Barriers given to `waldb` report `ContextBindingResolved() bool`

`waldb` asserts an unexported interface on the dynamic type of the barrier
passed to `waldb.WithBarrier`. A barrier missing the method compiles and fails
at `Open` with `waldb.ErrBarrierContextBindingUnreportable`.

The method reports whether `SupportsContextAAD()` is currently an answer: true
once the barrier is unsealed with its encryptor installed, false while it is
sealed or uninitialised. The WAL resolves the capability once and writes every
record against it, so it must tell "cannot bind" from "cannot say yet".
`*seal.Barrier` implements it.

### `seal.ContextBinding`'s zero value is `Unresolved`

| Constant | Value | Meaning |
|---|---|---|
| `ContextBindingUnresolved` | 0 | no answer yet: sealed or uninitialised. `Binds()` is false |
| `ContextBindingBarrier` | 1 | the barrier folds the caller's context into its own AEAD's AAD |
| `ContextBindingDelegated` | 2 | a delegated hardware or KMS encryptor owns the binding |
| `ContextBindingUnsupported` | 3 | the encryptor cannot bind context |
| `ContextBindingPlaintext` | 4 | not encrypting |

Compare against the named constants and never persist or transmit the numeric
value. A barrier can legitimately start sealed and be unsealed later, so any
consumer that caches the capability re-asks after `Unseal`.

### Custom `Barrier`s implement `EncryptWithEpochPinnedContext` and take a context

Sealing a snapshot frame and pinning its epoch must happen under one lock;
otherwise `Rotate` plus `PurgeEpochsBefore` could drop the DEK of a frame about
to be persisted. There is no fallback for a barrier without it.

`Rotate(ctx context.Context) (uint64, error)` and
`PurgeEpochsBefore(ctx context.Context, minEpoch uint64) (int, error)` on the
`quicraft.Barrier` interface take a leading context, which a key provider that
blocks honours.

### `seal.NewBarrier` returns `(*Barrier, error)`

An invalid configuration (unknown algorithm, non-approved cipher under FIPS
mode) is an error, never a panic, because such configurations come from files,
API requests and per-tenant settings.

| Constructor | Returns | Use when |
|---|---|---|
| `seal.NewBarrier(cfg)` | `(*Barrier, error)` | configuration assembled at run time |
| `seal.MustNewBarrier(cfg)` | `*Barrier`, panics | configuration from compile-time constants |
| `seal.NewEpochBarrier` / `seal.MustNewEpochBarrier` | aliases | the same functions under the epoch-barrier name |
| `crypto.NewBarrier` / `crypto.MustNewBarrier` | aliases | `pkg/crypto` re-exports |

## Errors the version checks return

Every one is fatal for the operation and none is retried. The remedy for each
format error is in [user.md](user.md#upgrade-path).

| Error | Package | Returned when |
|---|---|---|
| `ErrUnsupportedVersion` | `pkg/proto` | a `MessageBatch` or `SnapshotChunk` carries another `BinVer` |
| `*UnsupportedMembershipVersionError` | `pkg/proto` | a persisted `Membership` section's version byte is not 1; matches `ErrUnsupportedVersion` |
| `ErrBufferTooSmall` | `pkg/proto` | a `ConfigChange` payload is cut short |
| `ErrWALFormatUnsupportedVersion` (`*WALFormatVersionError`) | `pkg/logdb/waldb` | the `WALFORMAT` marker records another version |
| `ErrWALFormatMalformed` | `pkg/logdb/waldb` | the marker cannot be parsed |
| `ErrWALFormatMissing` | `pkg/logdb/waldb` | segments exist with no marker |
| `ErrUnknownPayloadType` | `pkg/logdb/waldb` | a record's payload-type byte is retired or unknown, or the WAL is encrypted and was opened with no barrier |
| `ErrTruncatedSnapshotRecord` | `pkg/logdb/waldb` | a snapshot record's membership section is shorter than 74 bytes |
| `ErrBarrierContextBindingUnreportable` | `pkg/logdb/waldb` | the barrier lacks `ContextBindingResolved()` |
| `ErrSnapshotCorrupt` | `pkg` | a `snapshot.meta` lacks `DataLen`/`DataHash`, an at-rest snapshot is truncated to nothing, or an artifact hash mismatches |
| `*SnapshotArtifactError` | `pkg` | an imported artifact's trailer is missing, truncated or mismatched |
| `ErrSealedStateUnauthenticated` | `pkg/seal` | a sealed root key has no state MAC, or one that does not verify |
| `*AlgorithmMismatchError` | `pkg/seal` | a sealed root key's `Algorithm` disagrees with the barrier configuration |
| `ErrMemberIdentityRequired` | `pkg` | a member is added without an identity |
| `ErrDuplicateMemberIdentity` | `pkg` | one identity is bound to two replicas |
| `*InvalidMemberError` | `pkg` | `StartShard` member identities are incomplete or inconsistent |

## Versioning and deprecation

- The exported Go API carries no compatibility promise between minor versions
  before 1.0 ([architecture.md](architecture.md#stability-guarantee)). A
  removal, a signature change, or a run-time behaviour change a caller can
  observe is listed in the CHANGELOG under BREAKING.
- There is no deprecation window. A deprecated alias lives at most until the
  next minor version; `seal.ProtectedCredentials` was removed in 0.17.0 that
  way.
- Every format version constant is pinned until 1.0
  ([architecture.md](architecture.md#format-versions-are-pinned-until-the-first-production-release)).
  None of them is exported for an embedder to compare against except
  `proto.WireVersion`.
