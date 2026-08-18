# Snapshots — CLI

> **Status: NORMATIVE** · Scope: go-quicraft snapshot creation, streaming,
> import/export and re-encryption · Last verified: 2026-08-19
> Conflicts with this document are defects. Fix them here first.

**No CLI surface: this feature is reachable only as a Go package.**

go-quicraft ships no binaries. The repository has no `cmd/` directory, and
`make build` runs `go build ./...` and produces no executable.

This is the feature where the absence is most likely to feel wrong, because
snapshot export, import and re-encryption *are* operator procedures. They are
exposed as Go functions because each one needs state a command line does not
have:

| Operation | Why it is a function, not a verb |
|---|---|
| `ExportSnapshot` | needs a live `Host` that knows the shard's applied index. A snapshot of a stopped node is a file copy, which needs no command. |
| `ImportSnapshot` | **rewrites a replica's membership and identity.** It is offline-only and destructive, and the arguments (`WithImportDataDir`, `WithImportMembership`) are exactly the ones a convenience command would guess. |
| `ReencryptSnapshotFile` | needs **two** unsealed barriers with their own credentials, which means a key-custody flow this repository does not own |

This feature therefore contributes **no** `CommandDefinition` to any integration
parity suite. The database layer above wraps these functions in whatever
operator surface and confirmation it considers adequate, and documents those
verbs in its own repository.

## Build-time targets

| Target | Runs |
|---|---|
| `make test-snapshot` | the snapshot suite, including durability, crash-filesystem and pin-balance tests |
| `make ci-errfs` | I/O error injection across the save and recover paths |
| `make ci-netfault` | a peer that disappears mid-transfer |

## Operational levers that are not verbs

| Lever | Where | Note |
|---|---|---|
| cadence | `Config.SnapshotEntries` | **zero does not disable it**; use `config.SnapshotsDisabled` |
| log retained after compaction | `Config.CompactionOverhead` | raise it if followers keep falling into snapshot transfer |
| size ceiling | `Config.MaxSnapshotSize` | 4 GB |
| transfer bandwidth | `TransportConfig.MaxSnapshotReceiveRate` | a recovering follower will use what you allow |
| transfer concurrency | `TransportConfig.MaxConcurrentSnapshotReceives` | |
| receive memory | `TransportConfig.MaxSnapshotReceiveMemory` | |
| compression | `config.CompressionType` | compress-then-encrypt when both are on |

## What an operator should actually watch

Not a command — three signals, described in
[`engineering.md`](engineering.md) §7:

1. **The age of the newest snapshot per shard.** A shard that has stopped
   snapshotting is a shard whose log will not compact and whose old DEKs can
   never be purged.
2. **Transfers in flight** against `MaxConcurrentSnapshotReceives`.
3. **Any `DataHash` verification failure**, which means a corrupt or truncated
   artifact rather than a transient error.
