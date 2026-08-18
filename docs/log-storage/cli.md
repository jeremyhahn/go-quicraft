# Log Storage — CLI

> **Status: NORMATIVE** · Scope: go-quicraft `pkg/logdb`, `pkg/logdb/waldb`,
> `pkg/logdb/memdb` · Last verified: 2026-08-19
> Conflicts with this document are defects. Fix them here first.

**No CLI surface: this feature is reachable only as a Go package.**

go-quicraft ships no binaries. The repository has no `cmd/` directory, and
`make build` runs `go build ./...` and produces no executable.

This feature contributes **no** `CommandDefinition` to any integration parity
suite. Two operations look like they should be verbs and deliberately are not:

- **Repair.** `waldb.Repair` takes an explicit shard, segment and clean-prefix
  offset, and refuses anything else. It **discards data**. A one-line command
  that guesses those arguments is precisely the tool that turns a recoverable
  segment into a shorter log, so the operation is exposed as a Go function that
  a custody layer wraps behind whatever confirmation it considers adequate.
- **Re-encryption.** `waldb.Reencrypt` needs both a source and a destination
  barrier, each already unsealed with its own credentials. Reaching those from a
  command line means a key-custody flow, which is not this repository's.

## Where the operator verbs live

The database layer above owns the operator surface over a data directory —
bootstrap, backup, restore, compaction policy, and the recovery procedures that
wrap the two functions above. Those verbs are documented in that repository,
against the portfolio's locked CLI surface.

## Build-time targets

| Target | Runs |
|---|---|
| `make test-waldb` | `go test ./pkg/logdb/waldb/...` |
| `make test-logdb` | `go test ./pkg/logdb/...` |
| `make bench-waldb` | the WAL write/fsync/compaction benchmarks |
| `make fuzz-waldb` | the record and segment decode fuzz targets |
| `make ci-errfs` | the `test/errfs` fault-injection suite — the I/O error paths |

## Operational levers that are not verbs

Everything is configuration passed at `Open` by the embedding application:

| Lever | Option | Note |
|---|---|---|
| block size | `WithBlockSize` | **immutable** for the life of the data directory; `Open` refuses a change |
| segment size | `WithMaxFileSize` | safe to raise on a populated directory; only affects future rotations. 256–512 MiB for clusters with thousands of shards |
| durability | `WithNoSync` | **never in production.** Disables every fsync *and* every directory sync, so recovery is always free to truncate |
| disk ceiling | `WithDiskMonitorOpts` | past the full threshold, proposals are rejected with `ErrDiskFull` instead of the WAL filling the volume |
| encryption | `WithBarrier` | the barrier must be initialized **and unsealed** before `Open` |
| instrumentation | `WithMetrics` | write and fsync latency are what separate "slow disk" from "slow pipeline" |

## Two things to check after every restart

Neither is a command; both are one call, and both are silent if you do not make
them.

```go
residuals := db.RecoveryResiduals() // empty on a clean recovery
```

Non-empty means the node **may have come back shorter than the log it
acknowledged**. That is the condition worth paging on.

```go
protection := db.AtRestProtection()
```

Check the store's own answer rather than inferring encryption from the fact that
you passed a barrier.
