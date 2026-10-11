# Snapshots — CLI

> **Status: NORMATIVE** · Scope: go-quicraft snapshot creation, streaming,
> import/export and re-encryption · Last verified: 2026-10-11
> Conflicts with this document are defects. Fix them here first.

**No CLI surface.** go-quicraft is a library and ships no binaries: there is no
`cmd/` directory, and `make build` runs `go build ./...`. This feature is used
through the Go API in [api.md](api.md) and contributes no `CommandDefinition` to
any parity suite.

Export, import and re-encryption are operator procedures, but each needs state a
command line does not have:

| Operation | Why it is a function, not a command |
|---|---|
| `ExportSnapshot` | needs a live `Host` that holds the shard. A snapshot of a stopped node is a file copy. |
| `ImportSnapshot` | rewrites a replica's membership. It is offline and destructive, and its options (`WithImportDataDir`, `WithImportMembership`, `WithImportMemberIdentities`) are exactly what a convenience command would have to guess. |
| `ReencryptSnapshotFile` | needs two unsealed barriers with their own credentials, which means a key-custody flow owned by the layer that holds the keys |

The database layer built on this library wraps these in its own operator
commands and documents them there.

## Tests

The snapshot tests, including the crash-filesystem and I/O-failure suites, run
with `make test`. `make test-snapshot` runs the tests whose names match
`Snapshot|Import|Export` across `./pkg/...`; snapshot code has no package of
its own, so there is no `coverage-snapshot`. See
[engineering.md](engineering.md#6-test-strategy).
