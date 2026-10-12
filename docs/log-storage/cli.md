# Log Storage — CLI

> **Status: NORMATIVE** · Scope: go-quicraft `pkg/logdb`, `pkg/logdb/waldb`,
> `pkg/logdb/memdb` · Last verified: 2026-10-11
> Conflicts with this document are defects. Fix them here first.

**No CLI surface.** go-quicraft is a library and ships no binaries: there is no
`cmd/` directory, and `make build` runs `go build ./...`. This feature is used
through the Go API in [api.md](api.md) and contributes no `CommandDefinition` to
any parity suite.

Two operations look like they should be commands and deliberately are not:

- **Repair.** `waldb.Repair` discards data. It takes an explicit shard, segment
  and clean-prefix offset and refuses anything else. A command that guessed
  those arguments would be the tool that turns a recoverable segment into a
  shorter log, so it stays a Go function that the operator tooling above wraps
  behind its own confirmation.
- **Re-encryption.** `waldb.Reencrypt` needs a source and a destination barrier,
  each already unsealed with its own credentials. Getting those from a command
  line means a key-custody flow, which belongs to the layer that owns the keys.

Operator commands over a data directory (bootstrap, backup, restore, recovery)
belong to the database layer built on this library and are documented there.

## Build targets

| Target | Runs |
|---|---|
| `make test-waldb` | `go test ./pkg/logdb/waldb/...` |
| `make test-logdb` | `go test ./pkg/logdb/...` |
| `make bench-waldb` | the `waldb` benchmarks |
| `make fuzz-waldb` | the `waldb` fuzz targets (chunk, entry, state and bootstrap codecs, the address and removed maps, segment replay) |
| `make ci-errfs` | the `test/errfs` fault-injection suite |
