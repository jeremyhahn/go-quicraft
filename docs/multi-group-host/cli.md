# Multi-Group Host — CLI

> **Status: NORMATIVE** · Scope: go-quicraft `pkg`, `pkg/config` ·
> Last verified: 2026-10-11
> Conflicts with this document are defects. Fix them here first.

**No CLI surface: go-quicraft is a library and ships no binaries.**

There is no `cmd/` directory. `make build` runs `go build ./...` and produces no
executable. You use the library by importing `pkg` and implementing its
interfaces. Every feature folder in this repository points here for the same
statement.

Because there are no verbs, this repository contributes no command definitions
to any CLI parity suite, and there is nothing here that can diverge from one.

## Where the operator commands live

An application embeds the `Host` and owns its operator surface. In this stack
that is go-qrdb, which provides peer discovery, cluster formation, bootstrap,
key custody and access control through its own SDK and CLI, documented in its
own repository.

## Build and verification targets

`make help` lists every target. The ones you are most likely to need:

| Target | Runs |
|---|---|
| `make build` | `go build ./...`. Produces no artifact |
| `make test` | the unit suite (`go test -race -count=1 ./...`, no build tags) |
| `make lint` | golangci-lint |
| `make ci` | `fmt no-mutants check-tool-versions vet lint security build test` |
| `make ci-full` | `ci` plus all modules, cross-platform build and vet, I/O faults, fuzz seeds and mutation, integration, network faults and the FIPS targets |
| `make coverage` | module coverage against `COVER_MIN = 91` |
| `make coverage-packages` | every package under `pkg/` against `COVER_MIN` |
| `make integration-test` | real clusters in containers |
| `make integration-test-linearizability` | the linearizability checker |
| `make ci-errfs` | I/O fault injection |
| `make ci-netfault` | partitions, delays, drops |
| `make fuzz-all` | every fuzz target |
| `make build-fips` | a compile check only. No artifact, and no cryptographic operation runs |
| `make verify-fips` | approved-only enforcement (`GODEBUG=fips140=only`) over everything except `pkg/internal/transport/revocation/...`. The QUIC transport is inside this gate |
| `make verify-fips-exempt` | that one excluded tree at `GODEBUG=fips140=on`; see the [exemption record](../quic-transport/architecture.md#fips-140-3-exemption-record) |
| `make bench-compare` | benchmark regression against stored baselines |
| `make security` | `gosec vuln modverify trivy` |
| `make deps-check` | every module's dependencies against their current releases |

Per-package `test-*`, `bench-*` and `fuzz-*` targets are listed in each feature
folder's `cli.md`.

Do not read `build-fips` as a compliance gate. The module has no `main`, and
`GOFIPS140` alone never executes a cryptographic operation. `verify-fips` is the
check that fails on a non-approved primitive.

## Settings that replace flags

All configuration is `HostConfig`, `Config` and `TransportConfig`, set by the
embedding application. These four have defaults that will hurt a production
deployment:

| Setting | Default | Why you set it |
|---|---|---|
| `MaxTotalInMemLogSize` | 0 (disabled) | otherwise one client can grow the in-memory log until the process dies |
| `MaxWALDiskSize` | 0 (unbounded) | otherwise the WAL fills the volume until a failed `fdatasync` poisons the store |
| `TransportConfig.MTLSConfig` | none | the host refuses to start without it whenever the transport is enabled |
| `KeyRotationInterval` | 0 (no scheduled rotation) | 24h is a reasonable production value when a barrier is configured |

Three fields do not treat zero as "off": `SnapshotEntries`,
`SessionExpiryEntries` and `MaxSessionsPerOwner`. Zero selects the default.
Assign `config.SnapshotsDisabled`, `config.SessionExpiryDisabled` or
`config.SessionsPerOwnerUnlimited` to turn them off.
