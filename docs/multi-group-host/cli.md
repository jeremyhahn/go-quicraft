# Multi-Group Host — CLI

> **Status: NORMATIVE** · Scope: go-quicraft `pkg`, `pkg/config` ·
> Last verified: 2026-08-19
> Conflicts with this document are defects. Fix them here first.

**No CLI surface: go-quicraft ships no binaries.**

The repository has no `cmd/` directory. `make build` runs `go build ./...` and
produces no executable, and `make clean` removes coverage output and profiles
rather than an artifact. This is correct for the project: it is a consensus
**library**, consumed by importing `pkg` and implementing its interfaces — not
by invoking a client.

Every feature folder in this repository states the same thing for its own
scope; this file is where the repository-wide statement lives.

## Consequences for the parity suite

This repository contributes **no** `CommandDefinition` to any integration parity
suite, and that absence is complete rather than partial: there is no verb here
that is merely unimplemented. A divergence between the portfolio's locked CLI
surface and this repository is therefore not possible, because this repository
has no CLI surface to diverge.

## Where the verbs an operator runs actually live

The `Host` is embedded by the layer above, and that layer owns the operator
surface:

| Owner | What it exposes over this library |
|---|---|
| **go-qrdb** | the database and clustering product: peer discovery, cluster formation, bootstrap and firstboot, the ceremony, barrier and share custody, identity and RBAC — all through its own SDK and CLI |
| **go-xkms** | hardware and cloud key custody, the CA, and the `xkey` desktop companion |
| **go-quicbaas** | the composed product surface |
| **go-trusted-platform** | `platformctl` and the platform control plane, re-exporting the composed stack |

Those verbs are documented in their own repositories, against the portfolio's
locked CLI surface. Nothing in this repository documents them, and a divergence
there is raised there.

## Build and verification targets

The complete list is `make help`. The ones that matter:

| Target | Runs |
|---|---|
| `make build` | `go build ./...` — produces no artifact |
| `make test` | the full unit suite |
| `make lint` | golangci-lint |
| `make ci` | the gate: `fmt no-mutants check-tool-versions vet lint security build test` |
| `make coverage` | coverage against `COVER_MIN = 91` |
| `make integration-test` | real clusters in containers |
| `make integration-test-linearizability` | the linearizability checker |
| `make ci-errfs` | I/O fault injection |
| `make ci-netfault` | partitions, delays, drops |
| `make fuzz-all` | every fuzz target |
| `make build-fips` | **a compile check only** — no artifact is produced and no cryptographic operation runs |
| `make verify-fips` | approved-only enforcement over everything except `pkg/internal/transport/revocation/...` — the QUIC transport is inside this gate |
| `make verify-fips-exempt` | that one excluded tree, at `GODEBUG=fips140=on` — see the quic-transport exemption record |
| `make bench-compare` | benchmark regression against stored baselines |
| `make security` | `gosec vuln modverify trivy` |

Per-package `test-*`, `bench-*` and `fuzz-*` targets are listed in each feature
folder's `cli.md`.

**Do not read `build-fips` as a compliance gate.** This module is a library with
no `main`, so it produces no artifact, and `GOFIPS140` alone never executes a
cryptographic operation. `verify-fips` is the check that can actually fail on a
non-approved primitive.

## Operational levers, since there are no flags

Everything is `HostConfig`, `Config` and `TransportConfig`, set by the embedding
application. The four whose default will hurt a production deployment:

| Lever | Default | Set it because |
|---|---|---|
| `MaxTotalInMemLogSize` | **0 = disabled** | one client can otherwise grow the in-memory log until the process dies |
| `MaxWALDiskSize` | **0 = unbounded** | the WAL otherwise fills the volume until `fdatasync` poisons the store |
| `MTLSConfig` | none | the transport refuses to start without it — which is the point |
| `KeyRotationInterval` | 0 = no scheduled rotation | 24h is a reasonable production value when a barrier is configured |

And the two whose zero value does not mean "off": `SnapshotEntries` and
`SessionExpiryEntries`. Zero selects the default; `config.SnapshotsDisabled` and
`config.SessionExpiryDisabled` are the values that disable.
