# QUIC Transport — CLI

> **Status: NORMATIVE** · Scope: go-quicraft `pkg/internal/transport` ·
> Last verified: 2026-10-11
> Conflicts with this document are defects. Fix them here first.

There is no CLI: go-quicraft is a library and ships no binaries, so the
transport is reached only through `config.TransportConfig` (see
[`api.md`](api.md)).

The repository has no `cmd/` directory, and the transport packages are
`internal/`. Operations that look like verbs belong elsewhere:

| Looks like a verb | Belongs to |
|---|---|
| issue, rotate or revoke a node certificate; publish a CRL | the CA in the key-management layer (go-xkms) |
| add a peer, resolve an address | membership ([`../membership-changes/cli.md`](../membership-changes/cli.md)); peer discovery is go-qrdb's |
| inspect connections | the metrics an embedder installs with `WithMetrics`; see [`engineering.md`](engineering.md#observability) |

## Build-time targets

| Target | Runs |
|---|---|
| `make test-transport` | `go test -race -count=1 -v ./pkg/internal/transport/...` |
| `make bench-transport` | the transport benchmarks |
| `make ci-netfault` | the `test/integration/e2e` suites with the `quicraft_netfault` partition injector |
| `make verify-fips` | every package except `pkg/internal/transport/revocation/...` under `GOFIPS140=latest GODEBUG=fips140=only`, `-tags fips` |
| `make verify-fips-exempt` | `pkg/internal/transport/revocation/...` under `GOFIPS140=latest GODEBUG=fips140=on`, `-tags fips` |

The revocation tree runs one notch lower because the OCSP `CertID` is SHA-1 by
protocol definition and cannot be made approved from here; everything else on
that path is still proven approved. See the
[FIPS 140-3 exemption record](architecture.md#fips-140-3-exemption-record).
