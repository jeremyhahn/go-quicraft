# Seal & Barrier — CLI

> **Status: NORMATIVE** · Scope: go-quicraft `pkg/seal` · Last verified: 2026-09-15
> Conflicts with this document are defects. Fix them here first.

**No CLI surface: this feature is reachable only as a Go package.**

go-quicraft ships no binaries. The repository has no `cmd/` directory, and
`make build` runs `go build ./...` and produces no executable. It is a consensus
library, consumed by importing it and implementing its interfaces.

This feature contributes **no** `CommandDefinition` to any integration parity
suite. That is not a gap: a `barrier init` or `barrier unseal` verb needs a
passphrase prompt, a share-collection session, a TPM handle, or a KMS
credential — every one of which belongs to a custody layer this repository
explicitly does not own.

## Where the operator verbs live

The `barrier`, `share`, `pin`, `ca`, `cert`, `trust` and `auth` verb groups an
operator actually runs belong to the layers above, and follow the portfolio's
locked CLI surface:

| Owner | Responsibility |
|---|---|
| **go-qrdb** | barrier registry, tenant barriers, the `shamir` strategy, share custody, quorum, recovery, rekey, the Security-Officer ceremony, firstboot/bootstrap |
| **go-xkms** | hardware and cloud strategies — TPM2, PKCS#11, AWS/GCP/Azure KMS, Vault |
| **go-xkms `xkey`** | the desktop custodian and credential client |
| **go-trusted-platform** | re-exports the composed stack through `platformctl` |

Those verbs are documented in their own repositories. A divergence between the
locked CLI surface and the code there is raised against the locked surface, not
resolved by writing a local variant down here.

## Build-time targets

Not verbs, but the only command-line surface this feature has, and they are
worth stating because one of them is routinely over-read:

| Target | What it does | What it proves |
|---|---|---|
| `make test-seal` | `go test ./pkg/seal/...` | the unit suite |
| `make bench-seal` | `go test -bench ./pkg/seal/...` | throughput of the data path |
| `make fuzz-seal` | the four `FuzzBarrier*` targets | decode robustness, and that `Decrypt` stays non-discriminating |
| `make build-fips` | `go build` (± `-tags fips`) under `GOFIPS140` | **only that the tree compiles.** This module has no `main`, so no artifact is produced, and `GOFIPS140` alone never executes a cryptographic operation. Not a compliance gate. |
| `make verify-fips` | `GOFIPS140=latest GODEBUG=fips140=only go test -tags fips` over every package except `pkg/internal/transport/revocation/...` | that everything inside the FIPS boundary — the QUIC transport included — **runs** under approved-only enforcement |
| `make verify-fips-exempt` | `GOFIPS140=latest GODEBUG=fips140=on go test -tags fips` over exactly `pkg/internal/transport/revocation/...` | that the excluded tree is still gated, at `on` rather than `only` — the exemption buys a weaker mode, not an unmeasured one |

Building a FIPS-at-rest consumer:

```sh
GOFIPS140=latest go build -tags fips ./...
```

`-tags fips` makes go-quicraft FIPS **at rest, not on the wire** — the QUIC
transport is excluded from the gate and cannot be made approved from here. See
[`engineering.md`](engineering.md) §4.

## Non-verb operational levers

Everything this feature exposes to an operator arrives as configuration on
`BarrierConfig` through the embedding application, not as a flag:

| Lever | Field | Why an operator cares |
|---|---|---|
| algorithm | `Algorithm` | **pin it**; the default reads the local CPU and a restored data directory then fails to decrypt |
| replay floor | `MinEpoch` | refuses a rolled-back sealed blob |
| positional binding | `RequireContextAAD` | refuses a deployment that cannot bind frame position |
| FIPS | `RequireFIPS` | turns a silent non-FIPS downgrade into a startup refusal |
| durability | `StatePersister` | without it the node is read-only after its first restart |
| memory lock | `RLIMIT_MEMLOCK` on the process/container | protected key buffers need it |

See [`user.md`](user.md) for how these are set and what happens when they are
not.
