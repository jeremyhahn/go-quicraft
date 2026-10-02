# Seal and barrier: CLI

> **Status: NORMATIVE** · Scope: go-quicraft `pkg/seal` · Last verified: 2026-10-02
> Conflicts with this document are defects. Fix them here first.

No CLI: go-quicraft is a library and ships no binaries (there is no `cmd/`
directory), so this feature is reachable only through the Go API in
[`api.md`](api.md). A `barrier init` or `barrier unseal` verb would need a
passphrase prompt, a share-collection session, a TPM handle or a KMS credential,
all of which belong to custody layers this repository does not own.

The operator verbs belong to the layers above and are documented there:

| Owner | Responsibility |
|---|---|
| go-qrdb | barrier registry, tenant barriers, the `shamir` strategy, share custody, quorum, recovery, rekey, the Security-Officer ceremony, first-boot bootstrap |
| go-xkms | hardware and cloud strategies: TPM2, PKCS#11, AWS/GCP/Azure KMS, Vault |
| go-xkms `xkey` | the desktop custodian and credential client |

## Build and test targets

The only command-line surface this feature has:

| Target | What it does | What it proves |
|---|---|---|
| `make test-seal` | `go test ./pkg/seal/...` | the unit suite |
| `make bench-seal` | benchmarks in `./pkg/seal/...` | throughput of the data path |
| `make fuzz-seal` | the four `FuzzBarrier*` targets | decode robustness, and that `Decrypt` stays non-discriminating |
| `make build-fips` | `go build` (± `-tags fips`) under `GOFIPS140` | only that the tree compiles. The module has no `main`, so nothing is produced, and `GOFIPS140` alone never runs a cryptographic operation. Not a compliance gate. |
| `make verify-fips` | `GOFIPS140=latest GODEBUG=fips140=only go test -tags fips` over every package except `pkg/internal/transport/revocation/...` | everything inside the FIPS boundary, the QUIC transport included, runs under approved-only enforcement |
| `make verify-fips-exempt` | `GOFIPS140=latest GODEBUG=fips140=on go test -tags fips` over exactly `pkg/internal/transport/revocation/...` | the excluded tree is still gated, at `on` rather than `only` |

Building a FIPS consumer:

```sh
GOFIPS140=latest go build -tags fips ./...
```

`-tags fips` makes go-quicraft FIPS at rest and on the wire, with one
configurable exemption on the OCSP revocation path. See
[`engineering.md`](engineering.md) §4.

## Operational levers

Everything an operator controls arrives as `BarrierConfig` fields set by the
embedding application, not as flags:

| Lever | Field | Why an operator cares |
|---|---|---|
| algorithm | `Algorithm` | pin it; the default reads the local CPU, and a data directory restored onto a different CPU then fails to decrypt |
| replay floor | `MinEpoch` | refuses a rolled-back sealed blob |
| positional binding | `RequireContextAAD` | refuses a deployment that cannot bind frame position |
| FIPS | `RequireFIPS` | turns a silent non-FIPS downgrade into a startup refusal |
| durability | `StatePersister` | without it the node cannot encrypt after its first restart |
| memory lock | `RLIMIT_MEMLOCK` on the process or container | protected key buffers need it |

See [`user.md`](user.md) for how these are set and what happens when they are
not.
