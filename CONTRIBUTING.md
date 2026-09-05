# Contributing to QuicRaft

Thanks for your interest. QuicRaft is a Raft consensus library that other
systems store their data in, so the bar for changes is set by what a defect
costs rather than by how large the diff is. This document says what that means
in practice.

## Before You Start

**Open an issue first** for anything beyond a typo or an obvious bug fix.
Protocol changes, wire-format changes and new public API are worth discussing
before they are written -- not because the idea will be rejected, but because
the constraints are unusual enough that a design conversation saves a rewrite.

**Security vulnerabilities do not go here.** See [SECURITY.md](SECURITY.md).

## Development Setup

Requires Go **1.27.0** or later, and Docker for the integration suite.

```sh
git clone https://github.com/jeremyhahn/go-quicraft
cd go-quicraft
make test          # unit tests with the race detector
make ci            # fmt, no-mutants, vet, lint, security, build, test
make ci-full       # ci + every build-tagged suite + every nested module
```

Useful targets:

| Target | What it does |
|--------|--------------|
| `make test` | All unit tests, race detector on |
| `make test-<pkg>` | One package (`test-raft`, `test-waldb`, `test-seal`, …) |
| `make coverage` | Coverage over `./...`, enforced against `COVER_MIN` (91) |
| `make coverage-<pkg>` | Coverage for one package, same floor |
| `make lint` | golangci-lint, from `make install-lint` rather than PATH |
| `make install-lint` | Builds the pinned golangci-lint with this repo's Go, into `.tools/bin` |
| `make fmt` | Fails if anything is unformatted |
| `make vet` | `go vet ./...` |
| `make no-mutants` | Fails if a mutation marker survived in production code |
| `make security` | gosec, govulncheck, `go mod verify`, Trivy |
| `make integration-test` | Multi-node suite, in the devcontainer |
| `make ci-integration` | Same suite on the host, matching CI exactly |
| `make ci-netfault` | Partitions between live replicas (`quicraft_netfault`) |
| `make ci-errfs` | Filesystem fault injection (`quicraft_errfs`) |
| `make ci-fuzz-seeds` | Every fuzz target against its seeds and committed corpus |
| `make build-fips` | FIPS 140 compile check (`GOFIPS140=v1.0.0`), with and without `-tags fips` |
| `make verify-fips` | Runs the FIPS boundary under `GODEBUG=fips140=only`, QUIC transport included (excludes only `pkg/internal/transport/revocation/...`) |
| `make verify-fips-exempt` | Runs that excluded tree under `GODEBUG=fips140=on`; the OCSP CertID exemption is recorded in `docs/quic-transport/architecture.md` |
| `make bench-<pkg>` | Benchmarks for one package (smoke test only) |
| `make bench-compare` | Interleaved baseline-vs-working-tree comparison, benchstat |
| `make remote-<target>` | Run any of the above on a remote build host instead of your workstation |

`make ci` stops at `go test ./...` with no build tags, which excludes the
integration, netfault, errfs and fuzz suites entirely. `make ci-full` is what
`.github/workflows/ci.yml` runs and what to reproduce a red CI run with. Run it
before opening a pull request.

Run the heavy parts off-box. `make remote-<target>` rsyncs your working tree,
uncommitted edits included, to a remote build host and runs the target there,
streaming output back. Point it at your own machine with `OMEGA_USER`,
`OMEGA_HOST` and `OMEGA_DIR`:

```sh
make remote-ci-full
make remote-integration-test
make remote-tools     # one-time: install the scanners the host lacks
```

This is not politeness about shared hardware. Every target runs under `-race`
and the integration suites stand up multi-node QUIC clusters; on a workstation
that is also running an editor and a browser, that contention expires the
30-second catch-up waits in the snapshot tests, and the failures look exactly
like raft defects. Chasing those has cost days more than once.

## Code Style

Go style follows *Effective Go*; `gofmt` output is not negotiable. Beyond
that, this repository has a few local conventions:

- **Typed or sentinel errors, never `fmt.Errorf`.** Every error a caller might
  branch on is a package-level sentinel or a typed error with an `Is` method.
- **No `Get` prefix on getters.** Field `owner` gets `Owner()` and
  `SetOwner()`.
- **Small interfaces**, named for what they do (`Reader`, `Hasher`).
- **Map-based dispatch over long switches** where the set is open.
- **Design types so the zero value is useful.** Where it cannot be -- where
  zero must mean "unset" rather than "off" -- use a pointer and say so in the
  godoc. `BarrierConfig.NonceTracking` is the cautionary example: as a plain
  `bool` its zero value silently disabled nonce tracking.
- **Comment the *why*.** The code says what it does. A comment earns its place
  by recording the reasoning, the alternative that was rejected, or the bug
  that a line exists to prevent. Several of the more surprising constructs in
  this codebase are load-bearing precisely because of what their comment says;
  do not delete one because the code "looks unused" without establishing why
  it was written.

## Tests

Every change needs tests. Three rules matter more than the coverage percentage:

**A regression test must fail before the fix.** Write the test, revert the fix
hunk, confirm it fails, then put the fix back. A test that passes against both
is not a regression test, it is decoration, and this project has shipped some:
three at once, each with a claim attached that it covered the bug. Do not take
the fix author's word for it, your own included. If you cannot make it fail,
say so in the pull request rather than implying it was verified.

**Count passes, never the absence of failures.** A bisect here once read
"setup failed" as "0 failures" and walked the wrong direction. A run that
emitted no FAIL lines because it never compiled looks, in a grep, exactly like
a clean one. Assert on how many tests ran.

**Test the failure path, not only the success path.** The interesting bugs in a
consensus engine live in crash recovery, partial writes, partitions and
restarts. Two injectors exist for this: `test/errfs` fails filesystem
operations under the WAL and LogDB, and `test/netfault` drops messages between
replicas that are still running. Use them. A test that kills a process is not a
partition test, and with a suite that only knew how to kill processes, an
election quorum weakened from a strict majority to `n/2` passed 18 of 19
linearizability tests.

Coverage is enforced by the Makefile against a single floor, `COVER_MIN = 91`,
applied by `make coverage` and every `make coverage-<pkg>` target alike, and
checked again on the module total in CI. See
[docs/testing/overview.md](docs/testing/overview.md) for how the gate is
guarded and [docs/testing/infrastructure.md](docs/testing/infrastructure.md)
for the suites behind each build tag.

If you mutate production code to prove a test fails, restore it. `make ci` runs
`no-mutants`, which fails on an `if false {`, `MUTANT` or `MUTATION` left in a
non-test file, because a mutation that is never restored disables a guard
silently: the suite goes green, which is the opposite of a visible failure.

## Changes That Need Extra Care

### Raft protocol

Raft's safety properties are not negotiable: election safety, leader
append-only, log matching, leader completeness, state machine safety. A change
that touches election, log replication, membership or snapshot install must
state which properties it could affect and how it preserves them. Cite the
[extended Raft paper](docs/references/raft.pdf) or the
[Ongaro thesis](docs/references/OngaroPhD.pdf) by section where relevant.

The linearizability suite (`test/integration/linearizability`, Porcupine-based)
is the backstop, not the argument. Note what it does and does not cover. It
asserts an operation floor as well as a Porcupine verdict, because a history
that is empty, or all writes, or all reads, is trivially linearizable, and the
suite once passed with half the operations dropped. It does *not* cover
election safety: `TestLinearizability_4Node_ElectionSafety` exercises churn,
while the invariant itself is checked by
`TestElectionSafety_SpentTermCannotBeWonTwice` under `quicraft_netfault`.

### Wire and persisted formats

Read [docs/wire-protocol/engineering.md](docs/wire-protocol/engineering.md) and
[docs/compatibility.md](docs/compatibility.md) first. The
two rules that get broken most often:

1. **The transport version is a SUPREMUM.** Bump `proto.WireVersion` whenever
   any type reachable from `MessageBatch` or `SnapshotChunk` changes layout --
   *including* the embedded persisted ones. A compile-time assertion enforces
   the `Membership` half; the rest is judgement. This rule binds **from 1.0
   onward** -- it is what the pinning in rule 2 is protecting, and the first
   bump is where it gets used.
2. **Every format is PINNED at version 1 until the first production release.**
   A layout change during pre-release is made **in place, at version 1**. Do
   not add a decode path for an old layout and do not bump a version number.
   Update the consuming repositories (go-qrdb, go-xkms, go-quicbaas,
   go-trusted-ca) to the new layout in the same change, and wipe and re-sync
   dev clusters rather than migrating them. There is no deployed data and no
   deployed peer whose bytes anyone has promised to keep reading, so a decode
   path per development layout buys nothing and costs a version number per
   change.

   The version byte itself stays, and every gate on it is **exact equality** --
   the value is pinned, not the mechanism. It is what makes the 1.0 bump
   possible in band, and it turns a commit skew into a named refusal instead of
   fields read at the wrong offsets. Accept the consequence: two binaries from
   different pre-release commits may be mutually unreadable while both stamp 1.

   From 1.0 a bump means what it normally means -- a decoder taught the old
   layout, a migration story, and a supported window that widens rather than
   moves.

If a change alters bytes, update the layout tables in the same commit. The
tables are the specification an alternative implementation is written against,
and this project has already shipped a release where they had drifted.

`MarshalTo` must enforce the same limits as `UnmarshalFrom`. A codec that can
emit what it cannot parse is a durability bug for anything persisted.

### The hot path

Benchmark any change to append, replicate or apply, and include the numbers.
Interleave the runs (`make bench-baseline` / `bench-compare`) -- a before-run
followed by an after-run on a machine whose clock governor is `powersave` is
not a measurement. See
[docs/testing/benchmarking.md](docs/testing/benchmarking.md).

### Public extension points

`logdb.LogDB`, `seal.Barrier` and the `pkg/sm` interfaces are implemented
outside this repository. Changing them, or changing what an implementation must
persist or guarantee, is a breaking change even when it compiles -- and the
worst version compiles *and* runs, then diverges. Document the new requirement
and put it in the CHANGELOG under BREAKING.

## Pull Requests

- Branch from `main`.
- One logical change per pull request.
- Commit messages: [Conventional Commits](https://www.conventionalcommits.org/)
  (`fix(raft):`, `feat(seal):`, `docs:`, `test:`, `ci:`, `build:`).
- Explain **why**, not just what. If you rejected an alternative approach, say
  which and why -- that reasoning is the most useful part of the review.
- Update `CHANGELOG.md` under `[Unreleased]`. Anything that changes behaviour,
  a signature, or a byte layout goes under `### BREAKING`, including changes
  that are source-compatible but break at run time. Say so explicitly when
  that is the case.
- `make ci-full` must pass. `make ci` alone leaves every build-tagged suite
  unrun.

## Documentation

Documentation lives in `docs/` and is treated as part of the change, not as
follow-up work.

**Verify against the code, never against another document.** Regenerate a
layout table by reading the `MarshalTo` body. Check a signature with `go doc`.
Confirm a default in `SetDefaults`, which is authoritative over the field's
own godoc when they disagree. Where two documents disagree,
`docs/compatibility.md` has historically been the correct one.

**If you cannot verify a claim, delete it rather than restate it.** An
unverifiable sentence carried forward is how the documentation drifted in the
first place.

**A documented `make` target must exist.** Check it against the Makefile, not
against another document. An audit in August 2026 found six targets described
across the testing docs (`integration-test-bootstrap`, `test-discovery`,
`test-bootstrap`, `bench-discovery`, `bench-bootstrap`, `bench`) that do not
exist, alongside a `test/integration/bootstrap/` suite that does not exist
either. `grep -n '^target-name:' Makefile` costs nothing.

## License

By contributing you agree that your contributions are licensed under the
[Apache License 2.0](LICENSE).
