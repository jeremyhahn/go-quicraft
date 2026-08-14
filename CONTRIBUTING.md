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

Requires Go **1.26.6** or later, and Docker for the integration suite.

```sh
git clone https://github.com/jeremyhahn/go-quicraft
cd go-quicraft
make test          # unit tests with the race detector
make ci            # fmt, vet, lint, security scans, build, test
```

Useful targets:

| Target | What it does |
|--------|--------------|
| `make test` | All unit tests, race detector on |
| `make test-<pkg>` | One package (`test-raft`, `test-waldb`, `test-seal`, …) |
| `make coverage` | Coverage across all packages |
| `make coverage-<pkg>` | Coverage for one package |
| `make lint` | golangci-lint |
| `make fmt` | Fails if anything is unformatted |
| `make vet` | `go vet ./...` |
| `make security` | gosec, govulncheck, module verification, Trivy |
| `make integration-test` | Multi-node suite, in Docker |
| `make build-fips` | FIPS 140 build (`GOFIPS140=v1.0.0`) |
| `make bench` | Benchmarks |

`make ci` is what CI runs. Run it before opening a pull request.

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

Every change needs tests. Two rules matter more than coverage percentage:

**A regression test must fail before the fix.** Write the test, confirm it
fails against the unfixed code, then apply the fix. A test that passes against
both is not a regression test, it is decoration -- and this project has shipped
some. If you cannot make it fail, say so in the pull request rather than
implying it was verified.

**Test the failure path, not only the success path.** The interesting bugs in a
consensus engine live in crash recovery, partial writes, partitions, and
restarts. `test/errfs` exists for injecting filesystem errors; use it.

Coverage is enforced per package by the Makefile against per-package
thresholds. Aggregate coverage is 91.5%; see
[docs/testing/overview.md](docs/testing/overview.md) for the current
per-package figures and which packages sit below the aggregate.

## Changes That Need Extra Care

### Raft protocol

Raft's safety properties are not negotiable: election safety, leader
append-only, log matching, leader completeness, state machine safety. A change
that touches election, log replication, membership or snapshot install must
state which properties it could affect and how it preserves them. Cite the
[extended Raft paper](docs/references/raft.pdf) or the
[Ongaro thesis](docs/references/OngaroPhD.pdf) by section where relevant.

The linearizability suite (`test/integration/linearizability`, Porcupine-based)
is the backstop, not the argument.

### Wire and persisted formats

Read [docs/protocol/wire-format.md](docs/protocol/wire-format.md) and
[docs/operations/compatibility.md](docs/operations/compatibility.md) first. The
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
[docs/performance/benchmarking.md](docs/performance/benchmarking.md).

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
- `make ci` must pass.

## Documentation

Documentation lives in `docs/` and is treated as part of the change, not as
follow-up work.

**Verify against the code, never against another document.** Regenerate a
layout table by reading the `MarshalTo` body. Check a signature with `go doc`.
Confirm a default in `SetDefaults`, which is authoritative over the field's
own godoc when they disagree. Where two documents disagree,
`docs/operations/compatibility.md` has historically been the correct one.

**If you cannot verify a claim, delete it rather than restate it.** An
unverifiable sentence carried forward is how the documentation drifted in the
first place.

## License

By contributing you agree that your contributions are licensed under the
[Apache License 2.0](LICENSE).
