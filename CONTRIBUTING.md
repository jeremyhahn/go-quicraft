# Contributing to QuicRaft

QuicRaft is a consensus library that other systems keep their data in, so the
bar for a change is set by what a defect would cost, not by the size of the
diff. This guide covers the workflow and the rules that matter most.

## Before you start

Open an issue first for anything beyond a typo or an obvious bug fix. Protocol
changes, format changes and new public API benefit from a design conversation
before code is written; the constraints below are unusual enough that it
usually saves a rewrite.

Do not report security vulnerabilities in public issues. See
[SECURITY.md](SECURITY.md).

## Setup

You need Go 1.27.4 or later (see `go.mod`), and Docker for the containerized
integration suite.

Dependencies under `github.com/jeremyhahn/` (go-enclave, and the Dragonboat
fork the benchmarks use) come from the project's Git forge. Configure Go to
fetch them from there before the first build, as described in the
[README](README.md#install).

```sh
git clone https://github.com/jeremyhahn/go-quicraft
cd go-quicraft
make install-tools   # golangci-lint, gosec, govulncheck, trivy, nancy, actionlint
make test
```

`make install-lint` builds the pinned golangci-lint with your Go toolchain into
`.tools/bin`, and `make lint` uses that binary rather than whatever is on your
`PATH`.

## Make targets

| Target | What it does |
|---|---|
| `make test` | All unit tests with the race detector |
| `make test-<pkg>` | One package, e.g. `test-raft`, `test-waldb`, `test-seal` |
| `make fmt` | Fails if `gofmt -l` reports any file |
| `make vet` | `go vet ./...` |
| `make lint` | `fmt` and `go vet`, golangci-lint in every module, actionlint over the workflows, then `api-check` |
| `make security` | gosec, govulncheck, `go mod verify`, Trivy |
| `make coverage` | Coverage over `./...`, enforced against `COVER_MIN` (91%) |
| `make coverage-<pkg>` / `make coverage-packages` | One package, or every package under `pkg/`, against the same floor |
| `make ci` | `fmt`, `no-mutants`, `check-tool-versions`, `vet`, `lint`, `security`, `build`, `test` |
| `make ci-full` | `ci` plus every nested module, built and vetted natively and for five other platforms, the integration, netfault, errfs, fuzz-seed and fuzz-mutation suites, and the FIPS checks |
| `make integration-test` | Multi-node integration suite in Docker |
| `make ci-integration` | The same suite on the host, as CI runs it |
| `make ci-netfault` | Network partitions between live replicas (`quicraft_netfault` tag) |
| `make ci-errfs` | Filesystem fault injection (`quicraft_errfs` tag) |
| `make ci-fuzz-seeds` | Every fuzz target against its seed corpus |
| `make fuzz-all` | Run the fuzzers |
| `make build-fips` / `make verify-fips` | FIPS 140-3 compile check, and the FIPS boundary under `GODEBUG=fips140=only` |
| `make bench-<pkg>` | Benchmarks for one package |
| `make bench-baseline` / `make bench-compare` | Interleaved baseline-vs-working-tree comparison with benchstat |
| `make remote-<target> REMOTE_HOST=<host>` | Run any target on another machine over ssh and rsync |

`make ci` runs `go test` with no build tags, so it skips the integration,
fault-injection and fuzz suites. `make ci-full` is the local equivalent of the
CI workflow. Run it before opening a pull request.

The full suite runs under `-race` and starts multi-node QUIC clusters. On a
busy workstation the contention can expire the catch-up timeouts in the
snapshot tests, and those failures look like Raft bugs. If that happens, rerun
on an idle machine (`make remote-ci-full REMOTE_HOST=…`) before you start
debugging.

### Running the suites in the dev container

`make docker-make` builds the dev image (`.devcontainer/Dockerfile.dev`, which
copies the source in) and runs one make target inside it, `ci` unless you name
another:

```sh
cp .devcontainer/.env.example .devcontainer/.env   # then fill in, chmod 600
make docker-make                                   # make ci in the container
make docker-make DOCKER_MAKE_TARGET=test
DOCKER_HOST=ssh://user@bench-host.example make docker-make DOCKER_MAKE_TARGET=ci-full
make docker-clean                                  # remove this checkout's images
```

`.devcontainer/.env` holds your own Gitea account and access token as the four
`GOPRIVATE`/`GIT_CONFIG_*` variables. It is the only place they live: the image
builds mount it as a BuildKit secret for `go mod download` (never written to a
layer), and the container and the compose services load it as their
environment. Without it the build fails at go-enclave, which only the forge
serves. Building needs the `docker-buildx` plugin.

The dev image also carries `.git` (its own `Dockerfile.dev.dockerignore` keeps
it; every other image leaves it out), because the releasability gate in
`make test` reads the git index. Run it from a regular clone: in a `git
worktree`, `.git` is only a pointer to a path on your machine.

Nothing is bind-mounted, so the same command works against a remote daemon
through `DOCKER_HOST`. Images are tagged per checkout, so checkouts sharing a
daemon do not overwrite each other.

## Code style

Follow *Effective Go*, and run `gofmt`. Local conventions:

- Every error a caller might branch on is a package-level sentinel or a typed
  error. `fmt.Errorf` is only used to wrap one with `%w`.
- Getters have no `Get` prefix: field `owner` gets `Owner()` and `SetOwner()`.
  `Host` follows the rule too (`ShardMembership`, `LeaderID`, `NoOPSession`);
  only `contrib/dragonboat` keeps Dragonboat's `Get`-prefixed names.
- Keep interfaces small and name them for what they do.
- Make the zero value useful. Where zero must mean "unset" rather than "off",
  use a pointer and say so in the doc comment (`BarrierConfig.NonceTracking`
  is a `*bool` so that nil means enabled).
- Comment the why: the reasoning, the rejected alternative, or the failure a
  line prevents. Before deleting code that looks unused, find out why it was
  written.

## Tests

Every change needs tests. Three rules matter more than the coverage number:

- **A regression test must fail before the fix.** Write the test, revert the
  fix, confirm it fails, then restore the fix. If you cannot make it fail, say
  so in the pull request.
- **Count passing tests; don't rely on the absence of failures.** A run that
  never compiled prints no `FAIL` lines either. Assert how many tests ran.
- **Test the failure paths.** The interesting bugs in a consensus engine are in
  crash recovery, partial writes, partitions and restarts. `test/errfs` fails
  filesystem operations under the WAL and LogDB, and `test/netfault` drops
  messages between replicas that keep running. A test that kills a process is
  not a partition test.

If you mutate production code to prove a test fails, restore it. `make ci` runs
`no-mutants`, which fails on an `if false {`, `MUTANT` or `MUTATION` left in a
non-test file.

`make coverage` and `make coverage-packages` hold the module and every package
under `pkg/` to `COVER_MIN` (91%); CI applies a 90% floor to both.
[docs/testing/overview.md](docs/testing/overview.md) describes the gates, and
[docs/testing/infrastructure.md](docs/testing/infrastructure.md) the suites
behind each build tag.

## Changes that need extra care

### Raft protocol

Raft's safety properties are not negotiable: election safety, leader
append-only, log matching, leader completeness and state machine safety. A
change that touches election, replication, membership or snapshot install must
say which properties it could affect and how it preserves them, citing the
[extended Raft paper](docs/references/raft.pdf) or the
[thesis](docs/references/OngaroPhD.pdf) by section where relevant.

The Porcupine linearizability suite (`test/integration/linearizability`) is a
backstop, not the argument. It asserts a minimum operation count as well as the
verdict, because an empty or one-sided history is trivially linearizable. It
does not check election safety: `TestLinearizability_4Node_ElectionSafety`
exercises churn, and the invariant itself is checked by
`TestElectionSafety_SpentTermCannotBeWonTwice` under `quicraft_netfault`.

### Wire and persisted formats

Read [docs/wire-protocol/engineering.md](docs/wire-protocol/engineering.md) and
[docs/compatibility/](docs/compatibility/architecture.md) first.

- **Every format version is pinned at its current value until the first
  stable release**: 1 for every format except the sealed-state MAC, which is 2.
  A pre-1.0 layout change is made in place at that value. Do not add a decode path
  for an old layout and do not bump a version constant. Development data
  directories are wiped and re-synced, not migrated. The version byte stays,
  and every check on it is exact equality, so a mismatch between builds is
  refused rather than parsed at the wrong offsets.
- **From 1.0, `proto.WireVersion` is a supremum.** It must be bumped whenever
  any type reachable from `MessageBatch` or `SnapshotChunk` changes layout,
  including the embedded persisted types. A compile-time assertion covers the
  `Membership` half of that rule.
- If a change alters bytes, update the layout tables in the same commit. They
  are the specification an independent implementation is written against.
- `MarshalTo` must enforce the same limits as `UnmarshalFrom`. A codec that can
  write what it cannot read is a durability bug for anything persisted.

### The hot path

Benchmark any change to append, replicate or apply, and include the numbers.
Interleave the runs with `make bench-baseline` and `make bench-compare`. A
before-run followed by an after-run on a machine with a `powersave` CPU
governor is not a measurement. See
[docs/testing/benchmarking.md](docs/testing/benchmarking.md).

### Public extension points

`logdb.LogDB`, `seal.Barrier` and the `pkg/sm` interfaces are implemented
outside this repository. Changing them, or what an implementation must persist
or guarantee, is a breaking change even when it compiles. Document the new
requirement and list it in the changelog as breaking.

## Pull requests

External pull requests are welcome. A maintainer reviews every pull request,
and nothing is merged without that review and a green CI run. Expect questions
on anything that touches the Raft protocol, a persisted format or the security
model.

- Branch from `main`, one logical change per pull request.
- Use [Conventional Commits](https://www.conventionalcommits.org/):
  `fix(raft):`, `feat(seal):`, `docs:`, `test:`, `ci:`, `build:`.
- Explain why, not only what. If you rejected an alternative, say which one and
  why.
- Add an entry to `CHANGELOG.md` under `[Unreleased]`. Anything that changes
  behaviour, a signature or a byte layout is marked BREAKING, including changes
  that compile but break at run time.
- `make ci-full` must pass.

## Releasing

Maintainers cut releases with the Makefile recipe only, never with a hand-made
tag:

```bash
make release-alpha VERSION=vX.Y.Z-alpha   # or: make release VERSION=vX.Y.Z
make release-candidate VERSION=vX.Y.Z-rc.N   # a release candidate, N >= 1
```

Run it on a clean `main` whose `CHANGELOG.md` has a `## [X.Y.Z-alpha]`
(`[X.Y.Z-rc.N]` for a candidate, `[X.Y.Z]` for `make release`) heading. The recipe writes `VERSION`, pins `contrib/dragonboat/go.mod`'s
`require github.com/jeremyhahn/go-quicraft` line to the version being cut,
commits both, creates the annotated root, `test/performance/` and
`contrib/dragonboat/` tags, verifies them, pushes the commit and all three tags
atomically, and reads the remote back. If any step before the push fails,
nothing is pushed.

`contrib/dragonboat` is a module of its own, so `go get
github.com/jeremyhahn/go-quicraft/contrib/dragonboat@vX.Y.Z-alpha` resolves the
`contrib/dragonboat/vX.Y.Z-alpha` tag. A consumer ignores the module's
`replace github.com/jeremyhahn/go-quicraft => ../..`, which stays for in-tree
development, and resolves the parent through its `require`, which the release
commit points at the same version. Between releases that `require` names the
last release; the `replace` keeps the in-tree build on the working tree.

| Variable | Default | Meaning |
|---|---|---|
| `RELEASE_GIT` | `git` | The command every git write runs through. It is split on whitespace, so a signing wrapper can take leading arguments: `RELEASE_GIT='/path/to/wrapper project'` |
| `RELEASE_ALLOWED_SIGNERS` | `.release-allowed-signers` | A git allowed-signers file (`gpg.ssh.allowedSignersFile` format), relative to the checkout root |
| `RELEASE_REMOTE` | `dev` | The remote the release is pushed to |

With plain git, signing comes from your own configuration: `gpg.format=ssh`,
`user.signingkey`, `commit.gpgsign=true` and `tag.gpgsign=true`. Before
pushing, the recipe requires the release commit's author e-mail to be a
principal in the allowed-signers file (matched literally), and the commit and
every tag to carry SSH signatures that verify against a key the file lists for
that principal. A missing file stops the recipe before anything is written.
The default file belongs to the private repository and is not published, so a
public maintainer keeps their own and passes it:

```bash
echo "you@example.org $(cat ~/.ssh/id_ed25519.pub)" > ~/release-signers
make release-alpha VERSION=vX.Y.Z-alpha RELEASE_REMOTE=origin \
  RELEASE_ALLOWED_SIGNERS=$HOME/release-signers
```

`RELEASE_GIT` is trusted to authenticate and for nothing else. It runs the
writes and the push, because a signing wrapper is also what carries the push
credentials; every read is plain git. Before pushing, plain git resolves the
commit and its tag objects to object IDs and checks that the commit is
exactly the `VERSION` change and the `contrib/dragonboat` parent pin on top of
the `main` you started from and that
each tag targets it under its own name. The signatures are verified on those
IDs, and the push names those IDs, not refs. After the push, plain git reads
the remote back at the URL resolved before `RELEASE_GIT` first ran, and the
release fails unless `main` and every tag hold exactly the verified objects.
If that read-back fails after a successful push, something was published:
inspect the remote before announcing or re-running anything. Your plain git
must be able to read `RELEASE_REMOTE`; the duplicate-tag check needs that too.

## Documentation

Documentation in `docs/` is part of the change, not follow-up work.

- Verify against the code, not against another document. Rebuild a layout
  table from the `MarshalTo` body, check a signature with `go doc`, and confirm
  a default in `SetDefaults`, which wins over a field's doc comment when they
  disagree.
- If you cannot verify a claim, delete it rather than carry it forward.
- A documented `make` target must exist: `grep -n '^target-name:' Makefile`.

## License

By contributing you agree that your contributions are licensed under the
[Apache License 2.0](LICENSE).
