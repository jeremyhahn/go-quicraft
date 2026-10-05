# Testing infrastructure

> **Status: NORMATIVE** · Scope: go-quicraft's test suites, build tags, injectors
> and gates: `Makefile`, `test/`, `.github/workflows/ci.yml` · Last verified: 2026-10-02
> Conflicts with this document are defects. Fix them here first.

What each suite is built from, which build tag gates it, and why the gates are
shaped as they are. Day-to-day commands, test patterns and the target reference
are in [Testing overview](overview.md).

## Two rules that outrank coverage

**A regression test must be seen to fail before the fix.** Write the test,
revert the fix hunk, watch it fail, then restore the fix. A regression test that
passes against unfixed code proves nothing, and an author's claim that it covers
the bug is not evidence; the observed failure is. If you cannot make a test
fail, say so in the pull request.

**Count passes, never the absence of failures.** A run that produced no FAIL
lines because it never compiled looks identical, in a grep, to a clean run.
Assert on the number of tests that ran.

Both rules are why several gates below fail closed on missing data instead of
passing quietly.

## Suites and their build tags

| Suite | Tag | Where | Runner |
|---|---|---|---|
| Unit | none | `pkg/...`, plus the untagged tests under `test/` | `make test`, `make test-<pkg>` |
| Integration | `integration` | `test/integration/{raft,host,e2e,linearizability}` | `make integration-test`, `make ci-integration` |
| Network partition | `integration quicraft_netfault` | `test/integration/e2e`, `test/integration/host` | `make ci-netfault` |
| I/O fault injection | `quicraft_errfs` | `test/errfs` | `make errfs-waldb`, `make ci-errfs` |
| Fuzz | `quicraft_fuzz` | six packages under `pkg/` | `make fuzz-<pkg>`, `make ci-fuzz-seeds`, `make ci-fuzz-mutation` |
| FIPS | `fips` | `./...` | `make build-fips`, `make verify-fips`, `make verify-fips-exempt` |

The Go toolchain does not warn when a build constraint excludes a file; it just
compiles a smaller program. `make ci` runs only the untagged suite. `make
ci-full` runs every tag and is the target to reproduce a red CI run with.

### Integration tests

Four packages under `test/integration/`, all behind `//go:build integration`:

| Package | Tests | Covers |
|---|---|---|
| `raft/` | 31 | elections, replication, failover, snapshots, read paths, multi-shard, stress, 3N and 5N |
| `host/` | 47 (+1 under `quicraft_netfault`) | public Host API, LeaseRead, membership change, observer promotion, rolling restart, snapshot under load |
| `e2e/` | 60 (+5 under `quicraft_netfault`) | cluster formation, partitions, quorum loss, barrier encryption, node replacement |
| `linearizability/` | 26 | Porcupine verification, chaos soaks, read-your-writes, proposal-key incarnations |

Counts include each suite's budget pin test.

`make integration-test` runs all four inside the devcontainer;
`make ci-integration` runs the same suite on the host, with the same command as
the `integration` job in `.github/workflows/ci.yml`.

Both pass `-p 1`, and that is load-bearing. Each package stands up multi-node
QUIC clusters under `-race`; four at once starve the CPU enough that the
30-second snapshot catch-up waits expire. Every package is stable standalone.
Do not remove `-p 1` to shorten the job.

Because `-p 1` makes every package share one deadline, the timeouts are large:
900s in the devcontainer and 1800s in CI, where a shared runner is slower. A
smaller cap fails the suite with a timeout panic and zero test failures, which
reads as a product bug instead of a budget.

### E2E over Docker Compose

`make integration-test-e2e` brings up
`.devcontainer/docker-compose.integration.yml`: one `test-runner` container
running a binary compiled from `./test/integration/e2e/...`. The e2e package
builds its own clusters (`newTestCluster`: real `quicraft.Host` instances over
real QUIC with mTLS on loopback), so the container adds the pinned toolchain
and the entrypoint's tuned UDP buffers, not topology. There is no node server
binary; the only `package main` in the tree is the benchmark orchestrator.

The build step in `.devcontainer/Dockerfile.integration` compiles under
`set -eux` with no stderr suppression and no fallback, then asserts the binary
is non-empty and lists at least one test. Both checks matter: `go test -c` on a
package with no test files writes nothing and still exits 0, and a zero-byte
executable run through a shell exits 0 having tested nothing.
`test/buildgates/dockerfile_integration_test.go` pins that shape.

### Network partition injection

`test/netfault` drops messages between live replicas, behind the
`quicraft_netfault` build tag.

A process kill (`stopNode`) cannot build the most dangerous configuration in
Raft: a live leader deposed by a quorum it can no longer reach. A killed node
holds no leadership belief and comes back with cold state. With only
`stopNode`, weakening the election quorum from a strict majority to `n/2`
passed 18 of the 19 linearizability tests then in the suite; the partition
primitive catches the same mutant in about a second.

| Primitive | Effect |
|---|---|
| `Isolate(replica)` / `Rejoin(replica)` | cuts one replica off in both directions without stopping it; it keeps its term, role and ticker |
| `Partition(groupA, groupB)` | blocks both directions across a cut; each side stays internally connected |
| `BlockLink(from, to)` / `HealLink(from, to)` | one directed edge, for asymmetric partitions |
| `Heal()` | removes every fault; register it with `t.Cleanup`, because the state is process-global |
| `BlockedCount()` | number of installed faults |

Drops apply on the sending side, which can express any directed cut, and they
are silent: connections stay up and no error surfaces, as on a real network.

Production cost is zero. `pkg/internal/transport/netfault_off.go` defines
`netFaultEnabled` as a false constant, so the compiler removes every
`if netFaultEnabled && netFaultDrop(...)` site, and only `netfault_on.go`,
itself behind the tag, imports `test/netfault`.

Every netfault test asserts `netfault.BlockedCount()` is non-zero before it
proceeds, so building without the tag fails loudly instead of exercising a
healthy cluster.

Six tests use it:

| Test | Package |
|---|---|
| `TestNetPartition_TwoOfFourCannotElect` | e2e |
| `TestNetPartition_DeposedLeaderStaysAliveAndCannotCommit` | e2e |
| `TestNetPartition_AsymmetricLeaderCannotBeHeard` | e2e |
| `TestElectionSafety_SpentTermCannotBeWonTwice` | e2e |
| `TestNetPartition_ProposeToLeader_ResolvesPastAnIsolatedIncumbent` | e2e |
| `TestNetPartition_ReadFromLeader_ResolvesPastAnIsolatedIncumbent` | host |

`make ci-netfault` and CI's `netfault` job run both packages (`NETFAULT_PKGS`),
each whole rather than filtered by test name. The target fails if a package
under `test/integration` carries the `quicraft_netfault` tag but is missing
from that list.

The older partition tests (`partition_test.go`, `asymmetric_partition_test.go`,
`quorum_loss_test.go`) still use `stopNode`. They are worth keeping but do not
cover the deposed-live-leader case.

The injector's own seven tests in `test/netfault/netfault_test.go` are
untagged and run under `make test`: `Drop` is false with nothing injected,
`BlockLink` is directional, `Isolate` cuts both ways, `Partition` cuts across
and not within, `Heal` clears everything, mutators are safe under concurrent
reads, and the shard ID is not consulted.

### I/O fault injection

`test/errfs` wraps the filesystem the WAL and LogDB write through and fails
operations on demand: the Nth call, only a named operation, a bounded number of
times, or a short write that disturbs the delegate before returning an error.

Two tiers:

- 23 tests behind `quicraft_errfs` (`waldb_test.go`, 21; `durability_test.go`,
  2) drive the WAL and LogDB through injected failures: torn tails and repair,
  a failed fsync losing only unacknowledged data, mid-entry write failure, read
  and `Stat` failure during replay, `OpenFile` and `MkdirAll` failure,
  compaction remove and readdir failure, double faults across write and
  rollback, and idempotent recovery.
- 13 untagged tests in `errfs_unit_test.go` and `harness_test.go` test the
  injector: the `injectAt` argument to `New` fires on exactly the Nth operation, `OnlyOps` restricts
  and rejects unknown operations, `Times` bounds the budget, a short write
  disturbs the delegate, and every non-injecting operation delegates. They run
  in `make test`, because an injector that silently stopped injecting would
  make the 23 tagged tests pass for the wrong reason.

`make errfs-waldb` runs the tagged suite with `-v`, which you want when one
fails: the test name is the only way to tell which injection point produced the
failure. `make ci-errfs` is the same without `-v`, matching the `errfs` job in
ci.yml and the `errfs` gate in release.yml. Both keep `-race`, because a torn
recovery path shows up as a race rather than a wrong answer.

### Fuzzing

37 targets behind `quicraft_fuzz`:

| Package | Targets | Surface |
|---|---|---|
| `pkg/proto` | 13 | Entry, Message, MessageBatch, State, Membership, ConfigChange, Snapshot: round-trip and crash |
| `pkg/logdb/waldb` | 9 | WAL record codec and recovery |
| `pkg/internal/transport` | 5 | frame header decode and the message-frame receive path |
| `pkg/internal/session` | 4 | session encoding boundaries |
| `pkg/seal` | 4 | AEAD encrypt/decrypt across epochs |
| `pkg/internal/raft` | 2 | adversarial step sequences, log recovery |

Every target has `f.Add` seeds. The committed corpus is 39 files under
`pkg/proto/testdata/fuzz/<Target>/`; nightly fuzzing uploads crashers as
artifacts, and committing one makes it a permanent regression test.

Three different things are called fuzzing here:

- `make fuzz-<pkg>` and `make fuzz-all` mutate each target for `FUZZ_TIME`
  (default 60s, or an execution count such as `20000x`), for local use.
  `go test -fuzz` accepts one package and one target per invocation, so each
  target lists its package's targets and runs them one at a time, stopping at
  the first failure. `ci-fuzz-mutation` uses the same helpers.
- `make ci-fuzz-seeds` runs `-run=Fuzz` without `-fuzz` over `./...`: each
  target once per seed and once per corpus file, deterministically. It is the
  PR gate. It walks `./...` so a new target in a seventh package cannot go
  unrun.
- `make ci-fuzz-mutation` is the release gate: a smoke check, not a search. Each
  target gets a fixed execution count (`-fuzztime=<N>x`), the nightly
  campaign's per-target budget divided by `FUZZ_MUT_DIVISOR` (6). A package
  without a calibrated budget fails the gate instead of running unbounded.

The unbounded search is `.github/workflows/fuzz.yml` on a nightly schedule,
deliberately off the PR path: a crash it finds depends on the RNG schedule, and
failing unrelated pull requests at random is how a gate stops being trusted.
Both the nightly and the release gate budget by execution count, not duration.
With a duration budget Go installs a deadline context, and its coordinator can
report a bare "context deadline exceeded" at shutdown that reads like a crash
and is not one; with a count there is no deadline to race.

### Linearizability

`test/integration/linearizability`, Porcupine-based, 26 tests. Clients issue
put/get operations against a KV state machine while faults are applied, and the
recorded history is checked for a valid sequential order.

| Test | Nodes | Clients | Duration | Fault |
|---|---|---|---|---|
| `3Node_Normal` | 3 | 8 | 5s | none |
| `5Node_Normal` | 5 | 8 | 5s | none |
| `3Node_SingleKeyContention` | 3 | 8 | 5s | none, one key |
| `3Node_LeaseRead` | 3 | 8 | 5s | none, LeaseRead path |
| `3Node_SessionProposal` | 3 | 4 | 5s | none, at-most-once sessions |
| `3Node_LeaderFailover` | 3 | 4 | 5s | leader stopped |
| `5Node_MinorityDown` | 5 | 8 | 8s | two followers stopped |
| `3Node_SnapshotDuringLoad` | 3 | 8 | 10s | snapshot under load |
| `4Node_ElectionSafety` | 4 | 4 | 12s | leadership churn |
| `3Node_FollowerRestart` | 3 | 4 | 12s | follower restarted on its own WAL |
| `3Node_LeaderRestart` | 3 | 4 | 12s | leader restarted, rejoins as follower |
| `5Node_MultipleLeaderFailovers` | 5 | 4 | 15s | two leaders stopped in sequence |
| `5Node_MembershipAddRemove` | 5 | 4 | 15s | add then remove a node |
| `5Node_RollingRestart` | 5 | 4 | 20s | each node restarted, never two down |
| `5Node_HighThroughput` | 5 | 16 | 30s | none |

The rest of the package: `TestRestartedReplicaResumesReplication`
(`catchup_probe_test.go`, a restarted replica starved by a full inflight
window); three soaks in `soak_chaos_test.go` (`ChaosUnderSustainedLoad`,
`RollingRestartUnderLoad`, `LeaderCloseDoesNotStallWrites`);
`TestSoak_RollingRestart_ReadYourWrites`;
`TestRestart_ProposalKeysDoNotCollideAcrossIncarnations`, which pins that
proposal keys stay unique across restarts; and the history-pruning and
partition-diagnosis tests behind the checker.

What makes the suite meaningful is the operation floor. An empty history, or
one with only writes or only reads, is trivially linearizable.
`checkLinearizability` fails unless the run recorded at least
`floorFor(numClients, active)` writes and as many reads: 30% of a measured
baseline of 5.4 completed writes per client-second. Chaos tests pass a reduced
`active` window, because elections and catch-up legitimately stall clients. At
30% the tightest test keeps a 3.3x margin, while a mutation that collapses
throughput lands one to three orders of magnitude below the floor. The checker
also fails on any client-observed protocol violation, independent of the
Porcupine verdict.

`TestLinearizability_4Node_ElectionSafety` exercises churn but does not prove
election safety. `TestElectionSafety_SpentTermCannotBeWonTwice`, under
`quicraft_netfault`, spends a term's votes on a candidate that cannot win and
asserts nothing else wins that term.

## Shared test packages

Every directory under `test/`:

| Directory | What it is |
|---|---|
| `integration/` | the four integration suites above |
| `errfs/` | the I/O fault-injecting filesystem and its suites |
| `netfault/` | the live-replica partition injector |
| `netport/` | reserves loopback UDP ports for hosts that do not exist yet, without the bind-close-rebind race of `127.0.0.1:0` |
| `testpki/` | one CA per cluster and one keypair per host, so every host has a distinct node identity |
| `electionbudget/` | the election wait budgets (`Round`, `LeaderElection`, `Failover`) every suite derives from its own `ElectionRTT` and `RTTMillisecond` |
| `proposer/` | submits proposals and linearizable reads to whichever replica leads at the moment of the call |
| `membership/` | applies membership changes through the current leader and treats the retry of an already committed change as success |
| `lockedmem/` | reads the kernel's view of this process's mappings (Linux), so tests can assert a page is locked or read-only |
| `buildgates/` | untagged tests for the parts of the build that are not Go: analyser version pins, CI gate wiring, the integration Dockerfile, FIPS and fuzz budgets, release inputs |
| `releasability/` | the public-releasability gate; see [public-releasability.md](../release/public-releasability.md) |
| `performance/` | the cross-system benchmark framework (separate modules) |

Each suite keeps a small pin test (`TestBudgetsDescribeTheClusterTheyBudgetFor`)
asserting that its own config builders set the `ElectionRTT` the budgets were
derived from.

`pkg/internal/engine` has its own, unrelated `newTestCluster` for in-process
engine tests.

## Gates shaped by past failures

### The coverage gate fails closed

`COVER_MIN` is 91. Every `coverage-*` target and `make coverage` (`./...`) fail
below it, and `make coverage-packages` applies it to every package under `pkg/`
from the same profile. CI's `test` job runs the same total and per-package
checks at `COVERAGE_THRESHOLD = 90`, one point lower so a single package's
flaky test (which contributes no coverage when it fails) does not fail the gate
twice.

`check-coverage` fails on a missing or empty profile and on any value that is
not a number before it compares. Without those guards an empty percentage made
`bc` print nothing, the shell test degenerated to `[ -eq 1 ]`, and the gate
passed exactly when it had no data. It reads the total with
`awk '/^total:/'`, because `grep total` also matches functions whose names
contain "total". CI duplicates the guards so the two gates cannot drift apart.

`-coverpkg=./...` is not passed. An untested package is already in the
denominator at 0%; `-coverpkg` only adds credit for statements covered by
another package's tests, which moved the same tree from 91.3% to 92.0%. Against
an unchanged floor that loosens the gate, so adopting it is a threshold
decision, not a wiring fix.

### The mutation-marker guard

`make no-mutants` runs early in `make ci`. It greps `pkg/`, `test/` and
`contrib/` for `if false {`, `MUTANT` and `MUTATION` outside `_test.go` files
and fails on any hit.

Mutation testing is how a fix is verified here, and it edits production code. A
mutation that is never restored is invisible, because a disabled guard makes
tests pass. An `if false {` around the WAL's rollback fsync once reached a
commit with every suite green, because the site had no test, which is why it was
being mutated. Test files are excluded: `if false` is legitimate there, and a
mutation in a test cannot weaken production behaviour.

### The nested-module gate

`make ci-modules` runs `go build` and `go vet` in every directory holding a
`go.mod`, skipping the directories the go tool ignores (names starting with `.`
or `_`, and `testdata`), and collecting failures instead of stopping at the first, so one broken
module cannot hide a second.

`GOWORK=off` is exported at the bottom of the Makefile and set in the workflow
env. Inside a workspace, nested modules resolve the root through the workspace
instead of their own `require`/`replace` directives, and the `go` directive
mismatch this gate exists to catch does not reproduce.

The CI `modules` job discovers modules with a shell loop and fails if it finds
fewer than `MODULE_FLOOR=6`, so a discovery that returns nothing cannot read as
clean. Lower the floor in the same commit that removes a module on purpose.

The same discovery (`MODULE_DIRS` in the Makefile) drives two more gates.
`make lint` and the CI lint step run golangci-lint in every module with the
root's `.golangci.yml` and `-tags=benchmark`, which brings in the harness entry
points under `test/performance` (without it their helpers read as unused).
`make ci-cross` and the CI `cross-arch` job build and vet every module for
linux/arm64, linux/386, windows/amd64, darwin/arm64 and freebsd/amd64, so a
Unix-only syscall or a build constraint that leaves a symbol undefined on one
OS fails before merge.

## Running heavyweight suites off-box

`make remote-<target>` rsyncs the working tree to `REMOTE_HOST` (required, no
default; optional `REMOTE_USER` and `REMOTE_WORKDIR`, default
`~/ci/go-quicraft`) and runs `make <target>` there, streaming output back. It is
a pattern rule, so no target needs enrolling.

Every target runs under `-race`, and the integration suites stand up multi-node
QUIC clusters. On a busy workstation that contention expires the 30-second
catch-up waits, and the failures read as Raft defects. Point `REMOTE_HOST` at
the machine CI runs on, so a local failure and a CI failure mean the same thing.

The sync uses `--delete`, so a file removed locally cannot linger remotely and
turn a broken build green. `.git` is excluded: the target ships the working
tree including uncommitted edits, and nothing on the remote can run a git
operation against this repository's history.

`make remote-tools` installs govulncheck, gosec, golangci-lint (pinned to
`GOLANGCI_VERSION`) and trivy on the remote host, and seeds `known_hosts` for
github.com and, when `FORGE_SSH_HOST` (with `FORGE_SSH_PORT`) names one, a
private module forge. Every tool is installed unconditionally: an analyser of
the wrong version gates on a different rule set from CI while looking like it
passed.

## Benchmarking

`make bench-<pkg>` covers proto, raft, waldb, engine, registry, seal,
transport, quicraft, server, writemode, batch and crypto. They are single
unreplicated runs and cannot detect a regression. `make bench-compare` is the
gate: it interleaves baseline and current binaries across many rounds at a fixed
iteration count and compares the distributions with benchstat. See
[Benchmark regression testing](benchmarking.md).

## Numbers (2026-10-02)

- 6,799 top-level test functions across the 24 packages under `pkg/`; 7,874
  across the module tree
- 170 integration test functions: 164 under `integration`, 6 more under
  `quicraft_netfault`
- 37 fuzz targets in six packages, all seeded; 39 committed corpus files
- 36 errfs tests: 23 injecting, 13 testing the injector
- 26 linearizability tests; 7 netfault injector tests
- Coverage floor 91% locally, 90% in CI, total and per package

Counts drift. Regenerate with `git grep -cE '^func Test'`.

## Cross-references

- [Testing overview](overview.md): commands, patterns, target reference
- [Benchmark regression testing](benchmarking.md): the `bench-compare` method
- [Contributing](../../CONTRIBUTING.md): what a change is expected to bring
- [Raft consensus engineering](../raft-consensus/engineering.md): the invariants the suites check
- [Log storage engineering](../log-storage/engineering.md): the WAL internals errfs drives
- [QUIC transport engineering](../quic-transport/engineering.md): where the netfault hook sits
