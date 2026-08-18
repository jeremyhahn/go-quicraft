# Testing Infrastructure

This document describes the frameworks: what each suite is built out of, which
build tag gates it, and why the gates are shaped the way they are. For the
day-to-day commands and unit-test patterns, see [Testing Overview](overview.md).

## Two rules that outrank coverage

**A regression test must be seen to fail before the fix.** Write the test,
revert the fix hunk, watch it fail, then put the fix back. This project has
shipped regression tests that passed against unfixed code, three of them in one
change, each accompanied by a claim that they covered the bug. The author's
claim is not evidence. The observed failure is.

**Count passes, never the absence of failures.** A bisect here once read
"setup failed" as "0 failures" and walked the wrong direction for hours. A run
that produced no FAIL lines because it never compiled looks identical, in a
grep, to a clean run. Assert on the number of tests that ran.

Both rules are why several gates below are written to fail closed on missing
data rather than to pass quietly.

## Suites and their build tags

| Suite | Tag | Where | Runner |
|-------|-----|-------|--------|
| Unit | none | `pkg/...` | `make test`, `make test-<pkg>` |
| Integration | `integration` | `test/integration/{raft,host,e2e,linearizability}` | `make integration-test`, `make ci-integration` |
| Network partition | `integration quicraft_netfault` | `test/integration/e2e` | `make ci-netfault` |
| I/O fault injection | `quicraft_errfs` | `test/errfs` | `make errfs-waldb`, `make ci-errfs` |
| Fuzz | `quicraft_fuzz` | 6 packages under `pkg/` | `make fuzz-<pkg>`, `make ci-fuzz-seeds`, `make ci-fuzz-mutation` |

A build tag is a way to hide tests from CI without anyone noticing: the Go
toolchain does not warn that a constraint excluded a file, it just compiles a
smaller program. `make ci` runs only the untagged suite. `make ci-full` runs
every tag, and that is the target to reproduce a red CI run with.

### Unit tests

`GOTEST := go test -race -count=1 -v`, so the race detector is always on and
results are never cached. 22 per-package targets exist (`test-proto`,
`test-raft`, `test-crypto`, and so on); the full list is in
[Testing Overview](overview.md#unit-test-targets).

There is no `-short` mode in any Makefile target. Two guards on
`testing.Short()` exist in `pkg/logdb/waldb/crash_test.go` and never fire,
because nothing passes the flag.

### Integration tests

Four packages under `test/integration/`, all behind `//go:build integration`:

| Package | Tests | Covers |
|---------|-------|--------|
| `raft/` | 30 | Elections, replication, failover, snapshots, multi-shard, 3N and 5N |
| `host/` | 43 | Public Host API, LeaseRead, membership change, rolling restart, snapshot under load |
| `e2e/` | 63 | Cluster formation, partitions, quorum loss, barrier encryption, node replacement |
| `linearizability/` | 19 | Porcupine verification and the chaos soaks |

`make integration-test` builds the devcontainer and runs all four inside it.
`make ci-integration` runs the same suite on the host, byte-identical to the
`integration` job in `.github/workflows/ci.yml`.

Both pass `-p 1`. That is deliberate and load-bearing. Each package stands up
multi-node QUIC clusters under `-race`, and running four of them concurrently
starves CPU enough that the 30-second snapshot catch-up waits expire. Every
package is stable standalone; the failure is contention, not a defect. Do not
remove `-p 1` to shorten the job.

Because `-p 1` makes every package share one deadline, the timeouts are large:
900s in the devcontainer, 1800s in CI, where a shared runner is slower. A 300s
cap once failed the suite with a timeout panic and zero test failures, which
reads as a product bug rather than as a budget.

### E2E over Docker Compose

`make integration-test-e2e` brings up `.devcontainer/docker-compose.integration.yml`:
a single `test-runner` container executing a binary compiled from
`./test/integration/e2e/...`.

It used to declare three `node1`/`node2`/`node3` containers as well. Those were
removed, for three reasons worth recording so they are not reintroduced. Nothing
read the `INTEGRATION_NODE*_ADDR` variables they were configured with. They
declared a `node` build stage that does not exist in `Dockerfile.integration`, so
they could never build. And there is no node server binary for them to run — the
only `package main` in the tree is the benchmark orchestrator. The e2e package
builds its own clusters instead: `newTestCluster` stands up real `quicraft.Host`
instances over real QUIC with mandatory mTLS on loopback ports, which is genuine
multi-replica coverage that simply does not need cross-container topology.

What the container still contributes over `make integration-test` is the pinned
toolchain and the entrypoint's tuned QUIC UDP buffers.

The build step in `.devcontainer/Dockerfile.integration` is also worth
understanding, because it was the subject of a real defect. It used to read:

```
go test -c ... 2>/dev/null || echo "no integration tests found yet" && touch /integration-tests
```

`||` and `&&` are equal-precedence and left-associative, so that parses as
`((go test || echo) && touch) && chmod`. A compile failure was hidden by
`2>/dev/null`, `echo` succeeded, and a zero-byte executable was installed;
`execve` on an empty file returns `ENOEXEC`, the shell falls back to running it
as a script, and an empty script exits 0. The compose run then reported success
having executed nothing. It now compiles with `set -eux`, no stderr suppression
and no fallback, then asserts the binary is non-empty and actually lists tests —
because `go test -c` on a package with no test files writes no output and still
exits 0.

### Network partition injection

`test/netfault` drops messages between **live** replicas, behind the
`quicraft_netfault` build tag. Run via `make ci-netfault`.

Why it exists: every test in the suite calling itself a partition test called
`stopNode`, which is a process kill. A killed node holds no leadership belief,
answers nothing, and comes back with cold in-memory state, so the most
dangerous configuration in Raft, a live leader deposed by a quorum it can no
longer reach, could not be constructed anywhere. The cost was measured: with
the suite as it stood, weakening the election quorum from a strict majority to
`n/2` passed 18 of the 19 linearizability tests. Against the partition
primitive the same mutant is caught in about 1.1 seconds.

| Primitive | Effect |
|-----------|--------|
| `Isolate(replica)` / `Rejoin(replica)` | Cuts one replica off in both directions without stopping it. It keeps its term, role and ticker |
| `Partition(groupA, groupB)` | Blocks both directions across a cut; each side stays internally connected |
| `BlockLink(from, to)` / `HealLink(from, to)` | A single directed edge, for asymmetric partitions |
| `Heal()` | Removes every fault. Tests register it with `t.Cleanup` because the state is process-global |
| `BlockedCount()` | Number of installed faults, used by the self-guard below |

Drops are applied on the sending side, which expresses any directed cut since
the edge A→B is controlled entirely at A, and they are silent: the sender's
connections stay up and no error surfaces, as on a real network.

Production cost is zero, and that is verified rather than asserted.
`pkg/internal/transport/netfault_off.go` defines `netFaultEnabled` as a false
`const`, so the compiler folds away every
`if netFaultEnabled && netFaultDrop(...)` site in the send path, and
`go list -deps ./pkg/internal/transport` does not contain `test/netfault`. Only
`netfault_on.go`, itself behind the tag, imports it.

Every test asserts `netfault.BlockedCount()` is non-zero before proceeding, so
building without the tag fails the suite loudly instead of silently exercising
a healthy cluster, which is precisely the defect the package was written to
correct.

Four tests use it today: `TestNetPartition_TwoOfFourCannotElect`,
`TestNetPartition_DeposedLeaderStaysAliveAndCannotCommit`,
`TestNetPartition_AsymmetricLeaderCannotBeHeard` and
`TestElectionSafety_SpentTermCannotBeWonTwice`. The older partition tests
(`partition_test.go`, `asymmetric_partition_test.go`, `quorum_loss_test.go`,
13 tests) still build under the plain `integration` tag and still simulate
topology with `stopNode`. They are worth keeping, but they do not cover the
deposed-live-leader case.

The injector has its own tests in `test/netfault/netfault_test.go` (7 of them,
untagged, so they run under `make test`): that `Drop` returns false with
nothing injected, that `BlockLink` is directional, that `Isolate` cuts both
ways, that `Partition` cuts across and not within, that `Heal` clears
everything, that the mutators are safe under concurrent reads, and that the
shard ID is not consulted.

### I/O fault injection

`test/errfs` wraps the filesystem the WAL and LogDB write through and fails
individual operations on demand: the Nth call, only a named operation, a
bounded number of times, or a short write that disturbs the delegate before
returning an error.

Two tiers, and the split matters:

- 23 tests behind `quicraft_errfs` drive the WAL and LogDB through injected
  failures (`waldb_test.go`, 21, and `durability_test.go`, 2). They cover torn
  tails and repair, failed fsync losing only unacknowledged data, mid-entry
  write failure, read failure during replay, `OpenFile` and `MkdirAll` failure,
  `Stat` failure during replay, compaction remove and readdir failure, double
  faults across write and rollback, and idempotent recovery.
- 13 untagged tests in `errfs_unit_test.go` and `harness_test.go` test the
  injector itself: that `InjectAt` fires on exactly the Nth operation, that
  `OnlyOps` restricts and rejects unknown operations, that `Times` bounds the
  budget, that a short write really disturbs the delegate, and that every
  non-injecting operation delegates. These run in the ordinary unit suite,
  which is the point. An injector that quietly stopped injecting would make
  the 23 tagged tests pass for the wrong reason.

`make errfs-waldb` runs the tagged suite with `-v`, which is what you want when
one fails: the failure is a sequence of I/O outcomes and the test name is the
only way to tell which injection point produced it. `make ci-errfs` is the same
suite without `-v`, byte-identical to the `errfs` job in ci.yml and the `errfs`
gate in release.yml. Both keep `-race`, because a torn recovery path shows up
as a race rather than as a wrong answer.

### Fuzzing

37 targets behind `quicraft_fuzz`, across six packages:

| Package | Targets | Surface |
|---------|---------|---------|
| `pkg/proto` | 13 | Entry, Message, MessageBatch, State, Membership, ConfigChange, Snapshot round-trip and crash |
| `pkg/logdb/waldb` | 9 | WAL record codec and recovery |
| `pkg/internal/transport` | 5 | Frame header decode and the message-frame receive path |
| `pkg/internal/session` | 4 | Session encoding boundaries |
| `pkg/seal` | 4 | AES-GCM encrypt/decrypt across epochs |
| `pkg/internal/raft` | 2 | Adversarial step sequences, log recovery |

Every one of the 37 has `f.Add` seeds. The committed corpus is 39 files under
`pkg/proto/testdata/fuzz/<Target>/`; no other package has promoted a
reproducer yet.

Three different things are called fuzzing here, and they are not
interchangeable:

- `make fuzz-<pkg>` and `make fuzz-all` mutate for `FUZZ_TIME` (60s default).
  Local use. `fuzz-raft` names its two targets explicitly because
  `go test -fuzz` runs exactly one target per invocation and the pattern
  `-fuzz=Fuzz` would match both.
- `make ci-fuzz-seeds` runs `-run=Fuzz` **without** `-fuzz`, over `./...`. That
  executes each target once per seed and once per corpus file, then exits: a
  deterministic regression suite over every input a previous run found
  interesting. It is the PR gate. `./...` rather than the six known packages,
  so a new target in a seventh package cannot go unrun.
- `make ci-fuzz-mutation` gives each target a 10s mutation budget. It is a
  release gate, not a campaign. The real search is `.github/workflows/fuzz.yml`
  on a nightly cron with a 60s budget per target.

The mutation gate retries, and the retry is not a weakening. Go's fuzzing
coordinator can lose its shutdown RPC and report a bare "context deadline
exceeded" after the full `-fuzztime` has elapsed, which reads exactly like a
crash and is not one (reproduced on an idle host running one target at a time).
The gate separates the two on the signal Go itself gives: a real crash always
writes a reproducer and prints "Failing input written to". That string present
means fail immediately and never retry; absent, with only the deadline message,
means retry; anything else fails. The retry is gated on the *absence* of the
reproducer a real crash produces, so a real crash cannot be retried away.

### Linearizability

`test/integration/linearizability`, Porcupine-based, 19 tests. Clients issue
alternating put/get operations against a KV state machine while faults are
applied, and the recorded history is checked for a valid sequential ordering.

| Test | Nodes | Clients | Duration | Fault |
|------|-------|---------|----------|-------|
| `3Node_Normal` | 3 | 8 | 5s | none |
| `5Node_Normal` | 5 | 8 | 5s | none |
| `3Node_SingleKeyContention` | 3 | 8 | 5s | none, 1 key |
| `3Node_LeaseRead` | 3 | 8 | 5s | none, LeaseRead path |
| `3Node_SessionProposal` | 3 | 4 | 5s | none, at-most-once sessions |
| `3Node_LeaderFailover` | 3 | 4 | 5s | leader stopped |
| `5Node_MinorityDown` | 5 | 8 | 8s | 2 followers stopped |
| `3Node_SnapshotDuringLoad` | 3 | 8 | 10s | snapshot triggered under load |
| `4Node_ElectionSafety` | 4 | 4 | 12s | leadership churn |
| `3Node_FollowerRestart` | 3 | 4 | 12s | follower stopped, restarted on its own WAL |
| `3Node_LeaderRestart` | 3 | 4 | 12s | leader stopped, restarted, rejoins as follower |
| `5Node_MultipleLeaderFailovers` | 5 | 4 | 15s | two leaders stopped in sequence |
| `5Node_MembershipAddRemove` | 5 | 4 | 15s | add then remove a node |
| `5Node_RollingRestart` | 5 | 4 | 20s | each node restarted, never two down |
| `5Node_HighThroughput` | 5 | 16 | 30s | none |

Plus `TestRestartedReplicaResumesReplication` (`catchup_probe_test.go`), which
targets a restarted replica being starved by a full inflight window, and three
soaks in `soak_chaos_test.go`: `ChaosUnderSustainedLoad`,
`RollingRestartUnderLoad`, `LeaderCloseDoesNotStallWrites`.

The important part is not the fault list, it is the operation floor. A history
that is empty, or that contains only writes, or only reads, is trivially
linearizable, and the suite once passed with half the entries dropped.
`checkLinearizability` therefore fails unless the run recorded at least
`floorFor(numClients, active)` writes *and* that many reads, where the floor is
30% of a measured baseline of 5.4 completed writes per client-second. Chaos
tests pass a reduced `active` window because elections and catch-up legitimately
stall clients. At 30% the tightest test keeps a 3.3x margin, while a mutation
that collapses throughput lands one to three orders of magnitude below the
floor. `checkLinearizability` also fails on any client-observed protocol
violation, independently of the Porcupine verdict.

The election-safety invariant is *not* covered here despite the test name.
`TestLinearizability_4Node_ElectionSafety` exercises churn;
`TestElectionSafety_SpentTermCannotBeWonTwice`, under `quicraft_netfault`, is
what spends a term's votes on a candidate structurally unable to win with them
and asserts nothing else can win that term.

## Shared test infrastructure

### In-memory LogDB

`pkg/logdb/memdb` implements the full `logdb.LogDB` interface over Go maps, so
unit tests and Host benchmarks skip WAL overhead:

```go
h, err := quicraft.NewHost(cfg, quicraft.WithMemoryLogDB(), quicraft.WithoutTransport())
```

`mockLogDB` in `pkg/host_test.go` is lighter still, with injectable errors for
Host-level error paths.

### Mock state machines

`kvStateMachine` is the standard test state machine, defined once per
integration package (`test/integration/e2e/helpers_test.go` and
`test/integration/host/helpers_test.go`). It implements `sm.StateMachine` over
a `[op:1][keyLen:2][key][value]` wire format. `kvCreateFunc()` returns a
`sm.CreateFunc` alongside an `smTracker` that hands tests the live instance:

```go
createFn, tracker := kvCreateFunc()
// ... start the shard with createFn ...
val, ok := tracker.Get(shardID, replicaID).Get("my-key")
```

### Transport pair

`transportPair(t, deploymentID)` in `pkg/internal/transport/quic_test.go`
creates two `QUICTransport` instances on loopback with mutual registration.
`transportPairWith` takes options; `transportPairWithRevocation` adds a
revocation checker. The `testHandler` records batches and snapshot chunks and
exposes notification channels, so assertions synchronize instead of sleeping.

### Cluster helpers

`newTestCluster(t, n)` builds N Hosts on loopback with real QUIC and
auto-generated TLS, each on an ephemeral port from `freePort()`. It offers
`startShard`, `stopNode`, `findLeader`, `findFollower`, `waitForClusterLeader`,
`observeLeaders` and `liveNodes`. `pkg/internal/engine` has its own unrelated
`newTestCluster` for in-process engine tests.

### Polling, not sleeping

Integration tests poll rather than sleep: `waitForClusterLeader`,
`waitForSingleLeader`, `waitForLeader`, `waitForApplied`, `waitForReplication`,
`waitForMembershipSize`, `waitForMembershipConverged`, `waitForNodeData`,
`waitForSnapshotArtifact`, and the generic `waitForCondition` /
`waitForConditionDiag`.

## Gates that are shaped by a past failure

### The coverage gate fails closed

`COVER_MIN` is 91, and it is one floor applied by every `coverage-*` target and
by `make coverage` (which measures `./...`). There is no per-package threshold
table; a package target and the total target are judged against the same
number.

The three guards in `check-coverage` are not defensive padding. The previous
version went straight to the comparison:

```make
COVERAGE=$$(go tool cover -func=$(1) | grep total | ...)
if [ $$(echo "$${COVERAGE} < $(COVER_MIN)" | bc -l) -eq 1 ]
```

On an empty or missing profile `COVERAGE` is the empty string, `bc` fails with
a syntax error and prints nothing, the test degenerates to `[ -eq 1 ]`, the
shell reports "unary operator expected", and the `if` takes the else branch.
The gate passed precisely when it had no coverage data to judge. Now a missing
or empty profile, and any value that is not a number, is an explicit failure.
`awk '/^total:/'` replaced `grep total` for the same reason: the old filter also
matched any function whose name contains "total", and two matches produced two
values, which lands back in the unparseable case.

The same guards are duplicated in the `test` job of ci.yml, deliberately, so
the CI gate cannot regress independently of the Makefile one.

`-coverpkg=./...` is deliberately not passed. This was measured rather than
assumed: `go test -coverprofile` already emits a 0% entry for a package with no
test file, so an untested package is already in the denominator (adding a
270-statement untested package moved the module total from 91.3% to 89.7%).
What `-coverpkg` adds is credit for statements covered by some *other*
package's tests, which raised the same tree from 91.3% to 92.0%. Against an
unchanged `COVER_MIN` that is a 0.7-point loosening, so adopting it is a
threshold decision, not a wiring fix.

### The mutation-marker guard

`make no-mutants` runs as the second step of `make ci`. It greps `pkg/`,
`test/` and `contrib/` for `if false {`, `MUTANT` and `MUTATION`, excluding
`_test.go` files, and fails if it finds any.

Mutation testing is how correctness is verified here: a fix is not trusted
until the defect it closes has been re-introduced and the test seen to fail.
That workflow edits production code, and a restore that never happens is
invisible, because a disabled guard makes tests pass rather than fail. It has
already happened once. An `if false {` around the rollback fsync in waldb
reached a commit, and a failed `SaveState` whose bytes had reached the medium
through ordinary writeback could then be resurrected by a crash and replayed as
committed entries the caller had been told were rejected. Every suite stayed
green, because the site had no test, which is exactly why it was being mutated.

Test files are excluded because `if false` is legitimate there for pinning a
compile-time branch, and a mutation left in a test file cannot silently weaken
production behavior.

### The nested-module gate, and the matrix that never ran

`make ci-modules` finds every `go.mod` in the tree and runs `go build` and
`go vet` in each, collecting failures rather than stopping at the first, so one
broken sibling cannot hide a second.

`GOWORK=off` is exported at the bottom of the Makefile and set in the workflow
env, and it is load-bearing here specifically. Inside a workspace these modules
resolve the root through the workspace instead of through their own
`require`/`replace` directives, and the `go` directive mismatch this gate exists
to catch does not reproduce. `contrib/dragonboat` sat broken for exactly that
reason, with nothing compiling it.

The CI job was previously a `discover-modules` job feeding
`matrix: ${{ fromJSON(needs.discover-modules.outputs.modules) }}`. The runner
does not expand a matrix sourced from another job's output. It scheduled one job
literally named `Module ${{ matrix.module }}` whose `working-directory`
interpolated to the empty string, so it rebuilt the repository root and was
green while building none of the nested modules. The replacement is a shell
loop, which expands identically everywhere, plus a `MODULE_FLOOR=6` check so
that a discovery returning nothing fails instead of reading as clean. If a
module is removed on purpose, lower the floor in the same commit.

## Running heavyweight suites off-box

`make remote-<target>` rsyncs the working tree to omega
(`jhahn@198.51.100.90`, `~/ci/go-quicraft`) and runs `make <target>` there,
streaming output back. It is a pattern rule, so no target needs enrolling:

```
make remote-ci-full            # the whole gate, off-box
make remote-integration-test
make remote-test               # or any other target
make remote-tools              # one-time: install the scanners omega lacks
make remote-test OMEGA_DIR='~/ci/qr-myfeature'   # avoid colliding with other work
```

This exists because every target runs under `-race` and the integration suites
stand up multi-node QUIC clusters. On a workstation also running editors,
browsers and agents, that contention expires the 30-second catch-up waits, and
the resulting failures read as raft defects rather than as the scheduling
artifacts they are. Chasing them has cost days. omega is the department's
spare-capacity host and is where the Gitea runner already executes CI, so this
gives the local inner loop the same machine.

The sync uses `--delete`, so a file removed locally cannot linger remotely and
turn a broken build green. `.git` is excluded deliberately: this ships the
working tree including uncommitted edits, which is the point during
development, and it means nothing on the remote can perform a git operation
against this repository's history.

`make remote-tools` installs govulncheck, gosec, golangci-lint (pinned to
`GOLANGCI_VERSION`, matching what quality.yml installs) and trivy, and seeds
`known_hosts`. Every tool is installed unconditionally rather than "if absent",
because omega had a v1 golangci-lint and a years-old gosec on PATH and
`command -v` guards skipped both installs. An analyser of the wrong version
gates on a different rule set from CI while looking like it passed.

## Benchmarking

`make bench-<pkg>` covers proto, raft, waldb, engine, registry, seal,
transport, quicraft, server, writemode, batch and crypto. These are single
unreplicated runs whose spread on a loaded machine reaches 5x. They cannot
detect a regression on their own.

`make bench-compare` is the gate. It builds baseline and current test binaries
with an identical toolchain, interleaves them across many rounds at a fixed
iteration count, and compares the distributions with benchstat. Binaries run on
the invoking machine by default; `PERF_HOST=user@host` ships them over ssh to a
quiet machine, which tightens the confidence intervals without changing the
method. `make bench-baseline` records provenance-stamped baselines.

Cross-system comparison against dragonboat, etcd/raft and OpenRaft lives in
`test/performance/` behind the `perfcompare` orchestrator, stored per machine
profile (`make perf-compare-all` for an ad-hoc report;
`make perf-remote PERF_HOST=user@host PERF_PROFILE=omega` for a stored,
documented run).

Full rationale and interpretation guidance:
[Benchmark Regression Testing](../testing/benchmarking.md) and
[Performance Architecture](../multi-group-host/engineering.md).

## Numbers, as of 0.5.1-alpha

- ~6,300 top-level test functions across the 25 packages under `pkg/`
- 155 integration test functions (151 under `integration`, 4 more under
  `quicraft_netfault`)
- 37 fuzz targets, all seeded, 39 committed corpus files
- 36 errfs tests: 23 injecting into the WAL and LogDB, 13 testing the injector
- 19 linearizability tests, 7 netfault injector tests
- Coverage floor: 91%, one threshold, enforced by the Makefile and again in CI

Counts drift. Regenerate them with `git grep -cE '^func Test'` rather than
trusting this list.

## Cross-References

- [Testing Overview](overview.md) -- commands, unit-test patterns, target reference
- [Contributing](../../CONTRIBUTING.md) -- what a change is expected to bring with it
- [Benchmark Regression Testing](../testing/benchmarking.md) -- bench-compare method
- [Performance Architecture](../multi-group-host/engineering.md) -- cross-system framework
- [Raft Specification](../raft-consensus/engineering.md) -- the invariants the suites are checking
- [Session Management](../client-sessions/engineering.md) -- at-most-once delivery
- [WAL Persistence](../log-storage/engineering.md) -- WAL internals the errfs suite drives
- [Transport](../quic-transport/engineering.md) -- where the netfault hook sits
