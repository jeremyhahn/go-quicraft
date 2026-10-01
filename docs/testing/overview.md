# Testing Overview

> **Status: REFERENCE** · Scope: how this repository's suites are organised and
> run — `Makefile`, `test/`, `.github/workflows/ci.yml` · Last verified: 2026-09-15

## Philosophy

Tests are written to catch real regressions, not to pad coverage numbers. Two
rules outrank the coverage percentage, and both come from something that went
wrong here:

**A regression test must be seen to fail before the fix.** Write the test,
revert the fix hunk, watch it fail, then put the fix back. Three regression
tests once shipped that passed against unfixed code, each with a claim
attached that it covered the bug. If you cannot make a test fail, say so in the
pull request instead of implying it was verified.

**Count passes, never the absence of failures.** A bisect here once read
"setup failed" as "0 failures". A run that emitted no FAIL lines because it
never compiled is indistinguishable, in a grep, from a clean run.

The rest:

- **Unit tests run fast**: in memory, no host modifications, no blocking ops
- **Integration tests use real resources**: QUIC transport, multi-node clusters
- **Meaningful assertions**: typed error checks, value validation, state inspection
- **Race detection always on**: `go test -race -count=1 -v`

There is no `-short` mode. Two `testing.Short()` guards exist in
`pkg/logdb/waldb/crash_test.go` and never fire, because no Makefile target
passes the flag. About two dozen `t.Skip` calls exist across the tree, mostly
for environment-dependent cases.

## Test Categories

### Unit Tests

Invoked via `make test` (or `make test-<package>` for a single package). The Makefile defines `GOTEST := go test -race -count=1 -v`, so every unit test run includes the race detector and disables result caching.

Unit tests:

- Run entirely in memory (use `t.TempDir()` for filesystem state)
- Complete in seconds, not minutes
- Never bind network ports or modify the host system
- Use the in-memory LogDB (`memdb.New()`) or lightweight mocks

### Integration and E2E Tests

Four packages under `test/integration/`, all gated by `//go:build integration`:
`raft/` (31 tests), `host/` (45), `e2e/` (60, plus 6 more behind
`quicraft_netfault`) and `linearizability/` (26).
There is no bootstrap suite and no `integration-test-bootstrap` target.

| Makefile Target                  | Suite                     | Description                              |
|----------------------------------|---------------------------|------------------------------------------|
| `integration-test`               | All                       | All four packages, in the devcontainer   |
| `integration-test-raft`          | `test/integration/raft/`  | Multi-node Raft protocol (leadership, reads, snapshots, stress) |
| `integration-test-host`          | `test/integration/host/`  | Host API: SyncPropose, SyncRead, StaleRead, LeaseRead, membership, rolling restart |
| `integration-test-linearizability` | `test/integration/linearizability/` | Porcupine verification and chaos soaks |
| `integration-test-e2e`           | `test/integration/e2e/`   | The same e2e package, run in a Docker Compose stack |
| `ci-integration`                 | All                       | Same suite on the host, byte-identical to the `integration` job in ci.yml |
| `ci-netfault`                    | `test/integration/e2e/`   | Adds `quicraft_netfault`: real link-level partitions between live replicas |

`integration-test` and `ci-integration` both pass `-p 1`, deliberately. Each
package stands up multi-node QUIC clusters under `-race`, and four of them
running concurrently starve CPU enough that the 30-second snapshot catch-up
waits expire. Every package is stable standalone; the failures are contention,
not defects. Because `-p 1` makes all four share one deadline, the timeouts are
900s in the devcontainer and 1800s in CI. An earlier 300s cap failed the suite
with a timeout panic and zero test failures, which reads as a product bug
rather than as a budget.

`make integration-test-e2e` brings up
`.devcontainer/docker-compose.integration.yml`, now a single test-runner
container. The three unused node containers it used to declare were removed:
nothing read the environment variables they were given, they named a build stage
that does not exist, and there is no node server binary for them to run. The e2e
tests stand up their own clusters through `newTestCluster` — real `quicraft.Host`
instances over real QUIC with mandatory mTLS on loopback — so the container's
contribution is the pinned toolchain and tuned UDP buffers, not topology.

The build step in that image previously swallowed compile failures and installed
an empty executable, letting the run exit 0 having tested nothing; see
[infrastructure.md](infrastructure.md) for the shell-precedence detail and what
replaced it.

### Fault Injection

Two injectors, each behind its own build tag, each with tests of its own so it
cannot quietly stop injecting:

- `test/errfs` (`quicraft_errfs`) fails filesystem operations under the WAL and
  LogDB: the Nth call, only a named operation, a bounded number of times, or a
  short write. 23 tests drive recovery through it; 13 untagged tests check the
  injector itself and run in `make test`.
- `test/netfault` (`quicraft_netfault`) drops messages between live replicas.
  Its 7 tests are untagged and also run in `make test`.

Run via `make errfs-waldb` / `make ci-errfs` and `make ci-netfault`. Details and
rationale in [Testing Infrastructure](infrastructure.md).

### Benchmarks

Invoked via `make bench-<package>`. The Makefile defines `GOBENCH := go test -run=^$ -bench=. -benchmem`, which excludes unit tests and includes memory allocation reporting.

These targets are smoke tests only: a single unreplicated run whose spread on a loaded machine reaches 5x. To detect an actual hot-path regression use `make bench-compare`, which interleaves a baseline revision against the working tree across many rounds and compares the distributions with benchstat. It runs on the invoking machine by default; `PERF_HOST=user@host` runs the same comparison on a dedicated machine for tighter intervals. See [Benchmark Regression Testing](../testing/benchmarking.md).

### Performance Comparisons

The `test/performance/` directory contains a cross-system benchmark framework that compares QuicRaft against dragonboat, etcd/raft, and OpenRaft. The `perfcompare` CLI orchestrator runs each system in Docker, collects JSON results, and generates comparison reports with SVG charts.

| Makefile Target                | Description                                   |
|--------------------------------|-----------------------------------------------|
| `perf-quicraft`               | QuicRaft-only benchmarks                       |
| `perf-etcd`                   | etcd/raft benchmarks                           |
| `perf-dragonboat`             | Dragonboat benchmarks                          |
| `perf-openraft`               | OpenRaft (Rust) benchmarks                     |
| `perf-compare-all`            | Compare all systems (terminal report only)     |
| `perf-all`                    | Run + store + generate for one machine profile |
| `perf-remote`                 | The same, on another machine (PERF_HOST)       |

Everything that stores a result requires `PERF_PROFILE`; see
[test/performance/README.md](../../test/performance/README.md).

## Unit Test Patterns

### Helper Functions for Setup/Teardown

Each package defines focused helper functions that construct test fixtures with sensible defaults. Cleanup is handled via `t.Cleanup()` or `defer`/`t.TempDir()` automatic cleanup.

**Raft package** (`pkg/internal/raft/`):

```go
func raftTestCfg(shardID, replicaID uint64) config.Config {
    cfg := config.Config{
        ShardID:      shardID,
        ReplicaID:    replicaID,
        ElectionRTT:  10,
        HeartbeatRTT: 1,
    }
    cfg.SetDefaults()
    return cfg
}

func newTestRaft(replicaID uint64) *raft {
    members := map[uint64]string{1: "addr1", 2: "addr2", 3: "addr3"}
    return newTestRaftWithMembers(replicaID, members)
}
```

Variant helpers exist for specific configurations: `raftTestCfgPreVote`, `raftTestCfgCheckQuorum`, `raftTestCfgQuiesce`, `newTestRaftPreVoteDisabled`, `newTestRaftCheckQuorumDisabled`.

**Engine package** (`pkg/internal/engine/`):

```go
func newTestHostConfig() config.HostConfig {
    cfg := config.HostConfig{
        WALDir: "/tmp/test-wal", NodeHostDir: "/tmp/test-node",
        ListenAddress: "127.0.0.1:0", RTTMillisecond: 200,
        NumWorkers: 2, MaxApplyWorkers: 2, CommitCBufferSize: 16,
        ShutdownTimeout: 5 * time.Second,
        MaxTotalInMemLogSize: 4 * 1024 * 1024 * 1024, DeploymentID: 1,
    }
    cfg.TransportConfig.SetDefaults()
    return cfg
}
```

**WAL package** (`pkg/logdb/waldb/`):

```go
func openTestDB(t *testing.T) *DB {
    t.Helper()
    dir := t.TempDir()
    db, err := Open(dir, WithNoSync(true), WithBlockSize(1024), WithMaxFileSize(64*1024))
    if err != nil { t.Fatalf("Open: %v", err) }
    t.Cleanup(func() { db.Close() })
    return db
}
```

Key pattern: WAL tests disable `fsync` via `WithNoSync(true)` for speed while retaining all other production behavior.

### Table-Driven Tests

Used for testing multiple input/output combinations. Two styles appear throughout the codebase:

**Subtest style** (config, session packages):

```go
func TestConfig_SetDefaults(t *testing.T) {
    t.Run("fills all zero fields", func(t *testing.T) {
        var c Config
        c.SetDefaults()
        if c.ElectionRTT != DefaultElectionRTT { ... }
    })
    t.Run("preserves non-zero fields", func(t *testing.T) { ... })
}
```

**Struct-slice style** (raft, memdb packages):

```go
func TestOperationsAfterCloseReturnErrClosed(t *testing.T) {
    db := New()
    db.Close()
    tests := []struct {
        name string
        fn   func() error
    }{
        {"SaveState", func() error { return db.SaveState(nil) }},
        {"IterateEntries", func() error { ... }},
        ...
    }
    for _, tt := range tests {
        t.Run(tt.name, func(t *testing.T) {
            if err := tt.fn(); err != logdb.ErrClosed { ... }
        })
    }
}
```

### In-Memory LogDB for Testing

The `memdb` package (`pkg/logdb/memdb/`) provides a complete `logdb.LogDB` implementation backed by Go maps. Used wherever tests need LogDB without WAL overhead:

```go
h, err := quicraft.NewHost(cfg, WithMemoryLogDB(), WithoutTransport(), WithoutEncryption())
```

The `mockLogDB` in `pkg/host_test.go` provides an even lighter mock with injectable errors for testing Host-level error paths.

### Mock State Machines

The `kvStateMachine` is the standard test state machine, defined once per integration package (`test/integration/e2e/helpers_test.go`, `test/integration/host/helpers_test.go`). It implements `sm.StateMachine` with:

- `Update`: Parses `[op:1][keyLen:2 BigEndian][key][value]`, stores in a map
- `Lookup`: Returns value by key
- `SaveSnapshot` / `RecoverFromSnapshot`: Binary serialization of the full map
- `Close`: No-op

It is not only a map. `checkIndexLocked` enforces strict forward progress of
the applied index: an index that repeats or rolls back records an apply
violation, and a jump records a gap counted by `ApplyGaps()`.
`RecoverCount()` reports how many times the machine was rebuilt from a
snapshot. A duplicate or reordered apply therefore fails on its own terms,
rather than being masked by a later write that happens to restore the expected
value.

A companion `smTracker` wraps the factory to give tests direct access to state machine internals for assertions:

```go
createFn, tracker := kvCreateFunc()
// ... start shard with createFn ...
sm := tracker.Get(shardID, replicaID)
val, ok := sm.Get("my-key")
```

### Transport Test Helpers

The transport package provides a `transportPair` function that creates two `QUICTransport` instances connected via loopback with mutual registration. A `testHandler` records received batches and snapshot chunks with notification channels for synchronized assertions:

```go
sender, receiver, sHandler, rHandler, cleanup := transportPair(t, 1)
defer cleanup()
// ... send messages ...
rHandler.waitBatch(t, 5*time.Second)
```

### Typed Error Assertions

Tests consistently check error types, not string messages:

```go
if err := db.Close(); err != logdb.ErrClosed {
    t.Fatalf("got %v, want %v", err, logdb.ErrClosed)
}

var npe *NoPeersError
if !errors.As(err, &npe) {
    t.Errorf("expected NoPeersError, got %T: %v", err, err)
}
```

## E2E Test Infrastructure

### Cluster Management

The `testCluster` struct manages N Host instances on loopback:

```go
cluster := newTestCluster(t, 3)  // 3-node cluster
defer cluster.close()
trackers := cluster.startShard(1) // start shard 1 on all nodes
```

Each host gets a unique ephemeral UDP port via `freePort()` which binds and immediately closes a temporary listener. Hosts use real QUIC transport with auto-generated TLS certificates.

### Wait Helpers

All E2E tests use polling-based wait functions instead of fixed sleeps:

| Helper                 | Purpose                                              |
|------------------------|------------------------------------------------------|
| `waitForClusterLeader` | Polls until one node reports `IsLeader` for a shard  |
| `waitForSingleLeader`  | Polls until exactly one node claims leadership       |
| `waitForLeader`        | Polls a single host for leader knowledge             |
| `waitForApplied`       | Polls until a shard's lastApplied reaches a target   |
| `waitForReplication`   | Polls all live nodes until a key-value appears in SM |
| `waitForMembershipSize` / `waitForMembershipConverged` | Membership change convergence |
| `waitForNodeData`      | Polls one node for a key-value                       |
| `waitForSnapshotArtifact` | Polls a directory for snapshot files on disk      |
| `waitForCondition` / `waitForConditionDiag` | Generic poll-until-true, with and without diagnostics |

### Timeout Constants

```go
const defaultTimeout        = 15 * time.Second   // standard operation timeout
const leaderElectionTimeout = 5 * time.Second    // leader election wait
const failoverTimeout       = 20 * time.Second   // failover: the old leader must time out first
```

With RTT=50ms and ElectionRTT=10 an election takes roughly 500ms, so
`leaderElectionTimeout` is 10x headroom for container and CI contention.

### E2E Test Files

`test/integration/e2e/` holds 63 tests. All build under `integration`; two
files need `quicraft_netfault` as well.

| File | Tests | Covers |
|------|-------|--------|
| `replication_test.go` | 10 | Single entry, sequential, concurrent proposals, stale read, large values, 3N and 5N |
| `failover_test.go` | 9 | Leader kill and re-election, data survival, proposals after failover, exact-quorum cases |
| `cluster_test.go` | 6 | 3N/5N election, all-nodes-agree convergence, multiple shards on one cluster |
| `asymmetric_partition_test.go` | 6 | Leader isolated from a minority / from a majority, follower isolated, snapshot catch-up after |
| `membership_test.go` | 5 | Add, remove, round-trip, proposal after removal |
| `membership_election_test.go` | 4 | Elections across a membership change |
| `barrier_test.go` | 4 | Propose and read through an encrypted barrier, replication, snapshot restore, 5N |
| `replacement_test.go` | 4 | Replace a follower in 3N and 5N, data integrity, continued writes |
| `barrier_replacement_test.go` | 3 | Replacement and snapshot catch-up with a barrier, key rotation mid-replacement |
| `snapshot_catchup_test.go` | 3 | New node catch-up via snapshot, data integrity, 5N |
| `partition_test.go` | 3 | Minority cannot elect, majority commits, heal and reconverge |
| `netpartition_test.go` | 3 | `quicraft_netfault`: deposed live leader, asymmetric leader, 2-of-4 cannot elect |
| `quorum_loss_test.go` | 2 | Writes rejected without quorum, recovery |
| `election_safety_netfault_test.go` | 1 | `quicraft_netfault`: a spent term cannot be won twice |

`partition_test.go`, `asymmetric_partition_test.go` and `quorum_loss_test.go`
create their topologies with `stopNode`, which is a process kill. Only the two
`quicraft_netfault` files drop packets between replicas that are still running.
The distinction matters: with the stopNode-only suite, an election quorum
weakened from a strict majority to `n/2` passed 18 of the 19 linearizability
tests. See [Testing Infrastructure](infrastructure.md#network-partition-injection).

### Docker Compose E2E

`.devcontainer/docker-compose.integration.yml` defines a 3-node cluster with
static discovery, plus a test-runner container gated on all three healthchecks:

```yaml
services:
  node1:
    command: [start, --listen=0.0.0.0:63001, --raft-address=node1:63001, ...]
    healthcheck:
      test: ["CMD-SHELL", "test -f /data/nodehost/host.id"]
      interval: 2s
      retries: 30
  test-runner:
    depends_on:
      node1: { condition: service_healthy }
      node2: { condition: service_healthy }
      node3: { condition: service_healthy }
```

The test-runner executes a binary compiled from `./test/integration/e2e/...`,
which spins up its own loopback clusters and ignores the node containers
entirely. `.devcontainer/Dockerfile.integration` also swallows a compile failure
(`2>/dev/null || echo ... && touch /integration-tests`) and substitutes an empty
executable, so this path can report success having run nothing. Use
`make ci-integration` when you need the suite to have actually run.

## Coverage Targets

One threshold, `COVER_MIN = 91`, applied by every coverage target. There is no
per-package threshold table: `make coverage-raft` and `make coverage` (which
measures `./...`) are judged against the same number. The CI `test` job repeats
the same check on the module total.

```
make coverage-raft         # one package
make coverage              # ./...
```

Profiles land in `build/coverage/<label>.out` and open with
`go tool cover -html=build/coverage/<label>.out`.

`check-coverage` guards three ways before it compares, and those guards are the
gate's history rather than defensive padding. The earlier version went straight
to `bc`, and on an empty or missing profile the percentage was the empty string,
`bc` failed printing nothing, the shell test degenerated to `[ -eq 1 ]`, and the
`if` took the else branch. The gate passed exactly when it had no coverage data
to judge. A missing or empty profile, or a value that is not a number, now fails
explicitly. `awk '/^total:/'` replaced `grep total` because the old filter also
matched functions whose names contain "total", and two matches produced two
values, which landed back in the same unparseable case.

`-coverpkg=./...` is deliberately not passed. Measured, not assumed: an
untested package is already in the denominator at 0%, and `-coverpkg` only adds
credit for statements covered by another package's tests, which moved the same
tree from 91.3% to 92.0%. Against an unchanged floor that loosens the gate by
0.7 points, so it is a threshold decision rather than a wiring fix.

## Benchmark Infrastructure

### Package-Level Benchmarks

Benchmark functions across the codebase measure critical hot paths. Each benchmark uses `b.ReportAllocs()` and `b.ResetTimer()` after setup. Crypto benchmarks additionally call `b.SetBytes()` for throughput reporting.

**Host-level benchmarks** (`pkg/bench_test.go`):

| Benchmark              | Measures                                            |
|------------------------|-----------------------------------------------------|
| `BenchmarkGetNoOPSession` | Session allocation cost                          |
| `BenchmarkGetLeaderID`    | Cached leader ID read (atomic load + map lookup) |
| `BenchmarkStaleRead`      | Fastest read path (direct SM Lookup)             |
| `BenchmarkSyncPropose`    | Full Raft pipeline: propose -> commit -> apply   |
| `BenchmarkSyncRead`       | Linearizable ReadIndex pipeline                  |

Host benchmarks use `WithMemoryLogDB()` and `WithoutTransport()` to isolate the engine pipeline from I/O:

```go
func benchNewHost(b *testing.B) *Host {
    cfg := benchHostConfig(b)
    h, err := NewHost(cfg, WithMemoryLogDB(), WithoutTransport(), WithoutEncryption())
    if err != nil { b.Fatalf("NewHost failed: %v", err) }
    b.Cleanup(func() { h.Close() })
    return h
}
```

**Seal benchmarks** (`pkg/seal/bench_test.go`):

Encrypt and Decrypt at 64B, 1KB, and 64KB payload sizes. Key rotation, seal status check, and DEK derivation benchmarks.

**Proto benchmarks** (`pkg/proto/`):

MarshalTo/UnmarshalFrom for Entry, Message, MessageBatch, State.

**WAL benchmarks** (`pkg/logdb/waldb/db_test.go`):

SaveState, GetEntries, ReadState, IterateEntries.

**Engine benchmarks** (`pkg/internal/engine/`):

ReadyMap Set / SetAndSwap / ConcurrentSet / SwapEmpty / SwapWithData, WorkSignal Notify (contended and uncontended), and `protoUpdatesToLogDB` buffer reuse at small and large batch sizes.

### Running Benchmarks

```
make bench-proto       # Proto marshal/unmarshal
make bench-raft        # Raft core: Tick, HasUpdate, GetUpdate, compression
make bench-waldb       # WAL: SaveState, GetEntries, segment write/sync
make bench-seal        # AEAD encrypt/decrypt at various sizes
make bench-engine      # ReadyMap, WorkSignal, LogDB update conversion
make bench-transport   # Frame marshal/unmarshal, header validation
make bench-quicraft    # Full pipeline: propose, read, leader lookup
make bench-registry    # Address registry resolve/register
make bench-crypto      # GF(256) Shamir primitive
make bench-server      # Rate limiter
make bench-writemode   # Write mode benchmarks
make bench-batch       # Batch processing benchmarks
```

## Fuzz Testing

37 targets behind `//go:build quicraft_fuzz`, across six packages:

| Target | Package | Fuzz functions | Surface |
|--------|---------|----------------|---------|
| `fuzz-proto` | `pkg/proto` | 13 | Entry, Message, MessageBatch, State, Membership, ConfigChange, Snapshot: round-trip and unmarshal-crash |
| `fuzz-waldb` | `pkg/logdb/waldb` | 9 | WAL record codec and recovery |
| `fuzz-transport` | `pkg/internal/transport` | 5 | Frame header decode, message-frame receive path |
| `fuzz-session` | `pkg/internal/session` | 4 | Session encoding boundaries |
| `fuzz-seal` | `pkg/seal` | 4 | AES-GCM encrypt/decrypt across epochs |
| `fuzz-raft` | `pkg/internal/raft` | 2 | Adversarial step sequences, log recovery |

All 37 carry `f.Add` seeds. `FUZZ_TIME` sets the per-target mutation budget
(default 60s). `fuzz-raft` names its two targets explicitly, because
`go test -fuzz` runs exactly one target per invocation.

The committed corpus is 39 files under `pkg/proto/testdata/fuzz/<Target>/`. No
other package has promoted a reproducer yet; nightly fuzzing uploads crashers
as artifacts and committing one is how it becomes a permanent regression test.

Three distinct things, not interchangeable:

| Target | What it does | Where it runs |
|--------|--------------|---------------|
| `fuzz-<pkg>`, `fuzz-all` | Mutates for `FUZZ_TIME` per target | Local |
| `ci-fuzz-seeds` | `-run=Fuzz` with no `-fuzz`: every seed and corpus file once, deterministic, over `./...` | PR gate |
| `ci-fuzz-mutation` | 10s mutation budget per target | Release gate |

`ci-fuzz-seeds` walks `./...` rather than the six known packages so a new target
in a seventh cannot go unrun. Unbounded searching lives in
`.github/workflows/fuzz.yml` on a nightly cron, deliberately off the PR path: a
crash it finds is a property of the RNG schedule, and failing unrelated pull
requests at random is how a gate stops being a gate.

## Error Injection Testing

`test/errfs` fails filesystem operations under the WAL and LogDB on demand:
the Nth call, only a named operation, a bounded number of times, or a short
write that disturbs the delegate before returning the error.

```
make errfs-waldb    # 23 tagged tests, -v (use this when one fails)
make ci-errfs       # same suite without -v, CI parity
```

Both keep `-race`: these tests drive the WAL through injected failures from
concurrent goroutines, which is where a torn recovery path shows up as a race
rather than as a wrong answer.

Coverage includes torn tails and repair, a failed fsync losing only
unacknowledged data, mid-entry write failure, read and `Stat` failure during
replay, `OpenFile` and `MkdirAll` failure, compaction remove and readdir
failure, double faults across write and rollback, and idempotent recovery.

A further 13 untagged tests in `errfs_unit_test.go` and `harness_test.go` test
the injector itself and run in `make test`. An injector that quietly stopped
injecting would make all 23 tagged tests pass for the wrong reason.

## Running Heavyweight Suites Off-Box

Every test target runs under `-race`, and the integration suites stand up
multi-node QUIC clusters. On a workstation that is also running editors,
browsers and agents, that contention is enough to expire the 30-second
catch-up waits in the snapshot tests — and those failures read as raft defects
rather than as the scheduling artefacts they are. Diagnosing them has
repeatedly cost days.

Run them on a **remote build host** instead — ideally the same machine CI
executes on, so a local failure and a CI failure mean the same thing. Configure
it with `REMOTE_HOST`, which is required and has no default, plus the optional
`REMOTE_USER` and `REMOTE_WORKDIR`:

```
export REMOTE_HOST=build-host.example
make remote-ci-full            # the whole gate, off-box
make remote-test               # any target: remote-<target> runs <target> there
make remote-integration-test
make remote-tools              # one-time: install the scanners the host lacks
```

`remote-<target>` is a pattern rule, so no target needs enrolling by hand. It
rsyncs the **working tree** — including uncommitted edits, which is the point
during development — with `--delete`, so a file removed locally cannot linger
remotely and turn a broken build green. `.git` is excluded deliberately:
nothing on the remote can perform a git operation against this repository's
history.

Override `REMOTE_WORKDIR` to keep concurrent work from colliding:

```
make remote-test REMOTE_WORKDIR='~/ci/qr-myfeature'
```

Targeted single-package runs are fine locally while developing. Final
verification goes through the Makefile targets, on the remote host.

## Makefile Target Reference

### Unit Test Targets

| Target            | Package Path                       |
|-------------------|------------------------------------|
| `test`            | `./...` (all packages)             |
| `test-proto`      | `./pkg/proto/...`                  |
| `test-config`     | `./pkg/config/...`                 |
| `test-queue`      | `./pkg/internal/queue/...`         |
| `test-invariant`  | `./pkg/internal/invariant/...`     |
| `test-logdb`      | `./pkg/logdb/...`                  |
| `test-raft`       | `./pkg/internal/raft/...`          |
| `test-waldb`      | `./pkg/logdb/waldb/...`            |
| `test-seal`       | `./pkg/seal/...`                   |
| `test-rsm`        | `./pkg/internal/rsm/...`           |
| `test-session`    | `./pkg/internal/session/...`       |
| `test-snapshot`   | `./pkg/internal/snapshot/...`      |
| `test-engine`     | `./pkg/internal/engine/...`        |
| `test-registry`   | `./pkg/internal/registry/...`      |
| `test-transport`  | `./pkg/internal/transport/...`     |
| `test-quicraft`   | `./pkg`                            |
| `test-errors`     | `./pkg`                            |
| `test-sm`         | `./pkg/sm/...`                     |
| `test-server`     | `./pkg/internal/server/...`        |
| `test-writemode`  | `./pkg/writemode/...`              |
| `test-batch`      | `./pkg/batch/...`                  |
| `test-crypto`     | `./pkg/crypto/...`                 |

All unit test targets use `go test -race -count=1 -v`.

### Coverage Targets

Same package set as unit tests, prefixed with `coverage-` instead of `test-`. Profiles go to `build/coverage/<label>.out` and are enforced against `COVER_MIN` (91). Example: `make coverage-raft`.

`make coverage` measures `./...` against the same floor.

### Integration Test Targets

| Target                             | Runs In         | Suite                             |
|------------------------------------|-----------------|-----------------------------------|
| `integration-test`                 | devcontainer    | All four packages, `-p 1`, 900s   |
| `integration-test-raft`            | devcontainer    | Raft protocol                     |
| `integration-test-host`            | devcontainer    | Host API                          |
| `integration-test-linearizability` | devcontainer    | Porcupine verification, 600s      |
| `integration-test-e2e`             | Docker Compose  | e2e package in the compose stack  |
| `ci-integration`                   | host            | All four packages, `-p 1`, 1800s  |
| `ci-netfault`                      | host            | e2e package + `quicraft_netfault` |

### Fault Injection Targets

| Target | Tag | Description |
|--------|-----|-------------|
| `errfs-waldb` | `quicraft_errfs` | WAL/LogDB I/O error injection, `-v` |
| `ci-errfs` | `quicraft_errfs` | Same suite, CI parity |
| `ci-netfault` | `quicraft_netfault` | Partitions between live replicas; the only tests that can construct a deposed-but-running leader |

### Fuzz Testing Targets

| Target | Description |
|--------|-------------|
| `fuzz-proto` | 13 codec targets |
| `fuzz-waldb` | 9 WAL record codec and recovery targets |
| `fuzz-transport` | 5 frame decode targets |
| `fuzz-session` | 4 session encoding targets |
| `fuzz-seal` | 4 AES-GCM targets |
| `fuzz-raft` | 2 targets, named explicitly |
| `fuzz-all` | All of the above (FUZZ_TIME=60s each) |
| `ci-fuzz-seeds` | Seeds and committed corpus only, deterministic, over `./...` |
| `ci-fuzz-mutation` | 10s bounded mutation per target, release gate |

### CI Pipeline Targets

| Target | Description |
|--------|-------------|
| `ci` | `fmt` → `no-mutants` → `vet` → `lint` → `security` → `build` → `test`. Untagged only |
| `ci-full` | `ci` plus `ci-modules`, `ci-errfs`, `ci-fuzz-seeds`, `ci-fuzz-mutation`, `ci-integration`, `ci-netfault`, and the three FIPS targets (`build-fips`, `verify-fips`, `verify-fips-exempt`). Matches ci.yml |
| `ci-modules` | Build and vet every `go.mod` in the tree, with `GOWORK=off` |
| `remote-<target>` | rsync the working tree to the remote build host and run `<target>` there |
| `remote-tools` | Provision the remote host's scanners |

`make ci` stops at `test`, which is `go test ./...` with no build tags. That
quietly skipped the integration, errfs and fuzz suites and computed the
coverage threshold over what was left. `ci-full` is what to run before opening
a pull request; `ci` is the fast inner loop.

### Docker Targets

| Target                       | Description                         |
|------------------------------|-------------------------------------|
| `docker-build`               | Build devcontainer image            |
| `docker-build-integration`   | Build integration test image        |
| `docker-run`                 | Run devcontainer shell              |
| `docker-test`                | Run unit tests in devcontainer      |
| `docker-integration-down`    | Stop and clean integration containers |

### Static Analysis

| Target | Description                    |
|--------|--------------------------------|
| `lint` | `golangci-lint run --timeout=5m ./...` |
| `vet`  | `go vet ./...`                 |
| `fmt`  | Check `gofmt` formatting       |
| `no-mutants` | Fail if `if false {`, `MUTANT` or `MUTATION` survives in non-test Go under `pkg/`, `test/`, `contrib/` |
| `security` | `gosec`, `govulncheck`, `go mod verify`, `trivy fs` |

`no-mutants` runs inside `make ci`. Mutation testing is how a fix is verified
here, and that workflow edits production code: a restore that never happens is
invisible, because a disabled guard makes tests pass rather than fail. An
`if false {` around the rollback fsync in waldb once reached a commit, and every
suite stayed green because the site had no test, which is why it was being
mutated in the first place.

## Known Test Patterns and Fixes

### E2E Failover Flakiness: Warmup Proposal

Tests that stop nodes require a **warmup proposal and full replication** before stopping any node. Without warmup, QUIC connections may not be fully established, causing re-election to stall under resource contention:

```go
trackers := cluster.startShard(1)
oldLeaderIdx := cluster.waitForClusterLeader(1, leaderElectionTimeout)

// Warm up: ensure all QUIC connections are established.
proposeAndWait(t, cluster, trackers, 1, "warmup", "ok")
waitForReplication(t, cluster, trackers, 1, "warmup", "ok", defaultTimeout)

// Now safe to stop a node.
cluster.stopNode(oldLeaderIdx)
```

### Leader Convergence: Poll All Nodes

After `waitForClusterLeader` returns (finds one leader), followers may not yet know the leader ID. Use `waitForCondition` to poll all nodes for agreement:

```go
cluster.waitForClusterLeader(1, leaderElectionTimeout)

waitForCondition(t, leaderElectionTimeout, "all nodes agree on leader", func() bool {
    var leaderID uint64
    for i, h := range cluster.hosts {
        if cluster.stopped[i] { continue }
        info, err := h.GetShardInfo(1)
        if err != nil || info.LeaderID == 0 { return false }
        if leaderID == 0 {
            leaderID = info.LeaderID
        } else if info.LeaderID != leaderID {
            return false
        }
    }
    return leaderID > 0
})
```

### ReadyMap/SwapBuffer Concurrent Tests

Concurrent producer-consumer tests use sequential drain after producer completion rather than concurrent drain. This avoids races in test assertions while still validating the data structure under concurrent write load.

### Rate Limit Retry in 5-Node Tests

Under Docker CI contention, 5-node clusters may hit rate limits during sequential proposal bursts. The pattern retries with exponential backoff:

```go
for attempt := range 5 {
    rs, err := cluster.hosts[leaderIdx].Propose(ctx, 1, cmd)
    if err != nil {
        if strings.Contains(err.Error(), "rate limit") {
            backoff := time.NewTimer(time.Duration(50*(attempt+1)) * time.Millisecond)
            select {
            case <-backoff.C:
            case <-ctx.Done():
                t.Fatalf("context canceled during backoff: %v", ctx.Err())
            }
            continue
        }
        t.Fatalf("Propose failed: %v", err)
    }
    // ... success ...
    break
}
```

## Cross-References

- [Architecture](../multi-group-host/architecture.md) -- Package structure, engine pipeline
- [WAL Persistence](../log-storage/engineering.md) -- WAL internals tested by waldb suite
- [Engine Pipeline](../raft-consensus/engineering.md) -- Worker pipeline tested by engine suite
- [Transport](../quic-transport/engineering.md) -- QUIC transport tested by transport suite
- [Testing Infrastructure](infrastructure.md) -- fault injectors, fuzzing, linearizability, and why each gate is shaped the way it is
- [Contributing](../../CONTRIBUTING.md) -- what a change is expected to bring with it
- [Benchmark Regression Testing](../testing/benchmarking.md) -- the `bench-compare` method
