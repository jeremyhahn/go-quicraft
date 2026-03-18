# Testing Overview

## Philosophy

QuicRaft follows test-driven development with per-package coverage targets enforced by the Makefile. Tests are written to catch real regressions, not to pad coverage numbers. Every function has at least two corresponding tests: one for the success path and one for error handling with invalid input.

Core principles:

- **Unit tests run fast**: in-memory, no host modifications, no blocking ops
- **Integration tests use real resources**: QUIC transport, multi-node clusters
- **No skip guards or `-short` flags**: every test always runs
- **Meaningful assertions**: typed error checks, value validation, state inspection
- **Race detection on by default**: `go test -race -count=1`

## Test Categories

### Unit Tests

Invoked via `make test` (or `make test-<package>` for a single package). The Makefile defines `GOTEST := go test -race -count=1`, so every unit test run includes the race detector and disables result caching.

Unit tests:

- Run entirely in memory (use `t.TempDir()` for filesystem state)
- Complete in seconds, not minutes
- Never bind network ports or modify the host system
- Use the in-memory LogDB (`memdb.New()`) or lightweight mocks

### Integration and E2E Tests

Invoked via `make integration-test` (or per-suite targets). All integration tests run inside Docker devcontainers, never on the host OS. Tests are gated by the `//go:build integration` build tag and a 300-second timeout.

Integration test suites:

| Makefile Target                  | Suite                     | Description                              |
|----------------------------------|---------------------------|------------------------------------------|
| `integration-test`               | All                       | Run all integration tests in devcontainer |
| `integration-test-raft`          | `test/integration/raft/`  | Multi-node Raft protocol (leadership, reads, snapshots, stress) |
| `integration-test-host`          | `test/integration/host/`  | Host API: SyncPropose, SyncRead, StaleRead, membership |
| `integration-test-e2e`           | `test/integration/e2e/`   | Full cluster E2E over Docker Compose (3 nodes) |
| `integration-test-bootstrap`     | `test/integration/bootstrap/` | Bootstrap and discovery integration  |
| `integration-test-linearizability` | `test/integration/linearizability/` | porcupine linearizability verification |

The E2E suite uses `.devcontainer/docker-compose.integration.yml` which spins up a 3-node QuicRaft cluster with health checks, then runs a test-runner container that waits for all nodes to be healthy before executing tests.

### Benchmarks

Invoked via `make bench-<package>`. The Makefile defines `GOBENCH := go test -run=^$ -bench=. -benchmem`, which excludes unit tests and includes memory allocation reporting.

### Performance Comparisons

The `test/performance/` directory contains a cross-system benchmark framework that compares QuicRaft against dragonboat, etcd/raft, and OpenRaft. The `perfcompare` CLI orchestrator runs each system in Docker, collects JSON results, and generates comparison reports with SVG charts.

| Makefile Target                | Description                                   |
|--------------------------------|-----------------------------------------------|
| `perf-quicraft`               | QuicRaft-only benchmarks                       |
| `perf-etcd`                   | etcd/raft benchmarks                           |
| `perf-dragonboat`             | Dragonboat benchmarks                          |
| `perf-openraft`               | OpenRaft (Rust) benchmarks                     |
| `perf-compare-all`            | Compare all systems                            |
| `perf-compare-all-visualize`  | Compare all + generate SVG charts              |
| `perf-docs`                   | Run all comparisons + generate charts for docs |

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
        ShutdownTimeout: 5 * time.Second, DeploymentID: 1,
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
h, err := quicraft.NewHost(cfg, WithMemoryLogDB(), WithoutTransport())
```

The `mockLogDB` in `pkg/host_test.go` provides an even lighter mock with injectable errors for testing Host-level error paths.

### Mock State Machines

The `kvStateMachine` is the standard test state machine across both unit and integration tests. It implements `sm.StateMachine` with:

- `Update`: Parses `[op:1][keyLen:2][key][value]` wire format, stores in map
- `Lookup`: Returns value by key
- `SaveSnapshot` / `RecoverFromSnapshot`: Binary serialization of the full map
- `Close`: No-op

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
| `waitForCondition`     | Generic poll-until-true with timeout and description |
| `waitForLeader`        | Polls a single host for leader knowledge             |
| `waitForApplied`       | Polls until a shard's lastApplied reaches a target   |
| `waitForReplication`   | Polls all live nodes until a key-value appears in SM |

### Timeout Constants

```go
const defaultTimeout        = 10 * time.Second   // standard operation timeout
const leaderElectionTimeout = 5 * time.Second    // leader election wait
const failoverTimeout       = 15 * time.Second   // failover with old leader timeout
```

### E2E Test Suites

E2E tests are organized into the following categories:

**Cluster formation** (`cluster_test.go`):
- 3-node and 5-node leader election
- All-nodes-agree-on-leader convergence
- Multiple independent shards on one cluster

**Replication** (`replication_test.go`):
- Single entry, sequential entries, concurrent proposals
- Stale read from follower
- Large value replication
- Tests repeated for both 3-node and 5-node clusters

**Failover** (`failover_test.go`):
- Leader kill and re-election
- Data survival across leader change
- Proposal acceptance after failover
- Minority partition (follower loss)
- 5-node cluster with 1 and 2 node failures (exact quorum test)

**Membership** (`membership_test.go`):
- Add node, remove node
- Add/remove round-trip, proposal after removal

**Replacement** (`replacement_test.go`):
- Replace follower in 3-node and 5-node clusters
- Data integrity and continued writes after replacement

**Barrier encryption** (`barrier_test.go`):
- Propose/read through encrypted barrier
- Replication, snapshot restore, 5-node barrier

**Barrier replacement** (`barrier_replacement_test.go`):
- Node replacement with barrier, snapshot catch-up with barrier
- Key rotation during node replacement

**Snapshot catch-up** (`snapshot_catchup_test.go`):
- New node catch-up via snapshot, data integrity, 5-node catch-up

**Discovery and bootstrap** (`discovery_test.go`):
- Static discovery peer enumeration
- 3-node bootstrap formation

### Docker Compose E2E

The `.devcontainer/docker-compose.integration.yml` defines a 3-node cluster where each node runs QuicRaft with static discovery. A test-runner container depends on health checks (`test -f /data/nodehost/host.id`) for all three nodes before executing:

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

## Coverage Targets

Per-package coverage targets are enforced by the Makefile `check-coverage` function, which parses `go tool cover -func` output and fails the build if any package drops below the configured threshold.

Running coverage for a single package:

```
make coverage-raft

make coverage
# Runs total coverage across all packages
```

Coverage profiles are written to `build/coverage/<package>.out` and can be viewed with `go tool cover -html=build/coverage/<package>.out`.

All packages maintain 91%+ coverage enforced in CI.

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
    h, err := NewHost(cfg, WithMemoryLogDB(), WithoutTransport())
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

ReadyMap Set/Swap, WorkSignal Notify (contended and uncontended), SpinMutex lock/unlock.

### Running Benchmarks

```
make bench-proto       # Proto marshal/unmarshal
make bench-raft        # Raft core: Tick, HasUpdate, GetUpdate, compression
make bench-waldb       # WAL: SaveState, GetEntries, segment write/sync
make bench-seal        # AEAD encrypt/decrypt at various sizes
make bench-engine      # ReadyMap, WorkSignal, SpinMutex
make bench-transport   # Frame marshal/unmarshal, header validation
make bench-quicraft    # Full pipeline: propose, read, leader lookup
make bench-registry    # Address registry resolve/register
make bench-discovery   # Static/multicast discovery
make bench-bootstrap   # Quorum size, leader determination
make bench-server      # Rate limiter
make bench-writemode   # Write mode benchmarks
make bench-batch       # Batch processing benchmarks
```

## Fuzz Testing

Native Go fuzzing targets cover high-risk codec and protocol surfaces:

| Target | Coverage |
|--------|----------|
| `fuzz-proto` | Entry/Message/Snapshot round-trip |
| `fuzz-transport` | FrameHeader decode with malformed input |
| `fuzz-waldb` | WAL record codec and recovery |
| `fuzz-seal` | AES-GCM encrypt/decrypt across epochs |
| `fuzz-session` | Session encoding boundaries |

Run via `make fuzz-all` or per-target `make fuzz-<package>`. FUZZ_TIME environment variable controls duration (default 60s).

## Error Injection Testing

ErrorFS framework injects I/O failures at the WAL/LogDB layer to verify crash recovery and error propagation without panic:

```
make errfs-waldb
```

Tests verify:
- Failed writes do not corrupt recovery
- Sync failures propagate as typed errors
- Re-open succeeds after injected failure

## Makefile Target Reference

### Unit Test Targets

| Target            | Package Path                       |
|-------------------|------------------------------------|
| `test`            | `./...` (all packages)             |
| `test-proto`      | `./pkg/proto/...`                  |
| `test-config`     | `./pkg/config/...`                 |
| `test-queue`      | `./pkg/internal/queue/...`         |
| `test-stopper`    | `./pkg/internal/stopper/...`       |
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
| `test-discovery`  | `./pkg/discovery/...`              |
| `test-bootstrap`  | `./pkg/bootstrap/...`              |
| `test-server`     | `./pkg/internal/server/...`        |
| `test-writemode`  | `./pkg/writemode/...`              |
| `test-batch`      | `./pkg/batch/...`                  |

All unit test targets use `go test -race -count=1`.

### Coverage Targets

Same package set as unit tests, prefixed with `coverage-` instead of `test-`. Outputs profiles to `build/coverage/<package>.out` and enforces the configured minimum threshold. Example: `make coverage-raft`.

`make coverage` runs total coverage across all packages.

### Integration Test Targets

| Target                         | Runs Inside         | Suite                 |
|--------------------------------|---------------------|-----------------------|
| `integration-test`             | devcontainer        | All integration tests |
| `integration-test-raft`        | devcontainer        | Raft protocol         |
| `integration-test-host`        | devcontainer        | Host API              |
| `integration-test-bootstrap`   | devcontainer        | Bootstrap/discovery   |
| `integration-test-e2e`         | Docker Compose      | Multi-node cluster    |
| `integration-test-linearizability` | devcontainer   | porcupine verification |

### ErrorFS Error Injection Targets

| Target       | Description                         |
|--------------|-------------------------------------|
| `errfs-waldb` | WAL/LogDB I/O error injection tests |

### Fuzz Testing Targets

| Target | Description |
|--------|-------------|
| `fuzz-proto` | Entry/Message round-trip |
| `fuzz-transport` | FrameHeader decode |
| `fuzz-waldb` | WAL record codec |
| `fuzz-seal` | AES-GCM crypto |
| `fuzz-session` | Session encoding |
| `fuzz-all` | All fuzz targets (FUZZ_TIME=60s) |

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
| `lint` | `golangci-lint run ./...`      |
| `vet`  | `go vet ./...`                 |
| `fmt`  | Check `gofmt` formatting       |

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

- [Architecture](../architecture/overview.md) -- Package structure, engine pipeline
- [WAL Persistence](../storage/wal.md) -- WAL internals tested by waldb suite
- [Engine Pipeline](../engine/pipeline.md) -- Worker pipeline tested by engine suite
- [Transport](../transport/quic.md) -- QUIC transport tested by transport suite
- [Testing Infrastructure Roadmap](infrastructure.md) -- Advanced testing: fuzzing, linearizability, error injection
