# Testing Infrastructure

## Philosophy

QuicRaft's testing infrastructure verifies correctness under normal and adversarial conditions. Tests are organized into categories based on scope and resource requirements, with specialized frameworks for distributed invariants, failure injection, and linearizability verification.

Core principles:

- **Unit tests**: in-memory, fast, 100% coverage enforced
- **Integration tests**: real QUIC transport, multi-node clusters, devcontainer-isolated
- **E2E tests**: Docker Compose orchestration, full lifecycle testing
- **Fuzz testing**: Go native fuzzing, codec round-trip verification
- **Error injection**: ErrorFS framework for I/O failure paths
- **Linearizability**: porcupine verification for distributed consistency

## Test Categories

### Unit Tests

Unit tests run locally with race detection enabled (`go test -race -count=1`). The Makefile enforces 91%+ coverage per package.

25 package test targets:
- Core: `test-proto`, `test-config`, `test-queue`, `test-stopper`, `test-invariant`
- Storage: `test-logdb`, `test-raft`, `test-waldb`, `test-seal`
- State machine: `test-rsm`, `test-session`, `test-snapshot`, `test-sm`
- Engine: `test-engine`, `test-registry`, `test-transport`
- Discovery: `test-discovery`, `test-bootstrap`
- Utilities: `test-quicraft`, `test-errors`, `test-server`, `test-writemode`, `test-batch`

Run via `make test` (all) or `make test-<package>`.

### Integration Tests

Integration tests use the `//go:build integration` tag and run inside devcontainers. Four suites:

- **Raft protocol** (`test/integration/raft/`): Multi-node Raft behavior (leadership, log replication, snapshots, stress)
- **Host API** (`test/integration/host/`): Public Host interface (SyncPropose, SyncRead, StaleRead, membership)
- **Bootstrap** (`test/integration/bootstrap/`): Cluster formation via discovery
- **Linearizability** (`test/integration/linearizability/`): porcupine distributed consistency verification

Run via `make integration-test-<suite>`.

### E2E Tests

The `test/integration/e2e/` suite uses `.devcontainer/docker-compose.integration.yml` to spin up a 3-node cluster with health checks, then validates complete cluster behavior: election, replication, failover, membership changes, snapshot recovery, barrier encryption.

Run via `make integration-test-e2e`.

### Fuzz Testing

Native Go fuzzing (go test -fuzz) targets high-risk code paths:

| Target | Coverage |
|--------|----------|
| `fuzz-proto` | Entry/Message/Snapshot round-trip |
| `fuzz-transport` | FrameHeader parsing with malformed input |
| `fuzz-waldb` | WAL record codec and recovery |
| `fuzz-seal` | AES-GCM encrypt/decrypt across epochs |
| `fuzz-session` | Session encoding boundaries |

Corpus seeds in `testdata/fuzz/<package>/`. Run via `make fuzz-<package>` (default FUZZ_TIME=60s).

### Error Injection

ErrorFS framework (`test/errfs/`) injects I/O failures at WAL/LogDB layer to verify crash recovery:

- **Write failures**: Mid-entry writes, partial batch writes
- **Sync failures**: fsync errors after successful write
- **Read failures**: Read errors during recovery
- **Open failures**: OpenFile errors on lazy init

11 test cases verify no panic, correct typed error, successful re-open. Run via `make errfs-waldb`.

### Linearizability Verification

porcupine-based framework (`test/integration/linearizability/`) verifies distributed consistency under faults:

| Scenario | Duration | Faults |
|----------|----------|--------|
| Normal (3N/5N) | 5s | None |
| Leader failover (3N/5N) | 5s | Kill leader at 2s |
| Multiple leader failovers (5N) | 15s | Kill 2 leaders sequentially |
| Minority partition (5N) | 8s | Stop 2 followers |
| Rolling restart (5N) | 20s | Restart 3 followers sequentially |
| Snapshot under load (3N) | 10s | SnapshotEntries=50 |
| Session proposals (3N) | 5s | At-most-once delivery |
| LeaseRead (3N) | 5s | No heartbeat RTT |
| Membership changes (5N) | 15s | Add node, remove node |
| High throughput (5N) | 30s | 16 clients, 20 keys |

Run via `make integration-test-linearizability`.

## Infrastructure Components

### In-Memory LogDB

The `memdb` package provides a complete `logdb.LogDB` interface backed by Go maps. Used in unit tests to eliminate WAL overhead:

```go
h, err := quicraft.NewHost(cfg, WithMemoryLogDB(), WithoutTransport())
```

### Mock State Machines

Standard test state machine (`kvStateMachine`) implements `sm.StateMachine` with KV protocol. Companion `smTracker` wrapper provides direct access to SM internals for assertions.

### Transport Test Helpers

The `transportPair()` function creates two `QUICTransport` instances connected via loopback. The `testHandler` records batches and snapshot chunks with notification channels for synchronized assertions.

### Cluster Management

The `testCluster` struct manages N Host instances on loopback with unique ephemeral ports. Provides warmup, failover, and node replacement helpers.

### Polling Wait Functions

All E2E tests use polling-based wait functions instead of fixed sleeps:
- `waitForClusterLeader`: Leader election convergence
- `waitForCondition`: Generic poll-until-true
- `waitForApplied`: Log application progress
- `waitForReplication`: Data replication across cluster

### Docker Integration

`.devcontainer/docker-compose.integration.yml` orchestrates multi-node cluster with health checks. Integration tests run in isolated containers via `docker-compose exec`.

## Benchmarking

### Package-Level Benchmarks

Measured via `make bench-<package>`. Key targets:

| Package | Measures |
|---------|----------|
| `proto` | Entry/Message/Snapshot marshal/unmarshal |
| `raft` | Tick, HasUpdate, GetUpdate, compression |
| `waldb` | SaveState, GetEntries, persist+fsync |
| `seal` | AES-GCM encrypt/decrypt at 64B, 1KB, 64KB |
| `engine` | ReadyMap, WorkSignal, SpinMutex contention |
| `transport` | Frame marshal, header validation, send batching |
| `quicraft` | End-to-end Propose, SyncRead, leader lookup |

### Performance Comparison

The `test/performance/` directory provides cross-system benchmarking:
- QuicRaft vs Dragonboat v4
- QuicRaft vs etcd-io/raft
- QuicRaft vs OpenRaft (Rust)

Results include throughput, latency, memory, and per-scenario breakdowns with SVG chart generation.

## Coverage Targets

Enforced per-package via Makefile `check-coverage`:

```
make coverage              # Total coverage across all packages
make coverage-<package>    # Single package
```

Minimum: 91% per package. Profiles written to `build/coverage/<package>.out`.

## Known Patterns and Fixes

### E2E Failover Warmup

Tests that stop nodes require a warmup proposal and full replication before stopping any node. Without warmup, QUIC connections may not be fully established, causing re-election to stall.

### Leader Convergence Polling

After `waitForClusterLeader` returns (one leader found), followers may not yet know the leader ID. Use `waitForCondition` to poll all nodes for agreement on leader.

### ReadyMap/SwapBuffer Testing

Concurrent producer-consumer tests drain sequentially after producer completion, not concurrently. This avoids races in assertions while still validating concurrent write load.

### Rate Limit Retry

5-node clusters may hit rate limits under CI contention. Tests retry with exponential backoff (50ms, 100ms, 150ms, etc.).

## Makefile Targets

### Unit Tests
All packages via `make test` or `make test-<package>`. Race detection enabled, caching disabled.

### Coverage
`make coverage` (all) or `make coverage-<package>`. Minimum 91% enforced.

### Integration Tests
- `make integration-test` -- all suites
- `make integration-test-raft` -- Raft protocol
- `make integration-test-host` -- Host API
- `make integration-test-bootstrap` -- Bootstrap/discovery
- `make integration-test-e2e` -- E2E over Docker Compose
- `make integration-test-linearizability` -- porcupine verification

### Error Injection
`make errfs-waldb` -- WAL/LogDB I/O failure injection

### Fuzz Testing
- `make fuzz-proto` -- Entry/Message round-trip
- `make fuzz-transport` -- FrameHeader decode
- `make fuzz-waldb` -- WAL record codec
- `make fuzz-seal` -- AES-GCM crypto
- `make fuzz-session` -- Session encoding
- `make fuzz-all` -- All targets (FUZZ_TIME=60s)

### Benchmarks
`make bench-<package>` for individual packages, `make perf-compare-all` for cross-system comparison.

### Static Analysis
- `make lint` -- golangci-lint
- `make vet` -- go vet
- `make fmt` -- gofmt check

### Docker
- `make docker-build` -- Build devcontainer
- `make docker-test` -- Run unit tests in container
- `make docker-integration-down` -- Stop integration containers

## Test Statistics

- **5,400+ unit tests** across 25 packages
- **15 E2E tests** covering 9 scenarios (cluster formation, replication, failover, membership, encryption, snapshots, bootstrap)
- **5 fuzz targets** with corpus seeds
- **11 ErrorFS injection tests**
- **10 linearizability verification tests**
- **Coverage**: All packages 90%+ (enforced in CI)
- **Build time**: `make test` < 30s (with race detector), `make integration-test` ~5m

## Cross-References

- [Testing Overview](overview.md) -- Unit, integration, E2E, and benchmark categories
- [Performance Architecture](../performance/architecture.md) -- Benchmark framework and cross-system comparison
- [Raft Specification](../raft/protocol.md) -- Spec compliance testing
- [Session Management](../sessions/deduplication.md) -- At-most-once delivery verification
- [WAL Persistence](../storage/wal.md) -- WAL internals tested by ErrorFS suite
