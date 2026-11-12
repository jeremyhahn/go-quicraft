# Testing overview

> **Status: REFERENCE** · Scope: how this repository's suites are organised and
> run: `Makefile`, `test/`, `.github/workflows/ci.yml` · Last verified: 2026-10-13

The commands, the common test patterns and the Makefile target reference. What
each suite is built from, which build tag gates it, and why each gate is shaped
the way it is are in [Testing infrastructure](infrastructure.md), which also
states the two rules that outrank coverage.

## Test categories

| Category | Tag | Run with |
|---|---|---|
| Unit | none | `make test`, `make test-<pkg>` |
| Integration and E2E | `integration` | `make integration-test` (devcontainer), `make ci-integration` (host, CI parity) |
| Network partitions | `integration quicraft_netfault` | `make ci-netfault` |
| I/O fault injection | `quicraft_errfs` | `make errfs-waldb`, `make ci-errfs` |
| Fuzz | `quicraft_fuzz` | `make fuzz-<pkg>`, `make ci-fuzz-seeds`, `make ci-fuzz-mutation` |
| Benchmarks | none | `make bench-<pkg>` (smoke), `make bench-compare` (regression) |
| Cross-system performance | none | `make perf-*` |

`GOTEST := go test -race -count=1 -v`, so every unit and coverage run has the
race detector on and never reads cached results. No Makefile target passes
`-short`; the two `testing.Short()` guards in `pkg/logdb/waldb/crash_test.go`
never fire.

### Unit tests

Unit tests run in memory (`t.TempDir()` for filesystem state), finish in
seconds, and never modify the host. Most use the in-memory LogDB or a mock.
`make test` runs `go test ./...` with no build tags, which also picks up the
untagged tests under `test/` (the injector self-tests, the `test/netport`,
`test/proposer` and `test/membership` helpers' own tests, `test/buildgates`
and the `test/releasability` gate).

### Integration and E2E tests

Four packages under `test/integration/`, all behind `//go:build integration`.
The package table and the reasoning behind `-p 1` and the timeouts are in
[infrastructure.md](infrastructure.md#integration-tests).

| Target | Runs | Where |
|---|---|---|
| `integration-test` | all four packages, `-p 1`, 900s | devcontainer |
| `integration-test-raft` | `test/integration/raft/` | devcontainer |
| `integration-test-host` | `test/integration/host/` | devcontainer |
| `integration-test-linearizability` | `test/integration/linearizability/`, 600s | devcontainer |
| `integration-test-e2e` | `test/integration/e2e/` as a compiled binary | Docker Compose test-runner container |
| `ci-integration` | all four packages, `-p 1`, 1800s | host; same command as the `integration` job in ci.yml |
| `ci-netfault` | `test/integration/e2e/` and `test/integration/host/` with `quicraft_netfault` | host |

`make integration-test-e2e` brings up
`.devcontainer/docker-compose.integration.yml`, a single `test-runner`
container. The e2e tests build their own loopback clusters, so the container
contributes the pinned toolchain and tuned UDP buffers, not topology.

### Benchmarks

`GOBENCH := go test -run=^$ -bench=. -benchmem`. The `bench-<pkg>` targets are
single unreplicated runs and cannot detect a regression; spread on a loaded
machine reaches 5x. Use `make bench-compare` for that. See
[Benchmark regression testing](benchmarking.md).

`test/performance/` compares QuicRaft against dragonboat, etcd/raft and
OpenRaft through the `perfcompare` orchestrator, running each system in Docker.
Every target that stores a result requires `PERF_PROFILE`; see
[test/performance/README.md](../../test/performance/README.md).

## Unit test patterns

### Fixture helpers

Each package builds fixtures through small helpers and cleans up with
`t.Cleanup` or `t.TempDir`.

| Package | Helpers |
|---|---|
| `pkg/internal/raft` | `raftTestCfg(shardID, replicaID)`, `newTestRaft(replicaID)`, `newTestRaftWithMembers`, plus `raftTestCfgPreVote`, `raftTestCfgCheckQuorum`, `raftTestCfgQuiesce`, `newTestRaftPreVoteDisabled`, `newTestRaftCheckQuorumDisabled` |
| `pkg/internal/engine` | `newTestHostConfig()` (2 workers, 200ms RTT, transport defaults) |
| `pkg/logdb/waldb` | `openTestDB(t)`: `Open(dir, WithNoSync(true), WithBlockSize(1024), WithMaxFileSize(64*1024))`. Tests disable fsync for speed and keep every other production behaviour. |
| `pkg/internal/transport` | `transportPair(t, deploymentID)` returns two loopback `QUICTransport`s with their `testHandler`s and a cleanup; `transportPairWith` and `transportPairWithRevocation` vary it. `testHandler.waitBatch` blocks on a notification channel instead of sleeping. |

Table-driven tests appear in both styles: `t.Run` subtests (config, session)
and a slice of cases ranged over (raft, memdb).

### Golden format fixtures

Every pinned wire and on-disk layout has a fixture under its owning package's
`testdata/golden/`, checked through `pkg/internal/golden`. `golden.Check`
compares encoded bytes with a fixture and reports the first differing offset;
`golden.Load` reads one for the decode side. Each `TestGolden*` test encodes a
fixed input (no clock, no randomness) and decodes the fixture back to it, and
runs under `-race`. `-update-golden` rewrites a package's fixtures for a
deliberate layout change only; CI never passes it. The inventory and the
regeneration procedure are in
[compatibility/engineering.md](../compatibility/engineering.md#pinned-layouts-and-their-golden-fixtures).

### In-memory storage

`pkg/logdb/memdb` implements the full `logdb.LogDB` over Go maps:

```go
h, err := quicraft.NewHost(cfg, quicraft.WithMemoryLogDB(), quicraft.WithoutTransport(), quicraft.WithoutEncryption())
```

`mockLogDB` in `pkg/host_test.go` is lighter still, with injectable errors for
Host-level error paths.

### Test state machine

`kvStateMachine`, defined once per integration package
(`test/integration/e2e/helpers_test.go`, `test/integration/host/helpers_test.go`),
stores `[op:1][keyLen:2 big-endian][key][value]` updates in a map and snapshots
the whole map. `checkIndexLocked` enforces strict forward progress of the
applied index: a repeated or rolled-back index records an apply violation, and
a jump records a gap counted by `ApplyGaps()`. `RecoverCount()` reports how many
times the machine was rebuilt from a snapshot. A duplicate or reordered apply
therefore fails on its own terms instead of being masked by a later write.

```go
createFn, tracker := kvCreateFunc()
// ... start the shard with createFn ...
val, ok := tracker.Get(shardID, replicaID).Get("my-key")
```

### Typed error assertions

Tests check error identity or type, never message text: `errors.Is` against a
sentinel, `errors.As` into a typed error.

## E2E helpers

`newTestCluster(t, n)` builds N `quicraft.Host`s on loopback over real QUIC
with mTLS. Each host gets its own keypair from `test/testpki` and a reserved
port from `freePort()`, which delegates to `test/netport`.

```go
cluster := newTestCluster(t, 3)
defer cluster.close()
trackers := cluster.startShard(1)
```

Tests poll instead of sleeping: `waitForClusterLeader`, `waitForSingleLeader`,
`waitForLeader`, `waitForApplied`, `waitForReplication`,
`waitForMembershipSize`, `waitForMembershipConverged`, `waitForNodeData`,
`waitForSnapshotArtifact`, and the generic `waitForCondition` /
`waitForConditionDiag`.

| Budget | Value | Source |
|---|---|---|
| `defaultTimeout` | 15s | one ordinary operation |
| `leaderElectionTimeout` | 30 election rounds, 30s at the suite's settings | `electionbudget.LeaderElection` |
| `failoverTimeout` | 20 election rounds, 20s | `electionbudget.Failover` |

An election round is twice `ElectionRTT` ticks, because Raft randomises the
election timer over `[ElectionRTT, 2*ElectionRTT)`. The suite runs at
`RTTMillisecond=50` and the default `ElectionRTT=10`, so one round is 1s. A cold
start gets the larger budget because it pays for connection handshakes first.

### E2E test files

`test/integration/e2e/` holds 48 scenario tests plus two harness pins in
`helpers_test.go` (the election budget pin and the host config that skips WAL
sync). Three files also need `quicraft_netfault`.

| File | Tests | Covers |
|---|---|---|
| `paused_replica_test.go` | 8 | a paused follower or leader, idle, under load and across a seal cycle; snapshot recovery after resume; a paused shard leaves its shared worker unaffected |
| `replication_test.go` | 5 | sequential and concurrent proposals, stale read from a follower, 3N and 5N |
| `asymmetric_partition_test.go` | 4 | leader isolated from a minority or majority, follower isolated, snapshot catch-up after |
| `membership_election_test.go` | 4 | membership change during an election and alongside proposals, multi-shard failover under load, leader transfer during concurrent writes |
| `sealed_replica_test.go` | 4 | a sealed follower or leader, on an idle cluster and under load |
| `cluster_test.go` | 3 | 3N election, 5N leader agreement, multiple shards on one cluster |
| `barrier_replacement_test.go` | 3 | replacement and snapshot catch-up with a barrier, key rotation mid-replacement |
| `netpartition_test.go` | 3 | `quicraft_netfault`: deposed live leader, asymmetric leader, 2-of-4 cannot elect |
| `failover_test.go` | 2 | proposals after failover, 3N and 5N |
| `membership_test.go` | 2 | add/remove round-trip, proposal after removal |
| `barrier_test.go` | 2 | propose and read through an encrypted barrier, 5N |
| `quorum_loss_test.go` | 2 | writes rejected without quorum, recovery |
| `replacement_test.go` | 1 | replace a follower in 3N |
| `snapshot_catchup_test.go` | 1 | new node catch-up via snapshot |
| `partition_test.go` | 1 | heal and reconverge after a partition |
| `quorum_loss_runbook_test.go` | 1 | the quorum-loss recovery runbook end to end |
| `election_safety_netfault_test.go` | 1 | `quicraft_netfault`: a spent term cannot be won twice |
| `propose_to_leader_netfault_test.go` | 1 | `quicraft_netfault`: a proposal resolves past an isolated incumbent leader |

`partition_test.go`, `asymmetric_partition_test.go` and `quorum_loss_test.go`
build their topologies with `stopNode`, a process kill. Only the netfault files
drop packets between replicas that are still running; see
[infrastructure.md](infrastructure.md#network-partition-injection).

### Patterns that keep E2E tests stable

- **Warm up before stopping a node.** Propose one entry and wait for it on every
  replica (`proposeAndWait`, then `waitForReplication`) so all QUIC connections
  exist before the test kills anything. Otherwise re-election can stall under
  contention.
- **Poll every node for leader agreement.** `waitForClusterLeader` returns once
  one node leads; followers may not know it yet. Use `waitForCondition` over
  `ShardInfo` on every live node.
- **Route through the current leader.** `test/proposer` and `test/membership`
  submit proposals, reads and membership changes to whichever replica leads at
  the moment of each attempt, and treat the retry of an already committed
  change as success.
- **Drain after producing.** Concurrent producer tests for `ReadyMap` and the
  swap buffer drain sequentially after the producers finish, which keeps the
  assertions race-free while still loading the structure concurrently.

## Coverage

`COVER_MIN = 91`. Every `coverage-<pkg>` target and `make coverage` (which
measures `./...` into `build/coverage/total.out`) fail below it.
`make coverage-packages` reuses that profile and holds every package under
`pkg/` to the same floor through `scripts/check-coverage-packages.sh`, so no
package hides under the total's average.

CI's `test` job applies the same two checks at `COVERAGE_THRESHOLD = 90`: one
point below the local floor, so a single package's flaky test (which
contributes no coverage when it fails) does not also fail the coverage gate.

Profiles land in `build/coverage/<label>.out`; open one with
`go tool cover -html=build/coverage/<label>.out`. The fail-closed guards and the
`-coverpkg` decision are in
[infrastructure.md](infrastructure.md#the-coverage-gate-fails-closed).

## Running heavyweight suites off-box

`make remote-<target>` rsyncs the working tree to `REMOTE_HOST` (required, no
default; `REMOTE_USER` and `REMOTE_WORKDIR` are optional) and runs
`make <target>` there:

```
make remote-ci-full REMOTE_HOST=build-host.example
make remote-integration-test REMOTE_HOST=build-host.example
make remote-test REMOTE_HOST=build-host.example REMOTE_WORKDIR='~/ci/qr-myfeature'
make remote-tools REMOTE_HOST=build-host.example   # one-time scanner install
```

Develop with targeted local runs; do final verification through the Makefile
targets on the remote host. Why is in
[infrastructure.md](infrastructure.md#running-heavyweight-suites-off-box).

## Makefile target reference

### Unit and coverage

| Target | Package path |
|---|---|
| `test` | `./...` |
| `test-proto` | `./pkg/proto/...` |
| `test-config` | `./pkg/config/...` |
| `test-queue` | `./pkg/internal/queue/...` |
| `test-invariant` | `./pkg/internal/invariant/...` |
| `test-logdb` | `./pkg/logdb/...` |
| `test-raft` | `./pkg/internal/raft/...` |
| `test-waldb` | `./pkg/logdb/waldb/...` |
| `test-seal` | `./pkg/seal/...` |
| `test-rsm` | `./pkg/internal/rsm/...` |
| `test-session` | `./pkg/internal/session/...` |
| `test-engine` | `./pkg/internal/engine/...` |
| `test-registry` | `./pkg/internal/registry/...` |
| `test-transport` | `./pkg/internal/transport/...` |
| `test-quicraft`, `test-errors` | `./pkg` |
| `test-sm` | `./pkg/sm/...` |
| `test-server` | `./pkg/internal/server/...` |
| `test-writemode` | `./pkg/writemode/...` |
| `test-batch` | `./pkg/batch/...` |
| `test-crypto` | `./pkg/crypto/...` |
| `test-snapshot` | `./pkg/...`, tests matching `Snapshot\|Import\|Export` |

Every `test-<pkg>` lists its selection first and fails if it selects no tests,
so a stale package path or `-run` pattern cannot pass vacuously. Each has a
matching `coverage-<pkg>` except `test-snapshot`, whose name-filtered run cannot
meet the per-package coverage floor. `make coverage` measures
`./...` and `make coverage-packages` checks every package from it.

### Fault injection and fuzzing

| Target | Tag | Runs |
|---|---|---|
| `errfs-waldb` | `quicraft_errfs` | `./test/errfs/...` with `-v` |
| `ci-errfs` | `quicraft_errfs` | the same without `-v` (CI parity) |
| `ci-netfault` | `integration quicraft_netfault` | `./test/integration/e2e/...` and `./test/integration/host/...` |
| `fuzz-proto`, `fuzz-waldb`, `fuzz-transport`, `fuzz-session`, `fuzz-seal`, `fuzz-raft` | `quicraft_fuzz` | every target in the package, one at a time, for `FUZZ_TIME` (60s) each |
| `fuzz-all` | `quicraft_fuzz` | all six of the above |
| `ci-fuzz-seeds` | `quicraft_fuzz` | `-run=Fuzz` over `./...`: seeds and committed corpus only |
| `ci-fuzz-mutation` | `quicraft_fuzz` | a fixed execution count per target; the release gate |

### CI pipeline

| Target | Runs |
|---|---|
| `ci` | `fmt`, `no-mutants`, `check-tool-versions`, `vet`, `lint`, `security`, `build`, `test`. Untagged suites only; the fast inner loop. |
| `ci-full` | `ci`, then `ci-modules`, `ci-cross`, `ci-errfs`, `ci-fuzz-seeds`, `ci-fuzz-mutation`, `ci-integration`, `ci-netfault`, `build-fips`, `verify-fips`, `verify-fips-exempt`. Run it before opening a pull request. |
| `ci-modules` | `go build` and `go vet` in every `go.mod` directory, with `GOWORK=off` |
| `ci-cross` | the same for each of `CROSS_PLATFORMS`: linux/arm64, linux/386, windows/amd64, darwin/arm64, freebsd/amd64 |
| `remote-<target>` | `<target>` on `REMOTE_HOST` |
| `remote-tools` | installs the remote host's scanners |

### Static analysis and security

| Target | Runs |
|---|---|
| `fmt` | fails if `gofmt -l .` lists anything |
| `vet` | `go vet ./...` |
| `lint` | `golangci-lint run --build-tags benchmark ./...` in every `go.mod` directory with the root's config, after `check-lint-toolchain`; then `actionlint` and `api-check` |
| `api-check` | the exported API of every public package (root module and `contrib/dragonboat`) against the last `v*` tag with the pinned `apidiff`; from v1.0.0 an incompatible change fails unless `VERSION` is a new major ([compatibility/engineering.md](../compatibility/engineering.md#exported-api-gate)) |
| `no-mutants` | fails if `if false {`, `MUTANT` or `MUTATION` appears in non-test Go under `pkg/`, `test/`, `contrib/` |
| `security` | `gosec`, `vuln` (govulncheck), `modverify` (`go mod verify`), `trivy` (filesystem scan) |
| `trivy-image`, `nancy`, `security-sarif` | opt-in scans outside `ci` |
| `deps-check` | every module's direct dependencies against their latest release |

### Docker

| Target | Runs |
|---|---|
| `docker-build` | builds the `toolchain` and `integration` stages of `build/docker/devcontainer/Dockerfile`, with every pin from `scripts/tool-pins.sh` |
| `docker-run CMD=…` | `CMD` in the toolchain image as the invoking user, checkout bind-mounted (compose service `toolchain`) |
| `toolchain-image` | prints the toolchain image tag, hashed from the Dockerfile, the pin file, `go.mod` and `go.sum` |
| `integration-test` | the integration suites under `-race` in the integration image (compose service `integration`: read-only, no network, no capabilities) |
| `docker-build-dev` | builds the legacy dev image (`Dockerfile.dev`), tagged for this checkout |
| `docker-build-integration` | builds the legacy integration image (`.devcontainer/Dockerfile`) |
| `docker-make` | `make $(DOCKER_MAKE_TARGET)` (default `ci`) in a fresh dev image; nothing is bind-mounted, so it honours `DOCKER_HOST` |
| `docker-clean` | removes this checkout's `docker-build*` images |
| `docker-shell` | a legacy devcontainer shell |
| `docker-test` | `make test` in the devcontainer |
| `docker-integration-down` | stops and removes the integration compose stack |

Module credentials for the forge come from `.devcontainer/.env` (format:
`.devcontainer/.env.example`): the builds mount it as the `gomodauth` BuildKit
secret, and `docker-make` and the compose services load it as the container
environment. See [Contributing](../../CONTRIBUTING.md#running-the-suites-in-the-dev-container).

## Cross-references

- [Testing infrastructure](infrastructure.md): suites, injectors, fuzzing, linearizability, gates
- [Benchmark regression testing](benchmarking.md): the `bench-compare` method
- [Pre-RC evidence](rc-evidence.md): the pre-RC pass on `main`, including draft
  pins, fuzz corpora, hot-path benchmarks against the last release, leadership tests
  and the batch run
- [Contributing](../../CONTRIBUTING.md): what a change is expected to bring with it
- Per-feature test strategy lives in each feature's `engineering.md`, for
  example [log-storage](../log-storage/engineering.md) and
  [quic-transport](../quic-transport/engineering.md).
