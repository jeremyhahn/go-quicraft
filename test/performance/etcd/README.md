# QuicRaft vs etcd-io/raft Performance Benchmarks

Single-node, apples-to-apples comparison of QuicRaft against etcd-io/raft.
Both implementations use identical Raft parameters:

| Parameter | Value |
|---|---|
| ElectionRTT | 10 ticks |
| HeartbeatRTT | 1 tick |
| RTTMillisecond | 1ms |
| CheckQuorum | true |
| PreVote | true |

Each benchmark is run in two storage modes:

- **WAL** -- production configuration with durable write-ahead log and fsync
- **Memory** -- in-memory LogDB (no disk I/O), isolating raw algorithm performance

## Running

```bash
make perf-compare-etcd
```

Or manually:

```bash
docker build -t quicraft-bench-etcd -f test/performance/etcd/Dockerfile .
docker run --rm quicraft-bench-etcd
```

## Architecture Notes

### Election Time Gap

QuicRaft Memory election (~17.8ms P50) is ~1.8ms slower than etcd Memory
(~16.0ms P50). This is the fundamental cost of QuicRaft's production
architecture vs etcd's minimal benchmark harness:

| | QuicRaft | etcd benchmark |
|---|---|---|
| Goroutines at startup | 14 (4 step + 4 commit + 4 apply + 2 snapshot) | 2 (event loop + ticker) |
| Goroutine stacks | ~112KB (14 x 8KB) | ~16KB (2 x 8KB) |
| Inter-worker channels | 8 buffered (commitC + applyC per worker) | 3 minimal |
| Shard dispatch | Sharded WorkSignal with 64-bucket ReadyMaps | None |
| Tick mechanism | Identical: `time.NewTicker(1ms)` | Identical |

The ~1.8ms gap is a **one-time startup cost** from spawning and scheduling
14 worker goroutines plus allocating production infrastructure (shard
registry, session management, parallel commit/apply pipeline). The election
timeout itself (randomized 10-19 ticks x 1ms) is identical.

In WAL mode, an additional ~4ms comes from creating the shard directory and
segment file with fallocate for the one active WAL shard. Shards are lazily
initialized -- only the shard that receives writes pays this cost.

This overhead is irrelevant in production where hosts start once and run
for the lifetime of the process. QuicRaft's architecture pays for itself
with 2-3x higher throughput and 2-4x lower latency under load.

### etcd-io/raft Benchmark Harness

The etcd-io/raft benchmark uses etcd's `raft` package directly (not the full
etcd server). The harness includes:

- A single-goroutine event loop that calls `raft.Node.Ready()`, persists
  entries, and calls `Advance()`
- A WAL writer with length-prefix framing and fsync (WAL mode) or
  `raft.MemoryStorage` only (Memory mode)
- A `time.NewTicker(1ms)` goroutine calling `node.Tick()` for natural
  election timeout (no `Campaign()` shortcut)

This is the minimal viable Raft integration -- no parallel workers, no
shard management, no session tracking. It represents the theoretical
floor for etcd-io/raft performance.

## Sample Results

Intel Xeon Platinum 8260 @ 2.40GHz, 96 cores, Linux (Docker).

### Throughput and Latency

```
Scenario: Propose Throughput (Sequential)
------------------------------------------------------------------
Implementation                   Ops/sec             P50             P99
------------------------------------------------------------------
QuicRaft (WAL)                    1033.0         844.5us          2.68ms
QuicRaft (Memory)                23183.2          30.1us         319.5us
etcd-io/raft (WAL)                    473.0          2.01ms          3.60ms
etcd-io/raft (Memory)               91602.8           8.1us          35.8us
------------------------------------------------------------------
WINNER (WAL)                    QuicRaft (2.18x throughput, 2.38x P50)

Scenario: Propose Throughput (Concurrent/8)
------------------------------------------------------------------
Implementation                   Ops/sec             P50             P99
------------------------------------------------------------------
QuicRaft (WAL)                    3698.0          1.91ms          4.38ms
QuicRaft (Memory)                74099.4          93.5us         466.1us
etcd-io/raft (WAL)                   1211.9          6.42ms         10.66ms
etcd-io/raft (Memory)              149080.3          44.2us         141.4us
------------------------------------------------------------------
WINNER (WAL)                    QuicRaft (3.05x throughput, 3.37x P50)

Scenario: Commit Latency
------------------------------------------------------------------
Implementation                   Ops/sec             P50             P99
------------------------------------------------------------------
QuicRaft (WAL)                    1148.6         787.7us          2.12ms
QuicRaft (Memory)                22810.4          33.8us         142.0us
etcd-io/raft (WAL)                    454.7          2.11ms          3.67ms
etcd-io/raft (Memory)               86745.7           8.6us          35.2us
------------------------------------------------------------------
WINNER (WAL)                    QuicRaft (2.53x throughput, 2.68x P50)

Scenario: Election Time
------------------------------------------------------------------
Implementation                   Ops/sec             P50             P99
------------------------------------------------------------------
QuicRaft (WAL)                      42.3         21.90ms         31.49ms
QuicRaft (Memory)                   54.4         17.85ms         25.24ms
etcd-io/raft (WAL)                     50.5         17.54ms         26.45ms
etcd-io/raft (Memory)                  61.9         16.05ms         22.42ms
------------------------------------------------------------------
WINNER (WAL)                   etcd-io/raft (see Architecture Notes above)
```

### Summary

| Scenario | WAL Winner | Speedup |
|---|---|---|
| Propose (Sequential) | QuicRaft | 2.18x |
| Propose (Concurrent/8) | QuicRaft | 3.05x |
| Commit Latency | QuicRaft | 2.53x |
| Election Time | etcd-io/raft | 0.84x (one-time startup cost) |
