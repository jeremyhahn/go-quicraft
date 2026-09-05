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
make perf-run PERF_PROFILE=<name> PERF_SYSTEMS=quicraft,etcd
make perf-generate PERF_PROFILE=<name> PERF_SYSTEMS=quicraft,etcd
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

## Baseline version

The numbers above were taken against `go.etcd.io/raft/v3` **v3.6.0**. The
harness now builds against **v3.7.0** (#127), and that release changes what
the etcd column measures on its own, before anything QuicRaft-side moves:
`raftpb` left gogo-protobuf for `google.golang.org/protobuf` generated types,
so every proto2 scalar is a heap-allocated pointer, `Ready` carries
`[]*raftpb.Entry` / `*raftpb.HardState` / `[]*raftpb.Message`, and
`appendEntry` `proto.Clone`s each proposed entry.

The migration was measured before and after on one machine, same binary
shape (`-benchtime 2s -benchmem`, pinned to four cores, before/after
interleaved, 14 rounds; a shared CI host at load 7-100 on 24 cores, so the
timing intervals are wide). Every allocation metric moved, deterministically,
and three timing metrics moved outside noise. Everything else was `~`
(benchstat, p > 0.05).

| Benchmark | allocs/op v3.6.0 → v3.7.0 | B/op | Timing |
|---|---|---|---|
| Propose (Sequential, WAL) | 15 → 45 (+200%) | +85% | ~ |
| Propose (Sequential, Memory) | 13 → 43 (+231%) | +82% | **P50 6 → 8 µs (+33%)** |
| Propose (Concurrent/8, WAL) | 13 → 32 (+146%) | +21% | ~ |
| Propose (Concurrent/8, Memory) | 12 → 39 (+225%) | +60% | **P50 30 → 41 µs (+37%)** |
| Commit Latency (WAL) | 15 → 45 (+200%) | +84% | ~ |
| Commit Latency (Memory) | 13 → 43 (+231%) | +80% | **P50 6 → 8 µs (+33%)** |
| Election (single, WAL / Memory) | +22% / +23% | +9% | ~ |
| 3N propose / commit (TLS + WAL) | +198% to +229% | +37% to +60% | ~ |
| 5N propose / commit (TLS + WAL) | +210% to +256% | +37% to +41% (concurrent −12%) | ~ |
| 3N / 5N election | +30% / +57% | ~ / +9% | ~ |

A `-memprofilerate=1` profile of the sequential in-memory proposal attributes
all thirty added allocations to the library: `raft.hardState` (+5, one
`HardState` plus a `new()` per field), `newStorageAppendRespMsg` (+4),
`appendEntry` (+3, `proto.Clone` plus `new(Term)`/`new(Index)`),
`node.Propose` (+2), `newStorageApplyRespMsg` (+2), `raft.send` (+1),
`MessageType.Enum` (+2), and the protobuf runtime's `unsafe_New`/`mergeBytes`
(+2). The harness's own per-proposal allocations, the wait-timer and the state
machine's value copy, are identical in both profiles: the adaptation in this
directory is value-to-pointer at each call site and adds nothing on the hot
path.

What that means for the tables above: the etcd **Memory** rows would read
roughly 2 µs higher at P50 on v3.7.0, which is the cost of ~30 extra small heap
allocations per proposal; the **WAL** and **3N/5N** rows are fsync- and
TLS-bound and did not move within measurement noise. The QuicRaft column is
untouched by this change. The absolute numbers are from different hardware
and are left as recorded; refresh them with `make perf-run` rather than
scaling them.
