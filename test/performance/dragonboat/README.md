# QuicRaft vs Dragonboat Performance Benchmarks

Single-node and multi-node comparison of QuicRaft against Dragonboat v4.

Both implementations use identical Raft parameters:

| Parameter | Value |
|---|---|
| ElectionRTT | 10 ticks |
| HeartbeatRTT | 1 tick |
| RTTMillisecond | 1ms |
| CheckQuorum | true |
| PreVote | true |

QuicRaft uses its own WAL implementation (`pkg/logdb/waldb/`) — a custom
log-structured write-ahead log with 16 shards, fixed-layout binary encoding,
and lazy shard initialization.

Dragonboat is configured with the **tan** LogDB backend, which is also
log-structured. This is the fair comparison target — Dragonboat's default
`sharded-pebble` backend is a KV store with fundamentally different I/O
characteristics. Tan is architecturally the closest equivalent to QuicRaft's
WAL.

Each benchmark is run in two storage modes:

- **WAL** — production configuration with durable write-ahead log and fsync
- **Memory** — in-memory LogDB (no disk I/O), isolating raw algorithm performance

Dragonboat does not expose a memory-only LogDB, so memory-mode results are
shown only for QuicRaft to demonstrate the gap between algorithm speed and
production overhead.

For complete multi-node results and full analysis, see [../dragonboat.md](../dragonboat.md).

## Running

```bash
make perf-compare-dragonboat
```

Or manually:

```bash
cp -r /path/to/dragonboat .build-cache/dragonboat
docker build -t quicraft-bench-dragonboat -f test/performance/dragonboat/Dockerfile .
docker run --rm quicraft-bench-dragonboat
```

## Architecture Notes

### Concurrent Propose P99 Tail Latency

QuicRaft wins throughput, P50, P90, and P95 in the single-node concurrent propose
benchmark. Dragonboat wins P99 by ~270us (6.8%). This is not a concurrency
bottleneck — it is GC jitter from QuicRaft's higher per-operation allocation
rate.

| Metric | QuicRaft | Dragonboat | Ratio |
|---|---|---|---|
| B/op | 10,969 | 1,234 | 8.9x |
| allocs/op | 8 | 8 | 1.0x |

The allocation difference comes from QuicRaft's message-passing architecture.
Each proposal flows through an inbox channel as a `proto.Message` containing
a `[]proto.Entry` slice. This creates heap allocations that Dragonboat avoids
through tighter internal coupling.

QuicRaft's inbox-based design provides:

- **Single-owner invariant**: Only the step worker touches Raft state
- **Safe concurrent access**: No shared mutable state between proposers
- **Pipeline parallelism**: Step, commit, and apply workers run concurrently

The 270us P99 difference is the cost of this clean architecture. QuicRaft
compensates with 24% higher throughput and 39% lower median latency at single-node.

At 3N and 5N, QuicRaft now leads concurrent propose (1.21x at 3N, 1.11x at 5N).
See [../dragonboat.md](../dragonboat.md) for multi-node analysis.

### Election Time Gap

QuicRaft WAL election (~25.9ms P50) is ~4ms slower than QuicRaft Memory
(~21.0ms P50). This gap comes from creating the WAL shard directory and
segment file with fallocate on first write. Shards are lazily initialized —
only the shard that receives writes pays this one-time cost.

Compared to Dragonboat, QuicRaft elections are substantially faster at all
cluster sizes:

| Cluster | QuicRaft | Dragonboat | Ratio |
|---|---|---|---|
| 1N | ~20ms | ~538ms | QuicRaft 34.9x |
| 3N | ~22ms | ~1.53s | QuicRaft 79.2x |
| 5N | ~20ms | ~2.54s | QuicRaft 89.4x |

Dragonboat's election benchmark includes full NodeHost startup with tan LogDB
initialization. Each election iteration creates a fresh NodeHost, which opens
log files, loads MANIFEST metadata, and rebuilds per-node indices. QuicRaft's
waldb has faster cold-start with simple file opens across 16 shards.

### SyncRead Performance

SyncRead performance depends significantly on cluster topology, revealing a
fundamental architecture trade-off between the two systems.

**Single-node (QuicRaft 1.6x faster):** QuicRaft achieves ~223K ops/s vs
Dragonboat's ~139K ops/s. Both systems short-circuit ReadIndex for single-node
clusters (no heartbeat quorum needed). QuicRaft's advantage comes from the
**read-only update bypass**: when the Raft step produces only ReadyToRead
states with no entries, state changes, or snapshots, the engine step worker
skips the commit/apply pipeline entirely. The OnReadyToRead callback fires
synchronously in the step worker, completing the RequestState without goroutine
hops or LogDB writes.

**Multi-node (Dragonboat wins 2.4-17.5x):** In multi-node clusters, ReadIndex
requires a heartbeat quorum -- a majority of nodes must confirm the leader's
lease. This turns SyncRead into a network-bound operation where transport
latency and Go scheduler wake-up latency dominate.

| Cluster | QuicRaft | Dragonboat | Winner | Ratio |
|---|---|---|---|---|
| 1N | ~223K ops/s | ~139K ops/s | QuicRaft | 1.6x |
| 3N | ~2.57K ops/s | ~6.26K ops/s | Dragonboat | 2.4x |
| 5N | ~290 ops/s | ~5.07K ops/s | Dragonboat | 17.5x |

The multi-node gap is attributed to Go scheduler wake-up latency on 96-core
NUMA machines compounding with QUIC per-packet overhead for small heartbeat
confirmation messages. The 5N gap widened significantly in this run (4.1x to
17.5x). QuicRaft's LeaseRead eliminates this gap entirely: 99K ops/s at 3N
(15.8x faster than Dragonboat) and 53.6K ops/s at 5N (10.6x faster than
Dragonboat). See [../dragonboat.md](../dragonboat.md) for LeaseRead analysis.

## Sample Results

Intel Xeon Platinum 8260 @ 2.40GHz, 96 cores, Linux (Docker).

Single-node results only. Multi-node benchmarks are available in
[../dragonboat.md](../dragonboat.md).

### Throughput (1N WAL, 2026-02-21)

```
Benchmark               QR ops/s  QR(M) ops/s   DB ops/s    Speedup     Winner
────────────────────────────────────────────────────────────────────────────────
StaleRead                  4.13M        3.82M       3.35M      1.23x   QuicRaft
SyncRead                  222.7K        193K       139.0K      1.60x   QuicRaft
Propose                    1.16K       67.6K       412.7       2.82x   QuicRaft
ConcurrentPropose          3.70K      135.8K       3.49K      1.06x   QuicRaft
CommitLatency              1.21K       64.7K       413.2       2.94x   QuicRaft
Election                    48.5         45.4          1.4     34.9x   QuicRaft
```

### Latency Percentiles (1N WAL, 2026-02-21)

```
Benchmark            QR p50  QR(M)p50    DB p50    Winner
────────────────────────────────────────────────────────────
StaleRead             137ns     ---       168ns   QuicRaft
SyncRead              3.2us     ---       4.5us   QuicRaft
Propose             787.6us     ---      2.37ms   QuicRaft
ConcurrentPropose    2.10ms     ---      2.24ms   QuicRaft
CommitLatency       746.1us     ---      2.41ms   QuicRaft
Election            20.34ms     ---    537.66ms   QuicRaft
```

### Summary

| Scenario | WAL Winner | Speedup |
|---|---|---|
| StaleRead | QuicRaft | 1.15-1.23x |
| SyncRead (1N) | QuicRaft | 1.60x |
| SyncRead (3N) | Dragonboat | 2.43x |
| SyncRead (5N) | Dragonboat | 17.47x |
| Propose (Sequential) | QuicRaft | 1.28-2.82x |
| Propose (Concurrent/8) | QuicRaft | 1.06-1.21x |
| Commit Latency | QuicRaft | 1.18-2.94x |
| Election Time | QuicRaft | 34.9-89.4x |
