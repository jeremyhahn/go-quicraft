# QuicRaft vs Dragonboat — `omega` profile

> **Generated file.** `perfcompare generate` deletes and rewrites this
> document from `results/*.json`; edits made here are lost on the next
> run. Written analysis belongs in [analysis.md](analysis.md), which the
> generator never touches.

| | |
|---|---|
| Profile | `omega` |
| Hardware | Intel(R) Core(TM) i9-9900X CPU @ 3.50GHz, 1 socket(s), 20 threads, 62.5 GiB, Linux Mint 22.1 (Xia), kernel 6.8.0-58-generic |
| Specs captured | 2026-08-17T16:30:11Z |
| QuicRaft run | 2026-08-17T10:02:39Z (version 0.0.0) |
| Dragonboat run | 2026-08-17T10:35:03Z (version v4.0.0) |
| Document generated | 2026-08-17 16:32:35 UTC |

Numbers below were measured on this profile's machine only. Nothing here is
comparable with a number from another profile; do not compute a ratio across two.

## Coverage on this profile

This document compares quicraft against dragonboat. Systems measured on `omega`: quicraft, dragonboat, openraft.

**Not yet measured on `omega`: etcd.** No results file exists for it,
so no document was generated for it. This is a gap in the data, not a result.

## Machine

Captured on the target at run time by `capture-env.sh`; never hand-edited.

| Component | Value |
|---|---|
| Profile | omega |
| Hostname | omega |
| CPU | Intel(R) Core(TM) i9-9900X CPU @ 3.50GHz |
| Sockets | 1 |
| Physical cores | 10 |
| Threads | 20 |
| NUMA nodes | 1 |
| Memory | 62.5 GiB |
| OS | Linux Mint |
| OS version | 22.1 (Xia) |
| Kernel | 6.8.0-58-generic |
| Docker | 29.1.3 |
| Go | **not captured** |
| Specs captured | 2026-08-17T16:30:11Z |

The capture could not obtain: Go.

## Charts

![QuicRaft vs Dragonboat](charts/quicraft_vs_dragonboat.svg)

## Head-to-head

| Scenario | QuicRaft | Dragonboat | Winner | Ratio |
|---|---|---|---|---|
| Election (WAL) | 52.5 ops/s, P50 17.31ms, P95 23.94ms, P99 27.47ms | 41.0 ops/s, P50 24.44ms, P95 30.46ms, P99 35.27ms | quicraft | 1.28x |
| Propose Sequential (WAL) | 598.3 ops/s, P50 1.50ms, P95 3.36ms, P99 3.46ms | 186.6 ops/s, P50 5.90ms, P95 6.45ms, P99 10.90ms | quicraft | 3.21x |
| Propose Concurrent/8 (WAL) | 2.09K ops/s, P50 3.12ms, P95 5.00ms, P99 6.31ms | 644.5 ops/s, P50 12.06ms, P95 15.34ms, P99 24.23ms | quicraft | 3.24x |
| Commit Latency (WAL) | 596.9 ops/s, P50 1.50ms, P95 3.36ms, P99 3.47ms | 369.1 ops/s, P50 2.63ms, P95 2.82ms, P99 5.80ms | quicraft | 1.62x |
| Sync Read (WAL) | 109.65K ops/s, P50 7.8us, P95 14.6us, P99 21.2us | 55.83K ops/s, P50 16.1us, P95 30.3us, P99 43.1us | quicraft | 1.96x |
| Stale Read (WAL) | 3.14M ops/s, P50 251ns, P95 268ns, P99 522ns | 2.57M ops/s, P50 296ns, P95 433ns, P99 932ns | quicraft | 1.22x |
| Election 3N (WAL) | 48.2 ops/s, P50 19.33ms, P95 25.78ms, P99 33.47ms | 23.7 ops/s, P50 39.99ms, P95 57.63ms, P99 71.46ms | quicraft | 2.04x |
| Propose Sequential 3N (WAL) | 286.6 ops/s, P50 3.08ms, P95 5.21ms, P99 7.46ms | 23.8 ops/s, P50 10.04ms, P95 13.42ms, P99 22.37ms | quicraft | 12.04x |
| Propose Concurrent/8 3N (WAL) | 1.16K ops/s, P50 6.65ms, P95 10.55ms, P99 12.38ms | 112.4 ops/s, P50 10.02ms, P95 15.12ms, P99 25.90ms | quicraft | 10.30x |
| Commit Latency 3N (WAL) | 287.9 ops/s, P50 3.06ms, P95 5.22ms, P99 7.52ms | 100.0 ops/s, P50 9.77ms, P95 10.35ms, P99 15.96ms | quicraft | 2.88x |
| Sync Read 3N (WAL) | 3.01K ops/s, P50 323.2us, P95 382.3us, P99 418.8us | 3.07K ops/s, P50 319.8us, P95 389.3us, P99 424.5us | dragonboat | 1.02x |
| Stale Read 3N (WAL) | 3.15M ops/s, P50 248ns, P95 266ns, P99 535ns | 2.61M ops/s, P50 296ns, P95 381ns, P99 956ns | quicraft | 1.21x |
| Election 5N (WAL) | 47.4 ops/s, P50 21.30ms, P95 26.72ms, P99 36.20ms | 14.2 ops/s, P50 67.28ms, P95 92.24ms, P99 109.71ms | quicraft | 3.34x |
| Propose Sequential 5N (WAL) | 242.9 ops/s, P50 3.60ms, P95 6.58ms, P99 8.46ms | 45.0 ops/s, P50 9.87ms, P95 10.94ms, P99 16.29ms | quicraft | 5.39x |
| Propose Concurrent/8 5N (WAL) | 863.4 ops/s, P50 8.33ms, P95 12.82ms, P99 15.67ms | 307.5 ops/s, P50 9.90ms, P95 19.64ms, P99 22.69ms | quicraft | 2.81x |
| Commit Latency 5N (WAL) | 243.6 ops/s, P50 3.60ms, P95 6.53ms, P99 8.71ms | 42.8 ops/s, P50 9.88ms, P95 10.93ms, P99 15.98ms | quicraft | 5.69x |
| Sync Read 5N (WAL) | 2.54K ops/s, P50 384.3us, P95 455.1us, P99 499.6us | 2.69K ops/s, P50 366.8us, P95 445.1us, P99 499.0us | dragonboat | 1.06x |
| Stale Read 5N (WAL) | 3.06M ops/s, P50 249ns, P95 282ns, P99 775ns | 2.61M ops/s, P50 292ns, P95 401ns, P99 955ns | quicraft | 1.17x |

**Score: QuicRaft 16, Dragonboat 2, across 18 head-to-head scenarios.**

## Measured for QuicRaft only

The Dragonboat harness implements no equivalent scenario.

| Scenario | Throughput | Latency |
|---|---|---|
| Election (Memory) | 54.2 ops/s | P50 20.29ms, P95 25.65ms, P99 26.34ms |
| Propose Sequential (Memory) | 26.82K ops/s | P50 32.4us, P95 61.3us, P99 74.1us |
| Propose Concurrent/8 (Memory) | 67.48K ops/s | P50 110.7us, P95 154.7us, P99 180.7us |
| Commit Latency (Memory) | 26.75K ops/s | P50 32.2us, P95 61.7us, P99 74.8us |
| Sync Read (Memory) | 111.21K ops/s | P50 7.7us, P95 14.5us, P99 21.6us |
| Stale Read (Memory) | 3.14M ops/s | P50 252ns, P95 265ns, P99 499ns |
| Sync Read 3N (WAL+LeaseRead) | 60.40K ops/s | P50 10.0us, P95 23.7us, P99 39.5us |
| Sync Read 5N (WAL+LeaseRead) | 59.28K ops/s | P50 9.6us, P95 26.3us, P99 56.6us |

## Run configuration

| Parameter | QuicRaft | Dragonboat |
|---|---|---|
| Payload | 128 bytes | 128 bytes |
| Concurrent goroutines | 8 | 8 |
| TLS | yes | yes |
| RTT | 1ms | 1ms |
| Election RTT | 10 ticks | 10 ticks |
| Heartbeat RTT | 1 ticks | 1 ticks |

Written analysis of these numbers lives in [analysis.md](analysis.md).
