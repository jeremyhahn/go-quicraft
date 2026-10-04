# QuicRaft vs Etcd — `ci-node-2` profile

> **Generated file.** `perfcompare generate` deletes and rewrites this
> document from `results/*.json`; edits made here are lost on the next
> run. Written analysis belongs in [analysis.md](analysis.md), which the
> generator never touches.

| | |
|---|---|
| Profile | `ci-node-2` |
| Hardware | Intel(R) Xeon(R) Gold 6258R CPU @ 2.70GHz, 2 socket(s), 112 threads, 372.2 GiB, Ubuntu 26.04.1 LTS (Resolute Raccoon), kernel 7.0.0-31-generic |
| Specs captured | 2026-10-04T20:00:07Z |
| QuicRaft run | 2026-10-04T20:00:25Z (version 0.17.0) |
| Etcd run | 2026-10-04T20:22:14Z (version v3.7.0) |
| Document generated | 2026-10-04 20:57:58 UTC |

Numbers below were measured on this profile's machine only. Nothing here is
comparable with a number from another profile; do not compute a ratio across two.

## Coverage on this profile

This document compares quicraft against etcd. Systems measured on `ci-node-2`: quicraft, etcd, dragonboat, openraft.

Every known system has results on this profile.

## Machine

Captured on the target at run time by `capture-env.sh`; never hand-edited.

| Component | Value |
|---|---|
| Profile | ci-node-2 |
| Hostname | ci-node-2 |
| CPU | Intel(R) Xeon(R) Gold 6258R CPU @ 2.70GHz |
| Sockets | 2 |
| Physical cores | 56 |
| Threads | 112 |
| NUMA nodes | 2 |
| Memory | 372.2 GiB |
| OS | Ubuntu |
| OS version | 26.04.1 LTS (Resolute Raccoon) |
| Kernel | 7.0.0-31-generic |
| Docker | 29.1.3 |
| Go | **not captured** |
| Specs captured | 2026-10-04T20:00:07Z |

The capture could not obtain: Go.

## Charts

![QuicRaft vs Etcd](charts/quicraft_vs_etcd.svg)

## Head-to-head

| Scenario | QuicRaft | Etcd | Winner | Ratio |
|---|---|---|---|---|
| Election (WAL) | 55.0 ops/s, P50 18.00ms, P95 24.63ms, P99 26.95ms | 55.2 ops/s, P50 18.19ms, P95 23.49ms, P99 24.93ms | etcd |  |
| Election (Memory) | 57.8 ops/s, P50 17.12ms, P95 23.53ms, P99 26.47ms | 59.1 ops/s, P50 16.90ms, P95 22.16ms, P99 23.55ms | etcd | 1.02x |
| Propose Sequential (WAL) | 1.21K ops/s, P50 738.4us, P95 1.41ms, P99 1.63ms | 726.4 ops/s, P50 1.35ms, P95 1.54ms, P99 1.73ms | quicraft | 1.67x |
| Propose Sequential (Memory) | 36.48K ops/s, P50 21.5us, P95 60.9us, P99 83.3us | 41.98K ops/s, P50 20.5us, P95 43.6us, P99 59.8us | etcd | 1.15x |
| Propose Concurrent/8 (WAL) | 4.35K ops/s, P50 1.74ms, P95 2.42ms, P99 2.54ms | 3.68K ops/s, P50 2.14ms, P95 2.49ms, P99 2.86ms | quicraft | 1.18x |
| Propose Concurrent/8 (Memory) | 89.35K ops/s, P50 82.9us, P95 138.8us, P99 169.3us | 75.91K ops/s, P50 100.9us, P95 162.0us, P99 199.1us | quicraft | 1.18x |
| Commit Latency (WAL) | 1.21K ops/s, P50 740.1us, P95 1.46ms, P99 1.64ms | 725.4 ops/s, P50 1.35ms, P95 1.54ms, P99 1.77ms | quicraft | 1.67x |
| Commit Latency (Memory) | 38.25K ops/s, P50 20.4us, P95 58.9us, P99 80.8us | 42.35K ops/s, P50 20.2us, P95 43.1us, P99 58.1us | etcd | 1.11x |
| Election 3N (WAL) | 53.2 ops/s, P50 18.46ms, P95 24.59ms, P99 27.33ms | 55.5 ops/s, P50 17.32ms, P95 23.40ms, P99 36.46ms | etcd | 1.04x |
| Propose Sequential 3N (WAL) | 554.5 ops/s, P50 1.66ms, P95 2.60ms, P99 3.06ms | 391.4 ops/s, P50 2.52ms, P95 2.98ms, P99 3.37ms | quicraft | 1.42x |
| Propose Concurrent/8 3N (WAL) | 2.43K ops/s, P50 3.23ms, P95 4.71ms, P99 5.40ms | 1.60K ops/s, P50 4.68ms, P95 7.94ms, P99 9.12ms | quicraft | 1.52x |
| Commit Latency 3N (WAL) | 498.5 ops/s, P50 1.80ms, P95 3.58ms, P99 4.13ms | 399.8 ops/s, P50 2.45ms, P95 2.85ms, P99 3.26ms | quicraft | 1.25x |
| Election 5N (WAL) | 53.1 ops/s, P50 18.48ms, P95 24.28ms, P99 27.48ms | 54.6 ops/s, P50 17.23ms, P95 28.91ms, P99 37.14ms | etcd | 1.03x |
| Propose Sequential 5N (WAL) | 339.5 ops/s, P50 2.87ms, P95 4.46ms, P99 5.30ms | 324.7 ops/s, P50 3.00ms, P95 3.71ms, P99 4.60ms | quicraft | 1.05x |
| Propose Concurrent/8 5N (WAL) | 1.70K ops/s, P50 4.63ms, P95 6.83ms, P99 7.88ms | 1.48K ops/s, P50 5.33ms, P95 6.80ms, P99 7.80ms | quicraft | 1.15x |
| Commit Latency 5N (WAL) | 378.5 ops/s, P50 2.53ms, P95 4.00ms, P99 4.76ms | 315.7 ops/s, P50 3.07ms, P95 3.76ms, P99 4.77ms | quicraft | 1.20x |

**Score: QuicRaft 10, Etcd 6, across 16 head-to-head scenarios.**

## Measured for QuicRaft only

The Etcd harness implements no equivalent scenario.

| Scenario | Throughput | Latency |
|---|---|---|
| Sync Read (WAL) | 189.50K ops/s | P50 4.5us, P95 11.3us, P99 17.0us |
| Sync Read (Memory) | 199.66K ops/s | P50 4.1us, P95 10.9us, P99 16.4us |
| Stale Read (WAL) | 5.54M ops/s | P50 118ns, P95 138ns, P99 197ns |
| Stale Read (Memory) | 5.54M ops/s | P50 118ns, P95 137ns, P99 239ns |
| Sync Read 3N (WAL) | 7.01K ops/s | P50 127.0us, P95 228.5us, P99 340.3us |
| Sync Read 3N (WAL+LeaseRead) | 121.62K ops/s | P50 5.3us, P95 12.2us, P99 24.9us |
| Stale Read 3N (WAL) | 5.32M ops/s | P50 122ns, P95 145ns, P99 327ns |
| Sync Read 5N (WAL) | 5.24K ops/s | P50 155.4us, P95 366.2us, P99 462.4us |
| Sync Read 5N (WAL+LeaseRead) | 91.74K ops/s | P50 6.8us, P95 16.9us, P99 41.1us |
| Stale Read 5N (WAL) | 5.12M ops/s | P50 125ns, P95 173ns, P99 452ns |

## Run configuration

| Parameter | QuicRaft | Etcd |
|---|---|---|
| Payload | 128 bytes | 128 bytes |
| Concurrent goroutines | 8 | 8 |
| TLS | yes | no |
| RTT | 1ms | 1ms |
| Election RTT | 10 ticks | 10 ticks |
| Heartbeat RTT | 1 ticks | 1 ticks |

Written analysis of these numbers lives in [analysis.md](analysis.md).
