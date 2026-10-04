# QuicRaft vs Openraft — `ci-node-2` profile

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
| Openraft run | 2026-10-04T20:57:54.201764993+00:00 (version v0.9) |
| Document generated | 2026-10-04 20:57:58 UTC |

Numbers below were measured on this profile's machine only. Nothing here is
comparable with a number from another profile; do not compute a ratio across two.

## Coverage on this profile

This document compares quicraft against openraft. Systems measured on `ci-node-2`: quicraft, etcd, dragonboat, openraft.

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

![QuicRaft vs Openraft](charts/quicraft_vs_openraft.svg)

## Head-to-head

| Scenario | QuicRaft | Openraft | Winner | Ratio |
|---|---|---|---|---|
| Propose Sequential (WAL) | 1.21K ops/s, P50 738.4us, P95 1.41ms, P99 1.63ms | 818.3 ops/s, P50 1.22ms, P95 1.55ms, P99 1.69ms | quicraft | 1.48x |
| Propose Sequential (Memory) | 36.48K ops/s, P50 21.5us, P95 60.9us, P99 83.3us | 71.34K ops/s, P50 12.0us, P95 22.7us, P99 26.6us | openraft | 1.96x |
| Propose Concurrent/8 (WAL) | 4.35K ops/s, P50 1.74ms, P95 2.42ms, P99 2.54ms | 996.0 ops/s, P50 8.00ms, P95 9.70ms, P99 10.91ms | quicraft | 4.37x |
| Propose Concurrent/8 (Memory) | 89.35K ops/s, P50 82.9us, P95 138.8us, P99 169.3us | 245.05K ops/s, P50 28.8us, P95 52.3us, P99 71.7us | openraft | 2.74x |
| Commit Latency (WAL) | 1.21K ops/s, P50 740.1us, P95 1.46ms, P99 1.64ms | 816.3 ops/s, P50 1.22ms, P95 1.55ms, P99 1.69ms | quicraft | 1.48x |
| Commit Latency (Memory) | 38.25K ops/s, P50 20.4us, P95 58.9us, P99 80.8us | 70.33K ops/s, P50 12.1us, P95 22.0us, P99 27.3us | openraft | 1.84x |
| Election 3N (WAL) | 53.2 ops/s, P50 18.46ms, P95 24.59ms, P99 27.33ms | 36.4 ops/s, P50 26.91ms, P95 33.43ms, P99 36.01ms | quicraft | 1.46x |
| Propose Sequential 3N (WAL) | 554.5 ops/s, P50 1.66ms, P95 2.60ms, P99 3.06ms | 383.2 ops/s, P50 2.55ms, P95 3.10ms, P99 3.38ms | quicraft | 1.45x |
| Propose Concurrent/8 3N (WAL) | 2.43K ops/s, P50 3.23ms, P95 4.71ms, P99 5.40ms | 866.2 ops/s, P50 9.13ms, P95 10.97ms, P99 12.91ms | quicraft | 2.80x |
| Commit Latency 3N (WAL) | 498.5 ops/s, P50 1.80ms, P95 3.58ms, P99 4.13ms | 375.7 ops/s, P50 2.64ms, P95 3.18ms, P99 3.50ms | quicraft | 1.33x |
| Election 5N (WAL) | 53.1 ops/s, P50 18.48ms, P95 24.28ms, P99 27.48ms | 34.1 ops/s, P50 29.27ms, P95 32.27ms, P99 35.01ms | quicraft | 1.56x |
| Propose Sequential 5N (WAL) | 339.5 ops/s, P50 2.87ms, P95 4.46ms, P99 5.30ms | 364.3 ops/s, P50 2.69ms, P95 3.21ms, P99 3.72ms | openraft | 1.07x |
| Propose Concurrent/8 5N (WAL) | 1.70K ops/s, P50 4.63ms, P95 6.83ms, P99 7.88ms | 778.7 ops/s, P50 10.06ms, P95 12.97ms, P99 16.35ms | quicraft | 2.18x |
| Commit Latency 5N (WAL) | 378.5 ops/s, P50 2.53ms, P95 4.00ms, P99 4.76ms | 369.0 ops/s, P50 2.66ms, P95 3.17ms, P99 3.72ms | quicraft | 1.03x |

**Score: QuicRaft 10, Openraft 4, across 14 head-to-head scenarios.**

## Measured for QuicRaft only

The Openraft harness implements no equivalent scenario.

| Scenario | Throughput | Latency |
|---|---|---|
| Election (WAL) | 55.0 ops/s | P50 18.00ms, P95 24.63ms, P99 26.95ms |
| Election (Memory) | 57.8 ops/s | P50 17.12ms, P95 23.53ms, P99 26.47ms |
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

| Parameter | QuicRaft | Openraft |
|---|---|---|
| Payload | 128 bytes | 128 bytes |
| Concurrent goroutines | 8 | 8 |
| TLS | yes | yes |
| RTT | 1ms | 1ms |
| Election RTT | 10 ticks | 10 ticks |
| Heartbeat RTT | 1 ticks | 1 ticks |

Written analysis of these numbers lives in [analysis.md](analysis.md).
