# QuicRaft vs Dragonboat — `ci-node-2` profile

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
| Dragonboat run | 2026-10-04T20:34:57Z (version v4.0.1) |
| Document generated | 2026-10-04 20:57:58 UTC |

Numbers below were measured on this profile's machine only. Nothing here is
comparable with a number from another profile; do not compute a ratio across two.

## Coverage on this profile

This document compares quicraft against dragonboat. Systems measured on `ci-node-2`: quicraft, etcd, dragonboat, openraft.

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

![QuicRaft vs Dragonboat](charts/quicraft_vs_dragonboat.svg)

## Head-to-head

| Scenario | QuicRaft | Dragonboat | Winner | Ratio |
|---|---|---|---|---|
| Election (WAL) | 55.0 ops/s, P50 18.00ms, P95 24.63ms, P99 26.95ms | 59.8 ops/s, P50 16.72ms, P95 21.68ms, P99 25.62ms | dragonboat | 1.09x |
| Propose Sequential (WAL) | 1.21K ops/s, P50 738.4us, P95 1.41ms, P99 1.63ms | 711.7 ops/s, P50 1.37ms, P95 1.65ms, P99 1.96ms | quicraft | 1.70x |
| Propose Concurrent/8 (WAL) | 4.35K ops/s, P50 1.74ms, P95 2.42ms, P99 2.54ms | 5.56K ops/s, P50 1.41ms, P95 1.67ms, P99 1.91ms | dragonboat | 1.28x |
| Commit Latency (WAL) | 1.21K ops/s, P50 740.1us, P95 1.46ms, P99 1.64ms | 730.5 ops/s, P50 1.35ms, P95 1.60ms, P99 1.77ms | quicraft | 1.65x |
| Sync Read (WAL) | 189.50K ops/s, P50 4.5us, P95 11.3us, P99 17.0us | 99.73K ops/s, P50 7.8us, P95 22.2us, P99 36.7us | quicraft | 1.90x |
| Stale Read (WAL) | 5.54M ops/s, P50 118ns, P95 138ns, P99 197ns | 4.45M ops/s, P50 142ns, P95 187ns, P99 354ns | quicraft | 1.25x |
| Election 3N (WAL) | 53.2 ops/s, P50 18.46ms, P95 24.59ms, P99 27.33ms | 50.8 ops/s, P50 18.44ms, P95 26.12ms, P99 44.03ms | quicraft | 1.05x |
| Propose Sequential 3N (WAL) | 554.5 ops/s, P50 1.66ms, P95 2.60ms, P99 3.06ms | 359.3 ops/s, P50 2.74ms, P95 3.27ms, P99 4.13ms | quicraft | 1.54x |
| Propose Concurrent/8 3N (WAL) | 2.43K ops/s, P50 3.23ms, P95 4.71ms, P99 5.40ms | 2.71K ops/s, P50 2.90ms, P95 3.53ms, P99 4.30ms | dragonboat | 1.12x |
| Commit Latency 3N (WAL) | 498.5 ops/s, P50 1.80ms, P95 3.58ms, P99 4.13ms | 235.1 ops/s, P50 3.47ms, P95 4.17ms, P99 4.85ms | quicraft | 2.12x |
| Sync Read 3N (WAL) | 7.01K ops/s, P50 127.0us, P95 228.5us, P99 340.3us | 8.34K ops/s, P50 115.0us, P95 165.3us, P99 205.9us | dragonboat | 1.19x |
| Stale Read 3N (WAL) | 5.32M ops/s, P50 122ns, P95 145ns, P99 327ns | 4.37M ops/s, P50 145ns, P95 172ns, P99 499ns | quicraft | 1.22x |
| Election 5N (WAL) | 53.1 ops/s, P50 18.48ms, P95 24.28ms, P99 27.48ms | 49.5 ops/s, P50 18.87ms, P95 35.64ms, P99 44.09ms | quicraft | 1.07x |
| Propose Sequential 5N (WAL) | 339.5 ops/s, P50 2.87ms, P95 4.46ms, P99 5.30ms | 140.2 ops/s, P50 4.09ms, P95 4.99ms, P99 7.09ms | quicraft | 2.42x |
| Propose Concurrent/8 5N (WAL) | 1.70K ops/s, P50 4.63ms, P95 6.83ms, P99 7.88ms | 2.28K ops/s, P50 3.50ms, P95 4.29ms, P99 5.39ms | dragonboat | 1.34x |
| Commit Latency 5N (WAL) | 378.5 ops/s, P50 2.53ms, P95 4.00ms, P99 4.76ms | 206.7 ops/s, P50 3.83ms, P95 4.86ms, P99 6.31ms | quicraft | 1.83x |
| Sync Read 5N (WAL) | 5.24K ops/s, P50 155.4us, P95 366.2us, P99 462.4us | 5.70K ops/s, P50 168.3us, P95 242.4us, P99 334.2us | dragonboat | 1.09x |
| Stale Read 5N (WAL) | 5.12M ops/s, P50 125ns, P95 173ns, P99 452ns | 4.36M ops/s, P50 143ns, P95 182ns, P99 536ns | quicraft | 1.17x |

**Score: QuicRaft 12, Dragonboat 6, across 18 head-to-head scenarios.**

## Measured for QuicRaft only

The Dragonboat harness implements no equivalent scenario.

| Scenario | Throughput | Latency |
|---|---|---|
| Election (Memory) | 57.8 ops/s | P50 17.12ms, P95 23.53ms, P99 26.47ms |
| Propose Sequential (Memory) | 36.48K ops/s | P50 21.5us, P95 60.9us, P99 83.3us |
| Propose Concurrent/8 (Memory) | 89.35K ops/s | P50 82.9us, P95 138.8us, P99 169.3us |
| Commit Latency (Memory) | 38.25K ops/s | P50 20.4us, P95 58.9us, P99 80.8us |
| Sync Read (Memory) | 199.66K ops/s | P50 4.1us, P95 10.9us, P99 16.4us |
| Stale Read (Memory) | 5.54M ops/s | P50 118ns, P95 137ns, P99 239ns |
| Sync Read 3N (WAL+LeaseRead) | 121.62K ops/s | P50 5.3us, P95 12.2us, P99 24.9us |
| Sync Read 5N (WAL+LeaseRead) | 91.74K ops/s | P50 6.8us, P95 16.9us, P99 41.1us |

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
