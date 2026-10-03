# QuicRaft vs Dragonboat — `ci-node-2` profile

> **Generated file.** `perfcompare generate` deletes and rewrites this
> document from `results/*.json`; edits made here are lost on the next
> run. Written analysis belongs in [analysis.md](analysis.md), which the
> generator never touches.

| | |
|---|---|
| Profile | `ci-node-2` |
| Hardware | Intel(R) Xeon(R) Gold 6258R CPU @ 2.70GHz, 2 socket(s), 112 threads, 372.2 GiB, Ubuntu 26.04.1 LTS (Resolute Raccoon), kernel 7.0.0-31-generic |
| Specs captured | 2026-10-02T22:44:29Z |
| QuicRaft run | 2026-10-02T22:44:47Z (version 0.16.0) |
| Dragonboat run | 2026-10-02T23:22:26Z (version v4.0.1) |
| Document generated | 2026-10-02 23:48:09 UTC |

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
| Specs captured | 2026-10-02T22:44:29Z |

The capture could not obtain: Go.

## Charts

![QuicRaft vs Dragonboat](charts/quicraft_vs_dragonboat.svg)

## Head-to-head

| Scenario | QuicRaft | Dragonboat | Winner | Ratio |
|---|---|---|---|---|
| Election (WAL) | 52.3 ops/s, P50 19.06ms, P95 26.22ms, P99 27.10ms | 54.0 ops/s, P50 17.40ms, P95 22.75ms, P99 26.60ms | dragonboat | 1.03x |
| Propose Sequential (WAL) | 1.24K ops/s, P50 730.3us, P95 1.30ms, P99 1.57ms | 746.3 ops/s, P50 1.32ms, P95 1.54ms, P99 1.74ms | quicraft | 1.66x |
| Propose Concurrent/8 (WAL) | 4.60K ops/s, P50 1.67ms, P95 2.31ms, P99 2.98ms | 5.94K ops/s, P50 1.33ms, P95 1.51ms, P99 1.62ms | dragonboat | 1.29x |
| Commit Latency (WAL) | 1.28K ops/s, P50 701.1us, P95 1.23ms, P99 1.55ms | 744.6 ops/s, P50 1.32ms, P95 1.53ms, P99 1.65ms | quicraft | 1.72x |
| Sync Read (WAL) | 224.45K ops/s, P50 3.4us, P95 9.8us, P99 14.0us | 125.69K ops/s, P50 6.4us, P95 16.6us, P99 25.6us | quicraft | 1.79x |
| Stale Read (WAL) | 5.08M ops/s, P50 122ns, P95 150ns, P99 311ns | 4.71M ops/s, P50 143ns, P95 164ns, P99 294ns | quicraft | 1.08x |
| Election 3N (WAL) | 51.5 ops/s, P50 18.67ms, P95 24.64ms, P99 29.68ms | 44.3 ops/s, P50 19.77ms, P95 39.11ms, P99 48.01ms | quicraft | 1.16x |
| Propose Sequential 3N (WAL) | 474.6 ops/s, P50 1.93ms, P95 3.53ms, P99 4.60ms | 257.8 ops/s, P50 2.75ms, P95 3.73ms, P99 4.21ms | quicraft | 1.84x |
| Propose Concurrent/8 3N (WAL) | 2.07K ops/s, P50 3.39ms, P95 5.45ms, P99 6.96ms | 2.94K ops/s, P50 2.71ms, P95 3.09ms, P99 3.45ms | dragonboat | 1.42x |
| Commit Latency 3N (WAL) | 557.0 ops/s, P50 1.67ms, P95 2.63ms, P99 3.23ms | 185.3 ops/s, P50 3.45ms, P95 4.06ms, P99 4.46ms | quicraft | 3.01x |
| Sync Read 3N (WAL) | 7.16K ops/s, P50 126.7us, P95 210.0us, P99 302.3us | 3.89K ops/s, P50 198.3us, P95 409.8us, P99 561.1us | quicraft | 1.84x |
| Stale Read 3N (WAL) | 4.63M ops/s, P50 133ns, P95 251ns, P99 412ns | 4.43M ops/s, P50 147ns, P95 173ns, P99 531ns | quicraft | 1.04x |
| Election 5N (WAL) | 51.7 ops/s, P50 19.30ms, P95 24.37ms, P99 27.82ms | 48.7 ops/s, P50 19.79ms, P95 34.32ms, P99 45.92ms | quicraft | 1.06x |
| Propose Sequential 5N (WAL) | 378.0 ops/s, P50 2.49ms, P95 4.27ms, P99 5.73ms | 174.4 ops/s, P50 3.69ms, P95 4.66ms, P99 5.57ms | quicraft | 2.17x |
| Propose Concurrent/8 5N (WAL) | 1.45K ops/s, P50 5.00ms, P95 7.68ms, P99 9.48ms | 1.35K ops/s, P50 4.43ms, P95 8.38ms, P99 9.55ms | quicraft | 1.07x |
| Commit Latency 5N (WAL) | 366.8 ops/s, P50 2.62ms, P95 4.16ms, P99 5.04ms | 124.2 ops/s, P50 3.98ms, P95 4.85ms, P99 5.93ms | quicraft | 2.95x |
| Sync Read 5N (WAL) | 5.74K ops/s, P50 149.5us, P95 320.2us, P99 419.6us | 5.03K ops/s, P50 180.1us, P95 339.0us, P99 423.2us | quicraft | 1.14x |
| Stale Read 5N (WAL) | 4.48M ops/s, P50 134ns, P95 263ns, P99 448ns | 4.39M ops/s, P50 147ns, P95 216ns, P99 542ns | quicraft | 1.02x |

**Score: QuicRaft 15, Dragonboat 3, across 18 head-to-head scenarios.**

## Measured for QuicRaft only

The Dragonboat harness implements no equivalent scenario.

| Scenario | Throughput | Latency |
|---|---|---|
| Election (Memory) | 53.8 ops/s | P50 17.61ms, P95 25.60ms, P99 26.64ms |
| Propose Sequential (Memory) | 46.22K ops/s | P50 17.1us, P95 47.0us, P99 62.9us |
| Propose Concurrent/8 (Memory) | 97.92K ops/s | P50 75.8us, P95 123.9us, P99 153.0us |
| Commit Latency (Memory) | 49.94K ops/s | P50 14.9us, P95 46.2us, P99 66.9us |
| Sync Read (Memory) | 223.52K ops/s | P50 3.4us, P95 9.8us, P99 14.1us |
| Stale Read (Memory) | 4.77M ops/s | P50 132ns, P95 221ns, P99 330ns |
| Sync Read 3N (WAL+LeaseRead) | 146.60K ops/s | P50 4.2us, P95 10.9us, P99 20.2us |
| Sync Read 5N (WAL+LeaseRead) | 122.82K ops/s | P50 4.9us, P95 13.3us, P99 28.3us |

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
