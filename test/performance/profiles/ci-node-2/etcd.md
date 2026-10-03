# QuicRaft vs Etcd — `ci-node-2` profile

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
| Etcd run | 2026-10-02T23:07:29Z (version v3.7.0) |
| Document generated | 2026-10-02 23:48:09 UTC |

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
| Specs captured | 2026-10-02T22:44:29Z |

The capture could not obtain: Go.

## Charts

![QuicRaft vs Etcd](charts/quicraft_vs_etcd.svg)

## Head-to-head

| Scenario | QuicRaft | Etcd | Winner | Ratio |
|---|---|---|---|---|
| Election (WAL) | 52.3 ops/s, P50 19.06ms, P95 26.22ms, P99 27.10ms | 59.0 ops/s, P50 16.90ms, P95 22.09ms, P99 23.10ms | etcd | 1.13x |
| Election (Memory) | 53.8 ops/s, P50 17.61ms, P95 25.60ms, P99 26.64ms | 61.4 ops/s, P50 16.31ms, P95 21.30ms, P99 22.20ms | etcd | 1.14x |
| Propose Sequential (WAL) | 1.24K ops/s, P50 730.3us, P95 1.30ms, P99 1.57ms | 716.1 ops/s, P50 1.33ms, P95 1.70ms, P99 2.67ms | quicraft | 1.73x |
| Propose Sequential (Memory) | 46.22K ops/s, P50 17.1us, P95 47.0us, P99 62.9us | 1.91K ops/s, P50 101.2us, P95 1.14ms, P99 1.18ms | quicraft | 24.21x |
| Propose Concurrent/8 (WAL) | 4.60K ops/s, P50 1.67ms, P95 2.31ms, P99 2.98ms | 3.87K ops/s, P50 2.05ms, P95 2.48ms, P99 2.99ms | quicraft | 1.19x |
| Propose Concurrent/8 (Memory) | 97.92K ops/s, P50 75.8us, P95 123.9us, P99 153.0us | 42.86K ops/s, P50 121.2us, P95 764.4us, P99 1.15ms | quicraft | 2.28x |
| Commit Latency (WAL) | 1.28K ops/s, P50 701.1us, P95 1.23ms, P99 1.55ms | 708.4 ops/s, P50 1.36ms, P95 1.64ms, P99 2.44ms | quicraft | 1.81x |
| Commit Latency (Memory) | 49.94K ops/s, P50 14.9us, P95 46.2us, P99 66.9us | 1.80K ops/s, P50 151.1us, P95 1.15ms, P99 1.22ms | quicraft | 27.70x |
| Election 3N (WAL) | 51.5 ops/s, P50 18.67ms, P95 24.64ms, P99 29.68ms | 11.6 ops/s, P50 83.59ms, P95 107.43ms, P99 113.08ms | quicraft | 4.43x |
| Propose Sequential 3N (WAL) | 474.6 ops/s, P50 1.93ms, P95 3.53ms, P99 4.60ms | 373.3 ops/s, P50 2.59ms, P95 3.34ms, P99 3.81ms | quicraft | 1.27x |
| Propose Concurrent/8 3N (WAL) | 2.07K ops/s, P50 3.39ms, P95 5.45ms, P99 6.96ms | 1.49K ops/s, P50 5.29ms, P95 6.47ms, P99 7.15ms | quicraft | 1.39x |
| Commit Latency 3N (WAL) | 557.0 ops/s, P50 1.67ms, P95 2.63ms, P99 3.23ms | 346.5 ops/s, P50 2.79ms, P95 3.63ms, P99 3.99ms | quicraft | 1.61x |
| Election 5N (WAL) | 51.7 ops/s, P50 19.30ms, P95 24.37ms, P99 27.82ms | 6.5 ops/s, P50 148.96ms, P95 187.87ms, P99 284.58ms | quicraft | 8.00x |
| Propose Sequential 5N (WAL) | 378.0 ops/s, P50 2.49ms, P95 4.27ms, P99 5.73ms | 341.9 ops/s, P50 2.77ms, P95 4.03ms, P99 6.34ms | quicraft | 1.11x |
| Propose Concurrent/8 5N (WAL) | 1.45K ops/s, P50 5.00ms, P95 7.68ms, P99 9.48ms | 1.26K ops/s, P50 6.22ms, P95 8.26ms, P99 10.24ms | quicraft | 1.15x |
| Commit Latency 5N (WAL) | 366.8 ops/s, P50 2.62ms, P95 4.16ms, P99 5.04ms | 332.4 ops/s, P50 2.92ms, P95 3.66ms, P99 4.91ms | quicraft | 1.10x |

**Score: QuicRaft 14, Etcd 2, across 16 head-to-head scenarios.**

## Measured for QuicRaft only

The Etcd harness implements no equivalent scenario.

| Scenario | Throughput | Latency |
|---|---|---|
| Sync Read (WAL) | 224.45K ops/s | P50 3.4us, P95 9.8us, P99 14.0us |
| Sync Read (Memory) | 223.52K ops/s | P50 3.4us, P95 9.8us, P99 14.1us |
| Stale Read (WAL) | 5.08M ops/s | P50 122ns, P95 150ns, P99 311ns |
| Stale Read (Memory) | 4.77M ops/s | P50 132ns, P95 221ns, P99 330ns |
| Sync Read 3N (WAL) | 7.16K ops/s | P50 126.7us, P95 210.0us, P99 302.3us |
| Sync Read 3N (WAL+LeaseRead) | 146.60K ops/s | P50 4.2us, P95 10.9us, P99 20.2us |
| Stale Read 3N (WAL) | 4.63M ops/s | P50 133ns, P95 251ns, P99 412ns |
| Sync Read 5N (WAL) | 5.74K ops/s | P50 149.5us, P95 320.2us, P99 419.6us |
| Sync Read 5N (WAL+LeaseRead) | 122.82K ops/s | P50 4.9us, P95 13.3us, P99 28.3us |
| Stale Read 5N (WAL) | 4.48M ops/s | P50 134ns, P95 263ns, P99 448ns |

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
