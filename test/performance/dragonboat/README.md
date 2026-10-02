# QuicRaft vs Dragonboat benchmark

> **Status: REFERENCE** · Scope: the Dragonboat harness in
> `test/performance/dragonboat` · Last verified: 2026-10-02

This harness runs the shared benchmark scenarios against Dragonboat v4 so they
can be compared with QuicRaft's. It covers single-node and 3- and 5-node
clusters with mutual TLS.

## What is compared

Both engines run with the same Raft timing:

| Parameter | QuicRaft | Dragonboat |
|---|---|---|
| `RTTMillisecond` | 1 | 1 |
| `ElectionRTT` | 10 ticks | 10 ticks |
| `HeartbeatRTT` | 1 tick | 1 tick |
| `CheckQuorum` | on (default) | on |
| `PreVote` | on (default) | off (Dragonboat default; the harness does not set it) |

Storage differs by design. QuicRaft uses its own 16-shard WAL
(`pkg/logdb/waldb`). The Dragonboat harness sets no `Expert.LogDBFactory`, so
Dragonboat runs its default `sharded-pebble` LogDB, a key-value store rather
than a log-structured WAL. Every scenario is measured in WAL mode with fsync.
QuicRaft is also measured with its in-memory LogDB; Dragonboat has no
memory-only LogDB, so those rows have no Dragonboat counterpart.

The engine under test is the fork `github.com/jeremyhahn/dragonboat/v4`
(`go.mod` pins v4.0.1): upstream v4.0.0 plus a Pebble v1.1.5 compatibility
fix. It is an ordinary versioned dependency, so nothing needs to be checked out
beforehand.

Read the [fairness audit](../profiles/r640/analysis.md#fairness-and-comparability)
before drawing conclusions. Some rows, elections in particular, do not measure
the same thing in both columns.

## Results

Results are stored per machine under [`../profiles/`](../profiles/). The most
recent full comparison is
[`profiles/ci-node-2/dragonboat.md`](../profiles/ci-node-2/dragonboat.md), <!-- lab-string-ok: benchmark provenance: the profile directory names the machine that measured the numbers -->
measured 2026-09-14. Numbers from different profiles are not comparable.

## Running

From the repository root:

```bash
make perf-run PERF_PROFILE=<name> PERF_SYSTEMS=quicraft,dragonboat
make perf-generate PERF_PROFILE=<name> PERF_SYSTEMS=quicraft,dragonboat
```

Or build and run the container directly; the build context must be the
repository root:

```bash
docker build -t quicraft-bench-dragonboat -f test/performance/dragonboat/Dockerfile .
docker run --rm quicraft-bench-dragonboat
```
