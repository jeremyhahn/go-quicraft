# QuicRaft vs etcd-io/raft benchmark

> **Status: REFERENCE** · Scope: the etcd-io/raft harness in
> `test/performance/etcd` · Last verified: 2026-10-03

This harness runs the shared benchmark scenarios against etcd-io/raft
(`go.etcd.io/raft/v3`, pinned at v3.7.0 in `go.mod`) so they can be compared
with QuicRaft's.

## What is compared

Both engines run with the same Raft timing:

| Parameter | QuicRaft | etcd-io/raft |
|---|---|---|
| Tick | 1 ms | 1 ms (`time.NewTicker`) |
| Election timeout | 10 ticks | 10 ticks (`ElectionTick`), at every cluster size |
| Heartbeat | 1 tick | 1 tick (`HeartbeatTick`) |
| CheckQuorum | on (default) | on |
| PreVote | on (default) | on |

Each single-node scenario runs in two storage modes:

- **WAL**: a durable log with fsync on every write.
- **Memory**: no disk I/O, isolating the cost of the engine itself.

etcd-io/raft is a library, not an engine, so the harness supplies the rest. It
uses etcd's `raft` package directly, not the etcd server:

- a single goroutine runs the `Ready()` / persist / `Advance()` event loop;
- WAL mode appends length-prefixed records to a file and calls `Sync()`
  (fsync) once per `Ready`, before anything from that `Ready` is sent,
  applied or acknowledged; Memory mode uses `raft.MemoryStorage` only;
- a `time.NewTicker(1ms)` goroutine calls `Tick()`;
- leaders, in setup and in the election scenarios, come from the randomized
  election timeout, never from `Campaign()`;
- each proposal carries a request id in its value padding (the command stays
  128 bytes), and the apply loop wakes the proposer of that entry, so a
  proposal's latency ends when its own entry applies;
- setup runs 10 proposals and one `ReadIndex` read before any timer;
- the election scenarios build TLS material, listeners and WAL files outside
  the timer and time only starting the nodes and waiting for a leader;
- 3- and 5-node clusters exchange messages over TCP with mutual TLS 1.3.

That is close to the smallest viable integration: no parallel workers, no
multi-shard management, no sessions. QuicRaft starts a full host (step,
commit and apply workers, shard registry, sessions), which is why its
single-node election is slower than the harness's even though the timeout is
the same.

etcd-io/raft v3.7.0 generates `raftpb` with `google.golang.org/protobuf`, so
`Ready` carries pointer types and each proposed entry is cloned. That costs
roughly 30 extra heap allocations per proposal compared with v3.6.0. It shows
in the Memory rows; the WAL and multi-node rows are dominated by fsync and TLS.

Read the [fairness rules](../README.md#fairness)
before drawing conclusions. Some rows, elections in particular, do not measure
the same thing in both columns.

## Results

Results are stored per machine under [`../profiles/`](../profiles/). The most
recent full comparison is
[`profiles/ci-node-2/etcd.md`](../profiles/ci-node-2/etcd.md), measured <!-- lab-string-ok: benchmark provenance: the profile directory names the machine that measured the numbers -->
2026-10-04. Numbers from different profiles are not comparable.

## Running

From the repository root:

```bash
make perf-run PERF_PROFILE=<name> PERF_SYSTEMS=quicraft,etcd
make perf-generate PERF_PROFILE=<name> PERF_SYSTEMS=quicraft,etcd
```

Or build and run the container directly; the build context must be the
repository root:

```bash
docker build -t quicraft-bench-etcd -f test/performance/etcd/Dockerfile .
docker run --rm quicraft-bench-etcd
```
