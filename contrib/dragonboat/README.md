# contrib/dragonboat

> **Status: REFERENCE** · Scope: the Dragonboat-compatible shim over QuicRaft in
> this directory · Last verified: 2026-10-20

A [Dragonboat](https://github.com/lni/dragonboat) v4 compatible layer for
[QuicRaft](../../README.md). Its `NodeHost` exposes Dragonboat's method names
and types over a `quicraft.Host`, so code written against Dragonboat can run on
QuicRaft's engine and QUIC transport with few call-site changes. It does not
depend on Dragonboat or on [Pebble](https://github.com/cockroachdb/pebble).

## What it is, and is not

- A nested Go module, `github.com/jeremyhahn/go-quicraft/contrib/dragonboat`.
  Every release tags it `contrib/dragonboat/vX.Y.Z` beside the root `vX.Y.Z`,
  and the release commit pins its `require` of the root module to that same
  version, so `go get github.com/jeremyhahn/go-quicraft/contrib/dragonboat@vX.Y.Z`
  works. Its `go.mod` also replaces the root module with `../..`; a consumer
  ignores that, and in-tree builds use it to build against the QuicRaft tree
  the module sits in. Applications written for QuicRaft use `pkg` directly.
- Packages: `config` (`NodeHostConfig`, `Config`), `statemachine` (the regular,
  concurrent and on-disk state machine interfaces and their adapters),
  `client` (`Session`), and `raftio` (event listener and LogDB interfaces and
  event types).
- Construction: `NewNodeHost` takes a `config.NodeHostConfig` and, like
  Dragonboat, stores the Raft log and snapshots as plaintext
  (`quicraft.WithoutEncryption`). `NewNodeHostWithOptions` also takes
  `quicraft.HostOption` values; they must state the at-rest encryption
  (`quicraft.WithBarrier` or `quicraft.WithoutEncryption`), and the
  constructor fails with a `NodeHostConstructionError` if they state neither.
- The `NodeHost` surface:
  - lifecycle: `StartReplica`, `StartConcurrentReplica`, `StartOnDiskReplica`,
    `StopReplica`, `StopShard`, `Close`;
  - proposals and reads: `SyncPropose`, `Propose`, `ProposeSession`,
    `SyncRead`, `ReadIndex`, `ReadLocalNode`, `NAReadLocalNode`, `StaleRead`;
  - sessions: `GetNoOPSession`, `SyncGetSession`, `SyncCloseSession`;
  - membership: `SyncGetShardMembership`, `GetShardMembership`, `GetLeaderID`,
    and `RequestAddReplica`, `RequestAddNonVoting`, `RequestAddWitness`, each
    with a `Sync…` form and a `…WithIdentity` form, plus
    `RequestDeleteReplica` / `SyncRequestDeleteReplica` and
    `RequestLeaderTransfer`;
  - snapshots and storage: `RequestSnapshot`, `SyncRequestSnapshot`,
    `ExportSnapshot`, `ImportSnapshot`, `RequestCompaction`, `RemoveData`,
    `SyncRemoveData`, `QueryRaftLog`, `GetLogReader`;
  - introspection: `ID`, `RaftAddress`, `GetNodeHostInfo`, `GetShardInfo`,
    `HasNodeInfo`, `GetNodeHostRegistry`, `GetNodeUser`, `NodeHostConfig`,
    `NodeIdentity`;
  - QuicRaft extensions: `SetQueryTimeout` and `QueryTimeout` bound the
    state machine `Lookup` behind `ReadLocalNode`. The default is 0, no
    deadline, as in Dragonboat.
- `NAReadLocalNode` behaves as in Dragonboat v4: after a completed
  `ReadIndex` it passes the byte-slice query to the state machine's
  `statemachine.IExtended.NALookup` and returns `statemachine.ErrNotImplemented`
  when the state machine does not implement `IExtended`. It runs under the same
  read guard and `QueryTimeout` as `ReadLocalNode`.
- Not supported:
  - `WriteHealthMetrics` writes nothing. Dragonboat writes its own
    process-wide metrics; QuicRaft's metrics are per `quicraft.Host`, through
    a `quicraft.MetricsCollector` (for example
    `quicraft.NewPrometheusMetricsCollector`) passed to
    `NewNodeHostWithOptions` with `quicraft.WithMetrics`, and are exposed by
    that collector. `config.NodeHostConfig.EnableMetrics` is accepted and
    ignored.
  - External snapshot files. A state machine that adds a file to the
    `ISnapshotFileCollection` during `SaveSnapshot` fails the snapshot with
    `statemachine.ErrSnapshotFilesNotSupported` rather than producing one
    without the file; keep all state in the snapshot stream.
- QuicRaft authorizes members by node identity. On a host with a transport,
  name the founding members' identities in `config.Config.MemberIdentities`
  (no Dragonboat counterpart) when the shard starts, and add later members
  with the `…WithIdentity` methods. The identity is the value the member's
  `NodeHost.NodeIdentity()` reports, which is its `quicraft.Host.NodeIdentity()`;
  it is empty on a host started with `quicraft.WithoutTransport`.
- Errors are translated to Dragonboat's sentinels. A QuicRaft
  `*MaybeAppliedError` survives the translation, so `quicraft.MaybeApplied`
  still distinguishes "refused before anything happened" from "accepted, then
  leadership was lost, so it may still commit". Errors with no Dragonboat
  equivalent are wrapped in `AdapterError`. `IsTempError` reports the
  retryable set: `ErrSystemBusy`, `ErrShardNotReady`, `ErrTimeout`,
  `ErrShardClosed`, `ErrShardNotInitialized`, `ErrClosed`, `ErrAborted`,
  `ErrNotLeader` and `quicraft.ErrShardPaused` (a paused shard, which reaches
  the caller wrapped in `AdapterError`).
- The shim's `Get`-prefixed methods (`GetLeaderID`, `GetShardMembership`,
  `GetNoOPSession`, …) keep Dragonboat's names; the `quicraft.Host` methods
  behind them carry no prefix (`LeaderID`, `ShardMembership`, `NoOPSession`).
  See [docs/multi-group-host/api.md](../../docs/multi-group-host/api.md).

The performance comparison in `test/performance/dragonboat` does not use this
shim. It benchmarks a Dragonboat fork directly; see
[test/performance/dragonboat](../../test/performance/dragonboat/README.md).

## Testing

```sh
cd contrib/dragonboat
make test               # unit tests, race detector on
make ci-integration     # multi-node integration suite (-tags=integration)
```

CI runs both suites in a dedicated job, and the root `make ci-modules` builds
and vets every nested module so a root change that breaks the shim fails there. The
integration helpers resolve the current leader before config changes and
leader-bound reads, so a retry against a stale leader cannot hide the
forwarding the suite is meant to exercise.
