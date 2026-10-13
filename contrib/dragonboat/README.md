# contrib/dragonboat

> **Status: REFERENCE** · Scope: the Dragonboat-compatible shim over QuicRaft in
> this directory · Last verified: 2026-10-13

A [Dragonboat](https://github.com/lni/dragonboat) v4 compatible layer for
[QuicRaft](../../README.md). Its `NodeHost` exposes Dragonboat's method names
and types over a `quicraft.Host`, so code written against Dragonboat can run on
QuicRaft's engine and QUIC transport with few call-site changes. It does not
depend on Dragonboat or on [Pebble](https://github.com/cockroachdb/pebble).

## What it is, and is not

- A nested Go module, `github.com/jeremyhahn/go-quicraft/contrib/dragonboat`,
  whose `go.mod` replaces the root module with `../..`. No tag carries it,
  so it always builds against the QuicRaft tree it sits in. Applications
  written for QuicRaft use `pkg` directly.
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
  `ErrShardClosed`, `ErrShardNotInitialized`, `ErrClosed`, `ErrAborted` and
  `ErrNotLeader`.
- The `Get`-prefixed methods on `quicraft.Host` that this shim calls are a
  documented naming exception; see
  [docs/multi-group-host/api.md](../../docs/multi-group-host/api.md).

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
