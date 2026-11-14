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
- The `NodeHost` surface, method by method, is in
  [NodeHost methods](#nodehost-methods) below.
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

## NodeHost methods

Every exported method of this package's `NodeHost`, against the `NodeHost` of
Dragonboat v4 (v4.0.1). "Dragonboat" says whether Dragonboat has the method;
"Difference" is empty when the method behaves as Dragonboat's does. Every
method takes the same arguments as Dragonboat's; `Target` is an alias of
`string`. `readme_table_test.go` fails when an exported method is missing from
this table or the table names one the package does not export.

| Method | Dragonboat | Difference |
| --- | --- | --- |
| `Close` | yes | Logs a warning when the QuicRaft host's close returns an error. |
| `ExportSnapshot` | no | QuicRaft extension: streams the shard's latest snapshot to an `io.Writer`; the shard needs at least one snapshot. |
| `GetLeaderID` | yes | |
| `GetLogReader` | yes | |
| `GetNoOPSession` | yes | |
| `GetNodeHostInfo` | yes | |
| `GetNodeHostRegistry` | yes | Always `nil, false`: QuicRaft has no gossip registry. |
| `GetNodeUser` | yes | The shard must be running on this host. |
| `GetShardInfo` | no | QuicRaft extension: the shard's `ShardInfo`; a shard not loaded here returns false and `ErrShardNotFound`. |
| `GetShardMembership` | no | QuicRaft extension: the committed membership from the local replica, without the `ReadIndex` of `SyncGetShardMembership`. |
| `HasNodeInfo` | yes | |
| `ID` | yes | |
| `ImportSnapshot` | no | QuicRaft extension, in place of Dragonboat's `tools.ImportSnapshot`: imports a snapshot from an `io.Reader` into a stopped shard, which recovers from it at its next start. |
| `NAReadLocalNode` | yes | Also bounded by `QueryTimeout`. |
| `NodeHostConfig` | yes | |
| `NodeIdentity` | no | QuicRaft extension: the identity peers must commit for this host before they count its messages; empty without a transport. |
| `Propose` | yes | |
| `ProposeSession` | yes | Registration runs in its own goroutine, since QuicRaft registers synchronously; the result comes through the `RequestState`. |
| `QueryRaftLog` | yes | Runs synchronously; the returned `RequestState` is already resolved. |
| `QueryTimeout` | no | QuicRaft extension: the deadline `ReadLocalNode` and `NAReadLocalNode` put on `Lookup`; 0, no deadline, by default. |
| `RaftAddress` | yes | |
| `ReadIndex` | yes | |
| `ReadLocalNode` | yes | Before the `ReadIndex` completes it returns `ErrInvalidOperation`; after a failed one, that failure. Bounded by `QueryTimeout`. |
| `ReloadMutualTLS` | no | QuicRaft extension: re-reads the configured CA, certificate and key and uses them for new handshakes; the node identity must not change. |
| `RemoveData` | yes | Persists a removal marker, so the replica cannot restart on this host; a marker failure returns `RemovalMarkerError`. |
| `RequestAddNonVoting` | yes | Refused on a host with a transport, which needs `RequestAddNonVotingWithIdentity`. |
| `RequestAddNonVotingWithIdentity` | no | QuicRaft extension: `RequestAddNonVoting` with the identity the shard commits for the member. |
| `RequestAddReplica` | yes | A change invalid against the current membership fails at once, with a nil `RequestState`. Refused on a host with a transport (`quicraft.ErrMemberIdentityRequired`), which needs `RequestAddReplicaWithIdentity`. |
| `RequestAddReplicaWithIdentity` | no | QuicRaft extension: `RequestAddReplica` with the identity the shard commits for the member. |
| `RequestAddWitness` | yes | A change invalid against the current membership fails at once, with a nil `RequestState`. Refused on a host with a transport, which needs `RequestAddWitnessWithIdentity`. |
| `RequestAddWitnessWithIdentity` | no | QuicRaft extension: `RequestAddWitness` with the identity the shard commits for the member. |
| `RequestCompaction` | yes | Runs synchronously; the `SysOpState` is already complete. A shard that is not loaded is not compacted to `MaxUint64`: segment GC follows the existing compaction markers. |
| `RequestDeleteReplica` | yes | A change invalid against the current membership fails at once, with a nil `RequestState`. |
| `RequestLeaderTransfer` | yes | |
| `RequestSnapshot` | yes | |
| `SetQueryTimeout` | no | QuicRaft extension: sets `QueryTimeout`; zero or negative restores no deadline. |
| `StaleRead` | yes | |
| `StartConcurrentReplica` | yes | On a host with a transport, `config.Config.MemberIdentities` names the founding members' identities. |
| `StartOnDiskReplica` | yes | As `StartConcurrentReplica`. |
| `StartReplica` | yes | As `StartConcurrentReplica`. |
| `StopReplica` | yes | |
| `StopShard` | yes | |
| `SyncCloseSession` | yes | |
| `SyncGetSession` | yes | |
| `SyncGetShardMembership` | yes | |
| `SyncPropose` | yes | |
| `SyncRead` | yes | |
| `SyncRemoveData` | yes | As `RemoveData`. |
| `SyncRequestAddNonVoting` | yes | Refused on a host with a transport, which needs `SyncRequestAddNonVotingWithIdentity`. |
| `SyncRequestAddNonVotingWithIdentity` | no | QuicRaft extension: `SyncRequestAddNonVoting` with the member's identity. |
| `SyncRequestAddReplica` | yes | Refused on a host with a transport, which needs `SyncRequestAddReplicaWithIdentity`. |
| `SyncRequestAddReplicaWithIdentity` | no | QuicRaft extension: `SyncRequestAddReplica` with the member's identity. |
| `SyncRequestAddWitness` | yes | Refused on a host with a transport, which needs `SyncRequestAddWitnessWithIdentity`. |
| `SyncRequestAddWitnessWithIdentity` | no | QuicRaft extension: `SyncRequestAddWitness` with the member's identity. |
| `SyncRequestDeleteReplica` | yes | |
| `SyncRequestSnapshot` | yes | |

Dragonboat v4 `NodeHost` methods this package does not provide: none. The
package-level `WriteHealthMetrics` exists but writes nothing (see "Not
supported" above).

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
