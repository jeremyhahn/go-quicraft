# Multi-Group Host — Usage

> **Status: NORMATIVE** · Scope: go-quicraft `pkg` — deploying, configuring,
> monitoring and operating a `Host` · Last verified: 2026-10-13
> Conflicts with this document are defects. Fix them here first.

How to run a `Host`: start it, form shards, change membership, read and write,
watch it, back it up and shut it down. Topics that belong to another feature
are linked rather than repeated: certificates and revocation in
[quic-transport](../quic-transport/user.md), encryption keys in
[seal-barrier](../seal-barrier/user.md).

Formats are pinned until the first stable release and change in place, so two
builds can be unable to read each other's data. There is no rolling upgrade
across a format change: you wipe and re-sync. Read
[Compatibility and upgrades](../compatibility/user.md) before planning a deployment
lifecycle.

## Before you start

You need, for every node:

- a WAL directory and a host directory, ideally on separate disks so WAL writes
  do not compete with snapshot reads;
- a certificate and key from a CA shared by the cluster, with SANs that cover
  only that node's host. A wildcard or shared certificate is refused at
  `StartShard`. See
  [issuing certificates](../quic-transport/user.md);
- the node's identity, which every other node needs at bootstrap. It is the
  value `Host.NodeIdentity()` returns, a hash of the certificate's public key,
  so you can compute it as soon as the key exists;
- an at-rest encryption decision: a barrier (`WithBarrier` or
  `WithShardBarriers`), or an explicit `WithoutEncryption()`. A host that
  states neither refuses to start. `WithShardBarriers` combines with either
  host-wide choice: under `WithoutEncryption`, shards with their own barrier
  are still encrypted and the rest are stored in plaintext.

## Run a single node

For development and tests, skip the transport:

```go
cfg := config.HostConfig{
    WALDir:        "/var/lib/quicraft/wal",
    NodeHostDir:   "/var/lib/quicraft/nodehost",
    RaftAddress:   "127.0.0.1:63001",
    ListenAddress: "127.0.0.1:63001",
    DeploymentID:  1,
}

host, err := quicraft.NewHost(cfg, quicraft.WithoutTransport(), quicraft.WithoutEncryption())
if err != nil {
    return err
}
defer host.Close()

shardCfg := config.Config{ShardID: 1, ReplicaID: 1}
members := map[uint64]string{1: "127.0.0.1:63001"}
if err := host.StartShard(members, false, kv.NewMemoryCreateFunc(), shardCfg); err != nil {
    return err
}
```

Without a transport no identities are needed, and messages to other replicas
are dropped.

### One host per data directory

A host owns its `NodeHostDir` and `WALDir` exclusively while it runs. `NewHost`
takes a non-blocking exclusive lock on each (`NodeHostDir/host.lock`, and
`WALDir/LOCK` for the default WAL) and holds them until `Close`; a second host
over either directory, in the same process or another, fails at once with
`ErrDirLocked`, which names the lock file. The locks are released by `Close`,
by a `NewHost` that fails, and by the kernel when the process dies, so a crash
never leaves a stale lock to clear by hand. Do not delete the lock files.

The first start also records the `DeploymentID` and `RaftAddress` in
`NodeHostDir/host.identity`. A later start under a different `DeploymentID`
fails with `ErrHostIdentityMismatch`, naming both IDs, before the log is
opened: the directory holds another deployment's Raft state. Configure the
recorded ID, or give the host fresh, empty `NodeHostDir` and `WALDir` and let
it re-sync from its peers. A changed `RaftAddress` is logged and allowed, since
a member may restart at a new address.

## Form a multi-node shard

The library takes an explicit member map; it does not discover peers. If you
want discovery (static, multicast, DNS SRV or token), quorum checks and
bootstrap orchestration, use go-qrdb's cluster formation (`cluster.BootstrapShard`
or its `qrdbd` daemon), which ends by calling `StartShard` for you.

To do it yourself, run this on every founding node with the same `members`,
the same `MemberIdentities` and the same shard settings, changing only
`ReplicaID`:

```go
hostCfg := config.HostConfig{
    WALDir:        "/var/lib/quicraft/wal",
    NodeHostDir:   "/var/lib/quicraft/nodehost",
    RaftAddress:   "node1.example.com:63100",
    ListenAddress: "0.0.0.0:63100",
    DeploymentID:  42,
    TransportConfig: config.TransportConfig{
        MTLSConfig: &config.MTLSConfig{CACert: caPEM, Cert: certPEM, Key: keyPEM},
    },
}
host, err := quicraft.NewHost(hostCfg, quicraft.WithBarrier(barrier))
if err != nil {
    return err
}

members := map[uint64]string{
    1: "node1.example.com:63100",
    2: "node2.example.com:63100",
    3: "node3.example.com:63100",
}
shardCfg := config.Config{
    ShardID:   100,
    ReplicaID: 1, // 2 on node2, 3 on node3
    MemberIdentities: map[uint64]string{
        1: node1Identity, // each node's host.NodeIdentity()
        2: node2Identity,
        3: node3Identity,
    },
}
if err := host.StartShard(members, false, sm.NewCreateFunc(newStore), shardCfg); err != nil {
    return err
}
```

**Give every founder identical input.** The initial membership, identities,
witness policy and session limits are committed as the shard's first log entry,
written independently by each founder. Founders that disagree write different
first entries, and the shard can never reconcile them.

`DeploymentID` keeps clusters from talking to each other by accident. It is not a
security boundary; give each cluster its own CA for that.

On restart, call `StartShard` the same way. Membership comes from the shard's own
log, not from the map you pass.

## Change membership

Membership changes go through Raft and are refused on a non-leader
(`*NotLeaderError` names the leader). On a host with a transport, every add must
carry the new node's identity; the forms without one return
`ErrMemberIdentityRequired`.

Read the membership first and pass its `ConfigChangeID`, so a change made against
a configuration that has since moved is rejected instead of applied:

```go
m, err := host.SyncGetShardMembership(ctx, shardID)
if err != nil {
    return err
}
err = host.SyncRequestAddNodeWithIdentity(ctx, shardID, 4,
    "node4.example.com:63100", node4Identity, m.ConfigChangeID)
```

Pass 0 instead of `ConfigChangeID` to skip the check. One change is in flight
per shard at a time.

| Task | Call |
|---|---|
| add a voter | `SyncRequestAddNodeWithIdentity` |
| add an observer (replicates, does not vote) | `SyncRequestAddObserverWithIdentity` |
| add a witness (votes, stores no data) | `SyncRequestAddWitnessWithIdentity` |
| promote an observer to voter | `SyncRequestAddNodeWithIdentity` with the same replica ID, address and identity |
| remove a replica | `SyncRequestRemoveNode` (async form: `RequestRemoveNode`) |
| move leadership | `RequestLeaderTransfer(ctx, shardID, target)`; the target must be a voter |
| change witness policy | `SyncRequestUpdateWitnessPolicy` |

Each `Sync*` call has an async `Request*` form returning a `*RequestState`.

### Join a new replica

1. On the new node, `StartShard` with `join=true`. Its
   `Config.MemberIdentities` must name at least one existing replica (the
   anchor), which is who it will accept the shard's state from. Naming the
   current leader, or every member, is safest.
2. On the leader, add it with the matching `...WithIdentity` call.
3. `host.IsShardJoining(shardID)` on the new node turns false once it has
   received its first snapshot.

Start witnesses and observers this way too. Never list them in the `members` map
of a bootstrap: that map is the voting set, and `StartShard` refuses it with
`*NonVotingRoleInMembersError`. An observer you later promote becomes a voter in
place; nothing needs restarting, and restarting it with `IsObserver` still set is
harmless.

Give observers and witnesses a stable `RaftAddress`. A voter re-advertises a new
address by campaigning; a non-voter cannot, and needs a config change to be found
again.

### Remove a replica for good

1. Remove it from the shard and wait for the change to commit.
2. Stop it on its host and call `host.RemoveData(shardID, replicaID)`.

`RemoveData` retires the replica ID on that host: `StartShard` refuses it
(`*ReplicaRetiredError`) from then on. The removal also tombstones the node's
identity in the shard, so the same keypair is refused under any replica ID
(`ErrRemovedMemberIdentity`). To bring the machine back, give it a new replica ID
and a new key.

## Read and write

| You need | Use |
|---|---|
| a write | `SyncPropose`, or `Propose` and wait on the `RequestState` |
| a write applied at most once across retries | a session: `NewSession`, then `SyncProposeWithSession` |
| a read that sees every committed write | `SyncRead` (or `SyncReadBuf` for `[]byte` keys) |
| a cheaper read, current with what this replica knows is committed | `FollowerRead` |
| the fastest read, possibly stale | `StaleRead` |

```go
if _, err := host.SyncPropose(ctx, shardID, cmd); err != nil {
    return err
}
value, err := host.SyncRead(ctx, shardID, query)
```

**Retrying safely.** When a call fails, check `quicraft.MaybeApplied(err)` first:
if true, the write may still land, and retrying without a session can apply it
twice. Retry only when `quicraft.Temporary(err)` is true, and use
`quicraft.NotLeader(err)` to find where to send it. See
[api.md](api.md#classification-helpers).

**Sessions.** A session gives at-most-once delivery. It serves one proposal at a
time (`ErrSessionBusy` otherwise), and after each success you call
`session.ProposalCompleted()` (`SyncProposeWithSession` does it for you). Close
sessions you are done with, or they expire after `SessionExpiryEntries` applied
entries. To cap how many sessions one tenant can hold, open them with
`NewSessionWithOwner` and an owner ID you have authenticated yourself.
Shards whose state machine is an on-disk `sm.DiskStateMachine` support only
no-op sessions (`NoOPSession`), so make those operations idempotent.

`LeaseRead: true` lets the leader answer linearizable reads without a heartbeat
round while its lease holds. It needs `CheckQuorum` (on by default) and clocks
whose rates differ by under 1%. Configure `ElectionRTT`, `RTTMillisecond` and
`LeaseRead` alike on every replica for lease reads to be fast: a follower that
promises a shorter vote window backs no lease, which is still safe, and reads
take the heartbeat round while too few followers back one. After a restart, a
replica of a lease-read shard does not vote or campaign for one election
timeout, so restarting the whole shard delays its first election by about that
long.

## Configure

Full field tables with defaults are in [api.md](api.md#configuration). The
decisions most deployments need to make:

| Setting | Default | Decide |
|---|---|---|
| `RTTMillisecond` | 200 | your network's round trip: 1 to 5 on a LAN |
| `ElectionRTT` / `HeartbeatRTT` | 10 / 1 | election timeout and heartbeat in RTT multiples; minimum `ElectionRTT` is 5 |
| `MaxTotalInMemLogSize` | 0 (off) | set it, or one client can grow memory until the process dies |
| `MaxInMemLogSize` | 0 (off) | per-shard backpressure |
| `MaxWALDiskSize` | 0 (off) | set it, or the WAL can fill the volume |
| `KeyRotationInterval` | 0 | 24h is a reasonable production value with a barrier |
| `SnapshotEntries` | 100000 | entries between snapshots. Zero means the default; `config.SnapshotsDisabled` means manual only |
| `SessionExpiryEntries` | 100000 | idle-session expiry, in applied entries. `config.SessionExpiryDisabled` turns it off |
| `MaxTotalSessions` / `MaxSessionsPerOwner` | 16384 / 128 | session caps. Fixed when the shard is created |
| `NoSyncWAL` | false | never in production: it gives up Raft's durability guarantee |

Witness policy and the four session limits are copied into the shard when it is
created and then live in the shard itself. Changing them in a node's `Config`
afterwards has no effect (a differing session limit is logged as a warning when
the node starts); read what a shard is actually using with
`ShardMembership`. Witness policy can be changed with
`SyncRequestUpdateWitnessPolicy`; the session limits cannot be changed after
creation, so size `MaxTotalSessions` up front.

`MaxResponsesPerSession` limits cached responses per session, not the number of
sessions.

### Tuning

| Environment | `RTTMillisecond` | `ElectionRTT` | Heartbeat | Election timeout |
|---|---|---|---|---|
| LAN | 1 to 5 | 10 | 1 to 5 ms | 10 to 50 ms |
| WAN, one region | 50 to 100 | 10 | 50 to 100 ms | 0.5 to 1 s |
| WAN, cross-region | 200 | 10 | 200 ms | 2 s |

- Raise `ElectionRTT` if GC pauses or disk stalls cause spurious elections.
- Raise `CommitCBufferSize` for write-heavy loads where commit lags the step
  tier.
- `NumWorkers` (default: CPU count) sets workers per pipeline tier;
  `MaxApplyWorkers` caps concurrent state machine applies.
- `Quiesce: true` stops idle shards ticking until traffic returns, which saves
  CPU on hosts with many idle shards.
- `EntryCompressionType` / `SnapshotCompressionType` = `config.Snappy` trade a
  little CPU for bandwidth and disk.
- `ZeroCopyEntryCmd: true` saves a copy per entry, but your state machine must
  copy `Cmd` to keep it after `Update` returns.
- `CompactionOverhead` (5000) is how many entries stay after compaction for slow
  followers; raise it if followers often need full snapshots.
- Transport limits (`MaxSnapshotReceiveRate`, `MaxConcurrentSnapshotReceives`,
  connection limits) are covered in
  [quic-transport](../quic-transport/user.md).

## Encrypt at rest

Build a barrier, initialize it on first boot (or load its sealed root key and
unseal it on later boots), and pass it with `WithBarrier`. The setup, how to
persist the sealed key, and key custody options are in
[seal-barrier](../seal-barrier/user.md).

```go
host, err := quicraft.NewHost(cfg, quicraft.WithBarrier(barrier))
```

- **Rotation.** `KeyRotationInterval` rotates on a schedule; the barrier also
  rotates on its own when a key nears its AEAD usage budget.
- **Purging old keys.** Purging the key that decrypts a snapshot still on disk
  makes that snapshot unreadable for good, and nothing notices until a recovery
  tries. The purge floor is computed from the snapshot directories, which is one
  reason an import refuses to guess where a snapshot belongs.
- **Emergency seal.** `host.EmergencySeal()` seals the barrier at once, without
  draining: every operation that needs a key then fails with
  `ErrBarrierSealed`, including an in-progress snapshot save. The `Host` has no
  unseal call. Recovery goes through the barrier object you passed in, using its
  strategy and credentials, so keep those reachable by your incident tooling, or
  the seal lasts until a restart.
- **While a barrier is sealed.** A shard whose barrier is sealed (its own
  barrier from `WithShardBarriers`, else the host-wide one) refuses client work
  up front with `ErrBarrierSealed`: `Propose`, `SyncPropose`,
  `ProposeWithSession`, session create, register and close, membership changes,
  `ReadIndex`, `SyncRead`, `StaleRead`, `StaleReadBuf`, `FollowerRead`,
  `QueryLocalNode`, `RequestSnapshot` and `ExecuteOnLeader`. Local reads are
  refused too, because sealing withdraws access to the shard's data. The refusal
  is returned, not logged, so it does not flood the log. Shards under other,
  unsealed barriers keep serving. Once the barrier is unsealed the same calls
  work again.
- **The failure latch while sealed.** The refusal covers client calls only.
  Raft keeps running: a follower still persists the entries its leader sends,
  and a leader still persists proposals other replicas forward to it. If such a
  write lands while the barrier is sealed, its encryption fails and the shard's
  failure latch trips: the shard halts with `ErrShardFailed`, and
  `OnShardFailed` fires. **Unsealing does not bring a halted shard back.** Stop
  and restart the shard (`StopShard`, then `StartShard`) after unsealing. A seal
  on a shard that receives no replicated writes, for example an idle cluster,
  does not trip the latch.
- **Per-tenant keys.** `WithShardBarriers` gives each shard its own barrier. The
  default WAL is still encrypted under the host-wide barrier, so for per-tenant
  log keys you need a LogDB that supports them, such as a multiplexing LogDB
  that reports `logdb.AtRestHostRouted`. That works with `WithBarrier` (even
  a plaintext-mode barrier, one initialized with the `None` strategy) and with `WithoutEncryption`; either way each
  tenant shard is checked against the LogDB at `StartShard`.

## Monitor

### Metrics

```go
collector := quicraft.NewPrometheusMetricsCollector()
host, err := quicraft.NewHost(cfg, quicraft.WithMetrics(collector), quicraft.WithoutEncryption())
if err != nil {
    return err
}
http.Handle("/metrics", promhttp.HandlerFor(collector.Registry, promhttp.HandlerOpts{}))
```

Metrics are on a dedicated registry under the `quicraft` namespace. The full
list is in [api.md](api.md#metrics).

#### Metrics cardinality

By default most metrics aggregate to host level. A histogram costs one series
per bucket plus two, so per-shard labels multiply quickly:

| Mode | Series per active shard | 10,000 shards |
|---|---:|---:|
| default (`PerShardMetrics: false`) | 6 | about 60,000 |
| `PerShardMetrics: true` | 113 | about 1,130,000 |

A million series from one host can destabilize a shared Prometheus for every
tenant on it. Turn per-shard labels on for small deployments or while chasing one
shard's latency:

```go
collector := quicraft.NewPrometheusMetricsCollectorWithConfig(config.MetricsConfig{
    PerShardMetrics: true,
})
```

The mode is fixed when the collector is built. Turning it off never costs an
alert, because these keep `shard_id` in both modes:

- `quicraft_raft_elections_total`
- `quicraft_raft_proposals_dropped_total`
- `quicraft_transport_messages_dropped_total`
- `quicraft_transport_snapshots_rejected_total`
- `quicraft_security_unauthorized_messages_dropped_total`
- `quicraft_security_unauthorized_snapshots_dropped_total`

Aggregated series keep their names and drop only `shard_id`, so a rule written
as `sum without(shard_id) (...)` works in both modes. The gauge
`quicraft_metrics_per_shard_enabled` (1 or 0) tells a dashboard which mode it is
reading.

`EnablePerMessageMetrics` adds per-message counters labelled by shard, peer and
message type. It is the most expensive option; use it only briefly.

What to alert on:

- any `quicraft_security_*` increase, especially `reason="identity_mismatch"`,
  which means a peer presented the wrong key for the replica it claimed;
- `unknown_member` that does not clear after a membership change: the replica
  missed the change that added the sender. Restart it with
  `Config.MemberIdentities` naming the current leader;
- `quicraft_transport_messages_dropped_total` with `reason` `unsendable` or
  `peer_rejected`, which retries cannot fix (`peer_rejected` usually means
  version skew);
- a rising `quicraft_raft_elections_total`.

### Health

```go
status := host.HealthCheck()
// status.Status: "sealed", "unhealthy" (no shards), "degraded" (a shard in
// error), or "ok"; map anything but "ok" to HTTP 503.
```

Only a shard in `error` degrades the host. Shards in `no-leader`, `lagging`,
`recovering` or `snapshotting` leave it `"ok"`, so alert on `status.Shards`
yourself if you care about them. `ShardInfo` gives one shard's state,
including its replicas' progress when asked on the leader.

### Events

Set callbacks on `HostConfig.EventListener`; leave the ones you do not need nil:

```go
cfg.EventListener = &config.EventListener{
    OnLeaderUpdated: func(info config.LeaderInfo) { /* ... */ },
    OnShardFailed: func(info config.ShardFailedInfo) {
        logger.Error("shard failed", "shard", info.ShardID, "err", info.Err)
    },
    OnLogDBDiskFull: func(info config.DiskInfo) { /* ... */ },
}
```

Leader, read-readiness, campaign, shard-failure, compaction, snapshot-created
and snapshot-rejected events arrive in order on one goroutine, and a handler may
call back into the host, even `StopShard`. Every callback must return quickly. The full list is in
[api.md](api.md#event-listener).

`OnShardFailed` fires once each time a shard's failure latch trips, and the
shard stays offline until it is reloaded. A failed snapshot save does not fire
it: the shard keeps running, the host logs the failure, and
`RequestSnapshot` returns it. No event reports a refused election: to detect a
leaderless shard, watch for `ShardNoLeader` health with no election activity.

A freshly elected leader cannot serve linearizable reads until it has committed
an entry of its own term, and `ReadIndex` returns `ErrReadIndexNotReady` until
then. Wait for `OnLeaderReady` instead of retrying: it fires on every replica
except a witness (the leader, followers and observers), once that commit is
known there, and from
then on reads issued on that replica are not refused for that reason. The next
`OnLeaderUpdated` ends it — including the same leader re-elected at a higher
term, which `OnLeaderLost` does not report. `ShardReadReady` gives the same
answer synchronously. The full contract is in
[api.md](api.md#event-listener).

## Run a request on the leader

Some state belongs to whichever replica leads a shard and needs no
replication, such as lease deadlines. `ExecuteOnLeader` sends a request to the
shard's leader for the current term, runs it there, and returns the reply. It
does not go through the Raft log. Register the handler on every replica that
can lead the shard, before or after `StartShard`:

```go
err := host.RegisterLeaderHandler(shardID,
    func(ctx context.Context, term uint64, req []byte) ([]byte, error) {
        return leases.Renew(term, req) // runs only on the leader of term
    })

reply, err := host.ExecuteOnLeader(ctx, shardID, []byte("renew client-7"))
```

You can call it on any replica. A follower forwards the request over the QUIC
transport, with the same mTLS and sender binding as a forwarded proposal. If
leadership moves while the request is in flight, the answer of the deposed
leader, reply or error, is discarded and the request runs again on the new leader under the new
term. It never runs twice under one term, but it can run once in each of two
terms, so make the handler idempotent across a leader change. If no leader
settles before `ctx` ends, the error matches `ErrNoLeader` or
`ErrLeadershipChanged`, both temporary. The guarantees and the full error list
are in [api.md](api.md#leader-executed-requests).

## Manage disk

Set `MaxWALDiskSize`. At 85% of it the host fires `OnLogDBDiskWarning`; at 95% it
fires `OnLogDBDiskFull`, refuses proposals with `ErrDiskFull` and reports
`DiskOK: false`. Callbacks fire on crossing a threshold, not on every check.
Proposals are accepted again once usage falls back below the full threshold, for
example after compaction or cleanup.

Snapshots drive log compaction. Every `SnapshotEntries` applied entries a shard
takes a snapshot and drops log entries older than the snapshot minus
`CompactionOverhead`; fully unused WAL segments are then deleted. You can also
snapshot on demand:

```go
index, err := host.SyncRequestSnapshot(ctx, shardID, quicraft.SnapshotOption{})
```

One snapshot per shard runs at a time (`ErrSnapshotInProgress` otherwise). A
snapshot larger than `MaxSnapshotSize` (4 GB) is discarded with an error.

Never delete WAL files by hand. The WAL directory carries a format marker that is
checked before any segment is read, and a WAL with segments but no marker is
refused.

## Back up and restore

`ExportSnapshot` writes a self-checking artifact of a shard's latest snapshot,
decrypted, so it can be restored under different keys. If export returns an
error, delete what was written: it is incomplete and import will refuse it.

```go
f, err := os.Create("/var/backups/shard-100.snap")
if err != nil {
    return err
}
if err := host.ExportSnapshot(ctx, 100, f); err != nil {
    f.Close()
    os.Remove("/var/backups/shard-100.snap")
    return err
}
if err := f.Close(); err != nil {
    return err
}
```

To restore onto a host:

1. Stop the shard on that host.
2. Import the artifact. On a host where the shard has run before, the layout is
   known:

   ```go
   err := host.ImportSnapshot(ctx, 100, artifact)
   ```

   On a fresh host the shard has never run, so say where its data lives. Use the
   shard's `Config.DataDir`, or `""` if it has none:

   ```go
   err := host.ImportSnapshot(ctx, 100, artifact,
       quicraft.WithImportDataDir("/var/lib/quicraft/data/shard-100"))
   ```

   Without it the import fails with `ErrImportLayoutUnknown`; a value that
   contradicts the shard's record fails with `ErrImportDataDirMismatch`. Both
   fail before anything is written. Keep each shard's `DataDir` with your
   restore procedure.
3. `StartShard` with the same `Config.DataDir`.
4. Confirm: `ShardInfo` shows `LastApplied` reaching the snapshot index and
   `Health` leaving `recovering`.

An import refuses an artifact that is not newer than the shard's current
snapshot, a destination directory that already exists, and an artifact with a
damaged or missing trailer. Artifacts exported by older builds have no trailer;
export them again.

If a shard has permanently lost a majority of its voters, follow the
[quorum-loss runbook](engineering.md#runbook-recovering-a-shard-that-has-lost-quorum).
It rewrites membership on import and can lose acknowledged writes, so read it in
full first.

## Shut down

```go
if err := host.Close(); err != nil {
    logger.Error("shutdown incomplete", "error", err)
}
```

`Close` drains first: new work is refused with `ErrDraining` while in-flight
work finishes, for up to `ShutdownTimeout` (30 s). It then stops everything,
flushes the log and seals the barrier. A non-nil error means something did not
finish cleanly, possibly lost writes for an on-disk state machine; do not ignore
it. Calling `Drain()` yourself first is optional, and draining cannot be undone.

`Close` also waits for any state machine `Update` that is running, with no time
limit, so a state machine that blocks in `Update` blocks shutdown.

## Troubleshooting

| Symptom | Cause and fix |
|---|---|
| `NewHost` returns `ErrAtRestEncryptionUnspecified` | pass `WithBarrier`, `WithShardBarriers` or `WithoutEncryption()` |
| `NewHost` returns `ErrBarrierPlaintextLogDB` or `ErrShardBarriersPlaintextWAL` | your custom LogDB would store the log unencrypted; use the default WAL or an encrypting LogDB. A `host_routed` LogDB needs `WithoutEncryption`, or `WithBarrier` with the barrier its host-wide route writes through |
| `StartShard` returns `ErrShardBarrierLogDBMismatch` | the LogDB holds this shard under a different barrier than your resolver returned; return the same pointer the LogDB was given |
| `StartShard` returns `ErrBarrierSealed` | the shard's resolved barrier is sealed; unseal it before starting the shard |
| `Propose`, a read or another client call returns `ErrBarrierSealed` | the shard's barrier is sealed; unseal it and retry |
| `ErrShardFailed` after an unseal, log shows `WAL record encryption failed: ... barrier is sealed` | a replicated write reached the log while sealed and tripped the failure latch; restart the shard |
| `NewHost` fails validation on `MTLSConfig` | the transport needs a CA, certificate and key |
| `StartShard` or a membership add returns `ErrSharedPeerCertificate` | a node's certificate also covers another member's host; issue one certificate per node |
| `StartShard` returns `*InvalidMemberError` about identities | `MemberIdentities` is missing a member, binds one identity twice, names no other replica on a join, or contradicts the shard's recorded identities |
| a membership add returns `ErrMemberIdentityRequired` | use the `...WithIdentity` form |
| an add returns `ErrRemovedMemberIdentity` | that key was removed from the shard; issue a new key |
| an add returns `ErrDuplicateMemberAddress` or `ErrDuplicateMemberIdentity` | another active member already holds that address or key; remove it first, or use a different one |
| `StartShard` returns `*ReplicaRetiredError` | `RemoveData` retired the ID; use a new replica ID |
| `StartShard` returns `ErrImportInProgress` or `ErrShardNotReady` | an import or another start of the shard is running; retry |
| `NewHost` fails opening the WAL with `ErrUnknownPayloadType` | usually an encrypted WAL opened without its barrier; otherwise data from an incompatible build, which must be wiped and re-synced |
| new replica never catches up, `unknown_member` drops on it | its `MemberIdentities` anchor does not name the replica sending to it; name the leader |
| `ErrSystemBusy` or `*BusyError` from `Propose` | memory limits reached; back off and retry |
| `ErrDropped` from `Propose` | the shard's proposal queue is full; back off and retry |
| `ErrDiskFull` | WAL usage reached 95% of `MaxWALDiskSize`; free space or raise the limit |
| `ErrShardFailed` | the state machine returned an error or panicked; the shard is offline until restarted or recovered from a snapshot |
| `Close` does not return | a state machine `Update` is blocked |
| `ShardHealth` JSON fails to decode | an unknown value is rejected, not read as `healthy` |

## FIPS builds

Build with `GOFIPS140` for FIPS 140-3 mode, and add `-tags fips` to compile the
non-approved algorithms out entirely. The boundary covers data at rest and the
QUIC transport; OCSP revocation checking is the one exempt path, and revocation
`Mode: "crl"` avoids it. `make verify-fips` is the check that can actually fail.
Details: [seal-barrier](../seal-barrier/user.md) and
[quic-transport](../quic-transport/user.md).

## Cross-references

- [API reference](api.md): every type, method, option and error
- [Lifecycle](engineering.md): startup, recovery, shutdown, quorum-loss runbook
- [Architecture](architecture.md): how a host, shards and the transport fit
- [Compatibility and upgrades](../compatibility/architecture.md)
- [QUIC transport usage](../quic-transport/user.md): certificates, identities,
  revocation, limits
- [Seal and barrier usage](../seal-barrier/user.md): keys, rotation, purging
- [Client sessions](../client-sessions/user.md)
- [Snapshots](../snapshots/user.md)
