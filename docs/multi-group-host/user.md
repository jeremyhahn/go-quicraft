# Multi-Group Host — Usage

> **Status: NORMATIVE** · Scope: go-quicraft `pkg` — deploying, configuring,
> monitoring and tuning a `Host` · Last verified: 2026-08-19
> Conflicts with this document are defects. Fix them here first.

Building and running a `Host`. Feature-specific tasks live in the other
folders' `user.md` files, which this document links to rather than repeating.


Deployment, monitoring, configuration tuning, and day-to-day administration of
QuicRaft clusters.

**Sources:** `pkg/host.go`, `pkg/config/`, `pkg/metrics.go`, `pkg/metrics_prometheus.go`, `pkg/logdb/waldb/diskmon.go`, `pkg/seal/barrier.go`, `pkg/internal/server/rate.go`, `Makefile`

> **This is alpha software at `0.5.1-alpha`.** Wire and on-disk formats are
> pinned at version 1 and change **in place**, so two builds from different
> commits can be mutually unreadable while both report version 1. There is no
> rolling upgrade across a format change and no migration tooling; every minor
> release so far has required wiping data directories. Before you plan a
> deployment lifecycle around this, read
> [Compatibility and Upgrades](../compatibility.md) — particularly
> [what 0.5.x does when it meets 0.4.x data](../compatibility.md#what-05x-does-when-it-meets-04x-data),
> which is not a clean refusal.

## Deployment

### Single-Node

A single-node cluster uses `WithoutTransport()` to skip QUIC setup. Useful for development, embedded use cases, and CI.

```go
cfg := config.HostConfig{
    WALDir:        "/var/lib/quicraft/wal",
    NodeHostDir:   "/var/lib/quicraft/nodehost",
    RaftAddress:   "127.0.0.1:63001",
    ListenAddress: "127.0.0.1:63001",
    DeploymentID:  1,
}

host, err := quicraft.NewHost(cfg, quicraft.WithoutTransport())
// ...
shardCfg := config.Config{ShardID: 1, ReplicaID: 1}
members := map[uint64]string{1: "127.0.0.1:63001"}
host.StartShard(members, false, kv.NewMemoryCreateFunc(), shardCfg)
```

### Multi-Node Cluster Formation

go-quicraft itself only provides the primitive `Host.StartShard(members, join, createFn, cfg)`, which takes an explicit `map[uint64]string` member map. The caller is responsible for producing that map.

Peer discovery (static / multicast / DNS SRV / token), quorum validation, and bootstrap orchestration live in **go-qrdb** (`pkg/discovery` + `pkg/cluster`). To form a multi-node cluster, use go-qrdb's `cluster.BootstrapShard` (or run the `qrdbd` daemon, which wraps it). See go-qrdb's `docs/discovery.md` and `docs/12-configuration.md`.

### Programmatic Shard Start (primitive)

When the member map is already known, call the quicraft primitive directly:

```go
members := map[uint64]string{
    1: "10.0.0.1:63001",
    2: "10.0.0.2:63001",
    3: "10.0.0.3:63001",
}
err := host.StartShard(members, false /* join */, sm.NewCreateFunc(newKV), shardCfg)
```

For discovery-driven formation (multicast/DNS/token/static-with-quorum), use go-qrdb's `cluster.BootstrapShard`, which discovers peers, adds self, validates quorum, builds the member map, and calls `Host.StartShard`.

### Directory Layout

```
/var/lib/quicraft/
  wal/                       # WAL segments (write-intensive, use fast disk)
    WALFORMAT                # 16-byte geometry+framing marker, read before any segment
    shard-0/                 # 16 WAL shards, shard index = shardID hash
    shard-1/
    ...
    shard-15/
  nodehost/                  # Metadata, snapshots, host ID
    host.id                  # 128-bit hex host identifier (0600 perms, enforced on read)
    snapshots/
      shard-100/
        replica-3/
          snapshot-00000000000000001000/   # index, zero-padded to 20 digits
            snapshot.dat
            snapshot.meta
```

Directories are created with mode 0750. The `WALFORMAT` marker is written before
the first segment exists and is checked before any segment byte is read; a
populated WAL directory with no marker is refused rather than replayed. Never
delete segments while leaving the marker, or the marker while leaving segments.

A shard with a non-empty `Config.DataDir` puts its snapshots under that path
instead of under `NodeHostDir`. `ImportSnapshot` needs to know which, so keep the
`DataDir` value with your restore procedure — see [Disaster
recovery](#disaster-recovery-importing-a-snapshot).

**Production recommendation:** Place `WALDir` and `NodeHostDir` on separate physical disks to isolate WAL write I/O from snapshot reads.

### Disaster Recovery: importing a snapshot

`Host.ImportSnapshot` restores a shard from a snapshot artifact produced by
`ExportSnapshot` on another cluster. The import must land inside the tree that
snapshot GC and the DEK purge floor scan, or the imported snapshot is invisible
to both: garbage collection never reclaims it, and — the dangerous half — the
next `PurgeEpochsBefore` legitimately drops the key that decrypts it, leaving
the restored state permanently unreadable.

The layout depends on the shard's `Config.DataDir`, which is per-shard and is
not part of the artifact. `ImportSnapshot` therefore discovers it from the
shard's bootstrap record when one exists:

```go
// Shard has run on this host before: layout is discovered, no option needed.
err := h.ImportSnapshot(ctx, shardID, artifact)
```

On a **fresh disaster-recovery host** the shard has never started, so there is
no bootstrap record and nothing on disk states the layout. The import cannot
guess it and refuses rather than writing to the wrong tree:

```go
// Fresh DR host: state the layout explicitly. It is cross-checked against the
// bootstrap record if one later exists, and a contradiction is refused.
err := h.ImportSnapshot(ctx, shardID, artifact,
    quicraft.WithImportDataDir("/var/lib/quicraft/data/shard-100"))

// The shard has no DataDir (default NodeHostDir layout) — say so explicitly:
err := h.ImportSnapshot(ctx, shardID, artifact, quicraft.WithImportDataDir(""))
```

Omitting the option on a shard with no bootstrap record returns
`ErrImportLayoutUnknown`. Supplying one that contradicts a recorded value
returns `ErrImportDataDirMismatch`. Both fail before anything is written.

**Order of operations for a full-shard restore:**

1. Ensure the shard is **stopped** on this host (`ImportSnapshot` requires it).
2. Import the artifact, stating `WithImportDataDir` if the shard has never run here.
3. `StartShard` with the same `Config.DataDir` used in step 2.
4. Confirm recovery: `ShardInfo.LastApplied` should reach the imported snapshot's
   index, and `Health` should leave `recovering`.

## Configuration Reference

### HostConfig (per-host)

Source: `pkg/config/hostconfig.go`

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `WALDir` | string | (required) | Write-ahead log directory |
| `NodeHostDir` | string | (required) | Metadata, snapshots, host ID directory |
| `ListenAddress` | string | (required) | Local QUIC bind address |
| `RaftAddress` | string | (required) | Address seen by peers (may differ from `ListenAddress` behind NAT). `Validate` rejects an empty value |
| `RTTMillisecond` | uint64 | `200` | Estimated round-trip time (ms). LAN: 1-5, WAN: 100-300. Capped at 60000 |
| `DeploymentID` | uint64 | (required, must be > 0) | Cluster isolation token. Prevents cross-cluster traffic |
| `NumWorkers` | uint64 | `runtime.NumCPU()` | Workers per tier (step, commit, apply) |
| `MaxApplyWorkers` | int | `runtime.NumCPU()` | Max concurrent per-shard apply goroutines |
| `CommitCBufferSize` | int | `64` | Commit channel buffer depth |
| `ShutdownTimeout` | Duration | `30s` | Max wait for graceful drain on Close() |
| `MaxWALDiskSize` | uint64 | `0` (unlimited) | WAL directory size limit in bytes |
| `NoSyncWAL` | bool | `false` | Skip fsync after WAL writes. **Testing only — this discards Raft's durability guarantee** |
| `MaxTotalInMemLogSize` | uint64 | `0` (disabled) | Global rate limiter threshold (bytes) |
| `AllowZeroDeploymentID` | bool | `false` | Allow unset DeploymentID (unsafe in production) |
| `NotifyCommit` | bool | `false` | Fire OnCommit callbacks |
| `HostID` | string | (auto-generated) | Unique host identifier (empty = generate on first boot) |
| `EventListener` | *EventListener | `nil` | Callback handlers |
| `TransportConfig` | TransportConfig | (required for multi-node) | QUIC transport configuration |
| `KeyRotationInterval` | Duration | `0` (disabled) | Auto barrier epoch rotation interval. A non-zero value must be >= 1s. Recommended: 24h |
| `MaxRetainedEpochs` | uint64 | `0` (retain all) | Barrier epochs to retain after compaction. `Validate` warns when key rotation is on and this is 0 |
| `LogConfig` | LogConfig | text/info | Format: text/json, Level: debug/info/warn/error |

### Config (per-shard)

Source: `pkg/config/config.go`

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `ShardID` | uint64 | (required, > 0) | Shard identifier (caller-provided) |
| `ReplicaID` | uint64 | (required, > 0, <= MaxUint32) | Replica identifier (caller-provided) |
| `IsObserver` | bool | `false` | Non-voting replica (no quorum contribution). Join-only: never list it in the `StartShard` members map — see below |
| `IsWitness` | bool | `false` | Non-storage replica (participates in quorum only). Join-only: never list it in the `StartShard` members map — see below |
| `Witness` | WitnessConfig | zero | Bootstrap seed for the shard's replicated witness policy. After bootstrap the policy lives in `proto.Membership` |
| `MemberIdentities` | map[uint64]string | `nil` | Bootstrap seed for committed node identities. **Every founder must pass an identical map** |
| `CheckQuorum` | *bool | `nil` = enabled | Leader steps down on quorum loss (safety-critical). Set with `config.BoolPtr(false)` to disable |
| `PreVote` | *bool | `nil` = enabled | Pre-vote prevents disruptive elections (safety-critical). Set with `config.BoolPtr(false)` to disable |
| `ElectionRTT` | uint64 | `10` | Election timeout in RTT multiples (must be >= 5) |
| `HeartbeatRTT` | uint64 | `1` | Heartbeat interval in RTT multiples |
| `SnapshotEntries` | uint64 | `100000` | Entries between auto-snapshots. **Zero does NOT disable it** — assign `config.SnapshotsDisabled` for manual-only |
| `CompactionOverhead` | uint64 | `5000` | Entries retained after compaction |
| `MaxInMemLogSize` | uint64 | `0` (disabled) | Per-shard in-memory log rate limit (bytes) |
| `MaxProposalPayloadSize` | uint64 | `8388608` (8 MiB) | Max individual proposal size |
| `MaxEntrySize` | uint64 | `8388608` (8 MiB) | Max entry size in log |
| `MaxApplyEntrySize` | uint64 | `16711672` (~15.94 MiB) | Max batch size per SM.Update call (replication chunk size). Cannot exceed `config.MaxReplicationPayloadBytes` |
| `MaxSnapshotSize` | uint64 | `4294967296` (4 GiB) | Max snapshot file size |
| `MaxSessionsPerClient` | uint64 | `128` | Cached responses retained per session, despite the name (bootstrap seed; replicated) |
| `MaxTotalSessions` | uint64 | `16384` | Per-shard total session limit (bootstrap seed; replicated) |
| `MaxSessionsPerOwner` | uint64 | `128` | Concurrent sessions one embedder-supplied `OwnerID` may hold. `config.SessionsPerOwnerUnlimited` disables (bootstrap seed; replicated) |
| `SnapshotCompressionType` | CompressionType | `NoCompression` | Snapshot compression: `NoCompression` or `Snappy` |
| `EntryCompressionType` | CompressionType | `NoCompression` | Entry compression: `NoCompression` or `Snappy` |
| `Quiesce` | bool | `false` | Quiescence for idle shards |
| `OrderedConfigChange` | *bool | `nil` = enabled | Serialize membership changes (required for safety). Set with `config.BoolPtr(false)` to disable |
| `ZeroCopyEntryCmd` | bool | `false` | Zero-copy entry data (SM must copy if retained) |
| `SessionExpiryEntries` | uint64 | `100000` | Bootstrap seed for the shard's replicated expiry threshold; `config.SessionExpiryDisabled` turns expiry off. A non-zero value must be >= 100 |
| `LeaseRead` | bool | `false` | Lease-based reads (no heartbeat RTT, requires CheckQuorum) |
| `DataDir` | string | `""` | Per-shard data directory. Empty puts the shard under the host's `NodeHostDir`; a non-empty value must be absolute. Recorded in the bootstrap record and used by `ImportSnapshot` |

Three fields use a sentinel rather than zero to mean "off", because an unset
field cannot mean both "I did not choose" and "never do this":
`SnapshotEntries` → `config.SnapshotsDisabled`, `SessionExpiryEntries` →
`config.SessionExpiryDisabled`, `MaxSessionsPerOwner` →
`config.SessionsPerOwnerUnlimited`. All three sentinels are `math.MaxUint64`.
Leaving any of them zero selects the default in the table, not the disabled
behaviour.

#### Starting a witness or an observer

A replica's witness/observer role is REPLICATED state (`Membership.Witnesses` /
`Membership.Observers`); `IsWitness` / `IsObserver` only tell the local replica
what it was started as. The `members` map passed to `StartShard` seeds
`Membership.Addresses` — the **full voter set** — on every replica that lists
it, so naming a witness there makes it a full voter in the configuration the
whole shard agrees on: the leader replicates full entry payloads to it (no
metadata-only stripping), it counts as a full replica for
`RequireFullReplicaMajority` and toward every quorum, it consumes full-replica
disk, and the only witness-like behaviour left is this host refusing the reads
you wanted it to serve. `StartShard` rejects that shape with
`*NonVotingRoleInMembersError` (sentinel `ErrNonVotingRoleInMembers`).

The check applies to a **first start only**. On a restart the membership is
reconstructed from the log, so the committed configuration governs and the local
`IsWitness` / `IsObserver` bit is not re-validated against the passed map.

Use the two-step flow:

1. Bootstrap the full voters with `StartShard(members, join=false, ...)`.
2. On the witness/observer node, `StartShard(nil, join=true, ...)` with
   `IsWitness` / `IsObserver` set.
3. On the leader, `SyncRequestAddWitness` / `SyncRequestAddObserver` for that
   replica ID. The role then enters membership through a committed config change
   every replica applies identically.

### TransportConfig

Source: `pkg/config/transportconfig.go`

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `MTLSConfig` | *MTLSConfig | (required unless `TransportDisabled`) | Mutual TLS certificates and key |
| `RevocationConfig` | *RevocationConfig | `nil` | Optional CRL/OCSP certificate revocation checks |
| `StreamPoolSize` | int | `16` | QUIC streams per connection |
| `MaxStreamPoolSize` | int | `256` | Upper bound for stream pool auto-scaling |
| `MaxStreamsPerConnection` | int | `256` | Peer-facing cap on concurrent streams per QUIC connection |
| `StreamReadIdleTimeout` | Duration | `30s` | Idle timeout on a stream read before it is abandoned |
| `MaxSnapshotReceiveRate` | int64 | `268435456` (256 MiB/s) | Inbound snapshot bandwidth per connection |
| `MaxConcurrentSnapshotReceives` | int | `4` | Concurrent inbound snapshot streams |
| `MaxSnapshotReceiveMemory` | int64 | `1073741824` (1 GiB) | Total snapshot receive byte budget |
| `MaxMessageReceiveMemory` | int64 | `268435456` (256 MiB) | Total in-flight inbound message byte budget |
| `MaxDecompressedSize` | uint32 | `16777216` (16 MiB) | Max Snappy decompressed frame size; pinned — `Validate` requires exact equality with the default, so it is not tunable |
| `MaxConnectionsPerIP` | int | `16` | Per-source-IP connection limit |
| `MaxIncomingConnections` | int | `256` | Total inbound connection limit |
| `SendBatchMaxSize` | int | `65536` (64 KiB) | Max send buffer size before flush |
| `UDPRecvBufSize` | int | `7340032` (7 MiB) | UDP receive socket buffer size |
| `UDPSendBufSize` | int | `7340032` (7 MiB) | UDP send socket buffer size |
| `Enable0RTT` | bool | `false` | QUIC 0-RTT (replayable; trusted networks only) |
| `DisableCompression` | bool | `false` | Disable message batch compression |
| `TransportDisabled` | bool | `false` | Single-node mode. Set by `WithoutTransport()`; relaxes the mTLS requirement |
| `RequireMemberIdentity` | bool | `false` | Drop quorum-bearing traffic from members with no committed node identity. See [Compatibility](../compatibility.md#enabling-requirememberidentity) before enabling |

### MTLSConfig

Source: `pkg/config/transportconfig.go`

```go
type MTLSConfig struct {
    CACert      []byte           // PEM-encoded CA certificate (always required)
    Cert        []byte           // PEM-encoded node certificate
    Key         []byte           // PEM-encoded private key
    Certificate *tls.Certificate // Pre-built leaf, as an alternative to Cert/Key
}
```

`CACert` is always required and must parse. Supply the leaf either as
`Cert`+`Key` PEM or as a pre-built `Certificate`; when `Certificate` is set,
`Cert` and `Key` may be empty and its `PrivateKey` must satisfy
`crypto.Signer`. The `Certificate` form is what a deployment backed by an HSM or
a KMS uses, since the private key never becomes PEM.

QuicRaft enforces mutual TLS with certificate verification on both client and
server sides. **There is no TCP fallback and no unauthenticated mode**: a
multi-node host without a usable `MTLSConfig` fails `Validate` and never starts.
See [Encryption](../seal-barrier/architecture.md) for key management workflows.

### RevocationConfig (optional)

```go
type RevocationConfig struct {
    CRLPaths           []string           // Paths to CRL files
    OCSPResponderURL   string             // OCSP responder endpoint
    OCSPCacheSeconds   int                // 0 -> 300
    OCSPTimeoutSeconds int                // 0 -> 5
    CheckInterval      time.Duration      // 0 -> 60s
    Mode               string             // "" (= "any") / "crl" / "ocsp" / "both" / "any"
    AllowSoftFail      bool               // ADMIT peers whose status is unknown (default false)
    OnCertRevoked      func(serial string) // Callback; serial is lowercase hex
}
```

Enables certificate revocation checking via CRL and/or OCSP. Optional; used only
if present.

`Mode` defaults to `"any"` when left empty, and any other unrecognised value is
rejected by `Validate`. `"crl"` and `"both"` require a non-empty `CRLPaths`;
`"ocsp"` and `"both"` require `OCSPResponderURL`. The three numeric zero values
are **not** filled in by `pkg/config` — they are substituted inside the
transport's revocation checker, so reading them back from your `HostConfig` will
still show zero.

**Operational behavior:**

- **Leaf-only checks:** Only the peer's leaf (end-entity) certificate is revocation-checked, not the full presented chain. The leaf is what gets revoked when a node is decommissioned or compromised; the CA chain is governed by the CA's own lifecycle. This avoids per-handshake OCSP latency for rarely-changing intermediate/root certificates. Checks run via the TLS `VerifyConnection` callback, so they also apply to resumed TLS sessions.
- **OCSP negative cache:** When an OCSP responder is unreachable (down, timeout, network error), the failure is cached for a short 10 s window. During that window, reconnect handshakes do not re-query the dead responder, so they avoid adding the full `OCSPTimeoutSeconds` to every handshake while the responder is down. The negative cache only suppresses re-queries — it never reports a cert as "good" or "revoked" — and its TTL is far shorter than the positive-result cache (`OCSPCacheSeconds`, default 300 s), so the responder is retried promptly once it recovers.
- **Fail-closed by default:** peers are rejected when revocation status cannot be determined (responder down, CRL stale or unreadable, no revocation source configured). The knob is `AllowSoftFail`, and its zero value (`false`) is the fail-closed posture — a `RevocationConfig` you never touch enforces revocation. Setting `AllowSoftFail: true` inverts that and admits unknown-status peers, so an attacker whose certificate was revoked can keep connecting by making the responder appear unreachable. A definitive "revoked" verdict rejects either way. (The transport's internal `revocation.Config` carries an `AllowSoftFail` field of the same sense, which the host copies across; a zero-valued internal config enforces too, so the fail-closed posture does not depend on the mapping.)

## Monitoring

### Prometheus Metrics

Source: `pkg/metrics_prometheus.go`

Enable with `WithMetrics`:

```go
collector := quicraft.NewPrometheusMetricsCollector()
host, _ := quicraft.NewHost(cfg, quicraft.WithMetrics(collector))

// Serve metrics via an HTTP handler:
http.Handle("/metrics", promhttp.HandlerFor(collector.Registry, promhttp.HandlerOpts{}))
```

All metrics use the `quicraft` namespace and are registered on a dedicated `prometheus.Registry` (not the global default).

Call `RegisterShard` when a shard starts and `UnregisterShard` when it stops. `UnregisterShard` deletes the shard's child series, so a host that starts and stops shards over time does not accumulate series forever.

#### Metrics cardinality

**The default aggregates most metrics to host level.** `NewPrometheusMetricsCollector` uses the zero `config.MetricsConfig`, in which `PerShardMetrics` is `false`. Opt in to a per-shard breakdown with:

```go
collector := quicraft.NewPrometheusMetricsCollectorWithConfig(config.MetricsConfig{
    PerShardMetrics: true,
})
```

The mode is fixed when the collector is built — a Prometheus vector's label set is decided at construction — so it cannot be changed on a running host, and it is deliberately not a `HostConfig` field.

Why the default is off: a histogram child is one series per bucket plus `_sum` and `_count`, so the per-shard cost is much larger than the number of shard-labelled metric names suggests. Measured:

| Mode | Series per active shard | 10,000 shards on one host |
|------|------------------------:|--------------------------:|
| `PerShardMetrics: false` (default) | 6 | ~60,000 |
| `PerShardMetrics: true` | 113 | ~1,130,000 |

A million active series from a single host is enough to destabilise a shared Prometheus, and that failure lands on every other tenant of it. Enable per-shard metrics for small deployments, or temporarily while bisecting one shard's latency.

**Turning it off never costs an alert.** These keep their `shard_id` label in *both* modes, because the shard is what makes the page actionable:

- `quicraft_raft_elections_total` (leader change)
- `quicraft_raft_proposals_dropped_total`
- `quicraft_transport_messages_dropped_total`
- `quicraft_transport_snapshots_rejected_total`
- `quicraft_security_unauthorized_messages_dropped_total`
- `quicraft_security_unauthorized_snapshots_dropped_total`

What aggregates is volume-and-latency telemetry, whose per-shard breakdown is a debugging aid rather than an alerting input.

**Reading the mode from the scrape.** Aggregated series keep the *same metric name* and drop only the `shard_id` label, so a rule written as `sum without(shard_id) (...)` is correct in both modes. Their `# HELP` text states that they are aggregated, and the mode is also exported directly:

| Metric | Type | Labels | Description |
|--------|------|--------|-------------|
| `quicraft_metrics_per_shard_enabled` | Gauge | -- | 1 when per-shard labels are on, 0 when aggregated |

A dashboard can branch on that gauge instead of reading the host config.

#### Metric reference

In the tables below, `shard_id*` marks a label present **only** when `PerShardMetrics` is enabled.

**Raft metrics** (`quicraft_raft_*`):

| Metric | Type | Labels | Description |
|--------|------|--------|-------------|
| `proposal_latency_seconds` | Histogram | `shard_id*` | End-to-end proposal latency |
| `commit_latency_seconds` | Histogram | `shard_id*` | Proposal-to-commit latency |
| `apply_latency_seconds` | Histogram | `shard_id*` | Commit-to-apply latency |
| `read_index_latency_seconds` | Histogram | `shard_id*` | ReadIndex latency |
| `proposal_batch_size` | Histogram | `shard_id*` | Proposals per Raft append |
| `snapshots_total` | Counter | `shard_id*` | Snapshot count |
| `piggyback_commits_total` | Counter | `shard_id*` | Commits that skipped a second pipeline cycle |
| `proposals_dropped_total` | Counter | `shard_id` | Dropped proposals |
| `elections_total` | Counter | `shard_id` | Election count |
| `commit_batch_size` | Histogram | -- | Entries per commit batch |

**LogDB metrics** (`quicraft_logdb_*`) — never per-shard:

| Metric | Type | Labels | Description |
|--------|------|--------|-------------|
| `write_latency_seconds` | Histogram | -- | WAL write latency |
| `fsync_latency_seconds` | Histogram | -- | WAL fsync latency |
| `compaction_latency_seconds` | Histogram | -- | WAL compaction latency |
| `write_bytes_total` | Counter | -- | Total WAL bytes written |

**Transport metrics** (`quicraft_transport_*`):

| Metric | Type | Labels | Description |
|--------|------|--------|-------------|
| `send_latency_seconds` | Histogram | -- | Message send latency |
| `messages_sent_total` | Counter | -- | Messages sent (aggregate) |
| `messages_received_total` | Counter | -- | Messages received (aggregate) |
| `messages_sent_detailed_total` | Counter | `shard_id`, `to`, `msg_type` | Messages sent, only when `EnablePerMessageMetrics` |
| `messages_received_detailed_total` | Counter | `shard_id`, `from`, `msg_type` | Messages received, only when `EnablePerMessageMetrics` |
| `snapshots_sent_total` | Counter | `shard_id*` | Snapshots sent |
| `snapshots_sent_bytes` | Histogram | `shard_id*` | Bytes per snapshot send |
| `snapshots_received_total` | Counter | `shard_id*` | Snapshots received |
| `snapshots_received_bytes` | Histogram | `shard_id*` | Bytes per snapshot receive |
| `messages_dropped_total` | Counter | `shard_id`, `reason` | Raft messages discarded by the transport |
| `snapshots_rejected_total` | Counter | `shard_id`, `reason` | Inbound snapshot transfers discarded |

`EnablePerMessageMetrics` is a separate switch, also off by default, and it is the highest-cardinality option here: its label set is shard x target replica x message type. Enable it only for short debugging sessions.

**Security metrics** (`quicraft_security_*`) — always per-shard, low frequency by design; every increment is a rejected message:

| Metric | Type | Labels | Description |
|--------|------|--------|-------------|
| `unauthorized_messages_dropped_total` | Counter | `shard_id`, `reason` | Inbound Raft messages failing shard-membership authorization |
| `unauthorized_snapshots_dropped_total` | Counter | `shard_id` | Inbound snapshots failing authorization or TLS identity binding |

The sender's replica id is deliberately **not** a label on the security and drop counters: it arrives unauthenticated in the inbound message, so an attacker could emit arbitrarily many distinct values and create unbounded, never-reclaimed series. It is recorded in the corresponding log line instead.

Latency buckets: 100us to 10s. Batch size buckets: 1 to 1024 (powers of two). Snapshot byte buckets: 1KB to 1GB.

### Health Checks

Source: `pkg/host.go`

`Host.HealthCheck()` returns a point-in-time `HealthStatus`:

```go
status := host.HealthCheck()
// status.Status: "ok" | "degraded" | "sealed" | "unhealthy"
// status.BarrierSealed: true if barrier is configured and sealed
// status.DiskOK: true if no disk full condition
// status.ShardCount: number of loaded shards
// status.Shards: per-shard ShardInfo
```

Status resolution order:
1. `"sealed"` -- barrier configured and sealed (503)
2. `"unhealthy"` -- no shards loaded (503)
3. `"degraded"` -- any shard has `ShardError` health (503)
4. `"ok"` -- all shards healthy (200)

Per-shard health values (`ShardHealth`, `pkg/info.go:24`): `healthy`,
`no-leader`, `lagging`, `unloaded`, `error`, `recovering`, `snapshotting`. Only
`error` contributes to a `degraded` host status.

`ShardHealth.UnmarshalJSON` decodes an unrecognised string to `healthy` rather
than erroring, so do not round-trip health through JSON to detect a typo.

### EventListener Callbacks

Source: `pkg/config/hostconfig.go`

Optional per-type callbacks invoked synchronously on engine workers. Callbacks must not block. Configure via `HostConfig.EventListener`:

```go
cfg.EventListener = &config.EventListener{
    // Raft lifecycle
    OnLeaderUpdated:      func(info config.LeaderInfo) { /* ... */ },
    OnLeaderLost:         func(info config.EventShardInfo) { /* ... */ },
    OnCampaignLaunched:   func(info config.EventShardInfo) { /* ... */ },

    // Snapshot operations
    OnSnapshotCreated:     func(info config.SnapshotInfo) { /* ... */ },
    OnSnapshotSendStarted: func(info config.SnapshotInfo) { /* ... */ },
    OnSnapshotSent:        func(info config.SnapshotInfo) { /* ... */ },
    OnSnapshotSendAborted: func(info config.SnapshotInfo) { /* ... */ },
    OnSnapshotReceived:    func(info config.SnapshotInfo) { /* ... */ },
    OnSnapshotRecovered:   func(info config.SnapshotInfo) { /* ... */ },
    OnSnapshotRejected:    func(info config.SnapshotInfo) { /* ... */ },
    OnSnapshotCompacted:   func(info config.SnapshotInfo) { /* ... */ },

    // System events
    OnNodeHostShuttingDown: func() { /* ... */ },
    OnNodeReady:           func(info config.EventShardInfo) { /* ... */ },
    OnNodeUnloaded:        func(info config.EventShardInfo) { /* ... */ },
    OnNodeDeleted:         func(info config.EventShardInfo) { /* ... */ },
    OnMembershipChanged:   func(info config.EventShardInfo) { /* ... */ },
    OnConnectionEstablished: func(info config.ConnectionInfo) { /* ... */ },
    OnConnectionFailed:    func(info config.ConnectionInfo) { /* ... */ },
    OnLogCompacted:        func(info config.EventShardInfo) { /* ... */ },
    OnLogDBCompacted:      func(info config.EventShardInfo) { /* ... */ },

    // Failures
    OnShardFailed:        func(info config.ShardFailedInfo) { /* ... */ },
    OnLogDBDiskWarning:   func(info config.DiskInfo) { /* ... */ },
    OnLogDBDiskFull:      func(info config.DiskInfo) { /* ... */ },

    // Proposal and read drops
    OnProposalDropped:    func(info config.EventShardInfo) { /* ... */ },
    OnReadIndexDropped:   func(info config.EventShardInfo) { /* ... */ },
}
```

`config.EventListener` is a **struct of 26 optional func fields**, not an
interface: set only the callbacks you need and leave the rest nil. Only
non-nil callbacks are invoked. `OnNodeHostShuttingDown` is the only one that
takes no argument.

> **`OnCampaignSkipped` is declared but never fires. Do not build on it.**
> The field exists on `config.EventListener`, the plumbing behind it is
> complete, and nothing ever calls it: `git grep -n OnCampaignSkipped -- pkg/`
> returns the interface declaration, the `hostEngineCallback` implementation,
> and test doubles. There is no production call site.
>
> It is left out of the example above because of *which* conditions it claims
> to report. Every campaign-refusal branch in `startElection` and
> `handleTimeoutNow` (`pkg/internal/raft/raft.go:1568+`) logs and returns
> without notifying the engine — `selfRemoved`, `roleMismatch`, `joining`, an
> unresolved stale config tail, and an unreadable own-log term. Those are
> precisely the states that leave a shard leaderless while quorum looks intact.
> Wiring this listener buys silence during the incident it appears to explain.
>
> Detect those conditions from the logs and from `ShardInfo` (`Health`
> reaching `no-leader` with no `elections_total` increment) instead.

Most shard-scoped callbacks share one payload type, `config.EventShardInfo`
(`ShardID`, `ReplicaID`, `LeaderID`, `Term`; `LeaderID` is 0 when no leader is
known). There is no per-event info type for membership changes, proposal drops
or read-index drops. The specialized payloads are:

| Type | Fields |
|------|--------|
| `config.LeaderInfo` | `ShardID`, `ReplicaID`, `Term`, `LeaderID` |
| `config.SnapshotInfo` | `ShardID`, `ReplicaID`, `Index`, `From` |
| `config.ShardFailedInfo` | `ShardID`, `ReplicaID`, `Err` |
| `config.DiskInfo` | `WALDir`, `UsedBytes`, `MaxBytes`, `Shards` |
| `config.ConnectionInfo` | `Address`, `Err` |

`config.LeaderInfo` and `config.EventShardInfo` carry the same four field names
in a different declaration order, so construct them with field names rather than
positionally.

Callbacks are invoked **synchronously on engine worker goroutines and must
not block.** In particular they run outside the per-node `raftMu`, and code
that re-enters the Host from a callback risks stalling the commit worker for
every shard that worker owns.

## Disk Management

### WAL Disk Monitoring

Source: `pkg/logdb/waldb/diskmon.go`

When `MaxWALDiskSize` is set, a `DiskMonitor` checks disk usage every 100 writes via `syscall.Statfs`. Two thresholds trigger callbacks:

| Threshold | Default | Behavior |
|-----------|---------|----------|
| Warning | 85% | Fires `OnLogDBDiskWarning` callback (once per crossing) |
| Full | 95% | Fires `OnLogDBDiskFull`, returns `ErrDiskFull`, sets `diskFull` flag |

The `diskFull` flag is reflected in `HealthCheck().DiskOK`. Callbacks fire on state transitions only (crossing above/below threshold), not on every check.

The `DiskMonitor` supports a recovery callback via `WithOnRecovery`. When disk usage drops below the full threshold after being in the full state, the recovery callback fires. This enables automatic clearing of external disk-full flags when disk space is freed (for example, after log compaction or manual cleanup).

```go
cfg := config.HostConfig{
    MaxWALDiskSize: 10 * 1024 * 1024 * 1024, // 10 GB
    EventListener: &config.EventListener{
        OnLogDBDiskWarning: func(info config.DiskInfo) {
            slog.Warn("disk warning", "used", info.UsedBytes, "max", info.MaxBytes)
        },
        OnLogDBDiskFull: func(info config.DiskInfo) {
            slog.Error("disk full", "used", info.UsedBytes, "max", info.MaxBytes)
        },
    },
}
```

### Compaction and Snapshot GC

Log compaction is triggered by the `SnapshotEntries` setting. When the applied index exceeds the last snapshot index by `SnapshotEntries`, the engine creates a snapshot and compacts the log, retaining `CompactionOverhead` entries for follower catch-up.

Compaction lifecycle:
1. Auto-snapshot triggered at `lastSnapshot + SnapshotEntries`
2. `SM.SaveSnapshot()` writes state to disk
3. `RemoveEntriesTo(snapshotIndex - CompactionOverhead)` marks old entries
4. Segment GC deletes files with no live entries
5. `OnSnapshotCreated` and `OnLogCompacted` callbacks fire

Manual snapshots via `Host.RequestSnapshot()` also trigger compaction.

### WAL Segment Rotation

Source: `pkg/logdb/waldb/segment.go`

Each WAL shard manages its own set of segment files. A segment is rotated (closed and replaced by a new one) when a write would exceed the segment's `MaxFileSize` (default 64 MB). The active segment returns `ErrSegmentFull` on the write attempt, and the WAL shard creates a new segment with an incremented ID.

WAL segment defaults:

| Parameter | Default | Description |
|-----------|---------|-------------|
| `BlockSize` | 32 KB | WAL block size. Records cannot cross block boundaries |
| `MaxFileSize` | 64 MB | Maximum segment file size before rotation |
| `MaxBatchSize` | 64 MB | Maximum write batch size |

New segments are pre-allocated on disk using `fallocate` (Linux) or `ftruncate` to avoid filesystem metadata updates on each append. This reduces fsync latency.

### WAL Compaction

Source: `pkg/logdb/waldb/db.go`

`LogDB.Compact(shardID, replicaID)` triggers garbage collection for a WAL shard. The compactor:

1. Collects the set of segment IDs still referenced by any live node's index entries
2. Deletes segment files that are not in the live set and are not the active segment
3. Returns the number of segments deleted

Index compaction uses binary search to discard entries at or below a given index, shifting remaining entries to the front. Compaction is idempotent and safe to call concurrently from different goroutines (the WAL shard's mutex serializes the operation).

### Snapshot Operations

Source: `pkg/internal/engine/snapshot_pool.go`, `pkg/host.go`

**Manual snapshots:**

```go
// SnapshotOption is a plain struct passed BY VALUE and is required.
// The zero value uses shard-level defaults.
rs, err := host.RequestSnapshot(ctx, shardID, quicraft.SnapshotOption{})
result, err := rs.Result()

// Synchronous form returns the snapshot index directly.
index, err := host.SyncRequestSnapshot(ctx, shardID, quicraft.SnapshotOption{})
```

`SnapshotOption` has no `With...` constructors -- set its fields directly:

| Field | Purpose |
|-------|---------|
| `ExportPath` | Write an exported snapshot to this path instead of the shard's snapshot dir |
| `CompactionOverhead` | Per-request override of the shard's compaction overhead |
| `OverrideCompactionOverhead` | Must be true for `CompactionOverhead` to take effect |
| `Exported` | Set automatically when `ExportPath` is non-empty; not normally set by callers |

`RequestSnapshot` atomically claims the per-shard snapshotting slot. Only one snapshot per shard can be in progress; concurrent requests return `ErrSnapshotInProgress`.

**MaxSnapshotSize enforcement:** After `SM.SaveSnapshot()` writes the data file and fsyncs it to disk, the snapshot pool checks the actual file size against `Config.MaxSnapshotSize` (default 4 GB). If the file exceeds the limit, the oversized snapshot directory is removed and a `SnapshotTooLargeError` is returned. The size check runs after fsync to measure the actual on-disk size (post-compression when Snappy is enabled).

**Snapshot encryption:** When a barrier is configured and not sealed, snapshot data files are encrypted in-place after creation and decrypted in-place before recovery. The encrypt/decrypt operation is atomic:

1. Read the plaintext/ciphertext file
2. Write the encrypted/decrypted output to a temporary file in the same directory
3. Fsync the temporary file
4. Rename the temporary file over the original (atomic on POSIX)
5. If any step fails, the temporary file is removed and the original is preserved intact

The encrypted format is a sequence of frames: `[ChunkLen:4 | EncryptedChunk]...` where each `EncryptedChunk` includes the barrier's epoch, nonce, and authentication tag prefix. The epoch is recorded in snapshot metadata so the correct DEK is used for decryption on recovery.

**Export and import:** For manual backup and restore of snapshot data:

```go
// Export the latest snapshot for a shard to a writer (e.g., file or network stream)
err := host.ExportSnapshot(ctx, shardID, writer)

// Import a snapshot for a stopped shard from a reader
// The shard must NOT be running on this host
err := host.ImportSnapshot(ctx, shardID, reader)
```

`ImportSnapshot` writes the data to disk, records metadata in LogDB, and the next `StartShard` call recovers from it. This is useful for bootstrapping a new replica or manual disaster recovery.

### Session Management Operations

Source: `pkg/config/config.go`, `pkg/internal/rsm/statemachine.go`

Client sessions provide at-most-once delivery semantics. Sessions are registered and unregistered through Raft proposals (not local-only), ensuring all replicas agree on the set of active sessions.

**Session limits.** Three replicated limits, none of which means quite what its
name suggests:

| Field | Default | What it actually bounds |
|---|---|---|
| `MaxTotalSessions` | 16384 | Sessions a shard admits, shard-wide. Over-limit registration is rejected with `ErrSessionLimitExceeded` |
| `MaxSessionsPerClient` | 128 | **Not a session count.** The depth of the per-session response cache; an over-limit response is rejected with `ErrResponseLimitExceeded` |
| `MaxSessionsPerOwner` | 128 | Concurrent sessions one embedder-supplied `OwnerID` may hold. Over-limit yields a `*SessionError` wrapping `ErrInvalidOperation` |

All three are bootstrap seeds for the shard's REPLICATED session policy, not
per-node settings: see the note below.

**Owner-scoped sessions.** `MaxTotalSessions` alone lets one embedder exhaust a
shard's whole session table, so sessions can be attributed to an owner:

```go
// Owner-scoped: counts against MaxSessionsPerOwner for ownerID.
session, err := host.GetNewSessionWithOwner(ctx, shardID, ownerID)

// Unowned: no owner declared, so no per-owner cap applies.
session, err := host.GetNewSession(ctx, shardID)
```

The library **cannot derive** an `OwnerID` — it authenticates nodes, not
clients — so its trustworthiness is entirely yours. Use a tenant ID, an account
number, or a hash of an authenticated subject. An `OwnerID` of 0 means "no owner
declared" and is exempt from the cap, so a deployment that wants the cap
enforced must supply a non-zero owner on every session it opens. The value is
immutable after construction, stamped into the replicated register entry as
`proto.Entry.OwnerID`, and the cap is enforced by the deterministic apply path
on every replica rather than as a leader-local admission decision.

On an `sm.DiskStateMachine` shard, every session form except `GetNoOPSession`
returns `ErrSessionsUnsupportedOnDisk`.

**Automatic session expiry:** `SessionExpiryEntries` controls deterministic cleanup of abandoned sessions. Sessions that have not been active within this many applied log entries are expired during the apply path. Expiry is driven by log index, not wall-clock time, and the threshold is replicated, so every replica expires the same sessions at the same point in the log. The default is 100,000 entries.

```go
shardCfg := config.Config{
    SessionExpiryEntries: 100000, // expire sessions inactive for 100K entries
}
```

> **The four session limits are REPLICATED, and the `Config` fields are only the bootstrap seed.** `SessionExpiryEntries`, `MaxTotalSessions`, `MaxSessionsPerClient` and `MaxSessionsPerOwner` all drive the deterministic apply path — the expiry checkpoints are multiples of the threshold, registrations are refused past `MaxTotalSessions`, and the response cache is bounded by `MaxSessionsPerClient` — so two replicas evaluating different values expire different sessions or disagree on whether a session exists at all. One then answers a session-managed proposal from its response cache while the other reports the session expired: a permanent state-machine divergence that no Raft invariant detects, because the log, the commit index and the applied index are identical on both sides.
>
> A founding replica therefore writes all three into the shard's initial membership, and every replica applies the committed values. What this means operationally:
>
> - **All founders must supply the same values.** A differing value produces a different index-1 entry, exactly as it does for the witness policy.
> - **A node whose configuration disagrees with its shard applies the shard's values** and logs the disagreement at WARN, naming both. It does not refuse to start: the committed value can only be changed through consensus, so refusing would strand the node. Grep for `applying the shard's replicated session policy` after a config change to see whether a node's edit actually took effect.
> - **`Host.GetShardMembership` reports the committed policy** (`SessionExpiryEntries`, `MaxTotalSessions`, `MaxSessionsPerClient`, `MaxSessionsPerOwner`), which is the value that governs.
> - **There is no config change for these yet.** The committed policy is fixed for the life of the shard, so `MaxTotalSessions` cannot be raised on a running shard. Size it at bootstrap.
> - **A shard bootstrapped by an earlier build carries no policy** and its replicas keep using local configuration; for those shards the old rule — set the values identically on every replica, and nothing enforces it — still applies.

Set `SessionExpiryEntries` to `config.SessionExpiryDisabled` to turn automatic expiry off; leaving it zero selects the default instead. With expiry disabled, sessions are only removed via explicit unregister or when `MaxTotalSessions` forces rejection of new registrations — so a client that registers a session and dies without `CloseSession` leaks an entry that is serialized into every subsequent snapshot and counts against `MaxTotalSessions` forever. Keep the threshold at or below a non-zero `SnapshotEntries` so expiry runs at least once per snapshot interval; `Validate` warns when it does not.

### Connection Limits

Source: `pkg/internal/transport/recv.go`

The QUIC transport enforces two tiers of inbound connection limits:

**Global connection limit:** A semaphore of size `MaxIncomingConnections` (default 256) gates the accept loop. When the semaphore is full, new connections are rejected with QUIC error code 1 ("connection limit reached"). The semaphore slot is released when the connection handler returns.

**Per-IP connection limit:** An `ipConnTracker` uses a mutex-protected map of `atomic.Int32` counters keyed by source IP. Before accepting a connection, `tryAcquire(ip)` atomically increments the counter via compare-and-swap, returning false if the count would exceed `MaxConnectionsPerIP` (default 16). Rejected connections receive QUIC error code 2 ("per-IP connection limit reached"). The counter is decremented when the connection handler exits.

The per-IP tracker prevents a single source from exhausting the global connection budget (for example, a misconfigured client reconnect loop or a denial-of-service attempt). Both limits are logged at `slog.Warn` level when triggered.

### Rate Limiting

Source: `pkg/host.go`, `pkg/internal/server/rate.go`

QuicRaft implements a two-tier rate limiting system that applies backpressure when in-memory log buffers grow too large.

**Per-shard rate limiting (`MaxInMemLogSize`):** When the in-memory log for a shard exceeds `MaxInMemLogSize`, the `InMemRateLimiter` activates with hysteresis: 100% of the limit to enter rate-limited state, 70% to exit. While rate-limited, `Propose()` returns a `BusyError` containing the shard ID, current size, and max size. The rate limiter also incorporates follower feedback: the leader tracks reported in-memory log sizes from followers and rate-limits when any follower exceeds the threshold.

**Global rate limiting (`MaxTotalInMemLogSize`):** When the total in-memory log size across all shards exceeds `MaxTotalInMemLogSize`, `Propose()` and `ReadIndex()` return `ErrSystemBusy`. The global rate limiter is checked in the `Host.Propose()` and `Host.ReadIndex()` methods before per-shard checks.

```go
// Per-shard rate limiting
shardCfg := config.Config{
    MaxInMemLogSize: 256 * 1024 * 1024, // 256 MB per shard
}

// Global rate limiting
hostCfg := config.HostConfig{
    MaxTotalInMemLogSize: 4 * 1024 * 1024 * 1024, // 4 GB total across all shards
}
```

When either rate limiter triggers, the caller should back off and retry. The `BusyError` (per-shard) includes diagnostic fields (`CurrentSize`, `MaxSize`) for log analysis. The global `ErrSystemBusy` indicates the host is overloaded and all shards should reduce proposal rates.

## Encryption at Rest

### Barrier Setup

```go
strategy, err := seal.NewSoftwareStrategy([]byte("passphrase"))
if err != nil {
    log.Fatal(err)
}
// NewBarrier returns (*Barrier, error): an invalid config is an error, never a
// panic. seal.MustNewBarrier is the panicking form for compile-time constants.
barrier, err := seal.NewBarrier(seal.DefaultBarrierConfig(deploymentID))
if err != nil {
    log.Fatal(err)
}

// First boot: generate root key, derive epoch-1 DEK, seal root key.
// Initialize takes (ctx, strategy, creds) and returns error.
if err := barrier.Initialize(ctx, strategy, seal.Credentials{}); err != nil {
    log.Fatal(err)
}

// Pass to host
host, _ := quicraft.NewHost(cfg, quicraft.WithBarrier(barrier))
```

On subsequent boots, load the persisted `SealedRootKey` and unseal:

```go
barrier := seal.MustNewBarrier(seal.DefaultBarrierConfig(deploymentID))
if err := barrier.SetSealedRootKey(loadedSealedKey); err != nil {
    log.Fatalf("invalid sealed key: %v", err)
}
// Unseal has the same shape as Initialize: (ctx, strategy, creds) error.
if err := barrier.Unseal(ctx, strategy, seal.Credentials{}); err != nil {
    log.Fatal(err)
}
host, _ := quicraft.NewHost(cfg, quicraft.WithBarrier(barrier))
```

### Automatic Key Rotation

Set `KeyRotationInterval` on the host config to enable periodic epoch rotation:

```go
cfg := config.HostConfig{
    // ...
    KeyRotationInterval: 24 * time.Hour,
}
```

The host runs a background goroutine that calls `barrier.Rotate()` at the configured interval. Each rotation derives a new DEK; old DEKs are retained for reading existing data.

### Manual Key Rotation

```go
newEpoch, err := barrier.Rotate()
```

### Epoch Purging

After compaction removes old WAL segments and snapshots, purge their DEKs:

```go
purged, err := barrier.PurgeEpochsBefore(minEpoch) // returns the count purged
```

Epochs with in-flight snapshot transfers (registered via `RegisterInFlightEpoch`)
are protected from purging. The current epoch is never purged.

**Get `minEpoch` wrong and the data is gone.** Purging the key that decrypts a
snapshot still on disk makes that snapshot permanently unreadable, and nothing
detects it until a recovery tries. The floor scan walks the snapshot tree, so an
imported snapshot outside that tree is invisible to it — which is why
`ImportSnapshot` refuses to guess a layout rather than writing somewhere the
scan will not look. See
[Disaster recovery](#disaster-recovery-importing-a-snapshot).

### Emergency Seal

For security incident response, immediately seal the barrier to make all at-rest data inaccessible:

```go
if err := host.EmergencySeal(); err != nil {
    slog.Error("emergency seal failed", "err", err)
}
```

All in-flight operations fail with `ErrBarrierSealed`, including a snapshot save
that was already in progress. Normal shutdown via `Host.Close()` seals the
barrier automatically after draining the pipeline; `EmergencySeal` skips the
drain, which is the point.

`Host` exposes no unseal counterpart — only `IsSealed()` to observe the state.
The embedder still holds the `*seal.Barrier` it passed to `WithBarrier`, so
recovery goes through that object's `Unseal`, which needs the strategy and
credentials again. Keep them reachable by whatever runs your incident response,
or the seal is one-way until a restart.

## Graceful Shutdown

### Drain Mode

Source: `pkg/host.go`

`Host.Drain()` transitions the host into drain mode:

1. Sets `draining` flag atomically via `CompareAndSwap` (idempotent -- second call returns immediately)
2. New `Propose()` and `ReadIndex()` calls return `ErrDraining`
3. Takes a fast path and returns immediately when no operations are pending
4. Otherwise blocks on a channel that `completePendingOp` closes when the pending-operation counter reaches zero -- event-driven, not polled -- against a single `ShutdownTimeout` timer
5. Returns when all in-flight work completes, or an error if `ShutdownTimeout` (default 30s) expires first

```go
// Programmatic
err := host.Drain()

// Check drain state
if host.IsDraining() { /* ... */ }
```

### Shutdown Orchestration

Source: `pkg/host.go`, `pkg/internal/engine/`

`Host.Close()` calls `Drain()` first, then tears down subsystems in a strict order. Each tier depends on the previous tier being operational, so the ordering prevents dropped work and data loss.

```
Host.Close()
  1. Set closed flag (atomic CAS, idempotent via sync.Once)

  2. Drain in-flight work
     - Drain() blocks until the pending-operation counter reaches zero
     - Bounded by ShutdownTimeout (default 30s)
     - If drain times out, logs a warning AND joins the error into Close's
       return value -- an abandoned drain must not report a clean shutdown

  3. Stop key rotation goroutine
     - close(keyRotStopC) signals, then <-keyRotDone waits
     - Prevents rotation during shutdown

  4. Stop pending-proposal sweeper
     - close(sweepStopC), then <-sweepDone

  5. Stop engine (tiered worker shutdown) -- BEFORE the transport
     - Per-tier WaitGroups ensure sequential close-and-wait:
       a. Step workers exit first (no more Raft ticks or proposal processing)
       b. Commit workers drain commitC, persist remaining entries, then exit
       c. Apply workers drain applyC, apply remaining entries to SM, then exit
       d. Snapshot workers cancel in-progress snapshots and exit

  6. Drain in-flight snapshot sends
     - transportSender.Wait() joins sends the (now-stopped) step workers
       launched before exiting; the transport is still up, so they complete

  7. Stop QUIC transport -- LAST
     - Closes all active QUIC connections
     - No send goroutine can touch a torn-down transport

  8. Stop all shard nodes
     - Set stopped flag, reject pending proposals/reads/snapshots with ErrClosed
     - Clear all pending maps

  9. Close LogDB
     - Flush buffered writes, fsync all WAL segments, close file handles

 10. Seal barrier (if configured)
     - barrier.Seal() zeros root key and all DEKs from memory
     - Already-sealed is not an error during shutdown
```

**Why this order matters:**

- **Engine before transport:** the engine's step workers are the *only*
  producers of snapshot sends, and each send does `snapshotWg.Add(1)`.
  `engine.Stop()` closes `stepStopC` and joins the step workers, after which
  no new `Add` can occur. Stopping the engine first is what eliminates the
  illegal "`WaitGroup.Add` concurrent with `Wait`" race, where a producer
  could call `Add(1)` while `Close` was already blocked in `Wait()`.
- **Transport last:** after all snapshot sends have drained, so no send
  goroutine touches a torn-down transport.
- **Step before commit before apply:** preserves the pipeline's natural flow
  direction. Each tier drains work from the previous tier.
- **LogDB before barrier:** LogDB reads and writes may need the barrier for
  decryption. Close LogDB before sealing the barrier.

`Close()` returns an `error`. It reports a drain that timed out, and any
error from the teardown steps; it no longer swallows them.

### Startup Order

Startup is the mirror image, and for the same reason -- **the transport
starts last**:

```
NewHost()
  1. Construct LogDB, barrier, registry, message handler
  2. Wire msgHandler.host, msgHandler.engine, the engine callback,
     and the per-shard import locker
  3. eng.Start()                     -- engine worker pipeline
  4. tr.Start()                      -- QUIC listener, LAST
```

The transport must start after **all** wiring is complete. The moment it
starts, its receive goroutines can call `HandleMessage`/`HandleSnapshot`,
which read `msgHandler.host` (registry, config, LogDB) and
`msgHandler.engine`. Starting the transport before those fields are set is a
data race between the receive goroutine reading them and `NewHost` writing
them. If `tr.Start()` fails, `NewHost` unwinds by stopping the engine and
then the transport.

### Membership Operations

All membership changes are proposed through Raft consensus:

Every membership call takes a **trailing `configChangeIndex uint64`**. Pass 0
to apply unconditionally, or the `ConfigChangeID` from a prior
`SyncGetShardMembership` to make the change conditional on the configuration
not having moved underneath you (see below).

```go
// Add a voting replica
rs, _ := host.RequestAddNode(ctx, shardID, replicaID, "10.0.0.4:63001", 0)
result, _ := rs.Result()

// Remove a replica
rs, _ := host.RequestRemoveNode(ctx, shardID, replicaID, 0)

// Add a non-voting observer
rs, _ := host.RequestAddObserver(ctx, shardID, replicaID, "10.0.0.5:63001", 0)

// Add a voting witness (no state machine)
rs, _ := host.RequestAddWitness(ctx, shardID, replicaID, "10.0.0.6:63001", 0)

// Transfer leadership
host.RequestLeaderTransfer(ctx, shardID, targetReplicaID)
```

Each also has an identity-carrying variant that commits the replica's stable
cryptographic node identity alongside its address --
`RequestAddNodeWithIdentity`, `RequestAddObserverWithIdentity`,
`RequestAddWitnessWithIdentity` -- taking `(ctx, shardID, replicaID, addr,
identity string, configChangeIndex uint64)`. Obtain the identity from the target
host's `Host.NodeIdentity()`.

Only the node form has a synchronous identity variant
(`SyncRequestAddNodeWithIdentity`). For observers and witnesses, use the async
form and wait on the returned `*RequestState`.

Note the asymmetric naming: the async remove is `RequestRemoveNode`, while
the synchronous form is `SyncRequestDeleteNode`. There is no
`RequestDeleteNode` and no `RequestAddNonVoting` (`AddNonVoting` is a
`proto` config-change constant; the Host method is `RequestAddObserver`).

> **Stable addresses for non-voting members.** Observers and witnesses do not
> run election timers, so — unlike a full voting replica, which re-advertises
> its address by campaigning after a restart — a non-voting member that
> restarts at a *new* address cannot teach the leader its new location until a
> committed config change re-registers it. Always give observers and witnesses
> a **stable advertised `RaftAddress`** (not an ephemeral port). A voting
> replica self-heals an address change on its next election timeout; a
> non-voting member relies on its address being stable or on an operator
> re-issuing the config change. (A periodic self-advertisement for non-voting
> members is a planned post-v1.0 enhancement.)

`OrderedConfigChange` serializes membership changes so only one is in-flight at a time, as required by the Raft specification. It is a `*bool` whose nil zero value means enabled, so the default needs no configuration; disabling it is `OrderedConfigChange: config.BoolPtr(false)` and is UNSAFE.

### Membership Queries

Source: `pkg/host.go`

Two methods provide membership views with different consistency guarantees:

**`SyncGetShardMembership(ctx, shardID)`** -- Linearizable membership read. Performs a ReadIndex to confirm leadership via heartbeat quorum, waits for the local state machine to catch up, then returns the committed membership. All config changes committed before this call are guaranteed to be reflected in the result. Use this when proposing membership changes to get the current `ConfigChangeID`.

**`GetShardMembership(shardID)`** -- Local in-memory membership read. Returns immediately from the Raft peer's cached membership. May be stale if a config change has been committed but not yet observed by this host. Use for monitoring and status display where strict consistency is not required.

```go
// Linearizable (for safe membership mutations)
membership, err := host.SyncGetShardMembership(ctx, shardID)

// Pass membership.ConfigChangeID as the trailing configChangeIndex argument
// to detect stale membership and serialize concurrent config changes.
rs, err := host.RequestAddNode(ctx, shardID, replicaID, addr, membership.ConfigChangeID)

// Local (for monitoring, dashboards)
membership, err := host.GetShardMembership(shardID)
```

Both methods return a `*Membership`. All maps are deep-copied to prevent
aliasing with internal state. Its fields:

| Field | Type | Meaning |
|-------|------|---------|
| `ConfigChangeID` | `uint64` | Log index of the config change that produced this membership |
| `Nodes` | `map[uint64]string` | Full voting members |
| `Observers` | `map[uint64]string` | Non-voting observers |
| `Witnesses` | `map[uint64]string` | Witnesses (vote, no state machine) |
| `Removed` | `map[uint64]struct{}` | Replicas removed from the cluster |
| `MaxWitnesses` | `uint64` | Replicated witness cap (0 = none) |
| `RequireFullReplicaMajority` | `bool` | Replicated full-replica-majority policy |
| `SessionExpiryEntries` | `uint64` | Replicated session expiry threshold |
| `MaxTotalSessions` | `uint64` | Replicated cap on registered sessions |
| `MaxSessionsPerClient` | `uint64` | Replicated cap on cached responses per session |
| `MaxSessionsPerOwner` | `uint64` | Replicated cap on concurrent sessions per `OwnerID` |
| `Identities` | `map[uint64]string` | Committed node identity per replica (sparse) |

`ConfigChangeIndex()` is a method returning the `ConfigChangeID` field; the
field and the accessor differ by one word, which is easy to mistype.

The public `Membership` does **not** expose `RemovedIdentities`. That map exists
in the wire and persisted `proto.Membership` and drives the removal deny-list,
but the host-level view does not surface it.

The witness and session policy fields are exposed so an operator can read
what a shard actually **committed**, rather than what any one node happens to
be configured with. The two differ exactly when someone edited a node's
`Config` after bootstrap — in which case the shard's value governs, and the
node logs the disagreement at startup.

A shard bootstrapped before the session policy was replicated reports all
three session fields as zero: it committed no policy, and every replica is
still running on its own configuration. There is currently no config change
that alters the session policy after bootstrap; the witness policy is changed
via `Host.RequestUpdateWitnessPolicy` / `SyncRequestUpdateWitnessPolicy`,
which take `(ctx, shardID, policy config.WitnessConfig, configChangeIndex uint64)`.

### Node Removal

Source: `pkg/host.go`, `pkg/logdb/waldb/db.go`

When a replica is removed from a shard, the following cleanup sequence runs:

1. `RequestRemoveNode(ctx, shardID, replicaID, configChangeIndex)` proposes a `RemoveNode` config change through Raft
2. Once committed and applied, the membership no longer includes the removed replica
3. On the removed node, call `LogDB.RemoveNodeData(shardID, replicaID)` to clean up persistent state:
   - Writes a durable `payloadTypeRemoveNode` record to the WAL (survives crash-recovery)
   - Clears all in-memory state: index, entries, hard state, bootstrap, snapshots, compaction markers
   - Triggers segment garbage collection: collects the set of segments still referenced by remaining live nodes and deletes orphaned segment files
   - Fsyncs the parent directory to make segment deletions durable

The durable WAL record ensures that if the process crashes after `RemoveNodeData` but before all segment files are deleted, WAL replay on restart will re-apply the removal and re-trigger garbage collection. Data for other nodes sharing the same WAL shard is unaffected.

## Tuning Guide

### RTT and Election Timeouts

The real election timeout is `ElectionRTT * RTTMillisecond`. The real heartbeat interval is `HeartbeatRTT * RTTMillisecond`.

| Environment | RTTMillisecond | ElectionRTT | Heartbeat interval | Election timeout |
|-------------|---------------|-------------|-------------------|-----------------|
| LAN | 1-5 | 10 | 1-5ms | 10-50ms |
| WAN (same region) | 50-100 | 10 | 50-100ms | 500ms-1s |
| WAN (cross-region) | 200 | 10 | 200ms | 2s |

Increase `ElectionRTT` if experiencing spurious elections due to GC pauses or disk I/O stalls. The minimum is 5; the recommended value is 10 (default).

### Batch Sizes and Throughput

**`SendBatchMaxSize` (64KB default):** When the send buffer reaches this size, it is flushed immediately.

**`MaxApplyEntrySize` (16,711,672 bytes, ~15.9 MiB, default):** Controls replication chunk size. Each Replicate message carries at most this many bytes of entries. Also used as the maximum batch size for `SM.Update()` calls. The default is `config.MaxReplicationPayloadBytes` — `transport.MaxFrameSize` minus frame and batch framing overhead — not a round number, because a batch the transport cannot frame is dropped at marshal time and the follower's next index never advances, so raft rebuilds the identical oversized batch forever.

**`CommitCBufferSize` (64 default):** Buffer depth between step and commit workers. Increase for write-heavy workloads where commit latency is consistently higher than step latency.

### Memory Management

**`MaxInMemLogSize` (per-shard):** When the in-memory log exceeds this limit, the per-shard rate limiter activates with hysteresis (100% to enter, 70% to exit). The rate limiter rejects `Propose()` with a `BusyError`. Set to 0 (default) to disable.

**`MaxTotalInMemLogSize` (global):** When total in-memory log size across all shards exceeds this threshold, the global rate limiter rejects `Propose()` with `ErrSystemBusy`. Set to 0 (default) to disable.

**Apply failures are not tunable.** There is no apply-retry setting: the apply path is unconditionally fail-fast, so the first `SM.Apply()` error fails the shard, with no retry and no consecutive-failure threshold. A `MaxApplyRetries` field existed through 0.5.1-alpha and never had any effect; it was removed. See [Lifecycle](engineering.md#error-handling-and-shard-failure).

### Compression

Enable Snappy compression for reduced bandwidth at minimal CPU cost:

```go
shardCfg := config.Config{
    EntryCompressionType:    config.Snappy,
    SnapshotCompressionType: config.Snappy,
}
```

The transport layer also compresses message batches with Snappy. Compression is applied before encryption when a barrier is active (compress-then-encrypt).

### Snapshot Tuning

**`SnapshotEntries` (100000 default):** Applied entries between auto-snapshots.
Must be greater than `CompactionOverhead`. A typical production value is
100000-1000000 depending on entry size.

**Zero does not disable it.** An unset field is read as "I did not choose" and
gets the 100000 default, because never compacting the log is not a default any
Raft deployment survives — and in this implementation it additionally stops
snapshot directories being reclaimed, old epoch keys being purged, and the
at-rest barrier ever being invoked. Assign `config.SnapshotsDisabled` to take
responsibility for calling `Host.RequestSnapshot` yourself.

**`CompactionOverhead` (5000 default):** Entries retained after log compaction for slow follower catch-up. Increase if followers frequently need snapshot transfers due to truncated logs.

**`MaxConcurrentSnapshotReceives` (4 default):** Limit concurrent inbound snapshot streams to prevent memory exhaustion.

**`MaxSnapshotReceiveRate` (256 MB/s default):** Per-connection bandwidth limit for inbound snapshots. Prevents snapshot transfers from saturating the network. The receiver enforces this by sleeping proportional to each chunk's size. A non-zero value below `MinMaxSnapshotReceiveRate` (64 KB/s) is rejected by config validation, because an absurdly low rate would compute a multi-day per-chunk sleep that holds the snapshot concurrency semaphore and memory budget for the entire transfer, stalling availability. Set to 0 to use the default. As a defense-in-depth backstop, each individual per-chunk sleep is also capped at 5 s regardless of the configured rate; the configured rate is still honored on average across chunks.

**`MaxSnapshotReceiveMemory` (1 GB default):** Total memory budget for all in-flight snapshot receives.

### Worker Counts

**`NumWorkers` (auto = NumCPU):** Number of step, commit, and apply workers. Shards are partitioned across workers by `shardID % NumWorkers`. Snapshot workers use `NumWorkers / 2` (minimum 2).

**`MaxApplyWorkers` (auto = NumCPU):** Semaphore limit for concurrent per-shard apply goroutines. The apply phase is typically the bottleneck (SM.Update), so this bounds CPU usage.

For hosts managing thousands of shards, the default values scale well. Reduce `NumWorkers` if CPU is constrained; increase if profiling shows step or commit workers as bottlenecks.

### Quiescence

Enable `Quiesce: true` for shards with intermittent traffic. Quiesced shards stop ticking (no heartbeats, no elections) until new client activity arrives. This reduces CPU usage when managing many idle shards. Quiescence exits automatically when a proposal is forwarded to the leader.

### Zero-Copy Mode

Set `ZeroCopyEntryCmd: true` to avoid copying `Entry.Cmd` during unmarshal. The state machine receives a slice aliased into the receive buffer. The SM must copy `Cmd` if retaining it beyond the `Update()` call. This reduces allocations on the apply path for state machines that process-and-discard.

### LeaseRead

Enable `LeaseRead: true` on a shard config to use lease-based reads (PhD 6.4 optimization). Lease reads skip the heartbeat round-trip required by normal linearizable reads, reducing read latency to zero (serving from local committed state). Requires CheckQuorum, which is on by default — `CheckQuorum` is a `*bool` whose nil zero value means enabled, so leave it unset rather than writing `CheckQuorum: true` (that does not compile; the compiling form is `config.BoolPtr(true)`). `Config.Validate` returns `*LeaseReadRequiresCheckQuorumError` when LeaseRead is set with CheckQuorum explicitly disabled, and LeaseRead also requires `ElectionRTT >= 5`. Not compatible with witness replicas.

## Build and CI

Source: `Makefile`, `.github/workflows/`

The Makefile is the single source of truth; the workflows call the same targets.
There are two gates, and the difference between them matters:

```
make ci       = fmt → no-mutants → vet → lint → security → build → test
make ci-full  = ci → ci-modules → ci-errfs → ci-fuzz-seeds
                   → ci-fuzz-mutation → ci-integration → ci-netfault
```

**`make ci` proves less than it looks like it proves.** Its `test` step is
`go test ./...` with no build tags, and the Go toolchain does not warn when a
build constraint excludes a file — it just compiles a smaller program. That
quietly skipped the integration tests, the errfs fault-injection tests and every
fuzz target, and computed the coverage threshold over what was left. `ci-full`
is what `.github/workflows/ci.yml` runs and what a release must pass.

`make no-mutants` sits inside `ci` for a related reason: a mutation-testing edit
(`if false` around a durability fsync) once reached a commit with every suite
green, because an unrestored mutation makes tests pass rather than fail.

### Running locally

```bash
make ci          # the cheap gate
make ci-full     # everything, including integration and fuzz
make install-tools
```

`make lint` runs the golangci-lint that `make install-lint` builds into
`.tools/bin`, not the first one on `PATH`. That is deliberate: a version pin
constrains which analyser, not which Go compiled it, and an analyser built by
an older Go refuses this module outright once `go.mod` moves forward. `make
install-lint` builds the pinned version with the toolchain that is about to
lint with it, and `make check-lint-toolchain` fails with the command to run if
the binary it finds cannot analyse the module.

Heavyweight targets under `-race` stand up multi-node QUIC clusters and will
flake on a workstation that is also running editors and browsers — the 30-second
catch-up waits in the snapshot tests are the usual casualty, and the failures
look like Raft defects. Offload them:

```bash
make remote-ci-full      # run the full gate on the designated spare host
make remote-<target>     # any target, run there instead of here
```

### Security scanners

`make security` runs `gosec`, `govulncheck`, `go mod verify` and `trivy fs`.

| Target | Tool | Scope |
|---|---|---|
| `make gosec` | gosec | Go source security analysis. Deliberately scans more than the CI job does |
| `make vuln` | govulncheck | Stdlib and dependency vulnerabilities |
| `make modverify` | `go mod verify` | Module integrity against `go.sum`, before any scanner reads the cache |
| `make trivy` | Trivy | Filesystem scan for CRITICAL/HIGH |
| `make nancy` | Nancy | Dependency vulnerability cross-check |
| `make trivy-image` | Trivy | Container image scan. **Not** part of `security` — run it separately |
| `make security-sarif` | gosec | SARIF output for CI upload |
| `make lint` | golangci-lint | Linter suite (5m timeout) |

gosec exclusions are documented in the Makefile header. All TLS connections use
verified mTLS.

### Docker CI Image

The CI Dockerfile (`.devcontainer/Dockerfile.dev`) pre-installs golangci-lint
v2.13.2 (pinned), plus gosec, govulncheck, nancy and Trivy at `@latest`.

## FIPS 140 Build

Source: `pkg/seal/kdf.go`, `pkg/seal/strategy_software.go`

QuicRaft supports FIPS 140 compliant builds using Go's `GOFIPS140` mechanism,
and `-tags fips` additionally removes the non-approved algorithms from the
binary rather than declining them at runtime. See
[Encryption > FIPS builds](../seal-barrier/engineering.md#fips-runtime-mode-vs-build-tag)
for what the tag removes.

**Read the boundary before you claim compliance for a deployment.** `-tags fips`
covers the data at rest — the seal layer, the WAL, snapshots — and, as of
`quic-go` v0.60.0, the wire as well: packet protection now runs through Go's
approved TLS 1.3 AES-GCM, so `pkg/internal/transport` passes
`GODEBUG=fips140=only`. The one tree that does not is
`pkg/internal/transport/revocation`, where OCSP's `CertID` is SHA-1 by protocol
definition (RFC 6960 §4.1.1, inside `x/crypto/ocsp`). It is gated one notch
lower instead, by `make verify-fips-exempt` at `GODEBUG=fips140=on`, and the
exemption is recorded with its citation in
[quic-transport/architecture.md](../quic-transport/architecture.md#fips-140-3-exemption-record).
Configure revocation `Mode: "crl"` and that path is never reached, which makes
an approved-only deployment achievable with no exemptions at all.

### Building

```bash
# Standard build (Argon2id KDF):
make build

# FIPS compile check, with and without the tag (PBKDF2-SHA256 KDF):
make build-fips

# Or directly:
GOFIPS140=v1.0.0 CGO_ENABLED=0 go build ./...
GOFIPS140=v1.0.0 CGO_ENABLED=0 go build -tags fips ./...
```

`make build-fips` only *compiles*: this module is a library with no `main`
package, so it produces no artifact, and `GOFIPS140` by itself never executes a
cryptographic operation. The check that can actually fail on a non-approved
primitive is:

```bash
# Every package inside the FIPS boundary, under approved-only enforcement:
make verify-fips

# The one exempt tree (pkg/internal/transport/revocation/...), module loaded:
make verify-fips-exempt
```

### KDF Selection

The barrier encryption KDF is selected at compile time:

| Build Mode | KDF | Parameters |
|---|---|---|
| Standard | Argon2id | time=4, memory=128MB, threads=4, keyLen=32 |
| FIPS (`GOFIPS140`) | PBKDF2-SHA256 | iterations=600000, keyLen=32, SHA-256 |

A version byte is stored in the sealed metadata so that data sealed with one KDF can be detected during unseal. Cross-KDF unseal uses default parameters for the stored version. Data sealed with custom (non-default) Argon2id parameters requires an identically-configured strategy for unseal.

## Cross-References

- [Lifecycle](engineering.md) -- startup, shard init, crash recovery, shutdown, quorum-loss runbook
- [Compatibility and Upgrades](../compatibility.md) -- format versions, upgrade paths, node identities
- [Architecture](../multi-group-host/architecture.md) -- system overview, data flow, package structure
- [WAL Persistence](../log-storage/engineering.md) -- WAL format, segments, compaction
- [Engine Pipeline](../raft-consensus/engineering.md) -- step/commit/apply workers
- [Transport](../quic-transport/engineering.md) -- QUIC transport, connection management
- [Encryption](../seal-barrier/architecture.md) -- barrier internals, key derivation, ciphertext format
- [Key custody](../seal-barrier/user.md) -- sealing strategies and where custody lives
- [Sessions](../client-sessions/engineering.md) -- at-most-once delivery, session tables
- [Raft protocol](../raft-consensus/engineering.md) -- elections, replication, snapshot control
- [API reference](../multi-group-host/api.md) -- full exported surface
