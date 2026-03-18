# Operations

This document covers deployment, monitoring, configuration tuning, and day-to-day administration of QuicRaft clusters.

**Sources:** `pkg/host.go`, `pkg/config/`, `pkg/metrics.go`, `pkg/metrics_prometheus.go`, `pkg/logdb/waldb/diskmon.go`, `pkg/seal/barrier.go`, `pkg/bootstrap/`, `pkg/internal/server/rate.go`

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

### Multi-Node (Static Peers)

For clusters with known members at deploy time, use static discovery. Each node specifies the full member map programmatically (see Programmatic Bootstrap below).

### Multi-Node (Dynamic Discovery)

When member addresses are not known in advance, use multicast or DNS SRV discovery. The bootstrapper handles peer discovery, quorum validation, and shard startup.

**Multicast** (LAN only, HMAC-SHA256 authenticated): Use `discovery.NewMulticastDiscovery` with a shared secret. Non-static discovery auto-generates `replica-id`, selects free ports, and namespaces data directories per replica to allow multiple nodes on the same host.

**DNS SRV** (WAN, requires `_raft._udp.<domain>` records): Use `discovery.NewDNSDiscovery` with the SRV domain.

### Programmatic Bootstrap

```go
disc := discovery.NewStaticDiscovery(discovery.StaticConfig{
    Peers: []discovery.Peer{
        {NodeID: 1, Address: "10.0.0.1:63001"},
        {NodeID: 2, Address: "10.0.0.2:63001"},
        {NodeID: 3, Address: "10.0.0.3:63001"},
    },
})

bs := bootstrap.NewBootstrapper(bootstrap.Config{
    NodeID:      1,
    Address:     "10.0.0.1:63001",
    ShardID:     100,
    ReplicaID:   1,
    Discovery:   disc,
    CreateFn:    sm.NewCreateFunc(newKV),
    ShardConfig: shardCfg,
    MinPeers:    3,  // default
}, host)

err := bs.Bootstrap()
```

The bootstrapper sequence: discover peers, add self, validate `len(peers) >= QuorumSize(MinPeers)`, build member map, call `Host.StartShard`.

### Directory Layout

```
/var/lib/quicraft/
  wal/                     # WAL segments (write-intensive, use fast disk)
    shard-0/               # 16 WAL shards by shardID hash
    shard-1/
    ...
  nodehost/                # Metadata, snapshots, host ID
    host.id                # 128-bit hex host identifier (0600 perms)
    snapshots/
      shard-100/
        snapshot-00000000001000/
          snapshot.dat
```

**Production recommendation:** Place `WALDir` and `NodeHostDir` on separate physical disks to isolate WAL write I/O from snapshot reads.

## Configuration Reference

### HostConfig (per-host)

Source: `pkg/config/hostconfig.go`

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `WALDir` | string | (required) | Write-ahead log directory |
| `NodeHostDir` | string | (required) | Metadata, snapshots, host ID directory |
| `ListenAddress` | string | (required) | Local QUIC bind address |
| `RaftAddress` | string | `""` | Address seen by peers (may differ behind NAT) |
| `RTTMillisecond` | uint64 | `200` | Estimated round-trip time (ms). LAN: 1-5, WAN: 100-300 |
| `DeploymentID` | uint64 | (required) | Cluster isolation token. Prevents cross-cluster traffic |
| `NumWorkers` | uint64 | `runtime.NumCPU()` | Workers per tier (step, commit, apply) |
| `MaxApplyWorkers` | int | `runtime.NumCPU()` | Max concurrent per-shard apply goroutines |
| `CommitCBufferSize` | int | `64` | Commit channel buffer depth |
| `ShutdownTimeout` | Duration | `30s` | Max wait for graceful drain on Close() |
| `MaxWALDiskSize` | uint64 | `0` (unlimited) | WAL directory size limit in bytes |
| `MaxApplyRetries` | uint64 | `100` | SM.Apply circuit breaker threshold |
| `MaxTotalInMemLogSize` | uint64 | `0` (disabled) | Global rate limiter threshold (bytes) |
| `AllowZeroDeploymentID` | bool | `false` | Allow unset DeploymentID (unsafe in production) |
| `NotifyCommit` | bool | `false` | Fire OnCommit callbacks |
| `HostID` | string | (auto-generated) | Unique host identifier (empty = generate on first boot) |
| `EventListener` | *EventListener | `nil` | Callback handlers |
| `TransportConfig` | TransportConfig | (required for multi-node) | QUIC transport configuration |
| `KeyRotationInterval` | Duration | `0` (disabled) | Auto barrier epoch rotation interval. Recommended: 24h |
| `MaxRetainedEpochs` | uint64 | `0` (retain all) | Barrier epochs to retain after compaction |
| `LogConfig` | LogConfig | text/info | Format: text/json, Level: debug/info/warn/error |

### Config (per-shard)

Source: `pkg/config/config.go`

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `ShardID` | uint64 | (set by StartShard) | Shard identifier (caller-provided) |
| `ReplicaID` | uint64 | (set by StartShard) | Replica identifier (caller-provided) |
| `IsObserver` | bool | `false` | Non-voting replica (no quorum contribution) |
| `IsWitness` | bool | `false` | Non-storage replica (participates in quorum only) |
| `CheckQuorum` | bool | `true` | Leader steps down on quorum loss (safety-critical) |
| `PreVote` | bool | `true` | Pre-vote prevents disruptive elections (safety-critical) |
| `ElectionRTT` | uint64 | `10` | Election timeout in RTT multiples (must be >= 5) |
| `HeartbeatRTT` | uint64 | `1` | Heartbeat interval in RTT multiples |
| `SnapshotEntries` | uint64 | `0` (disabled) | Entries between auto-snapshots |
| `CompactionOverhead` | uint64 | `5000` | Entries retained after compaction |
| `MaxInMemLogSize` | uint64 | `0` (disabled) | Per-shard in-memory log rate limit (bytes) |
| `MaxProposalPayloadSize` | uint64 | `8 MB` | Max individual proposal size |
| `MaxEntrySize` | uint64 | `8 MB` | Max entry size in log |
| `MaxApplyEntrySize` | uint64 | `64 MB` | Max batch size per SM.Update call (replication chunk size) |
| `MaxSnapshotSize` | uint64 | `4 GB` | Max snapshot file size |
| `MaxSessionsPerClient` | uint64 | `128` | Per-client session limit |
| `MaxTotalSessions` | uint64 | `16384` | Per-shard total session limit |
| `SnapshotCompressionType` | CompressionType | `NoCompression` | Snapshot compression: `NoCompression` or `Snappy` |
| `EntryCompressionType` | CompressionType | `NoCompression` | Entry compression: `NoCompression` or `Snappy` |
| `Quiesce` | bool | `false` | Quiescence for idle shards |
| `OrderedConfigChange` | bool | `true` | Serialize membership changes (required for safety) |
| `ZeroCopyEntryCmd` | bool | `false` | Zero-copy entry data (SM must copy if retained) |
| `SessionExpiryEntries` | uint64 | `0` (disabled) | Expire sessions inactive for this many log entries |
| `LeaseRead` | bool | `false` | Lease-based reads (no heartbeat RTT, requires CheckQuorum) |

### TransportConfig

Source: `pkg/config/transportconfig.go`

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `MTLSConfig` | *MTLSConfig | (required) | Mutual TLS certificates and key |
| `RevocationConfig` | *RevocationConfig | `nil` | Optional CRL/OCSP certificate revocation checks |
| `StreamPoolSize` | int | `16` | QUIC streams per connection |
| `MaxStreamPoolSize` | int | `256` | Upper bound for stream pool auto-scaling |
| `MaxSnapshotReceiveRate` | int64 | `256 MB/s` | Inbound snapshot bandwidth per connection |
| `MaxConcurrentSnapshotReceives` | int | `4` | Concurrent inbound snapshot streams |
| `MaxSnapshotReceiveMemory` | int64 | `1 GB` | Total snapshot receive buffer |
| `MaxDecompressedSize` | uint32 | `16 MB` | Max Snappy decompressed frame size |
| `MaxConnectionsPerIP` | int | `16` | Per-source-IP connection limit |
| `MaxIncomingConnections` | int | `256` | Total inbound connection limit |
| `SendBatchMaxSize` | int | `64 KB` | Max send buffer size before flush |
| `UDPRecvBufSize` | int | `7 MB` | UDP receive socket buffer size |
| `UDPSendBufSize` | int | `7 MB` | UDP send socket buffer size |
| `Enable0RTT` | bool | `false` | QUIC 0-RTT (replayable; trusted networks only) |
| `DisableCompression` | bool | `false` | Disable message batch compression |

### MTLSConfig

Source: `pkg/config/transportconfig.go`

```go
type MTLSConfig struct {
    CACert []byte  // PEM-encoded CA certificate
    Cert   []byte  // PEM-encoded node certificate
    Key    []byte  // PEM-encoded private key
}
```

All three fields are required for multi-node deployments. QuicRaft enforces mutual TLS with certificate verification on both client and server sides. See [Encryption](../security/encryption.md) for key management workflows.

### RevocationConfig (optional)

```go
type RevocationConfig struct {
    CRLPaths           []string      // Paths to CRL files
    OCSPResponderURL   string        // OCSP responder endpoint
    OCSPCacheSeconds   int           // Default: 300
    OCSPTimeoutSeconds int           // Default: 5
    CheckInterval      time.Duration // Default: 60s
    Mode               string        // "crl"/"ocsp"/"both"/"any"
    EnforceRevocation  bool          // Reject if revocation status unknown
    OnCertRevoked      func(...)     // Callback when cert is revoked
}
```

Enables certificate revocation checking via CRL and/or OCSP. Optional; used only if present.

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

**Raft metrics** (`quicraft_raft_*`):

| Metric | Type | Labels | Description |
|--------|------|--------|-------------|
| `proposal_latency_seconds` | Histogram | `shard_id` | End-to-end proposal latency |
| `commit_latency_seconds` | Histogram | `shard_id` | Proposal-to-commit latency |
| `apply_latency_seconds` | Histogram | `shard_id` | Commit-to-apply latency |
| `proposals_dropped_total` | Counter | `shard_id` | Dropped proposals |
| `proposal_batch_size` | Histogram | `shard_id` | Proposals per Raft append |
| `commit_batch_size` | Histogram | -- | Entries per commit batch |
| `elections_total` | Counter | `shard_id` | Election count |
| `snapshots_total` | Counter | `shard_id` | Snapshot count |
| `readindex_latency_seconds` | Histogram | `shard_id` | ReadIndex latency |

**LogDB metrics** (`quicraft_logdb_*`):

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
| `messages_sent_total` | Counter | `shard_id`, `to`, `msg_type` | Messages sent |
| `messages_received_total` | Counter | `shard_id`, `from`, `msg_type` | Messages received |
| `snapshots_sent_total` | Counter | `shard_id` | Snapshots sent |
| `snapshots_sent_bytes` | Histogram | `shard_id` | Bytes per snapshot send |
| `snapshots_received_total` | Counter | `shard_id` | Snapshots received |
| `snapshots_received_bytes` | Histogram | `shard_id` | Bytes per snapshot receive |

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

Per-shard health values: `healthy`, `no-leader`, `lagging`, `unloaded`, `error`.

### EventListener Callbacks

Source: `pkg/config/hostconfig.go`

Optional per-type callbacks invoked synchronously on engine workers. Callbacks must not block. Configure via `HostConfig.EventListener`:

```go
cfg.EventListener = &config.EventListener{
    // Raft lifecycle
    OnLeaderUpdated:      func(info config.LeaderInfo) { /* ... */ },
    OnLeaderLost:         func(info config.EventShardInfo) { /* ... */ },
    OnCampaignLaunched:   func(info config.EventShardInfo) { /* ... */ },
    OnCampaignSkipped:    func(info config.EventShardInfo) { /* ... */ },

    // Snapshot operations
    OnSnapshotCreated:    func(info config.SnapshotInfo) { /* ... */ },
    OnSnapshotSent:       func(info config.SnapshotInfo) { /* ... */ },
    OnSnapshotReceived:   func(info config.SnapshotInfo) { /* ... */ },
    OnSnapshotRecovered:  func(info config.SnapshotInfo) { /* ... */ },
    OnSnapshotRejected:   func(info config.SnapshotInfo) { /* ... */ },

    // System events
    OnNodeHostShuttingDown: func() { /* ... */ },
    OnNodeReady:           func(info config.EventShardInfo) { /* ... */ },
    OnNodeUnloaded:        func(info config.EventShardInfo) { /* ... */ },
    OnNodeDeleted:         func(info config.EventShardInfo) { /* ... */ },
    OnMembershipChanged:   func(info config.MembershipChangedInfo) { /* ... */ },

    // Failures
    OnShardFailed:        func(info config.ShardFailedInfo) { /* ... */ },
    OnLogDBDiskWarning:   func(info config.DiskInfo) { /* ... */ },
    OnLogDBDiskFull:      func(info config.DiskInfo) { /* ... */ },

    // Proposal and read drops
    OnProposalDropped:    func(info config.ProposalDroppedInfo) { /* ... */ },
    OnReadIndexDropped:   func(info config.ReadIndexDroppedInfo) { /* ... */ },
}
```

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
rs, err := host.RequestSnapshot(ctx, shardID)
result, err := rs.Result()
```

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

**Session limits:** `MaxSessionsPerClient` (default 128) limits concurrent sessions per client ID. `MaxTotalSessions` (default 16384) limits the total number of sessions per shard. When either limit is reached, new session registrations are rejected with `ErrSessionLimitExceeded`.

**Automatic session expiry:** `SessionExpiryEntries` controls deterministic cleanup of abandoned sessions. Sessions that have not been active within this many applied log entries are expired during the apply path. This is driven by log index (not wall-clock time), so all replicas expire the same sessions at the same point in the log, preserving determinism.

```go
shardCfg := config.Config{
    SessionExpiryEntries: 100000, // expire sessions inactive for 100K entries
}
```

Set `SessionExpiryEntries` to 0 (default) to disable automatic expiry. When disabled, sessions are only removed via explicit unregister or when `MaxTotalSessions` forces rejection of new registrations.

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
strategy := seal.NewSoftwareStrategy([]byte("passphrase"))
barrier := seal.NewBarrier()

// First boot: generate root key, derive epoch-1 DEK, seal root key
barrier.Initialize(strategy)

// Pass to host
host, _ := quicraft.NewHost(cfg, quicraft.WithBarrier(barrier))
```

On subsequent boots, load the persisted `SealedRootKey` and unseal:

```go
barrier := seal.NewBarrier()
if err := barrier.SetSealedRootKey(loadedSealedKey); err != nil {
    log.Fatalf("invalid sealed key: %v", err)
}
barrier.Unseal(strategy)
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
barrier.PurgeEpochsBefore(minEpoch)
```

Epochs with in-flight snapshot transfers (registered via `RegisterInFlightEpoch`) are protected from purging. The current epoch is never purged.

### Emergency Seal

For security incident response, immediately seal the barrier to make all at-rest data inaccessible:

```go
host.EmergencySeal()
```

All in-flight operations fail with `ErrBarrierSealed`. Normal shutdown via `Host.Close()` seals the barrier automatically after draining the pipeline.

## Graceful Shutdown

### Drain Mode

Source: `pkg/host.go`

`Host.Drain()` transitions the host into drain mode:

1. Sets `draining` flag atomically via `CompareAndSwap` (idempotent -- second call returns immediately)
2. New `Propose()` and `ReadIndex()` calls return `ErrDraining`
3. Polls pending proposals, reads, and snapshots every 50ms
4. Returns when all in-flight work completes or `ShutdownTimeout` expires

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
     - Drain() polls pending proposals, reads, snapshots every 50ms
     - Bounded by ShutdownTimeout (default 30s)
     - If drain times out, logs a warning and proceeds

  3. Stop key rotation goroutine
     - close(keyRotStopC) signals, then <-keyRotDone waits
     - Prevents rotation during shutdown

  4. Stop QUIC transport
     - No new inbound messages arrive while workers drain
     - Closes all active QUIC connections

  5. Stop engine (tiered worker shutdown)
     - Per-tier WaitGroups ensure sequential close-and-wait:
       a. Step workers exit first (no more Raft ticks or proposal processing)
       b. Commit workers drain commitC, persist remaining entries, then exit
       c. Apply workers drain applyC, apply remaining entries to SM, then exit
       d. Snapshot workers cancel in-progress snapshots and exit

  6. Stop all shard nodes
     - Set stopped flag, reject pending proposals/reads/snapshots with ErrClosed
     - Clear all pending maps

  7. Close LogDB
     - Flush buffered writes, fsync all WAL segments, close file handles

  8. Seal barrier (if configured)
     - barrier.Seal() zeros root key and all DEKs from memory
     - Already-sealed is not an error during shutdown
```

**Why this order matters:**

- **Transport before engine:** Stops new inbound messages so engine workers see a quiescent pipeline.
- **Engine before nodes:** Engine workers produce completions that nodes consume. Stopping engine first ensures all committed entries are applied before node state is cleaned up.
- **Step before commit before apply:** Preserves the pipeline's natural flow direction. Each tier drains work from the previous tier.
- **LogDB before barrier:** LogDB reads and writes may need the barrier for decryption. Close LogDB before sealing the barrier.

### Membership Operations

All membership changes are proposed through Raft consensus:

```go
// Add a voting replica
rs, _ := host.RequestAddNode(ctx, shardID, replicaID, "10.0.0.4:63001")
result, _ := rs.Result()

// Remove a replica
rs, _ := host.RequestRemoveNode(ctx, shardID, replicaID)

// Add a non-voting observer
rs, _ := host.RequestAddObserver(ctx, shardID, replicaID, "10.0.0.5:63001")

// Add a voting witness (no state machine)
rs, _ := host.RequestAddWitness(ctx, shardID, replicaID, "10.0.0.6:63001")

// Transfer leadership
host.RequestLeaderTransfer(ctx, shardID, targetReplicaID)
```

`OrderedConfigChange: true` (default) serializes membership changes so only one is in-flight at a time, as required by the Raft specification.

### Membership Queries

Source: `pkg/host.go`

Two methods provide membership views with different consistency guarantees:

**`SyncGetShardMembership(ctx, shardID)`** -- Linearizable membership read. Performs a ReadIndex to confirm leadership via heartbeat quorum, waits for the local state machine to catch up, then returns the committed membership. All config changes committed before this call are guaranteed to be reflected in the result. Use this when proposing membership changes to get the current `ConfigChangeID`.

**`GetShardMembership(shardID)`** -- Local in-memory membership read. Returns immediately from the Raft peer's cached membership. May be stale if a config change has been committed but not yet observed by this host. Use for monitoring and status display where strict consistency is not required.

```go
// Linearizable (for safe membership mutations)
membership, err := host.SyncGetShardMembership(ctx, shardID)
// membership.ConfigChangeID should be passed to RequestAddNode/RequestRemoveNode
// to detect stale membership and serialize concurrent config changes.

// Local (for monitoring, dashboards)
membership, err := host.GetShardMembership(shardID)
```

Both methods return a `*Membership` containing `Nodes`, `Observers`, `Witnesses`, `Removed`, and `ConfigChangeID`. All maps are deep-copied to prevent aliasing with internal state.

### Node Removal

Source: `pkg/host.go`, `pkg/logdb/waldb/db.go`

When a replica is removed from a shard, the following cleanup sequence runs:

1. `RequestRemoveNode(ctx, shardID, replicaID)` proposes a `RemoveNode` config change through Raft
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

**`MaxApplyEntrySize` (64MB default):** Controls replication chunk size. Each Replicate message carries at most this many bytes of entries. Also used as the maximum batch size for `SM.Update()` calls.

**`CommitCBufferSize` (64 default):** Buffer depth between step and commit workers. Increase for write-heavy workloads where commit latency is consistently higher than step latency.

### Memory Management

**`MaxInMemLogSize` (per-shard):** When the in-memory log exceeds this limit, the per-shard rate limiter activates with hysteresis (100% to enter, 70% to exit). The rate limiter rejects `Propose()` with a `BusyError`. Set to 0 (default) to disable.

**`MaxTotalInMemLogSize` (global):** When total in-memory log size across all shards exceeds this threshold, the global rate limiter rejects `Propose()` with `ErrSystemBusy`. Set to 0 (default) to disable.

**`MaxApplyRetries`:** Circuit breaker threshold for `SM.Apply()` failures. If a shard's state machine consistently fails to apply entries, after this many consecutive failures, the shard transitions to error state and is no longer available. Default: 100.

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

**`SnapshotEntries`:** Number of applied entries between auto-snapshots. Set to 0 to rely on manual snapshots only. When non-zero, must be greater than `CompactionOverhead`. A typical production value is 100000-1000000 depending on entry size.

**`CompactionOverhead` (5000 default):** Entries retained after log compaction for slow follower catch-up. Increase if followers frequently need snapshot transfers due to truncated logs.

**`MaxConcurrentSnapshotReceives` (4 default):** Limit concurrent inbound snapshot streams to prevent memory exhaustion.

**`MaxSnapshotReceiveRate` (256 MB/s default):** Per-connection bandwidth limit for inbound snapshots. Prevents snapshot transfers from saturating the network.

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

Enable `LeaseRead: true` on a shard config to use lease-based reads (PhD 6.4 optimization). Lease reads skip the heartbeat round-trip required by normal linearizable reads, reducing read latency to zero (serving from local committed state). Requires `CheckQuorum: true`. Not compatible with witness replicas.

## Build and CI

Source: `Makefile`, `.github/workflows/ci.yml`

The Makefile `ci` target is the single source of truth for the CI pipeline. The GitHub Actions workflow mirrors this exact sequence:

```
fmt → vet → lint → gosec → vuln → trivy → build → test
```

### Running Locally

```bash
# Full pipeline in devcontainer (recommended):
make ci

# Full pipeline on host (requires tools installed):
make ci-local

# Install all CI tools:
make install-tools
```

### Security Scanners

| Tool | Target | Description |
|---|---|---|
| `gosec` | `make gosec` | Go source security analysis (SARIF output in CI) |
| `govulncheck` | `make vuln` | Stdlib and dependency vulnerability check |
| `trivy` | `make trivy` | Filesystem scan for CRITICAL/HIGH vulnerabilities |
| `trivy-image` | `make trivy-image` | Docker image scan |
| `golangci-lint` | `make lint` | Comprehensive Go linter suite (5m timeout) |

gosec exclusions are documented in the Makefile header. All TLS connections use verified mTLS.

### Docker CI Image

The CI Dockerfile (`.devcontainer/Dockerfile.dev`) pre-installs:
- golangci-lint v2.7.2
- gosec (latest)
- govulncheck (latest)
- Trivy (latest)

## FIPS 140 Build

Source: `pkg/seal/kdf.go`, `pkg/seal/strategy_software.go`

QuicRaft supports FIPS 140 compliant builds using Go's `GOFIPS140` mechanism.

### Building

```bash
# Standard build (Argon2id KDF):
make build

# FIPS build (PBKDF2-SHA256 KDF):
make build-fips

# Or directly:
GOFIPS140=v1.0.0 CGO_ENABLED=0 go build ./...
```

### KDF Selection

The barrier encryption KDF is selected at compile time:

| Build Mode | KDF | Parameters |
|---|---|---|
| Standard | Argon2id | time=4, memory=128MB, threads=4, keyLen=32 |
| FIPS (`GOFIPS140`) | PBKDF2-SHA256 | iterations=600000, keyLen=32, SHA-256 |

A version byte is stored in the sealed metadata so that data sealed with one KDF can be detected during unseal. Cross-KDF unseal uses default parameters for the stored version. Data sealed with custom (non-default) Argon2id parameters requires an identically-configured strategy for unseal.

## Cross-References

- [Architecture](../architecture/overview.md) -- system overview, data flow, package structure
- [WAL Persistence](../storage/wal.md) -- WAL format, segments, compaction
- [Engine Pipeline](../engine/pipeline.md) -- step/commit/apply workers
- [Transport](../transport/quic.md) -- QUIC transport, connection management
- [Encryption](../security/encryption.md) -- barrier internals, key derivation, ciphertext format
