# QuicRaft QUIC Transport

## Overview

QuicRaft uses QUIC exclusively for peer-to-peer communication. There is no
TCP fallback. QUIC was chosen because it provides:

- **Built-in TLS 1.3**: every connection is encrypted, no bolt-on TLS wrapper
- **Stream multiplexing**: thousands of Raft shards share a single UDP socket
  without head-of-line blocking between shards
- **0-RTT reconnection**: optional fast reconnect for trusted networks (disabled
  by default due to replay risk per RFC 9001)
- **Connection migration**: tolerates IP changes (container rescheduling)

The transport is implemented in `pkg/internal/transport/` as a concrete
`QUICTransport` type (no interface -- single implementation, hot-path
efficiency). The underlying QUIC library is quic-go.

Source files:

| File               | Responsibility                                 |
|--------------------|------------------------------------------------|
| `quic.go`          | QUICTransport struct, Start/Stop, Send/SendSnapshot |
| `conn.go`          | Connection pool, dial, singleflight coalescing |
| `recv.go`          | Accept loop, stream dispatch, message/snapshot handlers |
| `frame.go`         | Frame wire format (header marshal/unmarshal)   |
| `flags.go`         | FlagSnapshot constant                          |
| `tls.go`           | mTLS certificate builder (`buildMTLSTLS`, `RequireAndVerifyClientCert`, no insecure fallback) |
| `config.go`        | TransportConfig, defaults, validation          |
| `handler.go`       | MessageHandler interface                       |
| `snapshot_header.go`| SnapshotHeader wire format (48 bytes)         |
| `snapshot_recv.go` | Snapshot receiver with rate limiting           |
| `errors.go`        | Typed transport errors (DialError, FrameDecompressError, sentinels) |

## Connection Management

### Connection Pool

`QUICTransport` maintains a connection pool keyed by target address:

```go
type QUICTransport struct {
    connMu    sync.RWMutex
    conns     map[string]*quic.Conn   // target address -> QUIC connection
    dialGroup singleflight.Group      // coalesce concurrent dials
    // ...
}
```

Source: `pkg/internal/transport/conn.go`

**Lookup (fast path)**: `RLock`, map lookup, verify `conn.Context()` is
not done. If alive, return immediately.

**Dial (slow path)**: `singleflight.Group` ensures only one goroutine dials
a given target. Other callers block on the same flight and receive the
shared result.

```
getConnection(target)
    |-- RLock: check conns[target]
    |   |-- alive? return
    |   |-- dead? removeConnection(target)
    |-- dialGroup.Do(target, dialConnection)
    |   |-- net.ResolveUDPAddr
    |   |-- net.SplitHostPort(target) -> host (fallback "localhost" if empty)
    |   |-- clientTLS.ServerName = host
    |   |-- quicTr.Dial(ctx, addr, clientTLS, quicCfg)
    |   |-- Lock: conns[target] = conn
    |-- return conn
```

**Extraction of server name:** `dialConnection` uses `net.SplitHostPort(target)`
to extract the hostname, then sets `clientTLS.ServerName = host` for mTLS
verification. If `SplitHostPort` fails (malformed target), a `DialError` is
returned with diagnostic context. For targets without an explicit port, the
hostname is extracted via fallback parsing.

**Stale connection eviction:** A background goroutine (`connectionCleanup`)
scans connections every 30 seconds and removes those whose `Context()` is done
(closed by peer, idle timeout, or transport error).

**Rolling restart detection:** When receiving on a new inbound connection,
`evictStaleOutboundOnRecv` compares the QUIC connection pointer with the
cached outbound connection. If they differ, the peer has restarted; the stale
outbound is evicted immediately (instead of waiting 30 seconds for the cleanup
scan). This eliminates multi-second ReplicateResp delays during rolling restarts.

**Connection liveness:** Before returning a cached connection, `getStream`
fast-checks `conn.Context().Done()` to verify the connection is still alive.
If done, the connection is removed and a fresh dial is triggered.

### Stream Pool and Heartbeat Streams

Streams are pooled to reduce per-message overhead. The pool is sized with
`StreamPoolSize` (default 16) and auto-scales up to `MaxStreamPoolSize` (256).

Stream index assignment:

- **Heartbeats**: always use stream index 0 (persistent, low-latency priority path)
- **Data messages**: `idx = (shardID % (poolSize - 1)) + 1`

A per-target shard bucket (16 shards, power of two) is selected by
`idx & (streamShardCount - 1)` to reduce mutex contention.

**Persistent heartbeat streams:** Stream 0 is pre-opened and kept alive for
the lifetime of the connection. This eliminates the OpenStreamSync overhead
for heartbeats, enabling low-latency heartbeat delivery (critical for leader
election latency).

Stream acquisition uses double-checked locking:

1. `RLock` -- check pool for stream
2. If miss: get connection, `OpenStreamSync`, `Lock`, double-check, store

**Failed write handling:** On `Write` or `SetWriteDeadline` failure, the
entire connection (all streams and the connection itself) is evicted via
`evictStreamsForTarget()`. This ensures no stale streams remain after a
connection drops. The next send attempt dials a fresh connection and opens
new streams.

### QUIC Configuration

Source: `pkg/internal/transport/quic.go`

```go
quicCfg := &quic.Config{
    MaxIdleTimeout:             30 * time.Second,
    HandshakeIdleTimeout:       5 * time.Second,
    MaxIncomingStreams:         int64(maxIncomingStreams), // see below
    MaxIncomingUniStreams:      -1,         // disabled
    KeepAlivePeriod:            10 * time.Second,
    InitialStreamReceiveWindow: 2 << 20,    // 2 MB
    MaxStreamReceiveWindow:     8 << 20,    // 8 MB
    MaxConnectionReceiveWindow: 32 << 20,   // 32 MB
    Allow0RTT:                  cfg.Enable0RTT,
}
```

`MaxIncomingStreams` is not a fixed constant. It is computed in
`NewQUICTransport` as `MaxStreamsPerConnection + MaxConcurrentSnapshotRecv`,
with a floor of `StreamPoolSize + snapshotStreamHeadroom` (4), so the
advertised incoming-stream budget matches the receiver's combined data +
snapshot processing capacity (see [Stream Isolation](#stream-isolation-data-vs-snapshot)).

**UDP buffer tuning:** The transport exposes `UDPRecvBufSize` and `UDPSendBufSize`
configuration (default 7 MB each) to allow platforms with high throughput to
tune kernel socket buffers. These are applied to the listener socket.

## Send Path

Source: `pkg/internal/transport/quic.go`

```
Engine Step Worker
    |
    |  sender.Send(msgs []proto.Message)
    v
QUICTransport.Send()
    |-- for each msg: deep-copy entries (break alias with reusable proposeBuf)
    |-- addr = registry.Resolve(msg.ShardID, msg.To)
    |-- enqueueToTarget(addr, msg)               // non-blocking enqueue
    v
sendQueue (per target: data channel cap 2048 + heartbeat channel cap 64)
    |
    v  drained by background sendQueueWorker(target) goroutine
sendQueueWorker
    |-- read first message, then drain buffered messages into a batch
    |   (heartbeats drained before data; up to 16 hb / 256 data per batch)
    |-- sendBatch(target, msgs)
        |-- build MessageBatch{BinVer, DeploymentID, SourceAddress, Requests}
        |-- bufPtr = getBuf(size + FrameHeaderSize)  // sync.Pool
        |-- mb.MarshalTo(payloadBuf)                 // serialize into pooled buffer
        |-- optionally: compress with Snappy (unless DisableCompression)
        |-- MarshalFrame(buf, payload, flags)        // prepend frame header
        |-- stream = getStream(target, shardID, isHeartbeat)
        |-- stream.SetWriteDeadline(now + 2s)
        |-- stream.Write(buf[:frameSize])
        |-- putBuf(bufPtr)                           // return to pool
```

Key properties:

- **Non-blocking enqueue**: `Send` only deep-copies entries and enqueues onto
  the per-target `sendQueue` channel; it never dials, opens streams, or writes.
  This keeps the engine step worker from stalling on transport I/O.
- **Best-effort drop on full**: if a target's channel is full, the message is
  dropped (a backpressure event is logged) and Raft retransmits it on the next
  heartbeat/replicate cycle.
- **Per-target send queues**: each target address has its own buffered queue and
  a dedicated background `sendQueueWorker` that batches and delivers messages.
  Slow targets cannot block sends to fast targets.
- **Target batching**: a worker drains all buffered messages for its target into
  one `MessageBatch`/frame before writing.
- **Buffer pooling**: `sync.Pool` with initial capacity `SendBatchMaxSize + FrameHeaderSize`.
  Buffers exceeding a threshold are not returned to the pool.
- **Compression**: Snappy compression is applied by default. Disable via
  `config.TransportConfig.DisableCompression` for CPU-constrained environments.
- **Heartbeat priority**: heartbeats use stream index 0 (persistent pre-opened stream),
  separate from data streams, enabling low-latency leader heartbeats.
- **Write deadline**: 2-second deadline per stream write. On failure, the entire
  connection and all its streams are evicted; the next send dials fresh.

### Per-Target Send Queues and Idle Pruning

`Send` is non-blocking because each target address has a dedicated buffered
`sendQueue` (data channel capacity 2048, plus a high-priority heartbeat channel)
drained by a background `sendQueueWorker`. To avoid leaking goroutines and memory
as cluster membership changes, a worker that sees no traffic for
`sendQueueIdleTimeout` (60 s) removes its queue from the `sendQueues` map and
exits; the next `Send` to that target lazily recreates the queue.

That idle-prune introduces a race: a producer may have already obtained the
`*sendQueue` from `getOrCreateSendQueue` and enqueued into a channel whose worker
has just pruned the map entry and returned, leaving the message in an orphaned
channel with no drainer. To close this without silently losing the message, each
`sendQueue` carries a `closed atomic.Bool`:

- The worker sets `closed = true` and deletes the map entry **under
  `sendQueuesMu`**, immediately before exiting on idle timeout.
- After a successful channel send, the enqueue path (`enqueueToTarget`) checks
  `sq.closed`. If it observes `true`, the message was orphaned, so it re-acquires
  a fresh queue via `getOrCreateSendQueue` (which starts a new worker) and
  re-enqueues exactly once.
- `getOrCreateSendQueue` treats a `closed` queue as absent on both its RLock
  fast path and its write-locked double-check, so it never hands back a
  mid-prune queue.

The retry is bounded to a single attempt: a freshly created queue cannot have
been pruned yet (its idle timer has not run), so a second orphaning is
impossible. Best-effort delivery is preserved — full queues still drop (Raft
retransmits) — but a message can no longer be silently lost into an orphaned
channel.

### Snapshot Send

Source: `pkg/internal/transport/quic.go`, `SendSnapshot()`

Snapshots are streamed over QUIC using a dedicated bidirectional stream
(not the pooled message streams):

1. Resolve target address from registry
2. Get connection to target
3. `conn.OpenStreamSync` -- fresh bidirectional stream
4. Write `SnapshotHeader` (48 bytes) as first frame with `FlagSnapshot` set
5. Write each `SnapshotChunk` as a subsequent frame
6. Await receiver acknowledgment before closing the stream

The sender enforces a write deadline to prevent hanging on unresponsive receivers.
Snapshots may be interrupted if the receiver fails to acknowledge chunks within
the deadline, freeing the stream and allowing the step worker to retry the
InstallSnapshot message.

## Receive Path

Source: `pkg/internal/transport/recv.go`

```
QUIC Listener
    |
    v
acceptLoop()  [scoped by shutdownCtx]
    |-- sem = make(chan struct{}, MaxIncomingConnections)
    |-- listener.Accept() -> conn
    |-- sem acquire (block if at limit)
    |-- go handleConnection(conn)
    v
handleConnection(conn)  [scoped by conn.Context()]
    |-- streamSem = make(chan struct{}, MaxStreamsPerConnection)   // data streams
    |-- snapSem   = make(chan struct{}, MaxConcurrentSnapshotRecv) // snapshot streams
    |-- loop: conn.AcceptStream() -> stream
    |-- streamSem acquire (block if at data-stream limit)
    |-- go handleStream(stream, streamSem, snapSem)
    v
handleStream(stream, streamSem, snapSem)
    |-- loop:
    |   |-- stream.SetReadDeadline(now + 30s)
    |   |-- ReadFrameHeader(stream) -> (length, flags)   // streamSem held
    |   |-- if flags & FlagSnapshot:
    |   |       release streamSem; acquire snapSem (non-blocking)
    |   |       handleSnapshotStream, return
    |   |-- handleMessageFrame(stream, length)
    v
handleMessageFrame(stream, length)
    |-- buf = getRecvBuf(length)            // sync.Pool
    |-- io.ReadFull(stream, payload)
    |-- optionally: decompress if FlagCompressed set
    |-- batch.UnmarshalFrom(payload)
    |-- validate DeploymentID (cross-cluster rejection)
    |-- validate message count <= 10000
    |
    |-- ** CRITICAL: Entry.Cmd copy for buffer safety **
    |-- for each msg.Entries[j].Cmd:
    |       cp = make([]byte, len(cmd))
    |       copy(cp, cmd)                   // break zero-copy alias
    |       entry.Cmd = cp
    |
    |-- putRecvBuf(buf)                     // return to pool
    |-- handler.HandleMessage(batch)
    v
hostMessageHandler.HandleMessage()
    |-- for each msg in batch.Requests:
    |       engine.DeliverMessage(msg.ShardID, msg)
    |       engine.NotifyWork(msg.ShardID)
```

**Connection limits:** `acceptLoop` enforces `MaxIncomingConnections` (default 256)
via a semaphore and a per-IP connection limit (`MaxConnectionsPerIP`, default 16)
via an `ipConnTracker`. If the global limit is reached the connection is rejected
with `QUICErrConnLimitReached`; if the per-IP limit is reached it is rejected with
`QUICErrIPLimitReached`.

**Accept-loop resilience:** Each accept iteration runs inside a deferred panic
recovery boundary (`acceptLoopIteration`). A recovered panic is logged with a
full stack trace and, before re-entering, the loop applies a
`workerPanicRestartDelay` (100 ms) backoff so a persistent panic trigger cannot
spin the CPU or flood logs. A normal (non-panicking) iteration re-enters
immediately, so accept latency is unaffected. This mirrors the backoff applied
to `sendQueueWorker` and `connectionCleanup`.

### Stream Isolation (Data vs Snapshot)

`handleConnection` maintains **two** per-connection semaphores so slow snapshot
transfers can never starve acceptance of replication and heartbeat streams:

- `streamSem` (sized to `MaxStreamsPerConnection`) bounds concurrent
  **data-stream** processing goroutines.
- `snapSem` (sized to `MaxConcurrentSnapshotRecv`, default 4) bounds concurrent
  **snapshot** receives.

Every accepted stream first acquires a `streamSem` slot and holds it only while
`handleStream` reads the stream's first frame header. On detecting
`FlagSnapshot`, `handleStream` releases the `streamSem` slot and acquires a
`snapSem` slot for the duration of the (potentially multi-second) transfer; a
`streamSemHeld` flag tracks ownership so the slot is released exactly once and
never double-released in the deferred cleanup. If the snapshot budget is
exhausted (or the connection is closing) the acquisition is non-blocking and the
snapshot is dropped rather than blocking acceptance of new streams.

Because snapshot transfers are accounted against `snapSem` rather than
`streamSem`, the QUIC-level `MaxIncomingStreams` limit (set in
`NewQUICTransport`) is sized as `MaxStreamsPerConnection + MaxConcurrentSnapshotRecv`
so flow-control backpressure kicks in exactly when the combined data + snapshot
processing capacity is reached. A floor of
`StreamPoolSize + snapshotStreamHeadroom` (4) is also enforced so an operator who
raises `StreamPoolSize` cannot starve a peer's snapshot stream on QUIC flow
control. (The previous hardcoded limit of 4096 advertised far more streams than
the receiver could actually service.)

### Entry.Cmd Buffer Safety

`Entry.UnmarshalFrom` uses zero-copy: `Cmd` aliases the source buffer.
The receive buffer is pooled and reused. If `Cmd` slices were passed to
the engine without copying, the step worker would read stale/overwritten
data when it processes the inbox later.

The receive path copies every `Entry.Cmd` before returning the buffer to
the pool:

```go
for i := range batch.Requests {
    for j := range batch.Requests[i].Entries {
        cmd := batch.Requests[i].Entries[j].Cmd
        if len(cmd) > 0 {
            cp := make([]byte, len(cmd))
            copy(cp, cmd)
            batch.Requests[i].Entries[j].Cmd = cp
        }
    }
}
```

Source: `pkg/internal/transport/recv.go`, `handleMessageFrame()`

### Snapshot Receive

When `FlagSnapshot` is set on the first frame, the stream is routed to
`handleSnapshotStream` -> `processSnapshotHeader` -> `snapshotReceiver`.

The `SnapshotHeader` (48 bytes) identifies the snapshot:

```go
type SnapshotHeader struct {
    ShardID    uint64
    ReplicaID  uint64
    Index      uint64
    Term       uint64
    ChunkCount uint64
    Epoch      uint64
}
```

Source: `pkg/internal/transport/snapshot_header.go`

The `snapshotReceiver` enforces concurrency and resource limits:

- **MaxConcurrentSnapshotReceives** (default 4): Maximum parallel snapshot
  downloads. New requests block until an in-flight receive completes.
- **MaxSnapshotReceiveMemory** (default 1 GB): Total memory budget for all
  in-flight snapshot chunks. As chunks arrive, they consume budget; stale
  chunks free budget when discarded.
- **MaxSnapshotReceiveRate** (default 256 MB/s): Bandwidth throttling per
  receiver. The receive loop paces chunk reads by sleeping proportional to each
  chunk's size (`rateLimitChunkSleep`).

**Rate-limit bounds:** A per-chunk sleep is capped at `maxRateLimitChunkSleep`
(5 s) so a pathologically small configured rate cannot compute a multi-day sleep
that holds the concurrency semaphore and memory budget for the entire duration.
The configured rate is still honored on average across chunks. As a complementary
guard at the config boundary, `TransportConfig.Validate` rejects any non-zero
`MaxSnapshotReceiveRate` below `MinMaxSnapshotReceiveRate` (64 KB/s); a value of 0
selects the default. The 5 s ceiling is a defense-in-depth backstop behind that
minimum-rate floor.

**Memory budget safety:** `receiveChunks` uses a `defer` statement to restore
`totalBytes` to the memory budget on all exit paths, including early returns
on error. Without this, early returns after processing N>0 chunks would
permanently leak the budget for those chunks, eventually starving other
snapshot receives.

**Error logging:** `HandleSnapshot` errors are logged via `slog.Warn` with
structured context (shard, replica, chunk count, error) instead of being
silently discarded. This enables operational visibility into snapshot failures.

**Timer hygiene:** Rate-limiting uses `time.NewTimer` with explicit `Stop()`
instead of `time.After`, preventing timer leaks when the receive loop aborts
early (e.g., on error or connection close).

## Frame Format

Source: `pkg/internal/transport/frame.go`

Every message and snapshot chunk is wrapped in an 8-byte binary frame header:

```
+-------+-------+---------+-------+---------------------------+
| Magic | Magic | Version | Flags |         Length            |
| 0x51  | 0x43  |  0x01   | 1byte |     4 bytes (BE)          |
+-------+-------+---------+-------+---------------------------+
|                     Payload (Length bytes)                  |
+-------------------------------------------------------------+
```

| Field   | Offset | Size   | Value                             |
|---------|--------|--------|-----------------------------------|
| Magic   | 0      | 2      | `0x51 0x43` ("QC" for QuicRaft)   |
| Version | 2      | 1      | `0x01` (non-zero required)        |
| Flags   | 3      | 1      | Bitfield (see below)              |
| Length  | 4      | 4      | uint32 big-endian, max 16 MB      |

Maximum frame payload: `MaxFrameSize = 16 * 1024 * 1024` (16 MB).

**Version validation:** Frames with version byte 0x00 are rejected (prevents
accidental misinterpretation of uninitialized buffers as valid frames).

**Decompression limit:** Payloads are decompressed only if the decompressed
size is within `MaxDecompressedSize` (default 16 MB). Oversized payloads are
rejected to prevent decompression bombs.

### Frame Flags

```go
type FrameFlags byte

const (
    FlagNone       FrameFlags = 0x00  // regular message batch (optionally compressed)
    FlagCompressed FrameFlags = 0x01  // Snappy-compressed payload
    FlagSnapshot   FrameFlags = 0x02  // snapshot header/chunk stream
)
```

The `FlagCompressed` bit is set when the payload has been compressed with
Snappy. The receiver decompresses before unmarshaling. `FlagSnapshot` indicates
the first frame of a snapshot stream (SnapshotHeader) and subsequent chunk
frames on the same stream.

### MessageBatch Wire Format

Source: `pkg/proto/messagebatch.go`

```
+----------+---------------+-----------+--------+-----------+-----+
| BinVer:8 | DeploymentID:8| AddrLen:4 | Addr:N | ReqCnt:4  | ... |
+----------+---------------+-----------+--------+-----------+-----+
                                                     |
                                          ReqCnt x Message
```

- `BinVer` validated against `proto.WireVersion` (currently 1)
- `DeploymentID` validated against transport config (cross-cluster rejection)
- `ReqCnt` validated against `maxMessagesPerBatch` (10000)
- Cumulative size tracked against `MaxBatchSize` (64 MB)

### Message Wire Format

Source: `pkg/proto/message.go`

11 fixed uint64 fields (88 bytes), followed by entries and optional snapshot:

```
+------+------+----+--------+-----------+------+---------+----------+
| Type | From | To | ShardID| ReplicaID | Term | LogTerm | LogIndex |
|  :8  |  :8  | :8 |   :8   |    :8     |  :8  |   :8    |    :8    |
+------+------+----+--------+-----------+------+---------+----------+
| Commit | Reject | Hint | EntryCnt:4 | entries... |
|   :8   |   :8   |  :8  |            |            |
+--------+--------+------+------------+------------+
| HasSnapshot:8 | snapshot... (if HasSnapshot) |
+---------------+------------------------------+
```

`Reject` is stored as uint64 on wire (0 or 1) for fixed-width encoding.

### Entry Wire Format

Source: `pkg/proto/entry.go`

60-byte fixed header + variable-length command:

```
+------+-------+------+-----+----------+----------+-------------+---------+------+
| Term | Index | Type | Key | ClientID | SeriesID | RespondedTo | CmdLen  | Cmd  |
|  :8  |  :8   |  :8  | :8  |    :8    |    :8    |     :8      |  :4(u32)| :N   |
+------+-------+------+-----+----------+----------+-------------+---------+------+
```

- `CmdLen` validated against `MaxEntrySize` (8 MB) before buffer access
- `Cmd` is zero-copy aliased into the source buffer after `UnmarshalFrom`
- `Type` lower 4 bits: entry type; bit 4: compression flag

## TLS Configuration

Source: `pkg/internal/transport/tls.go`, `pkg/config/transportconfig.go`

mTLS is **mandatory** for all transport connections. There is no insecure
fallback mode. Certificate material is provided via `config.TransportConfig`:

```go
// Public API (pkg/config/transportconfig.go)
type MTLSConfig struct {
    CACert []byte  // PEM-encoded CA certificate
    Cert   []byte  // PEM-encoded node certificate
    Key    []byte  // PEM-encoded private key
}

type RevocationConfig struct {
    CRLPaths          []string      // Paths to CRL PEM files (hot-reload)
    OCSPResponderURL  string        // OCSP responder URL (optional)
    OCSPCacheSeconds  int           // Positive-result cache TTL (default 300)
    OCSPTimeoutSeconds int          // OCSP HTTP timeout (default 5)
    CheckInterval     time.Duration // CRL reload interval (default 60s)
    Mode              string        // "crl" | "ocsp" | "both" | "any" (default "any")
    EnforceRevocation bool          // fail-closed on unknown status (default false)
    OnCertRevoked     func(serial string)
    // ... see config.RevocationConfig
}

type TransportConfig struct {
    MTLSConfig       *MTLSConfig        // Required when transport is enabled
    RevocationConfig *RevocationConfig  // Optional certificate revocation
    // ... other fields
}
```

**TLS properties:**

- **Server mode**: `RequireAndVerifyClientCert`, verifies peer certificate
  against CA pool
- **Client mode**: verifies server certificate against CA pool, presents own cert
- **ALPN**: `"quicraft"` (Application-Layer Protocol Negotiation)
- **Minimum TLS version**: 1.3
- **Certificate revocation**: optional CRL file support with hot-reload, or
  OCSP responder integration for runtime revocation checks

**Certificate revocation:** When `RevocationConfig` is set, a `VerifyConnection`
callback is installed on both the server and client `tls.Config`.
`VerifyConnection` (rather than `VerifyPeerCertificate`) is used deliberately
because it runs on **all** connections including resumed TLS sessions, so
revocation is still enforced on session resumption.

- **Leaf-only checking:** Only the leaf certificate (`PeerCertificates[0]`) is
  revocation-checked, not the entire presented chain. The leaf is the end-entity
  certificate that authenticates the peer and the one that gets revoked when a
  node is decommissioned or compromised. The CA trust pool has already validated
  the chain before `VerifyConnection` runs, and checking intermediates/root on
  every handshake added per-handshake OCSP latency for certificates governed by
  the CA's own lifecycle rather than per-node revocation.
- **OCSP negative cache:** When an OCSP query fails (responder down, timeout,
  network error) the failure is cached for `ocspUnavailableTTL` (10 s). During
  that short window, reconnect handshakes short-circuit instead of re-querying a
  dead responder and adding the full OCSP timeout to every handshake. The
  negative cache only suppresses re-queries — it never upgrades an unavailable
  result to "good" or "revoked". Its TTL is far shorter than the positive-result
  cache (`OCSPCacheSeconds`, default 300 s) so the responder is retried promptly
  once it recovers.
- **Fail-closed mode:** With `EnforceRevocation = true`, an unknown revocation
  status (OCSP unreachable, no CRL configured) rejects the connection. The
  default (`false`) is soft-fail: connections are allowed when status cannot be
  determined.

All nodes in a cluster must share the same CA certificate for mutual
verification. Each node may have its own leaf certificate (with unique CN/SAN)
signed by that CA.

The internal transport builds separate server and client `tls.Config`
instances from the provided PEM material. The `ServerName` field on the client
is dynamically set to the target host during dial (extracted via
`net.SplitHostPort`), enabling mTLS with non-localhost targets.

## Node Registry

Source: `pkg/internal/registry/registry.go`

The registry maps `(shardID, replicaID)` pairs to network addresses. It is
a concrete type (no interface) for hot-path efficiency.

```go
type Registry struct {
    mu        sync.RWMutex
    addresses map[NodeInfo]string
}

type NodeInfo struct {
    ShardID   uint64
    ReplicaID uint64
}
```

Operations:

| Method     | Lock    | Description                               |
|------------|---------|-------------------------------------------|
| `Resolve`  | RLock   | Look up address, return NodeNotFoundError |
| `Register` | Lock    | Add or update address                     |
| `Remove`   | Lock    | Delete entry (no-op if absent)            |
| `Count`    | RLock   | Number of registered nodes                |

The registry is populated by `Host.StartShard()` when members are registered,
and cleaned by `Host.StopShard()` when a shard is unloaded. The transport
calls `Resolve` on every `Send` to map `(ShardID, To)` to a target address.

### Address Resolution Flow

```
Step Worker produces Message{ShardID: 100, To: 3}
    |
    v
QUICTransport.Send()
    |-- reg.Resolve(msg.ShardID=100, msg.To=3) -> "10.0.0.3:4001"
    |-- enqueueToTarget("10.0.0.3:4001", msg)  // non-blocking
    v
per-target sendQueue -> sendQueueWorker drains/batches -> sendBatch
```

The registry is populated by `Host.StartShard()` when members are registered,
and cleaned by `Host.StopShard()` when a shard is unloaded.

## Transport Configuration Summary

`TransportConfig` fields (with defaults):

| Field                           | Type    | Default        | Description                          |
|---------------------------------|---------|----------------|--------------------------------------|
| `MTLSConfig`                    | pointer | required       | mTLS certificate material            |
| `RevocationConfig`              | pointer | nil            | Optional CRL/OCSP revocation         |
| `StreamPoolSize`                | int     | 16             | Initial stream pool size             |
| `MaxStreamPoolSize`             | int     | 256            | Maximum stream pool size             |
| `MaxSnapshotReceiveRate`        | int64   | 256 MB/s       | Snapshot bandwidth limit             |
| `MaxConcurrentSnapshotReceives` | int     | 4              | Parallel snapshot downloads          |
| `MaxSnapshotReceiveMemory`      | int64   | 1 GB           | Total snapshot buffer budget         |
| `MaxDecompressedSize`           | uint32  | 16 MB          | Decompression size limit             |
| `MaxConnectionsPerIP`           | int     | 16             | Concurrent connections per IP        |
| `MaxIncomingConnections`        | int     | 256            | Total incoming connection limit      |
| `SendBatchMaxSize`              | int     | 64 KB          | Max bytes per send batch             |
| `UDPRecvBufSize`                | int     | 7 MB           | Kernel receive buffer size           |
| `UDPSendBufSize`                | int     | 7 MB           | Kernel send buffer size              |
| `Enable0RTT`                    | bool    | false          | 0-RTT reconnection (replay risk)     |
| `DisableCompression`            | bool    | false          | Disable Snappy compression           |

## Transport Metrics

The transport can emit metrics via the `TransportMetrics` interface if configured:

```go
type TransportMetrics interface {
    ObserveSendLatency(d time.Duration)
    IncMessagesSent(shardID, to uint64, msgType uint8)
    IncMessagesReceived(shardID, from uint64, msgType uint8)
    IncSnapshotsSent(shardID uint64, bytes int64, duration time.Duration)
    IncSnapshotsReceived(shardID uint64, bytes int64, duration time.Duration)
}
```

These callbacks allow integrating with external observability platforms
(Prometheus, Datadog, etc.) for monitoring transport behavior.

## Cross-References

- [Architecture Overview](../architecture/overview.md) -- system overview, data flow, configuration
- [Snapshot Management](../snapshots/overview.md) -- snapshot streaming and persistence
- [Encryption and Security](../encryption/overview.md) -- mTLS setup and key rotation
