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
| `tls.go`           | Self-signed and mTLS certificate builders      |
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
    MaxIncomingStreams:          1024,
    MaxIncomingUniStreams:       -1,         // disabled
    KeepAlivePeriod:            10 * time.Second,
    InitialStreamReceiveWindow: 1 << 20,    // 1 MB
    MaxStreamReceiveWindow:     6 << 20,    // 6 MB
    MaxConnectionReceiveWindow: 15 << 20,   // 15 MB
    Allow0RTT:                  cfg.Enable0RTT,
}
```

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
    |-- group messages by target address via registry.Resolve()
    |-- for each target: spawn parallel goroutine sendBatch(target, batch)
    v
sendBatch(target, msgs)  [parallel per target]
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

- **Non-blocking**: `Send` is fire-and-forget. Raft handles retransmission.
- **Target grouping**: messages to the same peer are batched into one frame.
- **Parallel per-target sends**: each target address gets its own goroutine.
  Total latency is the maximum (not the sum) of all target sends, enabling
  faster batch completion when some targets are slower.
- **Buffer pooling**: `sync.Pool` with initial capacity `SendBatchMaxSize + FrameHeaderSize`.
  Buffers exceeding a threshold are not returned to the pool.
- **Compression**: Snappy compression is applied by default. Disable via
  `config.TransportConfig.DisableCompression` for CPU-constrained environments.
- **Heartbeat priority**: heartbeats use stream index 0 (persistent pre-opened stream),
  separate from data streams, enabling low-latency leader heartbeats.
- **Write deadline**: 2-second deadline per stream write. On failure, the entire
  connection and all its streams are evicted; the next send dials fresh.

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
    |-- loop: conn.AcceptStream() -> stream
    |-- go handleStream(stream)
    v
handleStream(stream)
    |-- loop:
    |   |-- stream.SetReadDeadline(now + 30s)
    |   |-- ReadFrameHeader(stream) -> (length, flags)
    |   |-- if flags & FlagSnapshot: handleSnapshotStream, return
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
via a semaphore. If the limit is reached, new connection attempts block until
an existing connection closes or times out.

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
  receiver. The receive loop paces chunk reads to respect the rate limit.

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
    CRLFiles       []string  // Paths to CRL PEM files (hot-reload)
    OCSPResponder  string    // OCSP responder URL (optional)
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
    |-- grouped["10.0.0.3:4001"] = append(..., msg)
    v
for each target in grouped:
    |-- go sendBatch(target, msgs)  [parallel]
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
