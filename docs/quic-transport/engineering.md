# QUIC Transport — Engineering Contract

> **Status: NORMATIVE** · Scope: go-quicraft `pkg/internal/transport`,
> `pkg/internal/transport/revocation`, `pkg/internal/registry`,
> `pkg/internal/netaddr` · Last verified: 2026-10-02
> Conflicts with this document are defects. Fix them here first.

These packages are internal: they are not importable from outside the
module. The transport is configured through `config.TransportConfig` and
`config.MTLSConfig` and driven by the `Host`; there is no `Transport` interface
to swap and no exported transport type. This document is the implementation
contract for work inside this repository. An embedder's surface is
[`api.md`](api.md).

Design rationale, rejected alternatives and the scope fence are in
[`architecture.md`](architecture.md).

## Overview

QuicRaft uses QUIC exclusively for peer-to-peer communication. There is no
TCP fallback and no insecure mode: `TransportConfig.Validate` refuses a
transport without mTLS material, and every connection is mutually
authenticated. The properties that made QUIC the right substrate:

- **Built-in TLS 1.3.** Encryption is part of the connection, not a wrapper
  around it, and the handshake carries the client certificate.
- **Stream multiplexing.** Every shard on a host shares one UDP socket, and all
  traffic to a given peer address shares one cached outbound connection whose
  streams are keyed by shard. A large replication burst on one shard does not
  head-of-line block another shard's heartbeats. (Each node dials the other
  independently, so a healthy pair normally holds two connections, one in each
  direction.)
- **Session resumption.** The client config keeps a 256-entry LRU session cache,
  so a reconnect costs one round trip instead of two.

0-RTT exists as a config knob (`Enable0RTT`) but buys this library nothing and
is off by default. 0-RTT data is replayable (RFC 9001 §8), the dial path uses
`Dial` rather than `DialEarly` so this node never *sends* early data, and the
receive path refuses any message frame or snapshot that arrived over 0-RTT.
Turning it on only opens a replay surface for non-quicraft clients.

The transport is implemented in `pkg/internal/transport/` as a concrete
`QUICTransport` type. There is no `Transport` interface: the engine holds
`*QUICTransport` directly, because there is one implementation and the send path
is hot. The underlying QUIC library is
[quic-go](https://github.com/quic-go/quic-go) v0.63.0 (see `go.mod`).
v0.60.0 is a hard floor, not merely the current requirement: below it, packet
protection is built with the general `cipher.NewGCM` and a process panics under
`GODEBUG=fips140=only`. See
[FIPS 140-3 exemption record](architecture.md#fips-140-3-exemption-record).

Source files:

| File                  | Responsibility                                              |
|-----------------------|-------------------------------------------------------------|
| `quic.go`             | `QUICTransport` struct, Start/Stop, Send, send queues, SendSnapshot |
| `conn.go`             | Connection pool, dial, stream pool, peer incarnations, certificate exclusivity |
| `recv.go`             | Accept loop, stream dispatch, message-frame decode, peer identity |
| `heartbeat.go`        | Pre-serialized heartbeat fast path                          |
| `frame.go`            | Frame wire format, send-side framing admission (`sendableGroup`) |
| `flags.go`            | `FlagSnapshot`, the one frame flag defined outside `frame.go` |
| `tls.go`              | mTLS config builder (`buildMTLSTLS`), CA bundle parsing, revocation callback |
| `revocation/`         | CRL/OCSP checker with hot reload and negative caching       |
| `config.go`           | Internal `Config`, defaults, validation, metric label sets  |
| `handler.go`          | `MessageHandler` and `PeerAuth`                             |
| `snapshot_header.go`  | `SnapshotHeader` wire format (56 bytes, big-endian)         |
| `snapshot_recv.go`    | Snapshot receiver: concurrency, memory, rate and liveness bounds |
| `shutdown_tracker.go` | Lock-free in-flight counter that can reject new work atomically |
| `errors.go`           | Typed transport errors and QUIC application error codes     |
| `netfault_on.go` / `netfault_off.go` | Build-tagged (`quicraft_netfault`) partition injector; compiled away in normal builds |

## Connection management

### Connection pool

`QUICTransport` maintains a connection pool keyed by target address:

```go
type QUICTransport struct {
    connMu    sync.RWMutex
    conns     map[string]*quic.Conn   // target address -> QUIC connection
    connEpoch map[string]uint64       // target address -> peer boot epoch it was dialed under
    dialGroup singleflight.Group      // coalesce concurrent dials
    // ...
}
```

Source: `pkg/internal/transport/conn.go`

`conns` and `connEpoch` are written and deleted at exactly the same sites under
`connMu`, so they never diverge. `connEpoch` is what makes restart detection
complete; see [Rolling restart detection](#rolling-restart-detection) below.

**Lookup (fast path)**: `RLock`, map lookup, verify `conn.Context()` is
not done. If alive, return immediately. If done, the connection is removed and
its pooled streams evicted before the dial.

**Dial (slow path)**: `singleflight.Group` ensures only one goroutine dials
a given target. Other callers block on the same flight and receive the
shared result. The dial deadline is `Config.DialTimeout` (default
`dialTimeout`, 5 s) and derives from `shutdownCtx`, so `Stop` unblocks it.

```
getConnection(target, want)
    |-- RLock: check conns[target]
    |   |-- alive? return
    |   |-- dead? removeConnection + evictStreamsForTarget
    |-- dialGroup.Do(target+want, dialConnection)
    |   |-- net.ResolveUDPAddr
    |   |-- net.SplitHostPort(target) -> host   (empty host => ErrEmptyDialHost)
    |   |-- dialEpoch = knownPeerEpoch(target)  // captured BEFORE the dial
    |   |-- clientTLS.ServerName = host
    |   |-- quicTr.Dial(ctx, addr, clientTLS, quicCfg)
    |   |-- peer identity == want, or close    // exact pin (see Identity pinning)
    |   |-- Lock: re-check cache, then conns[target] = conn
    |   |             connEpoch[target] = dialEpoch
    |   |-- go preOpenHeartbeatStream(target, conn)
    |-- return conn
```

**Server name and the empty-host rule.** `dialConnection` extracts the host with
`net.SplitHostPort(target)` and sets `clientTLS.ServerName = host`, which is what
makes the peer's SANs load bearing. A malformed target returns a `DialError`.
An *empty* host is refused with `ErrEmptyDialHost` rather than defaulted:
verifying a peer certificate against an implicit `"localhost"` would mask a
misconfigured address and silently fail SAN matching against real
hostname or IP certificates.

**Identity pinning.** The handshake proves only a cluster certificate whose SAN
covers the host, which a wildcard or shared-SAN certificate held by anyone on
the path to that address also satisfies. So every connection is taken for a
specific destination identity, `Config.ReplicaIdentity(shard, replica)`. The
host answers it with the identity membership binds to that replica, which is
the same identity an inbound sender from that replica is held to. The pin
travels with the send down to the point it takes a connection:

- `sendToTarget` splits a batch by destination identity (nearly always one
  group: every replica a node hosts presents that node's identity) and drops,
  as `identity_mismatch`, messages for a replica nothing binds.
- `getStream` hands out a pooled stream only when its connection's peer
  presented that identity (`connServes`); `getConnection` hands out the pooled
  connection only on the same condition; and `dialConnection` keeps a dial only
  when the peer presents **exactly** that identity, closing it otherwise
  (`QUICErrDialIdentity`, `DialIdentityError`). Every re-dial and retry on the
  write path goes through these, so no write can reach an unchecked peer.
- One connection is pooled per address. When the pooled peer presented another
  identity that is **still bound** at the address (two bound identities meet
  at one address only when a member has moved its own route onto another
  replica's), the send is refused and dropped, not re-dialed, so that member can
  neither receive the replica's traffic nor churn the connection
  (`replaceUnboundPeer`). When the pooled peer is bound to **nothing** there any
  more (its replica removed, or rebound to a new keypair at the same address),
  the connection is closed (compare-and-remove) and the send dials whoever is
  bound now.
- A refusal is remembered per (address, identity) for `dialRefusalCooldown`, so
  it costs no handshake, and no registry scan, per send.

The peer identity of each pooled connection is recorded at dial (`connPeer`,
under `connMu` with `conns`), so the pin costs no TLS-state copy or hash per
send: about 22 ns and no allocations for a one-message batch, and about 28 ns
per additional message (`BenchmarkPinnedSendOverhead`). With pinning on, an
empty destination identity fails closed: a caller that names none gets no
connection rather than an unpinned one.

**Dial race.** `singleflight` coalesces only overlapping dials. A dial that
starts just after another completed reaches the insert with the cache already
populated, so `dialConnection` re-checks under `connMu` and keeps the live
winner, closing the loser with `QUICErrDuplicateDial`. Overwriting would leak
the cached connection, which keep-alives would then hold open forever with its
pre-opened streams diverging from the pool's bookkeeping.

**Background cleanup.** One goroutine (`connectionCleanup`) runs every
`connectionCleanupInterval` (30 s) and performs four sweeps:

- `cleanStaleConnections` drops pooled outbounds whose `Context()` is done,
  evicts their streams, and fires `OnConnectionFailed`, all outside `connMu`,
  because that lock is on every shard's send path and the listener callback is
  user code of unbounded duration.
- `reapPeerIncarnations` forgets a peer's incarnation record once the address has
  had neither a cached outbound nor traffic for `peerIncarnationRetention`
  (10 × the interval, so 5 minutes).
- `revokeCheckConnections` re-runs the revocation verdict against every live
  inbound and outbound leaf. `VerifyConnection` fires only at handshake, so
  without this a certificate revoked after admission would keep its session for
  the life of the process.
- `reapIdleInboundConns` closes inbound connections that have carried no frame
  for `inboundIdleRetention` (also 5 minutes). Nothing else can reclaim them:
  this node's keep-alive (10 s) sits below `MaxIdleTimeout` (30 s), so QUIC's
  idle timeout never fires on an accepted connection.

The stamp used by the idle sweep is taken on every *frame*, not on stream
acceptance. Senders cache and reuse one stream across bursts, so a connection
carrying continuous replication traffic can accept no new stream for hours.

A committed membership removal reaches both halves: `CloseTarget` tears down the
outbound and `closeInboundFromPeer` closes every inbound whose verified leaf
covers the removed address. Matching there is by **certificate**, not by remote
address: a connection's remote address carries the peer's ephemeral source port,
which never equals the Raft address a membership change names.

### Connection close codes

`CloseWithError` codes are what a peer sees when this node drops a connection.
The reason *phrase* travels to the peer verbatim, so for the security verdicts
it is redacted (`closeReasonRedacted`) and the detail is logged locally instead;
the code stays distinct per verdict because that is the protocol contract.

| Code | Name | Meaning |
|------|------|---------|
| `0x1` | `QUICErrShutdown` | Graceful transport shutdown, or an outbound evicted as predating the peer's current incarnation |
| `0x2` | `QUICErrConnLimitReached` | `MaxIncomingConnections` exhausted |
| `0x3` | `QUICErrIPLimitReached` | `MaxConnectionsPerIP` exhausted |
| `0x4` | `QUICErrDuplicateDial` | This dial lost a race; the cached connection is being reused |
| `0x5` | `QUICErrPromiscuousCert` | The peer's leaf also covers this node's own host |
| `0x6` | `QUICErrRevoked` | The peer's certificate is no longer admissible |
| `0x7` | `QUICErrInboundIdle` | Reclaimed after `inboundIdleRetention` with no frames |
| `0x8` | `QUICErrPeerRemoved` | A committed membership change removed this peer |

Stream-level codes: `QUICStreamErrReceiverGone` (`0x1`) means this receiver has
abandoned the stream, `QUICStreamErrShutdown` (`0x2`) cancels a snapshot write
during shutdown, and `QUICStreamErrFrameRejected` (`0x3`) means the frame was
unacceptable but the connection is fine; see
[Failed write handling](#stream-pool-and-heartbeat-streams).

### Rolling restart detection

Every `MessageBatch` carries the sender's
`SourceEpoch`, a per-process boot epoch, and every outbound records, in
`connEpoch`, the epoch its target was believed to be running when it was
dialed. On receive, `evictStaleOutboundOnRecv` evicts the cached outbound when
that stamp does not match the epoch of the batch that just arrived: the
outbound belongs to an incarnation the peer has left behind, so it is
black-holed and would otherwise linger until `MaxIdleTimeout` (~30s), stalling
`HeartbeatResp`/`ReplicateResp` and triggering avoidable elections. The key is
the epoch, not the inbound `*quic.Conn` pointer: each peer dials independently
and re-dials benignly, and those re-dials carry the same epoch. Keying on the
connection pointer evicts on every benign re-dial and produces a
self-sustaining cascade.

Boot epochs are `UnixNano ^ crypto/rand`, so two of them cannot be ordered by
comparison. Each peer's record therefore keeps a small ring of superseded
epochs (`supersededEpochRing`, 4), making its incarnation history one-way. A
last-writer-wins record would go backwards: a restart leaves the old inbound
draining, so a frame written by the dead incarnation can arrive after the fresh
outbound has been dialed and evict the healthy connection. A batch bearing an
already-superseded epoch is ignored for both recording and eviction.

Acting on `SourceEpoch` requires authorization: it and `SourceAddress` are
in-band and unauthenticated by mTLS, so a record is created or advanced only
when the claimed target's host appears in the inbound connection's verified
peer certificate. The dead-outbound branch is exempt: a spoofed batch cannot
make our own outbound's context `Done`.

### Stream pool and heartbeat streams

Streams are pooled to reduce per-message overhead. The pool holds
`StreamPoolSize` (default 16) indices per target. `MaxStreamPoolSize` (default
256) is only the validation ceiling `StreamPoolSize` is checked against. **The
pool does not auto-scale**, and nothing in the transport grows it at runtime.

Stream index assignment (`streamIndexFor`):

- **Heartbeats**: always stream index 0.
- **Data messages**: `idx = (shardID % (poolSize - 1)) + 1`, or 0 when
  `poolSize <= 1`.

The pool itself is sharded into `streamShardCount` (16, a power of two) buckets
selected by `idx & (streamShardCount - 1)`, each with its own mutex, to reduce
contention between concurrent senders.

**Persistent heartbeat streams.** Stream 0 is pre-opened by a goroutine spawned
from the dial path (`preOpenHeartbeatStream`, 5 s deadline) and kept for the
life of the connection, keeping `OpenStreamSync` off the heartbeat and ReadIndex
critical path. Failure to pre-open is non-fatal; the first heartbeat opens it
lazily.

**Streams are judged by their owning connection, not the cached one.** Each
shard bucket keeps an `owners` map alongside `streams`, recording the
`*quic.Conn` each pooled stream was opened on. In the peer-restart window a
delayed pre-open can store a stream opened on a now-closed connection while a
fresh one is already cached for the same target. Comparing against the cached
connection alone would hand out that dead stream, the write would fail, and the
failure path would then tear down the healthy connection.

`cachedStream` therefore resolves three cases:

| Owner state | Action |
|-------------|--------|
| Owner is the cached conn and alive | Return the stream (after re-checking it is still the cached entry) |
| Owner is the cached conn and dead | Evict the connection and all its streams; redial |
| Owner is some other conn, or none is cached | Evict just that stream; the slow path opens a fresh one on the current connection |

**Failed write handling.** A `SetWriteDeadline` or `Write` failure evicts the
whole connection and all its streams (`removeAndCloseConnection` +
`evictStreamsForTarget`), so the next send dials fresh. The one exception is a
stream reset carrying `QUICStreamErrFrameRejected`: the peer's decoder refused
the frame, which proves the connection is alive, so only the batch is dropped
and only that stream is replaced (`evictStream`). Treating it as a dead
connection would turn one unacceptable batch into a dial storm.

### QUIC configuration

Source: `pkg/internal/transport/quic.go`, `NewQUICTransport`

```go
qcfg := &quic.Config{
    MaxIdleTimeout:             30 * time.Second,
    HandshakeIdleTimeout:       cfg.DialTimeout,            // 5 s by default
    MaxIncomingStreams:         int64(maxIncomingStreams), // derived, see below
    MaxIncomingUniStreams:      -1,                        // disabled
    KeepAlivePeriod:            10 * time.Second,
    InitialStreamReceiveWindow: initialStreamReceiveWindow, // 2 MB
    MaxStreamReceiveWindow:     maxStreamReceiveWindow,     // 8 MB
    MaxConnectionReceiveWindow: connectionReceiveWindow(
        cfg.MaxMessageReceiveMemory, cfg.MaxIncomingConnections), // derived
    Allow0RTT: cfg.Enable0RTT,
}
```

Two of these are derived rather than constant.

`MaxIncomingStreams` is `MaxStreamsPerConnection + MaxConcurrentSnapshotRecv`,
floored at `StreamPoolSize + snapshotStreamHeadroom` (4). Snapshot streams are
charged to a separate semaphore for the duration of their transfer, so the
advertised budget has to cover both; the floor stops an operator who raises
`StreamPoolSize` from starving a peer's snapshot stream on flow control. A
fixed large value would advertise far more streams than the receiver can
service. See [Stream isolation](#stream-isolation-data-vs-snapshot).

`HandshakeIdleTimeout` follows the dial budget: quic-go abandons a handshake
that idles that long and caps the whole handshake at twice it, so a dial
deadline wider than this could never be reached.

`MaxConnectionReceiveWindow` is `MaxMessageReceiveMemory / MaxIncomingConnections`,
floored at the 8 MB per-stream window and capped at 32 MB. quic-go buffers bytes
before the application reads them, so a window fixed independently of the
application budget multiplies by `MaxIncomingConnections` into memory that
budget does not cover: 32 MB × 256 connections is 8 GB against a 256 MB budget.
The floor means the QUIC-level footprint is not strictly inside the budget for
small budgets; it is bounded and documented rather than silently unbounded.

A receive window also caps a connection at `window / RTT`. At the defaults the
derivation floors at 8 MB, which sustains roughly 134 Gbps at a 0.5 ms same-AZ
RTT but caps a 10 Gbps path beyond about 6.7 ms RTT. Cross-region deployments on
10 GbE or faster should raise `MaxMessageReceiveMemory` (or lower
`MaxIncomingConnections`) to widen the window.

**UDP buffer tuning.** `Start` sets `SO_REUSEADDR`, `SO_RCVBUF`
(`UDPRecvBufSize`, default 7 MB) and `SO_SNDBUF` (`UDPSendBufSize`, default
7 MB) on the listening socket through a `net.ListenConfig` control function. A
`setsockopt` failure is logged at WARN naming `net.core.rmem_max` /
`net.core.wmem_max` rather than failing startup, so a host with tight sysctls
runs with the OS default instead of refusing to boot. Setting either to 0 skips
the call.

## Send path

Source: `pkg/internal/transport/quic.go`

```
Engine Step Worker
    |
    |  sender.Send(msgs []proto.Message)
    v
QUICTransport.Send()
    |-- for each msg: deep-copy entries + snapshot membership maps
    |   (break alias with the step worker's reusable proposeBuf)
    |-- addr = registry.Resolve(msg.ShardID, msg.To)   // miss => drop, "unresolved_target"
    |-- enqueueToTarget(addr, msg)                     // non-blocking enqueue
    v
sendQueue (per target: data channel cap 2048 + heartbeat channel cap 64)
    |
    v  drained by background sendQueueWorker(target) goroutine
sendQueueWorker
    |-- read first message, then drain buffered messages into a batch
    |   (heartbeats drained before data; up to 16 hb / 256 data per batch)
    |-- sendToTarget: heartbeat-only batch -> sendHeartbeatBatch (fast path)
    |                 otherwise            -> sendBatch
    v
sendBatch(target, msgs)
    |-- loop: sendableGroup(headerSize, msgs) -> how many fit ONE frame
    |   |-- msgs[0] unframeable? drop it ("unsendable"), continue
    |   |-- sendFrame(target, msgs[:n], payloadSize)
    v
sendFrame
    |-- build MessageBatch{BinVer, DeploymentID, SourceEpoch, SourceAddress, Requests}
    |-- bufPtr = getBuf(size + FrameHeaderSize)   // sync.Pool
    |-- mb.MarshalTo(payloadBuf)
    |-- non-heartbeat && !DisableCompression: Snappy encode; keep only if smaller
    |-- MarshalFrame(buf, payload, flags)         // prepend the 8-byte header
    |-- stream = getStream(target, shardID, isHeartbeat)
    |-- stream.SetWriteDeadline(now + sendWriteDeadline)   // 2s
    |-- stream.Write(buf[:frameSize])
    |-- putBuf(bufPtr)
```

Key properties:

- **Non-blocking enqueue.** `Send` deep-copies and enqueues; it never dials,
  opens streams, or writes. The step worker cannot stall on transport I/O.
- **Deep copy is not optional.** `Entry.Cmd` aliases the step worker's reusable
  `proposeBuf`, and `proto.Snapshot` embeds `Membership`'s six maps, which
  `MarshalTo` ranges over on the async send path. A caller mutating those in
  place would be an unrecoverable "concurrent map iteration and map write" fatal
  error that the worker's `recover` cannot catch.
- **Best-effort drop on full.** A full channel drops the message, counts
  `queue_full`, and Raft retransmits on the next heartbeat or replicate cycle.
- **Per-target send queues.** Each target address gets its own buffered queue and
  a dedicated `sendQueueWorker`, so a slow target cannot block a fast one.
- **A batch is a coalescing artifact, never all-or-nothing.** Messages whose
  combined marshaled size exceeds `MaxFrameSize` are split across consecutive
  frames on the same ordered stream rather than dropped together.
- **Unsendable messages are dropped before the write.** A message with more than
  `proto.MaxEntriesPerMessage` (10000) entries, or larger on its own than a
  frame, is refused by `sendBatch` and logged at ERROR with the drop reason
  `unsendable`. Writing it would make the peer's decoder reset the stream, which
  the sender can only read as an I/O failure, so the identical batch would be
  retried forever. See [Replication payload sizing](#replication-payload-sizing).
- **Buffer pooling.** `sync.Pool` with initial capacity
  `SendBatchMaxSize + FrameHeaderSize`; buffers above `maxPoolBufSize` (256 KB)
  are left for GC. A separate pool recycles Snappy encode destinations.
- **Compression.** Snappy is applied to non-heartbeat frames by default, and the
  compressed payload is kept only when it is actually smaller. Heartbeats skip
  it (under 200 bytes, where Snappy costs more latency than it saves).
  `TransportConfig.DisableCompression` turns it off entirely.
- **Heartbeat fast path.** A heartbeat-only batch uses `sendHeartbeatBatch`,
  which writes through a `MessageBatch` header prefix pre-serialized once in
  `Start` (`BinVer`, `DeploymentID`, `SourceEpoch`, `SourceAddress`), computes
  the frame size from the constant `proto.HeartbeatMessageSize` (100 bytes), and
  skips compression. The wire result is an ordinary `FlagNone` frame.
- **Write deadline.** `sendWriteDeadline` is 2 s, kept short so a peer restart is
  detected quickly instead of stalling replication.

### Replication payload sizing

`MaxReplicationPayloadBytes` (`pkg/internal/transport/frame.go`) is the largest
raw entry payload one replication batch may carry and still fit a frame:

```
MaxFrameSize - FrameHeaderSize - batchFramingReserve
  = 16777216  - 8               - 65536
  = 16711672 bytes (~15.94 MB)
```

Raft's `maxReplicationPayload` is `Config.MaxApplyEntrySize`, whose default is
this constant (`config.DefaultMaxApplyEntrySize`), and `config.Validate` rejects
any larger value with `*MaxApplyEntrySizeExceededError`. `pkg/config` carries a
duplicate of the constant with a compile-time assertion that fails the build if
the two ever drift, because `pkg/internal/transport` imports `pkg/config` and
the dependency cannot go the other way.

The clamp is load bearing. An oversized message is dropped by the send path,
raft does not advance the follower's next index, so it rebuilds the identical
oversized message on every replicate cycle and the follower never catches up:
no backoff, no batch-size reduction, and no snapshot fallback, because the
entries are still present. `proto.MaxEntriesPerMessage` is the other half of the
same contract: a byte-bounded batch of tiny entries can still exceed the
decoder's entry-count limit with the same outcome.

### Per-target send queues and idle pruning

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
impossible. Best-effort delivery is preserved (full queues still drop and Raft
retransmits), but a message cannot be silently lost into an orphaned
channel.

### Snapshot send

Source: `pkg/internal/transport/quic.go`, `SendSnapshot` and
`SendSnapshotStreaming`

Snapshots use a dedicated bidirectional stream, never the pooled message
streams:

1. `snapshotTracker.TryAdd()`, refused with `ErrStreamClosed` once `Stop` has
   begun, which is what lets shutdown wait for in-flight transfers without a
   `WaitGroup.Add`/`Wait` race.
2. Resolve the target address from the registry.
3. Get (or dial) the connection.
4. `conn.OpenStreamSync` on a context derived from `shutdownCtx` with a 60 s
   timeout covering the stream handshake.
5. Write the 56-byte `SnapshotHeader` as the first frame, **with `FlagSnapshot`
   set**. This flag is what routes the stream to the snapshot handler.
6. Write each `SnapshotChunk` as a subsequent frame with `FlagNone`. Only the
   header frame carries `FlagSnapshot`.
7. Close the stream (deferred).

**There is no in-stream acknowledgment.** The sender writes the chunks and
closes; it does not wait for the receiver to confirm them. Delivery is confirmed
at the raft layer instead, through the `SnapshotStatus` path described in
[Snapshot transfer sequencing](#snapshot-transfer-sequencing).

**Deadlines.** Every chunk write is preceded by
`SetWriteDeadline(now + snapshotChunkWriteDeadline)` (30 s), far longer than the
2 s message deadline because a chunk can be megabytes. Without it a stalled
`WriteFrame` would hold a `snapshotTracker` slot and make `Stop` hang forever.

**Shutdown interlock.** `cancelSnapshotWriteOnStop` arms a watcher that calls
`stream.CancelWrite` when `stopC` closes, so `Stop` does not wait out a 30 s
per-chunk deadline. The returned release func is idempotent and disarms under a
mutex, and both send paths call it **explicitly after the last chunk, before any
success bookkeeping**. Without that ordering, a `close(stopC)` landing between
the final write and the deferred release would let the watcher reset a stream
that already carried the whole snapshot, discarding the peer's buffered data
while the sender reported `OnSnapshotSent` and incremented `IncSnapshotsSent`.

**Streaming variant.** `SendSnapshotStreaming(meta, produce)` takes a
`SnapshotChunkProducer` that emits chunks one at a time through a callback, so
at most one chunk (typically 2 MB) is resident regardless of snapshot size.
`SendSnapshot`, which takes a fully materialized `[]proto.SnapshotChunk`, is the
older form and requires the whole snapshot in memory.

**Confidentiality in transit** comes from QUIC's mandatory TLS 1.3 plus mTLS.
There is no application-layer encryption of chunk payloads on top of it. At-rest
snapshot encryption is the barrier's job; see
[Encryption](../seal-barrier/architecture.md).

### Snapshot transfer sequencing

`msg.Hint` on an `InstallSnapshot` message carries `raft.snapshotSeq`, a
monotonically increasing counter local to the sending raft instance, stamped by
`sendSnapshot` and recorded on the remote's `snapshotSeq` by
`becomeSnapshot`. It is a **transfer sequence number, not a log index**.

When the `SnapshotStatus` comes back, raft accepts it only when
`rm.state == remoteSnapshot && msg.Hint == rm.snapshotSeq`; anything else is a
status for a superseded attempt and is discarded. That is what disambiguates
re-sends of the same snapshot *index*, which are otherwise indistinguishable.

`deliverSnapshotControl` (`pkg/messagehandler.go`) retries one-shot snapshot
control messages `snapshotCtrlDeliverAttempts` (5) times with a
`snapshotCtrlDeliverBackoff` (100 ms) pause, because these messages have no
protocol-level retransmission: a dropped status leaves the leader's remote
paused in snapshot state with no ack countdown armed. Full state-machine detail
lives in [Raft Protocol](../raft-consensus/engineering.md).

## Receive path

Source: `pkg/internal/transport/recv.go`

```
QUIC Listener
    |
    v
acceptLoop()  [scoped by shutdownCtx]
    |-- sem = make(chan struct{}, MaxIncomingConnections)
    |-- listener.Accept(shutdownCtx) -> conn
    |-- ipTracker.tryAcquire(ip)                  // MaxConnectionsPerIP
    |-- refuseInboundPromiscuousCert(conn)        // peer cert must not cover US
    |-- sem acquire (NON-blocking; refuse at limit)
    |-- go handleConnection(conn)
    v
handleConnection(conn)  [scoped by conn.Context()]
    |-- acquirePeerMsgBudgetRef(conn)             // per-PEER receive sub-budget
    |-- inboundConns.Store(conn, &inboundConnState{})  // reclaim policies
    |-- streamSem = make(chan struct{}, MaxStreamsPerConnection)   // data streams
    |-- snapSem   = make(chan struct{}, MaxConcurrentSnapshotRecv) // snapshot streams
    |-- loop: conn.AcceptStream(connCtx) -> stream
    |-- streamSem acquire (or connCtx done)
    |-- go handleStream(conn, stream, activity, streamSem, snapSem)
    v
handleStream(...)
    |-- loop:
    |   |-- readFrameHeaderResilient(idleTimeout=30s, activeTimeout=30s)
    |   |     |-- clean idle boundary on a LIVE conn -> errStreamIdle, keep stream
    |   |     |-- partial header -> finish under the active deadline
    |   |-- activity.touch(now)              // stamps the connection as live
    |   |-- if flags & FlagSnapshot:
    |   |       handoffToSnapSem: release streamSem, non-blocking acquire snapSem
    |   |       handleSnapshotStream, return
    |   |-- if conn.ConnectionState().Used0RTT: refuse (replayable), return
    |   |-- handleMessageFrame(conn, stream, length, flags)
    v
handleMessageFrame(...)
    |-- reserveMsgRecvBudget(decodeCharge(length))  // aggregate, length x MaxDecodeMemoryFactor
    |-- reserveConnMsgRecvBudget(conn, length)  // per-peer sub-budget, wire bytes
    |-- buf = getRecvBuf(length)             // sync.Pool
    |-- io.ReadFull(stream, payload)
    |-- if FlagCompressed: bound declared size, charge the delta, Snappy decode
    |-- batch.UnmarshalFrom(payload)         // pooled *proto.MessageBatch
    |-- reject any msg with ShardID == 0
    |-- validate DeploymentID               // cross-deployment rejection, FIRST
    |-- clear SourceAddress unless the peer cert covers it
    |-- evictStaleOutboundOnRecv(SourceAddress, SourceEpoch, conn)
    |-- validate message count <= proto.MaxMessagesPerBatch
    |-- bulk-copy every Entry.Cmd out of the pooled buffer (ONE allocation)
    |-- putRecvBuf(buf); return the Snappy buffer
    |-- handler.HandleMessage(batch, PeerAuth{Identity, AuthorizesAddress})
    v
hostMessageHandler.HandleMessage()
    |-- bind msg.From to PeerAuth.Identity (committed, pending or anchor config)
    |-- engine.DeliverMessage(msg.ShardID, msg); engine.NotifyWork(msg.ShardID)
```

**Connection limits.** `acceptLoop` enforces `MaxIncomingConnections` (default
256) via a semaphore and `MaxConnectionsPerIP` (default 16) via an
`ipConnTracker`. Both acquisitions are non-blocking: at the global limit the
connection is closed with `QUICErrConnLimitReached`, at the per-IP limit with
`QUICErrIPLimitReached`.

**Idle streams are kept open on purpose.** `StreamReadIdleTimeout` (30 s) bounds
how long a read waits *at a clean frame boundary* before re-checking the
connection's liveness. It is not a stream lifetime. A sender caches and reuses
one stream across bursts, so abandoning an idle stream on a live connection puts
the leader back to writing into a stream nobody drains: proposals stop
committing and never self-heal. Only a dead connection or a half-delivered frame
stalling past `frameReadActiveTimeout` (30 s) ends a stream. The cost is that
each accepted stream pins one goroutine and one `MaxStreamsPerConnection` slot
until its connection closes, bounding worst-case goroutines from authenticated
peers at `MaxIncomingConnections × MaxStreamsPerConnection` (65536 at the
defaults).

**Frame rejection keeps the connection.** When a frame is refused for a protocol
violation the stream is abandoned with `QUICStreamErrFrameRejected` as the
STOP_SENDING code, so the peer can tell "your frame was unacceptable" from "this
receiver is gone" and drop only the batch. `stream.CancelRead` is called on
every exit path because quic-go's `stream.Close` closes the send direction only;
leaving the receive direction open would black-hole up to
`MaxStreamReceiveWindow` of the peer's traffic while its `sendBatch` reported
success.

**Receive-budget exhaustion is transient.** `handleMessageFrame` drains the frame
body and returns `ErrMessageRecvMemoryBudgetExceeded`, and `handleStream`
*continues* rather than tearing the stream down. Tearing it down would fail the
peer's next write, evict the whole connection, and turn one peer's burst into
cluster-wide connection churn for every other peer sharing the budget.

**Peer-driven logs are sampled.** Every warning reachable from the receive path
is triggerable at frame rate by one authenticated peer, and slog's default
handler writes synchronously to stderr under a single mutex, so an unsampled
line there puts a write syscall on the raft receive path and serializes every
other log call in the process behind it. Each is gated by `LogSampled` at
`protocolViolationLogInterval` (10 s) and paired with an unconditional counter
(`recvBudgetDrops`, `zeroShardDrops`, `spoofedSourceClears`,
`snapshotStreamDrops`, `zeroRTTRefusals`, `peerFrameRejections`) so the sampled
line can carry the true total. The rate-limit stamps are per transport instance,
not package-global: many transports run in one process during tests and
in-process multi-node deployments.

**Accept-loop resilience:** Each accept iteration runs inside a deferred panic
recovery boundary (`acceptLoopIteration`). A recovered panic is logged with a
full stack trace and, before re-entering, the loop applies a
`workerPanicRestartDelay` (100 ms) backoff so a persistent panic trigger cannot
spin the CPU or flood logs. A normal (non-panicking) iteration re-enters
immediately, so accept latency is unaffected. This mirrors the backoff applied
to `sendQueueWorker` and `connectionCleanup`.

### Stream isolation (data vs snapshot)

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
control.

### Receive memory budgets

A frame's buffer is allocated from its declared length before the body is
read, so without an aggregate cap an authenticated peer can open many streams
that each declare `MaxFrameSize` and withhold the body, pinning 16 MB per stream
until the read deadline. Two budgets bound that:

- **`msgRecvBudget`** is the host-wide `MaxMessageReceiveMemory` (default
  256 MB). A frame reserves `decodeCharge(length)`, its declared length times
  `proto.MaxDecodeMemoryFactor` (8), before allocating, and releases on
  dispatch. The factor is the measured ceiling on heap per wire byte, so the
  budget bounds the heap decoded frames can occupy, not just the bytes read.
  The multiplication saturates rather than wraps.
- **`connMsgBudget`** is a per-peer sub-budget capped at `maxConnMsgRecv`
  (`MaxMessageReceiveMemory / 4`, floored at one maximum frame plus its
  decompression delta so a legal frame always fits). It is charged in wire
  bytes.

The sub-budget is keyed on the peer's authenticated node identity, not on
`*quic.Conn`. A per-connection key bounds nothing: `MaxConnectionsPerIP` is 16,
so one peer can open four connections, saturate four separate sub-budgets,
drive the global budget to zero, and have every other peer's frames dropped.
Entries are refcounted by live connection, removed when the last one closes,
and retired atomically so a concurrent acquire cannot split a peer's
accounting.

Compression is charged honestly. Snappy reaches ratios above 1000:1, so a
compressed frame reserves the compressed length up front and then reserves the
positive delta to the declared decompressed length before decoding, with
balanced deferred releases on every exit path. Charging only the compressed
length would defeat the aggregate budget.

### Entry.Cmd buffer safety

`Entry.UnmarshalFrom` is zero-copy: `Cmd` aliases the source buffer. The receive
buffer is pooled and recycled while the step worker still reads those fields
asynchronously, so any alias left behind would be read after the buffer had been
handed to another frame.

The receive path detaches every `Cmd` into **one bulk allocation** before
returning the buffer, reducing N allocations per frame to 1:

```go
totalCmdBytes := 0
for i := range batch.Requests {
    for j := range batch.Requests[i].Entries {
        totalCmdBytes += len(batch.Requests[i].Entries[j].Cmd)
    }
}
if totalCmdBytes > 0 {
    bulk := make([]byte, totalCmdBytes)
    off := 0
    for i := range batch.Requests {
        for j := range batch.Requests[i].Entries {
            cmd := batch.Requests[i].Entries[j].Cmd
            if len(cmd) > 0 {
                copy(bulk[off:], cmd)
                batch.Requests[i].Entries[j].Cmd = bulk[off : off+len(cmd) : off+len(cmd)]
                off += len(cmd)
            }
        }
    }
}
```

`Cmd` is the only such alias on a decoded `Message`. `Snapshot.Filepath` and
every membership map own their storage already (their decoders copy).

Source: `pkg/internal/transport/recv.go`, `handleMessageFrame()`

The same reasoning governs the read side that feeds `sendAppend`.
`LogReader.protoEntryBuf` (`pkg/logdb/reader.go`) is retained by `GetRange`'s
caller past the lock, so it is confined to the single goroutine that owns
`GetRange`: the step worker on a running shard, and `StartShard` before that
worker exists, which never overlap. Sharing it across goroutines is a data
race. Every other reader takes a separate path: `termLocked` has its own
`termLogdbBuf`/`termProtoBuf` and never retains a result, and `GetRangeCopy`
copies under the lock into a private buffer. See
[Engine Pipeline](../raft-consensus/engineering.md) and [WAL Storage](../log-storage/engineering.md).

### Snapshot receive

When `FlagSnapshot` is set on the first frame, the stream is routed to
`handleSnapshotStream` -> `processSnapshotHeader` -> `snapshotReceiver`.

The `SnapshotHeader` (56 bytes) identifies the snapshot and its sender:

```go
type SnapshotHeader struct {
    ShardID    uint64
    ReplicaID  uint64
    Index      uint64
    Term       uint64
    ChunkCount uint64
    Epoch      uint64
    From       uint64
}
```

All seven fields are big-endian. `Epoch` is the barrier encryption epoch the
snapshot was saved under (0 means plaintext); the receiver persists it to LogDB
so recovery can decrypt, and registers it with the barrier's `EpochTracker`
while the transfer is in flight so the key is not purged underneath it.

`From` is the sending replica. The receiver authorizes the transfer from the
header alone, through `MessageHandler.AuthorizeSnapshot(shard, replica, from,
index, peer)`, which the host answers with the same gates `HandleSnapshot`
applies, before it takes a receive slot or any memory. Authorizing only after
the transfer would let any holder of a cluster certificate, a member of no
shard, hold a peer's slots and half the snapshot memory budget for the length
of a transfer. Every chunk must then carry the header's `From` and `ReplicaID`
(as well as its shard, index and epoch), so what is persisted is what was
authorized; `HandleSnapshot` re-checks before it persists anything. A rejection
before authorization is counted under shard 0, since the shard the header names
is unverified until then. A snapshot offered to a shard that is failed or
stopping is refused as `not_accepting`, an ordinary state rather than an
authorization failure.

Source: `pkg/internal/transport/snapshot_header.go`

The `snapshotReceiver` enforces host-wide limits:

- **MaxConcurrentSnapshotReceives** (default 4): parallel inbound transfers,
  across every peer.
- **MaxSnapshotReceiveMemory** (default 1 GB): total in-flight chunk memory.
  A chunk is charged its frame length *plus* its retained
  `proto.SnapshotChunk` struct, because `receiveChunks` accumulates every
  accepted chunk for the whole transfer. Charging the frame length alone
  under-counts residency by roughly 2.6x for minimum-size chunks and lets
  concurrent transfers hold multiples of the configured budget.
- **MaxSnapshotReceiveRate** (default 256 MB/s): the receive loop paces chunk
  reads by sleeping proportional to each chunk's size (`rateLimitChunkSleep`).

...and per-peer sub-limits on top of them, because a host-global semaphore and
budget are both holdable in full by one peer, after which every legitimate
snapshot to a recovering follower is dropped and the shard cannot restore
quorum:

- `maxPerPeer` = `ceil(MaxConcurrentSnapshotRecv / 2)` concurrent transfers.
- `maxPerPeerMemory`, derived from the global budget in the same proportion
  (`perPeerSnapshotMemory`), floored so one maximum-size legal chunk always
  fits and clamped to the global budget. Without it a peer entitled to two of
  four slots could still charge the *whole* budget against either one.

Both are keyed on `snapshotFairnessKey`, derived from the peer's authenticated
certificate rather than from anything the peer picks per connection. Keying a
fairness limit on an `ip:port` makes it a formality.

**Whole-transfer liveness.** The per-frame read deadline is reset every
iteration, so on its own it bounds nothing: a peer emitting one frame header
just inside each deadline holds a concurrency slot indefinitely. Four bounds
close that:

| Bound | Value | Purpose |
|-------|-------|---------|
| `transferBudget` | derived, clamped to [`minSnapshotTransferDuration` 5 min, `maxSnapshotTransferDuration` 1 h] | Absolute deadline, scaled from the declared work at `snapshotTransferRateMultiple` (16) times the configured rate limit |
| `snapshotProgressGracePeriod` | 60 s | Grace before the throughput floor applies, absorbing stream setup and sender disk reads |
| `minSnapshotReceiveBytesPerSec` | 1024 | Average throughput required past the grace period; far below any real link, it only evicts a peer trickling bytes to hold a slot |
| `maxConsecutiveZeroLengthFrames` | 8 | A legitimate sender emits none, so a run of them is a peer burning the declared `ChunkCount` |

Every rejection routes through `rejectSnapshotStream`, which fires
`OnSnapshotRejected` and increments `IncSnapshotsRejected` with a reason from a
closed label set (`shutdown`, `concurrency_limit`, `memory_budget`, `liveness`,
`malformed`, `io_error`, `policy`, `unauthorized`, `not_accepting`). The set is
closed deliberately: the label
feeds Prometheus, and a per-transfer string would create unbounded child series.
A silent return would leave the sender waiting out its ack timeout with no local
record of why the transfer never landed.

**0-RTT snapshots are refused** under the `policy` reason, for the same replay
reason message frames are.

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

## Frame format

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
| Version | 2      | 1      | `0x01` exactly (`transport.WireVersion`) |
| Flags   | 3      | 1      | Bitfield (see below)              |
| Length  | 4      | 4      | uint32 big-endian, max 16 MB      |

Maximum frame payload: `MaxFrameSize = 16 * 1024 * 1024` (16 MB).

**Version validation:** the byte is gated on **exact equality** with
`transport.WireVersion` -- any other value, 0x00 and 0xFF alike, returns
`ErrUnsupportedVersion`. Rejecting 0x00 keeps an uninitialized buffer from
being read as a valid frame; rejecting everything above keeps a peer built from
another commit from being misparsed rather than refused. There is no accepted
range, because there is no version other than 1 to accept.

`transport.WireVersion`, like every format version in this repository, is
pinned at 1 until the first production release. A frame layout change before
then is made in place and every peer is rebuilt, so the byte does not move for
it, and two builds from different commits can disagree on layout while both
stamp `0x01`. See [Compatibility](../compatibility.md).

**Decompression limit:** Payloads are decompressed only if the decompressed
size is within `MaxDecompressedSize`, which is PINNED to `MaxFrameSize` (16 MB)
and rejected by `Validate` at any other value. It prevents decompression bombs
without being a per-node tunable: the send path admits frames against the
`MaxFrameSize` constant, so a receiver configured lower would refuse frames
every peer considers valid, and because a frame rejection deliberately keeps
the connection (see [Failed write handling](#stream-pool-and-heartbeat-streams)),
that replica would silently stop
replicating. Nothing is bought by lowering it either, since a peer wanting this
node to buffer 16 MB can simply send an uncompressed 16 MB frame.

### Frame flags

```go
// frame.go
type FrameFlags byte

const (
    FlagNone       FrameFlags = 0x00  // regular message batch, uncompressed
    FlagCompressed FrameFlags = 0x01  // Snappy-compressed payload
)

// flags.go
const (
    FlagSnapshot FrameFlags = 0x02  // opens a snapshot stream
)
```

`FlagCompressed` is set when the payload was compressed with Snappy; the
receiver decompresses before unmarshaling.

`FlagSnapshot` marks **only the first frame** of a snapshot stream, the one
carrying the `SnapshotHeader`. It is what routes the stream to
`handleSnapshotStream`. Every subsequent chunk frame on that stream is written
with `FlagNone`: both `SendSnapshot` and `SendSnapshotStreaming` call
`WriteFrame(stream, chunkBuf, FlagNone)`.

### Payload layouts

The frame payload is a `proto.MessageBatch` (or, on a snapshot stream, a
`SnapshotHeader` followed by `proto.SnapshotChunk` frames). Their byte layouts,
bounds and version gates are specified once, in
[`../wire-protocol/engineering.md`](../wire-protocol/engineering.md). The facts
this layer depends on:

- `MessageBatch.BinVer` must equal `proto.WireVersion` exactly, and
  `DeploymentID` must equal this host's; a mismatch drops the whole batch.
- `SourceAddress` and `SourceEpoch` are in-band and unauthenticated by mTLS;
  see [Per-node SANs](#per-node-sans-are-a-hard-deployment-precondition).
- A decoded `Entry.Cmd` aliases the receive buffer until the transport detaches
  it ([Entry.Cmd buffer safety](#entrycmd-buffer-safety)).
- `proto` integers are little-endian; the transport's own frame header and
  `SnapshotHeader` are big-endian.

## TLS configuration

Source: `pkg/internal/transport/tls.go`, `pkg/config/transportconfig.go`

mTLS is **mandatory** for all transport connections. There is no insecure
fallback mode. Certificate material is provided via `config.TransportConfig`:

```go
// Public API (pkg/config/transportconfig.go)
type MTLSConfig struct {
    CACert []byte  // PEM-encoded CA certificate (ALWAYS required)
    Cert   []byte  // PEM-encoded node certificate
    Key    []byte  // PEM-encoded private key

    // Certificate, when non-nil, replaces the PEM Cert/Key pair. Its
    // PrivateKey may be any crypto.Signer, including a TPM/HSM-resident
    // key that cannot be exported to PEM. CACert is still required.
    Certificate *tls.Certificate
}

type RevocationConfig struct {
    CRLPaths          []string      // Paths to CRL PEM files (hot-reload)
    OCSPResponderURL  string        // OCSP responder URL (optional)
    OCSPCacheSeconds  int           // Positive-result cache TTL (default 300)
    OCSPTimeoutSeconds int          // OCSP HTTP timeout (default 5)
    CheckInterval     time.Duration // CRL reload interval (default 60s)
    Mode              string        // "crl" | "ocsp" | "both" | "any" (default "any")
    AllowSoftFail     bool          // admit peers whose status is UNKNOWN (default false = fail closed)
    OnCertRevoked     func(serial string)
    // ... see config.RevocationConfig
}

type TransportConfig struct {
    MTLSConfig       *MTLSConfig        // Required when transport is enabled
    RevocationConfig *RevocationConfig  // Optional certificate revocation
    // ... other fields
}
```

`TransportConfig.Validate` rejects incomplete material at the config boundary
rather than letting it surface as an opaque handshake error: it requires a
CA bundle that parses, and either a `Certificate` whose `PrivateKey` is a
non-nil `crypto.Signer` or a `Cert`/`Key` pair that trial-parses as a keypair.

The `Certificate` field is what makes a TPM- or HSM-resident transport key
usable: its `PrivateKey` may be any `crypto.Signer`, so the key never has to
exist as PEM bytes. `buildMTLSTLS` parses the leaf from DER when the pre-built
certificate does not populate `Leaf`, and rejects one whose leaf cannot be parsed
at all, because the startup SAN-exclusivity precondition below cannot be evaluated
without it. This is the transport's own key, separate from the barrier root key;
see [Key Custody](../seal-barrier/architecture.md#delegated-hardware-mode-is-total-by-design) for the
at-rest side.

**TLS properties** (`buildMTLSTLS`):

- **Server config**: `ClientAuth: tls.RequireAndVerifyClientCert`, with the CA
  bundle as both `RootCAs` and `ClientCAs`.
- **Client config**: verifies the server against the same pool and presents the
  node's own certificate. `ServerName` is set per dial from the target host.
- **ALPN**: `"quicraft"`.
- **Minimum version**: TLS 1.3 (`MinVersion: tls.VersionTLS13`).
- **Curve preference**: `handshakeCurves()`, which is build-dependent:
  `[X25519, P-256, P-384]` in a standard build (`tls_curves_nofips.go`),
  `[P-256, P-384]` with `-tags fips` (`tls_curves_fips.go`). Pinning any list
  is what suppresses ML-KEM hybrid negotiation, worth roughly 5% for
  intra-cluster traffic where both endpoints are ours, and X25519 leads the
  standard list because it is the fastest ECDH curve at 128-bit security.
  X25519 is not FIPS 140-3 approved, so a FIPS build must not be able to name
  it; the NIST curves are in BOTH lists so the two builds still share a group
  and a cluster can be converted one node at a time. Do not trim either list to
  one entry: `TestHandshakeCurves_InteroperateAcrossBuilds` fails, and an
  X25519-only list fails every intra-cluster handshake under
  `GODEBUG=fips140=on`. No cipher suites are configured explicitly; TLS 1.3
  suite selection is left to `crypto/tls`. FIPS-mode *cipher* selection is a
  sealing-layer concern; see [Encryption](../seal-barrier/architecture.md).
- **Session resumption**: the client keeps a
  `tls.NewLRUClientSessionCache(256)`, cutting a reconnect from two round trips
  to one. Revocation still applies on resumption, because the callback is
  `VerifyConnection` (below).

**CA bundle parsing.** `parseCACertificates` parses *every* CERTIFICATE block in
the bundle and keeps those able to issue. TLS verification consumes the whole
bundle via `AppendCertsFromPEM`, so revocation must be able to select the same
issuer the peer's leaf was actually signed by, and in a root plus
issuing-intermediate PKI that is never the first block. A bundle with no usable
issuer is a fatal startup error (`CACertificateParseError`): OCSP requests would
otherwise name the wrong CA, which is a permanent silent no-op under soft-fail
and a total handshake outage when revocation is enforced.

**Certificate revocation.** When `RevocationConfig` is set, a `VerifyConnection`
callback is installed on both the server and client `tls.Config`.
`VerifyConnection` (rather than `VerifyPeerCertificate`) is used deliberately
because it runs on **all** connections including resumed TLS sessions, so
revocation is still enforced on session resumption.

- **Empty peer-certificate sets fail closed.** `ErrNoPeerCertificate` is
  returned rather than nil. Mandatory mTLS makes this unreachable today, but
  returning nil would encode "no cert ⇒ accept", a latent fail-open if the TLS
  config ever changed.
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
  negative cache only suppresses re-queries; it never upgrades an unavailable
  result to "good" or "revoked". Its TTL is far shorter than the positive-result
  cache (`OCSPCacheSeconds`, default 300 s) so the responder is retried promptly
  once it recovers.
- **Fail closed by default:** an unknown revocation status (OCSP responder
  unreachable, CRL stale or unreadable, no revocation source configured)
  **rejects** the connection. The knob is `RevocationConfig.AllowSoftFail`, and
  its zero value (`false`) is the fail-closed posture, so a `RevocationConfig`
  you never touch enforces revocation. Set `AllowSoftFail: true` only when the
  availability tradeoff is understood: it admits peers whose status cannot be
  determined, which lets an attacker keep using a revoked certificate simply by
  making the responder look unreachable. A definitive "revoked" verdict rejects
  either way.

  The internal `revocation.Config` carries a field of the same name and the
  same sense, so the host copies `AllowSoftFail` across rather than negating
  it. Fail-closed is therefore intrinsic to the checker: a zero-valued
  `revocation.Config` enforces no matter which code built it, and soft-fail
  has to be asked for.
- **Mode must name a source it can consult.** `Validate` rejects an unrecognised
  `Mode` (a typo like `"CRL"` or `"strict"` would otherwise silently select the
  default), requires `CRLPaths` for `"crl"`/`"both"` and `OCSPResponderURL` for
  `"ocsp"`/`"both"`, and rejects the default `"any"` mode with neither source
  configured while `AllowSoftFail` is false. That last combination admits every
  revoked certificate for the process lifetime with no error, no log and no
  metric, the exact failure revocation exists to prevent. At runtime the
  checker repeats the check for whichever mode it was given: under enforcement,
  a mode with no usable source returns an error rather than "not revoked", so a
  checker built without `Validate` cannot become a silent no-op.
- **Revocation is re-checked after admission.** `VerifyConnection` fires only at
  handshake, so `revokeCheckConnections` re-runs the verdict against every live
  inbound and outbound leaf on the 30 s cleanup tick and closes any that is no
  longer admissible with `QUICErrRevoked`. A checker *error* is treated the same
  as revoked, matching the handshake posture. Post-revocation survival is
  therefore bounded by one tick rather than by the process lifetime.
  The verdict is logged locally and deliberately kept out of the
  CONNECTION_CLOSE reason phrase, which travels to the peer verbatim.

All nodes in a cluster must share the same CA certificate for mutual
verification.

### Per-node SANs are a hard deployment precondition

Each node **must** have its own leaf certificate whose SANs name **only that
node's advertised Raft host**. This is not a hardening recommendation; it is a
correctness precondition for the two address-based controls below, and
`StartShard` **fails closed** when it is violated.

`MessageBatch.SourceAddress` and `MessageBatch.SourceEpoch` travel **in band**.
mTLS authenticates the *connection*, not those fields, so the transport binds
them to the sender's verified certificate via `inboundAuthorizedFor`
(`certs[0].VerifyHostname(host) == nil`). Two controls reduce to that answer:

1. clearing a spoofed `SourceAddress` before it reaches the handler
   (`recv.go`, which otherwise poisons the `(shardID, replicaID) -> address`
   registry),
2. deciding who may mutate `peerEpoch` and **evict a live outbound**
   (`observePeerEpoch` / `evictStaleOutboundOnRecv`).

The host's address learning (`learnFromSource` in `pkg/messagehandler.go`)
consumes the first: it records a sender's address only when the certificate
covers it.

`VerifyHostname` tests SAN **presence**, not SAN **exclusivity**. Issue the
cluster one wildcard certificate (`*.raft.svc.cluster.local`), or one
certificate carrying every node's SAN, and both answer "authorized" for
**every** peer address. A single compromised replica can then claim any peer's
address with a fresh `SourceEpoch` per batch and drive every node to evict its
healthy outbound to a **third-party** victim: an attacker-controlled,
cluster-wide replication stall, i.e. exactly the cascade the epoch mechanism
exists to prevent.

`Host.StartShard` therefore calls
`QUICTransport.VerifyPeerAddressExclusivity(RaftAddress, memberAddresses)` and
returns `transport.ErrSharedPeerCertificate` (a `*SharedPeerCertificateError`
naming the offending hosts, re-exported as `quicraft.ErrSharedPeerCertificate`
and `quicraft.SharedPeerCertificateError`) when this node's own leaf also covers another
member's host. It fails closed rather than warning because the weakness is
invisible from a running cluster, the operator has no signal to act on, and the
only remediation, re-issued certificates, has to happen before the cluster
carries data.

`StartShard`'s check is a **misconfiguration detector**, not an attack control:
it inspects the LOCAL node's own certificate against addresses an operator
handed the local API, so a malicious joiner never runs it. The enforcing
counterpart is on the receive path.

**Inbound peers with a promiscuous certificate are refused at accept.**
`refuseInboundPromiscuousCert` runs on every accepted connection, including
resumed TLS sessions, since `Accept` yields every inbound connection regardless,
and closes it with `QUICErrPromiscuousCert` (`0x5`) when the peer's verified
leaf **also covers this node's own host**. A certificate that covers this member
covers every member, so its holder can satisfy the address binding for any
replica of any shard. This node cannot enumerate the other members here (the
transport has no shard membership) and does not need to: it is itself a member,
so "covers me" is a sound proxy for "covers everyone", checkable against nothing
but its own configured address.

It runs **once per connection**, deliberately. The per-message alternative,
probing each member address through the `PeerAuth` callback, thrashes the
4-slot per-connection decision cache and trips its 16-distinct-miss fail-closed
cap (`inboundAuthorizedForCached`), which would permanently kill every
connection on any shard with five or more members. A certificate is fixed for
the life of a connection, so once is also sufficient.

**Known limitation: co-located replicas.** A certificate is matched on its
HOST only; the port is not part of the decision. Replicas separated only by port
(`127.0.0.1:5000`, `127.0.0.1:5001`) are therefore indistinguishable to both
controls, and no certificate policy can separate them. Both the startup
check and the receive-side refusal skip peers sharing this node's own host (the
same host under any spelling, or any loopback address: `isCoLocatedCertHost`),
since flagging them would fail every single-machine deployment while offering
nothing actionable. **Deployments that need these controls must give each
replica its own host.** Sender authorization is unaffected: it is by node
identity, so two hosts on one machine with separate keypairs stay distinct.

**What governs `msg.From`.** The address-based binding above governs only
`SourceAddress` learning and epoch-driven eviction. The replica ID Raft counts
toward quorum is never authorized by address: every member carries a committed
node identity, and every inbound message is bound to it. The identity is
`sha256(leaf.RawSubjectPublicKeyInfo)` (`NodeIdentityFromCertificate`, rendered
`spki:<hex>`). It is derived from the peer's public key rather than matched
against its SANs, so a wildcard certificate proves nothing about it, and it is
unchanged by certificate renewal on the same keypair. It reaches membership only
through consensus or the operator anchor (`Config.MemberIdentities`), never
captured from a connection. See
[Compatibility](../compatibility.md#node-identities-are-mandatory).

The internal transport builds separate server and client `tls.Config`
instances from the provided PEM material. The `ServerName` field on the client
is dynamically set to the target host during dial (extracted via
`net.SplitHostPort`), enabling mTLS with non-localhost targets.

### Peer-restart eviction budget

An authorized restart hint is bounded as well as authenticated. Each peer
address may drive at most `evictionBudget` (4) epoch-driven evictions per
`evictionWindow` (30 s); past that the hint is ignored and recovery degrades to
the QUIC `MaxIdleTimeout` path. A genuine restart costs exactly one eviction,
so the budget is invisible in normal operation, but without it an
authenticated peer could stamp a fresh `SourceEpoch` on every batch and, past
the 4-slot `supersededEpochRing` hysteresis, force a full QUIC handshake plus
stream-pool teardown **per batch**.

## Node registry

Source: `pkg/internal/registry/registry.go`

The registry maps `(shardID, replicaID)` pairs to network addresses. It is
a concrete type (no interface) for hot-path efficiency.

```go
type Registry struct {
    addresses sync.Map     // NodeInfo -> string
    count     atomic.Int64
}

type NodeInfo struct {
    ShardID   uint64
    ReplicaID uint64
}
```

`Resolve` runs on every message the transport sends, so the backing store is a
`sync.Map` rather than a mutex-guarded map: a lookup is an atomic pointer read
from the read-only map with no lock acquisition. Mutations take the slower
`Store`/`Delete` path, which is fine because they happen only on membership
change.

| Method                | Description                                            |
|-----------------------|--------------------------------------------------------|
| `Resolve`             | Look up an address; returns `*NodeNotFoundError` on miss |
| `Register`            | Add or overwrite an address                            |
| `RegisterIfAbsent`    | `LoadOrStore`: establish a mapping only if none exists  |
| `Remove`              | Delete an entry (no-op if absent)                       |
| `CompareAndRemove`    | `CompareAndDelete`: roll back a speculative pre-register |
| `CompareAndRegister`  | `CompareAndSwap`: restore a prior mapping on rollback   |
| `AddressInUse`        | Whether *any* shard still resolves a replica to an address |
| `ReplicasAt`          | Every `(shardID, replicaID)` currently routed to an address |
| `Count`               | Number of registered nodes                              |

The compare-and-swap variants are not conveniences. A sender the committed
configuration does not name may **establish** a mapping but must never
**replace** one, and doing that as a separate Resolve-then-Register is not
atomic: two concurrent receives for the same `(shard, replica)` both see no
entry, both register, and the loser's address silently replaces the winner's:
exactly the hijack the check exists to deny. `RegisterIfAbsent` closes that
window, leaving `Register` and `CompareAndRegister` as the only paths that can
replace an established mapping.

`AddressInUse` exists because one QUIC connection is keyed by **address** and
multiplexed across every shard talking to that host. Before a caller tears a
connection down for one shard's committed removal, it has to establish that no
other shard still resolves a replica to the same address.

The registry is populated by `Host.StartShard()` when members are registered,
and cleaned by `Host.StopShard()` when a shard is unloaded.

### Address resolution flow

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

A `Resolve` miss is not an error condition: it recurs for every message aimed at
a removed or not-yet-registered replica during membership churn, so the message
is dropped at Debug level and counted as `unresolved_target`.

## Transport configuration summary

`TransportConfig` fields (with defaults):

| Field                           | Type          | Default        | Description                          |
|---------------------------------|---------------|----------------|--------------------------------------|
| `MTLSConfig`                    | pointer       | required       | mTLS certificate material; required unless `TransportDisabled` |
| `RevocationConfig`              | pointer       | nil            | Optional CRL/OCSP revocation         |
| `StreamPoolSize`                | int           | 16             | Streams per connection; index 0 is the heartbeat stream |
| `MaxStreamPoolSize`             | int           | 256            | Validation ceiling for `StreamPoolSize` (the pool does not auto-scale) |
| `MaxStreamsPerConnection`       | int           | 256            | Concurrent inbound data-stream goroutines per connection |
| `StreamReadIdleTimeout`         | time.Duration | 30 s           | Idle re-check interval at a clean frame boundary; not a stream lifetime |
| `MaxSnapshotReceiveRate`        | int64         | 256 MB/s       | Inbound snapshot bandwidth; non-zero values below 64 KB/s are rejected |
| `MaxConcurrentSnapshotReceives` | int           | 4              | Parallel inbound snapshot transfers  |
| `MaxSnapshotReceiveMemory`      | int64         | 1 GB           | Total in-flight snapshot chunk budget |
| `MaxMessageReceiveMemory`       | int64         | 256 MB         | Aggregate in-flight message-frame heap budget (each frame charged at 8x its wire length); also derives the QUIC per-connection receive window |
| `MaxDecompressedSize`           | uint32        | 16 MB (pinned) | Decompression size limit; must equal `MaxFrameSize` |
| `MaxConnectionsPerIP`           | int           | 16             | Concurrent inbound connections per source IP |
| `MaxIncomingConnections`        | int           | 256            | Total inbound connection limit       |
| `SendBatchMaxSize`              | int           | 64 KB          | Initial capacity of pooled send buffers; it does not cap a batch |
| `UDPRecvBufSize`                | int           | 7 MB           | `SO_RCVBUF`; 0 uses the OS default   |
| `UDPSendBufSize`                | int           | 7 MB           | `SO_SNDBUF`; 0 uses the OS default   |
| `Enable0RTT`                    | bool          | false          | 0-RTT reconnection; replayable, and the receive path refuses 0-RTT frames anyway |
| `DisableCompression`            | bool          | false          | Disable Snappy compression           |
| `TransportDisabled`             | bool          | false          | Skip mTLS validation and skip building the transport (same effect as `WithoutTransport()`) |

The internal `transport.Config` adds two fields the public struct does not
expose directly: `ListenAddress` and `AdvertiseAddress`, both supplied by the
host from `HostConfig`. `AdvertiseAddress` takes priority over the OS-reported
socket address when stamping `MessageBatch.SourceAddress`, which is what lets a
node behind NAT or in a container advertise a reachable external address rather
than `0.0.0.0` or `[::]`.

## Transport metrics

The transport emits metrics through `quicraft.TransportMetrics`
(`pkg/metrics.go`) when one is configured; when nil, collection is skipped
entirely:

```go
type TransportMetrics interface {
    ObserveSendLatency(d time.Duration)
    IncMessagesSent(shardID, to uint64, msgType uint64)
    IncMessagesReceived(shardID, from uint64, msgType uint64)
    IncSnapshotsSent(shardID uint64, bytes int64, d time.Duration)
    IncSnapshotsReceived(shardID uint64, bytes int64, d time.Duration)
    IncSnapshotsRejected(shardID uint64, reason string)
    IncMessagesDropped(shardID, to uint64, reason string)
}
```

The two `reason` arguments come from closed sets defined by the transport and
must be treated as metric labels, never as free-form strings: deriving new
label values from them creates unbounded Prometheus child series.

| Counter | Reasons |
|---------|---------|
| `IncSnapshotsRejected` | `shutdown`, `concurrency_limit`, `memory_budget`, `liveness`, `malformed`, `io_error`, `policy`, `unauthorized`, `not_accepting` |
| `IncMessagesDropped` | `unresolved_target`, `queue_full`, `stream_unavailable`, `write_failed`, `marshal_failed`, `protocol_violation`, `unsendable`, `peer_rejected`, `identity_mismatch` |

`shardID` is 0 for `IncSnapshotsRejected` when the transfer failed before its
header parsed, and both `shardID` and `to` are 0 for `protocol_violation`, where
the frame is discarded before its messages are parsed.

Beyond the metrics interface, `QUICTransport` keeps unconditional atomic
counters for the sampled diagnostics (`PeerFrameRejections()` is exported;
`recvBudgetDrops`, `zeroShardDrops`, `spoofedSourceClears`,
`snapshotStreamDrops` and `zeroRTTRefusals` are reported inside the sampled log
lines), plus `ConnectionCount()`, `StreamCount()` and `SendQueueCount()`.

## Test strategy

Targets: `make test-transport`, `make bench-transport`, `make ci-fuzz-seeds`,
`make ci-fuzz-mutation`, `make ci-netfault`.

| Layer | What it covers |
|---|---|
| **Unit** (`pkg/internal/transport/*_test.go`) | frame encode/decode, the stream pool, connection admission and per-IP limits, the mTLS config builder, node-identity derivation, the revocation checker's CRL reload and OCSP cache |
| **Fuzz** (build tag `quicraft_fuzz`) | `FuzzFrameHeaderRoundTrip`, `FuzzFrameHeaderUnmarshalCrash`, `FuzzFrameWithPayload`, `FuzzMessageFrameDecode`, `FuzzHeartbeatDetection`: frame-header and payload decoding against arbitrary bytes. The transport is the one place in the repository where the input is genuinely adversarial. `make ci-fuzz-seeds` replays the seed corpora; `make ci-fuzz-mutation` runs a bounded mutation pass per target. |
| **Integration** (`test/integration`) | real QUIC connections between real hosts, with real certificates (one leaf per host from a per-cluster CA, `test/testpki`) |
| **Fault injection** (`test/netfault`, `make ci-netfault`) | directed partitions between live replicas, run over the `test/integration/e2e` suites with the `quicraft_netfault` build tag. It exercises what a process kill cannot: a live leader deposed by a quorum it can no longer reach. |

**Seams for fault injection**: `test/netfault` is consulted from the transport's
send path, and only in a build tagged `quicraft_netfault` (`netfault_on.go`); in
a normal build the check is a false constant and folds away. Drops are applied
on the sending side, so both nodes stay alive and keep their term and role.
There is deliberately no injectable dialer: a fake dialer cannot reproduce
QUIC's own loss recovery and flow control, which is what most transport bugs
are about.

**Fixtures**: certificates are generated per test run rather than checked in. A
checked-in key is a real key.

**Coverage** (measured 2026-10-02): `pkg/internal/transport` 91.4%,
`revocation` 99.0%, `registry` and `netaddr` 100%. The repository floor is
`COVER_MIN = 91`, applied to the module total.

## Observability

**Metrics.** An embedder sees the transport through the `MetricsCollector`
passed with `WithMetrics`: its `TransportMetrics` methods, plus
`SecurityMetrics.IncUnauthorizedMessageDropped` and
`IncUnauthorizedSnapshotDropped`, which the host's message handler records.
`ConnectionCount()`, `StreamCount()` and `SendQueueCount()` are methods on the
internal `QUICTransport` and are not reachable from outside the module. Metric
names and the Prometheus collector are in
[`../observability/engineering.md`](../observability/engineering.md).

The signals worth alerting on:

| Signal | Why |
|---|---|
| `IncMessagesDropped` with `queue_full` | a peer that cannot keep up shows here before it shows as a Raft timeout |
| `IncMessagesDropped` with `identity_mismatch` | a destination replica's bound identity is not what the peer at its address presents |
| `IncSnapshotsRejected` with `concurrency_limit` or `memory_budget` | snapshot transfers are being shed |
| `IncUnauthorizedMessageDropped` with `identity_mismatch` | a forged `From`, or a stale identity after a key rotation; never routine |
| `OnConnectionFailed` events | dial failures, including TLS handshake refusals and revoked peers |

**Logging** (`log/slog`). The events worth alerting on:

| Level | Message | Why |
|---|---|---|
| WARN | `refusing inbound connection: peer certificate is not exclusive` | the peer's leaf also covers this node's host (`refuseInboundPromiscuousCert`) |
| WARN | `closing connection: peer certificate is no longer admissible` | a live connection failed the periodic revocation re-check |
| WARN | `inbound frame header rejected` | bad magic, oversized frame, or a foreign wire version: two binaries from different commits (sampled) |
| WARN | `per-IP connection limit reached` | `MaxConnectionsPerIP`; the global `MaxIncomingConnections` refusal closes with `QUICErrConnLimitReached` and is not logged above Debug |
| WARN | unauthorized-message drops (host message handler, sampled) | the peer's authenticated identity is not one any configuration this replica holds binds to the claimed sender |
| INFO | `peer connection failed` | an outbound dial failed: unreachable UDP path, TLS refusal (wrong CA, SAN, revoked) or a peer presenting the wrong identity (`DialIdentityError`) |
| ERROR | `transport: dropping message that cannot be framed; ...` | a producer emitted a message no peer can accept (`unsendable`) |

**No certificate private key, no session secret, and no message payload appears
in any log line.** A refused peer is identified by its SPKI identity and its
address, both of which are already public.

## Cross-references

- [Architecture Overview](../multi-group-host/architecture.md) -- system overview, data flow, configuration
- [Wire Format](../wire-protocol/engineering.md) -- MessageBatch, Message, Entry and Membership encodings
- [Raft Protocol](../raft-consensus/engineering.md) -- snapshot state machine, `SnapshotStatus` sequencing, replication
- [Engine Pipeline](../raft-consensus/engineering.md) -- step workers, ready loop, LogReader buffer ownership
- [Encryption](../seal-barrier/architecture.md) -- barrier epochs, at-rest snapshot and WAL sealing
- [Compatibility](../compatibility.md) -- wire-version pinning, node identities
- [Session Deduplication](../client-sessions/engineering.md) -- Entry session fields and at-most-once delivery
- [Deployment](../multi-group-host/user.md) -- certificate issuance, per-node SAN requirements
