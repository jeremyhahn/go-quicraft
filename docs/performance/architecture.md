# Performance Architecture

This document describes the performance techniques implemented in QuicRaft, covering zero-allocation hot paths, compression, rate limiting, memory budget enforcement, and transport-level optimizations.

**On the numbers in this document.** Allocation counts (`B/op`, `allocs/op`) are
deterministic for a given build and are quoted here without qualification.
Timings and CPU percentages are not: absolute `ns/op` is a property of the
machine, and the few quoted below predate any provenance discipline in this
repository, so the hardware that produced them was not recorded. They are
flagged where they appear. New measurements belong in
[benchmarking.md](benchmarking.md), which records machine, governor, load, Go
version and commit alongside every sample.

## Hot Path Overview

The critical path for a proposal flows through every layer of the system:

```
Propose(cmd) -> EntryQueue[T].Add -> Step Worker -> Peer.Propose -> Peer.GetUpdate
  -> CommitWorker -> LogDB.write -> fsync
  -> Transport.Send -> QUIC stream write
  -> ApplyWorker -> StateMachine.Update -> Response
```

Each layer is designed for zero or near-zero allocations in steady state.

## Zero-Allocation Techniques

### Entry Serialization (Zero-Copy)

The `proto.Entry` type uses a custom fixed-layout binary format with a 68-byte header (8 uint64 fields + 1 uint32 CmdLen). Serialization avoids reflection, protobuf, and dynamic allocation.

`MarshalTo(buf []byte)` writes directly into the caller's buffer using `binary.LittleEndian` and `copy()`. `UnmarshalFrom(buf []byte)` reads fields in place and zero-copy aliases `Entry.Cmd` into the source buffer. No allocation occurs for the payload. The receiver must copy `Cmd` before reusing the buffer.

Source: `pkg/proto/entry.go`

### Per-Worker Commit Slots

The step worker maintains a `commitSlots` map (`map[uint64]*commitItem`) with pre-allocated `commitItem` structs per shard. When sending an Update through the commit channel, the worker reuses the existing slot rather than allocating a new struct. The pointer is sent through `commitC`, avoiding value copies of the Update struct on the channel hot path.

Source: `pkg/internal/engine/step.go`

### Double-Buffered Message Slices

The raft state machine uses a double-buffer swap for outbound messages. Two pre-allocated slices (`msgs` and `msgsBuf`) alternate roles: one accumulates messages during a tick cycle while the other is drained by `GetUpdate`. The swap avoids allocation. Both slices are pre-allocated at raft initialization with capacity equal to the number of peers.

Source: `pkg/internal/raft/raft.go`

### Pre-Allocated Sort Buffer

The `tryCommit()` function requires a sorted slice of match indices to determine the commit quorum. A pre-allocated `matched` slice on the raft struct is reused each cycle, avoiding per-commit allocation.

Source: `pkg/internal/raft/raft.go`

### Reusable Inbox Buffer

The step worker reuses a single `inboxBuf []proto.Message` slice across all shard inbox drains. `DrainInbox()` resets the slice to `[:0]` and appends, reusing the backing array.

Source: `pkg/internal/engine/step.go` (field and buffer), `pkg/internal/engine/node.go` (`DrainInbox`)

### Transport Buffer Pools

The QUIC transport uses `sync.Pool` for frame marshal buffers. The `getBuf()` and `putBuf()` methods manage pooled `*[]byte` pointers. Buffers exceeding 256 KB (`maxPoolBufSize`) are not returned to the pool to prevent unbounded pool growth. `Send()` no longer groups by target into a pooled map — grouping moved into the per-target send queues (see [Send Path](#send-path)).

Source: `pkg/internal/transport/quic.go`

### ReadIndex Zero-Allocation Tracking

Heartbeat acknowledgments for ReadIndex quorum confirmation use a `uint64` bitfield instead of a map. Each voting replica is assigned a bit position (0..63) via `replicaToBit`, built in `becomeLeader()` and rebuilt by `rebuildReplicaToBit` when membership changes mid-term. Ack processing is a single bitwise OR and increment -- zero allocation, O(1) per response.

Source: `pkg/internal/raft/raft.go`

### SwapBuffer Queue (not currently on any hot path)

The `SwapBuffer[T]` generic queue uses a lock-protected write buffer that is swapped with the previously returned read buffer, so a fresh slice is allocated only on the first `Swap`. The caller owns the returned slice until the next `Swap` recycles it. The backing array grows as needed but is never shrunk during normal operation.

`git grep SwapBuffer -- '*.go'` returns hits only inside `pkg/internal/queue` and its own tests. Nothing in the engine, raft, transport, or host layer uses it. It is described here because it exists, not because it carries traffic; the proposal path uses `EntryQueue[T]` (see [Double-Buffered MPSC Proposal Queue](#double-buffered-mpsc-proposal-queue-o19)).

Source: `pkg/internal/queue/swapbuffer.go`

## Compression Strategy

### Entry Compression (Snappy)

When `Config.EntryCompressionType` is set to `Snappy`, entry `Cmd` payloads are compressed at proposal time on the leader before appending to the log. Only `EntryNormal` entries are compressed; config changes remain uncompressed for universal readability.

Compression and decompression are handled by `CompressEntry()` and `DecompressEntry()` in the raft package. Incompressible data (compressed output >= original size) is left uncompressed. The `EntryCompressFlag` is OR'd into the `Type` field, preserving the base entry type.

**Decompression safety**: `DecompressEntry()` accepts a `maxSize` parameter to reject decompression bombs -- crafted payloads that claim a small compressed size but decompress to gigabytes.

Source: `pkg/internal/raft/compress.go`

### Transport-Level Compression

The transport layer applies Snappy compression to serialized `MessageBatch` payloads independently of entry-level compression. This compresses metadata, headers, and any uncompressed entries in the batch. Compression is only used when it reduces the payload size. The `FlagCompressed` bit in the frame header signals the receiver to decompress before deserialization.

Source: `pkg/internal/transport/quic.go`

### Where Compression Occurs

| Stage | What | Direction |
|---|---|---|
| Leader proposal (`handleLeaderPropose`) | Entry.Cmd (Snappy) | Compress |
| WAL write | Stores compressed bytes | Pass-through |
| Transport send (`sendBatch`) | MessageBatch payload (Snappy) | Compress |
| Transport receive | MessageBatch payload | Decompress |
| Apply worker | Entry.Cmd before state machine | Decompress |

When both compression and encryption are enabled, the order is compress-then-encrypt.

## Rate Limiting

### Per-Shard Rate Limiter (InMemRateLimiter)

Each shard maintains an `InMemRateLimiter` that tracks the in-memory Raft log size and follower feedback. The limiter uses hysteresis to prevent oscillation:

- **Trigger**: any tracked size (local or follower-reported) exceeds 100% of `MaxInMemLogSize`
- **Release**: all tracked sizes drop below 70% of `MaxInMemLogSize`
- **Debounce**: state transitions require at least `ChangeTickThreshold` (10) ticks between changes

Every mutation runs on the raft state machine goroutine, so the accounting needs no lock. The read side is not lock-free: `IsLimited()` is called from other goroutines and reads a `cachedLimited` atomic that the owning goroutine publishes. Follower feedback is sent via `RateLimit` messages every election timeout. The leader records each follower's reported in-memory log size and considers it when evaluating rate limit thresholds. Stale follower entries are garbage collected after `gcTick` (3) ticks without an update.

Source: `pkg/internal/server/rate.go`

### Global Rate Limiter (RateLimiter)

A host-level `RateLimiter` tracks the sum of all per-shard in-memory log sizes. When the sum exceeds `HostConfig.MaxTotalInMemLogSize`, all shards enter rate-limited mode. This prevents OOM from moderate load distributed across many shards (e.g., 1000 shards at 200 MB each = 200 GB total).

The global limiter uses lock-free atomics with CAS-loop decrement to prevent unsigned underflow.

Source: `pkg/internal/server/rate.go`

### Backpressure Flow

Every layer in the pipeline has a bounded queue or channel. When any layer is slow, backpressure propagates upward automatically:

```
Client (Propose)
  |
  v
EntryQueue[T].Add() -----> full? --> return ErrBusy
  |
  v
RateLimiter.IsLimited() --> yes? --> return ErrBusy
  |
  v
Step Worker --> Peer.Propose()
  |
  v
CommitWorker channel --> full? --> step worker blocks
  |
  v
LogDB.SaveState() --> fsync latency limits throughput
  |
  v
ApplyWorker channel --> full? --> commit worker blocks
  |
  v
StateMachine.Update() --> SM processing time limits throughput
  |
  v
RateLimiter.Decrease() --> release pressure
```

The only explicit check is `RateLimiter.IsLimited()` at the Propose entry point. Everything else is implicit via bounded buffers and channels.

### Configuration

| Parameter | Scope | Default | Description |
|---|---|---|---|
| `Config.MaxInMemLogSize` | Per-shard | 0 (disabled) | In-memory log size threshold for rate limiting |
| `HostConfig.MaxTotalInMemLogSize` | Global | 0 (disabled) | Cross-shard aggregate memory threshold |

Source: `pkg/config/config.go`, `pkg/config/hostconfig.go`

## ReadOnly Bypass

ReadIndex requests that produce no entries, state changes, or snapshots skip the commit/apply pipeline entirely. `Peer.GetUpdate()` sets `Update.ReadOnly = true` when all persistence-related fields are empty. The step worker checks this flag and continues to the next shard without sending the update to the commit channel.

This eliminates three goroutine hops (step -> commit -> apply -> callback) and the associated LogDB write for pure read operations. ReadyToRead states are delivered inline via the `OnReadyToRead` callback during `processReady()`, before the ReadOnly check.

**Performance impact**: ReadIndex latency drops by orders of magnitude, from multiple goroutine hops plus a LogDB write to a single inline callback.

Source: `pkg/internal/engine/step.go`, `pkg/internal/raft/peer.go`

## Replication Chunking

`sendAppend()` bounds the total entry bytes in a single Replicate message using `maxReplicationPayload`, set from `Config.MaxApplyEntrySize` (default `config.MaxReplicationPayloadBytes` = 16,711,672 bytes, ~15.9 MiB). This prevents unbounded message sizes when a follower is far behind. The follower acknowledges each batch via `ReplicateResp`, which triggers the leader to send the next chunk via `sendAppend()` in `handleAppendResp()`. This provides natural flow control without explicit windowing.

**Configuration**:

| Parameter | Default | Description |
|---|---|---|
| `Config.MaxApplyEntrySize` | 16,711,672 (~15.9 MiB) | Maximum Replicate message payload size |
| `Config.MaxEntrySize` | 8 MB | Maximum single entry payload size |

Source: `pkg/internal/raft/raft.go`

## ReadyMap

The `ReadyMap` is a double-buffered notification map that signals which shards
have pending work.

### Structure

One `sync.Mutex` guards one live map, `ready`. `maps [2]` holds the two maps that
alternate as the live one; `index` says which of the pair is currently live.
`NewReadyMap` allocates three maps, not two: the initial `ready` is a third map
that the first `Swap` hands to the caller and never reinstalls. From the second
swap onward the rotation is strictly between the pair.

`Set(shardID)` takes the mutex, inserts, and releases it. The critical section is
a single map insert, which is why one lock suffices: contention is on the *lock*,
not the map, and the hold time is short enough that the mutex never becomes the
bottleneck.

`Swap()` clears the alternate map outside the lock (the caller owns it), then
takes the lock only long enough to exchange the two pointers, and returns the
map that was live. This is O(1) — the caller iterates the returned map with no
lock held. `Swap()` is NOT safe for concurrent use: it must be called from the
single owning step worker.

The map returned by `Swap()` is owned by the caller only until the NEXT `Swap()`,
which recycles it via `clear()`. Retaining it past that point reads a map another
goroutine is refilling.

Source: `pkg/internal/engine/ready.go`, `pkg/internal/engine/signal.go`

## Cache-Line Padding

The `commitPending` and `lastApplied` fields on `engine.Node` are padded to 128 bytes each to prevent false sharing between the step worker (reader) and apply worker (writer). On modern x86 and ARM processors, cache lines are 64 bytes; padding to 128 bytes provides safety for adjacent-line prefetch pairs. `commitPending` is set by the step worker after queuing an Update, and cleared by the apply worker after applying entries. `lastApplied` is written by the apply worker and read by the step worker to determine which committed entries to include in the next `GetUpdate()` call. Without padding, these fields would share a cache line, causing cross-core invalidation traffic on every update.

Source: `pkg/internal/engine/node.go`

## Connection Pooling and Stream Reuse

### QUIC Connection Pool

The transport maintains a `map[string]*quic.Conn` of active connections, protected by an `RWMutex`. Connection establishment is coalesced via `singleflight.Group` to prevent redundant dials when multiple goroutines attempt to connect to the same target simultaneously. A background `connectionCleanup()` goroutine runs every 30 seconds, scanning for connections whose context is done and evicting them along with all associated pooled streams.

### Stream Pool

QUIC streams are pooled and reused across messages. The pool is sharded across 16 buckets (`streamShardCount = 16`) to reduce lock contention.

Stream routing separates heartbeats from data messages:
- **Heartbeats**: stream index 0 (dedicated, low-latency path)
- **Data messages**: `(shardID % (poolSize - 1)) + 1`

This ensures heartbeats are never blocked by large Replicate messages on the same stream.

Stream creation uses double-checked locking: the fast path acquires a read lock to check for an existing stream, and the slow path opens a new stream under a write lock with a re-check to handle races.

Eviction is scoped to the failure. A peer that rejects a frame costs only that stream: `evictStream()` removes the single entry and leaves the connection and its other streams alone. A write failure or a dead connection goes wider, and `evictStreamsForTarget()` bulk-removes every stream for that target so subsequent calls open fresh streams on a new connection rather than reusing stale handles.

### Send Path

`Send()` does not group or fan out. It walks the messages once, deep-copies each message's entries and snapshot references (breaking the alias with the step worker's reusable `proposeBuf`), resolves the target address from the registry, and enqueues the message on that target's `sendQueue` — a non-blocking hand-off to a per-target channel pair (data, cap 2048; heartbeat, cap 64).

Batching happens in the per-target `sendQueueWorker` goroutine, which drains its queues and coalesces up to 16 heartbeats or 256 data messages into one batch. Heartbeats are drained before data by a non-blocking priority select, so a replication backlog cannot delay heartbeats past the election timeout. Each batch is framed (`FrameHeaderSize` + payload) and optionally Snappy compressed. A batch that fits in one frame is one write to the pooled stream; `sendBatch` splits an oversized batch across as many frames as it needs, one write each. A worker exits after `sendQueueIdleTimeout` (60s) of inactivity, so removed peers do not leak goroutines.

See [QUIC Transport: Send Path](../transport/quic.md#send-path) for the full sequence.

Source: `pkg/internal/transport/quic.go`, `pkg/internal/transport/conn.go`

## Single-Owner Pipeline Invariant

The engine enforces a single-owner invariant on the `Peer` struct across worker tiers using the `commitPending` atomic flag. While `commitPending` is true:

- The step worker skips the shard in `tick()` and `processReady()`
- The commit worker owns the Peer for `Commit()`
- The apply worker owns the Peer data for `Apply()`

This eliminates the need for mutexes on the Raft state machine itself. The step worker does not re-enqueue skipped shards via `WorkSignal` to avoid a tight spin loop that would starve downstream workers.

Source: `pkg/internal/engine/step.go`

## Function Pointer Dispatch

Role-specific message handling and tick functions are assigned as function pointers during role transitions, providing zero-branch dispatch. The common `step()` method handles term-based transitions and vote requests, then delegates to `r.stepFunc()` for role-specific logic. This avoids a switch statement on every message.

Source: `pkg/internal/raft/raft.go`

## Hot Path Optimizations

### P0: LogReader termRingBuffer

The `LogReader` maintains a term cache so that `Term(index)` lookups avoid hitting LogDB. The original implementation used `map[uint64]uint64`, which required iterating up to 100K entries in `trimTermMap()` on every `appliedLogTo` call. A profile taken at the time attributed 82% of CPU to it under concurrent load; the machine was not recorded, so read that as "it dominated the profile", not as a portable figure.

The replacement `termRingBuffer` is a fixed-capacity circular buffer of `(index, term)` pairs held in a single `[]termRingEntry` slice of `maxTermCacheEntries` (100,000) elements, allocated once at 1.6 MB. Entries are appended at the tail and the buffer wraps when full, evicting the oldest entries automatically. Operations are:
- **Append**: O(1) -- write at `(head + count) % cap`, increment count
- **Lookup**: O(log n) -- binary search over the logical window
- **trimTo**: O(log n) -- binary search to find the cut point, advance head

Source: `pkg/logdb/reader.go`

### P1: inMemory Lazy Compaction

The `inMemory` struct in the raft log tracks entries pending persistence and application. A `wastedPrefix` counter tracks how many entries have been logically truncated without physically reallocating the slice.

Compaction requires *both* thresholds to be met: `wastedPrefix >= entrySliceSize` (512) and `wastedPrefix >= 2*len(entries)`. Requiring twice the live length means the copy is amortized against at least as much dead space as live data. A separate fast path releases the backing array outright when no live entries remain.

Source: `pkg/internal/raft/inmemory.go`

### P2: ReadyMap Two-Map Rotation

The `ReadyMap.Swap()` method returns a `map[uint64]struct{}` of notified shards. Two maps (`maps [2]`) alternate as the live one: the map handed to the caller last swap is `clear()`ed and installed as the new drain target on the next swap. This achieves zero steady-state allocations for the Swap path, at the cost of a one-swap ownership window on the returned map.

Source: `pkg/internal/engine/ready.go`

### P3: Host.nodes sync.Map

The `Host` struct's node registry was protected by `sync.RWMutex` over a plain `map[uint64]*nodeState`. Replacing with `sync.Map` provides lock-free reads on the hot path. A separate `activeNodes atomic.Int64` counter provides O(1) shard counting without iterating the map.

Source: `pkg/host.go`

## Engine Pipeline Optimizations

### Memory-Mode Commit Bypass (O5)

When `LogDB` is nil (memory-only mode), the step worker inlines `Peer.Commit()` and forwards directly to `applyC`, skipping the commit worker goroutine entirely. This eliminates one channel send/receive pair per update.

Source: `pkg/internal/engine/step.go`

### ReadIndex Heartbeat Batching (O6)

Multiple concurrent ReadIndex requests that arrive in the same step worker inbox drain share a single heartbeat broadcast round. The raft layer defers `broadcastHeartbeat()` during ReadIndex processing and flushes once after all inbox messages are handled. Each heartbeat carries a `ctx` field that correlates with the ReadIndex request. Responses are matched to the correct ReadIndex by context.

Source: `pkg/internal/raft/raft.go`

### Parallel Per-Target Sends (O17)

Sends to different peers proceed concurrently. The mechanism is no longer a goroutine spawned per `Send()` call: each target has a long-lived `sendQueueWorker` goroutine draining its own queue, so `Send()` only enqueues and returns, and the send phase costs `max(latencies)` rather than `sum(latencies)` without any per-call goroutine creation. A worker is created on first use for a target and retired after `sendQueueIdleTimeout` of inactivity.

Source: `pkg/internal/transport/quic.go`

### LogDB Group Commit (O18)

The commit worker uses a flat `[]logdb.Entry` buffer that is sub-sliced across all updates in a batch, reducing allocation from O(N) per-update slices to O(1) in steady state. Benchmarks show 0 B/op and 0 allocs/op in steady state for both small (4 updates, 8 entries) and large (64 updates, 1024 entries) batches.

Sources: `pkg/internal/engine/commit.go`, `pkg/internal/engine/convert.go`

### Double-Buffered MPSC Proposal Queue (O19)

Proposals now bypass the Go buffered channel (`inboxC`) and use a dedicated `EntryQueue[T]` double-buffered MPSC queue. It is guarded by a `sync.Mutex`, not lock-free; the win is amortization, not lock elimination. The queue has bounded capacity (`maxLen=256`) with backpressure -- `Add()` returns false at capacity, signaling the caller to return `ErrSystemBusy`. The double-buffer swap means producers hold the lock only for a slice append, and the consumer acquires the lock once per drain cycle. This eliminates channel serialization under 8+ concurrent writers.

Sources: `pkg/internal/queue/entryqueue.go`, `pkg/internal/engine/node.go`, `pkg/internal/engine/step.go`

### LeaseRead (O20) -- Raft PhD Section 6.4

When `Config.LeaseRead = true`, the leader tracks per-remote heartbeat ack timestamps in `remoteLastAck map[uint64]int`. When the lease is valid, ReadIndex requests are served immediately without a heartbeat round-trip. When the lease has expired, the standard ReadIndex path (heartbeat quorum confirmation) is used as a fallback.

Sources: `pkg/internal/raft/raft.go`, `pkg/config/config.go`

### Persistent Heartbeat Streams (O21)

`dialConnection()` spawns `preOpenHeartbeatStream()` asynchronously after establishing a QUIC connection. This pre-opens a QUIC stream at index 0 (the heartbeat stream slot) and stores it in the sharded stream pool. This removes the `OpenStreamSync` round-trip from the first heartbeat send, which sits on the critical path for ReadIndex and liveness detection. The cost it saves was measured once at 20-40us on unrecorded hardware; the shape of the saving (one stream-open round-trip) is the durable claim, not the figure.

Source: `pkg/internal/transport/conn.go`

## Benchmark Results

For pairwise comparisons against Dragonboat v4, etcd/raft and OpenRaft, and for
per-scenario breakdowns, see [test/performance/README.md](../../test/performance/README.md).
That file states which machine and which date each set of numbers came from, and
which comparisons between them are legitimate. Two datasets exist and they are
not interchangeable.

Changes to the hot paths described above must be benchmarked before merge. See [Benchmark Regression Testing](benchmarking.md) for `make bench-compare`, the stored baselines under `test/performance/baselines/`, and why a single `make bench-<pkg>` run cannot answer the question.

## Allocation Budget Summary

The 0 allocs/op target applies to the uncompressed, steady-state hot path with warm pools. With compression enabled, the realistic budget is 1-3 allocs/op.
