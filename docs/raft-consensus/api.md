# Raft Consensus — API

> **Status: NORMATIVE** · Scope: go-quicraft `pkg/internal/raft`,
> `pkg/internal/engine`, and the `Host` methods that drive them ·
> Last verified: 2026-09-15
> Conflicts with this document are defects. Fix them here first.

## Transport coverage

go-quicraft is a library. **The Go API is the API.**

| Transport | This feature |
|---|---|
| **Embedded (Go call)** | the `Host` methods below. This is the transport. |
| **QUIC** | Raft messages between replicas of one shard, as `proto.MessageBatch` frames. A peer protocol, **not** a client-facing service — there is no "propose" RPC an external caller can issue. |
| Unix socket / REST / gRPC / MCP | n/a — this repository serves none of them |

Not missing-capability rows. This feature **is** the go-quicraft capability row
covering Raft consensus, log replication and QUIC transport, which the portfolio
register marks as consumed by implementing interfaces rather than by calling a
client.

## There are no exported Raft types

`Peer`, `Node`, `ReadyMap`, `EntryQueue` and the rest live in `internal/`
packages and are not importable. That is deliberate: their shape is not a
compatibility surface, and the exposed host surface is `StartShard`,
`GetLeaderID` and replica add/remove — **nothing more**.

What follows is the `Host` surface that drives consensus. Shard lifecycle and
the full `Host` vocabulary are in
[`../multi-group-host/api.md`](../multi-group-host/api.md); membership is
[`../membership-changes/api.md`](../membership-changes/api.md).

## Go API

Import: `github.com/jeremyhahn/go-quicraft/pkg`

### Capability: propose a command

```go
func (h *Host) Propose(ctx context.Context, shardID uint64, cmd []byte) (*RequestState, error)
func (h *Host) SyncPropose(ctx context.Context, shardID uint64, cmd []byte) (sm.Result, error)
```

| | |
|---|---|
| Request | shard and command; `len(cmd) <= min(MaxProposalPayloadSize, MaxEntrySize)` |
| Response | a `*RequestState` to await, or the `sm.Result` directly |
| Errors | `NotLeaderError`, `ErrBusy` (`*BusyError`), `*EntryTooLargeError`, `ErrDiskFull`, `*MaybeAppliedError`, `ErrInvalidOperation` on a witness, context deadline |
| Permission | none in this library |

Rejected fast, before any work: a done context, a closed or draining host, an
unknown shard, a witness, a full disk, an oversized command, and **no leader
known at all** — which returns `NotLeaderError` immediately rather than waiting
for a timeout.

Session-bearing variants are in
[`../client-sessions/api.md`](../client-sessions/api.md).

### Capability: read

```go
func (h *Host) SyncRead(ctx context.Context, shardID uint64, query interface{}) (interface{}, error)
func (h *Host) ReadIndex(ctx context.Context, shardID uint64) (*RequestState, error)
func (h *Host) QueryLocalNode(ctx context.Context, shardID uint64, query interface{}) (interface{}, error)
func (h *Host) StaleRead(ctx context.Context, shardID uint64, query interface{}) (interface{}, error)
func (h *Host) FollowerRead(ctx context.Context, shardID uint64, query interface{}) (interface{}, error)
func (h *Host) SyncReadBuf(ctx context.Context, shardID uint64, key []byte) (result []byte, release func(), err error)
func (h *Host) StaleReadBuf(ctx context.Context, shardID uint64, key []byte) (result []byte, release func(), err error)
```

| Method | Guarantee |
|---|---|
| `SyncRead`, `ReadIndex` | **linearizable** — a confirmed read index, then the state machine |
| `StaleRead`, `QueryLocalNode` | local applied state, no guarantee |
| `FollowerRead` | a follower's applied state, which may lag |

| | |
|---|---|
| Errors | `NotLeaderError`, `ErrBusy`, `ErrInvalidOperation` on a witness (it stores no log data), context deadline |
| Permission | none |

The `*Buf` forms require your state machine to implement `sm.NALookup` /
`sm.NALookupInto` and return a buffer plus a `release` function instead of
allocating. **Call `release`.**

Concurrent linearizable readers on one shard share a single heartbeat round; see
[`architecture.md`](architecture.md).

### Capability: inspect consensus state

```go
func (h *Host) GetLeaderID(shardID uint64) (uint64, bool, error)
func (h *Host) GetLeaderIDAndTerm(shardID uint64) (uint64, uint64, bool, error)
func (h *Host) GetShardInfo(shardID uint64) (*ShardInfo, error)
func (h *Host) GetLogRange(shardID uint64) (LogRange, error)
func (h *Host) GetLogReader(shardID uint64) (ReadonlyLogReader, error)
func (h *Host) QueryRaftLog(ctx context.Context, shardID, firstIndex, lastIndex uint64, ...) ([]proto.Entry, error)
func (h *Host) StateMachineHash(shardID uint64) (hash uint64, appliedIndex uint64, ok bool, err error)
func (h *Host) HealthCheck() *HealthStatus
func (h *Host) IsShardJoining(shardID uint64) bool
```

`StateMachineHash` returns `ok == false` when the state machine does not
implement `sm.Hasher`.

### Capability: diagnostics

```go
func (h *Host) DiagRaftState(shardID uint64) (term, committed, processed, lastIdx uint64, msgs int, state string, err error)
func (h *Host) DiagPipelineState(shardID uint64) (commitPending bool, proposalQLen, inboxLen, pendingProposals int, ...)
func (h *Host) DiagShardGates(shardID uint64) (recovering, saving bool, applyInFlight int64, err error)
func (h *Host) DiagRemotes(shardID uint64) (string, error)
```

For incident response. Not a monitoring interface — their signatures are
diagnostic and may change.

### Capability: transfer leadership

```go
func (h *Host) RequestLeaderTransfer(ctx context.Context, shardID, targetReplicaID uint64) error
```

### `RequestState` and error classification

```go
type RequestState struct{ ... }   // await via its result channel
type RequestResult struct{ ... }
func NewRequestStatePool() *sync.Pool

func NotLeader(err error) (replicaID uint64, ok bool)
func Temporary(err error) bool
func MaybeApplied(err error) bool
```

`NotLeader` extracts the leader to redirect to, when one is known.
`Temporary` reports whether a retry is reasonable.

**`MaybeApplied` is the one to handle deliberately.** It reports that the
proposal may or may not have been applied — the honest answer after certain
failures. A caller that treats it as "failed" and retries a non-idempotent
command applies it twice; that is what
[`../client-sessions/`](../client-sessions/user.md) exists for.

### Error vocabulary

Sentinels: `ErrBusy`, `ErrInvalidOperation`, `ErrDiskFull`,
`ErrTooManyWitnesses`, `ErrEntropyDegenerate`, `ErrSessionsUnsupportedOnDisk`,
`ErrInvalidShardHealth`, `ErrNonVotingRoleInMembers`.

Typed: `*NotLeaderError`, `*BusyError`, `*MaybeAppliedError`,
`*EntryTooLargeError`, `*ReplicaNotFoundError`, `*ShardNotStoppedError`,
`*LoopPanicError`, `*TypeAssertionError`, `*UnauthorizedMessageError`,
`*CorruptConfigEntryError`, `*BootstrapEntryMissingError`,
`*StaleConfigChangeError`, `*HostInitError`, `*InvalidShardHealthError`.

All match with `errors.Is` / `errors.As`.

### Notifications

`config.EventListener` carries the user-visible callbacks. The cold-path ones —
`OnLeaderUpdated`, `OnCampaignLaunched`, shard failure — are dispatched on a
**dedicated goroutine that holds no engine lock**, so a listener may call any
`Host` API including the synchronous `StopShard`. The hot-path ones —
`OnApplied`, `OnCommitted`, `OnReadyToRead`, `OnProposalFailed`,
`OnReadIndexBatched`, `OnReadIndexFailed` — stay synchronous because they
resolve pending client requests, and **there is deliberately no ordering
guarantee between the two sets**.

Payload types: `LeaderInfo`, `ShardFailedInfo`, `SnapshotInfo`,
`ConnectionInfo`, `DiskInfo`, `EventShardInfo`.

## Configuration

The full table is in [`engineering.md`](engineering.md) §Configuration. The
three that change behavior rather than performance:

| Field | Default | Note |
|---|---|---|
| `CheckQuorum` | `*bool`, nil = **enabled** | leader steps down without quorum |
| `PreVote` | `*bool`, nil = **enabled** | stops a partitioned node disrupting a healthy term |
| `LeaseRead` | `false` | **requires `CheckQuorum` and `ElectionRTT >= 5`**; `ErrLeaseReadRequiresCheckQuorum` otherwise |

The `*bool`s are pointers so the zero value means "unset", not "off".

## Versioning and deprecation

- The internal packages carry **no compatibility promise** and are not
  importable.
- The `Host` methods above are the stable surface; a signature change breaks
  every consumer in the portfolio and is sequenced bottom-up.
- Raft messages on the wire are `proto.MessageBatch`, gated on
  `BinVer == proto.WireVersion` by exact match. See
  [`../wire-protocol/api.md`](../wire-protocol/api.md).
- Nothing here is deprecated.
