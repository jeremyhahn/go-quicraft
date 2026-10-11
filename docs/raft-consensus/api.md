# Raft consensus: API

> **Status: NORMATIVE** · Scope: go-quicraft `pkg/internal/raft`,
> `pkg/internal/engine`, and the `Host` methods that drive them ·
> Last verified: 2026-10-11
> Conflicts with this document are defects. Fix them here first.

## Transport coverage

go-quicraft is a library. The Go API is the API.

| Transport | This feature |
|---|---|
| Embedded (Go call) | the `Host` methods below |
| QUIC | Raft messages between replicas of one shard, as `proto.MessageBatch` frames. A peer protocol, not a client-facing service: there is no "propose" RPC an external caller can issue. |
| Unix socket / REST / gRPC / MCP | n/a: this repository serves none of them |

These are not missing-capability rows. This feature is the go-quicraft
capability row covering Raft consensus, log replication and QUIC transport,
which the portfolio register marks as consumed by implementing interfaces
rather than by calling a client.

## There are no exported Raft types

`Peer`, `Node`, `ReadyMap`, `EntryQueue` and the rest live in `internal/`
packages and cannot be imported. Their shape is not a compatibility surface.
Consensus is driven only through `pkg.Host`.

This page lists the `Host` methods that drive consensus. Shard lifecycle and
the rest of the `Host` vocabulary are in
[`../multi-group-host/api.md`](../multi-group-host/api.md); membership is in
[`../membership-changes/api.md`](../membership-changes/api.md).

## Go API

Import: `github.com/jeremyhahn/go-quicraft/pkg` (package `quicraft`).

### Capability: propose a command

```go
func (h *Host) Propose(ctx context.Context, shardID uint64, cmd []byte) (*RequestState, error)
func (h *Host) SyncPropose(ctx context.Context, shardID uint64, cmd []byte) (sm.Result, error)
```

| | |
|---|---|
| Request | shard and command; `len(cmd) <= min(MaxProposalPayloadSize, MaxEntrySize)` |
| Response | a `*RequestState` to await, or the `sm.Result` directly |
| Errors | `*NotLeaderError`, `ErrBusy` (`*BusyError`; the global limiter's `ErrSystemBusy` matches it), `ErrDropped` (proposal queue full), `*EntryTooLargeError`, `ErrDiskFull`, `ErrShardFailed`, `ErrTransferInProgress`, `*MaybeAppliedError`, `ErrInvalidOperation` on a witness, `ErrClosed`, `ErrDraining`, `ErrShardNotFound`, `ErrTimeout` / `ErrCanceled` |
| Permission | none in this library |

Rejected before any work: a done context, a closed or draining host, an unknown
shard, a witness, a full disk, an oversized command, and no known leader, which
returns `*NotLeaderError` at once rather than waiting for a timeout.

Session-bearing variants are in
[`../client-sessions/api.md`](../client-sessions/api.md).

### Capability: read

```go
func (h *Host) SyncRead(ctx context.Context, shardID uint64, query interface{}) (interface{}, error)
func (h *Host) ReadIndex(ctx context.Context, shardID uint64) (*RequestState, error)
func (h *Host) SyncReadBuf(ctx context.Context, shardID uint64, key []byte) (result []byte, release func(), err error)
func (h *Host) FollowerRead(ctx context.Context, shardID uint64, query interface{}) (interface{}, error)
func (h *Host) StaleRead(ctx context.Context, shardID uint64, query interface{}) (interface{}, error)
func (h *Host) QueryLocalNode(ctx context.Context, shardID uint64, query interface{}) (interface{}, error)
func (h *Host) StaleReadBuf(ctx context.Context, shardID uint64, key []byte) (result []byte, release func(), err error)
```

| Method | Guarantee |
|---|---|
| `SyncRead`, `ReadIndex`, `SyncReadBuf` | linearizable: a confirmed read index, then the state machine |
| `FollowerRead` | waits until local apply reaches the local commit index; no leadership check, so it may lag the leader during a partition |
| `StaleRead`, `QueryLocalNode`, `StaleReadBuf` | this replica's applied state, no guarantee. `StaleRead` is the preferred name; `QueryLocalNode` is the same call. |

| | |
|---|---|
| Errors | `*NotLeaderError`, `ErrSystemBusy` (read queue full), `ErrShardFailed`, `ErrShardNotReady`, `ErrTransferInProgress`, `ErrInvalidOperation` on a witness (it stores no log data), `ErrClosed`, `ErrDraining`, `ErrTimeout` / `ErrCanceled` |
| Permission | none |

The `*Buf` forms return a pooled buffer plus a `release` function. **Call
`release`.** They avoid the allocation when the state machine implements
`sm.NALookupInto` and fall back to an ordinary `Lookup` otherwise.

Concurrent linearizable readers on one shard share a single heartbeat round;
see [`architecture.md`](architecture.md).

### Capability: inspect consensus state

```go
func (h *Host) GetLeaderID(shardID uint64) (uint64, bool, error)
func (h *Host) GetLeaderIDAndTerm(shardID uint64) (uint64, uint64, bool, error)
func (h *Host) ShardReadReady(shardID uint64) (leaderID, term uint64, ready bool, err error)
func (h *Host) GetShardInfo(shardID uint64) (*ShardInfo, error)
func (h *Host) GetLogRange(shardID uint64) (LogRange, error)
func (h *Host) GetLogReader(shardID uint64) (ReadonlyLogReader, error)
func (h *Host) QueryRaftLog(ctx context.Context, shardID uint64, firstIndex uint64, lastIndex uint64, maxSize uint64) ([]proto.Entry, error)
func (h *Host) StateMachineHash(shardID uint64) (hash uint64, appliedIndex uint64, ok bool, err error)
func (h *Host) HealthCheck() *HealthStatus
func (h *Host) IsShardJoining(shardID uint64) bool
```

`StateMachineHash` returns `ok == false` when the state machine does not
implement `sm.Hasher`.

`GetShardInfo` returns a `*ShardInfo`. On the leader its `Replicas` field
(`[]ReplicaProgress`) is the leader's progress view of every remote member it
tracks: voters, observers (learners) and witnesses, each with a `Role`
(`ReplicaRoleVoter`, `ReplicaRoleObserver` or `ReplicaRoleWitness`), `Match`,
`Next` and `State`. It is empty on a follower. Only voters and witnesses count
toward quorum, so a caller computing quorum or health figures filters on
`Role`. The type is in
[`../multi-group-host/api.md`](../multi-group-host/api.md#replicaprogress).

`ShardReadReady` reports, without blocking, whether the shard's leader for
`term` has committed an entry of that term as this replica knows it — the
synchronous form of `OnLeaderReady` below. It is never ahead of the event.

### Capability: diagnostics

```go
func (h *Host) DiagRaftState(shardID uint64) (term, committed, processed, lastIdx uint64, msgs int, state string, err error)
func (h *Host) DiagPipelineState(shardID uint64) (commitPending bool, proposalQLen int, inboxLen int, pendingProposals int, err error)
func (h *Host) DiagShardGates(shardID uint64) (recovering, saving bool, applyInFlight int64, err error)
func (h *Host) DiagRemotes(shardID uint64) (string, error)
```

For incident response, not monitoring; their signatures are diagnostic and may
change.

### Capability: transfer leadership

```go
func (h *Host) RequestLeaderTransfer(ctx context.Context, shardID, targetReplicaID uint64) error
```

While a transfer is active the leader refuses proposals and reads with
`ErrTransferInProgress`.

### `RequestState` and error classification

```go
type RequestState struct{ ... }   // await via ResultC() or Result(); call Release()
type RequestResult struct{ ... }
func NewRequestStatePool() *sync.Pool

func NotLeader(err error) (replicaID uint64, ok bool)
func Temporary(err error) bool
func MaybeApplied(err error) bool
```

`NotLeader` extracts the leader to redirect to, when one is known.
`Temporary` reports whether a retry is reasonable.

**`MaybeApplied` needs deliberate handling.** It reports that the proposal may
or may not have been applied. A caller that treats it as "failed" and retries a
non-idempotent command applies it twice; sessions
([`../client-sessions/`](../client-sessions/user.md)) make that retry safe.
Every failure delivered after the proposal was handed to Raft reports it:
a timeout, a leadership change, a stopped, closed or halted shard, and a
persist, commit or apply failure. Only a refusal before the hand-off is
definite; the full list is in
[`../multi-group-host/api.md`](../multi-group-host/api.md#classification-helpers).

### Error vocabulary

Sentinels: `ErrBusy`, `ErrSystemBusy`, `ErrDropped`, `ErrRejected`,
`ErrInvalidOperation`, `ErrDiskFull`, `ErrShardFailed`,
`ErrTransferInProgress`, `ErrShardNotFound`, `ErrShardNotReady`, `ErrClosed`,
`ErrDraining`, `ErrTimeout`, `ErrCanceled`, `ErrEntropyDegenerate`,
`ErrSessionsUnsupportedOnDisk`, `ErrInvalidShardHealth`.

Typed: `*NotLeaderError`, `*BusyError`, `*MaybeAppliedError`,
`*EntryTooLargeError`, `*LoopPanicError`, `*TypeAssertionError`,
`*HostInitError`, `*InvalidShardHealthError`.

Membership errors are listed in
[`../membership-changes/api.md`](../membership-changes/api.md#errors). All match
with `errors.Is` / `errors.As`.

### Notifications

`config.EventListener` carries the user-visible callbacks. `OnLeaderUpdated`,
`OnLeaderReady`, `OnLeaderLost`, `OnCampaignLaunched` and `OnShardFailed` for step, commit and
apply failures are delivered on a dedicated dispatcher goroutine that holds no
engine lock, so a listener may call any `Host` API, including the synchronous
`StopShard`. Delivery is FIFO, so it is ordered per shard. The other callbacks
run on the goroutine that produced them and must not block. There is no
ordering guarantee between the two groups.

`OnLeaderReady` fires when the shard's leader for `LeaderInfo.Term` has
committed an entry of that term, the ReadIndex precondition of Raft thesis §6.4,
on the leader and on every follower and observer whose commit index reaches such
an entry. From then until the next `OnLeaderUpdated`, a `ReadIndex` on that
replica is not refused with `ErrReadIndexNotReady`. It follows the
`OnLeaderUpdated` for the same leader and term, at most once per announcement;
the next `OnLeaderUpdated` (always a different pair, including the same leader
re-elected at a higher term, which `OnLeaderLost` misses), `OnShardFailed` and
unload end it. Witnesses never receive it. The contract in full is in
[`../multi-group-host/api.md`](../multi-group-host/api.md#event-listener).

`OnCampaignSkipped` is declared but never fires; detect a leaderless shard with
`GetLeaderID` or `ShardInfo.Health`.

Payload types: `LeaderInfo`, `ShardFailedInfo`, `SnapshotInfo`,
`ConnectionInfo`, `DiskInfo`, `EventShardInfo`. `ShardFailedInfo.Err` is a
public error type such as `quicraft.ApplyError` or `quicraft.SMPanicError`; see
the [host API errors](../multi-group-host/api.md#errors).

## Configuration

The full table is in [`engineering.md`](engineering.md#configuration). The
three that change behavior rather than performance:

| Field | Default | Note |
|---|---|---|
| `CheckQuorum` | `*bool`, nil = enabled | the leader steps down without quorum |
| `PreVote` | `*bool`, nil = enabled | stops a partitioned node disrupting a healthy term |
| `LeaseRead` | `false` | requires `CheckQuorum` (`ErrLeaseReadRequiresCheckQuorum`) and `ElectionRTT >= 5` (`*config.ValidationError`) |

The `*bool`s are pointers so the zero value means "unset", not "off".

## Versioning and deprecation

- The internal packages carry no compatibility promise and cannot be imported.
- The `Host` methods above are the stable surface; a signature change breaks
  every consumer and is sequenced bottom-up.
- Raft messages on the wire are `proto.MessageBatch`, gated on
  `BinVer == proto.WireVersion` by exact match. See
  [`../wire-protocol/api.md`](../wire-protocol/api.md).
- Nothing here is deprecated.
