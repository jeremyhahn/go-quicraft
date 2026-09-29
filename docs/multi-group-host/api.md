# Multi-Group Host — API

> **Status: NORMATIVE** · Scope: go-quicraft `pkg` and `pkg/config` ·
> Last verified: 2026-09-15
> Conflicts with this document are defects. Fix them here first.

## Transport coverage

go-quicraft is a library. **The Go API is the API** — this document is the
whole of it.

| Transport | This feature |
|---|---|
| **Embedded (Go call)** | everything below. This is the transport. |
| **QUIC** | a peer protocol between replicas of one shard, not a client surface: there is no request an external caller can issue over it. See [`../quic-transport/api.md`](../quic-transport/api.md). |
| Unix socket | n/a — this repository serves no socket |
| REST | n/a — this repository exposes no HTTP handler |
| gRPC | n/a — this repository defines no gRPC service |
| MCP | n/a — this repository registers no MCP tools |

These are not missing-capability rows. In the portfolio capability register the
go-quicraft row for Raft consensus, log replication and QUIC transport is marked
as consumed **by implementing interfaces, not by client calls**, and carries no
transport tier. Exposing a client transport is the composing layer's job.

## The exposed host surface

`docs/00 §R` states it narrowly: `Host.StartShard`, `GetLeaderID` and replica
add/remove. Everything else below is the supporting vocabulary for those — the
proposal and read paths that drive a started shard, the lifecycle that starts
and stops the process, and the types those signatures name.

### Naming: the `Get` prefix is a recorded exception

`go-style.md` says getters take no `Get` prefix. Ten `Host` methods carry one
— `GetNewSession`, `GetNewSessionWithOwner`, `GetNoOPSession`,
`GetShardMembership`, `GetShardInfo`, `GetLeaderID`, `GetLeaderIDAndTerm`,
`GetNodeHostInfo`, `GetLogReader`, `GetLogRange` — and they are kept on
purpose (owner ruling 2026-09-15). Four are dragonboat's own names, which is
what `contrib/dragonboat` exists to present, and go-qrdb calls three of them
directly. There is no deprecation policy in the portfolio, so a rename would
be a breaking MINOR with no alias period, bought for convention alone. New
methods do not take the prefix; these ten are the exception, not a pattern.
The `config` getters over `*bool` fields with defaults (`GetCheckQuorum`,
`GetPreVote`, `GetOrderedConfigChange`, `GetNonceTracking`) are protobuf-style
optional-field accessors and are not getters in the convention's sense.

Source: `github.com/jeremyhahn/go-quicraft/pkg`

The `quicraft` package is the public entry point. One `Host` per process manages shard lifecycle, proposals, linearizable reads, sessions, membership, snapshots, and shutdown. Transport is QUIC only; there is no TCP fallback.

The supported surface:

| Package | Import path | Contains |
|---|---|---|
| `quicraft` | `.../pkg` | `Host`, `RequestState`, `Session`, `Membership`, errors, metrics |
| `config` | `.../pkg/config` | `HostConfig`, `Config`, `TransportConfig`, `EventListener` |
| `sm` | `.../pkg/sm` | The three state machine interfaces and their factory helpers |
| `proto` | `.../pkg/proto` | Wire types a caller sees: `Entry` (from `QueryRaftLog`), message/config-change type constants |
| `seal` | `.../pkg/seal` | The Layer-1 at-rest encryptor (`EpochBarrier`) behind `WithBarrier` |

Everything under `pkg/internal/` is off limits and its identifiers cannot be imported, which matters in one place: several membership validation errors originate there. See [Membership validation errors](#membership-validation-errors).

**What this library does not do.** There is no peer discovery, no bootstrap orchestration, no cluster manager. `Host.StartShard` takes the member set you hand it. Deciding which nodes exist and which shards they carry belongs to the layer above (in this stack, go-qrdb).

## Host

`Host` is the main entry point. One Host per process manages thousands of independent Raft shards.

### Constructor

```go
func NewHost(cfg config.HostConfig, opts ...HostOption) (*Host, error)
```

Creates a Host with the given configuration. Validates the config, creates or loads a host ID from disk, initializes LogDB, registry, QUIC transport, engine, and the worker pipeline. Starts the transport listener and engine workers before returning.

**HostOption functions:**

| Option                     | Signature                          | Description                                              |
|----------------------------|------------------------------------|----------------------------------------------------------|
| `WithLogDB(l)`             | `logdb.LogDB`                      | Custom LogDB. Default: WAL-based from the HostConfig dirs |
| `WithMemoryLogDB()`        | --                                 | In-memory LogDB (benchmarks, CI, ephemeral nodes)         |
| `WithBarrier(b)`           | `Barrier`                          | Host-wide at-rest encryption. Must be initialized and unsealed |
| `WithShardBarriers(fn)`    | `func(shardID uint64) Barrier`     | Per-shard at-rest encryption. See below                  |
| `WithoutEncryption()`      | --                                 | Declares plaintext at rest. Conflicts with the two above  |
| `WithMetrics(c)`           | `MetricsCollector`                 | Metrics collection                                        |
| `WithLogger(l)`            | `*slog.Logger`                     | Custom logger. Default: built from `LogConfig`            |
| `WithoutTransport()`       | --                                 | No-op sender; messages are dropped. Single-node testing only |

**At-rest encryption is stated, never defaulted.** Every host passes
`WithBarrier`, `WithShardBarriers`, or `WithoutEncryption`. `NewHost` returns
`ErrAtRestEncryptionUnspecified` for a host that passes none of them, and
`ErrAtRestEncryptionConflict` for `WithoutEncryption` combined with either
barrier option. The check runs before anything touches the disk.

**`WithShardBarriers`** seals each shard under its own barrier, so one tenant's key purge can never remove an epoch another tenant's snapshots still reference. The resolver returning `nil` for a shard falls back to the `WithBarrier` barrier; when there is no host-wide barrier, the miss fails closed with `ErrShardBarrierUnresolved` rather than writing that one tenant in plaintext. Key rotation for per-shard barriers is the caller's responsibility -- the host's rotation loop only rotates the `WithBarrier` barrier.

Per-shard barriers also constrain the LogDB, because the default WAL takes a single host-wide key:

| Error                          | Condition                                                                   |
|--------------------------------|-----------------------------------------------------------------------------|
| `ErrShardBarriersPlaintextWAL` | The LogDB does not report `logdb.AtRestBarrierEncrypted` or `AtRestNonDurable`, and no `WithBarrier` was supplied. Snapshots would be encrypted while every log entry stayed plaintext |
| `ErrShardBarrierLogDBMismatch` | The LogDB stores this shard's records under a different barrier than the resolver returned |

Supplying `WithLogDB` is not by itself a mitigation: an unencrypted `waldb.Open(dir)` produces exactly the state `ErrShardBarriersPlaintextWAL` exists to prevent. For genuine per-shard WAL keys the LogDB must implement `logdb.PerBarrierEpochAwareLogDB`.

**Errors:** `*config.ValidationError` when the config is invalid; otherwise `*HostInitError` naming the component that failed and wrapping its error.

### Shard Lifecycle

#### StartShard

```go
func (h *Host) StartShard(members map[uint64]string, join bool, create sm.CreateFunc, cfg config.Config) error
```

Starts a Raft shard on this host. `members` maps replicaID to address. `join` is true when this replica is joining a shard that already exists, in which case `members` may be empty and the configuration arrives by replication. `create` is the state machine factory. `cfg.ShardID` and `cfg.ReplicaID` must both be non-zero.

On a first start the member set is not an out-of-band seed. It is committed as an `EntryInitialMembership` entry at log index 1, so membership is a function of the log from the very first entry and a restart recovers it from durable state rather than from whatever map the caller happens to pass. Every founding replica (`join=false`) must therefore be given an identical `members` map, an identical `cfg.Witness` policy and an identical `cfg.MemberIdentities`: founders that disagree produce different index-1 entries, and Log Matching never reconciles them.

On a **restart** the map must still list every current member, non-voters included, or they disappear from this replica's routing until the configuration is recovered.

**Errors:**

| Error                          | Condition                                                        |
|--------------------------------|------------------------------------------------------------------|
| `ErrClosed`                    | Host is closed                                                   |
| `ErrInvalidConfig`             | `ShardID == 0`, `ReplicaID == 0`, or `cfg.Validate()` failed     |
| `ErrShardAlreadyExists`        | Shard already loaded on this host                                |
| `*EmptyMembersError`           | Bootstrap with an empty `members` map                            |
| `*SelfNotInMembersError`       | `cfg.ReplicaID` absent from `members` on a bootstrap             |
| `*InvalidMemberError`          | A member has a zero replica ID, an empty or malformed address, or a duplicate address. Matches `errors.Is(err, ErrInvalidMember)` |
| `*NonVotingRoleInMembersError` | `cfg.IsWitness` or `cfg.IsObserver` set while this replica appears in `members` |
| `*BootstrapEntryMissingError`  | The durable log holds something other than the founding configuration at the bootstrap index |
| `ErrInvalidSMType`             | `CreateFunc` returned nil or a type implementing none of the three SM interfaces |
| `ErrRecoveredTermUnknown`      | An on-disk SM recovered to an index whose Raft term cannot be resolved |
| `*HostInitError`               | Peer, state machine, snapshot recovery, or LogDB initialization failed. `Field` names which |
| `ErrLogDBWrite`                | Bootstrap save failed                                            |

`*NonVotingRoleInMembersError` applies only to a first start. `members` seeds the full voter set, so a witness or observer named there is a full voter in the configuration the whole shard agrees on: the leader replicates entry payloads to it, it counts toward every quorum, and only this host's local read gate still behaves like a non-voter. Bootstrap the voters first, then start the replica with `join=true` and add it with `RequestAddWitness` or `RequestAddObserver`.

**Concurrency:** Safe for concurrent calls with different shard IDs.

#### StopShard

```go
func (h *Host) StopShard(shardID uint64) error
```

Stops all replicas of the given shard, completing every pending proposal, read and snapshot with `ErrAborted`, then unloading the engine node and removing its registry entries. A non-nil return after teardown began is the state machine's `Close` error, which for an on-disk state machine is its backing store's final flush.

**Errors:** `ErrClosed`, `ErrShardNotFound`. A shard already being torn down by a concurrent `StopShard` or `Close` also returns `ErrShardNotFound`, since teardown is claimed once.

### Proposal Path

#### Propose

```go
func (h *Host) Propose(ctx context.Context, shardID uint64, cmd []byte) (*RequestState, error)
```

Submits an asynchronous proposal through Raft. Returns a `*RequestState` that the caller must poll via `Result()` and release via `Release()`.

A proposal is **not** rejected merely because this replica is a follower. It fails fast only when no leader is known at all; when a leader is known, the engine forwards the entry. An empty `cmd` is legal and commits as a no-op entry that never reaches the state machine, which is how you force a current-term committed entry after an election without inventing a throwaway payload (the ReadIndex warmup of Raft PhD 6.4).

**Errors:**

| Error                   | Condition                                                            |
|-------------------------|----------------------------------------------------------------------|
| `ErrCanceled` / `ErrTimeout` | `ctx` was already done on entry                                 |
| `ErrClosed`             | Host closed                                                          |
| `ErrDraining`           | Host in drain mode                                                   |
| `ErrShardNotFound`      | Shard not loaded                                                     |
| `ErrInvalidOperation`   | This replica is a witness; witnesses store no log data               |
| `ErrDiskFull`           | The WAL directory reached `MaxWALDiskSize`                           |
| `*EntryTooLargeError`   | `len(cmd)` exceeds `min(MaxProposalPayloadSize, MaxEntrySize)`       |
| `*NotLeaderError`       | No leader is known. `LeaderID` is 0 and `LeaderAddress` empty         |
| `ErrSystemBusy`         | Global rate limit (`MaxTotalInMemLogSize`) exceeded                   |
| `*BusyError`            | Per-shard rate limit (`MaxInMemLogSize`) exceeded                    |
| `ErrShardNotReady`      | Engine node or Peer not initialized                                  |
| `ErrDropped`            | The engine inbox is full                                             |

**Concurrency:** Safe for concurrent calls.

#### SyncPropose

```go
func (h *Host) SyncPropose(ctx context.Context, shardID uint64, cmd []byte) (sm.Result, error)
```

Synchronous wrapper around `Propose`. Blocks until the result is available or `ctx` is cancelled. Handles `RequestState` lifecycle internally (calls `Release()` on defer).

Returns `sm.Result{Value, Data}` from the state machine.

#### ProposeWithSession

```go
func (h *Host) ProposeWithSession(ctx context.Context, session *Session, cmd []byte) (*RequestState, error)
```

Submits an asynchronous session-managed proposal for at-most-once delivery. The session tracks the series ID for deduplication. The caller must call `session.ProposalCompleted()` after a successful result, or the series ID goes stale and no further proposal on that session succeeds.

On error the session state is untouched, so the same session can retry the same command; that is the point of the session.

A session serves **one proposal at a time**. The entry is stamped with the session's current series number, and the state machine treats every entry with the same `(ClientID, SeriesID)` as a retransmission of one command — the property that makes the retry safe, and the reason a second *concurrent* proposal on the same session is unsafe: only one of the two would run and both would report success. The series is reserved for the returned `RequestState`, and a second call while it is held returns `ErrSessionBusy` (`Temporary`). The reservation is released by `ProposalCompleted`, by an error result, or by `RequestState.Release`, whichever comes first.

Returns `ErrSessionsUnsupportedOnDisk` on a shard whose state machine is an `sm.DiskStateMachine`. A no-op session is unaffected. See [Sessions and on-disk state machines](#sessions-and-on-disk-state-machines).

#### SyncProposeWithSession

```go
func (h *Host) SyncProposeWithSession(ctx context.Context, session *Session, cmd []byte) (sm.Result, error)
```

Synchronous wrapper around `ProposeWithSession`. Blocks until the result is available and calls `session.ProposalCompleted()` for you on success.

### Linearizable Read Path

#### ReadIndex

```go
func (h *Host) ReadIndex(ctx context.Context, shardID uint64) (*RequestState, error)
```

Submits a ReadIndex request to confirm leadership via heartbeat quorum. Returns a `*RequestState` that resolves when the local SM has applied up to the confirmed commit index. The result's `Value` field carries the confirmed read index.

Witnesses cannot serve reads (`ErrInvalidOperation`). Observers and followers forward the ReadIndex to the leader.

**Errors:** `ErrCanceled` / `ErrTimeout` (context already done), `ErrClosed`, `ErrDraining`, `ErrShardNotFound`, `ErrInvalidOperation` (witness), `ErrShardNotReady`, `ErrShardFailed` (the state-machine failure latch has tripped — returned at once, before the read is registered, and to a read already pending when the latch trips; a halted shard never applies again, so the read could otherwise only time out, #185), `ErrSystemBusy` (read queue full). There is no entry-size or per-shard rate-limit check on this path. `ErrReadIndexNotReady` arrives on the `RequestState` rather than from this call when the leader has not yet committed an entry in its current term.

#### SyncRead

```go
func (h *Host) SyncRead(ctx context.Context, shardID uint64, query interface{}) (interface{}, error)
```

Performs a linearizable read in a single call:

1. Submits `ReadIndex` to confirm leadership via heartbeat quorum
2. Waits for `RequestState` to resolve (lastApplied >= readIndex)
3. Executes `SM.Lookup(query)` on the caught-up state machine

Returns the query result from the state machine. This is the strongest consistency guarantee: the read reflects all writes committed before it.

Step 3 acquires the RSM adapter's read guard, which an in-flight `Apply` holds for writing. On a follower catching up, that apply can carry up to 64 MiB of entries, and `sync.RWMutex` offers no context-aware acquire, so `ctx`'s deadline is not observed while the call is blocked on the lock. The same applies to `SyncReadBuf`, `FollowerRead`, `QueryLocalNode`, `StaleRead` and `StaleReadBuf`. The guard is on the adapter, so this is not something a tier choice avoids today; see [Lookup concurrency](../state-machines/engineering.md#lookup-concurrency).

#### SyncReadBuf

```go
func (h *Host) SyncReadBuf(ctx context.Context, shardID uint64, key []byte) (result []byte, release func(), err error)
```

Pooled-buffer variant of `SyncRead` for `[]byte` key queries. The caller **must** call `release()` when done, after which `result` must not be touched. Use it where the result is consumed immediately, such as being written into a response.

The allocation-free path requires the state machine to implement `sm.NALookupInto`. When it does not, the call still works -- it falls back to `Lookup` and allocates. `ErrInvalidOperation` comes back only when that fallback returns something other than `[]byte`, in which case the query belongs on `SyncRead`.

```go
result, release, err := host.SyncReadBuf(ctx, shardID, key)
if err != nil {
    return err
}
defer release()
// use result before returning
```

#### StaleRead

```go
func (h *Host) StaleRead(ctx context.Context, shardID uint64, query interface{}) (interface{}, error)
```

Reads local state machine state directly via `SM.Lookup`. **Not linearizable** -- the result may be arbitrarily stale. Behaviorally identical to `QueryLocalNode`; both names exist for compatibility with different callers. Use `SyncRead` when you need linearizability, or `FollowerRead` for something in between.

#### StaleReadBuf

```go
func (h *Host) StaleReadBuf(ctx context.Context, shardID uint64, key []byte) (result []byte, release func(), err error)
```

Pooled-buffer variant of `StaleRead`, with the same buffer ownership rules and the same `sm.NALookupInto` fallback behavior as `SyncReadBuf`.

#### FollowerRead

```go
func (h *Host) FollowerRead(ctx context.Context, shardID uint64, query interface{}) (interface{}, error)
```

Reads local state after waiting for the local state machine to apply up to this replica's current **committed** index. No quorum round-trip is performed, so this is not linearizable: the committed index it waits for may itself be behind the leader. What it does give you is a read that never reflects less than what this replica already knows is committed, which is stronger than `StaleRead` and cheaper than `SyncRead`.

The wait polls at 1 ms for the first 50 attempts, then backs off to 20 ms. It terminates on shard or host shutdown even when `ctx` carries no deadline.

**Errors:** `ErrClosed`, `ErrShardNotFound`, `ErrShardNotReady`, `ErrShardFailed` (latched shard, at once — see `ReadIndex`), `ErrInvalidOperation` (witness), `ErrTimeout` / `ErrCanceled` from `ctx`.

#### QueryLocalNode

```go
func (h *Host) QueryLocalNode(ctx context.Context, shardID uint64, query interface{}) (interface{}, error)
```

Reads local state machine state via `SM.Lookup`. **Not linearizable**. Behaviorally identical to `StaleRead`.

**Errors:** `ErrClosed`, `ErrShardNotFound`, `ErrShardNotReady`, `ErrCanceled`/`ErrTimeout` from `ctx`. All read paths check an already-done context before touching the state machine, so cancellation is a hard "do not execute this read" boundary rather than a suggestion.

### Leader Transfer

```go
func (h *Host) RequestLeaderTransfer(ctx context.Context, shardID, targetReplicaID uint64) error
```

Requests leadership transfer to a specific replica. Fire-and-forget: the call returns as soon as the request is delivered, and the transfer completes asynchronously through the Raft state machine. Watch `GetLeaderID` or the `OnLeaderUpdated` event to observe the outcome.

`targetReplicaID` must name a current **voting** member. It is validated here rather than only in the Raft layer, because the Raft layer's rejection surfaces asynchronously in the step worker where it can only be logged, and returning nil to the caller would falsely report success.

| `targetReplicaID`            | Result                                                    |
|------------------------------|-----------------------------------------------------------|
| `0`                          | `*InvalidMemberError` -- rejected, not "pick a follower"   |
| This replica                 | `nil`. The requested end state already holds               |
| An observer or witness       | `*InvalidMemberError`. Neither can become leader           |
| A removed replica            | `*InvalidMemberError`                                      |
| Unknown to this shard        | `*ReplicaNotFoundError`                                    |

**Errors:** the table above, plus `ErrClosed`, `ErrShardNotFound`, `*NotLeaderError`, `ErrShardNotReady`, `ErrSystemBusy`, and `ErrCanceled`/`ErrTimeout` from `ctx`.

### Membership Operations

All membership operations are config changes proposed through Raft. Each returns a `*RequestState` that resolves when the config change is committed and applied.

#### Async Config Changes

```go
func (h *Host) RequestAddNode(ctx context.Context, shardID, replicaID uint64, addr string, configChangeIndex uint64) (*RequestState, error)
func (h *Host) RequestRemoveNode(ctx context.Context, shardID, replicaID uint64, configChangeIndex uint64) (*RequestState, error)
func (h *Host) RequestAddObserver(ctx context.Context, shardID, replicaID uint64, addr string, configChangeIndex uint64) (*RequestState, error)
func (h *Host) RequestAddWitness(ctx context.Context, shardID, replicaID uint64, addr string, configChangeIndex uint64) (*RequestState, error)
```

| Method              | Config Change Type | Description                           |
|---------------------|--------------------|---------------------------------------|
| `RequestAddNode`    | `AddNode`          | Add a voting replica                  |
| `RequestRemoveNode` | `RemoveNode`       | Remove a replica                      |
| `RequestAddObserver`| `AddNonVoting`     | Add non-voting observer (receives log)|
| `RequestAddWitness` | `AddWitness`       | Add voting witness (no SM)            |

Each serializes a `proto.ConfigChange`, wraps it in an `EntryConfigChange` entry, and delivers it through the engine inbox. The change takes effect at commit, not at apply.

The `configChangeIndex` parameter is an optimistic concurrency guard. Pass 0 to skip the check, or pass the `ConfigChangeID` from `SyncGetShardMembership` so a request built against a membership that has since moved is rejected instead of applied.

**Pre-validation.** The request is checked against the current membership before it is proposed, so an invalid change fails the call instead of hanging the caller's `*RequestState` until its deadline. The check has to live here: the commit-time apply path deliberately does not re-validate, because a replica that refused an already-committed entry would diverge from its peers. Validation reads a lock-free membership snapshot, so it is conservative -- a config change committing concurrently can produce a false rejection, which the caller resolves by re-reading the membership and retrying.

#### Membership validation errors

These sentinels are declared in `pkg/internal/raft`, which external code cannot import. Three of them are re-exported from `quicraft` and can be matched with `errors.Is`:

| Error                          | Condition                                                                 |
|--------------------------------|---------------------------------------------------------------------------|
| `ErrTooManyWitnesses`          | A witness add would exceed the shard's replicated `MaxWitnesses`          |
| `ErrBreaksFullReplicaMajority` | The change would leave the full replicas unable to form a quorum on their own, violating the shard's `RequireFullReplicaMajority` policy |
| `ErrRemoveLastFullReplica`     | The change would remove the shard's only full replica. Witnesses cannot become leader, so the shard would be permanently headless |

The remaining validation failures are returned but have no importable sentinel. They are listed here so the messages are recognizable; do not match them by string, and open an issue if you need one of them as a typed error:

| Message fragment                   | Condition                                                             |
|------------------------------------|-----------------------------------------------------------------------|
| `replica ID must be non-zero`      | `replicaID == 0`                                                      |
| `address required for add`         | Add operation with an empty address                                   |
| `target node not found`            | Remove targets a replica that is not a current member                 |
| `node already exists`              | Add targets a replica already present as a voter or witness           |
| `node was previously removed`      | Add or remove targets a replica carrying a removal tombstone          |
| `exceeds 64 voting members`        | The add would exceed `raft.MaxVotingMembers` (64). The bound comes from the ReadIndex quorum bitfield, which is a `uint64` |
| `cannot remove last voting member` | Remove targets the last voting member                                 |

Removing a replica that is not a current member is an error, not a silent success.

**Other errors:** the delivery errors `Propose` returns (`ErrClosed`, `ErrDraining`, `ErrShardNotFound`, `ErrShardNotReady`, `ErrSystemBusy`, `ErrCanceled`/`ErrTimeout` for an already-done context) plus `*NotLeaderError` -- unlike `Propose`, a config change is refused outright on a non-leader rather than forwarded. `*StaleConfigChangeError` when `configChangeIndex` does not match. `*HostInitError` when serialization fails.

An add also re-runs the certificate-exclusivity precondition for the address it introduces, which is the only point at which a grow-by-join deployment can be checked at all: `StartShard` sees only the initial members map, and a joiner's map is empty.

**Concurrency:** with `OrderedConfigChange` true (the default), one membership change is in flight per shard at a time.

#### Config Changes With Node Identity

```go
func (h *Host) RequestAddNodeWithIdentity(ctx context.Context, shardID, replicaID uint64, addr, identity string, configChangeIndex uint64) (*RequestState, error)
func (h *Host) RequestAddObserverWithIdentity(ctx context.Context, shardID, replicaID uint64, addr, identity string, configChangeIndex uint64) (*RequestState, error)
func (h *Host) RequestAddWitnessWithIdentity(ctx context.Context, shardID, replicaID uint64, addr, identity string, configChangeIndex uint64) (*RequestState, error)
func (h *Host) SyncRequestAddNodeWithIdentity(ctx context.Context, shardID, replicaID uint64, addr, identity string, configChangeIndex uint64) error
```

Commits a stable **node identity** for the replica alongside its address. `identity` is the value that node returns from `Host.NodeIdentity`: the SHA-256 of its leaf certificate's SubjectPublicKeyInfo. Once committed, every replica authorizes that replica's quorum-bearing messages by comparing this identity against the authenticated identity of the connection they arrive on, and stops using its address as an authorization input.

That closes two holes at once. A wildcard or shared-SAN certificate no longer lets one peer assert another's `msg.From`, and a replica can change address without its votes and acks going uncounted.

The identity has to come from outside the cluster -- from the operator or the provisioning system that issued the certificate. It is deliberately not captured from the joining node's own connection: the only evidence available at that point is the certificate's address coverage, so a holder of a promiscuous certificate could claim the target's address and have its own identity committed for that replica ID, locking the legitimate node out with the very mechanism meant to protect it. A node is also customarily added before it is started, so there is often no connection to capture.

An empty `identity` is exactly the non-identity variant, and on a host with a transport both are refused with `ErrMemberIdentityRequired`: a member is authorized by its committed identity and by nothing else, so it is admitted with one or not at all (`config.Config.RequireMemberIdentities`, which the Host sets from whether it has a transport). Bootstrap-time identities are seeded through `config.Config.MemberIdentities`; entries beyond the members map, and the whole map on a `join=true` start, are the operator anchor — the replicas this host vouches for when no configuration it holds names the sender, which is what admits the leader's bootstrap snapshot to a joiner.

#### Witness Policy

```go
func (h *Host) RequestUpdateWitnessPolicy(ctx context.Context, shardID uint64, policy config.WitnessConfig, configChangeIndex uint64) (*RequestState, error)
func (h *Host) SyncRequestUpdateWitnessPolicy(ctx context.Context, shardID uint64, policy config.WitnessConfig, configChangeIndex uint64) error
```

Replaces the shard's witness policy. The policy is **replicated state** living in `proto.Membership`, not node-local configuration: it travels through Raft as an `UpdateWitnessPolicy` config change so every replica adopts it at the same log position. `config.Config.Witness` is only the bootstrap seed; editing it on a running node changes nothing.

The new policy must be satisfied by the current membership. Tightening a policy the shard already violates is rejected with `ErrTooManyWitnesses` or `ErrBreaksFullReplicaMajority` -- reshape the membership first, then tighten.

#### Sync Config Changes

```go
func (h *Host) SyncRequestAddNode(ctx context.Context, shardID, replicaID uint64, addr string, configChangeIndex uint64) error
func (h *Host) SyncRequestDeleteNode(ctx context.Context, shardID, replicaID uint64, configChangeIndex uint64) error
func (h *Host) SyncRequestAddObserver(ctx context.Context, shardID, replicaID uint64, addr string, configChangeIndex uint64) error
func (h *Host) SyncRequestAddWitness(ctx context.Context, shardID, replicaID uint64, addr string, configChangeIndex uint64) error
```

Synchronous wrappers around the async config change methods. Block until the config change is applied. Note the name asymmetry: the remove variant is `SyncRequestDeleteNode`, not `SyncRequestRemoveNode`.

#### Membership Queries

```go
func (h *Host) SyncGetShardMembership(ctx context.Context, shardID uint64) (*Membership, error)
func (h *Host) GetShardMembership(shardID uint64) (*Membership, error)
```

`SyncGetShardMembership` performs a linearizable membership read (ReadIndex round-trip). `GetShardMembership` reads the local in-memory membership without a quorum check and may be stale.

### Snapshots

#### RequestSnapshot

```go
func (h *Host) RequestSnapshot(ctx context.Context, shardID uint64, opt SnapshotOption) (*RequestState, error)
```

Requests an on-demand snapshot for the given shard. Returns a `*RequestState` whose `Value` field carries the snapshot index on success. A shard holds one snapshot slot; a second concurrent request is refused rather than queued.

```go
type SnapshotOption struct {
    ExportPath                 string // absolute dir; also marks the snapshot as exported
    CompactionOverhead         uint64 // applied only when OverrideCompactionOverhead is true
    OverrideCompactionOverhead bool
    Exported                   bool   // set automatically from ExportPath; callers rarely set it
}

func (o *SnapshotOption) IsExport() bool
func (o *SnapshotOption) Validate() error

var DefaultSnapshotOption = SnapshotOption{}
```

The zero value uses shard defaults. With `ExportPath` set the snapshot is written to that directory *and* to the standard snapshot directory, so log compaction still advances. `Validate` returns `*SnapshotOptionError`, which matches `errors.Is(err, ErrInvalidSnapshotOption)`.

**Errors:** `ErrClosed`, `ErrShardNotFound`, `ErrShardNotReady`, `ErrSnapshotInProgress`, `*SnapshotOptionError`.

#### SyncRequestSnapshot

```go
func (h *Host) SyncRequestSnapshot(ctx context.Context, shardID uint64, opt SnapshotOption) (uint64, error)
```

Synchronous wrapper around `RequestSnapshot`. Blocks until the snapshot completes and returns the snapshot index.

#### ExportSnapshot

```go
func (h *Host) ExportSnapshot(ctx context.Context, shardID uint64, w io.Writer) error
```

Streams a self-describing snapshot artifact for the shard to the writer. The shard must have at least one snapshot recorded in LogDB.

The artifact is a versioned header -- magic, version, the original `Index`, `Term`, `OnDiskIndex` and `Membership` -- followed by the snapshot data. The header is what makes the round-trip lossless: it preserves the logical snapshot identity so `ImportSnapshot` reconstructs the *same* snapshot on the destination host rather than a different one that happens to hold the same bytes. Emitting only the raw data, as an earlier version did, dropped membership and reset the Raft index and term.

The data section is always **plaintext**. When a barrier is configured, the on-disk `snapshot.dat` is stored in encrypted frames and this method decrypts them first. The decrypting barrier is resolved per shard, so a snapshot sealed under a `WithShardBarriers` key exports correctly instead of being handed to the wrong host-wide barrier.

**Errors:** `ErrClosed`, `ErrShardNotFound`, `ErrSnapshotNotFound`, `ErrSnapshotEncryptedBarrierUnavailable` (the snapshot is encrypted but no barrier is configured or the barrier is sealed -- it fails rather than emitting ciphertext), plus barrier decryption and `ctx` errors.

#### ImportSnapshot

```go
func (h *Host) ImportSnapshot(ctx context.Context, shardID uint64, r io.Reader, opts ...ImportOption) error

func WithImportDataDir(dir string) ImportOption
func WithImportMembership(members map[uint64]string) ImportOption
```

Imports an artifact produced by `ExportSnapshot`. The shard **must be stopped** on this host before calling. The header is parsed to recover the original `Index`/`Term`/`OnDiskIndex`/`Membership`, the remaining bytes are written as `snapshot.dat`, and the snapshot is recorded in LogDB so the next `StartShard` recovers that same logical snapshot.

The on-disk directory is named by the artifact's own snapshot index, the way every other snapshot writer names one, so the import takes its rightful place in the shard's ordered snapshot tree instead of sorting as the oldest. The artifact is written to the canonical snapshot path, which is what makes it visible to snapshot garbage collection and to the directory scan that computes the DEK purge floor.

Because the destination path is derived from the artifact's index, an import can resolve to a path it does not own -- an operator retry, a peer's artifact at an index this replica also reached, a concurrent second import. So the import never writes through that path. It stages into a temporary directory it provably owns and renames into place once the artifact is complete and durable.

**Layout discovery.** The snapshot tree root comes from the `Config.DataDir` that `StartShard` recorded in the LogDB bootstrap record. A shard started with `join=true` records none, and a record predating the `DataDir` field carries none; both fail closed with `ErrImportLayoutUnknown` unless you state the layout with `WithImportDataDir`. Pass `WithImportDataDir("")` to assert the shard has no `DataDir`. A supplied value contradicting the recorded one is refused rather than silently resolved.

**Errors:**

| Error                             | Condition                                                          |
|-----------------------------------|--------------------------------------------------------------------|
| `ErrClosed`                       | Host closed                                                        |
| `ErrInvalidOperation`             | Shard still running, or no LogDB                                   |
| `*ImportLayoutUnknownError`       | Layout not discoverable and none supplied                          |
| `*ImportDataDirMismatchError`     | `WithImportDataDir` disagrees with the recorded `DataDir`          |
| `*ImportSnapshotOutOfDateError`   | The artifact's index does not advance the recorded snapshot        |
| `*ImportSnapshotDirExistsError`   | The destination directory exists. Clearing it is an operator decision; an import never removes a directory it did not create |
| `*ImportMembershipInvalidError`   | `WithImportMembership` broke a rule (see below)                    |
| `*SnapshotTooLargeError`          | The artifact exceeds `config.DefaultMaxSnapshotSize` (4 GB). The shard is stopped, so its own `MaxSnapshotSize` is not available and the package default is the ceiling |
| `*SnapshotArtifactError`          | The artifact header is malformed or truncated                      |

Each `Import*Error` matches a corresponding `ErrImport*` sentinel with `errors.Is`.

**`WithImportMembership` is a disaster-recovery tool and it is unsafe by design.** It replaces the membership carried by the artifact, which is the only way to recover a shard whose quorum is permanently lost: removing dead replicas is a config change, a config change is a log entry, and committing one needs the quorum that no longer exists. Restoring a snapshot does not help either, because the snapshot carries the same configuration.

Rewriting membership discards Raft's safety argument. Excluded replicas may hold committed entries this snapshot does not, and nothing will ever reconcile that -- committed writes can be silently lost. Use it only when the alternative is losing the shard, and only once you have established the excluded replicas are permanently gone rather than merely unreachable.

The procedure: stop every surviving replica, export from the most up-to-date one, and import on each survivor with **the same** map naming only the survivors. Different maps on different survivors is split-brain by construction. The map is the complete new voting set; observers and witnesses from the artifact are dropped, shard policy (session limits, witness policy) is preserved, and the importing replica must appear in the map.

### Informational

```go
func (h *Host) ID() string
func (h *Host) RaftAddress() string
func (h *Host) NodeIdentity() string
func (h *Host) ShardCount() int
func (h *Host) GetLeaderID(shardID uint64) (leaderID uint64, known bool, err error)
func (h *Host) GetLeaderIDAndTerm(shardID uint64) (leaderID, term uint64, known bool, err error)
func (h *Host) GetShardInfo(shardID uint64) (*ShardInfo, error)
func (h *Host) GetNodeHostInfo(opt NodeHostInfoOption) *NodeHostInfo
func (h *Host) IsShardJoining(shardID uint64) bool
func (h *Host) StateMachineHash(shardID uint64) (hash, appliedIndex uint64, ok bool, err error)
```

`GetLeaderID` and `GetLeaderIDAndTerm` read local atomic state and may be stale. `GetNodeHostInfo` returns a snapshot of all loaded shards; pass `NodeHostInfoOption{SkipLogInfo: true}` to omit the per-shard log info, which is the expensive part.

`NodeIdentity` returns this host's stable identity: the SHA-256 of its leaf certificate's SubjectPublicKeyInfo. It is stable across certificate renewal on the same keypair and changes only when the keypair does. It is safe to log, print and copy -- it is a hash of a public key -- and it is what you feed to `Config.MemberIdentities` or `RequestAddNodeWithIdentity`. It returns `""` under `WithoutTransport`, which has no certificate.

`IsShardJoining` reports whether a shard started with `join=true` is still in its bootstrap window, meaning it has not yet accepted its first snapshot. Once it has, the shard leaves that window for the life of the process. Use it to tell when a dynamically added replica has caught up enough to be treated as an established member on future restarts.

`StateMachineHash` exists to detect the one failure Raft cannot: a state machine that is not a deterministic function of the log. Consensus keeps agreeing on the entry sequence while replicas derive different state from it, and the first symptom is usually a linearizable read returning different values depending on which replica served it. `ok` is false when the state machine does not implement `sm.Hasher`, which is an absence of the facility rather than an error. Compare across replicas **at the same applied index** -- that is what the returned index is for. Implementations must make the hash order-independent or it reports divergence that does not exist. It hashes the whole state machine, so it is a diagnostic, not a hot path.

### Health Check

```go
func (h *Host) HealthCheck() *HealthStatus

type HealthStatus struct {
    Status        string      `json:"status"`           // "ok", "degraded", "sealed", "unhealthy"
    BarrierSealed bool        `json:"barrier_sealed"`
    DiskOK        bool        `json:"disk_ok"`
    ShardCount    int         `json:"shard_count"`
    Shards        []ShardInfo `json:"shards,omitempty"`
}
```

Returns a point-in-time health assessment. The precedence is fixed in this order, so a sealed barrier reports `"sealed"` regardless of shard state:

| Status      | Condition                              | HTTP Code |
|-------------|----------------------------------------|-----------|
| `"sealed"`  | Barrier configured and sealed          | 503       |
| `"unhealthy"` | No shards loaded                     | 503       |
| `"degraded"` | Any shard has `ShardError` health     | 503       |
| `"ok"`      | All shards healthy                     | 200       |

Only `ShardError` degrades the aggregate. A shard reporting `ShardNoLeader`, `ShardLagging`, `ShardRecovering` or `ShardSnapshotting` still leaves the host `"ok"` -- inspect `Shards` if you need to alert on those. `DiskOK` is reported but does not affect `Status`.

### Encryption / Barrier

```go
func (h *Host) EmergencySeal() error
func (h *Host) IsSealed() bool
```

`EmergencySeal` seals the barrier immediately, without waiting for in-flight operations. Subsequent operations that need key material fail with `ErrBarrierSealed`. It is for security incident response, so the abruptness is the point.

`IsSealed` returns false when no barrier is configured.

The `Barrier` interface is declared in the `quicraft` package rather than imported, to break an import cycle. `seal.EpochBarrier` satisfies it. Its surface: `IsSealed`, `Seal`, `Encrypt`/`Decrypt`, `EncryptWithEpoch`, `DecryptForEpoch`, the context-AAD variants `EncryptWithEpochContext`/`DecryptWithContext`, `IsHardwareMode`, `SupportsContextAAD`, `Rotate`, `PurgeEpochsBefore`, `RegisterInFlightEpoch`/`DeregisterInFlightEpoch`, `CurrentEpoch` and `ShouldRotateKey`.

`IsHardwareMode` says only that sealing is delegated to an external encryptor. Use `SupportsContextAAD` to learn whether that encryptor can actually authenticate the caller context snapshot frames are bound to: it is true for a software barrier and for a delegated encryptor that carries AAD end to end, false for one that cannot and in no-encryption mode. That is a capability of the deployment's barrier, never a per-artifact choice. `DecryptWithContext` has no weaker retry -- a different context, or none where one was present, fails authentication, because the fallback is the vulnerability.

#### ReencryptSnapshotFile

```go
func ReencryptSnapshotFile(srcPath, dstPath string, shardID, replicaID, index uint64,
    srcBarrier, dstBarrier Barrier) (uint64, error)
```

Re-keys a snapshot data file from one barrier to another: every frame is decrypted under `srcBarrier` and re-encrypted under `dstBarrier`, streamed through an in-memory pipe so a large snapshot never loads fully into memory and its plaintext never lands on disk. Returns the destination's **minimum** at-rest epoch across the written frames, which is the value to record as the snapshot's `Epoch` so the DEK-purge floor protects every frame. It is 0 for an empty source or a plaintext destination.

`shardID`, `replicaID` and `index` are required, not optional: snapshot frames are bound to their position through the AEAD context AAD, and there is no weaker fallback for a caller that omits them.

A **nil** barrier denotes a plaintext side. A non-nil but **sealed** barrier does not -- it means encryption is configured but temporarily unavailable, and it returns `ErrSnapshotBarrierSealed` rather than falling through. Reading an encrypted source under a sealed barrier would copy ciphertext and record it as plaintext; a sealed destination would durably downgrade a snapshot that must be encrypted at rest.

The write is atomic and crash-safe: output goes to a temporary file in the destination's directory, is fsynced, then renamed, and the directory chain is fsynced before success is reported. On any failure before the rename the temp file is removed and both paths are left untouched. The one exception is a directory fsync that fails *after* the rename, which returns `*SnapshotCommittedNotDurableError` carrying the real epoch alongside the error.

### Data Removal

```go
func (h *Host) RemoveData(shardID, replicaID uint64) error
func (h *Host) SyncRemoveData(ctx context.Context, shardID, replicaID uint64) error
```

Removes all persistent data for a replica (logs, snapshots, metadata) and retires its replica ID on this host: `StartShard` refuses it from then on with `*ReplicaRetiredError` (`ImportSnapshot` finds no data left to import into and returns `ErrShardNotFound`), since its term, vote and owed lease vote window are gone. Rejoin under a new replica ID. The shard must be stopped first. `SyncRemoveData` is the synchronous variant (included for API consistency).

**Errors:** `ErrClosed`, `ErrShardNotFound`, `*ShardNotStoppedError` (shard still running).

### Replica Management

```go
func (h *Host) StopReplica(shardID, replicaID uint64) error
```

Stops a specific replica on a shard. Since each host runs at most one replica per shard, this validates the replicaID matches and delegates to `StopShard`.

**Errors:** `ErrClosed`, `ErrShardNotFound`, `*ReplicaNotFoundError`.

#### RequestCompaction

```go
func (h *Host) RequestCompaction(shardID, replicaID uint64) error
```

Triggers explicit log compaction for the given shard/replica pair. This reclaims disk space by removing entries superseded by snapshots.

**Errors:** `ErrClosed`, `ErrShardNotFound`, `*ReplicaNotFoundError`.

#### HasNodeInfo

```go
func (h *Host) HasNodeInfo(shardID, replicaID uint64) bool
```

Returns true when bootstrap information for the shard/replica pair exists in LogDB. False when the host is closed or the replica has never been bootstrapped here.

### Log Access

```go
func (h *Host) GetLogReader(shardID uint64) (ReadonlyLogReader, error)
```

Returns a read-only view of the shard's Raft log: range and node state, no mutation. The values its methods return reflect the log at call time.

**Errors:** `ErrClosed`, `ErrShardNotFound`.

#### QueryRaftLog

```go
func (h *Host) QueryRaftLog(ctx context.Context, shardID uint64, firstIndex, lastIndex, maxSize uint64) ([]proto.Entry, error)
```

Queries a range of Raft log entries. `maxSize` limits the total returned bytes; the call returns as many entries from the range as fit.

**Errors:** `ErrClosed`, `ErrShardNotFound`, `ErrInvalidRange` (`firstIndex >= lastIndex`, or `maxSize == 0`), `ErrCanceled`/`ErrTimeout` from `ctx`, plus LogDB errors such as `logdb.ErrCompacted` when the range has been compacted away.

#### GetLogRange

```go
func (h *Host) GetLogRange(shardID uint64) (LogRange, error)
```

Returns the first and last log indices for the shard.

**Errors:** `ErrClosed`, `ErrShardNotFound`.

### Diagnostics

```go
func (h *Host) DiagPipelineState(shardID uint64) (commitPending bool, proposalQLen int, inboxLen int, pendingProposals int, err error)
func (h *Host) DiagRaftState(shardID uint64) (term, committed, processed, lastIdx uint64, msgs int, state string, err error)
func (h *Host) DiagRemotes(shardID uint64) (string, error)
func (h *Host) DiagShardGates(shardID uint64) (recovering, saving bool, applyInFlight int64, err error)
```

Low-level diagnostics for troubleshooting. `DiagPipelineState` shows commit queuing, proposal queue depth, and inbox backlog. `DiagRaftState` shows Raft term, commit index, processed index, log size, and state string (Follower/Candidate/Leader). `DiagRemotes` returns a textual representation of peer replication states.

`DiagShardGates` reports the two gates that suspend state machine work. While `recovering` is true the state machine is being replaced by a received snapshot: the replica applies nothing and extracts no committed entries, though it keeps voting and replicating. While `saving` is true a snapshot capture has quiesced applies. `applyInFlight` is the drain counter both gates wait on; a non-zero value with a gate up means the drain has not finished. Both states also surface as `ShardInfo.Health`.

For structured peer replication state, prefer `ShardInfo.Replicas` over parsing `DiagRemotes`.

### Shutdown

#### Drain

```go
func (h *Host) Drain() error
func (h *Host) IsDraining() bool
```

`Drain` transitions to drain mode: new proposals, ReadIndex requests, session operations and config changes are rejected with `ErrDraining`. In-flight work is allowed to complete. Blocks until all pending proposals, reads, and snapshots complete, or `ShutdownTimeout` expires (`ErrShutdownTimeout`). Idempotent, and every caller gets the same answer: there is one drain attempt, and a concurrent caller joins it rather than returning early.

Drain is **irreversible**. There is no resume; a host that needs to accept proposals again has to be recreated. The wait is event-driven off a pending-operation counter, not a poll loop.

`IsDraining` returns true if the host is in drain mode.

#### Close

```go
func (h *Host) Close() error
```

Shuts down the host. Shutdown always runs to completion; the returned error is the `errors.Join` of everything that failed along the way -- an incomplete drain, each state machine's `Close`, the LogDB `Close`, the barrier `Seal`. `errors.Is` reaches each cause.

Treat a non-nil return as a durability signal, not as noise. For an on-disk state machine this `Close` is the backing store's final flush, so it is the last chance the embedding application has to learn that writes were lost.

A concurrent or repeated `Close` **blocks** until the shutdown already running has finished and then returns that same error. It does not return nil early.

**Shutdown order:** Drain -> key rotation stop -> pending-request sweeper stop -> engine stop -> in-flight snapshot sends drain -> transport stop -> pending requests failed with `ErrClosed` -> state machines closed -> LogDB close -> barrier seal.

The engine stops before the transport on purpose: the engine's step workers are the only producers of snapshot sends, so stopping them first means the send drain that follows races with no new sends, and the transport is still up while those sends complete.

## RequestState

```go
type RequestState struct { /* unexported fields */ }
```

Tracks a single asynchronous request through the Raft pipeline. Callers obtain a `*RequestState` from `Propose`, `ReadIndex`, `RequestSnapshot`, or membership operations.

### Lifecycle

1. **Created** from a `sync.Pool` with a pre-allocated buffered channel of capacity 1
2. **Pending** -- registered in the shard's pending-proposal, pending-read or pending-snapshot map under a unique monotonic key
3. **Completed** -- an engine callback sends exactly one `RequestResult` on the channel
4. **Released** -- the caller calls `Release()`

`RequestState` is safe for a single goroutine. Do not share one across goroutines without external synchronization.

### Methods

```go
func (rs *RequestState) Result() (RequestResult, error)
```

Blocks until the request completes or the deadline expires. Returns `ErrReleased` if already released. Returns `ErrTimeout` if the deadline (from the originating context) expires.

When no deadline is set (zero `time.Time`), blocks indefinitely.

```go
func (rs *RequestState) ResultC() <-chan RequestResult
```

Returns a read-only channel that receives exactly one result. Returns `nil` if released. Use for `select`-based non-blocking consumption.

```go
func (rs *RequestState) CommittedC() <-chan RequestResult
```

Returns a read-only channel that receives an early WAL-durable notification (~1-2ms). The result carries `Value=0` and nil `Data` because the state machine has not yet applied the entry. Returns `nil` if released.

Only signaled when `NotifyCommit` is enabled on the host. When disabled, this channel never receives.

```go
func (rs *RequestState) ApplyResultC() <-chan RequestResult
```

Returns a read-only channel that receives the full state machine apply result. It returns the same channel as `ResultC()` and exists for readability when both `CommittedC()` and the apply result appear in one `select`.

```go
func (rs *RequestState) Release()
```

Marks the `RequestState` released by the caller. Idempotent. After release, `Result()` returns `ErrReleased` and `ResultC()` returns nil.

The object goes back to the pool only when the caller has released it **and** every pending-map reference is gone. That matters for the common case where a `Sync*` caller times out and releases while the request is still registered: recycling early would hand the same object to a new request and deliver a stale result into it. The late completion or sweep drops the final reference and recycles then.

```go
func (rs *RequestState) Key() uint64
func (rs *RequestState) Deadline() time.Time
```

Accessors for the request key and deadline.

### Pool Pattern

`NewRequestStatePool()` creates a `sync.Pool` producing `RequestState` objects with their result and committed channels already allocated. Each shard holds its own pool, which keeps the proposal hot path off the allocator. It is exported so an embedder building its own request plumbing on top of the pipeline can reuse the same pooling; ordinary callers never need it.

## RequestResult

```go
type RequestResult struct {
    Value uint64   // Application-defined result code from sm.Result
    Data  []byte   // Optional result data from sm.Result
    Err   error    // Non-nil on failure (timeout, canceled, rejected, etc.)
}
```

Returned by `RequestState.Result()` and delivered on `RequestState.ResultC()`.

For proposals: `Value` and `Data` come from `sm.Result` written by the state machine's `Update()`.

For ReadIndex: `Value` carries the confirmed read index.

For snapshots: `Value` carries the snapshot index on success.

On failure: `Err` carries the error (`*NotLeaderError`, `ErrAborted`, `ErrTimeout`, `ErrReadIndexNotReady`, `ErrSessionExpired`, and so on). Classify it with `MaybeApplied`, `Temporary` and `NotLeader` rather than by comparing against sentinels yourself.

## Session API

Sessions provide **at-most-once** proposal semantics. The session manager keys on `(clientID, seriesID)` to detect and deduplicate retransmissions, so a retry after an ambiguous failure applies the command once rather than twice. Session state is replicated, and expiry is measured in applied log entries rather than wall-clock time so every replica expires the same session at the same index.

Sessions are unavailable on shards backed by an `sm.DiskStateMachine`; see [Sessions and on-disk state machines](#sessions-and-on-disk-state-machines).

### Session Type

```go
type Session struct { /* unexported fields */ }
```

Fields are unexported to prevent mutation that would break deduplication. Accessors:

```go
func (s *Session) ShardID() uint64
func (s *Session) ClientID() uint64
func (s *Session) SeriesID() uint64
func (s *Session) OwnerID() uint64
func (s *Session) RespondedTo() uint64
func (s *Session) IsNoOPSession() bool
```

### Constructors

```go
func NewSession(shardID, clientID uint64) *Session
func NewSessionWithOwner(shardID, clientID, ownerID uint64) *Session
func NewNoOPSession(shardID uint64) *Session
```

`NewSession` creates a tracked Session for the given shard and client ID. The client ID must not be zero; zero is reserved for no-op sessions. The session starts unregistered and must be registered through Raft before use.

`NewSessionWithOwner` declares a principal holding the session, so it counts against the shard's replicated `MaxSessionsPerOwner`. An `ownerID` of zero is identical to `NewSession`.

The owner is whatever the **embedder** decides identifies a principal: a tenant ID, an account number, a hash of an authenticated subject. This library cannot derive it -- it authenticates nodes, not clients, and a client ID is eight bytes the caller chooses. The cap is therefore only as trustworthy as the embedder's own authentication: a caller free to pick any `ownerID` is free to evade it, so the value must come from something the embedder verified rather than from the client.

`NewNoOPSession` creates a no-op session with client ID 0 that bypasses deduplication entirely.

### Registration Flow

Session registration and unregistration are proposed through Raft consensus so all replicas maintain consistent session state.

```go
func (h *Host) GetNewSession(ctx context.Context, shardID uint64) (*Session, error)
func (h *Host) GetNewSessionWithOwner(ctx context.Context, shardID, ownerID uint64) (*Session, error)
```

1. Draws a client ID from `crypto/rand`, rejecting zero
2. Sets `seriesID = SeriesIDForRegister`
3. Proposes registration through Raft
4. Blocks until committed and applied
5. Validates the result carries the expected client ID
6. Sets `seriesID = SeriesIDFirstProposal` for normal proposals

Zero is rejected at step 1 because it is the "no session" marker on the wire. A session minted with it is structurally a no-op session and the failure is silent in both directions: the registration entry is skipped by the state machine, and the caller's own `result.Value != clientID` check is satisfied by `0 != 0`. `GetNewSession` would return success and hand back a session whose every later proposal is unsessioned, with at-most-once delivery quietly off. If the CSPRNG returns zero on every attempt it is not producing entropy at all, and the call fails with `ErrEntropyDegenerate` rather than looping.

**Errors:** `ErrClosed`, `ErrShardNotFound`, `ErrSessionsUnsupportedOnDisk`, `*NotLeaderError`, `ErrSystemBusy`, `*SessionError` (wrapping `ErrEntropyDegenerate`, or `ErrInvalidOperation` when the owner is at its cap).

A per-owner cap rejection is a **replicated** refusal: every replica reaches it from the log alone, rather than it being a leader-local admission decision.

```go
func (h *Host) RegisterSession(ctx context.Context, session *Session) (*Session, error)
```

Registers a pre-created session through Raft, preserving its existing client ID rather than generating one. Used by adapters that create sessions with caller-specified client IDs. The session must have a non-zero client ID and belong to the given shard. On success it is prepared for normal proposals.

```go
func (h *Host) CloseSession(ctx context.Context, shardID uint64, session *Session) (*RequestState, error)
```

Proposes unregistration through Raft, setting `seriesID = SeriesIDForUnregister`. Returns a `*RequestState` that resolves when the unregistration is committed and applied.

**Errors:** `ErrClosed`, `ErrShardNotFound`, `ErrSessionsUnsupportedOnDisk`, `ErrInvalidOperation` (nil session), `ErrInvalidSession` (wrong shard, or a no-op session).

### No-Op Sessions

```go
func (h *Host) GetNoOPSession(shardID uint64) *Session
```

Returns a no-op session with client ID 0. No-op sessions bypass deduplication entirely, so use them only where the operation is idempotent. They need no registration and are the only session form available on a shard backed by an `sm.DiskStateMachine`.

### Session Constants

| Constant                  | Value               | Usage                        |
|---------------------------|---------------------|------------------------------|
| `SeriesIDForRegister`     | `math.MaxUint64-1`  | Register sentinel            |
| `SeriesIDForUnregister`   | `math.MaxUint64`    | Unregister sentinel          |
| `SeriesIDFirstProposal`   | `1`                 | First normal proposal series |

### Session Methods

```go
func (s *Session) PrepareForPropose()
func (s *Session) ProposalCompleted()
func (s *Session) PrepareForRegister()
func (s *Session) PrepareForUnregister()
func (s *Session) SetRespondedTo(seriesID uint64)
```

`PrepareForPropose()` sets the session for normal proposal mode, advancing seriesID to `SeriesIDFirstProposal`.

`ProposalCompleted()` advances the series ID and updates respondedTo after a proposal completes.

`PrepareForRegister()` and `PrepareForUnregister()` set the sentinel series IDs for registration and unregistration. `SetRespondedTo()` updates the highest acknowledged series ID; the host calls it internally, and you need it only when driving the session lifecycle by hand.

### Session Validation

```go
func (s *Session) ValidForSessionOp(shardID uint64) bool
func (s *Session) ValidForProposal(shardID uint64) bool
```

`ValidForSessionOp` returns true if the session belongs to the given shard and is not a no-op session.

`ValidForProposal` returns true if the session belongs to the given shard.

## Membership

```go
type Membership struct {
    ConfigChangeID uint64              // Raft log index of the config change that produced this
    Nodes          map[uint64]string   // Full voting members (replicaID -> addr)
    Observers      map[uint64]string   // Non-voting learners
    Witnesses      map[uint64]string   // Voting members without a state machine
    Removed        map[uint64]struct{} // Removal tombstones

    // Replicated witness policy. Changed via Host.RequestUpdateWitnessPolicy.
    MaxWitnesses               uint64  // 0 = no explicit cap
    RequireFullReplicaMajority bool

    // Replicated session policy. Seeded from the founder's Config at
    // bootstrap; no config change alters these afterwards.
    SessionExpiryEntries uint64
    MaxTotalSessions     uint64
    MaxSessionsPerClient uint64
    MaxSessionsPerOwner  uint64

    // Committed node identities. Sparse: a replica absent from this map
    // has none. replicaID -> the value that node reports from NodeIdentity.
    Identities map[uint64]string
}

func NewMembership() *Membership
func (m *Membership) ConfigChangeIndex() uint64 // alias for ConfigChangeID
```

The committed cluster membership for a shard, returned by `Host.SyncGetShardMembership` and `Host.GetShardMembership`. `NewMembership` returns a value with every map initialized.

**Session policy is replicated, not node-local.** A replica evaluating these limits from its own configuration would expire different sessions at different log indexes and diverge its state machine silently, so the founder's values are committed at bootstrap and every replica reads the shard's copy. They are exposed here so an operator can see what the shard actually committed rather than what any one node is configured with. The two differ exactly when someone edited a node's `Config` after bootstrap, in which case the shard's value governs and the node logs the disagreement at startup. A shard bootstrapped before the policy was replicated reports all four as zero.

Note the naming: `MaxSessionsPerClient` is the **response-cache depth**, how many cached results one session retains for duplicate detection. It does not limit sessions. `MaxSessionsPerOwner` is the per-principal session cap and `MaxTotalSessions` the shard-wide one.

**`Identities` is complete on any shard this build bootstrapped behind a transport**: every entry in `Nodes`, `Observers` and `Witnesses` appears here, because admission requires it. A replica absent from the map has every message refused (`missing_identity`); the only way to hold such a configuration is data written by a build that did not require identities, or a host started `WithoutTransport()`.

## Error Types

### Classification helpers

Do not re-derive "is this retryable?" from the sentinel list -- every consumer
that does will disagree with the others. Three helpers answer it:

```go
func MaybeApplied(err error) bool
func Temporary(err error) bool
func NotLeader(err error) (replicaID uint64, ok bool)
```

All three use `errors.Is`/`errors.As`, so wrapped errors classify correctly. A
nil error is not temporary, not maybe-applied and not a not-leader error.

**`MaybeApplied` first.** It reports whether the request had already been
delivered to Raft when it failed, so the entry may still commit and apply.
`Temporary` answers "is it reasonable to retry?" and says nothing about "did it
happen?", and for a consensus library those are different questions with
different consequences. A proposal that reached the log and then lost its wait
-- leadership moved, the deadline elapsed, the shard closed -- is exactly this
case, and retrying it without a client session can apply the command twice
because an unsessioned proposal carries client ID 0 and nothing deduplicates
it. Such errors are wrapped in `*MaybeAppliedError`, which preserves every
existing `errors.Is` result, so a caller that ignores the type behaves as
before.

**`Temporary`** reports whether the identical request, unchanged, can succeed on
retry once a transient condition clears without operator or caller
intervention. It is true for exactly these: `ErrBusy` (and therefore
`ErrSystemBusy`, which is a `*BusyError`), `ErrTimeout`, `ErrAborted`,
`ErrDropped`, `ErrShardNotStarted`, `ErrShardNotReady`, `ErrNotLeader`,
`ErrReadIndexNotReady`, `ErrSnapshotInProgress`, `ErrTransferInProgress`.

**Everything else is permanent, including errors this package never produced.**
Retrying a permanent failure forever turns one bad request into sustained load
and hides the real fault. Some exclusions look retryable at a glance and are
not:

| Excluded                     | Why                                                                 |
|------------------------------|---------------------------------------------------------------------|
| `ErrCanceled`                | The caller's own decision. A deadline that elapses server-side surfaces as `ErrTimeout`, which is temporary |
| `ErrDraining`                | A one-way transition into shutdown. Retry a different host          |
| `ErrStaleConfigChange`       | The identical request fails forever. Re-read membership with `SyncGetShardMembership` and build a new request |
| `ErrSessionLimitExceeded`    | Whether it clears depends on `SessionExpiryEntries` and on clients closing sessions. A retry loop against a client that leaks sessions spins forever |
| `ErrBarrierSealed`           | Clearing it requires an operator unseal ceremony                    |
| `ErrShardBarrierUnresolved`  | A resolver miss can mean "tenant not provisioned yet" (clears) or "misconfigured" (never clears), and the error cannot tell them apart |
| `ErrDiskFull`, `ErrLogDBWrite` | Storage faults needing operator action. Retrying into a full or failing volume makes the incident worse |
| `ErrShardFailed`             | The state-machine failure latch tripped and does not reset on its own |

**`NotLeader`** returns `ok == true` for any not-leader error, whether the bare
`ErrNotLeader` sentinel or a `*NotLeaderError`. `replicaID` is the leader this
replica believes is current, or **0 when the leader is unknown**. Treat
`(0, true)` as "retry, but discover the leader yourself". Every not-leader error
is also `Temporary`, so gate the retry loop on `Temporary` and use `NotLeader`
only to pick a target.

```go
for {
    _, err := host.SyncPropose(ctx, shardID, cmd)
    if err == nil {
        return nil
    }
    if quicraft.MaybeApplied(err) {
        // The entry may already be in the log. Retrying an unsessioned
        // proposal here can double-apply it: use a session, or verify.
        return err
    }
    if !quicraft.Temporary(err) {
        return err
    }
    if leaderID, ok := quicraft.NotLeader(err); ok && leaderID != 0 {
        redirect(leaderID)
    }
    // back off, then retry
}
```

### Sentinel Errors

All sentinel errors declared here use the `"quicraft:"` prefix. Check with `errors.Is()`.

#### Host and request lifecycle

| Error                | Returned When                                                       |
|----------------------|---------------------------------------------------------------------|
| `ErrClosed`          | Operation on a closed Host, or the request never started            |
| `ErrDraining`        | New work submitted during drain                                     |
| `ErrStopped`         | The shard or engine stopped underneath an in-progress snapshot save or recovery. Distinct from `ErrClosed` |
| `ErrShutdownTimeout` | Drain exceeded `ShutdownTimeout`                                    |

#### Request outcome

| Error         | Returned When                                          |
|---------------|---------------------------------------------------------|
| `ErrTimeout`  | Request deadline expired                                |
| `ErrCanceled` | Request canceled via context                            |
| `ErrRejected` | Proposal rejected, e.g. a config change already in progress |
| `ErrAborted`  | Internal state transition such as a leadership change   |
| `ErrDropped`  | Proposal dropped: queue overflow or shutdown            |
| `ErrReleased` | `Result`/`ResultC` on an already-released `RequestState` |

#### Shard state

| Error                   | Returned When                                     |
|-------------------------|---------------------------------------------------|
| `ErrShardNotFound`      | Shard not loaded on this Host                     |
| `ErrShardNotStarted`    | Shard registered but not initialized              |
| `ErrShardNotReady`      | Shard initializing, cannot serve requests         |
| `ErrShardAlreadyExists` | `StartShard` for an already-running shard         |
| `ErrShardNotStopped`    | `RemoveData` for a shard still loaded             |
| `ErrReplicaRetired`     | `StartShard` of a replica ID `RemoveData` retired |
| `ErrShardFailed`        | State machine panicked; the shard failure latch tripped |
| `ErrReplicaNotFound`    | Replica ID does not match the replica on this host |

#### Backpressure and leadership

| Error                  | Returned When                                                |
|------------------------|--------------------------------------------------------------|
| `ErrBusy`              | Per-shard in-memory log exceeded `MaxInMemLogSize`           |
| `ErrSystemBusy`        | Global limit `MaxTotalInMemLogSize` exceeded. It is a `*BusyError`, so `errors.Is(err, ErrBusy)` is also true |
| `ErrDiskFull`          | WAL directory reached `MaxWALDiskSize`                       |
| `ErrNotLeader`         | Sentinel form of `*NotLeaderError`                           |
| `ErrReadIndexNotReady` | Leader has no committed entry in its current term            |

#### Validation

| Error                       | Returned When                                                |
|-----------------------------|--------------------------------------------------------------|
| `ErrInvalidConfig`          | Config validation failure                                    |
| `ErrInvalidOperation`       | Invalid for the current state, e.g. a witness read or propose |
| `ErrInvalidSMType`          | `CreateFunc` returned a type implementing none of the SM interfaces |
| `ErrInvalidSession`         | Wrong shard, or a no-op session used for a session operation |
| `ErrSessionBusy`            | The session already has a proposal in flight; `Temporary`   |
| `ErrInvalidRange`           | Log query with `firstIndex >= lastIndex` or `maxSize == 0`   |
| `ErrInvalidMember`          | Sentinel form of `*InvalidMemberError`                       |
| `ErrEntryTooLarge`          | Sentinel form of `*EntryTooLargeError`                       |
| `ErrStaleConfigChange`      | Sentinel form of `*StaleConfigChangeError`                   |
| `ErrNonVotingRoleInMembers` | Sentinel form of `*NonVotingRoleInMembersError`              |
| `ErrInvalidSnapshotOption`  | Sentinel form of `*SnapshotOptionError`                      |
| `ErrRecoveredTermUnknown`   | An on-disk SM's recovered applied index has no resolvable Raft term. Seeding term 0 would make `isUpToDate` grant a vote to every candidate, so the shard start fails instead |

#### Membership policy

Aliases of the internal raft sentinels, matchable with `errors.Is`.

| Error                          | Returned When                                                 |
|--------------------------------|---------------------------------------------------------------|
| `ErrTooManyWitnesses`          | Witness add would exceed the shard's replicated `MaxWitnesses` |
| `ErrBreaksFullReplicaMajority` | Change would leave full replicas unable to form a quorum      |
| `ErrRemoveLastFullReplica`     | Change would remove the shard's only full replica             |

#### Sessions

| Error                          | Returned When                                              |
|--------------------------------|------------------------------------------------------------|
| `ErrSessionLimitExceeded`      | Shard-wide or per-owner session limit reached. `ErrTooManySessions` is a legacy alias for the same value |
| `ErrSessionNotFound`           | Referenced session does not exist                          |
| `ErrSessionExpired`            | Sentinel form of `*SessionExpiredError`                    |
| `ErrSessionAlreadyExists`      | Registering a duplicate session                            |
| `ErrResponseLimitExceeded`     | Session response cache full                                |
| `ErrSessionsUnsupportedOnDisk` | Any session entry point on a shard backed by an `sm.DiskStateMachine` |
| `ErrEntropyDegenerate`         | The system CSPRNG returned the rejected value on every attempt while minting a client ID |

#### Snapshot errors

| Error                                   | Returned When                                                  |
|-----------------------------------------|----------------------------------------------------------------|
| `ErrSnapshotNotFound`                   | No snapshot available on disk                                  |
| `ErrSnapshotInProgress`                 | The shard's snapshot slot is taken                             |
| `ErrSnapshotRecoveryFailed`             | Snapshot restore corruption or incompatibility. Produced as `*SnapshotFailedError` with `Recovery` set |
| `ErrSnapshotTooLarge`                   | Sentinel form of `*SnapshotTooLargeError`                      |
| `ErrSnapshotSizeMismatch`               | On-disk size disagrees with the `FileSize` in the message      |
| `ErrSnapshotCorrupt`                    | Corrupt or oversized frame length prefix during decryption     |
| `ErrSnapshotDestinationRecorded`        | A received snapshot's canonical directory is the one LogDB records as this replica's live snapshot |
| `ErrSnapshotBarrierSealed`              | A snapshot could not be **written** because the shard's barrier is configured but sealed |
| `ErrSnapshotEncryptedBarrierUnavailable` | An encrypted snapshot could not be **read** because no barrier is configured for the shard or it is sealed |
| `ErrImportLayoutUnknown`                | Sentinel form of `*ImportLayoutUnknownError`                   |
| `ErrImportDataDirMismatch`              | Sentinel form of `*ImportDataDirMismatchError`                 |
| `ErrImportSnapshotDirExists`            | Sentinel form of `*ImportSnapshotDirExistsError`               |
| `ErrImportMembershipInvalid`            | Sentinel form of `*ImportMembershipInvalidError`               |

`*ImportSnapshotOutOfDateError` deliberately has no local sentinel: it matches `logdb.ErrSnapshotOutOfDate`, which every LogDB backend returns for the same condition.

#### Encryption and storage

| Error                          | Returned When                                                    |
|--------------------------------|------------------------------------------------------------------|
| `ErrBarrierSealed`             | Encrypt or decrypt while the barrier is sealed. Re-exported from `pkg/seal` |
| `ErrBarrierNotInit`            | Barrier has no sealing strategy. Re-exported from `pkg/seal`     |
| `ErrDecryptionFailed`          | Corrupted ciphertext or wrong key. Re-exported from `pkg/seal`   |
| `ErrShardBarriersPlaintextWAL` | `WithShardBarriers` over a LogDB that would store plaintext      |
| `ErrShardBarrierUnresolved`    | The per-shard resolver returned nil and there is no host-wide barrier |
| `ErrShardBarrierLogDBMismatch` | The LogDB stores this shard under a different barrier than the resolver returned |
| `ErrLogDBWrite`                | LogDB write failure                                              |
| `ErrDecompressionBomb`         | Decompressed payload exceeds the configured limit                |

### Structured Error Types

#### NotLeaderError

```go
type NotLeaderError struct {
    ShardID       uint64
    LeaderID      uint64 // 0 if unknown
    LeaderAddress string // empty if unknown
}
```

Returned by membership operations and `RequestLeaderTransfer` on a non-leader, and by `Propose` when no leader is known at all. Carries a hint so the client can redirect without a discovery round. Satisfies `errors.Is(err, ErrNotLeader)`.

Prefer `NotLeader(err)` over a type assertion: it also matches the bare sentinel, and it returns `(0, true)` when the leader is unknown, which is the case you must not treat as an address.

#### EntryTooLargeError

```go
type EntryTooLargeError struct {
    Size    uint64
    MaxSize uint64
}
```

Returned by `Propose` when `len(cmd)` exceeds `min(MaxProposalPayloadSize, MaxEntrySize)`. `MaxSize` reports the effective limit that was applied, not either configured value on its own. Satisfies `errors.Is(err, ErrEntryTooLarge)`.

#### BusyError

```go
type BusyError struct {
    ShardID     uint64
    CurrentSize uint64
    MaxSize     uint64
    // Has unexported fields.
}
```

Returned by `Propose` when the per-shard in-memory log exceeds `MaxInMemLogSize`. Satisfies `errors.Is(err, ErrBusy)`.

`ErrSystemBusy` is itself a `*BusyError`, so the global limiter and the per-shard limiter are both caught by a single `errors.Is(err, ErrBusy)` check.

#### SessionError

```go
type SessionError struct {
    ClientID uint64
    Op       string // "register" or "unregister"
    Err      error
}
```

Wraps a session register or unregister failure. `Err` is the underlying cause: `ErrInvalidOperation` when the shard or the owner is at its session cap, `ErrEntropyDegenerate` when the CSPRNG failed to mint a client ID. Implements `Unwrap()`.

#### SessionExpiredError

```go
type SessionExpiredError struct {
    ShardID  uint64
    ClientID uint64
}
```

Returned when a session-managed proposal references a client ID whose session is no longer registered, after expiry or a snapshot restore that did not include it. Satisfies `errors.Is(err, ErrSessionExpired)`.

#### ShardNotStoppedError

```go
type ShardNotStoppedError struct {
    ShardID   uint64
    ReplicaID uint64
}
```

Returned when attempting to remove data for a running shard. Satisfies `errors.Is(err, ErrShardNotStopped)`.

#### ReplicaNotFoundError

```go
type ReplicaNotFoundError struct {
    ShardID   uint64
    ReplicaID uint64
}
```

Returned when a replica ID does not match the local replica (`StopReplica`, `RequestCompaction`) or names no member of the shard (`RequestLeaderTransfer`). Satisfies `errors.Is(err, ErrReplicaNotFound)`.

#### StaleConfigChangeError

```go
type StaleConfigChangeError struct {
    ShardID  uint64
    Expected uint64 // the shard's current ConfigChangeID
    Got      uint64 // the configChangeIndex the caller supplied
}
```

Returned when the config change index guard fails. Satisfies `errors.Is(err, ErrStaleConfigChange)`. Re-read the membership with `SyncGetShardMembership` and build a fresh request; retrying the identical one fails forever.

#### HostInitError

```go
type HostInitError struct {
    Field string
    Err   error
}
```

Wraps component initialization failures in `NewHost` and `StartShard`. Implements `Unwrap()`.

#### SnapshotSendError

```go
type SnapshotSendError struct {
    ShardID   uint64
    ReplicaID uint64
    Op        string // which step of the send failed
    Err       error
}
```

Returned when a snapshot send fails. Implements `Unwrap()`.

#### SnapshotReceiveError

```go
type SnapshotReceiveError struct {
    ShardID   uint64
    ReplicaID uint64
    Op        string // which step of the receive failed
    Err       error
}
```

Returned when a snapshot receive fails. Implements `Unwrap()`.

#### SnapshotFailedError

```go
type SnapshotFailedError struct {
    ShardID   uint64
    ReplicaID uint64
    Recovery  bool // true when installing an artifact, false when writing one out
    Err       error
}
```

Satisfies `errors.Is(err, ErrSnapshotRecoveryFailed)` when `Recovery` is true. Implements `Unwrap()`, so the underlying engine cause stays reachable.

#### SnapshotTooLargeError

```go
type SnapshotTooLargeError struct {
    ShardID uint64
    MaxSize uint64
}
```

Satisfies `errors.Is(err, ErrSnapshotTooLarge)`.

#### SnapshotArtifactError

```go
type SnapshotArtifactError struct {
    Reason string // "read header", "bad magic", "decode membership", ...
    Err    error
}
```

The export artifact header is malformed or truncated. Implements `Unwrap()`.

#### SnapshotOptionError

```go
type SnapshotOptionError struct {
    Field  string
    Reason string
}
```

Returned by `SnapshotOption.Validate()`. Satisfies `errors.Is(err, ErrInvalidSnapshotOption)`.

#### SnapshotCommittedNotDurableError

```go
type SnapshotCommittedNotDurableError struct {
    Path  string // destination artifact, which already holds the migration
    Epoch uint64 // the destination's minimum at-rest epoch
    Err   error
}
```

Returned by `ReencryptSnapshotFile` for the one failure that does not leave the destination untouched: a directory fsync that failed *after* the rename. The artifact is committed but not proven durable. The caller must record `Epoch` -- dropping it lets the next DEK purge orphan the artifact's key -- and treat the migration as unverified until it re-checks the destination. Implements `Unwrap()`.

#### UnauthorizedMessageError

```go
type UnauthorizedMessageError struct {
    ShardID uint64
    From    uint64
    MsgType uint64
}
```

An inbound Raft message failed shard-membership authorization.

#### TypeAssertionError

```go
type TypeAssertionError struct {
    Context      string
    ExpectedType string
    ActualValue  interface{}
}
```

Wraps internal type assertion failures for debugging.

#### LoopPanicError

```go
type LoopPanicError struct {
    LoopName string
    Value    any
    Stack    []byte
}
```

Wraps a recovered panic from a goroutine loop, with the stack captured at recovery.

#### EmptyMembersError

```go
type EmptyMembersError struct {
    ShardID uint64
}
```

Returned when bootstrapping a shard with no members.

#### SelfNotInMembersError

```go
type SelfNotInMembersError struct {
    ShardID   uint64
    ReplicaID uint64
}
```

Returned when the local replica is not in the initial members list.

#### InvalidMemberError

```go
type InvalidMemberError struct {
    ShardID   uint64
    ReplicaID uint64
    Address   string
    Reason    string
}
```

A member in the initial members map has a zero replica ID, an empty or malformed address, or an address another member already claims. Satisfies `errors.Is(err, ErrInvalidMember)`.

#### NonVotingRoleInMembersError

```go
type NonVotingRoleInMembersError struct {
    ShardID   uint64
    ReplicaID uint64
    Role      string // "witness" or "observer"
}
```

`StartShard` was asked to bootstrap a witness or observer through the initial members map, which seeds the full voter set. Satisfies `errors.Is(err, ErrNonVotingRoleInMembers)`.

#### MaybeAppliedError

```go
type MaybeAppliedError struct{ Err error }
```

Wraps an error whose request had already been submitted to Raft when it failed, so the entry may still commit and apply. Use `MaybeApplied(err)` rather than a type assertion. Wrapping preserves every existing `errors.Is`, `Temporary` and `NotLeader` result. Implements `Unwrap()`.

#### BootstrapEntryMissingError

```go
type BootstrapEntryMissingError struct {
    Index     uint64 // the bootstrap membership index inspected
    EntryType uint64 // the masked entry type actually found there
}
```

The durable log holds something other than the founding configuration at the bootstrap index. Config-in-log is the only bootstrap this library performs, so every shard it creates commits an `EntryInitialMembership` there before anything else can be appended; anything else means the log was damaged. The remedy is operational: restore this replica's data directory from backup, or wipe it and re-add the replica with `join=true` so the configuration arrives by replication. Rebuilding membership from the caller's `members` map instead would silently drop witnesses, observers, removal tombstones, committed identities and replicated policy.

#### CorruptConfigEntryError

```go
type CorruptConfigEntryError struct {
    ShardID uint64
    Index   uint64
    Kind    string // "config change" or "bootstrap membership"
    Err     error
}
```

A committed configuration entry failed to decode. Surfaced through the shard's failure path rather than returned from a call. Implements `Unwrap()`.

#### Import errors

```go
type ImportLayoutUnknownError struct{ ShardID, ReplicaID uint64 }
type ImportDataDirMismatchError struct {
    ShardID, ReplicaID uint64
    Declared, Recorded string
}
type ImportSnapshotDirExistsError struct {
    ShardID, ReplicaID, Index uint64
    Dir                       string
}
type ImportSnapshotOutOfDateError struct {
    ShardID, ReplicaID uint64
    Index              uint64 // the artifact's snapshot index
    Recorded           uint64 // the index already recorded in LogDB
}
type ImportMembershipInvalidError struct {
    ShardID, ReplicaID uint64
    Reason             string
}
```

See [ImportSnapshot](#importsnapshot). Each matches its `ErrImport*` sentinel, except `ImportSnapshotOutOfDateError`, which matches `logdb.ErrSnapshotOutOfDate`.

## Metrics Interface

### MetricsCollector

```go
type MetricsCollector interface {
    RaftMetrics
    LogDBMetrics
    TransportMetrics
    SecurityMetrics

    RegisterShard(shardID uint64)
    UnregisterShard(shardID uint64)
}
```

Composes four sub-interfaces plus the two shard-lifecycle hooks. Passed to `WithMetrics()`. Internally only the relevant sub-interface is threaded into each component: `RaftMetrics` into the engine, `TransportMetrics` into the transport, `LogDBMetrics` into the WAL.

`RegisterShard` and `UnregisterShard` exist so implementations can pre-bind per-shard metrics once at shard start instead of doing a label lookup on every hot-path call. `PrometheusMetricsCollector` is the reference implementation of that pattern.

### RaftMetrics (10 methods)

| Method                      | Signature                            |
|-----------------------------|--------------------------------------|
| `ObserveProposalLatency`    | `(shardID uint64, d time.Duration)`  |
| `ObserveCommitLatency`      | `(shardID uint64, d time.Duration)`  |
| `ObserveApplyLatency`       | `(shardID uint64, d time.Duration)`  |
| `ObserveReadIndexLatency`   | `(shardID uint64, d time.Duration)`  |
| `IncProposalDropped`        | `(shardID uint64)`                   |
| `ObserveProposalBatchSize`  | `(shardID uint64, size int)`         |
| `ObserveCommitBatchSize`    | `(size int)`                         |
| `IncElection`               | `(shardID uint64)`                   |
| `IncSnapshot`               | `(shardID uint64)`                   |
| `IncPiggybackCommit`        | `(shardID uint64)`                   |

`ObserveReadIndexLatency` covers the full ReadIndex round-trip: heartbeat quorum plus any wait for the state machine to catch up. A piggyback commit is one the commit worker detected and applied in the same cycle, saving a second pipeline traversal.

### LogDBMetrics (4 methods)

| Method                    | Signature              |
|---------------------------|------------------------|
| `ObserveWriteLatency`     | `(d time.Duration)`    |
| `ObserveFsyncLatency`     | `(d time.Duration)`    |
| `ObserveCompactionLatency`| `(d time.Duration)`    |
| `IncWriteBytes`           | `(n int64)`            |

### TransportMetrics (7 methods)

| Method                   | Signature                                        |
|--------------------------|--------------------------------------------------|
| `ObserveSendLatency`     | `(d time.Duration)`                              |
| `IncMessagesSent`        | `(shardID, to uint64, msgType uint64)`           |
| `IncMessagesReceived`    | `(shardID, from uint64, msgType uint64)`         |
| `IncSnapshotsSent`       | `(shardID uint64, bytes int64, d time.Duration)` |
| `IncSnapshotsReceived`   | `(shardID uint64, bytes int64, d time.Duration)` |
| `IncSnapshotsRejected`   | `(shardID uint64, reason string)`                |
| `IncMessagesDropped`     | `(shardID, to uint64, reason string)`            |

Both `reason` arguments come from closed sets fixed by the transport. Treat them as metric labels and never derive new label values from them.

| Method                 | Valid `reason` values                                                                   |
|------------------------|------------------------------------------------------------------------------------------|
| `IncSnapshotsRejected` | `shutdown`, `concurrency_limit`, `memory_budget`, `liveness`, `malformed`, `io_error`, `policy` |
| `IncMessagesDropped`   | `unresolved_target`, `queue_full`, `stream_unavailable`, `write_failed`, `marshal_failed`, `protocol_violation`, `unsendable`, `peer_rejected` |

`shardID` is 0 when a snapshot was rejected before its header parsed; `shardID` and `to` are both 0 for `protocol_violation`.

`unsendable` and `peer_rejected` are worth separating from the rest in an alerting rule. `unsendable` is a message no peer can accept -- more entries than `proto.MaxEntriesPerMessage`, or larger on its own than a frame -- refused by the send path before it is written, so retrying it unchanged can never succeed. `peer_rejected` is a batch the peer's decoder refused via a stream reset: the connection is healthy and only the frame was unacceptable, which usually means a version skew between the two ends. The `IncMessagesDropped` doc comment in `pkg/metrics.go` currently lists only the first six.

### SecurityMetrics (2 methods)

| Method                           | Signature                                 |
|----------------------------------|-------------------------------------------|
| `IncUnauthorizedMessageDropped`  | `(shardID, from uint64, reason string)`   |
| `IncUnauthorizedSnapshotDropped` | `(shardID, from uint64)`                  |

These are counted rather than only logged because every increment is either a misconfigured cluster or an injection attempt by an authenticated-but-compromised peer, and both need an alerting rule. The split by `reason` is operational: one aggregate counter cannot tell a rolling membership change from an attack.

| `reason`            | Meaning                                                                                    |
|---------------------|--------------------------------------------------------------------------------------------|
| `unknown_member`    | No configuration this replica holds (committed, pending in its own log, or `Config.MemberIdentities`) names the sender. Transient during a membership change; persistent means this replica missed the change that added the sender — restart it with a map that names the leader |
| `identity_mismatch` | The strongest signal here. Committed membership records a node identity for the claimed replica and the connection presented a different one, so the sender does not hold that replica's private key. A forged `From`, or a stale identity after a key rotation. Never routine |
| `missing_identity`  | The shard records no identity for a replica the configuration names. Admission requires one on every host with a transport, so this is a configuration written by a build that did not; wipe and re-bootstrap |
| `removed_replica`   | The sender was removed from the shard                                                      |
| `misrouted`         | The message names a shard this host does not serve                                         |

The dropped sender's replica ID is deliberately not a Prometheus label. It is by definition not a current member and arrives unauthenticated, so an attacker could emit arbitrarily many distinct values and create unbounded, never-reclaimed child series. It is in the drop log line for forensics.

### NoOpMetricsCollector

```go
type NoOpMetricsCollector struct{}
```

Every method is a no-op. Embed it and override selectively rather than implementing the full interface, which also keeps your implementation compiling when a method is added.

### SampledMetricsCollector

```go
type SampledMetricsCollector struct {
    MetricsCollector
    SampleRate uint64 // inverse rate: 100 records 1 observation in 100
    // Has unexported fields.
}

func NewSampledMetricsCollector(inner MetricsCollector, sampleRate uint64) *SampledMetricsCollector
```

Embeds another collector and thins the two hottest **histogram** observations, commit latency and apply latency, to 1 in `SampleRate`. A `sampleRate` of 0 becomes 100. Sampling a histogram costs resolution but keeps the distribution representative.

Counters are never sampled. A sampled `Counter.Inc()` would make every `*_total` series under-report by `SampleRate`, and every `rate()` built on it would be off by orders of magnitude, so message send and receive counts forward every call. Each sampled metric owns its own counter: a single shared counter would make the sampling decision depend on the interleaving of the two metrics, and with an even rate and a strictly alternating call pattern one metric would be sampled always and the other never.

### Panic containment

A caller-supplied `MetricsCollector` is wrapped internally so a panic inside it cannot unwind an engine worker. Panics are counted and logged at the first occurrence and then at powers of two, because a collector that panics on every call is on the Raft receive path and an unsampled log line there is its own denial of service. `NoOpMetricsCollector` and an already-wrapped collector are passed through unwrapped.

The interface is deliberately not documented as "must not panic": a library cannot enforce that, and a metrics backend that panics on a full buffer or a closed registry is an ordinary bug rather than a contract violation.

### PrometheusMetricsCollector

```go
func NewPrometheusMetricsCollector() *PrometheusMetricsCollector
func NewPrometheusMetricsCollectorWithConfig(cfg config.MetricsConfig) *PrometheusMetricsCollector
```

Production implementation using `prometheus/client_golang`. All metrics are registered on a dedicated `prometheus.Registry` (exposed as `collector.Registry`) to avoid polluting the global default.

`NewPrometheusMetricsCollector` uses the zero `config.MetricsConfig`, so **`PerShardMetrics` is off** and the volume-and-latency telemetry aggregates to one host-level series per metric. Use `NewPrometheusMetricsCollectorWithConfig` to opt in to a `shard_id` breakdown. The mode is fixed at construction and cannot be changed afterwards.

**Prometheus metric names.** `shard_id*` marks a label present only when `PerShardMetrics` is enabled. See [operations/deployment.md](../multi-group-host/user.md#metrics-cardinality) for the cardinality budget, the alerting-safe subset, and the full reference including the drop and security counters.

| Subsystem   | Metric Name                               | Type      | Labels                    |
|-------------|-------------------------------------------|-----------|---------------------------|
| `raft`      | `quicraft_raft_proposal_latency_seconds`  | Histogram | `shard_id*`               |
| `raft`      | `quicraft_raft_commit_latency_seconds`    | Histogram | `shard_id*`               |
| `raft`      | `quicraft_raft_apply_latency_seconds`     | Histogram | `shard_id*`               |
| `raft`      | `quicraft_raft_read_index_latency_seconds`| Histogram | `shard_id*`               |
| `raft`      | `quicraft_raft_proposal_batch_size`       | Histogram | `shard_id*`               |
| `raft`      | `quicraft_raft_snapshots_total`           | Counter   | `shard_id*`               |
| `raft`      | `quicraft_raft_piggyback_commits_total`   | Counter   | `shard_id*`               |
| `raft`      | `quicraft_raft_proposals_dropped_total`   | Counter   | `shard_id`                |
| `raft`      | `quicraft_raft_elections_total`           | Counter   | `shard_id`                |
| `raft`      | `quicraft_raft_commit_batch_size`         | Histogram | --                        |
| `logdb`     | `quicraft_logdb_write_latency_seconds`    | Histogram | --                        |
| `logdb`     | `quicraft_logdb_fsync_latency_seconds`    | Histogram | --                        |
| `logdb`     | `quicraft_logdb_compaction_latency_seconds` | Histogram | --                      |
| `logdb`     | `quicraft_logdb_write_bytes_total`        | Counter   | --                        |
| `transport` | `quicraft_transport_send_latency_seconds` | Histogram | --                        |
| `transport` | `quicraft_transport_messages_sent_total`  | Counter   | --                        |
| `transport` | `quicraft_transport_messages_received_total` | Counter | --                       |
| `transport` | `quicraft_transport_messages_sent_detailed_total` | Counter | `shard_id`, `to`, `msg_type` |
| `transport` | `quicraft_transport_messages_received_detailed_total` | Counter | `shard_id`, `from`, `msg_type` |
| `transport` | `quicraft_transport_snapshots_sent_total` | Counter   | `shard_id*`               |
| `transport` | `quicraft_transport_snapshots_sent_bytes` | Histogram | `shard_id*`               |
| `transport` | `quicraft_transport_snapshots_received_total` | Counter | `shard_id*`             |
| `transport` | `quicraft_transport_snapshots_received_bytes` | Histogram | `shard_id*`           |
| `transport` | `quicraft_transport_messages_dropped_total` | Counter | `shard_id`, `reason`      |
| `transport` | `quicraft_transport_snapshots_rejected_total` | Counter | `shard_id`, `reason`    |
| `security`  | `quicraft_security_unauthorized_messages_dropped_total` | Counter | `shard_id`, `reason` |
| `security`  | `quicraft_security_unauthorized_snapshots_dropped_total` | Counter | `shard_id`        |
| `metrics`   | `quicraft_metrics_per_shard_enabled`      | Gauge     | --                        |

The two `*_detailed_total` series are emitted only when `collector.EnablePerMessageMetrics` is set to true after construction. They are off by default because they are per-message on the hot path and their label set is the product of shard count, membership size and message type.

Latency buckets: 100us to 10s. Batch size buckets: 1 to 1024 (powers of 2). Snapshot byte buckets: 1KB to 1GB.

## Configuration

### HostConfig (per-host)

Source: `pkg/config/hostconfig.go`

| Field                   | Type            | Default              | Description                              |
|-------------------------|-----------------|----------------------|------------------------------------------|
| `WALDir`                | `string`        | (required)           | Write-ahead log directory                |
| `NodeHostDir`           | `string`        | (required)           | Metadata, snapshots, host ID             |
| `RaftAddress`           | `string`        | (required)           | External address peers dial              |
| `ListenAddress`         | `string`        | (required)           | Local bind address (host:port)           |
| `RTTMillisecond`        | `uint64`        | `200`                | Estimated round-trip time (ms). Must be 1..60000 |
| `DeploymentID`          | `uint64`        | (required, non-zero) | Cluster identity token                   |
| `AllowZeroDeploymentID` | `bool`          | `false`              | Permits `DeploymentID == 0`. Development and testing only |
| `HostID`                | `string`        | `""`                 | Host identifier. When empty, generated or loaded from `NodeHostDir/host.id` |
| `NumWorkers`            | `uint64`        | `runtime.NumCPU()`   | Workers per tier                         |
| `MaxApplyWorkers`       | `int`           | `runtime.NumCPU()`   | Max concurrent apply goroutines          |
| `CommitCBufferSize`     | `int`           | `64`                 | Commit channel buffer size               |
| `ShutdownTimeout`       | `time.Duration` | `30s`                | Graceful shutdown timeout                |
| `MaxWALDiskSize`        | `uint64`        | `0` (unlimited)      | Max WAL directory size (bytes)           |
| `NoSyncWAL`             | `bool`          | `false`              | Disable WAL fdatasync (testing only)     |
| `MaxTotalInMemLogSize`  | `uint64`        | `0` (disabled)       | Global rate limiter threshold (bytes)    |
| `KeyRotationInterval`   | `time.Duration` | `0` (disabled)       | Scheduled barrier key rotation interval  |
| `MaxRetainedEpochs`     | `uint64`        | `0` (retain all)     | Barrier epochs retained after rotation   |
| `NotifyCommit`          | `bool`          | `false`              | Enables `RequestState.CommittedC()`      |
| `EventListener`         | `*EventListener`| `nil`                | Event callbacks. Nil means zero overhead |
| `TransportConfig`       | `TransportConfig`| (see below)         | Transport tuning                         |
| `LogConfig`             | `LogConfig`     | text/info            | Logging format and level                 |

`RaftAddress` and `ListenAddress` are separate because they differ under NAT, port mapping and containers: peers dial `RaftAddress`, the listener binds `ListenAddress`.

`KeyRotationInterval` of 0 disables *scheduled* rotation only. Invocation-threshold rotation, which enforces the NIST SP 800-38D AEAD budget, still runs whenever a barrier is configured. A recommended production value is 24 hours. It requires `WithBarrier` and is ignored otherwise.

> **Apply failure note:** Apply is **fail-fast**, and there is no setting that changes that. The first `SM.Update()` or decompression error marks the shard failed -- it goes offline until reload or snapshot recovery -- and `lastApplied` is not advanced. Advancing `lastApplied` past a failed batch would diverge this state machine from its peers, so retrying in place is not done. A `MaxApplyRetries` field existed through 0.5.1-alpha and never had any effect; it was removed.

> **`NoSyncWAL` note:** Disables `fdatasync` on Raft WAL segment writes, trading durability for throughput. Intended only for test environments where filesystem fsync latency (overlay2 under Docker, for instance) causes proposal timeouts. Never enable in production.

### Config (per-shard)

Source: `pkg/config/config.go`

| Field                    | Type             | Default        | Description                              |
|--------------------------|------------------|----------------|------------------------------------------|
| `ShardID`                | `uint64`         | (required)     | Globally unique shard identifier         |
| `ReplicaID`              | `uint64`         | (required)     | Unique replica ID within the shard       |
| `IsObserver`             | `bool`           | `false`        | Non-voting learner. Join-only, see below |
| `IsWitness`              | `bool`           | `false`        | Voting member with no state machine. Join-only, see below |
| `Witness`                | `WitnessConfig`  | zero value     | Seeds the shard's replicated witness policy at bootstrap |
| `MemberIdentities`       | `map[uint64]string` | `nil`       | Founders: seeds the replicated node identities, every member named. Joiners: the anchor. Any entry beyond the members map is operator trust for the receive gate. Required on a host with a transport |
| `RequireMemberIdentities` | `bool`          | set by the Host | `true` on every shard behind a transport: admission refuses an identity-less member |
| `CheckQuorum`            | `*bool`          | `nil` = on     | Leader steps down on quorum loss. Disabling is unsafe |
| `PreVote`                | `*bool`          | `nil` = on     | Pre-vote extension. Disabling is unsafe  |
| `OrderedConfigChange`    | `*bool`          | `nil` = on     | Serialize membership changes             |
| `ElectionRTT`            | `uint64`         | `10`           | Election timeout in `RTTMillisecond` multiples |
| `HeartbeatRTT`           | `uint64`         | `1`            | Heartbeat interval in `RTTMillisecond` multiples |
| `SnapshotEntries`        | `uint64`         | `100000`       | Applied entries between automatic snapshots. `config.SnapshotsDisabled` turns them off |
| `CompactionOverhead`     | `uint64`         | `5000`         | Entries retained after compaction        |
| `MaxInMemLogSize`        | `uint64`         | `0` (disabled) | Per-shard rate limit threshold (bytes)   |
| `MaxProposalPayloadSize` | `uint64`         | `8 MB`         | User-facing proposal cap. May not exceed `proto.MaxEntrySize` |
| `MaxEntrySize`           | `uint64`         | `8 MB`         | Transport and storage entry cap; also the decompression ceiling on apply |
| `MaxApplyEntrySize`      | `uint64`         | `16711672`     | Replication batch chunk size (`config.MaxReplicationPayloadBytes`, ~15.94 MB) |
| `MaxSnapshotSize`        | `uint64`         | `4 GB`         | Maximum snapshot file size               |
| `MaxSessionsPerClient`   | `uint64`         | `128`          | Cached responses retained per session. Bootstrap seed; replicated |
| `MaxTotalSessions`       | `uint64`         | `16384`        | Shard-wide session cap. Bootstrap seed; replicated |
| `MaxSessionsPerOwner`    | `uint64`         | `128`          | Per-owner session cap. `config.SessionsPerOwnerUnlimited` removes it. Bootstrap seed; replicated |
| `SessionExpiryEntries`   | `uint64`         | `100000`       | Session inactivity threshold in log entries. `config.SessionExpiryDisabled` turns expiry off. Bootstrap seed; replicated |
| `Quiesce`                | `bool`           | `false`        | Quiescence for idle shards               |
| `ZeroCopyEntryCmd`       | `bool`           | `false`        | `sm.Entry.Cmd` aliases an internal buffer. Unsafe if retained past `Update` |
| `LeaseRead`              | `bool`           | `false`        | Lease-based reads (Raft PhD 6.4). Requires `CheckQuorum` |
| `DataDir`                | `string`         | `""`           | Absolute root for this shard's on-disk state machine data. Empty places it under `NodeHostDir` |

**Zero no longer means "off".** `SnapshotEntries`, `SessionExpiryEntries` and `MaxSessionsPerOwner` all take their default when left at zero, and each has a named sentinel for disabling. An unset field cannot mean both "I did not choose" and "never do this", and in each case the previous zero-means-off behavior shipped a defect as the default:

- Never snapshotting is not a viable steady state. The log and restart replay grow without bound, and in this implementation the snapshot-directory GC, the epoch purge floor and the at-rest barrier all advance only inside a snapshot save. A shard that never snapshots never reclaims a directory, never zeroes an old epoch's DEK, and never asks its barrier to encrypt anything.
- A client that registers a session and dies without `CloseSession` leaks an entry that is serialized into every subsequent snapshot and counts against `MaxTotalSessions` forever. Nothing else reclaims it.
- An owner able to register 16384 sessions is an owner able to deny at-most-once delivery to every other tenant on the shard.

`SessionExpiryEntries` is measured in log entries, not time, so the wall-clock window scales inversely with throughput: generous on a quiet shard, and still about ten seconds of client silence on one sustaining 10k entries/s.

`MaxProposalPayloadSize` and `MaxEntrySize` are both enforced at `Propose`; the effective limit is the minimum of the two. Set the former to impose a tighter user-facing cap while leaving the latter at the transport default.

> **`IsObserver` / `IsWitness` are join-only.** The authoritative role lives in
> replicated membership (`Membership.Observers` / `Membership.Witnesses`), not in
> the local config. These bits only tell the local replica what it was started
> as. The `members` map passed to `StartShard` seeds the **full voter set** on
> every replica that lists it, so a witness or observer named there is a full
> voter in the configuration the whole shard agrees on: the leader replicates
> full entry payloads to it, it counts toward every quorum, and only this host's
> local read gate still behaves like a non-voter. `StartShard` rejects that shape
> with `*NonVotingRoleInMembersError`. Bootstrap the voters first, then start the
> replica with `join=true` and add it with `RequestAddWitness` or
> `RequestAddObserver`.
>
> On a **restart** the same map must list every current member, non-voters
> included, or they vanish from this replica's routing and remotes. The
> rejection therefore applies only to a first start with no persisted state. The
> seeded voter placement that results is provisional: the committed
> `AddWitness` / `AddNonVoting` entries replay at commit time and move each
> replica to its real role (see "Convergent Apply" in
> [raft/protocol.md](../raft-consensus/engineering.md)).

### WitnessConfig

Source: `pkg/config/config.go`

```go
type WitnessConfig struct {
    MaxWitnesses               int    // 0 = no explicit cap; negative is a validation error
    RequireFullReplicaMajority *bool  // nil = true
}

func (w WitnessConfig) GetRequireFullReplicaMajority() bool
```

`Config.Witness` **seeds** the policy at bootstrap and nothing more. From then on the policy is replicated state in `proto.Membership`, changed only through `Host.RequestUpdateWitnessPolicy`. Editing this struct on a running node has no effect, and a restart recovers the replicated value rather than the local one.

`RequireFullReplicaMajority` rejects any config change -- witness add or full-replica removal -- that would leave the full, data-bearing replicas unable to form a quorum over the voting set. Adding a third witness to {3 full + 2 witnesses} is rejected because 3 full is below the quorum of 4; {2 full + 1 witness} is allowed because 2 >= 2. This is a bound on membership *shape* only: even in a compliant membership a commit can still rest on leader-plus-witnesses when the other full replicas lag.

### TransportConfig

Source: `pkg/config/transportconfig.go`

| Field                          | Type               | Default     | Description                       |
|--------------------------------|--------------------|-------------|-----------------------------------|
| `MTLSConfig`                   | `*MTLSConfig`      | `nil`       | CA cert plus either a PEM `Cert`/`Key` pair or a pre-built `*tls.Certificate`. Required unless `TransportDisabled` |
| `RevocationConfig`             | `*RevocationConfig`| `nil`       | CRL paths, OCSP responder, check interval, fail-open/closed mode |
| `StreamPoolSize`               | `int`              | `16`        | QUIC streams per connection       |
| `MaxStreamPoolSize`            | `int`              | `256`       | Upper bound for the stream pool   |
| `MaxStreamsPerConnection`      | `int`              | `256`       | QUIC stream limit advertised per connection |
| `StreamReadIdleTimeout`        | `time.Duration`    | `30s`       | Idle read timeout on a stream (0 uses the default) |
| `MaxSnapshotReceiveRate`       | `int64`            | `256 MB/s`  | Inbound snapshot bandwidth limit (floor 64 KB/s) |
| `MaxConcurrentSnapshotReceives`| `int`              | `4`         | Concurrent inbound snapshots      |
| `MaxSnapshotReceiveMemory`     | `int64`            | `1 GB`      | Total snapshot receive buffer     |
| `MaxMessageReceiveMemory`      | `int64`            | `256 MB`    | Total inbound message buffer      |
| `MaxDecompressedSize`          | `uint32`           | `16 MB`     | Max Snappy decompressed payload. Pinned to the frame size; any other value is a validation error |
| `MaxConnectionsPerIP`          | `int`              | `16`        | Connections per source IP         |
| `MaxIncomingConnections`       | `int`              | `256`       | Total inbound connections         |
| `SendBatchMaxSize`             | `int`              | `64 KB`     | Max send buffer before flush      |
| `UDPRecvBufSize`               | `int`              | `7 MB`      | UDP receive buffer, within typical `rmem_max` |
| `UDPSendBufSize`               | `int`              | `7 MB`      | UDP send buffer, within typical `wmem_max` |
| `Enable0RTT`                   | `bool`             | `false`     | QUIC 0-RTT. Replayable; requires `MTLSConfig` |
| `DisableCompression`           | `bool`             | `false`     | Disable Snappy compression        |
| `TransportDisabled`            | `bool`             | `false`     | Skip mTLS validation and transport construction. Equivalent to `WithoutTransport()` |

> **`MaxDecompressedSize` note:** `Validate` rejects any value other than `DefaultMaxDecompressedSize`. The send path admits frames against that same constant, so a lower value silently rejects frames every peer considers valid and stops replication to this node. It is a field for forward compatibility, not a tuning knob.

> **`MaxSnapshotReceiveRate` note:** `0` selects the default (256 MB/s). A non-zero value below the floor `MinMaxSnapshotReceiveRate` (64 KB/s = `64 * 1024`) is rejected by `TransportConfig.Validate`, and negative values are also rejected. The floor exists because a pathologically small rate would make the receiver's proportional-sleep limiter compute multi-day per-chunk sleeps while holding the snapshot concurrency semaphore and memory budget. As defense in depth, each per-chunk rate-limit sleep is additionally capped at 5 s; the configured rate is still honored on average across chunks.

mTLS is mandatory whenever the transport is enabled and there is no insecure fallback, so `Validate` rejects empty or malformed certificate material at the config boundary rather than letting it surface as an opaque handshake failure at runtime. A pre-built `Certificate` must carry a `crypto.Signer` private key, which is what lets a TPM- or PKCS#11-backed key work.

## State Machine Registration

Source: `pkg/sm/`

### CreateFunc

```go
type CreateFunc func(shardID, replicaID uint64) interface{}
```

The factory function type passed to `Host.StartShard`. Must return one of `StateMachine`, `ConcurrentStateMachine`, or `DiskStateMachine`. The Host detects the SM type via type assertion at startup.

### Type-Safe Wrappers

```go
func NewCreateFunc(fn CreateStateMachineFunc) CreateFunc
func NewConcurrentCreateFunc(fn CreateConcurrentStateMachineFunc) CreateFunc
func NewDiskCreateFunc(fn CreateDiskStateMachineFunc) CreateFunc
```

Wrap typed factory functions into `CreateFunc`, catching type errors at compile time:

```go
type CreateStateMachineFunc func(shardID, replicaID uint64) StateMachine
type CreateConcurrentStateMachineFunc func(shardID, replicaID uint64) ConcurrentStateMachine
type CreateDiskStateMachineFunc func(shardID, replicaID uint64) DiskStateMachine
```

### Determinism is mandatory

`Update` must be a pure function of (current state, entries). Every replica applies the identical entry sequence and must reach byte-identical state. In particular `Update` must not read wall-clock time, `math/rand`, environment variables, host identity, `GOMAXPROCS` or any other per-node value, and must not let Go's randomized map-iteration order affect the state it produces or the `Result`s it returns.

A violation is **not** detected by Raft. Consensus keeps agreeing on the log while replicas silently diverge in the state they derive from it, and the first symptom is usually a linearizable read returning different values depending on which replica served it. Implement `sm.Hasher` and compare replicas with `Host.StateMachineHash` if you need to detect it. The requirement is identical for all three interfaces.

### State Machine Interfaces

Three tiers, detected by type assertion at `StartShard` time:

**StateMachine** (type code 0) -- All state in memory. Serialized access: `Update` and `Lookup` are never called concurrently, enforced by the RSM adapter's `readGuard`. Local reads therefore wait on an in-flight apply; see [Lookup concurrency](../state-machines/engineering.md#lookup-concurrency).

```go
type StateMachine interface {
    Update(ctx context.Context, entries []Entry, results []Result) error
    Lookup(ctx context.Context, query interface{}) (interface{}, error)
    SaveSnapshot(ctx context.Context, w io.Writer, stopper <-chan struct{}) error
    RecoverFromSnapshot(ctx context.Context, r io.Reader, stopper <-chan struct{}) error
    Close(ctx context.Context) error
}
```

**ConcurrentStateMachine** (type code 1) -- Contracted for concurrent Lookup during Update, so it must synchronize itself. Two-phase snapshotting. The adapter's guard currently serializes this tier as well, which does not relax the contract.

```go
type ConcurrentStateMachine interface {
    Update(ctx context.Context, entries []Entry, results []Result) error
    Lookup(ctx context.Context, query interface{}) (interface{}, error)
    PrepareSnapshot() (interface{}, error)
    SaveSnapshot(ctx context.Context, snapshotCtx interface{}, w io.Writer, stopper <-chan struct{}) error
    RecoverFromSnapshot(ctx context.Context, r io.Reader, stopper <-chan struct{}) error
    Close(ctx context.Context) error
}
```

`Update` must be all-or-nothing: either every entry is applied and every `results[i]` written, or the state machine reverts to its pre-call state and returns an error. Partial application is not supported, and an error is fatal to the shard -- see the apply failure note.

**DiskStateMachine** (type code 2) -- Backed by persistent storage. Must handle idempotent replay after crash.

```go
type DiskStateMachine interface {
    Open(ctx context.Context, initialDir string, stopper <-chan struct{}) (uint64, error)
    Update(ctx context.Context, entries []Entry, results []Result) error
    Lookup(ctx context.Context, query interface{}) (interface{}, error)
    Sync() error
    PrepareSnapshot() (interface{}, error)
    SaveSnapshot(ctx context.Context, snapshotCtx interface{}, w io.Writer, stopper <-chan struct{}) error
    RecoverFromSnapshot(ctx context.Context, r io.Reader, stopper <-chan struct{}) error
    Close(ctx context.Context) error
}
```

### Supporting Types

```go
type Entry struct {
    Index uint64
    Cmd   []byte
}

type Result struct {
    Value uint64
    Data  []byte
}
```

`Entry.Cmd` is an owned copy by default. When `Config.ZeroCopyEntryCmd` is true it aliases an internal buffer that is reused after `Update` returns; call `Entry.CopyCmd()` for an owned copy if the data must outlive the call.

Two `Result.Value` codes are reserved by the RSM layer and must not be produced by application code:

| Constant                | Value             | Meaning                                                    |
|-------------------------|-------------------|-------------------------------------------------------------|
| `ResultSessionExpired`  | `MaxUint64 - 2`   | The entry referenced an unregistered client ID. The host translates it into `ErrSessionExpired` |
| `ResultInvalidCommand`  | `MaxUint64 - 3`   | The committed entry's payload could not be decoded          |

`ResultInvalidCommand` exists because a committed entry must be applied deterministically on every replica. Returning an `Update` error for a malformed payload would mark the shard failed and wedge it permanently on replay, so the entry yields this per-entry result and leaves the store unchanged instead.

### Sessions and on-disk state machines

A shard backed by an `sm.DiskStateMachine` supports **no-op sessions only**. `GetNewSession`, `GetNewSessionWithOwner`, `RegisterSession`, `ProposeWithSession`, `SyncProposeWithSession` and `CloseSession` all return `ErrSessionsUnsupportedOnDisk` there. Use `Host.GetNoOPSession` and make the operations idempotent, which is the same constraint dragonboat places on `IOnDiskStateMachine`.

The reason is `Open`. The session table is replicated state that travels only inside the snapshot stream, so it is restored by `RecoverFromSnapshot` -- and a `DiskStateMachine` reporting a non-zero applied index from `Open` never takes that path on restart, because recovering would replace its database files and roll it back to the snapshot index. The replica would come up with the right data and no sessions at all, then answer `ResultSessionExpired` for entries its peers apply normally. The client sees success because the leader completed the proposal from its own apply. That is permanent, silent divergence.

The refusal is a local, API-boundary decision reading only the state machine's own type, which is fixed for the life of the shard by the `sm.CreateFunc` passed to `StartShard`.

### Optional Interfaces

Detected via type assertion on the state machine instance:

| Interface      | Method                                          | Purpose                                     |
|----------------|-------------------------------------------------|---------------------------------------------|
| `Hasher`       | `GetHash() (uint64, error)`                     | Backs `Host.StateMachineHash`               |
| `NALookup`     | `BytesLookup(key []byte) ([]byte, error)`       | Byte-key reads without `interface{}` boxing |
| `NALookupInto` | `NALookup` + `BytesLookupInto(key, dst []byte) ([]byte, error)` | The allocation-free path used by `SyncReadBuf` and `StaleReadBuf` |

`BytesLookupInto` must copy the value into `dst`, growing it with `append` if needed, and return the populated sub-slice. Ownership of the returned slice belongs to the caller; the implementation must not retain a reference to it. A `Hasher` implementation must be order-independent -- both reference stores in `pkg/sm/kv` sort before hashing -- or it reports divergence that does not exist.

### CachedDiskStateMachine

```go
func NewCachedDiskStateMachine(inner DiskStateMachine, cfg CacheConfig) (*CachedDiskStateMachine, error)

type CacheConfig struct {
    MaxEntries   int              // must be > 0
    ExtractKV    EntryKeyFunc     // required
    EntryApplied EntryAppliedFunc // optional, but see below
}

type EntryKeyFunc func(cmd []byte) (key string, value []byte)
type EntryAppliedFunc func(cmd []byte, result Result) bool
```

Wraps a `DiskStateMachine` with a write-through LRU cache in front of disk reads, and implements `NALookup`. `CacheStats()` returns cumulative hits and misses.

`ExtractKV` returns `("", nil)` to skip an entry, and a non-empty key with a nil value to signal a cache delete.

**Conditional writes need `EntryApplied`.** The cache is populated from the value `ExtractKV` pulls out of the *command*, not from the store. For an unconditional write those agree. For a compare-and-set, put-if-absent or delete-if-match they do not: the command carries a value the store may have declined, and caching it would let a later `Lookup` return a value the store never held. Nothing here can infer the answer, because the outcome encoding is the state machine's own. `ResultInvalidCommand` is excluded from caching whether or not the predicate is set, and a predicate cannot opt back into it.

Invalidation happens before the inner write, not after. Repopulating afterwards would leave a window in which a `Lookup` racing `Update` returns the stale pre-write value from the cache while the write is already durable underneath. Deleting first degrades that window to a cache miss.

Cache hits return an owned copy. Handing out the live cache buffer would let a caller that mutates or pools the returned slice corrupt the cached value for every future reader.

`NewCachedDiskStateMachine` returns `*CacheMaxEntriesError` or `*InvalidCacheConfigError`. `BytesLookup` returns `*UnsupportedLookupResultError` when the wrapped `Lookup` answers with a type the byte-oriented path cannot represent, rather than `(nil, nil)` -- which would be indistinguishable from a genuine empty value and from a missing key.

## Event Listener

Source: `pkg/config/hostconfig.go`

```go
type EventListener struct { /* function fields */ }
```

Holds per-type optional callbacks. Only non-nil callbacks are invoked. Callbacks are called **synchronously** on engine worker goroutines and **must not block**.

### Raft Events

| Callback               | Info Type         | Fired When                                   |
|------------------------|-------------------|----------------------------------------------|
| `OnLeaderUpdated`      | `LeaderInfo`      | Leader change detected for a shard           |
| `OnLeaderLost`         | `EventShardInfo`  | Local node lost leadership                   |
| `OnCampaignLaunched`   | `EventShardInfo`  | Election campaign started                    |
| `OnCampaignSkipped`    | `EventShardInfo`  | **Never fires.** See below                   |
| `OnSnapshotRejected`   | `SnapshotInfo`    | Snapshot rejected by follower                |
| `OnProposalDropped`    | `EventShardInfo`  | Proposal dropped (queue full, etc.)          |
| `OnReadIndexDropped`   | `EventShardInfo`  | ReadIndex request dropped                    |

> **`OnCampaignSkipped` is currently inert.** The field exists on `EventListener`, the host forwards it correctly, and the engine's `Callback` interface declares it, but no engine code invokes it. A listener wired to it receives nothing. Do not build a leaderless-shard alert on it: the conditions it would report are exactly the ones that leave a shard without a leader while quorum looks intact, and silence there is indistinguishable from health. Use `GetLeaderID` or `ShardInfo.Health == ShardNoLeader` instead until it is emitted.

### System Events

| Callback                  | Info Type         | Fired When                                |
|---------------------------|-------------------|-------------------------------------------|
| `OnNodeReady`             | `EventShardInfo`  | Shard node ready for traffic              |
| `OnNodeUnloaded`          | `EventShardInfo`  | Shard node unloaded                       |
| `OnMembershipChanged`     | `EventShardInfo`  | Membership config change applied          |
| `OnNodeDeleted`           | `EventShardInfo`  | Node data removed from disk               |
| `OnNodeHostShuttingDown`  | `func()`          | Host shutdown started                     |
| `OnConnectionEstablished` | `ConnectionInfo`  | QUIC connection established to peer       |
| `OnConnectionFailed`      | `ConnectionInfo`  | QUIC connection attempt failed            |
| `OnSnapshotSendStarted`   | `SnapshotInfo`    | Snapshot transfer started                 |
| `OnSnapshotSent`          | `SnapshotInfo`    | Snapshot transfer sent                    |
| `OnSnapshotSendAborted`   | `SnapshotInfo`    | Snapshot transfer abandoned               |
| `OnSnapshotReceived`      | `SnapshotInfo`    | Snapshot transfer received                |
| `OnSnapshotRecovered`     | `SnapshotInfo`    | State machine restored from a snapshot    |
| `OnSnapshotCreated`       | `SnapshotInfo`    | On-demand or auto snapshot completed      |
| `OnSnapshotCompacted`     | `SnapshotInfo`    | Old snapshot directories reclaimed        |
| `OnLogCompacted`          | `EventShardInfo`  | Log compaction completed                  |
| `OnLogDBCompacted`        | `EventShardInfo`  | LogDB compaction completed                |

### Failure Events

| Callback             | Info Type          | Fired When                                  |
|----------------------|--------------------|---------------------------------------------|
| `OnShardFailed`      | `ShardFailedInfo`  | A fatal error was reported for the shard. Fired when the failure latch trips (SM error or panic, commit or save-state failure, snapshot recovery failure, corrupt committed config) AND for two failures that do not latch: a snapshot save that failed, and a `GetUpdate` error on the step worker. So the event does not by itself mean the shard is offline — `GetShardInfo` reports the latch. NOT fired for a stale snapshot recovery the engine declined: the shard is healthy and already past that snapshot (#129) |
| `OnLogDBDiskWarning` | `DiskInfo`         | WAL disk usage crossed warning threshold    |
| `OnLogDBDiskFull`    | `DiskInfo`         | WAL disk usage crossed full threshold       |

### Event Info Types

```go
type LeaderInfo struct {
    ShardID, ReplicaID, Term, LeaderID uint64
}

type EventShardInfo struct {
    ShardID, ReplicaID, LeaderID, Term uint64
}

type SnapshotInfo struct {
    ShardID, ReplicaID, Index, From uint64
}

type ShardFailedInfo struct {
    ShardID, ReplicaID uint64
    Err                error
}

type DiskInfo struct {
    WALDir    string
    UsedBytes uint64
    MaxBytes  uint64
    Shards    []uint64
}

type ConnectionInfo struct {
    Address string
    Err     error
}
```

## Information Types

### ShardHealth

```go
type ShardHealth uint8
```

Indicates shard health status. `String()` and `MarshalJSON` render the value as
the lowercase name in the third column; `UnmarshalJSON` accepts those names and
falls back to `ShardHealthy` for anything else.

| Constant | JSON / `String()` | Meaning |
|---|---|---|
| `ShardHealthy` | `healthy` | Known leader, apply pipeline running. Zero value. |
| `ShardNoLeader` | `no-leader` | No leader known: election in progress, or a partition |
| `ShardLagging` | `lagging` | Apply index significantly behind the commit index |
| `ShardUnloaded` | `unloaded` | Shard is not loaded on this host |
| `ShardError` | `error` | Fatal state machine or LogDB failure |
| `ShardRecovering` | `recovering` | `RecoverFromSnapshot` in progress: the replica still votes and replicates but applies nothing and serves no up-to-date local read |
| `ShardSnapshotting` | `snapshotting` | Snapshot capture in progress; applies are quiesced |

`ShardHealthy` is the zero value, so a `ShardInfo` you never populated reports
healthy. Check `ShardUnloaded` explicitly rather than testing against the zero
value.

### ShardInfo

```go
type ShardInfo struct {
    ShardID              uint64             `json:"shard_id"`
    ReplicaID            uint64             `json:"replica_id"`
    LeaderID             uint64             `json:"leader_id"`
    Term                 uint64             `json:"term"`
    IsLeader             bool               `json:"is_leader"`
    IsObserver           bool               `json:"is_observer"`
    IsWitness            bool               `json:"is_witness"`
    StateMachineType     uint64             `json:"state_machine_type"`
    StateMachineTypeName string             `json:"state_machine_type_name"`
    Pending              uint64             `json:"pending"`
    Nodes                map[uint64]string  `json:"nodes"`
    Health               ShardHealth        `json:"health"`
    LastApplied          uint64             `json:"last_applied"`
    ConfigChangeIndex    uint64             `json:"config_change_index"`
    Replicas             []ReplicaProgress  `json:"replicas,omitempty"`
}
```

Point-in-time snapshot of a shard's state, returned by `GetShardInfo`. It is a copy and is not live-updated: `LeaderID` and `Pending` reflect the state at call time. `StateMachineType` is 0 regular, 1 concurrent, 2 on-disk.

`Nodes` covers all known members; use `SyncGetShardMembership` when you need observers and witnesses broken out separately.

`Replicas` is empty on a follower, which tracks no such state, and empty on the first call for a shard because publication is opt-in and the next tick has not run yet.

### ReplicaProgress

```go
type ReplicaProgress struct {
    ReplicaID  uint64 `json:"replica_id"`
    Match      uint64 `json:"match"`
    Next       uint64 `json:"next"`
    State      string `json:"state"`      // "probe", "replicate", "snapshot"
    Paused     bool   `json:"paused"`
    Active     bool   `json:"active"`
    Inflights  int    `json:"inflights"`
    IsWitness  bool   `json:"is_witness"`
    IsObserver bool   `json:"is_observer"`
}
```

One follower's replication position as the leader understands it. Every field is the leader's local belief, which is what makes it useful for diagnosing a stall: a follower stuck in `probe` with a stale `Match` is a different problem from one that is merely behind, and one pinned at the inflight limit is flow-controlled rather than slow.

`Active` is the field to check before taking another replica down. An inactive follower -- one that has not responded within the election timeout -- still counts in the configuration but not toward a live quorum.

This is the structured form of what `DiagRemotes` renders as a debug string.

### NodeHostInfoOption

```go
type NodeHostInfoOption struct {
    SkipLogInfo bool
}
```

Options for `GetNodeHostInfo`. `SkipLogInfo = true` omits `LogInfo`, which is the expensive part of the call.

### NodeHostInfo

```go
type NodeHostInfo struct {
    RaftAddress   string           `json:"raft_address"`
    ShardInfoList []ShardInfo      `json:"shard_info_list"`
    LogInfo       []logdb.NodeInfo `json:"log_info,omitempty"`
}
```

Snapshot of all shards on the host. `LogInfo` is nil when `SkipLogInfo` was set.

### LogRange

```go
type LogRange struct {
    FirstIndex uint64
    LastIndex  uint64
}
```

First and last available entry indices in the Raft log, both inclusive.

### ReadonlyLogReader

```go
type ReadonlyLogReader interface {
    GetRange() (firstIndex uint64, lastIndex uint64)
    NodeState() (lastIndex uint64, term uint64)
}
```

Read-only view of a shard's Raft log state. The surface is deliberately minimal: it inspects range and node state and permits no mutation.

## Usage Patterns

### Synchronous Propose

```go
host, err := quicraft.NewHost(hostCfg, quicraft.WithBarrier(barrier))
if err != nil {
    return err
}
defer host.Close()

create := sm.NewCreateFunc(func(shardID, replicaID uint64) sm.StateMachine {
    return newKVStore()
})

if err := host.StartShard(members, false, create, shardCfg); err != nil {
    return err
}

result, err := host.SyncPropose(ctx, shardCfg.ShardID, []byte("key=value"))
if err != nil {
    var nle *quicraft.NotLeaderError
    if errors.As(err, &nle) && nle.LeaderAddress != "" {
        // redirect to nle.LeaderAddress
    }
    return err
}
// result.Value, result.Data
```

### Asynchronous Propose

```go
rs, err := host.Propose(ctx, shardID, cmd)
if err != nil {
    return err
}
defer rs.Release()

select {
case result := <-rs.ResultC():
    if result.Err != nil {
        return result.Err
    }
    // Use result.Value, result.Data
case <-ctx.Done():
    return ctx.Err()
}
```

### Linearizable Read

```go
value, err := host.SyncRead(ctx, shardID, "my-key")
if err != nil {
    return err
}
fmt.Println(value)
```

### Session-Based At-Most-Once Propose

```go
session, err := host.GetNewSession(ctx, shardID)
if err != nil {
    return err
}
defer host.CloseSession(ctx, shardID, session)

// Each proposal uses the session for deduplication
rs, err := host.ProposeWithSession(ctx, session, cmd)
if err != nil {
    return err
}
defer rs.Release()

result, err := rs.Result()
if err != nil {
    return err
}
session.ProposalCompleted()
fmt.Println(result.Value)
```

`SyncProposeWithSession` does the same thing in one call and advances the session for you.

### Membership Change

Read the current membership first so the change carries a `configChangeIndex` and a concurrent change is rejected rather than merged blindly.

```go
mship, err := host.SyncGetShardMembership(ctx, shardID)
if err != nil {
    return err
}

err = host.SyncRequestAddNode(ctx, shardID, newReplicaID,
    "10.0.0.4:4001", mship.ConfigChangeID)
switch {
case err == nil:
case errors.Is(err, quicraft.ErrStaleConfigChange):
    // someone else changed the membership; re-read and rebuild
    return err
default:
    return err
}
```

Pass 0 as the last argument to skip the guard.

### Graceful Shutdown

```go
// Drain first (optional, Close calls Drain internally)
if err := host.Drain(); err != nil {
    log.Warn("drain incomplete", "error", err)
}

// Close shuts down everything
if err := host.Close(); err != nil {
    log.Error("close failed", "error", err)
}
```

### Health Endpoint

```go
status := host.HealthCheck()
if status.Status != "ok" {
    // Return HTTP 503
}
```

### Prometheus Metrics

```go
metrics := quicraft.NewPrometheusMetricsCollectorWithConfig(config.MetricsConfig{
    PerShardMetrics: true,
})
host, err := quicraft.NewHost(hostCfg, quicraft.WithMetrics(metrics), quicraft.WithoutEncryption())
if err != nil {
    return err
}

http.Handle("/metrics", promhttp.HandlerFor(metrics.Registry, promhttp.HandlerOpts{}))
```

`NewPrometheusMetricsCollector()` uses the zero `MetricsConfig`, which leaves `PerShardMetrics` off and aggregates the volume and latency telemetry to one host-level series per metric. The mode is fixed at construction, because a Prometheus vector's label set is decided when the vector is built.

### Snapshot Export and Import

```go
// Source host: shard may be running.
f, err := os.Create("/backup/shard-7.snap")
if err != nil {
    return err
}
if err := host.ExportSnapshot(ctx, 7, f); err != nil {
    f.Close()
    return err
}
if err := f.Close(); err != nil {
    return err
}

// Destination host: the shard MUST be stopped.
r, err := os.Open("/backup/shard-7.snap")
if err != nil {
    return err
}
defer r.Close()
if err := dest.ImportSnapshot(ctx, 7, r); err != nil {
    return err
}
// Next StartShard recovers from the imported snapshot.
```

## Cross-References

- [Architecture Overview](../multi-group-host/architecture.md) -- System design, data flow, package structure
- [Engine Pipeline](../raft-consensus/engineering.md) -- Step, commit and apply workers behind `Propose`
- [Raft Protocol](../raft-consensus/engineering.md) -- Election and log safety, config-in-log, convergent apply, snapshot ack/retry
- [State Machine Interfaces](../state-machines/engineering.md) -- Choosing among the three tiers
- [RSM Adapter](../state-machines/engineering.md) -- What sits between the engine and your `sm` implementation
- [KV Backends](../state-machines/engineering.md) -- The reference stores in `pkg/sm/kv`
- [Session Deduplication](../client-sessions/engineering.md) -- At-most-once semantics and index-based expiry
- [Transport](../quic-transport/engineering.md) -- QUIC transport, connection management, frame format
- [Wire Format](../wire-protocol/engineering.md) -- Entry and message layouts behind `proto`
- [WAL Storage](../log-storage/engineering.md) -- LogDB and the write-ahead log
- [Encryption](../seal-barrier/architecture.md) -- Barrier, epochs, and the Layer-1 boundary
- [Key Custody](../seal-barrier/user.md) -- Who owns which key, and what this library deliberately does not hold
- [Deployment](../multi-group-host/user.md) -- Metrics cardinality, alerting, sizing
- [Lifecycle](../multi-group-host/engineering.md) -- Start, join, drain, shutdown
- [Compatibility](../compatibility.md) -- On-disk and wire format compatibility
- [Bootstrap and Discovery](../multi-group-host/architecture.md) -- Where the member set comes from
- [Testing](../testing/overview.md) -- Test infrastructure and fault injection
