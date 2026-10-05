# Multi-Group Host — API

> **Status: NORMATIVE** · Scope: go-quicraft `pkg` and `pkg/config` ·
> Last verified: 2026-10-02
> Conflicts with this document are defects. Fix them here first.

## Transport coverage

go-quicraft is a library, so the Go API is the whole API.

| Transport | This feature |
|---|---|
| Embedded (Go call) | everything below |
| QUIC | a peer protocol between replicas of one shard, not a client surface: no external caller can issue a request over it. See [quic-transport/api.md](../quic-transport/api.md) |
| Unix socket | n/a: no socket is served |
| REST | n/a: no HTTP handler is exposed |
| gRPC | n/a: no gRPC service is defined |
| MCP | n/a: no MCP tools are registered |

These are not missing capabilities. The library is consumed by implementing its
interfaces and calling the `Host`; exposing a client transport is the embedding
application's job.

## Naming: the `Get` prefix

Ten `Host` methods carry a `Get` prefix that Go convention would drop:
`GetNewSession`, `GetNewSessionWithOwner`, `GetNoOPSession`,
`GetShardMembership`, `GetShardInfo`, `GetLeaderID`, `GetLeaderIDAndTerm`,
`GetNodeHostInfo`, `GetLogReader`, `GetLogRange`. They are kept on purpose: four
are Dragonboat's own names, which `contrib/dragonboat` exists to present, and
renaming any of them would break every caller for the sake of convention alone.
New methods do not take the prefix. The `config.Config` accessors over `*bool`
fields with defaults (`GetCheckQuorum`, `GetPreVote`,
`GetOrderedConfigChange`) are optional-field accessors, not getters in that
sense.

## Packages

Source: `github.com/jeremyhahn/go-quicraft/pkg` (package `quicraft`).

| Package | Import path | Contains |
|---|---|---|
| `quicraft` | `.../pkg` | `Host`, `RequestState`, `Session`, `Membership`, errors, metrics |
| `config` | `.../pkg/config` | `HostConfig`, `Config`, `TransportConfig`, `EventListener` |
| `sm` | `.../pkg/sm` | the three state machine interfaces and their factory helpers |
| `proto` | `.../pkg/proto` | wire types a caller sees: `Entry` (from `QueryRaftLog`), config-change type constants |
| `seal` | `.../pkg/seal` | the at-rest encryptor (`seal.Barrier`, alias `EpochBarrier`) behind `WithBarrier` |

Nothing under `pkg/internal/` can be imported. That matters in one place:
several membership validation errors originate there; see
[Membership validation errors](#membership-validation-errors).

The library has no peer discovery, no bootstrap orchestration and no cluster
manager. `Host.StartShard` takes the member set you give it.

## Host

One `Host` per process manages thousands of independent Raft shards.

### Constructor

```go
func NewHost(cfg config.HostConfig, opts ...HostOption) (*Host, error)
```

Validates the config, creates or loads the host ID, opens the LogDB, and builds
the registry, QUIC transport and engine. The engine workers and then the
transport listener are running before it returns.

| Option | Argument | Description |
|---|---|---|
| `WithLogDB(l)` | `logdb.LogDB` | Custom LogDB. Default: the WAL in `HostConfig.WALDir` |
| `WithMemoryLogDB()` | | In-memory LogDB, for benchmarks, CI and ephemeral nodes |
| `WithBarrier(b)` | `Barrier` | Host-wide at-rest encryption. The barrier must already be initialized and unsealed |
| `WithShardBarriers(fn)` | `func(shardID uint64) Barrier` | Per-shard at-rest encryption; see below |
| `WithoutEncryption()` | | Declares plaintext at rest. Cannot be combined with the two above |
| `WithMetrics(c)` | `MetricsCollector` | Metrics collection |
| `WithLogger(l)` | `*slog.Logger` | Custom logger. Default: built from `LogConfig` |
| `WithoutTransport()` | | No-op sender; messages are dropped. Single-node testing only |

**At-rest encryption is stated, never defaulted.** Every host passes
`WithBarrier`, `WithShardBarriers` or `WithoutEncryption`. `NewHost` returns
`ErrAtRestEncryptionUnspecified` when none is passed and
`ErrAtRestEncryptionConflict` for `WithoutEncryption` with either barrier
option. Both checks run before anything touches the disk.

**`WithShardBarriers`** seals each shard under its own barrier, so one tenant's
key purge can never remove an epoch another tenant's snapshots reference. A
resolver that returns `nil` for a shard falls back to the `WithBarrier` barrier;
with no host-wide barrier the miss fails closed with
`ErrShardBarrierUnresolved` rather than writing that tenant in plaintext. Key
rotation for per-shard barriers is the caller's job: the host's rotation loop
rotates only the `WithBarrier` barrier. `Close` seals every barrier the resolver returned.

Barriers are matched by pointer identity (`logdb.SameBarrier`), never with `==`, because a `Barrier`'s
dynamic value need not be comparable. Two shards share a barrier when the
resolver returns the same pointer for both; `Close` seals it once. A barrier that
is not a pointer is distinct for every shard that resolves to it: `Close` seals
it per shard (an already-sealed result is not an error), the snapshot pool
declines the per-barrier DEK purges it cannot prove safe, and a per-barrier
LogDB reports that it does not own it, so `StartShard` with no `WithBarrier`
refuses it with `ErrShardBarrierLogDBMismatch`. Return pointers.

A stated encryption choice must cover the log, so the LogDB is checked too:

| Error | Condition |
|---|---|
| `ErrBarrierPlaintextLogDB` | `WithBarrier` with a caller-supplied LogDB that does not report `logdb.AtRestBarrierEncrypted` or `logdb.AtRestNonDurable`. The host-wide barrier is wired only into the default WAL, so every proposed command would be written in plaintext |
| `ErrShardBarriersPlaintextWAL` | `WithShardBarriers` with no `WithBarrier`, over a LogDB that does not report either value. Snapshots would be encrypted while every log entry stayed plaintext |
| `ErrShardBarrierLogDBMismatch` | (at `StartShard`) the LogDB stores this shard's records under a different barrier than the resolver returned |

Supplying `WithLogDB` does not by itself satisfy these: an unencrypted
`waldb.Open(dir)` is exactly the state they refuse. For genuine per-shard WAL
keys the LogDB must implement `logdb.PerBarrierEpochAwareLogDB`.

**Errors:** the encryption errors above; a `pkg/config` validation error (see
[Validation errors](#validation-errors)); otherwise `*HostInitError` naming the
component that failed and wrapping its error.

### Shard lifecycle

#### StartShard

```go
func (h *Host) StartShard(members map[uint64]string, join bool, create sm.CreateFunc, cfg config.Config) error
```

Starts a Raft shard on this host. `members` maps replica ID to address. `join`
is true when this replica joins a shard that already exists; its configuration
then arrives by replication. `create` is the state machine factory.
`cfg.ShardID` and `cfg.ReplicaID` must both be non-zero.

On a first start the member set is committed as an `EntryInitialMembership`
entry at log index 1, so membership is a function of the log from the first
entry. Every founder (`join=false`) must therefore be given an identical
`members` map, `cfg.Witness`, `cfg.MemberIdentities` and session policy:
founders that disagree produce different index-1 entries, and Log Matching never
reconciles them.

On a restart the membership comes from the recovered snapshot or is
reconstructed from the committed log, and address routing follows that
membership. The `members` map only supplies an address for a recovered member
that has none.

On a host with a transport, `cfg.MemberIdentities` is required: a founder must
name the identity of every member of `members`, a joiner must name at least one
other replica as its anchor, and no identity may be bound to two replicas. See
[Config changes with node identity](#config-changes-with-node-identity).

**Errors:**

| Error | Condition |
|---|---|
| `ErrClosed` | Host is closed, or closed while the start ran |
| `ErrInvalidConfig` | `ShardID == 0`, `ReplicaID == 0`, or `cfg.Validate()` failed |
| `ErrShardAlreadyExists` | Shard already running on this host |
| `ErrShardNotReady` | A concurrent `StartShard` of the same shard is still in progress (transient) |
| `ErrImportInProgress` | An `ImportSnapshot` of this shard is running (transient) |
| `*ReplicaRetiredError` | `RemoveData` retired this replica ID on this host. Rejoin under a new ID |
| `*EmptyMembersError` | Bootstrap with an empty `members` map |
| `*SelfNotInMembersError` | `cfg.ReplicaID` absent from `members` on a bootstrap |
| `*InvalidMemberError` | A member has a zero replica ID, an empty or malformed address, or a duplicate address; a required identity is missing or bound twice; a joiner names no anchor; or `MemberIdentities` contradicts the recovered membership. Matches `ErrInvalidMember` |
| `*NonVotingRoleInMembersError` | First start with `cfg.IsWitness` or `cfg.IsObserver` while this replica appears in `members` |
| `*SharedPeerCertificateError` (`ErrSharedPeerCertificate`) | This node's certificate also covers another member's host. `CoveredPeers` names those hosts |
| `ErrShardBarrierUnresolved`, `ErrShardBarrierLogDBMismatch` | Per-shard barrier problems; see [Constructor](#constructor) |
| `*BootstrapEntryMissingError` | The durable log holds something other than the founding configuration at the bootstrap index |
| `ErrInvalidSMType` | `CreateFunc` returned nil or a type implementing none of the three SM interfaces |
| `ErrRecoveredTermUnknown` | An on-disk SM recovered to an index whose Raft term cannot be resolved |
| `*HostInitError` | Log recovery, membership reconstruction, peer, state machine, snapshot recovery or bootstrap persistence failed. `Field` names which |
| `ErrLogDBWrite` | The bootstrap record could not be saved |

`*NonVotingRoleInMembersError` applies only to a first start. `members` seeds the
full voter set, so a witness or observer named there would be a full voter in
the configuration the whole shard agrees on: the leader replicates entry
payloads to it and it counts toward every quorum. Bootstrap the voters first,
then start the replica with `join=true` and add it with
`RequestAddWitnessWithIdentity` or `RequestAddObserverWithIdentity`.

Safe for concurrent calls with different shard IDs.

#### StopShard

```go
func (h *Host) StopShard(shardID uint64) error
```

Stops this host's replica of the shard: every pending proposal, read and
snapshot completes with `ErrAborted`, then the engine node is unloaded and its
registry entries removed. A non-nil return after teardown began is the state
machine's `Close` error, which for an on-disk state machine is its backing
store's final flush.

**Errors:** `ErrClosed`, `ErrShardNotFound`. A shard already being torn down by a
concurrent `StopShard` or `Close` also returns `ErrShardNotFound`, since teardown
is claimed once.

### Proposals

#### Propose

```go
func (h *Host) Propose(ctx context.Context, shardID uint64, cmd []byte) (*RequestState, error)
```

Submits a proposal asynchronously. The caller reads the outcome from the
returned `*RequestState` and must call `Release()`.

A proposal is not rejected because this replica is a follower: when a leader is
known, the engine forwards the entry. It fails fast only when no leader is known
at all. An empty `cmd` is legal and commits as a no-op entry that never reaches
the state machine, which is how you force a current-term committed entry after
an election (the ReadIndex warmup of Raft PhD §6.4).

**Errors:**

| Error | Condition |
|---|---|
| `ErrCanceled` / `ErrTimeout` | `ctx` was already done on entry |
| `ErrClosed` | Host closed |
| `ErrDraining` | Host in drain mode |
| `ErrShardNotFound` | Shard not loaded |
| `ErrInvalidOperation` | This replica is a witness; witnesses store no log data |
| `ErrDiskFull` | WAL usage reached the full threshold (95% of `MaxWALDiskSize`) |
| `*EntryTooLargeError` | `len(cmd)` exceeds `min(MaxProposalPayloadSize, MaxEntrySize)` |
| `*NotLeaderError` | No leader is known. `LeaderID` is 0 and `LeaderAddress` empty |
| `ErrSystemBusy` | Global rate limit (`MaxTotalInMemLogSize`) exceeded |
| `ErrShardNotReady` | Engine node or peer not initialized |
| `ErrShardFailed` | The shard's state-machine failure latch has tripped |
| `*BusyError` | Per-shard rate limit (`MaxInMemLogSize`) exceeded |
| `ErrDropped` | The shard's proposal queue is full |

Safe for concurrent calls.

#### SyncPropose

```go
func (h *Host) SyncPropose(ctx context.Context, shardID uint64, cmd []byte) (sm.Result, error)
```

Calls `Propose` and waits for the result or `ctx`. Manages the `RequestState`
itself. Returns the state machine's `sm.Result{Value, Data}`.

#### ProposeWithSession

```go
func (h *Host) ProposeWithSession(ctx context.Context, session *Session, cmd []byte) (*RequestState, error)
```

Submits a session-managed proposal for at-most-once delivery. The session must
be registered and prepared for proposals (`GetNewSession` does both); one that
is not returns `ErrInvalidSession`. After a successful result the caller must
call `session.ProposalCompleted()`, or the series ID goes stale and no further
proposal on that session succeeds.

On error the session state is unchanged, so the same session can retry the same
command. That is what the session is for.

A session serves one proposal at a time. The entry is stamped with the session's
current series number, and the state machine treats every entry with the same
`(ClientID, SeriesID)` as a retransmission of one command. That makes a retry
safe and a second concurrent proposal unsafe: only one of the two would run and
both would report success. The series is therefore reserved for the returned
`RequestState`, and a second call while it is held returns `ErrSessionBusy`
(`Temporary`). The reservation is released by `ProposalCompleted`, by an error
result, or by `RequestState.Release`, whichever comes first.

Returns `ErrSessionsUnsupportedOnDisk` on a shard whose state machine is an
`sm.DiskStateMachine`. A no-op session is unaffected. See
[Sessions and on-disk state machines](#sessions-and-on-disk-state-machines).

#### SyncProposeWithSession

```go
func (h *Host) SyncProposeWithSession(ctx context.Context, session *Session, cmd []byte) (sm.Result, error)
```

Calls `ProposeWithSession`, waits for the result, and calls
`session.ProposalCompleted()` on success.

### Reads

#### ReadIndex

```go
func (h *Host) ReadIndex(ctx context.Context, shardID uint64) (*RequestState, error)
```

Confirms leadership through a heartbeat quorum. The returned `*RequestState`
resolves when the local state machine has applied up to the confirmed commit
index; its `Value` is that read index. Observers and followers forward the
request to the leader. Witnesses cannot serve reads.

**Errors:** `ErrCanceled` / `ErrTimeout` (context already done), `ErrClosed`,
`ErrDraining`, `ErrShardNotFound`, `ErrInvalidOperation` (witness),
`ErrShardNotReady`, `ErrShardFailed`, `ErrSystemBusy` (the shard's read queue
is full). `ErrShardFailed` is returned at once when the failure latch has
tripped, and is also delivered to a read already pending when it trips: a halted
shard never applies again, so the read could otherwise only time out. There is
no entry-size or rate-limit check on this path. `ErrReadIndexNotReady` arrives
on the `RequestState`, not from this call, when the leader has not yet committed
an entry in its current term.

#### SyncRead

```go
func (h *Host) SyncRead(ctx context.Context, shardID uint64, query interface{}) (interface{}, error)
```

A linearizable read in one call:

1. Submit `ReadIndex` to confirm leadership.
2. Wait until `lastApplied >= readIndex`.
3. Run `SM.Lookup(query)` on the caught-up state machine.

The result reflects every write committed before the call.

On a basic `sm.StateMachine`, step 3 waits for any in-flight `Apply`, which
holds the RSM adapter's read guard for writing. `sync.RWMutex` has no
context-aware acquire, so `ctx`'s deadline is not observed while the call waits
on that lock. The same applies to `SyncReadBuf`, `FollowerRead`,
`QueryLocalNode`, `StaleRead` and `StaleReadBuf`. `ConcurrentStateMachine` and
`DiskStateMachine` lookups run alongside `Apply`. See
[Lookup concurrency](../state-machines/engineering.md#concurrency).

#### SyncReadBuf

```go
func (h *Host) SyncReadBuf(ctx context.Context, shardID uint64, key []byte) (result []byte, release func(), err error)
```

Pooled-buffer variant of `SyncRead` for `[]byte` keys. You must call `release()`
when done, after which `result` must not be touched. Use it where the result is
consumed immediately, such as being written into a response.

The allocation-free path needs the state machine to implement
`sm.NALookupInto`. Without it the call falls back to `Lookup` and allocates.
`ErrInvalidOperation` comes back only when that fallback returns something other
than `[]byte`; such a query belongs on `SyncRead`.

```go
result, release, err := host.SyncReadBuf(ctx, shardID, key)
if err != nil {
    return err
}
defer release()
// use result before returning
```

#### StaleRead and QueryLocalNode

```go
func (h *Host) StaleRead(ctx context.Context, shardID uint64, query interface{}) (interface{}, error)
func (h *Host) QueryLocalNode(ctx context.Context, shardID uint64, query interface{}) (interface{}, error)
```

Read this replica's state machine through `SM.Lookup` with no consensus check.
Not linearizable: the result may be arbitrarily stale, and successive reads are
not guaranteed to be monotonic. The two are the same operation; `StaleRead`
names its guarantee and delegates to `QueryLocalNode`, which keeps Dragonboat's
name. Prefer `StaleRead`.

**Errors:** `ErrClosed`, `ErrShardNotFound`, `ErrShardNotReady`,
`ErrCanceled` / `ErrTimeout` from `ctx`. Every read path checks for an
already-done context before touching the state machine, so cancellation means
the read does not execute.

#### StaleReadBuf

```go
func (h *Host) StaleReadBuf(ctx context.Context, shardID uint64, key []byte) (result []byte, release func(), err error)
```

Pooled-buffer variant of `StaleRead`, with the same buffer ownership and
`sm.NALookupInto` fallback as `SyncReadBuf`.

#### FollowerRead

```go
func (h *Host) FollowerRead(ctx context.Context, shardID uint64, query interface{}) (interface{}, error)
```

Waits for the local state machine to apply up to this replica's current
committed index, then reads. There is no quorum round-trip, so it is not
linearizable: the committed index may itself trail the leader during a
partition. It never reflects less than what this replica already knows is
committed, which is stronger than `StaleRead` and cheaper than `SyncRead`.

The wait polls every 1 ms for the first 50 attempts, then every 20 ms. It ends
on shard or host shutdown even when `ctx` has no deadline.

**Errors:** `ErrClosed`, `ErrShardNotFound`, `ErrShardNotReady`,
`ErrShardFailed` (at once, as for `ReadIndex`), `ErrInvalidOperation` (witness),
`ErrTimeout` / `ErrCanceled` from `ctx`.

### Leader transfer

```go
func (h *Host) RequestLeaderTransfer(ctx context.Context, shardID, targetReplicaID uint64) error
```

Asks for leadership to move to a specific replica. Once the target validates,
the call returns and the transfer completes asynchronously. Watch `GetLeaderID`
or `OnLeaderUpdated` for the outcome.

`targetReplicaID` must name a current voting member. It is checked here rather
than only in the Raft layer, whose rejection surfaces asynchronously in the step
worker where it can only be logged.

| `targetReplicaID` | Result |
|---|---|
| `0` | `*InvalidMemberError`. There is no automatic target selection |
| this replica | `nil`: the requested end state already holds |
| an observer or witness | `*InvalidMemberError`: neither can lead |
| a removed replica | `*InvalidMemberError` |
| unknown to this shard | `*ReplicaNotFoundError` |

**Errors:** the table above, plus `ErrClosed`, `ErrShardNotFound`,
`*NotLeaderError`, `ErrShardNotReady`, `ErrSystemBusy`, and
`ErrCanceled` / `ErrTimeout` from `ctx`.

### Membership changes

Membership changes are config changes proposed through Raft. Each async form
returns a `*RequestState` that resolves when the change is committed and
applied.

```go
func (h *Host) RequestAddNode(ctx context.Context, shardID, replicaID uint64, addr string, configChangeIndex uint64) (*RequestState, error)
func (h *Host) RequestRemoveNode(ctx context.Context, shardID, replicaID uint64, configChangeIndex uint64) (*RequestState, error)
func (h *Host) RequestAddObserver(ctx context.Context, shardID, replicaID uint64, addr string, configChangeIndex uint64) (*RequestState, error)
func (h *Host) RequestAddWitness(ctx context.Context, shardID, replicaID uint64, addr string, configChangeIndex uint64) (*RequestState, error)
```

| Method | Config change type | Effect |
|---|---|---|
| `RequestAddNode` | `AddNode` | add a voter, or promote an existing observer in place |
| `RequestRemoveNode` | `RemoveNode` | remove a replica |
| `RequestAddObserver` | `AddNonVoting` | add a non-voting observer |
| `RequestAddWitness` | `AddWitness` | add a voting witness (no state machine) |

On a host with a transport, these identity-less forms return
`ErrMemberIdentityRequired`; use the `WithIdentity` forms below.

Each serializes a `proto.ConfigChange`, wraps it in an `EntryConfigChange`
entry and delivers it through the engine. The change takes effect at commit, not
at apply.

`configChangeIndex` is an optimistic concurrency guard. Pass 0 to skip it, or
the `ConfigChangeID` from `SyncGetShardMembership` so a request built against a
membership that has since moved is rejected instead of applied.

**Pre-validation.** The request is checked against the current membership before
it is proposed, so an invalid change fails the call instead of hanging the
`*RequestState` until its deadline. It has to happen here: the apply path does
not re-validate a committed entry, because a replica that refused one would
diverge from its peers. Validation reads a lock-free membership snapshot, so a
change committing concurrently can cause a false rejection; re-read the
membership and retry.

#### Membership validation errors

These sentinels are declared in `pkg/internal/raft` and re-exported from
`quicraft` as the same values, so they match with `errors.Is` through any
wrapping:

| Error | Condition |
|---|---|
| `ErrTooManyWitnesses` | A witness add would exceed the shard's replicated `MaxWitnesses` |
| `ErrBreaksFullReplicaMajority` | The change would leave the full replicas unable to form a quorum on their own, violating `RequireFullReplicaMajority` |
| `ErrRemoveLastFullReplica` | The change would remove the shard's only full replica. Witnesses cannot lead, so the shard would be permanently headless |
| `ErrZeroReplicaID` | `replicaID == 0` |
| `ErrEmptyAddress` | Add with an empty address |
| `ErrNodeNotFound` | Remove targets a replica that is not a current member |
| `ErrNodeAlreadyExists` | Add targets a replica already present in the requested role, or a witness. Promoting an observer with `RequestAddNode` is allowed |
| `ErrNodeRemoved` | Add or remove targets a replica ID carrying a removal tombstone |
| `ErrTooManyVotingMembers` | The add would exceed `raft.MaxVotingMembers` (64), the width of the ReadIndex quorum bitfield |
| `ErrRemoveLastVoter` | Remove targets the last voting member |
| `ErrMemberIdentityRequired` | An identity-less add on a host with a transport |
| `ErrRemovedMemberIdentity` | The add binds a node identity this shard has removed, under any replica ID. The tombstone is never evicted, so the remedy is a new keypair. `Membership.RemovedIdentities` lists these identities, so a host can classify a joiner before proposing it |
| `ErrDuplicateMemberAddress` | The add names a new replica at an address another active member already holds. Promoting an observer at its own address is allowed |
| `ErrDuplicateMemberIdentity` | The add binds a node identity another active member already holds. There is no co-location exception |
| `ErrSharedPeerCertificate` / `*SharedPeerCertificateError` | This node's own certificate also covers the host of the replica being added; issue one certificate per node |

**Other errors:** the delivery errors `Propose` returns (`ErrClosed`,
`ErrDraining`, `ErrShardNotFound`, `ErrShardNotReady`, `ErrSystemBusy`,
`ErrCanceled` / `ErrTimeout` for an already-done context), plus
`*NotLeaderError`: unlike a proposal, a config change is refused on a
non-leader rather than forwarded. `*StaleConfigChangeError` when
`configChangeIndex` does not match. `*HostInitError` when serialization fails.

An add also checks certificate exclusivity for the address it introduces. That
is the only point where a grow-by-join deployment can be checked, since
`StartShard` sees only the initial members map.

With `OrderedConfigChange` on (the default), one membership change is in flight
per shard at a time.

#### Config changes with node identity

```go
func (h *Host) RequestAddNodeWithIdentity(ctx context.Context, shardID, replicaID uint64, addr, identity string, configChangeIndex uint64) (*RequestState, error)
func (h *Host) RequestAddObserverWithIdentity(ctx context.Context, shardID, replicaID uint64, addr, identity string, configChangeIndex uint64) (*RequestState, error)
func (h *Host) RequestAddWitnessWithIdentity(ctx context.Context, shardID, replicaID uint64, addr, identity string, configChangeIndex uint64) (*RequestState, error)
func (h *Host) SyncRequestAddNodeWithIdentity(ctx context.Context, shardID, replicaID uint64, addr, identity string, configChangeIndex uint64) error
func (h *Host) SyncRequestAddObserverWithIdentity(ctx context.Context, shardID, replicaID uint64, addr, identity string, configChangeIndex uint64) error
func (h *Host) SyncRequestAddWitnessWithIdentity(ctx context.Context, shardID, replicaID uint64, addr, identity string, configChangeIndex uint64) error
```

Commits a stable node identity for the replica alongside its address.
`identity` is what that node returns from `Host.NodeIdentity`: the SHA-256 of
its leaf certificate's SubjectPublicKeyInfo. Once committed, every replica
authorizes that replica's messages by comparing the identity against the
authenticated identity of the connection they arrive on. The address is routing
metadata only.

This means a wildcard or shared-SAN certificate cannot let one peer claim
another's `msg.From`, and a replica can change address without its votes and
acks going uncounted.

The identity has to come from outside the cluster: the operator, or the system
that issued the certificate. It is deliberately not captured from the joining
node's connection. At that point the only evidence is the certificate's address
coverage, so a holder of a promiscuous certificate could claim the target's
address and have its own identity committed for that replica ID, locking the
real node out. A node is also usually added before it is started, so there is
often no connection to capture.

An empty `identity` behaves like the non-identity form, and on a host with a
transport both are refused with `ErrMemberIdentityRequired`: a member is
admitted with an identity or not at all (`config.Config.RequireMemberIdentities`,
which the Host sets from whether it has a transport). Bootstrap identities come
from `config.Config.MemberIdentities`. Entries beyond the members map, and the
whole map on a `join=true` start, are the operator anchor: the replicas this
host vouches for when no configuration it holds names the sender. That is what
admits the leader's bootstrap snapshot to a joiner.

#### Witness policy

```go
func (h *Host) RequestUpdateWitnessPolicy(ctx context.Context, shardID uint64, policy config.WitnessConfig, configChangeIndex uint64) (*RequestState, error)
func (h *Host) SyncRequestUpdateWitnessPolicy(ctx context.Context, shardID uint64, policy config.WitnessConfig, configChangeIndex uint64) error
```

Replaces the shard's witness policy. The policy is replicated state in
`proto.Membership`, changed through an `UpdateWitnessPolicy` config change so
every replica adopts it at the same log position. `config.Config.Witness` only
seeds it at bootstrap; editing it on a running node changes nothing.

The new policy must hold for the current membership. Tightening a policy the
shard already violates is rejected with `ErrTooManyWitnesses` or
`ErrBreaksFullReplicaMajority`; reshape the membership first.

#### Synchronous config changes

```go
func (h *Host) SyncRequestAddNode(ctx context.Context, shardID, replicaID uint64, addr string, configChangeIndex uint64) error
func (h *Host) SyncRequestDeleteNode(ctx context.Context, shardID, replicaID uint64, configChangeIndex uint64) error
func (h *Host) SyncRequestAddObserver(ctx context.Context, shardID, replicaID uint64, addr string, configChangeIndex uint64) error
func (h *Host) SyncRequestAddWitness(ctx context.Context, shardID, replicaID uint64, addr string, configChangeIndex uint64) error
```

Block until the change is applied. The remove variant is `SyncRequestDeleteNode`
(Dragonboat's name); there is no `SyncRequestRemoveNode`.

#### Membership queries

```go
func (h *Host) SyncGetShardMembership(ctx context.Context, shardID uint64) (*Membership, error)
func (h *Host) GetShardMembership(shardID uint64) (*Membership, error)
```

`SyncGetShardMembership` is a linearizable read (a ReadIndex round-trip); every
config change committed before the call is reflected. `ctx` must carry a
deadline. `GetShardMembership` returns the local membership without a quorum
check and may be stale. Both return deep copies.

### Snapshots

#### RequestSnapshot

```go
func (h *Host) RequestSnapshot(ctx context.Context, shardID uint64, opt SnapshotOption) (*RequestState, error)
```

Requests a snapshot now. On success the `RequestState`'s `Value` is the snapshot
index. A shard has one snapshot slot; a second concurrent request is refused, not
queued.

```go
type SnapshotOption struct {
    ExportPath                 string // absolute dir; also marks the snapshot as exported
    CompactionOverhead         uint64 // used only when OverrideCompactionOverhead is true
    OverrideCompactionOverhead bool
    Exported                   bool   // set from ExportPath; callers rarely set it
}

func (o *SnapshotOption) IsExport() bool
func (o *SnapshotOption) Validate() error

var DefaultSnapshotOption = SnapshotOption{}
```

The zero value uses shard defaults. With `ExportPath` set, the snapshot is
written to that directory as well as the standard snapshot directory, so log
compaction still advances. `Validate` returns `*SnapshotOptionError`, which
matches `ErrInvalidSnapshotOption`.

**Errors:** `ErrClosed`, `ErrShardNotFound`, `ErrShardNotReady`,
`ErrSnapshotInProgress`, `*SnapshotOptionError`.

#### SyncRequestSnapshot

```go
func (h *Host) SyncRequestSnapshot(ctx context.Context, shardID uint64, opt SnapshotOption) (uint64, error)
```

Waits for the snapshot and returns its index.

#### ExportSnapshot

```go
func (h *Host) ExportSnapshot(ctx context.Context, shardID uint64, w io.Writer) error
```

Streams a self-describing artifact for the shard's latest recorded snapshot to
`w`. The shard must have at least one snapshot in LogDB.

The artifact is a versioned header (magic, version, the original `Index`,
`Term`, `OnDiskIndex`, compression and `Membership`), the snapshot data, and a
trailer binding the data's length and xxhash64. The header preserves the
snapshot's identity, so `ImportSnapshot` reconstructs the same logical snapshot
rather than one that merely holds the same bytes.

The data section is always plaintext. With a barrier configured, the encrypted
`snapshot.dat` frames are decrypted on the way out, using the shard's own
barrier (a `WithShardBarriers` key exports correctly).

The on-disk bytes are checked against the length and hash in `snapshot.meta` as
they are read, and a mismatch returns `ErrSnapshotCorrupt`. The trailer is
written last, only once every check has passed. **On any error, discard what
`w` received**: it has no valid trailer, and `ImportSnapshot` refuses it.

**Errors:** `ErrClosed`, `ErrShardNotFound`, `ErrSnapshotNotFound`,
`ErrSnapshotCorrupt`, `ErrSnapshotEncryptedBarrierUnavailable` (the snapshot is
encrypted but no barrier is configured or it is sealed; the export fails rather
than emitting ciphertext), plus barrier decryption and `ctx` errors.

#### ImportSnapshot

```go
func (h *Host) ImportSnapshot(ctx context.Context, shardID uint64, r io.Reader, opts ...ImportOption) error

func WithImportDataDir(dir string) ImportOption
func WithImportMembership(members map[uint64]string) ImportOption
func WithImportMemberIdentities(identities map[uint64]string) ImportOption
```

Imports an artifact produced by `ExportSnapshot`. The shard must be stopped on
this host. The header gives the original `Index`, `Term`, `OnDiskIndex` and
`Membership`; the data is written as `snapshot.dat` in this host's at-rest form;
and the snapshot is recorded in LogDB so the next `StartShard` recovers that
same snapshot.

The data is checked against the trailer's length and xxhash64 as it streams.
A mismatch, an early end of stream or a missing trailer fails the import before
anything is published, recorded or seen by Raft (`*SnapshotArtifactError`; a
length or hash mismatch also matches `ErrSnapshotCorrupt`). An artifact without
a trailer cannot be migrated; export it again.

While an import runs, `StartShard` of the same shard returns
`ErrImportInProgress`. Concurrent imports of one shard are serialized.

The destination directory is named by the artifact's snapshot index, as every
snapshot writer names one, so the import takes its place in the shard's ordered
snapshot tree. It is written to the canonical snapshot path, which is what makes
it visible to snapshot garbage collection and to the scan that computes the DEK
purge floor. Because the path comes from the artifact, an import can resolve to
a path it does not own (an operator retry, a peer's artifact at an index this
replica also reached, a concurrent import), so it never writes through that
path. It stages into a temporary directory it created and renames it into place
once the artifact is complete and durable.

**Layout discovery.** The snapshot tree root comes from the `Config.DataDir`
that `StartShard` recorded in the LogDB bootstrap record. A shard started with
`join=true`, or never started on this host, has no record; a record written
before the field existed has none either. Both fail closed with
`ErrImportLayoutUnknown` unless you state the layout with `WithImportDataDir`.
`WithImportDataDir("")` asserts the shard has no `DataDir`. A value that
contradicts the recorded one is refused.

**Identities.** On a host with a transport, every member of the imported
membership must have a node identity. `WithImportMemberIdentities` supplies or
overrides them (replica ID to `NodeIdentity()` value); the artifact's identity
is kept for any member left out. An entry for a replica the resulting membership
does not hold, or for an identity the artifact's tombstones hold, is refused.

**Errors:**

| Error | Condition |
|---|---|
| `ErrClosed` | Host closed |
| `ErrInvalidOperation` (wrapped) | The shard is running (including one started while the import waited for its lock), there is no LogDB, or this host holds data for more than one replica of the shard. Remove the stale replica's data (`RemoveData`) first |
| `*ImportLayoutUnknownError` | Layout not discoverable and none supplied |
| `*ImportDataDirMismatchError` | `WithImportDataDir` disagrees with the recorded `DataDir` |
| `*ImportSnapshotOutOfDateError` | The artifact's index does not advance the recorded snapshot |
| `*ImportSnapshotDirExistsError` | The destination directory exists. Clearing it is an operator decision; an import never removes a directory it did not create |
| `*ImportMembershipInvalidError` | `WithImportMembership` or `WithImportMemberIdentities` broke a rule, or a member lacks a required identity |
| `*SnapshotTooLargeError` | The artifact exceeds `config.DefaultMaxSnapshotSize` (4 GB). The shard is stopped, so its own `MaxSnapshotSize` is not available |
| `*SnapshotArtifactError` | The header is malformed, or the data fails the trailer check |

Each `Import*Error` matches its `ErrImport*` sentinel with `errors.Is`, except
`*ImportSnapshotOutOfDateError`, which matches `logdb.ErrSnapshotOutOfDate`.

**`WithImportMembership` is a disaster-recovery tool and unsafe by design.** It
replaces the membership the artifact carries, which is the only way to recover a
shard whose quorum is permanently lost: removing dead replicas is a config
change, a config change is a log entry, and committing it needs the quorum that
no longer exists. A snapshot does not help, because it carries the same
configuration.

Rewriting membership discards Raft's safety argument. Excluded replicas may
hold committed entries this snapshot lacks, and nothing will reconcile that, so
committed writes can be lost. Use it only when the alternative is losing the
shard, and only once you know the excluded replicas are permanently gone rather
than unreachable.

The procedure: stop every surviving replica, export from the most up-to-date
one, and import on each survivor with the same map naming only the survivors.
Different maps on different survivors is split-brain by construction. The map is
the complete new voting set; the artifact's observers and witnesses are dropped,
shard policy (session limits, witness policy) is kept, and the importing replica
must appear in the map. A survivor that moved to a new keypair needs its
identity supplied with `WithImportMemberIdentities`.

### Informational

```go
func (h *Host) ID() string
func (h *Host) RaftAddress() string
func (h *Host) NodeIdentity() string
func (h *Host) ShardCount() int
func (h *Host) GetLeaderID(shardID uint64) (uint64, bool, error)
func (h *Host) GetLeaderIDAndTerm(shardID uint64) (uint64, uint64, bool, error)
func (h *Host) GetShardInfo(shardID uint64) (*ShardInfo, error)
func (h *Host) GetNodeHostInfo(opt NodeHostInfoOption) *NodeHostInfo
func (h *Host) IsShardJoining(shardID uint64) bool
func (h *Host) StateMachineHash(shardID uint64) (hash uint64, appliedIndex uint64, ok bool, err error)
```

`GetLeaderID` returns `(leaderID, known, err)` and `GetLeaderIDAndTerm`
`(leaderID, term, known, err)`. Both read local atomic state and may be stale.
`GetNodeHostInfo` returns a snapshot of all loaded shards; pass
`NodeHostInfoOption{SkipLogInfo: true}` to skip the per-shard log info, which is
the expensive part.

`NodeIdentity` returns this host's stable identity: the SHA-256 of its leaf
certificate's SubjectPublicKeyInfo. It survives certificate renewal on the same
keypair and changes only when the keypair does. It is a hash of a public key, so
it is safe to log and copy. Feed it to `Config.MemberIdentities` or the
`WithIdentity` membership calls. It returns `""` under `WithoutTransport`, which
has no certificate.

`IsShardJoining` reports whether a shard started with `join=true` is still in
its bootstrap window, meaning it has not yet accepted its first snapshot. Once it
has, it stays out of that window for the life of the process.

`StateMachineHash` detects the one failure Raft cannot: a state machine that is
not a deterministic function of the log. Consensus keeps agreeing on the entry
sequence while replicas derive different state, and the first symptom is usually
a linearizable read whose answer depends on which replica served it. `ok` is
false when the state machine does not implement `sm.Hasher`, which is an absent
facility rather than an error. Compare replicas at the same `appliedIndex`. The
hash must be order-independent or it reports divergence that does not exist. It
hashes the whole state machine, so treat it as a diagnostic.

### Health check

```go
func (h *Host) HealthCheck() *HealthStatus

type HealthStatus struct {
    Status        string      `json:"status"` // "ok", "degraded", "sealed", "unhealthy"
    BarrierSealed bool        `json:"barrier_sealed"`
    DiskOK        bool        `json:"disk_ok"`
    ShardCount    int         `json:"shard_count"`
    Shards        []ShardInfo `json:"shards,omitempty"`
}
```

A point-in-time assessment. Precedence is fixed, so a sealed barrier reports
`"sealed"` whatever the shards say:

| Status | Condition | HTTP code |
|---|---|---|
| `"sealed"` | barrier configured and sealed | 503 |
| `"unhealthy"` | no shards loaded | 503 |
| `"degraded"` | any shard has `ShardError` health | 503 |
| `"ok"` | otherwise | 200 |

Only `ShardError` degrades the aggregate. A shard in `ShardNoLeader`,
`ShardLagging`, `ShardRecovering` or `ShardSnapshotting` leaves the host
`"ok"`; inspect `Shards` to alert on those. `DiskOK` is reported but does not
affect `Status`.

### Encryption and the barrier

```go
func (h *Host) EmergencySeal() error
func (h *Host) IsSealed() bool
```

`EmergencySeal` seals the barrier immediately, without waiting for in-flight
work. Operations that need key material then fail with `ErrBarrierSealed`. It is
for incident response, so the abruptness is intended. `IsSealed` returns false
when no barrier is configured.

The `Barrier` interface is declared in `quicraft` rather than imported, to break
an import cycle; `*seal.Barrier` satisfies it. Its methods: `IsSealed`, `Seal`,
`Encrypt`, `Decrypt`, `DecryptWithContext`, `EncryptWithEpoch`,
`EncryptWithEpochContext`, `EncryptWithEpochPinnedContext`, `DecryptForEpoch`,
`IsHardwareMode`, `SupportsContextAAD`, `Rotate`, `PurgeEpochsBefore`,
`RegisterInFlightEpoch`, `DeregisterInFlightEpoch`, `CurrentEpoch`,
`ShouldRotateKey`.

`Rotate(ctx)` and `PurgeEpochsBefore(ctx, minEpoch)` take a context, so a custom
`Barrier` must accept one. The host's key-rotation loop passes a context it
cancels when `Close` stops the loop; the snapshot pool's purge passes one it
cancels when the pool stops.

`IsHardwareMode` says only that sealing is delegated to an external encryptor.
`SupportsContextAAD` says whether that encryptor can authenticate the caller
context that snapshot frames are bound to: true for a software barrier and for a
delegated encryptor that carries AAD end to end, false for one that cannot and
in no-encryption mode. It is a property of the deployment's barrier, never a
per-artifact choice. `DecryptWithContext` has no weaker retry: a different
context, or none where one was present, fails authentication.

#### ReencryptSnapshotFile

```go
func ReencryptSnapshotFile(srcPath, dstPath string, shardID, replicaID, index uint64,
    srcBarrier, dstBarrier Barrier) (uint64, error)
```

Re-keys a snapshot data file: every frame is decrypted under `srcBarrier` and
re-encrypted under `dstBarrier`, streamed through an in-memory pipe so the
snapshot is never fully in memory and its plaintext never lands on disk. Epochs
referenced by the source are pinned on `srcBarrier` while it runs, so a
concurrent purge cannot remove a DEK mid-migration.

It returns the destination's minimum at-rest epoch across the written frames,
which is the value to record as the snapshot's `Epoch` so the purge floor
protects every frame. It is 0 for a plaintext destination; a sealed destination
always writes at least a terminal frame, so even an empty source yields a real
epoch.

`shardID`, `replicaID` and `index` are required: frames are bound to their
position through the AEAD context AAD, and there is no fallback for a caller
that omits them.

A nil barrier means a plaintext side. A non-nil but sealed barrier does not: it
means encryption is configured but unavailable, and the call returns
`ErrSnapshotBarrierSealed`. Reading an encrypted source under a sealed barrier
would copy ciphertext and record it as plaintext; a sealed destination would
durably downgrade a snapshot that must be encrypted.

The write is atomic and crash-safe: output goes to a temporary file in the
destination directory, is fsynced, renamed, and the directory chain is fsynced
before success. Any failure before the rename removes the temp file and leaves
both paths untouched. The exception is a directory fsync that fails after the
rename, which returns `*SnapshotCommittedNotDurableError` with the real epoch.

### Data removal

```go
func (h *Host) RemoveData(shardID, replicaID uint64) error
func (h *Host) SyncRemoveData(ctx context.Context, shardID, replicaID uint64) error
```

Removes all persistent data for a replica (log, snapshots, metadata) and retires
its replica ID on this host, across restarts: `StartShard` refuses it with
`*ReplicaRetiredError`, because its term, vote and any lease vote window it owed
are gone with its data and it could otherwise vote twice in one term. Rejoin
under a new replica ID. The shard must be stopped first. `SyncRemoveData` behaves
identically and exists for naming consistency.

**Errors:** `ErrClosed`, `ErrShardNotFound`, `*ShardNotStoppedError`.

### Replica management

```go
func (h *Host) StopReplica(shardID, replicaID uint64) error
func (h *Host) RequestCompaction(shardID, replicaID uint64) error
func (h *Host) HasNodeInfo(shardID, replicaID uint64) bool
```

`StopReplica` checks that `replicaID` is this host's replica of the shard (a
host runs at most one) and then stops the shard. `RequestCompaction` compacts
the log for the pair, reclaiming entries superseded by snapshots. Both return
`ErrClosed`, `ErrShardNotFound` or `*ReplicaNotFoundError`.

`HasNodeInfo` reports whether LogDB holds bootstrap information for the pair. It
returns false when the host is closed or the replica was never bootstrapped
here.

### Log access

```go
func (h *Host) GetLogReader(shardID uint64) (ReadonlyLogReader, error)
func (h *Host) QueryRaftLog(ctx context.Context, shardID uint64, firstIndex, lastIndex, maxSize uint64) ([]proto.Entry, error)
func (h *Host) GetLogRange(shardID uint64) (LogRange, error)
```

`GetLogReader` returns a read-only view of the shard's log (range and node
state). `GetLogRange` returns the first and last available indices. Both return
`ErrClosed` or `ErrShardNotFound`.

`QueryRaftLog` returns the entries in the half-open range
`[firstIndex, lastIndex)`, up to `maxSize` bytes in total, read from the local
LogDB with no leadership requirement. **Errors:** `ErrClosed`,
`ErrShardNotFound`, `ErrInvalidRange` (`firstIndex >= lastIndex` or
`maxSize == 0`), `ErrCanceled` / `ErrTimeout` from `ctx`, and LogDB errors such
as `logdb.ErrCompacted`.

### Diagnostics

```go
func (h *Host) DiagPipelineState(shardID uint64) (commitPending bool, proposalQLen int, inboxLen int, pendingProposals int, err error)
func (h *Host) DiagRaftState(shardID uint64) (term, committed, processed, lastIdx uint64, msgs int, state string, err error)
func (h *Host) DiagRemotes(shardID uint64) (string, error)
func (h *Host) DiagShardGates(shardID uint64) (recovering, saving bool, applyInFlight int64, err error)
```

For incident response, not monitoring; their signatures may change.
`DiagPipelineState` shows commit queuing, proposal queue depth and inbox
backlog. `DiagRaftState` shows term, commit index, processed index, last index,
pending message count and role. `DiagRemotes` renders peer replication state as
text; prefer `ShardInfo.Replicas` for structured data.

`DiagShardGates` reports the two gates that suspend state machine work. While
`recovering` is true the state machine is being replaced by a received
snapshot: the replica applies nothing, though it keeps voting and replicating.
While `saving` is true a snapshot capture has quiesced applies. `applyInFlight`
is the drain counter both gates wait on. Both states also appear in
`ShardInfo.Health`.

### Shutdown

#### Drain

```go
func (h *Host) Drain() error
func (h *Host) IsDraining() bool
```

`Drain` enters drain mode: new proposals, reads, session operations and config
changes are rejected with `ErrDraining`, and in-flight work may finish. It blocks
until pending proposals, reads and snapshots complete, or returns
`ErrShutdownTimeout` when `ShutdownTimeout` expires. There is one drain attempt:
a concurrent caller waits for it and gets the same result, and a later call
returns the recorded result at once.

Drain is irreversible; a host that must accept proposals again has to be
recreated. The wait is event-driven off a pending-operation counter.

#### Close

```go
func (h *Host) Close() error
```

Shuts the host down. Shutdown always runs to completion. The returned error is
the `errors.Join` of everything that failed: an incomplete drain, each state
machine's `Close`, the LogDB `Close` and each barrier `Seal`. `errors.Is` reaches
each cause.

Treat a non-nil return as a durability signal. For an on-disk state machine its
`Close` is the backing store's final flush, so this is the last chance to learn
that writes were lost.

A concurrent or repeated `Close` blocks until the running shutdown finishes, then
returns the same error.

Order: drain, stop key rotation, stop the pending-request sweeper, stop the
engine, drain in-flight snapshot sends, stop the transport, fail pending
requests with `ErrClosed`, close state machines, close LogDB, seal the host-wide
barrier and every per-shard barrier the resolver returned (each once).
The engine stops before the transport because its step workers are the only
producers of snapshot sends; stopping them first means the send drain races no
new sends while the transport is still up.

## RequestState

```go
type RequestState struct { /* unexported fields */ }
```

Tracks one asynchronous request. Returned by `Propose`, `ProposeWithSession`,
`ReadIndex`, `RequestSnapshot`, `CloseSession` and the async membership calls.

Lifecycle:

1. taken from a `sync.Pool` with its result channel (capacity 1) allocated
2. registered in the shard's pending map under a random 64-bit request key
3. completed: an engine callback sends exactly one `RequestResult`
4. released: the caller calls `Release()`

A `RequestState` is for one goroutine; share it only with external
synchronization.

```go
func (rs *RequestState) Result() (RequestResult, error)
func (rs *RequestState) ResultC() <-chan RequestResult
func (rs *RequestState) CommittedC() <-chan RequestResult
func (rs *RequestState) ApplyResultC() <-chan RequestResult
func (rs *RequestState) Release()
func (rs *RequestState) Key() uint64
func (rs *RequestState) Deadline() time.Time
```

- `Result` blocks until completion or the deadline from the originating context
  (`ErrTimeout`); with no deadline it blocks indefinitely. After `Release` it
  returns `ErrReleased`.
- `ResultC` receives exactly one result, for `select`-based use. It is nil after
  release.
- `CommittedC` receives an early notification once the entry is durable in the
  local WAL, with `Value` 0 and nil `Data`, because the state machine has not
  applied it. It fires only when `HostConfig.NotifyCommit` is on. It is nil
  after release.
- `ApplyResultC` returns the same channel as `ResultC`, for readability when both
  appear in one `select`.
- `Release` is idempotent. The object returns to the pool only once the caller
  has released it and every pending-map reference is gone, so a `Sync*` caller
  that times out and releases while the request is still registered cannot have
  a stale result delivered into a recycled object.

`NewRequestStatePool()` returns the `sync.Pool` each shard uses, exported so an
embedder building its own request plumbing can reuse the pooling. Ordinary
callers never need it.

## RequestResult

```go
type RequestResult struct {
    Value uint64 // from sm.Result
    Data  []byte // from sm.Result
    Err   error  // non-nil on failure
}
```

For proposals, `Value` and `Data` come from the state machine's `Update`. For
`ReadIndex`, `Value` is the confirmed read index. For snapshots, `Value` is the
snapshot index. On failure `Err` carries the error (`*NotLeaderError`,
`ErrAborted`, `ErrTimeout`, `ErrReadIndexNotReady`, `ErrSessionExpired`, ...).
Classify it with `MaybeApplied`, `Temporary` and `NotLeader`.

## Sessions

Sessions give at-most-once proposal semantics. The session manager keys on
`(clientID, seriesID)` to detect retransmissions, so a retry after an ambiguous
failure applies the command once. Session state is replicated, and expiry is
measured in applied log entries, so every replica expires the same session at
the same index.

Sessions are unavailable on shards backed by an `sm.DiskStateMachine`; see
[Sessions and on-disk state machines](#sessions-and-on-disk-state-machines).

### Session type

```go
type Session struct { /* unexported fields */ }

func NewSession(shardID, clientID uint64) *Session
func NewSessionWithOwner(shardID, clientID, ownerID uint64) *Session
func NewNoOPSession(shardID uint64) *Session

func (s *Session) ShardID() uint64
func (s *Session) ClientID() uint64
func (s *Session) SeriesID() uint64
func (s *Session) OwnerID() uint64
func (s *Session) RespondedTo() uint64
func (s *Session) IsNoOPSession() bool
```

Fields are unexported so deduplication cannot be broken by mutation.

`NewSession` creates a tracked session. The client ID must not be zero, which is
reserved for no-op sessions. The session must be registered through Raft before
use.

`NewSessionWithOwner` declares a principal, so the session counts against the
shard's replicated `MaxSessionsPerOwner`. An `ownerID` of zero is the same as
`NewSession`. The owner is whatever the embedder decides identifies a principal:
a tenant ID, an account number, a hash of an authenticated subject. This library
authenticates nodes, not clients, and cannot derive it. The cap is only as
trustworthy as the embedder's own authentication: a caller free to choose any
`ownerID` can evade it.

`NewNoOPSession` creates a session with client ID 0 that bypasses deduplication.

### Registration

```go
func (h *Host) GetNewSession(ctx context.Context, shardID uint64) (*Session, error)
func (h *Host) GetNewSessionWithOwner(ctx context.Context, shardID, ownerID uint64) (*Session, error)
```

1. Draw a client ID from `crypto/rand`, rejecting zero.
2. Set `seriesID = SeriesIDForRegister`.
3. Propose the registration through Raft and wait until it is applied.
4. Check the result carries the expected client ID.
5. Set `seriesID = SeriesIDFirstProposal`.

Zero is rejected because it is the "no session" marker on the wire. A session
minted with it would be a no-op session that looked registered, and at-most-once
delivery would be silently off. If the CSPRNG returns zero on every attempt, the
call fails with `ErrEntropyDegenerate` instead of looping.

**Errors:** `ErrClosed`, `ErrShardNotFound`, `ErrSessionsUnsupportedOnDisk`,
`*NotLeaderError`, `ErrSystemBusy`, `*SessionError` (wrapping
`ErrEntropyDegenerate`, or `ErrInvalidOperation` when the owner is at its cap).
The per-owner cap is a replicated refusal: every replica reaches it from the log.

```go
func (h *Host) RegisterSession(ctx context.Context, session *Session) (*Session, error)
```

Registers a pre-created session through Raft, keeping its client ID. Used by
adapters that choose client IDs themselves. The session must have a non-zero
client ID and belong to the shard. On success it is ready for proposals.

```go
func (h *Host) CloseSession(ctx context.Context, shardID uint64, session *Session) (*RequestState, error)
```

Proposes unregistration (`seriesID = SeriesIDForUnregister`). The
`RequestState` resolves when it is applied. **Errors:** `ErrClosed`,
`ErrShardNotFound`, `ErrSessionsUnsupportedOnDisk`, `ErrInvalidOperation` (nil
session), `ErrInvalidSession` (wrong shard, or a no-op session).

```go
func (h *Host) GetNoOPSession(shardID uint64) *Session
```

Returns a no-op session (client ID 0). It bypasses deduplication, so use it only
for idempotent operations. It needs no registration and is the only session form
an `sm.DiskStateMachine` shard supports.

### Constants and methods

| Constant | Value | Use |
|---|---|---|
| `SeriesIDForRegister` | `math.MaxUint64 - 1` | register sentinel |
| `SeriesIDForUnregister` | `math.MaxUint64` | unregister sentinel |
| `SeriesIDFirstProposal` | `1` | first normal proposal |

```go
func (s *Session) PrepareForPropose()
func (s *Session) ProposalCompleted()
func (s *Session) PrepareForRegister()
func (s *Session) PrepareForUnregister()
func (s *Session) SetRespondedTo(seriesID uint64)
func (s *Session) ValidForSessionOp(shardID uint64) bool
func (s *Session) ValidForProposal(shardID uint64) bool
```

`PrepareForPropose` sets the series to `SeriesIDFirstProposal`.
`ProposalCompleted` advances the series and `respondedTo` after a proposal.
`PrepareForRegister` and `PrepareForUnregister` set the sentinel series.
`SetRespondedTo` records the highest acknowledged series; the host calls it, and
you need it only when driving the lifecycle by hand.

`ValidForSessionOp` is true when the session belongs to the shard and is not a
no-op session. `ValidForProposal` additionally requires the series to be in the
normal range (at least `SeriesIDFirstProposal` and not a sentinel; a session
never registered, or not prepared, is still at 0) and `respondedTo <= seriesID`.

## Membership

```go
type Membership struct {
    ConfigChangeID uint64              // log index of the config change that produced this
    Nodes          map[uint64]string   // voters, replicaID -> address
    Observers      map[uint64]string   // non-voting
    Witnesses      map[uint64]string   // voting, no state machine
    Removed        map[uint64]struct{} // removal tombstones

    // Replicated witness policy. Changed via Host.RequestUpdateWitnessPolicy.
    MaxWitnesses               uint64 // 0 = no explicit cap
    RequireFullReplicaMajority bool

    // Replicated session policy, seeded from the founders' Config.
    // No config change alters these afterwards.
    SessionExpiryEntries uint64
    MaxTotalSessions     uint64
    MaxSessionsPerClient uint64
    MaxSessionsPerOwner  uint64

    // Committed node identities: replicaID -> NodeIdentity() value.
    Identities map[uint64]string

    // Identity deny-list: removed replicaID -> the identity it held.
    RemovedIdentities map[uint64]string
}

func NewMembership() *Membership
func (m *Membership) ConfigChangeIndex() uint64 // returns ConfigChangeID
```

The committed membership of a shard, returned by `SyncGetShardMembership` and
`GetShardMembership`. `NewMembership` returns a value with every map
initialized.

**Session policy is replicated.** A replica evaluating the limits from its own
configuration would expire different sessions at different log indexes and
silently diverge, so the founders' values are committed at bootstrap and every
replica reads the shard's copy. They are exposed so you can see what the shard
committed. If someone edits a node's `Config` after bootstrap, the shard's value
governs and the node logs the disagreement at startup.

`MaxSessionsPerClient` is the response-cache depth: how many cached results one
session keeps for duplicate detection. It does not limit sessions.
`MaxSessionsPerOwner` is the per-principal session cap and `MaxTotalSessions`
the shard-wide one.

**`RemovedIdentities` is the identity deny-list.** A committed removal tombstones
the identity the removed replica held, not only its replica ID, and nothing ever
evicts it. Every replica refuses traffic from that keypair whatever replica ID
it presents, and an add that would bind it again fails with
`ErrRemovedMemberIdentity`. A replica removed without a committed identity has
no entry.

**`Identities` is complete on any shard bootstrapped behind a transport**: every
member of `Nodes`, `Observers` and `Witnesses` appears, because admission
requires it. A member missing from the map has every message refused
(`missing_identity`); that configuration can only come from data written without
the requirement, or from a host started `WithoutTransport()`.

## Errors

### Classification helpers

Do not re-derive "is this retryable?" from the sentinel list; three helpers
answer it:

```go
func MaybeApplied(err error) bool
func Temporary(err error) bool
func NotLeader(err error) (replicaID uint64, ok bool)
```

All three use `errors.Is` / `errors.As`, so wrapped errors classify correctly. A
nil error is none of the three.

**Check `MaybeApplied` first.** It reports whether the request had already
been handed to Raft when it failed, so the entry may still commit and apply.
`Temporary` answers "is a retry reasonable?", not "did it happen?". Retrying
such a request without a client session can apply the command twice, because
an unsessioned proposal carries client ID 0 and nothing deduplicates it.

Every failure delivered to a proposal, config change or session operation
after the hand-off is wrapped in `*MaybeAppliedError`, which preserves every
`errors.Is`, `errors.As`, `Temporary` and `NotLeader` result:

- leadership moved after the step worker took it from the shard's proposal
  queue (`*NotLeaderError`); one still queued is left pending, and the step
  worker forwards it under the new leader or refuses it definitely;
- the caller's wait ended: its context's deadline or cancellation in a `Sync*`
  call (`ErrTimeout`, `ErrCanceled`), the request's own deadline in
  `RequestState.Result` (`ErrTimeout`), or the host's deadline sweep
  (`ErrTimeout`);
- the shard was stopped (`ErrAborted`), the host closed (`ErrClosed`), or the
  shard halted (`ErrShardFailed`) while it was pending;
- the leader could not persist it (`*SaveStateError`; Replicate is sent before
  the local persist, so followers may already hold it);
- its committed entry failed to commit or apply (`*CommitError`,
  `*ApplyError`, or a state-machine panic matching `ErrShardFailed`); a reload
  replays the committed entry.

A proposal is handed to Raft when the step worker takes it from the shard's
proposal queue. The caller's own wait (`Sync*`, `RequestState.Result`) is
classified by the same rule as every other failure. A failure that provably happened before that is definite and
never wrapped: the admission checks (`ErrShardFailed`, `ErrClosed`, `ErrBusy`
and the rest), a full proposal queue (`ErrDropped`), a proposal still queued
when its shard halted or stopped (the queue is closed first, so it can never
be taken), and a batch Raft refused before appending it (`*NotLeaderError` with no known leader,
`ErrTransferInProgress`, `ErrRejected` for a config change while another is
pending). Reads append nothing and
are never wrapped.

**`Temporary`** reports whether the identical request can succeed on retry once
a transient condition clears, with no operator or caller action. It is true for
exactly: `ErrBusy` (and so `ErrSystemBusy`, which is a `*BusyError`),
`ErrTimeout`, `ErrAborted`, `ErrDropped`, `ErrShardNotStarted`,
`ErrShardNotReady`, `ErrNotLeader`, `ErrReadIndexNotReady`,
`ErrSnapshotInProgress`, `ErrImportInProgress`, `ErrTransferInProgress`,
`ErrSessionBusy`.

**Everything else is permanent, including errors this package did not produce.**
Retrying a permanent failure turns one bad request into sustained load. Some
look retryable and are not:

| Excluded | Why |
|---|---|
| `ErrCanceled` | the caller's own decision. A deadline that elapses server-side surfaces as `ErrTimeout`, which is temporary |
| `ErrDraining` | a one-way move into shutdown. Retry a different host |
| `ErrStaleConfigChange` | the identical request fails forever. Re-read membership and build a new request |
| `ErrSessionLimitExceeded` | clearing depends on `SessionExpiryEntries` and on clients closing sessions; a retry loop against a leaking client spins forever |
| `ErrBarrierSealed` | needs an operator unseal |
| `ErrShardBarrierUnresolved` | may mean "tenant not provisioned yet" (clears) or "misconfigured" (never clears), and the error cannot tell which |
| `ErrDiskFull`, `ErrLogDBWrite` | storage faults needing operator action |
| `ErrShardFailed` | the failure latch does not reset on its own |

**`NotLeader`** returns `ok == true` for any not-leader error, the bare
`ErrNotLeader` or a `*NotLeaderError`. `replicaID` is the leader this replica
believes is current, or 0 when unknown: treat `(0, true)` as "retry, but find
the leader yourself". Every not-leader error is also `Temporary`, so gate the
retry loop on `Temporary` and use `NotLeader` only to pick a target.

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

### Sentinel errors

Match with `errors.Is`. Sentinels declared in this package have the
`"quicraft:"` prefix; those re-exported from an internal package keep that
package's prefix (`"raft:"`, `"engine:"`, `"transport:"`). A re-export is the
same value the internal package returns, so it matches through any wrapping.

Host and request lifecycle:

| Error | Returned when |
|---|---|
| `ErrClosed` | the Host is closed, or the request never started |
| `ErrDraining` | new work submitted during drain |
| `ErrStopped` | the shard or engine stopped underneath an in-progress snapshot save or recovery |
| `ErrShutdownTimeout` | drain exceeded `ShutdownTimeout` |
| `ErrAtRestEncryptionUnspecified` | `NewHost` was given no at-rest encryption choice |
| `ErrAtRestEncryptionConflict` | `WithoutEncryption` combined with a barrier option |

Request outcome:

| Error | Returned when |
|---|---|
| `ErrTimeout` | the request deadline expired |
| `ErrCanceled` | the request was canceled through its context |
| `ErrRejected` | the proposal was rejected, for example a config change already in progress |
| `ErrAborted` | an internal transition such as a leadership change or `StopShard` |
| `ErrDropped` | the request was dropped before it reached Raft: queue overflow or shutdown, or a proposal, ReadIndex or read result an engine worker took out of the shard and could not hand on because user code it ran (a metrics collector, a callback, a logger) panicked or called `runtime.Goexit`. Never `MaybeApplied` |
| `ErrReleased` | `Result` or `ResultC` on a released `RequestState` |

Shard state:

| Error | Returned when |
|---|---|
| `ErrShardNotFound` | the shard is not loaded on this Host |
| `ErrShardNotStarted` | the shard is registered but not initialized |
| `ErrShardNotReady` | the shard is initializing or starting |
| `ErrShardAlreadyExists` | `StartShard` for a running shard |
| `ErrShardNotStopped` | `RemoveData` on a loaded shard |
| `ErrReplicaRetired` | `StartShard` of a replica ID `RemoveData` retired |
| `ErrImportInProgress` | `StartShard` while an import of the shard runs |
| `ErrShardFailed` | the shard's failure latch tripped. A `*SMPanicError` matches it |
| `ErrReplicaNotFound` | the replica ID does not match the replica on this host |
| `ErrInvalidShardHealth` | `ShardHealth.UnmarshalJSON` got an unrecognized value |

Backpressure and leadership:

| Error | Returned when |
|---|---|
| `ErrBusy` | the per-shard in-memory log exceeded `MaxInMemLogSize` |
| `ErrSystemBusy` | the global limit `MaxTotalInMemLogSize` was exceeded, or a read queue is full. It is a `*BusyError`, so `errors.Is(err, ErrBusy)` is also true |
| `ErrDiskFull` | WAL usage reached the full threshold of `MaxWALDiskSize` |
| `ErrNotLeader` | sentinel form of `*NotLeaderError` |
| `ErrReadIndexNotReady` | the leader has no committed entry in its current term |
| `ErrTransferInProgress` | a leadership transfer is under way; raised before the entry enters the log |

Validation:

| Error | Returned when |
|---|---|
| `ErrInvalidConfig` | config validation failed |
| `ErrInvalidOperation` | invalid for the current state, such as a witness read or propose |
| `ErrInvalidSMType` | `CreateFunc` returned a type implementing none of the SM interfaces |
| `ErrInvalidSession` | wrong shard, a no-op session used for a session operation, or a session not prepared for proposals |
| `ErrSessionBusy` | the session already has a proposal in flight |
| `ErrInvalidRange` | log query with `firstIndex >= lastIndex` or `maxSize == 0` |
| `ErrInvalidMember` | sentinel form of `*InvalidMemberError` |
| `ErrEntryTooLarge` | sentinel form of `*EntryTooLargeError` |
| `ErrStaleConfigChange` | sentinel form of `*StaleConfigChangeError` |
| `ErrNonVotingRoleInMembers` | sentinel form of `*NonVotingRoleInMembersError` |
| `ErrInvalidSnapshotOption` | sentinel form of `*SnapshotOptionError` |
| `ErrRecoveredTermUnknown` | an on-disk SM's recovered applied index has no resolvable Raft term. Seeding term 0 would let the replica grant a vote to every candidate, so the start fails |

Membership policy (re-exported from the internal Raft package; the full set is
in [Membership validation errors](#membership-validation-errors)):
`ErrTooManyWitnesses`, `ErrBreaksFullReplicaMajority`,
`ErrRemoveLastFullReplica`, `ErrNodeNotFound`, `ErrNodeAlreadyExists`,
`ErrNodeRemoved`, `ErrRemoveLastVoter`, `ErrTooManyVotingMembers`,
`ErrZeroReplicaID`, `ErrEmptyAddress`, `ErrMemberIdentityRequired`,
`ErrRemovedMemberIdentity`, `ErrDuplicateMemberAddress`,
`ErrDuplicateMemberIdentity`.

Certificate exclusivity (re-exported from the internal transport package):
`ErrSharedPeerCertificate`, the sentinel form of `*SharedPeerCertificateError`.

Sessions:

| Error | Returned when |
|---|---|
| `ErrSessionLimitExceeded` | the shard-wide or per-owner session limit is reached. `ErrTooManySessions` is an alias for the same value |
| `ErrSessionNotFound` | the referenced session does not exist |
| `ErrSessionExpired` | sentinel form of `*SessionExpiredError` |
| `ErrSessionAlreadyExists` | registering a duplicate session |
| `ErrResponseLimitExceeded` | the session response cache is full |
| `ErrSessionsUnsupportedOnDisk` | any session entry point on an `sm.DiskStateMachine` shard |
| `ErrEntropyDegenerate` | the system CSPRNG returned the rejected value on every attempt while minting a client ID |

Snapshots:

| Error | Returned when |
|---|---|
| `ErrSnapshotNotFound` | no snapshot available |
| `ErrSnapshotInProgress` | the shard's snapshot slot is taken |
| `ErrSnapshotRecoveryFailed` | restoring a snapshot failed. Produced as `*SnapshotFailedError` with `Recovery` set |
| `ErrSnapshotTooLarge` | sentinel form of `*SnapshotTooLargeError` |
| `ErrSnapshotSizeMismatch` | the file size on disk does not match the `FileSize` in the message or in the snapshot's LogDB record |
| `ErrSnapshotCorrupt` | a corrupt or oversized frame length during decryption, or data that does not match the length and hash recorded in `snapshot.meta` or the export trailer. A `*SnapshotMetaInvalidError` matches it |
| `ErrSnapshotDestinationRecorded` | a received snapshot's directory is the one LogDB records as this replica's live snapshot |
| `ErrSnapshotBarrierSealed` | a snapshot could not be written because the shard's barrier is configured but sealed |
| `ErrSnapshotEncryptedBarrierUnavailable` | an encrypted snapshot could not be read because no barrier is configured for the shard or it is sealed |
| `ErrImportLayoutUnknown` | sentinel form of `*ImportLayoutUnknownError` |
| `ErrImportDataDirMismatch` | sentinel form of `*ImportDataDirMismatchError` |
| `ErrImportSnapshotDirExists` | sentinel form of `*ImportSnapshotDirExistsError` |
| `ErrImportMembershipInvalid` | sentinel form of `*ImportMembershipInvalidError` |

Encryption and storage:

| Error | Returned when |
|---|---|
| `ErrBarrierSealed` | encrypt or decrypt while the barrier is sealed. Re-exported from `pkg/seal` |
| `ErrBarrierNotInit` | the barrier has no sealing strategy. Re-exported from `pkg/seal` |
| `ErrDecryptionFailed` | corrupted ciphertext or wrong key. Re-exported from `pkg/seal` |
| `ErrBarrierPlaintextLogDB` | `WithBarrier` over a supplied LogDB that would store plaintext |
| `ErrShardBarriersPlaintextWAL` | `WithShardBarriers` over a LogDB that would store plaintext |
| `ErrShardBarrierUnresolved` | the per-shard resolver returned nil and there is no host-wide barrier |
| `ErrShardBarrierLogDBMismatch` | the LogDB stores this shard under a different barrier than the resolver returned |
| `ErrLogDBWrite` | a LogDB write failed |
| `ErrDecompressionBomb` | a decompressed payload exceeds the configured limit. Proposals in the failed batch receive it; `ShardFailedInfo.Err` is a `*ApplyError` wrapping it |

Engine, Raft and transport causes. These reach a caller inside a failed
proposal's `*ApplyError` or `*CommitError`, inside `ShardFailedInfo.Err`, or
inside a `*HostInitError`, and are re-exported so they can be matched:

| Error | Returned when |
|---|---|
| `ErrApplyGap` | a committed batch did not start at the entry after the last applied one. Cause inside `*ApplyError` |
| `ErrCorruptConfigEntry` | a committed config change or bootstrap membership entry does not decode. Cause inside `*CommitError`, or `HostInitError` `config_replay` / `membership_reconstruct` |
| `ErrSnapshotMetaCorrupt` | sentinel form of `*SnapshotMetaInvalidError` (which also matches `ErrSnapshotCorrupt`). `HostInitError` `snapshot_integrity`, or a failed recovery |
| `ErrScratchNotUnlinked` | sentinel form of `*ScratchUnlinkError`. `HostInitError` `snapshot_decrypt`, or a failed recovery |
| `ErrUnsupportedSnapshotCompression` | the snapshot record names a compression this binary cannot read. `HostInitError` `snapshot_compression`, or a failed recovery |
| `ErrEntropyUnavailable` | crypto/rand could not seed the Raft peer. `HostInitError` `peer` |
| `ErrCACertificateParse` | sentinel form of `*CACertificateParseError`. `HostInitError` `transport` |
| `ErrInvalidCertKeyPair` | a pre-built `MTLSConfig.Certificate` whose leaf does not parse. `HostInitError` `transport` |

### Structured error types

Each type below implements `error`; those with an `Err` field implement
`Unwrap()`.

| Type | Fields | Meaning |
|---|---|---|
| `NotLeaderError` | `ShardID`, `LeaderID` (0 if unknown), `LeaderAddress` (empty if unknown) | Returned by membership calls and `RequestLeaderTransfer` on a non-leader, and by `Propose` when no leader is known. Matches `ErrNotLeader`. Prefer `NotLeader(err)`, which also matches the bare sentinel |
| `EntryTooLargeError` | `Size`, `MaxSize` | `len(cmd)` exceeded the effective limit; `MaxSize` is that limit. Matches `ErrEntryTooLarge` |
| `BusyError` | `ShardID`, `CurrentSize`, `MaxSize`, unexported message | Per-shard rate limit. `ErrSystemBusy` is itself a `*BusyError`, so one `errors.Is(err, ErrBusy)` catches both limiters |
| `SessionError` | `ClientID`, `Op` ("register" or "unregister"), `Err` | A session register or unregister failed: `ErrInvalidOperation` at a session cap, `ErrEntropyDegenerate` when no client ID could be minted |
| `SessionExpiredError` | `ShardID`, `ClientID` | A session-managed proposal named a client ID whose session is no longer registered. Matches `ErrSessionExpired` |
| `ShardNotStoppedError` | `ShardID`, `ReplicaID` | `RemoveData` on a running shard. Matches `ErrShardNotStopped` |
| `ReplicaNotFoundError` | `ShardID`, `ReplicaID` | The replica ID does not match the local replica (`StopReplica`, `RequestCompaction`) or names no member (`RequestLeaderTransfer`). Matches `ErrReplicaNotFound` |
| `ReplicaRetiredError` | `ShardID`, `ReplicaID` | `StartShard` of a retired replica ID. Matches `ErrReplicaRetired` |
| `StaleConfigChangeError` | `ShardID`, `Expected`, `Got` | The `configChangeIndex` guard failed. Matches `ErrStaleConfigChange`. Re-read membership; the identical request fails forever |
| `HostInitError` | `Field`, `Err` | A component failed in `NewHost` or `StartShard`; `Field` names it. `Err` is a public value: a stdlib or `pkg/logdb`/`pkg/proto`/`pkg/config` error, the state machine's own error, or one of the re-exports in this section (for example `snapshot_integrity` carries `*SnapshotMetaInvalidError` or `ErrSnapshotSizeMismatch`, `snapshot_decrypt` a `*SnapshotEncryptError`, `transport` a `*CACertificateParseError` or `ErrInvalidCertKeyPair`) |
| `SnapshotSendError` | `ShardID`, `ReplicaID`, `Op`, `Err` | A snapshot send failed; `Op` names the step |
| `SnapshotReceiveError` | `ShardID`, `ReplicaID`, `Op`, `Err` | A snapshot receive failed; `Op` names the step |
| `SnapshotChunkError` | `ChunkID`, `ChunkCount`, `Reason` | Cause inside a `SnapshotReceiveError` with `Op` "validate": the delivered chunks are not one snapshot |
| `SnapshotSizeMismatchError` | `Declared`, `Actual` | Cause inside a `SnapshotReceiveError` with `Op` "validate": the reassembled bytes do not add up to the declared `FileSize` |
| `SnapshotFailedError` | `ShardID`, `ReplicaID`, `Recovery`, `Err` | A snapshot save or recovery failed. Matches `ErrSnapshotRecoveryFailed` when `Recovery` is true |
| `SnapshotTooLargeError` | `ShardID`, `MaxSize` | Matches `ErrSnapshotTooLarge` |
| `SnapshotArtifactError` | `Reason`, `Err` | An export artifact's header is malformed or its data fails the trailer check |
| `SnapshotOptionError` | `Field`, `Reason` | From `SnapshotOption.Validate`. Matches `ErrInvalidSnapshotOption` |
| `SnapshotCommittedNotDurableError` | `Path`, `Epoch`, `Err` | `ReencryptSnapshotFile`'s directory fsync failed after the rename. Record `Epoch` (dropping it lets the next purge orphan the artifact's key) and treat the migration as unverified |
| `UnauthorizedMessageError` | `ShardID`, `From`, `MsgType` | An inbound Raft message failed shard-membership authorization |
| `TypeAssertionError` | `Context`, `ExpectedType`, `ActualValue` | An internal type assertion failed |
| `LoopPanicError` | `LoopName`, `Value`, `Stack` | A recovered panic from a goroutine loop, with its stack |
| `EmptyMembersError` | `ShardID` | Bootstrap with no members |
| `SelfNotInMembersError` | `ShardID`, `ReplicaID` | The local replica is not in the initial members |
| `InvalidMemberError` | `ShardID`, `ReplicaID`, `Address`, `Reason` | A member or identity failed validation; see `StartShard`. Matches `ErrInvalidMember` |
| `NonVotingRoleInMembersError` | `ShardID`, `ReplicaID`, `Role` ("witness" or "observer") | A witness or observer named in the bootstrap members. Matches `ErrNonVotingRoleInMembers` |
| `MaybeAppliedError` | `Err` | The request reached Raft before failing; see `MaybeApplied` |
| `BootstrapEntryMissingError` | `Index`, `EntryType` | The log holds something other than the founding configuration at the bootstrap index. The log is damaged: restore this replica's data from backup, or wipe it and re-add the replica with `join=true`. Rebuilding membership from the `members` map would drop witnesses, observers, tombstones, identities and replicated policy |
| `CorruptConfigEntryError` | `ShardID`, `Index`, `Kind`, `Err` | A committed configuration entry failed to decode. Surfaced through the shard's failure path, not returned from a call |
| `ApplyError` | `ShardID`, `ReplicaID`, `Err` | A batch failed to apply: the state machine's own error, a `*DecompressError`, `ErrDecompressionBomb` or `ErrApplyGap`. Returned to the batch's proposals and delivered as `ShardFailedInfo.Err`. Does not match `ErrShardFailed` |
| `SMPanicError` | `ShardID`, `ReplicaID`, `Value`, `Stack` | The state machine panicked during apply or snapshot recovery. `ShardFailedInfo.Err`; matches `ErrShardFailed`. An apply panic's batch proposals receive it, wrapped in `*MaybeAppliedError`, so `errors.As` reaches it and `errors.Is(err, ErrShardFailed)` holds |
| `SnapshotSavePanicError` | `ShardID`, `ReplicaID`, `Value`, `Stack` | The state machine panicked in `SaveSnapshot`. The shard is not failed (a save only reads the state machine), so it does not match `ErrShardFailed`. A `SyncRequestSnapshot` caller gets it as the `Err` of a save `*SnapshotFailedError`; `OnSnapshotRejected` fires |
| `SaveStateError` | `ShardID`, `ReplicaID`, `Err` | A batch could not be persisted to the LogDB. Returned to the batch's proposals and delivered as `ShardFailedInfo.Err` |
| `CommitError` | `ShardID`, `ReplicaID`, `Err` | A persisted batch could not be committed (log reader refusal, `ErrCorruptConfigEntry`). Returned to the batch's proposals and delivered as `ShardFailedInfo.Err` |
| `WorkerPanicError` | `WorkerID`, `WorkerType`, `Value`, `Stack` | A step worker panicked during a snapshot install. `ShardFailedInfo.Err` |
| `SnapshotError` | `ShardID`, `ReplicaID`, `Save`, `Err` | A snapshot save or recovery failed in the snapshot pool. `ShardFailedInfo.Err` for a recovery that failed once `RecoverFromSnapshot` was invoked; a recovery that failed before it is rejected (`OnSnapshotRejected`, with it as `SnapshotInfo.Err`) and re-driven, and only the last rejected attempt fails the shard, as the cause of a `*SnapshotRecoveryExhaustedError`; a failed save fires no `OnShardFailed`, and a `SyncRequestSnapshot` caller gets either failure as `*SnapshotFailedError` |
| `SnapshotRecoveryBehindError` | `ShardID`, `ReplicaID`, `SnapshotIndex`, `Committed`, `Processed` | A recovery was abandoned because the Raft log never reached the snapshot index. The state machine was not touched, so the recovery is rejected (`OnSnapshotRejected` with it as `SnapshotInfo.Err`) and re-driven |
| `SnapshotRecoveryPanicError` | `ShardID`, `ReplicaID`, `Value`, `Stack` | A recovery panicked before `RecoverFromSnapshot` was invoked. The state machine was not touched, so it does not match `ErrShardFailed`: the recovery is rejected (`OnSnapshotRejected` with it as `SnapshotInfo.Err`) and re-driven |
| `SnapshotRecoveryExhaustedError` | `ShardID`, `ReplicaID`, `SnapshotIndex`, `Attempts`, `Err` | Every attempt to recover a snapshot the Raft log adopted was rejected before the restore began, over one continuous episode of holding entries, so the shard was failed. `Attempts` counts the whole episode, whatever snapshot each attempt was made against; `SnapshotIndex` is the snapshot of the last attempt, and `Err` its cause, reachable with `errors.Is`/`errors.As`. `ShardFailedInfo.Err` |
| `SnapshotEncryptError` | `Op`, `Err` | A snapshot frame could not be encrypted or decrypted; `ErrSnapshotCorrupt` for a damaged artifact. `HostInitError` `snapshot_decrypt`, or inside `*SnapshotError` |
| `SnapshotMetaInvalidError` | `Dir`, `Err` | A recorded snapshot's `snapshot.meta` cannot vouch for its data. Matches `ErrSnapshotCorrupt` and `ErrSnapshotMetaCorrupt` |
| `ScratchUnlinkError` | `Path`, `Err` | A decrypt scratch file could not be unlinked. Matches `ErrScratchNotUnlinked` |
| `DecompressError` | `Err` | A committed entry's payload does not decode. Cause inside `*ApplyError` |
| `CACertificateParseError` | `Reason`, `Err` | With `RevocationConfig` set, the CA bundle has an unparseable block, no certificate, or no issuer. Matches `ErrCACertificateParse` |
| `InvalidShardHealthError` | `Value` | `ShardHealth.UnmarshalJSON` got an unrecognized string. Matches `ErrInvalidShardHealth` |
| `ImportLayoutUnknownError` | `ShardID`, `ReplicaID` | see [ImportSnapshot](#importsnapshot) |
| `ImportDataDirMismatchError` | `ShardID`, `ReplicaID`, `Declared`, `Recorded` | see [ImportSnapshot](#importsnapshot) |
| `ImportSnapshotDirExistsError` | `ShardID`, `ReplicaID`, `Index`, `Dir` | see [ImportSnapshot](#importsnapshot) |
| `ImportSnapshotOutOfDateError` | `ShardID`, `ReplicaID`, `Index`, `Recorded` | matches `logdb.ErrSnapshotOutOfDate` |
| `ImportMembershipInvalidError` | `ShardID`, `ReplicaID`, `Reason` | see [ImportSnapshot](#importsnapshot) |

## Metrics

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

Passed to `WithMetrics`. Each component receives only its sub-interface:
`RaftMetrics` to the engine, `TransportMetrics` to the transport,
`LogDBMetrics` to the WAL. `RegisterShard` and `UnregisterShard` let an
implementation pre-bind per-shard metrics at shard start instead of looking up
labels on every hot-path call.

`RaftMetrics` (10 methods):

| Method | Signature |
|---|---|
| `ObserveProposalLatency` | `(shardID uint64, d time.Duration)` |
| `ObserveCommitLatency` | `(shardID uint64, d time.Duration)` |
| `ObserveApplyLatency` | `(shardID uint64, d time.Duration)` |
| `ObserveReadIndexLatency` | `(shardID uint64, d time.Duration)` |
| `IncProposalDropped` | `(shardID uint64)` |
| `ObserveProposalBatchSize` | `(shardID uint64, size int)` |
| `ObserveCommitBatchSize` | `(size int)` |
| `IncElection` | `(shardID uint64)` |
| `IncSnapshot` | `(shardID uint64)` |
| `IncPiggybackCommit` | `(shardID uint64)` |

`ObserveReadIndexLatency` covers the whole ReadIndex round-trip: the heartbeat
quorum plus any wait for the state machine. A piggyback commit is one the commit
worker detected and applied in the same cycle.

`LogDBMetrics` (4 methods): `ObserveWriteLatency(d time.Duration)`,
`ObserveFsyncLatency(d time.Duration)`, `ObserveCompactionLatency(d
time.Duration)`, `IncWriteBytes(n int64)`.

`TransportMetrics` (7 methods):

| Method | Signature |
|---|---|
| `ObserveSendLatency` | `(d time.Duration)` |
| `IncMessagesSent` | `(shardID, to uint64, msgType uint64)` |
| `IncMessagesReceived` | `(shardID, from uint64, msgType uint64)` |
| `IncSnapshotsSent` | `(shardID uint64, bytes int64, d time.Duration)` |
| `IncSnapshotsReceived` | `(shardID uint64, bytes int64, d time.Duration)` |
| `IncSnapshotsRejected` | `(shardID uint64, reason string)` |
| `IncMessagesDropped` | `(shardID, to uint64, reason string)` |

Both `reason` arguments come from closed sets fixed by the transport. Treat them
as labels and never derive new values from them.

| Method | `reason` values |
|---|---|
| `IncSnapshotsRejected` | `shutdown`, `concurrency_limit`, `memory_budget`, `liveness`, `malformed`, `io_error`, `policy`, `unauthorized`, `not_accepting` |
| `IncMessagesDropped` | `unresolved_target`, `queue_full`, `stream_unavailable`, `write_failed`, `marshal_failed`, `protocol_violation`, `unsendable`, `peer_rejected`, `identity_mismatch` |

`shardID` is 0 for a snapshot rejected before its sender was authorized, since
until then the shard ID is whatever the peer wrote. `shardID` and `to` are both
0 for `protocol_violation`.

Alert on `unsendable` and `peer_rejected` separately. `unsendable` is a message
no peer can accept (more entries than `proto.MaxEntriesPerMessage`, or larger
alone than a frame), refused before it is written, so retrying it unchanged
never succeeds. `peer_rejected` is a batch the peer's decoder refused with a
stream reset: the connection is fine and only the frame was unacceptable, which
usually means version skew.

`SecurityMetrics` (2 methods):

| Method | Signature |
|---|---|
| `IncUnauthorizedMessageDropped` | `(shardID, from uint64, reason string)` |
| `IncUnauthorizedSnapshotDropped` | `(shardID, from uint64)` |

Every increment is either a misconfigured cluster or an injection attempt by an
authenticated peer, so both need an alerting rule. `shardID` is 0 for a snapshot
naming a shard this host has not loaded.

| `reason` | Meaning |
|---|---|
| `unknown_member` | no configuration this replica holds (committed, pending in its own log, or `Config.MemberIdentities`) names the sender. Transient during a membership change. Persistent means this replica missed the change that added the sender: restart it with a map that names the leader |
| `identity_mismatch` | the strongest signal. Committed membership records an identity for the claimed replica and the connection presented another, so the sender does not hold that replica's key: a forged `From`, or a stale identity after a key change. Never routine |
| `missing_identity` | the shard records no identity for a replica the configuration names. Admission requires one behind a transport, so this configuration was written without that requirement; wipe and re-bootstrap |
| `removed_replica` | the sender was removed from the shard |
| `removed_identity` | the connection presents an identity the shard has removed |
| `misrouted` | the message names a shard this host does not serve |

The sender's replica ID is never a label: it arrives unauthenticated, so an
attacker could mint unbounded series. It is in the drop log line.

### Provided collectors

```go
type NoOpMetricsCollector struct{}

type SampledMetricsCollector struct {
    MetricsCollector
    SampleRate uint64 // inverse rate: 100 records 1 observation in 100
    // Has unexported fields.
}
func NewSampledMetricsCollector(inner MetricsCollector, sampleRate uint64) *SampledMetricsCollector

func NewPrometheusMetricsCollector() *PrometheusMetricsCollector
func NewPrometheusMetricsCollectorWithConfig(cfg config.MetricsConfig) *PrometheusMetricsCollector
```

`NoOpMetricsCollector` does nothing. Embed it and override selectively, which
also keeps your type compiling when a method is added.

`SampledMetricsCollector` thins the two hottest histogram observations, commit
latency and apply latency, to 1 in `SampleRate`; a `sampleRate` of 0 becomes
100. Counters are never sampled, because a sampled counter would make every
`rate()` built on it wrong. Each sampled metric has its own counter, so the
sampling decision for one never depends on calls to the other.

A caller-supplied collector is wrapped so a panic inside it cannot unwind an
engine worker. Panics are counted and logged at the first occurrence and then at
powers of two. `NoOpMetricsCollector` and an already-wrapped collector pass
through unwrapped.

`PrometheusMetricsCollector` registers every metric on its own
`prometheus.Registry` (`collector.Registry`), not the global default.
`NewPrometheusMetricsCollector` uses the zero `config.MetricsConfig`, so
`PerShardMetrics` is off and volume and latency telemetry aggregates to one
host-level series per metric. `NewPrometheusMetricsCollectorWithConfig` opts in
to a `shard_id` breakdown. The mode is fixed at construction, because a
Prometheus vector's label set is decided when the vector is built. The two
`*_detailed_total` series are emitted only when `collector.EnablePerMessageMetrics`
is set to true after construction; they are per-message on the hot path and
their label set is shard x peer x message type.

Metric names (`shard_id*` marks a label present only with `PerShardMetrics`;
the cardinality budget is in [user.md](user.md#metrics-cardinality)):

| Subsystem | Metric | Type | Labels |
|---|---|---|---|
| raft | `quicraft_raft_proposal_latency_seconds` | Histogram | `shard_id*` |
| raft | `quicraft_raft_commit_latency_seconds` | Histogram | `shard_id*` |
| raft | `quicraft_raft_apply_latency_seconds` | Histogram | `shard_id*` |
| raft | `quicraft_raft_read_index_latency_seconds` | Histogram | `shard_id*` |
| raft | `quicraft_raft_proposal_batch_size` | Histogram | `shard_id*` |
| raft | `quicraft_raft_snapshots_total` | Counter | `shard_id*` |
| raft | `quicraft_raft_piggyback_commits_total` | Counter | `shard_id*` |
| raft | `quicraft_raft_proposals_dropped_total` | Counter | `shard_id` |
| raft | `quicraft_raft_elections_total` | Counter | `shard_id` |
| raft | `quicraft_raft_commit_batch_size` | Histogram | none |
| logdb | `quicraft_logdb_write_latency_seconds` | Histogram | none |
| logdb | `quicraft_logdb_fsync_latency_seconds` | Histogram | none |
| logdb | `quicraft_logdb_compaction_latency_seconds` | Histogram | none |
| logdb | `quicraft_logdb_write_bytes_total` | Counter | none |
| transport | `quicraft_transport_send_latency_seconds` | Histogram | none |
| transport | `quicraft_transport_messages_sent_total` | Counter | none |
| transport | `quicraft_transport_messages_received_total` | Counter | none |
| transport | `quicraft_transport_messages_sent_detailed_total` | Counter | `shard_id`, `to`, `msg_type` |
| transport | `quicraft_transport_messages_received_detailed_total` | Counter | `shard_id`, `from`, `msg_type` |
| transport | `quicraft_transport_snapshots_sent_total` | Counter | `shard_id*` |
| transport | `quicraft_transport_snapshots_sent_bytes` | Histogram | `shard_id*` |
| transport | `quicraft_transport_snapshots_received_total` | Counter | `shard_id*` |
| transport | `quicraft_transport_snapshots_received_bytes` | Histogram | `shard_id*` |
| transport | `quicraft_transport_messages_dropped_total` | Counter | `shard_id`, `reason` |
| transport | `quicraft_transport_snapshots_rejected_total` | Counter | `shard_id`, `reason` |
| security | `quicraft_security_unauthorized_messages_dropped_total` | Counter | `shard_id`, `reason` |
| security | `quicraft_security_unauthorized_snapshots_dropped_total` | Counter | `shard_id` |
| metrics | `quicraft_metrics_per_shard_enabled` | Gauge | none |

Latency buckets run from 100µs to 10s, batch-size buckets from 1 to 1024 in
powers of two, and snapshot byte buckets from 1KB to 1GB.

## Configuration

Package `config` (`pkg/config`). `NewHost` and `StartShard` call `SetDefaults`
and then `Validate`. Most defaults below are also exported as
`Default<Field>` constants.

### HostConfig

Source: `pkg/config/hostconfig.go`.

| Field | Type | Default | Description |
|---|---|---|---|
| `WALDir` | `string` | required | write-ahead log directory |
| `NodeHostDir` | `string` | required | metadata, snapshots, host ID |
| `RaftAddress` | `string` | required | address peers dial |
| `ListenAddress` | `string` | required | local bind address (`host:port`) |
| `RTTMillisecond` | `uint64` | `200` | estimated round-trip time in ms; 1 to 60000 |
| `DeploymentID` | `uint64` | required, non-zero | cluster identity token. Not cryptographic; use a separate CA per cluster for cryptographic isolation |
| `AllowZeroDeploymentID` | `bool` | `false` | permits `DeploymentID == 0`, for development and tests |
| `HostID` | `string` | `""` | when empty, a random 128-bit hex ID is generated and stored in `NodeHostDir/host.id` |
| `NumWorkers` | `uint64` | `runtime.NumCPU()` | workers per tier |
| `MaxApplyWorkers` | `int` | `runtime.NumCPU()` | concurrent per-shard apply goroutines |
| `CommitCBufferSize` | `int` | `64` | commit channel buffer per worker |
| `ShutdownTimeout` | `time.Duration` | `30s` | bounds `Drain` only; see below |
| `MaxWALDiskSize` | `uint64` | `0` (unlimited) | WAL size budget in bytes |
| `NoSyncWAL` | `bool` | `false` | skip WAL `fdatasync`; testing only, and `Validate` logs a warning |
| `MaxTotalInMemLogSize` | `uint64` | `0` (disabled) | global rate limiter threshold in bytes |
| `NotifyCommit` | `bool` | `false` | enables `RequestState.CommittedC()` |
| `EventListener` | `*EventListener` | `nil` | event callbacks; nil costs nothing |
| `TransportConfig` | `TransportConfig` | see below | transport tuning |
| `KeyRotationInterval` | `time.Duration` | `0` | scheduled barrier rotation; a non-zero value must be at least 1s |
| `MaxRetainedEpochs` | `uint64` | `0` (retain all) | old barrier epochs kept in memory |
| `LogConfig` | `LogConfig` | text, info | `Format` (`LogFormatText`, `LogFormatJSON`) and `Level` (`LogLevelDebug` to `LogLevelError`) |

`KeyRotationInterval` of 0 disables only scheduled rotation. Rotation at the
NIST SP 800-38D AEAD invocation budget still runs whenever a barrier is
configured. 24 hours is a reasonable production value. It applies to the
`WithBarrier` barrier and is ignored otherwise. `Validate` warns when it is set
and `MaxRetainedEpochs` is 0.

`ShutdownTimeout` bounds `Drain` only. `Close` also waits, with no timeout, for
every in-flight `SM.Apply` to return, so a state machine that blocks in `Apply`
blocks `Close` indefinitely. Abandoning that wait would let `Close` return while
an apply goroutine still touches state the caller is about to free.

Apply is fail-fast and nothing configures that. The first `SM.Update` or
decompression error marks the shard failed, and `lastApplied` is not advanced,
because advancing past a failed batch would diverge this replica from its peers.

### Config (per shard)

Source: `pkg/config/config.go`.

| Field | Type | Default | Description |
|---|---|---|---|
| `ShardID` | `uint64` | required | shard identifier |
| `ReplicaID` | `uint64` | required | replica identifier within the shard |
| `IsObserver` | `bool` | `false` | non-voting. Initial hint only; the committed membership decides |
| `IsWitness` | `bool` | `false` | voting, no state machine. Binding; join-only |
| `Witness` | `WitnessConfig` | zero value | seeds the replicated witness policy at bootstrap |
| `MemberIdentities` | `map[uint64]string` | `nil` | founders: every member's identity, identical on each. Joiners: the anchor. Entries beyond the members map are operator trust for the receive gate. Required behind a transport |
| `RequireMemberIdentities` | `bool` | set by the Host | true on every shard behind a transport: admission refuses an identity-less member. Validate-only; a committed entry is never refused on apply |
| `CheckQuorum` | `*bool` | nil = on | leader steps down on quorum loss. Disabling is unsafe and logged |
| `PreVote` | `*bool` | nil = on | pre-vote extension. Disabling is unsafe and logged |
| `OrderedConfigChange` | `*bool` | nil = on | one membership change at a time. Disabling is unsafe and logged |
| `ElectionRTT` | `uint64` | `10` | election timeout in `RTTMillisecond` multiples; at least 5 and at least `2 * HeartbeatRTT` |
| `HeartbeatRTT` | `uint64` | `1` | heartbeat interval in `RTTMillisecond` multiples |
| `SnapshotEntries` | `uint64` | `100000` | applied entries between automatic snapshots. `config.SnapshotsDisabled` turns them off |
| `CompactionOverhead` | `uint64` | `5000` | entries kept after compaction |
| `MaxInMemLogSize` | `uint64` | `0` (disabled) | per-shard rate limit in bytes |
| `MaxProposalPayloadSize` | `uint64` | `8 MB` | user-facing proposal cap; may not exceed `proto.MaxEntrySize` |
| `MaxEntrySize` | `uint64` | `8 MB` | entry cap and the decompression ceiling on apply; may not exceed `proto.MaxEntrySize` |
| `MaxApplyEntrySize` | `uint64` | `16711672` | replication chunk size and `SM.Update` batch bound (`config.MaxReplicationPayloadBytes`, the most a frame can carry) |
| `MaxSnapshotSize` | `uint64` | `4 GB` | maximum snapshot file size |
| `MaxSessionsPerClient` | `uint64` | `128` | cached responses per session. Bootstrap seed; replicated |
| `MaxTotalSessions` | `uint64` | `16384` | shard-wide session cap. Bootstrap seed; replicated |
| `MaxSessionsPerOwner` | `uint64` | `128` | per-owner session cap; `config.SessionsPerOwnerUnlimited` removes it. Bootstrap seed; replicated |
| `SessionExpiryEntries` | `uint64` | `100000` | inactivity threshold in log entries; a non-zero value must be at least 100. `config.SessionExpiryDisabled` turns expiry off. Bootstrap seed; replicated |
| `SnapshotCompressionType` | `CompressionType` | `NoCompression` | `NoCompression` or `Snappy` |
| `EntryCompressionType` | `CompressionType` | `NoCompression` | `NoCompression` or `Snappy` |
| `Quiesce` | `bool` | `false` | quiescence for idle shards |
| `ZeroCopyEntryCmd` | `bool` | `false` | `sm.Entry.Cmd` aliases an internal buffer; unsafe to retain past `Update` |
| `LeaseRead` | `bool` | `false` | lease-based reads (Raft PhD §6.4). Requires `CheckQuorum` |
| `DataDir` | `string` | `""` | absolute root for this shard's on-disk state machine data; empty places it under `NodeHostDir`. The Raft log stays in the host-wide LogDB |

`Config` methods: `SetDefaults`, `Validate`, `ValidateElectionTimeout(rttMillisecond)`,
`GetCheckQuorum`, `GetPreVote`, `GetOrderedConfigChange`. Use
`config.BoolPtr(false)` to set a `*bool` field.

**Zero does not mean "off" for `SnapshotEntries`, `SessionExpiryEntries` or
`MaxSessionsPerOwner`.** Each takes its default at zero and has a named sentinel
(all `math.MaxUint64`) for disabling. An unset field cannot mean both "I did not
choose" and "never do this", and each "off" is a poor default:

- A shard that never snapshots grows its log and restart replay without bound,
  and here also never reclaims a snapshot directory, never zeroes an old epoch's
  DEK and never asks its barrier to encrypt anything: the GC, the purge floor and
  the barrier all advance only inside a snapshot save.
- A client that registers a session and dies without `CloseSession` leaks an
  entry into every later snapshot and counts against `MaxTotalSessions` forever
  unless sessions expire.
- An owner able to register 16384 sessions can deny at-most-once delivery to
  every other tenant on the shard.

`SessionExpiryEntries` is measured in log entries, so the wall-clock window
shrinks as throughput grows: generous on a quiet shard, about ten seconds of
client silence on one sustaining 10k entries/s. `Validate` warns when it
exceeds a non-zero `SnapshotEntries`.

`MaxProposalPayloadSize` and `MaxEntrySize` are both enforced at `Propose`; the
effective limit is the smaller. Set the first to impose a tighter user-facing
cap.

`LeaseRead` also assumes every replica uses the same `ElectionRTT`,
`RTTMillisecond` and `LeaseRead`, and that clock rates differ by less than 1%.
See [raft-consensus](../raft-consensus/engineering.md#15-leaseread).

> **`IsObserver` is an initial hint; `IsWitness` is binding.** The authoritative
> role is in replicated membership (`Membership.Observers`,
> `Membership.Witnesses`). `IsObserver` is consulted only while the shard's
> configuration does not mention this replica; after that the committed
> membership decides, in either direction. An observer promoted with
> `RequestAddNodeWithIdentity` (same replica ID, address and identity) becomes a
> voter in place: it votes, campaigns and serves linearizable reads without a
> restart, and `ShardInfo.IsObserver` reports the effective role. A witness keeps
> no state machine, so a replica whose configured witness role contradicts the
> membership refuses to campaign or serve reads until the two agree, and a
> witness is never promoted.

### WitnessConfig

```go
type WitnessConfig struct {
    MaxWitnesses               int   // 0 = no explicit cap; negative is a validation error
    RequireFullReplicaMajority *bool // nil = true
}

func (w WitnessConfig) GetRequireFullReplicaMajority() bool
```

`Config.Witness` seeds the policy at bootstrap and does nothing more. From then
on the policy is replicated state, changed only through
`Host.RequestUpdateWitnessPolicy`; a restart recovers the replicated value.

`RequireFullReplicaMajority` rejects any config change (a witness add or a
full-replica removal) that would leave the full, data-bearing replicas unable to
form a quorum of the voting set. Adding a third witness to {3 full, 2 witnesses}
is rejected, because 3 full replicas are below the quorum of 4; {2 full, 1
witness} is allowed, because 2 >= 2. This bounds membership shape only: a commit
can still rest on the leader plus witnesses when the other full replicas lag.

### TransportConfig

Source: `pkg/config/transportconfig.go`. Behaviour is documented in
[quic-transport](../quic-transport/api.md).

| Field | Type | Default | Description |
|---|---|---|---|
| `MTLSConfig` | `*MTLSConfig` | `nil` | CA certificate plus a PEM `Cert`/`Key` pair or a pre-built `*tls.Certificate`. Required unless `TransportDisabled` |
| `RevocationConfig` | `*RevocationConfig` | `nil` | CRL paths, OCSP responder, check interval, mode, soft-fail |
| `StreamPoolSize` | `int` | `16` | QUIC streams per connection |
| `MaxStreamPoolSize` | `int` | `256` | upper bound for the stream pool |
| `MaxStreamsPerConnection` | `int` | `256` | stream limit advertised per connection |
| `StreamReadIdleTimeout` | `time.Duration` | `30s` | idle timeout on a stream read |
| `MaxSnapshotReceiveRate` | `int64` | `256 MB/s` | inbound snapshot bandwidth; floor `MinMaxSnapshotReceiveRate` (64 KB/s) |
| `MaxConcurrentSnapshotReceives` | `int` | `4` | concurrent inbound snapshots |
| `MaxSnapshotReceiveMemory` | `int64` | `1 GB` | total snapshot receive buffer |
| `MaxMessageReceiveMemory` | `int64` | `512 MB` (derived) | total inbound message-frame heap budget; zero derives `MessageReceiveBudgetFrames` maximum-size frame charges (128 MB each), and an explicit value below one charge fails validation |
| `MessageReceiveBudgetFrames` | `int` | `4` | maximum-size frames the derived `MaxMessageReceiveMemory` default admits; ignored when the budget is set |
| `MaxDecompressedSize` | `uint32` | `16 MB` | must equal `DefaultMaxDecompressedSize` (the frame size); any other value fails validation |
| `MaxConnectionsPerIP` | `int` | `16` | inbound connections per source IP |
| `MaxIncomingConnections` | `int` | `256` | total inbound connections |
| `SendBatchMaxSize` | `int` | `64 KB` | send buffer size before flush |
| `UDPRecvBufSize` | `int` | `7 MB` | UDP receive buffer |
| `UDPSendBufSize` | `int` | `7 MB` | UDP send buffer |
| `Enable0RTT` | `bool` | `false` | QUIC 0-RTT; replayable |
| `DisableCompression` | `bool` | `false` | disable Snappy compression of message batches |
| `TransportDisabled` | `bool` | `false` | skip mTLS validation and transport construction; equivalent to `WithoutTransport()` |

`MaxDecompressedSize` is part of the wire contract, not a tuning knob: the send
path admits frames against the same constant, so a lower value would silently
reject frames every peer considers valid.

`MaxSnapshotReceiveRate` of 0 selects the default. A non-zero value below the
floor, or a negative one, fails validation, because a tiny rate would make the
receiver's proportional sleep run for days per chunk while holding the snapshot
semaphore and memory budget. Each per-chunk sleep is also capped at 5 s.

mTLS is mandatory whenever the transport is enabled, and `Validate` rejects
empty or malformed certificate material at the config boundary rather than as
an opaque handshake failure. A pre-built `Certificate` must carry a
`crypto.Signer` private key, which is what lets a TPM- or PKCS#11-backed key
work.

### Logging and other helpers

```go
func BoolPtr(v bool) *bool
func NewLogger(cfg LogConfig) *slog.Logger // text or JSON handler on os.Stderr
func SetDefaultLogger(cfg LogConfig)        // installs NewLogger(cfg) as the slog default

type CompressionType uint16 // NoCompression, Snappy; IsValid(), String()
type MetricsConfig struct{ PerShardMetrics bool }
```

`MetricsConfig` is deliberately not a `HostConfig` field: a Prometheus vector's
label set is fixed when the collector is built, before the host sees its
config, so it is passed to `quicraft.NewPrometheusMetricsCollectorWithConfig`.

### Validation errors

`*ValidationError` (`Field`, `Message`) for most constraints, plus typed errors
for the ones a caller may want to match:

| Error | Raised when |
|---|---|
| `*LogValidationError` | `LogConfig.Format` or `Level` is not recognized |
| `*MaxEntrySizeExceededError` | `MaxEntrySize` exceeds `proto.MaxEntrySize` |
| `*MaxApplyEntrySizeExceededError` | `MaxApplyEntrySize` exceeds `MaxReplicationPayloadBytes` |
| `*MaxProposalPayloadSizeExceededError` | `MaxProposalPayloadSize` exceeds `proto.MaxEntrySize` |
| `*SessionExpiryEntriesTooSmallError` | `SessionExpiryEntries` is non-zero and below 100 |
| `*KeyRotationIntervalTooSmallError` | `KeyRotationInterval` is non-zero and below 1s |
| `*LeaseReadRequiresCheckQuorumError` (`ErrLeaseReadRequiresCheckQuorum`) | `LeaseRead` with `CheckQuorum` disabled |
| `*MTLSConfigRequiredError` (`ErrMTLSConfigRequired`) | transport enabled with no `MTLSConfig` |

## State machine registration

Source: `pkg/sm/`.

```go
type CreateFunc func(shardID, replicaID uint64) interface{}

func NewCreateFunc(fn CreateStateMachineFunc) CreateFunc
func NewConcurrentCreateFunc(fn CreateConcurrentStateMachineFunc) CreateFunc
func NewDiskCreateFunc(fn CreateDiskStateMachineFunc) CreateFunc

type CreateStateMachineFunc func(shardID, replicaID uint64) StateMachine
type CreateConcurrentStateMachineFunc func(shardID, replicaID uint64) ConcurrentStateMachine
type CreateDiskStateMachineFunc func(shardID, replicaID uint64) DiskStateMachine
```

`CreateFunc` is the factory passed to `StartShard`; it must return one of the
three interfaces, detected by type assertion. The typed wrappers catch a wrong
type at compile time.

### Determinism is mandatory

`Update` must be a pure function of the current state and the entries. Every
replica applies the same entry sequence and must reach the same state. `Update`
must not read wall-clock time, `math/rand`, environment variables, host
identity, `GOMAXPROCS` or any other per-node value, and Go's randomized map
iteration order must not affect the state or the `Result`s.

Raft does not detect a violation. Replicas keep agreeing on the log while
silently diverging in state. Implement `sm.Hasher` and compare replicas with
`Host.StateMachineHash` to detect it.

### State machine interfaces

**`StateMachine`** (type code 0): all state in memory. `Update` and `Lookup`
never run concurrently; the RSM adapter's `readGuard` enforces it, so local reads
wait on an in-flight apply.

```go
type StateMachine interface {
    Update(ctx context.Context, entries []Entry, results []Result) error
    Lookup(ctx context.Context, query interface{}) (interface{}, error)
    SaveSnapshot(ctx context.Context, w io.Writer, stopper <-chan struct{}) error
    RecoverFromSnapshot(ctx context.Context, r io.Reader, stopper <-chan struct{}) error
    Close(ctx context.Context) error
}
```

**`ConcurrentStateMachine`** (type code 1): `Lookup` and `GetHash` run
concurrently with `Update`, so the implementation must synchronize itself, and
`GetHash` should hash whole batches. Two-phase snapshotting.

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

**`DiskStateMachine`** (type code 2): backed by persistent storage, with the same
concurrency as tier 1. `Open` returns the last applied index, and replay after a
crash must be idempotent.

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

On every tier `Update` must be all-or-nothing: either every entry is applied and
every `results[i]` written, or the state machine is left as it was and an error
is returned. An error is fatal to the shard (apply is fail-fast).

### Supporting types

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

`Entry.Cmd` is an owned copy by default. With `Config.ZeroCopyEntryCmd` it
aliases an internal buffer reused after `Update` returns; call `Entry.CopyCmd()`
for an owned copy.

Two `Result.Value` codes are reserved and must not be produced by application
code:

| Constant | Value | Meaning |
|---|---|---|
| `ResultSessionExpired` | `MaxUint64 - 2` | the entry named an unregistered client ID; the host turns it into `ErrSessionExpired` |
| `ResultInvalidCommand` | `MaxUint64 - 3` | the committed entry's payload could not be decoded |

`ResultInvalidCommand` exists because a committed entry must be applied the same
way on every replica. An `Update` error for a malformed payload would fail the
shard and wedge it on every replay, so the entry yields this result and leaves
the store unchanged.

### Sessions and on-disk state machines

A shard backed by an `sm.DiskStateMachine` supports no-op sessions only.
`GetNewSession`, `GetNewSessionWithOwner`, `RegisterSession`,
`ProposeWithSession`, `SyncProposeWithSession` and `CloseSession` return
`ErrSessionsUnsupportedOnDisk` there. Use `GetNoOPSession` and make operations
idempotent, the same constraint Dragonboat places on `IOnDiskStateMachine`.

The reason is `Open`. The session table travels only inside the snapshot
stream, so it is restored by `RecoverFromSnapshot`, and a `DiskStateMachine` that
reports a non-zero applied index from `Open` skips that path on restart
(recovering would roll its database back to the snapshot). The replica would
have the right data and no sessions, then answer `ResultSessionExpired` for
entries its peers apply normally: permanent, silent divergence.

### Optional interfaces

Detected by type assertion on the state machine:

| Interface | Method | Purpose |
|---|---|---|
| `Hasher` | `GetHash() (uint64, error)` | backs `Host.StateMachineHash` |
| `NALookup` | `BytesLookup(key []byte) ([]byte, error)` | byte-key reads without `interface{}` boxing |
| `NALookupInto` | `NALookup` plus `BytesLookupInto(key, dst []byte) ([]byte, error)` | the allocation-free path behind `SyncReadBuf` and `StaleReadBuf` |

These signatures must match exactly. Detection is by type assertion, so a wrong
signature compiles and is never called.

`BytesLookupInto` copies the value into `dst`, growing it with `append` if
needed, and returns the populated slice; the implementation must not keep a
reference to it. A `Hasher` must be order-independent (both reference stores in
`pkg/sm/kv` sort before hashing) or it reports divergence that does not exist.

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

Wraps a `DiskStateMachine` with a write-through LRU cache in front of disk reads
and implements `NALookup`. `CacheStats()` returns cumulative hits and misses.

`ExtractKV` returns `("", nil)` to skip an entry, and a key with a nil value to
signal a cache delete.

**Conditional writes need `EntryApplied`.** The cache is filled from the value
`ExtractKV` takes from the command, not from the store. For a compare-and-set,
put-if-absent or delete-if-match, the store may decline the value, and caching
it would let a later `Lookup` return a value the store never held. Only the
state machine knows its outcome encoding, so the predicate has to supply it.
`ResultInvalidCommand` is never cached.

Invalidation happens before the inner write, so a `Lookup` racing `Update` sees
a cache miss rather than the stale pre-write value. Cache hits return an owned
copy, so a caller that mutates the returned slice cannot corrupt the cache.

`NewCachedDiskStateMachine` returns `*CacheMaxEntriesError` or
`*InvalidCacheConfigError`. `BytesLookup` returns `*UnsupportedLookupResultError`
when the wrapped `Lookup` returns a type the byte path cannot represent, rather
than `(nil, nil)`, which would look like an empty value or a missing key.

## Event listener

Source: `pkg/config/hostconfig.go`.

```go
type EventListener struct { /* 26 optional func fields */ }
```

Set only the callbacks you need; nil callbacks are not invoked. Engine events
(leader updates, leader loss, campaign launches, shard failure, log compaction,
snapshot creation, snapshot rejection) are delivered in FIFO order on one
dispatcher goroutine that holds no engine lock, so a handler may call any Host method, including
`StopShard`. Other callbacks run synchronously on the goroutine that produced
them. Every callback must return promptly.

Raft events:

| Callback | Payload | Fired when |
|---|---|---|
| `OnLeaderUpdated` | `LeaderInfo` | a leader change is detected |
| `OnLeaderLost` | `EventShardInfo` | the local replica lost leadership |
| `OnCampaignLaunched` | `EventShardInfo` | an election campaign started |
| `OnCampaignSkipped` | `EventShardInfo` | **never fires**; see below |
| `OnSnapshotRejected` | `SnapshotInfo` | a snapshot was rejected: a request refused or abandoned, a save that panicked, an inbound transfer refused, or a recovery that failed before the state machine's `RecoverFromSnapshot` was invoked (the engine re-drives it). `SnapshotInfo.Err` carries the cause when there is one |
| `OnProposalDropped` | `EventShardInfo` | a proposal was dropped |
| `OnReadIndexDropped` | `EventShardInfo` | a ReadIndex request was dropped |

> **`OnCampaignSkipped` never fires.** The field exists and is forwarded, but no
> engine code invokes it. Do not build a leaderless-shard alert on it: the
> conditions it would report are exactly those that leave a shard without a
> leader while quorum looks intact, and silence is indistinguishable from
> health. Use `GetLeaderID` or `ShardInfo.Health == ShardNoLeader`.

System events:

| Callback | Payload | Fired when |
|---|---|---|
| `OnNodeReady` | `EventShardInfo` | a shard is ready for traffic |
| `OnNodeUnloaded` | `EventShardInfo` | a shard was unloaded |
| `OnMembershipChanged` | `EventShardInfo` | a membership change was applied |
| `OnNodeDeleted` | `EventShardInfo` | a replica's data was removed |
| `OnNodeHostShuttingDown` | none | host shutdown started |
| `OnConnectionEstablished` | `ConnectionInfo` | a QUIC connection to a peer was established |
| `OnConnectionFailed` | `ConnectionInfo` | a QUIC connection attempt failed |
| `OnSnapshotSendStarted` | `SnapshotInfo` | a snapshot transfer started |
| `OnSnapshotSent` | `SnapshotInfo` | a snapshot transfer was sent |
| `OnSnapshotSendAborted` | `SnapshotInfo` | a snapshot transfer was abandoned |
| `OnSnapshotReceived` | `SnapshotInfo` | a snapshot transfer was received |
| `OnSnapshotRecovered` | `SnapshotInfo` | the state machine was restored from a snapshot |
| `OnSnapshotCreated` | `SnapshotInfo` | an on-demand or automatic snapshot was written. A save with nothing new to capture writes none and fires nothing |
| `OnSnapshotCompacted` | `SnapshotInfo` | old snapshot directories were reclaimed |
| `OnLogCompacted` | `EventShardInfo` | log compaction completed; `Index` is the compaction point |
| `OnLogDBCompacted` | `EventShardInfo` | LogDB compaction completed; `Index` is the compaction point |

Failure events:

| Callback | Payload | Fired when |
|---|---|---|
| `OnShardFailed` | `ShardFailedInfo` | the shard's failure latch tripped, once per shard incarnation, reporting the failure that tripped it; failures that race it are not reported again. Causes: SM error or panic, commit or save-state failure, a step-worker `GetUpdate` error, a snapshot recovery that failed or panicked once the state machine's `RecoverFromSnapshot` was invoked, a recovery whose every re-driven attempt was rejected before the restore (`*SnapshotRecoveryExhaustedError`), a corrupt committed config entry. The shard is offline until reloaded. Not fired for a failed snapshot save, which leaves the shard running (it is logged and returned to the `RequestSnapshot` caller), for a recovery that failed before `RecoverFromSnapshot` was invoked while attempts remain (rejected through `OnSnapshotRejected` and re-driven), nor when the engine declines a stale snapshot recovery, since the shard is already past that snapshot |
| `OnLogDBDiskWarning` | `DiskInfo` | WAL usage crossed the warning threshold |
| `OnLogDBDiskFull` | `DiskInfo` | WAL usage crossed the full threshold |

Payload types:

```go
type LeaderInfo struct {
    ShardID, ReplicaID, Term, LeaderID uint64
}

type EventShardInfo struct {
    ShardID, ReplicaID, LeaderID, Term uint64
    Index uint64 // compaction index for OnLogCompacted / OnLogDBCompacted; 0 otherwise
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

`LeaderInfo` and `EventShardInfo` declare the same names in a different order, so
construct them with field names.

`ShardFailedInfo.Err` is one of `*ApplyError`, `*SMPanicError` (matches
`ErrShardFailed`), `*SaveStateError`, `*CommitError`, `*WorkerPanicError`,
`*SnapshotError`, `*SnapshotRecoveryExhaustedError` or `*CorruptConfigEntryError`;
match it with `errors.As`. Their causes are listed under [Errors](#errors). A
step-worker `GetUpdate` failure delivers the Raft log's own error, such as
`logdb.ErrCompacted` or `logdb.ErrUnavailable`; match it with `errors.Is`.

## Information types

### ShardHealth

```go
type ShardHealth uint8
```

`String()` and `MarshalJSON` render the lowercase name below. `UnmarshalJSON`
accepts those names and rejects anything else with `*InvalidShardHealthError`,
leaving the receiver unchanged: decoding a typo or an unknown state to
`ShardHealthy` would report a shard healthy on bad input.

| Constant | Name | Meaning |
|---|---|---|
| `ShardHealthy` | `healthy` | known leader, apply pipeline running. The zero value |
| `ShardNoLeader` | `no-leader` | no leader known: an election is running, or a partition |
| `ShardLagging` | `lagging` | apply index well behind commit index |
| `ShardUnloaded` | `unloaded` | the shard is not loaded on this host |
| `ShardError` | `error` | fatal state machine or LogDB failure |
| `ShardRecovering` | `recovering` | `RecoverFromSnapshot` in progress: the replica votes and replicates but applies nothing and serves no current local read |
| `ShardSnapshotting` | `snapshotting` | snapshot capture in progress; applies are quiesced |

`ShardHealthy` is the zero value, so a `ShardInfo` you never filled reports
healthy. Check `ShardUnloaded` explicitly.

### ShardInfo

```go
type ShardInfo struct {
    ShardID              uint64            `json:"shard_id"`
    ReplicaID            uint64            `json:"replica_id"`
    LeaderID             uint64            `json:"leader_id"`
    Term                 uint64            `json:"term"`
    IsLeader             bool              `json:"is_leader"`
    IsObserver           bool              `json:"is_observer"`
    IsWitness            bool              `json:"is_witness"`
    StateMachineType     uint64            `json:"state_machine_type"`
    StateMachineTypeName string            `json:"state_machine_type_name"`
    Pending              uint64            `json:"pending"`
    Nodes                map[uint64]string `json:"nodes"`
    Health               ShardHealth       `json:"health"`
    LastApplied          uint64            `json:"last_applied"`
    ConfigChangeIndex    uint64            `json:"config_change_index"`
    Replicas             []ReplicaProgress `json:"replicas,omitempty"`
}
```

A point-in-time copy returned by `GetShardInfo`. `StateMachineType` is 0
regular, 1 concurrent, 2 on-disk. `IsObserver` is the effective role from the
applied membership (a promoted observer reports `false` without a restart);
`IsWitness` is the configured witness role. `Nodes` covers all known members;
use `SyncGetShardMembership` for observers and witnesses separately.

`Replicas` is empty on a follower, which tracks no such state, and on the first
call for a shard, because it is published on the next tick.

### ReplicaProgress

```go
type ReplicaProgress struct {
    ReplicaID  uint64 `json:"replica_id"`
    Match      uint64 `json:"match"`
    Next       uint64 `json:"next"`
    State      string `json:"state"` // "probe", "replicate", "snapshot"
    Paused     bool   `json:"paused"`
    Active     bool   `json:"active"`
    Inflights  int    `json:"inflights"`
    IsWitness  bool   `json:"is_witness"`
    IsObserver bool   `json:"is_observer"`
}
```

One follower's replication position as the leader sees it. A follower stuck in
`probe` with a stale `Match` is a different problem from one that is merely
behind, and one pinned at the inflight limit is flow-controlled rather than
slow. Check `Active` before taking another replica down: an inactive follower
(no response within the election timeout) counts in the configuration but not
toward a live quorum.

### NodeHostInfo, LogRange, ReadonlyLogReader

```go
type NodeHostInfoOption struct {
    SkipLogInfo bool
}

type NodeHostInfo struct {
    RaftAddress   string           `json:"raft_address"`
    ShardInfoList []ShardInfo      `json:"shard_info_list"`
    LogInfo       []logdb.NodeInfo `json:"log_info,omitempty"` // nil with SkipLogInfo
}

type LogRange struct {
    FirstIndex uint64 // inclusive
    LastIndex  uint64 // inclusive
}

type ReadonlyLogReader interface {
    GetRange() (firstIndex uint64, lastIndex uint64)
    NodeState() (lastIndex uint64, term uint64)
}
```

## Usage patterns

### Start a shard and propose

```go
host, err := quicraft.NewHost(hostCfg, quicraft.WithBarrier(barrier))
if err != nil {
    return err
}
defer host.Close()

create := sm.NewCreateFunc(func(shardID, replicaID uint64) sm.StateMachine {
    return kv.NewMemoryStore()
})

if err := host.StartShard(members, false, create, shardCfg); err != nil {
    return err
}

result, err := host.SyncPropose(ctx, shardCfg.ShardID, cmd)
if err != nil {
    var nle *quicraft.NotLeaderError
    if errors.As(err, &nle) && nle.LeaderAddress != "" {
        // redirect to nle.LeaderAddress
    }
    return err
}
_ = result // result.Value, result.Data
```

### Asynchronous propose

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
    // use result.Value, result.Data
case <-ctx.Done():
    return ctx.Err()
}
```

### Linearizable read

```go
value, err := host.SyncRead(ctx, shardID, query)
if err != nil {
    return err
}
```

### At-most-once propose with a session

```go
session, err := host.GetNewSession(ctx, shardID)
if err != nil {
    return err
}
defer host.CloseSession(ctx, shardID, session)

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
_ = result.Value
```

`SyncProposeWithSession` does the same in one call and advances the session.

### Membership change

Read the membership first so the change carries a `configChangeIndex` and a
concurrent change is rejected rather than merged blindly.

```go
mship, err := host.SyncGetShardMembership(ctx, shardID)
if err != nil {
    return err
}

err = host.SyncRequestAddNodeWithIdentity(ctx, shardID, newReplicaID,
    "node4.example.com:63100", newNodeIdentity, mship.ConfigChangeID)
if errors.Is(err, quicraft.ErrStaleConfigChange) {
    // the membership changed underneath; re-read and rebuild
}
return err
```

Pass 0 as the last argument to skip the guard.

### Graceful shutdown

```go
// Optional: Close drains first anyway.
if err := host.Drain(); err != nil {
    logger.Warn("drain incomplete", "error", err)
}
if err := host.Close(); err != nil {
    logger.Error("close failed", "error", err)
}
```

### Health endpoint

```go
status := host.HealthCheck()
if status.Status != "ok" {
    // respond 503
}
```

### Prometheus metrics

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

### Snapshot export and import

```go
// Source host: the shard may be running.
f, err := os.Create("/backup/shard-7.snap")
if err != nil {
    return err
}
if err := host.ExportSnapshot(ctx, 7, f); err != nil {
    f.Close()
    os.Remove("/backup/shard-7.snap") // a failed export is not an artifact
    return err
}
if err := f.Close(); err != nil {
    return err
}

// Destination host: the shard must be stopped.
r, err := os.Open("/backup/shard-7.snap")
if err != nil {
    return err
}
defer r.Close()
if err := dest.ImportSnapshot(ctx, 7, r); err != nil {
    return err
}
// The next StartShard recovers from the imported snapshot.
```

## Cross-references

- [Architecture](architecture.md): system design, data flow, package structure
- [Lifecycle](engineering.md): start, join, drain, shutdown
- [Usage](user.md): deployment, metrics cardinality, alerting, sizing
- [Raft protocol](../raft-consensus/engineering.md): election and log safety,
  config-in-log, the engine pipeline
- [State machines](../state-machines/engineering.md): the three tiers, the RSM
  adapter, the `pkg/sm/kv` stores
- [Client sessions](../client-sessions/engineering.md): at-most-once semantics
  and index-based expiry
- [QUIC transport](../quic-transport/engineering.md): connections and frames
- [Wire protocol](../wire-protocol/engineering.md): the layouts behind `proto`
- [Log storage](../log-storage/engineering.md): LogDB and the WAL
- [Seal barrier](../seal-barrier/architecture.md) and
  [key custody](../seal-barrier/user.md)
- [Compatibility](../compatibility.md): on-disk and wire format compatibility
- [Testing](../testing/overview.md): test infrastructure and fault injection
