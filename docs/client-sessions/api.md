# Client Sessions — API

> **Status: NORMATIVE** · Scope: go-quicraft `pkg/session.go` and the `Host`
> session methods · Last verified: 2026-08-19
> Conflicts with this document are defects. Fix them here first.

## Transport coverage

go-quicraft is a library. **The Go API is the API.**

| Transport | This feature |
|---|---|
| **Embedded (Go call)** | the surface below. This is the transport. |
| QUIC | the session **fields** ride in every `proto.Entry` on the wire (`ClientID`, `SeriesID`, `RespondedTo`, `OwnerID`) and the policy rides in `proto.Membership`. There is no session RPC. |
| Unix socket / REST / gRPC / MCP | n/a — this repository serves none of them |

Not missing-capability rows. This feature is part of the go-quicraft Raft row,
which the portfolio register marks as consumed by implementing interfaces rather
than by calling a client.

## Go API

Import: `github.com/jeremyhahn/go-quicraft/pkg`

### Capability: obtain a session

```go
func (h *Host) GetNewSession(ctx context.Context, shardID uint64) (*Session, error)
func (h *Host) GetNewSessionWithOwner(ctx context.Context, shardID, ownerID uint64) (*Session, error)
func (h *Host) RegisterSession(ctx context.Context, session *Session) (*Session, error)
func (h *Host) GetNoOPSession(shardID uint64) *Session
```

| | |
|---|---|
| Request | the shard, and for the owner form the embedder's principal id |
| Response | a registered `*Session` |
| Errors | `ErrSessionsUnsupportedOnDisk` on a `DiskStateMachine` shard; `*SessionError`; the per-owner and per-shard caps; `NotLeaderError`; context cancellation |
| Permission | none in this library — `ClientID` and `OwnerID` are **caller-supplied and unauthenticated** |

`GetNewSession` registers **through the log**, so it is a consensus operation
and can fail the way any proposal can.

`GetNoOPSession` returns a session that carries no deduplication at all. It is
the explicit way to say "this proposal is idempotent, do not pay for a session";
it does not touch the log.

### Capability: propose under a session

```go
func (h *Host) ProposeWithSession(ctx context.Context, session *Session, cmd []byte) (*RequestState, error)
func (h *Host) SyncProposeWithSession(ctx context.Context, session *Session, cmd []byte) (sm.Result, error)
```

| | |
|---|---|
| Request | the session and the command |
| Response | a `*RequestState` (async) or an `sm.Result` (sync) |
| Errors | `*SessionExpiredError`, `*SessionError`, `NotLeaderError`, `ErrBusy`, `*EntryTooLargeError`, `ErrDiskFull`, `MaybeAppliedError` |
| Permission | none |

A retry of a series already applied returns the **cached result** and the state
machine is never called again. From the caller's side that is indistinguishable
from the first call.

### Capability: close a session

```go
func (h *Host) CloseSession(ctx context.Context, shardID uint64, session *Session) (*RequestState, error)
```

Unregisters through the log, releasing its cache entries. A session left open
until expiry is not an error, but expiry is measured in **applied entries**, so
an idle shard never expires anything.

### The `Session` type

```go
func NewSession(shardID, clientID uint64) *Session
func NewSessionWithOwner(shardID, clientID, ownerID uint64) *Session
func NewNoOPSession(shardID uint64) *Session

func (s *Session) ShardID() uint64
func (s *Session) ClientID() uint64
func (s *Session) OwnerID() uint64
func (s *Session) SeriesID() uint64
func (s *Session) RespondedTo() uint64
func (s *Session) SetRespondedTo(seriesID uint64)
func (s *Session) IsNoOPSession() bool
func (s *Session) ValidForProposal(shardID uint64) bool

// Lifecycle transitions, driven by the Host. Call these yourself only if you
// are driving the protocol directly.
func (s *Session) PrepareForRegister()
func (s *Session) PrepareForPropose()
func (s *Session) PrepareForUnregister()
func (s *Session) ProposalCompleted()
```

**Every field is unexported, deliberately.** Direct mutation would break the
deduplication invariants — the `seriesID` advances monotonically and the
manager keys on `(clientID, seriesID)`, so a caller that rewound it would be
served another proposal's cached result.

`NewSession` and `NewSessionWithOwner` construct the value; they do not register
it. `Host.RegisterSession` does that.

### Wire-level constants

```go
const proto.NotSessionManagedClientID    uint64 = 0
const proto.SessionSeriesIDForRegister   uint64 = math.MaxUint64 - 1
const proto.SessionSeriesIDForUnregister uint64 = math.MaxUint64
const quicraft.SeriesIDForRegister       uint64 = math.MaxUint64 - 1
```

`Entry.OwnerID` is `0` when unset, and an unset owner is never counted against
`MaxSessionsPerOwner`.

### Errors

| Error | Meaning |
|---|---|
| `ErrSessionsUnsupportedOnDisk` | client sessions are not supported on on-disk state machines |
| `*SessionError` | registration or lookup failed |
| `*SessionExpiredError` | the session expired; `sm.ResultSessionExpired` is the reserved `Result.Value` the RSM layer signals it with |

## Configuration

Per shard, on `config.Config`. All of it is **bootstrap seed only** — the live
policy is replicated in `proto.Membership`.

| Field | Default | Bounds |
|---|---|---|
| `MaxSessionsPerClient` | 128 | **cached responses per session.** The name is wrong and kept for compatibility; read it as `MaxResponsesPerSession`. It bounds nothing about how many sessions exist. |
| `MaxTotalSessions` | 16384 | total sessions on the shard |
| `MaxSessionsPerOwner` | 128 | concurrent sessions one declared `OwnerID` may hold; unset owners are exempt |
| `SessionExpiryEntries` | 100000 | inactivity threshold in **applied log entries**, so it is deterministic across replicas. Zero selects the default; assign `config.SessionExpiryDisabled` to turn it off. |

## Versioning and deprecation

- The session fields are part of `proto.Entry`'s 68-byte fixed header and the
  replicated policy is part of `proto.Membership`; both follow the repository's
  pre-release pinning rule — see
  [`../wire-protocol/api.md`](../wire-protocol/api.md).
- `MaxSessionsPerClient` is **misnamed, not deprecated.** Renaming a public
  field breaks compatibility, so the name stays and the meaning is documented.
- `OwnerID` and `MaxSessionsPerOwner` were added after `Entry`'s header grew
  from 60 to 68 bytes. A reader from before that change reads `OwnerID`'s low
  four bytes as `CmdLen`; the two releases share no on-disk data. See
  [`../compatibility.md`](../compatibility.md).
