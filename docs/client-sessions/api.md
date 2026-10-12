# Client Sessions — API

> **Status: NORMATIVE** · Scope: go-quicraft `pkg/session.go` and the `Host`
> session methods · Last verified: 2026-10-11
> Conflicts with this document are defects. Fix them here first.

## Transport coverage

| Transport | This feature |
|---|---|
| Embedded (Go call) | the surface below |
| QUIC | the session fields ride in every `proto.Entry` (`ClientID`, `SeriesID`, `RespondedTo`, `OwnerID`) and the policy in `proto.Membership`; there is no session RPC |
| Unix socket, REST, gRPC, MCP | not applicable: this repository serves none of them |

## Go API

Import: `github.com/jeremyhahn/go-quicraft/pkg` (package `quicraft`).

### Obtain a session

```go
func (h *Host) GetNewSession(ctx context.Context, shardID uint64) (*Session, error)
func (h *Host) GetNewSessionWithOwner(ctx context.Context, shardID, ownerID uint64) (*Session, error)
func (h *Host) RegisterSession(ctx context.Context, session *Session) (*Session, error)
```

| | |
|---|---|
| Request | the shard; for the owner form, the embedder's principal ID; for `RegisterSession`, a session built with `NewSession` or `NewSessionWithOwner` |
| Response | a registered `*Session`, ready for its first proposal |
| Errors | `ErrSessionsUnsupportedOnDisk`; `*SessionError` wrapping `ErrInvalidOperation` when the shard refused the registration (shard full, owner at its cap, or the client ID already registered); `*SessionError` wrapping `ErrInvalidSession` for a zero client ID (`RegisterSession`); `ErrInvalidSession` for a nil session; the proposal errors below; `ErrTimeout` or `ErrCanceled` |
| Permission | none; `ClientID` and `OwnerID` are not authenticated by this library |

`GetNewSession` draws the client ID from the system CSPRNG (never zero) and
registers it through the log, so it can fail like any proposal and blocks until
the registration is applied or `ctx` ends. `RegisterSession` registers a
caller-chosen client ID, which is how a client that persists its ID survives its
own restart.

### Propose under a session

```go
func (h *Host) ProposeWithSession(ctx context.Context, session *Session, cmd []byte) (*RequestState, error)
func (h *Host) SyncProposeWithSession(ctx context.Context, session *Session, cmd []byte) (sm.Result, error)
```

| | |
|---|---|
| Request | a registered session and the command |
| Response | a `*RequestState` (async) or the `sm.Result` (sync) |
| Errors at submit | `ErrSessionBusy` (another proposal is in flight on this session; `Temporary`), `ErrInvalidSession` (a no-op, unregistered or wrong-shard session), `ErrSessionsUnsupportedOnDisk`, `ErrInvalidOperation` (on a witness), `*EntryTooLargeError`, `ErrDiskFull`, `*NotLeaderError`, `ErrClosed`, `ErrDraining`, and the other proposal errors in [`../multi-group-host/api.md`](../multi-group-host/api.md) |
| Errors in the result | `*SessionExpiredError` (`errors.Is` matches `ErrSessionExpired`), or the proposal's own failure |
| Permission | none |

- A session serves one proposal at a time. `ProposeWithSession` reserves the
  session's current series for the returned `RequestState`; the reservation is
  released by `session.ProposalCompleted()`, by an error result, or by
  `RequestState.Release()`.
- After a successful `ProposeWithSession` result, call
  `session.ProposalCompleted()` to advance the series. `SyncProposeWithSession`
  does this for you.
- On error the session is unchanged, and retrying with the same session is safe:
  if the first attempt was applied, the retry returns the cached result and the
  state machine is not called again.
- Every failure delivered after the proposal was handed to Raft is wrapped in
  `*MaybeAppliedError`, so `MaybeApplied(err)` is true: a timeout or
  cancellation of the caller's wait, a leadership change, a stopped or halted
  shard (`ErrAborted`, `ErrClosed`, `ErrShardFailed`), and a persist, commit or
  apply failure. `errors.Is`, `errors.As`, `Temporary` and `NotLeader` still
  match the underlying error. A refusal before the hand-off (the submit errors
  above, a full proposal queue) is definite. The same classification applies to
  `GetNewSession`, `RegisterSession` and `CloseSession`, which are proposals.
- A no-op session is refused here with `ErrInvalidSession`. Propose without a
  session using `Propose` or `SyncPropose`.

### Close a session

```go
func (h *Host) CloseSession(ctx context.Context, shardID uint64, session *Session) (*RequestState, error)
```

Proposes the unregister entry and returns a `RequestState` that completes when
it is applied, releasing the session's cache and owner slot. Errors:
`ErrSessionsUnsupportedOnDisk`, `ErrInvalidOperation` for a nil session,
`ErrInvalidSession` for a no-op or wrong-shard session, and the proposal errors.
Unregistering a session the shard no longer has completes with
`*SessionExpiredError`.

A session left open is reclaimed by expiry, which is counted in applied entries:
an idle shard never expires anything.

### No-op sessions

```go
func (h *Host) GetNoOPSession(shardID uint64) *Session
func NewNoOPSession(shardID uint64) *Session
```

A no-op session has client ID 0, carries no deduplication and touches no log
entry. It mirrors dragonboat's no-op session. On `Host`,
`ProposeWithSession` and `CloseSession` reject it; propose idempotent commands
with `Propose`/`SyncPropose`.

### The `Session` type

```go
func NewSession(shardID, clientID uint64) *Session
func NewSessionWithOwner(shardID, clientID, ownerID uint64) *Session

func (s *Session) ShardID() uint64
func (s *Session) ClientID() uint64
func (s *Session) OwnerID() uint64
func (s *Session) SeriesID() uint64
func (s *Session) RespondedTo() uint64
func (s *Session) SetRespondedTo(seriesID uint64) // monotonic
func (s *Session) IsNoOPSession() bool
func (s *Session) ValidForProposal(shardID uint64) bool
func (s *Session) ValidForSessionOp(shardID uint64) bool

// Lifecycle transitions. The Host drives the first three; call
// ProposalCompleted yourself after an async ProposeWithSession succeeds.
func (s *Session) PrepareForRegister()
func (s *Session) PrepareForPropose()
func (s *Session) PrepareForUnregister()
func (s *Session) ProposalCompleted()
```

All fields are unexported. The series advances monotonically and the shard keys
its cache on `(clientID, seriesID)`, so a caller that rewound it would be served
another proposal's cached result.

`NewSession` and `NewSessionWithOwner` only construct the value;
`Host.RegisterSession` registers it. `ValidForProposal` is false for a no-op
session, a session on another shard, and a session still at series 0 (never
registered, or registered without `PrepareForPropose`).

### Constants

```go
// package quicraft
const SeriesIDForRegister   uint64 = math.MaxUint64 - 1
const SeriesIDForUnregister uint64 = math.MaxUint64
const SeriesIDFirstProposal uint64 = 1

// package proto (wire level)
const NotSessionManagedClientID    uint64 = 0
const SessionSeriesIDForRegister   uint64 = math.MaxUint64 - 1
const SessionSeriesIDForUnregister uint64 = math.MaxUint64
```

`Entry.OwnerID` is 0 when unset, and an unset owner is never counted against
`MaxSessionsPerOwner`.

### Errors

| Error | Meaning |
|---|---|
| `ErrSessionsUnsupportedOnDisk` | the shard runs a `DiskStateMachine`; sessions are refused at every entry point |
| `ErrSessionBusy` | the session already has a proposal in flight (`Temporary`) |
| `ErrInvalidSession` | the session cannot be used for this call |
| `*SessionError{ClientID, Op, Err}` | registration failed; `Err` says why |
| `*SessionExpiredError{ShardID, ClientID}` | the shard had no session for this proposal (expired, closed, never registered, or a stale incarnation), or the response was already acknowledged and evicted; matches `ErrSessionExpired` |

## Configuration

Per shard on `config.Config`. All four are bootstrap seeds; the live policy is
replicated in `proto.Membership` and fixed for the life of the shard.

| Field | Default | Bounds |
|---|---|---|
| `MaxSessionsPerClient` | 128 | cached responses per session. Misnamed and kept for compatibility; read it as "max responses per session". |
| `MaxTotalSessions` | 16384 | sessions on the shard |
| `MaxSessionsPerOwner` | 128 | concurrent sessions per declared `OwnerID`; ownerless sessions are exempt. Zero selects the default; `config.SessionsPerOwnerUnlimited` disables the cap. |
| `SessionExpiryEntries` | 100000 | inactivity threshold in applied entries. Zero selects the default; `config.SessionExpiryDisabled` disables expiry; values below 100 are rejected. |

## Versioning

- The session fields are part of `proto.Entry`'s 68-byte fixed header and the
  policy is part of `proto.Membership`. Both formats are pinned for the
  pre-release period; see [`../wire-protocol/api.md`](../wire-protocol/api.md)
  and [`../compatibility/`](../compatibility/architecture.md).
- `MaxSessionsPerClient` is misnamed, not deprecated. Renaming a public field
  breaks callers, so the name stays and the meaning is documented.
