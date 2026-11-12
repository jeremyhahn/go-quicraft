# Write Batching — API

> **Status: NORMATIVE** · Scope: go-quicraft `pkg/batch`, `pkg/writemode` ·
> Last verified: 2026-11-12
> Conflicts with this document are defects. Fix them here first.

## Transport coverage

| Transport | This feature |
|---|---|
| Embedded (Go call) | the surface below |
| QUIC | a flushed batch is one ordinary Raft entry; nothing on the wire is batch-specific |
| Unix socket, REST, gRPC, MCP | not applicable: this repository serves none of them |

## `pkg/batch`

Import: `github.com/jeremyhahn/go-quicraft/pkg/batch`

### Construct an aggregator

```go
func NewAggregator(proposer Proposer, opts ...Option) (*Aggregator, error)

type Option func(*Aggregator)
func WithConfig(cfg Config) Option                 // zero fields keep their defaults
func WithLogger(logger *slog.Logger) Option        // nil keeps slog.Default()
func WithRegistry(reg prometheus.Registerer) Option // nil keeps prometheus.DefaultRegisterer

type Proposer interface {
    SyncPropose(ctx context.Context, shardID uint64, cmd []byte) error
}
type SyncProposer interface {
    SyncPropose(ctx context.Context, shardID uint64, cmd []byte) (sm.Result, error)
}
type HostProposer struct{ Host SyncProposer }
```

| | |
|---|---|
| Request | a `Proposer` and options |
| Response | a running `*Aggregator` with its own flush goroutine |
| Errors | `*NilProposerError` when `proposer` is nil |

`*Host` does not satisfy `Proposer`; wrap it:

```go
agg, err := batch.NewAggregator(batch.HostProposer{Host: host})
```

`HostProposer` discards the state machine's `sm.Result`; a batch has no
per-command result.

### Submit a command

```go
func (a *Aggregator) Submit(ctx context.Context, shardID uint64, data []byte) error
func (a *Aggregator) SubmitNoCopy(ctx context.Context, shardID uint64, data []byte) error
```

| | |
|---|---|
| Request | shard and command bytes |
| Response | nil once the batch carrying the command has been proposed and applied |
| Errors | `*ClosedError` (matches `ErrClosed`), `*QueueFullError` (matches `ErrQueueFull`), `*FlushError` (matches `ErrFlush`) |
| Concurrency | safe from multiple goroutines |

`*FlushError{ShardID, Err, Indeterminate}` wraps the cause (`errors.Is` and
`errors.As` reach it):

| Cause | `Indeterminate` | Command committed? |
|---|---|---|
| your `ctx` ended while waiting | false if the flush had already dropped the command before encoding its batch; true otherwise | no when false; when true, no if its batch had not been encoded yet (the command is dropped), otherwise unknown |
| the propose timed out or was cancelled | true | unknown |
| the proposer returned another error (for example `*NotLeaderError`, `*EntryTooLargeError`) | false | no |
| the command cannot be encoded (`*EncodeError`, length above `math.MaxInt32`) | false | no |

Retrying an indeterminate failure can apply the whole batch twice: delivery is
at-least-once. In the rare case that `Close` races your call and your
context ends while you wait for the flush goroutine to finish, the bare
`ctx.Err()` is returned instead of a `*FlushError`; the command was enqueued,
so its outcome is unknown, as for an indeterminate failure.

`Submit` copies `data`. `SubmitNoCopy` does not and takes ownership of the
slice permanently, whatever it returns: the aggregator may read it later on the
flush goroutine. Use it only for a buffer allocated per call and never touched
again.

### Inspect and close

```go
func (a *Aggregator) BatchLen() int // proposals in the current unflushed batch
func (a *Aggregator) Close() error  // always nil
```

`Close` stops accepting proposals, drains the queue, flushes what is pending
and waits for the flush goroutine. Later `Submit` calls return `*ClosedError`.
Calling it again is a no-op.

### Encode and decode a payload

```go
func EncodeBatch(cmds [][]byte) ([]byte, error)
func DecodeBatch(data []byte) ([][]byte, error)
```

Each command is prefixed with its length as a 4-byte big-endian `uint32`.

- `EncodeBatch` of an empty slice returns zero-length output. A command longer
  than `math.MaxInt32` returns `*EncodeError`.
- `DecodeBatch` of empty or nil input returns `nil, nil`. A truncated length
  prefix, a length above `math.MaxInt32`, or a command running past the buffer
  returns `*DecodeError`.
- The decoded commands alias `data`. Copy them if you keep them past `Update`
  under `ZeroCopyEntryCmd`.

Your state machine calls `DecodeBatch`; an aggregator is useless unless
`Update` knows the entry is a batch.

### Configuration

```go
type Config struct {
    MaxBatchSize   int           // DefaultMaxBatchSize   = 1000
    MaxBatchBytes  uint64        // DefaultMaxBatchBytes  = uint64(proto.MaxEntrySize) (8 MiB)
    FlushInterval  time.Duration // DefaultFlushInterval  = 50 * time.Millisecond
    QueueSize      int           // DefaultQueueSize      = 10000
    ProposeTimeout time.Duration // DefaultProposeTimeout = 30 * time.Second
}
func DefaultConfig() Config
```

`MaxBatchBytes` applies to the whole accumulated batch, enforced by flushing
before a proposal would exceed it. `ProposeTimeout` is a ceiling: each batch is
proposed under `min(latest live caller deadline, now + ProposeTimeout)`.

### Errors

```go
var (
    ErrClosed      // "quicraft/batch: aggregator closed"
    ErrQueueFull   // "quicraft/batch: queue full"
    ErrNilProposer // "quicraft/batch: proposer is nil"
    ErrFlush       // "quicraft/batch: flush failed"
    ErrDecode      // "quicraft/batch: decode failed"
    ErrEncode      // "quicraft/batch: encode failed"
)

type ClosedError      struct{}
type QueueFullError   struct{ QueueSize int }
type NilProposerError struct{}
type FlushError       struct{ ShardID uint64; Err error; Indeterminate bool } // Unwrap
type DecodeError      struct{ Reason string }
type EncodeError      struct{ Reason string }
```

Each typed error implements `Is` against its sentinel, so `errors.Is(err,
batch.ErrQueueFull)` matches a `*QueueFullError`.

## `pkg/writemode`

Import: `github.com/jeremyhahn/go-quicraft/pkg/writemode`

```go
type Mode uint8
const (
    Sync       Mode = iota // acknowledged after quorum commit
    WALDurable             // acknowledged after local WAL persistence
    Batched                // acknowledged after quorum commit of the batch
)

func FromString(s string) (Mode, error)
func (m Mode) String() string // "sync", "wal_durable", "batched", or "unknown(N)"
func (m Mode) IsSynchronous() bool  // Sync, Batched
func (m Mode) IsAsync() bool        // WALDurable
func (m Mode) RequiresQuorum() bool // Sync, Batched
func (m Mode) Validate() error

type InvalidModeError struct{ Value string }
```

`FromString` accepts `""` and `"sync"` (Sync), `"wal_durable"`, `"wal"` and
`"async"` (WALDurable), and `"batched"` and `"batch"` (Batched). Any other
string returns `Sync` with `*InvalidModeError`; it is case-sensitive.

`Mode` is an open `uint8`. A value outside the three constants describes no
durability guarantee, and all three predicates report false for it. A caller
accepting a `Mode` from outside its own code must call `Validate` at that
boundary:

```go
if err := m.Validate(); err != nil {
    return err
}
```

`writemode` implements nothing; the modes are realized by which `Host` method
the caller uses, or by the aggregator.

## Versioning

- The batch payload format is application-level, not part of `pkg/proto`. It is
  a contract between your aggregator and your state machine, carries no version,
  and a layout change is yours to coordinate.
- Nothing here is deprecated.
