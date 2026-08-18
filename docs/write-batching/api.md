# Write Batching — API

> **Status: NORMATIVE** · Scope: go-quicraft `pkg/batch`, `pkg/writemode` ·
> Last verified: 2026-08-19
> Conflicts with this document are defects. Fix them here first.

## Transport coverage

go-quicraft is a library. **The Go API is the API.**

| Transport | This feature |
|---|---|
| **Embedded (Go call)** | the surface below. This is the transport. |
| QUIC | a flushed batch is one ordinary Raft entry; there is nothing batch-specific on the wire |
| Unix socket / REST / gRPC / MCP | n/a — this repository serves none of them |

Not missing-capability rows. This feature is an optional in-process component
above the `Host`, part of the go-quicraft Raft row.

## Go API — `pkg/batch`

Import: `github.com/jeremyhahn/go-quicraft/pkg/batch`

### Capability: construct an aggregator

```go
func NewAggregator(proposer Proposer, opts ...Option) (*Aggregator, error)

func WithConfig(cfg Config) Option
func WithLogger(logger *slog.Logger) Option
func WithRegistry(reg prometheus.Registerer) Option
```

| | |
|---|---|
| Request | a `Proposer` and options |
| Response | a running `*Aggregator` with its own background flush loop |
| Errors | `*NilProposerError` when `proposer` is nil |
| Permission | none — in-process call |

**`*Host` does not satisfy `Proposer`.** Use the adapter:

```go
agg, err := batch.NewAggregator(&batch.HostProposer{Host: host})
```

`Proposer.SyncPropose` returns `error` alone while `Host.SyncPropose` returns
`(sm.Result, error)`. `HostProposer` exists because the documented construction
`batch.NewAggregator(host, ...)` did not compile.

### Capability: submit a command

```go
func (a *Aggregator) Submit(ctx context.Context, shardID uint64, data []byte) error
func (a *Aggregator) SubmitNoCopy(ctx context.Context, shardID uint64, data []byte) error
```

| | |
|---|---|
| Request | shard and command bytes |
| Response | nil once the batch containing it commits |
| Errors | `ErrClosed`, `*QueueFullError`, `*FlushError`, `*EncodeError`, context deadline |
| Permission | none |
| Concurrency | safe from multiple goroutines |

**`Submit` blocks until the batch commits, but can return before that** — its
own context expiring is the common case. **Delivery is at-least-once.**

**`SubmitNoCopy` transfers ownership of `data`.** The aggregator reads the slice
at an unspecified later time — after your call has returned, on the flush
goroutine. Mutating or reusing that buffer commits a **wrong command on every
replica**. Use it only for a buffer allocated per call and then dropped; when in
doubt use `Submit`.

### Capability: inspect and close

```go
func (a *Aggregator) BatchLen() int
func (a *Aggregator) Close() error
```

`Close` stops accepting proposals, flushes what is pending, and makes every
subsequent `Submit` return `ErrClosed`. It is one-way.

### Capability: encode and decode a batch payload

```go
func EncodeBatch(cmds [][]byte) ([]byte, error)
func DecodeBatch(data []byte) ([][]byte, error)
```

Format: each command prefixed with its length as a **4-byte big-endian
uint32**.

`EncodeBatch` on an empty slice produces zero-length output. `DecodeBatch` on
empty or nil input returns a nil slice with **no error**; on a truncated length
prefix or a command length past the buffer it returns `*DecodeError`.

**Your state machine calls `DecodeBatch`.** That is the other half of the
contract: an aggregator on the write side is useless unless `Update` knows the
entry it receives is a batch.

### Configuration

```go
type Config struct {
    MaxBatchSize   int            // DefaultMaxBatchSize   = 1000
    MaxBatchBytes  uint64         // DefaultMaxBatchBytes  = proto.MaxEntrySize (8 MB)
    FlushInterval  time.Duration  // DefaultFlushInterval  = 50ms
    QueueSize      int            // DefaultQueueSize      = 10000
    ProposeTimeout time.Duration  // DefaultProposeTimeout = 30s
}
func DefaultConfig() Config
```

`MaxBatchBytes` applies to the **whole** accumulated batch, not per shard, and
is enforced by flushing first rather than by exceeding. `ProposeTimeout` is a
**ceiling**: a batch goes out under `min(latest live caller deadline,
ProposeTimeout)`.

### Errors

```go
var ErrClosed = errors.New("quicraft/batch: aggregator closed")

type ClosedError      struct{}   // Is(ErrClosed) == true
type NilProposerError struct{}
type QueueFullError   struct{ ... }
type FlushError       struct{ ... }
type EncodeError      struct{ ... }
type DecodeError      struct{ ... }
```

Sentinels are pre-allocated for zero-alloc hot-path returns and implement `Is`,
so `errors.Is` works against both forms.

## Go API — `pkg/writemode`

Import: `github.com/jeremyhahn/go-quicraft/pkg/writemode`

```go
type Mode uint8
const Sync       Mode = iota   // wait for quorum commit — strongest
const WALDurable               // wait for local WAL persistence — lower latency
const Batched                  // collect and propose as a single Raft entry

func FromString(s string) (Mode, error)
func (m Mode) String() string
func (m Mode) IsSynchronous() bool
func (m Mode) IsAsync() bool
func (m Mode) RequiresQuorum() bool
func (m Mode) Validate() error

type InvalidModeError struct{ ... }
```

**`Mode` is an open `uint8`.** A value outside the three constants is
representable — a field decoded from configuration or a wire format is the
ordinary way one arrives — and it describes **no durability guarantee**:
`String` reports `"unknown(N)"`, and `IsSynchronous`, `IsAsync` and
`RequiresQuorum` all report **false**.

An embedder dispatching on `RequiresQuorum` therefore takes the "quorum not
required" branch for a `Mode` that means nothing at all. The predicates cannot
report an error, so:

```go
if err := m.Validate(); err != nil {   // do this at the boundary
    return err
}
```

**A caller accepting a `Mode` from outside its own code MUST call `Validate` at
that boundary.** `TestMode_UnknownValueGuaranteesNothing` pins the predicate
behavior so it cannot change silently.

`writemode` implements nothing. The modes are realized by which `Host` method
the caller chooses, or by using the aggregator.

## Versioning and deprecation

- The batch payload format (4-byte big-endian length prefixes) is **application
  level**, not part of `pkg/proto`. It is the contract between your aggregator
  and your state machine; this library does not stamp a version into it, and a
  layout change is yours to coordinate.
- `HostProposer` is a correction, not a deprecation — `batch.NewAggregator(host)`
  never compiled.
- Nothing here is deprecated.
