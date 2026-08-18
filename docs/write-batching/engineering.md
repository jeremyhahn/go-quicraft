# Write Batching — Engineering Contract

> **Status: NORMATIVE** · Scope: go-quicraft `pkg/batch`, `pkg/writemode` ·
> Last verified: 2026-08-19
> Conflicts with this document are defects. Fix them here first.

Import paths:
`github.com/jeremyhahn/go-quicraft/pkg/batch`
`github.com/jeremyhahn/go-quicraft/pkg/writemode`

Design rationale and the scope fence are in
[`architecture.md`](architecture.md).

## 1. Exported surface

### `pkg/batch`

```go
func NewAggregator(proposer Proposer, opts ...Option) (*Aggregator, error)
func (a *Aggregator) Submit(ctx context.Context, shardID uint64, data []byte) error
func (a *Aggregator) SubmitNoCopy(ctx context.Context, shardID uint64, data []byte) error
func (a *Aggregator) BatchLen() int
func (a *Aggregator) Close() error

type Option func(*Aggregator)
func WithConfig(cfg Config) Option
func WithLogger(logger *slog.Logger) Option
func WithRegistry(reg prometheus.Registerer) Option

type Config struct {
    MaxBatchSize   int
    MaxBatchBytes  uint64
    FlushInterval  time.Duration
    QueueSize      int
    ProposeTimeout time.Duration
}
func DefaultConfig() Config

type Proposer interface {
    SyncPropose(ctx context.Context, shardID uint64, cmd []byte) error
}
type SyncProposer interface{ /* what *Host actually provides */ }
type HostProposer struct{ Host SyncProposer }

func EncodeBatch(cmds [][]byte) ([]byte, error)
func DecodeBatch(data []byte) ([][]byte, error)

const DefaultMaxBatchSize          = 1000
const DefaultFlushInterval         = 50 * time.Millisecond
const DefaultQueueSize             = 10000
const DefaultProposeTimeout        = 30 * time.Second
var   DefaultMaxBatchBytes  uint64 = uint64(proto.MaxEntrySize)   // 8 MB

var ErrClosed = errors.New("quicraft/batch: aggregator closed")

type ClosedError    struct{}
type NilProposerError struct{}
type QueueFullError struct{ ... }
type FlushError     struct{ ... }
type EncodeError    struct{ ... }
type DecodeError    struct{ ... }
```

### `pkg/writemode`

```go
type Mode uint8
const Sync       Mode = iota   // wait for quorum commit
const WALDurable               // wait for local WAL persistence
const Batched                  // collect and propose as one entry

func FromString(s string) (Mode, error)
func (m Mode) String() string
func (m Mode) IsSynchronous() bool
func (m Mode) IsAsync() bool
func (m Mode) RequiresQuorum() bool
func (m Mode) Validate() error

type InvalidModeError struct{ ... }
```

## 2. Invariants

**Delivery is AT-LEAST-ONCE.** The aggregator proposes a raw encoded batch and
may propose it more than once. `Submit` blocks until the batch commits, but can
return before that — its own context expiring is the common case.

**Flush triggers, whichever fires first**: `MaxBatchSize` proposals,
`MaxBatchBytes` encoded bytes, `FlushInterval` elapsed, or `Close`.

**`MaxBatchBytes` is enforced by flushing first, never by exceeding.** A
proposal that would push the batch past the cap triggers a flush, so the batch
handed to the proposer never exceeds it and the oversized proposal ends up alone
in its own batch instead of poisoning the unrelated commands batched with it.

**The cap applies to the whole accumulated batch, not per shard.** A per-shard
subset is never larger than the whole, so the bound holds for every shard's
proposal; a multi-shard workload flushes slightly sooner than per-shard
arithmetic would require.

**The propose deadline is `min(latest live caller deadline, ProposeTimeout)`.**
The aggregator never proposes for longer than `ProposeTimeout`, and never cuts a
caller short of its own deadline.

**`Submit` is safe for concurrent use** from multiple goroutines. The aggregator
owns a background flush loop.

**`SubmitNoCopy` transfers ownership of `data`.** The aggregator reads the slice
at an unspecified later time — after `Submit`'s caller has returned, on the
flush goroutine. Mutating or reusing the buffer commits a **wrong command on
every replica**. Use it only for a buffer allocated per call and then dropped.

**`Close` is a one-way transition**: it stops accepting proposals, flushes what
is pending, and every subsequent `Submit` returns `ErrClosed`.

**Batch payload format**: each command prefixed with its length as a **4-byte
big-endian uint32**. `EncodeBatch(nil)` and `EncodeBatch([][]byte{})` produce
zero-length output; `DecodeBatch(nil)` and `DecodeBatch([]byte{})` return a nil
slice with no error.

**`writemode.Mode` is an open `uint8`.** A value outside the three constants is
representable and describes **no guarantee**: `String` reports `"unknown(N)"`
and `IsSynchronous`, `IsAsync` and `RequiresQuorum` all report **false**. The
predicates cannot report an error, so `Validate` is the boundary check and a
caller accepting a `Mode` from outside its own code **must** run it there.

## 3. Error semantics

| Error | Returned when | Fail-closed? |
|---|---|---|
| `ErrClosed` / `*ClosedError` | `Submit` after `Close` | yes |
| `*NilProposerError` | `NewAggregator(nil, ...)` | yes — construction fails |
| `*QueueFullError` | the proposal channel is at `QueueSize` | yes — back-pressure, not a silent drop |
| `*FlushError` | the underlying `SyncPropose` failed | yes, for that batch |
| `*EncodeError` | a command cannot be encoded | yes |
| `*DecodeError` | malformed batch data — truncated length prefix, or a command length past the buffer | yes |
| `*InvalidModeError` | `writemode.Validate` on an unrecognized value, or `FromString` on an unknown name | yes |

Sentinel errors are **pre-allocated for zero-alloc hot-path returns** and
implement `Is`, so `errors.Is(err, batch.ErrClosed)` works against both the
sentinel and the typed form.

**The path that is deliberately not fail-closed**: `writemode`'s predicates on
an unknown `Mode`. They report false rather than erroring, which is why
`Validate` exists and why a test pins the behavior.

## 4. Configuration

`DefaultConfig()` returns production-ready values:

| Field | Default | Meaning |
|---|---|---|
| `MaxBatchSize` | `DefaultMaxBatchSize` = 1000 | proposals before a flush |
| `MaxBatchBytes` | `DefaultMaxBatchBytes` = `proto.MaxEntrySize` (8 MB) | encoded bytes before a flush; the batch never exceeds it |
| `FlushInterval` | `DefaultFlushInterval` = 50 ms | maximum time between flushes — **this is the latency you are trading for throughput** |
| `QueueSize` | `DefaultQueueSize` = 10000 | proposal channel capacity; past it, `*QueueFullError` |
| `ProposeTimeout` | `DefaultProposeTimeout` = 30 s | ceiling on one batch's `SyncPropose` |

Applied with `WithConfig(cfg)`. `WithLogger` and `WithRegistry` install a
`*slog.Logger` and a `prometheus.Registerer`.

`writemode` reads no configuration; `FromString` parses a mode name from
whatever the embedder's own configuration format is.

## 5. Persistence

**None.** A batch that has not flushed lives only in memory, and an aggregator
that is killed loses it. `Close` flushes; a `SIGKILL` does not.

What reaches disk is one ordinary Raft entry per batch, through the normal
proposal path — see [`../log-storage/engineering.md`](../log-storage/engineering.md).

## 6. Test Strategy

Targets: `make test-batch`, `make bench-batch`, `make test-writemode`,
`make bench-writemode`.

| Covers | Where |
|---|---|
| the three flush triggers, independently and in combination | `pkg/batch/*_test.go` |
| the oversized-proposal isolation path | as above |
| `min(caller deadline, ProposeTimeout)` | as above |
| concurrent `Submit` under `-race` | as above |
| `Close` semantics and post-`Close` `Submit` | as above |
| `EncodeBatch`/`DecodeBatch` round trips and malformed input | as above |
| **`TestMode_UnknownValueGuaranteesNothing`** | `pkg/writemode` |

**The test that must never be deleted**: `TestMode_UnknownValueGuaranteesNothing`.
It pins that an unrecognized `Mode` reports false from every predicate, which is
the behavior an embedder's dispatch depends on being stable.

**Fault-injection seam**: `Proposer` is an interface, so a test proposer that
fails, blocks, or counts invocations is one struct.

**Coverage note.** `pkg/batch` sits at 90.5%, just below the repository's own
`COVER_MIN = 91`, and the module-total gate does not catch it. Tracked work
rather than a settled state.

## 7. Observability

**Metrics** — Prometheus, when a registry is supplied:

```go
batch.NewAggregator(proposer, batch.WithRegistry(reg))
```

This is the only package in the repository that registers Prometheus collectors
directly rather than going through the host's `MetricsCollector`, because it is
an optional component that sits **above** the `Host` rather than inside it.

The two to watch: **flush rate** against `FlushInterval` — a flush rate at the
interval means batches are timing out rather than filling, so `MaxBatchSize` is
too high for the load — and **queue depth** against `QueueSize`, which becomes
`*QueueFullError` at the ceiling.

`BatchLen()` reports the current unflushed count for ad-hoc inspection.

**Logging** — a `*slog.Logger` via `WithLogger`; nil disables. Flush failures
are the events worth alerting on, because a failed flush is a batch of caller
commands that did not commit.

**No command payload is logged.** It is application data.
