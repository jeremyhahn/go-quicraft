# Wire Protocol — CLI

> **Status: NORMATIVE** · Scope: go-quicraft `pkg/proto` · Last verified: 2026-08-19
> Conflicts with this document are defects. Fix them here first.

**No CLI surface: this feature is reachable only as a Go package.**

go-quicraft ships no binaries. The repository has no `cmd/` directory, and
`make build` runs `go build ./...` and produces no executable. It is a consensus
library, consumed by importing it.

This feature contributes **no** `CommandDefinition` to any integration parity
suite, and there is no verb above it either: a wire format is not something an
operator invokes. What an operator sees of it is a decode error in a log line,
attributed by the transport or the WAL — the layers that know which peer or
which segment produced the bad bytes.

There is deliberately **no** `quicraft decode` / `quicraft dump` inspection
tool. Such a tool would have to be kept byte-exact with `pkg/proto` forever, and
the moment it drifted it would report confident nonsense about a log an operator
was trying to diagnose. The offset tables in
[`engineering.md`](engineering.md), which are verified against the code, are the
inspection surface. `xxd` plus the tables is the supported procedure.

## Build-time targets

| Target | Runs |
|---|---|
| `make test-proto` | `go test ./pkg/proto/...` |
| `make bench-proto` | the codec benchmarks |
| `make fuzz-proto` | the eleven `Fuzz*` targets against the checked-in corpora |
| `make fuzz-all` | `fuzz-proto` plus the transport, waldb, seal, session and raft targets |

## The one operational lever

Not a flag, but the number an operator has to compute rather than read:

`TransportConfig.MaxMessageReceiveMemory` is a **wire-byte** budget.
`MaxDecodeMemoryFactor` (8) is how much heap a decode may claim as a multiple of
that. A 256 MB receive budget therefore admits frames that can occupy up to 2 GB
of heap. Size the process accordingly.
