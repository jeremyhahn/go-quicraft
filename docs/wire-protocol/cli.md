# Wire Protocol — CLI

> **Status: NORMATIVE** · Scope: go-quicraft `pkg/proto` · Last verified: 2026-10-11
> Conflicts with this document are defects. Fix them here first.

There is no CLI: go-quicraft is a library and ships no binaries, and a wire
format is not something an operator invokes. The surface is the
`pkg/proto` Go package ([`api.md`](api.md)).

What an operator sees of this feature is a decode error in a log line,
attributed by the transport or the WAL, the layers that know which peer or
which segment produced the bad bytes.

There is deliberately no decode or dump tool. It would have to stay byte-exact
with `pkg/proto`, and the moment it drifted it would report confident nonsense
about a log someone was trying to diagnose. The offset tables in
[`engineering.md`](engineering.md), verified against the code, plus `xxd` are
the supported procedure.

## Build-time targets

| Target | Runs |
|---|---|
| `make test-proto` | `go test -race -count=1 -v ./pkg/proto/...` |
| `make bench-proto` | the codec benchmarks |
| `make ci-fuzz-seeds` | every `Fuzz*` target in the module, once per seed and checked-in corpus input (`-tags quicraft_fuzz`) |
| `make ci-fuzz-mutation` | a bounded mutation pass per target over `./pkg/proto` and the other fuzzed packages |
