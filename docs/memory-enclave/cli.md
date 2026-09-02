# Memory Enclave — CLI

> **Status: NORMATIVE** · Scope: go-quicraft `pkg/enclave` · Last verified: 2026-09-02
> Conflicts with this document are defects. Fix them here first.

**No CLI surface: this feature is reachable only as a Go package.**

go-quicraft ships no binaries at all. The repository has no `cmd/` directory,
and `make build` runs `go build ./...` and produces no executable. That is
correct for the project — it is a consensus **library**, consumed by importing
it and implementing its interfaces, not by invoking a client.

There is also no CLI surface *above* this feature. Memory protection is an
internal property of whatever holds a key, not an operator verb: exposing "open
this key into a buffer" as a command would defeat the point of the package. This
feature therefore contributes **no** `CommandDefinition` to any integration
parity suite, and that absence is not a gap.

The operator-visible consequences appear under the surfaces of the projects that
consume this library — the barrier and seal verbs of the database layer above,
and the key-custody verbs of the key-management layer above that. Those verbs are
documented in their own repositories.

The one operational lever that is not a verb: locked buffers consume
`RLIMIT_MEMLOCK`. A container running a consumer must be given a limit large
enough for its protected keys, or locked allocations start failing. That is a
deployment setting, not a flag.
