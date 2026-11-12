# Memory enclave: CLI

> **Status: NORMATIVE** · Scope: go-quicraft's use of go-enclave protected memory · Last verified: 2026-11-12
> Conflicts with this document are defects. Fix them here first.

No CLI: go-quicraft is a library and ships no binaries (there is no `cmd/`
directory), so this feature is reachable only through the Go API in
[`api.md`](api.md).

Memory protection is a property of whatever holds a key, not an operator verb,
so no consuming project exposes it as a command either. The operator-visible
seal and key-custody verbs belong to the projects that consume this library and
are documented there.

The one operational lever is not a flag: locked memory consumes
`RLIMIT_MEMLOCK`. A process or container running a consumer needs a limit large
enough for its protected keys, or locked allocations fail.
