# Membership changes: CLI

> **Status: NORMATIVE** · Scope: go-quicraft membership · Last verified: 2026-10-11
> Conflicts with this document are defects. Fix them here first.

**No CLI surface: go-quicraft is a library and ships no binaries.** There is no
`cmd/` directory, and `make build` runs `go build ./...` without producing an
executable. Membership changes are made through the `Host` methods in
[`api.md`](api.md).

This feature contributes no `CommandDefinition` to any integration parity suite.
A membership change needs a `configChangeIndex` read from the current membership
and must be sent to the current leader; a command line that guessed either would
mostly produce `*StaleConfigChangeError`.

## Where the operator verbs live

Join a node, evict one, rebalance: those verbs belong to the layer that owns
cluster formation.

| Owner | Responsibility |
|---|---|
| go-qrdb | peer discovery (static, multicast, DNS SRV, token), the discover → quorum → `StartShard` bootstrap sequence, and opt-in dynamic membership |
| go-xkms | the CA that issues the certificates whose SPKI pins become node identities |
| go-trusted-platform | re-exports the composed stack's verbs |

## Build-time targets

| Target | Runs |
|---|---|
| `make test-raft` | the config-change state machine |
| `make integration-test-host` | membership changes and observer promotion across real hosts |
| `make integration-test-e2e` | membership changes and replica replacement in a multi-node cluster |

Configuration levers (`OrderedConfigChange`, `IsObserver`, `IsWitness`,
`Witness`, `MemberIdentities`) are listed in [`api.md`](api.md#configuration),
and the operator procedures in [`user.md`](user.md).
