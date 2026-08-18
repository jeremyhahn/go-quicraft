# Shamir Primitive — CLI

> **Status: NORMATIVE** · Scope: go-quicraft `pkg/crypto/shamir` · Last verified: 2026-08-19
> Conflicts with this document are defects. Fix them here first.

**No CLI surface: this feature is reachable only as a Go package.**

go-quicraft ships no binaries. The repository has no `cmd/` directory, and
`make build` runs `go build ./...` and produces no executable. It is a consensus
library, consumed by importing it.

That absence is more pointed here than elsewhere. A `quicraft shamir split`
verb would mean this repository generating and printing share material — and
share custody is explicitly **not** owned here (see
[`architecture.md`](architecture.md) §The scope fence). This feature therefore
contributes **no** `CommandDefinition` to any integration parity suite, and
adding one would be a boundary violation rather than a feature.

## Where the operator verbs actually live

The `share`, `barrier` and ceremony verbs an operator runs — initialize an
M-of-N split, submit a share toward a quorum, check quorum progress, recover,
rekey — belong to the layers above:

| Owner | Surface |
|---|---|
| **go-qrdb** | the `shamir` sealing strategy, share custody, quorum sessions, recovery, rekey, root tokens, the Security-Officer ceremony |
| **go-xkms `xkey`** | the desktop custodian client — holding a custodian's share and submitting it |

Those verbs are documented in their own repositories, against the locked CLI
surface the portfolio maintains. A divergence between that locked surface and
the code is raised there, not resolved by writing a local variant down here.

What this repository provides to those verbs is the two function calls in
[`api.md`](api.md), and nothing else.

## Non-verb operational levers

None. This package reads no configuration, no environment variables, and no
files, and holds no state between calls. Its only tunables — `threshold` and
`total` — are call arguments chosen by the ceremony above.
