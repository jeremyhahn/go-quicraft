# Shamir primitive: CLI

> **Status: NORMATIVE** · Scope: go-quicraft `pkg/crypto/shamir` · Last verified: 2026-10-11
> Conflicts with this document are defects. Fix them here first.

No CLI: go-quicraft is a library and ships no binaries (there is no `cmd/`
directory), so this feature is reachable only through the Go API in
[`api.md`](api.md).

A `split` verb here would also mean this repository generating and printing share
material, and share custody is not owned here (see
[`architecture.md`](architecture.md), "The scope fence comes first").

The operator verbs (initialize an M-of-N split, submit a share toward a quorum,
check progress, recover, rekey) belong to the layers above and are documented
there:

| Owner | Surface |
|---|---|
| go-qrdb | the `shamir` sealing strategy, share custody, quorum sessions, recovery, rekey, root tokens, the Security-Officer ceremony |
| go-xkms `xkey` | the desktop custodian client that holds a custodian's share and submits it |

The package reads no configuration, environment variables or files and holds no
state between calls. Its only tunables, `threshold` and `total`, are call
arguments chosen by the ceremony above.
