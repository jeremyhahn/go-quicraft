# QUIC Transport — CLI

> **Status: NORMATIVE** · Scope: go-quicraft `pkg/internal/transport` ·
> Last verified: 2026-08-19
> Conflicts with this document are defects. Fix them here first.

**No CLI surface: this feature is reachable only through Go configuration.**

go-quicraft ships no binaries. The repository has no `cmd/` directory, and
`make build` runs `go build ./...` and produces no executable. The transport
packages are additionally `internal/`, so they are not importable from outside
the module either — an embedder reaches them only through
`config.TransportConfig`.

This feature contributes **no** `CommandDefinition` to any integration parity
suite. The operations that look like verbs belong elsewhere on purpose:

| Looks like a verb | Actually belongs to |
|---|---|
| issue / rotate a node certificate | the key-management layer above (go-xkms), which owns the CA |
| publish or refresh a CRL | the same CA |
| add a peer, resolve an address | membership and discovery — [`../membership-changes/cli.md`](../membership-changes/cli.md), and peer discovery is go-qrdb's |
| inspect connections | the metrics surface, not a command — see below |

## Build-time targets

| Target | Runs |
|---|---|
| `make test-transport` | `go test ./pkg/internal/transport/...` |
| `make bench-transport` | connection, stream-pool and frame benchmarks |
| `make fuzz-transport` | frame-header and length-prefix decoding against arbitrary bytes |
| `make ci-netfault` | the `test/netfault` partition, delay and drop suite |

Note that `make verify-fips` **excludes** `pkg/internal/transport/...` by an
explicit deny-list. That is not an oversight: QUIC packet protection and OCSP's
`CertID` cannot be made FIPS-approved from here. See
[`architecture.md`](architecture.md).

## Operational levers that are not verbs

All configuration, set by the embedding application:

| Lever | Field | Consequence of getting it wrong |
|---|---|---|
| certificates | `MTLSConfig` | the transport refuses to start without one |
| revocation | `RevocationConfig` | **fails closed by default**; a mode with no usable source refuses connections rather than admitting them |
| sender authorization | `RequireMemberIdentity` | `false` falls back to a SAN check against the committed Raft address; `true` drops messages from senders with no committed identity |
| 0-RTT | `Enable0RTT` | off by default because **0-RTT data is replayable** |
| inbound limits | `MaxIncomingConnections`, `MaxConnectionsPerIP` | at the ceiling, new peers are refused |
| receive memory | `MaxMessageReceiveMemory` | a **wire-byte** budget; multiply by 8 for heap |
| snapshot receive | `MaxSnapshotReceiveRate`, `MaxConcurrentSnapshotReceives`, `MaxSnapshotReceiveMemory` | a recovering follower can otherwise saturate the link |
| socket buffers | `UDPRecvBufSize`, `UDPSendBufSize` | 7 MB each; 0 uses the OS default, which is usually much smaller than a Raft cluster wants |

## Inspecting a running transport

Not commands — three calls, surfaced through whatever the embedder exposes:

```go
transport.ConnectionCount()
transport.StreamCount()
transport.SendQueueCount()
```

Send-queue depth is the one to watch: a peer that cannot keep up shows there
before it shows as a Raft election.
