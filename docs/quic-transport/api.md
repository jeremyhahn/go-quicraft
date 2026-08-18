# QUIC Transport — API

> **Status: NORMATIVE** · Scope: go-quicraft `pkg/internal/transport` and its
> configuration surface in `pkg/config` · Last verified: 2026-08-19
> Conflicts with this document are defects. Fix them here first.

## Transport coverage

This feature **is** the repository's only network transport, so the table reads
differently from the other features':

| Transport | This feature |
|---|---|
| **QUIC** | the listener and dialer this feature implements. It carries Raft messages and snapshot chunks between replicas of one cluster — it is **not** a client-facing API surface, and there is no request/response service on it. |
| **Embedded (Go call)** | the configuration surface below. The transport itself has **no exported Go type**: `pkg/internal/transport` is `internal/` and not importable. |
| Unix socket / REST / gRPC / MCP | n/a — this repository serves none of them |

Not missing-capability rows. A client-facing transport is the composing layer's
job; this one speaks a peer protocol between replicas.

## There is no exported transport API

The `Host` holds a concrete `*transport.QUICTransport` from an `internal/`
package. There is deliberately **no `Transport` interface, no exported
transport type, and no way to substitute an implementation** — see
[`architecture.md`](architecture.md).

What an embedder controls is configuration and one host option:

```go
func WithoutTransport() HostOption
```

Disables the transport entirely — the QUIC transport is not constructed, and
mTLS validation is skipped because there is nothing to validate. This is "no
transport", **not** "QUIC without TLS". Use it for single-node embedded
deployments and tests.

## Configuration surface

Import: `github.com/jeremyhahn/go-quicraft/pkg/config`

### `TransportConfig`

| Field | Type | Default | Meaning |
|---|---|---|---|
| `MTLSConfig` | `*MTLSConfig` | none | **Required** whenever the transport is enabled. `ErrMTLSConfigRequired` otherwise. |
| `RevocationConfig` | `*RevocationConfig` | none | CRL / OCSP checking |
| `StreamPoolSize` | `int` | `DefaultStreamPoolSize` = 16 | data streams per connection |
| `MaxStreamPoolSize` | `int` | 256 | ceiling for dynamic pool growth |
| `MaxStreamsPerConnection` | `int` | 256 | QUIC stream limit per connection |
| `MaxIncomingConnections` | `int` | 256 | total inbound connections |
| `MaxConnectionsPerIP` | `int` | 16 | inbound connections from one source IP |
| `StreamReadIdleTimeout` | `time.Duration` | 30s | idle timeout on a receive stream |
| `MaxMessageReceiveMemory` | `int64` | 256 MB | **wire bytes** reserved before a frame body is read, so a peer cannot declare a huge frame and withhold it. Multiply by `proto.MaxDecodeMemoryFactor` (8) for heap. |
| `MaxSnapshotReceiveRate` | `int64` | 256 MB/s | inbound snapshot bandwidth |
| `MaxConcurrentSnapshotReceives` | `int` | 4 | concurrent inbound transfers |
| `MaxSnapshotReceiveMemory` | `int64` | 1 GB | snapshot receive buffer pool |
| `MaxDecompressedSize` | `uint32` | — | decompression ceiling |
| `SendBatchMaxSize` | `int` | 64 KB | maximum batch to one peer |
| `UDPRecvBufSize` / `UDPSendBufSize` | `int` | 7 MB each; 0 uses the OS default | socket buffers |
| `Enable0RTT` | `bool` | **false** | QUIC 0-RTT. **0-RTT data is replayable.** |
| `DisableCompression` | `bool` | false | for benchmarking |
| `TransportDisabled` | `bool` | false | set by `WithoutTransport()` |
| `RequireMemberIdentity` | `bool` | **false** | drop messages from a sender with no committed identity, instead of falling back to a SAN check against the committed Raft address |

### `MTLSConfig`

| Field | Type | Meaning |
|---|---|---|
| `CACert` | `[]byte` | PEM CA bundle, used as both `RootCAs` and `ClientCAs` |
| `Cert` | `[]byte` | PEM leaf certificate |
| `Key` | `[]byte` | PEM private key |
| `Certificate` | `*tls.Certificate` | a pre-loaded certificate, as an alternative to the PEM fields |

Everything else about the TLS configuration is **fixed and not exposed**: TLS
1.3 minimum, `RequireAndVerifyClientCert`, `RootCAs` plus per-dial `ServerName`,
ALPN `"quicraft"`, X25519 only. See
[`architecture.md`](architecture.md#the-tls-configuration-is-fixed-not-configurable).

Certificate requirements an issuer must satisfy: each node's leaf must carry a
SAN covering the address peers dial it at, and **must not** cover another
member's address — a leaf that covers the local node is refused at inbound
connection setup.

### `RevocationConfig`

| Field | Type | Default | Meaning |
|---|---|---|---|
| `CRLPaths` | `[]string` | none | CRL files, hot-reloaded on SHA-256 content change |
| `OCSPResponderURL` | `string` | none | OCSP responder |
| `OCSPCacheSeconds` | `int` | 300 | response cache TTL; the cache holds 4096 entries |
| `OCSPTimeoutSeconds` | `int` | — | per-query timeout |
| `CheckInterval` | `time.Duration` | 60s | CRL reload poll |
| `Mode` | `string` | `"any"` | `"crl"`, `"ocsp"`, `"both"`, `"any"` |
| `AllowSoftFail` | `bool` | **false** | **Unknown status fails closed by default.** Setting this opts out. |
| `OnCertRevoked` | `func(serial string)` | nil | callback |

Under enforcement, a mode with no usable source returns
`ErrNoRevocationSources` from `IsRevoked` rather than "not revoked", in **every**
mode including `"crl"`. `Config.Validate` rejects such a configuration at
startup.

The check is installed as `VerifyConnection`, so **resumed sessions are checked
too**.

## Errors an embedder sees

| Error | Raised when |
|---|---|
| `ErrMTLSConfigRequired` (`*MTLSConfigRequiredError`) | the transport is enabled with no `MTLSConfig` |
| `ErrNoRevocationSources` | revocation enforcement is on and no source is usable |
| `*quicraft.UnauthorizedMessageError` | a message arrived from a sender that is not authorized for the shard |
| `*quicraft.SnapshotSendError` / `*quicraft.SnapshotReceiveError` | a snapshot transfer failed |

Host-level errors and the full vocabulary are in
[`../multi-group-host/api.md`](../multi-group-host/api.md).

## What crosses the wire

`proto.MessageBatch` and `proto.SnapshotChunk`, framed by this layer. The frame
header carries `transport.WireVersion` (`0x01`), which **mirrors**
`proto.WireVersion` and is gated by exact match. See
[`../wire-protocol/api.md`](../wire-protocol/api.md).

Node identity on the wire is derived from the mTLS leaf, never from the message
body:

```
"spki:" + hex(sha256(leaf.RawSubjectPublicKeyInfo))
```

## Versioning and deprecation

- `transport.WireVersion` = `0x01`, transport-scoped, **exact match**, pinned
  for the pre-release period along with every other format in the repository.
- **Bump it whenever any type reachable from `MessageBatch` or `SnapshotChunk`
  changes layout**, including the embedded persisted ones. It is a supremum.
- Nothing here is deprecated.
- `RequireMemberIdentity` defaults to `false` for compatibility with clusters
  whose membership predates committed identities. Enabling it on a live cluster
  is a sequenced migration — see [`../compatibility.md`](../compatibility.md).
