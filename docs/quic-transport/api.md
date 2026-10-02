# QUIC Transport — API

> **Status: NORMATIVE** · Scope: go-quicraft `pkg/internal/transport` and its
> configuration surface in `pkg/config` · Last verified: 2026-10-02
> Conflicts with this document are defects. Fix them here first.

## Transport coverage

This feature is the repository's only network transport, so the table reads
differently from the other features':

| Transport | This feature |
|---|---|
| **QUIC** | the listener and dialer this feature implements. It carries Raft messages and snapshot chunks between replicas of one cluster. It is not a client-facing API and there is no request/response service on it. |
| **Embedded (Go call)** | the configuration surface below. The transport itself has no exported Go type: `pkg/internal/transport` is `internal/` and not importable. |
| Unix socket / REST / gRPC / MCP | n/a; this repository serves none of them |

These are not missing capabilities. A client-facing transport is the composing
layer's job; this one speaks a peer protocol between replicas.

## There is no exported transport API

The `Host` holds a concrete `*transport.QUICTransport` from an `internal/`
package. There is deliberately no `Transport` interface, no exported transport
type, and no way to substitute an implementation; see
[`architecture.md`](architecture.md).

What an embedder controls is configuration and one host option:

```go
func WithoutTransport() HostOption
```

It disables the transport entirely: the QUIC transport is not constructed, and
mTLS validation is skipped because there is nothing to validate. This is "no
transport", not "QUIC without TLS". Use it for single-node embedded deployments
and tests.

## Configuration surface

Import: `github.com/jeremyhahn/go-quicraft/pkg/config`. The transport is
configured through `HostConfig.TransportConfig`, a `TransportConfig` value.
Zero-valued fields take the defaults below (`SetDefaults`).

### `TransportConfig`

| Field | Type | Default | Meaning |
|---|---|---|---|
| `MTLSConfig` | `*MTLSConfig` | none | Required whenever the transport is enabled; `ErrMTLSConfigRequired` otherwise. |
| `RevocationConfig` | `*RevocationConfig` | nil | CRL / OCSP checking; nil disables revocation checking |
| `StreamPoolSize` | `int` | 16 (`DefaultStreamPoolSize`) | data streams per connection; index 0 carries heartbeats |
| `MaxStreamPoolSize` | `int` | 256 | the ceiling `Validate` checks `StreamPoolSize` against; the pool does not grow at runtime |
| `MaxStreamsPerConnection` | `int` | 256 | concurrent inbound data streams (and goroutines) per connection |
| `MaxIncomingConnections` | `int` | 256 | total inbound connections |
| `MaxConnectionsPerIP` | `int` | 16 | inbound connections from one source IP |
| `StreamReadIdleTimeout` | `time.Duration` | 30s | how long a read waits at a clean frame boundary before re-checking the connection; not a stream lifetime |
| `MaxMessageReceiveMemory` | `int64` | 256 MB | aggregate heap budget for inbound message frames. Each frame reserves its declared length times `proto.MaxDecodeMemoryFactor` (8) before its body is read, so a peer cannot declare a huge frame and withhold it. One peer may hold at most a quarter of it. |
| `MaxSnapshotReceiveRate` | `int64` | 256 MB/s | inbound snapshot bandwidth; a non-zero value below 64 KB/s is rejected |
| `MaxConcurrentSnapshotReceives` | `int` | 4 | concurrent inbound snapshot transfers |
| `MaxSnapshotReceiveMemory` | `int64` | 1 GB | in-flight snapshot chunk memory |
| `MaxDecompressedSize` | `uint32` | 16 MB | Snappy decompression ceiling; must equal the transport's frame size, any other value is rejected |
| `SendBatchMaxSize` | `int` | 64 KB | initial capacity of pooled send buffers; it does not cap a batch |
| `UDPRecvBufSize` / `UDPSendBufSize` | `int` | 7 MB each; 0 uses the OS default | socket buffers |
| `Enable0RTT` | `bool` | false | QUIC 0-RTT. 0-RTT data is replayable, and the receive path refuses message frames and snapshots that arrive over it. |
| `DisableCompression` | `bool` | false | turn off Snappy for message frames (benchmarking) |
| `TransportDisabled` | `bool` | false | set by `WithoutTransport()`; skips mTLS validation and does not build the transport |

### `MTLSConfig`

| Field | Type | Meaning |
|---|---|---|
| `CACert` | `[]byte` | PEM CA bundle, used as both `RootCAs` and `ClientCAs`; always required |
| `Cert` | `[]byte` | PEM leaf certificate |
| `Key` | `[]byte` | PEM private key |
| `Certificate` | `*tls.Certificate` | a pre-built certificate in place of `Cert`/`Key`; its `PrivateKey` may be any `crypto.Signer`, such as a TPM- or HSM-resident key |

Everything else about the TLS configuration is fixed and not exposed: TLS 1.3
minimum, `RequireAndVerifyClientCert`, `RootCAs` plus a per-dial `ServerName`,
ALPN `"quicraft"`, and a curve list that depends on the build tag
(`[X25519, P-256, P-384]`, or `[P-256, P-384]` with `-tags fips`). See
[`architecture.md`](architecture.md#the-tls-configuration-is-fixed-not-configurable).

Certificate requirements an issuer must satisfy:

- Each node's leaf carries a SAN covering the address peers dial it at
  (`HostConfig.RaftAddress`).
- It must not cover another member's host. `StartShard` refuses to start when
  the local leaf covers another member's host, and a peer whose leaf covers
  the local node's host is refused at inbound connection setup.
- Each host has its own keypair. The node identity is
  `"spki:" + hex(sha256(leaf.RawSubjectPublicKeyInfo))`; `Host.NodeIdentity()`
  reports it, and every member admission names it.

### `RevocationConfig`

| Field | Type | Default | Meaning |
|---|---|---|---|
| `CRLPaths` | `[]string` | none | PEM CRL files, hot-reloaded on SHA-256 content change |
| `OCSPResponderURL` | `string` | none | OCSP responder |
| `OCSPCacheSeconds` | `int` | 300 | response cache TTL; the cache holds 4096 entries |
| `OCSPTimeoutSeconds` | `int` | 5 | per-query HTTP timeout |
| `CheckInterval` | `time.Duration` | 60s | CRL reload poll |
| `Mode` | `string` | `"any"` | `"crl"`, `"ocsp"`, `"both"`, `"any"`; any other value is rejected |
| `AllowSoftFail` | `bool` | false | Unknown status fails closed by default. Setting this admits peers whose status cannot be determined. |
| `OnCertRevoked` | `func(serial string)` | nil | called with the lowercase hex serial when a handshake finds a revoked certificate |

`Validate` requires `CRLPaths` for `"crl"` and `"both"`, `OCSPResponderURL` for
`"ocsp"` and `"both"`, and at least one of them for `"any"` unless
`AllowSoftFail` is set. At runtime, a mode with no usable source refuses the
connection rather than reporting "not revoked", in every mode including
`"crl"`.

The check is installed as `VerifyConnection`, so resumed sessions are checked
too, and live connections are re-checked every 30 seconds.

## Errors and signals an embedder sees

| Error or signal | Raised when |
|---|---|
| `config.ErrMTLSConfigRequired` (`*MTLSConfigRequiredError`) | the transport is enabled with no `MTLSConfig` |
| `*config.ValidationError` | a limit, the mTLS material or the revocation configuration is invalid; `Field` names it |
| `quicraft.ErrMemberIdentityRequired` | a member admission on a host with a transport names no node identity |
| `quicraft.ErrSharedPeerCertificate` (`*quicraft.SharedPeerCertificateError`) | `StartShard` or a membership add where the local leaf also covers another member's host; `CoveredPeers` names them |
| `*quicraft.SnapshotSendError` / `*quicraft.SnapshotReceiveError` | a snapshot transfer failed |
| `IncUnauthorizedMessageDropped` / `IncUnauthorizedSnapshotDropped` (metrics) | an inbound message or snapshot came from a peer whose identity no configuration this replica holds binds to the claimed sender. The handler refuses it with `*quicraft.UnauthorizedMessageError`, which is logged, not returned to the embedder. |
| `IncMessagesDropped` (metrics) | the transport dropped an outbound or inbound message; the reason labels are in [`engineering.md`](engineering.md#transport-metrics) |
| `EventListener.OnConnectionFailed` | an outbound dial failed, including TLS refusals and a peer presenting the wrong identity |

Host-level errors and the full vocabulary are in
[`../multi-group-host/api.md`](../multi-group-host/api.md).

## What crosses the wire

`proto.MessageBatch`, and on a snapshot stream a 56-byte `SnapshotHeader`
followed by `proto.SnapshotChunk` frames, each framed by this layer. The frame
header carries `transport.WireVersion` (`0x01`), gated by exact match. See
[`../wire-protocol/api.md`](../wire-protocol/api.md).

Node identity is derived from the mTLS leaf, never from the message body:

```
"spki:" + hex(sha256(leaf.RawSubjectPublicKeyInfo))
```

## Versioning and deprecation

- `transport.WireVersion` = `0x01`, transport-scoped, exact match. Like every
  format version in this repository, it stays at its current value until the
  first production release: a frame layout change before then is made in place,
  with every peer rebuilt from the same commit, and the byte does not move.
- From the first production release, the transport version moves whenever
  any type reachable from `MessageBatch` or `SnapshotChunk` changes layout,
  including the embedded persisted ones; see
  [`../wire-protocol/architecture.md`](../wire-protocol/architecture.md#the-transport-version-is-a-supremum).
- Nothing here is deprecated. There is no setting that relaxes sender
  authorization: a member is authorized by its committed node identity and by
  nothing else. See [`../compatibility.md`](../compatibility.md).
