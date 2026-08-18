# QUIC Transport — Architecture

> **Status: NORMATIVE** · Scope: go-quicraft `pkg/internal/transport`,
> `pkg/internal/transport/revocation`, `pkg/internal/registry`,
> `pkg/internal/netaddr` · Last verified: 2026-08-19
> Conflicts with this document are defects. Fix them here first.

## The problem

A host runs thousands of Raft groups over a handful of peer connections. On a
stream-oriented transport they share one byte stream, so one shard's
multi-megabyte replication batch stalls every other shard behind it — and a
heartbeat delayed past the election timeout is not a latency problem, it is a
spurious leader election.

## The shape chosen: QUIC only, mTLS mandatory

**There is no TCP fallback and no `Transport` interface to swap one in.** The
`Host` holds a concrete `*transport.QUICTransport` and wraps it in an internal
`transportSender` satisfying the engine's `MessageSender`.

QUIC 1.0 (RFC 9000) buys:

- **Multiplexed streams**, so one stalled shard does not head-of-line-block
  others. This is the reason for the choice; everything else is a bonus.
- **Built-in TLS 1.3**, with 0-RTT available and **off by default** (0-RTT data
  is replayable).
- **Stream 0 reserved for heartbeats**, pre-opened per connection and reused
  across ticks. Heartbeats also ride a separate priority channel drained ahead
  of data, so a large replication batch cannot delay one past the election
  timeout.
- `StreamPoolSize` (16) data streams per connection, growable to
  `MaxStreamPoolSize` (256), to cut mutex contention.
- Pooled byte buffers on send and receive.
- `singleflight.Group` coalescing concurrent dials to the same peer.

Snapshot transfers carry a dedicated frame flag routing them to a separate
handler, keeping multi-gigabyte streams out of the stream pool entirely.

### Rejected: TCP with a fallback path

A fallback is a second transport to test, a second set of failure modes, and a
silent downgrade waiting to happen — a cluster that "works" on TCP because the
UDP path is blocked has lost the head-of-line property it was built on without
telling anyone. If QUIC cannot reach a peer, that is a fact worth surfacing.

### Rejected: a `Transport` interface

An interface implies swappable implementations, and there is exactly one. What
it would actually buy is the ability for an embedder to substitute a transport
that does not do mTLS, which is the property this design is least willing to
give up.

### Rejected: one stream per shard

Thousands of shards means thousands of streams per connection, past QUIC's own
limits and past anything a stream pool can amortize. The pool multiplexes many
shards over a bounded stream set instead, and the ordering the algorithm
actually needs is per-peer, not per-shard.

### Rejected: optional mTLS

The transport refuses to start without an mTLS config
(`ErrMTLSConfigRequired`). `TransportConfig.TransportDisabled` — set by
`WithoutTransport()` — skips the validation, but it also prevents the transport
being constructed at all. That is "no transport", not "QUIC without TLS", and
the distinction is deliberate: there is no configuration that produces an
unauthenticated listener.

## Data flow

```
  engine step worker                                  peer
        │  MessageSender.Send(batch)                   │
        ▼                                              │
   transportSender                                     │
        │  registry.Resolve(shardID, replicaID) ──▶ address
        ▼                                              │
   connection pool ──── singleflight dial ─────────────┤ mTLS 1.3, ALPN "quicraft"
        │                                              │
        ├── stream 0 ──── heartbeats (priority chan) ──▶
        ├── stream pool ─ MessageBatch frames ─────────▶
        └── dedicated ─── snapshot chunks ─────────────▶

  inbound: accept ─▶ admission limits ─▶ leaf → SPKI identity
                  ─▶ frame header (wire version) ─▶ proto decode
                  ─▶ sender authorization ─▶ engine inbox
```

Everything crossing the wire is `pkg/proto`'s — see
[`../wire-protocol/architecture.md`](../wire-protocol/architecture.md). The
transport owns framing, the frame header byte that mirrors `WireVersion`, and
nothing about the payload's meaning.

## Trust boundaries

This is where the repository's network trust boundary is, so it is stated in
full rather than referenced.

### The TLS configuration is fixed, not configurable

There is exactly one TLS-config constructor, `buildMTLSTLS`:

| Setting | Value |
|---|---|
| `MinVersion` | TLS 1.3, both directions |
| `ClientAuth` | `RequireAndVerifyClientCert` against `ClientCAs` |
| Server verification | `RootCAs` plus per-dial `ServerName`, so hostname verification against the SANs is active |
| ALPN | `NextProtos: ["quicraft"]` |
| Curve | `X25519` only |

`InsecureSkipVerify` appears nowhere in non-test `pkg/` code. There is no TCP
listener and no TCP dialer. Cipher suites are not restricted, because Go does
not expose TLS 1.3 suite selection.

The X25519-only curve list is a deliberate ~5% ML-KEM saving on intra-cluster
traffic "where we control both endpoints". **X25519 is not FIPS 140-3
approved**, which couples this pin to the FIPS story below.

### Peer identity is an SPKI pin, not a name

```
identity = "spki:" + hex(sha256(leaf.RawSubjectPublicKeyInfo))
```

Derived from the mTLS-verified leaf and **never from the message body**, then
compared against the committed membership binding. It is deliberately an SPKI
pin rather than a SPIFFE URI SAN because four downstream products issue these
certificates and none of them agree on a naming scheme.

Where no identity is committed for a sender, the fallback is a SAN check against
the committed Raft address. `TransportConfig.RequireMemberIdentity` (default
`false`) drops such messages instead. A peer whose leaf also covers the local
node's host is refused at connection setup — a certificate that can impersonate
you is not a certificate you accept from someone else.

### Revocation fails closed

CRL files hot-reload on SHA-256 content change, polled every `CheckInterval`
(60s). OCSP goes through `OCSPResponderURL` with a 4096-entry cache and a TTL of
`OCSPCacheSeconds` (300). `Mode` is `"crl"`, `"ocsp"`, `"both"` or `"any"`
(default).

**Unknown status fails closed by default, and that default is intrinsic rather
than conferred by one caller**: the internal `revocation.Config` carries
`AllowSoftFail` with the same name and sense as the public one, so its zero
value enforces. A checker built by any other path enforces too. Under
enforcement, a mode with no usable source returns `ErrNoRevocationSources` from
`IsRevoked` rather than "not revoked" — **in every mode, including `"crl"`**.
`Config.Validate` rejects such a configuration at startup; the runtime check is
the backstop for a checker constructed without it.

The check is installed as `VerifyConnection`, not `VerifyPeerCertificate`, so
**resumed sessions are checked too**.

### Memory is budgeted before it is committed

A peer must not be able to make this process allocate by asserting a size.

- `MaxMessageReceiveMemory` (256 MB) is reserved against the declared frame
  length **before the body is read**, so a peer cannot declare a huge frame and
  withhold it.
- That budget counts **wire bytes**. Multiply by `proto.MaxDecodeMemoryFactor`
  (8) for the heap the process may actually hold.
- `MaxSnapshotReceiveMemory` (1 GB), `MaxConcurrentSnapshotReceives` (4) and
  `MaxSnapshotReceiveRate` (256 MB/s) bound snapshot receives independently.
- `MaxIncomingConnections` (256), `MaxConnectionsPerIP` (16) and
  `MaxStreamsPerConnection` (256) bound the connection surface.
- `StreamReadIdleTimeout` (30s) reclaims a stalled receive stream.

### What the transport does not protect

- **Snapshots cross as plaintext over the mTLS session.** The leader decrypts
  its at-rest snapshot to a sibling temp file, streams it, and the receiver
  re-encrypts under its **own** barrier. Nodes cannot decrypt each other's
  at-rest data; confidentiality in transit is TLS's, not the barrier's.
- **The transport is outside the FIPS boundary.**
  `pkg/internal/transport` and `.../revocation` do not pass
  `GODEBUG=fips140=only`, and `-tags fips` does not change that: QUIC's packet
  protection is arbitrary-IV AES-GCM by protocol definition (RFC 9001 §5.3) and
  OCSP's `CertID` is SHA-1 by protocol definition (RFC 6960 §4.1.1). Both live
  in dependencies and neither can be made approved from here. `make verify-fips`
  excludes this tree by an explicit deny-list, so a package added later is
  inside the gate by default.

  **So `-tags fips` makes go-quicraft FIPS at rest, not FIPS on the wire.** A
  process that opens a QUIC connection in a `fips140=only` environment panics
  inside the QUIC library.

## Component boundaries

- **`pkg/internal/transport`** — the QUIC transport, connection and stream
  pools, frame I/O, the mTLS config, snapshot receive.
- **`pkg/internal/transport/revocation`** — CRL and OCSP checking.
- **`pkg/internal/registry`** — `(shardID, replicaID) → address`, over a
  `sync.Map`, lock-free on the `Resolve` hot path.
- **`pkg/internal/netaddr`** — address canonicalization, so two spellings of one
  endpoint compare equal.
- **Consumers**: the `Host`, through `transportSender`.

All four are `internal/`: not importable from outside the module, and no
exported transport type exists.

This feature is part of the go-quicraft capability row covering Raft consensus,
log replication and QUIC transport in `docs/00 §R`.

## What this feature deliberately does not do

- **No TCP, no fallback, no pluggable transport.**
- **No unauthenticated mode.** `WithoutTransport()` disables the transport
  entirely rather than relaxing TLS.
- **No peer discovery, no cluster-formation bootstrap.** Relocated to go-qrdb
  and not to be added back. The registry maps an ID to an address that someone
  else supplied.
- **No certificate issuance, rotation, or CA.** That is go-xkms. This layer
  consumes a `MTLSConfig` and verifies against it.
- **No application-layer encryption of snapshots in transit.** TLS covers the
  session; the barrier covers rest.
- **No authorization beyond membership.** A verified peer that is a committed
  member may send Raft messages. There are no scopes, roles or per-shard
  permissions here.
- **No FIPS-approved wire.** Stated as a limit, not a footnote.
