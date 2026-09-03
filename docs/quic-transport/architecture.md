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
| Curve | build-dependent: `[X25519, P-256, P-384]` standard, `[P-256, P-384]` with `-tags fips` |

`InsecureSkipVerify` appears nowhere in non-test `pkg/` code. There is no TCP
listener and no TCP dialer. Cipher suites are not restricted, because Go does
not expose TLS 1.3 suite selection.

Pinning the curve list at all is a deliberate ~5% ML-KEM saving on
intra-cluster traffic "where we control both endpoints": naming any group
suppresses post-quantum hybrid negotiation, and X25519 is the fastest ECDH
curve at 128-bit security.

**Which** groups is the one build-dependent value in the table, because
**X25519 is not FIPS 140-3 approved** and the list used to hold nothing else.
`tls_curves_fips.go` compiles it out of a `-tags fips` build; `tls_curves_nofips.go`
keeps it in a standard build and adds the NIST curves after it. Neither list has
a single entry, and that is the load-bearing part: TLS 1.3 needs the two lists
to INTERSECT, and P-256 is the group they share, so a cluster can be converted
to FIPS one node at a time instead of all at once.

| client offers ↓ / server offers → | `[X25519, P-256, P-384]` | `[P-256, P-384]` |
|---|---|---|
| `[X25519, P-256, P-384]` | X25519 | P-256, after a HelloRetryRequest |
| `[P-256, P-384]` | P-256 | P-256 |

An `[X25519]`-only list on either side would have an empty intersection with a
FIPS build, and every such handshake fails with `tls: no supported elliptic
curves for ECDHE`. That is not hypothetical — it is what the list WAS, and
because Go filters non-approved groups out of the offer, it also meant a
process running under `GODEBUG=fips140=on` or `=only` could not complete a
single intra-cluster handshake. `TestHandshakeCurves_InteroperateAcrossBuilds`
pins every cell above.

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
- **The transport runs in FIPS mode but is not approved-only.** See the
  exemption record below. `-tags fips` makes go-quicraft FIPS **at rest**, and
  a process that opens a QUIC connection under `GODEBUG=fips140=only` still
  panics inside the QUIC library.

## Approved key exchange, and how a running cluster gets there

The handshake groups are chosen at BUILD time, not negotiated at cluster
formation, and the two builds are deliberately designed to interoperate:

| build | groups offered (`handshakeCurves()`) |
|---|---|
| `-tags fips` | `P-256`, `P-384` |
| standard | `X25519`, `P-256`, `P-384` |

X25519 is **absent** from the FIPS list rather than deprioritised, which is the
whole point: `GODEBUG=fips140=on` removes it from the offered groups anyway, and
a list holding nothing else fails EVERY handshake with `tls: no supported
elliptic curves for ECDHE`. That is not hypothetical — it is the defect the
`fips140=only` gate caught, and the reason the list is split by build tag at all.

### Why there is no cluster-formation negotiation

Because TLS 1.3 already does it, per connection. Adding a second negotiation at
cluster formation would duplicate the handshake's own group selection and give
two places for a cluster to disagree about its own cryptography. The build tag
decides what a node will OFFER; TLS decides what a given pair will USE.

### Rolling migration, with no flag day

The two lists intersect on `P-256` and `P-384`, so a `-tags fips` node and a
standard node complete a handshake with each other in **either** direction. A
cluster therefore migrates one node at a time:

1. Replace nodes with `-tags fips` builds one by one, in any order. Each new
   node handshakes with the remaining standard nodes on P-256, and with other
   upgraded nodes on P-256 as well.
2. No step requires quorum loss, a coordinated restart, or a window in which
   part of the cluster cannot talk to the rest.
3. Once every node is a `-tags fips` build, X25519 is offered by nobody and the
   cluster's key exchange is approved end to end.

Rolling BACK works the same way for the same reason. There is no persisted
state keyed to the curve, so a node may move in either direction at any time —
the choice lives entirely in the binary.

`TestHandshakeCurves_InteroperateAcrossBuilds` pins all three directions (each
build dialling the other, and itself), and `TestHandshakeCurves_DisjointListsFail`
pins that the first test is not vacuous: a handshake between genuinely disjoint
lists really does fail, so interoperability passing means an intersection exists
rather than that the harness accepts anything.

### What this migration does NOT achieve

Approved KEY EXCHANGE, and nothing beyond it. Completing the rollout above does
not make the transport approved-only: the packet protection and OCSP exemptions
in the record below are properties of the QUIC and OCSP standards, not of the
curve list, and no cluster configuration reaches them. A deployment that must be
approved-only end to end still has to terminate this transport outside the
boundary.

## FIPS 140-3 exemption record

This is the authoritative record of why `pkg/internal/transport/...` is the one
package tree `make verify-fips` does not judge, and what covers it instead. It
is not a waiver for the tree's own cryptography: the key exchange above is
approved in a `-tags fips` build, and `make verify-fips-transport` runs the
whole tree under `GODEBUG=fips140=on`, which is a gate that has already caught
a real defect.

| | Exemption 1 | Exemption 2 |
|---|---|---|
| Primitive | AES-GCM with a caller-supplied IV | SHA-1 |
| Where | `quic-go` packet protection (`internal/handshake`) | `golang.org/x/crypto/ocsp.CreateRequest` |
| Reached from | every QUIC connection | `revocation.Checker.queryOCSP` |
| Mandated by | RFC 9001 §5.3 — each nonce is the packet number XORed into a fixed IV, which `cipher.NewGCMWithRandomNonce` cannot express | RFC 6960 §4.1.1 — `CertID.issuerNameHash` / `issuerKeyHash` |
| What it protects | nothing regulated; it is the standard's own key-derivation construction | nothing; the hash is an ISSUER IDENTIFIER, matched against a responder's index |
| Fixable here? | No. In a dependency, and no version of any QUIC library can differ — the wire format requires it | No. In a dependency, and a compliant responder rejects any other hash |
| Behaviour under `fips140=only` | panics constructing the initial AEAD, before any handshake | panics inside `CreateRequest` |
| Behaviour under `fips140=on` | permitted | permitted |

Neither is a compliance defect under FedRAMP, PCI DSS or HIPAA: those
frameworks govern the protection of regulated data, and one of these is the
protocol's own AEAD construction while the other is an identifier. What they
do mean is that go-quicraft is **FIPS at rest, not FIPS on the wire** — a
deployment that must be approved-only end to end has to terminate this
transport outside the boundary.

The scope is exactly one tree and no more. `make verify-fips` filters it out by
a deny-list keyed on a single Makefile variable, so a package added tomorrow is
inside the approved-only gate by default, and
`test/buildgates/fips_gate_test.go` fails if that list ever grows, if the mode
is weakened from `only`, or if the excluded tree stops being run by
`make verify-fips-transport`.

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
- **No approved-only wire.** The transport runs under `GODEBUG=fips140=on` and
  its own key exchange is approved, but two protocol-mandated primitives in
  dependencies keep it out of `fips140=only`. Stated as a limit, not a
  footnote — see [FIPS 140-3 exemption record](#fips-140-3-exemption-record).
