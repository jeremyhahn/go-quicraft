# QUIC Transport — Architecture

> **Status: NORMATIVE** · Scope: go-quicraft `pkg/internal/transport`,
> `pkg/internal/transport/revocation`, `pkg/internal/registry`,
> `pkg/internal/netaddr` · Last verified: 2026-10-02
> Conflicts with this document are defects. Fix them here first.

## The problem

A host runs thousands of Raft groups over a handful of peer connections. On a
stream-oriented transport they share one byte stream, so one shard's
multi-megabyte replication batch stalls every other shard behind it. A
heartbeat delayed past the election timeout is not a latency problem: it is a
spurious leader election.

## The shape chosen: QUIC only, mTLS mandatory

There is no TCP fallback and no `Transport` interface to swap one in. The
`Host` holds a concrete `*transport.QUICTransport` and wraps it in an internal
`transportSender` that satisfies the engine's `MessageSender`.

QUIC 1.0 (RFC 9000) gives:

- **Multiplexed streams**, so one stalled shard does not head-of-line-block
  the others. This is the reason for the choice; the rest is a bonus.
- **Built-in TLS 1.3.** 0-RTT is available and off by default, because 0-RTT
  data is replayable.
- **Stream 0 reserved for heartbeats**, pre-opened per connection and reused
  across ticks. Heartbeats also ride a separate priority channel drained ahead
  of data, so a large replication batch cannot delay one past the election
  timeout.
- A fixed pool of `StreamPoolSize` (16) data streams per connection, to cut
  mutex contention. `MaxStreamPoolSize` (256) is only the ceiling `Validate`
  checks it against; the pool does not grow at runtime.
- Pooled byte buffers on send and receive.
- `singleflight.Group` coalescing concurrent dials to the same peer.

Snapshot transfers set a dedicated frame flag (`FlagSnapshot`) that routes the
stream to a separate handler, keeping multi-gigabyte streams out of the stream
pool.

### Rejected: TCP with a fallback path

A fallback is a second transport to test, a second set of failure modes, and a
silent downgrade waiting to happen. A cluster that "works" on TCP because the
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
actually needs is per peer, not per shard.

### Rejected: optional mTLS

The transport refuses to start without an mTLS config
(`ErrMTLSConfigRequired`). `TransportConfig.TransportDisabled`, which
`WithoutTransport()` sets, skips that validation, but it also stops the
transport being constructed at all. That is "no transport", not "QUIC without
TLS". No configuration produces an unauthenticated listener.

## Data flow

```
  engine step worker                                  peer
        │  MessageSender.Send(batch)                   │
        ▼                                              │
   transportSender                                     │
        │  registry.Resolve(shardID, replicaID) ──▶ address
        │  Config.ReplicaIdentity(shardID, replicaID) ──▶ destination identity
        ▼                                              │
   connection pool ──── singleflight dial ─────────────┤ mTLS 1.3, ALPN "quicraft",
        │                (peer must present the        │ peer identity pinned
        │                 destination identity)        │
        ├── stream 0 ──── heartbeats (priority chan) ──▶
        ├── stream pool ─ MessageBatch frames ─────────▶
        └── dedicated ─── snapshot header + chunks ────▶

  inbound: accept ─▶ admission limits ─▶ leaf → SPKI identity
                  ─▶ frame header (wire version) ─▶ proto decode
                  ─▶ sender bound to identity ─▶ engine inbox
  snapshot: header ─▶ sender authorized ─▶ receive slot + memory ─▶ chunks
```

Everything crossing the wire is `pkg/proto`'s; see
[`../wire-protocol/architecture.md`](../wire-protocol/architecture.md). The
transport owns framing and the frame header's version byte
(`transport.WireVersion`), and nothing about the payload's meaning.

## Trust boundaries

This is the repository's network trust boundary, so it is stated in full here
rather than by reference.

### The TLS configuration is fixed, not configurable

There is exactly one TLS-config constructor, `buildMTLSTLS`:

| Setting | Value |
|---|---|
| `MinVersion` | TLS 1.3, both directions |
| `ClientAuth` | `RequireAndVerifyClientCert` against `ClientCAs` |
| Server verification | `RootCAs` plus a per-dial `ServerName`, so hostname verification against the SANs is active; the dial is then kept only if the peer presents the node identity membership binds to the destination replica (`Config.ReplicaIdentity`) |
| ALPN | `NextProtos: ["quicraft"]` |
| Curves | build-dependent: `[X25519, P-256, P-384]` standard, `[P-256, P-384]` with `-tags fips` |

`InsecureSkipVerify` appears nowhere in non-test `pkg/` code. There is no TCP
listener and no TCP dialer. Cipher suites are not restricted, because Go does
not expose TLS 1.3 suite selection.

Naming the curve list at all suppresses post-quantum hybrid (ML-KEM)
negotiation, which saves about 5% of handshake cost on intra-cluster traffic
where both endpoints are ours. X25519 leads the standard list because it is the
fastest ECDH curve at 128-bit security.

The list is build-dependent because X25519 is not FIPS 140-3 approved.
`tls_curves_fips.go` compiles it out of a `-tags fips` build;
`tls_curves_nofips.go` keeps it in a standard build and adds the NIST curves
after it. Neither list has a single entry, and that is the part that matters:
TLS 1.3 needs the two lists to intersect, and P-256 is the group they share, so
a cluster can move to FIPS one node at a time.

| client offers ↓ / server offers → | `[X25519, P-256, P-384]` | `[P-256, P-384]` |
|---|---|---|
| `[X25519, P-256, P-384]` | X25519 | P-256, after a HelloRetryRequest |
| `[P-256, P-384]` | P-256 | P-256 |

An `[X25519]`-only list on either side would have an empty intersection with a
FIPS build, and every such handshake would fail with `tls: no supported
elliptic curves for ECDHE`. Because Go filters non-approved groups out of the
offer under `GODEBUG=fips140=on` or `=only`, such a list would also fail every
intra-cluster handshake in a FIPS-mode process.
`TestHandshakeCurves_InteroperateAcrossBuilds` pins every cell above.

### Peer identity is an SPKI pin, not a name

```
identity = "spki:" + hex(sha256(leaf.RawSubjectPublicKeyInfo))
```

It is derived from the mTLS-verified leaf, never from the message body, and
compared against the identity committed membership binds to the replica. It is
an SPKI pin rather than a SPIFFE URI SAN because the certificate issuers this
library is deployed with do not agree on a naming scheme, and a key hash needs
none. Renewing a certificate on the same keypair keeps the identity.

Every sender is bound to its authenticated identity, in both directions:

- **Inbound.** Every message type, heartbeats and vote requests included, is
  accepted only from a connection whose identity is bound to the claimed
  `From`. A replica can vouch for a sender from three facts it holds itself:
  the committed configuration, a pending configuration in its own log that
  admits the sender with an identity, and the operator anchor
  (`Config.MemberIdentities`). Where none names the sender, the message is
  dropped (`missing_identity` or `unknown_member`). There is no address
  fallback.
- **Outbound.** Every message and snapshot is written only to a connection
  whose peer presents the identity membership binds to the destination
  replica. A dial whose peer presents another identity is closed, and a
  message for a replica nothing binds is dropped (`identity_mismatch`).
- **Snapshots.** The snapshot stream header carries the sender, and the host
  authorizes it from the header before the transport takes a receive slot or
  any memory.

Identities are therefore mandatory on every host with a transport: admission
requires one (`config.Config.RequireMemberIdentities`, which the host sets).
A peer whose leaf also covers the local node's host is refused at connection
setup: a certificate that can impersonate you is not one you accept from
someone else.

### Revocation fails closed

CRL files hot-reload on SHA-256 content change, polled every `CheckInterval`
(60s). OCSP goes through `OCSPResponderURL` with a 4096-entry cache and a TTL of
`OCSPCacheSeconds` (300). `Mode` is `"crl"`, `"ocsp"`, `"both"` or `"any"`
(the default).

Unknown status fails closed by default, and the default does not depend on
which caller built the checker: the internal `revocation.Config` carries
`AllowSoftFail` with the same name and sense as the public one, so its zero
value enforces. Under enforcement, a mode with no usable source returns
`ErrNoRevocationSources` from `IsRevoked` rather than "not revoked", in every
mode including `"crl"`. `TransportConfig.Validate` rejects such a
configuration at startup; the runtime check is the backstop for a checker
constructed without it.

A delegated OCSP responder must carry the OCSP-signing extended key usage and
be within its validity period, or the response counts as status-unavailable.
Without that, a revoked node, which still holds its own CA-issued certificate,
could answer its own OCSP query "good" from a position on the path to the
responder.

The check is installed as `VerifyConnection`, not `VerifyPeerCertificate`, so
resumed sessions are checked too. Live connections are re-checked on the 30 s
cleanup tick, so a certificate revoked after admission loses its session
within one tick.

### Memory is budgeted before it is committed

A peer must not be able to make this process allocate by asserting a size.

- `MaxMessageReceiveMemory` (512 MB: four maximum-size frames) is the
  aggregate heap budget for inbound message frames. A frame reserves its
  declared length, plus any decompression delta, times
  `proto.MaxDecodeMemoryFactor` (8), the measured ceiling on heap per wire
  byte, before the body is read, so a peer cannot declare a huge frame and
  withhold it. Each peer is further capped at a quarter of the budget, in the
  same unit, and never below one maximum-size frame.
- `MaxSnapshotReceiveMemory` (1 GB), `MaxConcurrentSnapshotReceives` (4) and
  `MaxSnapshotReceiveRate` (256 MB/s) bound snapshot receives independently,
  with per-peer sub-limits.
- `MaxIncomingConnections` (256), `MaxConnectionsPerIP` (16) and
  `MaxStreamsPerConnection` (256) bound the connection surface.
- `StreamReadIdleTimeout` (30s) bounds how long a read waits at a clean frame
  boundary before re-checking that its connection is alive.

### What the transport does not protect

- **Snapshots cross as plaintext inside the mTLS session.** The leader decrypts
  its at-rest snapshot to an unlinked scratch file, streams it, and the
  receiver re-encrypts under its own barrier. Nodes cannot decrypt each other's
  at-rest data; confidentiality in transit is TLS's, not the barrier's. It does
  not rest on SAN exclusivity: every outbound write is pinned to the node
  identity membership binds to the destination replica, so a wildcard or
  shared-SAN certificate held by anyone on the path to a peer's address cannot
  receive its traffic.
- **One SHA-1 identifier on the OCSP path is outside approved-only.** See the
  exemption record below. It is the OCSP CertID hash, which RFC 6960 mandates
  and which protects nothing. A deployment configured with `Mode: "crl"` never
  computes it. Everything else in this tree, packet protection included, runs
  under `GODEBUG=fips140=only`.

## Approved key exchange, and how a running cluster gets there

The handshake groups are chosen at build time, not negotiated at cluster
formation, and the two builds interoperate:

| Build | Groups offered (`handshakeCurves()`) |
|---|---|
| `-tags fips` | `P-256`, `P-384` |
| standard | `X25519`, `P-256`, `P-384` |

X25519 is absent from the FIPS list rather than deprioritised. Under
`GODEBUG=fips140=on` Go removes it from the offer anyway, and a list holding
nothing else would fail every handshake with `tls: no supported elliptic curves
for ECDHE`.

### Why there is no cluster-formation negotiation

TLS 1.3 already negotiates, per connection. A second negotiation at cluster
formation would duplicate the handshake's own group selection and give a
cluster two places to disagree about its own cryptography. The build tag
decides what a node will offer; TLS decides what a given pair will use.

### Rolling migration, with no flag day

The two lists intersect on `P-256` and `P-384`, so a `-tags fips` node and a
standard node complete a handshake in either direction. A cluster migrates one
node at a time:

1. Replace nodes with `-tags fips` builds one by one, in any order. Each new
   node handshakes with the remaining standard nodes, and with other upgraded
   nodes, on P-256.
2. No step requires quorum loss, a coordinated restart, or a window in which
   part of the cluster cannot reach the rest.
3. Once every node is a `-tags fips` build, nobody offers X25519 and the
   cluster's key exchange is approved end to end.

Rolling back works the same way. No persisted state is keyed to the curve, so a
node may move in either direction at any time.

`TestHandshakeCurves_InteroperateAcrossBuilds` pins all three directions (each
build dialling the other, and itself). `TestHandshakeCurves_DisjointListsFail`
shows the first test is not vacuous: a handshake between disjoint lists does
fail, so a passing interoperability test means an intersection exists.

### What this migration does not achieve

It makes key exchange approved as a property of the binary, and nothing more.
Under `GODEBUG=fips140=on` or `=only` Go already filters X25519 out of the
offer, so the module is satisfied either way; the build tag adds that a
`-tags fips` binary cannot offer X25519 even with the module inactive. That is
the same absence-not-refusal shape `pkg/seal` uses, but it is not what makes a
deployment approved-only.

The rollout does not supply a `quic-go` at v0.60.0 or later (for approved
packet protection) or a revocation `Mode` that avoids OCSP. See
[the approved-only recipe](#the-approved-only-recipe): the rollout is necessary,
not sufficient.

## FIPS 140-3 exemption record

This is the authoritative record of why `pkg/internal/transport/revocation/...`
is the one package tree `make verify-fips` does not judge, and what covers it
instead. It is not a waiver for that tree's cryptography: `make
verify-fips-exempt` runs it under `GODEBUG=fips140=on`, so everything on the
revocation path other than the one primitive below is still proven approved.

The QUIC transport itself is inside the approved-only gate.

### Packet protection is approved from quic-go v0.60.0

RFC 9001 §5.3 derives each packet's nonce by XOR-ing the packet number into a
fixed IV, a caller-supplied IV that `cipher.NewGCMWithRandomNonce` cannot
express. Before v0.60.0, `quic-go` reached that construction through the
general `cipher.NewGCM`, which `fips140=only` declines, so a process panicked
constructing the initial AEAD before any handshake.

From v0.60.0, `quic-go` builds packet-protection AEADs through Go's TLS 1.3
AES-GCM implementation instead. In v0.63.0 that is
`internal/handshake/cipher_suite.go`, whose `aeadAESGCMTLS13` dispatches to
`aeadAESGCMTLS13FIPS140` when `crypto/fips140.Enabled()`: the same approved
path `crypto/tls` uses for its own records, for the same construction. The wire
format did not change.

How it reaches that path matters to an assessor. The standard library does not
yet export a constructor for it, so
`internal/handshake/cipher_suite_fips140.go` uses `go:linkname` to bind the
unexported `crypto/tls.aeadAESGCMTLS13`. Three consequences:

- The primitive executing is `crypto/tls`'s own AES-GCM, inside the Go
  Cryptographic Module. The linkname changes who can call it, not what it is.
- It depends on a Go toolchain that has that symbol, not only on a `quic-go`
  version. `quic-go` v0.63.0 declares `go 1.26.0`; this repository requires
  Go 1.27.0.
- It is a stopgap. `quic-go`'s own comment cites
  [golang/go#79219](https://github.com/golang/go/issues/79219) and the pending
  `NewGCMForQUIC` constructor; when the standard library exports one, the
  linkname goes away. A toolchain bump is something to re-measure this gate
  against, which `make verify-fips` does on every nightly run.

go-quicraft requires `quic-go` v0.63.0, and the dispatch, the linkname and the
`go` directive above were re-read from that version's source. Measured
2026-09-03 with real QUIC handshakes over loopback:

| | v0.59.1 | v0.62.0 |
|---|---|---|
| `pkg/internal/transport` under `fips140=only` | panics on the first dial | ok, 155.6s |

Re-measured 2026-09-04 against live clusters. Under `GOFIPS140=latest
GODEBUG=fips140=only go test -tags integration,fips`, three- and five-node
clusters form, elect a leader, agree on it across every node, and replicate:
single entries, concurrent proposals, large values, multiple shards, and stale
reads from a follower. `TestE2E_ThreeNodeCluster_LeaderElection` and
`TestE2E_FiveNode_Replication_ConcurrentProposals` are the ends of that range.

v0.60.0 is therefore the minimum version for an approved-only deployment. A
downgrade below it silently reintroduces the exemption, which is why the floor
is stated here and not only in `go.mod`.

### The one remaining exemption

| | Exemption |
|---|---|
| Primitive | SHA-1 |
| Where | `golang.org/x/crypto/ocsp.CreateRequest` |
| Reached from | `revocation.Checker.queryOCSP`, `pkg/internal/transport/revocation/checker.go:942` |
| Mandated by | RFC 6960 §4.1.1, `CertID.issuerNameHash` / `issuerKeyHash` |
| What it protects | nothing; the hash is an issuer identifier, matched against a responder's index |
| Fixable here? | No. It is in a dependency, and a compliant responder rejects any other hash |
| Avoidable here? | Yes, by configuration. See below |
| Behaviour under `fips140=only` | panics inside `CreateRequest` |
| Behaviour under `fips140=on` | permitted |

It is not a compliance defect under FedRAMP, PCI DSS or HIPAA: those frameworks
govern the protection of regulated data, and this hash carries none. It names
an issuer.

### The exemption is avoidable by configuration

`queryOCSP` is the only call site, and it runs only when the checker actually
queries a responder. `checkOCSPRaw` returns early when `OCSPResponderURL` is
empty, and `IsRevoked` dispatches on `Mode`:

| `Mode` | Reaches SHA-1? |
|---|---|
| `crl` | never, in any configuration |
| `ocsp` | yes; `Validate` requires a responder URL for this mode |
| `both` | yes; same requirement |
| `any` (the default) | only when `OCSPResponderURL` is set |

So `Mode: "crl"` is a supported configuration under which go-quicraft runs
fully approved-only, with no exemptions. That is a deployment choice, not a
limit of the library.

### The approved-only recipe

Four things, all checkable:

1. `quic-go` at v0.60.0 or later. go-quicraft requires v0.63.0.
2. Build with `-tags fips`. This is not for the curve list: Go filters X25519
   out of the offer under `fips140` mode by itself, and `TestHandshakeCurves`
   passes untagged under `only`. The tag is for the seal layer: Argon2id and
   ChaCha20-Poly1305 are compiled out rather than refused at runtime. It is
   also the only combination `make verify-fips` exercises.
3. Run with `GOFIPS140=latest GODEBUG=fips140=only`.
4. Configure revocation `Mode: "crl"` with at least one CRL path, so the OCSP
   CertID is never computed.

Drop (1), (3) or (4) and the deployment is no longer approved-only; dropping
(4) alone leaves exactly the residual this record covers. Drop (2) and it still
is, on the module's own enforcement. What you lose is the guarantee that
non-approved primitives are absent rather than merely refused, and the
assurance that what you shipped is what CI exercises. Keep it.

### Acceptance

The exemption above is accepted for the FedRAMP claim. It was accepted when the
residual covered two primitives; QUIC packet protection has since been removed
outright, so the acceptance applies to a strictly smaller residual than the one
it was granted for.

The grounds are the ones the table states: the OCSP CertID is an issuer
identifier matched against a responder's index, and carries no regulated data.

This record lives here, rather than only in an issue tracker, because it is the
artifact an assessor reads. Three things an assessor can check without taking
anyone's word:

1. **The scope is bounded and enforced.** `make verify-fips` excludes exactly
   one package tree, by a deny-list keyed on a single Makefile variable
   (`FIPS_EXCLUDED`), and `test/buildgates/fips_gate_test.go` fails if that
   list grows, if the tree it names is widened to the whole transport, if the
   mode is weakened from `only`, or if the excluded tree stops being run by
   `make verify-fips-exempt`.
2. **The excluded tree is still gated**, one notch lower, at
   `GODEBUG=fips140=on`. `on` loads the validated module, so it proves
   everything on the revocation path apart from the cited hash is approved.
3. **Everything else is approved-only, the QUIC transport included.** The seal
   layer, the WAL, snapshots and packet protection run under
   `GODEBUG=fips140=only` and are proven to.

What this acceptance does not say: that an OCSP-configured deployment is
approved-only. It is not, and that is what the acceptance covers. A deployment
that must be approved-only end to end does not need to terminate this
transport outside the boundary; it configures `Mode: "crl"`.

Terminating the transport outside the boundary was the alternative considered
and rejected on cost. It is now unnecessary on the merits as well.

## Component boundaries

- **`pkg/internal/transport`**: the QUIC transport, connection and stream
  pools, frame I/O, the mTLS config, snapshot receive.
- **`pkg/internal/transport/revocation`**: CRL and OCSP checking.
- **`pkg/internal/registry`**: `(shardID, replicaID) → address`, over a
  `sync.Map`, lock-free on the `Resolve` hot path.
- **`pkg/internal/netaddr`**: address canonicalization, so two spellings of one
  endpoint compare equal.
- **Consumer**: the `Host`, through `transportSender` and the host's
  `MessageHandler` implementation, which owns sender authorization.

All four are `internal/`: not importable from outside the module. The only
exported surface is configuration in `pkg/config`.

## What this feature deliberately does not do

- **No TCP, no fallback, no pluggable transport.**
- **No unauthenticated mode.** `WithoutTransport()` disables the transport
  entirely rather than relaxing TLS.
- **No peer discovery, no cluster-formation bootstrap.** That belongs to the
  layer above (go-qrdb) and is not to be added here. The registry maps an ID to
  an address someone else supplied.
- **No certificate issuance, rotation or CA.** That is go-xkms. This layer
  consumes an `MTLSConfig` and verifies against it.
- **No application-layer encryption of snapshots in transit.** TLS covers the
  session; the barrier covers rest.
- **No authorization beyond membership.** A verified peer whose identity is
  bound to a member may send that member's Raft messages. There are no scopes,
  roles or per-shard permissions here.
- **No approved-only OCSP.** The transport runs under `GODEBUG=fips140=only`,
  packet protection and key exchange included, but the OCSP CertID is SHA-1 by
  protocol definition (RFC 6960 §4.1.1) inside `golang.org/x/crypto/ocsp`.
  Revocation `Mode: "crl"` avoids it entirely; `ocsp`, `both`, and `any` with
  a responder URL do not. See
  [FIPS 140-3 exemption record](#fips-140-3-exemption-record).
