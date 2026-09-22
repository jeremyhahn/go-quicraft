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

Where no identity is committed for a sender, the message is dropped
(`missing_identity`); there is no address fallback, and admission requires the
identity on every host with a transport (`config.Config.RequireMemberIdentities`).
A peer whose leaf also covers the local node's host is refused at connection
setup — a certificate that can impersonate you is not a certificate you accept
from someone else.

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
- **One SHA-1 identifier on the OCSP path is outside approved-only.** See the
  exemption record below. It is the OCSP CertID hash, which RFC 6960 mandates
  and which protects nothing; a deployment configured `Mode: "crl"` never
  computes it. Everything else in this tree, packet protection included, runs
  under `GODEBUG=fips140=only`.

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

Approved KEY EXCHANGE as a property of the BINARY, and nothing beyond it. Under
`GODEBUG=fips140=on` or `=only` Go already filters X25519 out of the offer, so
the module is satisfied either way; what the build tag adds is that a
`-tags fips` binary cannot offer X25519 even with the module inactive. That is
the same absence-not-refusal shape `pkg/seal` uses, and it is worth having, but
it is not what makes a deployment approved-only.

Two things this rollout does not supply: a `quic-go` at v0.60.0 or later (for
approved packet protection) and a revocation `Mode` that does not consult OCSP.
See [the approved-only recipe](#the-approved-only-recipe) below — completing
this rollout alone is necessary, not sufficient.

## FIPS 140-3 exemption record

This is the authoritative record of why `pkg/internal/transport/revocation/...`
is the one package tree `make verify-fips` does not judge, and what covers it
instead. It is not a waiver for that tree's cryptography: `make
verify-fips-exempt` runs it under `GODEBUG=fips140=on`, so everything on the
revocation path OTHER than the one primitive below is still proven approved.

**The QUIC transport itself is inside the approved-only gate.** That changed in
#85, and the paragraph that used to stand here said the opposite.

### What changed: packet protection stopped being an exemption

Until `quic-go` v0.59.1 this record listed two exemptions, and the first was
QUIC packet protection. RFC 9001 §5.3 derives each packet's nonce by XOR-ing the
packet number into a fixed IV — a caller-supplied IV, which
`cipher.NewGCMWithRandomNonce` cannot express. `quic-go` reached it through the
general `cipher.NewGCM`, which `fips140=only` declines, so a process panicked
constructing the initial AEAD before any handshake.

`quic-go` **v0.60.0** fixed this by constructing packet-protection AEADs through
Go's TLS 1.3 AES-GCM implementation instead of the general API. In v0.63.0 it is
`internal/handshake/cipher_suite.go`, whose `aeadAESGCMTLS13` dispatches to
`aeadAESGCMTLS13FIPS140` when `crypto/fips140.Enabled()` — the same approved path
`crypto/tls` uses for its own records, for the same construction. The old record
asserted that "no version of any QUIC library can differ." That was true of the
public API surface available in 2026-08 and it is false now; the fix was to stop
using the public API, not to change the wire format.

**How it reaches that path, and why that matters to an assessor.** The standard
library does not yet export a constructor for it, so
`internal/handshake/cipher_suite_fips140.go` uses `go:linkname` to bind the
unexported `crypto/tls.aeadAESGCMTLS13`. Three consequences worth stating rather
than discovering:

- The primitive executing is `crypto/tls`'s own AES-GCM, inside the Go
  Cryptographic Module. The linkname changes who can call it, not what it is.
- It therefore depends on a **Go toolchain** that has that symbol, not only on a
  `quic-go` version. `quic-go` v0.63.0 declares `go 1.26.0`; this repository
  pins 1.27.0.
- It is explicitly a stopgap. `quic-go`'s own comment cites
  [golang/go#79219](https://github.com/golang/go/issues/79219) and the pending
  `NewGCMForQUIC` constructor; when the standard library exports one, the
  linkname goes away. A toolchain bump is therefore a thing to re-measure this
  gate against, not to assume through — which `make verify-fips` does on every
  nightly run.

go-quicraft moved to v0.62.0 in #85, and to v0.63.0 on 2026-09-22 with the
dispatch, the linkname and the `go` directive above re-read from its source.
Measured on 2026-09-03 with real QUIC handshakes over loopback:

| | v0.59.1 | v0.62.0 |
|---|---|---|
| `pkg/internal/transport` under `fips140=only` | panics on the first dial | **ok, 155.6s** |

Re-measured 2026-09-04 against live clusters rather than unit suites. Under
`GOFIPS140=latest GODEBUG=fips140=only go test -tags integration,fips`, three-
and five-node clusters form, elect a leader, agree on it across every node, and
replicate — single entry, concurrent proposals, large values, multiple shards,
and stale reads from a follower. `TestE2E_ThreeNodeCluster_LeaderElection` and
`TestE2E_FiveNode_Replication_ConcurrentProposals` are the ends of that range.
This is the demonstration #27
asked for and, on v0.59.1, could not have had.

`v0.60.0` is therefore the **minimum** version for an approved-only deployment.
A downgrade below it silently reintroduces the exemption, which is why the
version floor is stated here rather than only in `go.mod`.

### The one remaining exemption

| | Exemption |
|---|---|
| Primitive | SHA-1 |
| Where | `golang.org/x/crypto/ocsp.CreateRequest` |
| Reached from | `revocation.Checker.queryOCSP`, `pkg/internal/transport/revocation/checker.go:938` |
| Mandated by | RFC 6960 §4.1.1 — `CertID.issuerNameHash` / `issuerKeyHash` |
| What it protects | nothing; the hash is an ISSUER IDENTIFIER, matched against a responder's index |
| Fixable here? | No. In a dependency, and a compliant responder rejects any other hash |
| **Avoidable here?** | **Yes — by configuration. See below.** |
| Behaviour under `fips140=only` | panics inside `CreateRequest` |
| Behaviour under `fips140=on` | permitted |

It is not a compliance defect under FedRAMP, PCI DSS or HIPAA: those frameworks
govern the protection of regulated data, and this hash carries none — it names
an issuer.

### The exemption is avoidable by configuration

`queryOCSP` is the only call site, and it is reached only when this checker
actually queries a responder. `checkOCSPRaw` returns early when
`OCSPResponderURL` is empty, and `IsRevoked` dispatches on `Mode`:

| `Mode` | reaches SHA-1? |
|---|---|
| `crl` | **never**, at any configuration |
| `ocsp` | yes — `Config.Validate` requires a responder URL for this mode |
| `both` | yes — same requirement |
| `any` (the default) | only when `OCSPResponderURL` is set; never when it is empty |

So `Mode: "crl"` is a supported configuration under which go-quicraft runs
**fully approved-only, with no exemptions at all**. That is a stronger posture
than this document claimed before #85, and it is a deployment choice rather than
a limit of the library.

### The approved-only recipe

Four things, all of them checkable:

1. `quic-go` **≥ v0.60.0** — go-quicraft pins v0.63.0.
2. Build with **`-tags fips`**. Measured caveat, so the reason is not mistaken
   for the curve list: Go filters X25519 out of the offer under `fips140` mode
   by itself, and `TestHandshakeCurves` passes untagged under `only`. What the
   tag buys is the SEAL layer — Argon2id and ChaCha20-Poly1305 are compiled out
   rather than refused at runtime, which is the absence-not-refusal shape
   `pkg/seal` uses deliberately. It is also the only combination
   `make verify-fips` exercises.
3. Run with **`GOFIPS140=latest GODEBUG=fips140=only`**.
4. Configure revocation **`Mode: "crl"`** with at least one CRL path, so the
   OCSP CertID is never computed.

Drop (1), (3) or (4) and the deployment is no longer approved-only; dropping (4)
alone leaves exactly the residual this record covers. Drop (2) and it still is,
on the module's own enforcement — what you lose is the guarantee that the
non-approved primitives are ABSENT rather than merely refused, and the assurance
that the configuration you shipped is the one CI exercises. Keep it.

### Acceptance — owner ruling, 2026-09-03, re-recorded against the smaller residual

**The exemption above is ACCEPTED for the FedRAMP claim.** Ruled by the owner on
2026-09-03, closing #27.

The ruling was granted against **two** exemptions. One of them — QUIC packet
protection — has since been removed outright rather than accepted, so the
acceptance now applies to a strictly smaller residual than it was asked about.
It is re-recorded here rather than re-litigated: an acceptance that covered two
primitives covers one of them a fortiori.

The grounds are the ones the table states: the exemption does not protect
regulated data. The OCSP CertID is an issuer identifier matched against a
responder's index. FedRAMP, PCI DSS and HIPAA govern the protection of regulated
data; this construction carries none.

Recorded here rather than only in the issue tracker because this is the artifact
an assessor reads. Three things an assessor should be able to check without
taking anyone's word:

1. **The scope is bounded and enforced.** `make verify-fips` excludes exactly one
   package tree, by a deny-list keyed on a single Makefile variable, and
   `test/buildgates/fips_gate_test.go` fails if that list grows, if the tree it
   names is widened back to the whole transport, if the mode is weakened from
   `only`, or if the excluded tree stops being run by `make verify-fips-exempt`.
2. **The excluded tree is still gated**, one notch lower, at
   `GODEBUG=fips140=on`. `on` still loads the validated module, so it proves
   everything on the revocation path apart from the cited hash is approved.
3. **Everything else is approved-only, the QUIC transport included.** The seal
   layer, the WAL, snapshots, and packet protection all run under
   `GODEBUG=fips140=only` and are proven to.

**What this acceptance does NOT say.** It does not claim that an OCSP-configured
deployment is approved-only — it is not, and that is what the acceptance
covers. It no longer needs to say anything about the wire: since #85 the
transport is approved-only, and a deployment that must be approved-only end to
end **no longer has to terminate this transport outside the boundary**. The
sentence that said it did was true of v0.59.1 and has been removed.

The alternative considered in the original ruling — terminating the transport
outside the boundary — was rejected on cost. It is now unnecessary on the
merits as well.

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
- **No approved-only OCSP.** The transport runs under `GODEBUG=fips140=only`,
  packet protection and key exchange included, but the OCSP CertID is SHA-1 by
  protocol definition (RFC 6960 §4.1.1) inside `golang.org/x/crypto/ocsp`.
  Revocation `Mode: "crl"` avoids it entirely; `ocsp`, `both`, and `any` with a
  responder URL do not. See
  [FIPS 140-3 exemption record](#fips-140-3-exemption-record).
