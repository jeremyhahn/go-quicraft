# QUIC Transport — Usage

> **Status: NORMATIVE** · Scope: go-quicraft `pkg/internal/transport` and its
> configuration surface · Last verified: 2026-10-13
> Conflicts with this document are defects. Fix them here first.

You deal with the transport in four ways: giving it certificates, giving every
member its identity, sizing its limits, and diagnosing why two nodes will not
talk. There is nothing to install and no daemon to run; it is a UDP listener
inside your process.

## Before you start

- A UDP path between every pair of nodes. QUIC runs over UDP; a firewall that
  allows TCP on the port but not UDP produces a cluster that never forms and
  logs nothing more specific than dial failures.
- A CA and one certificate, on its own keypair, per node. There is no
  self-signed or skip-verification mode.
- `HostConfig.ListenAddress` for the local bind, and `HostConfig.RaftAddress`
  for the address peers dial. They differ whenever the node is behind NAT or in
  a container.

## Task: issue certificates that will work

Each node's leaf must:

1. Be signed by the CA in `MTLSConfig.CACert`, which is used as both
   `RootCAs` and `ClientCAs`; the same bundle verifies in both directions.
2. Carry a SAN covering the address other nodes dial it at: the `RaftAddress`,
   not the bind address. Hostname verification is on.
3. Not cover any other member's host. `StartShard` refuses to start when your
   leaf covers another member's host, and a peer whose leaf covers yours is
   refused at connection setup. Wildcard and shared certificates do not work.

```go
cfg.TransportConfig.MTLSConfig = &config.MTLSConfig{
	CACert: caPEM,
	Cert:   leafPEM,
	Key:    keyPEM,
}
```

If the key lives in a TPM or HSM, set `MTLSConfig.Certificate` to a
`*tls.Certificate` whose `PrivateKey` is the device's `crypto.Signer` instead of
`Cert` and `Key`.

There is no way to run without mTLS material. Omitting it fails with
`config.ErrMTLSConfigRequired`; `WithoutTransport()` disables the transport
entirely rather than relaxing it.

## Task: renew a certificate or replace the CA

Reload the new material in place. A node does not need to restart:

```go
err := host.ReloadMTLS(&config.MTLSConfig{CACert: caPEM, Cert: certPEM, Key: keyPEM})
```

The leaf must be issued for the node's existing key, because the key is the
node's identity (next task). A refused reload returns `*MTLSReloadError` and
leaves the old material in force. New handshakes use the new material right
away. Connections that are already up keep theirs until they reconnect. To
replace the CA itself, follow the three-step
[CA rollover runbook](ca-rollover.md). A node reloaded out of order refuses
its peers.

## Task: give every member its identity

A member is authorized by its node identity and by nothing else. A peer whose
identity no configuration names has every message dropped, and a message is
written only to the peer presenting the identity bound to its destination. The
identity is derived from the node's public key and reported by
`Host.NodeIdentity()`:

```go
self := host.NodeIdentity() // "spki:<hex>"
```

Every admission names it:

- **Founding a shard**: `Config.MemberIdentities` maps every replica in the
  members map to its node's identity.
- **Joining**: `Config.MemberIdentities` names the replicas the joiner will
  accept its bootstrap snapshot from.
- **Adding later**: `SyncRequestAddNodeWithIdentity`,
  `SyncRequestAddObserverWithIdentity` and `SyncRequestAddWitnessWithIdentity`
  (and their async `Request*` forms). The identity-less `RequestAddNode`,
  `RequestAddObserver` and `RequestAddWitness` return
  `ErrMemberIdentityRequired` on a host with a transport, unless the
  configuration already binds that replica (a role change such as promoting an
  observer keeps its existing binding).

```go
err := host.SyncRequestAddNodeWithIdentity(ctx, shardID, replicaID,
	"node4.example.com:5000", node4Identity, 0)
```

There is no setting for this; it follows from the host having a transport.
Renewing a certificate on the same keypair keeps the identity. A new keypair is
a new identity, and a restarted host must present the identity the shard
committed for it. The full procedure is in
[`../compatibility/architecture.md`](../compatibility/architecture.md#node-identities-are-mandatory).

## Task: turn on revocation

```go
cfg.TransportConfig.RevocationConfig = &config.RevocationConfig{
	CRLPaths:      []string{"/etc/quicraft/crl/issuing.crl"},
	Mode:          "crl",
	CheckInterval: 60 * time.Second,
}
```

Understand the default before you enable it: unknown status fails closed. A
mode with no usable source refuses connections rather than treating them as
unrevoked, in every mode including `"crl"`. That is the correct behaviour, and
it will take your cluster down if the CRL file goes missing.

Validation rejects a configuration with no source at startup, so the common
failure is a CRL that disappears later. If you ship CRLs separately from the
binary, have them present before the process starts and keep them present.

`AllowSoftFail: true` opts out. It is a real tradeoff, not a convenience flag:
it admits peers whose status cannot be determined, so a revoked node can keep
connecting by making the responder look unreachable.

CRLs hot-reload on content change, so replacing the file is enough; no restart
and no signal. Live connections are re-checked every 30 seconds, so a revoked
peer loses its existing sessions too.

## Task: size the limits

The defaults suit an ordinary cluster. The ones worth revisiting:

```go
cfg.TransportConfig.UDPRecvBufSize = 7 << 20 // the default; negative leaves the OS default, usually far smaller
cfg.TransportConfig.UDPSendBufSize = 7 << 20
cfg.TransportConfig.MessageReceiveBudgetFrames = 4 // or set MaxMessageReceiveMemory outright
cfg.TransportConfig.MaxSnapshotReceiveRate = 256 << 20
```

Two things people get wrong:

- **`MaxMessageReceiveMemory` is a heap budget, and each frame is charged
  eight times its wire size.** That factor is the measured ceiling on how much
  larger a decoded frame is than its bytes, so one maximum-size 16 MB frame is
  charged 128 MB. The default is derived in that unit:
  `MessageReceiveBudgetFrames` (4) maximum-size frames, 512 MB, which admits at
  most 64 MB of frames in flight. Raise the frame count, or set the budget
  outright, if many peers send large batches at once; a budget below 128 MB is
  rejected at startup, because it could never admit a maximum-size frame. One
  peer may hold at most a quarter of the budget, never less than one
  maximum-size frame. The budget also sets each connection's QUIC receive
  window (budget divided by `MaxIncomingConnections`, between 8 MB and 32 MB).
  On long fat links, 10 GbE beyond about 7 ms RTT, raise it or lower
  `MaxIncomingConnections`.
- **Raising `UDPRecvBufSize` above the kernel maximum silently clamps.** On
  Linux, check `net.core.rmem_max` and `net.core.wmem_max`; a refused
  `setsockopt` is logged at WARN and the OS default is used.

Leave `Enable0RTT` off. 0-RTT data is replayable, the transport never sends
early data, and it refuses message frames and snapshots that arrive over it,
so turning it on buys nothing.

## Task: watch the right signals

The transport's internals are not exported. You see it through the
`MetricsCollector` you pass with `WithMetrics` and the `EventListener` in
`HostConfig`:

- **`IncMessagesDropped` with reason `queue_full`** is the leading indicator. A
  peer that cannot keep up shows there well before it shows up as a spurious
  leader election.
- **`IncUnauthorizedMessageDropped` with `identity_mismatch`** means a peer
  presented an identity other than the one committed for the replica it
  claims. That is a forged sender or a key rotated without updating
  membership; it is never routine.
- **`IncMessagesDropped` with `identity_mismatch`** is the outbound side: the
  peer at a replica's address does not present that replica's identity.
- **`OnConnectionFailed`** fires for every failed dial, TLS refusals included.

## Task: run without a transport

```go
host, err := quicraft.NewHost(cfg, quicraft.WithoutTransport(), quicraft.WithoutEncryption())
```

For single-node embedded deployments and tests. The QUIC transport is not
constructed, mTLS validation is skipped, and member identities are not
required, because nothing crosses a wire.

## Task: choose a FIPS mode

If you deploy into a FIPS-regulated environment, this is the decision to make.

### The three modes

`GODEBUG=fips140=` selects how Go's FIPS 140-3 module behaves:

| Mode | Module active | Non-approved API |
|---|---|---|
| `off` (default) | no | allowed |
| `on` | yes | allowed |
| `only` | yes | refused |

### What works, measured

Measured with `quic-go` v0.62.0, `-tags fips`, real handshakes and
live three- and five-node clusters:

| | `fips140=on` | `fips140=only` |
|---|---|---|
| seal layer, WAL, snapshots (at rest) | works | works |
| TLS 1.3 handshake, approved curves and the SecP256r1MLKEM768 hybrid | works | works |
| QUIC packet protection | works | works |
| Raft replication across a live cluster | works | works |
| OCSP revocation checking | works | panics |

Use `GODEBUG=fips140=only`. With revocation `Mode: "crl"` nothing in
go-quicraft is refused under `only`.

### The approved-only recipe

go-quicraft is a library, so the build tag and the environment belong to your
binary:

```bash
go build -tags fips -o your-service ./cmd/your-service
GOFIPS140=latest GODEBUG=fips140=only ./your-service
```

```go
cfg.TransportConfig.RevocationConfig = &config.RevocationConfig{
	CRLPaths:      []string{"/etc/quicraft/crl/issuing.crl"},
	Mode:          "crl", // not "ocsp", "both", or "any"
	CheckInterval: 60 * time.Second,
}
```

Leaving `RevocationConfig` nil also avoids OCSP, at the cost of no revocation
checking at all. That is a security tradeoff, not a FIPS one; see
[Task: turn on revocation](#task-turn-on-revocation).

Four requirements. Three make it approved-only; the second is defence in
depth, and you want it anyway:

1. **`quic-go` v0.60.0 or later.** go-quicraft requires v0.65.0. Below v0.60.0
   the process panics constructing the initial packet-protection AEAD, before
   any handshake.
2. **`-tags fips`.** Not for the curve list: Go filters X25519 out of the
   offered groups under `fips140` mode on its own, and an untagged build
   handshakes fine under `only` (on `X25519MLKEM768`, whose ML-KEM half the
   module counts; a tagged build uses `SecP256r1MLKEM768`, both halves
   approved). The tag is for the seal layer: it compiles
   Argon2id and ChaCha20-Poly1305 out of the binary, so no configuration,
   environment or stored metadata can reach them. Untagged, they are present
   and refused at runtime (`ErrKDFNotFIPSApproved`). Absence beats refusal,
   and this is the only combination the project's gate tests.
3. **`GODEBUG=fips140=only`** with `GOFIPS140=latest`. `on` loads the module
   but still permits non-approved algorithms, so it proves less than it looks.
4. **Revocation `Mode: "crl"`.** This is the one that is easy to miss, because
   the default (`any`) reaches OCSP whenever a responder URL is configured.

If you run under `fips140=on` today, you can move to `only`. Nothing else in
your configuration needs to change, except the revocation mode if you use OCSP.

### Why OCSP refuses `only`

Not because OCSP is weak. The refused primitive is SHA-1, used as an identifier
rather than for any security property: RFC 6960 §4.1.1 defines a `CertID` as
the hash of the issuer's name and public key, which a responder matches against
its index. `golang.org/x/crypto/ocsp.CreateRequest` computes it, and under
`only` that call panics:

```
crypto/sha1: use of SHA-1 is not allowed in FIPS 140-only mode
```

A compliant responder rejects any other hash, so this cannot be fixed in the
library or the dependency. It can be avoided, which is what `Mode: "crl"` does:

| `Mode` | Reaches SHA-1? |
|---|---|
| `"crl"` | never |
| `"ocsp"` | yes |
| `"both"` | yes |
| `"any"` (default) | only when `OCSPResponderURL` is set |

If you need OCSP and approved-only, those requirements conflict, and the
conflict is the OCSP standard's.

### Do not use GOEXPERIMENT=boringcrypto

BoringCrypto is a different validated module. `NewGCMWithRandomNonce` fails
there with `cipher: NewGCMWithRandomNonce requires aes.Block`, and that is what
the seal layer uses. You would trade a working deployment for broken
data-at-rest.

### What we can and cannot tell you about compliance

Established:

- Under `fips140=only` the validated module is active and every algorithm
  go-quicraft executes is one the module vouches for: the seal layer, the WAL,
  snapshots, the TLS 1.3 handshake, and QUIC packet protection.
- `make verify-fips` proves it continuously, over every package except the
  revocation checker.
- `make verify-fips-exempt` runs the revocation checker under `fips140=on`, so
  it is gated rather than merely excluded.

Not established, and not claimed:

- That your deployment as a whole is FIPS 140-3 validated. This is a library.
  Go's module is the validated component; what a certificate covers, and what
  your assessor accepts, are questions for them.
- Anything about a deployment that keeps OCSP revocation enabled. That path
  executes SHA-1.

In short: with `quic-go` v0.60.0 or later, `-tags fips`,
`GODEBUG=fips140=only` and revocation `Mode: "crl"`, go-quicraft is FIPS at rest
and on the wire, with no exemptions. Enable OCSP and you take on one documented
exemption, for an issuer identifier that protects nothing.

The [FIPS 140-3 exemption record](architecture.md#fips-140-3-exemption-record)
carries the full citations, the scope enforcement and the acceptance; that is
the document to hand an assessor.

## Troubleshooting

**The cluster never forms and the logs show only dial failures.** Check UDP.
This is the most common cause, and QUIC gives no better diagnostic: from the
client's side an unreachable UDP port and a silent server look the same.

**`config.ErrMTLSConfigRequired`.** The transport is enabled with no
`MTLSConfig`. There is no unauthenticated mode; if you want one, you want
`WithoutTransport()`, which is a different thing.

**`StartShard` fails saying the local certificate also covers peer hosts.**
Your leaf is a wildcard or carries other members' SANs. Issue one certificate
per node naming only that node's host.

**Handshake failures between two nodes that both have valid certificates.** In
order of likelihood: the SAN does not cover the address being dialed; the
nodes were issued by different CAs and only one bundle was distributed; or one
leaf covers the other node's host, which is refused deliberately.

**Connections refused right after enabling revocation.** That is the
fail-closed default. A CRL path that does not exist, or an OCSP responder that
is unreachable, means unknown status, and unknown status is not "not revoked".
Fix the source rather than setting `AllowSoftFail`.

**Messages dropped as unauthorized (`unknown_member`).** The receiving replica
holds no configuration naming the sender. After a membership change this
clears once the replica applies it. If it persists for a leader, the replica
missed the change that added that leader; restart it with a
`Config.MemberIdentities` that names the leader.

**Messages dropped as unauthorized (`identity_mismatch`).** The peer's
certificate is on a different keypair from the one committed for the replica it
claims. Reissuing a certificate with a new key changes the identity; reissuing
on the same key does not. Rotate the key through a membership change, not by
swapping the certificate.

**Messages dropped as unauthorized (`missing_identity`).** The shard's
configuration records no identity for a member. That configuration was written
without identities; wipe and re-bootstrap it.

**`StartShard` on a restart fails with `*InvalidMemberError` saying the
committed identity is not the one this host presents.** The host's keypair
changed. Every peer would refuse it, so the start is refused instead. Restore
the original key, or remove and re-add the replica with the new identity. (If
the binding was committed after the snapshot the restart recovered from, the
host starts and logs the same message at ERROR instead.)

**Heartbeats look delayed under load.** They should not be: stream 0 is
reserved for heartbeats and they ride a priority channel drained ahead of data.
If you see it anyway, look at `queue_full` drops and at whether the process is
CPU-starved.

**A recovering follower saturates the link.** That is what
`MaxSnapshotReceiveRate`, `MaxConcurrentSnapshotReceives` and
`MaxSnapshotReceiveMemory` bound. Snapshot transfers use their own streams
outside the stream pool, so they do not block replication, but they use the
bandwidth you allow them.

**The process panics inside the QUIC library under `GODEBUG=fips140=only`.**
Your `quic-go` is older than v0.60.0. go-quicraft requires v0.65.0, so this
means a `replace` directive or a downgrade in the embedding module.

**The process panics with `crypto/sha1: use of SHA-1 is not allowed` under
`GODEBUG=fips140=only`.** That is OCSP, the one documented exemption. Set
revocation `Mode: "crl"` and it is never computed. See
[Choose a FIPS mode](#task-choose-a-fips-mode).

**`inbound frame header rejected` with an unsupported version, or decode
errors between nodes.** The two binaries were built from different commits.
Until the first production release, formats are pinned at version 1 and changed
in place, so builds from different commits may disagree on layout while both
stamp 1. Rebuild every node from one commit.
