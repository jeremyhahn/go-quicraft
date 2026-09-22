# QUIC Transport — Usage

> **Status: NORMATIVE** · Scope: go-quicraft `pkg/internal/transport` and its
> configuration surface · Last verified: 2026-08-19
> Conflicts with this document are defects. Fix them here first.

You interact with the transport in exactly three ways: giving it certificates,
sizing its limits, and diagnosing why two nodes will not talk. There is nothing
to install and no daemon to run — it is a UDP listener inside your process.

## Before you start

- A **UDP** path between every pair of nodes. QUIC is UDP; a firewall that
  allows TCP on the port and not UDP produces a cluster that never forms and
  logs nothing more specific than dial timeouts.
- A CA and one certificate per node. There is no self-signed or
  skip-verification mode.
- `HostConfig.ListenAddress` for the local bind, and `RaftAddress` for the
  address peers should dial — they differ whenever the node is behind NAT or in
  a container.

## Task: issue certificates that will actually work

Each node's leaf must:

1. Be signed by the CA in `MTLSConfig.CACert`, which is used as **both**
   `RootCAs` and `ClientCAs` — the same bundle verifies in both directions.
2. Carry a SAN covering the address other nodes dial it at — the `RaftAddress`,
   not the bind address. Hostname verification is on.
3. **Not** cover any other member's address. A peer whose leaf also covers the
   local node's host is refused at connection setup, because a certificate that
   can impersonate you is not one you accept from someone else.

```go
cfg.TransportConfig = &config.TransportConfig{
    MTLSConfig: &config.MTLSConfig{
        CACert: caPEM,
        Cert:   leafPEM,
        Key:    keyPEM,
    },
}
```

There is no way to run without this. Omitting it fails with
`ErrMTLSConfigRequired`; `WithoutTransport()` disables the transport entirely
rather than relaxing it.

## Task: turn on revocation

```go
cfg.TransportConfig.RevocationConfig = &config.RevocationConfig{
    CRLPaths:      []string{"<config-dir>/crl/issuing.crl"},
    Mode:          "crl",
    CheckInterval: 60 * time.Second,
}
```

**Understand the default before you enable it.** Unknown status fails **closed**.
A mode with no usable source refuses connections with
`ErrNoRevocationSources` rather than treating them as unrevoked — in every mode,
including `"crl"`. That is the correct behavior and it will take your cluster
down if the CRL file is missing.

`Config.Validate` rejects such a configuration at startup, so the common failure
is a CRL that disappears *later* — a deployment that ships CRLs separately from
the binary needs them present before the process starts and kept present.

`AllowSoftFail: true` opts out. It is a real tradeoff, not a convenience flag.

CRLs hot-reload on content change, so replacing the file is enough; no restart
and no signal.

## Task: size the limits

Defaults are tuned for an ordinary cluster. The three worth revisiting:

```go
tc := cfg.TransportConfig
tc.UDPRecvBufSize = 7 << 20   // 0 uses the OS default, usually far smaller
tc.UDPSendBufSize = 7 << 20
tc.MaxMessageReceiveMemory = 256 << 20
tc.MaxSnapshotReceiveRate  = 256 << 20
```

Two things people get wrong:

- **`MaxMessageReceiveMemory` counts wire bytes**, reserved before a frame's
  body is read. The decoded form is larger — multiply by 8
  (`proto.MaxDecodeMemoryFactor`) for the heap the process may hold. A 256 MB
  budget admits frames that can occupy 2 GB.
- **Raising `UDPRecvBufSize` above the kernel maximum silently clamps.** On
  Linux, check `net.core.rmem_max`.

Leave `Enable0RTT` off unless you have thought about it. **0-RTT data is
replayable**, and the round trip it saves is once per connection, not once per
message.

## Task: give every member its identity

A member is authorized by its committed node identity and by nothing else; a
sender the configuration holds no identity for has every message dropped. So
every host needs its own keypair, and every admission — the founding map, the
join anchor, every later add — names the identity the node reports from
`Host.NodeIdentity()`. There is no setting: the requirement follows from the
host having a transport at all. The procedure is in
[`../compatibility.md`](../compatibility.md).

## Task: watch the right signals

```go
transport.ConnectionCount()
transport.StreamCount()
transport.SendQueueCount()
```

**Send-queue depth is the leading indicator.** A peer that cannot keep up shows
there well before it shows up as a spurious leader election, and it is the one
signal that distinguishes "the network is slow" from "that node is slow".

Connection count against `MaxIncomingConnections` matters because at the ceiling
new peers are simply refused.

## Task: run without a transport

```go
host, err := quicraft.NewHost(cfg, quicraft.WithoutTransport())
```

Single-node embedded deployments and tests. The QUIC transport is not
constructed at all, and mTLS validation is skipped because there is nothing to
validate.

## Task: choose a FIPS mode

If you are deploying into a FIPS-regulated environment, this is the decision to
make. It used to have one sharp edge — a clustered process panicked under
`fips140=only` — and that edge is gone as of `quic-go` v0.60.0.

### The three modes

`GODEBUG=fips140=` selects how Go's FIPS 140-3 module behaves:

| mode | module active | non-approved API |
|---|---|---|
| `off` (default) | no | allowed |
| `on` | **yes** | allowed |
| `only` | **yes** | **refused** |

### What works, measured

Measured 2026-09-04 with `quic-go` v0.62.0, `-tags fips`, real handshakes and
live three- and five-node clusters:

| | `fips140=on` | `fips140=only` |
|---|---|---|
| seal layer, WAL, snapshots (at rest) | works | works |
| TLS 1.3 handshake, approved curves | works | works |
| QUIC packet protection | works | **works** |
| Raft replication across a live cluster | works | **works** |
| **OCSP revocation checking** | works | **panics** |

**Use `GODEBUG=fips140=only`.** With revocation `Mode: "crl"` there is nothing
in go-quicraft that `only` refuses.

### The approved-only recipe

go-quicraft is a library, so the tag and the environment belong to **your**
binary, not to this module:

```bash
go build -tags fips -o your-service ./cmd/your-service
GOFIPS140=latest GODEBUG=fips140=only ./your-service
```

```go
cfg.TransportConfig.RevocationConfig = &config.RevocationConfig{
    CRLPaths:      []string{"<config-dir>/crl/issuing.crl"},
    Mode:          "crl", // NOT "ocsp", "both", or "any"
    CheckInterval: 60 * time.Second,
}
```

Leaving `RevocationConfig` nil also avoids OCSP, at the cost of no revocation
checking at all. That is a security tradeoff, not a FIPS one — see
[Task: turn on revocation](#task-turn-on-revocation).

Four requirements. Three are what make it approved-only; the second is defence
in depth, and you want it anyway:

1. **`quic-go` ≥ v0.60.0.** go-quicraft pins v0.63.0. Below v0.60.0 the process
   panics constructing the initial AEAD, before any handshake.
2. **`-tags fips`.** Not for the curve list — Go filters X25519 out of the
   offered groups under `fips140` mode on its own, and an untagged build
   handshakes fine under `only` (measured). The tag is for the **seal layer**:
   it compiles Argon2id and ChaCha20-Poly1305 out of the binary entirely, so no
   configuration, environment or stored metadata can reach them. Untagged they
   are present and merely refused at runtime
   (`ErrKDFNotFIPSApproved`). Absence beats refusal, and this is the only
   combination the project's gate actually tests.
3. **`GODEBUG=fips140=only`** with `GOFIPS140=latest`. `on` loads the module but
   still permits non-approved algorithms, so it proves less than it looks like.
4. **Revocation `Mode: "crl"`.** This is the one that is easy to miss, because
   the default (`any`) reaches OCSP whenever a responder URL is configured.

### Why OCSP still refuses `only`

Not because OCSP is weak. The refused primitive is **SHA-1**, and it is used as
an IDENTIFIER rather than for any security property: RFC 6960 §4.1.1 defines a
`CertID` as the hash of the issuer's name and public key, which a responder
matches against its index. `golang.org/x/crypto/ocsp.CreateRequest` computes it,
and under `only` that call panics:

```
crypto/sha1: use of SHA-1 is not allowed in FIPS 140-only mode
```

A compliant responder rejects any other hash, so this cannot be fixed in the
library or in the dependency. It can be **avoided**, which is what `Mode: "crl"`
does:

| `Mode` | reaches SHA-1? |
|---|---|
| `"crl"` | **never** |
| `"ocsp"` | yes |
| `"both"` | yes |
| `"any"` (default) | only when `OCSPResponderURL` is set |

If you need OCSP *and* approved-only, those two requirements are in genuine
conflict and the conflict is the OCSP standard's, not this library's.

### What used to be here, and why it is gone

Earlier versions of this document told you to use `fips140=on` because QUIC
packet protection could not be approved: RFC 9001 §5.3 derives each nonce by
XOR-ing the packet number into a fixed IV, `cipher.NewGCMWithRandomNonce` cannot
express that, and `quic-go` reached the construction through the general
`cipher.NewGCM`, which `only` declines.

`quic-go` v0.60.0 removed that. It now builds packet-protection AEADs through
Go's TLS 1.3 AES-GCM implementation — the same approved path `crypto/tls` uses
for its own records, for the same construction — so the deterministic nonce that
SP 800-38D §8.2.1 permits is now reached through an approved service rather than
a declined one. **If you configured `fips140=on` on the old advice, you can move
to `only`.** Nothing in your configuration needs to change except that variable
and, if you use OCSP, the revocation mode.

### Do NOT reach for GOEXPERIMENT=boringcrypto

Still true, and still a trap. BoringCrypto is a different validated module:
`NewGCMWithRandomNonce` fails there with `cipher: NewGCMWithRandomNonce requires
aes.Block`, which is what the **seal layer** uses. You would trade a working
deployment for broken data-at-rest, to solve a problem that no longer exists.

### What we can and cannot tell you about compliance

Stated plainly, because this is the part an auditor will ask about and we would
rather you arrive prepared than surprised.

**What is established:**

- Under `fips140=only` the validated module is active and every algorithm
  go-quicraft executes is one the module vouches for — the seal layer, the WAL,
  snapshots, the TLS 1.3 handshake, and QUIC packet protection.
- `make verify-fips` proves it continuously, over every package except
  `pkg/internal/transport/revocation`.
- `make verify-fips-exempt` runs that one remaining tree under `fips140=on`, so
  it is gated rather than merely excluded.

**What is NOT established, and we will not claim:**

- That your deployment as a whole is FIPS 140-3 validated. This is a library. Go's
  module is the validated component; what a certificate covers, and what your
  assessor accepts, are questions for them.
- Anything about a deployment that keeps OCSP revocation enabled. That path
  executes SHA-1 and we say so rather than arguing it away.

The accurate summary: with `quic-go` ≥ v0.60.0, `-tags fips`,
`GODEBUG=fips140=only` and revocation `Mode: "crl"`, go-quicraft is **FIPS at
rest and on the wire**, with no exemptions. Enable OCSP and you take on one
documented exemption, for an issuer identifier that protects nothing.

`docs/quic-transport/architecture.md` §FIPS 140-3 exemption record carries the
full citations, the scope enforcement, and the owner's acceptance decision —
that is the document to hand an assessor.

## Troubleshooting

**The cluster never forms and the logs show only dial timeouts** — check UDP.
This is the single most common cause, and QUIC gives no better diagnostic
because from the client's side an unreachable UDP port and a silent server are
identical.

**`ErrMTLSConfigRequired`** — the transport is enabled with no `MTLSConfig`.
There is no unauthenticated mode; if you want one, you want
`WithoutTransport()`, which is a different thing.

**Handshake failures between two nodes that both have valid certificates** — in
order of likelihood: the SAN does not cover the address being dialed (hostname
verification is on); the two nodes were issued by different CAs and only one
bundle was distributed; or one leaf covers the other node's address, which is
refused deliberately.

**Connections refused right after enabling revocation** — this is the fail-closed
default doing its job. A CRL path that does not exist, or an OCSP responder
that is unreachable, means unknown status, and unknown status is not "not
revoked". Fix the source rather than setting `AllowSoftFail`.

**Revocation stopped being enforced after a session resumed** — it did not. The
check is installed as `VerifyConnection`, not `VerifyPeerCertificate`,
specifically so resumed sessions are checked too. Look elsewhere.

**Messages dropped as unauthorized** — the sender has no committed identity
(a configuration written by a build that did not require one; wipe and
re-bootstrap), or the committed SPKI does not match the leaf it presented. A certificate reissued with a **new key** changes the SPKI and
therefore the identity; reissuing with the same key does not.

**Heartbeats look delayed under load** — they should not be. Stream 0 is
reserved for heartbeats and they ride a priority channel drained ahead of data,
precisely so a large replication batch cannot push one past the election
timeout. If you see it anyway, look at send-queue depth and at whether the
process is CPU-starved, not at the transport's ordering.

**A recovering follower saturates the link** — that is what
`MaxSnapshotReceiveRate`, `MaxConcurrentSnapshotReceives` and
`MaxSnapshotReceiveMemory` bound. Snapshot transfers use a dedicated frame path
outside the stream pool, so they will not block ordinary replication, but they
will use the bandwidth you allow them.

**A peer declares a huge frame and the process does not grow** — correct. The
frame length is reserved against `MaxMessageReceiveMemory` before the body is
read, so a peer cannot make this process allocate by asserting a size.

**A process panics inside the QUIC library under `GODEBUG=fips140=only`** —
your `quic-go` is older than v0.60.0. That version routes packet protection
through Go's approved TLS 1.3 AES-GCM; before it, constructing the initial AEAD
panics before any handshake. go-quicraft pins v0.63.0, so this means a
`replace` directive or a stale `go.sum` in the embedding module.

**A process panics with `crypto/sha1: use of SHA-1 is not allowed` under
`GODEBUG=fips140=only`** — that is OCSP, and it is the one documented
exemption. The OCSP CertID is SHA-1 by protocol definition (RFC 6960 §4.1.1).
Set revocation `Mode: "crl"` and it is never computed. See
[Choose a FIPS mode](#task-choose-a-fips-mode).

**A frame rejected for a foreign wire version** — two binaries built from
different commits. Until the first production release, formats are pinned at 1
and changed in place. Rebuild everything from one commit.
