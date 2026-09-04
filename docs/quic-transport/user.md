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

## Task: decide on `RequireMemberIdentity`

| Setting | Behavior for a sender with no committed identity |
|---|---|
| `false` (default) | fall back to a SAN check against the committed Raft address |
| `true` | drop the message |

`true` is the stronger posture and is what a new cluster should run. Turning it
on for a **live** cluster is a sequenced migration: every member must have a
committed identity first, or you partition the cluster from itself. The
procedure is in [`../compatibility.md`](../compatibility.md).

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
make, and it has one sharp edge.

### The three modes

`GODEBUG=fips140=` selects how Go's FIPS 140-3 module behaves:

| mode | module active | non-approved API |
|---|---|---|
| `off` (default) | no | allowed |
| `on` | **yes** | allowed |
| `only` | **yes** | **refused** |

### What works, measured

| | `fips140=on` | `fips140=only` |
|---|---|---|
| seal layer, WAL, snapshots (at rest) | works | works |
| TLS 1.3 handshake, approved curves | works | works |
| **QUIC packet protection** | works | **panics** |

**Run a cluster under `GODEBUG=fips140=only` and the process panics** while
constructing the initial AEAD — before any handshake, so you get no connection
at all rather than a degraded one. Use `fips140=on`.

### Why `only` refuses it

Not because QUIC uses weak or unapproved cryptography. QUIC protects packets
with **AES-GCM**, and derives each nonce by XORing the packet number into a
static IV (RFC 9001 §5.3). That deterministic construction is one NIST
explicitly permits — SP 800-38D §8.2.1 — and is essentially what TLS 1.3 does
for its own records.

The refusal is about API surface, not algorithms. Go's module validates
`cipher.NewGCMWithRandomNonce`; `cipher.NewGCM` with a caller-supplied IV is
declined under `only`:

```
crypto/cipher: use of GCM with arbitrary IVs is not allowed in FIPS 140-only
mode, use NewGCMWithRandomNonce
```

`crypto/tls` reaches an approved path internally for the same construction.
`quic-go` cannot: it is third-party code and only public APIs are available to
it. No configuration of this library changes that, and no QUIC implementation
can differ — the wire format requires the construction.

### Do NOT reach for GOEXPERIMENT=boringcrypto

It looks like a fix and is a trap. BoringCrypto is a different validated module
and it fails the *other* way round: `cipher.NewGCM` with a caller-supplied IV
works, so QUIC is happy — but `NewGCMWithRandomNonce` fails with
`cipher: NewGCMWithRandomNonce requires aes.Block`, which is what the **seal
layer** uses. You trade a broken wire for broken data-at-rest.

TLS 1.3 over TCP is clean under both modules; QUIC is clean under neither in
`only`. That is the whole shape of the problem.

### What we can and cannot tell you about compliance

Stated plainly, because this is the part an auditor will ask about and we would
rather you arrive prepared than surprised.

**What is established:**

- Under `fips140=on` the validated module is active, and AES-GCM executes
  through it.
- The IV construction QUIC uses is a permitted one under SP 800-38D §8.2.1.
- Everything outside `pkg/internal/transport` runs and is tested under
  `fips140=only` — `make verify-fips` proves it, continuously.

**What is NOT established, and we will not claim:**

- Whether Go's CMVP certificate lists **GCM with an externally-supplied IV** as
  an approved *service* of the validated module. `only` declining it is evidence
  that the module does not vouch for it, whatever SP 800-38D permits of the
  construction in the abstract.

That distinction is a question for your assessor and for Go's FIPS 140-3
security policy, not for this project. **We are telling you exactly where the
boundary of our knowledge is rather than asserting a compliance posture we are
not qualified to grant.**

The accurate summary: go-quicraft is **FIPS at rest, not FIPS on the wire.** If
your assessment requires approved-only cryptography end to end on the wire, you
must terminate this transport inside an approved tunnel or outside the boundary.
If your assessment accepts `fips140=on` with the module active, nothing here
blocks you.

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

**Messages dropped as unauthorized** — `RequireMemberIdentity` is on and the
sender has no committed identity, or the committed SPKI does not match the leaf
it presented. A certificate reissued with a **new key** changes the SPKI and
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
expected, and not fixable from here. QUIC's packet protection is arbitrary-IV
AES-GCM by protocol definition. `-tags fips` makes go-quicraft FIPS **at rest,
not on the wire**.

**A frame rejected for a foreign wire version** — two binaries built from
different commits. Until the first production release, formats are pinned at 1
and changed in place. Rebuild everything from one commit.
