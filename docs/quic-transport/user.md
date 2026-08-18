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
