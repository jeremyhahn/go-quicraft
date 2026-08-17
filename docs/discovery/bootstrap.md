# Cluster Formation and the Discovery Boundary

**go-quicraft does not discover peers and does not orchestrate cluster
formation.** It is a Raft-primitives library. The primitive it exposes is:

```go
func (h *Host) StartShard(
    members map[uint64]string,
    join bool,
    create sm.CreateFunc,
    cfg config.Config,
) error
```

The caller hands it an explicit `map[uint64]string` of replica ID to Raft
address. Where that map came from — a config file, environment variables,
multicast, DNS SRV, a token exchange — is not this library's concern and there
is no code here that answers it.

Discovery, quorum validation and bootstrap orchestration live one layer up in
**go-qrdb**, the layer that owns the cluster. If you are asking "how do I start
a cluster?", the short answer is at the bottom of this page: use
[go-qrdb](#forming-a-cluster-automatically-go-qrdb).

## Why the boundary sits here

A consensus engine cannot safely guess its own membership. The initial
configuration is committed as a log entry, and every founding replica writes
that entry **independently** — so the map has to be an input, decided by
something that can see all the nodes, not something the engine infers.

## The primitive

`StartShard` takes its `ShardID` and `ReplicaID` from `cfg`; both must be
non-zero. A single-node shard:

```go
hostCfg := config.HostConfig{
    NodeHostDir:   "/var/data/quicraft",
    RaftAddress:   "node1.example.com:63100",
    ListenAddress: "0.0.0.0:63100",
    DeploymentID:  1,
    TransportConfig: config.TransportConfig{
        MTLSConfig: &config.MTLSConfig{CACert: caCert, Cert: cert, Key: key},
    },
}

host, err := quicraft.NewHost(hostCfg)
if err != nil {
    return err
}
defer host.Close()

members := map[uint64]string{
    1: "node1.example.com:63100",
    2: "node2.example.com:63100",
    3: "node3.example.com:63100",
}

shardCfg := config.Config{
    ShardID:   1,
    ReplicaID: 1, // 2 on node2, 3 on node3
}

if err := host.StartShard(members, false, kv.NewMemoryCreateFunc(), shardCfg); err != nil {
    return err
}
```

Every founder calls this with the **same** `members` map and the same shard
configuration, changing only `ReplicaID`.

### `join` and the two ways in

`join = false` bootstraps: this replica is a founding member and writes the
initial-membership entry itself.

`join = true` grows an existing shard. The joining replica passes an **empty**
`members` map — it has no committed configuration yet and learns one by
replicating the log. An operator on an existing member adds it first with
`Host.RequestAddNode` / `SyncRequestAddNode` (or the identity-carrying variants,
`RequestAddNodeWithIdentity` / `SyncRequestAddNodeWithIdentity`), and the
config change commits before the joiner catches up.

A replica configured as a witness (`cfg.IsWitness`) or observer
(`cfg.IsObserver`) must **not** appear in `members` — that map seeds the voting
membership, and passing one is rejected with `*NonVotingRoleInMembersError`.
Bootstrap the voters first, then start the witness or observer with
`join = true` and add it with `RequestAddWitness` / `RequestAddObserver`.

Other primitives a cluster manager builds on: `Host.StopShard`,
`Host.GetLeaderID`, `Host.GetLeaderIDAndTerm`, `Host.GetShardMembership`, and
the membership change requests in `pkg/membership.go`.

## What StartShard commits, and what it refuses

### Initial membership is a committed log entry

The initial configuration is written to the log as a single
`EntryInitialMembership` at **index 1**, term 1, whose `Cmd` is a marshaled
`proto.Membership`. Every replica reconstructs its membership by replaying the
committed log, so membership is a pure fold over the log.

It used to be injected out of band, which meant a replica's membership was not a
function of its durable log: a joining replica could learn the configuration only
from a snapshot, and if no snapshot ever arrived it never learned the full
configuration at all. That gap was a permanent authorization bypass for joiners.
One entry carrying the whole `Membership` is used rather than one `ConfigChange`
per member, so the first real proposal lands at index 2 and the index arithmetic
the recovery clamps depend on is perturbed as little as possible.

**This is why every founder must be given identical bootstrap input.**
`proto.Membership` marshals its maps with sorted keys, so the entry is
byte-identical across founders given identical input — but the input is the
whole `Membership`, not just the addresses. The witness policy
(`Config.Witness`) and the session policy (`SessionExpiryEntries`,
`MaxTotalSessions`, `MaxSessionsPerClient`, `MaxSessionsPerOwner`) are seeded
into it from each founder's local config. Founders that disagree on any of them
produce byte-different entries at the same `(index=1, term=1)`. AppendEntries
matches on `(index, term)` alone, so that divergence is never reconciled.

See [Wire Format](../protocol/wire-format.md) for the `Membership` encoding and
[Session Deduplication](../sessions/deduplication.md) for the session policy.

### Per-node certificate exclusivity is checked, and fails closed

`StartShard` calls
`QUICTransport.VerifyPeerAddressExclusivity(RaftAddress, memberAddresses)` and
returns `transport.ErrSharedPeerCertificate` when this node's own leaf
certificate also covers another member's host.

A wildcard certificate (`*.raft.svc.cluster.local`), or one certificate carrying
every node's SAN, makes every in-band authorization decision in the transport
answer "authorized" for every peer address. The check fails closed rather than
warning because the weakness is invisible from a running cluster and the only
remediation — re-issued certificates — has to happen before the cluster carries
data. Replicas that share this node's host (the same host under any spelling, or
any loopback address) are exempt, because no certificate policy can separate
them; deployments that need these controls must give each replica its own host.

This is a **misconfiguration detector**, not an attack control: a malicious
joiner never runs it. The enforcing counterpart is on the receive path
(`refuseInboundPromiscuousCert`). See
[QUIC Transport](../transport/quic.md#per-node-sans-are-a-hard-deployment-precondition).

## Joining is not an authorization bypass

Per-shard authorization is a **removed-node tombstone deny-list**, the model
etcd and Consul use, not a member allow-list. A shard records the replicas (and
node identities) whose removal it has committed, and refuses their traffic. It
does not require a replica to have been pre-approved before it may talk.

That is deliberate and it is what makes joining work at all. A replica joining a
shard starts with an empty configuration: it does not yet know the leader is a
member. If it could not record the leader's address it could not send its
`ReplicateResp` back, the leader would never advance its match index, and the
join would never complete.

The separation that keeps this safe is between **routing** and
**authorization**:

- Routing state is learnable. `learnFromSource` records a sender's address for
  the return path, but only when that address is covered by the sender's
  verified certificate. A poisoned routing entry is a liveness problem — the
  real member is black-holed until a config change or its own later traffic
  corrects the entry — never an admission decision.
- Authorization is not learnable at all. `authorizeInbound` binds every
  quorum-bearing message to the **committed** membership: to the peer's
  authenticated node identity where the shard has committed one, and otherwise
  to its committed address as covered by the peer certificate. Drops are counted
  by reason (`removed_replica`, `removed_identity`, `misrouted`,
  `unknown_member`, `address_mismatch`, `shared_address`, `identity_mismatch`,
  `missing_identity`), because one aggregate counter cannot tell a rolling
  membership change from an attack.

See [Compatibility](../operations/compatibility.md) for node identities and
`RequireMemberIdentity`.

## Forming a cluster automatically (go-qrdb)

Everything above the member map lives in **go-qrdb**:

- **`go-qrdb/pkg/discovery`** implements the discovery methods behind a common
  `Method` interface: `static.go`, `multicast.go`, `dns.go` (SRV) and
  `token.go`, plus a `Manager` that chains several methods as a fallback list.
- **`go-qrdb/pkg/cluster`** orchestrates formation. `Bootstrapper.Bootstrap`
  runs discover → validate addresses → check quorum → add self → build the
  member map → `StartShard`. `BootstrapShard(ctx, node, opts, shardID, createFn,
  shardCfg)` is the single reusable entry point that builds the discovery method
  from options and runs that sequence. `MinPeers` defaults to 3 and quorum is
  checked before any shard is started.
- **Dynamic membership** is opt-in and also in `pkg/cluster`: members register
  under the `__sys:cluster/members/` prefix with a lease (10 s TTL, refreshed
  every 3 s) and a watcher drives Raft add/remove with bounded retries.

To form a multi-node cluster automatically, call go-qrdb's
`cluster.BootstrapShard`, or run the `qrdbd` daemon, which wraps it. go-qrdb's
`docs/discovery.md` is the reference.

## Cross-References

- [API Reference](../api/reference.md) -- `Host.StartShard`, membership requests
- [Architecture Overview](../architecture/overview.md) -- how a Host, shards and the transport fit together
- [Raft Protocol](../raft/protocol.md) -- membership changes, config-in-log, witnesses
- [QUIC Transport](../transport/quic.md) -- certificate exclusivity, sender authorization
- [Deployment](../operations/deployment.md) -- certificate issuance and per-node SANs
- [Lifecycle](../operations/lifecycle.md) -- host and shard startup and shutdown
- [Compatibility](../operations/compatibility.md) -- node identities, wire-format pinning
