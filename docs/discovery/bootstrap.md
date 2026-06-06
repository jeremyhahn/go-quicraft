# Peer Discovery & Cluster Formation — Moved to go-qrdb

Peer discovery and cluster-formation orchestration **no longer live in
go-quicraft**. go-quicraft is now a Raft-primitives library only: it exposes
`Host.StartShard`, `Host.GetLeaderID`, and replica add/remove
(`pkg/membership.go`), but it does not discover peers or orchestrate cluster
formation.

Those concerns now live in **go-qrdb**, the layer that owns the cluster:

- **Discovery engine** — `go-qrdb/pkg/discovery` (static, multicast, DNS SRV,
  token methods, plus a fallback-chain `Manager`).
- **Bootstrap orchestration** — `go-qrdb/pkg/cluster`
  (`BootstrapShard`: discover → validate → quorum → build member map →
  `Host.StartShard`) plus an opt-in lease+watch dynamic membership manager.
- **SDK** — go-qrdb exposes a `Cluster()` client for membership and leadership
  operations.

See go-qrdb's `docs/discovery.md` for the full reference.

## What go-quicraft still provides

`Host.StartShard(members, join, create, cfg)` takes an explicit
`map[uint64]string` member map. Callers (go-qrdb's `pkg/cluster`, or any
embedder) produce that map — by discovery, static configuration, or any other
means — and then invoke the primitive. `Host.GetLeaderID` and
`pkg/membership.go` (replica add/remove) round out the primitives a cluster
manager builds on.
