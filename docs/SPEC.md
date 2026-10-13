# go-quicraft specification

> **Status: NORMATIVE** · Scope: what go-quicraft is, every feature, its requirements and status · Last verified: 2026-10-13
> A feature listed here, in any status, is never deleted (not its docs, code or issues) without the owner's change to this file.

## Purpose

QuicRaft is a multi-group Raft consensus library for Go with QUIC transport: one
process hosts many independent Raft groups (shards). It is Layer 1, the
foundational consensus engine. Its one local dependency is go-enclave, Layer 0,
the portfolio's protected-memory primitive. go-qrdb consumes it directly, and
through go-qrdb so do go-xkms, go-quicbaas, go-trusted-ca and the rest of the
trusted platform stack. The protocol follows the extended Raft paper and
Ongaro's thesis, kept in [references/](references/).

## Boundaries

**Owns:** the Raft primitives; the QUIC transport (never TCP); the WAL;
snapshots; the `Barrier`/`SealingStrategy` interfaces and `EpochBarrier`; the
`none` and `passphrase` sealing strategies; the Shamir GF(256) primitive.

**Never:**

- Shamir custody, the ceremony, recovery/rekey/root token, peer discovery and
  cluster formation: go-qrdb. The caller hands `StartShard` the member set.
- Hardware and cloud sealing strategies: go-xkms.
- An implicit default sealing strategy or a strategy ranking: selection is
  always explicit at barrier setup.
- A protected-memory type of its own: go-enclave.

## Features

Status is one of `shipped`, `in progress`, `planned`, `future`. Each feature
folder `docs/<feature>/` is the detail behind its row; where a row links an
`architecture.md`, that file states the full requirements.

| Feature | Status | Requirements | Detail |
|---|---|---|---|
| multi-group-host | shipped | One `Host` per process manages many shards, each with its own state machine, log and membership; it routes proposals, reads, sessions and membership; it starts shards from an explicit member map and discovers no peers. [architecture.md](multi-group-host/architecture.md) | [multi-group-host/](multi-group-host/) |
| raft-consensus | shipped | Raft per the extended paper and the thesis: election with PreVote and CheckQuorum, log replication, ReadIndex reads, leadership transfer, quiescence, run by the step/commit/apply worker pipeline. [architecture.md](raft-consensus/architecture.md) | [raft-consensus/](raft-consensus/) |
| lease-read | shipped | Opt-in (`Config.LeaseRead`) linearizable reads that skip the heartbeat round while a lease is valid; the lease quorum spans every voting member, witnesses included; the lease is shortened by `Config.MaxClockDrift` (default 1 %). | [raft-consensus/](raft-consensus/) (engineering.md §15) |
| membership-changes | shipped | Add and remove replicas, observers and witnesses; every member is authorized by its node identity (a hash of its certificate's public key), not its address; members join through the `...WithIdentity` requests. [architecture.md](membership-changes/architecture.md) | [membership-changes/](membership-changes/) |
| quic-transport | shipped | All inter-node traffic, snapshots included, runs over QUIC with mandatory mutual TLS and no TCP fallback; connection and stream pools, heartbeats on stream 0; CRL/OCSP revocation fails closed; the node registry. [architecture.md](quic-transport/architecture.md) | [quic-transport/](quic-transport/) |
| 0-rtt-policy | shipped | 0-RTT is off by default (`TransportConfig.Enable0RTT`); a node never sends early data; message frames and snapshots that arrive as 0-RTT early data are refused, because they are replayable. | [quic-transport/](quic-transport/) |
| fips-builds | shipped | `GOFIPS140` selects FIPS 140-3 mode and `-tags fips` compiles non-approved algorithms out; the boundary covers data at rest and the QUIC transport; OCSP revocation is the one exempt path; `make verify-fips` is the check. | [quic-transport/](quic-transport/), [seal-barrier/](seal-barrier/) |
| wire-protocol | shipped | A fixed-layout binary wire format with no protobuf, in two version namespaces; every format version stays pinned until v1.0.0. [architecture.md](wire-protocol/architecture.md) | [wire-protocol/](wire-protocol/) |
| log-storage | shipped | A 16-shard WAL (directory shard `shardID % 16`) with no embedded database; the `LogDB` interface, recovery, compaction and repair. [architecture.md](log-storage/architecture.md) | [log-storage/](log-storage/) |
| snapshots | shipped | Snapshot creation, streaming over QUIC, import and export, and re-encryption. [architecture.md](snapshots/architecture.md) | [snapshots/](snapshots/) |
| state-machines | shipped | Three interfaces an embedder implements (in-memory, concurrent, on-disk), the RSM adapter, and the reference KV backends. [architecture.md](state-machines/architecture.md) | [state-machines/](state-machines/) |
| client-sessions | shipped | At-most-once proposal delivery through client sessions. [architecture.md](client-sessions/architecture.md) | [client-sessions/](client-sessions/) |
| write-batching | shipped | The proposal aggregator (at-least-once, no deduplication) and the write-durability modes. [architecture.md](write-batching/architecture.md) | [write-batching/](write-batching/) |
| execute-on-leader | shipped | `Host.ExecuteOnLeader` runs the shard's registered `LeaderHandler` on the leader with no log entry; only on the leader of the term, after it commits an entry of that term; at most once per term; at most 1024 remote requests at once (`ErrBusy`). | [multi-group-host/](multi-group-host/) (architecture.md, Leader-executed requests) |
| seal-barrier | shipped | AEAD encryption at rest (AES-256-GCM or ChaCha20-Poly1305) of the WAL and snapshots with per-epoch DEKs, rotation and purge; the root key is sealed by an explicit `SealingStrategy`; plaintext is an explicit choice (`WithoutEncryption`); a sealed shard refuses proposals and reads with `ErrBarrierSealed`. [architecture.md](seal-barrier/architecture.md) | [seal-barrier/](seal-barrier/) |
| shamir-primitive | shipped | The GF(2^8) split/combine primitive and the share-custody interfaces; the engine never persists Shamir shares ([engineering.md §6](shamir-primitive/engineering.md#6-persistence)). [architecture.md](shamir-primitive/architecture.md) | [shamir-primitive/](shamir-primitive/) |
| memory-enclave | shipped | Key material is held in go-enclave locked memory (`MemoryEnclave`) and sealed enclaves (`SealedEnclave`), consumed directly, never aliased or wrapped. [architecture.md](memory-enclave/architecture.md) | [memory-enclave/](memory-enclave/) |
| observability | shipped | Metrics, logging and event notifications. [architecture.md](observability/architecture.md) | [observability/](observability/) |
| compatibility | shipped | The pinned format-version policy, every version discriminator, the interface requirements for embedders, and upgrade notes for each release. [architecture.md](compatibility/architecture.md) | [compatibility/](compatibility/) |
| contrib-dragonboat | shipped | A Dragonboat API compatibility layer over the `Host`, in its own module under `contrib/dragonboat/`. | [multi-group-host/](multi-group-host/) (architecture.md, Package structure) |
| testing | shipped | The test infrastructure: suite organisation, build tags, fault injectors, fuzzing, linearizability verification, coverage gates and benchmark regression. [overview.md](testing/overview.md) | [testing/](testing/) |
| release | shipped | Public releasability: what may be published from this tree, the development-tooling exclusion list and the `test/releasability` gate; the release recipe (`make release-candidate`, `make release-alpha`). [public-releasability.md](release/public-releasability.md) | [release/](release/) |

No feature is in progress, planned or future.

## Interfaces

- Go SDK: the `quicraft` package (`pkg/`) and `pkg/config`: [multi-group-host/api.md](multi-group-host/api.md).
- State machines: [state-machines/api.md](state-machines/api.md).
- Seal: [seal-barrier/api.md](seal-barrier/api.md).
- Wire protocol: [wire-protocol/](wire-protocol/).
- CLI: none; go-quicraft ships no binary.

## Release criteria

The current milestone is **Release candidate**: a public release, its version
confirmed at the owner's publish approval. It closes when:

- #309 is closed: `TestLinearizability_3Node_LeaderFailover` and its soak pass
  reliably (testing). It closes when PRs #320 (engine: step received replies
  before the tick) and #322 (transport: drain an epoch-evicted outbound) merge.
- #283 is closed: the spec-review scrub of `docs/` is verified by the
  department and its fixes have landed.
- The candidate is deployed to staging; production is considered after that.

The v1.0.0 bar, on top of the above:

- go-enclave is at a public release (memory-enclave).
- The compatibility policy is acknowledged (`release-versioning.md` §1, §8)
  (compatibility).
- A publication mechanism for agent files exists (release).
- The company CI template is neutralised for the public tree: `.github/workflows`
  names no internal repository or domain (release).
- Every wire and on-disk format version stays pinned until v1.0.0 ships
  (wire-protocol, log-storage).
