# go-quicraft specification

> **Status: NORMATIVE** · Scope: what go-quicraft is, every feature, its requirements and status · Last verified: 2026-10-14
> A feature listed here, in any status, is never deleted (not its docs, code or issues) without the owner's change to this file.

## Purpose

QuicRaft is the Raft library of the stack, which is one product built in layers
(owner ruling 2026-10-14). It is a multi-group Raft consensus library for Go
with QUIC transport: one process hosts many independent Raft groups (shards). It
is Layer 1, the consensus engine under every project above it. The protocol
follows the extended Raft paper and Ongaro's thesis, kept in
[references/](references/).

**Embeds:** go-enclave, Layer 0, as a library: its one local dependency, the
portfolio's protected-memory primitive. go-quicraft inherits locked and sealed
memory from it, and its SDK carries go-enclave's types (`MemoryEnclave`,
`SealedEnclave`) through unchanged, under go-enclave's names. It never aliases,
wraps or re-models them.

**Embedded by:** go-qrdb, the cluster engine and database system. go-qrdb
embeds go-quicraft as a library and re-exports it through go-qrdb's SDK. Every
layer above go-qrdb (go-xkms, go-quicbaas, go-trusted-platform) reaches
consensus only through go-qrdb's SDK, never by importing go-quicraft directly.

**One engine, one datastore:** a deployment runs one go-quicraft engine: the
one go-qrdb embeds. No layer starts a second `Host` or Raft cluster of its own
beside it. go-quicraft is not a datastore. Its WAL and snapshots replicate and
recover the state machines an embedder supplies. Records, tenants and their
isolation belong to go-qrdb.

## Boundaries

**Owns:** the Raft primitives; the QUIC transport (never TCP); the WAL;
snapshots; the `Barrier`/`SealingStrategy` interfaces and `EpochBarrier`; the
`none` and `passphrase` sealing strategies; the Shamir GF(256) primitive.

**Inherits from go-enclave:** locked plaintext (`MemoryEnclave`) and material
sealed at rest (`SealedEnclave`), consumed directly.

**Never:**

- A protected-memory type, store or handling of its own: go-enclave.
- A database, a tenant model or a client of its own: go-qrdb. `pkg/sm/kv` holds
  reference state machines and test fixtures, not a datastore any layer deploys.
- Shamir custody, the ceremony, recovery/rekey/root token, peer discovery and
  cluster formation: go-qrdb. The caller hands `StartShard` the member set.
- Hardware and cloud sealing strategies: go-xkms.
- An implicit default sealing strategy or a strategy ranking: selection is
  always explicit at barrier setup.

## Features

Status is one of `shipped`, `in progress`, `planned`, `future`. Each feature
folder `docs/<feature>/` is the detail behind its row; where a row links an
`architecture.md`, that file states the full requirements.

| Feature | Status | Requirements | Detail |
|---|---|---|---|
| multi-group-host | shipped | One `Host` per process manages many shards, each with its own state machine, log and membership; it routes proposals, reads, sessions and membership; it starts shards from an explicit member map and discovers no peers. [architecture.md](multi-group-host/architecture.md) | [multi-group-host/](multi-group-host/) |
| raft-consensus | shipped | Raft per the extended paper and the thesis: election with PreVote and CheckQuorum, log replication, ReadIndex reads, leadership transfer, quiescence, run by the step/commit/apply worker pipeline. [architecture.md](raft-consensus/architecture.md) | [raft-consensus/](raft-consensus/) |
| lease-read | shipped | Opt-in (`Config.LeaseRead`) linearizable reads that skip the heartbeat round while a lease is valid; the lease quorum spans every voting member, witnesses included; the lease is shortened by `Config.MaxClockDrift` (default 1 %). | [raft-consensus/](raft-consensus/) (engineering.md §15) |
| read-index-readiness | shipped | A ReadIndex read, `SyncRead` included, that the shard's leader cannot serve yet because it has not committed an entry of its current term returns `ErrReadIndexNotReady` (`errors.Is`-matchable, `Temporary` reports true) instead of waiting, both on the leader and on a follower that forwarded the read. A forwarded read or reply that is lost is still bounded only by the caller's context. Seam: go-qrdb's bounded startup read retries it until its own deadline. | [raft-consensus/](raft-consensus/) |
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

Every feature is shipped; no feature is in progress, planned or future.

## Interfaces

- Go SDK: the `quicraft` package (`pkg/`) and `pkg/config`: [multi-group-host/api.md](multi-group-host/api.md).
- State machines: [state-machines/api.md](state-machines/api.md).
- Seal: [seal-barrier/api.md](seal-barrier/api.md).
- Wire protocol: [wire-protocol/](wire-protocol/).
- CLI: none; go-quicraft ships no binary.

## Release criteria

The current milestone is **Dev complete** (owner ruling 2026-10-14): nothing is
dev complete until go-trusted-platform is, and every lower library has a stable,
complete API. Until then go-quicraft tags `-alpha` only, cuts an `-rc.N` only if
a consumer needs one, and keeps the rc tags already cut pinned. It closes when:

- #309 is closed: `TestLinearizability_3Node_LeaderFailover` and its soak pass
  reliably (testing). Met 2026-10-13: #320, #322 and #347 merged.
- #283 is closed: the spec-review scrub of `docs/` is verified by the
  department and its fixes have landed. Met 2026-10-13.
- Every feature row is shipped. Met 2026-10-14: #350 merged.
- Every seam row go-trusted-platform's SPEC names on go-quicraft is shipped. It
  names none: go-trusted-platform reaches consensus only through go-qrdb.
- The API is stable and complete.

Then release candidates are tagged, deployed to staging and released;
production is considered after staging.

The v1.0.0 bar, on top of the above:

- go-enclave is at a public release (memory-enclave).
- The compatibility policy is acknowledged (`release-versioning.md` §1, §8)
  (compatibility).
- A publication mechanism for agent files exists (release).
- The company CI template is neutralised for the public tree: `.github/workflows`
  names no internal repository or domain (release).
- Every wire and on-disk format version stays pinned until v1.0.0 ships
  (wire-protocol, log-storage).
