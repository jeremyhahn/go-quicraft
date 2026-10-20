# go-quicraft specification

> **Status: NORMATIVE** · Scope: what go-quicraft is, every feature, its requirements and status · Last verified: 2026-10-20
> A feature listed here, in any status, is never deleted (not its docs, code or issues) without the owner's change to this file.

## Purpose

QuicRaft is the Raft library of the stack, which is one product built in layers: a multi-group Raft consensus library for Go with QUIC transport, where one process hosts many independent Raft groups (shards).
It is Layer 1, the consensus engine under every project above it; the protocol follows the extended Raft paper and Ongaro's thesis, kept in [references/](references/).
It embeds go-enclave (Layer 0) as a library, and go-qrdb embeds it as the one consensus engine of a deployment and re-exports it through go-qrdb's SDK.

## Boundaries

- **Owns:** the Raft primitives; the QUIC transport (never TCP); the WAL; snapshots; the `Barrier`/`SealingStrategy` interfaces and `EpochBarrier`; the `none` and `passphrase` sealing strategies; the Shamir GF(256) primitive.
- **Embeds go-enclave:** its one local dependency, the portfolio's protected-memory primitive; locked plaintext (`ByteAccess`, from `enclave.NewMemory`) and material sealed at rest (`Sealed`) are consumed directly and carried through the SDK under go-enclave's names, never aliased, wrapped or re-modelled.
- **Embedded by go-qrdb only:** every layer above go-qrdb (go-xkms, go-quicbaas, go-trusted-platform) reaches consensus through go-qrdb's SDK, never by importing go-quicraft directly.
- **One engine per deployment:** the one go-qrdb embeds; no layer starts a second `Host` or Raft cluster of its own beside it.
- **Never a datastore:** the WAL and snapshots replicate and recover the state machines an embedder supplies; records, tenants and their isolation belong to go-qrdb.
- **Never:** a protected-memory type, store or handling of its own (go-enclave).
- **Never:** a database, a tenant model or a client of its own (go-qrdb); `pkg/sm/kv` holds reference state machines and test fixtures, not a datastore any layer deploys.
- **Never:** Shamir custody, the ceremony, recovery/rekey/root token, peer discovery or cluster formation (go-qrdb); the caller hands `StartShard` the member set.
- **Never:** hardware and cloud sealing strategies (go-xkms).
- **Never:** an implicit default sealing strategy or a strategy ranking; selection is always explicit at barrier setup.

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
| heartbeat-under-fsync | shipped | A follower answers a leader's Heartbeat without waiting for an in-flight WAL fsync when the step left its hard state (term, vote, log) unchanged (HeartbeatResp free-order in that case), so CheckQuorum does not depose a leader while a quorum of followers is in a bounded fsync stall; a HeartbeatResp never acknowledges a log index that is not yet durable, and replies to steps that changed the hard state stay ordered behind the fsync. | [raft-consensus/](raft-consensus/) |
| membership-changes | shipped | Add and remove replicas, observers and witnesses; every member is authorized by its node identity (a hash of its certificate's public key), not its address; members join through the `...WithIdentity` requests. [architecture.md](membership-changes/architecture.md) | [membership-changes/](membership-changes/) |
| quic-transport | shipped | All inter-node traffic, snapshots included, runs over QUIC with mandatory mutual TLS and no TCP fallback; connection and stream pools, heartbeats on stream 0; CRL/OCSP revocation fails closed; the node registry. [architecture.md](quic-transport/architecture.md) | [quic-transport/](quic-transport/) |
| 0-rtt-policy | shipped | 0-RTT is off by default (`TransportConfig.Enable0RTT`); a node never sends early data; message frames and snapshots that arrive as 0-RTT early data are refused, because they are replayable. | [quic-transport/](quic-transport/) |
| fips-builds | shipped | `GOFIPS140` selects FIPS 140-3 mode and `-tags fips` compiles non-approved algorithms out (X25519 and every hybrid built on it included; the handshake offers `SecP256r1MLKEM768`, P-256, P-384); the boundary covers data at rest and the QUIC transport; OCSP revocation is the one exempt path; `make verify-fips` is the check. | [quic-transport/](quic-transport/), [seal-barrier/](seal-barrier/) |
| pq-key-exchange | shipped | The TLS 1.3 handshake offers a post-quantum hybrid group first, per draft-ietf-tls-ecdhe-mlkem pinned at -05 (ECDH plus FIPS 203 ML-KEM-768): `X25519MLKEM768` then `SecP256r1MLKEM768` in the non-FIPS build, `SecP256r1MLKEM768` in the FIPS build, which the Go fips140 module accepts under `fips140=only` (`TestFIPSOnlyNegotiatesHybrid`). The classical groups follow, X25519 never in a FIPS build; the two builds share `SecP256r1MLKEM768` and P-256, so a mixed fleet negotiates a hybrid. No quicraft format version changes. Owner ruling 2026-10-17 (queue 49, option A); [#412](../../issues/412). | [quic-transport/](quic-transport/) |
| wire-protocol | shipped | A fixed-layout binary wire format with no protobuf, in two version namespaces; every format version stays pinned until v1.0.0. [architecture.md](wire-protocol/architecture.md) | [wire-protocol/](wire-protocol/) |
| log-storage | shipped | A 16-shard WAL (directory shard `shardID % 16`) with no embedded database; the `LogDB` interface, recovery, compaction and repair. [architecture.md](log-storage/architecture.md) | [log-storage/](log-storage/) |
| snapshots | shipped | Snapshot creation, streaming over QUIC, import and export, and re-encryption. [architecture.md](snapshots/architecture.md) | [snapshots/](snapshots/) |
| state-machines | shipped | Three interfaces an embedder implements (in-memory, concurrent, on-disk), the RSM adapter, and the reference KV backends. [architecture.md](state-machines/architecture.md) | [state-machines/](state-machines/) |
| client-sessions | shipped | At-most-once proposal delivery through client sessions. [architecture.md](client-sessions/architecture.md) | [client-sessions/](client-sessions/) |
| write-batching | shipped | The proposal aggregator (at-least-once, no deduplication) and the write-durability modes. [architecture.md](write-batching/architecture.md) | [write-batching/](write-batching/) |
| execute-on-leader | shipped | `Host.ExecuteOnLeader` runs the shard's registered `LeaderHandler` on the leader with no log entry; only on the leader of the term, after it commits an entry of that term; at most once per term; at most 1024 remote requests at once (`ErrBusy`). | [multi-group-host/](multi-group-host/) (architecture.md, Leader-executed requests) |
| seal-barrier | shipped | AEAD encryption at rest (AES-256-GCM or ChaCha20-Poly1305) of the WAL and snapshots with per-epoch DEKs, rotation and purge; the root key is sealed by an explicit `SealingStrategy`; plaintext is an explicit choice (`WithoutEncryption`); a sealed shard refuses proposals and reads with `ErrBarrierSealed`. [architecture.md](seal-barrier/architecture.md) | [seal-barrier/](seal-barrier/) |
| shard-pause | shipped | `Host.PauseShard(ctx, shardID)` and `Host.ResumeShard(ctx, shardID)` stop and restart one shard's replica on this host without unloading it, so that the embedder can seal the shard's barrier at runtime while the shard stays loaded (sealed means only unsealing). A paused replica keeps its log, state machine, membership and persisted Raft state as they were, writes nothing to the log or the snapshot store, and `ShardInfo` reports it paused. **Proposals:** a proposal, session operation, membership change, `ExecuteOnLeader` or leader-transfer request returns `ErrShardPaused` (`errors.Is`-matchable, `Temporary` reports true), and proposals forwarded by peers are not accepted; one accepted before the pause is never applied while paused and completes after resume, ends at its context deadline, or returns the maybe-applied `NotLeaderError` if leadership moved. **Applies:** no entry is applied while paused, including committed entries already handed to the apply worker; they apply in log order after resume. **Reads:** ReadIndex, `SyncRead`, LeaseRead, stale and follower reads return `ErrShardPaused`; read requests forwarded by peers are not served. **Leadership:** a paused replica neither campaigns, votes, replicates nor acknowledges; a paused leader first transfers leadership to an up-to-date voter if one is reachable, and otherwise stops heartbeating so the remaining members elect a leader if they hold a quorum. **Snapshots:** none is taken, sent, received, imported or recovered while paused, and a pause never exhausts snapshot recovery into a shard failure; a due snapshot or recovery runs after resume. **Timing:** `PauseShard` returns once no log, apply or snapshot work of the shard is in flight, bounded by its context; only then may the embedder seal the barrier. `ResumeShard` is called after unseal and rejoins the replica as a follower with a fresh election timeout. Both are idempotent; a pause is not persisted, so a restarted replica starts unpaused. A barrier sealed without a pause still takes the sealed-write hold (#296), never a permanent shard failure. Seam: go-qrdb pauses every shard of a node before sealing it at runtime and resumes them after unseal. | [seal-barrier/](seal-barrier/) |
| shamir-primitive | shipped | The GF(2^8) split/combine primitive and the share-custody interfaces; the engine never persists Shamir shares ([engineering.md §6](shamir-primitive/engineering.md#6-persistence)). [architecture.md](shamir-primitive/architecture.md) | [shamir-primitive/](shamir-primitive/) |
| memory-enclave | shipped | Key material is held in go-enclave locked memory (`ByteAccess`, from `enclave.NewMemory`) and sealed enclaves (`Sealed`), consumed directly, never aliased or wrapped. [architecture.md](memory-enclave/architecture.md) | [memory-enclave/](memory-enclave/) |
| observability | shipped | Metrics, logging and event notifications. [architecture.md](observability/architecture.md) | [observability/](observability/) |
| compatibility | shipped | The pinned format-version policy, every version discriminator, the interface requirements for embedders, and upgrade notes for each release. [architecture.md](compatibility/architecture.md) | [compatibility/](compatibility/) |
| contrib-dragonboat | shipped | A Dragonboat API compatibility layer over the `Host`, in its own module under `contrib/dragonboat/`. | [multi-group-host/](multi-group-host/) (architecture.md, Package structure) |
| testing | shipped | The test infrastructure: suite organisation, build tags, fault injectors, fuzzing, linearizability verification, coverage gates and benchmark regression. [overview.md](testing/overview.md) | [testing/](testing/) |
| release | shipped | Public releasability: what may be published from this tree, the development-tooling exclusion list and the `test/releasability` gate; the release recipe (`make release-candidate`, `make release-alpha`). [public-releasability.md](release/public-releasability.md) | [release/](release/) |

Every feature is shipped; no feature is in progress, planned or future.

## Decisions

Non-goals, decided and not planned. Each row is a scope decision, not a missing
feature; its reason is the one the cited architecture document gives.

| Non-goal | Decision | Reason | Source |
|---|---|---|---|
| Tracing and a health endpoint | Not provided | `context.Context` is threaded through the public API, so an embedder carries its own spans; `Host.HealthCheck()` returns a value, and serving it is the embedder's job. No OpenTelemetry integration, no metrics server. | [observability/architecture.md](observability/architecture.md#what-this-feature-deliberately-does-not-do) |
| Joint consensus | Rejected: one membership change at a time | Joint consensus needs two quorums live at once, a second configuration in flight, and a state machine with more states to get wrong; one-at-a-time reaches the same configurations by composition, at the cost of more round trips on a rare operation. | [membership-changes/architecture.md](membership-changes/architecture.md#rejected-joint-consensus) |
| Snapshot backup and retention policy | Not provided | Which exported snapshots to keep, and where, belongs to the layer that owns the data directory; `ExportSnapshot` and `ImportSnapshot` are the mechanism. | [snapshots/architecture.md](snapshots/architecture.md) |
| Client sessions on on-disk state machines | Rejected: refused with `ErrSessionsUnsupportedOnDisk` | The session table travels only in the snapshot stream; a disk state machine that reports a non-zero applied index from `Open` skips that recovery on restart, so the replica would come up with no sessions and answer `SESSION_EXPIRED` for entries its peers apply. Make commands idempotent and propose them without a session. | [client-sessions/architecture.md](client-sessions/architecture.md#rejected-sessions-on-disk-state-machines) |

## Interfaces

- Go SDK: the `quicraft` package (`pkg/`) and `pkg/config`: [multi-group-host/api.md](multi-group-host/api.md).
- State machines: [state-machines/api.md](state-machines/api.md).
- Seal: [seal-barrier/api.md](seal-barrier/api.md).
- Wire protocol: [wire-protocol/](wire-protocol/).
- CLI: none; go-quicraft ships no binary.

## Release criteria

The current milestone is **Dev complete**: go-quicraft tags `-alpha` only, cuts an `-rc.N` only if a consumer needs one, and keeps the rc tags already cut pinned. It closes when:

- #309 is closed: `TestLinearizability_3Node_LeaderFailover` and its soak pass reliably (testing). Met.
- #283 is closed: the spec-review scrub of `docs/` is verified by the department and its fixes have landed. Met.
- Every feature row planned for Dev complete is shipped, shard-pause included (a go-qrdb seam row); heartbeat-under-fsync (#390), planned after Dev complete, is shipped too (#398). Met: every Dev complete row in Features is `shipped` (pq-key-exchange, #412, planned after Dev complete, is shipped too), and #363, #369, #370 and #390 are closed.
- Every seam row go-trusted-platform's SPEC names on go-quicraft is shipped; it names none, since go-trusted-platform reaches consensus only through go-qrdb. Met.
- Nothing is dev complete until go-trusted-platform is, and every lower library has a stable, complete API. Not met: this holds until go-trusted-platform is dev complete, which this repository does not decide.
- CI conforms to `../handbook/software/conventions/ci-standard.md`: build, unit, integration, security and the latest-version check (`make build`, `make test`, `make integration-test`, `make security`, `make deps-check`), all in the project's devcontainer image (release). Met (ci-template v1.24): the `image` job builds the toolchain and integration images from `build/docker/devcontainer/` at the commit under test and runs `make lint`, `make build`, `make coverage` (the unit suite under `-race` with the total and per-package coverage floors) and, on the full tier, `make security`, `make deps-check` and `trivy image` in the toolchain image; the `integration` job (full tier) runs `make integration-test` in the content-keyed integration image (`make -s it-image`) and the `soak` job (nightly and dispatch only) loops it; `modules`, `cross-arch`, `netfault`, `errfs`, `zero-rtt`, `contrib-dragonboat` and `fips` (all full tier) and `fuzz-seeds` (nightly and dispatch only) each run their `make` target in the toolchain image. A docs-only PR runs lint and the releasability scan alone. No job installs Go on the runner; only `arch-gates` (a grep over the source) and `ci-gate` (on the `ci-gate` runners) run on the runner's shell.

Then release candidates are tagged, deployed to staging and released; production is considered after staging.

The v1.0.0 bar, on top of the above:

- go-enclave is at a public release (memory-enclave). Open: #427, which waits on go-enclave's public release.
- The compatibility policy is acknowledged (`release-versioning.md` §1, §8) (compatibility). Met: the owner acknowledged it on 2026-10-20, every answer as written (#404).
- A publication mechanism for agent files exists (release). Met (#436): the agent files are published and scanned like any other file; `.gitattributes` export-ignores only the private forge workflows and the release trust anchor, and `test/releasability` enforces the list ([release/public-releasability.md](release/public-releasability.md)).
- The company CI template is neutralised for the public tree: `.github/workflows` names no internal repository or domain (release). Met (#436): every published workflow passes the `test/releasability` scan; the private forge's own workflows are export-ignored ([release/public-releasability.md](release/public-releasability.md)).
- Every wire and on-disk format version stays pinned until v1.0.0 ships (wire-protocol, log-storage). Holding: every discriminator is at its pinned value ([compatibility/engineering.md](compatibility/engineering.md#version-discriminators)); it is met when v1.0.0 ships with none moved.
- The rest of the bar is rows V1-A to V1-M of the `v1.0.0 bar` milestone, one issue each. V1-A (#437: doc drift, the upgrade and root-key-compromise runbooks, the non-goal decisions below) is Met; V1-B to V1-M are Open until their issues close.
