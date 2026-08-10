# Changelog

All notable changes to QuicRaft will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### BREAKING

- **At-rest ciphertext is now bound to its position; existing encrypted snapshots and WALs cannot be read by this version.** The barrier previously authenticated only the epoch, so every frame sealed under one DEK+epoch authenticated identically no matter where it sat. An attacker with write access to the ciphertext — but no key — could reorder frames, splice a frame from one artifact into another at the same epoch, or truncate a trailing run, and every individual frame still verified. Each snapshot frame and each WAL record now carries its identity in the AEAD's additional authenticated data: `"QRSNAP1" | ShardID | ReplicaID | SnapshotIndex | ChunkSeq` for snapshot frames, `"QRWAL1" | WALShardIdx | SegmentID | RecordSeq` for WAL records. Moving, duplicating, or substituting one now fails authentication.

  The binding is **unconditional and there is no marker on disk**, which is deliberate: a marker is only useful if a reader may fall back to the weaker epoch-only binding, and that fallback would itself be the vulnerability — an attacker would simply strip the marker. Every candidate location for one is unauthenticated anyway (the WAL's CRC32 is a checksum, not a MAC, and `snapshot.meta` carries no MAC at all). The scheme is chosen by code, never read off disk, and a reader never retries with a weaker AAD. Both on-disk framings are otherwise byte-identical to before.

  **There is no migration: wipe existing data directories, or re-sync the node from its peers**, consistent with the `0.2.0-alpha` break. `waldb.Reencrypt` re-serializes every record and so produces correctly-bound output for a barrier migration. Barriers in hardware mode (`IsHardwareMode`) keep epoch-only binding as an explicit, logged capability limitation — the underlying encryptor accepts no caller AAD — decided once at construction from the deployment, never per artifact and never in response to a decrypt failure.

  API changes that follow from this: `Barrier` gained `EncryptWithEpochContext`, `DecryptWithContext` and `IsHardwareMode` (all already present on `*seal.Barrier`); `ReencryptSnapshotFile` gained `shardID, replicaID, index` parameters — a `SnapshotReencryptFunc` implementation already receives all three. A `Barrier` implementation that does not support caller-supplied AAD is refused when the WAL opens rather than silently writing weaker ciphertext.

- **`snapshot.meta` is now self-describing and grew to 72 bytes.** Every record written by this version carries a `QRSNAPMD` magic, a format version and a body length ahead of the existing fields (`Index | Term | ShardID | ReplicaID | CreatedAt | Epoch | Compression`), so a reader can tell a field that is ABSENT — written by a different version — from bytes that are CORRUPT. Body fields are append-only from here: appending one bumps the version and leaves every existing offset alone.

  Compatibility is one-way and requires no migration step. **Records written by earlier versions are still read**, positionally, at their original offsets: the 40-byte layout (the snapshot receive and import paths, which recorded no `Epoch`), and the 48- and 56-byte layouts (the snapshot save path, with `Epoch`, and with `Epoch` + `Compression`). Nothing rewrites an existing record; a legacy directory keeps its record until the snapshot rolls off. **An older binary cannot read a record written by this version** — it will read one as corrupt, which is fail-safe there (it suppresses DEK purging) but means a downgrade must be treated as a wipe-and-restore, consistent with the `0.2.0-alpha` on-disk break

- `TransportConfig.MaxDecompressedSize` is pinned to `MaxFrameSize` and rejected if set to anything else. Configuring it lower bought no bomb protection — a peer that wants the node to buffer 16 MB can simply send an uncompressed 16 MB frame — while creating an asymmetric wire contract in which a receiver silently stopped accepting replication that every sender considered valid. `SetDefaults` still maps 0 to the pinned value, so deployments that never set it are unaffected

### Fixed

- **A connection to a removed replica was never closed.** quic-go's keep-alive period sits below the idle timeout, so the connection was PINGed indefinitely and never idle-closed, while the transport's cleanup only reaps connections whose context is already dead. A replica removed from the configuration but still running therefore pinned a connection, its pooled streams and its epoch records for the life of the process. A committed removal now releases the connection — but only when no other shard still resolves to that address (one connection is address-keyed and multiplexed across shards) and no snapshot transfer to it is in flight. This is deliberately event-driven rather than a sweep: registry entries are legitimately absent under a live, healthy connection during shard teardown-and-restart, speculative pre-registration rollback, and a joining replica's first contact, so a sweep would close healthy connections in exactly those windows
- **A leader-change notification fired for campaign churn.** Broadening the change check from the leader ID to the `(leader, term)` pair — so a same-leader re-election at a higher term is not swallowed — also made a replica that cannot win publish every term it campaigned through while the leader was still unknown. That overwrote the host's cached leader with "none" and could complete pending proposals with a `NotLeaderError` for a leadership loss that never happened
- **A restarted peer could black-hole every vote response for ~30 seconds.** Survivors held outbound QUIC connections to the dead incarnation; `stream.Write` succeeded locally while nothing arrived until `MaxIdleTimeout`. With two nodes down in a five-node shard every survivor's grant is mandatory, so one silently broken direction left the cluster leaderless with quorum available. Outbound connections are now attributed to a peer incarnation and evicted when they cannot be
- **A leader could re-dial one follower thousands of times per second.** `sendAppend` bounded a `Replicate` message in bytes but never in entry COUNT, so a follower catching up on small entries drew a message every peer must reject — and that rejection was indistinguishable from a dead connection, so the sender tore down, re-dialed, and retried the identical message forever. Entry count is now bounded, oversized batches are split, and a rejected frame evicts only that stream
- **PreVote messages were withheld behind WAL persistence.** They persist nothing, so the ordering rule never applied to them; under sustained write load `commitPending` is set almost continuously and pre-vote traffic never left the node, so elections could not complete
- **A replica recovering from a snapshot was absent from consensus entirely** — no votes, no heartbeat replies — while its tick kept campaigning. Two concurrent recoveries could dissolve quorum with every node up. The gate now covers state-machine access only
- **A replica removed from the configuration could still elect itself**, counting its own vote toward a quorum it was not part of — two leaders in one term
- A same-term vote was granted while a live leader was known, and the grant then silently blackholed higher-term vote requests for a full election timeout
- `lastTerm` fabricated term 0 when the log was unreadable, so a replica holding a committed entry no one else had would vote for a candidate missing it
- A recovered commit index ahead of the last recovered entry was trusted absolutely, letting a follower acknowledge entries it did not have
- A follower whose election timer expired could suppress its own election to enter quiesce, then run at 30x the election timeout while leaderless
- Unload and reload raced through a random `select`, which could leave a shard permanently unable to apply while it still voted and won elections
- Apply-gap detection was disabled at `lastApplied == 0`, so a dropped first batch silently diverged the state machine
- A data race on the snapshot recovery markers could abandon a recovering replica's snapshot install, leaving it admitted to the configuration and starving

### Changed

- `ShardInfo.Health` reports `recovering` and `snapshotting` instead of `healthy` while those gates are up; `Host.DiagShardGates` exposes them directly. A replica invisible to consensus previously reported healthy
- `BarrierConfig.Validate` is exported so a runtime-built config can be rejected instead of panicking

### Security

- **A LogDB that persists encrypted records but cannot name their epoch no longer has its keys destroyed.** The host-wide DEK purge floor treated any LogDB that does not implement `EpochAwareLogDB` as imposing no floor at all and purged on the snapshot floor alone. That is correct for an in-memory store, but `logdb.LogDB` is a public extension point: a durable store that encrypts under the host barrier without implementing the optional epoch interface had the DEKs its own records still needed purged irreversibly. Purging now proceeds only on a positive `AtRestNonDurable` declaration — a store that declares `AtRestBarrierEncrypted`, declares `AtRestUnprotected`, or declares nothing at all suppresses purging and logs why. Silence costs an unbounded keyring, which is recoverable; the previous behavior cost the keys, which is not. The per-shard-barrier floor already failed safe here and is unchanged
- `WithShardBarriers` without `WithBarrier` or `WithLogDB` is refused: the default WAL takes one barrier, so per-shard encryption alone produced encrypted snapshots over a plaintext log while an operator's spot-check showed encryption working
- **A failed sweep of a snapshot decrypt scratch file is now reported.** Both decrypt paths (recovery and snapshot send) write a full plaintext copy of the state machine to a sibling scratch file and remove it when done; the sweeper reclaims what a crash orphaned. A failed removal — a read-only mount, a permissions or ownership change — was logged at DEBUG and returned to no caller, so the plaintext copy stayed next to its ciphertext for the life of the snapshot directory with nothing to signal it. Failures (and a directory the sweep cannot even list) now log at WARN and increment a process-wide counter, `engine.StaleDecryptSweepFailures()`; any increase warrants an alert
- **One snapshot directory with an indeterminate epoch no longer hides the others.** The DEK purge-floor scan returned on the first directory it could not evaluate, so its log named one directory while the next scan named the next. The scan now visits every directory and reports them all in a single record, with the reason for each (`legacy_record_without_epoch` vs `unreadable`). The VERDICT is unchanged and deliberately so: an epoch that cannot be established may be any epoch, so such a directory suppresses purging for the barrier that could have sealed it — in per-shard-barrier mode that is one tenant's barrier, and other tenants keep purging normally


## [0.2.0-alpha] - 2026-08-06

### BREAKING

- **On-disk and wire formats changed; there is no migration from `0.1.0-alpha`.** Witness policy became replicated state: `MaxWitnesses` and `RequireFullReplicaMajority` now live in `proto.Membership` and travel through the log, snapshots, the WAL, and LogDB. `Membership` grew from an 8-byte base to 24, `ConfigChange` from a 36-byte fixed section to 52, and the WAL membership section by 9 bytes. `config.WitnessConfig` is now only a bootstrap seed; the policy is changed at runtime through `Host.RequestUpdateWitnessPolicy` (and its `Sync` variant). Wipe existing data directories before upgrading

### Security

- **Sealed barriers no longer downgrade to plaintext.** A barrier that is configured but sealed means encryption is temporarily unavailable, not that the data is plaintext. Three paths conflated the two and durably wrote plaintext: `saveSnapshot` (reachable via `EmergencySeal`, so the control inverted during incident response), `ImportSnapshot` (recorded `Epoch: 0` unconditionally), and `ReencryptSnapshotFile`. All now fail closed with `ErrSnapshotBarrierSealed`. In `ReencryptSnapshotFile` a sealed *source* was worse than a downgrade: it copied ciphertext and recorded it as plaintext
- Imported snapshots are re-encrypted under the importing host's own barrier, so they are encrypted at rest like every other snapshot for the shard
- Certificate revocation configured fail-closed with no CRL or OCSP source was a silent total no-op, disabling the only mechanism for evicting a compromised certificate
- `Term: 0` wire messages bypassed every term check, letting a non-leader advance a follower's commit index
- A client could kill a shard cluster-wide by pairing `ProposeWithSession` with `CloseSession`: both batched into one `Apply`, which then errored, failing the shard deterministically at the same index on every replica
- `BarrierConfig.Validate` is now exported so embedders building a config at runtime can reject a bad algorithm instead of taking the process down on `NewBarrier`'s panic

### Fixed

- **Two leaders could be elected in one term.** Vote grants were counted by ranging the vote map while quorum came from the live voter set, so a `RemoveNode` committed mid-election shrank quorum while a removed node's stale vote survived. Votes are now tallied over configuration voters, as etcd and dragonboat do
- **An empty snapshot could be streamed to a follower.** The receive path never recorded `FileSize`, so dedup re-delivery published 0 alongside a non-empty path, and the receiving replica recovered its state machine from zero bytes
- **A WAL shard could be silently discarded and then truncated.** Any non-`ENOENT` `Stat` error was treated as `ENOENT`, so `EACCES`/`EIO`/a stale NFS handle skipped recovery and reopened the surviving segment with `O_TRUNC` — a double vote plus permanent loss of acknowledged writes
- **Snapshot recovery could roll the state machine backwards**, regressing `lastApplied` into a permanent `ErrApplyGap` outage
- A stale pending snapshot could supersede a newer one, rewinding the `LogReader` behind the apply frontier or failing the shard, and letting a rejected snapshot roll membership backwards
- A compacted index returned a term instead of `ErrCompacted`, suppressing the leader's `InstallSnapshot` fallback so a lagging follower could never catch up
- Raft could accept entry batches the WAL could never persist: one `Update` was one WAL record, and a record must fit one segment. Batches are now split across records
- A log-replay joiner kept membership `{self}` forever and could elect itself a single-node leader that acknowledged writes the cluster discarded
- `replayConfigChangesTo` applied uncommitted configuration changes
- The WAL fsynced only its root's immediate parent, losing acknowledged writes when the root sat more than one level below the durable point
- A non-monotonic persisted `compactedTo` turned `ErrCompacted` into a permanent `ErrLogGapDetected`, so raft never fell back to `InstallSnapshot`

### Changed

- Reads no longer block on a writer's fsync. The WAL shard mutex is split: segment file state keeps the existing lock, while the in-memory maps readers consult move to a separate `RWMutex` taken only for the brief commit. Previously every read for every raft group on a WAL shard waited out the fsync
- Toolchain pinned to Go 1.26.5 across `go.mod`, Docker, and CI
- `contrib/dragonboat` gained the `replace` directive it always needed; it previously resolved its parent only through a `go.work` outside the repo and could not be built or tested in CI

### Added

- `BarrierConfig.Validate` is exported, so a caller assembling a barrier config at runtime (from a file, an API request, or per-tenant settings) can reject a bad algorithm — or a non-FIPS-approved cipher under FIPS mode — instead of taking the process down on `NewBarrier`'s panic
- `BenchmarkThroughputQUIC`: the first end-to-end transport throughput benchmark. All prior transport benchmarks measured serialization only, so no test could observe a change to the QUIC receive window
- Per-shard barriers via `WithShardBarriers(fn func(shardID uint64) Barrier)` option, enabling multi-tenant or per-shard key isolation while maintaining backward compatibility with single host-wide barrier setup
- Logical snapshot re-encryption on receive: `reencryptReceivedSnapshot` decrypts sender's plaintext snapshots and re-encrypts under receiver's own barrier before install, eliminating shared cross-node keys for at-rest encryption
- WAL re-encryption primitive: `waldb.Reencrypt(srcDir, srcBarrier, dstDir, dstBarrier)` performs zero-downtime logical migration of WAL through public LogDB surface, enabling none→keyed encryption migrations and key rotation
- Per-shard barrier routing in snapshot transport via `host.barrierForShard(shardID)` for multi-tenant isolation

### Changed

- Host snapshot pool DEK purge now dispatches per-shard when `WithShardBarriers` is configured, preventing one tenant's barrier purge from removing epochs live under another tenant's barrier

## [0.1.0-alpha] - 2026-03-10

### Added

- Multi-group Raft consensus with per-shard configuration and sharded ReadyMap (64 buckets) for O(1) dispatch
- Full Raft specification compliance: leader election, log replication, membership changes, snapshots, PreVote, CheckQuorum, leadership transfer
- Linearizable reads via ReadIndex and LeaseRead
- QUIC-only transport built on quic-go with mTLS and multiplexed streams
- QUIC 0-RTT reconnection support for reduced handshake latency
- Custom fixed-layout binary wire format with zero-copy semantics (no protobuf dependency)
- Algorithm-agile at-rest encryption (AES-128/192/256-GCM, ChaCha20-Poly1305, XChaCha20-Poly1305) with automatic hardware detection, barrier/seal/unseal pattern, and epoch-based key rotation
- HKDF-SHA256 derived data encryption keys with brute-force protection
- 16-shard write-ahead log (WAL) for parallel persistence with fdatasync durability and CRC32 checksums
- WAL compaction and crash recovery with log replay
- Engine worker pipeline: step, commit, and apply workers with snapshot pool
- Client session management for at-most-once proposal delivery and cross-replica deduplication
- Three state machine interfaces: StateMachine, ConcurrentStateMachine, and DiskStateMachine
- Optional NALookup interface for zero-allocation reads
- Optional Hasher interface for cross-replica consistency checks
- RSM adapter wrapping state machine interfaces with two-phase snapshot support
- Cluster discovery via static peer lists, UDP multicast with HMAC-SHA256 authentication, and DNS SRV records
- Automated bootstrap orchestration with quorum validation
- Global and per-shard rate limiting for proposal backpressure
- Lock-free hot paths using padded atomics to prevent false sharing
- 16-shard QUIC stream pool with dedicated heartbeat stream (stream 0)
- Pooled byte buffers on send and receive paths via sync.Pool
- Connection coalescing via singleflight.Group
- Prometheus metrics for proposals, commits, elections, snapshots, transport, and WAL operations
- Graceful ordered shutdown: step, commit, apply, snapshot with drain guarantee
- In-memory KV state machine for testing and examples
- Porcupine-based linearizability test suite
- 500+ unit tests with race detector coverage
