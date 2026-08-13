# Changelog

All notable changes to QuicRaft will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### BREAKING

- **`NewPrometheusMetricsCollector` no longer labels most metrics with `shard_id` by default.** The collector targets a library whose documented design point is 10,000+ shards on one host, and per-shard labelling cost far more than the count of shard-labelled metric names suggested: a histogram child is one series per bucket plus `_sum` and `_count`, so each active shard cost a measured **113 active Prometheus series**. At 10k shards that is ~1.13M series from a single host, enough to destabilise a shared Prometheus — a failure that lands on every other tenant of it, with no way for an operator to opt out.

  The new `config.MetricsConfig.PerShardMetrics` selects the mode, and it defaults to **off**: proposal/commit/apply/ReadIndex latency, proposal batch size, snapshot counts and snapshot byte histograms now aggregate to one host-level series each, taking a shard to a measured **6 series**. Pass `NewPrometheusMetricsCollectorWithConfig(config.MetricsConfig{PerShardMetrics: true})` to restore the previous behaviour. The mode is fixed when the collector is built, because a Prometheus vector's label set is decided at construction; it is deliberately not a `HostConfig` field, which could not take effect.

  **Alerting is unaffected.** Every signal an operator pages on keeps its `shard_id` in both modes: `elections_total` (leader change), `proposals_dropped_total`, `messages_dropped_total`, `snapshots_rejected_total`, and both `quicraft_security_*` counters. What aggregates is volume-and-latency telemetry, whose per-shard breakdown is a debugging aid rather than an alerting input.

  Aggregated series keep the **same metric name** and drop only the `shard_id` label, so a rule written as `sum without(shard_id) (...)` is correct in both modes; dashboards that group *by* `shard_id` collapse to a single host-wide series rather than reporting wrong numbers. The change is made visible in three places: the `# HELP` text of an aggregated series says so, the new `quicraft_metrics_per_shard_enabled` gauge exports the mode (1/0) so a dashboard can branch on it without reading the config, and `docs/operations/deployment.md` gained a "Metrics cardinality" section with the measured budget.

- **`seal.NewBarrier` returns `(*Barrier, error)` instead of panicking on an invalid configuration.** An exported constructor in a public package that panics takes down the EMBEDDER's process, and `pkg/seal` is embedded in go-qrdb, go-xkms and go-quicbaas. The previous doc comment told runtime callers to call `BarrierConfig.Validate` first — a convention, not a guarantee, and the invalid configs it guards against (an unknown algorithm, a non-FIPS-approved cipher under FIPS mode) are exactly what a config read from a file, an API request, or per-tenant settings produces. An operator's typo must be an error the embedder can reject, not a crash.

  `seal.MustNewBarrier` is the panicking form, following the `regexp.MustCompile` precedent, for configs built from compile-time constants where an invalid value is a programmer error. `NewEpochBarrier` remains an alias of `NewBarrier` and so changes signature with it; `MustNewEpochBarrier` completes the alias set. `crypto.NewBarrier` re-exports the new signature and `crypto.MustNewBarrier` is added.

  `BarrierConfig.Validate` stays exported and is unchanged, but is now an admission-time convenience rather than a safety requirement: `NewBarrier` applies the same rules and returns the same error identity, so a caller that forgets it no longer risks a panic.

  **Downstream call sites needing coordination** (not edited here): `go-qrdb/pkg/seal/barrier.go:244` (`epochBarrier := quicraftseal.NewBarrier(epochConfig)` — inside `NewStorageBarrier`, which already returns an error, so the new error threads straight through) and `go-qrdb/pkg/seal/strategy.go:122` (`var NewEpochBarrier = quicraftseal.NewBarrier` — a re-export whose signature changes with it, making this a breaking change in go-qrdb's own API surface). go-xkms and go-quicbaas do **not** call this constructor: their `seal.NewBarrier` is their own package's four-argument function, and neither imports `go-quicraft/pkg/seal` at all — only `pkg/crypto/shamir`.

### Added

- `config.MetricsConfig` and `quicraft.NewPrometheusMetricsCollectorWithConfig`, controlling metric label cardinality (see BREAKING above).
- `quicraft_metrics_per_shard_enabled` gauge, reporting whether per-shard metric labels are enabled.

- **`quicraft.Temporary(err) bool` and `quicraft.NotLeader(err) (replicaID uint64, ok bool)` classify the package's ~60 sentinels.** The package mixed transient conditions (`ErrTimeout`, `ErrSystemBusy`, `ErrShardNotReady`, `ErrNotLeader`) and permanent ones (`ErrShardNotFound`, `ErrInvalidOperation`, `ErrRejected`, `ErrClosed`) with nothing distinguishing them, so a caller could not ask "should I retry?" without string-matching or enumerating sentinels — and four downstream products would each have re-derived the classification and disagreed.

  Every sentinel is classified deliberately, not by pattern. `Temporary` is true only for conditions that clear on their own without operator or caller intervention: `ErrBusy`/`ErrSystemBusy`, `ErrTimeout`, `ErrAborted`, `ErrDropped`, `ErrShardNotStarted`, `ErrShardNotReady`, `ErrNotLeader`, `ErrReadIndexNotReady`, `ErrRotateInProgress`, `ErrSnapshotInProgress`. **Everything else is permanent, including errors this package never produced** — retrying a permanent failure forever converts one bad request into sustained load and hides the real fault. Several plausible-looking candidates are excluded on purpose and the reason is recorded next to each in `pkg/errors.go`: `ErrStaleConfigChange` (the identical request fails forever; the caller must re-read membership and build a new one), `ErrDraining` (a one-way transition into shutdown), `ErrCanceled` (the caller's own decision, whereas a server-side deadline surfaces as `ErrTimeout`), `ErrSessionLimitExceeded`, `ErrBarrierSealed`, `ErrShardBarrierUnresolved`, `ErrDiskFull`, `ErrLogDBWrite` and `ErrShardFailed`.

  `NotLeader` surfaces `NotLeaderError.LeaderID` so callers redirect without type-asserting; `ok` is true for any not-leader error and `replicaID` is 0 when the leader is unknown. Both use `errors.Is`/`errors.As`, so wrapped errors and the typed errors' `Is` methods (`*BusyError`, `*NotLeaderError`) classify correctly. A table test asserts every exported sentinel, and a companion test parses `errors.go` and fails when it declares an exported `Err*` var the table does not classify — so a sentinel added later cannot become silently permanent without someone deciding that it should be.

### Fixed

- `docs/api/reference.md` and the metrics tables in `docs/operations/deployment.md` were stale: they named `readindex_latency_seconds` (the metric is `read_index_latency_seconds`), showed `messages_sent_total`/`messages_received_total` as carrying `shard_id`/`to`/`msg_type` when those labels belong to the opt-in `*_detailed_total` variants, and omitted `piggyback_commits_total`, `messages_dropped_total`, `snapshots_rejected_total` and both security counters entirely.

### Documentation

- **The deferred `SessionExpiryEntries` decision was resolved, not carried forward.** This section previously recorded that the default had to stay 0 because the threshold was node-local configuration the apply path treated as replicated, and proposed replicating it as the alternative. That alternative shipped: see the session-policy entry under `0.3.0-alpha` BREAKING. `TestSessionExpiry_DisagreeingThresholdsDiverge`, which demonstrated the divergence, was replaced by `TestSessionExpiry_ReplicatedThresholdGovernsBothReplicas`, which asserts that two replicas with different LOCAL configuration now apply identically — and, in a second subtest, that they both follow the shard's policy when it enables expiry rather than merely agreeing to do nothing.

## [0.3.0-alpha] - 2026-08-12

### Security

- **A remote peer could force a term REGRESSION, breaking Election Safety.**
  `Term` is a plain uint64 in the wire encoding with nothing bounding it, and
  `raft.step` adopted a higher term verbatim. A peer naming a term at the top of
  the range drove the receiving replica to it; `becomeCandidate`'s `r.term++`
  then wrapped to **term 0** on the next campaign. A replica whose term has gone
  backwards can vote a second time in a term it has already voted in, which is
  an Election Safety violation reachable from a single message. Even without the
  wrap, a replica driven to the ceiling can never campaign again, so one message
  permanently removed a voter.

  Closed at two layers, because they fail differently: `step` now refuses a wire
  message whose term falls in a reserved band below the ceiling
  (`ErrTermCeilingExceeded`), which stops a replica being parked there; and the
  increment in `becomeCandidate` is guarded, so the wrap is impossible even if
  some future path reaches a ceiling term without passing through `step`. Terms
  advance by one per election, so no cluster reaches the reserved band by
  running — refusing it costs nothing real.

  Found by the raft fuzz harness (`FuzzRaftStepSequence`) while verifying an
  unrelated change; it predates this release line.

This release is a security and durability hardening pass over `0.2.0-alpha`. It
is **not compatible with `0.2.0-alpha` data directories**: the WAL record
framing, the snapshot-record membership section, `snapshot.meta`, and the
at-rest AEAD binding all changed. The engine fails closed with a specific,
named error on each rather than misparsing, but there is **no migration** —
wipe the data directories, or re-sync each node from its peers.

Source compatibility for embedders is better than the format break suggests:
`go-qrdb`, `go-xkms` and `go-quicbaas` all build against this release without
source changes. The interface additions recorded under BREAKING affect only
embedders that IMPLEMENT those interfaces, which none of them do today.

### BREAKING

- **`msg.From` is now bound to a cryptographic NODE IDENTITY recorded in committed membership, not to an address.** `msg.From` — the replica ID Raft counts toward election and commit quorum — travels in-band and is not authenticated by mTLS. The previous gate required the peer's verified certificate to cover the address committed membership assigns that replica. That closed registry poisoning and left three holes: a **promiscuous certificate** (a wildcard SAN, or one certificate carrying every node's SAN) covers every member's address, so its holder could assert any member's `From` and have forged `ReplicateResp` and `RequestVoteResp` counted toward quorum; **two replica IDs at one address** gave the single host behind it two quorum identities; and **address is not identity**, which is why a member that recovered at a new address had its quorum traffic dropped until a config change committed — and during quorum loss no config change can commit.

  Committed membership now records `Identities`, a replica ID to node identity map. The identity is `sha256(leaf.RawSubjectPublicKeyInfo)` rendered `spki:<hex>` — a hash of the node's PUBLIC KEY, so only the holder of the matching private key can present it, and it survives certificate renewal on the same keypair. Where an identity is committed it is the ONLY check performed and the address becomes routing metadata. A SAN URI (`quicraft://<deployment>/<replicaID>`) would bind the replica ID more directly and was rejected because it requires a certificate-issuance change in four downstream products; SPKI needs none, at the cost that the replica-ID-to-identity mapping must be supplied out of band.

  Wire and persisted format changes: `proto.Membership` gained a **v4 layout** — the v3 fixed region extended with the replicated session policy (`SessionExpiryEntries`, `MaxTotalSessions`, `MaxSessionsPerClient`, base 25 → 49 bytes) and an `Identities` map appended after `Removed`; **v3 still decodes**, so a node can replay a log whose index-1 entry an older binary wrote, and it decodes with no identities and no session policy. `proto.ConfigChange` gained a trailing `NodeIdentity` field, discriminated by the buffer's end rather than a version byte (a `ConfigChange` is exactly one `Entry.Cmd`, so its end is readable). The `waldb` snapshot-record membership section is likewise at **version 4**, accepting the range 3-4 on replay; it carries the session policy as a tail after `Identities` rather than in its fixed region, because its decoder parses straight-line and a grown fixed region would give v3 and v4 different map offsets. The session policy and the identity map landed in the same layout revision deliberately: shipping one as v4 and the other as v5 would cost downstream a second on-disk format break for a version nothing outside this tree has written.

  API: `Host.NodeIdentity()` reports this node's own identity; `Host.RequestAddNodeWithIdentity` / `RequestAddObserverWithIdentity` / `RequestAddWitnessWithIdentity` / `SyncRequestAddNodeWithIdentity` commit one; `Config.MemberIdentities` seeds them at bootstrap; the public `Membership` gained `Identities`. The identity is an ARGUMENT and is never captured from the joiner's connection — a leader that captured it would have only the certificate's address coverage to decide which connection speaks for the replica, so a promiscuous-certificate holder would register ITSELF as the target replica permanently, which is worse than no identity at all. `transport.MessageHandler` now takes a `PeerAuth` (identity plus certificate coverage) in place of a `senderAuthorized` closure.

  **Existing clusters keep working**: a replica with no committed identity falls back to the address check, and `TransportConfig.RequireMemberIdentity` (default **false**) refuses that fallback for operators who have finished migrating. Migration is per-replica, needs no downtime, and is documented in `docs/operations/compatibility.md`. Before enabling the strict mode, confirm through `Host.GetShardMembership` that every member carries an identity: a member added by an earlier build has none until an operator re-adds it with `RequestAddNodeWithIdentity`, and enabling the strict mode first drops that member's quorum traffic.

  **Identities survive a restart**, which is what makes the strict mode usable at all. They reach membership through the log, so a replica whose log has been compacted past the `EntryInitialMembership` at index 1 and the config-change entries has only the `logdb` snapshot-metadata record to relearn them from. That record now carries them end to end: `logdb.Membership.Identities`, the `memdb` deep copy, the `waldb` snapshot-record membership section at **version 4**, `host.go`'s two converters and `engine`'s save-path converter. Without any one of those a restarted node rebuilds an identity-free configuration — silently downgrading every member to the address check under the default posture, and dropping all quorum-bearing traffic under the strict mode. The `waldb` section accepts the **range 3-4** on replay rather than an exact match, so a record written before identities existed still replays and decodes with an empty identity map; its pre-version-byte length guard stays at the v3 minimum so those sections are not rejected as truncated before the version check runs.

  This is pinned by the shared LogDB conformance suite, which runs `memdb` and `waldb` through ONE set of assertions and reopens the database rather than reading back through the live handle — `waldb` answers `GetSnapshot` and `ReadState` from an in-memory map that `SaveSnapshot` fills from the caller's value, so a save/read pair inside one process never reaches the record encoding and would pass against a backend that serialized nothing.

- **The three session limits are now REPLICATED state, and `Config.SessionExpiryEntries` defaults to 100000 instead of 0.** `SessionExpiryEntries`, `MaxTotalSessions` and `MaxSessionsPerClient` were per-node `config.Config` values that the deterministic apply path read: `rsm.StateMachine.Apply` uses the expiry threshold as the interval of its checkpoint walk, `session.Manager.Register` refuses past `MaxTotalSessions`, and the response cache is bounded by `MaxSessionsPerClient`. Two replicas holding different values expire different sessions at different log indexes, or disagree on whether a session exists at all — after which one answers a session-managed proposal from its response cache while the other reports `ResultSessionExpired`. That is a **state-machine divergence with every Raft invariant intact**: identical log, identical commit index, identical applied index, no error raised anywhere and nothing in the protocol able to detect it. Nothing replicated the values and nothing checked them against peers.

  They now live in `proto.Membership` (see the v4 layout above), seeded at bootstrap from the founder's `Config` exactly as `MaxWitnesses` is seeded from `config.WitnessConfig`, and travel through the log, snapshots, the WAL and LogDB like the rest of committed membership. The apply path reads the COMMITTED values; local configuration is used only while a shard has none — a shard bootstrapped by an earlier build (its membership decodes as v3), or a `join=true` replica that has not yet replicated the shard's configuration. `MaxTotalSessions` is the "policy recorded" discriminator, because `config.Validate` rejects a zero value, which is what lets an expiry threshold of 0 keep its own meaning of *expiry explicitly disabled cluster-wide*.

  **A node whose local configuration disagrees with what its shard committed applies the shard's values and logs the disagreement at WARN**, naming both. It does not refuse to start: the committed value can only be changed through consensus, so a node that refused would be unable to participate in changing the thing that keeps it from starting. The warning matters because two of the three bound this process's memory — a replicated limit above the local one is a resource decision taken elsewhere, and must be discoverable from the node's own logs.

  **The default became safe to change only because of the above.** With expiry disabled the per-shard session table never shrinks, is serialized into every snapshot, and a client that dies without `CloseSession` leaks an entry that counts against `MaxTotalSessions` forever — but flipping the default while the value was node-local would have made a rolling upgrade diverge silently, which is why `0.3.0-alpha` shipped the analysis instead of the change. `SetDefaults` now fills 100000: 1000x the floor `Validate` enforces, and the value `docs/operations/deployment.md` already recommended. Because zero can no longer mean "disabled", **`config.SessionExpiryDisabled` is added** for deployments that want no expiry; it is `math.MaxUint64` and needs no special case anywhere, being an inactivity threshold no log reaches.

  **There is no config change for these yet.** Unlike the witness policy (`Host.RequestUpdateWitnessPolicy`), the committed session policy is fixed for the life of the shard, so `MaxTotalSessions` cannot be raised on a running shard. That is a deliberate scope decision and a known gap: a runtime change also requires replacing the apply path's read of the published membership with adoption from the apply stream itself, because commit-time adoption plus a lagging apply path would let two replicas switch policies at different points in the log — the same class of bug, equally silent. `rsm.StateMachine.sessionPolicyFn` records that requirement where the change would be made.

  API: the public `Membership` gained `SessionExpiryEntries`, `MaxTotalSessions` and `MaxSessionsPerClient`, so an operator can read what a shard actually committed rather than what any one node is configured with. `rsm.NewStateMachine` takes a `proto.SessionPolicy` and a policy accessor in place of three adjacent `uint64` parameters — transposing the two limits used to compile cleanly while capping a shard at `MaxSessionsPerClient` clients.

- **Every WAL record now carries a durability generation; the chunk header grew from 7 to 11 bytes and `walFormatVersion` is 2. Existing WAL directories cannot be read by this version.** WAL recovery could not distinguish two physically different situations and resolved the ambiguity by refusing to start. A batch larger than one block issues one `WriteAt` per block before a single `fdatasync`, so a power loss can persist an arbitrary SUBSET of those pages; losing a leading or interior page (rather than a whole block, which reads as a zero hole) left CRC garbage with valid records after it — indistinguishable from interior rot, where truncating destroys entries the node already acknowledged to its leader. The safe reading was to fail closed, so an **ordinary power loss could leave a node that would not boot**, with no in-package remedy and, for a single-node deployment, no exit at all.

  The chunk header is now `[CRC32:4][Len:2][Type:1][DurSeq:4]`, with `DurSeq` inside the checksummed region. `DurSeq` is the number of `Sync` calls the segment has COMPLETED — a durability generation, not a record ordinal. A record ordinal cannot discriminate (rot destroys records too, so "records are missing above the damage" is true in both cases and only the magnitude differs — a heuristic). The generation gives a proof: the writer only emits generation `g+1` after `Sync #(g+1)` has RETURNED, so the presence on disk of any record above `g` proves everything at or below `g` was acknowledged. Contrapositive: records carrying the segment's highest generation have no completed fsync behind them, so discarding them destroys nothing acknowledged. Recovery therefore **truncates and self-heals** when everything above the damage is one generation, and refuses with `ErrMidSegmentCorruption` when it spans two, or with `ErrRecordSequenceRegression` when a survivor sits BELOW the clean prefix (bytes this writer cannot have produced). The torn-multi-block refuse-to-start condition is now unreachable.

  Cost, measured A/B interleaved on an idle bench node: **+1.1 ns/op** on `encodeRecord` for a 48-byte payload and **+4 bytes** on disk per record, against a ~2.5 ms `fdatasync` — four orders of magnitude below the operation it rides on. Allocations unchanged; write amplification stays at 1.0x.

  **Segments with no format marker are now fatal** (`ErrWALFormatMissing`), reversing the previous "adopt an unmarked populated directory" rule: a pre-marker segment parsed by this version fails every CRC, yields an empty clean prefix, and the torn-tail repair would truncate the whole log to zero — a silent misparse presented as a recovery. A v1 marker gets `*WALFormatVersionError` naming both versions, deliberately not `ErrWALFormatMalformed`, because the file is intelligible and the remedy is an upgrade rather than a corruption hunt. The `payloadType` ladder is unchanged: it versions what is INSIDE a chunk, while the marker versions the framing around it, and a record whose framing cannot be parsed never reaches a payload-type check. **There is no migration: wipe existing data directories, or re-sync the node from its peers.**

- **An inbound peer whose certificate also covers THIS node's host is refused at the transport**, before it can occupy a connection slot or open a stream. A certificate that covers this member covers every member, so its holder can satisfy the address binding for any replica. Peers that share this node's machine are exempt — the same host under any spelling, or any loopback address — because a certificate is matched on host alone and no certificate policy can separate them. **A cluster running one wildcard or shared-SAN certificate across DISTINCT hosts will stop connecting.** This is the enforcing counterpart of the existing `StartShard` / config-change exclusivity checks, which inspect the LOCAL node's own certificate and are therefore misconfiguration detectors a malicious joiner simply declines to run. The check runs once per CONNECTION, not per message: probing member addresses per message thrashes the 4-slot per-connection decision cache and trips its 16-distinct-miss fail-closed cap, killing every connection on any shard with five or more members

- **Duplicate member addresses are compared canonically.** They were compared as raw strings, so `"node-b:5001"` and `"NODE-B:5001"` were two distinct members to config-change validation and ONE host to `x509.VerifyHostname` — which is what the receive-side gate ultimately matches on. Two committed replica IDs the certificate cannot tell apart is the same quorum forgery the duplicate check exists to prevent. Comparison is now on a canonical form (lower-cased host, trailing FQDN dot removed, IP literals normalised, port preserved) shared by the transport and by raft through a new `pkg/internal/netaddr` package — raft cannot import the transport, which is why the normaliser previously existed on one side only. It canonicalises SPELLING ONLY and never resolves DNS: config-change validation is replicated logic that must give the same answer on every replica and on every replay. `StartShard` and `validateAddNode`/`validateAddWitness`/`validateAddNonVoting` all reject spelling-variant duplicates, and a configuration that already holds one fails closed on the receive path

- **At-rest ciphertext is now bound to its position; existing encrypted snapshots and WALs cannot be read by this version.** The barrier previously authenticated only the epoch, so every frame sealed under one DEK+epoch authenticated identically no matter where it sat. An attacker with write access to the ciphertext — but no key — could reorder frames, splice a frame from one artifact into another at the same epoch, or truncate a trailing run, and every individual frame still verified. Each snapshot frame and each WAL record now carries its identity in the AEAD's additional authenticated data: `"QRSNAP1" | ShardID | ReplicaID | SnapshotIndex | ChunkSeq` for snapshot frames, `"QRWAL1" | WALShardIdx | SegmentID | RecordSeq` for WAL records. Moving, duplicating, or substituting one now fails authentication.

  The binding is **unconditional and there is no marker on disk**, which is deliberate: a marker is only useful if a reader may fall back to the weaker epoch-only binding, and that fallback would itself be the vulnerability — an attacker would simply strip the marker. Every candidate location for one is unauthenticated anyway (the WAL's CRC32 is a checksum, not a MAC, and `snapshot.meta` carries no MAC at all). The scheme is chosen by code, never read off disk, and a reader never retries with a weaker AAD. Both on-disk framings are otherwise byte-identical to before.

  **There is no migration: wipe existing data directories, or re-sync the node from its peers**, consistent with the `0.2.0-alpha` break. `waldb.Reencrypt` re-serializes every record and so produces correctly-bound output for a barrier migration. Barriers in hardware mode (`IsHardwareMode`) keep epoch-only binding as an explicit, logged capability limitation — the underlying encryptor accepts no caller AAD — decided once at construction from the deployment, never per artifact and never in response to a decrypt failure.

  API changes that follow from this: `Barrier` gained `EncryptWithEpochContext`, `DecryptWithContext` and `IsHardwareMode` (all already present on `*seal.Barrier`); `ReencryptSnapshotFile` gained `shardID, replicaID, index` parameters — a `SnapshotReencryptFunc` implementation already receives all three. A `Barrier` implementation that does not support caller-supplied AAD is refused when the WAL opens rather than silently writing weaker ciphertext.

- **`snapshot.meta` is now self-describing and grew to 72 bytes.** Every record written by this version carries a `QRSNAPMD` magic, a format version and a body length ahead of the existing fields (`Index | Term | ShardID | ReplicaID | CreatedAt | Epoch | Compression`), so a reader can tell a field that is ABSENT — written by a different version — from bytes that are CORRUPT. Body fields are append-only from here: appending one bumps the version and leaves every existing offset alone.

  Compatibility is one-way and requires no migration step. **Records written by earlier versions are still read**, positionally, at their original offsets: the 40-byte layout (the snapshot receive and import paths, which recorded no `Epoch`), and the 48- and 56-byte layouts (the snapshot save path, with `Epoch`, and with `Epoch` + `Compression`). Nothing rewrites an existing record; a legacy directory keeps its record until the snapshot rolls off. **An older binary cannot read a record written by this version** — it will read one as corrupt, which is fail-safe there (it suppresses DEK purging) but means a downgrade must be treated as a wipe-and-restore, consistent with the `0.2.0-alpha` on-disk break

- `TransportConfig.MaxDecompressedSize` is pinned to `MaxFrameSize` and rejected if set to anything else. Configuring it lower bought no bomb protection — a peer that wants the node to buffer 16 MB can simply send an uncompressed 16 MB frame — while creating an asymmetric wire contract in which a receiver silently stopped accepting replication that every sender considered valid. `SetDefaults` still maps 0 to the pinned value, so deployments that never set it are unaffected

### Fixed

- **A restarted replica could permanently fail to resume replication.** With the leader alive, healthy and committing, a replica that restarted underneath a saturated pipeline was never sent another entry and stayed thousands of entries behind indefinitely — the shard kept running on the remaining members, one replica short, until that replica was restarted again. The leader had 256 append messages outstanding to the peer's previous incarnation, so no acknowledgement for them could ever arrive. The in-flight window only drains from ABOVE (records at or below an acknowledged index are freed), while every acknowledgement the returning replica can still send reports a position BELOW the window, so it freed nothing. A full window is what makes a peer "paused", and the paused peer was skipped on every send path: the leader could not send it the one entry that would let it advance, so it could never acknowledge anything that would free a slot. A heartbeat response now releases one slot, letting exactly one message through — acknowledged normally if the window was merely busy, rejected if the peer restarted, and the rejection drives the existing backtracking that repairs the peer's position. This restores etcd/raft's `MsgHeartbeatResp` behaviour: the primitive it needs (`inflights.freeFirstOne`) existed here and had been deleted as unused, which is what left the stall unrecoverable. The condition deliberately does not try to detect the restart from leader state alone — a peer that is merely busy is indistinguishable from a wedged one by match/next/window shape, so any such test fires on healthy pipelined replication and resets it continuously
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

- **A pending request made with a deadline-less context now expires.** A proposal, read, snapshot or config change registered under `context.Background()` recorded the zero time, which the sweeper skips by design, so the entry was never reclaimed: its `RequestState` never returned to the pool and `pendingOps` never fell back to zero, which made every later `Drain()` burn the full `ShutdownTimeout`. Such an entry is now clamped at registration, so `RequestState.Result()` returns `ErrTimeout` after 5 minutes for a proposal, read or config change instead of blocking indefinitely — a backstop for a request that can never complete (leader crash, partition, a lost apply), not a request SLA. Snapshots carry their own 1-hour backstop, matching the snapshot transfer ceiling, because serializing a multi-gigabyte state machine legitimately outruns the commit-path bound. A caller-supplied context deadline always wins
- `ShardInfo.Health` reports `recovering` and `snapshotting` instead of `healthy` while those gates are up; `Host.DiagShardGates` exposes them directly. A replica invisible to consensus previously reported healthy
- `BarrierConfig.Validate` is exported so a runtime-built config can be rejected instead of panicking

### Security

- **A delegated (hardware-backed) encryptor can now bind positional context, and a deployment can require it.** When a sealing strategy supplies its own encryptor the barrier hands off every operation and returns early, so that path had no positional binding — and, because it bypasses the barrier's own encryption entirely, no epoch AAD, no nonce-reuse tracking and no NIST SP 800-38D invocation counting either. The seam carried no AAD to bind with. A delegated encryptor may now implement the optional `seal.AADSymmetricEncrypter` to declare it authenticates caller-supplied AAD end to end, in which case context binding works exactly as it does in software mode (both paths build the AAD through the same helper, so they cannot drift). An encryptor that does not implement it still refuses a non-empty context rather than silently dropping it. Note this is a claim about end-to-end authentication: a backend that accepts an AAD parameter and discards it must not implement the interface, since advertising a binding that does not exist is worse than not having one.

  `BarrierConfig.RequireContextAAD` lets a deployment demand positional binding; it is checked when the barrier is initialized or unsealed, so a configuration that cannot provide it fails at startup rather than at the first write. It is refused for `StrategyNone`, where authenticated positional binding cannot exist because nothing is encrypted. Default is false, preserving existing behavior.

  `IsHardwareMode` previously conflated two unrelated situations, because `StrategyNone` also installs an (identity) encryptor: "nothing is encrypted, so there is nothing to bind" and "real encryption that cannot bind". Its meaning is unchanged, but `ContextBinding()`, `SupportsContextAAD()` and `IsPlaintextMode()` now distinguish them, and the snapshot and WAL layers decide suppression on the precise capability instead of the blunt flag. The delegated-mode bypass of the nonce tracker and invocation counter is now documented and logged at setup rather than left implicit

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
