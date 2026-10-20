# Compatibility and upgrades: user guide

> **Status: NORMATIVE** · Scope: go-quicraft, all persisted and wire formats ·
> Last verified: 2026-10-20
> Conflicts with this document are defects. Fix them here first.

How to move a cluster from one version to another, what each release changes,
and how to bootstrap a cluster under mandatory node identities. Why the rules
are what they are is in [architecture.md](architecture.md).

## Before upgrading

- Read the `BREAKING` section of every CHANGELOG entry between your version and
  the target, and the [notes by release](#upgrade-notes-by-release) below.
- If you need a restore point, export a snapshot per shard
  (`Host.ExportSnapshot(ctx, shardID, w)`) while still on the old version.
  `ImportSnapshot` on a fresh disaster-recovery host requires
  `WithImportDataDir`; see
  [Back up and restore](../multi-group-host/user.md#back-up-and-restore).
  Artifacts must carry the trailer, so export with a build that writes one.
- Start the target version against an empty data directory in staging first. A
  format break shows up at open time.
- Rebuild every consuming repository from the same commit: a repository built
  against the old layout writes bytes this build cannot parse while stamping
  the same version.

## Upgrade path

Before v1.0.0 there is none beyond wipe and re-sync. Nothing in the codebase
migrates an old data directory, and legacy formats fail closed.

The wire format is exact-match: two binaries built from different pre-release
commits may be mutually unreadable while both stamp the same version, so nothing
refuses the skew for you. A rolling upgrade is therefore not supported, at any
boundary: a new node and an old node never exchange traffic, so a node that
starts on the new version cannot re-sync from peers still on the old one.

Every upgrade is a whole-cluster cold upgrade:

1. If you need a restore point, export a snapshot per shard on the old version
   (see [Before upgrading](#before-upgrading)).
2. Stop every node.
3. Replace the binary on every node, and rebuild every consuming repository,
   from one commit.
4. If the [notes by release](#upgrade-notes-by-release) say data directories
   carry over, skip to step 6.
5. If the target changes an on-disk format, wipe every node: delete its
   `WALDir` and `NodeHostDir` contents, including the `WALFORMAT` marker
   (segments left without it are refused with `ErrWALFormatMissing`). Keep the
   sealed root key unless the release notes list an encryption break. Then
   restore each shard from an application-level backup, or import the
   artifact exported in step 1 on every replica of the shard as described in
   [Back up and restore](../multi-group-host/user.md#back-up-and-restore).
   Artifacts exported by a build with an older artifact layout are refused;
   in that case the application-level backup is the only restore point.
6. Start every node, and wait for `ShardInfo.Health` to leave `recovering` and
   `LastApplied` to catch up before the cluster takes traffic.

`waldb.Reencrypt` re-serialises every record for a barrier migration; it is not
a format migration tool.

From v1.0.0 the upgrade path is the acknowledged policy in
[architecture.md, From v1.0.0](architecture.md#from-v100); this page does not
restate it.

## Upgrade notes by release

Read the CHANGELOG `BREAKING` sections between your version and the target;
this table summarises them.

| Target | Data directories | What changes for an embedder |
|---|---|---|
| **Unreleased** | carry over; no format changed | go-enclave is v0.9.0-alpha: its constructors return interfaces. `*enclave.Memory` is gone (use `enclave.ByteAccess`), `*enclave.Sealed`, `*enclave.Sealer` and `*enclave.Allocator` are the interfaces `enclave.Sealed`, `enclave.Sealer` and `enclave.Allocator`, and `Freeze` is the `enclave.Freezer` capability reached with `enclave.As`. Every `pkg/seal` and `pkg/crypto/shamir` signature that named the old types names the new ones. |
| **0.21.0-rc.2** | carry over from 0.21.0-rc.1; no format changed | Run-time break: while a shard's barrier is sealed, `Propose`, `ProposeWithSession`, the session calls, the membership changes, `ReadIndex`/`SyncRead`, `StaleRead`, `FollowerRead`, `StaleReadBuf`, `RequestSnapshot` and `ExecuteOnLeader` return `ErrBarrierSealed` up front, so a health, seal-status or unseal path that reads a shard under the sealed barrier must use `Host.IsSealed()`, `Host.HealthCheck()` and the unseal material instead. Added `Config.MaxClockDrift`. |
| **0.21.0-rc.1** | carry over from 0.20.0; no format changed | Run-time break: `ShardInfo.Replicas` on the leader now lists observers and witnesses as well as voters, each with a `ReplicaProgress.Role` (`ReplicaRoleVoter`, `ReplicaRoleObserver`, `ReplicaRoleWitness`). Code doing quorum or health arithmetic over it must filter on `Role`. `Host.ExecuteOnLeader` adds message types `LeaderExec` (28) and `LeaderExecResp` (29), which a 0.20.0 node does not know: replace every node before calling it. |
| **0.20.0** | carry over from 0.19.0; no format changed | go-enclave is v0.8.0-alpha: `MemoryEnclave.Open`, `OpenString` and `NewMemoryEnclaveFromString` are gone (read inside `WithBytes`; construct with `NewMemoryEnclave`), `EqualTo` returns `(bool, error)` and `SealedEnclave.Destroy` returns an error. `shamir.ProtectedShare.Value` is removed: read the value inside `WithValue`. `WithShardBarriers` now combines with `WithoutEncryption`, so `ErrAtRestEncryptionConflict` is only `WithoutEncryption` with `WithBarrier`; `StartShard` refuses a sealed per-shard barrier with `ErrBarrierSealed`. |
| **0.19.0** | carry over from 0.18.0 | no breaking change: `EventListener.OnLeaderReady` and `Host.ShardReadReady` are added |
| **0.18.0** | carry over from 0.17.0; no format changed | `DefaultMaxMessageReceiveMemory` is removed; the default receive budget is derived from the frame size (512 MiB at the defaults). Every failure delivered to a proposal, config change or session operation after it reached Raft now reports `MaybeApplied(err) == true`, wrapped in `*MaybeAppliedError`, so an unsessioned caller must not blindly retry it. Raft's compacted and unavailable errors are `logdb.ErrCompacted` and `logdb.ErrUnavailable`, with `logdb:` text. A second bind of a live listener's UDP address now fails with `EADDRINUSE`. |
| **0.17.0** | carry over from 0.16.0; no format changed | `seal.NewCredentials`, `crypto.NewCredentials`, `seal.NewSoftwareStrategy`, `NewSoftwareStrategyWithParams`, `NewSoftwareStrategyFIPS`, and `(*Barrier).Rotate`, `PurgeEpochsBefore`, `RootKey`, `RootKeyProtected` and `StampSealedStateMAC` take a leading `context.Context`; so do `Rotate` and `PurgeEpochsBefore` on the `quicraft.Barrier` interface, so a custom `Barrier` must add it. `seal.ProtectedCredentials` and `seal.NewProtectedCredentials` are removed: use `Credentials` and `NewCredentials`. go-enclave is v0.7.0-alpha: `NewSealed` and `SealedEnclave.Open`/`OpenWith` take a context, `Open` returns a frozen enclave (copy into your own sized enclave to write), and `MemoryEnclave.Destroy` returns an error (`DestroyErr` is gone). |
| **0.16.0** | carry over from 0.16.0-alpha (identical code) | release candidate; nothing beyond 0.16.0-alpha |
| **0.16.0-alpha** | carry over from 0.15.x; no format changed | No API removed. `ConcurrentStateMachine` and `DiskStateMachine` now see `Lookup` and `GetHash` concurrently with `Update`, as documented: synchronise them and hash whole batches in `GetHash`. `UnmarshalShareSet` returns `*ProtectedMemoryError` when the kernel refuses a lock. A replica's observer role follows committed membership, so a promoted observer becomes a voter without a restart. Added `ErrRemovedMemberIdentity`. |
| **0.15.0-alpha** | **wipe**: hard-state record moved `0x02` → `0x0d` (adds `VoteWindow`); `0x06` retired | `logdb.LogDB` gains `NodeRemoved`. `RemoveData` retires the replica ID for good; a replica that rejoins needs a new ID. Exported artifacts carry a trailer; artifacts from earlier builds are refused, so re-export them. |
| **0.14.1-alpha** | carry over from 0.14.0-alpha | fixes only |
| **0.14.0-alpha** | **wipe**: at-rest snapshot frame context grew 39 → 40 bytes in place, so 0.13.x encrypted snapshots no longer authenticate | `NewHost` requires `WithBarrier`, `WithShardBarriers` or `WithoutEncryption()` (`ErrAtRestEncryptionUnspecified`, `ErrAtRestEncryptionConflict`). A custom `Barrier` must implement `EncryptWithEpochPinnedContext`. |
| **0.13.1-alpha**, **0.13.2-alpha** | carry over from 0.13.0-alpha | test fix; dependency updates |
| **0.13.0-alpha** | **wipe**: WAL chunk header grew 11 → 15 bytes in place | Identities required at admission (see [Node identities](architecture.md#node-identities-are-mandatory)); `TransportConfig.RequireMemberIdentity` removed. Every inbound message bound to a replica the receiver can vouch for. `WithMaxFileSize` above 4 GiB − 1 is `ErrInvalidMaxFileSize`. `logdb.RecoveryResidual`, `logdb.RecoveryResidualReporter` and `waldb.DB.RecoveryResiduals` removed. `ProposeWithSession` serves one proposal per session at a time (`ErrSessionBusy`); a session at series 0 is `ErrInvalidSession`. |
| **0.12.0-alpha** | carry over | no breaking change |
| **0.11.0-alpha** | carry over | `pkg/enclave` deleted: use `go-enclave` (`enclave.MemoryEnclave`, `enclave.SealedEnclave`) directly. `seal.Credentials.Secret` is a `*enclave.SealedEnclave`. `shamir.MaxShareSetBytes` is a `var`, not a `const`. |
| **0.10.0-alpha** | carry over | no breaking change |
| **0.9.0-alpha** | carry over; a stored Shamir init result must be re-created | `ShareSplitter` and `ShareAccumulator` removed in favour of the protected forms; `ShamirInitResult.Shares` is `[]*shamir.ProtectedShare` and its JSON `"shares"` is an array of share objects. `seal.StorageBackend` gains `CompareAndDelete`. |
| **0.8.0-alpha** | carry over | no breaking change |
| **0.7.0-alpha** | carry over | A `cipher.AEAD` returned by `pkg/seal` reports `NonceSize() == 0` and generates its own nonce; this package's own frames are unaffected. |
| **0.6.x** | **wipe** from 0.5.x: WAL payload types renumbered (`0x07` → `0x0b`, `0x0a` → `0x0c`) | `HostConfig.MaxApplyRetries`, `engine.CircuitBreakerError` and `proto.Bootstrap` removed; the revocation checker's enforcement field renamed and inverted. |

The 0.13.0-alpha chunk-header change is the dangerous kind. A 0.12.x directory
opened by 0.13.0-alpha or later is **not refused**: the marker passes, every
chunk fails the new CRC, which reads as a torn first batch, and recovery
truncates the segment to zero and opens with an empty log and no hard state.
Term and vote regress, so a quorum member upgraded in place can vote twice in
one term. Stop the node, wipe its WAL and NodeHost directories, upgrade and
re-sync from peers, one replica at a time.

### Releases before 0.6.0

All are wipe and re-sync; none has a migration.

| Boundary | Nature |
|---|---|
| 0.4.x → 0.5.x | Layouts grew in place: `proto.Entry` header 60 → 68 bytes (`OwnerID`), WAL entry record header 76 → 84, `proto.SessionPolicy` 24 → 32 (`MaxSessionsPerOwner`), `proto.Membership` fixed region 49 → 57, `waldb` minimum membership section 66 → 74. `proto.Snapshot` lost `Type`, `Imported`, `Witness` and gained `Compression`; `snapshot.meta` gained `DataLen` and `DataHash`; the state MAC moved to version 2 (migrates itself). `0.5.x` still used payload types `0x07`/`0x0a`, so it reads 0.4.x records without refusing them and misparses them; do not run 0.5.x against 0.4.x data. |
| 0.3.1 → 0.4.0 | Every format version pinned at 1. The `WALFORMAT` marker went 2 → 1, so a pre-0.4 WAL is refused with `*WALFormatVersionError`. `Membership` grew `RemovedIdentities`; `ConfigChange` stopped being length-discriminated; `Snapshot.Checksum` removed. |
| 0.3.0 → 0.3.1 | CI and test infrastructure only; formats unchanged. |
| 0.2.0 → 0.3.0 | Four on-disk formats changed. Symptoms: `ErrWALFormatMissing`, `ErrUnknownPayloadType`, and `ErrDecryptionFailed` on every read (at-rest AAD binding changed). |

Changes from 0.4.0 that compile but fail at run time on a jump from 0.3.x:

| Symptom | Meaning | Remedy |
|---|---|---|
| `ErrSealedStateUnauthenticated` on first unseal | the sealed root key predates the state MAC | set `BarrierConfig.AllowUnauthenticatedSealedState = true`, start once, re-persist `SealedRootKeyData()`, clear the flag |
| `ErrSessionsUnsupportedOnDisk` | client sessions on an `sm.DiskStateMachine` shard; the session table lives only in the snapshot stream, which an on-disk state machine never recovers from on restart | use no-op sessions (`Host.GetNoOPSession`) and idempotent operations |
| `ErrKDFNotFIPSApproved` on unseal | an Argon2id-wrapped blob under FIPS mode | unseal once with FIPS off, re-seal with `NewSoftwareStrategyFIPS` (PBKDF2-SHA256) |

## Downgrades

Not supported at any boundary. A downgrade is a wipe and a restore from a backup
taken on the older version. An older binary reads a current `snapshot.meta` as
corrupt and fails closed on a payload-type byte it does not know; anything else
it accepts by version and then misreads.

## From v1.0.0

Everything above describes the pre-release policy. The policy from the first
stable release, with the answer to each open question, is in
[architecture.md](architecture.md#from-v100). The owner acknowledged it on
2026-10-20 (#404); until v1.0.0 ships, plan an upgrade as described above.

## Bootstrapping and growing a cluster

Every host with a transport needs a node identity for every member
([why](architecture.md#node-identities-are-mandatory)).

1. **Collect** `Host.NodeIdentity()` on each node, with its replica ID. It is a
   hash of a public key and safe to log and store. It cannot be learned from
   the peer's own connection; the operator or provisioning system supplies it.
2. **Found** with an identical `Config.MemberIdentities` on every founding
   member. The bootstrap entry at index 1 is built from it, and founders that
   disagree produce divergent entries Log Matching never reconciles.
3. **Grow** with the identity:

   ```go
   err := leader.SyncRequestAddNodeWithIdentity(ctx, shardID, replicaID,
       addr, identity, membership.ConfigChangeID)
   ```

   Start the new replica with `join=true` and a `Config.MemberIdentities`
   naming at least the current leader, or every member if leadership may move
   before the join completes. Use `RequestAddObserverWithIdentity` and
   `RequestAddWitnessWithIdentity` for the other roles, and take a fresh
   `ConfigChangeID` from `SyncGetShardMembership` before each call.
4. **Verify** with `SyncGetShardMembership`, and alert on the unauthorized-drop
   counter with reason `identity_mismatch`: a committed identity that does not
   match the peer's keypair is a stale identity or a forgery, never routine.

Give every host its own keypair. A wildcard or shared-SAN certificate across
distinct hosts stops connecting, and two hosts sharing a keypair share an
identity, which membership refuses to commit twice.

## Session policy when membership carries none

When the committed membership carries no session policy, each replica applies
its own `Config`. Set `SessionExpiryEntries`, `MaxTotalSessions`,
`MaxSessionsPerClient` and `MaxSessionsPerOwner` identically on every replica in that case; nothing
enforces it, and disagreement diverges the state machine silently.

## Troubleshooting

| Symptom at start | Cause | Remedy |
|---|---|---|
| `ErrWALFormatMissing` | segments from a build older than the marker, or a marker deleted while segments were kept | wipe the WAL directory and re-sync |
| `ErrWALFormatUnsupportedVersion` | the directory was written by a build with another WAL format | wipe and re-sync |
| `ErrUnknownPayloadType` | an encrypted WAL opened with no barrier, or records from an older layout | open with the barrier; otherwise wipe and re-sync |
| `*SnapshotArtifactError` on import | the artifact has no trailer, or was damaged | export it again with a current build |
| every message from a member dropped as `missing_identity` | the data was written by a build that did not require identities, or by a host without a transport | wipe and re-sync from a cluster founded with `Config.MemberIdentities` |
