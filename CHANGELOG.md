# Changelog

All notable changes to QuicRaft will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- `ErrDuplicateMemberAddress`, `ErrDuplicateMemberIdentity`,
  `ErrSharedPeerCertificate` and the `SharedPeerCertificateError` type alias
  are re-exported from `pkg`, so membership-add and `StartShard` rejections
  match with `errors.Is` / `errors.As`.

### Fixed

- `make test-snapshot` now runs the snapshot tests across `./pkg/...` instead
  of a package that does not exist. Every `test-<pkg>` target fails when its
  selection matches no tests. `coverage-snapshot` is removed.
- `make fuzz-<pkg>` and `make fuzz-all` work again; each fuzzes every target
  in its package one at a time.
- `make ci-netfault` also runs the netfault test in `test/integration/host`
  and fails if a tagged package is left out.
- `make remote-*` follows the Go toolchain version in `go.mod` instead of a
  hardcoded 1.26.5 path.
- Go doc comments that contradicted the code are corrected, including
  NO-OP sessions on on-disk state machines, `EventListener` threading,
  `SendBatchMaxSize`, `HostID`, WAL batch rollback and the `snapshot.meta`
  layout.

## [0.16.0] - 2026-10-01

Release candidate. The code is identical to 0.16.0-alpha; see that section
for every change and the behaviour changes to check when upgrading. Before
v1.0.0, on-disk and wire formats may still change in place, and the upgrade
path for such a change is to wipe the data directory and re-sync from peers.

## [0.16.0-alpha] - 2026-10-01

Removes the shard-wide stall one slow read caused on concurrent and on-disk
state machines. A promoted observer becomes a voter in place, with no
restart. Shamir share memory can be charged to a go-enclave budget.
`Barrier.Reset` rolls back an uncompleted first boot.

No format version changed, and no API was removed or renamed. Behaviour
changes to check:

- **Concurrent and on-disk state machines.** A `ConcurrentStateMachine` or
  `DiskStateMachine` now sees `Lookup` and `GetHash` run concurrently with
  `Update`, as both interfaces document. Synchronise them, and make `GetHash`
  hash whole batches.
- **`UnmarshalShareSet` lock refusals.** When the kernel refuses a lock,
  `UnmarshalShareSet` returns `*ProtectedMemoryError`, as documented, instead
  of a `*ValidationError`.
- **Observer role.** A replica's observer role follows the committed
  membership. A replica the membership lists as an observer behaves as one
  instead of latching a role mismatch.
- **Makefile offload variables.** The remote-offload variables are renamed
  `OMEGA_*` → `REMOTE_*`, and `REMOTE_HOST` is required.

### Added
- **A promoted observer becomes a voter in place.** A replica's non-voting
  role follows the committed membership; `Config.IsObserver` is only the
  initial hint used while the configuration does not mention the replica.
  When an observer applies the committed `AddNode` naming it (same replica
  ID, same pinned identity), it votes, campaigns, can be elected and serves
  ReadIndex with no shard restart, as in etcd's learner promotion. Under
  `LeaseRead`, the persisted vote window is raised before any heartbeat ack
  carries the new promise, so lease safety holds across a restart right after
  promotion. A replica the committed membership lists as an observer behaves
  as one even if started without the bit, and steps down if it was leading or
  campaigning. Promotion takes effect only from the committed configuration,
  never from an uncommitted entry. A witness is never promoted, and a witness
  contradiction is still latched. `ShardInfo.IsObserver` reports the
  effective role.
- **`quicraft.ErrRemovedMemberIdentity`**: a membership add refused because
  the identity belongs to a removed member is a public sentinel, matchable
  with `errors.Is`. The public `Membership` view also carries the removed
  identities.
- **`seal.Barrier.Reset`** returns a sealed barrier to the uninitialized state
  so `Initialize` can run again. An embedder can roll back a first boot it
  could not finish (for example a Shamir initialization whose share
  distribution failed) without replacing the barrier instance the WAL and
  host already hold. Every key enclave is destroyed, and the epoch, DEK maps,
  load generation, backoff counters and invocation accounting are cleared.
  `Reset` is refused while the barrier is unsealed (`ErrBarrierUnsealed`),
  uninitialized (`ErrBarrierNotInit`), superseded (`ErrBarrierSuperseded`),
  holding an epoch pinned in flight (new `ErrEpochsInFlight`), or not an
  uncompleted first boot (new `ErrResetNotFirstBoot`: its root key came from
  `SetSealedRootKey`, or its epoch advanced past 1 through `Rotate` or
  `Unseal`'s forced rotation). The last rule keeps an older blob from
  re-deriving an already-used DEK with fresh counters. Persisted state is not
  touched. `Reset` is on `*Barrier` only; the `Barrier` and `WALBarrier`
  interfaces are unchanged.
- **Shamir shares can be charged to a go-enclave locked-memory budget.**
  `shamir.SplitProtectedWith`, `CombineProtectedWith` and
  `NewProtectedShareWith` take an `*enclave.Allocator` (normally
  `enclave.ProcessAllocator()`) and admit every region they lock against it:
  the split's coefficient pool, coefficient vector, payload scratch and each
  share value, and the combine's decode buffer and returned secret.
  Exhausting the budget is a `*ProtectedMemoryError` matching
  `enclave.ErrBudgetExhausted`, never a panic. A split refused partway
  destroys everything that call allocated; shares from earlier calls are
  untouched. A nil allocator is refused with `enclave.ErrNilAllocator`.
  `SplitProtected`, `CombineProtected` and `NewProtectedShare` keep their
  signatures and unbudgeted behaviour.
- **Budgeted share ingestion.** `shamir.UnmarshalShareSetWith(a, data)` and
  `(*Share).ProtectWith(a)` admit every share value they lock against `a`,
  with the same contract: nil allocator refused before the input is read,
  exhaustion is a `*ProtectedMemoryError` matching
  `enclave.ErrBudgetExhausted`, and a partial set is destroyed.
  `UnmarshalShareSetWith` still enforces the 255-share and `MaxShareSetBytes`
  bounds first. `ProtectedShare.UnmarshalJSON` cannot take an allocator and
  stays unbudgeted; use a one-element `UnmarshalShareSetWith`, or
  `NewProtectedShareWith`, for one budgeted share. `UnmarshalShareSet` and
  `Protect` keep their signatures. `UnmarshalShareSet` now reports a refused
  lock as a `*ProtectedMemoryError` naming the element, instead of a
  `*ValidationError` that hid the allocator's error from `errors.Is`.
- **Public-releasability gate.** `test/releasability` scans every published
  file (the git index minus `export-ignore` entries in `.gitattributes`) for
  private-lab values: addresses, hostnames, home paths, account names and
  internal identities. It runs under `make test`. A line can be accepted by an
  inline `lab-string-ok: <reason>` waiver or a reasoned, class-scoped
  allow-list entry; both are validated. The package is test-only. A new
  `.gitattributes` export-ignores agent and editor configuration and private
  CI workflows, and `.dockerignore` mirrors it. The Go module zip and a push
  of the history do not honour `export-ignore`. Policy:
  `docs/release/public-releasability.md`.

### Changed
- **`go-enclave` v0.6.1-alpha → v0.6.2-alpha.** A bug-fix release with no API
  change used here. A failed `Allocator.New`/`NewSized` no longer refunds
  budget for pages it could not release; that is the allocator the `...With`
  Shamir functions charge.
- **The remote-offload targets have no default host.** `make remote-*` and
  `make remote-tools` require `REMOTE_HOST` and print usage without it.
  `OMEGA_USER`, `OMEGA_HOST` and `OMEGA_DIR` are renamed `REMOTE_USER`
  (optional), `REMOTE_HOST` and `REMOTE_WORKDIR`. `remote-tools` seeds a
  forge host key only when `FORGE_SSH_HOST` (and `FORGE_SSH_PORT`, default 22)
  are set. Target names are unchanged. Lab addresses and hostnames are gone
  from shipped examples, test fixtures and benchmark baseline headers.

### Fixed
- **One slow read stalled the whole shard on concurrent and on-disk state
  machines.** `rsm.StateMachine.Apply` took its read guard for writing on
  every tier, so a long `Lookup` in a `ConcurrentStateMachine` or
  `DiskStateMachine` (a range scan, an export) blocked the shard's applies,
  and Go's `RWMutex` then parked every later reader behind the waiting apply.
  On those tiers `Apply` now holds the guard for reading, which still keeps
  `Close` out. A basic `sm.StateMachine` keeps its exclusion. Session-table
  exclusion between `Apply` and snapshot capture/recovery comes from a
  dedicated `applyMu`. With four goroutines running 20µs reads, `Apply` on
  those tiers drops from about 55µs to about 100ns.

## [0.15.0-alpha] - 2026-09-29

Lease reads now rest only on vote promises that are kept in real time and
survive restarts, snapshot artifacts verify their integrity end to end, and
`RemoveData` retires a replica ID permanently.
**The WAL format changes in place (hard-state record `0x02` → `0x0d`,
remove-node record `0x06` retired): wipe existing WAL directories and re-sync
from peers.** `logdb.LogDB` gains `NodeRemoved`. A replica that rejoins after
`RemoveData` needs a new replica ID.

### Changed
- **`go-enclave` v0.5.0-alpha → v0.6.1-alpha.** The releases in between are
  additive (a process-wide locked-memory allocator and its budget) and change
  no API this module uses.

### Fixed
- **An exported snapshot artifact could be imported truncated or damaged
  (export artifact format change).** `ExportSnapshot` now ends the artifact
  with a trailer binding the payload length and the xxhash64 of header and
  payload, written only after every check passes. `ImportSnapshot` verifies it
  while streaming and refuses a mismatch, truncation or missing trailer
  (`*SnapshotArtifactError`, matching `ErrSnapshotCorrupt` on a mismatch)
  before anything is published or seen by raft. No version constant changed.
  **Artifacts exported by earlier builds have no trailer and are refused;
  export them again.**
- **A transient read error while hashing a new snapshot made the shard
  unrecoverable.** The save recorded a zero hash and compacted the log, and
  recovery then refused the snapshot. A hash failure now fails the save before
  anything is published, recorded or compacted. Received and imported
  snapshots fail the same way, and encrypted exports now carry a verifiable
  hash.
- **A missing or damaged `snapshot.meta` disabled the integrity check.** A
  recorded snapshot whose metadata is missing, unreadable, malformed or lacks
  integrity fields now fails closed (`ErrSnapshotCorrupt`). Pre-v1 layouts are
  wiped, not migrated.
- **A corrupted plaintext snapshot could be sent to a follower or exported.**
  The transport send and `ExportSnapshot` now verify the recorded hash while
  reading and fail before the last bytes leave; a send withholds its final
  chunk, so the follower installs nothing. On an `ExportSnapshot` error the
  caller must discard what its writer received.
- **Encrypted snapshot recovery and import could leave plaintext on disk after
  a crash.** Recovery now decrypts into an unnamed file and an encrypted
  import streams straight through encryption.
- **A decrypt scratch whose unlink failed was still filled with plaintext.**
  A failed unlink now fails the recovery or send before decrypting
  (`ErrScratchNotUnlinked`) and is retried later. A name that cannot be
  removed is counted on `StaleDecryptSweepFailures` and reclaimed by the
  stale-scratch sweep.
- **`SnapshotOption.ExportPath` could report success without durable
  metadata.** Both files are now written, fsynced and renamed into place, the
  directory (and any directory the export creates) is fsynced, and a metadata
  failure fails the export. Publishing over an existing export moves the old
  pair aside and restores it on failure, so a crash leaves a complete pair or
  one every verifier refuses.
- **A restarted follower could let a stale `LeaseRead` through.** With
  `LeaseRead` on, a replica withholds its vote, pre-vote and campaign for one
  election timeout (monotonic clock) after it starts. Only `LeaseRead` shards
  pay this: a full-shard restart elects about one election timeout later.
  Witnesses are held down too, and the witness docs no longer call `LeaseRead`
  incompatible with them.
- **A follower configured with a shorter vote window than the leader's lease
  could let a stale `LeaseRead` through (WAL hard-state format change).** Each
  heartbeat ack now states the vote window its sender promises. The sender
  keeps that promise on its monotonic clock (no vote, pre-vote or campaign
  until it passes; only a leadership transfer by the promised leader releases
  it, and a leader that sent `TimeoutNow` takes no lease for the rest of that
  term). The leader counts only acks whose window covers its lease, so a
  differently configured follower backs no lease and reads take the ReadIndex
  round. The window is persisted in the hard state before any ack makes it,
  and a restarted replica honours the recorded window. `Host.StartShard`
  refuses an `ElectionRTT` whose product with `RTTMillisecond` overflows
  `time.Duration`. The hard-state record moved to payload type `0x0d`; `0x02`
  is retired. **WAL directories written by earlier builds are refused with
  `ErrUnknownPayloadType`; wipe them and re-sync from peers.**
- **`RemoveData` let a removed replica ID come back (behaviour change).**
  Restarting the same ID after its hard state was erased could let it vote
  twice in one term. `RemoveData` now retires the replica ID on this host
  (`LogDB.NodeRemoved`) and `StartShard` refuses it with
  `*ReplicaRetiredError` (`ErrReplicaRetired`). `RemoveData` takes the shard's
  import lock, so it cannot race a start. **A replica that rejoins after
  `RemoveData` must use a new replica ID.** `logdb.LogDB` gains `NodeRemoved`.
- **Every removed replica pinned a whole WAL segment forever (WAL format
  change).** Removals are now kept in a small checksummed `RETIRED` registry
  file in the WAL root (16 bytes per replica, fsync-and-rename), and replay
  drops a retired replica's records wherever they are, so its segments are
  reclaimed normally. An interrupted removal is finished on the next open.
  Every LogDB writer, `RemoveEntriesTo` included, refuses a retired replica
  (`logdb.ErrReplicaRetired`); `NodeRemoved` reports a retirement only once the
  records are gone; a malformed registry fails `Open`
  (`waldb.ErrRetiredRegistryMalformed`); `waldb.Reencrypt` carries the
  registry across. Payload type `0x06` is retired. **WAL directories written
  by earlier builds are refused with `ErrUnknownPayloadType`; wipe them and
  re-sync from peers.**
- **A crashed snapshot export's leftovers were never removed.** The next
  successful export into the directory removes leftover backups and temp
  files and fsyncs the directory.
- **A paused leader could serve a stale `LeaseRead`.** The lease is now
  measured on the leader's monotonic clock from when the acknowledged
  heartbeat round was sent (`ElectionRTT - 2` ticks of real time, less 1% for
  clock drift), so a pause can only shorten it. Heartbeats carry the round in
  `LogIndex` and followers echo it; the wire layout is unchanged, but both
  ends must run this version for leases to be granted. The `Config.LeaseRead`
  doc now states the assumptions: identical `ElectionRTT` and
  `RTTMillisecond` on every replica, and clock rates within 1% of each other.
- **An outbound connection was not tied to the replica it was dialed for.**
  Any CA-issued certificate covering the host (a wildcard or shared cert)
  could receive a replica's traffic and snapshots. Every outbound connection
  now requires the peer to present the node identity membership binds to a
  replica at the dialed address, and every message and snapshot is written
  only to a connection whose peer presents the destination replica's bound
  identity. Traffic for a replica whose address another member has taken over
  is dropped. A connection whose peer is no longer bound at the address is
  replaced, and a refused destination is not retried for a second.
- **A snapshot's sender was authorized only after the whole transfer,** so
  any cluster certificate holder could tie up receive slots and snapshot
  memory. The snapshot stream header now carries the sender (`From`; 56
  bytes, changed in place, so both ends upgrade together), and the transport
  authorizes it through the new `MessageHandler.AuthorizeSnapshot` before
  taking any resource. Every chunk must carry the header's sender and target.
  Rejections before authorization are counted under shard 0 with the new
  `unauthorized` reason.
- **A revoked node could vouch for itself over OCSP.** A delegated responder
  must now carry the OCSP-signing EKU and be within its validity period;
  otherwise the response is status-unavailable (fail closed). A CA answering
  OCSP itself is accepted as before.
- **A CRL reload undid runtime revocations.** `AddRevokedSerial` entries now
  survive reloads.
- **A peer's receive budget could split under connection churn.** Budget
  entries are now retired atomically before removal.
- **Unauthorized traffic could flood the log and create metric series.** Drop
  warnings are rate-limited (counters still record every drop), and unverified
  shard IDs are counted under shard 0.
- **`StopShard` could wait on an unrelated shard's state machine,** and hang
  if that shard's `Apply` was the caller. The commit worker now answers unload
  and reload requests while its forward to the apply tier is blocked.
- **Snapshot GC could delete a received snapshot raft had not yet adopted.**
  GC now spares claimed directories.
- **Snapshots offered to a failed or stopping shard were reported as
  unauthorized.** After every authorization gate, they now return a
  `SnapshotReceiveError` wrapping `ErrShardFailed` or `ErrShardNotReady`,
  logged at Debug. A snapshot for a shard that is not loaded still counts as
  unauthorized.

## [0.14.1-alpha] - 2026-09-23

Fixes the residual issues listed as known in 0.14.0-alpha and adds a stable
release target.

### Fixed
- **A snapshot being sent to a follower could be deleted mid-send.** The send
  now opens the artifact before the hand-off and reads only through handles,
  decrypting into an anonymous scratch file in the loaded replica's own
  snapshot directory. The scratch is released on every path, including a
  panic. A surviving scratch name is counted on `StaleDecryptSweepFailures`
  and reclaimed by the stale-scratch sweep, which now covers the replica
  directory at shard start and on every recovery. A send for a shard no longer
  loaded, or reloaded as a different replica, is refused.
- **A crash during snapshot recovery could leave plaintext outside every
  sweep.** Recovery now decrypts into the replica's own snapshot directory,
  and a stopped incarnation's late recovery can no longer sweep the next
  incarnation's scratch.
- **A witness in an encrypted cluster could never catch up by snapshot.** A
  snapshot with no file is now sent as metadata only.
- **Engine shutdown dropped committed work in two places.** A dispatcher
  blocked on a full apply channel now waits for the shard goroutine instead of
  failing committed proposals, and in memory mode the apply hand-off now tries
  the send before the stop.
- **`StartShard` could fail with `ErrImportInProgress` with no import
  running** when a stopped incarnation's late recovery held the import lock.
  That sweep is now fenced by the node's write lease instead.
- **The dragonboat adapter passed `ErrImportInProgress` through untranslated.**
  It now maps to `ErrSystemBusy`.

### Added
- **`make release VERSION=vX.Y.Z`** cuts a stable release. It shares one
  recipe with `make release-alpha` (same guards, signature verification and
  atomic push) and differs only in the version form it accepts. Recipe inputs
  are `override`, so command-line variables cannot change them.

## [0.14.0-alpha] - 2026-09-23

**Three breaking changes:** every `NewHost` must state its at-rest
encryption, the at-rest snapshot frame format changed in place, and a custom
`Barrier` must implement `EncryptWithEpochPinnedContext`.
**Wipe every dev data directory before upgrading**: an encrypted snapshot
written by 0.13.x does not authenticate under the new frame context.

### Changed (BREAKING): at-rest encryption must be stated

- **`NewHost` refuses a host that states no at-rest encryption.** A host
  without `WithBarrier` stored its log and snapshots as plaintext silently.
  Every host now passes `WithBarrier`, `WithShardBarriers` or the new
  `WithoutEncryption()`. `NewHost` returns `ErrAtRestEncryptionUnspecified`
  for none of them and `ErrAtRestEncryptionConflict` for `WithoutEncryption`
  combined with a barrier option. The check runs before anything touches the
  disk.
- **Dragonboat adapter:** `NewNodeHost` keeps Dragonboat's behaviour and
  declares `WithoutEncryption`; `NewNodeHostWithOptions` leaves the choice to
  the caller and wraps the refusal in `NodeHostConstructionError`.
- Embedders need a one-line opt-in when they take this version.

### Changed (BREAKING, on disk): snapshot tail truncation is detected

- **Every at-rest snapshot frame's context gains a terminal flag**
  (`SnapshotFrameAADLen` 39 → 40 bytes; magic `QRSNAP1` unchanged, changed in
  place). `Close` always emits a terminal frame and `engine.OpenSnapshotFrames`
  authenticates the last frame as terminal. A snapshot missing trailing
  frames, or with frames spliced after its terminal frame, now fails
  authentication; one truncated to nothing is refused as
  `ErrSnapshotCorrupt`.
- **One encoder, one decoder.** All paths now use `engine.SealSnapshotFrames`
  and `engine.OpenSnapshotFrames`; the duplicate encoder in `host.go` and
  `pkg/epoch_pinner.go` are removed.
- A source I/O error while reading a snapshot is reported as that error, not
  as `ErrSnapshotCorrupt`.

### Fixed

- **`OnLogCompacted` and `OnLogDBCompacted` fired after every snapshot, even
  when nothing was compacted.** The engine now reports compaction results
  through a new `Callback.OnLogCompacted`, delivered after
  `OnSnapshotCompleted`. `OnLogCompacted` fires only when the log was
  compacted, and `OnLogDBCompacted` only when the LogDB step also succeeded.

### Added

- **`config.EventShardInfo.Index`**: the index a compaction ran through, set
  for the two compaction events and 0 otherwise. The dragonboat adapter's
  `LogCompacted`/`LogDBCompacted` now carry it in `EntryInfo.Index` (it was
  always 0).

### Fixed

- **A stated barrier did not cover a caller-supplied log.** `WithBarrier` plus
  `WithLogDB(waldb.Open(dir))` wrote proposed commands to disk in cleartext.
  The LogDB must now report `barrier_encrypted` or `non_durable`, or the host
  is refused with the new `ErrBarrierPlaintextLogDB`.
- **A DEK purge could strand a stopped shard's snapshot.** The purge floor now
  also counts every unloaded shard's LogDB-recorded snapshot epoch (per
  barrier, without relying on `BarrierShardEnumerator`), and purges nothing
  when the LogDB cannot enumerate its shards. A stopped shard's older,
  superseded snapshots may become undecryptable (accepted).
- **A snapshot receive was made durable before raft decided.** The receive is
  now two-phase: the step worker reports raft's verdict on each injected
  install (`Node.AwaitInstallVerdict`), and only an adopted install is
  recorded, floored and recovered; a refused one discards its unrecorded
  directory, and an abandoned verdict keeps the artifact. Installs go to
  exactly the node awaiting the verdict (`Engine.DeliverLocalTo`), a stopped
  node adopts nothing, and recovery runs on the adopting node. A failed
  shard's receive is refused up front instead of holding a receive slot. A
  recovered step-worker panic now answers each dropped install, failing the
  shard when raft's state is unknown. Also fixed: a snapshot record converted
  from raft's `Update` named the data file instead of the directory.
- **A stopped shard could write LogDB after its unload returned.** The
  commit-side unload is now a fence `UnloadNode` waits on, and a queued update
  from an unloaded incarnation is dropped before `SaveState` with its
  messages, as a crash before fsync would. Pipeline guards now test whether
  `UnloadNode` marked the node rather than whether it stopped, so the shutdown
  drain no longer discards committed, durable work. Snapshot receive and save
  publication (directory rename, LogDB record, log marker, compaction, GC) run
  under a per-node write lease `UnloadNode` waits out. A local save now stages
  into its own `.save-*` directory and publishes under the lease, so user
  `SaveSnapshot` code never blocks `UnloadNode`. (A state machine that ignores
  the stopper still delays `StopShard` at `Close`; the `pkg/sm` contract now
  says so.) Saves and receives serialize on a per-node directory lock. Guards
  that spare the recorded snapshot now fail closed on an unreadable record.
  Recovery pins the directory it is reading. A rebuild is refused when the
  record names a different path or a known `FileSize` differs (a legacy
  `FileSize` of 0 still permits repair), and a successful rebuild refreshes
  the log reader's declared size (`LogReader.RefreshSnapshotFileSize`).
- **`ImportSnapshot` could commit underneath a running shard.** `StartShard`
  now holds the import lock for its whole start and refuses with the new
  transient `ErrImportInProgress` while an import runs; a concurrent duplicate
  start gets the transient `ErrShardNotReady`. The import re-checks under the
  lock that the shard is stopped. An import into a shard with data for more
  than one replica on this host is refused. A rebuild whose recorded format no
  longer matches the shard's encryption or compression is refused. Staged
  directories use mode 0750 like other snapshot directories (they were 0700).
  A save on a stopped node publishes nothing; orphaned stages are swept by
  `StartShard`. Snapshot `EventListener` callbacks now go through the listener
  dispatcher, so a listener calling `StopShard` cannot deadlock. A recovery
  whose node stopped under it is abandoned rather than reported against the
  next incarnation. Known limit: `Engine.Stop` can still fail the proposals of
  a committed item when a shard's apply channel is full as the apply tier
  stops, and in memory mode when the step tier's stop races the apply
  hand-off. With a WAL both are crash-equivalent: the entries are applied on
  restart.
- **A `StopShard` racing `Host.Close` could close a state machine mid-Apply.**
  The apply-side unload is now acknowledged only after the stopping
  dispatcher has joined every per-shard goroutine.
- **BREAKING: atomic seal-and-pin is part of the `Barrier` contract.** A
  custom `Barrier` must implement `EncryptWithEpochPinnedContext`; the
  pin/encrypt/pin fallback, which let `Rotate` plus `PurgeEpochsBefore` drop a
  DEK in use, is removed.
- **Spurious compaction events.** A compaction is reported only when this save
  moved the reader's marker. The new `LogReader.CompactTo` reports whether it
  moved.
- **Accepted limit:** snapshot frames carry nothing unique to one artifact,
  so two artifacts of the same identity can exchange frames given disk write
  access. Documented.
- Fixed a race in `TestWrapSysOpState_Release`.

### Changed: gates and coverage

- **Per-package coverage gate.** `scripts/check-coverage-packages.sh` fails
  the build if any package under `pkg/` is below the threshold. CI runs it at
  `COVERAGE_THRESHOLD` (90); `make coverage-packages` holds 91% locally.
- **Coverage raised on four packages:** `registry` 78.4 → 100%, `logdb`
  89.2 → 97.9%, `engine` 90.6 → 91.4%, `transport` 90.9 → 91.4%.
- **Hot path benchmarked against 0.12.0** (`make bench-compare
  BASELINE=v0.12.0-alpha` on ci-node-2, raft/engine/proto): no significant
  change.
- **Releases verify FIPS at runtime.** `make verify-fips`
  (`GODEBUG=fips140=only`) is now a release gate.

### Documentation

- **At-rest rollback of the WAL is a stated limit.** Someone with write
  access to the data directory can remove trailing records and their sync
  mark, which is indistinguishable from a crash. Write access to a data
  directory is compromise of that replica. SECURITY.md and
  `docs/seal-barrier/architecture.md` say so.
- The docs no longer promise observer self-advertisement after v1.0; the
  limitation and its workaround (a stable advertised `RaftAddress`) remain.

## [0.13.2-alpha] - 2026-09-22

### Changed

- **`quic-go` v0.62.0 → v0.63.0.** Its breaking changes are confined to
  `http3`, which this module does not import. The FIPS path the transport
  relies on is unchanged (`aeadAESGCMTLS13` still dispatches to the FIPS AEAD
  under `fips140.Enabled()`), and `make verify-fips` passes under
  `GODEBUG=fips140=only`. The upstream `Conn.peerParams` race is still
  present, so the 0.13.1 test workaround stays. `contrib/dragonboat` and
  `test/performance/quicraft` carry the bump as an indirect requirement.
- **`porcupine` v1.3.0 → v1.3.1.** Two upstream fixes: the
  `NondeterministicModel` hash closure captures `Hash` once, and the
  visualizer tooltip sets `textContent` instead of `innerHTML`.

### Fixed

- `TestRestart_ProposalKeysDoNotCollideAcrossIncarnations` no longer fails
  when leadership moves during the test. Proposals that are not about the
  restarted follower now go through `test/proposer`'s leader-re-resolving
  loop; the keyed proposals stay pinned to the follower with
  `quicraft.Temporary`.

## [0.13.1-alpha] - 2026-09-22

Supersedes `v0.13.0-alpha`, which was tagged but never published because its
release run failed on the quic-go race below. The code is the `0.13.0-alpha`
section's plus this one fix; nothing in the library changed.

### Fixed

- **The 0-RTT replay-policy test no longer fails the race detector on a
  quic-go race.** `TestSnapshotOver0RTTRejected` opens a stream before the
  handshake completes, and quic-go reads and writes its private
  `Conn.peerParams` field from two goroutines without synchronization. The race
  cannot be fixed from this module. No production path is exposed:
  `dialConnection` uses `Dial`, never `DialEarly`, and `Enable0RTT` is off by
  default. The test now skips under `-race` and runs in a dedicated no-race CI
  job; it is the only test in the repository run without the race detector.

## [0.13.0-alpha] - 2026-09-21

Wipe every dev data directory before upgrading: the WAL chunk framing changed
in place.

### Changed — BREAKING: a member is admitted with its node identity or not at all

- **`TransportConfig.RequireMemberIdentity` is removed, along with the address
  fallback on the receive path.** A member is authorized only by its committed
  node identity. Messages from a sender the configuration holds no identity for
  are dropped (`missing_identity`), and its routing entry cannot be re-pointed.
  The old default-off flag let any holder of a cluster certificate impersonate
  a member of an address-only shard.
- **Identities are required at every admission on a host with a transport**
  (`config.Config.RequireMemberIdentities`, set by the Host):
  - `StartShard(join=false)` refuses a founding map with a member missing from
    `Config.MemberIdentities` (`*InvalidMemberError` naming it).
  - `StartShard(join=true)` refuses an empty join anchor.
  - `RequestAddNode`, `RequestAddObserver` and `RequestAddWitness` (the
    identity-less forms) return `ErrMemberIdentityRequired`, and the leader
    applies the same rule to forwarded config changes
    (`raft.ErrMemberIdentityRequired`).
  - `ImportSnapshot` refuses a configuration with an identity-less active
    member; a `WithImportMembership` rewrite supplies identities through the
    new `WithImportMemberIdentities`.
  - A host started `WithoutTransport()` has no wire boundary and is unaffected.
- **`Config.MemberIdentities` is also the operator anchor.** On a first
  `join=true` start it names the replicas the joiner will accept its bootstrap
  snapshot (and every earlier message) from, each bound to the identity its
  connection must present. Previously any host with a valid cluster
  certificate could bootstrap a joining shard. On any start, entries beyond the
  members map are consulted by the receive gate for senders no held
  configuration names. Such entries are not written into the bootstrap entry.
  One identity bound to two replicas is refused at bootstrap, at import and at
  every add. An established replica restarted with `join=true` recovers its
  configuration and needs no anchor.
- `SyncRequestAddObserverWithIdentity` and `SyncRequestAddWitnessWithIdentity`
  join `SyncRequestAddNodeWithIdentity`. `contrib/dragonboat` gains
  `Config.MemberIdentities`, `NodeHost.NodeIdentity` and the three
  `SyncRequestAdd*WithIdentity` methods, since a `NodeHost` with a transport now
  needs an identity and dragonboat's API cannot express one.
- The `address_mismatch` and `shared_address` drop reasons are retired; the
  label values are not reused.
- Integration suites issue one leaf certificate per host from a per-cluster
  test CA (`test/testpki`).

### Changed — BREAKING: every inbound message is bound to a replica the receiver can vouch for

- The receive gate used to exempt leader-originated messages (`Replicate`,
  `Heartbeat`), vote requests and forwarded client work from sender binding.
  Any cluster-certificate holder could exploit that: a higher-term `Heartbeat`
  made it leader on every follower, a `RequestVote` forced an election, and a
  forwarded `Propose` appended an arbitrary command.
- Every message type is now bound. Three facts the replica holds can vouch for
  a sender, each binding the connection's identity: the committed
  configuration; the pending configuration (an appended but unapplied
  config-changing entry in this replica's own log that names the sender with an
  identity, exposed as `raft.Peer.PendingMemberIdentity`); and the operator
  anchor (`Config.MemberIdentities`).
- Consequence: a replica that never received the config change adding the new
  leader refuses that leader (`unknown_member`) until a leader it knows takes
  over, or until it is restarted with a `Config.MemberIdentities` naming the
  leader. The shard keeps quorum without it.
- Restart and identity rules:
  - The host must present the identity the shard committed for its replica,
    and an operator anchor may not contradict a committed binding or bind a
    committed member's identity to a second replica ID. `StartShard` and
    `ImportSnapshot` check the host's own entry against `Host.NodeIdentity`.
    Replica ID 0 is refused in `MemberIdentities`.
  - The receive gate checks the committed configuration first: a keypair bound
    to another replica is refused (`identity_mismatch`), a committed binding
    for this replica that the host does not present is logged at ERROR, and a
    snapshot binding this replica to an identity the host does not present is
    refused before it is persisted.
  - An anchor entry naming a removed identity is tolerated with a WARN. The
    founders' coverage rule applies only at bootstrap; on restart the members
    map is ignored.
  - The pending configuration is seeded on restart from the whole recovered
    uncommitted tail and from the committed stretch the applied configuration
    does not yet reflect (bounded at 64 MiB), so a follower restarting from an
    older snapshot still admits its current leader.
  - `RemoveNode` entries are tracked in the same pending set in log order. A
    removal after an add cancels it, and a committed removal is enforced (by
    replica ID and by keypair) before the replay reaches it. An uncommitted
    removal revokes nobody.
  - A first-start joiner must name a replica other than itself in its anchor
    or members map.
  - An import refuses an active identity that the artifact's tombstones hold,
    and `WithImportMemberIdentities` refuses an entry for a replica the
    resulting membership does not name (the lowest such replica is reported).
    It applies to the artifact's own membership as well as to a rewrite.
  - A sender admitted by the pending configuration or the anchor may fill a
    missing routing entry, so its acks can be routed back.
- A proposal batch is checked whole against `proto.MaxLogIndex`, and leadership
  is declined at that bound.
- `RequestState.Release` releases the session binding before publishing
  `released`, fixing a data race with a concurrent recycle.
- `Update.Snapshot` through `SaveState` is held to the snapshot bound on both
  LogDBs.
- `SECURITY.md` and the `proto.Membership` / `logdb.Membership` docs no longer
  describe the address fallback.

### Changed — BREAKING: WAL chunk header carries the generation start; `MaxFileSize` is at most 4 GiB − 1

- Every WAL chunk records the file offset where its durability generation
  began (the acknowledged end after the previous fdatasync). Recovery now
  decides exactly: damage at or above the acknowledged end is an interrupted
  write and is truncated; damage below it means acknowledged data was lost,
  and the open fails with `ErrAcknowledgedDataLost` (a
  `*SegmentCorruptionError`, so `Repair` remains available).
- `logdb.RecoveryResidual`, `logdb.RecoveryResidualReporter` and
  `waldb.DB.RecoveryResiduals` are removed.
- `Sync` appends a header-only `RecordSyncMark` chunk after its fdatasync, so
  corruption inside a fully acknowledged final generation fails closed instead
  of being truncated as a torn tail. `CanFit` reserves room for the mark; a
  `Sync` that acknowledges nothing new appends none; `segment.Close`
  fdatasyncs again; recovery fdatasyncs the highest surviving segment before
  writing; damage in a non-final segment is reported as
  `ErrNonFinalSegmentCorruption`.
- The chunk header grows from 11 to 15 bytes in place (`walFormatVersion`
  stays 1). **A `0.12.x` data directory is NOT refused: every chunk fails the
  new CRC, recovery truncates to an empty log with no hard state, and a quorum
  member upgraded in place can vote twice in one term.** Stop the node, wipe
  its WAL and NodeHost directories, upgrade and re-sync from peers, one replica
  at a time; see `docs/compatibility.md` §WALFORMAT.
- `WithMaxFileSize` above 4 GiB − 1 returns `ErrInvalidMaxFileSize`.

### Changed — contrib/dragonboat

- Adds `NodeHost.RequestAddReplicaWithIdentity`,
  `RequestAddNonVotingWithIdentity` and `RequestAddWitnessWithIdentity`, the
  asynchronous forms of the `Sync*WithIdentity` methods; the identity-less
  forms are refused on a NodeHost with a transport. The adapter's fixtures and
  `test/performance/quicraft` issue one leaf per NodeHost from `test/testpki`.

### Changed — BREAKING: a session serves one proposal at a time

- `ProposeWithSession` reserves the session's series number for the returned
  `RequestState`; a second concurrent call returns the new `ErrSessionBusy`
  (`Temporary`). Previously two concurrent proposals could carry the same
  `(ClientID, SeriesID)`, and the second caller received the first's result
  without its command being applied. The reservation is released by
  `ProposalCompleted`, by an error result (before the caller sees it, so a
  retry on the same session works), or by `RequestState.Release`.
- `ProposeWithSession` re-checks the claimed series after the reservation, so
  a claim racing `ProposalCompleted` cannot be answered as a retransmission.
- `Session.ValidForProposal` refuses a session at series 0 (built with
  `NewSession` and never registered, or registered without
  `PrepareForPropose`), so `ProposeWithSession` returns `ErrInvalidSession`.
- `RequestState.Release` must be called exactly once; a second call is a
  use-after-free on a pooled object. The dragonboat adapter's
  `RequestState.Release` and `SysOpState.Release` tolerate repeated calls, and
  its `SyncPropose` reservation covers only the in-flight window.

### Added

- `proto.MaxTerm` and `proto.MaxLogIndex` (`math.MaxUint64 - 1024`), with
  `Snapshot.CheckBounds`, `proto.ErrLogIndexOutOfRange` and
  `proto.ErrTermOutOfRange`. Snapshot index and term were unbounded, and a
  snapshot at `Index = MaxUint64` wrapped the log's first index to 0 durably.
  The bound is enforced at the transport's chunk validation, in both LogDB
  `SaveSnapshot` and `SaveState` implementations, and in
  `raft.handleSnapshot`. A leader refuses proposals once its log reaches the
  bound; a follower refuses a `Replicate` batch that would end above it, or that
  carries an entry with term 0 or above the leader's term. Opening persisted
  state is refused only at a slightly higher ceiling, so state a replica
  produced itself still opens.
- `ErrMemberIdentityRequired`, `ErrSessionBusy`, `waldb.ErrAcknowledgedDataLost`,
  `WithImportMemberIdentities`, `config.Config.RequireMemberIdentities`.
- CI cross-architecture builds cover `arm64` and `386`; 32-bit build and vet
  failures in tests were fixed.

### Fixed

- **A concurrent `Drain` joins the attempt in progress instead of returning
  nil.** A second caller (for example `Close`) used to return success at once
  and tear the engine down under the first drain, aborting accepted work while
  both reported a clean shutdown. `Drain` now runs once through a `sync.Once`,
  as `Close` does.
- **`Node.CommittedIndex` is seeded from the recovered log at construction.**
  It read 0 until the first tick, so a proposal registered right after
  `StartShard` had no committed-index floor against replayed entries.
- README badge, `docs/compatibility.md` and `SECURITY.md` now name the current
  release; stale doc paths and comments were corrected.

## [0.12.0-alpha] - 2026-09-16

### Added

- **`SnapshotChunkError` and `SnapshotSizeMismatchError`.** They replace the
  string errors inside `SnapshotReceiveError{Op: "validate"}`: a chunk set that
  is not one snapshot, and a reassembled size that does not match `FileSize`.
  `RequestUpdateWitnessPolicy`'s refusal of a negative `MaxWitnesses` is now a
  `config.ValidationError`. The wrappers are unchanged, so existing
  `errors.Is`/`errors.As` checks still match; the cause is one level down.

### Fixed

- **A restarted replica no longer acknowledges a proposal with a previous
  incarnation's entry.** Request keys were `(replicaID, per-Host sequence)`,
  so a restarted replica reused keys still in its log, and a replayed old
  entry could complete a new proposal with the old result. A caller could be
  told its write landed while it was still in flight, and a following
  linearizable read could return stale data. Request keys are now random
  non-zero 64-bit values from `math/rand/v2` (as in dragonboat), shared by
  proposals, reads and snapshots, and each pending proposal records the
  committed index at registration as a floor that `OnApplied` and
  `OnCommitted` check. No wire or disk format changes.
- **Benchmarks record the version they measured.** The quicraft binary is
  stamped from the root `VERSION` file; dragonboat and etcd report the module
  version their binary links.
- **The dragonboat read benchmarks seed through the current leader**, retrying
  after a leader change. The first complete four-system capture,
  `profiles/ci-node-2`, is on record.
- **Releases tag the `test/performance` module alongside the root module**, so
  `github.com/jeremyhahn/go-quicraft/test/performance@vX.Y.Z` resolves.
- **The internal transport `Config` gains `DialTimeout`** (default unchanged,
  must be positive; `HandshakeIdleTimeout` follows it), so test fixtures can
  dial on a longer budget than production's 5s.
- **Dependencies:** `golang.org/x/crypto` v0.57.0, `x/sync` v0.23.0, `x/sys`
  v0.48.0. `scripts/deps-check.sh` no longer reports a directory-replaced
  module as outdated.

### Changed

- **`encryptSnapshotFile` is removed from the engine.** It had no production
  caller since snapshot saves seal inline. Snapshot, host, raft, seal,
  compatibility and testing documentation was re-verified against the code, and
  the README's barrier examples compile again. The ten `Get`-prefixed `Host`
  methods are kept as a deliberate exception to Go naming conventions.

## [0.11.0-alpha] - 2026-09-13

### Added

- **`quicraft.ErrTransferInProgress`.** `raft.ErrTransferInProgress`, returned
  for every proposal type and for linearizable reads while a leadership
  transfer is in flight, lived in an internal package, so callers could only
  match its text. It is re-exported as a `Temporary` sentinel; the text is
  unchanged.

- **`SoftwareStrategy.KDFParams()` and `NewDeferredSoftwareStrategyWithParams`.**
  The tunable constructors were all bound and the only deferred one took no
  arguments, so a consumer installing a deferred replacement strategy (for
  example on a password change) reset a tuned KDF work factor to the defaults.
  `KDFParams()` reads the work factor off an existing strategy and the new
  constructor carries it into the replacement:

  ```go
  replacement, err := seal.NewDeferredSoftwareStrategyWithParams(old.KDFVersion(), old.KDFParams())
  ```

  This matters for correctness, not only hardening: a sealed blob records the
  KDF version byte but not the params, so a blob sealed at a non-default factor
  can only be unsealed by a strategy carrying that same factor. The constructor
  takes `(version, params)` together so a caller copying settings cannot pick
  the wrong algorithm. FIPS behaviour matches the bound constructors: a
  `-tags fips` build answers `ErrKDFUnsupportedVersion` for Argon2id, and
  runtime FIPS answers `ErrKDFNotFIPSApproved`. Purely additive.

### Fixed

- **A read against a shard whose state machine has failed returns
  `ErrShardFailed` immediately, on every read path.** After `SM.Apply` returned
  an error, `SyncRead`, `SyncReadBuf` and `FollowerRead` waited for the
  caller's deadline (5m6s by default) and then reported `quicraft: timeout`.
  They now refuse with `ErrShardFailed` as the propose path already did, a read
  already registered when the shard fails completes with the same error, and
  the halt is logged once with the state machine's error. `ErrShardFailed` is
  not `Temporary`: the shard stays failed until reloaded.

- **`SyncRead` / `ReadIndex` addressed to a campaigning replica is refused
  with a not-leader error instead of hanging to the deadline.** A candidate
  had no `ReadIndex` case, so the read was dropped.

- **A declined stale snapshot recovery is no longer reported through
  `EventListener.OnShardFailed`.** The engine already declined a
  `*SnapshotRecoveryStaleError` without failing the shard, but the host still
  reported the healthy shard as failed.

- **The local snapshot save encrypts as it writes.** `saveSnapshot` used to
  write the full state machine to `snapshot.dat` in the clear, fsync it, and
  encrypt it in place afterwards, leaving durable plaintext in between. The
  state machine now streams through the epoch barrier into the file, as the
  InstallSnapshot receive path does, and `MaxSnapshotSize` is measured on the
  plaintext the state machine produced rather than on the ciphertext.

- **`contrib/dragonboat`'s `translateError` preserves the `MaybeApplied`
  marker.** A post-delivery leadership change and a pre-flight refusal both
  arrived as a bare `ErrShardNotReady`, so an adapter caller could not tell
  "retry safely" from "may have applied". The mapped sentinel still matches
  through `errors.Is`.

- **`test/performance`.** The dragonboat baseline tolerates transient Raft
  errors (`IsTempError` only, at most 20% of a sample of at least 100) instead
  of aborting the run, a failed run stores nothing, every container's console
  is kept under the profile, and the quicraft image resolves go-enclave
  through a BuildKit secret that touches no layer, command line or log.

- **The Shamir locked-memory budget is priced in pages.** Consumer-visible:
  `MaxShareSetBytes` grows from the 1 MiB every earlier tag shipped (see the
  BREAKING entry below for its new value), and a wide split of a
  multi-kilobyte secret the old figure admitted is now refused with a
  `*SplitError`.

  The budget priced requested bytes, but go-enclave locks whole pages per
  enclave and a 255-share split makes 255 allocations.
  `SplitProtected(secret[1], 255, 255)` was priced at 1,536 bytes and locked
  1,044,480 (measured via `VmLck`). Locked memory is a process-wide resource,
  so a few such splits could exhaust a typical 8 MiB `RLIMIT_MEMLOCK` and make
  an unrelated allocation fail, including the barrier's root-key enclave
  (`Unseal` then fails with `ErrLockedMemoryUnavailable`).

  Every region is now priced through go-enclave's `LockedSize`.
  `UnmarshalShareSet`, which bounded the raw JSON length, now sums the
  page-rounded share values, which equals the split's per-share term, so
  anything a split admits the ingest path also admits. Its refusal message
  changed from `share set is N bytes of JSON, ...` to `share set needs at least
  N bytes of locked memory, ...`; the `*ValidationError{Field: "shares"}` shape
  is unchanged, but code matching the old text breaks. `SplitProtected`'s
  refusal now names the threshold as well as the total.

  The cap had to move with the pricing: the 255-share split of a 32-byte root
  key is 259 pages, more than the old 1 MiB (256 pages). The new cap admits any
  split whose encoded share fits one page (secrets up to 3,070 bytes at 255
  ways, peaking at 1,835,008 bytes); one byte more needs a second page per
  share (2,879,488 bytes) and is refused. The cap bounds one call, not
  concurrency; a process running several at once should use go-enclave's
  `Allocator`. The test for this property now measures the `VmLck` delta
  instead of counting regions.
- **Four hardening gaps in the seal surface.**

  - **`Barrier.RootKeyProtected` returns a frozen enclave.** It returned a
    sized enclave whose pages stayed writable, so a caller retaining the slice
    past its borrow could silently corrupt its copy of the root key. It now
    matches `UnsealRootKeyProtected`, and a refused freeze destroys the enclave
    instead of returning it. The barrier's own root key was never reachable
    this way. Consumer-visible: a write through a retained slice, including a
    caller zeroing the slice before `Destroy`, now faults; `Destroy` is the only
    release. Freezing also puts the wipe at `Destroy` behind an `mprotect` that
    a seccomp profile or hardened kernel can deny, in which case
    `MemoryEnclave.DestroyErr` reports `ErrReleasedWithoutWipe`. Call
    `DestroyErr` if you need to know.

  - **`NewProtectedCredentials`, `NewEpochBarrier`, `MustNewEpochBarrier`,
    `crypto.NewBarrier` and `crypto.MustNewBarrier` are functions, not
    reassignable variables**, so no package in a consumer's dependency graph
    can replace them from an `init()`. Call sites are unchanged. Assigning to
    the names no longer compiles, and `X == nil` on any of them is now a
    `go vet` `nilfunc` failure, which fails `go test` by default.

  - **`Barrier.Unseal` cannot leave the plaintext root key on the heap.** The
    wipe of the strategy's returned `[]byte` sat inside a `WithBytes` borrow
    that returns `ErrEnclaveDestroyed` without calling its function for a nil,
    destroyed or wiped enclave. That path was reachable in production through
    an `enclave.WipeAll` shutdown sweep racing a late `Unseal`. A `defer`
    registered as soon as `UnsealRootKey` returns, before the error check, now
    guarantees the wipe; the eager wipes stay where real work follows.

  - **`pkg/crypto` re-exports `NewCredentials`**, so the compatibility shim
    can construct the `Credentials` it exposes. The shim still re-exports no
    `Algorithm` constants, error sentinels or strategy constructors.

  Related fixes: `SoftwareStrategy.Close`'s doc now says correctly that an
  operation completes against the passphrase only if it has already acquired
  the borrow (otherwise it returns `ErrStrategyClosed`). Every sized
  allocation in `pkg/crypto/shamir` goes through one allocation path, so each
  locked-memory refusal path is tested and the split budget test compares
  requested bytes against `splitLockedBytes`.

- **`Stop()` announces a graceful shutdown to peers that dialed this node.**
  `stop()` closed only the connections this transport dialed; quic-go's
  `Transport.Close` destroyed accepted connections locally without sending
  `CONNECTION_CLOSE`, so peers kept a dead connection until `MaxIdleTimeout`
  (30s) or a failed write. A leader kept enqueuing to a follower that was
  gone. `stop()` now joins the accept loop and then closes every accepted
  connection with the same `QUICErrShutdown` code and reason the outbound side
  sends, before `t.quicTr.Close()`.

- **Test flakes fixed and five leaked QUIC transports closed in the test
  suite.** `TestCancelledConfigChange_SubmitsNothing`,
  `TestEngine_RequestSnapshot_DuplicateRejected`,
  `TestCoalescedBatchSplitsAcrossFrames` and
  `TestStartShard_OnDiskSM_ImportBootstrap` (via the shared
  `waitDiagProcessed` helper, which now waits for the marker to reach the
  index) no longer race unpublished state. Five test transports that were
  never stopped are now stopped.

### Changed — BREAKING: `shamir.MaxShareSetBytes` is a `var` derived from the host page size

- **`MaxShareSetBytes` is `var MaxShareSetBytes = maxShareSetBytesFor(int(enclave.LockedSize(1)))`
  (512 pages of the allocator's page size) and no longer a `const`.** Code
  using it in a constant context (array length, `const` declaration, `case`
  label) stops compiling; code that reads it is unaffected.

  A byte cap made the headline operation depend on the host: the 255-share
  split of a 32-byte root key (259 pages) was admitted with 4 KiB pages and
  refused with 16 KiB or 64 KiB pages (RHEL and CentOS Stream arm64 use 64
  KiB).

  Every earlier tag, `v0.9.0-alpha` and `v0.10.0-alpha` included, shipped
  `const MaxShareSetBytes = 1 << 20` together with byte-summed pricing that
  charged that split 20,657 bytes for the 1,060,864 it locked, so the cap was
  ineffective. This is the first release whose cap and pricing agree on every
  page size.

### Changed — BREAKING: `pkg/enclave` is deleted and `Credentials.Secret` is protected memory

Consumers implementing or calling seal strategies (go-qrdb, go-xkms and
others) take both the `Credentials` retype and Go 1.27 in this release.

- **`pkg/enclave` is deleted.** Locked plaintext is go-enclave's
  `MemoryEnclave` and material encrypted at rest is go-enclave's
  `SealedEnclave`, consumed directly. `pkg/seal/memprotect.go` and its
  re-exports (`seal.ProtectedBuffer`, `seal.ProtectedKey`,
  `seal.NewProtectedKey`, `seal.SealProtectedBuffer`) are removed, and
  `memguard` is gone from `go.mod` and `go.sum`.

  Migration: `ProtectedKey` → `enclave.SealedEnclave`, `NewProtectedKey(b)` →
  `enclave.NewSealed(b)` (which now returns an error, since locking pages can
  be refused), `Clear()` → `Destroy()`, `WipeBytes` → `enclave.Wipe`,
  `ErrEnclaveNil`/`NilError`/`OpenError` → `enclave.ErrEnclaveDestroyed`.

  **Under `GOEXPERIMENT=boringcrypto`, `enclave.NewSealed` always returns
  `ErrSealFailed`**, because go-enclave seals with
  `cipher.NewGCMWithRandomNonce`, which needs the standard library's AES block
  type. On such a build `NewSoftwareStrategy`, `seal.NewCredentials`,
  `Barrier.Initialize` and `Barrier.Unseal` all fail at construction. The
  error does not mention boringcrypto, so check `GOEXPERIMENT` first. For
  FIPS on Go 1.27 use `GOFIPS140=latest`, which is unaffected.

  `Destroy` erases the ciphertext, which memguard's `Clear` could not, so
  `Barrier.seal()`, `PurgeEpochsBefore` and `SoftwareStrategy.Close` now
  remove keys rather than only revoking access. The one copy that cannot be
  erased is the DEK inside a cached `cipher.AEAD`'s key schedule. The
  process-wide `purgeMu` lock is removed.

- **`Credentials.Secret` is a `*enclave.SealedEnclave`, not a `[]byte`.**
  `Credentials` crosses `SealRootKey`, `UnsealRootKey` and
  `BarrierEncryptor`, so every strategy implementation must update. Build one
  with `seal.NewCredentials([]byte)`, which consumes (wipes) the caller's
  slice. The zero value still means "no credentials supplied".

  **`ProtectedCredentials` and `NewProtectedCredentials` are deprecated
  aliases of `Credentials`/`NewCredentials`**, kept so implementations whose
  signatures name `ProtectedCredentials` still satisfy
  `ProtectedSealingStrategy`. They will be removed at the next breaking
  release.

  Behaviour change: the deferred `SealRootKey`/`UnsealRootKey` paths no longer
  build a temporary inner strategy from `creds.Secret`, so a FIPS-unapproved
  KDF is now refused at `DeriveKey` rather than at construction. Same error,
  same call.

- **Frozen results are built in place.** The unsealed root key, a Shamir
  share value and a reconstructed secret now fill one locked region and
  freeze it with `MemoryEnclave.Freeze`, instead of copying between two. This
  removes a term from `splitLockedBytes`: a 255-of-255 split of a 1757-byte
  secret prices at 1,047,032 and is admitted, and the first refused length at
  those dimensions is 1760. The root-key enclave held by `Initialize` and
  `Unseal` is deliberately not frozen, because `enclave.NewSealed` wipes it.

- **Go 1.27.0 is required**, because go-enclave `v0.5.0-alpha` requires it.
  The `go` directive moves in the root and all five nested modules
  (`contrib/dragonboat`, `test/performance` and its three benchmark modules);
  CI workflows and `golang` base images move to 1.27.0.

- **Documentation.** `docs/memory-enclave/` now describes how this repository
  uses go-enclave. `make test-enclave` and `make coverage-enclave` are removed
  with the package.

### Changed

- **Direct dependencies updated:** `golang.org/x/crypto` v0.55.0 → v0.56.0,
  `github.com/anishathalye/porcupine` v1.1.0 → v1.3.0,
  `prometheus/client_golang` v1.23.2 → v1.24.1, and
  `prometheus/client_model` v0.6.2 → v0.6.3. The `x/crypto` bump closes
  **CVE-2026-56855** and **CVE-2026-78662** (GO-2026-6355 / GO-2026-6354),
  SSH channel deadlock DoS advisories; this module never imports
  `x/crypto/ssh`, but scanners flag the module version. `porcupine` v1.3.0 is
  additive. `test/performance/etcd` stays pinned because `go.etcd.io/raft/v3`
  v3.7.0 does not compile against the benchmark; its `require` line carries a
  `DOCUMENTED PIN` comment.

- **`make deps-check`** runs nightly. `scripts/deps-check.sh` walks every
  `go.mod` with `go list -m -u` and fails when a direct dependency is behind
  unless its `require` line carries a `DOCUMENTED PIN` comment.
  `.github/dependabot.yml` is kept but does not run in the current CI setup.

- **The QUIC transport is inside the approved-only FIPS gate; `quic-go`
  moves v0.59.1 → v0.62.0.** RFC 9001 packet protection uses a
  caller-supplied IV, which older quic-go built through `cipher.NewGCM`, so
  `GODEBUG=fips140=only` panicked building the first AEAD. quic-go v0.60.0
  builds packet-protection AEADs through Go's TLS 1.3 AES-GCM implementation.
  Under `GOFIPS140=latest GODEBUG=fips140=only go test -tags fips` the whole
  `pkg/internal/transport` tree now passes.

  - `FIPS_EXCLUDED` shrinks from `pkg/internal/transport` to
    `pkg/internal/transport/revocation`, so `make verify-fips` covers the QUIC
    transport.
  - `make verify-fips-transport` is **renamed `make verify-fips-exempt`**.
  - `TestQUICTransportIsInsideApprovedOnly` pins which tree is excluded.

  The remaining exemption is SHA-1 in `ocsp.CreateRequest` (mandated by RFC
  6960 for the `CertID`). Revocation `Mode: "crl"` never reaches it, and
  `Mode: "any"` (the default) reaches it only when `OCSPResponderURL` is set.
  `quic-go` >= v0.60.0 with `-tags fips`, `GODEBUG=fips140=only` and
  `Mode: "crl"` is an approved-only deployment with no exemptions. The
  transport, seal-barrier, multi-group-host and testing docs, `README.md` and
  `CONTRIBUTING.md` are updated accordingly.

  Also bumped: `testify` v1.11.1 → v1.12.1 and `x/sync` v0.20.0 → v0.22.0. No
  wire or on-disk format version changes.

- **The barrier holds its root key in locked memory for the whole of
  `Initialize` and `Unseal`.** Both used to carry it in an ordinary heap slice
  (swappable, core-dumpable) across sealing, MAC stamping and DEK derivation.
  `Initialize` now generates the root key directly into a locked enclave.
  `Unseal` copies the strategy's returned `[]byte` into locked memory and
  wipes the heap slice immediately. No public signature changes;
  `SealingStrategy` is untouched and strategies implementing only the
  `[]byte` pair keep working.

### Changed

- **BREAKING: locked memory is provided by `go-enclave`.** `pkg/enclave` no
  longer implements `mlock`, guard pages, canaries or borrow tracking itself;
  this was the first step toward the `pkg/enclave` deletion above.

  **Removed, not renamed:** `enclave.NewMemoryEnclave(size)`,
  `enclave.NewMemoryEnclaveFromBytes(data)`, `MemoryEnclave.Bytes()`,
  `MemoryEnclave.Freeze()`, the `DestroyedError` type, and the
  `seal.NewProtectedBuffer` / `seal.NewProtectedBufferFromBytes` re-exports.
  Call go-enclave's `NewMemoryEnclaveSized` (fill in place) or
  `NewMemoryEnclave` (move bytes in) and handle the error, since the kernel
  can refuse to lock pages. Read a value with `MemoryEnclave.Open() ([]byte,
  error)` or `WithBytes`. `enclave.ErrEnclaveDestroyed` is go-enclave's
  sentinel.

  The barrier still holds no resident locked pages: the root key and epoch
  DEKs stay encrypted at rest, and locked pages exist only inside a borrow on
  setup, rekey or rotation paths. Encrypt and decrypt use a cached
  `cipher.AEAD`. Borrows now take only their own enclave's read lock, which
  removes a possible self-deadlock when a borrow enclosed an allocation.

- **`go` directive raised to 1.26.7** in the root and all five nested
  modules, required by `go-enclave v0.4.0-alpha` (superseded by Go 1.27.0
  above).

### Added

- **`shamir.ProtectedMemoryError`**, the typed error for a refused `mlock`.
  It differs from `ValidationError` in ownership: `NewProtectedShare` leaves
  the caller's value untouched on a `*ValidationError`, but by the time a
  `*ProtectedMemoryError` is returned the value has already been wiped. It
  means the host is out of locked memory, not that the input was wrong. Every
  locked-memory refusal in `pkg/crypto/shamir` reports as this type and
  unwraps to the allocator's error.

### Notes

- `ProtectedKey` used `memguard` until go-enclave gained `SealedEnclave`; in
  this release it is replaced by `enclave.SealedEnclave` and `memguard` is
  removed (see the BREAKING section above).

## [0.10.0-alpha] - 2026-09-01

### Added

- **`CI Gate` rollup job.** A single fail-closed status that covers every CI
  job (anything other than `success`, including `skipped` and `cancelled`,
  fails), so branch protection can require one context. `test/buildgates`
  checks that every job is both in `needs` and covered by a check.
- **`SoftwareStrategy.DeferredPassphrase() bool`** reports whether a strategy
  binds its passphrase at construction or at Seal/Unseal time. The two modes
  treat credentials oppositely: a bound strategy ignores the `Credentials`
  passed to `UnsealRootKey`, while a deferred one validates against them.
  Replacing a deferred strategy with a bound one therefore makes the barrier
  accept any passphrase. Callers that swap strategies can now preserve the
  mode. No behaviour of either mode changes.

### Changed

- **The release fuzz gate budgets by execution count and no longer retries.**
  `make ci-fuzz-mutation` used a duration (`-fuzztime=10s`), which could hit
  the Go fuzz coordinator's shutdown race and report a clean timeout as a
  failure with no reproducer. Budgets are now counts derived from the nightly
  calibration divided by `FUZZ_MUT_DIVISOR`, and the 3-attempt retry is gone.
  `test/buildgates` checks that the release and nightly budget tables agree.
- **CI analysis tools are pinned.** golangci-lint, gosec and govulncheck are
  baked into the self-hosted runner image at pinned versions instead of being
  installed (sometimes `@latest`) at job time. The GitHub mirror installs the
  same pins. Each job that runs a tool asserts its presence and version first.
- **`GOLANGCI_VERSION` `v2.12.2` -> `v2.13.2`**, in the Makefile and both
  devcontainer Dockerfiles. The tree reports 0 issues at this version.
- **`make release-alpha` takes an explicit version**
  (`make release-alpha VERSION=vX.Y.Z-alpha`) instead of auto-incrementing
  PATCH. It creates a signed annotated tag, pushes only the release commit and
  that tag atomically, and aborts on any failing step before the tag exists.
  `SUBMODULE_TAGS` is removed.

### Fixed

- **`rsm.StateMachine.Lookup` no longer returns a typed nil on the
  `sm.NALookup` fast path.** A nil `[]byte` result was boxed into a non-nil
  `interface{}`, so `result == nil` was false for an absent key and absence
  looked like an empty value. Every `sm.NALookup` implementor was affected
  (for example, an on-disk SM reported every never-written or deleted key as
  existing). A nil slice now becomes an untyped nil, so `result == nil` tests
  for absence on both paths. A non-nil empty slice is still returned as is,
  meaning "the key exists with an empty value". The generic path was not
  affected.
- **`release.yml` compares the VERSION file byte for byte.** It used to strip
  whitespace, which accepted malformed VERSION files that did not match the
  tag. On mismatch it prints `od -c` of both.
- **The nightly fuzz workflow uses execution counts and keeps reproducers.**
  Per-target budgets are execution counts (`-fuzztime=<N>x`) calibrated per
  package, so a clean end of budget is no longer reported as
  `context deadline exceeded`; `timeout-minutes` is the only wall-clock bound.
  Crash reproducers are printed into the job log, and artifact upload runs
  only on GitHub.

### Security

- **Every GitHub Action is pinned to a commit SHA** across all workflows,
  including `aquasecurity/trivy-action`, which tracked `@master`. A
  `test/buildgates` gate rejects any ref that is not 40 hex characters. When
  bumping an annotated tag such as `github/codeql-action@v3`, pin the peeled
  (`^{}`) commit SHA, not the tag object.
- **`release.yml` refuses a tag that moved after validation.** `validate`
  exports the commit it approved, and immediately before publishing the
  release job resolves what the tag points at on the remote (peeled) and
  requires a match. A missing, ambiguous or mismatched value fails the
  release rather than publishing a different tree.
- **`golang.org/x/crypto` bumped to `v0.55.0`.** `v0.54.0` carries
  CVE-2026-56854 (CRITICAL) in `x/crypto/ssh`. QuicRaft does not import that
  package, but the vulnerable version was in the build list. Bumped in the
  root module and every nested module (`contrib/dragonboat`,
  `test/performance/quicraft`, `test/performance/dragonboat`).
- **A settable `VERSION=` no longer runs make functions at parse time.** The
  build-metadata variable is now `BUILD_VERSION`; `VERSION` belongs to
  `release-alpha` only and is accepted only from the command line. Release
  inputs are read through `$(value ...)`, and the derived build-metadata
  variables (`BUILD_VERSION`, `GIT_COMMIT`, `BUILD_DATE`) are `override`.
  Before pushing, the release commit and tag must verify
  (`git verify-commit`/`verify-tag`) against the key pinned in the in-repo
  `.release-allowed-signers`.

### Documentation

- **`StorageBackend.CompareAndDelete` nil contract.** A nil `expected` is a
  value, not a wildcard: it matches a stored empty value and nothing else, and
  an absent key is always refused. A conformance test enforces this. Note that
  go-xkms's `CompareAndSwapper` reads nil as "require the key absent", so
  implementers coming from that interface should not carry the assumption over.
- **`SealingStrategy`** now states that honouring `Credentials` is
  implementation-defined, that replacing a registered strategy can change
  whether the barrier authenticates its unsealer, and that a nil error from
  `UnsealRootKey` does not prove the credentials were correct.

### Testing

- `TestHandleSnapshot_ConcurrentSameIndexReceives` was flaky because its
  snapshot chunks carried an empty `Membership`, which the restore path adopts
  and which then de-authorized the other concurrent receivers. The chunks now
  carry the shard's committed configuration (shared fixture
  `snapshotPathMembership`), and the test asserts the persisted membership.
- `TestHandleSnapshot_RestoredMembershipGovernsLaterReceives` pins that a
  snapshot replaces the committed membership and the replacement governs every
  later receive. No production code changed.

## [0.9.0-alpha] - 2026-08-24

### Changed (BREAKING): every share and root-key surface is protected memory

- **`ShareSplitter` and `ShareAccumulator` are removed.**
  `ProtectedShareSplitter` and `ProtectedShareAccumulator` are the only
  custody interfaces. The string forms forced every share back out into an
  unwipeable heap string.
- **`ShamirInitResult.Shares` is `[]*shamir.ProtectedShare`**, and the result
  owns its shares: `Destroy()` wipes and releases every one, and is safe on a
  nil receiver and safe to call twice.
- **The `"shares"` JSON shape changes.** It goes from an array of bare share
  values to an array of share objects, the
  `{"index","threshold","total","value"}` encoding `shamir.Share` and
  `shamir.ProtectedShare` use elsewhere. Init results written by older builds
  do not decode; there is no dual-format reader. Wipe local state and
  re-bootstrap.

### Added: `ProtectedSealingStrategy`, the protected root-key pair

- **`SealRootKeyProtected` and `UnsealRootKeyProtected`**, on a new optional
  interface that embeds `SealingStrategy`. Sealing borrows the root key from an
  `enclave.MemoryEnclave`; unsealing returns an enclave the caller owns and
  must `Destroy`. The `[]byte` signatures forced implementations to keep the
  root key in ordinary memory.
- **Optional and additive.** `SealingStrategy` is unchanged, so existing
  strategies still compile; callers type-assert for the protected pair. There
  is no fallback to the unprotected methods.
- **`SoftwareStrategy` implements both.** On unseal the plaintext root key is
  decrypted directly into an enclave sized to the root key. A sealed blob
  whose plaintext length is not exactly the root-key size is refused before
  anything is locked, so an oversized blob cannot exhaust locked memory before
  its AEAD tag is checked.
- **`NoneStrategy` implements both.** `UnsealRootKeyProtected` returns
  `ErrNoRootKeyAvailable`, since no-encryption mode has no root key.
  `SealRootKeyProtected` still validates the enclave it is given, so a
  destroyed root key is reported at seal time.
- **`ProtectedCredentials`.** The protected methods take the passphrase in an
  `enclave.ProtectedKey` rather than `Credentials`' plain `[]byte`; there is no
  overload taking `Credentials`. `NewProtectedCredentials` moves a secret into
  locked memory and wipes the caller's slice; release it with `Clear`.
- **Deferred protected paths no longer build a temporary inner strategy.** As
  a result, a FIPS-unapproved KDF is refused at `DeriveKey` rather than at
  inner construction, with the same error.
- **`Credentials` and the `Barrier` are unchanged.** The barrier still uses
  the `[]byte` pair because it needs raw bytes for HKDF epoch-DEK derivation
  and the sealed-state HMAC, and wipes them on every path.

### Added: protected-memory custody interfaces

- **`seal.ProtectedShareSplitter` and `seal.ProtectedShareAccumulator`**, the
  protected-memory counterparts of `ShareSplitter` and `ShareAccumulator`. In
  this release the string forms are deprecated but still work (they are
  removed above).
- `AddProtectedShare` takes ownership of a share already in locked memory, so
  a transport can decode into a wipeable buffer and hand it over via
  `NewProtectedShare` (which wipes its source). `ReconstructProtected` returns
  the secret in an `enclave.MemoryEnclave` rather than a `[]byte`.

### Fixed: a process-wide memguard purge could unmap pages mid-borrow

- memguard's failure path purges every live buffer process-wide and unmaps
  its pages. A goroutine inside `WithBytes` on one enclave could have its
  pages unmapped by another goroutine exhausting `RLIMIT_MEMLOCK` on an
  unrelated enclave, which is a SIGSEGV. `pkg/enclave` now has a package-wide
  `purgeMu`: purge-capable operations (construction, `Open`, `Destroy`,
  `Freeze`) take it exclusively, and borrows take it shared. Lock order is
  always `purgeMu` then the enclave's own mutex.
- **Behaviour change: a `WithBytes` callback must not touch any enclave**, not
  just its own. A nested borrow of a second enclave can deadlock against a
  queued purge-capable operation. The previous documentation allowed it.
  `CombineProtected` in `pkg/crypto/shamir` now materializes its decode buffer
  before borrowing, and the type doc, `WithBytes` contract,
  `docs/memory-enclave/api.md` and `docs/memory-enclave/engineering.md` are
  corrected.

### Added: `StorageBackend.CompareAndDelete`

- **BREAKING for implementers of `seal.StorageBackend`.** The interface gains
  `CompareAndDelete(ctx, key, expected) (deleted bool, err error)`, the
  release half of `PutIfAbsent`. A store without the method no longer
  satisfies the interface.
- A guarded create needs a guarded release. With `PutIfAbsent` plus a plain
  `Delete`, a stale releaser can delete a value another holder has since
  legitimately claimed, and re-reading first cannot close that window.
- Contract notes for implementers: an encrypting backend must guard on the
  ciphertext it read, not a fresh encryption of `expected` (random-nonce GCM
  would refuse every release). A refused delete is reported as
  `deleted=false`, never an error, while a failed guard read must return an
  error rather than decide.

## [0.8.0-alpha] - 2026-08-19

### Fixed: use-after-destroy race on protected memory

- **`MemoryEnclave` serializes its accessors against teardown.** `Bytes`,
  `Size` and `WithBytes` register a borrow; `Destroy`, `Freeze` and
  `SealMemoryEnclave` wait for every borrow to end before touching the region.
  A reader sees either a live buffer or a destroyed one. Previously the buffer
  was behind an `atomic.Pointer`, which did not stop `Destroy` from unmapping
  the region between a reader's load and its dereference (a SIGSEGV, since
  memguard unmaps rather than zeroes).

  Mutators take priority over readers: once one is queued, no new borrow is
  admitted until it finishes, so a stream of overlapping readers cannot
  starve `Destroy`. As a consequence, **a callback must not re-enter its own
  enclave**; a nested borrow is a new borrow and deadlocks behind a queued
  mutator.
- **`CombineProtected` reads every share through `WithValue`** instead of
  decoding from an aliasing slice after the borrow ended.
- **`ProtectedShare.MarshalJSON` encodes inside `WithValue`**, without copying
  the share value out. A destroyed share still reports the same
  `ValidationError`.
- **New `MemoryEnclave.WithBytes` and `ProtectedShare.WithValue`**, scoped
  accessors that hold the borrow across a callback. `Bytes` and `Value` return
  a slice aliasing the protected region that `Destroy` may unmap after they
  return; use the scoped forms wherever teardown may be concurrent with the
  read. `Bytes` and `Value` remain correct and cheaper under single-goroutine
  ownership.
- **`Freeze` takes the write lock**, so it cannot make pages read-only while a
  `WithBytes` callback is still writing to them.
- **`ErrEnclaveDestroyed`** (and `DestroyedError`) distinguishes a destroyed
  enclave from a zero-length secret, so a callback is never handed a nil slice
  it could read as an empty value.
- **`Destroy`'s doc comment** claimed it was safe concurrently with
  `Bytes`/`Size`; that is now true. `ProtectedShare.Destroy` is corrected the
  same way.

### Testing

`enclave.TestOnlyOnMutatorParked` and `TestOnlyOnReaderParked` are test-only
hooks (gated by `testing.Testing()`) that fire when a mutator, or a reader
queued behind one, is about to block, so concurrency tests can assert on a
parked mutator without sleeping. Both pass the enclave to the callback so
tests can filter on their own enclave.
`TestMemoryEnclave_TwoQueuedMutatorsBothComplete` and
`TestMemoryEnclave_SealExcludesInFlightCallback` cover multiple queued
mutators and `SealMemoryEnclave` against an in-flight callback.

### Hardening

- **`lockIdle` is panic- and Goexit-safe**, so an abnormal exit between
  locking and installing the unlock defer can no longer leave the enclave
  wedged.
- **The `CONCURRENCY` contract is corrected.** A frozen enclave is live and
  read-only, and a borrow orders a callback only against a `Freeze` it races.
  Writing inside `WithBytes` is sound for the producer's own
  allocate/fill/`Freeze` sequence; concurrent freezing must be ordered by the
  caller.
- **Test-hook installs do not nest across goroutines.** Documented as a
  one-goroutine, non-parallel restriction.

### Documentation

`docs/memory-enclave/architecture.md` and `engineering.md` now describe the
borrow discipline instead of the atomic-pointer design. `api.md` covers the
scoped accessors, the error vocabulary and the full exported surface
(including `MemoryEnclave` and `Freeze`); `user.md` has a troubleshooting
entry; and the custody obligations are updated (now in
`docs/shamir-primitive/engineering.md`).

### Performance

Accessor benchmarks, measured back to back on one machine (Core Ultra 9 285K,
5x3,000,000 iterations, best of five):

| | `Bytes` | `WithBytes` |
|---|---|---|
| unsynchronised (the pre-fix shape) | 2.40 ns/op | 1.85 ns/op |
| borrow counter (this change) | 21.6 ns/op | 22.1 ns/op |

The borrow costs about 20 ns per uncontended accessor call, against a
`Barrier.Decrypt` 64B floor of ~123 ns/op on the same machine: about 16% on the
smallest frame and proportionally less on larger ones.
`BenchmarkMemoryEnclave_BytesParallel` measures the contended case (112-138
ns/op across 24 threads on one enclave). Nothing in the tree shares one enclave
across goroutines on a hot path.

## [0.7.0-alpha] - 2026-08-18

Adds the `ProtectedShare` family, `MemoryEnclave` and `Freeze`, and changes the
AES-GCM data path for FIPS 140-3: `pkg/seal` no longer constructs AES-GCM with a
caller-supplied IV, so consumers running `GODEBUG=fips140=only` no longer fail
on it. Read the BREAKING section first if you obtain a raw `cipher.AEAD` from
this package.

### BREAKING — the AEAD returned to consumers reports `NonceSize() == 0`

`SoftwareAEADProvider.NewAEAD`, and any `cipher.AEAD` this package returns for
an AES-GCM algorithm, now uses `cipher.NewGCMWithRandomNonce`, which generates
the nonce itself:

| | before (`0.6.2-alpha`) | after (`0.7.0-alpha`) |
|---|---|---|
| `aead.NonceSize()` | 12 | **0** |
| `aead.Overhead()` | 16 | **28** |
| `len(aead.Seal(nil, nonce, pt5, nil))` | 21 | **33** |
| `SymmetricAlgorithm.NonceSize()` | 12 | 12 (the wire width, unchanged) |

The old AEAD requires a 12-byte nonce; the new one panics if given any nonce.

The break is silent for existing data. A consumer that sizes a nonce with
`make([]byte, aead.NonceSize())` and stores it beside the ciphertext now writes
a zero-length nonce. New data stays self-consistent, but a record written by
`0.6.2-alpha` has a 12-byte nonce field and a nonce-less ciphertext, and a
length check against `aead.NonceSize()` rejects it. If that record is a wrapped
root key, the barrier does not unseal.

Before upgrading, check whether you obtain a `cipher.AEAD` from this package and
either size a nonce from `aead.NonceSize()` or store the nonce separately from
the ciphertext (grep for `NonceSize()` and for structs pairing a `Nonce` field
with a `Ciphertext` field). If so, add a dual-read migration that accepts both
the separate-nonce and embedded-nonce shapes. Consumers that only call
`Barrier.Encrypt` / `Barrier.Decrypt` are unaffected.

This package's own frames are not affected. `NewGCMWithRandomNonce` emits
`nonce ‖ ciphertext ‖ tag`, the same layout `pkg/seal` already wrote, so the
barrier frame and sealed-root-key blob are unchanged in both directions.

### Added — Shamir share values in protected memory

- **`ProtectedShare`, the protected-memory counterpart of `Share`.**
  `Share.Value` is a Go string, which cannot be zeroed. A `ProtectedShare` holds
  its value in an `enclave.MemoryEnclave` (mlock'd, guard-paged,
  canary-checked, wiped by `Destroy`). New, additive API:

  - `SplitProtected(secret, threshold, total)`: `Split` with protected values.
  - `CombineProtected(shares)`: reconstructs from protected shares and returns
    the secret in a `*enclave.MemoryEnclave` the caller destroys.
  - `NewProtectedShare(index, threshold, total, value)`: copies a share value
    into protected memory and wipes the source.
  - `(*Share).Protect()`: migration bridge for callers holding plain shares.
  - `(*ProtectedShare)` `Value`, `Destroy`, `Validate`, `String`, `LogValue`,
    `MarshalJSON`, `UnmarshalJSON`.
  - `UnmarshalShareSet(data)` and `MaxShareSetBytes`: bounded ingestion for a
    share set arriving from outside the process.

  The JSON encoding is byte-for-byte what `Share` produces, so the types are
  interchangeable on the wire and call sites can migrate one at a time.
  `UnmarshalJSON` destroys any value the target share already held, since
  `json.Unmarshal` reuses slice elements.

  Both paths keep secret-derived bytes out of ordinary heap memory:
  `SplitProtected` writes coefficients and share values directly into locked
  memory, and `CombineProtected` decodes shares in place and interpolates into
  the locked buffer it returns, holding one decoded share at a time. Only share
  x-coordinates (public indices) stay on the heap. The string and protected
  paths share the same share-parsing and parameter-validation rules.

  Share values are validated against the full standard-base64 grammar (length,
  alphabet, padding) before entering protected memory, accepting exactly what
  `base64.StdEncoding.Decode` accepts. Each value is bounded to the encoding of a
  `MaxSecretBytes` payload before any locked memory is allocated.

  Whole sets are also bounded. `SplitProtected` returns a `*SplitError` if the
  locked footprint its arguments imply exceeds `MaxShareSetBytes`.
  `UnmarshalShareSet` checks the element count (255) and document size
  (`MaxShareSetBytes`, 1 MiB) on the undecoded input before allocating, and
  destroys the accepted prefix if a later element is rejected. A full 255-share
  ceremony over a 32-byte key is about 12 KiB of JSON; a caller that needs more
  decodes share by share with `NewProtectedShare`.
  `ProtectedShare.UnmarshalJSON` decodes one share and is not the ingestion path
  for an untrusted set.

  Every `ProtectedShare` must be destroyed, including one produced by
  `json.Unmarshal`. Each share is one `RLIMIT_MEMLOCK` allocation, so destroy
  shares as they are consumed.

### Changed — `pkg/enclave`: `ProtectedBuffer` is now `MemoryEnclave`

- **The locked-memory type is renamed `MemoryEnclave`**, with
  `NewMemoryEnclave`, `NewMemoryEnclaveFromBytes` and `SealMemoryEnclave`.
  Nothing breaks: `ProtectedBuffer` remains a deprecated type alias and the old
  constructors remain as deprecated wrappers, so `seal.ProtectedBuffer` and
  existing consumers keep compiling.

- **`(*MemoryEnclave).Freeze`** makes an enclave's memory read-only, so a
  producer can build output inside locked memory and then freeze it without an
  unprotected intermediate copy.

### Changed - FIPS 140-3 compliance

- **`pkg/seal` no longer supplies its own AES-GCM IVs.** `newAESGCM` uses
  `cipher.NewGCMWithRandomNonce`, so the nonce is generated inside the FIPS
  module boundary as FIPS 140-3 (SP 800-38D §8.2.2, CMVP IG C.H) requires. Go
  refuses the old construction under `GODEBUG=fips140=only`.

  No stored bytes change. The barrier frame
  (`[epoch:8][algID:1][nonce:12][ct+tag]`) and sealed-root-key blob
  (`[KDFSalt:32|Nonce:12|EncryptedRootKey|Tag:16]`) are unchanged in both
  directions; `pkg/seal/legacy_format_test.go` checks this against pre-migration
  bytes.

  `Barrier.Encrypt` no longer draws its own data-path nonce, so a failing random
  source can no longer fail an AES-GCM encryption. The nonce-reuse tracker now
  records the nonce after `Seal` (still fail-closed: the ciphertext is never
  returned on failure).

  The AEAD handed to consumers did change its calling convention; see the
  BREAKING section above.

- **New `fips` build tag compiles out every non-approved algorithm.** With
  `-tags fips`, `pkg/seal` contains no ChaCha20-Poly1305 / XChaCha20-Poly1305
  and no Argon2id. The exported API is unchanged:
  `NewSoftwareStrategyWithParams` still compiles and returns
  `ErrKDFUnsupportedVersion` in a FIPS build. Wire and metadata identifiers stay
  defined so a FIPS binary can name data it cannot read. The untagged build
  keeps every algorithm. See
  [docs/seal-barrier/engineering.md](docs/seal-barrier/engineering.md#4-configuration);
  `make build-fips` compiles both variants.

- **New `make verify-fips`.** `build-fips` only proves the tree compiles under
  `GOFIPS140`. `verify-fips` runs `-tags fips` under
  `GOFIPS140=latest GODEBUG=fips140=only` across every package except
  `pkg/internal/transport/...`: QUIC packet protection is arbitrary-IV AES-GCM
  by protocol (RFC 9001 §5.3) and OCSP `CertID` is SHA-1 by protocol (RFC 6960
  §4.1.1). `-tags fips` therefore makes go-quicraft FIPS at rest, not on the
  wire; the transport's FIPS status is undecided.

### Deprecated — the plain-string Shamir share path

- **`Share.Value`, `Split`, `Combine` and `CombineUnverified` are deprecated**,
  each naming its replacement above. Nothing is removed and no signature
  changes; `Share` remains the JSON shape for persisting and distributing
  shares.

- `SplitBytes` and `CombineBytes` are not deprecated. They remain the path for
  callers that manage their own wipeable buffers.

### Fixed - consensus safety

- **A replica that joined via snapshot transfer stopped applying once it caught
  up.** Installing a snapshot moved the log's committed and processed markers to
  the snapshot index immediately, but the state machine adopted that index only
  after the snapshot pool finished `RecoverFromSnapshot`. Entries extracted in
  between started at `snapshotIndex+1` over a stale `lastApplied`, tripping the
  terminal `ErrApplyGap`:

  ```
  ERROR committed entry gap before apply shard=1 replica=4
        last_applied=0 batch_first_index=54
  ```

  The replica kept acknowledging the leader and counting toward quorum but never
  applied another entry. Both extraction sites now hold entries back while
  snapshot adoption is pending; they stay in the log and are offered again once
  recovery completes. No Raft semantics change. Covered by
  `TestIntegration_DB_SnapshotTransferToNewNode`.

## [0.6.2-alpha] - 2026-08-17

This tag was cut without bumping `VERSION` (it stayed at `0.6.1-alpha`) and
without a changelog entry; both are reconciled as of `0.7.0-alpha`. The release
workflow now rejects a tag whose `VERSION` or changelog section does not match.

### Fixed - consensus safety

- **A replica could drop committed entries it had not yet applied.** Committed
  entries are now held until the state machine adopts a snapshot.

### Changed - CI

- `contrib/dragonboat` is report-only rather than a blocking gate.

## [0.6.1-alpha] - 2026-08-17

### Fixed - consensus safety

- **A follower could accept an AppendEntries that opened a hole in its log.**
  `raftLog.term()` had no upper bound: above the in-memory tail it fell through
  to the LogReader, which for one commit cycle after a conflicting append still
  served terms for entries the replica had already discarded. `matchTerm` could
  then pass against an entry the follower no longer held.

  One variant panicked the step worker with `invariant violated: gapped merge
  past in-memory tail`, and the shard never regained a leader because every
  retransmission retriggered it. The silent variant took the hole without
  asserting: the replica acknowledged a `lastIndex` covering entries it did not
  hold, and the leader counted that toward quorum.

  `term()` now returns `ErrUnavailable` above `lastIndex()`, which routes the
  message to `handleAppend`'s shorter-log branch so the leader backtracks.
  Leaders are unaffected. Reachable in production after a leader change followed
  by a Replicate into the just-discarded suffix within one commit cycle; found
  under sustained multi-node benchmark load.

### Fixed

- `pkg/internal/raft`'s `testLogDB` fixture reported `IndexRange() == (0, 0)`
  while serving terms for indices 1..4. The fixture is corrected so the tests
  built on it exercise the error translation they target.

## [0.6.0-alpha] - 2026-08-17

Breaking in both directions against `0.5.x`. On disk, the WAL payload type
bytes were renumbered, so a WAL written by this build is unreadable by
`0.5.1-alpha` and a `0.5.x` WAL is refused here. In the API,
`HostConfig.MaxApplyRetries`, `engine.CircuitBreakerError` and
`proto.Bootstrap` were removed and the revocation checker's enforcement field
was renamed and inverted, so an embedder using any of them no longer compiles.
Wipe data directories and rebuild every node and consuming service from one
commit.

### Changed — comparison benchmarks are stored per machine profile

- **Stored benchmark results belong to a named machine profile, with specs
  captured on that machine.** `test/performance/profiles/<name>/` holds
  `env.json` (captured by `capture-env.sh` on the target), `results/*.json`,
  generated `charts/` and `<system>.md` documents, and a hand-written
  `analysis.md`. Two profiles are tracked: `omega` (single-socket,
  desktop-grade) and `r640` (dual-socket, server-grade). The profile name is a
  required argument. A field the capture could not obtain renders as
  "not captured", never as zero.

- **One command measures any machine:**
  `make perf-remote PERF_HOST=user@host PERF_PROFILE=omega`. It stages the tree,
  captures `env.json`, runs the suite in Docker, fetches results and capture
  into the profile, and regenerates its documents. The phases remain available
  individually, and the remote run stays detached.

- **`analysis.md` is never written by the generator.** Generated documents say
  they are disposable.

- **A system with no results produces no document** rather than one full of
  `N/A` rows, and a stale document for an unmeasured system is deleted.
  Generation still refuses to run when a requested system is missing;
  `PERF_SYSTEMS` narrows the set explicitly. `omega` has no `etcd.md` for this
  reason.

- **The R640 baseline is preserved verbatim** in `profiles/r640/analysis.md`
  with its charts in `profiles/r640/charts/`. Those numbers were never captured
  as JSON, so they were not reconstructed. The harness fairness audit and
  scenario definitions apply to every profile.

- **The profiles tree is no longer ignored or deleted.** Two `.gitignore` rules
  hid stored results, and `make clean` removed `test/performance/profiles`.

- Removed the `perf-compare-{etcd,dragonboat,openraft}` and
  `perf-compare-all-visualize` targets, which wrote charts to the wrong path.
  `perf-docs` and the four `perf-run-<system>` targets are replaced by
  `perf-run`, `perf-generate` and `perf-all`, each taking `PERF_PROFILE` and
  `PERF_SYSTEMS`.

### BREAKING — WAL payload type bytes renumbered

- **A 0.4.x WAL is now refused instead of misparsed.** `payloadTypeEntries`
  moved `0x07` → **`0x0b`** and `payloadTypeSnapshot` `0x0a` → **`0x0c`**. The
  retired values stay unallocated, so a record carrying one fails replay with
  `ErrUnknownPayloadType`.

  0.5.0 grew the entry record header 76 → 84 bytes (`OwnerID`) and the snapshot
  record's session-policy tail 24 → 32 (`MaxSessionsPerOwner`) without new
  payload-type bytes. A 0.4.x entry record then decoded without error into a
  garbage `OwnerID` and a truncated command, and a 0.4.x snapshot record could be
  accepted with a fabricated `MaxSessionsPerOwner` and a corrupted
  `RemovedIdentities` deny-list.

  `walFormatVersion` stays at **1**. A 0.4.x directory opens and fails at the
  first legacy record.

  **Compatibility breaks in both directions.** `0.5.1-alpha` cannot read a WAL
  written by this build either (`ErrUnknownPayloadType`). Wipe the data
  directory and re-sync.

### Changed

- **`sm.StateMachine`'s serialization promise is enforced.**
  `rsm.StateMachine.Apply` now takes the adapter's `readGuard` for writing, so
  it excludes `Lookup`, `NALookup`, `NALookupBuf` and `GetHash`. Previously both
  `Apply` and `Lookup` took `RLock`, so `Update` and `Lookup` could run
  concurrently despite the documented promise.

  `GetHash` now takes the guard for reading and checks `closed`. Without a guard
  it could hash a half-applied batch and report a false divergence, or read a
  closed state machine.

  **Reads now wait behind an in-flight `Apply`.** `SyncRead`, `SyncReadBuf`,
  `FollowerRead`, `QueryLocalNode`, `StaleRead` and `StaleReadBuf` block while
  an `Apply` runs, which on a catching-up follower can carry up to 64 MiB of
  entries, and a read's deadline is not observed while blocked on the lock. The
  guard is on the adapter, so `ConcurrentStateMachine` and `DiskStateMachine`
  are excluded too. Keep implementing those tiers as concurrency-safe; the
  adapter may stop excluding them.

### Removed

- **`config.HostConfig.MaxApplyRetries`, `config.DefaultMaxApplyRetries` and
  `engine.CircuitBreakerError` are removed.** The field was validated and
  plumbed through but never read; no apply-retry policy existed.
  `CircuitBreakerError` was never constructed.

  Embedders that set the field no longer compile; remove the setting. No
  on-disk format, wire format or runtime behaviour changes.

  Shard-failure behaviour is unchanged. Apply is fail-fast: the first
  `SM.Apply()` (or entry-decompression) error fails the shard, `lastApplied` is
  not advanced, the batch's pending proposals fail, and
  `EventListener.OnShardFailed` fires. The shard stays offline until reloaded or
  recovered from a snapshot. `ErrShardFailed`, `*engine.ApplyError`,
  `*engine.SMPanicError` and `OnShardFailed` are unchanged.

- **`proto.Bootstrap` and its codec are removed.** It had no production caller.
  The record LogDB persists is `logdb.Bootstrap`, which is unaffected, and no
  on-disk byte was ever in the `proto.Bootstrap` shape. Shard bootstrap, restart
  and `ImportSnapshot` behaviour are unchanged.

## [0.5.1-alpha] - 2026-08-17

The first publishable 0.5.x: the same library content as 0.5.0-alpha, whose
release pipeline failed its fuzz gate, plus the fixes below.

**This version cannot read a 0.4.x data directory.** `proto.Entry`'s header went
60 → 68 bytes, the WAL entry record 76 → 84, and the sealed-state MAC v1 → v2.
There is no in-place upgrade: wipe the data directory, or export snapshots with
0.4.x and re-import. See 0.5.0-alpha below for the full list and the migration.

`v0.5.0-alpha` remains resolvable but never completed its release pipeline.
Depend on 0.5.1-alpha.

### Fixed — security

- **Certificate revocation now fails closed by default.**
  `revocation.Config.EnforceRevocation` is renamed **`AllowSoftFail`** with the
  inverted sense, matching `config.RevocationConfig.AllowSoftFail`. The zero
  value now enforces: a checker rejects a peer whose status cannot be
  determined unless soft-fail is requested. Previously only `Host` set
  enforcement, and every other construction path got soft-fail.

  Deployments configured through `Host` behave as before. Embedders that build
  `revocation.Config` directly move from soft-fail to fail-closed unless they set
  `AllowSoftFail: true`. The field is in an internal package, so the public API
  is source-compatible.

- **`ModeCRL` with no CRL file reported every certificate as not revoked.** Under
  enforcement it returned `(false, nil)` for every peer, while `"ocsp"`, `"both"`
  and `"any"` failed closed. `IsRevoked` now returns `ErrNoRevocationSources`
  here, and `Config.Validate` rejects the combination at startup. An explicit
  `AddRevokedSerial` verdict still takes precedence.

### Fixed

- **Proposals to a failed shard are refused immediately.** After an `SM.Apply`
  error or state-machine panic, later proposals were accepted and only failed at
  their deadline, so a dead shard looked slow. `Host.proposeEntry` now returns
  `ErrShardFailed` as soon as the shard fails. `ErrShardFailed` is still not
  `Temporary`, since the shard stays failed until reloaded or recovered. The
  first failing proposal still carries the state machine's own error.

- **`Membership.MarshalTo` now enforces the write-side bound on identities**, so
  the encoder no longer produces frames its decoder rejects. The fuzz harness
  that built such a membership is fixed, with the minimised input committed as a
  corpus seed.

- **The release fuzz gate no longer fails on Go's fuzzing-coordinator shutdown
  timeout.** A crash always writes a reproducer and fails immediately without
  retry; a bare `context deadline exceeded` with no reproducer is retried;
  anything else fails as unrecognised. The gate is now `make ci-fuzz-mutation`
  and runs as part of `make ci-full`.

- **19 of the 21 `errfs` fault-injection tests could not inject on a flush**
  because they ran with `WithNoSync(true)`. They now exercise flush faults.

### Added

- **Unit tests for the fault injectors**, in the ordinary build. `errfs`:
  `injectAt` fires on exactly the Nth operation, `OnlyOps` restricts which
  operation fires, `Times` bounds injection, `ShortWrite` disturbs the medium
  before failing, and a misspelled operation name panics. `netfault`:
  directional cuts, `Isolate` cuts both directions, `Partition` cuts only across
  the split, `Heal` clears everything, cuts are node-level, and the
  copy-on-write publication is race-free against the send path.

## [0.5.0-alpha] - 2026-08-17

A full-codebase review and remediation release, closing 6 critical, 31 high
and roughly 60 medium/low findings. Each fix has a test that fails with the
defect reintroduced, and a network-partition suite is added.

On-disk and wire formats changed in place, so this is a minor bump.

### BREAKING: existing data will not load

Formats are pinned at v1 until the production release, so layouts change in
place without version negotiation. This version cannot read a 0.4.x data
directory.

- `proto.Entry` header 60 → 68 bytes (`OwnerID` at +56)
- `proto.SessionPolicy` 24 → 32 bytes (`MaxSessionsPerOwner`)
- `proto.Snapshot`: `Compression` added; `Type`, `Imported` and `Witness`
  removed (written or reserved, never read)
- WAL entry record 76 → 84 bytes; WAL membership tail 66 → 74 bytes
- `EntryOverheadBytes` 76 → 84
- Sealed barrier state MAC v1 → v2 (`sealedStateMACVersion`), so the new
  load-generation counter is covered

**Migration:** wipe the data directory, or export snapshots with 0.4.x and
re-import. Sealed barrier state migrates in place: a pre-v2 blob is recognised
once and restamped on the next persist.

### Fixed: safety

- `InstallSnapshot` installed its sender as leader with no term or leadership
  check, so any replica could replace another's log and suppress its
  elections. It is now gated like `handleHeartbeat`.
- ReadIndex accepted a heartbeat context the leader never issued. One stale or
  forged response could confirm every pending read.
- An update lost between extraction and `SaveState` left the peer claiming
  durability it did not have, and the next update skipped the WAL too, so a
  vote could be answered from memory and never written.
- A membership in a peer's snapshot was unbounded; 65 voters panicked the step
  worker and every shard it owned.
- An over-cap membership already persisted panicked the node on every restart.
  It now fails that one shard and the host stays up.
- `configChangeIndex` could regress and permanently wedge membership changes on
  the shard.
- `handleTimeoutNow` burned a term it could not campaign with, unseating a
  healthy leader.
- `ReconstructMembership` silently skipped an undecodable committed config
  change, so two replicas could rebuild different memberships from one log.

### Fixed: durability and data loss

- A re-delivered `InstallSnapshot` truncated the live snapshot before
  validating anything, and the failure path deleted a directory it had not
  created. Receives now stage and publish by rename.
- A rollback truncation was not fsynced, so a crash could resurrect bytes from
  a batch reported as failed.
- A snapshot's recorded `FileSize` was never verified, so a truncated file
  recovered as if complete.
- Snapshots recorded a membership folded at commit time but indexed at apply
  time.
- A `SaveState` at or below the compaction floor destroyed the live suffix;
  both backends now reject it.
- Read paths returned live aliases of the log served to followers.
- memdb `Compact` could drop entries, hard state and snapshot.

### Fixed: security

- Decode amplified 9.77× on membership-dense frames, while the receive budget
  charged wire bytes, so a 256 MB limit admitted ~2 GB. Amplification is now
  bounded at 8× and charged accordingly.
- Two barriers loaded from one sealed blob derived the same DEK with
  independent nonce counters. A load-generation fence makes the second loader
  fail.
- The nonce tracker disabled itself at its ceiling and returned nil for a
  repeated nonce. It now saturates.
- `Encrypt`/`Decrypt` had no length bound; an oversized frame panicked inside
  `crypto/cipher`.
- One peer could pin the whole snapshot receive budget, since the cap bounded
  slots, not bytes.
- Inbound connections from removed peers could not be reclaimed, and close
  reason phrases leaked the authorization verdict to the peer.
- One embedder could exhaust a shard's session table. Sessions are now capped
  per embedder-supplied `OwnerID`, enforced from replicated state.

### Added

- **Quorum-loss recovery.** `WithImportMembership` reconstitutes a shard whose
  quorum is permanently lost. It is deliberately unsafe, documented as such,
  and comes with a runbook.
- Per-follower replication progress on `ShardInfo.Replicas` (match, state,
  paused, active, inflights).
- A network-partition test suite that isolates a live leader rather than
  killing processes.
- `make no-mutants`, which stops mutation-testing edits from reaching a commit.
- `make remote-<target>`, which runs heavyweight suites on a dedicated host.

### Fixed: tests that could not fail

Several tests could not detect the defect they named, including the
election-safety test, the persist-before-respond tests, session-encoder
determinism and follower-side membership checks. `make security` and the
coverage gate also passed with a scanner or coverage data missing. All are
fixed.

## [0.4.2-alpha] - 2026-08-15

Release-workflow fixes only. Nothing outside `.github/workflows/` differs from
0.4.1-alpha; the library, its formats and its behaviour are unchanged.

### Fixed

- The release gates ran concurrently and overloaded the runner, producing
  timeouts unrelated to the code under test. The integration gate now runs
  alone after the cheap gates, and the fuzz matrix is capped at three
  concurrent jobs. `ci.yml` gets the same change.

## [0.4.1-alpha] - 2026-08-15

Release and CI gate repairs, plus one behaviour fix. The 0.4.0-alpha format
and safety notices still apply in full. There is no format change, so moving
from 0.4.0-alpha needs no code changes. 0.4.0-alpha's release run failed its
gates and was never published.

### Fixed

- The multi-module CI gate never built a nested module: an unexpanded matrix
  expression made it rebuild the repository root and report success. Both
  workflows now loop over modules, collect failures, and fail if discovery
  finds fewer modules than exist.
- `test/performance/dragonboat` could not build on a clean checkout because it
  used a `replace` pointing outside the repository. It now requires the
  published fork `github.com/jeremyhahn/dragonboat/v4 v4.0.1`, and the
  workarounds (`.build-cache`, the Dockerfile rewrite, `perfcompare`'s staging
  hook, `DRAGONBOAT_SRC`) are removed.
- The seal fuzz targets ran Argon2id at 64 MiB per input, dropping to ~0
  exec/s. They now use 1 MiB (the suite already bypasses weak-parameter
  checks), raising a ten-second run from ~500 to ~185,000 executions.

### Fixed (behaviour)

- `Propose`, `ProposeWithSession` and `ReadIndex` now reject a context that is
  already done instead of submitting the request. Previously an abandoned
  request still entered the raft log, and `SyncPropose` with an
  already-canceled context returned success or `ErrCanceled` at random.
  `ErrCanceled` still never proves an entry did not commit when cancellation
  races an in-flight proposal.

### Changed

- A replication-wait timeout in the host and e2e integration suites now prints
  each live node's leader, term, applied index, log span, queue depth and
  health.

## [0.4.0-alpha] - 2026-08-15

**This release breaks both the wire format and the on-disk format, and the
version numbers do not record it.** Every wire and persisted format is pinned
at version 1 until the first production release: `proto.WireVersion`, the
`proto.Membership` section byte, the transport frame header, the `waldb`
snapshot-record membership section and the `WALFORMAT` marker. The layouts
changed underneath those numbers.

- Peers and on-disk data from any earlier build are incompatible. There is no
  rolling upgrade, migration or dual-read path.
- Dev clusters must be wiped and re-synced: stop every node, replace every
  binary, delete every data directory, start fresh.
- Every node and embedder must move to the new layout together.
- Two binaries from different pre-release commits may be mutually unreadable
  while both stamp version 1. The failure is a decode or recovery error, not a
  named version mismatch, so build everything from one commit.

Layout changes: `proto.Membership` carries six maps, including
`RemovedIdentities`; `proto.Snapshot.Checksum` is removed; and encoders now
refuse what their own decoders would reject.

Two breaking changes compile cleanly and fail only at run time: sealed-state
`state_mac` and session-managed proposals on on-disk state machines. Read them
before upgrading.

### BREAKING

- **Every format version is pinned at 1 for the pre-release period, and every
  gate on it is exact equality.** `proto.WireVersion`, the `proto.Membership`
  section version, the `waldb` snapshot-record membership section version and
  the `WALFORMAT` marker (was 2) are all 1; `transport.WireVersion` remains
  0x01. Multi-version decode paths and legacy layout constants are gone.
  - A WAL directory from an earlier build records `WALFORMAT` 2 and is refused
    with `*WALFormatVersionError`. Chunk framing is unchanged (11 bytes,
    `[CRC32:4][Len:2][Type:1][DurSeq:4]`). Wipe the WAL directory and re-sync.
  - `transport.ValidateFrameHeader` checks `!= WireVersion` instead of a range.
    The rejection is still `ErrUnsupportedVersion`.
  - Pre-release layout changes are made in place at version 1, with consumers
    updated together and dev clusters wiped rather than migrated. Version
    numbers start incrementing at the first production release.
  - The version byte is kept; only its value is pinned. A commit skew is
    refused cleanly (`ErrUnsupportedVersion`,
    `*UnsupportedMembershipVersionError`, `ErrUnsupportedMembershipVersion`)
    instead of misread.
  - The transport version stays a supremum over the persisted ones (asserted at
    compile time as `WireVersion >= membershipFormatVersion`), and the two
    constants stay independent so a future transport-only bump does not change
    the version byte a replica expects at log index 1.

- **`proto.Membership` layout: six maps, including `RemovedIdentities`.** At
  version 1 it is
  `[Version:1][ConfigChangeID:8][MaxWitnesses:8][RequireFullReplicaMajority:8]`
  `[SessionExpiryEntries:8][MaxTotalSessions:8][MaxSessionsPerClient:8]` then
  `Addresses`, `Observers`, `Witnesses`, `Removed`, `Identities`,
  `RemovedIdentities`. The fixed region is 49 bytes; an empty section is 73.
  Every map and the session policy are written unconditionally.

  `RemovedIdentities` maps each removed replica ID to the node identity it held
  at removal, so the removal deny-list is checked against the authenticated
  peer identity rather than the claimed `msg.From`. Without it a removed
  replica could evade its tombstone by claiming a different replica ID.
  Entries are never evicted; the key set is a subset of `Removed`, and
  `MarshalTo` returns `ErrTooManyMembershipEntries` past
  `MaxMembershipEntries`.

  `Membership.UnmarshalFrom` is all or nothing: the receiver is assigned only
  on success, so a failed decode cannot leave stale `Identities` or
  `RemovedIdentities`.

  The `waldb` snapshot-record membership section carries the same fields in its
  own layout: an 18-byte fixed region and a 66-byte empty section, which is
  also the read floor replay enforces.

- **`proto.ConfigChange` has one fixed layout, and a truncated config change is
  refused.** Every field, including the trailing `NodeIdentity`, is mandatory;
  `UnmarshalFrom` returns `ErrBufferTooSmall` for a short payload. This is a
  security fix: a truncated payload used to decode with an empty identity,
  which falls back to the weaker address-based authorization. Writers are
  unchanged; an identity-less change (`RemoveNode`, `UpdateWitnessPolicy`, or
  an add without `RequestAddNodeWithIdentity`) is encoded with an explicit
  zero-length prefix and still round-trips.

- **`Membership.MarshalTo` enforces write-side limits**, returning
  `ErrTooManyMembershipEntries` or `ErrAddressTooLong`. Previously
  `Host.StartShard` with an oversized members map committed a bootstrap entry
  at log index 1 that the next start could not decode.

- **`proto.Snapshot.Checksum` is removed.** The layout is now
  `…[FileSize:8][Type:8][Imported:8][OnDiskIndex:8][Witness:8][Epoch:8]`. The
  field was never computed or verified. Snapshot integrity rests on TLS in
  transit and barrier AEAD at rest; a shard with a plaintext barrier
  (`seal.None`) has no at-rest integrity check on snapshot artifacts.
  `proto.MaxChecksumLen` and `proto.ErrChecksumTooLong` are removed.

- **Sealed-state blobs carry a `state_mac` and are refused without one.**
  *(Source-compatible; breaks at run time on first unseal.)* The wrapping AEAD
  covered only `Ciphertext`, so other fields, notably the epoch, could be
  rewritten to resurrect a spent DEK with a reset nonce counter.

  `SealedRootKey.StateMAC` is an HMAC-SHA256, keyed from the root key, over a
  canonical encoding of `(Strategy, Version, DeploymentID, Epoch, Salt,
  KnownEpochs, HKDFVersion, Algorithm, Ciphertext, Metadata, Nonce,
  HardwarePayload)`. `Initialize`, `Rotate` and `PurgeEpochsBefore` stamp it;
  `Unseal` verifies it and fails closed with `ErrSealedStateUnauthenticated`
  when it is missing.

  **An embedder that re-wraps the root key itself (password change, rekey,
  Shamir re-split) must stamp the result**, or its next `Unseal` refuses it:

  - `(*Barrier).StampSealedStateMAC(*SealedRootKey) error` requires the
    barrier unsealed.
  - `StampSealedStateMACWithRootKey(rootKey []byte, deploymentID uint64,
    *SealedRootKey) error` is for a caller holding the plaintext root key with
    no unsealed barrier, such as a replica importing a leader-exported root
    key.

  Stamp last, after every covered field is final.

  **One-start migration:** set `BarrierConfig.AllowUnauthenticatedSealedState
  = true`, start once (`Unseal` logs a warning and restamps), re-persist
  `SealedRootKeyData()`, then clear the flag.

  Limits: `FailedAttempts`/`LastFailedUnixNano` are not covered, and the MAC
  cannot detect replay of an older authentic blob; `BarrierConfig.MinEpoch`
  covers that.

- **Session-managed proposals return `ErrSessionsUnsupportedOnDisk` on on-disk
  state machines.** *(Source-compatible; breaks at run time.)* Affects
  `GetNewSession`, `RegisterSession`, `ProposeWithSession`,
  `SyncProposeWithSession` and `CloseSession` on any shard whose state machine
  is an `sm.DiskStateMachine`. NO-OP sessions still work: use
  `Host.GetNoOPSession` with idempotent operations, as dragonboat requires for
  `IOnDiskStateMachine`. Previously a restarted on-disk replica came up with an
  empty session table and silently diverged from its peers. The check depends
  only on the state machine's type, so every replica decides it the same way.

- **Under FIPS mode, an Argon2id-wrapped blob cannot be unsealed.**
  `DeriveKey` returns `ErrKDFNotFIPSApproved` for `KDFArgon2id`. Migrate by
  unsealing once with FIPS off and re-sealing with `NewSoftwareStrategyFIPS`
  (PBKDF2-SHA256), which unseals in either mode.

- **`seal.BarrierConfig.NonceTracking` is now `*bool`.** As a `bool` the zero
  value disabled nonce tracking on every hand-built config. Nil now defaults to
  true. Read it with `GetNonceTracking()`; disable with `BoolPtr(false)`.

- **`StopShard` and `Host.Close` return errors they previously swallowed**,
  including a `Close` whose drain timed out with accepted proposals dropped.

- **`batch.EncodeBatch` returns `([]byte, error)`** (was `[]byte`).

- **`raft.Peer.SetProcessedIndex` returns `(uint64, error)`.** Internal
  (`pkg/internal/raft`), not public API.

### Added

- `proto.MaxMembershipEntries` (10000) is exported, alongside
  `MaxMessagesPerBatch` and `MaxEntriesPerMessage`. Producers must stay within
  these limits.
- `MarshalTo` on `Membership`, `ConfigChange`, `Entry`, `MessageBatch` and
  `Snapshot` enforces the same bounds as the decoders, so `pkg/proto` cannot
  emit a payload it refuses to parse.
- `SECURITY.md` (vulnerability reporting policy) and `CONTRIBUTING.md`.

### Changed

- `Membership.UnmarshalFrom` is all or nothing: it decodes into a local value
  and assigns the receiver only on success.
- `enclave.InitMemguard` is deleted. It had no callers, and calling it reset
  every signal handler in the process and turned SIGTERM into an immediate exit
  with no graceful shutdown. The library installs no signal handler; an
  embedder that wants signal-triggered wiping installs it in its own `main`
  alongside its graceful shutdown.

### Documentation

Documentation that had drifted from the code is corrected:

- Nine binary-layout specs are regenerated from the encoders, including
  `MessageBatch` (24-byte fixed portion with `SourceEpoch`), `Membership`, the
  WAL snapshot record (62 bytes, with `[Epoch:8][Compression:2]
  [FilePathLen:4]`), `snapshot.meta` (72 bytes), `ConfigChange`,
  `SnapshotChunk` (with `Epoch`) and the `WireVersion` gate.
- The entry type tables now list `EntryMetadata` (3) and
  `EntryInitialMembership` (4), and the config-change table lists
  `UpdateWitnessPolicy` (4).
- Context-AAD composition is corrected: snapshot frames bind a 39-byte
  big-endian context (`"QRSNAP1"(7) || shardID:8 || replicaID:8 || index:8 ||
  seq:8`); WAL records bind 30 bytes (`"QRWAL1"(6) || shardIdx:8 || segID:8 ||
  recordSeq:8`). Both are always applied, not opt-in.
- The WAL chunk header is `recordHeaderSize = 11`
  (`[CRC32:4][Length:2][Type:1][DurSeq:4]`), with the CRC over
  `[Type][DurSeq][Data]` and `maxRecordDataLen = 65535`.
- `sm.Hasher` and `sm.NALookup` have two-value signatures:
  `GetHash() (uint64, error)` and `BytesLookup(key []byte) ([]byte, error)`.
  An implementation of the previously documented three-value form compiled but
  was never invoked.
- `storage/wal.md` lists the missing `logdb` fields, including `Entry.Key`,
  `ClientID`, `SeriesID` and `RespondedTo`, which an alternative `logdb.LogDB`
  backend must persist; plus the missing `Membership` fields,
  `Snapshot.Compression` and `Bootstrap.DataDir`/`DataDirRecorded`.
- Shamir `Combine` performs no integrity check (`IntegrityError` is never
  returned). The caller must authenticate the reconstructed secret out of band.
- `pkg/enclave` is a memguard wrapper, not Apple Secure Enclave or Intel SGX
  integration.
- The documented `WriteVersion` rolling-upgrade procedure, for an API that does
  not exist, is deleted in favour of `operations/compatibility.md`.
- Shutdown and startup ordering are corrected: the engine stops first and the
  transport last; at startup the transport starts last, after all wiring.
- Nonexistent API is removed from the docs: `Host.LeaseRead` (lease read is
  `config.Config.LeaseRead`, transparent to ReadIndex/SyncRead),
  `config.MembershipChangedInfo`, `config.ProposalDroppedInfo`,
  `config.ReadIndexDroppedInfo` (the callbacks take `config.EventShardInfo`),
  `HostConfig.EnableMetrics`, and `RequestDeleteNode`/`RequestAddNonVoting`
  (the methods are `RequestRemoveNode` and `RequestAddObserver`; only the sync
  remove is `SyncRequestDeleteNode`).
- Example signatures are corrected: membership calls take a trailing
  `configChangeIndex`; `barrier.Initialize`/`Unseal` take `(ctx, …, creds)`;
  `RequestSnapshot` takes a `SnapshotOption`. `Host.Drain` is event-driven,
  not polling.
- The `EntryQueue` behind `proposalQ`/`inboxQ`/`readIndexQ` is mutex-guarded,
  not lock-free; it amortizes one acquisition over a batched drain.
- The step worker does not skip a shard while `commitPending` is set: `Tick`
  always runs, and the per-node `raftMu` mutex prevents the `Tick`/`Commit`
  race.
- README: added the `go get` line, corrected the Go and version badges,
  removed the stale "64 buckets" ReadyMap description, and replaced the
  coverage claim with per-package figures (`logdb` 85.2%, `waldb` 88.3%,
  `transport` 89.2%, `engine` 90.4%, aggregate 91.5%).
- Private lab identifiers are removed from the benchmarking docs,
  `test/performance/run-remote.sh` and the `Makefile`. `PERF_HOST` and
  `DRAGONBOAT_SRC` are required settings with no defaults, `REMOTE_DIR`
  defaults to a path relative to the remote home directory, and the release
  remote is `RELEASE_REMOTE` (default `origin`).
- `operations/compatibility.md` gains a `0.3.1-alpha` entry and the transport
  version supremum rule.

### Known Issues

- `pkg/config/config.go:292`: the godoc for `MaxApplyEntrySize` says "Default:
  64 MB". The default is `MaxReplicationPayloadBytes` = 16711672 (~15.94 MiB),
  and `Validate` rejects anything larger.
- `pkg/config/config.go:361`: the godoc for `SessionExpiryEntries` says 0
  disables expiry. `SetDefaults` rewrites 0 to `DefaultSessionExpiryEntries`
  (100000); the disable sentinel is `SessionExpiryDisabled`
  (`math.MaxUint64`).

## [0.3.1-alpha] - 2026-08-14

### Fixed

- WAL `SaveState` permission-failure tests (read-only shard directories) skip
  when running as root, since root bypasses permission bits and the writes the
  tests expect to fail succeed.

### Changed

- CI: SARIF uploads and `trivy-action` run on GitHub only; other runners gate
  with the trivy CLI installed from the release tarball.
- Makefile: `release-alpha` and `deps-latest` targets, and `GOWORK=off` for
  workspace-independent builds.

## [0.3.0-alpha] - 2026-08-12

### Security

- **A remote peer could force a term regression, breaking Election Safety.**
  `Term` is an unbounded uint64 on the wire and `raft.step` adopted a higher
  term verbatim. A peer naming a term at the top of the range drove the replica
  there, and `becomeCandidate`'s `r.term++` then wrapped to term 0, letting the
  replica vote twice in one term. Even without the wrap, a replica at the
  ceiling could never campaign again. `step` now refuses a wire message whose
  term falls in a reserved band below the ceiling (`ErrTermCeilingExceeded`),
  and the increment in `becomeCandidate` is guarded so the wrap is impossible
  by any path. Terms advance by one per election, so no real cluster reaches
  the band. Found by `FuzzRaftStepSequence`.

This release is a security and durability hardening pass over `0.2.0-alpha`. It
is **not compatible with `0.2.0-alpha` data directories**: the WAL record
framing, the snapshot-record membership section, `snapshot.meta` and the
at-rest AEAD binding all changed. The engine fails closed with a named error on
each rather than misparsing, but there is **no migration**: wipe the data
directories, or re-sync each node from its peers.

Source compatibility is better than the format break suggests. The interface
additions under BREAKING affect only embedders that implement those interfaces.

### BREAKING

- **`msg.From` is now bound to a cryptographic node identity recorded in committed membership, not to an address.** `msg.From`, the replica ID Raft counts toward election and commit quorum, travels in-band and is not authenticated by mTLS. The address-based gate left three holes: a promiscuous certificate (wildcard SAN, or one certificate carrying every node's SAN) could assert any member's `From` and have forged `ReplicateResp` and `RequestVoteResp` counted; two replica IDs at one address gave one host two quorum identities; and a member that recovered at a new address had its quorum traffic dropped until a config change committed, which cannot happen during quorum loss.

  Committed membership now records `Identities`, a replica ID to node identity map. The identity is `sha256(leaf.RawSubjectPublicKeyInfo)` rendered `spki:<hex>`: only the holder of the private key can present it, and it survives certificate renewal on the same keypair. Where an identity is committed it is the only check, and the address is routing metadata. It needs no certificate-issuance change, at the cost that the replica-ID-to-identity mapping is supplied out of band.

  Wire and persisted format changes: `proto.Membership` has a v4 layout, the v3 fixed region extended with the replicated session policy (`SessionExpiryEntries`, `MaxTotalSessions`, `MaxSessionsPerClient`; base 25 → 49 bytes) and an `Identities` map after `Removed`. v3 still decodes, with no identities and no session policy, so a log whose index-1 entry an older binary wrote still replays. `proto.ConfigChange` gained a trailing `NodeIdentity` field, discriminated by the buffer's end. The `waldb` snapshot-record membership section is at version 4 and accepts 3-4 on replay; it carries the session policy as a tail after `Identities`.

  API: `Host.NodeIdentity()` reports this node's identity; `Host.RequestAddNodeWithIdentity`, `RequestAddObserverWithIdentity`, `RequestAddWitnessWithIdentity` and `SyncRequestAddNodeWithIdentity` commit one; `Config.MemberIdentities` seeds them at bootstrap; the public `Membership` gained `Identities`. The identity is always an argument and is never captured from the joiner's connection, since capturing it would let a promiscuous-certificate holder register itself as the target replica. `transport.MessageHandler` now takes a `PeerAuth` (identity plus certificate coverage) in place of a `senderAuthorized` closure.

  Existing clusters keep working: a replica with no committed identity falls back to the address check, and `TransportConfig.RequireMemberIdentity` (default false) refuses that fallback. Migration is per-replica with no downtime; see `docs/operations/compatibility.md`. Before enabling the strict mode, confirm through `Host.GetShardMembership` that every member carries an identity. A member added by an earlier build has none until re-added with `RequestAddNodeWithIdentity`, and enabling strict mode first drops its quorum traffic.

  Identities survive a restart. A replica whose log is compacted past the index-1 `EntryInitialMembership` relearns them from the `logdb` snapshot-metadata record, which now carries them end to end (`logdb.Membership.Identities`, `memdb`, the `waldb` v4 section, and the host and engine converters). A record written before identities existed still replays, with an empty identity map. A shared LogDB conformance suite runs `memdb` and `waldb` through one set of assertions and reopens the database to read back through the record encoding.

- **The three session limits are now replicated state, and `Config.SessionExpiryEntries` defaults to 100000 instead of 0.** `SessionExpiryEntries`, `MaxTotalSessions` and `MaxSessionsPerClient` were per-node configuration read by the deterministic apply path. Two replicas with different values expired different sessions at different indexes, so one could answer a session-managed proposal from its response cache while the other reported `ResultSessionExpired`: a state-machine divergence with identical logs and no error anywhere.

  They now live in `proto.Membership` (the v4 layout above), seeded at bootstrap from the founder's `Config` like `MaxWitnesses`, and travel through the log, snapshots, the WAL and LogDB. The apply path reads the committed values; local configuration is used only while a shard has none (a shard bootstrapped by an earlier build, or a `join=true` replica that has not yet replicated the configuration). `MaxTotalSessions`, which `config.Validate` rejects at zero, marks a recorded policy, so an expiry threshold of 0 keeps meaning "expiry disabled cluster-wide".

  A node whose local configuration disagrees with its shard applies the shard's values and logs both at WARN. It does not refuse to start, since the committed value can only change through consensus.

  `SetDefaults` now fills `SessionExpiryEntries` with 100000. With expiry disabled the session table never shrinks and a client that dies without `CloseSession` leaks an entry against `MaxTotalSessions` forever. Since zero no longer means disabled, `config.SessionExpiryDisabled` (`math.MaxUint64`) is added for deployments that want no expiry.

  There is no config change for the session policy yet: it is fixed for the life of the shard, so `MaxTotalSessions` cannot be raised on a running shard.

  API: the public `Membership` gained `SessionExpiryEntries`, `MaxTotalSessions` and `MaxSessionsPerClient`. `rsm.NewStateMachine` takes a `proto.SessionPolicy` and a policy accessor in place of three adjacent `uint64` parameters.

- **Every WAL record now carries a durability generation; the chunk header grew from 7 to 11 bytes and `walFormatVersion` is 2. Existing WAL directories cannot be read by this version.** A batch larger than one block issues one `WriteAt` per block before a single `fdatasync`, so a power loss can persist any subset of those pages. Losing a leading or interior page left CRC garbage with valid records after it, indistinguishable from interior rot, so recovery refused to start and an ordinary power loss could leave a node that would not boot.

  The chunk header is now `[CRC32:4][Len:2][Type:1][DurSeq:4]`, with `DurSeq` inside the checksummed region. `DurSeq` is the number of `Sync` calls the segment has completed. The writer only emits generation `g+1` after `Sync #(g+1)` has returned, so records carrying the segment's highest generation have no completed fsync behind them and discarding them loses nothing acknowledged. Recovery truncates and self-heals when everything above the damage is one generation, refuses with `ErrMidSegmentCorruption` when it spans two, and refuses with `ErrRecordSequenceRegression` when a survivor sits below the clean prefix.

  Cost, measured A/B interleaved on an idle bench node: +1.1 ns/op on `encodeRecord` for a 48-byte payload and +4 bytes per record, against a ~2.5 ms `fdatasync`. Allocations unchanged; write amplification stays 1.0x.

  Segments with no format marker are now fatal (`ErrWALFormatMissing`), since parsing a pre-marker segment would truncate the whole log to zero. A v1 marker returns `*WALFormatVersionError` naming both versions. The `payloadType` values are unchanged. **There is no migration: wipe existing data directories, or re-sync the node from its peers.**

- **An inbound peer whose certificate also covers this node's host is refused at the transport**, before it occupies a connection slot or opens a stream. A certificate that covers this member covers every member. Peers on the same machine (the same host under any spelling, or any loopback address) are exempt, since a certificate is matched on host alone. **A cluster running one wildcard or shared-SAN certificate across distinct hosts will stop connecting.** The check runs once per connection, not per message.

- **Duplicate member addresses are compared canonically.** `"node-b:5001"` and `"NODE-B:5001"` were two members to config-change validation but one host to `x509.VerifyHostname`. Comparison now uses a canonical form (lower-cased host, trailing FQDN dot removed, IP literals normalised, port preserved) shared by the transport and raft through `pkg/internal/netaddr`. It canonicalises spelling only and never resolves DNS, so the result is the same on every replica and replay. `StartShard`, `validateAddNode`, `validateAddWitness` and `validateAddNonVoting` reject spelling-variant duplicates, and a configuration that already holds one fails closed on the receive path.

- **At-rest ciphertext is now bound to its position; existing encrypted snapshots and WALs cannot be read by this version.** The barrier authenticated only the epoch, so an attacker with write access but no key could reorder frames, splice frames between artifacts at the same epoch, or truncate a trailing run. Each snapshot frame and WAL record now carries its identity in the AEAD additional data: `"QRSNAP1" | ShardID | ReplicaID | SnapshotIndex | ChunkSeq` for snapshot frames and `"QRWAL1" | WALShardIdx | SegmentID | RecordSeq` for WAL records.

  The binding is unconditional with no on-disk marker, so there is no weaker fallback for an attacker to select; a reader never retries with a weaker AAD. Both framings are otherwise byte-identical.

  **There is no migration: wipe existing data directories, or re-sync the node from its peers.** `waldb.Reencrypt` produces correctly bound output for a barrier migration. Barriers in hardware mode (`IsHardwareMode`) keep epoch-only binding as a logged capability limitation, decided once at construction.

  API: `Barrier` gained `EncryptWithEpochContext`, `DecryptWithContext` and `IsHardwareMode` (already present on `*seal.Barrier`); `ReencryptSnapshotFile` gained `shardID, replicaID, index` parameters, which a `SnapshotReencryptFunc` already receives. A `Barrier` that does not support caller-supplied AAD is refused when the WAL opens.

- **`snapshot.meta` is now self-describing and grew to 72 bytes.** Each record carries a `QRSNAPMD` magic, a format version and a body length ahead of the existing fields (`Index | Term | ShardID | ReplicaID | CreatedAt | Epoch | Compression`), so a reader can tell an absent field from a corrupt one. Body fields are append-only.

  Records written by earlier versions are still read at their original offsets: the 40-byte layout (receive and import paths, no `Epoch`) and the 48- and 56-byte layouts (save path, with `Epoch`, and with `Epoch` + `Compression`). Nothing rewrites an existing record. **An older binary reads a record written by this version as corrupt**, so treat a downgrade as a wipe-and-restore.

- `TransportConfig.MaxDecompressedSize` is pinned to `MaxFrameSize` and rejected if set to anything else. A lower value gave no bomb protection (a peer can send an uncompressed frame of the same size) and made a receiver reject replication its senders considered valid. `SetDefaults` still maps 0 to the pinned value.

- **`NewPrometheusMetricsCollector` no longer labels most metrics with `shard_id` by default.** Each active shard cost a measured 113 Prometheus series, about 1.13M series at 10,000 shards on one host.

  `config.MetricsConfig.PerShardMetrics` selects the mode and defaults to off: proposal, commit, apply and ReadIndex latency, proposal batch size, snapshot counts and snapshot byte histograms aggregate to one host-level series each, taking a shard to a measured 6 series. Pass `NewPrometheusMetricsCollectorWithConfig(config.MetricsConfig{PerShardMetrics: true})` for the previous behaviour. The mode is fixed when the collector is built, so it is not a `HostConfig` field.

  Alerting signals keep `shard_id` in both modes: `elections_total`, `proposals_dropped_total`, `messages_dropped_total`, `snapshots_rejected_total` and both `quicraft_security_*` counters.

  Aggregated series keep the same metric name and drop only the `shard_id` label, so `sum without(shard_id) (...)` works in both modes; dashboards grouping by `shard_id` collapse to one host-wide series. The `# HELP` text of an aggregated series says so, the `quicraft_metrics_per_shard_enabled` gauge exports the mode (1/0), and `docs/operations/deployment.md` has a "Metrics cardinality" section with the measured budget.

- **`seal.NewBarrier` returns `(*Barrier, error)` instead of panicking on an invalid configuration**, such as an unknown algorithm or a non-FIPS-approved cipher under FIPS mode. `seal.MustNewBarrier` is the panicking form for configs built from constants. `NewEpochBarrier` remains an alias of `NewBarrier` and changes signature with it; `MustNewEpochBarrier` is added. `crypto.NewBarrier` re-exports the new signature and `crypto.MustNewBarrier` is added. Embedders that call or re-export `NewBarrier` must handle the error.

  `BarrierConfig.Validate` is unchanged and still exported; `NewBarrier` applies the same rules and returns the same error identity.

### Fixed

- **A restarted replica could permanently fail to resume replication.** A replica that restarted under a saturated pipeline (256 appends outstanding to its previous incarnation) was never sent another entry, because the in-flight window only drains from above and every acknowledgement it could send reported a position below the window. A heartbeat response now releases one in-flight slot, matching etcd/raft's `MsgHeartbeatResp` behaviour (`inflights.freeFirstOne`). That message is acknowledged normally if the window was merely busy, or rejected if the peer restarted, and the rejection drives the existing backtracking.
- **A connection to a removed replica was never closed.** quic-go's keep-alive kept it from idling out, so a removed but still running replica pinned a connection, its pooled streams and its epoch records for the life of the process. A committed removal now releases the connection, unless another shard still resolves to that address or a snapshot transfer to it is in flight.
- **A leader-change notification fired for campaign churn.** A replica that could not win published every term it campaigned through while the leader was unknown, overwriting the host's cached leader with "none" and possibly completing pending proposals with a spurious `NotLeaderError`.
- **A restarted peer could black-hole every vote response for ~30 seconds.** Survivors kept outbound QUIC connections to the dead incarnation until `MaxIdleTimeout`, which could leave a cluster leaderless with quorum available. Outbound connections are now attributed to a peer incarnation and evicted when they cannot be.
- **A leader could re-dial one follower thousands of times per second.** `sendAppend` bounded a `Replicate` message in bytes but not in entry count, so a follower catching up on small entries drew a message every peer rejects, and the sender re-dialed and retried forever. Entry count is now bounded, oversized batches are split, and a rejected frame evicts only that stream.
- **PreVote messages were withheld behind WAL persistence.** They persist nothing; under sustained write load pre-vote traffic never left the node and elections could not complete.
- **A replica recovering from a snapshot was absent from consensus entirely** (no votes, no heartbeat replies) while its tick kept campaigning. Two concurrent recoveries could dissolve quorum with every node up. The gate now covers state-machine access only.
- **A replica removed from the configuration could still elect itself**, counting its own vote toward a quorum it was not part of, giving two leaders in one term.
- A same-term vote was granted while a live leader was known, and the grant then blackholed higher-term vote requests for a full election timeout.
- `lastTerm` fabricated term 0 when the log was unreadable, so a replica holding a committed entry no one else had would vote for a candidate missing it.
- A recovered commit index ahead of the last recovered entry was trusted, letting a follower acknowledge entries it did not have.
- A follower whose election timer expired could suppress its own election to enter quiesce, then run at 30x the election timeout while leaderless.
- Unload and reload raced through a random `select`, which could leave a shard permanently unable to apply while it still voted and won elections.
- Apply-gap detection was disabled at `lastApplied == 0`, so a dropped first batch silently diverged the state machine.
- A data race on the snapshot recovery markers could abandon a recovering replica's snapshot install, leaving it admitted to the configuration and starving.

- `docs/api/reference.md` and the metrics tables in `docs/operations/deployment.md` are corrected: the metric is `read_index_latency_seconds`, the `shard_id`/`to`/`msg_type` labels belong to the opt-in `*_detailed_total` variants, and `piggyback_commits_total`, `messages_dropped_total`, `snapshots_rejected_total` and both security counters are listed.

### Changed

- **A pending request made with a deadline-less context now expires.** A proposal, read, snapshot or config change registered under `context.Background()` was never reclaimed, so `pendingOps` never returned to zero and every later `Drain()` waited the full `ShutdownTimeout`. Such an entry is now clamped at registration: `RequestState.Result()` returns `ErrTimeout` after 5 minutes for a proposal, read or config change, and after 1 hour for a snapshot. A caller-supplied context deadline always wins.
- `ShardInfo.Health` reports `recovering` and `snapshotting` instead of `healthy` while those gates are up; `Host.DiagShardGates` exposes them directly.
- `BarrierConfig.Validate` is exported so a runtime-built config can be rejected instead of panicking.

### Added

- `config.MetricsConfig` and `quicraft.NewPrometheusMetricsCollectorWithConfig`, controlling metric label cardinality (see BREAKING above).
- `quicraft_metrics_per_shard_enabled` gauge, reporting whether per-shard metric labels are enabled.

- **`quicraft.Temporary(err) bool` and `quicraft.NotLeader(err) (replicaID uint64, ok bool)` classify the package's ~60 sentinels**, so a caller can decide whether to retry without string-matching.

  `Temporary` is true only for conditions that clear on their own: `ErrBusy`/`ErrSystemBusy`, `ErrTimeout`, `ErrAborted`, `ErrDropped`, `ErrShardNotStarted`, `ErrShardNotReady`, `ErrNotLeader`, `ErrReadIndexNotReady`, `ErrSnapshotInProgress`. Everything else is permanent, including errors this package never produced. Deliberately permanent, with the reason recorded in `pkg/errors.go`: `ErrStaleConfigChange` (re-read membership and build a new request), `ErrDraining`, `ErrCanceled` (a server-side deadline surfaces as `ErrTimeout`), `ErrSessionLimitExceeded`, `ErrBarrierSealed`, `ErrShardBarrierUnresolved`, `ErrDiskFull`, `ErrLogDBWrite` and `ErrShardFailed`.

  `NotLeader` surfaces `NotLeaderError.LeaderID`; `ok` is true for any not-leader error and `replicaID` is 0 when the leader is unknown. Both use `errors.Is`/`errors.As`, so wrapped and typed errors (`*BusyError`, `*NotLeaderError`) classify correctly. A test fails when `errors.go` declares an exported `Err*` var the classification table does not cover.

### Documentation

- The `SessionExpiryEntries` divergence is resolved by the replicated session policy (see BREAKING). `TestSessionExpiry_ReplicatedThresholdGovernsBothReplicas` asserts that two replicas with different local configuration apply identically and both follow the shard's policy when it enables expiry.

### Security

- **A delegated (hardware-backed) encryptor can now bind positional context, and a deployment can require it.** When a sealing strategy supplies its own encryptor, the barrier hands off every operation, so that path had no positional binding, epoch AAD, nonce-reuse tracking or NIST SP 800-38D invocation counting. A delegated encryptor may now implement the optional `seal.AADSymmetricEncrypter` to declare that it authenticates caller-supplied AAD end to end; context binding then works as in software mode, through the same AAD helper. An encryptor that does not implement it refuses a non-empty context. A backend that accepts an AAD parameter and discards it must not implement the interface.

  `BarrierConfig.RequireContextAAD` demands positional binding; it is checked when the barrier is initialized or unsealed, so an unsupported configuration fails at startup. It is refused for `StrategyNone`. Default false.

  `IsHardwareMode` keeps its meaning, and `ContextBinding()`, `SupportsContextAAD()` and `IsPlaintextMode()` now distinguish "nothing is encrypted" from "real encryption that cannot bind". The snapshot and WAL layers decide on the precise capability. The delegated-mode bypass of the nonce tracker and invocation counter is documented and logged at setup.

- **A LogDB that persists encrypted records but cannot name their epoch no longer has its keys destroyed.** The host-wide DEK purge treated any LogDB not implementing `EpochAwareLogDB` as imposing no floor, so a durable custom store encrypting under the host barrier could have the DEKs its records needed purged irreversibly. Purging now proceeds only on a positive `AtRestNonDurable` declaration; a store that declares `AtRestBarrierEncrypted`, `AtRestUnprotected`, or nothing suppresses purging and logs why. The per-shard-barrier floor already failed safe and is unchanged.
- `WithShardBarriers` without `WithBarrier` or `WithLogDB` is refused: the default WAL takes one barrier, so per-shard encryption alone produced encrypted snapshots over a plaintext log.
- **A failed sweep of a snapshot decrypt scratch file is now reported.** Both decrypt paths (recovery and snapshot send) write a plaintext copy of the state machine to a sibling scratch file. A failed removal was logged at DEBUG only, leaving the plaintext next to its ciphertext. Failures, including a directory the sweep cannot list, now log at WARN and increment `engine.StaleDecryptSweepFailures()`; any increase warrants an alert.
- **One snapshot directory with an indeterminate epoch no longer hides the others.** The DEK purge-floor scan now visits every directory and reports all indeterminate ones in a single record with a reason for each (`legacy_record_without_epoch` or `unreadable`). Such a directory still suppresses purging for the barrier that could have sealed it; in per-shard-barrier mode other tenants keep purging normally.

## [0.2.0-alpha] - 2026-08-06

### BREAKING

- **On-disk and wire formats changed; there is no migration from `0.1.0-alpha`.** Witness policy is replicated state: `MaxWitnesses` and `RequireFullReplicaMajority` live in `proto.Membership` and travel through the log, snapshots, the WAL and LogDB. `Membership` grew from an 8-byte base to 24, `ConfigChange` from a 36-byte fixed section to 52, and the WAL membership section by 9 bytes. `config.WitnessConfig` is only a bootstrap seed; change the policy at runtime with `Host.RequestUpdateWitnessPolicy` (and its `Sync` variant). Wipe existing data directories before upgrading.

### Security

- **Sealed barriers no longer downgrade to plaintext.** A configured but sealed barrier means encryption is temporarily unavailable. `saveSnapshot` (reachable via `EmergencySeal`), `ImportSnapshot` (which recorded `Epoch: 0` unconditionally) and `ReencryptSnapshotFile` (which copied ciphertext from a sealed source and recorded it as plaintext) durably wrote plaintext. All now fail closed with `ErrSnapshotBarrierSealed`.
- Imported snapshots are re-encrypted under the importing host's own barrier.
- Certificate revocation configured fail-closed with no CRL or OCSP source was a silent no-op.
- `Term: 0` wire messages bypassed every term check, letting a non-leader advance a follower's commit index.
- A client could fail a shard cluster-wide by pairing `ProposeWithSession` with `CloseSession` in one `Apply` batch, which errored at the same index on every replica.
- `BarrierConfig.Validate` is exported so embedders building a config at runtime can reject a bad algorithm instead of hitting `NewBarrier`'s panic.

### Fixed

- **Two leaders could be elected in one term.** Votes were counted over the vote map while quorum came from the live voter set, so a `RemoveNode` committed mid-election shrank quorum while a removed node's stale vote survived. Votes are now tallied over configuration voters, as in etcd and dragonboat.
- **An empty snapshot could be streamed to a follower.** The receive path never recorded `FileSize`, so a dedup re-delivery published 0 with a non-empty path and the replica recovered its state machine from zero bytes.
- **A WAL shard could be silently discarded and then truncated.** Any non-`ENOENT` `Stat` error (`EACCES`, `EIO`, a stale NFS handle) was treated as `ENOENT`, skipping recovery and reopening the segment with `O_TRUNC`: a double vote plus permanent loss of acknowledged writes.
- **Snapshot recovery could roll the state machine backwards**, regressing `lastApplied` into a permanent `ErrApplyGap` outage.
- A stale pending snapshot could supersede a newer one, rewinding the `LogReader` behind the apply frontier or failing the shard, and letting a rejected snapshot roll membership backwards.
- A compacted index returned a term instead of `ErrCompacted`, suppressing the leader's `InstallSnapshot` fallback so a lagging follower could never catch up.
- Raft could accept entry batches the WAL could never persist, because one `Update` was one WAL record and a record must fit one segment. Batches are now split across records.
- A log-replay joiner kept membership `{self}` forever and could elect itself a single-node leader that acknowledged writes the cluster discarded.
- `replayConfigChangesTo` applied uncommitted configuration changes.
- The WAL fsynced only its root's immediate parent, losing acknowledged writes when the root sat more than one level below the durable point.
- A non-monotonic persisted `compactedTo` turned `ErrCompacted` into a permanent `ErrLogGapDetected`, so raft never fell back to `InstallSnapshot`.

### Changed

- Reads no longer block on a writer's fsync. The WAL shard mutex is split: segment file state keeps the existing lock, and the in-memory maps readers consult move to a separate `RWMutex` held only briefly at commit.
- Toolchain pinned to Go 1.26.5 across `go.mod`, Docker and CI.
- `contrib/dragonboat` has the `replace` directive it needs to build and test standalone.

### Added

- `BarrierConfig.Validate` is exported, so a caller assembling a barrier config at runtime can reject a bad algorithm, or a non-FIPS-approved cipher under FIPS mode, instead of hitting `NewBarrier`'s panic.
- `BenchmarkThroughputQUIC`, the first end-to-end transport throughput benchmark.
- Per-shard barriers via `WithShardBarriers(fn func(shardID uint64) Barrier)`, for multi-tenant or per-shard key isolation. A single host-wide barrier still works.
- Snapshot re-encryption on receive: `reencryptReceivedSnapshot` decrypts the sender's plaintext snapshot and re-encrypts it under the receiver's own barrier before install, so nodes share no at-rest keys.
- `waldb.Reencrypt(srcDir, srcBarrier, dstDir, dstBarrier)` migrates a WAL through the public LogDB surface, for none-to-keyed migration and key rotation.
- Per-shard barrier routing in snapshot transport via `host.barrierForShard(shardID)`.

### Changed

- Host snapshot pool DEK purge dispatches per shard when `WithShardBarriers` is configured, so one tenant's purge cannot remove epochs live under another tenant's barrier.

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
