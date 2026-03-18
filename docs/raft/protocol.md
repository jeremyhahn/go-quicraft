# Raft Protocol Implementation

This document describes the Raft consensus protocol as implemented in
QuicRaft. All code lives under `pkg/internal/raft/`. The implementation
has been verified against the extended Raft paper (Ongaro & Ousterhout,
2014) and the Ongaro PhD thesis through four comprehensive spec audits
covering all 5 safety properties and 33 figure rules. References are noted
as "Paper X.Y" and "PhD X.Y".

Source files:

    pkg/internal/raft/raft.go        Core state machine
    pkg/internal/raft/peer.go        Engine-to-raft boundary
    pkg/internal/raft/log.go         Composite log (inmem + LogDB)
    pkg/internal/raft/inmemory.go    In-memory entry buffer
    pkg/internal/raft/remote.go      Per-follower replication tracker
    pkg/internal/raft/inflights.go   Sliding-window flow control
    pkg/internal/raft/membership.go  Cluster membership management
    pkg/internal/raft/quiesce.go     Quiesce mode
    pkg/internal/raft/util.go        Quorum math
    pkg/internal/raft/errors.go      Typed error definitions

## Spec Compliance Status

QuicRaft has passed four comprehensive Raft specification audits across
the thesis and paper. All 5 safety properties are verified:

1. **Election Safety**: At most one leader can be elected per term.
2. **Leader Append-Only**: A leader never overwrites or deletes entries.
3. **Log Matching**: If two logs contain an entry with same index and term,
   all entries preceding that point are identical.
4. **Leader Completeness**: Every committed entry is present in the leader's log.
5. **State Machine Safety**: If a server has applied log entry at given index
   to state machine, no other server will apply a different entry at that index.

All 33 figure rules from the thesis are implemented and tested. See the
internal project memory for detailed audit findings.

## 1. Overview

The `raft` struct (`raft.go:76`) is a pure deterministic state machine.
It performs no I/O. Messages flow in via `step()`, messages flow out via
`drainMessages()`. The engine layer (through `Peer`) is responsible for
persisting state, sending network messages, and driving the clock.

```
Engine ──Tick/Handle/Propose──> Peer ──step()──> raft
                                                   |
                                          msgs []proto.Message
                                                   |
Engine <──GetUpdate/Commit──── Peer <──drainMessages()
```

The `raft` struct is owned by exactly one goroutine at a time. No
mutexes are used. The only concurrently-read field is `Peer.leaderID`,
which is an `atomic.Uint64` synced after every operation.

### Function Pointer Dispatch

Role-specific behavior is dispatched through two function pointers set
during role transitions (`raft.go:151-152`):

```go
stepFunc func(r *raft, msg proto.Message) error
tickFunc func(r *raft)
```

This eliminates a branch on every step/tick call. The concrete functions
are `stepFollower`/`stepCandidate`/`stepLeader` and
`tickFollower`/`tickCandidate`/`tickLeader`.

### Double-Buffered Message Output

Outbound messages accumulate in `r.msgs`. The `drainMessages()` method
(`raft.go:1470`) swaps `msgs` and `msgsBuf` to return the current batch
and prepare a fresh buffer, avoiding allocation on every drain cycle.

## 2. Role Transitions

Four roles are defined (`raft.go:23-28`):

    follower(0) --> preCandidate(2) --> candidate(1) --> leader(3)

### State Transition Diagram

```
                 +--timeout--+
                 |           |
                 v           |
    +----------[PreCandidate]----------+
    |  preVote     |                   |
    |  won         | preVote lost      |
    |              v                   |
    |         [Follower] <----+--------+
    |              |          |
    |              | timeout  | AppendEntries / higher term
    |              v          |
    +-------> [Candidate]-----+
                   |
                   | election won
                   v
              [Leader]
                   |
                   | loses quorum / higher term
                   v
              [Follower]
```

### becomeFollower (raft.go:250)

- Sets `stepFunc = stepFollower`, `tickFunc = tickFollower`.
- Clears `vote` ONLY when the term changes. Preserving the vote within
  the same term prevents double-voting (Paper Figure 2, Fix 1).
- Resets `transferTarget`, `pendingReadIndex`, `pendingConfigChange`.
- Randomizes the election timeout.

### becomeCandidate (raft.go:269)

- Asserts caller is not already leader.
- Increments `term`, votes for self (`vote = replicaID`).
- Resets vote map and records self-vote.

### becomePreCandidate (raft.go:288)

- Does NOT increment term or change vote (PhD 9.6).
- Uses the same `stepCandidate` handler as real candidates.
- Records self-vote in the vote map.

### becomeLeader (raft.go:306)

- Asserts caller is not a follower (must have won election).
- Resets all remote trackers to probe state with `next = lastIndex + 1`.
- Appends a no-op entry at the current term to establish commit authority
  over entries from prior terms (Paper 5.4.2).
- Calls `tryCommit()` (handles single-node clusters).
- Broadcasts the append to all peers.

## 3. Elections

### PreVote Protocol (PhD 9.6)

When `preVote` is enabled, a node first enters the `preCandidate` state
before running a real election. PreVote requests use `term + 1` as the
prospective term without actually incrementing the node's term
(`raft.go:621-624`). This prevents disruption from partitioned nodes
that would otherwise increment their term and force the cluster to
advance.

### Election Flow

```
startElection()  (raft.go:595)
  |
  +-- preVote enabled?
  |     |
  |     +-- becomePreCandidate()
  |     +-- broadcastPreVote()     --> campaign(RequestPreVote)
  |     +-- checkElectionResult()
  |           |
  |           +-- voteWon --> becomeCandidate() --> broadcastVote()
  |           +-- voteLost --> becomeFollower()
  |
  +-- preVote disabled?
        |
        +-- becomeCandidate()
        +-- broadcastVote()        --> campaign(RequestVote)
        +-- checkElectionResult()
              |
              +-- voteWon --> becomeLeader()
              +-- voteLost --> becomeFollower()
```

Observers and witnesses never start elections (`raft.go:596-598`).

### Vote Granting (raft.go:664)

Two conditions must hold for a vote grant:

1. **Log up-to-date check** (Paper 5.4.1): The candidate's last log entry
   must have a higher term, or the same term with an equal-or-higher index.

   ```go
   func (l *raftLog) isUpToDate(lastIndex, lastTerm uint64) bool
   ```

2. **Vote availability**:
   - Real vote: `vote == noNode || vote == msg.From`
   - PreVote: Log is up-to-date AND no active leader lease. The lease
     check is tick-based: `leaderID != noLeader && electionTick < electionTimeout`.

When granting a real vote, the election tick is reset to zero, extending
the current leader's implicit lease window.

### Randomized Election Timeout

The election timeout is randomized in `[electionTimeout, 2*electionTimeout)`
using a math/rand PRNG seeded from `crypto/rand` (`raft.go:1510-1511`):

```go
r.randomizedElectionTimeout = r.electionTimeout + r.rng.Intn(r.electionTimeout)
```

### Quorum Calculation

Vote tallying (`raft.go:742`) counts granted and rejected votes against
the election quorum. The election is decided early when either
`granted >= quorum` or `rejected > total - quorum`.

```go
func electionQuorum(remotes, witnesses map[uint64]*remote) int {
    return numVotingMembers(remotes, witnesses)/2 + 1
}

func numVotingMembers(remotes, witnesses map[uint64]*remote) int {
    return len(remotes) + len(witnesses) + 1  // +1 for self
}
```

Both full replicas and witnesses vote in elections.

## 4. Log Replication

### sendAppend (raft.go:817)

Called per-follower to send a `Replicate` (AppendEntries) message:

1. Skip if `rm.isPaused()` (probe waiting, snapshot in progress, or
   inflights full).
2. Look up `prevLogTerm` at `rm.next - 1`. If unavailable (compacted),
   fall back to `sendSnapshot`.
3. Fetch entries from `rm.next` to `lastIndex + 1`. If unavailable,
   fall back to `sendSnapshot`.
4. Send `Replicate` with `LogIndex` (prevLogIndex), `LogTerm`, entries,
   and current `committed` index.
5. In replicate state: optimistically advance `rm.next` and track
   the last entry index in inflights.
6. In probe state: set `probeSent = true` to pause further sends.

### handleAppend (raft.go:885)

Follower processing of AppendEntries:

1. Reset election tick and record leader ID.
2. Call `log.tryAppend(msg.LogIndex, msg.LogTerm, msg.Entries)`.
3. On success: advance commit to `min(msg.Commit, lastNewIdx)`.
   Respond with `ReplicateResp` carrying `lastIdx`.
4. On failure: respond with reject and `Hint = min(msg.LogIndex, lastIndex)`.

### handleAppendResp (raft.go:920)

Leader processing of AppendEntries response:

1. Mark the remote as active.
2. On **reject**: call `rm.declineFrom(msg.LogIndex, msg.Hint)` for
   index-based backtracking. If in replicate state, fall back to probe.
   Immediately retry with `sendAppend`.
3. On **success**: call `rm.update(msg.LogIndex)`. If match advanced and
   `tryCommit()` succeeds, broadcast the new commit to all followers.
   If the remote is the leader transfer target and now caught up,
   send `TimeoutNow`.
4. Free inflights up to `msg.LogIndex`. If window has capacity, send
   more entries.

### tryAppend (log.go:190)

The log matching check (Paper 5.3). Verifies `matchTerm(prevLogIndex,
prevLogTerm)`, then finds the first conflicting entry via
`getConflictIndex`. Entries are appended starting from the conflict
point; already-matching prefix entries are preserved.

**Critical safety property**: `getConflictIndex` skips entries at or
below the committed index (Paper 5.1 ensures these entries match across
all servers). This prevents falsely identifying committed entries as
conflicts in multi-leader scenarios where the same batch drain may
advance committed index partway through processing multiple `Replicate`
messages.

### Replication Chunking

The `sendAppend` method bounds each `Replicate` message using
`maxReplicationPayload` (`raft.go:160-163`), set from
`Config.MaxApplyEntrySize` (default 64 MB, matching dragonboat).

```go
entries, err := r.log.entries(rm.next, r.log.lastIndex()+1, r.maxReplicationPayload)
```

The `raftLog.entries(low, high, maxSize)` method (`log.go:120`) returns
entries from `[low, high)` up to `maxSize` total bytes. When a follower
is far behind, this produces a bounded batch instead of an unbounded
Replicate message that could exhaust memory on both sender and receiver.

The follower acknowledges each batch with `ReplicateResp`, which
triggers `handleAppendResp` on the leader. The leader advances
`rm.next` and calls `sendAppend` again, naturally streaming the next
chunk. This yields a pipelined chunking protocol where the follower
catches up in bounded increments without requiring explicit chunking
state.

When `maxReplicationPayload` is 0, entries are unlimited (not
recommended for production). The default 64 MB aligns with
`MaxApplyEntrySize` (apply batch limit) and `MaxMessageBatchSize`
(transport batch limit), ensuring a single Replicate message fits
within the transport's send buffer.

### Witness Replication Behavior

Witnesses are voting members that do not store full log entries. During
`broadcastAppend` (`raft.go:1009-1025`), the leader sends `Replicate`
messages to remotes and observers, and sends metadata-only Replicate
messages to witnesses via `sendWitnessAppend`:

```go
func (r *raft) broadcastAppend() {
    for id := range r.remotes {
        r.sendAppend(id)
    }
    for id := range r.observers {
        r.sendAppend(id)
    }
    for id := range r.witnesses {
        r.sendWitnessAppend(id)
    }
}
```

`sendWitnessAppend` (`raft.go:1105-1170`) sends Replicate messages
containing `EntryMetadata` entries that carry Index and Term but no
Cmd payload. Config change entries are sent unmodified so witnesses
can maintain correct membership. This allows witnesses to track log
position for election up-to-date checks and commit index advancement
without storing full entry data. Witnesses are excluded from
`commitQuorum` (see Section 6) because they do not replicate full
entries.

## 5. Remote State Machine

Each follower tracked by the leader has a `remote` struct (`remote.go:52`)
with four states:

```
                     +---------- success -----------+
                     |                              |
  [Probe] ----respondedTo()----> [Replicate] -------+
    ^  ^                              |
    |  |                         unreachable / reject
    |  |                              |
    |  +--------- becomeProbe --------+
    |
    +-- snapshotDone / snapshotFailed
    |
  [Snapshot] <--- sendSnapshot --- [Probe/Replicate]
```

**Probe**: One message in flight. `probeSent` flag gates further sends.
The leader waits for the response to discover the follower's position.

**Replicate**: Sliding window of up to `maxInflightMsgs` (256) entries
in flight. `rm.next` is advanced optimistically on send.

**Snapshot**: A snapshot is being transferred. All sends are paused.
On completion (`snapshotDone`/`snapshotFailed`), transitions to probe.

### Inflights (inflights.go)

Circular buffer tracking in-flight entry indices. Lazily allocated on
first `add()`. `freeTo(index)` frees all entries with index <= the
given value. `full()` returns true when `count >= size`.

### declineFrom (remote.go:190)

Handles rejected appends with index-based backtracking:

- **Replicate state**: Fall back to `match + 1` (known safe).
- **Probe state**: `next = max(1, min(rejectedIndex, hintIndex + 1))`.
  This gives O(entries) convergence per the dragonboat reference formula.

## 6. Commit Calculation

### tryCommit (raft.go:975)

Uses a pre-allocated sort buffer (`r.matched`) to find the median match:

```go
func (r *raft) tryCommit() bool {
    r.matched = r.matched[:0]
    r.matched = append(r.matched, r.log.lastIndex())  // leader's own
    for _, rm := range r.remotes {
        r.matched = append(r.matched, rm.match)
    }
    slices.Sort(r.matched)
    quorum := commitQuorum(r.remotes)
    mci := r.matched[len(r.matched)-quorum]
    return r.log.tryCommit(mci, r.term)
}
```

The `N-quorum`-th element from the sorted match array is the highest
index replicated to a quorum. The `log.tryCommit` call additionally
verifies the entry at that index has the leader's current term
(Paper 5.4.2 -- leaders only commit entries from their own term).

### Commit Quorum vs Election Quorum

```go
// Election quorum: remotes + witnesses + self.
// Witnesses vote but do not store data.
func electionQuorum(remotes, witnesses) int {
    return (len(remotes) + len(witnesses) + 1) / 2 + 1
}

// Commit quorum: remotes + self only.
// Witnesses are excluded because they do not replicate entries.
func commitQuorum(remotes) int {
    return (len(remotes) + 1) / 2 + 1
}
```

This separation means witnesses increase fault tolerance for leader
election without affecting the data replication quorum.

## 7. Snapshots

### Snapshot Membership Persistence

Snapshots now persist full membership information in the WAL, including
epoch and all members (voting, observer, witness). The `ReadState`
returned by `LogReader.ReadState()` includes the membership from the
latest snapshot, allowing recovery to restore the cluster topology
without requiring external configuration.

### handleSnapshot (raft.go:1092)

Processes `InstallSnapshot` on a follower (Paper Figure 13):

1. Reject if `ss.Index <= committed` (snapshot is stale, prevents applying
   older state).
2. **Log matching optimization**: If the existing log has an entry at
   `ss.Index` with the same term, only advance the commit index. Entries
   following the snapshot point are retained. This avoids discarding
   valid log entries the follower already has.
3. Otherwise, call `log.restore(ss)` which discards all entries and
   resets from the snapshot.
4. Rebuild all remote tracking maps from `ss.Membership`.
5. Set `recovering` atomic flag during log truncation to gate state
   machine apply operations and prevent concurrent recovery races.
6. Respond with `ReplicateResp` carrying the snapshot index.

### Snapshot Sending (raft.go:863)

When `sendAppend` cannot resolve `prevLogTerm` or entries (log
compacted), it calls `sendSnapshot`:

1. Get the latest snapshot from `log.snapshot()`.
2. Transition the remote to `remoteSnapshot` state.
3. Send `InstallSnapshot` message.

The engine layer (`transport`) handles snapshot chunking for large
snapshots, reassembling chunks on disk before calling `LogDB.SaveSnapshot`.

### rebuildRemotesFromMembership (raft.go:1131)

After snapshot restore, the peer set may have changed. This method
rebuilds `remotes`, `observers`, and `witnesses` maps from the
snapshot's membership, creating fresh `remote` structs for each peer.

### Snapshot Recovery vs Apply Race

The `recovering` atomic flag gates state machine apply operations during
snapshot recovery. When a snapshot is being restored:

1. Engine thread acquires `recovering` lock for log truncation.
2. Apply worker checks `recovering` flag before applying entries.
3. Once log is truncated and ready, `recovering` is released.
4. Apply worker resumes applying remaining entries.

This prevents the state machine from applying entries that exist in both
the snapshot and the log, maintaining deterministic SM state.

## 8. ReadIndex

Linearizable reads without log writes (PhD 6.4, Figure 6.4). The leader confirms
it still holds authority via a heartbeat quorum round.

### Batch Coalescing (OPT-8)

Multiple concurrent ReadIndex requests are coalesced into a single heartbeat
round. The step worker maintains a dedicated lock-free MPSC `readIndexQ`
queue separate from the proposal inbox. Each tick, the step worker drains
ALL pending ReadIndex keys at once and submits a single `Peer.Handle(ReadIndex)`
call with the first key as `batchKey`. All pending keys complete when the
`batchKey` heartbeat receives quorum confirmation. This reduces heartbeat
overhead from O(N) per caller to O(1) for N concurrent callers.

### Flow

```
Client ---ReadIndex(key)---> Host.ReadIndex()
                              |
                        append to readIndexQ (lock-free)
                              |
Step Worker (per tick)
  |
  +-- drain ALL keys from readIndexQ
  +-- submit one ReadIndex msg with first key as batchKey
  |
  +-- Peer.Handle(ReadIndex msg) -> raft leader/follower handler
  |
  +-- leader checks hasCommittedEntryAtCurrentTerm()
  |     |
  |     +-- single node? --> respond immediately
  |     +-- else: append to pendingReadIndex[]
  |            broadcastHeartbeatCtx(readIndexCtx++)
  |
  +-- Peer.GetUpdate() -> update.ReadyToRead = raft.readStates
  |
  +-- callback.OnReadyToRead() triggers for all aliases of batchKey
  |     |
  |     +-- mark as confirmed, set readIndex
  |     +-- if lastApplied >= readIndex: complete immediately
  |
  +-- (ReadOnly bypass: skip commit/apply pipeline for read-only updates)
  |
Apply Worker / OnApplied callback
  |
  +-- for each confirmed read where lastApplied >= readIndex:
  |     complete RequestState with readIndex value
```

### Key Design Points

**PhD 6.4 guard** (`raft.go:1238`): The leader must have committed at
least one entry from its own term (the no-op) before serving reads.
Without this, the committed index might not reflect the true state.

**Heartbeat context correlation** (`raft.go:1276-1286`): Each ReadIndex
request (or batch of coalesced requests) gets a unique `readIndexCtx`.
This ctx is carried in the heartbeat `Hint` field and echoed back in
responses, allowing multiple concurrent batches to be tracked independently.

**Out-of-order resolution** (`raft.go:1322`): Resolved requests are
compacted from `pendingReadIndex` regardless of position. A slow
heartbeat response for batch N does not block batch N+1 that already
has quorum. This eliminates head-of-line blocking.

**Follower forwarding** (`raft.go:1293`): Followers forward ReadIndex
requests to the leader via the inbox. When the leader's quorum confirms,
the leader emits `ReadIndexResp` messages that flow back to the originating
follower's callback, identified by `pending.from`.

**Context reservation** (`raft.go:123-126`): `readIndexCtx` is
pre-incremented before use, so the first ReadIndex ctx is 1. Context 0
is reserved for regular heartbeats (`broadcastHeartbeat` passes ctx=0).
This prevents regular heartbeat responses from falsely satisfying
ReadIndex quorum checks.

**Bitfield-based ack tracking** (`raft.go:68-86`): Each
`readIndexStatus` tracks heartbeat acks with a `uint64` bitfield instead
of a map. Replica IDs are mapped to bit positions 0..63 via
`raft.replicaToBit`, built once in `becomeLeader`. This gives
zero-allocation ack tracking on the hot path. Clusters are limited to
64 voting members in practice.

```go
type readIndexStatus struct {
    key      uint64  // client correlation key (first key in batch)
    index    uint64  // committed index at request time
    from     uint64  // originating follower, or 0 for local
    ctx      uint64  // heartbeat correlation context
    ackBits  uint64  // bitfield: bit i set when replica i acknowledged
    ackCount int     // popcount of ackBits (avoids recomputing)
}
```

**Commit quorum for ReadIndex** (`raft.go:1439`):
`checkReadIndexQuorum` uses `commitQuorum` (remotes + leader), not
`electionQuorum`. Witnesses are excluded from ReadIndex confirmation
by design—they return early from `handleHeartbeatResp` before the
ReadIndex ack path. This matches the data-bearing quorum used for commit
calculation.

**Witness ReadIndex rejection** (`raft.go:1261`): Witnesses cannot serve
ReadIndex requests; they return `ErrInvalidOperation`. Witnesses lack the
full log needed to provide linearizable reads.

### End-to-End Host Pipeline

The raft-level ReadIndex flow above produces `readStates` on the raft
struct. These flow through the engine and Host layers to complete the
caller's request:

```
Host.ReadIndex(shardID)
  |
  +-- generate readKey (unique uint64)
  +-- create pendingRead{rs: RequestState} in ns.pendingReads map
  +-- deliver ReadIndex message to engine inbox
  +-- wake step worker
  |
Step Worker (step.go)
  |
  +-- Peer.Handle(ReadIndex msg) -> raft ReadIndex handler
  +-- Peer.GetUpdate() -> update.ReadyToRead = raft.readStates
  +-- callback.OnReadyToRead(shardID, update.ReadyToRead)
  |     |
  |     +-- match readKey in ns.pendingReads
  |     +-- set pr.readIndex, pr.confirmed = true
  |     +-- if lastApplied >= readIndex: complete immediately
  |
  +-- (ReadOnly bypass: skip commit/apply pipeline)
  |
Apply Worker / OnApplied callback
  |
  +-- on each batch of applied entries:
  |     for each confirmed pendingRead where lastApplied >= readIndex:
  |       complete RequestState with readIndex value
  |
Caller
  |
  +-- rs.Result() returns once readIndex is reached
```

The `pendingRead` struct (`host.go:363`) tracks two phases:

```go
type pendingRead struct {
    rs        *RequestState
    readIndex uint64  // set when OnReadyToRead confirms
    confirmed bool    // true once readIndex is known
}
```

A read completes in one of two places:

1. **OnReadyToRead**: If `lastApplied >= readIndex` at the time the
   heartbeat quorum confirms, the read completes immediately.
2. **OnApplied**: If the state machine has not yet caught up, the read
   remains pending. Each `OnApplied` callback scans confirmed pending
   reads and completes those whose `readIndex` has been reached.

### SyncRead

`Host.SyncRead(ctx, shardID, query)` (`host.go:1128`) wraps the
asynchronous ReadIndex pipeline into a synchronous linearizable read:

1. Call `Host.ReadIndex()` to get a `RequestState`.
2. Wait for the `RequestState` to resolve (lastApplied >= readIndex).
3. Execute `SM.Lookup(query)` on the now-caught-up state machine.

This is the primary API for linearizable reads. The caller blocks until
the state machine has applied all entries up to the confirmed read
index, then queries the state machine directly.

### ReadOnly Bypass Optimization

ReadIndex-only updates carry no entries, state changes, or snapshots.
The `Peer.GetUpdate()` method (`peer.go:468`) detects this condition
and sets `update.ReadOnly = true`:

```go
update.ReadOnly = !stateChanged &&
    len(update.EntriesToSave) == 0 &&
    len(update.CommittedEntries) == 0 &&
    update.Snapshot.Index == 0
```

The step worker checks `ReadOnly` after dispatching messages and invoking
the `OnReadyToRead` callback:

```go
if update.ReadOnly {
    continue  // skip commit/apply pipeline
}
```

This bypasses the commit worker (LogDB persistence), apply worker (SM
apply), and the `commitPending` blocking flag. ReadIndex confirmations
are delivered inline in the step worker via the `OnReadyToRead`
callback, eliminating three goroutine hops and the associated context
switch latency.

When combined with batch coalescing (OPT-8), ReadIndex latency is
dominated by a single heartbeat round-trip: leader → followers → leader.
On a quiescent cluster, this is typically 1-5 milliseconds depending on
network and scheduling.

## 9. Membership Changes

### One-at-a-Time Configuration (PhD 4.1, Paper Section 6)

Only one config change entry is allowed in flight at a time. The
`pendingConfigChange` flag on the raft struct (`raft.go:125`) enforces
this constraint. Attempting to propose a second config change while one
is uncommitted returns `ErrPendingConfigChange`. The flag is cleared when
the config change entry commits (`peer.go:458`).

This ensures safe cluster reconfigurations by preventing two overlapping
config change entries, which could lead to two disjoint majorities.

### Config Change Takes Effect on Append

Per PhD thesis 4.1, membership changes take effect immediately when the
config change entry is appended to the log, not when it commits. This is
implemented in two places:

1. **Leader** (`peer.go:212-226`): After `ProposeConfigChange`, the
   membership is updated and `syncRaftRemotes()` adds/removes remote
   trackers immediately.

2. **Follower** (`peer.go:127-132`): After receiving `Replicate` with
   config change entries (verified actually appended via `matchTerm`),
   `processConfigChanges()` applies each to membership and syncs remotes.

This early application is safe because the entry's position in the log is
established before any node can become leader (Ongaro 4.1).

### ConfigChangeID Staleness Detection

When proposing a config change, each change is assigned a unique
`ConfigChangeID` (from `sync.Mutex` protected counter). Before appending
to the log, the leader verifies the `ConfigChangeID` matches the current
state; if it was already applied, the entry is skipped. This prevents
stale config changes from being re-applied after snapshot recovery.

### Config Change Rollback on Log Truncation

When the log is truncated (typically during snapshot recovery), committed
config changes may be removed. The `rebuildMembershipFromLog()` function
rescans the log to rebuild the membership to match reality. This prevents
a stale membership snapshot from persisting after log truncation.

### Dispatch Table

Config change handling uses a map-based dispatch (`membership.go:15-20`):

```go
var configChangeHandlers = map[uint64]configChangeHandler{
    proto.AddNode:      handleAddNode,
    proto.RemoveNode:   handleRemoveNode,
    proto.AddNonVoting: handleAddNonVoting,
    proto.AddWitness:   handleAddWitness,
}
```

Each handler validates preconditions and updates the `addresses`,
`observers`, or `witnesses` maps.

### Node Types

**Voters (Remotes)**: Full replicas that replicate the entire log and
participate in elections and commit quorum.

**Observers (Non-Voting Learners)**: Replicate the full log but do not
vote. Used to bring new nodes up to speed before promotion to voter.

**Witnesses**: Vote in elections but do not store full log entries (only
metadata). Increase fault tolerance for leadership without affecting data
quorum.

### Leader Self-Removal (Paper Section 6)

When a committed config change removes the leader from membership,
`checkLeaderSelfRemoval()` (`peer.go:286`) transitions the leader to
follower. This is checked during `Peer.Commit()` after processing
committed config change entries. A stepped-down leader continues sending
heartbeats to maintain liveness until it receives `Replicate` from the new
leader.

### Safety Validations

- Cannot remove the last voting member (`ErrRemoveLastVoter`).
- Cannot add a previously removed node (`ErrNodeRemoved`).
- Cannot add a node that already exists (`ErrNodeAlreadyExists`).
- Observer-to-voter promotion: `handleAddNode` deletes from observers
  before adding to addresses.

### CampaignSkipped Event

When a node receives a `Propose` or `ReadIndex` but has an uncommitted
config change, the raft core emits a `CampaignSkipped` event (via
`checkConfigChangeUnsafety` and event listener callback). The engine
can use this signal to emit metrics or warnings about pending config
changes blocking client requests.

## 10. Quiesce Mode

Quiesce reduces resource consumption for idle shards by suppressing
normal heartbeats and extending election timeouts. Idle is defined as
no proposals, config changes, or reads for an extended period.

### Configuration Constants (quiesce.go)

    quiesceThresholdMultiplier         = 10   (10 * electionTimeout ticks)
    quiesceLivenessMultiplier          = 10   (liveness probe interval)
    quiesceElectionTimeoutMultiplier   = 3    (3x liveness interval)

### Entry and Exit

```
Normal Operation
  |
  +-- no activity for (electionTimeout * 10) ticks
  |
  +-- tryEnter() returns true
  |     |
  |     +-- leader: broadcastQuiesce() to all peers
  |     +-- follower/candidate: enter via Quiesce message from leader
  |
  v
Quiesce Mode
  |
  +-- leader: sends liveness probes every (electionTimeout * 10) ticks
  +-- follower: uses election timeout of (livenessInterval * 3)
  |
  +-- any proposal, config change, or read index
  |     |
  |     +-- recordActivity() --> exit()
  |
  v
Normal Operation
```

**Leader tick** (`raft.go:544-551`): During quiesce, only liveness
probes are sent. Normal heartbeat and checkQuorum logic are skipped.
Liveness probes carry no log entries, just the leader's term and commit.

**Follower tick** (`raft.go:508-517`): Uses the extended election
timeout (`quiesceElectionTimeout()`). If the timer expires while
quiesced, the follower exits quiesce and starts an election. The
extended timeout prevents unnecessary elections on idle clusters.

**Activity detection**: `recordActivity()` (`quiesce.go:73`) resets the
inactivity counter and exits quiesce if currently active. Activity is
recorded in four places:

1. **Leader proposals** (`raft.go:859`): `handleLeaderPropose` calls
   `recordActivity()` to exit quiesce before accepting writes.
2. **Leader ReadIndex** (`raft.go:1354`): `handleLeaderReadIndex` calls
   `recordActivity()`.
3. **Leader config changes** (`raft.go:987`): proposal path calls
   `recordActivity()`.
4. **Follower proposal forwarding** (`raft.go:504-506`): When a follower
   receives a `Propose` message and forwards it to the leader,
   `stepFollower` calls `recordActivity()` to exit quiesce. Without
   this, the follower would stay quiesced with an extended election
   timeout while client proposals are in flight, potentially causing
   unnecessary elections if the leader is briefly delayed.

The quiesce mechanism is transparent to the application—transitions are
driven by the local tick clock without requiring explicit coordination.

## 11. Leader Transfer

### Flow

```
RequestLeaderTransfer(target)
  |
  handleLeaderTransfer()  (raft.go:1362)
  |
  +-- target caught up? (rm.match >= lastIndex)
  |     |
  |     +-- yes: sendTimeoutNow(target)
  |     +-- no:  sendAppend(target) to catch up
  |
  +-- transferTimeout expires (electionTimeout ticks)
  |     |
  |     +-- cancel: transferTarget = noNode
```

### TimeoutNow (raft.go:1407)

The target receives `TimeoutNow` and immediately starts an election,
bypassing PreVote. This ensures the transfer completes quickly since the
target is known to be caught up.

Proposals are rejected with `ErrTransferInProgress` while a transfer is
in progress (`raft.go:778`).

### Catch-Up Replication

If the target is not caught up, the leader resets `probeSent = false`
and sends an append immediately (`raft.go:1390-1391`). Each successful
`handleAppendResp` re-checks whether the target has caught up
(`raft.go:953-955`).

## 12. Rate Limiting

Per-shard in-memory log growth is controlled by a per-shard
`InMemRateLimiter` configured at shard startup.

### Leader Side

- `rl.Tick()` is called every leader tick (`raft.go:553-555`).
- `handleLeaderRateLimit()` records follower in-memory sizes reported
  via `RateLimit` messages carrying hints from followers.
- `rateLimited()` returns true when the threshold is exceeded, causing
  the leader to back off proposals.

### Follower Side

- `tickRateLimit()` (`raft.go:1188`) sends the local in-memory log size
  to the leader every `electionTimeout` ticks via a `RateLimit` message.
  The `Hint` field carries the byte size of the in-memory buffer.

### InMemory Tracking

The `inMemory` struct tracks rate limiter state (`inmemory.go:62-67`):

- `rl`: Per-shard `InMemRateLimiter` (checked on proposals)
- `globalRL`: Cross-shard global `RateLimiter` (bypass for ReadIndex)

**Note**: ReadIndex requests bypass the per-shard rate limiter (OPT-27)
to prevent reads from being blocked during write backpressure. Only
proposals are rate limited.

Both limiters are updated on `merge()` (entries appended) and
`appliedLogTo()` (entries truncated after apply).

## 13. Peer API and Concurrency Model

The `Peer` struct (`peer.go:31`) bridges the engine and the raft state
machine. It is owned by exactly one worker goroutine at a time. Ownership
transfers through strict ordering:

1. **Step Worker**: Calls `Tick()`, `Handle()`, `Propose()`, `ReadIndex()`,
   `GetUpdate()` in sequence each tick.
2. **Commit Worker** (if entries need persist): Calls `Commit()` after LogDB
   completes.
3. **Back to Step Worker**: Next tick cycle.

No mutexes protect the raft state machine itself. Synchronization is managed
at the engine layer through atomic flags and channels. The only field safe
for concurrent reads is `leaderID`, an `atomic.Uint64` synced after each
operation via `syncLeaderID()`.

### Core Methods

**Tick()** (`peer.go:109`): Advances the logical clock. Delegates to
`r.tickFunc(r)`. Syncs `leaderID` after.

**Handle(msg)** (`peer.go:118`): Routes an inbound message to
`r.step(msg)`. After step, processes config changes from Replicate
messages and handles snapshot membership updates.

**Propose(entries)** (`peer.go:154`): Wraps entries in a `Propose`
message and calls `r.step`. Returns the last assigned log index.

**ProposeConfigChange(cc)** (`peer.go:178`): Validates the change,
serializes to `EntryConfigChange`, proposes, then immediately applies
to membership and syncs raft remotes.

**ReadIndex(key)** (`peer.go:319`): Wraps in a `ReadIndex` message and
calls `r.step`.

**HasUpdate(lastApplied)** (`peer.go:354`): Returns true if any state
needs processing: hard state changed, messages pending, read states
available, entries to save or apply, or pending snapshot.

**GetUpdate(moreEntriesToApply, lastApplied)** (`peer.go:386`): Collects
the current delta into a `proto.Update`: entries to save, committed
entries to apply, messages to send, read states, snapshot, and hard
state. Records `prevState` for the next diff.

**Commit(update)** (`peer.go:448`): Acknowledges engine processing after
entries have been persisted to LogDB:

1. Advances the processed marker via `appliedTo()`.
2. Clears `pendingConfigChange` flag if a config change committed.
3. Checks leader self-removal (steps down if removed by config change).
4. Updates the LogReader with persisted entries (critical for term cache
   recovery after node restart).
5. Calls `log.commitUpdate()` to advance `savedTo` and truncate inmem buffer.

### Atomic LeaderID

`Peer.leaderID` is an `atomic.Uint64` (`peer.go:58`). It is the only
field safe for concurrent reads from any goroutine. Updated by
`syncLeaderID()` after every tick, step, propose, and GetUpdate call.
This enables lock-free `LeaderID()` and `IsLeader()` queries for routing
decisions without blocking the raft state machine.

## 14. Raft Log Architecture

The log is a two-tier structure: `raftLog` (`log.go:49`) composes
`inMemory` and `LogReader`.

```
  +---------- raftLog ----------+
  |                             |
  |   inmem (fast, volatile)    |  entries[0..N], markerIndex, savedTo
  |       |                     |
  |       v                     |
  |   LogReader (persistent)    |  LogDB on disk
  |                             |
  |   committed                 |  highest committed index
  |   processed                 |  highest index returned for apply
  +-----------------------------+
```

### Entry Lifecycle

1. **Append**: `raft.log.append()` -> `inmem.merge()`. Entries are
   assigned term and index, then placed in the in-memory buffer.

2. **Save**: `entriesToSave()` returns entries with index > `savedTo`.
   The engine persists these to LogDB.

3. **Commit**: Leader calculates the quorum match index. Follower
   learns the commit from the leader's `Commit` field in Replicate or
   Heartbeat messages.

4. **Apply**: `entriesToApply()` returns entries in
   `[processed+1, committed+1)`, limited to 64 MB
   (`maxEntriesToApplySize`). Up to 64 MB of entries are returned per
   apply tick to keep batches reasonably sized.

5. **Truncate**: After apply, `appliedLogTo()` truncates the inmem
   buffer from the front, advances the processed index, and decreases
   rate limiter counters.

### InMemory Buffer (inmemory.go)

Pre-allocated to 512 entries (`entrySliceSize`). The `merge()` method
handles three cases:

- **Contiguous**: New entries follow existing -- simple append.
- **Replace all**: New entries start at or before `markerIndex` -- reset.
- **Overlap**: New entries overlap -- keep prefix, append suffix. Guard
  against `offset > len(entries)` prevents panic on nil slices after
  snapshot restore.

Resize management: `shrunk` flag tracks when front-truncation has
occurred. `resizeIfNeeded()` reallocates when free capacity drops below
`minEntrySliceFreeSize` (64 entries). This avoids pathological reallocation
patterns where small truncations followed by new appends cause repeated
growth (O28 optimization).

### Term Resolution

`raftLog.term(index)` checks inmem first (O(1) index arithmetic), then
falls back to `LogReader.Term()`. The inmem layer also caches a single
`(appliedTo, appliedToTerm)` pair for lookups at the truncation boundary.

### LogDB Fallback on Recovery

After a node restarts, the inmem buffer is empty. `firstIndex()` and
`lastIndex()` fall back to `LogReader.IndexRange()` to determine log bounds
from persistent LogDB state. This is critical for correct term lookups
during recovery before the first log append.

## 15. LeaseRead

Lease-based linearizable reads (PhD 6.4) provide fast reads without
heartbeat latency when a valid lease exists. Lease reads skip the
heartbeat quorum round, improving latency from ~5ms (ReadIndex) to
sub-millisecond.

### Lease Validity

The leader maintains a lease if it has recently heard from a quorum.
Lease timeout is set to `ElectionRTT - 2 ticks` (default election timeout
minus 2), providing a safety margin. Requires:

1. `Config.LeaseRead = true` (opt-in).
2. `Config.CheckQuorum = true` (leader must periodically verify quorum).
3. Monotonic system clocks (no clock skew).

### Flow

```
Host.LeaseRead(ctx, shardID, query)
  |
  +-- check raft lease is valid
  |     |
  |     +-- leader && lease_ticks < lease_timeout? --> skip ReadIndex
  |     +-- else: fall back to ReadIndex
  |
  +-- if lease valid: SM.Lookup immediately (no wait)
  +-- if lease invalid: ReadIndex + wait + SM.Lookup (standard path)
```

### Failure Mode

If the leader's lease expires (no quorum replies within `leaseTimeout`),
it automatically falls back to the standard ReadIndex flow. Callers are
transparent to the failure mode.

## 16. Client Sessions

Sessions implement at-most-once delivery semantics (PhD Figure 6.1) for
exactly-once state machine semantics.

### Session Registration Through Raft

Sessions are registered and unregistered via Raft consensus, not locally.
When a client creates a new session, an entry with type `SessionRegister`
is proposed. Entry types use reserved series IDs:

- `SeriesIDForRegister = math.MaxUint64 - 1` (register entry)
- `SeriesIDForUnregister = math.MaxUint64` (unregister entry)

Entry helper methods query type: `IsNewSessionRequest()`,
`IsEndOfSessionRequest()`, `IsSessionManaged()`.

### Deterministic Index-Based Expiry

Sessions expire based on applied index, not wall-clock time. The
`rsm.Apply` layer maintains a `session.Manager` that tracks registered
sessions and their last-active index. On each apply tick, the manager
scans sessions and expires those where `(currentAppliedIndex - lastActiveIndex) > maxSessionLeaseIndex`.

This ensures all replicas make identical expiry decisions regardless of
clock skew, preventing duplicate response cache inconsistencies.

### Non-Deterministic Session Expiry Fix

When multiple sessions reach their expiry index simultaneously, they are
sorted by clientID before invoking the `ExpireSessions` callback. This
ensures deterministic expiry order across all replicas.

### Response Caching

The `session.Manager` tracks up to `maxResponses` completed operations
per session, keyed by (seriesID, clientID). On proposal, the engine checks
the response cache before applying to the state machine. If found,
the cached response is returned without re-execution.

Responding clients use (seriesID, clientID, commandID) tuples to correlate
responses. The response cache enforces a hard limit via
`RecordResult(...) error`, returning `ErrResponseLimitExceeded` when full
to force the client to issue an unregister before registering a new session.

### Session Proposal Flow

```
Propose(sessionID, clientID, seriesID, cmd)
  |
  +-- check session cache: (seriesID, clientID) -> cached response?
  |     |
  |     +-- yes: return cached response immediately
  |     +-- no: append entry with sessionID, proceed to apply
  |
Apply entry
  |
  +-- SM.Update(cmd)
  +-- recordResponse(sessionID, seriesID, clientID, result)
  |
  +-- response cache returns ErrResponseLimitExceeded at maxResponses
```

## 17. Error Handling

All errors are pre-allocated typed values (`errors.go`). The hot path
performs zero-allocation error returns. Key errors:

| Error                   | Condition                                            |
|-------------------------|------------------------------------------------------|
| `ErrCompacted`          | Requested entries removed by log compaction           |
| `ErrUnavailable`        | Index beyond last entry                               |
| `ErrTransferInProgress` | Proposal rejected during leader transfer              |
| `ErrPendingConfigChange`| Second config change proposed before first commits    |
| `ErrReadIndexNotReady`  | Leader has not committed entry in current term        |
| `ErrRemoveLastVoter`    | Attempt to remove the last voting member              |
| `ErrNodeRemoved`        | Attempt to re-add a removed node                      |
| `ErrInvalidOperation`   | Invalid operation for node type (e.g., witness ReadIndex) |
| `ErrResponseLimitExceeded` | Session response cache full                        |

The internal `notLeaderError` (`errors.go:103`) carries `LeaderID` and
`LeaderAddress` for client redirect. The Host layer translates it to a
public error type.

## 18. DrainFreeOrderMessages and Message Ordering

During the `commitPending` tick, when flushing raft-generated messages,
the engine distinguishes between ordered and free-order messages:

**Ordered messages** (preserved): `VoteResp`, `HeartbeatResp`. These
must be delivered in order to maintain election safety and heartbeat
liveness. Dropping or reordering them can cause stalled elections or
leader timeouts.

**Free-order messages** (drained): `Replicate`, `Snapshot`, `ReadIndexResp`,
etc. These can be batched or reordered without safety impact.

When the step worker is blocked (e.g., during LogDB persist), only
free-order messages are drained to keep election and heartbeat responses
flowing. This prevents voting deadlock where a candidate times out waiting
for its own `VoteResp` to be delivered.
