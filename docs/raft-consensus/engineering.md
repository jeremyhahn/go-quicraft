# Raft Consensus — Engineering Contract

> **Status: NORMATIVE** · Scope: go-quicraft `pkg/internal/raft`,
> `pkg/internal/engine`, `pkg/internal/queue`, `pkg/internal/server`,
> `pkg/internal/invariant` · Last verified: 2026-08-19
> Conflicts with this document are defects. Fix them here first.

These packages are **internal** and not importable from outside the module.
This document is therefore the implementation contract for work inside this
repository; the surface an embedder drives it through is
[`../multi-group-host/api.md`](../multi-group-host/api.md).

Design rationale, rejected alternatives and the scope fence are in
[`architecture.md`](architecture.md). Membership changes have their own folder,
[`../membership-changes/`](../membership-changes/architecture.md); snapshots
have [`../snapshots/`](../snapshots/architecture.md).


This document describes the Raft consensus protocol as implemented in
QuicRaft. All code lives under `pkg/internal/raft/`. References to the
extended Raft paper (Ongaro & Ousterhout, 2014) are noted as "Paper X.Y"
and references to Ongaro's dissertation as "PhD X.Y". Both papers are in
`docs/references/`.

Source files:

    pkg/internal/raft/raft.go        Core state machine
    pkg/internal/raft/peer.go        Engine-to-raft boundary
    pkg/internal/raft/log.go         Composite log (inmem + LogDB)
    pkg/internal/raft/inmemory.go    In-memory entry buffer
    pkg/internal/raft/remote.go      Per-follower replication tracker
    pkg/internal/raft/inflights.go   Sliding-window flow control
    pkg/internal/raft/membership.go  Cluster membership management
    pkg/internal/raft/bootstrap.go   Config-in-log bootstrap entry
    pkg/internal/raft/quiesce.go     Quiesce mode
    pkg/internal/raft/compress.go    Snappy entry payload compression
    pkg/internal/raft/util.go        Quorum math
    pkg/internal/raft/errors.go      Typed error definitions

Code is cited by file and symbol rather than by line number, because line
numbers rot faster than the doc is revised and a stale one is worse than none.

## Safety Properties

The five Raft safety properties and where each is enforced:

1. **Election Safety** (at most one leader per term). One vote per term
   (`becomeFollower` clears `vote` only on a term change), the up-to-date
   check in `handleVoteRequest`, and a single quorum taken over one voting
   set in `tallyVotes`/`countVotes`.
2. **Leader Append-Only**. `raftLog.append` asserts
   `entries[0].Index > committed`; a leader never truncates its own log.
3. **Log Matching**. `raftLog.tryAppend` verifies
   `matchTerm(prevLogIndex, prevLogTerm)` before any mutation and rejects
   non-contiguous batches.
4. **Leader Completeness**. The up-to-date check plus the unified quorum
   (Section 6): every election-winning coalition intersects every commit
   quorum in a replica holding the entry.
5. **State Machine Safety**. `raftLog.appliedTo` asserts the processed marker
   is monotonic and never exceeds `committed`; committed entries are never
   truncated, so no index is ever applied twice with different content.

## 1. Overview

The `raft` struct (`raft.go`) is a deterministic state machine. It sends and
receives no network messages and writes nothing to disk; it reads the log
through the `LogReader` interface. Messages flow in via `step()`, messages flow
out via `drainMessages()`. The engine layer (through `Peer`) persists state,
sends network messages, and drives the clock.

```
Engine ──Tick/Handle/Propose──> Peer ──step()──> raft
                                                   |
                                          msgs []proto.Message
                                                   |
Engine <──GetUpdate/Commit──── Peer <──drainMessages()
```

The `raft` struct is owned by exactly one goroutine at a time and uses no
mutexes. A few `Peer` fields are readable from other goroutines (Section 13);
the pair callers reach for most is `leaderID` and `term`, both `atomic.Uint64`,
written together by `syncLeaderID()` under a seqlock (`leaderSeq`) so
`Peer.LeaderAndTerm()` returns a combination that actually held. Loading the
two words separately can observe one that never did, which would report that a
replica led in a term it never led.

### Function Pointer Dispatch

Role-specific behavior is dispatched through two function pointers set during
role transitions:

```go
stepFunc func(r *raft, msg proto.Message) error
tickFunc func(r *raft)
```

The concrete functions are `stepFollower`/`stepCandidate`/`stepLeader` and
`tickFollower`/`tickCandidate`/`tickLeader`. A pre-candidate uses
`stepCandidate` and `tickCandidate`; the two are distinguished by `r.state`.

### Double-Buffered Message Output

Outbound messages accumulate in `r.msgs`. `drainMessages()` swaps `msgs` and
`msgsBuf` to return the current batch and prepare a fresh buffer, avoiding an
allocation on every drain cycle. `drainFreeOrderMessages()` and
`withholdOrderedMessages()` are the two partial drains; see Section 18.

## 2. Role Transitions

Four roles are defined (`raft.go`, `roleState`):

    follower(0)   candidate(1)   preCandidate(2)   leader(3)

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
                   | loses quorum / higher term / role mismatch
                   v
              [Follower]
```

### becomeFollower

- Sets `stepFunc = stepFollower`, `tickFunc = tickFollower`.
- Clears `vote` ONLY when the term changes. Preserving the vote within the
  same term prevents double-voting (Paper Figure 2).
- Resets `transferTarget`, `transferElapsed`, `pendingReadIndex`,
  `readIndexBroadcastPending`, `pendingConfigChange`.
- Sets `remoteLastAck = nil`, invalidating the read lease on the role change.
- Randomizes the election timeout and zeroes `electionTick`.

`stepDownToFollower` is the candidate-side wrapper. It clamps the term to
`max(msg.Term, r.term)` because a locally injected message carries no term:
passing `msg.Term == 0` straight through set a candidate's term to 0 and
cleared its vote, after which it granted votes again in terms it had already
voted in. That is reachable in production whenever a follower catching up by
snapshot has its election timer fire mid-transfer.

### becomeCandidate

- Asserts the caller is not already leader.
- Increments `term` unless `r.term >= maxAdoptableTerm`, votes for self.
- Resets the vote map and records the self-vote.

### becomePreCandidate

- Does NOT increment term or change vote (PhD 9.6).
- Uses `stepCandidate`/`tickCandidate`, with `r.state == preCandidate`.
- Records the self-vote in the vote map.

### becomeLeader

`becomeLeader` returns an error and can decline the promotion.

1. Refuses with `ErrTooManyVotingMembers` when `votingBitCount()` exceeds
   `MaxVotingMembers` (64). The check runs FIRST, before any field is written,
   so a refused promotion leaves the replica exactly as it was.
   `checkElectionResult` then falls back to follower at this term.
2. Re-arms `pendingConfigChange` from `hasConfigChangeToApply()`, which covers
   config changes inherited uncommitted from the previous leader (PhD 4.3).
3. Resets every remote (voters, observers, witnesses) to probe with
   `match = 0`, `next = lastIndex + 1`, and **`active = false`**. Clearing the
   activity flag matters because `remote` structs survive leadership changes: a
   node that led at term T, stepped down, and regains leadership at T+k would
   otherwise open its first CheckQuorum window with flags earned during a
   previous tenure and survive an extra election timeout with no contact at all.
4. Rebuilds `replicaToBit` for ReadIndex ack tracking.
5. Appends a no-op entry at the current term to establish commit authority over
   entries from prior terms (Paper 5.4.2).
6. Calls `tryCommit()` (handles single-node shards) and `broadcastAppend()`.

### Derived Campaign Gates

Four latches derived from the replicated configuration suppress campaigning.
None of them is ever proposed; each is re-evaluated everywhere the
configuration is (re)established (construction, snapshot restore, commit-time
config change).

- **`selfRemoved`** — this replica is in none of the configuration's voter,
  observer or witness sets. The surviving configuration computes its quorum
  without it, so a removed replica counting its own vote against its own stale
  voting set can win a term the real configuration also elects in (PhD 4.2.2).
  An empty configuration counts as "not a member".
- **`roleMismatch`** — the locally configured witness or observer role
  contradicts the replicated membership (`checkSelfRole`). The two roles run
  different data paths: the leader strips entry payloads for a replica the
  configuration calls a witness, so a replica started with `IsWitness=false`
  applies stripped entries into a full state machine while every local gate
  believes it is a full replica. The observer half is worse: `startElection`
  gates on the LOCAL `isObserver` while `quorum()` sizes the voting set from
  the replicated membership plus an unconditional +1 for self, so a replica
  the configuration lists as an observer but configured locally as a voter
  computes `quorumOf(V+1)` over a real voting set of V, and two such replicas
  can be elected in the same term. The latch refuses campaigning and read
  serving; voting is deliberately still allowed, because a replica that stopped
  voting would remove itself from a quorum the rest of the cluster still counts
  it in. A leader that learns of the mismatch steps down immediately.
- **`joining`** — a replica started with `join=true` holds a known-incomplete
  membership and would win with a quorum of one. It gates CAMPAIGNING ONLY: the
  election clock keeps ticking, because freezing `electionTick` left `leaderID`
  permanently "active", which made `handleVoteRequest` deny every PreVote and
  `dropHighTermVote` discard every higher-term RequestVote. A joining node that
  caught up by pure log replay then silently withheld its vote forever and
  could hold a healthy quorum leaderless.
- **`staleConfigThrough`** — see "Restart and the Stale Configuration Tail"
  below.

Every refusing branch in `startElection` sets `leaderID = noLeader` before
returning. `tickFollower` has already zeroed `electionTick` on the way in, so
leaving `leaderID` set keeps the shared leader-lease test
(`leaderID != noLeader && electionTick < electionTimeout`) true for most of
each tick cycle, and this replica would deny PreVotes and drop higher-term
RequestVotes while not campaigning itself.

### Restart and the Stale Configuration Tail

Membership is config-in-log (Section 9) and is reconstructed asynchronously
after a restart. A restarting replica recovers only a BASE configuration: the
snapshot record's membership, or the `EntryInitialMembership` at index 1. It
learns every change above that base by re-applying committed entries through
the ordinary pipeline. It is live and ticking throughout that replay, so its
election timer can fire while its voting set is still the superseded one. A
replica restarted against a 3-voter base while the committed log says 5 wins on
2 votes, in a term the real 5-voter configuration can elect in as well.

`Peer.staleConfigThrough` records the commit index the replica recovered with,
armed at `NewPeer` when `r.log.committed > ms.ConfigChangeID`. It is cleared by
whichever of these happens first:

- the ordinary commit pipeline reaching it (`processConfigChanges`), the free
  and overwhelmingly common case;
- `replayConfigChangesTo` covering it (`Peer.SetProcessedIndex`, for an on-disk
  state machine whose durable marker outruns the snapshot);
- `resolveStaleStartupConfig` folding the remainder ON DEMAND, which is what
  the campaign gates call through `raft.configCurrentForCampaign`.

`Peer.configFoldedThrough` is the separate marker the on-demand fold resumes
from. It is NOT the processed marker: `SetProcessedIndex` advances `processed`
unconditionally and then discards the report of whether the accompanying fold
reached the end, so resuming from `processed` declared an unread window empty.

Both campaign entry points call the resolver, because `handleTimeoutNow`
bypasses `startElection` entirely. Returning false suppresses the campaign for
THIS timeout only.

This gate is deliberately NOT `hasConfigChangeToApply`, and deliberately not a
"wait until the state machine catches up" gate. Blocking elections on
UNCOMMITTED log content is a liveness violation the specification calls out:
if a leader crashes after replicating an uncommitted config change to a
majority, no follower in that majority could ever start an election. Tying
campaigning to apply progress has the same shape and cannot self-clear on a
replica whose apply pipeline is not running at all. An earlier `replayFloor`
approach was removed for exactly this reason.

## 3. Elections

### PreVote Protocol (PhD 9.6)

When `preVote` is enabled a node first enters the `preCandidate` state.
PreVote requests use `term + 1` as the prospective term without incrementing
the node's term (`campaign`). This prevents a partitioned node that kept
incrementing its term from forcing the cluster to advance when it rejoins.

### Election Flow

```
startElection()
  |
  +-- gates: isObserver / isWitness / selfRemoved / stale config /
  |          roleMismatch / joining / unreadable last log term
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

Observers and witnesses never start elections.

`campaign` sends to `remotes` and `witnesses` only, never to observers. It
refuses to send anything at all when `log.lastTerm()` is unreadable: a vote
request advertising `LogTerm 0` is rejected by every peer holding a non-empty
log, so the round cannot be won. `startElection` and `handleTimeoutNow` check
the same condition BEFORE `becomeCandidate`, because by then the term has
already been incremented and `send()` stamps `r.term` on every reply, so the
raised term reaches the live leader through ordinary Replicate/Heartbeat
rejections and deposes it. One replica with a transient storage fault would
otherwise escalate into cluster-wide leader churn while emitting no election
traffic of its own.

### Vote Granting (`handleVoteRequest`)

Four gates run before the grant decision.

1. **Log up-to-date** (Paper 5.4.1): the candidate's last log entry must have a
   higher term, or the same term with an equal-or-higher index.

   ```go
   func (l *raftLog) isUpToDate(lastIndex, lastTerm uint64) (upToDate, readable bool)
   ```

   The second return value separates "this candidate is behind us" from "we
   cannot tell". Both deny, but the second is a storage fault on THIS replica
   that denies every candidate for as long as it lasts; on a majority it makes
   the shard unelectable with quorum apparently intact. It is counted in
   `unreadableLogVoteDenials`, surfaced by `Peer.DiagRaftState`, and warned
   about at most once per 10 seconds. Failing open here (fabricating term 0)
   would grant to a candidate missing entries this replica has committed.

2. **Candidate is in the configuration**: a vote is never granted to a replica
   this configuration does not contain. Once the receive path is a
   removed-node deny-list rather than a member allow-list (Section 9), refusing
   the VOTE is what stops a replica that is not part of this shard from winning
   a term. The test is skipped while `membershipKnown()` is false or while this
   replica is `joining`, because an incomplete configuration is not evidence of
   non-membership and denying from that state would strand a healthy shard.
   hashicorp/raft applies the same test under the same guard.

3. **Candidate is not a non-voting observer**: gate 2 is "known in any role",
   which is right for witnesses and wrong for observers. Without this, an
   observer whose local config says `IsObserver=false` collects grants from
   real voters, counts its own vote, and can be elected. This is the
   independent second half of `checkSelfRole`: it holds even if the observer's
   own latch is bypassed, because every voter enforces it.

4. **Vote availability**:
   - Real vote: `logOK && (vote == msg.From || (vote == noNode && noCompetingLeader))`,
     where `noCompetingLeader` is `leaderID == noLeader || msg.Hint == msg.From`.
     The known-leader clause is not optional (etcd/raft `canVote`, Paper 5.2,
     PhD p.42). A follower can reach term T without voting, by learning T from
     the leader's Replicate or Heartbeat, which clears `vote` while setting
     `leaderID`. A stale candidate campaigning at that same T is not filtered by
     `dropHighTermVote`, so without the clause the follower gratuitously
     unseats a healthy leader. Granting also sets `electionTick = 0` while
     `leaderID` still names the old leader, which arms `dropHighTermVote` and
     makes the follower discard every higher-term RequestVote for a full
     election timeout.
   - PreVote: `logOK && !leaderActive && msg.Term > r.term`, where
     `leaderActive` is `leaderID != noLeader && electionTick < electionTimeout`.
     The `msg.Term > r.term` test closes `msg.Term == r.term`: a replica that
     already voted in T would otherwise grant a pre-vote for that decided term,
     and the real-vote round would deny every grant. Safety is not at stake
     (one vote per term is enforced on the real-vote path); wasted elections
     are.

   Leadership-transfer votes (`Hint == From`) are exempt from the known-leader
   clause, because the outgoing leader asked for the election.

A granted PreVote response echoes `msg.Term` (the prospective term) so the
pre-candidate counts it at the term it is campaigning for. A REJECTED PreVote
response carries the responder's own `r.term`. Echoing the prospective term on
a rejection is unsafe: `step` treats a rejected `RequestPreVoteResp` with
`msg.Term > r.term` as term-advancing, so the pre-candidate would step down at
T+1 and then force the legitimate leader down through its stale-term reply,
which is the disruption PreVote exists to prevent.

When granting a real vote the election tick is reset to zero, extending the
current leader's implicit lease window.

### CheckQuorum Vote Dropping

`dropHighTermVote` drops a higher-term `RequestVote` while this node heard from
a live leader within the last election timeout (Paper §6 last paragraph, PhD
p.42). Transfer-hinted votes are exempt, but only at exactly `r.term + 1`.
`Hint` is a value the SENDER chooses, and RequestVote is not bound to committed
membership on the receive path (a member that recovered at a new address has to
be able to campaign during quorum loss), so unbounded, a forged
`RequestVote{Hint: From, Term: r.term + 1000}` walks past the lease and steps
the live leader down repeatably. A legitimate transferee campaigns at leader
term + 1 and therefore arrives at exactly `r.term + 1`; a peer far enough
behind for a larger gap has by definition not heard from the leader within the
election timeout, so the lease clause is already false.

This removes the amplification, not the disruption: a plain RequestVote still
forces a term advance whenever no leader lease is live.

### Term Ceiling

`maxAdoptableTerm = math.MaxUint64 - 1024` is an exclusive upper bound on a
term this replica will adopt from the wire (`ErrTermCeilingExceeded`), with the
same bound repeated in `becomeCandidate` where the increment happens. Nothing
bounds `Term` in the wire encoding, and adopting one near the ceiling is
unrecoverable in two ways: `becomeCandidate` does `term++`, so a node parked at
the maximum WRAPS to term 0 on its next campaign, after which it can vote a
second time in a term it already voted in; and even without the wrap, a node
driven to the ceiling can never campaign again, so one message permanently
removes a voter. Terms advance by one per election, so no cluster reaches this
range by running.

### Randomized Election Timeout

The election timeout is randomized in `[electionTimeout, 2*electionTimeout)`
using a `math/rand` PRNG seeded from `crypto/rand`
(`newRaft`/`resetElectionTimeout`):

```go
r.randomizedElectionTimeout = r.electionTimeout + r.rng.Intn(r.electionTimeout)
```

### Quorum Calculation

`tallyVotes` counts grants and rejections over the CURRENT configuration's
voting set. The election is decided early when `granted >= quorum` or
`rejected > total - quorum`.

```go
func quorum(remotes, witnesses map[uint64]*remote) int {
    return quorumOf(numVotingMembers(remotes, witnesses))
}

func quorumOf(n int) int { return n/2 + 1 }

func numVotingMembers(remotes, witnesses map[uint64]*remote) int {
    return len(remotes) + len(witnesses) + 1  // +1 for self
}
```

Both the quorum and the tally are taken over the same configuration, so a
mid-election membership change cannot produce a win on fewer votes than that
configuration requires:

- `countVotes` ranges the configuration's voting set and looks each member up
  in `r.votes`, rather than ranging `r.votes` directly, so a grant from a
  replica no longer in the configuration is not counted. Self is tallied only
  while `!selfRemoved`.
- `handleVoteResp` discards responses from replicas outside the voting set
  (`isVotingMember`).
- `purgeDepartedVotes` drops vote records whenever the configuration is
  reshaped, because `r.votes` is otherwise cleared only on the next campaign
  and a shrinking voting set lowers the quorum while stale grants remain
  countable.

Both full replicas and witnesses vote in elections. This is the same unified
`quorum` used for commit advancement (Section 6), for ReadIndex confirmation
(Section 8), for `hasQuorumActive`, and for the read lease (Section 15).

## 4. Log Replication

### sendAppend

Called per-follower to send `Replicate` (AppendEntries) messages. In Replicate
state it PIPELINES: after each send `optimisticUpdate` advances `rm.next`, so
the loop continues while more entries exist and the inflight window has
capacity. This reduces catch-up from O(N/batch) round-trips to
O(N/batch/inflights). In Probe state exactly one message is sent.

1. Skip if `rm.isPaused()` (probe already sent, snapshot in progress, or
   inflights full).
2. Look up `prevLogTerm` at `rm.next - 1`. If unavailable (compacted), fall
   back to `sendSnapshot`.
3. Fetch entries from `rm.next` to `lastIndex + 1`, bounded by
   `maxReplicationPayload` bytes and then by `limitEntryCount`. If unavailable,
   fall back to `sendSnapshot`.
4. An empty entries slice is still sent in Probe state: the follower uses
   `prevLogIndex`/`prevLogTerm` to detect divergence and returns a hint. In
   Replicate state an empty slice means the peer is caught up and nothing is
   sent.
5. Send `Replicate` with `LogIndex` (prevLogIndex), `LogTerm`, entries, and the
   current `committed` index.
6. In Replicate state: advance `rm.next` optimistically and record the last
   entry index in inflights, then loop.
7. In Probe state: set `probeSent = true` to pause further sends.

### handleAppend

Follower processing of AppendEntries:

1. Reset the election tick, record the leader ID, and `recordActivity()`.
2. If `msg.LogIndex < committed` the entries are stale. Answer immediately with
   `ReplicateResp{LogIndex: committed}` and return. This keeps `tryAppend` from
   ever seeing a conflict at or below the committed index.
3. Call `log.tryAppend(msg.LogIndex, msg.LogTerm, msg.Entries)`.
4. On success: advance commit to `min(msg.Commit, lastNewIdx)` and respond with
   `ReplicateResp` carrying the last index covered by the append.
5. On failure: respond with `Reject: true` and a backtracking hint (below).

### Rejection Hints and Term-Based Backtracking (Paper 5.3)

The follower builds one of two hint shapes:

- `msg.LogIndex > lastIndex` (the follower's log is shorter):
  `Hint = lastIndex`, `LogTerm = 0`.
- term mismatch at `msg.LogIndex`: `LogTerm = conflictTerm` and
  `Hint = firstIndexOfTerm(conflictTerm)`, so the leader can skip the entire
  conflicting term in one round-trip. If the term at `msg.LogIndex` cannot be
  read, it falls back to `Hint = msg.LogIndex`, `LogTerm = 0`.

`raft.declineRemote` consumes it on the leader:

- `hintTerm == 0`: delegate to `remote.declineFrom` (index-based).
- Replicate state: fall back to `match + 1`, which is known safe. A rejection
  at or below `match` is stale and ignored.
- Probe/Snapshot state: a rejection whose index is not `rm.next - 1` is stale
  and ignored. Otherwise, if the leader holds entries at `hintTerm`, set
  `next = lastIndexOfTerm(hintTerm) + 1`; if it holds none, set
  `next = min(hintIndex, rejectedIndex)`.

The clamp on that last branch is load-bearing. The hint is peer-supplied and is
only ever meaningful as a BACKTRACK. Log matching guarantees an honest follower
can never name a higher index, but unclamped, a follower replying
`Hint: 1<<62` drives `next` past the end of the leader's log; `sendAppend`'s
term lookup then fails forever and falls through to `sendSnapshot`, which on a
shard that has never compacted returns without touching the remote. No message
is ever sent to that peer again, so no rejection can arrive to backtrack it:
the replica removes itself from replication permanently while the leader still
reports it healthy. `remote.declineFrom` already applied the identical clamp.

### handleAppendResp

Leader processing of AppendEntries response:

1. Mark the remote active.
2. On **reject**: `declineRemote(rm, msg.LogIndex, msg.Hint, msg.LogTerm)`. If
   the next index actually moved, fall back to probe from replicate state and
   retry immediately with `sendAppend` (or `sendWitnessAppend` for a witness).
3. Refuse a match index beyond the leader's own log. A follower only ever
   echoes an index the leader sent it, so a higher one cannot be real, and
   accepting it would push `rm.match` past `lastIndex` and let `tryCommit`
   advance the commit index over entries that do not exist.
4. On **success**: `rm.update(msg.LogIndex)` and `rm.respondedTo()`. If the
   match advanced and `tryCommit()` succeeds, `broadcastAppend()`. If the
   remote is the leader transfer target and now caught up, send `TimeoutNow`.
5. Free inflights up to `msg.LogIndex`. If the window has capacity, send more.

The commit broadcast in step 4 does NOT reach a follower that is fully caught
up in `remoteReplicate`, because `sendAppend` returns early when there is
nothing to send for such a peer. Those followers learn the new commit index
from the next heartbeat or the next real append. That early return is a
deliberate volume tradeoff; etcd/raft makes the opposite choice.

### tryAppend

The log matching check (Paper 5.3). It verifies
`matchTerm(prevLogIndex, prevLogTerm)`, then rejects a non-contiguous batch
with `ErrGappedEntryBatch` before any mutation (the offset arithmetic and
`inMemory.merge` both assume `ents[i].Index == prevLogIndex+1+i`, and a gapped
batch would corrupt the in-memory log). It then finds the first conflicting
entry via `getConflictIndex` and appends from the conflict point; already
matching prefix entries are preserved.

**Safety property**: `getConflictIndex` skips entries at or below the committed
index (Paper 5.1 guarantees committed entries match across all servers). This
prevents falsely identifying committed entries as conflicts when the same batch
drain advances the committed index partway through processing multiple
`Replicate` messages. A compacted index is skipped for the same reason; only
`ErrUnavailable` (past the end of the log) is reported as the append point.

### Replication Chunking

`sendAppend` bounds each `Replicate` message two ways.

**Bytes.** `maxReplicationPayload` is set from `Config.MaxApplyEntrySize`,
whose default is `config.MaxReplicationPayloadBytes` = 16,711,672 bytes
(~15.94 MiB). `Config.Validate` refuses any larger value, so the field cannot
legally exceed the replication bound.

```go
entries, err := r.log.entries(rm.next, r.log.lastIndex()+1, r.maxReplicationPayload)
```

`raftLog.entries(low, high, maxSize)` returns entries from `[low, high)` up to
`maxSize` total bytes, always returning at least one entry.

The default is derived from the transport rather than chosen for roundness:
`MaxReplicationPayloadBytes` is `transport.MaxFrameSize` minus frame and batch
framing overhead, so a single Replicate message fits in one frame. A batch that
does not is rejected at marshal time and DROPPED, and because the follower's
next index never advances, raft rebuilds the identical oversized batch forever.
The follower can never catch up and there is no snapshot fallback, since the
entries still exist. A value of 0 means unlimited and is not usable in
production for the same reason.

**Count.** `limitEntryCount` caps a message at `proto.MaxEntriesPerMessage`
(10000). The byte bound alone is not enough: a follower catching up on small
entries produces a message that exceeds the wire's entry-count limit, every
peer's decoder rejects it with `proto.ErrTooManyMessages` and resets the
stream, and the leader rebuilds the identical message on the next cycle. The
count bound has to be applied wherever the byte bound is. Truncating costs
nothing because `sendAppend` pipelines.

The follower acknowledges each batch with `ReplicateResp`, the leader advances
`rm.next` and sends again, so catch-up streams in bounded increments with no
explicit chunking state.

### Witness Replication Behavior

Witnesses are voting members that do not store full log entries. During
`broadcastAppend` the leader sends `Replicate` to remotes and observers, and
metadata-only `Replicate` to witnesses via `sendWitnessAppend`:

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

`sendWitnessAppend` is `sendAppend` with `makeMetadataEntries` applied: each
entry is replaced by `proto.Entry{Index, Term, Type: EntryMetadata}` with no
`Cmd`. Config-change entries and the bootstrap `EntryInitialMembership` are
preserved intact, because a witness must apply membership changes to hold the
correct configuration, and the bootstrap entry is how a witness learns the
initial configuration at all. Witness `match` is tracked from these
metadata-only acks and counts toward the unified quorum (Section 6).

Observers receive the same full `Replicate` messages as voters. There is no
proactive catch-up mechanism for a lagging observer: `sendAppend` falls back to
`sendSnapshot` once the leader's log is compacted past the observer's next
index. Observers are non-voting, so their lag affects neither commit safety nor
election safety. Operators tune `CompactionOverhead` to retain enough history
for observer replication.

### Entry Compression

When `Config.EntryCompressionType == config.Snappy`, `handleLeaderPropose`
compresses `EntryNormal` payloads with `CompressEntry` before append. Config
changes are never compressed, so they stay readable regardless of a replica's
compression configuration. The compressed flag rides in the entry type
(`proto.EntryCompressFlag`), which is why every type test in this package masks
with `proto.EntryTypeMask`.

## 5. Remote State Machine

Each peer tracked by the leader has a `remote` struct (`remote.go`) with three
states:

```
                     +---------- success -----------+
                     |                              |
  [Probe] ----respondedTo()----> [Replicate] -------+
    ^  ^                              |
    |  |               unreachable / reject / stalled window
    |  |                              |
    |  +--------- becomeProbe --------+
    |
    +-- follower ack (respondedTo, match >= snapshotIndex)
    |   or countdown expiry (tickSnapshotWait)
    |
  [Snapshot] <--- sendSnapshot --- [Probe/Replicate]
```

**Probe**: one message in flight, gated by `probeSent`. The leader waits for
the response to discover the follower's position.

**Replicate**: sliding window of up to `maxInflightMsgs` (256) entries in
flight. `rm.next` advances optimistically on send.

**Snapshot**: a snapshot is being transferred or awaiting the follower's
raft-level acknowledgment. `isPaused()` is unconditionally true for the whole
phase. `becomeSnapshot` arms a backstop countdown of `snapshotTransferTicks`
(60) so a peer cannot sit paused forever if the transfer's status report never
arrives at all; a real status report overwrites that value rather than adding
to it. The transfer outcome report (`SnapshotStatus`) does NOT un-pause the
peer either; it re-arms the countdown. The peer exits to probe via the
follower's `ReplicateResp` ack (`respondedTo` with `match >= snapshotIndex`) or
countdown expiry. See Section 7, "Snapshot Status and the Post-Transfer Wait".

`becomeProbe` from snapshot state sets `next = max(match+1, snapshotIndex+1)`,
since the peer holds the snapshot data through `snapshotIndex`.

### Inflights (`inflights.go`)

Circular buffer of in-flight entry indices, lazily allocated on the first
`add()`. `freeTo(index)` frees all entries at or below the given value.
`full()` returns true when `count >= size`.

The window therefore drains only from ABOVE, which a peer RESTART can make
impossible: the outstanding messages went to the peer's previous incarnation,
and every ack the returning peer can send reports a position BELOW the window,
so it frees nothing. Since a full window is what makes `isPaused` true, the
peer would never be sent another entry. `handleHeartbeatResp` breaks that cycle
with `freeFirstOne()`, which releases the oldest slot without its ack: the
heartbeat response proves the peer is alive, so exactly one message is let
through, acked normally if the window was merely busy and rejected if the peer
restarted, and the rejection drives `declineFrom` back to `match + 1`. This
mirrors etcd/raft's `MsgHeartbeatResp` handling. `handleHeartbeatResp` records
that it released a slot (`forceSend`) so the send is not skipped for a ReadIndex
heartbeat; under sustained ReadIndex load every heartbeat carries a context, so
a `Hint == 0` response may never arrive and the released slot would be leaked
window capacity. The stall is not detectable from leader state alone: a busy
peer and a wedged one have the same match, next and window shape.

`handleHeartbeatResp` also repairs the other stall shape: `remoteReplicate`
with `next > lastIndex` and `inflights > 0` means the leader has nothing more
to send yet entries remain unacknowledged, which happens when both the
Replicate messages and the follower's rejections were dropped. That falls back
to probe.

### declineFrom

Index-based backtracking, used when the follower supplied no term hint:

- **Replicate state**: fall back to `match + 1` (known safe).
- **Probe/Snapshot state**: `next = max(1, min(rejectedIndex, hintIndex + 1))`,
  matching the dragonboat reference's `decreaseTo(rejected, last)` formula.

Both forms return false for a stale rejection, which the caller reads as "do
not retry".

## 6. Commit Calculation

### tryCommit

Uses a pre-allocated sort buffer (`r.matched`) to find the median match:

```go
func (r *raft) tryCommit() bool {
    r.matched = r.matched[:0]
    // The leader's own match is its DURABLE index, not lastIndex(): an
    // entry it has appended but not yet fsynced must not count toward the
    // commit quorum, or a leader crash could lose an entry it had already
    // reported committed.
    r.matched = append(r.matched, r.log.inmem.savedTo)
    for _, rm := range r.remotes {
        r.matched = append(r.matched, rm.match)
    }
    for _, wm := range r.witnesses {                  // unified quorum:
        r.matched = append(r.matched, wm.match)       // witness metadata
    }                                                 // acks count too
    slices.Sort(r.matched)
    q := quorum(r.remotes, r.witnesses)
    mci := r.matched[len(r.matched)-q]
    return r.log.tryCommit(mci, r.term)
}
```

The `N-quorum`-th element from the sorted match array is the highest index
replicated to a quorum. `log.tryCommit` additionally verifies the entry at that
index has the leader's current term (Paper 5.4.2: leaders only commit entries
from their own term).

Because the leader's own contribution is its DURABLE index, a commit can be
blocked purely on local durability. That case is re-evaluated at the
persistence edge: `Peer.Commit` calls `tryCommit()` again once `savedTo` has
advanced and broadcasts if the commit index moved. This mirrors etcd's
async-storage-writes design.

A follower advances its commit index through the same term check.
`handleHeartbeat` calls `log.tryCommit(min(msg.Commit, lastIndex), r.term)`
rather than a bare `commitTo`; see Section 7's heartbeat notes for why.

### Unified Quorum (Election == Commit)

```go
// Single quorum over the full voting set (remotes + witnesses + self),
// used for BOTH elections and commit advancement.
func quorum(remotes, witnesses) int {
    return (len(remotes) + len(witnesses) + 1) / 2 + 1
}
```

Witnesses vote in elections, so they **must** also be counted toward the commit
quorum. `tryCommit` includes each witness's `match` (tracked via the
metadata-only Replicate acks of `sendWitnessAppend`) in the sorted match array
alongside the full replicas and the leader's own durable index.

**Why a unified quorum is required (safety).** If the commit quorum excluded
witnesses (computing it over data nodes only) while the election quorum
included them, the two quorums would be taken over different member sets and
need not intersect in any node holding a committed entry. A commit could then
land on a data-node minority, and a candidate that never saw that entry could
win election on witness votes and truncate it, violating **Leader
Completeness** and silently losing committed data. Counting witnesses in both
quorums restores quorum intersection. This matches the dragonboat reference,
whose single `quorum()` spans `remotes + witnesses`.

**Why witness metadata is sufficient for safety.** A witness stores each
entry's `(Index, Term)` but not its `Cmd`. That is enough to gate commit via
its `match` index and to enforce the up-to-date check during elections, so a
candidate that never saw a committed entry can never win.

**Residual trade-off: durability under permanent storage loss.** A commit may
rest on a `{leader + witnesses}` quorum with its payload on the leader alone.
Committed data survives any minority of *crash-restart* failures: a full
replica holding the entry is required to win any subsequent election, so the
entry is at worst briefly unavailable until such a replica returns. But if
every full replica holding an entry suffers *permanent* storage loss (the
leader's disk is destroyed while the other full replicas lag), the entry is
unrecoverable, witnesses hold no payload, and the shard stalls: no surviving
replica can pass the witnesses' up-to-date vote check, and recovery requires
operator intervention (snapshot import at an earlier index). This is inherent
to the witness design and matches the dragonboat reference.

### Witness Policy Is Replicated State

`WitnessConfig.RequireFullReplicaMajority` (default true) bounds membership
*shape*: it rejects any witness-add or full-replica-removal that would leave
the full replicas unable to form a quorum on their own. It does **not** prevent
an individual commit from resting on `{leader + witnesses}` when the other full
replicas lag; it only bounds how witness-dependent the membership can become.
Set it false for witness-heavy deployments that knowingly accept the exposure.
`WitnessConfig.MaxWitnesses` caps witness fan-out (0 = no cap).

The policy lives in `proto.Membership` (log, snapshots, WAL) alongside the
member maps. It is seeded at shard bootstrap from `config.WitnessConfig` and
changes only via an `UpdateWitnessPolicy` config change (type 4) committed
through Raft (`Host.RequestUpdateWitnessPolicy` /
`SyncRequestUpdateWitnessPolicy`). Every replica therefore enforces the same
policy regardless of local configuration, and the policy survives restarts from
the replicated state.

A policy update must be satisfied by the CURRENT membership at validate time
(tightening a cap below the current witness count is rejected with
`ErrTooManyWitnesses`); reshape first, then tighten. Apply is unconditional so
an already-committed policy change replays identically on every replica.

Enforcement happens at validate-time gates only, never on the deterministic
apply path:

- `Peer.ValidateConfigChange`, called by the Host membership API against the
  published membership snapshot, so a caller gets a prompt error instead of a
  hung `RequestState`.
- `Peer.ProposeConfigChange`, against the mutable step-worker-owned membership.
- `raft.handleLeaderPropose` -> `validateProposedConfigChange`, the leader's
  last gate before log append. `NewPeer` wires `raft.validateConfigChange` to
  `Peer.validateProposedConfigChange`, so this runs the FULL
  `membership.validate`. It closes the follower-forward path, where a wire
  `Propose` reaches the leader having run none of the other two.
  `raft.validateConfigChangePolicy` is the fallback for a bare `raft` with no
  membership installed (the unit tests and the adversarial harness); it
  enforces the subset of rules raft can answer from its own remote/witness
  tracking, and is deliberately not run alongside the full validator.

## 7. Snapshots

### Message Admission: wire vs in-process injection

`raft.step` runs four admission gates before any role dispatch, in a fixed
order. Each gate's placement is a safety property.

**Process-local types from the wire.** `InstallSnapshot`, `SnapshotStatus`,
`Unreachable` and `LeaderTransfer` are refused at ANY term when the origin is
remote (`ErrWireLocalMessage`). None of them is ever exchanged between
replicas: a snapshot travels as a `SnapshotHeader` plus chunks
(`stepWorker.sendMessages` routes `InstallSnapshot` to `sender.SendSnapshot`),
and the other three are reports about work this host itself performed.

**Zero term.** Six message types carry no term, because nothing in this process
produces one for them: `Propose` and `ReadIndex` (built by the engine's
batching), `InstallSnapshot` (built by the snapshot receive path after the
artifact is persisted and verified), `SnapshotStatus` and `Unreachable`
(transport-local notifications), and `LeaderTransfer` (the host's admin API).
`Term == 0` is accepted only for a LOCAL-origin message of one of those six
types (`ErrZeroTermMessage` otherwise). Every type produced by `send()` carries
`r.term`, so a zero term on one of those would bypass every term comparison: a
`Replicate` could advance a follower's commit index over its uncommitted tail, a
vote response could escape the stale-term filter, and a `Heartbeat` could
install a leader at any term. `Propose` and `ReadIndex` are deliberately still
accepted from the wire WITH a term: followers forward both to the leader, and
`send()` stamps the sender's term on the forward.

**Term ceiling.** See Section 3.

**CheckQuorum vote dropping.** See Section 3.

The exemption belongs to the DELIVERY PATH, not to the message type. All six
decode off the wire (`proto.Message.UnmarshalFrom` accepts any known type), the
transport hands whole batches to `hostMessageHandler.HandleMessage` with no
type filter, and the handler delivers into the SAME per-shard inbox the
in-process injectors use.

`proto.Message.Origin` (`proto.MessageOrigin`) makes the two distinguishable.
It is process-local: `MarshalTo` never writes it, and `UnmarshalFrom` and
`Reset` force it to `OriginRemote`, so no peer can express it and a pooled
message cannot leak a stale stamp. `Node.Deliver` / `Engine.DeliverMessage`
(the wire path) stamp `OriginRemote`; `Node.DeliverLocal` /
`Engine.DeliverLocal` (used by `deliverSnapshotControl` and the host's
`LeaderTransfer`) stamp `OriginLocal`. The step worker's deferred-snapshot
buffer re-injects through `Node.redeliver`, which preserves whatever the
message already carried.

`Peer.Handle` repeats the origin check at the membership-adoption site, because
that is the code that replaces the set `quorum()` is computed over. A wire
`InstallSnapshot` carries a full `Membership` and its own `ConfigChangeID`,
both chosen by the sender, with no term, no log match and no committed entry
behind them; adopting one lets any authenticated shard member redefine the
voting set of every replica it can reach.

### Snapshot Membership Persistence

Snapshots persist full membership information, including the config-change ID,
the witness policy, and all members (voting, observer, witness).
`LogDB.ReadState(shardID, replicaID)` returns that membership alongside the
hard state, so recovery restores the cluster topology without requiring
external configuration. It is the BASE configuration a restart starts from; the
committed tail above it is folded separately (Section 2, "Restart and the Stale
Configuration Tail").

### handleSnapshot

Processes `InstallSnapshot` on a follower (Paper Figure 13).

1. **SIZE gate.** Refuse with `ErrTooManyVotingMembers` when the snapshot's
   membership names more voting members than the ReadIndex `ackBits` word can
   address (`snapshotVotingBitCount > maxVotingMembers`). This is the one place
   a configuration enters the shard without having passed an admission check:
   proposals are bounded by the propose-time validators, bootstrap and import
   by the Host's `MaxVotingMembers` checks, and a snapshot's membership by
   nothing between the wire and `rebuildRemotesFromMembership` (proto decodes
   up to `MaxMembershipEntries`). Accepting one installed the whole map and
   returned success, and the damage surfaced later as `rebuildReplicaToBit`
   failing the next time this replica won an election. It is a REJECTION rather
   than a clamp because dropping members to make the configuration fit computes
   quorum over a set no committed entry put there, which is a split-brain, not
   a degradation.
2. **SENDER gate.** Refuse with `ErrSnapshotNotFromLeader` when
   `leaderID != noLeader && msg.From != leaderID`. This is the same gate
   `handleHeartbeat` applies, applied to the strictly more powerful message: a
   heartbeat carries no log-matching proof and may move the commit index, while
   an `InstallSnapshot` carries no proof either and may REPLACE the entire log
   and rebuild the voting set from a membership the sender chose. The gate has
   to come first rather than only guarding the accepting branches, because the
   two unconditional writes in step 3 run even on the reject path: before it,
   any authenticated shard member could hold a healthy follower's election
   timer at zero indefinitely, and install itself as that follower's
   `leaderID`, simply by retransmitting one long-obsolete snapshot. Suppressing
   an election is not a lesser power than winning one. The term dimension is
   covered upstream: `step` drops a lower-term `InstallSnapshot` and, for a
   higher term, calls `becomeFollower(msg.Term, msg.From)`.
3. Reset `electionTick` and record `leaderID = msg.From`.
4. Reject if `ss.Index <= committed` (the snapshot is stale), answering with
   `ReplicateResp{LogIndex: committed}`.
5. **Log matching optimization**: if the existing log has an entry at
   `ss.Index` with the same term **and no snapshot is pending persistence**,
   advance the commit index (`commitTo`) and the processed marker
   (`appliedTo`). Entries following the snapshot point are retained, and
   remotes are NOT rebuilt, because retained entries may carry config changes
   newer than the snapshot's membership.

   Advancing `processed` here is a deliberate divergence from dragonboat and
   etcd, which advance only `committed`. It is what makes the
   no-pending-snapshot precondition load-bearing rather than cosmetic: a
   pending snapshot is the one `GetUpdate` has yet to hand to the engine, and
   it is cleared only by the engine's persistence ack. Taking this branch while
   one is pending would fast-forward both markers past a snapshot the engine
   has never seen, so the next `GetUpdate` would emit the now-superseded
   pending snapshot, rewinding the `LogReader` range behind the apply frontier
   (or failing the shard outright in `Peer.Commit`, where `ApplySnapshot`
   rejects it with `logdb.ErrSnapshotOutOfDate`), and silently skipping apply
   for every entry in
   `(pending.Index, ss.Index]`.

   The precondition is safe to enforce by falling through to `restore`: a
   pending snapshot is always at or below `committed`, and step 4 guarantees
   `ss.Index > committed`, so the incoming snapshot is strictly newer.
   Restoring keeps the pending snapshot monotonic. The entries discarded above
   `ss.Index` were never persisted or acked (`Peer.Commit` persists entries and
   clears the pending snapshot in the same cycle), and the `ReplicateResp`
   carries `ss.Index`, so the leader re-sends them.
6. Otherwise call `log.restore(ss)`, which discards all entries, sets
   `committed = processed = ss.Index`, and clears the pending config-change
   bound; then `rebuildRemotesFromMembership(ss.Membership)`.
7. Respond with `ReplicateResp` carrying the snapshot index.

`handleSnapshot` is idempotent for an already-applied snapshot: step 4 rejects
it and answers with the committed index.

### Snapshot Sending

When `sendAppend` cannot resolve `prevLogTerm` or entries (the log is
compacted), it calls `sendSnapshot`:

1. Get the latest snapshot from `log.snapshot()` (inmem first, then the
   LogReader's persisted snapshot). If there is none, log a warning and call
   `rm.becomeProbe()`. Returning without touching the remote used to leave
   `next` wherever the caller had put it, including past the end of the
   leader's log, where `term()` fails on every retry and nothing is ever sent
   to that peer again. `becomeProbe` re-seats `next` at `match + 1`, which the
   leader can always read.
2. Increment `r.snapshotSeq` and transition the remote to `remoteSnapshot`
   (paused) with that sequence.
3. Send `InstallSnapshot` with `Hint = snapshotSeq`. For a witness target the
   file data is stripped (`Filepath = ""`, `FileSize = 0`); a witness needs only
   Index, Term and Membership. The empty `Filepath` IS the statement that the
   transfer carries no payload.

The engine layer handles snapshot chunking for large snapshots, reassembling
chunks on disk before calling `LogDB.SaveSnapshot`. See
`docs/engine/pipeline.md`.

### Snapshot Status and the Post-Transfer Wait

The transport reports the transfer outcome to the leader's raft layer as a
`SnapshotStatus` message. `Hint` echoes the transfer sequence the leader
stamped on the `InstallSnapshot` (`raft.snapshotSeq`, monotonic per raft
instance, transport-local and never on the wire to the follower).
`handleSnapshotStatus` discards a status whose sequence does not match the
remote's current `snapshotSeq`: a late report from a superseded attempt,
including a re-send of the SAME snapshot index, must not affect a newer
in-flight one. Delivery is retried briefly on a transiently full inbox
(`deliverSnapshotControl`, 5 attempts at 100ms). The status is one-shot, and
losing it would leave the peer paused with only the `snapshotTransferTicks`
backstop.

Neither outcome un-pauses the peer directly. Both set the `snapshotWaitTicks`
countdown, decremented once per leader tick by `tickSnapshotWaits`, which runs
BEFORE any quiesce early-return since a snapshot cycle can run entirely under
quiesce:

- **Success** arms `snapshotAckWaitMultiplier` (3) election timeouts. Transfer
  completion only means the bytes arrived; the follower may still be
  reassembling, persisting, and processing the snapshot. Probing in that window
  draws a rejection whose hint backtracks `next` into compacted territory,
  re-streaming the entire snapshot redundantly. The peer normally exits earlier
  via its own `ReplicateResp{LogIndex: ss.Index}` ack (`update` +
  `respondedTo` with `match >= snapshotIndex`, which goes to probe).
- **Failure** arms one election timeout as a retry backoff, so a persistently
  failing transfer (unreachable peer, full receiver disk) cannot re-stream full
  snapshots back-to-back.

Countdown expiry falls back to probe, resuming the normal retry path including
a fresh snapshot send. On the receiver, a re-sent snapshot that is already
persisted is deduplicated: persistence is skipped, but the `InstallSnapshot`
raft message is re-delivered from LogDB (`HandleSnapshot` dedup branch), so a
follower whose original install delivery was dropped still advances and acks.

This mirrors the dragonboat reference's wait state and delayed snapshot ack.

### rebuildRemotesFromMembership

After a snapshot restore the peer set may have changed. This rebuilds
`remotes`, `observers` and `witnesses` from the snapshot's membership with
fresh `remote` structs, mirrors the snapshot's replicated witness policy into
`maxWitnesses`/`requireFullReplicaMajority`, re-derives `selfRemoved` and
`roleMismatch` (a snapshot can carry a configuration this replica is no longer
in), and calls `purgeDepartedVotes`.

### Snapshot Recovery vs Apply Race

The engine's per-node `recovering` atomic flag gates **state machine access
only**. When a snapshot is being restored:

1. The snapshot pool raises `recovering` before calling `RecoverFromSnapshot`.
2. The step worker keeps draining the node's inbox and stepping messages, but
   calls `GetUpdate` with `moreEntriesToApply = false`, so no committed entry
   is extracted for apply. The processed marker does not advance, so those
   entries are re-offered once the gate clears. Client work (proposals and
   ReadIndex) stays queued, which lets the queues apply their own backpressure
   (`ErrSystemBusy`).
3. The apply worker parks before touching the state machine, and the commit
   worker skips piggyback extraction (handing the apply worker a batch would
   park it holding `commitPending`).
4. `InstallSnapshot` is deferred via `stepWorker.deferredSnapshots` and
   re-delivered afterwards. Handling one mid-recovery would move the log's
   snapshot floor out from under the recovery and leave a permanent apply gap.
5. The pool clears `recovering` and notifies the work signal.

This prevents the state machine from applying entries that exist in both the
snapshot and the log.

**The gate deliberately does NOT suspend protocol participation.** Nothing in
`Handle(RequestVote)` or `Handle(Heartbeat)` touches the state machine; only
`GetUpdate`'s committed-entry extraction does. An earlier implementation
skipped the node entirely, including its inbox drain, so a recovering replica
answered no votes and no heartbeats while its tick kept campaigning into a
void. Because `RecoverFromSnapshot` runs arbitrary state-machine code with no
time bound, two concurrent recoveries could dissolve quorum with every node up:
a 5-replica test with 2 recovering and the leader killed elected no leader for
20 seconds. A replica that cannot apply must still vote.

Recovery is visible in `ShardInfo.Health` as `recovering` (and `snapshotting`
during a save) rather than `healthy`, and through `Host.DiagShardGates`.

## 8. ReadIndex

Linearizable reads without log writes (PhD 6.4, Figure 6.4). The leader
confirms it still holds authority via a heartbeat quorum round.

### Batch Coalescing

Multiple concurrent ReadIndex requests are coalesced into a single heartbeat
round. Each node has a dedicated MPSC `readIndexQ`, separate from the proposal
inbox. On each ready cycle the step worker drains ALL pending ReadIndex keys at
once (`drainAndHandleReadIndex`) and submits one `Peer.Handle(ReadIndex)` with
the first key as `batchKey`; the rest are registered as aliases through
`OnReadIndexBatched` BEFORE the submit, so a single-node shard that resolves
immediately cannot complete the batch key before its aliases exist. All keys
complete when the `batchKey` heartbeat receives quorum confirmation. This
reduces heartbeat overhead from O(N) per caller to O(1) for N concurrent
callers.

Within the raft layer, `handleLeaderReadIndex` defers the heartbeat broadcast
by setting `readIndexBroadcastPending`; `Peer.FlushReadIndex` sends one
broadcast with the latest ctx after all inbox messages are processed.

### Flow

```
Client ---ReadIndex(key)---> Host.ReadIndex()
                              |
                        append to readIndexQ (MPSC, batched drain)
                              |
Step Worker (per ready cycle)
  |
  +-- drain ALL keys from readIndexQ
  +-- register aliases, submit one ReadIndex msg with first key as batchKey
  |
  +-- Peer.Handle(ReadIndex msg) -> raft leader/follower handler
  |
  +-- leader checks roleMismatch, hasCommittedEntryAtCurrentTerm(),
  |     transferTarget
  |     |
  |     +-- single voting member, or valid lease? --> respond immediately
  |     +-- else: append to pendingReadIndex[], defer broadcast
  |
  +-- Peer.FlushReadIndex() -> broadcastHeartbeatCtx(readIndexCtx)
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

**PhD 6.4 guard** (`hasCommittedEntryAtCurrentTerm`): the leader must have
committed at least one entry from its own term (the no-op) before serving
reads, otherwise the committed index may not reflect the true commit state.
Rejected with `ErrReadIndexNotReady`.

**Leader transfer guard** (PhD 6.4.1): `ErrTransferInProgress` while a transfer
is active. The old leader must not serve reads because the transfer target may
begin committing writes at any moment.

**Role-mismatch guard**: `ErrWitnessRoleMismatch` on both the leader and
follower paths. A replica the configuration lists as a witness has been
receiving metadata-only entries, so its state machine never saw the data the
read would return. The leader-side check is needed in addition to the step-down
in `checkSelfRole` because a ReadIndex already in the same inbox batch would
otherwise still be answered.

**Heartbeat context correlation**: each ReadIndex batch gets a unique
`readIndexCtx`, carried in the heartbeat `Hint` field and echoed back in
responses, so concurrent batches are tracked independently.

**Context reservation**: `readIndexCtx` is pre-incremented before use, so the
first ReadIndex ctx is 1 (and wraps to 1, never 0). Context 0 is reserved for
regular heartbeats (`broadcastHeartbeat` passes ctx=0), which prevents regular
heartbeat responses from falsely satisfying a ReadIndex quorum check.

**Unissued context refusal**: `handleHeartbeatResp` refuses a response whose
`Hint` exceeds `r.readIndexCtx` (`ErrReadIndexContextNotIssued`). The `<=`
matching rule below is sound only for a context this leader ISSUED:
`becomeLeader` zeroes the counter, `handleLeaderReadIndex` is the only writer
and only increments, so the set of contexts this leader's heartbeats can have
carried is exactly `{0} ∪ [1, readIndexCtx]`. A response echoing anything above
that bound would ack EVERY pending read from one message, including reads whose
confirming heartbeat has not been broadcast yet, and with the leader's pre-set
self-ack that is already a quorum of two.

**Batch confirmation**: a heartbeat response confirms all pending entries with
`ctx <= Hint`, because ctx is monotonically increasing within a term. Entries
are appended in ctx order, so the scan breaks at the first entry above the
response context.

**Out-of-order resolution**: resolved requests are compacted from
`pendingReadIndex` regardless of position, so a slow heartbeat response for
batch N does not block batch N+1 that already has quorum.

**Follower forwarding** (`handleFollowerReadIndex`): followers forward ReadIndex
requests to `r.leaderID`. When the leader's quorum confirms, it emits
`ReadIndexResp` back to the originating follower, identified by `pending.from`.
`handleReadIndexResp` refuses a response from anyone but `r.leaderID`
(`ErrReadIndexRespNotFromLeader`): the correlation key is a plain monotonic
counter (`Host.readKeySeq`) and is therefore guessable, so a response from
another sender would serve a read at an index of the sender's choosing with no
quorum ever run. It deliberately does NOT check `msg.LogIndex` against the
local committed index, because the leader answers with ITS committed index,
which is by construction at or ahead of the follower's; that gap is the point
of ReadIndex, and the follower waits for `lastApplied` to reach the returned
index before serving.

**Bitfield-based ack tracking**: each `readIndexStatus` tracks heartbeat acks
with a `uint64` bitfield instead of a map. Replica IDs are mapped to bit
positions 0..63 via `raft.replicaToBit`, built in `becomeLeader` and rebuilt by
`rebuildReplicaToBit` on every membership change during leadership. This gives
zero-allocation ack tracking on the hot path and is the reason for the hard
64-voting-member cap (Section 9).

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

A `rebuildReplicaToBit` re-bases every in-flight entry's `ackBits` to self-only
and arms a fresh confirmation round. Their acks were recorded under the OLD
mapping, so a replica whose position moved could ack again into a fresh bit and
be counted twice, letting a sub-quorum set satisfy the check.

**ReadIndex quorum**: `checkReadIndexQuorum` uses the unified `quorum` (remotes
+ witnesses + self). Witness heartbeat acks count toward confirmation;
`rebuildReplicaToBit` assigns witnesses ack bits and `handleHeartbeatResp`
records their acks. Since witnesses vote in elections, the confirmation quorum
must span the same voting set so it overlaps with every possible
election-winning coalition.

**Bounded pending state**: `maxPendingReadIndex` (4096) caps in-flight requests
awaiting confirmation and `maxReadStates` (4096) caps resolved results not yet
drained; both reject with `ErrReadIndexOverloaded`. A resolved local request
that cannot be added to `readStates` is RETAINED as pending rather than
dropped, because a caller waiting without a deadline would otherwise hang
forever.

**Witnesses cannot serve ReadIndex**: `Host.ReadIndex` refuses on a witness
with `ErrInvalidOperation` before any raft work happens. Witnesses lack the
full log needed to provide linearizable reads. Their acks still count toward
another replica's confirmation quorum.

### End-to-End Host Pipeline

The raft-level flow produces `readStates` on the raft struct. These flow
through the engine and Host layers to complete the caller's request:

```
Host.ReadIndex(shardID)
  |
  +-- reject if this replica is a witness (ErrInvalidOperation)
  +-- generate readKey (Host.readKeySeq, monotonic uint64)
  +-- create pendingRead{rs: RequestState} in ns.pendingReads map
  +-- enqueue readKey on the node's readIndexQ (ErrSystemBusy if full)
  +-- wake step worker
  |
Step Worker (step.go)
  |
  +-- Peer.Handle(ReadIndex msg) -> raft ReadIndex handler
  +-- Peer.FlushReadIndex()
  +-- Peer.GetUpdate() -> update.ReadyToRead = raft.readStates
  +-- callback.OnReadyToRead(shardID, update.ReadyToRead)
  |     |
  |     +-- match readKey in ns.pendingReads
  |     +-- set pr.readIndex, pr.confirmed = true (and for every alias)
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

The `pendingRead` struct (`host.go`) tracks the batch:

```go
type pendingRead struct {
    rs        *RequestState
    readIndex uint64   // set when OnReadyToRead confirms
    confirmed bool     // true once readIndex is known
    aliases   []uint64 // other readKeys that share this ReadIndex result
}
```

A read completes in one of two places:

1. **OnReadyToRead**: if `lastApplied >= readIndex` when the heartbeat quorum
   confirms, the read completes immediately. The `lastApplied` load happens
   under `pendingReadsMu` so it is consistent with the map state.
2. **OnApplied**: otherwise the read stays pending, and each `OnApplied`
   callback scans confirmed pending reads and completes those whose
   `readIndex` has been reached.

### SyncRead

`Host.SyncRead(ctx, shardID, query)` wraps the asynchronous pipeline into a
synchronous linearizable read:

1. Call `Host.ReadIndex()` to get a `RequestState`.
2. Wait for it to resolve (`lastApplied >= readIndex`).
3. Execute `SM.Lookup(query)` on the now-caught-up state machine.

`Host.SyncReadBuf` is the zero-copy byte-slice variant.

### ReadOnly Bypass Optimization

ReadIndex-only updates carry no entries, state changes, or snapshots.
`Peer.GetUpdate` detects this and sets `update.ReadOnly`:

```go
update.ReadOnly = !stateChanged &&
    len(update.EntriesToSave) == 0 &&
    len(update.CommittedEntries) == 0 &&
    update.Snapshot.Index == 0
```

The step worker checks `ReadOnly` after invoking the callbacks, sends any
messages, and skips the commit/apply pipeline. This bypasses the commit worker
(LogDB persistence), the apply worker, and the `commitPending` blocking flag.
ReadIndex confirmations are delivered inline in the step worker via
`OnReadyToRead`, eliminating three goroutine hops.

`stateChanged` is forced true by `Peer.InvalidatePersistedState`, which the
engine calls when an extracted update is discarded without being persisted (a
panic between `GetUpdate` and the commit hand-off). Without it, `prevState`
would permanently claim a term and vote that never reached disk, the next
update would classify as ReadOnly, and a vote granted in the lost update would
be answered by a `RequestVoteResp` with no WAL write behind it.

With batch coalescing, ReadIndex latency on a healthy cluster is dominated by
one heartbeat round-trip: leader to followers to leader.

## 9. Membership Changes

### Config-in-Log Bootstrap

A shard's initial configuration is a single committed entry at index 1
(`proto.EntryInitialMembership`, built by `BuildInitialMembershipEntry`), not
an out-of-band seed. Membership is therefore a pure fold over the log, which is
the property the whole design turns on: a joining replica learns the full
configuration by ordinary replication rather than only from a snapshot that may
never arrive. `BootstrapMembershipIndex` is 1 and
`bootstrapMembershipTerm` is 1, so the first real proposal (and the new
leader's no-op) land at index 2 or above.

One entry carrying the full `Membership` is used rather than one ConfigChange
per member, so the bootstrap occupies exactly index 1 and perturbs the index
arithmetic the recovery clamps depend on as little as possible.
`proto.Membership` marshals its maps with sorted keys, so every founder
produces a byte-identical entry from identical input. That last qualifier is an
unenforced operator contract: `MaxWitnesses` and `RequireFullReplicaMajority`
come from each founder's local `Config.Witness`, so founders configured with
different witness policy (or different member addresses) produce byte-DIFFERENT
entries at the same `(index=1, term=1)`, AppendEntries matches on
`(index, term)` only, and the divergence is never reconciled.

`applyMembershipEntry` refuses an `EntryInitialMembership` at any index other
than 1. Applying one elsewhere would REPLACE the membership wholesale,
discarding every config change committed since bootstrap. The refusal is a pure
function of the entry's index, so every replica reaches it identically.

`ReconstructMembership` (`bootstrap.go`) is the recovery-time fold of the same
entries, and it makes the identical decisions, deliberately: a committed
config-carrying entry that does not decode is FATAL on both paths
(`ErrCorruptConfigEntry`), because skipping one reconstructs a membership that
differs from the rest of the shard. A partial fold is not a degraded
membership, it is a different one. A zero-length `EntryConfigChange` payload is
the silent no-op it has always been on both paths: every replica reads the same
nothing and skips identically.

### One-at-a-Time Configuration (PhD 4.1, Paper Section 6)

Only one config change may be in flight at a time. Attempting to propose a
second while one is uncommitted returns `ErrPendingConfigChange`. This prevents
two overlapping config change entries, which could produce two disjoint
majorities.

The authoritative test is `raftLog.confChangeIndex > raftLog.processed`
(`hasConfigChangeToApply`), and `r.pendingConfigChange` is a cached view of it
that `handleLeaderPropose` re-derives UNCONDITIONALLY on every proposal. The
guarded form (`if r.pendingConfigChange`) could only ever relax the flag, never
restore it: `Peer.Commit` clears it for every committed `EntryConfigChange` it
applies without asking whether a LATER one is still unapplied, so a catch-up
batch containing an earlier change cleared it while one was genuinely
outstanding.

The frontier is `processed`, NOT `committed`. Membership takes effect at commit
time, but "commit time" here means `Peer.Commit -> processConfigChanges`, a
stage BEHIND `r.log.committed`: the log's committed index advances in the step
worker while the commit/apply cycle is still in flight, and `becomeLeader` runs
inside that window. A test bounded at `committed` would report "nothing
pending" for a config change that has committed but has not been folded into
`p.membership` yet, so a new leader would arm `pendingConfigChange = false`,
accept a second config change, and compute its commit quorum over the OLD
voting set while the log's newest configuration is a larger one. This is
etcd/raft's `applied >= pendingConfIndex` shape.

`confChangeIndex` is maintained incrementally by `raftLog.append`, which is the
single path every entry enters the log through (leader propose, follower append
via `tryAppend`, the leadership no-op). `newRaftLog` seeds it CONSERVATIVELY
with the recovered last index, because nothing in this process classified the
recovered entries; that blocks new config-change proposals on a just-restarted
leader until it has applied the tail it started with, and releases
automatically. It is also strictly safer than the O(N) log scan it replaced,
which could fail to READ, and a failed read had to be treated as "a change
might be pending".

### Config Change Takes Effect on Commit

Membership changes take effect when the config change entry COMMITS, not when
it is appended (the etcd/dragonboat model, diverging from PhD 4.1's append-time
rule). The single application point is
`Peer.Commit -> processConfigChanges -> applyMembershipEntry`: when an update's
`CommittedEntries` contain config-carrying entries, they are applied to the
membership, the immutable snapshot is republished, and `syncRaftRemotes()`
adds and removes remote trackers, identically on leaders and followers.

Commit-time application avoids the append-time trap where a just-added
(possibly not yet running) member immediately counts toward the quorum required
to commit its own addition, stalling commit and stepping the leader down via
CheckQuorum.

`Peer.ProposeConfigChange` does NOT apply the change to membership. It
validates, serializes to an `EntryConfigChange`, and steps a `Propose`; the
membership mutation happens later, on commit, on every replica alike.

### Log Truncation Needs No Membership Rollback

Because membership only mutates on COMMITTED entries and committed entries are
never truncated, conflict-driven log truncation cannot remove an applied config
change. No rollback or log-rescan machinery exists; an appended-but-uncommitted
config change that gets truncated was never applied in the first place.

### Dispatch Tables

Config change handling uses map-based dispatch (`membership.go`), with a
mutating handler table and a read-only validator table:

```go
var configChangeHandlers = map[uint64]configChangeHandler{
    proto.AddNode:             handleAddNode,
    proto.RemoveNode:          handleRemoveNode,
    proto.AddNonVoting:        handleAddNonVoting,
    proto.AddWitness:          handleAddWitness,
    proto.UpdateWitnessPolicy: handleUpdateWitnessPolicy,
}
```

`configChangeValidators` mirrors it one-for-one. An unknown type is
`ErrInvalidConfigChangeType`. Every type except `UpdateWitnessPolicy` must name
a non-zero target replica (`ErrZeroReplicaID`); `UpdateWitnessPolicy` targets
the shard.

The handlers update `addresses`, `observers`, `witnesses`, `removed`,
`identities`, `removedIdentities`, the two witness-policy fields, and
`configChangeID`.

### Replay Guard

`membership.apply(cc, logIndex)` skips any change at or below
`m.configChangeID`, which is the log index of the most recently applied config
change. This covers restart replay of committed entries on top of a
snapshot-seeded membership and a follower re-receiving entries. Re-applying
would be wrong for non-idempotent changes (a replayed `UpdateWitnessPolicy`
would revert a newer policy) and would regress `configChangeID`. The skip is
deterministic across replicas because `configChangeID` is itself replicated
state.

The initial membership's `ConfigChangeID` is left at 0, so a caller observes 0
until the first real membership change.

### Node Types

**Voters (Remotes)**: full replicas that replicate the entire log and
participate in elections and the commit quorum.

**Observers (Non-Voting Learners)**: replicate the full log but do not vote.
Used to bring new nodes up to speed before promotion. They receive no
RequestVote, get no ReadIndex ack bit, and are never counted in `quorum()`.

**Witnesses**: vote in elections and count toward the unified commit quorum,
but store only entry metadata (Index/Term, no command payload). See Section 6
for the durability trade-off.

### Per-Shard Authorization Is a Deny-List

Per-shard authorization is a removed-node TOMBSTONE deny-list (the
etcd/Consul model), never a member allow-list. `Peer.IsRemoved` and
`Peer.IsRemovedIdentity` answer it. Joining is therefore not an auth bypass: a
replica the configuration has simply not heard of yet is not refused, which is
what a joining node and a member that recovered at a new address both depend
on. The `joining` flag exists for the two decisions an incomplete configuration
cannot safely support (campaigning and the candidate test in
`handleVoteRequest`), not to bypass authorization.

`removedIdentities` is what makes the deny-list answerable against the
AUTHENTICATED peer identity rather than the claimed `msg.From`.
`handleRemoveNode` MOVES the departed replica's identity from `identities` to
`removedIdentities` rather than deleting it: a removed replica still holds its
private key, so it authenticates perfectly well and would evade its own
tombstone by naming any other replica ID. It is moved rather than left in place
because `identities` is the ACTIVE binding that `identityHolder` scans to
refuse duplicates, and leaving a departed replica's identity there would refuse
the legitimate operation of recycling a decommissioned machine into a new
replica ID. Nothing is ever evicted from `removedIdentities`.

### Safety Validations

Validate-time (`configChangeValidators`), evaluated on the leader before the
entry is proposed:

- Cannot remove the last full (non-witness) replica (`ErrRemoveLastFullReplica`).
- Cannot remove the last voting member (`ErrRemoveLastVoter`).
- Cannot add a previously removed node (`ErrNodeRemoved`).
- Cannot add a node that already exists (`ErrNodeAlreadyExists`). Observer-to-voter
  promotion is exempt on the `AddNode` path.
- Cannot remove a node that is not a member (`ErrNodeNotFound`).
- Reject a zero replica ID (`ErrZeroReplicaID`) or an empty address on an add
  (`ErrEmptyAddress`).
- Cannot exceed the 64-voting-member cap (`ErrTooManyVotingMembers`).
- Cannot give one ACTIVE address to two replicas (`ErrDuplicateMemberAddress`).
  The inbound authorization chain is replica ID -> committed address ->
  certificate, and two active replicas at one address collapse it: the single
  host behind the address is authorized to speak as BOTH, so it holds two votes
  and two match indices in a quorum that believes they are independent. That is
  quorum forgery reachable through the ordinary admin API, with no forged
  message anywhere. Comparison is on the whole address (two replicas on one
  machine at different ports are a supported deployment) and on the CANONICAL
  spelling (`netaddr.Canonical`), because the certificate match this chain ends
  in is done by `x509.VerifyHostname`, which already treats two spellings of
  one host as one. Removed replicas do not count, so recycling a departed
  replica's address is allowed.
- Cannot bind one identity to two active replicas
  (`ErrDuplicateMemberIdentity`), for the same reason and with no co-location
  exception: two replicas on one machine still need distinct keypairs.
- Cannot add an identity this shard has committed a REMOVAL for
  (`ErrRemovedMemberIdentity`). Without this the add commits and then every one
  of the new member's messages is dropped by `Peer.IsRemovedIdentity` on every
  peer: a voter counted in every quorum that can never be heard from.
- Witness policy violations (`ErrTooManyWitnesses`,
  `ErrBreaksFullReplicaMajority`). `validateRemoveNode` enforces the
  full-replica-majority rule too: without it, removing a full replica from a
  compliant `{3 full + 2 witnesses}` silently produces the witness-heavy
  `{2 full + 2 witnesses}` the policy exists to reject.

Apply-time (`configChangeHandlers`), the deterministic replicated-state-machine
transition:

- `ErrEmptyAddress`, `ErrNodeRemoved`, `ErrNodeNotFound`,
  `ErrRemoveLastFullReplica`, `ErrRemoveLastVoter`, and `ErrNodeAlreadyExists`
  for the two role transitions a bootstrap seed can never manufacture
  (`AddNode` over a witness, `AddWitness`/`AddNonVoting` over each other's
  role).

### The 64-Voting-Member Cap

`MaxVotingMembers = 64` exists because ReadIndex ack tracking uses a `uint64`
`ackBits` bitfield, mapping replica IDs to bit positions 0..63. A 65th voting
member would alias bit positions and let a sub-quorum satisfy a ReadIndex
check. It is enforced at every point a voting set can GROW, and each of those
points fails in a way the operator can act on:

- `Host.StartShard` and `ImportSnapshot` (admission). Bootstrap does not run
  the config-change validators, so a 65-member map used to be committed as the
  index-1 entry and then killed every future election on that shard,
  durably, across restarts.
- `validateAddNode` / `validateAddWitness` / `validateConfigChangePolicy`
  (propose time).
- `handleSnapshot` (the snapshot membership gate, Section 7).
- `newRaft`, which refuses to build a replica whose RECOVERED configuration
  does not fit, returning `ErrTooManyVotingMembers` so `StartShard` reports the
  error and the host stays up. This is the durable form of the fault and the
  one an operator cannot escape on their own.
- `Peer.syncRaftRemotes`, which checks BEFORE touching any raft map and returns
  the error out through `Peer.Commit`, so the engine's `failCommit` marks the
  shard failed and fires `OnShardFailed` while the host and its other shards
  keep running. Leaving the raft mirror on the last configuration that fit is
  what makes this fail closed.
- `becomeLeader`, which declines the promotion before writing any field.
- `rebuildReplicaToBit`, the backstop, which RETURNS `ErrTooManyVotingMembers`.
  It used to be an `invariant.Assert`, which panicked the raft STEP WORKER, a
  goroutine shared by every shard that worker owns, over a fault concerning
  exactly one shard, and did so again on every restart. Truncating the map to
  fit is not an option: bit positions above 63 would alias onto live ones.

**Apply deliberately does NOT re-check the cap.** `handleAddNode` and
`handleAddWitness` skip it by design. `membership.apply` runs on followers
replicating a committed entry and during recovery replay; rejecting an
already-committed config change at apply time would make that replica diverge
from the agreed membership.

### Convergent Apply

The same reasoning that removes the cap check from apply shapes the
duplicate-add checks. `handleAddNode`, `handleAddNonVoting` and
`handleAddWitness` place the target in exactly the role the committed entry
names, whatever role the membership currently records for it. They refuse only
a `Removed` tombstone and the two role transitions the seed can never
manufacture.

This is required because a membership is not always built purely from committed
entries. `Host.StartShard` seeds `Membership.Addresses` from the caller's
`members` map whenever recovery restored nothing, which is the ordinary state
of a caught-up replica that has never snapshotted, since membership is
persisted only alongside a snapshot record. That map is the operator's list of
replica IDs and records no roles, so **every witness and observer in it is
seeded as a full voter**, including the local replica (`join=false` requires
self to be listed). The committed `AddWitness` / `AddNonVoting` entries that
gave those replicas their real roles are still in the log, since config changes
are compacted only behind a snapshot, and are re-applied on restart. Refusing
them left the wrong roles in place permanently: nothing else repairs the seed,
and `configChangeID` never advanced past the skipped entry. `checkSelfRole` is
not a substitute; it is a latch rather than a repair, and it only reports on
the local replica.

Converging is not a weakening. When the membership was built from committed
entries alone, `validate` ran on the leader before the entry was proposed and
guaranteed the target was not a member, so the role moves are provably no-ops.
The refusals that remain are exactly those whose precondition holds identically
on every replica, or that the seed can only LOOSEN:

- The add refusals read `removed` and `isWitness`/`isObserver`, and the seed
  only ever populates `addresses`, so it can manufacture neither a tombstone
  nor a non-voter role.
- `handleRemoveNode`'s three refusals read `len(m.addresses)`, `isMember` and
  `isWitness`. Seeding every listed non-voter as a voter makes `isMember` more
  likely true, `len(m.addresses)` larger, and `isWitness` less likely true, so
  each refusal is strictly LESS likely to fire than on the true membership: a
  committed remove the true membership would apply is never refused.

`processConfigChanges` reports two outcomes differently:

- A **decode failure** returns `ErrCorruptConfigEntry` and is FATAL. The bytes
  are committed replicated state every peer can read and this replica cannot,
  so continuing means running on a configuration that differs from the rest of
  the shard, silently, and durably once the next snapshot persists it.
  `Peer.Commit` propagates it and the engine's `failCommit` turns it into a
  shard fault. Recovery already treats the identical bytes as fatal; runtime
  failing open while recovery failed closed was the inconsistency. Whatever was
  folded before the corrupt entry is published first, so the shard's last
  coherent configuration is what its readers see.
- A **deterministic apply refusal** is logged at WARN and skipped. Every
  replica evaluates it identically, so it is agreement, not corruption.
  Faulting on those would kill shards over an outcome the whole cluster agrees
  on. A skip is still final (nothing re-delivers a committed entry), which is
  why the Host membership API pre-validates through `ValidateConfigChange`: a
  skip on the leader's own freshly proposed entry would hang the caller's
  `RequestState`.

### Leader Self-Removal (Paper Section 6)

When a committed config change removes the leader from membership,
`Peer.checkLeaderSelfRemoval` transitions it to follower. It runs during
`Peer.Commit`, after `processConfigChanges`, once per committed
`EntryConfigChange` in the batch, so self-removal always sees the post-change
membership. `syncRaftRemotes` sets `selfRemoved` in the same pass, so the
stepped-down leader also stops campaigning.

## 10. Quiesce Mode

Quiesce reduces resource consumption for idle shards by suppressing normal
heartbeats and extending election timeouts. Idle means no proposals, config
changes, or reads for an extended period.

### Configuration Constants (`quiesce.go`)

    quiesceThresholdMultiplier         = 10   (threshold = 10 * electionTimeout ticks)
    quiesceLivenessMultiplier          = 10   (livenessInterval = 10 * electionTimeout)
    quiesceElectionTimeoutMultiplier   = 3    (follower timeout = 3 * livenessInterval)

### Entry and Exit

```
Normal Operation
  |
  +-- no activity for (electionTimeout * 10) ticks
  |
  +-- tryEnter() returns true
  |     |
  |     +-- leader: reset lease + electionTick, broadcastQuiesce() to all peers
  |     +-- follower: enters via Quiesce message from its own leader
  |
  v
Quiesce Mode
  |
  +-- leader: liveness probes every (electionTimeout * 10) ticks
  +-- leader: CheckQuorum still runs, paced by the liveness interval
  +-- leader: tickSnapshotWaits still runs
  +-- follower: election timeout of (livenessInterval * 3)
  |
  +-- any proposal, config change, read index, or inbound Replicate
  |     |
  |     +-- recordActivity() --> exit()
  |
  v
Normal Operation
```

**Leader tick**: `tickSnapshotWaits()` runs FIRST, before the quiesce
early-return, because a lagging follower's catch-up runs entirely on response
messages, none of which reset the quiesce idle counter, so a snapshot cycle can
start and finish under quiesce. Skipping the countdown would strand the
follower paused in snapshot state until the next proposal.

During quiesce the leader sends liveness probes (plain `broadcastHeartbeat`)
instead of the normal heartbeat cadence, and **CheckQuorum keeps running**,
paced by `livenessInterval` rather than `electionTimeout`. Freezing
`electionTick` there would freeze the read lease as well: acks recorded before
or during quiesce would never age, `hasValidLease` would compute a negative
expiry and accept all of them, and a partitioned leader would keep answering
ReadIndex from a stale committed index for the whole partition. Losing quorum
steps the leader down exactly as in the normal path.

Quiesce entry is BLOCKED while a leader transfer is active, because
`transferElapsed` would never increment and the transfer timer would stall
indefinitely, leaving the leader rejecting proposals with
`ErrTransferInProgress` until restarted.

**Follower tick**: uses `quiesceElectionTimeout()`. If the timer expires while
quiesced, the follower exits quiesce and starts an election.

**Quiesce is leader-driven.** `handleQuiesceMessage` enters quiesce only when
the local `quiesce.enabled` flag is set, this replica is a follower, and the
message came from its own `leaderID`. Quiesce is a local configuration choice,
not replicated state, so a peer cannot impose it: entering it switches
`tickFollower` to a 30x election timeout, which on a node that never opted in
turns a leader failure into a multi-second leaderless window. A follower does
NOT self-quiesce; `tickFollower` used to call `tryEnter()` on election timeout
and RETURN when it fired, suppressing exactly the election a leaderless
follower must run.

**Activity detection**: `raft.recordActivity()` resets the inactivity counter
and exits quiesce if active. Exiting also invalidates the read lease and zeroes
`electionTick`, because the acks that earned the lease were recorded against a
clock paced by the liveness interval, which says nothing about the followers'
election timers once normal ticking resumes. Activity is recorded in four
places:

1. **Follower proposal forwarding** (`stepFollower`): a follower that forwards
   a `Propose` to the leader exits quiesce, so its election timer runs at
   normal speed while client proposals are in flight.
2. **Leader proposals** (`handleLeaderPropose`), which covers config-change
   proposals since they arrive as `Propose` entries.
3. **Follower append** (`handleAppend`): inbound replication is client work
   reaching this replica.
4. **Leader ReadIndex** (`handleLeaderReadIndex`).

`exitQuiesce` is also called explicitly by `handleLeaderTransfer` and
`handleTimeoutNow`, so a transfer never runs on the quiesced clock.

## 11. Leader Transfer

### Flow

```
RequestLeaderTransfer(target)
  |
  handleLeaderTransfer()
  |
  +-- target == self? --> no-op
  +-- transfer already in progress? --> ErrTransferInProgress
  +-- exitQuiesce()
  +-- target not a voting remote?
  |     +-- witness or observer --> ErrTransferTargetNotVoter
  |     +-- otherwise            --> ErrNodeNotFound
  |
  +-- set transferTarget, clear remoteLastAck (expire the read lease)
  |
  +-- target caught up? (rm.match >= lastIndex)
  |     |
  |     +-- yes: sendTimeoutNow(target)
  |     +-- no:  probeSent = false, sendAppend(target) to catch up
  |
  +-- transferElapsed reaches electionTimeout
        |
        +-- cancel: transferTarget = noNode, restore lease tracking
```

The lease is expired at transfer start per PhD 6.4.1: the old leader must not
serve LeaseReads because the new leader may begin committing writes as soon as
it wins. Both the timeout path and `syncRaftRemotes` (when the target is
removed from membership mid-transfer) restore `remoteLastAck`, without which
the leader would silently degrade to heartbeat-based ReadIndex for the rest of
its term.

Proposals and ReadIndex are rejected with `ErrTransferInProgress` while a
transfer is active.

### TimeoutNow

The target campaigns IMMEDIATELY, bypassing PreVote, and its RequestVotes carry
`Hint = self` so peers with a fresh leader lease still grant them. Because it
is decided in a single round with no pre-vote to catch a stale voting set,
`handleTimeoutNow` repeats every campaign gate rather than delegating: observer
or witness or `selfRemoved`, the on-demand configuration fold
(`ErrStaleConfigCannotCampaign`), `selfRemoved` re-checked after the fold,
`roleMismatch` (`ErrWitnessRoleMismatch`), `joining`
(`ErrJoiningNodeCannotCampaign`), already-leader, and an unreadable last log
term (`ErrUnreadableLogCannotCampaign`). `Peer.Handle` additionally refuses a
delivered `TimeoutNow` on a joining node before it reaches `step`.

### Catch-Up Replication

If the target is not caught up the leader clears `probeSent` and sends an
append immediately. Each successful `handleAppendResp` re-checks whether the
target has reached `lastIndex` and sends `TimeoutNow` when it has.

## 12. Rate Limiting

Per-shard in-memory log growth is controlled by an `InMemRateLimiter`
configured at shard startup (`Config.MaxInMemLogSize`; nil when zero). A
separate cross-shard `RateLimiter` is shared by every shard on the host.

### Leader Side

- `rl.Tick()` and `rateLimited()` run every non-quiesced leader tick.
- `handleLeaderRateLimit` records follower in-memory sizes reported via
  `RateLimit` messages (`msg.Hint` carries the size).
- `rateLimited()` reports whether the threshold is exceeded.

### Follower Side

- `tickRateLimit()` runs every non-quiesced follower tick and sends the local
  in-memory log size to the leader every `electionTimeout` ticks via a
  `RateLimit` message.

### InMemory Tracking

The `inMemory` struct tracks both limiters:

- `rl`: the per-shard `InMemRateLimiter`.
- `globalRL`: the cross-shard `RateLimiter`, with `globalContribution`
  recording this shard's current share so the shared counter is adjusted by the
  delta rather than re-added.

Both are updated on `merge()` (entries appended) and `appliedLogTo()` (entries
truncated after apply).

Admission is asymmetric by design: `Host.proposeEntry` refuses with
`ErrSystemBusy` when the global limiter is engaged, while `Host.ReadIndex`
performs no rate-limit check at all, so reads are not blocked during write
backpressure. The only backpressure a read sees is a full `readIndexQ`.

## 13. Peer API and Concurrency Model

The `Peer` struct (`peer.go`) bridges the engine and the raft state machine. It
is owned by exactly one worker goroutine at a time:

1. **Step Worker**: `Tick()`, `Handle()`, `Propose()`, `ReadIndex()`,
   `FlushReadIndex()`, `GetUpdate()` in sequence each cycle.
2. **Commit Worker** (if entries need persist): `Commit()` after LogDB
   completes.
3. **Back to Step Worker**: next cycle.

The raft state machine has no internal locking. Synchronization is managed at
the engine layer by the per-node `raftMu` mutex together with atomic flags and
channels.

`raftMu` serializes `Peer.Tick()` against `Peer.Commit()`. `Tick()` runs
unconditionally, regardless of the `commitPending` flag, because the leader
must keep advancing its heartbeat timer while WAL fsync and apply are in
progress. It also covers the step worker's `commitPending` inbox processing and
config-resolution paths; several `Peer` methods document "the caller must hold
raftMu". Contention is minimal, since both `Tick()` and `Commit()` run at most
once per RTT interval.

`raftMu` is **not reentrant**. Event callbacks are invoked outside it: a
callback running under `raftMu` blocks the commit worker's lock acquisition for
every shard that worker owns.

The fields safe for concurrent reads without the mutex are `leaderID` and
`term`, both `atomic.Uint64` written together under the `leaderSeq` seqlock by
`syncLeaderID()`, plus `joining` (`atomic.Bool`) and `membershipSnap`
(`atomic.Pointer[membershipSnapshot]`).

### Core Methods

**Tick()**: advances the logical clock via `r.tickFunc(r)`, then syncs
`leaderID`/`term`.

**Handle(msg)**: routes an inbound message to `r.step(msg)`. It refuses any
`TimeoutNow` delivered through this path while `joining` is set, before
stepping. After stepping it is the
site that adopts a snapshot's membership. Adoption requires ALL of: message
type `InstallSnapshot`, `err == nil`, local origin, a non-stale message term,
and `r.log.committed > committedBefore` (the positive test that raft actually
TOOK the snapshot). That last test is what closes the two paths that return nil
without installing anything: `handleSnapshot` rejecting a snapshot at or below
the committed index, and `stepLeader` having no `InstallSnapshot` case at all.
Given adoption, membership is replaced when the log was restored, or the
replica is joining, or the snapshot's `ConfigChangeID` is strictly newer.

**Propose(entries)**: returns `NotLeaderError` on a non-leader rather than a
fabricated index, wraps the entries in a `Propose` message, and returns the
last assigned log index.

**ProposeConfigChange(cc)**: validates against the mutable membership,
serializes to an `EntryConfigChange`, and steps a `Propose`. It does NOT mutate
membership; that happens at commit.

**ValidateConfigChange(cc)**: validates against the atomically published
membership snapshot, so it is safe from any goroutine. Conservative: a config
change that commits concurrently may cause a benign false rejection.

**ReadIndex(key)** / **FlushReadIndex()** / **DrainReadStatesOnly()**: the
ReadIndex entry points; see Section 8.

**HasUpdate()**: reports whether any state needs processing (messages, entries
to apply, read states, pending snapshot, hard-state change, entries to save),
ordered cheapest-first. Its parameter is unused.

**GetUpdate(moreEntriesToApply, lastApplied)**: collects the current delta into
a `proto.Update` (entries to save, committed entries to apply, messages, read
states, pending snapshot, hard state), classifies it `ReadOnly`, and advances
`prevState`.

**GetPiggybackUpdate(lastApplied)**: extracts committed entries and messages
onto an empty-commit cycle, clamped to the durable marker (`savedIndex()`) in
both the entries and the advertised `HardState.Commit`, so a crash cannot leave
`HardState.Commit` ahead of the last durable log entry. While `entriesToSave()`
is non-empty it withholds ORDERED messages (`withholdOrderedMessages`), because
the piggyback path fsyncs hard state only and releasing a `ReplicateResp` for a
RAM-only entry is the Figure 2 violation "updated on stable storage before
responding to RPCs" exists to prevent. It does not advance `prevState`;
`NotePiggybackPersisted(hs)` does that once the state is on disk.

**RequeueMessages(msgs)**: returns drained messages to the FRONT of the
outbound buffer after the engine failed to establish the durability they were
waiting on. Dropping them is not equivalent: a lost `VoteResp` stalls an
election until the next timeout.

**Commit(update)**: acknowledges engine persistence, in this order:

1. `logReader.ApplySnapshot` and `logReader.Append`. These are the only
   fallible steps, and they run FIRST so an error leaves the batch fully
   re-deliverable (the processed marker has not moved). `Append` also updates
   the term cache, without which the leader's `sendAppend` cannot resolve
   `term(prevLogIndex)` for slow followers after inmem truncation.
2. `log.appliedTo(lastProcessed)`, advancing the processed marker. Everything
   from here on is infallible.
3. `processConfigChanges(update.CommittedEntries)` — the point where membership
   takes effect. A fatal decode error is returned here, past `appliedTo`, which
   is correct: retrying would decode the same bytes.
4. For each committed `EntryConfigChange`: clear `pendingConfigChange` and run
   `checkLeaderSelfRemoval()`. A follower catching up can legitimately receive
   a batch with several config changes committed across separate leader rounds.
5. `log.commitUpdate()` to advance `savedTo` and truncate the inmem buffer.
6. On a leader whose entries just became durable, re-run `tryCommit()` and
   `broadcastAppend()` if the commit index moved (see Section 6).

### Atomic LeaderID

`Peer.leaderID` and `Peer.term` are updated by `syncLeaderID()` after every
tick, step, propose, and GetUpdate. This enables lock-free `LeaderID()`,
`IsLeader()` and `LeaderAndTerm()` queries for routing decisions without
blocking the raft state machine.

## 14. Raft Log Architecture

The log is a two-tier structure: `raftLog` (`log.go`) composes `inMemory` and
`LogReader`.

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
  |   confChangeIndex           |  upper bound on an unapplied config change
  +-----------------------------+
```

### Entry Lifecycle

1. **Append**: `raft.log.append()` -> `inmem.merge()`. Entries are assigned
   term and index, then placed in the in-memory buffer. `append` asserts
   `entries[0].Index > committed` and raises `confChangeIndex` for any
   config-carrying entry.

2. **Save**: `entriesToSave()` returns entries with index > `savedTo`. The
   engine persists these to LogDB.

3. **Commit**: the leader calculates the quorum match index. A follower learns
   the commit from the leader's `Commit` field in `Replicate` or `Heartbeat`.

4. **Apply**: `entriesToApply()` returns entries in
   `[processed+1, committed+1)`, limited to `maxEntriesToApplySize`, a fixed
   64 MiB constant in `log.go`. This is NOT `Config.MaxApplyEntrySize`, which
   despite its name bounds only the REPLICATION payload
   (`raft.maxReplicationPayload`); the two are independent.
   `entriesToApplyUpTo(ceiling)` is the variant the piggyback path uses to
   withhold entries that are committed but not yet durable.

5. **Truncate**: after apply, `appliedLogTo()` truncates the inmem buffer from
   the front, advances the processed index, and decreases the rate limiter
   counters.

### Recovery Clamps

`newRaftLog` seeds `committed` from the persisted hard state, raised to the
snapshot index (a snapshot's index is committed by construction, so it is a
lower bound on `committed` regardless of what HardState survived), then clamps
it DOWN to what the log actually holds.

The downward clamp is load-bearing. A recovered `HardState.Commit` ahead of the
last recovered entry (a torn batch that kept the state record but lost entries
behind it) would otherwise be trusted absolutely: `getConflictIndex` skips
every entry at or below `committed` as "cannot conflict", so `tryAppend`
reports success for indices this replica does not have, `handleAppend` sends a
NON-rejecting `ReplicateResp` for them, and the leader counts that phantom
match toward quorum, committing entries a majority does not hold. The total
loss case (`last == 0`: the state record survived but the log holds nothing at
all) clamps to 0, making the replica indistinguishable from a fresh one, and is
logged at ERROR because only an operator can tell a wiped volume from a restore
that was never finished.

`processed` is then clamped to `committed`, unconditionally. It is seeded from
`ss.Index` or `firstIdx-1`, neither of which is bounded by the recovered
`HardState.Commit`, so a log recovered with a gapped start and a lower commit
comes up with `processed > committed` and `handleSnapshot`'s matching branch
would trip `appliedTo`'s monotonicity assertion. Found by
`FuzzRaftLogRecovery`.

`confChangeIndex` is seeded conservatively at `lastIndex()`; see Section 9.

### InMemory Buffer (`inmemory.go`)

Pre-allocated to 512 entries (`entrySliceSize`). `merge()` handles three cases:

- **Contiguous**: new entries follow existing, so a simple append.
- **Replace all**: new entries start at or before `markerIndex`, so reset.
- **Overlap**: keep the matching prefix, append the suffix. A guard against
  `offset > len(entries)` prevents a panic on nil slices after snapshot
  restore. An overlapping merge resets `savedTo` to `firstNewIndex - 1` so
  `entriesToSave()` re-emits the replaced tail.

Resize management: the `shrunk` flag tracks front-truncation.
`resizeIfNeeded()` reallocates when free capacity drops below
`minEntrySliceFreeSize` (64 entries), avoiding repeated growth when small
truncations are followed by new appends.

### Term Resolution

`raftLog.term(index)` checks inmem first (O(1) index arithmetic), then falls
back to `LogReader.Term()`, translating `logdb.ErrCompacted`/`ErrUnavailable`
into the raft-package sentinels so `errors.Is` comparisons work. The inmem
layer also caches a single `(appliedTo, appliedToTerm)` pair for lookups at the
truncation boundary.

`raftLog.lastTerm()` returns `(term, ok)` rather than fabricating 0. Term 0 is
Raft's "no term" sentinel, and a fabricated 0 is dangerous in BOTH directions:
fed to `isUpToDate` as our own last term it makes every candidate look up to
date, and advertised in a campaign it makes this node look empty so every peer
rejects it.

`firstIndexOfTerm` and `lastIndexOfTerm` binary-search the monotonically
non-decreasing term sequence in O(log N) for the term-based backtracking in
Section 4.

### LogDB Fallback on Recovery

After a restart the inmem buffer is empty. `firstIndex()` and `lastIndex()`
fall back to `LogReader.IndexRange()` to determine log bounds from persistent
LogDB state, which is required for correct term lookups during recovery before
the first append.

## 15. LeaseRead

Lease-based linearizable reads (PhD 6.4) skip the heartbeat quorum round when a
valid lease exists.

### Lease Validity

`hasValidLease()` requires all of:

1. `Config.LeaseRead = true` (opt-in) and this replica is the leader with a
   non-nil `remoteLastAck`.
2. A quorum of ALL voting members (remotes + witnesses + self) acked a
   heartbeat within `leaseTimeout` ticks. Witnesses vote, so the lease quorum
   must span the same set as the election quorum; otherwise a lease could be
   held by a coalition that a new leader's election coalition does not
   intersect.

`leaseTimeout` is `ElectionRTT - 2` ticks. The safety argument (PhD 6.4):
followers will not grant votes to another candidate until their own election
timeout expires, and the 2-tick margin covers goroutine scheduling jitter
between the leader's lease check and the followers' election timers. The lease
is measured in TICKS, not wall-clock time, so it does not depend on clock
synchronization between nodes; it does depend on ticks being delivered at
roughly the configured rate on the leader.

CheckQuorum should be enabled (it is a `*bool` whose nil zero value means
enabled, so this holds unless explicitly disabled with `config.BoolPtr(false)`)
so the leader periodically re-verifies quorum and `resetLeaseAcks` forces the
lease to be re-earned.

The lease is invalidated at every point the clock or the role changes:
`becomeFollower`, `exitQuiesce`, quiesce entry, `handleLeaderTransfer`, and the
`resetLeaseAcks` call at the end of each CheckQuorum cycle. That last one is
not optional: after `electionTick` wraps to 0, `leaseExpiry = 0 - leaseTimeout`
underflows negative and every stale positive tick would appear valid.
`syncLeaseAcks` drops entries for replicas that left the voting set and
deliberately does NOT seed new ones, because a seeded tick of 0 is
indistinguishable from an ack recorded at tick 0 and would let a member that is
not even running contribute to the lease.

### Flow

**There is no `Host.LeaseRead` method.** Lease read is not a separate API; it
is an internal optimization inside the existing ReadIndex path, transparent to
callers. Enable it with `Config.LeaseRead = true` and keep calling
`Host.ReadIndex` / `Host.SyncRead`.

```
Host.ReadIndex(ctx, shardID)   (or Host.SyncRead)
  |
  +-- handleLeaderReadIndex checks numVotingMembers == 1 || hasValidLease()
  |     |
  |     +-- yes: resolve at the current committed index, no heartbeat round
  |     +-- no:  standard ReadIndex (heartbeat quorum confirmation)
  |
  +-- either way the caller still waits for lastApplied >= readIndex
```

The leader tracks per-remote heartbeat ack ticks in
`remoteLastAck map[uint64]int`.

### Failure Mode

If the lease expires, `hasValidLease()` returns false and the request takes the
standard ReadIndex path. Callers see no difference beyond latency.

## 16. Client Sessions

Sessions implement at-most-once delivery (PhD Figure 6.1) for exactly-once
state machine semantics. The full design is in
`docs/sessions/deduplication.md`; this section covers only the raft-visible
parts.

### Session Registration Through Raft

Sessions are registered and unregistered via Raft consensus, not locally. Entry
types are distinguished by reserved series IDs (`proto/constants.go`):

- `SessionSeriesIDForRegister = math.MaxUint64 - 1`
- `SessionSeriesIDForUnregister = math.MaxUint64`

Entry helpers query the type: `IsNewSessionRequest()`,
`IsEndOfSessionRequest()`, `IsSessionManaged()`.

### Deterministic Index-Based Expiry

Sessions expire on applied index, not wall-clock time. The `rsm` apply layer
maintains a `session.Manager` tracking registered sessions and their last-active
index; `ExpireSessions(currentIndex, inactivityThreshold, maxExpire)` removes
those inactive for longer than the threshold. All replicas therefore make
identical expiry decisions regardless of clock skew.

Candidates are sorted by client ID BEFORE the per-call cap is applied, so every
replica selects the same subset when more sessions are eligible than one call
may expire.

### Response Caching

The `session.Manager` tracks up to `maxResponses` (default 128) completed
operations per session, keyed by series ID within a client's session. On
proposal the engine checks the response cache before applying to the state
machine and returns the cached response without re-execution on a hit.

The cache enforces a hard limit: `RecordResult` returns
`ErrResponseLimitExceeded` when full, forcing the client to unregister before
registering a new session.

The three session limits (`maxTotal`, `maxResponses`, `maxPerOwner`) are
REPLICATED state carried on `proto.Membership` as a `SessionPolicy`, seeded at
bootstrap from `config.Config`. Unlike the witness policy there is no config
change that alters it, and that immutability is load-bearing: the state machine
reads the published value on the APPLY goroutine, which trails commit, so a
value that changed mid-log would be observed at a replica-local point in the
log.

### Session Proposal Flow

```
Propose(clientID, seriesID, cmd)
  |
  +-- check session cache: (clientID, seriesID) -> cached response?
  |     |
  |     +-- yes: return cached response immediately
  |     +-- no: append entry, proceed to apply
  |
Apply entry
  |
  +-- SM.Update(cmd)
  +-- RecordResult(clientID, seriesID, result)
  |
  +-- returns ErrResponseLimitExceeded at maxResponses
```

## 17. Error Handling

Errors in `pkg/internal/raft/errors.go` are pre-allocated sentinel values, so
the hot path returns them with no allocation. Selected errors:

| Error                          | Condition                                                        |
|--------------------------------|------------------------------------------------------------------|
| `ErrCompacted`                 | Requested entries removed by log compaction                       |
| `ErrUnavailable`               | Index beyond last entry                                           |
| `ErrGappedEntryBatch`          | Append batch is not index-contiguous                              |
| `ErrTransferInProgress`        | Proposal or ReadIndex rejected during leader transfer             |
| `ErrPendingConfigChange`       | Second config change proposed before the first commits            |
| `ErrReadIndexNotReady`         | Leader has not committed an entry in the current term             |
| `ErrReadIndexOverloaded`       | Pending-read or read-state cap reached                            |
| `ErrReadIndexContextNotIssued` | HeartbeatResp echoed a context this leader never issued           |
| `ErrReadIndexRespNotFromLeader`| ReadIndexResp from a replica that is not this shard's leader       |
| `ErrHeartbeatNotFromLeader`    | Heartbeat from a second claimant at the current term              |
| `ErrSnapshotNotFromLeader`     | InstallSnapshot from a second claimant at the current term        |
| `ErrWireLocalMessage`          | Process-local message type arrived from the wire                  |
| `ErrZeroTermMessage`           | Protocol message with no term and no local origin                 |
| `ErrTermCeilingExceeded`       | Message term inside the reserved ceiling band                     |
| `ErrReservedEntryType`         | Propose carried a reserved entry type                             |
| `ErrConfigChangeUnmarshal`     | Config change payload does not decode at propose time             |
| `ErrCorruptConfigEntry`        | COMMITTED config entry does not decode (fatal for the shard)      |
| `ErrTooManyVotingMembers`      | Voting set exceeds the 64-member ackBits cap                      |
| `ErrRemoveLastVoter`           | Attempt to remove the last voting member                          |
| `ErrRemoveLastFullReplica`     | Attempt to remove the last non-witness replica                    |
| `ErrNodeRemoved`               | Attempt to re-add a removed node                                  |
| `ErrDuplicateMemberAddress`    | Address already held by another active member                     |
| `ErrDuplicateMemberIdentity`   | Identity already bound to another active member                   |
| `ErrRemovedMemberIdentity`     | Identity carries a removal tombstone                              |
| `ErrTooManyWitnesses`          | Witness policy cap would be exceeded                              |
| `ErrBreaksFullReplicaMajority` | Change would leave full replicas unable to form a quorum          |
| `ErrWitnessRoleMismatch`       | Local role contradicts the replicated membership                  |
| `ErrJoiningNodeCannotCampaign` | TimeoutNow on a replica with incomplete membership                |
| `ErrStaleConfigCannotCampaign` | Configuration tail unresolved at a TimeoutNow                     |
| `ErrInflightsFull`             | Inflight window at capacity (defensive backstop)                  |

`NotLeaderError` (`errors.go`) is a struct carrying `LeaderID` and
`LeaderAddress` so a client can redirect without an extra round-trip. The Host
layer translates it to a public error type.

Errors surfaced to Host callers rather than produced by this package include
`ErrInvalidOperation` (`pkg/errors.go`, returned by `Host.ReadIndex` on a
witness) and `ErrResponseLimitExceeded` (`pkg/internal/session/errors.go`).

## 18. Message Ordering and Partial Drains

Some outbound messages may be released before the state they depend on is
durable, and some may not (Paper Figure 2: state is "updated on stable storage
before responding to RPCs"). `proto.IsFreeOrderMessage` is the single
predicate:

**Free-order** (may be released early): `Replicate`, `Ping`, `Heartbeat`,
`Quiesce`, `RequestPreVote`, `RequestPreVoteResp`. None of them acknowledges
anything about this replica's durable state, and PreVote never advances a term.

**Ordered** (everything else, including `RequestVoteResp`, `HeartbeatResp`,
`ReplicateResp`, `ReadIndexResp`, `InstallSnapshot`): these either grant a vote
or acknowledge a log position, so releasing one before the corresponding WAL
write lets a leader count a match it may lose in a crash, or lets a vote be
answered with no record of it on disk.

Three drains apply the rule:

- `drainMessages()` takes everything (the normal `GetUpdate` path, which is
  followed by persistence).
- `drainFreeOrderMessages()` takes only the free-order messages and leaves the
  ordered ones in `r.msgs`. The step worker uses it during `commitPending`
  ticks and when the output channel is full, so heartbeats keep flowing for
  leader liveness while ordered messages wait for the WAL. Without this,
  a SaveState retry backoff would silence every shard the worker owns and
  trigger elections against a live leader.
- `withholdOrderedMessages(msgs)` is the drain-first counterpart used by
  `GetPiggybackUpdate`: it compacts free-order messages to the front of an
  ALREADY-DRAINED batch and pushes the ordered ones back into `r.msgs`. It
  exists separately because `drainFreeOrderMessages` returns `r.freeMsgBuf`, a
  single buffer owned by the step worker, and the commit worker calls this one
  while the step worker is concurrently inside `processCommitPendingInbox`.

Withheld messages are never dropped: `len(r.msgs) > 0` keeps `HasUpdate()`
true, and the next durability-bearing cycle emits them.

---

## The engine pipeline

Source: `pkg/internal/engine/`

### Overview

The engine drives Raft consensus through a three-stage pipeline: step worker (Raft logic), commit worker (persistence), apply worker (state machine). The design buys pipelining (free-order network I/O overlapped with disk I/O), batching, and single-owner semantics via the `commitPending` atomic flag.

```
Host.Propose()  Host.ReadIndex()  transport
      |               |               |
   proposalQ      readIndexQ       inboxQ        (MPSC EntryQueues)
      \               |               /
       +--------------+--------------+
                      |
                 STEP WORKER  ── Peer.Handle() ── Peer.GetUpdate()
                      |                 |
                      |                 +──> QUIC send: FREE-ORDER msgs
                      |                      (Heartbeat, Replicate,
                      |                       RequestVote) BEFORE persist
                 commitPending = true
                      |
                   commitC
                      |
                COMMIT WORKER ── logdb.SaveState(batch)
                      |                 |
                      |                 +──> QUIC send: ORDERED msgs
                      |                      (VoteResp, HeartbeatResp,
                      |                       ReplicateResp) AFTER persist
                      |                 +──> OnCommitted (if NotifyCommit)
                 Peer.Commit()
                      |
                   applyC
                      |
              APPLY DISPATCHER ──> per-shard goroutine
                                          |
                                     RSM adapter ──> user state machine
                                          |
                                     OnApplied ──> RequestState.complete()
                                          |
                                   commitPending = false
```

**Partitioning**: Shards hash to workers via `shardID % NumWorkers`. Shards in different partitions never contend; shards sharing a partition share one goroutine per tier.

**Message ordering is a safety boundary, not a tuning knob.** Free-order messages go out from the step worker before the WAL persist, which is what overlaps network and disk I/O. Ordered messages — a vote grant, a heartbeat or replicate acknowledgement — go out from the commit worker only after `SaveState` returns, because each of them promises a state change is durable. Sending a `VoteResp` before the vote is on disk lets a crash-and-restart node vote twice in one term. Raft PhD §10.2.1.

**Batching**: The commit worker blocks on the first item, then non-blocking-drains whatever accumulated while the previous persist was in flight, and persists the lot in one `SaveState`.

**Single-owner**: The `commitPending` flag ensures only one worker tier touches a shard's Peer at any time. `Peer.Tick()` is the deliberate exception; see the Step Worker section.


### Engine Lifecycle

**Initialization**: `NumWorkers` (workers per tier: step/commit/apply, 0 means `runtime.NumCPU()`), `MaxApplyWorkers` (apply semaphore), `CommitCBufferSize` (commit channel, default 64) and a derived apply channel buffer of `max(CommitCBufferSize*4, 256)`, `RTTMillisecond` (tick interval). The snapshot pool gets `max(NumWorkers/2, 2)` workers. Tick interval derived from RTTMillisecond maps Raft election/heartbeat timeouts to real network latency.

**Memory mode**: when the LogDB is nil, step workers set `memoryMode` and inline `Peer.Commit`, forwarding straight to `applyC`. The commit worker goroutine is bypassed and the pipeline is two hops instead of three.

**Startup**: The listener dispatcher starts FIRST — until its goroutine is running, every notification would be invoked inline on the producing worker, which is exactly the reentrancy hazard it exists to remove. Then workers launch in reverse pipeline order (snapshot, apply, commit, step), so downstream consumers are ready before upstream producers emit. Each tier is tracked by a `sync.WaitGroup` for sequential shutdown.

**Restart on panic**: all three worker tiers run inside a restart loop. A panicking worker would otherwise stall every shard mapped to it permanently. The step-worker recovery clears `commitPending` for the single node that was mid-enqueue; the commit-worker recovery clears it for every node that worker owns. Both re-enter after a short delay so a deterministic panic does not become a tight loop.

**Shutdown (ordered)**: Guarantees no in-flight items lost between tiers:
1. `Node.Stop()` for all (close stopC channels, unblock snapshot ops)
2. `close(stepStopC)` → wait (no new Updates)
3. `close(commitStopC)` → drainAndProcess → wait (persist remaining commitC items)
4. `close(applyStopC)` → drain remaining applyC → wait (all per-shard goroutines finish)
5. `close(snapshotStopC)` → wait
6. Stop the listener dispatcher → wait

The dispatcher is stopped LAST, once every tier that can produce a notification has exited, and it drains what they queued before returning. A leadership loss or shard failure observed during teardown therefore still reaches the listener. A listener that blocks here blocks `Stop` — the same way it would previously have blocked its producing worker, except the dispatcher goroutine holds no engine lock.

Commit workers forward to apply unconditionally (no stopC escape) after persist: entries MUST be applied. Ordered shutdown guarantees apply workers still running.

**Node management**: `LoadNode` adds to `allNodes` and to the step worker at partition `shardID % NumWorkers`. `UnloadNode` reverses via `unloadC`. `DeliverMessage` (transport boundary) looks up the node under a read lock and calls `node.Deliver(msg)`, which stamps the message `OriginRemote`.


### Step Worker

**Source**: `step.go`

Drains proposals from the MPSC `EntryQueue` and ReadIndex requests from the dedicated `readIndexQ`. For each input, calls `Peer.Handle()` to process the Raft state change. Also drives Raft logical clocks via `Tick()` at intervals derived from `RTTMillisecond`. Produces `Update` (entries to persist, messages to send, committed entries) forwarded to commit worker.

**Main loop**: Woken by WorkSignal (message arrival, apply completion) or ticker. Drains inbox into reusable buffer, calls `peer.Handle()` for each message, checks `peer.HasUpdate()`, retrieves `Update`, and sends to commit worker via `commitC` (blocking with `stopC` escape).

**ReadIndex batching (OPT-8)**: ReadIndex requests accumulated in dedicated MPSC queue, drained as a batch with single key used as batchKey for coalescing. Multiple callers alias complete when batchKey resolves via `OnReadIndexBatched` callback.

**ReadOnly bypass**: When `Update.ReadOnly` flag set (no entries/state/snapshot), skips commit/apply pipeline entirely. Reduces ReadIndex latency from ~300us (3 goroutine hops) to ~5us (inline callback).

**Campaign detection**: Scans outbound `RequestVote`/`RequestPreVote` messages and calls `callback.OnCampaignLaunched(shardID, replicaID, term)` — cold path (elections seconds apart).

**Message pipelining**: Separates `InstallSnapshot` messages (routed to `sender.SendSnapshot()` for chunked streaming) from regular protocol messages (batched through `sender.Send()`). Only free-order messages leave here, before the entries are persisted; that is what overlaps network and disk I/O. Ordered messages are carried on the `commitItem` and released by the commit worker after `SaveState`.

**Ownership guard**: Checks `node.commitPending.Load()` before calling `Handle()` or `processReady()`. When true, the node is skipped for `Handle`/ready processing to prevent concurrent Peer access with the commit and apply workers.

`Tick()` is the exception: it runs **unconditionally**, regardless of `commitPending` state. This is required for leader liveness — the leader must always advance its heartbeat timer and send heartbeats, even while WAL fsync and state machine apply are in progress. Without it, a commit+apply cycle (200-500us under load) starves heartbeat delivery, followers time out and campaign, and the leader then fails `checkQuorum` and steps down.

What prevents `Tick()` racing `Commit()` is the per-node `raftMu` mutex, which serializes the two. Contention is minimal: `Tick()` is ~100ns (counter increment, conditional heartbeat broadcast) and `Commit()` ~200ns (marker updates, inmem truncation), and both run at most once per RTT interval. `raftMu` is **not reentrant** — event callbacks such as `OnLeaderUpdated` are invoked outside it, because a callback running under `raftMu` would block the commit worker for every shard that worker owns.

When `commitPending` is true, a `Tick()` may still produce messages that must go out immediately. `Peer.DrainFreeOrderMessages()` extracts only the free-order set — `Replicate`, `Ping`, `Heartbeat`, `Quiesce`, `RequestPreVote`, `RequestPreVoteResp` — and leaves ordered messages in the outbound buffer for the next `GetUpdate` cycle. Using the plain `DrainMessages()` here would silently drop a `VoteResp` produced by `processCommitPendingInbox`, so elections could not complete during a commit+apply cycle. Nothing ordered is ever sent before the WAL persist completes.

**Deferred InstallSnapshot redelivery**: Inbound `InstallSnapshot` messages that arrive during a `commitPending` window are buffered per-shard in `deferredSnapshots` rather than re-delivered to the inbox (avoiding a CPU-burning drain-redeliver loop). When the shard becomes processable again, `redeliverDeferredSnapshots` re-injects them via `node.Deliver()`. Only messages that `Deliver()` reports as accepted are dropped; messages rejected because the inbox is full are retained (compacted to the front of the buffer) for a later retry, so a transient full inbox never permanently loses a deferred snapshot.

**Batched proposal queue**: `internal/queue.EntryQueue` is MPSC with a *brief* `sync.Mutex`, not lock-free. Multiple producers take the lock only for a slice append (typically <50ns); the single consumer takes it once per drain cycle to `Swap` the write buffer with a caller-provided read buffer, regardless of item count. That amortization — one acquisition per drain rather than per item — is what reduces step worker contention versus a shared proposals map. `maxLen` provides bounded backpressure: `Add`/`AddOne` return false at capacity and add nothing. A refused proposal surfaces as `ErrDropped` (with `IncProposalDropped` and the `OnProposalDropped` event); a refused ReadIndex surfaces as `ErrSystemBusy`. `ErrSystemBusy` is reserved for the global `MaxTotalInMemLogSize` rate limiter, so reporting it for a full per-shard proposal queue would send an operator to the wrong metric.


### Commit Worker

**Source**: `commit.go`

Receives `Update` from step worker, persists entries to LogDB (sharded WAL with 16 shards), calls `Peer.Commit()` to advance internal state, forwards to apply worker. Uses drain-then-batch pattern: blocks on first item, non-blocking drains remaining items while previous persist completes, batches them together.

**Ordered-message send**: The commit worker holds back every ordered message the step worker produced (`commitItem.orderedMsgs`, accumulated per shard in `orderedMsgsBuf`) and sends them only after `SaveState` succeeds. `InstallSnapshot` is routed to `SendSnapshot` here as it is in the step worker. The overlap between network and disk I/O comes from the step worker sending free-order messages before the persist, not from sending these concurrently with it.

**ReadOnly bypass**: When `Update.ReadOnly` flag set, skips persistence entirely and bounces back to step worker. Reduces latency since no entries need durability.

**Piggyback commit (O29)**: On a multi-node proposal, entries are persisted in cycle 1 and the commit index advances during the `commitPending` window via `processCommitPendingInbox`. Rather than making those entries wait for a second full pipeline traversal, `Peer.GetPiggybackUpdate(lastApplied)` extracts the newly committable entries and applies them in the same cycle. It is deliberately narrower than `GetUpdate`: it writes no `EntriesToSave` (the entries it applies are clamped to the durable marker), leaves `readStates` and pending snapshots alone, releases only the messages the durability rules permit, and does not advance `prevState` — the engine does that through `NotePiggybackPersisted` once the hard state is on disk, so a failed persist re-emits it.

**CommitNotify**: When `HostConfig.NotifyCommit` is enabled, the commit worker fires `Callback.OnCommitted(shardID, entries)` after `SaveState` succeeds and before the apply forward, per update so the callback is scoped to one shard. This lets a caller learn an entry is WAL-durable before it is applied.

**SaveState retry**: 5 attempts with 4 inter-attempt waits — 10ms, 50ms, 200ms, 500ms, for ~760ms of pre-failure grace. The final attempt is not followed by a wait, which is why the schedule has one fewer element than there are attempts. Both terminal outcomes — retry exhaustion AND shutdown cancellation (`stopC` fired during a retry backoff) — funnel through the shared `failBatchSaveState` helper. For every node in the batch it marks `failed = true`, clears `commitPending`, reports a `SaveStateError` via `handleError`, and fails the batch's pending proposals via `OnProposalFailed`, so callers see an immediate error rather than blocking until their context deadline. The batch's entries were extracted but never persisted, so the node is left permanently failed (recover by reloading the shard) to avoid acknowledging unpersisted entries.

**Peer.Commit()**: Advances internal Peer state (log truncation, stable storage markers). Populates `LogReader.termRingBuffer` (O(log n) binary search term lookups, zero GC pressure) for efficient term resolution on restart.

**Forward to apply (unconditional)**: After successful persist, sends `applyItem` to apply worker via blocking send WITHOUT `stopC` escape. Persisted entries MUST be applied (dropping them leaves committed-but-unapplied state). Ordered shutdown (apply workers stop after commit) guarantees consumer is running.


### Apply Worker

**Source**: `apply.go`

Per-shard goroutines gated by semaphore (MaxApplyWorkers limit). Fan-out dispatcher reads from `applyC` and routes each item to per-shard goroutine via `shardChans[shardID]`. Each shard applies committed entries sequentially to RSM adapter.

**RSM adapter routing**: Session register/unregister entries routed to `session.Manager`, normal entries forwarded to state machine. Register/unregister proposed through Raft (not local-only stub). Routing is by the `proto.Entry.SeriesID` sentinels `proto.SessionSeriesIDForRegister` (`math.MaxUint64 - 1`) and `proto.SessionSeriesIDForUnregister` (`math.MaxUint64`).

**Entry decompression**: Before each `SM.Apply()` call, decompresses entries in-place if compressed on propose path (Snappy). Bounded by `MaxEntrySize` to prevent decompression bomb attacks.

**Semaphore per-call (not per-goroutine)**: Acquired at apply time, released after. Idle shard goroutines do not hold slots, preventing deadlock when shards > MaxApplyWorkers.

**Fail-fast on apply error**: Any `SM.Apply()` or decompression error is terminal. The shared `failApply` helper fails the batch's pending proposals via `OnProposalFailed`, sets `node.failed = true` immediately (on the FIRST error — no retry counting), and reports an `ApplyError` via `handleError`. `lastApplied` is deliberately NOT advanced: skipping the batch while advancing the watermark would silently diverge this state machine from its peers, violating Raft's state-machine safety property. The step worker then skips the shard (`IsFailed()` check) until operator intervention (reload or snapshot recovery). This mirrors the SM-panic recovery path, which already took the shard offline on the first panic.

**Post-apply flow**:
1. Callback delivers results to Host layer for completing pending proposals
2. Updates `node.lastApplied` to highest index in batch
3. Auto-snapshot trigger: if `SnapshotEntries > 0` and `lastApplied - lastSnapshotIndex >= SnapshotEntries`, requests a snapshot (concurrent requests prevented by CAS). `config.SnapshotsDisabled` (`math.MaxUint64`) needs no special case: it is a threshold the `>=` comparison never reaches. Note that a zero `SnapshotEntries` reaching the engine means the config was never passed through `SetDefaults`, which substitutes `DefaultSnapshotEntries` (100000).
4. Clears `commitPending = false` (in defer) to return ownership to step worker
5. Wakes step worker via `workSignal.Notify()` for immediate re-processing without waiting for next tick

**UnloadNode cleanup**: When `engine.UnloadNode(shardID)` called, dispatcher receives via `unloadC` channel, closes per-shard goroutine channel, allowing exit and resource cleanup. Prevents goroutine leak on node unload.

**Skip-apply optimization (O29)**: An update whose first cycle carries no `CommittedEntries` bypasses the apply pipeline entirely rather than paying a dispatcher hop and a semaphore acquisition to apply nothing.


### Node Struct and Concurrency

Each shard has a `Node` containing Raft state, state machine instance, message inbox, and ownership atomics.

**Cache-line padding**: `commitPending` and `lastApplied` padded to 128 bytes (prefetch pair on x86/ARM) to prevent false sharing between step worker (reader) and apply worker (writer). Primary cross-worker communication path.

**commitPending ownership**:
- Normal: Step worker owns Peer, reads both `commitPending` and `lastApplied`
- commitPending=true: Commit/apply workers hold Peer, step worker skips shard
- commitPending=false: Ownership returned to step worker via `workSignal.Notify()`

Atomic flag blocks step worker from concurrent Peer access during commit/apply phase, enforcing single-owner semantics.

**Three inbound queues**, all `internal/queue.EntryQueue` (MPSC, double-buffered, mutex-guarded):

| Queue | Element | maxLen | Producers |
|-------|---------|--------|-----------|
| `proposalQ` | `proto.Entry` | 256 | `Host.Propose` callers, via `DeliverProposal` |
| `inboxQ` | `proto.Message` | 4096 | transport delivery goroutines, via `Deliver` |
| `readIndexQ` | `uint64` (read key) | 4096 | `Host.ReadIndex` callers, via `DeliverReadIndex` |

`inboxQ` replaced a 256-capacity buffered channel that silently dropped messages under burst load; the queue drains in batches via `Swap` and reuses its slices, so steady state is allocation-free. Backpressure is still by refusal at `maxLen` — dropping a Raft message is safe because the sender retransmits, and a refused proposal surfaces as `ErrDropped` rather than a silent loss. Proposals were split out of `inboxQ` (optimization O19) to remove the channel serialization bottleneck under concurrent proposal load; `inboxQ` now carries only non-proposal transport messages.

**Message origin**: `Deliver` stamps `proto.OriginRemote`, `DeliverLocal` stamps `OriginLocal`. `OriginRemote` is the zero value, so a message decoded from the wire or recycled through the pool is untrusted by construction. Local-origin messages carry no term and are exempt from the term gate in `raft.step`, so the exemption must not be reachable from the network — which is why the marshaller never reads or writes the field.

**Failed shard gate**: On any SM.Apply or decompression error (and on SM panic), the apply worker sets `failed = true` immediately — there is no retry counting and no retry budget to configure. The step worker checks `IsFailed()` in both `tick()` and `processReady()` and skips the node entirely, removing it from the pipeline until operator intervention (reload or snapshot recovery).

**Snapshot adoption gate**: A received `InstallSnapshot` moves the raft log's committed AND processed markers to the snapshot index the instant the step worker steps it, but the state machine adopts that index only when the snapshot pool finishes `RecoverFromSnapshot` and seeds `lastApplied`. Between the two, raft offers committed entries from `snapshotIndex+1` over a `lastApplied` that is still behind — a batch the apply worker refuses as `ErrApplyGap`, taking a replica that had just finished joining permanently out of apply. The `recovering` flag does not cover that window: the pool raises it from its own goroutine and cannot do so until it has observed the install. So both extraction sites — `processReady`'s `GetUpdate` and the commit worker's piggyback — additionally check `snapshotAdoptionPending()` (raft `processed` > `lastApplied`) on the Peer's owning goroutine. Withholding cannot wedge the log: extraction is the only thing that advances `processed`, so the entries are re-offered once the marker is seeded.

**stopC channel**: Closed by `Stop()` to signal cancellation for snapshot operations (used in SaveSnapshot/RecoverFromSnapshot context).


### Work Signaling

`WorkSignal` routes shard notifications to correct worker partition without contention.

**Notify fast path**: Atomic pre-filter `notified[workerID]` avoids ~10-15ns channel send when worker already notified (most calls hit this). Only sends to channel when transitioning from non-notified → notified state.

**ReadyMap (double-buffered)**: One `sync.Mutex` over one live map, plus a two-map rotation (`maps [2]`). `Set()` holds the lock for a single map insert; `Swap()` clears the alternate map outside the lock, then holds it only to exchange pointers, and returns the map that was live. Two-map rotation keeps the swap path at zero steady-state allocations. The returned map is owned by the caller only until the next `Swap()`, which recycles it.

**HasWork() utility**: Returns atomic load of WorkSignal state, ~1ns check for poll-based optimization.


### Listener Dispatcher

**Source**: `dispatch.go`

User-visible cold-path notifications run on one dedicated goroutine rather than on the worker that produced them. This is a correctness fix, not a throughput optimization.

Every callback the step worker fired ran synchronously on the step-worker goroutine *while that goroutine held `stepWorker.nodesMu.RLock`* — `tick()` and `processReady()` each hold it for their whole body. `Host.StopShard` re-enters the same goroutine's `nodesMu.Lock()` via `Engine.UnloadNode` → `stepWorker.unloadNode`, and Go's `RWMutex` is not reentrant. So an operator handler that reacted to an event by stopping the shard did not stall, it deadlocked — permanently, and not only for that shard. Every shard mapped to the same worker (`shardID % NumWorkers`) stopped ticking: no heartbeats, no elections, no inbox drain, and a later `Host.Close` never returned from `Engine.Stop`'s `stepWg.Wait()`. "React to a failed shard by stopping it" is the obvious handler to write, which is what made it reachable.

Moving the notification off the worker goroutine removes the class rather than the instance. The dispatcher goroutine holds no engine lock, so a listener may call any Host API, including the synchronous `StopShard`.

**Ordering**: notifications are consumed by exactly one goroutine from a FIFO channel, so delivery follows the order the engine produced them — a total order, and therefore a per-shard order. There is deliberately no ordering guarantee against the callbacks that stay synchronous; those were already unordered with respect to each other, since they run on three different worker tiers.

**Queue-full policy** (`listenerQueueSize` = 1024): `dispatch` never blocks the producing worker — a worker parked on a full queue still holds `nodesMu.RLock`, which would reintroduce the deadlock. It also never drops silently: `dispatch` reports acceptance and each caller picks a policy. `OnLeaderUpdated` leaves its bookkeeping uncommitted and re-publishes next tick; `OnCampaignLaunched` and `OnShardFailed` drop, counted and logged at ERROR. The queue only fills when a listener violates the documented non-blocking contract.


### Snapshot Pool

Asynchronous snapshot operations. Bounded pool (NumWorkers/2, minimum 2) with per-shard dedup via sync.Map.

**Non-blocking completedC send**: Uses select/default when forwarding snapshot completion results. Prevents full channel from blocking all snapshot workers. Result is logged at slog.Warn and dropped if channel full.

**Per-shard dedup**: `LoadOrStore` ensures only one snapshot per shard in-flight. Returns `SnapshotDuplicateError` if shard already has snapshot in-flight, `SnapshotPoolBusyError` if request channel full.

**Save flow**: Creates directory, opens data file, calls state machine `SaveSnapshot()`, fsyncs, enforces `MaxSnapshotSize` (post-compression), encrypts if barrier unsealed, writes the `snapshot.meta` record, persists to LogDB including full cluster membership, garbage collects old snapshots (keep 3), updates `lastSnapshotIndex`, compacts log, purges old barrier epochs.

#### snapshot.meta

A snapshot directory MUST describe itself: it outlives its LogDB record (up to `maxSnapshotsRetained` directories are retained while only the newest is recorded), and the DEK purge floor is computed by scanning these directories and reading the `Epoch` field back as fact.

The record is **self-describing**: a 16-byte header followed by a body of little-endian uint64 fields. The body at version 1 is 9 fields (72 bytes), so a record this binary writes is **88 bytes**.

```
 off  size  field
   0     8  magic "QRSNAPMD"
   8     2  version (uint16, format version of the BODY)
  10     2  reserved (uint16, 0)
  12     4  body length (uint32, bytes of body that follow)
  16     8  Index
  24     8  Term
  32     8  ShardID
  40     8  ReplicaID
  48     8  CreatedAt (unix seconds)
  56     8  Epoch
  64     8  Compression
  72     8  DataLen
  80     8  DataHash (xxhash64)
```

`DataLen` and `DataHash` cover `snapshot.dat` **as written** — after compression and after encryption. They are the only at-rest integrity check a snapshot has when no barrier is configured: with a barrier, per-frame AEAD covers it, but a plaintext deployment had nothing, and a single flipped byte inside a serialized value leaves the file length unchanged, so no structural check trips. The replica then recovers the corrupt value into its state machine and serves it, diverging from its peers at the same log index with every Raft invariant intact. Hashing the final bytes rather than the pre-encryption ones is what lets all three consumers — recovery, the streamed receive path and `ImportSnapshot` — verify one value without holding a key. Records written before the field existed leave `DataHashRecorded` false and are recovered without the check rather than refused.

**Evolution rule**: body fields are append-only — an existing field never moves and never changes meaning. A new field is appended and the version is incremented. Because the body length is recorded, a reader can always tell a field that is ABSENT (written by an older version) from one that is CORRUPT (a truncated or foreign record). Changing the meaning of an existing field is not permitted under this rule; that requires a new magic.

**Compatibility**: three pre-magic ("v0") layouts are still accepted verbatim, positionally, at their original offsets — **40** bytes (`Index|Term|ShardID|ReplicaID|CreatedAt`), **48** bytes (`...|Epoch`) and **56** bytes (`...|Epoch|Compression`). The 40-byte layout was written by the snapshot receive and import paths and carries no `Epoch` even though the artifact beside it may be encrypted, so such a directory can hold ciphertext at an epoch nothing on disk names. It is therefore reported as UNEVALUABLE, never as epoch 0 — a zero `Epoch` with `EpochRecorded` false means UNKNOWN, not plaintext, and the two are opposite in consequence for DEK purging. Nothing rewrites an existing record: a v0 record stays v0 until its snapshot directory rolls off.

The declared body length is bounded at 64 KiB before use. It is a `uint32` read from a file and is used to slice the body, so on a 32-bit GOARCH an unbounded declared length could become a negative int and panic inside the purge-floor scan, whose entire input is bytes on disk.

**Recover flow**: Gets latest snapshot metadata from LogDB, locates the directory, verifies the artifact against `DataLen`/`DataHash` from `snapshot.meta` before anything reads it, then opens the data file, calls the state machine's `RecoverFromSnapshot()`, and updates `lastApplied` and `lastSnapshotIndex`. Sets the `recovering` atomic flag gating all SM operations during recovery (safety property S1).

When the LogDB record says `Epoch > 0`:

- If no barrier is available or the barrier is sealed, recovery fails closed with `ErrSnapshotEncryptedBarrierUnavailable`. The leader re-sends. Falling through would hand raw ciphertext to `RecoverFromSnapshot`.
- Otherwise the artifact is decrypted to a throwaway **sibling** file and recovery reads that, leaving the canonical `snapshot.dat` untouched. Decrypting in place is not crash-safe: a failure or crash after the rewrite would leave plaintext on disk while LogDB still records `Epoch > 0`, so every later recovery would re-attempt decryption on plaintext and fail, permanently losing an otherwise valid snapshot.

**Compression**: When `config.SnapshotCompressionType == config.Snappy`, wraps the file with `snappy.NewBufferedWriter` on save and `snappy.NewReader` on recover. `MaxSnapshotSize` check applied post-compression to measure actual disk usage. State machine unaware of compression.

**Encryption**: When `SnapshotBarrier` configured and unsealed, encrypts data file after fsync (ensures durable plaintext) and before metadata persist. Frame format: [ChunkLen:4 LE | EncryptedChunk]... per 64KB chunks, each including barrier's epoch+nonce+tag. Atomic temp-file + fsync + rename pattern. `DecryptForEpoch()` supports key rotation: old snapshots decrypted with epoch-specific DEK even after rotation.

**Epoch purge**: After garbage collection, scans remaining snapshot directories for oldest epoch in use, calls `barrier.PurgeEpochsBefore(minEpoch)` to zero/remove DEKs for epochs no longer referenced by retained snapshots or WAL segments. Limits key material exposure in memory.

**Leader snapshot streaming**: Step worker routes `InstallSnapshot` messages to `transport.SendSnapshot()`, chunked streaming transfer to follower. Receiver handles chunked reassembly on disk, saves to LogDB, triggers recovery. Prevents unbounded memory usage during snapshot transfer.


### Concurrency and Safety

**Single-owner Peer semantics**: Raft Peer is NOT thread-safe. `commitPending` atomic flag enforces exclusive ownership through pipeline topology:
- Normal: Step worker owns Peer (Tick, Handle, HasUpdate, GetUpdate)
- commitPending=true: Commit worker holds Peer via Peer.Commit(), then apply worker (SM.Apply only, Peer untouched)
- commitPending=false: Ownership returned to step worker

Step worker checks `commitPending.Load()` before touching Peer in `processReady()` and skips the node when set. No re-enqueue, to avoid spin loops; naturally re-checked on the next tick or WorkSignal notification. `tick()` is deliberately **not** gated this way — see the Step Worker ownership guard above.

**Happens-before proof (commitIndex safety)**: the `commitPending` flag blocks the step worker from producing a new Update until the apply worker finishes, so entries MUST be persisted AND applied before the step worker produces a new Update referencing them. It also prevents duplicate CommittedEntries: when `commitPending` is cleared, `lastApplied` has advanced past the committed entries, so the next `GetUpdate(lastApplied)` does not reproduce them.

The `Tick()`/`Commit()` race is prevented by the per-node `raftMu` mutex, not by the `commitPending` flag. Both `Peer.Tick()` and `Peer.Commit()` are called with `raftMu` held; `raftMu` also covers the step worker's `Peer.Step` and config-resolution paths and inbox message handling.

**Cross-worker atomics**: commitPending and lastApplied padded to 128B to prevent false sharing. Other fields (failed, snapshotting, lastSnapshotIndex) have specific reader/writer relationships enforced by pipeline topology.

**Channel topology**: `commitC` (step→commit, `CommitCBufferSize`, blocking with stopC escape), `applyC` (commit→apply, `max(CommitCBufferSize*4, 256)`, blocking WITHOUT escape), per-shard channels (apply dispatcher→goroutines, `defaultShardChanBuffer` = 64), `requestC`/`completedC` (snapshot workers), and the listener queue (1024, never blocking). Buffer sizes absorb normal bursts; sustained overload produces backpressure from apply through commit to step.

**Ordered shutdown**: Ensures no in-flight items lost between tiers:
1. `Node.Stop()` closes all stopC channels (unblocks snapshot ops)
2. `close(stepStopC)`, wait for step workers
3. `close(commitStopC)`, commit workers drain remaining commitC, wait
4. `close(applyStopC)`, apply workers drain remaining applyC, wait
5. `close(snapshotStopC)`, wait for snapshot workers
6. Stop the listener dispatcher, wait for it to drain


### End-to-End Data Flow

From client proposal to state machine apply:

```
Client Propose
   ↓
Host.Propose(shardID, cmd)
  • Validate, mint proposalKey, register in ns.pendingProposals
  • node.DeliverProposal(entry) → proposalQ (MPSC)
  • workSignal.Notify() → wakes step worker
   ↓
STEP WORKER (shardID % NumWorkers):
  • Drain proposalQ / inboxQ / readIndexQ → peer.Handle() under raftMu
  • peer.HasUpdate(lastApplied)?
  • peer.GetUpdate() → Update{EntriesToSave, CommittedEntries, Messages, ...}
  • OnLeaderUpdated       → listener dispatcher (if leader changed)
  • OnCampaignLaunched    → listener dispatcher (if vote messages)
  • OnReadyToRead         → inline (if reads confirmed)
  • sendMessages() [FREE-ORDER ONLY: send BEFORE persist]
    - Replicate/Heartbeat/PreVote → sender.Send()
    - InstallSnapshot → sender.SendSnapshot() (chunked streaming)
    - Ordered msgs held on commitItem.orderedMsgs
  • ReadOnly? → skip commit/apply entirely
  • commitPending.Store(true)
  • commitC ← commitItem (blocking, with stopC escape)
   ↓
COMMIT WORKER (same partition):
  • Batch drain (non-blocking collect accumulated items)
  • protoUpdatesToLogDBReuse() → Convert batch
  • logdb.SaveState(batch) [5 attempts; waits 10ms, 50ms, 200ms, 500ms]
  • Send ORDERED msgs — only now is the state they promise durable
  • OnCommitted (if NotifyCommit)
  • For each update:
    - peer.Commit() → Advance peer state, populate termRingBuffer
    - applyC ← applyItem (blocking, unconditional)
   ↓
APPLY DISPATCHER (same partition):
  • Route applyItem to per-shard goroutine
  • Listen for unloadC (shard removal)
   ↓
PER-SHARD GOROUTINE:
  • DecompressEntries() (if Snappy compressed)
  • sem ← (acquire, blocks if MaxApplyWorkers reached)
  • sm.Apply(entries, results) → State machine update
  • ← sem (release)
  • OnApplied callback (deliver results, complete proposals)
  • lastApplied.Store(lastIdx) → Advance watermark
  • Auto-snapshot check (SnapshotEntries > 0?)
  • commitPending.Store(false) [defer] → Return ownership
  • workSignal.Notify() [defer] → Wake step worker
   ↓
Step worker re-processes shard (inner loop)
```

**Error paths**:
- SaveState exhausts retries OR shutdown cancels mid-backoff: `failBatchSaveState` marks node failed, clears commitPending, reports SaveStateError to handleError, fails pending proposals (OnProposalFailed)
- Peer.Commit fails (log gap): commitPending cleared, CommitError to handleError, batch continues
- DecompressEntries fails: fail-fast — `failApply` marks node failed, reports ApplyError, fails pending proposals; lastApplied NOT advanced; step worker skips shard until operator intervention
- SM.Apply fails: fail-fast — same `failApply` path as decompression (shard offline on first error, no retry counting)
- SM.Apply panics: recovered, node failed, SMPanicError to handleError, step worker skips node


### Error Types

| Type                      | Trigger                                 | Effect                    |
|---------------------------|-----------------------------------------|---------------------------|
| `SaveStateError`          | SaveState retries exhausted / shutdown  | node.failed = true        |
| `CommitError`             | Peer.Commit() fails (log gap)           | commitPending cleared     |
| `ApplyError`              | SM.Apply or decompression fails         | node.failed = true        |
| `SMPanicError`            | SM.Apply panics (recovered)             | node.failed = true        |
| `SnapshotDuplicateError`  | Snapshot already in-flight for shard    | Request rejected          |
| `SnapshotPoolBusyError`   | Snapshot pool request channel full      | Request rejected (retry)  |
| `SnapshotError`           | Snapshot save/recover fails             | Reported via callback     |
| `SnapshotTooLargeError`   | Snapshot exceeds MaxSnapshotSize        | Snapshot dir cleaned up   |
| `MessageHandleError`      | Peer.Handle() returns error             | Logged at slog.Warn       |

Sentinels: `ErrShardFailed` (shard taken offline; `SMPanicError` matches it via `errors.Is`. `ApplyError` and `SaveStateError` also set `node.failed` but are distinct typed errors), `ErrStopped` (stopped), `ErrSnapshotCorrupt` (corrupt frame).

`SMPanicError` is the only type that matches `ErrShardFailed`, so an SM panic is the only apply failure a caller outside the module sees as the public `ErrShardFailed`. An `SM.Apply` error that is *returned* rather than panicked produces an `ApplyError`, which carries no `Is` method: it reaches the caller wrapping the state machine's own error, which is the more specific answer. Both fail the shard identically.

### Interfaces

**MessageSender**: `Send(msgs []proto.Message)` (batched protocol messages), `SendSnapshot(msg proto.Message)` (chunked streaming for InstallSnapshot). Non-blocking, concurrent-safe from multiple step workers. Undeliverable messages silently dropped (Raft retransmits).

**Callback**: all methods must be non-blocking and concurrent-safe. Two delivery classes:

*Synchronous, on the producing worker* — these resolve pending client requests and have happens-before requirements against `Peer.Handle`, so they cannot be queued behind user code:

| Method | Producer |
|--------|----------|
| `OnApplied(shardID, entries, results)` | per-shard apply goroutine |
| `OnCommitted(shardID, entries)` | commit worker, only when `NotifyCommit` is on |
| `OnReadyToRead(shardID, readyToRead)` | step worker |
| `OnReadIndexBatched(shardID, batchKey, aliasKeys)` | step worker, BEFORE `Peer.Handle` |
| `OnReadIndexFailed(shardID, keys, err)` | step worker |
| `OnProposalFailed(shardID, entries, err)` | step worker |
| `OnSnapshotCompleted(shardID, index, err)` | snapshot pool worker |
| `OnSnapshotRejected(shardID, replicaID)` | snapshot pool worker |
| `OnCampaignSkipped(shardID, replicaID, term)` | step worker — **NOT EMITTED** (see below) |

`OnCampaignSkipped` is declared on the interface and its doc comment names the step worker as the producer, but nothing in `pkg/internal/engine` calls it: every campaign-refusal branch in `startElection` and `handleTimeoutNow` logs and returns without notifying the engine, so `selfRemoved`, `roleMismatch`, `joining`, an unresolved stale config tail and an unreadable own-log term all produce no event. The Host-side plumbing down to `EventListener.OnCampaignSkipped` is wired and works if the hook is ever called, so implement against it if you like, but do not build monitoring that depends on receiving these events today.

*Via the listener dispatcher* — `OnLeaderUpdated(shardID, leaderID, term)` and `OnCampaignLaunched(shardID, replicaID, term)`. Their queue-full policies differ: a refused `OnLeaderUpdated` leaves the step worker's published-leader bookkeeping uncommitted, so the change is re-published on the next tick and never lost. `OnCampaignLaunched` is a cold path (elections are seconds apart), so a full queue drops it — counted and logged at ERROR — rather than reporting the election at the wrong time later. The election metric is incremented inline either way, so the counter stays exact.

**Applier**: `Apply(entries, results)` — State machine integration. Decouples engine from concrete rsm.StateMachine for testability.

**Snapshotter**: `SaveSnapshot(ctx, writer, stopper)`, `RecoverFromSnapshot(ctx, reader, stopper)`, `LastApplied()`. rsm.StateMachine satisfies both.

**SnapshotBarrier**: AEAD encryption (algorithm-agile: AES-256-GCM or ChaCha20-Poly1305) for snapshot data at rest. Concrete implementation: `seal.Barrier` (`crypto.Barrier` is a type alias for it). Nil means plaintext. `DecryptForEpoch()` supports key rotation, decrypting an old snapshot with its epoch-specific DEK after the epoch has advanced. `PurgeEpochsBefore()` zeros key material for epochs no longer in use.

---

---

## Configuration

Per shard, on `config.Config`. Every value that affects consensus timing is a
multiple of `HostConfig.RTTMillisecond` (200 ms) rather than an absolute
duration, so one number retunes a cluster for its network.

| Field | Default | Effect |
|---|---|---|
| `ElectionRTT` | 10 | election timeout, in RTT multiples |
| `HeartbeatRTT` | 1 | heartbeat interval, in RTT multiples |
| `CheckQuorum` | `*bool`, nil = **enabled** | the leader steps down when quorum becomes unavailable |
| `PreVote` | `*bool`, nil = **enabled** | the pre-vote extension, which stops a partitioned node disrupting a healthy term |
| `OrderedConfigChange` | `*bool`, nil = **enabled** | serialize membership changes |
| `LeaseRead` | `false` | lease-based linearizable reads (PhD §6.4). **Requires `CheckQuorum` and `ElectionRTT >= 5`** — `ErrLeaseReadRequiresCheckQuorum` otherwise. |
| `MaxEntrySize` | 8 MB | per-proposal cap and the decompression ceiling on apply. May not exceed `proto.MaxEntrySize`. |
| `MaxProposalPayloadSize` | 8 MB | user-facing cap; the effective limit at `Propose` is `min(MaxProposalPayloadSize, MaxEntrySize)` |
| `MaxApplyEntrySize` | `MaxReplicationPayloadBytes` = 16,711,672 | batch size per `SM.Update` call |
| `Quiesce` | `false` | stop ticking an idle shard |
| `ZeroCopyEntryCmd` | `false` | `Entry.Cmd` aliases the internal buffer; the state machine must copy to retain it |

The three `*bool` fields are pointers so the **zero value means "unset, use the
default"** rather than "off". A plain `bool` would have made every hand-built
`Config` silently disable `CheckQuorum` and `PreVote`. Use `config.BoolPtr(false)`
to disable deliberately.

Per host, on `config.HostConfig`:

| Field | Default | Effect |
|---|---|---|
| `RTTMillisecond` | 200 | the unit every timeout above is expressed in |
| `NumWorkers` | `runtime.NumCPU()` | goroutines per tier (step, commit, apply) |
| `MaxApplyWorkers` | `runtime.NumCPU()` | apply semaphore capacity |
| `CommitCBufferSize` | 64 | commit channel buffer per worker |
| `MaxTotalInMemLogSize` | 0 = disabled | global in-memory-log rate limiter threshold |
| `NotifyCommit` | `false` | fire `OnCommitted` after the WAL persist, before apply |
| `ShutdownTimeout` | 30s | graceful shutdown budget |

**There is no apply-retry setting**, and adding one would be a defect: the apply
path is fail-fast, and a shard is marked failed on the **first** `SM.Apply`
error with no retry counting.

## Test Strategy

Targets: `make test-raft`, `make test-engine`, `make test-queue`,
`make test-invariant`, `make test-server`, `make bench-raft`, `make bench-engine`,
`make fuzz-raft`, `make integration-test-raft`,
`make integration-test-linearizability`, `make ci-netfault`.

| Layer | Covers |
|---|---|
| **Unit** (`pkg/internal/raft`) | role transitions, elections, replication, commit calculation, ReadIndex, quiescence, leader transfer, the remote state machine |
| **Unit** (`pkg/internal/engine`) | the three worker tiers, the ready map, panic recovery, ordered shutdown, the listener dispatcher |
| **Unit** (`pkg/*_test.go`) | the host-level regression suites — `host_hardening_regression_test.go`, `host_medlow_regression_test.go`, `host_review12_test.go`, `host_review16_test.go`, `messagehandler_*_test.go` |
| **Fuzz** (`make fuzz-raft`) | message handling against arbitrary inputs |
| **Integration** (`test/integration`) | real multi-node clusters, elections under real transport |
| **Linearizability** (`make integration-test-linearizability`) | that observed histories are actually linearizable — the only check that can falsify the whole feature |
| **Fault injection** (`test/netfault`, `make ci-netfault`) | partitions, delays and drops; where split-brain and spurious-election bugs surface |

**What a change here must not break**, and where the test lives:

1. **Election Safety** — at most one leader per term.
2. **Leader Append-Only** — a leader never overwrites or deletes its own entries.
3. **Log Matching** — same index and term implies identical prefixes.
4. **Leader Completeness** — an entry committed in a term is present in every
   later leader's log.
5. **State Machine Safety** — no two replicas apply different entries at the
   same index.

These are the five Raft safety properties. A test that fails on any of them is
never "flaky".

**Fault-injection seams**: `WithoutTransport()` for a single-node engine,
`memdb` for a store with no durability, and the netfault harness at the socket.
There is deliberately no injectable clock — timing is expressed in RTT multiples
and driven by the tick, so a test controls timing by controlling ticks.

**A test must never use `time.Sleep` to wait for consensus.** Use the request
completion channels, the listener notifications, or the diagnostic accessors.

**Coverage note.** `pkg/internal/engine` sits at 89.1%, below the repository's
own `COVER_MIN = 91`, and the module-total gate does not catch it. `README.md`
names the engine pipeline, the WAL and the QUIC transport as the gaps that
matter; treat it as tracked work rather than a settled state.

## Observability

**Metrics** — the `RaftMetrics` collector, exposed through the host's
`MetricsCollector`. Names and the Prometheus implementation are in
[`../observability/engineering.md`](../observability/engineering.md).

The signals that matter, and what each one actually tells you:

| Signal | Reading |
|---|---|
| leader changes per shard | more than a trickle means elections are being triggered, usually by heartbeat delay rather than by node failure |
| commit latency | the step → commit → apply pipeline including the WAL fsync |
| apply latency | embedder code; the one number this library does not control |
| proposal queue depth | back-pressure before it becomes `ErrBusy` |
| in-memory log size against `MaxTotalInMemLogSize` | a follower falling behind |
| shards in quiesce | expected on an idle cluster |

**Diagnostics** — `Host` exposes four accessors that exist for incident
response rather than for monitoring, and they are worth knowing about before you
need them: `DiagRaftState`, `DiagPipelineState`, `DiagShardGates` and
`DiagRemotes`. They report term, commit and processed indexes, queue depths, the
recovering/saving gates and per-remote replication state.

**Logging** — `log/slog`. Worth alerting on:

| Level | Event | Why |
|---|---|---|
| ERROR | a shard marked failed after an apply error | that shard has stopped; there is no retry |
| ERROR | a worker panic and restart | the shard's state after a panic mid-apply is the state machine's business, not recoverable here |
| ERROR | `LoopPanicError` | as above, surfaced to the embedder |
| WARN | leadership lost, campaign launched | normal in isolation, a signal in volume |
| WARN | proposal rejected — `ErrBusy`, `ErrDiskFull`, not-leader | back-pressure or a misrouted client |

**No proposal payload is ever logged.** It is application data.

**User-visible notifications** go through `config.EventListener` on a dedicated
dispatcher goroutine: `OnLeaderUpdated`, `OnCampaignLaunched`, shard failure,
and the `LeaderInfo` / `ShardFailedInfo` / `SnapshotInfo` / `ConnectionInfo` /
`DiskInfo` / `EventShardInfo` payloads. The dispatcher holds no engine lock, so a
listener may call any `Host` API — including the synchronous `StopShard`.

**There is deliberately no ordering guarantee** between dispatched
notifications and the callbacks that stay synchronous (`OnApplied`,
`OnCommitted`, `OnReadyToRead`, `OnProposalFailed`, `OnReadIndexBatched`,
`OnReadIndexFailed`): those resolve pending client requests and have
happens-before requirements against `Peer.Handle`, so they cannot queue behind
user code.

## Related Documents

- `docs/engine/pipeline.md` — step/commit/apply workers, snapshot pool.
- `docs/protocol/wire-format.md` — `proto.Entry`, `proto.Message`,
  `proto.Membership` encodings.
- `docs/storage/wal.md` — LogDB and the WAL record layout.
- `docs/sessions/deduplication.md` — client sessions in full.
- `docs/discovery/bootstrap.md` — shard bootstrap and joining.
- `docs/api/reference.md` — the public `Host` API.
