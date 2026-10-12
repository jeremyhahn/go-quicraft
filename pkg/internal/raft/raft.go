// Copyright 2026 Jeremy Hahn
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package raft

import (
	"crypto/rand"
	"encoding/binary"
	"fmt"
	"log/slog"
	"math"
	mathrand "math/rand"
	"slices"
	"time"

	"github.com/jeremyhahn/go-quicraft/pkg/config"
	"github.com/jeremyhahn/go-quicraft/pkg/internal/invariant"
	"github.com/jeremyhahn/go-quicraft/pkg/internal/server"
	"github.com/jeremyhahn/go-quicraft/pkg/proto"
)

// roleState represents the role of a raft node.
type roleState uint8

const (
	follower     roleState = iota
	candidate    roleState = iota
	preCandidate roleState = iota
	leader       roleState = iota
)

// roleStateNames provides human-readable names for role states.
var roleStateNames = [...]string{
	follower:     "Follower",
	candidate:    "Candidate",
	preCandidate: "PreCandidate",
	leader:       "Leader",
}

// String returns the human-readable name of the role state.
func (s roleState) String() string {
	if int(s) < len(roleStateNames) {
		return roleStateNames[s]
	}
	return fmt.Sprintf("Unknown(%d)", s)
}

// maxVotingMembers is the maximum number of voting members (remotes +
// witnesses + self) supported by the ReadIndex ackBits bitfield. The
// bitfield is a uint64, so positions 0..63 are available.
// maxAdoptableTerm is the exclusive upper bound on a term this replica will
// adopt from the wire. Terms at or above it are refused.
//
// The headroom exists so that r.term++ in becomeCandidate can never wrap.
// Nothing bounds Term in the wire encoding — it is a plain uint64 — so without
// this a peer naming a term near the ceiling either wraps this node's term to 0
// on its next campaign (a term REGRESSION: it can then vote a second time in a
// term it already voted in) or parks it at a term it can never advance past,
// removing a voter permanently. One message, either way.
//
// It is proto.MaxTerm under this package's name: the same bound governs
// snapshot admission (proto.MaxLogIndex is its index twin), and the two must
// not drift.
//
// RESIDUAL, accepted: a term ONE below the bound is adopted, and the campaign
// it leads to — at exactly the bound — is refused by every peer, so a shard
// driven to maxAdoptableTerm-1 can never elect again. No bound avoids this;
// wherever the line is drawn, the term just below it has no campaignable
// successor. What matters is who can draw the shard there: only a BOUND sender
// — a member the configuration names, on its own keypair — since the receive
// gate admits nothing else, and a member is trusted for liveness by Raft's own
// assumptions (it can equally stop voting or stop responding). A term reached
// by running is ~2^64 elections away. The bound therefore closes what an
// unauthenticated or unnamed peer could do — wrap a term, or strand a voter —
// and leaves what a compromised member can do where Raft leaves it.
const maxAdoptableTerm = proto.MaxTerm

// persistedCeiling is the exclusive upper bound on a term or log index this
// replica will OPEN with, and it is deliberately higher than the admission
// bound: state that was admitted just below proto.MaxTerm / proto.MaxLogIndex
// can still move by local transitions — becomeCandidate's increment stops at
// maxAdoptableTerm, becomeLeader appends its no-op at lastIndex+1, and
// proposals are refused from proto.MaxLogIndex on (handleLeaderPropose) — so
// an open check at the admission bound would refuse state the replica itself
// produced legitimately and turn a restart into a permanent failure. What the
// open check must refuse is only what the arithmetic cannot survive: a value
// within a few of math.MaxUint64, where +1 wraps to 0. Everything the local
// transitions can reach from admitted state sits far below it.
const persistedCeiling = math.MaxUint64 - 8

// MaxVotingMembers is the maximum number of voting members (remotes +
// witnesses + self) a shard supports, bounded by the ReadIndex ackBits
// bitfield: it is a uint64, so positions 0..63 are available.
//
// Exported because the cap has to be enforced at ADMISSION, and the bootstrap
// path lives in package quicraft. The config-change validators
// (validateAddNode, validateAddWitness) do not run on the bootstrap path, so
// without an admission check StartShard would accept a 65-member map, commit it
// as the EntryInitialMembership at index 1, and the shard would then fail in
// becomeLeader on every election. The membership is durable, so the shard could
// never elect a leader again, across restarts.
//
// Do NOT add this check to applyMembershipEntry or the config-change handlers:
// refusing an already-committed entry there makes replicas diverge, which is
// what the NOTE in membership.go's handleAddNode warns about. Admission is the
// only place it can be enforced safely.
const MaxVotingMembers = 64

// maxVotingMembers is the unexported spelling used throughout this package.
const maxVotingMembers = MaxVotingMembers

// maxInflightMsgs is the default maximum number of in-flight append
// messages per remote peer.
const maxInflightMsgs = 256

// maxPendingReadIndex caps the number of in-flight ReadIndex requests
// awaiting heartbeat quorum confirmation. This prevents unbounded memory
// growth when heartbeat responses are lost or delayed. When reached, new
// ReadIndex requests are rejected with ErrReadIndexOverloaded.
const maxPendingReadIndex = 4096

// leaseDriftDivisor bounds the relative clock-rate drift the read lease
// tolerates: the lease is shortened by 1/leaseDriftDivisor (1%) of its
// tick-derived length. The lease compares the leader's monotonic clock
// against the followers' election timers, which run on their own clocks,
// so a leader clock running slow or a follower clock running fast would
// otherwise let the lease outlive a follower's vote-denial window. The
// kernel bounds CLOCK_MONOTONIC slew to 500 ppm (0.05%), so 1% covers the
// combined drift of both ends with a wide margin; it does NOT cover a
// clock that stops, which is why the lease is anchored at send time.
//
// It is the default; Config.MaxClockDrift replaces it per shard
// (leaseDriftPPMFor). The bound is exact: the shortest promise the leader
// accepts (ackBacksLease) is leaseSpan itself, and a follower promising
// exactly that, with clock rates that differ by more than the bound, frees
// its vote while the lease it backs still holds (lease_clock_rate_test.go).
const leaseDriftDivisor = 100

// partsPerMillion is the scale of leaseDriftPPM.
const partsPerMillion = 1_000_000

// leaseDriftPPMFor converts Config.MaxClockDrift, a fraction, into the parts
// per million the lease is shortened by. Zero selects the default bound,
// 1/leaseDriftDivisor. Config.Validate rejects a drift that is not finite or
// lies outside [config.MaxClockDriftMin, config.MaxClockDriftMax].
func leaseDriftPPMFor(drift float64) uint64 {
	if drift == 0 {
		return partsPerMillion / leaseDriftDivisor
	}
	return uint64(math.Round(drift * partsPerMillion))
}

// leaseDriftShortfall is d*ppm/partsPerMillion rounded up, so the lease errs
// short, computed without overflowing for any d a tick count can produce.
// A ppm of partsPerMillion or more consumes the whole span.
func leaseDriftShortfall(d time.Duration, ppm uint64) time.Duration {
	if ppm >= partsPerMillion {
		return d
	}
	q, rem := uint64(d)/partsPerMillion, uint64(d)%partsPerMillion
	return time.Duration(q*ppm + (rem*ppm+partsPerMillion-1)/partsPerMillion)
}

// leaseRoundWindow is how many recent heartbeat rounds the leader remembers
// the send time of (a power of two, so a round's slot is round &
// leaseRoundMask). A new round opens at most once per leaseRoundSlices-th
// of the lease (see nextLeaseRound), so every round sent within the last
// leaseDuration is always still in the window; an ack for an older round
// could not extend the lease anyway and is simply not counted.
const (
	leaseRoundWindow = 32
	leaseRoundMask   = leaseRoundWindow - 1
	leaseRoundSlices = leaseRoundWindow / 2
)

// maxReadStates caps the number of resolved ReadIndex results that have
// not yet been consumed by GetUpdate or DrainReadStatesOnly. If ReadIndex
// requests resolve faster than the step worker drains them (e.g., under
// sustained lease-read load with a stalled pipeline), this cap prevents
// unbounded memory growth. When reached, new ReadIndex requests are
// rejected with ErrReadIndexOverloaded.
const maxReadStates = 4096

// noLeader indicates no known leader.
const noLeader uint64 = 0

// noNode indicates no specific node.
const noNode uint64 = 0

// localControlMessages is the set of message types that the local process
// injects into step() without a term: the engine builds Propose and
// ReadIndex, the message handler builds InstallSnapshot and SnapshotStatus,
// the host builds LeaderTransfer, and Unreachable is a transport-local
// notification. They are the only types allowed past the Term != 0 gate in
// step(), and ONLY when the message is stamped proto.OriginLocal — the
// origin is process-local state the wire format cannot express (see
// proto.MessageOrigin), so the exemption is unreachable from the network.
var localControlMessages = map[uint64]struct{}{
	proto.Propose:         {},
	proto.ReadIndex:       {},
	proto.InstallSnapshot: {},
	proto.SnapshotStatus:  {},
	proto.Unreachable:     {},
	proto.LeaderTransfer:  {},
}

// processLocalMessages is the subset of localControlMessages that is NEVER
// legitimately received from a peer, at any term:
//
//   - InstallSnapshot travels as a SnapshotHeader + chunk stream, not as a
//     message (stepWorker.sendMessages routes it to sender.SendSnapshot);
//     the follower's own snapshot receive path rebuilds the message locally
//     once the artifact is on disk and verified.
//   - SnapshotStatus is the transport reporting the outcome of a transfer it
//     performed, to the raft instance that started it.
//   - Unreachable is a transport-local reachability notification.
//   - LeaderTransfer is only ever produced by this host's own API; nothing
//     forwards it between replicas.
//
// Accepting any of them from the wire hands an authenticated peer a message
// that no term, log, or quorum check constrains: a wire InstallSnapshot
// alone carries a full Membership — the very set quorum() is computed over —
// plus a Filepath the receiver would restore from. Propose and ReadIndex are
// deliberately NOT in this set: followers legitimately forward both to the
// leader, and those forwards carry the sender's term.
var processLocalMessages = map[uint64]struct{}{
	proto.InstallSnapshot: {},
	proto.SnapshotStatus:  {},
	proto.Unreachable:     {},
	proto.LeaderTransfer:  {},
}

// isLocalControlMessage reports whether a message type is locally injected
// and therefore exempt from the Term != 0 requirement.
func isLocalControlMessage(msgType uint64) bool {
	_, ok := localControlMessages[msgType]
	return ok
}

// isLocallyInjectedControlMessage reports whether msg is one this process built
// and injected itself, and is therefore exempt from the Term != 0 requirement.
//
// BOTH conditions are required. The exemption is keyed on the ORIGIN as well as
// the type: a wire Propose or ReadIndex forwarded by a follower carries that
// follower's term and must go through the normal term comparisons, while a
// term-less one can only have been built in this process.
func isLocallyInjectedControlMessage(msg proto.Message) bool {
	return msg.Origin.IsLocal() && isLocalControlMessage(msg.Type)
}

// isProcessLocalMessage reports whether a message type may only ever be
// injected in-process, never received from a peer.
func isProcessLocalMessage(msgType uint64) bool {
	_, ok := processLocalMessages[msgType]
	return ok
}

// readIndexStatus tracks a pending read index request. Heartbeat acks
// are tracked with a uint64 bitfield instead of a map for zero-allocation
// on the hot path. Replica IDs are mapped to bit positions 0..N via
// raft.replicaToBit, built once in becomeLeader.
type readIndexStatus struct {
	// key is the client correlation key.
	key uint64
	// index is the committed index at the time of the request.
	index uint64
	// from is the replica ID of the originating follower, or 0 for local.
	from uint64
	// ctx is the heartbeat context used to correlate heartbeat responses.
	ctx uint64
	// ackBits is a bitfield where bit i is set when the replica mapped to
	// position i (via raft.replicaToBit) has acknowledged the heartbeat.
	ackBits uint64
	// ackCount is the number of unique acks received (popcount of ackBits).
	ackCount int
}

// raft is the core Raft state machine. It is a pure function of its
// inputs: messages in, messages out. It performs no I/O; the engine
// is responsible for persisting state and sending messages.
//
// The raft struct is owned by a single goroutine and requires no
// synchronization.
type raft struct {
	shardID   uint64
	replicaID uint64
	term      uint64
	vote      uint64
	log       *raftLog
	state     roleState
	leaderID  uint64

	// Tick counters.
	electionTick  int
	heartbeatTick int

	// Timeouts.
	electionTimeout           int
	heartbeatTimeout          int
	randomizedElectionTimeout int

	// Peer tracking.
	remotes   map[uint64]*remote
	observers map[uint64]*remote
	witnesses map[uint64]*remote

	// snapshotSeq is a monotonically increasing identifier for snapshot
	// transfer attempts started by this raft instance. Stamped on each
	// InstallSnapshot handed to the transport (msg.Hint) and echoed back
	// in the SnapshotStatus report, so a status can be matched to the
	// exact attempt it describes (see remote.snapshotSeq).
	snapshotSeq uint64

	// maxWitnesses and requireFullReplicaMajority mirror the shard's
	// REPLICATED witness policy (proto.Membership) for the leader-side
	// propose-time policy check (validateConfigChangePolicy). Seeded from
	// the membership at newRaft and re-synced by the peer after every
	// config-change apply and snapshot restore. Enforced only before log
	// append on the leader — never on the deterministic apply/replay
	// path — so a forwarded config change that no node pre-validated
	// (the follower forward path bypasses the Host API's
	// ValidateConfigChange) still cannot commit a policy-violating
	// membership.
	maxWitnesses               uint64
	requireFullReplicaMajority bool

	// Messages to send. Double-buffered: msgs accumulates outbound
	// messages, msgsBuf holds the alternate buffer for swap in
	// drainMessages to avoid allocation.
	msgs    []proto.Message
	msgsBuf []proto.Message

	// freeMsgBuf is a reusable scratch buffer for drainFreeOrderMessages.
	// It avoids a slice allocation on every commitPending tick. The caller
	// consumes the returned slice before the next tick, so no copy is
	// needed.
	freeMsgBuf []proto.Message

	// Read index state. readIndexCtx is pre-incremented before use so
	// the first ReadIndex ctx is 1, reserving ctx=0 for regular heartbeats
	// (broadcastHeartbeat). This prevents regular heartbeat responses from
	// falsely satisfying ReadIndex quorum checks.
	readStates       []proto.ReadyToRead
	pendingReadIndex []readIndexStatus
	readIndexCtx     uint64

	// readIndexBroadcastPending is set when handleLeaderReadIndex defers
	// a heartbeat broadcast. Multiple ReadIndex requests in the same step
	// cycle share a single heartbeat round. flushReadIndex sends the
	// broadcast and clears this flag.
	readIndexBroadcastPending bool

	// replicaToBit maps replica IDs to bit positions (0..63) for the
	// readIndexStatus bitset. Built in becomeLeader and rebuilt by
	// rebuildReplicaToBit when membership changes during leadership.
	// Clusters are limited to <=64 voting members in practice.
	replicaToBit map[uint64]uint8

	// Pre-allocated sort buffer for commit calculation.
	matched []uint64

	// Feature flags.
	checkQuorum bool
	preVote     bool

	// isObserver is this replica's EFFECTIVE non-voting role. It is seeded
	// from config.IsObserver, which is only an initial hint: it governs while
	// the configuration says nothing about this replica (a joiner that has not
	// yet learned the entry adding it), and from then on the replicated
	// membership decides. checkSelfRole re-derives it wherever the
	// configuration is (re)established, so an observer whose own promotion
	// (AddNode for its replica ID) commits becomes a voter in place — it
	// campaigns, promises vote windows and serves ReadIndex — with no restart,
	// as etcd's learner promotion does. See checkSelfRole for why the observer
	// role may follow the configuration while the witness role may not.
	isObserver bool
	// isWitness is the LOCALLY configured witness role (config.IsWitness). It
	// is never re-derived: a witness keeps no state machine, so the role
	// decides what this replica physically holds, and a disagreement with the
	// replicated membership is latched as roleMismatch instead.
	isWitness bool

	// selfRemoved records that this replica is not part of the current
	// configuration: it is absent from the membership's voters, observers
	// and witnesses. Maintained by setSelfMembership from every point where
	// the configuration is (re)established — construction, snapshot restore,
	// and commit-time config change application.
	//
	// The raft struct's remotes/observers/witnesses maps deliberately exclude
	// self (quorum() adds the +1), so membership of self cannot be derived
	// from them the way dragonboat's selfRemoved() derives it from r.remotes.
	// This flag is that missing information.
	//
	// A self-removed replica must never campaign (Raft thesis §4.2.2). The
	// surviving configuration computes its quorum without this replica, so a
	// removed replica that counts its own vote against its own stale voting
	// set can "win" a term the real configuration also elects a leader in.
	selfRemoved bool

	// roleMismatch is set when this replica's LOCALLY configured witness
	// role contradicts the REPLICATED membership. It is the witness
	// analogue of selfRemoved: a safety latch, derived (never proposed)
	// from the configuration, re-evaluated everywhere the configuration is
	// (re)established. The observer role is not latched here: it follows
	// the configuration instead (see isObserver).
	//
	// The two roles run different data paths: a leader sends witnesses
	// metadata-only entries (sendAppend strips the payload). A replica the
	// configuration lists as a witness but that was started with
	// IsWitness=false therefore applies stripped entries into a full state
	// machine, while every local gate keyed on the config — propose, read,
	// campaign — believes it is a full replica. It can then win an election
	// and serve linearizable reads out of a state machine that never saw
	// the data. dragonboat fails closed on exactly this disagreement
	// (addNode/addWitness panic when the replicated role contradicts the
	// local one); this latch is the non-fatal equivalent: no campaigning,
	// no reads, until the two agree.
	roleMismatch bool

	// joining mirrors Peer.joining into the state machine. A replica started
	// with join=true holds an INCOMPLETE membership — the existing cluster was
	// bootstrapped through a confState that is not in the log, so this replica
	// may know only itself — and a campaign run against that voting set wins
	// with a quorum of one, in a term the real configuration is also electing
	// in. It is therefore a campaign gate of exactly the same kind as
	// selfRemoved and roleMismatch, consulted in the same two places
	// (startElection and handleTimeoutNow), and set from the same authority
	// that owns the membership.
	//
	// It gates CAMPAIGNING ONLY. The election clock keeps ticking (see
	// Peer.Tick): freezing electionTick left leaderID permanently "active",
	// which made handleVoteRequest deny every PreVote and dropHighTermVote
	// discard every higher-term RequestVote, so a joining node that had caught
	// up by pure log replay silently withheld its vote forever and could hold a
	// healthy quorum leaderless.
	//
	// Cleared when a snapshot delivers the full cluster membership (Peer.Handle).
	joining bool

	// Lease-based read fields (PhD Section 6.4). When leaseRead is
	// enabled, the leader can serve ReadIndex requests without a
	// heartbeat round-trip while a quorum of voting members has acked a
	// heartbeat round SENT less than leaseDuration ago, measured on the
	// monotonic clock (see hasValidLease). Every heartbeat broadcast
	// carries a round number in LogIndex that the follower echoes back;
	// leaseRoundSent remembers when recent rounds were sent, and
	// remoteLastAck holds, per voting member, the send time of the newest
	// round it acked. The lease is measured in real time rather than in
	// ticks because a descheduled leader's ticker coalesces the ticks it
	// missed, so a tick count cannot see a pause; elapsed time can, and a
	// pause can only shorten a lease anchored at send time.
	leaseRead      bool                 // config: enable lease reads
	leaseTimeout   int                  // lease length in ticks (electionTimeout - 2)
	leaseDuration  time.Duration        // leaseTimeout ticks in real time less the drift bound; 0 = no lease
	leaseSpan      time.Duration        // leaseTimeout ticks in real time: the vote window an ack must promise
	now            func() time.Time     // monotonic clock; time.Now outside tests
	leaseRound     uint64               // newest heartbeat round issued; 0 = none
	leaseRoundSent []time.Time          // ring of recent round send times, indexed by round & leaseRoundMask
	remoteLastAck  map[uint64]time.Time // replicaID -> send time of the newest round it acked

	// Vote hold-down (see voteHeldDown). A restarted replica has forgotten
	// its election timer, so without this it could grant a vote the moment
	// it comes back while a leader still counts its pre-restart ack toward
	// a live read lease. It therefore neither grants a vote nor campaigns
	// until voteHoldDuration has elapsed on its monotonic clock since
	// voteHoldStart, the instant it was constructed: the longer of the
	// window a previous incarnation may still owe (voteWindowOwed, as
	// recovered) and the window this one promises (voteWindow).
	voteHoldArmed    bool          // a hold-down applies: LeaseRead, or a window is owed
	voteHoldStart    time.Time     // construction time: the window's anchor
	voteHoldDuration time.Duration // the hold-down's length; 0 = not yet known
	voteHoldOver     bool          // latched once the window has elapsed
	electionSpan     time.Duration // electionTimeout ticks in real time; 0 = tick interval unknown

	// The live vote promise (see votePromised). Every heartbeat ack this
	// replica sends promises its leader a vote window from the moment it
	// acks; until promiseUntil it grants no vote or pre-vote and starts no
	// campaign, whatever happens to its term, its known leader or its
	// election timer meanwhile. promisedTo and promisedTerm name the leader
	// the newest promise was made to, which alone may release it by
	// transferring leadership.
	promiseUntil time.Time
	promisedTo   uint64
	promisedTerm uint64

	// timeoutNowSent records that this leader has sent a TimeoutNow in its
	// current term. From then on it takes no read lease for the rest of the
	// term (resumeLeaseAfterTransfer): the message can be delayed without
	// bound, and on arrival it — and its candidate's transfer-hinted votes —
	// release the vote promises made to this leader in this term, including
	// ones made after a transfer that timed out. Cleared by becomeLeader.
	timeoutNowSent bool

	// voteWindowOwed is the persisted VoteWindow of the hard state: the
	// longest vote window this replica may have promised in a heartbeat ack
	// and not yet outlived, in nanoseconds. It is raised to the current
	// promise before any ack carries it, and lowered to it once the
	// hold-down has outlived every earlier promise. See voteWindow.
	voteWindowOwed uint64

	// pendingConfigChange tracks whether a config change entry has been
	// appended but not yet committed. Per PhD thesis 4.1, only one
	// config change may be pending at a time to prevent safety issues.
	pendingConfigChange bool

	// unreadableLogVoteDenials counts votes denied because this replica's OWN
	// last log term could not be read, rather than because the candidate was
	// behind. It is a storage fault that denies every candidate while it lasts,
	// so a nonzero value on a majority explains a shard that cannot elect
	// despite quorum being up. Surfaced by DiagRaftState and by the
	// rate-limited warning in recordUnreadableLogVoteDenial.
	unreadableLogVoteDenials uint64

	// lastUnreadableLogWarn rate-limits that warning. A vote storm would
	// otherwise emit one line per RequestVote per candidate.
	lastUnreadableLogWarn time.Time

	// Leader transfer state.
	transferTarget  uint64
	transferElapsed int

	// Vote tracking for elections.
	votes map[uint64]bool

	// electionStartTick records the electionTick value when an election
	// started. Used to compute election duration for debug metrics.
	electionStartTick int

	// Quiesce.
	quiesce quiesceState

	// maxReplicationPayload bounds the total entry bytes in a single
	// outbound Replicate message. Set from Config.MaxApplyEntrySize, whose
	// default is config.MaxReplicationPayloadBytes (16711672 bytes, ~15.94MB
	// -- the largest entry payload that still fits one wire frame);
	// Config.Validate rejects anything larger. Zero means unlimited (not
	// recommended).
	//
	// This is the ONLY thing Config.MaxApplyEntrySize is wired to. Despite its
	// name it does NOT size the apply batch: entriesToApply is bounded by the
	// fixed, non-configurable constant maxEntriesToApplySize (64MiB, log.go).
	// The two are independent bounds on different things -- this one on an
	// outbound wire message, that one on an inbound batch handed to the state
	// machine -- and neither derives from the other.
	maxReplicationPayload uint64

	// entryCompression indicates whether entry payloads should be
	// compressed using Snappy before appending to the log. When true,
	// the leader compresses Cmd payloads in handleLeaderPropose.
	entryCompression bool

	// rl is the per-shard in-memory rate limiter. Nil when rate limiting
	// is disabled (MaxInMemLogSize == 0).
	rl *server.InMemRateLimiter

	// rateLimitTick counts ticks between rate limit feedback messages.
	// Followers send their in-memory log size to the leader every
	// election timeout.
	rateLimitTick int

	// PRNG for election jitter, seeded from crypto/rand.
	rng *mathrand.Rand

	// onCampaign is called when an election starts. Set by Peer. Nil-safe.
	onCampaign func(shardID, replicaID, term uint64)

	// resolveStaleConfig folds the committed configuration tail this replica
	// recovered with into the membership and reports whether the configuration
	// is now current. Set by NewPeer; see Peer.staleConfigThrough.
	//
	// It is NIL on a bare raft, which is the correct answer there rather than a
	// missing check: membership under config-in-log is owned by the Peer, and a
	// raft constructed without one has no recovered base to be stale relative
	// to and no tail to fold. Nil therefore means "no gate", never "assume
	// stale" — assuming stale is the shape that suppresses elections outright.
	resolveStaleConfig func() bool

	// validateConfigChange is the shard's FULL committed-membership validator
	// (membership.validate), installed by NewPeer. It is the propose-time gate
	// for a config change that arrives as a wire-forwarded Propose, which
	// bypasses the Host API's ValidateConfigChange and Peer.ProposeConfigChange
	// entirely — the two places the identical rules already ran.
	//
	// It must run HERE, at the leader's last gate before the entry enters the
	// log, and nowhere later: the apply path deliberately never re-validates
	// (membership.apply CONVERGES so a committed change replays identically on
	// every replica), so an entry that gets past this point is applied by the
	// whole shard whatever it says. A duplicate identity is the sharpest case —
	// two replica IDs bound to one keypair are both satisfiable by one physical
	// node, so it holds two votes and two match indices in a quorum that
	// believes they are independent, and quorum intersection is gone.
	//
	// Nil on a bare raft (the state-machine unit tests, the adversarial
	// harness), which has no membership at all; validateConfigChangePolicy is
	// the fallback there and enforces the subset of rules raft can answer from
	// its own remote/witness tracking. It is deliberately NOT run alongside the
	// full validator: membership.validate is a strict superset of it, and
	// running both would be the same rule stated in two places, free to drift.
	validateConfigChange func(cc proto.ConfigChange) error

	// Function pointers set on role transition for zero-branch dispatch.
	stepFunc func(r *raft, msg proto.Message) error
	tickFunc func(r *raft)

	// leaseDriftPPM is the clock-rate drift bound, in parts per million, the
	// lease is shortened by (Config.MaxClockDrift, or 1/leaseDriftDivisor).
	// Read only when the tick interval is set (setTickInterval), so it sits
	// last, clear of the fields the step and tick paths touch.
	leaseDriftPPM uint64
}

// newRaft creates a new raft state machine with the given configuration
// and log reader. The node starts as a follower.
func newRaft(cfg config.Config, logdb LogReader) (*raft, error) {
	// Seed the PRNG from crypto/rand.
	var seedBytes [8]byte
	if _, err := rand.Read(seedBytes[:]); err != nil {
		return nil, ErrEntropyUnavailable
	}
	seed := int64(binary.LittleEndian.Uint64(seedBytes[:]))

	// Refuse to open persisted state the protocol cannot do arithmetic on.
	// Every admission path bounds these before they are written (see
	// proto.MaxTerm), so a value here means data from a build that did not,
	// or a damaged store; either way the only remedy is wipe and resync, and
	// the shard must fail to start rather than run at the ceiling.
	if err := checkPersistedBounds(logdb); err != nil {
		return nil, err
	}

	rl := newRaftLog(logdb)
	state, membership := logdb.NodeState()

	r := &raft{
		shardID:               cfg.ShardID,
		replicaID:             cfg.ReplicaID,
		log:                   rl,
		checkQuorum:           cfg.GetCheckQuorum(),
		preVote:               cfg.GetPreVote(),
		isObserver:            cfg.IsObserver,
		isWitness:             cfg.IsWitness,
		leaseRead:             cfg.LeaseRead,
		leaseTimeout:          int(cfg.ElectionRTT) - 2,
		leaseDriftPPM:         leaseDriftPPMFor(cfg.MaxClockDrift),
		now:                   time.Now,
		electionTimeout:       int(cfg.ElectionRTT),
		heartbeatTimeout:      int(cfg.HeartbeatRTT),
		maxReplicationPayload: cfg.MaxApplyEntrySize,
		entryCompression:      cfg.EntryCompressionType == config.Snappy,
		remotes:               make(map[uint64]*remote),
		observers:             make(map[uint64]*remote),
		witnesses:             make(map[uint64]*remote),
		votes:                 make(map[uint64]bool),
		quiesce:               newQuiesceState(cfg.Quiesce, int(cfg.ElectionRTT)),
		rng:                   mathrand.New(mathrand.NewSource(seed)),
	}
	if r.leaseRead {
		r.leaseRoundSent = make([]time.Time, leaseRoundWindow)
	}

	// Initialize remotes from membership.
	r.initRemotes(membership)
	// Refuse to build a replica whose recovered configuration does not fit the
	// ReadIndex ackBits word, rather than building it and panicking later.
	//
	// This is the DURABLE form of the fault and the one an operator cannot
	// escape on their own. StartShard and ImportSnapshot bound the member map
	// at admission, but a configuration that got past them — through a build
	// without those checks, or through a snapshot accepted before
	// handleSnapshot gained its gate — is written to LogDB and recovered here
	// on every start. As an assertion downstream it panicked the node each
	// time, so the shard could not be started, could not be removed through a
	// running host, and took every other shard on that step worker with it.
	// Returning here fails exactly this shard: StartShard reports the error,
	// the host stays up, and the operator can remove or repair the replica.
	if n := r.votingBitCount(); n > maxVotingMembers {
		slog.Error("refusing to start shard: recovered membership exceeds the ReadIndex ackBits word",
			"shard", cfg.ShardID,
			"replica", cfg.ReplicaID,
			"voting_members", n,
			"max_voting_members", maxVotingMembers,
			"remotes", len(r.remotes),
			"witnesses", len(r.witnesses))
		return nil, ErrTooManyVotingMembers
	}
	// Seed the replicated witness-policy mirror from the recovered (or
	// bootstrap-seeded) membership; kept in sync by the peer thereafter.
	r.maxWitnesses = membership.MaxWitnesses
	r.requireFullReplicaMajority = membership.RequireFullReplicaMajority

	// Pre-allocate matched buffer and double-buffered msgs slices. The buffer
	// holds self + every remote + every witness (witnesses are counted toward
	// the unified commit quorum in tryCommit).
	r.matched = make([]uint64, 0, len(r.remotes)+len(r.witnesses)+1)
	msgCap := len(r.remotes) + len(r.observers) + len(r.witnesses)
	r.msgs = make([]proto.Message, 0, msgCap)
	r.msgsBuf = make([]proto.Message, 0, msgCap)

	// Restore persisted state.
	if state.Term > 0 {
		r.term = state.Term
		r.vote = state.Vote
	}
	r.armVoteHoldDown(state.VoteWindow)
	// Start as follower.
	r.becomeFollower(r.term, noLeader)

	// Wire rate limiter after raft struct creation to avoid name collision
	// with the raftLog local variable 'rl'.
	if cfg.MaxInMemLogSize > 0 {
		r.rl = server.NewInMemRateLimiter(cfg.MaxInMemLogSize)
		r.log.inmem.rl = r.rl
	}

	return r, nil
}

// checkPersistedBounds refuses recovered state whose term or log position
// lies at or above proto.MaxTerm / proto.MaxLogIndex. See newRaft.
func checkPersistedBounds(logdb LogReader) error {
	ss := logdb.Snapshot()
	if ss.Index >= persistedCeiling {
		slog.Error("refusing to open: persisted snapshot index is at the ceiling",
			"snapshot_index", ss.Index)
		return proto.ErrLogIndexOutOfRange
	}
	if ss.Term >= persistedCeiling {
		slog.Error("refusing to open: persisted snapshot term is at the ceiling",
			"snapshot_term", ss.Term)
		return proto.ErrTermOutOfRange
	}
	state, _ := logdb.NodeState()
	if state.Term >= persistedCeiling {
		slog.Error("refusing to open: persisted term is at the ceiling",
			"term", state.Term)
		return proto.ErrTermOutOfRange
	}
	if _, lastIdx := logdb.IndexRange(); lastIdx >= persistedCeiling {
		slog.Error("refusing to open: persisted log extends to the ceiling",
			"last_index", lastIdx)
		return proto.ErrLogIndexOutOfRange
	}
	return nil
}

// initRemotes initializes remote tracking from a membership configuration.
func (r *raft) initRemotes(membership proto.Membership) {
	nextIdx := r.log.lastIndex() + 1
	for id := range membership.Addresses {
		if id == r.replicaID {
			continue
		}
		r.remotes[id] = newRemote(nextIdx, maxInflightMsgs)
	}
	for id := range membership.Observers {
		if id == r.replicaID {
			continue
		}
		r.observers[id] = newRemote(nextIdx, maxInflightMsgs)
	}
	for id := range membership.Witnesses {
		if id == r.replicaID {
			continue
		}
		r.witnesses[id] = newRemote(nextIdx, maxInflightMsgs)
	}
	r.setSelfMembership(membershipHasSelf(membership, r.replicaID))
	_, listedWitness := membership.Witnesses[r.replicaID]
	_, listedObserver := membership.Observers[r.replicaID]
	r.checkSelfRole(membershipHasSelf(membership, r.replicaID), listedWitness, listedObserver)
}

// checkSelfRole reconciles this replica's role with the REPLICATED membership.
// Called wherever the configuration is (re)established: construction, snapshot
// restore, and commit-time config change application.
//
// The two non-voting roles are handled differently because they differ in what
// the replica physically holds.
//
// The OBSERVER role follows the configuration (setObserver). An observer and a
// voter are replicated identically — the leader's sendAppend ships both the
// full entry payloads — so they hold the same state machine and differ only in
// which gates apply: whether they campaign, promise vote windows in heartbeat
// acks, and count themselves toward quorum. Moving between the two needs no
// local repair, only a different answer from those gates, which is why a
// committed promotion takes effect in place. Deriving the role is also what
// keeps it safe. startElection gates campaigning on r.isObserver while
// quorum() and countVotes() size the voting set from the replicated membership
// plus an unconditional +1 for self, so a replica the configuration lists as an
// observer but that believes itself a voter computes quorumOf(V+1) over a real
// voting set of V, counts its own vote, and can win — two of them in the same
// term. Taking the role from the configuration closes that by construction,
// and every voter's refusal to grant a listed observer a vote
// (handleVoteRequest) stays as the independent second half. config.IsObserver
// is therefore only the INITIAL hint, consulted while the configuration does
// not mention this replica; etcd likewise makes the learner role a property of
// the ConfState rather than of local configuration.
//
// The WITNESS role is latched (roleMismatch). A witness keeps no state
// machine, and the leader strips entry payloads for any replica the
// configuration lists as one. A replica on the wrong side of that split runs a
// state machine that never saw the entry data while believing itself a full
// replica — or, the other way round, a witness listed as a full replica would
// answer reads from an empty one. That is a statement about what the replica
// holds, not about which gates apply, and nothing done in place repairs it.
// The latch refuses the two operations that can turn the bad state machine
// into an authoritative answer: campaigning and serving reads. Voting is
// deliberately NOT refused; witnesses vote, and a replica that stopped voting
// would remove itself from a quorum the rest of the cluster still counts it in.
//
// dragonboat fails closed on the witness disagreement by panicking
// (internal/raft/raft.go addNode / addWitness). Panicking a whole host over one
// shard's configuration is not available here — a host carries thousands of
// shards — so the latch is the non-fatal equivalent.
func (r *raft) checkSelfRole(inConfig, listedWitness, listedObserver bool) {
	// A replica the configuration does not mention makes no statement about
	// its role, so the current one — the config.IsObserver hint on a joiner —
	// stands. setSelfMembership already makes such a replica unelectable.
	if !inConfig {
		return
	}
	r.setObserver(listedObserver)
	mismatch := listedWitness != r.isWitness
	if mismatch == r.roleMismatch {
		return
	}
	r.roleMismatch = mismatch
	if !mismatch {
		slog.Info("replica witness role now agrees with the replicated membership",
			"shard", r.shardID,
			"replica", r.replicaID,
			"is_witness", r.isWitness)
		return
	}
	slog.Error("replica witness role contradicts the replicated membership: refusing to campaign or serve reads until they agree",
		"shard", r.shardID,
		"replica", r.replicaID,
		"configured_witness", r.isWitness,
		"membership_witness", listedWitness,
		"membership_observer", listedObserver,
		"term", r.term)
	// A leader that only now learns the configuration calls it a witness has
	// been answering reads and counting quorums it had no right to. Step
	// down immediately rather than waiting for an election timeout.
	if r.state == leader {
		r.becomeFollower(r.term, noLeader)
	}
}

// setObserver moves this replica's effective role to the one the replicated
// membership records for it: a non-voting observer, or a voting member. It is
// edge-triggered, so it logs and acts only when the role actually changes.
//
// PROMOTION (observer to voter) is etcd's learner promotion: from the moment
// the AddNode naming this replica is applied here it campaigns, counts its own
// vote, and serves ReadIndex like any other voter, with no restart. Before
// that moment it was already replicated with full payloads and already
// answered vote requests, so the only new obligation is the vote window its
// heartbeat acks now promise (voteWindow). sizeVoteHoldDown records that
// window as owed in the hard state BEFORE any ack carries it — the hard state
// is persisted ahead of every message the next update sends — so a restart
// straight after the promotion still honours the promise. It also sizes a
// LeaseRead replica's post-restart hold-down, which an observer (promising
// nothing) left at zero, i.e. holding votes down indefinitely; without this
// the promoted replica could never vote or campaign.
//
// DEMOTION (voter to observer) is not something a live voter can be asked for
// — validateAddNonVoting refuses an add whose target is already a member — but
// a committed AddNonVoting can still converge a voter placement that came from
// the StartShard members-map seed (see the "Convergent application" comment in
// membership.go), and a snapshot can carry a configuration that lists this
// replica as an observer. Either way a non-voter must not be leading or
// running a campaign whose quorum counts its own vote, so it steps down.
// The vote window already owed is kept: sizeVoteHoldDown only ever raises the
// record, and the promises already made must still be honoured.
func (r *raft) setObserver(observer bool) {
	if r.isObserver == observer {
		return
	}
	r.isObserver = observer
	if !observer {
		r.sizeVoteHoldDown()
		slog.Info("replica promoted in place: the replicated membership lists it as a voting member",
			"shard", r.shardID,
			"replica", r.replicaID,
			"term", r.term)
		return
	}
	slog.Info("replica is a non-voting observer in the replicated membership and will not campaign",
		"shard", r.shardID,
		"replica", r.replicaID,
		"state", r.state,
		"term", r.term)
	if r.state != follower {
		r.becomeFollower(r.term, noLeader)
	}
}

// setJoining records whether this replica is still joining the cluster with an
// incomplete membership. See the joining field: it suppresses campaigning only,
// never ticking or voting.
func (r *raft) setJoining(joining bool) {
	if r.joining == joining {
		return
	}
	r.joining = joining
	if joining {
		slog.Info("replica is joining and will not campaign until it learns the full membership",
			"shard", r.shardID,
			"replica", r.replicaID,
			"term", r.term)
		return
	}
	slog.Info("replica learned the full membership and may campaign",
		"shard", r.shardID,
		"replica", r.replicaID,
		"term", r.term)
}

// membershipHasSelf reports whether replicaID appears anywhere in the
// configuration (voter, observer or witness).
func membershipHasSelf(m proto.Membership, replicaID uint64) bool {
	if _, ok := m.Addresses[replicaID]; ok {
		return true
	}
	if _, ok := m.Observers[replicaID]; ok {
		return true
	}
	_, ok := m.Witnesses[replicaID]
	return ok
}

// setSelfMembership records whether this replica is part of the current
// configuration. See the selfRemoved field for why this is tracked.
//
// An empty configuration counts as "not a member", matching dragonboat's
// selfRemoved(): a replica that knows no configuration has no basis to
// campaign, and a joining node is seeded with exactly the existing members
// (self excluded) precisely so it cannot self-elect before the leader adds
// it. The flag is cleared as soon as the AddNode config change commits.
func (r *raft) setSelfMembership(present bool) {
	if r.selfRemoved == !present {
		return
	}
	r.selfRemoved = !present
	if r.selfRemoved {
		// Entering selfRemoved makes this replica permanently unelectable:
		// startElection returns immediately, so nothing this node does can ever
		// restore it — only a config change that re-adds it, applied from the
		// leader. Debug is far too quiet for a terminal state. It is normal and
		// expected for a replica that has just been removed, and a serious fault
		// for one that has not (a truncated or empty membership restored from a
		// damaged snapshot), and only an operator can tell those apart. The
		// transition is edge-triggered — this line is unreachable unless the
		// value actually changes — so it cannot become a log flood.
		slog.Warn("replica is not in the current configuration and will not campaign",
			"shard", r.shardID,
			"replica", r.replicaID,
			"term", r.term)
		return
	}
	slog.Info("replica is back in the current configuration and may campaign",
		"shard", r.shardID,
		"replica", r.replicaID,
		"term", r.term)
}

// unreadableLogWarnInterval rate-limits the vote-denial warning emitted when
// this replica's own last log term cannot be read. Every RequestVote from every
// candidate hits the same condition, so an unrate-limited line would flood.
const unreadableLogWarnInterval = 10 * time.Second

// recordUnreadableLogVoteDenial counts and reports a vote denied because this
// replica could not read its OWN last log term.
//
// The denial is correct — granting on a fabricated term 0 would make every
// candidate look up to date and violate Leader Completeness — but it is caused
// by local storage, not by the candidate, and it applies to every candidate
// equally. On a majority of replicas it makes the shard unelectable while every
// node is up and reachable, which without this report is entirely silent.
func (r *raft) recordUnreadableLogVoteDenial(from uint64) {
	r.unreadableLogVoteDenials++
	now := time.Now()
	if now.Sub(r.lastUnreadableLogWarn) < unreadableLogWarnInterval {
		return
	}
	r.lastUnreadableLogWarn = now
	slog.Warn("vote denied: local last log term unreadable",
		"shard", r.shardID,
		"replica", r.replicaID,
		"term", r.term,
		"candidate", from,
		"last_index", r.log.lastIndex(),
		"denials", r.unreadableLogVoteDenials,
		"rate_limit", unreadableLogWarnInterval,
	)
}

// --------------------------------------------------------------------------
// Role transitions
// --------------------------------------------------------------------------

// becomeFollower transitions the node to the follower state.
// Per Raft Figure 2: the vote is only cleared on term advancement.
// When transitioning to follower in the same term (e.g., candidate
// receiving AppendEntries from the winning leader), the vote must be
// preserved to prevent double-voting.
func (r *raft) becomeFollower(term, leaderID uint64) {
	r.stepFunc = stepFollower
	r.tickFunc = tickFollower
	r.state = follower
	if term != r.term {
		r.vote = noNode
	}
	r.term = term
	r.leaderID = leaderID
	r.transferTarget = noNode
	r.transferElapsed = 0
	r.pendingReadIndex = r.pendingReadIndex[:0]
	r.readIndexBroadcastPending = false
	r.pendingConfigChange = false
	r.remoteLastAck = nil // invalidate lease on role change
	r.resetElectionTimeout()
	r.electionTick = 0
	slog.Debug("became follower",
		"shard", r.shardID,
		"replica", r.replicaID,
		"term", term,
		"leader", leaderID)
}

// becomeCandidate transitions the node to the candidate state.
// Increments term and votes for self.
func (r *raft) becomeCandidate() {
	invariant.Assert(r.state != leader, "leader cannot become candidate directly")
	r.stepFunc = stepCandidate
	r.tickFunc = tickCandidate
	r.state = candidate
	// Defence in depth behind the wire ceiling in step(): the increment is the
	// operation that would wrap, so it is guarded where it happens rather than
	// only where the input arrives. A term this high is unreachable by running
	// (one increment per election), so refusing to advance costs no real
	// cluster anything, while wrapping to 0 would let this replica vote twice
	// in one term.
	if r.term < maxAdoptableTerm {
		r.term++
	} else {
		slog.Error("refusing to advance term at the ceiling; replica cannot campaign",
			"shard", r.shardID,
			"replica", r.replicaID,
			"term", r.term)
	}
	r.vote = r.replicaID
	r.leaderID = noLeader
	r.transferTarget = noNode
	r.transferElapsed = 0
	r.pendingConfigChange = false
	r.resetElectionTimeout()
	r.electionTick = 0
	r.resetVotes()
	r.votes[r.replicaID] = true
	slog.Debug("became candidate",
		"shard", r.shardID,
		"replica", r.replicaID,
		"term", r.term)
}

// becomePreCandidate transitions the node to the pre-candidate state.
// Does NOT increment term (PreVote protocol).
func (r *raft) becomePreCandidate() {
	invariant.Assert(r.state != leader, "leader cannot become pre-candidate directly")
	r.stepFunc = stepCandidate
	r.tickFunc = tickCandidate
	r.state = preCandidate
	// Do not change term or vote in pre-vote.
	r.leaderID = noLeader
	r.transferTarget = noNode
	r.transferElapsed = 0
	r.pendingConfigChange = false
	r.resetElectionTimeout()
	r.electionTick = 0
	r.resetVotes()
	r.votes[r.replicaID] = true
	slog.Debug("became pre-candidate",
		"shard", r.shardID,
		"replica", r.replicaID,
		"term", r.term)
}

// becomeLeader transitions the node to the leader state. Appends a
// no-op entry to establish leadership and initializes remote tracking.
//
// Returns ErrTooManyVotingMembers when the voting set does not fit the
// ReadIndex ackBits word. The check is FIRST, before any field is written, so a
// refused promotion leaves the replica exactly as it was and the caller can
// simply not seat it as leader. It cannot be folded into the
// rebuildReplicaToBit call further down: by then stepFunc, state, leaderID and
// every remote's progress have already been reset, and the transition would
// have to be unwound rather than declined.
func (r *raft) becomeLeader() error {
	invariant.Assert(r.state != follower, "follower cannot become leader directly")
	// Leadership appends a no-op at lastIndex+1; a log already at the
	// admission bound has no room for it, and a leader that could not append
	// could not commit anything at its term anyway. Declined before any
	// field is written, like the voting-set check below, so the caller seats
	// the replica as a follower and nothing is unwound. Unreachable by
	// running; enforced so the bound holds on every append path.
	if r.log.lastIndex() >= proto.MaxLogIndex {
		slog.Error("refusing leadership: the log has reached the protocol's index bound and the leadership no-op cannot be appended",
			"shard", r.shardID,
			"replica", r.replicaID,
			"last_index", r.log.lastIndex(),
			"term", r.term)
		return proto.ErrLogIndexOutOfRange
	}
	if n := r.votingBitCount(); n > maxVotingMembers {
		slog.Error("refusing leadership: voting set exceeds the ReadIndex ackBits word, this shard cannot serve linearizable reads and must not lead",
			"shard", r.shardID,
			"replica", r.replicaID,
			"voting_members", n,
			"max_voting_members", maxVotingMembers,
			"term", r.term)
		return ErrTooManyVotingMembers
	}
	r.stepFunc = stepLeader
	r.tickFunc = tickLeader
	r.state = leader
	r.leaderID = r.replicaID
	r.transferTarget = noNode
	r.transferElapsed = 0
	r.pendingConfigChange = false
	// PhD 4.3: scan uncommitted log entries for config changes inherited
	// from the previous leader. If an uncommitted config change exists, we
	// must block further config change proposals until it commits.
	if r.hasConfigChangeToApply() {
		r.pendingConfigChange = true
	}
	r.heartbeatTick = 0
	r.electionTick = 0
	r.readIndexCtx = 0
	r.readIndexBroadcastPending = false

	// Reset rate limiter state so stale followerSizes from a prior
	// leadership tenure do not incorrectly trigger or suppress rate
	// limiting during the new term.
	if r.rl != nil {
		r.rl.Reset()
	}

	// Reset lease tracking. The lease must be earned through the first
	// heartbeat round after becoming leader. A new term has sent no
	// TimeoutNow yet, so it may take a lease again.
	r.timeoutNowSent = false
	if r.leaseRead {
		r.remoteLastAck = make(map[uint64]time.Time)
	} else {
		r.remoteLastAck = nil
	}

	// Reset all remote tracking. Set next to lastIndex+1 so the leader
	// probes each follower starting at its own last known position.
	// becomeProbe() resets inflights and state but we override next after.
	//
	// The activity flag must be cleared too (etcd/raft reset(): RecentActive
	// = (id == r.id)). remote structs survive leadership changes, so a node
	// that led at term T, stepped down, and regains leadership at T+k would
	// otherwise open its first CheckQuorum window with active flags earned
	// during a previous tenure — hasQuorumActive() would return true on
	// evidence a whole leadership term old and the leader would survive an
	// extra election timeout with no contact at all, which is precisely the
	// window CheckQuorum exists to close. dragonboat gets this for free by
	// allocating fresh remote structs in resetRemotes().
	lastIdx := r.log.lastIndex()
	for _, rm := range r.remotes {
		rm.match = 0
		rm.becomeProbe()
		rm.next = lastIdx + 1
		rm.setNotActive()
	}
	for _, rm := range r.observers {
		rm.match = 0
		rm.becomeProbe()
		rm.next = lastIdx + 1
		rm.setNotActive()
	}
	for _, wm := range r.witnesses {
		wm.match = 0
		wm.becomeProbe()
		wm.next = lastIdx + 1
		wm.setNotActive()
	}

	// Build replicaToBit mapping for zero-allocation ReadIndex ack tracking.
	// The guard at the top of this function already established that the set
	// fits, and nothing between the two changes it, so this cannot fail; the
	// error is propagated rather than dropped so the two stay coupled if one
	// of them is ever moved.
	if err := r.rebuildReplicaToBit(); err != nil {
		return err
	}

	// Append a no-op entry to commit entries from previous terms.
	noop := proto.Entry{
		Term:  r.term,
		Index: r.log.lastIndex() + 1,
		Type:  proto.EntryNormal,
	}
	r.log.append([]proto.Entry{noop})

	// Try to commit immediately (works for single-node clusters).
	r.tryCommit()

	// Broadcast the append to all peers.
	r.broadcastAppend()
	slog.Debug("became leader",
		"shard", r.shardID,
		"replica", r.replicaID,
		"term", r.term,
		"last_index", r.log.lastIndex())
	return nil
}

// --------------------------------------------------------------------------
// Message dispatch
// --------------------------------------------------------------------------

// dropHighTermVote reports whether a higher-term RequestVote should be
// dropped under the CheckQuorum rule: the node has a known leader and has
// heard from it within the last election timeout.
//
// Transfer-hinted votes (Hint == From) are exempt, because the outgoing leader
// asked for the election and its own lease must not veto it — but only at
// exactly r.term+1. The exemption is a value the SENDER chooses: RequestVote is
// not bound to committed membership on the receive path (a member that
// recovered at a new address has to be able to campaign during quorum loss), so
// any host with a valid cluster certificate can set Hint = From. Unbounded, a
// forged RequestVote{Hint: From, Term: r.term + 1000} walks past the lease,
// forces becomeFollower at an arbitrary term on every recipient, and steps the
// live leader down — repeatably, for as long as the sender keeps sending.
//
// r.term+1 is not a heuristic: the exemption only ever MATTERS while a leader
// lease is live, and a replica hearing from a live leader is at that leader's
// term, so a legitimate transferee — which campaigns at leader term + 1 —
// arrives at exactly r.term+1. A peer far enough behind for a larger gap has by
// definition not heard from the leader within the election timeout, so the
// lease clause below is already false and the vote is not dropped anyway.
//
// What this does NOT close: a plain RequestVote (Hint != From) still forces a
// term advance whenever no leader lease is live, which is the common case
// during an election. Bounding that means binding RequestVote to committed
// membership — which costs the quorum-loss recovery path — or replica-ID-bound
// certificates. The cap here removes the amplification, not the disruption.
func (r *raft) dropHighTermVote(msg proto.Message) bool {
	if msg.Type != proto.RequestVote || !r.checkQuorum || msg.Term <= r.term {
		return false
	}
	if msg.Hint == msg.From && msg.Term == r.term+1 {
		return false
	}
	return r.leaderID != noLeader && r.electionTick < r.electionTimeout
}

// step handles a Raft message. Common term-based logic is handled first,
// then delegation to the role-specific step function.
//
//nolint:gocyclo // the protocol entry point: origin and term gates must run in a fixed order before any role dispatch, and each gate's placement is a safety property (see the wire/local and zero-term comments below)
func (r *raft) step(msg proto.Message) error {
	// Refuse process-local message types that arrived from the wire, at ANY
	// term. These four are never sent between replicas (see
	// processLocalMessages), so a peer naming one is claiming to be this
	// host's own snapshot receiver, transport or admin API. The origin stamp
	// is set by the delivery API and cannot be marshaled, so this is a
	// property of HOW the message arrived, not of a field the sender chose —
	// which is what makes it a real boundary rather than another value the
	// attacker controls.
	if !msg.Origin.IsLocal() && isProcessLocalMessage(msg.Type) {
		slog.Warn("dropped process-local message received from the wire",
			"shard", r.shardID,
			"replica", r.replicaID,
			"from", msg.From,
			"type", proto.MessageTypeName(msg.Type),
			"msg_term", msg.Term,
			"term", r.term)
		return ErrWireLocalMessage
	}

	// Term 0 is the local-injection escape hatch, not a wire value. Every
	// message type produced by send() carries r.term, so a zero term on one
	// of those types can only come from a peer that would thereby bypass
	// every term comparison below: a Replicate could advance a follower's
	// commit index over its uncommitted tail (the entries then reach the
	// state machine and a later truncation by the real leader violates
	// State Machine Safety), a vote response could escape the stale-term
	// filter, and a Heartbeat could install a leader at any term.
	//
	// The hatch is keyed on the ORIGIN, not on the type alone: a wire
	// Propose or ReadIndex forwarded by a follower carries that follower's
	// term and passes through the normal term comparisons below, while a
	// term-less one can only have been built in this process.
	if msg.Term == 0 && !isLocallyInjectedControlMessage(msg) {
		slog.Warn("dropped protocol message with no term",
			"shard", r.shardID,
			"replica", r.replicaID,
			"from", msg.From,
			"type", proto.MessageTypeName(msg.Type),
			"term", r.term)
		return ErrZeroTermMessage
	}

	// Refuse a term within the reserved headroom at the top of the range.
	// Nothing bounds Term on the wire, so a peer can name any uint64, and
	// adopting one near the ceiling is unrecoverable in two ways at once:
	// becomeCandidate does term++, so a node parked at the maximum WRAPS to
	// term 0 on its next campaign — a term regression, after which it can
	// vote a second time in a term it has already voted in, and Election
	// Safety is gone. Even without the wrap, a node driven to the ceiling can
	// never campaign again, so one message permanently removes a voter.
	//
	// Terms advance by one per election. A cluster cannot reach this range by
	// running, only by being told to, so refusing it costs nothing real.
	// Checked here rather than at the codec because Term is a legitimate
	// uint64 field — this is a protocol bound, not a parse error.
	if msg.Term >= maxAdoptableTerm && !msg.Origin.IsLocal() {
		slog.Warn("dropped message with a term in the reserved ceiling range",
			"shard", r.shardID,
			"replica", r.replicaID,
			"from", msg.From,
			"type", proto.MessageTypeName(msg.Type),
			"msg_term", msg.Term,
			"term", r.term)
		return ErrTermCeilingExceeded
	}

	// CheckQuorum guard (Raft paper §6 last paragraph, thesis p.42): drop
	// a higher-term RequestVote while this node heard from a live,
	// quorum-backed leader within the last election timeout. Without
	// this, a network-partitioned node that kept incrementing its term
	// disrupts a stable cluster the moment it rejoins — the higher term
	// forces the leader to step down even though the partitioned node
	// cannot win. Leadership-transfer votes (Hint == From) are exempt.
	// PreVote is unaffected: it never advances terms. Mirrors
	// dragonboat's dropRequestVoteFromHighTermNode.
	if r.dropHighTermVote(msg) {
		slog.Debug("dropped higher-term RequestVote, leader lease active",
			"shard", r.shardID,
			"replica", r.replicaID,
			"from", msg.From,
			"msg_term", msg.Term,
			"term", r.term)
		return nil
	}

	// Handle term-based transitions first.
	if msg.Term > 0 {
		if msg.Term > r.term {
			// PreVote and granted PreVoteResp do not cause term advancement.
			if msg.Type != proto.RequestPreVote &&
				(msg.Type != proto.RequestPreVoteResp || msg.Reject) {
				// Only leader-originated message types reliably identify the
				// leader. Vote requests and all response messages do not.
				leaderID := noLeader
				if msg.Type == proto.Replicate || msg.Type == proto.Heartbeat ||
					msg.Type == proto.InstallSnapshot || msg.Type == proto.TimeoutNow {
					leaderID = msg.From
				}
				r.becomeFollower(msg.Term, leaderID)
			}
		} else if msg.Term < r.term && msg.Type != proto.Propose && msg.Type != proto.ReadIndex {
			// Stale message from an older term.
			//
			// Propose and ReadIndex are EXEMPT: a follower forwards them to the
			// leader stamped with its own (older) term, and term carries no
			// protocol meaning for a client request (etcd treats MsgProp and
			// MsgReadIndex the same way). Dropping them here would hang the
			// client to its deadline, so they fall through to the role handler
			// below — stepLeader accepts them, stepFollower re-forwards Propose
			// and serves ReadIndex, stepCandidate refuses safely.
			if msg.Type == proto.Heartbeat || msg.Type == proto.Replicate || msg.Type == proto.InstallSnapshot {
				// Send a corrective response so the stale sender learns the
				// newer term (send() fills Term = r.term). Mark it Reject:true:
				// this node did NOT accept the message, so a non-rejected
				// ReplicateResp would be a false success in handleAppendResp.
				// Carry the committed index as the hint. The sender steps down
				// on the higher term before acting on the reject, so this is
				// purely defensive, but it keeps the response semantically
				// honest. (dragonboat behaves the same way.)
				r.send(proto.Message{
					Type:     proto.ReplicateResp,
					To:       msg.From,
					Reject:   true,
					LogIndex: r.log.committed,
				})
			}
			if msg.Type == proto.RequestVote || msg.Type == proto.RequestPreVote {
				// Reject vote requests from older terms.
				respType := proto.RequestVoteResp
				if msg.Type == proto.RequestPreVote {
					respType = proto.RequestPreVoteResp
				}
				r.send(proto.Message{
					Type:   respType,
					To:     msg.From,
					Reject: true,
				})
			}
			return nil
		}
	}

	// Handle vote requests at all roles.
	if msg.Type == proto.RequestVote || msg.Type == proto.RequestPreVote {
		return r.handleVoteRequest(msg)
	}

	// Handle quiesce messages.
	if msg.Type == proto.Quiesce {
		r.handleQuiesceMessage(msg)
		return nil
	}

	// Handle TimeoutNow at any state.
	if msg.Type == proto.TimeoutNow {
		return r.handleTimeoutNow(msg)
	}

	// Delegate to role-specific handler.
	return r.stepFunc(r, msg)
}

// --------------------------------------------------------------------------
// Per-role step functions
// --------------------------------------------------------------------------

// stepFollower handles messages when the node is a follower.
func stepFollower(r *raft, msg proto.Message) error {
	switch msg.Type {
	case proto.Replicate:
		return r.handleAppend(msg)
	case proto.Heartbeat:
		return r.handleHeartbeat(msg)
	case proto.InstallSnapshot:
		return r.handleSnapshot(msg)
	case proto.ReadIndex:
		return r.handleFollowerReadIndex(msg)
	case proto.ReadIndexResp:
		return r.handleReadIndexResp(msg)
	case proto.Propose:
		if r.leaderID == noLeader {
			return &NotLeaderError{LeaderID: 0}
		}
		// Client activity: exit quiesce so the election timer runs at
		// normal speed while proposals are in flight.
		r.recordActivity()
		// Forward to leader.
		//
		// COPY the entries first: msg.Entries aliases the step worker's REUSABLE
		// proposal batch buffer, and a forwarded Propose is an ORDERED message, so
		// it sits in the outbound buffer until a GetUpdate cycle — which cannot run
		// while commitPending is true. The step worker meanwhile hands that same
		// buffer back to the proposal queue on its next drain, and the queue writes
		// new client proposals straight into the entry structs this message still
		// points at, Key included. The result is deterministic (not a race):
		// forwarded proposals are silently REPLACED — one proposal never reaches
		// the leader and blocks to its caller's deadline, while another is
		// forwarded twice and, having no session dedup, commits twice.
		//
		// Copying here rather than at the drain is what actually closes it: this is
		// the one point where a reference to the buffer escapes the step worker's
		// own iteration. The entry structs are copied; Cmd is shared, which is safe
		// because the queue only ever overwrites the Entry structs it owns and
		// never mutates the caller's command bytes.
		if len(msg.Entries) > 0 {
			forwarded := make([]proto.Entry, len(msg.Entries))
			copy(forwarded, msg.Entries)
			msg.Entries = forwarded
		}
		msg.To = r.leaderID
		r.send(msg)
		return nil
	default:
		return nil
	}
}

// stepCandidate handles messages when the node is a candidate or
// pre-candidate. Vote responses are filtered by state to prevent
// stale PreVoteResp messages from being counted as real votes
// after a preCandidate→candidate transition within the same inbox
// batch. Without this guard, multiple candidates can independently
// reach quorum at the same term, violating Raft's safety property.
// stepDownToFollower abandons a candidacy because a leader for this term (or
// a later one) has made itself known.
//
// It must never REGRESS the term. A locally injected message carries no term
// -- the snapshot receive path builds InstallSnapshot with Term 0 -- and
// becomeFollower assigns msg.Term unconditionally, so passing it straight
// through set a candidate's term to 0 and cleared its vote. The replica would
// then grant votes again in terms it had already voted in, which is an
// Election Safety violation: two leaders in one term. Reachable in production
// whenever a follower catching up by snapshot has its election timer fire
// mid-transfer and the install lands on the resulting candidate.
func (r *raft) stepDownToFollower(msg proto.Message) {
	term := msg.Term
	if term < r.term {
		term = r.term
	}
	r.becomeFollower(term, msg.From)
}

func stepCandidate(r *raft, msg proto.Message) error {
	switch msg.Type {
	case proto.RequestVoteResp:
		if r.state != candidate {
			return nil // Ignore real vote responses in pre-candidate state.
		}
		return r.handleVoteResp(msg)
	case proto.RequestPreVoteResp:
		if r.state != preCandidate {
			return nil // Ignore stale pre-vote responses in candidate state.
		}
		return r.handleVoteResp(msg)
	case proto.Replicate:
		r.stepDownToFollower(msg)
		return r.handleAppend(msg)
	case proto.Heartbeat:
		r.stepDownToFollower(msg)
		return r.handleHeartbeat(msg)
	case proto.InstallSnapshot:
		r.stepDownToFollower(msg)
		return r.handleSnapshot(msg)
	case proto.Propose, proto.ReadIndex:
		// A campaigning replica knows no leader, so it refuses both client
		// request types the way handleFollowerReadIndex and stepFollower's
		// Propose case do when r.leaderID == noLeader. ReadIndex must be
		// listed explicitly: letting it fall to the default drops it, and a
		// dropped ReadIndex is never answered — nothing is queued, forwarded
		// or resolved, so the host's pending read waits to the caller's
		// deadline and reports a timeout for a not-leader condition.
		return &NotLeaderError{LeaderID: 0}
	default:
		return nil
	}
}

// stepLeader handles messages when the node is the leader.
func stepLeader(r *raft, msg proto.Message) error {
	switch msg.Type {
	case proto.Propose:
		return r.handleLeaderPropose(msg)
	case proto.ReplicateResp:
		return r.handleAppendResp(msg)
	case proto.HeartbeatResp:
		return r.handleHeartbeatResp(msg)
	case proto.ReadIndex:
		return r.handleLeaderReadIndex(msg)
	case proto.LeaderTransfer:
		return r.handleLeaderTransfer(msg)
	case proto.SnapshotStatus:
		return r.handleSnapshotStatus(msg)
	case proto.Unreachable:
		return r.handleUnreachable(msg)
	case proto.RateLimit:
		return r.handleLeaderRateLimit(msg)
	default:
		return nil
	}
}

// --------------------------------------------------------------------------
// Tick functions
// --------------------------------------------------------------------------

// tickFollower advances the follower tick. Starts an election if the
// election timer expires.
func tickFollower(r *raft) {
	if r.quiesce.active {
		// In quiesce mode, use longer election timeout.
		r.electionTick++
		if r.electionTick >= r.quiesce.quiesceElectionTimeout() {
			r.electionTick = 0
			r.exitQuiesce()
			r.startElection()
		}
		return
	}

	r.tickRateLimit()

	r.electionTick++
	if r.electionTick >= r.randomizedElectionTimeout {
		slog.Debug("election timeout reached",
			"shard", r.shardID,
			"replica", r.replicaID,
			"role", "follower",
			"election_elapsed", r.electionTick,
			"election_timeout", r.randomizedElectionTimeout,
		)
		r.electionTick = 0
		// A follower whose election timer just expired has heard nothing from
		// a leader for a full timeout, which is precisely when it must
		// campaign. It does NOT try to enter quiesce here: doing so would
		// suppress the election and then run at quiesceElectionTimeout (30x
		// the normal timeout). Nothing on the follower path resets the
		// quiesce counter for heartbeats either (recordActivity is reached
		// only from the write paths), so it would advance exactly while the
		// follower was leaderless — a liveness hazard.
		//
		// Quiesce is leader-driven by design: the leader enters it and calls
		// broadcastQuiesce, and followers enter via handleQuiesceMessage,
		// which is gated on the local enabled flag. A follower does not
		// self-quiesce independently of its leader.
		r.startElection()
	}
}

// tickCandidate advances the candidate tick. Restarts the election if
// the election timer expires.
func tickCandidate(r *raft) {
	r.electionTick++
	if r.electionTick >= r.randomizedElectionTimeout {
		slog.Debug("election timeout reached",
			"shard", r.shardID,
			"replica", r.replicaID,
			"role", "candidate",
			"election_elapsed", r.electionTick,
			"election_timeout", r.randomizedElectionTimeout,
		)
		r.electionTick = 0
		r.startElection()
	}
}

// tickLeader advances the leader tick. Sends heartbeats and checks
// quorum connectivity.
func tickLeader(r *raft) {
	// Snapshot ack/retry countdowns must advance even while quiesced:
	// a lagging follower's catch-up runs entirely on response messages,
	// none of which reset the quiesce idle counter, so a snapshot cycle
	// can start and finish under quiesce. Skipping this behind the
	// quiesce early-return would freeze an armed countdown and strand
	// the follower paused in snapshot state until the next proposal.
	r.tickSnapshotWaits()

	if r.quiesce.active {
		// During quiesce, only send liveness probes.
		if r.quiesce.tickLiveness() {
			r.broadcastHeartbeat()
		}
		// CheckQuorum must keep running while quiesced: a partitioned
		// leader that never re-checks its quorum never steps down. The
		// read lease ages by elapsed time on its own (hasValidLease), so
		// it lapses between liveness probes regardless; the quiesce
		// liveness interval is the quiesced equivalent of the election
		// timeout, so it paces both the step-down check and the lease
		// reset.
		if r.checkQuorum {
			r.electionTick++
			if r.electionTick >= r.quiesce.livenessInterval {
				r.electionTick = 0
				r.logQuorumStatus()
				if !r.hasQuorumActive() {
					slog.Info("quiesced leader lost quorum, stepping down to follower",
						"shard", r.shardID,
						"replica", r.replicaID,
						"term", r.term,
					)
					r.becomeFollower(r.term, noLeader)
					return
				}
				r.resetRemoteActivity()
				r.resetLeaseAcks()
			}
		}
		return
	}

	if r.rl != nil {
		r.rl.Tick()
		r.rateLimited()
	}

	r.heartbeatTick++
	if r.heartbeatTick >= r.heartbeatTimeout {
		r.heartbeatTick = 0
		slog.Debug("heartbeat timeout, broadcasting to followers",
			"shard", r.shardID,
			"replica", r.replicaID,
			"term", r.term,
			"heartbeat_elapsed", r.heartbeatTick,
		)
		r.broadcastHeartbeat()
	}

	if r.checkQuorum {
		r.electionTick++
		if r.electionTick >= r.electionTimeout {
			r.electionTick = 0
			// Log quorum status before checking for lost quorum.
			r.logQuorumStatus()
			if !r.hasQuorumActive() {
				slog.Info("leader lost quorum, stepping down to follower",
					"shard", r.shardID,
					"replica", r.replicaID,
					"term", r.term,
				)
				r.becomeFollower(r.term, noLeader)
				return
			}
			r.resetRemoteActivity()
			// Reset lease acks so the lease must be re-earned through
			// fresh heartbeat acknowledgments each checkQuorum cycle,
			// the same cadence at which remote activity is re-proven.
			// hasValidLease ages acks by elapsed time on its own; this
			// only ever shortens the lease further.
			r.resetLeaseAcks()
		}
	}

	// Check leader transfer timeout.
	if r.transferTarget != noNode {
		r.transferElapsed++
		if r.transferElapsed >= r.electionTimeout {
			slog.Info("leader transfer timed out",
				"shard", r.shardID,
				"replica", r.replicaID,
				"target", r.transferTarget,
				"term", r.term,
				"elapsed", r.transferElapsed,
			)
			r.transferTarget = noNode
			r.transferElapsed = 0

			r.resumeLeaseAfterTransfer()
		}
	}

	// Try to enter quiesce. Block quiesce entry when a leader transfer
	// is active, because quiesce freezes the electionTick counter and
	// transferElapsed would never increment, causing the transfer timer
	// to stall indefinitely. The leader would reject proposals with
	// ErrTransferInProgress until manually restarted.
	if r.transferTarget == noNode && r.quiesce.tryEnter() {
		// Invalidate the lease on quiesce entry and restart the quiesced
		// CheckQuorum cycle from zero so the lease must be re-earned
		// against the liveness-probe interval rather than inheriting a
		// partially elapsed election timeout.
		r.resetLeaseAcks()
		r.electionTick = 0
		r.broadcastQuiesce()
	}
}

// --------------------------------------------------------------------------
// Election
// --------------------------------------------------------------------------

// hasConfigChangeToApply reports whether the log may still hold a config
// change whose membership effect has not taken hold yet. During that window
// the cluster is transitioning between configurations, and proposing a second
// change would put two of them in flight at once.
//
// The frontier is `processed`, NOT `committed`. Membership takes effect at
// COMMIT time in this implementation, but "commit time" here means
// Peer.Commit -> processConfigChanges, a stage BEHIND r.log.committed:
// r.log.committed advances in the step worker while the commit/apply cycle is
// still in flight, and becomeLeader runs inside that window. A test bounded at
// `committed` therefore reports "nothing pending" for a config change that has
// committed but has NOT been folded into p.membership yet — so a new leader
// arms pendingConfigChange=false, accepts a second config change, and computes
// its commit quorum over the OLD voting set while the log's newest
// configuration is a larger one. The commit quorum and the election quorum then
// need not intersect, and Leader Completeness fails. Anchoring at `processed`
// is etcd/raft's `applied >= pendingConfIndex` shape: no new config change
// until every earlier one has actually been applied.
//
// This is O(1). Scanning the whole unapplied range instead, from becomeLeader on
// the step worker, does not scale: on a restart processed starts at the
// snapshot index and Config.SnapshotEntries has no default, so a default
// deployment never compacts and the range is the ENTIRE log — 200k unapplied
// 1KB entries would mean ~200MB read synchronously before the first heartbeat
// could go out, so the election that had just been won would time out again.
// The bound is maintained incrementally by the log itself
// (raftLog.confChangeIndex), which is also strictly safer: a scan could fail to
// READ, and a failed read would have to be treated as "a change might be
// pending"; there is nothing to read here.
func (r *raft) hasConfigChangeToApply() bool {
	return r.log.confChangeIndex > r.log.processed
}

// configCurrentForCampaign resolves the committed configuration tail recovered
// at startup and reports whether this replica may campaign against the voting
// set it currently holds.
//
// Both campaign entry points — startElection and handleTimeoutNow — call it,
// for the same reason they both check selfRemoved, roleMismatch and joining:
// each of those is a statement about the configuration, and TimeoutNow bypasses
// startElection entirely.
//
// This is NOT hasConfigChangeToApply. That bound is deliberately conservative
// and covers UNCOMMITTED entries, and blocking elections on uncommitted log
// content is a liveness violation the Raft specification calls out explicitly
// (see startElection). This asks a narrower and answerable question: has this
// replica folded the config changes its own log already COMMITTED? It resolves
// that question by doing the work, not by waiting for someone else to.
func (r *raft) configCurrentForCampaign() bool {
	if r.resolveStaleConfig == nil {
		return true
	}
	return r.resolveStaleConfig()
}

// startElection starts a new election. Uses PreVote if enabled.
//
// Per the Raft specification (Section 5.2, Section 6, Figure 4), elections
// are never blocked based on uncommitted log content. The "only one config
// change pending at a time" constraint applies to the LEADER when PROPOSING
// new config changes (enforced in becomeLeader via pendingConfigChange),
// not to followers or candidates starting elections. Blocking elections
// here would cause a liveness violation: if a leader crashes after
// replicating an uncommitted config change to a majority, no follower in
// that majority could ever start an election, permanently stalling the
// cluster.
// A replica that is no longer in the configuration never campaigns
// (dragonboat gates followerTick on !selfRemoved for the same reason). The
// surviving configuration's quorum is computed without this replica, so a
// removed replica that still counts its own vote against its own stale
// voting set can declare itself leader for a term the real configuration is
// electing a leader in — two leaders in one term. It has no work to do
// either: it is no longer replicated to and its removal is already
// committed.
func (r *raft) startElection() {
	if r.isWitness || r.selfRemoved {
		// Reaching here means the election timeout expired without a heartbeat.
		// Forget the leader for the SAME reason the joining branch below does:
		// tickFollower has already zeroed electionTick, so leaving leaderID set
		// keeps the shared leader-lease test (leaderID != noLeader &&
		// electionTick < electionTimeout) true for most of every tick cycle.
		// A witness's vote counts toward quorum and a self-removed/observer
		// replica still answers RequestVote, so a stale leaderID here makes
		// them deny PreVotes and silently drop higher-term RequestVotes,
		// which can hold a shard with an otherwise-healthy quorum leaderless.
		r.leaderID = noLeader
		return
	}
	// Fold the committed configuration tail before deciding anything else.
	//
	// Membership is config-in-log and is rebuilt asynchronously after a
	// restart, so up to this point the voting set may still be the BASE
	// configuration — the snapshot's, or the founding one at index 1 — while
	// the committed log has moved on. Campaigning there computes quorum over
	// the wrong denominator, and a replica whose base is smaller than the
	// current configuration wins on a minority of the real voting set, in a
	// term the real configuration can elect in as well.
	//
	// It runs BEFORE the gates below because folding the tail is exactly what
	// can turn any of them on or off: a committed RemoveNode for self sets
	// selfRemoved, a committed AddNode promoting this replica clears
	// isObserver, a committed AddWitness sets roleMismatch, and learning any
	// member other than self clears joining. Deciding those against the base
	// configuration would answer them for a configuration the shard has left.
	// That is why the observer test is NOT in the pre-fold gate above: a
	// replica restarted after its promotion committed, from a base that still
	// lists it as an observer, would otherwise never fold the tail that
	// promotes it. A real observer pays nothing for the move — the fold is a
	// no-op unless a recovered tail is still outstanding, and then it is paid
	// once.
	//
	// This never latches. It resolves by reading THIS replica's own log — no
	// leader, no peer, no state machine and no external call is involved — and
	// a read failure suppresses only the current timeout.
	if !r.configCurrentForCampaign() {
		// Same stale-leaderID hazard as every other refusing branch here:
		// tickFollower has already zeroed electionTick, so leaving leaderID set
		// keeps the shared leader-lease test true and this replica would deny
		// PreVotes and drop higher-term RequestVotes while it is not itself
		// campaigning.
		r.leaderID = noLeader
		return
	}
	// Re-check self-removal against the configuration that fold just
	// established. The pre-fold test above ran against the base membership, so
	// a RemoveNode for this replica committed since the base was written would
	// have been invisible to it.
	if r.selfRemoved {
		r.leaderID = noLeader
		return
	}
	// A non-voting observer never campaigns: it is not in the voting set its
	// own quorum() would count it in (see checkSelfRole). Same stale-leaderID
	// hazard as the pre-fold branch: an observer still answers RequestVote.
	if r.isObserver {
		r.leaderID = noLeader
		return
	}
	// The replicated configuration calls this replica a witness while it was
	// started as a full replica (or the reverse). Its state machine and the
	// data path the leader uses for it disagree, so a term it won would put
	// an incomplete state machine in charge of answering reads. See
	// checkSelfRole.
	if r.roleMismatch {
		slog.Warn("election skipped: local role contradicts the replicated membership",
			"shard", r.shardID,
			"replica", r.replicaID,
			"configured_witness", r.isWitness,
			"term", r.term)
		// Same stale-leaderID hazard as the branches above: a role-mismatched
		// replica the replicated membership still counts as a voter must not
		// keep asserting the dead leader is live, or it stalls the election it
		// is a required part of.
		r.leaderID = noLeader
		return
	}
	// A joining replica's membership is incomplete by construction: the
	// existing cluster was bootstrapped through a confState that is not in the
	// log, so this replica may hold only itself and would "win" with a quorum
	// of one. Its election clock still runs (Peer.Tick) so that it keeps
	// granting votes to the real members; only the campaign is suppressed.
	//
	// Reaching here means the election timeout expired: this replica has not
	// heard from the leader for a full timeout. Forget it. A campaigning
	// replica clears leaderID in becomePreCandidate/becomeCandidate; one that
	// refuses to campaign must still stop asserting the leader is live, because
	// tickFollower has ALREADY zeroed electionTick on the way in. Leaving
	// leaderID set would keep `leaderID != noLeader && electionTick <
	// electionTimeout` — the leader-lease test shared by handleVoteRequest and
	// dropHighTermVote — true for all but the (randomizedElectionTimeout -
	// electionTimeout) ticks at the top of each cycle, and permanently true
	// whenever that draw happens to be zero. This replica would then deny most
	// PreVotes and silently drop most higher-term RequestVotes, holding a shard
	// with a healthy quorum leaderless.
	if r.joining {
		slog.Debug("election skipped: still joining with incomplete membership",
			"shard", r.shardID,
			"replica", r.replicaID,
			"term", r.term,
			"last_leader", r.leaderID)
		r.leaderID = noLeader
		return
	}
	// A replica inside its post-restart vote hold-down must not vote for
	// itself either: a self-vote counts toward the election quorum exactly
	// as a grant to another candidate does, and a leader may still count
	// this replica's pre-restart ack toward a read lease (voteHeldDown).
	// Same stale-leaderID reasoning as the joining branch above.
	if r.voteHeldDown() || r.votePromised() {
		slog.Debug("election skipped: a leader may be counting this replica's ack toward a read lease",
			"shard", r.shardID,
			"replica", r.replicaID,
			"term", r.term,
			"last_leader", r.leaderID)
		r.leaderID = noLeader
		// The window ends at a point in real time, not on a tick boundary, so
		// the timer that fired just short of it retries on the next tick
		// rather than waiting out a whole new election timeout. Only a
		// replica whose timer has already expired gets here, so this costs
		// one clock read per tick until the window ends.
		r.electionTick = max(r.randomizedElectionTimeout-1, 0)
		return
	}
	// Refuse to start an election at all when this node's own last log term
	// is unreadable, BEFORE becomeCandidate/becomePreCandidate runs.
	//
	// campaign() also checks this, but by then becomeCandidate has already
	// incremented the term, cleared leaderID and reset the timers. The node
	// would sit as a candidate at T+1 having sent zero RequestVotes -- and
	// because send() stamps r.term on every reply, its rising term reaches
	// the live leader through ordinary Replicate/Heartbeat rejections and
	// forces a step-down on each exchange. One replica with an unreadable
	// log term would escalate into cluster-wide leader churn while emitting
	// no election traffic of its own.
	if _, ok := r.log.lastTerm(); !ok {
		slog.Warn("election skipped: local last log term unreadable",
			"shard", r.shardID,
			"replica", r.replicaID,
			"term", r.term,
			"last_index", r.log.lastIndex())
		return
	}
	if r.onCampaign != nil {
		r.onCampaign(r.shardID, r.replicaID, r.term)
	}
	slog.Debug("starting election",
		"shard", r.shardID,
		"replica", r.replicaID,
		"pre_vote", r.preVote)
	// Record election start time for duration tracking.
	r.electionStartTick = r.electionTick
	if r.preVote {
		r.becomePreCandidate()
		r.broadcastPreVote()
		// Single-node cluster: check immediately.
		if r.checkElectionResult() {
			return
		}
	} else {
		r.becomeCandidate()
		r.broadcastVote()
		if r.checkElectionResult() {
			return
		}
	}
}

// campaign sends vote requests to all voting members. The message
// template is created once and only the To field is mutated per peer.
// send() copies the struct by value when appending, so reuse is safe.
func (r *raft) campaign(msgType uint64, transfer bool) {
	term := r.term
	if msgType == proto.RequestPreVote {
		// For PreVote, use term+1 as the prospective term without
		// actually incrementing.
		term = r.term + 1
	}

	// Campaigning while our own last term is unreadable would advertise
	// LogTerm 0, which every peer with a non-empty log rejects — so the round
	// is guaranteed to fail while still resetting timers and, for a real vote,
	// burning a term. Skip it and retry on the next election timeout, by
	// which point the transient read failure has usually cleared.
	campaignLogTerm, ok := r.log.lastTerm()
	if !ok {
		slog.Warn("campaign skipped: local last log term unreadable",
			"shard", r.shardID,
			"replica", r.replicaID,
			"term", r.term,
			"last_index", r.log.lastIndex())
		return
	}

	msg := proto.Message{
		Type:     msgType,
		Term:     term,
		LogTerm:  campaignLogTerm,
		LogIndex: r.log.lastIndex(),
	}
	// Mark a leadership-transfer election by setting Hint to the
	// candidate's own ID (Hint == From on receipt). Recipients exempt
	// such votes from the CheckQuorum higher-term-vote guard, which
	// would otherwise drop them while the old leader's lease is fresh
	// (Raft thesis p.42).
	if transfer {
		msg.Hint = r.replicaID
	}

	for id := range r.remotes {
		if id == r.replicaID {
			continue
		}
		msg.To = id
		r.send(msg)
	}
	for id := range r.witnesses {
		if id == r.replicaID {
			continue
		}
		msg.To = id
		r.send(msg)
	}
}

// broadcastPreVote sends RequestPreVote to all voting members.
func (r *raft) broadcastPreVote() {
	r.campaign(proto.RequestPreVote, false)
}

// broadcastVote sends RequestVote to all voting members.
func (r *raft) broadcastVote() {
	r.campaign(proto.RequestVote, false)
}

// broadcastTransferVote sends RequestVote to all voting members with the
// leadership-transfer hint set, exempting the votes from recipients'
// CheckQuorum higher-term-vote guard.
func (r *raft) broadcastTransferVote() {
	r.campaign(proto.RequestVote, true)
}

// handleVoteRequest processes a RequestVote or RequestPreVote message.
func (r *raft) handleVoteRequest(msg proto.Message) error {
	isPreVote := msg.Type == proto.RequestPreVote
	respType := proto.RequestVoteResp
	if isPreVote {
		respType = proto.RequestPreVoteResp
	}

	// Check if the candidate's log is at least as up-to-date. A denial because
	// our OWN last log term is unreadable is a storage fault on this replica,
	// not a statement about the candidate, and it denies EVERY candidate for as
	// long as it lasts. Report it: hit a majority and the shard cannot elect at
	// all, with a silently leaderless shard as the only symptom. The sibling
	// check in startElection warns as well.
	logOK, logReadable := r.log.isUpToDate(msg.LogIndex, msg.LogTerm)
	if !logReadable {
		r.recordUnreadableLogVoteDenial(msg.From)
	}

	// Never grant a vote to a replica our configuration does not contain. This
	// is the counterpart to the receive path's deny-list (Peer.IsRemoved): once
	// inbound replication is no longer allow-listed, refusing the VOTE is what
	// stops a replica that is not part of this shard from ever winning a term
	// and becoming its leader. Denying a vote can only cost liveness, never
	// safety, which is why the configuration test belongs here rather than on
	// the receive path where staleness would wedge the replica instead.
	//
	// Liveness is preserved because it is gated on knowing a configuration at
	// all: a replica with no configuration yet (a joiner before the bootstrap
	// entry arrives) skips the test and votes normally. And a legitimately added
	// candidate cannot be starved by stale voters — its AddNode could only have
	// committed if a quorum had already applied it, and those replicas hold it
	// durably, so a quorum that recognises the candidate always exists.
	//
	// The test is skipped while this replica is JOINING, because that flag means
	// its configuration is known-incomplete by construction — it may hold only
	// itself, having applied its own AddNode without yet holding the entry that
	// names the rest of the shard. Denying votes from that state would strand an
	// otherwise healthy shard: the joiner is a voter its quorum needs, and it
	// would refuse every real member. An incomplete configuration is not
	// evidence of non-membership. This is the same reasoning that keeps the
	// receive path off an allow-list, applied to the one place a configuration
	// test is still safe.
	//
	// hashicorp/raft applies exactly this test, in requestVote and
	// requestPreVote, under the same "only when a configuration is known" guard.
	if !r.joining && r.membershipKnown() && !r.isKnownMember(msg.From) {
		slog.Warn("vote denied: candidate is not in the current configuration",
			"shard", r.shardID,
			"replica", r.replicaID,
			"candidate", msg.From,
			"term", r.term,
			"msg_term", msg.Term,
			"pre_vote", isPreVote,
		)
		r.send(proto.Message{Type: respType, To: msg.From, Reject: true})
		return nil
	}

	// A candidate the configuration lists as NON-VOTING must never be granted a
	// vote. The membership test above is deliberately "known in any role"
	// (isKnownMember consults remotes, observers AND witnesses), which is right
	// for witnesses — they vote — and wrong for observers, which by definition
	// do not. Without this, an observer whose LOCAL config says IsObserver=false
	// campaigns, collects grants from real voters, counts its own vote, and can
	// be elected; two of them can be elected in the same term because each sizes
	// its quorum as quorumOf(V+1) over a real voting set of V.
	//
	// checkSelfRole latches that disagreement on the observer itself, and this
	// is the independent second half: it holds even if the observer's own latch
	// is bypassed, because it is enforced by every voter rather than by the
	// candidate. Denying a vote can only ever cost liveness, never safety —
	// the same argument the configuration test above rests on.
	if !r.joining && r.membershipKnown() {
		if _, nonVoting := r.observers[msg.From]; nonVoting {
			slog.Warn("vote denied: candidate is a non-voting observer in the current configuration",
				"shard", r.shardID,
				"replica", r.replicaID,
				"candidate", msg.From,
				"term", r.term,
				"msg_term", msg.Term,
				"pre_vote", isPreVote,
			)
			r.send(proto.Message{Type: respType, To: msg.From, Reject: true})
			return nil
		}
	}

	// Check if we can grant the vote.
	var canVote bool
	if isPreVote {
		// For PreVote: grant if the candidate is prospecting for a term
		// STRICTLY ABOVE ours, its log is up-to-date, and we have not heard
		// from a valid leader recently. The "leader lease" check uses ticks,
		// not wall-clock time.
		//
		// The msg.Term > r.term test is not optional (etcd/raft folds the same
		// clause into canVote; dragonboat's handleNodeRequestPreVote grants
		// only on m.Term > r.term). step() already drops msg.Term < r.term, so
		// without it the hole is exactly msg.Term == r.term: a replica at term
		// T that has ALREADY voted in T would still grant a pre-vote to a
		// different candidate prospecting for that same, already-decided term.
		// The real-vote path would then deny every one of those grants, so the
		// pre-candidate burns a term increment and a broadcast on every voter
		// for a round that cannot succeed — precisely the disruption PreVote
		// exists to prevent. Safety is not at stake (one vote per term is
		// enforced on the real-vote path); wasted elections are.
		leaderActive := r.leaderID != noLeader && r.electionTick < r.electionTimeout
		canVote = logOK && !leaderActive && msg.Term > r.term
	} else {
		// For a real vote: grant if the log is up-to-date AND either this
		// is a repeat of the vote already cast this term, or no vote has
		// been cast AND no leader is known for this term.
		//
		// The "no leader known" clause is not optional (etcd/raft canVote,
		// Raft §5.2 + thesis p.42). A follower can reach term T without
		// voting — it learns T from the leader's Replicate/Heartbeat, which
		// clears r.vote in becomeFollower while setting r.leaderID. A stale
		// candidate that campaigns at that same term T is NOT filtered by
		// dropHighTermVote (that guard only fires for msg.Term > r.term), so
		// without this clause the follower gratuitously unseats a healthy
		// leader. Worse, granting sets r.electionTick = 0 while r.leaderID
		// still names the old leader, which arms dropHighTermVote and makes
		// the follower silently discard every higher-term RequestVote for a
		// full election timeout — a self-inflicted vote blackout.
		//
		// Leadership-transfer votes (Hint == From) are exempt, exactly as in
		// dropHighTermVote: the outgoing leader asked for this election, so
		// the known-leader lease must not veto it.
		noCompetingLeader := r.leaderID == noLeader || msg.Hint == msg.From
		canVote = logOK && (r.vote == msg.From ||
			(r.vote == noNode && noCompetingLeader))
	}

	// A replica that restarted recently on a LeaseRead shard withholds every
	// vote and pre-vote, the transfer-hinted ones included, until its
	// election timeout has passed in real time: a leader may still be
	// counting its pre-restart ack toward a read lease (voteHeldDown). The
	// refusal takes the ordinary rejection path below. Checked only when the
	// vote would otherwise be granted, so a refusal reads no clock.
	if canVote && (r.voteHeldDown() || (r.votePromised() && !r.transferReleasesPromise(msg))) {
		canVote = false
		slog.Debug("vote withheld: a leader may be counting this replica's ack toward a read lease",
			"shard", r.shardID,
			"replica", r.replicaID,
			"candidate", msg.From,
			"term", r.term,
			"msg_term", msg.Term,
			"pre_vote", isPreVote)
	}

	// PreVote response term depends on the outcome:
	//   - GRANT: echo the requested (prospective) term msg.Term so the
	//     pre-candidate counts the grant at the term it is campaigning for.
	//     This also fixes the failover case where a follower at term T-1
	//     would otherwise reply with a term < the pre-candidate's and have
	//     its grant dropped as stale in step().
	//   - REJECT: use the responder's OWN current term r.term. Echoing the
	//     prospective term on a rejection is unsafe: step() treats a rejected
	//     RequestPreVoteResp with msg.Term > r.term as term-advancing, so the
	//     pre-candidate would becomeFollower(T+1) and then, via its stale-term
	//     reply to the real leader's heartbeat, force the legitimate leader to
	//     step down — exactly the disruption PreVote exists to prevent.
	// For real votes, Term=0 lets send() fill in r.term.
	respTerm := uint64(0)
	if isPreVote {
		if canVote {
			respTerm = msg.Term
		} else {
			respTerm = r.term
		}
	}

	if canVote {
		if !isPreVote {
			r.vote = msg.From
			r.electionTick = 0
		}
		r.send(proto.Message{
			Type:   respType,
			To:     msg.From,
			Term:   respTerm,
			Reject: false,
		})
	} else {
		r.send(proto.Message{
			Type:   respType,
			To:     msg.From,
			Term:   respTerm,
			Reject: true,
		})
	}

	slog.Debug("vote request",
		"shard", r.shardID,
		"from", msg.From,
		"pre_vote", isPreVote,
		"granted", canVote,
		"term", msg.Term)
	return nil
}

// handleVoteResp processes a RequestVoteResp or RequestPreVoteResp.
// Responses from replicas outside the CURRENT configuration are discarded:
// the quorum in tallyVotes is computed over the live voting set, so counting
// a grant from a replica that was removed mid-election (or was never a
// member) lets a candidate win with fewer configuration votes than the
// quorum requires — two leaders in one term.
func (r *raft) handleVoteResp(msg proto.Message) error {
	if !r.isVotingMember(msg.From) {
		slog.Debug("ignoring vote response from outside the configuration",
			"shard", r.shardID,
			"replica", r.replicaID,
			"from", msg.From,
			"term", r.term)
		return nil
	}
	slog.Debug("vote response",
		"shard", r.shardID,
		"from", msg.From,
		"granted", !msg.Reject,
		"term", r.term)
	r.votes[msg.From] = !msg.Reject
	r.checkElectionResult()
	return nil
}

// isVotingMember reports whether id belongs to the current configuration's
// voting set (remotes + witnesses + self). Self counts only while it is
// still part of the configuration: a self-removed replica is not a voting
// member of anything, so its own vote must not be tallied toward a quorum
// the configuration computes without it.
func (r *raft) isVotingMember(id uint64) bool {
	if id == r.replicaID {
		return !r.selfRemoved
	}
	if _, ok := r.remotes[id]; ok {
		return true
	}
	_, ok := r.witnesses[id]
	return ok
}

// purgeDepartedVotes drops vote records for replicas that left the voting
// set. Called whenever the configuration is reshaped, because r.votes is
// otherwise cleared only on the next campaign: a shrinking voting set
// lowers the quorum while stale grants remain countable.
func (r *raft) purgeDepartedVotes() {
	for id := range r.votes {
		if !r.isVotingMember(id) {
			delete(r.votes, id)
		}
	}
}

// countVotes tallies grants and rejections over the CURRENT configuration's
// voting set. Ranging r.votes instead would count replicas that are no
// longer part of the configuration the quorum is computed from.
func (r *raft) countVotes() (granted, rejected int) {
	tally := func(id uint64) {
		v, ok := r.votes[id]
		switch {
		case !ok:
			return
		case v:
			granted++
		default:
			rejected++
		}
	}
	if !r.selfRemoved {
		tally(r.replicaID)
	}
	for id := range r.remotes {
		if id != r.replicaID {
			tally(id)
		}
	}
	for id := range r.witnesses {
		if id != r.replicaID {
			tally(id)
		}
	}
	return granted, rejected
}

// checkElectionResult tallies votes and transitions state if the
// election is decided. Returns true if a transition occurred.
func (r *raft) checkElectionResult() bool {
	result := r.tallyVotes()
	q := quorum(r.remotes, r.witnesses)
	granted, _ := r.countVotes()
	switch result {
	case voteWon:
		if r.state == preCandidate {
			// Won pre-vote: proceed to real election.
			r.becomeCandidate()
			r.broadcastVote()
			// Check if single-node wins immediately.
			return r.checkElectionResult()
		}
		// Won real election. Log election completion metrics.
		electionDuration := r.electionTick - r.electionStartTick
		slog.Debug("election completed",
			"shard", r.shardID,
			"replica", r.replicaID,
			"won", true,
			"term", r.term,
			"votes_received", granted,
			"votes_needed", q,
			"duration_ticks", electionDuration,
		)
		if err := r.becomeLeader(); err != nil {
			// The election was won but leadership cannot be taken. Fall back
			// to the same state a LOST election produces: follower at this
			// term with no leader. Nothing has been mutated (becomeLeader
			// declines before its first write), so this is a clean decline
			// rather than an unwind.
			//
			// The shard is separately failed by whichever growth site let the
			// oversized configuration in — Peer.syncRaftRemotes returns the
			// same error out through Peer.Commit, and newRaft refuses to
			// start at all — so this branch is a backstop that keeps the step
			// worker alive, not the place the operator finds out.
			slog.Error("won the election but cannot take leadership",
				"shard", r.shardID,
				"replica", r.replicaID,
				"term", r.term,
				"error", err)
			r.becomeFollower(r.term, noLeader)
		}
		return true
	case voteLost:
		// Lost: revert to follower. Log election completion metrics.
		electionDuration := r.electionTick - r.electionStartTick
		slog.Debug("election completed",
			"shard", r.shardID,
			"replica", r.replicaID,
			"won", false,
			"term", r.term,
			"votes_received", granted,
			"votes_needed", q,
			"duration_ticks", electionDuration,
		)
		r.becomeFollower(r.term, noLeader)
		return true
	default:
		return false
	}
}

// tallyVotes counts votes over the current voting set and returns the
// result. Both the quorum and the tally are taken over the same
// configuration, so a mid-election membership change can never produce a
// win on fewer votes than that configuration requires.
func (r *raft) tallyVotes() voteResult {
	q := quorum(r.remotes, r.witnesses)
	totalVoters := numVotingMembers(r.remotes, r.witnesses)
	granted, rejected := r.countVotes()

	if granted >= q {
		return voteWon
	}
	if rejected > totalVoters-q {
		return voteLost
	}
	return votePending
}

// resetVotes clears the vote tracking map.
func (r *raft) resetVotes() {
	for k := range r.votes {
		delete(r.votes, k)
	}
}

// --------------------------------------------------------------------------
// Log replication
// --------------------------------------------------------------------------

// proposableEntryTypes is the allow-list of masked entry types a Propose may
// carry. Only these three originate from legitimate Host-side construction:
// EntryNormal (data), EntryConfigChange (membership), and EntryEncodedEntry
// (session-managed). EntryInitialMembership and EntryMetadata are produced only
// internally and must never enter the log through the wire-forwarded Propose
// path (see ErrReservedEntryType).
var proposableEntryTypes = map[uint64]struct{}{
	proto.EntryNormal:       {},
	proto.EntryConfigChange: {},
	proto.EntryEncodedEntry: {},
}

// handleLeaderPropose handles a proposal on the leader.
func (r *raft) handleLeaderPropose(msg proto.Message) error {
	if r.transferTarget != noNode {
		return ErrTransferInProgress
	}
	// The log stops growing at the admission bound. A leader whose log would
	// pass proto.MaxLogIndex with this batch — unreachable by running,
	// reachable only from admitted state just below it — must not append it,
	// or the local transitions would carry the index toward the ceiling the
	// open check refuses. The whole batch is judged, not the current last
	// index: the engine batches queued proposals, so one check on the
	// starting point would let a batch straddle the bound. Unreachable in
	// practice; enforced because the bound is load-bearing for the arithmetic
	// everywhere else.
	if last := r.log.lastIndex(); last >= proto.MaxLogIndex || uint64(len(msg.Entries)) > proto.MaxLogIndex-last {
		return proto.ErrLogIndexOutOfRange
	}

	// Sanitize entry types before any state mutation. The leader assigns
	// Term/Index but never inspected Type, so a follower-forwarded wire Propose
	// could smuggle a reserved type into the log: an injected
	// EntryInitialMembership keeps hasConfigChangeToApply true and wedges every
	// legitimate config change with ErrPendingConfigChange, and an EntryMetadata
	// would land a payload-less entry in the durable log. Reject the whole batch
	// up front; this runs alongside the two-pass config-change validation below.
	for i := range msg.Entries {
		if _, ok := proposableEntryTypes[msg.Entries[i].Type&proto.EntryTypeMask]; !ok {
			return ErrReservedEntryType
		}
	}

	r.recordActivity()

	// Re-evaluate the arming becomeLeader did. It is a CONSERVATIVE bound
	// (see raftLog.confChangeIndex): on a restart every recovered entry is
	// assumed to be a possible unapplied config change, and that assumption
	// has to be released as the tail is applied — otherwise a leader elected
	// straight after a restart would reject every membership change for the
	// whole term. Re-reading the bound is O(1), so this runs unconditionally.
	//
	// UNCONDITIONALLY, not `if r.pendingConfigChange` — the guarded form could
	// only ever RELAX the flag, never restore it. Peer.Commit clears the flag
	// for every committed EntryConfigChange it applies without asking whether a
	// LATER config change is still unapplied, so a catch-up batch containing an
	// earlier change cleared it while one was genuinely outstanding, and nothing
	// repaired it. The bound (confChangeIndex > processed) is a complete
	// standalone test, raised by l.append on both the propose and the
	// follower-append path; the boolean only ever contributed false negatives.
	r.pendingConfigChange = r.hasConfigChangeToApply()

	// Per PhD thesis 4.1: reject config change if one is already pending.
	// Validate the entire batch before mutating state. If a batch contains
	// two config changes, the first would set pendingConfigChange=true and
	// the second would trigger rejection. Without this two-pass approach,
	// the rejected batch would leave pendingConfigChange permanently stuck
	// true, blocking all future config change proposals on this leader.
	foundCC := false
	for i := range msg.Entries {
		if msg.Entries[i].Type&proto.EntryTypeMask == proto.EntryConfigChange {
			if r.pendingConfigChange || foundCC {
				return ErrPendingConfigChange
			}
			// Leader-side membership validation before append. The Host API
			// pre-validates on the node the client contacted, but a
			// follower-forwarded Propose reaches this leader with no validation
			// at all, and a differently-configured contact node may have
			// validated against a laxer policy. This is the last gate before the
			// change enters the log; the committed-replay path never re-checks
			// (determinism), so anything admitted here is applied shard-wide.
			if err := r.validateProposedConfigChange(msg.Entries[i].Cmd); err != nil {
				return err
			}
			foundCC = true
		}
	}
	// All entries validated — safe to mutate state.
	if foundCC {
		r.pendingConfigChange = true
	}

	entries := msg.Entries
	for i := range entries {
		entries[i].Term = r.term
		entries[i].Index = r.log.lastIndex() + uint64(i) + 1
	}
	// Compress entry payloads if Snappy compression is enabled. Only
	// normal entries are compressed; config changes must remain readable
	// by all nodes regardless of compression configuration.
	if r.entryCompression {
		for i := range entries {
			if entries[i].Type&proto.EntryTypeMask == proto.EntryNormal {
				CompressEntry(&entries[i])
			}
		}
	}
	r.log.append(entries)

	// Log proposal metrics for throughput monitoring.
	if len(entries) > 0 {
		slog.Debug("proposal received",
			"shard", r.shardID,
			"entries_count", len(entries),
			"first_index", entries[0].Index,
			"last_index", entries[len(entries)-1].Index,
		)
	}

	r.broadcastAppend()
	// Leader also counts its own entries as matched.
	r.tryCommit()
	return nil
}

// validateProposedConfigChange is the leader's propose-time gate for one
// EntryConfigChange payload. It decodes the payload and hands it to the full
// committed-membership validator when a Peer installed one, falling back to the
// raft-local policy subset on a bare raft.
//
// An undecodable payload is REJECTED, not warned past. Matching the apply
// path's tolerance would be wrong: that tolerance exists because a COMMITTED
// entry must produce the same outcome on every replica, which says nothing
// about whether an entry should be allowed to commit in the first place.
// A payload nothing can decode is a payload no gate has judged — the cap, the
// witness policy, the duplicate-identity and duplicate-address rules all read
// fields that failed to parse — so admitting it would put an unvalidated
// membership change into the durable log of every replica. Rejecting costs a legitimate
// client nothing: it never produces bytes that do not decode.
func (r *raft) validateProposedConfigChange(cmd []byte) error {
	var cc proto.ConfigChange
	if _, err := cc.UnmarshalFrom(cmd); err != nil {
		slog.Warn("rejected config change with an undecodable payload at propose time",
			"shard", r.shardID,
			"replica", r.replicaID,
			"error", err,
		)
		return ErrConfigChangeUnmarshal
	}
	if r.validateConfigChange != nil {
		return r.validateConfigChange(cc)
	}
	return r.validateConfigChangePolicy(cc)
}

// validateConfigChangePolicy enforces this node's WitnessConfig policy
// against a proposed config change using the leader's live peer tracking
// (remotes/witnesses; the leader itself is always a full voting replica).
// It mirrors the membership-level validateAddWitness/validateRemoveNode
// policy checks but runs at the raft layer, for a bare raft that has no
// membership to consult. Only the policy rules are checked here — the
// structural rules (duplicate identity, duplicate address, removed-node
// tombstones, empty address, last-voter) live in membership.validate, which
// validateProposedConfigChange prefers whenever a Peer installed it.
func (r *raft) validateConfigChangePolicy(cc proto.ConfigChange) error {
	// The voting-member cap must be enforced HERE for every net-new voting
	// member. This is the last gate before the entry enters the log on a
	// follower-forwarded Propose, which bypasses the Host API's
	// ValidateConfigChange entirely. The committed-replay path deliberately
	// does not re-check (determinism), so a committed 65th voter would make
	// rebuildReplicaToBit's invariant panic on every node that becomes
	// leader — permanently killing the shard.
	switch cc.Type {
	case proto.AddNode, proto.AddWitness:
		_, isRemote := r.remotes[cc.ReplicaID]
		_, isWitness := r.witnesses[cc.ReplicaID]
		if !isRemote && !isWitness && cc.ReplicaID != r.replicaID {
			if len(r.remotes)+len(r.witnesses)+1 >= maxVotingMembers {
				return ErrTooManyVotingMembers
			}
		}
	}

	switch cc.Type {
	case proto.UpdateWitnessPolicy:
		// The NEW policy must be satisfied by the current membership,
		// mirroring validateUpdateWitnessPolicy.
		if cc.MaxWitnesses > 0 && uint64(len(r.witnesses)) > cc.MaxWitnesses {
			return ErrTooManyWitnesses
		}
		if cc.RequireFullReplicaMajority && len(r.witnesses) > 0 {
			fullReplicas := len(r.remotes) + 1 // + self
			if fullReplicas < quorumOf(fullReplicas+len(r.witnesses)) {
				return ErrBreaksFullReplicaMajority
			}
		}
	case proto.AddWitness:
		if _, exists := r.witnesses[cc.ReplicaID]; exists {
			// Re-adding an existing witness is a no-op at apply time;
			// policy arithmetic below assumes a net-new witness.
			return nil
		}
		newWitnessCount := uint64(len(r.witnesses) + 1)
		if r.maxWitnesses > 0 && newWitnessCount > r.maxWitnesses {
			return ErrTooManyWitnesses
		}
		if r.requireFullReplicaMajority {
			fullReplicas := len(r.remotes) + 1 // + self
			if fullReplicas < quorumOf(fullReplicas+int(newWitnessCount)) {
				return ErrBreaksFullReplicaMajority
			}
		}
	case proto.RemoveNode:
		if !r.requireFullReplicaMajority || len(r.witnesses) == 0 {
			return nil
		}
		_, isRemote := r.remotes[cc.ReplicaID]
		if !isRemote && cc.ReplicaID != r.replicaID {
			// Removing a witness or observer never worsens the ratio.
			return nil
		}
		fullReplicas := len(r.remotes) // + self - removed member
		if fullReplicas < quorumOf(fullReplicas+len(r.witnesses)) {
			return ErrBreaksFullReplicaMajority
		}
	}
	return nil
}

// broadcastAppend sends append messages to all remotes, observers, and
// witnesses. Witnesses receive metadata-only Replicate messages containing
// Index and Term but no Cmd payload, so they can track log position for
// election up-to-date checks without storing full entry data. Config change
// entries are sent to witnesses unmodified so they can maintain correct
// membership.
//
// Observer catch-up: observers receive the same Replicate messages as full
// voting replicas. When an observer falls behind and the leader's log has
// been compacted past the observer's next index, sendAppend falls back to
// sendSnapshot, which triggers an InstallSnapshot to the observer. There is
// no proactive catch-up mechanism (e.g., periodic snapshot push to lagging
// observers). This is a deliberate design choice:
//
//   - Simplicity: the same replication path handles both voters and observers,
//     avoiding a separate catch-up protocol.
//   - Correctness: observers are non-voting; their lag does not affect commit
//     safety or leader election. The Raft invariant holds with or without
//     observer catch-up.
//   - Trade-off: if log compaction is aggressive and an observer is slow, it
//     may receive repeated snapshots. Operators should tune compaction
//     thresholds (CompactionOverhead) to retain enough log history for
//     observer replication.
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

// sendAppend sends append (Replicate) messages to the given peer. In
// Replicate state, multiple batches are sent in a pipeline loop: after
// each send, optimisticUpdate advances rm.next, so the next iteration
// picks up where the previous left off. The loop continues while more
// entries exist beyond rm.next and the inflight window has capacity.
// This reduces replication catch-up from O(N/batch) round-trips to
// O(N/batch/inflights) by filling the pipeline window immediately.
// In Probe state, only one message is sent (probeSent guard).
func (r *raft) sendAppend(to uint64) {
	rm := r.getRemote(to)
	if rm == nil {
		return
	}
	if rm.isPaused() {
		return
	}

	for {
		prevLogIndex := rm.next - 1
		prevLogTerm, err := r.log.term(prevLogIndex)
		if err != nil {
			// If we can't get the term, the peer needs a snapshot.
			r.sendSnapshot(to)
			return
		}

		// Get entries to send, bounded by maxReplicationPayload to prevent
		// unbounded Replicate messages when a follower is far behind.
		entries, err := r.log.entries(rm.next, r.log.lastIndex()+1, r.maxReplicationPayload)
		if err != nil {
			// Log compacted, send snapshot instead.
			r.sendSnapshot(to)
			return
		}
		entries = limitEntryCount(entries)

		// Send the Replicate message. An empty entries slice is valid: in
		// Probe state the follower uses prevLogIndex/prevLogTerm to detect
		// log divergence and sends a rejection hint, which drives the
		// leader's backtracking (declineFrom). Without this probe, newly
		// added nodes whose next == lastIndex+1 would never receive entries
		// because the leader would incorrectly treat them as caught up.
		// In Replicate state with no entries, the peer is genuinely caught
		// up and no message is needed.
		if len(entries) == 0 && rm.state == remoteReplicate {
			return
		}

		msg := proto.Message{
			Type:     proto.Replicate,
			To:       to,
			LogIndex: prevLogIndex,
			LogTerm:  prevLogTerm,
			Entries:  entries,
			Commit:   r.log.committed,
		}
		r.send(msg)

		if rm.state == remoteReplicate && len(entries) > 0 {
			lastIdx := entries[len(entries)-1].Index
			rm.optimisticUpdate(lastIdx)
			if err := rm.inflights.add(lastIdx); err != nil {
				slog.Error("inflights full during sendAppend",
					"shard", r.shardID, "to", to, "index", lastIdx, "error", err)
				return
			}
			// Continue pipelining if more entries exist and the inflight
			// window has capacity.
			if rm.next <= r.log.lastIndex() && !rm.inflights.full() {
				continue
			}
		}
		if rm.state == remoteProbe {
			rm.probeSent = true
		}
		return
	}
}

// sendWitnessAppend sends Replicate messages with metadata-only entries
// to a witness. Witnesses track Index/Term for election up-to-date checks
// but don't store Cmd payloads. Config change entries are sent unmodified
// so witnesses can maintain correct cluster membership. The Commit field
// is included so witnesses can track the commit index for leader authority.
// Like sendAppend, this pipelines multiple batches when the witness is in
// Replicate state and the inflight window has capacity.
func (r *raft) sendWitnessAppend(to uint64) {
	rm := r.getRemote(to)
	if rm == nil {
		return
	}
	if rm.isPaused() {
		return
	}

	for {
		prevLogIndex := rm.next - 1
		prevLogTerm, err := r.log.term(prevLogIndex)
		if err != nil {
			// Witness needs a snapshot.
			r.sendSnapshot(to)
			return
		}

		entries, err := r.log.entries(rm.next, r.log.lastIndex()+1, r.maxReplicationPayload)
		if err != nil {
			// Log compacted, send snapshot instead.
			r.sendSnapshot(to)
			return
		}
		entries = limitEntryCount(entries)

		// Same probe logic as sendAppend: witnesses in Probe state need
		// empty Replicate messages for log divergence detection.
		if len(entries) == 0 && rm.state == remoteReplicate {
			return
		}

		// Strip entries to metadata-only for witnesses. Config changes are
		// preserved so witnesses can apply membership updates.
		metaEntries := makeMetadataEntries(entries)

		msg := proto.Message{
			Type:     proto.Replicate,
			To:       to,
			LogIndex: prevLogIndex,
			LogTerm:  prevLogTerm,
			Entries:  metaEntries,
			Commit:   r.log.committed,
		}
		r.send(msg)

		if rm.state == remoteReplicate && len(entries) > 0 {
			lastIdx := entries[len(entries)-1].Index
			rm.optimisticUpdate(lastIdx)
			if err := rm.inflights.add(lastIdx); err != nil {
				slog.Error("inflights full during sendWitnessAppend",
					"shard", r.shardID, "to", to, "index", lastIdx, "error", err)
				return
			}
			// Continue pipelining if more entries exist and the inflight
			// window has capacity.
			if rm.next <= r.log.lastIndex() && !rm.inflights.full() {
				continue
			}
		}
		if rm.state == remoteProbe {
			rm.probeSent = true
		}
		return
	}
}

// makeMetadataEntries strips entries to metadata-only form for witness
// replication. Witnesses need Index and Term to participate in elections
// with correct up-to-date checks, but don't need Cmd data (which they
// never apply). Type is set to EntryMetadata to distinguish from normal
// entries on the wire. Config change entries are preserved unmodified
// because witnesses must apply membership changes to maintain correct
// cluster membership.
func makeMetadataEntries(entries []proto.Entry) []proto.Entry {
	if len(entries) == 0 {
		return nil
	}
	meta := make([]proto.Entry, 0, len(entries))
	for i := range entries {
		if entries[i].IsConfigChange() || entries[i].IsInitialMembership() {
			// Witnesses must apply membership changes to hold the correct
			// configuration, and the bootstrap entry at index 1 is how a witness
			// learns the initial configuration in the first place, so both are
			// preserved with their Cmd intact rather than stripped to metadata.
			meta = append(meta, entries[i])
		} else {
			meta = append(meta, proto.Entry{
				Index: entries[i].Index,
				Term:  entries[i].Term,
				Type:  proto.EntryMetadata,
			})
		}
	}
	return meta
}

// sendSnapshot sends a snapshot to the given peer. For witness targets,
// the snapshot file data is stripped (Filepath cleared, FileSize zeroed)
// since witnesses only need the metadata (Index, Term, Membership) for
// election participation. The empty Filepath IS the statement that this
// transfer carries no payload — forEachSnapshotChunk branches on exactly
// that to emit a single metadata-only chunk — so nothing else has to be
// stamped to say so.
func (r *raft) sendSnapshot(to uint64) {
	rm := r.getRemote(to)
	if rm == nil {
		return
	}

	ss := r.log.snapshot()
	if ss == nil {
		// The follower needs entries that have been compacted away, but no
		// snapshot exists yet to bring it forward. It cannot make progress
		// until one is created. Surface this rather than stalling silently.
		slog.Warn("cannot send snapshot to lagging follower: no snapshot available",
			"shard", r.shardID,
			"to", to,
		)
		// Returning without touching the remote would leave next wherever
		// the caller had put it — including past the end of the leader's log,
		// where term() fails on every retry and nothing is ever sent to this
		// peer again. becomeProbe re-seats next at match+1, which is always a
		// index the leader can read, so the ordinary probe/backtrack loop
		// keeps running and the peer stays reachable. This is a safety net for
		// any caller that leaves next unreadable, not just declineRemote.
		rm.becomeProbe()
		return
	}

	r.snapshotSeq++
	rm.becomeSnapshot(ss.Index, r.snapshotSeq)

	snap := *ss
	// Strip snapshot file data for witnesses. They only need the
	// metadata (Index, Term, Membership) for election participation.
	if _, isWitness := r.witnesses[to]; isWitness {
		snap.Filepath = ""
		snap.FileSize = 0
	}

	// Hint carries the transfer sequence; the transport echoes it back in
	// the SnapshotStatus report. It is transport-local and never crosses
	// the wire to the follower (the snapshot streams as SnapshotHeader +
	// chunks).
	r.send(proto.Message{
		Type:     proto.InstallSnapshot,
		To:       to,
		Hint:     r.snapshotSeq,
		Snapshot: snap,
	})
}

// handleAppend processes an AppendEntries (Replicate) message.
func (r *raft) handleAppend(msg proto.Message) error {
	r.electionTick = 0
	r.leaderID = msg.From
	r.recordActivity()

	// If the leader's prevLogIndex is behind our committed index, the
	// entries are stale. Respond immediately with the committed index
	// so the leader can advance its tracking of this follower. This
	// guard prevents tryAppend from encountering a conflict at or
	// below the committed index (which would violate the Raft safety
	// invariant that committed entries are never overwritten).
	if msg.LogIndex < r.log.committed {
		r.send(proto.Message{
			Type:     proto.ReplicateResp,
			To:       msg.From,
			LogIndex: r.log.committed,
		})
		return nil
	}

	// The admission bound holds for replicated entries as it does for
	// proposed ones (handleLeaderPropose): a batch whose last entry would sit
	// above proto.MaxLogIndex is refused outright, or a leader that admitted
	// state just below the bound could carry a follower's log to the ceiling
	// the open check refuses. The message's own indexes are checked, not the
	// local last index, since a conflicting append replaces the tail: the
	// batch ends at LogIndex+len(Entries). Dropped without a response, as an
	// unreadable batch is; the leader retries and gets the same answer.
	if n := uint64(len(msg.Entries)); n > 0 && (msg.LogIndex >= proto.MaxLogIndex || n > proto.MaxLogIndex-msg.LogIndex) {
		return proto.ErrLogIndexOutOfRange
	}
	// And the term bound: an entry's term can never exceed the term of the
	// leader replicating it (a leader appends at its own term and forwards
	// older entries at theirs), and msg.Term itself passed the ceiling check
	// in step. An entry term above msg.Term is a corrupt or hostile batch;
	// persisting it would poison lastTerm across restarts — a candidate with
	// a real log could never be up to date against it — and let this replica,
	// leading later, append entries with decreasing terms. Refused whole,
	// before the log is touched; an entry with term 0 has no leader that
	// could have written it and is refused too.
	for i := range msg.Entries {
		if term := msg.Entries[i].Term; term == 0 || term > msg.Term {
			return proto.ErrTermOutOfRange
		}
	}

	lastIdx, ok, truncated, err := r.log.tryAppend(msg.LogIndex, msg.LogTerm, msg.Entries)
	if err != nil {
		// Conflict detection encountered a compacted or unavailable
		// entry. Drop the message; the leader will retransmit or
		// send a snapshot. This is safe because the leader tracks
		// match/next per follower and will retry.
		return err
	}
	if ok {
		// Truncation cannot remove an applied config change: membership
		// only mutates on COMMITTED entries (Peer.Commit ->
		// processConfigChanges) and committed entries are never
		// truncated, so no membership rollback is needed here.
		if truncated {
			slog.Debug("log truncated due to conflict",
				"shard", r.shardID,
				"replica", r.replicaID,
				"from_leader", msg.From,
				"prev_log_index", msg.LogIndex,
				"new_last_index", lastIdx,
			)
		}

		// Success: advance commit index.
		if msg.Commit > r.log.committed {
			commitTo := min(msg.Commit, lastIdx)
			r.log.commitTo(commitTo)
		}
		r.send(proto.Message{
			Type:     proto.ReplicateResp,
			To:       msg.From,
			LogIndex: lastIdx,
		})
	} else {
		// Reject: provide hint for fast backtracking (Section 5.3).
		// Two cases:
		//   1. msg.LogIndex > lastIndex: the follower's log is shorter.
		//      Set Hint = lastIndex (index-based), LogTerm = 0.
		//   2. msg.LogIndex <= lastIndex but term mismatch: include the
		//      conflicting term and the first index of that term so the
		//      leader can skip the entire conflicting term in one RTT.
		lastIdx := r.log.lastIndex()
		var hintIndex uint64
		var hintTerm uint64
		if msg.LogIndex > lastIdx {
			// Follower's log is shorter than the leader expects.
			hintIndex = lastIdx
		} else {
			// Term mismatch at msg.LogIndex. Include the conflicting
			// term and the first index of that term for term-based
			// backtracking.
			conflictTerm, termErr := r.log.term(msg.LogIndex)
			if termErr != nil {
				// Entry compacted or unavailable; fall back to
				// index-based hint.
				hintIndex = msg.LogIndex
			} else {
				hintTerm = conflictTerm
				firstIdx := r.log.firstIndexOfTerm(conflictTerm)
				if firstIdx == 0 {
					firstIdx = msg.LogIndex
				}
				hintIndex = firstIdx
			}
		}
		r.send(proto.Message{
			Type:     proto.ReplicateResp,
			To:       msg.From,
			LogIndex: msg.LogIndex,
			Reject:   true,
			Hint:     hintIndex,
			LogTerm:  hintTerm,
		})
	}
	return nil
}

// declineRemote handles a rejected AppendEntries response using the
// term-based fast backtracking optimization from Raft Section 5.3.
//
// When hintTerm > 0, the follower included the term of its conflicting
// entry and the first index of that term. The leader uses this to skip
// entire terms:
//   - If the leader has entries at hintTerm: set next to the last index
//     of that term + 1 (skip past the leader's entries at the conflicting
//     term, since all prior entries at that term must also conflict).
//   - If the leader does NOT have entries at hintTerm: set next to
//     hintIndex (the follower's first index of the conflicting term),
//     skipping the entire conflicting term on the follower.
//
// When hintTerm == 0 (legacy/index-based): falls back to the existing
// index-based backtracking via remote.declineFrom.
//
// Returns true if the next index was actually changed.
func (r *raft) declineRemote(rm *remote, rejectedIndex, hintIndex, hintTerm uint64) bool {
	if hintTerm == 0 {
		// No term hint: fall back to index-based backtracking.
		return rm.declineFrom(rejectedIndex, hintIndex)
	}

	if rm.state == remoteReplicate {
		// In replicate state, a rejection means our optimistic next
		// was wrong. Fall back to match+1 which is known safe.
		if rejectedIndex <= rm.match {
			return false
		}
		rm.next = rm.match + 1
		return true
	}

	// Stale rejection check for probe/snapshot state.
	if rm.next-1 != rejectedIndex {
		return false
	}

	// Term-based backtracking: search the leader's log for the
	// conflicting term reported by the follower.
	leaderLastIdx := r.log.lastIndexOfTerm(hintTerm)
	if leaderLastIdx > 0 {
		// Leader has entries at the conflicting term. Set next to
		// one past the last entry at that term, since all entries
		// at this term before that point must also conflict.
		rm.next = leaderLastIdx + 1
	} else {
		// Leader does not have any entries at the conflicting term.
		// Use the follower's hint (first index of the conflicting
		// term) to skip the entire term on the follower's log.
		//
		// The hint is peer-supplied and is only ever meaningful as a
		// BACKTRACK, so it is clamped to the index that was rejected. Log
		// matching guarantees an honest follower can never name a higher one:
		// if its term at rejectedIndex is T and the leader's is lower, the
		// leader holds no entries at T at all (holding (rejectedIndex+1, T)
		// would require holding (rejectedIndex, T)), so lastIndexOfTerm(T)
		// is 0 and hintIndex <= rejectedIndex.
		//
		// Unclamped, a buggy or malicious follower replies with
		// Hint: 1<<62 and drives next past the end of the leader's log.
		// sendAppend's term() lookup then fails forever, and it falls through
		// to sendSnapshot — which, on a shard that has never compacted,
		// returns without touching rm.state or rm.next. No message is ever
		// sent to that peer again, so no rejection can arrive to backtrack it:
		// the replica removes itself from replication permanently while the
		// leader still reports it healthy. remote.declineFrom already applies
		// the identical clamp; this branch was the one that did not.
		rm.next = min(hintIndex, rejectedIndex)
	}

	// Ensure next never drops below 1.
	if rm.next < 1 {
		rm.next = 1
	}
	rm.probeSent = false
	return true
}

// handleAppendResp processes an AppendEntries response on the leader.
func (r *raft) handleAppendResp(msg proto.Message) error {
	rm := r.getRemote(msg.From)
	if rm == nil {
		return nil
	}
	rm.setActive()

	if msg.Reject {
		slog.Debug("append entries rejected",
			"shard", r.shardID,
			"replica", r.replicaID,
			"from", msg.From,
			"hint_index", msg.Hint,
			"hint_term", msg.LogTerm,
		)
		// Use term-based fast log backtracking (Section 5.3).
		// When the follower includes a conflicting term (LogTerm > 0),
		// the leader can skip entire terms rather than one index per RTT.
		if r.declineRemote(rm, msg.LogIndex, msg.Hint, msg.LogTerm) {
			if rm.state == remoteReplicate {
				rm.becomeProbe()
			}
			// Witnesses receive metadata-only entries via sendWitnessAppend,
			// not full entries via sendAppend. This matches broadcastAppend
			// and handleHeartbeatResp behavior.
			if _, isWitness := r.witnesses[msg.From]; isWitness {
				r.sendWitnessAppend(msg.From)
			} else {
				r.sendAppend(msg.From)
			}
		}
		return nil
	}

	// A match index beyond the leader's own log cannot be real: a follower
	// only ever echoes an index the leader sent it. Accepting it would push
	// rm.match past lastIndex and let tryCommit advance the commit index
	// over entries that do not exist.
	if msg.LogIndex > r.log.lastIndex() {
		slog.Warn("append response reports match beyond leader log",
			"shard", r.shardID,
			"replica", r.replicaID,
			"from", msg.From,
			"reported_index", msg.LogIndex,
			"last_index", r.log.lastIndex(),
		)
		return nil
	}

	// Success.
	updated := rm.update(msg.LogIndex)
	rm.respondedTo()

	// Log replication progress metrics for monitoring.
	slog.Debug("replication progress",
		"shard", r.shardID,
		"from", msg.From,
		"match_index", rm.match,
		"next_index", rm.next,
		"commit_index", r.log.committed,
		"lag", r.log.lastIndex()-rm.match,
	)

	if updated {
		if r.tryCommit() {
			// Commit index advanced. Broadcast so followers can advance
			// their own commit and apply committed entries.
			//
			// This reaches every peer that has entries outstanding, and every
			// peer NOT in the steady replicate state (probe/snapshot), where
			// sendAppend emits a message regardless. It does NOT reach a
			// follower that is fully caught up in remoteReplicate: sendAppend
			// returns early when there are no entries to send for such a peer,
			// so no empty append is generated to carry the new commit index
			// alone. Those followers learn the commit from the next heartbeat
			// (one HeartbeatRTT, one tick by default) or the next real append.
			//
			// That early return is a deliberate volume tradeoff: emitting an
			// empty append to every idle follower on every commit is a hot-path
			// cost. etcd/raft makes the opposite choice. Revisiting it is a
			// latency-vs-throughput decision to be benchmarked, not a
			// correctness fix — commit propagation is bounded either way.
			r.broadcastAppend()
		}
		// If leader transfer target is now caught up, send TimeoutNow.
		if r.transferTarget == msg.From && rm.match >= r.log.lastIndex() {
			r.sendTimeoutNow(msg.From)
		}
	}

	// Free inflights and send more if we can.
	if rm.state == remoteReplicate {
		rm.inflights.freeTo(msg.LogIndex)
		if !rm.inflights.full() {
			// Witnesses receive metadata-only entries via sendWitnessAppend,
			// not full entries via sendAppend. This matches broadcastAppend
			// and handleHeartbeatResp behavior.
			if _, isWitness := r.witnesses[msg.From]; isWitness {
				r.sendWitnessAppend(msg.From)
			} else {
				r.sendAppend(msg.From)
			}
		}
	}

	return nil
}

// --------------------------------------------------------------------------
// Commit tracking
// --------------------------------------------------------------------------

// tryCommit attempts to advance the commit index based on quorum
// replication. Uses a pre-allocated sort buffer.
func (r *raft) tryCommit() bool {
	r.matched = r.matched[:0]
	// The leader's own match is its DURABLE index (inmem.savedTo), not
	// lastIndex(). lastIndex() includes entries that exist only in memory
	// and have not been fsynced by the commit worker; counting them would
	// let a single follower ack complete a "quorum" in which the leader's
	// contribution is not crash-safe — a power failure before the fsync
	// then loses a committed entry. Commits blocked only on local
	// durability are re-evaluated at the persistence edge in Peer.Commit.
	r.matched = append(r.matched, r.log.inmem.savedTo)
	for _, rm := range r.remotes {
		r.matched = append(r.matched, rm.match)
	}
	// Witness matches (tracked via metadata-only Replicate acks) count toward
	// the unified quorum; see util.go quorum() for the safety rationale.
	for _, wm := range r.witnesses {
		r.matched = append(r.matched, wm.match)
	}
	slices.Sort(r.matched)
	q := quorum(r.remotes, r.witnesses)
	mci := r.matched[len(r.matched)-q]
	oldCommit := r.log.committed
	advanced := r.log.tryCommit(mci, r.term)
	if advanced {
		slog.Debug("commit index advanced",
			"shard", r.shardID,
			"replica", r.replicaID,
			"old_commit", oldCommit,
			"new_commit", r.log.committed,
			"term", r.term,
		)
	}
	return advanced
}

// --------------------------------------------------------------------------
// Heartbeat
// --------------------------------------------------------------------------

// broadcastHeartbeat sends heartbeat messages to all remotes, observers,
// and witnesses. If there are pending ReadIndex requests, the heartbeat
// carries the latest ReadIndex context so that follower responses can
// satisfy pending reads without a separate heartbeat round. This matches
// dragonboat's broadcastHeartbeatMessage() behavior.
func (r *raft) broadcastHeartbeat() {
	if len(r.pendingReadIndex) > 0 {
		r.broadcastHeartbeatCtx(r.readIndexCtx)
		r.readIndexBroadcastPending = false
	} else {
		r.broadcastHeartbeatCtx(0)
	}
}

// broadcastHeartbeatCtx sends heartbeat messages with a context key to
// all remotes, observers, and witnesses. The context key is used to
// correlate heartbeat responses with specific ReadIndex requests; the
// lease round (see nextLeaseRound) correlates them with the time the
// round was sent.
func (r *raft) broadcastHeartbeatCtx(ctx uint64) {
	round := r.nextLeaseRound()
	for id := range r.remotes {
		r.sendHeartbeatTo(id, ctx, round)
	}
	for id := range r.observers {
		r.sendHeartbeatTo(id, ctx, round)
	}
	for id := range r.witnesses {
		r.sendHeartbeatTo(id, ctx, round)
	}
}

// sendHeartbeatTo sends a heartbeat message with context to the given peer.
// LogIndex carries the lease round, which a Heartbeat otherwise leaves
// unused and handleHeartbeat echoes back unchanged.
func (r *raft) sendHeartbeatTo(to uint64, ctx uint64, round uint64) {
	commit := min(r.log.committed, r.getMatchForPeer(to))
	r.send(proto.Message{
		Type:     proto.Heartbeat,
		To:       to,
		Commit:   commit,
		Hint:     ctx,
		LogIndex: round,
	})
}

// handleHeartbeat processes a heartbeat message on a follower.
//
// A Heartbeat is the ONLY leader-to-follower push that carries no log-matching
// proof — no prevLogIndex, no prevLogTerm, no entries — yet it installs a leader
// and advances the commit index. handleAppend earns those rights by proving its
// sender holds a log that matches this replica's; a Heartbeat can prove nothing,
// so both rights are gated here instead.
//
//  1. SENDER. A replica that already recognises a leader for this term refuses a
//     Heartbeat from anyone else. Election Safety guarantees one leader per term,
//     so a second claimant at the same term is a stale duplicate or a forgery. A
//     replica with no leader yet (r.leaderID == noLeader) accepts, which is what
//     keeps first contact after an election working; step() has already installed
//     msg.From as leader for a HIGHER term before reaching here, so this gate is
//     about the current term only.
//
//  2. COMMIT. The advance goes through raftLog.tryCommit with the CURRENT TERM,
//     the same rule the leader applies to its own match-index quorum (Raft
//     §5.4.2), rather than through a bare commitTo clamped to lastIndex. The
//     difference is what a follower can PROVE locally: an entry at r.term was
//     appended by this term's leader through handleAppend, which log-matched the
//     whole prefix beneath it, so committing to it is sound. An entry from an
//     OLDER term is exactly the tail a new leader may still truncate, and the
//     bare clamp committed it on nothing but the sender's say-so.
//
// The two together close the forged-commit escalation: a Heartbeat naming a
// higher term is stepped down to by step(), but its commit claim then meets a
// log whose tail is at the OLD term and advances nothing; a Heartbeat at the
// CURRENT term never gets that far because the sender gate refuses it. The
// transport-layer leader-originated binding (see the message handler's
// leaderOriginatedMessages) is the third leg: it stops an arbitrary certificate
// holder from naming an established member's replica ID in the first place.
//
// LIVENESS: refusing the advance costs one heartbeat interval at most. A
// follower whose tail is behind the current term is by definition one the leader
// is still replicating to, and handleAppend advances its commit index as the
// entries land. A caught-up follower holds this term's entries (becomeLeader
// appends a no-op at r.term on every election), so the ordinary steady-state
// heartbeat advance is unaffected.
func (r *raft) handleHeartbeat(msg proto.Message) error {
	if r.leaderID != noLeader && msg.From != r.leaderID {
		slog.Warn("dropped heartbeat from a replica that is not this shard's leader",
			"shard", r.shardID,
			"replica", r.replicaID,
			"from", msg.From,
			"leader", r.leaderID,
			"msg_term", msg.Term,
			"term", r.term)
		return ErrHeartbeatNotFromLeader
	}

	r.electionTick = 0
	r.leaderID = msg.From

	if msg.Commit > r.log.committed {
		commitTo := min(msg.Commit, r.log.lastIndex())
		if !r.log.tryCommit(commitTo, r.term) && commitTo > r.log.committed {
			slog.Debug("heartbeat commit advance refused: log tail is not from the current term",
				"shard", r.shardID,
				"replica", r.replicaID,
				"from", msg.From,
				"msg_commit", msg.Commit,
				"commit_to", commitTo,
				"committed", r.log.committed,
				"last_index", r.log.lastIndex(),
				"term", r.term)
		}
	}

	// Echo back the context so the leader can correlate ReadIndex responses,
	// and the lease round so it can date the ack by when the heartbeat was
	// sent (hasValidLease), together with the vote window this replica
	// promises from now (voteWindow), which decides whether the leader may
	// count the ack toward a lease at all. The promise is taken on here,
	// before the ack leaves (promiseVotes).
	r.promiseVotes(msg.From)
	r.send(proto.Message{
		Type:     proto.HeartbeatResp,
		To:       msg.From,
		Hint:     msg.Hint,
		LogIndex: msg.LogIndex,
		LogTerm:  r.voteWindow(),
	})
	return nil
}

// handleHeartbeatResp processes a heartbeat response on the leader.
func (r *raft) handleHeartbeatResp(msg proto.Message) error {
	rm := r.getRemote(msg.From)
	if rm == nil {
		return nil
	}
	rm.setActive()

	// Track heartbeat ack for both lease-based reads and ReadIndex
	// leadership confirmation. Since witnesses vote in elections, the
	// lease and ReadIndex quorums use the unified quorum (remotes + witnesses
	// + self). All voting members must be tracked so their heartbeat
	// acks count toward the leadership confirmation quorum.
	//
	// The ack is dated by when the round it answers was SENT, never by when
	// it arrived or was processed: time spent between the two — a slow
	// network, or this leader descheduled with the ack queued — would
	// otherwise be credited to the lease. Only a newer round moves the date
	// forward, so a reordered older ack cannot pull it back either way.
	//
	// It counts only when the member promises, in the ack, a vote window at
	// least as long as this leader's lease (ackBacksLease): the lease is sound
	// only while every ack it rests on is backed by a member that will not
	// help elect anyone else before it ends, and that promise is made by the
	// member's own configuration, not this one's.
	if r.remoteLastAck != nil && r.ackBacksLease(msg) {
		_, isRemote := r.remotes[msg.From]
		_, isWitness := r.witnesses[msg.From]
		if sent, ok := r.leaseRoundSentAt(msg.LogIndex); ok && (isRemote || isWitness) {
			if prev, seen := r.remoteLastAck[msg.From]; !seen || sent.After(prev) {
				r.remoteLastAck[msg.From] = sent
			}
		}
	}

	// A heartbeat response means the peer is reachable. Always attempt
	// to send pending entries:
	//   - Probe: clear probeSent to unblock the single in-flight probe.
	//     The original MsgApp may have been lost during connection setup.
	//   - Replicate with stalled inflights: the leader has optimistically
	//     advanced next past the last log entry but still has unacknowledged
	//     in-flight messages. This means the transport silently dropped
	//     Replicate messages (non-blocking send queue) AND the follower's
	//     rejection responses were also lost. Without recovery, the leader
	//     keeps next ahead of lastIndex, entries(next, last+1) returns
	//     empty, and the follower never catches up (permanent stall).
	//
	//     The condition rm.next > lastIndex AND inflights > 0 precisely
	//     captures this stall: the leader has nothing more to send (next
	//     is past the end) yet entries remain unacknowledged. During normal
	//     replication next <= lastIndex (more entries to send) so this
	//     does not trigger. When fully caught up inflights == 0 so this
	//     also does not trigger.
	//
	//     Falling back to probe resets next=match+1 and clears inflights,
	//     allowing the leader to resend from the follower's actual position.
	//   - Replicate with a FULL window: the peer is paused purely because the
	//     window is full, and the window can only drain from ABOVE (freeTo
	//     frees records at or below an acknowledged index). If those messages
	//     went to a previous incarnation of the peer — it restarted — every
	//     ack it can still send reports a position BELOW the window and frees
	//     nothing, so the peer stays paused forever with the leader alive and
	//     committing. Release one slot so exactly one message gets through:
	//     if the window was merely busy it is acked normally, and if the peer
	//     restarted it is rejected, which backtracks next to the peer's real
	//     position and resumes replication. This is etcd/raft's
	//     MsgHeartbeatResp behavior; the primitive it needs (freeFirstOne)
	//     had been dropped here as unused.
	// sendAppend internally checks isPaused() and skips if appropriate.
	//
	// forceSend records that a slot was released so the send below is not
	// skipped for a ReadIndex heartbeat: under sustained ReadIndex load every
	// heartbeat carries a context (broadcastHeartbeat), so a Hint==0 response
	// may never arrive, and a slot released without a message to fill it is
	// window capacity silently leaked.
	forceSend := false
	switch {
	case rm.state == remoteProbe:
		rm.probeSent = false
	case rm.state == remoteReplicate &&
		rm.next > r.log.lastIndex() &&
		rm.inflights.Count() > 0:
		rm.becomeProbe()
	case rm.state == remoteReplicate && rm.inflights.full():
		rm.inflights.freeFirstOne()
		forceSend = true
	}

	// Skip sendAppend for ReadIndex heartbeats when the follower is caught
	// up. ReadIndex heartbeats (Hint > 0) only confirm quorum liveness -
	// they don't require replication. Sending empty Replicate messages wastes
	// network bandwidth and step worker CPU for each follower.
	//
	// When Hint == 0, it's a regular periodic heartbeat response and
	// sendAppend must fire as a recovery path for dropped MsgApp.
	// When the remote is NOT in Replicate state (e.g., Probe after stall
	// recovery above), sendAppend is needed to re-establish replication.
	//
	// Witnesses receive metadata-only entries via sendWitnessAppend, not
	// full entries via sendAppend. This matches broadcastAppend behavior.
	if msg.Hint == 0 || rm.state != remoteReplicate || forceSend {
		if _, isWitness := r.witnesses[msg.From]; isWitness {
			r.sendWitnessAppend(msg.From)
		} else {
			r.sendAppend(msg.From)
		}
	}

	// Handle read index confirmation using heartbeat context correlation.
	// The <= comparison allows a single heartbeat response to confirm all
	// pending ReadIndex requests with ctx <= the response ctx. This is safe
	// because ctx is monotonically increasing within a term: a heartbeat
	// response confirming leadership at ctx X also confirms leadership at
	// all earlier ctx values. Regular heartbeats use ctx=0 and ReadIndex
	// entries have ctx >= 1, so <= 0 never matches any ReadIndex entry.
	//
	// That reasoning holds only for a context this leader ISSUED. r.readIndexCtx
	// is the highest one ever assigned in this tenure — becomeLeader zeroes it,
	// handleLeaderReadIndex is the only writer and only ever increments, and
	// every broadcast stamps its then-current value — so the set of contexts a
	// heartbeat of this leader's can possibly have carried is exactly
	// {0} u [1, readIndexCtx]. A response echoing anything ABOVE that bound
	// corresponds to no heartbeat this leader ever sent, and under the <= rule
	// it would ack EVERY pending read from one message, including reads whose
	// confirming heartbeat has not been broadcast yet (handleLeaderReadIndex
	// defers the broadcast to flushReadIndex). With the leader's own pre-set
	// self-ack that is already a quorum of two, so a single stale or forged
	// response would resolve a linearizable read at a committed index no
	// heartbeat round ever validated — the exact guarantee ReadIndex exists to
	// provide. Refuse the message rather than clamping it: an unissued context
	// is not a weaker proof of liveness, it is not this leader's protocol at
	// all, and the peer's setActive/replication handling above has already run.
	if msg.Hint > r.readIndexCtx {
		slog.Warn("dropped heartbeat response echoing an unissued read index context",
			"shard", r.shardID,
			"replica", r.replicaID,
			"from", msg.From,
			"resp_ctx", msg.Hint,
			"issued_ctx", r.readIndexCtx,
			"term", r.term)
		return ErrReadIndexContextNotIssued
	}
	if len(r.pendingReadIndex) > 0 {
		ctx := msg.Hint
		bitPos, known := r.replicaToBit[msg.From]
		if known {
			mask := uint64(1) << bitPos
			for i := range r.pendingReadIndex {
				// Entries are appended in ctx order (monotonically
				// increasing). Once ctx exceeds the response context,
				// all subsequent entries also exceed it. Break early
				// to avoid scanning the entire slice.
				if r.pendingReadIndex[i].ctx > ctx {
					break
				}
				if r.pendingReadIndex[i].ackBits&mask == 0 {
					r.pendingReadIndex[i].ackBits |= mask
					r.pendingReadIndex[i].ackCount++
				}
			}
		}
		r.checkReadIndexQuorum()
	}

	return nil
}

// --------------------------------------------------------------------------
// Snapshot handling
// --------------------------------------------------------------------------

// snapshotVotingBitCount returns how many ReadIndex ackBits positions the given
// membership would consume on this replica. It deliberately mirrors the
// arithmetic of rebuildRemotesFromMembership followed by rebuildReplicaToBit —
// remotes are Addresses minus self, witnesses are Witnesses minus self, plus
// one bit for self — rather than counting the maps directly, because the two
// differ whenever the snapshot names a configuration this replica is not in,
// and it is rebuildReplicaToBit's count that has to be bounded. Observers are
// excluded for the same reason they are there: they get no bit.
func snapshotVotingBitCount(m proto.Membership, self uint64) int {
	// Self always occupies bit 0, whether or not the membership lists it.
	count := 1
	for id := range m.Addresses {
		if id != self {
			count++
		}
	}
	for id := range m.Witnesses {
		if id != self {
			count++
		}
	}
	return count
}

// handleSnapshot processes an InstallSnapshot message on a follower.
// Per Raft Figure 13: if existing log entry has same index and term as
// snapshot's last included entry, retain log entries following it.
// Remotes are rebuilt from the snapshot membership only when the log
// is fully replaced (non-matching case). When entries are retained,
// they may contain config changes newer than the snapshot's membership,
// so rebuilding would lose those changes.
//
// SENDER. An InstallSnapshot from a replica that is not the leader this node
// already recognises for the current term is refused, before it can touch any
// state. This is the same gate handleHeartbeat applies, applied to the strictly
// more powerful message: a Heartbeat carries no log-matching proof and may move
// the commit index, while an InstallSnapshot carries no proof either and may
// REPLACE the entire log and rebuild the voting set from a membership the sender
// chose. Election Safety says there is one leader per term, so a second claimant
// at the current term is a stale duplicate or a forgery.
//
// The two unconditional writes below are why the gate has to come first rather
// than only guarding the accepting branches. They run even on the REJECT path
// (a snapshot at or below the committed index), so before this gate any
// authenticated shard member could hold a healthy follower's election timer at
// zero indefinitely — and install itself as that follower's leaderID, which
// makes handleVoteRequest's leader-lease test deny PreVotes and dropHighTermVote
// discard higher-term RequestVotes — simply by retransmitting one long-obsolete
// snapshot. Suppressing an election is not a lesser power than winning one.
//
// The term dimension of a WIRE InstallSnapshot is covered upstream and
// deliberately not repeated: step() drops a lower-term one (answering with a
// corrective ReplicateResp) and, for a higher term, calls
// becomeFollower(msg.Term, msg.From) — InstallSnapshot is one of the
// leader-originated types it trusts to name a leader — so msg.From IS
// r.leaderID by the time the handler runs. The installs the host injects after
// receiving a snapshot are different: the snapshot itself never passed step()
// (the host's handler received it and authorized the sender against committed
// membership), and the injected message carries no message term (Term 0), so
// step()'s term gate does not run for it. Within raft the sender check below is
// its only gate: the install is adopted only from the replica this one already
// follows, or when it follows no one yet — the same power a member already has
// through a higher-term message. stepCandidate
// reaches the same state through stepDownToFollower. A replica with no leader
// for this term yet (leaderID == noLeader) accepts, which is what keeps first
// contact after an election working.
//
// LIVENESS: refusing costs at most one snapshot retry interval, and only in the
// case where this replica has already accepted a DIFFERENT leader — which by
// Election Safety means the sender was deposed. That leader is replicating to
// this replica and sends its own snapshot if one is needed. The locally injected
// installs (messagehandler's post-receive and its persisted re-delivery) carry
// the sending leader's replica ID in From, so they pass exactly when the wire
// message that triggered them would have.
//
// SIZE. A membership naming more voting members than the ReadIndex ackBits word
// can address is refused here, alongside the sender check and for the same
// reason: this is the one place a configuration enters the shard WITHOUT having
// passed an admission check. Proposals are bounded by
// validateConfigChangePolicy, bootstrap and import by Host's MaxVotingMembers
// checks; a snapshot's membership is bounded by nothing between the wire and
// rebuildRemotesFromMembership — proto decodes up to MaxMembershipEntries
// (10000) of them. Accepting one installed the whole map and returned success,
// and the damage surfaced later and elsewhere, as rebuildReplicaToBit's
// assertion panicking the step worker the next time this replica won an
// election. Being an assertion, it was fatal every time, so a single such
// snapshot cost the shard every future election on this replica.
//
// This is a REJECTION rather than a clamp because there is no correct way to
// serve a configuration that does not fit: dropping members to make it fit
// computes quorum over a set no committed entry put there, which is a
// split-brain, not a degradation. A legitimate leader cannot produce this
// message — the admission checks it passed to hold that configuration are the
// same bound — so the cost of refusing is zero and the sender is by definition
// buggy or hostile.
func (r *raft) handleSnapshot(msg proto.Message) error {
	// A peer-chosen position, so a check that fails the MESSAGE rather than an
	// invariant (see pkg/internal/invariant). The transport's chunk validation
	// and LogDB admission have already applied the same predicate; this is
	// the last gate before the position becomes the log's arithmetic base.
	if err := msg.Snapshot.CheckBounds(); err != nil {
		slog.Error("dropped install snapshot whose position is outside the protocol range",
			"shard", r.shardID,
			"replica", r.replicaID,
			"from", msg.From,
			"snapshot_index", msg.Snapshot.Index,
			"snapshot_term", msg.Snapshot.Term,
			"error", err)
		return err
	}
	if bits := snapshotVotingBitCount(msg.Snapshot.Membership, r.replicaID); bits > maxVotingMembers {
		slog.Error("dropped install snapshot naming more voting members than the ReadIndex ackBits word can address",
			"shard", r.shardID,
			"replica", r.replicaID,
			"from", msg.From,
			"voting_members", bits,
			"max_voting_members", maxVotingMembers,
			"snapshot_index", msg.Snapshot.Index,
			"snapshot_term", msg.Snapshot.Term)
		return ErrTooManyVotingMembers
	}
	if r.leaderID != noLeader && msg.From != r.leaderID {
		slog.Warn("dropped install snapshot from a replica that is not this shard's leader",
			"shard", r.shardID,
			"replica", r.replicaID,
			"from", msg.From,
			"leader", r.leaderID,
			"msg_term", msg.Term,
			"term", r.term,
			"snapshot_index", msg.Snapshot.Index)
		return ErrSnapshotNotFromLeader
	}

	r.electionTick = 0
	r.leaderID = msg.From

	ss := msg.Snapshot
	if ss.Index <= r.log.committed {
		// Snapshot is behind our committed index; reject it.
		slog.Info("snapshot rejected, behind committed index",
			"shard", r.shardID,
			"replica", r.replicaID,
			"from", msg.From,
			"snapshot_index", ss.Index,
			"snapshot_term", ss.Term,
			"committed", r.log.committed,
		)
		r.send(proto.Message{
			Type:     proto.ReplicateResp,
			To:       msg.From,
			LogIndex: r.log.committed,
		})
		return nil
	}

	// Per Raft Figure 13: if existing log entry has same index and
	// term as snapshot's last included entry, retain following entries.
	//
	// The matching branch is additionally gated on the log holding NO
	// pending (not yet persisted) snapshot. GetUpdate ships that pending
	// snapshot to the engine, which registers it with the LogReader and
	// recovers the state machine from it; it is cleared only by the
	// engine's persistence ack (commitUpdate -> savedSnapshot). Taking the
	// matching branch while one is pending fast-forwards committed AND
	// processed past a snapshot the engine has not been told about yet, so
	// the next GetUpdate emits the now-SUPERSEDED pending snapshot: the
	// LogReader's tracked range is rewound behind the apply frontier (or
	// ApplySnapshot rejects it as out of date, failing the shard in
	// Peer.Commit), and the entries in (pending.Index, ss.Index] are never
	// emitted for apply. A pending snapshot is always at or below committed
	// (restore sets committed to its index), and ss.Index is above committed
	// here, so this snapshot is strictly newer: restoring from it keeps the
	// pending snapshot monotonic and the engine always sees the newest one.
	// The restore discards entries retained above ss.Index, but while a
	// snapshot is pending nothing above it has been persisted or
	// acknowledged (Peer.Commit persists entries and clears the pending
	// snapshot in the same cycle), so the leader simply re-sends them.
	if t, err := r.log.term(ss.Index); err == nil && t == ss.Term &&
		r.log.pendingSnapshot() == nil {
		// Log matches at snapshot point; advance commit AND the
		// processed marker. The engine restores the state machine from
		// the received snapshot (HandleSnapshot requests SM recovery for
		// every install raft adopts, this branch included), so every entry at
		// or below ss.Index is already represented in the recovered
		// state. Leaving processed behind would re-emit those entries
		// to the apply worker and double-apply them on top of the
		// recovered state, violating state-machine safety.
		// Do NOT rebuild remotes here: retained entries may contain
		// config changes newer than the snapshot's membership.
		// Overwriting would lose those changes.
		slog.Info("snapshot accepted, log matches at snapshot point",
			"shard", r.shardID,
			"replica", r.replicaID,
			"from", msg.From,
			"snapshot_index", ss.Index,
			"snapshot_term", ss.Term,
		)
		r.log.commitTo(ss.Index)
		r.log.appliedTo(ss.Index)
	} else {
		// Log does not match; discard and restore from snapshot.
		slog.Info("snapshot accepted, restoring from snapshot",
			"shard", r.shardID,
			"replica", r.replicaID,
			"from", msg.From,
			"snapshot_index", ss.Index,
			"snapshot_term", ss.Term,
		)
		r.log.restore(ss)
		// Rebuild remotes from snapshot membership only in the
		// non-matching case where the log is fully replaced.
		// In the matching case above, the log retains entries
		// beyond the snapshot point which may contain config
		// changes; rebuilding from the snapshot's older membership
		// would lose those changes.
		r.rebuildRemotesFromMembership(ss.Membership)
	}

	r.send(proto.Message{
		Type:     proto.ReplicateResp,
		To:       msg.From,
		LogIndex: ss.Index,
	})
	return nil
}

// rebuildRemotesFromMembership rebuilds the remote tracking maps from
// the given membership. This is called after snapshot restore to ensure
// the raft node tracks the correct set of peers.
func (r *raft) rebuildRemotesFromMembership(m proto.Membership) {
	// The snapshot's membership carries the replicated witness policy.
	r.maxWitnesses = m.MaxWitnesses
	r.requireFullReplicaMajority = m.RequireFullReplicaMajority
	nextIdx := r.log.lastIndex() + 1
	r.remotes = make(map[uint64]*remote)
	for id := range m.Addresses {
		if id != r.replicaID {
			r.remotes[id] = newRemote(nextIdx, maxInflightMsgs)
		}
	}
	r.observers = make(map[uint64]*remote)
	for id := range m.Observers {
		if id != r.replicaID {
			r.observers[id] = newRemote(nextIdx, maxInflightMsgs)
		}
	}
	r.witnesses = make(map[uint64]*remote)
	for id := range m.Witnesses {
		if id != r.replicaID {
			r.witnesses[id] = newRemote(nextIdx, maxInflightMsgs)
		}
	}
	// A snapshot can carry a configuration this replica is no longer in
	// (it was removed while it lagged), so self membership is re-derived
	// here before purgeDepartedVotes drops the now-invalid self vote.
	r.setSelfMembership(membershipHasSelf(m, r.replicaID))
	_, listedWitness := m.Witnesses[r.replicaID]
	_, listedObserver := m.Observers[r.replicaID]
	r.checkSelfRole(membershipHasSelf(m, r.replicaID), listedWitness, listedObserver)
	r.purgeDepartedVotes()
}

// snapshotAckWaitMultiplier sizes the post-transfer ack wait as a multiple
// of the election timeout. Transfer success is reported when the last chunk
// hits the wire; the follower's ack arrives only after it reassembles the
// chunks, persists them, and its raft layer processes the InstallSnapshot —
// which under load can exceed a single election timeout. A premature expiry
// costs a redundant snapshot re-stream (the receiver dedups it), so the
// wait errs generous; a crashed follower just waits this long before the
// leader's retry resumes.
const snapshotAckWaitMultiplier = 3

// handleSnapshotStatus processes a snapshot transfer status report on the
// leader. The status is bound to the exact transfer attempt by the
// sequence in msg.Hint (stamped by sendSnapshot, echoed by the transport);
// a stale report from a superseded attempt — including a re-send of the
// SAME snapshot index — is discarded so it cannot arm the ack countdown
// (or force a backoff) for a newer in-flight stream. Both outcomes keep
// the peer paused in remoteSnapshot with a countdown armed (see
// remote.snapshotDone): on success the peer exits via its own raft-level
// ack (ReplicateResp → respondedTo) or the countdown fallback; on failure
// the countdown is a retry backoff, preventing a persistently failing
// transfer (unreachable peer, full receiver disk) from re-streaming full
// snapshots back-to-back.
func (r *raft) handleSnapshotStatus(msg proto.Message) error {
	rm := r.getRemote(msg.From)
	if rm == nil {
		return nil
	}
	if rm.state != remoteSnapshot || msg.Hint != rm.snapshotSeq {
		return nil
	}
	if msg.Reject {
		rm.snapshotFailed(r.electionTimeout)
	} else {
		rm.snapshotDone(snapshotAckWaitMultiplier * r.electionTimeout)
	}
	return nil
}

// tickSnapshotWaits advances the snapshot ack/retry countdown (see
// remote.snapshotDone) for every peer category that can receive snapshots
// (remotes, observers, witnesses). A peer whose countdown expires falls
// back to probe so the normal retry path — including a fresh snapshot
// send — resumes. Called once per leader tick, before any quiesce
// early-return.
func (r *raft) tickSnapshotWaits() {
	expire := func(peers map[uint64]*remote) {
		for id, rm := range peers {
			if rm.tickSnapshotWait() {
				slog.Warn("snapshot ack timed out, falling back to probe",
					"shard", r.shardID,
					"replica", r.replicaID,
					"peer", id,
					"snapshot_index", rm.snapshotIndex,
				)
				rm.becomeProbe()
			}
		}
	}
	expire(r.remotes)
	expire(r.observers)
	expire(r.witnesses)
}

// handleUnreachable processes an unreachable report on the leader.
func (r *raft) handleUnreachable(msg proto.Message) error {
	rm := r.getRemote(msg.From)
	if rm == nil {
		return nil
	}
	if rm.state == remoteReplicate {
		rm.becomeProbe()
	}
	return nil
}

// --------------------------------------------------------------------------
// Rate limiting
// --------------------------------------------------------------------------

// tickRateLimit sends rate limit feedback to the leader periodically.
// Called every tick from the follower's tick function.
func (r *raft) tickRateLimit() {
	if r.rl == nil || !r.rl.Enabled() {
		return
	}
	r.rateLimitTick++
	if r.rateLimitTick >= r.electionTimeout {
		r.rateLimitTick = 0
		r.sendRateLimitMessage()
	}
}

// sendRateLimitMessage sends the local in-memory log size to the leader
// via a RateLimit message. The Hint field carries the size.
func (r *raft) sendRateLimitMessage() {
	if r.leaderID == noLeader {
		return
	}
	r.send(proto.Message{
		Type: proto.RateLimit,
		To:   r.leaderID,
		Hint: r.rl.Get(),
	})
}

// handleLeaderRateLimit records a follower's in-memory log size for
// rate limiting decisions.
func (r *raft) handleLeaderRateLimit(msg proto.Message) error {
	if r.rl != nil && r.rl.Enabled() {
		r.rl.SetFollowerState(msg.From, msg.Hint)
	}
	return nil
}

// rateLimited returns true if the per-shard in-memory log has exceeded
// the configured rate limit threshold.
func (r *raft) rateLimited() bool {
	if r.rl == nil {
		return false
	}
	return r.rl.RateLimited()
}

// --------------------------------------------------------------------------
// Read index
// --------------------------------------------------------------------------

// hasCommittedEntryAtCurrentTerm returns true if the leader has committed
// at least one entry from its current term. Per PhD thesis 6.4, a new
// leader must commit its no-op entry before serving ReadIndex requests,
// otherwise the committed index may not reflect the true commit state.
func (r *raft) hasCommittedEntryAtCurrentTerm() bool {
	t, err := r.log.term(r.log.committed)
	if err != nil {
		return false
	}
	return t == r.term
}

// hasValidLease returns true if the leader holds a valid read lease.
//
// A lease is valid when a quorum of all voting members (remotes + witnesses
// + self) has acknowledged a heartbeat round that this leader SENT less than
// leaseDuration ago by its monotonic clock. Since witnesses vote in
// elections, the lease quorum must overlap with every possible
// election-winning coalition to prevent stale reads after leadership change.
//
// The safety argument (PhD 6.4): a follower that acks a heartbeat sent at T
// received it no earlier than T, reset its election timer then, and grants
// no vote to another candidate (handleVoteRequest, dropHighTermVote) until
// electionTimeout of its own ticks have elapsed. Its ticker can deliver one
// coalesced tick early, and one more can land just after the reset, so that
// window is at least electionTimeout - 2 tick intervals of real time —
// leaseTimeout. leaseDuration is that span less the clock-rate drift bound
// (leaseDriftDivisor), so a quorum of acks for rounds sent at or after T
// means no other leader can exist before T + leaseDuration. A follower that
// restarts in between loses that election timer; voteHeldDown carries the
// promise across the restart.
//
// Anchoring at SEND time is what makes a pause harmless. Measured from ack
// receipt, or in ticks, the lease would survive a leader that was
// descheduled for longer than the election timeout: its ticker drops the
// ticks it missed, so on resuming it sees one tick and still-fresh acks,
// while the followers have meanwhile elected a new leader and committed
// past it. Elapsed monotonic time since the round was sent covers the
// pause, so the lease is already expired and the read takes the heartbeat
// quorum round instead. A pause can only shorten the lease, never extend
// it.
//
// It assumes every replica runs the same ElectionRTT and tick interval
// (RTTMillisecond) and that clock rates differ by less than the drift
// bound. It does not survive a clock that stops or jumps backward, which
// Go's monotonic clock does not do.
func (r *raft) hasValidLease() bool {
	if !r.leaseRead || r.state != leader || r.remoteLastAck == nil || r.leaseDuration <= 0 {
		return false
	}
	q := quorum(r.remotes, r.witnesses)
	count := 1 // count self
	now := r.now()
	for _, sent := range r.remoteLastAck {
		if elapsed := now.Sub(sent); elapsed >= 0 && elapsed < r.leaseDuration {
			count++
		}
	}
	return count >= q
}

// setTickInterval derives leaseDuration from the real duration of one tick:
// leaseTimeout ticks, shortened by the drift bound (rounded up, so the
// lease errs short). A non-positive result disables the lease.
//
// It also sizes the vote hold-down (sizeVoteHoldDown) from electionTimeout
// ticks of real time, deliberately NOT shortened by the drift bound (see
// voteHeldDown). An interval that gives no positive span leaves a LeaseRead
// replica's hold-down unsized, which holds votes down indefinitely rather
// than releasing them.
func (r *raft) setTickInterval(tick time.Duration) {
	if span, ok := ticksToDuration(r.electionTimeout, tick); ok {
		r.electionSpan = span
		r.sizeVoteHoldDown()
	}
	d, ok := ticksToDuration(r.leaseTimeout, tick)
	if !ok {
		r.leaseDuration = 0
		r.leaseSpan = 0
		return
	}
	r.leaseSpan = d
	r.leaseDuration = d - leaseDriftShortfall(d, r.leaseDriftPPM)
}

// ticksToDuration returns n ticks of the given interval as a duration, and
// false when that is not a positive duration: a non-positive count or
// interval, or a product too large for time.Duration. The overflow case is
// refused rather than wrapped, since a wrapped product is negative and would
// silently leave the vote hold-down with no length.
func ticksToDuration(n int, tick time.Duration) (time.Duration, bool) {
	if n <= 0 || tick <= 0 || int64(n) > math.MaxInt64/int64(tick) {
		return 0, false
	}
	return time.Duration(n) * tick, true
}

// armVoteHoldDown sets up the post-restart vote hold-down at construction.
// owed is the vote window the previous incarnation recorded in the hard
// state as possibly still owed.
//
// The hold-down is anchored here, not at the first tick: this replica
// answers RequestVote from the moment it is constructed, before any tick,
// and every ack a previous incarnation sent was sent before this peer
// existed. It applies when this replica runs LeaseRead (it will promise a
// window, and a restart must not break the promise) OR when a window is
// owed, whatever the current configuration: an operator may have restarted
// the replica into one that promises less, or nothing, and the leader still
// counting the old ack relies on the old promise.
//
// An owed window's length is known now; the current promise is known only
// once SetTickInterval supplies the tick, which then re-sizes the hold-down.
func (r *raft) armVoteHoldDown(owed uint64) {
	r.voteWindowOwed = owed
	if !r.leaseRead && owed == 0 {
		return
	}
	r.voteHoldArmed = true
	r.voteHoldStart = r.now()
	if !r.leaseRead {
		r.voteHoldDuration = time.Duration(min(owed, uint64(math.MaxInt64)))
	}
}

// sizeVoteHoldDown sets the hold-down's length to the longer of the window
// owed from before the restart and the window this incarnation promises, and
// raises the owed window to that promise. The raise reaches the hard state,
// and hard state is persisted before any ordered message — a HeartbeatResp
// among them — leaves this replica (the engine's SaveState-then-send order),
// so the promise is durable before the first ack that makes it.
func (r *raft) sizeVoteHoldDown() {
	promise := r.voteWindow()
	if promise > r.voteWindowOwed {
		r.voteWindowOwed = promise
	}
	if r.voteHoldArmed && !r.voteHoldOver {
		r.voteHoldDuration = time.Duration(min(r.voteWindowOwed, uint64(math.MaxInt64)))
	}
}

// promiseVotes takes on the vote promise a heartbeat ack is about to make to
// leader: for voteWindow from now on this replica's monotonic clock it grants
// no vote and starts no campaign (votePromised). A replica that promises no
// window — no LeaseRead, or no known tick — reads no clock.
func (r *raft) promiseVotes(leader uint64) {
	w := r.voteWindow()
	if w == 0 {
		return
	}
	if until := r.now().Add(time.Duration(w)); until.After(r.promiseUntil) {
		r.promiseUntil = until
	}
	r.promisedTo = leader
	r.promisedTerm = r.term
}

// votePromised reports whether a vote promise made in a heartbeat ack is
// still outstanding (promiseVotes).
//
// This is what keeps the promise while the replica runs. It is enforced in
// real time and in nothing else, because everything else a follower could
// lean on can end early. The election timer counts ticks, and the ticker is
// not re-phased by the heartbeat that resets the count, so the next tick can
// land at once and a minimum-length timer fires about one tick short of the
// window the ack promised. The known-leader tests (dropHighTermVote, the
// leader-active test in handleVoteRequest) end the moment any higher-term
// message — a delayed pre-vote rejection from an old campaign, say — moves
// the replica to a new term with no leader, after which the next candidate's
// vote is granted at once. Either would let this replica help elect a new
// leader inside a lease its ack still backs.
//
// A replica that has promised nothing returns before reading the clock.
func (r *raft) votePromised() bool {
	return !r.promiseUntil.IsZero() && r.now().Before(r.promiseUntil)
}

// transferReleasesPromise reports whether a vote request comes from the
// candidate of a leadership transfer started by the leader this replica's
// newest promise was made to: a transfer-hinted request (Hint == From) for
// the term after the promise's. One term has one leader, so that transfer was
// started by the promisee, and a leader clears its own lease before it starts
// a transfer (handleLeaderTransfer), so no lease rests on the promise any more.
// A transfer hint at any other term proves nothing about the promisee and
// releases nothing.
func (r *raft) transferReleasesPromise(msg proto.Message) bool {
	return msg.Hint == msg.From && r.promisedTerm != 0 && msg.Term == r.promisedTerm+1
}

// voteWindow is the vote window this replica promises in a heartbeat ack:
// how long, from the moment it acks, it will grant no vote to another
// candidate and start no election. The leader counts the ack toward a read
// lease only if this is at least the lease's span (ackBacksLease), so a
// member whose configuration makes a weaker promise than the leader assumes —
// a shorter ElectionRTT, a shorter RTTMillisecond, no CheckQuorum, or no
// LeaseRead — backs no lease rather than an unsound one.
//
// The promise is electionTimeout ticks of real time (electionSpan), and it is
// kept in real time on both paths an ack can outlive: while this replica
// runs, votePromised refuses every vote, pre-vote and campaign until the
// window has passed since the ack; if it restarts, the vote hold-down
// (voteHeldDown) does the same from construction, which is after the ack,
// for at least the window recorded as owed — and the promise is recorded as
// owed before any ack makes it (sizeVoteHoldDown). The record, not the
// configuration, is what the restarted replica honours, so restarting into a
// configuration that promises less, or nothing, does not break a promise
// already made.
//
// It is 0 — no promise — without LeaseRead, so a shard that does not use
// lease reads never records a window, reads no clock per heartbeat and never
// holds its votes down; on an observer, whose ack no lease quorum counts and
// who has no vote to withhold; and until the tick interval is known.
func (r *raft) voteWindow() uint64 {
	if !r.leaseRead || r.isObserver || r.electionSpan <= 0 {
		return 0
	}
	return uint64(r.electionSpan)
}

// ackBacksLease reports whether a heartbeat ack promises a vote window
// (voteWindow, carried in LogTerm) at least as long as this leader's lease
// span. The span is compared before the drift bound is taken off, which
// keeps the margin the uniform-configuration argument relies on: the lease
// ends 1% before the span on this clock, and the member's window, measured
// on its clock, lasts at least the span.
//
// An ack from a build that sent no window carries 0 and backs no lease, so
// reads take the heartbeat quorum round.
func (r *raft) ackBacksLease(msg proto.Message) bool {
	return r.leaseSpan > 0 && msg.LogTerm <= math.MaxInt64 &&
		time.Duration(msg.LogTerm) >= r.leaseSpan
}

// voteHeldDown reports whether this replica must still withhold its vote,
// from other candidates and from itself, because it restarted recently and a
// leader may still be counting an ack it sent before the restart. A replica
// that neither runs LeaseRead nor owes a window from before its restart
// returns false before reading the clock, so its vote handling is unchanged.
//
// The hazard it closes: the lease safety argument (hasValidLease) rests on a
// follower that acked a heartbeat sent at T granting no vote until its
// election timeout has elapsed. That promise lives in the follower's election
// timer, which a restart discards: the replica comes back with no known
// leader, so dropHighTermVote does not fire and handleVoteRequest grants the
// first up-to-date candidate. In a three-node shard, leader L holds a lease
// on its own ack plus F1's; F1 restarts and votes for F2, F2 wins with F1 and
// itself, and L — whose lease still counts F1's pre-restart ack — serves a
// read that misses F2's writes until that ack's round ages out.
//
// The window is anchored at construction (voteHoldStart) on this replica's
// monotonic clock, and lasts the longer of the window recorded as owed in the
// hard state and the one this incarnation promises (sizeVoteHoldDown). Every
// ack this replica sent before the restart promised a window no longer than
// the one recorded, was sent before construction, and answered a round sent
// no later than that, so the hold-down ends at least that window after any
// such round was sent. A leader counts an ack only when the window it
// promises covers the lease span (ackBacksLease): leaseTimeout =
// electionTimeout - 2 of the leader's tick intervals, of which the lease is
// the span less 1% (leaseDriftDivisor), on the leader's clock. With clock
// rates within that 1% of each other the window outlasts every lease a
// pre-restart ack can back by about two ticks, which is why the window
// itself needs no drift allowance of its own. Measuring in ticks instead
// would reintroduce the pause bug the lease was moved to real time to fix:
// a descheduled process's ticker coalesces the ticks it missed, so a tick
// count would declare the window over before the time had passed.
//
// Anchoring at the first tick instead would be later, not safer: votes
// arrive from the moment the peer is constructed, and a tick-anchored window
// would have to treat "no tick yet" as held anyway.
//
// A self-vote is a vote. A candidate counts its own grant, and a quorum that
// contains this replica's self-vote can omit every other member of the lease
// quorum just as a quorum containing a vote it granted can (F1 campaigning
// with F2's grant is the scenario above with the roles swapped), so
// startElection and handleTimeoutNow consult this too. That includes the
// leadership-transfer path: the Hint == From exemption is chosen by the
// sender, and a restarted replica cannot tell whether the leader asking for
// the transfer is the one whose lease its old ack backs. A transfer that
// needs this replica during the window times out and is retried by the
// caller. It costs no normal-case latency: a restarted replica's own
// election timer is randomized over [electionTimeout, 2*electionTimeout)
// ticks, which is already about the window.
//
// Only vote-granting is held down. The replica still accepts Replicate and
// Heartbeat from a leader and acks them, so the leader's CheckQuorum counts it
// active; an ack sent in this incarnation is covered by the ordinary
// election-timer denial (dropHighTermVote and the leader-active test in
// handleVoteRequest), which the window does not replace. A refused
// higher-term RequestVote still advances this replica's term in step(), as
// any refused vote does. Quiesce neither extends nor ends the window: it
// changes how often ticks arrive, and the window is not counted in ticks.
//
// Until SetTickInterval supplies the tick interval the window's length is
// unknown, so votes stay held. The leader side of the same peer grants no
// lease without the interval, but a leader on another host may, and it is
// that leader's lease this replica's old ack backs.
//
// Once the window has elapsed the result is latched, so the clock is read
// only while the window might still be open.
func (r *raft) voteHeldDown() bool {
	if !r.voteHoldArmed || r.voteHoldOver {
		return false
	}
	if r.voteHoldDuration > 0 && r.now().Sub(r.voteHoldStart) >= r.voteHoldDuration {
		r.voteHoldOver = true
		// Every promise an earlier incarnation made has now been outlived, so
		// only this incarnation's own is still owed. Lowering the record is
		// what lets a later restart into a configuration that promises less
		// stop paying for a promise nobody holds any more.
		r.voteWindowOwed = r.voteWindow()
		return false
	}
	return true
}

// nextLeaseRound returns the heartbeat round a broadcast about to be sent
// carries in LogIndex, or 0 when lease tracking is off (including during a
// leader transfer, so no ack of a heartbeat sent then can ever count).
//
// A new round opens only when the current one is older than
// leaseDuration/leaseRoundSlices; until then broadcasts reuse it. Reuse is
// safe because the round keeps the send time of its FIRST broadcast, which
// is earlier than any later one, so an ack of a reused round anchors the
// lease early, never late. It is also what bounds the rounds in flight:
// under sustained ReadIndex load every step cycle broadcasts, and a round
// per broadcast would evict a round from the window before its acks came
// back, leaving the lease unrenewable for as long as the load lasted.
func (r *raft) nextLeaseRound() uint64 {
	if r.remoteLastAck == nil || r.leaseDuration <= 0 {
		return 0
	}
	now := r.now()
	if r.leaseRound != 0 &&
		now.Sub(r.leaseRoundSent[r.leaseRound&leaseRoundMask]) < r.leaseDuration/leaseRoundSlices {
		return r.leaseRound
	}
	r.leaseRound++
	r.leaseRoundSent[r.leaseRound&leaseRoundMask] = now
	return r.leaseRound
}

// leaseRoundSentAt returns when heartbeat round was first sent. It reports
// false for round 0 (no round: lease tracking was off, or the follower
// predates rounds), for a round this leader never issued, and for one old
// enough to have left the window, none of which may renew the lease.
func (r *raft) leaseRoundSentAt(round uint64) (time.Time, bool) {
	if round == 0 || round > r.leaseRound || r.leaseRound-round >= leaseRoundWindow {
		return time.Time{}, false
	}
	return r.leaseRoundSent[round&leaseRoundMask], true
}

// handleLeaderReadIndex handles a read index request on the leader.
func (r *raft) handleLeaderReadIndex(msg proto.Message) error {
	r.recordActivity()

	// A leader that has just learned the replicated configuration calls it a
	// witness steps down in checkSelfRole, but a ReadIndex already in the
	// same inbox batch would still be answered from a state machine that
	// never saw the entry data. See checkSelfRole.
	if r.roleMismatch {
		return ErrWitnessRoleMismatch
	}

	// Per PhD thesis 6.4: leader must have committed an entry in its
	// current term before serving linearizable reads.
	if !r.hasCommittedEntryAtCurrentTerm() {
		return ErrReadIndexNotReady
	}

	// Per PhD thesis 6.4.1: block ReadIndex during active leadership
	// transfer. The old leader must not serve reads because the transfer
	// target may become leader and commit new writes at any moment.
	if r.transferTarget != noNode {
		return ErrTransferInProgress
	}

	// Single-node cluster or valid lease: respond immediately without
	// a heartbeat round-trip.
	// The lease is evaluated once: it reads the clock, and the log line
	// below would otherwise read it again on every fast-path read.
	singleVoter := numVotingMembers(r.remotes, r.witnesses) == 1
	leaseValid := !singleVoter && r.hasValidLease()
	if singleVoter || leaseValid {
		slog.Debug("read index fast path",
			"shard", r.shardID,
			"replica", r.replicaID,
			"key", msg.Hint,
			"commit_index", r.log.committed,
			"lease_valid", leaseValid,
		)
		if msg.From != 0 && msg.From != r.replicaID {
			// Forwarded from a follower; respond to originator.
			r.send(proto.Message{
				Type:     proto.ReadIndexResp,
				To:       msg.From,
				Hint:     msg.Hint,
				LogIndex: r.log.committed,
			})
		} else {
			if err := r.addReadState(proto.ReadyToRead{
				Key:   msg.Hint,
				Index: r.log.committed,
			}); err != nil {
				return err
			}
		}
		return nil
	}

	// No valid lease: confirm leadership via heartbeat quorum.
	// Cap pending reads to prevent unbounded growth when heartbeat
	// responses are lost or delayed under sustained ReadIndex load.
	if len(r.pendingReadIndex) >= maxPendingReadIndex {
		return ErrReadIndexOverloaded
	}

	// Pre-increment so the first ReadIndex ctx is 1, reserving
	// ctx=0 for regular heartbeats (broadcastHeartbeat). This prevents
	// regular heartbeat responses from falsely satisfying ReadIndex quorum.
	r.readIndexCtx++
	if r.readIndexCtx == 0 {
		r.readIndexCtx = 1
	}

	// Pre-set leader's self-ack at creation time. This avoids a redundant
	// map lookup + bitwise OR on every checkReadIndexQuorum call. The
	// leader always acks its own ReadIndex.
	selfBitPos := r.replicaToBit[r.replicaID]
	selfMask := uint64(1) << selfBitPos
	r.pendingReadIndex = append(r.pendingReadIndex, readIndexStatus{
		key:      msg.Hint,
		index:    r.log.committed,
		from:     msg.From,
		ctx:      r.readIndexCtx,
		ackBits:  selfMask,
		ackCount: 1,
	})

	slog.Debug("read index queued for heartbeat confirmation",
		"shard", r.shardID,
		"replica", r.replicaID,
		"key", msg.Hint,
		"commit_index", r.log.committed,
		"ctx", r.readIndexCtx,
		"pending_count", len(r.pendingReadIndex),
	)

	// Defer heartbeat broadcast. Multiple ReadIndex requests in the same
	// step cycle share a single heartbeat round. flushReadIndex() sends
	// one broadcast with the latest ctx, and the <= comparison in
	// handleHeartbeatResp confirms all pending entries at once.
	r.readIndexBroadcastPending = true
	return nil
}

// flushReadIndex sends a single heartbeat broadcast for all deferred
// ReadIndex requests accumulated in the current step cycle. This batches
// N concurrent ReadIndex requests into one heartbeat round, reducing
// network messages from N*(cluster_size-1) to (cluster_size-1).
//
// Called by Peer.FlushReadIndex after all inbox messages are processed.
func (r *raft) flushReadIndex() {
	if !r.readIndexBroadcastPending {
		return
	}
	r.readIndexBroadcastPending = false
	r.broadcastHeartbeatCtx(r.readIndexCtx)
	r.checkReadIndexQuorum()
}

// addReadState appends a resolved ReadIndex result to readStates if capacity
// permits. Returns ErrReadIndexOverloaded if the readStates slice has
// reached maxReadStates, preventing unbounded memory growth.
func (r *raft) addReadState(rs proto.ReadyToRead) error {
	if len(r.readStates) >= maxReadStates {
		return ErrReadIndexOverloaded
	}
	r.readStates = append(r.readStates, rs)
	return nil
}

// handleFollowerReadIndex handles a read index request on a follower
// by forwarding to the leader.
func (r *raft) handleFollowerReadIndex(msg proto.Message) error {
	// A replica whose local role contradicts the replicated configuration
	// must not serve a linearizable read: if the configuration lists it as a
	// witness, the leader has been sending it metadata-only entries and its
	// state machine never saw the data the read would return. See
	// checkSelfRole.
	if r.roleMismatch {
		return ErrWitnessRoleMismatch
	}
	if r.leaderID == noLeader {
		return &NotLeaderError{LeaderID: 0}
	}
	msg.To = r.leaderID
	r.send(msg)
	return nil
}

// handleReadIndexResp handles a read index response forwarded from leader.
//
// It is the WIRE ENCODING of a completed heartbeat-quorum round: the resulting
// ReadyToRead resolves a pending linearizable read at msg.LogIndex, and the
// correlation key is a plain monotonic counter (Host.readKeySeq), so it is
// guessable. A response from anyone but the leader this replica actually asked
// therefore serves a read at an index of the sender's choosing with no quorum
// ever run. handleFollowerReadIndex only ever forwards to r.leaderID, so
// requiring the answer to come from r.leaderID rejects exactly the messages
// that answer nothing.
//
// msg.LogIndex is NOT checked against r.log.committed, and must not be. The
// leader answers with ITS committed index, which is by construction at or ahead
// of this follower's — that gap is the whole point of ReadIndex, and the
// follower waits for lastApplied to reach the returned index before serving the
// read (see hostEngineCallback.OnReadyToRead). Rejecting LogIndex >
// r.log.committed would refuse the common case: every read taken while the
// follower is even one entry behind.
//
// The gate is in addition to, not instead of, the transport-layer sender
// binding: ReadIndexResp is a gated message type there, so msg.From is already
// tied to the address committed membership assigns it.
func (r *raft) handleReadIndexResp(msg proto.Message) error {
	if msg.From != r.leaderID || r.leaderID == noLeader {
		slog.Warn("dropped ReadIndexResp from a replica that is not this shard's leader",
			"shard", r.shardID,
			"replica", r.replicaID,
			"from", msg.From,
			"leader", r.leaderID,
			"msg_term", msg.Term,
			"term", r.term)
		return ErrReadIndexRespNotFromLeader
	}
	return r.addReadState(proto.ReadyToRead{
		Key:   msg.Hint,
		Index: msg.LogIndex,
	})
}

// checkReadIndexQuorum checks if enough heartbeat responses have been
// received to confirm leadership for pending read index requests.
//
// Resolved requests are compacted from the slice regardless of position.
// Earlier implementations broke at the first unresolved request, which
// caused head-of-line blocking: a slow heartbeat response for request N
// would stall request N+1 even if N+1 already had quorum. Since each
// read captures the committed index at request time and confirms
// leadership via its own heartbeat round, resolving out-of-order is safe.
func (r *raft) checkReadIndexQuorum() {
	if len(r.pendingReadIndex) == 0 {
		return
	}
	// ReadIndex confirms leadership via heartbeat responses from all voting
	// members (remotes + witnesses + self). Since witnesses vote in elections,
	// the leadership confirmation quorum must overlap with every possible
	// election-winning coalition to prevent stale reads after leadership change.
	// Use the unified quorum (remotes + witnesses + leader) for linearizability.
	q := quorum(r.remotes, r.witnesses)

	// Compact resolved entries by overwriting with unresolved ones.
	// Self-ack is pre-set at creation time in handleLeaderReadIndex,
	// so no self-bit check is needed here.
	n := 0
	resolvedCount := 0
	for i := range r.pendingReadIndex {
		pending := &r.pendingReadIndex[i]
		if pending.ackCount < q {
			r.pendingReadIndex[n] = r.pendingReadIndex[i]
			n++
			continue
		}
		if pending.from != 0 && pending.from != r.replicaID {
			// Forwarded from a follower; send response back.
			r.send(proto.Message{
				Type:     proto.ReadIndexResp,
				To:       pending.from,
				Hint:     pending.key,
				LogIndex: pending.index,
			})
		} else if err := r.addReadState(proto.ReadyToRead{
			Key:   pending.key,
			Index: pending.index,
		}); err != nil {
			// Local request, but readStates is full (ErrReadIndexOverloaded).
			// The result MUST NOT be dropped: a caller waiting without a
			// deadline never receives its ReadyToRead and hangs forever.
			// Retain the resolved request so it is retried on the next
			// compaction pass, when readStates has drained. The loop is
			// never blocked — the request simply stays pending.
			r.pendingReadIndex[n] = r.pendingReadIndex[i]
			n++
			continue
		}
		// Delivered (forwarded response sent, or read state accepted).
		resolvedCount++
		slog.Debug("read index confirmed",
			"shard", r.shardID,
			"replica", r.replicaID,
			"key", pending.key,
			"index", pending.index,
			"ctx", pending.ctx,
			"ack_count", pending.ackCount,
			"quorum", q,
		)
	}
	r.pendingReadIndex = r.pendingReadIndex[:n]
	if resolvedCount > 0 {
		slog.Debug("read index batch resolved",
			"shard", r.shardID,
			"replica", r.replicaID,
			"resolved", resolvedCount,
			"remaining", n,
		)
	}
}

// --------------------------------------------------------------------------
// Leader transfer
// --------------------------------------------------------------------------

// handleLeaderTransfer initiates a leadership transfer to the target node.
func (r *raft) handleLeaderTransfer(msg proto.Message) error {
	target := msg.Hint
	if target == 0 {
		target = msg.From
	}

	if target == r.replicaID {
		// Already leader.
		return nil
	}
	if r.transferTarget != noNode {
		return ErrTransferInProgress
	}

	// Exit quiesce so tickLeader runs the normal path with transfer
	// timeout logic. Without this, a quiesced leader that transfers to
	// an unreachable target would be permanently stuck: no proposals
	// accepted and the transfer never times out.
	r.exitQuiesce()

	rm, ok := r.remotes[target]
	if !ok {
		// The target is not a voting replica. Check whether it exists
		// as a witness or observer so we can return a precise error
		// instead of the misleading ErrNodeNotFound.
		if _, isWitness := r.witnesses[target]; isWitness {
			return ErrTransferTargetNotVoter
		}
		if _, isObserver := r.observers[target]; isObserver {
			return ErrTransferTargetNotVoter
		}
		return ErrNodeNotFound
	}

	r.transferTarget = target
	r.transferElapsed = 0

	// Per PhD thesis 6.4.1: a leader must expire its lease before
	// transferring leadership. During the transfer window, the old leader
	// must not serve LeaseReads because the new leader may begin committing
	// writes as soon as it wins the election.
	r.remoteLastAck = nil

	slog.Debug("leader transfer",
		"shard", r.shardID,
		"replica", r.replicaID,
		"target", target)

	if rm.match >= r.log.lastIndex() {
		r.sendTimeoutNow(target)
	} else {
		// Fast-track replication: reset probeSent so we can send
		// immediately even if a probe was already in flight.
		rm.probeSent = false
		r.sendAppend(target)
	}
	return nil
}

// sendTimeoutNow sends a TimeoutNow message to the given peer,
// instructing it to start an election immediately.
func (r *raft) sendTimeoutNow(to uint64) {
	slog.Info("leader transfer completing, sending TimeoutNow",
		"shard", r.shardID,
		"replica", r.replicaID,
		"target", to,
		"term", r.term,
	)
	// The lease is forfeit for the rest of this term; see timeoutNowSent.
	// handleLeaderTransfer already cleared it, and this keeps it cleared
	// even if the transfer is later abandoned.
	r.timeoutNowSent = true
	r.remoteLastAck = nil
	r.send(proto.Message{
		Type: proto.TimeoutNow,
		To:   to,
	})
}

// resumeLeaseAfterTransfer restores the lease tracking handleLeaderTransfer
// cleared, once a transfer is abandoned — unless a TimeoutNow was sent in
// this term. Such a message may still arrive, however late, and the vote
// promises it and its candidate's transfer-hinted votes release
// (votePromised) include any made after the abandonment; a lease resting on
// them would let the transfer's candidate be elected inside it. Reads then
// take the heartbeat quorum round until the next term.
func (r *raft) resumeLeaseAfterTransfer() {
	if r.leaseRead && r.remoteLastAck == nil && !r.timeoutNowSent {
		r.remoteLastAck = make(map[uint64]time.Time)
	}
}

// handleTimeoutNow processes a TimeoutNow message by immediately
// starting an election.
func (r *raft) handleTimeoutNow(msg proto.Message) error {
	// A removed replica must not campaign even when told to: a TimeoutNow
	// in flight when the removal committed would otherwise hand leadership
	// to a replica the configuration no longer contains. The same applies to
	// a replica whose local role contradicts the replicated configuration —
	// TimeoutNow is the campaign entry point that bypasses startElection.
	if r.isWitness || r.selfRemoved {
		return nil
	}
	// TimeoutNow campaigns IMMEDIATELY and bypasses PreVote, so a stale voting
	// set here is decided in a single round. Fold the committed configuration
	// tail first, for the reasons startElection spells out; a transferee is
	// caught up on the LOG by definition (the leader only transfers to a
	// matched replica) but that says nothing about whether it has folded the
	// config changes that log contains.
	if !r.configCurrentForCampaign() {
		return ErrStaleConfigCannotCampaign
	}
	// The fold may have applied a committed RemoveNode for this replica that
	// the base configuration did not carry — or the AddNode promoting it, which
	// is why the observer test sits here and not in the pre-fold gate (see
	// startElection).
	if r.selfRemoved || r.isObserver {
		return nil
	}
	if r.roleMismatch {
		slog.Warn("ignoring TimeoutNow: local role contradicts the replicated membership",
			"shard", r.shardID,
			"replica", r.replicaID,
			"configured_witness", r.isWitness,
			"term", r.term)
		return ErrWitnessRoleMismatch
	}
	// A joining replica must not campaign even when told to; see the joining
	// field. Peer.Handle refuses the wire TimeoutNow before it reaches step,
	// but the gate belongs here too: this is the campaign entry point that
	// bypasses startElection, and a locally injected TimeoutNow reaches it
	// directly.
	if r.joining {
		slog.Warn("ignoring TimeoutNow: still joining with incomplete membership",
			"shard", r.shardID,
			"replica", r.replicaID,
			"term", r.term)
		return ErrJoiningNodeCannotCampaign
	}
	// Nor may a replica inside its post-restart vote hold-down, however it
	// was asked: the transfer's campaign would open with this replica's
	// self-vote while a leader may still count its pre-restart ack toward a
	// read lease (voteHeldDown). The leader's transfer times out and can be
	// retried once the window has passed.
	//
	// A live promise is released only by the leader it was made to asking for
	// the transfer at the promise's own term: that leader cleared its lease
	// before sending TimeoutNow (handleLeaderTransfer). From anyone else, or at
	// another term, the promise stands and the transfer is refused the same way.
	promiseStands := r.votePromised() && (msg.From != r.promisedTo || msg.Term != r.promisedTerm)
	if r.voteHeldDown() || promiseStands {
		slog.Warn("ignoring TimeoutNow: a leader may be counting this replica's ack toward a read lease",
			"shard", r.shardID,
			"replica", r.replicaID,
			"from", msg.From,
			"term", r.term)
		return ErrLeaseVoteHoldDown
	}
	// A leader must never call becomeCandidate (invariant assertion).
	// This can happen if a same-term TimeoutNow arrives at the leader
	// (e.g., stale message from a previous transfer round).
	if r.state == leader {
		return nil
	}
	// Refuse to campaign at all when this replica's own last log term is
	// unreadable, BEFORE becomeCandidate runs.
	//
	// campaign() checks the same thing and returns without sending anything —
	// a vote request advertising LogTerm 0 is rejected by every peer holding a
	// non-empty log, so the round cannot be won. But by then becomeCandidate
	// has already incremented the term, cleared leaderID and reset the timers,
	// leaving this replica parked at T+1 having sent zero RequestVotes. The
	// term is not inert: send() stamps r.term on every reply, so it reaches the
	// live leader through ordinary Replicate/Heartbeat rejections and deposes
	// it — a healthy leader lost to a transient local storage fault, for an
	// election that never happened.
	//
	// startElection carries the same bar. TimeoutNow is the OTHER campaign
	// entry point, which is why the bar
	// is repeated here rather than left to the shared campaign() call below —
	// the same reason every gate above it is repeated.
	//
	// Refusing costs only liveness, and only for this round: the leader retries
	// the transfer on its transfer timeout, and the ordinary election timeout
	// campaigns normally once the log becomes readable again.
	if _, ok := r.log.lastTerm(); !ok {
		slog.Warn("ignoring TimeoutNow: local last log term unreadable",
			"shard", r.shardID,
			"replica", r.replicaID,
			"term", r.term,
			"last_index", r.log.lastIndex())
		return ErrUnreadableLogCannotCampaign
	}
	// Exit quiesce before starting the election. Without this, a
	// quiesced follower that wins the election would use the quiesce
	// liveness probe interval instead of normal heartbeats, and if
	// the election fails, tickFollower would use the extended quiesce
	// election timeout (30x normal).
	r.exitQuiesce()
	// Start an election immediately, bypassing PreVote. The votes carry
	// the leadership-transfer hint so peers with a fresh leader lease
	// still grant them.
	r.becomeCandidate()
	r.broadcastTransferVote()
	r.checkElectionResult()
	return nil
}

// --------------------------------------------------------------------------
// Quiesce
// --------------------------------------------------------------------------

// broadcastQuiesce sends a quiesce notification to all peers.
func (r *raft) broadcastQuiesce() {
	for id := range r.remotes {
		r.send(proto.Message{
			Type: proto.Quiesce,
			To:   id,
		})
	}
	for id := range r.observers {
		r.send(proto.Message{
			Type: proto.Quiesce,
			To:   id,
		})
	}
	for id := range r.witnesses {
		r.send(proto.Message{
			Type: proto.Quiesce,
			To:   id,
		})
	}
}

// handleQuiesceMessage processes a quiesce notification from the leader.
//
// Quiesce is a local configuration choice, not replicated state, so a peer
// cannot impose it: a node started with Quiesce disabled must keep its
// normal election timer. Entering quiesce switches tickFollower to
// quiesceElectionTimeout() (livenessInterval * 3 = electionTimeout * 30),
// which on an operator-configured non-quiescing node turns a leader failure
// into a multi-second leaderless window it never opted into.
func (r *raft) handleQuiesceMessage(msg proto.Message) {
	if !r.quiesce.enabled {
		return
	}
	if r.state == follower && msg.From == r.leaderID {
		r.quiesce.active = true
		r.quiesce.livenessTick = 0
	}
}

// --------------------------------------------------------------------------
// Helper functions
// --------------------------------------------------------------------------

// send appends a message to the outbound message buffer. The message's
// From, Term, ShardID, and ReplicaID fields are populated automatically.
func (r *raft) send(msg proto.Message) {
	msg.From = r.replicaID
	if msg.Term == 0 {
		msg.Term = r.term
	}
	msg.ShardID = r.shardID
	msg.ReplicaID = r.replicaID
	r.msgs = append(r.msgs, msg)
}

// drainMessages returns and clears the outbound message buffer using
// a double-buffer swap to avoid allocation. Returns nil when empty.
func (r *raft) drainMessages() []proto.Message {
	if len(r.msgs) == 0 {
		return nil
	}
	msgs := r.msgs
	r.msgs = r.msgsBuf[:0]
	r.msgsBuf = msgs[:0]
	return msgs
}

// withholdOrderedMessages partitions an ALREADY-DRAINED batch: it compacts the
// free-order messages to the front of msgs (returning that prefix) and pushes
// the ordered ones back into r.msgs so a later, durability-bearing cycle emits
// them.
//
// It is the drain-first counterpart of drainFreeOrderMessages and exists
// because the two run on different goroutines. drainFreeOrderMessages returns
// r.freeMsgBuf, a single buffer owned by the step worker; the commit worker
// calls this one on the piggyback path while the step worker is concurrently
// inside processCommitPendingInbox, so reusing freeMsgBuf there would let one
// worker overwrite the batch the other is still sending. Operating on the
// caller's already-swapped drainMessages() slice keeps each worker on its own
// storage while applying the identical proto.IsFreeOrderMessage rule.
//
// The in-place compaction is safe because the write index never runs ahead of
// the read index, and r.msgs is the OTHER half of the drainMessages double
// buffer, so appending to it cannot alias msgs.
func (r *raft) withholdOrderedMessages(msgs []proto.Message) []proto.Message {
	n := 0
	for i := range msgs {
		if proto.IsFreeOrderMessage(msgs[i].Type) {
			msgs[n] = msgs[i]
			n++
		} else {
			r.msgs = append(r.msgs, msgs[i])
		}
	}
	return msgs[:n]
}

// requeueMessages puts previously drained messages back at the FRONT of the
// outbound buffer. Used when the engine drained messages, failed to persist
// the state they were waiting on, and must hand them back rather than drop
// them. Front placement preserves emission order: everything currently in
// r.msgs was produced after these were drained.
func (r *raft) requeueMessages(msgs []proto.Message) {
	if len(msgs) == 0 {
		return
	}
	if len(r.msgs) == 0 {
		r.msgs = append(r.msgs, msgs...)
		return
	}
	combined := make([]proto.Message, 0, len(msgs)+len(r.msgs))
	combined = append(combined, msgs...)
	combined = append(combined, r.msgs...)
	r.msgs = combined
}

// drainFreeOrderMessages extracts only free-order messages (Replicate,
// Ping, Heartbeat, Quiesce) from the outbound buffer, leaving ordered
// messages (VoteResp, HeartbeatResp, ReplicateResp, etc.) in place.
// This is used by the step worker during commitPending ticks: heartbeats
// must be sent immediately for leader liveness, but ordered messages must
// remain in the buffer until the next GetUpdate after WAL persistence.
func (r *raft) drainFreeOrderMessages() []proto.Message {
	if len(r.msgs) == 0 {
		return nil
	}
	r.freeMsgBuf = r.freeMsgBuf[:0]
	n := 0
	for i := range r.msgs {
		if proto.IsFreeOrderMessage(r.msgs[i].Type) {
			r.freeMsgBuf = append(r.freeMsgBuf, r.msgs[i])
		} else {
			r.msgs[n] = r.msgs[i]
			n++
		}
	}
	r.msgs = r.msgs[:n]
	if len(r.freeMsgBuf) == 0 {
		return nil
	}
	return r.freeMsgBuf
}

// getRemote returns the remote tracker for a peer. Checks remotes first,
// then observers, then witnesses. Returns nil if not found.
// membershipKnown reports whether this replica knows a configuration at all.
//
// It cannot be answered from the remotes/observers/witnesses maps alone: those
// deliberately EXCLUDE self, so a single-replica shard has all three empty while
// its configuration is perfectly well known. Self-membership carries the rest of
// the answer, which is what selfRemoved tracks.
//
// False means this replica holds no configuration whatsoever — a joiner that has
// not yet received the bootstrap entry. Callers that gate on the configuration
// must skip their check in that state rather than treat "I know nothing" as "you
// are not a member".
func (r *raft) membershipKnown() bool {
	if !r.selfRemoved {
		return true
	}
	return len(r.remotes)+len(r.observers)+len(r.witnesses) > 0
}

// isKnownMember reports whether id is this replica or appears in the current
// configuration in any role.
func (r *raft) isKnownMember(id uint64) bool {
	return id == r.replicaID || r.getRemote(id) != nil
}

func (r *raft) getRemote(id uint64) *remote {
	if rm, ok := r.remotes[id]; ok {
		return rm
	}
	if rm, ok := r.observers[id]; ok {
		return rm
	}
	if rm, ok := r.witnesses[id]; ok {
		return rm
	}
	return nil
}

// getMatchForPeer returns the match index for a peer, used to bound
// the commit index in heartbeats. Returns 0 for unknown peers.
func (r *raft) getMatchForPeer(to uint64) uint64 {
	if rm, ok := r.remotes[to]; ok {
		return rm.match
	}
	if rm, ok := r.observers[to]; ok {
		return rm.match
	}
	// Witnesses have a remote struct with a valid match field that gets
	// updated when they respond to Replicate messages.
	if rm, ok := r.witnesses[to]; ok {
		return rm.match
	}
	return 0
}

// resetElectionTimeout randomizes the election timeout to prevent
// synchronized elections.
func (r *raft) resetElectionTimeout() {
	r.randomizedElectionTimeout = r.electionTimeout + r.rng.Intn(r.electionTimeout)
}

// hasQuorumActive checks if a quorum of remotes (and witnesses) have
// been active recently.
func (r *raft) hasQuorumActive() bool {
	activeCount := 1 // count self
	for _, rm := range r.remotes {
		if rm.isActive() {
			activeCount++
		}
	}
	for _, wm := range r.witnesses {
		if wm.isActive() {
			activeCount++
		}
	}
	return activeCount >= quorum(r.remotes, r.witnesses)
}

// logQuorumStatus logs the current quorum health status. Called
// periodically by the leader to provide visibility into cluster health.
func (r *raft) logQuorumStatus() {
	activeCount := 1 // count self
	for _, rm := range r.remotes {
		if rm.isActive() {
			activeCount++
		}
	}
	for _, wm := range r.witnesses {
		if wm.isActive() {
			activeCount++
		}
	}
	q := quorum(r.remotes, r.witnesses)
	slog.Debug("quorum status",
		"shard", r.shardID,
		"replica", r.replicaID,
		"term", r.term,
		"commit_index", r.log.committed,
		"last_index", r.log.lastIndex(),
		"active_replicas", activeCount,
		"quorum_size", q,
	)
}

// resetRemoteActivity clears the active flag on all remotes and witnesses.
func (r *raft) resetRemoteActivity() {
	for _, rm := range r.remotes {
		rm.setNotActive()
	}
	for _, wm := range r.witnesses {
		wm.setNotActive()
	}
}

// recordActivity notes client or protocol activity and leaves quiesce.
// Leaving quiesce invalidates the read lease: the acks that earned it were
// recorded against a clock paced by the liveness-probe interval, which
// says nothing about the followers' election timers once normal ticking
// resumes.
func (r *raft) recordActivity() {
	if r.quiesce.active {
		r.exitQuiesce()
		return
	}
	r.quiesce.recordActivity()
}

// exitQuiesce leaves quiesce mode and invalidates the read lease. Every
// quiesce exit path must go through here so no lease survives the clock
// change.
func (r *raft) exitQuiesce() {
	wasActive := r.quiesce.active
	r.quiesce.exit()
	if wasActive {
		r.resetLeaseAcks()
		r.electionTick = 0
	}
}

// resetLeaseAcks clears all remoteLastAck timestamps so the lease must
// be re-earned through fresh heartbeat acknowledgments. Called when the
// electionTick wraps to 0 during checkQuorum and on quiesce exit, so no
// ack from a previous cycle or clock regime contributes to hasValidLease().
func (r *raft) resetLeaseAcks() {
	if r.remoteLastAck == nil {
		return
	}
	for id := range r.remoteLastAck {
		delete(r.remoteLastAck, id)
	}
}

// votingBitCount returns how many ReadIndex ackBits positions this replica's
// CURRENT voting set occupies: every remote, every witness, and self, which is
// seated at bit 0 whether or not the configuration lists this replica.
// It is the exact quantity rebuildReplicaToBit is bounded by, factored out so
// that the places which can GROW that set check the same number the place which
// consumes it does.
func (r *raft) votingBitCount() int {
	return len(r.remotes) + len(r.witnesses) + 1
}

// rebuildReplicaToBit constructs the replicaToBit mapping from the current
// set of all voting members (remotes + witnesses + self). Both ReadIndex
// and LeaseRead use the unified quorum (remotes + witnesses + self) because
// witnesses vote in elections and the leadership confirmation quorum must
// overlap with every possible election-winning coalition. Including
// witnesses ensures their heartbeat acks count toward ReadIndex quorum.
//
// Called from becomeLeader on initial leadership and from
// syncRaftRemotes when membership changes during leadership.
//
// Returns ErrTooManyVotingMembers rather than asserting when the voting set
// does not fit the ackBits word. An assertion would panic the raft STEP WORKER
// — a goroutine shared by every shard that worker owns, and joined by
// Host.Close — over a fault that concerns exactly one shard. Worse, the
// condition is durable: a membership persisted over the cap would panic the
// node again on every restart, with no operator action that could clear it. The three places that can grow the voting set all check the
// bound before they mutate (newRaft, Peer.syncRaftRemotes, and handleSnapshot's
// gate), so reaching this return means one of them was bypassed; it is a
// backstop, and it fails the shard rather than the process. Truncating the map
// to fit is NOT an option: bit positions above 63 would alias onto live ones
// and two distinct replicas would ack into the same bit, satisfying a ReadIndex
// quorum that a majority never granted.
func (r *raft) rebuildReplicaToBit() error {
	votingCount := r.votingBitCount()
	if votingCount > maxVotingMembers {
		slog.Error("voting set exceeds the ReadIndex ackBits word: refusing to build the ack map",
			"shard", r.shardID,
			"replica", r.replicaID,
			"voting_members", votingCount,
			"max_voting_members", maxVotingMembers,
			"remotes", len(r.remotes),
			"witnesses", len(r.witnesses))
		return ErrTooManyVotingMembers
	}
	r.replicaToBit = make(map[uint64]uint8, votingCount)
	var bit uint8
	r.replicaToBit[r.replicaID] = bit
	bit++
	for id := range r.remotes {
		r.replicaToBit[id] = bit
		bit++
	}
	for id := range r.witnesses {
		r.replicaToBit[id] = bit
		bit++
	}

	// Re-base in-flight ReadIndex acks to self-only. Their ackBits were
	// recorded under the OLD bit mapping; after a rebuild (membership
	// change during leadership) a replica whose position moved could ack
	// again into a fresh bit and be counted twice, letting a sub-quorum
	// set of replicas satisfy the ReadIndex check — a stale-read hazard.
	// Dropping recorded acks is safe: the pending heartbeat round keeps
	// re-confirming and the entries simply take one more response each.
	// Self is always bit 0, matching the creation-time self-ack.
	selfMask := uint64(1) << r.replicaToBit[r.replicaID]
	for i := range r.pendingReadIndex {
		r.pendingReadIndex[i].ackBits = selfMask
		r.pendingReadIndex[i].ackCount = 1
	}
	// Arm a fresh confirmation heartbeat round: responses to the previous
	// round may have all arrived already (and were just discarded), and
	// regular heartbeats carry ctx=0 which never confirms ReadIndex.
	if len(r.pendingReadIndex) > 0 {
		r.readIndexBroadcastPending = true
	}
	return nil
}

// hardState returns the current hard state for persistence.
func (r *raft) hardState() proto.State {
	return proto.State{
		Term:       r.term,
		Vote:       r.vote,
		Commit:     r.log.committed,
		VoteWindow: r.voteWindowOwed,
	}
}
