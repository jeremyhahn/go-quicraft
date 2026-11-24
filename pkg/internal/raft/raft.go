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

// maxAdoptableTerm (proto.MaxTerm) is the exclusive bound on a wire term this replica adopts, so becomeCandidate's increment never wraps.
const maxAdoptableTerm = proto.MaxTerm

// persistedCeiling is the exclusive bound on a term or index this replica opens with, above the admission bound so locally produced state always reopens.
const persistedCeiling = math.MaxUint64 - 8

// MaxVotingMembers is the most voting members a shard supports (the uint64 ackBits width); enforce it at admission only, never on apply.
const MaxVotingMembers = 64

// maxVotingMembers is the unexported spelling used throughout this package.
const maxVotingMembers = MaxVotingMembers

// maxInflightMsgs is the default maximum number of in-flight append
// messages per remote peer.
const maxInflightMsgs = 256

// maxPendingReadIndex caps in-flight ReadIndex requests; beyond it they get ErrReadIndexOverloaded.
const maxPendingReadIndex = 4096

// leaseDriftDivisor shortens the read lease by 1% for clock-rate drift between leader and followers; Config.MaxClockDrift overrides it per shard.
const leaseDriftDivisor = 100

// partsPerMillion is the scale of leaseDriftPPM.
const partsPerMillion = 1_000_000

// leaseDriftPPMFor converts Config.MaxClockDrift to parts per million; zero selects 1/leaseDriftDivisor.
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

// leaseRoundWindow is how many recent heartbeat rounds' send times the leader keeps (a power of two); older acks are not counted.
const (
	leaseRoundWindow = 32
	leaseRoundMask   = leaseRoundWindow - 1
	leaseRoundSlices = leaseRoundWindow / 2
)

// maxReadStates caps resolved, unconsumed ReadIndex results; beyond it new requests get ErrReadIndexOverloaded.
const maxReadStates = 4096

// noLeader indicates no known leader.
const noLeader uint64 = 0

// noNode indicates no specific node.
const noNode uint64 = 0

// localControlMessages are the types allowed past step()'s Term != 0 gate, and only when stamped proto.OriginLocal.
var localControlMessages = map[uint64]struct{}{
	proto.Propose:         {},
	proto.ReadIndex:       {},
	proto.InstallSnapshot: {},
	proto.SnapshotStatus:  {},
	proto.Unreachable:     {},
	proto.LeaderTransfer:  {},
}

// processLocalMessages are local control types never legitimately received from a peer: InstallSnapshot, SnapshotStatus, Unreachable, LeaderTransfer.
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

// isLocallyInjectedControlMessage reports whether msg is a local control type with proto.OriginLocal, exempt from Term != 0.
func isLocallyInjectedControlMessage(msg proto.Message) bool {
	return msg.Origin.IsLocal() && isLocalControlMessage(msg.Type)
}

// isProcessLocalMessage reports whether a message type may only ever be
// injected in-process, never received from a peer.
func isProcessLocalMessage(msgType uint64) bool {
	_, ok := processLocalMessages[msgType]
	return ok
}

// readIndexStatus tracks a pending read index request; heartbeat acks are a uint64 bitfield indexed by raft.replicaToBit.
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

// raft is the core Raft state machine: messages in, messages out, no I/O; owned by one goroutine.
type raft struct {
	shardID   uint64
	replicaID uint64
	term      uint64
	vote      uint64
	log       *raftLog
	state     roleState
	leaderID  uint64

	// wire is the wire-version window an UpdateWireVersion proposal must
	// fall inside; zero means proto.DefaultWireWindow. Set by
	// Peer.SetWireWindow.
	wire proto.WireWindow

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

	// snapshotSeq numbers snapshot transfer attempts; stamped in msg.Hint and echoed in SnapshotStatus.
	snapshotSeq uint64

	// maxWitnesses and requireFullReplicaMajority mirror the replicated witness policy for the leader's propose-time check, never on apply.
	maxWitnesses               uint64
	requireFullReplicaMajority bool

	// Messages to send. Double-buffered: msgs accumulates outbound
	// messages, msgsBuf holds the alternate buffer for swap in
	// drainMessages to avoid allocation.
	msgs    []proto.Message
	msgsBuf []proto.Message

	// freeMsgBuf is the reusable buffer drainFreeOrderMessages returns.
	freeMsgBuf []proto.Message

	// orderedHeld is the prefix of msgs a free-order drain already classified as ordered; reset when msgs is handed out whole.
	orderedHeld int

	// readIndexCtx starts at 1; ctx 0 is reserved for regular heartbeats.
	readStates       []proto.ReadyToRead
	pendingReadIndex []readIndexStatus
	readIndexCtx     uint64
	// readRejects holds the keys of reads this replica forwarded that the
	// leader refused (a ReadIndexResp with Reject set). Drained by
	// Peer.DrainReadRejects; the engine fails each with ErrReadIndexNotReady.
	readRejects []uint64

	// readIndexBroadcastPending marks a deferred heartbeat broadcast shared by this step's ReadIndex requests; flushReadIndex sends it.
	readIndexBroadcastPending bool

	// replicaToBit maps voting replica IDs to readIndexStatus bit positions 0..63; built in becomeLeader.
	replicaToBit map[uint64]uint8

	// Pre-allocated sort buffer for commit calculation.
	matched []uint64

	// Feature flags.
	checkQuorum bool
	preVote     bool

	// isObserver is this replica's effective non-voting role: config.IsObserver until the replicated membership names it, then the membership (checkSelfRole).
	isObserver bool
	// isWitness is the locally configured witness role; a disagreement with membership is latched as roleMismatch.
	isWitness bool

	// selfRemoved records that this replica is absent from the current configuration; such a replica never campaigns (thesis 4.2.2).
	selfRemoved bool

	// roleMismatch latches a local witness role that contradicts the replicated membership: no campaigning and no reads until they agree.
	roleMismatch bool

	// joining mirrors Peer.joining: a joining replica may not campaign but keeps its election clock; cleared when a snapshot delivers the membership.
	joining bool

	// Lease-read fields (thesis 6.4): the lease holds while a voting quorum acked a heartbeat round sent less than leaseDuration ago on the monotonic clock.
	leaseRead      bool                 // config: enable lease reads
	leaseTimeout   int                  // lease length in ticks (electionTimeout - 2)
	leaseDuration  time.Duration        // leaseTimeout ticks in real time less the drift bound; 0 = no lease
	leaseSpan      time.Duration        // leaseTimeout ticks in real time: the vote window an ack must promise
	now            func() time.Time     // monotonic clock; time.Now outside tests
	leaseRound     uint64               // newest heartbeat round issued; 0 = none
	leaseRoundSent []time.Time          // ring of recent round send times, indexed by round & leaseRoundMask
	remoteLastAck  map[uint64]time.Time // replicaID -> send time of the newest round it acked
	// leaseRoundFloor is the newest round invalidateLease retired: an ack for
	// it or any older round renews nothing. 0 retires only the "no round" 0.
	leaseRoundFloor uint64

	// Vote hold-down: after a restart the replica neither votes nor campaigns until voteHoldDuration has elapsed since voteHoldStart.
	voteHoldArmed    bool          // a hold-down applies: LeaseRead, or a window is owed
	voteHoldStart    time.Time     // construction time: the window's anchor
	voteHoldDuration time.Duration // the hold-down's length; 0 = not yet known
	voteHoldOver     bool          // latched once the window has elapsed
	electionSpan     time.Duration // electionTimeout ticks in real time; 0 = tick interval unknown

	// The live vote promise: until promiseUntil no vote, pre-vote or campaign; only the leader promisedTo in promisedTerm may release it by transfer.
	promiseUntil time.Time
	promisedTo   uint64
	promisedTerm uint64

	// timeoutNowSent marks that this leader sent a TimeoutNow this term and takes no read lease until becomeLeader clears it.
	timeoutNowSent bool

	// voteWindowOwed is the hard state's VoteWindow: the longest vote window possibly promised and not yet outlived, in nanoseconds.
	voteWindowOwed uint64

	// pendingConfigChange tracks whether a config change entry has been
	// appended but not yet committed. Per PhD thesis 4.1, only one
	// config change may be pending at a time to prevent safety issues.
	pendingConfigChange bool

	// unreadableLogVoteDenials counts votes denied because this replica could not read its own last log term.
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

	// maxReplicationPayload bounds entry bytes per outbound Replicate (Config.MaxApplyEntrySize, default config.MaxReplicationPayloadBytes); zero means unlimited.
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

	// resolveStaleConfig folds the recovered committed configuration tail into membership and reports whether it is current; nil on a bare raft means no gate.
	resolveStaleConfig func() bool

	// validateConfigChange is the full membership validator, run at the leader before a forwarded config change enters the log; nil on a bare raft, where validateConfigChangePolicy applies.
	validateConfigChange func(cc proto.ConfigChange) error

	// takeBoundConfigChange reports and forgets whether a config change's proposal key was bound to its ConfigChangeID; nil means none are bound.
	takeBoundConfigChange func(key uint64) bool

	// Function pointers set on role transition for zero-branch dispatch.
	stepFunc func(r *raft, msg proto.Message) error
	tickFunc func(r *raft)

	// leaseDriftPPM is the lease's clock-drift bound in parts per million.
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

	// Refuse persisted state at the arithmetic ceiling: the shard fails to start.
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
	// Refuse a recovered configuration too large for the ackBits word, failing only this shard.
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

// checkSelfRole reconciles this replica's role with the replicated membership: the observer role follows it (setObserver), the witness role is latched (roleMismatch).
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

// setObserver applies the role the membership records, on change only: a promotion takes effect in place and sizes the vote hold-down; a demotion steps down.
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

// setSelfMembership records whether this replica is in the current configuration; an empty configuration counts as not a member.
func (r *raft) setSelfMembership(present bool) {
	if r.selfRemoved == !present {
		return
	}
	r.selfRemoved = !present
	if r.selfRemoved {
		// Becoming selfRemoved is terminal until a config change re-adds this replica, so it is logged above debug level.
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

// recordUnreadableLogVoteDenial counts and reports a vote denied because this replica could not read its own last log term.
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

// becomeFollower moves to follower; the vote is cleared only when the term advances (Figure 2).
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
	// Never let the term increment wrap.
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

// becomeLeader moves to leader and appends a no-op; it returns ErrTooManyVotingMembers, before changing anything, when the voting set does not fit ackBits.
func (r *raft) becomeLeader() error {
	invariant.Assert(r.state != follower, "follower cannot become leader directly")
	// Decline leadership when the log is at the admission bound and has no room for the no-op.
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

	// Reset every remote: next = lastIndex+1 and the activity flag cleared so CheckQuorum starts fresh.
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

	// Rebuild replicaToBit for this term; the guard above makes this infallible.
	r.replicaToBit = nil
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

// dropHighTermVote reports whether a higher-term RequestVote is dropped under CheckQuorum; transfer-hinted votes are exempt only at exactly r.term+1.
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
	// Refuse process-local message types that arrived from the wire, at any term; the origin stamp cannot be marshaled.
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

	// Term 0 is only for locally injected control messages; a wire message of a send()-produced type with Term 0 is refused.
	if msg.Term == 0 && !isLocallyInjectedControlMessage(msg) {
		slog.Warn("dropped protocol message with no term",
			"shard", r.shardID,
			"replica", r.replicaID,
			"from", msg.From,
			"type", proto.MessageTypeName(msg.Type),
			"term", r.term)
		return ErrZeroTermMessage
	}

	// Refuse a wire term in the reserved band below the uint64 ceiling (see maxAdoptableTerm).
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

	// CheckQuorum guard (thesis p.42): drop a higher-term RequestVote while a live leader was heard within the election timeout; transfer votes are exempt.
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
			// Stale message; forwarded Propose and ReadIndex are exempt and fall through to the role handler.
			if msg.Type == proto.Heartbeat || msg.Type == proto.Replicate || msg.Type == proto.InstallSnapshot {
				// Reply with a rejection carrying our term and committed index so the stale sender learns the newer term.
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
		// Checked before the no-leader refusal so the binding is consumed
		// either way, and before forwarding, which would drop it.
		if r.takeBoundConfigChanges(msg.Entries) {
			return ErrBoundConfigChangeNotLeader
		}
		if r.leaderID == noLeader {
			return &NotLeaderError{LeaderID: 0}
		}
		// Client activity: exit quiesce so the election timer runs at
		// normal speed while proposals are in flight.
		r.recordActivity()
		// Forward to the leader with a copy of the entries: msg.Entries aliases the step worker's reusable proposal buffer.
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

// stepDownToFollower abandons a candidacy for a known leader without ever regressing the term (local messages carry Term 0).
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
		if msg.Type == proto.Propose && r.takeBoundConfigChanges(msg.Entries) {
			return ErrBoundConfigChangeNotLeader
		}
		// A candidate knows no leader, so it refuses Propose and ReadIndex explicitly rather than dropping them.
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
		err := r.handleLeaderReadIndex(msg)
		if err != nil && isForwardedRead(msg, r.replicaID) {
			// The follower that forwarded this read has no other way to
			// learn of the refusal: answer it, or its caller waits for a
			// response that never comes.
			r.rejectForwardedRead(msg, err)
			return nil
		}
		return err
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
		// An expired follower campaigns and never self-quiesces; quiesce is leader-driven.
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
	// Snapshot ack/retry countdowns advance even while quiesced.
	r.tickSnapshotWaits()

	if r.quiesce.active {
		// During quiesce, only send liveness probes.
		if r.quiesce.tickLiveness() {
			r.broadcastHeartbeat()
		}
		// CheckQuorum keeps running while quiesced, paced by the quiesce liveness interval.
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
			// Reset lease acks so the lease is re-earned every checkQuorum cycle.
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

	// Do not quiesce during a leader transfer, which needs electionTick to advance.
	if r.transferTarget == noNode && r.quiesce.tryEnter() {
		// On quiesce entry invalidate the lease and restart the quiesced CheckQuorum cycle.
		r.resetLeaseAcks()
		r.electionTick = 0
		r.broadcastQuiesce()
	}
}

// --------------------------------------------------------------------------
// Election
// --------------------------------------------------------------------------

// hasConfigChangeToApply reports whether a config change above processed (raftLog.confChangeIndex) may still be unapplied; O(1).
func (r *raft) hasConfigChangeToApply() bool {
	return r.log.confChangeIndex > r.log.processed
}

// configCurrentForCampaign folds the recovered committed configuration tail and reports whether this replica may campaign.
func (r *raft) configCurrentForCampaign() bool {
	if r.resolveStaleConfig == nil {
		return true
	}
	return r.resolveStaleConfig()
}

// startElection starts an election (PreVote if enabled); never blocked on uncommitted log content, and never run by a removed replica.
func (r *raft) startElection() {
	if r.isWitness || r.selfRemoved {
		// Forget the leader: the election timeout expired without a heartbeat.
		r.leaderID = noLeader
		return
	}
	// Fold the committed configuration tail before the role gates below, from this replica's own log; a read failure only skips this timeout.
	if !r.configCurrentForCampaign() {
		// Forget the leader so this replica does not deny votes while not campaigning.
		r.leaderID = noLeader
		return
	}
	// Re-check self-removal against the folded configuration.
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
	// Witness role mismatch: do not campaign (see checkSelfRole).
	if r.roleMismatch {
		slog.Warn("election skipped: local role contradicts the replicated membership",
			"shard", r.shardID,
			"replica", r.replicaID,
			"configured_witness", r.isWitness,
			"term", r.term)
		// Forget the leader so this voter does not stall the election.
		r.leaderID = noLeader
		return
	}
	// A joining replica does not campaign but forgets the leader, so it keeps granting votes to real members.
	if r.joining {
		slog.Debug("election skipped: still joining with incomplete membership",
			"shard", r.shardID,
			"replica", r.replicaID,
			"term", r.term,
			"last_leader", r.leaderID)
		r.leaderID = noLeader
		return
	}
	// No self-vote inside the post-restart vote hold-down (voteHeldDown).
	if r.voteHeldDown() || r.votePromised() {
		slog.Debug("election skipped: a leader may be counting this replica's ack toward a read lease",
			"shard", r.shardID,
			"replica", r.replicaID,
			"term", r.term,
			"last_leader", r.leaderID)
		r.leaderID = noLeader
		// Retry on the next tick, since the window ends in real time.
		r.electionTick = max(r.randomizedElectionTimeout-1, 0)
		return
	}
	// Refuse to start an election while our own last log term is unreadable, before any term increment.
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

	// Skip campaigning while our own last term is unreadable; retry on the next timeout.
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
	// Hint == From marks a leadership-transfer election, exempt from the CheckQuorum vote guard.
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

	// Check the candidate's log is up to date; report denials caused by our own unreadable last term.
	logOK, logReadable := r.log.isUpToDate(msg.LogIndex, msg.LogTerm)
	if !logReadable {
		r.recordUnreadableLogVoteDenial(msg.From)
	}

	// Never vote for a replica outside our known configuration, unless we are joining or know no configuration.
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

	// Never vote for a candidate the configuration lists as an observer.
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
		// PreVote: grant for a term strictly above ours, an up-to-date log and no recently heard leader.
		leaderActive := r.leaderID != noLeader && r.electionTick < r.electionTimeout
		canVote = logOK && !leaderActive && msg.Term > r.term
	} else {
		// Real vote: up-to-date log and either a repeat of our vote or no vote and no known leader this term; transfer votes are exempt from the leader clause.
		noCompetingLeader := r.leaderID == noLeader || msg.Hint == msg.From
		canVote = logOK && (r.vote == msg.From ||
			(r.vote == noNode && noCompetingLeader))
	}

	// Inside the post-restart hold-down withhold every vote and pre-vote (voteHeldDown).
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

	// PreVote response term: the prospective term on a grant, our own term on a rejection; real votes use r.term.
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

// handleVoteResp processes vote and pre-vote responses, discarding those from replicas outside the current configuration.
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

// isVotingMember reports whether id is in the current voting set; self only while not removed.
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

// purgeDepartedVotes drops vote records of replicas that left the voting set.
func (r *raft) purgeDepartedVotes() {
	for id := range r.votes {
		if !r.isVotingMember(id) {
			delete(r.votes, id)
		}
	}
}

// countVotes tallies grants and rejections over the current voting set.
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
			// Won but cannot lead: decline cleanly to follower at this term with no leader.
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

// tallyVotes counts votes and computes the quorum over the same current voting set.
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

// proposableEntryTypes are the entry types a Propose may carry: EntryNormal, EntryConfigChange, EntryEncodedEntry.
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
	// Refuse a batch that would take the log past proto.MaxLogIndex.
	if last := r.log.lastIndex(); last >= proto.MaxLogIndex || uint64(len(msg.Entries)) > proto.MaxLogIndex-last {
		return proto.ErrLogIndexOutOfRange
	}

	// Reject the whole batch if any entry has a reserved type, before any state change.
	for i := range msg.Entries {
		if _, ok := proposableEntryTypes[msg.Entries[i].Type&proto.EntryTypeMask]; !ok {
			return ErrReservedEntryType
		}
	}

	r.recordActivity()

	// Re-derive pendingConfigChange from confChangeIndex > processed, unconditionally.
	r.pendingConfigChange = r.hasConfigChangeToApply()

	// Validate the whole batch before mutating state: at most one pending config change (thesis 4.1).
	foundCC := false
	for i := range msg.Entries {
		if msg.Entries[i].Type&proto.EntryTypeMask == proto.EntryConfigChange {
			if r.pendingConfigChange || foundCC {
				return ErrPendingConfigChange
			}
			// Leader-side membership validation, the last gate before the change enters the log.
			if err := r.validateProposedConfigChange(msg.Entries[i].Key, msg.Entries[i].Cmd); err != nil {
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

// validateProposedConfigChange decodes one EntryConfigChange payload and validates it; an undecodable payload is rejected.
func (r *raft) validateProposedConfigChange(key uint64, cmd []byte) error {
	var cc proto.ConfigChange
	if _, err := cc.UnmarshalFrom(cmd); err != nil {
		slog.Warn("rejected config change with an undecodable payload at propose time",
			"shard", r.shardID,
			"replica", r.replicaID,
			"error", err,
		)
		return ErrConfigChangeUnmarshal
	}
	if r.takeBoundConfigChange != nil && key != 0 && r.takeBoundConfigChange(key) {
		cc.MustMatchConfigChangeID = true
	}
	if r.validateConfigChange != nil {
		return r.validateConfigChange(cc)
	}
	return r.validateConfigChangePolicy(cc)
}

// ErrBoundConfigChangeNotLeader refuses a bound config change proposed on a non-leader; it matches ErrStaleConfigChange, so the caller re-reads membership and retries.
var ErrBoundConfigChangeNotLeader = fmt.Errorf("%w: bound config change proposed on a replica that is not the leader", ErrStaleConfigChange)

// takeBoundConfigChanges consumes the binding of every bound config change in entries and reports whether any was bound.
func (r *raft) takeBoundConfigChanges(entries []proto.Entry) bool {
	if r.takeBoundConfigChange == nil {
		return false
	}
	found := false
	for i := range entries {
		if entries[i].Type&proto.EntryTypeMask == proto.EntryConfigChange &&
			entries[i].Key != 0 && r.takeBoundConfigChange(entries[i].Key) {
			found = true
		}
	}
	return found
}

// validateConfigChangePolicy enforces WitnessConfig policy against the leader's peer tracking on a bare raft; structural rules live in membership.validate.
func (r *raft) validateConfigChangePolicy(cc proto.ConfigChange) error {
	// Enforce the voting-member cap for every net-new voter; replay never re-checks.
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
	case proto.UpdateWireVersion:
		return r.validateWireVersionProposal(cc)
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

// validateWireVersionProposal refuses an UpdateWireVersion outside this replica's window.
func (r *raft) validateWireVersionProposal(cc proto.ConfigChange) error {
	w := r.wire
	if w == (proto.WireWindow{}) {
		w = proto.DefaultWireWindow()
	}
	if !w.Accepts(cc.WireVersion) {
		return proto.ErrUnsupportedVersion
	}
	return nil
}

// broadcastAppend replicates to remotes, observers and witnesses; witnesses get metadata-only entries, and lagging observers catch up by snapshot through sendAppend.
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

// sendAppend sends Replicate messages to a peer, pipelining batches in Replicate state while the inflight window allows; one probe in Probe state.
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

		// An empty probe is valid in Probe state and drives backtracking; in Replicate state with no entries nothing is sent.
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

// sendWitnessAppend sends metadata-only Replicate messages to a witness, config changes intact and Commit included, pipelined like sendAppend.
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

// makeMetadataEntries strips entries to EntryMetadata (Index and Term) for witnesses, keeping config change entries intact.
func makeMetadataEntries(entries []proto.Entry) []proto.Entry {
	if len(entries) == 0 {
		return nil
	}
	meta := make([]proto.Entry, 0, len(entries))
	for i := range entries {
		if entries[i].IsConfigChange() || entries[i].IsInitialMembership() {
			// Membership changes and the bootstrap entry keep their Cmd for witnesses.
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

// sendSnapshot sends a snapshot to a peer; for a witness the Filepath is empty, which makes it a metadata-only transfer.
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
		// Fall back to probe so next is re-seated at a readable index and the peer stays reachable.
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

	// Hint carries the transfer sequence, echoed in SnapshotStatus; it never crosses the wire.
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

	// Entries at or below our committed index are stale; answer with the committed index so committed entries are never overwritten.
	if msg.LogIndex < r.log.committed {
		r.send(proto.Message{
			Type:     proto.ReplicateResp,
			To:       msg.From,
			LogIndex: r.log.committed,
		})
		return nil
	}

	// Drop a batch whose last index would exceed proto.MaxLogIndex.
	if n := uint64(len(msg.Entries)); n > 0 && (msg.LogIndex >= proto.MaxLogIndex || n > proto.MaxLogIndex-msg.LogIndex) {
		return proto.ErrLogIndexOutOfRange
	}
	// Refuse the whole batch if any entry term is zero or above msg.Term.
	for i := range msg.Entries {
		if term := msg.Entries[i].Term; term == 0 || term > msg.Term {
			return proto.ErrTermOutOfRange
		}
	}

	lastIdx, ok, truncated, err := r.log.tryAppend(msg.LogIndex, msg.LogTerm, msg.Entries)
	if err != nil {
		// Compacted or unavailable entry during conflict detection: drop; the leader retries.
		return err
	}
	if ok {
		// Truncation never removes an applied config change: membership changes only on commit.
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
		// Reject with a backtracking hint (section 5.3): our last index if the log is short, else the conflicting term and its first index.
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

// declineRemote handles a rejected append with term-based fast backtracking (section 5.3), or index-based when hintTerm is 0; it reports whether next changed.
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
		// The leader has no entries at the conflicting term: back up to the follower's hint, clamped to the rejected index.
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

	// Ignore a match index beyond the leader's own log.
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
			// Commit advanced: broadcast so followers apply; caught-up followers learn it from the next heartbeat or append.
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
	// The leader's own match is its durable index (inmem.savedTo), not lastIndex().
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

// broadcastHeartbeat sends heartbeats to every peer, carrying the latest ReadIndex context when reads are pending.
func (r *raft) broadcastHeartbeat() {
	if len(r.pendingReadIndex) > 0 {
		r.broadcastHeartbeatCtx(r.readIndexCtx)
		r.readIndexBroadcastPending = false
	} else {
		r.broadcastHeartbeatCtx(0)
	}
}

// broadcastHeartbeatCtx sends heartbeats carrying a ReadIndex context and the lease round.
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

// handleHeartbeat processes a heartbeat on a follower: only the recognised leader is accepted, and commit advances only via tryCommit at the current term.
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

	// Echo the context and lease round with the vote window promised from now (promiseVotes).
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

	// Record the ack for lease and ReadIndex quorums, dated by the round's send time and counted only if it backs the lease (ackBacksLease).
	if r.remoteLastAck != nil && r.ackBacksLease(msg) {
		_, isRemote := r.remotes[msg.From]
		_, isWitness := r.witnesses[msg.From]
		if sent, ok := r.leaseRoundSentAt(msg.LogIndex); ok && (isRemote || isWitness) {
			if prev, seen := r.remoteLastAck[msg.From]; !seen || sent.After(prev) {
				r.remoteLastAck[msg.From] = sent
			}
		}
	}

	// The peer is reachable: unblock a probe, fall back to probe on a stalled window, or release one slot of a full window (freeFirstOne); forceSend marks a released slot.
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

	// Skip sendAppend for a ReadIndex heartbeat to a caught-up follower; witnesses use sendWitnessAppend.
	if msg.Hint == 0 || rm.state != remoteReplicate || forceSend {
		if _, isWitness := r.witnesses[msg.From]; isWitness {
			r.sendWitnessAppend(msg.From)
		} else {
			r.sendAppend(msg.From)
		}
	}

	// A response ctx confirms every pending read with ctx <= it; a ctx above readIndexCtx was never issued and is refused.
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
				// Entries are in ctx order; stop at the first one above the response ctx.
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

// snapshotVotingBitCount returns how many ackBits positions membership would take on this replica, as rebuildReplicaToBit would count them.
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

// handleSnapshot installs a snapshot on a follower (Figure 13): only from the recognised leader, only if its voting set fits ackBits, rebuilding remotes only when the log is replaced.
func (r *raft) handleSnapshot(msg proto.Message) error {
	// Validate the peer-chosen snapshot position before it becomes the log's base.
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

	// Figure 13: keep the entries after a matching snapshot point, unless a snapshot is still pending persistence.
	if t, err := r.log.term(ss.Index); err == nil && t == ss.Term &&
		r.log.pendingSnapshot() == nil {
		// Log matches: advance committed and processed to the snapshot; keep remotes, since retained entries may hold newer config changes.
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
		// Log replaced: rebuild remotes from the snapshot membership.
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
	// Re-derive self membership before purgeDepartedVotes.
	r.setSelfMembership(membershipHasSelf(m, r.replicaID))
	_, listedWitness := m.Witnesses[r.replicaID]
	_, listedObserver := m.Observers[r.replicaID]
	r.checkSelfRole(membershipHasSelf(m, r.replicaID), listedWitness, listedObserver)
	r.purgeDepartedVotes()
}

// snapshotAckWaitMultiplier sizes the post-transfer ack wait in election timeouts; a premature expiry only re-streams the snapshot.
const snapshotAckWaitMultiplier = 3

// handleSnapshotStatus handles a transfer report bound to its attempt by msg.Hint; stale reports are discarded, and both outcomes arm the snapshotDone countdown.
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

// tickSnapshotWaits advances every peer's snapshot countdown; an expired peer falls back to probe. Runs before the quiesce early-return.
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

// hasCommittedEntryAtCurrentTerm reports whether the leader committed an entry in its term, required before ReadIndex (thesis 6.4).
func (r *raft) hasCommittedEntryAtCurrentTerm() bool {
	t, err := r.log.term(r.log.committed)
	if err != nil {
		return false
	}
	return t == r.term
}

// hasValidLease reports whether a voting quorum acked a heartbeat round sent less than leaseDuration ago on the monotonic clock (thesis 6.4).
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

// setTickInterval derives leaseDuration (drift-shortened) and the vote hold-down (not shortened) from the tick duration.
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

// ticksToDuration returns n ticks as a duration, and false unless it is positive and does not overflow.
func ticksToDuration(n int, tick time.Duration) (time.Duration, bool) {
	if n <= 0 || tick <= 0 || int64(n) > math.MaxInt64/int64(tick) {
		return 0, false
	}
	return time.Duration(n) * tick, true
}

// armVoteHoldDown starts the post-restart vote hold-down at construction when LeaseRead is on or a window is owed.
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

// sizeVoteHoldDown sets the hold-down to the longer of the owed and promised windows and raises the owed window, persisted before any ack.
func (r *raft) sizeVoteHoldDown() {
	promise := r.voteWindow()
	if promise > r.voteWindowOwed {
		r.voteWindowOwed = promise
	}
	if r.voteHoldArmed && !r.voteHoldOver {
		r.voteHoldDuration = time.Duration(min(r.voteWindowOwed, uint64(math.MaxInt64)))
	}
}

// promiseVotes takes on the vote promise a heartbeat ack is about to make to leader (votePromised); no window means no clock read.
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

// votePromised reports whether a heartbeat-ack vote promise is still outstanding, kept in real time because tick timers and leader tests can end early.
func (r *raft) votePromised() bool {
	return !r.promiseUntil.IsZero() && r.now().Before(r.promiseUntil)
}

// transferReleasesPromise reports whether a vote request is a transfer-hinted request for the term after the promise's, which only the promisee could have started.
func (r *raft) transferReleasesPromise(msg proto.Message) bool {
	return msg.Hint == msg.From && r.promisedTerm != 0 && msg.Term == r.promisedTerm+1
}

// voteWindow is the vote window a heartbeat ack promises (electionTimeout ticks of real time); 0 without LeaseRead, on an observer, or before the tick interval is known.
func (r *raft) voteWindow() uint64 {
	if !r.leaseRead || r.isObserver || r.electionSpan <= 0 {
		return 0
	}
	return uint64(r.electionSpan)
}

// ackBacksLease reports whether an ack's promised window (in LogTerm) covers this leader's lease span; an ack with no window backs no lease.
func (r *raft) ackBacksLease(msg proto.Message) bool {
	return r.leaseSpan > 0 && msg.LogTerm <= math.MaxInt64 &&
		time.Duration(msg.LogTerm) >= r.leaseSpan
}

// voteHeldDown reports whether a recently restarted replica must still withhold every vote, its own included, because a leader may count a pre-restart ack toward a lease; latched once elapsed.
func (r *raft) voteHeldDown() bool {
	if !r.voteHoldArmed || r.voteHoldOver {
		return false
	}
	if r.voteHoldDuration > 0 && r.now().Sub(r.voteHoldStart) >= r.voteHoldDuration {
		r.voteHoldOver = true
		// Earlier incarnations' promises are outlived: lower the owed record to this incarnation's.
		r.voteWindowOwed = r.voteWindow()
		return false
	}
	return true
}

// nextLeaseRound returns the round the next broadcast carries, or 0 when lease tracking is off; a round is reused until older than leaseDuration/leaseRoundSlices.
func (r *raft) nextLeaseRound() uint64 {
	if r.remoteLastAck == nil || r.leaseDuration <= 0 {
		return 0
	}
	now := r.now()
	if r.leaseRound > r.leaseRoundFloor &&
		now.Sub(r.leaseRoundSent[r.leaseRound&leaseRoundMask]) < r.leaseDuration/leaseRoundSlices {
		return r.leaseRound
	}
	r.leaseRound++
	r.leaseRoundSent[r.leaseRound&leaseRoundMask] = now
	return r.leaseRound
}

// leaseRoundSentAt returns when round was first sent; false for round 0, an unissued round, or one outside the window.
func (r *raft) leaseRoundSentAt(round uint64) (time.Time, bool) {
	if round <= r.leaseRoundFloor || round > r.leaseRound || r.leaseRound-round >= leaseRoundWindow {
		return time.Time{}, false
	}
	return r.leaseRoundSent[round&leaseRoundMask], true
}

// handleLeaderReadIndex handles a read index request on the leader.
func (r *raft) handleLeaderReadIndex(msg proto.Message) error {
	r.recordActivity()

	// Refuse reads on a witness role mismatch (see checkSelfRole).
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

	// Single node or valid lease: answer at once; the lease is evaluated once.
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
		if isForwardedRead(msg, r.replicaID) {
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

	// Defer the broadcast so this step's ReadIndex requests share one heartbeat round.
	r.readIndexBroadcastPending = true
	return nil
}

// flushReadIndex sends one heartbeat broadcast for the ReadIndex requests deferred this step; called by Peer.FlushReadIndex.
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
	// Refuse linearizable reads on a witness role mismatch (see checkSelfRole).
	if r.roleMismatch {
		return ErrWitnessRoleMismatch
	}
	if r.leaderID == noLeader {
		return &NotLeaderError{LeaderID: 0}
	}
	// Refuse a local read until an entry of the current term has committed; relayed reads are still forwarded.
	if !isForwardedRead(msg, r.replicaID) && !r.hasCommittedEntryAtCurrentTerm() {
		return ErrReadIndexNotReady
	}
	msg.To = r.leaderID
	r.send(msg)
	return nil
}

// isForwardedRead reports whether a ReadIndex was relayed from another
// replica rather than submitted on this one.
func isForwardedRead(msg proto.Message, self uint64) bool {
	return msg.From != 0 && msg.From != self
}

// rejectForwardedRead answers a refused forwarded ReadIndex with Reject set and LogIndex math.MaxUint64; the caller sees ErrReadIndexNotReady.
func (r *raft) rejectForwardedRead(msg proto.Message, err error) {
	slog.Debug("forwarded read index refused",
		"shard", r.shardID,
		"replica", r.replicaID,
		"from", msg.From,
		"key", msg.Hint,
		"error", err,
	)
	r.send(proto.Message{
		Type:     proto.ReadIndexResp,
		To:       msg.From,
		Hint:     msg.Hint,
		LogIndex: math.MaxUint64,
		Reject:   true,
	})
}

// handleReadIndexResp resolves a forwarded read, accepting the response only from r.leaderID; LogIndex may exceed our committed index.
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
	// A reject is a refusal, never a read state: Reject is tested before
	// LogIndex is looked at, so no reject is served at any index.
	if msg.Reject {
		if len(r.readRejects) >= maxReadStates {
			return ErrReadIndexOverloaded
		}
		r.readRejects = append(r.readRejects, msg.Hint)
		return nil
	}
	return r.addReadState(proto.ReadyToRead{
		Key:   msg.Hint,
		Index: msg.LogIndex,
	})
}

// checkReadIndexQuorum resolves pending read index requests that reached quorum, in any order.
func (r *raft) checkReadIndexQuorum() {
	if len(r.pendingReadIndex) == 0 {
		return
	}
	// Confirm leadership over the unified voting quorum (remotes, witnesses, self).
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
			// readStates is full: keep the resolved request pending for the next pass rather than drop it.
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

	// Exit quiesce so the transfer timeout runs.
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

	// Expire the lease before transferring leadership (thesis 6.4.1).
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

// resumeLeaseAfterTransfer restores lease tracking after an abandoned transfer, unless a TimeoutNow was sent this term.
func (r *raft) resumeLeaseAfterTransfer() {
	if r.leaseRead && r.remoteLastAck == nil && !r.timeoutNowSent {
		r.remoteLastAck = make(map[uint64]time.Time)
	}
}

// handleTimeoutNow processes a TimeoutNow message by immediately
// starting an election.
func (r *raft) handleTimeoutNow(msg proto.Message) error {
	// A removed or role-mismatched replica ignores TimeoutNow.
	if r.isWitness || r.selfRemoved {
		return nil
	}
	// Fold the committed configuration tail before the immediate campaign (see startElection).
	if !r.configCurrentForCampaign() {
		return ErrStaleConfigCannotCampaign
	}
	// Re-check removal and observer role after the fold.
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
	// A joining replica ignores TimeoutNow.
	if r.joining {
		slog.Warn("ignoring TimeoutNow: still joining with incomplete membership",
			"shard", r.shardID,
			"replica", r.replicaID,
			"term", r.term)
		return ErrJoiningNodeCannotCampaign
	}
	// Refuse during the vote hold-down or a live promise not released by its promisee.
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
	// Refuse while our own last log term is unreadable, before any term increment.
	if _, ok := r.log.lastTerm(); !ok {
		slog.Warn("ignoring TimeoutNow: local last log term unreadable",
			"shard", r.shardID,
			"replica", r.replicaID,
			"term", r.term,
			"last_index", r.log.lastIndex())
		return ErrUnreadableLogCannotCampaign
	}
	// Exit quiesce before the election.
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

// handleQuiesceMessage processes a leader's quiesce notification, only when quiesce is enabled locally.
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
	r.orderedHeld = 0
	return msgs
}

// withholdOrderedMessages compacts free-order messages to the front of an already-drained batch and requeues the ordered ones; it uses the caller's slice, not freeMsgBuf.
func (r *raft) withholdOrderedMessages(msgs []proto.Message) []proto.Message {
	n := 0
	for i := range msgs {
		if proto.IsFreeOrder(&msgs[i]) {
			msgs[n] = msgs[i]
			n++
		} else {
			r.msgs = append(r.msgs, msgs[i])
		}
	}
	r.orderedHeld = len(r.msgs)
	return msgs[:n]
}

// requeueMessages puts drained messages back at the front of the outbound buffer, preserving order.
func (r *raft) requeueMessages(msgs []proto.Message) {
	if len(msgs) == 0 {
		return
	}
	// The requeued messages were handed out as ordered once already; they
	// keep that classification ahead of whatever is queued now.
	r.orderedHeld = min(r.orderedHeld, len(r.msgs)) + len(msgs)
	if len(r.msgs) == 0 {
		r.msgs = append(r.msgs, msgs...)
		return
	}
	combined := make([]proto.Message, 0, len(msgs)+len(r.msgs))
	combined = append(combined, msgs...)
	combined = append(combined, r.msgs...)
	r.msgs = combined
}

// drainFreeOrderMessages extracts free-order messages, plus HeartbeatResp when heartbeatRespFree and not held, leaving ordered messages for the next GetUpdate.
func (r *raft) drainFreeOrderMessages(heartbeatRespFree bool) []proto.Message {
	if len(r.msgs) == 0 {
		return nil
	}
	held := min(r.orderedHeld, len(r.msgs))
	r.freeMsgBuf = r.freeMsgBuf[:0]
	n := 0
	for i := range r.msgs {
		if proto.IsFreeOrder(&r.msgs[i]) ||
			(heartbeatRespFree && i >= held && r.msgs[i].Type == proto.HeartbeatResp) {
			r.freeMsgBuf = append(r.freeMsgBuf, r.msgs[i])
		} else {
			r.msgs[n] = r.msgs[i]
			n++
		}
	}
	r.msgs = r.msgs[:n]
	// Everything left is ordered and stays ordered: a HeartbeatResp kept
	// back here was produced by a step that changed the hard state.
	r.orderedHeld = n
	if len(r.freeMsgBuf) == 0 {
		return nil
	}
	return r.freeMsgBuf
}

// membershipKnown reports whether this replica knows any configuration; false only for a joiner without the bootstrap entry.
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

// recordActivity notes activity, leaves quiesce and invalidates the lease.
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

// resetLeaseAcks clears remoteLastAck so the lease is re-earned with fresh acks.
func (r *raft) resetLeaseAcks() {
	if r.remoteLastAck == nil {
		return
	}
	for id := range r.remoteLastAck {
		delete(r.remoteLastAck, id)
	}
}

// invalidateLease ends the lease and retires every issued round (leaseRoundFloor).
func (r *raft) invalidateLease() {
	r.resetLeaseAcks()
	r.leaseRoundFloor = r.leaseRound
}

// votingBitCount returns the ackBits positions the current voting set occupies: remotes, witnesses and self.
func (r *raft) votingBitCount() int {
	return len(r.remotes) + len(r.witnesses) + 1
}

// rebuildReplicaToBit maps every voting member (remotes, witnesses, self) to an ackBits position; it returns ErrTooManyVotingMembers rather than panicking or truncating.
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
	// A voter added during this tenure gets one CheckQuorum window of grace, as in etcd/raft.
	prev := r.replicaToBit
	r.replicaToBit = make(map[uint64]uint8, votingCount)
	var bit uint8
	r.replicaToBit[r.replicaID] = bit
	bit++
	for id, rm := range r.remotes {
		r.replicaToBit[id] = bit
		bit++
		if _, known := prev[id]; prev != nil && !known {
			rm.setActive()
		}
	}
	for id, wm := range r.witnesses {
		r.replicaToBit[id] = bit
		bit++
		if _, known := prev[id]; prev != nil && !known {
			wm.setActive()
		}
	}

	// Re-base in-flight ReadIndex acks to self only (bit 0), since bit positions moved.
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
