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
	mathrand "math/rand"
	"slices"

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
const maxVotingMembers = 64

// maxInflightMsgs is the default maximum number of in-flight append
// messages per remote peer.
const maxInflightMsgs = 256

// maxPendingReadIndex caps the number of in-flight ReadIndex requests
// awaiting heartbeat quorum confirmation. This prevents unbounded memory
// growth when heartbeat responses are lost or delayed. When reached, new
// ReadIndex requests are rejected with ErrReadIndexOverloaded.
const maxPendingReadIndex = 4096

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

// maxConfigChangeScanSize is the maximum total byte size of entries loaded
// when scanning for uncommitted config change entries. This prevents an
// unbounded log read from causing OOM when the uncommitted tail is very
// large. 64 MiB is generous enough to cover any practical config change
// scan while bounding memory usage.
const maxConfigChangeScanSize uint64 = 64 * 1024 * 1024

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
	isObserver  bool
	isWitness   bool

	// Lease-based read fields (PhD Section 6.4). When leaseRead is
	// enabled, the leader can serve ReadIndex requests without a
	// heartbeat round-trip if it has received acks from a quorum of
	// data-bearing remotes within leaseTimeout ticks. The per-remote
	// tick at which the last heartbeat ack was received is tracked in
	// remoteLastAck. leaseEnabled is only true when the config flag
	// is set AND the node is leader.
	leaseRead     bool           // config: enable lease reads
	leaseTimeout  int            // lease duration in ticks (electionTimeout - 2)
	remoteLastAck map[uint64]int // replicaID -> electionTick at last ack

	// pendingConfigChange tracks whether a config change entry has been
	// appended but not yet committed. Per PhD thesis 4.1, only one
	// config change may be pending at a time to prevent safety issues.
	pendingConfigChange bool

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
	// Replicate message. Set from Config.MaxApplyEntrySize (default 64MB,
	// matching dragonboat). Zero means unlimited (not recommended).
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

	// logTruncated is set by handleAppend when tryAppend detects a
	// conflict and truncates log entries. This signals to Peer that
	// membership may need rebuilding from the committed base, since
	// truncated entries may have contained config changes that were
	// already applied to the membership. Per PhD thesis 4.1: "a log
	// entry for a configuration change can be removed (if leadership
	// changes); in this case, a server must be prepared to fall back
	// to the previous configuration in its log."
	logTruncated bool

	// Function pointers set on role transition for zero-branch dispatch.
	stepFunc func(r *raft, msg proto.Message) error
	tickFunc func(r *raft)
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

	// Initialize remotes from membership.
	r.initRemotes(membership)

	// Pre-allocate matched buffer and double-buffered msgs slices.
	r.matched = make([]uint64, 0, len(r.remotes)+1)
	msgCap := len(r.remotes) + len(r.observers) + len(r.witnesses)
	r.msgs = make([]proto.Message, 0, msgCap)
	r.msgsBuf = make([]proto.Message, 0, msgCap)

	// Restore persisted state.
	if state.Term > 0 {
		r.term = state.Term
		r.vote = state.Vote
	}
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
	r.term++
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
func (r *raft) becomeLeader() {
	invariant.Assert(r.state != follower, "follower cannot become leader directly")
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
	// heartbeat round after becoming leader.
	if r.leaseRead {
		r.remoteLastAck = make(map[uint64]int)
	} else {
		r.remoteLastAck = nil
	}

	// Reset all remote tracking. Set next to lastIndex+1 so the leader
	// probes each follower starting at its own last known position.
	// becomeProbe() resets inflights and state but we override next after.
	lastIdx := r.log.lastIndex()
	for _, rm := range r.remotes {
		rm.match = 0
		rm.becomeProbe()
		rm.next = lastIdx + 1
	}
	for _, rm := range r.observers {
		rm.match = 0
		rm.becomeProbe()
		rm.next = lastIdx + 1
	}
	for _, wm := range r.witnesses {
		wm.match = 0
		wm.becomeProbe()
		wm.next = lastIdx + 1
	}

	// Build replicaToBit mapping for zero-allocation ReadIndex ack tracking.
	r.rebuildReplicaToBit()

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
}

// --------------------------------------------------------------------------
// Message dispatch
// --------------------------------------------------------------------------

// step handles a Raft message. Common term-based logic is handled first,
// then delegation to the role-specific step function.
func (r *raft) step(msg proto.Message) error {
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
		} else if msg.Term < r.term {
			// Stale message from an older term.
			if msg.Type == proto.Heartbeat || msg.Type == proto.Replicate || msg.Type == proto.InstallSnapshot {
				// Send a response so the sender learns the new term.
				r.send(proto.Message{
					Type: proto.ReplicateResp,
					To:   msg.From,
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
	case proto.SnapshotReceived:
		return r.handleSnapshotReceived(msg)
	case proto.Propose:
		if r.leaderID == noLeader {
			return &NotLeaderError{LeaderID: 0}
		}
		// Client activity: exit quiesce so the election timer runs at
		// normal speed while proposals are in flight.
		r.quiesce.recordActivity()
		// Forward to leader.
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
		r.becomeFollower(msg.Term, msg.From)
		return r.handleAppend(msg)
	case proto.Heartbeat:
		r.becomeFollower(msg.Term, msg.From)
		return r.handleHeartbeat(msg)
	case proto.InstallSnapshot:
		r.becomeFollower(msg.Term, msg.From)
		return r.handleSnapshot(msg)
	case proto.SnapshotReceived:
		return r.handleSnapshotReceived(msg)
	case proto.Propose:
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
	case proto.SnapshotReceived:
		return r.handleSnapshotReceived(msg)
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
			r.quiesce.exit()
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
		// Check if quiesce should be entered.
		if r.quiesce.tryEnter() {
			return
		}
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
	if r.quiesce.active {
		// During quiesce, only send liveness probes.
		if r.quiesce.tickLiveness() {
			r.broadcastHeartbeat()
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
			// Reset lease ack timestamps so the lease must be re-earned
			// through fresh heartbeat acknowledgments. Without this,
			// stale ack ticks from the previous cycle would satisfy
			// hasValidLease() after the electionTick wraps to 0, because
			// leaseExpiry = 0 - leaseTimeout underflows negative and all
			// positive stale ticks appear valid.
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

			// Restore lease tracking invalidated by handleLeaderTransfer.
			// Without this, the leader silently degrades to heartbeat-based
			// ReadIndex for the rest of its term.
			if r.leaseRead && r.remoteLastAck == nil {
				r.remoteLastAck = make(map[uint64]int)
			}
		}
	}

	// Try to enter quiesce. Block quiesce entry when a leader transfer
	// is active, because quiesce freezes the electionTick counter and
	// transferElapsed would never increment, causing the transfer timer
	// to stall indefinitely. The leader would reject proposals with
	// ErrTransferInProgress until manually restarted.
	if r.transferTarget == noNode && r.quiesce.tryEnter() {
		// Invalidate the lease on quiesce entry. During quiesce,
		// electionTick is frozen, so hasValidLease would compute
		// leaseExpiry = frozenTick - leaseTimeout which can underflow
		// negative, making all remoteLastAck entries appear valid.
		// This prevents stale reads on a partitioned leader that exits
		// quiesce and serves a ReadIndex before earning a fresh lease.
		r.resetLeaseAcks()
		r.broadcastQuiesce()
	}
}

// --------------------------------------------------------------------------
// Election
// --------------------------------------------------------------------------

// hasConfigChangeToApply returns true if there are uncommitted config
// change entries between the committed index and the last log index.
// During this window, the cluster is transitioning between membership
// configurations, and starting an election could violate the overlap
// guarantee required for safe membership changes.
func (r *raft) hasConfigChangeToApply() bool {
	if r.log.committed >= r.log.lastIndex() {
		return false
	}
	// Scan entries from committed+1 to lastIndex for config changes.
	// Use a bounded maxSize to prevent loading an unbounded log tail
	// into memory. Config changes are rare and block further config
	// changes, so they appear near the start of the uncommitted range.
	entries, err := r.log.entries(r.log.committed+1, r.log.lastIndex()+1, maxConfigChangeScanSize)
	if err != nil {
		// If we cannot read entries (compacted), be conservative and
		// allow the election. The log is already behind.
		return false
	}
	for i := range entries {
		if entries[i].Type&proto.EntryTypeMask == proto.EntryConfigChange {
			return true
		}
	}
	return false
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
func (r *raft) startElection() {
	if r.isObserver || r.isWitness {
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
func (r *raft) campaign(msgType uint64) {
	term := r.term
	if msgType == proto.RequestPreVote {
		// For PreVote, use term+1 as the prospective term without
		// actually incrementing.
		term = r.term + 1
	}

	msg := proto.Message{
		Type:     msgType,
		Term:     term,
		LogTerm:  r.log.lastTerm(),
		LogIndex: r.log.lastIndex(),
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
	r.campaign(proto.RequestPreVote)
}

// broadcastVote sends RequestVote to all voting members.
func (r *raft) broadcastVote() {
	r.campaign(proto.RequestVote)
}

// handleVoteRequest processes a RequestVote or RequestPreVote message.
func (r *raft) handleVoteRequest(msg proto.Message) error {
	isPreVote := msg.Type == proto.RequestPreVote
	respType := proto.RequestVoteResp
	if isPreVote {
		respType = proto.RequestPreVoteResp
	}

	// Check if the candidate's log is at least as up-to-date.
	logOK := r.log.isUpToDate(msg.LogIndex, msg.LogTerm)

	// Check if we can grant the vote.
	var canVote bool
	if isPreVote {
		// For PreVote: grant if the candidate's log is up-to-date and
		// we haven't received a heartbeat from a valid leader recently.
		// The "leader lease" check uses ticks, not wall-clock time.
		leaderActive := r.leaderID != noLeader && r.electionTick < r.electionTimeout
		canVote = logOK && !leaderActive
	} else {
		// For real vote: grant if log is up-to-date and we haven't
		// voted for someone else in this term.
		canVote = logOK && (r.vote == noNode || r.vote == msg.From)
	}

	// PreVote responses must echo the requested term (msg.Term), not the
	// responder's current term. Without this, a pre-candidate at term T
	// receiving a PreVoteResp from a follower at term T-1 would see
	// msg.Term < r.term and drop it as stale in step(). This causes
	// election liveness failures after leader failover. For real votes,
	// Term=0 lets send() fill in r.term (which matches msg.Term since
	// becomeFollower updated it).
	respTerm := uint64(0)
	if isPreVote {
		respTerm = msg.Term
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
func (r *raft) handleVoteResp(msg proto.Message) error {
	slog.Debug("vote response",
		"shard", r.shardID,
		"from", msg.From,
		"granted", !msg.Reject,
		"term", r.term)
	r.votes[msg.From] = !msg.Reject
	r.checkElectionResult()
	return nil
}

// checkElectionResult tallies votes and transitions state if the
// election is decided. Returns true if a transition occurred.
func (r *raft) checkElectionResult() bool {
	result := r.tallyVotes()
	quorum := electionQuorum(r.remotes, r.witnesses)
	granted := 0
	for _, v := range r.votes {
		if v {
			granted++
		}
	}
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
			"votes_needed", quorum,
			"duration_ticks", electionDuration,
		)
		r.becomeLeader()
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
			"votes_needed", quorum,
			"duration_ticks", electionDuration,
		)
		r.becomeFollower(r.term, noLeader)
		return true
	default:
		return false
	}
}

// tallyVotes counts votes and returns the result.
func (r *raft) tallyVotes() voteResult {
	quorum := electionQuorum(r.remotes, r.witnesses)
	granted := 0
	rejected := 0
	totalVoters := numVotingMembers(r.remotes, r.witnesses)

	for _, v := range r.votes {
		if v {
			granted++
		} else {
			rejected++
		}
	}

	if granted >= quorum {
		return voteWon
	}
	if rejected > totalVoters-quorum {
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

// handleLeaderPropose handles a proposal on the leader.
func (r *raft) handleLeaderPropose(msg proto.Message) error {
	if r.transferTarget != noNode {
		return ErrTransferInProgress
	}

	r.quiesce.recordActivity()

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
		if entries[i].IsConfigChange() {
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
// the snapshot file data is stripped (Filepath cleared, FileSize zeroed,
// Witness flag set) since witnesses only need the metadata (Index, Term,
// Membership) for election participation.
func (r *raft) sendSnapshot(to uint64) {
	rm := r.getRemote(to)
	if rm == nil {
		return
	}

	ss := r.log.snapshot()
	if ss == nil {
		// No snapshot available; nothing we can do.
		return
	}

	rm.becomeSnapshot(ss.Index)

	snap := *ss
	// Strip snapshot file data for witnesses. They only need the
	// metadata (Index, Term, Membership) for election participation.
	if _, isWitness := r.witnesses[to]; isWitness {
		snap.Filepath = ""
		snap.FileSize = 0
		snap.Witness = true
	}

	r.send(proto.Message{
		Type:     proto.InstallSnapshot,
		To:       to,
		Snapshot: snap,
	})
}

// handleAppend processes an AppendEntries (Replicate) message.
func (r *raft) handleAppend(msg proto.Message) error {
	r.electionTick = 0
	r.leaderID = msg.From
	r.quiesce.recordActivity()

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

	lastIdx, ok, truncated, err := r.log.tryAppend(msg.LogIndex, msg.LogTerm, msg.Entries)
	if err != nil {
		// Conflict detection encountered a compacted or unavailable
		// entry. Drop the message; the leader will retransmit or
		// send a snapshot. This is safe because the leader tracks
		// match/next per follower and will retry.
		return err
	}
	if ok {
		// Signal to Peer that log entries were overwritten due to
		// conflict resolution. Peer must rebuild membership from
		// the committed base to ensure truncated config change
		// entries are rolled back (PhD thesis 4.1).
		if truncated {
			r.logTruncated = true
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
		rm.next = hintIndex
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
			// Commit index advanced. Broadcast the new commit index
			// to all followers so they can advance their own commit
			// and apply committed entries. Without this broadcast,
			// followers whose MsgAppResp hasn't arrived yet would
			// only learn about the commit via heartbeats, which
			// carry min(match, committed) — useless if match is 0.
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
	r.matched = append(r.matched, r.log.lastIndex()) // leader's own match
	for _, rm := range r.remotes {
		r.matched = append(r.matched, rm.match)
	}
	slices.Sort(r.matched)
	quorum := commitQuorum(r.remotes)
	mci := r.matched[len(r.matched)-quorum]
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
// dragonboat's broadcastHeartbeatMessage() behavior (raft.go:849-856).
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
// correlate heartbeat responses with specific ReadIndex requests.
func (r *raft) broadcastHeartbeatCtx(ctx uint64) {
	for id := range r.remotes {
		r.sendHeartbeatTo(id, ctx)
	}
	for id := range r.observers {
		r.sendHeartbeatTo(id, ctx)
	}
	for id := range r.witnesses {
		r.sendHeartbeatTo(id, ctx)
	}
}

// sendHeartbeatTo sends a heartbeat message with context to the given peer.
func (r *raft) sendHeartbeatTo(to uint64, ctx uint64) {
	commit := min(r.log.committed, r.getMatchForPeer(to))
	r.send(proto.Message{
		Type:   proto.Heartbeat,
		To:     to,
		Commit: commit,
		Hint:   ctx,
	})
}

// handleHeartbeat processes a heartbeat message on a follower.
func (r *raft) handleHeartbeat(msg proto.Message) error {
	r.electionTick = 0
	r.leaderID = msg.From

	if msg.Commit > r.log.committed {
		commitTo := min(msg.Commit, r.log.lastIndex())
		if commitTo > r.log.committed {
			r.log.commitTo(commitTo)
		}
	}

	// Echo back the context so the leader can correlate ReadIndex responses.
	r.send(proto.Message{
		Type: proto.HeartbeatResp,
		To:   msg.From,
		Hint: msg.Hint,
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
	// lease and ReadIndex quorums use electionQuorum (remotes + witnesses
	// + self). All voting members must be tracked so their heartbeat
	// acks count toward the leadership confirmation quorum.
	if r.remoteLastAck != nil {
		if _, isRemote := r.remotes[msg.From]; isRemote {
			r.remoteLastAck[msg.From] = r.electionTick
		} else if _, isWitness := r.witnesses[msg.From]; isWitness {
			r.remoteLastAck[msg.From] = r.electionTick
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
	// sendAppend internally checks isPaused() and skips if appropriate.
	if rm.state == remoteProbe {
		rm.probeSent = false
	} else if rm.state == remoteReplicate &&
		rm.next > r.log.lastIndex() &&
		rm.inflights.Count() > 0 {
		rm.becomeProbe()
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
	if msg.Hint == 0 || rm.state != remoteReplicate {
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

// handleSnapshot processes an InstallSnapshot message on a follower.
// Per Raft Figure 13: if existing log entry has same index and term as
// snapshot's last included entry, retain log entries following it.
// Remotes are rebuilt from the snapshot membership only when the log
// is fully replaced (non-matching case). When entries are retained,
// they may contain config changes newer than the snapshot's membership,
// so rebuilding would lose those changes.
func (r *raft) handleSnapshot(msg proto.Message) error {
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
	if t, err := r.log.term(ss.Index); err == nil && t == ss.Term {
		// Log matches at snapshot point; just advance commit.
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
}

// handleSnapshotReceived processes a SnapshotReceived message, which is
// delivered after the receiver successfully persists a snapshot to disk
// and LogDB. This rebuilds the remote tracking maps from the snapshot's
// membership so the raft layer has an accurate view of cluster peers.
// All roles handle this message because any node may receive a snapshot
// (e.g., a follower joining mid-cluster, or a candidate that fell behind).
func (r *raft) handleSnapshotReceived(msg proto.Message) error {
	if !msg.Snapshot.IsEmpty() {
		r.rebuildRemotesFromMembership(msg.Snapshot.Membership)
	}
	return nil
}

// handleSnapshotStatus processes a snapshot status report on the leader.
func (r *raft) handleSnapshotStatus(msg proto.Message) error {
	rm := r.getRemote(msg.From)
	if rm == nil {
		return nil
	}
	if rm.state != remoteSnapshot {
		return nil
	}
	if msg.Reject {
		rm.snapshotFailed()
	} else {
		rm.snapshotDone()
	}
	return nil
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
// A lease is valid when a quorum of all voting members (remotes + witnesses
// + self) have acknowledged heartbeats within leaseTimeout ticks. Since
// witnesses vote in elections, the lease quorum must overlap with every
// possible election-winning coalition to prevent stale reads after
// leadership change. The safety argument (PhD 6.4): followers will not
// grant votes to another candidate until their election timeout expires,
// and leaseTimeout = electionTimeout - 2 provides a 2-tick safety margin
// that accounts for goroutine scheduling jitter between the leader's
// lease check and followers' election timeout, preventing split-brain reads.
func (r *raft) hasValidLease() bool {
	if !r.leaseRead || r.state != leader || r.remoteLastAck == nil {
		return false
	}
	quorum := electionQuorum(r.remotes, r.witnesses)
	count := 1 // count self
	leaseExpiry := r.electionTick - r.leaseTimeout
	for _, tick := range r.remoteLastAck {
		if tick >= leaseExpiry {
			count++
		}
	}
	return count >= quorum
}

// handleLeaderReadIndex handles a read index request on the leader.
func (r *raft) handleLeaderReadIndex(msg proto.Message) error {
	r.quiesce.recordActivity()

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
	if numVotingMembers(r.remotes, r.witnesses) == 1 || r.hasValidLease() {
		slog.Debug("read index fast path",
			"shard", r.shardID,
			"replica", r.replicaID,
			"key", msg.Hint,
			"commit_index", r.log.committed,
			"lease_valid", r.hasValidLease(),
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
	if r.leaderID == noLeader {
		return &NotLeaderError{LeaderID: 0}
	}
	msg.To = r.leaderID
	r.send(msg)
	return nil
}

// handleReadIndexResp handles a read index response forwarded from leader.
func (r *raft) handleReadIndexResp(msg proto.Message) error {
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
	// Use electionQuorum (remotes + witnesses + leader) for linearizability.
	quorum := electionQuorum(r.remotes, r.witnesses)

	// Compact resolved entries by overwriting with unresolved ones.
	// Self-ack is pre-set at creation time in handleLeaderReadIndex,
	// so no self-bit check is needed here.
	n := 0
	resolvedCount := 0
	for i := range r.pendingReadIndex {
		pending := &r.pendingReadIndex[i]
		if pending.ackCount < quorum {
			r.pendingReadIndex[n] = r.pendingReadIndex[i]
			n++
			continue
		}
		resolvedCount++
		slog.Debug("read index confirmed",
			"shard", r.shardID,
			"replica", r.replicaID,
			"key", pending.key,
			"index", pending.index,
			"ctx", pending.ctx,
			"ack_count", pending.ackCount,
			"quorum", quorum,
		)
		if pending.from != 0 && pending.from != r.replicaID {
			// Forwarded from a follower; send response back.
			r.send(proto.Message{
				Type:     proto.ReadIndexResp,
				To:       pending.from,
				Hint:     pending.key,
				LogIndex: pending.index,
			})
		} else {
			// Local request; add to read states. The cap check is
			// best-effort here: we don't fail the quorum check if
			// readStates is full, because the pending entry has already
			// been resolved. Dropping the result is better than blocking
			// the entire pendingReadIndex compaction loop.
			_ = r.addReadState(proto.ReadyToRead{
				Key:   pending.key,
				Index: pending.index,
			})
		}
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
	r.quiesce.exit()

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
	r.send(proto.Message{
		Type: proto.TimeoutNow,
		To:   to,
	})
}

// handleTimeoutNow processes a TimeoutNow message by immediately
// starting an election.
func (r *raft) handleTimeoutNow(_ proto.Message) error {
	if r.isObserver || r.isWitness {
		return nil
	}
	// A leader must never call becomeCandidate (invariant assertion).
	// This can happen if a same-term TimeoutNow arrives at the leader
	// (e.g., stale message from a previous transfer round).
	if r.state == leader {
		return nil
	}
	// Exit quiesce before starting the election. Without this, a
	// quiesced follower that wins the election would use the quiesce
	// liveness probe interval instead of normal heartbeats, and if
	// the election fails, tickFollower would use the extended quiesce
	// election timeout (30x normal).
	r.quiesce.exit()
	// Start an election immediately, bypassing PreVote.
	r.becomeCandidate()
	r.broadcastVote()
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
func (r *raft) handleQuiesceMessage(msg proto.Message) {
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
	return activeCount >= electionQuorum(r.remotes, r.witnesses)
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
	quorum := electionQuorum(r.remotes, r.witnesses)
	slog.Debug("quorum status",
		"shard", r.shardID,
		"replica", r.replicaID,
		"term", r.term,
		"commit_index", r.log.committed,
		"last_index", r.log.lastIndex(),
		"active_replicas", activeCount,
		"quorum_size", quorum,
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

// resetLeaseAcks clears all remoteLastAck timestamps so the lease must
// be re-earned through fresh heartbeat acknowledgments. Called after the
// electionTick wraps to 0 during checkQuorum to prevent stale ack values
// from the previous cycle from satisfying hasValidLease().
func (r *raft) resetLeaseAcks() {
	if r.remoteLastAck == nil {
		return
	}
	for id := range r.remoteLastAck {
		delete(r.remoteLastAck, id)
	}
}

// rebuildReplicaToBit constructs the replicaToBit mapping from the current
// set of all voting members (remotes + witnesses + self). Both ReadIndex
// and LeaseRead use electionQuorum (remotes + witnesses + self) because
// witnesses vote in elections and the leadership confirmation quorum must
// overlap with every possible election-winning coalition. Including
// witnesses ensures their heartbeat acks count toward ReadIndex quorum.
//
// Called from becomeLeader on initial leadership and from
// syncRaftRemotes when membership changes during leadership.
func (r *raft) rebuildReplicaToBit() {
	votingCount := len(r.remotes) + len(r.witnesses) + 1
	invariant.Assert(votingCount <= maxVotingMembers,
		"cluster exceeds 64 voting members: ReadIndex ackBits is uint64, bit positions would alias and break quorum checks")
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
}

// hardState returns the current hard state for persistence.
func (r *raft) hardState() proto.State {
	return proto.State{
		Term:   r.term,
		Vote:   r.vote,
		Commit: r.log.committed,
	}
}
