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

package config

import (
	"log/slog"
	"math"
	"path/filepath"

	"github.com/jeremyhahn/go-quicraft/pkg/proto"
)

// Default values for Config fields.
const (
	DefaultElectionRTT            uint64 = 10
	DefaultHeartbeatRTT           uint64 = 1
	DefaultCompactionOverhead     uint64 = 5000
	DefaultMaxInMemLogSize        uint64 = 0               // 0 = disabled
	DefaultMaxEntrySize           uint64 = 8 * 1024 * 1024 // 8 MB
	DefaultMaxApplyEntrySize      uint64 = MaxReplicationPayloadBytes
	DefaultMaxSnapshotSize        uint64 = 4 * 1024 * 1024 * 1024
	DefaultMaxResponsesPerSession uint64 = 128
	DefaultMaxTotalSessions       uint64 = 16384

	// DefaultMaxSessionsPerOwner is the number of concurrent sessions one
	// declared owner may hold on a shard that does not choose otherwise.
	//
	// It is set rather than left at zero (unlimited) because the whole point of
	// the cap is that MaxTotalSessions cannot defend itself: an owner able to
	// register 16384 sessions is an owner able to deny at-most-once delivery to
	// every other tenant on the shard. A default of 128 is two orders of
	// magnitude below the shard-wide cap, so a shard needs at least ~128
	// distinct owners before the total is even reachable.
	//
	// It costs existing embedders nothing. The cap applies only to sessions
	// that declare an owner (proto.Entry.OwnerID), and an embedder that does
	// not use Host.NewSessionWithOwner declares none, so every session it
	// registers is exempt no matter what this value is.
	DefaultMaxSessionsPerOwner uint64 = 128

	// SessionsPerOwnerUnlimited turns the per-owner session cap off. Assign it
	// to Config.MaxSessionsPerOwner; leaving that field zero selects
	// DefaultMaxSessionsPerOwner instead, for the reason SessionExpiryDisabled
	// gives — an unset field cannot mean both "I did not choose" and "no limit".
	//
	// It needs no special case anywhere: it is a cap no shard can reach, since
	// MaxTotalSessions bounds the whole table far below it, so the per-owner
	// check is satisfied by every registration the shard-wide check already
	// admitted. Choosing it means accepting that one principal can consume the
	// entire session table — see MaxSessionsPerOwner.
	SessionsPerOwnerUnlimited uint64 = math.MaxUint64

	// DefaultSessionExpiryEntries is the session inactivity threshold, in
	// applied log entries, that a shard bootstraps with unless the operator
	// chooses otherwise.
	//
	// It is non-zero because zero — the previous default — leaks: a client
	// that registers a session and dies without CloseSession leaves an entry
	// that is never removed, is serialized into every subsequent snapshot, and
	// counts against MaxTotalSessions forever, until the shard refuses new
	// registrations outright. Nothing else reclaims it.
	//
	// The value is 1000x the floor Validate enforces (100), which is set where
	// it is because a threshold that short expires sessions faster than a slow
	// client can round-trip. Expiry is measured in LOG ENTRIES, not time, so
	// the wall-clock window it buys scales inversely with shard throughput —
	// generous on a quiet shard, and on a shard sustaining 10k entries/s still
	// ten seconds of client silence before a session is reclaimed. Erring
	// upward is the right direction: an over-long threshold costs a bounded
	// amount of session table (MaxTotalSessions caps it regardless), while an
	// over-short one expires live clients, and a client whose session is
	// expired mid-retry loses its at-most-once guarantee for that retry.
	//
	// It could only become a default once the value was REPLICATED. As
	// node-local configuration, changing it would have made a mixed-version
	// cluster — old binary on one replica, new on another — expire different
	// sessions at different log indexes and diverge silently. It is now seeded
	// into the shard's membership at bootstrap, so this default applies only
	// to shards bootstrapped by this binary, and a replica of an older shard
	// must be configured with the value that shard committed or it is refused
	// with ErrSessionPolicyMismatch.
	DefaultSessionExpiryEntries uint64 = 100000

	// SessionExpiryDisabled turns automatic session expiry off. Assign it to
	// Config.SessionExpiryEntries; leaving that field zero now selects
	// DefaultSessionExpiryEntries instead, because an unset field cannot mean
	// both "I did not choose" and "never expire anything".
	//
	// It needs no special case anywhere: it is an inactivity threshold no log
	// can reach, so the apply path's checkpoint walk finds no checkpoint and
	// no session is ever old enough to expire. Choosing it means accepting
	// that a client which dies without CloseSession leaks a session entry
	// permanently — see SessionExpiryEntries.
	SessionExpiryDisabled uint64 = math.MaxUint64

	// DefaultSnapshotEntries is the number of applied entries between automatic
	// snapshots when Config.SnapshotEntries is left unset.
	//
	// There IS a default, and there has to be, because the alternative default
	// is "never compact the log" — and an unbounded log is not a configuration
	// a practical Raft implementation can run in (Ongaro, "Consensus: Bridging
	// Theory and Practice", ch. 5, treats compaction as a requirement rather
	// than a feature). Disk grows without bound and restart replay grows with
	// it, linearly, forever.
	//
	// In THIS implementation the cost is sharper than disk, because three
	// things advance only inside saveSnapshot: the snapshot-directory GC, the
	// epoch purge floor, and the at-rest barrier itself. A shard that never
	// snapshots therefore never reclaims a snapshot directory, never zeroes an
	// old epoch's DEK, and never asks its barrier to encrypt anything.
	//
	// The value is the only one that satisfies every constraint already in this
	// file: it must exceed DefaultCompactionOverhead (5000), or Validate
	// rejects the pair; and it must be at least DefaultSessionExpiryEntries
	// (100000), or a wholly default configuration trips validateSessions'
	// "sessions may never expire between snapshots" warning on every start. It
	// also matches etcd's DefaultSnapshotCount, so it is a figure with
	// production mileage rather than an invention.
	DefaultSnapshotEntries uint64 = 100000

	// SnapshotsDisabled turns automatic snapshotting off. Assign it to
	// Config.SnapshotEntries; leaving that field zero now selects
	// DefaultSnapshotEntries instead, for the same reason zero no longer
	// disables session expiry — an unset field cannot mean both "I did not
	// choose" and "never do this".
	//
	// Choosing it means taking responsibility for calling Host.RequestSnapshot,
	// and accepting the three consequences named above until you do. It needs
	// no special case: the apply path's trigger is a >= comparison against a
	// threshold no applied index can reach.
	SnapshotsDisabled uint64 = math.MaxUint64
)

// Bounds Validate places on a non-zero Config.MaxClockDrift. Below the floor
// the lease would be shortened by less than a part per million, which no
// clock is good for; above the ceiling half the lease is spent on drift, and
// clocks that bad are better served by the heartbeat-based ReadIndex.
const (
	MaxClockDriftMin float64 = 1e-6
	MaxClockDriftMax float64 = 0.5
)

// MaxReplicationPayloadBytes bounds MaxApplyEntrySize, which raft uses as the
// size limit when chunking a replication batch. A batch larger than the
// transport can frame is rejected at marshal time and DROPPED — and because
// the follower's next index never advances, raft rebuilds the identical
// oversized batch forever, so a lagging follower can never catch up and there
// is no snapshot fallback (the entries still exist).
//
// The value is transport.MaxFrameSize minus frame and batch framing overhead.
// It is duplicated here rather than imported because pkg/internal/transport
// imports this package; transport asserts at compile time that the two agree.
const MaxReplicationPayloadBytes uint64 = 16711672

// BoolPtr returns a pointer to the given bool value. This is the standard
// Go pattern for setting *bool config fields where nil means "use default".
// Exported for use by contrib packages (e.g., dragonboat adapter).
func BoolPtr(v bool) *bool { return &v }

// WitnessConfig tunes witness-replica policy for a shard. Witnesses are voting
// members that store metadata-only log entries (Index/Term, no command
// payload): they add election fault-tolerance without a full replica's storage
// cost. Witnesses count toward BOTH the election and commit quorums (a Raft
// safety requirement — a split quorum could let a committed entry be
// truncated), so an individual commit may rest on a {leader + witnesses}
// quorum with its payload on the leader alone: committed data survives any
// minority of crash-restart failures, but permanent storage loss of every
// full replica holding an entry makes it unrecoverable. These knobs bound
// membership shape to limit that exposure; they do NOT constrain which nodes
// form any individual commit quorum. The full safety and durability analysis
// lives in docs/raft-consensus/engineering.md Section 6.
//
// Lifecycle: WitnessConfig is the BOOTSTRAP SEED for the shard's
// replicated witness policy. When a shard is created, the policy is
// written into the replicated membership state (proto.Membership) and
// from then on is carried by the log, snapshots, and WAL like the member
// maps themselves — every replica enforces the identical policy
// regardless of local configuration, and it survives restarts from the
// replicated state, not from this struct. Change it at runtime via
// Host.RequestUpdateWitnessPolicy / SyncRequestUpdateWitnessPolicy,
// which commits an UpdateWitnessPolicy config change through Raft.
// Enforcement is validate-time only (Host API pre-validation plus the
// leader's propose gate), never on the deterministic apply path.
type WitnessConfig struct {
	// MaxWitnesses caps the number of witnesses in the shard. Zero means no
	// explicit cap (witnesses are still bounded by the 64 voting-member limit);
	// negative values are rejected by Config.Validate. A witness-add config
	// change that would exceed the cap is rejected at validation time with
	// ErrTooManyWitnesses.
	MaxWitnesses int

	// RequireFullReplicaMajority, when enabled, rejects any config change —
	// witness add or full-replica removal — that would leave the full
	// (data-bearing) replicas unable to form a quorum over the voting set.
	// This bounds how witness-dependent commits can become and is the default
	// (nil => true). For example, adding a third witness to {3 full +
	// 2 witnesses} is rejected (full 3 < quorum 4), as is removing a full
	// replica from that membership; {2 full + 1 witness} is allowed (full 2 >=
	// quorum 2). Note this is a membership-shape bound only: even in a
	// compliant membership a commit can rest on {leader + witnesses} when the
	// other full replicas lag (see the durability trade-off above). Set to
	// BoolPtr(false) to allow witness-heavy memberships that knowingly accept
	// full witness dependence for commit.
	RequireFullReplicaMajority *bool
}

// GetRequireFullReplicaMajority returns the effective RequireFullReplicaMajority
// policy, defaulting to true (the safe posture) when unset.
func (w WitnessConfig) GetRequireFullReplicaMajority() bool {
	if w.RequireFullReplicaMajority == nil {
		return true
	}
	return *w.RequireFullReplicaMajority
}

// Config is per-shard Raft configuration. It controls election timing,
// log compaction, entry size limits, session limits, and compression.
// ShardID and ReplicaID are set internally by Host.StartShard() and
// should not be set by users directly.
type Config struct {
	// ShardID is the globally unique identifier for the Raft shard.
	// Set internally by Host.StartShard().
	ShardID uint64

	// ReplicaID is the unique identifier for this replica within the shard.
	// Set internally by Host.StartShard().
	ReplicaID uint64

	// IsObserver marks this replica as a non-voting learner that receives
	// replicated data but does not participate in elections or quorum.
	// Mutually exclusive with IsWitness.
	//
	// The role lives in REPLICATED membership, not here: this bit is only the
	// INITIAL hint, consulted while the shard's configuration does not yet
	// mention this replica. Once it does, the committed membership decides,
	// in either direction. An observer whose promotion (Host.RequestAddNode
	// for its replica ID) commits becomes a voter in place — it votes,
	// campaigns and serves linearizable reads with no restart — and a replica
	// started without this bit that the shard added as an observer behaves as
	// one. Restarting a promoted replica with the bit still set is harmless.
	//
	// A replica with this bit set must be added through a committed
	// AddObserver config change, so Host.StartShard refuses to bootstrap it
	// through the initial members map (which seeds the full voting set). Start
	// it with join=true and add it with Host.RequestAddObserver.
	IsObserver bool

	// IsWitness marks this replica as a voting member that participates
	// in elections and quorum but does not maintain a state machine.
	// Mutually exclusive with IsObserver.
	//
	// Unlike IsObserver, this bit is binding: a witness keeps no state
	// machine, so the replicated membership must agree with it. A replica
	// whose configured witness role contradicts the membership refuses to
	// campaign or serve reads until the two agree, and a witness can never be
	// promoted to a voter. Host.StartShard refuses to bootstrap a witness
	// through the initial members map; start it with join=true and add it with
	// Host.RequestAddWitness.
	IsWitness bool

	// Witness seeds the shard's replicated witness policy at bootstrap.
	// The zero value is a safe default (RequireFullReplicaMajority
	// enabled, no explicit witness cap). After bootstrap the policy lives
	// in replicated membership state; see WitnessConfig.
	Witness WitnessConfig

	// MemberIdentities maps replica ID to the stable NODE IDENTITY that node
	// reports through Host.NodeIdentity — the SHA-256 of its certificate's
	// SubjectPublicKeyInfo. It has two roles, decided by StartShard's join
	// argument:
	//
	//   - FOUNDING (join=false): it seeds the shard's replicated identities. It
	//     is the bootstrap counterpart of the identity argument on
	//     Host.RequestAddNodeWithIdentity, and the same lifecycle applies: this
	//     is the ONLY point where it enters the shard, and from here on it
	//     lives in replicated membership state (log, snapshot, WAL) and changes
	//     only through committed config changes. Every founding member must be
	//     given an IDENTICAL map: the bootstrap entry at log index 1 is built
	//     from it, byte-identically on each founder, and founders that disagree
	//     produce a divergent index-1 entry that Log Matching never reconciles
	//     — the same rule that already applies to the members map and to
	//     Witness.
	//
	//   - ANCHOR (every start, and the whole of a join=true start): it is the
	//     OPERATOR ANCHOR — replicas this host vouches for, each bound to the
	//     identity the connection must present, consulted by the receive gate
	//     for a sender neither the committed configuration nor a pending
	//     config change in this replica's own log names. A joiner holds no
	//     configuration at all, so the anchor is the only thing that can say
	//     who may hand it one: before it existed, any host holding a valid
	//     cluster certificate could bootstrap a joining shard. An established
	//     replica that was down while a member was added and elected holds no
	//     entry for that leader either; restarting it with a map that names
	//     the leader is how its operator vouches for it, without a wipe. Name
	//     the current leader at least, or every member if leadership may move;
	//     an entry naming a replica that never sends costs nothing, and an
	//     entry for a replica outside a founder's members map is anchor only —
	//     it is not written into the bootstrap entry. A removed identity is
	//     refused by the tombstone before the anchor is consulted.
	//
	// Its effect on an established shard is on the RECEIVE-SIDE SENDER GATE. A
	// member is authorized by its identity — a value derived from its keypair,
	// which no other holder of a valid cluster certificate can present — and by
	// nothing else; its address is routing metadata. There is no address
	// fallback: a member the configuration holds no identity for has its
	// quorum-bearing and leader-originated traffic dropped.
	//
	// It is therefore REQUIRED on every host that has a transport
	// (RequireMemberIdentities, which the Host sets from that fact): a founder
	// must name every member of the members map, a joiner must name at least
	// one anchor replica, no identity may be bound to two replicas, and every
	// later add must carry an identity. It cannot
	// be populated automatically: an identity has to be collected from each
	// node out of band, because learning it from the peer's own connection
	// would let a promiscuous-certificate holder register itself as any replica
	// — the very attack identities close.
	MemberIdentities map[uint64]string

	// RequireMemberIdentities makes every path that ADMITS a member into this
	// shard's configuration refuse one that carries no node identity:
	// StartShard's founding map and anchor, and every AddNode,
	// AddObserver and AddWitness the leader is asked to append — including a
	// config change forwarded from another replica, which never passed the
	// proposing host's API. It is validate-only: a committed entry is never
	// refused on the apply path, or replicas would diverge.
	//
	// The Host SETS it from whether it has a transport, overriding whatever
	// the caller put here: the requirement is a property of the wire boundary
	// — the receive-side gate authorizes a member by identity and by nothing
	// else — and a host with no transport (WithoutTransport) has no wire
	// boundary for it to matter at. A bare raft, or a test driving one, may
	// set it explicitly.
	RequireMemberIdentities bool

	// CheckQuorum enables the leader to step down if it cannot reach a
	// quorum within an election timeout. Defaults to true when nil.
	// Set to BoolPtr(false) to explicitly disable for benchmarking
	// or compatibility testing. Disabling is UNSAFE for production.
	CheckQuorum *bool

	// PreVote enables the pre-vote protocol extension to prevent disruptive
	// elections from partitioned nodes. Defaults to true when nil.
	// Set to BoolPtr(false) to explicitly disable for benchmarking
	// or compatibility testing. Disabling is UNSAFE for production.
	PreVote *bool

	// ElectionRTT is the election timeout measured in multiples of
	// RTTMillisecond. The recommended minimum is 10. Must be greater
	// than HeartbeatRTT.
	ElectionRTT uint64

	// HeartbeatRTT is the heartbeat interval measured in multiples of
	// RTTMillisecond. Must be at least 1.
	HeartbeatRTT uint64

	// SnapshotEntries is the number of applied entries between automatic
	// snapshots. Must be greater than CompactionOverhead.
	//
	// Assign SnapshotsDisabled for manual-only snapshotting via
	// Host.RequestSnapshot. Zero does NOT disable it — SetDefaults reads an
	// unset field as "I did not choose" and substitutes
	// DefaultSnapshotEntries, because never compacting the log is not a
	// default any practical Raft deployment can live with, and it additionally
	// stops this implementation from reclaiming snapshot directories, purging
	// old epoch keys, or ever invoking the at-rest barrier. See
	// DefaultSnapshotEntries and SnapshotsDisabled.
	SnapshotEntries uint64

	// CompactionOverhead is the number of entries to retain after log
	// compaction for follower catch-up. Default: 5000.
	CompactionOverhead uint64

	// MaxInMemLogSize is the maximum in-memory Raft log size in bytes
	// before rate limiting. Default: 0 (disabled).
	MaxInMemLogSize uint64

	// MaxProposalPayloadSize is the maximum size of a single proposal payload
	// in bytes. Proposals exceeding this limit are rejected at Propose() with
	// ErrEntryTooLarge. This value must not exceed the wire protocol's
	// proto.MaxEntrySize (8 MB), which is the compile-time maximum enforced
	// during deserialization. Users may set a lower limit to constrain
	// proposal sizes but can never exceed the protocol maximum.
	//
	// Relationship with MaxEntrySize: At Propose() time, the effective limit
	// is min(MaxProposalPayloadSize, MaxEntrySize) when both are non-zero.
	// A proposal is rejected if it exceeds EITHER limit. This ensures that
	// MaxProposalPayloadSize can restrict proposals below MaxEntrySize, but
	// never bypass the transport/storage constraint imposed by MaxEntrySize.
	//
	// Default: proto.MaxEntrySize (8 MB).
	MaxProposalPayloadSize uint64

	// MaxEntrySize is the maximum Raft entry payload size in bytes for
	// transport and storage. Proposals exceeding this are rejected at
	// Propose() with ErrEntryTooLarge.
	//
	// Relationship with MaxProposalPayloadSize: Both limits are enforced at
	// Propose() time. The effective limit is min(MaxProposalPayloadSize,
	// MaxEntrySize) when both are set. Set MaxProposalPayloadSize to impose
	// a tighter user-facing limit while keeping MaxEntrySize at the
	// transport default.
	//
	// MaxEntrySize is also the decompression ceiling on the apply path, so
	// it may not exceed proto.MaxEntrySize (8 MB); Validate rejects larger
	// values with *MaxEntrySizeExceededError.
	//
	// Default: 8 MB.
	MaxEntrySize uint64

	// MaxApplyEntrySize bounds the leader's replication payload: it is wired
	// to raft's maxReplicationPayload and nothing else, so it caps the entries
	// carried by one outbound Replicate message. It can never exceed
	// MaxReplicationPayloadBytes.
	//
	// Despite the name it does NOT bound a single SM.Update() call. The apply
	// batch is bounded by maxEntriesToApplySize, a fixed 64 MiB constant in
	// pkg/internal/raft/log.go that no configuration feeds. The two are
	// independent limits on different things, and reading this field as an
	// apply-batch bound has repeatedly misled both readers and source comments.
	//
	// Default: MaxReplicationPayloadBytes (16711672 bytes, ~15.94 MB). NOT
	// 64 MB — validateSizeLimits refuses anything above the replication bound,
	// so 64 MB is not merely a stale figure, it is a value this field cannot
	// legally hold.
	MaxApplyEntrySize uint64

	// MaxSnapshotSize is the maximum snapshot file size in bytes.
	// Default: 4 GB.
	MaxSnapshotSize uint64

	// MaxResponsesPerSession is the RESPONSE-CACHE DEPTH: the maximum number of
	// cached RESPONSES retained per registered session, which is the depth of
	// the at-most-once dedup cache and not a count of sessions at all.
	// Default: 128.
	//
	// THE NAME IS WRONG and is kept only because renaming a public field is a
	// breaking change the pre-release pinning does not cover. Read it as
	// "MaxResponsesPerSession": it is passed to rsm.NewStateMachine as
	// maxResponses and reaches session.Manager as the bound on one session's
	// response map. Nothing about it limits how many sessions exist.
	//
	// The two limits it is confused with are both elsewhere and both real:
	// MaxTotalSessions bounds sessions shard-wide, and MaxSessionsPerOwner
	// bounds them per declared owner. Transposing this field with either has
	// happened here before, and neither mistake fails loudly — one caps a shard
	// at a response depth, the other lets a single client pin the whole session
	// table.
	MaxResponsesPerSession uint64

	// MaxTotalSessions is the maximum total sessions per shard across all
	// clients. Default: 16384.
	//
	// It is a shard-wide bound and therefore no defence against a single
	// principal exhausting it — clientIDs are chosen by the caller, so one
	// client can register under as many as it likes. MaxSessionsPerOwner is
	// what bounds that.
	MaxTotalSessions uint64

	// MaxSessionsPerOwner is the maximum number of concurrent sessions ONE
	// owner may hold on this shard. Default: 128; SetDefaults replaces 0
	// with it. SessionsPerOwnerUnlimited turns the cap off.
	//
	// An "owner" is a principal the EMBEDDER declares when it registers a
	// session (Host.NewSessionWithOwner, NewSessionWithOwner), carried in
	// the replicated log as proto.Entry.OwnerID. This library cannot derive one
	// on its own: it authenticates nodes, not clients, and a clientID is eight
	// bytes the caller picks. Sessions registered without an owner — every
	// session, for an embedder that does not use those APIs — are exempt.
	//
	// Like the other three session limits this is a SEED. It is written into
	// the shard's initial membership at bootstrap (proto.SessionPolicy) and
	// from then on every replica applies the committed value, because the apply
	// path reads it and a per-node value would diverge the state machines. See
	// SessionExpiryEntries for the same argument at length.
	MaxSessionsPerOwner uint64

	// SnapshotCompressionType selects the compression algorithm for
	// snapshot data.
	SnapshotCompressionType CompressionType

	// EntryCompressionType selects the compression algorithm for Raft
	// entry payloads.
	EntryCompressionType CompressionType

	// Quiesce enables quiescence for idle shards, reducing CPU usage when
	// there are no pending proposals.
	Quiesce bool

	// OrderedConfigChange has no effect. Membership changes are always
	// serialised: while one config change is appended but not yet applied,
	// the leader refuses every further one, and the Host API reports the
	// refusal as ErrRejected (nothing entered the log, so a retry once the
	// pending change commits succeeds). The Raft paper mandates
	// single-membership-change-at-a-time for safety, so the gate is not
	// configurable. Defaults to true when nil; an explicit BoolPtr(false)
	// is accepted, logs a warning in Validate, and changes nothing. The
	// field is kept for configuration compatibility.
	OrderedConfigChange *bool

	// ZeroCopyEntryCmd controls whether Entry.Cmd aliases the internal
	// buffer (zero-copy, zero-alloc). When true, the state machine MUST
	// copy Cmd if retaining it beyond Update(). Default: false (safe copy).
	ZeroCopyEntryCmd bool

	// SessionExpiryEntries controls automatic cleanup of abandoned client
	// sessions. Sessions that have not been active within this many applied
	// log entries are expired during the apply path. Expiry is driven by log
	// index rather than wall-clock time, so replicas that agree on this value
	// expire exactly the same sessions at exactly the same log index.
	//
	// It is load-bearing for apply determinism: StateMachine.Apply derives its
	// expiry checkpoints from it (multiples of the threshold), and
	// Manager.ExpireSessions uses it as the inactivity bound. Two replicas
	// evaluating different values expire different sessions, after which one
	// replica answers a session-managed proposal from its response cache while
	// the other reports the session expired — a permanent state-machine
	// divergence that no Raft invariant detects.
	//
	// It is therefore REPLICATED, and this field is only the SEED. A founding
	// replica writes it into the shard's initial membership at bootstrap
	// (proto.SessionPolicy, alongside MaxTotalSessions and
	// MaxResponsesPerSession); from then on the apply path reads the committed
	// value. All founders must supply the same value or their index-1 entries
	// differ, and every later replica must carry it too: one whose
	// configuration disagrees with what its shard committed is refused with
	// ErrSessionPolicyMismatch (StartShard on a restart; the shard fails when
	// a joiner learns the policy). There is currently no config change that
	// alters the committed policy, so it is fixed for the life of the shard.
	//
	// Assign SessionExpiryDisabled to disable automatic expiry. Zero does NOT
	// disable it — SetDefaults reads an unset field as "I did not choose" and
	// substitutes DefaultSessionExpiryEntries, because one value cannot mean
	// both that and "never expire anything". Disabling is an explicit,
	// replicated choice, not an absent one. When disabled, sessions are only
	// removed via explicit unregister or when the MaxTotalSessions limit
	// forces rejection of new registrations, so a client that registers a
	// session and dies without CloseSession leaks an entry that is serialized
	// into every subsequent snapshot and counts against MaxTotalSessions
	// forever. That leak is why the default is non-zero — see
	// DefaultSessionExpiryEntries for the choice of value.
	//
	// Keep it at or below a non-zero SnapshotEntries — Validate warns
	// otherwise — so that expiry runs at least once per snapshot interval and
	// the session table cannot grow unboundedly between snapshots. Values
	// below 100 are rejected by Validate, because a threshold that short
	// expires sessions faster than a slow client can round-trip.
	SessionExpiryEntries uint64

	// LeaseRead enables lease-based linearizable reads. While a quorum of
	// voting members has acknowledged a heartbeat round the leader SENT less
	// than the lease duration ago, ReadIndex requests are served immediately
	// without a heartbeat round-trip. The lease duration is ElectionRTT - 2
	// ticks of real time (RTTMillisecond each), less MaxClockDrift (1% by
	// default) for clock-rate drift: a follower grants no vote to another candidate until its
	// election timeout has elapsed since the heartbeat it acked, and the
	// lease ends before that. Per Raft PhD thesis Section 6.4.
	//
	// The lease is measured on the leader's monotonic clock from the time
	// the acknowledged round was sent, so a leader that is paused or
	// descheduled loses it: on resuming, the elapsed time already exceeds
	// the lease and reads take the heartbeat quorum round until fresh acks
	// arrive. It is NOT measured in ticks, which a paused process drops.
	//
	// That promise is each follower's own, so it is carried explicitly: every
	// heartbeat acknowledgement states the vote window its sender promises
	// (its ElectionRTT ticks of real time, when it runs LeaseRead), the
	// sender keeps that promise on its monotonic clock — granting no vote or
	// pre-vote and starting no campaign until the window has passed, whatever
	// happens to its term or known leader meanwhile — and the leader counts an
	// acknowledgement toward the lease only when that window covers the lease.
	// Replicas therefore need not be configured alike for lease reads to be
	// SAFE: a follower that promises less — a shorter ElectionRTT or
	// RTTMillisecond, or no LeaseRead — simply backs no lease, and while too
	// few followers back one, reads take the ReadIndex heartbeat round.
	// Configure the shard's replicas alike for lease reads to be FAST.
	//
	// A restart erases the election timer that promise lives in, so the
	// promised window is recorded in the replica's persisted Raft hard state
	// before any acknowledgement makes it, and a restarted replica withholds
	// its vote and pre-vote, and does not campaign, for the longer of that
	// recorded window and the one it now promises, measured on its monotonic
	// clock from when it starts: a leader may still be counting its
	// pre-restart acknowledgement toward a lease. This holds whatever the
	// replica restarts into, LeaseRead off included; once the window has
	// passed the record is lowered to the current promise. It keeps accepting
	// replication and heartbeats from a leader meanwhile. The cost is
	// availability: a restarted replica cannot help elect a leader for about
	// one election timeout, so after a full-shard restart the first leader is
	// elected about one election timeout later than it would be without
	// LeaseRead. Witnesses vote and their acknowledgements count toward the
	// lease, so the hold-down applies to them as well. A replica that has
	// never run LeaseRead records nothing and votes immediately after a
	// restart, as before.
	//
	// Safety assumes that the clock rates of any two nodes differ by less
	// than MaxClockDrift (1% by default). Clock OFFSET between nodes does not
	// matter; a clock that runs fast on a follower or slow on the leader
	// beyond that bound does.
	// Default: false (use heartbeat-based ReadIndex).
	LeaseRead bool

	// MaxClockDrift is the largest relative difference between the clock
	// RATES of any two replicas that LeaseRead tolerates, as a fraction:
	// 0.01 is 1%. The leader shortens its lease by this fraction of the lease
	// span, so that a lease measured on a leader clock running slow ends
	// before the vote window promised by a follower whose clock runs fast.
	// The bound is exact: with rates further apart than this, a follower
	// that promises no more than the lease span can grant a vote while the
	// lease its acknowledgement backs still holds, and a stale read becomes
	// possible.
	//
	// Raise it on hosts whose clocks are not disciplined (no NTP or PTP) or
	// whose oscillators are known to wander; the cost is a shorter lease, so
	// reads fall back to the heartbeat round a little sooner. Lowering it
	// below the default lengthens the lease and is safe only when every
	// replica's clock rate is known to stay within the smaller bound.
	//
	// Only the leader reads it, and only when LeaseRead is enabled. Zero
	// selects the default, 1%. A non-zero value must lie between
	// MaxClockDriftMin and MaxClockDriftMax; Validate rejects anything else,
	// including NaN and infinities.
	MaxClockDrift float64

	// DataDir is the directory where this shard's on-disk state-machine data
	// is stored. When empty (the default), the on-disk state machine's data
	// is placed under the host NodeHostDir, preserving the historical layout.
	// When non-empty, this shard's on-disk state-machine data (the persistent
	// store the application opens in its DiskStateMachine.Open) is rooted under
	// DataDir instead, restoring dragonboat's "the on-disk state machine owns
	// its storage location" behavior so a per-tenant shard can live on the
	// tenant's own volume. It must be an absolute path when set.
	//
	// The Raft log/WAL is unaffected and always remains in the shared,
	// host-wide LogDB regardless of this value.
	DataDir string
}

// SetDefaults fills zero-valued fields with their documented defaults.
//
// CheckQuorum, PreVote, and OrderedConfigChange default to true when nil.
// These are Raft safety invariants: disabling CheckQuorum permits a stale
// leader to serve reads after a partition heals, and disabling PreVote
// allows disruptive elections from partitioned nodes. Explicitly setting
// these to BoolPtr(false) is respected but will emit a warning during
// Validate(). OrderedConfigChange is the exception: membership changes are
// always serialised, so BoolPtr(false) is accepted and warned about but has
// no effect (see the field).
func (c *Config) SetDefaults() {
	// Safety-critical defaults: only apply when user has not explicitly set.
	if c.CheckQuorum == nil {
		c.CheckQuorum = BoolPtr(true)
	}
	if c.PreVote == nil {
		c.PreVote = BoolPtr(true)
	}
	if c.OrderedConfigChange == nil {
		c.OrderedConfigChange = BoolPtr(true)
	}

	if c.ElectionRTT == 0 {
		c.ElectionRTT = DefaultElectionRTT
	}
	if c.HeartbeatRTT == 0 {
		c.HeartbeatRTT = DefaultHeartbeatRTT
	}
	if c.CompactionOverhead == 0 {
		c.CompactionOverhead = DefaultCompactionOverhead
	}
	// An unset snapshot interval takes the default rather than meaning
	// "disabled". Never snapshotting is not a viable steady state: the log
	// grows without bound, restart replay grows with it, and in this
	// implementation the snapshot-directory GC, the epoch purge floor and the
	// at-rest barrier all advance ONLY inside saveSnapshot. Operators who
	// genuinely want manual-only snapshotting say so with SnapshotsDisabled.
	if c.SnapshotEntries == 0 {
		c.SnapshotEntries = DefaultSnapshotEntries
	}
	if c.MaxProposalPayloadSize == 0 {
		c.MaxProposalPayloadSize = uint64(proto.MaxEntrySize)
	}
	if c.MaxEntrySize == 0 {
		c.MaxEntrySize = DefaultMaxEntrySize
	}
	if c.MaxApplyEntrySize == 0 {
		c.MaxApplyEntrySize = DefaultMaxApplyEntrySize
	}
	if c.MaxSnapshotSize == 0 {
		c.MaxSnapshotSize = DefaultMaxSnapshotSize
	}
	if c.MaxResponsesPerSession == 0 {
		c.MaxResponsesPerSession = DefaultMaxResponsesPerSession
	}
	if c.MaxTotalSessions == 0 {
		c.MaxTotalSessions = DefaultMaxTotalSessions
	}
	// An unset per-owner cap takes the default rather than meaning "no limit",
	// following SessionExpiryEntries: the whole reason the field exists is that
	// an unbounded per-owner count lets one principal exhaust MaxTotalSessions,
	// so defaulting to unbounded would ship the defect as the default. Removing
	// the cap is available but has to be asked for — see
	// SessionsPerOwnerUnlimited.
	if c.MaxSessionsPerOwner == 0 {
		c.MaxSessionsPerOwner = DefaultMaxSessionsPerOwner
	}
	// An unset expiry threshold takes the default rather than meaning
	// "disabled": with expiry off, a session whose client dies without
	// CloseSession is never reclaimed. Disabling is still available, but it
	// has to be asked for — see SessionExpiryDisabled.
	if c.SessionExpiryEntries == 0 {
		c.SessionExpiryEntries = DefaultSessionExpiryEntries
	}
}

// Validate checks all cross-field constraints. It returns a
// *ValidationError describing the first violated rule, or nil
// if the configuration is valid.
func (c *Config) Validate() error {
	if err := c.validateIdentity(); err != nil {
		return err
	}
	if err := c.validateTiming(); err != nil {
		return err
	}
	if err := c.validateSizeLimits(); err != nil {
		return err
	}
	if err := c.validateSessions(); err != nil {
		return err
	}
	if err := c.validateCompression(); err != nil {
		return err
	}
	if err := c.validateLeaseRead(); err != nil {
		return err
	}
	if err := c.validateDataDir(); err != nil {
		return err
	}
	c.warnDisabledSafetyFlags()
	return nil
}

// validateDataDir checks the optional per-shard on-disk state-machine data
// directory. An empty DataDir is valid and selects the default location under
// the host NodeHostDir. When set it must be an absolute path so the on-disk
// state machine's storage location is unambiguous and independent of the
// process working directory.
func (c *Config) validateDataDir() error {
	if c.DataDir == "" {
		return nil
	}
	if !filepath.IsAbs(c.DataDir) {
		return newValidationError("DataDir", "must be an absolute path when set")
	}
	return nil
}

func (c *Config) validateIdentity() error {
	if c.ShardID == 0 {
		return newValidationError("ShardID", "must be > 0")
	}
	if c.ReplicaID == 0 {
		return newValidationError("ReplicaID", "must be > 0")
	}
	// The host packs the replica ID into the high bits of each proposal key so
	// the key is unique across the SHARD rather than just across one host --
	// without it every replica mints the same keys and an applied entry can
	// complete another replica's unrelated pending request. Bounding the ID here
	// is what keeps that packing exact instead of silently truncating. Replica
	// IDs are small operator-assigned ordinals; four billion is far beyond any
	// real deployment.
	if c.ReplicaID > math.MaxUint32 {
		return newValidationError("ReplicaID",
			"must be <= 4294967295 (the proposal key packs the replica ID)")
	}
	if c.IsObserver && c.IsWitness {
		return newValidationError("IsObserver", "IsObserver and IsWitness are mutually exclusive")
	}
	if c.Witness.MaxWitnesses < 0 {
		return newValidationError("Witness.MaxWitnesses", "must be >= 0 (0 means no explicit cap)")
	}
	return nil
}

// ValidateElectionTimeout checks that this shard's election timeout,
// ElectionRTT ticks of rttMillisecond each, is representable as a
// time.Duration on the host that runs it. Validate cannot check it, because
// the tick length is host configuration: ElectionRTT and RTTMillisecond are
// each bounded on their own, but their product is not, and a product that
// overflows wraps negative. The raft layer measures the post-restart vote
// hold-down as that product, so an unrepresentable timeout would leave the
// replica unable to measure the end of the hold-down. Host.StartShard calls
// this with its RTTMillisecond.
func (c *Config) ValidateElectionTimeout(rttMillisecond uint64) error {
	const nanosPerMilli = 1_000_000
	if rttMillisecond > 0 && c.ElectionRTT > uint64(math.MaxInt64)/nanosPerMilli/rttMillisecond {
		return newValidationError("ElectionRTT",
			"ElectionRTT * RTTMillisecond overflows time.Duration on this host")
	}
	return nil
}

func (c *Config) validateTiming() error {
	if c.HeartbeatRTT < 1 {
		return newValidationError("HeartbeatRTT", "must be >= 1")
	}
	if c.ElectionRTT < 5 {
		return newValidationError("ElectionRTT", "must be >= 5 (10 recommended)")
	}
	// Bound both RTTs to math.MaxInt32. The raft layer narrows these uint64
	// values to int (int(cfg.ElectionRTT)) and feeds ElectionRTT into
	// rng.Intn(electionTimeout). A value >= 2^63 narrows to a NEGATIVE int and
	// panics StartShard; a value in (2^62, 2^63) survives the narrowing but
	// produces an electionTimeout so large the shard never elects. Capping at
	// MaxInt32 keeps every downstream int conversion and arithmetic
	// (including 2*HeartbeatRTT below) safely in range. Real deployments use
	// tick multiples in the tens; two billion is far beyond any legitimate value.
	if c.HeartbeatRTT > math.MaxInt32 {
		return newValidationError("HeartbeatRTT",
			"must be <= 2147483647 (raft narrows the tick multiple to int)")
	}
	if c.ElectionRTT > math.MaxInt32 {
		return newValidationError("ElectionRTT",
			"must be <= 2147483647 (raft narrows the tick multiple to int)")
	}
	// Require a comfortable margin between the election timeout and the
	// heartbeat interval. Raft needs the election timeout to span several
	// heartbeat intervals so a healthy leader's heartbeats reliably reset
	// follower election timers; a ratio barely above 1 (e.g. Heartbeat=4,
	// Election=5) passes "Election > Heartbeat" yet causes spurious elections
	// and leadership churn under normal latency jitter. Enforce at least 2x
	// (10x recommended: HeartbeatRTT=1, ElectionRTT=10). Compare as
	// ElectionRTT/2 < HeartbeatRTT so the check cannot wrap: 2*HeartbeatRTT
	// would overflow uint64 for a large HeartbeatRTT and spuriously pass.
	if c.ElectionRTT/2 < c.HeartbeatRTT {
		return newValidationError("ElectionRTT",
			"must be >= 2*HeartbeatRTT (10*HeartbeatRTT recommended)")
	}
	return nil
}

func (c *Config) validateSizeLimits() error {
	if c.SnapshotEntries > 0 && c.SnapshotEntries <= c.CompactionOverhead {
		return newValidationError("SnapshotEntries", "must be > CompactionOverhead when non-zero")
	}
	if c.MaxProposalPayloadSize == 0 {
		return newValidationError("MaxProposalPayloadSize", "must be > 0")
	}
	if c.MaxProposalPayloadSize > uint64(proto.MaxEntrySize) {
		return &MaxProposalPayloadSizeExceededError{
			Configured:      c.MaxProposalPayloadSize,
			ProtocolMaximum: uint64(proto.MaxEntrySize),
		}
	}
	if c.MaxEntrySize == 0 {
		return newValidationError("MaxEntrySize", "must be > 0")
	}
	// MaxEntrySize doubles as the decompression ceiling on the apply path,
	// so an operator-raised value above the protocol maximum would let a
	// small compressed entry expand to an arbitrary size in memory.
	if c.MaxEntrySize > uint64(proto.MaxEntrySize) {
		return &MaxEntrySizeExceededError{
			Configured:      c.MaxEntrySize,
			ProtocolMaximum: uint64(proto.MaxEntrySize),
		}
	}
	if c.MaxApplyEntrySize == 0 {
		return newValidationError("MaxApplyEntrySize", "must be > 0")
	}
	if c.MaxApplyEntrySize > MaxReplicationPayloadBytes {
		return &MaxApplyEntrySizeExceededError{
			Configured:   c.MaxApplyEntrySize,
			FrameMaximum: MaxReplicationPayloadBytes,
		}
	}
	return nil
}

func (c *Config) validateSessions() error {
	if c.MaxResponsesPerSession == 0 {
		return newValidationError("MaxResponsesPerSession", "must be > 0")
	}
	if c.MaxTotalSessions == 0 {
		return newValidationError("MaxTotalSessions", "must be > 0")
	}
	if c.SessionExpiryEntries != 0 && c.SessionExpiryEntries < 100 {
		return &SessionExpiryEntriesTooSmallError{
			Configured: c.SessionExpiryEntries,
			Minimum:    100,
		}
	}
	if c.SessionExpiryEntries != 0 && c.SnapshotEntries != 0 &&
		c.SessionExpiryEntries > c.SnapshotEntries {
		slog.Warn("SessionExpiryEntries exceeds SnapshotEntries: sessions may never expire between snapshots",
			"session_expiry_entries", c.SessionExpiryEntries,
			"snapshot_entries", c.SnapshotEntries,
		)
	}
	return nil
}

func (c *Config) validateCompression() error {
	if !c.SnapshotCompressionType.IsValid() {
		return newValidationError("SnapshotCompressionType", "unrecognized compression type")
	}
	if !c.EntryCompressionType.IsValid() {
		return newValidationError("EntryCompressionType", "unrecognized compression type")
	}
	return nil
}

func (c *Config) validateLeaseRead() error {
	if c.LeaseRead && !c.GetCheckQuorum() {
		return &LeaseReadRequiresCheckQuorumError{}
	}
	if c.LeaseRead && c.ElectionRTT < 5 {
		return newValidationError("ElectionRTT",
			"must be >= 5 when LeaseRead is enabled (lease timeout = ElectionRTT - 2)")
	}
	// Checked whether or not LeaseRead is on, so a value that would be
	// rejected is not carried silently until the lease is enabled.
	if d := c.MaxClockDrift; d != 0 && (math.IsNaN(d) || d < MaxClockDriftMin || d > MaxClockDriftMax) {
		return newValidationError("MaxClockDrift",
			"must be 0 (the default, 1%) or between MaxClockDriftMin (1e-6) and MaxClockDriftMax (0.5)")
	}
	return nil
}

// warnDisabledSafetyFlags emits warnings when safety booleans are explicitly
// disabled. These are not rejected to support benchmarking and compatibility testing.
func (c *Config) warnDisabledSafetyFlags() {
	if c.CheckQuorum != nil && !*c.CheckQuorum {
		slog.Warn("CheckQuorum disabled: leader will not step down on quorum loss")
	}
	if c.PreVote != nil && !*c.PreVote {
		slog.Warn("PreVote disabled: partitioned nodes may cause disruptive elections")
	}
	if c.OrderedConfigChange != nil && !*c.OrderedConfigChange {
		slog.Warn("OrderedConfigChange=false has no effect: membership changes are always serialised")
	}
}

// GetCheckQuorum returns the effective CheckQuorum value.
// Returns true if CheckQuorum is nil (safe default).
func (c *Config) GetCheckQuorum() bool {
	if c.CheckQuorum == nil {
		return true
	}
	return *c.CheckQuorum
}

// GetPreVote returns the effective PreVote value.
// Returns true if PreVote is nil (safe default).
func (c *Config) GetPreVote() bool {
	if c.PreVote == nil {
		return true
	}
	return *c.PreVote
}

// GetOrderedConfigChange returns the effective OrderedConfigChange value.
// Returns true if OrderedConfigChange is nil (safe default). The value
// governs nothing: membership changes are always serialised.
func (c *Config) GetOrderedConfigChange() bool {
	if c.OrderedConfigChange == nil {
		return true
	}
	return *c.OrderedConfigChange
}
