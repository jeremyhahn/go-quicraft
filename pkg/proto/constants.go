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

// Package proto defines the binary wire format types for the QuicRaft
// Raft protocol. All serializable types use a custom fixed-layout binary
// encoding: no varints, no type tags, no code generation, no external
// dependencies. Every uint64 field is encoded as 8 bytes little-endian.
// Booleans are stored as uint64 on wire (0 or 1). Variable-length fields
// use a 4-byte uint32 length prefix followed by raw data. Maps are encoded
// with a 4-byte count prefix and entries sorted by key ascending for
// deterministic output.
//
// # Compatibility contract: every format version is PINNED pre-release
//
// go-quicraft has not made a production release. Until it does, EVERY wire and
// persisted format version in this repository is pinned at its current value.
// Most sit at 1:
// WireVersion, membershipFormatVersion, internal/transport.WireVersion,
// logdb/waldb.membershipSectionVersion, logdb/waldb.walFormatVersion and
// internal/engine.snapshotMetaVersion. Two sit at 2 and stay there: the
// exported snapshot artifact (snapshotArtifactVersion in package quicraft) and
// the sealed-state MAC (seal.sealedStateMACVersion). The rest of this comment
// speaks of the formats this package owns, which sit at 1 and whose decoders
// gate on EXACT equality with 1.
//
// A layout change during this period is made IN PLACE, at the pinned version,
// and the consuming repositories (go-qrdb and any other embedder) are
// updated to match in the same change. Development clusters are WIPED and
// re-synced rather than migrated. There is no dual-read path, no migration
// window, and no legacy decode case, because there is no deployed data and no
// deployed peer whose bytes anyone has promised to keep reading.
//
// The consequence is blunt and intended: two binaries built from different
// pre-release commits may be mutually unreadable while BOTH stamp version 1.
// Mixing them — over the wire, or over a data directory one of them wrote —
// is unsupported, and the failure it produces is a decode error, not a
// version mismatch that names itself. Rebuild every node and every consuming
// repo from the same commit.
//
// The version numbers increment for the first time at the first production
// release. From that point a bump means what it normally means: a decoder
// that must be taught the old layout, a migration story, and a supported
// window that widens rather than moves.
//
// # Why the version byte stays
//
// Pinning the VALUE is not the same as removing the mechanism. Every format
// still writes its version and still refuses anything that is not 1, for two
// reasons. It is what makes the 1.0 bump possible at all: a format with no
// discriminator cannot be superseded in band, which is exactly the state
// Membership was in before it grew a leading byte, and a section with no
// version byte is byte-indistinguishable from one whose first field happens to
// start with the same value. And it is what turns a commit skew into a clean
// refusal instead of a misparse — the wrong bytes are rejected at the gate
// rather than interpreted at the wrong offsets.
//
// # Two namespaces, one value
//
// The package carries two version namespaces which happen to hold the same
// value today and must NOT be derived from each other:
//
//   - TRANSPORT-scoped: WireVersion. Stamped into MessageBatch.BinVer and
//     SnapshotChunk.BinVer, and mirrored by the frame header byte in
//     internal/transport. Both ends of a QUIC connection are live processes,
//     so a mismatch costs one rejected connection and nothing durable.
//
//   - PERSISTED-scoped: membershipFormatVersion (see membership.go). Stamped
//     as the leading byte of every Membership section, which is written to
//     the WAL, into snapshot records and exported snapshot artifacts, and
//     into the EntryInitialMembership entry at log index 1. Those bytes
//     outlive the process — and the binary — that wrote them.
//
// The distinction is not stylistic, and it is why the two survive as separate
// constants through the pinned period. Membership is the bootstrap
// configuration at log index 1, so if the persisted version were derived from
// WireVersion, the first post-1.0 transport-only bump would simultaneously
// change the version byte every replica expects at log index 1 and leave a
// released cluster unable to replay its own log.
//
// # The transport version is a SUPREMUM
//
// Independent does not mean unrelated. Membership is embedded in Snapshot,
// which is embedded in Message, which is embedded in MessageBatch, and it is
// embedded directly in SnapshotChunk. Both of those gate on
// BinVer == WireVersion by EXACT equality, so a Membership layout change
// alters the transport bytes without altering the discriminator that is
// supposed to describe them — two dialects stamping one version number.
//
// THE RULE, from the first production release onward: bump WireVersion
// whenever ANY type reachable from MessageBatch or SnapshotChunk changes
// layout, INCLUDING the embedded persisted ones. The transport number is the
// supremum of every layout version on the wire, never a number that only
// tracks the transport-only fields. The blank-identifier constant assertion in
// membership.go enforces the Membership half of that at compile time — it
// holds trivially at 1 >= 1 and is retained precisely because it is what stops
// the two drifting apart at the bump that matters.
//
// The converse does NOT hold: WireVersion may move without membershipFormatVersion
// moving, because a transport-only change leaves every persisted byte alone. That
// asymmetry is why the assertion is >= and not ==.
package proto

import (
	"fmt"
	"math"
)

// WireVersion is the TRANSPORT wire format version. It is written as the first
// 8 bytes of MessageBatch and into SnapshotChunk.BinVer. Receivers validate it
// for exact equality before unmarshaling; a mismatch is reported as
// ErrUnsupportedVersion.
//
// It is PINNED at 1 for the whole pre-release period and does not move for a
// layout change — the layout changes in place and every consumer is rebuilt.
// See the compatibility contract in the package comment for the policy, what
// it costs (peers from different pre-release commits may be mutually
// unreadable while both stamp 1), and when the pinning ends.
//
// Scope: transport only. Nothing stamped with WireVersion is read back off
// disk. It is NOT the version of any persisted structure — in particular, the
// persisted Membership section carries its own membershipFormatVersion, which
// this constant must never be substituted for or derived into.
const WireVersion uint64 = 1

// PreviousWireVersion is the WireVersion of the previous MINOR release, the
// second member of the rolling-upgrade accept set (docs/compatibility,
// architecture question 3). In v1.0 there is no previous release, so it
// equals WireVersion and the set has exactly one member. A release that moves
// WireVersion sets this to the value it replaced.
const PreviousWireVersion uint64 = WireVersion

// WireWindow is the closed set of transport versions a build accepts:
// Current, which it advertises, and Previous, which it still reads and keeps
// sending until its shard commits the switch. It is a set, not a range, so it
// never widens when a constant moves.
type WireWindow struct {
	Current  uint64
	Previous uint64
}

// DefaultWireWindow returns this build's window.
func DefaultWireWindow() WireWindow {
	return WireWindow{Current: WireVersion, Previous: PreviousWireVersion}
}

// Accepts reports whether v is a member of the window.
func (w WireWindow) Accepts(v uint64) bool {
	return v == w.Current || v == w.Previous
}

// CheckCommit validates v as the committed wire version of a shard whose
// current committed value is cur (zero meaning never switched): v must be in
// the window and must not be lower than cur's effective value. A switch is
// never reverted. Both failures are ErrUnsupportedVersion, so a build that
// meets a switch it cannot read stops the shard instead of applying it.
func (w WireWindow) CheckCommit(cur, v uint64) error {
	if !w.Accepts(v) {
		return ErrUnsupportedVersion
	}
	if v < EffectiveWireVersion(cur) {
		return ErrUnsupportedVersion
	}
	return nil
}

// EffectiveWireVersion is the version a shard whose committed
// Membership.WireVersion is v sends at: PreviousWireVersion until it has
// switched.
func EffectiveWireVersion(v uint64) uint64 {
	if v == 0 {
		return PreviousWireVersion
	}
	return v
}

// AcceptsWireVersion reports whether v is in this build's accept set.
func AcceptsWireVersion(v uint64) bool {
	return DefaultWireWindow().Accepts(v)
}

// MaxEntrySize is the maximum byte length of an Entry.Cmd payload.
// Enforced during UnmarshalFrom to prevent memory exhaustion from
// crafted transport payloads.
const MaxEntrySize uint32 = 8 * 1024 * 1024 // 8MB

// MaxBatchSize is the maximum cumulative byte size of a deserialized
// MessageBatch. Prevents a single malicious batch from allocating
// unbounded memory. Default 64MB. Type is uint64 to allow safe
// comparison against int offsets without truncation on 64-bit platforms.
const MaxBatchSize uint64 = 64 * 1024 * 1024 // 64MB

// MaxDecodeMemoryFactor is the maximum heap memory one decode may allocate,
// expressed as a multiple of the frame's WIRE length. A receiver that admits an
// N-byte frame must expect to hold up to N*MaxDecodeMemoryFactor bytes while it
// processes the decoded value.
//
// It exists because a wire-byte budget is not a memory budget. The decoded form
// of a frame is materially larger than the frame: a 100-byte heartbeat becomes a
// 304-byte Message in a slice, and a membership map entry costs 12 bytes on the
// wire and about 57 in a Go map (see mapAllocBytesPerEntry). A budget charged
// in wire bytes describes bytes read rather than bytes held: 256 MB of admitted
// frames can occupy up to 2 GB of heap. The QUIC transport therefore reserves
// each frame's length MULTIPLIED by this factor against its aggregate
// message-receive budget (MaxMessageReceiveMemory) before reading the body, so
// that budget bounds decoded heap, not wire bytes. DecodeCharge computes that
// charge, and FrameDecodeCharge the charge of a frame plus its decompression
// delta; a receiver built on this package without that transport must apply
// them itself.
//
// The bound is MEASURED, not derived — TestDecodeMemoryAmplification builds the
// densest frames this format admits and fails if any exceeds it — and the
// measured worst case is 5.1x, on a batch of small messages each carrying a
// snapshot whose membership maps hold a single entry (a one-entry Go map owns a
// whole 8-slot group). The constant carries headroom above that because the
// dominant term is the Go runtime's map layout, which this package does not
// control and cannot assert at compile time. It is not headroom for adding
// allocations: a decoder that clamps its map size hint and pays for the growth
// reaches 9.8x, which is the regression the test catches.
const MaxDecodeMemoryFactor = 8

// MaxTerm is the exclusive upper bound on a Raft term this library adopts from
// a peer, admits into a snapshot, or opens from persisted state; MaxLogIndex is
// the same bound for a log index. Nothing bounds either in the wire encoding —
// both are plain uint64 fields — and the protocol does arithmetic on both:
// becomeCandidate increments the term, and a restored snapshot's index is the
// base of Index+1 for the log's first index, every remote's next index and the
// in-memory log marker. At math.MaxUint64 each of those WRAPS to zero. A term
// that wrapped lets a replica vote a second time in a term it already voted in
// (Election Safety), and an index that wrapped puts the log's floor at 0 while
// the state machine and persisted snapshot sit at the ceiling — and because a
// snapshot is written to LogDB before raft restores it, the poisoned position
// survives restart.
//
// The two are enforced where each value is ADMITTED: raft.step refuses a wire
// term at or above MaxTerm; snapshot admission (the transport's chunk
// validation, raft's InstallSnapshot handler and both LogDB SaveSnapshot and
// SaveState implementations) refuses a snapshot whose Index or Term is out of
// range via Snapshot.CheckBounds; and a leader refuses proposals once its log
// has reached MaxLogIndex. InstallSnapshot is delivered process-locally after
// the artifact is on disk, so it never passes raft.step's wire gate — which is
// why the snapshot bound is enforced on its own path rather than inherited.
//
// Opening persisted state is held to a HIGHER ceiling (raft.persistedCeiling,
// a few below math.MaxUint64), not to these bounds: state admitted just below
// them can be moved by local transitions — the candidate's increment, the
// leader's no-op — and refusing it at open would turn a legitimate restart into
// a permanent failure. The headroom between the two is what those transitions
// spend.
//
// A term advances by one per election and an index by one per entry, so no
// cluster reaches this range by running; only a peer naming it can.
const (
	MaxTerm     uint64 = math.MaxUint64 - 1024
	MaxLogIndex uint64 = math.MaxUint64 - 1024
)

// MaxAddressLength is the maximum byte length of a node address string
// in membership changes and config change proposals. Enforced during
// UnmarshalFrom to prevent unbounded allocation from crafted payloads.
const MaxAddressLength = 256

// MaxNodeIdentityLength is the maximum byte length of a committed node
// identity string. The identity this build produces is "spki:" plus a 64-char
// SHA-256 hex digest — 69 bytes — so the bound is generous headroom for a
// future scheme (a SAN URI, say) while still refusing an unbounded allocation
// from a crafted payload.
const MaxNodeIdentityLength = 256

// MaxFilepathLen is the maximum byte length of a filepath string in
// snapshot and snapshot file metadata. Enforced during UnmarshalFrom
// to prevent unbounded allocation from crafted payloads.
const MaxFilepathLen = 4096

// MaxMetadataLen is the maximum byte length of application-defined
// metadata in snapshot file records. Enforced during UnmarshalFrom.
const MaxMetadataLen uint32 = 65536 // 64KB

// MaxSnapshotChunkDataLen is the maximum byte length of a snapshot
// chunk's data payload. Matches MaxEntrySize as a reasonable upper bound.
const MaxSnapshotChunkDataLen uint32 = MaxEntrySize

// MaxMessagesPerBatch is the maximum number of messages allowed in a
// single MessageBatch. Validated before allocation in MessageBatch's
// UnmarshalFrom, which returns ErrTooManyMessages beyond it.
//
// Exported because it is a property of the wire protocol, not of the
// decoder: a producer that exceeds it emits a batch every peer rejects,
// and a rejection is indistinguishable from a broken connection at the
// sender. Producers MUST bound themselves by this value.
const MaxMessagesPerBatch = 10000

// MaxEntriesPerMessage is the maximum number of log entries a single
// Message may carry. Validated before allocation in Message's
// UnmarshalFrom, which returns ErrTooManyMessages beyond it.
//
// Exported for the same reason as MaxMessagesPerBatch: raft sizes a
// Replicate message against Config.MaxApplyEntrySize (a BYTE bound), so
// with small entries it can build a message no peer will accept. Every
// producer of multi-entry messages must also bound the entry COUNT by
// this value.
const MaxEntriesPerMessage = 10000

// MaxMembershipEntries is the maximum number of entries allowed in a single
// Membership map. Validated before allocation in UnmarshalFrom, and mirrored on
// the write side in MarshalTo.
//
// Exported for the same reason as MaxMessagesPerBatch and MaxEntriesPerMessage:
// it is a property of the format, not of the decoder, and callers that build a
// membership have to bound themselves by it. Host.StartShard in particular takes
// the members map straight from the operator into Membership.Addresses and
// commits it at log index 1, so a caller that exceeds this writes a bootstrap
// entry no binary can read back.
const MaxMembershipEntries = 10000

// Entry type constants identify the kind of log entry. The lower 4 bits
// of Entry.Type hold the entry type; upper bits hold compression flags.
// Always mask with EntryTypeMask before comparing:
//
//	entryType := e.Type & EntryTypeMask
const (
	EntryNormal            uint64 = 0
	EntryConfigChange      uint64 = 1
	EntryEncodedEntry      uint64 = 2 // session-managed entry
	EntryMetadata          uint64 = 3 // witness metadata entry (Index+Term only, no Cmd)
	EntryInitialMembership uint64 = 4 // bootstrap membership (Cmd = marshaled proto.Membership)
)

// Compression constants. Compression is indicated by OR'ing the
// compression flag into Entry.Type's upper bits.
const (
	EntryTypeMask     uint64 = 0x0F
	EntryCompressFlag uint64 = 0x10
)

// Message type constants identify the kind of Raft protocol message.
// Used as the Message.Type field on the wire.
const (
	LocalTick          uint64 = 0
	Election           uint64 = 1
	LeaderHeartbeat    uint64 = 2
	ConfigChangeEvent  uint64 = 3
	NoOP               uint64 = 4
	Ping               uint64 = 5
	Pong               uint64 = 6
	Propose            uint64 = 7
	SnapshotStatus     uint64 = 8
	Unreachable        uint64 = 9
	CheckQuorum        uint64 = 10
	BatchedReadIndex   uint64 = 11
	Replicate          uint64 = 12
	ReplicateResp      uint64 = 13
	RequestVote        uint64 = 14
	RequestVoteResp    uint64 = 15
	InstallSnapshot    uint64 = 16
	Heartbeat          uint64 = 17
	HeartbeatResp      uint64 = 18
	ReadIndex          uint64 = 19
	ReadIndexResp      uint64 = 20
	Quiesce            uint64 = 21
	SnapshotReceived   uint64 = 22
	LeaderTransfer     uint64 = 23
	TimeoutNow         uint64 = 24
	RequestPreVote     uint64 = 25
	RequestPreVoteResp uint64 = 26
	RateLimit          uint64 = 27
	// LeaderExec carries a leader-executed request from the replica a caller
	// submitted it on to the replica that caller believes leads the shard. It
	// never reaches raft: the host's message handler takes it once the sender
	// is bound and answers it with LeaderExecResp. Term is the leadership term
	// the request may be executed under, Hint the caller's request ID,
	// LogIndex the caller's remaining budget in nanoseconds (0 for none) and
	// Entries[0].Cmd the request payload.
	LeaderExec uint64 = 28
	// LeaderExecResp answers a LeaderExec. Term and Hint echo the request's,
	// Reject marks a refusal whose reason code is in LogIndex, and
	// Entries[0].Cmd carries the handler's reply (or its error text).
	LeaderExecResp uint64 = 29
)

// maxMessageType is the largest valid Message.Type, the bound MarshalTo and
// UnmarshalFrom hold every message to.
const maxMessageType = LeaderExecResp

// Config change type constants identify the kind of membership change.
const (
	AddNode      uint64 = 0
	RemoveNode   uint64 = 1
	AddNonVoting uint64 = 2
	AddWitness   uint64 = 3
	// UpdateWitnessPolicy replaces the shard's replicated witness policy
	// (MaxWitnesses, RequireFullReplicaMajority). Carries no target
	// replica; the policy fields on ConfigChange hold the new values.
	UpdateWitnessPolicy uint64 = 4
	// UpdateWireVersion commits the shard's switch to a newer transport
	// version (docs/compatibility, "The switch is a config change"). Carries
	// no target replica; ConfigChange.WireVersion holds the new value, which
	// apply refuses if it is lower than the current one or outside the
	// accept set.
	UpdateWireVersion uint64 = 5
)

// maxConfigChangeType is the upper bound for valid ConfigChange.Type values.
// Used in UnmarshalFrom validation instead of referencing AddWitness directly.
const maxConfigChangeType = UpdateWireVersion

// messageTypeNames maps message type constants to human-readable names.
var messageTypeNames = map[uint64]string{
	LocalTick:          "LocalTick",
	Election:           "Election",
	LeaderHeartbeat:    "LeaderHeartbeat",
	ConfigChangeEvent:  "ConfigChangeEvent",
	NoOP:               "NoOP",
	Ping:               "Ping",
	Pong:               "Pong",
	Propose:            "Propose",
	SnapshotStatus:     "SnapshotStatus",
	Unreachable:        "Unreachable",
	CheckQuorum:        "CheckQuorum",
	BatchedReadIndex:   "BatchedReadIndex",
	Replicate:          "Replicate",
	ReplicateResp:      "ReplicateResp",
	RequestVote:        "RequestVote",
	RequestVoteResp:    "RequestVoteResp",
	InstallSnapshot:    "InstallSnapshot",
	Heartbeat:          "Heartbeat",
	HeartbeatResp:      "HeartbeatResp",
	ReadIndex:          "ReadIndex",
	ReadIndexResp:      "ReadIndexResp",
	Quiesce:            "Quiesce",
	SnapshotReceived:   "SnapshotReceived",
	LeaderTransfer:     "LeaderTransfer",
	TimeoutNow:         "TimeoutNow",
	RequestPreVote:     "RequestPreVote",
	RequestPreVoteResp: "RequestPreVoteResp",
	RateLimit:          "RateLimit",
	LeaderExec:         "LeaderExec",
	LeaderExecResp:     "LeaderExecResp",
}

// MessageTypeName returns the human-readable name for a message type constant.
// Returns "Unknown(N)" for unrecognized values.
func MessageTypeName(t uint64) string {
	if name, ok := messageTypeNames[t]; ok {
		return name
	}
	return fmt.Sprintf("Unknown(%d)", t)
}

// configChangeTypeNames maps config change type constants to human-readable names.
var configChangeTypeNames = map[uint64]string{
	AddNode:             "AddNode",
	RemoveNode:          "RemoveNode",
	AddNonVoting:        "AddNonVoting",
	AddWitness:          "AddWitness",
	UpdateWitnessPolicy: "UpdateWitnessPolicy",
	UpdateWireVersion:   "UpdateWireVersion",
}

// ConfigChangeTypeName returns the human-readable name for a config change type constant.
// Returns "Unknown(N)" for unrecognized values.
func ConfigChangeTypeName(t uint64) string {
	if name, ok := configChangeTypeNames[t]; ok {
		return name
	}
	return fmt.Sprintf("Unknown(%d)", t)
}

// IsFreeOrderMessage returns true for message types that may be sent
// before the corresponding state is persisted to stable storage.
// Per Raft PhD thesis section 10.2.1, only Replicate and Ping messages
// are "free-order" -- they carry data to followers but do not constitute
// an RPC response that depends on persisted state. Heartbeat messages
// are also free-order because they are leader-initiated authority
// assertions, not responses to RPCs. Quiesce is free-order because it
// is an advisory message with no persistence dependency.
//
// PRE-VOTE is free-order for a different reason: it persists nothing.
// becomePreCandidate explicitly does not change term or vote, and
// handleVoteRequest sets r.vote only on the real-vote path, so neither
// RequestPreVote nor RequestPreVoteResp has any durable state to wait for.
// Treating them as ordered starved elections under sustained write load:
// while commitPending is set the step worker transmits only free-order
// messages, and on a busy shard commitPending is set almost continuously,
// so pre-vote traffic sat in the outbound buffer. Measured on a 5-node
// cluster under chaos, all three surviving replicas pre-campaigned ~28
// times each without a single round completing, and the shard had no leader
// for over 20 seconds with quorum fully available. PreVote's whole purpose
// is to test electability without touching persistent state; delaying it
// behind a WAL write inverts that.
//
// All remaining messages (VoteResp, HeartbeatResp, ReplicateResp, etc.)
// are "ordered" and MUST NOT be sent until the corresponding state
// change (vote grant, term update, log append) is durable on stable
// storage. Violating this invariant can cause split-brain: a follower
// could grant VoteResp before persisting its vote, crash, then vote
// again for a different candidate in the same term.
func IsFreeOrderMessage(msgType uint64) bool {
	return msgType == Replicate || msgType == Ping ||
		msgType == Heartbeat || msgType == Quiesce ||
		msgType == RequestPreVote || msgType == RequestPreVoteResp
}

// IsFreeOrder is IsFreeOrderMessage for a message rather than a type, and it
// is the predicate every free-order/ordered split applies. It adds the one
// case the type alone cannot decide: a ReadIndexResp with Reject set. A
// leader's refusal of a forwarded read names no index and depends on no
// state the leader persists (Raft PhD 6.4 refuses the read precisely because
// nothing of the term has committed), so it has nothing to wait for. Held as
// ordered it waited out the leader's commit pipeline, which for a leader that
// has not yet committed in its term is the whole window the refusal covers,
// and the follower's caller waited with it (#376). An accepted ReadIndexResp
// stays ordered.
//
// HeartbeatResp is ordered here but conditionally free-order in one place:
// Peer.DrainFreeOrderMessages, the drain the step worker uses while a WAL
// write is in flight, releases it when the step that produced it left term,
// vote, owed vote window and log at their durable values (#390). That needs
// the follower's durable state, which no message predicate can see, so the
// decision lives in the raft package.
func IsFreeOrder(m *Message) bool {
	return IsFreeOrderMessage(m.Type) || (m.Type == ReadIndexResp && m.Reject)
}

// State machine type constants.
const (
	RegularStateMachine    uint64 = 0
	ConcurrentStateMachine uint64 = 1
	OnDiskStateMachine     uint64 = 2
)

// StateMachineTypeName returns a human-readable name for the given
// state machine type code. Used in log output and diagnostics.
func StateMachineTypeName(smType uint64) string {
	switch smType {
	case RegularStateMachine:
		return "regular"
	case ConcurrentStateMachine:
		return "concurrent"
	case OnDiskStateMachine:
		return "on-disk"
	default:
		return "unknown"
	}
}

// Session management constants. These sentinel SeriesID values distinguish
// session register/unregister operations from normal proposals.
const (
	// NotSessionManagedClientID is the client ID that indicates the entry
	// is not managed by a client session (no-op sessions).
	NotSessionManagedClientID uint64 = 0

	// SessionSeriesIDForRegister is the sentinel SeriesID for registering
	// a new client session through Raft consensus.
	SessionSeriesIDForRegister uint64 = 1<<64 - 2 // math.MaxUint64 - 1

	// SessionSeriesIDForUnregister is the sentinel SeriesID for
	// unregistering a client session through Raft consensus.
	SessionSeriesIDForUnregister uint64 = 1<<64 - 1 // math.MaxUint64
)
