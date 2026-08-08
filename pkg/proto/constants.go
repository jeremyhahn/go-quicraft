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
package proto

import "fmt"

// WireVersion is the current binary wire format version. It is written as
// the first 8 bytes of on-disk structures. Receivers validate the version
// before unmarshaling; a mismatch is reported as ErrUnsupportedVersion.
//
// Version history:
//
//	v1: original layout (Membership base 8 bytes, ConfigChange fixed 36 bytes).
//	v2: replicated witness policy — Membership gained MaxWitnesses and
//	    RequireFullReplicaMajority (base 8 -> 24 bytes) and ConfigChange
//	    gained the same fields (fixed 36 -> 52 bytes). The v1 layouts carry
//	    no per-type version byte and cannot be decoded by v2.
//	v3: Membership gained a leading version byte (base 24 -> 25 bytes), so a
//	    legacy section is rejected in band instead of being misparsed. The
//	    embedding types (Snapshot, SnapshotChunk) grow by the same byte.
//	    Bumped because v2 and v3 encode Membership differently while both
//	    would otherwise stamp BinVer 2, which is the exact ambiguity the
//	    version byte removes.
const WireVersion uint64 = 3

// MaxEntrySize is the maximum byte length of an Entry.Cmd payload.
// Enforced during UnmarshalFrom to prevent memory exhaustion from
// crafted transport payloads.
const MaxEntrySize uint32 = 8 * 1024 * 1024 // 8MB

// MaxBatchSize is the maximum cumulative byte size of a deserialized
// MessageBatch. Prevents a single malicious batch from allocating
// unbounded memory. Default 64MB. Type is uint64 to allow safe
// comparison against int offsets without truncation on 64-bit platforms.
const MaxBatchSize uint64 = 64 * 1024 * 1024 // 64MB

// MaxAddressLength is the maximum byte length of a node address string
// in membership changes and config change proposals. Enforced during
// UnmarshalFrom to prevent unbounded allocation from crafted payloads.
const MaxAddressLength = 256

// MaxFilepathLen is the maximum byte length of a filepath string in
// snapshot and snapshot file metadata. Enforced during UnmarshalFrom
// to prevent unbounded allocation from crafted payloads.
const MaxFilepathLen = 4096

// MaxChecksumLen is the maximum byte length of a checksum field in
// snapshot metadata. Sized for SHA-512 (64 bytes).
const MaxChecksumLen uint32 = 64

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

// maxMembershipEntries is the maximum number of entries allowed in a
// single Membership map. Validated before allocation in UnmarshalFrom.
const maxMembershipEntries = 10000

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
)

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
)

// maxConfigChangeType is the upper bound for valid ConfigChange.Type values.
// Used in UnmarshalFrom validation instead of referencing AddWitness directly.
const maxConfigChangeType = UpdateWitnessPolicy

// maxStateMachineType is the upper bound for valid Snapshot.Type values.
// Used in UnmarshalFrom validation to reject unknown state machine types.
const maxStateMachineType = OnDiskStateMachine

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
