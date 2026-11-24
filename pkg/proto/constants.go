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

// Package proto defines the fixed-layout little-endian binary wire format; format versions stay pinned until the first release, and WireVersion must cover every layout reachable from MessageBatch or SnapshotChunk.
package proto

import (
	"fmt"
	"math"
)

// WireVersion is the transport wire format version, checked for exact equality (ErrUnsupportedVersion); never used for persisted formats.
const WireVersion uint64 = 1

// PreviousWireVersion is the previous minor release's WireVersion, the second member of the accept set; it equals WireVersion until a release moves it.
const PreviousWireVersion uint64 = WireVersion

// WireWindow is the closed set of transport versions a build accepts: Current and Previous.
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

// CheckCommit validates v as a shard's committed wire version: in the window and not below cur; failures are ErrUnsupportedVersion.
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

// MaxBatchSize is the maximum cumulative byte size of a deserialized MessageBatch (64MB).
const MaxBatchSize uint64 = 64 * 1024 * 1024 // 64MB

// MaxDecodeMemoryFactor is the maximum heap one decode may allocate per wire byte; the transport reserves frame length times this, measured by TestDecodeMemoryAmplification.
const MaxDecodeMemoryFactor = 8

// MaxTerm and MaxLogIndex are exclusive bounds on terms and indexes adopted from peers, snapshots or persisted state; persisted state uses a higher ceiling.
const (
	MaxTerm     uint64 = math.MaxUint64 - 1024
	MaxLogIndex uint64 = math.MaxUint64 - 1024
)

// MaxAddressLength is the maximum byte length of a node address string
// in membership changes and config change proposals. Enforced during
// UnmarshalFrom to prevent unbounded allocation from crafted payloads.
const MaxAddressLength = 256

// MaxNodeIdentityLength is the maximum byte length of a committed node identity.
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

// MaxMessagesPerBatch is the maximum messages in one MessageBatch (ErrTooManyMessages); producers must respect it.
const MaxMessagesPerBatch = 10000

// MaxEntriesPerMessage is the maximum entries in one Message (ErrTooManyMessages); producers must bound the count.
const MaxEntriesPerMessage = 10000

// MaxMembershipEntries is the maximum entries in one Membership map, checked on read and write.
const MaxMembershipEntries = 10000

// Entry types occupy the low 4 bits of Entry.Type; mask with EntryTypeMask before comparing.
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
	// LeaderExec carries a leader-executed request to the presumed leader's host handler, answered with LeaderExecResp; never reaches raft.
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
	// UpdateWireVersion commits a shard's switch to ConfigChange.WireVersion; apply refuses a lower or out-of-window value.
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

// IsFreeOrderMessage reports whether a message type may be sent before state is persisted (thesis 10.2.1): Replicate, Ping, Heartbeat, Quiesce and pre-vote.
func IsFreeOrderMessage(msgType uint64) bool {
	return msgType == Replicate || msgType == Ping ||
		msgType == Heartbeat || msgType == Quiesce ||
		msgType == RequestPreVote || msgType == RequestPreVoteResp
}

// IsFreeOrder is IsFreeOrderMessage for a message, also treating a rejecting ReadIndexResp as free-order.
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
