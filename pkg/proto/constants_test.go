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

package proto

import "testing"

func TestMessageTypeName(t *testing.T) {
	tests := []struct {
		input uint64
		want  string
	}{
		{LocalTick, "LocalTick"},
		{Election, "Election"},
		{LeaderHeartbeat, "LeaderHeartbeat"},
		{ConfigChangeEvent, "ConfigChangeEvent"},
		{NoOP, "NoOP"},
		{Ping, "Ping"},
		{Pong, "Pong"},
		{Propose, "Propose"},
		{SnapshotStatus, "SnapshotStatus"},
		{Unreachable, "Unreachable"},
		{CheckQuorum, "CheckQuorum"},
		{BatchedReadIndex, "BatchedReadIndex"},
		{Replicate, "Replicate"},
		{ReplicateResp, "ReplicateResp"},
		{RequestVote, "RequestVote"},
		{RequestVoteResp, "RequestVoteResp"},
		{InstallSnapshot, "InstallSnapshot"},
		{Heartbeat, "Heartbeat"},
		{HeartbeatResp, "HeartbeatResp"},
		{ReadIndex, "ReadIndex"},
		{ReadIndexResp, "ReadIndexResp"},
		{Quiesce, "Quiesce"},
		{SnapshotReceived, "SnapshotReceived"},
		{LeaderTransfer, "LeaderTransfer"},
		{TimeoutNow, "TimeoutNow"},
		{RequestPreVote, "RequestPreVote"},
		{RequestPreVoteResp, "RequestPreVoteResp"},
		{RateLimit, "RateLimit"},
	}
	for _, tt := range tests {
		t.Run(tt.want, func(t *testing.T) {
			got := MessageTypeName(tt.input)
			if got != tt.want {
				t.Errorf("MessageTypeName(%d) = %q, want %q", tt.input, got, tt.want)
			}
		})
	}
}

func TestMessageTypeName_Unknown(t *testing.T) {
	got := MessageTypeName(9999)
	if got != "Unknown(9999)" {
		t.Errorf("MessageTypeName(9999) = %q, want %q", got, "Unknown(9999)")
	}
}

func TestConfigChangeTypeName(t *testing.T) {
	tests := []struct {
		input uint64
		want  string
	}{
		{AddNode, "AddNode"},
		{RemoveNode, "RemoveNode"},
		{AddNonVoting, "AddNonVoting"},
		{AddWitness, "AddWitness"},
	}
	for _, tt := range tests {
		t.Run(tt.want, func(t *testing.T) {
			got := ConfigChangeTypeName(tt.input)
			if got != tt.want {
				t.Errorf("ConfigChangeTypeName(%d) = %q, want %q", tt.input, got, tt.want)
			}
		})
	}
}

func TestConfigChangeTypeName_Unknown(t *testing.T) {
	got := ConfigChangeTypeName(999)
	if got != "Unknown(999)" {
		t.Errorf("ConfigChangeTypeName(999) = %q, want %q", got, "Unknown(999)")
	}
}

// TestEntryMetadataConstant verifies the EntryMetadata constant value is
// distinct from other entry type constants and fits within EntryTypeMask.
func TestEntryMetadataConstant(t *testing.T) {
	if EntryMetadata != 3 {
		t.Fatalf("EntryMetadata = %d, want 3", EntryMetadata)
	}
	// Must not collide with other entry types.
	if EntryMetadata == EntryNormal {
		t.Fatal("EntryMetadata must differ from EntryNormal")
	}
	if EntryMetadata == EntryConfigChange {
		t.Fatal("EntryMetadata must differ from EntryConfigChange")
	}
	if EntryMetadata == EntryEncodedEntry {
		t.Fatal("EntryMetadata must differ from EntryEncodedEntry")
	}
	// Must fit within the 4-bit EntryTypeMask.
	if EntryMetadata&EntryTypeMask != EntryMetadata {
		t.Fatalf("EntryMetadata (0x%x) does not fit within EntryTypeMask (0x%x)",
			EntryMetadata, EntryTypeMask)
	}
}

// TestEntryMetadataConstant_InvalidValue verifies that values above the
// entry type range are rejected by the mask.
func TestEntryMetadataConstant_InvalidValue(t *testing.T) {
	// A value outside the 4-bit range should not equal EntryMetadata after masking.
	invalid := EntryMetadata | 0x10 // set bit 4
	if invalid&EntryTypeMask != EntryMetadata {
		t.Fatalf("expected masked invalid (0x%x) to equal EntryMetadata, got 0x%x",
			invalid, invalid&EntryTypeMask)
	}
	// But the raw value should differ.
	if invalid == EntryMetadata {
		t.Fatal("invalid should differ from EntryMetadata before masking")
	}
}

// TestIsFreeOrderMessage verifies that only Replicate, Ping, Heartbeat,
// and Quiesce are classified as free-order messages per Raft PhD thesis
// section 10.2.1. Free-order messages may be sent before the
// corresponding state is persisted to stable storage.
func TestIsFreeOrderMessage(t *testing.T) {
	tests := []struct {
		name    string
		msgType uint64
		want    bool
	}{
		// Free-order messages: safe to send before WAL persist.
		{"Replicate", Replicate, true},
		{"Ping", Ping, true},
		{"Heartbeat", Heartbeat, true},
		{"Quiesce", Quiesce, true},
		// Ordered messages: MUST wait for WAL persist.
		{"LocalTick", LocalTick, false},
		{"Election", Election, false},
		{"LeaderHeartbeat", LeaderHeartbeat, false},
		{"ConfigChangeEvent", ConfigChangeEvent, false},
		{"NoOP", NoOP, false},
		{"Pong", Pong, false},
		{"Propose", Propose, false},
		{"SnapshotStatus", SnapshotStatus, false},
		{"Unreachable", Unreachable, false},
		{"CheckQuorum", CheckQuorum, false},
		{"BatchedReadIndex", BatchedReadIndex, false},
		{"ReplicateResp", ReplicateResp, false},
		{"RequestVote", RequestVote, false},
		{"RequestVoteResp", RequestVoteResp, false},
		{"InstallSnapshot", InstallSnapshot, false},
		{"HeartbeatResp", HeartbeatResp, false},
		{"ReadIndex", ReadIndex, false},
		{"ReadIndexResp", ReadIndexResp, false},
		{"SnapshotReceived", SnapshotReceived, false},
		{"LeaderTransfer", LeaderTransfer, false},
		{"TimeoutNow", TimeoutNow, false},
		{"RequestPreVote", RequestPreVote, false},
		{"RequestPreVoteResp", RequestPreVoteResp, false},
		{"RateLimit", RateLimit, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := IsFreeOrderMessage(tt.msgType)
			if got != tt.want {
				t.Errorf("IsFreeOrderMessage(%d) = %v, want %v",
					tt.msgType, got, tt.want)
			}
		})
	}
}

// TestIsFreeOrderMessage_UnknownType verifies that unknown message
// types are classified as ordered (not free-order), providing a safe
// default for any future message types.
func TestIsFreeOrderMessage_UnknownType(t *testing.T) {
	unknownTypes := []uint64{100, 255, 9999, 1<<64 - 1}
	for _, msgType := range unknownTypes {
		if IsFreeOrderMessage(msgType) {
			t.Errorf("IsFreeOrderMessage(%d) = true, want false for unknown type", msgType)
		}
	}
}

// TestStateMachineTypeName verifies that all known state machine type
// codes return their expected human-readable names.
func TestStateMachineTypeName(t *testing.T) {
	tests := []struct {
		smType uint64
		want   string
	}{
		{RegularStateMachine, "regular"},
		{ConcurrentStateMachine, "concurrent"},
		{OnDiskStateMachine, "on-disk"},
	}
	for _, tt := range tests {
		got := StateMachineTypeName(tt.smType)
		if got != tt.want {
			t.Errorf("StateMachineTypeName(%d) = %q, want %q", tt.smType, got, tt.want)
		}
	}
}

// TestStateMachineTypeName_Unknown verifies that unrecognized type
// codes return "unknown" as a safe fallback.
func TestStateMachineTypeName_Unknown(t *testing.T) {
	unknownTypes := []uint64{3, 99, 255, 1<<64 - 1}
	for _, smType := range unknownTypes {
		got := StateMachineTypeName(smType)
		if got != "unknown" {
			t.Errorf("StateMachineTypeName(%d) = %q, want %q", smType, got, "unknown")
		}
	}
}
