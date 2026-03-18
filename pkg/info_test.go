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

package quicraft

import (
	"encoding/json"
	"strings"
	"testing"
)

// ---------------------------------------------------------------------------
// ShardHealth constants
// ---------------------------------------------------------------------------

func TestShardHealth_Values(t *testing.T) {
	tests := []struct {
		health ShardHealth
		want   ShardHealth
	}{
		{ShardHealthy, 0},
		{ShardNoLeader, 1},
		{ShardLagging, 2},
		{ShardUnloaded, 3},
		{ShardError, 4},
	}

	for _, tc := range tests {
		if tc.health != tc.want {
			t.Errorf("ShardHealth %v = %d, want %d", tc.health, tc.health, tc.want)
		}
	}
}

func TestShardHealth_Distinct(t *testing.T) {
	values := []ShardHealth{
		ShardHealthy, ShardNoLeader, ShardLagging, ShardUnloaded, ShardError,
	}
	seen := make(map[ShardHealth]bool)
	for _, v := range values {
		if seen[v] {
			t.Errorf("duplicate ShardHealth value: %d", v)
		}
		seen[v] = true
	}
}

func TestShardHealth_String(t *testing.T) {
	tests := []struct {
		health ShardHealth
		want   string
	}{
		{ShardHealthy, "healthy"},
		{ShardNoLeader, "no-leader"},
		{ShardLagging, "lagging"},
		{ShardUnloaded, "unloaded"},
		{ShardError, "error"},
	}

	for _, tc := range tests {
		got := tc.health.String()
		if got != tc.want {
			t.Errorf("ShardHealth(%d).String() = %q, want %q", tc.health, got, tc.want)
		}
	}
}

func TestShardHealth_String_Unknown(t *testing.T) {
	unknown := ShardHealth(255)
	got := unknown.String()
	// Unknown values return empty string from the map.
	if got != "" {
		t.Errorf("ShardHealth(255).String() = %q, want empty string", got)
	}
}

// ---------------------------------------------------------------------------
// ShardInfo construction
// ---------------------------------------------------------------------------

func TestShardInfo_Construction(t *testing.T) {
	info := &ShardInfo{
		ShardID:           1,
		ReplicaID:         2,
		LeaderID:          3,
		Term:              10,
		IsLeader:          true,
		IsObserver:        false,
		IsWitness:         false,
		StateMachineType:  0,
		Pending:           5,
		Nodes:             map[uint64]string{1: "a", 2: "b", 3: "c"},
		Health:            ShardHealthy,
		LastApplied:       100,
		ConfigChangeIndex: 42,
	}

	if info.ShardID != 1 {
		t.Errorf("ShardID = %d, want 1", info.ShardID)
	}
	if info.ReplicaID != 2 {
		t.Errorf("ReplicaID = %d, want 2", info.ReplicaID)
	}
	if info.LeaderID != 3 {
		t.Errorf("LeaderID = %d, want 3", info.LeaderID)
	}
	if info.Term != 10 {
		t.Errorf("Term = %d, want 10", info.Term)
	}
	if !info.IsLeader {
		t.Error("IsLeader should be true")
	}
	if info.IsObserver {
		t.Error("IsObserver should be false")
	}
	if info.IsWitness {
		t.Error("IsWitness should be false")
	}
	if info.StateMachineType != 0 {
		t.Errorf("StateMachineType = %d, want 0", info.StateMachineType)
	}
	if info.Pending != 5 {
		t.Errorf("Pending = %d, want 5", info.Pending)
	}
	if len(info.Nodes) != 3 {
		t.Errorf("Nodes length = %d, want 3", len(info.Nodes))
	}
	if info.Health != ShardHealthy {
		t.Errorf("Health = %v, want ShardHealthy", info.Health)
	}
	if info.LastApplied != 100 {
		t.Errorf("LastApplied = %d, want 100", info.LastApplied)
	}
	if info.ConfigChangeIndex != 42 {
		t.Errorf("ConfigChangeIndex = %d, want 42", info.ConfigChangeIndex)
	}
}

func TestShardInfo_ZeroValue(t *testing.T) {
	info := &ShardInfo{}
	if info.ShardID != 0 {
		t.Errorf("ShardID = %d, want 0", info.ShardID)
	}
	if info.Health != ShardHealthy {
		t.Errorf("Health = %v, want ShardHealthy (zero value)", info.Health)
	}
	if info.Nodes != nil {
		t.Error("Nodes should be nil for zero value")
	}
	if info.ConfigChangeIndex != 0 {
		t.Errorf("ConfigChangeIndex = %d, want 0 for zero value", info.ConfigChangeIndex)
	}
}

// ---------------------------------------------------------------------------
// ShardInfo ConfigChangeIndex
// ---------------------------------------------------------------------------

func TestShardInfo_ConfigChangeIndex(t *testing.T) {
	info := &ShardInfo{
		ShardID:           1,
		ReplicaID:         1,
		ConfigChangeIndex: 500,
	}

	if info.ConfigChangeIndex != 500 {
		t.Errorf("ConfigChangeIndex = %d, want 500", info.ConfigChangeIndex)
	}
}

func TestShardInfo_ConfigChangeIndex_ZeroDefault(t *testing.T) {
	// ConfigChangeIndex should be 0 when no config change has been applied.
	info := &ShardInfo{
		ShardID:   1,
		ReplicaID: 1,
	}

	if info.ConfigChangeIndex != 0 {
		t.Errorf("ConfigChangeIndex = %d, want 0 when not set", info.ConfigChangeIndex)
	}
}

func TestShardInfo_ConfigChangeIndex_JSONRoundTrip(t *testing.T) {
	info := ShardInfo{
		ShardID:           1,
		ReplicaID:         2,
		ConfigChangeIndex: 777,
	}

	data, err := json.Marshal(info)
	if err != nil {
		t.Fatalf("Marshal error: %v", err)
	}

	var got ShardInfo
	if err := json.Unmarshal(data, &got); err != nil {
		t.Fatalf("Unmarshal error: %v", err)
	}

	if got.ConfigChangeIndex != 777 {
		t.Errorf("ConfigChangeIndex round-trip = %d, want 777", got.ConfigChangeIndex)
	}
}

func TestShardInfo_ConfigChangeIndex_JSONKey(t *testing.T) {
	info := ShardInfo{
		ShardID:           1,
		ConfigChangeIndex: 123,
	}

	data, err := json.Marshal(info)
	if err != nil {
		t.Fatalf("Marshal error: %v", err)
	}

	s := string(data)
	if !strings.Contains(s, `"config_change_index"`) {
		t.Errorf("JSON should contain config_change_index key, got: %s", s)
	}
	if !strings.Contains(s, `"config_change_index":123`) {
		t.Errorf("JSON should contain config_change_index:123, got: %s", s)
	}
}

// ---------------------------------------------------------------------------
// NodeHostInfoOption
// ---------------------------------------------------------------------------

func TestNodeHostInfoOption_Default(t *testing.T) {
	opt := NodeHostInfoOption{}
	if opt.SkipLogInfo {
		t.Error("SkipLogInfo should default to false")
	}
}

func TestNodeHostInfoOption_SkipLogInfo(t *testing.T) {
	opt := NodeHostInfoOption{SkipLogInfo: true}
	if !opt.SkipLogInfo {
		t.Error("SkipLogInfo should be true when set")
	}
}

// ---------------------------------------------------------------------------
// NodeHostInfo construction
// ---------------------------------------------------------------------------

func TestNodeHostInfo_Construction(t *testing.T) {
	info := &NodeHostInfo{
		RaftAddress: "10.0.0.1:5000",
		ShardInfoList: []ShardInfo{
			{ShardID: 1, Health: ShardHealthy},
			{ShardID: 2, Health: ShardNoLeader},
		},
	}

	if info.RaftAddress != "10.0.0.1:5000" {
		t.Errorf("RaftAddress = %q, want %q", info.RaftAddress, "10.0.0.1:5000")
	}
	if len(info.ShardInfoList) != 2 {
		t.Errorf("ShardInfoList length = %d, want 2", len(info.ShardInfoList))
	}
	if info.ShardInfoList[0].ShardID != 1 {
		t.Errorf("ShardInfoList[0].ShardID = %d, want 1", info.ShardInfoList[0].ShardID)
	}
	if info.ShardInfoList[1].Health != ShardNoLeader {
		t.Errorf("ShardInfoList[1].Health = %v, want ShardNoLeader", info.ShardInfoList[1].Health)
	}
}

func TestNodeHostInfo_ZeroValue(t *testing.T) {
	info := &NodeHostInfo{}
	if info.RaftAddress != "" {
		t.Errorf("RaftAddress = %q, want empty", info.RaftAddress)
	}
	if info.ShardInfoList != nil {
		t.Error("ShardInfoList should be nil for zero value")
	}
	if info.LogInfo != nil {
		t.Error("LogInfo should be nil for zero value")
	}
}

// ---------------------------------------------------------------------------
// JSON serialization
// ---------------------------------------------------------------------------

func TestShardHealth_UnmarshalJSON(t *testing.T) {
	tests := []struct {
		input string
		want  ShardHealth
	}{
		{`"healthy"`, ShardHealthy},
		{`"no-leader"`, ShardNoLeader},
		{`"lagging"`, ShardLagging},
		{`"unloaded"`, ShardUnloaded},
		{`"error"`, ShardError},
		{`"unknown"`, ShardHealthy}, // unknown defaults to healthy
	}

	for _, tc := range tests {
		var got ShardHealth
		if err := json.Unmarshal([]byte(tc.input), &got); err != nil {
			t.Fatalf("UnmarshalJSON(%s) error: %v", tc.input, err)
		}
		if got != tc.want {
			t.Errorf("UnmarshalJSON(%s) = %v, want %v", tc.input, got, tc.want)
		}
	}
}

func TestShardHealth_JSONRoundTrip(t *testing.T) {
	for _, h := range []ShardHealth{ShardHealthy, ShardNoLeader, ShardLagging, ShardUnloaded, ShardError} {
		data, err := json.Marshal(h)
		if err != nil {
			t.Fatalf("Marshal(%v) error: %v", h, err)
		}
		var got ShardHealth
		if err := json.Unmarshal(data, &got); err != nil {
			t.Fatalf("Unmarshal(%s) error: %v", data, err)
		}
		if got != h {
			t.Errorf("round-trip: got %v, want %v", got, h)
		}
	}
}

func TestShardHealth_MarshalJSON(t *testing.T) {
	tests := []struct {
		health ShardHealth
		want   string
	}{
		{ShardHealthy, `"healthy"`},
		{ShardNoLeader, `"no-leader"`},
		{ShardLagging, `"lagging"`},
		{ShardUnloaded, `"unloaded"`},
		{ShardError, `"error"`},
	}

	for _, tc := range tests {
		got, err := json.Marshal(tc.health)
		if err != nil {
			t.Fatalf("MarshalJSON(%v) error: %v", tc.health, err)
		}
		if string(got) != tc.want {
			t.Errorf("MarshalJSON(%v) = %s, want %s", tc.health, got, tc.want)
		}
	}
}

func TestShardInfo_JSON(t *testing.T) {
	info := ShardInfo{
		ShardID:           1,
		ReplicaID:         2,
		LeaderID:          3,
		Term:              10,
		IsLeader:          true,
		Health:            ShardHealthy,
		LastApplied:       42,
		ConfigChangeIndex: 99,
		Nodes:             map[uint64]string{1: "127.0.0.1:63001"},
	}

	data, err := json.Marshal(info)
	if err != nil {
		t.Fatalf("Marshal error: %v", err)
	}

	s := string(data)
	for _, key := range []string{
		`"shard_id"`, `"replica_id"`, `"leader_id"`, `"term"`,
		`"is_leader"`, `"health"`, `"last_applied"`, `"nodes"`,
		`"config_change_index"`,
	} {
		if !strings.Contains(s, key) {
			t.Errorf("JSON missing key %s, got: %s", key, s)
		}
	}

	// Health should be serialized as a string.
	if !strings.Contains(s, `"healthy"`) {
		t.Errorf("expected health as string, got: %s", s)
	}
}

func TestNodeHostInfo_JSON(t *testing.T) {
	info := NodeHostInfo{
		RaftAddress: "10.0.0.1:5000",
		ShardInfoList: []ShardInfo{
			{ShardID: 1, Health: ShardHealthy},
		},
	}

	data, err := json.Marshal(info)
	if err != nil {
		t.Fatalf("Marshal error: %v", err)
	}

	s := string(data)
	if !strings.Contains(s, `"raft_address"`) {
		t.Errorf("JSON missing raft_address, got: %s", s)
	}
	if !strings.Contains(s, `"shard_info_list"`) {
		t.Errorf("JSON missing shard_info_list, got: %s", s)
	}
	// LogInfo omitted when nil.
	if strings.Contains(s, `"log_info"`) {
		t.Errorf("log_info should be omitted when nil, got: %s", s)
	}
}
