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
	"errors"
	"strings"
	"testing"
)

// allShardHealthValues is every ShardHealth the type defines. A round-trip
// test that enumerates a subset silently stops covering whatever is added
// next, which is how ShardRecovering and ShardSnapshotting reached the type
// with no JSON coverage at all.
var allShardHealthValues = []ShardHealth{
	ShardHealthy,
	ShardNoLeader,
	ShardLagging,
	ShardUnloaded,
	ShardError,
	ShardRecovering,
	ShardSnapshotting,
}

// TestShardHealth_JSONRoundTrip pins that MarshalJSON and UnmarshalJSON agree
// for EVERY value the type defines, and that the name tables are complete in
// both directions.
func TestShardHealth_JSONRoundTrip(t *testing.T) {
	if len(shardHealthNames) != len(allShardHealthValues) ||
		len(shardHealthValues) != len(allShardHealthValues) {
		t.Fatalf("name tables hold %d/%d entries, want %d: a value was added without its JSON mapping",
			len(shardHealthNames), len(shardHealthValues), len(allShardHealthValues))
	}
	for _, want := range allShardHealthValues {
		data, err := json.Marshal(want)
		if err != nil {
			t.Fatalf("MarshalJSON(%d): %v", want, err)
		}
		if string(data) == `""` {
			t.Fatalf("MarshalJSON(%d) produced an empty name; the value has no shardHealthNames entry", want)
		}
		var got ShardHealth
		if err := json.Unmarshal(data, &got); err != nil {
			t.Fatalf("UnmarshalJSON(%s): %v", data, err)
		}
		if got != want {
			t.Errorf("round trip of %d through %s = %d, want %d", want, data, got, want)
		}
	}
}

// TestShardHealth_UnmarshalJSONFailsClosed pins the direction an unrecognized
// health value must fail in.
//
// ShardHealthy is the ZERO value, so decoding an unknown string to it is a
// fail-open on a health signal: a typo in a monitoring payload, a truncated
// field, or a state a newer binary knows and this one does not all read back as
// "this shard is fine". The decode must refuse and leave the receiver alone.
func TestShardHealth_UnmarshalJSONFailsClosed(t *testing.T) {
	for _, s := range []string{"", "healty", "HEALTHY", "unknown-value", "0"} {
		data, err := json.Marshal(s)
		if err != nil {
			t.Fatalf("marshal fixture %q: %v", s, err)
		}
		h := ShardError
		err = h.UnmarshalJSON(data)
		if !errors.Is(err, ErrInvalidShardHealth) {
			t.Errorf("UnmarshalJSON(%s) = %v, want ErrInvalidShardHealth", data, err)
		}
		var invalid *InvalidShardHealthError
		if !errors.As(err, &invalid) || invalid.Value != s {
			t.Errorf("UnmarshalJSON(%s) error does not carry the offending value %q: %v", data, s, err)
		}
		if h != ShardError {
			t.Errorf("UnmarshalJSON(%s) overwrote the receiver with %v; a refused decode must not assign", data, h)
		}
	}
}

// TestShardInfo_HealthFieldFailsClosed pins that the refusal survives at the
// level a monitoring tool actually decodes: a whole ShardInfo document. Without
// this, a per-value test can pass while the enclosing struct swallows the
// error and hands back a ShardInfo reporting a healthy shard.
func TestShardInfo_HealthFieldFailsClosed(t *testing.T) {
	var info ShardInfo
	err := json.Unmarshal([]byte(`{"shard_id":7,"health":"healty"}`), &info)
	if !errors.Is(err, ErrInvalidShardHealth) {
		t.Fatalf("decoding a ShardInfo with a misspelled health = %v, want ErrInvalidShardHealth", err)
	}
	if info.Health != ShardHealthy {
		t.Fatalf("Health = %v, want the untouched zero value", info.Health)
	}
}

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
		{ShardRecovering, "recovering"},
		{ShardSnapshotting, "snapshotting"},
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
// ShardInfo ConfigChangeIndex
// ---------------------------------------------------------------------------

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
		{`"recovering"`, ShardRecovering},
		{`"snapshotting"`, ShardSnapshotting},
		// An unknown name is NOT in this table: it is refused, not decoded.
		// See TestShardHealth_UnmarshalJSONFailsClosed.
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
		{ShardRecovering, `"recovering"`},
		{ShardSnapshotting, `"snapshotting"`},
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
