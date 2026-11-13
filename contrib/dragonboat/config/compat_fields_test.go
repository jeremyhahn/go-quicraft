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
	"context"
	"errors"
	"reflect"
	"testing"
)

// compatBaseConfig returns a valid NodeHostConfig with none of the
// compatibility-only fields set and the mapped Expert.Engine fields set,
// so a compatibility-only field cannot hide behind a zero mapped value.
func compatBaseConfig(ctx context.Context, t *testing.T) NodeHostConfig {
	t.Helper()
	if err := ctx.Err(); err != nil {
		t.Fatalf("context: %v", err)
	}
	return NodeHostConfig{
		NodeHostID:     "nh-compat",
		DeploymentID:   7,
		WALDir:         "/tmp/compat/wal",
		NodeHostDir:    "/tmp/compat/nh",
		RTTMillisecond: 200,
		RaftAddress:    "localhost:63001",
		ListenAddress:  "0.0.0.0:63001",
		NotifyCommit:   true,
		Expert: ExpertConfig{
			Engine: EngineConfig{ExecShards: 8, ApplyShards: 4},
		},
	}
}

// TestCompatFields_ToQuicraftIgnoresCompatibilityOnlyFields checks that
// every field documented as accepted for dragonboat compatibility and
// ignored leaves the ToQuicraft result unchanged and passes Validate.
func TestCompatFields_ToQuicraftIgnoresCompatibilityOnlyFields(t *testing.T) {
	ctx := context.Background()
	base := compatBaseConfig(ctx, t)
	want, err := base.ToQuicraft()
	if err != nil {
		t.Fatalf("base ToQuicraft: %v", err)
	}

	tests := []struct {
		name string
		set  func(*NodeHostConfig)
	}{
		{"EnableMetrics", func(c *NodeHostConfig) { c.EnableMetrics = true }},
		{"MaxSendQueueSize", func(c *NodeHostConfig) { c.MaxSendQueueSize = 1 << 20 }},
		{"MaxReceiveQueueSize", func(c *NodeHostConfig) { c.MaxReceiveQueueSize = 1 << 20 }},
		{"DefaultNodeRegistryEnabled", func(c *NodeHostConfig) { c.DefaultNodeRegistryEnabled = true }},
		{"ExpertEngineCommitShards", func(c *NodeHostConfig) { c.Expert.Engine.CommitShards = 16 }},
		{"ExpertEngineSnapshotShards", func(c *NodeHostConfig) { c.Expert.Engine.SnapshotShards = 48 }},
		{"ExpertEngineCloseShards", func(c *NodeHostConfig) { c.Expert.Engine.CloseShards = 32 }},
		{"ExpertLogDBShards", func(c *NodeHostConfig) { c.Expert.LogDB.Shards = 16 }},
		{"All", func(c *NodeHostConfig) {
			c.EnableMetrics = true
			c.MaxSendQueueSize = 1 << 20
			c.MaxReceiveQueueSize = 1 << 20
			c.DefaultNodeRegistryEnabled = true
			c.Expert.Engine.CommitShards = 16
			c.Expert.Engine.SnapshotShards = 48
			c.Expert.Engine.CloseShards = 32
			c.Expert.LogDB.Shards = 16
		}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			cfg := compatBaseConfig(ctx, t)
			tc.set(&cfg)
			if err := cfg.Validate(); err != nil {
				t.Fatalf("Validate: %v", err)
			}
			got, err := cfg.ToQuicraft()
			if err != nil {
				t.Fatalf("ToQuicraft: %v", err)
			}
			if !reflect.DeepEqual(got, want) {
				t.Errorf("ToQuicraft with %s set = %+v, want %+v", tc.name, got, want)
			}
		})
	}
}

// TestCompatFields_ValidateRefusesAnyGossipField checks that Validate
// refuses a GossipConfig with any single field set, Meta included, with
// a ValidationError naming Gossip.
func TestCompatFields_ValidateRefusesAnyGossipField(t *testing.T) {
	ctx := context.Background()
	tests := []struct {
		name   string
		gossip GossipConfig
	}{
		{"BindAddress", GossipConfig{BindAddress: "0.0.0.0:7946"}},
		{"AdvertiseAddress", GossipConfig{AdvertiseAddress: "10.0.0.1:7946"}},
		{"Seed", GossipConfig{Seed: []string{"10.0.0.2:7946"}}},
		{"Meta", GossipConfig{Meta: []byte("meta")}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			cfg := compatBaseConfig(ctx, t)
			cfg.Gossip = tc.gossip
			err := cfg.Validate()
			var ve *ValidationError
			if !errors.As(err, &ve) {
				t.Fatalf("Validate with Gossip.%s set = %v, want *ValidationError", tc.name, err)
			}
			if ve.Field != "Gossip" {
				t.Errorf("ValidationError.Field = %q, want %q", ve.Field, "Gossip")
			}
		})
	}
}
