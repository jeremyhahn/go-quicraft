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

import "testing"

// TestConfig_SetDefaults_MaxSessionsPerOwner pins the two documented values
// of Config.MaxSessionsPerOwner. An unset field cannot mean both "I did not
// choose" and "no limit", so zero selects DefaultMaxSessionsPerOwner and
// SessionsPerOwnerUnlimited is the explicit opt-out. A SetDefaults that
// treated the opt-out as unset, or a default that drifted to zero, would
// otherwise pass every test.
func TestConfig_SetDefaults_MaxSessionsPerOwner(t *testing.T) {
	t.Run("an unset field takes the default", func(t *testing.T) {
		c := Config{ShardID: 1, ReplicaID: 1}
		c.SetDefaults()
		if c.MaxSessionsPerOwner != DefaultMaxSessionsPerOwner {
			t.Fatalf("SetDefaults left MaxSessionsPerOwner = %d, want "+
				"DefaultMaxSessionsPerOwner (%d): an unset per-owner cap must "+
				"select the default, not mean no limit",
				c.MaxSessionsPerOwner, DefaultMaxSessionsPerOwner)
		}
		if err := c.Validate(); err != nil {
			t.Fatalf("Validate() of a default config = %v, want nil", err)
		}
	})

	t.Run("the default is a real cap below the shard total", func(t *testing.T) {
		if DefaultMaxSessionsPerOwner == 0 {
			t.Fatal("DefaultMaxSessionsPerOwner is 0: zero is the unset value " +
				"SetDefaults replaces, so the default cannot be it")
		}
		if DefaultMaxSessionsPerOwner == SessionsPerOwnerUnlimited {
			t.Fatal("DefaultMaxSessionsPerOwner is SessionsPerOwnerUnlimited: " +
				"the default must cap an owner")
		}
		if DefaultMaxSessionsPerOwner >= DefaultMaxTotalSessions {
			t.Fatalf("DefaultMaxSessionsPerOwner (%d) must be below "+
				"DefaultMaxTotalSessions (%d), or one owner can fill a "+
				"default shard and the default cap bounds nothing",
				DefaultMaxSessionsPerOwner, DefaultMaxTotalSessions)
		}
	})

	kept := []struct {
		name  string
		value uint64
	}{
		{"SessionsPerOwnerUnlimited", SessionsPerOwnerUnlimited},
		{"an explicit 7", 7},
	}
	for _, tc := range kept {
		t.Run(tc.name+" survives SetDefaults and validates", func(t *testing.T) {
			c := Config{ShardID: 1, ReplicaID: 1, MaxSessionsPerOwner: tc.value}
			c.SetDefaults()
			if c.MaxSessionsPerOwner != tc.value {
				t.Fatalf("SetDefaults changed MaxSessionsPerOwner from %d to %d: "+
					"an explicit value must be kept", tc.value, c.MaxSessionsPerOwner)
			}
			if err := c.Validate(); err != nil {
				t.Fatalf("Validate() with MaxSessionsPerOwner = %d: %v, want nil",
					tc.value, err)
			}
		})
	}
}
