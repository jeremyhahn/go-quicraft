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
	"math"
	"testing"
)

// The three numeric ceilings in Validate had NO test: no occurrence of
// MaxUint32, 4294967295, MaxInt32 or 2147483647 existed in any test in this
// package, so all three guards could be deleted with the suite staying green.
//
// They are also the three highest-consequence rules in the file by their own
// comments: an ElectionRTT at or above 2^63 narrows to a NEGATIVE int and
// PANICS StartShard, and a ReplicaID above 2^32 silently truncates the
// proposal-key packing so an applied entry can complete another replica's
// unrelated pending request.
//
// Each is pinned at the boundary in both directions, so an off-by-one in
// either guard fails too.

func TestConfig_Validate_ReplicaIDCeiling(t *testing.T) {
	t.Run("at the uint32 maximum is accepted", func(t *testing.T) {
		c := validConfig()
		c.ReplicaID = math.MaxUint32
		if err := c.Validate(); err != nil {
			t.Errorf("Validate() = %v, want nil at the boundary", err)
		}
	})
	t.Run("above the uint32 maximum is refused", func(t *testing.T) {
		c := validConfig()
		c.ReplicaID = uint64(math.MaxUint32) + 1
		assertValidationError(t, c.Validate(), "ReplicaID")
	})
}

func TestConfig_Validate_TickCeilings(t *testing.T) {
	t.Run("HeartbeatRTT above MaxInt32 is refused", func(t *testing.T) {
		c := validConfig()
		c.HeartbeatRTT = uint64(math.MaxInt32) + 1
		// ElectionRTT must stay above HeartbeatRTT or the ratio rule fires
		// first and the test would pin the wrong guard.
		c.ElectionRTT = math.MaxUint64
		assertValidationError(t, c.Validate(), "HeartbeatRTT")
	})
	t.Run("ElectionRTT above MaxInt32 is refused", func(t *testing.T) {
		c := validConfig()
		c.HeartbeatRTT = 1
		c.ElectionRTT = uint64(math.MaxInt32) + 1
		assertValidationError(t, c.Validate(), "ElectionRTT")
	})
	t.Run("at MaxInt32 both are accepted", func(t *testing.T) {
		c := validConfig()
		c.HeartbeatRTT = 2
		c.ElectionRTT = math.MaxInt32
		if err := c.Validate(); err != nil {
			t.Errorf("Validate() = %v, want nil at the boundary", err)
		}
	})
}

// TestConfig_Validate_ElectionHeartbeatMargin closes the band the existing
// tests skip. They cover ratio <= 1 (5/5 and 5/6) and a valid 5/1, so
// weakening the rule from `ElectionRTT/2 < HeartbeatRTT` to
// `ElectionRTT <= HeartbeatRTT` passed cleanly — while the 1x..2x band is
// exactly the configuration the production comment names as harmful, because
// an election timeout under two heartbeat intervals produces election storms.
func TestConfig_Validate_ElectionHeartbeatMargin(t *testing.T) {
	t.Run("ratio above 1x but below 2x is refused", func(t *testing.T) {
		c := validConfig()
		c.HeartbeatRTT = 4
		c.ElectionRTT = 5 // ratio 1.25 — the case the comment names
		assertValidationError(t, c.Validate(), "ElectionRTT")
	})
	t.Run("ratio of exactly 2x is the accepted minimum", func(t *testing.T) {
		// The rule refuses when ElectionRTT/2 < HeartbeatRTT, so 10/5 gives
		// 5 < 5 = false and is accepted: 2x is the smallest permitted margin.
		// Pinning both sides of that boundary is what makes an off-by-one in
		// either direction visible.
		c := validConfig()
		c.HeartbeatRTT = 5
		c.ElectionRTT = 10
		if err := c.Validate(); err != nil {
			t.Errorf("Validate() = %v at exactly 2x, want nil", err)
		}
	})
	t.Run("just under 2x is refused", func(t *testing.T) {
		c := validConfig()
		c.HeartbeatRTT = 5
		c.ElectionRTT = 9 // 9/2 = 4 < 5
		assertValidationError(t, c.Validate(), "ElectionRTT")
	})
	t.Run("comfortably above 2x is accepted", func(t *testing.T) {
		c := validConfig()
		c.HeartbeatRTT = 5
		c.ElectionRTT = 11
		if err := c.Validate(); err != nil {
			t.Errorf("Validate() = %v, want nil", err)
		}
	})
	t.Run("a huge HeartbeatRTT does not wrap", func(t *testing.T) {
		// The rule divides ElectionRTT rather than doubling HeartbeatRTT
		// precisely so 2*HeartbeatRTT cannot overflow uint64. That choice is
		// untested otherwise.
		c := validConfig()
		c.HeartbeatRTT = math.MaxInt32
		c.ElectionRTT = 10
		assertValidationError(t, c.Validate(), "ElectionRTT")
	})
}
