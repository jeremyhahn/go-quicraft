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

package session

import (
	"testing"

	"github.com/jeremyhahn/go-quicraft/pkg/sm"
)

// ---------------------------------------------------------------------------
// Session LRU: eviction order by lastActiveIndex (deterministic via clientID sort)
// ---------------------------------------------------------------------------

func TestSessionLRU_EvictionOrder(t *testing.T) {
	t.Run("recently touched sessions survive eviction of stale ones", func(t *testing.T) {
		m := NewManager(100, 10, 0)

		// Register 5 sessions at index 0.
		for id := uint64(1); id <= 5; id++ {
			if err := m.Register(id, 0, 0); err != nil {
				t.Fatalf("Register(%d) = %v", id, err)
			}
		}

		// Touch sessions 3 and 5 at index 900.
		if err := m.RecordResult(3, 1, 0, 900, sm.Result{Value: 1}); err != nil {
			t.Fatalf("RecordResult(3) = %v", err)
		}
		if err := m.RecordResult(5, 1, 0, 900, sm.Result{Value: 1}); err != nil {
			t.Fatalf("RecordResult(5) = %v", err)
		}

		// At index 1000 with threshold 500:
		// Sessions 1,2,4 (last=0): gap=1000 > 500, expired
		// Sessions 3,5 (last=900): gap=100 < 500, survive
		expired := m.ExpireSessions(1000, 500, 10)
		if len(expired) != 3 {
			t.Fatalf("len(expired) = %d, want 3", len(expired))
		}

		// Verify the expired are [1,2,4].
		expectedExpired := map[uint64]bool{1: true, 2: true, 4: true}
		for _, id := range expired {
			if !expectedExpired[id] {
				t.Errorf("unexpected expired session %d", id)
			}
		}

		// Sessions 3 and 5 should survive.
		if !m.IsRegistered(3) {
			t.Error("session 3 should survive (recently touched)")
		}
		if !m.IsRegistered(5) {
			t.Error("session 5 should survive (recently touched)")
		}
		if m.Count() != 2 {
			t.Errorf("Count() = %d, want 2", m.Count())
		}
	})
}
