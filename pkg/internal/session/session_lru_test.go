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
	"bytes"
	"testing"

	"github.com/jeremyhahn/go-quicraft/pkg/sm"
)

// ---------------------------------------------------------------------------
// Session LRU: eviction order by lastActiveIndex (deterministic via clientID sort)
// ---------------------------------------------------------------------------

func TestSessionLRU_EvictionOrder(t *testing.T) {
	t.Run("sessions with lowest lastActiveIndex expire first", func(t *testing.T) {
		m := NewManager(100, 10, 0)

		// Register sessions with different activity levels.
		// Session 30 is most stale, session 10 most recent.
		if err := m.Register(10, 0, 900); err != nil {
			t.Fatalf("Register(10) = %v", err)
		}
		if err := m.Register(20, 0, 500); err != nil {
			t.Fatalf("Register(20) = %v", err)
		}
		if err := m.Register(30, 0, 100); err != nil {
			t.Fatalf("Register(30) = %v", err)
		}

		// At index 1200 with threshold 200:
		// Session 10 (last=900): gap=300 > 200, expired
		// Session 20 (last=500): gap=700 > 200, expired
		// Session 30 (last=100): gap=1100 > 200, expired
		// With limit=1, the lowest clientID (10) is selected first.
		expired := m.ExpireSessions(1200, 200, 1)
		if len(expired) != 1 {
			t.Fatalf("len(expired) = %d, want 1", len(expired))
		}
		if expired[0] != 10 {
			t.Errorf("expired[0] = %d, want 10 (lowest clientID)", expired[0])
		}

		// Second call with limit=1 gets the next lowest.
		expired2 := m.ExpireSessions(1200, 200, 1)
		if len(expired2) != 1 {
			t.Fatalf("len(expired2) = %d, want 1", len(expired2))
		}
		if expired2[0] != 20 {
			t.Errorf("expired2[0] = %d, want 20", expired2[0])
		}

		// Third call gets the last one.
		expired3 := m.ExpireSessions(1200, 200, 1)
		if len(expired3) != 1 {
			t.Fatalf("len(expired3) = %d, want 1", len(expired3))
		}
		if expired3[0] != 30 {
			t.Errorf("expired3[0] = %d, want 30", expired3[0])
		}

		if m.Count() != 0 {
			t.Errorf("Count() = %d, want 0", m.Count())
		}
	})

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

// ---------------------------------------------------------------------------
// Session LRU: ordering preservation after save/restore
// ---------------------------------------------------------------------------

func TestSessionLRU_OrderingPreservedAfterRestore(t *testing.T) {
	t.Run("expiry order is deterministic after marshal-unmarshal", func(t *testing.T) {
		m1 := NewManager(100, 10, 0)

		// Register sessions with varying activity.
		if err := m1.Register(10, 0, 0); err != nil {
			t.Fatalf("Register(10) = %v", err)
		}
		if err := m1.Register(20, 0, 0); err != nil {
			t.Fatalf("Register(20) = %v", err)
		}
		if err := m1.Register(30, 0, 0); err != nil {
			t.Fatalf("Register(30) = %v", err)
		}

		// Touch session 20 at index 500.
		if err := m1.RecordResult(20, 1, 0, 500, sm.Result{Value: 1}); err != nil {
			t.Fatalf("RecordResult(20) = %v", err)
		}

		// Marshal.
		var buf bytes.Buffer
		if err := m1.MarshalTo(&buf); err != nil {
			t.Fatalf("MarshalTo = %v", err)
		}

		// Unmarshal into new manager.
		m2 := NewManager(100, 10, 0)
		if err := m2.UnmarshalFrom(bytes.NewReader(buf.Bytes())); err != nil {
			t.Fatalf("UnmarshalFrom = %v", err)
		}

		// At index 600 with threshold 200:
		// Session 10 (last=0): gap=600 > 200, expired
		// Session 20 (last=500): gap=100 < 200, survives
		// Session 30 (last=0): gap=600 > 200, expired
		expired1 := m1.ExpireSessions(600, 200, 10)
		expired2 := m2.ExpireSessions(600, 200, 10)

		if len(expired1) != len(expired2) {
			t.Fatalf("expiry count differs: original=%d, restored=%d",
				len(expired1), len(expired2))
		}
		for i := range expired1 {
			if expired1[i] != expired2[i] {
				t.Errorf("expired[%d] differs: original=%d, restored=%d",
					i, expired1[i], expired2[i])
			}
		}

		// Session 20 should survive in both.
		if !m1.IsRegistered(20) {
			t.Error("m1: session 20 should survive")
		}
		if !m2.IsRegistered(20) {
			t.Error("m2: session 20 should survive")
		}
	})

}

// ---------------------------------------------------------------------------
// Session LRU: session mutability and isolation
// ---------------------------------------------------------------------------

func TestSessionLRU_MutabilityAndIsolation(t *testing.T) {
	t.Run("unregistering one session does not affect another", func(t *testing.T) {
		m := NewManager(100, 10, 0)
		if err := m.Register(10, 0, 0); err != nil {
			t.Fatalf("Register(10) = %v", err)
		}
		if err := m.Register(20, 0, 0); err != nil {
			t.Fatalf("Register(20) = %v", err)
		}

		if err := m.Unregister(10); err != nil {
			t.Fatalf("Unregister(10) = %v", err)
		}

		if m.IsRegistered(10) {
			t.Error("session 10 should be unregistered")
		}
		if !m.IsRegistered(20) {
			t.Error("session 20 should still be registered")
		}
		if m.Count() != 1 {
			t.Errorf("Count() = %d, want 1", m.Count())
		}
	})
}
