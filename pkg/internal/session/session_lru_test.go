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
		m := NewManager(100, 10)

		// Register sessions with different activity levels.
		// Session 30 is most stale, session 10 most recent.
		if err := m.Register(10, 900); err != nil {
			t.Fatalf("Register(10) = %v", err)
		}
		if err := m.Register(20, 500); err != nil {
			t.Fatalf("Register(20) = %v", err)
		}
		if err := m.Register(30, 100); err != nil {
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
		m := NewManager(100, 10)

		// Register 5 sessions at index 0.
		for id := uint64(1); id <= 5; id++ {
			if err := m.Register(id, 0); err != nil {
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
		m1 := NewManager(100, 10)

		// Register sessions with varying activity.
		if err := m1.Register(10, 0); err != nil {
			t.Fatalf("Register(10) = %v", err)
		}
		if err := m1.Register(20, 0); err != nil {
			t.Fatalf("Register(20) = %v", err)
		}
		if err := m1.Register(30, 0); err != nil {
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
		m2 := NewManager(100, 10)
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

	t.Run("response cache preserved after save-restore enables dedup", func(t *testing.T) {
		m1 := NewManager(100, 10)
		if err := m1.Register(10, 0); err != nil {
			t.Fatalf("Register = %v", err)
		}
		if err := m1.RecordResult(10, 5, 0, 100, sm.Result{Value: 42, Data: []byte("cached-data")}); err != nil {
			t.Fatalf("RecordResult = %v", err)
		}

		// Marshal-unmarshal.
		var buf bytes.Buffer
		if err := m1.MarshalTo(&buf); err != nil {
			t.Fatalf("MarshalTo = %v", err)
		}
		m2 := NewManager(100, 10)
		if err := m2.UnmarshalFrom(bytes.NewReader(buf.Bytes())); err != nil {
			t.Fatalf("UnmarshalFrom = %v", err)
		}

		// Check duplicate on restored manager.
		cached, isDup, err := m2.CheckDuplicate(10, 5, 200)
		if err != nil {
			t.Fatalf("CheckDuplicate = %v", err)
		}
		if !isDup {
			t.Error("series 5 should be duplicate after restore")
		}
		if cached.Value != 42 {
			t.Errorf("cached Value = %d, want 42", cached.Value)
		}
		if !bytes.Equal(cached.Data, []byte("cached-data")) {
			t.Errorf("cached Data = %v, want %v", cached.Data, []byte("cached-data"))
		}
	})
}

// ---------------------------------------------------------------------------
// Session LRU: session mutability and isolation
// ---------------------------------------------------------------------------

func TestSessionLRU_MutabilityAndIsolation(t *testing.T) {
	t.Run("recording result for one session does not affect another", func(t *testing.T) {
		m := NewManager(100, 10)
		if err := m.Register(10, 0); err != nil {
			t.Fatalf("Register(10) = %v", err)
		}
		if err := m.Register(20, 0); err != nil {
			t.Fatalf("Register(20) = %v", err)
		}

		// Record result for session 10.
		if err := m.RecordResult(10, 1, 0, 100, sm.Result{Value: 100}); err != nil {
			t.Fatalf("RecordResult(10) = %v", err)
		}

		// Session 20 should have no cached responses.
		_, isDup, err := m.CheckDuplicate(20, 1, 200)
		if err != nil {
			t.Fatalf("CheckDuplicate(20) = %v", err)
		}
		if isDup {
			t.Error("session 20 should not have cached result from session 10")
		}

		// Session 10 should have the cached response.
		cached, isDup10, err10 := m.CheckDuplicate(10, 1, 300)
		if err10 != nil {
			t.Fatalf("CheckDuplicate(10) = %v", err10)
		}
		if !isDup10 {
			t.Error("session 10 should have cached result")
		}
		if cached.Value != 100 {
			t.Errorf("cached Value = %d, want 100", cached.Value)
		}
	})

	t.Run("unregistering one session does not affect another", func(t *testing.T) {
		m := NewManager(100, 10)
		if err := m.Register(10, 0); err != nil {
			t.Fatalf("Register(10) = %v", err)
		}
		if err := m.Register(20, 0); err != nil {
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

	t.Run("respondedTo advancement for one session does not affect another", func(t *testing.T) {
		m := NewManager(100, 10)
		if err := m.Register(10, 0); err != nil {
			t.Fatalf("Register(10) = %v", err)
		}
		if err := m.Register(20, 0); err != nil {
			t.Fatalf("Register(20) = %v", err)
		}

		// Record results for both sessions at the same seriesID.
		if err := m.RecordResult(10, 5, 0, 100, sm.Result{Value: 10}); err != nil {
			t.Fatalf("RecordResult(10) = %v", err)
		}
		if err := m.RecordResult(20, 5, 0, 100, sm.Result{Value: 20}); err != nil {
			t.Fatalf("RecordResult(20) = %v", err)
		}

		// Advance respondedTo for session 10 to 5 by recording series 6.
		if err := m.RecordResult(10, 6, 5, 200, sm.Result{Value: 60}); err != nil {
			t.Fatalf("RecordResult(10, series 6) = %v", err)
		}

		// Session 10, series 5 should be evicted.
		_, isDup10, err10 := m.CheckDuplicate(10, 5, 300)
		if err10 != ErrResponseEvicted {
			t.Errorf("session 10 series 5: err = %v, want ErrResponseEvicted", err10)
		}
		if isDup10 {
			t.Error("session 10 series 5 should not be a duplicate (evicted)")
		}

		// Session 20, series 5 should still be cached (independent).
		cached20, isDup20, err20 := m.CheckDuplicate(20, 5, 300)
		if err20 != nil {
			t.Fatalf("session 20 series 5: err = %v", err20)
		}
		if !isDup20 {
			t.Error("session 20 series 5 should still be a duplicate")
		}
		if cached20.Value != 20 {
			t.Errorf("cached Value = %d, want 20", cached20.Value)
		}
	})
}

// ---------------------------------------------------------------------------
// Session LRU: response eviction boundary conditions
// ---------------------------------------------------------------------------

func TestSessionLRU_ResponseEvictionBoundary(t *testing.T) {
	t.Run("evictUpTo at exact boundary evicts matching seriesID", func(t *testing.T) {
		m := NewManager(100, 10)
		if err := m.Register(10, 0); err != nil {
			t.Fatalf("Register = %v", err)
		}

		// Record series 1, 2, 3.
		for sid := uint64(1); sid <= 3; sid++ {
			if err := m.RecordResult(10, sid, 0, uint64(sid*100), sm.Result{Value: sid}); err != nil {
				t.Fatalf("RecordResult(%d) = %v", sid, err)
			}
		}

		// Record series 4 with respondedTo=2 (evicts 1, 2).
		if err := m.RecordResult(10, 4, 2, 400, sm.Result{Value: 4}); err != nil {
			t.Fatalf("RecordResult(4) = %v", err)
		}

		// Series 1 and 2 should be evicted.
		for _, sid := range []uint64{1, 2} {
			_, isDup, err := m.CheckDuplicate(10, sid, 500)
			if err != ErrResponseEvicted {
				t.Errorf("series %d: err = %v, want ErrResponseEvicted", sid, err)
			}
			if isDup {
				t.Errorf("series %d should not be duplicate", sid)
			}
		}

		// Series 3 and 4 should still be cached.
		for _, sid := range []uint64{3, 4} {
			_, isDup, err := m.CheckDuplicate(10, sid, 500)
			if err != nil {
				t.Fatalf("series %d: err = %v", sid, err)
			}
			if !isDup {
				t.Errorf("series %d should be duplicate", sid)
			}
		}
	})

	t.Run("response limit with eviction allows continued operation", func(t *testing.T) {
		// maxResponses = 2.
		m := NewManager(100, 2)
		if err := m.Register(10, 0); err != nil {
			t.Fatalf("Register = %v", err)
		}

		// Fill cache to limit.
		if err := m.RecordResult(10, 1, 0, 100, sm.Result{Value: 1}); err != nil {
			t.Fatalf("RecordResult(1) = %v", err)
		}
		if err := m.RecordResult(10, 2, 0, 200, sm.Result{Value: 2}); err != nil {
			t.Fatalf("RecordResult(2) = %v", err)
		}

		// Third without eviction should fail.
		err := m.RecordResult(10, 3, 0, 300, sm.Result{Value: 3})
		if err != ErrResponseLimitExceeded {
			t.Fatalf("RecordResult(3) = %v, want ErrResponseLimitExceeded", err)
		}

		// Third with respondedTo=1 (evicts series 1) should succeed.
		if err := m.RecordResult(10, 3, 1, 300, sm.Result{Value: 3}); err != nil {
			t.Fatalf("RecordResult(3, respondedTo=1) = %v", err)
		}

		// Now {2, 3} should be cached. Fourth with respondedTo=2 evicts series 2.
		if err := m.RecordResult(10, 4, 2, 400, sm.Result{Value: 4}); err != nil {
			t.Fatalf("RecordResult(4, respondedTo=2) = %v", err)
		}

		// Only {3, 4} should be cached.
		cached3, isDup3, _ := m.CheckDuplicate(10, 3, 500)
		if !isDup3 {
			t.Error("series 3 should be cached")
		}
		if cached3.Value != 3 {
			t.Errorf("series 3 Value = %d, want 3", cached3.Value)
		}

		cached4, isDup4, _ := m.CheckDuplicate(10, 4, 500)
		if !isDup4 {
			t.Error("series 4 should be cached")
		}
		if cached4.Value != 4 {
			t.Errorf("series 4 Value = %d, want 4", cached4.Value)
		}
	})
}
