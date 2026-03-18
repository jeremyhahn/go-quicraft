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
	"encoding/binary"
	"errors"
	"io"
	"testing"

	"github.com/jeremyhahn/go-quicraft/pkg/sm"
)

func TestNewManager(t *testing.T) {
	t.Run("custom limits", func(t *testing.T) {
		m := NewManager(100, 50)
		if m.maxTotal != 100 {
			t.Errorf("maxTotal = %d, want 100", m.maxTotal)
		}
		if m.maxResponses != 50 {
			t.Errorf("maxResponses = %d, want 50", m.maxResponses)
		}
		if m.Count() != 0 {
			t.Errorf("Count() = %d, want 0", m.Count())
		}
	})

	t.Run("defaults when zero", func(t *testing.T) {
		m := NewManager(0, 0)
		if m.maxTotal != 16384 {
			t.Errorf("maxTotal = %d, want 16384", m.maxTotal)
		}
		if m.maxResponses != 128 {
			t.Errorf("maxResponses = %d, want 128", m.maxResponses)
		}
	})
}

func TestManagerRegister(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		m := NewManager(10, 10)
		if err := m.Register(1, 100); err != nil {
			t.Fatalf("Register(1) = %v, want nil", err)
		}
		if !m.IsRegistered(1) {
			t.Error("IsRegistered(1) = false, want true")
		}
		if m.Count() != 1 {
			t.Errorf("Count() = %d, want 1", m.Count())
		}
	})

	t.Run("already exists", func(t *testing.T) {
		m := NewManager(10, 10)
		if err := m.Register(1, 100); err != nil {
			t.Fatalf("Register(1) = %v, want nil", err)
		}
		err := m.Register(1, 101)
		if !errors.Is(err, ErrSessionAlreadyExists) {
			t.Errorf("Register(1) = %v, want ErrSessionAlreadyExists", err)
		}
	})

	t.Run("limit exceeded", func(t *testing.T) {
		m := NewManager(2, 10)
		if err := m.Register(1, 100); err != nil {
			t.Fatalf("Register(1) = %v", err)
		}
		if err := m.Register(2, 101); err != nil {
			t.Fatalf("Register(2) = %v", err)
		}
		err := m.Register(3, 102)
		if !errors.Is(err, ErrSessionLimitExceeded) {
			t.Errorf("Register(3) = %v, want ErrSessionLimitExceeded", err)
		}
	})

	t.Run("sets initial lastActiveIndex", func(t *testing.T) {
		m := NewManager(10, 10)
		if err := m.Register(1, 500); err != nil {
			t.Fatalf("Register = %v", err)
		}
		sess := m.sessions[1]
		if sess.lastActiveIndex != 500 {
			t.Errorf("lastActiveIndex = %d, want 500", sess.lastActiveIndex)
		}
	})
}

func TestManagerUnregister(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		m := NewManager(10, 10)
		if err := m.Register(1, 100); err != nil {
			t.Fatalf("Register(1) = %v", err)
		}
		if err := m.Unregister(1); err != nil {
			t.Errorf("Unregister(1) = %v, want nil", err)
		}
		if m.IsRegistered(1) {
			t.Error("IsRegistered(1) = true after unregister, want false")
		}
		if m.Count() != 0 {
			t.Errorf("Count() = %d, want 0", m.Count())
		}
	})

	t.Run("not found", func(t *testing.T) {
		m := NewManager(10, 10)
		err := m.Unregister(999)
		if !errors.Is(err, ErrSessionNotFound) {
			t.Errorf("Unregister(999) = %v, want ErrSessionNotFound", err)
		}
	})
}

func TestManagerIsRegistered(t *testing.T) {
	t.Run("registered", func(t *testing.T) {
		m := NewManager(10, 10)
		if err := m.Register(42, 100); err != nil {
			t.Fatalf("Register = %v", err)
		}
		if !m.IsRegistered(42) {
			t.Error("IsRegistered(42) = false, want true")
		}
	})

	t.Run("not registered", func(t *testing.T) {
		m := NewManager(10, 10)
		if m.IsRegistered(42) {
			t.Error("IsRegistered(42) = true, want false")
		}
	})
}

func TestManagerCheckDuplicate(t *testing.T) {
	t.Run("new proposal not duplicate", func(t *testing.T) {
		m := NewManager(10, 10)
		if err := m.Register(1, 100); err != nil {
			t.Fatalf("Register = %v", err)
		}
		result, isDup, err := m.CheckDuplicate(1, 5, 101)
		if err != nil {
			t.Fatalf("CheckDuplicate = %v", err)
		}
		if isDup {
			t.Error("CheckDuplicate returned duplicate for new proposal")
		}
		if result.Value != 0 {
			t.Errorf("result.Value = %d, want 0", result.Value)
		}
	})

	t.Run("duplicate detected", func(t *testing.T) {
		m := NewManager(10, 10)
		if err := m.Register(1, 100); err != nil {
			t.Fatalf("Register = %v", err)
		}
		expected := sm.Result{Value: 42, Data: []byte("data")}
		if err := m.RecordResult(1, 5, 0, 101, expected); err != nil {
			t.Fatalf("RecordResult = %v", err)
		}

		result, isDup, err := m.CheckDuplicate(1, 5, 102)
		if err != nil {
			t.Fatalf("CheckDuplicate = %v", err)
		}
		if !isDup {
			t.Error("CheckDuplicate returned not-duplicate for recorded result")
		}
		if result.Value != expected.Value {
			t.Errorf("result.Value = %d, want %d", result.Value, expected.Value)
		}
		if !bytes.Equal(result.Data, expected.Data) {
			t.Errorf("result.Data = %v, want %v", result.Data, expected.Data)
		}
	})

	t.Run("unregistered client returns ErrSessionNotFound", func(t *testing.T) {
		m := NewManager(10, 10)
		_, isDup, err := m.CheckDuplicate(999, 1, 100)
		if !errors.Is(err, ErrSessionNotFound) {
			t.Errorf("CheckDuplicate(unregistered) err = %v, want ErrSessionNotFound", err)
		}
		if isDup {
			t.Error("CheckDuplicate returned duplicate for unregistered client")
		}
	})

	t.Run("series already responded to returns ErrResponseEvicted", func(t *testing.T) {
		m := NewManager(10, 10)
		if err := m.Register(1, 100); err != nil {
			t.Fatalf("Register = %v", err)
		}
		// Record result for series 5, advance respondedTo to 5.
		if err := m.RecordResult(1, 5, 0, 101, sm.Result{Value: 10}); err != nil {
			t.Fatalf("RecordResult(5) = %v", err)
		}
		if err := m.RecordResult(1, 6, 5, 102, sm.Result{Value: 20}); err != nil {
			t.Fatalf("RecordResult(6) = %v", err)
		}

		// Series 3 is <= respondedTo (5), should return ErrResponseEvicted.
		_, isDup, err := m.CheckDuplicate(1, 3, 103)
		if !errors.Is(err, ErrResponseEvicted) {
			t.Errorf("CheckDuplicate(evicted series) err = %v, want ErrResponseEvicted", err)
		}
		if isDup {
			t.Error("CheckDuplicate returned duplicate for series <= respondedTo")
		}
	})

	t.Run("series equal to respondedTo returns ErrResponseEvicted", func(t *testing.T) {
		m := NewManager(10, 10)
		if err := m.Register(1, 100); err != nil {
			t.Fatalf("Register = %v", err)
		}
		if err := m.RecordResult(1, 5, 0, 101, sm.Result{Value: 10}); err != nil {
			t.Fatalf("RecordResult(5) = %v", err)
		}
		if err := m.RecordResult(1, 6, 5, 102, sm.Result{Value: 20}); err != nil {
			t.Fatalf("RecordResult(6) = %v", err)
		}

		// Series 5 == respondedTo (5), should return ErrResponseEvicted.
		_, isDup, err := m.CheckDuplicate(1, 5, 103)
		if !errors.Is(err, ErrResponseEvicted) {
			t.Errorf("CheckDuplicate(series == respondedTo) err = %v, want ErrResponseEvicted", err)
		}
		if isDup {
			t.Error("CheckDuplicate returned duplicate for series == respondedTo")
		}
	})

	t.Run("updates lastActiveIndex on duplicate hit", func(t *testing.T) {
		m := NewManager(10, 10)
		if err := m.Register(1, 100); err != nil {
			t.Fatalf("Register = %v", err)
		}
		if err := m.RecordResult(1, 5, 0, 101, sm.Result{Value: 42}); err != nil {
			t.Fatalf("RecordResult = %v", err)
		}
		_, _, _ = m.CheckDuplicate(1, 5, 500)
		if m.sessions[1].lastActiveIndex != 500 {
			t.Errorf("lastActiveIndex = %d, want 500", m.sessions[1].lastActiveIndex)
		}
	})
}

func TestManagerRecordResult(t *testing.T) {
	t.Run("record and retrieve", func(t *testing.T) {
		m := NewManager(10, 10)
		if err := m.Register(1, 100); err != nil {
			t.Fatalf("Register = %v", err)
		}
		expected := sm.Result{Value: 100, Data: []byte("result-data")}
		if err := m.RecordResult(1, 10, 0, 101, expected); err != nil {
			t.Fatalf("RecordResult = %v", err)
		}

		result, isDup, err := m.CheckDuplicate(1, 10, 102)
		if err != nil {
			t.Fatalf("CheckDuplicate = %v", err)
		}
		if !isDup {
			t.Error("CheckDuplicate returned not-duplicate after RecordResult")
		}
		if result.Value != expected.Value {
			t.Errorf("result.Value = %d, want %d", result.Value, expected.Value)
		}
	})

	t.Run("eviction via respondedTo", func(t *testing.T) {
		m := NewManager(10, 10)
		if err := m.Register(1, 100); err != nil {
			t.Fatalf("Register = %v", err)
		}
		if err := m.RecordResult(1, 1, 0, 101, sm.Result{Value: 1}); err != nil {
			t.Fatalf("RecordResult(1) = %v", err)
		}
		if err := m.RecordResult(1, 2, 0, 102, sm.Result{Value: 2}); err != nil {
			t.Fatalf("RecordResult(2) = %v", err)
		}
		if err := m.RecordResult(1, 3, 0, 103, sm.Result{Value: 3}); err != nil {
			t.Fatalf("RecordResult(3) = %v", err)
		}

		// Advance respondedTo to 2 by recording series 4.
		if err := m.RecordResult(1, 4, 2, 104, sm.Result{Value: 4}); err != nil {
			t.Fatalf("RecordResult(4) = %v", err)
		}

		// Series 1 and 2 are <= respondedTo (2), should return ErrResponseEvicted.
		_, isDup1, err1 := m.CheckDuplicate(1, 1, 105)
		_, isDup2, err2 := m.CheckDuplicate(1, 2, 106)
		if !errors.Is(err1, ErrResponseEvicted) {
			t.Errorf("series 1: err = %v, want ErrResponseEvicted", err1)
		}
		if isDup1 {
			t.Error("series 1 should not be marked as duplicate")
		}
		if !errors.Is(err2, ErrResponseEvicted) {
			t.Errorf("series 2: err = %v, want ErrResponseEvicted", err2)
		}
		if isDup2 {
			t.Error("series 2 should not be marked as duplicate")
		}

		// Series 3 and 4 should still be there.
		_, isDup3, err3 := m.CheckDuplicate(1, 3, 107)
		_, isDup4, err4 := m.CheckDuplicate(1, 4, 108)
		if err3 != nil {
			t.Fatalf("series 3: err = %v", err3)
		}
		if !isDup3 {
			t.Error("series 3 should still exist")
		}
		if err4 != nil {
			t.Fatalf("series 4: err = %v", err4)
		}
		if !isDup4 {
			t.Error("series 4 should still exist")
		}
	})

	t.Run("unregistered client returns error", func(t *testing.T) {
		m := NewManager(10, 10)
		err := m.RecordResult(999, 1, 0, 100, sm.Result{Value: 1})
		if !errors.Is(err, ErrSessionNotFound) {
			t.Errorf("RecordResult(unregistered) = %v, want ErrSessionNotFound", err)
		}
		if m.Count() != 0 {
			t.Errorf("Count() = %d, want 0", m.Count())
		}
	})

	t.Run("response limit exceeded", func(t *testing.T) {
		// maxResponses = 3, fill to capacity then attempt one more.
		m := NewManager(10, 3)
		if err := m.Register(1, 100); err != nil {
			t.Fatalf("Register = %v", err)
		}
		// Fill the response cache to capacity (3 responses, respondedTo=0
		// so none are evicted).
		for i := uint64(1); i <= 3; i++ {
			if err := m.RecordResult(1, i, 0, 100+i, sm.Result{Value: i}); err != nil {
				t.Fatalf("RecordResult(%d) = %v", i, err)
			}
		}
		// The 4th should exceed the limit.
		err := m.RecordResult(1, 4, 0, 104, sm.Result{Value: 4})
		if !errors.Is(err, ErrResponseLimitExceeded) {
			t.Errorf("RecordResult(4) = %v, want ErrResponseLimitExceeded", err)
		}
		// Verify the 4th was not stored.
		_, isDup, checkErr := m.CheckDuplicate(1, 4, 105)
		if checkErr != nil {
			t.Fatalf("CheckDuplicate = %v", checkErr)
		}
		if isDup {
			t.Error("series 4 should not be stored after limit exceeded")
		}
	})

	t.Run("response limit not exceeded after eviction", func(t *testing.T) {
		// maxResponses = 3. Fill to capacity, then advance respondedTo
		// to evict old entries, making room for new ones.
		m := NewManager(10, 3)
		if err := m.Register(1, 100); err != nil {
			t.Fatalf("Register = %v", err)
		}
		for i := uint64(1); i <= 3; i++ {
			if err := m.RecordResult(1, i, 0, 100+i, sm.Result{Value: i}); err != nil {
				t.Fatalf("RecordResult(%d) = %v", i, err)
			}
		}
		// Record series 4 with respondedTo=2, which evicts series 1 and 2.
		// After eviction: responses={3, 4} (2 entries), under limit of 3.
		if err := m.RecordResult(1, 4, 2, 104, sm.Result{Value: 4}); err != nil {
			t.Fatalf("RecordResult(4, respondedTo=2) = %v", err)
		}
		// Series 4 should be stored.
		result, isDup, checkErr := m.CheckDuplicate(1, 4, 105)
		if checkErr != nil {
			t.Fatalf("CheckDuplicate = %v", checkErr)
		}
		if !isDup {
			t.Error("series 4 should be stored after eviction made room")
		}
		if result.Value != 4 {
			t.Errorf("result.Value = %d, want 4", result.Value)
		}
	})

	t.Run("response limit at boundary", func(t *testing.T) {
		// maxResponses = 1. Only one response can be cached at a time.
		m := NewManager(10, 1)
		if err := m.Register(1, 100); err != nil {
			t.Fatalf("Register = %v", err)
		}
		if err := m.RecordResult(1, 1, 0, 101, sm.Result{Value: 1}); err != nil {
			t.Fatalf("RecordResult(1) = %v", err)
		}
		// Second without eviction should fail.
		err := m.RecordResult(1, 2, 0, 102, sm.Result{Value: 2})
		if !errors.Is(err, ErrResponseLimitExceeded) {
			t.Errorf("RecordResult(2) = %v, want ErrResponseLimitExceeded", err)
		}
		// Advance respondedTo to evict series 1.
		if err := m.RecordResult(1, 2, 1, 103, sm.Result{Value: 2}); err != nil {
			t.Fatalf("RecordResult(2, respondedTo=1) = %v", err)
		}
	})

	t.Run("updates lastActiveIndex", func(t *testing.T) {
		m := NewManager(10, 10)
		if err := m.Register(1, 100); err != nil {
			t.Fatalf("Register = %v", err)
		}
		if err := m.RecordResult(1, 1, 0, 500, sm.Result{Value: 1}); err != nil {
			t.Fatalf("RecordResult = %v", err)
		}
		if m.sessions[1].lastActiveIndex != 500 {
			t.Errorf("lastActiveIndex = %d, want 500", m.sessions[1].lastActiveIndex)
		}
	})
}

func TestManagerExpireSessions(t *testing.T) {
	t.Run("expires inactive sessions", func(t *testing.T) {
		m := NewManager(10, 10)
		// Register sessions at low indexes.
		if err := m.Register(1, 100); err != nil {
			t.Fatalf("Register = %v", err)
		}
		if err := m.Register(2, 100); err != nil {
			t.Fatalf("Register = %v", err)
		}

		// Current index is far ahead, threshold is 1000.
		expired := m.ExpireSessions(2000, 1000, 10)
		if len(expired) != 2 {
			t.Errorf("len(expired) = %d, want 2", len(expired))
		}
		if m.Count() != 0 {
			t.Errorf("Count() = %d, want 0", m.Count())
		}
	})

	t.Run("respects maxExpire limit", func(t *testing.T) {
		m := NewManager(10, 10)
		for i := uint64(1); i <= 5; i++ {
			if err := m.Register(i, 100); err != nil {
				t.Fatalf("Register(%d) = %v", i, err)
			}
		}

		expired := m.ExpireSessions(2000, 1000, 2)
		if len(expired) != 2 {
			t.Errorf("len(expired) = %d, want 2", len(expired))
		}
		if m.Count() != 3 {
			t.Errorf("Count() = %d, want 3", m.Count())
		}
		// The two lowest clientIDs must be selected after sorting.
		if len(expired) == 2 {
			if expired[0] != 1 || expired[1] != 2 {
				t.Errorf("expired = %v, want [1, 2] (lowest clientIDs)", expired)
			}
		}
	})

	t.Run("does not expire active sessions", func(t *testing.T) {
		m := NewManager(10, 10)
		if err := m.Register(1, 900); err != nil {
			t.Fatalf("Register = %v", err)
		}
		// Current index 1000, threshold 1000. Gap is only 100, < 1000.
		expired := m.ExpireSessions(1000, 1000, 10)
		if len(expired) != 0 {
			t.Errorf("len(expired) = %d, want 0", len(expired))
		}
	})

	t.Run("maxExpire zero returns nil", func(t *testing.T) {
		m := NewManager(10, 10)
		if err := m.Register(1, 100); err != nil {
			t.Fatalf("Register = %v", err)
		}
		expired := m.ExpireSessions(2000, 1000, 0)
		if expired != nil {
			t.Errorf("expired = %v, want nil", expired)
		}
	})

	t.Run("negative maxExpire returns nil", func(t *testing.T) {
		m := NewManager(10, 10)
		expired := m.ExpireSessions(2000, 1000, -1)
		if expired != nil {
			t.Errorf("expired = %v, want nil", expired)
		}
	})

	t.Run("zero threshold returns nil", func(t *testing.T) {
		m := NewManager(10, 10)
		if err := m.Register(1, 100); err != nil {
			t.Fatalf("Register = %v", err)
		}
		expired := m.ExpireSessions(2000, 0, 10)
		if expired != nil {
			t.Errorf("expired = %v, want nil (disabled)", expired)
		}
	})

	t.Run("touch prevents expiry", func(t *testing.T) {
		m := NewManager(10, 10)
		if err := m.Register(1, 100); err != nil {
			t.Fatalf("Register = %v", err)
		}
		// Record a result at a high index to update activity.
		if err := m.RecordResult(1, 1, 0, 1900, sm.Result{Value: 1}); err != nil {
			t.Fatalf("RecordResult = %v", err)
		}
		// At index 2000 with threshold 1000, gap is only 100.
		expired := m.ExpireSessions(2000, 1000, 10)
		if len(expired) != 0 {
			t.Errorf("len(expired) = %d, want 0 (session was recently active)", len(expired))
		}
	})

	t.Run("deterministic across replicas", func(t *testing.T) {
		// Two managers simulating two replicas processing the same log.
		m1 := NewManager(10, 10)
		m2 := NewManager(10, 10)

		// Both register the same client at the same index.
		if err := m1.Register(1, 100); err != nil {
			t.Fatalf("m1.Register = %v", err)
		}
		if err := m2.Register(1, 100); err != nil {
			t.Fatalf("m2.Register = %v", err)
		}

		// Both expire at the same current index and threshold.
		expired1 := m1.ExpireSessions(2000, 1000, 10)
		expired2 := m2.ExpireSessions(2000, 1000, 10)

		if len(expired1) != len(expired2) {
			t.Errorf("replica divergence: m1 expired %d, m2 expired %d",
				len(expired1), len(expired2))
		}
	})

	t.Run("expired sessions sorted by clientID", func(t *testing.T) {
		// Register sessions with non-sequential clientIDs and verify
		// the returned slice is sorted in ascending clientID order.
		m := NewManager(100, 10)
		ids := []uint64{500, 100, 300, 200, 400}
		for _, id := range ids {
			if err := m.Register(id, 10); err != nil {
				t.Fatalf("Register(%d) = %v", id, err)
			}
		}

		expired := m.ExpireSessions(5000, 1000, 100)
		if len(expired) != 5 {
			t.Fatalf("len(expired) = %d, want 5", len(expired))
		}
		for i := 1; i < len(expired); i++ {
			if expired[i] <= expired[i-1] {
				t.Errorf("expired not sorted: expired[%d]=%d <= expired[%d]=%d",
					i, expired[i], i-1, expired[i-1])
			}
		}
		want := []uint64{100, 200, 300, 400, 500}
		for i, id := range want {
			if expired[i] != id {
				t.Errorf("expired[%d] = %d, want %d", i, expired[i], id)
			}
		}
	})

	t.Run("maxExpire selects same sessions regardless of map order", func(t *testing.T) {
		// Create many independent managers with identical state, each
		// calling ExpireSessions with a maxExpire smaller than the
		// number of expired sessions. All must produce the exact same
		// result, which proves the sort-then-limit approach is
		// deterministic despite Go's randomized map iteration.
		const numReplicas = 50
		const numSessions = 20
		const maxExpire = 5

		results := make([][]uint64, numReplicas)
		for r := 0; r < numReplicas; r++ {
			m := NewManager(100, 10)
			for id := uint64(1); id <= numSessions; id++ {
				if err := m.Register(id, 10); err != nil {
					t.Fatalf("replica %d: Register(%d) = %v", r, id, err)
				}
			}
			results[r] = m.ExpireSessions(5000, 1000, maxExpire)
			if len(results[r]) != maxExpire {
				t.Fatalf("replica %d: len(expired) = %d, want %d",
					r, len(results[r]), maxExpire)
			}
		}

		// All replicas must select the same clientIDs.
		expected := results[0]
		for r := 1; r < numReplicas; r++ {
			for i := 0; i < maxExpire; i++ {
				if results[r][i] != expected[i] {
					t.Errorf("replica %d diverged: expired[%d] = %d, want %d",
						r, i, results[r][i], expected[i])
				}
			}
		}

		// The selected sessions must be the lowest 5 clientIDs.
		for i, id := range expected {
			if id != uint64(i+1) {
				t.Errorf("expected[%d] = %d, want %d (lowest clientIDs)", i, id, i+1)
			}
		}
	})

	t.Run("repeated calls are deterministic with same state", func(t *testing.T) {
		// Build identical managers and call ExpireSessions multiple times
		// to confirm each invocation yields the same result.
		buildManager := func() *Manager {
			m := NewManager(100, 10)
			for id := uint64(10); id <= 60; id += 10 {
				if err := m.Register(id, 5); err != nil {
					t.Fatalf("Register(%d) = %v", id, err)
				}
			}
			return m
		}

		const iterations = 100
		var reference []uint64
		for i := 0; i < iterations; i++ {
			m := buildManager()
			expired := m.ExpireSessions(5000, 1000, 3)
			if i == 0 {
				reference = expired
				if len(reference) != 3 {
					t.Fatalf("len(expired) = %d, want 3", len(reference))
				}
				continue
			}
			if len(expired) != len(reference) {
				t.Fatalf("iteration %d: len(expired) = %d, want %d",
					i, len(expired), len(reference))
			}
			for j := range reference {
				if expired[j] != reference[j] {
					t.Errorf("iteration %d: expired[%d] = %d, want %d",
						i, j, expired[j], reference[j])
				}
			}
		}

		// Verify the reference is the three lowest clientIDs: 10, 20, 30.
		want := []uint64{10, 20, 30}
		for i, id := range want {
			if reference[i] != id {
				t.Errorf("reference[%d] = %d, want %d", i, reference[i], id)
			}
		}
	})
}

func TestManagerCount(t *testing.T) {
	t.Run("empty manager", func(t *testing.T) {
		m := NewManager(10, 10)
		if m.Count() != 0 {
			t.Errorf("Count() = %d, want 0", m.Count())
		}
	})

	t.Run("after registrations", func(t *testing.T) {
		m := NewManager(10, 10)
		for i := uint64(1); i <= 5; i++ {
			if err := m.Register(i, 100+i); err != nil {
				t.Fatalf("Register(%d) = %v", i, err)
			}
		}
		if m.Count() != 5 {
			t.Errorf("Count() = %d, want 5", m.Count())
		}
	})
}

func TestManagerMaxResponses(t *testing.T) {
	t.Run("returns configured value", func(t *testing.T) {
		m := NewManager(10, 64)
		if m.MaxResponses() != 64 {
			t.Errorf("MaxResponses() = %d, want 64", m.MaxResponses())
		}
	})

	t.Run("returns default value", func(t *testing.T) {
		m := NewManager(10, 0)
		if m.MaxResponses() != 128 {
			t.Errorf("MaxResponses() = %d, want 128", m.MaxResponses())
		}
	})
}

func TestManagerMarshalUnmarshal(t *testing.T) {
	t.Run("roundtrip with data", func(t *testing.T) {
		m1 := NewManager(100, 100)
		if err := m1.Register(1, 100); err != nil {
			t.Fatalf("Register(1) = %v", err)
		}
		if err := m1.Register(2, 101); err != nil {
			t.Fatalf("Register(2) = %v", err)
		}
		if err := m1.RecordResult(1, 10, 0, 102, sm.Result{Value: 100, Data: []byte("hello")}); err != nil {
			t.Fatalf("RecordResult = %v", err)
		}
		if err := m1.RecordResult(1, 11, 10, 103, sm.Result{Value: 200}); err != nil {
			t.Fatalf("RecordResult = %v", err)
		}
		if err := m1.RecordResult(2, 5, 0, 104, sm.Result{Value: 50, Data: []byte("world")}); err != nil {
			t.Fatalf("RecordResult = %v", err)
		}

		var buf bytes.Buffer
		if err := m1.MarshalTo(&buf); err != nil {
			t.Fatalf("MarshalTo = %v", err)
		}

		m2 := NewManager(100, 100)
		if err := m2.UnmarshalFrom(&buf); err != nil {
			t.Fatalf("UnmarshalFrom = %v", err)
		}

		// Verify session count.
		if m2.Count() != 2 {
			t.Errorf("Count() = %d, want 2", m2.Count())
		}

		// Verify session 1 results.
		result, isDup, checkErr := m2.CheckDuplicate(1, 11, 200)
		if checkErr != nil {
			t.Fatalf("CheckDuplicate(1, 11) = %v", checkErr)
		}
		if !isDup {
			t.Error("series 11 should be duplicate after recovery")
		}
		if result.Value != 200 {
			t.Errorf("result.Value = %d, want 200", result.Value)
		}

		// Series 10 should be evicted (respondedTo was advanced to 10).
		_, isDup10, err10 := m2.CheckDuplicate(1, 10, 201)
		if !errors.Is(err10, ErrResponseEvicted) {
			t.Errorf("series 10: err = %v, want ErrResponseEvicted", err10)
		}
		if isDup10 {
			t.Error("series 10 should be evicted after respondedTo advancement")
		}

		// Verify session 2 results.
		result2, isDup2, checkErr2 := m2.CheckDuplicate(2, 5, 202)
		if checkErr2 != nil {
			t.Fatalf("CheckDuplicate(2, 5) = %v", checkErr2)
		}
		if !isDup2 {
			t.Error("series 5 should be duplicate after recovery")
		}
		if result2.Value != 50 {
			t.Errorf("result2.Value = %d, want 50", result2.Value)
		}
		if !bytes.Equal(result2.Data, []byte("world")) {
			t.Errorf("result2.Data = %v, want %v", result2.Data, []byte("world"))
		}
	})

	t.Run("roundtrip empty manager", func(t *testing.T) {
		m1 := NewManager(100, 100)
		var buf bytes.Buffer
		if err := m1.MarshalTo(&buf); err != nil {
			t.Fatalf("MarshalTo = %v", err)
		}
		m2 := NewManager(100, 100)
		if err := m2.UnmarshalFrom(&buf); err != nil {
			t.Fatalf("UnmarshalFrom = %v", err)
		}
		if m2.Count() != 0 {
			t.Errorf("Count() = %d, want 0", m2.Count())
		}
	})

	t.Run("roundtrip preserves lastActiveIndex", func(t *testing.T) {
		m1 := NewManager(100, 100)
		if err := m1.Register(1, 500); err != nil {
			t.Fatalf("Register = %v", err)
		}
		if err := m1.RecordResult(1, 1, 0, 750, sm.Result{Value: 1}); err != nil {
			t.Fatalf("RecordResult = %v", err)
		}

		var buf bytes.Buffer
		if err := m1.MarshalTo(&buf); err != nil {
			t.Fatalf("MarshalTo = %v", err)
		}

		m2 := NewManager(100, 100)
		if err := m2.UnmarshalFrom(&buf); err != nil {
			t.Fatalf("UnmarshalFrom = %v", err)
		}

		// The recovered session should have lastActiveIndex=750.
		sess := m2.sessions[1]
		if sess.lastActiveIndex != 750 {
			t.Errorf("lastActiveIndex = %d, want 750", sess.lastActiveIndex)
		}

		// Should not be expired at index 800 with threshold 1000.
		expired := m2.ExpireSessions(800, 1000, 10)
		if len(expired) != 0 {
			t.Errorf("recovered session should not be expired at index 800")
		}

		// Should be expired at index 2000 with threshold 1000.
		expired = m2.ExpireSessions(2000, 1000, 10)
		if len(expired) != 1 {
			t.Errorf("recovered session should be expired at index 2000")
		}
	})

	t.Run("marshal error on write", func(t *testing.T) {
		m := NewManager(10, 10)
		if err := m.Register(1, 100); err != nil {
			t.Fatalf("Register = %v", err)
		}
		w := &failWriter{failAfter: 0}
		err := m.MarshalTo(w)
		if err == nil {
			t.Fatal("MarshalTo should fail on broken writer")
		}
		var marshalErr *MarshalError
		if !errors.As(err, &marshalErr) {
			t.Errorf("error should be *MarshalError, got %T", err)
		}
	})

	t.Run("unmarshal error on read", func(t *testing.T) {
		m := NewManager(10, 10)
		r := &failReader{}
		err := m.UnmarshalFrom(r)
		if err == nil {
			t.Fatal("UnmarshalFrom should fail on broken reader")
		}
		var unmarshalErr *UnmarshalError
		if !errors.As(err, &unmarshalErr) {
			t.Errorf("error should be *UnmarshalError, got %T", err)
		}
	})

	t.Run("unmarshal truncated clientID", func(t *testing.T) {
		// Write valid session count (1) but no session data.
		var buf bytes.Buffer
		b := make([]byte, 4)
		b[0] = 1 // numSessions = 1
		buf.Write(b)

		m := NewManager(10, 10)
		err := m.UnmarshalFrom(&buf)
		if err == nil {
			t.Fatal("UnmarshalFrom should fail on truncated data")
		}
	})

	t.Run("unmarshal truncated respondedTo", func(t *testing.T) {
		var buf bytes.Buffer
		b := make([]byte, 12) // 4 (count) + 8 (clientID) -- missing respondedTo
		b[0] = 1              // numSessions = 1
		b[4] = 42             // clientID = 42
		buf.Write(b)

		m := NewManager(10, 10)
		err := m.UnmarshalFrom(&buf)
		if err == nil {
			t.Fatal("UnmarshalFrom should fail on truncated respondedTo")
		}
	})

	t.Run("unmarshal truncated response count", func(t *testing.T) {
		var buf bytes.Buffer
		b := make([]byte, 20) // 4 (count) + 8 (clientID) + 8 (respondedTo) -- missing numResponses
		b[0] = 1
		buf.Write(b)

		m := NewManager(10, 10)
		err := m.UnmarshalFrom(&buf)
		if err == nil {
			t.Fatal("UnmarshalFrom should fail on truncated response count")
		}
	})

	t.Run("unmarshal truncated seriesID in response", func(t *testing.T) {
		var buf bytes.Buffer
		b := make([]byte, 24) // 4 (count) + 8 (clientID) + 8 (respondedTo) + 4 (numResponses)
		b[0] = 1              // numSessions = 1
		b[20] = 1             // numResponses = 1, but no actual response data
		buf.Write(b)

		m := NewManager(10, 10)
		err := m.UnmarshalFrom(&buf)
		if err == nil {
			t.Fatal("UnmarshalFrom should fail on truncated seriesID")
		}
	})

	t.Run("unmarshal truncated result value", func(t *testing.T) {
		var buf bytes.Buffer
		b := make([]byte, 32) // count + clientID + respondedTo + numResponses + seriesID -- missing value
		b[0] = 1              // numSessions = 1
		b[20] = 1             // numResponses = 1
		buf.Write(b)

		m := NewManager(10, 10)
		err := m.UnmarshalFrom(&buf)
		if err == nil {
			t.Fatal("UnmarshalFrom should fail on truncated result value")
		}
	})

	t.Run("unmarshal truncated data length", func(t *testing.T) {
		var buf bytes.Buffer
		b := make([]byte, 40) // ... + seriesID + value -- missing dataLen
		b[0] = 1              // numSessions
		b[20] = 1             // numResponses
		buf.Write(b)

		m := NewManager(10, 10)
		err := m.UnmarshalFrom(&buf)
		if err == nil {
			t.Fatal("UnmarshalFrom should fail on truncated data length")
		}
	})

	t.Run("unmarshal truncated data payload", func(t *testing.T) {
		var buf bytes.Buffer
		b := make([]byte, 44)
		b[0] = 1  // numSessions
		b[20] = 1 // numResponses
		b[40] = 5 // dataLen = 5, but no data follows
		buf.Write(b)

		m := NewManager(10, 10)
		err := m.UnmarshalFrom(&buf)
		if err == nil {
			t.Fatal("UnmarshalFrom should fail on truncated data payload")
		}
	})

	t.Run("unmarshal truncated lastActiveIndex", func(t *testing.T) {
		// Valid session with 0 responses but missing lastActiveIndex.
		var buf bytes.Buffer
		b := make([]byte, 24) // count(4) + clientID(8) + respondedTo(8) + numResponses(4)
		b[0] = 1              // numSessions = 1
		b[20] = 0             // numResponses = 0
		buf.Write(b)

		m := NewManager(10, 10)
		err := m.UnmarshalFrom(&buf)
		if err == nil {
			t.Fatal("UnmarshalFrom should fail on truncated lastActiveIndex")
		}
	})
}

func TestMarshalError(t *testing.T) {
	t.Run("error message", func(t *testing.T) {
		err := &MarshalError{Op: "write clientID", Err: io.ErrShortWrite}
		expected := "session: marshal failed at write clientID: short write"
		if err.Error() != expected {
			t.Errorf("Error() = %q, want %q", err.Error(), expected)
		}
	})

	t.Run("unwrap", func(t *testing.T) {
		inner := io.ErrShortWrite
		err := &MarshalError{Op: "test", Err: inner}
		if !errors.Is(err, inner) {
			t.Error("Unwrap should return inner error")
		}
	})
}

func TestUnmarshalError(t *testing.T) {
	t.Run("error message", func(t *testing.T) {
		err := &UnmarshalError{Op: "read clientID", Err: io.ErrUnexpectedEOF}
		expected := "session: unmarshal failed at read clientID: unexpected EOF"
		if err.Error() != expected {
			t.Errorf("Error() = %q, want %q", err.Error(), expected)
		}
	})

	t.Run("unwrap", func(t *testing.T) {
		inner := io.ErrUnexpectedEOF
		err := &UnmarshalError{Op: "test", Err: inner}
		if !errors.Is(err, inner) {
			t.Error("Unwrap should return inner error")
		}
	})
}

func TestManagerMarshalWriterFailures(t *testing.T) {
	t.Run("fail writing session count", func(t *testing.T) {
		m := NewManager(10, 10)
		w := &failWriter{failAfter: 0}
		err := m.MarshalTo(w)
		if err == nil {
			t.Fatal("should fail")
		}
	})

	t.Run("fail writing clientID", func(t *testing.T) {
		m := NewManager(10, 10)
		if err := m.Register(1, 100); err != nil {
			t.Fatalf("Register = %v", err)
		}
		w := &failWriter{failAfter: 1} // fail on second write
		err := m.MarshalTo(w)
		if err == nil {
			t.Fatal("should fail")
		}
	})

	t.Run("fail writing respondedTo", func(t *testing.T) {
		m := NewManager(10, 10)
		if err := m.Register(1, 100); err != nil {
			t.Fatalf("Register = %v", err)
		}
		w := &failWriter{failAfter: 2}
		err := m.MarshalTo(w)
		if err == nil {
			t.Fatal("should fail")
		}
	})

	t.Run("fail writing response count", func(t *testing.T) {
		m := NewManager(10, 10)
		if err := m.Register(1, 100); err != nil {
			t.Fatalf("Register = %v", err)
		}
		w := &failWriter{failAfter: 3}
		err := m.MarshalTo(w)
		if err == nil {
			t.Fatal("should fail")
		}
	})

	t.Run("fail writing seriesID", func(t *testing.T) {
		m := NewManager(10, 10)
		if err := m.Register(1, 100); err != nil {
			t.Fatalf("Register = %v", err)
		}
		if err := m.RecordResult(1, 5, 0, 101, sm.Result{Value: 42}); err != nil {
			t.Fatalf("RecordResult = %v", err)
		}
		w := &failWriter{failAfter: 4} // session count, clientID, respondedTo, numResponses, then fail
		err := m.MarshalTo(w)
		if err == nil {
			t.Fatal("should fail")
		}
	})

	t.Run("fail writing result value", func(t *testing.T) {
		m := NewManager(10, 10)
		if err := m.Register(1, 100); err != nil {
			t.Fatalf("Register = %v", err)
		}
		if err := m.RecordResult(1, 5, 0, 101, sm.Result{Value: 42}); err != nil {
			t.Fatalf("RecordResult = %v", err)
		}
		w := &failWriter{failAfter: 5}
		err := m.MarshalTo(w)
		if err == nil {
			t.Fatal("should fail")
		}
	})

	t.Run("fail writing data length", func(t *testing.T) {
		m := NewManager(10, 10)
		if err := m.Register(1, 100); err != nil {
			t.Fatalf("Register = %v", err)
		}
		if err := m.RecordResult(1, 5, 0, 101, sm.Result{Value: 42}); err != nil {
			t.Fatalf("RecordResult = %v", err)
		}
		w := &failWriter{failAfter: 6}
		err := m.MarshalTo(w)
		if err == nil {
			t.Fatal("should fail")
		}
	})

	t.Run("fail writing data payload", func(t *testing.T) {
		m := NewManager(10, 10)
		if err := m.Register(1, 100); err != nil {
			t.Fatalf("Register = %v", err)
		}
		if err := m.RecordResult(1, 5, 0, 101, sm.Result{Value: 42, Data: []byte("payload")}); err != nil {
			t.Fatalf("RecordResult = %v", err)
		}
		w := &failWriter{failAfter: 7}
		err := m.MarshalTo(w)
		if err == nil {
			t.Fatal("should fail")
		}
	})

	t.Run("fail writing lastActiveIndex", func(t *testing.T) {
		m := NewManager(10, 10)
		if err := m.Register(1, 100); err != nil {
			t.Fatalf("Register = %v", err)
		}
		if err := m.RecordResult(1, 5, 0, 101, sm.Result{Value: 42, Data: []byte("payload")}); err != nil {
			t.Fatalf("RecordResult = %v", err)
		}
		w := &failWriter{failAfter: 8}
		err := m.MarshalTo(w)
		if err == nil {
			t.Fatal("should fail")
		}
	})
}

func TestDeduplicationEndToEnd(t *testing.T) {
	t.Run("same proposal applied only once", func(t *testing.T) {
		m := NewManager(100, 100)
		if err := m.Register(1, 100); err != nil {
			t.Fatalf("Register = %v", err)
		}

		// First application.
		_, isDup, err := m.CheckDuplicate(1, 1, 101)
		if err != nil {
			t.Fatalf("CheckDuplicate = %v", err)
		}
		if isDup {
			t.Fatal("first proposal should not be duplicate")
		}
		if err := m.RecordResult(1, 1, 0, 101, sm.Result{Value: 42}); err != nil {
			t.Fatalf("RecordResult = %v", err)
		}

		// Retry of same proposal.
		result, isDup, err := m.CheckDuplicate(1, 1, 102)
		if err != nil {
			t.Fatalf("CheckDuplicate = %v", err)
		}
		if !isDup {
			t.Fatal("retry should be detected as duplicate")
		}
		if result.Value != 42 {
			t.Errorf("cached result.Value = %d, want 42", result.Value)
		}
	})
}

// failWriter is a test helper that fails after N writes.
type failWriter struct {
	failAfter int
	count     int
}

func (w *failWriter) Write(p []byte) (int, error) {
	if w.count >= w.failAfter {
		return 0, io.ErrShortWrite
	}
	w.count++
	return len(p), nil
}

// failReader is a test helper that always returns an error.
type failReader struct{}

func (r *failReader) Read(p []byte) (int, error) {
	return 0, io.ErrUnexpectedEOF
}

func TestUnmarshalFrom_BoundsCheckNumSessions(t *testing.T) {
	t.Run("exceeds maxTotal", func(t *testing.T) {
		// Manager with maxTotal=5. Forge a stream claiming 10 sessions.
		m := NewManager(5, 10)
		var buf bytes.Buffer
		b := make([]byte, 4)
		binary.LittleEndian.PutUint32(b, 10) // numSessions = 10
		buf.Write(b)

		err := m.UnmarshalFrom(&buf)
		if err == nil {
			t.Fatal("UnmarshalFrom should fail when numSessions > maxTotal")
		}
		var boundsErr *BoundsError
		if !errors.As(err, &boundsErr) {
			t.Fatalf("error should be *BoundsError, got %T: %v", err, err)
		}
		if boundsErr.Field != "numSessions" {
			t.Errorf("Field = %q, want %q", boundsErr.Field, "numSessions")
		}
		if boundsErr.Value != 10 {
			t.Errorf("Value = %d, want 10", boundsErr.Value)
		}
		if boundsErr.Limit != 5 {
			t.Errorf("Limit = %d, want 5", boundsErr.Limit)
		}
		if !errors.Is(err, ErrSnapshotCorrupted) {
			t.Error("BoundsError should match ErrSnapshotCorrupted via Is()")
		}
	})

	t.Run("at maxTotal boundary succeeds", func(t *testing.T) {
		// Create a manager with maxTotal=2, marshal 2 sessions, unmarshal.
		m1 := NewManager(2, 10)
		if err := m1.Register(1, 100); err != nil {
			t.Fatalf("Register(1) = %v", err)
		}
		if err := m1.Register(2, 101); err != nil {
			t.Fatalf("Register(2) = %v", err)
		}
		var buf bytes.Buffer
		if err := m1.MarshalTo(&buf); err != nil {
			t.Fatalf("MarshalTo = %v", err)
		}

		m2 := NewManager(2, 10)
		if err := m2.UnmarshalFrom(&buf); err != nil {
			t.Fatalf("UnmarshalFrom should succeed at boundary: %v", err)
		}
		if m2.Count() != 2 {
			t.Errorf("Count() = %d, want 2", m2.Count())
		}
	})

	t.Run("max uint32 numSessions", func(t *testing.T) {
		// Forge a stream with numSessions = 4294967295 (max uint32).
		m := NewManager(100, 10)
		var buf bytes.Buffer
		b := make([]byte, 4)
		binary.LittleEndian.PutUint32(b, 0xFFFFFFFF)
		buf.Write(b)

		err := m.UnmarshalFrom(&buf)
		if err == nil {
			t.Fatal("UnmarshalFrom should fail for max uint32 numSessions")
		}
		if !errors.Is(err, ErrSnapshotCorrupted) {
			t.Errorf("error should match ErrSnapshotCorrupted, got %v", err)
		}
	})
}

func TestUnmarshalFrom_BoundsCheckNumResponses(t *testing.T) {
	t.Run("exceeds maxResponses", func(t *testing.T) {
		// Manager with maxResponses=3. Forge a stream with 1 session
		// claiming 100 responses.
		m := NewManager(10, 3)
		var buf bytes.Buffer

		// numSessions = 1
		b := make([]byte, 4)
		binary.LittleEndian.PutUint32(b, 1)
		buf.Write(b)

		// clientID = 1
		b = make([]byte, 8)
		binary.LittleEndian.PutUint64(b, 1)
		buf.Write(b)

		// respondedTo = 0
		b = make([]byte, 8)
		buf.Write(b)

		// numResponses = 100 (exceeds maxResponses=3)
		b = make([]byte, 4)
		binary.LittleEndian.PutUint32(b, 100)
		buf.Write(b)

		err := m.UnmarshalFrom(&buf)
		if err == nil {
			t.Fatal("UnmarshalFrom should fail when numResponses > maxResponses")
		}
		var boundsErr *BoundsError
		if !errors.As(err, &boundsErr) {
			t.Fatalf("error should be *BoundsError, got %T: %v", err, err)
		}
		if boundsErr.Field != "numResponses" {
			t.Errorf("Field = %q, want %q", boundsErr.Field, "numResponses")
		}
		if boundsErr.Value != 100 {
			t.Errorf("Value = %d, want 100", boundsErr.Value)
		}
		if boundsErr.Limit != 3 {
			t.Errorf("Limit = %d, want 3", boundsErr.Limit)
		}
	})

	t.Run("at maxResponses boundary succeeds", func(t *testing.T) {
		// maxResponses=3, create session with exactly 3 responses.
		m1 := NewManager(10, 3)
		if err := m1.Register(1, 100); err != nil {
			t.Fatalf("Register = %v", err)
		}
		for i := uint64(1); i <= 3; i++ {
			if err := m1.RecordResult(1, i, 0, 100+i, sm.Result{Value: i}); err != nil {
				t.Fatalf("RecordResult(%d) = %v", i, err)
			}
		}
		var buf bytes.Buffer
		if err := m1.MarshalTo(&buf); err != nil {
			t.Fatalf("MarshalTo = %v", err)
		}

		m2 := NewManager(10, 3)
		if err := m2.UnmarshalFrom(&buf); err != nil {
			t.Fatalf("UnmarshalFrom should succeed at boundary: %v", err)
		}
	})
}

func TestUnmarshalFrom_BoundsCheckDataLen(t *testing.T) {
	t.Run("exceeds maxResponseDataSize", func(t *testing.T) {
		// Forge a stream with 1 session, 1 response, with
		// dataLen = maxResponseDataSize + 1.
		m := NewManager(10, 10)
		var buf bytes.Buffer

		// numSessions = 1
		b := make([]byte, 4)
		binary.LittleEndian.PutUint32(b, 1)
		buf.Write(b)

		// clientID = 1
		b = make([]byte, 8)
		binary.LittleEndian.PutUint64(b, 1)
		buf.Write(b)

		// respondedTo = 0
		b = make([]byte, 8)
		buf.Write(b)

		// numResponses = 1
		b = make([]byte, 4)
		binary.LittleEndian.PutUint32(b, 1)
		buf.Write(b)

		// seriesID = 1
		b = make([]byte, 8)
		binary.LittleEndian.PutUint64(b, 1)
		buf.Write(b)

		// result value = 42
		b = make([]byte, 8)
		binary.LittleEndian.PutUint64(b, 42)
		buf.Write(b)

		// dataLen = maxResponseDataSize + 1 (exceeds limit)
		oversized := uint32(maxResponseDataSize + 1)
		b = make([]byte, 4)
		binary.LittleEndian.PutUint32(b, oversized)
		buf.Write(b)
		// No actual data follows; the bounds check fires before read.

		err := m.UnmarshalFrom(&buf)
		if err == nil {
			t.Fatal("UnmarshalFrom should fail when dataLen > maxResponseDataSize")
		}
		var boundsErr *BoundsError
		if !errors.As(err, &boundsErr) {
			t.Fatalf("error should be *BoundsError, got %T: %v", err, err)
		}
		if boundsErr.Field != "responseDataLen" {
			t.Errorf("Field = %q, want %q", boundsErr.Field, "responseDataLen")
		}
		if boundsErr.Value != uint64(oversized) {
			t.Errorf("Value = %d, want %d", boundsErr.Value, oversized)
		}
		if boundsErr.Limit != maxResponseDataSize {
			t.Errorf("Limit = %d, want %d", boundsErr.Limit, maxResponseDataSize)
		}
	})

	t.Run("zero dataLen succeeds", func(t *testing.T) {
		// Normal roundtrip with a result that has no data.
		m1 := NewManager(10, 10)
		if err := m1.Register(1, 100); err != nil {
			t.Fatalf("Register = %v", err)
		}
		if err := m1.RecordResult(1, 1, 0, 101, sm.Result{Value: 99}); err != nil {
			t.Fatalf("RecordResult = %v", err)
		}
		var buf bytes.Buffer
		if err := m1.MarshalTo(&buf); err != nil {
			t.Fatalf("MarshalTo = %v", err)
		}
		m2 := NewManager(10, 10)
		if err := m2.UnmarshalFrom(&buf); err != nil {
			t.Fatalf("UnmarshalFrom should succeed: %v", err)
		}
		result, isDup, checkErr := m2.CheckDuplicate(1, 1, 200)
		if checkErr != nil {
			t.Fatalf("CheckDuplicate = %v", checkErr)
		}
		if !isDup || result.Value != 99 {
			t.Errorf("unexpected result after roundtrip: isDup=%v, Value=%d", isDup, result.Value)
		}
	})

	t.Run("valid data within limit", func(t *testing.T) {
		// Result with 1KB data payload - well within the 64MiB limit.
		m1 := NewManager(10, 10)
		if err := m1.Register(1, 100); err != nil {
			t.Fatalf("Register = %v", err)
		}
		data := make([]byte, 1024)
		for i := range data {
			data[i] = byte(i % 256)
		}
		if err := m1.RecordResult(1, 1, 0, 101, sm.Result{Value: 7, Data: data}); err != nil {
			t.Fatalf("RecordResult = %v", err)
		}
		var buf bytes.Buffer
		if err := m1.MarshalTo(&buf); err != nil {
			t.Fatalf("MarshalTo = %v", err)
		}
		m2 := NewManager(10, 10)
		if err := m2.UnmarshalFrom(&buf); err != nil {
			t.Fatalf("UnmarshalFrom should succeed: %v", err)
		}
		result, isDup, checkErr := m2.CheckDuplicate(1, 1, 200)
		if checkErr != nil {
			t.Fatalf("CheckDuplicate = %v", checkErr)
		}
		if !isDup || result.Value != 7 {
			t.Errorf("unexpected result: isDup=%v, Value=%d", isDup, result.Value)
		}
		if !bytes.Equal(result.Data, data) {
			t.Error("data mismatch after roundtrip")
		}
	})
}

func TestBoundsError(t *testing.T) {
	t.Run("error message", func(t *testing.T) {
		err := &BoundsError{Field: "numSessions", Value: 100, Limit: 50}
		expected := "session: snapshot bounds exceeded: numSessions = 100, limit = 50"
		if err.Error() != expected {
			t.Errorf("Error() = %q, want %q", err.Error(), expected)
		}
	})

	t.Run("is ErrSnapshotCorrupted", func(t *testing.T) {
		err := &BoundsError{Field: "test", Value: 1, Limit: 0}
		if !errors.Is(err, ErrSnapshotCorrupted) {
			t.Error("BoundsError should match ErrSnapshotCorrupted")
		}
	})

	t.Run("is not unrelated error", func(t *testing.T) {
		err := &BoundsError{Field: "test", Value: 1, Limit: 0}
		if errors.Is(err, ErrSessionNotFound) {
			t.Error("BoundsError should not match ErrSessionNotFound")
		}
	})
}

// TestManager_ExpireSessions_DeterministicAcrossReplicas verifies that two
// independent session managers with identical configuration and state expire
// the exact same set of sessions (same clientIDs in the same order) when
// driven to the same log index. This is critical for Raft state machine
// safety: all replicas must apply the same deterministic transitions so that
// their state machines remain identical after every applied entry.
//
// Unlike the simpler "deterministic across replicas" subtest inside
// TestManagerExpireSessions (which uses a single session), this test
// exercises a realistic scenario with multiple sessions at varying activity
// levels, a maxExpire limit smaller than the expired set, and verifies
// element-by-element ordering equality.
func TestManager_ExpireSessions_DeterministicAcrossReplicas(t *testing.T) {
	const (
		maxTotal            = 100
		maxResponses        = 10
		inactivityThreshold = 1000
		currentIndex        = 5000
		maxExpire           = 3
		numSessions         = 8
	)

	// buildManager creates a manager and registers numSessions clients
	// at varying initial indexes. Some sessions will be active (registered
	// at high indexes) and some will be stale (registered at low indexes).
	// The pattern is identical across both replicas.
	buildManager := func(t *testing.T) *Manager {
		t.Helper()
		m := NewManager(maxTotal, maxResponses)

		// Stale sessions: registered at low indexes, will be expired.
		staleIDs := []uint64{300, 100, 500, 200, 400}
		for _, id := range staleIDs {
			if err := m.Register(id, 10); err != nil {
				t.Fatalf("Register(%d) = %v", id, err)
			}
		}

		// Active sessions: registered at recent indexes, will NOT be expired.
		activeIDs := []uint64{600, 700, 800}
		for _, id := range activeIDs {
			if err := m.Register(id, currentIndex-100); err != nil {
				t.Fatalf("Register(%d) = %v", id, err)
			}
		}
		return m
	}

	m1 := buildManager(t)
	m2 := buildManager(t)

	expired1 := m1.ExpireSessions(currentIndex, inactivityThreshold, maxExpire)
	expired2 := m2.ExpireSessions(currentIndex, inactivityThreshold, maxExpire)

	// Both replicas must expire exactly maxExpire sessions.
	if len(expired1) != maxExpire {
		t.Fatalf("m1: len(expired) = %d, want %d", len(expired1), maxExpire)
	}
	if len(expired2) != maxExpire {
		t.Fatalf("m2: len(expired) = %d, want %d", len(expired2), maxExpire)
	}

	// Element-by-element comparison: same clientIDs in the same order.
	for i := range expired1 {
		if expired1[i] != expired2[i] {
			t.Errorf("replica divergence at index %d: m1=%d, m2=%d",
				i, expired1[i], expired2[i])
		}
	}

	// The expired set must be sorted (ascending clientID) per the
	// deterministic sort-then-limit algorithm.
	for i := 1; i < len(expired1); i++ {
		if expired1[i] <= expired1[i-1] {
			t.Errorf("expired not sorted: expired[%d]=%d <= expired[%d]=%d",
				i, expired1[i], i-1, expired1[i-1])
		}
	}

	// Since maxExpire=3 and 5 stale sessions exist (100,200,300,400,500),
	// the lowest 3 clientIDs must be selected.
	want := []uint64{100, 200, 300}
	for i, id := range want {
		if expired1[i] != id {
			t.Errorf("expired1[%d] = %d, want %d", i, expired1[i], id)
		}
	}

	// Active sessions must remain on both managers.
	for _, id := range []uint64{600, 700, 800} {
		if !m1.IsRegistered(id) {
			t.Errorf("m1: active session %d was incorrectly expired", id)
		}
		if !m2.IsRegistered(id) {
			t.Errorf("m2: active session %d was incorrectly expired", id)
		}
	}

	// Remaining session counts must match.
	if m1.Count() != m2.Count() {
		t.Errorf("session count divergence: m1=%d, m2=%d",
			m1.Count(), m2.Count())
	}
}

// TestManager_ReRegisterAfterUnregister verifies that a client can be
// re-registered after being unregistered, and that the new session starts
// fresh with no cached responses from the previous session.
func TestManager_ReRegisterAfterUnregister(t *testing.T) {
	m := NewManager(10, 10)

	// Register, record a result, then unregister.
	if err := m.Register(42, 100); err != nil {
		t.Fatalf("Register(1st) = %v", err)
	}
	if err := m.RecordResult(42, 1, 0, 200, sm.Result{Value: 99}); err != nil {
		t.Fatalf("RecordResult = %v", err)
	}
	if err := m.Unregister(42); err != nil {
		t.Fatalf("Unregister = %v", err)
	}

	// Re-register with the same clientID.
	if err := m.Register(42, 300); err != nil {
		t.Fatalf("Register(2nd) = %v", err)
	}

	// The new session should have no cached responses.
	_, isDup, err := m.CheckDuplicate(42, 1, 400)
	if err != nil {
		t.Fatalf("CheckDuplicate = %v", err)
	}
	if isDup {
		t.Error("re-registered session should not have cached responses from previous session")
	}
}

// TestManager_ReRegisterAfterUnregister_ReturnsError verifies that
// re-registering without unregistering first returns ErrSessionAlreadyExists.
func TestManager_ReRegisterAfterUnregister_ReturnsError(t *testing.T) {
	m := NewManager(10, 10)

	if err := m.Register(42, 100); err != nil {
		t.Fatalf("Register = %v", err)
	}
	err := m.Register(42, 200)
	if err != ErrSessionAlreadyExists {
		t.Fatalf("Register(duplicate) = %v, want ErrSessionAlreadyExists", err)
	}
}

// TestManager_SessionLimitBoundary verifies that registering exactly at the
// session limit succeeds, and the next registration fails with
// ErrSessionLimitExceeded.
func TestManager_SessionLimitBoundary(t *testing.T) {
	const maxTotal = 3
	m := NewManager(maxTotal, 10)

	// Register exactly maxTotal sessions.
	for id := uint64(1); id <= maxTotal; id++ {
		if err := m.Register(id, 100); err != nil {
			t.Fatalf("Register(%d) = %v", id, err)
		}
	}
	if m.Count() != int(maxTotal) {
		t.Fatalf("Count = %d, want %d", m.Count(), maxTotal)
	}

	// One more should fail.
	err := m.Register(maxTotal+1, 100)
	if err != ErrSessionLimitExceeded {
		t.Fatalf("Register(over limit) = %v, want ErrSessionLimitExceeded", err)
	}

	// Count should not change.
	if m.Count() != int(maxTotal) {
		t.Errorf("Count = %d, want %d after failed register", m.Count(), maxTotal)
	}
}

// TestManager_RespondedToMonotonicallyAdvances verifies that respondedTo
// can only advance forward. A RecordResult with a lower respondedTo does
// not regress the session's respondedTo marker.
func TestManager_RespondedToMonotonicallyAdvances(t *testing.T) {
	m := NewManager(10, 10)

	if err := m.Register(1, 100); err != nil {
		t.Fatalf("Register = %v", err)
	}

	// Record series 1, advance respondedTo to 0.
	if err := m.RecordResult(1, 1, 0, 101, sm.Result{Value: 10}); err != nil {
		t.Fatalf("RecordResult(1) = %v", err)
	}

	// Record series 2, advance respondedTo to 5.
	if err := m.RecordResult(1, 2, 5, 102, sm.Result{Value: 20}); err != nil {
		t.Fatalf("RecordResult(2) = %v", err)
	}

	// Record series 6 with respondedTo=3 (lower than current 5).
	// respondedTo should NOT regress from 5 to 3.
	if err := m.RecordResult(1, 6, 3, 103, sm.Result{Value: 60}); err != nil {
		t.Fatalf("RecordResult(6) = %v", err)
	}

	// Series 4 should be evicted (4 <= 5, from the earlier respondedTo=5).
	_, _, err := m.CheckDuplicate(1, 4, 200)
	if !errors.Is(err, ErrResponseEvicted) {
		t.Errorf("series 4: expected ErrResponseEvicted, got %v", err)
	}

	// Series 5 should also be evicted (5 <= 5).
	_, _, err = m.CheckDuplicate(1, 5, 200)
	if !errors.Is(err, ErrResponseEvicted) {
		t.Errorf("series 5: expected ErrResponseEvicted, got %v", err)
	}

	// Series 6 should be cached.
	result, isDup, err := m.CheckDuplicate(1, 6, 200)
	if err != nil {
		t.Fatalf("series 6: %v", err)
	}
	if !isDup {
		t.Error("series 6 should be duplicate")
	}
	if result.Value != 60 {
		t.Errorf("series 6 Value = %d, want 60", result.Value)
	}
}

// TestManager_ExpireSessionsZeroMaxExpire verifies that ExpireSessions
// returns nil when maxExpire is zero, even when there are stale sessions.
func TestManager_ExpireSessionsZeroMaxExpire(t *testing.T) {
	m := NewManager(10, 10)
	if err := m.Register(1, 0); err != nil {
		t.Fatalf("Register = %v", err)
	}

	// Session is stale (gap=1000, threshold=100).
	expired := m.ExpireSessions(1000, 100, 0)
	if expired != nil {
		t.Errorf("ExpireSessions(maxExpire=0) = %v, want nil", expired)
	}
	if m.Count() != 1 {
		t.Errorf("Count = %d, want 1 (no sessions should be expired)", m.Count())
	}
}

// TestManager_ExpireSessionsNegativeMaxExpire verifies that
// ExpireSessions handles a negative maxExpire gracefully.
func TestManager_ExpireSessionsNegativeMaxExpire(t *testing.T) {
	m := NewManager(10, 10)
	if err := m.Register(1, 0); err != nil {
		t.Fatalf("Register = %v", err)
	}

	expired := m.ExpireSessions(1000, 100, -1)
	if expired != nil {
		t.Errorf("ExpireSessions(maxExpire=-1) = %v, want nil", expired)
	}
}

// TestManager_CheckDuplicateUnregistered verifies that CheckDuplicate
// returns ErrSessionNotFound for a session that was never registered.
func TestManager_CheckDuplicateUnregistered(t *testing.T) {
	m := NewManager(10, 10)

	_, isDup, err := m.CheckDuplicate(999, 1, 100)
	if !errors.Is(err, ErrSessionNotFound) {
		t.Fatalf("CheckDuplicate = %v, want ErrSessionNotFound", err)
	}
	if isDup {
		t.Error("isDup should be false for unregistered session")
	}
}

// TestManager_MarshalUnmarshalManySessionsWithResponses verifies that
// a large number of sessions with multiple cached responses each survive
// a snapshot round-trip with all data intact.
func TestManager_MarshalUnmarshalManySessionsWithResponses(t *testing.T) {
	const numSessions = 50
	const numResponses = 10
	m := NewManager(1000, 1000)

	for id := uint64(1); id <= numSessions; id++ {
		if err := m.Register(id, id*10); err != nil {
			t.Fatalf("Register(%d) = %v", id, err)
		}
		for sid := uint64(1); sid <= numResponses; sid++ {
			result := sm.Result{Value: id*1000 + sid}
			if err := m.RecordResult(id, sid, 0, id*10+sid, result); err != nil {
				t.Fatalf("RecordResult(%d, %d) = %v", id, sid, err)
			}
		}
	}

	// Marshal.
	var buf bytes.Buffer
	if err := m.MarshalTo(&buf); err != nil {
		t.Fatalf("MarshalTo: %v", err)
	}

	// Unmarshal into a new manager.
	restored := NewManager(1000, 1000)
	if err := restored.UnmarshalFrom(bytes.NewReader(buf.Bytes())); err != nil {
		t.Fatalf("UnmarshalFrom: %v", err)
	}

	if restored.Count() != numSessions {
		t.Fatalf("Count = %d, want %d", restored.Count(), numSessions)
	}

	// Spot-check every session and response.
	for id := uint64(1); id <= numSessions; id++ {
		if !restored.IsRegistered(id) {
			t.Errorf("session %d not registered after restore", id)
			continue
		}
		for sid := uint64(1); sid <= numResponses; sid++ {
			result, isDup, err := restored.CheckDuplicate(id, sid, 99999)
			if err != nil {
				t.Fatalf("CheckDuplicate(%d, %d) = %v", id, sid, err)
			}
			if !isDup {
				t.Errorf("session %d series %d should be duplicate", id, sid)
			}
			want := id*1000 + sid
			if result.Value != want {
				t.Errorf("session %d series %d Value = %d, want %d",
					id, sid, result.Value, want)
			}
		}
	}
}
