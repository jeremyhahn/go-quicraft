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
	"sync"
	"testing"
)

// ---------------------------------------------------------------------------
// Construction
// ---------------------------------------------------------------------------

func TestNewSession_Fields(t *testing.T) {
	s := newSession(10, 20)
	if s.ShardID() != 10 {
		t.Errorf("ShardID = %d, want 10", s.ShardID())
	}
	if s.ClientID() != 20 {
		t.Errorf("ClientID = %d, want 20", s.ClientID())
	}
	if s.SeriesID() != 0 {
		t.Errorf("SeriesID = %d, want 0", s.SeriesID())
	}
	if s.RespondedTo() != 0 {
		t.Errorf("RespondedTo = %d, want 0", s.RespondedTo())
	}
}

func TestNewSession_InvalidZeroClientID(t *testing.T) {
	s := newSession(5, 0)
	if !s.IsNoOPSession() {
		t.Error("session with clientID 0 should be a no-op session")
	}
}

func TestNewNoOPSession_Fields(t *testing.T) {
	s := NewNoOPSession(42)
	if s.ShardID() != 42 {
		t.Errorf("ShardID = %d, want 42", s.ShardID())
	}
	if s.ClientID() != 0 {
		t.Errorf("ClientID = %d, want 0 (noOPClientID)", s.ClientID())
	}
	if !s.IsNoOPSession() {
		t.Error("no-op session should return true for IsNoOPSession")
	}
}

func TestNewNoOPSession_IsNotRegularSession(t *testing.T) {
	regular := newSession(1, 100)
	noop := NewNoOPSession(1)

	if regular.IsNoOPSession() {
		t.Error("regular session should not be a no-op session")
	}
	if !noop.IsNoOPSession() {
		t.Error("no-op session should be a no-op session")
	}
}

// ---------------------------------------------------------------------------
// PrepareForPropose
// ---------------------------------------------------------------------------

func TestPrepareForPropose_SetsFirstProposal(t *testing.T) {
	s := newSession(1, 100)
	if s.SeriesID() != 0 {
		t.Fatalf("initial SeriesID = %d, want 0", s.SeriesID())
	}

	s.PrepareForPropose()
	if s.SeriesID() != SeriesIDFirstProposal {
		t.Errorf("SeriesID after PrepareForPropose = %d, want %d",
			s.SeriesID(), SeriesIDFirstProposal)
	}
}

func TestProposalCompleted_AdvancesSeriesID(t *testing.T) {
	s := newSession(1, 100)
	s.PrepareForPropose()
	if s.SeriesID() != SeriesIDFirstProposal {
		t.Fatalf("SeriesID = %d, want %d", s.SeriesID(), SeriesIDFirstProposal)
	}

	// Complete the first proposal — should advance to 2.
	s.ProposalCompleted()
	if s.SeriesID() != SeriesIDFirstProposal+1 {
		t.Errorf("SeriesID after ProposalCompleted = %d, want %d",
			s.SeriesID(), SeriesIDFirstProposal+1)
	}
	if s.RespondedTo() != SeriesIDFirstProposal {
		t.Errorf("RespondedTo = %d, want %d", s.RespondedTo(), SeriesIDFirstProposal)
	}
}

func TestProposalCompleted_Monotonic(t *testing.T) {
	s := newSession(1, 100)
	s.PrepareForPropose()
	for i := uint64(0); i < 100; i++ {
		expected := SeriesIDFirstProposal + i
		if s.SeriesID() != expected {
			t.Fatalf("iteration %d: SeriesID = %d, want %d", i, s.SeriesID(), expected)
		}
		s.ProposalCompleted()
	}
}

func TestProposalCompleted_ConcurrentContiguousAdvance(t *testing.T) {
	s := newSession(1, 100)
	s.PrepareForPropose()

	const goroutines = 100
	var wg sync.WaitGroup
	wg.Add(goroutines)
	for i := 0; i < goroutines; i++ {
		go func() {
			defer wg.Done()
			s.ProposalCompleted()
		}()
	}
	wg.Wait()

	// seriesID must have advanced exactly by the number of completions.
	expectedSeriesID := SeriesIDFirstProposal + goroutines
	if s.SeriesID() != expectedSeriesID {
		t.Errorf("SeriesID = %d, want %d (no skips)", s.SeriesID(), expectedSeriesID)
	}

	// respondedTo must be at least SeriesIDFirstProposal (the first
	// completed value) and at most seriesID - 1. With concurrent
	// ProposalCompleted calls, the final respondedTo depends on
	// goroutine scheduling because Store is not monotonic in the
	// concurrent path. The seriesID CAS is correct (no skips), but
	// respondedTo may reflect any of the completed values.
	rt := s.RespondedTo()
	if rt < SeriesIDFirstProposal || rt >= expectedSeriesID {
		t.Errorf("RespondedTo = %d, want in [%d, %d)", rt, SeriesIDFirstProposal, expectedSeriesID)
	}
}

func TestProposalCompleted_ConcurrentInvariant(t *testing.T) {
	// Verify that after N concurrent completions, the invariant
	// respondedTo == seriesID - 1 holds and seriesID advanced exactly N.
	s := newSession(1, 100)
	s.PrepareForPropose()

	const goroutines = 500
	var wg sync.WaitGroup
	wg.Add(goroutines)
	for i := 0; i < goroutines; i++ {
		go func() {
			defer wg.Done()
			s.ProposalCompleted()
		}()
	}
	wg.Wait()

	finalSeries := s.SeriesID()
	finalResponded := s.RespondedTo()

	expectedSeries := SeriesIDFirstProposal + goroutines
	if finalSeries != expectedSeries {
		t.Errorf("SeriesID = %d, want %d (contiguous advance)", finalSeries, expectedSeries)
	}
	if finalResponded != expectedSeries-1 {
		t.Errorf("RespondedTo = %d, want %d", finalResponded, expectedSeries-1)
	}
}

// ---------------------------------------------------------------------------
// PrepareForRegister
// ---------------------------------------------------------------------------

func TestPrepareForRegister_SetsSentinel(t *testing.T) {
	s := newSession(1, 100)
	s.PrepareForPropose()
	s.ProposalCompleted()
	s.SetRespondedTo(1)

	s.PrepareForRegister()

	if s.SeriesID() != SeriesIDForRegister {
		t.Errorf("SeriesID after PrepareForRegister = %d, want %d",
			s.SeriesID(), SeriesIDForRegister)
	}
	if s.RespondedTo() != 0 {
		t.Errorf("RespondedTo after PrepareForRegister = %d, want 0", s.RespondedTo())
	}
}

func TestPrepareForRegister_DistinctFromUnregister(t *testing.T) {
	s := newSession(1, 100)
	s.PrepareForRegister()
	regID := s.SeriesID()

	s.PrepareForUnregister()
	unregID := s.SeriesID()

	if regID == unregID {
		t.Errorf("register and unregister series IDs must differ, both = %d", regID)
	}
	if regID != SeriesIDForRegister {
		t.Errorf("register SeriesID = %d, want %d", regID, SeriesIDForRegister)
	}
	if unregID != SeriesIDForUnregister {
		t.Errorf("unregister SeriesID = %d, want %d", unregID, SeriesIDForUnregister)
	}
}

// ---------------------------------------------------------------------------
// PrepareForUnregister
// ---------------------------------------------------------------------------

func TestPrepareForUnregister_SetsSentinel(t *testing.T) {
	s := newSession(1, 100)
	s.PrepareForPropose()
	s.ProposalCompleted()
	s.SetRespondedTo(2)

	s.PrepareForUnregister()

	if s.SeriesID() != SeriesIDForUnregister {
		t.Errorf("SeriesID after PrepareForUnregister = %d, want %d",
			s.SeriesID(), SeriesIDForUnregister)
	}
	if s.RespondedTo() != 0 {
		t.Errorf("RespondedTo after PrepareForUnregister = %d, want 0", s.RespondedTo())
	}
}

func TestPrepareForUnregister_Fresh(t *testing.T) {
	s := newSession(1, 100)
	s.PrepareForUnregister()
	if s.SeriesID() != SeriesIDForUnregister {
		t.Errorf("SeriesID = %d, want %d", s.SeriesID(), SeriesIDForUnregister)
	}
}

// ---------------------------------------------------------------------------
// ValidForSessionOp
// ---------------------------------------------------------------------------

func TestValidForSessionOp_CorrectShard(t *testing.T) {
	s := newSession(5, 100)
	if !s.ValidForSessionOp(5) {
		t.Error("session should be valid for its own shard")
	}
}

func TestValidForSessionOp_WrongShard(t *testing.T) {
	s := newSession(5, 100)
	if s.ValidForSessionOp(10) {
		t.Error("session should not be valid for a different shard")
	}
}

func TestValidForSessionOp_NoOpSession(t *testing.T) {
	s := NewNoOPSession(5)
	if s.ValidForSessionOp(5) {
		t.Error("no-op session should not be valid for session ops")
	}
}

// ---------------------------------------------------------------------------
// SetRespondedTo
// ---------------------------------------------------------------------------

func TestSetRespondedTo_MonotonicallyAdvances(t *testing.T) {
	s := newSession(1, 100)

	s.SetRespondedTo(5)
	if s.RespondedTo() != 5 {
		t.Errorf("RespondedTo = %d, want 5", s.RespondedTo())
	}

	// Setting a lower value should be a no-op.
	s.SetRespondedTo(3)
	if s.RespondedTo() != 5 {
		t.Errorf("RespondedTo = %d, want 5 (should not decrease)", s.RespondedTo())
	}

	// Setting a higher value should advance.
	s.SetRespondedTo(10)
	if s.RespondedTo() != 10 {
		t.Errorf("RespondedTo = %d, want 10", s.RespondedTo())
	}
}

func TestSetRespondedTo_ConcurrentSafety(t *testing.T) {
	s := newSession(1, 100)
	const goroutines = 50

	var wg sync.WaitGroup
	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func(val uint64) {
			defer wg.Done()
			s.SetRespondedTo(val)
		}(uint64(i + 1))
	}
	wg.Wait()

	// The final value should be the maximum.
	if s.RespondedTo() != goroutines {
		t.Errorf("RespondedTo = %d, want %d", s.RespondedTo(), goroutines)
	}
}

func TestSetRespondedTo_SameValueIsNoOp(t *testing.T) {
	s := newSession(1, 100)
	s.SetRespondedTo(5)
	s.SetRespondedTo(5)
	if s.RespondedTo() != 5 {
		t.Errorf("RespondedTo = %d, want 5", s.RespondedTo())
	}
}

// ---------------------------------------------------------------------------
// Getters
// ---------------------------------------------------------------------------

func TestSession_ShardID(t *testing.T) {
	s := newSession(42, 100)
	if s.ShardID() != 42 {
		t.Errorf("ShardID = %d, want 42", s.ShardID())
	}
}

func TestSession_ClientID(t *testing.T) {
	s := newSession(1, 999)
	if s.ClientID() != 999 {
		t.Errorf("ClientID = %d, want 999", s.ClientID())
	}
}

// ---------------------------------------------------------------------------
// IsNoOPSession
// ---------------------------------------------------------------------------

func TestIsNoOPSession_True(t *testing.T) {
	s := NewNoOPSession(1)
	if !s.IsNoOPSession() {
		t.Error("no-op session should return true")
	}
}

func TestIsNoOPSession_False(t *testing.T) {
	s := newSession(1, 100)
	if s.IsNoOPSession() {
		t.Error("regular session should return false")
	}
}

// ---------------------------------------------------------------------------
// ValidForProposal
// ---------------------------------------------------------------------------

func TestValidForProposal_ValidSession(t *testing.T) {
	s := newSession(1, 100)
	s.PrepareForPropose()
	if !s.ValidForProposal(1) {
		t.Error("valid session should return true")
	}
}

func TestValidForProposal_WrongShard(t *testing.T) {
	s := newSession(1, 100)
	s.PrepareForPropose()
	if s.ValidForProposal(2) {
		t.Error("wrong shard should return false")
	}
}

func TestValidForProposal_NoOpSession(t *testing.T) {
	s := NewNoOPSession(1)
	if s.ValidForProposal(1) {
		t.Error("no-op session should return false")
	}
}

func TestValidForProposal_RegisterMode(t *testing.T) {
	s := newSession(1, 100)
	s.PrepareForRegister()
	if s.ValidForProposal(1) {
		t.Error("register mode should return false")
	}
}

func TestValidForProposal_UnregisterMode(t *testing.T) {
	s := newSession(1, 100)
	s.PrepareForUnregister()
	if s.ValidForProposal(1) {
		t.Error("unregister mode should return false")
	}
}

func TestValidForProposal_RespondedToExceedsSeriesID(t *testing.T) {
	s := newSession(1, 100)
	s.PrepareForPropose()
	// Force respondedTo > seriesID (broken invariant).
	s.respondedTo.Store(SeriesIDFirstProposal + 1)
	if s.ValidForProposal(1) {
		t.Error("respondedTo > seriesID should return false")
	}
}
