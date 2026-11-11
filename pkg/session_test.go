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
	s := newSession(10, 20, 0)
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
	if s.IsNoOPSession() {
		t.Error("session with a non-zero clientID should not be a no-op session")
	}
}

func TestNewSession_InvalidZeroClientID(t *testing.T) {
	s := newSession(5, 0, 0)
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

// ---------------------------------------------------------------------------
// PrepareForPropose
// ---------------------------------------------------------------------------

func TestProposalCompleted_Monotonic(t *testing.T) {
	s := newSession(1, 100, 0)
	s.PrepareForPropose()
	for i := uint64(0); i < 100; i++ {
		expected := SeriesIDFirstProposal + i
		if s.SeriesID() != expected {
			t.Fatalf("iteration %d: SeriesID = %d, want %d", i, s.SeriesID(), expected)
		}
		s.ProposalCompleted()
		if s.RespondedTo() != expected {
			t.Fatalf("iteration %d: RespondedTo = %d, want %d", i, s.RespondedTo(), expected)
		}
	}
}

func TestProposalCompleted_ConcurrentInvariant(t *testing.T) {
	// Verify that after N concurrent completions, the invariant
	// respondedTo == seriesID - 1 holds and seriesID advanced exactly N.
	s := newSession(1, 100, 0)
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
	s := newSession(1, 100, 0)
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

// ---------------------------------------------------------------------------
// PrepareForUnregister
// ---------------------------------------------------------------------------

func TestPrepareForUnregister_SetsSentinel(t *testing.T) {
	s := newSession(1, 100, 0)
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

// ---------------------------------------------------------------------------
// ValidForSessionOp
// ---------------------------------------------------------------------------

func TestValidForSessionOp(t *testing.T) {
	cases := []struct {
		name    string
		session *Session
		shardID uint64
		want    bool
	}{
		{"own shard", newSession(5, 100, 0), 5, true},
		{"different shard", newSession(5, 100, 0), 10, false},
		{"no-op session", NewNoOPSession(5), 5, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := tc.session.ValidForSessionOp(tc.shardID); got != tc.want {
				t.Errorf("ValidForSessionOp(%d) = %v, want %v", tc.shardID, got, tc.want)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// SetRespondedTo
// ---------------------------------------------------------------------------

func TestSetRespondedTo_MonotonicallyAdvances(t *testing.T) {
	s := newSession(1, 100, 0)

	s.SetRespondedTo(5)
	if s.RespondedTo() != 5 {
		t.Errorf("RespondedTo = %d, want 5", s.RespondedTo())
	}

	// Setting the same value again should leave it unchanged.
	s.SetRespondedTo(5)
	if s.RespondedTo() != 5 {
		t.Errorf("RespondedTo = %d, want 5 (same value)", s.RespondedTo())
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
	s := newSession(1, 100, 0)
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

// ---------------------------------------------------------------------------
// ValidForProposal
// ---------------------------------------------------------------------------

func TestValidForProposal(t *testing.T) {
	proposing := func() *Session {
		s := newSession(1, 100, 0)
		s.PrepareForPropose()
		return s
	}
	registering := newSession(1, 100, 0)
	registering.PrepareForRegister()
	unregistering := newSession(1, 100, 0)
	unregistering.PrepareForUnregister()
	// Force respondedTo > seriesID (broken invariant).
	broken := proposing()
	broken.respondedTo.Store(SeriesIDFirstProposal + 1)

	cases := []struct {
		name    string
		session *Session
		shardID uint64
		want    bool
	}{
		{"valid session", proposing(), 1, true},
		{"wrong shard", proposing(), 2, false},
		{"no-op session", NewNoOPSession(1), 1, false},
		{"register mode", registering, 1, false},
		{"unregister mode", unregistering, 1, false},
		{"respondedTo exceeds seriesID", broken, 1, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := tc.session.ValidForProposal(tc.shardID); got != tc.want {
				t.Errorf("ValidForProposal(%d) = %v, want %v", tc.shardID, got, tc.want)
			}
		})
	}
}
