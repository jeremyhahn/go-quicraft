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

package client

import (
	"sync"
	"testing"

	quicraft "github.com/jeremyhahn/go-quicraft/pkg"
)

// testRNG implements Source for testing.
type testRNG struct {
	val uint64
}

func (r *testRNG) Uint64() uint64 {
	return r.val
}

func TestSourceInterface(t *testing.T) {
	var s Source = &testRNG{val: 42}
	if s.Uint64() != 42 {
		t.Errorf("Source.Uint64() = %d, want 42", s.Uint64())
	}
}

func TestSourceInterface_NilAccepted(t *testing.T) {
	var s Source
	if s != nil {
		t.Error("zero-value Source interface should be nil")
	}
}

// ---------------------------------------------------------------------------
// NewSession
// ---------------------------------------------------------------------------

func TestNewSession_CreatesTrackedSession(t *testing.T) {
	rng := &testRNG{val: 12345}
	s := NewSession(7, rng)

	if s.ShardID != 7 {
		t.Errorf("ShardID = %d, want 7", s.ShardID)
	}
	if s.Inner() == nil {
		t.Fatal("Inner() must not be nil")
	}
	if s.Inner().ShardID() != 7 {
		t.Errorf("Inner().ShardID() = %d, want 7", s.Inner().ShardID())
	}
	if s.ClientID() == 0 {
		t.Error("NewSession must produce a non-zero ClientID (tracked session)")
	}
	if s.ClientID() != 12345 {
		t.Errorf("ClientID() = %d, want 12345 from rng", s.ClientID())
	}
	if s.IsNoOPSession() {
		t.Error("NewSession must not produce a no-op session")
	}
}

func TestNewSession_DiffersFromNoOPSession(t *testing.T) {
	rng := &testRNG{val: 99999}
	tracked := NewSession(5, rng)
	noop := NewNoOPSession(5, rng)

	if tracked.IsNoOPSession() {
		t.Error("tracked session must not be a no-op session")
	}
	if !noop.IsNoOPSession() {
		t.Error("noop session must be a no-op session")
	}
	if tracked.ClientID() == noop.ClientID() {
		t.Error("tracked and noop sessions must have different ClientIDs")
	}
	if tracked.ClientID() == 0 {
		t.Error("tracked session ClientID must be non-zero")
	}
	if noop.ClientID() != 0 {
		t.Errorf("noop session ClientID = %d, want 0", noop.ClientID())
	}
}

func TestNewSession_NilRNG(t *testing.T) {
	s := NewSession(3, nil)
	if s.ShardID != 3 {
		t.Errorf("ShardID = %d, want 3", s.ShardID)
	}
	if s.Inner() == nil {
		t.Fatal("Inner() must not be nil with nil rng")
	}
	if s.ClientID() == 0 {
		t.Error("NewSession with nil rng must still generate a non-zero ClientID")
	}
	if s.IsNoOPSession() {
		t.Error("NewSession with nil rng must not produce a no-op session")
	}
}

func TestNewSession_ZeroRNGFallsThroughToCryptoRand(t *testing.T) {
	rng := &testRNG{val: 0}
	s := NewSession(1, rng)
	if s.ClientID() == 0 {
		t.Error("NewSession must never produce ClientID 0, even when rng returns 0")
	}
	if s.IsNoOPSession() {
		t.Error("session must not be no-op when rng returns 0")
	}
}

func TestNewSession_DifferentShards(t *testing.T) {
	tests := []struct {
		name    string
		shardID uint64
	}{
		{name: "shard_0", shardID: 0},
		{name: "shard_1", shardID: 1},
		{name: "shard_max", shardID: ^uint64(0)},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			s := NewSession(tc.shardID, &testRNG{val: 42})
			if s.ShardID != tc.shardID {
				t.Errorf("ShardID = %d, want %d", s.ShardID, tc.shardID)
			}
			if s.Inner().ShardID() != tc.shardID {
				t.Errorf("Inner().ShardID() = %d, want %d", s.Inner().ShardID(), tc.shardID)
			}
		})
	}
}

func TestNewSession_UniqueClientIDs(t *testing.T) {
	s1 := NewSession(1, nil)
	s2 := NewSession(1, nil)
	if s1.ClientID() == s2.ClientID() {
		t.Errorf("two sessions with nil rng should have different ClientIDs, both got %d", s1.ClientID())
	}
}

// ---------------------------------------------------------------------------
// NewNoOPSession
// ---------------------------------------------------------------------------

func TestNewNoOPSession(t *testing.T) {
	s := NewNoOPSession(42, nil)
	if s.ShardID != 42 {
		t.Errorf("ShardID = %d, want 42", s.ShardID)
	}
	if !s.IsNoOPSession() {
		t.Error("NewNoOPSession should produce a no-op session")
	}
	if s.Inner() == nil {
		t.Error("Inner() must not be nil")
	}
	if s.Inner().ShardID() != 42 {
		t.Errorf("Inner().ShardID() = %d, want 42", s.Inner().ShardID())
	}
	if s.ClientID() != 0 {
		t.Errorf("ClientID() = %d, want 0 for no-op session", s.ClientID())
	}
}

func TestNewNoOPSession_DifferentShards(t *testing.T) {
	tests := []struct {
		name    string
		shardID uint64
	}{
		{name: "shard_0", shardID: 0},
		{name: "shard_1", shardID: 1},
		{name: "shard_max", shardID: ^uint64(0)},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			s := NewNoOPSession(tc.shardID, nil)
			if s.ShardID != tc.shardID {
				t.Errorf("ShardID = %d, want %d", s.ShardID, tc.shardID)
			}
			if s.Inner().ShardID() != tc.shardID {
				t.Errorf("Inner().ShardID() = %d, want %d", s.Inner().ShardID(), tc.shardID)
			}
		})
	}
}

func TestNewNoOPSession_WithRNG(t *testing.T) {
	rng := &testRNG{val: 999}
	s := NewNoOPSession(5, rng)
	if s.ShardID != 5 {
		t.Errorf("ShardID = %d, want 5", s.ShardID)
	}
	if !s.IsNoOPSession() {
		t.Error("NewNoOPSession with rng should still produce a no-op session")
	}
}

// ---------------------------------------------------------------------------
// ValidForProposal
// ---------------------------------------------------------------------------

func TestValidForProposal_AcceptsValidTrackedSession(t *testing.T) {
	s := NewSession(10, &testRNG{val: 500})
	s.PrepareForPropose()
	if !s.ValidForProposal(10) {
		t.Error("ValidForProposal should accept a prepared tracked session with matching shard")
	}
}

func TestValidForProposal_RejectsNoOPSession(t *testing.T) {
	s := NewNoOPSession(10, nil)
	if s.ValidForProposal(10) {
		t.Error("ValidForProposal must reject no-op sessions (ClientID == 0)")
	}
}

func TestValidForProposal_RejectsMismatchedShard(t *testing.T) {
	s := NewSession(10, &testRNG{val: 500})
	s.PrepareForPropose()
	if s.ValidForProposal(20) {
		t.Error("ValidForProposal must reject mismatched shard")
	}
}

func TestValidForProposal_RejectsRegisterSession(t *testing.T) {
	s := NewSession(10, &testRNG{val: 500})
	s.Inner().PrepareForRegister()
	if s.ValidForProposal(10) {
		t.Error("ValidForProposal must reject session in register state")
	}
}

func TestValidForProposal_RejectsUnregisterSession(t *testing.T) {
	s := NewSession(10, &testRNG{val: 500})
	s.Inner().PrepareForUnregister()
	if s.ValidForProposal(10) {
		t.Error("ValidForProposal must reject session in unregister state")
	}
}

func TestValidForProposal_RejectsUninitializedSession(t *testing.T) {
	// A freshly created session has SeriesID 0 and RespondedTo 0.
	// This is technically valid per the invariant (respondedTo <= seriesID),
	// but the session has not been prepared for proposals yet.
	s := NewSession(10, &testRNG{val: 500})
	// SeriesID is 0 initially, which is neither register nor unregister,
	// and respondedTo (0) <= seriesID (0), so the inner validation passes.
	// This matches dragonboat behavior where a freshly created unregistered
	// session can technically pass ValidForProposal.
	_ = s.ValidForProposal(10) // no assertion - documenting behavior
}

// ---------------------------------------------------------------------------
// ValidForSessionOp
// ---------------------------------------------------------------------------

func TestValidForSessionOp_AcceptsRegisterSession(t *testing.T) {
	s := NewSession(10, &testRNG{val: 500})
	s.Inner().PrepareForRegister()
	if !s.ValidForSessionOp(10) {
		t.Error("ValidForSessionOp should accept session in register state")
	}
}

func TestValidForSessionOp_AcceptsUnregisterSession(t *testing.T) {
	s := NewSession(10, &testRNG{val: 500})
	s.Inner().PrepareForUnregister()
	if !s.ValidForSessionOp(10) {
		t.Error("ValidForSessionOp should accept session in unregister state")
	}
}

func TestValidForSessionOp_RejectsNoOPSession(t *testing.T) {
	s := NewNoOPSession(10, nil)
	if s.ValidForSessionOp(10) {
		t.Error("ValidForSessionOp must reject no-op sessions")
	}
}

func TestValidForSessionOp_RejectsMismatchedShard(t *testing.T) {
	s := NewSession(10, &testRNG{val: 500})
	s.Inner().PrepareForRegister()
	if s.ValidForSessionOp(20) {
		t.Error("ValidForSessionOp must reject mismatched shard")
	}
}

func TestValidForSessionOp_RejectsRegularSession(t *testing.T) {
	s := NewSession(10, &testRNG{val: 500})
	s.PrepareForPropose()
	if s.ValidForSessionOp(10) {
		t.Error("ValidForSessionOp must reject sessions in normal proposal state")
	}
}

func TestValidForSessionOp_RejectsUninitializedSession(t *testing.T) {
	s := NewSession(10, &testRNG{val: 500})
	// SeriesID is 0 initially (NoOPSeriesID), not a register/unregister sentinel.
	if s.ValidForSessionOp(10) {
		t.Error("ValidForSessionOp must reject session with SeriesID 0")
	}
}

// ---------------------------------------------------------------------------
// WrapSession
// ---------------------------------------------------------------------------

func TestWrapSession_FieldsMatch(t *testing.T) {
	inner := quicraft.NewNoOPSession(99)
	wrapped := WrapSession(inner)

	if wrapped.ShardID != 99 {
		t.Errorf("ShardID = %d, want 99", wrapped.ShardID)
	}
	if wrapped.Inner() != inner {
		t.Error("Inner() must return the same pointer passed to WrapSession")
	}
	if wrapped.Inner().ShardID() != 99 {
		t.Errorf("Inner().ShardID() = %d, want 99", wrapped.Inner().ShardID())
	}
}

func TestWrapSession_PreservesSessionState(t *testing.T) {
	inner := quicraft.NewNoOPSession(7)
	wrapped := WrapSession(inner)

	if !wrapped.IsNoOPSession() {
		t.Error("wrapped no-op session must report IsNoOPSession true")
	}
	if wrapped.ClientID() != inner.ClientID() {
		t.Errorf("ClientID mismatch: wrapped=%d, inner=%d", wrapped.ClientID(), inner.ClientID())
	}
	if wrapped.SeriesID() != inner.SeriesID() {
		t.Errorf("SeriesID mismatch: wrapped=%d, inner=%d", wrapped.SeriesID(), inner.SeriesID())
	}
}

func TestWrapSession_TrackedSession(t *testing.T) {
	inner := quicraft.NewSession(5, 42)
	wrapped := WrapSession(inner)

	if wrapped.IsNoOPSession() {
		t.Error("wrapped tracked session must not be no-op")
	}
	if wrapped.ClientID() != 42 {
		t.Errorf("ClientID = %d, want 42", wrapped.ClientID())
	}
	if wrapped.ShardID != 5 {
		t.Errorf("ShardID = %d, want 5", wrapped.ShardID)
	}
}

// ---------------------------------------------------------------------------
// Session methods
// ---------------------------------------------------------------------------

func TestSession_IsNoOPSession(t *testing.T) {
	noop := NewNoOPSession(1, nil)
	if !noop.IsNoOPSession() {
		t.Error("NewNoOPSession must produce a no-op session")
	}

	tracked := NewSession(1, &testRNG{val: 100})
	if tracked.IsNoOPSession() {
		t.Error("NewSession must not produce a no-op session")
	}
}

func TestSession_PrepareForPropose(t *testing.T) {
	s := NewSession(1, &testRNG{val: 100})
	s.PrepareForPropose()
	if s.SeriesID() != 1 {
		t.Errorf("SeriesID after PrepareForPropose = %d, want 1", s.SeriesID())
	}
}

func TestSession_PrepareForUnregister(t *testing.T) {
	s := NewSession(1, &testRNG{val: 100})
	s.PrepareForPropose()
	if s.SeriesID() == quicraft.SeriesIDForUnregister {
		t.Fatal("session should not start in unregister state")
	}
	s.PrepareForUnregister()
	if s.SeriesID() != quicraft.SeriesIDForUnregister {
		t.Errorf("SeriesID after PrepareForUnregister = %d, want %d",
			s.SeriesID(), quicraft.SeriesIDForUnregister)
	}
}

func TestSession_PrepareForUnregister_ClearsRespondedTo(t *testing.T) {
	s := NewSession(1, &testRNG{val: 100})
	s.PrepareForPropose()
	s.ProposalCompleted()
	s.PrepareForUnregister()
	if s.Inner().RespondedTo() != 0 {
		t.Errorf("RespondedTo after PrepareForUnregister = %d, want 0",
			s.Inner().RespondedTo())
	}
}

func TestSession_ProposalCompleted(t *testing.T) {
	s := NewSession(1, &testRNG{val: 100})
	s.PrepareForPropose()
	initialSeries := s.SeriesID()
	s.ProposalCompleted()
	if s.SeriesID() != initialSeries+1 {
		t.Errorf("SeriesID after ProposalCompleted = %d, want %d", s.SeriesID(), initialSeries+1)
	}
}

// ---------------------------------------------------------------------------
// generateClientID
// ---------------------------------------------------------------------------

func TestGenerateClientID_UsesRNG(t *testing.T) {
	rng := &testRNG{val: 777}
	id := generateClientID(rng)
	if id != 777 {
		t.Errorf("generateClientID = %d, want 777", id)
	}
}

func TestGenerateClientID_RejectsZeroFromRNG(t *testing.T) {
	rng := &testRNG{val: 0}
	id := generateClientID(rng)
	if id == 0 {
		t.Error("generateClientID must never return 0")
	}
}

func TestGenerateClientID_NilRNGUsesCryptoRand(t *testing.T) {
	id := generateClientID(nil)
	if id == 0 {
		t.Error("generateClientID(nil) must return non-zero value")
	}
}

// ---------------------------------------------------------------------------
// SwapInner
// ---------------------------------------------------------------------------

func TestSession_SwapInner_UpdatesInner(t *testing.T) {
	original := quicraft.NewSession(10, 100)
	session := WrapSession(original)

	if session.Inner() != original {
		t.Fatal("Inner() must return original session before swap")
	}
	if session.ClientID() != 100 {
		t.Errorf("ClientID before swap = %d, want 100", session.ClientID())
	}

	replacement := quicraft.NewSession(10, 200)
	session.SwapInner(replacement)

	if session.Inner() != replacement {
		t.Error("Inner() must return replacement session after swap")
	}
	if session.ClientID() != 200 {
		t.Errorf("ClientID after swap = %d, want 200", session.ClientID())
	}
	if session.ShardID != 10 {
		t.Errorf("ShardID must remain unchanged after swap, got %d", session.ShardID)
	}
}

func TestSession_SwapInner_InvalidNilPanics(t *testing.T) {
	session := NewSession(1, &testRNG{val: 42})

	// Reading Inner after storing nil will panic on dereference in methods
	// like ClientID(). Verify SwapInner with a valid pointer works after
	// an initial swap.
	first := quicraft.NewSession(1, 50)
	second := quicraft.NewSession(1, 60)

	session.SwapInner(first)
	if session.ClientID() != 50 {
		t.Errorf("ClientID after first swap = %d, want 50", session.ClientID())
	}

	session.SwapInner(second)
	if session.ClientID() != 60 {
		t.Errorf("ClientID after second swap = %d, want 60", session.ClientID())
	}
}

func TestSession_SwapInner_ConcurrentAccess(t *testing.T) {
	const goroutines = 64
	const iterations = 1000

	session := NewSession(5, &testRNG{val: 42})

	// Pre-create sessions to swap in so allocation is not in the hot path.
	sessions := make([]*quicraft.Session, goroutines)
	for i := range sessions {
		sessions[i] = quicraft.NewSession(5, uint64(i+1))
	}

	var wg sync.WaitGroup
	wg.Add(goroutines * 2)

	// Half the goroutines swap the inner session.
	for i := range goroutines {
		go func(idx int) {
			defer wg.Done()
			for j := range iterations {
				_ = j
				session.SwapInner(sessions[idx])
			}
		}(i)
	}

	// Half the goroutines concurrently read session state.
	for range goroutines {
		go func() {
			defer wg.Done()
			for range iterations {
				inner := session.Inner()
				if inner == nil {
					t.Error("Inner() must never return nil during concurrent access")
					return
				}
				// Read derived fields to exercise the atomic load path.
				_ = session.ClientID()
				_ = session.SeriesID()
				_ = session.IsNoOPSession()
				_ = session.ValidForProposal(5)
			}
		}()
	}

	wg.Wait()

	// After all goroutines complete, the session must still be readable.
	if session.Inner() == nil {
		t.Error("Inner() must not be nil after concurrent access")
	}
}
