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
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// TestSession_InFlightReservation pins the reservation primitives:
// one series at a time, released only by the series that holds it, and
// released by ProposalCompleted as it advances.
func TestSession_InFlightReservation(t *testing.T) {
	s := newSession(1, 100, 0)
	s.PrepareForPropose()
	series := s.SeriesID()

	if !s.beginProposal(series) {
		t.Fatal("beginProposal on an idle session = false")
	}
	if s.beginProposal(series) {
		t.Fatal("second beginProposal while in flight = true")
	}
	// A release for a series that is not the one held is a no-op: it belongs
	// to a request the session has already moved past.
	s.endProposal(series + 1)
	if s.beginProposal(series) {
		t.Fatal("endProposal of a foreign series cleared the reservation")
	}
	// ProposalCompleted advances the series and releases the reservation.
	s.ProposalCompleted()
	if got := s.SeriesID(); got != series+1 {
		t.Fatalf("SeriesID after ProposalCompleted = %d, want %d", got, series+1)
	}
	if !s.beginProposal(series + 1) {
		t.Fatal("beginProposal after ProposalCompleted = false: the reservation was not released")
	}
	// A late endProposal for the COMPLETED series must not clear the new one.
	s.endProposal(series)
	if s.beginProposal(series + 1) {
		t.Fatal("a late release of the previous series cleared the current reservation")
	}
	s.endProposal(series + 1)
	if !s.beginProposal(series + 1) {
		t.Fatal("endProposal of the held series did not release it")
	}
}

// TestRequestState_ErrorCompletionReleasesSession pins that a failed
// session proposal releases its series BEFORE the caller can observe the
// error, since the documented recovery is a retry on the same session, and
// that Release does the same for a caller that gives up.
func TestRequestState_ErrorCompletionReleasesSession(t *testing.T) {
	pool := NewRequestStatePool()
	s := newSession(1, 100, 0)
	s.PrepareForPropose()
	series := s.SeriesID()

	// Error completion.
	if !s.beginProposal(series) {
		t.Fatal("beginProposal = false")
	}
	rs := newRequestState(pool, 1, time.Now().Add(time.Second), false)
	rs.bindSession(s, series)
	rs.complete(RequestResult{Err: ErrAborted})
	if !s.beginProposal(series) {
		t.Fatal("series still reserved after an error completion; the documented retry would be refused")
	}
	s.endProposal(series)
	rs.Release()

	// Success completion keeps the reservation for ProposalCompleted...
	if !s.beginProposal(series) {
		t.Fatal("beginProposal = false")
	}
	rs = newRequestState(pool, 2, time.Now().Add(time.Second), false)
	rs.bindSession(s, series)
	rs.complete(RequestResult{Value: 7})
	if s.beginProposal(series) {
		t.Fatal("a successful completion released the series before ProposalCompleted")
	}
	// ...and Release is the catch-all for a caller that never calls it.
	rs.Release()
	if !s.beginProposal(series) {
		t.Fatal("Release did not release the series")
	}
	s.endProposal(series)

	// A recycled RequestState carries no session into its next use.
	rs = newRequestState(pool, 3, time.Now().Add(time.Second), false)
	if rs.session.Load() != nil || rs.sessionSeries.Load() != 0 {
		t.Fatal("newRequestState did not reset the session binding")
	}
}

// TestProposeWithSession_RefusesConcurrentProposal is the host-level pin for
// the session reservation. Two concurrent proposals on one session would both
// carry the same series number; the state machine would apply the first and
// answer the second with the first's result, and both callers would be told
// their command ran. The second is refused with ErrSessionBusy until the first
// has completed.
func TestProposeWithSession_RefusesConcurrentProposal(t *testing.T) {
	h := newTestHost(t)
	members := map[uint64]string{1: "127.0.0.1:5000"}
	h.StartShard(members, false, testCreateFunc, testShardConfig(1, 1))
	waitForLeader(t, h, 1)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	session, err := h.GetNewSession(ctx, 1)
	if err != nil {
		t.Fatalf("GetNewSession: %v", err)
	}

	rs1, err := h.ProposeWithSession(ctx, session, []byte("debit A"))
	if err != nil {
		t.Fatalf("first ProposeWithSession: %v", err)
	}
	defer rs1.Release()

	// Refused while the first is in flight — and still refused after it has
	// APPLIED, until the caller acknowledges it: the series is not advanced
	// until ProposalCompleted, so a second command would still collide.
	if _, err := h.ProposeWithSession(ctx, session, []byte("credit B")); !errors.Is(err, ErrSessionBusy) {
		t.Fatalf("second ProposeWithSession while the first is in flight = %v, want ErrSessionBusy", err)
	}
	if !Temporary(ErrSessionBusy) {
		t.Fatalf("Temporary(ErrSessionBusy) = false; the condition clears on its own")
	}
	first, err := rs1.Result()
	if err != nil || first.Err != nil {
		t.Fatalf("first proposal result = %v / %v", err, first.Err)
	}
	if _, err := h.ProposeWithSession(ctx, session, []byte("credit B")); !errors.Is(err, ErrSessionBusy) {
		t.Fatalf("second ProposeWithSession after the first applied but before ProposalCompleted = %v, want ErrSessionBusy", err)
	}

	// Acknowledged: the next proposal is admitted and carries the next series.
	session.ProposalCompleted()
	rs2, err := h.ProposeWithSession(ctx, session, []byte("credit B"))
	if err != nil {
		t.Fatalf("ProposeWithSession after ProposalCompleted: %v", err)
	}
	defer rs2.Release()
	second, err := rs2.Result()
	if err != nil || second.Err != nil {
		t.Fatalf("second proposal result = %v / %v", err, second.Err)
	}
	// The mock state machine answers with the applied entry's index, so two
	// commands that both ran answer with two different indexes; a deduplicated
	// second command would have echoed the first's.
	if second.Value == first.Value {
		t.Fatalf("second proposal answered with the first's result (%d): it was deduplicated, not applied", first.Value)
	}
	session.ProposalCompleted()
}

// TestProposeWithSession_AdmissionErrorReleasesReservation pins that a
// proposal refused before it is registered — here, no known leader — leaves
// the session free for the documented retry.
func TestProposeWithSession_AdmissionErrorReleasesReservation(t *testing.T) {
	h := newTestHost(t)
	members := map[uint64]string{1: "127.0.0.1:5000"}
	h.StartShard(members, false, testCreateFunc, testShardConfig(1, 1))
	// No leader yet: ProposeWithSession fails fast at admission.

	session := newSession(1, 100, 0)
	session.PrepareForPropose()
	for i := range 3 {
		_, err := h.ProposeWithSession(context.Background(), session, []byte("cmd"))
		if err == nil {
			t.Fatalf("attempt %d: ProposeWithSession succeeded with no leader", i)
		}
		if errors.Is(err, ErrSessionBusy) {
			t.Fatalf("attempt %d: ErrSessionBusy after an admission failure; the reservation leaked", i)
		}
	}
}

// TestSyncProposeWithSession_ReleaseWithoutCompletedAdmitsNext pins the
// pre-existing contract for a caller that releases a successful request
// without calling ProposalCompleted: the session is not advanced (its series
// is stale, as documented), but it is not stuck either.
func TestSyncProposeWithSession_ReleaseWithoutCompletedAdmitsNext(t *testing.T) {
	h := newTestHost(t)
	members := map[uint64]string{1: "127.0.0.1:5000"}
	h.StartShard(members, false, testCreateFunc, testShardConfig(1, 1))
	waitForLeader(t, h, 1)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	session, err := h.GetNewSession(ctx, 1)
	if err != nil {
		t.Fatalf("GetNewSession: %v", err)
	}
	rs, err := h.ProposeWithSession(ctx, session, []byte("cmd"))
	if err != nil {
		t.Fatalf("ProposeWithSession: %v", err)
	}
	if _, err := rs.Result(); err != nil {
		t.Fatalf("Result: %v", err)
	}
	rs.Release()
	rs2, err := h.ProposeWithSession(ctx, session, []byte("cmd"))
	if err != nil {
		t.Fatalf("ProposeWithSession after Release without ProposalCompleted = %v, want admitted", err)
	}
	rs2.Release()
}

// TestRequestState_ConcurrentReleaseElectsOneReleaser pins the ordering in
// Release: the object is pinned before the releaser is elected, so of two
// Releases that have both pinned, exactly one ends the session reservation
// and recycles the object, and the loser touches nothing. (A second call is
// outside Release's contract; this exercises the pin under the race
// detector, not a guarantee for callers that break it — see Release.)
func TestRequestState_ConcurrentReleaseElectsOneReleaser(t *testing.T) {
	pool := NewRequestStatePool()
	s := newSession(1, 100, 0)
	s.PrepareForPropose()
	series := s.SeriesID()

	for i := 0; i < 2000; i++ {
		if !s.beginProposal(series) {
			t.Fatalf("iteration %d: series still reserved", i)
		}
		rs := newRequestState(pool, uint64(i+1), time.Now().Add(time.Second), false)
		rs.bindSession(s, series)
		// Registered, then completed and removed from its pending map: the
		// state a Sync* caller's deferred Release finds after a result.
		rs.acquireRef()
		rs.complete(RequestResult{Value: 1})
		rs.releaseRef()

		var wg sync.WaitGroup
		for r := 0; r < 2; r++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				rs.Release()
			}()
		}
		wg.Wait()

		if !rs.recycled.Load() {
			t.Fatalf("iteration %d: released with no references outstanding but not recycled", i)
		}
		if got := rs.outstanding.Load(); got != 0 {
			t.Fatalf("iteration %d: outstanding = %d after both Releases, want 0 (a pin leaked)", i, got)
		}
		if got := s.inFlight.Load(); got != 0 {
			t.Fatalf("iteration %d: session still holds series %d in flight", i, got)
		}
		// The next request built on the recycled object starts unbound.
		next := newRequestState(pool, uint64(i+1), time.Now().Add(time.Second), false)
		if next.session.Load() != nil {
			t.Fatalf("iteration %d: recycled object carried a session binding", i)
		}
		next.Release()
	}
}

// TestSession_ValidForProposal_RefusesSeriesZero pins that a session that was
// never registered (series 0) is refused for proposals. Series 0 is the
// reservation's "nothing held" value, so admitting it would let two
// concurrent proposals both reserve and both carry series 0.
func TestSession_ValidForProposal_RefusesSeriesZero(t *testing.T) {
	s := NewSession(1, 100)
	if s.SeriesID() != 0 {
		t.Fatalf("fresh session series = %d, want 0", s.SeriesID())
	}
	if s.ValidForProposal(1) {
		t.Fatal("an unregistered session (series 0) was accepted for a proposal")
	}
	s.PrepareForPropose()
	if !s.ValidForProposal(1) {
		t.Fatal("a registered session at SeriesIDFirstProposal was refused")
	}
}

// TestSession_BeginCurrentProposal_RechecksTheSeries pins the claim against
// ProposalCompleted: the series is read, then claimed, and a completion that
// lands between the two advances the series before it releases the
// reservation — so a claim that succeeded may hold the PREVIOUS series, and
// a proposal stamped with it would be answered as that command's
// retransmission without being applied. The claim is re-checked and retaken.
func TestSession_BeginCurrentProposal_RechecksTheSeries(t *testing.T) {
	s := newSession(1, 100, 0)
	s.PrepareForPropose()
	first := s.SeriesID()

	// The plain path: the current series is claimed.
	got, ok := s.beginCurrentProposal()
	if !ok || got != first {
		t.Fatalf("beginCurrentProposal = (%d, %v), want (%d, true)", got, ok, first)
	}
	// Completion advances the series and releases the reservation.
	s.ProposalCompleted()

	// Model the race: a claim of the OLD series that succeeded because the
	// completion had already released it. beginCurrentProposal must not be
	// fooled by the stale read it started from.
	if !s.beginProposal(first) {
		t.Fatal("fixture: the stale series could not be claimed")
	}
	s.endProposal(first)
	got, ok = s.beginCurrentProposal()
	if !ok || got != first+1 {
		t.Fatalf("beginCurrentProposal after a completion = (%d, %v), want (%d, true)", got, ok, first+1)
	}
	if _, ok := s.beginCurrentProposal(); ok {
		t.Fatal("a second claim was admitted while the first is held")
	}
	s.endProposal(first + 1)

	// Under contention: proposals racing completions never hold a series the
	// session has moved past.
	var wg sync.WaitGroup
	var stale atomic.Int64
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 2000; j++ {
				series, ok := s.beginCurrentProposal()
				if !ok {
					continue
				}
				if s.SeriesID() != series {
					stale.Add(1)
				}
				s.ProposalCompleted()
			}
		}()
	}
	wg.Wait()
	if n := stale.Load(); n != 0 {
		t.Fatalf("%d claims held a series the session had moved past", n)
	}
}
