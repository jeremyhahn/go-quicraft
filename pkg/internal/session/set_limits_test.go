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
	"context"
	"errors"
	"testing"

	"github.com/jeremyhahn/go-quicraft/pkg/sm"
)

// mustRegister registers clientIDs first..first+n-1 under ownerID and fails the
// test on the first refusal.
func mustRegister(ctx context.Context, t *testing.T, m *Manager, first, n, ownerID uint64) {
	t.Helper()
	if err := ctx.Err(); err != nil {
		t.Fatalf("context done before register: %v", err)
	}
	for id := first; id < first+n; id++ {
		if err := m.Register(id, ownerID, id); err != nil {
			t.Fatalf("Register(%d, owner %d) = %v, want nil", id, ownerID, err)
		}
	}
}

// requireLimitExceeded asserts that registering clientID under ownerID is
// refused with ErrSessionLimitExceeded and leaves the table unchanged.
func requireLimitExceeded(ctx context.Context, t *testing.T, m *Manager, clientID, ownerID uint64) {
	t.Helper()
	if err := ctx.Err(); err != nil {
		t.Fatalf("context done before register: %v", err)
	}
	before := m.Count()
	if err := m.Register(clientID, ownerID, clientID); !errors.Is(err, ErrSessionLimitExceeded) {
		t.Fatalf("Register(%d, owner %d) = %v, want ErrSessionLimitExceeded", clientID, ownerID, err)
	}
	if m.IsRegistered(clientID) {
		t.Fatalf("refused client %d is registered", clientID)
	}
	if got := m.Count(); got != before {
		t.Fatalf("Count after refusal = %d, want %d", got, before)
	}
}

// TestSetLimits_ZeroTotalAndResponsesKeepPrevious pins (a): a zero maxTotal or
// maxResponses is ignored, so the previous values stay in force.
func TestSetLimits_ZeroTotalAndResponsesKeepPrevious(t *testing.T) {
	ctx := t.Context()
	for _, perOwner := range []uint64{0, 3} {
		m := NewManager(4, 7, 2)
		m.SetLimits(0, 0, perOwner)
		if got := m.MaxResponses(); got != 7 {
			t.Fatalf("perOwner=%d: MaxResponses = %d, want 7", perOwner, got)
		}
		// Ownerless sessions are bounded only by the total, which must still be 4.
		mustRegister(ctx, t, m, 1, 4, noOwnerID)
		requireLimitExceeded(ctx, t, m, 5, noOwnerID)
	}
}

// TestSetLimits_PerOwnerZeroDisablesThenReEnables pins (b): the per-owner cap
// is assigned unconditionally, so zero turns it off, and a later non-zero cap
// is enforced on the next Register.
func TestSetLimits_PerOwnerZeroDisablesThenReEnables(t *testing.T) {
	ctx := t.Context()
	const owner, other = 7, 8
	m := NewManager(100, 8, 2)
	mustRegister(ctx, t, m, 1, 2, owner)
	requireLimitExceeded(ctx, t, m, 3, owner)

	m.SetLimits(0, 0, 0)
	mustRegister(ctx, t, m, 3, 3, owner) // five sessions, past the old cap of 2

	m.SetLimits(0, 0, 4)
	requireLimitExceeded(ctx, t, m, 6, owner)
	mustRegister(ctx, t, m, 10, 1, other) // the cap is per owner
	for id := uint64(1); id <= 5; id++ {
		if !m.IsRegistered(id) {
			t.Fatalf("client %d evicted by a lowered per-owner cap", id)
		}
	}
	for _, id := range []uint64{1, 2} {
		if err := m.Unregister(id); err != nil {
			t.Fatalf("Unregister(%d) = %v", id, err)
		}
	}
	mustRegister(ctx, t, m, 6, 1, owner) // owner now holds 3 < 4
	requireLimitExceeded(ctx, t, m, 7, owner)
}

// TestSetLimits_LoweredTotalEvictsNobody pins (c): lowering maxTotal below
// Count leaves every session registered and only stops new registrations
// until enough sessions leave.
func TestSetLimits_LoweredTotalEvictsNobody(t *testing.T) {
	ctx := t.Context()
	m := NewManager(10, 8, 0)
	mustRegister(ctx, t, m, 1, 6, noOwnerID)

	m.SetLimits(3, 0, 0)
	if got := m.Count(); got != 6 {
		t.Fatalf("Count after lowering total = %d, want 6", got)
	}
	for id := uint64(1); id <= 6; id++ {
		if !m.IsRegistered(id) {
			t.Fatalf("client %d evicted by a lowered total", id)
		}
	}
	requireLimitExceeded(ctx, t, m, 20, noOwnerID)

	// Down to 3 sessions: still at the bound, still refused.
	for id := uint64(1); id <= 3; id++ {
		if err := m.Unregister(id); err != nil {
			t.Fatalf("Unregister(%d) = %v", id, err)
		}
	}
	requireLimitExceeded(ctx, t, m, 20, noOwnerID)

	if err := m.Unregister(4); err != nil {
		t.Fatalf("Unregister(4) = %v", err)
	}
	mustRegister(ctx, t, m, 20, 1, noOwnerID)
	requireLimitExceeded(ctx, t, m, 21, noOwnerID)
}

// TestSetLimits_LoweredResponsesRefuseButKeepHeld pins (d): a lowered
// maxResponses refuses new results at the new bound while the responses a
// session already holds are still served by CheckDuplicate.
func TestSetLimits_LoweredResponsesRefuseButKeepHeld(t *testing.T) {
	const client = 1
	for _, newMax := range []uint64{3, 2} {
		m := NewManager(16, 8, 0)
		if err := m.Register(client, noOwnerID, 1); err != nil {
			t.Fatalf("Register = %v", err)
		}
		for sid := uint64(1); sid <= 3; sid++ {
			if err := m.RecordResult(client, sid, 0, 1+sid, sm.Result{Value: 100 + sid}); err != nil {
				t.Fatalf("RecordResult(series %d) = %v", sid, err)
			}
		}
		if err := m.CanRecord(client, 0, 0); err != nil {
			t.Fatalf("CanRecord before lowering = %v, want nil", err)
		}

		m.SetLimits(0, newMax, 0)
		if got := m.MaxResponses(); got != newMax {
			t.Fatalf("MaxResponses = %d, want %d", got, newMax)
		}
		if err := m.CanRecord(client, 0, 0); !errors.Is(err, ErrResponseLimitExceeded) {
			t.Fatalf("max=%d: CanRecord = %v, want ErrResponseLimitExceeded", newMax, err)
		}
		if err := m.RecordResult(client, 4, 0, 10, sm.Result{Value: 104}); !errors.Is(err, ErrResponseLimitExceeded) {
			t.Fatalf("max=%d: RecordResult = %v, want ErrResponseLimitExceeded", newMax, err)
		}
		for sid := uint64(1); sid <= 3; sid++ {
			res, dup, err := m.CheckDuplicate(client, sid, 11)
			if err != nil || !dup || res.Value != 100+sid {
				t.Fatalf("max=%d: CheckDuplicate(series %d) = (%v, %v, %v), want (%d, true, nil)",
					newMax, sid, res.Value, dup, err, 100+sid)
			}
		}
		if _, dup, err := m.CheckDuplicate(client, 4, 12); err != nil || dup {
			t.Fatalf("max=%d: refused series 4 was cached: dup=%v err=%v", newMax, dup, err)
		}
	}
}

// TestSetLimits_RaisedTotalAdmitsAtOnce pins (e): raising maxTotal admits new
// registrations on the very next Register.
func TestSetLimits_RaisedTotalAdmitsAtOnce(t *testing.T) {
	ctx := t.Context()
	m := NewManager(2, 8, 0)
	mustRegister(ctx, t, m, 1, 2, noOwnerID)
	requireLimitExceeded(ctx, t, m, 3, noOwnerID)

	m.SetLimits(5, 0, 0)
	mustRegister(ctx, t, m, 3, 3, noOwnerID)
	requireLimitExceeded(ctx, t, m, 6, noOwnerID)
	if got := m.Count(); got != 5 {
		t.Fatalf("Count = %d, want 5", got)
	}
}
