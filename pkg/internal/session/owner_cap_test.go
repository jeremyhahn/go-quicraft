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
	"errors"
	"testing"
)

// TestOwnerCapStopsOneOwnerExhaustingTheTable is the regression gate for the
// defect the per-owner cap exists to close.
//
// MaxTotalSessions is a SHARD-WIDE bound, and a shard-wide bound cannot defend
// itself: a clientID is eight bytes the caller chooses, so a single principal
// registers under as many of them as it likes until the table is full. From
// that moment every other tenant on the shard is refused at-most-once delivery
// — not by a bug in Raft, but by a limit working exactly as written.
//
// The assertion is deliberately in two halves, and the SECOND half is the one
// that matters. "Owner A is eventually refused" passes against a manager with
// no per-owner cap at all, because A hits the shard-wide limit sooner or later;
// what distinguishes a fixed manager from a broken one is that owner B is still
// admitted afterwards. Without the cap, A has consumed the table and B's
// registration fails too.
func TestOwnerCapStopsOneOwnerExhaustingTheTable(t *testing.T) {
	const (
		ownerA      = 0xA
		ownerB      = 0xB
		maxPerOwner = 4
		// Deliberately far above maxPerOwner*2 so the shard-wide limit is never
		// the reason anything is refused. If this test ever fails because
		// maxTotal was reached, it is measuring the wrong limit.
		maxTotal = 1000
	)

	m := NewManager(maxTotal, 16, maxPerOwner)

	// Owner A registers up to its cap. Every one of these must be admitted:
	// the cap is a ceiling, not a budget the first registration spends.
	for i := 0; i < maxPerOwner; i++ {
		clientID := uint64(1000 + i)
		if err := m.Register(clientID, ownerA, uint64(i+1)); err != nil {
			t.Fatalf("Register(client=%d, owner=A) #%d: %v, want nil "+
				"(the cap is %d, so the first %d must be admitted)",
				clientID, i, err, maxPerOwner, maxPerOwner)
		}
	}

	// One past the cap. A distinct, previously unseen clientID, so nothing but
	// the owner cap can refuse it.
	err := m.Register(2000, ownerA, 100)
	if !errors.Is(err, ErrSessionLimitExceeded) {
		t.Fatalf("Register #%d for owner A = %v, want ErrSessionLimitExceeded. "+
			"One owner is holding %d sessions against a cap of %d; if this "+
			"succeeds the owner can keep going until MaxTotalSessions (%d) is "+
			"exhausted and every other tenant on the shard loses at-most-once "+
			"delivery.", maxPerOwner+1, err, m.Count(), maxPerOwner, maxTotal)
	}

	// The half that actually distinguishes the fix: a DIFFERENT owner is
	// unaffected. A's refusal must be A's alone.
	if err := m.Register(3000, ownerB, 101); err != nil {
		t.Fatalf("Register for owner B = %v, want nil. Owner A hitting its cap "+
			"must not consume anything B needs — that is the whole point of a "+
			"PER-OWNER cap rather than a shard-wide one. B failing here means "+
			"A exhausted the shared table.", err)
	}

	if got, want := m.Count(), maxPerOwner+1; got != want {
		t.Fatalf("Count() = %d, want %d (A's %d plus B's 1)", got, want, maxPerOwner)
	}
}

// TestOwnerCapReleasesOnUnregisterAndExpiry pins that the owner index is not
// write-only.
//
// The cap is counted from a derived secondary index, and a derived index that
// only ever increments is worse than no index at all: it would refuse an owner
// that holds nothing, permanently, and it would do so on whichever replica
// happened to expire a session first. Both removal paths — the explicit one and
// the deterministic-expiry one — must give the slot back.
func TestOwnerCapReleasesOnUnregisterAndExpiry(t *testing.T) {
	const owner = 0xC0FFEE

	t.Run("unregister", func(t *testing.T) {
		m := NewManager(1000, 16, 2)
		if err := m.Register(1, owner, 1); err != nil {
			t.Fatalf("Register(1): %v", err)
		}
		if err := m.Register(2, owner, 2); err != nil {
			t.Fatalf("Register(2): %v", err)
		}
		if err := m.Register(3, owner, 3); !errors.Is(err, ErrSessionLimitExceeded) {
			t.Fatalf("Register at cap = %v, want ErrSessionLimitExceeded", err)
		}
		if err := m.Unregister(1); err != nil {
			t.Fatalf("Unregister: %v", err)
		}
		if err := m.Register(3, owner, 4); err != nil {
			t.Fatalf("Register after Unregister = %v, want nil: the owner "+
				"released one session, so one slot must come back", err)
		}
		// Exactly one slot came back: the owner still holds session 2, so
		// the release decremented its count rather than clearing it.
		if err := m.Register(4, owner, 5); !errors.Is(err, ErrSessionLimitExceeded) {
			t.Fatalf("Register at cap after one release = %v, want "+
				"ErrSessionLimitExceeded", err)
		}
	})

	t.Run("expiry", func(t *testing.T) {
		m := NewManager(1000, 16, 1)
		if err := m.Register(1, owner, 1); err != nil {
			t.Fatalf("Register: %v", err)
		}
		// Expire it: index-driven, so this is the same removal every replica
		// performs at the same point in the log.
		if expired := m.ExpireSessions(1000, 10, 8); len(expired) != 1 {
			t.Fatalf("ExpireSessions removed %d sessions, want 1", len(expired))
		}
		if err := m.Register(2, owner, 1001); err != nil {
			t.Fatalf("Register after expiry = %v, want nil: an expired session "+
				"must release its owner slot, or a long-lived shard eventually "+
				"refuses every registration from an owner holding nothing", err)
		}
	})
}

// TestOwnerCapIgnoresUnsetOwner pins the compatibility guarantee.
//
// Every session registered before Entry.OwnerID existed decodes to owner zero,
// as does every session from an embedder that does not declare owners. Those
// must be exempt. Treating zero as an owner in its own right would pool every
// ownerless session on the shard under one key and refuse them all at the cap —
// which is not a stricter version of the old behaviour, it is a shard that
// stops accepting sessions.
func TestOwnerCapIgnoresUnsetOwner(t *testing.T) {
	m := NewManager(1000, 16, 2)

	// Ten sessions with no declared owner, against a per-owner cap of 2.
	for i := 0; i < 10; i++ {
		if err := m.Register(uint64(i+1), noOwnerID, uint64(i+1)); err != nil {
			t.Fatalf("Register #%d with no owner = %v, want nil: an unset owner "+
				"is not an owner and must not be capped", i, err)
		}
	}
	if got := m.Count(); got != 10 {
		t.Fatalf("Count() = %d, want 10", got)
	}
	if n := len(m.ownerCounts); n != 0 {
		t.Fatalf("ownerCounts holds %d keys, want 0: the zero owner must never "+
			"become a key, or it accumulates every ownerless session on the shard", n)
	}
}

// TestOwnerCapDisabledWhenPolicyIsZero pins that a shard which committed no
// per-owner cap behaves exactly as it did before the field existed.
//
// Zero is the value a membership with no committed session policy decodes to,
// so this is not a hypothetical configuration — it is the state of every shard
// bootstrapped before the policy grew this field.
func TestOwnerCapDisabledWhenPolicyIsZero(t *testing.T) {
	m := NewManager(1000, 16, 0)
	for i := 0; i < 50; i++ {
		if err := m.Register(uint64(i+1), 0xFEED, uint64(i+1)); err != nil {
			t.Fatalf("Register #%d = %v, want nil: maxPerOwner==0 means the cap "+
				"is off, not that nothing is admitted", i, err)
		}
	}
}

// TestOwnerCapSurvivesSnapshotRoundTrip pins the owner through
// MarshalTo/UnmarshalFrom.
//
// A snapshot that loses the owner does not fail. The sessions come back looking
// ownerless, every owner's count comes back at zero, and the recovering replica
// then admits registrations its peers refuse — a state-machine divergence with
// every Raft invariant intact, which is the exact failure mode replicating the
// policy was meant to remove.
//
// It also pins that the index is REBUILT rather than trusted: the restored
// manager must refuse at the cap immediately, without needing a registration
// first to discover what it holds.
func TestOwnerCapSurvivesSnapshotRoundTrip(t *testing.T) {
	const owner = 0xBEEF

	src := NewManager(1000, 16, 2)
	if err := src.Register(1, owner, 1); err != nil {
		t.Fatalf("Register(1): %v", err)
	}
	if err := src.Register(2, owner, 2); err != nil {
		t.Fatalf("Register(2): %v", err)
	}
	// An ownerless session in the same snapshot, so the round-trip has to keep
	// the two kinds apart rather than defaulting everything one way.
	if err := src.Register(3, noOwnerID, 3); err != nil {
		t.Fatalf("Register(3): %v", err)
	}

	var buf bytes.Buffer
	if err := src.MarshalTo(&buf); err != nil {
		t.Fatalf("MarshalTo: %v", err)
	}

	dst := NewManager(1000, 16, 2)
	if err := dst.UnmarshalFrom(&buf); err != nil {
		t.Fatalf("UnmarshalFrom: %v", err)
	}

	if got, want := dst.sessions[1].ownerID, uint64(owner); got != want {
		t.Fatalf("restored session 1 ownerID = %d, want %d: the owner did not "+
			"survive the snapshot, so the cap silently resets on every restore",
			got, want)
	}
	if got := dst.sessions[3].ownerID; got != noOwnerID {
		t.Fatalf("restored session 3 ownerID = %d, want %d (unset)", got, noOwnerID)
	}
	if got, want := dst.ownerCounts[owner], uint64(2); got != want {
		t.Fatalf("restored ownerCounts[%#x] = %d, want %d: the index is rebuilt "+
			"by counting the decoded table, so this failing means the rebuild "+
			"did not happen", owner, got, want)
	}

	// The restored manager must enforce the cap straight away.
	if err := dst.Register(4, owner, 4); !errors.Is(err, ErrSessionLimitExceeded) {
		t.Fatalf("Register on restored manager = %v, want ErrSessionLimitExceeded: "+
			"the owner already holds 2 of 2", err)
	}
}
