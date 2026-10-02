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

package waldb

import (
	"context"
	"errors"
	"testing"

	"github.com/jeremyhahn/go-quicraft/pkg/logdb"
)

// TestMinLiveEpochForBarrier_IdentityMatch verifies that a single-barrier
// waldb.DB answers the per-barrier epoch query precisely by barrier identity:
// when the requested barrier is the one the DB was opened with, the result is
// (global MinLiveEpoch, known=true); for any other barrier handle the DB
// reports known=false (it does NOT own that barrier) — never a misleading
// (0, true) that the caller could mistake for a zero-record non-constraint.
// This is the building block the snapshot pool's per-barrier purge floor relies
// on.
func TestMinLiveEpochForBarrier_IdentityMatch(t *testing.T) {
	own := newTestBarrier(t)
	other := newTestBarrier(t)
	db := openEncryptedTestDB(t, own)

	// Write at epoch 1, rotate to 2, write again. Global min stays 1.
	if err := db.SaveEntries(1, 1, makeEntries(1, 5, 1)); err != nil {
		t.Fatalf("SaveEntries (epoch 1): %v", err)
	}
	if _, err := own.Rotate(context.Background()); err != nil {
		t.Fatalf("Rotate: %v", err)
	}
	if err := db.SaveEntries(1, 1, makeEntries(6, 5, 1)); err != nil {
		t.Fatalf("SaveEntries (epoch 2): %v", err)
	}

	global, err := db.MinLiveEpoch()
	if err != nil {
		t.Fatalf("MinLiveEpoch: %v", err)
	}
	if global != 1 {
		t.Fatalf("MinLiveEpoch = %d, want 1", global)
	}

	// Matching barrier -> equals the global floor, known=true.
	got, known, err := db.MinLiveEpochForBarrier(own)
	if err != nil {
		t.Fatalf("MinLiveEpochForBarrier(own): %v", err)
	}
	if !known || got != global {
		t.Fatalf("MinLiveEpochForBarrier(own) = (%d, %v), want (%d, true)", got, known, global)
	}

	// Different barrier -> this DB does NOT own it -> known=false (fail safe),
	// NOT a (0, true) non-constraint.
	got, known, err = db.MinLiveEpochForBarrier(other)
	if err != nil {
		t.Fatalf("MinLiveEpochForBarrier(other): %v", err)
	}
	if known || got != 0 {
		t.Fatalf("MinLiveEpochForBarrier(other) = (%d, %v), want (0, false)", got, known)
	}

	// Nil barrier handle -> known=false, no error.
	got, known, err = db.MinLiveEpochForBarrier(nil)
	if err != nil {
		t.Fatalf("MinLiveEpochForBarrier(nil): %v", err)
	}
	if known || got != 0 {
		t.Fatalf("MinLiveEpochForBarrier(nil) = (%d, %v), want (0, false)", got, known)
	}
}

// TestMinLiveEpochForBarrier_NoBarrier verifies that a DB opened without a
// barrier reports known=false for any barrier handle (it owns no barrier, so it
// cannot attribute any records) — fail safe, not a (0, true) non-constraint.
func TestMinLiveEpochForBarrier_NoBarrier(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir, WithNoSync(true))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { db.Close() })

	got, known, err := db.MinLiveEpochForBarrier(newTestBarrier(t))
	if err != nil {
		t.Fatalf("MinLiveEpochForBarrier: %v", err)
	}
	if known || got != 0 {
		t.Fatalf("MinLiveEpochForBarrier = (%d, %v), want (0, false) (no barrier)", got, known)
	}
}

// TestMinLiveEpochForBarrier_ClosedDB verifies the closed-DB fail-safe: the
// method returns ErrClosed (and known=false) rather than a stale value the
// caller might purge on.
func TestMinLiveEpochForBarrier_ClosedDB(t *testing.T) {
	barrier := newTestBarrier(t)
	db := openEncryptedTestDB(t, barrier)
	if err := db.SaveEntries(1, 1, makeEntries(1, 3, 1)); err != nil {
		t.Fatalf("SaveEntries: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	if _, known, err := db.MinLiveEpochForBarrier(barrier); !errors.Is(err, logdb.ErrClosed) || known {
		t.Fatalf("MinLiveEpochForBarrier on closed DB: got (known=%v, err=%v), want (false, ErrClosed)", known, err)
	}
}

// Compile-time assertion that the concrete DB satisfies the per-barrier
// interface used by the snapshot pool.
var _ logdb.PerBarrierEpochAwareLogDB = (*DB)(nil)
