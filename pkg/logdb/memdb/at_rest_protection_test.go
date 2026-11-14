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

package memdb

import (
	"context"
	"testing"

	"github.com/jeremyhahn/go-quicraft/pkg/logdb"
)

// The in-memory store must advertise its at-rest guarantee.
var _ logdb.AtRestProtectedLogDB = New()

// assertNonDurable checks that db reports AtRestNonDurable and none of the
// other at-rest states, naming stage in any failure.
func assertNonDurable(ctx context.Context, t *testing.T, db logdb.AtRestProtectedLogDB, stage string) {
	t.Helper()
	if err := ctx.Err(); err != nil {
		t.Fatalf("%s: context done before the check: %v", stage, err)
	}
	got := db.AtRestProtection()
	if got != logdb.AtRestNonDurable {
		t.Fatalf("%s: AtRestProtection() = %v, want %v", stage, got, logdb.AtRestNonDurable)
	}
	for _, other := range []logdb.AtRestProtection{
		logdb.AtRestUnprotected,
		logdb.AtRestBarrierEncrypted,
		logdb.AtRestHostRouted,
	} {
		if got == other {
			t.Fatalf("%s: AtRestProtection() = %v, a state an in-memory store must not report", stage, got)
		}
	}
	if s := got.String(); s != "non_durable" {
		t.Fatalf("%s: AtRestProtection().String() = %q, want %q", stage, s, "non_durable")
	}
}

// TestAtRestProtectionReportsNonDurableOpenOrClosed verifies that a MemDB
// reports AtRestNonDurable on a new store, after it holds entries, and after
// Close (called twice, as Close is idempotent): the guarantee does not depend
// on the records stored or on whether the store is open.
func TestAtRestProtectionReportsNonDurableOpenOrClosed(t *testing.T) {
	ctx := context.Background()
	db := New()

	var anyDB logdb.LogDB = db
	protected, ok := anyDB.(logdb.AtRestProtectedLogDB)
	if !ok {
		t.Fatal("New() does not implement logdb.AtRestProtectedLogDB")
	}

	assertNonDurable(ctx, t, protected, "new store")

	if err := db.SaveState([]logdb.Update{{
		ShardID:   1,
		ReplicaID: 1,
		State:     logdb.State{Term: 1, Commit: 3},
		Entries: []logdb.Entry{
			{Index: 1, Term: 1, Cmd: []byte("a")},
			{Index: 2, Term: 1, Cmd: []byte("b")},
			{Index: 3, Term: 1, Cmd: []byte("c")},
		},
	}}); err != nil {
		t.Fatalf("SaveState: %v", err)
	}
	assertNonDurable(ctx, t, protected, "after SaveState")

	if err := db.Close(); err != nil {
		t.Fatalf("first Close: %v", err)
	}
	assertNonDurable(ctx, t, protected, "after Close")

	if err := db.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}
	assertNonDurable(ctx, t, protected, "after a second Close")
}
