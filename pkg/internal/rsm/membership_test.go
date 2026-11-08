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

package rsm

import (
	"bytes"
	"context"
	"fmt"
	"testing"

	"github.com/jeremyhahn/go-quicraft/pkg/proto"
	"github.com/jeremyhahn/go-quicraft/pkg/sm"
)

// ---------------------------------------------------------------------------
// Session membership: register via Apply entry
// ---------------------------------------------------------------------------

func TestMembership_RegisterViaApply(t *testing.T) {
	t.Run("register via Apply does not invoke user SM", func(t *testing.T) {
		kv := &failingUpdateKV{
			testKV:     testKV{data: make(map[string]string)},
			failAfterN: 0, // Would fail if called
		}
		createFn := func(shardID, replicaID uint64) interface{} {
			return kv
		}
		rsm, err := NewStateMachine(1, 1, createFn, t.TempDir(), testPolicy(100, 100, 0), nil, false)
		if err != nil {
			t.Fatalf("NewStateMachine = %v", err)
		}
		defer rsm.Close()

		entry := proto.Entry{
			Index:    1,
			ClientID: 42,
			SeriesID: proto.SessionSeriesIDForRegister,
			Type:     proto.EntryNormal,
		}
		results := make([]sm.Result, 1)
		if err := rsm.Apply([]proto.Entry{entry}, results); err != nil {
			t.Fatalf("Apply should not invoke user SM for register: %v", err)
		}
		if kv.callCount != 0 {
			t.Errorf("Update call count = %d, want 0", kv.callCount)
		}
	})
}

// ---------------------------------------------------------------------------
// Session membership: re-register after unregister
// ---------------------------------------------------------------------------

func TestMembership_ReRegisterAfterUnregister(t *testing.T) {
	t.Run("register-unregister-register succeeds", func(t *testing.T) {
		createFn := sm.NewCreateFunc(func(shardID, replicaID uint64) sm.StateMachine {
			return newTestKV()
		})
		rsm, err := NewStateMachine(1, 1, createFn, t.TempDir(), testPolicy(100, 100, 0), nil, false)
		if err != nil {
			t.Fatalf("NewStateMachine = %v", err)
		}
		defer rsm.Close()

		// Register.
		reg1 := proto.Entry{Index: 1, ClientID: 50, SeriesID: proto.SessionSeriesIDForRegister, Type: proto.EntryNormal}
		r1 := make([]sm.Result, 1)
		if err := rsm.Apply([]proto.Entry{reg1}, r1); err != nil {
			t.Fatalf("Apply(register) = %v", err)
		}

		// Unregister.
		unreg := proto.Entry{Index: 2, ClientID: 50, SeriesID: proto.SessionSeriesIDForUnregister, Type: proto.EntryNormal}
		r2 := make([]sm.Result, 1)
		if err := rsm.Apply([]proto.Entry{unreg}, r2); err != nil {
			t.Fatalf("Apply(unregister) = %v", err)
		}
		if rsm.Sessions().IsRegistered(50) {
			t.Fatal("session 50 should not be registered after unregister")
		}

		// Re-register.
		reg2 := proto.Entry{Index: 3, ClientID: 50, SeriesID: proto.SessionSeriesIDForRegister, Type: proto.EntryNormal}
		r3 := make([]sm.Result, 1)
		if err := rsm.Apply([]proto.Entry{reg2}, r3); err != nil {
			t.Fatalf("Apply(re-register) = %v", err)
		}
		if r3[0].Value != 50 {
			t.Errorf("re-register Value = %d, want 50", r3[0].Value)
		}
		if !rsm.Sessions().IsRegistered(50) {
			t.Error("session 50 should be registered after re-register")
		}
	})
}

// ---------------------------------------------------------------------------
// Session membership: session limit enforcement
// ---------------------------------------------------------------------------

func TestMembership_SessionLimitEnforced(t *testing.T) {
	t.Run("register fails at session limit", func(t *testing.T) {
		createFn := sm.NewCreateFunc(func(shardID, replicaID uint64) sm.StateMachine {
			return newTestKV()
		})
		// maxSessions = 2.
		rsm, err := NewStateMachine(1, 1, createFn, t.TempDir(), testPolicy(2, 100, 0), nil, false)
		if err != nil {
			t.Fatalf("NewStateMachine = %v", err)
		}
		defer rsm.Close()

		// Register 2 sessions.
		for i := uint64(1); i <= 2; i++ {
			reg := proto.Entry{
				Index: i, ClientID: i * 100,
				SeriesID: proto.SessionSeriesIDForRegister,
				Type:     proto.EntryNormal,
			}
			r := make([]sm.Result, 1)
			if err := rsm.Apply([]proto.Entry{reg}, r); err != nil {
				t.Fatalf("Apply(register %d) = %v", i, err)
			}
			if r[0].Value != i*100 {
				t.Errorf("register %d Value = %d, want %d", i, r[0].Value, i*100)
			}
		}

		// Third registration should fail (returns empty result due to limit).
		reg3 := proto.Entry{
			Index: 3, ClientID: 300,
			SeriesID: proto.SessionSeriesIDForRegister,
			Type:     proto.EntryNormal,
		}
		r3 := make([]sm.Result, 1)
		if err := rsm.Apply([]proto.Entry{reg3}, r3); err != nil {
			t.Fatalf("Apply(register 3) = %v", err)
		}
		// Registration at limit returns empty result (Value=0).
		if r3[0].Value != 0 {
			t.Errorf("limit-exceeded register Value = %d, want 0", r3[0].Value)
		}
		if rsm.Sessions().IsRegistered(300) {
			t.Error("session 300 should NOT be registered at limit")
		}
	})
}

// ---------------------------------------------------------------------------
// Session membership: session preserved across snapshot with expiry
// ---------------------------------------------------------------------------

func TestMembership_SessionPreservedAcrossSnapshotWithExpiry(t *testing.T) {
	t.Run("snapshot preserves sessions that survive expiry", func(t *testing.T) {
		createFn := sm.NewCreateFunc(func(shardID, replicaID uint64) sm.StateMachine {
			return newTestKV()
		})
		rsm1, err := NewStateMachine(1, 1, createFn, t.TempDir(), testPolicy(100, 100, 1000), nil, false)
		if err != nil {
			t.Fatalf("NewStateMachine = %v", err)
		}
		defer rsm1.Close()

		if err := rsm1.Sessions().Register(10, 0, 0); err != nil {
			t.Fatalf("Register(10) = %v", err)
		}
		if err := rsm1.Sessions().Register(20, 0, 0); err != nil {
			t.Fatalf("Register(20) = %v", err)
		}

		// Touch session 10 at index 800.
		entries := []proto.Entry{
			{Index: 800, ClientID: 10, SeriesID: 1, Cmd: makeCmd("pr", "sv")},
		}
		results := make([]sm.Result, 1)
		if err := applyContiguous(t, rsm1, entries, results); err != nil {
			t.Fatalf("Apply = %v", err)
		}

		// Snapshot.
		var buf bytes.Buffer
		stopper := make(chan struct{})
		if err := rsm1.SaveSnapshot(context.Background(), &buf, stopper); err != nil {
			t.Fatalf("SaveSnapshot = %v", err)
		}

		// Restore.
		rsm2, err := NewStateMachine(1, 1, createFn, t.TempDir(), testPolicy(100, 100, 1000), nil, false)
		if err != nil {
			t.Fatalf("NewStateMachine(2) = %v", err)
		}
		defer rsm2.Close()

		if err := rsm2.RecoverFromSnapshot(context.Background(), &buf, stopper); err != nil {
			t.Fatalf("RecoverFromSnapshot = %v", err)
		}

		// At index 1500 with threshold 1000:
		// Session 10 (last active 800): gap=700 < 1000, survives.
		// Session 20 (last active 0): gap=1500 > 1000, expires.
		expired := rsm2.Sessions().ExpireSessions(1500, 1000, 10)
		if len(expired) != 1 {
			t.Fatalf("expired count = %d, want 1", len(expired))
		}
		if expired[0] != 20 {
			t.Errorf("expired[0] = %d, want 20", expired[0])
		}
		if !rsm2.Sessions().IsRegistered(10) {
			t.Error("session 10 should still be registered")
		}
	})
}

// ---------------------------------------------------------------------------
// Session membership: dedup after snapshot recovery
// ---------------------------------------------------------------------------

func TestMembership_DedupAfterSnapshotRecovery(t *testing.T) {
	t.Run("duplicate detection works after snapshot restore", func(t *testing.T) {
		createFn := sm.NewCreateFunc(func(shardID, replicaID uint64) sm.StateMachine {
			return newTestKV()
		})
		rsm1, err := NewStateMachine(1, 1, createFn, t.TempDir(), testPolicy(100, 100, 0), nil, false)
		if err != nil {
			t.Fatalf("NewStateMachine = %v", err)
		}
		defer rsm1.Close()

		// Register and apply.
		if err := rsm1.Sessions().Register(10, 0, 0); err != nil {
			t.Fatalf("Register = %v", err)
		}
		entries := []proto.Entry{
			{Index: 1, ClientID: 10, SeriesID: 1, Cmd: makeCmd("dd", "val1")},
			{Index: 2, ClientID: 10, SeriesID: 2, RespondedTo: 1, Cmd: makeCmd("dd2", "val2")},
		}
		results := make([]sm.Result, 2)
		if err := rsm1.Apply(entries, results); err != nil {
			t.Fatalf("Apply = %v", err)
		}
		originalSeries2Result := results[1].Value

		// Snapshot and restore.
		var buf bytes.Buffer
		stopper := make(chan struct{})
		if err := rsm1.SaveSnapshot(context.Background(), &buf, stopper); err != nil {
			t.Fatalf("SaveSnapshot = %v", err)
		}

		rsm2, err := NewStateMachine(1, 1, createFn, t.TempDir(), testPolicy(100, 100, 0), nil, false)
		if err != nil {
			t.Fatalf("NewStateMachine(2) = %v", err)
		}
		defer rsm2.Close()

		if err := rsm2.RecoverFromSnapshot(context.Background(), &buf, stopper); err != nil {
			t.Fatalf("RecoverFromSnapshot = %v", err)
		}

		// Attempt to apply series 2 again. Should be detected as duplicate.
		dupEntries := []proto.Entry{
			{Index: 10, ClientID: 10, SeriesID: 2, Cmd: makeCmd("dd2", "DIFFERENT")},
		}
		dupResults := make([]sm.Result, 1)
		if err := applyContiguous(t, rsm2, dupEntries, dupResults); err != nil {
			t.Fatalf("Apply(dup) = %v", err)
		}
		if dupResults[0].Value != originalSeries2Result {
			t.Errorf("dup result Value = %d, want %d (cached from before snapshot)",
				dupResults[0].Value, originalSeries2Result)
		}

		// Verify original data was not overwritten.
		val, err := rsm2.Lookup(context.Background(), "dd2")
		if err != nil {
			t.Fatalf("Lookup = %v", err)
		}
		if val.(string) != "val2" {
			t.Errorf("Lookup(dd2) = %q, want %q (should not be overwritten by dup)", val, "val2")
		}
	})
}

// ---------------------------------------------------------------------------
// Session membership: session eviction ordering via respondedTo
// ---------------------------------------------------------------------------

func TestMembership_SessionEvictionOrdering(t *testing.T) {
	t.Run("respondedTo evicts older series in order", func(t *testing.T) {
		createFn := sm.NewCreateFunc(func(shardID, replicaID uint64) sm.StateMachine {
			return newTestKV()
		})
		rsm, err := NewStateMachine(1, 1, createFn, t.TempDir(), testPolicy(100, 100, 0), nil, false)
		if err != nil {
			t.Fatalf("NewStateMachine = %v", err)
		}
		defer rsm.Close()

		if err := rsm.Sessions().Register(10, 0, 0); err != nil {
			t.Fatalf("Register = %v", err)
		}

		// Apply series 1 through 5 with progressive respondedTo.
		for i := uint64(1); i <= 5; i++ {
			entries := []proto.Entry{
				{Index: i, ClientID: 10, SeriesID: i, RespondedTo: i - 1, Cmd: makeCmd(fmt.Sprintf("s%d", i), "v")},
			}
			results := make([]sm.Result, 1)
			if err := rsm.Apply(entries, results); err != nil {
				t.Fatalf("Apply(%d) = %v", i, err)
			}
		}

		// Series 1 through 4 should have been evicted by respondedTo advancement.
		// Replay series 1 should return SESSION_EXPIRED.
		for _, evictedSeries := range []uint64{1, 2, 3, 4} {
			entries := []proto.Entry{
				{Index: 100 + evictedSeries, ClientID: 10, SeriesID: evictedSeries, Cmd: makeCmd("fail", "fail")},
			}
			results := make([]sm.Result, 1)
			if err := applyContiguous(t, rsm, entries, results); err != nil {
				t.Fatalf("Apply(replay %d) = %v", evictedSeries, err)
			}
			if results[0].Value != sm.ResultSessionExpired {
				t.Errorf("replay series %d result = %d, want ResultSessionExpired", evictedSeries, results[0].Value)
			}
		}

		// Series 5 replay should return the cached result (still in cache).
		dupEntries := []proto.Entry{
			{Index: 200, ClientID: 10, SeriesID: 5, Cmd: makeCmd("fail", "fail")},
		}
		dupResults := make([]sm.Result, 1)
		if err := applyContiguous(t, rsm, dupEntries, dupResults); err != nil {
			t.Fatalf("Apply(dup series 5) = %v", err)
		}
		if dupResults[0].Value == sm.ResultSessionExpired {
			t.Error("series 5 should still be cached, not expired")
		}
	})
}

// ---------------------------------------------------------------------------
// Session membership: max responses per session enforced
// ---------------------------------------------------------------------------

func TestMembership_MaxResponsesEnforced(t *testing.T) {
	t.Run("respondedTo frees slots allowing new responses", func(t *testing.T) {
		createFn := sm.NewCreateFunc(func(shardID, replicaID uint64) sm.StateMachine {
			return newTestKV()
		})
		// maxResponses = 3.
		rsm, err := NewStateMachine(1, 1, createFn, t.TempDir(), testPolicy(100, 3, 0), nil, false)
		if err != nil {
			t.Fatalf("NewStateMachine = %v", err)
		}
		defer rsm.Close()

		if err := rsm.Sessions().Register(10, 0, 0); err != nil {
			t.Fatalf("Register = %v", err)
		}

		// Apply 3 series.
		for i := uint64(1); i <= 3; i++ {
			entries := []proto.Entry{
				{Index: i, ClientID: 10, SeriesID: i, Cmd: makeCmd(fmt.Sprintf("mr%d", i), "v")},
			}
			results := make([]sm.Result, 1)
			if err := rsm.Apply(entries, results); err != nil {
				t.Fatalf("Apply(%d) = %v", i, err)
			}
		}

		// Apply series 4 with respondedTo=2, which evicts series 1 and 2.
		entries := []proto.Entry{
			{Index: 4, ClientID: 10, SeriesID: 4, RespondedTo: 2, Cmd: makeCmd("mr4", "v")},
		}
		results := make([]sm.Result, 1)
		if err := rsm.Apply(entries, results); err != nil {
			t.Fatalf("Apply(4 with respondedTo) = %v", err)
		}
		if results[0].Value == sm.ResultSessionExpired {
			t.Error("series 4 should succeed after eviction via respondedTo")
		}
	})
}
