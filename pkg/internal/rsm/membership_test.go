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
	t.Run("register new session via entry returns clientID", func(t *testing.T) {
		createFn := sm.NewCreateFunc(func(shardID, replicaID uint64) sm.StateMachine {
			return newTestKV()
		})
		rsm, err := NewStateMachine(1, 1, createFn, t.TempDir(), 100, 100, 0, false)
		if err != nil {
			t.Fatalf("NewStateMachine = %v", err)
		}
		defer rsm.Close()

		entry := proto.Entry{
			Index:    1,
			ClientID: 500,
			SeriesID: proto.SessionSeriesIDForRegister,
			Type:     proto.EntryNormal,
		}
		results := make([]sm.Result, 1)
		if err := rsm.Apply([]proto.Entry{entry}, results); err != nil {
			t.Fatalf("Apply = %v", err)
		}
		if results[0].Value != 500 {
			t.Errorf("register result Value = %d, want 500", results[0].Value)
		}
		if !rsm.Sessions().IsRegistered(500) {
			t.Error("session 500 should be registered")
		}
	})

	t.Run("register via Apply does not invoke user SM", func(t *testing.T) {
		kv := &failingUpdateKV{
			testKV:     testKV{data: make(map[string]string)},
			failAfterN: 0, // Would fail if called
		}
		createFn := func(shardID, replicaID uint64) interface{} {
			return kv
		}
		rsm, err := NewStateMachine(1, 1, createFn, t.TempDir(), 100, 100, 0, false)
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
// Session membership: unregister via Apply entry
// ---------------------------------------------------------------------------

func TestMembership_UnregisterViaApply(t *testing.T) {
	t.Run("unregister existing session via entry returns clientID", func(t *testing.T) {
		createFn := sm.NewCreateFunc(func(shardID, replicaID uint64) sm.StateMachine {
			return newTestKV()
		})
		rsm, err := NewStateMachine(1, 1, createFn, t.TempDir(), 100, 100, 0, false)
		if err != nil {
			t.Fatalf("NewStateMachine = %v", err)
		}
		defer rsm.Close()

		// Register first.
		regEntry := proto.Entry{
			Index:    1,
			ClientID: 500,
			SeriesID: proto.SessionSeriesIDForRegister,
			Type:     proto.EntryNormal,
		}
		regResults := make([]sm.Result, 1)
		if err := rsm.Apply([]proto.Entry{regEntry}, regResults); err != nil {
			t.Fatalf("Apply(register) = %v", err)
		}

		// Unregister.
		unregEntry := proto.Entry{
			Index:    2,
			ClientID: 500,
			SeriesID: proto.SessionSeriesIDForUnregister,
			Type:     proto.EntryNormal,
		}
		unregResults := make([]sm.Result, 1)
		if err := rsm.Apply([]proto.Entry{unregEntry}, unregResults); err != nil {
			t.Fatalf("Apply(unregister) = %v", err)
		}
		if unregResults[0].Value != 500 {
			t.Errorf("unregister result Value = %d, want 500", unregResults[0].Value)
		}
		if rsm.Sessions().IsRegistered(500) {
			t.Error("session 500 should NOT be registered after unregister")
		}
	})

	t.Run("unregister nonexistent session returns SESSION_EXPIRED", func(t *testing.T) {
		createFn := sm.NewCreateFunc(func(shardID, replicaID uint64) sm.StateMachine {
			return newTestKV()
		})
		rsm, err := NewStateMachine(1, 1, createFn, t.TempDir(), 100, 100, 0, false)
		if err != nil {
			t.Fatalf("NewStateMachine = %v", err)
		}
		defer rsm.Close()

		entry := proto.Entry{
			Index:    1,
			ClientID: 999,
			SeriesID: proto.SessionSeriesIDForUnregister,
			Type:     proto.EntryNormal,
		}
		results := make([]sm.Result, 1)
		if err := rsm.Apply([]proto.Entry{entry}, results); err != nil {
			t.Fatalf("Apply = %v", err)
		}
		if results[0].Value != sm.ResultSessionExpired {
			t.Errorf("result Value = %d, want ResultSessionExpired", results[0].Value)
		}
	})
}

// ---------------------------------------------------------------------------
// Session membership: duplicate registration
// ---------------------------------------------------------------------------

func TestMembership_DuplicateRegistrationRejected(t *testing.T) {
	t.Run("second register returns empty result", func(t *testing.T) {
		createFn := sm.NewCreateFunc(func(shardID, replicaID uint64) sm.StateMachine {
			return newTestKV()
		})
		rsm, err := NewStateMachine(1, 1, createFn, t.TempDir(), 100, 100, 0, false)
		if err != nil {
			t.Fatalf("NewStateMachine = %v", err)
		}
		defer rsm.Close()

		// First register.
		reg1 := proto.Entry{
			Index: 1, ClientID: 42,
			SeriesID: proto.SessionSeriesIDForRegister,
			Type:     proto.EntryNormal,
		}
		r1 := make([]sm.Result, 1)
		if err := rsm.Apply([]proto.Entry{reg1}, r1); err != nil {
			t.Fatalf("Apply(1) = %v", err)
		}
		if r1[0].Value != 42 {
			t.Errorf("first register Value = %d, want 42", r1[0].Value)
		}

		// Second register with same clientID.
		reg2 := proto.Entry{
			Index: 2, ClientID: 42,
			SeriesID: proto.SessionSeriesIDForRegister,
			Type:     proto.EntryNormal,
		}
		r2 := make([]sm.Result, 1)
		if err := rsm.Apply([]proto.Entry{reg2}, r2); err != nil {
			t.Fatalf("Apply(2) = %v", err)
		}
		if r2[0].Value != 0 {
			t.Errorf("duplicate register Value = %d, want 0", r2[0].Value)
		}

		// Session count should still be 1.
		if rsm.Sessions().Count() != 1 {
			t.Errorf("session count = %d, want 1", rsm.Sessions().Count())
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
		rsm, err := NewStateMachine(1, 1, createFn, t.TempDir(), 100, 100, 0, false)
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
		rsm, err := NewStateMachine(1, 1, createFn, t.TempDir(), 2, 100, 0, false)
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
// Session membership: mixed register, data, and unregister in single batch
// ---------------------------------------------------------------------------

func TestMembership_MixedBatchOperations(t *testing.T) {
	t.Run("register, apply non-session, and unregister different session in single batch", func(t *testing.T) {
		createFn := sm.NewCreateFunc(func(shardID, replicaID uint64) sm.StateMachine {
			return newTestKV()
		})
		rsm, err := NewStateMachine(1, 1, createFn, t.TempDir(), 100, 100, 0, false)
		if err != nil {
			t.Fatalf("NewStateMachine = %v", err)
		}
		defer rsm.Close()

		// Pre-register session 100 for unregister in the batch.
		if err := rsm.Sessions().Register(100, 0); err != nil {
			t.Fatalf("Register = %v", err)
		}

		// Note: we avoid mixing session-managed data entries and an unregister
		// for the same session in a single batch, because Apply processes
		// session ops (register/unregister) inline but defers RecordResult
		// until after Update(). An unregister that removes a session before
		// RecordResult runs for the same session would cause an error.
		entries := []proto.Entry{
			// Register new session 200.
			{Index: 1, ClientID: 200, SeriesID: proto.SessionSeriesIDForRegister, Type: proto.EntryNormal},
			// Non-session data entry (no clientID, bypasses dedup).
			{Index: 2, Cmd: makeCmd("mk", "mv")},
			// Another non-session data entry.
			{Index: 3, Cmd: makeCmd("nk", "nv")},
			// Unregister session 100 (different from the data entries).
			{Index: 4, ClientID: 100, SeriesID: proto.SessionSeriesIDForUnregister, Type: proto.EntryNormal},
		}
		results := make([]sm.Result, 4)
		if err := rsm.Apply(entries, results); err != nil {
			t.Fatalf("Apply = %v", err)
		}

		// Entry 0: register session 200, Value = 200.
		if results[0].Value != 200 {
			t.Errorf("register result Value = %d, want 200", results[0].Value)
		}

		// Entry 1: non-session data, should have been applied.
		if results[1].Value == 0 {
			t.Error("data entry result should be non-zero")
		}

		// Entry 2: non-session data, should have been applied.
		if results[2].Value == 0 {
			t.Error("non-session entry result should be non-zero")
		}

		// Entry 3: unregister session 100, Value = 100.
		if results[3].Value != 100 {
			t.Errorf("unregister result Value = %d, want 100", results[3].Value)
		}

		// Session 200 should be registered.
		if !rsm.Sessions().IsRegistered(200) {
			t.Error("session 200 should be registered")
		}
		// Session 100 should be unregistered.
		if rsm.Sessions().IsRegistered(100) {
			t.Error("session 100 should NOT be registered")
		}

		// Verify data was applied.
		val, err := rsm.Lookup(context.Background(), "mk")
		if err != nil {
			t.Fatalf("Lookup(mk) = %v", err)
		}
		if val.(string) != "mv" {
			t.Errorf("Lookup(mk) = %q, want %q", val, "mv")
		}

		if rsm.LastApplied() != 4 {
			t.Errorf("LastApplied = %d, want 4", rsm.LastApplied())
		}
	})
}

// ---------------------------------------------------------------------------
// Session membership: session expiry via Apply
// ---------------------------------------------------------------------------

func TestMembership_SessionExpiryViaApply(t *testing.T) {
	t.Run("stale sessions expire after Apply with expiry enabled", func(t *testing.T) {
		createFn := sm.NewCreateFunc(func(shardID, replicaID uint64) sm.StateMachine {
			return newTestKV()
		})
		// sessionExpiryEntries = 100.
		rsm, err := NewStateMachine(1, 1, createFn, t.TempDir(), 100, 100, 100, false)
		if err != nil {
			t.Fatalf("NewStateMachine = %v", err)
		}
		defer rsm.Close()

		// Register a session at index 0 via manager.
		if err := rsm.Sessions().Register(10, 0); err != nil {
			t.Fatalf("Register = %v", err)
		}

		// Apply a batch at index 200 (gap = 200 > threshold 100).
		entries := []proto.Entry{
			{Index: 200, Cmd: makeCmd("ex", "val")},
		}
		results := make([]sm.Result, 1)
		if err := rsm.Apply(entries, results); err != nil {
			t.Fatalf("Apply = %v", err)
		}

		// Session 10 should have been expired by the Apply call.
		if rsm.Sessions().IsRegistered(10) {
			t.Error("stale session 10 should have been expired")
		}
	})

	t.Run("active sessions survive expiry after Apply", func(t *testing.T) {
		createFn := sm.NewCreateFunc(func(shardID, replicaID uint64) sm.StateMachine {
			return newTestKV()
		})
		// sessionExpiryEntries = 100.
		rsm, err := NewStateMachine(1, 1, createFn, t.TempDir(), 100, 100, 100, false)
		if err != nil {
			t.Fatalf("NewStateMachine = %v", err)
		}
		defer rsm.Close()

		if err := rsm.Sessions().Register(10, 0); err != nil {
			t.Fatalf("Register = %v", err)
		}

		// Apply a session-managed entry at index 50 to keep session active.
		entries1 := []proto.Entry{
			{Index: 50, ClientID: 10, SeriesID: 1, Cmd: makeCmd("act", "ive")},
		}
		r1 := make([]sm.Result, 1)
		if err := rsm.Apply(entries1, r1); err != nil {
			t.Fatalf("Apply(1) = %v", err)
		}

		// Apply at index 120 (gap from 50 is 70 < threshold 100).
		entries2 := []proto.Entry{
			{Index: 120, Cmd: makeCmd("xx", "yy")},
		}
		r2 := make([]sm.Result, 1)
		if err := rsm.Apply(entries2, r2); err != nil {
			t.Fatalf("Apply(2) = %v", err)
		}

		// Session 10 should still be active.
		if !rsm.Sessions().IsRegistered(10) {
			t.Error("active session 10 should NOT be expired")
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
		rsm1, err := NewStateMachine(1, 1, createFn, t.TempDir(), 100, 100, 1000, false)
		if err != nil {
			t.Fatalf("NewStateMachine = %v", err)
		}
		defer rsm1.Close()

		if err := rsm1.Sessions().Register(10, 0); err != nil {
			t.Fatalf("Register(10) = %v", err)
		}
		if err := rsm1.Sessions().Register(20, 0); err != nil {
			t.Fatalf("Register(20) = %v", err)
		}

		// Touch session 10 at index 800.
		entries := []proto.Entry{
			{Index: 800, ClientID: 10, SeriesID: 1, Cmd: makeCmd("pr", "sv")},
		}
		results := make([]sm.Result, 1)
		if err := rsm1.Apply(entries, results); err != nil {
			t.Fatalf("Apply = %v", err)
		}

		// Snapshot.
		var buf bytes.Buffer
		stopper := make(chan struct{})
		if err := rsm1.SaveSnapshot(context.Background(), &buf, stopper); err != nil {
			t.Fatalf("SaveSnapshot = %v", err)
		}

		// Restore.
		rsm2, err := NewStateMachine(1, 1, createFn, t.TempDir(), 100, 100, 1000, false)
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
// Session membership: multiple clients in batch
// ---------------------------------------------------------------------------

func TestMembership_MultipleClientRegistrations(t *testing.T) {
	t.Run("batch with multiple register entries", func(t *testing.T) {
		createFn := sm.NewCreateFunc(func(shardID, replicaID uint64) sm.StateMachine {
			return newTestKV()
		})
		rsm, err := NewStateMachine(1, 1, createFn, t.TempDir(), 100, 100, 0, false)
		if err != nil {
			t.Fatalf("NewStateMachine = %v", err)
		}
		defer rsm.Close()

		entries := make([]proto.Entry, 10)
		for i := 0; i < 10; i++ {
			entries[i] = proto.Entry{
				Index:    uint64(i + 1),
				ClientID: uint64((i + 1) * 100),
				SeriesID: proto.SessionSeriesIDForRegister,
				Type:     proto.EntryNormal,
			}
		}
		results := make([]sm.Result, 10)
		if err := rsm.Apply(entries, results); err != nil {
			t.Fatalf("Apply = %v", err)
		}

		for i := 0; i < 10; i++ {
			expectedID := uint64((i + 1) * 100)
			if results[i].Value != expectedID {
				t.Errorf("results[%d].Value = %d, want %d", i, results[i].Value, expectedID)
			}
			if !rsm.Sessions().IsRegistered(expectedID) {
				t.Errorf("session %d should be registered", expectedID)
			}
		}
		if rsm.Sessions().Count() != 10 {
			t.Errorf("session count = %d, want 10", rsm.Sessions().Count())
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
		rsm1, err := NewStateMachine(1, 1, createFn, t.TempDir(), 100, 100, 0, false)
		if err != nil {
			t.Fatalf("NewStateMachine = %v", err)
		}
		defer rsm1.Close()

		// Register and apply.
		if err := rsm1.Sessions().Register(10, 0); err != nil {
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

		rsm2, err := NewStateMachine(1, 1, createFn, t.TempDir(), 100, 100, 0, false)
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
		if err := rsm2.Apply(dupEntries, dupResults); err != nil {
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
// Session membership: respondedTo advancement in batch
// ---------------------------------------------------------------------------

func TestMembership_RespondedToAdvancement(t *testing.T) {
	t.Run("respondedTo evicts old series within batch", func(t *testing.T) {
		createFn := sm.NewCreateFunc(func(shardID, replicaID uint64) sm.StateMachine {
			return newTestKV()
		})
		rsm, err := NewStateMachine(1, 1, createFn, t.TempDir(), 100, 100, 0, false)
		if err != nil {
			t.Fatalf("NewStateMachine = %v", err)
		}
		defer rsm.Close()

		if err := rsm.Sessions().Register(10, 0); err != nil {
			t.Fatalf("Register = %v", err)
		}

		// Apply series 1, 2, 3 with progressive respondedTo.
		entries := []proto.Entry{
			{Index: 1, ClientID: 10, SeriesID: 1, Cmd: makeCmd("s1", "v1")},
		}
		r1 := make([]sm.Result, 1)
		if err := rsm.Apply(entries, r1); err != nil {
			t.Fatalf("Apply(1) = %v", err)
		}

		entries2 := []proto.Entry{
			{Index: 2, ClientID: 10, SeriesID: 2, RespondedTo: 1, Cmd: makeCmd("s2", "v2")},
		}
		r2 := make([]sm.Result, 1)
		if err := rsm.Apply(entries2, r2); err != nil {
			t.Fatalf("Apply(2) = %v", err)
		}

		entries3 := []proto.Entry{
			{Index: 3, ClientID: 10, SeriesID: 3, RespondedTo: 2, Cmd: makeCmd("s3", "v3")},
		}
		r3 := make([]sm.Result, 1)
		if err := rsm.Apply(entries3, r3); err != nil {
			t.Fatalf("Apply(3) = %v", err)
		}

		// Series 1 and 2 should be evicted (respondedTo advanced past them).
		// Attempt to replay series 1 should get SESSION_EXPIRED.
		replayEntries := []proto.Entry{
			{Index: 4, ClientID: 10, SeriesID: 1, Cmd: makeCmd("s1", "DIFFERENT")},
		}
		replayResults := make([]sm.Result, 1)
		if err := rsm.Apply(replayEntries, replayResults); err != nil {
			t.Fatalf("Apply(replay) = %v", err)
		}
		if replayResults[0].Value != sm.ResultSessionExpired {
			t.Errorf("replay result Value = %d, want ResultSessionExpired", replayResults[0].Value)
		}
	})
}

// ---------------------------------------------------------------------------
// Session membership: deep copy of sessions (mutations don't affect original)
// ---------------------------------------------------------------------------

func TestMembership_SessionDeepCopy(t *testing.T) {
	t.Run("snapshot creates independent session copies", func(t *testing.T) {
		createFn := sm.NewCreateFunc(func(shardID, replicaID uint64) sm.StateMachine {
			return newTestKV()
		})
		rsm1, err := NewStateMachine(1, 1, createFn, t.TempDir(), 100, 100, 0, false)
		if err != nil {
			t.Fatalf("NewStateMachine = %v", err)
		}
		defer rsm1.Close()

		if err := rsm1.Sessions().Register(10, 0); err != nil {
			t.Fatalf("Register = %v", err)
		}
		entries := []proto.Entry{
			{Index: 1, ClientID: 10, SeriesID: 1, Cmd: makeCmd("dc", "dv")},
		}
		results := make([]sm.Result, 1)
		if err := rsm1.Apply(entries, results); err != nil {
			t.Fatalf("Apply = %v", err)
		}

		// Snapshot.
		var buf bytes.Buffer
		stopper := make(chan struct{})
		if err := rsm1.SaveSnapshot(context.Background(), &buf, stopper); err != nil {
			t.Fatalf("SaveSnapshot = %v", err)
		}

		// Restore.
		rsm2, err := NewStateMachine(1, 1, createFn, t.TempDir(), 100, 100, 0, false)
		if err != nil {
			t.Fatalf("NewStateMachine(2) = %v", err)
		}
		defer rsm2.Close()

		if err := rsm2.RecoverFromSnapshot(context.Background(), bytes.NewReader(buf.Bytes()), stopper); err != nil {
			t.Fatalf("RecoverFromSnapshot = %v", err)
		}

		// Mutate rsm1's sessions.
		if err := rsm1.Sessions().Register(99, 0); err != nil {
			t.Fatalf("Register(99) = %v", err)
		}

		// rsm2 should NOT have session 99 (independent copy).
		if rsm2.Sessions().IsRegistered(99) {
			t.Error("session 99 should NOT exist in restored SM (independent copy)")
		}
	})
}

// ---------------------------------------------------------------------------
// Session membership: session state after Apply with no session entries
// ---------------------------------------------------------------------------

func TestMembership_ApplyNonSessionEntriesLeavesSessionsUnchanged(t *testing.T) {
	t.Run("non-session entries do not affect session count", func(t *testing.T) {
		createFn := sm.NewCreateFunc(func(shardID, replicaID uint64) sm.StateMachine {
			return newTestKV()
		})
		rsm, err := NewStateMachine(1, 1, createFn, t.TempDir(), 100, 100, 0, false)
		if err != nil {
			t.Fatalf("NewStateMachine = %v", err)
		}
		defer rsm.Close()

		// Register one session.
		if err := rsm.Sessions().Register(10, 0); err != nil {
			t.Fatalf("Register = %v", err)
		}

		// Apply non-session entries.
		entries := make([]proto.Entry, 50)
		for i := 0; i < 50; i++ {
			key := fmt.Sprintf("ns-%d", i)
			entries[i] = proto.Entry{
				Index: uint64(i + 1),
				Cmd:   makeCmd(key, "val"),
			}
		}
		results := make([]sm.Result, 50)
		if err := rsm.Apply(entries, results); err != nil {
			t.Fatalf("Apply = %v", err)
		}

		// Session count should still be 1.
		if rsm.Sessions().Count() != 1 {
			t.Errorf("session count = %d, want 1", rsm.Sessions().Count())
		}
		if !rsm.Sessions().IsRegistered(10) {
			t.Error("session 10 should still be registered")
		}
	})
}

// ---------------------------------------------------------------------------
// Session membership: lastApplied tracking across batches
// ---------------------------------------------------------------------------

func TestMembership_LastAppliedTracking(t *testing.T) {
	t.Run("lastApplied tracks highest index across multiple batches", func(t *testing.T) {
		createFn := sm.NewCreateFunc(func(shardID, replicaID uint64) sm.StateMachine {
			return newTestKV()
		})
		rsm, err := NewStateMachine(1, 1, createFn, t.TempDir(), 100, 100, 0, false)
		if err != nil {
			t.Fatalf("NewStateMachine = %v", err)
		}
		defer rsm.Close()

		if rsm.LastApplied() != 0 {
			t.Errorf("initial LastApplied = %d, want 0", rsm.LastApplied())
		}

		// Batch 1: indices 1-5.
		entries1 := make([]proto.Entry, 5)
		for i := range entries1 {
			entries1[i] = proto.Entry{Index: uint64(i + 1), Cmd: makeCmd(fmt.Sprintf("b1-%d", i), "v")}
		}
		results1 := make([]sm.Result, 5)
		if err := rsm.Apply(entries1, results1); err != nil {
			t.Fatalf("Apply(batch1) = %v", err)
		}
		if rsm.LastApplied() != 5 {
			t.Errorf("after batch1: LastApplied = %d, want 5", rsm.LastApplied())
		}

		// Batch 2: indices 6-10.
		entries2 := make([]proto.Entry, 5)
		for i := range entries2 {
			entries2[i] = proto.Entry{Index: uint64(i + 6), Cmd: makeCmd(fmt.Sprintf("b2-%d", i), "v")}
		}
		results2 := make([]sm.Result, 5)
		if err := rsm.Apply(entries2, results2); err != nil {
			t.Fatalf("Apply(batch2) = %v", err)
		}
		if rsm.LastApplied() != 10 {
			t.Errorf("after batch2: LastApplied = %d, want 10", rsm.LastApplied())
		}
	})

	t.Run("empty batch does not update lastApplied", func(t *testing.T) {
		createFn := sm.NewCreateFunc(func(shardID, replicaID uint64) sm.StateMachine {
			return newTestKV()
		})
		rsm, err := NewStateMachine(1, 1, createFn, t.TempDir(), 100, 100, 0, false)
		if err != nil {
			t.Fatalf("NewStateMachine = %v", err)
		}
		defer rsm.Close()

		// Apply an empty batch.
		results := make([]sm.Result, 0)
		if err := rsm.Apply(nil, results); err != nil {
			t.Fatalf("Apply(empty) = %v", err)
		}
		if rsm.LastApplied() != 0 {
			t.Errorf("LastApplied after empty batch = %d, want 0", rsm.LastApplied())
		}
	})
}

// ---------------------------------------------------------------------------
// Session membership: session-managed entry for expired session
// ---------------------------------------------------------------------------

func TestMembership_ExpiredSessionEntryRejected(t *testing.T) {
	t.Run("entry for expired session returns SESSION_EXPIRED", func(t *testing.T) {
		createFn := sm.NewCreateFunc(func(shardID, replicaID uint64) sm.StateMachine {
			return newTestKV()
		})
		rsm, err := NewStateMachine(1, 1, createFn, t.TempDir(), 100, 100, 50, false)
		if err != nil {
			t.Fatalf("NewStateMachine = %v", err)
		}
		defer rsm.Close()

		// Register session at index 0.
		if err := rsm.Sessions().Register(10, 0); err != nil {
			t.Fatalf("Register = %v", err)
		}

		// Apply non-session entry at index 200 to trigger expiry (gap=200 > 50).
		entries := []proto.Entry{
			{Index: 200, Cmd: makeCmd("trigger", "expiry")},
		}
		results := make([]sm.Result, 1)
		if err := rsm.Apply(entries, results); err != nil {
			t.Fatalf("Apply = %v", err)
		}

		// Session 10 should be expired.
		if rsm.Sessions().IsRegistered(10) {
			t.Fatal("session 10 should be expired after index 200")
		}

		// Now try to apply a session-managed entry for the expired session.
		sessionEntries := []proto.Entry{
			{Index: 201, ClientID: 10, SeriesID: 1, Cmd: makeCmd("fail", "fail")},
		}
		sessionResults := make([]sm.Result, 1)
		if err := rsm.Apply(sessionEntries, sessionResults); err != nil {
			t.Fatalf("Apply(expired session) = %v", err)
		}
		if sessionResults[0].Value != sm.ResultSessionExpired {
			t.Errorf("result Value = %d, want ResultSessionExpired", sessionResults[0].Value)
		}
	})
}

// ---------------------------------------------------------------------------
// Session membership: concurrent read of LastApplied during Apply
// ---------------------------------------------------------------------------

func TestMembership_ConcurrentLastAppliedRead(t *testing.T) {
	t.Run("LastApplied is safe to read concurrently with Apply", func(t *testing.T) {
		createFn := sm.NewCreateFunc(func(shardID, replicaID uint64) sm.StateMachine {
			return newTestKV()
		})
		rsm, err := NewStateMachine(1, 1, createFn, t.TempDir(), 100, 100, 0, false)
		if err != nil {
			t.Fatalf("NewStateMachine = %v", err)
		}
		defer rsm.Close()

		const numEntries = 1000
		done := make(chan struct{})

		// Reader goroutine continuously reads LastApplied.
		go func() {
			defer close(done)
			var prev uint64
			for i := 0; i < numEntries*10; i++ {
				current := rsm.LastApplied()
				if current < prev {
					t.Errorf("LastApplied decreased: %d -> %d", prev, current)
					return
				}
				prev = current
				if current >= numEntries {
					return
				}
			}
		}()

		// Writer goroutine applies entries.
		for i := uint64(1); i <= numEntries; i++ {
			entries := []proto.Entry{
				{Index: i, Cmd: makeCmd(fmt.Sprintf("k%d", i), "v")},
			}
			results := make([]sm.Result, 1)
			if err := rsm.Apply(entries, results); err != nil {
				t.Fatalf("Apply(%d) = %v", i, err)
			}
		}

		<-done
		if rsm.LastApplied() != numEntries {
			t.Errorf("final LastApplied = %d, want %d", rsm.LastApplied(), numEntries)
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
		rsm, err := NewStateMachine(1, 1, createFn, t.TempDir(), 100, 100, 0, false)
		if err != nil {
			t.Fatalf("NewStateMachine = %v", err)
		}
		defer rsm.Close()

		if err := rsm.Sessions().Register(10, 0); err != nil {
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
			if err := rsm.Apply(entries, results); err != nil {
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
		if err := rsm.Apply(dupEntries, dupResults); err != nil {
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
	t.Run("response limit exceeded returns error from Apply", func(t *testing.T) {
		createFn := sm.NewCreateFunc(func(shardID, replicaID uint64) sm.StateMachine {
			return newTestKV()
		})
		// maxResponses = 3.
		rsm, err := NewStateMachine(1, 1, createFn, t.TempDir(), 100, 3, 0, false)
		if err != nil {
			t.Fatalf("NewStateMachine = %v", err)
		}
		defer rsm.Close()

		if err := rsm.Sessions().Register(10, 0); err != nil {
			t.Fatalf("Register = %v", err)
		}

		// Apply 3 series to fill up maxResponses.
		for i := uint64(1); i <= 3; i++ {
			entries := []proto.Entry{
				{Index: i, ClientID: 10, SeriesID: i, Cmd: makeCmd(fmt.Sprintf("mr%d", i), "v")},
			}
			results := make([]sm.Result, 1)
			if err := rsm.Apply(entries, results); err != nil {
				t.Fatalf("Apply(%d) = %v", i, err)
			}
		}

		// Fourth series without respondedTo should hit the limit.
		entries := []proto.Entry{
			{Index: 4, ClientID: 10, SeriesID: 4, Cmd: makeCmd("mr4", "v")},
		}
		results := make([]sm.Result, 1)
		err = rsm.Apply(entries, results)
		if err == nil {
			t.Fatal("Apply should fail when response limit exceeded")
		}
	})

	t.Run("respondedTo frees slots allowing new responses", func(t *testing.T) {
		createFn := sm.NewCreateFunc(func(shardID, replicaID uint64) sm.StateMachine {
			return newTestKV()
		})
		// maxResponses = 3.
		rsm, err := NewStateMachine(1, 1, createFn, t.TempDir(), 100, 3, 0, false)
		if err != nil {
			t.Fatalf("NewStateMachine = %v", err)
		}
		defer rsm.Close()

		if err := rsm.Sessions().Register(10, 0); err != nil {
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

// ---------------------------------------------------------------------------
// Session membership: high respondedTo evicts multiple series at once
// ---------------------------------------------------------------------------

func TestMembership_HighRespondedToEvictsMultiple(t *testing.T) {
	t.Run("respondedTo jumps evict all intermediate series", func(t *testing.T) {
		createFn := sm.NewCreateFunc(func(shardID, replicaID uint64) sm.StateMachine {
			return newTestKV()
		})
		rsm, err := NewStateMachine(1, 1, createFn, t.TempDir(), 100, 100, 0, false)
		if err != nil {
			t.Fatalf("NewStateMachine = %v", err)
		}
		defer rsm.Close()

		if err := rsm.Sessions().Register(10, 0); err != nil {
			t.Fatalf("Register = %v", err)
		}

		// Apply series 1, 2, 3 without respondedTo advancement.
		for i := uint64(1); i <= 3; i++ {
			entries := []proto.Entry{
				{Index: i, ClientID: 10, SeriesID: i, Cmd: makeCmd(fmt.Sprintf("h%d", i), "v")},
			}
			results := make([]sm.Result, 1)
			if err := rsm.Apply(entries, results); err != nil {
				t.Fatalf("Apply(%d) = %v", i, err)
			}
		}

		// Now apply series 10 with respondedTo=3, evicting series 1, 2, and 3.
		entries := []proto.Entry{
			{Index: 10, ClientID: 10, SeriesID: 10, RespondedTo: 3, Cmd: makeCmd("h10", "v")},
		}
		results := make([]sm.Result, 1)
		if err := rsm.Apply(entries, results); err != nil {
			t.Fatalf("Apply(10) = %v", err)
		}

		// Replay series 1, 2, 3 should all return SESSION_EXPIRED.
		for _, evicted := range []uint64{1, 2, 3} {
			replayEntries := []proto.Entry{
				{Index: 100 + evicted, ClientID: 10, SeriesID: evicted, Cmd: makeCmd("x", "y")},
			}
			replayResults := make([]sm.Result, 1)
			if err := rsm.Apply(replayEntries, replayResults); err != nil {
				t.Fatalf("Apply(replay %d) = %v", evicted, err)
			}
			if replayResults[0].Value != sm.ResultSessionExpired {
				t.Errorf("replay series %d = %d, want ResultSessionExpired", evicted, replayResults[0].Value)
			}
		}
	})
}

// ---------------------------------------------------------------------------
// Session membership: register and unregister different sessions in same batch
// ---------------------------------------------------------------------------

func TestMembership_RegisterUnregisterInSameBatch(t *testing.T) {
	t.Run("register and unregister different sessions in one batch", func(t *testing.T) {
		createFn := sm.NewCreateFunc(func(shardID, replicaID uint64) sm.StateMachine {
			return newTestKV()
		})
		rsm, err := NewStateMachine(1, 1, createFn, t.TempDir(), 100, 100, 0, false)
		if err != nil {
			t.Fatalf("NewStateMachine = %v", err)
		}
		defer rsm.Close()

		// Pre-register session 100 for unregister.
		if err := rsm.Sessions().Register(100, 0); err != nil {
			t.Fatalf("Register(100) = %v", err)
		}

		entries := []proto.Entry{
			// Register new session 200.
			{Index: 1, ClientID: 200, SeriesID: proto.SessionSeriesIDForRegister, Type: proto.EntryNormal},
			// Unregister session 100.
			{Index: 2, ClientID: 100, SeriesID: proto.SessionSeriesIDForUnregister, Type: proto.EntryNormal},
			// Register new session 300.
			{Index: 3, ClientID: 300, SeriesID: proto.SessionSeriesIDForRegister, Type: proto.EntryNormal},
		}
		results := make([]sm.Result, 3)
		if err := rsm.Apply(entries, results); err != nil {
			t.Fatalf("Apply = %v", err)
		}

		if results[0].Value != 200 {
			t.Errorf("register 200 = %d, want 200", results[0].Value)
		}
		if results[1].Value != 100 {
			t.Errorf("unregister 100 = %d, want 100", results[1].Value)
		}
		if results[2].Value != 300 {
			t.Errorf("register 300 = %d, want 300", results[2].Value)
		}

		if rsm.Sessions().IsRegistered(100) {
			t.Error("session 100 should be unregistered")
		}
		if !rsm.Sessions().IsRegistered(200) {
			t.Error("session 200 should be registered")
		}
		if !rsm.Sessions().IsRegistered(300) {
			t.Error("session 300 should be registered")
		}
		if rsm.Sessions().Count() != 2 {
			t.Errorf("session count = %d, want 2", rsm.Sessions().Count())
		}
	})
}

// ---------------------------------------------------------------------------
// Session membership: session-managed entry for non-existent session
// ---------------------------------------------------------------------------

func TestMembership_SessionManagedEntryNonExistentSession(t *testing.T) {
	t.Run("entry for never-registered session returns SESSION_EXPIRED", func(t *testing.T) {
		createFn := sm.NewCreateFunc(func(shardID, replicaID uint64) sm.StateMachine {
			return newTestKV()
		})
		rsm, err := NewStateMachine(1, 1, createFn, t.TempDir(), 100, 100, 0, false)
		if err != nil {
			t.Fatalf("NewStateMachine = %v", err)
		}
		defer rsm.Close()

		// Apply a session-managed entry without registering the session first.
		entries := []proto.Entry{
			{Index: 1, ClientID: 999, SeriesID: 1, Cmd: makeCmd("nosess", "val")},
		}
		results := make([]sm.Result, 1)
		if err := rsm.Apply(entries, results); err != nil {
			t.Fatalf("Apply = %v", err)
		}
		if results[0].Value != sm.ResultSessionExpired {
			t.Errorf("result = %d, want ResultSessionExpired", results[0].Value)
		}
	})

	t.Run("multiple entries for non-existent sessions all return SESSION_EXPIRED", func(t *testing.T) {
		createFn := sm.NewCreateFunc(func(shardID, replicaID uint64) sm.StateMachine {
			return newTestKV()
		})
		rsm, err := NewStateMachine(1, 1, createFn, t.TempDir(), 100, 100, 0, false)
		if err != nil {
			t.Fatalf("NewStateMachine = %v", err)
		}
		defer rsm.Close()

		entries := []proto.Entry{
			{Index: 1, ClientID: 111, SeriesID: 1, Cmd: makeCmd("a", "b")},
			{Index: 2, ClientID: 222, SeriesID: 1, Cmd: makeCmd("c", "d")},
			{Index: 3, ClientID: 333, SeriesID: 1, Cmd: makeCmd("e", "f")},
		}
		results := make([]sm.Result, 3)
		if err := rsm.Apply(entries, results); err != nil {
			t.Fatalf("Apply = %v", err)
		}
		for i, r := range results {
			if r.Value != sm.ResultSessionExpired {
				t.Errorf("results[%d] = %d, want ResultSessionExpired", i, r.Value)
			}
		}
	})
}

// ---------------------------------------------------------------------------
// Session membership: zero clientID bypasses session deduplication
// ---------------------------------------------------------------------------

func TestMembership_ZeroClientIDBypassesSession(t *testing.T) {
	t.Run("entries with clientID 0 go directly to user SM without session check", func(t *testing.T) {
		createFn := sm.NewCreateFunc(func(shardID, replicaID uint64) sm.StateMachine {
			return newTestKV()
		})
		rsm, err := NewStateMachine(1, 1, createFn, t.TempDir(), 100, 100, 0, false)
		if err != nil {
			t.Fatalf("NewStateMachine = %v", err)
		}
		defer rsm.Close()

		// Apply non-session entries (clientID = 0).
		entries := []proto.Entry{
			{Index: 1, ClientID: 0, SeriesID: 0, Cmd: makeCmd("nosess1", "v1")},
			{Index: 2, ClientID: 0, SeriesID: 0, Cmd: makeCmd("nosess2", "v2")},
		}
		results := make([]sm.Result, 2)
		if err := rsm.Apply(entries, results); err != nil {
			t.Fatalf("Apply = %v", err)
		}

		// Both should be applied (not rejected as SESSION_EXPIRED).
		for i, r := range results {
			if r.Value == sm.ResultSessionExpired {
				t.Errorf("results[%d] should not be SESSION_EXPIRED for clientID=0", i)
			}
		}

		// Verify data was stored.
		val, err := rsm.Lookup(context.Background(), "nosess1")
		if err != nil {
			t.Fatalf("Lookup = %v", err)
		}
		if val.(string) != "v1" {
			t.Errorf("Lookup(nosess1) = %q, want %q", val, "v1")
		}
	})

	t.Run("same non-session entry applied twice both execute", func(t *testing.T) {
		createFn := sm.NewCreateFunc(func(shardID, replicaID uint64) sm.StateMachine {
			return newTestKV()
		})
		rsm, err := NewStateMachine(1, 1, createFn, t.TempDir(), 100, 100, 0, false)
		if err != nil {
			t.Fatalf("NewStateMachine = %v", err)
		}
		defer rsm.Close()

		// Apply same key twice with clientID=0 - no dedup should happen.
		entries1 := []proto.Entry{
			{Index: 1, ClientID: 0, Cmd: makeCmd("dup", "first")},
		}
		r1 := make([]sm.Result, 1)
		if err := rsm.Apply(entries1, r1); err != nil {
			t.Fatalf("Apply(1) = %v", err)
		}

		entries2 := []proto.Entry{
			{Index: 2, ClientID: 0, Cmd: makeCmd("dup", "second")},
		}
		r2 := make([]sm.Result, 1)
		if err := rsm.Apply(entries2, r2); err != nil {
			t.Fatalf("Apply(2) = %v", err)
		}

		// Second write should have overwritten the first.
		val, err := rsm.Lookup(context.Background(), "dup")
		if err != nil {
			t.Fatalf("Lookup = %v", err)
		}
		if val.(string) != "second" {
			t.Errorf("Lookup(dup) = %q, want %q", val, "second")
		}
	})
}
