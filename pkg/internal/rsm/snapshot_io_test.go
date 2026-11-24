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
	"crypto/sha256"
	"fmt"
	"io"
	"sync"
	"testing"

	"github.com/jeremyhahn/go-quicraft/pkg/proto"
	"github.com/jeremyhahn/go-quicraft/pkg/sm"
)

// ---------------------------------------------------------------------------
// Snapshot IO: malformed input (bit-flip, garbage, empty, truncation) rejected
// ---------------------------------------------------------------------------

func TestSnapshot_MalformedInputRejected(t *testing.T) {
	createFn := sm.NewCreateFunc(func(shardID, replicaID uint64) sm.StateMachine {
		return newTestKV()
	})
	rsm1, err := NewStateMachine(1, 1, createFn, t.TempDir(), testPolicy(100, 100, 0), nil, false)
	if err != nil {
		t.Fatalf("NewStateMachine = %v", err)
	}
	defer rsm1.Close()

	// Register a session and apply data to create a non-trivial snapshot.
	if err := rsm1.Sessions().Register(10, 0, 0); err != nil {
		t.Fatalf("Register = %v", err)
	}
	entries := []proto.Entry{
		{Index: 1, ClientID: 10, SeriesID: 1, Cmd: makeCmd("snap", "truncate-test-value")},
	}
	results := make([]sm.Result, 1)
	if err := rsm1.Apply(entries, results); err != nil {
		t.Fatalf("Apply = %v", err)
	}

	var buf bytes.Buffer
	stopper := make(chan struct{})
	if err := rsm1.SaveSnapshot(context.Background(), &buf, stopper); err != nil {
		t.Fatalf("SaveSnapshot = %v", err)
	}
	full := buf.Bytes()
	if len(full) < 8 {
		t.Fatal("snapshot too short")
	}

	// Bit-flip the first bytes (session count in the session marshal format).
	flipped := make([]byte, len(full))
	copy(flipped, full)
	flipped[0] ^= 0xFF
	flipped[1] ^= 0xFF

	cases := []struct {
		name string
		data []byte
	}{
		{"bit-flipped session count", flipped},
		{"random bytes", []byte{0xDE, 0xAD, 0xBE, 0xEF, 0xCA, 0xFE, 0xBA, 0xBE,
			0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08}},
		{"empty reader", nil},
		{"truncated at 25%", full[:len(full)*25/100]},
		{"truncated at 50%", full[:len(full)*50/100]},
		{"truncated at 75%", full[:len(full)*75/100]},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rsm2, err := NewStateMachine(1, 1, createFn, t.TempDir(), testPolicy(100, 100, 0), nil, false)
			if err != nil {
				t.Fatalf("NewStateMachine = %v", err)
			}
			defer rsm2.Close()

			if err := rsm2.RecoverFromSnapshot(context.Background(), bytes.NewReader(tc.data), stopper); err == nil {
				t.Fatal("RecoverFromSnapshot = nil, want an error")
			}
		})
	}
}

// ---------------------------------------------------------------------------
// Snapshot IO: multiple sessions with multiple responses per session
// ---------------------------------------------------------------------------

func TestSnapshot_MultipleSessionsMultipleResponses(t *testing.T) {
	t.Run("snapshot preserves all sessions and all cached responses", func(t *testing.T) {
		createFn := sm.NewCreateFunc(func(shardID, replicaID uint64) sm.StateMachine {
			return newTestKV()
		})
		rsm1, err := NewStateMachine(1, 1, createFn, t.TempDir(), testPolicy(100, 100, 0), nil, false)
		if err != nil {
			t.Fatalf("NewStateMachine = %v", err)
		}
		defer rsm1.Close()

		// Register 5 sessions.
		for i := uint64(1); i <= 5; i++ {
			if err := rsm1.Sessions().Register(i*100, 0, 0); err != nil {
				t.Fatalf("Register(%d) = %v", i*100, err)
			}
		}

		// Apply 3 entries per session (15 total), each with its own seriesID.
		idx := uint64(1)
		type appliedEntry struct {
			clientID uint64
			seriesID uint64
			result   sm.Result
		}
		var applied []appliedEntry

		for clientNum := uint64(1); clientNum <= 5; clientNum++ {
			clientID := clientNum * 100
			for seriesID := uint64(1); seriesID <= 3; seriesID++ {
				key := fmt.Sprintf("c%d-s%d", clientNum, seriesID)
				val := fmt.Sprintf("val%d%d", clientNum, seriesID)
				entries := []proto.Entry{
					{Index: idx, ClientID: clientID, SeriesID: seriesID, Cmd: makeCmd(key, val)},
				}
				results := make([]sm.Result, 1)
				if err := rsm1.Apply(entries, results); err != nil {
					t.Fatalf("Apply(%d) = %v", idx, err)
				}
				applied = append(applied, appliedEntry{clientID, seriesID, results[0]})
				idx++
			}
		}

		// Save and restore snapshot.
		var buf bytes.Buffer
		stopper := make(chan struct{})
		if err := rsm1.SaveSnapshot(context.Background(), &buf, stopper); err != nil {
			t.Fatalf("SaveSnapshot = %v", err)
		}

		rsm2, err := NewStateMachine(1, 1, createFn, t.TempDir(), testPolicy(100, 100, 0), nil, false)
		if err != nil {
			t.Fatalf("NewStateMachine = %v", err)
		}
		defer rsm2.Close()

		if err := rsm2.RecoverFromSnapshot(context.Background(), &buf, stopper); err != nil {
			t.Fatalf("RecoverFromSnapshot = %v", err)
		}

		// Verify all sessions are registered.
		if rsm2.Sessions().Count() != 5 {
			t.Errorf("session count = %d, want 5", rsm2.Sessions().Count())
		}

		// Verify all cached responses survived the snapshot.
		for _, a := range applied {
			cached, isDup, err := rsm2.Sessions().CheckDuplicate(a.clientID, a.seriesID, 1000)
			if err != nil {
				t.Fatalf("CheckDuplicate(client=%d, series=%d) = %v",
					a.clientID, a.seriesID, err)
			}
			if !isDup {
				t.Errorf("client %d series %d should be duplicate after restore",
					a.clientID, a.seriesID)
			}
			if cached.Value != a.result.Value {
				t.Errorf("client %d series %d cached value = %d, want %d",
					a.clientID, a.seriesID, cached.Value, a.result.Value)
			}
		}

		// Verify KV data survived.
		for clientNum := uint64(1); clientNum <= 5; clientNum++ {
			for seriesID := uint64(1); seriesID <= 3; seriesID++ {
				key := fmt.Sprintf("c%d-s%d", clientNum, seriesID)
				expected := fmt.Sprintf("val%d%d", clientNum, seriesID)
				val, err := rsm2.Lookup(context.Background(), key)
				if err != nil {
					t.Fatalf("Lookup(%s) = %v", key, err)
				}
				if val.(string) != expected {
					t.Errorf("Lookup(%s) = %q, want %q", key, val, expected)
				}
			}
		}
	})
}

// ---------------------------------------------------------------------------
// Snapshot IO: concurrent save then restore to verify thread safety
// ---------------------------------------------------------------------------

func TestSnapshot_ConcurrentSaveAndRestore(t *testing.T) {
	t.Run("multiple goroutines save and then each restores independently", func(t *testing.T) {
		createFn := sm.NewCreateFunc(func(shardID, replicaID uint64) sm.StateMachine {
			return newTestKV()
		})
		rsm1, err := NewStateMachine(1, 1, createFn, t.TempDir(), testPolicy(100, 100, 0), nil, false)
		if err != nil {
			t.Fatalf("NewStateMachine = %v", err)
		}
		defer rsm1.Close()

		if err := rsm1.Sessions().Register(10, 0, 0); err != nil {
			t.Fatalf("Register = %v", err)
		}
		entries := []proto.Entry{
			{Index: 1, ClientID: 10, SeriesID: 1, Cmd: makeCmd("shared", "value")},
		}
		results := make([]sm.Result, 1)
		if err := rsm1.Apply(entries, results); err != nil {
			t.Fatalf("Apply = %v", err)
		}

		// Concurrently save 4 snapshots.
		const numGoroutines = 4
		stopper := make(chan struct{})
		snapshots := make([][]byte, numGoroutines)
		saveErrs := make([]error, numGoroutines)
		var wg sync.WaitGroup

		for i := 0; i < numGoroutines; i++ {
			wg.Add(1)
			go func(idx int) {
				defer wg.Done()
				var buf bytes.Buffer
				saveErrs[idx] = rsm1.SaveSnapshot(context.Background(), &buf, stopper)
				if saveErrs[idx] == nil {
					snapshots[idx] = buf.Bytes()
				}
			}(i)
		}
		wg.Wait()

		for i, e := range saveErrs {
			if e != nil {
				t.Fatalf("goroutine %d SaveSnapshot = %v", i, e)
			}
		}

		// All snapshots should produce the same bytes (deterministic).
		for i := 1; i < numGoroutines; i++ {
			if !bytes.Equal(snapshots[0], snapshots[i]) {
				t.Errorf("snapshot %d differs from snapshot 0 (len %d vs %d)",
					i, len(snapshots[i]), len(snapshots[0]))
			}
		}

		// Restore each snapshot independently and verify.
		for i := 0; i < numGoroutines; i++ {
			restored, err := NewStateMachine(1, 1, createFn, t.TempDir(), testPolicy(100, 100, 0), nil, false)
			if err != nil {
				t.Fatalf("NewStateMachine(%d) = %v", i, err)
			}
			if err := restored.RecoverFromSnapshot(
				context.Background(), bytes.NewReader(snapshots[i]), stopper,
			); err != nil {
				t.Fatalf("RecoverFromSnapshot(%d) = %v", i, err)
			}
			val, err := restored.Lookup(context.Background(), "shared")
			if err != nil {
				t.Fatalf("Lookup(%d) = %v", i, err)
			}
			if val.(string) != "value" {
				t.Errorf("Lookup(%d) = %q, want %q", i, val, "value")
			}
			restored.Close()
		}
	})
}

// ---------------------------------------------------------------------------
// Snapshot IO: unregistered session excluded from snapshot
// ---------------------------------------------------------------------------

func TestSnapshot_UnregisteredSessionExcluded(t *testing.T) {
	t.Run("unregistered session is not present in restored snapshot", func(t *testing.T) {
		createFn := sm.NewCreateFunc(func(shardID, replicaID uint64) sm.StateMachine {
			return newTestKV()
		})
		rsm1, err := NewStateMachine(1, 1, createFn, t.TempDir(), testPolicy(100, 100, 0), nil, false)
		if err != nil {
			t.Fatalf("NewStateMachine = %v", err)
		}
		defer rsm1.Close()

		// Register two sessions.
		if err := rsm1.Sessions().Register(10, 0, 0); err != nil {
			t.Fatalf("Register(10) = %v", err)
		}
		if err := rsm1.Sessions().Register(20, 0, 0); err != nil {
			t.Fatalf("Register(20) = %v", err)
		}

		// Unregister session 10 via Apply.
		unregEntry := proto.Entry{
			Index:    1,
			ClientID: 10,
			SeriesID: proto.SessionSeriesIDForUnregister,
			Type:     proto.EntryNormal,
		}
		results := make([]sm.Result, 1)
		if err := rsm1.Apply([]proto.Entry{unregEntry}, results); err != nil {
			t.Fatalf("Apply(unregister) = %v", err)
		}

		// Snapshot.
		var buf bytes.Buffer
		stopper := make(chan struct{})
		if err := rsm1.SaveSnapshot(context.Background(), &buf, stopper); err != nil {
			t.Fatalf("SaveSnapshot = %v", err)
		}

		// Restore.
		rsm2, err := NewStateMachine(1, 1, createFn, t.TempDir(), testPolicy(100, 100, 0), nil, false)
		if err != nil {
			t.Fatalf("NewStateMachine = %v", err)
		}
		defer rsm2.Close()

		if err := rsm2.RecoverFromSnapshot(context.Background(), &buf, stopper); err != nil {
			t.Fatalf("RecoverFromSnapshot = %v", err)
		}

		// Session 10 should NOT be present.
		if rsm2.Sessions().IsRegistered(10) {
			t.Error("session 10 should NOT be registered after unregister + snapshot")
		}
		// Session 20 should be present.
		if !rsm2.Sessions().IsRegistered(20) {
			t.Error("session 20 should be registered after snapshot")
		}
		if rsm2.Sessions().Count() != 1 {
			t.Errorf("session count = %d, want 1", rsm2.Sessions().Count())
		}
	})
}

// ---------------------------------------------------------------------------
// Snapshot IO: multiple sequential snapshot/restore cycles
// ---------------------------------------------------------------------------

func TestSnapshot_MultipleSequentialCycles(t *testing.T) {
	t.Run("three sequential snapshot-restore cycles preserve state", func(t *testing.T) {
		createFn := sm.NewCreateFunc(func(shardID, replicaID uint64) sm.StateMachine {
			return newTestKV()
		})

		current, err := NewStateMachine(1, 1, createFn, t.TempDir(), testPolicy(100, 100, 0), nil, false)
		if err != nil {
			t.Fatalf("NewStateMachine = %v", err)
		}

		if err := current.Sessions().Register(10, 0, 0); err != nil {
			t.Fatalf("Register = %v", err)
		}

		stopper := make(chan struct{})

		for cycle := 0; cycle < 3; cycle++ {
			// Apply a unique entry per cycle.
			key := fmt.Sprintf("cycle-%d", cycle)
			val := fmt.Sprintf("value-%d", cycle)
			entries := []proto.Entry{
				{Index: uint64(cycle*10 + 1), ClientID: 10, SeriesID: uint64(cycle + 1), Cmd: makeCmd(key, val)},
			}
			results := make([]sm.Result, 1)
			if err := applyContiguous(t, current, entries, results); err != nil {
				t.Fatalf("cycle %d: Apply = %v", cycle, err)
			}

			// Snapshot.
			var buf bytes.Buffer
			if err := current.SaveSnapshot(context.Background(), &buf, stopper); err != nil {
				t.Fatalf("cycle %d: SaveSnapshot = %v", cycle, err)
			}
			current.Close()

			// Restore into a new SM.
			current, err = NewStateMachine(1, 1, createFn, t.TempDir(), testPolicy(100, 100, 0), nil, false)
			if err != nil {
				t.Fatalf("cycle %d: NewStateMachine = %v", cycle, err)
			}
			if err := current.RecoverFromSnapshot(context.Background(), &buf, stopper); err != nil {
				t.Fatalf("cycle %d: RecoverFromSnapshot = %v", cycle, err)
			}
		}

		// After 3 cycles, verify all data from all cycles is present.
		for cycle := 0; cycle < 3; cycle++ {
			key := fmt.Sprintf("cycle-%d", cycle)
			expected := fmt.Sprintf("value-%d", cycle)
			val, err := current.Lookup(context.Background(), key)
			if err != nil {
				t.Fatalf("Lookup(%s) = %v", key, err)
			}
			if val.(string) != expected {
				t.Errorf("Lookup(%s) = %q, want %q", key, val, expected)
			}
		}

		// Verify session survived all cycles.
		if !current.Sessions().IsRegistered(10) {
			t.Error("session 10 should be registered after 3 cycles")
		}
		current.Close()
	})
}

// ---------------------------------------------------------------------------
// Snapshot IO: save snapshot with failing writer
// ---------------------------------------------------------------------------

// limitedWriter fails after writing limit bytes.
type limitedWriter struct {
	limit   int
	written int
}

func (w *limitedWriter) Write(p []byte) (int, error) {
	if w.written+len(p) > w.limit {
		remaining := w.limit - w.written
		if remaining > 0 {
			w.written += remaining
			return remaining, io.ErrShortWrite
		}
		return 0, io.ErrShortWrite
	}
	w.written += len(p)
	return len(p), nil
}

func TestSnapshot_SaveWithFailingWriter(t *testing.T) {
	t.Run("partial write returns error", func(t *testing.T) {
		createFn := sm.NewCreateFunc(func(shardID, replicaID uint64) sm.StateMachine {
			return newTestKV()
		})
		rsm1, err := NewStateMachine(1, 1, createFn, t.TempDir(), testPolicy(100, 100, 0), nil, false)
		if err != nil {
			t.Fatalf("NewStateMachine = %v", err)
		}
		defer rsm1.Close()

		if err := rsm1.Sessions().Register(10, 0, 0); err != nil {
			t.Fatalf("Register = %v", err)
		}
		entries := []proto.Entry{
			{Index: 1, ClientID: 10, SeriesID: 1, Cmd: makeCmd("fw", "fv")},
		}
		results := make([]sm.Result, 1)
		if err := rsm1.Apply(entries, results); err != nil {
			t.Fatalf("Apply = %v", err)
		}

		// Writer that fails after 2 bytes.
		w := &limitedWriter{limit: 2}
		stopper := make(chan struct{})
		err = rsm1.SaveSnapshot(context.Background(), w, stopper)
		if err == nil {
			t.Fatal("SaveSnapshot should fail with limited writer")
		}
	})
}

// ---------------------------------------------------------------------------
// Snapshot IO: snapshot of SM with Hasher support verifies hash consistency
// ---------------------------------------------------------------------------

func TestSnapshot_HasherSMRoundTrip(t *testing.T) {
	t.Run("SM with Hasher preserves hash across snapshot", func(t *testing.T) {
		createFn := func(shardID, replicaID uint64) interface{} {
			return &testHasherSM{testKV: testKV{data: make(map[string]string)}}
		}
		rsm1, err := NewStateMachine(1, 1, createFn, t.TempDir(), testPolicy(100, 100, 0), nil, false)
		if err != nil {
			t.Fatalf("NewStateMachine = %v", err)
		}
		defer rsm1.Close()

		entries := []proto.Entry{
			{Index: 1, Cmd: makeCmd("hk", "hv")},
		}
		results := make([]sm.Result, 1)
		if err := rsm1.Apply(entries, results); err != nil {
			t.Fatalf("Apply = %v", err)
		}

		hash1, supported, err := rsm1.GetHash()
		if err != nil {
			t.Fatalf("GetHash = %v", err)
		}
		if !supported {
			t.Fatal("GetHash should be supported")
		}

		// Snapshot and restore.
		var buf bytes.Buffer
		stopper := make(chan struct{})
		if err := rsm1.SaveSnapshot(context.Background(), &buf, stopper); err != nil {
			t.Fatalf("SaveSnapshot = %v", err)
		}

		rsm2, err := NewStateMachine(1, 1, createFn, t.TempDir(), testPolicy(100, 100, 0), nil, false)
		if err != nil {
			t.Fatalf("NewStateMachine = %v", err)
		}
		defer rsm2.Close()

		if err := rsm2.RecoverFromSnapshot(context.Background(), &buf, stopper); err != nil {
			t.Fatalf("RecoverFromSnapshot = %v", err)
		}

		hash2, _, err := rsm2.GetHash()
		if err != nil {
			t.Fatalf("GetHash after restore = %v", err)
		}
		if hash1 != hash2 {
			t.Errorf("hash mismatch: before=%d, after=%d", hash1, hash2)
		}
	})
}

// ---------------------------------------------------------------------------
// Snapshot IO: recover from snapshot replaces existing state
// ---------------------------------------------------------------------------

func TestSnapshot_RecoverReplacesExistingState(t *testing.T) {
	t.Run("recovery overwrites pre-existing sessions and data", func(t *testing.T) {
		createFn := sm.NewCreateFunc(func(shardID, replicaID uint64) sm.StateMachine {
			return newTestKV()
		})

		// First SM: register session 10, apply data.
		rsm1, err := NewStateMachine(1, 1, createFn, t.TempDir(), testPolicy(100, 100, 0), nil, false)
		if err != nil {
			t.Fatalf("NewStateMachine(1) = %v", err)
		}
		defer rsm1.Close()

		if err := rsm1.Sessions().Register(10, 0, 0); err != nil {
			t.Fatalf("Register(10) = %v", err)
		}
		entries := []proto.Entry{
			{Index: 1, ClientID: 10, SeriesID: 1, Cmd: makeCmd("rk1", "rv1")},
		}
		results := make([]sm.Result, 1)
		if err := rsm1.Apply(entries, results); err != nil {
			t.Fatalf("Apply = %v", err)
		}

		// Save snapshot.
		var buf bytes.Buffer
		stopper := make(chan struct{})
		if err := rsm1.SaveSnapshot(context.Background(), &buf, stopper); err != nil {
			t.Fatalf("SaveSnapshot = %v", err)
		}

		// Second SM: register session 99 and apply different data.
		rsm2, err := NewStateMachine(1, 1, createFn, t.TempDir(), testPolicy(100, 100, 0), nil, false)
		if err != nil {
			t.Fatalf("NewStateMachine(2) = %v", err)
		}
		defer rsm2.Close()

		if err := rsm2.Sessions().Register(99, 0, 0); err != nil {
			t.Fatalf("Register(99) = %v", err)
		}
		entries2 := []proto.Entry{
			{Index: 1, ClientID: 99, SeriesID: 1, Cmd: makeCmd("other", "stuff")},
		}
		results2 := make([]sm.Result, 1)
		if err := rsm2.Apply(entries2, results2); err != nil {
			t.Fatalf("Apply(2) = %v", err)
		}

		// Recover rsm2 from rsm1's snapshot. This should REPLACE rsm2's state.
		if err := rsm2.RecoverFromSnapshot(context.Background(), bytes.NewReader(buf.Bytes()), stopper); err != nil {
			t.Fatalf("RecoverFromSnapshot = %v", err)
		}

		// Session 99 should be gone (replaced by snapshot state).
		if rsm2.Sessions().IsRegistered(99) {
			t.Error("session 99 should be gone after recovery from different snapshot")
		}
		// Session 10 should be present.
		if !rsm2.Sessions().IsRegistered(10) {
			t.Error("session 10 should be present from snapshot")
		}
		// Data from rsm1 should be present.
		val, err := rsm2.Lookup(context.Background(), "rk1")
		if err != nil {
			t.Fatalf("Lookup = %v", err)
		}
		if val.(string) != "rv1" {
			t.Errorf("Lookup(rk1) = %q, want %q", val, "rv1")
		}
	})
}

// ---------------------------------------------------------------------------
// Snapshot IO: empty state machine roundtrip
// ---------------------------------------------------------------------------

func TestSnapshot_EmptyStateMachineRoundTrip(t *testing.T) {
	t.Run("SM with no data or sessions snapshots and recovers cleanly", func(t *testing.T) {
		createFn := sm.NewCreateFunc(func(shardID, replicaID uint64) sm.StateMachine {
			return newTestKV()
		})
		rsm1, err := NewStateMachine(1, 1, createFn, t.TempDir(), testPolicy(100, 100, 0), nil, false)
		if err != nil {
			t.Fatalf("NewStateMachine = %v", err)
		}
		defer rsm1.Close()

		var buf bytes.Buffer
		stopper := make(chan struct{})
		if err := rsm1.SaveSnapshot(context.Background(), &buf, stopper); err != nil {
			t.Fatalf("SaveSnapshot = %v", err)
		}
		if buf.Len() == 0 {
			t.Fatal("empty SM snapshot should still have header data")
		}

		rsm2, err := NewStateMachine(1, 1, createFn, t.TempDir(), testPolicy(100, 100, 0), nil, false)
		if err != nil {
			t.Fatalf("NewStateMachine(2) = %v", err)
		}
		defer rsm2.Close()

		if err := rsm2.RecoverFromSnapshot(context.Background(), bytes.NewReader(buf.Bytes()), stopper); err != nil {
			t.Fatalf("RecoverFromSnapshot = %v", err)
		}

		if rsm2.Sessions().Count() != 0 {
			t.Errorf("sessions count = %d, want 0", rsm2.Sessions().Count())
		}

		// Re-saving the recovered empty SM reproduces the original bytes.
		var buf2 bytes.Buffer
		if err := rsm2.SaveSnapshot(context.Background(), &buf2, stopper); err != nil {
			t.Fatalf("SaveSnapshot(2) = %v", err)
		}
		if sha256.Sum256(buf2.Bytes()) != sha256.Sum256(buf.Bytes()) {
			t.Error("empty SM snapshot hashes should match after roundtrip")
		}
	})
}
