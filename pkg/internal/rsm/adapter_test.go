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
	"errors"
	"io"
	"testing"

	"github.com/jeremyhahn/go-quicraft/pkg/sm"
)

func TestRegularSMWrapper(t *testing.T) {
	t.Run("update and lookup", func(t *testing.T) {
		kv := newTestKV()
		w := &regularSMWrapper{sm: kv}

		entries := []sm.Entry{
			{Index: 1, Cmd: makeCmd("k1", "v1")},
		}
		results := make([]sm.Result, 1)
		if err := w.Update(context.Background(), entries, results); err != nil {
			t.Fatalf("Update = %v", err)
		}
		if results[0].Value != 1 {
			t.Errorf("results[0].Value = %d, want 1", results[0].Value)
		}

		val, err := w.Lookup(context.Background(), "k1")
		if err != nil {
			t.Fatalf("Lookup = %v", err)
		}
		if val.(string) != "v1" {
			t.Errorf("Lookup = %q, want %q", val, "v1")
		}
	})

	t.Run("snapshot and recover", func(t *testing.T) {
		kv := newTestKV()
		w := &regularSMWrapper{sm: kv}

		entries := []sm.Entry{
			{Index: 1, Cmd: makeCmd("snap", "data")},
		}
		results := make([]sm.Result, 1)
		if err := w.Update(context.Background(), entries, results); err != nil {
			t.Fatalf("Update = %v", err)
		}

		var buf bytes.Buffer
		stopper := make(chan struct{})
		if err := w.SaveSnapshot(context.Background(), &buf, stopper); err != nil {
			t.Fatalf("SaveSnapshot = %v", err)
		}

		kv2 := newTestKV()
		w2 := &regularSMWrapper{sm: kv2}
		if err := w2.RecoverFromSnapshot(context.Background(), &buf, stopper); err != nil {
			t.Fatalf("RecoverFromSnapshot = %v", err)
		}

		val, err := w2.Lookup(context.Background(), "snap")
		if err != nil {
			t.Fatalf("Lookup = %v", err)
		}
		if val.(string) != "data" {
			t.Errorf("Lookup = %q, want %q", val, "data")
		}
	})

	t.Run("open returns zero", func(t *testing.T) {
		w := &regularSMWrapper{sm: newTestKV()}
		lastApplied, err := w.Open(context.Background(), t.TempDir(), make(chan struct{}))
		if err != nil {
			t.Fatalf("Open = %v", err)
		}
		if lastApplied != 0 {
			t.Errorf("lastApplied = %d, want 0", lastApplied)
		}
	})

	t.Run("NALookup not supported", func(t *testing.T) {
		w := &regularSMWrapper{sm: newTestKV()}
		result, supported, err := w.NALookup([]byte("key"))
		if err != nil {
			t.Fatalf("NALookup = %v", err)
		}
		if supported {
			t.Error("NALookup should return false for testKV")
		}
		if result != nil {
			t.Errorf("NALookup result = %v, want nil", result)
		}
	})

	t.Run("GetHash not supported", func(t *testing.T) {
		w := &regularSMWrapper{sm: newTestKV()}
		hash, supported, err := w.GetHash()
		if err != nil {
			t.Fatalf("GetHash = %v", err)
		}
		if supported {
			t.Error("GetHash should return false for testKV")
		}
		if hash != 0 {
			t.Errorf("GetHash = %d, want 0", hash)
		}
	})
}

// testHasherSM implements StateMachine + Hasher + NALookup for testing.
type testHasherSM struct {
	testKV
}

func (h *testHasherSM) GetHash() (uint64, error) {
	return 42, nil
}

func (h *testHasherSM) BytesLookup(key []byte) ([]byte, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	val, ok := h.data[string(key)]
	if !ok {
		return nil, nil
	}
	return []byte(val), nil
}

// testHasherSMError returns errors from GetHash and BytesLookup.
type testHasherSMError struct {
	testKV
}

func (h *testHasherSMError) GetHash() (uint64, error) {
	return 0, &testError{}
}

func (h *testHasherSMError) BytesLookup(_ []byte) ([]byte, error) {
	return nil, &testError{}
}

type testError struct{}

func (e *testError) Error() string { return "test error" }

func TestRegularSMWrapper_NALookupSupported(t *testing.T) {
	kv := &testHasherSM{testKV: testKV{data: make(map[string]string)}}
	kv.data["hello"] = "world"
	w := &regularSMWrapper{sm: kv}

	result, supported, err := w.NALookup([]byte("hello"))
	if err != nil {
		t.Fatalf("NALookup = %v", err)
	}
	if !supported {
		t.Fatal("NALookup should return true for testHasherSM")
	}
	if string(result) != "world" {
		t.Errorf("NALookup = %q, want %q", result, "world")
	}
}

func TestRegularSMWrapper_NALookupError(t *testing.T) {
	kv := &testHasherSMError{testKV: testKV{data: make(map[string]string)}}
	w := &regularSMWrapper{sm: kv}

	result, supported, err := w.NALookup([]byte("key"))
	if !supported {
		t.Fatal("NALookup should return true even on error")
	}
	if err == nil {
		t.Fatal("NALookup should return error")
	}
	var te *testError
	if !errors.As(err, &te) {
		t.Errorf("NALookup error = %T, want *testError", err)
	}
	if result != nil {
		t.Errorf("NALookup result = %v, want nil on error", result)
	}
}

func TestRegularSMWrapper_GetHashSupported(t *testing.T) {
	kv := &testHasherSM{testKV: testKV{data: make(map[string]string)}}
	w := &regularSMWrapper{sm: kv}

	hash, supported, err := w.GetHash()
	if err != nil {
		t.Fatalf("GetHash = %v", err)
	}
	if !supported {
		t.Fatal("GetHash should return true for testHasherSM")
	}
	if hash != 42 {
		t.Errorf("GetHash = %d, want 42", hash)
	}
}

func TestRegularSMWrapper_GetHashError(t *testing.T) {
	kv := &testHasherSMError{testKV: testKV{data: make(map[string]string)}}
	w := &regularSMWrapper{sm: kv}

	hash, supported, err := w.GetHash()
	if !supported {
		t.Fatal("GetHash should return true even on error")
	}
	if err == nil {
		t.Fatal("GetHash should return error")
	}
	var te *testError
	if !errors.As(err, &te) {
		t.Errorf("GetHash error = %T, want *testError", err)
	}
	if hash != 0 {
		t.Errorf("GetHash = %d, want 0 on error", hash)
	}
}

func TestConcurrentSMWrapper(t *testing.T) {
	t.Run("update and lookup", func(t *testing.T) {
		kv := newTestConcurrentKV()
		w := &concurrentSMWrapper{sm: kv}

		entries := []sm.Entry{
			{Index: 1, Cmd: makeCmd("ck", "cv")},
		}
		results := make([]sm.Result, 1)
		if err := w.Update(context.Background(), entries, results); err != nil {
			t.Fatalf("Update = %v", err)
		}

		val, err := w.Lookup(context.Background(), "ck")
		if err != nil {
			t.Fatalf("Lookup = %v", err)
		}
		if val.(string) != "cv" {
			t.Errorf("Lookup = %q, want %q", val, "cv")
		}
	})

	t.Run("snapshot calls PrepareSnapshot then SaveSnapshot", func(t *testing.T) {
		kv := newTestConcurrentKV()
		w := &concurrentSMWrapper{sm: kv}

		entries := []sm.Entry{
			{Index: 1, Cmd: makeCmd("cs", "cd")},
		}
		results := make([]sm.Result, 1)
		if err := w.Update(context.Background(), entries, results); err != nil {
			t.Fatalf("Update = %v", err)
		}

		var buf bytes.Buffer
		stopper := make(chan struct{})
		if err := w.SaveSnapshot(context.Background(), &buf, stopper); err != nil {
			t.Fatalf("SaveSnapshot = %v", err)
		}

		kv2 := newTestConcurrentKV()
		w2 := &concurrentSMWrapper{sm: kv2}
		if err := w2.RecoverFromSnapshot(context.Background(), &buf, stopper); err != nil {
			t.Fatalf("RecoverFromSnapshot = %v", err)
		}

		val, err := w2.Lookup(context.Background(), "cs")
		if err != nil {
			t.Fatalf("Lookup = %v", err)
		}
		if val.(string) != "cd" {
			t.Errorf("Lookup = %q, want %q", val, "cd")
		}
	})

	t.Run("open returns zero", func(t *testing.T) {
		w := &concurrentSMWrapper{sm: newTestConcurrentKV()}
		lastApplied, err := w.Open(context.Background(), t.TempDir(), make(chan struct{}))
		if err != nil {
			t.Fatalf("Open = %v", err)
		}
		if lastApplied != 0 {
			t.Errorf("lastApplied = %d, want 0", lastApplied)
		}
	})
}

func TestOnDiskSMWrapper(t *testing.T) {
	t.Run("update and lookup", func(t *testing.T) {
		kv := newTestOnDiskKV()
		w := &onDiskSMWrapper{sm: kv}

		entries := []sm.Entry{
			{Index: 1, Cmd: makeCmd("dk", "dv")},
		}
		results := make([]sm.Result, 1)
		if err := w.Update(context.Background(), entries, results); err != nil {
			t.Fatalf("Update = %v", err)
		}

		val, err := w.Lookup(context.Background(), "dk")
		if err != nil {
			t.Fatalf("Lookup = %v", err)
		}
		if val.(string) != "dv" {
			t.Errorf("Lookup = %q, want %q", val, "dv")
		}
	})

	t.Run("snapshot calls Sync, PrepareSnapshot, SaveSnapshot", func(t *testing.T) {
		kv := newTestOnDiskKV()
		w := &onDiskSMWrapper{sm: kv}

		entries := []sm.Entry{
			{Index: 1, Cmd: makeCmd("ds", "dd")},
		}
		results := make([]sm.Result, 1)
		if err := w.Update(context.Background(), entries, results); err != nil {
			t.Fatalf("Update = %v", err)
		}

		var buf bytes.Buffer
		stopper := make(chan struct{})
		if err := w.SaveSnapshot(context.Background(), &buf, stopper); err != nil {
			t.Fatalf("SaveSnapshot = %v", err)
		}

		kv2 := newTestOnDiskKV()
		w2 := &onDiskSMWrapper{sm: kv2}
		if err := w2.RecoverFromSnapshot(context.Background(), &buf, stopper); err != nil {
			t.Fatalf("RecoverFromSnapshot = %v", err)
		}

		val, err := w2.Lookup(context.Background(), "ds")
		if err != nil {
			t.Fatalf("Lookup = %v", err)
		}
		if val.(string) != "dd" {
			t.Errorf("Lookup = %q, want %q", val, "dd")
		}
	})
}

func TestOnDiskSMWrapper_OpenReturnsLastApplied(t *testing.T) {
	kv := &testOnDiskKVWithApplied{
		testOnDiskKV: *newTestOnDiskKV(),
		lastApplied:  42,
	}
	w := &onDiskSMWrapper{sm: kv}
	lastApplied, err := w.Open(context.Background(), t.TempDir(), make(chan struct{}))
	if err != nil {
		t.Fatalf("Open = %v", err)
	}
	if lastApplied != 42 {
		t.Errorf("lastApplied = %d, want 42", lastApplied)
	}
}

// ---------------------------------------------------------------------------
// Concurrent SM wrapper: PrepareSnapshot error propagation
// ---------------------------------------------------------------------------

func TestConcurrentSMWrapper_PrepareSnapshotError(t *testing.T) {
	t.Run("prepare snapshot error stops save", func(t *testing.T) {
		kv := &testConcurrentKVPrepError{testConcurrentKV: *newTestConcurrentKV()}
		w := &concurrentSMWrapper{sm: kv}

		var buf bytes.Buffer
		stopper := make(chan struct{})
		err := w.SaveSnapshot(context.Background(), &buf, stopper)
		if err == nil {
			t.Fatal("SaveSnapshot should fail when PrepareSnapshot fails")
		}
		var pe *testPrepareError
		if !errors.As(err, &pe) {
			t.Errorf("error = %T, want *testPrepareError", err)
		}
	})
}

// ---------------------------------------------------------------------------
// On-disk SM wrapper: Sync error propagation
// ---------------------------------------------------------------------------

func TestOnDiskSMWrapper_SyncError(t *testing.T) {
	t.Run("sync error stops save snapshot", func(t *testing.T) {
		kv := &testOnDiskKVSyncErr{testOnDiskKV: *newTestOnDiskKV()}
		w := &onDiskSMWrapper{sm: kv}

		var buf bytes.Buffer
		stopper := make(chan struct{})
		err := w.SaveSnapshot(context.Background(), &buf, stopper)
		if err == nil {
			t.Fatal("SaveSnapshot should fail when Sync fails")
		}
		var se *testAdapterSyncError
		if !errors.As(err, &se) {
			t.Errorf("error = %T, want *testAdapterSyncError", err)
		}
	})
}

// ---------------------------------------------------------------------------
// On-disk SM wrapper: PrepareSnapshot error propagation
// ---------------------------------------------------------------------------

func TestOnDiskSMWrapper_PrepareSnapshotError(t *testing.T) {
	t.Run("prepare snapshot error stops save", func(t *testing.T) {
		kv := &testOnDiskKVPrepErr{testOnDiskKV: *newTestOnDiskKV()}
		w := &onDiskSMWrapper{sm: kv}

		var buf bytes.Buffer
		stopper := make(chan struct{})
		err := w.SaveSnapshot(context.Background(), &buf, stopper)
		if err == nil {
			t.Fatal("SaveSnapshot should fail when PrepareSnapshot fails")
		}
		var pe *testPrepareError
		if !errors.As(err, &pe) {
			t.Errorf("error = %T, want *testPrepareError", err)
		}
	})
}

// ---------------------------------------------------------------------------
// On-disk SM wrapper: Close error propagation
// ---------------------------------------------------------------------------

func TestOnDiskSMWrapper_CloseError(t *testing.T) {
	t.Run("close error propagates", func(t *testing.T) {
		kv := &testOnDiskKVCloseErr{testOnDiskKV: *newTestOnDiskKV()}
		w := &onDiskSMWrapper{sm: kv}

		err := w.Close(context.Background())
		if err == nil {
			t.Fatal("Close should return error")
		}
		var ce *testAdapterCloseError
		if !errors.As(err, &ce) {
			t.Errorf("error = %T, want *testAdapterCloseError", err)
		}
	})
}

// ---------------------------------------------------------------------------
// Regular SM wrapper: Close error propagation
// ---------------------------------------------------------------------------

func TestRegularSMWrapper_CloseError(t *testing.T) {
	t.Run("close error propagates", func(t *testing.T) {
		kv := &testRegularKVCloseErr{testKV: testKV{data: make(map[string]string)}}
		w := &regularSMWrapper{sm: kv}

		err := w.Close(context.Background())
		if err == nil {
			t.Fatal("Close should return error")
		}
		var ce *testAdapterCloseError
		if !errors.As(err, &ce) {
			t.Errorf("error = %T, want *testAdapterCloseError", err)
		}
	})
}

// ---------------------------------------------------------------------------
// Concurrent SM wrapper: Close error propagation
// ---------------------------------------------------------------------------

func TestConcurrentSMWrapper_CloseError(t *testing.T) {
	t.Run("close error propagates", func(t *testing.T) {
		kv := &testConcurrentKVCloseErr{testConcurrentKV: *newTestConcurrentKV()}
		w := &concurrentSMWrapper{sm: kv}

		err := w.Close(context.Background())
		if err == nil {
			t.Fatal("Close should return error")
		}
		var ce *testAdapterCloseError
		if !errors.As(err, &ce) {
			t.Errorf("error = %T, want *testAdapterCloseError", err)
		}
	})
}

// ---------------------------------------------------------------------------
// Regular SM wrapper: Update error propagation
// ---------------------------------------------------------------------------

func TestRegularSMWrapper_UpdateError(t *testing.T) {
	t.Run("update error propagates", func(t *testing.T) {
		kv := &testRegularKVUpdateErr{testKV: testKV{data: make(map[string]string)}}
		w := &regularSMWrapper{sm: kv}

		entries := []sm.Entry{{Index: 1, Cmd: makeCmd("k", "v")}}
		results := make([]sm.Result, 1)
		err := w.Update(context.Background(), entries, results)
		if err == nil {
			t.Fatal("Update should return error")
		}
		var ue *testAdapterUpdateError
		if !errors.As(err, &ue) {
			t.Errorf("error = %T, want *testAdapterUpdateError", err)
		}
	})
}

// ---------------------------------------------------------------------------
// Concurrent SM wrapper: Update error propagation
// ---------------------------------------------------------------------------

func TestConcurrentSMWrapper_UpdateError(t *testing.T) {
	t.Run("update error propagates", func(t *testing.T) {
		kv := &testConcurrentKVUpdateErr{testConcurrentKV: *newTestConcurrentKV()}
		w := &concurrentSMWrapper{sm: kv}

		entries := []sm.Entry{{Index: 1, Cmd: makeCmd("k", "v")}}
		results := make([]sm.Result, 1)
		err := w.Update(context.Background(), entries, results)
		if err == nil {
			t.Fatal("Update should return error")
		}
		var ue *testAdapterUpdateError
		if !errors.As(err, &ue) {
			t.Errorf("error = %T, want *testAdapterUpdateError", err)
		}
	})
}

// ---------------------------------------------------------------------------
// On-disk SM wrapper: Update error propagation
// ---------------------------------------------------------------------------

func TestOnDiskSMWrapper_UpdateError(t *testing.T) {
	t.Run("update error propagates", func(t *testing.T) {
		kv := &testOnDiskKVUpdateErr{testOnDiskKV: *newTestOnDiskKV()}
		w := &onDiskSMWrapper{sm: kv}

		entries := []sm.Entry{{Index: 1, Cmd: makeCmd("k", "v")}}
		results := make([]sm.Result, 1)
		err := w.Update(context.Background(), entries, results)
		if err == nil {
			t.Fatal("Update should return error")
		}
		var ue *testAdapterUpdateError
		if !errors.As(err, &ue) {
			t.Errorf("error = %T, want *testAdapterUpdateError", err)
		}
	})
}

// ---------------------------------------------------------------------------
// Regular SM wrapper: Lookup error propagation
// ---------------------------------------------------------------------------

func TestRegularSMWrapper_LookupError(t *testing.T) {
	t.Run("lookup error propagates", func(t *testing.T) {
		w := &regularSMWrapper{sm: newTestKV()}

		// testKV expects string queries. Non-string returns error.
		_, err := w.Lookup(context.Background(), 42)
		if err == nil {
			t.Fatal("Lookup should return error for non-string query")
		}
	})
}

// ---------------------------------------------------------------------------
// Concurrent SM wrapper: Lookup error propagation
// ---------------------------------------------------------------------------

func TestConcurrentSMWrapper_LookupError(t *testing.T) {
	t.Run("lookup error propagates", func(t *testing.T) {
		w := &concurrentSMWrapper{sm: newTestConcurrentKV()}

		_, err := w.Lookup(context.Background(), 42)
		if err == nil {
			t.Fatal("Lookup should return error for non-string query")
		}
	})
}

// ---------------------------------------------------------------------------
// On-disk SM wrapper: Lookup error propagation
// ---------------------------------------------------------------------------

func TestOnDiskSMWrapper_LookupError(t *testing.T) {
	t.Run("lookup error propagates", func(t *testing.T) {
		w := &onDiskSMWrapper{sm: newTestOnDiskKV()}

		_, err := w.Lookup(context.Background(), 42)
		if err == nil {
			t.Fatal("Lookup should return error for non-string query")
		}
	})
}

// ---------------------------------------------------------------------------
// On-disk SM wrapper: RecoverFromSnapshot error propagation
// ---------------------------------------------------------------------------

func TestOnDiskSMWrapper_RecoverError(t *testing.T) {
	t.Run("recover error propagates", func(t *testing.T) {
		kv := &testOnDiskKVRecoverErr{testOnDiskKV: *newTestOnDiskKV()}
		w := &onDiskSMWrapper{sm: kv}

		err := w.RecoverFromSnapshot(context.Background(), &bytes.Buffer{}, make(chan struct{}))
		if err == nil {
			t.Fatal("RecoverFromSnapshot should return error")
		}
		var re *testAdapterRecoverError
		if !errors.As(err, &re) {
			t.Errorf("error = %T, want *testAdapterRecoverError", err)
		}
	})
}

// ---------------------------------------------------------------------------
// Regular SM wrapper: SaveSnapshot error propagation
// ---------------------------------------------------------------------------

func TestRegularSMWrapper_SaveSnapshotError(t *testing.T) {
	t.Run("save snapshot error propagates", func(t *testing.T) {
		kv := &testRegularKVSaveErr{testKV: testKV{data: make(map[string]string)}}
		w := &regularSMWrapper{sm: kv}

		var buf bytes.Buffer
		err := w.SaveSnapshot(context.Background(), &buf, make(chan struct{}))
		if err == nil {
			t.Fatal("SaveSnapshot should return error")
		}
		var se *testAdapterSaveError
		if !errors.As(err, &se) {
			t.Errorf("error = %T, want *testAdapterSaveError", err)
		}
	})
}

// ---------------------------------------------------------------------------
// Regular SM wrapper: RecoverFromSnapshot error propagation
// ---------------------------------------------------------------------------

func TestRegularSMWrapper_RecoverError(t *testing.T) {
	t.Run("recover error propagates", func(t *testing.T) {
		kv := &testRegularKVRecoverErr{testKV: testKV{data: make(map[string]string)}}
		w := &regularSMWrapper{sm: kv}

		err := w.RecoverFromSnapshot(context.Background(), &bytes.Buffer{}, make(chan struct{}))
		if err == nil {
			t.Fatal("RecoverFromSnapshot should return error")
		}
		var re *testAdapterRecoverError
		if !errors.As(err, &re) {
			t.Errorf("error = %T, want *testAdapterRecoverError", err)
		}
	})
}

// ---------------------------------------------------------------------------
// Concurrent SM wrapper: NALookup and GetHash with support
// ---------------------------------------------------------------------------

func TestConcurrentSMWrapper_NALookupSupported(t *testing.T) {
	kv := &testConcurrentHasherSM{
		testConcurrentKV: *newTestConcurrentKV(),
	}
	kv.data["hello"] = "world"
	w := &concurrentSMWrapper{sm: kv}

	result, supported, err := w.NALookup([]byte("hello"))
	if err != nil {
		t.Fatalf("NALookup = %v", err)
	}
	if !supported {
		t.Fatal("NALookup should return true")
	}
	if string(result) != "world" {
		t.Errorf("NALookup = %q, want %q", result, "world")
	}
}

func TestConcurrentSMWrapper_GetHashSupported(t *testing.T) {
	kv := &testConcurrentHasherSM{
		testConcurrentKV: *newTestConcurrentKV(),
	}
	w := &concurrentSMWrapper{sm: kv}

	hash, supported, err := w.GetHash()
	if err != nil {
		t.Fatalf("GetHash = %v", err)
	}
	if !supported {
		t.Fatal("GetHash should return true")
	}
	if hash != 99 {
		t.Errorf("GetHash = %d, want 99", hash)
	}
}

// ---------------------------------------------------------------------------
// On-disk SM wrapper: NALookup and GetHash with support
// ---------------------------------------------------------------------------

func TestOnDiskSMWrapper_NALookupSupported(t *testing.T) {
	kv := &testOnDiskHasherSM{
		testOnDiskKV: *newTestOnDiskKV(),
	}
	kv.data["hello"] = "world"
	w := &onDiskSMWrapper{sm: kv}

	result, supported, err := w.NALookup([]byte("hello"))
	if err != nil {
		t.Fatalf("NALookup = %v", err)
	}
	if !supported {
		t.Fatal("NALookup should return true")
	}
	if string(result) != "world" {
		t.Errorf("NALookup = %q, want %q", result, "world")
	}
}

func TestOnDiskSMWrapper_GetHashSupported(t *testing.T) {
	kv := &testOnDiskHasherSM{
		testOnDiskKV: *newTestOnDiskKV(),
	}
	w := &onDiskSMWrapper{sm: kv}

	hash, supported, err := w.GetHash()
	if err != nil {
		t.Fatalf("GetHash = %v", err)
	}
	if !supported {
		t.Fatal("GetHash should return true")
	}
	if hash != 77 {
		t.Errorf("GetHash = %d, want 77", hash)
	}
}

// ---------------------------------------------------------------------------
// Test helper types for adapter tests
// ---------------------------------------------------------------------------

// testPrepareError is a typed error for PrepareSnapshot failures.
type testPrepareError struct{}

func (e *testPrepareError) Error() string { return "test prepare error" }

// testConcurrentKVPrepError fails PrepareSnapshot.
type testConcurrentKVPrepError struct {
	testConcurrentKV
}

func (kv *testConcurrentKVPrepError) PrepareSnapshot() (interface{}, error) {
	return nil, &testPrepareError{}
}

// testOnDiskKVSyncErr fails Sync.
type testOnDiskKVSyncErr struct {
	testOnDiskKV
}

// testAdapterSyncError is a typed error for Sync.
type testAdapterSyncError struct{}

func (e *testAdapterSyncError) Error() string { return "test adapter sync error" }

func (kv *testOnDiskKVSyncErr) Sync() error {
	return &testAdapterSyncError{}
}

// testOnDiskKVPrepErr fails PrepareSnapshot.
type testOnDiskKVPrepErr struct {
	testOnDiskKV
}

func (kv *testOnDiskKVPrepErr) PrepareSnapshot() (interface{}, error) {
	return nil, &testPrepareError{}
}

// testAdapterCloseError is a typed error for Close.
type testAdapterCloseError struct{}

func (e *testAdapterCloseError) Error() string { return "test adapter close error" }

// testOnDiskKVCloseErr fails Close.
type testOnDiskKVCloseErr struct {
	testOnDiskKV
}

func (kv *testOnDiskKVCloseErr) Close(_ context.Context) error {
	return &testAdapterCloseError{}
}

// testRegularKVCloseErr fails Close.
type testRegularKVCloseErr struct {
	testKV
}

func (kv *testRegularKVCloseErr) Close(_ context.Context) error {
	return &testAdapterCloseError{}
}

// testConcurrentKVCloseErr fails Close.
type testConcurrentKVCloseErr struct {
	testConcurrentKV
}

func (kv *testConcurrentKVCloseErr) Close(_ context.Context) error {
	return &testAdapterCloseError{}
}

// testAdapterUpdateError is a typed error for Update.
type testAdapterUpdateError struct{}

func (e *testAdapterUpdateError) Error() string { return "test adapter update error" }

// testRegularKVUpdateErr fails Update.
type testRegularKVUpdateErr struct {
	testKV
}

func (kv *testRegularKVUpdateErr) Update(_ context.Context, _ []sm.Entry, _ []sm.Result) error {
	return &testAdapterUpdateError{}
}

// testConcurrentKVUpdateErr fails Update.
type testConcurrentKVUpdateErr struct {
	testConcurrentKV
}

func (kv *testConcurrentKVUpdateErr) Update(_ context.Context, _ []sm.Entry, _ []sm.Result) error {
	return &testAdapterUpdateError{}
}

// testOnDiskKVUpdateErr fails Update.
type testOnDiskKVUpdateErr struct {
	testOnDiskKV
}

func (kv *testOnDiskKVUpdateErr) Update(_ context.Context, _ []sm.Entry, _ []sm.Result) error {
	return &testAdapterUpdateError{}
}

// testAdapterRecoverError is a typed error for RecoverFromSnapshot.
type testAdapterRecoverError struct{}

func (e *testAdapterRecoverError) Error() string { return "test adapter recover error" }

// testOnDiskKVRecoverErr fails RecoverFromSnapshot.
type testOnDiskKVRecoverErr struct {
	testOnDiskKV
}

func (kv *testOnDiskKVRecoverErr) RecoverFromSnapshot(_ context.Context, _ io.Reader, _ <-chan struct{}) error {
	return &testAdapterRecoverError{}
}

// testAdapterSaveError is a typed error for SaveSnapshot.
type testAdapterSaveError struct{}

func (e *testAdapterSaveError) Error() string { return "test adapter save error" }

// testRegularKVSaveErr fails SaveSnapshot.
type testRegularKVSaveErr struct {
	testKV
}

func (kv *testRegularKVSaveErr) SaveSnapshot(_ context.Context, _ io.Writer, _ <-chan struct{}) error {
	return &testAdapterSaveError{}
}

// testRegularKVRecoverErr fails RecoverFromSnapshot.
type testRegularKVRecoverErr struct {
	testKV
}

func (kv *testRegularKVRecoverErr) RecoverFromSnapshot(_ context.Context, _ io.Reader, _ <-chan struct{}) error {
	return &testAdapterRecoverError{}
}

// testConcurrentHasherSM implements ConcurrentStateMachine + Hasher + NALookup.
type testConcurrentHasherSM struct {
	testConcurrentKV
}

func (kv *testConcurrentHasherSM) GetHash() (uint64, error) {
	return 99, nil
}

func (kv *testConcurrentHasherSM) BytesLookup(key []byte) ([]byte, error) {
	kv.mu.RLock()
	defer kv.mu.RUnlock()
	val, ok := kv.data[string(key)]
	if !ok {
		return nil, nil
	}
	return []byte(val), nil
}

// testOnDiskHasherSM implements DiskStateMachine + Hasher + NALookup.
type testOnDiskHasherSM struct {
	testOnDiskKV
}

func (kv *testOnDiskHasherSM) GetHash() (uint64, error) {
	return 77, nil
}

func (kv *testOnDiskHasherSM) BytesLookup(key []byte) ([]byte, error) {
	kv.mu.RLock()
	defer kv.mu.RUnlock()
	val, ok := kv.data[string(key)]
	if !ok {
		return nil, nil
	}
	return []byte(val), nil
}

// ---------------------------------------------------------------------------
// Concurrent SM wrapper: RecoverFromSnapshot error propagation
// ---------------------------------------------------------------------------

func TestConcurrentSMWrapper_RecoverError(t *testing.T) {
	t.Run("recover error propagates", func(t *testing.T) {
		kv := &testConcurrentKVRecoverErr{testConcurrentKV: *newTestConcurrentKV()}
		w := &concurrentSMWrapper{sm: kv}

		err := w.RecoverFromSnapshot(context.Background(), &bytes.Buffer{}, make(chan struct{}))
		if err == nil {
			t.Fatal("RecoverFromSnapshot should return error")
		}
		var re *testAdapterRecoverError
		if !errors.As(err, &re) {
			t.Errorf("error = %T, want *testAdapterRecoverError", err)
		}
	})
}

// testConcurrentKVRecoverErr fails RecoverFromSnapshot.
type testConcurrentKVRecoverErr struct {
	testConcurrentKV
}

func (kv *testConcurrentKVRecoverErr) RecoverFromSnapshot(_ context.Context, _ io.Reader, _ <-chan struct{}) error {
	return &testAdapterRecoverError{}
}

// ---------------------------------------------------------------------------
// Concurrent SM wrapper: SaveSnapshot error from underlying SM
// ---------------------------------------------------------------------------

func TestConcurrentSMWrapper_SaveSnapshotError(t *testing.T) {
	t.Run("save snapshot error from SM propagates", func(t *testing.T) {
		kv := &testConcurrentKVSaveErr{testConcurrentKV: *newTestConcurrentKV()}
		w := &concurrentSMWrapper{sm: kv}

		var buf bytes.Buffer
		err := w.SaveSnapshot(context.Background(), &buf, make(chan struct{}))
		if err == nil {
			t.Fatal("SaveSnapshot should return error")
		}
		var se *testAdapterSaveError
		if !errors.As(err, &se) {
			t.Errorf("error = %T, want *testAdapterSaveError", err)
		}
	})
}

// testConcurrentKVSaveErr fails SaveSnapshot after PrepareSnapshot succeeds.
type testConcurrentKVSaveErr struct {
	testConcurrentKV
}

func (kv *testConcurrentKVSaveErr) SaveSnapshot(_ context.Context, _ interface{}, _ io.Writer, _ <-chan struct{}) error {
	return &testAdapterSaveError{}
}

// ---------------------------------------------------------------------------
// On-disk SM wrapper: Open error propagation
// ---------------------------------------------------------------------------

func TestOnDiskSMWrapper_OpenError(t *testing.T) {
	t.Run("open error propagates from underlying SM", func(t *testing.T) {
		kv := &testOnDiskKVOpenErr{testOnDiskKV: *newTestOnDiskKV()}
		w := &onDiskSMWrapper{sm: kv}

		_, err := w.Open(context.Background(), t.TempDir(), make(chan struct{}))
		if err == nil {
			t.Fatal("Open should return error")
		}
		var oe *testAdapterOpenError
		if !errors.As(err, &oe) {
			t.Errorf("error = %T, want *testAdapterOpenError", err)
		}
	})
}

// testAdapterOpenError is a typed error for Open.
type testAdapterOpenError struct{}

func (e *testAdapterOpenError) Error() string { return "test adapter open error" }

// testOnDiskKVOpenErr fails Open.
type testOnDiskKVOpenErr struct {
	testOnDiskKV
}

func (kv *testOnDiskKVOpenErr) Open(_ context.Context, _ string, _ <-chan struct{}) (uint64, error) {
	return 0, &testAdapterOpenError{}
}

// ---------------------------------------------------------------------------
// On-disk SM wrapper: SaveSnapshot error from underlying SM
// ---------------------------------------------------------------------------

func TestOnDiskSMWrapper_SaveSnapshotError(t *testing.T) {
	t.Run("save snapshot error from SM propagates", func(t *testing.T) {
		kv := &testOnDiskKVSaveErr{testOnDiskKV: *newTestOnDiskKV()}
		w := &onDiskSMWrapper{sm: kv}

		var buf bytes.Buffer
		err := w.SaveSnapshot(context.Background(), &buf, make(chan struct{}))
		if err == nil {
			t.Fatal("SaveSnapshot should return error")
		}
		var se *testAdapterSaveError
		if !errors.As(err, &se) {
			t.Errorf("error = %T, want *testAdapterSaveError", err)
		}
	})
}

// testOnDiskKVSaveErr fails SaveSnapshot after Sync and PrepareSnapshot succeed.
type testOnDiskKVSaveErr struct {
	testOnDiskKV
}

func (kv *testOnDiskKVSaveErr) SaveSnapshot(_ context.Context, _ interface{}, _ io.Writer, _ <-chan struct{}) error {
	return &testAdapterSaveError{}
}

// ---------------------------------------------------------------------------
// NALookupInto wrapper tests
// ---------------------------------------------------------------------------

// testNALookupIntoSM extends testHasherSM with NALookupInto support.
type testNALookupIntoSM struct {
	testHasherSM
}

func (h *testNALookupIntoSM) BytesLookupInto(key, dst []byte) ([]byte, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	val, ok := h.data[string(key)]
	if !ok {
		return dst[:0], nil
	}
	dst = dst[:0]
	dst = append(dst, []byte(val)...)
	return dst, nil
}

// testNALookupIntoSMError returns an error from BytesLookupInto.
type testNALookupIntoSMError struct {
	testNALookupIntoSM
}

func (h *testNALookupIntoSMError) BytesLookupInto(_, _ []byte) ([]byte, error) {
	return nil, &testError{}
}

func TestRegularSMWrapper_NALookupIntoError(t *testing.T) {
	w := &regularSMWrapper{sm: &testNALookupIntoSMError{}}

	result, supported, err := w.NALookupInto([]byte("key"), nil)
	if !supported {
		t.Fatal("NALookupInto should return true even on error")
	}
	var te *testError
	if !errors.As(err, &te) {
		t.Errorf("NALookupInto error = %T, want *testError", err)
	}
	if result != nil {
		t.Errorf("NALookupInto result = %v, want nil on error", result)
	}
}

func TestRegularSMWrapper_NALookupIntoSupported(t *testing.T) {
	kv := &testNALookupIntoSM{testHasherSM: testHasherSM{testKV: testKV{data: map[string]string{"hello": "world"}}}}
	w := &regularSMWrapper{sm: kv}

	dst := make([]byte, 0, 8)
	result, supported, err := w.NALookupInto([]byte("hello"), dst)
	if err != nil {
		t.Fatalf("NALookupInto = %v", err)
	}
	if !supported {
		t.Fatal("NALookupInto should return true for testNALookupIntoSM")
	}
	if string(result) != "world" {
		t.Errorf("NALookupInto = %q, want %q", result, "world")
	}
}

func TestRegularSMWrapper_NALookupIntoNotSupported(t *testing.T) {
	kv := newTestKV()
	w := &regularSMWrapper{sm: kv}

	result, supported, err := w.NALookupInto([]byte("key"), nil)
	if err != nil {
		t.Fatalf("NALookupInto = %v", err)
	}
	if supported {
		t.Error("NALookupInto should return false for testKV")
	}
	if result != nil {
		t.Errorf("NALookupInto result = %v, want nil", result)
	}
}
