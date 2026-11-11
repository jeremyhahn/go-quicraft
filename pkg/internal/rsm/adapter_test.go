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

func TestInMemorySMWrappers_OpenReturnsZero(t *testing.T) {
	cases := []struct {
		name string
		w    smWrapper
	}{
		{"regular", &regularSMWrapper{sm: newTestKV()}},
		{"concurrent", &concurrentSMWrapper{sm: newTestConcurrentKV()}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			lastApplied, err := tc.w.Open(context.Background(), t.TempDir(), make(chan struct{}))
			if err != nil {
				t.Fatalf("Open = %v", err)
			}
			if lastApplied != 0 {
				t.Errorf("lastApplied = %d, want 0", lastApplied)
			}
		})
	}
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

// ---------------------------------------------------------------------------
// Concurrent SM wrapper: PrepareSnapshot error propagation
// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------
// On-disk SM wrapper: Sync error propagation
// ---------------------------------------------------------------------------

func TestSMWrappers_UserSMErrorsPropagate(t *testing.T) {
	ctx := context.Background()
	stopper := make(chan struct{})
	entries := []sm.Entry{{Index: 1, Cmd: makeCmd("k", "v")}}
	save := func(w smWrapper) func() error {
		return func() error { return w.SaveSnapshot(ctx, &bytes.Buffer{}, stopper) }
	}
	update := func(w smWrapper) func() error {
		return func() error { return w.Update(ctx, entries, make([]sm.Result, 1)) }
	}
	recoverFrom := func(w smWrapper) func() error {
		return func() error { return w.RecoverFromSnapshot(ctx, &bytes.Buffer{}, stopper) }
	}
	closeW := func(w smWrapper) func() error {
		return func() error { return w.Close(ctx) }
	}
	lookup := func(w smWrapper) func() error {
		return func() error {
			// The test SMs expect string queries; a non-string query fails.
			_, err := w.Lookup(ctx, 42)
			return err
		}
	}
	isType := func(target interface{}) func(error) bool {
		return func(err error) bool { return errors.As(err, target) }
	}
	anyErr := func(error) bool { return true }

	cases := []struct {
		name  string
		call  func() error
		match func(error) bool
	}{
		{"concurrent prepare stops save", save(&concurrentSMWrapper{sm: &testConcurrentKVPrepError{testConcurrentKV: *newTestConcurrentKV()}}), isType(new(*testPrepareError))},
		{"concurrent save", save(&concurrentSMWrapper{sm: &testConcurrentKVSaveErr{testConcurrentKV: *newTestConcurrentKV()}}), isType(new(*testAdapterSaveError))},
		{"concurrent update", update(&concurrentSMWrapper{sm: &testConcurrentKVUpdateErr{testConcurrentKV: *newTestConcurrentKV()}}), isType(new(*testAdapterUpdateError))},
		{"concurrent recover", recoverFrom(&concurrentSMWrapper{sm: &testConcurrentKVRecoverErr{testConcurrentKV: *newTestConcurrentKV()}}), isType(new(*testAdapterRecoverError))},
		{"concurrent close", closeW(&concurrentSMWrapper{sm: &testConcurrentKVCloseErr{testConcurrentKV: *newTestConcurrentKV()}}), isType(new(*testAdapterCloseError))},
		{"concurrent lookup", lookup(&concurrentSMWrapper{sm: newTestConcurrentKV()}), anyErr},
		{"on-disk prepare stops save", save(&onDiskSMWrapper{sm: &testOnDiskKVPrepErr{testOnDiskKV: *newTestOnDiskKV()}}), isType(new(*testPrepareError))},
		{"on-disk save", save(&onDiskSMWrapper{sm: &testOnDiskKVSaveErr{testOnDiskKV: *newTestOnDiskKV()}}), isType(new(*testAdapterSaveError))},
		{"on-disk update", update(&onDiskSMWrapper{sm: &testOnDiskKVUpdateErr{testOnDiskKV: *newTestOnDiskKV()}}), isType(new(*testAdapterUpdateError))},
		{"on-disk recover", recoverFrom(&onDiskSMWrapper{sm: &testOnDiskKVRecoverErr{testOnDiskKV: *newTestOnDiskKV()}}), isType(new(*testAdapterRecoverError))},
		{"on-disk close", closeW(&onDiskSMWrapper{sm: &testOnDiskKVCloseErr{testOnDiskKV: *newTestOnDiskKV()}}), isType(new(*testAdapterCloseError))},
		{"on-disk lookup", lookup(&onDiskSMWrapper{sm: newTestOnDiskKV()}), anyErr},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.call()
			if err == nil {
				t.Fatal("expected an error from the user SM")
			}
			if !tc.match(err) {
				t.Errorf("error = %T (%v), want the user SM's typed error", err, err)
			}
		})
	}
}

func TestSMWrappers_HasherSupported(t *testing.T) {
	concurrent := &testConcurrentHasherSM{testConcurrentKV: *newTestConcurrentKV()}
	concurrent.data["hello"] = "world"
	onDisk := &testOnDiskHasherSM{testOnDiskKV: *newTestOnDiskKV()}
	onDisk.data["hello"] = "world"
	cases := []struct {
		name     string
		w        smWrapper
		wantHash uint64
	}{
		{"concurrent", &concurrentSMWrapper{sm: concurrent}, 99},
		{"on-disk", &onDiskSMWrapper{sm: onDisk}, 77},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			result, supported, err := tc.w.NALookup([]byte("hello"))
			if err != nil || !supported {
				t.Fatalf("NALookup supported=%v err=%v, want supported and no error", supported, err)
			}
			if string(result) != "world" {
				t.Errorf("NALookup = %q, want %q", result, "world")
			}
			hash, supported, err := tc.w.GetHash()
			if err != nil || !supported {
				t.Fatalf("GetHash supported=%v err=%v, want supported and no error", supported, err)
			}
			if hash != tc.wantHash {
				t.Errorf("GetHash = %d, want %d", hash, tc.wantHash)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// On-disk SM wrapper: PrepareSnapshot error propagation
// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------
// On-disk SM wrapper: Close error propagation
// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------
// Regular SM wrapper: Close error propagation
// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------
// Concurrent SM wrapper: Close error propagation
// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------
// Regular SM wrapper: Update error propagation
// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------
// Concurrent SM wrapper: Update error propagation
// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------
// On-disk SM wrapper: Update error propagation
// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------
// Regular SM wrapper: Lookup error propagation
// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------
// Concurrent SM wrapper: Lookup error propagation
// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------
// On-disk SM wrapper: Lookup error propagation
// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------
// On-disk SM wrapper: RecoverFromSnapshot error propagation
// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------
// Regular SM wrapper: SaveSnapshot error propagation
// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------
// Regular SM wrapper: RecoverFromSnapshot error propagation
// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------
// Concurrent SM wrapper: NALookup and GetHash with support
// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------
// On-disk SM wrapper: NALookup and GetHash with support
// ---------------------------------------------------------------------------

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

// ---------------------------------------------------------------------------
// On-disk SM wrapper: SaveSnapshot error from underlying SM
// ---------------------------------------------------------------------------

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
