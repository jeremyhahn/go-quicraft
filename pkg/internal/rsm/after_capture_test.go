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
	"context"
	"errors"
	"io"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jeremyhahn/go-quicraft/pkg/proto"
	"github.com/jeremyhahn/go-quicraft/pkg/sm"
)

// newAfterCaptureSM builds a StateMachine around the given user state
// machine, the way stream_guard_test.go does.
func newAfterCaptureSM(_ context.Context, t *testing.T, userSM interface{}) *StateMachine {
	t.Helper()
	createFn := func(uint64, uint64) interface{} { return userSM }
	machine, err := NewStateMachine(1, 1, createFn, t.TempDir(), testPolicy(100, 100, 0), nil, true)
	if err != nil {
		t.Fatalf("NewStateMachine = %v", err)
	}
	return machine
}

// saveCountingAfterCapture runs SaveSnapshotGated with an afterCapture that
// counts its calls, and returns the count and the save's error.
func saveCountingAfterCapture(ctx context.Context, t *testing.T, machine *StateMachine, w io.Writer, stopper <-chan struct{}) (int64, error) {
	t.Helper()
	var calls atomic.Int64
	err := machine.SaveSnapshotGated(ctx, w, stopper, func() { calls.Add(1) })
	return calls.Load(), err
}

// applyOneWithin applies one entry and fails the test if Apply does not
// return nil before ctx is done.
func applyOneWithin(ctx context.Context, t *testing.T, machine *StateMachine) {
	t.Helper()
	done := make(chan error, 1)
	go func() {
		entries := []proto.Entry{{Index: 1, Term: 1, Type: proto.EntryNormal, Cmd: []byte("after-failed-save")}}
		done <- machine.Apply(entries, make([]sm.Result, 1))
	}()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Apply after a failed save = %v, want nil", err)
		}
	case <-ctx.Done():
		t.Fatal("Apply after a failed save did not return: the capture guards were not released")
	}
}

func TestSaveSnapshotGatedAfterCaptureNotCalledWhenClosed(t *testing.T) {
	ctx := context.Background()
	machine := newAfterCaptureSM(ctx, t, newTestConcurrentKV())
	if err := machine.Close(); err != nil {
		t.Fatalf("Close = %v", err)
	}
	calls, err := saveCountingAfterCapture(ctx, t, machine, io.Discard, nil)
	if !errors.Is(err, ErrClosed) {
		t.Fatalf("SaveSnapshotGated on a closed SM = %v, want ErrClosed", err)
	}
	if calls != 0 {
		t.Fatalf("afterCapture calls = %d, want 0", calls)
	}
}

func TestSaveSnapshotGatedAfterCaptureNotCalledWhenStopped(t *testing.T) {
	ctx := context.Background()
	machine := newAfterCaptureSM(ctx, t, newTestConcurrentKV())
	defer machine.Close()
	stopper := make(chan struct{})
	close(stopper)
	calls, err := saveCountingAfterCapture(ctx, t, machine, io.Discard, stopper)
	if !errors.Is(err, ErrStopped) {
		t.Fatalf("SaveSnapshotGated with a closed stopper = %v, want ErrStopped", err)
	}
	if calls != 0 {
		t.Fatalf("afterCapture calls = %d, want 0", calls)
	}
}

func TestSaveSnapshotGatedAfterCaptureNotCalledOnSessionWriteError(t *testing.T) {
	cases := []struct {
		name   string
		userSM func() interface{}
	}{
		{"regular", func() interface{} { return newTestKV() }},
		{"concurrent", func() interface{} { return newTestConcurrentKV() }},
		{"on-disk", func() interface{} { return newTestOnDiskKV() }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			machine := newAfterCaptureSM(ctx, t, tc.userSM())
			defer machine.Close()
			calls, err := saveCountingAfterCapture(ctx, t, machine, &limitedWriter{limit: 2}, nil)
			if err == nil {
				t.Fatal("SaveSnapshotGated with a failing session write = nil, want an error")
			}
			if calls != 0 {
				t.Fatalf("afterCapture calls = %d, want 0", calls)
			}
			applyCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
			defer cancel()
			applyOneWithin(applyCtx, t, machine)
		})
	}
}

func TestSaveSnapshotGatedAfterCaptureNotCalledOnPrepareOrSyncError(t *testing.T) {
	cases := []struct {
		name   string
		userSM interface{}
		match  func(error) bool
	}{
		{"concurrent prepare", &testConcurrentKVPrepError{testConcurrentKV: *newTestConcurrentKV()},
			func(err error) bool { var e *testPrepareError; return errors.As(err, &e) }},
		{"on-disk prepare", &testOnDiskKVPrepErr{testOnDiskKV: *newTestOnDiskKV()},
			func(err error) bool { var e *testPrepareError; return errors.As(err, &e) }},
		{"on-disk sync", &testOnDiskKVSyncError{testOnDiskKV: *newTestOnDiskKV()},
			func(err error) bool { var e *testSyncError; return errors.As(err, &e) }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			machine := newAfterCaptureSM(ctx, t, tc.userSM)
			defer machine.Close()
			calls, err := saveCountingAfterCapture(ctx, t, machine, io.Discard, nil)
			if !tc.match(err) {
				t.Fatalf("SaveSnapshotGated = %v, want the fake's error", err)
			}
			if calls != 0 {
				t.Fatalf("afterCapture calls = %d, want 0", calls)
			}
			applyCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
			defer cancel()
			applyOneWithin(applyCtx, t, machine)
		})
	}
}

func TestSaveSnapshotGatedAfterCaptureOnSaveError(t *testing.T) {
	cases := []struct {
		name      string
		userSM    interface{}
		wantCalls int64
	}{
		// A regular SM's capture is its whole save: a failed save never
		// completed the capture.
		{"regular", &testRegularKVSaveErr{testKV: testKV{data: make(map[string]string)}}, 0},
		// Concurrent and on-disk SMs capture in PrepareSnapshot; the stream
		// fails after the capture completed.
		{"concurrent", &testConcurrentKVSaveErr{testConcurrentKV: *newTestConcurrentKV()}, 1},
		{"on-disk", &testOnDiskKVSaveErr{testOnDiskKV: *newTestOnDiskKV()}, 1},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			machine := newAfterCaptureSM(ctx, t, tc.userSM)
			defer machine.Close()
			calls, err := saveCountingAfterCapture(ctx, t, machine, io.Discard, nil)
			var saveErr *testAdapterSaveError
			if !errors.As(err, &saveErr) {
				t.Fatalf("SaveSnapshotGated = %v, want *testAdapterSaveError", err)
			}
			if calls != tc.wantCalls {
				t.Fatalf("afterCapture calls = %d, want %d", calls, tc.wantCalls)
			}
			applyCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
			defer cancel()
			applyOneWithin(applyCtx, t, machine)
		})
	}
}
