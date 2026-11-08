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
	"io"
	"testing"
	"time"

	"github.com/jeremyhahn/go-quicraft/pkg/proto"
	"github.com/jeremyhahn/go-quicraft/pkg/sm"
)

// blockingConcurrentSM is a ConcurrentStateMachine whose SaveSnapshot parks
// until the test releases it. That is what turns "applies are not blocked by a
// streaming snapshot" into an assertion a test can actually make: the stream is
// held open for as long as the test needs, and an Apply either completes during
// it or does not.
type blockingConcurrentSM struct {
	streaming chan struct{} // closed once SaveSnapshot has started
	release   chan struct{} // closed by the test to let SaveSnapshot finish
	applied   chan uint64   // every applied index, buffered
}

func newBlockingConcurrentSM() *blockingConcurrentSM {
	return &blockingConcurrentSM{
		streaming: make(chan struct{}),
		release:   make(chan struct{}),
		applied:   make(chan uint64, 64),
	}
}

func (b *blockingConcurrentSM) Update(_ context.Context, entries []sm.Entry, results []sm.Result) error {
	for i, e := range entries {
		if i < len(results) {
			results[i] = sm.Result{Value: e.Index}
		}
		select {
		case b.applied <- e.Index:
		default:
		}
	}
	return nil
}

func (b *blockingConcurrentSM) Lookup(_ context.Context, _ interface{}) (interface{}, error) {
	return uint64(0), nil
}

func (b *blockingConcurrentSM) PrepareSnapshot() (interface{}, error) {
	return struct{}{}, nil
}

func (b *blockingConcurrentSM) SaveSnapshot(_ context.Context, _ interface{}, w io.Writer, _ <-chan struct{}) error {
	close(b.streaming)
	<-b.release
	_, err := w.Write([]byte{1})
	return err
}

func (b *blockingConcurrentSM) RecoverFromSnapshot(_ context.Context, r io.Reader, _ <-chan struct{}) error {
	_, err := io.ReadAll(r)
	return err
}

func (b *blockingConcurrentSM) Close(_ context.Context) error { return nil }

// TestAppliesResumeWhileSnapshotStreams pins the reason streamGuard exists.
//
// SaveSnapshotGated captures a point-in-time, fires afterCapture so the engine
// releases its apply gate, and then streams. The whole purpose of that split is
// that a large snapshot does not pause the shard: applies must run alongside the
// stream. Two guards make that possible — readGuard covers the capture and is
// dropped at afterCapture, while streamGuard is held across the stream purely to
// keep Close out.
//
// Collapsing those two roles into readGuard alone is not a hypothetical: it is
// what this code did when Apply became a write-lock holder, and the effect was a
// shard-wide stall for the duration of every transfer. With one guard doing both
// jobs, the Apply below blocks until the stream finishes, the stream does not
// finish until the test releases it, and the test releases it only after the
// Apply returns — so a regression deadlocks here and is reported as a timeout
// rather than passing quietly.
func TestAppliesResumeWhileSnapshotStreams(t *testing.T) {
	blocking := newBlockingConcurrentSM()
	createFn := func(uint64, uint64) interface{} { return blocking }

	machine, err := NewStateMachine(1, 1, createFn, t.TempDir(), testPolicy(100, 100, 0), nil, true)
	if err != nil {
		t.Fatalf("NewStateMachine = %v", err)
	}
	defer machine.Close()

	if machine.smType != SMTypeConcurrent {
		t.Fatalf("smType = %v, want %v: this test is about the gated-save path, "+
			"which only the concurrent and on-disk tiers take", machine.smType, SMTypeConcurrent)
	}

	captured := make(chan struct{})
	saveDone := make(chan error, 1)
	go func() {
		saveDone <- machine.SaveSnapshotGated(context.Background(), io.Discard, nil, func() {
			close(captured)
		})
	}()

	// Wait until the capture is done and the stream is genuinely in progress.
	select {
	case <-captured:
	case <-time.After(10 * time.Second):
		t.Fatal("afterCapture never fired; the gated save did not reach its capture")
	}
	select {
	case <-blocking.streaming:
	case <-time.After(10 * time.Second):
		t.Fatal("SaveSnapshot never started streaming")
	}

	// The stream is now parked inside the user SM. An apply must still get
	// through. If streamGuard's job were folded back into readGuard, this Apply
	// would wait on the stream, which waits on a release that only happens after
	// this Apply returns.
	applyDone := make(chan error, 1)
	go func() {
		entries := []proto.Entry{{
			Index: 1,
			Term:  1,
			Type:  proto.EntryNormal,
			Cmd:   []byte("during-stream"),
		}}
		applyDone <- machine.Apply(entries, make([]sm.Result, 1))
	}()

	select {
	case err := <-applyDone:
		if err != nil {
			close(blocking.release)
			t.Fatalf("Apply during snapshot stream = %v", err)
		}
	case <-time.After(15 * time.Second):
		close(blocking.release)
		<-saveDone
		t.Fatal("Apply did not complete while a snapshot was streaming: the " +
			"streaming guard is excluding applies, which defeats the gated save " +
			"and pauses the shard for the whole transfer")
	}

	// Confirm the apply actually reached the user state machine, so a future
	// short-circuit cannot make this pass without doing the work.
	select {
	case idx := <-blocking.applied:
		if idx != 1 {
			t.Fatalf("user SM saw index %d, want 1", idx)
		}
	case <-time.After(5 * time.Second):
		close(blocking.release)
		t.Fatal("Apply returned but the user state machine never saw the entry")
	}

	close(blocking.release)
	if err := <-saveDone; err != nil {
		t.Fatalf("SaveSnapshotGated = %v", err)
	}
}

// TestCloseWaitsForStreamingSnapshot pins the other half of streamGuard: moving
// the Close-blocking job off readGuard must not drop it.
//
// Close must not tear the state machine down under a snapshot that is still
// streaming. readGuard alone can no longer prove that, because the gated save
// releases it at capture time; streamGuard is what carries the guarantee now.
func TestCloseWaitsForStreamingSnapshot(t *testing.T) {
	blocking := newBlockingConcurrentSM()
	createFn := func(uint64, uint64) interface{} { return blocking }

	machine, err := NewStateMachine(1, 1, createFn, t.TempDir(), testPolicy(100, 100, 0), nil, true)
	if err != nil {
		t.Fatalf("NewStateMachine = %v", err)
	}

	saveDone := make(chan error, 1)
	go func() {
		saveDone <- machine.SaveSnapshotGated(context.Background(), io.Discard, nil, nil)
	}()

	select {
	case <-blocking.streaming:
	case <-time.After(10 * time.Second):
		t.Fatal("SaveSnapshot never started streaming")
	}

	closeReturned := make(chan struct{})
	go func() {
		_ = machine.Close()
		close(closeReturned)
	}()

	// Close must still be waiting: the snapshot has not finished streaming.
	// This window is a deliberate wait, not a condition timeout: the test
	// checks that Close does NOT return, and there is no event to wait on for
	// something that must not happen. Every other wait in this file is a
	// deadline on a condition.
	select {
	case <-closeReturned:
		close(blocking.release)
		t.Fatal("Close returned while a snapshot was still streaming; the user " +
			"state machine can be torn down under an in-flight save")
	case <-time.After(250 * time.Millisecond):
	}

	close(blocking.release)

	select {
	case <-closeReturned:
	case <-time.After(10 * time.Second):
		t.Fatal("Close never returned after the snapshot stream finished")
	}
	if err := <-saveDone; err != nil {
		t.Fatalf("SaveSnapshotGated = %v", err)
	}
}

// TestGetHashAfterCloseReturnsErrClosed pins the guard GetHash takes.
//
// Without a guard Host.StateMachineHash could hash a user SM concurrently with
// Apply — a replica-divergence check reporting a
// mismatch that does not exist — or read one that Close had already torn down.
// The Apply exclusion is covered by TestApplyExcludesLookupOnBasicTier, which
// drives GetHash alongside Lookup; this covers the closed case.
func TestGetHashAfterCloseReturnsErrClosed(t *testing.T) {
	detector := &overlapDetectorSM{}
	createFn := func(uint64, uint64) interface{} { return detector }

	machine, err := NewStateMachine(1, 1, createFn, t.TempDir(), testPolicy(100, 100, 0), nil, true)
	if err != nil {
		t.Fatalf("NewStateMachine = %v", err)
	}

	if _, _, err := machine.GetHash(); err != nil {
		t.Fatalf("GetHash before Close = %v, want nil", err)
	}
	if err := machine.Close(); err != nil {
		t.Fatalf("Close = %v", err)
	}
	if _, _, err := machine.GetHash(); err != ErrClosed {
		t.Fatalf("GetHash after Close = %v, want ErrClosed", err)
	}
}
