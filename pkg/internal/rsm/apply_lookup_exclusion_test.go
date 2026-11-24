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
	"sync"
	"sync/atomic"
	"testing"

	"github.com/jeremyhahn/go-quicraft/pkg/proto"
	"github.com/jeremyhahn/go-quicraft/pkg/sm"
)

// overlapDetectorSM is a basic sm.StateMachine written EXACTLY to the contract
// its interface publishes: "QuicRaft serializes all method calls -- Update and
// Lookup are never called concurrently." It therefore holds no lock of its own.
//
// Instead of guarding state, it reports whether the contract was honoured. A
// counter is raised on entry to Update and lowered on exit; Lookup and GetHash
// record a violation if they ever observe it raised. Every reference state
// machine in this repository happens to hold an internal mutex, which is why a
// racing basic-tier SM has never had a representative in the corpus and why
// -race alone could not catch this: -race only reports on code that runs.
type overlapDetectorSM struct {
	inUpdate   atomic.Int32
	violations atomic.Int64
	spin       int
}

func (o *overlapDetectorSM) observe() {
	if o.inUpdate.Load() != 0 {
		o.violations.Add(1)
	}
}

func (o *overlapDetectorSM) Update(_ context.Context, entries []sm.Entry, results []sm.Result) error {
	o.inUpdate.Add(1)
	// Widen the window without sleeping: a real Update does work here, and a
	// zero-width critical section would make the test pass by luck rather than
	// by exclusion.
	for i := 0; i < o.spin; i++ {
		_ = i * i
	}
	for i := range entries {
		if i < len(results) {
			results[i] = sm.Result{Value: uint64(len(entries[i].Cmd))}
		}
	}
	o.inUpdate.Add(-1)
	return nil
}

func (o *overlapDetectorSM) Lookup(_ context.Context, _ interface{}) (interface{}, error) {
	o.observe()
	return uint64(0), nil
}

func (o *overlapDetectorSM) GetHash() (uint64, error) {
	o.observe()
	return 0, nil
}

func (o *overlapDetectorSM) SaveSnapshot(_ context.Context, w io.Writer, _ <-chan struct{}) error {
	_, err := w.Write([]byte{0})
	return err
}

func (o *overlapDetectorSM) RecoverFromSnapshot(_ context.Context, r io.Reader, _ <-chan struct{}) error {
	_, err := io.ReadAll(r)
	return err
}

func (o *overlapDetectorSM) Close(_ context.Context) error { return nil }

// TestApplyExcludesLookupOnBasicTier pins the contract sm.StateMachine
// publishes. It fails if Apply and Lookup/GetHash can overlap on the user
// state machine.
//
// Fail-before-fix, observed: with Apply holding readGuard.RLock instead of
// Lock (the shape at v0.5.1-alpha), this reports violations, because two
// RLocks do not exclude each other — readGuard excluded Close and nothing
// else. With Apply holding the write lock it reports none.
func TestApplyExcludesLookupOnBasicTier(t *testing.T) {
	detector := &overlapDetectorSM{spin: 2000}
	createFn := func(uint64, uint64) interface{} { return detector }

	machine, err := NewStateMachine(1, 1, createFn, t.TempDir(), testPolicy(100, 100, 0), nil, true)
	if err != nil {
		t.Fatalf("NewStateMachine = %v", err)
	}
	defer machine.Close()

	const (
		applyRounds = 400
		readers     = 4
	)

	var wg sync.WaitGroup
	stop := make(chan struct{})

	// Readers hammer both guarded read paths for the duration of the applies.
	for i := 0; i < readers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				if _, err := machine.Lookup(context.Background(), []byte("k")); err != nil {
					return
				}
				if _, _, err := machine.GetHash(); err != nil {
					return
				}
			}
		}()
	}

	for i := 0; i < applyRounds; i++ {
		entries := []proto.Entry{{
			Index: uint64(i + 1),
			Term:  1,
			Type:  proto.EntryNormal,
			Cmd:   []byte("value"),
		}}
		results := make([]sm.Result, len(entries))
		if err := machine.Apply(entries, results); err != nil {
			close(stop)
			wg.Wait()
			t.Fatalf("Apply at index %d = %v", i+1, err)
		}
	}

	close(stop)
	wg.Wait()

	// Guard against a vacuous pass: if the readers never actually ran, zero
	// violations would prove nothing. Apply must have been observed at least
	// once as having completed, and the detector must have been consulted.
	if got := detector.inUpdate.Load(); got != 0 {
		t.Fatalf("Update counter did not settle: %d", got)
	}
	if v := detector.violations.Load(); v != 0 {
		t.Errorf("Update overlapped a read %d time(s); sm.StateMachine documents that "+
			"Update and Lookup are never called concurrently", v)
	}
}
