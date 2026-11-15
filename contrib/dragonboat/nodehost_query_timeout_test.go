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

package dragonboat

import (
	"bytes"
	"context"
	"errors"
	"io"
	"sync"
	"testing"
	"time"

	dbSM "github.com/jeremyhahn/go-quicraft/contrib/dragonboat/statemachine"
)

// lookupGate blocks lookups until the test opens it. entered receives a
// signal each time a lookup starts waiting on the gate.
type lookupGate struct {
	mu      sync.Mutex
	release chan struct{}
	entered chan struct{}
}

func newLookupGate() *lookupGate {
	return &lookupGate{entered: make(chan struct{}, 64)}
}

// close the gate: later lookups block until open is called.
func (g *lookupGate) block() {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.release == nil {
		g.release = make(chan struct{})
	}
}

// open releases every blocked lookup and lets later ones through.
func (g *lookupGate) open() {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.release != nil {
		close(g.release)
		g.release = nil
	}
}

func (g *lookupGate) wait() {
	g.mu.Lock()
	release := g.release
	g.mu.Unlock()
	if release == nil {
		return
	}
	select {
	case g.entered <- struct{}{}:
	default:
	}
	<-release
}

// blockingLookupSM stores the last proposed command; its Lookup waits on
// the gate before returning it.
type blockingLookupSM struct {
	gate *lookupGate
	mu   sync.Mutex
	data []byte
}

func (sm *blockingLookupSM) Update(entry dbSM.Entry) (dbSM.Result, error) {
	sm.mu.Lock()
	sm.data = append([]byte(nil), entry.Cmd...)
	sm.mu.Unlock()
	return dbSM.Result{Value: uint64(len(entry.Cmd))}, nil
}

func (sm *blockingLookupSM) value() []byte {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	return append([]byte(nil), sm.data...)
}

func (sm *blockingLookupSM) Lookup(_ interface{}) (interface{}, error) {
	sm.gate.wait()
	return sm.value(), nil
}

func (sm *blockingLookupSM) SaveSnapshot(w io.Writer, _ dbSM.ISnapshotFileCollection, _ <-chan struct{}) error {
	_, err := w.Write(sm.value())
	return err
}

func (sm *blockingLookupSM) RecoverFromSnapshot(r io.Reader, _ []dbSM.SnapshotFile, _ <-chan struct{}) error {
	data, err := io.ReadAll(r)
	if err != nil {
		return err
	}
	sm.mu.Lock()
	sm.data = data
	sm.mu.Unlock()
	return nil
}

func (sm *blockingLookupSM) Close() error { return nil }

// blockingNALookupSM adds a gated NALookup (statemachine.IExtended).
type blockingNALookupSM struct {
	blockingLookupSM
}

var _ dbSM.IExtended = (*blockingNALookupSM)(nil)

func (sm *blockingNALookupSM) NALookup(_ []byte) ([]byte, error) {
	sm.gate.wait()
	return sm.value(), nil
}

// startBlockingShard starts shard 1 on a fresh host with a gated state
// machine, stores want in it and returns the host and the gate. The gate
// is opened at cleanup before the host closes, so no lookup outlives it.
func startBlockingShard(t *testing.T, extended bool, want []byte) (*NodeHost, *lookupGate) {
	t.Helper()
	nh := newTestNodeHost(t)
	gate := newLookupGate()
	t.Cleanup(gate.open)
	startTestShardWith(t, nh, 1, 1, func(_, _ uint64) dbSM.IStateMachine {
		if extended {
			return &blockingNALookupSM{blockingLookupSM{gate: gate}}
		}
		return &blockingLookupSM{gate: gate}
	})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := nh.SyncPropose(ctx, nh.GetNoOPSession(1), want); err != nil {
		t.Fatalf("SyncPropose: %v", err)
	}
	return nh, gate
}

// waitEntered asserts a lookup is (still) blocked on the gate.
func waitEntered(t *testing.T, gate *lookupGate) {
	t.Helper()
	select {
	case <-gate.entered:
	case <-time.After(2 * time.Second):
		t.Fatal("lookup never reached the gate")
	}
}

// callWithin runs read and fails the test if it does not return within d.
func callWithin(t *testing.T, d time.Duration, read func() ([]byte, error)) ([]byte, time.Duration, error) {
	t.Helper()
	type res struct {
		val []byte
		err error
	}
	start := time.Now()
	ch := make(chan res, 1)
	go func() {
		v, err := read()
		ch <- res{v, err}
	}()
	select {
	case r := <-ch:
		return r.val, time.Since(start), r.err
	case <-time.After(d):
		t.Fatalf("read did not return within %v", d)
		return nil, 0, nil
	}
}

func readLocal(nh *NodeHost, rs *RequestState) func() ([]byte, error) {
	return func() ([]byte, error) {
		v, err := nh.ReadLocalNode(rs, nil)
		if err != nil {
			return nil, err
		}
		b, _ := v.([]byte)
		return b, nil
	}
}

func TestNodeHost_QueryTimeout_BoundsBlockedReadLocalNode(t *testing.T) {
	want := []byte("stored-value")
	nh, gate := startBlockingShard(t, false, want)
	nh.SetQueryTimeout(200 * time.Millisecond)

	// (a) A blocked Lookup is cut off at the query deadline.
	rs := completedReadIndex(t, nh, 1)
	gate.block()
	_, took, err := callWithin(t, 2*time.Second, readLocal(nh, rs))
	if !errors.Is(err, ErrTimeout) {
		t.Fatalf("ReadLocalNode with a blocked Lookup: err = %v, want ErrTimeout", err)
	}
	if took < 200*time.Millisecond {
		t.Fatalf("ReadLocalNode returned after %v, before the 200ms deadline", took)
	}
	waitEntered(t, gate)

	// (c) Once released, the next read returns the stored value.
	gate.open()
	got, _, err := callWithin(t, 2*time.Second, readLocal(nh, completedReadIndex(t, nh, 1)))
	if err != nil {
		t.Fatalf("ReadLocalNode after release: %v", err)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("ReadLocalNode after release = %q, want %q", got, want)
	}
}

func TestNodeHost_QueryTimeout_BoundsBlockedNAReadLocalNode(t *testing.T) {
	want := []byte("na-stored-value")
	nh, gate := startBlockingShard(t, true, want)
	nh.SetQueryTimeout(200 * time.Millisecond)

	// (b) A blocked NALookup is cut off at the query deadline.
	rs := completedReadIndex(t, nh, 1)
	gate.block()
	_, took, err := callWithin(t, 2*time.Second, func() ([]byte, error) {
		return nh.NAReadLocalNode(rs, []byte("k"))
	})
	if !errors.Is(err, ErrTimeout) {
		t.Fatalf("NAReadLocalNode with a blocked NALookup: err = %v, want ErrTimeout", err)
	}
	if took < 200*time.Millisecond {
		t.Fatalf("NAReadLocalNode returned after %v, before the 200ms deadline", took)
	}
	waitEntered(t, gate)

	gate.open()
	got, _, err := callWithin(t, 2*time.Second, func() ([]byte, error) {
		return nh.NAReadLocalNode(completedReadIndex(t, nh, 1), []byte("k"))
	})
	if err != nil {
		t.Fatalf("NAReadLocalNode after release: %v", err)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("NAReadLocalNode after release = %q, want %q", got, want)
	}
}

func TestNodeHost_QueryTimeout_ZeroOrNegativeWaitsForLookup(t *testing.T) {
	want := []byte("unbounded-value")
	nh, gate := startBlockingShard(t, false, want)

	// (d) Zero and negative restore no deadline: a read waits for a
	// Lookup released after 300ms, longer than any earlier deadline.
	for _, d := range []time.Duration{0, -1} {
		nh.SetQueryTimeout(100 * time.Millisecond)
		nh.SetQueryTimeout(d)
		if got := nh.QueryTimeout(); got != 0 {
			t.Fatalf("after SetQueryTimeout(%v): QueryTimeout() = %v, want 0", d, got)
		}
		rs := completedReadIndex(t, nh, 1)
		gate.block()
		go func() {
			time.Sleep(300 * time.Millisecond)
			gate.open()
		}()
		got, took, err := callWithin(t, 5*time.Second, readLocal(nh, rs))
		if err != nil {
			t.Fatalf("SetQueryTimeout(%v): ReadLocalNode: %v", d, err)
		}
		if !bytes.Equal(got, want) {
			t.Fatalf("SetQueryTimeout(%v): ReadLocalNode = %q, want %q", d, got, want)
		}
		if took < 300*time.Millisecond {
			t.Fatalf("SetQueryTimeout(%v): ReadLocalNode returned after %v, before the Lookup was released", d, took)
		}
		select {
		case <-gate.entered:
		default:
			t.Fatalf("SetQueryTimeout(%v): Lookup never blocked on the gate", d)
		}
	}
}
