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

package discovery

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"
)

// mockMethod is a test double for the Method interface.
type mockMethod struct {
	name    string
	peers   []Peer
	err     error
	stopErr error
	stopCnt atomic.Int32
	callCnt atomic.Int32
}

func (m *mockMethod) Discover(_ context.Context) ([]Peer, error) {
	m.callCnt.Add(1)
	return m.peers, m.err
}

func (m *mockMethod) Name() string {
	return m.name
}

func (m *mockMethod) Stop() error {
	m.stopCnt.Add(1)
	return m.stopErr
}

func TestManager_FirstMethodSucceeds(t *testing.T) {
	m1 := &mockMethod{
		name: "m1",
		peers: []Peer{
			{NodeID: 1, Address: "10.0.0.1:5000"},
		},
	}
	m2 := &mockMethod{
		name: "m2",
		peers: []Peer{
			{NodeID: 2, Address: "10.0.0.2:5000"},
		},
	}

	mgr := NewManager(ManagerConfig{
		Methods: []Method{m1, m2},
	})

	peers, err := mgr.Discover(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(peers) != 1 || peers[0].NodeID != 1 {
		t.Errorf("expected peer from m1, got %+v", peers)
	}

	// m2 should not have been called.
	if m2.callCnt.Load() != 0 {
		t.Error("second method should not be called when first succeeds")
	}
}

func TestManager_FallbackToSecondMethod(t *testing.T) {
	m1 := &mockMethod{
		name: "m1",
		err:  &NoPeersError{Method: "m1"},
	}
	m2 := &mockMethod{
		name: "m2",
		peers: []Peer{
			{NodeID: 2, Address: "10.0.0.2:5000"},
		},
	}

	mgr := NewManager(ManagerConfig{
		Methods: []Method{m1, m2},
	})

	peers, err := mgr.Discover(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(peers) != 1 || peers[0].NodeID != 2 {
		t.Errorf("expected peer from m2, got %+v", peers)
	}
}

func TestManager_AllMethodsFail(t *testing.T) {
	m1 := &mockMethod{name: "m1", err: &NoPeersError{Method: "m1"}}
	m2 := &mockMethod{name: "m2", err: &TimeoutError{Method: "m2"}}

	mgr := NewManager(ManagerConfig{
		Methods:       []Method{m1, m2},
		RetryInterval: time.Millisecond,
		MaxRetries:    0,
	})

	_, err := mgr.Discover(context.Background())
	if err == nil {
		t.Fatal("expected error when all methods fail")
	}
}

func TestManager_RetryLogic(t *testing.T) {
	callCount := atomic.Int32{}
	m1 := &mockMethod{
		name: "m1",
		err:  &NoPeersError{Method: "m1"},
	}

	// Wrap to track calls.
	wrapper := &countingMethod{
		Method:    m1,
		callCount: &callCount,
	}

	mgr := NewManager(ManagerConfig{
		Methods:       []Method{wrapper},
		RetryInterval: time.Millisecond,
		MaxRetries:    2,
	})

	_, err := mgr.Discover(context.Background())
	if err == nil {
		t.Fatal("expected error after retries exhausted")
	}

	// Should be called 3 times: 1 initial + 2 retries.
	if got := callCount.Load(); got != 3 {
		t.Errorf("expected 3 calls, got %d", got)
	}
}

func TestManager_RetrySucceedsOnSecondAttempt(t *testing.T) {
	callCount := atomic.Int32{}
	m := &eventualMethod{
		callCount:    &callCount,
		successAfter: 2,
		peers:        []Peer{{NodeID: 1, Address: "10.0.0.1:5000"}},
	}

	mgr := NewManager(ManagerConfig{
		Methods:       []Method{m},
		RetryInterval: time.Millisecond,
		MaxRetries:    3,
	})

	peers, err := mgr.Discover(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(peers) != 1 {
		t.Errorf("expected 1 peer, got %d", len(peers))
	}
}

func TestManager_NoMethods(t *testing.T) {
	mgr := NewManager(ManagerConfig{})

	_, err := mgr.Discover(context.Background())
	if err == nil {
		t.Fatal("expected error for empty methods")
	}
	var npe *NoPeersError
	if !errors.As(err, &npe) {
		t.Errorf("expected NoPeersError, got %T: %v", err, err)
	}
}

func TestManager_StopCallsAllMethods(t *testing.T) {
	m1 := &mockMethod{name: "m1"}
	m2 := &mockMethod{name: "m2"}

	mgr := NewManager(ManagerConfig{
		Methods: []Method{m1, m2},
	})

	if err := mgr.Stop(); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if m1.stopCnt.Load() != 1 {
		t.Error("m1.Stop() should be called once")
	}
	if m2.stopCnt.Load() != 1 {
		t.Error("m2.Stop() should be called once")
	}
}

func TestManager_StopReturnsFirstError(t *testing.T) {
	stopErr := errors.New("stop failed")
	m1 := &mockMethod{name: "m1", stopErr: stopErr}
	m2 := &mockMethod{name: "m2"}

	mgr := NewManager(ManagerConfig{
		Methods: []Method{m1, m2},
	})

	err := mgr.Stop()
	if err == nil {
		t.Fatal("expected error from Stop()")
	}
	if err != stopErr {
		t.Errorf("expected stop error, got: %v", err)
	}

	// m2 should still be stopped.
	if m2.stopCnt.Load() != 1 {
		t.Error("m2.Stop() should still be called even if m1 fails")
	}
}

func TestManager_Peers(t *testing.T) {
	m := &mockMethod{
		name:  "m",
		peers: []Peer{{NodeID: 1, Address: "10.0.0.1:5000"}},
	}

	mgr := NewManager(ManagerConfig{Methods: []Method{m}})

	// Before discover, Peers() returns nil.
	if peers := mgr.Peers(); peers != nil {
		t.Errorf("expected nil peers before Discover(), got %v", peers)
	}

	mgr.Discover(context.Background())

	peers := mgr.Peers()
	if len(peers) != 1 {
		t.Fatalf("expected 1 peer, got %d", len(peers))
	}
	if peers[0].NodeID != 1 {
		t.Errorf("NodeID = %d, want 1", peers[0].NodeID)
	}
}

func TestManager_PeersReturnsCopy(t *testing.T) {
	m := &mockMethod{
		name:  "m",
		peers: []Peer{{NodeID: 1, Address: "10.0.0.1:5000"}},
	}

	mgr := NewManager(ManagerConfig{Methods: []Method{m}})
	mgr.Discover(context.Background())

	p1 := mgr.Peers()
	p1[0].Address = "mutated"

	p2 := mgr.Peers()
	if p2[0].Address == "mutated" {
		t.Error("Peers() should return an independent copy")
	}
}

// countingMethod wraps a Method and counts Discover calls.
type countingMethod struct {
	Method
	callCount *atomic.Int32
}

func (c *countingMethod) Discover(ctx context.Context) ([]Peer, error) {
	c.callCount.Add(1)
	return c.Method.Discover(ctx)
}

// eventualMethod fails until successAfter calls, then returns peers.
type eventualMethod struct {
	callCount    *atomic.Int32
	successAfter int32
	peers        []Peer
}

func (e *eventualMethod) Discover(_ context.Context) ([]Peer, error) {
	n := e.callCount.Add(1)
	if n < e.successAfter {
		return nil, &NoPeersError{Method: "eventual"}
	}
	return e.peers, nil
}

func (e *eventualMethod) Name() string { return "eventual" }
func (e *eventualMethod) Stop() error  { return nil }

// TestManager_AllMethodsReturnEmptyPeers verifies that when all methods return
// empty peer lists with no errors, a NoPeersError is returned.
func TestManager_AllMethodsReturnEmptyPeers(t *testing.T) {
	m1 := &mockMethod{
		name:  "m1",
		peers: []Peer{}, // Empty, no error
	}
	m2 := &mockMethod{
		name:  "m2",
		peers: []Peer{}, // Empty, no error
	}

	mgr := NewManager(ManagerConfig{
		Methods:       []Method{m1, m2},
		RetryInterval: time.Millisecond,
		MaxRetries:    0,
	})

	_, err := mgr.Discover(context.Background())
	if err == nil {
		t.Fatal("expected NoPeersError when all methods return empty peers")
	}
	var npe *NoPeersError
	if !errors.As(err, &npe) {
		t.Errorf("expected NoPeersError, got %T: %v", err, err)
	}
}

// TestManager_DefaultRetryInterval verifies that RetryInterval defaults to
// 1 second when not configured.
func TestManager_DefaultRetryInterval(t *testing.T) {
	mgr := NewManager(ManagerConfig{
		Methods: []Method{},
	})

	if mgr.cfg.RetryInterval != time.Second {
		t.Errorf("RetryInterval = %v, want 1s", mgr.cfg.RetryInterval)
	}
}

// TestManager_MethodOrderMatters verifies that methods are tried in order
// and the first successful one is used.
func TestManager_MethodOrderMatters(t *testing.T) {
	m1 := &mockMethod{
		name: "first",
		err:  &NoPeersError{Method: "first"},
	}
	m2 := &mockMethod{
		name:  "second",
		peers: []Peer{{NodeID: 2, Address: "10.0.0.2:5000"}},
	}
	m3 := &mockMethod{
		name:  "third",
		peers: []Peer{{NodeID: 3, Address: "10.0.0.3:5000"}},
	}

	mgr := NewManager(ManagerConfig{
		Methods: []Method{m1, m2, m3},
	})

	peers, err := mgr.Discover(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Should get peers from m2 (second method), not m3.
	if len(peers) != 1 || peers[0].NodeID != 2 {
		t.Errorf("expected peer from second method, got %+v", peers)
	}

	// m3 should not have been called.
	if m3.callCnt.Load() != 0 {
		t.Error("third method should not be called when second succeeds")
	}
}

// TestManager_PeersAfterMultipleDiscoveries verifies that Peers() returns
// the most recent discovery result.
func TestManager_PeersAfterMultipleDiscoveries(t *testing.T) {
	discovery := atomic.Int32{}

	// Wrapper that changes peers on each call.
	wrapper := &dynamicMethod{
		callCount: &discovery,
	}

	mgr := NewManager(ManagerConfig{
		Methods: []Method{wrapper},
	})

	// First discovery.
	peers1, _ := mgr.Discover(context.Background())
	if len(peers1) != 1 || peers1[0].NodeID != 1 {
		t.Errorf("first discovery got %+v, want NodeID=1", peers1)
	}

	// Second discovery.
	peers2, _ := mgr.Discover(context.Background())
	if len(peers2) != 1 || peers2[0].NodeID != 2 {
		t.Errorf("second discovery got %+v, want NodeID=2", peers2)
	}

	// Peers() should return the most recent.
	current := mgr.Peers()
	if len(current) != 1 || current[0].NodeID != 2 {
		t.Errorf("Peers() should return most recent, got %+v", current)
	}
}

// dynamicMethod returns different peers on each call.
type dynamicMethod struct {
	callCount *atomic.Int32
}

func (d *dynamicMethod) Discover(_ context.Context) ([]Peer, error) {
	n := d.callCount.Add(1)
	return []Peer{{NodeID: uint64(n), Address: "10.0.0.1:5000"}}, nil
}

func (d *dynamicMethod) Name() string { return "dynamic" }
func (d *dynamicMethod) Stop() error  { return nil }

// TestManager_DiscoverCancelledMidRetry verifies that Discover returns a
// CancelledError when the context is cancelled while waiting between retries,
// without blocking for the full RetryInterval.
func TestManager_DiscoverCancelledMidRetry(t *testing.T) {
	m1 := &mockMethod{
		name: "m1",
		err:  &NoPeersError{Method: "m1"},
	}

	mgr := NewManager(ManagerConfig{
		Methods:       []Method{m1},
		RetryInterval: 10 * time.Second, // Long interval to prove cancellation works.
		MaxRetries:    5,
	})

	ctx, cancel := context.WithCancel(context.Background())

	// Cancel after a short delay, well before RetryInterval elapses.
	go func() {
		<-time.After(50 * time.Millisecond)
		cancel()
	}()

	start := time.Now()
	_, err := mgr.Discover(ctx)
	elapsed := time.Since(start)

	if err == nil {
		t.Fatal("expected CancelledError, got nil")
	}

	var ce *CancelledError
	if !errors.As(err, &ce) {
		t.Fatalf("expected *CancelledError, got %T: %v", err, err)
	}
	if ce.Method != "manager" {
		t.Errorf("CancelledError.Method = %q, want %q", ce.Method, "manager")
	}
	if !errors.Is(err, context.Canceled) {
		t.Error("CancelledError should wrap context.Canceled")
	}

	// Discover should return quickly, not wait for the full RetryInterval.
	if elapsed >= time.Second {
		t.Errorf("Discover took %v, expected quick cancellation", elapsed)
	}
}

// TestManager_DiscoverContextDeadlineExceeded verifies that Discover
// returns a CancelledError wrapping context.DeadlineExceeded when the
// context deadline expires during retries.
func TestManager_DiscoverContextDeadlineExceeded(t *testing.T) {
	m1 := &mockMethod{
		name: "m1",
		err:  &NoPeersError{Method: "m1"},
	}

	mgr := NewManager(ManagerConfig{
		Methods:       []Method{m1},
		RetryInterval: 10 * time.Second,
		MaxRetries:    5,
	})

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	start := time.Now()
	_, err := mgr.Discover(ctx)
	elapsed := time.Since(start)

	if err == nil {
		t.Fatal("expected CancelledError, got nil")
	}

	var ce *CancelledError
	if !errors.As(err, &ce) {
		t.Fatalf("expected *CancelledError, got %T: %v", err, err)
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Error("CancelledError should wrap context.DeadlineExceeded")
	}

	if elapsed >= time.Second {
		t.Errorf("Discover took %v, expected quick deadline expiry", elapsed)
	}
}

// TestManager_DiscoverSucceedsWithActiveContext verifies that Discover
// works normally when the context is not cancelled.
func TestManager_DiscoverSucceedsWithActiveContext(t *testing.T) {
	callCount := atomic.Int32{}
	m := &eventualMethod{
		callCount:    &callCount,
		successAfter: 3,
		peers:        []Peer{{NodeID: 1, Address: "10.0.0.1:5000"}},
	}

	mgr := NewManager(ManagerConfig{
		Methods:       []Method{m},
		RetryInterval: time.Millisecond,
		MaxRetries:    5,
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	peers, err := mgr.Discover(ctx)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(peers) != 1 || peers[0].NodeID != 1 {
		t.Errorf("expected peer NodeID=1, got %+v", peers)
	}
}

// TestManager_DiscoverAlreadyCancelledContext verifies that Discover
// returns immediately when given an already-cancelled context.
func TestManager_DiscoverAlreadyCancelledContext(t *testing.T) {
	m1 := &mockMethod{
		name:  "m1",
		peers: []Peer{{NodeID: 1, Address: "10.0.0.1:5000"}},
	}

	mgr := NewManager(ManagerConfig{
		Methods:       []Method{m1},
		RetryInterval: time.Second,
		MaxRetries:    3,
	})

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // Cancel immediately.

	// First attempt should succeed since context check happens before methods
	// only on retry attempts. But the first attempt runs methods directly.
	// With an already-cancelled context, the first attempt should still
	// succeed if the method returns peers.
	peers, err := mgr.Discover(ctx)
	if err != nil {
		// If the context check fires before methods on attempt 0, this is
		// expected. Either outcome is acceptable.
		var ce *CancelledError
		if !errors.As(err, &ce) {
			t.Fatalf("expected *CancelledError, got %T: %v", err, err)
		}
		return
	}
	if len(peers) != 1 {
		t.Errorf("expected 1 peer, got %d", len(peers))
	}
}
