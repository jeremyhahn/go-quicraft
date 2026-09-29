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

package transport

import (
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jeremyhahn/go-quicraft/pkg/config"
	"github.com/jeremyhahn/go-quicraft/pkg/internal/registry"
)

// ---------------------------------------------------------------------------
// User callbacks must never run under the connection-pool write lock
// ---------------------------------------------------------------------------

// TestCleanStaleConnections_ListenerRunsOutsideConnLock is the regression test
// for the OnConnectionFailed callback that used to be invoked from INSIDE the
// t.connMu write section, during the map iteration itself.
//
// connMu is the transport's hottest lock: every outbound Raft message on every
// shard reads it through getStream/getConnection. A listener is user code of
// unbounded duration, so holding the write lock across it stalls every
// heartbeat host-wide — long enough to time out elections on shards that have
// nothing to do with the dead peer — and a listener that re-enters transport
// teardown (Stop, CloseTarget) deadlocks outright, sync.RWMutex not being
// reentrant.
//
// The check is TryLock rather than an actual re-entrant call so a regression
// fails the test instead of hanging the suite.
func TestCleanStaleConnections_ListenerRunsOutsideConnLock(t *testing.T) {
	var holder atomic.Pointer[QUICTransport]
	var fired atomic.Int64
	var lockWasHeld atomic.Bool
	var seenAddress atomic.Value

	listener := &config.EventListener{
		OnConnectionFailed: func(info config.ConnectionInfo) {
			fired.Add(1)
			seenAddress.Store(info.Address)
			tr := holder.Load()
			if tr == nil {
				return
			}
			if tr.connMu.TryLock() {
				tr.connMu.Unlock()
				return
			}
			lockWasHeld.Store(true)
		},
	}

	t1, t2, cleanup := transportPairWith(t, 42, newTestHandler(), newTestHandler(),
		func(cfg *Config) { cfg.EventListener = listener })
	defer cleanup()
	holder.Store(t1)

	target := t2.Addr().String()
	conn, err := t1.getConnection(target, "")
	if err != nil {
		t.Fatalf("getConnection: %v", err)
	}
	if err := conn.CloseWithError(0, "regression: kill the connection"); err != nil {
		t.Fatalf("CloseWithError: %v", err)
	}
	pollUntil(t, 10*time.Second, 10*time.Millisecond,
		"the killed connection's context never completed",
		func() bool { return conn.Context().Err() != nil })

	t1.cleanStaleConnections()

	if got := fired.Load(); got != 1 {
		t.Fatalf("OnConnectionFailed fired %d times, want 1", got)
	}
	if lockWasHeld.Load() {
		t.Fatal("OnConnectionFailed was invoked while connMu was held: a slow " +
			"listener now stalls every shard's sends and a re-entrant one deadlocks")
	}
	if addr, _ := seenAddress.Load().(string); addr != target {
		t.Fatalf("OnConnectionFailed address = %q, want %q", addr, target)
	}
	if n := t1.ConnectionCount(); n != 0 {
		t.Fatalf("connection count = %d after cleanup, want 0", n)
	}
}

// ---------------------------------------------------------------------------
// Snapshot per-peer fairness must key on identity, not on an ip:port
// ---------------------------------------------------------------------------

// TestSnapshotFairnessKey_IndependentOfPeerAddress is the regression test for
// the per-peer snapshot slot limit, which used to be keyed on
// conn.RemoteAddr().String() — an address whose PORT the peer chooses.
//
// One authenticated host simply opened MaxConnectionsPerIP (16 by default)
// connections from 16 source ports, presented 16 distinct keys, and became
// entitled to 16 * maxPerPeer slots against a host-global budget of only
// MaxConcurrentSnapshotRecv (4). Each slot is holdable while sustaining only
// the throughput floor, so every legitimate InstallSnapshot was rejected — and
// a follower below the leader's compaction floor cannot be repaired by
// replication either, so the shard stays under-replicated indefinitely.
//
// The test builds two connections to two DIFFERENT ip:ports that present the
// SAME certificate, which is what one host opening several connections looks
// like to the receiver, and pins that they resolve to one fairness key sharing
// one budget.
func TestSnapshotFairnessKey_IndependentOfPeerAddress(t *testing.T) {
	// One shared mTLS identity across all three endpoints: t2 and t3 stand in
	// for two connections from the same host.
	mtls := testMTLSConfig(t)
	newTransport := func(name string) *QUICTransport {
		t.Helper()
		tr, err := NewQUICTransport(Config{
			ListenAddress: "127.0.0.1:0",
			DeploymentID:  42,
			MTLSConfig:    mtls,
		}, newTestHandler(), registry.NewRegistry())
		if err != nil {
			t.Fatalf("NewQUICTransport(%s): %v", name, err)
		}
		if err := tr.Start(); err != nil {
			t.Fatalf("%s.Start: %v", name, err)
		}
		return tr
	}

	t1 := newTransport("t1")
	defer func() { _ = t1.Stop() }()
	t2 := newTransport("t2")
	defer func() { _ = t2.Stop() }()
	t3 := newTransport("t3")
	defer func() { _ = t3.Stop() }()

	connA, err := t1.dialConnection(t2.Addr().String(), "")
	if err != nil {
		t.Fatalf("dialConnection(t2): %v", err)
	}
	connB, err := t1.dialConnection(t3.Addr().String(), "")
	if err != nil {
		t.Fatalf("dialConnection(t3): %v", err)
	}

	addrA, addrB := remoteAddrOf(connA), remoteAddrOf(connB)
	if addrA == addrB {
		t.Fatalf("both connections report %q; the test cannot distinguish "+
			"address-keyed from identity-keyed fairness", addrA)
	}

	keyA, keyB := snapshotFairnessKey(connA), snapshotFairnessKey(connB)
	if keyA != keyB {
		t.Fatalf("fairness keys differ for one identity: %q vs %q — a peer can "+
			"mint a fresh slot budget per connection", keyA, keyB)
	}
	if keyA == "" {
		t.Fatal("fairness key is empty for a fully handshaked connection: the " +
			"per-peer limit would not be applied at all")
	}
	for _, addr := range []string{addrA, addrB} {
		if strings.Contains(keyA, addr) {
			t.Fatalf("fairness key %q embeds the peer-chosen address %q", keyA, addr)
		}
	}
	if !strings.HasPrefix(keyA, "spki:") {
		t.Fatalf("fairness key = %q, want the certificate-identity form under "+
			"mandatory mTLS", keyA)
	}

	// The property that matters: several connections from one identity share
	// ONE per-peer budget and cannot together drain the host-global semaphore.
	const maxConcurrent = 4
	sr := newSnapshotReceiver(maxConcurrent, 1<<20, 1<<20, 0, newTestHandler())
	if sr.maxPerPeer >= maxConcurrent {
		t.Fatalf("maxPerPeer = %d with maxConcurrent = %d: one peer may hold "+
			"the whole global budget", sr.maxPerPeer, maxConcurrent)
	}
	for i := 0; i < sr.maxPerPeer; i++ {
		if !sr.acquirePeerSlot(keyA) {
			t.Fatalf("slot %d of %d refused for a peer within its budget", i+1, sr.maxPerPeer)
		}
	}
	if sr.acquirePeerSlot(keyB) {
		t.Fatal("a second connection from the same identity was granted a fresh " +
			"per-peer budget: the host-global snapshot semaphore is drainable by one peer")
	}
}

// ---------------------------------------------------------------------------
// The whole-transfer ceiling is derived from the declared work
// ---------------------------------------------------------------------------

// TestTransferBudget_DerivedFromDeclaredWork pins the replacement for the flat
// one-hour transfer ceiling. Combined with the 1 KB/s throughput floor, the
// flat hour let a single slot be held for barely 3.6 MB of work regardless of
// what the sender claimed it was sending. The budget now scales with the
// declared chunk count and the receiver's configured rate, and is clamped at
// both ends.
func TestTransferBudget_DerivedFromDeclaredWork(t *testing.T) {
	const ratePerSec = 256 * 1024 * 1024
	sr := newSnapshotReceiver(4, 1<<30, ratePerSec, 0, newTestHandler())

	// The cheapest possible declaration no longer buys the absolute ceiling.
	small := sr.transferBudget(1)
	if small != minSnapshotTransferDuration {
		t.Fatalf("budget for a 1-chunk declaration = %v, want %v", small, minSnapshotTransferDuration)
	}
	if small >= maxSnapshotTransferDuration {
		t.Fatalf("a minimal declaration still buys the absolute ceiling (%v)", small)
	}

	// A mid-size declaration lands strictly between the two clamps and scales
	// with the work declared.
	mid := sr.transferBudget(1024)
	if mid <= minSnapshotTransferDuration || mid >= maxSnapshotTransferDuration {
		t.Fatalf("budget for 1024 chunks = %v, want strictly between %v and %v",
			mid, minSnapshotTransferDuration, maxSnapshotTransferDuration)
	}
	if larger := sr.transferBudget(2048); larger <= mid {
		t.Fatalf("budget did not grow with declared work: %v for 2048 chunks vs %v for 1024",
			larger, mid)
	}

	// A declaration large enough to exceed the ceiling is clamped, never
	// extended past it, and an absurd wire value cannot overflow the
	// arithmetic into a nonsensical (or negative) budget.
	for _, count := range []uint64{1 << 20, 1 << 40, ^uint64(0)} {
		if got := sr.transferBudget(count); got != maxSnapshotTransferDuration {
			t.Fatalf("budget for %d chunks = %v, want the clamp %v",
				count, got, maxSnapshotTransferDuration)
		}
	}

	// A disabled ceiling stays disabled, and a pinned tiny ceiling wins over
	// the floor so tests can exercise the liveness path cheaply.
	sr.maxTransferDuration = 0
	if got := sr.transferBudget(1); got != 0 {
		t.Fatalf("budget with the ceiling disabled = %v, want 0", got)
	}
	sr.maxTransferDuration = time.Nanosecond
	if got := sr.transferBudget(1); got != time.Nanosecond {
		t.Fatalf("budget with a 1ns ceiling = %v, want 1ns", got)
	}
}

// ---------------------------------------------------------------------------
// Stop's completion guarantee holds for every caller
// ---------------------------------------------------------------------------

// TestStop_ConcurrentCallersWaitForTeardown pins Stop's documented contract:
// when it returns, every goroutine has finished. A second or concurrent call
// used to return nil immediately off a failed CompareAndSwap, telling that
// caller the transport was fully torn down while the first Stop was still
// closing connections and draining the WaitGroup.
func TestStop_ConcurrentCallersWaitForTeardown(t *testing.T) {
	tr, err := NewQUICTransport(Config{
		ListenAddress: "127.0.0.1:0",
		DeploymentID:  42,
		MTLSConfig:    testMTLSConfig(t),
	}, newTestHandler(), registry.NewRegistry())
	if err != nil {
		t.Fatalf("NewQUICTransport: %v", err)
	}
	if err := tr.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}

	const callers = 8
	done := make(chan struct{}, callers)
	start := make(chan struct{})
	for i := 0; i < callers; i++ {
		go func() {
			<-start
			if stopErr := tr.Stop(); stopErr != nil {
				t.Errorf("Stop: %v", stopErr)
			}
			// Every caller, not just the one that ran the teardown, must
			// observe a finished shutdown.
			select {
			case <-tr.stopDone:
			default:
				t.Error("Stop returned before the teardown had completed")
			}
			done <- struct{}{}
		}()
	}
	close(start)

	for i := 0; i < callers; i++ {
		select {
		case <-done:
		case <-time.After(30 * time.Second):
			t.Fatalf("only %d of %d Stop callers returned", i, callers)
		}
	}

	// Still idempotent on a later, uncontended call.
	if stopErr := tr.Stop(); stopErr != nil {
		t.Fatalf("repeat Stop: %v", stopErr)
	}
}
