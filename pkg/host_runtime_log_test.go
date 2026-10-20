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

package quicraft

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jeremyhahn/go-quicraft/pkg/internal/engine"
	"github.com/jeremyhahn/go-quicraft/pkg/proto"
	"github.com/jeremyhahn/go-quicraft/pkg/sm"
)

// hostLogs is a slog.Handler that keeps every record it is handed, so a test
// can assert that a runtime diagnostic reached the Host's logger (WithLogger)
// rather than the process slog default.
type hostLogs struct {
	mu      sync.Mutex
	records []slog.Record
}

func (c *hostLogs) Enabled(context.Context, slog.Level) bool { return true }

func (c *hostLogs) Handle(_ context.Context, r slog.Record) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.records = append(c.records, r.Clone())
	return nil
}

func (c *hostLogs) WithAttrs([]slog.Attr) slog.Handler { return c }

func (c *hostLogs) WithGroup(string) slog.Handler { return c }

// await returns the first record with message msg, waiting for one emitted
// contained on the diagnostics worker, and fails the test when none arrives.
func (c *hostLogs) await(t *testing.T, msg string) slog.Record {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		c.mu.Lock()
		for _, r := range c.records {
			if r.Message == msg {
				c.mu.Unlock()
				return r
			}
		}
		n := len(c.records)
		c.mu.Unlock()
		if time.Now().After(deadline) {
			t.Fatalf("no %q record reached the Host logger (got %d records)", msg, n)
		}
		time.Sleep(time.Millisecond)
	}
}

// switchHandler forwards to a handler a test can replace while the Host runs,
// so a test can install a hostile handler only once setup has logged.
type switchHandler struct{ to atomic.Pointer[slog.Handler] }

func newSwitchHandler(h slog.Handler) *switchHandler {
	s := &switchHandler{}
	s.set(h)
	return s
}

func (s *switchHandler) set(h slog.Handler) { s.to.Store(&h) }

func (s *switchHandler) Enabled(ctx context.Context, l slog.Level) bool {
	return (*s.to.Load()).Enabled(ctx, l)
}

func (s *switchHandler) Handle(ctx context.Context, r slog.Record) error {
	return (*s.to.Load()).Handle(ctx, r)
}

func (s *switchHandler) WithAttrs([]slog.Attr) slog.Handler { return s }

func (s *switchHandler) WithGroup(string) slog.Handler { return s }

// newLoggedTestHost is newTestHost with handler as the Host's logger.
func newLoggedTestHost(t *testing.T, handler slog.Handler) *Host {
	t.Helper()
	h, err := NewHost(testHostConfig(t), WithMemoryLogDB(), WithoutTransport(),
		WithoutEncryption(), WithLogger(slog.New(handler)))
	if err != nil {
		t.Fatalf("NewHost failed: %v", err)
	}
	t.Cleanup(func() { h.Close() })
	return h
}

// wantRecord asserts r's level and, when shardID is non-zero, its shard_id.
func wantRecord(t *testing.T, r slog.Record, level slog.Level, shardID uint64) {
	t.Helper()
	if r.Level != level {
		t.Errorf("%q: level = %v, want %v", r.Message, r.Level, level)
	}
	if shardID != 0 {
		if got := recordAttr(t, r, "shard_id"); got != shardID {
			t.Errorf("%q: shard_id = %d, want %d", r.Message, got, shardID)
		}
	}
}

// wantTypeAssertion asserts r carries a *TypeAssertionError as its error.
func wantTypeAssertion(t *testing.T, r slog.Record) {
	t.Helper()
	var found bool
	r.Attrs(func(a slog.Attr) bool {
		if a.Key != "error" {
			return true
		}
		err, _ := a.Value.Any().(error)
		_, found = errors.AsType[*TypeAssertionError](err)
		return false
	})
	if !found {
		t.Errorf("%q carries no *TypeAssertionError", r.Message)
	}
}

// TestHostRuntimeLog_NodeStateTypeAssertions pins that the ERROR every engine
// callback and host method logs on a malformed nodes-map value reaches the
// Host's logger with its shard and its TypeAssertionError.
func TestHostRuntimeLog_NodeStateTypeAssertions(t *testing.T) {
	logs := &hostLogs{}
	h := newLoggedTestHost(t, logs)
	h.nodes.Store(uint64(1), "bad-type")
	defer h.nodes.Delete(uint64(1))
	cb := &hostEngineCallback{h: h}
	entries := []proto.Entry{{Index: 1, Term: 1, Key: 1}}

	for _, tt := range []struct {
		msg     string
		shardID uint64
		call    func()
	}{
		{"OnApplied type assertion failed", 1,
			func() { cb.OnApplied(1, entries, make([]sm.Result, 1)) }},
		{"OnReadyToRead: nodeState type assertion failed", 1,
			func() { cb.OnReadyToRead(1, []proto.ReadyToRead{{Key: 1, Index: 1}}) }},
		{"OnReadIndexBatched: nodeState type assertion failed", 1,
			func() { cb.OnReadIndexBatched(1, 1, []uint64{2}) }},
		{"OnLeaderUpdated: nodeState type assertion failed", 1,
			func() { cb.OnLeaderUpdated(1, 1, 1) }},
		{"OnLeaderReady: nodeState type assertion failed", 1,
			func() { cb.OnLeaderReady(1, 1, 1, 1) }},
		{"OnSnapshotCompleted: nodeState type assertion failed", 1,
			func() { cb.OnSnapshotCompleted(1, 100, engine.SnapshotSave, nil) }},
		{"OnSnapshotRequestCompleted: nodeState type assertion failed", 1,
			func() { cb.OnSnapshotRequestCompleted(1, 1, 100, nil) }},
		{"OnCommitted: nodeState type assertion failed", 1,
			func() { cb.OnCommitted(1, entries) }},
		{"OnProposalFailed: nodeState type assertion failed", 1,
			func() { cb.OnProposalFailed(1, entries, errors.New("failed")) }},
		{"OnReadIndexFailed: nodeState type assertion failed", 1,
			func() { cb.OnReadIndexFailed(1, []uint64{1}, errors.New("failed")) }},
		{"OnShardFailed: nodeState type assertion failed", 1,
			func() { cb.OnShardFailed(1, errors.New("halted")) }},
		{"updateNodeLeader: nodeState type assertion failed", 1,
			func() { h.updateNodeLeader(1, 1, 1) }},
		{"RemoveData: nodeState type assertion failed", 1,
			func() { _ = h.RemoveData(1, 1) }},
		{"NodeHostInfo: nodes map value type assertion failed", 0,
			func() { h.NodeHostInfo(NodeHostInfoOption{}) }},
		{"HealthStatus: nodes map value type assertion failed", 0,
			func() { h.HealthCheck() }},
		{"sweepExpiredPending: nodes map value type assertion failed", 0,
			func() { h.sweepExpiredPending() }},
	} {
		t.Run(tt.msg, func(t *testing.T) {
			tt.call()
			r := logs.await(t, tt.msg)
			wantRecord(t, r, slog.LevelError, tt.shardID)
			wantTypeAssertion(t, r)
		})
	}
}

// TestHostRuntimeLog_DiagPipelineStateTypeAssertion pins the
// DiagPipelineState ERROR, which is reached only for a shard the engine holds.
func TestHostRuntimeLog_DiagPipelineStateTypeAssertion(t *testing.T) {
	logs := &hostLogs{}
	h := newLoggedTestHost(t, logs)
	if err := h.StartShard(map[uint64]string{1: "127.0.0.1:5000"}, false,
		testCreateFunc, testShardConfig(1, 1)); err != nil {
		t.Fatalf("StartShard: %v", err)
	}
	ns, _ := h.nodes.Load(uint64(1))
	h.nodes.Store(uint64(1), "bad-type")
	h.DiagPipelineState(1)
	h.nodes.Store(uint64(1), ns)

	r := logs.await(t, "DiagPipelineState: nodeState type assertion failed")
	wantRecord(t, r, slog.LevelError, 1)
	wantTypeAssertion(t, r)
}

// testNodeState returns a minimal live nodeState for shard 1, replica 1,
// registered on h.
func testNodeState(h *Host) *nodeState {
	ns := &nodeState{
		shardID:      1,
		replicaID:    1,
		nodes:        map[uint64]string{1: "127.0.0.1:5000"},
		pendingReads: make(map[uint64]*pendingRead),
		requestPool:  NewRequestStatePool(),
		openStopper:  make(chan struct{}),
	}
	ns.cfg.ShardID, ns.cfg.ReplicaID = 1, 1
	ns.health.Store(uint32(ShardHealthy))
	h.nodes.Store(uint64(1), ns)
	return ns
}

// TestHostRuntimeLog_PendingProposalTypeAssertions pins that the ERROR every
// path logs on a malformed pending-proposal value reaches the Host's logger.
func TestHostRuntimeLog_PendingProposalTypeAssertions(t *testing.T) {
	logs := &hostLogs{}
	h := newLoggedTestHost(t, logs)
	ns := testNodeState(h)
	defer h.nodes.Delete(uint64(1))
	cb := &hostEngineCallback{h: h}
	const key = uint64(42)
	entries := []proto.Entry{{Index: 1, Term: 1, Key: key}}

	for _, tt := range []struct {
		msg      string
		shardID  uint64
		entryKey bool
		call     func()
	}{
		{"OnApplied pendingProposal type assertion failed", 1, true,
			func() { cb.OnApplied(1, entries, make([]sm.Result, 1)) }},
		{"OnCommitted: pendingProposal type assertion failed", 1, true,
			func() { cb.OnCommitted(1, entries) }},
		{"OnProposalFailed: pendingProposal type assertion failed", 1, true,
			func() { cb.OnProposalFailed(1, entries, errors.New("failed")) }},
		{"sweepPendingProposals: pendingProposals value type assertion failed", 0, false,
			func() { h.sweepPendingProposals(ns, time.Now()) }},
		{"failPendingClientRequests: pendingProposals value type assertion failed", 0, false,
			func() { h.failPendingClientRequests(ns, ErrShardFailed) }},
	} {
		t.Run(tt.msg, func(t *testing.T) {
			ns.pendingProposals.Store(key, "bad-type")
			defer ns.pendingProposals.Delete(key)
			tt.call()
			r := logs.await(t, tt.msg)
			wantRecord(t, r, slog.LevelError, tt.shardID)
			wantTypeAssertion(t, r)
			if tt.entryKey {
				if got := recordAttr(t, r, "entry_key"); got != key {
					t.Errorf("entry_key = %d, want %d", got, key)
				}
			}
		})
	}
}

// TestHostRuntimeLog_ShortResults pins the ERROR OnApplied logs when the
// engine hands it fewer results than entries.
func TestHostRuntimeLog_ShortResults(t *testing.T) {
	logs := &hostLogs{}
	h := newLoggedTestHost(t, logs)
	testNodeState(h)
	defer h.nodes.Delete(uint64(1))
	cb := &hostEngineCallback{h: h}

	cb.OnApplied(1, []proto.Entry{{Index: 1, Term: 1}, {Index: 2, Term: 1}}, make([]sm.Result, 1))
	r := logs.await(t, "OnApplied: results slice is shorter than entries; "+
		"engine invariant violated, proposals beyond the results will "+
		"time out instead of completing")
	wantRecord(t, r, slog.LevelError, 1)
	var got int64
	r.Attrs(func(a slog.Attr) bool {
		if a.Key == "entries" {
			got = a.Value.Int64()
			return false
		}
		return true
	})
	if got != 2 {
		t.Errorf("entries = %d, want 2", got)
	}
}

// TestHostRuntimeLog_ShardHalted pins that the contained halt line reaches
// the Host's logger with the shard, the replica and the cause.
func TestHostRuntimeLog_ShardHalted(t *testing.T) {
	logs := &hostLogs{}
	h := newLoggedTestHost(t, logs)
	testNodeState(h)
	defer h.nodes.Delete(uint64(1))
	h.activeNodes.Add(1)
	defer h.activeNodes.Add(-1)
	cause := errors.New("halted")

	(&hostEngineCallback{h: h}).OnShardFailed(1, cause)
	r := logs.await(t, "shard halted: failure latch tripped, refusing reads and proposals until the shard is reloaded")
	wantRecord(t, r, slog.LevelError, 1)
	if got := recordAttr(t, r, "replica_id"); got != 1 {
		t.Errorf("replica_id = %d, want 1", got)
	}
	var gotErr error
	r.Attrs(func(a slog.Attr) bool {
		if a.Key == "error" {
			gotErr, _ = a.Value.Any().(error)
			return false
		}
		return true
	})
	if !errors.Is(gotErr, cause) {
		t.Errorf("error = %v, want %v", gotErr, cause)
	}
}

// TestHostRuntimeLog_IdentityMismatch pins the ERROR for a committed
// identity binding this replica that the host does not present.
func TestHostRuntimeLog_IdentityMismatch(t *testing.T) {
	logs := &hostLogs{}
	cfg := sanHostConfig(t, "node-a.raft.test:5000", "node-a.raft.test")
	h, err := NewHost(cfg, WithMemoryLogDB(), WithoutEncryption(), WithLogger(slog.New(logs)))
	if err != nil {
		t.Fatalf("NewHost: %v", err)
	}
	defer h.Close()
	ns := testNodeState(h)
	defer h.nodes.Delete(uint64(1))

	cc := proto.ConfigChange{Type: proto.AddNode, ReplicaID: 1, Address: "node-a.raft.test:5000",
		NodeIdentity: testIdentity(9)}
	cmd := make([]byte, cc.Size())
	if _, err := cc.MarshalTo(cmd); err != nil {
		t.Fatalf("MarshalTo: %v", err)
	}
	(&hostEngineCallback{h: h}).applyMembershipRouting(ns, 1,
		[]proto.Entry{{Index: 5, Term: 1, Type: proto.EntryConfigChange, Cmd: cmd}})

	r := logs.await(t, "quicraft: the committed identity for this replica is not the one this host "+
		"presents; every peer will refuse its messages — restore the keypair, or remove and re-add the replica")
	wantRecord(t, r, slog.LevelError, 0)
	if got := recordAttr(t, r, "shard"); got != 1 {
		t.Errorf("shard = %d, want 1", got)
	}
	if got := recordAttr(t, r, "replica"); got != 1 {
		t.Errorf("replica = %d, want 1", got)
	}
	for key, want := range map[string]string{
		"committed_identity": testIdentity(9),
		"host_identity":      h.NodeIdentity(),
	} {
		var got string
		r.Attrs(func(a slog.Attr) bool {
			if a.Key == key {
				got = a.Value.String()
				return false
			}
			return true
		})
		if got != want {
			t.Errorf("%s = %q, want %q", key, got, want)
		}
	}
}

// TestHostRuntimeLog_ListenerDropped pins the ERROR naming the event a full
// listener queue dropped.
func TestHostRuntimeLog_ListenerDropped(t *testing.T) {
	logs := &hostLogs{}
	h := newLoggedTestHost(t, logs)
	release := make(chan struct{})
	defer close(release)
	for i := 0; h.engine.DispatchListener(func() { <-release }); i++ {
		if i > 1<<20 {
			t.Fatal("the listener queue never filled")
		}
	}

	(&hostEngineCallback{h: h}).dispatchListener("OnTestEvent", 7, func() {})
	r := logs.await(t, "listener notification dropped: the EventListener is not consuming")
	wantRecord(t, r, slog.LevelError, 7)
	var event string
	r.Attrs(func(a slog.Attr) bool {
		if a.Key == "event" {
			event = a.Value.String()
			return false
		}
		return true
	})
	if event != "OnTestEvent" {
		t.Errorf("event = %q, want OnTestEvent", event)
	}
}

// TestHostRuntimeLog_LoopPanics pins that the contained restart line of each
// supervised background loop reaches the Host's logger.
func TestHostRuntimeLog_LoopPanics(t *testing.T) {
	t.Run("key-rotation", func(t *testing.T) {
		logs := &hostLogs{}
		h := &Host{
			keyRotStopC: make(chan struct{}),
			barrier:     newPanickingBarrier(1, 1, "test rotation panic"),
			logger:      slog.New(logs),
		}
		if !h.keyRotationLoopOnce(context.Background(), 10*time.Millisecond) {
			t.Fatal("keyRotationLoopOnce did not report the panic")
		}
		r := logs.await(t, "key rotation loop panic recovered, restarting")
		wantRecord(t, r, slog.LevelError, 0)
	})
	t.Run("pending-sweep", func(t *testing.T) {
		logs := &hostLogs{}
		h := &Host{sweepStopC: make(chan struct{}), logger: slog.New(logs)}
		ns := &nodeState{shardID: 999, replicaID: 1}
		ns.pendingProposals.Store(uint64(1), &pendingProposal{deadline: time.Now().Add(-time.Second)})
		h.nodes.Store(uint64(999), ns)
		if !h.pendingSweepLoopOnce(10 * time.Millisecond) {
			t.Fatal("pendingSweepLoopOnce did not report the panic")
		}
		r := logs.await(t, "pending sweep loop panic recovered, restarting")
		wantRecord(t, r, slog.LevelError, 0)
	})
}

// TestHostRuntimeLog_CloseTypeAssertions pins the ERRORs Close logs for a
// malformed nodes-map key and value.
func TestHostRuntimeLog_CloseTypeAssertions(t *testing.T) {
	logs := &hostLogs{}
	h := newLoggedTestHost(t, logs)
	h.nodes.Store("bad-key", "bad-type")
	h.nodes.Store(uint64(2), "bad-type")
	if err := h.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	wantTypeAssertion(t, logs.await(t, "Close: nodes map key type assertion failed"))
	r := logs.await(t, "Close: nodes map value type assertion failed")
	wantRecord(t, r, slog.LevelError, 2)
	wantTypeAssertion(t, r)
}

// TestHostRuntimeLog_ShardBarriersDefaultWAL pins that NewHost's WARN that
// per-shard barriers leave the default WAL under the host-wide key reaches
// the Host's logger.
func TestHostRuntimeLog_ShardBarriersDefaultWAL(t *testing.T) {
	logs := &hostLogs{}
	shardBarrier := createTestBarrier(t)
	h, err := NewHost(testHostConfig(t),
		WithBarrier(createTestBarrier(t)),
		WithShardBarriers(func(uint64) Barrier { return shardBarrier }),
		WithoutTransport(),
		WithLogger(slog.New(logs)),
	)
	if err != nil {
		t.Fatalf("NewHost: %v", err)
	}
	defer h.Close()
	r := logs.await(t, "per-shard barriers do not cover the default WAL; "+
		"log entries are encrypted under the host-wide barrier, not "+
		"per-shard keys. Supply WithLogDB with a multiplexing LogDB "+
		"for per-shard WAL encryption.")
	wantRecord(t, r, slog.LevelWarn, 0)
}
