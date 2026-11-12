package quicraft

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/jeremyhahn/go-quicraft/pkg/config"
)

// lifecycleEvent is one recorded system event callback.
type lifecycleEvent struct {
	name      string
	shardID   uint64
	replicaID uint64
}

// lifecycleRecorder records every system event callback in order.
type lifecycleRecorder struct {
	mu     sync.Mutex
	events []lifecycleEvent
}

func (r *lifecycleRecorder) record(name string, info config.EventShardInfo) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.events = append(r.events, lifecycleEvent{name: name, shardID: info.ShardID, replicaID: info.ReplicaID})
}

func (r *lifecycleRecorder) snapshot() []lifecycleEvent {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]lifecycleEvent, len(r.events))
	copy(out, r.events)
	return out
}

// count returns how many recorded events have the name and, when
// matchReplica is set, the shard and replica given.
func (r *lifecycleRecorder) count(name string, shardID, replicaID uint64, matchReplica bool) int {
	n := 0
	for _, e := range r.snapshot() {
		if e.name != name || e.shardID != shardID {
			continue
		}
		if matchReplica && e.replicaID != replicaID {
			continue
		}
		n++
	}
	return n
}

func (r *lifecycleRecorder) countName(name string) int {
	n := 0
	for _, e := range r.snapshot() {
		if e.name == name {
			n++
		}
	}
	return n
}

func (r *lifecycleRecorder) listener() *config.EventListener {
	return &config.EventListener{
		OnNodeReady: func(info config.EventShardInfo) { r.record("OnNodeReady", info) },
		OnMembershipChanged: func(info config.EventShardInfo) {
			r.record("OnMembershipChanged", info)
		},
		OnNodeUnloaded: func(info config.EventShardInfo) { r.record("OnNodeUnloaded", info) },
		OnNodeDeleted:  func(info config.EventShardInfo) { r.record("OnNodeDeleted", info) },
		OnNodeHostShuttingDown: func() {
			r.record("OnNodeHostShuttingDown", config.EventShardInfo{})
		},
	}
}

// TestLifecycleEvents_SystemCallbacksFireOnceWithShardAndReplica drives a
// Host through start, a membership change, stop, data removal and a
// repeated and concurrent Close, and checks each system event callback fires
// as docs/multi-group-host/api.md and config.EventListener document: once
// per lifecycle step, carrying the shard and replica it concerns.
func TestLifecycleEvents_SystemCallbacksFireOnceWithShardAndReplica(t *testing.T) {
	rec := &lifecycleRecorder{}
	cfg := testHostConfig(t)
	cfg.EventListener = rec.listener()
	h, err := NewHost(cfg, WithMemoryLogDB(), WithoutTransport(), WithoutEncryption())
	if err != nil {
		t.Fatalf("NewHost: %v", err)
	}
	t.Cleanup(func() { _ = h.Close() })

	// StartShard: exactly one OnNodeReady{1,1}.
	scfg := testShardConfig(1, 1)
	scfg.CheckQuorum = config.BoolPtr(false)
	if err := h.StartShard(map[uint64]string{1: "127.0.0.1:5000"}, false, testCreateFunc, scfg); err != nil {
		t.Fatalf("StartShard: %v", err)
	}
	waitForLeader(t, h, 1)
	if got := rec.countName("OnNodeReady"); got != 1 {
		t.Fatalf("OnNodeReady fired %d times after StartShard, want 1: %+v", got, rec.snapshot())
	}
	if got := rec.count("OnNodeReady", 1, 1, true); got != 1 {
		t.Fatalf("OnNodeReady{1,1} fired %d times, want 1: %+v", got, rec.snapshot())
	}

	// A committed membership change: at least one more
	// OnMembershipChanged{ShardID 1} than before it.
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	// OnApplied publishes lastApplied before it fires OnMembershipChanged,
	// so the bootstrap callback may still be pending after waitForLeader.
	// Wait for it, so it cannot stand in for the observer's callback.
	bootDeadline := time.Now().Add(5 * time.Second)
	for rec.count("OnMembershipChanged", 1, 0, false) < 1 && time.Now().Before(bootDeadline) {
		time.Sleep(time.Millisecond)
	}
	if rec.count("OnMembershipChanged", 1, 0, false) < 1 {
		t.Fatalf("bootstrap OnMembershipChanged{ShardID 1} never fired: %+v", rec.snapshot())
	}
	before := rec.count("OnMembershipChanged", 1, 0, false)
	if err := h.SyncRequestAddObserver(ctx, 1, 2, "127.0.0.1:6000", 0); err != nil {
		t.Fatalf("SyncRequestAddObserver: %v", err)
	}
	// The listener fires on the apply worker after the batch's proposals
	// complete, so it may trail the Sync call's return.
	deadline := time.Now().Add(5 * time.Second)
	for rec.count("OnMembershipChanged", 1, 0, false) <= before && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if got := rec.count("OnMembershipChanged", 1, 0, false); got <= before {
		t.Fatalf("OnMembershipChanged{ShardID 1} count %d after adding an observer, want > %d: %+v",
			got, before, rec.snapshot())
	}
	membership := rec.snapshot()
	onShard, onReplica := 0, 0
	for _, e := range membership {
		if e.name != "OnMembershipChanged" || e.shardID != 1 {
			continue
		}
		onShard++
		if e.replicaID == 1 {
			onReplica++
		}
	}
	if onReplica != onShard {
		t.Fatalf("OnMembershipChanged on shard 1 carried a replica other than 1: %+v", membership)
	}

	// StopShard: exactly one OnNodeUnloaded{1,1}.
	if err := h.StopShard(1); err != nil {
		t.Fatalf("StopShard: %v", err)
	}
	if got := rec.countName("OnNodeUnloaded"); got != 1 {
		t.Fatalf("OnNodeUnloaded fired %d times after StopShard, want 1: %+v", got, rec.snapshot())
	}
	if got := rec.count("OnNodeUnloaded", 1, 1, true); got != 1 {
		t.Fatalf("OnNodeUnloaded{1,1} fired %d times, want 1: %+v", got, rec.snapshot())
	}

	// SyncRemoveData: exactly one OnNodeDeleted{1,1}.
	if err := h.SyncRemoveData(ctx, 1, 1); err != nil {
		t.Fatalf("SyncRemoveData: %v", err)
	}
	if got := rec.countName("OnNodeDeleted"); got != 1 {
		t.Fatalf("OnNodeDeleted fired %d times after SyncRemoveData, want 1: %+v", got, rec.snapshot())
	}
	if got := rec.count("OnNodeDeleted", 1, 1, true); got != 1 {
		t.Fatalf("OnNodeDeleted{1,1} fired %d times, want 1: %+v", got, rec.snapshot())
	}

	// Close concurrently from several goroutines, then twice more in
	// sequence: exactly one OnNodeHostShuttingDown.
	const closers = 4
	var wg sync.WaitGroup
	start := make(chan struct{})
	for i := 0; i < closers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			_ = h.Close()
		}()
	}
	close(start)
	wg.Wait()
	_ = h.Close()
	_ = h.Close()
	if got := rec.countName("OnNodeHostShuttingDown"); got != 1 {
		t.Fatalf("OnNodeHostShuttingDown fired %d times across %d Close calls, want 1: %+v",
			got, closers+2, rec.snapshot())
	}

	// A StartShard after Close is refused and fires no OnNodeReady.
	if err := h.StartShard(map[uint64]string{1: "127.0.0.1:5000"}, false, testCreateFunc, testShardConfig(3, 1)); err == nil {
		t.Fatal("StartShard after Close succeeded, want an error")
	}
	events := rec.snapshot()
	shutdownAt := -1
	for i, e := range events {
		if e.name == "OnNodeHostShuttingDown" {
			shutdownAt = i
		}
	}
	for _, e := range events[shutdownAt+1:] {
		if e.name == "OnNodeReady" {
			t.Fatalf("OnNodeReady fired after OnNodeHostShuttingDown: %+v", events)
		}
	}

	// Every shard event names its shard.
	for _, e := range events {
		if e.name != "OnNodeHostShuttingDown" && e.shardID == 0 {
			t.Fatalf("%s fired with a zero ShardID: %+v", e.name, events)
		}
	}
}
