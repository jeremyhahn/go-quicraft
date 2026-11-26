package quicraft

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/jeremyhahn/go-quicraft/pkg/proto"
)

// TestQueryRaftLog_GatesReads checks that QueryRaftLog applies the same
// barrier and shard-state gates as QueryLocalNode before reading the log.
func TestQueryRaftLog_GatesReads(t *testing.T) {
	barrier := newTestHostBarrier(1)
	h, err := NewHost(testHostConfig(t), WithMemoryLogDB(), WithoutTransport(), WithBarrier(barrier))
	if err != nil {
		t.Fatalf("NewHost: %v", err)
	}
	defer h.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	members := map[uint64]string{1: "127.0.0.1:5000"}
	if err := h.StartShard(members, false, testCreateFunc, testShardConfig(1, 1)); err != nil {
		t.Fatalf("StartShard: %v", err)
	}
	waitForLeader(t, h, 1)
	if _, err := h.SyncPropose(ctx, 1, []byte("entry")); err != nil {
		t.Fatalf("SyncPropose: %v", err)
	}

	query := func() ([]proto.Entry, error) {
		return h.QueryRaftLog(ctx, 1, 1, 2, 1<<20)
	}

	if entries, err := query(); err != nil || len(entries) == 0 {
		t.Fatalf("QueryRaftLog before seal: entries=%d err=%v, want entries", len(entries), err)
	}

	barrier.setSealed(true)
	entries, err := query()
	if !errors.Is(err, ErrBarrierSealed) {
		t.Errorf("QueryRaftLog while sealed: err = %v, want ErrBarrierSealed", err)
	}
	if len(entries) != 0 {
		t.Errorf("QueryRaftLog while sealed returned %d entries, want none", len(entries))
	}

	barrier.setSealed(false)
	if entries, err := query(); err != nil || len(entries) == 0 {
		t.Fatalf("QueryRaftLog after unseal: entries=%d err=%v, want entries", len(entries), err)
	}

	ns, err := h.getNode(1)
	if err != nil {
		t.Fatalf("getNode: %v", err)
	}

	ns.paused.Store(true)
	_, wantErr := h.QueryLocalNode(ctx, 1, "q")
	if _, err := query(); !errors.Is(wantErr, ErrShardPaused) || !errors.Is(err, wantErr) {
		t.Errorf("QueryRaftLog while paused: err = %v, QueryLocalNode err = %v, want ErrShardPaused", err, wantErr)
	}
	ns.paused.Store(false)

	ns.stopped.Store(true)
	_, wantErr = h.QueryLocalNode(ctx, 1, "q")
	if _, err := query(); wantErr == nil || !errors.Is(err, wantErr) {
		t.Errorf("QueryRaftLog on stopped shard: err = %v, want QueryLocalNode's err %v", err, wantErr)
	}
	ns.stopped.Store(false)
}
