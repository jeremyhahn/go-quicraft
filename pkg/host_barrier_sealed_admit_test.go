package quicraft

import (
	"context"
	"errors"
	"testing"
	"time"
)

// setSealed flips the fake barrier's sealed state. testHostBarrier has a Seal
// but no Unseal; the host only ever reads IsSealed, so toggling the field is
// what an operator's Seal/Unseal looks like from the host's side.
func (b *testHostBarrier) setSealed(sealed bool) {
	b.mu.Lock()
	b.sealedVal = sealed
	b.mu.Unlock()
}

// sealedAdmitCall is one client entry point, reduced to the error it returns.
type sealedAdmitCall struct {
	name string
	call func(ctx context.Context) error
}

// sealedAdmitCalls lists every client entry point the sealed admit check
// covers. session is a registered session on shard 1.
func sealedAdmitCalls(h *Host, session *Session) []sealedAdmitCall {
	return []sealedAdmitCall{
		{"Propose", func(ctx context.Context) error {
			_, err := h.Propose(ctx, 1, []byte("cmd"))
			return err
		}},
		{"SyncPropose", func(ctx context.Context) error {
			_, err := h.SyncPropose(ctx, 1, []byte("cmd"))
			return err
		}},
		{"ProposeWithSession", func(ctx context.Context) error {
			_, err := h.ProposeWithSession(ctx, session, []byte("cmd"))
			return err
		}},
		{"NewSession", func(ctx context.Context) error {
			_, err := h.NewSession(ctx, 1)
			return err
		}},
		{"RegisterSession", func(ctx context.Context) error {
			_, err := h.RegisterSession(ctx, NewSession(1, 0xfeed))
			return err
		}},
		{"CloseSession", func(ctx context.Context) error {
			_, err := h.CloseSession(ctx, 1, session)
			return err
		}},
		{"ReadIndex", func(ctx context.Context) error {
			_, err := h.ReadIndex(ctx, 1)
			return err
		}},
		{"SyncRead", func(ctx context.Context) error {
			_, err := h.SyncRead(ctx, 1, "q")
			return err
		}},
		{"StaleRead", func(ctx context.Context) error {
			_, err := h.StaleRead(ctx, 1, "q")
			return err
		}},
		{"StaleReadBuf", func(ctx context.Context) error {
			_, release, err := h.StaleReadBuf(ctx, 1, []byte("k"))
			if release != nil {
				release()
			}
			return err
		}},
		{"FollowerRead", func(ctx context.Context) error {
			_, err := h.FollowerRead(ctx, 1, "q")
			return err
		}},
		{"RequestSnapshot", func(ctx context.Context) error {
			_, err := h.RequestSnapshot(ctx, 1, SnapshotOption{})
			return err
		}},
		{"ExecuteOnLeader", func(ctx context.Context) error {
			_, err := h.ExecuteOnLeader(ctx, 1, []byte("req"))
			return err
		}},
		// Last: the membership change commits once unsealed and leaves the
		// single-node shard waiting on a replica that never starts.
		{"RequestAddNode", func(ctx context.Context) error {
			_, err := h.RequestAddNode(ctx, 1, 2, "127.0.0.1:5001", 0)
			return err
		}},
	}
}

// assertSealedAdmitRefusals seals the barrier, checks that every client entry
// point refuses with ErrBarrierSealed, then unseals and checks that the shard
// accepts work again: a proposal commits, a linearizable read is served, and
// no entry point still reports the barrier sealed.
func assertSealedAdmitRefusals(t *testing.T, h *Host, barrier *testHostBarrier) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	members := map[uint64]string{1: "127.0.0.1:5000"}
	if err := h.StartShard(members, false, testCreateFunc, testShardConfig(1, 1)); err != nil {
		t.Fatalf("StartShard: %v", err)
	}
	waitForLeader(t, h, 1)
	session, err := h.NewSession(ctx, 1)
	if err != nil {
		t.Fatalf("NewSession: %v", err)
	}

	barrier.setSealed(true)
	for _, c := range sealedAdmitCalls(h, session) {
		if err := c.call(ctx); !errors.Is(err, ErrBarrierSealed) {
			t.Errorf("%s while sealed: err = %v, want ErrBarrierSealed", c.name, err)
		}
	}
	// The refusals touched nothing: the shard is still healthy, not halted.
	if engNode := h.engine.GetNode(1); engNode == nil || engNode.IsFailed() {
		t.Fatal("shard failed while sealed: the admit check must refuse before work reaches the log")
	}

	barrier.setSealed(false)
	if _, err := h.SyncPropose(ctx, 1, []byte("after-unseal")); err != nil {
		t.Fatalf("SyncPropose after unseal: %v", err)
	}
	if _, err := h.SyncRead(ctx, 1, "q"); err != nil {
		t.Fatalf("SyncRead after unseal: %v", err)
	}
	for _, c := range sealedAdmitCalls(h, session) {
		if err := c.call(ctx); errors.Is(err, ErrBarrierSealed) {
			t.Errorf("%s after unseal still refused with ErrBarrierSealed", c.name)
		}
	}
}

// TestSealedHostBarrier_RefusesClientWork covers the host-wide barrier: every
// client entry point returns ErrBarrierSealed up front while it is sealed, and
// the shard serves again once it is unsealed (#296).
func TestSealedHostBarrier_RefusesClientWork(t *testing.T) {
	barrier := newTestHostBarrier(1)
	h, err := NewHost(testHostConfig(t), WithMemoryLogDB(), WithoutTransport(), WithBarrier(barrier))
	if err != nil {
		t.Fatalf("NewHost: %v", err)
	}
	defer h.Close()
	assertSealedAdmitRefusals(t, h, barrier)
}

// TestSealedShardBarrier_RefusesClientWork covers a per-shard barrier: the
// shard's own barrier, not a host-wide one, gates its entry points (#296).
func TestSealedShardBarrier_RefusesClientWork(t *testing.T) {
	barrier := newTestHostBarrier(1)
	h, err := NewHost(testHostConfig(t), WithMemoryLogDB(), WithoutTransport(),
		WithShardBarriers(func(uint64) Barrier { return barrier }))
	if err != nil {
		t.Fatalf("NewHost: %v", err)
	}
	defer h.Close()
	assertSealedAdmitRefusals(t, h, barrier)
}

// TestSealedShardBarrier_OtherShardUnaffected verifies that sealing one
// tenant's barrier refuses only that tenant's shard: a shard under a different,
// unsealed barrier on the same host keeps serving.
func TestSealedShardBarrier_OtherShardUnaffected(t *testing.T) {
	sealedB := newTestHostBarrier(1)
	openB := newTestHostBarrier(1)
	h, err := NewHost(testHostConfig(t), WithMemoryLogDB(), WithoutTransport(),
		WithShardBarriers(func(shardID uint64) Barrier {
			if shardID == 1 {
				return sealedB
			}
			return openB
		}))
	if err != nil {
		t.Fatalf("NewHost: %v", err)
	}
	defer h.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	for _, id := range []uint64{1, 2} {
		if err := h.StartShard(map[uint64]string{1: "127.0.0.1:5000"}, false,
			testCreateFunc, testShardConfig(id, 1)); err != nil {
			t.Fatalf("StartShard(%d): %v", id, err)
		}
		waitForLeader(t, h, id)
	}

	sealedB.setSealed(true)
	if _, err := h.SyncPropose(ctx, 1, []byte("cmd")); !errors.Is(err, ErrBarrierSealed) {
		t.Fatalf("SyncPropose on the sealed tenant: err = %v, want ErrBarrierSealed", err)
	}
	if _, err := h.SyncPropose(ctx, 2, []byte("cmd")); err != nil {
		t.Fatalf("SyncPropose on the unsealed tenant: %v", err)
	}
}
