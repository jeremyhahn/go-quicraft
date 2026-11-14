package quicraft

import (
	"context"
	"errors"
	"maps"
	"testing"
	"time"

	"github.com/jeremyhahn/go-quicraft/pkg/config"
)

// TestOrderedConfigChange_SecondChangeWhilePendingIsRejected pins the
// documented answer to a membership change proposed while an earlier one is
// still uncommitted (docs/membership-changes/user.md, Troubleshooting): the
// second change is refused with ErrRejected, definitely (MaybeApplied false),
// and the membership is left as it was.
func TestOrderedConfigChange_SecondChangeWhilePendingIsRejected(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	runOrderedConfigChangeSequence(ctx, t, nil)
}

// TestOrderedConfigChange_FalseStillSerialises pins what
// Config.OrderedConfigChange = BoolPtr(false) does: nothing. Raft's
// pending-change gate is a safety requirement (PhD thesis 4.1) and applies
// whatever the field says, so the sequence is refused exactly as with the
// default, as the field's doc comment states.
func TestOrderedConfigChange_FalseStillSerialises(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	runOrderedConfigChangeSequence(ctx, t, config.BoolPtr(false))
}

// runOrderedConfigChangeSequence drives steps (a)-(c) on a single-replica
// shard of a transportless Host, with OrderedConfigChange set to ordered
// (nil keeps the default).
func runOrderedConfigChangeSequence(ctx context.Context, t *testing.T, ordered *bool) {
	t.Helper()
	h := newTestHost(t)

	scfg := config.Config{
		ShardID:             1,
		ReplicaID:           1,
		OrderedConfigChange: ordered,
		// The lone live voter must keep leadership once the voting set
		// grows to two; with CheckQuorum it would step down after an
		// election timeout and the refusals below would race NotLeader.
		CheckQuorum: config.BoolPtr(false),
	}
	scfg.SetDefaults()
	if got := scfg.GetOrderedConfigChange(); ordered != nil && got != *ordered {
		t.Fatalf("GetOrderedConfigChange = %v, want %v", got, *ordered)
	}
	members := map[uint64]string{1: "127.0.0.1:5000"}
	if err := h.StartShard(members, false, testCreateFunc, scfg); err != nil {
		t.Fatalf("StartShard: %v", err)
	}
	waitForLeader(t, h, 1)

	// (a) Replica 2 is never started: once this commits the shard needs
	// two votes and nothing further can commit.
	if err := h.SyncRequestAddNode(ctx, 1, 2, "127.0.0.1:5002", 0); err != nil {
		t.Fatalf("(a) SyncRequestAddNode(2): %v", err)
	}
	before, err := h.ShardMembership(1)
	if err != nil {
		t.Fatalf("ShardMembership: %v", err)
	}
	if len(before.Nodes) != 2 {
		t.Fatalf("(a) voters after adding replica 2 = %v, want replicas 1 and 2", before.Nodes)
	}

	waitForDiagLive(t, h, 1)
	diag, err := h.DiagRaftState(1)
	if err != nil {
		t.Fatalf("DiagRaftState: %v", err)
	}
	lastBefore := diag.LastIndex

	// (b) A change that is appended but cannot commit.
	pendingCtx, pendingCancel := context.WithTimeout(ctx, 20*time.Second)
	defer pendingCancel()
	pending, err := h.RequestAddNode(pendingCtx, 1, 3, "127.0.0.1:5003", 0)
	if err != nil {
		t.Fatalf("(b) RequestAddNode(3): %v", err)
	}
	if pending == nil {
		t.Fatal("(b) RequestAddNode(3) returned a nil *RequestState")
	}
	defer pending.Release()
	// The refusals below must reach raft in a later Propose than (b): a
	// batch holding two config changes is refused whole, (b) included.
	if !waitUntil(5*time.Second, func() bool {
		d, derr := h.DiagRaftState(1)
		return derr == nil && d.LastIndex > lastBefore
	}) {
		t.Fatal("(b) the add of replica 3 never reached the log")
	}

	// (c) Each further change while (b) is pending is refused.
	t.Run("RequestAddObserver", func(t *testing.T) {
		rs, err := h.RequestAddObserver(ctx, 1, 4, "127.0.0.1:5004", 0)
		assertConfigChangeRejected(ctx, t, h, before, rs, err)
	})
	t.Run("RequestRemoveNode", func(t *testing.T) {
		rs, err := h.RequestRemoveNode(ctx, 1, 2, 0)
		assertConfigChangeRejected(ctx, t, h, before, rs, err)
	})

	// (b) is still pending: it has neither committed nor failed.
	select {
	case <-pending.ResultC():
		t.Fatal("(b) the add of replica 3 completed though it cannot commit")
	default:
	}
}

// assertConfigChangeRejected checks that a config change call refused with
// ErrRejected, either at once or through its RequestState, that the refusal
// is definite, and that the membership still equals before.
func assertConfigChangeRejected(ctx context.Context, t *testing.T, h *Host, before *Membership, rs *RequestState, err error) {
	t.Helper()
	if err == nil {
		if rs == nil {
			t.Fatal("no error and a nil *RequestState")
		}
		defer rs.Release()
		select {
		case res := <-rs.ResultC():
			err = res.Err
		case <-ctx.Done():
			t.Fatalf("the refused change never completed: %v", ctx.Err())
		}
	}
	if !errors.Is(err, ErrRejected) {
		t.Fatalf("error = %v, want one matching ErrRejected", err)
	}
	if MaybeApplied(err) {
		t.Fatalf("MaybeApplied(%v) = true, want false: nothing entered the log", err)
	}
	after, merr := h.ShardMembership(1)
	if merr != nil {
		t.Fatalf("ShardMembership: %v", merr)
	}
	if after.ConfigChangeID != before.ConfigChangeID ||
		!maps.Equal(after.Nodes, before.Nodes) ||
		!maps.Equal(after.Observers, before.Observers) ||
		!maps.Equal(after.Witnesses, before.Witnesses) ||
		len(after.Removed) != len(before.Removed) {
		t.Fatalf("membership changed by a refused change: before %+v, after %+v", before, after)
	}
}
