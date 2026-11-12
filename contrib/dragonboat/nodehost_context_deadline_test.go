package dragonboat

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"testing"
	"time"
)

// deadlineCall is one NodeHost Sync call that takes a context, run
// against the given shard.
type deadlineCall struct {
	name string
	call func(ctx context.Context, nh *NodeHost, shardID uint64) error
}

// deadlineCalls lists every Sync call guarded by checkContextDeadline.
func deadlineCalls() []deadlineCall {
	return []deadlineCall{
		{"SyncPropose", func(ctx context.Context, nh *NodeHost, shardID uint64) error {
			_, err := nh.SyncPropose(ctx, nh.GetNoOPSession(shardID), []byte("x"))
			return err
		}},
		{"SyncRead", func(ctx context.Context, nh *NodeHost, shardID uint64) error {
			_, err := nh.SyncRead(ctx, shardID, nil)
			return err
		}},
		{"SyncGetSession", func(ctx context.Context, nh *NodeHost, shardID uint64) error {
			_, err := nh.SyncGetSession(ctx, shardID)
			return err
		}},
		{"SyncCloseSession", func(ctx context.Context, nh *NodeHost, shardID uint64) error {
			return nh.SyncCloseSession(ctx, nh.GetNoOPSession(shardID))
		}},
		{"SyncGetShardMembership", func(ctx context.Context, nh *NodeHost, shardID uint64) error {
			_, err := nh.SyncGetShardMembership(ctx, shardID)
			return err
		}},
		{"SyncRequestAddReplica", func(ctx context.Context, nh *NodeHost, shardID uint64) error {
			return nh.SyncRequestAddReplica(ctx, shardID, 2, "localhost:63002", 0)
		}},
		{"SyncRequestAddReplicaWithIdentity", func(ctx context.Context, nh *NodeHost, shardID uint64) error {
			return nh.SyncRequestAddReplicaWithIdentity(ctx, shardID, 2, "localhost:63002", "node-2", 0)
		}},
		{"SyncRequestAddNonVotingWithIdentity", func(ctx context.Context, nh *NodeHost, shardID uint64) error {
			return nh.SyncRequestAddNonVotingWithIdentity(ctx, shardID, 3, "localhost:63003", "node-3", 0)
		}},
		{"SyncRequestAddWitnessWithIdentity", func(ctx context.Context, nh *NodeHost, shardID uint64) error {
			return nh.SyncRequestAddWitnessWithIdentity(ctx, shardID, 4, "localhost:63004", "node-4", 0)
		}},
		{"SyncRequestAddNonVoting", func(ctx context.Context, nh *NodeHost, shardID uint64) error {
			return nh.SyncRequestAddNonVoting(ctx, shardID, 3, "localhost:63003", 0)
		}},
		{"SyncRequestAddWitness", func(ctx context.Context, nh *NodeHost, shardID uint64) error {
			return nh.SyncRequestAddWitness(ctx, shardID, 4, "localhost:63004", 0)
		}},
		{"SyncRequestDeleteReplica", func(ctx context.Context, nh *NodeHost, shardID uint64) error {
			return nh.SyncRequestDeleteReplica(ctx, shardID, 1, 0)
		}},
		{"SyncRequestSnapshot", func(ctx context.Context, nh *NodeHost, shardID uint64) error {
			_, err := nh.SyncRequestSnapshot(ctx, shardID, DefaultSnapshotOption)
			return err
		}},
	}
}

// shardMembership reads a shard's membership with a valid deadline.
func shardMembership(ctx context.Context, t *testing.T, nh *NodeHost, shardID uint64) *Membership {
	t.Helper()
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	m, err := nh.SyncGetShardMembership(ctx, shardID)
	if err != nil {
		t.Fatalf("SyncGetShardMembership(%d) = %v", shardID, err)
	}
	return m
}

// TestNodeHost_SyncCallsContextDeadline checks that every Sync call that
// takes a context refuses one with no deadline with ErrDeadlineNotSet and
// an expired one with ErrInvalidDeadline, on a running shard and on one
// the host does not run, and that a refused call changes nothing.
func TestNodeHost_SyncCallsContextDeadline(t *testing.T) {
	nh := newTestNodeHost(t)
	startTestShard(t, nh, 1, 1)

	before := shardMembership(context.Background(), t, nh, 1)

	calls := deadlineCalls()
	if len(calls) != 13 {
		t.Fatalf("table has %d calls, want 13", len(calls))
	}
	for _, shardID := range []uint64{1, 99} {
		for _, c := range calls {
			t.Run(fmt.Sprintf("shard%d/%s", shardID, c.name), func(t *testing.T) {
				if err := c.call(context.Background(), nh, shardID); !errors.Is(err, ErrDeadlineNotSet) {
					t.Errorf("shard %d, no deadline: err = %v, want ErrDeadlineNotSet", shardID, err)
				}
				expired, cancel := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
				defer cancel()
				if err := c.call(expired, nh, shardID); !errors.Is(err, ErrInvalidDeadline) {
					t.Errorf("shard %d, expired deadline: err = %v, want ErrInvalidDeadline", shardID, err)
				}
			})
		}
	}

	after := shardMembership(context.Background(), t, nh, 1)
	if !reflect.DeepEqual(before, after) {
		t.Errorf("membership changed: before %+v, after %+v", before, after)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := nh.SyncPropose(ctx, nh.GetNoOPSession(1), []byte("after")); err != nil {
		t.Errorf("SyncPropose with a valid deadline = %v, want nil", err)
	}
}
