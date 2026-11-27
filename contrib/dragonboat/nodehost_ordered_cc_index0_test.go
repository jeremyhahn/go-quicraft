package dragonboat

import (
	"context"
	"errors"
	"testing"

	dbConfig "github.com/jeremyhahn/go-quicraft/contrib/dragonboat/config"
)

// startOrderedCCShard starts a single-replica shard with the given
// OrderedConfigChange setting and commits one membership change, so the
// current ConfigChangeID is no longer the initial one.
func startOrderedCCShard(t *testing.T, ordered bool) (*NodeHost, uint64) {
	t.Helper()
	nh := newTestNodeHost(t)
	const shardID = uint64(1)
	const replicaID = uint64(1)
	cfg := dbConfig.Config{
		ShardID:             shardID,
		ReplicaID:           replicaID,
		ElectionRTT:         5,
		HeartbeatRTT:        1,
		CheckQuorum:         true,
		OrderedConfigChange: ordered,
	}
	if err := nh.StartReplica(map[uint64]Target{replicaID: nh.RaftAddress()}, false, testSMFactory(), cfg); err != nil {
		t.Fatalf("StartReplica: %v", err)
	}
	waitAsyncMembershipReadable(t, nh, shardID, replicaID)

	cur := asyncMembership(t, nh, shardID).ConfigChangeID
	ctx, cancel := context.WithTimeout(context.Background(), asyncMembershipTimeout)
	defer cancel()
	if err := nh.SyncRequestAddNonVoting(ctx, shardID, 2, "localhost:9998", cur); err != nil {
		t.Fatalf("first SyncRequestAddNonVoting: %v", err)
	}
	if asyncMembership(t, nh, shardID).ConfigChangeID == 0 {
		t.Fatal("ConfigChangeID after a committed change is 0")
	}
	return nh, shardID
}

func isOrderedRefusal(err error) bool {
	return errors.Is(err, ErrStaleConfigChange) || errors.Is(err, ErrRejected)
}

// TestOrderedConfigChange_Index0RefusedAfterChange is the #1247 regression:
// under OrderedConfigChange, an add naming index 0 (the initial membership's
// ConfigChangeID) on a shard whose membership has changed must be refused,
// as dragonboat refuses any index other than the current one.
func TestOrderedConfigChange_Index0RefusedAfterChange(t *testing.T) {
	nh, shardID := startOrderedCCShard(t, true)

	ctx, cancel := context.WithTimeout(context.Background(), asyncMembershipTimeout)
	defer cancel()
	if err := nh.SyncRequestAddNonVoting(ctx, shardID, 3, "localhost:9997", 0); !isOrderedRefusal(err) {
		t.Fatalf("SyncRequestAddNonVoting(index 0): want stale or rejected, got %v", err)
	}
	if err := nh.SyncRequestAddWitness(ctx, shardID, 4, "localhost:9996", 0); !isOrderedRefusal(err) {
		t.Fatalf("SyncRequestAddWitness(index 0): want stale or rejected, got %v", err)
	}

	rs, err := nh.RequestAddReplica(shardID, 5, "localhost:9995", 0, asyncMembershipTimeout)
	if err != nil {
		if !isOrderedRefusal(err) {
			t.Fatalf("RequestAddReplica(index 0): want stale or rejected, got %v", err)
		}
	} else {
		result := asyncMembershipResult(t, rs)
		rs.Release()
		if result.Completed() {
			t.Fatal("RequestAddReplica(index 0) completed under OrderedConfigChange")
		}
	}

	m := asyncMembership(t, nh, shardID)
	for _, id := range []uint64{3, 4, 5} {
		if _, ok := m.NonVotings[id]; ok {
			t.Fatalf("index-0 add committed non-voting %d", id)
		}
		if _, ok := m.Witnesses[id]; ok {
			t.Fatalf("index-0 add committed witness %d", id)
		}
		if _, ok := m.Nodes[id]; ok {
			t.Fatalf("index-0 add committed replica %d", id)
		}
	}
}

// TestOrderedConfigChange_Index0UnorderedUnchanged pins that an unordered
// shard keeps accepting an add with index 0 after a membership change.
func TestOrderedConfigChange_Index0UnorderedUnchanged(t *testing.T) {
	nh, shardID := startOrderedCCShard(t, false)

	ctx, cancel := context.WithTimeout(context.Background(), asyncMembershipTimeout)
	defer cancel()
	if err := nh.SyncRequestAddNonVoting(ctx, shardID, 3, "localhost:9997", 0); err != nil {
		t.Fatalf("unordered SyncRequestAddNonVoting(index 0): %v", err)
	}
	if _, ok := asyncMembership(t, nh, shardID).NonVotings[3]; !ok {
		t.Fatal("unordered index-0 add did not commit")
	}
}
