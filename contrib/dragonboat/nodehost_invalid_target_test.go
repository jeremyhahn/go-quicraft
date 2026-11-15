package dragonboat

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	dbConfig "github.com/jeremyhahn/go-quicraft/contrib/dragonboat/config"
)

// invalidAddTargets are malformed targets every add call must refuse with
// ErrInvalidAddress before it reaches the shard.
var invalidAddTargets = []struct {
	name   string
	target Target
}{
	{name: "no port", target: "localhost"},
	{name: "empty host", target: ":63002"},
	{name: "non-numeric port", target: "localhost:abc"},
}

// membershipOf reads the shard's committed membership, failing the test when
// it cannot.
func membershipOf(ctx context.Context, t *testing.T, nh *NodeHost, shardID uint64) *Membership {
	t.Helper()
	readCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	m, err := nh.SyncGetShardMembership(readCtx, shardID)
	if err != nil {
		t.Fatalf("SyncGetShardMembership(%d): %v", shardID, err)
	}
	return m
}

// assertMembershipUnchanged fails the test when the shard's membership
// differs from before.
func assertMembershipUnchanged(ctx context.Context, t *testing.T, nh *NodeHost, shardID uint64, before *Membership) {
	t.Helper()
	after := membershipOf(ctx, t, nh, shardID)
	if !reflect.DeepEqual(before, after) {
		t.Errorf("membership changed: before %+v, after %+v", before, after)
	}
}

func TestNodeHost_AsyncAdd_InvalidAddTarget(t *testing.T) {
	ctx := context.Background()
	const shardID = 1
	nh := newTestNodeHost(t)
	startTestShard(t, nh, shardID, 1)
	before := membershipOf(ctx, t, nh, shardID)

	type asyncAdd func(target Target) (*RequestState, error)
	calls := []struct {
		name string
		add  asyncAdd
	}{
		{"RequestAddReplica", func(tg Target) (*RequestState, error) {
			return nh.RequestAddReplica(shardID, 2, tg, 0, time.Second)
		}},
		{"RequestAddReplicaWithIdentity", func(tg Target) (*RequestState, error) {
			return nh.RequestAddReplicaWithIdentity(shardID, 2, tg, "node-2", 0, time.Second)
		}},
		{"RequestAddNonVoting", func(tg Target) (*RequestState, error) {
			return nh.RequestAddNonVoting(shardID, 3, tg, 0, time.Second)
		}},
		{"RequestAddNonVotingWithIdentity", func(tg Target) (*RequestState, error) {
			return nh.RequestAddNonVotingWithIdentity(shardID, 3, tg, "node-3", 0, time.Second)
		}},
		{"RequestAddWitness", func(tg Target) (*RequestState, error) {
			return nh.RequestAddWitness(shardID, 4, tg, 0, time.Second)
		}},
		{"RequestAddWitnessWithIdentity", func(tg Target) (*RequestState, error) {
			return nh.RequestAddWitnessWithIdentity(shardID, 4, tg, "node-4", 0, time.Second)
		}},
	}
	for _, c := range calls {
		for _, tc := range invalidAddTargets {
			t.Run(c.name+"/"+tc.name, func(t *testing.T) {
				rs, err := c.add(tc.target)
				if rs != nil {
					rs.Release()
					t.Errorf("%s(%q) returned a non-nil RequestState", c.name, tc.target)
				}
				if !errors.Is(err, ErrInvalidAddress) {
					t.Errorf("%s(%q) = %v, want ErrInvalidAddress", c.name, tc.target, err)
				}
				assertMembershipUnchanged(ctx, t, nh, shardID, before)
			})
		}
	}
}

func TestNodeHost_SyncAdd_InvalidAddTarget(t *testing.T) {
	ctx := context.Background()
	const shardID = 1
	nh := newTestNodeHost(t)
	startTestShard(t, nh, shardID, 1)
	before := membershipOf(ctx, t, nh, shardID)

	calls := []struct {
		name string
		add  func(ctx context.Context, target Target) error
	}{
		{"SyncRequestAddReplica", func(ctx context.Context, tg Target) error {
			return nh.SyncRequestAddReplica(ctx, shardID, 2, tg, 0)
		}},
		{"SyncRequestAddNonVoting", func(ctx context.Context, tg Target) error {
			return nh.SyncRequestAddNonVoting(ctx, shardID, 3, tg, 0)
		}},
		{"SyncRequestAddWitness", func(ctx context.Context, tg Target) error {
			return nh.SyncRequestAddWitness(ctx, shardID, 4, tg, 0)
		}},
	}
	for _, c := range calls {
		for _, tc := range invalidAddTargets {
			t.Run(c.name+"/"+tc.name, func(t *testing.T) {
				callCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
				defer cancel()
				if err := c.add(callCtx, tc.target); !errors.Is(err, ErrInvalidAddress) {
					t.Errorf("%s(%q) = %v, want ErrInvalidAddress", c.name, tc.target, err)
				}
				assertMembershipUnchanged(ctx, t, nh, shardID, before)
			})
		}
	}
}

func TestNodeHost_StartReplicaVariants_InvalidAddTarget(t *testing.T) {
	starts := []struct {
		name  string
		start func(nh *NodeHost, members map[uint64]Target, cfg dbConfig.Config) error
	}{
		{"StartConcurrentReplica", func(nh *NodeHost, members map[uint64]Target, cfg dbConfig.Config) error {
			return nh.StartConcurrentReplica(members, false, testConcurrentSMFactory(), cfg)
		}},
		{"StartOnDiskReplica", func(nh *NodeHost, members map[uint64]Target, cfg dbConfig.Config) error {
			return nh.StartOnDiskReplica(members, false, testOnDiskSMFactory(), cfg)
		}},
	}
	for _, s := range starts {
		for _, tc := range invalidAddTargets {
			t.Run(s.name+"/"+tc.name, func(t *testing.T) {
				nh := newTestNodeHost(t)
				cfg := dbConfig.Config{
					ShardID:      7,
					ReplicaID:    1,
					ElectionRTT:  5,
					HeartbeatRTT: 1,
				}
				members := map[uint64]Target{1: nh.RaftAddress(), 2: tc.target}
				if err := s.start(nh, members, cfg); !errors.Is(err, ErrInvalidTarget) {
					t.Errorf("%s(%q) = %v, want ErrInvalidTarget", s.name, tc.target, err)
				}
				info := nh.GetNodeHostInfo(NodeHostInfoOption{SkipLogInfo: true})
				if n := len(info.ShardInfoList); n != 0 {
					t.Errorf("%s(%q) left %d shard(s) running: %+v", s.name, tc.target, n, info.ShardInfoList)
				}
				if _, _, _, err := nh.GetLeaderID(cfg.ShardID); err == nil {
					t.Errorf("GetLeaderID(%d) after refused %s = nil error, want shard not found", cfg.ShardID, s.name)
				}
			})
		}
	}
}
