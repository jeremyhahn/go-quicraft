// Copyright 2026 Jeremy Hahn

package dragonboat

import (
	"context"
	"errors"
	"testing"
	"time"

	dbConfig "github.com/jeremyhahn/go-quicraft/contrib/dragonboat/config"
)

// shardInfoKindCase names a state machine kind, how to start a replica of
// it, and the dragonboat StateMachineType GetShardInfo must report for it.
type shardInfoKindCase struct {
	name     string
	smType   uint64
	startOne func(nh *NodeHost, members map[uint64]Target, cfg dbConfig.Config) error
}

func shardInfoKindCases() []shardInfoKindCase {
	return []shardInfoKindCase{
		{
			name:   "regular",
			smType: 1,
			startOne: func(nh *NodeHost, members map[uint64]Target, cfg dbConfig.Config) error {
				return nh.StartReplica(members, false, testSMFactory(), cfg)
			},
		},
		{
			name:   "concurrent",
			smType: 2,
			startOne: func(nh *NodeHost, members map[uint64]Target, cfg dbConfig.Config) error {
				return nh.StartConcurrentReplica(members, false, testConcurrentSMFactory(), cfg)
			},
		},
		{
			name:   "ondisk",
			smType: 3,
			startOne: func(nh *NodeHost, members map[uint64]Target, cfg dbConfig.Config) error {
				return nh.StartOnDiskReplica(members, false, testOnDiskSMFactory(), cfg)
			},
		},
	}
}

// startShardInfoShard starts a single-voter shard of the given kind and
// waits until it has elected this replica leader, is readable, and has
// applied at least one entry, so the shard is idle when it returns.
func startShardInfoShard(t *testing.T, nh *NodeHost, tc shardInfoKindCase, shardID, replicaID uint64) {
	t.Helper()
	members := map[uint64]Target{replicaID: nh.RaftAddress()}
	cfg := dbConfig.Config{
		ShardID:      shardID,
		ReplicaID:    replicaID,
		ElectionRTT:  5,
		HeartbeatRTT: 1,
		CheckQuorum:  true,
	}
	if err := tc.startOne(nh, members, cfg); err != nil {
		t.Fatalf("start %s replica: %v", tc.name, err)
	}

	deadline := time.Now().Add(5 * time.Second)
	for {
		leaderID, _, valid, err := nh.GetLeaderID(shardID)
		if err == nil && valid && leaderID == replicaID {
			readCtx, cancel := context.WithTimeout(context.Background(), time.Second)
			_, readErr := nh.SyncGetShardMembership(readCtx, shardID)
			cancel()
			if readErr == nil {
				hi, hiErr := nh.host.ShardInfo(shardID)
				if hiErr == nil && hi != nil && hi.LastApplied > 0 {
					return
				}
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("%s shard %d not leader, readable and applied within 5s", tc.name, shardID)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// TestNodeHost_GetShardInfo_MatchesHostForEveryKind checks that, for a
// regular, a concurrent and an on-disk state machine, GetShardInfo reports
// the leader, term and role the Host reports, the dragonboat state machine
// type numbering (Regular 1, Concurrent 2, OnDisk 3), and no pending work on
// an idle shard.
func TestNodeHost_GetShardInfo_MatchesHostForEveryKind(t *testing.T) {
	for i, tc := range shardInfoKindCases() {
		t.Run(tc.name, func(t *testing.T) {
			nh := newTestNodeHost(t)
			shardID := uint64(9310 + i)
			replicaID := uint64(1)
			startShardInfoShard(t, nh, tc, shardID, replicaID)

			info, found, err := nh.GetShardInfo(shardID)
			if err != nil {
				t.Fatalf("GetShardInfo: %v", err)
			}
			if !found || info == nil {
				t.Fatalf("GetShardInfo found=%v info=%v, want found=true and non-nil", found, info)
			}

			hostInfo, err := nh.host.ShardInfo(shardID)
			if err != nil {
				t.Fatalf("host.ShardInfo: %v", err)
			}
			if hostInfo == nil {
				t.Fatal("host.ShardInfo returned nil for a running shard")
			}
			leaderID, term, valid, err := nh.GetLeaderID(shardID)
			if err != nil || !valid {
				t.Fatalf("GetLeaderID: leader=%d valid=%v err=%v", leaderID, valid, err)
			}

			if info.ShardID != shardID || info.ReplicaID != replicaID {
				t.Errorf("ShardID/ReplicaID = %d/%d, want %d/%d",
					info.ShardID, info.ReplicaID, shardID, replicaID)
			}
			if info.LeaderID != hostInfo.LeaderID || info.LeaderID != leaderID {
				t.Errorf("LeaderID = %d, host %d, GetLeaderID %d",
					info.LeaderID, hostInfo.LeaderID, leaderID)
			}
			if info.LeaderID != replicaID {
				t.Errorf("LeaderID = %d, want the single voter %d", info.LeaderID, replicaID)
			}
			if info.Term != hostInfo.Term || info.Term != term {
				t.Errorf("Term = %d, host %d, GetLeaderID %d", info.Term, hostInfo.Term, term)
			}
			if info.Term == 0 {
				t.Error("Term = 0 after a leader was elected")
			}
			if info.IsLeader != hostInfo.IsLeader || !info.IsLeader {
				t.Errorf("IsLeader = %v, host %v, want true", info.IsLeader, hostInfo.IsLeader)
			}
			if info.IsObserver != hostInfo.IsObserver || info.IsObserver {
				t.Errorf("IsObserver = %v, host %v, want false", info.IsObserver, hostInfo.IsObserver)
			}
			if info.IsWitness != hostInfo.IsWitness || info.IsWitness {
				t.Errorf("IsWitness = %v, host %v, want false", info.IsWitness, hostInfo.IsWitness)
			}
			if info.StateMachineType != tc.smType {
				t.Errorf("StateMachineType = %d, want %d (host code %d)",
					info.StateMachineType, tc.smType, hostInfo.StateMachineType)
			}

			// (b) An idle shard has applied entries and nothing queued.
			if info.Pending {
				t.Error("Pending = true on an idle shard that has applied entries")
			}
			if info.PendingProposals != 0 {
				t.Errorf("PendingProposals = %d on an idle shard, want 0", info.PendingProposals)
			}
		})
	}
}

// TestNodeHost_GetShardInfo_NotLoadedReturnsFalse checks that a shard not
// loaded here, whether stopped by StopShard or never started, gives
// found=false, a nil ShardInfo and ErrShardNotFound (the error that
// TestNodeHost_GetShardInfo_InvalidShard also requires).
func TestNodeHost_GetShardInfo_NotLoadedReturnsFalse(t *testing.T) {
	nh := newTestNodeHost(t)

	t.Run("never_started", func(t *testing.T) {
		info, found, err := nh.GetShardInfo(93199)
		if !errors.Is(err, ErrShardNotFound) {
			t.Fatalf("GetShardInfo of a never-started shard: err=%v, want ErrShardNotFound", err)
		}
		if found || info != nil {
			t.Fatalf("GetShardInfo of a never-started shard: found=%v info=%v, want false/nil", found, info)
		}
	})

	t.Run("after_stop", func(t *testing.T) {
		shardID := uint64(9320)
		startShardInfoShard(t, nh, shardInfoKindCases()[0], shardID, 1)
		if _, found, err := nh.GetShardInfo(shardID); err != nil || !found {
			t.Fatalf("GetShardInfo before StopShard: found=%v err=%v", found, err)
		}
		if err := nh.StopShard(shardID); err != nil {
			t.Fatalf("StopShard: %v", err)
		}
		info, found, err := nh.GetShardInfo(shardID)
		if !errors.Is(err, ErrShardNotFound) {
			t.Fatalf("GetShardInfo after StopShard: err=%v, want ErrShardNotFound", err)
		}
		if found || info != nil {
			t.Fatalf("GetShardInfo after StopShard: found=%v info=%v, want false/nil", found, info)
		}
	})
}
