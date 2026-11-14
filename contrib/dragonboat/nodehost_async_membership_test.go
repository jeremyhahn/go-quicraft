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

package dragonboat

import (
	"context"
	"testing"
	"time"

	dbConfig "github.com/jeremyhahn/go-quicraft/contrib/dragonboat/config"
)

const asyncMembershipTimeout = 5 * time.Second

// asyncMembershipResult waits for the single result of rs, failing the test
// when none arrives within asyncMembershipTimeout.
func asyncMembershipResult(t *testing.T, rs *RequestState) RequestResult {
	t.Helper()
	select {
	case result := <-rs.ResultC():
		return result
	case <-time.After(asyncMembershipTimeout):
		t.Fatal("async membership request: no result within 5s")
		return RequestResult{}
	}
}

// asyncMembership reads the shard membership linearizably.
func asyncMembership(t *testing.T, nh *NodeHost, shardID uint64) *Membership {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), asyncMembershipTimeout)
	defer cancel()
	m, err := nh.SyncGetShardMembership(ctx, shardID)
	if err != nil {
		t.Fatalf("SyncGetShardMembership: %v", err)
	}
	return m
}

// TestAsyncMembership_AddDeleteAndInvalidChange drives the dragonboat async
// membership calls on a single-replica shard of a host without a transport:
// valid changes complete and show in GetShardMembership, and changes invalid
// against the membership fail at once with a nil RequestState.
func TestAsyncMembership_AddDeleteAndInvalidChange(t *testing.T) {
	nh := newTestNodeHost(t)
	const shardID = uint64(1)
	startTestShard(t, nh, shardID, 1)

	// (a) Add non-voting replicas 2 and 3, each with the current ID.
	before := asyncMembership(t, nh, shardID)
	prevID := before.ConfigChangeID
	addrs := map[uint64]Target{2: "localhost:9998", 3: "localhost:9997"}
	for _, replicaID := range []uint64{2, 3} {
		cur := asyncMembership(t, nh, shardID)
		rs, err := nh.RequestAddNonVoting(shardID, replicaID, addrs[replicaID], cur.ConfigChangeID, asyncMembershipTimeout)
		if err != nil {
			t.Fatalf("RequestAddNonVoting(%d): %v", replicaID, err)
		}
		result := asyncMembershipResult(t, rs)
		rs.Release()
		if !result.Completed() {
			t.Fatalf("RequestAddNonVoting(%d): result not completed: %+v", replicaID, result)
		}
	}
	m, err := nh.GetShardMembership(shardID)
	if err != nil {
		t.Fatalf("GetShardMembership: %v", err)
	}
	for _, replicaID := range []uint64{2, 3} {
		if _, ok := m.NonVotings[replicaID]; !ok {
			t.Fatalf("replica %d not in NonVotings after add: %+v", replicaID, m.NonVotings)
		}
	}
	if m.ConfigChangeID <= prevID {
		t.Fatalf("ConfigChangeID did not grow: before %d, after %d", prevID, m.ConfigChangeID)
	}
	prevID = m.ConfigChangeID

	// (b) Delete replica 3.
	rs, err := nh.RequestDeleteReplica(shardID, 3, m.ConfigChangeID, asyncMembershipTimeout)
	if err != nil {
		t.Fatalf("RequestDeleteReplica(3): %v", err)
	}
	result := asyncMembershipResult(t, rs)
	rs.Release()
	if !result.Completed() {
		t.Fatalf("RequestDeleteReplica(3): result not completed: %+v", result)
	}
	m, err = nh.GetShardMembership(shardID)
	if err != nil {
		t.Fatalf("GetShardMembership: %v", err)
	}
	if _, ok := m.Removed[3]; !ok {
		t.Fatalf("replica 3 not in Removed after delete: %+v", m.Removed)
	}
	if _, ok := m.NonVotings[3]; ok {
		t.Fatalf("replica 3 still in NonVotings after delete: %+v", m.NonVotings)
	}
	if _, ok := m.NonVotings[2]; !ok {
		t.Fatalf("replica 2 lost from NonVotings after deleting 3: %+v", m.NonVotings)
	}
	if m.ConfigChangeID <= prevID {
		t.Fatalf("ConfigChangeID did not grow on delete: before %d, after %d", prevID, m.ConfigChangeID)
	}

	// (c) Changes invalid against the membership fail at once.
	rs, err = nh.RequestDeleteReplica(shardID, 3, m.ConfigChangeID, asyncMembershipTimeout)
	if err == nil {
		if rs != nil {
			rs.Release()
		}
		t.Fatal("RequestDeleteReplica of a removed replica: want an error, got nil")
	}
	if rs != nil {
		t.Fatalf("RequestDeleteReplica of a removed replica: want nil RequestState, got %v", rs)
	}
	rs, err = nh.RequestAddNonVoting(shardID, 3, "localhost:9996", m.ConfigChangeID, asyncMembershipTimeout)
	if err == nil {
		if rs != nil {
			rs.Release()
		}
		t.Fatal("RequestAddNonVoting of a removed replica ID: want an error, got nil")
	}
	if rs != nil {
		t.Fatalf("RequestAddNonVoting of a removed replica ID: want nil RequestState, got %v", rs)
	}

	// The refused changes left the membership as it was.
	after, err := nh.GetShardMembership(shardID)
	if err != nil {
		t.Fatalf("GetShardMembership: %v", err)
	}
	if after.ConfigChangeID != m.ConfigChangeID {
		t.Fatalf("refused changes moved ConfigChangeID: %d -> %d", m.ConfigChangeID, after.ConfigChangeID)
	}
	if _, ok := after.NonVotings[3]; ok {
		t.Fatalf("removed replica 3 is back in NonVotings: %+v", after.NonVotings)
	}
}

// TestAsyncMembership_OrderedStaleConfigChangeID checks that, on a shard
// started with OrderedConfigChange true, RequestAddNonVoting with a stale
// ConfigChangeID is refused at once or rejected, and never completes.
func TestAsyncMembership_OrderedStaleConfigChangeID(t *testing.T) {
	nh := newTestNodeHost(t)
	const shardID = uint64(1)
	const replicaID = uint64(1)
	cfg := dbConfig.Config{
		ShardID:             shardID,
		ReplicaID:           replicaID,
		ElectionRTT:         5,
		HeartbeatRTT:        1,
		CheckQuorum:         true,
		OrderedConfigChange: true,
	}
	if err := nh.StartReplica(map[uint64]Target{replicaID: nh.RaftAddress()}, false, testSMFactory(), cfg); err != nil {
		t.Fatalf("StartReplica: %v", err)
	}
	waitAsyncMembershipReadable(t, nh, shardID, replicaID)

	// A ConfigChangeID of 0 skips the check, so take a non-zero one: the
	// ID after a first change, made stale by a second change.
	var stale uint64
	for _, replicaID := range []uint64{2, 3} {
		cur := asyncMembership(t, nh, shardID).ConfigChangeID
		addr := Target("localhost:9998")
		if replicaID == 3 {
			addr = "localhost:9997"
		}
		rs, err := nh.RequestAddNonVoting(shardID, replicaID, addr, cur, asyncMembershipTimeout)
		if err != nil {
			t.Fatalf("RequestAddNonVoting(%d): %v", replicaID, err)
		}
		result := asyncMembershipResult(t, rs)
		rs.Release()
		if !result.Completed() {
			t.Fatalf("RequestAddNonVoting(%d): result not completed: %+v", replicaID, result)
		}
		if replicaID == 2 {
			stale = asyncMembership(t, nh, shardID).ConfigChangeID
		}
	}
	if stale == 0 {
		t.Fatal("ConfigChangeID after a change is 0")
	}
	cur := asyncMembership(t, nh, shardID)
	if cur.ConfigChangeID <= stale {
		t.Fatalf("ConfigChangeID did not grow: before %d, after %d", stale, cur.ConfigChangeID)
	}

	rs, err := nh.RequestAddNonVoting(shardID, 4, "localhost:9996", stale, asyncMembershipTimeout)
	var result RequestResult
	if err != nil {
		if rs != nil {
			t.Fatalf("stale RequestAddNonVoting: error %v with a non-nil RequestState", err)
		}
	} else {
		result = asyncMembershipResult(t, rs)
		rs.Release()
		if result.Completed() {
			t.Fatal("stale RequestAddNonVoting completed under OrderedConfigChange")
		}
		if !result.Rejected() {
			t.Fatalf("stale RequestAddNonVoting: want Rejected, got %+v", result)
		}
	}

	m := asyncMembership(t, nh, shardID)
	if _, ok := m.NonVotings[4]; ok {
		t.Fatalf("stale change added replica 4: %+v", m.NonVotings)
	}
}

// waitAsyncMembershipReadable waits until replicaID leads shardID and the
// shard serves a linearizable membership read.
func waitAsyncMembershipReadable(t *testing.T, nh *NodeHost, shardID, replicaID uint64) {
	t.Helper()
	deadline := time.Now().Add(asyncMembershipTimeout)
	for time.Now().Before(deadline) {
		leaderID, _, valid, err := nh.GetLeaderID(shardID)
		if err == nil && valid && leaderID == replicaID {
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			_, readErr := nh.SyncGetShardMembership(ctx, shardID)
			cancel()
			if readErr == nil {
				return
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("shard %d not led by %d and readable within 5s", shardID, replicaID)
}
