// Copyright 2026 Jeremy Hahn
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
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
	"testing"
	"time"

	"github.com/jeremyhahn/go-quicraft/pkg/config"
)

// TestRequestLeaderTransferTargets pins the documented result of
// RequestLeaderTransfer (docs/multi-group-host/api.md, Leader transfer) for
// target 0, the leader itself, a witness, a replica unknown to the shard, an
// unknown shard and a closed Host. Observer, removed and follower targets are
// covered by TestRequestLeaderTransfer_ObserverAndRemovedTargets and the
// follower test in host_test.go.
func TestRequestLeaderTransferTargets(t *testing.T) {
	t.Run("target zero", func(t *testing.T) {
		h := startSingleVoterShard(t)
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		err := h.RequestLeaderTransfer(ctx, 1, 0)
		var invalid *InvalidMemberError
		if !errors.As(err, &invalid) {
			t.Fatalf("RequestLeaderTransfer to 0 = %v (%T), want *InvalidMemberError", err, err)
		}
		if invalid.ShardID != 1 || invalid.ReplicaID != 0 {
			t.Fatalf("InvalidMemberError = shard %d replica %d, want shard 1 replica 0", invalid.ShardID, invalid.ReplicaID)
		}
		if !errors.Is(err, ErrInvalidMember) {
			t.Fatalf("RequestLeaderTransfer to 0 = %v, want a match for ErrInvalidMember", err)
		}
	})

	t.Run("leader itself", func(t *testing.T) {
		h := startSingleVoterShard(t)
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		if err := h.RequestLeaderTransfer(ctx, 1, 1); err != nil {
			t.Fatalf("RequestLeaderTransfer to the leader itself = %v, want nil", err)
		}
		// A transfer to self is a no-op: nothing is delivered, so the
		// leader must still be replica 1 after a few election ticks.
		time.Sleep(50 * time.Millisecond)
		leader, ok, err := h.LeaderID(1)
		if err != nil {
			t.Fatalf("LeaderID: %v", err)
		}
		if !ok || leader != 1 {
			t.Fatalf("LeaderID = %d (known %v), want 1", leader, ok)
		}
	})

	t.Run("witness", func(t *testing.T) {
		h := startSingleVoterShard(t)
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		// The default policy refuses a witness on a single-replica shard
		// (1 full replica < quorum 2); lift it so the witness can join.
		if err := h.SyncRequestUpdateWitnessPolicy(ctx, 1, config.WitnessConfig{
			RequireFullReplicaMajority: config.BoolPtr(false),
		}, 0); err != nil {
			t.Fatalf("SyncRequestUpdateWitnessPolicy: %v", err)
		}
		if err := h.SyncRequestAddWitness(ctx, 1, 7, "127.0.0.1:6002", 0); err != nil {
			t.Fatalf("SyncRequestAddWitness: %v", err)
		}
		members, err := h.ShardMembership(1)
		if err != nil {
			t.Fatalf("ShardMembership: %v", err)
		}
		if _, ok := members.Witnesses[7]; !ok {
			t.Fatalf("replica 7 is not a witness: %+v", members)
		}

		err = h.RequestLeaderTransfer(ctx, 1, 7)
		var invalid *InvalidMemberError
		if !errors.As(err, &invalid) {
			t.Fatalf("RequestLeaderTransfer to a witness = %v (%T), want *InvalidMemberError", err, err)
		}
		if invalid.ReplicaID != 7 {
			t.Fatalf("InvalidMemberError.ReplicaID = %d, want 7", invalid.ReplicaID)
		}
	})

	t.Run("replica not in the membership", func(t *testing.T) {
		h := startSingleVoterShard(t)
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		err := h.RequestLeaderTransfer(ctx, 1, 99)
		var notFound *ReplicaNotFoundError
		if !errors.As(err, &notFound) {
			t.Fatalf("RequestLeaderTransfer to an unknown replica = %v (%T), want *ReplicaNotFoundError", err, err)
		}
		if notFound.ShardID != 1 || notFound.ReplicaID != 99 {
			t.Fatalf("ReplicaNotFoundError = shard %d replica %d, want shard 1 replica 99", notFound.ShardID, notFound.ReplicaID)
		}
		if !errors.Is(err, ErrReplicaNotFound) {
			t.Fatalf("RequestLeaderTransfer to an unknown replica = %v, want a match for ErrReplicaNotFound", err)
		}
	})

	t.Run("unknown shard", func(t *testing.T) {
		h := startSingleVoterShard(t)
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		if err := h.RequestLeaderTransfer(ctx, 999, 1); !errors.Is(err, ErrShardNotFound) {
			t.Fatalf("RequestLeaderTransfer on an unknown shard = %v, want ErrShardNotFound", err)
		}
	})

	t.Run("closed host", func(t *testing.T) {
		h := startSingleVoterShard(t)
		if err := h.Close(); err != nil {
			t.Fatalf("Close: %v", err)
		}
		if err := h.RequestLeaderTransfer(context.Background(), 1, 1); !errors.Is(err, ErrClosed) {
			t.Fatalf("RequestLeaderTransfer on a closed Host = %v, want ErrClosed", err)
		}
	})
}
