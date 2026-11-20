// Copyright 2026 Jeremy Hahn
// SPDX-License-Identifier: Apache-2.0

package quicraft

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/jeremyhahn/go-quicraft/pkg/config"
	"github.com/jeremyhahn/go-quicraft/pkg/internal/raft"
	"github.com/jeremyhahn/go-quicraft/pkg/proto"
)

// TestTranslateProposalError_StaleConfigChange verifies that the leader
// worker's refusal of a bound add reaches the caller as the public
// *StaleConfigChangeError, carrying the shard and both indexes, and not as
// the internal raft error or a pending-change rejection.
func TestTranslateProposalError_StaleConfigChange(t *testing.T) {
	h := &Host{}
	ns := &nodeState{shardID: 9}
	in := fmt.Errorf("propose: %w", &raft.StaleConfigChangeError{Expected: 12, Got: 7})
	err := h.translateProposalError(ns, in)
	if !errors.Is(err, ErrStaleConfigChange) {
		t.Fatalf("translateProposalError = %v, want ErrStaleConfigChange", err)
	}
	if errors.Is(err, ErrRejected) {
		t.Fatalf("translateProposalError = %v, must not be ErrRejected", err)
	}
	var stale *StaleConfigChangeError
	if !errors.As(err, &stale) {
		t.Fatalf("translateProposalError = %T, want *StaleConfigChangeError", err)
	}
	if stale.ShardID != 9 || stale.Expected != 12 || stale.Got != 7 {
		t.Fatalf("got %+v, want ShardID 9, Expected 12, Got 7", *stale)
	}
}

type workerAddKind struct {
	name string
	add  func(ctx context.Context, h *Host, replicaID uint64, addr string, idx uint64, opts ...ConfigChangeOption) error
	// prep runs before the index is read; the witness kind relaxes the
	// full-replica-majority policy so a single-voter shard can take a witness.
	prep func(ctx context.Context, t *testing.T, h *Host)
}

var workerAddKinds = []workerAddKind{
	{name: "voter", add: func(ctx context.Context, h *Host, id uint64, addr string, idx uint64, opts ...ConfigChangeOption) error {
		return h.SyncRequestAddNode(ctx, 1, id, addr, idx, opts...)
	}},
	{name: "observer", add: func(ctx context.Context, h *Host, id uint64, addr string, idx uint64, opts ...ConfigChangeOption) error {
		return h.SyncRequestAddObserver(ctx, 1, id, addr, idx, opts...)
	}},
	{name: "witness", add: func(ctx context.Context, h *Host, id uint64, addr string, idx uint64, opts ...ConfigChangeOption) error {
		return h.SyncRequestAddWitness(ctx, 1, id, addr, idx, opts...)
	}, prep: func(ctx context.Context, t *testing.T, h *Host) {
		t.Helper()
		if err := h.SyncRequestUpdateWitnessPolicy(ctx, 1, config.WitnessConfig{
			RequireFullReplicaMajority: config.BoolPtr(false),
		}, 0); err != nil {
			t.Fatalf("SyncRequestUpdateWitnessPolicy: %v", err)
		}
	}},
}

// interleaveConfigChange makes the next config change request commit another
// membership change (an observer add) after the Host's checks have passed and
// before its entry reaches the leader's worker: the window the Host's early
// check cannot close.
func interleaveConfigChange(ctx context.Context, t *testing.T, h *Host) *bool {
	t.Helper()
	fired := new(bool)
	testHookBeforeConfigChangeDeliver = func(proto.ConfigChange) {
		testHookBeforeConfigChangeDeliver = nil
		*fired = true
		if err := h.SyncRequestAddObserver(ctx, 1, 90, "127.0.0.1:6090", 0); err != nil {
			t.Errorf("interleaved SyncRequestAddObserver: %v", err)
		}
	}
	t.Cleanup(func() { testHookBeforeConfigChangeDeliver = nil })
	return fired
}

func currentConfigChangeID(t *testing.T, h *Host) uint64 {
	t.Helper()
	engNode := h.engine.GetNode(1)
	if engNode == nil || engNode.Peer() == nil {
		t.Fatal("shard 1 has no engine node")
	}
	return engNode.Peer().GetMembership().ConfigChangeID
}

func assertWorkerStale(t *testing.T, err error, got uint64) {
	t.Helper()
	if !errors.Is(err, ErrStaleConfigChange) {
		t.Fatalf("add = %v, want ErrStaleConfigChange from the leader's worker", err)
	}
	var stale *StaleConfigChangeError
	if !errors.As(err, &stale) {
		t.Fatalf("add = %T, want *StaleConfigChangeError", err)
	}
	if stale.ShardID != 1 || stale.Got != got || stale.Expected == got {
		t.Fatalf("StaleConfigChangeError = %+v, want ShardID 1, Got %d, Expected != Got", *stale, got)
	}
}

// TestRequestAdd_WorkerRefusesStaleIndex verifies, on the real Host, that an
// add checked against index C is refused at the leader's worker when another
// membership change commits between the Host's check and the proposal, for
// voter, observer and witness, from C > 0 (the index is always bound) and
// for voter and observer from the initial configuration (C = 0, bound by
// WithBoundConfigChangeIndex). Nothing enters the log: the same replica is
// then added successfully against the current index.
func TestRequestAdd_WorkerRefusesStaleIndex(t *testing.T) {
	for _, kind := range workerAddKinds {
		for _, fromZero := range []bool{false, true} {
			if fromZero && kind.prep != nil {
				// The witness kind needs a policy change first, so it can
				// never start from index 0 on a single-voter shard; the
				// validator unit tests cover the bound witness add at 0.
				continue
			}
			name := kind.name + "/from_C"
			if fromZero {
				name = kind.name + "/from_0"
			}
			t.Run(name, func(t *testing.T) {
				h := startSingleVoterShard(t)
				ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
				defer cancel()
				if kind.prep != nil {
					kind.prep(ctx, t, h)
				}
				if !fromZero && currentConfigChangeID(t, h) == 0 {
					if err := h.SyncRequestAddObserver(ctx, 1, 80, "127.0.0.1:6080", 0); err != nil {
						t.Fatalf("SyncRequestAddObserver: %v", err)
					}
				}
				idx := currentConfigChangeID(t, h)
				if fromZero != (idx == 0) {
					t.Fatalf("precondition: ConfigChangeID = %d, fromZero = %v", idx, fromZero)
				}
				var opts []ConfigChangeOption
				if fromZero {
					opts = append(opts, WithBoundConfigChangeIndex())
				}

				fired := interleaveConfigChange(ctx, t, h)
				err := kind.add(ctx, h, 50, "127.0.0.1:6050", idx, opts...)
				if !*fired {
					t.Fatal("the interleaving hook never ran")
				}
				assertWorkerStale(t, err, idx)

				// Nothing entered the log: re-adding replica 50 against the
				// current index is not refused as a duplicate. Voter adds would
				// stall a single-voter shard's commit, so only check the
				// observer and witness kinds this way.
				if kind.name != "voter" {
					if err := kind.add(ctx, h, 50, "127.0.0.1:6050", currentConfigChangeID(t, h)); err != nil {
						t.Fatalf("re-add against the current index: %v", err)
					}
				} else if id := currentConfigChangeID(t, h); id == idx {
					t.Fatalf("interleaved change did not commit (ConfigChangeID still %d)", id)
				}
			})
		}
	}
}

// TestRequestAdd_UnboundZeroIndexUnchecked verifies that an add with index 0
// and no WithBoundConfigChangeIndex is not checked, as before: it commits even
// when another change commits inside the window.
func TestRequestAdd_UnboundZeroIndexUnchecked(t *testing.T) {
	h := startSingleVoterShard(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	fired := interleaveConfigChange(ctx, t, h)
	if err := h.SyncRequestAddObserver(ctx, 1, 50, "127.0.0.1:6050", 0); err != nil {
		t.Fatalf("unbound SyncRequestAddObserver at 0: %v", err)
	}
	if !*fired {
		t.Fatal("the interleaving hook never ran")
	}
}

// TestRequestAdd_BoundZeroIndexAccepted verifies that a bound add at 0 on the
// initial configuration commits when nothing interleaves.
func TestRequestAdd_BoundZeroIndexAccepted(t *testing.T) {
	h := startSingleVoterShard(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if id := currentConfigChangeID(t, h); id != 0 {
		t.Fatalf("precondition: initial ConfigChangeID = %d, want 0", id)
	}
	if err := h.SyncRequestAddObserver(ctx, 1, 50, "127.0.0.1:6050", 0, WithBoundConfigChangeIndex()); err != nil {
		t.Fatalf("bound SyncRequestAddObserver at 0: %v", err)
	}
	// Bound at 0 after a change committed: refused by the Host's early check.
	err := h.SyncRequestAddObserver(ctx, 1, 51, "127.0.0.1:6051", 0, WithBoundConfigChangeIndex())
	if !errors.Is(err, ErrStaleConfigChange) {
		t.Fatalf("bound add at 0 after a commit = %v, want ErrStaleConfigChange", err)
	}
}
