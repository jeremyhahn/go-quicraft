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

package quicraft

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/jeremyhahn/go-quicraft/pkg/sm"
)

// Client sessions on an on-disk state machine are refused at the API boundary.
//
// The session table is replicated state that lives ONLY inside the snapshot
// stream: rsm.StateMachine writes it ahead of the user state machine's bytes and
// reads it back in RecoverFromSnapshot. A DiskStateMachine whose Open() reports
// a durable applied index never takes that path on restart — StartShard skips
// RecoverFromSnapshot entirely, because recovering would replace the user's
// database files and roll it back to the snapshot index — so the replica comes
// up with the correct data and ZERO sessions.
//
// Nothing downstream notices. The peers that still hold the session route a
// session-managed entry into sm.Update; the restarted replica hits
// ErrSessionNotFound, answers ResultSessionExpired, and never applies the write
// at all. The client sees success, because the leader completed the proposal
// from its own apply. That is a permanently missing write on one replica of a
// database, with every raft invariant intact.
//
// dragonboat places the same constraint on IOnDiskStateMachine (NO-OP sessions
// only), and the rejection here is deliberately a LOCAL decision: it reads the
// state machine's own type, fixed for the life of the shard by the sm.CreateFunc
// passed to StartShard, and never replicated state — so it cannot evaluate
// differently on two replicas, and it lands before the entry reaches the log.

// startOnDiskShard starts a single-replica shard backed by a DiskStateMachine
// and waits for it to lead, so the session entry points below are refused on
// their own merits rather than by a missing-leader precheck.
func startOnDiskShard(t *testing.T, h *Host, shardID uint64) {
	t.Helper()
	create := func(uint64, uint64) interface{} { return &recordingOnDiskSM{} }
	if err := h.StartShard(map[uint64]string{1: "127.0.0.1:5000"}, false,
		create, testShardConfig(shardID, 1)); err != nil {
		t.Fatalf("StartShard with an on-disk state machine: %v", err)
	}
	waitForLeader(t, h, shardID)
}

// TestSessions_RefusedOnDiskStateMachine walks every session entry point on the
// Host. Each must refuse with ErrSessionsUnsupportedOnDisk, and each must refuse
// BEFORE anything reaches the raft log — a rejection that arrived at apply time
// would be the divergence itself.
func TestSessions_RefusedOnDiskStateMachine(t *testing.T) {
	const shardID uint64 = 1

	h := newTestHost(t)
	startOnDiskShard(t, h, shardID)
	defer func() { _ = h.StopShard(shardID) }()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	managed := NewSession(shardID, 4242)
	managed.PrepareForPropose()

	tests := []struct {
		name string
		call func() error
	}{
		{"GetNewSession", func() error {
			_, err := h.GetNewSession(ctx, shardID)
			return err
		}},
		{"RegisterSession", func() error {
			_, err := h.RegisterSession(ctx, NewSession(shardID, 99))
			return err
		}},
		{"ProposeWithSession", func() error {
			_, err := h.ProposeWithSession(ctx, managed, []byte("v"))
			return err
		}},
		{"SyncProposeWithSession", func() error {
			_, err := h.SyncProposeWithSession(ctx, managed, []byte("v"))
			return err
		}},
		{"CloseSession", func() error {
			_, err := h.CloseSession(ctx, shardID, managed)
			return err
		}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.call()
			if !errors.Is(err, ErrSessionsUnsupportedOnDisk) {
				t.Fatalf("%s on an on-disk shard = %v, want ErrSessionsUnsupportedOnDisk. "+
					"An accepted session cannot survive a restart of this replica: its "+
					"table lives only in the snapshot stream, which an on-disk state "+
					"machine never reads back", tc.name, err)
			}
		})
	}

	// The refusal must be an API-boundary decision, so nothing may have been
	// proposed. A single-replica shard applies everything it commits, so the
	// applied index still names the leader's term-opening no-op and nothing
	// beyond it.
	ns := testLoadNode(h, shardID)
	engNode := h.engine.GetNode(shardID)
	if engNode == nil {
		t.Fatal("shard is not loaded in the engine")
	}
	if got := engNode.CommittedIndex(); got > 2 {
		t.Fatalf("committed index = %d after five refused session calls, want at "+
			"most 2 (bootstrap membership plus the leader no-op): a refusal that "+
			"lets the entry into the log has already replicated the operation it "+
			"is refusing", got)
	}
	if ns.rsm == nil || !ns.rsm.IsOnDisk() {
		t.Fatal("the fixture is not backed by an on-disk state machine")
	}
}

// TestSessions_NoOPSessionStillWorksOnDiskStateMachine pins the other half:
// dragonboat's model for an on-disk state machine is NO-OP sessions with
// idempotent operations, so those must keep working. A fix that refused every
// session shape would leave on-disk shards — go-qrdb's whole storage layer —
// with no proposal path at all.
func TestSessions_NoOPSessionStillWorksOnDiskStateMachine(t *testing.T) {
	const shardID uint64 = 1

	h := newTestHost(t)
	startOnDiskShard(t, h, shardID)
	defer func() { _ = h.StopShard(shardID) }()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	noop := h.GetNoOPSession(shardID)
	if noop == nil || !noop.IsNoOPSession() {
		t.Fatalf("GetNoOPSession returned %+v, want a no-op session", noop)
	}
	// A no-op session carries no session state, so it is not what
	// ErrSessionsUnsupportedOnDisk refuses. It has never been valid for
	// ProposeWithSession either (ValidForProposal rejects clientID 0), so the
	// supported call for it is the plain proposal path.
	if _, err := h.ProposeWithSession(ctx, noop, []byte("v")); errors.Is(err, ErrSessionsUnsupportedOnDisk) {
		t.Fatalf("ProposeWithSession with a NO-OP session = %v; the on-disk refusal "+
			"must not cover the one session form on-disk shards are supposed to use",
			err)
	}

	var result sm.Result
	result, err := h.SyncPropose(ctx, shardID, []byte("write"))
	if err != nil {
		t.Fatalf("SyncPropose on an on-disk shard = %v; refusing session-managed "+
			"proposals must leave the ordinary proposal path intact", err)
	}
	if result.Value == 0 {
		t.Fatalf("SyncPropose result = %+v, want the applied entry's index", result)
	}
}
