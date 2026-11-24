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

package engine

import (
	"errors"
	"testing"
	"time"

	"github.com/jeremyhahn/go-quicraft/pkg/config"
	"github.com/jeremyhahn/go-quicraft/pkg/internal/raft"
	"github.com/jeremyhahn/go-quicraft/pkg/logdb"
	"github.com/jeremyhahn/go-quicraft/pkg/proto"
)

// Node.redeliver must preserve the origin stamp.

// TestNode_RedeliverPreservesOrigin checks a message taken out of the inbox and put back keeps its origin, so a deferred local InstallSnapshot is not refused.
func TestNode_RedeliverPreservesOrigin(t *testing.T) {
	tests := []struct {
		name    string
		deliver func(*Node, proto.Message) bool
		want    proto.MessageOrigin
	}{
		{
			name:    "locally injected InstallSnapshot",
			deliver: (*Node).DeliverLocal,
			want:    proto.OriginLocal,
		},
		{
			name:    "wire-delivered Replicate",
			deliver: (*Node).Deliver,
			want:    proto.OriginRemote,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := config.Config{ShardID: 1, ReplicaID: 1}
			cfg.SetDefaults()
			node := NewNode(nil, nil, nil, cfg, nil, "", nil)

			msg := proto.Message{
				Type:     proto.InstallSnapshot,
				From:     2,
				To:       1,
				ShardID:  1,
				Snapshot: proto.Snapshot{Index: 500, Term: 10},
			}
			if !tt.deliver(node, msg) {
				t.Fatal("initial delivery was refused")
			}
			drained := node.DrainInbox(nil)
			if len(drained) != 1 {
				t.Fatalf("drained %d messages, want 1", len(drained))
			}
			if drained[0].Origin != tt.want {
				t.Fatalf("delivery stamped Origin %v, want %v", drained[0].Origin, tt.want)
			}

			if !node.redeliver(drained[0]) {
				t.Fatal("redelivery was refused")
			}
			back := node.DrainInbox(nil)
			if len(back) != 1 {
				t.Fatalf("redelivered %d messages, want 1", len(back))
			}
			if back[0].Origin != tt.want {
				t.Fatalf("redelivery re-stamped Origin %v, want the recorded %v; "+
					"re-stamping either grants a wire message a local origin or "+
					"strips a legitimate local one", back[0].Origin, tt.want)
			}
		})
	}
}

// TestStepWorker_DeferredSnapshotSurvivesRedeliveryIntoRaft checks a redelivered deferred snapshot is accepted by a real raft.Peer; a wire-stamped one is the negative control.
func TestStepWorker_DeferredSnapshotSurvivesRedeliveryIntoRaft(t *testing.T) {
	ldb := newTestLogDB()
	lr := logdb.NewLogReader(1, 1, ldb)
	if err := lr.ApplySnapshot(proto.Snapshot{
		Membership: proto.Membership{Addresses: map[uint64]string{1: "addr1", 2: "addr2"}},
	}); err != nil {
		t.Fatalf("ApplySnapshot: %v", err)
	}
	cfg := config.Config{ShardID: 1, ReplicaID: 1}
	cfg.SetDefaults()
	peer, err := raft.NewPeer(cfg, lr, nil)
	if err != nil {
		t.Fatalf("NewPeer failed: %v", err)
	}
	node := NewNode(peer, nil, nil, cfg, nil, "", nil)

	sender := newTestSender()
	ws := NewWorkSignal(1)
	commitC := make(chan *commitItem, 16)
	stopC := make(chan struct{})
	sw := newStepWorker(0, time.Millisecond, sender, ws, commitC, nil, stopC, nil)
	sw.loadNode(node)

	// The snapshot receive path's local message, deferred because commitPending was set.
	snap := proto.Message{
		Type:     proto.InstallSnapshot,
		From:     2,
		To:       1,
		ShardID:  1,
		Snapshot: proto.Snapshot{Index: 500, Term: 10},
	}
	if !node.DeliverLocal(snap) {
		t.Fatal("DeliverLocal was refused")
	}
	drained := node.DrainInbox(nil)
	sw.deferSnapshot(node, 1, drained[0])

	sw.redeliverDeferredSnapshots(1, node)
	if got := len(sw.deferredSnapshots[1]); got != 0 {
		t.Fatalf("deferred buffer still holds %d messages after a successful "+
			"redelivery, so a refusal below would be retried rather than lost", got)
	}

	back := node.DrainInbox(nil)
	if len(back) != 1 {
		t.Fatalf("drained %d messages after redelivery, want 1", len(back))
	}
	if handleErr := peer.Handle(back[0]); errors.Is(handleErr, raft.ErrWireLocalMessage) {
		t.Fatal("raft refused the redelivered InstallSnapshot as a process-local " +
			"message received from the wire: the deferred buffer has already " +
			"dropped it, so the follower's snapshot install is silently lost and " +
			"it never catches up")
	} else if handleErr != nil {
		t.Fatalf("peer.Handle of the redelivered snapshot: %v", handleErr)
	}

	// Negative control: the refusal the assertion above rules out is real, and
	// is reached by exactly the stamp redelivery must not apply.
	forged := snap
	forged.Snapshot.Index = 600
	forged.Origin = proto.OriginRemote
	if handleErr := peer.Handle(forged); !errors.Is(handleErr, raft.ErrWireLocalMessage) {
		t.Fatalf("a wire-stamped InstallSnapshot was accepted (err = %v); the "+
			"assertion above proves nothing if raft does not enforce the origin",
			handleErr)
	}
}
