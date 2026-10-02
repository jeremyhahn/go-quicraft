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
	"testing"

	"github.com/jeremyhahn/go-quicraft/pkg/config"
	"github.com/jeremyhahn/go-quicraft/pkg/internal/raft"
	"github.com/jeremyhahn/go-quicraft/pkg/logdb"
	"github.com/jeremyhahn/go-quicraft/pkg/proto"
)

// TestNewNode_SeedsLogMarkersFromRecoveredLog pins that a node built over a
// recovered log publishes its committed marker before any tick, so a proposal
// registered the instant StartShard returns records a real committed-index
// floor rather than 0.
func TestNewNode_SeedsLogMarkersFromRecoveredLog(t *testing.T) {
	const shardID, replicaID = 7, 1
	lr := logdb.NewLogReader(shardID, replicaID, newTestLogDB())
	if err := lr.ApplySnapshot(proto.Snapshot{
		Index:      0,
		Term:       0,
		Membership: proto.Membership{Addresses: map[uint64]string{replicaID: "localhost:0"}},
	}); err != nil {
		t.Fatalf("ApplySnapshot: %v", err)
	}
	// A recovered log of three entries at term 1 with HardState.Commit = 3,
	// exactly what a restart over a WAL that had committed them presents.
	entries := []proto.Entry{
		{Index: 1, Term: 1, Type: proto.EntryNormal},
		{Index: 2, Term: 1, Type: proto.EntryNormal},
		{Index: 3, Term: 1, Type: proto.EntryNormal},
	}
	if err := lr.Append(entries); err != nil {
		t.Fatalf("Append: %v", err)
	}
	lr.SetState(proto.State{Term: 1, Vote: replicaID, Commit: 3})

	cfg := config.Config{ShardID: shardID, ReplicaID: replicaID}
	cfg.SetDefaults()
	peer, err := raft.NewPeer(cfg, lr, nil)
	if err != nil {
		t.Fatalf("NewPeer: %v", err)
	}
	node := NewNode(peer, nil, lr, cfg, nil, "", nil)

	if got := node.CommittedIndex(); got != 3 {
		t.Fatalf("CommittedIndex before the first tick = %d, want 3 (marker not seeded from the recovered log)", got)
	}
	if committed, _ := node.LogMarkers(); committed != 3 {
		t.Fatalf("LogMarkers committed = %d, want 3", committed)
	}
}
