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
	"sync"
	"testing"

	"github.com/jeremyhahn/go-quicraft/pkg/internal/raft"
	"github.com/jeremyhahn/go-quicraft/pkg/proto"
)

// installSnapshotMessage builds the InstallSnapshot a leader sends to bring a
// lagging replica forward.
func installSnapshotMessage(node *Node, index, term uint64) proto.Message {
	return proto.Message{
		Type: proto.InstallSnapshot, Origin: proto.OriginLocal,
		From:    2,
		To:      node.ReplicaID(),
		ShardID: node.ShardID(),
		Term:    term,
		Snapshot: proto.Snapshot{
			Index: index,
			Term:  term,
			Membership: proto.Membership{Addresses: map[uint64]string{
				node.ReplicaID(): "127.0.0.1:0",
				2:                "127.0.0.1:1",
			}},
		},
	}
}

// TestPeerLogMarkers_ReadsPublishedMarkersNeverThePeer pins that peerLogMarkers reports the step worker's published markers, not the Peer.
func TestPeerLogMarkers_ReadsPublishedMarkersNeverThePeer(t *testing.T) {
	node, peer := newRecoveryTestNode(t, 1, 1, newSnapshotTestSnapshotter(0))

	// Step the InstallSnapshot into the raft log exactly as the step worker's
	// inbox drain does — and, unlike the step worker, publish nothing.
	if err := peer.Handle(installSnapshotMessage(node, 100, 7)); err != nil {
		t.Fatalf("install snapshot into raft log: %v", err)
	}
	if committed := peer.CommittedIndex(); committed != 100 {
		t.Fatalf("raft log committed = %d, want 100: the install did not land", committed)
	}

	committed, processed, ok := peerLogMarkers(node)
	if !ok {
		t.Fatal("peerLogMarkers reported no peer for a node that has one")
	}
	if committed != 0 || processed != 0 {
		t.Fatalf("peerLogMarkers = (committed %d, processed %d), want (0, 0): "+
			"it read the Peer instead of the markers the step worker published",
			committed, processed)
	}

	// Once the owner publishes, the same call reports the install.
	node.publishLogMarkers()
	committed, processed, _ = peerLogMarkers(node)
	if committed != 100 || processed != 100 {
		t.Fatalf("after publish peerLogMarkers = (committed %d, processed %d), want (100, 100)",
			committed, processed)
	}
}

// TestPeerLogMarkers_DoesNotRaceWithInboxHandling runs inbox InstallSnapshot handling and marker reads concurrently; run with -race.
func TestPeerLogMarkers_DoesNotRaceWithInboxHandling(t *testing.T) {
	node, peer := newRecoveryTestNode(t, 1, 1, newSnapshotTestSnapshotter(0))

	const installs = 400
	const target uint64 = installs * 10

	stepDone := make(chan struct{})
	var wg sync.WaitGroup

	// The step worker: drainAndHandleInbox holds no lock, then publishes.
	wg.Add(1)
	go func() {
		defer wg.Done()
		defer close(stepDone)
		for i := uint64(1); i <= installs; i++ {
			stepInstall(t, node, peer, i*10, 7)
		}
	}()

	// The snapshot pool: awaitRecoverableLog polls the markers until the log
	// holds the snapshot.
	req := snapshotRequest{shardID: 1, replicaID: 1, node: node}
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stepDone:
				return
			default:
			}
			_ = recoveryBlockedByLog(req, target)
		}
	}()

	wg.Wait()

	if committed, _ := node.LogMarkers(); committed != target {
		t.Fatalf("published committed marker = %d, want %d", committed, target)
	}
}

// stepInstall performs one step-worker cycle for an InstallSnapshot: handle it
// off the inbox (no lock — that is what drainAndHandleInbox does), drain the
// resulting update so the next snapshot is not shadowed by a pending one, and
// publish the log markers.
func stepInstall(t *testing.T, node *Node, peer *raft.Peer, index, term uint64) {
	t.Helper()
	if err := peer.Handle(installSnapshotMessage(node, index, term)); err != nil {
		t.Errorf("handle InstallSnapshot at index %d: %v", index, err)
		return
	}
	lastApplied := node.LastApplied()
	if peer.HasUpdate(lastApplied) {
		update, err := peer.GetUpdate(true, lastApplied)
		if err != nil {
			t.Errorf("GetUpdate after install at index %d: %v", index, err)
			return
		}
		if err := peer.Commit(update); err != nil {
			t.Errorf("Commit after install at index %d: %v", index, err)
			return
		}
	}
	node.publishLogMarkers()
}
