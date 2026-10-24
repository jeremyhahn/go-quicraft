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
	"bytes"
	"context"
	"testing"

	"github.com/jeremyhahn/go-quicraft/pkg/logdb"
	"github.com/jeremyhahn/go-quicraft/pkg/logdb/memdb"
	"github.com/jeremyhahn/go-quicraft/pkg/proto"
)

// assertRouted reports whether (shardID, replicaID) resolves in the host's
// address registry, and to which address.
func assertRouted(t *testing.T, h *Host, shardID, replicaID uint64, want string, routed bool) {
	t.Helper()
	addr, err := h.registry.Resolve(shardID, replicaID)
	switch {
	case routed && err != nil:
		t.Errorf("replica %d is not routed after the restart: %v", replicaID, err)
	case routed && addr != want:
		t.Errorf("replica %d routes to %q, want %q", replicaID, addr, want)
	case !routed && err == nil:
		t.Errorf("replica %d is routed to %q after the restart, but the recovered "+
			"membership does not list it", replicaID, addr)
	}
}

// TestStartShard_RestartDoesNotRouteCallerOnlyReplicas verifies that on
// restart a replica ID the caller's members map lists, but the recovered
// membership does not, is not routed, while the recovered members are.
func TestStartShard_RestartDoesNotRouteCallerOnlyReplicas(t *testing.T) {
	const shardID, replicaID uint64 = 71, 1
	const extraAddr = "127.0.0.1:5004"
	members := map[uint64]string{
		1: "127.0.0.1:5001",
		2: "127.0.0.1:5002",
		3: "127.0.0.1:5003",
	}

	db := memdb.New()
	h, err := NewHost(testHostConfig(t), WithLogDB(db), WithoutTransport(), WithoutEncryption())
	if err != nil {
		t.Fatalf("NewHost failed: %v", err)
	}
	defer h.Close()

	scfg := testShardConfig(shardID, replicaID)
	if startErr := h.StartShard(members, false, testCreateFunc, scfg); startErr != nil {
		t.Fatalf("first StartShard failed: %v", startErr)
	}
	if stopErr := h.StopShard(shardID); stopErr != nil {
		t.Fatalf("StopShard failed: %v", stopErr)
	}

	restart := map[uint64]string{4: extraAddr}
	for id, addr := range members {
		restart[id] = addr
	}
	if startErr := h.StartShard(restart, false, testCreateFunc, scfg); startErr != nil {
		t.Fatalf("restart StartShard failed: %v", startErr)
	}
	defer func() { _ = h.StopShard(shardID) }()

	for id, addr := range members {
		assertRouted(t, h, shardID, id, addr, true)
	}
	assertRouted(t, h, shardID, 4, extraAddr, false)
}

// TestStartShard_RestartDoesNotRouteRemovedReplicas verifies that on restart a
// replica the recovered membership records as removed is not routed, even when
// the caller's members map still lists it.
func TestStartShard_RestartDoesNotRouteRemovedReplicas(t *testing.T) {
	const shardID, replicaID uint64 = 72, 1
	const removedAddr = "127.0.0.1:5005"
	payload := []byte("restart routing state")
	recovered := map[uint64]string{
		1: "127.0.0.1:5001",
		2: "127.0.0.1:5002",
		3: "127.0.0.1:5003",
	}

	db := memdb.New()
	h, err := NewHost(testHostConfig(t), WithLogDB(db), WithoutTransport(), WithoutEncryption())
	if err != nil {
		t.Fatalf("NewHost failed: %v", err)
	}
	defer h.Close()

	if bsErr := db.SaveBootstrap(shardID, replicaID, logdb.Bootstrap{
		Addresses: recovered,
	}); bsErr != nil {
		t.Fatalf("SaveBootstrap failed: %v", bsErr)
	}
	memb := proto.Membership{
		ConfigChangeID: 6,
		Addresses:      recovered,
		Removed:        map[uint64]bool{5: true},
	}
	artifact := buildSnapshotArtifact(t, 900, 5, 0, memb, snapshotEnvelope(payload))
	if impErr := h.ImportSnapshot(context.Background(), shardID,
		bytes.NewReader(artifact)); impErr != nil {
		t.Fatalf("seeding the recovered snapshot: %v", impErr)
	}

	members := map[uint64]string{5: removedAddr}
	for id, addr := range recovered {
		members[id] = addr
	}
	memSM := &recordingMemSM{want: payload}
	create := func(_, _ uint64) interface{} { return memSM }
	if startErr := h.StartShard(members, false, create, testShardConfig(shardID, replicaID)); startErr != nil {
		t.Fatalf("restart StartShard failed: %v", startErr)
	}
	defer func() { _ = h.StopShard(shardID) }()
	if !memSM.recoverCalled.Load() {
		t.Fatal("the recovered snapshot was not applied, so this test is not exercising a restart")
	}

	for id, addr := range recovered {
		assertRouted(t, h, shardID, id, addr, true)
	}
	assertRouted(t, h, shardID, 5, removedAddr, false)
}
