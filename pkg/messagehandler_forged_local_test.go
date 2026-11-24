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
	"testing"
	"time"

	"github.com/jeremyhahn/go-quicraft/pkg/config"
	"github.com/jeremyhahn/go-quicraft/pkg/internal/engine"
	"github.com/jeremyhahn/go-quicraft/pkg/internal/registry"
	"github.com/jeremyhahn/go-quicraft/pkg/logdb/memdb"
	"github.com/jeremyhahn/go-quicraft/pkg/proto"
)

// TestHandleMessage_ForgedProcessLocalTypesAreNotStepped: none of the six term-exempt local message types from a peer changes membership or stops the shard.
func TestHandleMessage_ForgedProcessLocalTypesAreNotStepped(t *testing.T) {
	cfg := config.HostConfig{
		WALDir:                t.TempDir(),
		NodeHostDir:           t.TempDir(),
		RaftAddress:           "127.0.0.1:5000",
		ListenAddress:         "127.0.0.1:5000",
		DeploymentID:          1,
		AllowZeroDeploymentID: false,
	}
	cfg.SetDefaults()

	ldb := memdb.New()
	eng := engine.NewEngine(cfg, ldb, &noopSender{}, nil, nil, nil)
	eng.Start()
	defer eng.Stop()

	h := &Host{cfg: cfg, logdb: ldb, registry: registry.NewRegistry()}
	handler := &hostMessageHandler{engine: eng, host: h}

	const shardID, victim, attacker = uint64(32), uint64(1), uint64(2)
	voters := map[uint64]string{victim: "127.0.0.1:6001", attacker: "127.0.0.1:6002", 3: "127.0.0.1:6003"}
	node := newEngineNodeWithMembership(t, shardID, victim, voters)
	eng.LoadNode(node)
	defer eng.UnloadNode(shardID)

	for _, msgType := range []uint64{
		proto.Propose,
		proto.ReadIndex,
		proto.InstallSnapshot,
		proto.SnapshotStatus,
		proto.Unreachable,
		proto.LeaderTransfer,
	} {
		batch := proto.MessageBatch{
			SourceAddress: "127.0.0.1:6002",
			Requests: []proto.Message{{
				Type:    msgType,
				From:    attacker,
				To:      victim,
				ShardID: shardID,
				Term:    0,
				Hint:    attacker,
				Snapshot: proto.Snapshot{
					Index: 1,
					Term:  1,
					Membership: proto.Membership{
						ConfigChangeID: 1 << 62,
						Addresses:      map[uint64]string{attacker: "attacker"},
					},
				},
			}},
		}
		if err := handler.HandleMessage(batch, authorizeAllSenders); err != nil {
			t.Fatalf("HandleMessage(%s): %v", proto.MessageTypeName(msgType), err)
		}
	}

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if !node.IsMember(3) {
			break
		}
		// Kept: no removal across this window is the behaviour under test.
		time.Sleep(5 * time.Millisecond)
	}
	for _, replicaID := range []uint64{victim, attacker, 3} {
		if !node.IsMember(replicaID) {
			t.Fatalf("replica %d left the membership after a batch of forged "+
				"term-less local-control messages", replicaID)
		}
	}
}

// TestMessageOrigin_IsNotOnTheWire pins the property the whole fix rests on:
// the origin stamp cannot be expressed in the wire format, so a peer cannot
// send one. A marshal/unmarshal round trip of a message stamped OriginLocal
// must come back OriginRemote.
func TestMessageOrigin_IsNotOnTheWire(t *testing.T) {
	src := proto.Message{
		Type:    proto.InstallSnapshot,
		From:    2,
		To:      1,
		ShardID: 7,
		Origin:  proto.OriginLocal,
	}
	buf := make([]byte, src.Size())
	if _, err := src.MarshalTo(buf); err != nil {
		t.Fatalf("MarshalTo: %v", err)
	}

	// Decode into a message that already carries a local stamp; the decoder must clear it.
	dst := proto.Message{Origin: proto.OriginLocal}
	if _, err := dst.UnmarshalFrom(buf); err != nil {
		t.Fatalf("UnmarshalFrom: %v", err)
	}
	if dst.Origin.IsLocal() {
		t.Fatal("a wire-decoded message came back stamped OriginLocal: the " +
			"local-injection exemption in raft.step is reachable from the network")
	}

	// Reset must scrub it too: messages live in a pool and are reused across
	// connections.
	pooled := proto.Message{Origin: proto.OriginLocal}
	pooled.Reset()
	if pooled.Origin.IsLocal() {
		t.Fatal("Reset left OriginLocal on a pooled message")
	}
}
