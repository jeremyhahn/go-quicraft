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
	"sync"
	"testing"

	"github.com/jeremyhahn/go-quicraft/pkg/config"
	"github.com/jeremyhahn/go-quicraft/pkg/internal/engine"
	"github.com/jeremyhahn/go-quicraft/pkg/logdb/memdb"
	"github.com/jeremyhahn/go-quicraft/pkg/proto"
)

// capturingEngineDeliverer wraps a real engineDeliverer and records every
// message delivered to the raft layer, so a test can assert exactly what an
// InstallSnapshot carries.
type capturingEngineDeliverer struct {
	engineDeliverer
	mu        sync.Mutex
	delivered []proto.Message
}

func (c *capturingEngineDeliverer) DeliverMessage(shardID uint64, msg proto.Message) bool {
	c.mu.Lock()
	c.delivered = append(c.delivered, msg)
	c.mu.Unlock()
	return c.engineDeliverer.DeliverMessage(shardID, msg)
}

// DeliverLocal captures the IN-PROCESS injection path, which is the one the
// snapshot receive path uses for InstallSnapshot (deliverSnapshotControl).
func (c *capturingEngineDeliverer) DeliverLocal(shardID uint64, msg proto.Message) bool {
	c.mu.Lock()
	c.delivered = append(c.delivered, msg)
	c.mu.Unlock()
	return c.engineDeliverer.DeliverLocal(shardID, msg)
}

// firstInstallSnapshot returns the first InstallSnapshot message delivered to
// the raft layer, which is the message that seeds the LogReader's snapshot
// record and therefore what this node would stream as leader.
func (c *capturingEngineDeliverer) firstInstallSnapshot() (proto.Message, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, m := range c.delivered {
		if m.Type == proto.InstallSnapshot {
			return m, true
		}
	}
	return proto.Message{}, false
}

func (c *capturingEngineDeliverer) installSnapshotEpoch() (uint64, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, m := range c.delivered {
		if m.Type == proto.InstallSnapshot {
			return m.Snapshot.Epoch, true
		}
	}
	return 0, false
}

// TestHandleSnapshot_DeliversReceiverAtRestEpoch is the CX1 regression test.
// When the receiver has a barrier, the inbound snapshot is re-encrypted at rest
// under that barrier (recvEpoch != 0) and that epoch is persisted to LogDB. The
// InstallSnapshot delivered to raft MUST carry the SAME at-rest epoch, not the
// wire epoch (0). Otherwise the in-memory raft layer advertises the encrypted
// on-disk snapshot as plaintext, and if this node later becomes leader it
// streams ciphertext that followers cannot recover.
func TestHandleSnapshot_DeliversReceiverAtRestEpoch(t *testing.T) {
	nodeHostDir := t.TempDir()
	cfg := config.HostConfig{
		WALDir:                t.TempDir(),
		NodeHostDir:           nodeHostDir,
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

	barrier := createTestBarrier(t)
	h := &Host{cfg: cfg, logdb: ldb, barrier: barrier}
	capEng := &capturingEngineDeliverer{engineDeliverer: eng}
	handler := &hostMessageHandler{engine: capEng, host: h}

	const shardID, localReplicaID, fromReplicaID = uint64(20), uint64(1), uint64(2)
	authorizeSnapshotPath(t, eng, h, shardID, localReplicaID, fromReplicaID)

	chunks := []proto.SnapshotChunk{
		{
			ShardID:    shardID,
			ReplicaID:  localReplicaID,
			From:       fromReplicaID,
			Index:      888,
			Term:       25,
			ChunkID:    0,
			ChunkCount: 1,
			Data:       []byte("plaintext-snapshot-payload-streamed-on-the-wire"),
		},
	}

	if err := handler.HandleSnapshot(chunks, authorizeAllSenders); err != nil {
		t.Fatalf("HandleSnapshot: %v", err)
	}

	wantEpoch := barrier.CurrentEpoch()
	if wantEpoch == 0 {
		t.Fatal("test barrier reported epoch 0; cannot exercise the at-rest epoch path")
	}

	// The LogDB snapshot record carries the receiver's at-rest epoch.
	ss, err := ldb.GetSnapshot(shardID, localReplicaID)
	if err != nil {
		t.Fatalf("GetSnapshot: %v", err)
	}
	if ss.Epoch != wantEpoch {
		t.Fatalf("LogDB snapshot epoch = %d, want %d (receiver at-rest epoch)", ss.Epoch, wantEpoch)
	}

	// The InstallSnapshot delivered to raft MUST carry the SAME at-rest epoch.
	gotEpoch, ok := capEng.installSnapshotEpoch()
	if !ok {
		t.Fatal("no InstallSnapshot was delivered to the engine")
	}
	if gotEpoch != wantEpoch {
		t.Fatalf("InstallSnapshot epoch = %d, want %d (the at-rest epoch persisted to LogDB); "+
			"delivering the wire epoch makes raft advertise the encrypted snapshot as plaintext", gotEpoch, wantEpoch)
	}
}
