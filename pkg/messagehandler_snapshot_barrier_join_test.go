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
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/jeremyhahn/go-quicraft/pkg/config"
	"github.com/jeremyhahn/go-quicraft/pkg/internal/engine"
	"github.com/jeremyhahn/go-quicraft/pkg/internal/raft"
	"github.com/jeremyhahn/go-quicraft/pkg/internal/registry"
	"github.com/jeremyhahn/go-quicraft/pkg/logdb"
	"github.com/jeremyhahn/go-quicraft/pkg/logdb/memdb"
	"github.com/jeremyhahn/go-quicraft/pkg/proto"
)

// snapshotReceiveFixture wires the minimum a HandleSnapshot call needs: a
// running engine, a LogDB, and a Host carrying the barrier under test.
type snapshotReceiveFixture struct {
	handler *hostMessageHandler
	host    *Host
	engine  *engine.Engine
	logdb   logdb.LogDB
}

func newSnapshotReceiveFixture(t *testing.T, barrier Barrier) *snapshotReceiveFixture {
	t.Helper()
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
	t.Cleanup(eng.Stop)

	h := &Host{cfg: cfg, logdb: ldb, barrier: barrier, registry: registry.NewRegistry()}
	return &snapshotReceiveFixture{
		handler: &hostMessageHandler{engine: eng, host: h},
		host:    h,
		engine:  eng,
		logdb:   ldb,
	}
}

// snapshotChunkFor builds a single-chunk snapshot as the leader streams it:
// plaintext data (the sender decrypts before streaming) at Epoch 0.
func snapshotChunkFor(shardID, replicaID, from, index, term uint64,
	addrs map[uint64]string, data []byte) []proto.SnapshotChunk {
	return []proto.SnapshotChunk{{
		ShardID:    shardID,
		ReplicaID:  replicaID,
		From:       from,
		Index:      index,
		Term:       term,
		ChunkID:    0,
		ChunkCount: 1,
		Data:       data,
		Membership: proto.Membership{ConfigChangeID: 1, Addresses: addrs},
	}}
}

// TestHandleSnapshot_SealedBarrierFailsClosed pins the receive half of the
// plaintext-downgrade defect. The bytes on the wire are plaintext, so a
// receiver that treats a SEALED barrier the same as an ABSENT one persists
// them unencrypted at rest — durable, restart-surviving, exportable plaintext
// for a shard configured to be encrypted, created at the exact moment an
// operator sealed the host. The nil-barrier case is asserted alongside it: a
// plaintext deployment must keep working.
func TestHandleSnapshot_SealedBarrierFailsClosed(t *testing.T) {
	const payload = "received-snapshot-payload"

	t.Run("sealed_barrier_fails_closed", func(t *testing.T) {
		barrier := createTestBarrier(t)
		f := newSnapshotReceiveFixture(t, barrier)
		authorizeSnapshotPath(t, f.engine, f.host, 1, 2, 1)
		if err := barrier.Seal(); err != nil {
			t.Fatalf("Seal: %v", err)
		}

		chunks := snapshotChunkFor(1, 2, 1, 500, 10,
			map[uint64]string{1: "127.0.0.1:6002", 2: "127.0.0.1:6001"}, []byte(payload))

		// Errorf, not Fatalf: the persistence assertions below are the
		// substance of the finding and must be reported even when the
		// receive wrongly succeeds.
		err := f.handler.HandleSnapshot(chunks, authorizeAllSenders)
		if !errors.Is(err, ErrSnapshotBarrierSealed) {
			t.Errorf("HandleSnapshot error = %v, want ErrSnapshotBarrierSealed", err)
		}
		if _, getErr := f.logdb.GetSnapshot(1, 2); getErr == nil {
			t.Error("a snapshot was recorded in LogDB while the barrier was sealed")
		}
		snapshotDir := f.engine.GetNode(1).SnapshotSaveDir(500)
		if data, readErr := os.ReadFile(filepath.Join(snapshotDir, "snapshot.dat")); readErr == nil {
			t.Errorf("plaintext snapshot artifact left on disk: %q", data)
		}
	})

	t.Run("nil_barrier_stays_plaintext", func(t *testing.T) {
		f := newSnapshotReceiveFixture(t, nil)
		authorizeSnapshotPath(t, f.engine, f.host, 1, 2, 1)

		chunks := snapshotChunkFor(1, 2, 1, 500, 10,
			map[uint64]string{1: "127.0.0.1:6002", 2: "127.0.0.1:6001"}, []byte(payload))

		if err := f.handler.HandleSnapshot(chunks, authorizeAllSenders); err != nil {
			t.Fatalf("HandleSnapshot with encryption disabled: %v", err)
		}
		snap, getErr := f.logdb.GetSnapshot(1, 2)
		if getErr != nil {
			t.Fatalf("GetSnapshot: %v", getErr)
		}
		if snap.Epoch != 0 {
			t.Errorf("recorded epoch = %d, want 0 for a shard with no barrier", snap.Epoch)
		}
		data, readErr := os.ReadFile(filepath.Join(snap.Filepath, "snapshot.dat"))
		if readErr != nil {
			t.Fatalf("read snapshot artifact: %v", readErr)
		}
		if !bytes.Equal(data, []byte(payload)) {
			t.Errorf("artifact = %q, want the plaintext payload %q", data, payload)
		}
	})

	t.Run("unsealed_barrier_encrypts", func(t *testing.T) {
		barrier := createTestBarrier(t)
		f := newSnapshotReceiveFixture(t, barrier)
		authorizeSnapshotPath(t, f.engine, f.host, 1, 2, 1)

		chunks := snapshotChunkFor(1, 2, 1, 500, 10,
			map[uint64]string{1: "127.0.0.1:6002", 2: "127.0.0.1:6001"}, []byte(payload))

		if err := f.handler.HandleSnapshot(chunks, authorizeAllSenders); err != nil {
			t.Fatalf("HandleSnapshot with a live barrier: %v", err)
		}
		snap, getErr := f.logdb.GetSnapshot(1, 2)
		if getErr != nil {
			t.Fatalf("GetSnapshot: %v", getErr)
		}
		if snap.Epoch == 0 {
			t.Error("recorded epoch = 0; the received snapshot was not re-encrypted at rest")
		}
		data, readErr := os.ReadFile(filepath.Join(snap.Filepath, "snapshot.dat"))
		if readErr != nil {
			t.Fatalf("read snapshot artifact: %v", readErr)
		}
		if bytes.Contains(data, []byte(payload)) {
			t.Error("snapshot artifact still contains the plaintext payload")
		}
	})
}

// newJoiningEngineNode builds the node a native StartShard(join=true) with
// empty initial members produces: no membership, no registered addresses.
func newJoiningEngineNode(t *testing.T, shardID, replicaID uint64) *engine.Node {
	t.Helper()
	lr := logdb.NewLogReader(shardID, replicaID, memdb.New())
	lr.ApplySnapshot(proto.Snapshot{Membership: proto.Membership{}})
	cfg := config.Config{ShardID: shardID, ReplicaID: replicaID}
	cfg.SetDefaults()
	peer, err := raft.NewPeer(cfg, lr, nil)
	if err != nil {
		t.Fatalf("NewPeer: %v", err)
	}
	peer.MarkJoining()
	return engine.NewNode(peer, nil, lr, cfg, 100, nil, t.TempDir(), nil)
}

// TestHandleSnapshot_JoiningShardAcceptsBootstrapSnapshot pins the join
// bootstrap window. A shard started with join=true and empty initial members
// registers no addresses, so requiring a registry entry for the SENDER rejects
// the leader's bootstrap snapshot — the only message that can deliver the
// membership — and blocks catch-up permanently.
//
// What the fix authorizes, and nothing more: for a shard the Host itself
// marked joining, the sender's address may come from the membership the
// snapshot carries INSTEAD of the registry, and it must still be covered by
// the connection's verified peer certificate. The window closes on the first
// accepted snapshot.
func TestHandleSnapshot_JoiningShardAcceptsBootstrapSnapshot(t *testing.T) {
	const leaderAddr = "127.0.0.1:7001"

	f := newSnapshotReceiveFixture(t, nil)
	node := newJoiningEngineNode(t, 1, 2)
	f.engine.LoadNode(node)
	t.Cleanup(func() { f.engine.UnloadNode(1) })
	f.host.markShardJoining(1)

	addrs := map[uint64]string{1: leaderAddr, 2: "127.0.0.1:7002"}
	chunks := snapshotChunkFor(1, 2, 1, 500, 10, addrs, []byte("bootstrap-membership"))

	// Only the address the snapshot claims for the sender is covered by the
	// connection certificate: authorization stays bound to the QUIC peer.
	certCovers := certCovering(leaderAddr)

	if err := f.handler.HandleSnapshot(chunks, certCovers); err != nil {
		t.Fatalf("joining replica rejected its own bootstrap snapshot: %v", err)
	}
	if _, getErr := f.logdb.GetSnapshot(1, 2); getErr != nil {
		t.Fatalf("bootstrap snapshot was not persisted: %v", getErr)
	}
	if addr, resolveErr := f.host.registry.Resolve(1, 1); resolveErr != nil || addr != leaderAddr {
		t.Errorf("registry.Resolve(1, 1) = (%q, %v), want the learned leader address", addr, resolveErr)
	}
	if f.host.isShardJoining(1) {
		t.Error("the join window must close once a snapshot has been accepted")
	}
}

// TestHandleSnapshot_NonJoiningShardRequiresCommittedMembership pins the
// negative: taking the sender's address from the snapshot's OWN membership is
// reachable ONLY for a shard the Host marked joining and that raft also
// considers incomplete. On a normally started shard, a sender the COMMITTED
// membership does not name is rejected even though the snapshot claims an
// address the connection's certificate covers — otherwise every established
// replica would accept a state-machine replacement from any certificate holder
// willing to name itself in the membership it ships.
func TestHandleSnapshot_NonJoiningShardRequiresCommittedMembership(t *testing.T) {
	const leaderAddr, strangerAddr = "127.0.0.1:7001", "127.0.0.1:7009"

	f := newSnapshotReceiveFixture(t, nil)
	node := newEngineNodeWithMembership(t, 1, 2, map[uint64]string{
		1: leaderAddr,
		2: "127.0.0.1:7002",
	})
	f.engine.LoadNode(node)
	t.Cleanup(func() { f.engine.UnloadNode(1) })
	// Replica 3 is not in the committed configuration, and there is no join
	// marker, so nothing can vouch for the address it claims for itself.
	if _, ok := node.MemberBinding(3); ok {
		t.Fatal("fixture: replica 3 must not be in the committed configuration")
	}

	chunks := snapshotChunkFor(1, 2, 3, 500, 10,
		map[uint64]string{2: "127.0.0.1:7002", 3: strangerAddr}, []byte("data"))

	err := f.handler.HandleSnapshot(chunks, certCovering(strangerAddr))
	var authErr *UnauthorizedMessageError
	if !errors.As(err, &authErr) {
		t.Fatalf("HandleSnapshot error = %T (%v), want *UnauthorizedMessageError", err, err)
	}
}
