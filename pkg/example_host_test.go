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

package quicraft_test

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"time"

	quicraft "github.com/jeremyhahn/go-quicraft/pkg"
	"github.com/jeremyhahn/go-quicraft/pkg/config"
	"github.com/jeremyhahn/go-quicraft/pkg/sm/kv"
)

// exampleHostConfig returns a HostConfig for a single-node host whose
// directories live under dir. The addresses are never dialled: the
// examples start the host WithoutTransport.
func exampleHostConfig(dir string) config.HostConfig {
	return config.HostConfig{
		WALDir:          dir + "/wal",
		NodeHostDir:     dir + "/host",
		RaftAddress:     "127.0.0.1:63100",
		ListenAddress:   "127.0.0.1:63100",
		RTTMillisecond:  1,
		DeploymentID:    1,
		ShutdownTimeout: 500 * time.Millisecond,
	}
}

// waitReadReady polls ShardReadReady until the shard's leader has committed
// an entry of its term, so a linearizable read is served rather than
// refused with ErrReadIndexNotReady.
func waitReadReady(ctx context.Context, h *quicraft.Host, shardID uint64) error {
	for {
		_, _, ready, err := h.ShardReadReady(shardID)
		if err != nil && !errors.Is(err, quicraft.ErrShardNotReady) {
			return err
		}
		if ready {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(5 * time.Millisecond):
		}
	}
}

// startExampleShard creates a single-node host in dir and starts shard 1,
// replica 1, running the in-memory key-value state machine, then waits
// until the shard serves linearizable reads.
func startExampleShard(ctx context.Context, dir string) (*quicraft.Host, error) {
	// A single node needs no transport. WithoutEncryption declares that the
	// log and snapshots are stored in plaintext; NewHost refuses a host that
	// neither configures a barrier nor says this.
	h, err := quicraft.NewHost(exampleHostConfig(dir),
		quicraft.WithoutTransport(), quicraft.WithoutEncryption())
	if err != nil {
		return nil, err
	}
	members := map[uint64]string{1: "127.0.0.1:63100"}
	shardCfg := config.Config{ShardID: 1, ReplicaID: 1}
	if err := h.StartShard(members, false, kv.NewMemoryCreateFunc(), shardCfg); err != nil {
		_ = h.Close()
		return nil, err
	}
	if err := waitReadReady(ctx, h, 1); err != nil {
		_ = h.Close()
		return nil, err
	}
	return h, nil
}

// This example runs the whole Host lifecycle on one node: NewHost,
// StartShard with the reference key-value state machine, a SyncPropose
// that writes a key, a linearizable SyncRead of it, and Close.
func ExampleNewHost() {
	dir, err := os.MkdirTemp("", "quicraft-example-")
	if err != nil {
		log.Fatal(err)
	}
	defer os.RemoveAll(dir)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	h, err := startExampleShard(ctx, dir)
	if err != nil {
		log.Fatal(err)
	}

	cmd, err := kv.EncodePut([]byte("hello"), []byte("world"))
	if err != nil {
		log.Fatal(err)
	}
	if _, err := h.SyncPropose(ctx, 1, cmd); err != nil {
		log.Fatal(err)
	}

	val, err := h.SyncRead(ctx, 1, []byte("hello"))
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("hello = %s\n", val.([]byte))

	if err := h.Close(); err != nil {
		log.Fatal(err)
	}
	// Output:
	// hello = world
}

// This example writes through a client session, which gives at-most-once
// semantics: NewSession registers the session through Raft,
// SyncProposeWithSession applies a command under it and advances its
// series, and CloseSession unregisters it.
func ExampleHost_NewSession() {
	dir, err := os.MkdirTemp("", "quicraft-example-")
	if err != nil {
		log.Fatal(err)
	}
	defer os.RemoveAll(dir)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	h, err := startExampleShard(ctx, dir)
	if err != nil {
		log.Fatal(err)
	}
	defer h.Close()

	session, err := h.NewSession(ctx, 1)
	if err != nil {
		log.Fatal(err)
	}

	cmd, err := kv.EncodePut([]byte("k"), []byte("v"))
	if err != nil {
		log.Fatal(err)
	}
	if _, err := h.SyncProposeWithSession(ctx, session, cmd); err != nil {
		log.Fatal(err)
	}

	rs, err := h.CloseSession(ctx, 1, session)
	if err != nil {
		log.Fatal(err)
	}
	result, err := rs.Result()
	rs.Release()
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("session closed:", result.Value == session.ClientID())

	val, err := h.SyncRead(ctx, 1, []byte("k"))
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("k = %s\n", val.([]byte))
	// Output:
	// session closed: true
	// k = v
}

// This example asks the leader of shard 1 to hand leadership to replica 2.
// A transfer needs a second voting replica that the leader reaches over
// the network, so the example has no Output line: it is compiled, not run.
// The hosts here are assumed to be configured with a transport and a
// barrier, and shard 1 to have been started on both.
func ExampleHost_RequestLeaderTransfer() {
	var h *quicraft.Host // a running host holding replica 1 of shard 1

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := h.RequestLeaderTransfer(ctx, 1, 2); err != nil {
		log.Fatal(err)
	}

	// The transfer completes asynchronously; LeaderID reports the new
	// leader once the target has won its election.
	leaderID, known, err := h.LeaderID(1)
	if err != nil {
		log.Fatal(err)
	}
	if known {
		fmt.Println("leader:", leaderID)
	}
}
