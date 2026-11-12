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

package dragonboat_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	dragonboat "github.com/jeremyhahn/go-quicraft/contrib/dragonboat"
	"github.com/jeremyhahn/go-quicraft/contrib/dragonboat/config"
	sm "github.com/jeremyhahn/go-quicraft/contrib/dragonboat/statemachine"
	quicraft "github.com/jeremyhahn/go-quicraft/pkg"
)

// kvStore is a Dragonboat-style regular state machine: a string map
// updated by "key=value" commands and read by key. It is written exactly
// as it would be for Dragonboat v4; only the import paths differ.
type kvStore struct {
	mu   sync.RWMutex
	data map[string]string
}

func newKVStore(shardID, replicaID uint64) sm.IStateMachine {
	return &kvStore{data: make(map[string]string)}
}

func (s *kvStore) Update(e sm.Entry) (sm.Result, error) {
	key, value, ok := strings.Cut(string(e.Cmd), "=")
	if !ok {
		return sm.Result{}, fmt.Errorf("malformed command %q", e.Cmd)
	}
	s.mu.Lock()
	s.data[key] = value
	n := len(s.data)
	s.mu.Unlock()
	return sm.Result{Value: uint64(n)}, nil
}

func (s *kvStore) Lookup(query interface{}) (interface{}, error) {
	key, ok := query.(string)
	if !ok {
		return nil, errors.New("query must be a string key")
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.data[key], nil
}

func (s *kvStore) SaveSnapshot(w io.Writer, _ sm.ISnapshotFileCollection, _ <-chan struct{}) error {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return json.NewEncoder(w).Encode(s.data)
}

func (s *kvStore) RecoverFromSnapshot(r io.Reader, _ []sm.SnapshotFile, _ <-chan struct{}) error {
	data := make(map[string]string)
	if err := json.NewDecoder(r).Decode(&data); err != nil {
		return err
	}
	s.mu.Lock()
	s.data = data
	s.mu.Unlock()
	return nil
}

func (s *kvStore) Close() error { return nil }

// ExampleNewNodeHost shows the construction a Dragonboat user already
// writes: a config.NodeHostConfig and NewNodeHost. As in Dragonboat, the
// Raft log and snapshots are stored as plaintext. On a host with a
// transport, name each founding member's identity (its NodeIdentity) in
// config.Config.MemberIdentities when the shard starts.
func ExampleNewNodeHost() {
	dir, err := os.MkdirTemp("", "nodehost")
	if err != nil {
		log.Fatal(err)
	}
	defer os.RemoveAll(dir)

	nh, err := dragonboat.NewNodeHost(config.NodeHostConfig{
		DeploymentID:   1,
		WALDir:         filepath.Join(dir, "wal"),
		NodeHostDir:    filepath.Join(dir, "data"),
		RaftAddress:    "localhost:63001",
		ListenAddress:  "localhost:63001",
		RTTMillisecond: 200,
		// The transport requires mutual TLS. The CA bundle, this host's
		// certificate and its private key are PEM files the caller
		// provides; the certificate's public key is the host's identity.
		MutualTLS: true,
		CAFile:    "/etc/myapp/tls/ca.pem",
		CertFile:  "/etc/myapp/tls/host.pem",
		KeyFile:   "/etc/myapp/tls/host-key.pem",
	})
	if err != nil {
		log.Fatal(err)
	}
	defer nh.Close()

	fmt.Println("raft address:", nh.RaftAddress())
	fmt.Println("node identity:", nh.NodeIdentity())
}

// ExampleNodeHost_SyncPropose starts a single-replica shard with a
// Dragonboat-style state machine, writes through SyncPropose with a no-op
// session, and reads back with SyncRead. The host runs without a transport
// and with an in-memory log, through NewNodeHostWithOptions, so the
// example is self-contained; a migrated application uses NewNodeHost, or
// NewNodeHostWithOptions with quicraft.WithBarrier for encryption at rest.
func ExampleNodeHost_SyncPropose() {
	dir, err := os.MkdirTemp("", "nodehost")
	if err != nil {
		log.Fatal(err)
	}
	defer os.RemoveAll(dir)

	nh, err := dragonboat.NewNodeHostWithOptions(config.NodeHostConfig{
		DeploymentID:   1,
		WALDir:         filepath.Join(dir, "wal"),
		NodeHostDir:    filepath.Join(dir, "data"),
		RaftAddress:    "localhost:63001",
		ListenAddress:  "localhost:63001",
		RTTMillisecond: 1,
	},
		quicraft.WithMemoryLogDB(),
		quicraft.WithoutTransport(),
		quicraft.WithoutEncryption(),
	)
	if err != nil {
		log.Fatal(err)
	}
	defer nh.Close()

	const shardID, replicaID = 1, 1
	members := map[uint64]dragonboat.Target{replicaID: nh.RaftAddress()}
	if err := nh.StartReplica(members, false, newKVStore, config.Config{
		ShardID:      shardID,
		ReplicaID:    replicaID,
		ElectionRTT:  5,
		HeartbeatRTT: 1,
		CheckQuorum:  true,
	}); err != nil {
		log.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Until the shard has a leader, proposals fail with a temporary
	// error; retry those, as a Dragonboat application does.
	session := nh.GetNoOPSession(shardID)
	propose := func(cmd string) sm.Result {
		for {
			attempt, cancelAttempt := context.WithTimeout(ctx, time.Second)
			result, err := nh.SyncPropose(attempt, session, []byte(cmd))
			cancelAttempt()
			if err == nil {
				return result
			}
			if !dragonboat.IsTempError(err) || ctx.Err() != nil {
				log.Fatal(err)
			}
			time.Sleep(10 * time.Millisecond)
		}
	}

	// A malformed command is rejected in its result; the shard carries on.
	rejected := propose("garbage")
	fmt.Println("garbage rejected:", string(rejected.Data))

	result := propose("greeting=hello")
	fmt.Println("keys after update:", result.Value)

	var value interface{}
	for {
		attempt, cancelAttempt := context.WithTimeout(ctx, time.Second)
		value, err = nh.SyncRead(attempt, shardID, "greeting")
		cancelAttempt()
		if err == nil {
			break
		}
		if !dragonboat.IsTempError(err) || ctx.Err() != nil {
			log.Fatal(err)
		}
		time.Sleep(10 * time.Millisecond)
	}
	fmt.Println("greeting:", value)

	// Output:
	// garbage rejected: malformed command
	// keys after update: 1
	// greeting: hello
}

func TestKVStore_MalformedCommandThenValid(t *testing.T) {
	s := newKVStore(1, 1).(*kvStore)

	result, err := s.Update(sm.Entry{Index: 1, Cmd: []byte("garbage")})
	if err != nil {
		t.Fatalf("Update(garbage) error = %v, want nil (a terminal apply failure)", err)
	}
	if got := string(result.Data); got != "malformed command" {
		t.Fatalf("Update(garbage) result data = %q, want %q", got, "malformed command")
	}
	if len(s.data) != 0 {
		t.Fatalf("map after garbage = %v, want empty", s.data)
	}

	result, err = s.Update(sm.Entry{Index: 2, Cmd: []byte("k=v")})
	if err != nil {
		t.Fatalf("Update(k=v) error = %v", err)
	}
	if result.Value != 1 || s.data["k"] != "v" {
		t.Fatalf("after k=v: result %d, map %v; want 1, map[k:v]", result.Value, s.data)
	}
}
