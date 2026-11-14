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
	"path/filepath"
	"time"

	quicraft "github.com/jeremyhahn/go-quicraft/pkg"
	"github.com/jeremyhahn/go-quicraft/pkg/config"
	"github.com/jeremyhahn/go-quicraft/pkg/sm/kv"
)

// examplePut writes key=value to shardID through Raft.
func examplePut(ctx context.Context, h *quicraft.Host, shardID uint64, key, value string) error {
	cmd, err := kv.EncodePut([]byte(key), []byte(value))
	if err != nil {
		return err
	}
	_, err = h.SyncPropose(ctx, shardID, cmd)
	return err
}

// Example_multipleShards runs two shards on one Host, as the multi-group-host
// guide describes: each shard is its own Raft group with its own state, so
// the same key holds a different value in each.
func Example_multipleShards() {
	dir, err := os.MkdirTemp("", "quicraft-example-")
	if err != nil {
		log.Fatal(err)
	}
	defer os.RemoveAll(dir)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	h, err := quicraft.NewHost(exampleHostConfig(dir),
		quicraft.WithoutTransport(), quicraft.WithoutEncryption())
	if err != nil {
		log.Fatal(err)
	}
	members := map[uint64]string{1: "127.0.0.1:63100"}
	for _, shardID := range []uint64{1, 2} {
		shardCfg := config.Config{ShardID: shardID, ReplicaID: 1}
		if err := h.StartShard(members, false, kv.NewMemoryCreateFunc(), shardCfg); err != nil {
			log.Fatal(err)
		}
		if err := waitReadReady(ctx, h, shardID); err != nil {
			log.Fatal(err)
		}
	}

	if err := examplePut(ctx, h, 1, "tenant", "alpha"); err != nil {
		log.Fatal(err)
	}
	if err := examplePut(ctx, h, 2, "tenant", "beta"); err != nil {
		log.Fatal(err)
	}
	if err := examplePut(ctx, h, 1, "only-in-1", "yes"); err != nil {
		log.Fatal(err)
	}

	for _, shardID := range []uint64{1, 2} {
		val, err := h.SyncRead(ctx, shardID, []byte("tenant"))
		if err != nil {
			log.Fatal(err)
		}
		fmt.Printf("shard %d: tenant = %s\n", shardID, val.([]byte))
	}
	_, err = h.SyncRead(ctx, 2, []byte("only-in-1"))
	var notFound *kv.KeyNotFoundError
	fmt.Println("shard 2 has only-in-1:", !errors.As(err, &notFound))

	if err := h.Close(); err != nil {
		log.Fatal(err)
	}
	// Output:
	// shard 1: tenant = alpha
	// shard 2: tenant = beta
	// shard 2 has only-in-1: false
}

// ExampleHost_ExecuteOnLeader registers a leader handler on a shard and runs
// a request on the shard's leader. The handler's reply is returned to the
// caller; the request does not go through the Raft log.
func ExampleHost_ExecuteOnLeader() {
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

	err = h.RegisterLeaderHandler(1,
		func(_ context.Context, term uint64, req []byte) ([]byte, error) {
			if term == 0 {
				return nil, errors.New("no term")
			}
			return append([]byte("renewed "), req...), nil
		})
	if err != nil {
		log.Fatal(err)
	}

	reply, err := h.ExecuteOnLeader(ctx, 1, []byte("client-7"))
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("reply: %s\n", reply)

	if err := h.Close(); err != nil {
		log.Fatal(err)
	}
	// Output:
	// reply: renewed client-7
}

// ExampleHost_HealthCheck reads a host's aggregate health. A host whose
// shards are running reports "ok"; anything else maps to HTTP 503.
func ExampleHost_HealthCheck() {
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

	status := h.HealthCheck()
	fmt.Println("status:", status.Status)
	fmt.Println("shards:", status.ShardCount)

	if err := h.Close(); err != nil {
		log.Fatal(err)
	}
	// Output:
	// status: ok
	// shards: 1
}

// ExampleHost_ExportSnapshot takes a snapshot of a shard on demand and
// exports it to a file as a self-checking backup artifact.
func ExampleHost_ExportSnapshot() {
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
	if err := examplePut(ctx, h, 1, "hello", "world"); err != nil {
		log.Fatal(err)
	}
	if _, err := h.SyncRequestSnapshot(ctx, 1, quicraft.SnapshotOption{}); err != nil {
		log.Fatal(err)
	}

	path := filepath.Join(dir, "shard-1.snap")
	f, err := os.Create(path)
	if err != nil {
		log.Fatal(err)
	}
	if err := h.ExportSnapshot(ctx, 1, f); err != nil {
		f.Close()
		os.Remove(path)
		log.Fatal(err)
	}
	if err := f.Close(); err != nil {
		log.Fatal(err)
	}

	fi, err := os.Stat(path)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("exported:", fi.Size() > 0)

	if err := h.Close(); err != nil {
		log.Fatal(err)
	}
	// Output:
	// exported: true
}
