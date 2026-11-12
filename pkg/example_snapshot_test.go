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

// The examples in this file are the flows docs/snapshots/user.md shows:
// taking a snapshot on demand, exporting one to a directory and to a stream,
// and importing a stream into another host. Each ends with an Output block,
// so go test checks that the guide's calls do what the guide says.

import (
	"bytes"
	"context"
	"fmt"
	"io/fs"
	"log"
	"os"
	"path/filepath"
	"time"

	quicraft "github.com/jeremyhahn/go-quicraft/pkg"
	"github.com/jeremyhahn/go-quicraft/pkg/config"
	"github.com/jeremyhahn/go-quicraft/pkg/sm/kv"
)

// putExampleKeys writes each key with its value to shard 1 of h.
func putExampleKeys(ctx context.Context, h *quicraft.Host, kvs ...string) error {
	for i := 0; i+1 < len(kvs); i += 2 {
		cmd, err := kv.EncodePut([]byte(kvs[i]), []byte(kvs[i+1]))
		if err != nil {
			return err
		}
		if _, err := h.SyncPropose(ctx, 1, cmd); err != nil {
			return err
		}
	}
	return nil
}

// hasRegularFile reports whether the tree under root holds a non-empty file.
func hasRegularFile(root string) bool {
	found := false
	_ = filepath.WalkDir(root, func(_ string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return nil
		}
		if info, err := d.Info(); err == nil && info.Size() > 0 {
			found = true
		}
		return nil
	})
	return found
}

// ExampleHost_SyncRequestSnapshot takes a snapshot on demand, as the guide's
// "Take a snapshot on demand" section does, and gets back its index.
func ExampleHost_SyncRequestSnapshot() {
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

	if err := putExampleKeys(ctx, h, "a", "1", "b", "2", "c", "3"); err != nil {
		log.Fatal(err)
	}

	index, err := h.SyncRequestSnapshot(ctx, 1, quicraft.DefaultSnapshotOption)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("snapshot index is non-zero:", index > 0)
	// Output:
	// snapshot index is non-zero: true
}

// ExampleSnapshotOption_exportPath exports a snapshot to a directory as part
// of taking it, as the guide's "Export a snapshot" section does. The
// directory does not exist beforehand; the export creates it.
func ExampleSnapshotOption_exportPath() {
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

	if err := putExampleKeys(ctx, h, "k", "v"); err != nil {
		log.Fatal(err)
	}

	exportDir := filepath.Join(dir, "backups", "shard-1") // absolute
	opt := quicraft.SnapshotOption{ExportPath: exportDir}
	index, err := h.SyncRequestSnapshot(ctx, 1, opt)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("snapshot index is non-zero:", index > 0)
	fmt.Println("exported file exists:", hasRegularFile(exportDir))
	// Output:
	// snapshot index is non-zero: true
	// exported file exists: true
}

// ExampleHost_ImportSnapshot exports the latest snapshot to a stream and
// imports it into a fresh Host, as the guide's "Export a snapshot" and
// "Import a snapshot" sections do. The import is offline: the receiving
// shard is stopped while it runs and restarted afterwards.
func ExampleHost_ImportSnapshot() {
	srcDir, err := os.MkdirTemp("", "quicraft-example-")
	if err != nil {
		log.Fatal(err)
	}
	defer os.RemoveAll(srcDir)
	dstDir, err := os.MkdirTemp("", "quicraft-example-")
	if err != nil {
		log.Fatal(err)
	}
	defer os.RemoveAll(dstDir)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// The source host writes a key, snapshots it and exports the snapshot.
	src, err := startExampleShard(ctx, srcDir)
	if err != nil {
		log.Fatal(err)
	}
	if err := putExampleKeys(ctx, src, "k", "v"); err != nil {
		log.Fatal(err)
	}
	if _, err := src.SyncRequestSnapshot(ctx, 1, quicraft.DefaultSnapshotOption); err != nil {
		log.Fatal(err)
	}
	var artifact bytes.Buffer
	if err := src.ExportSnapshot(ctx, 1, &artifact); err != nil {
		log.Fatal(err)
	}
	if err := src.Close(); err != nil {
		log.Fatal(err)
	}

	// The fresh host has the shard's replica, stopped, and none of its data.
	dst, err := startExampleShard(ctx, dstDir)
	if err != nil {
		log.Fatal(err)
	}
	defer dst.Close()
	if err := dst.StopShard(1); err != nil {
		log.Fatal(err)
	}

	// The shard has no DataDir, so the snapshot tree is under NodeHostDir.
	if err := dst.ImportSnapshot(ctx, 1, &artifact, quicraft.WithImportDataDir("")); err != nil {
		log.Fatal(err)
	}

	members := map[uint64]string{1: "127.0.0.1:63100"}
	shardCfg := config.Config{ShardID: 1, ReplicaID: 1}
	if err := dst.StartShard(members, false, kv.NewMemoryCreateFunc(), shardCfg); err != nil {
		log.Fatal(err)
	}
	if err := waitReadReady(ctx, dst, 1); err != nil {
		log.Fatal(err)
	}
	val, err := dst.SyncRead(ctx, 1, []byte("k"))
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("k = %s\n", val.([]byte))
	// Output:
	// k = v
}
