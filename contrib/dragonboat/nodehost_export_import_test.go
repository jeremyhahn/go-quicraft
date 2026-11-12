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

package dragonboat

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	quicraft "github.com/jeremyhahn/go-quicraft/pkg"
)

const exportImportShard = uint64(1)

// readShard returns the testStateMachine's value through a linearizable read.
func readShard(t *testing.T, nh *NodeHost, shardID uint64) []byte {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	v, err := nh.SyncRead(ctx, shardID, nil)
	if err != nil {
		t.Fatalf("SyncRead: %v", err)
	}
	b, ok := v.([]byte)
	if !ok {
		t.Fatalf("SyncRead returned %T, want []byte", v)
	}
	return b
}

// stopShard stops the replica and fails the test on error.
func stopShard(t *testing.T, nh *NodeHost, shardID, replicaID uint64) {
	t.Helper()
	if err := nh.StopReplica(shardID, replicaID); err != nil {
		t.Fatalf("StopReplica: %v", err)
	}
}

// importNoPanic runs ImportSnapshot and turns a panic into a test failure.
func importNoPanic(t *testing.T, nh *NodeHost, shardID uint64, artifact []byte) (err error) {
	t.Helper()
	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("ImportSnapshot panicked: %v", r)
		}
	}()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	return nh.ImportSnapshot(ctx, shardID, bytes.NewReader(artifact))
}

// TestNodeHost_ExportImport_FreshNodeHostSameState exports a snapshot from
// NodeHost A and imports it into a fresh NodeHost B, whose shard then starts
// with A's state. It also covers the documented refusals: re-importing an
// index already recorded, importing into a running shard, and a truncated
// artifact.
func TestNodeHost_ExportImport_FreshNodeHostSameState(t *testing.T) {
	const replicaID = uint64(1)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// (a) NodeHost A: 20 proposals, a snapshot, an export.
	a := newTestNodeHost(t)
	startTestShardWith(t, a, exportImportShard, replicaID, testSMFactory())
	session := a.GetNoOPSession(exportImportShard)
	var last []byte
	for i := 0; i < 20; i++ {
		last = []byte(fmt.Sprintf("value-%02d", i))
		if _, err := a.SyncPropose(ctx, session, last); err != nil {
			t.Fatalf("SyncPropose %d: %v", i, err)
		}
	}
	if _, err := a.SyncRequestSnapshot(ctx, exportImportShard, DefaultSnapshotOption); err != nil {
		t.Fatalf("SyncRequestSnapshot: %v", err)
	}
	var buf bytes.Buffer
	if err := a.ExportSnapshot(ctx, exportImportShard, &buf); err != nil {
		t.Fatalf("ExportSnapshot: %v", err)
	}
	artifact := buf.Bytes()
	if len(artifact) == 0 {
		t.Fatal("ExportSnapshot wrote an empty artifact")
	}

	// (b) A stopped: re-importing its own snapshot's index is refused as
	// documented (or accepted), never a panic.
	stopShard(t, a, exportImportShard, replicaID)
	if err := importNoPanic(t, a, exportImportShard, artifact); err != nil {
		t.Logf("re-import into A: %v", err)
		var outOfDate *quicraft.ImportSnapshotOutOfDateError
		var dirExists *quicraft.ImportSnapshotDirExistsError
		if !errors.As(err, &outOfDate) && !errors.As(err, &dirExists) {
			t.Fatalf("ImportSnapshot into A's own recorded index = %v, want nil, "+
				"*ImportSnapshotOutOfDateError or *ImportSnapshotDirExistsError", err)
		}
	}

	// (d) Fresh NodeHost B: an import into the running shard is refused.
	b := newTestNodeHost(t)
	startTestShardWith(t, b, exportImportShard, replicaID, testSMFactory())
	if err := importNoPanic(t, b, exportImportShard, artifact); !errors.Is(err, ErrInvalidOperation) {
		t.Fatalf("ImportSnapshot into a running shard = %v, want ErrInvalidOperation", err)
	}

	// (d) A truncated artifact is refused, and B's shard then starts empty.
	stopShard(t, b, exportImportShard, replicaID)
	truncated := artifact[:len(artifact)-1]
	if err := importNoPanic(t, b, exportImportShard, truncated); err == nil {
		t.Fatal("ImportSnapshot of a truncated artifact = nil, want an error")
	} else {
		t.Logf("truncated import into B: %v", err)
		var artErr *quicraft.SnapshotArtifactError
		if !errors.Is(err, quicraft.ErrSnapshotCorrupt) && !errors.As(err, &artErr) {
			t.Fatalf("ImportSnapshot of a truncated artifact = %v, want ErrSnapshotCorrupt "+
				"or *SnapshotArtifactError", err)
		}
	}
	startTestShardWith(t, b, exportImportShard, replicaID, testSMFactory())
	if got := readShard(t, b, exportImportShard); len(got) != 0 {
		t.Fatalf("B's shard after a refused import reads %q, want empty", got)
	}
	stopShard(t, b, exportImportShard, replicaID)

	// (c) The whole artifact imports into B's stopped shard, which starts
	// with A's state.
	if err := importNoPanic(t, b, exportImportShard, artifact); err != nil {
		if errors.Is(err, quicraft.ErrImportLayoutUnknown) {
			t.Fatalf("ImportSnapshot on a fresh NodeHost = %v: the README must say how "+
				"a NodeHost user imports on a fresh host", err)
		}
		t.Fatalf("ImportSnapshot on a fresh NodeHost: %v", err)
	}
	startTestShardWith(t, b, exportImportShard, replicaID, testSMFactory())
	if got := readShard(t, b, exportImportShard); !bytes.Equal(got, last) {
		t.Fatalf("B's shard after import reads %q, want A's last value %q", got, last)
	}
}
