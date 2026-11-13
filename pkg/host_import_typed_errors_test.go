// Copyright 2026 Jeremy Hahn
// SPDX-License-Identifier: Apache-2.0

package quicraft

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/jeremyhahn/go-quicraft/pkg/logdb"
	"github.com/jeremyhahn/go-quicraft/pkg/proto"
)

// importRefusalSentinels are the three sentinels ImportSnapshot's typed
// layout, DataDir and out-of-date refusals match. Each typed refusal must Is
// exactly its own and none of the others.
var importRefusalSentinels = map[string]error{
	"ErrImportLayoutUnknown":   ErrImportLayoutUnknown,
	"ErrImportDataDirMismatch": ErrImportDataDirMismatch,
	"ErrSnapshotOutOfDate":     logdb.ErrSnapshotOutOfDate,
}

// assertIsOnlySentinel checks err Is the sentinel named want and none of the
// other import refusal sentinels.
func assertIsOnlySentinel(t *testing.T, err error, want string) {
	t.Helper()
	for name, sentinel := range importRefusalSentinels {
		got := errors.Is(err, sentinel)
		if name == want && !got {
			t.Errorf("errors.Is(%v, %s) = false, want true", err, name)
		}
		if name != want && got {
			t.Errorf("errors.Is(%v, %s) = true; a refusal must match only %s", err, name, want)
		}
	}
}

// assertMessageOrder checks msg contains every part, in the given order.
func assertMessageOrder(t *testing.T, msg string, parts ...string) {
	t.Helper()
	rest := msg
	for _, part := range parts {
		i := strings.Index(rest, part)
		if i < 0 {
			t.Fatalf("Error() = %q: want %q after the preceding parts %q", msg, part, parts)
		}
		rest = rest[i+len(part):]
	}
}

// TestImportSnapshot_TypedRefusals pins the typed contract of ImportSnapshot's
// layout, DataDir and out-of-date refusals: the fields an operator acts on
// (which shard, which replica, which values conflict) and the message that
// reports them, not only the sentinel they match.
func TestImportSnapshot_TypedRefusals(t *testing.T) {
	// Shard and replica IDs are distinct so a swapped or zeroed field shows.
	const shardID, replicaID uint64 = 4421, 3

	t.Run("layout unknown", func(t *testing.T) {
		cfg := testHostConfig(t)
		db := newMockLogDB()
		db.mu.Lock()
		db.nodeInfos = []logdb.NodeInfo{{ShardID: shardID, ReplicaID: replicaID}}
		db.mu.Unlock()
		h, err := NewHost(cfg, WithLogDB(db), WithoutTransport(), WithoutEncryption())
		if err != nil {
			t.Fatalf("NewHost failed: %v", err)
		}
		defer h.Close()

		artifact := buildSnapshotArtifact(t, 900, 5, 0, proto.Membership{},
			snapshotEnvelope([]byte("payload")))
		impErr := h.ImportSnapshot(context.Background(), shardID, bytes.NewReader(artifact))

		var typed *ImportLayoutUnknownError
		if !errors.As(impErr, &typed) {
			t.Fatalf("err = %v (%T), want *ImportLayoutUnknownError", impErr, impErr)
		}
		if typed.ShardID != shardID || typed.ReplicaID != replicaID {
			t.Fatalf("ShardID/ReplicaID = %d/%d, want %d/%d",
				typed.ShardID, typed.ReplicaID, shardID, replicaID)
		}
		assertIsOnlySentinel(t, impErr, "ErrImportLayoutUnknown")
		assertMessageOrder(t, typed.Error(),
			fmt.Sprintf("shard %d", shardID),
			fmt.Sprintf("replica %d", replicaID),
			"WithImportDataDir")
	})

	t.Run("DataDir mismatch", func(t *testing.T) {
		recorded := t.TempDir()
		declared := t.TempDir()
		if recorded == "" || declared == "" || recorded == declared {
			t.Fatalf("fixture DataDirs %q and %q must be distinct and non-empty", recorded, declared)
		}

		cfg := testHostConfig(t)
		db := newMockLogDB()
		h, err := NewHost(cfg, WithLogDB(db), WithoutTransport(), WithoutEncryption())
		if err != nil {
			t.Fatalf("NewHost failed: %v", err)
		}
		defer h.Close()

		shardCfg := testShardConfig(shardID, replicaID)
		shardCfg.DataDir = recorded
		if startErr := h.StartShard(map[uint64]string{replicaID: "127.0.0.1:5000"},
			false, testCreateFunc, shardCfg); startErr != nil {
			t.Fatalf("StartShard failed: %v", startErr)
		}
		if stopErr := h.StopShard(shardID); stopErr != nil {
			t.Fatalf("StopShard failed: %v", stopErr)
		}
		db.mu.Lock()
		db.nodeInfos = []logdb.NodeInfo{{ShardID: shardID, ReplicaID: replicaID}}
		db.mu.Unlock()

		artifact := buildSnapshotArtifact(t, 900, 5, 0, proto.Membership{},
			snapshotEnvelope([]byte("payload")))
		impErr := h.ImportSnapshot(context.Background(), shardID,
			bytes.NewReader(artifact), WithImportDataDir(declared))

		var typed *ImportDataDirMismatchError
		if !errors.As(impErr, &typed) {
			t.Fatalf("err = %v (%T), want *ImportDataDirMismatchError", impErr, impErr)
		}
		if typed.ShardID != shardID || typed.ReplicaID != replicaID {
			t.Fatalf("ShardID/ReplicaID = %d/%d, want %d/%d",
				typed.ShardID, typed.ReplicaID, shardID, replicaID)
		}
		if typed.Declared != declared {
			t.Errorf("Declared = %q, want the WithImportDataDir value %q", typed.Declared, declared)
		}
		if typed.Recorded != recorded {
			t.Errorf("Recorded = %q, want the bootstrap DataDir %q", typed.Recorded, recorded)
		}
		assertIsOnlySentinel(t, impErr, "ErrImportDataDirMismatch")
		// Documented order: the recorded DataDir, then the declared one.
		assertMessageOrder(t, typed.Error(),
			fmt.Sprintf("shard %d", shardID),
			fmt.Sprintf("replica %d", replicaID),
			fmt.Sprintf("started with DataDir %q", recorded),
			fmt.Sprintf("declares %q", declared))
	})

	t.Run("out of date", func(t *testing.T) {
		const live, stale uint64 = 900, 700
		f := newLiveImportFixture(t, false)
		if f.shardID == f.replicaID {
			t.Fatalf("fixture shard and replica IDs are both %d; a swap would not show", f.shardID)
		}
		if err := f.importArtifact(t, live, []byte("ORIGINAL")); err != nil {
			t.Fatalf("first ImportSnapshot failed: %v", err)
		}
		impErr := f.importArtifact(t, stale, []byte("STALE"))

		var typed *ImportSnapshotOutOfDateError
		if !errors.As(impErr, &typed) {
			t.Fatalf("err = %v (%T), want *ImportSnapshotOutOfDateError", impErr, impErr)
		}
		if typed.ShardID != f.shardID || typed.ReplicaID != f.replicaID {
			t.Fatalf("ShardID/ReplicaID = %d/%d, want %d/%d",
				typed.ShardID, typed.ReplicaID, f.shardID, f.replicaID)
		}
		if typed.Index != stale {
			t.Errorf("Index = %d, want the artifact index %d", typed.Index, stale)
		}
		if typed.Recorded != live {
			t.Errorf("Recorded = %d, want the live snapshot index %d", typed.Recorded, live)
		}
		assertIsOnlySentinel(t, impErr, "ErrSnapshotOutOfDate")
		assertMessageOrder(t, typed.Error(),
			fmt.Sprintf("shard %d", f.shardID),
			fmt.Sprintf("replica %d", f.replicaID),
			fmt.Sprintf("snapshot index %d recorded", live),
			fmt.Sprintf("importing index %d", stale))
	})
}
