// Copyright 2026 Jeremy Hahn
// SPDX-License-Identifier: Apache-2.0

package quicraft

import (
	"bytes"
	"errors"
	"path/filepath"
	"testing"

	"github.com/jeremyhahn/go-quicraft/pkg/internal/engine"
)

// TestReencryptSnapshotFile_DirFsyncFailureReturnsEpoch pins the
// committed-but-unsynced contract. A directory fsync that fails AFTER the
// rename leaves the destination holding the migrated artifact sealed at epoch
// E. Returning epoch 0 there told the caller the artifact was plaintext: it
// recorded no epoch, the deferred deregister dropped E's in-flight pin, and
// the next PurgeEpochsBefore destroyed E's DEK — leaving the migrated snapshot
// permanently undecryptable, the exact outcome the pinning machinery exists to
// prevent.
func TestReencryptSnapshotFile_DirFsyncFailureReturnsEpoch(t *testing.T) {
	srcBarrier := createTestBarrier(t)
	dstBarrier := createTestBarrier(t)
	migrated := bytes.Repeat([]byte("migrated-snapshot-content"), 200)

	root := "/vol"
	cfs := newCrashFS(root)
	srcPath := filepath.Join(root, "src", "snapshot.dat")
	dstPath := filepath.Join(root, "dst", "snapshot.dat")
	writeDurableFile(t, cfs, srcPath, encryptToBytes(t, srcBarrier, migrated))
	writeDurableFile(t, cfs, dstPath, encryptToBytes(t, dstBarrier, []byte("stale")))

	cfs.failOnce(opDirSync, "dst", errInjected)
	epoch, err := reencryptSnapshotFile(cfs, srcPath, dstPath,
		testSnapShardID, testSnapReplicaID, testSnapIndex, srcBarrier, dstBarrier)
	if err == nil {
		t.Fatal("a directory fsync failure must be reported, not swallowed")
	}
	if !errors.Is(err, errInjected) {
		t.Fatalf("injected cause not propagated: %v", err)
	}
	if epoch == 0 {
		t.Fatal("a post-rename fsync failure returned epoch 0 although the destination " +
			"already holds the sealed artifact; the caller records no epoch and the next " +
			"DEK purge orphans the migrated snapshot")
	}

	// The destination really is the migrated artifact at that epoch.
	stored, ok := cfs.liveBytes(dstPath)
	if !ok {
		t.Fatalf("%s is missing after the post-rename fsync failure", dstPath)
	}
	var out bytes.Buffer
	if _, decErr := decryptSnapshotFrames(dstBarrier, testSnapShardID, testSnapReplicaID,
		testSnapIndex, bytes.NewReader(stored), &out); decErr != nil {
		t.Fatalf("%s is not a complete image: %v", dstPath, decErr)
	}
	if !bytes.Equal(out.Bytes(), migrated) {
		t.Fatal("the destination does not hold the migrated content")
	}

	// No temporary file may remain: the rename consumed it.
	for _, name := range cfs.entries(filepath.Dir(dstPath)) {
		if name != "snapshot.dat" {
			t.Fatalf("leftover file %q in %s", name, filepath.Dir(dstPath))
		}
	}

	// The epoch must ALSO travel inside the error. The return-value contract
	// alone is unenforceable: every caller writes `if err != nil { return 0,
	// err }`, which drops exactly the epoch this case delivers, after which the
	// deferred deregister releases the pin and the next purge orphans the DEK.
	var notDurable *SnapshotCommittedNotDurableError
	if !errors.As(err, &notDurable) {
		t.Fatalf("err = %v (%T), want a *SnapshotCommittedNotDurableError so errors.As "+
			"recovers the epoch however the caller handles err", err, err)
	}
	if notDurable.Epoch != epoch {
		t.Errorf("error epoch = %d, want %d (the returned epoch)", notDurable.Epoch, epoch)
	}
	if notDurable.Path != dstPath {
		t.Errorf("error path = %q, want %q", notDurable.Path, dstPath)
	}

	// The idiomatic caller — the one that drops the return value entirely —
	// still recovers the epoch.
	if got := epochFromDroppedReturn(cfs, srcPath, dstPath, srcBarrier, dstBarrier); got != epoch {
		t.Errorf("epoch recovered from a caller that returned (0, err) = %d, want %d",
			got, epoch)
	}
}

// epochFromDroppedReturn models the caller ReencryptSnapshotFile's (epoch, err)
// contract cannot survive: it discards the epoch return value and recovers the
// epoch from the error alone.
func epochFromDroppedReturn(cfs *crashFS, srcPath, dstPath string,
	srcBarrier, dstBarrier Barrier) uint64 {
	cfs.failOnce(opDirSync, "dst", errInjected)
	_, err := reencryptSnapshotFile(cfs, srcPath, dstPath,
		testSnapShardID, testSnapReplicaID, testSnapIndex, srcBarrier, dstBarrier)
	var notDurable *SnapshotCommittedNotDurableError
	if errors.As(err, &notDurable) {
		return notDurable.Epoch
	}
	return 0
}

// TestReencryptSnapshotFile_CrashFSSourceMetaRestamped re-encrypts a source
// snapshot whose snapshot.data AND snapshot.meta live only in the injected
// snapshotFS. The source check must read the record through that filesystem:
// read through os it is missing, the check fails closed, and no fault test can
// reach the restamp the commit path performs.
func TestReencryptSnapshotFile_CrashFSSourceMetaRestamped(t *testing.T) {
	srcBarrier := createTestBarrier(t)
	dstBarrier := createTestBarrier(t)
	migrated := bytes.Repeat([]byte("restamped-snapshot-content"), 200)

	root := "/vol"
	cfs := newCrashFS(root)
	srcPath := filepath.Join(root, "src", "snapshot.dat")
	dstPath := filepath.Join(root, "dst", "snapshot.dat")
	srcData := encryptToBytes(t, srcBarrier, migrated)
	writeDurableFile(t, cfs, srcPath, srcData)
	writeDurableFile(t, cfs, filepath.Join(root, "src", "snapshot.meta"),
		reencryptTestSourceMeta(srcData))

	epoch, err := reencryptSnapshotFile(cfs, srcPath, dstPath,
		testSnapShardID, testSnapReplicaID, testSnapIndex, srcBarrier, dstBarrier)
	if err != nil {
		t.Fatalf("re-encrypting a valid crashFS-held snapshot failed: %v", err)
	}
	stored, ok := cfs.liveBytes(dstPath)
	if !ok {
		t.Fatalf("%s is missing after a successful re-encryption", dstPath)
	}
	metaBytes, ok := cfs.liveBytes(filepath.Join(root, "dst", "snapshot.meta"))
	if !ok {
		t.Fatal("the destination snapshot.meta was not restamped")
	}
	meta, err := engine.ParseSnapshotMeta(metaBytes)
	if err != nil {
		t.Fatalf("restamped snapshot.meta does not parse: %v", err)
	}
	sum, size, _ := engine.HashSnapshotReader(bytes.NewReader(stored))
	if meta.Epoch != epoch || meta.DataLen != size || meta.DataHash != sum {
		t.Fatalf("restamped meta = epoch %d len %d hash %x, want epoch %d len %d hash %x",
			meta.Epoch, meta.DataLen, meta.DataHash, epoch, size, sum)
	}
	if meta.Index != testSnapIndex || meta.ShardID != testSnapShardID ||
		meta.ReplicaID != testSnapReplicaID || meta.Term != reencryptTestTerm {
		t.Fatalf("restamped meta lost the source identity: %+v", meta)
	}
}

// reencryptTestTerm is the term the source snapshot.meta records.
const reencryptTestTerm = 7

// reencryptTestSourceMeta returns a snapshot.meta record that vouches for
// srcData as stored.
func reencryptTestSourceMeta(srcData []byte) []byte {
	sum, size, _ := engine.HashSnapshotReader(bytes.NewReader(srcData))
	return engine.MarshalSnapshotMeta(engine.SnapshotMeta{
		Index:     testSnapIndex,
		Term:      reencryptTestTerm,
		ShardID:   testSnapShardID,
		ReplicaID: testSnapReplicaID,
		Epoch:     1,
		DataLen:   size,
		DataHash:  sum,
	})
}
