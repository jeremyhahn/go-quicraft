// Copyright 2026 Jeremy Hahn
// SPDX-License-Identifier: Apache-2.0

package quicraft

import (
	"bytes"
	"errors"
	"io/fs"
	"path/filepath"
	"strings"
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

// closeFailFS fails Close on every file OpenFile returns for a path
// containing match, after closing it: crashFS has no Close hook, and the
// metadata restamp reports its write handle's Close error.
type closeFailFS struct {
	*crashFS
	match string
	err   error
}

func (c closeFailFS) OpenFile(name string, flag int, perm fs.FileMode) (snapshotFile, error) {
	f, err := c.crashFS.OpenFile(name, flag, perm)
	if err != nil || !strings.Contains(name, c.match) {
		return f, err
	}
	return closeFailFile{snapshotFile: f, err: c.err}, nil
}

type closeFailFile struct {
	snapshotFile
	err error
}

func (f closeFailFile) Close() error {
	_ = f.snapshotFile.Close()
	return f.err
}

// TestReencryptSnapshotFile_RestampFaults drives every failure on the
// snapshot.meta restamp path through crashFS. A failure before the data
// rename returns epoch 0 and leaves no destination; a failure after it
// returns the committed epoch, inside a *SnapshotCommittedNotDurableError as
// well, with the migrated data live; and since the directory fsync never
// completed, neither the data nor the restamped meta survives a crash.
func TestReencryptSnapshotFile_RestampFaults(t *testing.T) {
	cases := []struct {
		name      string
		inject    func(cfs *crashFS) snapshotFS
		cause     error
		committed bool
	}{
		{
			name: "source meta read",
			inject: func(cfs *crashFS) snapshotFS {
				cfs.failOnce(opOpen, "src/snapshot.meta", errInjected)
				return cfs
			},
			cause: errInjected,
		},
		{
			name: "destination meta write",
			inject: func(cfs *crashFS) snapshotFS {
				cfs.failOnce(opWrite, "dst/snapshot.meta", errInjected)
				return cfs
			},
			cause:     errInjected,
			committed: true,
		},
		{
			name: "destination meta sync",
			inject: func(cfs *crashFS) snapshotFS {
				cfs.failOnce(opSync, "dst/snapshot.meta", errInjected)
				return cfs
			},
			cause:     errInjected,
			committed: true,
		},
		{
			name: "destination meta close",
			inject: func(cfs *crashFS) snapshotFS {
				return closeFailFS{crashFS: cfs, match: "dst/snapshot.meta", err: errInjected}
			},
			cause:     errInjected,
			committed: true,
		},
		{
			name: "crash between data rename and directory fsync",
			inject: func(cfs *crashFS) snapshotFS {
				cfs.crashOn(opDirSync, "dst")
				return cfs
			},
			cause:     errCrashed,
			committed: true,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			srcBarrier := createTestBarrier(t)
			dstBarrier := createTestBarrier(t)
			migrated := bytes.Repeat([]byte("restamp-fault-content"), 200)

			root := "/vol"
			dstDir := filepath.Join(root, "dst")
			cfs := newCrashFS(root, dstDir)
			srcPath := filepath.Join(root, "src", "snapshot.dat")
			dstPath := filepath.Join(dstDir, "snapshot.dat")
			dstMeta := filepath.Join(dstDir, "snapshot.meta")
			srcData := encryptToBytes(t, srcBarrier, migrated)
			writeDurableFile(t, cfs, srcPath, srcData)
			writeDurableFile(t, cfs, filepath.Join(root, "src", "snapshot.meta"),
				reencryptTestSourceMeta(srcData))

			epoch, err := reencryptSnapshotFile(tc.inject(cfs), srcPath, dstPath,
				testSnapShardID, testSnapReplicaID, testSnapIndex, srcBarrier, dstBarrier)
			if !errors.Is(err, tc.cause) {
				t.Fatalf("err = %v, want the injected cause %v", err, tc.cause)
			}
			var notDurable *SnapshotCommittedNotDurableError
			isNotDurable := errors.As(err, &notDurable)

			if !tc.committed {
				if epoch != 0 {
					t.Fatalf("epoch = %d for a failure before the commit, want 0", epoch)
				}
				if isNotDurable {
					t.Fatalf("a failure before the commit reported %v as committed", err)
				}
				if names := cfs.entries(dstDir); len(names) != 0 {
					t.Fatalf("%s holds %v after a failure before the commit", dstDir, names)
				}
				return
			}

			if epoch == 0 {
				t.Fatal("a failure after the data rename returned epoch 0; the caller " +
					"records no epoch and the next DEK purge orphans the artifact")
			}
			if !isNotDurable {
				t.Fatalf("err = %v (%T), want a *SnapshotCommittedNotDurableError", err, err)
			}
			if notDurable.Epoch != epoch || notDurable.Path != dstPath {
				t.Fatalf("error carries epoch %d path %q, want epoch %d path %q",
					notDurable.Epoch, notDurable.Path, epoch, dstPath)
			}

			if tc.cause != errCrashed {
				stored, ok := cfs.liveBytes(dstPath)
				if !ok {
					t.Fatalf("%s is missing although the data rename committed", dstPath)
				}
				var out bytes.Buffer
				if _, decErr := decryptSnapshotFrames(dstBarrier, testSnapShardID,
					testSnapReplicaID, testSnapIndex, bytes.NewReader(stored), &out); decErr != nil {
					t.Fatalf("%s is not a complete image: %v", dstPath, decErr)
				}
				if !bytes.Equal(out.Bytes(), migrated) {
					t.Fatal("the destination does not hold the migrated content")
				}
				cfs.setHook(nil)
				cfs.Crash()
			}

			// The directory fsync never completed, so neither entry is durable.
			if cfs.exists(dstPath) {
				t.Fatalf("%s survived a crash before its directory fsync", dstPath)
			}
			if cfs.exists(dstMeta) {
				t.Fatalf("%s survived a crash before its directory fsync", dstMeta)
			}
			if !cfs.exists(dstDir) {
				t.Fatalf("the durable directory %s was lost", dstDir)
			}
		})
	}
}

// assertReencryptPairVerifies fails the test unless dir holds a snapshot.dat
// and a snapshot.meta whose DataLen and DataHash describe it, the check
// recovery applies before it accepts the artifact.
func assertReencryptPairVerifies(t *testing.T, cfs *crashFS, dir string) {
	t.Helper()
	stored, ok := cfs.liveBytes(filepath.Join(dir, "snapshot.dat"))
	if !ok {
		t.Fatalf("%s/snapshot.dat is missing", dir)
	}
	metaBytes, ok := cfs.liveBytes(filepath.Join(dir, "snapshot.meta"))
	if !ok {
		t.Fatalf("%s/snapshot.meta is missing", dir)
	}
	meta, err := engine.ParseSnapshotMeta(metaBytes)
	if err != nil {
		t.Fatalf("%s/snapshot.meta does not parse: %v", dir, err)
	}
	sum, size, _ := engine.HashSnapshotReader(bytes.NewReader(stored))
	if meta.DataLen != size || meta.DataHash != sum {
		t.Fatalf("%s/snapshot.meta records len %d hash %x, snapshot.dat has len %d hash %x",
			dir, meta.DataLen, meta.DataHash, size, sum)
	}
}

// TestReencryptSnapshotFile_CrashFSExistingDestinationKept: a destination
// that already holds a published snapshot.dat/snapshot.meta pair is refused
// before anything is written. Restamping snapshot.meta in place beside it
// would leave a crash window in which the old snapshot.dat entry comes back
// with the new record, and recovery would refuse a snapshot that was valid.
func TestReencryptSnapshotFile_CrashFSExistingDestinationKept(t *testing.T) {
	srcBarrier := createTestBarrier(t)
	dstBarrier := createTestBarrier(t)

	root := "/vol"
	cfs := newCrashFS(root)
	srcPath := filepath.Join(root, "src", "snapshot.dat")
	dstDir := filepath.Join(root, "dst")
	dstPath := filepath.Join(dstDir, "snapshot.dat")
	dstMetaPath := filepath.Join(dstDir, "snapshot.meta")
	srcData := encryptToBytes(t, srcBarrier, bytes.Repeat([]byte("new-source-content"), 200))
	writeDurableFile(t, cfs, srcPath, srcData)
	writeDurableFile(t, cfs, filepath.Join(root, "src", "snapshot.meta"),
		reencryptTestSourceMeta(srcData))
	oldData := encryptToBytes(t, dstBarrier, bytes.Repeat([]byte("published-destination"), 150))
	oldMeta := reencryptTestSourceMeta(oldData)
	writeDurableFile(t, cfs, dstPath, oldData)
	writeDurableFile(t, cfs, dstMetaPath, oldMeta)

	_, err := reencryptSnapshotFile(cfs, srcPath, dstPath,
		testSnapShardID, testSnapReplicaID, testSnapIndex, srcBarrier, dstBarrier)
	if !errors.Is(err, fs.ErrExist) {
		t.Fatalf("err = %v, want fs.ErrExist for a destination that already holds a pair", err)
	}
	if !strings.Contains(err.Error(), dstPath) {
		t.Fatalf("err = %v does not name %s", err, dstPath)
	}
	if got, _ := cfs.liveBytes(dstPath); !bytes.Equal(got, oldData) {
		t.Fatal("the published snapshot.dat was changed")
	}
	if got, _ := cfs.liveBytes(dstMetaPath); !bytes.Equal(got, oldMeta) {
		t.Fatal("the published snapshot.meta was changed")
	}
	if names := cfs.entries(dstDir); len(names) != 2 {
		t.Fatalf("%s holds %v, want only the published pair", dstDir, names)
	}

	cfs.Crash()
	assertReencryptPairVerifies(t, cfs, dstDir)
}

// TestReencryptSnapshotFile_CrashFSMetaReplacedWhole: the restamp replaces a
// snapshot.meta already beside the destination (the runbook copies the
// source's) by rename, so a crash at any point of the write leaves the old
// record or the new one whole, never a torn or truncated one.
func TestReencryptSnapshotFile_CrashFSMetaReplacedWhole(t *testing.T) {
	cases := []struct {
		name string
		op   crashOp
	}{
		{name: "crash on the meta write", op: opWrite},
		{name: "crash on the meta sync", op: opSync},
		{name: "crash on the meta rename", op: opRename},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			srcBarrier := createTestBarrier(t)
			dstBarrier := createTestBarrier(t)

			root := "/vol"
			cfs := newCrashFS(root)
			srcPath := filepath.Join(root, "src", "snapshot.dat")
			dstDir := filepath.Join(root, "dst")
			dstPath := filepath.Join(dstDir, "snapshot.dat")
			dstMetaPath := filepath.Join(dstDir, "snapshot.meta")
			srcData := encryptToBytes(t, srcBarrier, bytes.Repeat([]byte("meta-whole-content"), 200))
			srcMeta := reencryptTestSourceMeta(srcData)
			writeDurableFile(t, cfs, srcPath, srcData)
			writeDurableFile(t, cfs, filepath.Join(root, "src", "snapshot.meta"), srcMeta)
			writeDurableFile(t, cfs, dstMetaPath, srcMeta)

			cfs.crashOn(tc.op, "dst/snapshot.meta")
			_, err := reencryptSnapshotFile(cfs, srcPath, dstPath,
				testSnapShardID, testSnapReplicaID, testSnapIndex, srcBarrier, dstBarrier)
			if !errors.Is(err, errCrashed) {
				t.Fatalf("err = %v, want the injected crash", err)
			}
			cfs.setHook(nil)

			metaBytes, ok := cfs.liveBytes(dstMetaPath)
			if !ok {
				t.Fatalf("%s was lost by a crash during its restamp", dstMetaPath)
			}
			meta, err := engine.ParseSnapshotMeta(metaBytes)
			if err != nil {
				t.Fatalf("%s is torn after a crash during its restamp: %v", dstMetaPath, err)
			}
			if meta.Index != testSnapIndex || meta.ShardID != testSnapShardID ||
				meta.ReplicaID != testSnapReplicaID || meta.Term != reencryptTestTerm {
				t.Fatalf("the surviving record lost the source identity: %+v", meta)
			}
		})
	}
}
