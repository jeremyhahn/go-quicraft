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
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/jeremyhahn/go-quicraft/pkg/internal/engine"
	"github.com/jeremyhahn/go-quicraft/pkg/logdb"
)

// dataDirChangedError is returned by StartShard when the replica's bootstrap
// record states a different DataDir from the Config's or, for a replica with
// no bootstrap record, its snapshot record lies under a different one. The replica's
// snapshots, and the LogDB records naming them, are under the recorded
// DataDir: a start under another one would recover them from the old tree
// while recording the new one, and a later RemoveData or ImportSnapshot,
// which locate the tree by the record, would act on the wrong directory.
type dataDirChangedError struct {
	// ShardID is the shard whose start was refused.
	ShardID uint64
	// ReplicaID is the replica whose start was refused.
	ReplicaID uint64
	// Recorded is the DataDir the replica's bootstrap record states or,
	// for a replica with no bootstrap record, the DataDir its snapshot
	// record's path lies under ("" for the NodeHostDir layout).
	Recorded string
	// Requested is the DataDir of the Config passed to StartShard.
	Requested string
}

// Error implements the error interface.
func (e *dataDirChangedError) Error() string {
	return fmt.Sprintf("quicraft: shard %d replica %d was started with DataDir %q, refusing DataDir %q",
		e.ShardID, e.ReplicaID, e.Recorded, e.Requested)
}

// Is reports whether target is ErrInvalidOperation, so a caller matches
// StartShard's changed-DataDir refusal with errors.Is as an invalid operation.
func (e *dataDirChangedError) Is(target error) bool {
	return target == ErrInvalidOperation
}

// refuseDataDirChange returns *dataDirChangedError when the replica's
// bootstrap record states a DataDir other than dataDir or, with no bootstrap
// record, its snapshot record lies under another DataDir, and a LogDB error
// when that cannot be determined. It runs before StartShard recovers or
// writes anything, so the bootstrap record keeps stating the tree the
// replica's snapshots are in.
func (h *Host) refuseDataDirChange(shardID, replicaID uint64, dataDir string) error {
	if h.logdb == nil {
		return nil
	}
	bs, ok, err := h.logdb.GetBootstrap(shardID, replicaID)
	if err != nil {
		return err
	}
	if ok {
		if bs.DataDir != dataDir {
			return &dataDirChangedError{
				ShardID:   shardID,
				ReplicaID: replicaID,
				Recorded:  bs.DataDir,
				Requested: dataDir,
			}
		}
		return nil
	}
	// No bootstrap record: a replica joined before joins were recorded.
	// Its snapshot record still names the tree its snapshots are in, and
	// RemoveData locates them by it only until this start records dataDir,
	// so the two must agree. A record in no replica layout is left to
	// RemoveData, which refuses it.
	requestedDir := engine.ReplicaSnapshotDir(
		shardSnapshotBaseDir(h.cfg.NodeHostDir, dataDir, shardID), shardID, replicaID)
	changed := func(recordedDir string) error {
		// recordedDir is <DataDir>/shard-<id>/shard-<id>/replica-<id>; the
		// NodeHostDir layout is reported as the empty DataDir that selects it.
		recorded := filepath.Dir(filepath.Dir(filepath.Dir(recordedDir)))
		if recorded == filepath.Join(h.cfg.NodeHostDir, "snapshots") {
			recorded = ""
		}
		return &dataDirChangedError{
			ShardID:   shardID,
			ReplicaID: replicaID,
			Recorded:  recorded,
			Requested: dataDir,
		}
	}
	// A pending removal intent is what RemoveData's retry locates the tree
	// by while no bootstrap record exists, so a start (whose node then
	// records snapshots) must place them in the intent's tree too, or the
	// retry deletes the intent's tree and orphans the new one.
	if p, pending, err := h.readRemoveDataIntent(shardID, replicaID); err != nil {
		return err
	} else if pending && p != requestedDir {
		return changed(p)
	}
	ss, err := h.logdb.GetSnapshot(shardID, replicaID)
	if errors.Is(err, logdb.ErrNoSnapshot) {
		return nil
	}
	if err != nil {
		return err
	}
	recordedDir, inLayout := replicaSnapshotDirFromRecord(ss.Filepath, shardID, replicaID)
	if !inLayout || recordedDir == requestedDir {
		return nil
	}
	return changed(recordedDir)
}

// removeDataLayoutUnknownError is returned by RemoveData when the replica's
// LogDB snapshot record names a Filepath that lies in no replica snapshot
// layout this host creates, whether or not a bootstrap record states its
// DataDir (a pending removal intent, read only without a bootstrap record,
// is checked the same way). RemoveData refuses before anything is retired or
// deleted, rather than report a success that leaves the snapshots behind.
// It matches ErrInvalidOperation under errors.Is.
type removeDataLayoutUnknownError struct {
	// shardID is the shard whose data was to be removed.
	shardID uint64
	// replicaID is the replica whose data was to be removed.
	replicaID uint64
	// filepath is the snapshot record's path that matched no layout.
	filepath string
}

// Error returns a human-readable description of the error.
func (e *removeDataLayoutUnknownError) Error() string {
	return fmt.Sprintf(
		"quicraft: RemoveData of shard %d replica %d: snapshot path %q (from its snapshot record or removal intent) lies in no replica snapshot layout",
		e.shardID, e.replicaID, e.filepath,
	)
}

// Is reports whether target is ErrInvalidOperation, so callers classify the
// refusal with errors.Is(err, ErrInvalidOperation).
func (e *removeDataLayoutUnknownError) Is(target error) bool {
	return target == ErrInvalidOperation
}

// removeDataSnapshotDir resolves the snapshot directory RemoveData removes
// for the replica, and reports whether it came from a pending removal intent
// (which then need not be written again). In order: the bootstrap record's
// DataDir (StartShard records one for every start, a join included); else
// a pending intent's path (a removal that retired the replica, dropping the
// record, and then failed or crashed); else the replica directory the LogDB
// snapshot record's Filepath lies in (a replica imported with
// WithImportDataDir, or joined before joins were recorded, under a custom
// DataDir); else, with no snapshot record, the NodeHostDir layout StartShard
// uses for an empty DataDir. A snapshot record whose Filepath lies in no
// replica layout is a *removeDataLayoutUnknownError (errors.Is
// ErrInvalidOperation), and a snapshot record lying in another replica
// directory than the bootstrap record's DataDir selects is a
// *removeDataLayoutConflictError; both are returned before anything is
// retired, deleted or recorded.
func (h *Host) removeDataSnapshotDir(shardID, replicaID uint64) (string, bool, error) {
	bs, ok, err := h.logdb.GetBootstrap(shardID, replicaID)
	if err != nil {
		return "", false, err
	}
	if ok {
		dir := engine.ReplicaSnapshotDir(
			shardSnapshotBaseDir(h.cfg.NodeHostDir, bs.DataDir, shardID), shardID, replicaID)
		// Releases before v1.0.0 let a replica that had snapshotted under
		// one DataDir restart under another, which recovered the snapshot
		// from the old tree but rewrote the bootstrap record to state the
		// new one. The snapshot record still points into the old tree, so
		// it is cross-checked here: removing only the bootstrap record's
		// directory would retire the replica and report success while its
		// snapshot survives elsewhere.
		ss, err := h.logdb.GetSnapshot(shardID, replicaID)
		if err != nil && !errors.Is(err, logdb.ErrNoSnapshot) {
			return "", false, err
		}
		if err == nil && ss.Filepath != "" {
			recorded, inLayout := replicaSnapshotDirFromRecord(ss.Filepath, shardID, replicaID)
			if !inLayout {
				return "", false, &removeDataLayoutUnknownError{
					shardID:   shardID,
					replicaID: replicaID,
					filepath:  ss.Filepath,
				}
			}
			if recorded != dir {
				return "", false, &removeDataLayoutConflictError{
					ShardID:      shardID,
					ReplicaID:    replicaID,
					BootstrapDir: dir,
					SnapshotDir:  recorded,
				}
			}
		}
		return dir, false, nil
	}
	if p, pending, err := h.readRemoveDataIntent(shardID, replicaID); err != nil {
		return "", false, err
	} else if pending {
		return p, true, nil
	}
	ss, err := h.logdb.GetSnapshot(shardID, replicaID)
	if err != nil && !errors.Is(err, logdb.ErrNoSnapshot) {
		return "", false, err
	}
	if err == nil && ss.Filepath != "" {
		dir, ok := replicaSnapshotDirFromRecord(ss.Filepath, shardID, replicaID)
		if !ok {
			return "", false, &removeDataLayoutUnknownError{
				shardID:   shardID,
				replicaID: replicaID,
				filepath:  ss.Filepath,
			}
		}
		return dir, false, nil
	}
	return engine.ReplicaSnapshotDir(
		shardSnapshotBaseDir(h.cfg.NodeHostDir, "", shardID), shardID, replicaID), false, nil
}

// replicaSnapshotDirFromRecord returns the replica snapshot directory a
// snapshot record's path lies in. It walks up from path to the ancestor
// named replica-<replicaID> whose parent is shard-<shardID>, derives the
// DataDir that layout implies (shardSnapshotBaseDir puts one more
// shard-<shardID> level between the DataDir and that parent), and accepts
// the ancestor only when it is exactly the directory StartShard creates for
// that DataDir. The NodeHostDir layout is the DataDir NodeHostDir/snapshots.
func replicaSnapshotDirFromRecord(path string, shardID, replicaID uint64) (string, bool) {
	if path == "" {
		return "", false
	}
	replicaBase := fmt.Sprintf("replica-%d", replicaID)
	shardBase := fmt.Sprintf("shard-%d", shardID)
	dir := filepath.Clean(path)
	for {
		parent := filepath.Dir(dir)
		if filepath.Base(dir) == replicaBase && filepath.Base(parent) == shardBase {
			dataDir := filepath.Dir(filepath.Dir(parent))
			want := engine.ReplicaSnapshotDir(shardSnapshotBaseDir("", dataDir, shardID), shardID, replicaID)
			if dir == want {
				return dir, true
			}
			return "", false
		}
		if parent == dir {
			return "", false
		}
		dir = parent
	}
}

// removeDataIntentDir is the directory under NodeHostDir holding RemoveData's
// removal intents, one file per replica being removed.
const removeDataIntentDir = "remove-data"

// removeDataIntentPath returns the removal intent file of the replica:
// <NodeHostDir>/remove-data/shard-<S>-replica-<R>.
//
// The intent is a host-local file RemoveData alone writes and reads, holding
// nothing but the replica's snapshot directory path. It is not part of any
// versioned format (no LogDB record, snapshot artifact or wire message
// changes) and carries no version of its own.
func removeDataIntentPath(nodeHostDir string, shardID, replicaID uint64) string {
	return filepath.Join(nodeHostDir, removeDataIntentDir,
		fmt.Sprintf("shard-%d-replica-%d", shardID, replicaID))
}

// readRemoveDataIntent returns the snapshot directory a pending removal
// intent of the replica names, and whether one exists.
func (h *Host) readRemoveDataIntent(shardID, replicaID uint64) (string, bool, error) {
	p := removeDataIntentPath(h.cfg.NodeHostDir, shardID, replicaID)
	fail := func(op string, err error) (string, bool, error) {
		return "", false, &removeDataError{ShardID: shardID, ReplicaID: replicaID, Op: op, Path: p, Err: err}
	}
	f, err := h.snapshotFsys().Open(p)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return "", false, nil
		}
		return fail("open removal intent", err)
	}
	data, readErr := io.ReadAll(f)
	closeErr := f.Close()
	if readErr != nil {
		return fail("read removal intent", readErr)
	}
	if closeErr != nil {
		return fail("close removal intent", closeErr)
	}
	if len(data) == 0 {
		// An intent is published by rename only once complete, so an empty
		// one was never written by RemoveData.
		return fail("read empty removal intent", ErrInvalidOperation)
	}
	// RemoveData passes the intent to RemoveAll, so a damaged intent must
	// not redirect the deletion: it is trusted only when it names exactly
	// the replica's own snapshot directory in a layout StartShard creates,
	// never an ancestor, a path below it or another replica's directory.
	named := string(data)
	if dir, ok := replicaSnapshotDirFromRecord(named, shardID, replicaID); !ok || dir != named {
		return fail("check removal intent", &removeDataLayoutUnknownError{
			shardID:   shardID,
			replicaID: replicaID,
			filepath:  named,
		})
	}
	return named, true, nil
}

// writeRemoveDataIntent durably records that the replica's snapshot
// directory snapDir is being removed: a temporary file in the intent
// directory, written and synced, renamed into place, and then the intent
// directory and NodeHostDir synced (syncRemoveDataIntentDir).
func (h *Host) writeRemoveDataIntent(shardID, replicaID uint64, snapDir string) error {
	fsys := h.snapshotFsys()
	fail := func(op, path string, err error) error {
		return &removeDataError{ShardID: shardID, ReplicaID: replicaID, Op: op, Path: path, Err: err}
	}
	dir := filepath.Join(h.cfg.NodeHostDir, removeDataIntentDir)
	if err := fsys.MkdirAll(dir, 0o700); err != nil {
		return fail("create removal intent directory", dir, err)
	}
	tmp, err := fsys.CreateTemp(dir, ".intent-*")
	if err != nil {
		return fail("create removal intent in", dir, err)
	}
	tmpPath := tmp.Name()
	if _, err := tmp.Write([]byte(snapDir)); err != nil {
		_ = tmp.Close()
		_ = fsys.Remove(tmpPath)
		return fail("write removal intent", tmpPath, err)
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		_ = fsys.Remove(tmpPath)
		return fail("sync removal intent", tmpPath, err)
	}
	if err := tmp.Close(); err != nil {
		_ = fsys.Remove(tmpPath)
		return fail("close removal intent", tmpPath, err)
	}
	p := removeDataIntentPath(h.cfg.NodeHostDir, shardID, replicaID)
	if err := fsys.Rename(tmpPath, p); err != nil {
		_ = fsys.Remove(tmpPath)
		return fail("publish removal intent", p, err)
	}
	return syncRemoveDataIntentDir(fsys, h.cfg.NodeHostDir, shardID, replicaID)
}

// syncRemoveDataIntentDir makes the removal intents under nodeHostDir
// durable: it syncs the intent directory, which holds each intent's entry,
// and then nodeHostDir, which holds the intent directory's entry. Both are
// synced on every call, whichever of them existed before, so neither a
// rewrite after the directory was created nor a retry from a pending intent
// relies on an earlier sync that may have failed.
func syncRemoveDataIntentDir(fsys snapshotFS, nodeHostDir string, shardID, replicaID uint64) error {
	dir := filepath.Join(nodeHostDir, removeDataIntentDir)
	if err := syncDir(fsys, dir); err != nil {
		return &removeDataError{ShardID: shardID, ReplicaID: replicaID, Op: "sync removal intent directory", Path: dir, Err: err}
	}
	if err := syncDir(fsys, nodeHostDir); err != nil {
		return &removeDataError{ShardID: shardID, ReplicaID: replicaID, Op: "sync removal intent parent directory", Path: nodeHostDir, Err: err}
	}
	return nil
}

// clearRemoveDataIntent durably removes the replica's removal intent once
// every step it records is done. A missing intent is not an error.
func (h *Host) clearRemoveDataIntent(shardID, replicaID uint64) error {
	fsys := h.snapshotFsys()
	p := removeDataIntentPath(h.cfg.NodeHostDir, shardID, replicaID)
	if err := fsys.Remove(p); err != nil && !errors.Is(err, os.ErrNotExist) {
		return &removeDataError{ShardID: shardID, ReplicaID: replicaID, Op: "remove removal intent", Path: p, Err: err}
	}
	dir := filepath.Dir(p)
	if err := syncDir(fsys, dir); err != nil && !errors.Is(err, os.ErrNotExist) {
		return &removeDataError{ShardID: shardID, ReplicaID: replicaID, Op: "sync removal intent directory", Path: dir, Err: err}
	}
	return nil
}
