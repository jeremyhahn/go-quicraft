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

package engine

import (
	"os"
	"path/filepath"

	"github.com/jeremyhahn/go-quicraft/pkg/config"
)

// openSaveStage creates a save's staging directory under stageParent,
// creating stageParent first when it is missing, and opens the staging data
// file for writing. The caller holds the node's write lease. On failure it
// removes the staging directory it created and returns no file.
func openSaveStage(stageParent string) (stageDir string, f *os.File, cleanupErr, err error) {
	if err := os.MkdirAll(stageParent, 0o750); err != nil {
		return "", nil, nil, err
	}
	stageDir, err = MkdirStageDir(stageParent, SaveStageDirPattern)
	if err != nil {
		return "", nil, nil, err
	}
	// The engine.Snapshotter interface accepts io.Writer, and *os.File
	// satisfies io.Writer, so the save manages the file itself.
	f, err = saveStageOpenData(filepath.Join(stageDir, snapshotDataFile))
	if err != nil {
		// The failed removal is returned, not logged: this runs under the
		// node's write lease, and no user slog.Handler may run under it.
		if rmErr := saveStageRemoveAll(stageDir); rmErr != nil && !os.IsNotExist(rmErr) {
			return stageDir, nil, rmErr, err
		}
		return "", nil, nil, err
	}
	return stageDir, f, nil, nil
}

// openLeasedSaveStage runs openSaveStage under a brief, non-blocking write
// lease on node. The release is deferred, so no exit from the open can leak
// the lease, and the open logs nothing while it is held: a stage it could
// not remove is returned as cleanupErr, with its path in stageDir, for the
// caller to log once the lease is released. A node that has stopped is
// refused the lease, and the result is ErrStopped with nothing created.
func openLeasedSaveStage(node *Node, stageParent string) (stageDir string, f *os.File, cleanupErr, err error) {
	if !node.TryAcquireWriteLease() {
		return "", nil, nil, ErrStopped
	}
	defer node.ReleaseWriteLease()
	return openSaveStage(stageParent)
}

// writeLeasedSnapshotMetadata runs writeSnapshotMetadata under a brief,
// non-blocking write lease on node, released by defer so no exit from the
// write can leak it. A node that has stopped is refused the lease, and the
// result is ErrStopped with nothing written.
func writeLeasedSnapshotMetadata(node *Node, dir string, index, term, shardID, replicaID, epoch uint64,
	compression config.CompressionType) error {
	// Hash snapshot.dat BEFORE the lease: it is a full read of the artifact,
	// and UnloadNode waits out every write lease. Under the lease only the
	// node is checked and the metadata file is written. A node already
	// stopped is refused before the read, as the lease would refuse it.
	if node.IsStopped() {
		return ErrStopped
	}
	dataHash, dataLen, err := stampSnapshotHash(filepath.Join(dir, snapshotDataFile))
	if err != nil {
		return err
	}
	if !node.TryAcquireWriteLease() {
		return ErrStopped
	}
	defer node.ReleaseWriteLease()
	return writeSnapshotMetaFile(dir, index, term, shardID, replicaID, epoch, compression, dataHash, dataLen)
}

// saveStageOpenData creates a save stage's data file. A variable so a test
// can fail the create and reach the stage's cleanup path.
var saveStageOpenData = func(path string) (*os.File, error) {
	return os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0o600)
}

// saveStageRemoveAll removes a save stage whose data file could not be
// created. A variable so a test can fail the removal and reach its
// diagnostic.
var saveStageRemoveAll = os.RemoveAll
