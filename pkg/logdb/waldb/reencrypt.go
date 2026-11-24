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

package waldb

import (
	"errors"

	"github.com/jeremyhahn/go-quicraft/pkg/logdb"
)

// reencryptBatchBytes bounds each re-encryption read batch; a var so tests can lower it.
var reencryptBatchBytes uint64 = 64 << 20 // 64 MiB

// ReencryptError wraps a failure during WAL re-encryption, naming the failed
// step so operators can see where a none->keyed (or key-rotation) migration
// stopped.
type ReencryptError struct {
	Op  string
	Err error
}

// Error implements the error interface.
func (e *ReencryptError) Error() string {
	return "waldb: reencrypt: " + e.Op + ": " + e.Err.Error()
}

// Unwrap returns the underlying error.
func (e *ReencryptError) Unwrap() error { return e.Err }

// SnapshotReencryptFunc re-encrypts a node's host-owned snapshot data file between barriers and returns the destination epoch; it is called once per node with a snapshot.
type SnapshotReencryptFunc func(shardID, replicaID uint64, snap logdb.Snapshot) (dstEpoch uint64, err error)

// ErrSnapshotReencryptRequired is returned when a snapshot must be re-encrypted but no SnapshotReencryptFunc was given.
var ErrSnapshotReencryptRequired = errors.New(
	"waldb: reencrypt: snapshot data file re-encryption required but no SnapshotReencryptFunc provided")

// Reencrypt copies the LogDB at srcDir (read via srcBarrier) into an empty dstDir (written via dstBarrier) node by node, hard state last, and returns the node count; opts apply to both sides, and srcDir may be repaired by recovery.
func Reencrypt(srcDir string, srcBarrier WALBarrier, dstDir string, dstBarrier WALBarrier, snapFn SnapshotReencryptFunc, opts ...Option) (count int, err error) {
	srcOpts := make([]Option, 0, len(opts)+1)
	srcOpts = append(srcOpts, opts...)
	if srcBarrier != nil {
		srcOpts = append(srcOpts, WithBarrier(srcBarrier))
	}
	src, err := Open(srcDir, srcOpts...)
	if err != nil {
		return 0, &ReencryptError{Op: "open-source", Err: err}
	}
	// The source close error is discarded; nothing in it needs to survive.
	defer closeReencryptSource(src)

	dstOpts := make([]Option, 0, len(opts)+1)
	dstOpts = append(dstOpts, opts...)
	if dstBarrier != nil {
		dstOpts = append(dstOpts, WithBarrier(dstBarrier))
	}
	dst, err := Open(dstDir, dstOpts...)
	if err != nil {
		return 0, &ReencryptError{Op: "open-dest", Err: err}
	}
	// Report a destination close failure without masking an earlier error.
	defer func() {
		if cerr := dst.Close(); cerr != nil && err == nil {
			err = &ReencryptError{Op: "close-dest", Err: cerr}
		}
	}()

	// Copy retirements first, in one registry write, so removed replica IDs stay retired.
	if err := dst.retireNodes(src.removedNodes()); err != nil {
		return 0, &ReencryptError{Op: "retire-nodes", Err: err}
	}

	nodes, err := src.ListNodeInfo()
	if err != nil {
		return 0, &ReencryptError{Op: "list-nodes", Err: err}
	}

	barrierInvolved := srcBarrier != nil || dstBarrier != nil
	for _, ni := range nodes {
		if err := reencryptNode(src, dst, ni, barrierInvolved, snapFn); err != nil {
			return count, err
		}
		count++
	}
	return count, nil
}

// closeReencryptSource closes the source and removes the empty segments Open created in it.
func closeReencryptSource(src *DB) {
	for _, s := range src.shards {
		if s != nil {
			s.closeAfterFailedOpen()
		}
	}
	_ = src.Close()
}

// reencryptNode copies a single shard/replica's persisted Raft state from src
// to dst (which decrypt/encrypt under their respective barriers).
func reencryptNode(src, dst *DB, ni logdb.NodeInfo, barrierInvolved bool, snapFn SnapshotReencryptFunc) error {
	// Bootstrap (shard type + initial membership + join flag).
	if bs, ok, err := src.GetBootstrap(ni.ShardID, ni.ReplicaID); err != nil {
		return &ReencryptError{Op: "get-bootstrap", Err: err}
	} else if ok {
		if err := dst.saveBootstrap(ni.ShardID, ni.ReplicaID, bs); err != nil {
			return &ReencryptError{Op: "save-bootstrap", Err: err}
		}
	}

	// Snapshot metadata first, since it carries the membership; ErrNoSnapshot is not a failure.
	if snap, err := src.GetSnapshot(ni.ShardID, ni.ReplicaID); err != nil {
		if !errors.Is(err, logdb.ErrNoSnapshot) {
			return &ReencryptError{Op: "get-snapshot", Err: err}
		}
	} else if snap.Index > 0 || snap.Term > 0 {
		dstSnap := snap
		// Re-encrypt the snapshot data file through the hook and record the destination epoch.
		if barrierInvolved {
			if snapFn == nil {
				return &ReencryptError{Op: "snapshot-reencrypt", Err: ErrSnapshotReencryptRequired}
			}
			dstEpoch, ferr := snapFn(ni.ShardID, ni.ReplicaID, snap)
			if ferr != nil {
				return &ReencryptError{Op: "snapshot-reencrypt", Err: ferr}
			}
			dstSnap.Epoch = dstEpoch
		}
		if err := dst.SaveSnapshot(ni.ShardID, ni.ReplicaID, dstSnap); err != nil {
			return &ReencryptError{Op: "save-snapshot", Err: err}
		}
	}

	// Carry the compaction floor so reads below it return ErrCompacted.
	if compactedTo, err := src.CompactedTo(ni.ShardID, ni.ReplicaID); err != nil {
		return &ReencryptError{Op: "compacted-to", Err: err}
	} else if compactedTo > 0 {
		if err := dst.RemoveEntriesTo(ni.ShardID, ni.ReplicaID, compactedTo); err != nil {
			return &ReencryptError{Op: "remove-entries-to", Err: err}
		}
	}

	// Entries before the hard state, so a crash never recovers a commit past the last entry.
	if err := streamEntries(src, dst, ni); err != nil {
		return err
	}

	// Hard state last, in its own batch.
	if state, _, stateErr := src.ReadState(ni.ShardID, ni.ReplicaID); stateErr == nil {
		upd := logdb.Update{ShardID: ni.ShardID, ReplicaID: ni.ReplicaID, State: state}
		if err := dst.SaveState([]logdb.Update{upd}); err != nil {
			return &ReencryptError{Op: "save-state", Err: err}
		}
	}

	return nil
}

// streamEntries copies a node's live entries from src to dst in bounded batches, one SaveState each.
func streamEntries(src, dst *DB, ni logdb.NodeInfo) error {
	first, length, err := src.EntryRange(ni.ShardID, ni.ReplicaID)
	if err != nil {
		return &ReencryptError{Op: "entry-range", Err: err}
	}
	if length == 0 {
		return nil
	}
	high := first + length
	for low := first; low < high; {
		// Fresh slice each iteration (nil dst) so memory does not accumulate
		// across batches.
		batch, _, iterErr := src.IterateEntries(nil, 0, ni.ShardID, ni.ReplicaID, low, high, reencryptBatchBytes)
		if iterErr != nil {
			return &ReencryptError{Op: "iterate-entries", Err: iterErr}
		}
		if len(batch) == 0 {
			break // no forward progress (defensive against a stuck range)
		}
		upd := logdb.Update{ShardID: ni.ShardID, ReplicaID: ni.ReplicaID, Entries: batch}
		if err := dst.SaveState([]logdb.Update{upd}); err != nil {
			return &ReencryptError{Op: "save-state", Err: err}
		}
		low = batch[len(batch)-1].Index + 1
	}
	return nil
}
