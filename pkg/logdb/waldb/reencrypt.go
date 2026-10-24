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

// reencryptBatchBytes bounds the per-iteration byte size when streaming a
// node's log entries during re-encryption, keeping peak memory flat regardless
// of log length. A var (not const) so tests can lower it to exercise the
// multi-batch streaming path without writing 64 MiB of entries.
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

// SnapshotReencryptFunc re-encrypts a node's snapshot DATA FILE from the source
// barrier to the destination barrier and returns the destination at-rest epoch
// to record in the migrated snapshot metadata. The snapshot data file lives
// OUTSIDE the WAL — in the host/engine snapshot directory, in a host-defined
// frame format sealed by the host's seal.Barrier — so only the layer that owns
// those files can re-encrypt them. waldb owns LogDB records (entries, hard
// state, bootstrap, and snapshot METADATA) and copies those itself.
//
// It is invoked once per node that has a snapshot. Returning an error aborts the
// migration. It may be nil only when no barrier is involved on either side
// (plaintext-to-plaintext) or when no node has a snapshot; otherwise Reencrypt
// fails closed with ErrSnapshotReencryptRequired.
type SnapshotReencryptFunc func(shardID, replicaID uint64, snap logdb.Snapshot) (dstEpoch uint64, err error)

// ErrSnapshotReencryptRequired is returned when a node has a snapshot that must
// be re-encrypted during migration (a barrier is involved on at least one side)
// but no SnapshotReencryptFunc was provided. Reencrypt fails closed rather than
// copy snapshot metadata whose data file is still sealed under the source
// barrier — which the destination, opened under a different barrier, cannot
// decrypt (silent, unrecoverable snapshot loss).
var ErrSnapshotReencryptRequired = errors.New(
	"waldb: reencrypt: snapshot data file re-encryption required but no SnapshotReencryptFunc provided")

// Reencrypt re-encrypts an entire WAL/LogDB at srcDir — read through srcBarrier
// — into a freshly-opened LogDB at dstDir written through dstBarrier, returning
// the number of Raft nodes (shard/replica pairs) copied.
//
// It performs a logical copy through the public LogDB surface: for every node it
// reproduces the bootstrap record, the most recent snapshot metadata (which
// carries cluster membership), the compaction floor, the full live log-entry
// range in bounded batches, and finally the hard state. The source LogDB
// decrypts each record under srcBarrier and the destination encrypts under
// dstBarrier, so WAL records are byte-for-byte identical at the Raft level while
// their at-rest encryption changes.
//
// The hard state is written LAST on purpose: each step is independently fsynced
// and the destination WAL recovers at a record prefix, so a crash mid-migration
// can only lose the state while keeping the entries, never recover a commit
// index past the last durable entry. See reencryptNode.
//
// Snapshot DATA FILES are NOT WAL records and live outside this layer. When a
// barrier is involved, snapFn (provided by the host) re-encrypts each node's
// snapshot file and reports the destination epoch recorded in the migrated
// metadata. If snapFn is nil but a node has a snapshot, Reencrypt fails closed
// with ErrSnapshotReencryptRequired — it never copies snapshot metadata pointing
// at a source-sealed file the destination cannot decrypt.
//
// Either barrier may be nil for a plaintext side. dstDir must be empty; the
// caller verifies the result and performs the atomic directory swap, so a
// partial run never changes the Raft-visible content of the source.
//
// Closing the DESTINATION is part of the migration and its failure is reported
// (ReencryptError, Op "close-dest"). DB.Close flushes, truncates and closes
// every shard's active segment, so a swallowed error here would return "the
// migration succeeded" for a rekey whose newly written WAL was never completed
// on disk — and the caller's next act is to swap that directory into place.
//
// The source is NOT opened read-only: Open runs full crash recovery on
// srcDir, which may repair a corrupt segment tail, discard orphaned segments
// above it, create a new active segment and fsync the directory. That is the
// same recovery any normal Open would perform, and it never removes records
// that a normal Open would have replayed — but srcDir is modified on disk, so
// the caller must not treat it as byte-for-byte immutable across a run.
//
// opts are the same options Open takes and are applied to BOTH sides, which is
// what makes a non-default geometry migratable at all: opening the source with
// the default block size when it was written with another is refused outright
// by Open's WALFORMAT check (a WAL written with WithBlockSize could not be
// re-encrypted AT ALL), and opening the destination with the default would
// produce a migrated directory the caller's own configuration then rejects on
// the next Open. One option set for both sides makes the destination a
// geometry-faithful copy by construction, and a block size that disagrees with
// what srcDir records still fails closed, naming both values — Reencrypt is
// not the one path in this package where a wrong geometry is silently
// accepted. Pass the SAME options the node normally opens its WAL with.
//
// The srcBarrier and dstBarrier parameters are applied after opts and are
// authoritative: a WithBarrier in opts cannot override the side-specific
// barrier the caller named explicitly.
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
	// The SOURCE is a read path: it was opened only so its records could be
	// listed and decrypted, and nothing this call did needs to survive in it.
	// Its close error is genuinely uninteresting and stays discarded.
	defer func() { _ = src.Close() }()

	dstOpts := make([]Option, 0, len(opts)+1)
	dstOpts = append(dstOpts, opts...)
	if dstBarrier != nil {
		dstOpts = append(dstOpts, WithBarrier(dstBarrier))
	}
	dst, err := Open(dstDir, dstOpts...)
	if err != nil {
		return 0, &ReencryptError{Op: "open-dest", Err: err}
	}
	// The DESTINATION close is NOT: DB.Close flushes, truncates and closes every
	// shard's active segment and reports the first failure, so discarding it
	// returns (count, nil) — "the migration succeeded" — for a rekey whose newly
	// written WAL could not be completed on disk. The operator then swaps that
	// directory into place. Report it instead, without masking an error the
	// migration itself already produced.
	defer func() {
		if cerr := dst.Close(); cerr != nil && err == nil {
			err = &ReencryptError{Op: "close-dest", Err: cerr}
		}
	}()

	// Retirements first: the retired-replica registry is what keeps a
	// removed replica ID from ever being started again
	// (logdb.LogDB.NodeRemoved), and a migration that dropped it would hand
	// the ID back. Carried in one registry write however many there are.
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

	// Snapshot metadata FIRST — it carries the membership that ReadState
	// reports, so it must exist before the state is read back on recovery. A node
	// that has never snapshotted (e.g. a young shard whose log fits in the live
	// range) legitimately has none; ErrNoSnapshot is not a failure.
	if snap, err := src.GetSnapshot(ni.ShardID, ni.ReplicaID); err != nil {
		if !errors.Is(err, logdb.ErrNoSnapshot) {
			return &ReencryptError{Op: "get-snapshot", Err: err}
		}
	} else if snap.Index > 0 || snap.Term > 0 {
		dstSnap := snap
		// The snapshot DATA FILE is host-owned and sealed under the source
		// barrier. When a barrier is involved it must be re-encrypted via the
		// caller's hook and the resulting destination epoch recorded; copying
		// the metadata as-is would leave the destination pointing at a
		// source-sealed file it cannot decrypt.
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

	// Compaction floor. Without carrying the marker, the destination's
	// compactedTo stays 0 and a post-migration request below the true floor
	// would fail as a log gap (or, before that guard existed, be served as
	// a silently shifted range) instead of returning ErrCompacted.
	if compactedTo, err := src.CompactedTo(ni.ShardID, ni.ReplicaID); err != nil {
		return &ReencryptError{Op: "compacted-to", Err: err}
	} else if compactedTo > 0 {
		if err := dst.RemoveEntriesTo(ni.ShardID, ni.ReplicaID, compactedTo); err != nil {
			return &ReencryptError{Op: "remove-entries-to", Err: err}
		}
	}

	// Live log entries, streamed in bounded batches, BEFORE the hard state.
	// Each SaveState call is independently fsynced, so the destination WAL's
	// record order is this call order, and the WAL truncates at a prefix. Every
	// entry record therefore becomes durable before the state record naming the
	// commit index that covers it. Writing the state first would invert exactly
	// the invariant writeEntries preserves for a normal Update: a crash midway
	// through a migration would leave dstDir recovering CLEANLY with
	// HardState.Commit = N over a log that ends at k < N -- a commit index past
	// the last entry, which nothing in waldb cross-validates on reopen. The
	// state is a single small record, so it costs nothing to write it last.
	if err := streamEntries(src, dst, ni); err != nil {
		return err
	}

	// Hard state LAST, in its own batch so a large entry log does not have to
	// be buffered alongside it, and so a torn tail can only lose the state
	// while keeping the entries -- never the reverse.
	if state, _, stateErr := src.ReadState(ni.ShardID, ni.ReplicaID); stateErr == nil {
		upd := logdb.Update{ShardID: ni.ShardID, ReplicaID: ni.ReplicaID, State: state}
		if err := dst.SaveState([]logdb.Update{upd}); err != nil {
			return &ReencryptError{Op: "save-state", Err: err}
		}
	}

	return nil
}

// streamEntries copies a node's entire live log-entry range from src to dst in
// bounded batches, writing each batch as its OWN SaveState. Peak memory stays
// flat regardless of log length, and no single destination WAL record can exceed
// the segment size limit. The prior implementation accumulated the entire live
// log into one slice and emitted one SaveState, which on a multi-segment log
// allocated the whole log and could fail with ErrSegmentFull or OOM.
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
