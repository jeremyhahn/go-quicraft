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
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"runtime/debug"
	"sort"
	"strconv"
	"sync"
	"time"

	"github.com/golang/snappy"
	"github.com/jeremyhahn/go-quicraft/pkg/config"
	"github.com/jeremyhahn/go-quicraft/pkg/logdb"
	"github.com/jeremyhahn/go-quicraft/pkg/proto"
)

// SnapshotBarrier is the interface for encrypting and decrypting snapshot
// data at rest. The concrete implementation is crypto.Barrier. This
// interface lives in the engine package to avoid an import cycle:
// engine cannot import quicraft or crypto directly.
type SnapshotBarrier interface {
	// Encrypt encrypts plaintext using AES-256-GCM with the current epoch DEK.
	// dst may be provided as a pre-allocated buffer. Returns the ciphertext.
	Encrypt(dst, plaintext []byte) ([]byte, error)

	// EncryptWithEpoch encrypts and returns both ciphertext and the epoch used.
	EncryptWithEpoch(dst, plaintext []byte) ([]byte, uint64, error)

	// Decrypt decrypts ciphertext using the epoch embedded in the ciphertext
	// prefix. Returns the plaintext.
	Decrypt(dst, ciphertext []byte) ([]byte, error)

	// DecryptForEpoch decrypts ciphertext using the DEK for the given epoch.
	DecryptForEpoch(dst, ciphertext []byte, epoch uint64) ([]byte, error)

	// CurrentEpoch returns the current key rotation epoch.
	CurrentEpoch() uint64

	// IsSealed returns true if the barrier is sealed.
	IsSealed() bool

	// RegisterInFlightEpoch increments the in-flight reference count for an
	// epoch, preventing PurgeEpochsBefore from removing it.
	RegisterInFlightEpoch(epoch uint64)

	// DeregisterInFlightEpoch decrements the in-flight reference count for
	// an epoch.
	DeregisterInFlightEpoch(epoch uint64)

	// PurgeEpochsBefore removes DEKs for epochs strictly less than minEpoch.
	// Returns the number of epochs actually purged.
	PurgeEpochsBefore(minEpoch uint64) (int, error)
}

// snapshotRequest is sent to the snapshot pool to request a snapshot
// save or recover operation for a shard.
//
// appliedToTerm is captured by the caller before sending the request to
// the pool. This eliminates a data race: the snapshot pool worker runs
// on a different goroutine from the step worker, but inMemory (which
// owns the appliedToTerm field) is documented as single-goroutine-owned
// by the step worker. By capturing the value at request creation time
// (when the caller either holds raftMu or commitPending prevents step
// worker writes), the pool worker reads a local copy instead of racing
// on the shared inMemory field.
type snapshotRequest struct {
	shardID       uint64
	replicaID     uint64
	node          *Node
	appliedToTerm uint64 // term at appliedTo, captured before pool dispatch
	save          bool   // true=save, false=recover

	// Per-request option fields, passed through from Host.RequestSnapshot.
	// These are zero-valued for auto-triggered snapshots and recovery requests.

	// exportPath, when non-empty, causes the snapshot data to be copied
	// to this directory in addition to the standard snapshot directory.
	exportPath string

	// compactionOverhead overrides the shard-level Config.CompactionOverhead
	// for this snapshot's log compaction when overrideCompaction is true.
	compactionOverhead uint64

	// overrideCompaction controls whether compactionOverhead is used.
	overrideCompaction bool

	// exported marks this snapshot as an export operation on the metadata.
	exported bool
}

// snapshotResult is the outcome of a snapshot operation, sent back
// to the engine coordinator via the completedC channel.
type snapshotResult struct {
	shardID   uint64
	replicaID uint64
	index     uint64 // snapshot log index (0 on error or recover)
	err       error
}

// snapshotPool manages a shared pool of snapshot worker goroutines.
// All workers send results to a single completion channel, eliminating
// the need for reflect.Select. Per-shard dedup via sync.Map ensures
// only one snapshot per shard is in-flight at a time.
type snapshotPool struct {
	workers    int
	logdb      logdb.LogDB
	barrier    SnapshotBarrier
	callback   Callback
	requestC   chan snapshotRequest
	completedC chan snapshotResult
	inFlight   sync.Map // shardID -> struct{} for per-shard dedup
	stopC      chan struct{}

	// workSignal is used to notify the step worker after snapshot
	// recovery completes so it re-evaluates the node immediately
	// rather than waiting for the next tick interval.
	workSignal *WorkSignal

	// metrics collects raft-level metrics. Nil when disabled.
	metrics RaftMetrics
}

// Snapshot file constants. These mirror snapshot.dataFileName and
// snapshot.metadataFileName but are defined here to avoid an import
// cycle (engine -> snapshot -> pkg -> engine).
const (
	snapshotDataFile     = "snapshot.dat"
	snapshotMetadataFile = "snapshot.meta"
	snapshotMetaSize     = 6 * 8 // 6 uint64 fields (Index, Term, ShardID, ReplicaID, CreatedAt, Epoch)
	maxSnapshotsRetained = 3     // keep last 3 snapshots

	// snapshotEncryptChunkSize is the chunk size for streaming encryption
	// of snapshot data. 64 KB balances between per-chunk overhead and
	// memory usage during encryption.
	snapshotEncryptChunkSize = 64 * 1024
)

// newSnapshotPool creates a snapshot pool with the given worker count.
// The requestC and completedC channels are sized to the worker count
// for reasonable buffering. The logdb is used to persist snapshot
// metadata and perform log compaction. The barrier, when non-nil,
// encrypts and decrypts snapshot data at rest. The callback receives
// completion notifications.
func newSnapshotPool(workers int, ldb logdb.LogDB, barrier SnapshotBarrier, callback Callback, workSignal *WorkSignal, stopC chan struct{}, metrics RaftMetrics) *snapshotPool {
	if workers < 1 {
		workers = 2
	}
	return &snapshotPool{
		workers:    workers,
		logdb:      ldb,
		barrier:    barrier,
		callback:   callback,
		requestC:   make(chan snapshotRequest, workers),
		completedC: make(chan snapshotResult, workers),
		workSignal: workSignal,
		stopC:      stopC,
		metrics:    metrics,
	}
}

// start launches the pool's worker goroutines. Each goroutine is tracked
// by the provided WaitGroup so the engine can wait for clean shutdown.
func (p *snapshotPool) start(wg *sync.WaitGroup) {
	for range p.workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			p.worker()
		}()
	}
}

// worker is the main loop for a single snapshot worker goroutine. It
// reads requests from requestC and sends results to completedC. After
// sending the result, it notifies the engine callback if one is set.
//
// Panic recovery: if user-provided state machine code (SaveSnapshot or
// RecoverFromSnapshot) panics, the worker recovers, cleans up all
// per-shard state (inFlight, snapshotting, recovering flags), fires
// OnSnapshotRejected, and continues the loop. Without recovery, the
// goroutine would die permanently, reducing pool capacity until all
// workers are dead and snapshot requests queue forever.
func (p *snapshotPool) worker() {
	for {
		select {
		case <-p.stopC:
			return
		case req := <-p.requestC:
			p.handleRequest(req)
		}
	}
}

// handleRequest processes a single snapshot request with panic recovery.
// On panic, all per-shard cleanup is performed and the callback is
// notified so the shard is not permanently wedged.
func (p *snapshotPool) handleRequest(req snapshotRequest) {
	// Recover from panics in user-provided state machine code
	// (SaveSnapshot, RecoverFromSnapshot). The deferred recovery
	// runs before the deferred cleanup below, ensuring all flags
	// are cleared even on panic.
	panicked := true
	defer func() {
		if !panicked {
			return
		}
		r := recover()
		if r == nil {
			return
		}
		stack := debug.Stack()
		slog.Error("snapshot worker panic recovered",
			"shard_id", req.shardID,
			"replica_id", req.replicaID,
			"save", req.save,
			"panic", fmt.Sprint(r),
			"stack", string(stack),
		)
		// Mirror the normal cleanup path: clear recovering flag
		// for recovery requests and signal the step worker.
		if !req.save {
			req.node.SetRecovering(false)
			if p.workSignal != nil {
				p.workSignal.Notify(req.shardID)
			}
		}
		// Clear in-flight so the next request for this shard
		// can be accepted.
		p.inFlight.Delete(req.shardID)
		// Clear snapshotting flag on the node.
		req.node.snapshotting.Store(false)
		// Notify via callback that the snapshot was rejected due
		// to the panic, so the caller can retry or report.
		if p.callback != nil {
			p.callback.OnSnapshotRejected(req.shardID, req.replicaID)
		}
	}()

	// Set the recovering flag BEFORE calling handleSnapshot
	// for recovery requests. This gates the apply worker and
	// step worker from operating on the state machine while
	// RecoverFromSnapshot executes in this goroutine.
	if !req.save {
		req.node.SetRecovering(true)
	}
	index, err := p.handleSnapshot(req)

	// Mark that we completed without panicking so the deferred
	// recovery handler becomes a no-op.
	panicked = false

	// Clear the recovering flag AFTER recovery completes
	// (success or error). Signal the work signal so the step
	// worker re-evaluates this node immediately rather than
	// waiting for the next tick.
	if !req.save {
		req.node.SetRecovering(false)
		if p.workSignal != nil {
			p.workSignal.Notify(req.shardID)
		}
	}
	// Clear in-flight before sending result so the next
	// request for the same shard can be accepted.
	p.inFlight.Delete(req.shardID)
	// Clear snapshotting flag on the node.
	req.node.snapshotting.Store(false)
	result := snapshotResult{
		shardID:   req.shardID,
		replicaID: req.replicaID,
		index:     index,
		err:       err,
	}
	// Increment metrics and fire callback before sending
	// on completedC. completedC unblocks the receiving
	// goroutine, and the scheduler may context-switch to it
	// before we execute the next line. By recording metrics
	// first, consumers see a consistent state upon receive.
	if p.metrics != nil && err == nil && req.save {
		p.metrics.IncSnapshot(req.shardID)
	}
	if p.callback != nil {
		p.callback.OnSnapshotCompleted(req.shardID, index, err)
	}
	// Non-blocking send to completedC. The callback is the
	// production notification path; completedC is used only
	// by tests. A blocking send would permanently stall ALL
	// snapshot operations once the buffered channel fills up
	// (after `workers` completions with no reader).
	select {
	case p.completedC <- result:
	default:
		slog.Warn("snapshot completedC full, result dropped",
			"shard_id", req.shardID,
			"replica_id", req.replicaID,
			"index", index,
		)
	}
}

// handleSnapshot executes a single snapshot save or recover operation.
// Returns the snapshot index for save operations, or 0 for recovers.
func (p *snapshotPool) handleSnapshot(req snapshotRequest) (uint64, error) {
	if req.save {
		return p.saveSnapshot(req)
	}
	err := p.recoverSnapshot(req)
	return 0, err
}

// saveSnapshot saves the state machine state to a snapshot directory,
// writes metadata, persists snapshot info to LogDB, garbage collects
// old snapshots, and compacts the log. Returns the snapshot index on
// success.
//
//nolint:gocyclo // snapshot save is a multi-phase operation: create, write, persist, compact, cleanup
func (p *snapshotPool) saveSnapshot(req snapshotRequest) (uint64, error) {
	node := req.node
	snapshotter := node.Snapshotter()
	if snapshotter == nil {
		return 0, &SnapshotError{
			ShardID:   req.shardID,
			ReplicaID: req.replicaID,
			Save:      true,
			Err:       ErrStopped,
		}
	}

	// Get the snapshot index from the SM's lastApplied.
	snapshotIndex := snapshotter.LastApplied()
	if snapshotIndex == 0 {
		return 0, nil // nothing to snapshot
	}

	// Create snapshot directory using the standard naming convention:
	// <snapshotDir>/shard-<N>/replica-<N>/snapshot-<index>
	saveDir := snapshotSaveDir(node.SnapshotDir(), req.shardID, req.replicaID, snapshotIndex)
	if err := os.MkdirAll(saveDir, 0o750); err != nil {
		return 0, &SnapshotError{
			ShardID:   req.shardID,
			ReplicaID: req.replicaID,
			Save:      true,
			Err:       err,
		}
	}

	// Open data file. The engine.Snapshotter interface accepts io.Writer,
	// and *os.File satisfies io.Writer, so we manage the file here.
	dataPath := filepath.Join(saveDir, snapshotDataFile)
	f, err := os.OpenFile(dataPath, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0o600)
	if err != nil {
		return 0, &SnapshotError{
			ShardID:   req.shardID,
			ReplicaID: req.replicaID,
			Save:      true,
			Err:       err,
		}
	}

	// Save SM state to the data file. When Snappy compression is
	// configured, wrap the file writer with a Snappy buffered writer
	// for transparent streaming compression.
	ctx := context.Background()
	var w io.Writer = f
	var snappyW *snappy.Writer
	if node.cfg.SnapshotCompressionType == config.Snappy {
		snappyW = snappy.NewBufferedWriter(f)
		w = snappyW
	}
	if err := snapshotter.SaveSnapshot(ctx, w, node.stopC); err != nil {
		if closeErr := f.Close(); closeErr != nil {
			slog.Debug("snapshot: file close failed after save error",
				"shard_id", req.shardID,
				"replica_id", req.replicaID,
				"path", dataPath,
				"error", closeErr,
			)
		}
		return 0, &SnapshotError{
			ShardID:   req.shardID,
			ReplicaID: req.replicaID,
			Save:      true,
			Err:       err,
		}
	}
	// Flush the Snappy writer to ensure all compressed data reaches
	// the underlying file before fsync.
	if snappyW != nil {
		if err := snappyW.Close(); err != nil {
			if closeErr := f.Close(); closeErr != nil {
				slog.Debug("snapshot: file close failed after snappy flush error",
					"shard_id", req.shardID,
					"replica_id", req.replicaID,
					"path", dataPath,
					"error", closeErr,
				)
			}
			return 0, &SnapshotError{
				ShardID:   req.shardID,
				ReplicaID: req.replicaID,
				Save:      true,
				Err:       err,
			}
		}
	}

	// Sync data to disk before recording metadata.
	if err := f.Sync(); err != nil {
		if closeErr := f.Close(); closeErr != nil {
			slog.Debug("snapshot: file close failed after sync error",
				"shard_id", req.shardID,
				"replica_id", req.replicaID,
				"path", dataPath,
				"error", closeErr,
			)
		}
		return 0, &SnapshotError{
			ShardID:   req.shardID,
			ReplicaID: req.replicaID,
			Save:      true,
			Err:       err,
		}
	}

	// Enforce MaxSnapshotSize: reject snapshots that exceed the
	// configured limit before proceeding with encryption or metadata
	// persistence. The check is performed after fsync to measure the
	// actual on-disk size (post-compression if Snappy is enabled).
	if node.cfg.MaxSnapshotSize > 0 {
		fi, statErr := f.Stat()
		if statErr != nil {
			if closeErr := f.Close(); closeErr != nil {
				slog.Debug("snapshot: file close failed after stat error",
					"shard_id", req.shardID,
					"replica_id", req.replicaID,
					"path", dataPath,
					"error", closeErr,
				)
			}
			return 0, &SnapshotError{
				ShardID:   req.shardID,
				ReplicaID: req.replicaID,
				Save:      true,
				Err:       statErr,
			}
		}
		if uint64(fi.Size()) > node.cfg.MaxSnapshotSize {
			if closeErr := f.Close(); closeErr != nil {
				slog.Debug("snapshot: file close failed before cleanup",
					"shard_id", req.shardID,
					"replica_id", req.replicaID,
					"path", dataPath,
					"error", closeErr,
				)
			}
			// Clean up the oversized snapshot directory.
			if removeErr := os.RemoveAll(saveDir); removeErr != nil {
				slog.Debug("snapshot: remove oversized directory failed",
					"shard_id", req.shardID,
					"replica_id", req.replicaID,
					"path", saveDir,
					"error", removeErr,
				)
			}
			return 0, &SnapshotTooLargeError{
				ShardID:   req.shardID,
				ReplicaID: req.replicaID,
				Size:      uint64(fi.Size()),
				Limit:     node.cfg.MaxSnapshotSize,
			}
		}
	}

	if closeErr := f.Close(); closeErr != nil {
		slog.Debug("snapshot: file close failed after successful write",
			"shard_id", req.shardID,
			"replica_id", req.replicaID,
			"path", dataPath,
			"error", closeErr,
		)
	}

	// Encrypt the snapshot data file in-place when a barrier is configured.
	var epoch uint64
	if p.barrier != nil && !p.barrier.IsSealed() {
		encryptErr := p.encryptSnapshotFile(dataPath)
		if encryptErr != nil {
			return 0, &SnapshotError{
				ShardID:   req.shardID,
				ReplicaID: req.replicaID,
				Save:      true,
				Err:       encryptErr,
			}
		}
		epoch = p.barrier.CurrentEpoch()
	}

	// Use the term captured at request creation time. The caller captures
	// Peer.AppliedToTerm() before sending the request to the pool, either
	// under raftMu (Host-initiated) or while commitPending prevents step
	// worker writes (auto-triggered by apply worker). Reading it here
	// from the Peer would race with the step worker's inMemory mutations.
	term := req.appliedToTerm

	// Write snapshot metadata file (includes epoch for encrypted snapshots).
	if err := writeSnapshotMetadata(saveDir, snapshotIndex, term, req.shardID, req.replicaID, epoch); err != nil {
		return 0, &SnapshotError{
			ShardID:   req.shardID,
			ReplicaID: req.replicaID,
			Save:      true,
			Err:       err,
		}
	}

	// Persist snapshot metadata to LogDB, including the current cluster
	// membership. Without membership, ReadState on restart returns empty
	// membership which breaks quorum calculation.
	if p.logdb != nil {
		membership := protoMembershipToLogDB(&proto.Membership{})
		if peer := node.Peer(); peer != nil {
			m := peer.GetMembership()
			membership = protoMembershipToLogDB(&m)
		}
		logdbSnap := logdb.Snapshot{
			Index:      snapshotIndex,
			Term:       term,
			Membership: membership,
			Filepath:   saveDir,
			Epoch:      epoch,
		}
		if err := p.logdb.SaveSnapshot(req.shardID, req.replicaID, logdbSnap); err != nil {
			return 0, &SnapshotError{
				ShardID:   req.shardID,
				ReplicaID: req.replicaID,
				Save:      true,
				Err:       err,
			}
		}
	}

	// Notify the LogReader of the new snapshot so the raft layer can
	// send InstallSnapshot to followers that are behind the compacted
	// region. The LogReader stores the snapshot metadata (including
	// Filepath and FileSize) and serves it via Snapshot() when
	// inmem.snapshot is nil.
	if lr := node.LogReader(); lr != nil {
		var m proto.Membership
		if peer := node.Peer(); peer != nil {
			m = peer.GetMembership()
		}
		snapDataPath := filepath.Join(saveDir, snapshotDataFile)
		var fileSize uint64
		if fi, statErr := os.Stat(snapDataPath); statErr == nil {
			fileSize = uint64(fi.Size())
		}
		if err := lr.CreateSnapshot(proto.Snapshot{
			ShardID:    req.shardID,
			ReplicaID:  req.replicaID,
			Index:      snapshotIndex,
			Term:       term,
			Filepath:   snapDataPath,
			FileSize:   fileSize,
			Membership: m,
			Epoch:      epoch,
			Imported:   req.exported,
		}); err != nil {
			if errors.Is(err, logdb.ErrSnapshotOutOfDate) {
				slog.Warn("LogReader snapshot out of date",
					"shard_id", req.shardID,
					"replica_id", req.replicaID,
					"snapshot_index", snapshotIndex,
					"error", err,
				)
			} else {
				slog.Error("LogReader CreateSnapshot failed",
					"shard_id", req.shardID,
					"replica_id", req.replicaID,
					"snapshot_index", snapshotIndex,
					"error", err,
				)
			}
		}
	}

	// Garbage collect old snapshot directories.
	if gcErr := gcSnapshotDirs(node.SnapshotDir(), req.shardID, req.replicaID); gcErr != nil {
		slog.Warn("snapshot GC failed",
			"shard_id", req.shardID,
			"replica_id", req.replicaID,
			"error", gcErr,
		)
	}

	// Update the node's last snapshot index.
	node.lastSnapshotIndex.Store(snapshotIndex)

	// Export the snapshot data to the requested path when ExportPath is set.
	if req.exportPath != "" {
		if exportErr := exportSnapshotToPath(saveDir, req.exportPath); exportErr != nil {
			return 0, &SnapshotError{
				ShardID:   req.shardID,
				ReplicaID: req.replicaID,
				Save:      true,
				Err:       exportErr,
			}
		}
	}

	// Log compaction: remove log entries covered by the snapshot, keeping
	// CompactionOverhead entries for follower catch-up. When the request
	// carries an override, use it instead of the shard-level config value.
	if p.logdb != nil {
		overhead := node.Config().CompactionOverhead
		if req.overrideCompaction {
			overhead = req.compactionOverhead
		}
		if snapshotIndex > overhead {
			compactIndex := snapshotIndex - overhead
			if removeErr := p.logdb.RemoveEntriesTo(req.shardID, req.replicaID, compactIndex); removeErr != nil {
				slog.Warn("log compaction remove failed",
					"shard_id", req.shardID,
					"replica_id", req.replicaID,
					"compact_index", compactIndex,
					"error", removeErr,
				)
			}
			if compactErr := p.logdb.Compact(req.shardID, req.replicaID); compactErr != nil {
				slog.Warn("log compaction failed",
					"shard_id", req.shardID,
					"replica_id", req.replicaID,
					"error", compactErr,
				)
			}
		}
	}

	// Purge old barrier epoch DEKs after compaction and GC. This ensures
	// key material for epochs no longer referenced by any retained
	// snapshot or WAL segment is zeroed from memory.
	p.purgeOldEpochs(node.SnapshotDir(), req.shardID, req.replicaID)

	return snapshotIndex, nil
}

// recoverSnapshot recovers the state machine from the most recent
// snapshot stored in LogDB. If no snapshot exists, recovery is a no-op.
func (p *snapshotPool) recoverSnapshot(req snapshotRequest) error {
	node := req.node
	snapshotter := node.Snapshotter()
	if snapshotter == nil {
		return nil // nothing to recover (e.g. witness node)
	}

	// Get the latest snapshot metadata from LogDB.
	if p.logdb == nil {
		return nil
	}
	logdbSnap, err := p.logdb.GetSnapshot(req.shardID, req.replicaID)
	if err != nil {
		return &SnapshotError{
			ShardID:   req.shardID,
			ReplicaID: req.replicaID,
			Save:      false,
			Err:       err,
		}
	}
	if logdbSnap.Index == 0 {
		return nil // no snapshot to recover
	}

	// Determine snapshot directory. Prefer the filepath stored in LogDB
	// metadata; fall back to discovering the latest directory on disk.
	snapshotDir := logdbSnap.Filepath
	if snapshotDir == "" {
		dir, _, dirErr := latestSnapshotDir(node.SnapshotDir(), req.shardID, req.replicaID)
		if dirErr != nil {
			return &SnapshotError{
				ShardID:   req.shardID,
				ReplicaID: req.replicaID,
				Save:      false,
				Err:       dirErr,
			}
		}
		snapshotDir = dir
	}

	// Decrypt the snapshot data file in-place when the snapshot was
	// encrypted (non-zero epoch) and a barrier is available.
	dataPath := filepath.Join(snapshotDir, snapshotDataFile)
	if logdbSnap.Epoch > 0 && p.barrier != nil && !p.barrier.IsSealed() {
		decryptErr := p.decryptSnapshotFile(dataPath, logdbSnap.Epoch)
		if decryptErr != nil {
			return &SnapshotError{
				ShardID:   req.shardID,
				ReplicaID: req.replicaID,
				Save:      false,
				Err:       decryptErr,
			}
		}
	}

	// Open the snapshot data file. The engine.Snapshotter interface
	// accepts io.Reader, and *os.File satisfies io.Reader.
	f, err := os.Open(dataPath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil // snapshot file was cleaned up, skip recovery
		}
		return &SnapshotError{
			ShardID:   req.shardID,
			ReplicaID: req.replicaID,
			Save:      false,
			Err:       err,
		}
	}
	defer func() {
		if closeErr := f.Close(); closeErr != nil {
			slog.Debug("snapshot: file close failed during recovery",
				"shard_id", req.shardID,
				"replica_id", req.replicaID,
				"path", dataPath,
				"error", closeErr,
			)
		}
	}()

	// Recover SM state from the snapshot data. When Snappy compression
	// is configured, wrap the file reader with a Snappy reader for
	// transparent streaming decompression.
	ctx := context.Background()
	var r io.Reader = f
	if node.cfg.SnapshotCompressionType == config.Snappy {
		r = snappy.NewReader(f)
	}
	if err := snapshotter.RecoverFromSnapshot(ctx, r, node.stopC); err != nil {
		return &SnapshotError{
			ShardID:   req.shardID,
			ReplicaID: req.replicaID,
			Save:      false,
			Err:       err,
		}
	}

	// Update node tracking fields to reflect recovered state.
	node.lastApplied.Store(logdbSnap.Index)
	node.lastSnapshotIndex.Store(logdbSnap.Index)

	return nil
}

// encryptSnapshotFile reads the plaintext snapshot data file, encrypts
// each chunk using the barrier, and atomically replaces the file with
// the encrypted output. The encrypted format is a sequence of frames:
// [ChunkLen:4 | EncryptedChunk]...
// where each EncryptedChunk includes the barrier's epoch+nonce+tag prefix.
//
// Atomic write: output goes to a temporary file in the same directory,
// synced to disk, then renamed over the original. If any step fails the
// temp file is removed and the original is preserved intact.
func (p *snapshotPool) encryptSnapshotFile(path string) error {
	// Open the plaintext file for streaming reads. Only one chunk
	// (snapshotEncryptChunkSize = 64KB) is held in memory at a time,
	// avoiding OOM on large snapshot files.
	srcFile, err := os.Open(path)
	if err != nil {
		return &SnapshotEncryptError{Op: "read", Err: err}
	}
	defer func() {
		if closeErr := srcFile.Close(); closeErr != nil {
			slog.Debug("snapshot: source file close failed during encryption",
				"path", path,
				"error", closeErr,
			)
		}
	}()

	dir := filepath.Dir(path)
	tmpFile, err := os.CreateTemp(dir, ".encrypt-*.tmp")
	if err != nil {
		return &SnapshotEncryptError{Op: "create_temp", Err: err}
	}
	tmpPath := tmpFile.Name()
	committed := false
	defer func() {
		if !committed {
			if closeErr := tmpFile.Close(); closeErr != nil {
				slog.Debug("snapshot: temp file close failed during encryption cleanup",
					"path", tmpPath,
					"error", closeErr,
				)
			}
			if removeErr := os.Remove(tmpPath); removeErr != nil {
				slog.Debug("snapshot: temp file remove failed during encryption cleanup",
					"path", tmpPath,
					"error", removeErr,
				)
			}
		}
	}()

	// Encrypt in chunks to bound memory usage.
	chunkBuf := make([]byte, snapshotEncryptChunkSize)
	var frameBuf [4]byte
	for {
		n, readErr := srcFile.Read(chunkBuf)
		if n > 0 {
			encrypted, encErr := p.barrier.Encrypt(nil, chunkBuf[:n])
			if encErr != nil {
				return &SnapshotEncryptError{Op: "encrypt", Err: encErr}
			}

			// Write frame: 4-byte little-endian length + encrypted data.
			binary.LittleEndian.PutUint32(frameBuf[:], uint32(len(encrypted)))
			if _, err := tmpFile.Write(frameBuf[:]); err != nil {
				return &SnapshotEncryptError{Op: "write_frame", Err: err}
			}
			if _, err := tmpFile.Write(encrypted); err != nil {
				return &SnapshotEncryptError{Op: "write_data", Err: err}
			}
		}
		if readErr == io.EOF {
			break
		}
		if readErr != nil {
			return &SnapshotEncryptError{Op: "read", Err: readErr}
		}
	}

	// Fsync before rename to guarantee durability.
	if err := tmpFile.Sync(); err != nil {
		return &SnapshotEncryptError{Op: "sync", Err: err}
	}
	if err := tmpFile.Close(); err != nil {
		return &SnapshotEncryptError{Op: "close", Err: err}
	}

	// Atomic rename replaces the original file. Both paths are internal
	// (tmpPath from os.CreateTemp, path from snapshot metadata) — not
	// user-controlled, so path traversal is not a concern.
	if err := os.Rename(tmpPath, path); err != nil { // #nosec G703 -- paths are internal, not user input
		return &SnapshotEncryptError{Op: "rename", Err: err}
	}
	committed = true
	return nil
}

// decryptSnapshotFile reads the encrypted snapshot data file, decrypts
// each chunk using the barrier for the given epoch, and atomically
// replaces the file with the plaintext output for recovery.
//
// Atomic write: output goes to a temporary file in the same directory,
// synced to disk, then renamed over the original. If any step fails the
// temp file is removed and the encrypted original is preserved intact.
func (p *snapshotPool) decryptSnapshotFile(path string, epoch uint64) error {
	// Open the encrypted file for streaming reads. Only one frame at a
	// time is held in memory, avoiding OOM on large snapshot files.
	srcFile, err := os.Open(path)
	if err != nil {
		return &SnapshotEncryptError{Op: "read", Err: err}
	}
	defer func() {
		if closeErr := srcFile.Close(); closeErr != nil {
			slog.Debug("snapshot: source file close failed during decryption",
				"path", path,
				"epoch", epoch,
				"error", closeErr,
			)
		}
	}()

	// If the file is empty, nothing to decrypt.
	info, err := srcFile.Stat()
	if err != nil {
		return &SnapshotEncryptError{Op: "read", Err: err}
	}
	if info.Size() == 0 {
		return nil
	}

	dir := filepath.Dir(path)
	tmpFile, err := os.CreateTemp(dir, ".decrypt-*.tmp")
	if err != nil {
		return &SnapshotEncryptError{Op: "create_temp", Err: err}
	}
	tmpPath := tmpFile.Name()
	committed := false
	defer func() {
		if !committed {
			if closeErr := tmpFile.Close(); closeErr != nil {
				slog.Debug("snapshot: temp file close failed during decryption cleanup",
					"path", tmpPath,
					"epoch", epoch,
					"error", closeErr,
				)
			}
			if removeErr := os.Remove(tmpPath); removeErr != nil {
				slog.Debug("snapshot: temp file remove failed during decryption cleanup",
					"path", tmpPath,
					"epoch", epoch,
					"error", removeErr,
				)
			}
		}
	}()

	// Read and decrypt each frame. Frame format: [4-byte LE length][data].
	var frameLenBuf [4]byte
	for {
		// Read the 4-byte frame length header.
		_, readErr := io.ReadFull(srcFile, frameLenBuf[:])
		if readErr == io.EOF {
			break
		}
		if readErr != nil {
			return &SnapshotEncryptError{Op: "decrypt", Err: ErrSnapshotCorrupt}
		}
		frameLen := binary.LittleEndian.Uint32(frameLenBuf[:])

		// Read the encrypted frame data.
		frame := make([]byte, frameLen)
		if _, readErr = io.ReadFull(srcFile, frame); readErr != nil {
			return &SnapshotEncryptError{Op: "decrypt", Err: ErrSnapshotCorrupt}
		}

		plaintext, decErr := p.barrier.DecryptForEpoch(nil, frame, epoch)
		if decErr != nil {
			return &SnapshotEncryptError{Op: "decrypt", Err: decErr}
		}

		if _, err := tmpFile.Write(plaintext); err != nil {
			return &SnapshotEncryptError{Op: "write", Err: err}
		}
	}

	// Fsync before rename to guarantee durability.
	if err := tmpFile.Sync(); err != nil {
		return &SnapshotEncryptError{Op: "sync", Err: err}
	}
	if err := tmpFile.Close(); err != nil {
		return &SnapshotEncryptError{Op: "close", Err: err}
	}

	// Atomic rename replaces the original file. Both paths are internal
	// (tmpPath from os.CreateTemp, path from snapshot metadata) — not
	// user-controlled, so path traversal is not a concern.
	if err := os.Rename(tmpPath, path); err != nil { // #nosec G703 -- paths are internal, not user input
		return &SnapshotEncryptError{Op: "rename", Err: err}
	}
	committed = true
	return nil
}

// purgeOldEpochs scans remaining snapshots and live WAL records for the
// oldest epoch still in use and purges barrier DEKs for older epochs.
// This is called after log compaction and snapshot garbage collection to
// reclaim old key material. The barrier must be non-nil and unsealed.
//
// The purge floor is min(snapshotMinEpoch, walMinEpoch). This prevents
// premature DEK purging when WAL segments still contain records encrypted
// at an older epoch than the oldest retained snapshot. Without this
// check, a crash after purge would fail WAL replay with WALDecryptError.
func (p *snapshotPool) purgeOldEpochs(snapshotDir string, shardID, replicaID uint64) {
	if p.barrier == nil || p.barrier.IsSealed() {
		return
	}

	dirs, err := listSnapshotDirs(snapshotDir, shardID, replicaID)
	if err != nil || len(dirs) == 0 {
		return
	}

	// Find the minimum epoch across retained snapshots by reading metadata.
	var snapshotMinEpoch uint64
	for _, d := range dirs {
		epoch := readSnapshotEpoch(d.path)
		if epoch == 0 {
			continue // unencrypted snapshot, skip
		}
		if snapshotMinEpoch == 0 || epoch < snapshotMinEpoch {
			snapshotMinEpoch = epoch
		}
	}

	if snapshotMinEpoch == 0 {
		return // all plaintext snapshots, nothing to purge
	}

	// Query the WAL/LogDB for the minimum epoch across all live records.
	// If the LogDB supports epoch awareness, use the WAL's minimum epoch
	// as an additional floor to prevent purging DEKs still needed for
	// WAL recovery.
	purgeFloor := snapshotMinEpoch
	if epochDB, ok := p.logdb.(logdb.EpochAwareLogDB); ok {
		walMinEpoch, walErr := epochDB.MinLiveEpoch()
		if walErr != nil {
			// When the WAL epoch query fails, skip purge entirely to
			// avoid data loss. The DEKs remain in memory until the next
			// successful purge cycle.
			slog.Warn("epoch purge skipped: WAL epoch query failed",
				"error", walErr,
			)
			return
		}
		if walMinEpoch > 0 && walMinEpoch < purgeFloor {
			purgeFloor = walMinEpoch
		}
	}

	// Only purge if the floor is above epoch 1 (epoch 1 is the initial
	// epoch and has no predecessor to purge).
	if purgeFloor > 1 {
		purged, purgeErr := p.barrier.PurgeEpochsBefore(purgeFloor)
		if purgeErr != nil {
			slog.Warn("epoch purge failed",
				"purge_floor", purgeFloor,
				"snapshot_min_epoch", snapshotMinEpoch,
				"error", purgeErr,
			)
		} else if purged > 0 {
			slog.Info("snapshot epoch purge completed",
				"purged", purged,
				"purge_floor", purgeFloor,
				"snapshot_min_epoch", snapshotMinEpoch,
			)
		}
	}
}

// readSnapshotEpoch reads the epoch field from a snapshot metadata file.
// Returns 0 if the metadata cannot be read or is in the old 40-byte
// format (without epoch).
func readSnapshotEpoch(dir string) uint64 {
	metaPath := filepath.Join(dir, snapshotMetadataFile)
	data, err := os.ReadFile(metaPath)
	if err != nil || len(data) < snapshotMetaSize {
		return 0
	}
	return binary.LittleEndian.Uint64(data[40:])
}

// setCallback replaces the engine callback. Used by Engine.SetCallback
// to propagate the callback to the snapshot pool.
func (p *snapshotPool) setCallback(callback Callback) {
	p.callback = callback
}

// requestSnapshot submits a snapshot request to the pool. Returns a
// SnapshotDuplicateError if a snapshot is already in-flight for the
// shard. Returns a SnapshotPoolBusyError if the request channel is
// full; the caller should retry later.
func (p *snapshotPool) requestSnapshot(req snapshotRequest) error {
	// Per-shard dedup: only one snapshot per shard in-flight.
	if _, loaded := p.inFlight.LoadOrStore(req.shardID, struct{}{}); loaded {
		if p.callback != nil {
			p.callback.OnSnapshotRejected(req.shardID, req.replicaID)
		}
		return &SnapshotDuplicateError{ShardID: req.shardID}
	}
	select {
	case p.requestC <- req:
		return nil
	default:
		// Request channel full. Clear the in-flight flag and let
		// the caller retry.
		p.inFlight.Delete(req.shardID)
		if p.callback != nil {
			p.callback.OnSnapshotRejected(req.shardID, req.replicaID)
		}
		return &SnapshotPoolBusyError{ShardID: req.shardID}
	}
}

// CompletedC returns the read-only completion channel. This channel is
// provided primarily for testing; the production notification path uses
// the Callback interface (OnSnapshotCompleted). The channel is buffered
// to the worker count. If no goroutine reads from it, results are
// dropped with a warning log (non-blocking send in worker()).
func (p *snapshotPool) CompletedC() <-chan snapshotResult {
	return p.completedC
}

// snapshotSaveDir returns the directory path for saving a snapshot at
// the given log index, using the standard naming convention:
// <baseDir>/shard-<N>/replica-<N>/snapshot-<index>
func snapshotSaveDir(baseDir string, shardID, replicaID, index uint64) string {
	return filepath.Join(
		baseDir,
		fmt.Sprintf("shard-%d", shardID),
		fmt.Sprintf("replica-%d", replicaID),
		fmt.Sprintf("snapshot-%020d", index),
	)
}

// snapshotBaseDir returns the per-shard/replica base directory.
func snapshotBaseDir(baseDir string, shardID, replicaID uint64) string {
	return filepath.Join(
		baseDir,
		fmt.Sprintf("shard-%d", shardID),
		fmt.Sprintf("replica-%d", replicaID),
	)
}

// writeSnapshotMetadata writes a 48-byte binary metadata file to the
// snapshot directory. The format is:
// Index(8) | Term(8) | ShardID(8) | ReplicaID(8) | CreatedAt(8) | Epoch(8).
// Epoch is 0 for unencrypted snapshots.
func writeSnapshotMetadata(dir string, index, term, shardID, replicaID, epoch uint64) error {
	metaPath := filepath.Join(dir, snapshotMetadataFile)
	f, err := os.OpenFile(metaPath, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0o600)
	if err != nil {
		return err
	}
	defer func() {
		if closeErr := f.Close(); closeErr != nil {
			slog.Debug("snapshot: metadata file close failed",
				"path", metaPath,
				"shard_id", shardID,
				"replica_id", replicaID,
				"error", closeErr,
			)
		}
	}()

	var buf [snapshotMetaSize]byte
	binary.LittleEndian.PutUint64(buf[0:], index)
	binary.LittleEndian.PutUint64(buf[8:], term)
	binary.LittleEndian.PutUint64(buf[16:], shardID)
	binary.LittleEndian.PutUint64(buf[24:], replicaID)
	binary.LittleEndian.PutUint64(buf[32:], uint64(time.Now().Unix()))
	binary.LittleEndian.PutUint64(buf[40:], epoch)

	if _, err := f.Write(buf[:]); err != nil {
		return err
	}
	return f.Sync()
}

// snapshotDirEntry holds a snapshot directory path and its parsed index.
type snapshotDirEntry struct {
	path  string
	index uint64
}

// listSnapshotDirs returns all snapshot directories for the given
// shard/replica, sorted by index ascending.
func listSnapshotDirs(baseDir string, shardID, replicaID uint64) ([]snapshotDirEntry, error) {
	base := snapshotBaseDir(baseDir, shardID, replicaID)
	entries, err := os.ReadDir(base)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}

	var dirs []snapshotDirEntry
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		name := entry.Name()
		// Parse "snapshot-XXXXX" format.
		if len(name) < 10 || name[:9] != "snapshot-" {
			continue
		}
		idx, parseErr := strconv.ParseUint(name[9:], 10, 64)
		if parseErr != nil {
			continue // skip non-numeric directories
		}
		dirs = append(dirs, snapshotDirEntry{
			path:  filepath.Join(base, name),
			index: idx,
		})
	}

	sort.Slice(dirs, func(i, j int) bool { return dirs[i].index < dirs[j].index })
	return dirs, nil
}

// latestSnapshotDir returns the path and index of the most recent
// snapshot directory for the given shard/replica.
func latestSnapshotDir(baseDir string, shardID, replicaID uint64) (string, uint64, error) {
	dirs, err := listSnapshotDirs(baseDir, shardID, replicaID)
	if err != nil {
		return "", 0, err
	}
	if len(dirs) == 0 {
		return "", 0, &SnapshotError{
			ShardID:   shardID,
			ReplicaID: replicaID,
			Save:      false,
			Err:       ErrStopped, // no snapshot found
		}
	}
	latest := dirs[len(dirs)-1]
	return latest.path, latest.index, nil
}

// exportSnapshotToPath copies the snapshot data file from the internal
// snapshot directory to the specified export path. The export directory
// is created with 0750 permissions if it does not exist. The snapshot
// data file is copied (not moved) so the internal snapshot directory
// remains intact for log compaction and follower catch-up.
func exportSnapshotToPath(saveDir, exportPath string) error {
	// Clean the export path to prevent directory traversal. The public
	// SnapshotOption.Validate() already ensures it is an absolute path.
	exportPath = filepath.Clean(exportPath)
	if err := os.MkdirAll(exportPath, 0o750); err != nil {
		return &SnapshotExportError{
			ExportPath: exportPath,
			Err:        err,
		}
	}

	srcPath := filepath.Join(saveDir, snapshotDataFile)
	src, err := os.Open(srcPath)
	if err != nil {
		return &SnapshotExportError{
			ExportPath: exportPath,
			Err:        err,
		}
	}
	defer func() {
		if closeErr := src.Close(); closeErr != nil {
			slog.Debug("snapshot export: source file close failed",
				"path", srcPath,
				"error", closeErr,
			)
		}
	}()

	dstPath := filepath.Join(exportPath, snapshotDataFile)
	dst, err := os.OpenFile(dstPath, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0o600)
	if err != nil {
		return &SnapshotExportError{
			ExportPath: exportPath,
			Err:        err,
		}
	}
	defer func() {
		if closeErr := dst.Close(); closeErr != nil {
			slog.Debug("snapshot export: destination file close failed",
				"path", dstPath,
				"error", closeErr,
			)
		}
	}()

	if _, err := io.Copy(dst, src); err != nil {
		return &SnapshotExportError{
			ExportPath: exportPath,
			Err:        err,
		}
	}

	if err := dst.Sync(); err != nil {
		return &SnapshotExportError{
			ExportPath: exportPath,
			Err:        err,
		}
	}

	// Copy the metadata file as well for completeness.
	srcMetaPath := filepath.Join(saveDir, snapshotMetadataFile)
	if metaData, readErr := os.ReadFile(srcMetaPath); readErr == nil {
		dstMetaPath := filepath.Join(exportPath, snapshotMetadataFile)
		if writeErr := os.WriteFile(dstMetaPath, metaData, 0o600); writeErr != nil { // #nosec G703 -- exportPath is filepath.Clean'd and validated as absolute
			slog.Warn("snapshot export: metadata copy failed",
				"path", dstMetaPath,
				"error", writeErr,
			)
		}
	}

	return nil
}

// gcSnapshotDirs removes old snapshot directories, keeping only the
// most recent maxSnapshotsRetained. Returns nil if no cleanup is needed.
func gcSnapshotDirs(baseDir string, shardID, replicaID uint64) error {
	dirs, err := listSnapshotDirs(baseDir, shardID, replicaID)
	if err != nil {
		return err
	}
	if len(dirs) <= maxSnapshotsRetained {
		return nil
	}
	// Remove oldest snapshots (dirs are sorted ascending by index).
	toRemove := dirs[:len(dirs)-maxSnapshotsRetained]
	for _, d := range toRemove {
		if err := os.RemoveAll(d.path); err != nil {
			return err
		}
	}
	return nil
}
