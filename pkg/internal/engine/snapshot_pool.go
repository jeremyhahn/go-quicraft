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
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math/rand/v2"
	"os"
	"path/filepath"
	"runtime/debug"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/golang/snappy"

	"github.com/jeremyhahn/go-quicraft/pkg/config"
	"github.com/jeremyhahn/go-quicraft/pkg/fsutil"
	"github.com/jeremyhahn/go-quicraft/pkg/internal/supervise"
	"github.com/jeremyhahn/go-quicraft/pkg/logdb"
	"github.com/jeremyhahn/go-quicraft/pkg/proto"
)

// SnapshotBarrier encrypts and decrypts snapshot data at rest; seal.Barrier implements it.
type SnapshotBarrier interface {
	// Encrypt encrypts plaintext using AES-256-GCM with the current epoch DEK.
	// dst may be provided as a pre-allocated buffer. Returns the ciphertext.
	Encrypt(dst, plaintext []byte) ([]byte, error)

	// EncryptWithEpoch encrypts and returns both ciphertext and the epoch used.
	EncryptWithEpoch(dst, plaintext []byte) ([]byte, uint64, error)

	// EncryptWithEpochContext is EncryptWithEpoch with a context folded into the AAD; an empty context is identical.
	EncryptWithEpochContext(dst, plaintext, contextAAD []byte) ([]byte, uint64, error)

	// EncryptWithEpochPinnedContext seals and registers the epoch in flight under one lock; the caller must DeregisterInFlightEpoch it.
	EncryptWithEpochPinnedContext(dst, plaintext, contextAAD []byte) ([]byte, uint64, error)

	// Decrypt decrypts ciphertext using the epoch embedded in the ciphertext
	// prefix. Returns the plaintext.
	Decrypt(dst, ciphertext []byte) ([]byte, error)

	// DecryptWithContext decrypts ciphertext sealed with the same context; there is no weaker retry.
	DecryptWithContext(dst, ciphertext, contextAAD []byte) ([]byte, error)

	// DecryptForEpoch decrypts ciphertext using the DEK for the given epoch.
	DecryptForEpoch(dst, ciphertext []byte, epoch uint64) ([]byte, error)

	// IsHardwareMode reports whether the barrier delegates sealing to an
	// external encryptor. It says nothing about whether that encryptor can
	// authenticate caller context — use SupportsContextAAD for that.
	IsHardwareMode() bool

	// SupportsContextAAD reports whether the barrier authenticates the caller context; see NewSnapshotAAD.
	SupportsContextAAD() bool

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
	PurgeEpochsBefore(ctx context.Context, minEpoch uint64) (int, error)
}

// snapshotRequest asks the pool to save or recover a shard's snapshot; appliedToTerm is captured by the caller to avoid a race.
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

	// requestKey identifies the waiting requester; zero means nobody waits.
	requestKey uint64
}

// snapshotResult is the outcome of a snapshot operation, sent on a test's
// completedC tap (see recordSnapshotResults).
type snapshotResult struct {
	shardID   uint64
	replicaID uint64
	index     uint64 // snapshot log index (0 on error or recover)
	err       error
}

// snapshotPool runs the snapshot workers, with at most one snapshot in flight per shard.
type snapshotPool struct {
	workers int
	logdb   logdb.LogDB
	barrier SnapshotBarrier

	// shardBarriers, when set, routes each shard to its own barrier; nil from it falls back to the process-wide barrier.
	shardBarriers func(shardID uint64) SnapshotBarrier

	// plaintextHostRoute lets a shard with no resolved barrier take the declared plaintext route.
	plaintextHostRoute bool

	callback Callback
	requestC chan snapshotRequest
	// completedC is a test tap, nil in production.
	completedC chan snapshotResult
	// inFlight maps shardID to the node holding its single-flight slot; a stopped incarnation's slot is taken over.
	inFlight sync.Map
	stopC    chan struct{}

	// workSignal is used to notify the step worker after snapshot
	// recovery completes so it re-evaluates the node immediately
	// rather than waiting for the next tick interval.
	workSignal *WorkSignal

	// nodes enumerates loaded nodes for the global safe-purge floor; nil in standalone tests.
	nodes func() []*Node

	// metrics collects raft-level metrics. Nil when disabled.
	metrics RaftMetrics

	// scratchFS creates and unlinks the anonymous scratch file a recovery
	// decrypts into. Nil means the real filesystem; tests substitute one to
	// observe that the scratch name is gone before any plaintext is written.
	scratchFS ScratchFS
}

// scratchFsys returns the filesystem the recovery decrypt scratch is created
// on: the configured one, or the real filesystem.
func (p *snapshotPool) scratchFsys() ScratchFS {
	if p.scratchFS != nil {
		return p.scratchFS
	}
	return osScratchFS{}
}

// Snapshot file constants. These mirror snapshot.dataFileName and
// snapshot.metadataFileName but are defined here to avoid an import
// cycle (engine -> snapshot -> pkg -> engine).
const (
	snapshotDataFile     = "snapshot.dat"
	snapshotMetadataFile = "snapshot.meta"
	maxSnapshotsRetained = 3 // keep last 3 snapshots

	// snapshotEncryptChunkSize is the chunk size for streaming encryption
	// of snapshot data. 64 KB balances between per-chunk overhead and
	// memory usage during encryption.
	snapshotEncryptChunkSize = 64 * 1024

	// maxSnapshotFrameOverhead bounds a frame's AEAD overhead, rejecting a corrupt length prefix before allocation.
	maxSnapshotFrameOverhead = 256
)

// Snapshot-pool sentinel errors. They live here rather than in errors.go
// because they are produced only by the snapshot pool's save/recover/enumerate
// paths; keeping them beside those paths keeps the typed-error contract local.
var (
	// ErrSnapshotArtifactMissing reports a recorded snapshot whose snapshot.dat is gone; a failure, so the leader re-sends.
	ErrSnapshotArtifactMissing = errors.New(
		"engine: recorded snapshot artifact (snapshot.dat) is missing")

	// ErrSnapshotNotFound reports no snapshot directory for a shard/replica, distinct from ErrStopped.
	ErrSnapshotNotFound = errors.New("engine: no snapshot directory found")
)

// newSnapshotPool creates a snapshot pool with the given worker count, LogDB, optional barrier and callback.
func newSnapshotPool(workers int, ldb logdb.LogDB, barrier SnapshotBarrier, callback Callback, workSignal *WorkSignal, stopC chan struct{}, metrics RaftMetrics) *snapshotPool {
	if workers < 1 {
		workers = 2
	}
	p := &snapshotPool{
		workers:    workers,
		logdb:      ldb,
		barrier:    barrier,
		callback:   callback,
		requestC:   make(chan snapshotRequest, workers),
		workSignal: workSignal,
		stopC:      stopC,
		metrics:    metrics,
	}
	if recordSnapshotResults {
		p.completedC = make(chan snapshotResult, workers)
	}
	return p
}

// recordSnapshotResults makes newSnapshotPool give the pool a completedC
// tap. Only the package's tests set it, before any pool is built; production
// reads every snapshot outcome through the Callback.
var recordSnapshotResults bool

// barrierForShard returns the shard's barrier, else the process-wide one; with neither it fails with ErrShardBarrierUnresolved unless the host declared plaintext.
func (p *snapshotPool) barrierForShard(shardID uint64) (SnapshotBarrier, error) {
	if p.shardBarriers == nil {
		return p.barrier, nil
	}
	if b := p.shardBarriers(shardID); b != nil {
		return b, nil
	}
	if p.barrier == nil {
		if p.plaintextHostRoute {
			return nil, nil
		}
		return nil, ErrShardBarrierUnresolved
	}
	return p.barrier, nil
}

// start launches the pool's worker goroutines. Each goroutine is tracked
// by the provided WaitGroup so the engine can wait for clean shutdown.
func (p *snapshotPool) start(wg *sync.WaitGroup) {
	for range p.workers {
		// Supervised: a Goexit in user code restarts the worker; handleRequest settles the request.
		supervise.Supervise(wg, p.stopC, p.worker, p.drainRequests)
	}
}

// worker runs snapshot requests from requestC, recovering from user-code panics and cleaning up the shard's state.
func (p *snapshotPool) worker() {
	for {
		select {
		case <-p.stopC:
			// Release requests still queued, or their shards could never snapshot again.
			p.drainRequests()
			return
		case req := <-p.requestC:
			p.handleRequest(req)
		}
	}
}

// drainRequests releases every request still queued on requestC. Safe to
// run from several goroutines at once: a channel receive is exclusive, so
// each request is released exactly once.
func (p *snapshotPool) drainRequests() {
	// Each abandoned request is reported rejected; the drain survives a panicking callback.
	supervise.RunResilient(func() {
		for {
			select {
			case req := <-p.requestC:
				p.abandonRequest(req)
			default:
				return
			}
		}
	})
}

// runRecovery waits for the raft log to hold the snapshot, takes the write lease, raises the recovering gate, drains applies, then restores.
func (p *snapshotPool) runRecovery(req snapshotRequest) error {
	if blocked := rejectBeforeRestore(p.awaitRecoverableLog(req)); blocked != nil {
		return blocked
	}
	stopped := &SnapshotError{
		ShardID:   req.shardID,
		ReplicaID: req.replicaID,
		Save:      false,
		Err:       ErrStopped,
	}
	if !req.node.AcquireWriteLease() {
		return stopped
	}
	defer req.node.ReleaseWriteLease()
	req.node.SetRecovering(true)
	if !req.node.WaitForApplyDrain(req.node.StopC()) {
		return stopped
	}
	_, _, err := p.handleSnapshot(req)
	return err
}

// abandonRequest undoes the per-shard state requestSnapshot established for
// a request that will never run, and reports the rejection so the caller
// does not wait for a completion that cannot come.
func (p *snapshotPool) abandonRequest(req snapshotRequest) {
	p.releaseInFlight(req)
	if req.node != nil {
		req.node.snapshotting.Store(false)
	}
	if p.callback != nil {
		p.callback.OnSnapshotRejected(req.shardID, req.replicaID, ErrStopped)
	}
}

// settleRecovery classifies how a recovery ended, lowers the gate, and reports whether it was abandoned or rejected.
func (p *snapshotPool) settleRecovery(req snapshotRequest, err error, stopped bool) (abandoned, rejected bool) {
	var (
		stale    *SnapshotRecoveryStaleError
		rejectBy *recoveryRejectedError
	)
	switch {
	case stopped:
		// The node stopped under the recovery: its successor recovers from the LogDB record, so report nothing.
		abandoned = true
		slog.Info("snapshot recovery abandoned: the node stopped",
			"shard_id", req.shardID,
			"replica_id", req.replicaID,
			"error", err,
		)
	case errors.As(err, &stale):
		// A stale refusal is not a failure: the state machine is already past the snapshot.
		slog.Info("snapshot recovery declined: the state machine is already past "+
			"this snapshot, and raft rejected it too",
			"shard_id", req.shardID,
			"replica_id", req.replicaID,
			"snapshot_index", stale.SnapshotIndex,
			"last_applied", stale.LastApplied,
		)
	case errors.As(err, &rejectBy):
		// Rejected before the restore: the state machine is untouched, so it is re-driven until the attempts are exhausted.
		if p.settleRejectedRecovery(req, rejectBy.err) {
			slog.Error("snapshot recovery rejected before the state machine "+
				"restore began, and no attempts remain; failing the shard",
				"shard_id", req.shardID,
				"replica_id", req.replicaID,
				"attempts", maxRecoveryAttempts,
				"error", rejectBy.err,
			)
			break
		}
		rejected = true
		slog.Error("snapshot recovery rejected before the state machine restore "+
			"began; the shard keeps running and the recovery is re-driven",
			"shard_id", req.shardID,
			"replica_id", req.replicaID,
			"error", rejectBy.err,
		)
	case err != nil:
		// A failed restore fails the shard: the user state machine may be half-restored.
		slog.Error("snapshot recovery failed; failing the shard rather "+
			"than resuming applies on a possibly half-restored state machine",
			"shard_id", req.shardID,
			"replica_id", req.replicaID,
			"error", err,
		)
		failShard(req.node, p.callback, err, nil)
	default:
		// The recovery completed: nothing is owed any more.
		req.node.redrive.reset()
	}
	req.node.SetRecovering(false)
	if p.workSignal != nil {
		p.workSignal.Notify(req.shardID)
	}
	return abandoned, rejected
}

// rejectBeforeRestore wraps a pre-restore error as *recoveryRejectedError; stale refusals and nil pass through.
func rejectBeforeRestore(err error) error {
	var stale *SnapshotRecoveryStaleError
	if err == nil || errors.As(err, &stale) {
		return err
	}
	return &recoveryRejectedError{err: err}
}

// requestProgress is how far handleRequest got, for settlePanickedRequest.
type requestProgress struct {
	// completed: the snapshot operation itself finished (a later callback
	// panicked instead); index and err are its outcome.
	completed bool
	index     uint64
	err       error
	// abandoned: the request's node stopped, so nothing reports it.
	abandoned bool
	// released: the shard's snapshot slots were already released, and the
	// requester is being (or has been) resolved by the normal path.
	released bool
}

// settlePanickedRequest undoes the per-shard state of a request whose handling
// panicked, then reports it. Slots already released are left alone: a
// successor request may hold them by then.
func (p *snapshotPool) settlePanickedRequest(req snapshotRequest, value any, progress requestProgress) {
	completed, released := progress.completed, progress.released
	stack := debug.Stack()
	// The diagnostic comes last and contained (deferred): nothing a slog
	// handler or the panic value's own methods do can skip the settlement.
	defer supervise.LogContained(func() {
		slog.Error("snapshot worker panic recovered",
			"shard_id", req.shardID,
			"replica_id", req.replicaID,
			"save", req.save,
			"completed", completed,
			"released", released,
			"panic", fmt.Sprint(value),
			"stack", string(stack),
		)
	})
	// A pre-restore or post-completion panic lowers the gate and signals the step worker; an incomplete recovery is reported rejected.
	if !req.save {
		req.node.SetRecovering(false)
		if p.workSignal != nil {
			p.workSignal.Notify(req.shardID)
		}
	}
	// Clear in-flight and the node's snapshotting flag so the next request
	// for this shard can be accepted — unless they were already released, in
	// which case they may now belong to the next request.
	if !released {
		p.releaseInFlight(req)
		req.node.snapshotting.Store(false)
	}
	// The panic is the cause the rejection reports.
	var cause error = &SnapshotSavePanicError{
		ShardID:   req.shardID,
		ReplicaID: req.replicaID,
		Value:     value,
		Stack:     stack,
	}
	// Settle a pre-restore panic like a rejection; a Goexit during recovery fails the shard.
	if !req.save && !completed && value == supervise.ErrGoexit {
		if req.node.AcquireWriteLease() {
			func() {
				defer req.node.ReleaseWriteLease()
				failShard(req.node, p.callback, &SMPanicError{
					ShardID:   req.shardID,
					ReplicaID: req.replicaID,
					Value:     value,
					Stack:     stack,
				}, nil)
			}()
		}
		return
	}
	exhausted := false
	if !req.save && !completed {
		cause = &SnapshotRecoveryPanicError{
			ShardID:   req.shardID,
			ReplicaID: req.replicaID,
			Value:     value,
			Stack:     stack,
		}
		if req.node.AcquireWriteLease() {
			func() {
				defer req.node.ReleaseWriteLease()
				exhausted = p.settleRejectedRecovery(req, cause)
			}()
		}
	}
	// Report a request the panic stopped as rejected, unless it had already completed.
	if !completed && !exhausted && p.callback != nil {
		func() {
			defer func() {
				if r2 := recover(); r2 != nil {
					supervise.LogContained(func() {
						slog.Error("OnSnapshotRejected callback panicked",
							"shard_id", req.shardID,
							"replica_id", req.replicaID,
							"panic", fmt.Sprint(r2),
						)
					})
				}
			}()
			p.callback.OnSnapshotRejected(req.shardID, req.replicaID, cause)
		}()
	}
	// Resolve the requester; a save stopped by a panic fails with SnapshotSavePanicError, not a shard failure.
	if req.save && !released && !progress.abandoned && req.requestKey != 0 && p.callback != nil {
		index, err := progress.index, progress.err
		if !completed {
			index, err = 0, &SnapshotError{
				ShardID:   req.shardID,
				ReplicaID: req.replicaID,
				Save:      true,
				Err:       cause,
			}
		}
		func() {
			defer func() {
				if r2 := recover(); r2 != nil {
					supervise.LogContained(func() {
						slog.Error("OnSnapshotRequestCompleted callback panicked",
							"shard_id", req.shardID,
							"replica_id", req.replicaID,
							"panic", fmt.Sprint(r2),
						)
					})
				}
			}()
			p.callback.OnSnapshotRequestCompleted(req.shardID, req.requestKey, index, err)
		}()
	}
}

// handleRequest processes a single snapshot request with panic recovery.
// On panic, all per-shard cleanup is performed and the callback is
// notified so the shard is not permanently wedged.
func (p *snapshotPool) handleRequest(req snapshotRequest) {
	// Recover panics and Goexits from user code and listeners for the whole function, before the deferred cleanup.
	var progress requestProgress
	normalReturn := false
	defer func() {
		if r, abnormal := supervise.ClassifyExit(recover(), normalReturn); abnormal {
			p.settlePanickedRequest(req, r, progress)
		}
	}()

	// A paused shard takes, recovers and installs no snapshot.
	if req.node.paused.Load() {
		p.refusePaused(req)
		progress.released = true
		normalReturn = true
		return
	}

	var (
		index      uint64
		compaction LogCompaction
		err        error
	)
	if req.save {
		index, compaction, err = p.handleSnapshot(req)
	} else {
		err = p.runRecovery(req)
	}

	// Mark that the snapshot operation itself finished, so a later panic is
	// reported as a post-completion panic rather than a rejected snapshot.
	progress.completed, progress.index, progress.err = true, index, err

	// Clear the recovering flag and signal the step worker; outcomes are reported under the node's write lease.
	leased := req.node.AcquireWriteLease()
	if leased {
		defer req.node.ReleaseWriteLease()
	}
	abandoned := !leased
	progress.abandoned = abandoned
	rejected := false
	if !req.save {
		abandoned, rejected = p.settleRecovery(req, err, !leased)
		progress.abandoned = abandoned
	} else if err != nil && !abandoned {
		// A failed save does not fail the shard; this log line is an automatic save's only report.
		slog.Warn("snapshot save failed; the shard keeps running",
			"shard_id", req.shardID,
			"replica_id", req.replicaID,
			"error", err,
		)
	}
	result := snapshotResult{
		shardID:   req.shardID,
		replicaID: req.replicaID,
		index:     index,
		err:       err,
	}
	// Record metrics before sending on completedC.
	if p.metrics != nil && err == nil && req.save {
		p.metrics.IncSnapshot(req.shardID)
	}
	// Report completion before releasing the slots, so the shard's snapshots are reported in order.
	if p.callback != nil && !abandoned && !rejected {
		p.reportCompleted(req, index, compaction, err)
	}
	// Release the slots so the next request for this shard can be accepted.
	p.releaseInFlight(req)
	req.node.snapshotting.Store(false)
	progress.released = true
	// A rejected recovery is reported once the slots are free, as an abandoned
	// request is (abandonRequest), so a retry it prompts is admitted. err is
	// the *recoveryRejectedError; the cause it wraps is what is reported.
	if p.callback != nil && !abandoned && rejected {
		p.callback.OnSnapshotRejected(req.shardID, req.replicaID, errors.Unwrap(err))
	}
	// Resolve the requester after the slots are free, matched by key.
	if p.callback != nil && !abandoned && req.requestKey != 0 {
		p.callback.OnSnapshotRequestCompleted(req.shardID, req.requestKey, index, err)
	}
	// Non-blocking send to the test tap.
	if p.completedC != nil {
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
	normalReturn = true
}

// reportCompleted calls OnSnapshotCompleted, then OnLogCompacted; the caller holds the slots and checked p.callback.
func (p *snapshotPool) reportCompleted(req snapshotRequest, index uint64, compaction LogCompaction, err error) {
	op := SnapshotRecover
	if req.save {
		op = SnapshotSave
	}
	p.callback.OnSnapshotCompleted(req.shardID, index, op, err)
	if err == nil && compaction.Reader {
		p.callback.OnLogCompacted(req.shardID, req.replicaID, compaction)
	}
}

// handleSnapshot executes a single snapshot save or recover operation.
// Returns the snapshot index and what log compaction did for save operations,
// or 0 and no compaction for recovers.
func (p *snapshotPool) handleSnapshot(req snapshotRequest) (uint64, LogCompaction, error) {
	if req.save {
		return p.saveSnapshotCompacting(req)
	}
	err := p.recoverSnapshot(req)
	return 0, LogCompaction{}, err
}

func recordedArtifactExists(rec logdb.Snapshot) bool {
	if rec.Filepath == "" {
		// A metadata-only record (a witness snapshot) has no artifact to lose.
		return true
	}
	if _, err := os.Stat(filepath.Join(rec.Filepath, snapshotDataFile)); err != nil {
		return !errors.Is(err, os.ErrNotExist)
	}
	return true
}

// saveSnapshot is saveSnapshotCompacting for callers that need only the
// index: the tests that drive a save directly. The pool's own path uses the
// full form so the compaction outcome reaches OnLogCompacted.
func (p *snapshotPool) saveSnapshot(req snapshotRequest) (uint64, error) {
	index, _, err := p.saveSnapshotCompacting(req)
	return index, err
}

// saveSnapshotCompacting saves, records and garbage-collects a snapshot and compacts the log; it returns the index and the compaction.
//
//nolint:gocyclo // snapshot save is a multi-phase operation: create, write, persist, compact, cleanup
func (p *snapshotPool) saveSnapshotCompacting(req snapshotRequest) (uint64, LogCompaction, error) {
	var compaction LogCompaction
	node := req.node
	snapshotter := node.Snapshotter()
	if snapshotter == nil {
		return 0, LogCompaction{}, &SnapshotError{
			ShardID:   req.shardID,
			ReplicaID: req.replicaID,
			Save:      true,
			Err:       ErrStopped,
		}
	}

	// Cheap early-out: nothing applied yet means nothing to snapshot.
	if snapshotter.LastApplied() == 0 {
		return 0, LogCompaction{}, nil
	}

	// Fail before writing anything when the configured barrier is sealed or the per-shard barrier is unresolved.
	barrier, barrierErr := p.barrierForShard(req.shardID)
	if barrierErr != nil {
		return 0, LogCompaction{}, &SnapshotError{
			ShardID:   req.shardID,
			ReplicaID: req.replicaID,
			Save:      true,
			Err:       barrierErr,
		}
	}
	if barrier != nil && barrier.IsSealed() {
		return 0, LogCompaction{}, &SnapshotError{
			ShardID:   req.shardID,
			ReplicaID: req.replicaID,
			Save:      true,
			Err:       ErrSnapshotBarrierSealed,
		}
	}

	// Quiesce applies for the point-in-time capture; afterCapture releases the gate, the defer is a backstop.
	node.SetSaving(true)
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { node.SetSaving(false) }) }
	defer release()
	if !node.WaitForApplyDrain(node.StopC()) {
		return 0, LogCompaction{}, &SnapshotError{
			ShardID:   req.shardID,
			ReplicaID: req.replicaID,
			Save:      true,
			Err:       ErrStopped,
		}
	}

	// With applies drained, capture a consistent (index, term) for the snapshot.
	snapshotIndex := snapshotter.LastApplied()
	if snapshotIndex == 0 {
		return 0, LogCompaction{}, nil
	}

	// A save at an index the durable LogDB record already covers is a no-op (an export fails), except a rebuild of a missing recorded artifact.
	rebuildingRecordedArtifact := false
	// rebuildFileSize is the FileSize the surviving record carries. A rebuild
	// keeps that record, and recovery rejects an artifact whose size differs
	// from it, so a rebuild publishes only a file of exactly that size.
	var rebuildFileSize uint64
	if p.logdb != nil {
		rec, recErr := p.logdb.GetSnapshot(req.shardID, req.replicaID)
		switch {
		case errors.Is(recErr, logdb.ErrNoSnapshot):
			// No record yet: the first save.
		case recErr != nil:
			// Unknown record state: fail the save; the snapshot on disk stays intact.
			return 0, LogCompaction{}, &SnapshotError{
				ShardID:   req.shardID,
				ReplicaID: req.replicaID,
				Save:      true,
				Err:       recErr,
			}
		case rec.Index != 0 && rec.Index >= snapshotIndex:
			// An export never rides on a non-advancing save.
			if req.exportPath != "" {
				return 0, LogCompaction{}, &SnapshotError{
					ShardID:   req.shardID,
					ReplicaID: req.replicaID,
					Save:      true,
					Err:       ErrSnapshotIndexNotAdvanced,
				}
			}
			switch {
			case recordedArtifactExists(rec):
				slog.Debug("snapshot: skipping non-advancing save",
					"shard_id", req.shardID,
					"replica_id", req.replicaID,
					"snapshot_index", snapshotIndex,
					"recorded_index", rec.Index,
				)
				return 0, LogCompaction{}, nil
			case rec.Index != snapshotIndex:
				// The record names another directory; nothing to do until the index advances.
				slog.Error("snapshot: recorded snapshot artifact is missing and cannot be rebuilt at this index",
					"shard_id", req.shardID,
					"replica_id", req.replicaID,
					"snapshot_index", snapshotIndex,
					"recorded_index", rec.Index,
					"recorded_path", rec.Filepath,
				)
				return 0, LogCompaction{}, nil
			case filepath.Clean(rec.Filepath) != filepath.Clean(snapshotSaveDir(node.SnapshotDir(), req.shardID, req.replicaID, snapshotIndex)):
				// The record names a non-canonical directory, which a rebuild cannot restore.
				slog.Error("snapshot: recorded snapshot artifact is missing and its recorded path is not the one a rebuild writes",
					"shard_id", req.shardID,
					"replica_id", req.replicaID,
					"snapshot_index", snapshotIndex,
					"recorded_path", rec.Filepath,
				)
				return 0, LogCompaction{}, nil
			case (rec.Epoch == 0) != (barrier == nil) || rec.Compression != node.cfg.SnapshotCompressionType:
				// The encryption or compression setting changed since the recorded snapshot, so only a new index can repair it.
				slog.Error("snapshot: recorded snapshot artifact is missing and cannot be rebuilt in its recorded format",
					"shard_id", req.shardID,
					"replica_id", req.replicaID,
					"snapshot_index", snapshotIndex,
					"recorded_path", rec.Filepath,
					"recorded_epoch", rec.Epoch,
					"recorded_compression", rec.Compression,
					"barrier_configured", barrier != nil,
					"compression", node.cfg.SnapshotCompressionType,
				)
				return 0, LogCompaction{}, nil
			default:
				// Rebuild the missing recorded artifact in place; the record itself is not rewritten.
				rebuildingRecordedArtifact = true
				rebuildFileSize = rec.FileSize
				slog.Warn("snapshot: rebuilding the recorded snapshot artifact, which is missing from disk",
					"shard_id", req.shardID,
					"replica_id", req.replicaID,
					"snapshot_index", snapshotIndex,
					"recorded_path", rec.Filepath,
				)
			}
		}
	}

	// Capture the term inside the gate with the index, so a later leader change cannot mismatch them.
	term := node.lastAppliedTerm.Load()
	if term == 0 {
		term = req.appliedToTerm
	}

	// Capture membership once, under the gate, for both records.
	var membership proto.Membership
	if peer := node.Peer(); peer != nil {
		membership = peer.GetMembership()
	}

	// Refuse the save while the captured membership folded a change above snapshotIndex (ConfigChangeID > snapshotIndex); transient.
	if membership.ConfigChangeID > snapshotIndex {
		return 0, LogCompaction{}, &SnapshotError{
			ShardID:   req.shardID,
			ReplicaID: req.replicaID,
			Save:      true,
			Err: &SnapshotMembershipAheadError{
				ShardID:        req.shardID,
				ReplicaID:      req.replicaID,
				SnapshotIndex:  snapshotIndex,
				ConfigChangeID: membership.ConfigChangeID,
			},
		}
	}

	// Stage the artifact in the save's own directory; only the publish runs under the write lease.
	canonicalDir := snapshotSaveDir(node.SnapshotDir(), req.shardID, req.replicaID, snapshotIndex)
	stageParent := filepath.Dir(canonicalDir)
	if err := os.MkdirAll(stageParent, 0o750); err != nil {
		return 0, LogCompaction{}, &SnapshotError{
			ShardID:   req.shardID,
			ReplicaID: req.replicaID,
			Save:      true,
			Err:       err,
		}
	}
	stageDir, err := MkdirStageDir(stageParent, SaveStageDirPattern)
	if err != nil {
		return 0, LogCompaction{}, &SnapshotError{
			ShardID:   req.shardID,
			ReplicaID: req.replicaID,
			Save:      true,
			Err:       err,
		}
	}

	// One deferred guard removes the staging directory on every exit before the publish.
	published := false
	defer func() {
		if published {
			return
		}
		if rmErr := os.RemoveAll(stageDir); rmErr != nil && !os.IsNotExist(rmErr) {
			slog.Warn("snapshot: failed to remove an unpublished save staging directory",
				"shard_id", req.shardID,
				"replica_id", req.replicaID,
				"path", stageDir,
				"error", rmErr,
			)
		}
	}()
	saveDir := stageDir

	// Open data file. The engine.Snapshotter interface accepts io.Writer,
	// and *os.File satisfies io.Writer, so we manage the file here.
	dataPath := filepath.Join(saveDir, snapshotDataFile)
	f, err := os.OpenFile(dataPath, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0o600)
	if err != nil {
		return 0, LogCompaction{}, &SnapshotError{
			ShardID:   req.shardID,
			ReplicaID: req.replicaID,
			Save:      true,
			Err:       err,
		}
	}

	// Write the state to the data file, through Snappy when configured; the choice is recorded with the artifact.
	compression := node.cfg.SnapshotCompressionType
	ctx := context.Background()

	// Seal as written (state machine -> snappy -> frame writer -> file), so no plaintext is ever durable; a nil barrier writes plaintext as configured.
	var w io.Writer = f
	var frameW *snapshotFrameWriter
	if barrier != nil {
		frameW = newSnapshotFrameWriter(f, barrier, req.shardID, req.replicaID, snapshotIndex)
		w = frameW
		// Release every pinned epoch on every exit, registered before the first frame; releaseAll is idempotent.
		defer frameW.ReleasePins()
	}
	var snappyW *snappy.Writer
	if compression == config.Snappy {
		snappyW = snappy.NewBufferedWriter(w)
		w = snappyW
	}
	// Use the gated save when available so the apply gate is released right after capture.
	var saveErr error
	if gs, ok := snapshotter.(gatedSnapshotter); ok {
		saveErr = gs.SaveSnapshotGated(ctx, w, node.stopC, release)
	} else {
		saveErr = snapshotter.SaveSnapshot(ctx, w, node.stopC)
	}
	if err := saveErr; err != nil {
		if closeErr := f.Close(); closeErr != nil {
			slog.Debug("snapshot: file close failed after save error",
				"shard_id", req.shardID,
				"replica_id", req.replicaID,
				"path", dataPath,
				"error", closeErr,
			)
		}
		return 0, LogCompaction{}, &SnapshotError{
			ShardID:   req.shardID,
			ReplicaID: req.replicaID,
			Save:      true,
			Err:       err,
		}
	}
	// Flush the Snappy writer to ensure all compressed data reaches
	// the frame writer (and through it the file) before fsync.
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
			return 0, LogCompaction{}, &SnapshotError{
				ShardID:   req.shardID,
				ReplicaID: req.replicaID,
				Save:      true,
				Err:       err,
			}
		}
	}

	// Emit the trailing frame; a barrier sealed mid-artifact fails before anything is fsynced.
	if frameW != nil {
		if err := frameW.Close(); err != nil {
			if closeErr := f.Close(); closeErr != nil {
				slog.Debug("snapshot: file close failed after frame flush error",
					"shard_id", req.shardID,
					"replica_id", req.replicaID,
					"path", dataPath,
					"error", closeErr,
				)
			}
			return 0, LogCompaction{}, &SnapshotError{
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
		return 0, LogCompaction{}, &SnapshotError{
			ShardID:   req.shardID,
			ReplicaID: req.replicaID,
			Save:      true,
			Err:       err,
		}
	}

	// Enforce MaxSnapshotSize on the post-compression, pre-sealing size; FileSize stays the on-disk size.
	if node.cfg.MaxSnapshotSize > 0 {
		var measured uint64
		if frameW != nil {
			measured = uint64(frameW.PlaintextBytes())
		} else {
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
				return 0, LogCompaction{}, &SnapshotError{
					ShardID:   req.shardID,
					ReplicaID: req.replicaID,
					Save:      true,
					Err:       statErr,
				}
			}
			measured = uint64(fi.Size())
		}
		if measured > node.cfg.MaxSnapshotSize {
			if closeErr := f.Close(); closeErr != nil {
				slog.Debug("snapshot: file close failed before cleanup",
					"shard_id", req.shardID,
					"replica_id", req.replicaID,
					"path", dataPath,
					"error", closeErr,
				)
			}
			return 0, LogCompaction{}, &SnapshotTooLargeError{
				ShardID:   req.shardID,
				ReplicaID: req.replicaID,
				Size:      measured,
				Limit:     node.cfg.MaxSnapshotSize,
			}
		}
	}

	// Close after the fsync and fail the save on a close error, which can carry a deferred writeback error.
	if closeErr := f.Close(); closeErr != nil {
		return 0, LogCompaction{}, &SnapshotError{
			ShardID:   req.shardID,
			ReplicaID: req.replicaID,
			Save:      true,
			Err:       closeErr,
		}
	}

	// The artifact is already sealed; record the minimum frame epoch, with every epoch pinned until the record is durable.
	var epoch uint64
	if frameW != nil {
		epoch = frameW.MinEpoch()
	}

	// Write snapshot.meta with the (index, term) captured under the gate.
	if err := writeSnapshotMetadata(saveDir, snapshotIndex, term, req.shardID, req.replicaID, epoch, compression); err != nil {
		// No snapshot.meta beside the data file: discard the unreferenced directory.
		return 0, LogCompaction{}, &SnapshotError{
			ShardID:   req.shardID,
			ReplicaID: req.replicaID,
			Save:      true,
			Err:       err,
		}
	}
	// Fsync the directory chain before recording the snapshot in LogDB.
	if err := fsutil.SyncDirChain(saveDir, node.SnapshotDir()); err != nil {
		return 0, LogCompaction{}, &SnapshotError{
			ShardID:   req.shardID,
			ReplicaID: req.replicaID,
			Save:      true,
			Err:       err,
		}
	}

	// Export before recording, as plaintext with Epoch 0, so an export failure fails an unpublished snapshot.
	if req.exportPath != "" {
		if exportErr := p.exportSnapshotBackup(saveDir, req.exportPath, barrier, SnapshotMeta{
			Index:       snapshotIndex,
			Term:        term,
			ShardID:     req.shardID,
			ReplicaID:   req.replicaID,
			Epoch:       epoch,
			Compression: compression,
		}); exportErr != nil {
			return 0, LogCompaction{}, &SnapshotError{
				ShardID:   req.shardID,
				ReplicaID: req.replicaID,
				Save:      true,
				Err:       exportErr,
			}
		}
	}

	// Measure the file after sealing; an unmeasurable artifact fails the save.
	fi, statErr := os.Stat(dataPath)
	if statErr != nil {
		return 0, LogCompaction{}, &SnapshotError{
			ShardID:   req.shardID,
			ReplicaID: req.replicaID,
			Save:      true,
			Err:       statErr,
		}
	}
	fileSize := uint64(fi.Size())
	// A recorded FileSize of 0 is the legacy "size unknown", which recovery
	// does not validate and restart re-measures, so it constrains nothing.
	if rebuildingRecordedArtifact && rebuildFileSize != 0 && fileSize != rebuildFileSize {
		// The rebuilt artifact's size differs from the record, so it could never be read; remove the stage.
		slog.Error("snapshot: rebuilt artifact does not match the recorded size and cannot replace it",
			"shard_id", req.shardID,
			"replica_id", req.replicaID,
			"snapshot_index", snapshotIndex,
			"recorded_size", rebuildFileSize,
			"rebuilt_size", fileSize,
		)
		return 0, LogCompaction{}, nil
	}

	// Publish under the node's write lease; a stopped node publishes nothing.
	if !node.AcquireWriteLease() {
		return 0, LogCompaction{}, &SnapshotError{
			ShardID:   req.shardID,
			ReplicaID: req.replicaID,
			Save:      true,
			Err:       ErrStopped,
		}
	}
	defer node.ReleaseWriteLease()
	// Publish, record, log reader and GC run under the snapshot-directory lock, released once the GC is done.
	node.LockSnapshotDir()
	unlockSnapshotDir := sync.OnceFunc(node.UnlockSnapshotDir)
	defer unlockSnapshotDir()
	if err := p.publishStagedSave(stageDir, canonicalDir, snapshotIndex, req, rebuildingRecordedArtifact); err != nil {
		return 0, LogCompaction{}, &SnapshotError{
			ShardID:   req.shardID,
			ReplicaID: req.replicaID,
			Save:      true,
			Err:       err,
		}
	}
	published = true
	saveDir = canonicalDir
	dataPath = filepath.Join(saveDir, snapshotDataFile)
	// The rename is durable only once the parent directory is fsynced, and
	// the record below must never name a directory a crash could still undo.
	if err := fsutil.SyncDirChain(saveDir, node.SnapshotDir()); err != nil {
		return 0, LogCompaction{}, &SnapshotError{
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
		logdbSnap := logdb.Snapshot{
			Index:       snapshotIndex,
			Term:        term,
			Membership:  protoMembershipToLogDB(&membership),
			Filepath:    saveDir,
			FileSize:    fileSize,
			Epoch:       epoch,
			Compression: compression,
		}
		if err := p.logdb.SaveSnapshot(req.shardID, req.replicaID, logdbSnap); err != nil {
			// A rebuild's non-advancing record is expected to be rejected.
			if !rebuildingRecordedArtifact || !errors.Is(err, logdb.ErrSnapshotOutOfDate) {
				return 0, LogCompaction{}, &SnapshotError{
					ShardID:   req.shardID,
					ReplicaID: req.replicaID,
					Save:      true,
					Err:       err,
				}
			}
			slog.Warn("snapshot: rebuilt artifact keeps the existing LogDB record",
				"shard_id", req.shardID,
				"replica_id", req.replicaID,
				"snapshot_index", snapshotIndex,
			)
		}
	}

	// Tell the LogReader about the snapshot; a rebuild refreshes the recorded size instead.
	if lr := node.LogReader(); lr != nil && rebuildingRecordedArtifact {
		if err := lr.RefreshSnapshotFileSize(snapshotIndex, fileSize); err != nil {
			slog.Warn("LogReader did not take the rebuilt snapshot's size; sends of it fail until restart",
				"shard_id", req.shardID,
				"replica_id", req.replicaID,
				"snapshot_index", snapshotIndex,
				"error", err,
			)
		}
	} else if lr != nil {
		if err := lr.CreateSnapshot(proto.Snapshot{
			ShardID:    req.shardID,
			ReplicaID:  req.replicaID,
			Index:      snapshotIndex,
			Term:       term,
			Filepath:   dataPath,
			FileSize:   fileSize,
			Membership: membership,
			Epoch:      epoch,
			// The compression this save used, which the send path reads; OnDiskIndex is not a raft index and stays unset.
			Compression: uint64(compression),
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
				// Any other setSnapshot error skips compaction; the next snapshot retries it.
				return snapshotIndex, compaction, nil
			}
		}
	}

	// Garbage collect old snapshot directories — unless the record cannot be
	// read, when there is no telling which directory GC must spare.
	if recorded, recErr := p.recordedSnapshotDir(req.shardID, req.replicaID); recErr != nil {
		slog.Warn("snapshot GC skipped: the snapshot record cannot be read",
			"shard_id", req.shardID,
			"replica_id", req.replicaID,
			"error", recErr,
		)
	} else if gcErr := gcSnapshotDirs(node.SnapshotDir(), req.shardID, req.replicaID, recorded,
		node.GCSparedSnapshotDirs()...); gcErr != nil {
		slog.Warn("snapshot GC failed",
			"shard_id", req.shardID,
			"replica_id", req.replicaID,
			"error", gcErr,
		)
	}

	unlockSnapshotDir()

	// Update the node's last snapshot index.
	node.lastSnapshotIndex.Store(snapshotIndex)

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
			// Advance the LogReader's compaction marker before removing LogDB entries; advanced records whether this save moved it.
			advanced := false
			if lr := node.LogReader(); lr != nil {
				moved, lrErr := lr.CompactTo(compactIndex)
				advanced = lrErr == nil && moved
				if lrErr != nil && !errors.Is(lrErr, logdb.ErrCompacted) {
					slog.Warn("log reader compaction failed, skipping LogDB removal",
						"shard_id", req.shardID,
						"replica_id", req.replicaID,
						"compact_index", compactIndex,
						"error", lrErr,
					)
					// The marker did not advance: skip the removal and let the next snapshot retry.
					return snapshotIndex, compaction, nil
				}
			}
			p.carryBootstrapForward(req.shardID, req.replicaID)
			removeErr := p.logdb.RemoveEntriesTo(req.shardID, req.replicaID, compactIndex)
			if removeErr != nil {
				slog.Warn("log compaction remove failed",
					"shard_id", req.shardID,
					"replica_id", req.replicaID,
					"compact_index", compactIndex,
					"error", removeErr,
				)
			}
			compactErr := p.logdb.Compact(req.shardID, req.replicaID)
			if compactErr != nil {
				slog.Warn("log compaction failed",
					"shard_id", req.shardID,
					"replica_id", req.replicaID,
					"error", compactErr,
				)
			}
			if advanced {
				compaction = LogCompaction{
					Index:  compactIndex,
					Reader: true,
					LogDB:  removeErr == nil && compactErr == nil,
				}
			}
		}
	}

	// Purge DEKs of epochs no retained snapshot or WAL segment references.
	p.purgeOldEpochs(node.SnapshotDir(), req.shardID, req.replicaID)

	return snapshotIndex, compaction, nil
}

// publishStagedSave renames the staged artifact onto snapshot-<index>, replacing only an orphan or the artifact being rebuilt; the caller holds the lease and lock.
func (p *snapshotPool) publishStagedSave(stageDir, canonicalDir string, index uint64, req snapshotRequest, rebuilding bool) error {
	if _, statErr := os.Stat(canonicalDir); statErr == nil {
		if req.node.IsSnapshotDirClaimed(index) || req.node.IsSnapshotDirPinned(canonicalDir) {
			return ErrSnapshotIndexNotAdvanced
		}
		if p.logdb != nil {
			rec, recErr := p.logdb.GetSnapshot(req.shardID, req.replicaID)
			switch {
			case errors.Is(recErr, logdb.ErrNoSnapshot):
			case recErr != nil:
				return recErr
			case filepath.Clean(rec.Filepath) == filepath.Clean(canonicalDir) && !rebuilding:
				return ErrSnapshotIndexNotAdvanced
			}
		}
		if rmErr := os.RemoveAll(canonicalDir); rmErr != nil {
			return rmErr
		}
	} else if !os.IsNotExist(statErr) {
		return statErr
	}
	return os.Rename(stageDir, canonicalDir)
}

// peerLogMarkers returns the published committed and processed markers; false when the node has no peer.
func peerLogMarkers(node *Node) (committed, processed uint64, ok bool) {
	if node.Peer() == nil {
		return 0, 0, false
	}
	committed, processed = node.LogMarkers()
	return committed, processed, true
}

// recoveryBlockedByLog reports why recovery to snapIndex must not proceed: behind the apply frontier, or not yet in the raft log.
func recoveryBlockedByLog(req snapshotRequest, snapIndex uint64) error {
	if applied := req.node.LastApplied(); snapIndex <= applied {
		return &SnapshotRecoveryStaleError{
			ShardID:       req.shardID,
			ReplicaID:     req.replicaID,
			SnapshotIndex: snapIndex,
			LastApplied:   applied,
		}
	}
	committed, processed, hasPeer := peerLogMarkers(req.node)
	if hasPeer && (committed < snapIndex || processed < snapIndex) {
		return &SnapshotRecoveryBehindError{
			ShardID:       req.shardID,
			ReplicaID:     req.replicaID,
			SnapshotIndex: snapIndex,
			Committed:     committed,
			Processed:     processed,
		}
	}
	return nil
}

// Bounds on how long a recovery waits for the snapshot to reach the raft log.
const (
	recoveryLogWaitInterval = 5 * time.Millisecond
	recoveryLogWaitBudget   = 5 * time.Second
)

// awaitRecoverableLog waits, without the recovering gate, until the log bounds allow the recovery, or returns the refusal.
func (p *snapshotPool) awaitRecoverableLog(req snapshotRequest) error {
	snapIndex := p.pendingRecoveryIndex(req)
	if snapIndex == 0 {
		return nil // nothing to recover; recoverSnapshot reports any read error
	}
	deadline := time.Now().Add(recoveryLogWaitBudget)
	timer := time.NewTimer(recoveryLogWaitInterval)
	defer timer.Stop()
	for {
		err := recoveryBlockedByLog(req, snapIndex)
		var behind *SnapshotRecoveryBehindError
		if !errors.As(err, &behind) {
			return err
		}
		if !time.Now().Before(deadline) {
			slog.Error("snapshot recovery abandoned: raft log never installed the snapshot",
				"shard_id", req.shardID,
				"replica_id", req.replicaID,
				"snapshot_index", snapIndex,
				"committed", behind.Committed,
				"processed", behind.Processed,
			)
			return err
		}
		select {
		case <-req.node.StopC():
			return err
		case <-p.stopC:
			return err
		case <-timer.C:
			timer.Reset(recoveryLogWaitInterval)
		}
	}
}

// pendingRecoveryIndex returns the snapshot index a recovery would restore, or 0; it reads only LogDB.
func (p *snapshotPool) pendingRecoveryIndex(req snapshotRequest) uint64 {
	if p.logdb == nil || req.node.Snapshotter() == nil {
		return 0
	}
	logdbSnap, err := p.logdb.GetSnapshot(req.shardID, req.replicaID)
	if err != nil {
		return 0 // recoverSnapshot reports the read failure
	}
	return logdbSnap.Index
}

// recoverSnapshot restores the state machine from the latest LogDB snapshot; failures before RecoverFromSnapshot are rejections, after it fail the shard.
func (p *snapshotPool) recoverSnapshot(req snapshotRequest) (err error) {
	restoring := false
	defer func() {
		if !restoring {
			// A pre-restore panic keeps unwinding to handleRequest; a
			// pre-restore error is a rejection.
			err = rejectBeforeRestore(err)
			return
		}
		if v := recover(); v != nil {
			stack := debug.Stack()
			err = &SMPanicError{
				ShardID:   req.shardID,
				ReplicaID: req.replicaID,
				Value:     v,
				Stack:     stack,
			}
			supervise.LogContained(func() {
				slog.Error("snapshot recovery panicked after the state machine restore began",
					"shard_id", req.shardID,
					"replica_id", req.replicaID,
					"panic", fmt.Sprint(v),
					"stack", string(stack),
				)
			})
		}
	}()
	node := req.node
	snapshotter := node.Snapshotter()
	if snapshotter == nil {
		return nil // nothing to recover (e.g. witness node)
	}

	// Get the latest snapshot metadata from LogDB.
	if p.logdb == nil {
		return nil
	}
	// Choose and pin the artifact under the snapshot-directory lock, unlocked by defer.
	logdbSnap, snapshotDir, artifactIndex, err := func() (logdb.Snapshot, string, uint64, error) {
		node.LockSnapshotDir()
		defer node.UnlockSnapshotDir()
		snap, dir, idx, chooseErr := p.chooseRecoverySnapshot(req)
		if chooseErr == nil && dir != "" {
			node.PinSnapshotDir(dir)
		}
		return snap, dir, idx, chooseErr
	}()
	if err != nil {
		return err
	}
	if snapshotDir == "" {
		return nil // no snapshot to recover
	}
	defer func() {
		node.LockSnapshotDir()
		node.UnpinSnapshotDir(snapshotDir)
		node.UnlockSnapshotDir()
	}()

	// Check both apply-frontier bounds before touching the snapshotter.
	if err := recoveryBlockedByLog(req, logdbSnap.Index); err != nil {
		return err
	}

	// Decrypt an encrypted snapshot to an unlinked scratch file and recover from it, leaving snapshot.dat untouched.
	dataPath := filepath.Join(snapshotDir, snapshotDataFile)

	// Verify the artifact against its snapshot.meta before anything reads it, failing closed; a missing snapshot.dat is reported first.
	if _, statErr := os.Stat(dataPath); os.IsNotExist(statErr) {
		return &SnapshotError{
			ShardID:   req.shardID,
			ReplicaID: req.replicaID,
			Save:      false,
			Err:       ErrSnapshotArtifactMissing,
		}
	}
	if verifyErr := verifyRecoveryArtifact(snapshotDir, dataPath, logdbSnap); verifyErr != nil {
		return &SnapshotError{
			ShardID:   req.shardID,
			ReplicaID: req.replicaID,
			Save:      false,
			Err:       verifyErr,
		}
	}

	// Sweep stale scratch files and retry the import-stage reclaim across the shard tree, fenced by the write lease the caller holds.
	SweepStaleDecryptTmpTree(node.SnapshotDir(), req.shardID, req.replicaID)
	SweepStaleDecryptTmp(snapshotDir)
	SweepStaleImportStageDirs(node.SnapshotDir(), req.shardID, req.replicaID)

	// An encrypted artifact with no usable barrier fails closed; the leader re-sends.
	barrier, barrierErr := p.barrierForShard(req.shardID)
	if barrierErr != nil {
		return &SnapshotError{
			ShardID:   req.shardID,
			ReplicaID: req.replicaID,
			Save:      false,
			Err:       barrierErr,
		}
	}
	if logdbSnap.Epoch > 0 && (barrier == nil || barrier.IsSealed()) {
		return &SnapshotError{
			ShardID:   req.shardID,
			ReplicaID: req.replicaID,
			Save:      false,
			Err:       ErrSnapshotEncryptedBarrierUnavailable,
		}
	}
	// Read plaintext through the open handle: an anonymous scratch for an encrypted artifact, in place for a plaintext one.
	var f SnapshotFile
	if logdbSnap.Epoch > 0 {
		scratch, decryptErr := p.decryptSnapshotToAnonymousScratch(
			node.ReplicaSnapshotDir(), dataPath, req.shardID, req.replicaID, artifactIndex, logdbSnap.Epoch, barrier)
		if decryptErr != nil {
			return &SnapshotError{
				ShardID:   req.shardID,
				ReplicaID: req.replicaID,
				Save:      false,
				Err:       decryptErr,
			}
		}
		f = scratch
	} else {
		// The engine.Snapshotter interface accepts io.Reader, and *os.File
		// satisfies io.Reader.
		plain, err := os.Open(dataPath)
		if err != nil {
			if os.IsNotExist(err) {
				// A recorded snapshot whose artifact is gone is a typed failure, so the leader re-sends.
				return &SnapshotError{
					ShardID:   req.shardID,
					ReplicaID: req.replicaID,
					Save:      false,
					Err:       ErrSnapshotArtifactMissing,
				}
			}
			return &SnapshotError{
				ShardID:   req.shardID,
				ReplicaID: req.replicaID,
				Save:      false,
				Err:       err,
			}
		}
		f = plain
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

	// The reader chain follows the snapshot record, not node.cfg.
	ctx := context.Background()
	r, readerErr := DecompressingSnapshotReader(f, logdbSnap.Compression)
	if readerErr != nil {
		return &SnapshotError{
			ShardID:   req.shardID,
			ReplicaID: req.replicaID,
			Save:      false,
			Err:       readerErr,
		}
	}
	// The restore begins: from here on the state machine may be partially
	// overwritten, and a panic fails the shard (see the deferred recover).
	restoring = true
	if err := snapshotter.RecoverFromSnapshot(ctx, r, node.stopC); err != nil {
		return &SnapshotError{
			ShardID:   req.shardID,
			ReplicaID: req.replicaID,
			Save:      false,
			Err:       err,
		}
	}

	// Update node tracking fields to reflect recovered state. lastAppliedTerm is
	// seeded from the snapshot's term so a snapshot taken after recovery but
	// before the next apply records a consistent (index, term).
	node.lastApplied.Store(logdbSnap.Index)
	node.lastAppliedTerm.Store(logdbSnap.Term)
	node.lastSnapshotIndex.Store(logdbSnap.Index)

	// Seed the snapshotter's own applied counter with the node markers.
	if setter, ok := snapshotter.(appliedSetter); ok {
		setter.SetLastApplied(logdbSnap.Index)
	}

	return nil
}

// chooseRecoverySnapshot resolves the directory a recovery reads and its index; empty when there is nothing to recover. The caller holds the snapshot-directory lock.
func (p *snapshotPool) chooseRecoverySnapshot(req snapshotRequest) (logdb.Snapshot, string, uint64, error) {
	logdbSnap, err := p.logdb.GetSnapshot(req.shardID, req.replicaID)
	switch {
	case errors.Is(err, logdb.ErrNoSnapshot):
		// No snapshot is a no-op, not a failure.
		return logdb.Snapshot{}, "", 0, nil
	case err != nil:
		return logdb.Snapshot{}, "", 0, &SnapshotError{
			ShardID:   req.shardID,
			ReplicaID: req.replicaID,
			Save:      false,
			Err:       err,
		}
	case logdbSnap.Index == 0:
		return logdb.Snapshot{}, "", 0, nil
	}
	// Prefer the filepath stored in LogDB metadata; fall back to discovering
	// the latest directory on disk.
	if logdbSnap.Filepath != "" {
		return logdbSnap, logdbSnap.Filepath, logdbSnap.Index, nil
	}
	dir, dirIndex, dirErr := latestSnapshotDir(req.node.SnapshotDir(), req.shardID, req.replicaID)
	if dirErr != nil {
		return logdb.Snapshot{}, "", 0, &SnapshotError{
			ShardID:   req.shardID,
			ReplicaID: req.replicaID,
			Save:      false,
			Err:       dirErr,
		}
	}
	return logdbSnap, dir, dirIndex, nil
}

// decryptSnapshotToAnonymousScratch decrypts path into an already-unlinked scratch file in scratchDir and returns it rewound; identity must match the sealing AAD.
func (p *snapshotPool) decryptSnapshotToAnonymousScratch(scratchDir, path string, shardID, replicaID, index, epoch uint64, barrier SnapshotBarrier) (SnapshotFile, error) {
	srcFile, err := os.Open(path)
	if err != nil {
		return nil, &SnapshotEncryptError{Op: "read", Err: err}
	}
	defer closeDecryptSource(srcFile, path, epoch)

	tmpFile, err := CreateAnonymousScratch(p.scratchFsys(), scratchDir, RecoverDecryptTempPattern)
	if err != nil {
		return nil, &SnapshotEncryptError{Op: "create_temp", Err: err}
	}
	handedOff := false
	defer func() {
		if handedOff {
			return
		}
		if closeErr := tmpFile.Close(); closeErr != nil && !errors.Is(closeErr, os.ErrClosed) {
			slog.Debug("snapshot: decrypt scratch close failed during cleanup",
				"epoch", epoch,
				"error", closeErr,
			)
		}
	}()

	// See decryptSnapshotToScratch for what each frame is authenticated
	// against.
	if _, err := OpenSnapshotFrames(barrier, shardID, replicaID, index, srcFile, tmpFile); err != nil {
		return nil, err
	}
	// No fsync: the file has no name, so nothing after a crash could read it
	// back. The recovery reads it through this handle.
	if _, err := tmpFile.Seek(0, io.SeekStart); err != nil {
		return nil, &SnapshotEncryptError{Op: "seek", Err: err}
	}
	handedOff = true // ownership of the handle passes to the caller
	return tmpFile, nil
}

// decryptSnapshotToScratch decrypts path into a named temp file in scratchDir for the export path; the caller removes it.
func (p *snapshotPool) decryptSnapshotToScratch(scratchDir, path string, shardID, replicaID, index, epoch uint64, barrier SnapshotBarrier) (string, error) {
	srcFile, err := os.Open(path)
	if err != nil {
		return "", &SnapshotEncryptError{Op: "read", Err: err}
	}
	defer closeDecryptSource(srcFile, path, epoch)

	tmpFile, err := os.CreateTemp(scratchDir, RecoverDecryptTempPattern)
	if err != nil {
		return "", &SnapshotEncryptError{Op: "create_temp", Err: err}
	}
	tmpPath := tmpFile.Name()
	committed := false
	defer func() {
		if !committed {
			if closeErr := tmpFile.Close(); closeErr != nil && !errors.Is(closeErr, os.ErrClosed) {
				slog.Debug("snapshot: temp file close failed during decryption cleanup",
					"path", tmpPath,
					"epoch", epoch,
					"error", closeErr,
				)
			}
			// The file may already hold plaintext, so a name left behind is
			// warned and counted rather than debug-logged.
			if removeErr := os.Remove(tmpPath); removeErr != nil && !os.IsNotExist(removeErr) {
				NoteScratchNotReclaimed(tmpPath, removeErr)
			}
		}
	}()

	// Each frame is authenticated against its position and identity and decrypts under its own header epoch.
	if _, err := OpenSnapshotFrames(barrier, shardID, replicaID, index, srcFile, tmpFile); err != nil {
		return "", err
	}

	if err := tmpFile.Sync(); err != nil {
		return "", &SnapshotEncryptError{Op: "sync", Err: err}
	}
	if err := tmpFile.Close(); err != nil {
		return "", &SnapshotEncryptError{Op: "close", Err: err}
	}
	committed = true // ownership of tmpPath passes to the caller
	return tmpPath, nil
}

// closeDecryptSource closes a decrypt's source ciphertext file. The ciphertext
// is the canonical artifact and stays in place, so a failed close costs a
// descriptor and nothing at rest; it is logged at debug.
func closeDecryptSource(srcFile *os.File, path string, epoch uint64) {
	if closeErr := srcFile.Close(); closeErr != nil {
		slog.Debug("snapshot: source file close failed during decryption",
			"path", path,
			"epoch", epoch,
			"error", closeErr,
		)
	}
}

// snapshotFloorBlocker names one snapshot directory that could not be
// evaluated, and why.
type snapshotFloorBlocker struct {
	path      string
	shardID   uint64
	replicaID uint64
	reason    string
}

// snapshotFloorScan folds retained snapshot directories into the minimum epoch they need, scanning all to report every unevaluable directory.
type snapshotFloorScan struct {
	// minEpoch is the lowest epoch any evaluable retained artifact still
	// needs, or 0 when no evaluable artifact is encrypted.
	minEpoch uint64

	// blockedCount counts every unevaluable directory; non-zero means nothing may be purged.
	blockedCount uint64

	// blockers is a bounded sample of blocking directories for the log.
	blockers []snapshotFloorBlocker
}

// maxReportedFloorBlockers bounds the blocking directories one log record names.
const maxReportedFloorBlockers = 8

// floorReportInterval bounds how often the purge-suppression warning is emitted.
const floorReportInterval = time.Minute

// lastFloorReport is when the suppression warning was last emitted, process-wide.
var lastFloorReport atomic.Int64

// floorReportDue reports whether enough time has passed to emit the warning
// again, claiming the slot when it has. The compare-and-swap makes concurrent
// scans (one per snapshot worker) yield exactly one record.
func floorReportDue() bool {
	now := time.Now().UnixNano()
	prev := lastFloorReport.Load()
	if prev != 0 && now-prev < int64(floorReportInterval) {
		return false
	}
	return lastFloorReport.CompareAndSwap(prev, now)
}

// addNode folds every retained snapshot directory of one node into the scan.
func (s *snapshotFloorScan) addNode(node *Node) {
	dirs, err := listSnapshotDirs(node.SnapshotDir(), node.ShardID(), node.ReplicaID())
	if err != nil {
		// Cannot enumerate this shard's snapshots: which epochs it still
		// references is unknown, so the floor is unusable.
		s.block(node.SnapshotDir(), node, "snapshot_directory_scan_failed")
		return
	}
	for _, d := range dirs {
		epoch, status := readSnapshotEpoch(d.path)
		switch status {
		case snapshotEpochKnown:
			if epoch == 0 {
				continue // unencrypted (or GC'd) snapshot, contributes no floor
			}
			if s.minEpoch == 0 || epoch < s.minEpoch {
				s.minEpoch = epoch
			}
		default:
			s.block(d.path, node, status.String())
		}
	}
}

// block counts an unevaluable directory and samples up to maxReportedFloorBlockers.
func (s *snapshotFloorScan) block(path string, node *Node, reason string) {
	s.blockedCount++
	if len(s.blockers) < maxReportedFloorBlockers {
		s.blockers = append(s.blockers, snapshotFloorBlocker{
			path:      path,
			shardID:   node.ShardID(),
			replicaID: node.ReplicaID(),
			reason:    reason,
		})
	}
}

// blocked reports whether any directory could not be evaluated.
func (s *snapshotFloorScan) blocked() bool {
	return s.blockedCount > 0
}

// report logs the blocking directories as one rate-limited record with the full count.
func (s *snapshotFloorScan) report(scope string) {
	if !s.blocked() {
		return
	}
	if !floorReportDue() {
		return
	}
	paths := make([]string, 0, len(s.blockers))
	for _, b := range s.blockers {
		paths = append(paths, fmt.Sprintf("%s (shard=%d replica=%d reason=%s)",
			b.path, b.shardID, b.replicaID, b.reason))
	}
	slog.Warn("epoch purge suppressed: snapshot directories with an indeterminate epoch",
		"scope", scope,
		"blocking_directories", s.blockedCount,
		"sample", paths,
	)
}

// globalSafePurgeFloor returns the minimum epoch any shard's retained snapshot or the WAL still needs; when ok is false, purge nothing.
func (p *snapshotPool) globalSafePurgeFloor() (uint64, bool) {
	// Scan every loaded shard's snapshot directories for the global minimum
	// retained encrypted-snapshot epoch. A nil enumerator (standalone tests)
	// means no global view is available and purging must not proceed.
	if p.nodes == nil {
		return 0, false
	}

	nodes := p.nodes()

	// Count stopped shards' recorded snapshots from the LogDB, which no scan of loaded bases sees.
	unloadedMin, ok := p.unloadedSnapshotFloor(nodes, func(uint64) (bool, error) { return true, nil })
	if !ok {
		return 0, false
	}

	var scan snapshotFloorScan
	for _, node := range nodes {
		scan.addNode(node)
	}
	if scan.blocked() {
		// Fail safe: at least one directory's epoch is indeterminate, so we
		// cannot know which DEKs are still referenced.
		scan.report("global")
		return 0, false
	}
	snapshotMinEpoch := minNonZero(scan.minEpoch, unloadedMin)

	// Determine the global WAL floor. The LogDB is per-host, so MinLiveEpoch
	// already spans all shards (verified: waldb.DB.MinLiveEpoch ranges all
	// shards). A query error is fail-safe: skip purge entirely.
	var walMinEpoch uint64
	if epochDB, ok := p.logdb.(logdb.EpochAwareLogDB); ok {
		var walErr error
		walMinEpoch, walErr = epochDB.MinLiveEpoch()
		if walErr != nil {
			slog.Warn("epoch purge skipped: WAL epoch query failed",
				"error", walErr,
			)
			return 0, false
		}
	} else if p.logdb != nil {
		// A LogDB without an epoch floor blocks the purge unless it declares AtRestNonDurable; a nil LogDB contributes 0.
		if protection := engineLogDBAtRestProtection(p.logdb); protection != logdb.AtRestNonDurable {
			slog.Warn("epoch purge suppressed: LogDB reports no epoch floor and does not "+
				"declare its records non-durable; a purge could destroy a DEK the log still needs",
				"at_rest_protection", protection.String(),
			)
			return 0, false
		}
	}

	// Combine the two floors. Either may be zero (no encrypted snapshots, or
	// no encrypted WAL records). The global floor is the minimum of the
	// non-zero values; if both are zero nothing encrypted is retained.
	floor := snapshotMinEpoch
	if walMinEpoch > 0 && (floor == 0 || walMinEpoch < floor) {
		floor = walMinEpoch
	}
	if floor == 0 {
		return 0, false // nothing encrypted retained, nothing to purge
	}
	return floor, true
}

// engineLogDBAtRestProtection reports a LogDB's declared at-rest guarantee; no declaration reads as unprotected.
func engineLogDBAtRestProtection(db logdb.LogDB) logdb.AtRestProtection {
	p, ok := db.(logdb.AtRestProtectedLogDB)
	if !ok {
		return logdb.AtRestUnprotected
	}
	return p.AtRestProtection()
}

// unloadedSnapshotFloor returns the minimum recorded snapshot epoch of unloaded shards include accepts; ok is false on any doubt.
func (p *snapshotPool) unloadedSnapshotFloor(loaded []*Node,
	include func(shardID uint64) (bool, error)) (uint64, bool) {
	if p.logdb == nil {
		return 0, true
	}
	infos, err := p.logdb.ListNodeInfo()
	if err != nil {
		slog.Warn("epoch purge skipped: the LogDB could not enumerate its shards, "+
			"so a stopped shard's snapshot epoch cannot be accounted",
			"error", err,
		)
		return 0, false
	}
	type replicaKey struct{ shardID, replicaID uint64 }
	isLoaded := make(map[replicaKey]struct{}, len(loaded))
	for _, node := range loaded {
		isLoaded[replicaKey{node.ShardID(), node.ReplicaID()}] = struct{}{}
	}
	var floor uint64
	for _, info := range infos {
		if _, ok := isLoaded[replicaKey{info.ShardID, info.ReplicaID}]; ok {
			continue // scanned from its snapshot tree by the caller
		}
		counted, incErr := include(info.ShardID)
		if incErr != nil {
			return 0, false
		}
		if !counted {
			continue
		}
		snap, snapErr := p.logdb.GetSnapshot(info.ShardID, info.ReplicaID)
		if errors.Is(snapErr, logdb.ErrNoSnapshot) {
			continue // nothing recorded, nothing to strand
		}
		if snapErr != nil {
			slog.Warn("epoch purge skipped: a stopped shard's snapshot record could not be read",
				"shard_id", info.ShardID,
				"replica_id", info.ReplicaID,
				"error", snapErr,
			)
			return 0, false
		}
		floor = minNonZero(floor, snap.Epoch)
	}
	return floor, true
}

// minNonZero returns the smaller of a and b, treating 0 as "no constraint".
func minNonZero(a, b uint64) uint64 {
	switch {
	case a == 0:
		return b
	case b == 0:
		return a
	case a < b:
		return a
	default:
		return b
	}
}

// perBarrierSafePurgeFloor returns the minimum epoch the snapshots and WAL records of one barrier still need; when ok is false, purge nothing.
func (p *snapshotPool) perBarrierSafePurgeFloor(target SnapshotBarrier) (uint64, bool) {
	if p.nodes == nil || target == nil {
		return 0, false
	}
	if !logdb.SameBarrier(target, target) {
		return 0, false // no pointer identity
	}

	// One snapshot of the loaded shards mapped to the target, indexed by shard ID.
	allLoaded := p.nodes()
	loadedForTarget := make(map[uint64]*Node)
	for _, node := range allLoaded {
		uses, barrierErr := p.shardUsesBarrier(node.ShardID(), target)
		if barrierErr != nil {
			// A loaded shard whose barrier cannot be resolved or identified blocks the floor.
			return 0, false
		}
		if !uses {
			continue // belongs to a different barrier; do not let it lower/raise this floor
		}
		loadedForTarget[node.ShardID()] = node
	}

	// When the LogDB enumerates the barrier's complete shard set, every one must be loaded; otherwise fall back to the loaded shards.
	if enum, ok := p.logdb.(logdb.BarrierShardEnumerator); ok {
		if wantShards, known := enum.ShardsForBarrier(target); known {
			for _, sid := range wantShards {
				if _, present := loadedForTarget[sid]; !present {
					return 0, false // a mapped shard is not loaded: fail safe
				}
			}
		}
	}

	// Count unloaded shards mapped to the target; an unresolvable one declines the purge.
	unloadedMin, ok := p.unloadedSnapshotFloor(allLoaded, func(shardID uint64) (bool, error) {
		return p.shardUsesBarrier(shardID, target)
	})
	if !ok {
		return 0, false
	}

	// (a) Snapshot floor over the target barrier's shards only.
	var scan snapshotFloorScan
	for _, node := range loadedForTarget {
		scan.addNode(node)
	}
	if scan.blocked() {
		scan.report("barrier")
		return 0, false // fail safe: this barrier has an indeterminate directory
	}
	snapshotMinEpoch := minNonZero(scan.minEpoch, unloadedMin)

	// (b) WAL floor over the target barrier's records; a LogDB without per-barrier queries fails safe, a nil LogDB contributes 0.
	var walMinEpoch uint64
	if p.logdb != nil {
		perBarrierDB, ok := p.logdb.(logdb.PerBarrierEpochAwareLogDB)
		if !ok {
			return 0, false // cannot determine this barrier's WAL floor: fail safe
		}
		epoch, known, walErr := perBarrierDB.MinLiveEpochForBarrier(target)
		if walErr != nil {
			slog.Warn("per-barrier epoch purge skipped: WAL epoch query failed",
				"error", walErr,
			)
			return 0, false
		}
		if !known {
			// The LogDB cannot attribute this barrier, so do not purge.
			return 0, false
		}
		// known == true with epoch == 0 is a true non-constraint: the barrier is
		// owned but has zero live encrypted WAL records, so the WAL imposes no
		// floor (the snapshot floor below still applies).
		walMinEpoch = epoch
	}

	floor := snapshotMinEpoch
	if walMinEpoch > 0 && (floor == 0 || walMinEpoch < floor) {
		floor = walMinEpoch
	}
	if floor == 0 {
		return 0, false // nothing encrypted retained for this barrier
	}
	return floor, true
}

// shardUsesBarrier reports whether shardID's barrier is target by pointer identity; a barrier without identity returns errBarrierWithoutIdentity.
func (p *snapshotPool) shardUsesBarrier(shardID uint64, target SnapshotBarrier) (bool, error) {
	b, err := p.barrierForShard(shardID)
	if err != nil {
		return false, err
	}
	if !logdb.SameBarrier(b, b) {
		return false, errBarrierWithoutIdentity
	}
	return logdb.SameBarrier(b, target), nil
}

// carryBootstrapForward rewrites the bootstrap record under the current epoch before log removal, so old WAL segments and epochs can be reclaimed.
func (p *snapshotPool) carryBootstrapForward(shardID, replicaID uint64) {
	if p.logdb == nil || (p.barrier == nil && p.shardBarriers == nil) {
		return
	}
	bootstrap, ok, err := p.logdb.GetBootstrap(shardID, replicaID)
	if err == nil && ok {
		err = p.logdb.SaveBootstrap(shardID, replicaID, bootstrap)
	}
	if err != nil {
		slog.Warn("bootstrap carry-forward failed; the WAL epoch floor stays put",
			"shard_id", shardID,
			"replica_id", replicaID,
			"error", err,
		)
	}
}

// purgeOldEpochs purges DEKs below the global floor, or below the shard's barrier floor in per-shard mode; an unevaluable directory blocks its barrier's purge.
func (p *snapshotPool) purgeOldEpochs(_ string, shardID, replicaID uint64) {
	if p.shardBarriers == nil {
		// Original global single-barrier path, unchanged.
		if p.barrier == nil || p.barrier.IsSealed() {
			return
		}
		purgeFloor, ok := p.globalSafePurgeFloor()
		if !ok {
			return // fail safe: floor unknown or nothing encrypted retained
		}
		p.applyPurge(p.barrier, purgeFloor, shardID, replicaID)
		return
	}

	// Per-shard-barrier path: dispatch to the shard's own barrier. An
	// unresolved barrier has no keyring to purge from.
	barrier, barrierErr := p.barrierForShard(shardID)
	if barrierErr != nil || barrier == nil || barrier.IsSealed() {
		return
	}
	purgeFloor, ok := p.perBarrierSafePurgeFloor(barrier)
	if !ok {
		return // fail safe: floor unknown or nothing encrypted retained for this barrier
	}
	p.applyPurge(barrier, purgeFloor, shardID, replicaID)
}

// lifetimeContext returns a context cancelled when stopC closes; the caller must call cancel.
func (p *snapshotPool) lifetimeContext() (context.Context, context.CancelFunc) {
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		select {
		case <-p.stopC:
			cancel()
		case <-ctx.Done():
		}
	}()
	return ctx, cancel
}

// applyPurge invokes PurgeEpochsBefore on the given barrier when the floor is
// above the initial epoch. Epoch 1 has no predecessor to purge. Shared by the
// global and per-barrier purge paths so both log and guard identically.
func (p *snapshotPool) applyPurge(barrier SnapshotBarrier, purgeFloor, shardID, replicaID uint64) {
	if purgeFloor <= 1 {
		return
	}
	ctx, cancel := p.lifetimeContext()
	defer cancel()
	purged, purgeErr := barrier.PurgeEpochsBefore(ctx, purgeFloor)
	if purgeErr != nil && ctx.Err() != nil && errors.Is(purgeErr, ctx.Err()) {
		// Cancelled by stop before anything was persisted; the next save purges again.
		slog.Debug("epoch purge abandoned: snapshot pool is stopping",
			"shard_id", shardID,
			"replica_id", replicaID,
			"error", purgeErr,
		)
		return
	}
	if purgeErr != nil {
		slog.Warn("epoch purge failed",
			"shard_id", shardID,
			"replica_id", replicaID,
			"purge_floor", purgeFloor,
			"error", purgeErr,
		)
	} else if purged > 0 {
		slog.Info("snapshot epoch purge completed",
			"shard_id", shardID,
			"replica_id", replicaID,
			"purged", purged,
			"purge_floor", purgeFloor,
		)
	}
}

// Snapshot scratch-file prefixes: every scratch writer must use one so SweepStaleScratchTmp can reclaim it.
const (
	// ScratchTempSuffix terminates every snapshot scratch file name.
	ScratchTempSuffix = ".tmp"

	// DecryptTempSuffix is the historical name for ScratchTempSuffix, kept
	// because the decrypt patterns below read more clearly with it.
	DecryptTempSuffix = ScratchTempSuffix

	// RecoverDecryptTempPrefix marks a recovery decrypt scratch file; holds plaintext.
	RecoverDecryptTempPrefix = ".decrypt-"

	// SendDecryptTempPrefix marks a send decrypt scratch file; holds plaintext.
	SendDecryptTempPrefix = ".send-decrypt-"

	// RecvEncryptTempPrefix marks a re-encryption scratch file, kept so orphans are swept; holds ciphertext.
	RecvEncryptTempPrefix = ".recv-encrypt-"

	// ReencryptTempPrefix marks a barrier-to-barrier migration scratch file; holds ciphertext.
	ReencryptTempPrefix = ".reencrypt-snapshot-"

	// SaveEncryptTempPrefix marks a save encrypt scratch file; holds ciphertext.
	SaveEncryptTempPrefix = ".encrypt-"

	// SaveEncryptTempPattern is the os.CreateTemp pattern for that scratch file.
	SaveEncryptTempPattern = SaveEncryptTempPrefix + "*" + ScratchTempSuffix

	// RecoverDecryptTempPattern is the os.CreateTemp pattern for a recovery
	// decrypt scratch file.
	RecoverDecryptTempPattern = RecoverDecryptTempPrefix + "*" + ScratchTempSuffix

	// SendDecryptTempPattern is the os.CreateTemp pattern for a send decrypt
	// scratch file.
	SendDecryptTempPattern = SendDecryptTempPrefix + "*" + ScratchTempSuffix

	// RecvEncryptTempPattern is the os.CreateTemp pattern for an in-place
	// re-encryption scratch file.
	RecvEncryptTempPattern = RecvEncryptTempPrefix + "*" + ScratchTempSuffix

	// ReencryptTempPattern is the os.CreateTemp pattern for a barrier-to-barrier
	// migration scratch file.
	ReencryptTempPattern = ReencryptTempPrefix + "*" + ScratchTempSuffix
)

// snapshotScratchTempPrefixes is the complete set of scratch-file prefixes any
// snapshot path may create. The matcher and the writers read the same list, so
// the two cannot drift.
var snapshotScratchTempPrefixes = [...]string{
	RecoverDecryptTempPrefix,
	SendDecryptTempPrefix,
	RecvEncryptTempPrefix,
	ReencryptTempPrefix,
	SaveEncryptTempPrefix,
}

// IsSnapshotScratchTemp reports whether name is a snapshot scratch file; the sweeper's single matcher.
func IsSnapshotScratchTemp(name string) bool {
	if !strings.HasSuffix(name, ScratchTempSuffix) {
		return false
	}
	for _, prefix := range snapshotScratchTempPrefixes {
		if strings.HasPrefix(name, prefix) {
			return true
		}
	}
	return false
}

// Import staging-directory naming: the stage holds plaintext and is invisible to listSnapshotDirs, so it must be swept.
const (
	// ImportStageDirPrefix marks a directory an import stages its artifact in.
	// The leading dot and the "import" word make an orphan self-identifying,
	// and the name deliberately cannot parse as "snapshot-<index>".
	ImportStageDirPrefix = ".import-"

	// ImportStageDirPattern is the os.MkdirTemp pattern for a staging
	// directory. Every import must build its name from this.
	ImportStageDirPattern = ImportStageDirPrefix + "*"
)

// IsSnapshotImportStage reports whether name is an import staging directory; the sweeper's single matcher.
func IsSnapshotImportStage(name string) bool {
	return strings.HasPrefix(name, ImportStageDirPrefix)
}

// Receive staging-directory naming: a receive stages and publishes by rename; only StartShard may sweep these.
const (
	// RecvStageDirPrefix marks a receive staging directory; it never parses as snapshot-<index>.
	RecvStageDirPrefix = ".recv-"

	// RecvStageDirPattern is the os.MkdirTemp pattern for a receive staging
	// directory. Every receive must build its name from this.
	RecvStageDirPattern = RecvStageDirPrefix + "*"
)

// IsSnapshotRecvStage reports whether name is a receive staging directory; the sweeper's single matcher.
func IsSnapshotRecvStage(name string) bool {
	return strings.HasPrefix(name, RecvStageDirPrefix)
}

// SweepStaleRecvStageDirs removes orphaned receive staging directories; call only from StartShard, errors are logged.
func SweepStaleRecvStageDirs(baseDir string, shardID, replicaID uint64) {
	sweepStageDirs(baseDir, shardID, replicaID, IsSnapshotRecvStage, "InstallSnapshot receive")
}

// Save staging-directory naming: a save stages and publishes by rename under the lease; only StartShard may sweep these.
const (
	// SaveStageDirPrefix marks a save staging directory; it never parses as snapshot-<index>.
	SaveStageDirPrefix = ".save-"

	// SaveStageDirPattern is the os.MkdirTemp pattern for a save staging
	// directory. Every save must build its name from this.
	SaveStageDirPattern = SaveStageDirPrefix + "*"
)

// IsSnapshotSaveStage reports whether name is a save staging directory; the sweeper's single matcher.
func IsSnapshotSaveStage(name string) bool {
	return strings.HasPrefix(name, SaveStageDirPrefix)
}

// MkdirStageDir creates a staging directory like os.MkdirTemp but with mode 0750 under the umask.
func MkdirStageDir(parent, pattern string) (string, error) {
	prefix, suffix := pattern, ""
	if i := strings.LastIndex(pattern, "*"); i >= 0 {
		prefix, suffix = pattern[:i], pattern[i+1:]
	}
	for range 10000 {
		dir := filepath.Join(parent, prefix+strconv.FormatUint(uint64(rand.Uint32()), 10)+suffix)
		err := os.Mkdir(dir, 0o750)
		if err == nil {
			return dir, nil
		}
		if !os.IsExist(err) {
			return "", err
		}
	}
	return "", &os.PathError{Op: "mkdirstage", Path: filepath.Join(parent, pattern), Err: os.ErrExist}
}

// SweepStaleSaveStageDirs removes crash-orphaned save staging directories; call only from StartShard.
func SweepStaleSaveStageDirs(baseDir string, shardID, replicaID uint64) {
	sweepStageDirs(baseDir, shardID, replicaID, IsSnapshotSaveStage, "snapshot save")
}

// sweepStageDirs removes every directory in a shard/replica's snapshot tree
// whose name the matcher claims, logging each reclaim under what. Failures are
// logged and counted, never returned: a failed cleanup must not abort startup.
func sweepStageDirs(baseDir string, shardID, replicaID uint64, match func(string) bool, what string) {
	base := snapshotBaseDir(baseDir, shardID, replicaID)
	entries, err := os.ReadDir(base)
	if err != nil {
		if !os.IsNotExist(err) {
			noteStaleDecryptSweepFailure(base, "read_dir", err)
		}
		return
	}
	for _, entry := range entries {
		if !entry.IsDir() || !match(entry.Name()) {
			continue
		}
		stalePath := filepath.Join(base, entry.Name())
		if rmErr := os.RemoveAll(stalePath); rmErr != nil && !os.IsNotExist(rmErr) {
			noteStaleDecryptSweepFailure(stalePath, "remove_all", rmErr)
			continue
		}
		slog.Warn("snapshot: reclaimed an orphaned "+what+" staging directory",
			"path", stalePath,
			"shard_id", shardID,
			"replica_id", replicaID,
		)
	}
}

// SweepStaleDecryptTmpTree removes orphaned scratch files from every retained snapshot directory of the replica; errors are logged.
func SweepStaleDecryptTmpTree(baseDir string, shardID, replicaID uint64) {
	// The replica's base directory first, where a send decrypts.
	SweepStaleDecryptTmp(snapshotBaseDir(baseDir, shardID, replicaID))
	dirs, err := listSnapshotDirs(baseDir, shardID, replicaID)
	if err != nil {
		slog.Debug("snapshot: stale decrypt tmp tree sweep: list failed",
			"base_dir", baseDir,
			"shard_id", shardID,
			"replica_id", replicaID,
			"error", err,
		)
		return
	}
	for _, d := range dirs {
		SweepStaleDecryptTmp(d.path)
	}
}

// SweepStaleImportStageDirs removes orphaned plaintext import staging directories; the caller ensures no import is in flight, errors are logged.
func SweepStaleImportStageDirs(baseDir string, shardID, replicaID uint64) {
	sweepStageDirs(baseDir, shardID, replicaID, IsSnapshotImportStage,
		"import (it held the imported state machine in PLAINTEXT)")
}

// SweepStaleDecryptTmp removes orphaned scratch files from one snapshot directory; errors are logged.
func SweepStaleDecryptTmp(dir string) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		if !os.IsNotExist(err) {
			// The sweep could not even look. Whether a plaintext copy is
			// sitting there is now unknown, which is the same operational
			// state as a failed removal: report it the same way.
			noteStaleDecryptSweepFailure(dir, "read_dir", err)
		}
		return
	}
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		name := entry.Name()
		if !IsSnapshotScratchTemp(name) {
			continue
		}
		stalePath := filepath.Join(dir, name)
		if rmErr := sweepRemove(stalePath); rmErr != nil && !os.IsNotExist(rmErr) {
			noteStaleDecryptSweepFailure(stalePath, "remove", rmErr)
		}
	}
}

// sweepRemove unlinks a swept path; a variable only so tests can fail it.
var sweepRemove = os.Remove

// staleDecryptSweepFailures counts failed scratch or staging reclaims; any increase may mean plaintext at rest.
var staleDecryptSweepFailures atomic.Uint64

// StaleDecryptSweepFailures returns the number of scratch files and staging directories this process failed to reclaim.
func StaleDecryptSweepFailures() uint64 {
	return staleDecryptSweepFailures.Load()
}

// noteStaleDecryptSweepFailure records a failed sweep at WARN.
func noteStaleDecryptSweepFailure(path, op string, err error) {
	staleDecryptSweepFailures.Add(1)
	slog.Warn("snapshot: scratch path not reclaimed; plaintext state machine data may remain at rest",
		"path", path,
		"op", op,
		"error", err,
		"total_failures", staleDecryptSweepFailures.Load(),
	)
}

// NoteScratchNotReclaimed records a decrypt scratch file that could not be
// unlinked — by CreateAnonymousScratch, for this package's recovery and the
// host's recovery and send — on the same counter and log as a failed sweep.
func NoteScratchNotReclaimed(path string, err error) {
	noteStaleDecryptSweepFailure(path, "remove", err)
}

// setCallback replaces the engine callback. Used by Engine.SetCallback
// to propagate the callback to the snapshot pool.
func (p *snapshotPool) setCallback(callback Callback) {
	p.callback = callback
}

// requestSnapshot submits a snapshot request and reports a refusal through OnSnapshotRejected as well as the returned error.
func (p *snapshotPool) requestSnapshot(req snapshotRequest) error {
	err := p.submit(req)
	if err != nil && p.callback != nil {
		p.callback.OnSnapshotRejected(req.shardID, req.replicaID, err)
	}
	return err
}

// submit admits req or returns *SnapshotDuplicateError or *SnapshotPoolBusyError, reporting nothing.
func (p *snapshotPool) submit(req snapshotRequest) error {
	// Per-shard dedup: only one snapshot per shard in-flight.
	if !p.claimInFlight(req) {
		return &SnapshotDuplicateError{ShardID: req.shardID}
	}
	select {
	case p.requestC <- req:
		// Shutdown may have raced the send: re-check and drain so no request keeps its slot.
		select {
		case <-p.stopC:
			p.drainRequests()
		default:
		}
		return nil
	default:
		// Request channel full. Clear the in-flight flag and let
		// the caller retry.
		p.releaseInFlight(req)
		return &SnapshotPoolBusyError{ShardID: req.shardID}
	}
}

// requestRecovery submits a recovery request for node for a snapshot receive; the caller holds the snapshotting flag.
func (p *snapshotPool) requestRecovery(shardID, replicaID uint64, node *Node) error {
	return p.requestSnapshot(snapshotRequest{
		shardID:   shardID,
		replicaID: replicaID,
		node:      node,
		save:      false,
	})
}

// claimInFlight claims the shard's single-flight slot for req's node, taking over one held by a stopped incarnation.
func (p *snapshotPool) claimInFlight(req snapshotRequest) bool {
	for {
		prev, loaded := p.inFlight.LoadOrStore(req.shardID, req.node)
		if !loaded {
			return true
		}
		owner, ok := prev.(*Node)
		if !ok || owner == nil || owner == req.node || !owner.IsStopped() {
			return false
		}
		if p.inFlight.CompareAndSwap(req.shardID, prev, req.node) {
			return true
		}
		// The slot changed under us (released, or taken over by another
		// claimant); look again.
	}
}

// releaseInFlight releases the shard's single-flight slot if req's node still
// holds it. A slot taken over by a later incarnation is left alone.
func (p *snapshotPool) releaseInFlight(req snapshotRequest) {
	p.inFlight.CompareAndDelete(req.shardID, req.node)
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

// SnapshotSaveDir is snapshotSaveDir for callers outside the engine; every artifact writer must use it.
func SnapshotSaveDir(baseDir string, shardID, replicaID, index uint64) string {
	return snapshotSaveDir(baseDir, shardID, replicaID, index)
}

// ReplicaSnapshotDir returns the replica's own snapshot directory under baseDir, where plaintext scratch files are created and swept.
func ReplicaSnapshotDir(baseDir string, shardID, replicaID uint64) string {
	return snapshotBaseDir(baseDir, shardID, replicaID)
}

// snapshotBaseDir returns the per-shard/replica base directory.
func snapshotBaseDir(baseDir string, shardID, replicaID uint64) string {
	return filepath.Join(
		baseDir,
		fmt.Sprintf("shard-%d", shardID),
		fmt.Sprintf("replica-%d", replicaID),
	)
}

// DecompressingSnapshotReader wraps r in the decompressor the artifact was written through; an unknown algorithm returns ErrUnsupportedSnapshotCompression.
func DecompressingSnapshotReader(r io.Reader, c config.CompressionType) (io.Reader, error) {
	switch c {
	case config.NoCompression:
		return r, nil
	case config.Snappy:
		return snappy.NewReader(r), nil
	default:
		return nil, ErrUnsupportedSnapshotCompression
	}
}

// writeSnapshotMetadata writes the metadata record to the snapshot directory; the layout is documented on MarshalSnapshotMeta.
func writeSnapshotMetadata(dir string, index, term, shardID, replicaID, epoch uint64,
	compression config.CompressionType) error {
	// Stamp the integrity fields over the data file as written; a hash error fails the save before anything is recorded.
	dataHash, dataLen, err := stampSnapshotHash(filepath.Join(dir, snapshotDataFile))
	if err != nil {
		return err
	}
	metaPath := filepath.Join(dir, snapshotMetadataFile)
	f, err := os.OpenFile(metaPath, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0o600)
	if err != nil {
		return err
	}
	// closed stops the defer from double-closing; a Close error on the write handle is returned like any write error.
	closed := false
	defer func() {
		if closed {
			return
		}
		if closeErr := f.Close(); closeErr != nil {
			slog.Debug("snapshot: metadata file close failed after write error",
				"path", metaPath,
				"shard_id", shardID,
				"replica_id", replicaID,
				"error", closeErr,
			)
		}
	}()

	buf := MarshalSnapshotMeta(SnapshotMeta{
		Index:       index,
		Term:        term,
		ShardID:     shardID,
		ReplicaID:   replicaID,
		Epoch:       epoch,
		Compression: compression,
		DataLen:     dataLen,
		DataHash:    dataHash,
	})
	if _, err := f.Write(buf); err != nil {
		return err
	}
	if err := f.Sync(); err != nil {
		return err
	}
	closed = true
	return f.Close()
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
		// "No snapshot on disk" is a storage-state answer, not a shutdown
		// signal. Reporting ErrStopped here would conflate the two;
		// ErrSnapshotNotFound keeps them distinct per the typed-errors convention.
		return "", 0, &SnapshotError{
			ShardID:   shardID,
			ReplicaID: replicaID,
			Save:      false,
			Err:       ErrSnapshotNotFound,
		}
	}
	latest := dirs[len(dirs)-1]
	return latest.path, latest.index, nil
}

// exportSnapshotBackup writes a plaintext copy of a just-saved snapshot to exportPath, decrypting an encrypted artifact first; no usable barrier fails closed.
func (p *snapshotPool) exportSnapshotBackup(saveDir, exportPath string, barrier SnapshotBarrier, meta SnapshotMeta) error {
	if meta.Epoch == 0 {
		return exportSnapshotToPath(saveDir, exportPath)
	}
	if barrier == nil || barrier.IsSealed() {
		return &SnapshotExportError{
			ExportPath: filepath.Clean(exportPath),
			Err:        ErrSnapshotBarrierSealed,
		}
	}
	dataPath := filepath.Join(saveDir, snapshotDataFile)
	plainPath, decErr := p.decryptSnapshotToScratch(
		saveDir, dataPath, meta.ShardID, meta.ReplicaID, meta.Index, meta.Epoch, barrier)
	if decErr != nil {
		return &SnapshotExportError{
			ExportPath: filepath.Clean(exportPath),
			Err:        decErr,
		}
	}
	defer func() {
		if rmErr := os.Remove(plainPath); rmErr != nil && !os.IsNotExist(rmErr) {
			NoteScratchNotReclaimed(plainPath, rmErr)
		}
	}()
	// The export is plaintext: Epoch 0, compression kept, integrity fields stamped over the decrypted bytes.
	exportMeta := meta
	exportMeta.Epoch = 0
	sum, size, hashErr := stampSnapshotHash(plainPath)
	if hashErr != nil {
		return &SnapshotExportError{
			ExportPath: filepath.Clean(exportPath),
			Err:        hashErr,
		}
	}
	exportMeta.DataHash, exportMeta.DataLen = sum, size
	return writeSnapshotExport(plainPath, exportPath, MarshalSnapshotMeta(exportMeta))
}

// exportSnapshotToPath copies a plaintext artifact and its required snapshot.meta to exportPath; encrypted artifacts go through exportSnapshotBackup.
func exportSnapshotToPath(saveDir, exportPath string) error {
	srcDataPath := filepath.Join(saveDir, snapshotDataFile)
	metaBytes, readErr := readBoundedSnapshotMeta(filepath.Join(saveDir, snapshotMetadataFile))
	if readErr != nil {
		return &SnapshotExportError{
			ExportPath: filepath.Clean(exportPath),
			Err:        readErr,
		}
	}
	return writeSnapshotExport(srcDataPath, exportPath, metaBytes)
}

// exportTempPattern is the os.CreateTemp pattern each exported file is staged
// under before it is renamed onto its final name in the export directory.
const exportTempPattern = ".snapshot-export-*.tmp"

// writeSnapshotExport writes snapshot.dat and snapshot.meta to exportPath atomically and durably, keeping any previous export until the new pair is published.
func writeSnapshotExport(srcDataPath, exportPath string, metaBytes []byte) error {
	// Clean the export path to prevent directory traversal. The public
	// SnapshotOption.Validate() already ensures it is an absolute path.
	exportPath = filepath.Clean(exportPath)
	fail := func(err error) error {
		return &SnapshotExportError{ExportPath: exportPath, Err: err}
	}
	if len(metaBytes) == 0 {
		return fail(ErrSnapshotMetaCorrupt)
	}
	// Recorded before MkdirAll: the final fsync covers every directory it creates.
	syncRoot := fsutil.DeepestExistingAncestor(exportPath)
	if err := os.MkdirAll(exportPath, 0o750); err != nil {
		return fail(err)
	}

	src, err := os.Open(srcDataPath)
	if err != nil {
		return fail(err)
	}
	defer func() {
		if closeErr := src.Close(); closeErr != nil {
			slog.Debug("snapshot export: source file close failed",
				"path", srcDataPath,
				"error", closeErr,
			)
		}
	}()

	// Every staged temp not renamed onto its final name is removed on the way
	// out, so a failed export leaves no half-written file behind. One that
	// publication already renamed is simply gone.
	var pending []string
	defer func() {
		for _, tmp := range pending {
			if rmErr := os.Remove(tmp); rmErr != nil && !os.IsNotExist(rmErr) {
				slog.Warn("snapshot export: failed to remove a staged temp file",
					"path", tmp,
					"error", rmErr,
				)
			}
		}
	}()

	dataTmp, err := writeExportTemp(exportPath, src)
	if dataTmp != "" {
		pending = append(pending, dataTmp)
	}
	if err != nil {
		return fail(err)
	}
	metaTmp, err := writeExportTemp(exportPath, bytes.NewReader(metaBytes))
	if metaTmp != "" {
		pending = append(pending, metaTmp)
	}
	if err != nil {
		return fail(err)
	}

	if err := publishSnapshotExport(exportPath, dataTmp, metaTmp, syncRoot); err != nil {
		return fail(err)
	}
	pending = nil
	return nil
}

// exportBackupPattern is the os.MkdirTemp pattern of the directory, inside the
// export directory, that a previous export's pair is moved into while a new
// pair is published over it.
const exportBackupPattern = ".snapshot-export-prev-*"

// exportRename and exportSyncDir are variables only so tests can fail a step; production code must never assign to them.
var (
	exportRename  = os.Rename
	exportSyncDir = fsutil.SyncDir
)

// publishSnapshotExport renames the staged pair into exportPath durably, restoring the previous export on failure; every crash state is a complete pair or one verifiers refuse.
func publishSnapshotExport(exportPath, dataTmp, metaTmp, syncRoot string) (err error) {
	backup, err := stashPreviousExport(exportPath)
	if err != nil {
		return err
	}
	var published []string
	defer func() {
		if err != nil {
			backup.restore(exportPath, published)
			return
		}
		backup.discard()
		sweepExportOrphans(exportPath)
	}()

	if err = exportRename(dataTmp, filepath.Join(exportPath, snapshotDataFile)); err != nil {
		return err
	}
	published = append(published, snapshotDataFile)
	if err = exportRename(metaTmp, filepath.Join(exportPath, snapshotMetadataFile)); err != nil {
		return err
	}
	published = append(published, snapshotMetadataFile)

	// Fsync the export directory so both renames survive a crash, then every
	// directory the export created, so the export directory itself does.
	return fsutil.SyncDirChainWith(exportSyncDir, exportPath, syncRoot)
}

// sweepExportOrphans durably removes backup directories and temp files a crashed export left; failures are warned.
func sweepExportOrphans(exportPath string) {
	removed := false
	for _, pattern := range []string{exportBackupPattern, exportTempPattern} {
		matches, err := filepath.Glob(filepath.Join(exportPath, pattern))
		if err != nil {
			continue // only ErrBadPattern, and both patterns are constants
		}
		for _, orphan := range matches {
			if rmErr := os.RemoveAll(orphan); rmErr != nil {
				slog.Warn("snapshot export: failed to remove a crashed export's leftover",
					"path", orphan,
					"error", rmErr,
				)
				continue
			}
			removed = true
		}
	}
	if !removed {
		return
	}
	if err := exportSyncDir(exportPath); err != nil {
		slog.Warn("snapshot export: removing a crashed export's leftovers is not durable",
			"path", exportPath,
			"error", err,
		)
	}
}

// exportBackup is a previous export's pair, moved aside while a new one is
// published. Its zero value is an empty backup: there was nothing to move.
type exportBackup struct {
	dir   string
	moved []string
}

// stashPreviousExport moves the existing snapshot.meta then snapshot.dat into a new backup directory and fsyncs both; a failure restores them.
func stashPreviousExport(exportPath string) (*exportBackup, error) {
	var present []string
	for _, name := range []string{snapshotMetadataFile, snapshotDataFile} {
		info, err := os.Lstat(filepath.Join(exportPath, name))
		if err != nil {
			if os.IsNotExist(err) {
				continue
			}
			return nil, err
		}
		if info.Mode().IsRegular() {
			present = append(present, name)
		}
	}
	b := &exportBackup{}
	if len(present) == 0 {
		return b, nil
	}
	dir, err := os.MkdirTemp(exportPath, exportBackupPattern)
	if err != nil {
		return nil, err
	}
	b.dir = dir
	for _, name := range present {
		if err := exportRename(filepath.Join(exportPath, name), filepath.Join(dir, name)); err != nil {
			b.restore(exportPath, nil)
			return nil, err
		}
		b.moved = append(b.moved, name)
	}
	for _, d := range []string{dir, exportPath} {
		if err := exportSyncDir(d); err != nil {
			b.restore(exportPath, nil)
			return nil, err
		}
	}
	return b, nil
}

// restore undoes a failed publication, renaming moved files back and removing unmatched published ones.
func (b *exportBackup) restore(exportPath string, published []string) {
	var firstErr error
	for _, name := range []string{snapshotDataFile, snapshotMetadataFile} {
		dst := filepath.Join(exportPath, name)
		var err error
		switch {
		case slices.Contains(b.moved, name):
			err = exportRename(filepath.Join(b.dir, name), dst)
		case slices.Contains(published, name):
			if err = os.Remove(dst); os.IsNotExist(err) {
				err = nil
			}
		}
		if err != nil && firstErr == nil {
			firstErr = err
		}
	}
	if b.dir == "" && len(published) == 0 {
		return
	}
	if err := exportSyncDir(exportPath); err != nil && firstErr == nil {
		firstErr = err
	}
	if firstErr != nil {
		slog.Warn("snapshot export: the previous export could not be restored after a failed publish",
			"path", exportPath,
			"backup", b.dir,
			"error", firstErr,
		)
		return
	}
	if b.dir != "" {
		if err := os.Remove(b.dir); err != nil {
			slog.Warn("snapshot export: failed to remove the emptied backup directory",
				"path", b.dir,
				"error", err,
			)
		}
	}
}

// discard removes the backup once the new pair is durable. A backup that
// cannot be removed is warned: it holds the previous export's plaintext.
func (b *exportBackup) discard() {
	if b.dir == "" {
		return
	}
	if err := os.RemoveAll(b.dir); err != nil {
		slog.Warn("snapshot export: failed to remove the previous export's backup",
			"path", b.dir,
			"error", err,
		)
	}
}

// writeExportTemp copies r into a new fsynced temp file in dir and returns its path even on error; a Close error is reported.
func writeExportTemp(dir string, r io.Reader) (string, error) {
	dst, err := os.CreateTemp(dir, exportTempPattern)
	if err != nil {
		return "", err
	}
	_, writeErr := io.Copy(dst, r)
	if writeErr == nil {
		writeErr = dst.Sync()
	}
	closeErr := dst.Close()
	if writeErr != nil {
		return dst.Name(), writeErr
	}
	return dst.Name(), closeErr
}

// GCSnapshotDirs keeps the newest maxSnapshotsRetained directories for paths that publish without saveSnapshot; call after the new snapshot is recorded. recorded and spare are never removed.
func GCSnapshotDirs(baseDir string, shardID, replicaID uint64, recorded string, spare ...string) error {
	return gcSnapshotDirs(baseDir, shardID, replicaID, recorded, spare...)
}

// gcSnapshotDirs removes old snapshot directories, keeping only the
// most recent maxSnapshotsRetained. Returns nil if no cleanup is needed.
func gcSnapshotDirs(baseDir string, shardID, replicaID uint64, recorded string, spare ...string) error {
	dirs, err := listSnapshotDirs(baseDir, shardID, replicaID)
	if err != nil {
		return err
	}
	if len(dirs) <= maxSnapshotsRetained {
		return nil
	}
	// Remove the oldest directories first, never the one LogDB records.
	toRemove := dirs[:len(dirs)-maxSnapshotsRetained]
	for _, d := range toRemove {
		if recorded != "" && filepath.Clean(d.path) == recorded {
			slog.Error("snapshot: refusing to GC the directory LogDB records",
				"shard_id", shardID,
				"replica_id", replicaID,
				"path", d.path,
			)
			continue
		}
		// Spare a directory a recovery is reading or a receive has published but raft has not adopted.
		if slices.Contains(spare, filepath.Clean(d.path)) {
			continue
		}
		if err := os.RemoveAll(d.path); err != nil {
			return err
		}
	}
	// Fsync so a crash cannot resurrect a removed directory.
	return fsutil.SyncDir(snapshotBaseDir(baseDir, shardID, replicaID))
}

// recordedSnapshotDir returns the cleaned path LogDB records for the node, "" when there is none, and an error when the record is unreadable.
func (p *snapshotPool) recordedSnapshotDir(shardID, replicaID uint64) (string, error) {
	if p.logdb == nil {
		return "", nil
	}
	rec, err := p.logdb.GetSnapshot(shardID, replicaID)
	if errors.Is(err, logdb.ErrNoSnapshot) {
		return "", nil
	}
	if err != nil {
		return "", err
	}
	if rec.Filepath == "" {
		return "", nil
	}
	return filepath.Clean(rec.Filepath), nil
}
