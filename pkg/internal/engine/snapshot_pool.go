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
	"strings"
	"sync"
	"sync/atomic"
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

// atomicEpochPinner is an OPTIONAL capability a SnapshotBarrier may implement:
// seal a frame and register the epoch it sealed under as in-flight in ONE atomic
// step, under the barrier's own lock.
//
// EncryptWithEpoch releases that lock before returning, so a Rotate followed by
// PurgeEpochsBefore can complete in the gap before the caller pins the returned
// epoch — dropping the DEK of an already-sealed frame and making the snapshot
// permanently undecryptable. Doing both under one lock closes that window.
// encryptSnapshotFile uses it when available and falls back to
// pin-before + pin-after otherwise.
type atomicEpochPinner interface {
	EncryptWithEpochPinned(dst, plaintext []byte) ([]byte, uint64, error)
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
	workers int
	logdb   logdb.LogDB
	barrier SnapshotBarrier

	// shardBarriers, when non-nil, routes each Raft shard to its OWN at-rest
	// barrier (per-tenant WAL/snapshot encryption). It is wired by the Host
	// via WithShardBarriers and is purely ADDITIVE: when nil (the default),
	// every shard uses the single process-wide barrier above and the pool
	// behaves byte-for-byte as before. When set, snapshot encrypt/decrypt and
	// DEK purging dispatch per shard so that purging one barrier's DEKs can
	// never affect epochs still live under another barrier. A nil return from
	// the function for a given shard falls back to the process-wide barrier.
	shardBarriers func(shardID uint64) SnapshotBarrier

	callback   Callback
	requestC   chan snapshotRequest
	completedC chan snapshotResult
	inFlight   sync.Map // shardID -> struct{} for per-shard dedup
	stopC      chan struct{}

	// workSignal is used to notify the step worker after snapshot
	// recovery completes so it re-evaluates the node immediately
	// rather than waiting for the next tick interval.
	workSignal *WorkSignal

	// nodes enumerates every node currently loaded in the engine. It is
	// wired by NewEngine after the Engine struct is assembled so the pool
	// can compute a GLOBAL safe-purge floor across all shards sharing the
	// process-wide barrier. May be nil in standalone tests, in which case
	// the global floor falls back to the calling shard only.
	nodes func() []*Node

	// tryImportLock takes the host's per-shard import lock without waiting,
	// reporting whether it did; the returned func releases it. It is wired by
	// the Host, which owns the lock ImportSnapshot holds for the DURATION of
	// an import.
	//
	// The import staging directory holds the imported state machine in
	// PLAINTEXT and is indistinguishable by name from one a crash orphaned, so
	// the sweep that reclaims it may only run when no import is in flight. Nil
	// means no host owns this pool (standalone engine tests): nothing can be
	// importing, so the sweep runs.
	tryImportLock func(shardID uint64) (func(), bool)

	// metrics collects raft-level metrics. Nil when disabled.
	metrics RaftMetrics
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

	// maxSnapshotFrameOverhead bounds the AEAD framing a single encrypted
	// snapshot frame adds over its plaintext chunk: epoch(8) + algID(1) +
	// nonce(<=24) + tag(16). 256 is a generous ceiling used to reject a
	// corrupt frame-length prefix before allocation during recovery.
	maxSnapshotFrameOverhead = 256
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

// barrierForShard returns the barrier that encrypts the given shard's
// snapshots and WAL records. When per-shard barriers are not configured
// (shardBarriers == nil) it returns the single process-wide barrier, which
// preserves the original single-barrier behavior exactly. When per-shard
// barriers ARE configured and the mapping returns nil for this shard, the shard
// falls back to the process-wide barrier — but ONLY when there is one.
//
// With no process-wide barrier the fallback has no encrypting outcome, so the
// unmapped shard fails CLOSED with ErrShardBarrierUnresolved instead of being
// written in plaintext. A resolver typed func(shardID) SnapshotBarrier cannot
// say "this shard is deliberately plaintext" as distinct from "I do not know
// this shard", and the natural map-lookup implementation returns nil for an
// async-provisioned tenant or a remapped shard ID; reading that as plaintext
// downgrades one tenant while every other tenant still looks encrypted.
//
// A nil barrier with a nil error remains the configured plaintext deployment:
// no per-shard resolver and no process-wide barrier.
func (p *snapshotPool) barrierForShard(shardID uint64) (SnapshotBarrier, error) {
	if p.shardBarriers == nil {
		return p.barrier, nil
	}
	if b := p.shardBarriers(shardID); b != nil {
		return b, nil
	}
	if p.barrier == nil {
		return nil, ErrShardBarrierUnresolved
	}
	return p.barrier, nil
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
			// Release requests still queued behind us. Returning without
			// this strands them: their inFlight entry and the node's
			// snapshotting flag were both set by requestSnapshot and are
			// only ever cleared by the worker that runs the request, so
			// the shard could never snapshot again and the caller's
			// pending RequestState would wait for a completion callback
			// that can no longer fire.
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
	for {
		select {
		case req := <-p.requestC:
			p.abandonRequest(req)
		default:
			return
		}
	}
}

// abandonRequest undoes the per-shard state requestSnapshot established for
// a request that will never run, and reports the rejection so the caller
// does not wait for a completion that cannot come.
func (p *snapshotPool) abandonRequest(req snapshotRequest) {
	p.inFlight.Delete(req.shardID)
	if req.node != nil {
		req.node.snapshotting.Store(false)
	}
	if p.callback != nil {
		p.callback.OnSnapshotRejected(req.shardID, req.replicaID)
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
	// The recover stays armed for the WHOLE function, not just the
	// handleSnapshot call: everything after it — SetRecovering, the work
	// signal, metrics, and OnSnapshotCompleted — reaches user EventListener
	// callbacks through the host. A panicking listener on a pool worker
	// goroutine would otherwise abort the entire host process, taking every
	// unrelated shard with it.
	completed := false
	defer func() {
		r := recover()
		if r == nil {
			return
		}
		stack := debug.Stack()
		slog.Error("snapshot worker panic recovered",
			"shard_id", req.shardID,
			"replica_id", req.replicaID,
			"save", req.save,
			"completed", completed,
			"panic", fmt.Sprint(r),
			"stack", string(stack),
		)
		// Mirror the normal cleanup path: clear recovering flag
		// for recovery requests and signal the step worker. The cleanup is
		// idempotent, so running it again after a post-completion panic is
		// harmless and guarantees the shard is never left wedged.
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
		// Notify via callback that the snapshot was rejected due to the
		// panic, so the caller can retry or report. Only when the snapshot
		// itself did not complete: after completion OnSnapshotCompleted has
		// already been delivered, and a second terminal callback would
		// misreport a snapshot that exists. The nested recover keeps a
		// panicking listener from escaping this deferred handler.
		if !completed && p.callback != nil {
			func() {
				defer func() {
					if r2 := recover(); r2 != nil {
						slog.Error("OnSnapshotRejected callback panicked",
							"shard_id", req.shardID,
							"replica_id", req.replicaID,
							"panic", fmt.Sprint(r2),
						)
					}
				}()
				p.callback.OnSnapshotRejected(req.shardID, req.replicaID)
			}()
		}
	}()

	// Set the recovering flag BEFORE calling handleSnapshot
	// for recovery requests. This gates the apply worker and
	// step worker from operating on the state machine while
	// RecoverFromSnapshot executes in this goroutine.
	//
	// Setting the flag stops NEW applies at the gate, but an apply that
	// already passed the gate could still be inside SM.Apply. Drain it
	// before RecoverFromSnapshot touches the state machine, otherwise the
	// two would race and corrupt SM/session state. The increment-then-check
	// (apply) / set-then-drain (here) ordering guarantees any in-flight
	// apply is observed by the drain.
	var blocked error
	if !req.save {
		// The step worker DEFERS InstallSnapshot while the recovering gate is
		// up (handling one would move the raft log's snapshot floor out from
		// under this recovery), so the InstallSnapshot that advances the log
		// to the snapshot index cannot be installed once the gate is raised.
		// Wait for the log to reach the snapshot index BEFORE raising it,
		// otherwise the recovery would exclude the very message that makes it
		// admissible.
		blocked = p.awaitRecoverableLog(req)
	}
	if !req.save && blocked == nil {
		req.node.SetRecovering(true)
		// If the drain aborts because the node is stopping, do NOT touch the
		// state machine: an apply may still be in flight (engine Stop closes
		// stopC before the apply workers drain), and RecoverFromSnapshot would
		// race it. Abort the recovery cleanly instead.
		if !req.node.WaitForApplyDrain(req.node.StopC()) {
			req.node.SetRecovering(false)
			p.inFlight.Delete(req.shardID)
			req.node.snapshotting.Store(false)
			if p.workSignal != nil {
				p.workSignal.Notify(req.shardID)
			}
			return
		}
	}
	var index uint64
	err := blocked
	if err == nil {
		index, err = p.handleSnapshot(req)
	}

	// Mark that the snapshot operation itself finished, so a later panic is
	// reported as a post-completion panic rather than a rejected snapshot.
	completed = true

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

// discardSaveDir removes a snapshot save directory that will never be
// recorded in LogDB. It is used on the failure paths that run while the data
// file still holds plaintext, or holds an artifact that must not be retained;
// a removal failure is logged and otherwise ignored because the caller is
// already returning an error.
func discardSaveDir(saveDir string, req snapshotRequest) {
	if err := os.RemoveAll(saveDir); err != nil {
		slog.Debug("snapshot: discard of save directory failed",
			"shard_id", req.shardID,
			"replica_id", req.replicaID,
			"path", saveDir,
			"error", err,
		)
	}
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

	// Cheap early-out: nothing applied yet means nothing to snapshot.
	if snapshotter.LastApplied() == 0 {
		return 0, nil
	}

	// A CONFIGURED barrier that is SEALED means encryption is temporarily
	// unavailable — not that this shard is a plaintext deployment. Fail here,
	// before any directory, data file, metadata file or LogDB record exists,
	// so a host sealed during incident response cannot durably write its next
	// snapshot as plaintext at rest. A nil barrier is the distinct
	// "encryption disabled" state and proceeds unencrypted as configured.
	//
	// An UNRESOLVED per-shard barrier fails at the same point and for the same
	// reason: the shard would be the only one written in plaintext.
	barrier, barrierErr := p.barrierForShard(req.shardID)
	if barrierErr != nil {
		return 0, &SnapshotError{
			ShardID:   req.shardID,
			ReplicaID: req.replicaID,
			Save:      true,
			Err:       barrierErr,
		}
	}
	if barrier != nil && barrier.IsSealed() {
		return 0, &SnapshotError{
			ShardID:   req.shardID,
			ReplicaID: req.replicaID,
			Save:      true,
			Err:       ErrSnapshotBarrierSealed,
		}
	}

	// Quiesce applies for this node so the consistent-capture phase of the
	// snapshot (session-manager marshal + user-SM PrepareSnapshot, or a regular
	// SM's whole in-memory save) cannot race a concurrent SM.Apply — a race
	// that otherwise produces a fatal "concurrent map iteration and map write"
	// on the session manager and crashes the whole host process. The gate is
	// released via the afterCapture callback as soon as the point-in-time is
	// captured, so an on-disk/concurrent SM streams its snapshot WITHOUT pausing
	// applies. The deferred release is a backstop covering every error path.
	node.SetSaving(true)
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { node.SetSaving(false) }) }
	defer release()
	if !node.WaitForApplyDrain(node.StopC()) {
		return 0, &SnapshotError{
			ShardID:   req.shardID,
			ReplicaID: req.replicaID,
			Save:      true,
			Err:       ErrStopped,
		}
	}

	// With applies drained, lastApplied and its term are stable: capture a
	// CONSISTENT (index, term) for the snapshot point. The SM state that
	// SaveSnapshot captures reflects exactly this index (no apply can advance it
	// while the gate holds), so recording it — rather than a stale dispatch-time
	// index — prevents an under-recorded index (which would re-apply
	// already-snapshotted entries on recovery and double-apply) and a mismatched
	// term (which would corrupt raft log matching on InstallSnapshot).
	snapshotIndex := snapshotter.LastApplied()
	if snapshotIndex == 0 {
		return 0, nil
	}

	// The term MUST be captured here, INSIDE the gate and together with the
	// index. afterCapture releases the gate as soon as the point-in-time is
	// captured, after which applies resume; a leader change during the (long)
	// stream/flush/fsync/encrypt phases would then apply entries at term T+1
	// and a late lastAppliedTerm read would record (index@T, term=T+1) — a
	// Log Matching violation for any follower that installs the snapshot.
	// The req.appliedToTerm fallback covers only the rare forced snapshot
	// taken after recovery before any apply has run (lastAppliedTerm seeded
	// to 0 with no snapshot term available).
	term := node.lastAppliedTerm.Load()
	if term == 0 {
		term = req.appliedToTerm
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
	// The compression decision is recorded with the artifact (metadata file,
	// LogDB record, export header) so every reader derives its chain from the
	// artifact rather than from its own — per-shard, mutable, per-host —
	// config. Reading it back from config is how a follower with a different
	// setting, or an operator flipping it across a restart, silently feeds
	// compressed bytes to the state machine.
	compression := node.cfg.SnapshotCompressionType
	ctx := context.Background()
	var w io.Writer = f
	var snappyW *snappy.Writer
	if compression == config.Snappy {
		snappyW = snappy.NewBufferedWriter(f)
		w = snappyW
	}
	// Use the gated save when available (rsm.StateMachine) so the apply gate is
	// released right after the point-in-time capture; otherwise fall back to a
	// full-save gating (release happens via the deferred backstop after the
	// save returns).
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
			discardSaveDir(saveDir, req)
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
	// The recorded epoch is the MINIMUM epoch across all encrypted frames
	// (returned by encryptSnapshotFile), not a post-hoc CurrentEpoch() read.
	// A key rotation can run concurrently on a separate goroutine, so frames
	// written before the rotation are sealed at epoch E and frames after at
	// E+1; recording the min guarantees the global DEK-purge floor never
	// purges a DEK any retained frame still needs. As defense-in-depth,
	// RegisterInFlightEpoch pins the epoch live at save start so a concurrent
	// purge cannot remove it mid-save; the deregister runs after encryption.
	//
	// The data file on disk is PLAINTEXT until encryptSnapshotFile replaces
	// it, so every failure from here to the end of the encrypt discards the
	// whole save directory: leaving it behind would strand exactly the
	// plaintext artifact this branch exists to prevent.
	var epoch uint64
	if barrier != nil {
		// Re-check: an operator seal (EmergencySeal) can land between the
		// entry check and here, after the plaintext data file was written.
		if barrier.IsSealed() {
			discardSaveDir(saveDir, req)
			return 0, &SnapshotError{
				ShardID:   req.shardID,
				ReplicaID: req.replicaID,
				Save:      true,
				Err:       ErrSnapshotBarrierSealed,
			}
		}
		// encryptSnapshotFile pins EVERY epoch it seals a frame under (before and
		// after each seal) and returns the set. Hold those pins until saveSnapshot
		// returns — past the metadata-file and LogDB snapshot-record writes below —
		// so a concurrent rotate+purge cannot remove any frame's DEK in the window
		// before the snapshot's recorded epoch becomes durable and visible to the
		// snapshot-floor scan. Pinning every epoch (not just the pre-read) closes
		// the intermediate-frame-epoch gap a mid-loop rotation opens.
		minEpoch, encPins, encryptErr := p.encryptSnapshotFile(dataPath, barrier)
		defer func() {
			for _, e := range encPins {
				barrier.DeregisterInFlightEpoch(e)
			}
		}()
		if encryptErr != nil {
			discardSaveDir(saveDir, req)
			return 0, &SnapshotError{
				ShardID:   req.shardID,
				ReplicaID: req.replicaID,
				Save:      true,
				Err:       encryptErr,
			}
		}
		epoch = minEpoch
	}

	// Write snapshot metadata file (includes epoch for encrypted snapshots).
	// The (snapshotIndex, term) pair was captured together under the apply
	// gate above; do NOT re-read lastAppliedTerm here — applies may have
	// resumed since afterCapture released the gate.
	if err := writeSnapshotMetadata(saveDir, snapshotIndex, term, req.shardID, req.replicaID, epoch, compression); err != nil {
		return 0, &SnapshotError{
			ShardID:   req.shardID,
			ReplicaID: req.replicaID,
			Save:      true,
			Err:       err,
		}
	}

	// Make the directory entries durable BEFORE recording the snapshot in
	// LogDB. The data and metadata files are fsynced above, but the
	// directory entries created by MkdirAll, the file creates, and the
	// encrypt-in-place rename are not durable until each directory in the
	// chain is fsynced. Without this, a crash could leave LogDB's
	// durably-recorded snapshot pointing at a directory that no longer
	// exists — a multi-node cluster re-streams from the leader, but a
	// single-node deployment would lose the snapshot and the compacted
	// entries it covers.
	if err := SyncDirChain(saveDir, node.SnapshotDir()); err != nil {
		return 0, &SnapshotError{
			ShardID:   req.shardID,
			ReplicaID: req.replicaID,
			Save:      true,
			Err:       err,
		}
	}

	// Measure the artifact AFTER encryption so FileSize always means "size of
	// the file at Filepath". A record whose FileSize is 0 while Filepath is
	// set makes this replica stream an empty snapshot once it becomes leader.
	var fileSize uint64
	if fi, statErr := os.Stat(dataPath); statErr == nil {
		fileSize = uint64(fi.Size())
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
			Index:       snapshotIndex,
			Term:        term,
			Membership:  membership,
			Filepath:    saveDir,
			FileSize:    fileSize,
			Epoch:       epoch,
			Compression: compression,
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
		if err := lr.CreateSnapshot(proto.Snapshot{
			ShardID:    req.shardID,
			ReplicaID:  req.replicaID,
			Index:      snapshotIndex,
			Term:       term,
			Filepath:   dataPath,
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
			// Advance the LogReader's compaction marker BEFORE the LogDB
			// removal. LogReader.Compact resolves the term at compactIndex,
			// which stops being readable once RemoveEntriesTo has run — the
			// reverse order fails with ErrCompacted and silently leaves
			// markerIndex stale, so raft keeps probing for entries the
			// LogDB has already discarded.
			if lr := node.LogReader(); lr != nil {
				if lrErr := lr.Compact(compactIndex); lrErr != nil &&
					!errors.Is(lrErr, logdb.ErrCompacted) {
					slog.Warn("log reader compaction failed",
						"shard_id", req.shardID,
						"replica_id", req.replicaID,
						"compact_index", compactIndex,
						"error", lrErr,
					)
				}
			}
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

// peerLogMarkers returns the Raft log's committed and processed markers as
// last published by the step worker. The third result is false when the node
// has no peer (there is then no raft log whose markers a recovery could get
// ahead of).
//
// The markers are read from the node's published atomics, NEVER from the Peer.
// raftMu serializes the step worker's Tick with the commit worker's Commit; it
// does NOT cover the step worker's inbox handling, so reading Peer.r from this
// (snapshot pool) goroutine races with the very handleSnapshot the wait below
// is waiting for — see the Node.diagSnap comment for the same rule applied to
// diagnostics. The published values can only lag the log, never lead it, so a
// stale read costs one more poll interval and can never let a recovery start
// before the log holds the snapshot.
//
// BOTH markers are needed. committed alone is not sufficient: with
// processed < index <= committed, seeding lastApplied to the snapshot index
// makes the next Update carry a LastApplied above the log's processed marker,
// which commitUpdate asserts against.
func peerLogMarkers(node *Node) (committed, processed uint64, ok bool) {
	if node.Peer() == nil {
		return 0, 0, false
	}
	committed, processed = node.LogMarkers()
	return committed, processed, true
}

// recoveryBlockedByLog reports why a state machine recovery for snapIndex must
// not proceed, or nil when it may. Both bounds are evaluated against markers
// that only the raft log and the apply pipeline own, so this MUST run before
// the recovering gate is raised and before RecoverFromSnapshot touches the
// state machine:
//
//   - Lower bound: the snapshot must be strictly ahead of the apply frontier.
//     Recovering behind it rolls the state machine backwards and regresses
//     lastApplied, which fails the shard with ErrApplyGap on the next batch.
//   - Upper bound: the raft log must already hold the snapshot, i.e. the
//     InstallSnapshot must have been installed by the step worker. Until then
//     the recovery is merely early, not wrong — the error says so.
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

// Bounds on how long a recovery request waits for the step worker to install
// the snapshot into the raft log. The install is a single inbox message the
// step worker picks up on the next work signal, so the common wait is a
// scheduling delay; the budget only has to outlast a loaded machine.
const (
	recoveryLogWaitInterval = 5 * time.Millisecond
	recoveryLogWaitBudget   = 5 * time.Second
)

// awaitRecoverableLog blocks until a recovery request's log bounds are
// satisfiable, WITHOUT raising the recovering gate, and returns nil once they
// are. A permanent refusal (the snapshot is behind the apply frontier) is
// returned immediately. A transient one — the raft log has not installed the
// snapshot yet — is retried until the budget is exhausted, then returned so
// the transfer fails loudly instead of leaving the state machine silently
// behind a snapshot raft already accounts for.
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

// pendingRecoveryIndex returns the index of the snapshot a recovery request
// would restore, or 0 when there is nothing to recover. It reads only LogDB,
// so handleRequest can evaluate the recovery bounds BEFORE raising the
// recovering gate.
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

	// Evaluate both apply-frontier bounds BEFORE the snapshotter is touched.
	// Running RecoverFromSnapshot first and checking afterwards leaves the
	// state machine replaced by a snapshot whose markers were then refused.
	if err := recoveryBlockedByLog(req, logdbSnap.Index); err != nil {
		return err
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

	// When the snapshot was encrypted at rest (non-zero epoch) and a barrier
	// is available, decrypt it to a throwaway SIBLING file and recover from
	// that, leaving the canonical encrypted snapshot.dat untouched. Decrypting
	// in place is NOT crash/retry-safe: if RecoverFromSnapshot then fails or the
	// process crashes, snapshot.dat would be plaintext while LogDB still records
	// Epoch>0, so every later recovery/restart would re-attempt decryption on
	// plaintext and fail — permanently losing an otherwise-valid snapshot.
	dataPath := filepath.Join(snapshotDir, snapshotDataFile)
	recoverPath := dataPath

	// Sweep stale decrypt scratch files left by a crash during a prior
	// recovery OR a prior snapshot SEND. Both write plaintext to a sibling
	// scratch file and remove it via defer once done, but a process crash
	// orphans these files. The whole shard tree is swept, not just the
	// directory being recovered, because the send path decrypts inside
	// whichever snapshot directory it streams. snapshotDir is swept too: a
	// LogDB-recorded Filepath is not required to sit under the node's base.
	SweepStaleDecryptTmpTree(node.SnapshotDir(), req.shardID, req.replicaID)
	SweepStaleDecryptTmp(snapshotDir)

	// Reclaim an import staging directory orphaned by a crash for the same
	// reason and on the same trigger: it holds the imported state machine in
	// PLAINTEXT and no scan on the host will ever look at it again.
	//
	// "The shard is running, so no import can be in flight" is NOT a valid
	// justification for sweeping unlocked. Host.ImportSnapshot checks that the
	// shard is stopped in its PREFLIGHT, before it takes the import lock and
	// long before it finishes writing; StartShard registers the node hundreds
	// of lines after its own sweep. An import that passed preflight and is
	// midway through a multi-gigabyte write is therefore perfectly compatible
	// with a shard that is running by the time a leader's InstallSnapshot lands
	// here — and RemoveAll on its live staging directory fails the operator's
	// disaster-recovery restore for no reason.
	//
	// Take the same try-lock StartShard takes: it is the only thing that
	// distinguishes a live staging directory from an orphaned one, and an
	// import that holds it owns its staging directory and removes it itself.
	sweepImportStage(p.tryImportLock, node.SnapshotDir(), req.shardID, req.replicaID)

	// An encrypted artifact with no usable barrier fails CLOSED, matching
	// Host.StartShard on the identical predicate. Falling through would hand
	// the raw ciphertext to RecoverFromSnapshot; the leader must re-send
	// instead. Reachable whenever the barrier is sealed or torn down between
	// the snapshot receive and the (asynchronous, retried) recovery.
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
	if logdbSnap.Epoch > 0 {
		plainPath, decryptErr := p.decryptSnapshotToSibling(dataPath, logdbSnap.Epoch, barrier)
		if decryptErr != nil {
			return &SnapshotError{
				ShardID:   req.shardID,
				ReplicaID: req.replicaID,
				Save:      false,
				Err:       decryptErr,
			}
		}
		recoverPath = plainPath
		defer func() {
			if rmErr := os.Remove(plainPath); rmErr != nil && !os.IsNotExist(rmErr) {
				slog.Debug("snapshot: failed to remove decrypted scratch file",
					"shard_id", req.shardID,
					"replica_id", req.replicaID,
					"path", plainPath,
					"error", rmErr,
				)
			}
		}()
	}

	// Open the (plaintext) snapshot data file. The engine.Snapshotter interface
	// accepts io.Reader, and *os.File satisfies io.Reader.
	f, err := os.Open(recoverPath)
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
				"path", recoverPath,
				"error", closeErr,
			)
		}
	}()

	// Recover SM state from the snapshot data. The reader chain mirrors the
	// chain the ARTIFACT was written through, taken from the snapshot record
	// — not from node.cfg, which may have been changed since the save, or may
	// never have matched the leader that streamed this snapshot.
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

	// Seed the snapshotter's OWN applied counter alongside the node markers.
	// saveSnapshot takes its snapshot INDEX from snapshotter.LastApplied() and
	// its TERM from node.lastAppliedTerm; the recovered state machine reflects
	// logdbSnap.Index, so the two sources must agree here. Without this, a
	// snapshot requested right after recovery (before the next Apply) would
	// record the stale pre-recovery index against post-recovery state —
	// regressing lastSnapshotIndex and double-applying on replay.
	if setter, ok := snapshotter.(appliedSetter); ok {
		setter.SetLastApplied(logdbSnap.Index)
	}

	return nil
}

// encryptSnapshotFile reads the plaintext snapshot data file, encrypts
// each chunk using the barrier, and atomically replaces the file with
// the encrypted output. The encrypted format is a sequence of frames:
// [ChunkLen:4 | EncryptedChunk]...
// where each EncryptedChunk includes the barrier's epoch+nonce+tag prefix.
//
// It returns the MINIMUM epoch used across all encrypted frames. A key
// rotation can run concurrently on a different goroutine, so chunks written
// before the rotation use epoch E while chunks written after use E+1. Each
// frame embeds its own epoch in its ciphertext header (so decryption is
// per-frame correct), but the snapshot's single recorded Epoch must be the
// minimum so the DEK-purge floor (globalSafePurgeFloor) never purges a DEK
// that any retained frame still needs. The returned epoch is 0 only when the
// source file is empty (no frames written).
//
// Atomic write: output goes to a temporary file in the same directory,
// synced to disk, then renamed over the original. If any step fails the
// temp file is removed and the original is preserved intact.
func (p *snapshotPool) encryptSnapshotFile(path string, barrier SnapshotBarrier) (uint64, []uint64, error) {
	// Open the plaintext file for streaming reads. Only one chunk
	// (snapshotEncryptChunkSize = 64KB) is held in memory at a time,
	// avoiding OOM on large snapshot files.
	srcFile, err := os.Open(path)
	if err != nil {
		return 0, nil, &SnapshotEncryptError{Op: "read", Err: err}
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
	tmpFile, err := os.CreateTemp(dir, SaveEncryptTempPattern)
	if err != nil {
		return 0, nil, &SnapshotEncryptError{Op: "create_temp", Err: err}
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

	// Encrypt in chunks to bound memory usage. Track the minimum epoch used
	// across all frames: a concurrent rotation can advance the barrier epoch
	// between chunks, so EncryptWithEpoch returns the epoch each frame was
	// sealed with and minEpoch records the lowest. The caller persists
	// minEpoch as the snapshot's Epoch so the DEK-purge floor protects every
	// frame. minEpoch stays 0 for an empty file (no frames), which the caller
	// treats as a plaintext/unencrypted snapshot.
	chunkBuf := make([]byte, snapshotEncryptChunkSize)
	var frameBuf [4]byte
	var minEpoch uint64
	// Pin EVERY epoch a frame is sealed under, not just the minimum, so a
	// concurrent rotation + PurgeEpochsBefore cannot orphan an INTERMEDIATE frame
	// epoch's DEK (which the min-only record left exposed once two rotations
	// landed mid-loop), leaving the snapshot undecryptable. The returned set is
	// released by the caller AFTER the snapshot's epoch is durably recorded.
	pins := newEpochPinner(barrier)
	encErrOf := func(op string, e error) error {
		pins.releaseAll()
		return &SnapshotEncryptError{Op: op, Err: e}
	}
	for {
		n, readErr := srcFile.Read(chunkBuf)
		if n > 0 {
			encrypted, frameEpoch, encErr := pins.sealFrame(chunkBuf[:n])
			if encErr != nil {
				return 0, nil, encErrOf("encrypt", encErr)
			}
			if minEpoch == 0 || frameEpoch < minEpoch {
				minEpoch = frameEpoch
			}

			// Write frame: 4-byte little-endian length + encrypted data.
			binary.LittleEndian.PutUint32(frameBuf[:], uint32(len(encrypted)))
			if _, err := tmpFile.Write(frameBuf[:]); err != nil {
				return 0, nil, encErrOf("write_frame", err)
			}
			if _, err := tmpFile.Write(encrypted); err != nil {
				return 0, nil, encErrOf("write_data", err)
			}
		}
		if readErr == io.EOF {
			break
		}
		if readErr != nil {
			return 0, nil, encErrOf("read", readErr)
		}
	}

	// Fsync before rename to guarantee durability.
	if err := tmpFile.Sync(); err != nil {
		return 0, nil, encErrOf("sync", err)
	}
	if err := tmpFile.Close(); err != nil {
		return 0, nil, encErrOf("close", err)
	}

	// Atomic rename replaces the original file. Both paths are internal
	// (tmpPath from os.CreateTemp, path from snapshot metadata) — not
	// user-controlled, so path traversal is not a concern.
	if err := os.Rename(tmpPath, path); err != nil { // #nosec G703 -- paths are internal, not user input
		return 0, nil, encErrOf("rename", err)
	}
	committed = true
	// Fsync the directory so the rename itself is durable; without it a
	// crash can roll the directory entry back to the (deleted) plaintext
	// temp name even though the encrypted bytes were fsynced.
	if err := SyncDir(dir); err != nil {
		return 0, nil, encErrOf("sync_dir", err)
	}
	return minEpoch, pins.epochs(), nil
}

// decryptSnapshotToSibling reads the encrypted snapshot data file and writes
// its decrypted plaintext to a NEW sibling temp file in the same directory,
// returning that temp file's path. It does NOT modify or replace the source
// file: the canonical encrypted snapshot.dat is left intact.
//
// This is deliberately not an in-place transform. Decrypting in place (rename
// plaintext over snapshot.dat) is not crash/retry-safe: a recovery failure or
// a crash after the rename would leave plaintext on disk while LogDB still
// records Epoch>0, so any subsequent recovery/restart would re-attempt
// decryption on plaintext and fail permanently, losing an otherwise-valid
// snapshot. The caller is responsible for removing the returned temp file once
// recovery is done. Only one frame is held in memory at a time, so this is
// safe for large snapshots.
//
// The epoch parameter is advisory: each frame is decrypted with the epoch
// embedded in its own ciphertext header (via barrier.Decrypt), not with this
// value. It is retained only for log context. The caller uses the recorded
// epoch solely to decide whether decryption is needed at all (Epoch>0).
func (p *snapshotPool) decryptSnapshotToSibling(path string, epoch uint64, barrier SnapshotBarrier) (string, error) {
	srcFile, err := os.Open(path)
	if err != nil {
		return "", &SnapshotEncryptError{Op: "read", Err: err}
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

	dir := filepath.Dir(path)
	tmpFile, err := os.CreateTemp(dir, RecoverDecryptTempPattern)
	if err != nil {
		return "", &SnapshotEncryptError{Op: "create_temp", Err: err}
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

	// Read and decrypt each frame. Frame format: [4-byte LE length][data]. An
	// empty source yields an immediate EOF and an empty plaintext temp file,
	// which RecoverFromSnapshot treats as a no-op — matching the prior behavior.
	var frameLenBuf [4]byte
	for {
		_, readErr := io.ReadFull(srcFile, frameLenBuf[:])
		if readErr == io.EOF {
			break
		}
		if readErr != nil {
			return "", &SnapshotEncryptError{Op: "decrypt", Err: ErrSnapshotCorrupt}
		}
		frameLen := binary.LittleEndian.Uint32(frameLenBuf[:])
		// A frame is exactly one encrypted plaintext chunk
		// (snapshotEncryptChunkSize) plus AEAD framing overhead (epoch + algID
		// + nonce + tag, < 256 bytes). Reject a corrupt/oversized length prefix
		// before allocating, so a damaged frame header cannot trigger a
		// multi-GB allocation during recovery.
		if frameLen > snapshotEncryptChunkSize+maxSnapshotFrameOverhead {
			return "", &SnapshotEncryptError{Op: "decrypt", Err: ErrSnapshotCorrupt}
		}

		frame := make([]byte, frameLen)
		if _, readErr = io.ReadFull(srcFile, frame); readErr != nil {
			return "", &SnapshotEncryptError{Op: "decrypt", Err: ErrSnapshotCorrupt}
		}

		// Decrypt using the epoch embedded in each frame's ciphertext header
		// rather than the single recorded snapshot epoch. A snapshot saved
		// concurrently with a key rotation can contain frames at different
		// epochs (earlier chunks at E, later chunks at E+1); recording one
		// epoch and decrypting every frame with it would fail AEAD on the
		// mismatched frames. barrier.Decrypt reads the embedded epoch and
		// selects the correct DEK, matching the WAL replay path. The recorded
		// epoch parameter is advisory only (it gates whether decryption is
		// attempted at all, and pins the DEK-purge floor at the minimum frame
		// epoch so no frame's DEK is purged while the snapshot is retained).
		plaintext, decErr := barrier.Decrypt(nil, frame)
		if decErr != nil {
			return "", &SnapshotEncryptError{Op: "decrypt", Err: decErr}
		}

		if _, err := tmpFile.Write(plaintext); err != nil {
			return "", &SnapshotEncryptError{Op: "write", Err: err}
		}
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

// snapshotFloorBlocker names one snapshot directory that could not be
// evaluated, and why.
type snapshotFloorBlocker struct {
	path      string
	shardID   uint64
	replicaID uint64
	reason    string
}

// snapshotFloorScan folds the retained snapshot directories of one or more
// shards into the minimum barrier epoch those artifacts still require.
//
// It NEVER short-circuits. The verdict is fixed the moment the first
// unevaluable directory is seen — an epoch that cannot be established may be
// any epoch, so purging must stop — but the scan continues so a single
// operator-facing log names EVERY blocking directory. Returning on the first
// one is what made this condition so hard to clear: the operator fixed the
// directory the log named and the next scan simply named the next.
//
// A directory can be unevaluable for two different reasons (a legacy record
// that predates the Epoch field, or an unreadable/corrupt one); both are
// recorded with their reason and both block. The distinction is diagnostic
// ONLY — see the note on purgeOldEpochs for why neither can be skipped.
type snapshotFloorScan struct {
	// minEpoch is the lowest epoch any evaluable retained artifact still
	// needs, or 0 when no evaluable artifact is encrypted.
	minEpoch uint64

	// blockedCount counts EVERY directory that could not be evaluated. Non-zero
	// means the floor is unusable and nothing may be purged. It is the verdict
	// and the operator-facing total; it is a counter rather than len(blockers)
	// because the slice below is deliberately bounded.
	blockedCount uint64

	// blockers is a bounded SAMPLE of the blocking directories, kept for the
	// operator log. This scan runs on EVERY snapshot save (purgeOldEpochs), on
	// a host that can hold thousands of shards, and an unevaluable directory is
	// a persistent condition rather than a transient one — so accumulating one
	// struct plus one path string per blocking directory per save allocated
	// tens of thousands of objects per save, forever, to print at most
	// maxReportedFloorBlockers of them.
	blockers []snapshotFloorBlocker
}

// maxReportedFloorBlockers bounds how many blocking directories a single log
// record names, and with it how many the scan retains. The COUNT is always
// reported in full; the paths are a sample.
const maxReportedFloorBlockers = 8

// floorReportInterval bounds how often the purge-suppression warning is
// emitted. The condition it describes is persistent — it clears only when the
// directories roll off or are repaired — while the scan that detects it runs
// after every snapshot save, so an unthrottled warning is one log record per
// save per shard saying the same thing.
const floorReportInterval = time.Minute

// lastFloorReport is the unix-nano time the suppression warning was last
// emitted, process-wide. Process-wide is the right scope: the message names a
// condition of the host's snapshot tree, and an operator needs it once, not
// once per shard.
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

// block records an unevaluable directory. The COUNT is always incremented — it
// is the verdict, and the scan must not short-circuit — while the retained
// sample stops at maxReportedFloorBlockers, which is all report can print.
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

// report logs the blocking directories as ONE record, so an operator can see at
// a glance why key purging is suppressed and which directories must roll off
// (or be repaired) before it resumes. The full count is always given; the paths
// are a bounded sample.
//
// It is rate-limited process-wide: the caller runs it after every snapshot
// save, and the condition it reports does not change between saves.
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

// globalSafePurgeFloor computes the minimum barrier epoch still required by
// ANY shard sharing this process-wide barrier. It is the single source of
// truth for safe DEK purging and is the minimum over:
//
//	(a) every loaded shard's retained encrypted-snapshot epoch, and
//	(b) the global WAL MinLiveEpoch (the LogDB is per-host, so MinLiveEpoch
//	    already spans all shards).
//
// The second return value is false when the floor cannot be safely
// determined: any snapshot-dir scan error, a MinLiveEpoch error, or when
// nothing encrypted is retained. Callers MUST NOT purge when ok is false.
// This is the fail-safe contract: when in doubt, purge nothing.
//
// The barrier (one per host) is shared by all shards, so a per-shard floor
// is unsafe — shard A could purge a DEK that shard B's retained snapshot
// still needs. Globalizing over all loaded nodes' snapshot dirs and the
// already-global WAL floor closes that data-loss hole.
func (p *snapshotPool) globalSafePurgeFloor() (uint64, bool) {
	// Scan every loaded shard's snapshot directories for the global minimum
	// retained encrypted-snapshot epoch. A nil enumerator (standalone tests)
	// means no global view is available and purging must not proceed.
	if p.nodes == nil {
		return 0, false
	}

	var scan snapshotFloorScan
	for _, node := range p.nodes() {
		scan.addNode(node)
	}
	if scan.blocked() {
		// Fail safe: at least one directory's epoch is indeterminate, so we
		// cannot know which DEKs are still referenced.
		scan.report("global")
		return 0, false
	}
	snapshotMinEpoch := scan.minEpoch

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

// perBarrierSafePurgeFloor computes the minimum barrier epoch still required by
// the records encrypted under a SINGLE target barrier. It is the per-tenant
// analogue of globalSafePurgeFloor and is the minimum over:
//
//	(a) the retained encrypted-snapshot epoch of every loaded shard whose
//	    barrier is THIS barrier (barrierForShard(shardID) == target), and
//	(b) the WAL min-live epoch restricted to THIS barrier's records, obtained
//	    via PerBarrierEpochAwareLogDB.MinLiveEpochForBarrier(target).
//
// This is the core of Invariant 1 (per-barrier purge floor) and Invariant 3
// (per-shard isolation): barrier A's floor never counts barrier B's epochs, so
// purging A can neither over-retain (A keeps only what A needs) nor under-retain
// (A never purges an epoch some A-encrypted snapshot or WAL record still needs).
//
// The second return value follows the same fail-safe contract as
// globalSafePurgeFloor: false on a nil node enumerator, any snapshot-dir scan
// error, a WAL query error, an unsupported (non-per-barrier) LogDB, or when
// nothing encrypted is retained for this barrier. Callers MUST NOT purge when
// ok is false.
func (p *snapshotPool) perBarrierSafePurgeFloor(target SnapshotBarrier) (uint64, bool) {
	if p.nodes == nil || target == nil {
		return 0, false
	}

	// Snapshot the currently-loaded shards mapped to the target barrier, indexed
	// by shard ID so the completeness guard below can check membership.
	loadedForTarget := make(map[uint64]*Node)
	for _, node := range p.nodes() {
		nodeBarrier, barrierErr := p.barrierForShard(node.ShardID())
		if barrierErr != nil {
			// A loaded shard whose barrier cannot be resolved may or may not
			// belong to the target: it can neither be scanned nor excluded, so
			// its retained epochs cannot be proven absent from this floor.
			return 0, false
		}
		if nodeBarrier != target {
			continue // belongs to a different barrier; do not let it lower/raise this floor
		}
		loadedForTarget[node.ShardID()] = node
	}

	// Completeness guard (MED-2): when the LogDB can enumerate the COMPLETE,
	// fixed set of shards mapped to this barrier (a dedicated per-tenant
	// barrier), every such shard MUST be currently loaded so its retained
	// snapshots are visible to the scan below. A mapped shard that is
	// transiently absent (e.g. mid-restart) would silently drop its
	// possibly-older snapshot epochs from the floor, over-purging the SHARED
	// barrier's DEKs and stranding the data those snapshots still need. When the
	// shard set is not enumerable (known == false: a shared/system barrier whose
	// membership is open-ended, or a barrier this LogDB does not own) the guard
	// is skipped and the scan falls back to the loaded shards — matching the
	// global single-barrier behavior; an unowned barrier is independently caught
	// by the WAL known-check below.
	if enum, ok := p.logdb.(logdb.BarrierShardEnumerator); ok {
		if wantShards, known := enum.ShardsForBarrier(target); known {
			for _, sid := range wantShards {
				if _, present := loadedForTarget[sid]; !present {
					return 0, false // a mapped shard is not loaded: fail safe
				}
			}
		}
	}

	// (a) Snapshot floor restricted to shards mapped to the target barrier. A
	// directory belonging to a DIFFERENT barrier is never scanned here, so an
	// indeterminate directory under barrier B cannot suppress purging for
	// barrier A: the blast radius of an unevaluable directory is exactly the
	// set of shards whose DEKs it could name.
	var scan snapshotFloorScan
	for _, node := range loadedForTarget {
		scan.addNode(node)
	}
	if scan.blocked() {
		scan.report("barrier")
		return 0, false // fail safe: this barrier has an indeterminate directory
	}
	snapshotMinEpoch := scan.minEpoch

	// (b) WAL floor restricted to the target barrier's records. When a LogDB
	// is present it MUST support per-barrier epoch queries; a plain
	// EpochAwareLogDB only exposes a cross-barrier minimum, which would be
	// unsafe to attribute to a single barrier, so we fail safe. A nil LogDB
	// (memory mode) has no WAL records and contributes 0.
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
			// The LogDB does not own / cannot attribute this barrier. This is
			// the UNSAFE case a bare epoch==0 used to mask: we cannot prove no
			// live WAL record needs an old DEK, so we must not purge.
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

// purgeOldEpochs purges barrier DEKs for epochs strictly below the safe-purge
// floor. It is called after log compaction and snapshot garbage collection to
// reclaim old key material.
//
// Single-barrier mode (shardBarriers == nil): the process-wide barrier encrypts
// every shard, so the floor MUST be the GLOBAL minimum across all shards'
// retained snapshots and the cross-shard WAL floor. Computing it from only the
// calling shard would let one shard purge a DEK another shard's retained
// snapshot still needs. This path is byte-for-byte identical to the original.
//
// Per-shard-barrier mode (shardBarriers != nil): the floor and the purge are
// dispatched to the SINGLE barrier mapped to shardID, using a per-barrier floor.
// Purging barrier A then only ever removes A's DEKs and only below A's own live
// epoch, so it can never affect epochs still live under barrier B.
//
// The barrier's in-flight epoch protection inside PurgeEpochsBefore remains the
// last line of defense in both modes.
//
// # Why an unevaluable snapshot directory cannot simply be skipped
//
// The floor is a MINIMUM over the retained artifacts. A directory whose epoch
// cannot be established constrains that minimum from BELOW by an unknown
// amount: it may hold ciphertext sealed at any epoch, and the record that would
// have said which is exactly the one that is missing. Excluding it and purging
// below the other directories' minimum would destroy the DEK it needs — the
// artifact becomes permanently undecryptable and StartShard fails closed on it.
// So "this directory cannot be evaluated" can only mean "do not purge"; it can
// never mean "this directory imposes no floor". The two are opposites, and no
// evidence available to the scan distinguishes them (a pre-Epoch-field record
// was written by builds whose receive path DID re-encrypt under the receiver's
// barrier, so such a directory is frequently encrypted).
//
// What IS bounded is the blast radius. An unevaluable directory suppresses
// purging only for the barrier that could have sealed it: in per-shard-barrier
// mode that is the one barrier its shard maps to, and other tenants' barriers
// keep purging normally. In single-barrier mode every shard shares one keyring,
// so there is no smaller scope to fall back to — suppression is host-wide by
// construction, not by conservatism.
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

// applyPurge invokes PurgeEpochsBefore on the given barrier when the floor is
// above the initial epoch. Epoch 1 has no predecessor to purge. Shared by the
// global and per-barrier purge paths so both log and guard identically.
func (p *snapshotPool) applyPurge(barrier SnapshotBarrier, purgeFloor, shardID, replicaID uint64) {
	if purgeFloor <= 1 {
		return
	}
	purged, purgeErr := barrier.PurgeEpochsBefore(purgeFloor)
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

// Snapshot scratch-file naming. EVERY path that writes a sibling scratch file
// into a snapshot directory must build its os.CreateTemp pattern from these
// constants, because SweepStaleScratchTmp is what reclaims such a file when the
// process dies before its owner can remove it.
//
// The DECRYPT names carry the higher stake: an orphan under one of them is a
// COMPLETE PLAINTEXT COPY of the state machine left next to the ciphertext for
// the life of the snapshot directory. The ENCRYPT names hold ciphertext, so an
// orphan is a disk leak rather than a confidentiality one — but it is the same
// unswept-name class, and the invariant that makes the sweeper trustworthy is
// that a name which can be CREATED is by construction a name that can be
// RECLAIMED. Adding a path that writes a scratch file means adding its prefix
// here and to IsSnapshotScratchTemp, not inventing an unswept name.
//
// The prefixes differ only so an orphan names the path that created it.
const (
	// ScratchTempSuffix terminates every snapshot scratch file name.
	ScratchTempSuffix = ".tmp"

	// DecryptTempSuffix is the historical name for ScratchTempSuffix, kept
	// because the decrypt patterns below read more clearly with it.
	DecryptTempSuffix = ScratchTempSuffix

	// RecoverDecryptTempPrefix marks a scratch file written by a snapshot
	// RECOVERY decrypt (snapshotPool.decryptSnapshotToSibling and
	// Host.StartShard's local-snapshot recovery). Holds PLAINTEXT.
	RecoverDecryptTempPrefix = ".decrypt-"

	// SendDecryptTempPrefix marks a scratch file written by the transport
	// SEND decrypt (Host's transportSender.decryptSnapshotForSend). Holds
	// PLAINTEXT.
	SendDecryptTempPrefix = ".send-decrypt-"

	// RecvEncryptTempPrefix marks the scratch file an in-place re-encryption of
	// a just-received or just-imported artifact writes its output to
	// (quicraft.reencryptReceivedSnapshot). Holds CIPHERTEXT.
	RecvEncryptTempPrefix = ".recv-encrypt-"

	// ReencryptTempPrefix marks the scratch file a barrier-to-barrier snapshot
	// migration writes its output to (quicraft.ReencryptSnapshotFile). Holds
	// CIPHERTEXT.
	ReencryptTempPrefix = ".reencrypt-snapshot-"

	// SaveEncryptTempPrefix is the prefix for the scratch file saveSnapshot
	// encrypts a freshly captured snapshot into before renaming it over
	// snapshot.dat. It holds CIPHERTEXT, so an orphan is a disk leak rather than
	// a confidentiality one — but it is the same unswept-name class, and the
	// invariant that makes the sweeper trustworthy is that a name which can be
	// CREATED is by construction a name that can be RECLAIMED.
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

// IsSnapshotScratchTemp reports whether a directory entry name is a snapshot
// scratch file produced by any encrypt or decrypt path. It is the single
// matcher the sweeper uses, so a name that can be CREATED is by construction a
// name that can be RECLAIMED.
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

// Snapshot import staging-directory naming. Host.ImportSnapshot writes the
// artifact into a directory it provably owns and publishes it with a single
// rename, so a half-written import is never visible under a canonical
// "snapshot-<index>" name.
//
// The prefix lives here, beside the decrypt scratch names, for the reason those
// names do: the staging directory holds the artifact PLAINTEXT for the window
// between the write and the re-encryption (an exported artifact carries
// plaintext by construction), so a process killed inside that window leaves a
// COMPLETE PLAINTEXT COPY of the state machine on disk. Unlike a partially
// written canonical directory, which snapshot garbage collection eventually
// reclaims, a staging directory is invisible to listSnapshotDirs — nothing else
// on the host will ever look at it again. It must therefore be SWEPT, and the
// prefix a writer builds its name from must be the same prefix the sweeper
// matches.
const (
	// ImportStageDirPrefix marks a directory an import stages its artifact in.
	// The leading dot and the "import" word make an orphan self-identifying,
	// and the name deliberately cannot parse as "snapshot-<index>".
	ImportStageDirPrefix = ".import-"

	// ImportStageDirPattern is the os.MkdirTemp pattern for a staging
	// directory. Every import must build its name from this.
	ImportStageDirPattern = ImportStageDirPrefix + "*"
)

// IsSnapshotImportStage reports whether a directory entry name is an import
// staging directory. It is the single matcher the sweeper uses, so a name that
// can be CREATED is by construction a name that can be RECLAIMED.
func IsSnapshotImportStage(name string) bool {
	return strings.HasPrefix(name, ImportStageDirPrefix)
}

// SweepStaleDecryptTmpTree removes orphaned snapshot scratch files from EVERY
// retained snapshot directory of the given shard/replica, not just the one
// currently being recovered.
//
// The whole tree must be swept because the transport send path decrypts inside
// whichever snapshot directory it is streaming — which need not be the newest
// retained snapshot — and a leader killed mid-send orphans a full plaintext copy
// there. Sweeping only the recovered directory leaves that copy on disk
// permanently: the snapshot directory is retained indefinitely, so the leak is
// too, while snapshot.dat next to it still reads as ciphertext.
//
// Errors are logged and ignored: a failed cleanup must not abort startup.
func SweepStaleDecryptTmpTree(baseDir string, shardID, replicaID uint64) {
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

// sweepImportStage runs SweepStaleImportStageDirs only when the shard's import
// lock can be taken without waiting, and releases it afterwards. A nil tryLock
// means no host owns the caller (standalone engine tests), so nothing can be
// importing and the sweep runs unconditionally.
//
// The wait must never happen: an import can run for as long as it takes to
// write a multi-gigabyte artifact, and neither StartShard nor snapshot recovery
// may block behind one. "An import holds the lock" is itself the answer —
// whatever the sweep would have found belongs to that import.
func sweepImportStage(tryLock func(shardID uint64) (func(), bool), baseDir string, shardID, replicaID uint64) {
	if tryLock == nil {
		SweepStaleImportStageDirs(baseDir, shardID, replicaID)
		return
	}
	release, ok := tryLock(shardID)
	if !ok {
		return
	}
	defer release()
	SweepStaleImportStageDirs(baseDir, shardID, replicaID)
}

// SweepStaleImportStageDirs removes orphaned import staging directories from a
// shard/replica's snapshot tree.
//
// An import stages the artifact PLAINTEXT (an exported artifact is decrypted
// before it is written, so an import never carries ciphertext) and re-encrypts
// it under this host's own barrier immediately afterwards. A process killed
// between the two leaves the complete plaintext state machine in the staging
// directory.
//
// Nothing else ever reclaims it. listSnapshotDirs — and with it snapshot
// garbage collection, the recovery fallback and the DEK purge-floor scan — skips
// every name without the "snapshot-" prefix, which is precisely the property
// that keeps a half-written import out of those scans. Without this sweep the
// staging name converts a plaintext exposure that GC would eventually have
// reclaimed into a PERMANENT one, which is the outcome
// staleDecryptSweepFailures exists to make visible.
//
// The sweep is strictly scoped: it only removes direct children of the
// shard/replica base directory whose names match IsSnapshotImportStage, so it
// can never touch a canonical snapshot directory. Errors are logged and
// otherwise ignored: a failed cleanup must not abort startup or recovery.
//
// The caller must ensure no import is in flight for the shard, since a live
// staging directory is indistinguishable from an orphaned one by name — use
// sweepImportStage, which takes the shard's import lock to establish exactly
// that.
func SweepStaleImportStageDirs(baseDir string, shardID, replicaID uint64) {
	base := snapshotBaseDir(baseDir, shardID, replicaID)
	entries, err := os.ReadDir(base)
	if err != nil {
		if !os.IsNotExist(err) {
			// The sweep could not even look. Whether a plaintext copy is
			// sitting there is now unknown, which is the same operational state
			// as a failed removal: report it the same way.
			noteStaleDecryptSweepFailure(base, "read_dir", err)
		}
		return
	}
	for _, entry := range entries {
		if !entry.IsDir() || !IsSnapshotImportStage(entry.Name()) {
			continue
		}
		stalePath := filepath.Join(base, entry.Name())
		if rmErr := os.RemoveAll(stalePath); rmErr != nil && !os.IsNotExist(rmErr) {
			noteStaleDecryptSweepFailure(stalePath, "remove_all", rmErr)
			continue
		}
		slog.Warn("snapshot: reclaimed an orphaned import staging directory; it held the imported state machine in PLAINTEXT",
			"path", stalePath,
			"shard_id", shardID,
			"replica_id", replicaID,
		)
	}
}

// SweepStaleDecryptTmp removes orphaned snapshot scratch files from the given
// snapshot directory: every name IsSnapshotScratchTemp matches, which is every
// name a snapshot encrypt or decrypt path can create (see the prefix block
// above). They are normally removed via defer once the caller is done, but a
// process crash orphans them. This sweep, run at startup and at recovery start,
// prevents both a disk leak and — for the decrypt prefixes, which hold
// PLAINTEXT — an indefinitely retained copy of the state machine.
//
// The sweep is strictly scoped to the snapshot directory and only matches
// IsSnapshotScratchTemp names, so it never touches the canonical
// snapshot.dat / snapshot.meta files or any unrelated content. Errors are
// logged and ignored: a failed cleanup must not abort recovery.
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
		if rmErr := os.Remove(stalePath); rmErr != nil && !os.IsNotExist(rmErr) {
			noteStaleDecryptSweepFailure(stalePath, "remove", rmErr)
		}
	}
}

// staleDecryptSweepFailures counts the times the sweeper could not reclaim (or
// could not even inspect) a snapshot decrypt scratch file or an import staging
// directory.
//
// A non-zero value means a COMPLETE PLAINTEXT COPY of a state machine may be
// sitting on disk next to its ciphertext, for the life of the snapshot
// directory — or, for an import staging directory, indefinitely. That is the exact outcome the sweeper exists to prevent, and its
// usual causes — a read-only mount, a permissions or ownership change, an
// immutable attribute — are silent, persistent, and invisible in the data path.
// The counter is monotonic and process-wide; a monitor should alert on any
// increase.
var staleDecryptSweepFailures atomic.Uint64

// StaleDecryptSweepFailures returns the number of snapshot decrypt scratch
// files and import staging directories this process has failed to reclaim or
// inspect since start. See
// staleDecryptSweepFailures for why a non-zero value is a security condition
// and not merely a disk-space one.
func StaleDecryptSweepFailures() uint64 {
	return staleDecryptSweepFailures.Load()
}

// noteStaleDecryptSweepFailure records a failed sweep. It logs at WARN, not
// DEBUG: the failure leaves plaintext state-machine data at rest with nothing
// else to signal it — no error is returned to any caller (the sweep must never
// abort startup or recovery), and the data path behaves normally.
func noteStaleDecryptSweepFailure(path, op string, err error) {
	staleDecryptSweepFailures.Add(1)
	slog.Warn("snapshot: scratch path not reclaimed; plaintext state machine data may remain at rest",
		"path", path,
		"op", op,
		"error", err,
		"total_failures", staleDecryptSweepFailures.Load(),
	)
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
		// Shutdown may have raced the send. The workers drain requestC as
		// they exit, but one that already exited cannot, so re-check and
		// drain here: whichever request this drain pulls out (ours or
		// another producer's) is released with exactly the same cleanup,
		// so no queued request is left holding inFlight and the node's
		// snapshotting flag after the pool is gone.
		select {
		case <-p.stopC:
			p.drainRequests()
		default:
		}
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

// SnapshotSaveDir is snapshotSaveDir for callers outside the engine (the Host's
// snapshot import path). Every writer of a snapshot artifact must use it so the
// artifact lands in the tree garbage collection and the DEK purge-floor scan
// enumerate; see Node.SnapshotSaveDir for why writing elsewhere destroys keys.
func SnapshotSaveDir(baseDir string, shardID, replicaID, index uint64) string {
	return snapshotSaveDir(baseDir, shardID, replicaID, index)
}

// snapshotBaseDir returns the per-shard/replica base directory.
func snapshotBaseDir(baseDir string, shardID, replicaID uint64) string {
	return filepath.Join(
		baseDir,
		fmt.Sprintf("shard-%d", shardID),
		fmt.Sprintf("replica-%d", replicaID),
	)
}

// DecompressingSnapshotReader wraps r in the decompressor matching the
// compression an artifact was WRITTEN through. It is the single decision
// point shared by both recovery paths (snapshotPool.recoverSnapshot and
// Host.StartShard), so neither can derive the chain from its own config.
//
// An algorithm this binary cannot decompress is rejected with
// ErrUnsupportedSnapshotCompression rather than read as plain bytes: guessing
// hands the state machine an unknown encoding and corrupts it silently.
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

// writeSnapshotMetadata writes the metadata record to the snapshot directory.
// The layout and its compatibility rules are documented on MarshalSnapshotMeta.
//
// Epoch is 0 for unencrypted snapshots; Compression is 0 (NoCompression) for
// uncompressed ones. Both describe the artifact next to it, so a directory
// that outlives its LogDB record still says how to read itself.
func writeSnapshotMetadata(dir string, index, term, shardID, replicaID, epoch uint64,
	compression config.CompressionType) error {
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

	buf := MarshalSnapshotMeta(SnapshotMeta{
		Index:       index,
		Term:        term,
		ShardID:     shardID,
		ReplicaID:   replicaID,
		Epoch:       epoch,
		Compression: compression,
	})
	if _, err := f.Write(buf); err != nil {
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

	// Fsync the export directory so the exported files' directory entries
	// survive a crash.
	if err := SyncDir(exportPath); err != nil {
		return &SnapshotExportError{
			ExportPath: exportPath,
			Err:        err,
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
