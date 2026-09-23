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

	// EncryptWithEpochContext is EncryptWithEpoch with a caller-supplied
	// context folded into the AEAD AAD alongside the epoch. An empty context
	// is byte-identical to EncryptWithEpoch. Snapshot frames always supply the
	// positional context built by NewSnapshotAAD.
	EncryptWithEpochContext(dst, plaintext, contextAAD []byte) ([]byte, uint64, error)

	// EncryptWithEpochPinnedContext is EncryptWithEpochContext that also
	// registers the epoch it sealed under as in-flight, in ONE atomic step
	// under the barrier's own lock; the caller must DeregisterInFlightEpoch it.
	//
	// It is required, not optional. EncryptWithEpoch releases the lock before
	// returning, so a Rotate followed by PurgeEpochsBefore could otherwise
	// complete in the gap before the caller pinned the returned epoch —
	// purging the DEK of ciphertext about to be persisted, which would then be
	// permanently undecryptable.
	EncryptWithEpochPinnedContext(dst, plaintext, contextAAD []byte) ([]byte, uint64, error)

	// Decrypt decrypts ciphertext using the epoch embedded in the ciphertext
	// prefix. Returns the plaintext.
	Decrypt(dst, ciphertext []byte) ([]byte, error)

	// DecryptWithContext decrypts ciphertext that was sealed with the same
	// caller context. Supplying a different context — or omitting one that was
	// present at encryption time — fails authentication. There is deliberately
	// no weaker retry: the fallback is the vulnerability.
	DecryptWithContext(dst, ciphertext, contextAAD []byte) ([]byte, error)

	// DecryptForEpoch decrypts ciphertext using the DEK for the given epoch.
	DecryptForEpoch(dst, ciphertext []byte, epoch uint64) ([]byte, error)

	// IsHardwareMode reports whether the barrier delegates sealing to an
	// external encryptor. It says nothing about whether that encryptor can
	// authenticate caller context — use SupportsContextAAD for that.
	IsHardwareMode() bool

	// SupportsContextAAD reports whether the barrier can ACTUALLY authenticate
	// the caller context snapshot frames are bound to. It is true for a
	// software barrier and for a delegated encryptor that carries AAD end to
	// end; false for a delegated encryptor that cannot, and in no-encryption
	// mode. Frames are bound to their position exactly when it is true; see
	// NewSnapshotAAD.
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

// Snapshot-pool sentinel errors. They live here rather than in errors.go
// because they are produced only by the snapshot pool's save/recover/enumerate
// paths; keeping them beside those paths keeps the typed-error contract local.
var (
	// ErrSnapshotArtifactMissing is returned by recoverSnapshot when a LogDB
	// snapshot record points at a directory whose snapshot.dat no longer
	// exists. It is a typed FAILURE — never a silent success — so the leader's
	// ack-expiry -> re-send -> dedup -> re-recovery path engages, matching the
	// encrypted branch that already fails on a missing artifact. Reporting the
	// missing artifact as recovery success would clear the recovering gate
	// without seeding lastApplied, stranding the shard behind its own recorded
	// snapshot until an ErrApplyGap kills it or it serves stale reads.
	ErrSnapshotArtifactMissing = errors.New(
		"engine: recorded snapshot artifact (snapshot.dat) is missing")

	// ErrSnapshotNotFound is returned by latestSnapshotDir when no snapshot
	// directory exists on disk for a shard/replica. It is distinct from
	// ErrStopped so a storage-state answer ("there is no snapshot") is never
	// conflated with a shutdown signal.
	ErrSnapshotNotFound = errors.New("engine: no snapshot directory found")
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

// settleRecovery acts on how a recovery request ended — abandoned because its
// node stopped (the caller was refused the node's write lease), declined as
// stale, or failed — and lowers the recovering gate. It
// reports whether the recovery was abandoned, in which case no callback may
// report it.
func (p *snapshotPool) settleRecovery(req snapshotRequest, err error, stopped bool) (abandoned bool) {
	var stale *SnapshotRecoveryStaleError
	switch {
	case stopped:
		// Its node stopped under it — whether the restore then failed or not.
		// A failure is the unload, not a failed restore, and a success
		// restored a state machine that is discarded with the node; either
		// way its successor recovers from the LogDB record on start. Failing
		// the node, or reporting to callbacks that resolve by shard ID, would
		// land on that successor instead: a spurious shard failure, or its
		// pending snapshot requests completed with this recovery's result.
		abandoned = true
		slog.Info("snapshot recovery abandoned: the node stopped",
			"shard_id", req.shardID,
			"replica_id", req.replicaID,
			"error", err,
		)
	case errors.As(err, &stale):
		// NOT a shard failure, and nothing to do.
		//
		// recoveryBlockedByLog produces this refusal BEFORE the recovering
		// gate is raised and before RecoverFromSnapshot is called, so the
		// state machine has not been touched. The reasoning below — a
		// possibly half-restored state machine — does not apply to it.
		//
		// It is also a NORMAL condition rather than a fault. A leader that
		// has just been elected rebuilds every remote's next index, and a
		// rejection walk-down can drive one below the compacted floor, so it
		// ships an InstallSnapshot to a follower that is not actually behind.
		// raft.handleSnapshot rejects that snapshot itself ("snapshot
		// rejected, behind committed index") and answers with the follower's
		// real committed index, which is how the leader learns to stop
		// sending it. The snapshot receive path queues recovery only for an
		// install raft ADOPTED, so that case no longer reaches the pool from
		// a receive; the dedup path's re-delivery of an already-recorded
		// snapshot still can, and a restart racing a new commit can too.
		//
		// The state machine is at or past the snapshot index, so it already
		// holds everything that snapshot represents: declining IS the correct
		// and complete action. Failing the shard here turned a race the
		// protocol handles by itself into a dead shard (#106).
		slog.Info("snapshot recovery declined: the state machine is already past "+
			"this snapshot, and raft rejected it too",
			"shard_id", req.shardID,
			"replica_id", req.replicaID,
			"snapshot_index", stale.SnapshotIndex,
			"last_applied", stale.LastApplied,
		)
	case err != nil:
		// A FAILED restore must fail the shard, not resume applies on it.
		//
		// StateMachine.RecoverFromSnapshot now stages the session table so
		// a failure cannot leave sessions and user state at different
		// indexes — but the USER state machine is still free to have
		// mutated itself before returning an error. sm.StateMachine
		// documents all-or-nothing only for Update, not for
		// RecoverFromSnapshot, and the engine has no way to roll a user SM
		// back. Resuming the apply pipeline against a possibly
		// half-restored state machine applies committed entries onto an
		// undefined base — divergence with every Raft invariant intact.
		//
		// Failing the shard is the same terminal state a permanent
		// SaveState failure reaches. Recovery is an operator action:
		// restart the shard, which builds a fresh state machine instance
		// and replays from the last good snapshot.
		slog.Error("snapshot recovery failed; failing the shard rather "+
			"than resuming applies on a possibly half-restored state machine",
			"shard_id", req.shardID,
			"replica_id", req.replicaID,
			"error", err,
		)
		req.node.Fail(err)
		if p.callback != nil {
			p.callback.OnShardFailed(req.shardID, err)
		}
	}
	req.node.SetRecovering(false)
	if p.workSignal != nil {
		p.workSignal.Notify(req.shardID)
	}
	return abandoned
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
	var (
		index      uint64
		compaction LogCompaction
	)
	err := blocked
	if err == nil {
		index, compaction, err = p.handleSnapshot(req)
	}

	// Mark that the snapshot operation itself finished, so a later panic is
	// reported as a post-completion panic rather than a rejected snapshot.
	completed = true

	// Clear the recovering flag AFTER recovery completes
	// (success or error). Signal the work signal so the step
	// worker re-evaluates this node immediately rather than
	// waiting for the next tick.
	// Every outcome is reported by shard ID — a failed node, the completion
	// that resolves the shard's pending snapshot requests, the compaction
	// event — so it is settled and reported under the node's write lease.
	// UnloadNode cannot complete while it is held, so no next incarnation
	// exists to receive a report meant for this one; a node that has stopped
	// refuses the lease, and its request is abandoned — its own pending
	// requests are failed by the teardown instead. The host dispatches every
	// user EventListener fan-out from these callbacks, so no user code runs
	// under the lease. The release is deferred so a panicking callback cannot
	// leak the lease and hang the unload. A save's own lease was released
	// when it returned, so this is never a nested acquire.
	leased := req.node.AcquireWriteLease()
	if leased {
		defer req.node.ReleaseWriteLease()
	}
	abandoned := !leased
	if !req.save {
		abandoned = p.settleRecovery(req, err, !leased)
	}
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
	// Deliver the completion BEFORE the in-flight/snapshotting slots are
	// released. The callback resolves the pending requests registered for this
	// shard WITHOUT matching them to a specific request, so clearing the slots
	// first opens a window in which a brand-new request registers and is then
	// completed with THIS snapshot's index or error — an export caller would be
	// told its backup succeeded before its export directory exists. Both slots
	// stay claimed across the callback, so no later request can be admitted
	// until it has drained.
	if p.callback != nil && !abandoned {
		p.callback.OnSnapshotCompleted(req.shardID, index, err)
		// After the completion, so a listener sees the snapshot before the
		// compaction it made possible, and still inside the held slots.
		if err == nil && compaction.Reader {
			p.callback.OnLogCompacted(req.shardID, req.replicaID, compaction)
		}
	}
	// Release the slots so the next request for this shard can be accepted.
	p.inFlight.Delete(req.shardID)
	req.node.snapshotting.Store(false)
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

// saveSnapshotCompacting saves the state machine state to a snapshot directory,
// writes metadata, persists snapshot info to LogDB, garbage collects
// old snapshots, and compacts the log. Returns the snapshot index on
// success, and what log compaction did (see LogCompaction; the zero value when
// nothing was compacted).
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
		return 0, LogCompaction{}, &SnapshotError{
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
		return 0, LogCompaction{}, nil
	}

	// A non-advancing index is a NO-OP, and it must be decided here, before any
	// directory is touched. saveDir below is derived purely from the index, so a
	// second save at an index the durable snapshot already covers resolves to the
	// SAME directory and opens its snapshot.dat with O_TRUNC — destroying the
	// artifact LogDB records for as long as the state machine takes to
	// re-serialize (minutes for a large SM), and discarding the directory
	// outright on any failure before the metadata write. The compacted entries it
	// covers are already gone, so a single-node deployment loses its state
	// permanently, and a crash inside the truncate window does the same with no
	// failure path involved. The re-save could not even be RECORDED: LogDB
	// rejects a snapshot whose index does not advance, which turned what should
	// be a no-op into an operator-visible shard failure.
	//
	// The authority here is the DURABLE LogDB record, NOT node.lastSnapshotIndex:
	// SeedLastApplied seeds that atomic with the recovered APPLIED index on
	// restart, where no artifact need exist at that index at all — gating on it
	// would refuse the first snapshot after every restart.
	//
	// An EXPORT is not a pure no-op — its caller asked for an artifact at
	// exportPath — so it fails loudly rather than reporting a success that
	// produced no export. It already failed (at the LogDB record) before this
	// guard existed; the difference is that it now fails without touching the
	// recorded snapshot.
	//
	// p.logdb is nil ONLY in tests that construct a pool without one. Production
	// always has a LogDB (Host wires it at NewHost), so the guard is never
	// silently skipped in a deployment; a nil LogDB also means there is no
	// durable record to destroy.
	//
	// rebuildingRecordedArtifact marks the one non-advancing save that must
	// still run: the record survived but its artifact did not.
	rebuildingRecordedArtifact := false
	// rebuildFileSize is the FileSize the surviving record carries. A rebuild
	// keeps that record, and recovery rejects an artifact whose size differs
	// from it, so a rebuild publishes only a file of exactly that size.
	var rebuildFileSize uint64
	if p.logdb != nil {
		rec, recErr := p.logdb.GetSnapshot(req.shardID, req.replicaID)
		switch {
		case errors.Is(recErr, logdb.ErrNoSnapshot):
			// No record yet: this is the FIRST save. Proceed.
		case recErr != nil:
			// The record state is UNKNOWN, and this branch exists precisely to
			// keep a recorded artifact from being replaced. Falling through on
			// a read error would run a save whose publish then has to decide
			// whether the canonical directory is recorded — through the SAME
			// call — so one persistent read error would leave the whole
			// decision to a guess. Fail the save instead; the snapshot on disk
			// stays intact and the next attempt retries.
			return 0, LogCompaction{}, &SnapshotError{
				ShardID:   req.shardID,
				ReplicaID: req.replicaID,
				Save:      true,
				Err:       recErr,
			}
		case rec.Index != 0 && rec.Index >= snapshotIndex:
			// An export never rides on a non-advancing save, whatever the state
			// of the recorded artifact: its caller asked for an artifact at
			// exportPath, and the LogDB record could not be advanced to describe
			// one.
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
				// The artifact is gone, but a save at THIS index resolves to a
				// different directory than the one the record names, so it would
				// leave the record dangling and add an orphan beside it. Nothing
				// useful to do until the index advances past the record.
				slog.Error("snapshot: recorded snapshot artifact is missing and cannot be rebuilt at this index",
					"shard_id", req.shardID,
					"replica_id", req.replicaID,
					"snapshot_index", snapshotIndex,
					"recorded_index", rec.Index,
					"recorded_path", rec.Filepath,
				)
				return 0, LogCompaction{}, nil
			case filepath.Clean(rec.Filepath) != filepath.Clean(snapshotSaveDir(node.SnapshotDir(), req.shardID, req.replicaID, snapshotIndex)):
				// The record names a directory other than the one a save at this
				// index publishes into — a legacy or relocated layout. Rebuilding
				// the canonical one would leave the record naming the still-missing
				// original, and LogDB cannot be re-pointed at the same index.
				slog.Error("snapshot: recorded snapshot artifact is missing and its recorded path is not the one a rebuild writes",
					"shard_id", req.shardID,
					"replica_id", req.replicaID,
					"snapshot_index", snapshotIndex,
					"recorded_path", rec.Filepath,
				)
				return 0, LogCompaction{}, nil
			case (rec.Epoch == 0) != (barrier == nil) || rec.Compression != node.cfg.SnapshotCompressionType:
				// The record survived but its artifact did not, and the
				// artifact cannot be rebuilt to match it. A rebuild keeps the
				// record — LogDB rejects a same-index update — so recovery
				// reads the new bytes through the OLD record's format: an
				// encrypted rebuild under a plaintext record (Epoch 0) is never
				// decrypted, and a Snappy rebuild under a NoCompression record
				// is fed to the state machine compressed. The encryption or
				// compression setting has changed since that snapshot was
				// taken, so only a snapshot at a NEW index can repair this.
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
				// The record survived but its artifact did not (operator
				// cleanup, a partial restore). Skipping on the record alone
				// would make that permanent for a shard taking no further
				// writes — a single-node deployment could then only be repaired
				// by streaming from a peer it does not have. Rebuild it in
				// place, at the index and directory the record already names.
				//
				// The record itself is NOT rewritten: every LogDB rejects a
				// non-advancing index. Its Epoch and FileSize therefore stay at
				// the pre-loss values, which is safe in both directions — an
				// older recorded epoch only holds the DEK purge floor LOWER than
				// the rebuilt frames need, and recoverLogReader re-measures the
				// file on the next restart.
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

	// L21: capture cluster membership ONCE, here under the apply gate together
	// with the (index, term) pair. The LogDB record and the LogReader record
	// below both derive from this single snapshot; reading membership twice
	// (once per record, after the gate released) let a config change committing
	// mid-save give the two records DIFFERENT memberships. A node with no peer
	// contributes empty membership, exactly as before.
	var membership proto.Membership
	if peer := node.Peer(); peer != nil {
		membership = peer.GetMembership()
	}

	// The membership must be the one in effect AT snapshotIndex, and the two
	// come from DIFFERENT frontiers, so that has to be checked rather than
	// assumed.
	//
	// snapshotIndex is the APPLY frontier: it is what the state machine has
	// consumed, which is what SaveSnapshot is about to serialize. Membership is
	// folded at COMMIT time (Peer.Commit -> processConfigChanges, the
	// commit-time semantics ProposeConfigChange documents), and commit runs
	// ahead of apply — by construction here, because the saving gate this code
	// holds STOPS applies while the step worker keeps committing.
	//
	// So a config change committed at C > snapshotIndex is already in the
	// published membership while its entry has not been applied, and recording
	// it produces a snapshot that claims a configuration which did not exist at
	// its own index. A follower that installs it adopts membership as of C and
	// then sets configFoldedThrough to the snapshot's index, so every entry in
	// (index, C] is offered to the fold a SECOND time; and until it catches up
	// it computes quorums from a voting set the log at that index does not
	// justify.
	//
	// Membership.ConfigChangeID is the log index of the last config change
	// folded into it, which makes the test exact: anything applied at or below
	// snapshotIndex was necessarily committed — and therefore folded — first, so
	// ConfigChangeID <= snapshotIndex means the captured membership is precisely
	// the one in effect at snapshotIndex.
	//
	// The condition is TRANSIENT and clears on its own: it lasts only until the
	// apply frontier passes C, which the very act of returning here permits (the
	// deferred release re-opens the gate). The save is refused rather than
	// recorded wrong, and the next trigger takes it.
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

	// STAGE, then publish. The save writes its artifact — the capture,
	// snapshot.meta, the export — into a staging directory of its own, and
	// only the publish below, which renames it onto the canonical
	// snapshot-<index> name and records it, runs under the node's write lease.
	//
	// The canonical directory is shared with a snapshot receive at the same
	// index, so writing there needs the lease UnloadNode waits out. Holding it
	// across the capture, as an earlier form did, made UnloadNode wait for the
	// user state machine — and a SaveSnapshot that waited on a proposal to its
	// own shard then deadlocked, since UnloadNode is what fails that proposal.
	// (A state machine that IGNORES the stopper still delays StopShard, at
	// its own Close, which must wait for an in-flight stream; that is the
	// stopper contract in pkg/sm, and was always so.) Nothing any other writer
	// can see is touched before the publish, so the capture needs no fence. A save
	// whose node stops meanwhile is refused the lease and publishes nothing.
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

	// EVERY way out before the publish removes the staging directory, and one
	// deferred guard enforces it rather than a removal per branch — which is
	// how four early returns once came to leak a directory apiece. The stage
	// is this save's own and no record can name it, so removing it needs no
	// lease and cannot destroy anything else. Before the frame writer runs,
	// its bytes are a partial PLAINTEXT copy of the state machine; the name
	// keeps it out of GC and the purge-floor scan, so nothing else would ever
	// reclaim it short of a restart's sweep.
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

	// SEAL AS WE WRITE (#116). The state machine's bytes reach the barrier
	// before they reach the disk, so no plaintext state machine is ever durable
	// under the canonical snapshot.dat.
	//
	// The previous form wrote the state machine in the clear, fsynced it, and
	// encrypted in place afterwards. Between that fsync and the rename a
	// COMPLETE PLAINTEXT STATE MACHINE was on disk under the name every
	// consumer, backup and operator reads: measured live at 206ms and 461ms
	// under load, and PERMANENT after a kill in the window, because the
	// directory is then left with snapshot.dat and no snapshot.meta and the
	// deferred discard guard never runs. This is the receive path's shape
	// (messagehandler.go), which never had the window for the same reason.
	//
	// The on-disk frame layout is UNCHANGED -- same frames, same AAD, same
	// per-frame epoch -- so no format version moves and nothing needs
	// migrating; encryptSnapshotFile, which the recovery, import and
	// re-encryption paths still use, now drives the same writer.
	//
	// A nil barrier is the configured plaintext deployment and writes straight
	// to the file, exactly as the receive path copies through for a nil
	// barrier. There is nothing to seal with and nothing to leak.
	//
	// ORDER: state machine -> snappy -> frame writer -> file. Compression comes
	// first because ciphertext does not compress, which is also the order the
	// two-step form produced.
	var w io.Writer = f
	var frameW *snapshotFrameWriter
	if barrier != nil {
		frameW = newSnapshotFrameWriter(f, barrier, req.shardID, req.replicaID, snapshotIndex)
		w = frameW
		// ONE release, registered the moment the pins can start existing.
		//
		// Every frame the writer emits pins its epoch, and an epoch with an
		// in-flight reference is one PurgeEpochsBefore will never retire — so a
		// leaked pin keeps that DEK in protected memory for the life of the
		// process. The release therefore has to cover EVERY way out of this
		// function from here on, and a deferred call is the only construct that
		// does; the alternative is remembering to release at each of the ten-odd
		// returns below, which was tried and missed five of them.
		//
		// It is deliberately registered BEFORE the first frame rather than after
		// frameW.Close(): the writer starts pinning as soon as the state machine
		// fills a chunk, so a return between here and Close would otherwise
		// leak. releaseAll is idempotent (it nils its own set), so the writer's
		// internal fail() release and this one cannot double-decrement.
		//
		// On the SUCCESS path this runs at function exit, which is after the
		// metadata file and the LogDB record are durable -- exactly the window
		// the pins exist to protect.
		defer frameW.ReleasePins()
	}
	var snappyW *snappy.Writer
	if compression == config.Snappy {
		snappyW = snappy.NewBufferedWriter(w)
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

	// Emit the trailing partial frame. Everything the state machine wrote is
	// sealed by the time this returns, and only then is anything fsynced.
	//
	// A barrier SEALED mid-artifact fails here or in an earlier frame, and it
	// fails with no plaintext artifact in existence -- which is what the old
	// post-write re-check was reaching for and could not achieve, because by
	// the time it ran the plaintext was already fsynced.
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

	// Enforce MaxSnapshotSize: reject snapshots that exceed the configured
	// limit before proceeding with metadata persistence.
	//
	// The quantity measured is the state machine's output AFTER compression
	// and BEFORE sealing -- the "post-compression size" the limit is
	// documented against (docs/multi-group-host/user.md). Before inline
	// sealing that was simply the file's size, because the file held
	// plaintext at this point. It no longer does: the file now holds framed
	// ciphertext, ~41 bytes larger per 64 KiB chunk, so stat would measure
	// the encryption overhead against a limit that was never defined to
	// include it -- and a snapshot exactly at the limit, which used to be
	// admitted, would be refused. The frame writer counts what it was handed,
	// which is precisely the old measurement. Without a barrier the file is
	// plaintext and stat is that same number.
	//
	// FileSize on the LogDB record, below, stays the stat'd on-disk size:
	// that field means "bytes at Filepath" and is used to size transfers.
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

	// Close AFTER the fsync above, and FAIL the save when it errors. This is a
	// write handle: on Linux, Close is where a deferred writeback error
	// (EIO/ENOSPC recorded after the last successful write) is reported, so
	// swallowing it would let a truncated or unwritten artifact continue on to
	// encryption, the LogDB snapshot record and log compaction — the record
	// would name a snapshot that cannot be read back, and the entries it covers
	// would already be gone. The sync-then-close-then-check idiom is the same
	// one SyncDir, encryptSnapshotFile and writeSnapshotMetadata use. Nothing
	// references the directory yet, so the deferred discard guard reclaims it.
	if closeErr := f.Close(); closeErr != nil {
		return 0, LogCompaction{}, &SnapshotError{
			ShardID:   req.shardID,
			ReplicaID: req.replicaID,
			Save:      true,
			Err:       closeErr,
		}
	}

	// The artifact is ALREADY sealed: the frame writer above encrypted every
	// byte on its way to the file, so there is no separate encrypt step and
	// never was a plaintext file to replace (#116).
	//
	// The recorded epoch is the MINIMUM epoch across all frames, not a post-hoc
	// CurrentEpoch() read. A key rotation can run concurrently on a separate
	// goroutine, so frames written before it are sealed at epoch E and frames
	// after at E+1; recording the min guarantees the global DEK-purge floor
	// never purges a DEK any retained frame still needs.
	//
	// The pins cover EVERY epoch a frame was sealed under, and are held until
	// saveSnapshot returns — past the metadata-file and LogDB snapshot-record
	// writes below — so a concurrent rotate+purge cannot remove any frame's DEK
	// in the window before the recorded epoch becomes durable and visible to
	// the snapshot-floor scan. Pinning every epoch, not just the minimum,
	// closes the intermediate-frame-epoch gap a mid-artifact rotation opens.
	//
	// There is no seal re-check here any more. The old one ran AFTER the
	// plaintext was fsynced, so it could not prevent what it was checking for;
	// a seal landing mid-artifact now fails inside the frame writer, with no
	// artifact in existence to leak.
	var epoch uint64
	if frameW != nil {
		epoch = frameW.MinEpoch()
	}

	// Write snapshot metadata file (includes epoch for encrypted snapshots).
	// The (snapshotIndex, term) pair was captured together under the apply
	// gate above; do NOT re-read lastAppliedTerm here — applies may have
	// resumed since afterCapture released the gate.
	if err := writeSnapshotMetadata(saveDir, snapshotIndex, term, req.shardID, req.replicaID, epoch, compression); err != nil {
		// M14: the data file is (possibly encrypted) ciphertext with no
		// snapshot.meta beside it. readSnapshotEpoch classifies such a directory
		// UNEVALUABLE, which makes the global/per-barrier purge floor fail safe
		// and suppresses DEK purging host-wide until three newer snapshots GC it.
		// Nothing references this directory yet (no LogDB record, no LogReader
		// entry), so the deferred guard discards it. This is the LAST point at
		// which the directory is not yet self-describing.
		return 0, LogCompaction{}, &SnapshotError{
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
		return 0, LogCompaction{}, &SnapshotError{
			ShardID:   req.shardID,
			ReplicaID: req.replicaID,
			Save:      true,
			Err:       err,
		}
	}

	// L22/M15: export the snapshot BEFORE it is recorded in LogDB and published
	// to the LogReader. Running it afterwards failed a snapshot that was already
	// durable, recorded and published — skipping compaction and epoch purge —
	// on an export error; doing it here means an export failure fails a snapshot
	// that is not yet visible, and the caller's retry re-takes it cleanly. The
	// staged snapshot.dat is fsynced and self-describing at this point, and a
	// failure removes the unpublished stage.
	//
	// The export is PLAINTEXT: exportSnapshotBackup decrypts an encrypted
	// artifact and records Epoch 0, because the export lands OUTSIDE every DEK
	// purge-floor scan (Host.ExportSnapshot's plaintext contract, and
	// Host.ImportSnapshot's staging invariant that an exported artifact never
	// carries ciphertext). Emitting epoch-bound ciphertext there would silently
	// expire the backup when its DEK is purged.
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

	// Measure the artifact AFTER encryption so FileSize always means "size of
	// the file at Filepath". A record whose FileSize is 0 while Filepath is
	// set makes this replica stream an empty snapshot once it becomes leader,
	// so an artifact that cannot be MEASURED fails the save rather than being
	// recorded at size 0 — the same fail-closed rule the recovery and import
	// paths apply. The stage is still unpublished, so the failure removes it.
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
		// The state machine serialized the same state to a different size —
		// map order, a compression block boundary. Recovery measures the file
		// against the record it keeps, so this artifact could never be read;
		// publishing it would report a repair that is not one. The stage is
		// removed, and a snapshot at the next index repairs the shard.
		slog.Error("snapshot: rebuilt artifact does not match the recorded size and cannot replace it",
			"shard_id", req.shardID,
			"replica_id", req.replicaID,
			"snapshot_index", snapshotIndex,
			"recorded_size", rebuildFileSize,
			"rebuilt_size", fileSize,
		)
		return 0, LogCompaction{}, nil
	}

	// PUBLISH, under the node's write lease. From here the save writes state
	// another writer can see — the canonical directory a receive at the same
	// index publishes into, the LogDB record, the log reader's marker and the
	// entries below it, the GC — so UnloadNode waits it out; none of it runs
	// user code, so the wait is short. A node that has stopped refuses the
	// lease: the save publishes nothing, and its stage is removed. The release
	// is deferred after the stage guard, so it runs first; the guard needs no
	// lease, since the stage is this save's own.
	if !node.AcquireWriteLease() {
		return 0, LogCompaction{}, &SnapshotError{
			ShardID:   req.shardID,
			ReplicaID: req.replicaID,
			Save:      true,
			Err:       ErrStopped,
		}
	}
	defer node.ReleaseWriteLease()
	// And under the snapshot-directory lock: the publish, the record, the log
	// reader and the GC form one step no receive can interleave with (see
	// Node.snapshotDirMu). It is released explicitly once the GC is done — the
	// log compaction and epoch purge after it touch neither the directories
	// nor the record, and a receive should not queue behind them — and by the
	// deferred call on every earlier return, before the lease.
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
	if err := SyncDirChain(saveDir, node.SnapshotDir()); err != nil {
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
			// A rebuild deliberately re-saves at the index the record already
			// holds, and every LogDB rejects a non-advancing index. The record
			// already names this directory and its artifact is back on disk, so
			// the rejection is the expected outcome of a repair, not a failure.
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

	// Notify the LogReader of the new snapshot so the raft layer can
	// send InstallSnapshot to followers that are behind the compacted
	// region. The LogReader stores the snapshot metadata (including
	// Filepath and FileSize) and serves it via Snapshot() when
	// inmem.snapshot is nil.
	//
	// A REBUILD cannot: the reader already records this index, and a
	// non-advancing CreateSnapshot is refused. It refreshes the recorded
	// snapshot's size instead — the reader was loaded with 0 when the artifact
	// was missing at start, and the send path refuses a file whose size
	// differs from what the reader declares.
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
			// The compression this save actually used. The LogReader's copy is
			// what the SEND path reads when this replica ships the snapshot to
			// a follower, and a snapshot whose record says NoCompression while
			// its bytes are snappy is fed to the receiving state machine as
			// garbage.
			//
			// OnDiskIndex is deliberately NOT set: it is the state machine's own
			// internal applied counter, NOT a raft log index (see StartShard),
			// and nothing in the local save path can observe it — the
			// Snapshotter interface does not report it. Seeding it with
			// snapshotIndex would put a raft log index in a field documented not
			// to hold one, which is worse than leaving it unstated.
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
				// Unreachable today — setSnapshot's only error is
				// ErrSnapshotOutOfDate, handled above — but the fall-through was
				// unguarded, so the day setSnapshot grows a second error the
				// compaction block below would trim the log against a snapshot
				// the LogReader never installed. The artifact is durable and
				// recorded; only the discretionary follow-up work is skipped, and
				// the next snapshot retries it.
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
		node.PinnedSnapshotDirs()...); gcErr != nil {
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
			// Advance the LogReader's compaction marker BEFORE the LogDB
			// removal. LogReader.Compact resolves the term at compactIndex,
			// which stops being readable once RemoveEntriesTo has run — the
			// reverse order fails with ErrCompacted and silently leaves
			// markerIndex stale, so raft keeps probing for entries the
			// LogDB has already discarded.
			// advanced records whether THIS save moved the marker. Only then did
			// it compact anything: ErrCompacted means an earlier snapshot, a
			// received one or an import already took the marker past
			// compactIndex, and a nil reader cannot say either way, so neither
			// is reported to OnLogCompacted.
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
					// The marker did NOT advance. Removing the entries anyway
					// produces exactly the stale-markerIndex state the ordering
					// five lines above exists to prevent: raft keeps probing for
					// entries the LogDB has discarded. Compaction is discretionary
					// — the snapshot is durable, recorded and published — so skip
					// the whole removal and let the next snapshot retry against a
					// marker it can advance. ErrCompacted is not an error here: it
					// means the marker is already at or past compactIndex.
					return snapshotIndex, compaction, nil
				}
			}
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

	// Purge old barrier epoch DEKs after compaction and GC. This ensures
	// key material for epochs no longer referenced by any retained
	// snapshot or WAL segment is zeroed from memory.
	p.purgeOldEpochs(node.SnapshotDir(), req.shardID, req.replicaID)

	return snapshotIndex, compaction, nil
}

// publishStagedSave renames a save's staged artifact onto its canonical
// snapshot-<index> directory. The caller holds the node's write lease and its
// snapshot-directory lock.
//
// An occupant of the canonical name is replaced only when it is an orphan —
// no receive claims it (Node.ClaimSnapshotDir) and no LogDB record names it —
// or when it is the recorded artifact this save is rebuilding. A claimed
// directory is a receive's published artifact still awaiting raft's verdict:
// unrecorded now and possibly recorded a moment later, so the record alone
// cannot tell it from an orphan. A recorded one is raft's. Both are refused
// with ErrSnapshotIndexNotAdvanced. A true orphan — a crash between a save's
// rename and its record, or a failed receive's leftovers — is replaced; a
// quiescent shard keeps re-saving at the same index, so refusing it would
// block that shard's snapshots for good, and GC never reclaims the newest
// directory. A record that cannot be read fails the publish closed.
func (p *snapshotPool) publishStagedSave(stageDir, canonicalDir string, index uint64, req snapshotRequest, rebuilding bool) error {
	if _, statErr := os.Stat(canonicalDir); statErr == nil {
		if req.node.snapshotDirClaimed(index) || req.node.IsSnapshotDirPinned(canonicalDir) {
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
	// Choose the artifact and PIN it, under the snapshot-directory lock, so
	// the GC a receive or save runs after recording a newer snapshot cannot
	// delete it while this recovery reads it by path (Node.PinSnapshotDir).
	// The lock is held only for the choice: verification and decryption read
	// the whole file, and receives must not queue behind them.
	node.LockSnapshotDir()
	logdbSnap, snapshotDir, artifactIndex, err := p.chooseRecoverySnapshot(req)
	if err == nil && snapshotDir != "" {
		node.PinSnapshotDir(snapshotDir)
	}
	node.UnlockSnapshotDir()
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

	// Evaluate both apply-frontier bounds BEFORE the snapshotter is touched.
	// Running RecoverFromSnapshot first and checking afterwards leaves the
	// state machine replaced by a snapshot whose markers were then refused.
	if err := recoveryBlockedByLog(req, logdbSnap.Index); err != nil {
		return err
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

	// CONSUMER 1 of 3: verify the artifact against the integrity fields in its
	// snapshot.meta BEFORE anything reads it. The check is over the file as
	// written — post-compression, post-encryption — so it runs here, on the
	// canonical path, rather than after the decrypt-to-scratch step below.
	//
	// Without a barrier this is the ONLY at-rest integrity check a snapshot
	// has: a flipped byte inside a serialized value leaves the length
	// unchanged, no structural check trips, and the corrupt value is recovered
	// into the state machine and served — a permanent single-replica
	// divergence at the same log index with every Raft invariant intact.
	//
	// Fails CLOSED. Falling back to an older snapshot directory is NOT the
	// remedy: that would roll the replica backwards silently.
	if verifyErr := verifyRecoveryArtifact(snapshotDir, dataPath, logdbSnap); verifyErr != nil {
		return &SnapshotError{
			ShardID:   req.shardID,
			ReplicaID: req.replicaID,
			Save:      false,
			Err:       verifyErr,
		}
	}

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
		plainPath, decryptErr := p.decryptSnapshotToSibling(
			dataPath, req.shardID, req.replicaID, artifactIndex, logdbSnap.Epoch, barrier)
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
			// M13: a LogDB-recorded snapshot whose artifact is gone is a
			// FAILURE, not a no-op. Returning nil here clears the recovering
			// gate without seeding lastApplied, which later fails the shard
			// with ErrApplyGap or serves stale reads. Fail with a typed error
			// so the leader's ack-expiry -> re-send -> dedup -> re-recovery
			// path engages, matching the encrypted branch (which already fails
			// on a missing artifact via SnapshotEncryptError{Op:"read"}).
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

// chooseRecoverySnapshot reads the snapshot record and resolves the directory
// a recovery reads, returning an empty directory when there is nothing to
// recover. The caller holds the snapshot-directory lock.
//
// artifactIndex tracks the index of the directory ACTUALLY being read, not
// the one LogDB recorded. The two agree on the recorded-filepath branch (the
// directory is named snapshot-<Index>), but the discovery branch picks the
// newest directory on disk, which may be a different index — and that
// directory's frames were sealed under ITS OWN index. Using the record's index
// as the frame context there would fail authentication on a valid artifact.
func (p *snapshotPool) chooseRecoverySnapshot(req snapshotRequest) (logdb.Snapshot, string, uint64, error) {
	logdbSnap, err := p.logdb.GetSnapshot(req.shardID, req.replicaID)
	switch {
	case errors.Is(err, logdb.ErrNoSnapshot):
		// "No snapshot" is the documented no-op, not a failure. Every real
		// LogDB (memdb, waldb) reports it as this sentinel rather than a zero
		// record, so lumping it in with a genuine read error below failed
		// recovery on every replica that has never snapshotted.
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
// embedded in its own ciphertext header (via barrier.DecryptWithContext), not
// with this value. It is retained only for log context. The caller uses the
// recorded epoch solely to decide whether decryption is needed at all
// (Epoch>0).
//
// shardID/replicaID/index identify the artifact and MUST be the identity the
// frames were sealed under; together with the frame ordinal they reproduce the
// positional context AAD (see NewSnapshotAAD). A mismatch fails authentication
// and there is no weaker retry: an epoch-only fallback would readmit exactly
// the frame-splicing this binding exists to prevent.
func (p *snapshotPool) decryptSnapshotToSibling(path string, shardID, replicaID, index, epoch uint64, barrier SnapshotBarrier) (string, error) {
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

	// Every frame is authenticated against its position, its artifact
	// identity and whether it is the terminal frame; see OpenSnapshotFrames.
	// Each frame carries its own epoch in its header and decrypts under that
	// epoch's DEK, so an artifact spanning a mid-save key rotation reads back
	// whole. The recorded epoch parameter is advisory only: it gates whether
	// decryption is attempted at all, and pins the DEK-purge floor at the
	// minimum frame epoch so no frame's DEK is purged while the snapshot is
	// retained.
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

	nodes := p.nodes()

	// A StopShard'd shard keeps its encrypted snapshot on disk while dropping
	// out of p.nodes(), and in single-barrier mode its DEK is still needed:
	// purging below its epoch makes its StartShard fail closed on decrypt,
	// forever. Every shard has its OWN snapshot base (…/snapshots/shard-N, or
	// its DataDir), so no scan of the loaded shards' bases can see it. The
	// LogDB can: it records every shard's latest snapshot, loaded or not.
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
		// L23: a NON-NIL LogDB that cannot report an epoch floor contributed 0
		// and let the purge proceed on the snapshot floor alone — irreversibly
		// destroying DEKs a durable, encrypted LogDB may still need. Only a
		// POSITIVE AtRestNonDurable declaration proves otherwise: no durable
		// artifact means no record can be stranded. Silence (an implementation
		// that does not declare, or declares the AtRestUnprotected zero value)
		// also covers "durable, encrypted, forgot to declare" — the exact
		// hazard — so it fails safe. The cost of silence is keyring growth,
		// which is recoverable; the cost of guessing wrong is DEK destruction,
		// which is not. perBarrierSafePurgeFloor already fails safe on the
		// analogous unsupported-LogDB case; a NIL LogDB (memory mode) is not a
		// store at all and stays a 0 contribution in both.
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

// engineLogDBAtRestProtection reports a LogDB's declared at-rest guarantee. A
// LogDB that does not implement logdb.AtRestProtectedLogDB declares nothing,
// which reads as UNPROTECTED so every caller fails closed on silence.
//
// It is the engine-local twin of the host's logDBAtRestProtection: this package
// deliberately does not import the quicraft package (that import runs the other
// way), so the two-line assertion is duplicated rather than the dependency.
func engineLogDBAtRestProtection(db logdb.LogDB) logdb.AtRestProtection {
	p, ok := db.(logdb.AtRestProtectedLogDB)
	if !ok {
		return logdb.AtRestUnprotected
	}
	return p.AtRestProtection()
}

// unloadedSnapshotFloor returns the minimum recorded snapshot epoch over every
// (shard, replica) the LogDB holds state for that is not among the loaded
// nodes and that include accepts, and whether that minimum is known.
//
// The LogDB is the authority for shards that are not loaded: it records every
// shard's latest snapshot and the epoch it was sealed at, wherever the shard's
// snapshot tree lives. That record is the snapshot a restart recovers from, so
// counting it is what keeps a purge from stranding the shard. A stopped shard's
// OLDER retained snapshots are superseded and not counted; a purge may leave
// them undecryptable (owner ruling, 2026-09-23).
//
// It fails safe: an enumeration error, a record that cannot be read, or an
// include that cannot decide returns ok == false, and the caller must not
// purge. A nil LogDB has no durable records and contributes nothing.
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
	// One snapshot of the loaded set, used for both the target's own shards
	// and the unloaded-shard accounting below, so the two cannot disagree.
	allLoaded := p.nodes()
	loadedForTarget := make(map[uint64]*Node)
	for _, node := range allLoaded {
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

	// Shards mapped to the target that are NOT loaded — the enumerator above
	// is optional, and its absence cannot mean the loaded set is complete.
	// The resolver is a function of shardID, so an unloaded shard's barrier is
	// as resolvable as a loaded one's; one that cannot be resolved can neither
	// be counted nor excluded, which declines the purge.
	unloadedMin, ok := p.unloadedSnapshotFloor(allLoaded, func(shardID uint64) (bool, error) {
		b, err := p.barrierForShard(shardID)
		return b == target, err
	})
	if !ok {
		return 0, false
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
	snapshotMinEpoch := minNonZero(scan.minEpoch, unloadedMin)

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

// Snapshot RECEIVE staging-directory naming, the InstallSnapshot counterpart of
// the import names above and used for the same reason: a receive must not write
// into the canonical "snapshot-<index>" directory, because same-index re-sends
// are a supported, routine event (msg.Hint carries a transfer sequence
// precisely to disambiguate them) and opening snapshot.dat with O_TRUNC there
// destroys whatever was already at that index before a single received byte has
// been validated. The receive stages into a directory it created, then
// publishes with one rename.
//
// It gets its OWN prefix rather than reusing ImportStageDirPrefix because the
// two have different liveness proofs. An import staging directory is protected
// by the shard's import lock, which is what lets sweepImportStage reclaim
// orphans while the shard is running; a receive holds no such lock and can be
// in flight concurrently with a snapshot recovery, so a sweeper matching this
// prefix must run only where no receive is possible — Host.StartShard, before
// the shard's node exists to route an InstallSnapshot to.
//
// Unlike an import staging directory, this one holds CIPHERTEXT whenever the
// shard has a barrier: the receive encrypts inline as it writes. An orphan is
// therefore a disk leak rather than a plaintext exposure — still worth
// reclaiming, since nothing else enumerates the name.
const (
	// RecvStageDirPrefix marks a directory an InstallSnapshot receive stages
	// its artifact in. The name deliberately cannot parse as
	// "snapshot-<index>", so listSnapshotDirs, snapshot GC and the DEK
	// purge-floor scan all skip a half-written receive.
	RecvStageDirPrefix = ".recv-"

	// RecvStageDirPattern is the os.MkdirTemp pattern for a receive staging
	// directory. Every receive must build its name from this.
	RecvStageDirPattern = RecvStageDirPrefix + "*"
)

// IsSnapshotRecvStage reports whether a directory entry name is a receive
// staging directory. It is the single matcher the sweeper uses, so a name that
// can be CREATED is by construction a name that can be RECLAIMED.
func IsSnapshotRecvStage(name string) bool {
	return strings.HasPrefix(name, RecvStageDirPrefix)
}

// SweepStaleRecvStageDirs removes orphaned InstallSnapshot receive staging
// directories from a shard/replica's snapshot tree.
//
// A receive removes its own staging directory on every error return; this
// reclaims the ones a CRASH left behind, which nothing else ever will — the
// name is invisible to listSnapshotDirs by construction, so snapshot garbage
// collection, the recovery fallback and the DEK purge-floor scan all skip it.
//
// The caller must ensure no receive is in flight for the shard. Host.StartShard
// is the only such place: a receive needs the shard's engine node to route to,
// and StartShard runs before that node exists.
//
// Errors are logged and counted through the same reporting path a failed
// scratch sweep uses; a failed cleanup must not abort startup.
func SweepStaleRecvStageDirs(baseDir string, shardID, replicaID uint64) {
	sweepStageDirs(baseDir, shardID, replicaID, IsSnapshotRecvStage, "InstallSnapshot receive")
}

// Snapshot SAVE staging-directory naming. A local save captures the state
// machine into a directory of its own and publishes it onto the canonical
// "snapshot-<index>" name with one rename under the node's write lease, the
// receive's discipline and for the same reason: the canonical directory is
// shared with a snapshot receive at the same index, and a save that wrote
// there directly had to hold the lease — which UnloadNode waits out — across
// the user state machine's capture, so a state machine that ignored the
// stopper, or waited on its own shard, could hang StopShard forever.
//
// It gets its own prefix, like the receive, because its liveness proof is
// the same one: only Host.StartShard, before the shard's node exists to take
// a save, may sweep it. A save of a PREVIOUS incarnation still capturing when
// that sweep runs loses its stage and fails — it could not have published
// anyway, since its stopped node refuses the lease.
const (
	// SaveStageDirPrefix marks a directory a local snapshot save stages its
	// artifact in. The name deliberately cannot parse as "snapshot-<index>",
	// so listSnapshotDirs, snapshot GC and the DEK purge-floor scan all skip
	// a half-written save.
	SaveStageDirPrefix = ".save-"

	// SaveStageDirPattern is the os.MkdirTemp pattern for a save staging
	// directory. Every save must build its name from this.
	SaveStageDirPattern = SaveStageDirPrefix + "*"
)

// IsSnapshotSaveStage reports whether a directory entry name is a save
// staging directory. It is the single matcher the sweeper uses, so a name that
// can be CREATED is by construction a name that can be RECLAIMED.
func IsSnapshotSaveStage(name string) bool {
	return strings.HasPrefix(name, SaveStageDirPrefix)
}

// MkdirStageDir creates a staging directory under parent from pattern, as
// os.MkdirTemp does — the last "*" in pattern is replaced by a random string,
// retried on collision — but with the snapshot tree's documented mode 0750
// under the process umask, as MkdirAll gives every other snapshot directory.
// os.MkdirTemp creates 0700, and a staged directory is published by RENAME,
// which keeps its mode: every save, receive and import would otherwise
// publish a canonical snapshot directory the operator's group cannot traverse.
// Chmod after MkdirTemp is no answer either — it ignores the umask.
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

// SweepStaleSaveStageDirs removes the save staging directories a crash left
// in a shard/replica's snapshot tree. The caller must ensure no save of the
// shard can publish — Host.StartShard, before the shard's node exists; see
// SaveStageDirPrefix.
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
		if rmErr := sweepRemove(stalePath); rmErr != nil && !os.IsNotExist(rmErr) {
			noteStaleDecryptSweepFailure(stalePath, "remove", rmErr)
		}
	}
}

// sweepRemove unlinks a swept scratch path. It is a variable solely so the
// failure branch above can be exercised: the real causes — a read-only mount,
// a permissions or ownership change, an immutable attribute — either need
// privileges to construct or, worse, do NOT apply to a process running as
// root, which is how the test suite runs in a CI container. A test that
// chmods the directory and hopes unlink fails silently proves nothing there,
// and the branch it guards is the one that makes a plaintext state machine
// left at rest visible at all.
//
// Production code must never assign to it.
var sweepRemove = os.Remove

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
	// Stamp the integrity fields over the data file AS WRITTEN. This runs
	// after the data file has been written, encrypted and fsynced, so the
	// bytes hashed here are exactly the bytes every consumer will later read.
	//
	// A hash error is NOT fatal to the save: the artifact itself is already
	// durable and correct, and refusing to record a snapshot because it could
	// not be re-read would turn a transient read fault into a lost snapshot.
	// The record is written without the fields, which makes the artifact
	// recover without an integrity check rather than fail one.
	var dataLen, dataHash uint64
	if sum, size, hashErr := hashSnapshotData(filepath.Join(dir, snapshotDataFile)); hashErr == nil {
		dataHash, dataLen = sum, size
	} else {
		slog.Warn("snapshot: could not hash the data file; recording metadata "+
			"without an integrity check",
			"dir", dir, "shard_id", shardID, "replica_id", replicaID, "error", hashErr)
	}
	metaPath := filepath.Join(dir, snapshotMetadataFile)
	f, err := os.OpenFile(metaPath, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0o600)
	if err != nil {
		return err
	}
	// closed tracks whether the sync-then-close sequence below already ran, so
	// the cleanup defer does not double-close a handle whose Close error was
	// already reported. A Close error on a WRITE handle is the last chance to
	// observe a deferred writeback failure, and this record is exactly what
	// readSnapshotEpoch consults: an unreadable one classifies the directory
	// UNEVALUABLE and suppresses DEK purging for the whole barrier until it
	// rolls off. Dropping the error here (the old Debug-only defer) is what let
	// that happen silently, so it is returned like every other write error.
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
		// L25: "no snapshot on disk" is a storage-state answer, not a shutdown
		// signal. Reporting ErrStopped here conflated the two; ErrSnapshotNotFound
		// keeps them distinct per the typed-errors convention.
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

// exportSnapshotBackup writes a PLAINTEXT copy of a just-saved snapshot to
// exportPath. exportPath lies OUTSIDE every DEK purge-floor scan, and the export
// contract requires plaintext (Host.ExportSnapshot decrypts; Host.ImportSnapshot's
// staging invariant is that an exported artifact never carries ciphertext), so an
// epoch-bound ciphertext export would silently expire the backup when its DEK is
// purged.
//
// When the on-disk artifact is unencrypted (meta.Epoch == 0) the canonical
// snapshot.dat is already plaintext and its snapshot.meta already records Epoch 0,
// so a verbatim copy is a valid, purge-immune backup. When it is encrypted
// (meta.Epoch > 0) the artifact is decrypted to a plaintext scratch sibling and
// THAT is exported, with the exported meta rewritten to Epoch 0; the canonical
// encrypted snapshot.dat is left untouched. A configured-but-unavailable barrier
// fails the export closed rather than emit ciphertext.
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
	plainPath, decErr := p.decryptSnapshotToSibling(
		dataPath, meta.ShardID, meta.ReplicaID, meta.Index, meta.Epoch, barrier)
	if decErr != nil {
		return &SnapshotExportError{
			ExportPath: filepath.Clean(exportPath),
			Err:        decErr,
		}
	}
	defer func() {
		if rmErr := os.Remove(plainPath); rmErr != nil && !os.IsNotExist(rmErr) {
			slog.Debug("snapshot export: decrypted scratch remove failed",
				"path", plainPath,
				"error", rmErr,
			)
		}
	}()
	// The exported artifact is plaintext, so its meta must say Epoch 0. Every
	// other field describes the same snapshot (Compression is preserved: decrypt
	// removes the encryption layer but not the compression layer beneath it).
	exportMeta := meta
	exportMeta.Epoch = 0
	return writeSnapshotExport(plainPath, exportPath, MarshalSnapshotMeta(exportMeta))
}

// exportSnapshotToPath copies the (already-plaintext) snapshot artifact from the
// internal snapshot directory to the specified export path, meta included
// verbatim. The data file is copied (not moved) so the internal snapshot
// directory remains intact for log compaction and follower catch-up.
//
// Callers that may hold an ENCRYPTED artifact must use exportSnapshotBackup,
// which decrypts first; this function copies bytes as-is and would otherwise
// emit ciphertext into an unscanned directory.
func exportSnapshotToPath(saveDir, exportPath string) error {
	srcDataPath := filepath.Join(saveDir, snapshotDataFile)
	var metaBytes []byte
	if b, readErr := os.ReadFile(filepath.Join(saveDir, snapshotMetadataFile)); readErr == nil {
		metaBytes = b
	}
	return writeSnapshotExport(srcDataPath, exportPath, metaBytes)
}

// writeSnapshotExport copies srcDataPath into exportPath/snapshot.dat and, when
// metaBytes is non-nil, writes it as exportPath/snapshot.meta, then fsyncs the
// export directory so both entries survive a crash. The export directory is
// created with 0750 permissions if it does not exist. A metadata write failure
// is non-fatal — the data file is the artifact — while every other step fails
// the export. It is the single copy path shared by the plaintext export and the
// decrypt-then-export backup so neither can drift from the other.
func writeSnapshotExport(srcDataPath, exportPath string, metaBytes []byte) error {
	// Clean the export path to prevent directory traversal. The public
	// SnapshotOption.Validate() already ensures it is an absolute path.
	exportPath = filepath.Clean(exportPath)
	if err := os.MkdirAll(exportPath, 0o750); err != nil {
		return &SnapshotExportError{
			ExportPath: exportPath,
			Err:        err,
		}
	}

	src, err := os.Open(srcDataPath)
	if err != nil {
		return &SnapshotExportError{
			ExportPath: exportPath,
			Err:        err,
		}
	}
	defer func() {
		if closeErr := src.Close(); closeErr != nil {
			slog.Debug("snapshot export: source file close failed",
				"path", srcDataPath,
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
	// dstClosed marks the sync-then-close sequence below as already run so the
	// cleanup defer does not double-close. dst is a WRITE handle, so its Close
	// is where a deferred writeback error surfaces; the old Debug-only defer
	// dropped it while io.Copy, Sync and SyncDir errors all failed the export,
	// which meant a truncated backup could be reported as a successful one.
	dstClosed := false
	defer func() {
		if dstClosed {
			return
		}
		if closeErr := dst.Close(); closeErr != nil {
			slog.Debug("snapshot export: destination file close failed after write error",
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

	dstClosed = true
	if err := dst.Close(); err != nil {
		return &SnapshotExportError{
			ExportPath: exportPath,
			Err:        err,
		}
	}

	// Write the metadata beside it for completeness.
	if metaBytes != nil {
		dstMetaPath := filepath.Join(exportPath, snapshotMetadataFile)
		if writeErr := os.WriteFile(dstMetaPath, metaBytes, 0o600); writeErr != nil { // #nosec G703 -- exportPath is filepath.Clean'd and validated as absolute
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

// GCSnapshotDirs reclaims a shard's superseded snapshot directories, keeping
// only the most recent maxSnapshotsRetained. It is the entry point for the two
// paths that PUBLISH a snapshot directory without going through saveSnapshot —
// the inbound-snapshot receive path (hostMessageHandler.HandleSnapshot, which
// creates Node.SnapshotSaveDir(index) per received snapshot) and
// Host.ImportSnapshot — neither of which previously reclaimed anything.
//
// It must be called AFTER the new snapshot is durably recorded, so retention is
// computed against the set that includes it; calling it before would count the
// new directory as absent and could evict a directory that is about to become
// the only recent one.
//
// This is not cosmetic. A shard configured with SnapshotEntries == 0 — a
// supported setting with no default, so the out-of-the-box state — never runs
// saveSnapshot at all, so without this a follower that repeatedly falls behind
// the leader's compaction floor accumulates one full state-machine image per
// catch-up, forever. Worse, snapshotFloorScan.addNode folds EVERY retained
// directory of EVERY loaded node into the minimum and globalSafePurgeFloor
// takes the minimum across all of them, so one such shard's stale directories
// pin the HOST-WIDE DEK purge floor: co-hosted shards that do save can never
// purge a DEK and old epoch key material is never zeroed, defeating
// purgeOldEpochs entirely.
// recorded is the path LogDB records for this node, or "" when the caller does
// not know it. It is never removed: GC selects by index order alone, so an
// unusable higher-indexed directory (a partial receive left by a crash) can
// push the recorded one into the removal window, and deleting it leaves the
// LogDB record pointing at nothing. spare lists further cleaned directories
// that must not be removed — those a recovery is reading
// (Node.PinnedSnapshotDirs).
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
	// Remove oldest snapshots (dirs are sorted ascending by index).
	//
	// The directory LogDB RECORDS is skipped, whatever its rank. GC selects by
	// index order alone, so higher-indexed directories that are unusable —
	// e.g. partial receives left by a crash — push the recorded one into the
	// removal window, and deleting it leaves the LogDB record pointing at
	// nothing. The save's publish (publishStagedSave) has the same guard; GC,
	// which removes far more directories, did not.
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
		// Likewise a directory a recovery is reading (Node.PinSnapshotDir):
		// it chose the recorded snapshot of its moment, and a newer record
		// landing since does not make that artifact safe to delete under it.
		if slices.Contains(spare, filepath.Clean(d.path)) {
			continue
		}
		if err := os.RemoveAll(d.path); err != nil {
			return err
		}
	}
	// Make the removals durable. Without this a metadata-lazy filesystem can
	// resurrect a GC'd directory after a crash, and the resurrected one is a
	// half-deleted tree no consumer validates. waldb fsyncs after segment
	// deletion for exactly this reason.
	return SyncDir(snapshotBaseDir(baseDir, shardID, replicaID))
}

// recordedSnapshotDir returns the cleaned path LogDB records for the node: ""
// with a nil error when there is no record (or no LogDB), and an error when
// the record cannot be read. An unreadable record is NOT "no record": the GC
// guard it feeds exists to spare the recorded directory, and treating a read
// error as "none" let one transient failure delete it.
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
