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
	"fmt"
	"io"
	"log/slog"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jeremyhahn/go-quicraft/pkg/config"
	"github.com/jeremyhahn/go-quicraft/pkg/internal/queue"
	"github.com/jeremyhahn/go-quicraft/pkg/internal/raft"
	"github.com/jeremyhahn/go-quicraft/pkg/internal/supervise"
	"github.com/jeremyhahn/go-quicraft/pkg/logdb"
	"github.com/jeremyhahn/go-quicraft/pkg/proto"
	"github.com/jeremyhahn/go-quicraft/pkg/sm"
)

// Applier is the interface for applying committed entries to a state
// machine. It decouples the engine from the concrete rsm.StateMachine
// type for testability.
type Applier interface {
	// Apply processes committed entries through the state machine.
	// The results slice must be pre-allocated with len == len(entries).
	Apply(entries []proto.Entry, results []sm.Result) error
}

// Snapshotter is the interface for snapshot save/recover operations.
// It decouples the engine from the concrete rsm.StateMachine type.
// The rsm.StateMachine satisfies both Applier and Snapshotter.
type Snapshotter interface {
	SaveSnapshot(ctx context.Context, w io.Writer, stopper <-chan struct{}) error
	RecoverFromSnapshot(ctx context.Context, r io.Reader, stopper <-chan struct{}) error
	LastApplied() uint64
}

// appliedSetter lets the snapshot pool seed the snapshotter's applied counter after recovery.
type appliedSetter interface {
	SetLastApplied(idx uint64)
}

// gatedSnapshotter lets the snapshot pool release the apply gate once the point-in-time capture is done, before streaming.
type gatedSnapshotter interface {
	SaveSnapshotGated(ctx context.Context, w io.Writer, stopper <-chan struct{}, afterCapture func()) error
}

// defaultProposalQueueSize is the proposal queue capacity before backpressure.
const defaultProposalQueueSize = 256

// defaultInboxQueueSize is the transport inbox queue capacity, sized to absorb peer bursts without dropping messages.
const defaultInboxQueueSize = 4096

// defaultReadIndexQueueSize is the ReadIndex key queue capacity; keys are coalesced into one ReadIndex per step.
const defaultReadIndexQueueSize = 4096

// Node is the per-shard controller the engine workers share; the pipeline gives one tier ownership at a time, and hot fields are cache-line padded.
type Node struct {
	// shardID is the globally unique identifier for the Raft shard.
	shardID uint64

	// replicaID is the unique identifier for this replica within the shard.
	replicaID uint64

	// raftMu serializes Peer Tick and Commit between the step and commit workers.
	raftMu sync.Mutex

	// diagEnabled gates publishing diagSnap, the raft-state snapshot diagnostic accessors read.
	diagEnabled atomic.Bool
	diagSnap    atomic.Pointer[diagSnapshot]

	// peer is the Raft protocol peer. Owned by exactly one worker at a
	// time, enforced by the commitPending flag and channel pipeline.
	// Tick() and Commit() are additionally serialized by raftMu.
	peer *raft.Peer

	// sm is the managed state machine (applier interface).
	sm Applier

	// logReader is the cached log reader for this shard.
	logReader *logdb.LogReader

	// cfg is the per-shard configuration.
	cfg config.Config

	// notifyCommit enables OnCommitted after WAL persist and before apply (HostConfig.NotifyCommit).
	notifyCommit bool

	// proposalQ is the MPSC proposal queue: Host writers, the step worker drains.
	proposalQ *queue.EntryQueue[proto.Entry]

	// inboxQ is the MPSC queue for non-proposal transport messages; a full queue drops, which Raft retransmission covers.
	inboxQ *queue.EntryQueue[proto.Message]

	// readIndexQ is the MPSC queue of ReadIndex keys, coalesced into one raft ReadIndex per step; pendingReads is registered first.
	readIndexQ *queue.EntryQueue[uint64]

	// commitPending is set by the step worker before sending the Update and cleared by the apply worker; while set only inbound protocol messages are stepped. Padded against false sharing.
	commitPending struct {
		atomic.Bool
		_ [127]byte // pad to 128 bytes total (atomic.Bool = 1 byte)
	}

	// sealHeld marks a shard the commit worker parked because the barrier is sealed; the shard is frozen until the write is retried after unseal.
	sealHeld atomic.Bool

	// lastApplied is the highest applied index: written by the apply worker, read by the step worker. Padded against false sharing.
	lastApplied struct {
		atomic.Uint64
		_ [120]byte // pad to 128 bytes total (atomic.Uint64 = 8 bytes)
	}

	// lastAppliedTerm is the term at lastApplied, written with it so the snapshot pool reads a consistent pair.
	lastAppliedTerm atomic.Uint64

	// committedIndex is the highest committed index, published by the step worker for FollowerRead.
	committedIndex atomic.Uint64

	// processedIndex mirrors the log's processed marker so the snapshot pool can see an installed snapshot without reading the Peer.
	processedIndex atomic.Uint64

	// failed is the shard-failure latch: set on the first apply error without advancing lastApplied; the step worker then skips the shard.
	failed atomic.Bool

	// listener is the event listener for error and lifecycle callbacks.
	// May be nil if no listener is configured.
	listener *config.EventListener

	// listeners runs notifications off the producing goroutine so a listener may stop the shard; nil outside an engine, which dispatches inline.
	listeners *listenerDispatcher

	// snapshotter provides snapshot save/recover capability for the SM.
	// May be nil for nodes that don't support snapshots (e.g. witnesses).
	snapshotter Snapshotter

	// snapshotDir is the base directory for this shard's snapshots.
	snapshotDir string

	// lastSnapshotIndex tracks the log index of the last completed snapshot.
	// Used by the apply worker to determine when auto-snapshot should trigger.
	lastSnapshotIndex atomic.Uint64

	// snapshotting prevents concurrent snapshot operations on the same node.
	// Set to true when a snapshot starts, cleared when it completes.
	snapshotting atomic.Bool

	// recovering gates state machine access during RecoverFromSnapshot (no apply, no extraction); Raft protocol traffic continues, InstallSnapshot is deferred.
	recovering atomic.Bool

	// redrive tracks recoveries rejected before restore began; see recoveryRedrive.
	redrive recoveryRedrive

	// saving blocks applies while SaveSnapshot captures its point in time, so the capture never races SM.Apply; exclusive with recovering.
	saving atomic.Bool

	// applyInFlight counts running apply closures; incremented before the gate check so snapshot save and recovery can drain it.
	applyInFlight atomic.Int64

	// stopC is closed when the node is unloaded from the engine.
	stopC chan struct{}
	// stopOnce makes Stop idempotent.
	stopOnce sync.Once

	// installMu guards installVerdicts: the verdict each locally injected
	// InstallSnapshot is waiting on, keyed by snapshot index. See
	// AwaitInstallVerdict.
	installMu       sync.Mutex
	installVerdicts map[uint64]chan InstallVerdict

	// leaseMu admits a snapshot receive's durable writes and lets UnloadNode wait them out.
	leaseMu sync.RWMutex

	// snapshotDirMu serializes save and receive changes to the snapshot directories and record; taken after the write lease.
	snapshotDirMu sync.Mutex

	// claimedSnapshotDirs are indexes a receive published and has not settled; a save must not replace them. Guarded by snapshotDirMu.
	claimedSnapshotDirs map[uint64]struct{}

	// pinnedSnapshotDirs counts, per cleaned directory, the snapshot
	// recoveries reading it; the snapshot GC spares every one. Guarded by
	// snapshotDirMu.
	pinnedSnapshotDirs map[string]int

	// unloaded marks an incarnation torn down by UnloadNode; Engine.Stop alone never sets it.
	unloaded atomic.Bool

	// paused is set while the replica is paused for a runtime seal; the node does nothing until ResumeNode. Never persisted.
	paused atomic.Bool
	// pauseMu serializes pause and resume, and guards resumeC: the channel a
	// parked apply waits on, created by the pause and closed by the resume.
	pauseMu sync.Mutex
	resumeC chan struct{}
	// applyParked is true while the apply goroutine holds a committed batch
	// parked on the pause gate. It lets PauseNode tell a pipeline item that
	// has reached the gate (settled) from one still being persisted.
	applyParked atomic.Bool
	// snapshotTransfers counts the host's snapshot sends and receives of this
	// shard in progress; PauseNode waits for it to reach zero, and a transfer
	// that would begin while paused is refused (BeginSnapshotTransfer).
	snapshotTransfers atomic.Int64
	// pausedSaveDropped records an automatic snapshot save the pool refused
	// because the shard was paused, so ResumeNode re-requests it.
	pausedSaveDropped atomic.Bool
}

// NewNode creates a new engine Node for the given shard. The node must
// be loaded into the engine via Engine.LoadNode before it participates
// in the worker pipeline.
func NewNode(
	peer *raft.Peer,
	sm Applier,
	logReader *logdb.LogReader,
	cfg config.Config,
	snapshotter Snapshotter,
	snapshotDir string,
	listener *config.EventListener,
) *Node {
	n := &Node{
		shardID:     cfg.ShardID,
		replicaID:   cfg.ReplicaID,
		peer:        peer,
		sm:          sm,
		logReader:   logReader,
		cfg:         cfg,
		proposalQ:   queue.NewEntryQueue[proto.Entry](64, defaultProposalQueueSize),
		inboxQ:      queue.NewEntryQueue[proto.Message](64, defaultInboxQueueSize),
		readIndexQ:  queue.NewEntryQueue[uint64](16, defaultReadIndexQueueSize),
		snapshotter: snapshotter,
		snapshotDir: snapshotDir,
		listener:    listener,
		stopC:       make(chan struct{}),
	}
	// Seed the log markers from the recovered log so CommittedIndex is right before the first tick.
	n.publishLogMarkers()
	return n
}

// SetNotifyCommit sets the WAL-durable early notification flag. Called
// by the Host when loading a node into the engine to propagate the
// HostConfig.NotifyCommit setting.
func (n *Node) SetNotifyCommit(enabled bool) {
	n.notifyCommit = enabled
}

// DeliverProposal adds entries to the proposal queue and reports false when it is full; safe for concurrent use.
func (n *Node) DeliverProposal(entries []proto.Entry) bool {
	return n.proposalQ.Add(entries)
}

// DeliverTrackedProposal is DeliverProposal that records the queue position in *at for ProposalTaken.
func (n *Node) DeliverTrackedProposal(entries []proto.Entry, at *uint64) bool {
	return n.proposalQ.AddTracked(entries, at)
}

// ProposalTaken reports whether the proposal recorded in *at was handed to raft, and whether that answer is final.
func (n *Node) ProposalTaken(at *uint64) (taken, final bool) {
	return n.proposalQ.Taken(at)
}

// DrainProposalBatch is DrainProposals that also returns the id of the batch
// it took, for HandOffProposals; 0 when it took nothing.
func (n *Node) DrainProposalBatch(buf []proto.Entry) ([]proto.Entry, uint64) {
	return n.proposalQ.SwapBatch(buf)
}

// HandOffProposals declares batch id handed to raft; on false the caller must not call Peer.Handle.
func (n *Node) HandOffProposals(id uint64) bool {
	return n.proposalQ.HandOff(id)
}

// DrainProposals swaps out the queued entries, reusing dst; step worker only.
func (n *Node) DrainProposals(buf []proto.Entry) []proto.Entry {
	return n.proposalQ.Swap(buf)
}

// Deliver queues a wire-received message stamped OriginRemote and reports false when the inbox is full; safe for concurrent use.
func (n *Node) Deliver(msg proto.Message) bool {
	msg.Origin = proto.OriginRemote
	return n.inboxQ.AddOne(msg)
}

// DeliverLocal queues a process-injected message stamped OriginLocal, which raft exempts from the term gate; safe for concurrent use.
func (n *Node) DeliverLocal(msg proto.Message) bool {
	msg.Origin = proto.OriginLocal
	return n.inboxQ.AddOne(msg)
}

// redeliver re-queues a drained message with its recorded origin.
func (n *Node) redeliver(msg proto.Message) bool {
	return n.inboxQ.AddOne(msg)
}

// DeliverReadIndex queues a ReadIndex key and reports false when full; safe for concurrent use.
func (n *Node) DeliverReadIndex(readKey uint64) bool {
	return n.readIndexQ.AddOne(readKey)
}

// DrainReadIndex swaps out the queued keys, reusing dst; step worker only.
func (n *Node) DrainReadIndex(buf []uint64) []uint64 {
	return n.readIndexQ.Swap(buf)
}

// DrainInbox swaps out the queued messages, reusing dst; step worker only.
func (n *Node) DrainInbox(buf []proto.Message) []proto.Message {
	return n.inboxQ.Swap(buf)
}

// ShardID returns the shard identifier.
func (n *Node) ShardID() uint64 {
	return n.shardID
}

// ReplicaID returns the replica identifier.
func (n *Node) ReplicaID() uint64 {
	return n.replicaID
}

// Peer returns the Raft peer. Callers must respect the single-owner
// invariant enforced by the engine pipeline.
func (n *Node) Peer() *raft.Peer {
	return n.peer
}

// StateMachine returns the managed state machine applier.
func (n *Node) StateMachine() Applier {
	return n.sm
}

// LogReader returns the log reader for this shard.
func (n *Node) LogReader() *logdb.LogReader {
	return n.logReader
}

// Config returns the per-shard configuration.
func (n *Node) Config() config.Config {
	return n.cfg
}

// Snapshotter returns the snapshot saver/recoverer for this node.
func (n *Node) Snapshotter() Snapshotter {
	return n.snapshotter
}

// SnapshotDir returns the base snapshot directory for this shard.
func (n *Node) SnapshotDir() string {
	return n.snapshotDir
}

// ReplicaSnapshotDir returns <SnapshotDir()>/shard-<N>/replica-<N>, the parent of this replica's snapshot directories.
func (n *Node) ReplicaSnapshotDir() string {
	return ReplicaSnapshotDir(n.snapshotDir, n.shardID, n.replicaID)
}

// SnapshotSaveDir returns <SnapshotDir()>/shard-<N>/replica-<N>/snapshot-<index>, the path save, GC and DEK-purge scans all use.
func (n *Node) SnapshotSaveDir(index uint64) string {
	return snapshotSaveDir(n.snapshotDir, n.shardID, n.replicaID, index)
}

// LastSnapshotIndex returns the log index of the last completed snapshot.
func (n *Node) LastSnapshotIndex() uint64 {
	return n.lastSnapshotIndex.Load()
}

// LastApplied returns the highest log index applied to the state machine,
// as tracked by the apply worker (and seeded on restart/recovery).
func (n *Node) LastApplied() uint64 {
	return n.lastApplied.Load()
}

// LastAppliedTerm returns the Raft term of the entry at LastApplied. It is the
// term the next snapshot records, which is why the pair must be recovered
// together: see SeedLastApplied.
func (n *Node) LastAppliedTerm() uint64 {
	return n.lastAppliedTerm.Load()
}

// SeedLastApplied sets the applied markers to a recovered (index, term) before LoadNode; it must match the peer's processed marker.
func (n *Node) SeedLastApplied(idx, term uint64) {
	n.lastApplied.Store(idx)
	n.lastAppliedTerm.Store(term)
	n.lastSnapshotIndex.Store(idx)
}

// IsFailed returns true if the shard-failure latch has tripped.
func (n *Node) IsFailed() bool {
	return n.failed.Load()
}

// failShard trips the failure latch, runs failBatch, then, if this call won the latch, reports err to both OnShardFailed channels; it reports whether it won.
func failShard(node *Node, cb Callback, err error, failBatch func()) bool {
	if !node.markFailed() {
		if failBatch != nil {
			failBatch()
		}
		return false
	}
	// The winner owes both reports; deferred so they run even if failBatch panics.
	batchDone := false
	defer func() {
		if p := reportShardFailure(node, cb, err); p != nil {
			if !batchDone {
				// The batch's panic is already unwinding and is the one the
				// worker's recovery must see; this one is only logged.
				shardID, replicaID := node.shardID, node.replicaID
				supervise.LogContained(func() {
					slog.Error("shard failure report panicked while a failed batch was panicking",
						"shard", shardID,
						"replica", replicaID,
						"panic", p,
					)
				})
				return
			}
			panic(p)
		}
	}()
	if failBatch != nil {
		failBatch()
	}
	batchDone = true
	return true
}

// reportShardFailure makes both failure reports, each isolated from the other, and returns the first panic.
func reportShardFailure(node *Node, cb Callback, err error) (panicked any) {
	report := func(fn func()) {
		defer func() {
			if r := recover(); r != nil && panicked == nil {
				panicked = r
			}
		}()
		fn()
	}
	// Deferred so the engine callback still runs if the listener calls runtime.Goexit.
	if cb != nil {
		defer report(func() { cb.OnShardFailed(node.shardID, err) })
	}
	report(func() { node.handleError(err) })
	return panicked
}

// markFailed sets the failure latch by compare-and-swap, reports whether this call won, and abandons waiting installs.
func (n *Node) markFailed() bool {
	if !n.failed.CompareAndSwap(false, true) {
		return false
	}
	// Close the proposal queue so a queued proposal's fate is final before the failure is reported.
	n.proposalQ.Close()
	n.abandonInstallVerdicts()
	return true
}

// AcquireWriteLease admits one durable write for this incarnation and reports false once stopped; pair with one ReleaseWriteLease and never nest.
func (n *Node) AcquireWriteLease() bool {
	n.leaseMu.RLock()
	if n.IsStopped() {
		n.leaseMu.RUnlock()
		return false
	}
	return true
}

// TryAcquireWriteLease is a non-blocking AcquireWriteLease that also fails while an unload is waiting.
func (n *Node) TryAcquireWriteLease() bool {
	if !n.leaseMu.TryRLock() {
		return false
	}
	if n.IsStopped() {
		n.leaseMu.RUnlock()
		return false
	}
	return true
}

// ReleaseWriteLease ends a lease granted by AcquireWriteLease.
func (n *Node) ReleaseWriteLease() {
	n.leaseMu.RUnlock()
}

// LockSnapshotDir takes the shard's snapshot-directory lock (see the
// snapshotDirMu field). The caller must already hold a write lease, and must
// release the lock with UnlockSnapshotDir before releasing the lease.
func (n *Node) LockSnapshotDir() {
	n.snapshotDirMu.Lock()
}

// UnlockSnapshotDir releases the lock taken by LockSnapshotDir.
func (n *Node) UnlockSnapshotDir() {
	n.snapshotDirMu.Unlock()
}

// ClaimSnapshotDir records that a snapshot receive has published the
// canonical directory for index and has not yet settled it. The caller holds
// the snapshot-directory lock.
func (n *Node) ClaimSnapshotDir(index uint64) {
	if n.claimedSnapshotDirs == nil {
		n.claimedSnapshotDirs = make(map[uint64]struct{})
	}
	n.claimedSnapshotDirs[index] = struct{}{}
}

// ReleaseSnapshotDir ends a claim taken by ClaimSnapshotDir. The caller holds
// the snapshot-directory lock.
func (n *Node) ReleaseSnapshotDir(index uint64) {
	delete(n.claimedSnapshotDirs, index)
}

// IsSnapshotDirClaimed reports whether a receive holds a claim on index. The
// caller holds the snapshot-directory lock.
func (n *Node) IsSnapshotDirClaimed(index uint64) bool {
	_, ok := n.claimedSnapshotDirs[index]
	return ok
}

// PinSnapshotDir keeps dir from snapshot GC while a recovery reads it; pins nest. Caller holds the snapshot-directory lock.
func (n *Node) PinSnapshotDir(dir string) {
	if n.pinnedSnapshotDirs == nil {
		n.pinnedSnapshotDirs = make(map[string]int)
	}
	n.pinnedSnapshotDirs[filepath.Clean(dir)]++
}

// UnpinSnapshotDir ends one pin taken by PinSnapshotDir. The caller holds the
// snapshot-directory lock.
func (n *Node) UnpinSnapshotDir(dir string) {
	dir = filepath.Clean(dir)
	if n.pinnedSnapshotDirs[dir] <= 1 {
		delete(n.pinnedSnapshotDirs, dir)
		return
	}
	n.pinnedSnapshotDirs[dir]--
}

// GCSparedSnapshotDirs returns the pinned and claimed directories GC must spare. Caller holds the snapshot-directory lock.
func (n *Node) GCSparedSnapshotDirs() []string {
	dirs := make([]string, 0, len(n.pinnedSnapshotDirs)+len(n.claimedSnapshotDirs))
	for dir := range n.pinnedSnapshotDirs {
		dirs = append(dirs, dir)
	}
	for index := range n.claimedSnapshotDirs {
		dirs = append(dirs, filepath.Clean(n.SnapshotSaveDir(index)))
	}
	return dirs
}

// IsSnapshotDirPinned reports whether a recovery is reading dir. Caller holds the snapshot-directory lock.
func (n *Node) IsSnapshotDirPinned(dir string) bool {
	return n.pinnedSnapshotDirs[filepath.Clean(dir)] > 0
}

// awaitWriteLeases returns once no write lease is held. Called after Stop, it
// is a fence: every lease granted before stopC closed has been released, and
// none can be granted after.
func (n *Node) awaitWriteLeases() {
	n.leaseMu.Lock()
	//nolint:staticcheck // SA2001: an empty critical section is the point; see above.
	n.leaseMu.Unlock()
}

// isUnloaded reports whether UnloadNode has begun tearing this incarnation
// down. See the unloaded field.
func (n *Node) isUnloaded() bool {
	return n.unloaded.Load()
}

// IsStopped reports whether Stop has been called: the node is being unloaded
// and must not adopt anything further.
func (n *Node) IsStopped() bool {
	select {
	case <-n.stopC:
		return true
	default:
		return false
	}
}

// CommittedIndex returns the last known committed log index.
// Updated atomically by the step worker; safe to read from any goroutine.
func (n *Node) CommittedIndex() uint64 {
	return n.committedIndex.Load()
}

// publishLogMarkers copies the committed and processed markers into atomics; step worker only. Both are monotonic, so readers only lag.
func (n *Node) publishLogMarkers() {
	if n.peer == nil {
		return
	}
	committed, processed := n.peer.LogMarkers()
	n.committedIndex.Store(committed)
	n.processedIndex.Store(processed)
	// Same owner, same moment: the pending configuration the receive gate
	// consults is derived from the log this goroutine just moved.
	n.peer.PublishPendingConfig()
}

// PendingMemberIdentity returns the identity an uncommitted config entry admits replicaID with; safe from any goroutine.
func (n *Node) PendingMemberIdentity(replicaID uint64) (string, bool) {
	if n.peer == nil {
		return "", false
	}
	return n.peer.PendingMemberIdentity(replicaID)
}

// LogMarkers returns the last committed and processed markers published by
// the step worker. Safe to call from any goroutine.
func (n *Node) LogMarkers() (committed, processed uint64) {
	return n.committedIndex.Load(), n.processedIndex.Load()
}

// SetCommittedIndex updates the committed log index. Must only be called
// from the step goroutine after processing Raft updates via GetUpdate.
func (n *Node) SetCommittedIndex(idx uint64) {
	n.committedIndex.Store(idx)
}

// handleError reports an error for this shard to the event listener through the dispatcher; it does not stop the shard.
func (n *Node) handleError(err error) {
	if n.listener == nil || n.listener.OnShardFailed == nil {
		return
	}
	cb := n.listener.OnShardFailed
	info := config.ShardFailedInfo{
		ShardID:   n.shardID,
		ReplicaID: n.replicaID,
		Err:       err,
	}
	// Dropped rather than retried; the log below and ShardInfo still show the failure.
	if !n.listeners.dispatch(func() { cb(info) }) {
		slog.Error("shard failure notification dropped: listener queue full",
			"shard", n.shardID,
			"replica", n.replicaID,
			"error", err,
			"dropped_total", n.listeners.droppedCount(),
		)
	}
}

// TryStartSnapshot atomically sets the snapshotting flag from false to true.
// Returns true if the CAS succeeded (snapshot started), false if a snapshot
// is already in progress.
func (n *Node) TryStartSnapshot() bool {
	return n.snapshotting.CompareAndSwap(false, true)
}

// ClearSnapshotting resets the snapshotting flag. Called when a snapshot
// request is rejected and the host needs to undo the CAS.
func (n *Node) ClearSnapshotting() {
	n.snapshotting.Store(false)
}

// IsRecovering reports whether a snapshot recovery is in progress; see the recovering field.
func (n *Node) IsRecovering() bool {
	return n.recovering.Load()
}

// SetRecovering sets the recovering flag. Called by the snapshot pool
// before and after RecoverFromSnapshot.
func (n *Node) SetRecovering(v bool) {
	n.recovering.Store(v)
}

// snapshotAdoptionPending reports whether the log's processed marker is above lastApplied, between a snapshot install and its recovery; extraction waits then. Needs exclusive Peer access.
func (n *Node) snapshotAdoptionPending() bool {
	if n.peer == nil {
		return false
	}
	_, processed := n.peer.LogMarkers()
	return processed > n.lastApplied.Load()
}

// IsSaving reports whether a SaveSnapshot capture is in progress.
func (n *Node) IsSaving() bool {
	return n.saving.Load()
}

// SetSaving sets the saving flag. Called by the snapshot pool around the
// consistent-capture phase of SaveSnapshot.
func (n *Node) SetSaving(v bool) {
	n.saving.Store(v)
}

// WaitForApplyDrain blocks until no apply is in flight, after SetRecovering(true) or SetSaving(true); false if the node stopped.
func (n *Node) WaitForApplyDrain(stopC <-chan struct{}) bool {
	for n.applyInFlight.Load() > 0 {
		select {
		case <-stopC:
			return false
		default:
		}
		// Apply closures are short and bounded (a single SM.Apply batch). A
		// brief yield avoids a hot spin while the in-flight apply completes.
		time.Sleep(50 * time.Microsecond)
	}
	select {
	case <-stopC:
		return false
	default:
		return true
	}
}

// StopC returns a read-only channel that is closed when the node stops.
// Used to pass cancellation to state machine operations like Open.
func (n *Node) StopC() <-chan struct{} {
	return n.stopC
}

// IsMember reports whether replicaID is in the current configuration (true when the peer is nil); a view, not an authorization.
func (n *Node) IsMember(replicaID uint64) bool {
	if n.peer == nil {
		return true
	}
	return n.peer.IsMember(replicaID)
}

// MemberBinding returns the identity, address and exclusivity the committed configuration authorizes replicaID with; fails closed when the peer is nil.
func (n *Node) MemberBinding(replicaID uint64) (raft.MemberBinding, bool) {
	if n.peer == nil {
		return raft.MemberBinding{}, false
	}
	return n.peer.MemberBinding(replicaID)
}

// LeaderAndTerm returns the leader ID and term as a consistent pair. Use this,
// not LeaderID() and Term() separately, whenever both are reported together.
func (n *Node) LeaderAndTerm() (uint64, uint64) {
	if n.peer == nil {
		return 0, 0
	}
	return n.peer.LeaderAndTerm()
}

// IsJoining reports whether this node started with join=true and has not caught up; false when the peer is nil.
func (n *Node) IsJoining() bool {
	if n.peer == nil {
		return false
	}
	return n.peer.IsJoining()
}

// IsRemoved reports whether replicaID has a committed Removed tombstone (the transport's deny-list); false when the peer is nil.
func (n *Node) IsRemoved(replicaID uint64) bool {
	if n.peer == nil {
		return false
	}
	return n.peer.IsRemoved(replicaID)
}

// IsRemovedIdentity reports whether an authenticated node identity was removed from this shard; false when the peer is nil.
func (n *Node) IsRemovedIdentity(identity string) bool {
	if n.peer == nil {
		return false
	}
	return n.peer.IsRemovedIdentity(identity)
}

// PendingRemoval reports whether a RemoveNode entry above the applied
// configuration removes replicaID or the holder of identity; see
// raft.Peer.PendingRemoval. False when the peer is nil.
func (n *Node) PendingRemoval(replicaID uint64, identity string) bool {
	if n.peer == nil {
		return false
	}
	return n.peer.PendingRemoval(replicaID, identity)
}

// IdentityHolder reports the replica the committed configuration binds the
// given node identity to; see raft.Peer.IdentityHolder. False when the peer is
// nil.
func (n *Node) IdentityHolder(identity string) (uint64, bool) {
	if n.peer == nil {
		return 0, false
	}
	return n.peer.IdentityHolder(identity)
}

// DiagPipeline is a snapshot of a node's pipeline gates: commitPending, recovering, saving and the apply drain counter.
type DiagPipeline struct {
	CommitPending bool
	ProposalQLen  int
	InboxLen      int
	ReadIndexQLen int
	Recovering    bool
	Saving        bool
	ApplyInFlight int64
}

// DiagState returns the node's diagnostic pipeline state. Used for debugging
// pipeline stalls.
func (n *Node) DiagState() DiagPipeline {
	return DiagPipeline{
		CommitPending: n.commitPending.Load(),
		ProposalQLen:  n.proposalQ.Len(),
		InboxLen:      n.inboxQ.Len(),
		ReadIndexQLen: n.readIndexQ.Len(),
		Recovering:    n.recovering.Load(),
		Saving:        n.saving.Load(),
		ApplyInFlight: n.applyInFlight.Load(),
	}
}

// diagSnapshot is a point-in-time copy of the Peer's diagnostic state,
// published by the step worker under raftMu and read lock-free by the
// external diagnostic accessors.
type diagSnapshot struct {
	term      uint64
	committed uint64
	processed uint64
	lastIdx   uint64
	msgs      int
	state     string
	// progress is the leader's view of each follower, or nil when this
	// replica is not the leader. Published structured rather than rendered;
	// DiagRemotes formats it on demand.
	progress []raft.RemoteProgress
}

// publishDiagLocked copies the Peer's diagnostic state into diagSnap; caller holds n.raftMu. No-op until diagnostics are enabled.
func (n *Node) publishDiagLocked() {
	if n.peer == nil || !n.diagEnabled.Load() {
		return
	}
	term, committed, processed, lastIdx, msgs, state := n.peer.DiagRaftState()
	// Publish structured progress; format text only on request.
	progress := n.peer.RemoteProgress()
	n.diagSnap.Store(&diagSnapshot{
		term:      term,
		committed: committed,
		processed: processed,
		lastIdx:   lastIdx,
		msgs:      msgs,
		state:     state,
		progress:  progress,
	})
}

// DiagRaftState returns the last published raft state; the first call enables publication.
func (n *Node) DiagRaftState() (term, committed, processed, lastIdx uint64, msgs int, state string) {
	if n.peer == nil {
		return 0, 0, 0, 0, 0, "nil"
	}
	n.diagEnabled.Store(true)
	if snap := n.diagSnap.Load(); snap != nil {
		return snap.term, snap.committed, snap.processed, snap.lastIdx, snap.msgs, snap.state
	}
	return 0, 0, 0, 0, 0, "(diagnostics warming up)"
}

// DiagRemotes returns diagnostic remote state for debugging commit stalls.
// Like DiagRaftState, it reads the step worker's published snapshot.
func (n *Node) DiagRemotes() string {
	if n.peer == nil {
		return "nil"
	}
	n.diagEnabled.Store(true)
	snap := n.diagSnap.Load()
	if snap == nil {
		return "(diagnostics warming up)"
	}
	if snap.progress == nil {
		return "not leader"
	}
	var sb strings.Builder
	for i := range snap.progress {
		if i > 0 {
			sb.WriteByte(' ')
		}
		rp := &snap.progress[i]
		fmt.Fprintf(&sb, "r%d[role=%s match=%d next=%d state=%s paused=%v active=%v inflights=%d]",
			rp.ReplicaID, rp.Role, rp.Match, rp.Next, rp.State, rp.Paused, rp.Active, rp.Inflights)
	}
	return sb.String()
}

// ReplicaProgress returns the leader's view of each follower's replication position, or nil when not leader; the first call enables publication.
func (n *Node) ReplicaProgress() []raft.RemoteProgress {
	if n.peer == nil {
		return nil
	}
	n.diagEnabled.Store(true)
	if snap := n.diagSnap.Load(); snap != nil {
		return snap.progress
	}
	return nil
}

// Stop signals the node to stop. Called when unloading from the engine.
func (n *Node) Stop() {
	n.stopOnce.Do(func() {
		close(n.stopC)
	})
	// A stopped node hands nothing further to raft (see markFailed).
	n.proposalQ.Close()
	n.abandonInstallVerdicts()
}

// InstallOutcome is what raft did with a locally injected InstallSnapshot.
type InstallOutcome uint8

const (
	// InstallAdopted means raft took the snapshot as its log's new base: the
	// committed index moved to the snapshot index, so the state machine must
	// be recovered from it and the snapshot recorded.
	InstallAdopted InstallOutcome = iota + 1
	// InstallDeclined means raft stepped the install without error but
	// adopted nothing, because it was already at or past the snapshot.
	InstallDeclined
	// InstallRefused means raft refused the install with an error: its
	// sender is not the current leader, or its position or membership is out
	// of bounds. Err carries the reason.
	InstallRefused
	// InstallSuperseded means the install was dropped before it was stepped:
	// a newer install for the shard displaced it, or a recovered step-worker
	// panic lost it from the inbox batch. Raft never saw it.
	InstallSuperseded
	// InstallAbandoned means no verdict will come; raft may still have adopted the install, so keep the artifact.
	InstallAbandoned
)

// InstallVerdict reports what became of a locally injected InstallSnapshot.
type InstallVerdict struct {
	Outcome InstallOutcome
	Err     error
}

// AwaitInstallVerdict registers for raft's verdict on the InstallSnapshot at index, delivered once on a buffered channel; call CancelInstallVerdict if delivery fails.
func (n *Node) AwaitInstallVerdict(index uint64) <-chan InstallVerdict {
	ch := make(chan InstallVerdict, 1)
	n.installMu.Lock()
	defer n.installMu.Unlock()
	// A stopped or failed node steps nothing more, so answer now.
	if n.failed.Load() || n.IsStopped() {
		ch <- InstallVerdict{Outcome: InstallAbandoned}
		return ch
	}
	if n.installVerdicts == nil {
		n.installVerdicts = make(map[uint64]chan InstallVerdict)
	}
	if prev, ok := n.installVerdicts[index]; ok {
		prev <- InstallVerdict{Outcome: InstallSuperseded}
	}
	n.installVerdicts[index] = ch
	return ch
}

// CancelInstallVerdict withdraws the registration for index without a
// verdict: the caller could not inject the install.
func (n *Node) CancelInstallVerdict(index uint64) {
	n.installMu.Lock()
	defer n.installMu.Unlock()
	delete(n.installVerdicts, index)
}

// signalInstallVerdict delivers v to the caller awaiting the install at
// index, if any. Installs nobody awaits — the dedup path's re-deliveries —
// are ignored.
func (n *Node) signalInstallVerdict(index uint64, v InstallVerdict) {
	n.installMu.Lock()
	defer n.installMu.Unlock()
	if ch, ok := n.installVerdicts[index]; ok {
		delete(n.installVerdicts, index)
		ch <- v
	}
}

// awaitingInstall reports whether a receive is still waiting on the verdict
// for the install at index: false once it has been answered or cancelled.
func (n *Node) awaitingInstall(index uint64) bool {
	n.installMu.Lock()
	defer n.installMu.Unlock()
	_, ok := n.installVerdicts[index]
	return ok
}

// abandonInstallVerdicts answers every awaited install with InstallAbandoned.
func (n *Node) abandonInstallVerdicts() {
	n.installMu.Lock()
	defer n.installMu.Unlock()
	for index, ch := range n.installVerdicts {
		delete(n.installVerdicts, index)
		ch <- InstallVerdict{Outcome: InstallAbandoned}
	}
}
