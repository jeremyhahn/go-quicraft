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
	"io"
	"sync"
	"sync/atomic"

	"github.com/jeremyhahn/go-quicraft/pkg/config"
	"github.com/jeremyhahn/go-quicraft/pkg/internal/queue"
	"github.com/jeremyhahn/go-quicraft/pkg/internal/raft"
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

// defaultProposalQueueSize is the maximum number of proposal entries
// buffered in the MPSC proposal queue before backpressure is applied.
const defaultProposalQueueSize = 256

// defaultInboxQueueSize is the maximum number of transport messages
// buffered in the MPSC inbox queue. This must be large enough to absorb
// burst traffic from all peers during a single commit/apply cycle. With
// N nodes each sending Replicate+ReplicateResp at high throughput, the
// previous channel-based inbox (256 cap) overflowed and silently dropped
// critical messages (ReplicateResp, Heartbeat), causing leader step-down
// and multi-node stalls. The MPSC queue pattern (same as proposalQ) with
// a generous limit eliminates this bottleneck while providing bounded
// memory usage.
const defaultInboxQueueSize = 4096

// defaultReadIndexQueueSize is the maximum number of ReadIndex request
// keys buffered in the MPSC queue before backpressure is applied. Each
// slot is a uint64 key (8 bytes), so even at 4096 entries the memory
// overhead is only 32KB per shard. This supports highly concurrent
// ReadIndex workloads where thousands of goroutines issue reads
// simultaneously; the step worker coalesces all pending keys into a
// single raft ReadIndex operation per step cycle.
const defaultReadIndexQueueSize = 4096

// Node is the per-shard controller that engine workers operate on. It
// holds references to the Raft peer, state machine, log reader, and
// cross-worker synchronization state.
//
// Node is not safe for concurrent access from arbitrary goroutines.
// The engine enforces single-owner semantics through its channel-based
// pipeline and the commitPending atomic flag. At any point in time,
// exactly one worker tier owns the node's mutable state.
//
// The inboxC channel is the exception: it is written by the transport
// (any goroutine) and read by the step worker. The buffered channel
// provides the synchronization boundary.
//
// The commitPending and lastApplied fields are cache-line padded to
// prevent false sharing between the step worker (reader) and apply
// worker (writer). On modern x86 and ARM processors, cache lines are
// 64 bytes; padding to 128 bytes provides safety for prefetch-adjacent
// line pairs.
type Node struct {
	// shardID is the globally unique identifier for the Raft shard.
	shardID uint64

	// replicaID is the unique identifier for this replica within the shard.
	replicaID uint64

	// raftMu serializes Tick() and Commit() calls on the Peer. The step
	// worker holds this during Tick(), and the commit worker holds it
	// during Commit(). This allows the step worker to tick nodes whose
	// commitPending is true (preserving heartbeat and checkQuorum
	// timing) without racing on the raft state machine. The mutex is
	// per-node so contention is limited to a single shard. Critical
	// sections are sub-microsecond (Tick ~100ns, Commit ~200ns).
	raftMu sync.Mutex

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

	// notifyCommit indicates whether WAL-durable early notification is
	// enabled for this node. When true, the commit worker fires
	// OnCommitted after WAL persist but before state machine apply.
	// Set from HostConfig.NotifyCommit when the node is created.
	notifyCommit bool

	// proposalQ is a multi-producer single-consumer double-buffered queue
	// for Raft proposals. Multiple Host.Propose goroutines write entries
	// via DeliverProposal, and the step worker drains them via
	// DrainProposals. Separating proposals from transport messages
	// eliminates the channel serialization bottleneck under high
	// concurrent proposal load.
	proposalQ *queue.EntryQueue[proto.Entry]

	// inboxQ is a multi-producer single-consumer double-buffered queue
	// for incoming Raft messages from the transport layer. Multiple
	// transport delivery goroutines write messages via Deliver, and the
	// step worker drains them via DrainInbox using Swap.
	//
	// This replaces the previous buffered channel (inboxC, 256 cap) which
	// silently dropped messages under burst load. The MPSC queue pattern
	// provides higher capacity (4096 default), batch drain via Swap, and
	// slice reuse for zero steady-state allocations. Backpressure is still
	// applied when the queue reaches maxLen -- dropped messages are safe
	// because Raft handles retransmission.
	//
	// After the O19 optimization, proposals are routed through proposalQ
	// instead. inboxQ carries only non-proposal transport messages
	// (Replicate, Heartbeat, ReadIndex, etc.).
	inboxQ *queue.EntryQueue[proto.Message]

	// readIndexQ is a multi-producer single-consumer double-buffered queue
	// for ReadIndex request keys. Multiple Host.ReadIndex goroutines
	// enqueue readKey uint64 values, and the step worker drains them all
	// at once to create a single batched ReadIndex operation per step
	// cycle. This coalesces N concurrent ReadIndex callers into 1 raft
	// ReadIndex + 1 heartbeat quorum round, matching dragonboat's batch
	// ReadIndex behavior.
	//
	// The queue stores only the uint64 key (not the full Message) to
	// minimize memory overhead and avoid the import cycle between the
	// engine and the quicraft package. The Host registers the
	// RequestState in pendingReads before enqueuing the key.
	readIndexQ *queue.EntryQueue[uint64]

	// commitPending is set to true by the step worker after sending an
	// Update to the commit channel, and cleared by the apply worker
	// after applying committed entries. While true, the step worker
	// skips this shard (re-enqueues it) to enforce single-owner
	// semantics on the Peer.
	//
	// Padded to 128 bytes to avoid false sharing with lastApplied.
	commitPending struct {
		atomic.Bool
		_ [127]byte // pad to 128 bytes total (atomic.Bool = 1 byte)
	}

	// lastApplied tracks the highest log index applied to the state
	// machine. Written by the apply worker, read by the step worker
	// to determine which committed entries to include in the next
	// GetUpdate call.
	//
	// Padded to 128 bytes to avoid false sharing with commitPending.
	lastApplied struct {
		atomic.Uint64
		_ [120]byte // pad to 128 bytes total (atomic.Uint64 = 8 bytes)
	}

	// committedIndex tracks the highest log index known to be committed
	// in the Raft log. Written exclusively by the step worker after
	// each GetUpdate call and atomically visible to user goroutines
	// calling FollowerRead. This enables followers to serve reads
	// without a ReadIndex round-trip while still guaranteeing the
	// result reflects all entries committed at the time of the read.
	committedIndex atomic.Uint64

	// applyRetries tracks consecutive SM.Apply failures for the circuit
	// breaker. Reset to 0 on successful apply. Atomic because the
	// snapshot pool goroutine may read adjacent fields on this struct,
	// and non-atomic access across goroutines is undefined under the
	// Go memory model.
	applyRetries atomic.Uint64

	// maxApplyRetries is the threshold for the circuit breaker. When
	// applyRetries >= maxApplyRetries, the shard transitions to failed.
	maxApplyRetries uint64

	// failed indicates whether the circuit breaker has tripped. When
	// true, the step worker skips this shard entirely.
	failed atomic.Bool

	// listener is the event listener for error and lifecycle callbacks.
	// May be nil if no listener is configured.
	listener *config.EventListener

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

	// recovering is set to true when the snapshot pool begins a
	// RecoverFromSnapshot operation and cleared when it completes
	// (successfully or with error). While true, the apply worker
	// skips entry application and the step worker skips GetUpdate
	// for this node to prevent concurrent state machine access
	// between the recovery goroutine and the apply pipeline.
	//
	// Without this guard, a leader could send Replicate messages
	// with entries at ss.Index+1 immediately after InstallSnapshot,
	// and those entries would flow through step->commit->apply
	// concurrently with the snapshot pool's RecoverFromSnapshot
	// call, corrupting state machine state.
	recovering atomic.Bool

	// stopC is closed when the node is unloaded from the engine.
	stopC chan struct{}
}

// NewNode creates a new engine Node for the given shard. The node must
// be loaded into the engine via Engine.LoadNode before it participates
// in the worker pipeline.
func NewNode(
	peer *raft.Peer,
	sm Applier,
	logReader *logdb.LogReader,
	cfg config.Config,
	maxApplyRetries uint64,
	snapshotter Snapshotter,
	snapshotDir string,
	listener *config.EventListener,
) *Node {
	n := &Node{
		shardID:         cfg.ShardID,
		replicaID:       cfg.ReplicaID,
		peer:            peer,
		sm:              sm,
		logReader:       logReader,
		cfg:             cfg,
		proposalQ:       queue.NewEntryQueue[proto.Entry](64, defaultProposalQueueSize),
		inboxQ:          queue.NewEntryQueue[proto.Message](64, defaultInboxQueueSize),
		readIndexQ:      queue.NewEntryQueue[uint64](16, defaultReadIndexQueueSize),
		maxApplyRetries: maxApplyRetries,
		snapshotter:     snapshotter,
		snapshotDir:     snapshotDir,
		listener:        listener,
		stopC:           make(chan struct{}),
	}
	return n
}

// SetNotifyCommit sets the WAL-durable early notification flag. Called
// by the Host when loading a node into the engine to propagate the
// HostConfig.NotifyCommit setting.
func (n *Node) SetNotifyCommit(enabled bool) {
	n.notifyCommit = enabled
}

// DeliverProposal adds proposal entries to the MPSC proposal queue.
// Returns true if the entries were accepted, false if the queue is full.
// When false is returned, the caller should return ErrSystemBusy.
//
// DeliverProposal is safe for concurrent use by multiple goroutines
// (Host.Propose, Host.proposeSession, config change goroutines).
func (n *Node) DeliverProposal(entries []proto.Entry) bool {
	return n.proposalQ.Add(entries)
}

// DrainProposals swaps the proposal queue and returns all accumulated
// entries. The caller should pass the previously returned slice (reset
// to length 0) to reuse its backing array and avoid allocation.
//
// DrainProposals must only be called by the step worker (single consumer).
func (n *Node) DrainProposals(buf []proto.Entry) []proto.Entry {
	return n.proposalQ.Swap(buf)
}

// Deliver sends a Raft message to this node's inbox for processing by
// the step worker. Returns true if the message was accepted, false if
// the inbox queue is at capacity. Dropped messages are safe because
// Raft handles retransmission via heartbeat/replicate cycles.
//
// Deliver is safe for concurrent use by multiple goroutines (transport
// delivery goroutines).
func (n *Node) Deliver(msg proto.Message) bool {
	return n.inboxQ.AddOne(msg)
}

// DeliverReadIndex adds a ReadIndex request key to the MPSC queue.
// Returns true if accepted, false if the queue is full. When false is
// returned, the caller should return ErrSystemBusy.
//
// Multiple Host.ReadIndex goroutines call DeliverReadIndex concurrently.
// The step worker drains all accumulated keys and submits a single
// batched ReadIndex operation, coalescing N callers into 1 heartbeat
// quorum round.
//
// DeliverReadIndex is safe for concurrent use by multiple goroutines.
func (n *Node) DeliverReadIndex(readKey uint64) bool {
	return n.readIndexQ.AddOne(readKey)
}

// DrainReadIndex swaps the ReadIndex queue and returns all accumulated
// keys. The caller should pass the previously returned slice (reset to
// length 0) to reuse its backing array and avoid allocation.
//
// DrainReadIndex must only be called by the step worker (single consumer).
func (n *Node) DrainReadIndex(buf []uint64) []uint64 {
	return n.readIndexQ.Swap(buf)
}

// DrainInbox swaps the inbox queue and returns all accumulated messages.
// The caller should pass the previously returned slice (reset to length
// 0) to reuse its backing array and avoid allocation.
//
// DrainInbox must only be called by the step worker (single consumer).
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

// LastSnapshotIndex returns the log index of the last completed snapshot.
func (n *Node) LastSnapshotIndex() uint64 {
	return n.lastSnapshotIndex.Load()
}

// IsFailed returns true if the circuit breaker has tripped.
func (n *Node) IsFailed() bool {
	return n.failed.Load()
}

// CommittedIndex returns the last known committed log index.
// Updated atomically by the step worker; safe to read from any goroutine.
func (n *Node) CommittedIndex() uint64 {
	return n.committedIndex.Load()
}

// SetCommittedIndex updates the committed log index. Must only be called
// from the step goroutine after processing Raft updates via GetUpdate.
func (n *Node) SetCommittedIndex(idx uint64) {
	n.committedIndex.Store(idx)
}

// handleError reports an error for this shard. It invokes the event
// listener if configured. This does not stop the shard; the circuit
// breaker in the apply worker handles permanent failure escalation.
func (n *Node) handleError(err error) {
	if n.listener != nil && n.listener.OnShardFailed != nil {
		n.listener.OnShardFailed(config.ShardFailedInfo{
			ShardID:   n.shardID,
			ReplicaID: n.replicaID,
			Err:       err,
		})
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

// IsRecovering returns true if a snapshot recovery is in progress for
// this node. While recovering, the apply worker skips entry application
// and the step worker skips GetUpdate to prevent concurrent state
// machine access.
func (n *Node) IsRecovering() bool {
	return n.recovering.Load()
}

// SetRecovering sets the recovering flag. Called by the snapshot pool
// before and after RecoverFromSnapshot.
func (n *Node) SetRecovering(v bool) {
	n.recovering.Store(v)
}

// StopC returns a read-only channel that is closed when the node stops.
// Used to pass cancellation to state machine operations like Open.
func (n *Node) StopC() <-chan struct{} {
	return n.stopC
}

// IsMember returns true if the given replicaID is a known member of
// this shard (in remotes, observers, or witnesses). This is used by
// the transport message handler to enforce per-shard authorization,
// preventing compromised nodes from injecting messages into shards
// they do not belong to.
//
// When the peer is nil (e.g., during initialization), IsMember returns
// true to avoid rejecting legitimate messages before the raft state
// machine is fully initialized.
func (n *Node) IsMember(replicaID uint64) bool {
	if n.peer == nil {
		return true
	}
	return n.peer.IsMember(replicaID)
}

// DiagState returns diagnostic pipeline state: commitPending, proposalQ depth,
// inboxQ depth, readIndexQ depth. Used for debugging pipeline stalls.
func (n *Node) DiagState() (commitPending bool, proposalQLen int, inboxLen int, readIndexQLen int) {
	return n.commitPending.Load(), n.proposalQ.Len(), n.inboxQ.Len(), n.readIndexQ.Len()
}

// DiagRaftState returns diagnostic raft state for debugging.
func (n *Node) DiagRaftState() (term, committed, processed, lastIdx uint64, msgs int, state string) {
	if n.peer == nil {
		return 0, 0, 0, 0, 0, "nil"
	}
	return n.peer.DiagRaftState()
}

// DiagRemotes returns diagnostic remote state for debugging commit stalls.
func (n *Node) DiagRemotes() string {
	if n.peer == nil {
		return "nil"
	}
	return n.peer.DiagRemotes()
}

// Stop signals the node to stop. Called when unloading from the engine.
func (n *Node) Stop() {
	select {
	case <-n.stopC:
		// Already stopped.
	default:
		close(n.stopC)
	}
}
