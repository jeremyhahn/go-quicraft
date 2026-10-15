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

// appliedSetter is an optional upgrade of Snapshotter. When the concrete
// snapshotter implements it (the rsm.StateMachine does), the snapshot pool
// seeds the snapshotter's own applied counter after RecoverFromSnapshot
// installs snapshot state, keeping it consistent with the node's
// (lastApplied, lastAppliedTerm) markers. saveSnapshot reads its snapshot
// index from Snapshotter.LastApplied(), so the counter MUST reflect the
// recovered index or the next snapshot would pair a stale index with
// post-recovery state.
type appliedSetter interface {
	SetLastApplied(idx uint64)
}

// gatedSnapshotter is an optional upgrade of Snapshotter. When the concrete
// snapshotter implements it (the rsm.StateMachine does), the snapshot pool uses
// it to release the apply gate as soon as the consistent point-in-time has been
// captured — session state marshaled and, for concurrent/on-disk SMs,
// PrepareSnapshot completed — so the (potentially long) streaming of an on-disk
// SM's snapshot runs without pausing applies. afterCapture is invoked exactly
// once after capture and before streaming; for regular in-memory SMs the whole
// save is the capture, so it fires at the end. If the save errors before the
// capture completes, afterCapture is not called and the caller's deferred
// release handles the gate. Snapshotters that do not implement this are gated
// for the full save (correct, just without early release).
type gatedSnapshotter interface {
	SaveSnapshotGated(ctx context.Context, w io.Writer, stopper <-chan struct{}, afterCapture func()) error
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

	// diagEnabled gates publication of the diagnostic snapshot. The raft
	// state (Peer.r) is owned by the step worker and the commit worker
	// (under raftMu); reading it from an external goroutine races. So the
	// diagnostic accessors do NOT touch the Peer directly: they read
	// diagSnap, a snapshot the step worker publishes under raftMu during
	// tick(). Publication only happens once a diagnostic has been
	// requested (diagEnabled set true by the first DiagRaftState/
	// DiagRemotes call), so steady-state cost is a single atomic load per
	// tick.
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
	// Proposals are routed through proposalQ
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

	// commitPending is set to true by the step worker BEFORE it sends the
	// Update to the commit channel, and cleared by the apply worker after
	// applying committed entries. The ordering is load-bearing, not incidental:
	// the flag must already be visible when the commit worker picks the item
	// up, or the step worker could extract a second Update for the same shard
	// and the same entries could be applied twice.
	//
	// While true, the step worker does not run its normal cycle for this shard;
	// it takes the processCommitPendingInbox path instead, which steps inbound
	// protocol messages under raftMu so ReadIndex quorum and elections still
	// make progress. Nothing is re-enqueued.
	//
	// Padded to 128 bytes to avoid false sharing with lastApplied.
	commitPending struct {
		atomic.Bool
		_ [127]byte // pad to 128 bytes total (atomic.Bool = 1 byte)
	}

	// sealHeld reports that the commit worker has PARKED this shard: its log
	// write was refused because the at-rest barrier is sealed, so nothing of
	// it reached disk and nothing it owes may be sent, acked or applied (#296).
	// Set only while commitPending is true and cleared before commitPending
	// is, both by the commit worker (holdSealed, releaseHeld). While it is
	// set the step worker neither ticks the node nor steps its inbox: the shard
	// is frozen, as a replica paused mid-fsync is, so its raft state — the
	// unstable entries the refused write carried — is unchanged when the
	// write is re-derived and retried after the unseal.
	sealHeld atomic.Bool

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

	// lastAppliedTerm is the Raft term of the entry at lastApplied. It is
	// written together with lastApplied by the apply worker (and seeded on
	// recovery), so the snapshot pool can read a CONSISTENT (index, term) pair
	// for the snapshot point while applies are quiesced by the saving gate.
	// Recording a term that does not match lastApplied would corrupt raft log
	// matching on InstallSnapshot; recording an index below the captured SM
	// state would cause entries to be re-applied on recovery. Reading it here
	// (instead of peer.AppliedToTerm under raftMu) also removes the data race
	// with the step worker's lock-free inMemory.appliedToTerm writes.
	lastAppliedTerm atomic.Uint64

	// committedIndex tracks the highest log index known to be committed
	// in the Raft log. Written exclusively by the step worker (see
	// publishLogMarkers) and atomically visible to user goroutines
	// calling FollowerRead. This enables followers to serve reads
	// without a ReadIndex round-trip while still guaranteeing the
	// result reflects all entries committed at the time of the read.
	committedIndex atomic.Uint64

	// processedIndex mirrors the Raft log's processed marker — the highest
	// committed index already extracted for apply. Written by the step
	// worker alongside committedIndex, it lets the snapshot pool decide
	// whether the log has installed a snapshot WITHOUT reading the Peer
	// from its own goroutine, which the Peer's single-owner invariant
	// forbids (see the diagSnap comment above).
	processedIndex atomic.Uint64

	// failed indicates whether the shard-failure latch has tripped. The apply
	// policy is FAIL-FAST: failApply sets it on the FIRST SM.Apply (or
	// decompression) error and does NOT advance lastApplied — a deterministic
	// apply failure cannot be made to succeed by retrying, and advancing past a
	// failed entry would diverge this state machine from its peers, breaking
	// Raft state-machine safety. While true, the step worker skips this shard
	// entirely.
	failed atomic.Bool

	// listener is the event listener for error and lifecycle callbacks.
	// May be nil if no listener is configured.
	listener *config.EventListener

	// listeners carries the listener notifications above off the goroutine that
	// produced them. handleError is reachable from the step worker while it
	// holds nodesMu.RLock, and from a per-shard apply goroutine that a
	// synchronous UnloadNode joins on — so a listener reacting to OnShardFailed
	// by stopping the shard deadlocks against its own producer. Set by
	// Engine.LoadNode; nil on a node built outside an engine, which dispatches
	// inline (correctly: with no worker goroutine there is no lock to be
	// reentrant against).
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

	// recovering is set to true when the snapshot pool begins a
	// RecoverFromSnapshot operation and cleared when it completes
	// (successfully or with error). It gates STATE MACHINE ACCESS ONLY:
	// the apply worker holds committed batches at the gate, and the step
	// worker stops extracting committed entries (GetUpdate is called with
	// moreEntriesToApply=false, and the commit worker skips the piggyback
	// extraction). Without that, a leader could send Replicate messages
	// with entries at ss.Index+1 immediately after InstallSnapshot and
	// those entries would flow through step->commit->apply concurrently
	// with RecoverFromSnapshot, corrupting state machine state.
	//
	// It deliberately does NOT gate Raft protocol traffic. The node keeps
	// draining its inbox, stepping RequestVote/Heartbeat, persisting hard
	// state and sending the resulting responses, because none of that
	// touches the state machine. Gating it made a recovering replica
	// invisible to consensus for the entire unbounded duration of user
	// RecoverFromSnapshot code — two replicas recovering at once dissolved
	// a five-replica shard's quorum with every node still up. The one
	// message type still withheld is InstallSnapshot (deferred by the step
	// worker), which would move the log's snapshot floor out from under the
	// recovery in progress.
	//
	// Client work (proposals, ReadIndex) also stays queued: it cannot
	// produce a result until applies resume.
	recovering atomic.Bool

	// redrive tracks snapshot recoveries rejected before the state machine
	// restore began, so the step worker re-queues them with backoff and the
	// snapshot pool fails the shard once they are exhausted. See
	// recoveryRedrive.
	redrive recoveryRedrive

	// saving is set to true while the snapshot pool captures a consistent
	// point-in-time for SaveSnapshot, and cleared once the capture completes
	// (the gate is released before the potentially-long streaming of an
	// on-disk/concurrent SM's snapshot, which reads an immutable prepared
	// context and is safe to run alongside applies). While true, the apply
	// worker skips entry application for this node — exactly like recovering —
	// so the snapshot read of shared in-memory state (the session manager,
	// which is not safe for concurrent use, and a regular in-memory SM) cannot
	// race a concurrent SM.Apply. Without this guard, SaveSnapshot's
	// sessions.MarshalTo iterates the session map while Apply writes it,
	// producing a fatal "concurrent map iteration and map write" that bypasses
	// recover() and crashes the whole host. saving and recovering are mutually
	// exclusive per node (both are gated by the snapshotting flag), so at most
	// one is set at a time.
	saving atomic.Bool

	// applyInFlight counts apply closures that are currently executing (or
	// about to check the recovering/saving gate) for this node. It is
	// incremented at the very top of the per-item apply closure — BEFORE the
	// gate check — and decremented when the closure returns. Snapshot recovery
	// AND snapshot save use it as a drain barrier: after setting the gate, the
	// snapshot worker waits for applyInFlight to reach 0 before touching the
	// state machine, guaranteeing no apply is mid-`SM.Apply` when
	// RecoverFromSnapshot replaces SM state or SaveSnapshot reads it. The
	// increment-then-check / set-then-wait ordering is a standard lock-free
	// quiesce: any apply that passed the gate must have incremented first, so
	// the drain observes it.
	applyInFlight atomic.Int64

	// stopC is closed when the node is unloaded from the engine.
	stopC chan struct{}
	// stopOnce guards the close of stopC so Stop() is idempotent and safe
	// against concurrent callers (a future second caller would otherwise
	// double-close and panic; today the sole caller is engine.Stop's serial
	// teardown, but this makes the contract explicit and race-proof).
	stopOnce sync.Once

	// installMu guards installVerdicts: the verdict each locally injected
	// InstallSnapshot is waiting on, keyed by snapshot index. See
	// AwaitInstallVerdict.
	installMu       sync.Mutex
	installVerdicts map[uint64]chan InstallVerdict

	// leaseMu admits the durable writes a snapshot receive makes on this
	// incarnation's behalf — publishing its directory, recording an adopted
	// install, raising the log floor — and lets UnloadNode wait them out. A
	// lease is a read lock taken only while the node is not stopped;
	// awaitWriteLeases takes the write lock once stopC is closed, so when it
	// returns no lease is held and none can be granted.
	leaseMu sync.RWMutex

	// snapshotDirMu serializes the changes a local save and a snapshot receive
	// make to the shard's canonical snapshot-<index> directories and to its
	// LogDB snapshot record: a save's publish and record, a receive's publish,
	// its record of an adopted install, and a failed receive's discard. (The
	// commit worker also records a snapshot raft adopted, from the Update,
	// outside this lock; it names the same directory the adopting receive
	// published and claims, and a non-advancing record is a no-op.) The write
	// lease is SHARED — it fences UnloadNode, not one writer from another — so
	// without this a save could replace a receive's published directory while
	// the receive awaited raft's verdict, or a declined receive could discard
	// the directory a save had just recorded. Taken after the write lease when
	// both are held, never before, so the two cannot deadlock.
	snapshotDirMu sync.Mutex

	// claimedSnapshotDirs holds the indexes whose canonical directory a
	// snapshot receive has published and not yet settled — recorded, or
	// discarded after raft declined it. A save must not replace such a
	// directory, though no record names it yet; it may replace any OTHER
	// unrecorded occupant, which is an orphan. Guarded by snapshotDirMu.
	claimedSnapshotDirs map[uint64]struct{}

	// pinnedSnapshotDirs counts, per cleaned directory, the snapshot
	// recoveries reading it; the snapshot GC spares every one. Guarded by
	// snapshotDirMu.
	pinnedSnapshotDirs map[string]int

	// unloaded is set by UnloadNode before it stops the node: this
	// incarnation is being torn down while the engine keeps running, and a
	// new incarnation of the shard may follow. A node stopped by Engine.Stop
	// alone is never marked, so the shutdown drain keeps persisting,
	// committing and applying its work. The pipeline's incarnation guards
	// read this rather than stopC, which both kinds of stop close.
	unloaded atomic.Bool

	// paused is set while the replica is paused for a runtime seal
	// (Engine.PauseNode) and cleared by Engine.ResumeNode. While it is set
	// the step worker neither ticks the node nor steps its inbox (inbound
	// messages are discarded, so the replica votes, acknowledges and serves
	// nothing), the apply goroutine parks before the state machine, and the
	// snapshot pool refuses the shard's saves and recoveries. It is never
	// persisted. Every gate costs one atomic load while it is clear.
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
	// Seed the published log markers from the peer's recovered log before
	// anyone else can read them. publishLogMarkers otherwise first runs on the
	// step worker's first tick, and until then CommittedIndex reports 0. The
	// host records that value as a pending proposal's committed-index floor,
	// so without the seed a proposal registered between StartReplica returning
	// and the first tick would get floor 0 and rest on the random key alone
	// against a replayed entry after a restart. Nothing owns the peer yet, so
	// reading it here is as race-free as the worker path.
	n.publishLogMarkers()
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

// DeliverTrackedProposal is DeliverProposal that also records, in *at, where
// the entries entered the proposal queue, so ProposalTaken can later tell
// whether the step worker has taken them — the moment a proposal is handed to
// raft. The host uses it for every proposal it registers. *at is written under
// the queue's lock and must only be read through ProposalTaken.
func (n *Node) DeliverTrackedProposal(entries []proto.Entry, at *uint64) bool {
	return n.proposalQ.AddTracked(entries, at)
}

// ProposalTaken reports whether the proposal DeliverTrackedProposal recorded
// in *at has been handed to raft, and whether that answer is final. The step
// worker declares a batch handed off (HandOffProposals) only as it enters
// Peer.Handle, which appends it or forwards it to the leader, or refuses all
// of it (OnProposalRejected); a batch it took from the queue but never
// brought that far is not handed off. The answer is final once the queue is
// closed — when the node fails or stops — since nothing is handed off after
// that; until then an untaken proposal may still be taken. A proposal that
// was never delivered (*at is zero) is not taken.
func (n *Node) ProposalTaken(at *uint64) (taken, final bool) {
	return n.proposalQ.Taken(at)
}

// DrainProposalBatch is DrainProposals that also returns the id of the batch
// it took, for HandOffProposals; 0 when it took nothing.
func (n *Node) DrainProposalBatch(buf []proto.Entry) ([]proto.Entry, uint64) {
	return n.proposalQ.SwapBatch(buf)
}

// HandOffProposals declares the batch DrainProposalBatch returned as id handed
// to raft. The step worker calls it immediately before Peer.Handle and must
// not call Peer.Handle when it reports false: the node failed or stopped, its
// queue is closed, and the batch's proposals have been reported never taken.
func (n *Node) HandOffProposals(id uint64) bool {
	return n.proposalQ.HandOff(id)
}

// DrainProposals swaps the proposal queue and returns all accumulated
// entries. The caller should pass the previously returned slice (reset
// to length 0) to reuse its backing array and avoid allocation.
//
// DrainProposals must only be called by the step worker (single consumer).
func (n *Node) DrainProposals(buf []proto.Entry) []proto.Entry {
	return n.proposalQ.Swap(buf)
}

// Deliver sends a WIRE-RECEIVED Raft message to this node's inbox for
// processing by the step worker. Returns true if the message was accepted,
// false if the inbox queue is at capacity. Dropped messages are safe because
// Raft handles retransmission via heartbeat/replicate cycles.
//
// The message is stamped OriginRemote. Local and wire messages share one
// inbox, so the origin is what tells raft.step which of the two it is
// holding: the term gate (and the membership adoption that hangs off it) is
// exempted only for LOCALLY injected control messages, and that exemption
// must be unreachable from the network. Stamping here rather than trusting
// the decoder means a future caller cannot hand wire bytes to the inbox with
// a local stamp still attached.
//
// Deliver is safe for concurrent use by multiple goroutines (transport
// delivery goroutines).
func (n *Node) Deliver(msg proto.Message) bool {
	msg.Origin = proto.OriginRemote
	return n.inboxQ.AddOne(msg)
}

// DeliverLocal sends a message injected by THIS process (the snapshot
// receive path's InstallSnapshot, the transport's SnapshotStatus, the host's
// LeaderTransfer) to this node's inbox, stamping it OriginLocal.
//
// These messages carry no term — they are not produced by raft.send() and
// have no term to carry — so raft.step exempts them from its term gate. The
// exemption is keyed on the origin stamp, which the wire format cannot
// express, so an authenticated peer cannot reach it by naming one of these
// types in a MessageBatch.
//
// DeliverLocal is safe for concurrent use by multiple goroutines.
func (n *Node) DeliverLocal(msg proto.Message) bool {
	msg.Origin = proto.OriginLocal
	return n.inboxQ.AddOne(msg)
}

// redeliver re-injects a message that was already drained from this node's
// inbox, preserving its recorded origin. It exists for the step worker's
// deferred-snapshot buffer: those messages were admitted through Deliver or
// DeliverLocal once already, and re-stamping them would either grant a wire
// message a local origin or strip a legitimate local one.
func (n *Node) redeliver(msg proto.Message) bool {
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

// ReplicaSnapshotDir returns this replica's own snapshot directory,
// <SnapshotDir()>/shard-<N>/replica-<N>: the parent of every canonical
// snapshot directory, owned by this replica alone and swept for stale scratch
// files at every start and recovery (SweepStaleDecryptTmpTree).
func (n *Node) ReplicaSnapshotDir() string {
	return ReplicaSnapshotDir(n.snapshotDir, n.shardID, n.replicaID)
}

// SnapshotSaveDir returns the on-disk directory for the snapshot at index:
// <SnapshotDir()>/shard-<N>/replica-<N>/snapshot-<index>. This is the SAME
// construction the snapshot pool uses for saving, garbage collection, and
// DEK-purge-floor scans, so an inbound-snapshot receiver that writes here stays
// consistent with — and visible to — those paths, including when a per-shard
// DataDir roots SnapshotDir() off the shared NodeHostDir (otherwise received
// snapshots land where GC/purge never look: they leak, and key rotation can
// purge a DEK such a snapshot still needs).
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

// SeedLastApplied initializes the node's applied markers to a recovered
// (index, term) on restart, before the node is loaded into the engine pipeline.
// lastApplied, lastAppliedTerm, and lastSnapshotIndex are set so that follower
// and linearizable reads can be served immediately after recovery without
// waiting for a subsequent write to advance the applied marker, and so the
// first snapshot taken after recovery (before any apply) records the correct
// term. This must be called before LoadNode and must match the raft peer's
// processed marker. For a fresh start (idx == 0) this is a harmless no-op.
func (n *Node) SeedLastApplied(idx, term uint64) {
	n.lastApplied.Store(idx)
	n.lastAppliedTerm.Store(term)
	n.lastSnapshotIndex.Store(idx)
}

// IsFailed returns true if the shard-failure latch has tripped.
func (n *Node) IsFailed() bool {
	return n.failed.Load()
}

// failShard is the one latch-and-report path for a shard failure, in this
// order:
//
//  1. Trip node's failure latch.
//  2. Run failBatch, when it is not nil: the caller's own batch of requests,
//     failed with their specific error (a SaveStateError, an ApplyError).
//     It runs whether or not this call won the latch — the batch is the
//     caller's, and no other path will answer it.
//  3. If this call tripped the latch, report err once on each outward
//     channel: the node's EventListener.OnShardFailed (handleError) and the
//     engine Callback.OnShardFailed, with which the host fails every request
//     still pending on the shard with ErrShardFailed.
//
// The latch comes first because failing a request wakes its caller, and a
// caller that retried before the latch tripped would pass the host's
// admission check and register a request the halted shard never answers. The
// reports come last so the batch keeps its specific error rather than the
// halt's generic one.
//
// It serves every failure: the apply and commit workers' batch failures, a
// step-worker GetUpdate error or install panic, a failed or panicking
// snapshot recovery, and, through Engine.FailShard, the host's own
// detections. A shard another path failed first has been reported by that
// path, and is not reported again; the winner always performs both reports,
// even when failBatch panics, and a panic in one report cannot suppress the
// other. A panic from failBatch or a report is re-raised once both reports
// have been made, so the worker's own recovery still sees it. It reports
// whether this call tripped the latch.
func failShard(node *Node, cb Callback, err error, failBatch func()) bool {
	if !node.markFailed() {
		if failBatch != nil {
			failBatch()
		}
		return false
	}
	// This call won the latch, so it alone owes the reports, and no other
	// path will make them: a worker's panic recovery that fails the shard
	// again loses the latch. They are therefore made from a deferred call,
	// which runs even when failBatch panics; that panic then continues to
	// the worker's recovery with its own stack once they are made.
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

// reportShardFailure makes failShard's two reports — the node's
// EventListener.OnShardFailed (handleError) and the engine
// Callback.OnShardFailed — each isolated from the other, so a panic or a
// runtime.Goexit in one cannot suppress the other. It returns the first report's panic, if any, for
// the caller to re-raise once both have run.
func reportShardFailure(node *Node, cb Callback, err error) (panicked any) {
	report := func(fn func()) {
		defer func() {
			if r := recover(); r != nil && panicked == nil {
				panicked = r
			}
		}()
		fn()
	}
	// The engine callback is a deferred call of its own, not a statement after
	// the listener report: recover returns nil for runtime.Goexit, so a
	// listener that Goexits cannot be caught, and only a deferred call still
	// runs as the goroutine exits. The callback is what drains the shard's
	// pending requests on the host.
	if cb != nil {
		defer report(func() { cb.OnShardFailed(node.shardID, err) })
	}
	report(func() { node.handleError(err) })
	return panicked
}

// markFailed is the ONE way a node becomes failed, and reports whether this
// call is the one that failed it. The latch is a compare-and-swap so that,
// when failures race — a SaveState failure on the commit worker and a recovery
// exhausting its attempts, say — exactly one caller wins, and the winner alone
// emits the terminal OnShardFailed reports. Its only caller is failShard, so
// the winner always performs both.
//
// A failed node's step worker never steps another message, so every install
// waiting on a verdict is answered InstallAbandoned here; setting the flag
// anywhere else would leave a snapshot receive blocked holding a transport
// receive slot until the shard was reloaded. The flag is set before the abandon
// takes installMu, so an AwaitInstallVerdict that takes installMu afterwards
// sees it and answers at once. Only the winner abandons: a loser found the
// flag already set by a winner that abandons after setting it.
func (n *Node) markFailed() bool {
	if !n.failed.CompareAndSwap(false, true) {
		return false
	}
	// A failed node hands nothing further to raft: closing the proposal queue
	// fences the step worker from taking anything still queued, which makes a
	// queued proposal's fate final (ProposalTaken) before the failure is
	// reported and the host fails what is pending.
	n.proposalQ.Close()
	n.abandonInstallVerdicts()
	return true
}

// AcquireWriteLease admits one durable write made on this incarnation's
// behalf outside the engine's own pipeline, and reports false once the node
// has stopped. A true result must be paired with exactly one
// ReleaseWriteLease, and the holder must not acquire another lease before
// releasing it: a nested acquire deadlocks against a waiting UnloadNode.
func (n *Node) AcquireWriteLease() bool {
	n.leaseMu.RLock()
	if n.IsStopped() {
		n.leaseMu.RUnlock()
		return false
	}
	return true
}

// TryAcquireWriteLease is AcquireWriteLease for a caller that must never
// block — a step worker, which owns a whole partition of shards. It also
// reports false when an unload is already waiting for the leases to drain:
// that wait begins only after Stop, so the node is stopping, and blocking
// behind it would stall every shard on the worker for as long as the holder
// runs. A true result must be paired with exactly one ReleaseWriteLease.
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

// PinSnapshotDir marks dir as being read by a snapshot recovery, so the
// snapshot GC spares it until UnpinSnapshotDir. Recovery chooses the directory
// the LogDB record names at that moment and reads it by path for its whole
// length; a receive or save recording a newer snapshot meanwhile would
// otherwise GC it underneath the recovery and fail the shard. The caller
// holds the snapshot-directory lock; pins nest.
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

// GCSparedSnapshotDirs returns the directories the snapshot GC must spare
// besides the recorded one: those recoveries are reading (PinSnapshotDir) and
// those receives have published and not yet settled (ClaimSnapshotDir). A
// claimed directory is unrecorded until raft's verdict, and GC selects by
// index alone, so unrecorded directories at higher indexes — receives a stop
// abandoned, or a crash left — could push it into the removal window; raft
// adopting it then records a snapshot whose directory is gone. The caller
// holds the snapshot-directory lock.
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

// IsSnapshotDirPinned reports whether a recovery is reading dir. A publish
// that would replace an unrecorded occupant of a canonical name must refuse
// a pinned one: a recovery falls back to the newest directory on disk when
// the record names none, so what it reads can be unrecorded. The caller holds
// the snapshot-directory lock.
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

// publishLogMarkers copies the Raft log's committed and processed markers
// out of the Peer into atomics other goroutines may read. It MUST run on
// the step worker goroutine — the Peer's owner — because that is the only
// race-free place to read Peer.r from (the commit worker is excluded by
// raftMu, which tick() holds, or by commitPending).
//
// Both markers are monotonically non-decreasing (raftLog.commitTo,
// appliedTo's assertion, and restore's "snapshot index >= committed"
// invariant), so a reader that loads them separately still gets values that
// were each true at some instant, and a conjunction over both — the only
// way they are consumed — therefore held at the later of the two. A reader
// can only ever be BEHIND the log, never ahead of it, which is the safe
// direction for every consumer.
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

// PendingMemberIdentity returns the node identity a config-carrying entry in
// this replica's log admits replicaID with, above the committed configuration,
// and whether there is one. See raft.Peer.PendingMemberIdentity. Safe from any
// goroutine.
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

// handleError reports an error for this shard. It invokes the event
// listener if configured. This does not stop the shard; the circuit
// breaker in the apply worker handles permanent failure escalation.
//
// The listener is NOT run on the calling goroutine once the node is loaded into
// an engine. handleError is reachable from the step worker (a GetUpdate error,
// under nodesMu.RLock) and from a per-shard apply goroutine that a synchronous
// Engine.UnloadNode joins on, and "react to a failed shard by stopping it" is
// the natural handler to write — which deadlocked both paths against their own
// producer. See listenerDispatcher for the ordering guarantee and the
// queue-full policy.
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
	// Dropped rather than retried: this is the terminal-failure notification,
	// and the only way the queue is full is a listener that is not consuming,
	// so holding engine liveness for it would be the deadlock this dispatcher
	// exists to remove. The event still reaches the operator through the log
	// below, and the shard's failed state remains observable via GetShardInfo.
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

// IsRecovering returns true if a snapshot recovery is in progress for this
// node. While recovering, the apply worker holds committed batches and the
// step worker extracts no committed entries, so nothing reaches the state
// machine; Raft protocol traffic keeps flowing. See the recovering field.
func (n *Node) IsRecovering() bool {
	return n.recovering.Load()
}

// SetRecovering sets the recovering flag. Called by the snapshot pool
// before and after RecoverFromSnapshot.
func (n *Node) SetRecovering(v bool) {
	n.recovering.Store(v)
}

// snapshotAdoptionPending reports whether the raft log's processed marker
// stands ABOVE the state machine's lastApplied — the state a replica is in
// between the instant a received snapshot is installed into the log
// (raftLog.restore sets committed and processed to the snapshot index) and the
// instant the snapshot pool finishes RecoverFromSnapshot and seeds lastApplied
// to that same index.
//
// Committed entries must not be extracted in that window. Raft offers them
// from processed+1 — the snapshot index + 1 — while lastApplied is still at
// its pre-snapshot value, so the batch reaches the apply worker with a hole
// beneath it and is refused as ErrApplyGap, failing a shard that had just
// finished joining. The recovering gate does not cover the window: the
// snapshot pool raises it from its own goroutine and cannot do so until it has
// OBSERVED the install (awaitRecoverableLog, polled), so every instant between
// the install and that observation is unguarded — which is precisely the
// window a leader fills with the entries that follow the snapshot.
//
// Withholding extraction cannot wedge the log: extraction is the only thing
// that advances the processed marker, so the entries stay in the log and are
// offered again on a later cycle, once recovery seeds lastApplied (or once a
// failed recovery fails the shard, which the step worker skips entirely).
//
// Outside that window the two markers cannot diverge in this direction. The
// apply worker advances lastApplied to the batch's last index before it clears
// commitPending, and no extraction happens while commitPending is set; an
// apply that does NOT advance lastApplied fails the shard.
//
// MUST be called with the same exclusive Peer access GetUpdate requires: from
// the step worker, or from the commit worker under raftMu. It deliberately
// reads the Peer rather than the published markers peerLogMarkers uses — the
// snapshot pool must see a stale-and-behind view (never a torn one) from its
// own goroutine, whereas this gate has to observe an install in the very cycle
// that performs it, one statement before the extraction it guards.
func (n *Node) snapshotAdoptionPending() bool {
	if n.peer == nil {
		return false
	}
	_, processed := n.peer.LogMarkers()
	return processed > n.lastApplied.Load()
}

// IsSaving returns true while the snapshot pool is capturing a consistent
// point-in-time for SaveSnapshot. While saving, the apply worker skips entry
// application for this node so the snapshot read of shared in-memory state
// (session manager, regular SM) cannot race a concurrent SM.Apply.
func (n *Node) IsSaving() bool {
	return n.saving.Load()
}

// SetSaving sets the saving flag. Called by the snapshot pool around the
// consistent-capture phase of SaveSnapshot.
func (n *Node) SetSaving(v bool) {
	n.saving.Store(v)
}

// WaitForApplyDrain blocks until no apply closure is in flight for this node,
// or until stopC is closed. It MUST be called only after SetRecovering(true)
// or SetSaving(true): the gate stops new applies from accessing the state
// machine, and this drains the at-most-one apply that may have passed the gate
// before the flag was set. Together they guarantee RecoverFromSnapshot (or the
// consistent-capture phase of SaveSnapshot) has exclusive access to the state
// machine. Returns false if stopC is closed, whether or not an apply was in
// flight: a stopping node's state machine is about to be closed, so the
// caller must not begin an operation on it. The check after the drain is not
// a fence on its own — Stop can land a moment later — which is why a recovery
// also holds the node's write lease across the restore (runRecovery).
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

// IsMember reports whether replicaID is in this shard's current configuration.
// It is a view of replicated state, NOT an authorization decision — see
// IsRemoved, which is what the transport uses.
//
// When the peer is nil (e.g. during initialization) it returns true, since
// nothing is known yet.
func (n *Node) IsMember(replicaID uint64) bool {
	if n.peer == nil {
		return true
	}
	return n.peer.IsMember(replicaID)
}

// MemberBinding returns what this shard's COMMITTED configuration authorizes
// replicaID with — its node identity, its address, and whether that address
// identifies replicaID exclusively — plus whether the replica is in the
// configuration at all. It is the authorization input the transport message
// handler binds a claimed msg.From to; see raft.Peer.MemberBinding for why this
// — and not IsMember — is safe to enforce on the receive path, and for how the
// two fields rank.
//
// It FAILS CLOSED when the peer is nil (e.g. during initialization): with no
// replicated configuration there is nothing committed to authorize against, and
// answering "unknown" would let the caller admit a message the shard cannot
// vouch for. Note this is the opposite convention to IsMember, which reports
// true when nothing is known — IsMember is a view, this is a gate.
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

// IsJoining reports whether this node's own configuration is known-incomplete
// (started with join=true and not yet caught up to the entries naming the
// shard). Used by the transport handler to decide whether first-contact address
// learning is legitimate. False when the peer is nil.
func (n *Node) IsJoining() bool {
	if n.peer == nil {
		return false
	}
	return n.peer.IsJoining()
}

// IsRemoved reports whether replicaID carries a committed Removed tombstone for
// this shard. This is the per-shard authorization predicate the transport
// message handler enforces on inbound messages: a DENY-list, because a
// replica's member set lags by design and using it as an allow-list makes a
// stale replica reject the leader that would bring it up to date. See
// raft.Peer.IsRemoved for the full rationale.
//
// When the peer is nil (e.g. during initialization) nothing is known to be
// removed, so it returns false and the message is allowed through to a raft
// layer that will apply its own term and log rules.
func (n *Node) IsRemoved(replicaID uint64) bool {
	if n.peer == nil {
		return false
	}
	return n.peer.IsRemoved(replicaID)
}

// IsRemovedIdentity reports whether the AUTHENTICATED node identity of a
// connection is one this shard has committed a removal for. It is the half of
// the removal deny-list that a removed replica cannot evade by claiming a
// different msg.From; see raft.Peer.IsRemovedIdentity for the full rationale and
// for the address-only-shard residual.
//
// When the peer is nil (e.g. during initialization) nothing is known to be
// removed, so it returns false — matching IsRemoved, and for the same reason:
// the message is allowed through to a raft layer that will apply its own term
// and log rules.
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

// DiagPipeline is a point-in-time snapshot of a node's pipeline state.
//
// It carries the three gates that can hold a shard back — commitPending, and
// the snapshot recovering/saving gates with their apply drain counter — not
// just commitPending. A shard whose recovering gate is up applies nothing and
// extracts no committed entries for as long as user RecoverFromSnapshot code
// runs, which is exactly the state an operator needs to see when diagnosing a
// stalled or election-churning shard.
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

// publishDiagLocked captures the Peer's diagnostic state into diagSnap.
// The CALLER MUST HOLD n.raftMu (the step worker holds it during tick()),
// which excludes the commit worker and — because publication runs on the
// step worker goroutine — is sequential with Handle/GetUpdate. This is
// the only race-free point to read Peer.r from. It is a no-op until a
// diagnostic accessor has enabled publication, so it adds only one atomic
// load to the tick path in the common (no-debugger) case.
func (n *Node) publishDiagLocked() {
	if n.peer == nil || !n.diagEnabled.Load() {
		return
	}
	term, committed, processed, lastIdx, msgs, state := n.peer.DiagRaftState()
	// Publish STRUCTURED progress and format only when a caller asks for text.
	// Publishing the rendered DiagRemotes string would cost an fmt.Sprintf per
	// follower per tick for as long as diagnostics stay enabled, whether or not
	// anyone reads it.
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

// DiagRaftState returns diagnostic raft state for debugging. It reads the
// snapshot last published by the step worker rather than touching the
// Peer directly (which would race with the step/commit workers). The
// first call enables publication; until the next tick publishes, it
// reports a warming-up placeholder.
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

// ReplicaProgress returns the leader's view of every follower's replication
// position, or nil when this replica is not the leader.
//
// Same publication path as DiagRemotes — it reads the snapshot the step worker
// stores under raftMu — so it neither races the workers nor blocks them. Like
// the other diagnostics it is opt-in: the first call enables publication, and
// until the next tick publishes it returns nil.
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
	// InstallAbandoned means the node stopped or failed, or a recovered
	// step-worker panic interrupted the install's own step, so no verdict will
	// come. It does NOT prove raft never stepped the install: a step already
	// in progress may have adopted it. A caller must therefore keep the
	// artifact, never discard it, on this outcome.
	InstallAbandoned
)

// InstallVerdict reports what became of a locally injected InstallSnapshot.
type InstallVerdict struct {
	Outcome InstallOutcome
	Err     error
}

// AwaitInstallVerdict registers interest in raft's verdict on the
// InstallSnapshot at index, which the caller is about to inject with
// DeliverLocal. The snapshot receive path makes a received snapshot durable —
// its LogDB record, the log floor, the state-machine recovery — only once
// raft has ADOPTED it, because raft can still refuse or decline an install
// that passed the host's own checks. Exactly one verdict is delivered on the
// returned channel, which is buffered, so the step worker never blocks on a
// caller that has gone away. A caller whose delivery fails must call
// CancelInstallVerdict.
func (n *Node) AwaitInstallVerdict(index uint64) <-chan InstallVerdict {
	ch := make(chan InstallVerdict, 1)
	n.installMu.Lock()
	defer n.installMu.Unlock()
	// A node that is stopped or failed steps nothing more, so no verdict can
	// come; answer now rather than register a wait nothing will end. Both
	// states are set before their abandon takes installMu, so this check
	// cannot miss one that the abandon has already run for.
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
