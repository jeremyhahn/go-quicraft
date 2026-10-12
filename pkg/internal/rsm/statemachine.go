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

// Package rsm provides the managed state machine wrapper that bridges
// user-implemented state machines to the QuicRaft engine. It handles
// session-based deduplication, entry conversion, and snapshot coordination.
package rsm

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math"
	"sync"
	"sync/atomic"

	"github.com/jeremyhahn/go-quicraft/pkg/internal/session"
	"github.com/jeremyhahn/go-quicraft/pkg/proto"
	"github.com/jeremyhahn/go-quicraft/pkg/sm"
)

// lookupBufPool is a pool of []byte buffers used on the NALookupBuf path
// to avoid per-call heap allocation when the underlying SM implements
// sm.NALookupInto. Buffers are pooled as *[]byte so that the pool entry
// itself does not escape to the heap on Get/Put.
//
// Initial capacity of 256 bytes covers the common case for small KV values.
// The underlying SM grows the buffer via append when the value is larger.
var lookupBufPool = sync.Pool{
	New: func() interface{} {
		buf := make([]byte, 0, 256)
		return &buf
	},
}

// SMType identifies the type of state machine being managed.
// It is exported so that the quicraft host package can read the
// canonical type code from a constructed StateMachine without
// maintaining a duplicate set of constants.
type SMType uint8

const (
	// SMTypeRegular is the standard single-threaded state machine.
	SMTypeRegular SMType = iota
	// SMTypeConcurrent is a state machine that supports concurrent reads.
	SMTypeConcurrent
	// SMTypeOnDisk is a state machine backed by persistent storage.
	SMTypeOnDisk
)

// String returns a human-readable name for the state machine type.
func (t SMType) String() string {
	switch t {
	case SMTypeRegular:
		return "regular"
	case SMTypeConcurrent:
		return "concurrent"
	case SMTypeOnDisk:
		return "on-disk"
	default:
		return fmt.Sprintf("unknown(%d)", t)
	}
}

// TypeCode returns the numeric type code as uint64 for serialization
// into logdb.Bootstrap.Type and ShardInfo.StateMachineType.
func (t SMType) TypeCode() uint64 {
	return uint64(t)
}

// defaultMaxExpirePerCheckpoint is the maximum number of sessions to expire
// at each deterministic expiry checkpoint (see Apply). Capping the count
// amortizes the O(sessions) expiry scan. The cap is keyed to a fixed index
// RANGE (one checkpoint per inactivity window), not to the number of Apply
// calls, so the same committed-entry sequence expires the same sessions at the
// same log indices regardless of how a replica batches applies.
const defaultMaxExpirePerCheckpoint = 64

// maxExpirePerCheckpoint returns the per-checkpoint reclaim cap, scaled to the
// shard's REPLICATED table capacity rather than fixed at 64.
//
// A fixed 64 makes the worst-case recovery time depend on MaxTotalSessions: at
// the defaults (16384 sessions, 100000-entry expiry window) a full table needs
// 16384/64 = 256 windows to drain, i.e. 25.6 MILLION applied entries during
// which every new GetNewSession on the shard fails. A crashed client fleet —
// a rolling deploy of a service that does not close its sessions — is enough
// to reach that state, and the recovery constant is three orders of magnitude
// larger than the failure that caused it.
//
// Scaling to MaxTotalSessions/8 bounds the drain at 8 windows for ANY
// configured capacity while keeping the amortised scan small. The value is
// derived from the REPLICATED policy, so every replica computes the same cap
// from the same committed state and expiry stays deterministic; ExpireSessions
// already sorts candidates before truncating to the cap, so which sessions are
// selected is unchanged in kind.
func (s *StateMachine) maxExpirePerCheckpoint() int {
	n := s.sessionPolicy.MaxTotalSessions / 8
	if n < defaultMaxExpirePerCheckpoint {
		return defaultMaxExpirePerCheckpoint
	}
	if n > math.MaxInt32 {
		n = math.MaxInt32
	}
	return int(n)
}

// StateMachine wraps a user-implemented state machine with session
// deduplication, entry conversion, and snapshot coordination. The
// type switch on the underlying SM happens once at construction time;
// all subsequent calls go through direct method calls on the concrete
// wrapper type.
type StateMachine struct {
	sm          smWrapper
	smType      SMType
	sessions    *session.Manager
	snapshotDir string
	lastApplied atomic.Uint64
	shardID     uint64
	replicaID   uint64

	// sessionPolicy is the session configuration the apply path is CURRENTLY
	// evaluating: the expiry threshold and the two session-manager limits.
	// It is the shard's replicated policy whenever one is known, and
	// localPolicy otherwise; refreshSessionPolicy maintains that.
	//
	// It is read and written only from the apply goroutine (Apply) and from
	// snapshot recovery, which the engine quiesces applies around, so it
	// needs no synchronization of its own.
	sessionPolicy proto.SessionPolicy

	// localPolicy is this node's own configuration, used only while the shard
	// has no replicated policy: a shard bootstrapped before the policy became
	// replicated state, or a join=true replica that has not yet received the
	// shard's configuration. It is never preferred over a replicated value —
	// that preference is exactly the divergence the replication closes.
	localPolicy proto.SessionPolicy

	// sessionPolicyFn returns the shard's replicated session policy, or a
	// policy reporting IsSet false when none is known yet. It is
	// raft.Peer.SessionPolicy in production and nil in tests that drive the
	// state machine directly.
	//
	// It is consulted at the START of each Apply rather than at construction
	// because a replica can learn its configuration after its state machine
	// exists (a joiner replicating the shard's EntryInitialMembership, or
	// adopting an InstallSnapshot), and it must converge without a restart.
	//
	// DETERMINISM CONTRACT. This is safe only because the replicated policy is
	// BOOTSTRAP-ONLY: nothing changes it after the shard's initial membership
	// commits, so the value this observes is the same on every replica no
	// matter how far the apply path trails commit. If a config change that
	// alters the policy is ever added, this function must be replaced by
	// adoption from the apply stream itself — decoding the policy out of the
	// committed entry that carries it, at the exact log index where it applies
	// — because commit-time adoption plus a lagging apply path would otherwise
	// let two replicas switch policies at different points in the log. That is
	// the same class of bug as batching-dependent expiry, and it would be
	// just as silent.
	sessionPolicyFn func() proto.SessionPolicy

	// zeroCopyEntryCmd controls whether Entry.Cmd aliases the internal
	// buffer (zero-copy, zero-alloc) or is copied for the state machine.
	// When true, the sm.Entry.Cmd passed to Update() aliases the proto
	// entry's Cmd slice. The state machine MUST copy Cmd if retaining
	// it beyond Update(). When false (default), a safe owned copy is
	// made for each entry. This field mirrors Config.ZeroCopyEntryCmd.
	zeroCopyEntryCmd bool

	// logger receives the session-policy disagreement report
	// (logPolicyDisagreement); nil means the process slog default. Set once,
	// at construction, through WithLogger.
	logger *slog.Logger

	// Pre-allocated buffers for Apply(). Reused via [:0] reset to avoid
	// per-call heap allocations. Safe because Apply() is called from a
	// single apply goroutine per shard and the buffers are fully consumed
	// before Apply() returns.
	smEntryBuf  []sm.Entry
	smResultBuf []sm.Result
	indexMapBuf []int

	// readGuard excludes Close from everything that calls into the underlying
	// state machine, and — for the basic tier only — excludes reads
	// (Lookup/NALookup/NALookupBuf/GetHash) from Apply. Readers hold RLock for
	// the duration of the call into the underlying state machine — and no
	// longer, so no caller-held resource can pin it — which is what keeps the
	// SM from being closed mid-lookup. Close takes the write lock.
	// A pooled NALookupBuf buffer outlives the guard: its contents are a COPY
	// (see the sm.NALookupInto contract), so it stays valid after Close.
	//
	// How Apply holds it depends on the tier, because the tiers publish
	// opposite contracts (lockApply):
	//
	//   - sm.StateMachine (SMTypeRegular): Apply takes the WRITE lock. That
	//     tier promises "QuicRaft serializes all method calls -- Update and
	//     Lookup are never called concurrently", and the promise is the whole
	//     point of the tier. The engine enforces the same premise for
	//     snapshots: the snapshot pool sets the saving gate and drains applies
	//     before a regular SM's whole-state save (see
	//     internal/engine/snapshot_pool.go and rsm/adapter.go).
	//
	//   - sm.ConcurrentStateMachine and sm.DiskStateMachine: Apply takes the
	//     READ lock. Both document Lookup as "May be called concurrently with
	//     Update ... responsible for its own internal synchronization", so
	//     excluding reads here would serialise a state machine that was built
	//     not to be: one long read (a range scan, an export) would block the
	//     shard's Apply, and sync.RWMutex would then park every new reader
	//     behind that waiting writer, so a single slow read would stall reads
	//     and writes for the whole shard. The read lock still excludes
	//     Close.
	//
	// Cost on the basic tier: a local read parks behind an in-flight Apply,
	// which during follower catch-up can carry up to maxEntriesToApplySize of
	// entries, and sync.RWMutex has no context-aware acquire, so a deadline is
	// not observed while blocked here. An implementation that needs
	// finer-grained concurrency should implement sm.ConcurrentStateMachine,
	// which is exactly the choice the tiers exist to offer.
	readGuard sync.RWMutex

	// applyMu serializes everything that owns the apply stream's state: Apply,
	// the capture phase of SaveSnapshotGated, and RecoverFromSnapshot. That
	// state is the session manager (not safe for concurrent use), the session
	// policy and the expiry checkpoint walk.
	//
	// readGuard cannot carry this exclusion: Apply is a READER on the
	// concurrent and on-disk tiers, and two readGuard readers do not exclude
	// each other, so the role lives here — the same split streamGuard makes
	// for Close. The engine's
	// apply gate quiesces applies around both snapshot paths as well; this is
	// what keeps a misordered caller from racing the session table instead of
	// relying on that gate alone.
	//
	// Readers never take it, so it costs the read path nothing, and on the
	// apply path it is uncontended except against a snapshot capture or
	// recovery, which exclude Apply by design.
	//
	// LOCK ORDER: readGuard, then applyMu, then streamGuard.
	applyMu sync.Mutex

	// streamGuard blocks Close for as long as a snapshot is STREAMING, without
	// blocking Apply.
	//
	// It exists because readGuard cannot carry both meanings. Before Apply held
	// the write lock, SaveSnapshotGated's readGuard.RLock did two jobs at once:
	// it blocked Close (a writer) while leaving Apply (then also a reader) free
	// to resume the moment afterCapture released the engine's apply gate. That
	// is the entire point of the gated save — concurrentSMWrapper captures a
	// point-in-time, fires afterCapture, and streams the immutable snapshot
	// alongside resumed applies. Once Apply became a writer the two jobs
	// collapsed into one: applies queued behind the stream for its whole
	// duration, and Go's RWMutex then parks new readers behind the waiting
	// writer, so local reads stalled too. On a large on-disk snapshot that is a
	// shard-wide pause precisely where the two-phase split was meant to avoid
	// one.
	//
	// So the Close-blocking half moves here. The stream holds this for reading
	// from capture until the last byte; Close takes it for writing after
	// readGuard, and therefore still waits out an in-flight save.
	//
	// LOCK ORDER: readGuard, then applyMu, then streamGuard, everywhere,
	// without exception.
	streamGuard sync.RWMutex
	// closed is set under readGuard.Lock by Close so that any reader which was
	// blocked acquiring RLock observes the closed state and returns ErrClosed
	// rather than touching a closed SM (in-memory backends do not self-track
	// closure and would otherwise return stale data post-Close).
	closed bool
}

// NewStateMachine creates a managed state machine wrapper using the
// given factory function. The factory must return one of StateMachine,
// ConcurrentStateMachine, or DiskStateMachine. Returns ErrInvalidSMType
// if the returned type does not match any known interface.
//
// The session policy governs apply: the expiry threshold segments the apply
// path's checkpoint walk, and the two limits decide which registrations and
// cached responses are admitted. All three must therefore be identical on
// every replica, which is why the shard REPLICATES them (proto.SessionPolicy)
// and why the two arguments here are asymmetric:
//
//   - local is this node's own configuration. It is used only while the shard
//     has no replicated policy of its own.
//   - replicated returns the shard's committed policy and always wins over
//     local. It may be nil, which pins the state machine to local — the shape
//     tests want, and never what a live shard wants.
//
// Passing the policy as one value also removes a hazard the previous signature
// had: it took the two limits as adjacent uint64 parameters, and transposing
// them compiled cleanly while silently capping a shard at MaxSessionsPerClient
// clients and letting one client pin MaxTotalSessions cached results.
func NewStateMachine(
	shardID, replicaID uint64,
	createFn sm.CreateFunc,
	snapshotDir string,
	local proto.SessionPolicy,
	replicated func() proto.SessionPolicy,
	zeroCopyEntryCmd bool,
	opts ...Option,
) (*StateMachine, error) {
	raw := createFn(shardID, replicaID)
	wrapper, st, err := wrapStateMachine(raw)
	if err != nil {
		return nil, err
	}
	s := &StateMachine{
		sm:               wrapper,
		smType:           st,
		snapshotDir:      snapshotDir,
		shardID:          shardID,
		replicaID:        replicaID,
		localPolicy:      local,
		sessionPolicy:    local,
		sessionPolicyFn:  replicated,
		zeroCopyEntryCmd: zeroCopyEntryCmd,
	}
	for _, opt := range opts {
		opt(s)
	}
	// Resolve before the manager exists so a shard that already knows its
	// replicated policy — every restart, and every bootstrap — never builds a
	// session manager with limits it is about to replace.
	s.sessionPolicy = s.effectiveSessionPolicy()
	s.sessions = session.NewManager(
		s.sessionPolicy.MaxTotalSessions, s.sessionPolicy.MaxSessionsPerClient,
		s.sessionPolicy.MaxSessionsPerOwner)
	s.logPolicyDisagreement(local, s.sessionPolicy)
	return s, nil
}

// Option configures a StateMachine at construction.
type Option func(*StateMachine)

// WithLogger sets the logger the StateMachine reports on; nil, or no
// WithLogger at all, means the process slog default.
func WithLogger(l *slog.Logger) Option {
	return func(s *StateMachine) {
		s.logger = l
	}
}

// effectiveSessionPolicy returns the policy the apply path must use: the
// shard's replicated policy when one is known, this node's configuration
// otherwise.
//
// The replicated value wins even when it is the LOOSER of the two. That is not
// a preference, it is the only choice that terminates: the alternative — a
// replica that enforces its own stricter limit — is a replica that refuses
// registrations its peers admit, and from then on answers ResultSessionExpired
// for entries the rest of the shard applies normally. A local limit cannot be
// honoured by one replica alone; it can only be proposed, at bootstrap.
func (s *StateMachine) effectiveSessionPolicy() proto.SessionPolicy {
	if s.sessionPolicyFn == nil {
		return s.localPolicy
	}
	if replicated := s.sessionPolicyFn(); replicated.IsSet() {
		return replicated
	}
	return s.localPolicy
}

// refreshSessionPolicy adopts the shard's replicated session policy if it has
// changed since the last call.
//
// It runs on the apply goroutine (and during snapshot recovery, which the
// engine quiesces applies around), so it may mutate the session manager's
// limits directly. In steady state the shard's policy never changes, so this
// is one atomic load and a three-field comparison per Apply batch.
func (s *StateMachine) refreshSessionPolicy() {
	effective := s.effectiveSessionPolicy()
	if effective == s.sessionPolicy {
		return
	}
	previous := s.sessionPolicy
	s.sessionPolicy = effective
	s.sessions.SetLimits(effective.MaxTotalSessions, effective.MaxSessionsPerClient,
		effective.MaxSessionsPerOwner)
	s.logPolicyDisagreement(previous, effective)
}

// logPolicyDisagreement records, loudly, that the policy the apply path uses is
// not the one this node is configured with.
//
// The shard's value governs and there is no way for one replica to opt out, so
// this is a warning and not an error — but it is never silent. MaxTotalSessions
// and MaxSessionsPerClient bound this process's memory, so a replicated value
// above the local one is an operator-visible resource decision that was taken
// elsewhere (at bootstrap, by the founders) and must be discoverable from this
// node's logs rather than only by reading the shard's membership.
func (s *StateMachine) logPolicyDisagreement(previous, effective proto.SessionPolicy) {
	if effective == s.localPolicy {
		return
	}
	logger := s.logger
	if logger == nil {
		logger = slog.Default()
	}
	logger.Warn("rsm: applying the shard's replicated session policy, which differs "+
		"from this node's configuration; the replicated value governs and cannot "+
		"be overridden locally",
		"shardID", s.shardID,
		"replicaID", s.replicaID,
		"previous_expiry_entries", previous.ExpiryEntries,
		"expiry_entries", effective.ExpiryEntries,
		"local_expiry_entries", s.localPolicy.ExpiryEntries,
		"max_total_sessions", effective.MaxTotalSessions,
		"local_max_total_sessions", s.localPolicy.MaxTotalSessions,
		"max_sessions_per_client", effective.MaxSessionsPerClient,
		"local_max_sessions_per_client", s.localPolicy.MaxSessionsPerClient,
	)
}

// wrapStateMachine performs the one-time type switch to determine the
// concrete wrapper type for the user's state machine. The check order
// is most-specific to least-specific: DiskStateMachine is checked
// first because it is a superset of ConcurrentStateMachine's methods
// and would otherwise match the concurrent case.
func wrapStateMachine(raw interface{}) (smWrapper, SMType, error) {
	switch typed := raw.(type) {
	case sm.DiskStateMachine:
		return &onDiskSMWrapper{sm: typed}, SMTypeOnDisk, nil
	case sm.ConcurrentStateMachine:
		return &concurrentSMWrapper{sm: typed}, SMTypeConcurrent, nil
	case sm.StateMachine:
		return &regularSMWrapper{sm: typed}, SMTypeRegular, nil
	default:
		return nil, 0, ErrInvalidSMType
	}
}

// Open initializes the underlying state machine. For on-disk state machines,
// this opens the persistent storage and returns the last applied index for
// crash recovery. For in-memory state machines, this is a no-op returning (0, nil).
func (s *StateMachine) Open(stopper <-chan struct{}) (uint64, error) {
	lastApplied, err := s.sm.Open(context.Background(), s.snapshotDir, stopper)
	if err != nil {
		return 0, err
	}
	if lastApplied > 0 {
		s.lastApplied.Store(lastApplied)
	}
	return lastApplied, nil
}

// IsOnDisk returns true if the underlying state machine is a DiskStateMachine.
func (s *StateMachine) IsOnDisk() bool {
	return s.smType == SMTypeOnDisk
}

// lockApply acquires the guards Apply runs under: readGuard exclusively for a
// basic-tier state machine and shared for the concurrent and on-disk tiers,
// then applyMu. smType is fixed at construction, so lockApply and unlockApply
// always agree on the mode.
func (s *StateMachine) lockApply() {
	if s.smType == SMTypeRegular {
		s.readGuard.Lock()
	} else {
		s.readGuard.RLock()
	}
	s.applyMu.Lock()
}

// unlockApply releases what lockApply acquired, in reverse order.
func (s *StateMachine) unlockApply() {
	s.applyMu.Unlock()
	if s.smType == SMTypeRegular {
		s.readGuard.Unlock()
	} else {
		s.readGuard.RUnlock()
	}
}

// Apply processes committed entries through the session lifecycle:
//   - Session register entries (SeriesID == SessionSeriesIDForRegister)
//     register the client with the session manager.
//   - Session unregister entries (SeriesID == SessionSeriesIDForUnregister)
//     remove the client from the session manager.
//   - Normal session entries are deduplicated and forwarded to the user SM.
//   - Non-session entries (clientID == 0) go directly to the user SM.
//
// The results slice must be pre-allocated with len(results) == len(entries).
//
// DETERMINISM CONTRACT (session expiry). The state produced by Apply — the
// user SM state AND the session table — must be a pure function of the
// committed entry SEQUENCE, never of how a replica happens to group entries
// into Apply calls (apply batching is replica-local: it varies with message
// timing, restart replay, and the recovery skip-prefix filter). Session expiry
// is the subtle case: evaluating it at the batch's last index would let two
// replicas that batch differently expire a session at different points and
// diverge permanently while every Raft invariant still holds. Apply therefore
// evaluates expiry only at fixed log-index checkpoints — multiples of the
// inactivity threshold, so expiry is checked once per inactivity window —
// splitting the incoming entries at those checkpoints so each expiry runs
// against the session state as of exactly the entries at or below the
// checkpoint index. When expiry is disabled the whole batch is applied in one
// shot with no segmentation overhead.
//
// The threshold comes from the shard's REPLICATED session policy
// (proto.SessionPolicy), refreshed at the top of this function, and so does
// every other limit the session lifecycle below enforces. That is what makes
// the checkpoint set and the resulting expiry sequence identical on every
// replica: a threshold taken from local configuration would be a second,
// invisible input to apply, and two replicas configured differently would
// expire different sessions at different indexes with nothing in the protocol
// able to notice.
func (s *StateMachine) Apply(entries []proto.Entry, results []sm.Result) error {
	// lockApply takes readGuard in the mode the tier's contract demands —
	// exclusive on the basic tier, so Apply excludes Lookup/NALookup/
	// NALookupBuf/GetHash as sm.StateMachine promises; shared otherwise — and
	// then applyMu. See the readGuard and applyMu field comments.
	//
	// Either mode also subsumes the original defence-in-depth role: the
	// engine's synchronous UnloadNode guarantees no Apply is in flight when
	// Close runs, but a misordered caller must get a typed error instead of
	// running user code on a closed SM, and holding the guard forces Close to
	// wait out an in-flight Apply rather than closing the SM under it.
	s.lockApply()
	defer s.unlockApply()
	if s.closed {
		return ErrClosed
	}
	if len(entries) == 0 {
		return nil
	}

	// The batch must not start ABOVE lastApplied+1. lastApplied is not
	// bookkeeping here: it is the lower bound of the expiry checkpoint walk
	// below, so a batch that leaves a hole beneath it makes this replica
	// evaluate expiry over a different index range than the replicas that
	// applied the same committed log contiguously — a divergence that leaves
	// every raft invariant intact and would otherwise be silent.
	//
	// A hole has two causes and both are fatal to this state machine:
	// committed entries were dropped before reaching it, or lastApplied was
	// never seeded from the index the shard RESUMED at (Host.StartShard seeds
	// it beside engine Node.SeedLastApplied, and the runtime InstallSnapshot
	// path seeds it via SetLastApplied). Failing loudly is the point — the
	// alternative is a replica that quietly answers ResultSessionExpired for
	// entries its peers apply normally.
	//
	// This is the same assertion the engine's apply worker makes one layer
	// down against its own counter (internal/engine, ErrApplyGap), stated as
	// an upper bound rather than an equality because the two layers see
	// different streams: the worker filters entries at or below its marker
	// before checking, whereas an already-applied entry that reaches here is
	// the documented on-disk replay case and stays a no-op.
	if prev := s.lastApplied.Load(); entries[0].Index > prev+1 {
		slog.Error("rsm: committed entry gap before apply",
			"shardID", s.shardID,
			"replicaID", s.replicaID,
			"last_applied", prev,
			"batch_first_index", entries[0].Index,
		)
		return ErrApplyGap
	}

	// Adopt the shard's replicated policy before a single entry is classified,
	// so the limits this batch is evaluated against are the shard's and not
	// this node's. See sessionPolicyFn for why it is read here rather than
	// captured once at construction.
	s.refreshSessionPolicy()

	lastIndex := entries[len(entries)-1].Index

	// Fast path: expiry disabled — apply the whole batch in one shot. This
	// keeps the hot path free of any segmentation overhead.
	if s.sessionPolicy.ExpiryEntries == 0 {
		if err := s.applyBatch(entries, results); err != nil {
			return err
		}
		s.advanceLastApplied(lastIndex)
		return nil
	}

	// Expiry enabled: walk the fixed checkpoints (multiples of the inactivity
	// threshold) that fall in (prevIndex, lastIndex]. prevIndex is the last index applied
	// before this call, so the checkpoint set continues seamlessly across Apply
	// calls and across restart — but ONLY because lastApplied is explicitly
	// SEEDED to the resumed index by the two paths that resume a state machine
	// (Host.StartShard and the engine's InstallSnapshot recovery, both via
	// SetLastApplied). RecoverFromSnapshot does NOT restore it: the session
	// table is in the snapshot stream, this counter is not. A replica that
	// resumed with it left at 0 would re-walk every checkpoint its peers
	// already walked, and ExpireSessions is capped per call, so the re-walk
	// deletes sessions the shard still holds. The contiguity guard at the top
	// of Apply is what makes that unseeded state fail instead of diverge. For
	// each checkpoint we first flush every not-yet-applied entry at or below
	// the checkpoint index, then run expiry — so expiry always observes a
	// consistent, fully-applied session state as of that exact index.
	interval := s.sessionPolicy.ExpiryEntries
	prevIndex := s.lastApplied.Load()
	firstK := prevIndex/interval + 1
	lastK := lastIndex / interval
	segStart := 0
	i := 0
	for k := firstK; k <= lastK; k++ {
		boundary := k * interval
		for i < len(entries) && entries[i].Index <= boundary {
			i++
		}
		if i > segStart {
			if err := s.applyBatch(entries[segStart:i], results[segStart:i]); err != nil {
				return err
			}
			segStart = i
		}
		s.sessions.ExpireSessions(boundary, interval, s.maxExpirePerCheckpoint())
	}
	// Flush the trailing segment above the last checkpoint (indices in
	// (lastK*interval, lastIndex]); its checkpoint is reached in a future Apply.
	if segStart < len(entries) {
		if err := s.applyBatch(entries[segStart:], results[segStart:]); err != nil {
			return err
		}
	}
	s.advanceLastApplied(lastIndex)
	return nil
}

// advanceLastApplied stores idx as the new last-applied index when it advances
// the counter. The atomic store guards against a concurrent LastApplied() read
// from the snapshot pool worker goroutine.
func (s *StateMachine) advanceLastApplied(idx uint64) {
	if idx > s.lastApplied.Load() {
		s.lastApplied.Store(idx)
	}
}

// applyBatch processes one contiguous run of committed entries through the
// session lifecycle and the user state machine. It is the body of Apply,
// invoked once per expiry-checkpoint segment (and exactly once on the
// expiry-disabled fast path). It does NOT evaluate session expiry and does NOT
// advance lastApplied — Apply owns both so their evaluation stays a pure
// function of the committed log rather than of segment boundaries. The caller
// holds what lockApply acquires (readGuard in its tier's mode, then applyMu);
// results must be pre-allocated with
// len(results) == len(entries).
//
//nolint:gocyclo // the classification loop is not a per-entry function: it reads and mutates six pieces of BATCH-scoped state (admittedThisBatch, batchSeen, mirrors, sessionLifecycleAt and the three reused smEntries/smResults/indexMap buffers), and four of those are keyed by the entry's POSITION in the batch, which the second loop then resolves against. Extracting the body would have to thread all of it through the helper or hoist it into a struct — relocating the state rather than reducing it
func (s *StateMachine) applyBatch(entries []proto.Entry, results []sm.Result) error {
	// Build the filtered entry list and result mapping.
	// We convert proto.Entry to sm.Entry for the user SM, skipping
	// entries that are detected as duplicates by the session manager,
	// and handling session register/unregister operations inline.
	// Buffers are reused across calls via [:0] reset, growing on demand.
	smEntries := s.smEntryBuf[:0]
	smResults := s.smResultBuf[:0]
	indexMap := s.indexMapBuf[:0]

	// admittedThisBatch counts, per client, the new session-managed series
	// admitted earlier in this batch. RecordResult runs only after the
	// batched user-SM Update, so the capacity gate below must account for
	// peers already queued in the same batch to avoid a batch of new series
	// collectively overflowing the response cache. nil until first use to
	// avoid an allocation for batches with no session-managed entries.
	var admittedThisBatch map[uint64]uint64

	// batchSeen maps (clientID, seriesID) to the entry index of the FIRST
	// occurrence of that proposal within this batch. A client may retry a
	// request whose original proposal has not yet committed; if both the
	// original and the retry commit and land in the same apply batch, both
	// carry the same (clientID, seriesID) and neither is yet in the session
	// table — so CheckDuplicate reports both as new. Applying both would run
	// the user operation twice, violating at-most-once delivery. batchSeen
	// lets us detect the second (and later) occurrence and mirror the first
	// occurrence's result without re-applying. nil until first use, local to
	// this call, never leaked across Apply invocations.
	var batchSeen map[uint64]map[uint64]int
	// mirrors maps an in-batch duplicate's entry index to the entry index of
	// the first occurrence whose result it must copy. Resolved after the
	// user-SM Update populates results for the first occurrences.
	var mirrors map[int]int

	// sessionLifecycleAt maps a clientID to the entry index of the most
	// recent session lifecycle transition (successful Register or Unregister)
	// in this batch. RecordResult runs only after the batched user-SM Update,
	// so an entry admitted earlier in the batch may reach it after the session
	// it was admitted against is gone — replaced or removed by a later entry
	// in the SAME batch. An entry whose clientID has a transition at a HIGHER
	// entry index than its own must not record a response: for a closed
	// session the cache is moot, and for a re-registered clientID caching into
	// the replacement session would deduplicate a future proposal of that
	// series which the replacement session never applied. The absent-key zero
	// value is a valid "no transition" marker: index 0 can only ever precede
	// the entries it would invalidate. nil until the first transition.
	var sessionLifecycleAt map[uint64]int

	for i := range entries {
		entry := &entries[i]

		// Skip config change entries. These are Raft membership change proposals
		// (AddNode, RemoveNode, AddNonVoting, AddWitness) committed by the Raft
		// layer. They must NOT be forwarded to the user state machine — the
		// membership change is applied by processConfigChanges in peer.go before
		// entries reach here. Forwarding config change Cmd to the user SM would
		// cause it to decode the config change wire format as an application op
		// and return an UnknownOpError.
		//
		// The bootstrap EntryInitialMembership at index 1 is skipped for the same
		// reason: its Cmd is a marshaled Membership, not a user command, and the
		// membership it carries is adopted in processConfigChanges before entries
		// reach here.
		if entry.IsConfigChange() || entry.IsInitialMembership() {
			results[i] = sm.Result{}
			continue
		}

		// Skip the leader's no-op entry (and any other empty-command,
		// non-session entry). Raft commits an empty EntryNormal on election to
		// advance the commit index; it carries no user command and must NOT be
		// forwarded to the user state machine (which would otherwise receive
		// an empty Cmd and could panic or miscount). lastApplied still advances
		// because the engine tracks the batch's last index regardless.
		if !entry.IsSessionManaged() && !entry.IsNewSessionRequest() &&
			!entry.IsEndOfSessionRequest() && len(entry.Cmd) == 0 {
			results[i] = sm.Result{}
			continue
		}

		// Handle session registration: register the client and return
		// the clientID as the result Value. Skip user SM.
		if entry.IsNewSessionRequest() {
			// entry.OwnerID is read straight from the replicated entry, which is
			// what makes the per-owner cap a state-machine decision: the same
			// log produces the same accept/reject on every replica and on every
			// replay after a restart. Nothing about the connection that carried
			// the proposal is consulted, because none of it is in the log.
			if err := s.sessions.Register(entry.ClientID, entry.OwnerID, entry.Index); err != nil {
				// Already registered, over the shard-wide session limit, or over
				// the owner's concurrent-session cap — none is fatal. Return an
				// empty result and leave the session table untouched; the host
				// layer turns the mismatched result Value into a failed
				// registration for the caller.
				results[i] = sm.Result{}
			} else {
				if sessionLifecycleAt == nil {
					sessionLifecycleAt = make(map[uint64]int)
				}
				sessionLifecycleAt[entry.ClientID] = i
				// A fresh session incarnation for this clientID starts its
				// (clientID, seriesID) numbering over. Discard any in-batch
				// dedup bookkeeping from a PRIOR incarnation that was
				// unregistered/re-registered earlier in this same batch:
				// otherwise the new session's first proposal could collide
				// with a stale batchSeen entry (mirrored to the prior
				// incarnation's result instead of applied) or be spuriously
				// rejected by a stale admittedThisBatch count. delete on a nil
				// map is a safe no-op.
				delete(batchSeen, entry.ClientID)
				delete(admittedThisBatch, entry.ClientID)
				results[i] = sm.Result{Value: entry.ClientID}
			}
			continue
		}

		// Handle session unregistration: remove the client and return
		// the clientID as the result Value. Skip user SM.
		if entry.IsEndOfSessionRequest() {
			if err := s.sessions.Unregister(entry.ClientID); err != nil {
				// Session not found — signal the same way as session-managed
				// entries with an unregistered session so the host layer
				// translates this to ErrSessionExpired → ErrRejected.
				results[i] = sm.Result{Value: sm.ResultSessionExpired}
			} else {
				if sessionLifecycleAt == nil {
					sessionLifecycleAt = make(map[uint64]int)
				}
				sessionLifecycleAt[entry.ClientID] = i
				// The session is gone; its in-batch dedup bookkeeping must not
				// survive to bind a later re-registration of the same clientID
				// in this batch. delete on a nil map is a safe no-op.
				delete(batchSeen, entry.ClientID)
				delete(admittedThisBatch, entry.ClientID)
				results[i] = sm.Result{Value: entry.ClientID}
			}
			continue
		}

		// Check for session-based deduplication on normal entries.
		if entry.IsSessionManaged() {
			cached, isDup, err := s.sessions.CheckDuplicate(entry.ClientID, entry.SeriesID, entry.Index)
			if err != nil {
				// ErrSessionNotFound: session expired or not restored from snapshot.
				// ErrResponseEvicted: seriesID already completed, cached response evicted.
				// Both conditions map to SESSION_EXPIRED per Raft PhD Figure 6.1, step 3:
				// "Reply SESSION_EXPIRED if no record of clientId or if response
				// for client's sequenceNum already discarded."
				slog.Warn("rsm: session error, returning SESSION_EXPIRED",
					"shardID", s.shardID,
					"replicaID", s.replicaID,
					"clientID", entry.ClientID,
					"seriesID", entry.SeriesID,
					"index", entry.Index,
					"error", err,
				)
				results[i] = sm.Result{Value: sm.ResultSessionExpired}
				continue
			}
			if isDup {
				// Return an OWNED copy of the cached response Data. The cache
				// outlives this Apply and is serialized into snapshots via
				// MarshalTo; handing the client the cache's backing array would
				// let application mutation of the returned Data corrupt the
				// replica-local dedup cache (and the snapshot). Value is a
				// scalar and needs no copy.
				if len(cached.Data) > 0 {
					owned := make([]byte, len(cached.Data))
					copy(owned, cached.Data)
					cached.Data = owned
				}
				results[i] = cached
				continue
			}
			// In-batch duplicate detection. This MUST run before the CanRecord
			// capacity gate and before admittedThisBatch is incremented: a retry
			// that lands in the same batch as its original consumes no new cache
			// slot (the first occurrence already accounts for it), so gating it
			// could spuriously reject it as "cache full" and, worse, applying it
			// would mutate the user SM a second time. Record this entry as a
			// mirror of the first occurrence and skip the user-SM path entirely.
			if seriesMap, ok := batchSeen[entry.ClientID]; ok {
				if firstIdx, seen := seriesMap[entry.SeriesID]; seen {
					if mirrors == nil {
						mirrors = make(map[int]int)
					}
					mirrors[i] = firstIdx
					continue
				}
			}
			// Capacity gate: reject a new series that could not be cached
			// BEFORE applying it to the user SM. If the response cache is
			// already full (a misbehaving client not advancing respondedTo),
			// applying the entry without being able to cache its result
			// would break at-most-once: a later replay would find no cached
			// response and re-apply the mutation. Rejecting it up front
			// (treated as SESSION_EXPIRED, same as an evicted response per
			// Raft PhD Figure 6.1 step 3) keeps the entry from ever mutating
			// the state machine. The check is deterministic across replicas
			// (index/series driven, no wall clock).
			if err := s.sessions.CanRecord(
				entry.ClientID, entry.RespondedTo,
				admittedThisBatch[entry.ClientID]); err != nil {
				slog.Warn("rsm: session response cache full, returning SESSION_EXPIRED",
					"shardID", s.shardID,
					"replicaID", s.replicaID,
					"clientID", entry.ClientID,
					"seriesID", entry.SeriesID,
					"index", entry.Index,
					"error", err,
				)
				results[i] = sm.Result{Value: sm.ResultSessionExpired}
				continue
			}
			if admittedThisBatch == nil {
				admittedThisBatch = make(map[uint64]uint64)
			}
			admittedThisBatch[entry.ClientID]++
			// Record this as the first occurrence of (clientID, seriesID) in the
			// batch so a later retry in the same batch is detected as an in-batch
			// duplicate and mirrors this entry's result.
			if batchSeen == nil {
				batchSeen = make(map[uint64]map[uint64]int)
			}
			seriesMap := batchSeen[entry.ClientID]
			if seriesMap == nil {
				seriesMap = make(map[uint64]int)
				batchSeen[entry.ClientID] = seriesMap
			}
			seriesMap[entry.SeriesID] = i
		}

		// Non-session or new session proposal — forward to user SM.
		// When zeroCopyEntryCmd is false (default), make a safe owned
		// copy of the command payload so the state machine can retain
		// it beyond the Update() call. When true, the SM receives a
		// direct alias into the internal buffer (zero-copy, zero-alloc)
		// and MUST copy Cmd if retaining it.
		cmd := entry.Cmd
		if !s.zeroCopyEntryCmd && len(cmd) > 0 {
			owned := make([]byte, len(cmd))
			copy(owned, cmd)
			cmd = owned
		}
		smEntries = append(smEntries, sm.Entry{
			Index: entry.Index,
			Cmd:   cmd,
		})
		smResults = append(smResults, sm.Result{})
		indexMap = append(indexMap, i)
	}

	// Apply non-duplicate entries to the user SM if any exist.
	if len(smEntries) > 0 {
		ctx := context.Background()
		if err := s.sm.Update(ctx, smEntries, smResults); err != nil {
			return err
		}

		// Copy results back and record in session cache.
		for j, origIdx := range indexMap {
			results[origIdx] = smResults[j]
			entry := &entries[origIdx]

			if entry.IsSessionManaged() {
				// The session this entry was admitted against was closed or
				// replaced by a later entry in this same batch. The entry was
				// legally applied (it precedes the transition in the log), so
				// its result stands, but it must not be cached: see
				// sessionLifecycleAt.
				if sessionLifecycleAt[entry.ClientID] > origIdx {
					continue
				}
				cached := smResults[j]
				// Deep-copy the Result Data before caching it. Two independent
				// aliasing hazards make this mandatory regardless of
				// zeroCopyEntryCmd:
				//   1. The SAME smResults[j].Data was just handed to the client
				//      via results[origIdx] above → OnApplied → RequestResult.
				//      Application mutation of that returned buffer would
				//      corrupt the replica-local dedup cache and, via MarshalTo,
				//      the snapshot — a determinism hazard.
				//   2. In zero-copy mode the user SM may have returned Data that
				//      aliases the engine's pooled CommittedEntries buffer, which
				//      the engine recycles after Apply returns.
				// The client keeps the original buffer (results[origIdx] is left
				// untouched); only the cached copy is owned.
				if len(cached.Data) > 0 {
					owned := make([]byte, len(cached.Data))
					copy(owned, cached.Data)
					cached.Data = owned
				}
				if err := s.sessions.RecordResult(
					entry.ClientID,
					entry.SeriesID,
					entry.RespondedTo,
					entry.Index,
					cached,
				); err != nil {
					// A missing session is a property of the committed log, not
					// an infrastructure failure: returning it would fail the
					// shard deterministically on every replica. The
					// sessionLifecycleAt check above already covers every
					// in-batch removal path; this is defence in depth against
					// any future path that drops a session mid-batch. The
					// response cache of a session that no longer exists is
					// moot, so dropping the record is safe and deterministic.
					// Any other error is an invariant violation (the CanRecord
					// gate makes ErrResponseLimitExceeded unreachable here) and
					// stays fatal.
					if !errors.Is(err, session.ErrSessionNotFound) {
						return err
					}
					slog.Warn("rsm: session removed within apply batch, response not cached",
						"shardID", s.shardID,
						"replicaID", s.replicaID,
						"clientID", entry.ClientID,
						"seriesID", entry.SeriesID,
						"index", entry.Index,
					)
				}
			}
		}
	}

	// Resolve in-batch duplicates: each mirror copies the result of its
	// first occurrence, which the user-SM Update + RecordResult loop above
	// has now populated in results[]. The first occurrence's RecordResult
	// already cached the (clientID, seriesID) response, so a mirror must NOT
	// call RecordResult again (it would be redundant and could push a
	// genuine new series out of the cache). This preserves at-most-once: the
	// operation was applied exactly once, and every duplicate observes the
	// identical result.
	for mirrorIdx, firstIdx := range mirrors {
		results[mirrorIdx] = results[firstIdx]
	}

	// Retain grown buffers for the next applyBatch() call.
	s.smEntryBuf = smEntries
	s.smResultBuf = smResults
	s.indexMapBuf = indexMap

	return nil
}

// Lookup forwards the query to the user state machine.
//
// On a basic sm.StateMachine the read guard serializes this against Apply,
// which holds the same guard for writing there, so Update and Lookup never
// overlap — the contract its doc comment publishes. Concurrent and on-disk
// state machines are documented as handling their own synchronization, and
// their Apply holds the guard for reading, so this runs alongside Update; on
// those tiers the guard keeps out only Close (see lockApply). The caller does
// not serialize this: Host.SyncRead, FollowerRead, QueryLocalNode and
// StaleRead all reach it on their own goroutine while the apply worker runs.
//
// For a []byte query answered by the sm.NALookup fast path, an absent key is
// reported as an untyped nil interface, so `result == nil` at the call site is
// the test for absence on both paths. A key that exists holding an empty value
// comes back as a non-nil interface wrapping a zero-length slice, and stays
// distinguishable from absence.
func (s *StateMachine) Lookup(ctx context.Context, query interface{}) (interface{}, error) {
	s.readGuard.RLock()
	defer s.readGuard.RUnlock()
	if s.closed {
		return nil, ErrClosed
	}
	// Fast path: try NALookup for []byte queries to avoid boxing.
	if key, ok := query.([]byte); ok {
		result, supported, err := s.sm.NALookup(key)
		if err != nil {
			return nil, err
		}
		if supported {
			// Fold a nil slice into an UNTYPED nil. Returning the nil []byte
			// directly boxes it, and a boxed typed nil is NOT equal to nil:
			// the caller's `result == nil` is false, the value it then reads
			// is a zero-length slice, and an absent key is indistinguishable
			// from a key holding an empty value. Nothing fails loudly — a
			// consumer branching on existence (an index probing whether a
			// claim is held, a DAO choosing insert versus update, a first-boot
			// path deciding whether to seed) simply takes the other branch and
			// carries on.
			//
			// Only a nil slice is folded. A non-nil zero-length slice is boxed
			// as it stands, because that is how an SM says "this key exists and
			// its value is empty". Testing len(result) == 0 instead was
			// rejected for exactly that reason: it erases the distinction this
			// is here to preserve.
			//
			// The generic path below needs no equivalent and is not the same
			// bug: it forwards the user SM's own interface value untouched, so
			// how absence is represented there is that SM's choice. The boxing
			// is the adapter's own, which makes this the one site where the
			// fast path can be made indistinguishable from the generic one —
			// the property an optimisation owes its caller.
			if result == nil {
				return nil, nil
			}
			return result, nil
		}
	}
	return s.sm.Lookup(ctx, query)
}

// NALookup performs a byte-slice lookup if the underlying state machine
// supports it. Returns (nil, false, nil) if not supported.
func (s *StateMachine) NALookup(key []byte) ([]byte, bool, error) {
	s.readGuard.RLock()
	defer s.readGuard.RUnlock()
	if s.closed {
		return nil, false, ErrClosed
	}
	return s.sm.NALookup(key)
}

// NALookupBuf performs a pooled byte-slice lookup when the underlying SM
// supports sm.NALookupInto. It acquires a buffer from lookupBufPool, fills
// it with the value, and returns the populated slice along with a release
// function. The caller MUST call release() when it is done with the returned
// slice, after which the slice MUST NOT be used.
//
// If the underlying SM does not support NALookupInto, NALookupBuf falls
// back to NALookup (standard allocation, nop release). In either case the
// returned release function is safe to call.
//
// Returns (nil, nopRelease, false, nil) when the SM supports neither
// NALookupInto nor NALookup for []byte keys.
func (s *StateMachine) NALookupBuf(key []byte) (result []byte, release func(), supported bool, err error) {
	// The read guard is held for the LOOKUP ONLY — never for the lifetime of
	// the returned buffer. release() is called by APPLICATION code (the buffer
	// is handed out through Host.SyncReadBuf/StaleReadBuf), so a caller that
	// never releases would pin the read lock forever: Close blocks on the write
	// lock, and once Close queues as a writer every subsequent Lookup on the
	// shard queues behind it. Holding it that long is also unnecessary — the
	// sm.NALookupInto contract requires implementations to COPY the value into
	// dst, so the returned slice does not alias SM-owned memory.
	s.readGuard.RLock()
	if s.closed {
		s.readGuard.RUnlock()
		return nil, nopRelease, false, ErrClosed
	}

	// Fast path: try NALookupInto with a pooled buffer.
	poolBuf := lookupBufPool.Get().(*[]byte)
	filled, ok, lookupErr := s.sm.NALookupInto(key, *poolBuf)
	if ok {
		s.readGuard.RUnlock()
		if lookupErr != nil {
			// Return the pool buffer unused on error.
			*poolBuf = (*poolBuf)[:0]
			lookupBufPool.Put(poolBuf)
			return nil, nopRelease, true, lookupErr
		}
		// The SM may have grown the buffer via append; update the pool entry to
		// track the (possibly larger) backing array so it is reused on Put.
		*poolBuf = filled[:cap(filled)]
		// releaseOnce covers the WHOLE release body, Put included. Guarding only
		// part of it lets a double release put the same pointer into the pool
		// twice, after which two goroutines are handed the same buffer and
		// silently overwrite each other's read results.
		var releaseOnce sync.Once
		rel := func() {
			releaseOnce.Do(func() {
				*poolBuf = (*poolBuf)[:0]
				lookupBufPool.Put(poolBuf)
			})
		}
		return filled, rel, true, nil
	}

	// NALookupInto not supported — return the pool buffer unused.
	lookupBufPool.Put(poolBuf)

	// Slow path: fall back to standard NALookup (allocates).
	result2, ok2, err2 := s.sm.NALookup(key)
	s.readGuard.RUnlock()
	if ok2 {
		return result2, nopRelease, true, err2
	}
	return nil, nopRelease, false, nil
}

// nopRelease is a no-op release function returned when no pool buffer was used.
func nopRelease() {}

// GetHash returns the state machine hash if the underlying SM supports it.
// Returns (0, false, nil) if not supported.
//
// It takes the read guard for the same reasons Lookup does, with the same
// tier split. On a basic sm.StateMachine the guard excludes Apply, so the hash
// never observes a half-applied batch. On the concurrent and on-disk tiers it
// excludes only Close, so GetHash runs alongside Update, and the sm.Hasher
// contract makes the implementation responsible for hashing whole batches.
// Holding applyMu here instead would let a long hash stall the shard's
// applies. On every tier the guard keeps a
// closed SM from being read.
func (s *StateMachine) GetHash() (uint64, bool, error) {
	s.readGuard.RLock()
	defer s.readGuard.RUnlock()
	if s.closed {
		return 0, false, ErrClosed
	}
	return s.sm.GetHash()
}

// SaveSnapshot saves both the user SM state and session state to the
// writer. Format: [session data][SM data]. The stopper channel signals
// cancellation. The caller is responsible for ensuring no concurrent Apply
// runs for the duration of this call (the engine quiesces applies via the
// node saving gate); use SaveSnapshotGated to release that gate early once the
// point-in-time is captured.
func (s *StateMachine) SaveSnapshot(ctx context.Context, w io.Writer, stopper <-chan struct{}) error {
	return s.SaveSnapshotGated(ctx, w, stopper, nil)
}

// SaveSnapshotGated saves session + user SM state and invokes afterCapture
// exactly once, after the consistent point-in-time is captured (session state
// marshaled and, for concurrent/on-disk SMs, PrepareSnapshot completed) and
// before the user SM snapshot streams. This lets the engine hold the apply gate
// only across the capture — which reads the session manager (never safe for
// concurrent use) and, for a regular SM, its in-memory state — and release it
// so applies resume while an on-disk/concurrent snapshot streams its immutable
// context. afterCapture may be nil (equivalent to SaveSnapshot). If the save
// errors before the capture completes, afterCapture is not called.
func (s *StateMachine) SaveSnapshotGated(ctx context.Context, w io.Writer, stopper <-chan struct{}, afterCapture func()) error {
	// Two guards, because this call spans two phases with different needs.
	//
	// readGuard and applyMu cover the CAPTURE only. applyMu excludes Apply on
	// every tier, which is what makes the session MarshalTo below safe (the
	// session manager is not safe for concurrent use); readGuard's read lock
	// additionally excludes a basic-tier Apply, which is what a regular SM's
	// whole-state save requires. Both are released the instant the capture
	// completes, so applies resume while the snapshot streams — the reason
	// SaveSnapshotGated exists.
	//
	// streamGuard covers CAPTURE AND STREAM. It excludes only Close, so an
	// in-flight save still finishes (or aborts via stopper) before the SM is
	// closed under it, without holding applies for the transfer.
	//
	// Lock order is readGuard, applyMu, streamGuard, as documented on the
	// fields.
	s.readGuard.RLock()
	s.applyMu.Lock()
	s.streamGuard.RLock()
	defer s.streamGuard.RUnlock()

	// releaseCapture drops the capture guards exactly once, whether the
	// capture completed or the save failed before it. sync.Once rather than a
	// bool because afterCapture is invoked from the wrapper, off this
	// goroutine's straight-line path.
	var once sync.Once
	releaseCapture := func() {
		once.Do(func() {
			s.applyMu.Unlock()
			s.readGuard.RUnlock()
		})
	}
	defer releaseCapture()

	if s.closed {
		return ErrClosed
	}

	// Check for cancellation before starting.
	select {
	case <-stopper:
		return ErrStopped
	default:
	}

	// Save session state first. The session manager is not safe for concurrent
	// use, so this MarshalTo must run with applies quiesced -- by the caller's
	// gate and by applyMu, which is still held here.
	if err := s.sessions.MarshalTo(w); err != nil {
		return err
	}

	// Save user SM state. The capture guard is released in lockstep with the
	// engine's apply gate: whatever resumes applies must also stop excluding
	// them, or the gate release buys nothing.
	gated := func() {
		releaseCapture()
		if afterCapture != nil {
			afterCapture()
		}
	}
	return s.sm.saveSnapshotGated(ctx, w, stopper, gated)
}

// RecoverFromSnapshot restores both session state and user SM state
// from the reader. The stopper channel signals cancellation.
func (s *StateMachine) RecoverFromSnapshot(ctx context.Context, r io.Reader, stopper <-chan struct{}) error {
	// Defence in depth: never install snapshot state into a closed SM (see
	// Apply). The read guard is held for the whole recovery, so Close blocks
	// until it finishes; the stopper aborts it promptly once the node stops.
	// applyMu excludes Apply on every tier for the same span, because the
	// recovery replaces the session table and policy Apply works against.
	s.readGuard.RLock()
	defer s.readGuard.RUnlock()
	s.applyMu.Lock()
	defer s.applyMu.Unlock()
	if s.closed {
		return ErrClosed
	}

	// Check for cancellation before starting.
	select {
	case <-stopper:
		return ErrStopped
	default:
	}

	// Adopt the shard's replicated policy before reading the session table.
	// UnmarshalFrom bounds-checks the table against the manager's limits, and
	// the table was written by a replica applying the SHARD's policy: checking
	// it against this node's configuration instead would reject a snapshot the
	// rest of the shard considers well-formed, and the reject is fatal to
	// recovery. For a replica bootstrapped by an InstallSnapshot this is also
	// the first point at which a policy is known at all.
	s.refreshSessionPolicy()

	// The session table is decoded into a STAGED manager and swapped in only
	// after the user state machine has also restored. Restoring a snapshot is
	// all-or-nothing: the table and the user state describe the same log
	// index, and installing one without the other breaks at-most-once
	// delivery in both directions.
	//
	// Decoding straight into s.sessions replaced the live table before
	// s.sm.RecoverFromSnapshot ran. If the user restore then failed — a
	// truncated payload, a user SM that rejects it — the node kept user data
	// from index N beside session metadata from a later index M. A client
	// retry could then be answered from a cached result for an operation the
	// user state machine never applied, or a cached response could vanish and
	// let a duplicate command execute a second time. Neither surfaces an
	// error anywhere.
	//
	// The reader is sequential, so staging costs nothing: UnmarshalFrom
	// advances r to exactly the position the user restore must start from,
	// whether or not the staged manager is ultimately adopted.
	staged := session.NewManager(
		s.sessionPolicy.MaxTotalSessions, s.sessionPolicy.MaxSessionsPerClient,
		s.sessionPolicy.MaxSessionsPerOwner)
	if err := staged.UnmarshalFrom(r); err != nil {
		return err
	}

	// Recover user SM state. Only once this succeeds is the snapshot's session
	// table adopted.
	if err := s.sm.RecoverFromSnapshot(ctx, r, stopper); err != nil {
		return err
	}

	s.sessions = staged
	return nil
}

// Close releases resources held by both the session manager and the
// user state machine.
func (s *StateMachine) Close() error {
	// Acquire the write lock to block until all in-flight reads and applies
	// release, then mark closed so anyone who was blocked acquiring the guard
	// returns ErrClosed instead of touching the now-closed SM.
	s.readGuard.Lock()
	s.closed = true
	s.readGuard.Unlock()

	// Then close the user SM while HOLDING streamGuard, which waits out a
	// snapshot that is STREAMING. The gated save released readGuard at capture
	// so applies could resume, so readGuard alone no longer proves no save is
	// in flight — streamGuard is what carries that now. Taken after readGuard,
	// per the documented lock order.
	//
	// Held ACROSS the close rather than acquired and released as a barrier: a
	// bare Lock/Unlock pair is an empty critical section, which reads as a
	// mistake and is one staticcheck flags (SA2001). Holding it also closes a
	// real gap — a save that had not yet taken its read lock could otherwise
	// slip in between the barrier and the close and stream from a closed SM.
	// It cannot deadlock: sm.Close does not call back into a guarded method,
	// and any save that acquires the guard afterwards sees closed and returns
	// ErrClosed.
	//
	// This is a wait, not a cancellation: the engine closes the node's stopC
	// before Close runs, which aborts an in-flight save promptly via the
	// stopper checks, and this blocks only until that unwinds.
	s.streamGuard.Lock()
	defer s.streamGuard.Unlock()

	return s.sm.Close(context.Background())
}

// LastApplied returns the index of the last applied entry.
// Safe for concurrent access from the snapshot pool worker goroutine.
func (s *StateMachine) LastApplied() uint64 {
	return s.lastApplied.Load()
}

// SetLastApplied records idx as the index of the last applied entry. Every
// path that RESUMES a state machine at a non-zero index must call it, because
// nothing else sets this counter: the on-disk Open path seeds it from the SM's
// own durable position and RecoverFromSnapshot sets it not at all (the session
// table travels in the snapshot stream; this counter does not).
//
// The two callers are the engine's snapshot pool, after RecoverFromSnapshot
// installs snapshot state at idx, and Host.StartShard, beside the engine node's
// SeedLastApplied on the restart path. Both matter for the same two reasons:
// the counter is the index source for the NEXT snapshot save, so a snapshot
// taken before the next Apply would otherwise pair a stale index with
// post-recovery state; and it is the lower bound of Apply's session expiry
// checkpoint walk, so a stale value silently re-expires sessions the rest of
// the shard still holds (see Apply).
//
// Applies are quiesced for the duration of recovery and the node is not yet in
// the pipeline during StartShard, so no concurrent Apply can race this store.
func (s *StateMachine) SetLastApplied(idx uint64) {
	s.lastApplied.Store(idx)
}

// SMType returns the type of the underlying state machine.
func (s *StateMachine) SMType() SMType {
	return s.smType
}

// Sessions returns this state machine's session manager for INSPECTION.
//
// CONCURRENCY. session.Manager is not safe for concurrent use, and this hands
// out the live one — the same object Apply, RecoverFromSnapshot and
// SaveSnapshotGated mutate. Every method on it, including the read-only-looking
// Count and IsRegistered, must therefore be called only when no Apply and no
// snapshot save can be in flight for this shard: from a test that drives the
// state machine directly, or from a shard the engine has quiesced. There is no
// lock here to make that safe, and adding one would not help — the manager's
// invariants span calls, so a caller reading Count and then acting on it needs
// the whole window, not one method's worth.
//
// It has no production caller. In particular there is no leader-driven TTL
// expiration scan: session expiry is driven from Apply at fixed log-index checkpoints precisely
// BECAUSE a leader-local wall-clock scan would not be deterministic across
// replicas (see Apply). The accessor is kept for the shard-level tests in this
// package and for the host's session-policy tests, which assert the limits the
// apply path actually resolved; nothing else may use it, and a future caller
// that wants one of these numbers on a live shard needs a snapshot of the value
// taken on the apply goroutine, not this pointer.
func (s *StateMachine) Sessions() *session.Manager {
	return s.sessions
}
