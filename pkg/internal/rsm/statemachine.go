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

// lookupBufPool pools 256-byte *[]byte buffers for the NALookupBuf path.
var lookupBufPool = sync.Pool{
	New: func() interface{} {
		buf := make([]byte, 0, 256)
		return &buf
	},
}

// SMType identifies the kind of state machine being managed.
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

// defaultMaxExpirePerCheckpoint caps sessions expired per checkpoint; checkpoints are keyed to index ranges, so expiry is deterministic.
const defaultMaxExpirePerCheckpoint = 64

// maxExpirePerCheckpoint returns the per-checkpoint expiry cap, MaxTotalSessions/8 from the replicated policy, so a full table drains in 8 windows.
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

// StateMachine wraps a user state machine with session deduplication, entry conversion and snapshot coordination.
type StateMachine struct {
	sm          smWrapper
	smType      SMType
	sessions    *session.Manager
	snapshotDir string
	lastApplied atomic.Uint64
	shardID     uint64
	replicaID   uint64

	// sessionPolicy is the policy apply currently uses: the replicated one when known, else localPolicy; apply goroutine only.
	sessionPolicy proto.SessionPolicy

	// localPolicy is this node's configuration, used only while the shard has no replicated policy.
	localPolicy proto.SessionPolicy

	// sessionPolicyFn returns the replicated session policy, read at the start of each Apply; it is deterministic only because the policy is bootstrap-only.
	sessionPolicyFn func() proto.SessionPolicy

	// zeroCopyEntryCmd makes Entry.Cmd alias the internal buffer, which the SM must copy to retain; mirrors Config.ZeroCopyEntryCmd.
	zeroCopyEntryCmd bool

	// logger receives the session-policy disagreement report
	// (logPolicyDisagreement); nil means the process slog default. Set once,
	// at construction, through WithLogger.
	logger *slog.Logger

	// Buffers reused by the single apply goroutine across Apply calls.
	smEntryBuf  []sm.Entry
	smResultBuf []sm.Result
	indexMapBuf []int

	// readGuard excludes Close from calls into the SM; Apply holds it exclusively on the basic tier and shared on the concurrent and on-disk tiers.
	readGuard sync.RWMutex

	// applyMu serializes Apply, snapshot capture and RecoverFromSnapshot. Lock order: readGuard, applyMu, streamGuard.
	applyMu sync.Mutex

	// streamGuard blocks Close while a snapshot streams, without blocking Apply. Lock order: readGuard, applyMu, streamGuard.
	streamGuard sync.RWMutex
	// closed is set under readGuard by Close so blocked readers return ErrClosed.
	closed bool
}

// NewStateMachine wraps the SM from createFn (ErrInvalidSMType otherwise) under local session policy, refusing a replicated policy that differs with ErrSessionPolicyMismatch.
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
	if err := s.checkSessionPolicy(s.sessionPolicy); err != nil {
		return nil, err
	}
	s.sessions = session.NewManager(
		s.sessionPolicy.MaxTotalSessions, s.sessionPolicy.MaxResponsesPerSession,
		s.sessionPolicy.MaxSessionsPerOwner)
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

// effectiveSessionPolicy returns the replicated policy when known, else the local one.
func (s *StateMachine) effectiveSessionPolicy() proto.SessionPolicy {
	if s.sessionPolicyFn == nil {
		return s.localPolicy
	}
	if replicated := s.sessionPolicyFn(); replicated.IsSet() {
		return replicated
	}
	return s.localPolicy
}

// refreshSessionPolicy re-checks the replicated policy when it changes and returns ErrSessionPolicyMismatch on disagreement; apply goroutine only.
func (s *StateMachine) refreshSessionPolicy() error {
	effective := s.effectiveSessionPolicy()
	if effective == s.sessionPolicy {
		return nil
	}
	if err := s.checkSessionPolicy(effective); err != nil {
		return err
	}
	s.sessionPolicy = effective
	s.sessions.SetLimits(effective.MaxTotalSessions, effective.MaxResponsesPerSession,
		effective.MaxSessionsPerOwner)
	return nil
}

// checkSessionPolicy refuses a set replicated policy that differs from local configuration, naming the first differing setting.
func (s *StateMachine) checkSessionPolicy(committed proto.SessionPolicy) error {
	err := sessionPolicyMismatch(s.localPolicy, committed)
	if err == nil {
		return nil
	}
	logger := s.logger
	if logger == nil {
		logger = slog.Default()
	}
	logger.Error("rsm: refusing the shard: this node's session settings disagree "+
		"with the shard's committed session policy",
		"shardID", s.shardID,
		"replicaID", s.replicaID,
		"error", err,
	)
	return err
}

// sessionPolicyMismatch returns nil when committed is unset or equal to local,
// and otherwise ErrSessionPolicyMismatch wrapped with the first disagreeing
// config.Config setting's name and both values.
func sessionPolicyMismatch(local, committed proto.SessionPolicy) error {
	if !committed.IsSet() || local == committed {
		return nil
	}
	settings := [...]struct {
		name             string
		local, committed uint64
	}{
		{"SessionExpiryEntries", local.ExpiryEntries, committed.ExpiryEntries},
		{"MaxTotalSessions", local.MaxTotalSessions, committed.MaxTotalSessions},
		{"MaxResponsesPerSession", local.MaxResponsesPerSession, committed.MaxResponsesPerSession},
		{"MaxSessionsPerOwner", local.MaxSessionsPerOwner, committed.MaxSessionsPerOwner},
	}
	for _, st := range settings {
		if st.local != st.committed {
			return fmt.Errorf("%w: %s is %d in this node's Config, %d in the shard's committed policy",
				ErrSessionPolicyMismatch, st.name, st.local, st.committed)
		}
	}
	return nil
}

// wrapStateMachine picks the wrapper type once, checking DiskStateMachine before ConcurrentStateMachine.
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

// lockApply takes readGuard (exclusive on the basic tier, shared otherwise), then applyMu.
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

// Apply runs committed entries through the session lifecycle into results (len(entries)); expiry runs only at fixed log-index checkpoints from the replicated policy, so state is independent of batching.
func (s *StateMachine) Apply(entries []proto.Entry, results []sm.Result) error {
	// Take readGuard in the tier's mode, then applyMu; this also makes Close wait out an in-flight Apply.
	s.lockApply()
	defer s.unlockApply()
	if s.closed {
		return ErrClosed
	}
	if len(entries) == 0 {
		return nil
	}

	// A batch starting above lastApplied+1 is ErrApplyGap; an already-applied entry is the on-disk replay no-op.
	if prev := s.lastApplied.Load(); entries[0].Index > prev+1 {
		slog.Error("rsm: committed entry gap before apply",
			"shardID", s.shardID,
			"replicaID", s.replicaID,
			"last_applied", prev,
			"batch_first_index", entries[0].Index,
		)
		return ErrApplyGap
	}

	// Adopt the replicated policy before classifying entries; a mismatch fails the batch.
	if err := s.refreshSessionPolicy(); err != nil {
		return err
	}

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

	// Walk checkpoints in (prevIndex, lastIndex], flushing entries at or below each before running expiry; requires lastApplied to be seeded.
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

// applyBatch applies one run of entries through the session lifecycle and user SM, without expiry or advancing lastApplied; the caller holds lockApply.
//
//nolint:gocyclo // the classification loop is not a per-entry function: it reads and mutates six pieces of BATCH-scoped state (admittedThisBatch, batchSeen, mirrors, sessionLifecycleAt and the three reused smEntries/smResults/indexMap buffers), and four of those are keyed by the entry's POSITION in the batch, which the second loop then resolves against. Extracting the body would have to thread all of it through the helper or hoist it into a struct — relocating the state rather than reducing it
func (s *StateMachine) applyBatch(entries []proto.Entry, results []sm.Result) error {
	// Convert entries for the user SM, skipping duplicates and handling session register/unregister inline.
	smEntries := s.smEntryBuf[:0]
	smResults := s.smResultBuf[:0]
	indexMap := s.indexMapBuf[:0]

	// admittedThisBatch counts new series admitted per client in this batch for the capacity gate.
	var admittedThisBatch map[uint64]uint64

	// batchSeen maps (clientID, seriesID) to its first entry in this batch so retries mirror that result.
	var batchSeen map[uint64]map[uint64]int
	// mirrors maps an in-batch duplicate's entry index to the entry index of
	// the first occurrence whose result it must copy. Resolved after the
	// user-SM Update populates results for the first occurrences.
	var mirrors map[int]int

	// sessionLifecycleAt maps a clientID to its latest in-batch Register or Unregister; earlier entries must not record a response.
	var sessionLifecycleAt map[uint64]int

	for i := range entries {
		entry := &entries[i]

		// Config change and initial-membership entries are applied by processConfigChanges and never reach the user SM.
		if entry.IsConfigChange() || entry.IsInitialMembership() {
			results[i] = sm.Result{}
			continue
		}

		// Skip the leader's no-op and other empty non-session entries.
		if !entry.IsSessionManaged() && !entry.IsNewSessionRequest() &&
			!entry.IsEndOfSessionRequest() && len(entry.Cmd) == 0 {
			results[i] = sm.Result{}
			continue
		}

		// Handle session registration: register the client and return
		// the clientID as the result Value. Skip user SM.
		if entry.IsNewSessionRequest() {
			// The per-owner cap uses the replicated entry.OwnerID, so every replica decides the same.
			if err := s.sessions.Register(entry.ClientID, entry.OwnerID, entry.Index); err != nil {
				// A refused registration returns an empty result and leaves the table untouched.
				results[i] = sm.Result{}
			} else {
				if sessionLifecycleAt == nil {
					sessionLifecycleAt = make(map[uint64]int)
				}
				sessionLifecycleAt[entry.ClientID] = i
				// A new incarnation discards in-batch bookkeeping from the prior one.
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
				// ErrSessionNotFound and ErrResponseEvicted both map to SESSION_EXPIRED (Raft thesis Figure 6.1, step 3).
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
				// Return an owned copy of the cached response Data.
				if len(cached.Data) > 0 {
					owned := make([]byte, len(cached.Data))
					copy(owned, cached.Data)
					cached.Data = owned
				}
				results[i] = cached
				continue
			}
			// Mirror an in-batch retry of an earlier occurrence before the capacity gate, without applying it.
			if seriesMap, ok := batchSeen[entry.ClientID]; ok {
				if firstIdx, seen := seriesMap[entry.SeriesID]; seen {
					if mirrors == nil {
						mirrors = make(map[int]int)
					}
					mirrors[i] = firstIdx
					continue
				}
			}
			// Reject a new series whose result could not be cached before it reaches the user SM, as SESSION_EXPIRED.
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

		// Forward to the user SM, copying Cmd unless zeroCopyEntryCmd is set.
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
				// The session was closed or replaced later in this batch: keep the result but do not cache it.
				if sessionLifecycleAt[entry.ClientID] > origIdx {
					continue
				}
				cached := smResults[j]
				// Cache a deep copy of the result Data; the client's buffer and pooled zero-copy buffers must not alias the cache.
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
					// A missing session drops the record deterministically; any other error is fatal.
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

	// Each in-batch duplicate copies its first occurrence's result without recording it again.
	for mirrorIdx, firstIdx := range mirrors {
		results[mirrorIdx] = results[firstIdx]
	}

	// Retain grown buffers for the next applyBatch() call.
	s.smEntryBuf = smEntries
	s.smResultBuf = smResults
	s.indexMapBuf = indexMap

	return nil
}

// Lookup forwards query to the user SM under readGuard; for the []byte NALookup path an absent key is an untyped nil and an empty value a non-nil empty slice.
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
			// Fold a nil slice into an untyped nil so absence is result == nil; a zero-length non-nil slice means an empty value.
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

// NALookupBuf returns a pooled value from sm.NALookupInto and a release func the caller must call, falling back to NALookup; ok is false when neither is supported.
func (s *StateMachine) NALookupBuf(key []byte) (result []byte, release func(), supported bool, err error) {
	// Hold readGuard for the lookup only; NALookupInto copies into dst.
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
		// releaseOnce covers the whole release, including Put, so a buffer is never pooled twice.
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

// GetHash returns the SM hash if supported, else (0, false, nil), under readGuard like Lookup.
func (s *StateMachine) GetHash() (uint64, bool, error) {
	s.readGuard.RLock()
	defer s.readGuard.RUnlock()
	if s.closed {
		return 0, false, ErrClosed
	}
	return s.sm.GetHash()
}

// SaveSnapshot writes session data then SM data to w; the caller must exclude Apply for the whole call.
func (s *StateMachine) SaveSnapshot(ctx context.Context, w io.Writer, stopper <-chan struct{}) error {
	return s.SaveSnapshotGated(ctx, w, stopper, nil)
}

// SaveSnapshotGated saves session and SM state, calling afterCapture once after the point-in-time capture and before streaming; nil afterCapture equals SaveSnapshot.
func (s *StateMachine) SaveSnapshotGated(ctx context.Context, w io.Writer, stopper <-chan struct{}, afterCapture func()) error {
	// readGuard and applyMu cover the capture only; streamGuard covers capture and stream to exclude Close.
	s.readGuard.RLock()
	s.applyMu.Lock()
	s.streamGuard.RLock()
	defer s.streamGuard.RUnlock()

	// releaseCapture drops the capture guards exactly once.
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
	// Hold readGuard and applyMu for the whole recovery.
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

	// Adopt the replicated policy before reading the session table; a mismatch fails recovery.
	if err := s.refreshSessionPolicy(); err != nil {
		return err
	}

	// Decode the session table into a staged manager, adopted only after the user SM restores.
	staged := session.NewManager(
		s.sessionPolicy.MaxTotalSessions, s.sessionPolicy.MaxResponsesPerSession,
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

	// Close the user SM while holding streamGuard, which waits out a streaming snapshot.
	s.streamGuard.Lock()
	defer s.streamGuard.Unlock()

	return s.sm.Close(context.Background())
}

// LastApplied returns the index of the last applied entry.
// Safe for concurrent access from the snapshot pool worker goroutine.
func (s *StateMachine) LastApplied() uint64 {
	return s.lastApplied.Load()
}

// SetLastApplied records idx as the last applied index; every path resuming an SM at a non-zero index must call it.
func (s *StateMachine) SetLastApplied(idx uint64) {
	s.lastApplied.Store(idx)
}

// SMType returns the type of the underlying state machine.
func (s *StateMachine) SMType() SMType {
	return s.smType
}

// Sessions returns the live session manager for inspection by tests; it is safe only while no Apply or snapshot save is in flight.
func (s *StateMachine) Sessions() *session.Manager {
	return s.sessions
}
