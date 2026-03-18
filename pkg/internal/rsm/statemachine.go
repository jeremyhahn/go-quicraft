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
	"fmt"
	"io"
	"log/slog"
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

// defaultMaxExpirePerBatch is the maximum number of sessions to expire
// per Apply call. This amortizes the cost of session expiry over multiple
// batches, preventing a single Apply from doing excessive cleanup work.
const defaultMaxExpirePerBatch = 64

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

	// sessionExpiryEntries is the inactivity threshold in number of
	// applied log entries. Sessions that have not been active within
	// this many entries are expired. A value of 0 disables automatic
	// session expiry (default). This is deterministic across replicas
	// because it is driven by log index, not wall-clock time.
	sessionExpiryEntries uint64

	// zeroCopyEntryCmd controls whether Entry.Cmd aliases the internal
	// buffer (zero-copy, zero-alloc) or is copied for the state machine.
	// When true, the sm.Entry.Cmd passed to Update() aliases the proto
	// entry's Cmd slice. The state machine MUST copy Cmd if retaining
	// it beyond Update(). When false (default), a safe owned copy is
	// made for each entry. This field mirrors Config.ZeroCopyEntryCmd.
	zeroCopyEntryCmd bool

	// Pre-allocated buffers for Apply(). Reused via [:0] reset to avoid
	// per-call heap allocations. Safe because Apply() is called from a
	// single apply goroutine per shard and the buffers are fully consumed
	// before Apply() returns.
	smEntryBuf  []sm.Entry
	smResultBuf []sm.Result
	indexMapBuf []int
}

// NewStateMachine creates a managed state machine wrapper using the
// given factory function. The factory must return one of StateMachine,
// ConcurrentStateMachine, or DiskStateMachine. Returns ErrInvalidSMType
// if the returned type does not match any known interface.
//
// sessionExpiryEntries controls automatic session expiry: sessions
// inactive for more than this many log entries are cleaned up. Set to 0
// to disable (default). When enabled, all replicas expire the same
// sessions at the same log index, preserving determinism.
func NewStateMachine(
	shardID, replicaID uint64,
	createFn sm.CreateFunc,
	snapshotDir string,
	maxSessions, maxResponses uint64,
	sessionExpiryEntries uint64,
	zeroCopyEntryCmd bool,
) (*StateMachine, error) {
	raw := createFn(shardID, replicaID)
	wrapper, st, err := wrapStateMachine(raw)
	if err != nil {
		return nil, err
	}
	return &StateMachine{
		sm:                   wrapper,
		smType:               st,
		sessions:             session.NewManager(maxSessions, maxResponses),
		snapshotDir:          snapshotDir,
		shardID:              shardID,
		replicaID:            replicaID,
		sessionExpiryEntries: sessionExpiryEntries,
		zeroCopyEntryCmd:     zeroCopyEntryCmd,
	}, nil
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

// Apply processes committed entries through the session lifecycle:
//   - Session register entries (SeriesID == SessionSeriesIDForRegister)
//     register the client with the session manager.
//   - Session unregister entries (SeriesID == SessionSeriesIDForUnregister)
//     remove the client from the session manager.
//   - Normal session entries are deduplicated and forwarded to the user SM.
//   - Non-session entries (clientID == 0) go directly to the user SM.
//
// The results slice must be pre-allocated with len(results) == len(entries).
func (s *StateMachine) Apply(entries []proto.Entry, results []sm.Result) error {
	// Build the filtered entry list and result mapping.
	// We convert proto.Entry to sm.Entry for the user SM, skipping
	// entries that are detected as duplicates by the session manager,
	// and handling session register/unregister operations inline.
	// Buffers are reused across calls via [:0] reset, growing on demand.
	smEntries := s.smEntryBuf[:0]
	smResults := s.smResultBuf[:0]
	indexMap := s.indexMapBuf[:0]

	for i := range entries {
		entry := &entries[i]

		// Handle session registration: register the client and return
		// the clientID as the result Value. Skip user SM.
		if entry.IsNewSessionRequest() {
			if err := s.sessions.Register(entry.ClientID, entry.Index); err != nil {
				// Already registered is not fatal — return empty result.
				results[i] = sm.Result{}
			} else {
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
				results[i] = cached
				continue
			}
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
				if err := s.sessions.RecordResult(
					entry.ClientID,
					entry.SeriesID,
					entry.RespondedTo,
					entry.Index,
					smResults[j],
				); err != nil {
					return err
				}
			}
		}
	}

	// Retain grown buffers for the next Apply() call.
	s.smEntryBuf = smEntries
	s.smResultBuf = smResults
	s.indexMapBuf = indexMap

	// Update lastApplied to the highest index in the batch.
	// Uses atomic store because LastApplied() may be called concurrently
	// from the snapshot pool worker goroutine.
	if len(entries) > 0 {
		lastIndex := entries[len(entries)-1].Index
		if lastIndex > s.lastApplied.Load() {
			s.lastApplied.Store(lastIndex)
		}

		// Expire inactive sessions after each batch. This is deterministic
		// because it is driven by log index: all replicas process the same
		// entries and expire the same sessions at the same point.
		if s.sessionExpiryEntries > 0 {
			s.sessions.ExpireSessions(lastIndex, s.sessionExpiryEntries, defaultMaxExpirePerBatch)
		}
	}

	return nil
}

// Lookup forwards the query to the user state machine. For StateMachine,
// this is serialized with Apply by the caller. For concurrent and on-disk
// types, the user SM handles its own synchronization.
func (s *StateMachine) Lookup(ctx context.Context, query interface{}) (interface{}, error) {
	// Fast path: try NALookup for []byte queries to avoid boxing.
	if key, ok := query.([]byte); ok {
		result, supported, err := s.sm.NALookup(key)
		if err != nil {
			return nil, err
		}
		if supported {
			return result, nil
		}
	}
	return s.sm.Lookup(ctx, query)
}

// NALookup performs a byte-slice lookup if the underlying state machine
// supports it. Returns (nil, false, nil) if not supported.
func (s *StateMachine) NALookup(key []byte) ([]byte, bool, error) {
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
	// Fast path: try NALookupInto with a pooled buffer.
	poolBuf := lookupBufPool.Get().(*[]byte)
	filled, ok, lookupErr := s.sm.NALookupInto(key, *poolBuf)
	if ok {
		if lookupErr != nil {
			// Return the pool buffer unused on error.
			*poolBuf = (*poolBuf)[:0]
			lookupBufPool.Put(poolBuf)
			return nil, nopRelease, true, lookupErr
		}
		// The SM may have grown the buffer via append; update the pool entry to
		// track the (possibly larger) backing array so it is reused on Put.
		*poolBuf = filled[:cap(filled)]
		rel := func() {
			*poolBuf = (*poolBuf)[:0]
			lookupBufPool.Put(poolBuf)
		}
		return filled, rel, true, nil
	}

	// NALookupInto not supported — return the pool buffer unused.
	lookupBufPool.Put(poolBuf)

	// Slow path: fall back to standard NALookup (allocates).
	result2, ok2, err2 := s.sm.NALookup(key)
	if ok2 {
		return result2, nopRelease, true, err2
	}
	return nil, nopRelease, false, nil
}

// nopRelease is a no-op release function returned when no pool buffer was used.
func nopRelease() {}

// GetHash returns the state machine hash if the underlying SM supports it.
// Returns (0, false, nil) if not supported.
func (s *StateMachine) GetHash() (uint64, bool, error) {
	return s.sm.GetHash()
}

// SaveSnapshot saves both the user SM state and session state to the
// writer. Format: [session data][SM data]. The stopper channel signals
// cancellation.
func (s *StateMachine) SaveSnapshot(ctx context.Context, w io.Writer, stopper <-chan struct{}) error {
	// Check for cancellation before starting.
	select {
	case <-stopper:
		return ErrStopped
	default:
	}

	// Save session state first.
	if err := s.sessions.MarshalTo(w); err != nil {
		return err
	}

	// Save user SM state.
	return s.sm.SaveSnapshot(ctx, w, stopper)
}

// RecoverFromSnapshot restores both session state and user SM state
// from the reader. The stopper channel signals cancellation.
func (s *StateMachine) RecoverFromSnapshot(ctx context.Context, r io.Reader, stopper <-chan struct{}) error {
	// Check for cancellation before starting.
	select {
	case <-stopper:
		return ErrStopped
	default:
	}

	// Recover session state first.
	if err := s.sessions.UnmarshalFrom(r); err != nil {
		return err
	}

	// Recover user SM state.
	return s.sm.RecoverFromSnapshot(ctx, r, stopper)
}

// Close releases resources held by both the session manager and the
// user state machine.
func (s *StateMachine) Close() error {
	return s.sm.Close(context.Background())
}

// LastApplied returns the index of the last applied entry.
// Safe for concurrent access from the snapshot pool worker goroutine.
func (s *StateMachine) LastApplied() uint64 {
	return s.lastApplied.Load()
}

// SMType returns the type of the underlying state machine.
func (s *StateMachine) SMType() SMType {
	return s.smType
}

// Sessions returns the session manager for external access (e.g.,
// by the leader for TTL expiration scans).
func (s *StateMachine) Sessions() *session.Manager {
	return s.sessions
}
