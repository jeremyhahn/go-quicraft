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

// Package session provides client session management for at-most-once
// delivery semantics. The session manager tracks client sessions and
// caches results for duplicate proposal detection. Session data is
// included in snapshots for recovery.
package session

import (
	"encoding/binary"
	"io"
	"slices"

	"github.com/jeremyhahn/go-quicraft/pkg/sm"
)

// Manager tracks client sessions for a single shard, providing
// at-most-once delivery semantics through duplicate detection.
// The Manager is not safe for concurrent use; the caller (the apply
// worker) is responsible for serialization.
type Manager struct {
	sessions     map[uint64]*clientSession
	maxTotal     uint64 // maximum total sessions per shard
	maxResponses uint64 // maximum cached responses per session
	maxPerOwner  uint64 // maximum concurrent sessions per owner; 0 disables

	// ownerCounts is the secondary index the per-owner cap is decided from:
	// ownerID -> number of live sessions registered under it. It is DERIVED
	// state, reconstructible from sessions alone, and exists only so Register
	// is O(1) instead of a scan of the whole table on every registration — the
	// apply path runs this per committed entry.
	//
	// Being derived, it has exactly one correctness requirement: every mutation
	// of sessions must move it in step. Register adds, Unregister and
	// ExpireSessions subtract, and UnmarshalFrom rebuilds it from the decoded
	// table rather than trusting anything on the wire. A count that drifts
	// above the truth refuses registrations the shard should admit — and it
	// would drift identically on every replica only by luck, so the divergence
	// this whole policy exists to prevent would come back through the index.
	//
	// The zero owner is never a key here: an unset owner is not capped, so
	// counting it would be pure cost.
	ownerCounts map[uint64]uint64
}

// NewManager creates a session manager with the given limits.
// maxTotal controls the total number of concurrent sessions per shard.
// maxResponses controls the maximum cached responses per session.
// maxPerOwner controls the maximum concurrent sessions one owner may hold;
// zero disables the per-owner cap.
//
// maxTotal and maxResponses substitute a default when zero because no caller
// can mean "admit nothing" by either. maxPerOwner does NOT, because zero has a
// meaning there — the cap is off — and that is the state every shard
// bootstrapped before the field existed is in.
func NewManager(maxTotal, maxResponses, maxPerOwner uint64) *Manager {
	if maxTotal == 0 {
		maxTotal = 16384
	}
	if maxResponses == 0 {
		maxResponses = 128
	}
	return &Manager{
		sessions:     make(map[uint64]*clientSession),
		maxTotal:     maxTotal,
		maxResponses: maxResponses,
		maxPerOwner:  maxPerOwner,
		ownerCounts:  make(map[uint64]uint64),
	}
}

// SetLimits replaces the manager's limits. All three are load-bearing for apply
// determinism — Register refuses past maxTotal and past maxPerOwner, and the
// response cache is bounded by maxResponses, and a replica that refuses what
// its peers admit diverges — so they are the shard's REPLICATED values, and
// this exists for the one moment a replica learns them: a state machine created
// before its shard's configuration was known (see
// rsm.StateMachine.refreshSessionPolicy).
//
// A zero maxTotal or maxResponses is ignored for the same reason NewManager
// substitutes a default for it: no caller can mean "admit nothing". maxPerOwner
// is assigned unconditionally, because zero is a real value there — the cap
// off — and ignoring it would make "the shard committed no per-owner cap"
// impossible to express once any non-zero value had been seen.
//
// Existing sessions are left alone. Lowering maxTotal below the current count
// therefore does not evict anyone; it stops further registrations, which is
// the same rule Register already applies to a table that filled up. Callers
// must invoke this from the apply goroutine, like every other Manager method.
func (m *Manager) SetLimits(maxTotal, maxResponses, maxPerOwner uint64) {
	if maxTotal != 0 {
		m.maxTotal = maxTotal
	}
	if maxResponses != 0 {
		m.maxResponses = maxResponses
	}
	m.maxPerOwner = maxPerOwner
}

// Register creates a new session for the given client ID, owned by ownerID.
// The index parameter is the log index of the register entry, used as the
// initial activity marker for deterministic expiry.
//
// ownerID is the principal the embedder declared for this session
// (proto.Entry.OwnerID). Zero means no owner was declared: the session is not
// counted against, and is never refused by, the per-owner cap. Every session
// registered before that field existed is in exactly that state, which is why
// zero cannot be treated as an owner in its own right — doing so would pool
// every ownerless session on the shard under one key and refuse them all at
// the cap.
//
// Returns ErrSessionAlreadyExists if the client is already registered, or
// ErrSessionLimitExceeded if either the shard-wide total or this owner's
// concurrent-session cap is reached. The two exhaustion cases deliberately
// report the SAME sentinel: the caller (rsm.StateMachine.Apply) must treat a
// refused registration identically whatever refused it, and every replica
// reaches the same verdict from the same log because both limits are
// replicated state and both counters are derived from the applied entries
// alone. A distinct error would invite a distinct code path, and a code path
// only one replica takes is the divergence this cap exists to prevent.
func (m *Manager) Register(clientID, ownerID, index uint64) error {
	if _, exists := m.sessions[clientID]; exists {
		return ErrSessionAlreadyExists
	}
	if uint64(len(m.sessions)) >= m.maxTotal {
		return ErrSessionLimitExceeded
	}
	// The per-owner gate is checked AFTER the shard-wide one so that a full
	// table reports the same way it always has regardless of ownership, and
	// only when both pass is anything mutated — a partially applied
	// registration would desynchronize ownerCounts from sessions.
	if m.ownerCapped(ownerID) {
		return ErrSessionLimitExceeded
	}
	m.sessions[clientID] = newClientSession(clientID, ownerID, index)
	if ownerID != noOwnerID {
		m.ownerCounts[ownerID]++
	}
	return nil
}

// ownerCapped reports whether ownerID already holds the maximum number of
// concurrent sessions. An unset owner and a zero cap are both exemptions, and
// both are the pre-existing behaviour rather than special cases: a shard that
// committed no per-owner cap, or an embedder that declares no owner, gets the
// bound this manager has always had — maxTotal — and nothing more.
func (m *Manager) ownerCapped(ownerID uint64) bool {
	if ownerID == noOwnerID || m.maxPerOwner == 0 {
		return false
	}
	return m.ownerCounts[ownerID] >= m.maxPerOwner
}

// releaseOwner drops one session from ownerID's count, deleting the key when it
// reaches zero so the index cannot outgrow the set of owners that actually hold
// sessions. Every removal from m.sessions must call this; see Manager.ownerCounts.
func (m *Manager) releaseOwner(ownerID uint64) {
	if ownerID == noOwnerID {
		return
	}
	if n := m.ownerCounts[ownerID]; n > 1 {
		m.ownerCounts[ownerID] = n - 1
	} else {
		delete(m.ownerCounts, ownerID)
	}
}

// Unregister removes the session for the given client ID. Returns
// ErrSessionNotFound if the client is not registered.
func (m *Manager) Unregister(clientID uint64) error {
	sess, exists := m.sessions[clientID]
	if !exists {
		return ErrSessionNotFound
	}
	delete(m.sessions, clientID)
	m.releaseOwner(sess.ownerID)
	return nil
}

// IsRegistered returns true if the given client ID has an active session.
func (m *Manager) IsRegistered(clientID uint64) bool {
	_, exists := m.sessions[clientID]
	return exists
}

// CheckDuplicate checks whether a proposal from the given client with
// the given series ID has already been applied. The index parameter is
// the log index of the entry being checked, used to update the session's
// activity marker for deterministic expiry.
//
// Returns:
//   - (cached result, true, nil) if the entry is a duplicate with a cached response.
//   - (zero Result, false, nil) if the entry is new and should be applied.
//   - (zero Result, false, ErrSessionNotFound) if the client is not registered.
//   - (zero Result, false, ErrResponseEvicted) if the seriesID was already
//     completed and the cached response was evicted (seriesID <= respondedTo).
//     Re-applying this entry would violate at-most-once semantics. Per Raft
//     PhD Figure 6.1 step 3, the caller should treat this the same as
//     SESSION_EXPIRED.
func (m *Manager) CheckDuplicate(clientID, seriesID, index uint64) (sm.Result, bool, error) {
	sess, exists := m.sessions[clientID]
	if !exists {
		return sm.Result{}, false, ErrSessionNotFound
	}
	// An entry BELOW the index that created this incarnation belongs to a
	// PREVIOUS incarnation of the same clientID: the session expired and the
	// client re-registered under an ID it had persisted (Host.RegisterSession
	// accepts a caller-supplied ID, which is the documented way a client
	// survives its own restart). The new incarnation starts with respondedTo
	// zero and an empty response cache, so without this the stale entry looks
	// brand new — seriesID > 0 and no cached result — and is applied a second
	// time. At-most-once is broken with no error raised anywhere.
	//
	// Answering it the way an expired session is answered is the behaviour
	// Raft PhD Figure 6.1 step 3 prescribes. The comparison is index-based, so
	// it is deterministic and replicated for free.
	if index < sess.registeredIndex {
		return sm.Result{}, false, ErrSessionNotFound
	}
	// If seriesID <= respondedTo, the client has already acknowledged
	// this result and the cached response was evicted. Re-applying would
	// violate at-most-once semantics.
	if seriesID <= sess.respondedTo {
		return sm.Result{}, false, ErrResponseEvicted
	}
	result, found := sess.responses[seriesID]
	if found {
		sess.touchIndex(index)
		return result, true, nil
	}
	return sm.Result{}, false, nil
}

// CanRecord reports whether a new (non-duplicate) result for the given
// client could be cached without exceeding the per-session response
// limit. It performs the same respondedTo-advance and eviction
// computation as RecordResult but WITHOUT mutating any state, so it is
// safe to call before the user state machine applies the entry.
//
// pending is the number of new series for this same client that have
// already been admitted earlier in the current apply batch but not yet
// recorded (RecordResult runs after the batched user-SM Update). Counting
// them prevents a batch of new series from collectively overflowing the
// cache: each entry is gated against the projected post-batch size, not
// just the pre-batch size.
//
// It returns nil if the result can be recorded, ErrSessionNotFound if the
// client is not registered, or ErrResponseLimitExceeded if the cache
// would still be full after eviction. Callers use this to reject an
// over-capacity entry BEFORE applying it to the user state machine,
// preserving at-most-once semantics: an entry that cannot be cached must
// never be applied, otherwise a later replay would re-apply it because no
// cached response exists to deduplicate against.
// The entry's own seriesID is deliberately not a parameter: the question is
// whether the cache has ROOM after eviction, and a new response always consumes
// a slot (a duplicate seriesID is answered from the cache by the dedup path and
// never reaches this check).
func (m *Manager) CanRecord(clientID, respondedTo, pending uint64) error {
	sess, exists := m.sessions[clientID]
	if !exists {
		return ErrSessionNotFound
	}
	// Compute the effective respondedTo after the advance RecordResult
	// would perform, then count responses that would survive eviction.
	effectiveRespondedTo := sess.respondedTo
	if respondedTo > effectiveRespondedTo {
		effectiveRespondedTo = respondedTo
	}
	surviving := uint64(0)
	for sid := range sess.responses {
		if sid > effectiveRespondedTo {
			surviving++
		}
	}
	if surviving+pending >= m.maxResponses {
		return ErrResponseLimitExceeded
	}
	return nil
}

// RecordResult caches the result for a proposal from the given client.
// Also evicts all cached responses with seriesID <= respondedTo and
// advances the session's respondedTo marker. The index parameter is
// the log index of the applied entry, used to update the session's
// activity marker for deterministic expiry.
//
// Returns ErrSessionNotFound if the client is not registered, or
// ErrResponseLimitExceeded if the session's response cache has reached
// the configured maximum after eviction, indicating a misbehaving client
// that is not advancing respondedTo.
func (m *Manager) RecordResult(clientID, seriesID, respondedTo, index uint64, result sm.Result) error {
	sess, exists := m.sessions[clientID]
	if !exists {
		return ErrSessionNotFound
	}
	if respondedTo > sess.respondedTo {
		sess.respondedTo = respondedTo
	}
	sess.evictUpTo(sess.respondedTo)
	// Advance the activity marker first: an entry was applied for this
	// session, so it was demonstrably active even if its response cache is
	// full. Doing this before the limit check prevents an active-but-full
	// session from looking idle to deterministic expiry.
	sess.touchIndex(index)
	if uint64(len(sess.responses)) >= m.maxResponses {
		return ErrResponseLimitExceeded
	}
	sess.responses[seriesID] = result
	return nil
}

// ExpireSessions removes sessions that have been inactive for more than
// inactivityThreshold entries relative to currentIndex. At most maxExpire
// sessions are removed per call to amortize cleanup cost. Returns the
// client IDs of expired sessions.
//
// Because expiry is driven by log index (not wall-clock time), all
// replicas expire the same sessions at the same point in the log,
// preserving deterministic state machine behavior across the cluster.
func (m *Manager) ExpireSessions(currentIndex, inactivityThreshold uint64, maxExpire int) []uint64 {
	if maxExpire <= 0 || inactivityThreshold == 0 {
		return nil
	}

	// Collect all expired candidates first. Sorting before applying the
	// maxExpire limit ensures every replica expires the exact same set of
	// sessions at the same log index, regardless of Go's randomized map
	// iteration order.
	var candidates []uint64
	for clientID, sess := range m.sessions {
		if sess.isExpired(currentIndex, inactivityThreshold) {
			candidates = append(candidates, clientID)
		}
	}

	if len(candidates) == 0 {
		return nil
	}

	// Deterministic ordering by clientID.
	slices.Sort(candidates)

	// Apply limit after sort so every replica selects the same subset.
	if len(candidates) > maxExpire {
		candidates = candidates[:maxExpire]
	}

	// Delete in deterministic order, releasing each session's owner slot as it
	// goes. Expiry is the one removal path that is not driven by an explicit
	// client action, so forgetting it here would leak owner counts upward until
	// a long-lived shard refused every registration from an owner that in fact
	// holds none.
	for _, clientID := range candidates {
		sess := m.sessions[clientID]
		delete(m.sessions, clientID)
		m.releaseOwner(sess.ownerID)
	}

	return candidates
}

// Count returns the number of active sessions.
func (m *Manager) Count() int {
	return len(m.sessions)
}

// MaxResponses returns the configured maximum responses per session.
func (m *Manager) MaxResponses() uint64 {
	return m.maxResponses
}

// MarshalTo serializes all session state to the writer for snapshot
// inclusion. The format is:
// [numSessions:4][session1][session2]...
// Each session: [clientID:8][ownerID:8][registeredIndex:8][respondedTo:8]
//
//	[numResponses:4][{seriesID:8, value:8, dataLen:4, data:N}...]
//	[lastActiveIndex:8]
//
// ownerCounts is NOT written. It is derived state, and UnmarshalFrom rebuilds
// it by counting the decoded sessions — which is both smaller and safer than
// persisting it, because a restored count can then never disagree with the
// table it describes.
func (m *Manager) MarshalTo(w io.Writer) error {
	// Write number of sessions.
	var buf [8]byte
	binary.LittleEndian.PutUint32(buf[:4], uint32(len(m.sessions)))
	if _, err := w.Write(buf[:4]); err != nil {
		return &MarshalError{Op: "write session count", Err: err}
	}

	// Sort client IDs for deterministic output.
	clientIDs := make([]uint64, 0, len(m.sessions))
	for id := range m.sessions {
		clientIDs = append(clientIDs, id)
	}
	slices.Sort(clientIDs)

	for _, clientID := range clientIDs {
		sess := m.sessions[clientID]
		if err := marshalSession(w, sess); err != nil {
			return err
		}
	}
	return nil
}

// marshalSession writes a single session to the writer.
func marshalSession(w io.Writer, sess *clientSession) error {
	var buf [8]byte

	// clientID
	binary.LittleEndian.PutUint64(buf[:8], sess.clientID)
	if _, err := w.Write(buf[:8]); err != nil {
		return &MarshalError{Op: "write clientID", Err: err}
	}

	// ownerID. It must survive the snapshot or the per-owner cap silently
	// resets on every restore: the restored sessions would all read as
	// ownerless, their owners' counts would come back at zero, and a replica
	// that recovered from a snapshot would admit registrations its peers refuse
	// — the exact divergence the cap is replicated to avoid. The wire format is
	// pinned at v1 pre-release, so the layout is changed in place and both sides
	// are updated in lockstep.
	binary.LittleEndian.PutUint64(buf[:8], sess.ownerID)
	if _, err := w.Write(buf[:8]); err != nil {
		return &MarshalError{Op: "write ownerID", Err: err}
	}

	// registeredIndex. It must survive the snapshot or the incarnation floor
	// in CheckDuplicate disappears on restore — which is exactly the path
	// (restart, then a stale entry commits) the floor exists to close. The
	// wire format is pinned at v1 pre-release, so the layout is changed in
	// place and both sides are updated in lockstep.
	binary.LittleEndian.PutUint64(buf[:8], sess.registeredIndex)
	if _, err := w.Write(buf[:8]); err != nil {
		return &MarshalError{Op: "write registeredIndex", Err: err}
	}

	// respondedTo
	binary.LittleEndian.PutUint64(buf[:8], sess.respondedTo)
	if _, err := w.Write(buf[:8]); err != nil {
		return &MarshalError{Op: "write respondedTo", Err: err}
	}

	// numResponses
	binary.LittleEndian.PutUint32(buf[:4], uint32(len(sess.responses)))
	if _, err := w.Write(buf[:4]); err != nil {
		return &MarshalError{Op: "write response count", Err: err}
	}

	// Sort series IDs for deterministic output.
	seriesIDs := make([]uint64, 0, len(sess.responses))
	for sid := range sess.responses {
		seriesIDs = append(seriesIDs, sid)
	}
	slices.Sort(seriesIDs)

	for _, sid := range seriesIDs {
		result := sess.responses[sid]

		// seriesID
		binary.LittleEndian.PutUint64(buf[:8], sid)
		if _, err := w.Write(buf[:8]); err != nil {
			return &MarshalError{Op: "write seriesID", Err: err}
		}

		// result value
		binary.LittleEndian.PutUint64(buf[:8], result.Value)
		if _, err := w.Write(buf[:8]); err != nil {
			return &MarshalError{Op: "write result value", Err: err}
		}

		// result data length + data
		binary.LittleEndian.PutUint32(buf[:4], uint32(len(result.Data)))
		if _, err := w.Write(buf[:4]); err != nil {
			return &MarshalError{Op: "write result data length", Err: err}
		}
		if len(result.Data) > 0 {
			if _, err := w.Write(result.Data); err != nil {
				return &MarshalError{Op: "write result data", Err: err}
			}
		}
	}

	// lastActiveIndex
	binary.LittleEndian.PutUint64(buf[:8], sess.lastActiveIndex)
	if _, err := w.Write(buf[:8]); err != nil {
		return &MarshalError{Op: "write lastActiveIndex", Err: err}
	}

	return nil
}

// UnmarshalFrom deserializes session state from the reader, replacing
// all existing sessions. Used during snapshot recovery.
//
// Bounds checks are enforced against the configured limits to prevent
// OOM from corrupted or malicious snapshot data:
//   - numSessions must be <= m.maxTotal
//   - numResponses per session must be <= m.maxResponses
//   - dataLen per response must be <= maxResponseDataSize (64 MiB)
func (m *Manager) UnmarshalFrom(r io.Reader) error {
	var buf [8]byte

	// Read number of sessions.
	if _, err := io.ReadFull(r, buf[:4]); err != nil {
		return &UnmarshalError{Op: "read session count", Err: err}
	}
	numSessions := binary.LittleEndian.Uint32(buf[:4])

	// Bounds check: numSessions must not exceed the configured limit.
	if uint64(numSessions) > m.maxTotal {
		return &BoundsError{
			Field: "numSessions",
			Value: uint64(numSessions),
			Limit: m.maxTotal,
		}
	}

	// Parse into a staging map and swap it in only after the full stream
	// parses cleanly. Populating m.sessions incrementally would, on a
	// mid-stream failure, leave the live node with a hybrid of new sessions
	// and whatever the caller does after the error — inconsistent with the
	// pre-recovery user-SM state. Building aside keeps recovery atomic.
	staged := make(map[uint64]*clientSession, numSessions)
	stagedOwners := make(map[uint64]uint64)
	for i := uint32(0); i < numSessions; i++ {
		sess, err := unmarshalSession(r, m.maxResponses)
		if err != nil {
			return err
		}
		staged[sess.clientID] = sess
		if sess.ownerID != noOwnerID {
			stagedOwners[sess.ownerID]++
		}
	}
	// The owner index is REBUILT from the decoded table, never read from the
	// stream. Recomputing it costs one pass over sessions already in hand and
	// makes a disagreement between the count and the table unrepresentable —
	// whereas a persisted count is one more thing a corrupt or truncated
	// snapshot can get wrong, in a way no bounds check would catch and that
	// would show up only as registrations refused on one replica.
	//
	// It is swapped in alongside sessions, so the same all-or-nothing property
	// UnmarshalFrom already had covers both: a mid-stream failure leaves the
	// live table AND its index exactly as they were.
	m.sessions = staged
	m.ownerCounts = stagedOwners
	return nil
}

// unmarshalSession reads a single session from the reader.
// maxResponses is the upper bound for the number of cached responses
// per session; values exceeding this limit produce a BoundsError.
func unmarshalSession(r io.Reader, maxResponses uint64) (*clientSession, error) {
	var buf [8]byte

	// clientID
	if _, err := io.ReadFull(r, buf[:8]); err != nil {
		return nil, &UnmarshalError{Op: "read clientID", Err: err}
	}
	clientID := binary.LittleEndian.Uint64(buf[:8])

	// ownerID — the per-owner cap's key. See marshalSession.
	if _, err := io.ReadFull(r, buf[:8]); err != nil {
		return nil, &UnmarshalError{Op: "read ownerID", Err: err}
	}
	ownerID := binary.LittleEndian.Uint64(buf[:8])

	// registeredIndex — the incarnation floor. See marshalSession.
	if _, err := io.ReadFull(r, buf[:8]); err != nil {
		return nil, &UnmarshalError{Op: "read registeredIndex", Err: err}
	}
	registeredIndex := binary.LittleEndian.Uint64(buf[:8])

	// respondedTo
	if _, err := io.ReadFull(r, buf[:8]); err != nil {
		return nil, &UnmarshalError{Op: "read respondedTo", Err: err}
	}
	respondedTo := binary.LittleEndian.Uint64(buf[:8])

	// numResponses
	if _, err := io.ReadFull(r, buf[:4]); err != nil {
		return nil, &UnmarshalError{Op: "read response count", Err: err}
	}
	numResponses := binary.LittleEndian.Uint32(buf[:4])

	// Bounds check: numResponses must not exceed the configured limit.
	if uint64(numResponses) > maxResponses {
		return nil, &BoundsError{
			Field: "numResponses",
			Value: uint64(numResponses),
			Limit: maxResponses,
		}
	}

	responses := make(map[uint64]sm.Result, numResponses)
	for j := uint32(0); j < numResponses; j++ {
		// seriesID
		if _, err := io.ReadFull(r, buf[:8]); err != nil {
			return nil, &UnmarshalError{Op: "read seriesID", Err: err}
		}
		seriesID := binary.LittleEndian.Uint64(buf[:8])

		// result value
		if _, err := io.ReadFull(r, buf[:8]); err != nil {
			return nil, &UnmarshalError{Op: "read result value", Err: err}
		}
		value := binary.LittleEndian.Uint64(buf[:8])

		// result data length + data
		if _, err := io.ReadFull(r, buf[:4]); err != nil {
			return nil, &UnmarshalError{Op: "read result data length", Err: err}
		}
		dataLen := binary.LittleEndian.Uint32(buf[:4])

		// Bounds check: dataLen must not exceed the hardcoded maximum.
		if uint64(dataLen) > maxResponseDataSize {
			return nil, &BoundsError{
				Field: "responseDataLen",
				Value: uint64(dataLen),
				Limit: maxResponseDataSize,
			}
		}

		var data []byte
		if dataLen > 0 {
			data = make([]byte, dataLen)
			if _, err := io.ReadFull(r, data); err != nil {
				return nil, &UnmarshalError{Op: "read result data", Err: err}
			}
		}

		responses[seriesID] = sm.Result{Value: value, Data: data}
	}

	// lastActiveIndex
	if _, err := io.ReadFull(r, buf[:8]); err != nil {
		return nil, &UnmarshalError{Op: "read lastActiveIndex", Err: err}
	}
	lastActiveIndex := binary.LittleEndian.Uint64(buf[:8])

	return &clientSession{
		clientID:        clientID,
		ownerID:         ownerID,
		registeredIndex: registeredIndex,
		respondedTo:     respondedTo,
		responses:       responses,
		lastActiveIndex: lastActiveIndex,
	}, nil
}

// MarshalError is returned when session serialization fails.
type MarshalError struct {
	Op  string
	Err error
}

// Error returns a human-readable description of the marshal error.
func (e *MarshalError) Error() string {
	return "session: marshal failed at " + e.Op + ": " + e.Err.Error()
}

// Unwrap returns the underlying error.
func (e *MarshalError) Unwrap() error {
	return e.Err
}

// UnmarshalError is returned when session deserialization fails.
type UnmarshalError struct {
	Op  string
	Err error
}

// Error returns a human-readable description of the unmarshal error.
func (e *UnmarshalError) Error() string {
	return "session: unmarshal failed at " + e.Op + ": " + e.Err.Error()
}

// Unwrap returns the underlying error.
func (e *UnmarshalError) Unwrap() error {
	return e.Err
}
