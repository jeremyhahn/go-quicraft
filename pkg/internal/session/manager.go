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

// Package session provides client sessions and cached results for at-most-once delivery; session data is included in snapshots.
package session

import (
	"encoding/binary"
	"io"
	"slices"

	"github.com/jeremyhahn/go-quicraft/pkg/sm"
)

// Manager tracks client sessions for one shard; it is not safe for concurrent use.
type Manager struct {
	sessions     map[uint64]*clientSession
	maxTotal     uint64 // maximum total sessions per shard
	maxResponses uint64 // maximum cached responses per session
	maxPerOwner  uint64 // maximum concurrent sessions per owner; 0 disables

	// ownerCounts is the derived per-owner live session count; every sessions mutation updates it, and the zero owner is never counted.
	ownerCounts map[uint64]uint64
}

// NewManager creates a manager with the given limits; zero maxTotal or maxResponses means the default, zero maxPerOwner disables the cap.
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

// SetLimits replaces the limits with the shard's replicated values; zero maxTotal or maxResponses is ignored, and existing sessions are kept. Apply goroutine only.
func (m *Manager) SetLimits(maxTotal, maxResponses, maxPerOwner uint64) {
	if maxTotal != 0 {
		m.maxTotal = maxTotal
	}
	if maxResponses != 0 {
		m.maxResponses = maxResponses
	}
	m.maxPerOwner = maxPerOwner
}

// Register creates a session for clientID at index, owned by ownerID (0 is uncapped); it returns ErrSessionAlreadyExists or ErrSessionLimitExceeded.
func (m *Manager) Register(clientID, ownerID, index uint64) error {
	if _, exists := m.sessions[clientID]; exists {
		return ErrSessionAlreadyExists
	}
	if uint64(len(m.sessions)) >= m.maxTotal {
		return ErrSessionLimitExceeded
	}
	// Check the per-owner gate after the shard-wide one, and mutate only when both pass.
	if m.ownerCapped(ownerID) {
		return ErrSessionLimitExceeded
	}
	m.sessions[clientID] = newClientSession(clientID, ownerID, index)
	if ownerID != noOwnerID {
		m.ownerCounts[ownerID]++
	}
	return nil
}

// ownerCapped reports whether ownerID holds the per-owner maximum; an unset owner or zero cap is never capped.
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

// CheckDuplicate reports whether clientID's seriesID was already applied, returning the cached result, ErrSessionNotFound, or ErrResponseEvicted (treat as SESSION_EXPIRED).
func (m *Manager) CheckDuplicate(clientID, seriesID, index uint64) (sm.Result, bool, error) {
	sess, exists := m.sessions[clientID]
	if !exists {
		return sm.Result{}, false, ErrSessionNotFound
	}
	// An entry below the index that created this incarnation belongs to a previous one; answer it as expired.
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

// CanRecord reports, without mutating, whether a new result for clientID fits the response cache after eviction, counting pending admissions in this batch.
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

// RecordResult caches result, evicts responses at or below respondedTo and advances it; it returns ErrSessionNotFound or ErrResponseLimitExceeded.
func (m *Manager) RecordResult(clientID, seriesID, respondedTo, index uint64, result sm.Result) error {
	sess, exists := m.sessions[clientID]
	if !exists {
		return ErrSessionNotFound
	}
	if respondedTo > sess.respondedTo {
		sess.respondedTo = respondedTo
	}
	sess.evictUpTo(sess.respondedTo)
	// Advance the activity marker before the limit check so a full session is not seen as idle.
	sess.touchIndex(index)
	if uint64(len(sess.responses)) >= m.maxResponses {
		return ErrResponseLimitExceeded
	}
	sess.responses[seriesID] = result
	return nil
}

// ExpireSessions removes up to maxExpire sessions idle for more than inactivityThreshold entries at currentIndex and returns their client IDs.
func (m *Manager) ExpireSessions(currentIndex, inactivityThreshold uint64, maxExpire int) []uint64 {
	if maxExpire <= 0 || inactivityThreshold == 0 {
		return nil
	}

	// Sort candidates before applying maxExpire so every replica expires the same set.
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

	// Delete in order, releasing each session's owner slot.
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

// MarshalTo writes [numSessions:4] then per session [clientID:8][ownerID:8][registeredIndex:8][respondedTo:8][numResponses:4][{seriesID:8, value:8, dataLen:4, data:N}...][lastActiveIndex:8]; ownerCounts is rebuilt on load.
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

	// ownerID, kept so the per-owner cap survives a restore.
	binary.LittleEndian.PutUint64(buf[:8], sess.ownerID)
	if _, err := w.Write(buf[:8]); err != nil {
		return &MarshalError{Op: "write ownerID", Err: err}
	}

	// registeredIndex, kept so the incarnation floor survives a restore.
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

// UnmarshalFrom replaces all sessions from r, bounding sessions by maxTotal, responses by maxResponses and data by maxResponseDataSize (64 MiB).
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

	// Parse into a staging map and swap it in only after the whole stream parses.
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
	// Rebuild the owner index from the decoded table and swap it in with sessions.
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
