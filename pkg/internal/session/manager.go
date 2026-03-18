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
}

// NewManager creates a session manager with the given limits.
// maxTotal controls the total number of concurrent sessions per shard.
// maxResponses controls the maximum cached responses per session.
func NewManager(maxTotal, maxResponses uint64) *Manager {
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
	}
}

// Register creates a new session for the given client ID. The index
// parameter is the log index of the register entry, used as the
// initial activity marker for deterministic expiry. Returns
// ErrSessionLimitExceeded if the total session limit is reached, or
// ErrSessionAlreadyExists if the client is already registered.
func (m *Manager) Register(clientID uint64, index uint64) error {
	if _, exists := m.sessions[clientID]; exists {
		return ErrSessionAlreadyExists
	}
	if uint64(len(m.sessions)) >= m.maxTotal {
		return ErrSessionLimitExceeded
	}
	m.sessions[clientID] = newClientSession(clientID, index)
	return nil
}

// Unregister removes the session for the given client ID. Returns
// ErrSessionNotFound if the client is not registered.
func (m *Manager) Unregister(clientID uint64) error {
	if _, exists := m.sessions[clientID]; !exists {
		return ErrSessionNotFound
	}
	delete(m.sessions, clientID)
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
	if uint64(len(sess.responses)) >= m.maxResponses {
		return ErrResponseLimitExceeded
	}
	sess.responses[seriesID] = result
	sess.touchIndex(index)
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

	// Delete in deterministic order.
	for _, clientID := range candidates {
		delete(m.sessions, clientID)
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
// Each session: [clientID:8][respondedTo:8][numResponses:4]
//
//	[{seriesID:8, value:8, dataLen:4, data:N}...][lastActiveIndex:8]
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

	// Clear existing sessions and pre-allocate.
	m.sessions = make(map[uint64]*clientSession, numSessions)

	for i := uint32(0); i < numSessions; i++ {
		sess, err := unmarshalSession(r, m.maxResponses)
		if err != nil {
			return err
		}
		m.sessions[sess.clientID] = sess
	}
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
