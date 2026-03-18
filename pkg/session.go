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

package quicraft

import (
	"math"
	"sync/atomic"
)

// Session operation types used to mark the session's current intent.
// These are internal constants used by PrepareForRegister and
// PrepareForUnregister to set the seriesID to a well-known value
// that the session manager recognizes. Values match dragonboat's
// client.SeriesIDForRegister / SeriesIDForUnregister.
const (
	// noOPClientID is the fixed client ID for no-op sessions.
	// Proposals with this client ID bypass session deduplication.
	noOPClientID uint64 = 0

	// SeriesIDForRegister is the sentinel series ID used for registering
	// a new client session through Raft consensus.
	SeriesIDForRegister uint64 = math.MaxUint64 - 1

	// SeriesIDForUnregister is the sentinel series ID used for
	// unregistering a client session through Raft consensus.
	SeriesIDForUnregister uint64 = math.MaxUint64

	// SeriesIDFirstProposal is the first series ID used for normal
	// proposals after a session is registered.
	SeriesIDFirstProposal uint64 = 1
)

// Session represents a client session for at-most-once proposal semantics.
// Sessions are created via Host.GetNewSession and closed via Host.CloseSession.
//
// The seriesID field advances monotonically with each proposal. The session
// manager uses (clientID, seriesID) to detect and deduplicate retransmissions.
//
// Fields are unexported to prevent direct mutation which would break
// deduplication invariants.
type Session struct {
	shardID     uint64
	clientID    uint64
	seriesID    atomic.Uint64
	respondedTo atomic.Uint64
}

// newSession creates a Session for the given shard and client.
func newSession(shardID, clientID uint64) *Session {
	return &Session{
		shardID:  shardID,
		clientID: clientID,
	}
}

// NewSession creates a tracked Session for the given shard and client ID.
// The clientID must not be zero; zero is reserved for no-op sessions.
// The session is created in an unregistered state. The caller must register
// it through the Raft consensus group before using it for proposals.
func NewSession(shardID, clientID uint64) *Session {
	return &Session{
		shardID:  shardID,
		clientID: clientID,
	}
}

// NewNoOPSession creates a no-op session for the given shard. No-op sessions
// use a fixed clientID of 0 and bypass session deduplication. Use for
// idempotent operations that do not require at-most-once delivery.
func NewNoOPSession(shardID uint64) *Session {
	return &Session{
		shardID:  shardID,
		clientID: noOPClientID,
	}
}

// ShardID returns the shard this session belongs to.
func (s *Session) ShardID() uint64 {
	return s.shardID
}

// ClientID returns the unique client identifier for this session.
func (s *Session) ClientID() uint64 {
	return s.clientID
}

// SeriesID returns the current series ID. Each proposal increments this
// value to ensure uniqueness within the session.
func (s *Session) SeriesID() uint64 {
	return s.seriesID.Load()
}

// RespondedTo returns the highest series ID that has been acknowledged.
func (s *Session) RespondedTo() uint64 {
	return s.respondedTo.Load()
}

// PrepareForPropose sets the session for normal proposal mode. After
// registration completes, the caller invokes this to advance the seriesID
// to SeriesIDFirstProposal so subsequent proposals use the normal range.
//
// Thread safety: seriesID uses atomic.Uint64 to support concurrent use.
// However, concurrent proposals on the same session are discouraged because
// responses may arrive out of order.
func (s *Session) PrepareForPropose() {
	s.seriesID.Store(SeriesIDFirstProposal)
}

// ProposalCompleted advances the series ID after a proposal is applied.
// Called by the caller after Result() returns for a normal proposal.
//
// Uses a CAS loop to atomically claim the current seriesID, store it
// as respondedTo, and increment seriesID. Without this, concurrent
// callers could both read the same seriesID, both store the same
// respondedTo value, and double-increment seriesID (skipping a value).
func (s *Session) ProposalCompleted() {
	for {
		current := s.seriesID.Load()
		if s.seriesID.CompareAndSwap(current, current+1) {
			s.respondedTo.Store(current)
			return
		}
	}
}

// PrepareForRegister sets the session state for a register proposal.
// The seriesID is set to SeriesIDForRegister and respondedTo is cleared.
func (s *Session) PrepareForRegister() {
	s.seriesID.Store(SeriesIDForRegister)
	s.respondedTo.Store(0)
}

// PrepareForUnregister sets the session state for an unregister proposal.
// The seriesID is set to SeriesIDForUnregister and respondedTo is cleared.
func (s *Session) PrepareForUnregister() {
	s.seriesID.Store(SeriesIDForUnregister)
	s.respondedTo.Store(0)
}

// ValidForSessionOp returns true if the session is valid for a session
// register or unregister operation on the given shard.
func (s *Session) ValidForSessionOp(shardID uint64) bool {
	return s.shardID == shardID && s.clientID != noOPClientID
}

// ValidForProposal returns true if the session is in a valid state for
// submitting a normal (non-register, non-unregister) proposal on the given
// shard. Specifically:
//   - The shard must match the session's shard
//   - The session must not be a no-op session (clientID != 0)
//   - The seriesID must be in the normal proposal range (not register/unregister)
//   - The respondedTo must not exceed seriesID (invariant: respondedTo <= seriesID)
func (s *Session) ValidForProposal(shardID uint64) bool {
	if s.shardID != shardID || s.clientID == noOPClientID {
		return false
	}
	sid := s.seriesID.Load()
	if sid == SeriesIDForRegister || sid == SeriesIDForUnregister {
		return false
	}
	return s.respondedTo.Load() <= sid
}

// SetRespondedTo updates the highest acknowledged series ID. Called
// internally when a response is received for a proposal.
func (s *Session) SetRespondedTo(seriesID uint64) {
	for {
		current := s.respondedTo.Load()
		if seriesID <= current {
			return
		}
		if s.respondedTo.CompareAndSwap(current, seriesID) {
			return
		}
	}
}

// IsNoOPSession returns true if this is a no-op session (clientID == 0).
func (s *Session) IsNoOPSession() bool {
	return s.clientID == noOPClientID
}
