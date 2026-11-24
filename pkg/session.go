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

// Session series IDs for register and unregister, matching dragonboat's values.
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

// Session is a client session for at-most-once proposals, created by Host.NewSession and closed by Host.CloseSession.
type Session struct {
	shardID  uint64
	clientID uint64
	// ownerID is the declared owning principal, stamped into the register entry; zero means none.
	ownerID     uint64
	seriesID    atomic.Uint64
	respondedTo atomic.Uint64

	// inFlight is the series reserved by the current proposal, or 0; one proposal at a time.
	inFlight atomic.Uint64
}

// beginProposal reserves series for a proposal and reports whether it did.
// It fails when another proposal is in flight on this session.
func (s *Session) beginProposal(series uint64) bool {
	return s.inFlight.CompareAndSwap(0, series)
}

// beginCurrentProposal reserves and returns the current series, re-checking it after the claim.
func (s *Session) beginCurrentProposal() (uint64, bool) {
	for {
		series := s.seriesID.Load()
		if !s.beginProposal(series) {
			return 0, false
		}
		if s.seriesID.Load() == series {
			return series, true
		}
		s.endProposal(series)
	}
}

// endProposal releases the reservation held for series. Releasing a series
// that is not the one reserved is a no-op: the reservation has already moved
// on, and this call belongs to the request that held it.
func (s *Session) endProposal(series uint64) {
	s.inFlight.CompareAndSwap(series, 0)
}

// newSession creates a Session for the given shard and client, owned by
// ownerID (zero for none).
func newSession(shardID, clientID, ownerID uint64) *Session {
	return &Session{
		shardID:  shardID,
		clientID: clientID,
		ownerID:  ownerID,
	}
}

// NewSession creates an unregistered Session for shardID and a non-zero clientID with no owner.
func NewSession(shardID, clientID uint64) *Session {
	return &Session{
		shardID:  shardID,
		clientID: clientID,
	}
}

// NewSessionWithOwner creates a Session counted against the per-owner cap for ownerID, which must come from the embedder's authentication; zero is NewSession.
func NewSessionWithOwner(shardID, clientID, ownerID uint64) *Session {
	return &Session{
		shardID:  shardID,
		clientID: clientID,
		ownerID:  ownerID,
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

// OwnerID returns the principal the embedder declared owns this session, or
// zero if none was declared. See NewSessionWithOwner.
func (s *Session) OwnerID() uint64 {
	return s.ownerID
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

// PrepareForPropose moves the series to SeriesIDFirstProposal after registration; a session serves one proposal at a time.
func (s *Session) PrepareForPropose() {
	s.seriesID.Store(SeriesIDFirstProposal)
}

// ProposalCompleted advances the series, raises respondedTo monotonically and releases the reservation.
func (s *Session) ProposalCompleted() {
	for {
		current := s.seriesID.Load()
		if s.seriesID.CompareAndSwap(current, current+1) {
			for {
				r := s.respondedTo.Load()
				if current <= r || s.respondedTo.CompareAndSwap(r, current) {
					break
				}
			}
			s.endProposal(current)
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

// ValidForProposal reports whether the session can propose on shardID: same shard, non-zero client, series in the normal range, respondedTo <= seriesID.
func (s *Session) ValidForProposal(shardID uint64) bool {
	if s.shardID != shardID || s.clientID == noOPClientID {
		return false
	}
	sid := s.seriesID.Load()
	if sid < SeriesIDFirstProposal || sid == SeriesIDForRegister || sid == SeriesIDForUnregister {
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
