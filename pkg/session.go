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
// Sessions are created via Host.NewSession and closed via Host.CloseSession.
//
// The seriesID field advances monotonically with each proposal. The session
// manager uses (clientID, seriesID) to detect and deduplicate retransmissions.
//
// Fields are unexported to prevent direct mutation which would break
// deduplication invariants.
type Session struct {
	shardID  uint64
	clientID uint64
	// ownerID is the principal the embedder declares owns this session. It is
	// read once, at registration, and stamped into the replicated register
	// entry (proto.Entry.OwnerID) where the shard's per-owner session cap is
	// evaluated against it. It is immutable after construction for the same
	// reason clientID is: changing it would not migrate the session between
	// owners, it would only make the local value disagree with what the log
	// committed. Zero means no owner was declared.
	ownerID     uint64
	seriesID    atomic.Uint64
	respondedTo atomic.Uint64

	// inFlight holds the series number of the proposal currently reserved
	// on this session, or 0 when none is. A session carries ONE series at a
	// time: ProposeWithSession stamps the entry with the current seriesID
	// and the state machine treats every entry with the same (clientID,
	// seriesID) as a retransmission of one command, applying the first and
	// answering the rest with its result. Without the reservation two
	// concurrent proposals both reported success while one of them was
	// silently discarded as a duplicate. beginProposal claims the series,
	// endProposal releases exactly that series, and a release for any other
	// series is a no-op, so a late Release of a completed request cannot
	// clear the reservation of the proposal that followed it.
	inFlight atomic.Uint64
}

// beginProposal reserves series for a proposal and reports whether it did.
// It fails when another proposal is in flight on this session.
func (s *Session) beginProposal(series uint64) bool {
	return s.inFlight.CompareAndSwap(0, series)
}

// beginCurrentProposal reserves the session's CURRENT series and returns it.
// Reading the series and claiming it are two steps, and ProposalCompleted
// can run between them — it advances the series and only then releases the
// reservation — so a claim that succeeded may hold a series the session has
// moved past. Such a proposal would carry the previous command's series and
// be answered as its retransmission without being applied. The claim is
// therefore re-checked against the series after it is taken and, if the
// session has moved on, released and taken again for the new series.
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

// NewSession creates a tracked Session for the given shard and client ID.
// The clientID must not be zero; zero is reserved for no-op sessions.
// The session is created in an unregistered state. The caller must register
// it through the Raft consensus group before using it for proposals.
//
// The session declares no owner and is therefore exempt from the shard's
// per-owner session cap. Use NewSessionWithOwner to declare one.
func NewSession(shardID, clientID uint64) *Session {
	return &Session{
		shardID:  shardID,
		clientID: clientID,
	}
}

// NewSessionWithOwner creates a tracked Session that declares ownerID as the
// principal holding it, so the session counts against — and is limited by —
// the shard's replicated per-owner cap
// (config.Config.MaxSessionsPerOwner).
//
// The owner is whatever the EMBEDDER decides identifies a principal: a tenant
// ID, an account number, a hash of an authenticated subject. This library
// cannot derive it, because it authenticates nodes rather than clients and a
// clientID is eight bytes the caller chooses. It follows that the cap is only
// as trustworthy as the embedder's own authentication: a caller free to pick
// any ownerID is free to evade the cap, so the value must come from something
// the embedder verified, not from the client.
//
// An ownerID of zero is identical to NewSession — no owner declared, no cap
// applied.
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

// PrepareForPropose sets the session for normal proposal mode. After
// registration completes, the caller invokes this to advance the seriesID
// to SeriesIDFirstProposal so subsequent proposals use the normal range.
//
// Thread safety: the fields are atomics, so the Session may be read from
// any goroutine, but it serves ONE proposal at a time. ProposeWithSession
// refuses a second concurrent proposal with ErrSessionBusy — see inFlight —
// because both would carry the same series number and the state machine
// would apply only one of them.
func (s *Session) PrepareForPropose() {
	s.seriesID.Store(SeriesIDFirstProposal)
}

// ProposalCompleted advances the series ID after a proposal is applied.
// Called by the caller after Result() returns for a normal proposal.
//
// Uses a CAS loop to atomically claim the current seriesID and increment
// it. respondedTo is then advanced MONOTONICALLY to the claimed value:
// a plain Store would let a goroutine that won an earlier CAS but ran its
// store later regress respondedTo below a higher completion's value,
// breaking the respondedTo == seriesID-1 invariant under concurrent
// completions. The monotonic advance mirrors SetRespondedTo and keeps
// respondedTo pinned to the highest completed series.
//
// It also releases the reservation ProposeWithSession took for the completed
// series, so the next proposal on this session is admitted.
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

// ValidForProposal returns true if the session is in a valid state for
// submitting a normal (non-register, non-unregister) proposal on the given
// shard. Specifically:
//   - The shard must match the session's shard
//   - The session must not be a no-op session (clientID != 0)
//   - The seriesID must be in the normal proposal range: at least
//     SeriesIDFirstProposal (a session that was never registered, or whose
//     registration has not been followed by PrepareForPropose, still carries
//     0) and neither of the register/unregister sentinels
//   - The respondedTo must not exceed seriesID (invariant: respondedTo <= seriesID)
//
// Series 0 is refused for the reservation's sake as much as the protocol's:
// inFlight uses 0 for "nothing reserved", so a session at series 0 could
// admit two concurrent proposals, both stamped 0 — the double admission
// ErrSessionBusy exists to prevent.
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
