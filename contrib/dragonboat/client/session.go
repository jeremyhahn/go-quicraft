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

// Package client provides dragonboat-compatible client sessions; ClientID, SeriesID and RespondedTo are methods, not fields as in dragonboat.
package client

import (
	"crypto/rand"
	"encoding/binary"
	"sync/atomic"
	"time"

	quicraft "github.com/jeremyhahn/go-quicraft/pkg"
)

// Source is a random number source used for client ID generation.
// This matches the random.Source interface from github.com/lni/goutils/random
// for dragonboat v4 API compatibility.
type Source interface {
	// Uint64 returns the next pseudo-random 64-bit value.
	Uint64() uint64
}

// Session wraps a quicraft Session (swapped atomically on registration) and exposes ShardID as a field.
type Session struct {
	inner atomic.Pointer[quicraft.Session]

	// ShardID is the shard this session belongs to. Exposed as a public
	// field for dragonboat API compatibility.
	ShardID uint64
}

// newSessionFromInner creates a Session from an existing quicraft Session
// pointer, storing it atomically.
func newSessionFromInner(inner *quicraft.Session, shardID uint64) *Session {
	s := &Session{ShardID: shardID}
	s.inner.Store(inner)
	return s
}

// NewSession creates an unregistered session for shardID with a non-zero client ID; a nil rng uses crypto/rand.
func NewSession(shardID uint64, rng Source) *Session {
	clientID := generateClientID(rng)
	return newSessionFromInner(quicraft.NewSession(shardID, clientID), shardID)
}

// generateClientID produces a non-zero random uint64 using the provided
// Source. If rng is nil, crypto/rand is used as a fallback.
func generateClientID(rng Source) uint64 {
	if rng != nil {
		id := rng.Uint64()
		if id != 0 {
			return id
		}
		// Fall through to crypto/rand if rng returned zero.
	}
	// On persistent crypto/rand failure fall back to a unique clock-and-counter ID rather than panicking.
	for attempt := 0; attempt < 8; attempt++ {
		var buf [8]byte
		if _, err := rand.Read(buf[:]); err != nil {
			break
		}
		if id := binary.LittleEndian.Uint64(buf[:]); id != 0 {
			return id
		}
	}
	for {
		id := uint64(time.Now().UnixNano()) ^ (fallbackClientIDCounter.Add(1) * 0x9E3779B97F4A7C15)
		if id != 0 {
			return id
		}
	}
}

// fallbackClientIDCounter backs the non-crypto client-ID fallback used only
// when crypto/rand is persistently unavailable (see generateClientID).
var fallbackClientIDCounter atomic.Uint64

// NewNoOPSession returns a session without at-most-once deduplication; rng is accepted for API compatibility and unused.
func NewNoOPSession(shardID uint64, rng Source) *Session {
	_ = rng // accepted for dragonboat v4 API compatibility
	return newSessionFromInner(quicraft.NewNoOPSession(shardID), shardID)
}

// Inner returns the underlying quicraft Session. This is used internally
// by the adapter when forwarding calls to quicraft's Host methods.
// The load is atomic and safe for concurrent use.
func (s *Session) Inner() *quicraft.Session {
	return s.inner.Load()
}

// SwapInner atomically replaces the underlying quicraft Session pointer.
// This is used by ProposeSession to update the session after registration
// completes without causing torn reads on concurrent goroutines.
func (s *Session) SwapInner(inner *quicraft.Session) {
	s.inner.Store(inner)
}

// IsNoOPSession returns true if this is a no-op session (no at-most-once
// deduplication tracking).
func (s *Session) IsNoOPSession() bool {
	return s.inner.Load().IsNoOPSession()
}

// ClientID returns the unique client identifier for this session.
func (s *Session) ClientID() uint64 {
	return s.inner.Load().ClientID()
}

// SeriesID returns the current series ID. Each proposal increments this
// value to ensure uniqueness within the session.
func (s *Session) SeriesID() uint64 {
	return s.inner.Load().SeriesID()
}

// PrepareForPropose sets the session for normal proposal mode after
// registration completes.
func (s *Session) PrepareForPropose() {
	s.inner.Load().PrepareForPropose()
}

// ProposalCompleted advances the series ID after a proposal is applied.
func (s *Session) ProposalCompleted() {
	s.inner.Load().ProposalCompleted()
}

// PrepareForRegister sets the series to SeriesIDForRegister and clears respondedTo before ProposeSession.
func (s *Session) PrepareForRegister() {
	s.inner.Load().PrepareForRegister()
}

// PrepareForUnregister sets the session state for an unregister proposal.
// The seriesID is set to SeriesIDForUnregister and respondedTo is cleared.
func (s *Session) PrepareForUnregister() {
	s.inner.Load().PrepareForUnregister()
}

// ValidForProposal reports whether the session matches shardID, is not no-op, has a normal series and RespondedTo below SeriesID.
func (s *Session) ValidForProposal(shardID uint64) bool {
	in := s.inner.Load()
	if !in.ValidForProposal(shardID) {
		return false
	}
	return in.RespondedTo() < in.SeriesID()
}

// ValidForSessionOp reports whether the session matches shardID, is not no-op, and its series is the register or unregister sentinel.
func (s *Session) ValidForSessionOp(shardID uint64) bool {
	inner := s.inner.Load()
	if s.ShardID != shardID || inner.IsNoOPSession() {
		return false
	}
	sid := inner.SeriesID()
	return sid == quicraft.SeriesIDForRegister || sid == quicraft.SeriesIDForUnregister
}

// WrapSession wraps a quicraft Session in a dragonboat-compatible Session.
// This is used internally by the adapter to wrap sessions returned from
// quicraft's Host methods.
func WrapSession(inner *quicraft.Session) *Session {
	return newSessionFromInner(inner, inner.ShardID())
}
