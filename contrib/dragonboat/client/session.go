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

// Package client provides dragonboat-compatible client session types that
// wrap quicraft's Session for at-most-once proposal semantics.
package client

import (
	"crypto/rand"
	"encoding/binary"
	"sync/atomic"

	quicraft "github.com/jeremyhahn/go-quicraft/pkg"
)

// Source is a random number source used for client ID generation.
// This matches the random.Source interface from github.com/lni/goutils/random
// for dragonboat v4 API compatibility.
type Source interface {
	Uint64() uint64
}

// Session represents a client session for at-most-once proposal semantics.
// It wraps quicraft's Session and exposes the ShardID as a public field
// for dragonboat API compatibility.
//
// The inner session pointer is stored via atomic.Pointer to allow lock-free
// concurrent reads while ProposeSession atomically swaps the underlying
// quicraft session after registration.
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

// NewSession creates a new tracked client session for the specified shard.
// The rng parameter provides the random source for client ID generation,
// matching dragonboat v4's API signature. If rng is nil, a cryptographically
// secure random source is used. The generated client ID is guaranteed to be
// non-zero.
//
// The session is NOT yet registered. The caller must register it through
// NodeHost.SyncGetSession or by proposing a session registration before
// using it for normal proposals.
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
	for {
		var buf [8]byte
		if _, err := rand.Read(buf[:]); err != nil {
			panic("client: failed to generate random client ID: " + err.Error())
		}
		id := binary.LittleEndian.Uint64(buf[:])
		if id != 0 {
			return id
		}
	}
}

// NewNoOPSession returns a no-op session that bypasses at-most-once
// deduplication. Use for idempotent operations that do not require
// exactly-once delivery guarantees. The rng parameter is accepted for
// dragonboat v4 API compatibility but is not used, as quicraft generates
// client IDs internally.
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

// PrepareForUnregister sets the session state for an unregister proposal.
// The seriesID is set to SeriesIDForUnregister and respondedTo is cleared.
func (s *Session) PrepareForUnregister() {
	s.inner.Load().PrepareForUnregister()
}

// ValidForProposal returns true if the session is valid for submitting
// normal proposals to the specified shard. This enforces:
//   - ShardID must match
//   - ClientID must not be 0 (not a no-op session)
//   - SeriesID must not be a register/unregister sentinel
//   - RespondedTo must not exceed SeriesID
func (s *Session) ValidForProposal(shardID uint64) bool {
	return s.inner.Load().ValidForProposal(shardID)
}

// ValidForSessionOp returns true if the session is valid for session
// operations (registration/unregistration) on the specified shard. This
// enforces:
//   - ShardID must match
//   - ClientID must not be 0 (not a no-op session)
//   - SeriesID must be exactly SeriesIDForRegister or SeriesIDForUnregister
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
