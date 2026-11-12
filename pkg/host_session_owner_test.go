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
	"context"
	"errors"
	"testing"
	"time"
)

// requireOwnerCapRefusal asserts err is the documented per-owner cap refusal:
// a *SessionError wrapping ErrInvalidOperation (docs/client-sessions/api.md,
// "Register a session").
func requireOwnerCapRefusal(t *testing.T, what string, s *Session, err error) {
	t.Helper()
	if err == nil {
		t.Fatalf("%s: err = nil (session %v), want a *SessionError wrapping "+
			"ErrInvalidOperation: the owner already holds MaxSessionsPerOwner "+
			"sessions", what, s)
	}
	var se *SessionError
	if !errors.As(err, &se) {
		t.Fatalf("%s: err = %v (%T), want a *SessionError", what, err, err)
	}
	if !errors.Is(err, ErrInvalidOperation) {
		t.Fatalf("%s: err = %v, want it to wrap ErrInvalidOperation", what, err)
	}
}

// TestHostSessionOwnerCap drives the per-owner session cap through the public
// Host API: NewSessionWithOwner and RegisterSession hold each owner to
// MaxSessionsPerOwner, other owners and owner 0 are unaffected, and
// CloseSession frees the owner's slot.
func TestHostSessionOwnerCap(t *testing.T) {
	const (
		ownerA = uint64(0xA)
		ownerB = uint64(0xB)
	)
	h := newTestHost(t)
	scfg := testShardConfig(1, 1)
	scfg.MaxSessionsPerOwner = 2
	if err := h.StartShard(map[uint64]string{1: "127.0.0.1:5000"}, false,
		testCreateFunc, scfg); err != nil {
		t.Fatalf("StartShard failed: %v", err)
	}
	waitForLeader(t, h, 1)

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	// Owner A registers up to its cap.
	var held []*Session
	for i := 0; i < 2; i++ {
		s, err := h.NewSessionWithOwner(ctx, 1, ownerA)
		if err != nil {
			t.Fatalf("NewSessionWithOwner(A) #%d: %v, want success under the cap", i+1, err)
		}
		if got := s.OwnerID(); got != ownerA {
			t.Fatalf("NewSessionWithOwner(A) #%d: OwnerID() = %#x, want %#x", i+1, got, ownerA)
		}
		held = append(held, s)
	}

	// The third A session is refused.
	s, err := h.NewSessionWithOwner(ctx, 1, ownerA)
	requireOwnerCapRefusal(t, "third NewSessionWithOwner(A)", s, err)

	// Another owner is unaffected.
	sb, err := h.NewSessionWithOwner(ctx, 1, ownerB)
	if err != nil {
		t.Fatalf("NewSessionWithOwner(B): %v, want success: owner A's cap must "+
			"not limit owner B", err)
	}
	if got := sb.OwnerID(); got != ownerB {
		t.Fatalf("NewSessionWithOwner(B): OwnerID() = %#x, want %#x", got, ownerB)
	}

	// Owner 0 is never counted or capped.
	for i := 0; i < 3; i++ {
		var s0 *Session
		if i == 0 {
			s0, err = h.NewSession(ctx, 1)
		} else {
			s0, err = h.NewSessionWithOwner(ctx, 1, 0)
		}
		if err != nil {
			t.Fatalf("owner-0 session #%d: %v, want success: owner 0 is never capped", i+1, err)
		}
		if got := s0.OwnerID(); got != 0 {
			t.Fatalf("owner-0 session #%d: OwnerID() = %#x, want 0", i+1, got)
		}
	}

	// RegisterSession of a session built with NewSessionWithOwner is held to
	// the same cap.
	pre := NewSessionWithOwner(1, 0x842A0001, ownerA)
	if got := pre.OwnerID(); got != ownerA {
		t.Fatalf("NewSessionWithOwner(1, id, A).OwnerID() = %#x, want %#x", got, ownerA)
	}
	rs, err := h.RegisterSession(ctx, pre)
	requireOwnerCapRefusal(t, "RegisterSession(A) at the cap", rs, err)

	// CloseSession of one A session frees its slot once it completes.
	req, err := h.CloseSession(ctx, 1, held[0])
	if err != nil {
		t.Fatalf("CloseSession(A #1): %v", err)
	}
	_, err = awaitResult(ctx, req)
	req.Release()
	if err != nil {
		t.Fatalf("CloseSession(A #1) result: %v", err)
	}

	sa, err := h.NewSessionWithOwner(ctx, 1, ownerA)
	if err != nil {
		t.Fatalf("NewSessionWithOwner(A) after CloseSession: %v, want success: "+
			"CloseSession must release the owner's slot", err)
	}
	if got := sa.OwnerID(); got != ownerA {
		t.Fatalf("NewSessionWithOwner(A) after CloseSession: OwnerID() = %#x, want %#x", got, ownerA)
	}

	// A is at its cap again; free a slot and register the pre-built session.
	s, err = h.NewSessionWithOwner(ctx, 1, ownerA)
	requireOwnerCapRefusal(t, "NewSessionWithOwner(A) at the cap again", s, err)

	req, err = h.CloseSession(ctx, 1, held[1])
	if err != nil {
		t.Fatalf("CloseSession(A #2): %v", err)
	}
	_, err = awaitResult(ctx, req)
	req.Release()
	if err != nil {
		t.Fatalf("CloseSession(A #2) result: %v", err)
	}
	pre2 := NewSessionWithOwner(1, 0x842A0002, ownerA)
	if _, err := h.RegisterSession(ctx, pre2); err != nil {
		t.Fatalf("RegisterSession(A) after CloseSession: %v, want success: "+
			"CloseSession must release the owner's slot", err)
	}
}
