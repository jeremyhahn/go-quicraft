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

// startOwnerCapShard starts a single-voter shard whose MaxSessionsPerOwner
// is 2 and waits for it to elect itself.
func startOwnerCapShard(t *testing.T) *Host {
	t.Helper()
	h := newTestHost(t)
	scfg := testShardConfig(1, 1)
	scfg.MaxSessionsPerOwner = 2
	if err := h.StartShard(map[uint64]string{1: "127.0.0.1:5000"}, false,
		testCreateFunc, scfg); err != nil {
		t.Fatalf("StartShard failed: %v", err)
	}
	waitForLeader(t, h, 1)
	return h
}

// requireOwnerCapRefusal asserts err is the documented refusal of a session
// registration past the owner's cap.
func requireOwnerCapRefusal(t *testing.T, what string, err error) {
	t.Helper()
	if err == nil {
		t.Fatalf("%s succeeded; want a refusal: the owner already holds "+
			"MaxSessionsPerOwner (2) sessions", what)
	}
	var se *SessionError
	if !errors.As(err, &se) {
		t.Fatalf("%s error = %v (%T); want *SessionError", what, err, err)
	}
	if se.Op != "register" {
		t.Errorf("%s SessionError.Op = %q, want \"register\"", what, se.Op)
	}
	if !errors.Is(err, ErrInvalidOperation) {
		t.Errorf("%s error = %v; want errors.Is ErrInvalidOperation", what, err)
	}
}

func closeOwnerCapSession(ctx context.Context, t *testing.T, h *Host, s *Session) {
	t.Helper()
	rs, err := h.CloseSession(ctx, 1, s)
	if err != nil {
		t.Fatalf("CloseSession failed: %v", err)
	}
	defer rs.Release()
	if _, err := awaitResult(ctx, rs); err != nil {
		t.Fatalf("CloseSession result: %v", err)
	}
}

func TestSessionOwnerCap_NewSessionWithOwnerEnforcesCapAndCloseFreesSlot(t *testing.T) {
	h := startOwnerCapShard(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// (a) Two sessions for owner 7 fit under the cap of 2.
	var owned [2]*Session
	for i := range owned {
		s, err := h.NewSessionWithOwner(ctx, 1, 7)
		if err != nil {
			t.Fatalf("NewSessionWithOwner(owner 7) #%d failed: %v", i+1, err)
		}
		if got := s.OwnerID(); got != 7 {
			t.Fatalf("session #%d OwnerID() = %d, want 7", i+1, got)
		}
		owned[i] = s
	}

	// (b) A third for owner 7 is refused.
	_, err := h.NewSessionWithOwner(ctx, 1, 7)
	requireOwnerCapRefusal(t, "third NewSessionWithOwner(owner 7)", err)

	// (c) The cap is per owner: owner 8 and owner-less sessions still register.
	for i := 0; i < 3; i++ {
		s, err := h.NewSessionWithOwner(ctx, 1, 8)
		if i < 2 {
			if err != nil {
				t.Fatalf("NewSessionWithOwner(owner 8) #%d failed: %v", i+1, err)
			}
			if got := s.OwnerID(); got != 8 {
				t.Fatalf("owner-8 session OwnerID() = %d, want 8", got)
			}
		} else {
			requireOwnerCapRefusal(t, "third NewSessionWithOwner(owner 8)", err)
		}
	}
	for i := 0; i < 4; i++ {
		s, err := h.NewSession(ctx, 1)
		if err != nil {
			t.Fatalf("NewSession (owner 0) #%d failed: %v; a session with no "+
				"owner is never counted against MaxSessionsPerOwner", i+1, err)
		}
		if got := s.OwnerID(); got != 0 {
			t.Fatalf("NewSession OwnerID() = %d, want 0", got)
		}
	}

	// (d) Closing one owner-7 session frees its slot.
	closeOwnerCapSession(ctx, t, h, owned[0])
	again, err := h.NewSessionWithOwner(ctx, 1, 7)
	if err != nil {
		t.Fatalf("NewSessionWithOwner(owner 7) after CloseSession failed: %v; "+
			"CloseSession must release the owner's slot", err)
	}
	if got := again.OwnerID(); got != 7 {
		t.Fatalf("reopened session OwnerID() = %d, want 7", got)
	}
	_, err = h.NewSessionWithOwner(ctx, 1, 7)
	requireOwnerCapRefusal(t, "NewSessionWithOwner(owner 7) after refilling", err)
}

func TestSessionOwnerCap_RegisterSessionCountsAgainstSameCap(t *testing.T) {
	h := startOwnerCapShard(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// (e) A caller-built owner-7 session counts against the same cap as the
	// host-built ones.
	built := NewSessionWithOwner(1, 0xC0FFEE, 7)
	if got := built.OwnerID(); got != 7 {
		t.Fatalf("NewSessionWithOwner(1, client, 7).OwnerID() = %d, want 7", got)
	}
	registered, err := h.RegisterSession(ctx, built)
	if err != nil {
		t.Fatalf("RegisterSession(owner 7) failed: %v", err)
	}
	if got := registered.OwnerID(); got != 7 {
		t.Fatalf("registered session OwnerID() = %d, want 7", got)
	}
	if _, err := h.NewSessionWithOwner(ctx, 1, 7); err != nil {
		t.Fatalf("second owner-7 session failed: %v", err)
	}
	_, err = h.NewSessionWithOwner(ctx, 1, 7)
	requireOwnerCapRefusal(t, "NewSessionWithOwner(owner 7) after RegisterSession", err)

	// The cap refuses a caller-built session the same way.
	_, err = h.RegisterSession(ctx, NewSessionWithOwner(1, 0xBEEF, 7))
	requireOwnerCapRefusal(t, "RegisterSession(owner 7) past the cap", err)

	// Closing the registered one frees a slot for a caller-built session.
	closeOwnerCapSession(ctx, t, h, registered)
	if _, err := h.RegisterSession(ctx, NewSessionWithOwner(1, 0xBEEF, 7)); err != nil {
		t.Fatalf("RegisterSession(owner 7) after CloseSession failed: %v", err)
	}
}
