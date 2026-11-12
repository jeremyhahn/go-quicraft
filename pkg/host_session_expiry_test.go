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

	"github.com/jeremyhahn/go-quicraft/pkg/config"
)

const (
	// sessionExpiryTestEntries is the configured expiry threshold, the floor
	// config.Config.Validate accepts.
	sessionExpiryTestEntries = 100
	// sessionExpiryTestTraffic is the number of entries applied while a
	// session is under test: past two full expiry checkpoints after any
	// registration index.
	sessionExpiryTestTraffic = 250
	// sessionExpiryTestActiveEvery is how often an active session proposes.
	sessionExpiryTestActiveEvery = 50
)

// startSessionExpiryShard starts a single-replica shard 1 on a fresh Host with
// SessionExpiryEntries set to expiry (and the per-owner cap to perOwner when
// non-zero), and waits for it to elect itself.
func startSessionExpiryShard(t *testing.T, expiry, perOwner uint64) *Host {
	t.Helper()
	h := newTestHost(t)
	scfg := testShardConfig(1, 1)
	scfg.SessionExpiryEntries = expiry
	if perOwner != 0 {
		scfg.MaxSessionsPerOwner = perOwner
	}
	members := map[uint64]string{1: "127.0.0.1:5000"}
	if err := h.StartShard(members, false, testCreateFunc, scfg); err != nil {
		t.Fatalf("StartShard failed: %v", err)
	}
	waitForLeader(t, h, 1)
	return h
}

// applyTraffic applies n plain SyncPropose entries to shard 1.
func applyTraffic(ctx context.Context, t *testing.T, h *Host, n int) {
	t.Helper()
	for i := 0; i < n; i++ {
		if _, err := h.SyncPropose(ctx, 1, []byte("traffic")); err != nil {
			t.Fatalf("SyncPropose %d failed: %v", i, err)
		}
	}
}

// requireSessionExpired fails unless err is a *SessionExpiredError that
// errors.Is ErrSessionExpired.
func requireSessionExpired(t *testing.T, what string, err error) {
	t.Helper()
	var se *SessionExpiredError
	if !errors.As(err, &se) {
		t.Fatalf("%s: got %v (%T), want *SessionExpiredError", what, err, err)
	}
	if !errors.Is(err, ErrSessionExpired) {
		t.Fatalf("%s: errors.Is(%v, ErrSessionExpired) = false", what, err)
	}
}

// closeSessionResult proposes CloseSession for s and waits for its completion.
func closeSessionResult(ctx context.Context, t *testing.T, h *Host, s *Session) error {
	t.Helper()
	rs, err := h.CloseSession(ctx, 1, s)
	if err != nil {
		return err
	}
	defer rs.Release()
	_, err = awaitResult(ctx, rs)
	return err
}

// TestHostSessionExpiry_IdleSessionExpires: a session with no activity while
// sessionExpiryTestTraffic entries are applied is reclaimed; a later
// SyncProposeWithSession and a completed CloseSession both report
// *SessionExpiredError.
func TestHostSessionExpiry_IdleSessionExpires(t *testing.T) {
	h := startSessionExpiryShard(t, sessionExpiryTestEntries, 0)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	proposer, err := h.NewSession(ctx, 1)
	if err != nil {
		t.Fatalf("NewSession (proposer) failed: %v", err)
	}
	closer, err := h.NewSession(ctx, 1)
	if err != nil {
		t.Fatalf("NewSession (closer) failed: %v", err)
	}

	applyTraffic(ctx, t, h, sessionExpiryTestTraffic)

	_, err = h.SyncProposeWithSession(ctx, proposer, []byte("late"))
	requireSessionExpired(t, "SyncProposeWithSession after idle span", err)

	requireSessionExpired(t, "CloseSession after idle span", closeSessionResult(ctx, t, h, closer))
}

// TestHostSessionExpiry_ActiveSessionNeverExpires: a session that proposes
// every sessionExpiryTestActiveEvery entries over the same span survives.
func TestHostSessionExpiry_ActiveSessionNeverExpires(t *testing.T) {
	h := startSessionExpiryShard(t, sessionExpiryTestEntries, 0)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	s, err := h.NewSession(ctx, 1)
	if err != nil {
		t.Fatalf("NewSession failed: %v", err)
	}
	for done := 0; done < sessionExpiryTestTraffic; done += sessionExpiryTestActiveEvery {
		if _, err := h.SyncProposeWithSession(ctx, s, []byte("active")); err != nil {
			t.Fatalf("SyncProposeWithSession after %d entries failed: %v", done, err)
		}
		applyTraffic(ctx, t, h, sessionExpiryTestActiveEvery-1)
	}
	if _, err := h.SyncProposeWithSession(ctx, s, []byte("final")); err != nil {
		t.Fatalf("SyncProposeWithSession at end of span failed: %v", err)
	}
	if err := closeSessionResult(ctx, t, h, s); err != nil {
		t.Fatalf("CloseSession of active session failed: %v", err)
	}
}

// TestHostSessionExpiry_DisabledIdleSessionSurvives: with
// config.SessionExpiryDisabled an idle session survives the same traffic.
func TestHostSessionExpiry_DisabledIdleSessionSurvives(t *testing.T) {
	h := startSessionExpiryShard(t, config.SessionExpiryDisabled, 0)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	s, err := h.NewSession(ctx, 1)
	if err != nil {
		t.Fatalf("NewSession failed: %v", err)
	}
	applyTraffic(ctx, t, h, sessionExpiryTestTraffic)
	if _, err := h.SyncProposeWithSession(ctx, s, []byte("late")); err != nil {
		t.Fatalf("SyncProposeWithSession with expiry disabled failed: %v", err)
	}
	if err := closeSessionResult(ctx, t, h, s); err != nil {
		t.Fatalf("CloseSession with expiry disabled failed: %v", err)
	}
}

// TestHostSessionExpiry_IdleShardExpiresNothing: with no traffic, wall-clock
// time alone expires nothing.
func TestHostSessionExpiry_IdleShardExpiresNothing(t *testing.T) {
	h := startSessionExpiryShard(t, sessionExpiryTestEntries, 0)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	s, err := h.NewSession(ctx, 1)
	if err != nil {
		t.Fatalf("NewSession failed: %v", err)
	}
	time.Sleep(2 * time.Second)
	if _, err := h.SyncProposeWithSession(ctx, s, []byte("after-wait")); err != nil {
		t.Fatalf("SyncProposeWithSession after idle wait failed: %v", err)
	}
}

// TestHostSessionExpiry_ExpiredOwnerSessionFreesSlot: with
// MaxSessionsPerOwner = 1, an owner at its cap is refused a second session
// until its first expires, and then is granted one.
func TestHostSessionExpiry_ExpiredOwnerSessionFreesSlot(t *testing.T) {
	h := startSessionExpiryShard(t, sessionExpiryTestEntries, 1)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	const owner = 7
	first, err := h.NewSessionWithOwner(ctx, 1, owner)
	if err != nil {
		t.Fatalf("NewSessionWithOwner (first) failed: %v", err)
	}
	if _, err := h.NewSessionWithOwner(ctx, 1, owner); err == nil {
		t.Fatal("NewSessionWithOwner at the per-owner cap succeeded, want refusal")
	}

	applyTraffic(ctx, t, h, sessionExpiryTestTraffic)

	_, err = h.SyncProposeWithSession(ctx, first, []byte("late"))
	requireSessionExpired(t, "SyncProposeWithSession of expired owner session", err)

	second, err := h.NewSessionWithOwner(ctx, 1, owner)
	if err != nil {
		t.Fatalf("NewSessionWithOwner after expiry failed: %v", err)
	}
	if _, err := h.SyncProposeWithSession(ctx, second, []byte("fresh")); err != nil {
		t.Fatalf("SyncProposeWithSession on replacement session failed: %v", err)
	}
}
