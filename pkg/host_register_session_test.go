// Copyright 2026 Jeremy Hahn
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
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
	"io"
	"sync"
	"testing"
	"time"

	"github.com/jeremyhahn/go-quicraft/pkg/config"
	"github.com/jeremyhahn/go-quicraft/pkg/sm"
)

// applyCountingSM counts how many times each command reaches Update, so a
// test can assert that a session-managed proposal is applied exactly once.
type applyCountingSM struct {
	mu     sync.Mutex
	counts map[string]int
}

func (m *applyCountingSM) Update(_ context.Context, entries []sm.Entry, results []sm.Result) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	for i := range entries {
		m.counts[string(entries[i].Cmd)]++
		results[i] = sm.Result{Value: entries[i].Index}
	}
	return nil
}

func (m *applyCountingSM) Lookup(_ context.Context, query interface{}) (interface{}, error) {
	return query, nil
}

func (m *applyCountingSM) SaveSnapshot(_ context.Context, _ io.Writer, _ <-chan struct{}) error {
	return nil
}

func (m *applyCountingSM) RecoverFromSnapshot(_ context.Context, _ io.Reader, _ <-chan struct{}) error {
	return nil
}

func (m *applyCountingSM) Close(_ context.Context) error { return nil }

func (m *applyCountingSM) count(cmd string) int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.counts[cmd]
}

var _ sm.StateMachine = (*applyCountingSM)(nil)

// startRegisterSessionShard starts shard 1 on a fresh single-replica Host
// with a counting state machine and waits for it to elect itself.
func startRegisterSessionShard(t *testing.T, cfg config.Config) (*Host, *applyCountingSM) {
	t.Helper()
	h := newTestHost(t)
	csm := &applyCountingSM{counts: make(map[string]int)}
	create := func(_, _ uint64) interface{} { return csm }
	if err := h.StartShard(map[uint64]string{1: "127.0.0.1:5000"}, false, create, cfg); err != nil {
		t.Fatalf("StartShard failed: %v", err)
	}
	waitForLeader(t, h, 1)
	return h, csm
}

// proposeAppliedOnce proposes cmd on s and asserts it is applied exactly once.
func proposeAppliedOnce(ctx context.Context, t *testing.T, h *Host, csm *applyCountingSM, s *Session, cmd string) {
	t.Helper()
	if _, err := h.SyncProposeWithSession(ctx, s, []byte(cmd)); err != nil {
		t.Fatalf("SyncProposeWithSession(%q) on client %#x: %v", cmd, s.ClientID(), err)
	}
	if got := csm.count(cmd); got != 1 {
		t.Fatalf("%q applied %d times, want exactly once", cmd, got)
	}
}

// closeSessionWait closes s and waits for the close to be applied.
func closeSessionWait(ctx context.Context, t *testing.T, h *Host, s *Session) {
	t.Helper()
	req, err := h.CloseSession(ctx, s.ShardID(), s)
	if err != nil {
		t.Fatalf("CloseSession(%#x): %v", s.ClientID(), err)
	}
	_, err = awaitResult(ctx, req)
	req.Release()
	if err != nil {
		t.Fatalf("CloseSession(%#x) result: %v", s.ClientID(), err)
	}
}

// requireSessionError asserts err is a *SessionError for clientID.
func requireSessionError(t *testing.T, what string, clientID uint64, err error) {
	t.Helper()
	if err == nil {
		t.Fatalf("%s: err = nil, want a *SessionError", what)
	}
	var se *SessionError
	if !errors.As(err, &se) {
		t.Fatalf("%s: err = %v (%T), want a *SessionError", what, err, err)
	}
	if se.ClientID != clientID {
		t.Fatalf("%s: SessionError.ClientID = %#x, want %#x", what, se.ClientID, clientID)
	}
}

// staleSessionEntry rebuilds the proposal state an earlier incarnation of a
// session had when it proposed series: the same client ID, series ID and
// RespondedTo, as a client restarting from persisted state would send it.
func staleSessionEntry(shardID, clientID, series, respondedTo uint64) *Session {
	s := NewSession(shardID, clientID)
	s.PrepareForPropose()
	s.seriesID.Store(series)
	s.respondedTo.Store(respondedTo)
	return s
}

// TestRegisterSessionClientIDHeldIsRefused checks (a): a client ID the shard
// holds cannot be registered again, and the holder keeps proposing.
func TestRegisterSessionClientIDHeldIsRefused(t *testing.T) {
	const clientID = uint64(0x939A0001)
	h, csm := startRegisterSessionShard(t, testShardConfig(1, 1))
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	first, err := h.RegisterSession(ctx, NewSession(1, clientID))
	if err != nil {
		t.Fatalf("first RegisterSession(%#x): %v", clientID, err)
	}
	proposeAppliedOnce(ctx, t, h, csm, first, "a-before")

	_, err = h.RegisterSession(ctx, NewSession(1, clientID))
	requireSessionError(t, "second RegisterSession of a held client ID", clientID, err)

	proposeAppliedOnce(ctx, t, h, csm, first, "a-after")
	if got := csm.count("a-before"); got != 1 {
		t.Fatalf("a-before applied %d times after the refused registration, want once", got)
	}
}

// TestRegisterSessionClientIDReregisterAfterClose checks (b): after
// CloseSession the client ID registers again, and an entry rebuilt from the
// earlier session with its old series ID is answered as expired and is not
// applied a second time.
func TestRegisterSessionClientIDReregisterAfterClose(t *testing.T) {
	const clientID = uint64(0x939B0001)
	h, csm := startRegisterSessionShard(t, testShardConfig(1, 1))
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	old, err := h.RegisterSession(ctx, NewSession(1, clientID))
	if err != nil {
		t.Fatalf("RegisterSession(%#x): %v", clientID, err)
	}
	oldSeries, oldResponded := old.SeriesID(), old.RespondedTo()
	proposeAppliedOnce(ctx, t, h, csm, old, "b-old")
	proposeAppliedOnce(ctx, t, h, csm, old, "b-old-2")
	closeSessionWait(ctx, t, h, old)

	// Closed and not yet re-registered: the stale entry is expired.
	stale := staleSessionEntry(1, clientID, oldSeries, oldResponded)
	_, err = h.SyncProposeWithSession(ctx, stale, []byte("b-old"))
	if !errors.Is(err, ErrSessionExpired) {
		t.Fatalf("stale entry after CloseSession: err = %v, want ErrSessionExpired", err)
	}
	if got := csm.count("b-old"); got != 1 {
		t.Fatalf("b-old applied %d times after CloseSession, want once", got)
	}

	renewed, err := h.RegisterSession(ctx, NewSession(1, clientID))
	if err != nil {
		t.Fatalf("RegisterSession(%#x) after CloseSession: %v, want success", clientID, err)
	}

	// The re-registration did not re-apply anything the earlier session
	// committed. An entry SENT after this point is a proposal of the new
	// session: the log carries no incarnation, so the shard cannot tell it
	// from one (docs/client-sessions/user.md, "Choose the client ID
	// yourself").
	if got := csm.count("b-old"); got != 1 {
		t.Fatalf("b-old applied %d times after re-registration, want once", got)
	}
	if got := csm.count("b-old-2"); got != 1 {
		t.Fatalf("b-old-2 applied %d times after re-registration, want once", got)
	}

	proposeAppliedOnce(ctx, t, h, csm, renewed, "b-new")
	proposeAppliedOnce(ctx, t, h, csm, renewed, "b-new-2")
}

// TestRegisterSessionClientIDMaxTotalSessions checks (c): MaxTotalSessions N
// admits N sessions, refuses the next with a *SessionError, and admits it
// once one session is closed.
func TestRegisterSessionClientIDMaxTotalSessions(t *testing.T) {
	const (
		limit = 3
		base  = uint64(0x939C0000)
	)
	cfg := testShardConfig(1, 1)
	cfg.MaxTotalSessions = limit
	h, csm := startRegisterSessionShard(t, cfg)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	held := make([]*Session, 0, limit)
	for i := uint64(1); i <= limit; i++ {
		s, err := h.RegisterSession(ctx, NewSession(1, base+i))
		if err != nil {
			t.Fatalf("RegisterSession #%d under MaxTotalSessions=%d: %v", i, limit, err)
		}
		held = append(held, s)
	}
	for i, s := range held {
		proposeAppliedOnce(ctx, t, h, csm, s, "c-held-"+string(rune('0'+i)))
	}

	next := base + limit + 1
	_, err := h.RegisterSession(ctx, NewSession(1, next))
	requireSessionError(t, "RegisterSession past MaxTotalSessions", next, err)

	closeSessionWait(ctx, t, h, held[0])

	s, err := h.RegisterSession(ctx, NewSession(1, next))
	if err != nil {
		t.Fatalf("RegisterSession after CloseSession freed a slot: %v, want success", err)
	}
	proposeAppliedOnce(ctx, t, h, csm, s, "c-next")
	for i, s := range held[1:] {
		proposeAppliedOnce(ctx, t, h, csm, s, "c-still-"+string(rune('0'+i)))
	}
}
