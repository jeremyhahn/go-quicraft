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

// TestHostSessionsPerOwnerUnlimited pins that SessionsPerOwnerUnlimited
// turns the per-owner cap off without a special case: one owner may fill
// the shard, and MaxTotalSessions is what stops it.
//
// The refusal shape is the one the code produces: session.Manager.Register
// returns ErrSessionLimitExceeded for the shard-wide total, the state
// machine answers the register with a value other than the client ID, and
// Host.NewSessionWithOwner reports that as a *SessionError from "register"
// wrapping ErrInvalidOperation.
func TestHostSessionsPerOwnerUnlimited(t *testing.T) {
	const (
		owner = uint64(0x744)
		total = 3
	)
	h := newTestHost(t)
	scfg := testShardConfig(1, 1)
	scfg.MaxSessionsPerOwner = config.SessionsPerOwnerUnlimited
	scfg.MaxTotalSessions = total
	if err := h.StartShard(map[uint64]string{1: "127.0.0.1:5000"}, false,
		testCreateFunc, scfg); err != nil {
		t.Fatalf("StartShard failed: %v", err)
	}
	waitForLeader(t, h, 1)

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	// One owner holds every slot the shard has.
	var held []*Session
	for i := 0; i < total; i++ {
		s, err := h.NewSessionWithOwner(ctx, 1, owner)
		if err != nil {
			t.Fatalf("NewSessionWithOwner #%d: %v, want success: the per-owner "+
				"cap is SessionsPerOwnerUnlimited and the shard holds %d", i+1, err, total)
		}
		if got := s.OwnerID(); got != owner {
			t.Fatalf("NewSessionWithOwner #%d: OwnerID() = %#x, want %#x", i+1, got, owner)
		}
		held = append(held, s)
	}

	// The next one is refused by the shard-wide total.
	s, err := h.NewSessionWithOwner(ctx, 1, owner)
	if err == nil {
		t.Fatalf("NewSessionWithOwner #%d: err = nil (session %v), want a refusal: "+
			"MaxTotalSessions = %d bounds the shard even with no per-owner cap",
			total+1, s, total)
	}
	var se *SessionError
	if !errors.As(err, &se) || se.Op != "register" {
		t.Fatalf("NewSessionWithOwner #%d: err = %v (%T), want a *SessionError from register",
			total+1, err, err)
	}
	if !errors.Is(err, ErrInvalidOperation) {
		t.Fatalf("NewSessionWithOwner #%d: err = %v, want it to wrap ErrInvalidOperation",
			total+1, err)
	}

	// Closing one frees a slot the same owner can take again.
	req, err := h.CloseSession(ctx, 1, held[0])
	if err != nil {
		t.Fatalf("CloseSession(#1): %v", err)
	}
	_, err = awaitResult(ctx, req)
	req.Release()
	if err != nil {
		t.Fatalf("CloseSession(#1) result: %v", err)
	}
	again, err := h.NewSessionWithOwner(ctx, 1, owner)
	if err != nil {
		t.Fatalf("NewSessionWithOwner after CloseSession: %v, want success: "+
			"CloseSession must free a shard-wide slot", err)
	}
	if got := again.OwnerID(); got != owner {
		t.Fatalf("NewSessionWithOwner after CloseSession: OwnerID() = %#x, want %#x", got, owner)
	}
}
