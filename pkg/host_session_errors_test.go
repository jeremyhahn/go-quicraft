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

// TestHostSessionErrors_ThroughTheHost drives a real Host through the cheapest
// public call for each session refusal and records what errors.Is matches
// with the error the caller receives.
//
// The Host does not pass the session manager's error to the caller. The apply
// path turns each refusal into a result value, because a refusal is a
// replicated outcome and not a host failure (pkg/internal/rsm/statemachine.go):
//
//   - ErrSessionLimitExceeded / ErrTooManySessions: NewSession past
//     MaxTotalSessions applies to an empty result. NewSession reports it as a
//     *SessionError from "register" wrapping ErrInvalidOperation.
//   - ErrSessionAlreadyExists: RegisterSession of a registered session applies
//     to an empty result too, and it is reported the same way.
//   - ErrSessionNotFound: a proposal under an unregistered client ID applies
//     to ResultSessionExpired, which the Host returns as a
//     *SessionExpiredError (errors.Is ErrSessionExpired).
//   - ErrResponseLimitExceeded: cannot be reached through the public API.
//     Session.ProposalCompleted always moves RespondedTo forward to the series
//     it completed, so the session manager evicts each cached response before
//     the next one is recorded, and the cache never fills.
//
// So no public call returns a public session value today. The test asserts
// that boundary so it cannot change unnoticed: when the Host returns the
// public values, the want flips to true here.
func TestHostSessionErrors_ThroughTheHost(t *testing.T) {
	h := newTestHost(t)

	scfg := testShardConfig(1, 1)
	scfg.MaxTotalSessions = 2
	if err := h.StartShard(map[uint64]string{1: "127.0.0.1:5000"}, false,
		testCreateFunc, scfg); err != nil {
		t.Fatalf("StartShard failed: %v", err)
	}
	waitForLeader(t, h, 1)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	first, err := h.NewSession(ctx, 1)
	if err != nil {
		t.Fatalf("NewSession #1 = %v", err)
	}
	_, existsErr := h.RegisterSession(ctx, NewSession(1, first.ClientID()))
	if _, err := h.NewSession(ctx, 1); err != nil {
		t.Fatalf("NewSession #2 = %v (MaxTotalSessions=2 must admit it)", err)
	}
	_, limitErr := h.NewSession(ctx, 1)

	unknown := NewSession(1, 0x5e55_1000_0000_0001)
	unknown.PrepareForPropose()
	_, notFoundErr := h.SyncProposeWithSession(ctx, unknown, []byte("x"))

	tests := []struct {
		name     string
		got      error
		public   error
		returned error
	}{
		{"ErrSessionLimitExceeded", limitErr, ErrSessionLimitExceeded, ErrInvalidOperation},
		{"ErrTooManySessions", limitErr, ErrTooManySessions, ErrInvalidOperation},
		{"ErrSessionAlreadyExists", existsErr, ErrSessionAlreadyExists, ErrInvalidOperation},
		{"ErrSessionNotFound", notFoundErr, ErrSessionNotFound, ErrSessionExpired},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if tc.got == nil {
				t.Fatal("the Host call succeeded; want a refusal")
			}
			if !errors.Is(tc.got, tc.returned) {
				t.Errorf("errors.Is(%v, %v) = false; the Host returns a different refusal now",
					tc.got, tc.returned)
			}
			if errors.Is(tc.got, tc.public) {
				t.Errorf("errors.Is(%v, %s) = true; the Host now returns the public "+
					"session value, so flip this case to require it", tc.got, tc.name)
			}
		})
	}
}
