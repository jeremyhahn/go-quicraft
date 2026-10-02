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
	"sync/atomic"
	"testing"
)

// closeCounterSM counts Close calls so a test can assert the state machine is
// released on a failed StartShard.
type closeCounterSM struct {
	mockStateMachine
	closes *atomic.Int64
}

func (s *closeCounterSM) Close(ctx context.Context) error {
	s.closes.Add(1)
	return s.mockStateMachine.Close(ctx)
}

// TestStartShard_ClosesStateMachineOnFailure is the regression test. A
// StartShard that opens the state machine (acquiring its persistent
// resources/file lock) but then fails on a LATER step must Close it, or a retry
// on the same data directory is permanently bricked with "lock held by current
// process". The failure is injected at SaveBootstrap, which runs after Open.
func TestStartShard_ClosesStateMachineOnFailure(t *testing.T) {
	cfg := testHostConfig(t)
	db := newMockLogDB()
	db.saveBootstrapErr = errors.New("injected SaveBootstrap failure")
	h, err := NewHost(cfg, WithLogDB(db), WithoutTransport(), WithoutEncryption())
	if err != nil {
		t.Fatalf("NewHost: %v", err)
	}
	defer h.Close()

	var closes atomic.Int64
	create := func(_, _ uint64) interface{} { return &closeCounterSM{closes: &closes} }
	members := map[uint64]string{1: "127.0.0.1:5000"}

	// StartShard opens the SM, then SaveBootstrap fails: the opened SM must be
	// closed (releasing any file lock), not leaked.
	startErr := h.StartShard(members, false, create, testShardConfig(1, 1))
	if startErr == nil {
		t.Fatal("StartShard succeeded; expected the injected SaveBootstrap failure")
	}
	if got := closes.Load(); got != 1 {
		t.Fatalf("state machine closed %d times after a failed StartShard, want 1 (leaked open SM)", got)
	}
	if h.ShardCount() != 0 {
		t.Fatalf("ShardCount = %d, want 0 after failed StartShard", h.ShardCount())
	}
}
