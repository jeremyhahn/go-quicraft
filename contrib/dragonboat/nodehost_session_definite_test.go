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

package dragonboat

import (
	"context"
	"errors"
	"testing"
	"time"

	dbClient "github.com/jeremyhahn/go-quicraft/contrib/dragonboat/client"
	quicraft "github.com/jeremyhahn/go-quicraft/pkg"
)

// closedQueueWait stands in for the host's wait on a proposal its node's
// queue closed on before the step worker took it: the request can never
// reach Raft, so the host reports the caller's lost wait as a definite
// ErrTimeout or ErrCanceled (pkg TestWait_OnAClosedQueueIsDefinite).
func closedQueueWait(ctx context.Context, _ *quicraft.RequestState) (quicraft.RequestResult, error) {
	<-ctx.Done()
	if ctx.Err() == context.DeadlineExceeded {
		return quicraft.RequestResult{}, quicraft.ErrTimeout
	}
	return quicraft.RequestResult{}, quicraft.ErrCanceled
}

// definiteLostWaitCases runs call once with a deadline and once with a
// cancellation while the host classifies the request as never handed off,
// and asserts each lost wait stays definite: the adapter must not override
// the host's classification with maybe-applied.
func definiteLostWaitCases(t *testing.T, sessions []*dbClient.Session, call func(context.Context, *dbClient.Session) error) {
	t.Helper()
	saved := waitRequest
	waitRequest = closedQueueWait
	t.Cleanup(func() { waitRequest = saved })

	t.Run("deadline", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
		defer cancel()
		assertDefiniteLostWait(t, call(ctx, sessions[0]), ErrTimeout)
	})
	t.Run("cancel", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		time.AfterFunc(200*time.Millisecond, cancel)
		assertDefiniteLostWait(t, call(ctx, sessions[1]), ErrCanceled)
	})
}

func assertDefiniteLostWait(t *testing.T, err, want error) {
	t.Helper()
	if !errors.Is(err, want) {
		t.Fatalf("err = %v, want errors.Is(err, %v)", err, want)
	}
	if quicraft.MaybeApplied(err) {
		t.Fatalf("quicraft.MaybeApplied(%v) = true, want false: the request never left a closed queue", err)
	}
}

func TestNodeHost_SyncPropose_TrackedSession_ClosedQueueLostWaitIsDefinite(t *testing.T) {
	nh, gate, sessions := startApplyBlockedShard(t, 2)
	definiteLostWaitCases(t, sessions, func(ctx context.Context, cs *dbClient.Session) error {
		_, err := nh.SyncPropose(ctx, cs, []byte("tracked"))
		return err
	})
	gate.open()
}

func TestNodeHost_SyncCloseSession_ClosedQueueLostWaitIsDefinite(t *testing.T) {
	nh, gate, sessions := startApplyBlockedShard(t, 2)
	definiteLostWaitCases(t, sessions, func(ctx context.Context, cs *dbClient.Session) error {
		return nh.SyncCloseSession(ctx, cs)
	})
	gate.open()
}
