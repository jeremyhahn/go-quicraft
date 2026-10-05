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
	"errors"
	"fmt"
	"sync"
	"testing"

	"github.com/jeremyhahn/go-quicraft/pkg/config"
	"github.com/jeremyhahn/go-quicraft/pkg/internal/engine"
)

// shardFailedRecorder collects every OnShardFailed report a Host fires, so a
// test can assert on the absence of one as well as its presence.
type shardFailedRecorder struct {
	mu   sync.Mutex
	seen []config.ShardFailedInfo
}

func (r *shardFailedRecorder) listener() *config.EventListener {
	return &config.EventListener{
		OnShardFailed: func(info config.ShardFailedInfo) {
			r.mu.Lock()
			r.seen = append(r.seen, info)
			r.mu.Unlock()
		},
	}
}

func (r *shardFailedRecorder) reports() []config.ShardFailedInfo {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]config.ShardFailedInfo(nil), r.seen...)
}

// TestOnSnapshotCompleted_ErrorIsNotAShardFailureReport pins the HOST half of
// the OnShardFailed contract for snapshot completions: a failed completion
// never fires OnShardFailed from here, whatever the error.
//
// OnShardFailed reports a tripped failure latch, once. The only snapshot
// outcome that trips it — a failed RecoverFromSnapshot — is reported by the
// latch itself (engine.Node.Fail in the snapshot pool, pinned by the engine's
// TestHandleRequest_PostRestoreErrorLatches), so a report here
// counts the same failure twice. A failed save does not latch at all.
//
// The declined stale recovery is the case that made this visible first. The
// engine refuses a recovery whose snapshot is not ahead of the apply frontier
// and leaves the replica serving, yet the completion still carries that
// refusal as its error. Reported as a shard failure it failed whole
// integration runs on a healthy cluster, with the engine's own "snapshot
// recovery declined" line a few microseconds earlier in the same log:
//
//	INFO  snapshot rejected, behind committed index shard=1 replica=4 committed=25
//	INFO  snapshot recovery declined: the state machine is already past this
//	      snapshot, and raft rejected it too shard_id=1 replica_id=4
//	      snapshot_index=25 last_applied=25
//	FAIL  shard 1 replica 4 failed: engine: snapshot recovery for shard 1
//	      replica 4 at index 25 is not ahead of applied index 25
func TestOnSnapshotCompleted_ErrorIsNotAShardFailureReport(t *testing.T) {
	tests := []struct {
		name string
		op   engine.SnapshotOperation
		err  error
		why  string
	}{
		{
			name: "declined stale recovery",
			op:   engine.SnapshotRecover,
			err: &engine.SnapshotRecoveryStaleError{
				ShardID: 1, ReplicaID: 2, SnapshotIndex: 31, LastApplied: 34,
			},
			why: "the engine declined this recovery and left the replica serving",
		},
		{
			name: "declined stale recovery, wrapped",
			op:   engine.SnapshotRecover,
			err: fmt.Errorf("delivering install snapshot: %w",
				&engine.SnapshotRecoveryStaleError{
					ShardID: 1, ReplicaID: 2, SnapshotIndex: 42, LastApplied: 44,
				}),
			why: "wrapping the refusal does not make it a failure",
		},
		{
			name: "failed RecoverFromSnapshot",
			op:   engine.SnapshotRecover,
			err: &engine.SnapshotError{
				ShardID: 1, ReplicaID: 2, Save: false,
				Err: errors.New("user state machine refused the snapshot"),
			},
			why: "the latch that this failure trips has already reported it; " +
				"a second report counts one shard failure twice",
		},
		{
			name: "failed snapshot save",
			op:   engine.SnapshotSave,
			err: &engine.SnapshotError{
				ShardID: 1, ReplicaID: 2, Save: true,
				Err: errors.New("no space left on device"),
			},
			why: "a failed save leaves the shard running; it is logged and " +
				"returned to the requester instead",
		},
		{
			name: "unclassified error",
			op:   engine.SnapshotRecover,
			err:  errors.New("disk full"),
			why:  "only a tripped latch is a shard failure",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rec := &shardFailedRecorder{}
			h := newTestHost(t)
			h.cfg.EventListener = rec.listener()
			addTestNodeState(h, 1, 2)

			cb := &hostEngineCallback{h: h}
			cb.OnSnapshotCompleted(1, 0, tt.op, tt.err)
			flushListeners(t, cb)

			if got := rec.reports(); len(got) != 0 {
				t.Fatalf("OnShardFailed reported %q: %s", got[0].Err, tt.why)
			}
		})
	}
}

// TestOnSnapshotCompleted_DeclinedStaleRecoveryIsNotReportedAsRecovered pins
// the flip side of the same completion: declining is not succeeding either.
//
// Suppressing the failure report by zeroing the error instead of by matching it
// would be the obvious shortcut and is wrong. A nil error on a recovery is the
// SUCCESSFUL-RECOVERY signal: it fires OnSnapshotRecovered and reconciles the
// host's routing table from the peer's membership. Doing that for a snapshot
// nothing ever restored would announce a recovery that did not happen.
func TestOnSnapshotCompleted_DeclinedStaleRecoveryIsNotReportedAsRecovered(t *testing.T) {
	var recovered, failed int
	h := newTestHost(t)
	h.cfg.EventListener = &config.EventListener{
		OnSnapshotRecovered: func(config.SnapshotInfo) { recovered++ },
		OnShardFailed:       func(config.ShardFailedInfo) { failed++ },
	}
	addTestNodeState(h, 1, 2)

	cb := &hostEngineCallback{h: h}
	cb.OnSnapshotCompleted(1, 0, engine.SnapshotRecover, &engine.SnapshotRecoveryStaleError{
		ShardID: 1, ReplicaID: 2, SnapshotIndex: 31, LastApplied: 34,
	})
	flushListeners(t, cb)

	if failed != 0 {
		t.Errorf("OnShardFailed fired %d times for a declined recovery, want 0", failed)
	}
	if recovered != 0 {
		t.Errorf("OnSnapshotRecovered fired %d times for a declined recovery, want 0: "+
			"nothing was restored, so announcing a recovery would be a false report "+
			"in the other direction", recovered)
	}
}
