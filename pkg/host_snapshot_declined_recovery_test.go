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

// TestOnSnapshotCompleted_DeclinedStaleRecoveryIsNotAShardFailure pins the
// half of the stale-recovery contract that lives in the HOST rather than the
// engine.
//
// # The engine half
//
// The snapshot pool does not call Node.Fail on a stale recovery: the refusal
// is produced before the recovering gate is raised and before
// RecoverFromSnapshot runs, so the state machine is untouched and already at or
// past the snapshot index. The replica is alive and serving.
//
// The pool still reports that refusal as the completion's ERROR, and must -- a
// declined recovery is not a successful one either, and calling it success
// would fire OnSnapshotRecovered and reconcile membership from a snapshot
// nothing restored. But handing every non-nil error straight to
// EventListener.OnShardFailed is wrong: that is the only outward report that a
// replica makes no further progress without an operator StopShard/StartShard.
//
// The shard would stay alive while the host announced it dead. Under load that
// fails whole integration runs on a healthy cluster, with the engine's own
// "snapshot recovery declined" line sitting in the same log a few microseconds
// earlier:
//
//	INFO  snapshot rejected, behind committed index shard=1 replica=4 committed=25
//	INFO  snapshot recovery declined: the state machine is already past this
//	      snapshot, and raft rejected it too shard_id=1 replica_id=4
//	      snapshot_index=25 last_applied=25
//	FAIL  shard 1 replica 4 failed: engine: snapshot recovery for shard 1
//	      replica 4 at index 25 is not ahead of applied index 25
//
// # The other half
//
// The narrowness is the point, so the terminal case is pinned alongside it: a
// recovery that actually failed -- RecoverFromSnapshot returning an error --
// leaves a possibly half-restored state machine and MUST still be reported.
// Widening this into "snapshot errors are not shard failures" would silence it.
func TestOnSnapshotCompleted_DeclinedStaleRecoveryIsNotAShardFailure(t *testing.T) {
	tests := []struct {
		name       string
		err        error
		wantReport bool
		why        string
	}{
		{
			name: "declined stale recovery",
			err: &engine.SnapshotRecoveryStaleError{
				ShardID: 1, ReplicaID: 2, SnapshotIndex: 31, LastApplied: 34,
			},
			wantReport: false,
			why: "the engine declined this recovery and left the replica serving; " +
				"reporting it through OnShardFailed tells an operator the replica " +
				"is finished when it is not",
		},
		{
			name: "declined stale recovery, wrapped",
			err: fmt.Errorf("delivering install snapshot: %w",
				&engine.SnapshotRecoveryStaleError{
					ShardID: 1, ReplicaID: 2, SnapshotIndex: 42, LastApplied: 44,
				}),
			wantReport: false,
			why: "the match is on the error CHAIN, so a caller that adds context " +
				"with %w cannot resurrect the false failure report",
		},
		{
			name: "failed RecoverFromSnapshot",
			err: &engine.SnapshotError{
				ShardID: 1, ReplicaID: 2, Save: false,
				Err: errors.New("user state machine refused the snapshot"),
			},
			wantReport: true,
			why: "the user state machine is free to have mutated itself before " +
				"returning an error, so applies must not resume and the shard IS " +
				"finished",
		},
		{
			name: "failed snapshot save",
			err: &engine.SnapshotError{
				ShardID: 1, ReplicaID: 2, Save: true,
				Err: errors.New("no space left on device"),
			},
			wantReport: true,
			why:        "a save that cannot be made durable is the terminal state it always was",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rec := &shardFailedRecorder{}
			h := newTestHost(t)
			h.cfg.EventListener = rec.listener()
			addTestNodeState(h, 1, 2)

			cb := &hostEngineCallback{h: h}
			// index 0 is what a RECOVERY completion carries; a save carries the
			// saved index. Neither reaches the success branches here, because
			// every case is an error.
			cb.OnSnapshotCompleted(1, 0, tt.err)
			flushListeners(t, cb)

			got := rec.reports()
			if tt.wantReport && len(got) == 0 {
				t.Fatalf("no OnShardFailed report for %v: %s", tt.err, tt.why)
			}
			if !tt.wantReport && len(got) != 0 {
				t.Fatalf("OnShardFailed reported %q: %s", got[0].Err, tt.why)
			}
			if tt.wantReport {
				if got[0].ShardID != 1 || got[0].ReplicaID != 2 {
					t.Errorf("report named shard %d replica %d, want 1/2",
						got[0].ShardID, got[0].ReplicaID)
				}
				if !errors.Is(got[0].Err, tt.err) {
					t.Errorf("report carried %v, want the engine's own error %v",
						got[0].Err, tt.err)
				}
			}
		})
	}
}

// TestOnSnapshotCompleted_DeclinedStaleRecoveryIsNotReportedAsRecovered pins
// the flip side of the same completion: declining is not succeeding either.
//
// Suppressing the failure report by zeroing the error instead of by matching it
// would be the obvious shortcut and is wrong. A nil error with index 0 is the
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
	cb.OnSnapshotCompleted(1, 0, &engine.SnapshotRecoveryStaleError{
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
