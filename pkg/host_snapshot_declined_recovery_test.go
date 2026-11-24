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

// TestOnSnapshotCompleted_ErrorIsNotAShardFailureReport verifies a failed completion never fires OnShardFailed from the host.
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

// TestOnSnapshotCompleted_DeclinedStaleRecoveryIsNotReportedAsRecovered verifies a declined recovery fires no recovered event.
func TestOnSnapshotCompleted_DeclinedStaleRecoveryIsNotReportedAsRecovered(t *testing.T) {
	var recovered int
	h := newTestHost(t)
	h.cfg.EventListener = &config.EventListener{
		OnSnapshotRecovered: func(config.SnapshotInfo) { recovered++ },
	}
	addTestNodeState(h, 1, 2)

	cb := &hostEngineCallback{h: h}
	cb.OnSnapshotCompleted(1, 0, engine.SnapshotRecover, &engine.SnapshotRecoveryStaleError{
		ShardID: 1, ReplicaID: 2, SnapshotIndex: 31, LastApplied: 34,
	})
	flushListeners(t, cb)

	if recovered != 0 {
		t.Errorf("OnSnapshotRecovered fired %d times for a declined recovery, want 0: "+
			"nothing was restored, so announcing a recovery would be a false report "+
			"in the other direction", recovered)
	}
}
