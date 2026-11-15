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

	dbConfig "github.com/jeremyhahn/go-quicraft/contrib/dragonboat/config"
	dbSM "github.com/jeremyhahn/go-quicraft/contrib/dragonboat/statemachine"
)

// errNodeHostSMErrorLookup and errNodeHostSMErrorUpdate are the sentinels
// the failing state machines below return; the tests check that NodeHost
// callers can still match them after the adapter and the host wrap them.
var (
	errNodeHostSMErrorLookup = errors.New("sm error test: lookup failed")
	errNodeHostSMErrorUpdate = errors.New("sm error test: update failed")
)

// smErrorFailures selects which user state machine method fails.
type smErrorFailures struct {
	lookup bool
	update bool
}

// smErrorRegularSM is a regular state machine whose Lookup or Update
// returns a sentinel error.
type smErrorRegularSM struct {
	testStateMachine
	fail smErrorFailures
}

func (sm *smErrorRegularSM) Update(entry dbSM.Entry) (dbSM.Result, error) {
	if sm.fail.update {
		return dbSM.Result{}, errNodeHostSMErrorUpdate
	}
	return sm.testStateMachine.Update(entry)
}

func (sm *smErrorRegularSM) Lookup(query interface{}) (interface{}, error) {
	if sm.fail.lookup {
		return nil, errNodeHostSMErrorLookup
	}
	return sm.testStateMachine.Lookup(query)
}

// smErrorConcurrentSM is a concurrent state machine whose Lookup or Update
// returns a sentinel error.
type smErrorConcurrentSM struct {
	testConcurrentStateMachine
	fail smErrorFailures
}

func (sm *smErrorConcurrentSM) Update(entries []dbSM.Entry) ([]dbSM.Entry, error) {
	if sm.fail.update {
		return nil, errNodeHostSMErrorUpdate
	}
	return sm.testConcurrentStateMachine.Update(entries)
}

func (sm *smErrorConcurrentSM) Lookup(query interface{}) (interface{}, error) {
	if sm.fail.lookup {
		return nil, errNodeHostSMErrorLookup
	}
	return sm.testConcurrentStateMachine.Lookup(query)
}

// smErrorOnDiskSM is an on-disk state machine whose Lookup or Update
// returns a sentinel error.
type smErrorOnDiskSM struct {
	testOnDiskStateMachine
	fail smErrorFailures
}

func (sm *smErrorOnDiskSM) Update(entries []dbSM.Entry) ([]dbSM.Entry, error) {
	if sm.fail.update {
		return nil, errNodeHostSMErrorUpdate
	}
	return sm.testOnDiskStateMachine.Update(entries)
}

func (sm *smErrorOnDiskSM) Lookup(query interface{}) (interface{}, error) {
	if sm.fail.lookup {
		return nil, errNodeHostSMErrorLookup
	}
	return sm.testOnDiskStateMachine.Lookup(query)
}

// smErrorStarter starts a replica of one state machine kind with the
// given failures on nh.
type smErrorStarter func(nh *NodeHost, members map[uint64]Target, cfg dbConfig.Config, fail smErrorFailures) error

// smErrorKinds lists the three dragonboat state machine kinds.
var smErrorKinds = []struct {
	name  string
	start smErrorStarter
}{
	{
		name: "regular",
		start: func(nh *NodeHost, members map[uint64]Target, cfg dbConfig.Config, fail smErrorFailures) error {
			return nh.StartReplica(members, false, func(_, _ uint64) dbSM.IStateMachine {
				return &smErrorRegularSM{fail: fail}
			}, cfg)
		},
	},
	{
		name: "concurrent",
		start: func(nh *NodeHost, members map[uint64]Target, cfg dbConfig.Config, fail smErrorFailures) error {
			return nh.StartConcurrentReplica(members, false, func(_, _ uint64) dbSM.IConcurrentStateMachine {
				return &smErrorConcurrentSM{fail: fail}
			}, cfg)
		},
	},
	{
		name: "ondisk",
		start: func(nh *NodeHost, members map[uint64]Target, cfg dbConfig.Config, fail smErrorFailures) error {
			return nh.StartOnDiskReplica(members, false, func(_, _ uint64) dbSM.IOnDiskStateMachine {
				return &smErrorOnDiskSM{fail: fail}
			}, cfg)
		},
	},
}

// startNodeHostSMErrorShard starts a single-replica shard with start and
// waits, bounded by ctx, until the replica leads and serves a
// linearizable membership read.
func startNodeHostSMErrorShard(ctx context.Context, t *testing.T, nh *NodeHost, shardID uint64, start smErrorStarter, fail smErrorFailures) {
	t.Helper()
	const replicaID = uint64(1)
	members := map[uint64]Target{replicaID: nh.RaftAddress()}
	cfg := dbConfig.Config{
		ShardID:      shardID,
		ReplicaID:    replicaID,
		ElectionRTT:  5,
		HeartbeatRTT: 1,
		CheckQuorum:  true,
	}
	if err := start(nh, members, cfg, fail); err != nil {
		t.Fatalf("start replica: %v", err)
	}

	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	var lastErr error
	for {
		leaderID, _, valid, err := nh.GetLeaderID(shardID)
		if err == nil && valid && leaderID == replicaID {
			readCtx, cancel := context.WithTimeout(ctx, time.Second)
			_, err = nh.SyncGetShardMembership(readCtx, shardID)
			cancel()
			if err == nil {
				return
			}
		}
		lastErr = err
		select {
		case <-ctx.Done():
			t.Fatalf("shard %d not ready: %v (last error: %v)", shardID, ctx.Err(), lastErr)
		case <-ticker.C:
		}
	}
}

// checkNodeHostSMErrorLookup asserts that err carries the Lookup sentinel
// inside an *AdapterLookupError.
func checkNodeHostSMErrorLookup(_ context.Context, t *testing.T, call string, err error) {
	t.Helper()
	if err == nil {
		t.Fatalf("%s: got nil error, want the Lookup error", call)
	}
	if !errors.Is(err, errNodeHostSMErrorLookup) {
		t.Errorf("%s: errors.Is(%v, sentinel) = false", call, err)
	}
	var lookupErr *dbSM.AdapterLookupError
	if !errors.As(err, &lookupErr) {
		t.Errorf("%s: errors.As(%v, *AdapterLookupError) = false (%T)", call, err, err)
	} else if !errors.Is(lookupErr.Cause, errNodeHostSMErrorLookup) {
		t.Errorf("%s: AdapterLookupError.Cause = %v, want the sentinel", call, lookupErr.Cause)
	}
}

// TestNodeHostSMError_LookupReachesReads checks that a Lookup error from
// each state machine kind reaches SyncRead and StaleRead callers with the
// cause intact and wrapped in *AdapterLookupError.
func TestNodeHostSMError_LookupReachesReads(t *testing.T) {
	for _, kind := range smErrorKinds {
		t.Run(kind.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
			defer cancel()
			nh := newTestNodeHost(t)
			const shardID = uint64(1006)
			startNodeHostSMErrorShard(ctx, t, nh, shardID, kind.start, smErrorFailures{lookup: true})

			readCtx, readCancel := context.WithTimeout(ctx, 5*time.Second)
			result, err := nh.SyncRead(readCtx, shardID, []byte("query"))
			readCancel()
			if result != nil {
				t.Errorf("SyncRead: result = %v, want nil", result)
			}
			checkNodeHostSMErrorLookup(ctx, t, "SyncRead", err)

			result, err = nh.StaleRead(shardID, []byte("query"))
			if result != nil {
				t.Errorf("StaleRead: result = %v, want nil", result)
			}
			checkNodeHostSMErrorLookup(ctx, t, "StaleRead", err)
		})
	}
}

// TestNodeHostSMError_UpdateFailsSyncPropose checks that an Update error
// from each state machine kind makes SyncPropose return an error and no
// result before its deadline, and that a later SyncPropose on the same
// shard also returns an error before its deadline instead of hanging.
func TestNodeHostSMError_UpdateFailsSyncPropose(t *testing.T) {
	const proposeTimeout = 5 * time.Second
	for _, kind := range smErrorKinds {
		t.Run(kind.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			nh := newTestNodeHost(t)
			const shardID = uint64(1006)
			startNodeHostSMErrorShard(ctx, t, nh, shardID, kind.start, smErrorFailures{update: true})

			for attempt := 1; attempt <= 2; attempt++ {
				propCtx, propCancel := context.WithTimeout(ctx, proposeTimeout)
				began := time.Now()
				result, err := nh.SyncPropose(propCtx, nh.GetNoOPSession(shardID), []byte("cmd"))
				elapsed := time.Since(began)
				deadlineHit := propCtx.Err() != nil
				propCancel()

				t.Logf("SyncPropose attempt %d: err=%v (%T) after %v", attempt, err, err, elapsed)
				if err == nil {
					t.Fatalf("SyncPropose attempt %d: got result %+v and nil error, want an error", attempt, result)
				}
				if result.Value != 0 || len(result.Data) != 0 {
					t.Errorf("SyncPropose attempt %d: result = %+v, want the zero Result", attempt, result)
				}
				if deadlineHit || errors.Is(err, ErrTimeout) {
					t.Errorf("SyncPropose attempt %d: returned %v after %v, at its %v deadline instead of before it",
						attempt, err, elapsed, proposeTimeout)
				}
				if attempt == 1 {
					if !errors.Is(err, errNodeHostSMErrorUpdate) {
						t.Errorf("SyncPropose: errors.Is(%v, sentinel) = false", err)
					}
					var updateErr *dbSM.AdapterUpdateError
					switch {
					case !errors.As(err, &updateErr):
						t.Errorf("SyncPropose: errors.As(%v, *AdapterUpdateError) = false (%T)", err, err)
					case kind.name == "regular" && updateErr.Index == 0:
						// The regular adapter applies entries one by one and
						// names the failing entry's log index.
						t.Errorf("SyncPropose: AdapterUpdateError.Index = 0, want the entry's log index")
					case kind.name != "regular" && updateErr.Index != 0:
						// A batch Update cannot name the failing entry, so the
						// concurrent and on-disk adapters report Index 0.
						t.Errorf("SyncPropose: AdapterUpdateError.Index = %d, want 0 for a batch adapter", updateErr.Index)
					}
				}
			}
		})
	}
}
