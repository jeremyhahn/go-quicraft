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

package engine

import (
	"sync"
	"testing"

	"github.com/jeremyhahn/go-quicraft/pkg/config"
)

// TestSnapshotPool_WorkerExitReleasesQueuedRequests asserts that a snapshot
// request still sitting on requestC when the pool shuts down is released
// rather than stranded.
//
// requestSnapshot establishes two pieces of per-shard state BEFORE the request
// is queued — the inFlight entry that dedupes the shard, and the node's
// snapshotting flag — and both are cleared only by the goroutine that
// eventually runs the request. A worker that returns on stopC while requests
// remain queued therefore wedges those shards permanently: inFlight never
// clears, so requestSnapshot rejects every future snapshot for the shard with
// SnapshotDuplicateError, snapshotting stays true, and the caller's pending
// RequestState waits for a completion callback that can no longer fire.
//
// The worker's select has both stopC and requestC ready at shutdown and Go
// chooses between ready cases at random, so the invariant is exercised over
// many rounds: with the release in place every round is clean regardless of
// which branch is taken, and only a regression depends on the coin flips.
func TestSnapshotPool_WorkerExitReleasesQueuedRequests(t *testing.T) {
	const rounds = 40

	for round := range rounds {
		func() {
			cb := newTestCallback()
			stopC := make(chan struct{})
			p := newSnapshotPool(1, nil, nil, cb, nil, stopC, nil)

			// Queue requests exactly as requestSnapshot does — the per-shard
			// state first, then the send — but with no worker running yet, so
			// the pool is guaranteed to shut down with a full requestC.
			const queued = 4
			p.requestC = make(chan snapshotRequest, queued)
			nodes := make([]*Node, 0, queued)
			for i := range queued {
				shardID := uint64(i + 1)
				node := NewNode(nil, nil, nil,
					config.Config{ShardID: shardID, ReplicaID: 1}, nil, "", nil)

				node.snapshotting.Store(true)
				p.inFlight.Store(shardID, struct{}{})
				p.requestC <- snapshotRequest{
					shardID:   shardID,
					replicaID: 1,
					node:      node,
					save:      true,
				}
				nodes = append(nodes, node)
			}

			close(stopC)
			var wg sync.WaitGroup
			p.start(&wg)
			wg.Wait()

			var stranded []uint64
			p.inFlight.Range(func(key, _ any) bool {
				stranded = append(stranded, key.(uint64))
				return true
			})
			if len(stranded) > 0 {
				t.Fatalf("round %d: shards %v still marked in-flight after the "+
					"snapshot pool shut down: requestSnapshot rejects every "+
					"future snapshot for them with SnapshotDuplicateError for "+
					"the lifetime of the process", round, stranded)
			}
			for i, node := range nodes {
				if node.snapshotting.Load() {
					t.Fatalf("round %d: shard %d is still marked snapshotting "+
						"after the pool shut down with its request queued",
						round, i+1)
				}
			}
			if int(cb.snapshotRejected.Load())+int(cb.snapshotCallCount.Load()) < queued {
				t.Fatalf("round %d: only %d of %d queued requests were reported "+
					"to the caller; the rest wait for a completion that can "+
					"never come", round,
					cb.snapshotRejected.Load()+cb.snapshotCallCount.Load(), queued)
			}
		}()
	}
}
