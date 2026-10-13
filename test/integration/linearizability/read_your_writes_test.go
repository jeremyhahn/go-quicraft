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

//go:build integration

package linearizability_test

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	quicraft "github.com/jeremyhahn/go-quicraft/pkg"
)

// staleRead is one read-your-writes violation with everything needed to
// attribute it: which host served the read, what read index the leader
// confirmed, what the serving state machine had applied when the read was
// released to Lookup, and where the value it returned came from.
type staleRead struct {
	client, host int
	replica      uint64
	key          string
	wrote        uint64 // index of the client's own acknowledged put
	readIndex    uint64 // the index the leader confirmed for this read
	smApplied    uint64 // the serving SM's applied marker when the read completed
	hostApplied  uint64 // the host's applied marker (GetShardInfo) at the same moment
	observed     uint64 // index of the write that produced the value returned
	value        string
	leader       int
}

func (s staleRead) String() string {
	return fmt.Sprintf("client=%d host=%d replica=%d key=%s wrote@%d readIndex=%d smApplied=%d hostApplied=%d observed@%d value=%q leader=%d",
		s.client, s.host, s.replica, s.key, s.wrote, s.readIndex, s.smApplied, s.hostApplied, s.observed, s.value, s.leader)
}

// TestSoak_RollingRestart_ReadYourWrites reproduces the stale-read shape a
// rolling-restart soak can return as porcupine Illegal: a SyncRead serving a
// value two acknowledged writes old, one of them the reading client's own,
// right after a restarted replica caught up. The soak can say that much and
// no more -- its history does not record which host served a read, what read
// index the leader confirmed, or whether the serving state machine had
// applied the write when the read was released.
//
// This test asks the narrowest question that history raises, and records
// every one of those facts for each answer. Each client writes and then
// reads ITS OWN key, so every read is a read-your-writes check with maximal
// sensitivity: a stale value has nowhere to hide behind another client's
// concurrent write. The put result carries the entry index; the read is
// taken apart into ReadIndex -> sample markers -> indexed Lookup, exactly the
// sequence SyncRead performs, so the diagnosis is of SyncRead's own path.
//
// Even clients go leader-first (the soak's choice); odd clients are pinned to
// one host each and keep knocking through its restarts, because the window of
// interest is the replay a restarted replica performs before it is caught
// up, and only a client standing at that replica during it can be lied to.
func TestSoak_RollingRestart_ReadYourWrites(t *testing.T) {
	const (
		numNodes   = 5
		numClients = 8
		shardID    = 1
	)

	duration := soakDuration(t)
	t.Logf("rolling restart (read-your-writes): duration=%v", duration)

	cluster := newTestCluster(t, numNodes)
	defer cluster.close()

	cluster.startShard(shardID)
	cluster.waitForClusterLeader(shardID, leaderElectionTimeout)
	warmupCluster(t, cluster, shardID)
	startLeaderTermGuard(t, cluster, shardID)

	var (
		stalesMu  sync.Mutex
		stales    []staleRead
		reads     atomic.Int64
		writes    atomic.Int64
		falseAcks atomic.Int64
		done      = make(chan struct{})
		wg        sync.WaitGroup
	)
	record := func(s staleRead) {
		stalesMu.Lock()
		stales = append(stales, s)
		stalesMu.Unlock()
	}

	// pickHost returns the host a read or write goes to and its index. An
	// even client goes leader-first, the soak's choice, falling back to the
	// live host after `turn` in rotation. An odd client is PINNED to one host
	// and keeps knocking while it is down or restarting: a restarted replica
	// replays its whole log before it is caught up, and the proposals it
	// accepts during that replay are the ones a previous incarnation's keys
	// could collide with. A client that wanders off to another host at
	// the first refusal never stands in that window.
	pickHost := func(clientID, turn int) (*quicraft.Host, int) {
		cluster.hostsMu.RLock()
		defer cluster.hostsMu.RUnlock()
		if clientID%2 == 1 {
			idx := clientID % numNodes
			if cluster.stopped[idx].Load() {
				return nil, -1
			}
			return cluster.hosts[idx], idx
		}
		for i, h := range cluster.hosts {
			if cluster.stopped[i].Load() {
				continue
			}
			if info, err := h.GetShardInfo(shardID); err == nil && info.IsLeader {
				return h, i
			}
		}
		for i := range numNodes {
			idx := (turn + i) % numNodes
			if !cluster.stopped[idx].Load() {
				return cluster.hosts[idx], idx
			}
		}
		return nil, -1
	}

	clientsStartedAt := time.Now()
	for c := range numClients {
		wg.Add(1)
		go func(clientID int) {
			defer wg.Done()
			key := fmt.Sprintf("ryw-%d", clientID)
			deadline := time.Now().Add(duration)
			var lastWrote, lastObserved uint64
			for turn := 0; time.Now().Before(deadline); turn++ {
				select {
				case <-done:
					return
				default:
				}
				host, hostIdx := pickHost(clientID, turn)
				if host == nil {
					pause(time.Millisecond, done)
					continue
				}

				// Write. The leader's applied marker BEFORE the write begins
				// bounds every entry that existed then: an acknowledgement
				// carrying an index at or below it is another entry's result
				// handed to this caller -- a false acknowledgement --
				// and is recorded as such, since the value's provenance alone
				// cannot see it (the old entry's index arrives in res.Data too).
				var appliedBefore uint64
				if leader := cluster.leaderHost(shardID); leader != nil {
					if info, infoErr := leader.GetShardInfo(shardID); infoErr == nil {
						appliedBefore = info.LastApplied
					}
				}
				value := fmt.Sprintf("v-%d-%d", clientID, turn)
				ctx, cancel := context.WithTimeout(context.Background(), proposeTimeout)
				//quicraft:direct-propose the host under test is the point; a re-resolving helper would hide it
				res, err := host.SyncPropose(ctx, shardID, encodeKVPut(key, value))
				cancel()
				if err != nil {
					// A pinned client retries at once: the refusal it is most
					// likely seeing is "no leader known" from a replica that is
					// seconds from accepting, and the window is short.
					if clientID%2 == 1 {
						pause(time.Millisecond, done)
					} else {
						pause(retryBackoff(err), done)
					}
					continue
				}
				writes.Add(1)
				wrote := decodeIndex(res.Data)
				if wrote == 0 {
					t.Errorf("client %d: put result carried no index (%v)", clientID, res)
					return
				}
				if wrote <= appliedBefore {
					falseAcks.Add(1)
					record(staleRead{
						client: clientID, host: hostIdx, replica: uint64(hostIdx + 1), key: key,
						wrote: wrote, observed: appliedBefore, value: value,
						leader: cluster.findLeader(shardID),
					})
					continue
				}
				lastWrote = wrote

				// Read, taken apart the way SyncRead does it so the markers can be
				// sampled at the exact moment the read is released to Lookup.
				host, hostIdx = pickHost(clientID, turn)
				if host == nil {
					continue
				}
				ctx, cancel = context.WithTimeout(context.Background(), readTimeout)
				//quicraft:direct-read same reason as the propose above
				rs, err := host.ReadIndex(ctx, shardID)
				if err != nil {
					cancel()
					pause(retryBackoff(err), done)
					continue
				}
				var readIndex uint64
				select {
				case r := <-rs.ResultC():
					if r.Err != nil {
						rs.Release()
						cancel()
						pause(retryBackoff(r.Err), done)
						continue
					}
					readIndex = r.Value
				case <-ctx.Done():
					rs.Release()
					cancel()
					continue
				}
				rs.Release()

				cluster.hostsMu.RLock()
				tracker := cluster.trackers[hostIdx]
				stoppedNow := cluster.stopped[hostIdx].Load()
				cluster.hostsMu.RUnlock()
				replica := uint64(hostIdx + 1)
				var smApplied, hostApplied uint64
				if !stoppedNow {
					if machine := tracker.Get(shardID, replica); machine != nil {
						smApplied = machine.LastApplied()
					}
					if info, infoErr := host.GetShardInfo(shardID); infoErr == nil {
						hostApplied = info.LastApplied
					}
				}
				//quicraft:direct-read the Lookup half of SyncRead, on the host that confirmed the read index
				got, err := host.QueryLocalNode(ctx, shardID, indexedGet(key))
				cancel()
				if err != nil {
					pause(retryBackoff(err), done)
					continue
				}
				iv, ok := got.(indexedValue)
				if !ok {
					t.Errorf("client %d: indexed lookup returned %T", clientID, got)
					return
				}
				reads.Add(1)

				// A value written by an EARLIER turn can legitimately appear here
				// with a HIGHER index: that turn's proposal timed out, the client
				// moved on, and the entry landed afterwards -- the indeterminate
				// outcome porcupine models explicitly. So the value itself is
				// not compared; provenance is, against the write just
				// acknowledged and against everything this client has seen.
				if iv.Index < lastWrote || iv.Index < lastObserved {
					record(staleRead{
						client: clientID, host: hostIdx, replica: replica, key: key,
						wrote: lastWrote, readIndex: readIndex, smApplied: smApplied,
						hostApplied: hostApplied, observed: iv.Index, value: iv.Value,
						leader: cluster.findLeader(shardID),
					})
				}
				if iv.Index > lastObserved {
					lastObserved = iv.Index
				}
			}
		}(c)
	}

	workloadEnds := clientsStartedAt.Add(duration)
	workloadActive := func() bool { return time.Now().Before(workloadEnds.Add(-2 * time.Second)) }
	restartDeadline := time.Now().Add(duration - 5*time.Second)
	rounds := 0
	for round := 0; time.Now().Before(restartDeadline) && workloadActive(); round++ {
		idx := round % numNodes
		cluster.stopNode(idx)
		cluster.waitForClusterLeader(shardID, failoverTimeout)
		cluster.restartNode(idx, shardID)
		leaderIdx := cluster.waitForClusterLeader(shardID, failoverTimeout)
		waitForCatchUp(t, cluster, shardID, idx, leaderIdx, failoverTimeout, workloadActive)
		rounds++
		time.Sleep(500 * time.Millisecond)
	}

	wg.Wait()
	close(done)

	t.Logf("read-your-writes: %d restarts, %d writes, %d reads, %d false acknowledgements", rounds, writes.Load(), reads.Load(), falseAcks.Load())
	if rounds == 0 {
		t.Fatalf("no restart rounds completed in %v; increase %s", duration, soakDurationEnv)
	}
	if reads.Load() == 0 {
		t.Fatal("no reads completed; the check never ran")
	}
	if len(stales) > 0 {
		var sb strings.Builder
		for _, s := range stales {
			sb.WriteString("  " + s.String() + "\n")
		}
		t.Fatalf("%d violation(s) -- a write acknowledged with another entry's index (observed@ is the leader's applied marker before the write), or a read released after its own acknowledged write returning an older value:\n%s", len(stales), sb.String())
	}
}
