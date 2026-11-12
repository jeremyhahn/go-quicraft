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
	"fmt"
	"hash/fnv"
	"os"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/jeremyhahn/go-quicraft/pkg/sm"
)

// cycleStateMachine hashes every applied command. The digest is a sum of
// per-command hashes, so it is independent of the log indexes the commands
// land at (no-ops and membership entries carry no Cmd) and of the order a
// replay visits them in, while a lost entry or an entry applied twice changes
// it.
type cycleStateMachine struct {
	mockStateMachine
	mu  sync.Mutex
	sum uint64
}

func cycleCmdHash(cmd []byte) uint64 {
	f := fnv.New64a()
	_, _ = f.Write(cmd)
	return f.Sum64()
}

func (c *cycleStateMachine) Update(ctx context.Context, entries []sm.Entry, results []sm.Result) error {
	c.mu.Lock()
	for i := range entries {
		if len(entries[i].Cmd) > 0 {
			c.sum += cycleCmdHash(entries[i].Cmd)
		}
	}
	c.mu.Unlock()
	return c.mockStateMachine.Update(ctx, entries, results)
}

func (c *cycleStateMachine) GetHash() (uint64, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.sum, nil
}

func cycleCreateFunc(shardID, replicaID uint64) interface{} {
	return &cycleStateMachine{}
}

// openFDs counts this process's open file descriptors, or returns -1 where
// /proc/self/fd does not exist.
func openFDs(t *testing.T) int {
	t.Helper()
	if runtime.GOOS != "linux" {
		return -1
	}
	entries, err := os.ReadDir("/proc/self/fd")
	if err != nil {
		t.Fatalf("read /proc/self/fd: %v", err)
	}
	return len(entries)
}

// settledGoroutines returns runtime.NumGoroutine once it stops changing
// across a short settle (GC between reads), so a baseline is not taken while
// start-up goroutines are still exiting.
func settledGoroutines() int {
	deadline := time.Now().Add(2 * time.Second)
	runtime.GC()
	prev := runtime.NumGoroutine()
	for time.Now().Before(deadline) {
		time.Sleep(50 * time.Millisecond)
		runtime.GC()
		n := runtime.NumGoroutine()
		if n == prev {
			return n
		}
		prev = n
	}
	return prev
}

// settle polls until read() is at most limit, returning the last reading. Shard
// teardown is asynchronous (worker goroutines exit after StopShard returns),
// so a single sample would race it.
func settle(read func() int, limit int) int {
	deadline := time.Now().Add(5 * time.Second)
	n := read()
	for n > limit && time.Now().Before(deadline) {
		runtime.GC()
		time.Sleep(20 * time.Millisecond)
		n = read()
	}
	return n
}

// TestStopStartCycle_RecoversStateAndLeaksNothing stops and restarts one shard
// on one Host 10 times. Each restart must replay every acknowledged entry
// exactly once from the default WAL, and the cycles must not accumulate
// goroutines or file descriptors. pkg/host_restart_routing_test.go covers a
// single restart's routing only.
func TestStopStartCycle_RecoversStateAndLeaksNothing(t *testing.T) {
	const (
		shardID, replicaID uint64 = 794, 1
		cycles                    = 10
		perCycle                  = 5
		// The margin is below the number of measured cycles, so one leaked
		// goroutine per stopped incarnation fails the check.
		goroutineMargin = 3
		fdMargin        = 0
	)

	h, err := NewHost(testHostConfig(t), WithoutTransport(), WithoutEncryption())
	if err != nil {
		t.Fatalf("NewHost: %v", err)
	}
	t.Cleanup(func() { h.Close() })

	members := map[uint64]string{replicaID: "127.0.0.1:5000"}
	scfg := testShardConfig(shardID, replicaID)
	if err := h.StartShard(members, false, cycleCreateFunc, scfg); err != nil {
		t.Fatalf("StartShard: %v", err)
	}
	waitForLeader(t, h, shardID)

	var want uint64
	seq := 0
	// Baseline before cycle 0, with one incarnation running as after the last
	// cycle, so every one of the cycles is measured.
	baseGoroutines := settledGoroutines()
	baseFDs := openFDs(t)
	for cycle := 0; cycle < cycles; cycle++ {
		for i := 0; i < perCycle; i++ {
			cmd := []byte(fmt.Sprintf("cycle-%d-entry-%d", cycle, seq))
			seq++
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			_, perr := h.SyncPropose(ctx, shardID, cmd)
			cancel()
			if perr != nil {
				t.Fatalf("cycle %d: SyncPropose %q: %v", cycle, cmd, perr)
			}
			want += cycleCmdHash(cmd)
		}

		if err := h.StopShard(shardID); err != nil {
			t.Fatalf("cycle %d: StopShard: %v", cycle, err)
		}
		if err := h.StartShard(members, false, cycleCreateFunc, scfg); err != nil {
			t.Fatalf("cycle %d: StartShard: %v", cycle, err)
		}
		waitForLeader(t, h, shardID)

		var got uint64
		deadline := time.Now().Add(5 * time.Second)
		for {
			hash, _, ok, herr := h.StateMachineHash(shardID)
			if herr != nil {
				t.Fatalf("cycle %d: StateMachineHash: %v", cycle, herr)
			}
			if !ok {
				t.Fatalf("cycle %d: StateMachineHash reported no hasher", cycle)
			}
			got = hash
			if got == want || time.Now().After(deadline) {
				break
			}
			time.Sleep(5 * time.Millisecond)
		}
		if got != want {
			t.Fatalf("cycle %d: restarted state hash %d, want %d covering all %d "+
				"acknowledged entries: an entry was lost or applied twice",
				cycle, got, want, seq)
		}

	}

	if n := settle(runtime.NumGoroutine, baseGoroutines+goroutineMargin); n > baseGoroutines+goroutineMargin {
		buf := make([]byte, 1<<20)
		buf = buf[:runtime.Stack(buf, true)]
		t.Fatalf("goroutines grew from %d before the first cycle to %d after the "+
			"last %d cycles (margin %d):\n%s", baseGoroutines, n, cycles, goroutineMargin, buf)
	}

	if runtime.GOOS != "linux" {
		t.Log("not Linux: no /proc/self/fd; skipping the descriptor check")
		return
	}
	if n := settle(func() int { return openFDs(t) }, baseFDs+fdMargin); n > baseFDs+fdMargin {
		t.Fatalf("open file descriptors grew from %d before the first cycle to %d "+
			"after the last (margin %d)", baseFDs, n, fdMargin)
	}
}
