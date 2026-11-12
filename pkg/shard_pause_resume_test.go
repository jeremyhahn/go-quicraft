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
	"fmt"
	"hash/fnv"
	"sync"
	"testing"
	"time"

	"github.com/jeremyhahn/go-quicraft/pkg/sm"
)

// pauseResumeStateMachine hashes every applied command into an
// order-independent sum, so a test can compute the expected hash from the
// commands it proposed.
type pauseResumeStateMachine struct {
	mockStateMachine
	mu  sync.Mutex
	sum uint64
}

func pauseResumeCmdHash(cmd []byte) uint64 {
	f := fnv.New64a()
	_, _ = f.Write(cmd)
	return f.Sum64()
}

func (p *pauseResumeStateMachine) Update(ctx context.Context, entries []sm.Entry, results []sm.Result) error {
	p.mu.Lock()
	for i := range entries {
		if len(entries[i].Cmd) > 0 {
			p.sum += pauseResumeCmdHash(entries[i].Cmd)
		}
	}
	p.mu.Unlock()
	return p.mockStateMachine.Update(ctx, entries, results)
}

func (p *pauseResumeStateMachine) GetHash() (uint64, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.sum, nil
}

func pauseResumeCreateFunc(shardID, replicaID uint64) interface{} {
	return &pauseResumeStateMachine{}
}

// TestShardPauseResume_FiveCycles drives one shard through five
// PauseShard/ResumeShard cycles. In each, every request entry point refuses
// with ErrShardPaused, a second pause is idempotent, the hash covers every
// entry applied before the pause, and ResumeShard restores proposals and
// reads.
func TestShardPauseResume_FiveCycles(t *testing.T) {
	const shardID = 1
	h, err := NewHost(testHostConfig(t), WithMemoryLogDB(), WithoutTransport(), WithoutEncryption())
	if err != nil {
		t.Fatalf("NewHost: %v", err)
	}
	t.Cleanup(func() { h.Close() })
	if err := h.StartShard(map[uint64]string{1: "127.0.0.1:5000"}, false,
		pauseResumeCreateFunc, testShardConfig(shardID, 1)); err != nil {
		t.Fatalf("StartShard: %v", err)
	}
	waitForLeader(t, h, shardID)

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	var want uint64
	propose := func(cmd string) {
		t.Helper()
		if _, err := h.SyncPropose(ctx, shardID, []byte(cmd)); err != nil {
			t.Fatalf("SyncPropose(%s): %v", cmd, err)
		}
		want += pauseResumeCmdHash([]byte(cmd))
	}
	checkHash := func(when string) {
		t.Helper()
		got, _, ok, err := h.StateMachineHash(shardID)
		if err != nil || !ok {
			t.Fatalf("%s: StateMachineHash = ok %v, err %v", when, ok, err)
		}
		if got != want {
			t.Fatalf("%s: StateMachineHash = %#x, want %#x", when, got, want)
		}
	}

	for cycle := 0; cycle < 5; cycle++ {
		for i := 0; i < 4; i++ {
			propose(fmt.Sprintf("c%d-before-%d", cycle, i))
		}

		if err := h.PauseShard(ctx, shardID); err != nil {
			t.Fatalf("cycle %d: PauseShard: %v", cycle, err)
		}
		if err := h.PauseShard(ctx, shardID); err != nil {
			t.Fatalf("cycle %d: second PauseShard = %v, want nil (idempotent)", cycle, err)
		}
		if !shardPaused(t, h, shardID) {
			t.Fatalf("cycle %d: ShardInfo does not report the shard paused", cycle)
		}
		checkHash(fmt.Sprintf("cycle %d paused", cycle))

		refusals := []struct {
			name string
			call func() error
		}{
			{"SyncPropose", func() error {
				_, err := h.SyncPropose(ctx, shardID, []byte("refused"))
				return err
			}},
			{"Propose", func() error {
				rs, err := h.Propose(ctx, shardID, []byte("refused"))
				if rs != nil {
					rs.Release()
				}
				return err
			}},
			{"SyncRead", func() error {
				_, err := h.SyncRead(ctx, shardID, "q")
				return err
			}},
			{"ReadIndex", func() error {
				rs, err := h.ReadIndex(ctx, shardID)
				if rs != nil {
					rs.Release()
				}
				return err
			}},
			{"NewSession", func() error {
				_, err := h.NewSession(ctx, shardID)
				return err
			}},
		}
		for _, r := range refusals {
			if err := r.call(); !errors.Is(err, ErrShardPaused) {
				t.Errorf("cycle %d: %s while paused: err = %v, want ErrShardPaused", cycle, r.name, err)
			}
		}
		if t.Failed() {
			t.FailNow()
		}

		if err := h.ResumeShard(ctx, shardID); err != nil {
			t.Fatalf("cycle %d: ResumeShard: %v", cycle, err)
		}
		if shardPaused(t, h, shardID) {
			t.Fatalf("cycle %d: ShardInfo reports the shard paused after ResumeShard", cycle)
		}
		waitForLeader(t, h, shardID)
		checkHash(fmt.Sprintf("cycle %d resumed", cycle))

		propose(fmt.Sprintf("c%d-after", cycle))
		cmd := fmt.Sprintf("c%d-async", cycle)
		rs, err := h.Propose(ctx, shardID, []byte(cmd))
		if err != nil {
			t.Fatalf("cycle %d: Propose after resume: %v", cycle, err)
		}
		_, err = awaitResult(ctx, rs)
		rs.Release()
		if err != nil {
			t.Fatalf("cycle %d: Propose result after resume: %v", cycle, err)
		}
		want += pauseResumeCmdHash([]byte(cmd))

		if _, err := h.SyncRead(ctx, shardID, "q"); err != nil {
			t.Fatalf("cycle %d: SyncRead after resume: %v", cycle, err)
		}
		rs, err = h.ReadIndex(ctx, shardID)
		if err != nil {
			t.Fatalf("cycle %d: ReadIndex after resume: %v", cycle, err)
		}
		_, err = awaitResult(ctx, rs)
		rs.Release()
		if err != nil {
			t.Fatalf("cycle %d: ReadIndex result after resume: %v", cycle, err)
		}
		session, err := h.NewSession(ctx, shardID)
		if err != nil {
			t.Fatalf("cycle %d: NewSession after resume: %v", cycle, err)
		}
		_ = session
		checkHash(fmt.Sprintf("cycle %d end", cycle))
	}
}
