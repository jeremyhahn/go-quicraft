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
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/jeremyhahn/go-quicraft/pkg/config"
	"github.com/jeremyhahn/go-quicraft/pkg/sm"
)

// manyShardsCount is the number of shards TestHost_ManyShards_* runs on one
// Host. docs/multi-group-host/user.md says a Host runs many shards sharing
// its WAL and workers; two hundred is far past what any other test starts.
const manyShardsCount = 200

// manyShardsProposals is the number of commands proposed to each shard.
const manyShardsProposals = 5

// manyShardsCmd is the i-th command proposed to a shard. The shard ID is in
// the bytes, so a command applied to the wrong shard is a different command.
func manyShardsCmd(shardID uint64, i int) []byte {
	return []byte(fmt.Sprintf("shard=%d;seq=%d", shardID, i))
}

// manyShardsApplied is one command a shard's state machine applied, with the
// log index it was applied at.
type manyShardsApplied struct {
	Index uint64 `json:"index"`
	Cmd   string `json:"cmd"`
}

// manyShardsRecordingSM records every command-bearing entry it applies, in
// apply order, with its index. Unlike a digest it keeps each command's
// identity and its association with its index, so a command applied to the
// wrong shard, at the wrong index, or out of order is visible. The record is
// carried through snapshots, so a recovered shard reports the same sequence.
type manyShardsRecordingSM struct {
	mockStateMachine
	mu      sync.Mutex
	applied []manyShardsApplied
}

func (m *manyShardsRecordingSM) Update(ctx context.Context, entries []sm.Entry, results []sm.Result) error {
	m.mu.Lock()
	for i := range entries {
		if len(entries[i].Cmd) == 0 {
			continue
		}
		m.applied = append(m.applied, manyShardsApplied{
			Index: entries[i].Index,
			Cmd:   string(entries[i].Cmd),
		})
	}
	m.mu.Unlock()
	return m.mockStateMachine.Update(ctx, entries, results)
}

// Lookup returns a copy of the record, whatever the query.
func (m *manyShardsRecordingSM) Lookup(_ context.Context, _ interface{}) (interface{}, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]manyShardsApplied(nil), m.applied...), nil
}

func (m *manyShardsRecordingSM) SaveSnapshot(_ context.Context, w io.Writer, _ <-chan struct{}) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	return json.NewEncoder(w).Encode(m.applied)
}

func (m *manyShardsRecordingSM) RecoverFromSnapshot(_ context.Context, r io.Reader, _ <-chan struct{}) error {
	var applied []manyShardsApplied
	if err := json.NewDecoder(r).Decode(&applied); err != nil {
		return err
	}
	m.mu.Lock()
	m.applied = applied
	m.mu.Unlock()
	return nil
}

func manyShardsRecordingCreateFunc(_, _ uint64) interface{} {
	return &manyShardsRecordingSM{}
}

// manyShardsCompare reports the first difference between the sequence a shard
// applied and the sequence it must have applied: exactly its own commands, in
// order, each at its own index. nil means the two are identical.
func manyShardsCompare(ctx context.Context, shardID uint64, got, want []manyShardsApplied) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	for i := 0; i < len(got) && i < len(want); i++ {
		if got[i] != want[i] {
			return fmt.Errorf("shard %d: applied #%d is (index %d, %q), want (index %d, %q)",
				shardID, i, got[i].Index, got[i].Cmd, want[i].Index, want[i].Cmd)
		}
	}
	if len(got) != len(want) {
		return fmt.Errorf("shard %d: applied %d commands, want %d", shardID, len(got), len(want))
	}
	return nil
}

// manyShardsRecord reads a shard's applied sequence through a linearizable read.
func manyShardsRecord(ctx context.Context, t *testing.T, h *Host, shardID uint64) []manyShardsApplied {
	t.Helper()
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	v, err := h.SyncRead(ctx, shardID, nil)
	if err != nil {
		t.Fatalf("SyncRead(%d): %v", shardID, err)
	}
	rec, ok := v.([]manyShardsApplied)
	if !ok {
		t.Fatalf("SyncRead(%d) returned %T, want []manyShardsApplied", shardID, v)
	}
	return rec
}

// manyShardsCheckAll fails the test unless every shard's applied sequence is
// exactly want[id].
func manyShardsCheckAll(ctx context.Context, t *testing.T, h *Host, step string, want map[uint64][]manyShardsApplied) {
	t.Helper()
	for id := uint64(1); id <= manyShardsCount; id++ {
		if err := manyShardsCompare(ctx, id, manyShardsRecord(ctx, t, h, id), want[id]); err != nil {
			t.Fatalf("(%s) %v", step, err)
		}
	}
}

func manyShardsStartAll(ctx context.Context, t *testing.T, h *Host, addr string) {
	t.Helper()
	if err := ctx.Err(); err != nil {
		t.Fatalf("manyShardsStartAll: %v", err)
	}
	members := map[uint64]string{1: addr}
	for id := uint64(1); id <= manyShardsCount; id++ {
		if err := h.StartShard(members, false, manyShardsRecordingCreateFunc, testShardConfig(id, 1)); err != nil {
			t.Fatalf("StartShard(%d): %v", id, err)
		}
	}
	for id := uint64(1); id <= manyShardsCount; id++ {
		waitForLeader(t, h, id)
	}
}

// manyShardsPropose proposes cmd to a shard and returns what the shard must
// record for it: the command at the index its result reports.
func manyShardsPropose(ctx context.Context, h *Host, shardID uint64, cmd []byte) (manyShardsApplied, error) {
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	res, err := h.SyncPropose(ctx, shardID, cmd)
	if err != nil {
		return manyShardsApplied{}, err
	}
	return manyShardsApplied{Index: res.Value, Cmd: string(cmd)}, nil
}

// TestHost_ManyShards_OracleRejectsSwapAndReorder pins that the isolation
// oracle cannot be fooled the way a byte-sum digest was: shards 12 and 21
// propose commands with equal byte sums at equal indexes, so swapping their
// streams kept every sum; reordering commands within one shard kept it too.
func TestHost_ManyShards_OracleRejectsSwapAndReorder(t *testing.T) {
	ctx := context.Background()
	stream := func(shardID uint64) []manyShardsApplied {
		out := make([]manyShardsApplied, manyShardsProposals)
		for i := range out {
			out[i] = manyShardsApplied{Index: uint64(3 + i), Cmd: string(manyShardsCmd(shardID, i))}
		}
		return out
	}
	want12, want21 := stream(12), stream(21)
	if err := manyShardsCompare(ctx, 12, stream(12), want12); err != nil {
		t.Fatalf("identical stream rejected: %v", err)
	}
	if err := manyShardsCompare(ctx, 12, want21, want12); err == nil {
		t.Fatal("shard 21's stream accepted as shard 12's")
	}
	if err := manyShardsCompare(ctx, 21, want12, want21); err == nil {
		t.Fatal("shard 12's stream accepted as shard 21's")
	}
	reordered := stream(12)
	reordered[1].Cmd, reordered[2].Cmd = reordered[2].Cmd, reordered[1].Cmd
	if err := manyShardsCompare(ctx, 12, reordered, want12); err == nil {
		t.Fatal("stream with two commands swapped accepted")
	}
	shifted := stream(12)
	shifted[4].Index++
	if err := manyShardsCompare(ctx, 12, shifted, want12); err == nil {
		t.Fatal("stream with a command at the wrong index accepted")
	}
	if err := manyShardsCompare(ctx, 12, want12[:4], want12); err == nil {
		t.Fatal("stream missing its last command accepted")
	}
}

func newManyShardsHost(t *testing.T, cfg config.HostConfig) *Host {
	t.Helper()
	h, err := NewHost(cfg, WithoutTransport(), WithoutEncryption())
	if err != nil {
		t.Fatalf("NewHost: %v", err)
	}
	return h
}

// TestHost_ManyShards_IsolatedStopAndRestart pins the multi-group claim of
// docs/multi-group-host/user.md on a real WAL: two hundred shards on one
// Host each apply only their own entries, stop independently, and each
// recover their own state when a new Host opens the same directories.
func TestHost_ManyShards_IsolatedStopAndRestart(t *testing.T) {
	ctx := context.Background()
	cfg := testHostConfig(t)
	h := newManyShardsHost(t, cfg)
	closed := false
	t.Cleanup(func() {
		if !closed {
			h.Close()
		}
	})

	// (a) every shard starts and is counted and listed.
	manyShardsStartAll(ctx, t, h, cfg.RaftAddress)
	if n := h.ShardCount(); n != manyShardsCount {
		t.Fatalf("(a) ShardCount = %d, want %d", n, manyShardsCount)
	}
	info := h.NodeHostInfo(NodeHostInfoOption{SkipLogInfo: true})
	listed := make(map[uint64]bool, len(info.ShardInfoList))
	for _, si := range info.ShardInfoList {
		listed[si.ShardID] = true
	}
	if len(info.ShardInfoList) != manyShardsCount || len(listed) != manyShardsCount {
		t.Fatalf("(a) NodeHostInfo lists %d entries for %d distinct shards, want %d",
			len(info.ShardInfoList), len(listed), manyShardsCount)
	}
	want := make(map[uint64][]manyShardsApplied, manyShardsCount)
	manyShardsCheckAll(ctx, t, h, "a", want)

	// (b) a distinct sequence per shard, from 16 goroutines; each shard
	// applied exactly its own commands, in order, at the indexes its
	// proposals reported.
	expected := make([][]manyShardsApplied, manyShardsCount+1)
	const workers = 16
	var wg sync.WaitGroup
	errCh := make(chan error, manyShardsCount)
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for id := uint64(w + 1); id <= manyShardsCount; id += workers {
				for i := 0; i < manyShardsProposals; i++ {
					a, err := manyShardsPropose(ctx, h, id, manyShardsCmd(id, i))
					if err != nil {
						errCh <- fmt.Errorf("SyncPropose(shard %d, seq %d): %w", id, i, err)
						return
					}
					expected[id] = append(expected[id], a)
				}
			}
		}(w)
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		t.Fatalf("(b) %v", err)
	}
	for id := uint64(1); id <= manyShardsCount; id++ {
		want[id] = expected[id]
	}
	manyShardsCheckAll(ctx, t, h, "b", want)

	// (c) stopping the even shards leaves the odd ones serving.
	for id := uint64(2); id <= manyShardsCount; id += 2 {
		if err := h.StopShard(id); err != nil {
			t.Fatalf("(c) StopShard(%d): %v", id, err)
		}
	}
	if n := h.ShardCount(); n != manyShardsCount/2 {
		t.Fatalf("(c) ShardCount = %d after stopping the even shards, want %d",
			n, manyShardsCount/2)
	}
	for id := uint64(1); id <= manyShardsCount; id++ {
		a, err := manyShardsPropose(ctx, h, id, manyShardsCmd(id, manyShardsProposals))
		if id%2 == 1 {
			if err != nil {
				t.Fatalf("(c) SyncPropose on running shard %d: %v", id, err)
			}
			want[id] = append(want[id], a)
			continue
		}
		if !errors.Is(err, ErrShardNotFound) {
			t.Fatalf("(c) SyncPropose on stopped shard %d: err = %v, want ErrShardNotFound",
				id, err)
		}
	}
	for id := uint64(1); id <= manyShardsCount; id += 2 {
		if err := manyShardsCompare(ctx, id, manyShardsRecord(ctx, t, h, id), want[id]); err != nil {
			t.Fatalf("(c) %v", err)
		}
	}

	// (d) a new Host on the same directories recovers every shard's exact
	// sequence: its own commands, in order, at their indexes.
	if err := h.Close(); err != nil {
		t.Fatalf("(d) Close: %v", err)
	}
	closed = true
	h2 := newManyShardsHost(t, cfg)
	t.Cleanup(func() { h2.Close() })
	manyShardsStartAll(ctx, t, h2, cfg.RaftAddress)
	if n := h2.ShardCount(); n != manyShardsCount {
		t.Fatalf("(d) ShardCount after restart = %d, want %d", n, manyShardsCount)
	}
	manyShardsCheckAll(ctx, t, h2, "d", want)
}
