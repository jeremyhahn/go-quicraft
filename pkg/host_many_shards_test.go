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
	"sync"
	"testing"
	"time"

	"github.com/jeremyhahn/go-quicraft/pkg/config"
)

// manyShardsCount is the number of shards TestHost_ManyShards_* runs on one
// Host. docs/multi-group-host/user.md says a Host runs many shards sharing
// its WAL and workers; two hundred is far past what any other test starts.
const manyShardsCount = 200

// manyShardsProposals is the number of commands proposed to each shard.
const manyShardsProposals = 5

// manyShardsCmd is the i-th command proposed to a shard. The shard ID is in
// the bytes, so a command applied to the wrong shard changes its hash.
func manyShardsCmd(shardID uint64, i int) []byte {
	return []byte(fmt.Sprintf("shard=%d;seq=%d", shardID, i))
}

// manyShardsReplay is the hash hashingStateMachine reaches from base when it
// applies exactly cmds at indexes firstIndex, firstIndex+1, ...
func manyShardsReplay(base, firstIndex uint64, cmds [][]byte) uint64 {
	sum := base
	for i, cmd := range cmds {
		sum += (firstIndex + uint64(i)) * 1000003
		for _, b := range cmd {
			sum += uint64(b)
		}
	}
	return sum
}

func manyShardsStartAll(t *testing.T, h *Host, addr string) {
	t.Helper()
	members := map[uint64]string{1: addr}
	for id := uint64(1); id <= manyShardsCount; id++ {
		if err := h.StartShard(members, false, hashingCreateFunc, testShardConfig(id, 1)); err != nil {
			t.Fatalf("StartShard(%d): %v", id, err)
		}
	}
	for id := uint64(1); id <= manyShardsCount; id++ {
		waitForLeader(t, h, id)
	}
}

func manyShardsHash(t *testing.T, h *Host, shardID uint64) (uint64, uint64) {
	t.Helper()
	hash, idx, ok, err := h.StateMachineHash(shardID)
	if err != nil {
		t.Fatalf("StateMachineHash(%d): %v", shardID, err)
	}
	if !ok {
		t.Fatalf("StateMachineHash(%d): ok=false for a sm.Hasher", shardID)
	}
	return hash, idx
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
	cfg := testHostConfig(t)
	h := newManyShardsHost(t, cfg)
	closed := false
	t.Cleanup(func() {
		if !closed {
			h.Close()
		}
	})

	// (a) every shard starts and is counted and listed.
	manyShardsStartAll(t, h, cfg.RaftAddress)
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

	// (b) a distinct sequence per shard, from 16 goroutines; each shard's
	// hash is a replay of only its own sequence.
	baseHash := make(map[uint64]uint64, manyShardsCount)
	baseIdx := make(map[uint64]uint64, manyShardsCount)
	for id := uint64(1); id <= manyShardsCount; id++ {
		baseHash[id], baseIdx[id] = manyShardsHash(t, h, id)
	}
	const workers = 16
	var wg sync.WaitGroup
	errCh := make(chan error, manyShardsCount)
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for id := uint64(w + 1); id <= manyShardsCount; id += workers {
				for i := 0; i < manyShardsProposals; i++ {
					ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
					_, err := h.SyncPropose(ctx, id, manyShardsCmd(id, i))
					cancel()
					if err != nil {
						errCh <- fmt.Errorf("SyncPropose(shard %d, seq %d): %w", id, i, err)
						return
					}
				}
			}
		}(w)
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		t.Fatalf("(b) %v", err)
	}
	before := make(map[uint64]uint64, manyShardsCount)
	for id := uint64(1); id <= manyShardsCount; id++ {
		cmds := make([][]byte, manyShardsProposals)
		for i := range cmds {
			cmds[i] = manyShardsCmd(id, i)
		}
		hash, idx := manyShardsHash(t, h, id)
		if idx != baseIdx[id]+manyShardsProposals {
			t.Fatalf("(b) shard %d applied index %d, want %d: it applied entries "+
				"that were not its own or lost some", id, idx, baseIdx[id]+manyShardsProposals)
		}
		if want := manyShardsReplay(baseHash[id], baseIdx[id]+1, cmds); hash != want {
			t.Fatalf("(b) shard %d hash %d, want %d from a replay of only its own sequence",
				id, hash, want)
		}
		before[id] = hash
	}

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
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		_, err := h.SyncPropose(ctx, id, manyShardsCmd(id, manyShardsProposals))
		cancel()
		if id%2 == 1 && err != nil {
			t.Fatalf("(c) SyncPropose on running shard %d: %v", id, err)
		}
		if id%2 == 0 && !errors.Is(err, ErrShardNotFound) {
			t.Fatalf("(c) SyncPropose on stopped shard %d: err = %v, want ErrShardNotFound",
				id, err)
		}
	}
	// The odd shards applied one more entry; record their new hashes.
	for id := uint64(1); id <= manyShardsCount; id += 2 {
		before[id], _ = manyShardsHash(t, h, id)
	}

	// (d) a new Host on the same directories recovers every shard's state.
	if err := h.Close(); err != nil {
		t.Fatalf("(d) Close: %v", err)
	}
	closed = true
	h2 := newManyShardsHost(t, cfg)
	t.Cleanup(func() { h2.Close() })
	manyShardsStartAll(t, h2, cfg.RaftAddress)
	if n := h2.ShardCount(); n != manyShardsCount {
		t.Fatalf("(d) ShardCount after restart = %d, want %d", n, manyShardsCount)
	}
	for id := uint64(1); id <= manyShardsCount; id++ {
		if hash, _ := manyShardsHash(t, h2, id); hash != before[id] {
			t.Fatalf("(d) shard %d hash after restart %d, want %d from before the restart",
				id, hash, before[id])
		}
	}
}
