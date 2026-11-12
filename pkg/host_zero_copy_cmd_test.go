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
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"hash/fnv"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/jeremyhahn/go-quicraft/pkg/sm"
	"github.com/jeremyhahn/go-quicraft/pkg/sm/kv"
)

const (
	zeroCopyCmdShard        = 1
	zeroCopyCmdPayloads     = 300
	zeroCopyCmdPayloadSize  = 512
	zeroCopyCmdProposers    = 4
	zeroCopyCmdKVKeys       = 200
	zeroCopyCmdProposeLimit = 10 * time.Second
)

// keepingCmdSM keeps, for every applied entry, both the raw Entry.Cmd slice
// (exactly as the engine handed it over) and an Entry.CopyCmd copy. Its
// hash is an order-independent sum over the payloads it applied, computed
// inside Update while Cmd is guaranteed valid. The result of each entry is
// its log index, so a proposer learns where its payload landed.
type keepingCmdSM struct {
	mu     sync.Mutex
	raw    map[uint64][]byte
	copies map[uint64][]byte
	sum    uint64
}

func newKeepingCmdSM() *keepingCmdSM {
	return &keepingCmdSM{
		raw:    make(map[uint64][]byte),
		copies: make(map[uint64][]byte),
	}
}

func (s *keepingCmdSM) Update(_ context.Context, entries []sm.Entry, results []sm.Result) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	for i := range entries {
		e := &entries[i]
		results[i] = sm.Result{Value: e.Index}
		if len(e.Cmd) == 0 {
			continue
		}
		s.raw[e.Index] = e.Cmd
		s.copies[e.Index] = e.CopyCmd()
		h := fnv.New64a()
		_, _ = h.Write(e.Cmd)
		s.sum += h.Sum64()
	}
	return nil
}

func (s *keepingCmdSM) Lookup(_ context.Context, query interface{}) (interface{}, error) {
	return query, nil
}

func (s *keepingCmdSM) SaveSnapshot(context.Context, io.Writer, <-chan struct{}) error {
	return nil
}

func (s *keepingCmdSM) RecoverFromSnapshot(context.Context, io.Reader, <-chan struct{}) error {
	return nil
}

func (s *keepingCmdSM) Close(context.Context) error { return nil }

func (s *keepingCmdSM) GetHash() (uint64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.sum, nil
}

// zeroCopyCmdPayload returns the i-th distinct, deterministic payload.
func zeroCopyCmdPayload(i int) []byte {
	p := make([]byte, zeroCopyCmdPayloadSize)
	x := uint64(i)*0x9E3779B97F4A7C15 + 1
	for off := 0; off < len(p); off += 8 {
		x ^= x << 13
		x ^= x >> 7
		x ^= x << 17
		binary.LittleEndian.PutUint64(p[off:], x)
	}
	binary.LittleEndian.PutUint64(p, uint64(i))
	return p
}

// zeroCopyCmdHost starts a single-voter transportless Host with one shard
// whose ZeroCopyEntryCmd is set as given, and waits for its leader.
func zeroCopyCmdHost(t *testing.T, zeroCopy bool, create sm.CreateFunc) *Host {
	t.Helper()
	h, err := NewHost(testHostConfig(t), WithMemoryLogDB(), WithoutTransport(), WithoutEncryption())
	if err != nil {
		t.Fatalf("NewHost: %v", err)
	}
	t.Cleanup(func() { h.Close() })
	scfg := testShardConfig(zeroCopyCmdShard, 1)
	scfg.ZeroCopyEntryCmd = zeroCopy
	members := map[uint64]string{1: "127.0.0.1:5000"}
	if err := h.StartShard(members, false, create, scfg); err != nil {
		t.Fatalf("StartShard: %v", err)
	}
	waitForLeader(t, h, zeroCopyCmdShard)
	return h
}

// runZeroCopyCmdWorkload proposes the payload set: the first half in
// sequence, the second half from zeroCopyCmdProposers goroutines. It
// returns the state machine, the payload proposed at each applied index,
// and the state machine hash reported by the Host.
func runZeroCopyCmdWorkload(t *testing.T, zeroCopy bool) (*keepingCmdSM, map[uint64][]byte, uint64) {
	t.Helper()
	keeper := newKeepingCmdSM()
	create := sm.NewCreateFunc(func(_, _ uint64) sm.StateMachine { return keeper })
	h := zeroCopyCmdHost(t, zeroCopy, create)

	var (
		mu    sync.Mutex
		byIdx = make(map[uint64][]byte, zeroCopyCmdPayloads)
	)
	propose := func(i int) error {
		payload := zeroCopyCmdPayload(i)
		ctx, cancel := context.WithTimeout(context.Background(), zeroCopyCmdProposeLimit)
		defer cancel()
		res, err := h.SyncPropose(ctx, zeroCopyCmdShard, payload)
		if err != nil {
			return fmt.Errorf("SyncPropose payload %d: %w", i, err)
		}
		mu.Lock()
		defer mu.Unlock()
		if prev, dup := byIdx[res.Value]; dup {
			return fmt.Errorf("payload %d reported index %d already taken by payload %d",
				i, res.Value, binary.LittleEndian.Uint64(prev))
		}
		byIdx[res.Value] = payload
		return nil
	}

	half := zeroCopyCmdPayloads / 2
	for i := 0; i < half; i++ {
		if err := propose(i); err != nil {
			t.Fatal(err)
		}
	}
	var wg sync.WaitGroup
	errs := make(chan error, zeroCopyCmdPayloads)
	for g := 0; g < zeroCopyCmdProposers; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := half + g; i < zeroCopyCmdPayloads; i += zeroCopyCmdProposers {
				if err := propose(i); err != nil {
					errs <- err
					return
				}
			}
		}(g)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}
	if len(byIdx) != zeroCopyCmdPayloads {
		t.Fatalf("%d payloads acknowledged at distinct indexes, want %d", len(byIdx), zeroCopyCmdPayloads)
	}

	hash, _, ok, err := h.StateMachineHash(zeroCopyCmdShard)
	if err != nil {
		t.Fatalf("StateMachineHash: %v", err)
	}
	if !ok {
		t.Fatal("StateMachineHash reported ok=false for a state machine implementing sm.Hasher")
	}
	return keeper, byIdx, hash
}

// TestZeroCopyCmd_DefaultOwnedCopyMayBeKept pins (a): with ZeroCopyEntryCmd
// false, Entry.Cmd is an owned copy, so every raw slice a state machine
// kept past Update still holds the payload proposed at its index after the
// whole workload has been applied.
func TestZeroCopyCmd_DefaultOwnedCopyMayBeKept(t *testing.T) {
	keeper, byIdx, _ := runZeroCopyCmdWorkload(t, false)
	keeper.mu.Lock()
	defer keeper.mu.Unlock()
	if len(keeper.raw) != zeroCopyCmdPayloads {
		t.Fatalf("state machine kept %d payloads, want %d", len(keeper.raw), zeroCopyCmdPayloads)
	}
	for idx, want := range byIdx {
		got, ok := keeper.raw[idx]
		if !ok {
			t.Fatalf("no entry applied at index %d", idx)
		}
		if !bytes.Equal(got, want) {
			t.Fatalf("raw Cmd kept at index %d no longer equals its payload: "+
				"the default must hand the state machine an owned copy", idx)
		}
	}
}

// TestZeroCopyCmd_CopyCmdAppliesEveryPayload pins (b): with ZeroCopyEntryCmd
// true, a state machine that copies with CopyCmd still holds every payload
// exactly, and its hash equals the hash of the default mode for the same
// payload set. Raw slices are not read: their reuse is allowed, not promised.
func TestZeroCopyCmd_CopyCmdAppliesEveryPayload(t *testing.T) {
	_, _, ownedHash := runZeroCopyCmdWorkload(t, false)
	keeper, byIdx, zeroCopyHash := runZeroCopyCmdWorkload(t, true)

	keeper.mu.Lock()
	n := len(keeper.copies)
	copies := make(map[uint64][]byte, n)
	for idx, c := range keeper.copies {
		copies[idx] = c
	}
	keeper.mu.Unlock()
	if n != zeroCopyCmdPayloads {
		t.Fatalf("state machine kept %d copies, want %d", n, zeroCopyCmdPayloads)
	}
	for idx, want := range byIdx {
		got, ok := copies[idx]
		if !ok {
			t.Fatalf("no entry applied at index %d", idx)
		}
		if !bytes.Equal(got, want) {
			t.Fatalf("CopyCmd copy at index %d differs from its payload", idx)
		}
	}
	if zeroCopyHash != ownedHash {
		t.Fatalf("state machine hash with ZeroCopyEntryCmd = %#x, without = %#x: "+
			"the same payload set must apply to the same state", zeroCopyHash, ownedHash)
	}
}

// TestZeroCopyCmd_ConcurrentKVStoreReadsBackEveryValue pins (c): the sm/kv
// concurrent store, which retains keys and values, copies what it keeps,
// so under ZeroCopyEntryCmd every key reads back the value written for it.
func TestZeroCopyCmd_ConcurrentKVStoreReadsBackEveryValue(t *testing.T) {
	h := zeroCopyCmdHost(t, true, kv.NewConcurrentCreateFunc())
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	key := func(i int) []byte { return []byte(fmt.Sprintf("zc-key-%03d", i)) }
	value := func(i int) []byte {
		return []byte(fmt.Sprintf("zc-value-%03d-%s", i, bytes.Repeat([]byte{byte('a' + i%26)}, 64+i%32)))
	}
	for i := 0; i < zeroCopyCmdKVKeys; i++ {
		cmd, err := kv.EncodePut(key(i), value(i))
		if err != nil {
			t.Fatalf("EncodePut %d: %v", i, err)
		}
		if _, err := h.SyncPropose(ctx, zeroCopyCmdShard, cmd); err != nil {
			t.Fatalf("SyncPropose key %d: %v", i, err)
		}
	}
	for i := 0; i < zeroCopyCmdKVKeys; i++ {
		got, err := h.SyncRead(ctx, zeroCopyCmdShard, key(i))
		if err != nil {
			t.Fatalf("SyncRead key %d: %v", i, err)
		}
		b, ok := got.([]byte)
		if !ok {
			t.Fatalf("SyncRead key %d returned %T, want []byte", i, got)
		}
		if !bytes.Equal(b, value(i)) {
			t.Fatalf("key %q read back %q, want %q", key(i), b, value(i))
		}
	}
}
