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
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/jeremyhahn/go-quicraft/pkg/sm"
)

// Command encoding for cachedKVDiskSM. Each command is one opcode byte, then
// the key, then (for puts) '=' and the value:
//
//	P key=value  put
//	A key=value  put-if-absent (declined when the key exists)
//	D key        delete
const (
	cachedKVOpPut         = 'P'
	cachedKVOpPutIfAbsent = 'A'
	cachedKVOpDelete      = 'D'

	// cachedKVResultApplied and cachedKVResultDeclined are the Result.Value
	// codes cachedKVDiskSM reports for every command.
	cachedKVResultApplied  uint64 = 1
	cachedKVResultDeclined uint64 = 0
)

func cachedKVCmd(op byte, key, value string) []byte {
	if op == cachedKVOpDelete {
		return append([]byte{op}, key...)
	}
	return append(append(append([]byte{op}, key...), '='), value...)
}

// parseCachedKVCmd splits a command into opcode, key and value. ok is false
// for an empty or malformed command (the leader's no-op among them).
func parseCachedKVCmd(cmd []byte) (op byte, key string, value []byte, ok bool) {
	if len(cmd) < 2 {
		return 0, "", nil, false
	}
	op, rest := cmd[0], cmd[1:]
	switch op {
	case cachedKVOpDelete:
		return op, string(rest), nil, true
	case cachedKVOpPut, cachedKVOpPutIfAbsent:
		i := bytes.IndexByte(rest, '=')
		if i <= 0 {
			return 0, "", nil, false
		}
		return op, string(rest[:i]), rest[i+1:], true
	default:
		return 0, "", nil, false
	}
}

// cachedKVExtract is the CacheConfig.ExtractKV for cachedKVDiskSM: a put or
// put-if-absent yields its key and value, a delete its key and a nil value.
func cachedKVExtract(cmd []byte) (string, []byte) {
	op, key, value, ok := parseCachedKVCmd(cmd)
	if !ok {
		return "", nil
	}
	if op == cachedKVOpDelete {
		return key, nil
	}
	return key, value
}

// cachedKVApplied is the CacheConfig.EntryApplied for cachedKVDiskSM.
func cachedKVApplied(_ []byte, result sm.Result) bool {
	return result.Value == cachedKVResultApplied
}

// cachedKVState is the file format of cachedKVDiskSM.
type cachedKVState struct {
	LastApplied uint64            `json:"last_applied"`
	KV          map[string][]byte `json:"kv"`
}

// cachedKVDiskSM is a file-backed key-value DiskStateMachine: its whole state
// and lastApplied are rewritten to kv.json in the Open dir on every Update,
// so a restarted shard recovers them from disk. Lookup takes a string or
// []byte key and returns an owned []byte, or nil for a missing key.
type cachedKVDiskSM struct {
	mu    sync.Mutex
	dir   string
	state cachedKVState
}

func (m *cachedKVDiskSM) path() string { return filepath.Join(m.dir, "kv.json") }

func (m *cachedKVDiskSM) Open(_ context.Context, dir string, _ <-chan struct{}) (uint64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.dir = dir
	m.state = cachedKVState{KV: map[string][]byte{}}
	if err := os.MkdirAll(dir, 0o750); err != nil {
		return 0, err
	}
	data, err := os.ReadFile(m.path())
	if os.IsNotExist(err) {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	if err := json.Unmarshal(data, &m.state); err != nil {
		return 0, err
	}
	if m.state.KV == nil {
		m.state.KV = map[string][]byte{}
	}
	return m.state.LastApplied, nil
}

func (m *cachedKVDiskSM) persistLocked() error {
	data, err := json.Marshal(&m.state)
	if err != nil {
		return err
	}
	tmp := m.path() + ".tmp"
	if err := os.WriteFile(tmp, data, 0o600); err != nil {
		return err
	}
	return os.Rename(tmp, m.path())
}

func (m *cachedKVDiskSM) Update(_ context.Context, entries []sm.Entry, results []sm.Result) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	for i := range entries {
		m.state.LastApplied = entries[i].Index
		op, key, value, ok := parseCachedKVCmd(entries[i].Cmd)
		if !ok {
			results[i] = sm.Result{Value: cachedKVResultDeclined}
			continue
		}
		switch op {
		case cachedKVOpPut:
			m.state.KV[key] = append([]byte(nil), value...)
			results[i] = sm.Result{Value: cachedKVResultApplied}
		case cachedKVOpPutIfAbsent:
			if _, exists := m.state.KV[key]; exists {
				results[i] = sm.Result{Value: cachedKVResultDeclined}
				continue
			}
			m.state.KV[key] = append([]byte(nil), value...)
			results[i] = sm.Result{Value: cachedKVResultApplied}
		case cachedKVOpDelete:
			delete(m.state.KV, key)
			results[i] = sm.Result{Value: cachedKVResultApplied}
		}
	}
	return m.persistLocked()
}

func (m *cachedKVDiskSM) Lookup(_ context.Context, query interface{}) (interface{}, error) {
	var key string
	switch q := query.(type) {
	case string:
		key = q
	case []byte:
		key = string(q)
	default:
		return nil, nil
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	v, ok := m.state.KV[key]
	if !ok {
		return nil, nil
	}
	return append([]byte(nil), v...), nil
}

func (m *cachedKVDiskSM) Sync() error                           { return nil }
func (m *cachedKVDiskSM) PrepareSnapshot() (interface{}, error) { return nil, nil }

func (m *cachedKVDiskSM) SaveSnapshot(_ context.Context, _ interface{}, w io.Writer, _ <-chan struct{}) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	return json.NewEncoder(w).Encode(&m.state)
}

func (m *cachedKVDiskSM) RecoverFromSnapshot(_ context.Context, r io.Reader, _ <-chan struct{}) error {
	var st cachedKVState
	if err := json.NewDecoder(r).Decode(&st); err != nil {
		return err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if st.KV == nil {
		st.KV = map[string][]byte{}
	}
	m.state = st
	return m.persistLocked()
}

func (m *cachedKVDiskSM) Close(_ context.Context) error { return nil }

var _ sm.DiskStateMachine = (*cachedKVDiskSM)(nil)

// TestHost_CachedDiskSM runs a CachedDiskStateMachine wrapping a file-backed
// key-value DiskStateMachine on a single-voter transportless Host with a real
// WAL, and checks the contract docs/multi-group-host/api.md
// ("CachedDiskStateMachine") states: reads are served from the cache, an
// overwrite, a delete or a declined conditional write is never served stale
// from it, a returned value is the caller's own copy, and every key keeps its
// last value across StopShard and StartShard.
func TestHost_CachedDiskSM(t *testing.T) {
	cfg := testHostConfig(t)
	h, err := NewHost(cfg, WithoutTransport(), WithoutEncryption())
	if err != nil {
		t.Fatalf("NewHost: %v", err)
	}
	defer h.Close()

	const shardID uint64 = 922
	var mu sync.Mutex
	var current *sm.CachedDiskStateMachine
	create := func(_, _ uint64) interface{} {
		c, cErr := sm.NewCachedDiskStateMachine(&cachedKVDiskSM{}, sm.CacheConfig{
			MaxEntries:   64,
			ExtractKV:    cachedKVExtract,
			EntryApplied: cachedKVApplied,
		})
		if cErr != nil {
			t.Errorf("NewCachedDiskStateMachine: %v", cErr)
			return nil
		}
		mu.Lock()
		current = c
		mu.Unlock()
		return c
	}
	cached := func() *sm.CachedDiskStateMachine {
		mu.Lock()
		defer mu.Unlock()
		return current
	}

	members := map[uint64]string{1: "127.0.0.1:5000"}
	scfg := testShardConfig(shardID, 1)
	if err := h.StartShard(members, false, create, scfg); err != nil {
		t.Fatalf("StartShard: %v", err)
	}
	waitForLeader(t, h, shardID)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	propose := func(op byte, key, value string) sm.Result {
		t.Helper()
		res, pErr := h.SyncPropose(ctx, shardID, cachedKVCmd(op, key, value))
		if pErr != nil {
			t.Fatalf("SyncPropose %c %s: %v", op, key, pErr)
		}
		return res
	}
	syncRead := func(key string) []byte {
		t.Helper()
		v, rErr := h.SyncRead(ctx, shardID, key)
		if rErr != nil {
			t.Fatalf("SyncRead %s: %v", key, rErr)
		}
		return cachedKVValue(t, key, v)
	}
	staleRead := func(key string) []byte {
		t.Helper()
		v, rErr := h.StaleRead(ctx, shardID, key)
		if rErr != nil {
			t.Fatalf("StaleRead %s: %v", key, rErr)
		}
		return cachedKVValue(t, key, v)
	}
	want := func(what string, got []byte, value string) {
		t.Helper()
		if got == nil || string(got) != value {
			t.Fatalf("%s = %q (nil=%v), want %q", what, got, got == nil, value)
		}
	}

	// (a) After a put, SyncRead and StaleRead return the value from the
	// cache: the write filled it, so each read is a hit.
	if res := propose(cachedKVOpPut, "alpha", "one"); res.Value != cachedKVResultApplied {
		t.Fatalf("put alpha result = %d, want %d", res.Value, cachedKVResultApplied)
	}
	hits0, _ := cached().CacheStats()
	want("(a) SyncRead alpha after put", syncRead("alpha"), "one")
	want("(a) StaleRead alpha after put", staleRead("alpha"), "one")
	hits1, _ := cached().CacheStats()
	if hits1 < hits0+2 {
		t.Fatalf("(a) cache hits after two reads of a written key = %d, want >= %d", hits1, hits0+2)
	}
	want("(a) repeated SyncRead alpha", syncRead("alpha"), "one")
	if hits2, _ := cached().CacheStats(); hits2 <= hits1 {
		t.Fatalf("(a) repeated read did not raise cache hits: %d -> %d", hits1, hits2)
	}

	// (b) An overwrite and a delete are visible to the next read: the cache
	// never serves the pre-write value.
	propose(cachedKVOpPut, "alpha", "two")
	want("(b) SyncRead alpha after overwrite", syncRead("alpha"), "two")
	want("(b) StaleRead alpha after overwrite", staleRead("alpha"), "two")
	propose(cachedKVOpPut, "gone", "soon")
	want("(b) SyncRead gone before delete", syncRead("gone"), "soon")
	propose(cachedKVOpDelete, "gone", "")
	if v := syncRead("gone"); v != nil {
		t.Fatalf("(b) SyncRead gone after delete = %q, want missing", v)
	}
	if v := staleRead("gone"); v != nil {
		t.Fatalf("(b) StaleRead gone after delete = %q, want missing", v)
	}

	// (c) A put-if-absent the store declines, reported through EntryApplied,
	// leaves the first value served.
	if res := propose(cachedKVOpPutIfAbsent, "once", "first"); res.Value != cachedKVResultApplied {
		t.Fatalf("(c) first put-if-absent result = %d, want applied", res.Value)
	}
	want("(c) SyncRead once after first put-if-absent", syncRead("once"), "first")
	if res := propose(cachedKVOpPutIfAbsent, "once", "second"); res.Value != cachedKVResultDeclined {
		t.Fatalf("(c) second put-if-absent result = %d, want declined", res.Value)
	}
	want("(c) SyncRead once after declined put-if-absent", syncRead("once"), "first")
	want("(c) StaleRead once after declined put-if-absent", staleRead("once"), "first")

	// (d) A returned value is the caller's own copy: changing it does not
	// change the next read, from the cache or from the store.
	for _, read := range []struct {
		name string
		fn   func(string) []byte
	}{{"SyncRead", syncRead}, {"StaleRead", staleRead}} {
		got := read.fn("alpha")
		want("(d) "+read.name+" alpha", got, "two")
		for i := range got {
			got[i] = 'X'
		}
		want("(d) "+read.name+" alpha after the caller changed its copy", read.fn("alpha"), "two")
	}

	// (e) After StopShard and StartShard the fresh wrapper starts with an
	// empty cache and every key reads back its last value from disk.
	first := cached()
	appliedBeforeStop := testLoadNode(h, shardID).lastApplied.Load()
	if err := h.StopShard(shardID); err != nil {
		t.Fatalf("StopShard: %v", err)
	}
	if err := h.StartShard(members, false, create, scfg); err != nil {
		t.Fatalf("StartShard (restart): %v", err)
	}
	waitForLeader(t, h, shardID)
	// The restarted replica reports the applied index it recovered from disk,
	// which already satisfies waitForLeader; ReadIndex serves only once the
	// new term's no-op, the next index, is applied.
	waitForApplied(t, h, shardID, appliedBeforeStop+1)
	if cached() == first {
		t.Fatal("(e) restart did not create a fresh cached state machine")
	}
	want("(e) SyncRead alpha after restart", syncRead("alpha"), "two")
	want("(e) StaleRead alpha after restart", staleRead("alpha"), "two")
	want("(e) SyncRead once after restart", syncRead("once"), "first")
	if v := syncRead("gone"); v != nil {
		t.Fatalf("(e) SyncRead gone after restart = %q, want missing", v)
	}

	// Writes after the restart are cached again.
	propose(cachedKVOpPut, "alpha", "three")
	hits3, _ := cached().CacheStats()
	want("(e) SyncRead alpha after a post-restart overwrite", syncRead("alpha"), "three")
	if hits4, _ := cached().CacheStats(); hits4 <= hits3 {
		t.Fatalf("(e) read of a post-restart write was not a cache hit: %d -> %d", hits3, hits4)
	}
}

// cachedKVValue converts a read result to []byte; a nil result is a missing
// key.
func cachedKVValue(t *testing.T, key string, v interface{}) []byte {
	t.Helper()
	if v == nil {
		return nil
	}
	b, ok := v.([]byte)
	if !ok {
		t.Fatalf("read %s returned %T, want []byte", key, v)
	}
	return b
}
