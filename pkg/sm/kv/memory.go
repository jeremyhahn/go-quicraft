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

package kv

import (
	"context"
	"encoding/binary"
	"io"
	"sort"
	"strings"
	"sync"

	"github.com/cespare/xxhash/v2"

	"github.com/jeremyhahn/go-quicraft/pkg/sm"
)

// opHandlerFunc is a handler for a specific operation within Update.
type opHandlerFunc func(m *MemoryStore, cmd Command) sm.Result

// opHandlers maps operation codes to their handler functions for O(1) dispatch.
var opHandlers = map[byte]opHandlerFunc{
	OpPut:    handlePut,
	OpGet:    handleGetNoop,
	OpDelete: handleDelete,
}

func handlePut(m *MemoryStore, cmd Command) sm.Result {
	val := make([]byte, len(cmd.Value))
	copy(val, cmd.Value)
	m.data[string(cmd.Key)] = val
	return sm.Result{Value: uint64(len(cmd.Value))}
}

func handleGetNoop(_ *MemoryStore, _ Command) sm.Result {
	// Get in Update is a no-op (reads go through Lookup).
	return sm.Result{Value: 0}
}

func handleDelete(m *MemoryStore, cmd Command) sm.Result {
	delete(m.data, string(cmd.Key))
	return sm.Result{Value: 1}
}

// MemoryStore is a simple in-memory KV state machine. A RWMutex protects
// the data map because SaveSnapshot may be called from the engine's snapshot
// pool concurrently with Update. Although QuicRaft serializes Update and Lookup
// calls, the mutex provides additional safety and allows SaveSnapshot to take a
// read lock without blocking Update.
type MemoryStore struct {
	mu   sync.RWMutex
	data map[string][]byte
}

// NewMemoryStore creates a new MemoryStore.
func NewMemoryStore() *MemoryStore {
	return &MemoryStore{data: make(map[string][]byte)}
}

// Update applies committed entries to the in-memory store.
func (m *MemoryStore) Update(_ context.Context, entries []sm.Entry, results []sm.Result) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	for i, e := range entries {
		cmd, err := DecodeCommand(e.Cmd)
		if err != nil {
			return err
		}
		handler, ok := opHandlers[cmd.Op]
		if !ok {
			return &InvalidCommandError{Reason: "unknown op"}
		}
		results[i] = handler(m, cmd)
	}
	return nil
}

// Lookup reads a value by key. Accepts []byte, string, or ListQuery query types.
func (m *MemoryStore) Lookup(_ context.Context, query interface{}) (interface{}, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if lq, ok := query.(ListQuery); ok {
		var keys []string
		for k := range m.data {
			if strings.HasPrefix(k, lq.Prefix) {
				keys = append(keys, k)
			}
		}
		sort.Strings(keys)
		if keys == nil {
			keys = []string{}
		}
		return keys, nil
	}
	key, ok := query.([]byte)
	if !ok {
		if s, ok := query.(string); ok {
			key = []byte(s)
		} else {
			return nil, &InvalidCommandError{Reason: "query must be []byte or string"}
		}
	}
	val, exists := m.data[string(key)]
	if !exists {
		return nil, &KeyNotFoundError{Key: string(key)}
	}
	cp := make([]byte, len(val))
	copy(cp, val)
	return cp, nil
}

// BytesLookup performs a byte-slice lookup, returning an owned copy.
func (m *MemoryStore) BytesLookup(key []byte) ([]byte, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	val, exists := m.data[string(key)]
	if !exists {
		return nil, &KeyNotFoundError{Key: string(key)}
	}
	cp := make([]byte, len(val))
	copy(cp, val)
	return cp, nil
}

// BytesLookupInto implements sm.NALookupInto. It copies the value for key
// into dst, growing it via append if necessary, and returns the populated
// sub-slice. The caller owns the returned slice and MUST NOT retain dst
// across a subsequent call that may use the same backing array.
func (m *MemoryStore) BytesLookupInto(key, dst []byte) ([]byte, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	val, exists := m.data[string(key)]
	if !exists {
		return nil, &KeyNotFoundError{Key: string(key)}
	}
	dst = dst[:0]
	dst = append(dst, val...)
	return dst, nil
}

// GetHash computes a deterministic xxhash64 hash of all key-value pairs.
func (m *MemoryStore) GetHash() (uint64, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	h := xxhash.New()
	keys := make([]string, 0, len(m.data))
	for k := range m.data {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		if _, err := h.WriteString(k); err != nil {
			return 0, &HashWriteError{Err: err}
		}
		if _, err := h.Write(m.data[k]); err != nil {
			return 0, &HashWriteError{Err: err}
		}
	}
	return h.Sum64(), nil
}

// SaveSnapshot serializes all key-value pairs to the writer.
// Format: [count:4 LE] then for each pair: [keyLen:4 LE][key][valLen:4 LE][val].
//
// A deep copy of the data map is taken under a read lock so that
// serialization proceeds without blocking concurrent Update calls.
func (m *MemoryStore) SaveSnapshot(_ context.Context, w io.Writer, stopper <-chan struct{}) error {
	// Deep-copy under read lock.
	m.mu.RLock()
	clone := make(map[string][]byte, len(m.data))
	for k, v := range m.data {
		cp := make([]byte, len(v))
		copy(cp, v)
		clone[k] = cp
	}
	m.mu.RUnlock()

	count := uint32(len(clone))
	if err := binary.Write(w, binary.LittleEndian, count); err != nil {
		return err
	}
	for k, v := range clone {
		select {
		case <-stopper:
			return context.Canceled
		default:
		}
		keyBytes := []byte(k)
		if err := binary.Write(w, binary.LittleEndian, uint32(len(keyBytes))); err != nil {
			return err
		}
		if _, err := w.Write(keyBytes); err != nil {
			return err
		}
		if err := binary.Write(w, binary.LittleEndian, uint32(len(v))); err != nil {
			return err
		}
		if _, err := w.Write(v); err != nil {
			return err
		}
	}
	return nil
}

// RecoverFromSnapshot restores state from the reader, replacing all
// existing data.
func (m *MemoryStore) RecoverFromSnapshot(_ context.Context, r io.Reader, stopper <-chan struct{}) error {
	var count uint32
	if err := binary.Read(r, binary.LittleEndian, &count); err != nil {
		return err
	}

	newData := make(map[string][]byte, int(count))
	for i := uint32(0); i < count; i++ {
		select {
		case <-stopper:
			return context.Canceled
		default:
		}
		var keyLen uint32
		if err := binary.Read(r, binary.LittleEndian, &keyLen); err != nil {
			return err
		}
		key := make([]byte, keyLen)
		if _, err := io.ReadFull(r, key); err != nil {
			return err
		}
		var valLen uint32
		if err := binary.Read(r, binary.LittleEndian, &valLen); err != nil {
			return err
		}
		val := make([]byte, valLen)
		if _, err := io.ReadFull(r, val); err != nil {
			return err
		}
		newData[string(key)] = val
	}

	m.mu.Lock()
	m.data = newData
	m.mu.Unlock()
	return nil
}

// Close releases all resources held by the store.
func (m *MemoryStore) Close(_ context.Context) error {
	m.mu.Lock()
	m.data = nil
	m.mu.Unlock()
	return nil
}

// Compile-time assertions.
var (
	_ sm.StateMachine = (*MemoryStore)(nil)
	_ sm.Hasher       = (*MemoryStore)(nil)
	_ sm.NALookup     = (*MemoryStore)(nil)
	_ sm.NALookupInto = (*MemoryStore)(nil)
)
