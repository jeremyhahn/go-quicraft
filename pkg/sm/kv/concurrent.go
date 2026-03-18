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

// Package kv provides key-value state machine implementations for Raft.
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

const numBuckets = 16

// bucket is a single shard of the concurrent store.
type bucket struct {
	mu   sync.RWMutex
	data map[string][]byte
}

// ConcurrentStore implements sm.ConcurrentStateMachine with sharded locking.
// It uses 16 buckets, each with its own RWMutex, for concurrent read access
// during writes. Bucket selection is via xxhash64 of the key.
type ConcurrentStore struct {
	buckets [numBuckets]bucket
}

// NewConcurrentStore creates a new ConcurrentStore.
func NewConcurrentStore() *ConcurrentStore {
	cs := &ConcurrentStore{}
	for i := range cs.buckets {
		cs.buckets[i].data = make(map[string][]byte)
	}
	return cs
}

// bucketIndex returns the bucket index for a given key using xxhash64.
func bucketIndex(key []byte) uint32 {
	return uint32(xxhash.Sum64(key) % numBuckets)
}

// Update applies committed entries. Acquires write locks per affected bucket.
func (cs *ConcurrentStore) Update(_ context.Context, entries []sm.Entry, results []sm.Result) error {
	for i, e := range entries {
		cmd, err := DecodeCommand(e.Cmd)
		if err != nil {
			return err
		}
		idx := bucketIndex(cmd.Key)
		b := &cs.buckets[idx]

		b.mu.Lock()
		switch cmd.Op {
		case OpPut:
			val := make([]byte, len(cmd.Value))
			copy(val, cmd.Value)
			b.data[string(cmd.Key)] = val
			results[i] = sm.Result{Value: uint64(len(cmd.Value))}
		case OpDelete:
			delete(b.data, string(cmd.Key))
			results[i] = sm.Result{Value: 1}
		case OpGet:
			results[i] = sm.Result{Value: 0}
		default:
			b.mu.Unlock()
			return &InvalidCommandError{Reason: "unknown op"}
		}
		b.mu.Unlock()
	}
	return nil
}

// Lookup reads a value by key. Acquires a read lock on the target bucket.
// For ListQuery, acquires read locks on all buckets to get a consistent view.
func (cs *ConcurrentStore) Lookup(_ context.Context, query interface{}) (interface{}, error) {
	if lq, ok := query.(ListQuery); ok {
		for i := range cs.buckets {
			cs.buckets[i].mu.RLock()
		}
		var keys []string
		for i := range cs.buckets {
			for k := range cs.buckets[i].data {
				if strings.HasPrefix(k, lq.Prefix) {
					keys = append(keys, k)
				}
			}
		}
		for i := range cs.buckets {
			cs.buckets[i].mu.RUnlock()
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
	idx := bucketIndex(key)
	b := &cs.buckets[idx]

	b.mu.RLock()
	val, exists := b.data[string(key)]
	b.mu.RUnlock()

	if !exists {
		return nil, &KeyNotFoundError{Key: string(key)}
	}
	cp := make([]byte, len(val))
	copy(cp, val)
	return cp, nil
}

// BytesLookup performs a byte-slice lookup, returning an owned copy.
func (cs *ConcurrentStore) BytesLookup(key []byte) ([]byte, error) {
	idx := bucketIndex(key)
	b := &cs.buckets[idx]

	b.mu.RLock()
	val, exists := b.data[string(key)]
	b.mu.RUnlock()

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
func (cs *ConcurrentStore) BytesLookupInto(key, dst []byte) ([]byte, error) {
	idx := bucketIndex(key)
	b := &cs.buckets[idx]

	b.mu.RLock()
	val, exists := b.data[string(key)]
	b.mu.RUnlock()

	if !exists {
		return nil, &KeyNotFoundError{Key: string(key)}
	}
	dst = dst[:0]
	dst = append(dst, val...)
	return dst, nil
}

// GetHash computes a deterministic xxhash64 hash of all key-value pairs
// across all buckets. Acquires read locks on all buckets.
func (cs *ConcurrentStore) GetHash() (uint64, error) {
	h := xxhash.New()

	// Collect all keys from all buckets.
	var allKeys []string
	for i := range cs.buckets {
		b := &cs.buckets[i]
		b.mu.RLock()
		for k := range b.data {
			allKeys = append(allKeys, k)
		}
		b.mu.RUnlock()
	}
	sort.Strings(allKeys)

	for _, k := range allKeys {
		idx := bucketIndex([]byte(k))
		b := &cs.buckets[idx]
		b.mu.RLock()
		if _, err := h.WriteString(k); err != nil {
			b.mu.RUnlock()
			return 0, &HashWriteError{Err: err}
		}
		if _, err := h.Write(b.data[k]); err != nil {
			b.mu.RUnlock()
			return 0, &HashWriteError{Err: err}
		}
		b.mu.RUnlock()
	}
	return h.Sum64(), nil
}

// PrepareSnapshot deep-clones all buckets atomically and returns the clone.
func (cs *ConcurrentStore) PrepareSnapshot() (interface{}, error) {
	// Lock all buckets to get a consistent snapshot.
	for i := range cs.buckets {
		cs.buckets[i].mu.RLock()
	}

	clone := make(map[string][]byte)
	for i := range cs.buckets {
		for k, v := range cs.buckets[i].data {
			cp := make([]byte, len(v))
			copy(cp, v)
			clone[k] = cp
		}
	}

	for i := range cs.buckets {
		cs.buckets[i].mu.RUnlock()
	}

	return clone, nil
}

// SaveSnapshot serializes the cloned data from PrepareSnapshot.
func (cs *ConcurrentStore) SaveSnapshot(_ context.Context, snapshotCtx interface{}, w io.Writer, stopper <-chan struct{}) error {
	data, ok := snapshotCtx.(map[string][]byte)
	if !ok {
		return &InvalidCommandError{Reason: "invalid snapshot context type"}
	}
	count := uint32(len(data))
	if err := binary.Write(w, binary.LittleEndian, count); err != nil {
		return err
	}
	for k, v := range data {
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

// RecoverFromSnapshot restores state from the reader, replacing all data.
func (cs *ConcurrentStore) RecoverFromSnapshot(_ context.Context, r io.Reader, stopper <-chan struct{}) error {
	var count uint32
	if err := binary.Read(r, binary.LittleEndian, &count); err != nil {
		return err
	}

	// Lock all buckets for the full restore.
	for i := range cs.buckets {
		cs.buckets[i].mu.Lock()
		cs.buckets[i].data = make(map[string][]byte)
	}

	for i := uint32(0); i < count; i++ {
		select {
		case <-stopper:
			for j := range cs.buckets {
				cs.buckets[j].mu.Unlock()
			}
			return context.Canceled
		default:
		}

		var keyLen uint32
		if err := binary.Read(r, binary.LittleEndian, &keyLen); err != nil {
			for j := range cs.buckets {
				cs.buckets[j].mu.Unlock()
			}
			return err
		}
		key := make([]byte, keyLen)
		if _, err := io.ReadFull(r, key); err != nil {
			for j := range cs.buckets {
				cs.buckets[j].mu.Unlock()
			}
			return err
		}

		var valLen uint32
		if err := binary.Read(r, binary.LittleEndian, &valLen); err != nil {
			for j := range cs.buckets {
				cs.buckets[j].mu.Unlock()
			}
			return err
		}
		val := make([]byte, valLen)
		if _, err := io.ReadFull(r, val); err != nil {
			for j := range cs.buckets {
				cs.buckets[j].mu.Unlock()
			}
			return err
		}

		idx := bucketIndex(key)
		cs.buckets[idx].data[string(key)] = val
	}

	for i := range cs.buckets {
		cs.buckets[i].mu.Unlock()
	}
	return nil
}

// Close releases all resources held by the store.
func (cs *ConcurrentStore) Close(_ context.Context) error {
	for i := range cs.buckets {
		cs.buckets[i].mu.Lock()
		cs.buckets[i].data = nil
		cs.buckets[i].mu.Unlock()
	}
	return nil
}

// Compile-time assertions.
var (
	_ sm.ConcurrentStateMachine = (*ConcurrentStore)(nil)
	_ sm.Hasher                 = (*ConcurrentStore)(nil)
	_ sm.NALookup               = (*ConcurrentStore)(nil)
	_ sm.NALookupInto           = (*ConcurrentStore)(nil)
)
