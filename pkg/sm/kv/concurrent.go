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

	// ops is a scratch slice reused across Update calls for the decode
	// pass, avoiding a per-batch allocation. Update is serialized on the
	// apply worker, so no lock guards it; entries alias Entry.Cmd only for
	// the duration of a single Update call.
	ops []decodedOp
}

// decodedOp is a pre-validated command staged during the first pass of
// Update, paired with its target bucket. valid is false for entries whose
// payload failed to decode; those receive the reserved
// sm.ResultInvalidCommand result and are skipped during the apply pass.
type decodedOp struct {
	cmd    Command
	bucket uint32
	valid  bool
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

// Update applies committed entries.
//
// The entries are committed Raft log entries: they MUST be applied
// deterministically on every replica, so a payload that fails to decode
// produces the reserved sm.ResultInvalidCommand result for that entry
// rather than an Update error (the engine treats an Update error as
// terminal for the shard).
//
// All entries are decoded before any mutation, and the write locks of every
// affected bucket are held together for the whole apply pass, so the batch
// is all-or-nothing AND concurrent Lookups never observe a partially
// applied batch. Buckets are locked in ascending index order -- the same
// order used by Lookup(ListQuery) and PrepareSnapshot -- so lock
// acquisition cannot deadlock.
func (cs *ConcurrentStore) Update(_ context.Context, entries []sm.Entry, results []sm.Result) error {
	// Pass 1: decode every command before touching any bucket. An
	// undecodable payload is marked invalid and its reserved per-entry
	// result is written now.
	if cap(cs.ops) < len(entries) {
		cs.ops = make([]decodedOp, len(entries))
	}
	ops := cs.ops[:len(entries)]
	var affected [numBuckets]bool
	for i, e := range entries {
		cmd, err := DecodeCommand(e.Cmd)
		if err != nil {
			ops[i] = decodedOp{}
			results[i] = sm.Result{Value: sm.ResultInvalidCommand}
			continue
		}
		switch cmd.Op {
		case OpPut, OpGet, OpDelete, OpPutIfAbsent:
		default:
			// Unreachable while DecodeCommand validates the opcode
			// range; kept so a future opcode gap stays deterministic.
			ops[i] = decodedOp{}
			results[i] = sm.Result{Value: sm.ResultInvalidCommand}
			continue
		}
		idx := bucketIndex(cmd.Key)
		ops[i] = decodedOp{cmd: cmd, bucket: idx, valid: true}
		if cmd.Op != OpGet {
			affected[idx] = true
		}
	}

	// Pass 2: apply under all affected bucket locks at once. No error
	// return can occur past this point, so the batch applies fully.
	for i := range cs.buckets {
		if affected[i] {
			cs.buckets[i].mu.Lock()
		}
	}
	for i := range ops {
		if !ops[i].valid {
			continue
		}
		cmd := &ops[i].cmd
		b := &cs.buckets[ops[i].bucket]
		switch cmd.Op {
		case OpPut:
			val := make([]byte, len(cmd.Value))
			copy(val, cmd.Value)
			b.data[string(cmd.Key)] = val
			results[i] = sm.Result{Value: uint64(len(cmd.Value))}
		case OpPutIfAbsent:
			// Evaluated here, under the bucket lock and against replicated
			// state, so every replica reaches the same decision. See
			// handlePutIfAbsent in memory.go for why the check cannot live on
			// the proposer.
			if _, exists := b.data[string(cmd.Key)]; exists {
				results[i] = sm.Result{Value: ResultNotStored}
				break
			}
			val := make([]byte, len(cmd.Value))
			copy(val, cmd.Value)
			b.data[string(cmd.Key)] = val
			results[i] = sm.Result{Value: ResultStored}
		case OpDelete:
			delete(b.data, string(cmd.Key))
			results[i] = sm.Result{Value: 1}
		case OpGet:
			// Get in Update is a no-op (reads go through Lookup).
			results[i] = sm.Result{Value: 0}
		}
	}
	for i := range cs.buckets {
		if affected[i] {
			cs.buckets[i].mu.Unlock()
		}
	}
	// Drop aliases into Entry.Cmd buffers, which the engine may recycle
	// after Update returns (Config.ZeroCopyEntryCmd).
	clear(ops)
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
// across all buckets. The read locks of EVERY bucket are held together for
// the whole computation so the hash reflects a state that actually existed
// atomically — releasing and re-acquiring per key would let a concurrent
// Update interleave, producing a hash of a state no replica ever held and
// raising false divergence alarms. Buckets are locked in ascending index
// order, matching Lookup(ListQuery)/PrepareSnapshot/Update, so lock
// acquisition cannot deadlock. Each key and value is hashed via
// hashLenPrefixed so pair boundaries are unambiguous.
func (cs *ConcurrentStore) GetHash() (uint64, error) {
	for i := range cs.buckets {
		cs.buckets[i].mu.RLock()
	}
	defer func() {
		for i := range cs.buckets {
			cs.buckets[i].mu.RUnlock()
		}
	}()

	// Collect keys and values together under the held locks.
	total := 0
	for i := range cs.buckets {
		total += len(cs.buckets[i].data)
	}
	allKeys := make([]string, 0, total)
	pairs := make(map[string][]byte, total)
	for i := range cs.buckets {
		for k, v := range cs.buckets[i].data {
			allKeys = append(allKeys, k)
			pairs[k] = v
		}
	}
	sort.Strings(allKeys)

	h := xxhash.New()
	for _, k := range allKeys {
		if err := hashLenPrefixed(h, k, pairs[k]); err != nil {
			return 0, err
		}
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
	// Serialise in key order, as MemoryStore does, so identical state gives
	// identical snapshot bytes on every replica and in both store kinds. The
	// decoder is order-independent, so this changes only what is written.
	keys := make([]string, 0, len(data))
	for k := range data {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		v := data[k]
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
//
// The entire stream is read and validated into local staging maps BEFORE
// any live bucket is touched: a truncated or corrupt stream must return an
// error while leaving the existing state fully intact. Only after a clean
// full parse are the bucket write locks taken and the staged maps swapped
// in atomically (mirrors MemoryStore.RecoverFromSnapshot).
func (cs *ConcurrentStore) RecoverFromSnapshot(_ context.Context, r io.Reader, stopper <-chan struct{}) error {
	var count uint32
	if err := binary.Read(r, binary.LittleEndian, &count); err != nil {
		return err
	}

	// count is an untrusted length prefix from a peer snapshot. Cap the
	// per-bucket capacity hint so a hostile count cannot force a huge
	// preallocation; the maps grow organically past the hint if needed.
	hint := int(count)
	if hint > maxSnapshotMapHint {
		hint = maxSnapshotMapHint
	}
	var staged [numBuckets]map[string][]byte
	for i := range staged {
		staged[i] = make(map[string][]byte, hint/numBuckets)
	}

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
		if keyLen > MaxKeyLength {
			return errCorruptSnapshotLength
		}
		key := make([]byte, keyLen)
		if _, err := io.ReadFull(r, key); err != nil {
			return err
		}

		var valLen uint32
		if err := binary.Read(r, binary.LittleEndian, &valLen); err != nil {
			return err
		}
		if valLen > MaxValueLength {
			return errCorruptSnapshotLength
		}
		val := make([]byte, valLen)
		if _, err := io.ReadFull(r, val); err != nil {
			return err
		}

		staged[bucketIndex(key)][string(key)] = val
	}

	// Full stream parsed cleanly; install the staged maps atomically.
	// Ascending lock order matches Lookup(ListQuery)/PrepareSnapshot/Update.
	for i := range cs.buckets {
		cs.buckets[i].mu.Lock()
	}
	for i := range cs.buckets {
		cs.buckets[i].data = staged[i]
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
