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
	"bytes"
	"context"
	"encoding/binary"
	"sync"
	"testing"

	"github.com/jeremyhahn/go-quicraft/pkg/sm"
)

// TestUpdate_MalformedEntryYieldsResultNotError is the regression test for
// A committed entry whose payload cannot be decoded (e.g. a single
// stray byte) must produce the reserved sm.ResultInvalidCommand result for
// that entry only. Update must return nil and the remaining entries in the
// batch must still apply, otherwise the engine marks the shard failed and
// the same committed entry re-wedges it on every restart.
func TestUpdate_MalformedEntryYieldsResultNotError(t *testing.T) {
	for _, b := range backends() {
		t.Run(b.name, func(t *testing.T) {
			ctx := context.Background()
			entries := []sm.Entry{
				{Index: 1, Cmd: mustEncodePut([]byte("before"), []byte("v1"))},
				{Index: 2, Cmd: []byte{0x00}}, // 1-byte junk proposal
				{Index: 3, Cmd: mustEncodePut([]byte("after"), []byte("v2"))},
			}
			results := make([]sm.Result, len(entries))
			if err := b.update(ctx, entries, results); err != nil {
				t.Fatalf("Update = %v, want nil for malformed committed entry", err)
			}
			if results[1].Value != sm.ResultInvalidCommand {
				t.Errorf("results[1].Value = %d, want ResultInvalidCommand", results[1].Value)
			}
			if results[0].Value != 2 {
				t.Errorf("results[0].Value = %d, want 2", results[0].Value)
			}
			if results[2].Value != 2 {
				t.Errorf("results[2].Value = %d, want 2", results[2].Value)
			}
			for _, key := range []string{"before", "after"} {
				if _, err := b.bytes([]byte(key)); err != nil {
					t.Errorf("key %q not applied after malformed sibling entry: %v", key, err)
				}
			}
		})
	}
}

// twoBucketKeys returns two keys that hash to different ConcurrentStore
// buckets, so a batch touching both exercises multi-bucket atomicity.
func twoBucketKeys(t *testing.T) ([]byte, []byte) {
	t.Helper()
	first := []byte("atomic-key-0")
	for i := 1; i < 1024; i++ {
		candidate := []byte("atomic-key-" + string(rune('a'+i%26)) + string(rune('a'+i/26)))
		if bucketIndex(candidate) != bucketIndex(first) {
			return first, candidate
		}
	}
	t.Fatal("could not find two keys in different buckets")
	return nil, nil
}

// TestConcurrentStore_UpdateBatchAtomicity is a regression test:
// a concurrent Lookup must never observe a partially applied Update batch.
// Each batch writes the same round number to two keys in different buckets
// (keyA first, then keyB). A reader that loads keyA and then keyB must
// therefore never see round(keyA) > round(keyB): that ordering is only
// possible if the batch became visible between its two entries.
// Run with -race.
func TestConcurrentStore_UpdateBatchAtomicity(t *testing.T) {
	cs := NewConcurrentStore()
	ctx := context.Background()
	keyA, keyB := twoBucketKeys(t)

	encodeRound := func(n uint64) []byte {
		v := make([]byte, 8)
		binary.LittleEndian.PutUint64(v, n)
		return v
	}

	// Seed round 0 so the reader never hits KeyNotFound.
	seed := []sm.Entry{
		{Index: 1, Cmd: mustEncodePut(keyA, encodeRound(0))},
		{Index: 2, Cmd: mustEncodePut(keyB, encodeRound(0))},
	}
	if err := cs.Update(ctx, seed, make([]sm.Result, len(seed))); err != nil {
		t.Fatalf("seed Update = %v", err)
	}

	const rounds = 2000
	done := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-done:
				return
			default:
			}
			va, err := cs.BytesLookup(keyA)
			if err != nil {
				t.Errorf("BytesLookup(keyA) = %v", err)
				return
			}
			vb, err := cs.BytesLookup(keyB)
			if err != nil {
				t.Errorf("BytesLookup(keyB) = %v", err)
				return
			}
			ra := binary.LittleEndian.Uint64(va)
			rb := binary.LittleEndian.Uint64(vb)
			// Rounds only increase; keyB is written after keyA within
			// an atomic batch, so an A-then-B read must see rb >= ra.
			if ra > rb {
				t.Errorf("observed partial batch: round(keyA)=%d > round(keyB)=%d", ra, rb)
				return
			}
		}
	}()

	for n := uint64(1); n <= rounds; n++ {
		entries := []sm.Entry{
			{Index: 2*n + 1, Cmd: mustEncodePut(keyA, encodeRound(n))},
			{Index: 2*n + 2, Cmd: mustEncodePut(keyB, encodeRound(n))},
		}
		if err := cs.Update(ctx, entries, make([]sm.Result, len(entries))); err != nil {
			close(done)
			t.Fatalf("Update round %d = %v", n, err)
		}
	}
	close(done)
	wg.Wait()
}

// TestConcurrentStore_RecoverFromSnapshotTruncatedPreservesState is the
// regression test: RecoverFromSnapshot on a truncated stream must
// return an error AND leave the pre-recovery contents fully intact, instead
// of destroying every bucket before discovering the stream is bad.
func TestConcurrentStore_RecoverFromSnapshotTruncatedPreservesState(t *testing.T) {
	ctx := context.Background()

	// Source store: produce a valid snapshot stream to truncate.
	src := NewConcurrentStore()
	srcEntries := []sm.Entry{
		{Index: 1, Cmd: mustEncodePut([]byte("snap-k1"), []byte("snap-v1"))},
		{Index: 2, Cmd: mustEncodePut([]byte("snap-k2"), []byte("snap-v2"))},
	}
	if err := src.Update(ctx, srcEntries, make([]sm.Result, len(srcEntries))); err != nil {
		t.Fatalf("src Update = %v", err)
	}
	snapCtx, err := src.PrepareSnapshot()
	if err != nil {
		t.Fatalf("PrepareSnapshot = %v", err)
	}
	var snap bytes.Buffer
	if err := src.SaveSnapshot(ctx, snapCtx, &snap, nil); err != nil {
		t.Fatalf("SaveSnapshot = %v", err)
	}

	// Target store with pre-existing state that must survive.
	cs := NewConcurrentStore()
	liveEntries := []sm.Entry{
		{Index: 1, Cmd: mustEncodePut([]byte("live-k1"), []byte("live-v1"))},
		{Index: 2, Cmd: mustEncodePut([]byte("live-k2"), []byte("live-v2"))},
	}
	if err := cs.Update(ctx, liveEntries, make([]sm.Result, len(liveEntries))); err != nil {
		t.Fatalf("target Update = %v", err)
	}
	hashBefore, err := cs.GetHash()
	if err != nil {
		t.Fatalf("GetHash = %v", err)
	}

	// Truncate the stream mid-record and attempt recovery.
	truncated := snap.Bytes()[:snap.Len()-3]
	if err := cs.RecoverFromSnapshot(ctx, bytes.NewReader(truncated), nil); err == nil {
		t.Fatal("RecoverFromSnapshot on truncated stream = nil, want error")
	}

	hashAfter, err := cs.GetHash()
	if err != nil {
		t.Fatalf("GetHash after failed recovery = %v", err)
	}
	if hashAfter != hashBefore {
		t.Errorf("state changed by failed recovery: hash %d -> %d", hashBefore, hashAfter)
	}
	for key, want := range map[string]string{"live-k1": "live-v1", "live-k2": "live-v2"} {
		got, err := cs.BytesLookup([]byte(key))
		if err != nil {
			t.Errorf("pre-recovery key %q lost after failed recovery: %v", key, err)
			continue
		}
		if string(got) != want {
			t.Errorf("key %q = %q, want %q", key, got, want)
		}
	}
	for _, key := range []string{"snap-k1", "snap-k2"} {
		if _, err := cs.BytesLookup([]byte(key)); err == nil {
			t.Errorf("snapshot key %q leaked into store from failed recovery", key)
		}
	}

	// The intact stream must still recover cleanly afterwards.
	if err := cs.RecoverFromSnapshot(ctx, bytes.NewReader(snap.Bytes()), nil); err != nil {
		t.Fatalf("RecoverFromSnapshot on intact stream = %v", err)
	}
	if _, err := cs.BytesLookup([]byte("snap-k1")); err != nil {
		t.Errorf("snap-k1 missing after successful recovery: %v", err)
	}
	if _, err := cs.BytesLookup([]byte("live-k1")); err == nil {
		t.Error("live-k1 should be replaced by successful recovery")
	}
}
