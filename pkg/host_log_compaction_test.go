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
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"testing"
	"time"

	"github.com/jeremyhahn/go-quicraft/pkg/config"
	"github.com/jeremyhahn/go-quicraft/pkg/logdb"
	"github.com/jeremyhahn/go-quicraft/pkg/sm"
)

// logCompactionOverhead is the per-snapshot CompactionOverhead the tests
// request: the log may then drop every entry below snapshotIndex-overhead+1.
const logCompactionOverhead uint64 = 10

// proposeLogCompactionEntries proposes n distinct commands on shard 1.
func proposeLogCompactionEntries(t *testing.T, h *Host, n int, prefix string) {
	t.Helper()
	for i := 0; i < n; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		_, err := h.SyncPropose(ctx, 1, []byte(fmt.Sprintf("%s-%d", prefix, i)))
		cancel()
		if err != nil {
			t.Fatalf("SyncPropose %s-%d: %v", prefix, i, err)
		}
	}
}

// snapshotWithLogCompactionOverhead takes a snapshot of shard 1 with the
// test's CompactionOverhead and returns its index.
func snapshotWithLogCompactionOverhead(t *testing.T, h *Host) uint64 {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	idx, err := h.SyncRequestSnapshot(ctx, 1, SnapshotOption{
		OverrideCompactionOverhead: true,
		CompactionOverhead:         logCompactionOverhead,
	})
	if err != nil {
		t.Fatalf("SyncRequestSnapshot: %v", err)
	}
	if idx <= logCompactionOverhead {
		t.Fatalf("snapshot index = %d, want above the overhead %d", idx, logCompactionOverhead)
	}
	return idx
}

// waitForLogCompaction waits until LogRange's FirstIndex moves above 1 and
// returns the range. It fails if FirstIndex passes snapIdx-overhead+1, which
// would drop entries the overhead promises to keep.
func waitForLogCompaction(t *testing.T, h *Host, snapIdx uint64) LogRange {
	t.Helper()
	limit := snapIdx - logCompactionOverhead + 1
	deadline := time.Now().Add(10 * time.Second)
	var lr LogRange
	for time.Now().Before(deadline) {
		var err error
		lr, err = h.LogRange(1)
		if err != nil {
			t.Fatalf("LogRange: %v", err)
		}
		if lr.FirstIndex > limit {
			t.Fatalf("LogRange FirstIndex = %d after a snapshot at %d with overhead %d, "+
				"want at most %d: the log dropped entries the overhead keeps",
				lr.FirstIndex, snapIdx, logCompactionOverhead, limit)
		}
		if lr.FirstIndex > 1 {
			return lr
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("LogRange FirstIndex still %d 10s after a snapshot at %d with overhead %d; "+
		"the snapshot did not compact the log", lr.FirstIndex, snapIdx, logCompactionOverhead)
	return lr
}

func mustStateMachineHash(t *testing.T, h *Host) (uint64, uint64) {
	t.Helper()
	hash, applied, ok, err := h.StateMachineHash(1)
	if err != nil || !ok {
		t.Fatalf("StateMachineHash = (ok=%v, err=%v), want a hash", ok, err)
	}
	return hash, applied
}

func TestLogCompaction_SnapshotMovesRetainedRange(t *testing.T) {
	h := newTestHost(t)
	if err := h.StartShard(map[uint64]string{1: "127.0.0.1:5000"}, false,
		hashingCreateFunc, testShardConfig(1, 1)); err != nil {
		t.Fatalf("StartShard: %v", err)
	}
	waitForLeader(t, h, 1)

	// (a) 200 entries, a snapshot with overhead 10, the retained range moves.
	proposeLogCompactionEntries(t, h, 200, "e")
	hashBefore, appliedBefore := mustStateMachineHash(t, h)
	snapIdx := snapshotWithLogCompactionOverhead(t, h)
	lr := waitForLogCompaction(t, h, snapIdx)
	if lr.LastIndex < snapIdx {
		t.Fatalf("LogRange LastIndex = %d, want at least the snapshot index %d", lr.LastIndex, snapIdx)
	}

	// (b) LogReader reports the same range.
	reader, err := h.LogReader(1)
	if err != nil {
		t.Fatalf("LogReader: %v", err)
	}
	if first, _ := reader.GetRange(); first != lr.FirstIndex {
		t.Fatalf("LogReader GetRange first = %d, LogRange FirstIndex = %d; want equal",
			first, lr.FirstIndex)
	}

	// (c) QueryRaftLog: compacted prefix, reversed range, retained range.
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if _, err := h.QueryRaftLog(ctx, 1, 1, lr.FirstIndex, 1<<20); !errors.Is(err, logdb.ErrCompacted) {
		t.Fatalf("QueryRaftLog [1, %d) = %v, want logdb.ErrCompacted", lr.FirstIndex, err)
	}
	if _, err := h.QueryRaftLog(ctx, 1, lr.FirstIndex+1, lr.FirstIndex, 1<<20); !errors.Is(err, ErrInvalidRange) {
		t.Fatalf("QueryRaftLog reversed range = %v, want ErrInvalidRange", err)
	}
	ents, err := h.QueryRaftLog(ctx, 1, lr.FirstIndex, lr.LastIndex+1, 1<<20)
	if err != nil {
		t.Fatalf("QueryRaftLog [%d, %d) = %v, want entries", lr.FirstIndex, lr.LastIndex+1, err)
	}
	if len(ents) == 0 || ents[0].Index != lr.FirstIndex {
		t.Fatalf("QueryRaftLog [%d, %d) returned %d entries (first %v), want entries from %d",
			lr.FirstIndex, lr.LastIndex+1, len(ents), ents, lr.FirstIndex)
	}

	// (d) RequestCompaction on this host's replica and on one it does not hold.
	if err := h.RequestCompaction(1, 1); err != nil {
		t.Fatalf("RequestCompaction(1, 1) = %v, want nil", err)
	}
	var rnf *ReplicaNotFoundError
	if err := h.RequestCompaction(1, 99); !errors.As(err, &rnf) {
		t.Fatalf("RequestCompaction(1, 99) = %v, want *ReplicaNotFoundError", err)
	}

	// (e) Compaction did not touch the state machine.
	hashAfter, appliedAfter := mustStateMachineHash(t, h)
	if hashAfter != hashBefore || appliedAfter != appliedBefore {
		t.Fatalf("StateMachineHash after compaction = (%d, applied %d), before = (%d, applied %d); "+
			"compaction changed the state machine", hashAfter, appliedAfter, hashBefore, appliedBefore)
	}
}

// snapshottingHasher is a hashing state machine whose digest survives a
// snapshot: SaveSnapshot writes it and RecoverFromSnapshot restores it. It
// hashes only entries that carry a command, so the no-op a new leader
// appends after a restart does not change the digest.
type snapshottingHasher struct {
	mockStateMachine
	sum uint64
}

func (s *snapshottingHasher) Update(ctx context.Context, entries []sm.Entry, results []sm.Result) error {
	for i := range entries {
		if len(entries[i].Cmd) == 0 {
			continue
		}
		s.sum += entries[i].Index * 1000003
		for _, b := range entries[i].Cmd {
			s.sum += uint64(b)
		}
	}
	return s.mockStateMachine.Update(ctx, entries, results)
}

func (s *snapshottingHasher) GetHash() (uint64, error) { return s.sum, nil }

func (s *snapshottingHasher) SaveSnapshot(_ context.Context, w io.Writer, _ <-chan struct{}) error {
	var buf [8]byte
	binary.LittleEndian.PutUint64(buf[:], s.sum)
	_, err := w.Write(buf[:])
	return err
}

func (s *snapshottingHasher) RecoverFromSnapshot(_ context.Context, r io.Reader, _ <-chan struct{}) error {
	var buf [8]byte
	if _, err := io.ReadFull(r, buf[:]); err != nil {
		return err
	}
	s.sum = binary.LittleEndian.Uint64(buf[:])
	return nil
}

var _ sm.Hasher = (*snapshottingHasher)(nil)

func snapshottingHasherCreateFunc(shardID, replicaID uint64) interface{} {
	return &snapshottingHasher{}
}

// newLogCompactionWALHost opens a transportless Host on cfg's real WAL.
func newLogCompactionWALHost(t *testing.T, cfg config.HostConfig) *Host {
	t.Helper()
	h, err := NewHost(cfg, WithoutTransport(), WithoutEncryption())
	if err != nil {
		t.Fatalf("NewHost on a real WAL: %v", err)
	}
	if err := h.StartShard(map[uint64]string{1: "127.0.0.1:5000"}, false,
		snapshottingHasherCreateFunc, testShardConfig(1, 1)); err != nil {
		h.Close()
		t.Fatalf("StartShard: %v", err)
	}
	return h
}

func TestLogCompaction_RestartOnWALKeepsState(t *testing.T) {
	cfg := testHostConfig(t)
	h := newLogCompactionWALHost(t, cfg)
	waitForLeader(t, h, 1)

	proposeLogCompactionEntries(t, h, 200, "w")
	snapIdx := snapshotWithLogCompactionOverhead(t, h)
	waitForLogCompaction(t, h, snapIdx)
	hashBefore, appliedBefore := mustStateMachineHash(t, h)
	h.Close()

	// (f) Restart on the same WAL: the snapshot plus the retained tail
	// rebuild the same state, and the shard accepts new proposals.
	h = newLogCompactionWALHost(t, cfg)
	defer h.Close()
	waitForLeader(t, h, 1)

	deadline := time.Now().Add(10 * time.Second)
	hashAfter, appliedAfter := mustStateMachineHash(t, h)
	for appliedAfter < appliedBefore && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
		hashAfter, appliedAfter = mustStateMachineHash(t, h)
	}
	if appliedAfter < appliedBefore {
		t.Fatalf("applied index after restart = %d, want at least %d", appliedAfter, appliedBefore)
	}
	if hashAfter != hashBefore {
		t.Fatalf("StateMachineHash after restart = %d (applied %d), before = %d (applied %d); "+
			"the compacted log did not rebuild the same state", hashAfter, appliedAfter, hashBefore, appliedBefore)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if _, err := h.SyncPropose(ctx, 1, []byte("after-restart")); err != nil {
		t.Fatalf("SyncPropose after restart = %v", err)
	}
	if _, applied := mustStateMachineHash(t, h); applied <= appliedAfter {
		t.Fatalf("applied index after a new proposal = %d, want above %d", applied, appliedAfter)
	}
}
