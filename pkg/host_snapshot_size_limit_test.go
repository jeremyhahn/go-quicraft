// Copyright 2026 Jeremy Hahn
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package quicraft

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jeremyhahn/go-quicraft/pkg/config"
	"github.com/jeremyhahn/go-quicraft/pkg/internal/engine"
	"github.com/jeremyhahn/go-quicraft/pkg/sm"
)

// sizeLimitDigestQuery asks a growingStateMachine for the digest of every
// command it has applied, in apply order.
const sizeLimitDigestQuery = "digest"

// sizeLimitOverhead is the compaction overhead both snapshot requests in
// the size-limit test use: small enough that each snapshot is eligible to
// compact the log.
const sizeLimitOverhead = 10

// growingStateMachine keeps every applied command, so its snapshot grows
// with the data proposed and its digest is a function of the exact applied
// sequence: a lost, duplicated or reordered entry changes it.
type growingStateMachine struct {
	mu   sync.Mutex
	cmds [][]byte
}

func growingCreateFunc(uint64, uint64) interface{} { return &growingStateMachine{} }

func (g *growingStateMachine) Update(_ context.Context, entries []sm.Entry, results []sm.Result) error {
	g.mu.Lock()
	defer g.mu.Unlock()
	for i, e := range entries {
		g.cmds = append(g.cmds, append([]byte(nil), e.Cmd...))
		results[i] = sm.Result{Value: e.Index}
	}
	return nil
}

func (g *growingStateMachine) Lookup(_ context.Context, query interface{}) (interface{}, error) {
	if query != sizeLimitDigestQuery {
		return nil, fmt.Errorf("unknown query %v", query)
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	return digestCommands(g.cmds), nil
}

func (g *growingStateMachine) SaveSnapshot(_ context.Context, w io.Writer, _ <-chan struct{}) error {
	g.mu.Lock()
	defer g.mu.Unlock()
	var hdr [4]byte
	for _, c := range g.cmds {
		binary.BigEndian.PutUint32(hdr[:], uint32(len(c)))
		if _, err := w.Write(hdr[:]); err != nil {
			return err
		}
		if _, err := w.Write(c); err != nil {
			return err
		}
	}
	return nil
}

func (g *growingStateMachine) RecoverFromSnapshot(_ context.Context, r io.Reader, _ <-chan struct{}) error {
	var cmds [][]byte
	var hdr [4]byte
	for {
		if _, err := io.ReadFull(r, hdr[:]); err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return err
		}
		c := make([]byte, binary.BigEndian.Uint32(hdr[:]))
		if _, err := io.ReadFull(r, c); err != nil {
			return err
		}
		cmds = append(cmds, c)
	}
	g.mu.Lock()
	g.cmds = cmds
	g.mu.Unlock()
	return nil
}

func (g *growingStateMachine) Close(context.Context) error { return nil }

func digestCommands(cmds [][]byte) [32]byte {
	h := sha256.New()
	var hdr [4]byte
	for _, c := range cmds {
		binary.BigEndian.PutUint32(hdr[:], uint32(len(c)))
		h.Write(hdr[:])
		h.Write(c)
	}
	var out [32]byte
	copy(out[:], h.Sum(nil))
	return out
}

// sizeLimitRecorder records the events this test asserts on, across every
// Host incarnation that shares it.
type sizeLimitRecorder struct {
	mu      sync.Mutex
	created []uint64
	failed  atomic.Int32
}

func (r *sizeLimitRecorder) listener() *config.EventListener {
	return &config.EventListener{
		OnShardFailed: func(config.ShardFailedInfo) { r.failed.Add(1) },
		OnSnapshotCreated: func(info config.SnapshotInfo) {
			r.mu.Lock()
			r.created = append(r.created, info.Index)
			r.mu.Unlock()
		},
	}
}

func (r *sizeLimitRecorder) createdIndices() []uint64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]uint64(nil), r.created...)
}

// TestSnapshotSizeLimit_OversizedSnapshotFailsAndShardKeepsServing pins the
// documented MaxSnapshotSize contract (docs/snapshots/user.md "Size limit"):
// a snapshot whose output exceeds the limit is discarded with
// *SnapshotTooLargeError, the log and the previous snapshot stay in place,
// the shard is not failed and keeps serving, and a later snapshot under the
// default limit succeeds.
func TestSnapshotSizeLimit_OversizedSnapshotFailsAndShardKeepsServing(t *testing.T) {
	const (
		shardID   = uint64(1)
		replicaID = uint64(1)
		limit     = uint64(64 * 1024)
	)
	members := map[uint64]string{replicaID: "127.0.0.1:5000"}
	rec := &sizeLimitRecorder{}
	hcfg := testHostConfig(t)
	hcfg.EventListener = rec.listener()

	var acked [][]byte
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	shardCfg := func(maxSnapshotSize uint64) config.Config {
		c := config.Config{ShardID: shardID, ReplicaID: replicaID, MaxSnapshotSize: maxSnapshotSize}
		c.SetDefaults()
		c.SnapshotEntries = 0
		return c
	}
	start := func(maxSnapshotSize uint64) *Host {
		t.Helper()
		h, err := NewHost(hcfg, WithoutTransport(), WithoutEncryption())
		if err != nil {
			t.Fatalf("NewHost: %v", err)
		}
		if err := h.StartShard(members, false, growingCreateFunc, shardCfg(maxSnapshotSize)); err != nil {
			h.Close()
			t.Fatalf("StartShard: %v", err)
		}
		waitForLeader(t, h, shardID)
		return h
	}
	propose := func(h *Host, n, size int) {
		t.Helper()
		for i := 0; i < n; i++ {
			cmd := make([]byte, size)
			if _, err := rand.Read(cmd); err != nil {
				t.Fatalf("rand.Read: %v", err)
			}
			if _, err := h.SyncPropose(ctx, shardID, cmd); err != nil {
				t.Fatalf("SyncPropose %d: %v", i, err)
			}
			acked = append(acked, cmd)
		}
	}
	checkDigest := func(h *Host, phase string) {
		t.Helper()
		got, err := h.SyncRead(ctx, shardID, sizeLimitDigestQuery)
		if err != nil {
			t.Fatalf("%s: SyncRead digest: %v", phase, err)
		}
		if got.([32]byte) != digestCommands(acked) {
			t.Fatalf("%s: the state machine digest does not match a replay of the %d "+
				"acknowledged proposals", phase, len(acked))
		}
	}

	// Both snapshot requests use a small compaction overhead, so each is
	// eligible to compact the log: S1 establishes a compaction point, and
	// a refused snapshot that wrongly reached compaction would move it.
	snapOpt := SnapshotOption{OverrideCompactionOverhead: true, CompactionOverhead: sizeLimitOverhead}

	// (a) Under a large limit a snapshot succeeds and compacts the log: S1.
	h := start(0)
	propose(h, 3*sizeLimitOverhead, 1024)
	s1, err := h.SyncRequestSnapshot(ctx, shardID, snapOpt)
	if err != nil {
		h.Close()
		t.Fatalf("(a) SyncRequestSnapshot under the default limit: %v", err)
	}
	if s1 <= sizeLimitOverhead {
		h.Close()
		t.Fatalf("(a) SyncRequestSnapshot index = %d, want > the overhead %d", s1, sizeLimitOverhead)
	}
	compacted, err := h.LogRange(shardID)
	if err != nil {
		h.Close()
		t.Fatalf("(a) LogRange: %v", err)
	}
	if compacted.FirstIndex != s1-sizeLimitOverhead+1 {
		h.Close()
		t.Fatalf("(a) LogRange.FirstIndex = %d after S1 = %d with overhead %d: S1 did not "+
			"compact the log", compacted.FirstIndex, s1, sizeLimitOverhead)
	}
	h.Close()

	// (b) Restart below the next snapshot's size; the snapshot is refused.
	h = start(limit)
	before, err := h.LogRange(shardID)
	if err != nil {
		h.Close()
		t.Fatalf("(b) LogRange: %v", err)
	}
	// After a restart the log starts no later than just past S1.
	if before.FirstIndex > s1+1 {
		h.Close()
		t.Fatalf("(b) LogRange.FirstIndex after restart = %d, want <= S1+1 = %d",
			before.FirstIndex, s1+1)
	}
	propose(h, 200, 1024)
	_, snapErr := h.SyncRequestSnapshot(ctx, shardID, snapOpt)
	var tooLarge *SnapshotTooLargeError
	if !errors.As(snapErr, &tooLarge) {
		h.Close()
		t.Fatalf("(b) SyncRequestSnapshot over the limit = %v, want *SnapshotTooLargeError", snapErr)
	}
	if tooLarge.ShardID != shardID || tooLarge.MaxSize != limit {
		h.Close()
		t.Fatalf("(b) SnapshotTooLargeError = {ShardID: %d, MaxSize: %d}, want {%d, %d}",
			tooLarge.ShardID, tooLarge.MaxSize, shardID, limit)
	}
	if !errors.Is(snapErr, ErrSnapshotTooLarge) {
		h.Close()
		t.Fatalf("(b) errors.Is(%v, ErrSnapshotTooLarge) = false", snapErr)
	}

	// (c) The log was not compacted past S1's compaction point and no
	// artifact newer than S1 remains.
	after, err := h.LogRange(shardID)
	if err != nil {
		h.Close()
		t.Fatalf("(c) LogRange: %v", err)
	}
	// With the small overhead, a refused snapshot that reached compaction
	// would move FirstIndex to about S1+190; it must stay where it was.
	if after.FirstIndex != before.FirstIndex {
		h.Close()
		t.Fatalf("(c) LogRange.FirstIndex moved from %d to %d (S1 = %d, overhead %d): "+
			"the refused snapshot compacted the log",
			before.FirstIndex, after.FirstIndex, s1, sizeLimitOverhead)
	}
	base := filepath.Dir(engine.SnapshotSaveDir(
		shardSnapshotBaseDir(hcfg.NodeHostDir, "", shardID), shardID, replicaID, s1))
	entries, err := os.ReadDir(base)
	if err != nil {
		h.Close()
		t.Fatalf("(c) reading the snapshot directory %q: %v", base, err)
	}
	s1Name := filepath.Base(engine.SnapshotSaveDir(
		shardSnapshotBaseDir(hcfg.NodeHostDir, "", shardID), shardID, replicaID, s1))
	sawS1 := false
	for _, e := range entries {
		if e.Name() == s1Name {
			sawS1 = true
			continue
		}
		// The engine stages a snapshot in a save directory and refuses an
		// oversized one before publishing it; a staging directory left here
		// is the leaked artifact. This runs before any Close or restart, so
		// a startup sweep cannot hide it.
		if engine.IsSnapshotSaveStage(e.Name()) {
			h.Close()
			t.Fatalf("(c) save staging directory %q remains under %q after the refused "+
				"snapshot: the oversized artifact was not discarded", e.Name(), base)
		}
		if !strings.HasPrefix(e.Name(), "snapshot-") {
			continue
		}
		idx, parseErr := strconv.ParseUint(strings.TrimPrefix(e.Name(), "snapshot-"), 10, 64)
		if parseErr != nil || idx > s1 {
			h.Close()
			t.Fatalf("(c) %q remains under %q after the refused snapshot: the "+
				"oversized artifact was not discarded", e.Name(), base)
		}
	}
	if !sawS1 {
		h.Close()
		t.Fatalf("(c) the previous snapshot %q is gone from %q", s1Name, base)
	}

	// (d) The shard keeps serving and its state is exactly the acknowledged
	// proposals, in order.
	propose(h, 1, 64)
	checkDigest(h, "(d)")
	h.Close()

	if n := rec.failed.Load(); n != 0 {
		t.Fatalf("(c) OnShardFailed fired %d times for a refused oversized snapshot, want 0", n)
	}
	if got := rec.createdIndices(); len(got) != 1 || got[0] != s1 {
		t.Fatalf("(c) OnSnapshotCreated indices = %v, want only S1 = %d", got, s1)
	}

	// (e) Under the default limit the next snapshot succeeds past S1.
	h = start(0)
	defer h.Close()
	// A proposal first: it commits the new term's entry, so the linearizable
	// read below does not race the leader's first commit after the restart.
	propose(h, 1, 64)
	checkDigest(h, "(e) after restart")
	s2, err := h.SyncRequestSnapshot(ctx, shardID, SnapshotOption{})
	if err != nil {
		t.Fatalf("(e) SyncRequestSnapshot under the default limit: %v", err)
	}
	if s2 <= s1 {
		t.Fatalf("(e) SyncRequestSnapshot index = %d, want > S1 = %d", s2, s1)
	}
}
