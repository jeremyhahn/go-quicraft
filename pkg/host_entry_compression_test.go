// Copyright 2026 Jeremy Hahn
// SPDX-License-Identifier: Apache-2.0

package quicraft

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/binary"
	"hash/fnv"
	"sync"
	"testing"
	"time"

	"github.com/jeremyhahn/go-quicraft/pkg/config"
	"github.com/jeremyhahn/go-quicraft/pkg/proto"
	"github.com/jeremyhahn/go-quicraft/pkg/sm"
)

// appliedCmd is one entry as the state machine saw it.
type appliedCmd struct {
	index uint64
	cmd   []byte
}

// compressionRecordingSM records every applied Cmd by index, in apply order,
// and folds each (index, Cmd) pair into an order-independent hash.
type compressionRecordingSM struct {
	mockStateMachine
	mu      sync.Mutex
	applied []appliedCmd
	sum     uint64
}

func (s *compressionRecordingSM) Update(_ context.Context, entries []sm.Entry, results []sm.Result) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	for i := range entries {
		cmd := append([]byte(nil), entries[i].Cmd...)
		s.applied = append(s.applied, appliedCmd{index: entries[i].Index, cmd: cmd})
		h := fnv.New64a()
		var idx [8]byte
		binary.BigEndian.PutUint64(idx[:], entries[i].Index)
		_, _ = h.Write(idx[:])
		_, _ = h.Write(cmd)
		s.sum += h.Sum64()
		results[i] = sm.Result{Value: entries[i].Index}
	}
	return nil
}

func (s *compressionRecordingSM) GetHash() (uint64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.sum, nil
}

func (s *compressionRecordingSM) snapshot() []appliedCmd {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]appliedCmd(nil), s.applied...)
}

func (s *compressionRecordingSM) cmdAt(index uint64) ([]byte, int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var got []byte
	n := 0
	for _, a := range s.applied {
		if a.index == index {
			got = a.cmd
			n++
		}
	}
	return got, n
}

// compressionHost starts shard 1 on a transportless Host over cfg's real WAL
// with the given entry compression and returns the Host and its state machine.
func compressionHost(t *testing.T, cfg config.HostConfig, ct config.CompressionType) (*Host, *compressionRecordingSM) {
	t.Helper()
	h, err := NewHost(cfg, WithoutTransport(), WithoutEncryption())
	if err != nil {
		t.Fatalf("NewHost: %v", err)
	}
	rec := &compressionRecordingSM{}
	create := func(_, _ uint64) interface{} { return rec }
	scfg := testShardConfig(1, 1)
	scfg.EntryCompressionType = ct
	if err := h.StartShard(map[uint64]string{1: "127.0.0.1:5000"}, false, create, scfg); err != nil {
		mustCloseHost(t, h)
		t.Fatalf("StartShard: %v", err)
	}
	waitForLeader(t, h, 1)
	return h, rec
}

// waitReplayed waits until rec has applied at least n entries.
func waitReplayed(t *testing.T, rec *compressionRecordingSM, n int) []appliedCmd {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for {
		got := rec.snapshot()
		if len(got) >= n {
			return got
		}
		if time.Now().After(deadline) {
			t.Fatalf("replay applied %d entries, want at least %d", len(got), n)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func assertSameApplied(t *testing.T, label string, want, got []appliedCmd) {
	t.Helper()
	if len(got) < len(want) {
		t.Fatalf("%s: applied %d entries, want at least %d", label, len(got), len(want))
	}
	for i := range want {
		if got[i].index != want[i].index || !bytes.Equal(got[i].cmd, want[i].cmd) {
			t.Fatalf("%s: apply #%d = index %d (%d bytes), want index %d (%d bytes)",
				label, i, got[i].index, len(got[i].cmd), want[i].index, len(want[i].cmd))
		}
	}
}

func TestHost_EntryCompression_SnappyEndToEnd(t *testing.T) {
	cfg := testHostConfig(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	compressible := bytes.Repeat([]byte{'z'}, 64*1024)
	random := make([]byte, 4*1024)
	if _, err := rand.Read(random); err != nil {
		t.Fatalf("rand.Read: %v", err)
	}
	tiny := []byte{0x7f}
	sessionCmd := bytes.Repeat([]byte("session-payload-"), 256)

	h, rec := compressionHost(t, cfg, config.Snappy)

	// (a) every payload reaches the state machine byte for byte.
	payloads := [][]byte{compressible, random, tiny}
	indexes := make([]uint64, len(payloads))
	for i, p := range payloads {
		res, err := h.SyncPropose(ctx, 1, p)
		if err != nil {
			mustCloseHost(t, h)
			t.Fatalf("SyncPropose %d: %v", i, err)
		}
		indexes[i] = res.Value
		got, n := rec.cmdAt(res.Value)
		if n != 1 || !bytes.Equal(got, p) {
			mustCloseHost(t, h)
			t.Fatalf("payload %d at index %d: applied %d times, %d bytes, want once with the %d proposed bytes",
				i, res.Value, n, len(got), len(p))
		}
	}

	// (b) the stored entries: the compressible one carries the flag and a
	// shorter Cmd; the random and 1-byte ones are stored as proposed.
	for i, p := range payloads {
		ents, err := h.QueryRaftLog(ctx, 1, indexes[i], indexes[i]+1, 1<<24)
		if err != nil || len(ents) != 1 {
			mustCloseHost(t, h)
			t.Fatalf("QueryRaftLog(%d): %d entries, err %v", indexes[i], len(ents), err)
		}
		e := ents[0]
		if e.Type&proto.EntryTypeMask != proto.EntryNormal {
			t.Errorf("entry %d: type %#x, want EntryNormal", e.Index, e.Type)
		}
		flagged := e.Type&proto.EntryCompressFlag != 0
		if i == 0 {
			if !flagged || len(e.Cmd) >= len(p) {
				t.Errorf("compressible entry %d: flag %v, stored %d bytes, want the flag and fewer than %d bytes",
					e.Index, flagged, len(e.Cmd), len(p))
			}
			continue
		}
		if flagged || !bytes.Equal(e.Cmd, p) {
			t.Errorf("incompressible entry %d: flag %v, stored %d bytes, want no flag and the %d proposed bytes",
				e.Index, flagged, len(e.Cmd), len(p))
		}
	}

	// (c) a session proposal under compression applies once and returns
	// its result.
	session, err := h.NewSession(ctx, 1)
	if err != nil {
		mustCloseHost(t, h)
		t.Fatalf("NewSession: %v", err)
	}
	res, err := h.SyncProposeWithSession(ctx, session, sessionCmd)
	if err != nil {
		mustCloseHost(t, h)
		t.Fatalf("SyncProposeWithSession: %v", err)
	}
	if got, n := rec.cmdAt(res.Value); n != 1 || !bytes.Equal(got, sessionCmd) {
		t.Errorf("session proposal at index %d: applied %d times, %d bytes, want once with %d bytes",
			res.Value, n, len(got), len(sessionCmd))
	}
	ents, err := h.QueryRaftLog(ctx, 1, res.Value, res.Value+1, 1<<24)
	if err != nil || len(ents) != 1 || ents[0].Type&proto.EntryCompressFlag == 0 {
		t.Errorf("session entry %d: %d entries, err %v; want one compressed entry", res.Value, len(ents), err)
	}
	if t.Failed() {
		mustCloseHost(t, h)
		t.FailNow()
	}

	before := rec.snapshot()
	hashBefore, _, ok, err := h.StateMachineHash(1)
	if err != nil || !ok {
		mustCloseHost(t, h)
		t.Fatalf("StateMachineHash before restart: ok %v, err %v", ok, err)
	}
	mustCloseHost(t, h)

	// (d) a restart with the same config replays the compressed log: the
	// same Cmds in the same order, and the same state machine hash.
	h, rec = compressionHost(t, cfg, config.Snappy)
	assertSameApplied(t, "restart with Snappy", before, waitReplayed(t, rec, len(before)))
	hashAfter, _, _, err := h.StateMachineHash(1)
	if err != nil {
		mustCloseHost(t, h)
		t.Fatalf("StateMachineHash after restart: %v", err)
	}
	if hashAfter != hashBefore {
		mustCloseHost(t, h)
		t.Fatalf("StateMachineHash after restart = %d, want %d from before the restart", hashAfter, hashBefore)
	}
	mustCloseHost(t, h)

	// (e) a restart with NoCompression still applies the compressed entries
	// already in the log unchanged, and stores a new entry uncompressed.
	h, rec = compressionHost(t, cfg, config.NoCompression)
	defer mustCloseHost(t, h)
	assertSameApplied(t, "restart with NoCompression", before, waitReplayed(t, rec, len(before)))
	res, err = h.SyncPropose(ctx, 1, compressible)
	if err != nil {
		t.Fatalf("SyncPropose under NoCompression: %v", err)
	}
	if got, n := rec.cmdAt(res.Value); n != 1 || !bytes.Equal(got, compressible) {
		t.Fatalf("entry %d under NoCompression: applied %d times, %d bytes", res.Value, n, len(got))
	}
	ents, err = h.QueryRaftLog(ctx, 1, res.Value, res.Value+1, 1<<24)
	if err != nil || len(ents) != 1 {
		t.Fatalf("QueryRaftLog(%d): %d entries, err %v", res.Value, len(ents), err)
	}
	if ents[0].Type&proto.EntryCompressFlag != 0 || !bytes.Equal(ents[0].Cmd, compressible) {
		t.Fatalf("entry %d under NoCompression: flag set or Cmd changed (%d bytes)", res.Value, len(ents[0].Cmd))
	}
}
