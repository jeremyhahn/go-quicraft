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

//go:build integration

package durable_test

import (
	"bytes"
	"context"
	"errors"
	"io"
	"io/fs"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	quicraft "github.com/jeremyhahn/go-quicraft/pkg"
	"github.com/jeremyhahn/go-quicraft/pkg/config"
	"github.com/jeremyhahn/go-quicraft/pkg/logdb"
	"github.com/jeremyhahn/go-quicraft/pkg/logdb/waldb"
	"github.com/jeremyhahn/go-quicraft/pkg/sm"
	"github.com/jeremyhahn/go-quicraft/pkg/writemode"
	"github.com/jeremyhahn/go-quicraft/test/testpki"
)

const (
	shardID    = uint64(1)
	numNodes   = 3
	kernelAddr = "127.0.0.1:0"

	// holdWindow is how long the test watches a proposal whose quorum
	// cannot have fsynced it. It is shorter than the default election
	// timeout so the leader keeps its term while two followers are held.
	holdWindow    = 750 * time.Millisecond
	leaderTimeout = 30 * time.Second
	ackTimeout    = 20 * time.Second
)

// defaultMode is the write mode a caller gets without choosing one. The test
// acknowledges its write the way this mode does, so setting it to a mode that
// acknowledges before a quorum fsync (writemode.WALDurable) fails the test.
var defaultMode writemode.Mode

// holdFS is a waldb.FS that, while held, blocks the Syncs of the one
// SaveState that carries the held proposal's entry until it is released. It
// models a replica whose disk has not yet made that write durable: the bytes
// are written, the fsync has not returned. Every other Sync (warm-up commit
// index, hard-state-only SaveStates) passes through. The WAL syncs inline
// under its shard lock, so with one shard a Sync blocked while that SaveState
// is in flight is that SaveState's.
type holdFS struct {
	waldb.FS

	mu      sync.Mutex
	held    bool
	payload []byte // command of the held proposal
	release chan struct{}

	inHeld  atomic.Int64  // SaveStates carrying the held entry, in flight
	waiting atomic.Int64  // Syncs of such a SaveState currently blocked
	index   atomic.Uint64 // log index of the held entry, once seen
	saved   atomic.Bool   // the SaveState carrying the held entry returned
}

func newHoldFS() *holdFS { return &holdFS{FS: waldb.DefaultFS()} }

// hold makes the Syncs of a later SaveState that carries an entry whose
// command is payload block until unhold.
func (h *holdFS) hold(payload []byte) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if !h.held {
		h.held = true
		h.payload = payload
		h.release = make(chan struct{})
	}
}

// unhold lets every blocked and later Sync through.
func (h *holdFS) unhold() {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.held {
		h.held = false
		close(h.release)
	}
}

// match reports whether updates carry the held proposal's entry, recording
// its index.
func (h *holdFS) match(updates []logdb.Update) bool {
	h.mu.Lock()
	payload := h.payload
	h.mu.Unlock()
	if payload == nil {
		return false
	}
	for _, u := range updates {
		if u.ShardID != shardID {
			continue
		}
		for _, e := range u.Entries {
			if bytes.Equal(e.Cmd, payload) {
				h.index.Store(e.Index)
				return true
			}
		}
	}
	return false
}

func (h *holdFS) gate() {
	h.mu.Lock()
	held, release := h.held, h.release
	h.mu.Unlock()
	if !held || h.inHeld.Load() == 0 {
		return
	}
	h.waiting.Add(1)
	<-release
	h.waiting.Add(-1)
}

// holdDB is the replica's WAL with the held-entry barrier: a SaveState that
// carries the held proposal's entry has its Syncs gated by fs; every other
// SaveState passes straight through.
type holdDB struct {
	*waldb.DB
	fs *holdFS
}

func (d *holdDB) SaveState(updates []logdb.Update) error {
	if !d.fs.match(updates) {
		return d.DB.SaveState(updates)
	}
	d.fs.inHeld.Add(1)
	err := d.DB.SaveState(updates)
	d.fs.inHeld.Add(-1)
	d.fs.saved.Store(true)
	return err
}

func (h *holdFS) OpenFile(name string, flag int, perm fs.FileMode) (waldb.File, error) {
	f, err := h.FS.OpenFile(name, flag, perm)
	if err != nil {
		return nil, err
	}
	return &holdFile{File: f, fs: h}, nil
}

func (h *holdFS) Open(name string) (waldb.File, error) {
	f, err := h.FS.Open(name)
	if err != nil {
		return nil, err
	}
	return &holdFile{File: f, fs: h}, nil
}

type holdFile struct {
	waldb.File
	fs *holdFS
}

func (f *holdFile) Sync() error {
	f.fs.gate()
	return f.File.Sync()
}

// countSM counts applied entries; the test needs only that a write applies.
type countSM struct{ applied atomic.Uint64 }

func (s *countSM) Update(_ context.Context, entries []sm.Entry, results []sm.Result) error {
	for i := range entries {
		results[i] = sm.Result{Value: s.applied.Add(1)}
	}
	return nil
}

func (s *countSM) Lookup(_ context.Context, _ interface{}) (interface{}, error) {
	return s.applied.Load(), nil
}

func (s *countSM) SaveSnapshot(_ context.Context, w io.Writer, _ <-chan struct{}) error {
	_, err := w.Write([]byte{0})
	return err
}

func (s *countSM) RecoverFromSnapshot(_ context.Context, r io.Reader, _ <-chan struct{}) error {
	_, err := io.Copy(io.Discard, r)
	return err
}

func (s *countSM) Close(_ context.Context) error { return nil }

type node struct {
	host *quicraft.Host
	db   *waldb.DB
	fs   *holdFS
}

// startCluster starts three hosts with the default HostConfig: only the
// fields a host cannot start without are set, and NoSyncWAL is left false.
// Each host's WAL goes through its own holdFS and holdDB.
func startCluster(t *testing.T, mode writemode.Mode) []*node {
	t.Helper()
	ca := testpki.NewCA(t)
	leaves := ca.Leaves(t, numNodes)

	nodes := make([]*node, numNodes)
	for i := range nodes {
		cfg := config.HostConfig{
			WALDir:        t.TempDir(),
			NodeHostDir:   t.TempDir(),
			RaftAddress:   kernelAddr,
			ListenAddress: kernelAddr,
			DeploymentID:  1,
			TransportConfig: config.TransportConfig{
				MTLSConfig: leaves[i],
			},
		}
		// NotifyCommit is the opt-in a non-quorum mode needs to acknowledge
		// at all; the default mode leaves the default (false).
		cfg.NotifyCommit = mode == writemode.WALDurable
		if cfg.NoSyncWAL {
			t.Fatal("the default HostConfig skips the WAL fsync")
		}

		hfs := newHoldFS()
		db, err := waldb.Open(cfg.WALDir, waldb.WithFS(hfs))
		if err != nil {
			t.Fatalf("waldb.Open[%d]: %v", i, err)
		}
		h, err := quicraft.NewHost(cfg, quicraft.WithLogDB(&holdDB{DB: db, fs: hfs}), quicraft.WithoutEncryption())
		if err != nil {
			_ = db.Close()
			t.Fatalf("NewHost[%d]: %v", i, err)
		}
		nodes[i] = &node{host: h, db: db, fs: hfs}
	}
	t.Cleanup(func() {
		for _, n := range nodes {
			n.fs.unhold()
			_ = n.host.Close()
			_ = n.db.Close()
		}
	})

	members := make(map[uint64]string, numNodes)
	ids := make(map[uint64]string, numNodes)
	for i, n := range nodes {
		members[uint64(i+1)] = n.host.RaftAddress()
		ids[uint64(i+1)] = n.host.NodeIdentity()
	}
	for i, n := range nodes {
		scfg := config.Config{ShardID: shardID, ReplicaID: uint64(i + 1)}
		scfg.SetDefaults()
		scfg.MemberIdentities = ids
		create := sm.NewCreateFunc(func(uint64, uint64) sm.StateMachine { return &countSM{} })
		if err := n.host.StartShard(members, false, create, scfg); err != nil {
			t.Fatalf("StartShard[%d]: %v", i, err)
		}
	}
	return nodes
}

// waitLeader returns the index of the node that leads the shard once every
// node agrees on it.
func waitLeader(t *testing.T, nodes []*node) int {
	t.Helper()
	deadline := time.Now().Add(leaderTimeout)
	for time.Now().Before(deadline) {
		var leader uint64
		agreed := true
		for _, n := range nodes {
			id, ok, err := n.host.LeaderID(shardID)
			if err != nil || !ok || id == 0 || (leader != 0 && id != leader) {
				agreed = false
				break
			}
			leader = id
		}
		if agreed {
			return int(leader - 1)
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatal("no leader elected")
	return -1
}

// acknowledge proposes cmd on h and returns a channel that receives the
// outcome when the write is acknowledged the way mode acknowledges it.
func acknowledge(ctx context.Context, t *testing.T, h *quicraft.Host, mode writemode.Mode, cmd []byte) <-chan error {
	t.Helper()
	done := make(chan error, 1)
	switch mode {
	case writemode.Sync:
		go func() {
			_, err := h.SyncPropose(ctx, shardID, cmd)
			done <- err
		}()
	case writemode.WALDurable:
		rs, err := h.Propose(ctx, shardID, cmd)
		if err != nil {
			t.Fatalf("Propose: %v", err)
		}
		go func() {
			defer rs.Release()
			select {
			case r := <-rs.CommittedC():
				_ = r
				done <- nil
			case <-ctx.Done():
				done <- ctx.Err()
			}
		}()
	default:
		t.Fatalf("write mode %v has no acknowledgement path in this test", mode)
	}
	return done
}

// TestDefaultWriteAcknowledgedOnlyAfterQuorumFsync proves the durable-by-
// default row end to end: on a three-replica shard with the default
// HostConfig and write mode, a write whose WAL fsync is held on both
// followers is not acknowledged although the leader has fsynced it and both
// followers hold its bytes; releasing one follower's fsync (a quorum of two)
// completes it.
func TestDefaultWriteAcknowledgedOnlyAfterQuorumFsync(t *testing.T) {
	nodes := startCluster(t, defaultMode)
	leader := waitLeader(t, nodes)

	ctx, cancel := context.WithTimeout(context.Background(), ackTimeout)
	defer cancel()

	// A first write with nothing held: the shard is live.
	if err := <-acknowledge(ctx, t, nodes[leader].host, defaultMode, []byte("warm")); err != nil {
		t.Fatalf("warm-up write: %v", err)
	}

	var followers []*node
	for i, n := range nodes {
		if i != leader {
			followers = append(followers, n)
		}
	}
	held := []byte("durable-held-proposal")
	for _, f := range followers {
		f.fs.hold(held)
	}

	done := acknowledge(ctx, t, nodes[leader].host, defaultMode, held)

	// Both followers must have received the held write and be blocked in
	// the fsync of the SaveState that carries its entry: the only thing
	// missing for a quorum is durability. No other Sync is held, so a
	// blocked Sync here is that entry's.
	deadline := time.Now().Add(ackTimeout)
	for followers[0].fs.waiting.Load() == 0 || followers[1].fs.waiting.Load() == 0 {
		if time.Now().After(deadline) {
			t.Fatal("the followers never reached the WAL fsync of the held write")
		}
		select {
		case err := <-done:
			t.Fatalf("write acknowledged (err=%v) before both followers reached its fsync", err)
		case <-time.After(5 * time.Millisecond):
		}
	}
	index := followers[0].fs.index.Load()
	if other := followers[1].fs.index.Load(); index == 0 || other != index {
		t.Fatalf("the followers hold different entries: index %d and %d", index, other)
	}

	select {
	case err := <-done:
		t.Fatalf("write acknowledged (err=%v) while only the leader had fsynced entry %d: "+
			"the default acknowledges before a quorum fsync", err, index)
	case <-time.After(holdWindow):
	}

	// One follower's fsync returns: leader plus one follower is a quorum.
	followers[0].fs.unhold()

	select {
	case err := <-done:
		if err != nil {
			if errors.Is(err, context.DeadlineExceeded) {
				t.Fatal("write not acknowledged after a quorum fsynced it")
			}
			t.Fatalf("write failed after a quorum fsynced it: %v", err)
		}
	case <-time.After(ackTimeout):
		t.Fatal("write not acknowledged after a quorum fsynced it")
	}
	// The ack came from the released follower's completed fsync of the held
	// entry, not from the follower that is still held.
	if !followers[0].fs.saved.Load() {
		t.Fatalf("write acknowledged before the released follower's fsync of entry %d completed", index)
	}
	if followers[1].fs.saved.Load() || followers[1].fs.waiting.Load() == 0 {
		t.Fatalf("the held follower's fsync of entry %d returned while it was held", index)
	}
	followers[1].fs.unhold()
}
