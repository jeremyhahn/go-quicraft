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

package host_test

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"

	quicraft "github.com/jeremyhahn/go-quicraft/pkg"
	"github.com/jeremyhahn/go-quicraft/pkg/config"
	"github.com/jeremyhahn/go-quicraft/pkg/sm"
)

// appliedIndexDiskSM is an on-disk state machine that durably records the
// index of the last entry its Update saw, the way a database-backed state
// machine stores its applied index beside its data. Open reports that index,
// so it is exactly what Host.StartShard recovers to.
//
// Entries the host applies without calling Update (the leader's no-op,
// config changes, session register/unregister) never reach this record,
// which is what #363 is about.
type appliedIndexDiskSM struct {
	mu      sync.Mutex
	dir     string
	applied uint64
	count   uint64
}

const appliedIndexFile = "applied.bin"

func (m *appliedIndexDiskSM) Open(_ context.Context, dir string, _ <-chan struct{}) (uint64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.dir = dir
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return 0, err
	}
	data, err := os.ReadFile(filepath.Join(dir, appliedIndexFile))
	if errors.Is(err, os.ErrNotExist) {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	if len(data) != 16 {
		return 0, fmt.Errorf("%s: %d bytes, want 16", appliedIndexFile, len(data))
	}
	m.applied = binary.LittleEndian.Uint64(data[0:8])
	m.count = binary.LittleEndian.Uint64(data[8:16])
	return m.applied, nil
}

func (m *appliedIndexDiskSM) encodeLocked() []byte {
	var buf [16]byte
	binary.LittleEndian.PutUint64(buf[0:8], m.applied)
	binary.LittleEndian.PutUint64(buf[8:16], m.count)
	return buf[:]
}

// persistLocked writes the record durably: temp file, fsync, rename.
func (m *appliedIndexDiskSM) persistLocked() error {
	path := filepath.Join(m.dir, appliedIndexFile)
	tmp := path + ".tmp"
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o600)
	if err != nil {
		return err
	}
	if _, err := f.Write(m.encodeLocked()); err != nil {
		f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}

func (m *appliedIndexDiskSM) Update(_ context.Context, entries []sm.Entry, results []sm.Result) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	for i, e := range entries {
		m.count++
		m.applied = e.Index
		results[i] = sm.Result{Value: m.count}
	}
	return m.persistLocked()
}

func (m *appliedIndexDiskSM) Lookup(_ context.Context, _ interface{}) (interface{}, error) {
	return m.Applied(), nil
}

// Sync is a no-op: Update persists the record before it returns.
func (m *appliedIndexDiskSM) Sync() error { return nil }

func (m *appliedIndexDiskSM) PrepareSnapshot() (interface{}, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.encodeLocked(), nil
}

func (m *appliedIndexDiskSM) SaveSnapshot(_ context.Context, snapCtx interface{}, w io.Writer, _ <-chan struct{}) error {
	data, ok := snapCtx.([]byte)
	if !ok {
		return fmt.Errorf("snapshot context is %T, want []byte", snapCtx)
	}
	_, err := w.Write(data)
	return err
}

func (m *appliedIndexDiskSM) RecoverFromSnapshot(_ context.Context, r io.Reader, _ <-chan struct{}) error {
	var buf [16]byte
	if _, err := io.ReadFull(r, buf[:]); err != nil {
		return err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.applied = binary.LittleEndian.Uint64(buf[0:8])
	m.count = binary.LittleEndian.Uint64(buf[8:16])
	return m.persistLocked()
}

func (m *appliedIndexDiskSM) Close(_ context.Context) error { return nil }

// Applied returns the applied index the state machine has stored.
func (m *appliedIndexDiskSM) Applied() uint64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.applied
}

var _ sm.DiskStateMachine = (*appliedIndexDiskSM)(nil)

// diskRestartCluster is a three-node cluster of appliedIndexDiskSM replicas
// built by newTestClusterWithRestart, so every host keeps the real WAL sync
// and can be reopened on its own directories.
type diskRestartCluster struct {
	*testCluster
	configs []nodeConfig
	sms     []atomic.Pointer[appliedIndexDiskSM]
	shardID uint64
}

func newDiskRestartCluster(t *testing.T) *diskRestartCluster {
	t.Helper()
	cluster, configs := newTestClusterWithRestart(t, 3)
	t.Cleanup(func() { cluster.close() })
	c := &diskRestartCluster{
		testCluster: cluster,
		configs:     configs,
		sms:         make([]atomic.Pointer[appliedIndexDiskSM], 3),
		shardID:     1,
	}
	for i := range 3 {
		if err := c.startShardOn(i, c.hosts[i]); err != nil {
			t.Fatalf("StartShard[%d] failed: %v", i, err)
		}
	}
	c.waitForClusterLeader(c.shardID, leaderElectionTimeout)
	return c
}

// startShardOn starts the shard on node idx with a fresh state machine, which
// Open seeds from the node's on-disk record. Automatic snapshots are off: the
// only snapshots are the ones the test requests, at the indexes it chose.
func (c *diskRestartCluster) startShardOn(idx int, h *quicraft.Host) error {
	members := make(map[uint64]string, len(c.addrs))
	for i, addr := range c.addrs {
		members[uint64(i+1)] = addr
	}
	create := func(_, _ uint64) interface{} {
		m := &appliedIndexDiskSM{}
		c.sms[idx].Store(m)
		return m
	}
	scfg := testShardConfig(c.shardID, uint64(idx+1))
	scfg.SnapshotEntries = config.SnapshotsDisabled
	scfg.MemberIdentities = c.identities()
	return h.StartShard(members, false, create, scfg)
}

func (c *diskRestartCluster) hostApplied(idx int) uint64 {
	c.hostsMu.RLock()
	h := c.hosts[idx]
	c.hostsMu.RUnlock()
	info, err := h.ShardInfo(c.shardID)
	if err != nil {
		return 0
	}
	return info.LastApplied
}

func (c *diskRestartCluster) smApplied(idx int) uint64 {
	if m := c.sms[idx].Load(); m != nil {
		return m.Applied()
	}
	return 0
}

// propose commits n user entries through the leader and waits until every
// node's state machine has stored the last of them. It returns that index.
func (c *diskRestartCluster) propose(t *testing.T, n int) uint64 {
	t.Helper()
	for i := range n {
		proposeToLeader(t, c.testCluster, c.shardID, []byte(fmt.Sprintf("entry-%d", i)), failoverTimeout)
	}
	// The leader applied the last proposal before it returned, and only user
	// entries reach Update, so the highest index any state machine stores is
	// the last proposal's. An attempt that failed but still committed lands
	// before the accepted one in the log.
	var last uint64
	for i := range 3 {
		last = max(last, c.smApplied(i))
	}
	for i := range 3 {
		waitForCondition(t, failoverTimeout,
			fmt.Sprintf("node[%d] state machine stores applied index %d", i, last),
			func() bool { return c.smApplied(i) == last && c.hostApplied(i) == last })
	}
	return last
}

// snapshotOn requests a snapshot on node idx and returns its index.
func (c *diskRestartCluster) snapshotOn(t *testing.T, idx int) uint64 {
	t.Helper()
	return requestSnapshotIndex(t, c.hosts[idx], c.shardID)
}

// restartAll stops every node cleanly, then reopens each host on its own
// directories and starts the shard on it. It returns each node's StartShard
// error; the hosts are registered with the cluster either way, so cleanup
// closes them.
func (c *diskRestartCluster) restartAll(t *testing.T) []error {
	t.Helper()
	c.close()
	errs := make([]error, 3)
	for i := range 3 {
		nc := c.configs[i]
		h, err := quicraft.NewHost(hostConfigAt(nc.walDir, nc.nodeHostDir, nc.addr, c.leaves[i]), quicraft.WithoutEncryption())
		if err != nil {
			t.Fatalf("restart: NewHost[%d] failed: %v", i, err)
		}
		c.hostsMu.Lock()
		c.hosts[i] = h
		c.stopped[i].Store(false)
		c.hostsMu.Unlock()
		errs[i] = c.startShardOn(i, h)
	}
	return errs
}

// requireRestarted fails the test with every node whose shard did not start,
// and otherwise proves the restarted cluster still commits and applies.
func (c *diskRestartCluster) requireRestarted(t *testing.T, errs []error, snapIdx int, snapIndex uint64, smIndex []uint64) {
	t.Helper()
	failed := false
	for i, err := range errs {
		if err == nil {
			continue
		}
		failed = true
		t.Errorf("node[%d] StartShard after a clean stop of every node failed "+
			"(snapshot taken on node[%d] at index %d; node[%d] state machine stored applied index %d): %v",
			i, snapIdx, snapIndex, i, smIndex[i], err)
	}
	if failed {
		t.FailNow()
	}
	c.waitForClusterLeader(c.shardID, failoverTimeout)
	c.propose(t, 1)
}

// TestOnDiskSM_RestartAfterSnapshotPastAppliedIndex is the regression test
// for #363: a snapshot captured right after a new leader's no-op sits one
// index past the applied index the on-disk state machine stored, because the
// no-op is applied without reaching its Update. After a clean stop of every
// node, the replica holding that snapshot recovers from it (#295); before
// that, it looked up the raft term of the index its state machine recovered
// to, below the snapshot and gone from the log reader, and StartShard failed
// with ErrRecoveredTermUnknown.
//
// The order is driven, not raced: automatic snapshots are off, nothing is
// proposed after the leader change, and the snapshot is requested only once
// the snapshot node has applied the no-op and its state machine is verified to
// sit below it.
func TestOnDiskSM_RestartAfterSnapshotPastAppliedIndex(t *testing.T) {
	c := newDiskRestartCluster(t)

	userIndex := c.propose(t, 5)

	// A leader change: the new leader commits a no-op in its new term.
	oldLeader := c.findLeader(c.shardID)
	newLeader := (oldLeader + 1) % 3
	transferLeadershipTo(t, c.testCluster, c.shardID, newLeader, failoverTimeout)

	// The snapshot node is the old leader, now a follower. Wait for the no-op
	// to be applied there; its state machine must not move.
	snapNode := oldLeader
	waitForCondition(t, failoverTimeout,
		fmt.Sprintf("node[%d] applied the new leader's no-op", snapNode),
		func() bool {
			leaderApplied := c.hostApplied(newLeader)
			return leaderApplied > userIndex && c.hostApplied(snapNode) == leaderApplied
		})
	if got := c.smApplied(snapNode); got != userIndex {
		t.Fatalf("precondition: node[%d] state machine stored %d, want %d (the no-op must not reach Update)",
			snapNode, got, userIndex)
	}

	snapIndex := c.snapshotOn(t, snapNode)
	smIndex := []uint64{c.smApplied(0), c.smApplied(1), c.smApplied(2)}
	if snapIndex <= smIndex[snapNode] {
		t.Fatalf("precondition: snapshot index %d is not past the state machine's stored applied index %d",
			snapIndex, smIndex[snapNode])
	}
	t.Logf("node[%d]: snapshot at %d, state machine stored applied index %d", snapNode, snapIndex, smIndex[snapNode])

	errs := c.restartAll(t)
	c.requireRestarted(t, errs, snapNode, snapIndex, smIndex)
}

// TestOnDiskSM_RestartAfterSnapshotAtAppliedIndex is the passing edge of
// #363: the snapshot is taken right after a user entry, so its index equals
// the state machine's stored applied index and the term resolves from the
// snapshot record.
func TestOnDiskSM_RestartAfterSnapshotAtAppliedIndex(t *testing.T) {
	c := newDiskRestartCluster(t)

	userIndex := c.propose(t, 5)
	snapNode := c.findLeader(c.shardID)
	snapIndex := c.snapshotOn(t, snapNode)
	if snapIndex != userIndex {
		t.Fatalf("precondition: snapshot index %d, want the state machine's stored applied index %d",
			snapIndex, userIndex)
	}
	smIndex := []uint64{c.smApplied(0), c.smApplied(1), c.smApplied(2)}

	errs := c.restartAll(t)
	c.requireRestarted(t, errs, snapNode, snapIndex, smIndex)
}

// TestOnDiskSM_RestartAfterSnapshotBelowAppliedIndex is the other passing
// edge of #363: user entries applied after the snapshot put the state
// machine's stored applied index above it, and the term resolves from the
// log.
func TestOnDiskSM_RestartAfterSnapshotBelowAppliedIndex(t *testing.T) {
	c := newDiskRestartCluster(t)

	c.propose(t, 5)
	snapNode := c.findLeader(c.shardID)
	snapIndex := c.snapshotOn(t, snapNode)
	userIndex := c.propose(t, 3)
	if userIndex <= snapIndex {
		t.Fatalf("precondition: state machine stored applied index %d is not above snapshot index %d",
			userIndex, snapIndex)
	}
	smIndex := []uint64{c.smApplied(0), c.smApplied(1), c.smApplied(2)}

	errs := c.restartAll(t)
	c.requireRestarted(t, errs, snapNode, snapIndex, smIndex)
}
