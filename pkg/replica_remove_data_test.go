// Copyright 2026 Jeremy Hahn
// SPDX-License-Identifier: Apache-2.0

package quicraft

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/jeremyhahn/go-quicraft/pkg/config"
	"github.com/jeremyhahn/go-quicraft/pkg/internal/engine"
	"github.com/jeremyhahn/go-quicraft/test/netport"
	"github.com/jeremyhahn/go-quicraft/test/testpki"
)

// removeDataCluster is three in-process hosts on loopback mTLS, each with its
// own on-disk WAL, running one shard.
type removeDataCluster struct {
	t     *testing.T
	ca    *testpki.CA
	hosts []*Host
	cfgs  []config.HostConfig
	addrs []string
}

const removeDataShard = 1

func newRemoveDataCluster(t *testing.T, opts ...HostOption) *removeDataCluster {
	t.Helper()
	ca := testpki.NewCA(t)
	leaves := ca.Leaves(t, 3)
	c := &removeDataCluster{t: t, ca: ca}
	for i := range 3 {
		addr := netport.MustReserveUDP(t.Fatalf)
		cfg := testHostConfig(t)
		cfg.RaftAddress = addr
		cfg.ListenAddress = addr
		cfg.TransportConfig.MTLSConfig = leaves[i]
		cfg.ShutdownTimeout = 2 * time.Second
		h, err := NewHost(cfg, append([]HostOption{WithoutEncryption()}, opts...)...)
		if err != nil {
			t.Fatalf("NewHost[%d]: %v", i, err)
		}
		t.Cleanup(func() { _ = h.Close() })
		c.hosts = append(c.hosts, h)
		c.cfgs = append(c.cfgs, cfg)
		c.addrs = append(c.addrs, addr)
	}
	return c
}

// shardConfig is replica i+1's shard config; the same value restarts it.
func (c *removeDataCluster) shardConfig(replicaID uint64) config.Config {
	scfg := testShardConfig(removeDataShard, replicaID)
	scfg.MemberIdentities = map[uint64]string{}
	for i, h := range c.hosts {
		scfg.MemberIdentities[uint64(i+1)] = h.NodeIdentity()
	}
	return scfg
}

func (c *removeDataCluster) members() map[uint64]string {
	m := map[uint64]string{}
	for i, a := range c.addrs {
		m[uint64(i+1)] = a
	}
	return m
}

// leader returns the index of the host leading the shard, polling until the
// live hosts agree on one.
func (c *removeDataCluster) leader(live []int) int {
	c.t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		for _, i := range live {
			id, ok, err := c.hosts[i].LeaderID(removeDataShard)
			if err != nil || !ok || id == 0 {
				continue
			}
			for _, j := range live {
				if c.replicaOf(j) == id {
					return j
				}
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	c.t.Fatalf("no leader among hosts %v", live)
	return -1
}

// replicaOf is the replica ID host i runs (0 when it runs none).
func (c *removeDataCluster) replicaOf(i int) uint64 {
	v, ok := c.hosts[i].nodes.Load(uint64(removeDataShard))
	if !ok {
		return 0
	}
	ns, ok := v.(*nodeState)
	if !ok || ns.stopped.Load() {
		return 0
	}
	return ns.replicaID
}

// propose commits n entries through the leader among live, retrying across a
// leader change; a failure means the live replicas stopped serving.
func (c *removeDataCluster) propose(live []int, tag string, n int) {
	c.t.Helper()
	for k := range n {
		cmd := []byte(fmt.Sprintf("set:%s-%d=%d", tag, k, k))
		deadline := time.Now().Add(15 * time.Second)
		for {
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			_, err := c.hosts[c.leader(live)].SyncPropose(ctx, removeDataShard, cmd)
			cancel()
			if err == nil {
				break
			}
			if time.Now().After(deadline) {
				c.t.Fatalf("replicas %v stopped serving: propose %q: %v", live, cmd, err)
			}
			time.Sleep(20 * time.Millisecond)
		}
	}
}

// waitHashMatch waits until host i's state machine hash equals the leader's
// at the same applied index.
func (c *removeDataCluster) waitHashMatch(i int, live []int) {
	c.t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	var lastErr string
	for time.Now().Before(deadline) {
		l := c.leader(live)
		lh, li, lok, lerr := c.hosts[l].StateMachineHash(removeDataShard)
		fh, fi, fok, ferr := c.hosts[i].StateMachineHash(removeDataShard)
		if lerr == nil && ferr == nil && lok && fok && li == fi && lh == fh {
			return
		}
		lastErr = fmt.Sprintf("leader (%d,%d,%v,%v) host %d (%d,%d,%v,%v)",
			lh, li, lok, lerr, i, fh, fi, fok, ferr)
		time.Sleep(20 * time.Millisecond)
	}
	c.t.Fatalf("host %d never reached the leader's state: %s", i, lastErr)
}

// replicaSnapshotDir is the snapshot directory StartShard creates for the
// replica on host i (the shard config sets no DataDir).
func (c *removeDataCluster) replicaSnapshotDir(i int, replicaID uint64) string {
	base := shardSnapshotBaseDir(c.cfgs[i].NodeHostDir, "", removeDataShard)
	return engine.ReplicaSnapshotDir(base, removeDataShard, replicaID)
}

type replicaLogState struct {
	first, length uint64
	term, commit  uint64
	bootstrapped  bool
}

func readReplicaLogState(t *testing.T, h *Host, replicaID uint64) replicaLogState {
	t.Helper()
	var s replicaLogState
	var err error
	if s.first, s.length, err = h.logdb.EntryRange(removeDataShard, replicaID); err != nil {
		t.Fatalf("EntryRange: %v", err)
	}
	st, _, err := h.logdb.ReadState(removeDataShard, replicaID)
	if err != nil && s.length > 0 {
		t.Fatalf("ReadState: %v", err)
	}
	s.term, s.commit = st.Term, st.Commit
	if _, s.bootstrapped, err = h.logdb.GetBootstrap(removeDataShard, replicaID); err != nil {
		t.Fatalf("GetBootstrap: %v", err)
	}
	return s
}

// TestSyncRemoveData_RefusesRunningRemovesStoppedReplica drives RemoveData and
// SyncRemoveData against a follower of a live three-replica shard: both refuse
// while it runs and leave its data alone; once stopped, SyncRemoveData drops
// its WAL data and retires its ID, and it rejoins under a new ID and catches
// up while the other two replicas keep serving.
func TestSyncRemoveData_RefusesRunningRemovesStoppedReplica(t *testing.T) {
	c := newRemoveDataCluster(t)
	for i, h := range c.hosts {
		if err := h.StartShard(c.members(), false, hashingCreateFunc, c.shardConfig(uint64(i+1))); err != nil {
			t.Fatalf("StartShard[%d]: %v", i, err)
		}
	}
	all := []int{0, 1, 2}
	c.propose(all, "before", 10)

	leader := c.leader(all)
	f := (leader + 1) % 3
	fid := uint64(f + 1)
	var live []int
	for _, i := range all {
		if i != f {
			live = append(live, i)
		}
	}
	c.waitHashMatch(f, all)
	fh := c.hosts[f]

	// A running follower is refused by both entry points, and its data stays.
	before := readReplicaLogState(t, fh, fid)
	if before.length == 0 || !before.bootstrapped {
		t.Fatalf("follower holds no WAL data before removal: %+v", before)
	}
	snapDir := c.replicaSnapshotDir(f, fid)
	if _, err := os.Stat(snapDir); err != nil {
		t.Fatalf("follower snapshot directory missing before removal: %v", err)
	}
	err := fh.SyncRemoveData(context.Background(), removeDataShard, fid)
	if !errors.Is(err, ErrShardNotStopped) {
		t.Fatalf("SyncRemoveData on a running replica = %v, want ErrShardNotStopped", err)
	}
	if err := fh.RemoveData(removeDataShard, fid); !errors.Is(err, ErrShardNotStopped) {
		t.Fatalf("RemoveData on a running replica = %v, want ErrShardNotStopped", err)
	}
	if after := readReplicaLogState(t, fh, fid); after.first != before.first ||
		after.length < before.length || after.term < before.term || !after.bootstrapped {
		t.Fatalf("a refused removal changed the running replica's WAL: before %+v after %+v", before, after)
	}
	if removed, err := fh.logdb.NodeRemoved(removeDataShard, fid); err != nil || removed {
		t.Fatalf("a refused removal retired the running replica: removed=%v err=%v", removed, err)
	}
	if _, err := os.Stat(snapDir); err != nil {
		t.Fatalf("a refused removal touched the snapshot directory: %v", err)
	}
	c.propose(all, "refused", 5)
	c.waitHashMatch(f, all)

	// Stopped, the follower's data goes.
	if err := fh.StopReplica(removeDataShard, fid); err != nil {
		t.Fatalf("StopReplica: %v", err)
	}
	c.propose(live, "stopped", 5)
	if err := fh.SyncRemoveData(context.Background(), removeDataShard, fid); err != nil {
		t.Fatalf("SyncRemoveData on a stopped replica: %v", err)
	}
	if got := readReplicaLogState(t, fh, fid); got.length != 0 || got.bootstrapped || got.term != 0 || got.commit != 0 {
		t.Fatalf("the WAL still holds the removed replica's data: %+v", got)
	}
	if removed, err := fh.logdb.NodeRemoved(removeDataShard, fid); err != nil || !removed {
		t.Fatalf("the WAL does not report the replica removed: removed=%v err=%v", removed, err)
	}
	if _, err := os.Stat(snapDir); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("the removed replica's snapshot directory remains (stat err %v): %s", err, snapDir)
	}
	c.propose(live, "removed", 5)

	// The ID is retired (RemoveData's contract): the same config is refused.
	err = fh.StartShard(c.members(), false, hashingCreateFunc, c.shardConfig(fid))
	if !errors.Is(err, ErrReplicaRetired) {
		t.Fatalf("restart of a removed replica under its old ID = %v, want ErrReplicaRetired", err)
	}

	// It rejoins under a new ID from the same address and data directory and
	// catches up. The removed replica's key is tombstoned by the shard
	// (raft.ErrRemovedMemberIdentity), so the host restarts with a new one.
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if err := c.hosts[c.leader(live)].SyncRequestRemoveNode(ctx, removeDataShard, fid, 0); err != nil {
		t.Fatalf("SyncRequestRemoveNode(%d): %v", fid, err)
	}
	if err := fh.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	rcfg := c.cfgs[f]
	rcfg.TransportConfig.MTLSConfig = c.ca.Leaves(t, 1)[0]
	fh, err = NewHost(rcfg, WithoutEncryption())
	if err != nil {
		t.Fatalf("NewHost(rejoin): %v", err)
	}
	t.Cleanup(func() { _ = fh.Close() })
	c.hosts[f] = fh
	newID := uint64(4)
	// The removal can move leadership while the add is in flight; a
	// not-leader refusal may still commit, so retry until it is a member.
	for {
		lh := c.hosts[c.leader(live)]
		err := lh.SyncRequestAddNodeWithIdentity(ctx, removeDataShard, newID, c.addrs[f], fh.NodeIdentity(), 0)
		if err == nil {
			break
		}
		if m, mErr := lh.SyncGetShardMembership(ctx, removeDataShard); mErr == nil {
			if _, ok := m.Nodes[newID]; ok {
				break
			}
		}
		if !errors.Is(err, ErrNotLeader) || ctx.Err() != nil {
			t.Fatalf("SyncRequestAddNodeWithIdentity(%d): %v", newID, err)
		}
		time.Sleep(20 * time.Millisecond)
	}
	rejoin := testShardConfig(removeDataShard, newID)
	rejoin.MemberIdentities = map[uint64]string{newID: fh.NodeIdentity()}
	for _, i := range live {
		rejoin.MemberIdentities[uint64(i+1)] = c.hosts[i].NodeIdentity()
	}
	if err := fh.StartShard(nil, true, hashingCreateFunc, rejoin); err != nil {
		t.Fatalf("StartShard(join, %d): %v", newID, err)
	}
	c.propose(live, "rejoined", 5)
	c.waitHashMatch(f, live)
}

// TestSyncRemoveData_RemovesSnapshotDirUnderDataDir: a replica whose shard
// roots its snapshot tree off Config.DataDir has that tree's replica
// directory removed, resolved from the bootstrap record after the replica
// has been stopped and unloaded.
func TestSyncRemoveData_RemovesSnapshotDirUnderDataDir(t *testing.T) {
	c := newRemoveDataCluster(t)
	dataDirs := make([]string, len(c.hosts))
	for i, h := range c.hosts {
		dataDirs[i] = t.TempDir()
		scfg := c.shardConfig(uint64(i + 1))
		scfg.DataDir = dataDirs[i]
		if err := h.StartShard(c.members(), false, hashingCreateFunc, scfg); err != nil {
			t.Fatalf("StartShard[%d]: %v", i, err)
		}
	}
	const f = 1
	fid := uint64(f + 1)
	fh := c.hosts[f]
	snapDir := engine.ReplicaSnapshotDir(
		shardSnapshotBaseDir(c.cfgs[f].NodeHostDir, dataDirs[f], removeDataShard), removeDataShard, fid)
	if _, err := os.Stat(snapDir); err != nil {
		t.Fatalf("snapshot directory missing before removal: %v", err)
	}
	if err := fh.StopReplica(removeDataShard, fid); err != nil {
		t.Fatalf("StopReplica: %v", err)
	}
	if err := fh.SyncRemoveData(context.Background(), removeDataShard, fid); err != nil {
		t.Fatalf("SyncRemoveData on a stopped replica: %v", err)
	}
	if _, err := os.Stat(snapDir); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("the removed replica's snapshot directory remains (stat err %v): %s", err, snapDir)
	}
}

// A replica started with join=true and a custom DataDir roots its snapshot
// tree under that DataDir; RemoveData must find it there, not under
// NodeHostDir, so the joined replica's snapshots do not outlive its removal.
func TestSyncRemoveData_RemovesJoinedReplicaSnapshotsUnderDataDir(t *testing.T) {
	c := newRemoveDataCluster(t)
	const f = 2
	fid := uint64(f + 1)
	dataDir := t.TempDir()
	for i, h := range c.hosts {
		scfg := c.shardConfig(uint64(i + 1))
		members, join := c.members(), false
		if i == f {
			scfg.DataDir = dataDir
			members, join = nil, true
		}
		if err := h.StartShard(members, join, hashingCreateFunc, scfg); err != nil {
			t.Fatalf("StartShard[%d] (join=%v): %v", i, join, err)
		}
	}
	fh := c.hosts[f]
	snapDir := engine.ReplicaSnapshotDir(
		shardSnapshotBaseDir(c.cfgs[f].NodeHostDir, dataDir, removeDataShard), removeDataShard, fid)
	if _, err := os.Stat(snapDir); err != nil {
		t.Fatalf("joined replica's snapshot directory missing before removal: %v", err)
	}
	if err := fh.StopReplica(removeDataShard, fid); err != nil {
		t.Fatalf("StopReplica: %v", err)
	}
	if err := fh.SyncRemoveData(context.Background(), removeDataShard, fid); err != nil {
		t.Fatalf("SyncRemoveData on a stopped joined replica: %v", err)
	}
	if _, err := os.Stat(snapDir); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("the removed joined replica's snapshot directory remains under its "+
			"DataDir (stat err %v): %s", err, snapDir)
	}
}

// failRemoveAllFS is the OS snapshot filesystem whose first RemoveAll of the
// armed path fails with errInjectedRemoveAll and has no effect.
type failRemoveAllFS struct {
	osSnapshotFS
	mu     sync.Mutex
	target string
	fired  bool
}

var errInjectedRemoveAll = errors.New("injected RemoveAll failure")

func (f *failRemoveAllFS) arm(path string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.target = filepath.Clean(path)
}

func (f *failRemoveAllFS) RemoveAll(path string) error {
	f.mu.Lock()
	fail := !f.fired && f.target != "" && filepath.Clean(path) == f.target
	if fail {
		f.fired = true
	}
	f.mu.Unlock()
	if fail {
		return errInjectedRemoveAll
	}
	return f.osSnapshotFS.RemoveAll(path)
}

// A RemoveData whose snapshot removal fails must leave the replica
// retryable: it reports the error, does not retire the replica, and keeps
// the record that locates the snapshot tree, so a second RemoveData removes
// the directory under the shard's DataDir.
func TestSyncRemoveData_RetriesAfterSnapshotRemovalFails(t *testing.T) {
	fsys := &failRemoveAllFS{}
	c := newRemoveDataCluster(t, withSnapshotFS(fsys))
	dataDirs := make([]string, len(c.hosts))
	for i, h := range c.hosts {
		dataDirs[i] = t.TempDir()
		scfg := c.shardConfig(uint64(i + 1))
		scfg.DataDir = dataDirs[i]
		if err := h.StartShard(c.members(), false, hashingCreateFunc, scfg); err != nil {
			t.Fatalf("StartShard[%d]: %v", i, err)
		}
	}
	const f = 1
	fid := uint64(f + 1)
	fh := c.hosts[f]
	snapDir := engine.ReplicaSnapshotDir(
		shardSnapshotBaseDir(c.cfgs[f].NodeHostDir, dataDirs[f], removeDataShard), removeDataShard, fid)
	if _, err := os.Stat(snapDir); err != nil {
		t.Fatalf("snapshot directory missing before removal: %v", err)
	}
	if err := fh.StopReplica(removeDataShard, fid); err != nil {
		t.Fatalf("StopReplica: %v", err)
	}

	fsys.arm(snapDir)
	if err := fh.RemoveData(removeDataShard, fid); !errors.Is(err, errInjectedRemoveAll) {
		t.Fatalf("RemoveData with a failing snapshot removal: err = %v, want %v",
			err, errInjectedRemoveAll)
	}
	removed, err := fh.logdb.NodeRemoved(removeDataShard, fid)
	if err != nil {
		t.Fatalf("NodeRemoved: %v", err)
	}
	if removed {
		t.Fatal("a RemoveData that failed to remove the snapshot directory retired the " +
			"replica: a retry no longer finds the record that locates its snapshots")
	}

	if err := fh.RemoveData(removeDataShard, fid); err != nil {
		t.Fatalf("RemoveData retry: %v", err)
	}
	if _, err := os.Stat(snapDir); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("the retried removal left the snapshot directory under the DataDir "+
			"(stat err %v): %s", err, snapDir)
	}
	removed, err = fh.logdb.NodeRemoved(removeDataShard, fid)
	if err != nil || !removed {
		t.Fatalf("after the retry NodeRemoved = %v (err %v), want true", removed, err)
	}
}
