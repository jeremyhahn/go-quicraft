// Copyright 2026 Jeremy Hahn
// SPDX-License-Identifier: Apache-2.0

package quicraft

import (
	"bytes"
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
	"github.com/jeremyhahn/go-quicraft/pkg/logdb"
	"github.com/jeremyhahn/go-quicraft/pkg/logdb/memdb"
	"github.com/jeremyhahn/go-quicraft/pkg/logdb/waldb"
	"github.com/jeremyhahn/go-quicraft/pkg/proto"
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
	return newRemoveDataClusterWith(t, nil, opts...)
}

// newRemoveDataClusterWith is newRemoveDataCluster with perHost, when not nil,
// adding host i's own options (a LogDB wrapper, say) after opts.
func newRemoveDataClusterWith(t *testing.T, perHost func(i int, cfg config.HostConfig) []HostOption, opts ...HostOption) *removeDataCluster {
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
		hopts := append([]HostOption{WithoutEncryption()}, opts...)
		if perHost != nil {
			hopts = append(hopts, perHost(i, cfg)...)
		}
		h, err := NewHost(cfg, hopts...)
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

// waitAppliedMatch waits until host i has applied the leader's index. It is
// waitHashMatch for a replica restarted from its own snapshot: the hashing
// state machine keeps its digest out of its snapshot, so such a replica
// reaches the leader's index with a digest of only the entries after it.
func (c *removeDataCluster) waitAppliedMatch(i int, live []int) {
	c.t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	var lastErr string
	for time.Now().Before(deadline) {
		l := c.leader(live)
		_, li, lok, lerr := c.hosts[l].StateMachineHash(removeDataShard)
		_, fi, fok, ferr := c.hosts[i].StateMachineHash(removeDataShard)
		if lerr == nil && ferr == nil && lok && fok && li == fi {
			return
		}
		lastErr = fmt.Sprintf("leader (%d,%v,%v) host %d (%d,%v,%v)", li, lok, lerr, i, fi, fok, ferr)
		time.Sleep(20 * time.Millisecond)
	}
	c.t.Fatalf("host %d never applied the leader's index: %s", i, lastErr)
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
	// The replica is retired before its snapshots are touched, and the
	// durable intent names the directory the retry must still remove.
	removed, err := fh.logdb.NodeRemoved(removeDataShard, fid)
	if err != nil {
		t.Fatalf("NodeRemoved: %v", err)
	}
	if !removed {
		t.Fatal("RemoveData reached the snapshot removal without retiring the replica first")
	}
	intent := removeDataIntentFile(c.cfgs[f].NodeHostDir, removeDataShard, fid)
	if _, err := os.Stat(intent); err != nil {
		t.Fatalf("the failed snapshot removal left no removal intent at %s: %v", intent, err)
	}

	if err := fh.RemoveData(removeDataShard, fid); err != nil {
		t.Fatalf("RemoveData retry: %v", err)
	}
	if _, err := os.Stat(intent); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("the retried removal left its intent (stat err %v): %s", err, intent)
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

// removeDataIntentFile is where RemoveData records its removal intent for
// the replica, as the test expects it (NodeHostDir/remove-data).
func removeDataIntentFile(nodeHostDir string, shardID, replicaID uint64) string {
	return filepath.Join(nodeHostDir, "remove-data",
		fmt.Sprintf("shard-%d-replica-%d", shardID, replicaID))
}

// errInjectedRetire is the RemoveNodeData failure removeDataLogDB injects.
var errInjectedRetire = errors.New("injected RemoveNodeData failure")

// removeDataLogDB wraps the LogDB a host uses: RemoveNodeData fails
// failRetire times, and GetBootstrap reports no record while hideBootstrap
// is set (a replica joined, or imported into, before joins were recorded).
type removeDataLogDB struct {
	logdb.LogDB
	mu            sync.Mutex
	failRetire    int
	hideBootstrap bool
	// snapshotPath, when set, is the Filepath GetSnapshot reports.
	snapshotPath string
	// onRetire, when set, runs before each RemoveNodeData that is passed
	// through to the LogDB.
	onRetire func()
}

func (w *removeDataLogDB) GetSnapshot(shardID, replicaID uint64) (logdb.Snapshot, error) {
	w.mu.Lock()
	p := w.snapshotPath
	w.mu.Unlock()
	if p != "" {
		return logdb.Snapshot{Index: 1, Term: 1, Filepath: p}, nil
	}
	return w.LogDB.GetSnapshot(shardID, replicaID)
}

func (w *removeDataLogDB) setSnapshotPath(p string) {
	w.mu.Lock()
	w.snapshotPath = p
	w.mu.Unlock()
}

func (w *removeDataLogDB) RemoveNodeData(shardID, replicaID uint64) error {
	w.mu.Lock()
	onRetire := w.onRetire
	if w.failRetire > 0 {
		w.failRetire--
		w.mu.Unlock()
		return errInjectedRetire
	}
	w.mu.Unlock()
	if onRetire != nil {
		onRetire()
	}
	return w.LogDB.RemoveNodeData(shardID, replicaID)
}

func (w *removeDataLogDB) GetBootstrap(shardID, replicaID uint64) (logdb.Bootstrap, bool, error) {
	w.mu.Lock()
	hide := w.hideBootstrap
	w.mu.Unlock()
	if hide {
		return logdb.Bootstrap{}, false, nil
	}
	return w.LogDB.GetBootstrap(shardID, replicaID)
}

func (w *removeDataLogDB) set(failRetire int, hideBootstrap bool) {
	w.mu.Lock()
	w.failRetire, w.hideBootstrap = failRetire, hideBootstrap
	w.mu.Unlock()
}

// walWrapperFor returns a perHost option set giving host f a removeDataLogDB
// over its own WAL (opened where NewHost would open it) and storing the
// wrapper in *out.
func walWrapperFor(t *testing.T, f int, out **removeDataLogDB) func(int, config.HostConfig) []HostOption {
	return func(i int, cfg config.HostConfig) []HostOption {
		if i != f {
			return nil
		}
		wal, err := waldb.Open(cfg.WALDir)
		if err != nil {
			t.Fatalf("waldb.Open: %v", err)
		}
		*out = &removeDataLogDB{LogDB: wal}
		return []HostOption{WithLogDB(*out)}
	}
}

// TestSyncRemoveData_FailedRetirementKeepsCompactedSnapshot: a RemoveData
// whose retirement fails leaves the replica unretired, so it must also leave
// the snapshot its compacted log depends on; the replica then restarts.
func TestSyncRemoveData_FailedRetirementKeepsCompactedSnapshot(t *testing.T) {
	const f = 1
	fid := uint64(f + 1)
	var w *removeDataLogDB
	c := newRemoveDataClusterWith(t, walWrapperFor(t, f, &w))
	for i, h := range c.hosts {
		scfg := c.shardConfig(uint64(i + 1))
		scfg.CompactionOverhead = 10
		if err := h.StartShard(c.members(), false, hashingCreateFunc, scfg); err != nil {
			t.Fatalf("StartShard[%d]: %v", i, err)
		}
	}
	all := []int{0, 1, 2}
	c.propose(all, "before", 60)
	c.waitHashMatch(f, all)
	fh := c.hosts[f]
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	snapIndex, err := fh.SyncRequestSnapshot(ctx, removeDataShard, SnapshotOption{})
	if err != nil {
		t.Fatalf("SyncRequestSnapshot on the follower: %v", err)
	}
	if lr, err := fh.LogRange(removeDataShard); err != nil || lr.FirstIndex <= 1 {
		t.Fatalf("follower log after the snapshot at %d = %+v (err %v), want it compacted",
			snapIndex, lr, err)
	}
	snapDir := c.replicaSnapshotDir(f, fid)
	if err := fh.StopReplica(removeDataShard, fid); err != nil {
		t.Fatalf("StopReplica: %v", err)
	}

	w.set(1, false)
	if err := fh.RemoveData(removeDataShard, fid); !errors.Is(err, errInjectedRetire) {
		t.Fatalf("RemoveData with a failing retirement: err = %v, want %v", err, errInjectedRetire)
	}
	if removed, err := fh.logdb.NodeRemoved(removeDataShard, fid); err != nil || removed {
		t.Fatalf("after the failed retirement NodeRemoved = %v (err %v), want false", removed, err)
	}
	if _, err := os.Stat(snapDir); err != nil {
		t.Fatalf("a RemoveData that did not retire the replica deleted its snapshot "+
			"directory %s (stat err %v): its compacted log can no longer start", snapDir, err)
	}

	if err := fh.StartShard(c.members(), false, hashingCreateFunc, c.shardConfig(fid)); err != nil {
		t.Fatalf("StartShard of the unretired replica: %v", err)
	}
	c.waitAppliedMatch(f, all)
	if err := fh.StopReplica(removeDataShard, fid); err != nil {
		t.Fatalf("StopReplica (second): %v", err)
	}
	if err := fh.RemoveData(removeDataShard, fid); err != nil {
		t.Fatalf("RemoveData retry: %v", err)
	}
	if _, err := os.Stat(snapDir); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("the second RemoveData left the snapshot directory (stat err %v): %s", err, snapDir)
	}
}

// TestSyncRemoveData_RemovesImportedSnapshotUnderDataDir: a snapshot imported
// with WithImportDataDir for a replica this host never started (no bootstrap
// record) lives under that DataDir; RemoveData finds it from the LogDB
// snapshot record, removes it and retires the replica.
func TestSyncRemoveData_RemovesImportedSnapshotUnderDataDir(t *testing.T) {
	const shardID, replicaID uint64 = 7, 3
	const index uint64 = 900
	cfg := testHostConfig(t)
	real := memdb.New()
	db := &removeDataLogDB{LogDB: real}
	h, err := NewHost(cfg, WithLogDB(db), WithoutTransport(), WithoutEncryption())
	if err != nil {
		t.Fatalf("NewHost: %v", err)
	}
	defer h.Close()
	// The replica's only trace on this host: LogDB data without a bootstrap
	// record the host can read.
	if err := real.SaveBootstrap(shardID, replicaID, logdb.Bootstrap{
		Addresses: map[uint64]string{replicaID: "127.0.0.1:5000"},
	}); err != nil {
		t.Fatalf("SaveBootstrap: %v", err)
	}
	db.set(0, true)

	custom := t.TempDir()
	artifact := buildSnapshotArtifact(t, index, 5, 0, proto.Membership{},
		snapshotEnvelope([]byte("IMPORTED-UNDER-DATADIR")))
	if err := h.ImportSnapshot(context.Background(), shardID, bytes.NewReader(artifact),
		WithImportDataDir(custom)); err != nil {
		t.Fatalf("ImportSnapshot: %v", err)
	}
	replicaDir := engine.ReplicaSnapshotDir(
		shardSnapshotBaseDir(cfg.NodeHostDir, custom, shardID), shardID, replicaID)
	if _, err := os.Stat(replicaDir); err != nil {
		t.Fatalf("imported replica directory missing: %v", err)
	}

	if err := h.SyncRemoveData(context.Background(), shardID, replicaID); err != nil {
		t.Fatalf("SyncRemoveData: %v", err)
	}
	if _, err := os.Stat(replicaDir); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("the imported snapshot under the DataDir survived the removal "+
			"(stat err %v): %s", err, replicaDir)
	}
	if removed, err := h.logdb.NodeRemoved(shardID, replicaID); err != nil || !removed {
		t.Fatalf("NodeRemoved = %v (err %v), want true", removed, err)
	}
}

// TestSyncRemoveData_RemovesPreRecordJoinedReplicaSnapshots: a replica joined
// with a custom DataDir before joins were recorded (no bootstrap record) took
// a snapshot; RemoveData finds its directory from the snapshot record.
func TestSyncRemoveData_RemovesPreRecordJoinedReplicaSnapshots(t *testing.T) {
	const f = 2
	fid := uint64(f + 1)
	var w *removeDataLogDB
	c := newRemoveDataClusterWith(t, walWrapperFor(t, f, &w))
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
	all := []int{0, 1, 2}
	c.propose(all, "joined", 20)
	c.waitHashMatch(f, all)
	fh := c.hosts[f]
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if _, err := fh.SyncRequestSnapshot(ctx, removeDataShard, SnapshotOption{}); err != nil {
		t.Fatalf("SyncRequestSnapshot on the joined replica: %v", err)
	}
	snapDir := engine.ReplicaSnapshotDir(
		shardSnapshotBaseDir(c.cfgs[f].NodeHostDir, dataDir, removeDataShard), removeDataShard, fid)
	if err := fh.StopReplica(removeDataShard, fid); err != nil {
		t.Fatalf("StopReplica: %v", err)
	}

	w.set(0, true)
	if err := fh.SyncRemoveData(context.Background(), removeDataShard, fid); err != nil {
		t.Fatalf("SyncRemoveData on a pre-record joined replica: %v", err)
	}
	if _, err := os.Stat(snapDir); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("the pre-record joined replica's snapshots survived under its DataDir "+
			"(stat err %v): %s", err, snapDir)
	}
}

// TestReplicaSnapshotDirFromRecord: the helper finds the replica directory a
// snapshot record's Filepath lies in only when it is the layout StartShard
// creates for that shard and replica.
func TestReplicaSnapshotDirFromRecord(t *testing.T) {
	const shardID, replicaID uint64 = 5, 2
	root := filepath.Join(t.TempDir(), "data")
	dataDirLayout := engine.ReplicaSnapshotDir(shardSnapshotBaseDir("", root, shardID), shardID, replicaID)
	nhLayout := engine.ReplicaSnapshotDir(shardSnapshotBaseDir(root, "", shardID), shardID, replicaID)
	cases := []struct {
		name string
		path string
		want string
		ok   bool
	}{
		{"snapshot dir under a DataDir", filepath.Join(dataDirLayout, "snapshot-0000000384"), dataDirLayout, true},
		{"data file under a DataDir", filepath.Join(dataDirLayout, "snapshot-0000000384", "snapshot.dat"), dataDirLayout, true},
		{"the replica directory itself", dataDirLayout, dataDirLayout, true},
		{"NodeHostDir layout", filepath.Join(nhLayout, "snapshot-0000000001"), nhLayout, true},
		{"outside any replica layout", filepath.Join(root, "elsewhere", "snapshot.dat"), "", false},
		{"another replica", filepath.Join(root, "shard-5", "shard-5", "replica-9", "snapshot-1"), "", false},
		{"another shard", filepath.Join(root, "shard-6", "shard-6", "replica-2", "snapshot-1"), "", false},
		{"no shard base directory", filepath.Join(root, "x", "shard-5", "replica-2", "snapshot-1"), "", false},
		{"right replica, DataDir layout differs", filepath.Join(root, "shard-5", "x", "shard-5", "replica-2", "snapshot-1"), "", false},
		{"empty", "", "", false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, ok := replicaSnapshotDirFromRecord(tc.path, shardID, replicaID)
			if ok != tc.ok || got != tc.want {
				t.Fatalf("replicaSnapshotDirFromRecord(%q) = (%q, %v), want (%q, %v)",
					tc.path, got, ok, tc.want, tc.ok)
			}
		})
	}
}

// TestSyncRemoveData_RefusesUnknownSnapshotLayout: with no bootstrap record,
// no intent and a snapshot record whose Filepath lies outside any replica
// layout, RemoveData cannot locate the snapshot directory. It refuses before
// anything is retired or recorded, rather than reporting a success that
// leaves the snapshots behind.
func TestSyncRemoveData_RefusesUnknownSnapshotLayout(t *testing.T) {
	const f = 1
	fid := uint64(f + 1)
	var w *removeDataLogDB
	c := newRemoveDataClusterWith(t, walWrapperFor(t, f, &w))
	for i, h := range c.hosts {
		scfg := c.shardConfig(uint64(i + 1))
		if i == f {
			scfg.DataDir = t.TempDir()
		}
		if err := h.StartShard(c.members(), false, hashingCreateFunc, scfg); err != nil {
			t.Fatalf("StartShard[%d]: %v", i, err)
		}
	}
	fh := c.hosts[f]
	if err := fh.StopReplica(removeDataShard, fid); err != nil {
		t.Fatalf("StopReplica: %v", err)
	}

	stray := filepath.Join(t.TempDir(), "elsewhere", "snapshot.dat")
	w.set(0, true)
	w.setSnapshotPath(stray)
	err := fh.RemoveData(removeDataShard, fid)
	if !errors.Is(err, ErrInvalidOperation) {
		t.Fatalf("RemoveData with an unknown snapshot layout: err = %v, want %v",
			err, ErrInvalidOperation)
	}
	var le *removeDataLayoutUnknownError
	if !errors.As(err, &le) {
		t.Fatalf("RemoveData error %v is not a *removeDataLayoutUnknownError", err)
	}
	if le.shardID != removeDataShard || le.replicaID != fid || le.filepath != stray {
		t.Fatalf("removeDataLayoutUnknownError = %+v, want shard %d replica %d filepath %q",
			*le, removeDataShard, fid, stray)
	}
	if removed, err := fh.logdb.NodeRemoved(removeDataShard, fid); err != nil || removed {
		t.Fatalf("after the refusal NodeRemoved = %v (err %v), want false", removed, err)
	}
	intent := removeDataIntentFile(c.cfgs[f].NodeHostDir, removeDataShard, fid)
	if _, err := os.Stat(intent); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("the refused removal recorded an intent (stat err %v): %s", err, intent)
	}
}

// failParentSyncFS removes the armed directory for real, then fails the
// next Open of its parent once: the directory sync that makes the removal
// durable.
type failParentSyncFS struct {
	osSnapshotFS
	mu     sync.Mutex
	target string
	parent string
	fired  bool
}

var errInjectedParentSync = errors.New("injected parent directory sync failure")

func (f *failParentSyncFS) arm(path string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.target = filepath.Clean(path)
}

func (f *failParentSyncFS) RemoveAll(path string) error {
	if err := f.osSnapshotFS.RemoveAll(path); err != nil {
		return err
	}
	f.mu.Lock()
	if !f.fired && f.target != "" && filepath.Clean(path) == f.target {
		f.parent = filepath.Dir(f.target)
	}
	f.mu.Unlock()
	return nil
}

func (f *failParentSyncFS) Open(name string) (snapshotFile, error) {
	f.mu.Lock()
	fail := !f.fired && f.parent != "" && filepath.Clean(name) == f.parent
	if fail {
		f.fired = true
	}
	f.mu.Unlock()
	if fail {
		return nil, errInjectedParentSync
	}
	return f.osSnapshotFS.Open(name)
}

// TestSyncRemoveData_SyncFailureKeepsIntent: the snapshot removal is durable
// only once its parent directory is synced, so a failed sync returns the
// error and keeps the intent; a retry finishes and drops it.
func TestSyncRemoveData_SyncFailureKeepsIntent(t *testing.T) {
	fsys := &failParentSyncFS{}
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
	if err := fh.StopReplica(removeDataShard, fid); err != nil {
		t.Fatalf("StopReplica: %v", err)
	}

	fsys.arm(snapDir)
	if err := fh.RemoveData(removeDataShard, fid); !errors.Is(err, errInjectedParentSync) {
		t.Fatalf("RemoveData with a failing parent sync: err = %v, want %v",
			err, errInjectedParentSync)
	}
	intent := removeDataIntentFile(c.cfgs[f].NodeHostDir, removeDataShard, fid)
	if _, err := os.Stat(intent); err != nil {
		t.Fatalf("the unsynced snapshot removal dropped its intent %s: %v", intent, err)
	}

	if err := fh.RemoveData(removeDataShard, fid); err != nil {
		t.Fatalf("RemoveData retry: %v", err)
	}
	if _, err := os.Stat(intent); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("the retried removal left its intent (stat err %v): %s", err, intent)
	}
	if _, err := os.Stat(snapDir); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("the retried removal left the snapshot directory (stat err %v): %s", err, snapDir)
	}
}

// intentSyncFS records each directory sync (an Open of a watched
// directory) of the removal intent directory and of NodeHostDir, and fails
// the next sync of the intent directory once when armed.
type intentSyncFS struct {
	osSnapshotFS
	mu        sync.Mutex
	intentDir string
	hostDir   string
	failNext  bool
	events    []string
}

var errInjectedIntentDirSync = errors.New("injected removal intent directory sync failure")

func (f *intentSyncFS) watch(hostDir string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.hostDir = filepath.Clean(hostDir)
	f.intentDir = filepath.Join(f.hostDir, removeDataIntentDir)
	f.failNext = true
}

func (f *intentSyncFS) record(event string) {
	f.mu.Lock()
	f.events = append(f.events, event)
	f.mu.Unlock()
}

// take returns the recorded events and starts a new record.
func (f *intentSyncFS) take() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	ev := f.events
	f.events = nil
	return ev
}

func (f *intentSyncFS) Open(name string) (snapshotFile, error) {
	name = filepath.Clean(name)
	f.mu.Lock()
	var fail bool
	switch {
	case f.intentDir != "" && name == f.intentDir:
		f.events = append(f.events, "sync remove-data")
		fail = f.failNext
		f.failNext = false
	case f.hostDir != "" && name == f.hostDir:
		f.events = append(f.events, "sync NodeHostDir")
	}
	f.mu.Unlock()
	if fail {
		return nil, errInjectedIntentDirSync
	}
	return f.osSnapshotFS.Open(name)
}

// TestSyncRemoveData_IntentDirSyncFailureDoesNotRetire: a removal intent is
// durable only once the intent directory and NodeHostDir are synced, so a
// failed sync of the intent directory returns the error with the replica
// unretired and its snapshots kept; a retry from the pending intent syncs
// both directories before it retires the replica. "rewrite" retries with
// the bootstrap record still present, so the retry writes the intent again;
// "pending" hides the record, as after a crash past the retirement or for a
// replica joined before joins were recorded, so the retry reads the pending
// intent and skips the write.
func TestSyncRemoveData_IntentDirSyncFailureDoesNotRetire(t *testing.T) {
	t.Run("rewrite", func(t *testing.T) { testIntentDirSyncFailure(t, false) })
	t.Run("pending", func(t *testing.T) { testIntentDirSyncFailure(t, true) })
}

func testIntentDirSyncFailure(t *testing.T, pendingOnRetry bool) {
	const f = 1
	fid := uint64(f + 1)
	fsys := &intentSyncFS{}
	var w *removeDataLogDB
	c := newRemoveDataClusterWith(t, walWrapperFor(t, f, &w), withSnapshotFS(fsys))
	w.mu.Lock()
	w.onRetire = func() { fsys.record("retire") }
	w.mu.Unlock()
	dataDirs := make([]string, len(c.hosts))
	for i, h := range c.hosts {
		dataDirs[i] = t.TempDir()
		scfg := c.shardConfig(uint64(i + 1))
		scfg.DataDir = dataDirs[i]
		if err := h.StartShard(c.members(), false, hashingCreateFunc, scfg); err != nil {
			t.Fatalf("StartShard[%d]: %v", i, err)
		}
	}
	fh := c.hosts[f]
	snapDir := engine.ReplicaSnapshotDir(
		shardSnapshotBaseDir(c.cfgs[f].NodeHostDir, dataDirs[f], removeDataShard), removeDataShard, fid)
	if err := fh.StopReplica(removeDataShard, fid); err != nil {
		t.Fatalf("StopReplica: %v", err)
	}
	if _, err := os.Stat(snapDir); err != nil {
		t.Fatalf("snapshot directory before RemoveData: %v", err)
	}

	fsys.watch(c.cfgs[f].NodeHostDir)
	if err := fh.RemoveData(removeDataShard, fid); !errors.Is(err, errInjectedIntentDirSync) {
		t.Fatalf("RemoveData with a failing intent directory sync: err = %v, want %v",
			err, errInjectedIntentDirSync)
	}
	if removed, err := fh.logdb.NodeRemoved(removeDataShard, fid); err != nil || removed {
		t.Fatalf("after the failed intent sync NodeRemoved = %v (err %v), want false", removed, err)
	}
	if _, err := os.Stat(snapDir); err != nil {
		t.Fatalf("the failed intent sync removed the snapshot directory %s: %v", snapDir, err)
	}
	fsys.take()

	if pendingOnRetry {
		w.set(0, true)
		if _, pending, err := fh.readRemoveDataIntent(removeDataShard, fid); err != nil || !pending {
			t.Fatalf("after the failed intent sync pending = %v (err %v), want a pending intent", pending, err)
		}
	}
	if err := fh.RemoveData(removeDataShard, fid); err != nil {
		t.Fatalf("RemoveData retry: %v", err)
	}
	ev := fsys.take()
	var upToRetire []string
	for _, e := range ev {
		upToRetire = append(upToRetire, e)
		if e == "retire" {
			break
		}
	}
	want := []string{"sync remove-data", "sync NodeHostDir", "retire"}
	if len(upToRetire) != len(want) {
		t.Fatalf("retry events before the retirement = %v, want %v", upToRetire, want)
	}
	for i := range want {
		if upToRetire[i] != want[i] {
			t.Fatalf("retry events before the retirement = %v, want %v", upToRetire, want)
		}
	}
	if removed, err := fh.logdb.NodeRemoved(removeDataShard, fid); err != nil || !removed {
		t.Fatalf("after the retry NodeRemoved = %v (err %v), want true", removed, err)
	}
	if _, err := os.Stat(snapDir); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("the retried removal left the snapshot directory (stat err %v): %s", err, snapDir)
	}
}
