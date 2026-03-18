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

package e2e_test

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/binary"
	"encoding/pem"
	"fmt"
	"io"
	"math/big"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	quicraft "github.com/jeremyhahn/go-quicraft/pkg"
	"github.com/jeremyhahn/go-quicraft/pkg/config"
	"github.com/jeremyhahn/go-quicraft/pkg/seal"
	"github.com/jeremyhahn/go-quicraft/pkg/sm"
)

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

// defaultTimeout is the standard timeout for E2E test operations.
// 15s gives headroom for QUIC connection establishment under
// Docker/CI resource contention.
const defaultTimeout = 15 * time.Second

// leaderElectionTimeout is the time to wait for a leader election.
// With RTT=50ms and ElectionRTT=10, election timeout is ~500ms.
// We give generous headroom for container/CI environments.
const leaderElectionTimeout = 5 * time.Second

// failoverTimeout is extended timeout for failover scenarios where
// the old leader must time out before a new election starts.
const failoverTimeout = 20 * time.Second

// ---------------------------------------------------------------------------
// KV Command Encoding
// ---------------------------------------------------------------------------

// Wire format: [op:1][keyLen:2 BigEndian][key][value]
const (
	opPut byte = 1
	opGet byte = 2
)

// encodeKVPut encodes a put command for the kvStateMachine.
func encodeKVPut(key, value string) []byte {
	keyBytes := []byte(key)
	valBytes := []byte(value)
	buf := make([]byte, 1+2+len(keyBytes)+len(valBytes))
	buf[0] = opPut
	binary.BigEndian.PutUint16(buf[1:3], uint16(len(keyBytes)))
	copy(buf[3:3+len(keyBytes)], keyBytes)
	copy(buf[3+len(keyBytes):], valBytes)
	return buf
}

// encodeKVGet encodes a get query key for kvStateMachine.Lookup.
func encodeKVGet(key string) []byte {
	return []byte(key)
}

// ---------------------------------------------------------------------------
// KV State Machine
// ---------------------------------------------------------------------------

// kvStateMachine is a simple in-memory key-value store that implements
// sm.StateMachine for E2E testing. It supports put and get operations
// and full snapshot save/recover.
type kvStateMachine struct {
	mu      sync.RWMutex
	data    map[string]string
	applied atomic.Uint64
}

func newKVStateMachine() *kvStateMachine {
	return &kvStateMachine{
		data: make(map[string]string),
	}
}

// Update applies committed entries to the KV store.
func (kv *kvStateMachine) Update(_ context.Context, entries []sm.Entry, results []sm.Result) error {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	for i, entry := range entries {
		if len(entry.Cmd) < 3 {
			results[i] = sm.Result{Value: 0}
			continue
		}
		op := entry.Cmd[0]
		keyLen := binary.BigEndian.Uint16(entry.Cmd[1:3])
		if int(keyLen)+3 > len(entry.Cmd) {
			results[i] = sm.Result{Value: 0}
			continue
		}
		key := string(entry.Cmd[3 : 3+keyLen])
		switch op {
		case opPut:
			value := string(entry.Cmd[3+keyLen:])
			kv.data[key] = value
			results[i] = sm.Result{Value: uint64(len(value))}
		default:
			results[i] = sm.Result{Value: 0}
		}
		kv.applied.Store(entry.Index)
	}
	return nil
}

// Lookup reads a value by key from the KV store.
func (kv *kvStateMachine) Lookup(_ context.Context, query interface{}) (interface{}, error) {
	key, ok := query.(string)
	if !ok {
		keyBytes, isByteSlice := query.([]byte)
		if !isByteSlice {
			return nil, fmt.Errorf("kvStateMachine: unsupported query type %T", query)
		}
		key = string(keyBytes)
	}
	kv.mu.RLock()
	defer kv.mu.RUnlock()
	val, exists := kv.data[key]
	if !exists {
		return "", nil
	}
	return val, nil
}

// SaveSnapshot serializes the entire KV store to the writer.
func (kv *kvStateMachine) SaveSnapshot(_ context.Context, w io.Writer, stopper <-chan struct{}) error {
	kv.mu.RLock()
	defer kv.mu.RUnlock()

	countBuf := make([]byte, 4)
	binary.BigEndian.PutUint32(countBuf, uint32(len(kv.data)))
	if _, err := w.Write(countBuf); err != nil {
		return err
	}

	for k, v := range kv.data {
		select {
		case <-stopper:
			return fmt.Errorf("kvStateMachine: snapshot cancelled")
		default:
		}
		keyBytes := []byte(k)
		valBytes := []byte(v)

		header := make([]byte, 6)
		binary.BigEndian.PutUint16(header[0:2], uint16(len(keyBytes)))
		binary.BigEndian.PutUint32(header[2:6], uint32(len(valBytes)))
		if _, err := w.Write(header); err != nil {
			return err
		}
		if _, err := w.Write(keyBytes); err != nil {
			return err
		}
		if _, err := w.Write(valBytes); err != nil {
			return err
		}
	}
	return nil
}

// RecoverFromSnapshot restores the KV store from a snapshot reader.
func (kv *kvStateMachine) RecoverFromSnapshot(_ context.Context, r io.Reader, stopper <-chan struct{}) error {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	countBuf := make([]byte, 4)
	if _, err := io.ReadFull(r, countBuf); err != nil {
		return err
	}
	count := binary.BigEndian.Uint32(countBuf)

	newData := make(map[string]string, count)
	for i := uint32(0); i < count; i++ {
		select {
		case <-stopper:
			return fmt.Errorf("kvStateMachine: recovery cancelled")
		default:
		}
		header := make([]byte, 6)
		if _, err := io.ReadFull(r, header); err != nil {
			return err
		}
		keyLen := binary.BigEndian.Uint16(header[0:2])
		valLen := binary.BigEndian.Uint32(header[2:6])

		keyBuf := make([]byte, keyLen)
		if _, err := io.ReadFull(r, keyBuf); err != nil {
			return err
		}
		valBuf := make([]byte, valLen)
		if _, err := io.ReadFull(r, valBuf); err != nil {
			return err
		}
		newData[string(keyBuf)] = string(valBuf)
	}
	kv.data = newData
	return nil
}

// Close is a no-op for the in-memory KV store.
func (kv *kvStateMachine) Close(_ context.Context) error {
	return nil
}

// LastApplied returns the index of the last applied entry.
func (kv *kvStateMachine) LastApplied() uint64 {
	return kv.applied.Load()
}

// Len returns the number of entries in the KV store.
func (kv *kvStateMachine) Len() int {
	kv.mu.RLock()
	defer kv.mu.RUnlock()
	return len(kv.data)
}

// Get returns the value for a key directly (for test assertions).
func (kv *kvStateMachine) Get(key string) (string, bool) {
	kv.mu.RLock()
	defer kv.mu.RUnlock()
	v, ok := kv.data[key]
	return v, ok
}

// Snapshot returns a copy of the KV data for test assertions.
func (kv *kvStateMachine) Snapshot() map[string]string {
	kv.mu.RLock()
	defer kv.mu.RUnlock()
	cp := make(map[string]string, len(kv.data))
	for k, v := range kv.data {
		cp[k] = v
	}
	return cp
}

// SnapshotBytes serializes the state machine to bytes for comparison.
func (kv *kvStateMachine) SnapshotBytes() ([]byte, error) {
	var buf bytes.Buffer
	stopper := make(chan struct{})
	if err := kv.SaveSnapshot(context.Background(), &buf, stopper); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// Compile-time assertion.
var _ sm.StateMachine = (*kvStateMachine)(nil)

// ---------------------------------------------------------------------------
// State Machine Tracking
// ---------------------------------------------------------------------------

// smKey identifies a state machine by shard and replica.
type smKey struct {
	shardID   uint64
	replicaID uint64
}

// smTracker tracks kvStateMachine instances created by the factory
// so tests can assert on internal state.
type smTracker struct {
	mu       sync.Mutex
	machines map[smKey]*kvStateMachine
}

func (t *smTracker) Get(shardID, replicaID uint64) *kvStateMachine {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.machines[smKey{shardID, replicaID}]
}

// kvCreateFunc returns a sm.CreateFunc that produces kvStateMachine instances.
// The returned smTracker provides access to the underlying state machines
// for test assertions.
func kvCreateFunc() (sm.CreateFunc, *smTracker) {
	tracker := &smTracker{
		machines: make(map[smKey]*kvStateMachine),
	}
	createFn := func(shardID, replicaID uint64) interface{} {
		machine := newKVStateMachine()
		tracker.mu.Lock()
		tracker.machines[smKey{shardID, replicaID}] = machine
		tracker.mu.Unlock()
		return machine
	}
	return createFn, tracker
}

// ---------------------------------------------------------------------------
// Host Construction Helpers
// ---------------------------------------------------------------------------

// testMTLSConfig generates an ephemeral ECDSA P-256 CA and leaf certificate
// pair for integration test mTLS. All nodes in a cluster must share the same
// return value so they can verify each other's certificates.
func testMTLSConfig(t *testing.T) *config.MTLSConfig {
	t.Helper()

	// Generate CA key pair.
	caKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generate CA key: %v", err)
	}

	caTemplate := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject: pkix.Name{
			Organization: []string{"QuicRaft Test"},
		},
		NotBefore:             time.Now().Add(-1 * time.Minute),
		NotAfter:              time.Now().Add(1 * time.Hour),
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageCRLSign,
		BasicConstraintsValid: true,
		IsCA:                  true,
	}

	caCertDER, err := x509.CreateCertificate(rand.Reader, caTemplate, caTemplate, &caKey.PublicKey, caKey)
	if err != nil {
		t.Fatalf("create CA cert: %v", err)
	}

	caCert, err := x509.ParseCertificate(caCertDER)
	if err != nil {
		t.Fatalf("parse CA cert: %v", err)
	}

	caCertPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: caCertDER})

	// Generate leaf key pair signed by the CA.
	leafKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generate leaf key: %v", err)
	}

	leafTemplate := &x509.Certificate{
		SerialNumber: big.NewInt(2),
		Subject: pkix.Name{
			Organization: []string{"QuicRaft Test"},
			CommonName:   "localhost",
		},
		DNSNames:              []string{"localhost"},
		IPAddresses:           []net.IP{net.IPv4(127, 0, 0, 1), net.IPv6loopback, net.IPv6zero},
		NotBefore:             time.Now().Add(-1 * time.Minute),
		NotAfter:              time.Now().Add(1 * time.Hour),
		KeyUsage:              x509.KeyUsageDigitalSignature,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
		BasicConstraintsValid: true,
	}

	leafCertDER, err := x509.CreateCertificate(rand.Reader, leafTemplate, caCert, &leafKey.PublicKey, caKey)
	if err != nil {
		t.Fatalf("create leaf cert: %v", err)
	}

	leafCertPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: leafCertDER})
	leafKeyDER, err := x509.MarshalECPrivateKey(leafKey)
	if err != nil {
		t.Fatalf("marshal leaf key: %v", err)
	}
	leafKeyPEM := pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: leafKeyDER})

	return &config.MTLSConfig{
		CACert: caCertPEM,
		Cert:   leafCertPEM,
		Key:    leafKeyPEM,
	}
}

// testHostConfig creates a valid HostConfig for E2E testing. Each call
// gets unique temp directories via t.TempDir(). The mtls parameter
// configures mandatory mTLS for the QUIC transport; all nodes in a
// cluster must share the same CA certificate.
func testHostConfig(t *testing.T, listenAddr string, mtls *config.MTLSConfig) config.HostConfig {
	t.Helper()
	return config.HostConfig{
		WALDir:                t.TempDir(),
		NodeHostDir:           t.TempDir(),
		RaftAddress:           listenAddr,
		ListenAddress:         listenAddr,
		RTTMillisecond:        50,
		DeploymentID:          1,
		AllowZeroDeploymentID: false,
		NumWorkers:            1,
		MaxApplyWorkers:       1,
		TransportConfig: config.TransportConfig{
			MTLSConfig: mtls,
		},
	}
}

// testShardConfig creates a per-shard config for E2E testing.
func testShardConfig(shardID, replicaID uint64) config.Config {
	c := config.Config{
		ShardID:   shardID,
		ReplicaID: replicaID,
	}
	c.SetDefaults()
	return c
}

// freePort returns a free UDP port by binding and immediately closing a
// temporary listener. The OS assigns an ephemeral port.
func freePort(t *testing.T) string {
	t.Helper()
	pc, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to find free port: %v", err)
	}
	addr := pc.LocalAddr().String()
	pc.Close()
	return addr
}

// ---------------------------------------------------------------------------
// Test Cluster
// ---------------------------------------------------------------------------

// testCluster manages a group of Hosts for multi-node E2E tests. Each
// Host runs on a separate listen address with real QUIC transport.
type testCluster struct {
	hostsMu  sync.RWMutex
	hosts    []*quicraft.Host
	addrs    []string
	trackers []*smTracker
	stopped  []atomic.Bool // tracks which nodes have been stopped
	mtls     *config.MTLSConfig
	t        *testing.T
}

// newTestCluster creates an N-node cluster with real QUIC transport.
// Each host gets a unique listen address on the loopback interface.
// The cluster is not started (no shards); call startShard to begin.
// Host creation retries with a fresh port on bind failures (TOCTOU
// between freePort and NewHost).
func newTestCluster(t *testing.T, numNodes int) *testCluster {
	t.Helper()

	const maxRetries = 3
	mtls := testMTLSConfig(t)
	addrs := make([]string, numNodes)
	hosts := make([]*quicraft.Host, numNodes)

	for i := range numNodes {
		var h *quicraft.Host
		var addr string
		for attempt := range maxRetries {
			addr = freePort(t)
			cfg := testHostConfig(t, addr, mtls)
			var err error
			h, err = quicraft.NewHost(cfg)
			if err == nil {
				break
			}
			if attempt == maxRetries-1 {
				// Clean up any previously created hosts.
				for j := 0; j < i; j++ {
					hosts[j].Close()
				}
				t.Fatalf("NewHost[%d] failed after %d attempts: %v", i, maxRetries, err)
			}
			t.Logf("NewHost[%d] at %s failed (attempt %d), retrying: %v", i, addr, attempt+1, err)
		}
		addrs[i] = addr
		hosts[i] = h
	}

	return &testCluster{
		hosts:   hosts,
		addrs:   addrs,
		stopped: make([]atomic.Bool, numNodes),
		mtls:    mtls,
		t:       t,
	}
}

// startShard starts a shard on all nodes in the cluster. Returns the
// smTrackers for each node so tests can inspect state machine state.
func (c *testCluster) startShard(shardID uint64) []*smTracker {
	c.t.Helper()

	members := make(map[uint64]string, len(c.hosts))
	for i, addr := range c.addrs {
		replicaID := uint64(i + 1)
		members[replicaID] = addr
	}

	trackers := make([]*smTracker, len(c.hosts))
	for i, h := range c.hosts {
		if c.stopped[i].Load() {
			continue
		}
		replicaID := uint64(i + 1)
		createFn, tracker := kvCreateFunc()
		trackers[i] = tracker

		scfg := testShardConfig(shardID, replicaID)
		if err := h.StartShard(members, false, createFn, scfg); err != nil {
			c.t.Fatalf("StartShard[%d] shard=%d failed: %v", i, shardID, err)
		}
	}

	c.trackers = trackers
	return trackers
}

// close shuts down all hosts in the cluster.
func (c *testCluster) close() {
	c.hostsMu.Lock()
	defer c.hostsMu.Unlock()
	for i, h := range c.hosts {
		if !c.stopped[i].Load() {
			h.Close()
		}
	}
}

// findLeader returns the index of the node that is the current leader
// for the given shard, or -1 if no leader is known.
func (c *testCluster) findLeader(shardID uint64) int {
	c.hostsMu.RLock()
	defer c.hostsMu.RUnlock()
	for i, h := range c.hosts {
		if c.stopped[i].Load() {
			continue
		}
		info, err := h.GetShardInfo(shardID)
		if err != nil {
			continue
		}
		if info.IsLeader {
			return i
		}
	}
	return -1
}

// findFollower returns the index of a follower node for the given shard,
// or -1 if no follower is found.
func (c *testCluster) findFollower(shardID uint64) int {
	leaderIdx := c.findLeader(shardID)
	c.hostsMu.RLock()
	defer c.hostsMu.RUnlock()
	for i := range c.hosts {
		if c.stopped[i].Load() || i == leaderIdx {
			continue
		}
		return i
	}
	return -1
}

// waitForClusterLeader waits until one of the nodes in the cluster
// becomes leader for the given shard.
func (c *testCluster) waitForClusterLeader(shardID uint64, timeout time.Duration) int {
	c.t.Helper()
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	deadline := time.NewTimer(timeout)
	defer deadline.Stop()
	for {
		idx := c.findLeader(shardID)
		if idx >= 0 {
			return idx
		}
		select {
		case <-deadline.C:
			c.t.Fatalf("timed out after %v waiting for cluster leader on shard %d", timeout, shardID)
			return -1
		case <-ticker.C:
		}
	}
}

// stopNode stops the host at the given index. Subsequent calls to
// findLeader, findFollower, and close skip stopped nodes.
func (c *testCluster) stopNode(idx int) {
	c.t.Helper()
	if c.stopped[idx].Load() {
		return
	}
	c.hostsMu.Lock()
	c.hosts[idx].Close()
	c.stopped[idx].Store(true)
	c.hostsMu.Unlock()
}

// leaderHost returns the Host instance for the leader of the given shard.
func (c *testCluster) leaderHost(shardID uint64) *quicraft.Host {
	c.hostsMu.RLock()
	defer c.hostsMu.RUnlock()
	for i, h := range c.hosts {
		if c.stopped[i].Load() {
			continue
		}
		info, err := h.GetShardInfo(shardID)
		if err != nil {
			continue
		}
		if info.IsLeader {
			return h
		}
	}
	return nil
}

// liveNodes returns the count of nodes that are not stopped.
func (c *testCluster) liveNodes() int {
	count := 0
	for i := range c.stopped {
		if !c.stopped[i].Load() {
			count++
		}
	}
	return count
}

// ---------------------------------------------------------------------------
// Wait Helpers
// ---------------------------------------------------------------------------

// waitForLeader polls until the host for the shard elects a leader.
func waitForLeader(t *testing.T, h *quicraft.Host, shardID uint64, timeout time.Duration) {
	t.Helper()
	ticker := time.NewTicker(5 * time.Millisecond)
	defer ticker.Stop()
	deadline := time.NewTimer(timeout)
	defer deadline.Stop()
	for {
		leaderID, known, err := h.GetLeaderID(shardID)
		if err == nil && known && leaderID > 0 {
			return
		}
		select {
		case <-deadline.C:
			t.Fatalf("timed out after %v waiting for leader on shard %d", timeout, shardID)
			return
		case <-ticker.C:
		}
	}
}

// waitForApplied polls the host until the shard's last applied index
// reaches or exceeds the target index.
func waitForApplied(t *testing.T, h *quicraft.Host, shardID, targetIndex uint64, timeout time.Duration) {
	t.Helper()
	ticker := time.NewTicker(5 * time.Millisecond)
	defer ticker.Stop()
	deadline := time.NewTimer(timeout)
	defer deadline.Stop()
	for {
		info, err := h.GetShardInfo(shardID)
		if err == nil && info.LastApplied >= targetIndex {
			return
		}
		select {
		case <-deadline.C:
			t.Fatalf("timed out after %v waiting for shard %d to apply index %d", timeout, shardID, targetIndex)
			return
		case <-ticker.C:
		}
	}
}

// waitForCondition polls until the predicate returns true or the timeout
// expires.
func waitForCondition(t *testing.T, timeout time.Duration, desc string, predicate func() bool) {
	t.Helper()
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	deadline := time.NewTimer(timeout)
	defer deadline.Stop()
	for {
		if predicate() {
			return
		}
		select {
		case <-deadline.C:
			t.Fatalf("timed out after %v waiting for condition: %s", timeout, desc)
			return
		case <-ticker.C:
		}
	}
}

// waitForReplication waits for a key to appear in all live nodes' state
// machines with the expected value.
func waitForReplication(t *testing.T, cluster *testCluster, trackers []*smTracker, shardID uint64, key, expectedVal string, timeout time.Duration) {
	t.Helper()
	for i := range cluster.hosts {
		if cluster.stopped[i].Load() {
			continue
		}
		if i >= len(trackers) {
			continue
		}
		replicaID := uint64(i + 1)
		sm := trackers[i].Get(shardID, replicaID)
		if sm == nil {
			continue
		}
		waitForCondition(t, timeout,
			fmt.Sprintf("node[%d] replicates key=%q", i, key),
			func() bool {
				val, ok := sm.Get(key)
				return ok && val == expectedVal
			},
		)
	}
}

// proposeAndWait proposes a key-value pair through the leader and waits
// for it to be applied on the leader's state machine.
func proposeAndWait(t *testing.T, cluster *testCluster, trackers []*smTracker, shardID uint64, key, value string) {
	t.Helper()

	cmd := encodeKVPut(key, value)
	retryTicker := time.NewTicker(50 * time.Millisecond)
	defer retryTicker.Stop()
	overallDeadline := time.NewTimer(defaultTimeout)
	defer overallDeadline.Stop()

	for {
		leaderIdx := cluster.findLeader(shardID)
		if leaderIdx < 0 {
			select {
			case <-overallDeadline.C:
				t.Fatalf("timed out after %v waiting to propose key=%q: no leader", defaultTimeout, key)
				return
			case <-retryTicker.C:
			}
			continue
		}
		leaderHost := cluster.hosts[leaderIdx]

		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		rs, err := leaderHost.Propose(ctx, shardID, cmd)
		if err != nil {
			cancel()
			if isTransientLeadershipError(err) {
				select {
				case <-overallDeadline.C:
					t.Fatalf("timed out after %v waiting to propose key=%q: %v", defaultTimeout, key, err)
					return
				case <-retryTicker.C:
				}
				continue
			}
			t.Fatalf("Propose(%q=%q) failed: %v", key, value, err)
		}

		// Wait for the proposal to be committed and applied.
		_, resErr := rs.Result()
		rs.Release()
		cancel()

		if resErr != nil {
			if isTransientLeadershipError(resErr) {
				select {
				case <-overallDeadline.C:
					t.Fatalf("timed out after %v waiting for proposal result key=%q: %v", defaultTimeout, key, resErr)
					return
				case <-retryTicker.C:
				}
				continue
			}
			t.Fatalf("Propose(%q=%q) result failed: %v", key, value, resErr)
		}

		// Proposal committed. Wait for the leader's SM to apply.
		replicaID := uint64(leaderIdx + 1)
		var leaderTracker *smTracker
		if leaderIdx < len(trackers) {
			leaderTracker = trackers[leaderIdx]
		} else if leaderIdx < len(cluster.trackers) {
			leaderTracker = cluster.trackers[leaderIdx]
		}
		if leaderTracker == nil {
			t.Fatalf("no tracker for leader node[%d]", leaderIdx)
		}
		leaderSM := leaderTracker.Get(shardID, replicaID)
		if leaderSM == nil {
			t.Fatal("leader state machine not found")
		}
		waitForCondition(t, defaultTimeout,
			fmt.Sprintf("leader applies key=%q", key),
			func() bool {
				val, ok := leaderSM.Get(key)
				return ok && val == value
			},
		)
		return
	}
}

// ---------------------------------------------------------------------------
// Membership / Snapshot / Barrier Helpers
// ---------------------------------------------------------------------------

// membershipTimeout is the generous timeout for config change operations
// that may require leader round-trips, log replication, and QUIC setup.
const membershipTimeout = 30 * time.Second

// testShardConfigWithSnapshot creates a per-shard config with aggressive
// snapshot settings to force log compaction for testing.
func testShardConfigWithSnapshot(shardID, replicaID, snapshotEntries, compactionOverhead uint64) config.Config {
	c := config.Config{
		ShardID:            shardID,
		ReplicaID:          replicaID,
		SnapshotEntries:    snapshotEntries,
		CompactionOverhead: compactionOverhead,
	}
	c.SetDefaults()
	return c
}

// startShardWithConfig starts a shard on all nodes in the cluster using
// a custom config factory. Returns smTrackers for each node.
func (c *testCluster) startShardWithConfig(shardID uint64, cfgFn func(shardID, replicaID uint64) config.Config) []*smTracker {
	c.t.Helper()

	members := make(map[uint64]string, len(c.hosts))
	for i, addr := range c.addrs {
		replicaID := uint64(i + 1)
		members[replicaID] = addr
	}

	trackers := make([]*smTracker, len(c.hosts))
	for i, h := range c.hosts {
		if c.stopped[i].Load() {
			continue
		}
		replicaID := uint64(i + 1)
		createFn, tracker := kvCreateFunc()
		trackers[i] = tracker

		scfg := cfgFn(shardID, replicaID)
		if err := h.StartShard(members, false, createFn, scfg); err != nil {
			c.t.Fatalf("StartShard[%d] shard=%d failed: %v", i, shardID, err)
		}
	}

	c.trackers = trackers
	return trackers
}

// addNodeResult holds the resources for a dynamically added node.
type addNodeResult struct {
	host    *quicraft.Host
	addr    string
	tracker *smTracker
	idx     int
}

// addNodeToCluster creates a new host, requests a membership addition from
// the leader, and starts the shard in join mode. The new host is appended
// to the cluster's hosts/addrs/trackers/stopped slices.
func addNodeToCluster(
	t *testing.T,
	cluster *testCluster,
	shardID, newReplicaID uint64,
	cfgFn func(shardID, replicaID uint64) config.Config,
	opts ...quicraft.HostOption,
) addNodeResult {
	t.Helper()

	// Create a new host on a fresh port, reusing the cluster's mTLS config
	// so the new node can verify and be verified by existing peers.
	addr := freePort(t)
	hostCfg := testHostConfig(t, addr, cluster.mtls)
	h, err := quicraft.NewHost(hostCfg, opts...)
	if err != nil {
		t.Fatalf("NewHost for new node failed: %v", err)
	}

	// Request membership addition from the leader.
	leaderIdx := cluster.findLeader(shardID)
	if leaderIdx < 0 {
		h.Close()
		t.Fatal("no leader found for membership addition")
	}

	ctx, cancel := context.WithTimeout(context.Background(), membershipTimeout)
	defer cancel()

	if err := cluster.hosts[leaderIdx].SyncRequestAddNode(ctx, shardID, newReplicaID, addr, 0); err != nil {
		h.Close()
		t.Fatalf("SyncRequestAddNode(replica=%d) failed: %v", newReplicaID, err)
	}

	// Start the shard in join mode on the new host.
	createFn, tracker := kvCreateFunc()
	scfg := cfgFn(shardID, newReplicaID)
	if err := h.StartShard(nil, true, createFn, scfg); err != nil {
		h.Close()
		t.Fatalf("StartShard(join=true) for replica=%d failed: %v", newReplicaID, err)
	}

	// Append to cluster tracking slices.
	cluster.hostsMu.Lock()
	idx := len(cluster.hosts)
	cluster.hosts = append(cluster.hosts, h)
	cluster.addrs = append(cluster.addrs, addr)
	cluster.trackers = append(cluster.trackers, tracker)
	cluster.stopped = append(cluster.stopped, atomic.Bool{})
	cluster.hostsMu.Unlock()

	return addNodeResult{
		host:    h,
		addr:    addr,
		tracker: tracker,
		idx:     idx,
	}
}

// proposeNEntries proposes N entries through the leader and returns the
// expected key-value map. Keys are formatted as "<prefix>-NNN".
func proposeNEntries(
	t *testing.T,
	cluster *testCluster,
	trackers []*smTracker,
	shardID uint64,
	n int,
	keyPrefix string,
) map[string]string {
	t.Helper()

	expected := make(map[string]string, n)
	for i := range n {
		key := fmt.Sprintf("%s-%03d", keyPrefix, i)
		value := fmt.Sprintf("val-%s-%03d", keyPrefix, i)
		proposeAndWait(t, cluster, trackers, shardID, key, value)
		expected[key] = value
	}
	return expected
}

// verifyAllData verifies all KV pairs from expected are present on all
// live nodes' state machines.
func verifyAllData(
	t *testing.T,
	cluster *testCluster,
	trackers []*smTracker,
	shardID uint64,
	expected map[string]string,
) {
	t.Helper()

	for i := range cluster.hosts {
		if cluster.stopped[i].Load() || i >= len(trackers) {
			continue
		}
		replicaID := uint64(i + 1)
		nodeSM := trackers[i].Get(shardID, replicaID)
		if nodeSM == nil {
			continue
		}
		for key, val := range expected {
			waitForCondition(t, membershipTimeout,
				fmt.Sprintf("node[%d] has key=%q", i, key),
				func() bool {
					got, ok := nodeSM.Get(key)
					return ok && got == val
				},
			)
		}
	}
}

// waitForMembershipSize polls GetShardMembership until the number of
// voting nodes matches expectedSize.
func waitForMembershipSize(
	t *testing.T,
	host *quicraft.Host,
	shardID uint64,
	expectedSize int,
	timeout time.Duration,
) {
	t.Helper()
	waitForCondition(t, timeout,
		fmt.Sprintf("membership size=%d", expectedSize),
		func() bool {
			m, err := host.GetShardMembership(shardID)
			if err != nil {
				return false
			}
			return len(m.Nodes) == expectedSize
		},
	)
}

// waitForNodeData waits for a specific node's SM to contain all expected data.
func waitForNodeData(
	t *testing.T,
	tracker *smTracker,
	shardID, replicaID uint64,
	expected map[string]string,
	timeout time.Duration,
) {
	t.Helper()
	nodeSM := tracker.Get(shardID, replicaID)
	if nodeSM == nil {
		// SM may not be registered yet; poll until it appears.
		waitForCondition(t, timeout,
			fmt.Sprintf("replica=%d SM registered", replicaID),
			func() bool {
				nodeSM = tracker.Get(shardID, replicaID)
				return nodeSM != nil
			},
		)
	}
	for key, val := range expected {
		waitForCondition(t, timeout,
			fmt.Sprintf("replica=%d has key=%q", replicaID, key),
			func() bool {
				got, ok := nodeSM.Get(key)
				return ok && got == val
			},
		)
	}
}

// ---------------------------------------------------------------------------
// Barrier Test Cluster
// ---------------------------------------------------------------------------

// barrierTestCluster wraps testCluster with per-host barriers for
// testing at-rest encryption E2E.
type barrierTestCluster struct {
	*testCluster
	barriers []*seal.Barrier
}

// newBarrierTestCluster creates an N-node cluster where each host has
// its own independently initialized and unsealed barrier. Uses fast
// Argon2id parameters (time=1, memory=64KB, threads=1) for test speed.
func newBarrierTestCluster(t *testing.T, numNodes int) *barrierTestCluster {
	t.Helper()

	const maxRetries = 3
	mtls := testMTLSConfig(t)
	addrs := make([]string, numNodes)
	hosts := make([]*quicraft.Host, numNodes)
	barriers := make([]*seal.Barrier, numNodes)

	for i := range numNodes {
		// Create and initialize a barrier for this node.
		b := seal.NewBarrier(seal.DefaultBarrierConfig(1))
		s, err := seal.NewSoftwareStrategyWithParams(
			[]byte(fmt.Sprintf("test-passphrase-%d", i)), 1, 64*1024, 1)
		if err != nil {
			// Clean up previously created hosts.
			for j := 0; j < i; j++ {
				hosts[j].Close()
			}
			t.Fatalf("NewSoftwareStrategyWithParams[%d]: %v", i, err)
		}
		if err := b.Initialize(context.Background(), s, seal.Credentials{}); err != nil {
			for j := 0; j < i; j++ {
				hosts[j].Close()
			}
			t.Fatalf("barrier.Initialize[%d]: %v", i, err)
		}
		barriers[i] = b

		var h *quicraft.Host
		var addr string
		for attempt := range maxRetries {
			addr = freePort(t)
			cfg := testHostConfig(t, addr, mtls)
			h, err = quicraft.NewHost(cfg, quicraft.WithBarrier(b))
			if err == nil {
				break
			}
			if attempt == maxRetries-1 {
				for j := 0; j < i; j++ {
					hosts[j].Close()
				}
				t.Fatalf("NewHost[%d] with barrier failed after %d attempts: %v", i, maxRetries, err)
			}
			t.Logf("NewHost[%d] at %s failed (attempt %d), retrying: %v", i, addr, attempt+1, err)
		}
		addrs[i] = addr
		hosts[i] = h
	}

	tc := &testCluster{
		hosts:   hosts,
		addrs:   addrs,
		stopped: make([]atomic.Bool, numNodes),
		mtls:    mtls,
		t:       t,
	}
	return &barrierTestCluster{
		testCluster: tc,
		barriers:    barriers,
	}
}

// newBarrierForNode creates a fresh initialized+unsealed barrier suitable
// for a replacement node in barrier cluster tests.
func newBarrierForNode(t *testing.T) *seal.Barrier {
	t.Helper()
	b := seal.NewBarrier(seal.DefaultBarrierConfig(1))
	s, err := seal.NewSoftwareStrategyWithParams(
		[]byte("replacement-passphrase"), 1, 64*1024, 1)
	if err != nil {
		t.Fatalf("NewSoftwareStrategyWithParams for replacement: %v", err)
	}
	if err := b.Initialize(context.Background(), s, seal.Credentials{}); err != nil {
		t.Fatalf("barrier.Initialize for replacement: %v", err)
	}
	return b
}
