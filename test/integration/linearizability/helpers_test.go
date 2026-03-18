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

package linearizability_test

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
	"github.com/jeremyhahn/go-quicraft/pkg/sm"
)

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

// defaultTimeout is the standard timeout for linearizability test operations.
const defaultTimeout = 15 * time.Second

// leaderElectionTimeout is the time to wait for a leader election.
const leaderElectionTimeout = 5 * time.Second

// failoverTimeout is extended timeout for failover scenarios.
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
// sm.StateMachine for linearizability testing.
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
			return nil, &LookupTypeError{Type: query}
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
			return &SnapshotCancelledError{Op: "save"}
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
			return &SnapshotCancelledError{Op: "recover"}
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
// Typed Errors
// ---------------------------------------------------------------------------

// LookupTypeError is returned when Lookup receives an unsupported query type.
type LookupTypeError struct {
	Type interface{}
}

// Error returns a description of the unsupported query type.
func (e *LookupTypeError) Error() string {
	return "kvStateMachine: unsupported query type"
}

// SnapshotCancelledError is returned when a snapshot operation is cancelled.
type SnapshotCancelledError struct {
	Op string
}

// Error returns a description of the cancelled snapshot operation.
func (e *SnapshotCancelledError) Error() string {
	return "kvStateMachine: " + e.Op + " snapshot cancelled"
}

// ---------------------------------------------------------------------------
// State Machine Tracking
// ---------------------------------------------------------------------------

// smKey identifies a state machine by shard and replica.
type smKey struct {
	shardID   uint64
	replicaID uint64
}

// smTracker tracks kvStateMachine instances created by the factory.
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
// pair for integration test mTLS.
func testMTLSConfig(t *testing.T) *config.MTLSConfig {
	t.Helper()

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

// testHostConfig creates a valid HostConfig for linearizability testing.
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

// testShardConfig creates a per-shard config for linearizability testing.
func testShardConfig(shardID, replicaID uint64) config.Config {
	c := config.Config{
		ShardID:   shardID,
		ReplicaID: replicaID,
	}
	c.SetDefaults()
	return c
}

// freePort returns a free UDP port by binding and immediately closing a
// temporary listener.
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

// NodeNotStoppedError is returned when restartNode is called on a node
// that is not currently stopped.
type NodeNotStoppedError struct {
	Index int
}

// Error returns a description of the node restart precondition failure.
func (e *NodeNotStoppedError) Error() string {
	return fmt.Sprintf("restartNode: node[%d] is not stopped", e.Index)
}

// testCluster manages a group of Hosts for multi-node linearizability tests.
type testCluster struct {
	hostsMu  sync.RWMutex
	hosts    []*quicraft.Host
	addrs    []string
	configs  []config.HostConfig
	trackers []*smTracker
	stopped  []atomic.Bool
	mtls     *config.MTLSConfig
	t        *testing.T
}

// newTestCluster creates an N-node cluster with real QUIC transport.
func newTestCluster(t *testing.T, numNodes int) *testCluster {
	t.Helper()

	const maxRetries = 3
	mtls := testMTLSConfig(t)
	addrs := make([]string, numNodes)
	hosts := make([]*quicraft.Host, numNodes)
	configs := make([]config.HostConfig, numNodes)

	for i := range numNodes {
		var h *quicraft.Host
		var addr string
		var cfg config.HostConfig
		for attempt := range maxRetries {
			addr = freePort(t)
			cfg = testHostConfig(t, addr, mtls)
			var err error
			h, err = quicraft.NewHost(cfg)
			if err == nil {
				break
			}
			if attempt == maxRetries-1 {
				for j := 0; j < i; j++ {
					hosts[j].Close()
				}
				t.Fatalf("NewHost[%d] failed after %d attempts: %v", i, maxRetries, err)
			}
			t.Logf("NewHost[%d] at %s failed (attempt %d), retrying: %v", i, addr, attempt+1, err)
		}
		addrs[i] = addr
		hosts[i] = h
		configs[i] = cfg
	}

	return &testCluster{
		hosts:   hosts,
		addrs:   addrs,
		configs: configs,
		stopped: make([]atomic.Bool, numNodes),
		mtls:    mtls,
		t:       t,
	}
}

// members builds the members map from the cluster's address list.
// Replica IDs are 1-indexed (replicaID = nodeIndex + 1).
func (c *testCluster) members() map[uint64]string {
	m := make(map[uint64]string, len(c.addrs))
	for i, addr := range c.addrs {
		m[uint64(i+1)] = addr
	}
	return m
}

// startShard starts a shard on all nodes in the cluster.
func (c *testCluster) startShard(shardID uint64) []*smTracker {
	c.t.Helper()

	members := c.members()

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

// findLeader returns the index of the current leader for the shard, or -1.
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

// waitForClusterLeader waits until one node becomes leader for the shard.
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

// stopNode stops the host at the given index.
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

// restartNode recreates a stopped host with the same WAL/NodeHost directories,
// allowing it to recover state from its write-ahead log. The node is then
// re-joined to the existing shard.
func (c *testCluster) restartNode(idx int, shardID uint64) {
	c.t.Helper()

	if !c.stopped[idx].Load() {
		c.t.Fatalf("%v", &NodeNotStoppedError{Index: idx})
	}

	cfg := c.configs[idx]
	h, err := quicraft.NewHost(cfg)
	if err != nil {
		c.t.Fatalf("restartNode[%d]: NewHost failed: %v", idx, err)
	}

	createFn, tracker := kvCreateFunc()
	replicaID := uint64(idx + 1)
	scfg := testShardConfig(shardID, replicaID)

	if err := h.StartShard(c.members(), false, createFn, scfg); err != nil {
		h.Close()
		c.t.Fatalf("restartNode[%d]: StartShard shard=%d failed: %v", idx, shardID, err)
	}

	c.hostsMu.Lock()
	c.hosts[idx] = h
	c.trackers[idx] = tracker
	c.stopped[idx].Store(false)
	c.hostsMu.Unlock()
}

// startShardWithConfig starts a shard on all nodes using a custom config factory.
func (c *testCluster) startShardWithConfig(shardID uint64, cfgFn func(shardID, replicaID uint64) config.Config) []*smTracker {
	c.t.Helper()
	members := c.members()
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

// addNode dynamically adds a new node to the cluster for the given shard.
// The new node is registered with the leader via SyncRequestAddNode and
// starts the shard in join mode. Returns the index of the new node.
func (c *testCluster) addNode(shardID uint64) int {
	c.t.Helper()

	addr := freePort(c.t)
	cfg := testHostConfig(c.t, addr, c.mtls)
	h, err := quicraft.NewHost(cfg)
	if err != nil {
		c.t.Fatalf("addNode: NewHost failed: %v", err)
	}

	// Find the leader and request membership addition.
	leaderIdx := c.findLeader(shardID)
	if leaderIdx < 0 {
		h.Close()
		c.t.Fatal("addNode: no leader found for membership addition")
	}

	c.hostsMu.RLock()
	leaderHost := c.hosts[leaderIdx]
	c.hostsMu.RUnlock()

	newReplicaID := uint64(len(c.addrs) + 1)

	ctx, cancel := context.WithTimeout(context.Background(), defaultTimeout)
	defer cancel()

	if err := leaderHost.SyncRequestAddNode(ctx, shardID, newReplicaID, addr, 0); err != nil {
		h.Close()
		c.t.Fatalf("addNode: SyncRequestAddNode(replica=%d) failed: %v", newReplicaID, err)
	}

	// Start the shard in join mode on the new host.
	createFn, tracker := kvCreateFunc()
	scfg := testShardConfig(shardID, newReplicaID)
	if err := h.StartShard(nil, true, createFn, scfg); err != nil {
		h.Close()
		c.t.Fatalf("addNode: StartShard(join=true) replica=%d failed: %v", newReplicaID, err)
	}

	// Append to all cluster slices under write lock.
	c.hostsMu.Lock()
	idx := len(c.hosts)
	c.hosts = append(c.hosts, h)
	c.addrs = append(c.addrs, addr)
	c.configs = append(c.configs, cfg)
	c.trackers = append(c.trackers, tracker)
	c.stopped = append(c.stopped, atomic.Bool{})
	c.hostsMu.Unlock()

	return idx
}

// removeNode removes a node from the shard's membership via the leader,
// then stops the removed node. The leader must not be the node being removed.
func (c *testCluster) removeNode(idx int, shardID uint64) {
	c.t.Helper()

	leaderIdx := c.findLeader(shardID)
	if leaderIdx < 0 {
		c.t.Fatal("removeNode: no leader found")
	}
	if leaderIdx == idx {
		c.t.Fatalf("removeNode: cannot remove the leader (node[%d])", idx)
	}

	c.hostsMu.RLock()
	leaderHost := c.hosts[leaderIdx]
	c.hostsMu.RUnlock()

	replicaID := uint64(idx + 1)

	ctx, cancel := context.WithTimeout(context.Background(), defaultTimeout)
	defer cancel()

	if err := leaderHost.SyncRequestDeleteNode(ctx, shardID, replicaID, 0); err != nil {
		c.t.Fatalf("removeNode: SyncRequestDeleteNode(replica=%d) failed: %v", replicaID, err)
	}

	c.stopNode(idx)
}

// ---------------------------------------------------------------------------
// Wait Helpers
// ---------------------------------------------------------------------------

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
