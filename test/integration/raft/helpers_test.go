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

package raft_test

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
	"errors"
	"fmt"
	"io"
	"io/fs"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	quicraft "github.com/jeremyhahn/go-quicraft/pkg"
	"github.com/jeremyhahn/go-quicraft/pkg/config"
	"github.com/jeremyhahn/go-quicraft/pkg/sm"
	"github.com/jeremyhahn/go-quicraft/test/proposer"
)

// ---------------------------------------------------------------------------
// Test State Machine: KV Store
// ---------------------------------------------------------------------------

// kvCommand is the wire format for a key-value command. The first byte is
// the operation type, followed by a 2-byte key length, the key bytes, and
// optionally the value bytes (for put operations).
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
// Apply-Order Violations
// ---------------------------------------------------------------------------

// applyOrderViolations collects apply-order invariant breaches observed by any
// kvStateMachine in the package. The state machines are driven by engine
// goroutines that have no *testing.T, so a breach is parked here and surfaced
// by the t.Cleanup that assertApplyOrder installs. Integration tests in this
// package never run in parallel, so one package-level sink is unambiguous.
var applyOrderViolations struct {
	mu      sync.Mutex
	list    []string
	armedBy *testing.T
}

// recordApplyViolation parks an apply-order breach for the running test.
func recordApplyViolation(format string, args ...interface{}) {
	msg := fmt.Sprintf(format, args...)
	applyOrderViolations.mu.Lock()
	applyOrderViolations.list = append(applyOrderViolations.list, msg)
	applyOrderViolations.mu.Unlock()
}

// assertApplyOrder arms the violation sink for t and installs a t.Cleanup that
// fails the test if any state machine observed a duplicate, backward or
// replayed apply while the test was running. It is idempotent: arming the
// same test twice must not discard breaches already recorded for it.
func assertApplyOrder(t *testing.T) {
	t.Helper()

	applyOrderViolations.mu.Lock()
	if applyOrderViolations.armedBy == t {
		applyOrderViolations.mu.Unlock()
		return
	}
	applyOrderViolations.armedBy = t
	applyOrderViolations.list = nil
	applyOrderViolations.mu.Unlock()

	t.Cleanup(func() {
		applyOrderViolations.mu.Lock()
		found := applyOrderViolations.list
		applyOrderViolations.list = nil
		applyOrderViolations.armedBy = nil
		applyOrderViolations.mu.Unlock()
		if len(found) == 0 {
			return
		}
		shown := found
		if len(shown) > 20 {
			shown = shown[:20]
		}
		t.Errorf("state machine apply-order violations (%d total):\n  %s",
			len(found), strings.Join(shown, "\n  "))
	})
}

// ---------------------------------------------------------------------------
// KV State Machine
// ---------------------------------------------------------------------------

// kvStateMachine is a simple in-memory key-value store that implements
// StateMachine for integration testing. It supports put and get
// operations and full snapshot save/recover.
type kvStateMachine struct {
	mu   sync.RWMutex
	data map[string]string
	// prevIndex is the raft index of the previously applied entry. Every
	// entry handed to Update must carry a STRICTLY greater index: an index
	// that repeats or moves backward means the entry was applied twice or
	// replayed, which no amount of state comparison can detect afterwards on
	// a last-write-wins store. RecoverFromSnapshot is the only legal reset
	// point.
	//
	// Note: consecutive indices are NOT required. The user state machine only
	// ever sees user commands; the engine legitimately consumes indices for
	// the initial-membership entry, each leader's no-op entry, config changes
	// and session register/unregister entries, so gaps are expected and are
	// counted rather than failed. See applyGaps.
	prevIndex uint64
	// applyGaps counts skipped index ranges, for diagnostics only.
	applyGaps atomic.Uint64
	applied   atomic.Uint64
}

// checkIndexLocked enforces strict forward progress of the applied index and
// records a violation if the entry repeats or rolls back. kv.mu must be held.
func (kv *kvStateMachine) checkIndexLocked(index uint64) {
	switch {
	case index <= kv.prevIndex:
		recordApplyViolation(
			"kvStateMachine: applied index did not advance: entry index %d applied after %d (duplicate, reordered or replayed apply)",
			index, kv.prevIndex)
	case index > kv.prevIndex+1:
		kv.applyGaps.Add(1)
	}
	kv.prevIndex = index
	kv.applied.Store(index)
}

// ApplyGaps returns the number of skipped index ranges observed so far.
func (kv *kvStateMachine) ApplyGaps() uint64 {
	return kv.applyGaps.Load()
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
		// The index invariant holds for every entry the state machine is
		// handed, including malformed ones, so it is checked before any
		// command-shape early exit.
		kv.checkIndexLocked(entry.Index)

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

	// The applied index is written ahead of the payload so a recovering
	// replica can tell whether the snapshot moves it forward or rolls it back.
	indexBuf := make([]byte, 8)
	binary.BigEndian.PutUint64(indexBuf, kv.prevIndex)
	if _, err := w.Write(indexBuf); err != nil {
		return err
	}

	// Format: [applied:uint64][count:uint32][key_len:uint16][key][val_len:uint32][val]...
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

	// This is the only point at which the applied-index expectation may be
	// reset, and the reset must still move the replica FORWARD: installing a
	// snapshot older than what this replica already applied would silently
	// roll the state machine back.
	indexBuf := make([]byte, 8)
	if _, err := io.ReadFull(r, indexBuf); err != nil {
		return err
	}
	snapshotIndex := binary.BigEndian.Uint64(indexBuf)
	if snapshotIndex < kv.prevIndex {
		recordApplyViolation(
			"kvStateMachine: snapshot rolled the state machine backward: recovered at index %d, already applied through %d",
			snapshotIndex, kv.prevIndex)
	}
	kv.prevIndex = snapshotIndex
	kv.applied.Store(snapshotIndex)

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
// Counter State Machine (for stress tests)
// ---------------------------------------------------------------------------

// counterStateMachine is a simple counter that increments for every
// applied entry. Used for stress tests where we only care about
// counting applied entries, not their content.
type counterStateMachine struct {
	mu      sync.Mutex
	count   atomic.Int64
	applied atomic.Uint64
	// prevIndex carries the same strict-advance invariant as
	// kvStateMachine.prevIndex: a counter that is incremented twice for one
	// entry is otherwise indistinguishable from one that saw two entries.
	prevIndex uint64
}

func (c *counterStateMachine) Update(_ context.Context, entries []sm.Entry, results []sm.Result) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	for i, entry := range entries {
		if entry.Index <= c.prevIndex {
			recordApplyViolation(
				"counterStateMachine: applied index did not advance: entry index %d applied after %d (duplicate, reordered or replayed apply)",
				entry.Index, c.prevIndex)
		}
		c.prevIndex = entry.Index
		c.count.Add(1)
		results[i] = sm.Result{Value: uint64(c.count.Load())}
		c.applied.Store(entry.Index)
	}
	return nil
}

func (c *counterStateMachine) Lookup(_ context.Context, query interface{}) (interface{}, error) {
	return c.count.Load(), nil
}

func (c *counterStateMachine) SaveSnapshot(_ context.Context, w io.Writer, _ <-chan struct{}) error {
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, uint64(c.count.Load()))
	_, err := w.Write(buf)
	return err
}

func (c *counterStateMachine) RecoverFromSnapshot(_ context.Context, r io.Reader, _ <-chan struct{}) error {
	buf := make([]byte, 8)
	if _, err := io.ReadFull(r, buf); err != nil {
		return err
	}
	c.count.Store(int64(binary.BigEndian.Uint64(buf)))
	return nil
}

func (c *counterStateMachine) Close(_ context.Context) error {
	return nil
}

func (c *counterStateMachine) Count() int64 {
	return c.count.Load()
}

var _ sm.StateMachine = (*counterStateMachine)(nil)

// ---------------------------------------------------------------------------
// Host construction helpers
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

// testHostConfig creates a valid HostConfig for integration testing.
// Each call gets unique temp directories via t.TempDir().
func testHostConfig(t *testing.T, listenAddr string, mtls *config.MTLSConfig) config.HostConfig {
	t.Helper()
	assertApplyOrder(t)
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

// testShardConfig creates a per-shard config for integration testing.
func testShardConfig(shardID, replicaID uint64) config.Config {
	c := config.Config{
		ShardID:   shardID,
		ReplicaID: replicaID,
	}
	c.SetDefaults()
	return c
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

// counterCreateFunc returns a sm.CreateFunc that produces counterStateMachine instances.
func counterCreateFunc() (sm.CreateFunc, *counterTracker) {
	tracker := &counterTracker{
		machines: make(map[smKey]*counterStateMachine),
	}
	createFn := func(shardID, replicaID uint64) interface{} {
		machine := &counterStateMachine{}
		tracker.mu.Lock()
		tracker.machines[smKey{shardID, replicaID}] = machine
		tracker.mu.Unlock()
		return machine
	}
	return createFn, tracker
}

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

// counterTracker tracks counterStateMachine instances.
type counterTracker struct {
	mu       sync.Mutex
	machines map[smKey]*counterStateMachine
}

func (t *counterTracker) Get(shardID, replicaID uint64) *counterStateMachine {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.machines[smKey{shardID, replicaID}]
}

// ---------------------------------------------------------------------------
// Wait helpers
// ---------------------------------------------------------------------------

// waitForLeader polls until the engine node for the shard elects a leader.
// For single-node clusters, self-election happens within a few RTT ticks.
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
// expires. This is a general-purpose helper for integration tests.
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

// defaultTimeout is the standard timeout for integration test operations.
const defaultTimeout = 10 * time.Second

// leaderElectionTimeout is the time to wait for a leader election.
// With RTT=50ms and ElectionRTT=10, election timeout is ~500ms. We
// give 5x headroom for CI environments.
const leaderElectionTimeout = 5 * time.Second

// ---------------------------------------------------------------------------
// Linearizable read assertions
// ---------------------------------------------------------------------------

// syncReadExact performs a LINEARIZABLE read of key through h (ReadIndex plus
// a state machine lookup at the confirmed read index) and asserts the value is
// exactly want.
//
// Transport-level failures are retried until the timeout: right after an
// election the new leader may briefly reject ReadIndex while it commits its
// no-op entry. A read that SUCCEEDS with the wrong value is never retried and
// fails immediately — an entry that was committed before the read cannot
// legally change or disappear, so retrying would only hide the violation
// behind a timeout message.
func syncReadExact(t *testing.T, h *quicraft.Host, shardID uint64, key, want string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	var lastErr error
	for {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		result, err := h.SyncRead(ctx, shardID, key)
		cancel()
		if err == nil {
			got, ok := result.(string)
			if !ok {
				t.Fatalf("SyncRead(shard %d, %q) returned %T, want string", shardID, key, result)
			}
			if got != want {
				t.Fatalf("linearizable read of %q on shard %d = %q, want %q: a committed entry did not survive into the current leader's term (Leader Completeness violated)",
					key, shardID, got, want)
			}
			return
		}
		lastErr = err
		if time.Now().After(deadline) {
			t.Fatalf("timed out after %v waiting for a linearizable read of %q on shard %d to succeed: %v",
				timeout, key, shardID, lastErr)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// assertSMHasExact fails the test unless the state machine holds key with
// exactly want. Unlike a Lookup through the Host this reads the replica's own
// applied state directly, so it cannot be satisfied by a read that was served
// somewhere else.
func assertSMHasExact(t *testing.T, machine *kvStateMachine, label, key, want string) {
	t.Helper()
	if machine == nil {
		t.Fatalf("%s: state machine not found", label)
	}
	got, ok := machine.Get(key)
	if !ok {
		t.Fatalf("%s state machine is missing key %q (want %q): a committed entry was lost", label, key, want)
	}
	if got != want {
		t.Fatalf("%s state machine has %q = %q, want %q", label, key, got, want)
	}
}

// proposeCommitted proposes a put through h and waits for it to be COMMITTED
// AND APPLIED: RequestState.Result resolves from the state machine apply
// callback, whereas a bare Propose followed by Release proves only that the
// proposal entered the pipeline. Every write a test later makes an assertion
// about goes through here.
//
// Transient rejections (a leader elected moments ago, a momentarily busy
// pipeline) are retried until timeout; the timeout itself is a hard failure.
// Retrying is safe because a put is idempotent under last-write-wins.
func proposeCommitted(t *testing.T, h *quicraft.Host, shardID uint64, key, value string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	cmd := encodeKVPut(key, value)
	var lastErr error
	for {
		attemptTimeout := time.Until(deadline)
		if attemptTimeout > 5*time.Second {
			attemptTimeout = 5 * time.Second
		}
		if attemptTimeout <= 0 {
			t.Fatalf("timed out after %v committing %q=%q on shard %d: %v",
				timeout, key, value, shardID, lastErr)
		}
		ctx, cancel := context.WithTimeout(context.Background(), attemptTimeout)
		//quicraft:direct-propose h is the caller's chosen replica; see #97 on this loop's blanket retry
		rs, err := h.Propose(ctx, shardID, cmd)
		if err == nil {
			_, lastErr = rs.Result()
			rs.Release()
			if lastErr == nil {
				cancel()
				return
			}
		} else {
			lastErr = err
		}
		cancel()
		if time.Now().After(deadline) {
			t.Fatalf("timed out after %v committing %q=%q on shard %d: %v",
				timeout, key, value, shardID, lastErr)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// proposeToLeader proposes cmd to whichever node leads shardID AT THE MOMENT
// OF THE CALL and returns the committed result. See package
// test/proposer for why observing a leader does not pin it, and for the
// callers that must NOT use this.
func proposeToLeader(
	t *testing.T,
	cluster *testCluster,
	shardID uint64,
	cmd []byte,
	timeout time.Duration,
) quicraft.RequestResult {
	t.Helper()
	return proposer.ToLeader(t, cluster.leaderHost, shardID, cmd, timeout)
}

// tryProposeToLeader is proposeToLeader's error-returning form, for callers
// that layer their own retry on top and for goroutines, which must not touch t.
func tryProposeToLeader(
	cluster *testCluster,
	shardID uint64,
	cmd []byte,
	timeout time.Duration,
) (quicraft.RequestResult, error) {
	return proposer.Try(cluster.leaderHost, shardID, cmd, timeout)
}

// ---------------------------------------------------------------------------
// Snapshot artifact assertions
// ---------------------------------------------------------------------------

// snapshotArtifact is one durable snapshot found on disk.
type snapshotArtifact struct {
	index uint64
	path  string
	size  int64
}

func (a snapshotArtifact) String() string {
	return fmt.Sprintf("index=%d size=%d path=%s", a.index, a.size, a.path)
}

// findSnapshotArtifacts walks dir and returns every snapshot.dat written for
// shardID/replicaID, with the index decoded from the enclosing
// snapshot-<020d index> directory (see snapshotSaveDir in
// pkg/internal/engine/snapshot_pool.go). A recursive walk is used rather than
// a hardcoded path so the assertion survives layout changes above the
// shard-<n>/replica-<n>/snapshot-<index> suffix.
//
// The artifact is the only durable proof that a snapshot was actually taken:
// a state machine that merely holds the expected keys is equally explained by
// plain log replay.
func findSnapshotArtifacts(t *testing.T, dir string, shardID, replicaID uint64) []snapshotArtifact {
	t.Helper()

	shardSeg := fmt.Sprintf("shard-%d", shardID)
	replicaSeg := fmt.Sprintf("replica-%d", replicaID)

	var found []snapshotArtifact
	walkErr := filepath.WalkDir(dir, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			// Snapshot GC can remove a directory mid-walk; anything else is
			// a real failure.
			if errors.Is(err, fs.ErrNotExist) {
				return nil
			}
			return err
		}
		if d.IsDir() || d.Name() != "snapshot.dat" {
			return nil
		}
		snapDir := filepath.Dir(path)
		base := filepath.Base(snapDir)
		if !strings.HasPrefix(base, "snapshot-") {
			return nil
		}
		replicaDir := filepath.Dir(snapDir)
		if filepath.Base(replicaDir) != replicaSeg {
			return nil
		}
		if filepath.Base(filepath.Dir(replicaDir)) != shardSeg {
			return nil
		}
		index, parseErr := strconv.ParseUint(strings.TrimPrefix(base, "snapshot-"), 10, 64)
		if parseErr != nil {
			return fmt.Errorf("undecodable snapshot directory name %q: %w", base, parseErr)
		}
		info, statErr := os.Stat(path)
		if statErr != nil {
			if errors.Is(statErr, fs.ErrNotExist) {
				return nil
			}
			return statErr
		}
		found = append(found, snapshotArtifact{index: index, path: path, size: info.Size()})
		return nil
	})
	if walkErr != nil {
		t.Fatalf("walking %s for snapshot artifacts of shard %d replica %d: %v",
			dir, shardID, replicaID, walkErr)
	}
	sort.Slice(found, func(i, j int) bool { return found[i].index < found[j].index })
	return found
}

// waitForSnapshotArtifact fails the test unless a non-empty snapshot.dat for
// shardID/replicaID at exactly index appears under dir within timeout.
func waitForSnapshotArtifact(t *testing.T, dir string, shardID, replicaID, index uint64, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		artifacts := findSnapshotArtifacts(t, dir, shardID, replicaID)
		for _, a := range artifacts {
			if a.index != index {
				continue
			}
			if a.size == 0 {
				t.Fatalf("snapshot artifact %s is zero bytes: an empty snapshot restores nothing", a.path)
			}
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("no snapshot artifact at index %d for shard %d replica %d under %s after %v (found %v): the snapshot request reported success without writing a durable snapshot",
				index, shardID, replicaID, dir, timeout, artifacts)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// awaitSnapshot issues a snapshot request on h, WAITS for its result (the
// request is asynchronous, so a bare Release waits for nothing at all), and
// asserts the reported snapshot index is non-zero and covers everything the
// replica had applied when the request was made. It then asserts the matching
// durable artifact exists under nodeHostDir and returns the snapshot index.
func awaitSnapshot(t *testing.T, h *quicraft.Host, nodeHostDir string, shardID, replicaID uint64) uint64 {
	t.Helper()

	info, err := h.GetShardInfo(shardID)
	if err != nil {
		t.Fatalf("GetShardInfo(shard %d) before snapshot: %v", shardID, err)
	}
	appliedBefore := info.LastApplied
	if appliedBefore == 0 {
		t.Fatalf("shard %d has applied nothing before the snapshot request; the snapshot would be vacuous", shardID)
	}

	ctx, cancel := context.WithTimeout(context.Background(), defaultTimeout*3)
	defer cancel()

	// The snapshot pool releases a shard's snapshotting slot only AFTER the
	// completion callback has run (see snapshot_pool.go), so a request issued
	// immediately after a previous snapshot resolved can still be rejected.
	// That is a scheduling artifact, not a snapshot failure, so it is retried;
	// every other error is fatal.
	var rs *quicraft.RequestState
	submitDeadline := time.Now().Add(defaultTimeout)
	for {
		rs, err = h.RequestSnapshot(ctx, shardID, quicraft.SnapshotOption{})
		if err == nil {
			break
		}
		if !errors.Is(err, quicraft.ErrSnapshotInProgress) {
			t.Fatalf("RequestSnapshot(shard %d) failed: %v", shardID, err)
		}
		if time.Now().After(submitDeadline) {
			t.Fatalf("RequestSnapshot(shard %d) still reported a snapshot in progress after %v",
				shardID, defaultTimeout)
		}
		time.Sleep(20 * time.Millisecond)
	}
	if rs == nil {
		t.Fatalf("RequestSnapshot(shard %d) returned nil", shardID)
	}
	defer rs.Release()

	select {
	case result := <-rs.ResultC():
		if result.Err != nil {
			t.Fatalf("snapshot on shard %d failed: %v", shardID, result.Err)
		}
		if result.Value == 0 {
			t.Fatalf("snapshot on shard %d completed at index 0: no snapshot was taken", shardID)
		}
		if result.Value < appliedBefore {
			t.Fatalf("snapshot on shard %d completed at index %d, below the %d entries already applied: the snapshot does not cover committed state",
				shardID, result.Value, appliedBefore)
		}
		waitForSnapshotArtifact(t, nodeHostDir, shardID, replicaID, result.Value, defaultTimeout)
		return result.Value
	case <-ctx.Done():
		t.Fatalf("timed out waiting for the snapshot on shard %d to complete: %v", shardID, ctx.Err())
		return 0
	}
}
