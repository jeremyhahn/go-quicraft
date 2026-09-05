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
	"slices"
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
// Raft timing, and the election budgets derived from it
//
// Every election budget below is DERIVED from the shard configuration these
// tests actually start, and the config builders are written in terms of the
// same constants: hostConfigAt (behind testHostConfig, newTestClusterWithRestart
// and restartNode) reads testRTTMillisecond, and testShardConfig /
// testShardConfigWithSnapshot leave ElectionRTT to config.Config.SetDefaults,
// which is the value testElectionRTT names rather than restates. Neither side
// can be changed without the other following, and
// TestBudgetsDescribeTheClusterTheyBudgetFor pins that they still agree.
//
// A budget written as a literal is a value that is not pinned treated as
// though it were — the class this repository keeps being bitten by (#84, #88,
// #90, #91, #103, #107, #111, #112, #115, #121). In those the unpinned value
// was a leader index or a replica ID; here it is the shard configuration the
// budget is supposed to describe. A literal cannot follow a config change and
// gives no signal when it stops matching; a derivation cannot drift at all.
//
// This suite was the THIRD copy of the 5s literal (#130). The derivation is
// the one contrib/dragonboat took in PR #124 and test/integration/e2e took in
// PR #125, mirrored on purpose: the suites keep separate copies because each
// owns its own helpers and its own config builders; the shape is shared, the
// constants are not.
// ---------------------------------------------------------------------------

// defaultTimeout is the standard budget for a single Raft operation that is
// NOT an election: a proposal round trip, a lookup, a membership query. It is
// deliberately not expressed in election rounds, because none of its callers
// is waiting for an election.
const defaultTimeout = 10 * time.Second

// testRTTMillisecond is the Raft tick interval, in milliseconds, that
// hostConfigAt gives every host in this suite. One tick is one RTTMillisecond,
// so every timeout the engine expresses in ticks is this many milliseconds per
// tick.
const testRTTMillisecond = 50

// testElectionRTT is the number of ticks a follower waits without hearing from
// a leader before it stands for election.
//
// testShardConfig does not set ElectionRTT, so Config.SetDefaults supplies it.
// This reads THAT value rather than restating it, so the budgets below follow
// the default if it ever moves.
const testElectionRTT = config.DefaultElectionRTT

// raftTick is one Raft tick in wall-clock time.
const raftTick = testRTTMillisecond * time.Millisecond

// electionRound is the longest a SINGLE election attempt can run before the
// next candidate's timer fires and supersedes it.
//
// It is TWICE ElectionRTT, not once. Raft randomises each replica's election
// timer over [electionTimeout, 2*electionTimeout) —
// randomizedElectionTimeout = electionTimeout + rng.Intn(electionTimeout), see
// pkg/internal/raft — so the upper bound on one attempt is two election
// timeouts' worth of ticks. A budget written against the un-randomised value
// is half the size it appears to be, which is how a "10x headroom" constant
// turns out to be 5x. Here: 2 * 10 * 50ms = 1s.
const electionRound = 2 * time.Duration(testElectionRTT) * raftTick

// coldStartElectionRounds is how many worst-case election rounds
// leaderElectionTimeout allows a shard that was started moments ago to produce
// its FIRST leader.
//
// FOUR of them are the protocol's own worst case for a cold start, each
// bounded by electionRound:
//
//   - the QUIC handshake and mTLS exchange between all the hosts, which must
//     complete before the first RequestVote can even be answered;
//   - the PreVote round (SetDefaults turns PreVote on), which precedes the
//     real one;
//   - the vote round itself;
//   - one split-vote retry. On a cold start a split is likely rather than
//     exceptional: every replica arms its election timer within microseconds
//     of the others, which is the one situation the randomised timer has least
//     room to separate.
//
// The remaining factor is host saturation, and it is a MULTIPLIER rather than
// an addend. A Raft tick is a goroutine wakeup; on a box oversubscribed k
// times a 50ms tick lands roughly k times late, and the whole cold start
// stretches with it. The distribution measured for the e2e suite under that
// load — the same root module, the same 50ms tick and ElectionRTT 10, the same
// CI box — is heavy-tailed and is recorded on #111; the failure this suite
// produced under it (#130) was a cold start that ran past the 5s literal.
//
// Thirty rounds is thirty seconds. It is BOUNDED on purpose, and the bound is
// the point of the number rather than an afterthought: a shard that CANNOT
// elect — a lost quorum, a broken transport, a membership that does not agree
// — is still reported in half a minute, naming the shard, instead of running
// to the suite timeout. That case is verified directly rather than assumed:
// a three-member shard with only one replica running fails this wait at the
// 30s mark (30.57s of test time, the budget plus cluster setup).
//
// This budget is LARGER than failoverTimeout even though the names suggest the
// opposite, and that is the correction. A cold start is the HARDER of the two
// waits: a failover happens over transport that is already established between
// survivors that already know one another, while a cold start pays for the
// handshakes first. The easier case had 20s and the harder one had 5s.
const coldStartElectionRounds = 30

// leaderElectionTimeout is the budget for a freshly started shard to produce
// its FIRST leader. See coldStartElectionRounds for the derivation.
//
// It is a SETUP budget and nothing else. Every one of its call sites waits on
// a shard started moments earlier, before the test's subject is reached, so a
// test that exhausts it has asserted nothing at all. A wait for a SUBSEQUENT
// leader, after one has been stopped or partitioned away, is a different
// question and takes failoverTimeout.
const leaderElectionTimeout = coldStartElectionRounds * electionRound

// failoverTimeout is the budget for a shard that has already been led to elect
// a REPLACEMENT leader after the incumbent is stopped or partitioned away.
//
// Its value is unchanged at 20s; it is re-expressed in election rounds so that
// it tracks ElectionRTT and RTTMillisecond alongside leaderElectionTimeout
// instead of drifting away from them. A failover needs one election timeout for
// the survivors to notice the silence and a round or two to elect, over warm
// connections.
const failoverTimeout = 20 * electionRound

// membershipTimeout is the generous timeout for config change operations
// that may require leader round-trips, log replication, and QUIC setup.
const membershipTimeout = 30 * time.Second

// ---------------------------------------------------------------------------
// KV Command Encoding
// ---------------------------------------------------------------------------

// Wire format: [op:1][keyLen:2 BigEndian][key][value]
const (
	opPut byte = 1
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
// sm.StateMachine for host API integration testing. It supports put
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
	// recoveries counts RecoverFromSnapshot calls. It is the only direct
	// evidence a replica caught up by INSTALLING A SNAPSHOT rather than by
	// replaying the log: a test that claims snapshot-based catch-up and only
	// checks that the keys arrived proves nothing, because log replay puts
	// exactly the same keys in exactly the same place.
	recoveries atomic.Uint64
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

// Recoveries returns the number of times this state machine was restored
// from a snapshot.
func (kv *kvStateMachine) Recoveries() uint64 {
	return kv.recoveries.Load()
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
	kv.recoveries.Add(1)
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

// hostConfigAt is the ONE HostConfig this suite starts hosts from, over the
// given directories and address. testHostConfig, newTestClusterWithRestart and
// restartNode all build through it, so the Raft tick every budget in this file
// is derived from is read from testRTTMillisecond in exactly one place; three
// inline literals were three places for it to drift (#130).
func hostConfigAt(walDir, nodeHostDir, addr string, mtls *config.MTLSConfig) config.HostConfig {
	return config.HostConfig{
		WALDir:                walDir,
		NodeHostDir:           nodeHostDir,
		RaftAddress:           addr,
		ListenAddress:         addr,
		RTTMillisecond:        testRTTMillisecond,
		DeploymentID:          1,
		AllowZeroDeploymentID: false,
		NumWorkers:            1,
		MaxApplyWorkers:       1,
		TransportConfig: config.TransportConfig{
			MTLSConfig: mtls,
		},
	}
}

// testHostConfig creates a valid HostConfig for host API testing. Each call
// gets unique temp directories via t.TempDir(). The mtls parameter must be
// shared across all nodes in a cluster for mutual verification.
func testHostConfig(t *testing.T, listenAddr string, mtls *config.MTLSConfig) config.HostConfig {
	t.Helper()
	assertApplyOrder(t)
	return hostConfigAt(t.TempDir(), t.TempDir(), listenAddr, mtls)
}

// testShardConfig creates a per-shard config for host API testing.
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

// testCluster manages a group of Hosts for multi-node host API tests. Each
// Host runs on a separate listen address with real QUIC transport.
type testCluster struct {
	hostsMu  sync.RWMutex
	hosts    []*quicraft.Host
	addrs    []string
	trackers []*smTracker
	stopped  []atomic.Bool // tracks which nodes have been stopped
	// nodeHostDirs records each node's NodeHostDir so snapshot tests can
	// assert that a snapshot artifact really landed on disk.
	nodeHostDirs []string
	mtls         *config.MTLSConfig
	t            *testing.T
}

// newTestCluster creates an N-node cluster with real QUIC transport.
// Each host gets a unique listen address on the loopback interface.
// The cluster is not started (no shards); call startShard to begin.
func newTestCluster(t *testing.T, numNodes int) *testCluster {
	t.Helper()

	assertApplyOrder(t)

	mtls := testMTLSConfig(t)

	addrs := make([]string, numNodes)
	for i := range numNodes {
		addrs[i] = freePort(t)
	}

	hosts := make([]*quicraft.Host, numNodes)
	nodeHostDirs := make([]string, numNodes)
	for i := range numNodes {
		cfg := testHostConfig(t, addrs[i], mtls)
		nodeHostDirs[i] = cfg.NodeHostDir
		h, err := quicraft.NewHost(cfg)
		if err != nil {
			// Clean up any previously created hosts.
			for j := 0; j < i; j++ {
				hosts[j].Close()
			}
			t.Fatalf("NewHost[%d] at %s failed: %v", i, addrs[i], err)
		}
		hosts[i] = h
	}

	return &testCluster{
		hosts:        hosts,
		addrs:        addrs,
		stopped:      make([]atomic.Bool, numNodes),
		nodeHostDirs: nodeHostDirs,
		mtls:         mtls,
		t:            t,
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

// findFollower returns the index of a follower node for the given shard
// that also knows who the current leader is (leaderID > 0). This ensures
// the follower can forward proposals to the leader. Returns -1 if no
// suitable follower is found.
func (c *testCluster) findFollower(shardID uint64) int {
	leaderIdx := c.findLeader(shardID)
	c.hostsMu.RLock()
	defer c.hostsMu.RUnlock()
	for i, h := range c.hosts {
		if c.stopped[i].Load() || i == leaderIdx {
			continue
		}
		leaderID, known, err := h.GetLeaderID(shardID)
		if err != nil || !known || leaderID == 0 {
			continue
		}
		return i
	}
	return -1
}

// leaderHost returns the Host that currently leads shardID, or nil when no
// live node reports leadership. Resolved under hostsMu and returned as a Host
// rather than an index, so a caller cannot hold on to a position in a slice
// that restartNode is free to replace.
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

// waitForFollower waits until at least one follower node knows the leader
// for the given shard, then returns that follower's index. Returns -1 and
// calls t.Fatalf if no follower becomes aware of the leader before timeout.
func (c *testCluster) waitForFollower(shardID uint64, timeout time.Duration) int {
	c.t.Helper()
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	deadline := time.NewTimer(timeout)
	defer deadline.Stop()
	for {
		idx := c.findFollower(shardID)
		if idx >= 0 {
			return idx
		}
		select {
		case <-deadline.C:
			c.t.Fatalf("timed out after %v waiting for follower with known leader on shard %d", timeout, shardID)
			return -1
		case <-ticker.C:
		}
	}
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

// waitForCondition polls until the predicate returns true or the timeout
// expires.
func waitForCondition(t *testing.T, timeout time.Duration, desc string, predicate func() bool) {
	t.Helper()
	waitForConditionDiag(t, timeout, desc, predicate, nil)
}

// waitForConditionDiag is waitForCondition with a diagnostic callback that is
// invoked ONCE, on timeout, and appended to the failure message.
//
// A replication wait that expires says only that some node did not reach some
// state within N seconds, which is the same message whether the cluster is
// leaderless, wedged behind a stuck replication window, or merely slow on a
// loaded machine. When that happened on a release runner the log gave no way
// to tell those apart, and the failure did not reproduce in thirty local runs
// under deliberate CPU contention — so the one observation that existed had to
// be thrown away. The diagnostic runs only on the failing path, so it costs
// nothing when the suite is green.
func waitForConditionDiag(
	t *testing.T,
	timeout time.Duration,
	desc string,
	predicate func() bool,
	diag func() string,
) {
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
			if diag != nil {
				t.Fatalf("timed out after %v waiting for condition: %s\n%s",
					timeout, desc, diag())
			}
			t.Fatalf("timed out after %v waiting for condition: %s", timeout, desc)
			return
		case <-ticker.C:
		}
	}
}

// shardStateDump renders the raft state of every live node for one shard: who
// each node believes the leader is, the term it is in, how far it has applied,
// what its local log actually spans, and how much is still queued. A node that
// is behind, a node that disagrees about the leader, and a node whose log stops
// short all look different here, which is the whole point.
//
// It never fails the test: it runs while a test is already failing, so an error
// reading one node is reported inline and the remaining nodes still print.
func shardStateDump(cluster *testCluster, shardID uint64) string {
	var b strings.Builder
	fmt.Fprintf(&b, "shard %d state at failure:\n", shardID)

	for _, i := range cluster.liveNodeIndexes() {
		cluster.hostsMu.RLock()
		h := cluster.hosts[i]
		cluster.hostsMu.RUnlock()
		if h == nil {
			fmt.Fprintf(&b, "  node[%d]: <nil host>\n", i)
			continue
		}

		info, err := h.GetShardInfo(shardID)
		if err != nil {
			fmt.Fprintf(&b, "  node[%d]: GetShardInfo: %v\n", i, err)
			continue
		}

		logSpan := "unavailable"
		if lr, err := h.GetLogRange(shardID); err == nil {
			logSpan = fmt.Sprintf("[%d,%d]", lr.FirstIndex, lr.LastIndex)
		}

		fmt.Fprintf(&b,
			"  node[%d] replica=%d leader=%d term=%d isLeader=%v applied=%d log=%s pending=%d confChange=%d health=%v\n",
			i, info.ReplicaID, info.LeaderID, info.Term, info.IsLeader,
			info.LastApplied, logSpan, info.Pending, info.ConfigChangeIndex, info.Health)
	}
	return b.String()
}

// waitForConditionNoFatal polls the predicate until timeout, returning true
// if the condition was met before the deadline.
func waitForConditionNoFatal(timeout time.Duration, predicate func() bool) bool {
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	deadline := time.NewTimer(timeout)
	defer deadline.Stop()
	for {
		if predicate() {
			return true
		}
		select {
		case <-deadline.C:
			return false
		case <-ticker.C:
		}
	}
}

// liveNodes returns the indexes of every node that has not been stopped.
func (c *testCluster) liveNodeIndexes() []int {
	c.hostsMu.RLock()
	numNodes := len(c.hosts)
	c.hostsMu.RUnlock()

	live := make([]int, 0, numNodes)
	for i := range numNodes {
		if !c.stopped[i].Load() {
			live = append(live, i)
		}
	}
	return live
}

// resolveStateMachine returns the state machine for node i on the given shard.
//
// The cluster's own tracker slice is consulted FIRST because it is the live
// one: addNodeToCluster appends to it and restartNode overwrites entries in it,
// while a trackers slice a test captured from startShard is a snapshot from
// before those changes and points at state machines that no longer run. The
// caller's slice is the fallback, which covers a shard whose trackers were
// never stored on the cluster (startShard overwrites cluster.trackers, so only
// the most recently started shard is there).
func resolveStateMachine(cluster *testCluster, trackers []*smTracker, shardID uint64, i int) *kvStateMachine {
	replicaID := uint64(i + 1)

	cluster.hostsMu.RLock()
	live := make([]*smTracker, len(cluster.trackers))
	copy(live, cluster.trackers)
	cluster.hostsMu.RUnlock()

	if i < len(live) && live[i] != nil {
		if machine := live[i].Get(shardID, replicaID); machine != nil {
			return machine
		}
	}
	if i < len(trackers) && trackers[i] != nil {
		if machine := trackers[i].Get(shardID, replicaID); machine != nil {
			return machine
		}
	}
	return nil
}

// waitForStateMachine returns node i's state machine for the shard, waiting
// for the engine to register it if it has not appeared yet.
//
// A nil state machine is NOT a reason to skip the node: tracker.Get returns nil
// exactly when the engine never invoked the state-machine factory for that
// shard/replica, which is a startup or recovery failure. Waiting (and failing
// on timeout) is the only treatment that cannot silently drop a node from the
// verification set.
func waitForStateMachine(t *testing.T, cluster *testCluster, trackers []*smTracker, shardID uint64, i int, timeout time.Duration) *kvStateMachine {
	t.Helper()
	machine := resolveStateMachine(cluster, trackers, shardID, i)
	if machine != nil {
		return machine
	}
	waitForCondition(t, timeout,
		fmt.Sprintf("node[%d] registers a state machine for shard %d", i, shardID),
		func() bool {
			machine = resolveStateMachine(cluster, trackers, shardID, i)
			return machine != nil
		},
	)
	return machine
}

// waitForReplication waits for a key to appear with the expected value in the
// state machine of EVERY live node, and fails if it did not actually check
// every one of them.
//
// The count is the point. The previous version skipped any node whose index was
// past the end of the caller's trackers slice and any node whose state machine
// had not registered yet, silently and without a word in the log — so a node
// that never came up at all was reported as verified.
func waitForReplication(t *testing.T, cluster *testCluster, trackers []*smTracker, shardID uint64, key, expectedVal string, timeout time.Duration) {
	t.Helper()

	live := cluster.liveNodeIndexes()
	if len(live) == 0 {
		t.Fatalf("waitForReplication(key=%q): no live nodes to verify", key)
	}

	verified := 0
	for _, i := range live {
		nodeSM := waitForStateMachine(t, cluster, trackers, shardID, i, timeout)
		waitForConditionDiag(t, timeout,
			fmt.Sprintf("node[%d] replicates key=%q", i, key),
			func() bool {
				val, ok := nodeSM.Get(key)
				return ok && val == expectedVal
			},
			func() string { return shardStateDump(cluster, shardID) },
		)
		verified++
	}

	if verified < len(live) {
		t.Fatalf("waitForReplication(key=%q): verified %d nodes but %d are live",
			key, verified, len(live))
	}
}

// ---------------------------------------------------------------------------
// Cluster Operations
// ---------------------------------------------------------------------------

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

// ---------------------------------------------------------------------------
// Proposal & Verification Helpers
// ---------------------------------------------------------------------------

// proposeAndWait proposes a key-value pair through the leader and waits
// for it to be applied on the leader's state machine.
func proposeAndWait(t *testing.T, cluster *testCluster, trackers []*smTracker, shardID uint64, key, value string) {
	t.Helper()

	// Retry the propose cycle to handle leadership changes that can
	// occur during rolling restarts. The leader found by findLeader
	// may step down before or during the Propose call.
	cmd := encodeKVPut(key, value)
	retryTicker := time.NewTicker(100 * time.Millisecond)
	defer retryTicker.Stop()
	overallDeadline := time.NewTimer(defaultTimeout)
	defer overallDeadline.Stop()
	for {
		leaderIdx := cluster.findLeader(shardID)
		if leaderIdx < 0 {
			select {
			case <-overallDeadline.C:
				t.Fatalf("proposeAndWait(%q=%q) timed out after %v: no leader", key, value, defaultTimeout)
				return
			case <-retryTicker.C:
			}
			continue
		}
		leaderHost := cluster.hosts[leaderIdx]

		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		//quicraft:direct-propose resolved at the top of this same retry iteration
		_, err := leaderHost.Propose(ctx, shardID, cmd)
		cancel()
		if err != nil {
			select {
			case <-overallDeadline.C:
				t.Fatalf("proposeAndWait(%q=%q) timed out after %v: propose error: %v", key, value, defaultTimeout, err)
				return
			case <-retryTicker.C:
			}
			continue
		}

		// Check ANY non-stopped node's SM for the applied value. After
		// leadership changes, the proposing node may no longer be the
		// leader, but the entry should still replicate to all nodes.
		applied := waitForConditionNoFatal(5*time.Second,
			func() bool {
				for _, i := range cluster.liveNodeIndexes() {
					machine := resolveStateMachine(cluster, trackers, shardID, i)
					if machine == nil {
						continue
					}
					if val, ok := machine.Get(key); ok && val == value {
						return true
					}
				}
				return false
			},
		)
		if applied {
			return
		}
		// Entry wasn't applied — likely leadership changed mid-propose.
		// Retry with a fresh leader lookup.
		select {
		case <-overallDeadline.C:
			t.Fatalf("proposeAndWait(%q=%q) timed out after %v", key, value, defaultTimeout)
			return
		case <-retryTicker.C:
		}
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

// verifyAllData verifies that every KV pair in expected is present on the
// state machine of EVERY live node, and fails if it did not actually check
// every one of them.
//
// Like waitForReplication, this used to skip a node whose state machine had
// not registered — the exact symptom of a node that failed to start or to
// recover — while the caller logged that all nodes were verified.
func verifyAllData(
	t *testing.T,
	cluster *testCluster,
	trackers []*smTracker,
	shardID uint64,
	expected map[string]string,
) {
	t.Helper()

	live := cluster.liveNodeIndexes()
	if len(live) == 0 {
		t.Fatal("verifyAllData: no live nodes to verify")
	}

	verified := 0
	for _, i := range live {
		nodeSM := waitForStateMachine(t, cluster, trackers, shardID, i, membershipTimeout)
		for key, val := range expected {
			waitForConditionDiag(t, membershipTimeout,
				fmt.Sprintf("node[%d] has key=%q", i, key),
				func() bool {
					got, ok := nodeSM.Get(key)
					return ok && got == val
				},
				func() string { return shardStateDump(cluster, shardID) },
			)
		}
		verified++
	}

	if verified < len(live) {
		t.Fatalf("verifyAllData: verified %d nodes but %d are live", verified, len(live))
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

// waitForMembershipConverged waits until EVERY live replica reports exactly
// wantNodes as its voting members and at least wantRemoved as removed.
//
// waitForMembershipSize above asks ONE host — in practice the leader, which is
// the replica that proposed the change and therefore the one guaranteed to have
// applied it. The failure worth catching is the opposite: a config change that
// commits and applies on the leader but never lands on a follower. That
// follower goes on sizing quorums over a configuration the shard has left,
// which is how a minority comes to believe it has a majority. A single-replica
// assertion cannot see it.
//
// Convergence rather than instantaneous agreement is the property: followers
// apply the change when the entry commits on them, a replication round trip
// behind the leader. The timeout is what distinguishes trailing from diverged.
// Stopped replicas are skipped — a dead node holds no configuration.
func waitForMembershipConverged(
	t *testing.T,
	cluster *testCluster,
	shardID uint64,
	wantNodes []uint64,
	wantRemoved []uint64,
	timeout time.Duration,
) {
	t.Helper()

	deadline := time.Now().Add(timeout)
	var diag string
	for {
		diag = membershipDivergence(cluster, shardID, wantNodes, wantRemoved)
		if diag == "" {
			t.Logf("membership converged on every live replica: nodes=%v removed=%v",
				wantNodes, wantRemoved)
			return
		}
		if !time.Now().Before(deadline) {
			break
		}
		timer := time.NewTimer(50 * time.Millisecond)
		<-timer.C
		timer.Stop()
	}
	t.Fatalf("membership did not converge on every live replica within %v "+
		"(want nodes=%v, removed=%v):%s", timeout, wantNodes, wantRemoved, diag)
}

// membershipDivergence describes how the live replicas' configurations differ
// from the expected one, or returns the empty string when they all agree.
func membershipDivergence(
	cluster *testCluster,
	shardID uint64,
	wantNodes []uint64,
	wantRemoved []uint64,
) string {
	cluster.hostsMu.RLock()
	hosts := make([]*quicraft.Host, len(cluster.hosts))
	copy(hosts, cluster.hosts)
	cluster.hostsMu.RUnlock()

	var sb strings.Builder
	for i, h := range hosts {
		if h == nil || cluster.stopped[i].Load() {
			continue
		}
		// A replica that is itself in wantRemoved is excluded. Convergence is a
		// property of the CURRENT configuration, and a removed replica is
		// outside it: once the removal commits the leader stops replicating to
		// it, so whether it ever learns of its own eviction depends on where it
		// happened to be in the replication stream when the entry committed.
		// Requiring it to agree asserts on a race, not on a property — and
		// asserting on a race is how a suite ends up with a test that fails a
		// few times in a hundred and gets dismissed as flaky.
		//
		// Every replica still IN the configuration is required to converge.
		// That is the divergence being hunted: a config change that applies on
		// the leader and silently fails on a follower.
		if slices.Contains(wantRemoved, uint64(i+1)) {
			continue
		}
		m, err := h.GetShardMembership(shardID)
		if err != nil {
			fmt.Fprintf(&sb, "\n  node[%d]: GetShardMembership failed: %v", i, err)
			continue
		}
		var missing, unexpected, notRemoved []uint64
		for _, id := range wantNodes {
			if _, ok := m.Nodes[id]; !ok {
				missing = append(missing, id)
			}
		}
		for id := range m.Nodes {
			if !slices.Contains(wantNodes, id) {
				unexpected = append(unexpected, id)
			}
		}
		for _, id := range wantRemoved {
			if _, ok := m.Removed[id]; !ok {
				notRemoved = append(notRemoved, id)
			}
		}
		if len(missing) == 0 && len(unexpected) == 0 && len(notRemoved) == 0 {
			continue
		}
		slices.Sort(unexpected)
		fmt.Fprintf(&sb, "\n  node[%d]: has nodes=%v", i, sortedReplicaIDs(m.Nodes))
		if len(missing) > 0 {
			fmt.Fprintf(&sb, " (missing %v)", missing)
		}
		if len(unexpected) > 0 {
			fmt.Fprintf(&sb, " (unexpected %v)", unexpected)
		}
		if len(notRemoved) > 0 {
			fmt.Fprintf(&sb, " (not in removed set: %v)", notRemoved)
		}
	}
	return sb.String()
}

// sortedReplicaIDs returns a membership map's replica IDs in ascending order so
// that failure messages are stable across runs.
func sortedReplicaIDs(m map[uint64]string) []uint64 {
	ids := make([]uint64, 0, len(m))
	for id := range m {
		ids = append(ids, id)
	}
	slices.Sort(ids)
	return ids
}

// ---------------------------------------------------------------------------
// Membership Helpers
// ---------------------------------------------------------------------------

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
) addNodeResult {
	t.Helper()

	// Create a new host on a fresh port, sharing the cluster's mTLS config.
	addr := freePort(t)
	hostCfg := testHostConfig(t, addr, cluster.mtls)
	h, err := quicraft.NewHost(hostCfg)
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
	cluster.nodeHostDirs = append(cluster.nodeHostDirs, hostCfg.NodeHostDir)
	cluster.hostsMu.Unlock()

	return addNodeResult{
		host:    h,
		addr:    addr,
		tracker: tracker,
		idx:     idx,
	}
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
	var nodeSM *kvStateMachine
	// SM may not be registered yet; poll until it appears.
	waitForCondition(t, timeout,
		fmt.Sprintf("replica=%d SM registered", replicaID),
		func() bool {
			nodeSM = tracker.Get(shardID, replicaID)
			return nodeSM != nil
		},
	)
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
// Snapshot Helpers
// ---------------------------------------------------------------------------

// snapshotDataFileName is the on-disk name of a snapshot payload
// (engine.snapshotDataFile). It is duplicated here rather than exported from
// the engine because a test that reads the constant from the code under test
// cannot notice the code writing somewhere else entirely.
const snapshotDataFileName = "snapshot.dat"

// publishedSnapshotDirPrefix is the name a snapshot directory carries once the
// artifact in it is FINISHED: snapshot-<020d index> (engine snapshotSaveDir).
const publishedSnapshotDirPrefix = "snapshot-"

// The two names a snapshot directory can carry BEFORE it is published. Each is
// a directory a writer creates for itself, fills, and then renames onto the
// canonical snapshot-<index> name in one step, so the canonical name only ever
// holds a complete artifact. Both are deliberately unparsable as
// snapshot-<index> so that the engine's own listSnapshotDirs, snapshot GC and
// DEK purge-floor scan skip them; this scan must skip them for the same reason.
//
// The set is the one Host.StartShard's sweeps recognise, restated from
// pkg/internal/engine/snapshot_pool.go rather than imported (the package is
// internal to pkg/, and a scan that borrowed the writer's matcher could not
// notice the writer staging under a name it does not sweep):
//
//   - engine.RecvStageDirPrefix, ".recv-": the InstallSnapshot RECEIVE path
//     (pkg/messagehandler.go) stages here, encrypting inline from the
//     in-memory chunks, and publishes by rename. Swept by
//     engine.SweepStaleRecvStageDirs.
//   - engine.ImportStageDirPrefix, ".import-": Host.ImportSnapshot stages
//     here and publishes by rename. Swept by engine.SweepStaleImportStageDirs.
//
// engine.SaveEncryptTempPrefix, ".encrypt-", is NOT in this set because it is
// not a directory: it names the scratch FILE (".encrypt-*.tmp") a local save
// encrypts into INSIDE an already-published snapshot-<index> directory before
// renaming it over snapshot.dat. A scratch file never carries the payload name
// this scan matches on, so the walk never sees one. The local-save window in
// which snapshot.dat exists in a published directory but is not yet finished
// is #108's, and it is judged by snapshot.meta (PR #119), not by the name.
const (
	recvStageDirPrefix   = ".recv-"
	importStageDirPrefix = ".import-"
)

// isSnapshotStagingDir reports whether name is a snapshot directory that has
// not been published yet. A directory that is not yet published is not an
// artifact: asserting "every artifact lives in snapshot-<index>/" over one of
// these reports the receive path's ordinary staging discipline as a malformed
// artifact (#130), which is the receive-path twin of #108.
func isSnapshotStagingDir(name string) bool {
	return strings.HasPrefix(name, recvStageDirPrefix) ||
		strings.HasPrefix(name, importStageDirPrefix)
}

// snapshotArtifacts returns the path of every PUBLISHED snapshot data file
// under dir.
//
// A snapshot artifact lives at
// <snapshotDir>/shard-<N>/replica-<N>/snapshot-<020d index>/snapshot.dat
// (pkg/internal/engine/snapshot_pool.go, snapshotSaveDir), and snapshotDir is
// rooted under the host's NodeHostDir. Walking the whole NodeHostDir keeps the
// check independent of that layout.
//
// The walk does not descend into a staging directory (isSnapshotStagingDir):
// a receive or import in flight is by design not an artifact yet, and every
// caller of this scan polls, so skipping the directory now and finding the
// published one on a later poll IS waiting the staging out. Nothing else is
// skipped. In particular a payload under any OTHER unexpected directory name
// is still returned, so snapshotArtifactIndexes still rejects it.
func snapshotArtifacts(t *testing.T, dir string) []string {
	t.Helper()
	var found []string
	err := filepath.WalkDir(dir, func(path string, d fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			// Snapshot directories are garbage-collected concurrently, so a
			// directory can vanish between the readdir and the visit.
			if errors.Is(walkErr, fs.ErrNotExist) {
				return nil
			}
			return walkErr
		}
		if d.IsDir() && isSnapshotStagingDir(d.Name()) {
			return filepath.SkipDir
		}
		if !d.IsDir() && d.Name() == snapshotDataFileName {
			found = append(found, path)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("walking %s for snapshot artifacts: %v", dir, err)
	}
	return found
}

// snapshotArtifactIndex returns the raft index a published snapshot artifact
// at path was recorded under, read from its parent directory's
// snapshot-<020d index> name. It is the judgement snapshotArtifactIndexes
// applies to every path the scan returns, kept non-fatal so a test can
// exercise the rejections directly.
func snapshotArtifactIndex(path string) (uint64, error) {
	name := filepath.Base(filepath.Dir(path))
	digits, ok := strings.CutPrefix(name, publishedSnapshotDirPrefix)
	if !ok {
		return 0, fmt.Errorf("snapshot artifact %s is not inside a %s<index> directory",
			path, publishedSnapshotDirPrefix)
	}
	index, err := strconv.ParseUint(digits, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("snapshot artifact %s has an unparsable index %q: %w", path, digits, err)
	}
	return index, nil
}

// snapshotArtifactIndexes returns the raft indexes of the published snapshot
// artifacts under dir, ascending. A published artifact whose directory does not
// parse as snapshot-<index> fails the test: that is a malformed artifact, not
// one in flight, and the scan is deliberately not narrowed to the newest one
// so that a stale artifact under a wrong name is still found (#116).
func snapshotArtifactIndexes(t *testing.T, dir string) []uint64 {
	t.Helper()
	var indexes []uint64
	for _, path := range snapshotArtifacts(t, dir) {
		index, err := snapshotArtifactIndex(path)
		if err != nil {
			t.Fatal(err)
		}
		indexes = append(indexes, index)
	}
	sort.Slice(indexes, func(a, b int) bool { return indexes[a] < indexes[b] })
	return indexes
}

// waitForSnapshotArtifact waits until at least one snapshot artifact exists
// under dir and returns their indexes. It fails the test on timeout: "the keys
// are there" is satisfied by plain log replay, so an artifact on disk is the
// only proof that a snapshot was actually taken.
func waitForSnapshotArtifact(t *testing.T, dir string, timeout time.Duration) []uint64 {
	t.Helper()
	var indexes []uint64
	waitForCondition(t, timeout,
		fmt.Sprintf("a snapshot artifact appears under %s", dir),
		func() bool {
			indexes = snapshotArtifactIndexes(t, dir)
			return len(indexes) > 0
		},
	)
	return indexes
}

// assertSnapshotArtifact fails unless a snapshot artifact at index >= minIndex
// exists under dir. Older artifacts are garbage-collected, so the assertion is
// on the newest index rather than on an exact one.
func assertSnapshotArtifact(t *testing.T, dir string, minIndex uint64, timeout time.Duration) {
	t.Helper()
	var indexes []uint64
	found := waitForConditionNoFatal(timeout, func() bool {
		indexes = snapshotArtifactIndexes(t, dir)
		for _, idx := range indexes {
			if idx >= minIndex {
				return true
			}
		}
		return false
	})
	if !found {
		t.Fatalf("no snapshot artifact at index >= %d on disk under %s (found indexes %v): "+
			"the state machine holding the right keys is equally consistent with plain "+
			"log replay, so the artifact is the only evidence a snapshot was taken",
			minIndex, dir, indexes)
	}
}

// requestSnapshotIndex requests a snapshot on host and returns the index the
// engine reported.
//
// RequestSnapshot is asynchronous: it returns as soon as the request is
// queued. Releasing the RequestState without reading its result — which every
// snapshot test in this package used to do — asserts nothing beyond "the
// request was accepted", and in particular does not wait for the snapshot to
// be taken or notice that it failed.
//
// A snapshot whose index does not advance past the recorded one is a no-op
// that reports index 0 (snapshot_pool.go), so callers must make the applied
// index advance before asking; a zero index here is treated as a failure.
func requestSnapshotIndex(t *testing.T, host *quicraft.Host, shardID uint64) uint64 {
	t.Helper()

	const maxAttempts = 20
	const retryInterval = 250 * time.Millisecond

	for attempt := range maxAttempts {
		ctx, cancel := context.WithTimeout(context.Background(), membershipTimeout)
		rs, err := host.RequestSnapshot(ctx, shardID, quicraft.SnapshotOption{})
		if err != nil {
			cancel()
			if errors.Is(err, quicraft.ErrSnapshotInProgress) {
				// An automatic snapshot holds the shard's snapshot slot.
				retryTimer := time.NewTimer(retryInterval)
				<-retryTimer.C
				retryTimer.Stop()
				continue
			}
			t.Fatalf("RequestSnapshot(shard=%d) failed: %v", shardID, err)
		}
		if rs == nil {
			cancel()
			t.Fatalf("RequestSnapshot(shard=%d) returned a nil RequestState", shardID)
		}

		result, resErr := rs.Result()
		rs.Release()
		cancel()
		if resErr != nil {
			t.Fatalf("snapshot on shard %d did not complete: %v", shardID, resErr)
		}
		if result.Value == 0 {
			// Index 0 is what a SKIPPED save reports. Retry rather than fail
			// on the first one: the host completes every pending snapshot
			// request for a shard from any one completion, so a concurrent
			// automatic no-op can resolve this request. An engine that never
			// snapshots reports 0 every time and still fails below.
			t.Logf("snapshot on shard %d reported index 0 (skipped save), retrying (attempt %d/%d)",
				shardID, attempt+1, maxAttempts)
			retryTimer := time.NewTimer(retryInterval)
			<-retryTimer.C
			retryTimer.Stop()
			continue
		}
		return result.Value
	}

	t.Fatalf("RequestSnapshot(shard=%d) never completed a snapshot after %d attempts: "+
		"the snapshot slot stayed busy or every save reported index 0 (skipped)",
		shardID, maxAttempts)
	return 0
}

// ---------------------------------------------------------------------------
// Leader Transfer Helpers
// ---------------------------------------------------------------------------

// transferLeadershipTo asks the current leader to hand leadership to the node
// at targetIdx and waits until that node — the REQUESTED one, not merely some
// node — reports itself leader.
//
// Leadership transfer is a hint: the leader aborts it if the target does not
// campaign within an election timeout, and the caller learns nothing from the
// nil error RequestLeaderTransfer returns. Re-requesting until the target
// actually takes over (or the deadline expires) is what makes "leadership moved
// to the target" an assertion instead of a hope.
func transferLeadershipTo(t *testing.T, cluster *testCluster, shardID uint64, targetIdx int, timeout time.Duration) {
	t.Helper()

	targetReplicaID := uint64(targetIdx + 1)
	deadline := time.Now().Add(timeout)
	attempts := 0

	for time.Now().Before(deadline) {
		leaderIdx := cluster.findLeader(shardID)
		if leaderIdx == targetIdx {
			return
		}
		if leaderIdx < 0 {
			retryTimer := time.NewTimer(100 * time.Millisecond)
			<-retryTimer.C
			retryTimer.Stop()
			continue
		}

		ctx, cancel := context.WithTimeout(context.Background(), defaultTimeout)
		err := cluster.hosts[leaderIdx].RequestLeaderTransfer(ctx, shardID, targetReplicaID)
		cancel()
		attempts++
		if err != nil && !errors.Is(err, quicraft.ErrNotLeader) {
			t.Fatalf("RequestLeaderTransfer(shard=%d, target=%d) failed: %v",
				shardID, targetReplicaID, err)
		}

		// Give the target an election timeout or two to take over before
		// asking again.
		if waitForConditionNoFatal(3*time.Second, func() bool {
			info, infoErr := cluster.hosts[targetIdx].GetShardInfo(shardID)
			return infoErr == nil && info.IsLeader
		}) {
			return
		}
	}

	t.Fatalf("leadership never moved to the requested target node[%d] (replica=%d) "+
		"on shard %d after %d transfer requests within %v; current leader is node[%d]",
		targetIdx, targetReplicaID, shardID, attempts, timeout, cluster.findLeader(shardID))
}

// ---------------------------------------------------------------------------
// Restart Helpers
// ---------------------------------------------------------------------------

// nodeConfig holds the configuration needed to restart a host node. The WAL
// and NodeHost directories are preserved from the original host so that
// persisted state survives the restart.
type nodeConfig struct {
	walDir      string
	nodeHostDir string
	addr        string
}

// newTestClusterWithRestart creates an N-node cluster that supports node
// restarts by preserving WAL and NodeHost directories per node.
func newTestClusterWithRestart(t *testing.T, numNodes int) (*testCluster, []nodeConfig) {
	t.Helper()

	mtls := testMTLSConfig(t)

	addrs := make([]string, numNodes)
	for i := range numNodes {
		addrs[i] = freePort(t)
	}

	hosts := make([]*quicraft.Host, numNodes)
	configs := make([]nodeConfig, numNodes)
	nodeHostDirs := make([]string, numNodes)
	for i := range numNodes {
		walDir := t.TempDir()
		nodeHostDir := t.TempDir()
		h, err := quicraft.NewHost(hostConfigAt(walDir, nodeHostDir, addrs[i], mtls))
		if err != nil {
			for j := 0; j < i; j++ {
				hosts[j].Close()
			}
			t.Fatalf("NewHost[%d] at %s failed: %v", i, addrs[i], err)
		}
		hosts[i] = h
		nodeHostDirs[i] = nodeHostDir
		configs[i] = nodeConfig{
			walDir:      walDir,
			nodeHostDir: nodeHostDir,
			addr:        addrs[i],
		}
	}

	return &testCluster{
		hosts:        hosts,
		addrs:        addrs,
		stopped:      make([]atomic.Bool, numNodes),
		nodeHostDirs: nodeHostDirs,
		mtls:         mtls,
		t:            t,
	}, configs
}

// restartNode stops the host at the given index, creates a new host on the
// same address using the same WAL and NodeHost directories (preserving
// persisted state), and starts the shard. The cluster's trackers slice is
// updated with a new smTracker for the restarted node.
func restartNode(
	t *testing.T,
	cluster *testCluster,
	idx int,
	nc nodeConfig,
	shardID uint64,
	cfgFn func(shardID, replicaID uint64) config.Config,
) {
	t.Helper()

	// Stop the node if it is still running.
	if !cluster.stopped[idx].Load() {
		cluster.hostsMu.Lock()
		cluster.hosts[idx].Close()
		cluster.stopped[idx].Store(true)
		cluster.hostsMu.Unlock()
	}

	// Create a new host reusing the same data directories and address.
	h, err := quicraft.NewHost(hostConfigAt(nc.walDir, nc.nodeHostDir, nc.addr, cluster.mtls))
	if err != nil {
		t.Fatalf("restartNode[%d] NewHost failed: %v", idx, err)
	}

	// Build the original members map (all initial nodes).
	members := make(map[uint64]string, len(cluster.addrs))
	for i, addr := range cluster.addrs {
		members[uint64(i+1)] = addr
	}

	replicaID := uint64(idx + 1)
	createFn, tracker := kvCreateFunc()
	scfg := cfgFn(shardID, replicaID)
	if err := h.StartShard(members, false, createFn, scfg); err != nil {
		h.Close()
		t.Fatalf("restartNode[%d] StartShard failed: %v", idx, err)
	}

	// Update the cluster state.
	cluster.hostsMu.Lock()
	cluster.hosts[idx] = h
	cluster.stopped[idx].Store(false)
	if cluster.trackers != nil && idx < len(cluster.trackers) {
		cluster.trackers[idx] = tracker
	}
	cluster.hostsMu.Unlock()
}

// TestBudgetsDescribeTheClusterTheyBudgetFor pins the derivation at the top of
// this file to the configuration the suite actually starts.
//
// The constants are only worth having if the config builders keep reading
// them. If a future change gives testShardConfig an explicit ElectionRTT, or
// hands hostConfigAt a different RTTMillisecond, electionRound silently starts
// describing a cluster that no longer exists and every budget built on it is
// wrong by that factor — which is exactly the drift the derivation replaced a
// literal to prevent. This is the only place that can notice.
func TestBudgetsDescribeTheClusterTheyBudgetFor(t *testing.T) {
	if got := testHostConfig(t, "127.0.0.1:1", nil).RTTMillisecond; got != testRTTMillisecond {
		t.Fatalf("testHostConfig sets RTTMillisecond=%d but the budgets are derived "+
			"from testRTTMillisecond=%d; one Raft tick is not what electionRound (%v) "+
			"thinks it is", got, uint64(testRTTMillisecond), electionRound)
	}
	for _, tc := range []struct {
		name string
		cfg  config.Config
	}{
		{"testShardConfig", testShardConfig(1, 1)},
		{"testShardConfigWithSnapshot", testShardConfigWithSnapshot(1, 1, 10, 5)},
	} {
		if got := tc.cfg.ElectionRTT; got != testElectionRTT {
			t.Fatalf("%s sets ElectionRTT=%d but the budgets are derived from "+
				"testElectionRTT=%d; electionRound (%v) and every budget built on it "+
				"are wrong by a factor of %.2f", tc.name, got, testElectionRTT,
				electionRound, float64(got)/float64(testElectionRTT))
		}
	}
	if electionRound != 2*time.Duration(testElectionRTT)*raftTick {
		t.Fatalf("electionRound=%v is not 2*ElectionRTT*tick", electionRound)
	}
	t.Logf("tick=%v electionRound=%v leaderElectionTimeout=%v (%d rounds) failoverTimeout=%v",
		raftTick, electionRound, leaderElectionTimeout, coldStartElectionRounds, failoverTimeout)
}

// TestSnapshotScanJudgesOnlyPublishedArtifacts pins the two halves of the
// artifact scan against each other over a tree laid out the way the engine
// lays one out: a staging directory mid-flight is not an artifact and is not
// judged, while a PUBLISHED directory under a malformed name still is (#130).
//
// The tree is built by hand rather than by driving a cluster into an
// InstallSnapshot, because the window in which a .recv-<n> directory exists is
// the length of one receive and cannot be held open from a test; the layout is
// the engine's documented one (engine.RecvStageDirPattern,
// engine.ImportStageDirPattern, snapshotSaveDir).
func TestSnapshotScanJudgesOnlyPublishedArtifacts(t *testing.T) {
	root := t.TempDir()
	replica := filepath.Join(root, "snapshots", "shard-1", "shard-1", "replica-2")
	payload := func(dir string) string {
		full := filepath.Join(replica, dir)
		if err := os.MkdirAll(full, 0o750); err != nil {
			t.Fatal(err)
		}
		path := filepath.Join(full, snapshotDataFileName)
		if err := os.WriteFile(path, []byte("payload"), 0o600); err != nil {
			t.Fatal(err)
		}
		return path
	}

	published := payload(fmt.Sprintf("%s%020d", publishedSnapshotDirPrefix, 42))
	staging := []string{
		payload(recvStageDirPrefix + "2503549359"),
		payload(importStageDirPrefix + "1180032867"),
	}

	found := snapshotArtifacts(t, replica)
	if len(found) != 1 || found[0] != published {
		t.Fatalf("scan over a published artifact beside %d staging dirs returned %v, want only %s",
			len(staging), found, published)
	}
	if got := snapshotArtifactIndexes(t, replica); len(got) != 1 || got[0] != 42 {
		t.Fatalf("indexes = %v, want [42]", got)
	}

	// A malformed PUBLISHED directory is still a finding. It is not a staging
	// name, so the walk returns it, and the judgement rejects it.
	for _, dir := range []string{
		"snapshot-",            // no index at all
		"snapshot-notanumber",  // unparsable index
		"snap-000000000000043", // wrong prefix entirely
		".stale-42",            // dotted, but not a name any writer stages under
	} {
		path := payload(dir)
		if _, err := snapshotArtifactIndex(path); err == nil {
			t.Fatalf("a published artifact under %q was accepted as well-formed", dir)
		}
		if seen := snapshotArtifacts(t, replica); !slices.Contains(seen, path) {
			t.Fatalf("the walk skipped %q, so its malformed artifact could never be judged: %v", dir, seen)
		}
		if err := os.RemoveAll(filepath.Dir(path)); err != nil {
			t.Fatal(err)
		}
	}

	// And a staging directory holds no artifact, however it is judged: it is
	// never handed to the judgement in the first place.
	for _, path := range staging {
		if seen := snapshotArtifacts(t, replica); slices.Contains(seen, path) {
			t.Fatalf("the walk returned the staging payload %s as an artifact", path)
		}
	}
}
