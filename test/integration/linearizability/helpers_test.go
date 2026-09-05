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
// same constants: testHostConfig reads testRTTMillisecond, and testShardConfig
// leaves ElectionRTT to config.Config.SetDefaults, which is the value
// testElectionRTT names rather than restates. Neither side can be changed
// without the other following, and TestBudgetsDescribeTheClusterTheyBudgetFor
// pins that they still agree.
//
// A budget written as a literal is a value that is not pinned treated as
// though it were — the class this repository keeps being bitten by (#84, #88,
// #90, #91, #103, #107, #111, #112, #115, #121). In those the unpinned value
// was a leader index or a replica ID; here it is the shard configuration the
// budget is supposed to describe. A literal cannot follow a config change and
// gives no signal when it stops matching; a derivation cannot drift at all.
//
// This suite was the FIFTH copy of the 5s literal, found while #130 fixed the third. The derivation is the one contrib/dragonboat took in PR #124
// and test/integration/e2e took in PR #125, mirrored on purpose: the suites
// keep separate copies because each owns its own helpers and its own config
// builders; the shape is shared, the constants are not.
// ---------------------------------------------------------------------------

// defaultTimeout is the standard budget for a single Raft operation that is
// NOT an election: a proposal round trip, a lookup, a membership query. It is
// deliberately not expressed in election rounds, because none of its callers
// is waiting for an election.
const defaultTimeout = 15 * time.Second

// testRTTMillisecond is the Raft tick interval, in milliseconds, that
// testHostConfig gives every host in this suite. One tick is one
// RTTMillisecond, so every timeout the engine expresses in ticks is this many
// milliseconds per tick.
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
// CI box — is heavy-tailed and is recorded on #111.
//
// Thirty rounds is thirty seconds. It is BOUNDED on purpose, and the bound is
// the point of the number rather than an afterthought: a shard that CANNOT
// elect — a lost quorum, a broken transport, a membership that does not agree
// — is still reported in half a minute, naming the shard, instead of running
// to the suite timeout. That case was verified directly in the host suite
// (#130): a three-member shard with only one replica running fails this wait
// at the 30s mark (30.57s of test time, the budget plus cluster setup).
const coldStartElectionRounds = 30

// leaderElectionTimeout is the budget for a freshly started shard to produce
// its FIRST leader. See coldStartElectionRounds for the derivation.
//
// It is a SETUP budget and nothing else. Every one of its call sites waits on
// a shard started moments earlier, before the test's subject is reached, so a
// test that exhausts it has asserted nothing at all.
const leaderElectionTimeout = coldStartElectionRounds * electionRound

// failoverTimeout is the budget for a shard that has already been led to elect
// a REPLACEMENT leader after the incumbent is stopped or partitioned away.
//
// Its value is unchanged at 20s; it is re-expressed in election rounds so that
// it tracks ElectionRTT and RTTMillisecond alongside leaderElectionTimeout
// instead of drifting away from them. A failover needs one election timeout for
// the survivors to notice the silence and a round or two to elect, over warm
// connections. It is SMALLER than leaderElectionTimeout on purpose: a failover
// happens over transport that is already established between survivors that
// already know one another, while a cold start pays for the handshakes first.
const failoverTimeout = 20 * electionRound

// ---------------------------------------------------------------------------
// KV Command Encoding
// ---------------------------------------------------------------------------

// Wire format: [op:1][keyLen:2 BigEndian][key][payload]
const (
	opPut    byte = 1
	opGet    byte = 2
	opAppend byte = 3
)

// appendSep separates the tokens accumulated by an opAppend command. The
// append op exists so the session workload is NOT idempotent: re-applying an
// append produces a value the model forbids, which is what makes a duplicate
// apply observable. A last-write-wins put can never expose one.
const appendSep = ","

// encodeKVCmd encodes a command for the kvStateMachine.
func encodeKVCmd(op byte, key, payload string) []byte {
	keyBytes := []byte(key)
	payloadBytes := []byte(payload)
	buf := make([]byte, 1+2+len(keyBytes)+len(payloadBytes))
	buf[0] = op
	binary.BigEndian.PutUint16(buf[1:3], uint16(len(keyBytes)))
	copy(buf[3:3+len(keyBytes)], keyBytes)
	copy(buf[3+len(keyBytes):], payloadBytes)
	return buf
}

// encodeKVPut encodes a put command for the kvStateMachine.
func encodeKVPut(key, value string) []byte {
	return encodeKVCmd(opPut, key, value)
}

// encodeKVAppend encodes an append command for the kvStateMachine. The token
// is appended to the key's current value, separated by appendSep.
func encodeKVAppend(key, token string) []byte {
	return encodeKVCmd(opAppend, key, token)
}

// encodeKVGet encodes a get query key for kvStateMachine.Lookup.
func encodeKVGet(key string) []byte {
	return []byte(key)
}

// appendResult is the value the kvStateMachine returns for an opAppend. It
// carries the FULL post-append value so a caller can compare the result it
// was handed against the value the model predicts, and so a session retry can
// be checked byte-for-byte against the cached response.
func appendResult(value string) sm.Result {
	return sm.Result{Value: uint64(len(value)), Data: []byte(value)}
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
// sm.StateMachine for linearizability testing.
type kvStateMachine struct {
	mu   sync.RWMutex
	data map[string]string
	// prevIndex is the raft index of the previously applied entry. Every
	// entry handed to Update must carry a STRICTLY greater index: an index
	// that repeats or moves backward means the entry was applied twice or
	// replayed, which no amount of state comparison can detect afterwards
	// on a last-write-wins store. RecoverFromSnapshot is the only legal
	// reset point.
	//
	// Note: consecutive indices are NOT required. The user state machine
	// only ever sees user commands; the engine legitimately consumes
	// indices for the initial-membership entry, each leader's no-op entry,
	// config changes and session register/unregister entries, so gaps are
	// expected and are recorded rather than failed. See applyGaps.
	prevIndex uint64
	// applyGaps counts skipped index ranges, for diagnostics only.
	applyGaps atomic.Uint64
	applied   atomic.Uint64
}

func newKVStateMachine() *kvStateMachine {
	return &kvStateMachine{
		data: make(map[string]string),
	}
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
		case opAppend:
			token := string(entry.Cmd[3+keyLen:])
			value := kv.data[key]
			if value == "" {
				value = token
			} else {
				value = value + appendSep + token
			}
			kv.data[key] = value
			results[i] = appendResult(value)
		default:
			results[i] = sm.Result{Value: 0}
		}
	}
	return nil
}

// ApplyGaps returns the number of skipped index ranges observed so far.
func (kv *kvStateMachine) ApplyGaps() uint64 {
	return kv.applyGaps.Load()
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

// SaveSnapshot serializes the entire KV store to the writer. The applied
// index is written ahead of the payload so a recovering replica can tell
// whether the snapshot moves it forward or rolls it backward.
func (kv *kvStateMachine) SaveSnapshot(_ context.Context, w io.Writer, stopper <-chan struct{}) error {
	kv.mu.RLock()
	defer kv.mu.RUnlock()

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

// RecoverFromSnapshot restores the KV store from a snapshot reader. This is
// the only point at which the applied-index expectation may be reset, and the
// reset must still move the replica FORWARD: installing a snapshot older than
// what this replica already applied would silently roll the state machine
// back, so it is recorded as a violation.
func (kv *kvStateMachine) RecoverFromSnapshot(_ context.Context, r io.Reader, stopper <-chan struct{}) error {
	kv.mu.Lock()
	defer kv.mu.Unlock()

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
	assertApplyOrder(t)
	return config.HostConfig{
		WALDir:                t.TempDir(),
		NodeHostDir:           t.TempDir(),
		RaftAddress:           listenAddr,
		ListenAddress:         listenAddr,
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

// leaderHost returns the Host that currently leads shardID, or nil when no
// live replica reports leadership. Resolved under hostsMu and returned as a
// Host rather than an index, so a caller cannot hold a position in a slice the
// harness is free to replace.
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

// proposeToLeader proposes cmd to whichever node leads shardID AT THE MOMENT
// OF THE CALL. See package test/proposer for why observing a leader
// does not pin it.
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

	// Wait for a leader rather than sampling once. addNode is called mid-test
	// under sustained client load, where a transient election leaves findLeader
	// returning -1 for a few ticks; an instantaneous sample turns that ordinary
	// window into a test failure. This still fails — loudly — if no leader
	// appears within leaderElectionTimeout, so the assertion is not weakened,
	// only made insensitive to when it is sampled.
	//
	// h is closed on the failure path via defer rather than inline:
	// waitForClusterLeader calls t.Fatalf, which runs Goexit and would otherwise
	// leak the host we just constructed.
	leaderFound := false
	defer func() {
		if !leaderFound {
			h.Close()
		}
	}()
	leaderIdx := c.waitForClusterLeader(shardID, leaderElectionTimeout)
	leaderFound = true

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
// Leader/Term Safety Guard
// ---------------------------------------------------------------------------

// leaderObservation is one poll sample of one host's view of a shard.
type leaderObservation struct {
	index     int
	replicaID uint64
	term      uint64
	isLeader  bool
}

// observeLeaders samples (node, IsLeader, Term) from EVERY host currently in
// the cluster and returns the leaders it found.
//
// It deliberately does not consult c.stopped: restartNode and addNode replace
// or extend c.hosts, and a check that skips indices from a stale view of which
// nodes are up is exactly how a second leader escapes notice. A host that is
// genuinely down returns an error from GetShardInfo and contributes nothing.
func (c *testCluster) observeLeaders(shardID uint64) []leaderObservation {
	c.hostsMu.RLock()
	hosts := make([]*quicraft.Host, len(c.hosts))
	copy(hosts, c.hosts)
	c.hostsMu.RUnlock()

	var leaders []leaderObservation
	for i, h := range hosts {
		if h == nil {
			continue
		}
		info, err := h.GetShardInfo(shardID)
		if err != nil {
			continue
		}
		if !info.IsLeader {
			continue
		}
		leaders = append(leaders, leaderObservation{
			index:     i,
			replicaID: info.ReplicaID,
			term:      info.Term,
			isLeader:  true,
		})
	}
	return leaders
}

// leaderTermGuard continuously asserts the Raft election-safety invariant:
// at most one leader may exist in a given term. It fails the test the INSTANT
// two distinct replicas report leadership of the same term — there is no
// convergence window, because two leaders in one term is never a transient
// state that resolves itself, it is a log-safety violation that has already
// happened.
//
// GetShardInfo returns (LeaderID, Term, IsLeader) as a seqlock-consistent
// triple, so a replica can never report leading a term it did not lead; a
// same-term pair from two replicas is therefore real and not a torn read.
type leaderTermGuard struct {
	t       *testing.T
	cluster *testCluster
	shardID uint64
	stopC   chan struct{}
	doneC   chan struct{}
	polls   atomic.Int64
	maxTerm atomic.Uint64
}

// startLeaderTermGuard launches the guard and registers its shutdown so that
// a test cannot finish without the invariant having been checked.
func startLeaderTermGuard(t *testing.T, cluster *testCluster, shardID uint64) *leaderTermGuard {
	t.Helper()
	g := &leaderTermGuard{
		t:       t,
		cluster: cluster,
		shardID: shardID,
		stopC:   make(chan struct{}),
		doneC:   make(chan struct{}),
	}
	go g.run()
	t.Cleanup(g.stop)
	return g
}

// run polls the cluster until stopped.
func (g *leaderTermGuard) run() {
	defer close(g.doneC)
	ticker := time.NewTicker(5 * time.Millisecond)
	defer ticker.Stop()
	for {
		g.check()
		select {
		case <-g.stopC:
			return
		case <-ticker.C:
		}
	}
}

// check performs one sweep and reports any same-term multi-leader sighting.
func (g *leaderTermGuard) check() {
	leaders := g.cluster.observeLeaders(g.shardID)
	g.polls.Add(1)

	byTerm := make(map[uint64][]leaderObservation, len(leaders))
	for _, obs := range leaders {
		if obs.term > g.maxTerm.Load() {
			g.maxTerm.Store(obs.term)
		}
		byTerm[obs.term] = append(byTerm[obs.term], obs)
	}
	for term, obs := range byTerm {
		if len(obs) < 2 {
			continue
		}
		var sb strings.Builder
		for _, o := range obs {
			fmt.Fprintf(&sb, " node[%d](replica=%d)", o.index, o.replicaID)
		}
		// Errorf, not Fatalf: this runs on the guard goroutine.
		g.t.Errorf("election safety violated on shard %d: %d leaders in term %d:%s",
			g.shardID, len(obs), term, sb.String())
	}
}

// stop halts the guard, runs one final sweep, and fails the test if the guard
// never actually observed the cluster — a guard that never polled would
// otherwise "pass" without checking anything.
func (g *leaderTermGuard) stop() {
	select {
	case <-g.stopC:
		return
	default:
	}
	close(g.stopC)
	<-g.doneC
	g.check()
	if polls := g.polls.Load(); polls < 2 {
		g.t.Errorf("leader/term guard for shard %d only completed %d polls; the election-safety invariant was not exercised",
			g.shardID, polls)
	}
	if g.maxTerm.Load() == 0 {
		g.t.Errorf("leader/term guard for shard %d never observed a leader in any term", g.shardID)
	}
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

// TestBudgetsDescribeTheClusterTheyBudgetFor pins the derivation beside the
// budgets to the configuration the suite actually starts.
//
// The constants are only worth having if the config builders keep reading
// them. If a future change gives testShardConfig an explicit ElectionRTT, or
// hands testHostConfig a different RTTMillisecond, electionRound silently
// starts describing a cluster that no longer exists and every budget built on
// it is wrong by that factor — which is exactly the drift the derivation
// replaced a literal to prevent. This is the only place that can notice.
func TestBudgetsDescribeTheClusterTheyBudgetFor(t *testing.T) {
	if got := testHostConfig(t, "127.0.0.1:1", nil).RTTMillisecond; got != testRTTMillisecond {
		t.Fatalf("testHostConfig sets RTTMillisecond=%d but the budgets are derived "+
			"from testRTTMillisecond=%d; one Raft tick is not what electionRound (%v) "+
			"thinks it is", got, uint64(testRTTMillisecond), electionRound)
	}
	if got := testShardConfig(1, 1).ElectionRTT; got != testElectionRTT {
		t.Fatalf("testShardConfig sets ElectionRTT=%d but the budgets are derived from "+
			"testElectionRTT=%d; electionRound (%v) and every budget built on it "+
			"are wrong by a factor of %.2f", got, testElectionRTT,
			electionRound, float64(got)/float64(testElectionRTT))
	}
	if electionRound != 2*time.Duration(testElectionRTT)*raftTick {
		t.Fatalf("electionRound=%v is not 2*ElectionRTT*tick", electionRound)
	}
	t.Logf("tick=%v electionRound=%v leaderElectionTimeout=%v (%d rounds)%s",
		raftTick, electionRound, leaderElectionTimeout, coldStartElectionRounds, fmt.Sprintf(" failoverTimeout=%v", failoverTimeout))
}
