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
	"errors"
	"fmt"
	"io"
	"io/fs"
	"log"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"strings"
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
// Engine Failure Detection
// ---------------------------------------------------------------------------

// engineFailures is the sink for engine-side faults observed while a test
// runs: a failed shard reported through EventListener.OnShardFailed, and a
// worker-goroutine panic that the engine's recover-and-restart loop absorbed.
//
// Neither reaches the test goroutine on its own. A failed shard is a callback
// on an engine worker, and a worker panic is recovered, logged and restarted
// by design so one bad shard cannot take the host down. That is right for a
// library and wrong for a test: the shard the test was driving is dead or
// wedged, so every wait the test is sitting in runs to its full timeout, then
// the test itself runs to `panic: test timed out` -- around thirty minutes of
// CI, with the actual panic scrolled far above the failure and no shard named
// at the point of failure.
//
// So the fault is parked here by whichever goroutine sees it, and the polling
// helpers below check the sink on every tick and fail on the TEST goroutine,
// where t.Fatal is legal. This is the same trade the apply-order sink above
// makes and for the same reason: the reporting goroutine has no *testing.T.
//
// The fix deliberately lives here and not in the engine. Restart-on-panic is
// the engine's chosen production behaviour and is covered by its own unit
// tests; a test harness that cannot see the restart is a harness defect.
var engineFailures struct {
	mu    sync.Mutex
	first string
	// armedBy is the test the sink currently belongs to, so arming twice for
	// one test does not discard a fault already recorded for it.
	armedBy *testing.T
}

// recordEngineFailure parks the FIRST engine fault of a test. Later faults are
// dropped: a wedged shard cascades (the restarted worker panics again on the
// same state, the shard fails, its peers report connection failures), and the
// first one is the one that explains the rest.
func recordEngineFailure(format string, args ...interface{}) {
	msg := fmt.Sprintf(format, args...)
	engineFailures.mu.Lock()
	if engineFailures.first == "" {
		engineFailures.first = msg
	}
	engineFailures.mu.Unlock()
}

// armEngineFailureDetector binds the engine-failure sink to t and routes the
// engine's own slog output through a watcher for the duration of the test. It
// is idempotent for a given t.
//
// Watching the log output is what catches a worker panic. The panic is
// recovered inside the engine and never becomes an error any Host API returns;
// the only evidence that escapes the process is the Error-level "… panic
// recovered …" record carrying the panic value and its stack. An invariant
// violation (invariant.Assert) is exactly this shape: it panics on an engine
// worker, the worker restarts, and nothing else in the process changes.
//
// The interception is on log.SetOutput rather than slog.SetDefault, which is
// NOT a stylistic choice. The engine logs through the package-level slog
// functions, and the default slog handler formats through the standard log
// package. Installing a wrapper via slog.SetDefault ALSO redirects the log
// package into that same wrapper (slog.SetDefault does both), so a record
// forwarded to the original default handler re-enters the wrapper from
// underneath while log's mutex is held, and the first engine panic deadlocks
// the whole binary on it — a self-inflicted version of the hang this detector
// exists to prevent. Tapping the writer sits below all of that: log holds its
// mutex across the Write, the tap only appends to its own buffer, and nothing
// re-enters log.
func armEngineFailureDetector(t *testing.T) {
	t.Helper()

	engineFailures.mu.Lock()
	if engineFailures.armedBy == t {
		engineFailures.mu.Unlock()
		return
	}
	engineFailures.armedBy = t
	engineFailures.first = ""
	engineFailures.mu.Unlock()

	prev := log.Writer()
	log.SetOutput(&panicWatchWriter{inner: prev})
	t.Cleanup(func() {
		log.SetOutput(prev)
		engineFailures.mu.Lock()
		engineFailures.first = ""
		engineFailures.armedBy = nil
		engineFailures.mu.Unlock()
	})
}

// panicWatchWriter passes every log line through untouched and, in addition,
// parks the ones reporting a recovered panic. Every recover-and-continue site
// in the engine and the host words it "… panic recovered …" — step, commit and
// apply workers, the apply dispatcher, the snapshot worker, the event-listener
// dispatch and the state machine — and no healthy integration run emits one.
type panicWatchWriter struct {
	inner io.Writer
}

func (w *panicWatchWriter) Write(p []byte) (int, error) {
	if bytes.Contains(p, []byte("panic recovered")) {
		recordEngineFailure("engine absorbed a panic and restarted the worker\n    %s",
			trimPanicLogLine(string(p)))
	}
	return w.inner.Write(p)
}

// trimPanicLogLine cuts the recovered panic's log line down to the part that
// names the fault. The record carries the whole stack as one quoted attribute,
// which is thousands of characters of one line and unreadable inside a test
// failure; the full text is still in the log the writer forwarded, so what
// belongs in the failure message is the message, the worker and the panic
// value that precede it.
func trimPanicLogLine(line string) string {
	line = strings.TrimRight(line, "\n")
	if i := strings.Index(line, " stack="); i >= 0 {
		line = line[:i] + " (stack in the log line above)"
	}
	return line
}

// failFastOnEngineFailure fails the running test if the engine reported a
// failed shard or absorbed a worker panic. Every polling helper in this file
// calls it on each tick, so a fault that would otherwise be waited out for the
// full timeout -- and then for the whole test binary's timeout -- surfaces
// within one poll interval, quoting the original panic and naming the shard
// whenever the engine named one (OnShardFailed does; a worker panic identifies
// the worker, since a step worker owns a partition rather than one shard).
func failFastOnEngineFailure(t *testing.T) {
	t.Helper()
	engineFailures.mu.Lock()
	first := engineFailures.first
	engineFailures.mu.Unlock()
	if first == "" {
		return
	}
	t.Fatalf("aborting: the engine failed under this test, so no further "+
		"progress is possible and waiting can only burn the timeout\n  %s", first)
}

// ---------------------------------------------------------------------------
// KV State Machine
// ---------------------------------------------------------------------------

// kvStateMachine is a simple in-memory key-value store that implements
// sm.StateMachine for E2E testing. It supports put and get operations
// and full snapshot save/recover.
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
	// recovers counts RecoverFromSnapshot calls. It is how a test can tell
	// "this replica caught up by receiving a SNAPSHOT" from "this replica
	// caught up by replaying the LOG" -- two paths that leave identical data
	// behind, so every assertion phrased over the resulting key set is
	// satisfied by either one. Several tests logged the snapshot claim while
	// checking nothing that could distinguish them.
	recovers atomic.Uint64
}

// RecoverCount returns how many times this state machine has been restored
// from a snapshot.
func (kv *kvStateMachine) RecoverCount() uint64 {
	return kv.recovers.Load()
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

	kv.recovers.Add(1)

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

// forShard returns the one state machine this tracker holds for shardID,
// regardless of replica ID, or nil when there is not exactly one.
//
// A tracker belongs to exactly one host and a host runs exactly one replica of
// a given shard, so the lookup is unambiguous. It exists because the caller's
// "replicaID is nodeIndex+1" convention stops holding the moment a test
// replaces a node with a fresh replica ID — and a lookup that misses then
// returns nil, which the old helpers read as "nothing to verify here".
func (t *smTracker) forShard(shardID uint64) *kvStateMachine {
	t.mu.Lock()
	defer t.mu.Unlock()
	var found *kvStateMachine
	for k, machine := range t.machines {
		if k.shardID != shardID {
			continue
		}
		if found != nil {
			return nil
		}
		found = machine
	}
	return found
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
	assertApplyOrder(t)
	armEngineFailureDetector(t)
	return config.HostConfig{
		// OnShardFailed is the engine's only outward report that a shard is
		// finished: after it fires, that replica makes no further progress
		// without an operator StopShard/StartShard. A test waiting on the
		// shard therefore cannot succeed, and every wait it is in should
		// abort now rather than run out its timeout. See engineFailures.
		EventListener: &config.EventListener{
			OnShardFailed: func(info config.ShardFailedInfo) {
				recordEngineFailure("shard %d replica %d failed: %v",
					info.ShardID, info.ReplicaID, info.Err)
			},
		},
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
	configs  []config.HostConfig
	trackers []*smTracker
	stopped  []atomic.Bool // tracks which nodes have been stopped
	mtls     *config.MTLSConfig
	t        *testing.T
}

// nodeHostDir returns the NodeHostDir the host at idx was constructed with. It
// is the root under which that node's snapshot artifacts land, so a test can
// inspect what was actually written to disk rather than trusting a round trip.
func (c *testCluster) nodeHostDir(idx int) string {
	c.hostsMu.RLock()
	defer c.hostsMu.RUnlock()
	if idx < 0 || idx >= len(c.configs) {
		c.t.Fatalf("nodeHostDir: no recorded config for node[%d] (have %d)", idx, len(c.configs))
	}
	return c.configs[idx].NodeHostDir
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
		configs[i] = cfg
	}

	c := &testCluster{
		hosts:   hosts,
		addrs:   addrs,
		configs: configs,
		stopped: make([]atomic.Bool, numNodes),
		mtls:    mtls,
		t:       t,
	}
	setDiagCluster(t, c)
	return c
}

// diagCluster holds the cluster most recently built by newTestCluster, so a
// wait that expires can print the raft state behind it.
//
// A package-level handle rather than a parameter threaded through the twenty
// call sites of waitForNodeData: this is the same trade the apply-order sink in
// this package already makes, and for the same reason — integration tests here
// never run in parallel, so "the current cluster" is unambiguous. The Cleanup
// clears it so a later test can never print a torn-down cluster's state.
var diagCluster struct {
	mu sync.Mutex
	c  *testCluster
}

func setDiagCluster(t *testing.T, c *testCluster) {
	diagCluster.mu.Lock()
	diagCluster.c = c
	diagCluster.mu.Unlock()
	t.Cleanup(func() {
		diagCluster.mu.Lock()
		if diagCluster.c == c {
			diagCluster.c = nil
		}
		diagCluster.mu.Unlock()
	})
}

// shardStateDump renders each live node's raft state for one shard: who it
// believes the leader is, its term, how far it has applied, what its local log
// spans and how much is queued. A node still catching up, a node that disagrees
// about the leader, and a node whose log stops short all look different here.
//
// It never fails the test — it runs while one is already failing, so an error
// on one node is reported inline and the rest still print.
func shardStateDump(shardID uint64) string {
	diagCluster.mu.Lock()
	c := diagCluster.c
	diagCluster.mu.Unlock()
	if c == nil {
		return "shard state: no cluster registered"
	}

	var b strings.Builder
	fmt.Fprintf(&b, "shard %d state at failure:\n", shardID)

	c.hostsMu.RLock()
	hosts := make([]*quicraft.Host, len(c.hosts))
	copy(hosts, c.hosts)
	c.hostsMu.RUnlock()

	for i, h := range hosts {
		if h == nil || c.stopped[i].Load() {
			fmt.Fprintf(&b, "  node[%d]: stopped\n", i)
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
		failFastOnEngineFailure(c.t)
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
// Leader/Term Safety
// ---------------------------------------------------------------------------

// leaderObservation is one poll sample of one host's view of a shard.
type leaderObservation struct {
	index     int
	replicaID uint64
	term      uint64
}

// observeLeaders samples (node, IsLeader, Term) from EVERY host currently in
// the cluster and returns the leaders it found.
//
// It deliberately does NOT consult c.stopped. A node this harness believes is
// down but which is still running — or one that was restarted and re-added —
// is exactly where a second leader hides, and skipping it is how a same-term
// split brain becomes invisible. A host that is genuinely closed returns an
// error from GetShardInfo and contributes nothing.
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
		if err != nil || !info.IsLeader {
			continue
		}
		leaders = append(leaders, leaderObservation{
			index:     i,
			replicaID: info.ReplicaID,
			term:      info.Term,
		})
	}
	return leaders
}

// splitBrain returns a description of a same-term multi-leader sighting, or the
// empty string when the observations are consistent with election safety.
//
// Two leaders in DIFFERENT terms is a legal, ordinary handover: the old leader
// simply has not learned it was superseded. Two leaders in the SAME term is the
// Raft election-safety violation, and it is never transient — it has already
// happened by the time it is observed, so it must never be waited out.
func splitBrain(leaders []leaderObservation) string {
	byTerm := make(map[uint64][]leaderObservation, len(leaders))
	for _, obs := range leaders {
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
		return fmt.Sprintf("%d leaders in term %d:%s", len(obs), term, sb.String())
	}
	return ""
}

// waitForSingleLeader polls until exactly one host reports leadership of the
// shard, checking the election-safety invariant on EVERY iteration.
//
// This replaces the older "poll until the leader count is 1" shape, which
// treated two simultaneous leaders as an intermediate state to wait out. That
// IS the violation: waiting for it to resolve turns the one condition the test
// exists to detect into a reason to keep polling. It also never read Term, so a
// same-term split brain was indistinguishable from a legal handover, and it
// skipped stopped nodes, so a partitioned node holding a second claim was
// invisible.
func waitForSingleLeader(t *testing.T, cluster *testCluster, shardID uint64, timeout time.Duration) int {
	t.Helper()

	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	deadline := time.NewTimer(timeout)
	defer deadline.Stop()

	var last []leaderObservation
	for {
		failFastOnEngineFailure(t)
		last = cluster.observeLeaders(shardID)
		if violation := splitBrain(last); violation != "" {
			t.Fatalf("election safety violated on shard %d: %s", shardID, violation)
			return -1
		}
		if len(last) == 1 {
			return last[0].index
		}
		select {
		case <-deadline.C:
			t.Fatalf("timed out after %v waiting for exactly one leader on shard %d; observed %d leader(s): %v",
				timeout, shardID, len(last), last)
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
		failFastOnEngineFailure(t)
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
		failFastOnEngineFailure(t)
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
		failFastOnEngineFailure(t)
		if predicate() {
			return
		}
		select {
		case <-deadline.C:
			t.Fatalf("timed out after %v waiting for condition: %s\n%s", timeout, desc, diag())
			return
		case <-ticker.C:
		}
	}
}

func waitForCondition(t *testing.T, timeout time.Duration, desc string, predicate func() bool) {
	t.Helper()
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	deadline := time.NewTimer(timeout)
	defer deadline.Stop()
	for {
		failFastOnEngineFailure(t)
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

// resolveStateMachine returns the state machine for node i on the given shard.
//
// The cluster's own tracker slice is consulted FIRST because it is the live
// one: addNodeToCluster appends to it and recovery paths overwrite entries in
// it, while a trackers slice a test captured from startShard is a snapshot
// from before those changes and points at state machines that no longer run.
// The caller's slice is the fallback, which covers a shard whose trackers were
// never stored on the cluster.
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
	// replicaID == i+1 stops holding once a test replaces a node with a fresh
	// replica ID, so fall back to the tracker's single machine for the shard.
	// Without this the lookup returns nil for a node that is running perfectly
	// well, and every caller reads that as "no state machine to check".
	if i < len(live) && live[i] != nil {
		if machine := live[i].forShard(shardID); machine != nil {
			return machine
		}
	}
	if i < len(trackers) && trackers[i] != nil {
		if machine := trackers[i].forShard(shardID); machine != nil {
			return machine
		}
	}
	return nil
}

// waitForReplication waits for a key to appear with the expected value in the
// state machine of EVERY live node, and fails if it did not actually check
// every one of them.
//
// The count is the point. The previous version skipped any node whose index
// was past the end of the caller's trackers slice and any node whose state
// machine had not registered yet, silently and without a word in the log. A
// test that adds a replacement node and then "verifies data intact on all live
// nodes" verified nothing at all for that node, because the trackers slice was
// captured before the node existed.
func waitForReplication(t *testing.T, cluster *testCluster, trackers []*smTracker, shardID uint64, key, expectedVal string, timeout time.Duration) {
	t.Helper()

	cluster.hostsMu.RLock()
	numNodes := len(cluster.hosts)
	cluster.hostsMu.RUnlock()

	live := make([]int, 0, numNodes)
	for i := range numNodes {
		if !cluster.stopped[i].Load() {
			live = append(live, i)
		}
	}
	if len(live) == 0 {
		t.Fatalf("waitForReplication(key=%q): no live nodes to verify", key)
	}

	verified := 0
	for _, i := range live {
		machine := resolveStateMachine(cluster, trackers, shardID, i)
		if machine == nil {
			// The shard may not have registered its state machine yet; that
			// is a condition to wait for, not one to skip.
			waitForCondition(t, timeout,
				fmt.Sprintf("node[%d] registers a state machine for shard %d", i, shardID),
				func() bool {
					machine = resolveStateMachine(cluster, trackers, shardID, i)
					return machine != nil
				},
			)
		}
		waitForCondition(t, timeout,
			fmt.Sprintf("node[%d] replicates key=%q", i, key),
			func() bool {
				val, ok := machine.Get(key)
				return ok && val == expectedVal
			},
		)
		verified++
	}

	if verified < len(live) {
		t.Fatalf("waitForReplication(key=%q): verified %d nodes but %d are live",
			key, verified, len(live))
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

	// Request membership addition from whichever node is currently the leader,
	// retrying a re-election rather than failing on it.
	//
	// h is closed on the failure path via defer: syncAddNode calls t.Fatalf,
	// which runs Goexit and would otherwise leak the host just constructed.
	added := false
	defer func() {
		if !added {
			h.Close()
		}
	}()
	syncAddNode(t, cluster, shardID, newReplicaID, addr, membershipTimeout)
	added = true

	// Start the shard in join mode on the new host.
	createFn, tracker := kvCreateFunc()
	scfg := cfgFn(shardID, newReplicaID)
	if err := h.StartShard(nil, true, createFn, scfg); err != nil {
		added = false
		t.Fatalf("StartShard(join=true) for replica=%d failed: %v", newReplicaID, err)
	}

	// Append to cluster tracking slices.
	cluster.hostsMu.Lock()
	idx := len(cluster.hosts)
	cluster.hosts = append(cluster.hosts, h)
	cluster.addrs = append(cluster.addrs, addr)
	cluster.configs = append(cluster.configs, hostCfg)
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

// syncAddNode adds a voting replica through whichever node is CURRENTLY the
// leader, retrying transient leadership errors until it succeeds or timeout.
//
// The membership calls used to be issued against a leader index sampled once,
// then treated ErrNotLeader as fatal. A re-election between the sample and the
// call is ordinary Raft behaviour, so the test failed with "not leader, leader
// unknown" for a membership change that was never actually refused. The
// assertion is unchanged in strength: the change must still succeed, and it
// still fails hard on a non-transient error or on running out of time.
func syncAddNode(t *testing.T, cluster *testCluster, shardID, replicaID uint64, addr string, timeout time.Duration) {
	t.Helper()
	syncMembershipChange(t, cluster, shardID, timeout,
		fmt.Sprintf("SyncRequestAddNode(replica=%d)", replicaID),
		func(ctx context.Context, h *quicraft.Host) error {
			return h.SyncRequestAddNode(ctx, shardID, replicaID, addr, 0)
		})
}

// syncDeleteNode removes a replica through whichever node is CURRENTLY the
// leader, retrying transient leadership errors. See syncAddNode.
func syncDeleteNode(t *testing.T, cluster *testCluster, shardID, replicaID uint64, timeout time.Duration) {
	t.Helper()
	syncMembershipChange(t, cluster, shardID, timeout,
		fmt.Sprintf("SyncRequestDeleteNode(replica=%d)", replicaID),
		func(ctx context.Context, h *quicraft.Host) error {
			return h.SyncRequestDeleteNode(ctx, shardID, replicaID, 0)
		})
}

// syncMembershipChange runs fn against the current leader until it succeeds,
// retrying only transient leadership errors.
func syncMembershipChange(
	t *testing.T,
	cluster *testCluster,
	shardID uint64,
	timeout time.Duration,
	desc string,
	fn func(context.Context, *quicraft.Host) error,
) {
	t.Helper()

	deadline := time.Now().Add(timeout)
	var lastErr error
	for time.Now().Before(deadline) {
		leaderHost := cluster.leaderHost(shardID)
		if leaderHost == nil {
			lastErr = quicraft.ErrNotLeader
			timer := time.NewTimer(50 * time.Millisecond)
			<-timer.C
			timer.Stop()
			continue
		}

		ctx, cancel := context.WithTimeout(context.Background(), membershipTimeout)
		err := fn(ctx, leaderHost)
		cancel()
		if err == nil {
			return
		}
		if !isTransientLeadershipError(err) {
			t.Fatalf("%s failed: %v", desc, err)
			return
		}
		lastErr = err
		timer := time.NewTimer(50 * time.Millisecond)
		<-timer.C
		timer.Stop()
	}
	t.Fatalf("%s never succeeded within %v: %v", desc, timeout, lastErr)
}

// proposeUntilCommitted proposes one key/value through whichever node is
// CURRENTLY the leader, retrying transient leadership errors until the entry
// commits or the deadline passes. It returns nil on success and an error the
// caller should treat as fatal otherwise.
//
// It exists for the concurrent-writer goroutines, which pinned the leader index
// captured at the start of the test and treated every error — including
// ErrNotLeader from a perfectly ordinary re-election — as a hard failure. That
// is not a weaker assertion than "no errors": every entry must still commit,
// and the caller still verifies all of them replicated afterwards. It only
// stops a legal leadership change from being reported as a lost write.
//
// Safe to call from a non-test goroutine: it never touches t.
func proposeUntilCommitted(cluster *testCluster, shardID uint64, key, value string, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	cmd := encodeKVPut(key, value)
	var lastErr error

	for time.Now().Before(deadline) {
		leaderHost := cluster.leaderHost(shardID)
		if leaderHost == nil {
			lastErr = quicraft.ErrNotLeader
			timer := time.NewTimer(20 * time.Millisecond)
			<-timer.C
			timer.Stop()
			continue
		}

		ctx, cancel := context.WithTimeout(context.Background(), defaultTimeout)
		rs, err := leaderHost.Propose(ctx, shardID, cmd)
		if err != nil {
			cancel()
			if !isTransientLeadershipError(err) {
				return fmt.Errorf("propose %q: %w", key, err)
			}
			lastErr = err
			timer := time.NewTimer(20 * time.Millisecond)
			<-timer.C
			timer.Stop()
			continue
		}

		_, resErr := rs.Result()
		rs.Release()
		cancel()
		if resErr == nil {
			return nil
		}
		if !isTransientLeadershipError(resErr) {
			return fmt.Errorf("commit %q: %w", key, resErr)
		}
		lastErr = resErr
		timer := time.NewTimer(20 * time.Millisecond)
		<-timer.C
		timer.Stop()
	}
	return fmt.Errorf("%q never committed within %v: %w", key, timeout, lastErr)
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

// verifyAllData verifies that every KV pair in expected is present on the state
// machine of EVERY live node, and fails if it did not actually check every one
// of them.
//
// The count is the point, and it is the same point as in waitForReplication.
// The previous version skipped three separate classes of node in silence: any
// index past the end of the caller's trackers slice (which is every node a test
// added after capturing it), any nil tracker entry, and any node whose state
// machine lookup returned nil. That last one is the damaging case: the tracker
// records a machine the instant the engine invokes the state-machine factory,
// so a nil result means the factory was never invoked for that replica — a
// startup or recovery failure. Skipping it removes the one broken node from
// every data assertion while the caller logs "verified on N nodes". This helper
// is the sole final assertion of the rolling-restart tests, where the node that
// failed to come back is exactly the node being skipped.
func verifyAllData(
	t *testing.T,
	cluster *testCluster,
	trackers []*smTracker,
	shardID uint64,
	expected map[string]string,
) {
	t.Helper()

	cluster.hostsMu.RLock()
	numNodes := len(cluster.hosts)
	cluster.hostsMu.RUnlock()

	live := make([]int, 0, numNodes)
	for i := range numNodes {
		if !cluster.stopped[i].Load() {
			live = append(live, i)
		}
	}
	if len(live) == 0 {
		t.Fatalf("verifyAllData(shard=%d): no live nodes to verify", shardID)
	}

	verified := 0
	for _, i := range live {
		nodeSM := resolveStateMachine(cluster, trackers, shardID, i)
		if nodeSM == nil {
			// A missing state machine is a condition to WAIT for and then
			// FAIL on, never one to skip.
			waitForCondition(t, membershipTimeout,
				fmt.Sprintf("node[%d] registers a state machine for shard %d", i, shardID),
				func() bool {
					nodeSM = resolveStateMachine(cluster, trackers, shardID, i)
					return nodeSM != nil
				},
			)
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
		verified++
	}

	if verified < len(live) {
		t.Fatalf("verifyAllData(shard=%d): verified %d nodes but %d are live",
			shardID, verified, len(live))
	}
}

// assertCaughtUpViaSnapshot fails unless the replica actually restored itself
// from a SNAPSHOT rather than replaying the log.
//
// Holding the right keys proves nothing about which path produced them: log
// replay and snapshot install leave byte-identical state, so "the joiner has
// the keys" is satisfied by either. Several tests logged "caught up via
// snapshot transfer (not log replay)" while asserting only the key set. The
// state machine counts its own RecoverFromSnapshot calls, which is the one
// signal that separates the two.
func assertCaughtUpViaSnapshot(t *testing.T, tracker *smTracker, shardID, replicaID uint64, timeout time.Duration) {
	t.Helper()

	nodeSM := tracker.Get(shardID, replicaID)
	if nodeSM == nil {
		waitForCondition(t, timeout,
			fmt.Sprintf("replica=%d registers a state machine", replicaID),
			func() bool {
				nodeSM = tracker.Get(shardID, replicaID)
				return nodeSM != nil
			},
		)
	}
	waitForCondition(t, timeout,
		fmt.Sprintf("replica=%d recovers from a snapshot rather than replaying the log", replicaID),
		func() bool { return nodeSM.RecoverCount() > 0 },
	)
	t.Logf("replica=%d restored from a snapshot %d time(s)", replicaID, nodeSM.RecoverCount())
}

// assertSnapshotTaken fails unless the node at idx has written at least one
// snapshot artifact under its NodeHostDir. It is the companion to
// assertCaughtUpViaSnapshot on the SENDING side: a snapshot has to exist before
// one can be transferred.
func assertSnapshotTaken(t *testing.T, cluster *testCluster, idx int) {
	t.Helper()
	dir := cluster.nodeHostDir(idx)
	paths := waitForSnapshotArtifact(t, fmt.Sprintf("node[%d]", idx), dir, membershipTimeout)
	t.Logf("node[%d] wrote %d snapshot artifact(s) under %s", idx, len(paths), dir)
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
		waitForConditionDiag(t, timeout,
			fmt.Sprintf("replica=%d has key=%q", replicaID, key),
			func() bool {
				got, ok := nodeSM.Get(key)
				return ok && got == val
			},
			func() string { return shardStateDump(shardID) },
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
	barriers []*countingBarrier
}

// ---------------------------------------------------------------------------
// Counting Barrier
// ---------------------------------------------------------------------------

// countingBarrier wraps a seal.Barrier and counts the frames the SNAPSHOT
// pipeline seals through it, so a test can assert that at-rest encryption was
// actually INVOKED rather than merely configured.
//
// It exists because the only thing the barrier tests used to assert was
// IsSealed() == false, an in-memory flag that no production code path touches.
// A total plaintext downgrade — the engine resolving no barrier for the shard
// and writing the snapshot in the clear — leaves that flag exactly as it was.
//
// EncryptWithEpochPinnedContext and EncryptWithEpochContext are the only two
// entry points the snapshot encrypt loop uses: engine.epochPinner.sealFrame
// takes the pinned one when the barrier offers it (seal.Barrier does) and the
// context one otherwise. Neither is reachable from the WAL, which seals records
// through EncryptWithEpoch/EncryptWithContext, so the count is specific to
// snapshot frames and cannot be satisfied by ordinary log traffic.
//
// The overriding methods shadow the promoted ones, including for the dynamic
// type assertions the engine performs (atomicEpochPinner, walContextBarrier).
type countingBarrier struct {
	*seal.Barrier
	frameSeals atomic.Int64
}

// EncryptWithEpochPinnedContext seals one snapshot frame and counts it.
func (b *countingBarrier) EncryptWithEpochPinnedContext(dst, plaintext, contextAAD []byte) ([]byte, uint64, error) {
	ct, epoch, err := b.Barrier.EncryptWithEpochPinnedContext(dst, plaintext, contextAAD)
	if err == nil {
		b.frameSeals.Add(1)
	}
	return ct, epoch, err
}

// EncryptWithEpochContext seals one snapshot frame on the non-atomic pinning
// path and counts it.
func (b *countingBarrier) EncryptWithEpochContext(dst, plaintext, contextAAD []byte) ([]byte, uint64, error) {
	ct, epoch, err := b.Barrier.EncryptWithEpochContext(dst, plaintext, contextAAD)
	if err == nil {
		b.frameSeals.Add(1)
	}
	return ct, epoch, err
}

// snapshotFrameSeals reports how many snapshot frames this barrier has sealed.
func (b *countingBarrier) snapshotFrameSeals() int64 {
	return b.frameSeals.Load()
}

// Compile-time assertion that the wrapper is still a usable host barrier.
var _ quicraft.Barrier = (*countingBarrier)(nil)

// testBarrierConfig returns a seal.BarrierConfig carrying a no-op
// StatePersister. seal.Barrier.Rotate and PurgeEpochsBefore fail closed with
// seal.ErrNoStatePersister when the barrier cannot durably persist its sealed
// root-key state, so every test barrier that rotates needs one.
func testBarrierConfig(deploymentID uint64) seal.BarrierConfig {
	cfg := seal.DefaultBarrierConfig(deploymentID)
	cfg.StatePersister = func(*seal.SealedRootKey) error { return nil }
	return cfg
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
	configs := make([]config.HostConfig, numNodes)
	barriers := make([]*countingBarrier, numNodes)

	for i := range numNodes {
		// Create and initialize a barrier for this node.
		_ = seal.AllowWeakKDFParams()
		b := &countingBarrier{Barrier: seal.MustNewBarrier(testBarrierConfig(1))}
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
		var cfg config.HostConfig
		for attempt := range maxRetries {
			addr = freePort(t)
			cfg = testHostConfig(t, addr, mtls)
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
		configs[i] = cfg
	}

	tc := &testCluster{
		hosts:   hosts,
		addrs:   addrs,
		configs: configs,
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
func newBarrierForNode(t *testing.T) *countingBarrier {
	t.Helper()
	_ = seal.AllowWeakKDFParams()
	b := &countingBarrier{Barrier: seal.MustNewBarrier(testBarrierConfig(1))}
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

// ---------------------------------------------------------------------------
// At-Rest Snapshot Artifact Assertions
// ---------------------------------------------------------------------------

// snapshotDataFileName is the payload file the engine writes inside each
// snapshot directory (<snapshotDir>/shard-N/replica-N/snapshot-<index>/).
// It is duplicated here rather than exported from the engine because a test
// that reads the constant from the code under test cannot notice the code
// writing somewhere else entirely.
const snapshotDataFileName = "snapshot.dat"

// snapshotArtifacts returns every snapshot payload file beneath dir. The whole
// tree is walked rather than the documented path being reconstructed so the
// search still finds the artifacts if the layout changes.
func snapshotArtifacts(t *testing.T, dir string) []string {
	t.Helper()
	var found []string
	err := filepath.WalkDir(dir, func(path string, d fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
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

// waitForSnapshotArtifact blocks until at least one snapshot payload file
// exists beneath dir, and fails if none appears. Snapshots are written
// asynchronously by the snapshot pool, so the artifact lags the request.
func waitForSnapshotArtifact(t *testing.T, label, dir string, timeout time.Duration) []string {
	t.Helper()
	var paths []string
	waitForCondition(t, timeout,
		fmt.Sprintf("%s writes a snapshot artifact under %s", label, dir),
		func() bool {
			paths = snapshotArtifacts(t, dir)
			return len(paths) > 0
		},
	)
	return paths
}

// assertSnapshotsSealed fails unless dir holds at least one snapshot artifact
// AND no artifact contains any of the plaintext needles.
//
// This is deliberately a check of the BYTES ON DISK rather than a round trip.
// A round trip through the engine reads back whatever it wrote, so it passes
// identically whether the artifact was sealed or written in the clear — which
// is exactly the plaintext downgrade at-rest encryption exists to prevent. The
// non-empty artifact count is part of the assertion: a search that finds no
// files to search is vacuous, not a pass.
func assertSnapshotsSealed(t *testing.T, label, dir string, needles []string) {
	t.Helper()
	if len(needles) == 0 {
		t.Fatalf("%s: assertSnapshotsSealed called with no plaintext needles", label)
	}
	paths := waitForSnapshotArtifact(t, label, dir, membershipTimeout)
	for _, p := range paths {
		data, err := os.ReadFile(p)
		if err != nil {
			t.Fatalf("%s: reading snapshot artifact %s: %v", label, p, err)
		}
		if len(data) == 0 {
			t.Fatalf("%s: snapshot artifact %s is empty", label, p)
		}
		for _, needle := range needles {
			if bytes.Contains(data, []byte(needle)) {
				t.Fatalf("%s: snapshot artifact %s (%d bytes) contains the plaintext "+
					"bytes %q; the at-rest barrier did not encrypt it",
					label, p, len(data), needle)
			}
		}
	}
	t.Logf("%s: %d snapshot artifact(s) under %s, none containing plaintext", label, len(paths), dir)
}

// requestSnapshotSync asks host for an explicit snapshot of shardID and WAITS
// for the result, returning the snapshot index.
//
// Reading the result is the point: RequestSnapshot is asynchronous, so a caller
// that Release()s the state without reading it has asserted nothing about
// whether a snapshot was taken, only that one was asked for. An automatic
// snapshot may already be running, which is the one legitimate transient
// failure, so that case retries; everything else fails immediately.
func requestSnapshotSync(t *testing.T, host *quicraft.Host, shardID uint64) uint64 {
	t.Helper()

	const maxSnapRetries = 20
	const snapRetryInterval = 500 * time.Millisecond

	for attempt := range maxSnapRetries {
		ctx, cancel := context.WithTimeout(context.Background(), defaultTimeout)
		rs, err := host.RequestSnapshot(ctx, shardID, quicraft.SnapshotOption{})
		cancel()
		if err == nil {
			result, resultErr := rs.Result()
			rs.Release()
			if resultErr != nil {
				t.Fatalf("RequestSnapshot(shard=%d) result failed: %v", shardID, resultErr)
			}
			if result.Value == 0 {
				t.Fatalf("RequestSnapshot(shard=%d) completed at index 0; no snapshot was taken", shardID)
			}
			return result.Value
		}
		if errors.Is(err, quicraft.ErrSnapshotInProgress) {
			timer := time.NewTimer(snapRetryInterval)
			<-timer.C
			timer.Stop()
			continue
		}
		t.Fatalf("RequestSnapshot(shard=%d) failed on attempt %d: %v", shardID, attempt+1, err)
	}
	t.Fatalf("RequestSnapshot(shard=%d) never completed: a snapshot was in progress for %v",
		shardID, time.Duration(maxSnapRetries)*snapRetryInterval)
	return 0
}
