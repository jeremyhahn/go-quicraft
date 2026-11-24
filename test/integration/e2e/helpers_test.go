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
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"log"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	quicraft "github.com/jeremyhahn/go-quicraft/pkg"
	"github.com/jeremyhahn/go-quicraft/pkg/config"
	"github.com/jeremyhahn/go-quicraft/pkg/seal"
	"github.com/jeremyhahn/go-quicraft/pkg/sm"
	"github.com/jeremyhahn/go-quicraft/test/electionbudget"
	"github.com/jeremyhahn/go-quicraft/test/membership"
	"github.com/jeremyhahn/go-quicraft/test/proposer"
	"github.com/jeremyhahn/go-quicraft/test/testpki"
)

// Raft timing and election budgets, derived from the shard configuration these tests start and pinned by TestBudgetsDescribeTheClusterTheyBudgetFor.

// defaultTimeout is the standard budget for a single Raft operation that is
// NOT an election: a proposal round trip, a lookup, a membership query. 15s
// gives headroom for QUIC connection establishment under Docker/CI resource
// contention. It is deliberately not expressed in election rounds, because
// none of its callers is waiting for an election.
const defaultTimeout = 15 * time.Second

// testRTTMillisecond is the tick in milliseconds testHostConfig gives every host: 50ms, or 500ms in WAL-sync mode.
var testRTTMillisecond = harnessRTTMillisecond(walSyncMode)

// walSyncEnv names the switch that selects the harness's WAL-sync mode. With
// QUICRAFT_IT_WAL_SYNC=1 every host fdatasyncs its WAL before an ack, as
// production does (make integration-test-walsync runs the Partition cases
// so); unset, the harness skips the fsync (NoSyncWAL).
const walSyncEnv = "QUICRAFT_IT_WAL_SYNC"

// noSyncRTTMillisecond is the tick of the default, NoSyncWAL mode.
const noSyncRTTMillisecond = 50

// walSyncRTTMillisecond is the tick of WAL-sync mode. A real fdatasync on a
// shared, contended disk was measured to stall 2.5-4.6s, and Raft persists
// term and vote before any vote leaves the node, so the shortest election
// timeout, testElectionRTT ticks, must outlast that stall: 10 ticks of 500ms
// is 5s. TestBudgetsDescribeTheClusterTheyBudgetFor pins it.
const walSyncRTTMillisecond = 500

// walSyncStallCeiling is the longest fdatasync stall measured on CI hosts.
const walSyncStallCeiling = 4600 * time.Millisecond

// walSyncMode is the mode this run of the suite is in, read once from the
// environment.
var walSyncMode = harnessWALSync(os.Getenv)

// harnessWALSync reports whether getenv selects WAL-sync mode.
func harnessWALSync(getenv func(string) string) bool {
	return getenv(walSyncEnv) == "1"
}

// harnessRTTMillisecond is the Raft tick, in milliseconds, of a harness mode.
func harnessRTTMillisecond(walSync bool) uint64 {
	if walSync {
		return walSyncRTTMillisecond
	}
	return noSyncRTTMillisecond
}

// testElectionRTT is the election timeout in ticks, read from Config.SetDefaults.
const testElectionRTT = config.DefaultElectionRTT

// raftTick is one Raft tick in wall-clock time.
var raftTick = time.Duration(testRTTMillisecond) * time.Millisecond

// Election budgets from test/electionbudget, derived from testElectionRTT and testRTTMillisecond.
var (
	electionRound         = electionbudget.Round(testElectionRTT, testRTTMillisecond)
	leaderElectionTimeout = electionbudget.LeaderElection(testElectionRTT, testRTTMillisecond)
	failoverTimeout       = electionbudget.Failover(testElectionRTT, testRTTMillisecond)
)

// coldStartElectionRounds is re-exposed under its long-standing local name so
// the suites' log lines keep naming the round count.
const coldStartElectionRounds = electionbudget.ColdStartRounds

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

// engineFailures collects engine faults (OnShardFailed, recovered worker panics) so polling helpers fail on the test goroutine.
var engineFailures struct {
	mu    sync.Mutex
	first string
	// armedBy is the test the sink currently belongs to, so arming twice for
	// one test does not discard a fault already recorded for it.
	armedBy *testing.T
}

// recordEngineFailure keeps the first engine fault of a test.
func recordEngineFailure(format string, args ...interface{}) {
	msg := fmt.Sprintf(format, args...)
	engineFailures.mu.Lock()
	if engineFailures.first == "" {
		engineFailures.first = msg
	}
	engineFailures.mu.Unlock()
}

// armEngineFailureDetector binds the sink to t and taps log output for recovered panics via log.SetOutput; idempotent per t.
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

// trimPanicLogLine cuts a recovered panic's log line to the message, worker and panic value.
func trimPanicLogLine(line string) string {
	line = strings.TrimRight(line, "\n")
	if i := strings.Index(line, " stack="); i >= 0 {
		line = line[:i] + " (stack in the log line above)"
	}
	return line
}

// failFastOnEngineFailure fails the test if the engine reported a failed shard or a worker panic.
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
	// prevIndex is the previous applied index; each Update must carry a strictly greater one, with gaps allowed. RecoverFromSnapshot is the only reset.
	prevIndex uint64
	// applyGaps counts skipped index ranges, for diagnostics only.
	applyGaps atomic.Uint64
	applied   atomic.Uint64
	// recovers counts RecoverFromSnapshot calls, telling snapshot catch-up from log replay.
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

	// The reset must still move forward.
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

// forShard returns this tracker's single state machine for shardID, or nil unless exactly one exists.
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

// testHostConfig creates a valid HostConfig for E2E testing. Each call
// gets unique temp directories via t.TempDir(). The mtls parameter
// configures mandatory mTLS for the QUIC transport; all nodes in a
// cluster must share the same CA certificate.
func testHostConfig(t *testing.T, listenAddr string, mtls *config.MTLSConfig) config.HostConfig {
	t.Helper()
	return harnessHostConfig(t, listenAddr, mtls, walSyncMode)
}

// harnessHostConfig is testHostConfig for an explicit harness mode, so the
// pin tests can check both modes in one run.
func harnessHostConfig(t *testing.T, listenAddr string, mtls *config.MTLSConfig, walSync bool) config.HostConfig {
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
		RTTMillisecond:        harnessRTTMillisecond(walSync),
		DeploymentID:          1,
		AllowZeroDeploymentID: false,
		NumWorkers:            1,
		MaxApplyWorkers:       1,
		// Skip only the fdatasync so elections finish on a shared disk; QUICRAFT_IT_WAL_SYNC=1 runs the fsync path with a longer tick.
		NoSyncWAL: !walSync,
		TransportConfig: config.TransportConfig{
			MTLSConfig: mtls,
		},
	}
}

// TestHarness_WALSyncMode pins the harness mode of this suite: NoSyncWAL with
// QUICRAFT_IT_WAL_SYNC unset, the real fdatasync with it set to 1
// (make integration-test-walsync).
func TestHarness_WALSyncMode(t *testing.T) {
	for _, tc := range []struct {
		env         map[string]string
		wantNoSync  bool
		description string
	}{
		{map[string]string{}, true, "unset"},
		{map[string]string{walSyncEnv: "0"}, true, "0"},
		{map[string]string{walSyncEnv: "1"}, false, "1"},
	} {
		walSync := harnessWALSync(func(k string) string { return tc.env[k] })
		cfg := harnessHostConfig(t, "127.0.0.1:0", nil, walSync)
		if cfg.NoSyncWAL != tc.wantNoSync {
			t.Fatalf("%s=%s: NoSyncWAL=%v, want %v", walSyncEnv, tc.description, cfg.NoSyncWAL, tc.wantNoSync)
		}
	}
	if got, want := testHostConfig(t, "127.0.0.1:0", nil).NoSyncWAL, !harnessWALSync(os.Getenv); got != want {
		t.Fatalf("testHostConfig NoSyncWAL=%v under %s=%q, want %v", got, walSyncEnv, os.Getenv(walSyncEnv), want)
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

// kernelAddr binds a host on a kernel-assigned loopback port; read it from Host.RaftAddress.
const kernelAddr = "127.0.0.1:0"

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
	// nodeLocks holds one *sync.Mutex per node index, made on first use: it
	// serializes a node's stop and restart without hostsMu, which is never
	// held across Host.Close (see stopNode).
	nodeLocks sync.Map
	// ca issues every leaf; leaves holds each host's own keypair and node identity.
	ca     *testpki.CA
	leaves []*config.MTLSConfig
	// ids holds each host's node identity, which MemberIdentities must list for every member.
	ids []string
	t   *testing.T
}

// identities maps every replica ID (index+1) to the node identity its host
// reports: what a shard's Config.MemberIdentities has to name, and the join
// anchor a new node is started with.
func (c *testCluster) identities() map[uint64]string {
	c.hostsMu.RLock()
	defer c.hostsMu.RUnlock()
	ids := make(map[uint64]string, len(c.ids))
	for i, id := range c.ids {
		ids[uint64(i+1)] = id
	}
	return ids
}

// nodeIdentities reads each host's identity in order, for a cluster that has
// just been built.
func nodeIdentities(hosts []*quicraft.Host) []string {
	ids := make([]string, len(hosts))
	for i, h := range hosts {
		ids[i] = h.NodeIdentity()
	}
	return ids
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
// Each host binds a kernel-assigned port; the member map is built from the
// bound addresses before any shard starts.
func newTestCluster(t *testing.T, numNodes int) *testCluster {
	t.Helper()

	ca := testpki.NewCA(t)
	leaves := ca.Leaves(t, numNodes)
	addrs := make([]string, numNodes)
	hosts := make([]*quicraft.Host, numNodes)
	configs := make([]config.HostConfig, numNodes)

	for i := range numNodes {
		cfg := testHostConfig(t, kernelAddr, leaves[i])
		h, err := quicraft.NewHost(cfg, quicraft.WithoutEncryption())
		if err != nil {
			// Clean up any hosts already created.
			for j := 0; j < i; j++ {
				hosts[j].Close()
			}
			t.Fatalf("NewHost[%d] failed: %v", i, err)
		}
		addrs[i] = h.RaftAddress()
		hosts[i] = h
		configs[i] = cfg
	}

	c := &testCluster{
		hosts:   hosts,
		addrs:   addrs,
		configs: configs,
		stopped: make([]atomic.Bool, numNodes),
		ca:      ca,
		leaves:  leaves,
		ids:     nodeIdentities(hosts),
		t:       t,
	}
	setDiagCluster(t, c)
	return c
}

// diagCluster holds the latest cluster from newTestCluster so an expired wait can print its raft state.
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

// shardStateDump renders each live node's raft state for shardID, reporting per-node errors inline.
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
		info, err := h.ShardInfo(shardID)
		if err != nil {
			fmt.Fprintf(&b, "  node[%d]: ShardInfo: %v\n", i, err)
			continue
		}
		logSpan := "unavailable"
		if lr, err := h.LogRange(shardID); err == nil {
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
		scfg.MemberIdentities = c.identities()
		if err := h.StartShard(members, false, createFn, scfg); err != nil {
			c.t.Fatalf("StartShard[%d] shard=%d failed: %v", i, shardID, err)
		}
	}

	c.trackers = trackers
	return trackers
}

// close shuts down all hosts in the cluster.
func (c *testCluster) close() {
	c.hostsMu.RLock()
	n := len(c.hosts)
	c.hostsMu.RUnlock()
	for i := range n {
		lk := c.nodeLock(i)
		lk.Lock()
		if h := c.takeHost(i); h != nil {
			h.Close()
		}
		lk.Unlock()
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
		info, err := h.ShardInfo(shardID)
		if err != nil {
			continue
		}
		if info.IsLeader {
			return i
		}
	}
	return -1
}

// replicaIDAt returns the replica ID node idx runs for shardID, from ShardInfo; ok is false when there is none.
func (c *testCluster) replicaIDAt(shardID uint64, idx int) (uint64, bool) {
	c.hostsMu.RLock()
	defer c.hostsMu.RUnlock()
	if idx < 0 || idx >= len(c.hosts) {
		return 0, false
	}
	if c.stopped[idx].Load() {
		return 0, false
	}
	info, err := c.hosts[idx].ShardInfo(shardID)
	if err != nil {
		return 0, false
	}
	return info.ReplicaID, true
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
	lk := c.nodeLock(idx)
	lk.Lock()
	defer lk.Unlock()
	// Close outside hostsMu; it can take seconds.
	if h := c.takeHost(idx); h != nil {
		h.Close()
	}
}

// nodeLock returns the mutex that serializes stopNode, restartNode and close
// on node idx, so a restart never overlaps the Close of the host it replaces.
func (c *testCluster) nodeLock(idx int) *sync.Mutex {
	lk, _ := c.nodeLocks.LoadOrStore(idx, new(sync.Mutex))
	return lk.(*sync.Mutex)
}

// takeHost marks node idx stopped and returns its host, or nil when it was
// stopped already. The mark is made under hostsMu before the caller's Close
// begins, so a reader that honours c.stopped never picks up a closing host.
// The caller holds nodeLock(idx).
func (c *testCluster) takeHost(idx int) *quicraft.Host {
	c.hostsMu.Lock()
	defer c.hostsMu.Unlock()
	if c.stopped[idx].Load() {
		return nil
	}
	c.stopped[idx].Store(true)
	return c.hosts[idx]
}

// leaderHost returns the Host instance for the leader of the given shard.
func (c *testCluster) leaderHost(shardID uint64) *quicraft.Host {
	c.hostsMu.RLock()
	defer c.hostsMu.RUnlock()
	for i, h := range c.hosts {
		if c.stopped[i].Load() {
			continue
		}
		info, err := h.ShardInfo(shardID)
		if err != nil {
			continue
		}
		if info.IsLeader {
			return h
		}
	}
	return nil
}

// awaitLeaderHostInterval is how often awaitLeaderHost re-asks. An election
// completes in roughly one election timeout, so polling faster only burns CPU
// that the shard needs to hold the election.
const awaitLeaderHostInterval = 5 * time.Millisecond

// awaitLeaderHost returns the Host leading shardID within timeout, or nil; it never touches t.
func (c *testCluster) awaitLeaderHost(shardID uint64, timeout time.Duration) *quicraft.Host {
	ticker := time.NewTicker(awaitLeaderHostInterval)
	defer ticker.Stop()
	deadline := time.NewTimer(timeout)
	defer deadline.Stop()
	for {
		if h := c.leaderHost(shardID); h != nil {
			return h
		}
		select {
		case <-deadline.C:
			// One last look: the leader may have appeared while this goroutine
			// was descheduled, and reporting leaderless then would blame the
			// cluster for the runner.
			return c.leaderHost(shardID)
		case <-ticker.C:
		}
	}
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

// observeLeaders returns the leaders reported by every host in the cluster, stopped ones included.
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
		info, err := h.ShardInfo(shardID)
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

// splitBrain describes two leaders in the same term, or returns "".
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

// waitForSingleLeader polls until exactly one host leads the shard, failing at once on a same-term split brain.
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

// resolveStateMachine returns node i's state machine for shardID, preferring the cluster's live trackers and the host's replica ID.
func resolveStateMachine(cluster *testCluster, trackers []*smTracker, shardID uint64, i int) *kvStateMachine {
	replicaID, ok := cluster.replicaIDAt(shardID, i)
	if !ok {
		// A stopped host answers nothing, and its state machine is still in
		// the tracker for a caller that wants to read what it applied before
		// it went down. i+1 is the founding convention and is the best guess
		// available here; the forShard fallback below covers the rest.
		replicaID = uint64(i + 1)
	}

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

// waitForReplication waits for key to hold value on every live node and fails unless every node was checked.
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
		//quicraft:direct-propose resolved at the top of this same retry iteration
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

		// Wait for the leader's state machine to apply, found via resolveStateMachine.
		leaderSM := resolveStateMachine(cluster, trackers, shardID, leaderIdx)
		if leaderSM == nil {
			replicaID, ok := cluster.replicaIDAt(shardID, leaderIdx)
			t.Fatalf("leader state machine not found: node[%d] (replica=%d known=%t) leads shard %d "+
				"and committed key=%q, but no tracker holds its state machine",
				leaderIdx, replicaID, ok, shardID, key)
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

// proposeToLeader proposes cmd to the current leader of shardID and returns the result; see test/proposer.
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

// startShardWithConfig starts a shard on all nodes with a config factory and returns their trackers.
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
		scfg.MemberIdentities = c.identities()
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

// addNodeToCluster creates a host, adds it through the leader and starts the shard in join mode; opts must state encryption.
func addNodeToCluster(
	t *testing.T,
	cluster *testCluster,
	shardID, newReplicaID uint64,
	cfgFn func(shardID, replicaID uint64) config.Config,
	opts ...quicraft.HostOption,
) addNodeResult {
	t.Helper()

	// Create a new host on a fresh port under its own leaf from the cluster's
	// CA — its own keypair, its own identity — so existing peers verify it
	// and it verifies them.
	leaf := cluster.ca.Leaf(t)
	hostCfg := testHostConfig(t, kernelAddr, leaf)
	if len(opts) == 0 {
		opts = []quicraft.HostOption{quicraft.WithoutEncryption()}
	}
	h, err := quicraft.NewHost(hostCfg, opts...)
	if err != nil {
		t.Fatalf("NewHost for new node failed: %v", err)
	}
	addr := h.RaftAddress()

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
	syncAddNode(t, cluster, shardID, newReplicaID, addr, h.NodeIdentity(), membershipTimeout)
	added = true

	// Start the shard in join mode on the new host, anchored to the live
	// members: the bootstrap snapshot may come from whichever of them leads.
	createFn, tracker := kvCreateFunc()
	scfg := cfgFn(shardID, newReplicaID)
	scfg.MemberIdentities = cluster.identities()
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
	cluster.leaves = append(cluster.leaves, leaf)
	cluster.ids = append(cluster.ids, h.NodeIdentity())
	cluster.hostsMu.Unlock()

	return addNodeResult{
		host:    h,
		addr:    addr,
		tracker: tracker,
		idx:     idx,
	}
}

// syncAddNode adds a voter through the current leader, using test/membership's idempotent retry.
func syncAddNode(t *testing.T, cluster *testCluster, shardID, replicaID uint64, addr, identity string, timeout time.Duration) {
	t.Helper()
	membership.AddNode(t, cluster.leaderHost, shardID, replicaID, addr, identity, timeout)
}

// syncDeleteNode removes a replica through the current leader, with syncAddNode's idempotence.
func syncDeleteNode(t *testing.T, cluster *testCluster, shardID, replicaID uint64, timeout time.Duration) {
	t.Helper()
	membership.DeleteNode(t, cluster.leaderHost, shardID, replicaID, timeout)
}

// proposeUntilCommitted proposes key/value through the current leader via proposer.TryWith, retrying quicraft.Temporary errors; it never touches t.
func proposeUntilCommitted(cluster *testCluster, shardID uint64, key, value string, timeout time.Duration) error {
	_, err := proposer.TryWith(cluster.leaderHost, shardID, encodeKVPut(key, value),
		timeout, quicraft.Temporary)
	if err != nil {
		// Not "never committed within <timeout>": that phrasing is true only
		// for the deadline case and would report a permanent rejection as a
		// slow shard. TryWith returns a
		// permanent error on its first occurrence, so the budget is context
		// here rather than a claim about how long this took.
		return fmt.Errorf("%q did not commit (budget %v): %w", key, timeout, err)
	}
	return nil
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

// verifyAllData checks every pair in expected on every live node and fails unless every node was checked.
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
			// A missing state machine is waited for, then failed on.
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

// assertCaughtUpViaSnapshot fails unless the replica's state machine recovered from a snapshot.
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

// assertSnapshotTaken fails unless node idx wrote a snapshot under its NodeHostDir.
func assertSnapshotTaken(t *testing.T, cluster *testCluster, idx int) {
	t.Helper()
	dir := cluster.nodeHostDir(idx)
	paths := waitForSnapshotArtifact(t, fmt.Sprintf("node[%d]", idx), dir, membershipTimeout)
	t.Logf("node[%d] wrote %d snapshot artifact(s) under %s", idx, len(paths), dir)
}

// waitForLogCompacted waits until every live replica of shardID has compacted past index 1.
func waitForLogCompacted(t *testing.T, cluster *testCluster, shardID uint64, timeout time.Duration) {
	t.Helper()
	for i := range cluster.hosts {
		if cluster.stopped[i].Load() {
			continue
		}
		h := cluster.hosts[i]
		var last quicraft.LogRange
		waitForCondition(t, timeout,
			fmt.Sprintf("node[%d] compacts shard %d past index 1", i, shardID),
			func() bool {
				lr, err := h.LogRange(shardID)
				last = lr
				return err == nil && lr.FirstIndex > 1
			},
		)
		t.Logf("node[%d] log compacted to %+v", i, last)
	}
}

// waitForMembershipSize polls ShardMembership until the number of
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
			m, err := host.ShardMembership(shardID)
			if err != nil {
				return false
			}
			return len(m.Nodes) == expectedSize
		},
	)
}

// waitForMembershipConverged waits until every live replica reports exactly wantNodes voters and at least wantRemoved removed.
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

// membershipDivergence returns a per-replica description of how the live
// replicas' configurations differ from the expected one, or the empty string
// when they all agree with it.
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
		// Removed replicas need not converge.
		if slices.Contains(wantRemoved, uint64(i+1)) {
			continue
		}
		m, err := h.ShardMembership(shardID)
		if err != nil {
			fmt.Fprintf(&sb, "\n  node[%d]: ShardMembership failed: %v", i, err)
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
		fmt.Fprintf(&sb, "\n  node[%d]: has nodes=%v removed=%v", i,
			sortedKeys(m.Nodes), sortedRemoved(m.Removed))
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

// sortedKeys returns the replica IDs of a membership map in ascending order so
// that failure messages are stable across runs.
func sortedKeys(m map[uint64]string) []uint64 {
	ids := make([]uint64, 0, len(m))
	for id := range m {
		ids = append(ids, id)
	}
	slices.Sort(ids)
	return ids
}

// sortedRemoved returns the replica IDs of a removed set in ascending order.
func sortedRemoved(m map[uint64]struct{}) []uint64 {
	ids := make([]uint64, 0, len(m))
	for id := range m {
		ids = append(ids, id)
	}
	slices.Sort(ids)
	return ids
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

// countingBarrier counts snapshot frames sealed through a seal.Barrier, so tests can check encryption ran.
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

	ca := testpki.NewCA(t)
	leaves := ca.Leaves(t, numNodes)
	addrs := make([]string, numNodes)
	hosts := make([]*quicraft.Host, numNodes)
	configs := make([]config.HostConfig, numNodes)
	barriers := make([]*countingBarrier, numNodes)

	for i := range numNodes {
		// Create and initialize a barrier for this node.
		_ = seal.AllowWeakKDFParams()
		b := &countingBarrier{Barrier: seal.MustNewBarrier(testBarrierConfig(1))}
		s, err := newTestSealStrategy([]byte(fmt.Sprintf("test-passphrase-%d", i)))
		if err != nil {
			// Clean up hosts already created.
			for j := 0; j < i; j++ {
				hosts[j].Close()
			}
			t.Fatalf("newTestSealStrategy[%d]: %v", i, err)
		}
		if err := b.Initialize(context.Background(), s, seal.Credentials{}); err != nil {
			for j := 0; j < i; j++ {
				hosts[j].Close()
			}
			t.Fatalf("barrier.Initialize[%d]: %v", i, err)
		}
		barriers[i] = b

		cfg := testHostConfig(t, kernelAddr, leaves[i])
		h, err := quicraft.NewHost(cfg, quicraft.WithBarrier(b))
		if err != nil {
			for j := 0; j < i; j++ {
				hosts[j].Close()
			}
			t.Fatalf("NewHost[%d] with barrier failed: %v", i, err)
		}
		addrs[i] = h.RaftAddress()
		hosts[i] = h
		configs[i] = cfg
	}

	tc := &testCluster{
		hosts:   hosts,
		addrs:   addrs,
		configs: configs,
		stopped: make([]atomic.Bool, numNodes),
		ca:      ca,
		leaves:  leaves,
		ids:     nodeIdentities(hosts),
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
	s, err := newTestSealStrategy([]byte("replacement-passphrase"))
	if err != nil {
		t.Fatalf("newTestSealStrategy for replacement: %v", err)
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

// snapshotMetaFileName is the metadata file the engine writes beside each finished snapshot.
const snapshotMetaFileName = "snapshot.meta"

// snapshotArtifacts returns every snapshot payload under dir, skipping entries removed mid-walk.
func snapshotArtifacts(t *testing.T, dir string) []string {
	t.Helper()
	var found []string
	err := filepath.WalkDir(dir, func(path string, d fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			if errors.Is(walkErr, fs.ErrNotExist) {
				return nil
			}
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

// snapshotArtifactEpoch parses the Epoch field from the snapshot.meta record at metaPath by its documented layout; ok is false for an incomplete record, and 0 means plaintext.
func snapshotArtifactEpoch(metaPath string) (epoch uint64, ok bool, why string) {
	const (
		magic      = "QRSNAPMD"
		headerLen  = 16
		bodyLenOff = 12
		epochOff   = 40
		minBodyLen = epochOff + 8
	)
	raw, err := os.ReadFile(metaPath)
	if err != nil {
		return 0, false, fmt.Sprintf("%s: %v", snapshotMetaFileName, err)
	}
	if len(raw) < headerLen+minBodyLen {
		return 0, false, fmt.Sprintf("%s holds %d bytes, short of the %d-byte minimum record",
			snapshotMetaFileName, len(raw), headerLen+minBodyLen)
	}
	if string(raw[:len(magic)]) != magic {
		return 0, false, fmt.Sprintf("%s does not start with the %q record magic", snapshotMetaFileName, magic)
	}
	bodyLen := int(binary.LittleEndian.Uint32(raw[bodyLenOff : bodyLenOff+4]))
	if bodyLen < minBodyLen || headerLen+bodyLen > len(raw) {
		return 0, false, fmt.Sprintf("%s declares a %d-byte body in a %d-byte file",
			snapshotMetaFileName, bodyLen, len(raw))
	}
	return binary.LittleEndian.Uint64(raw[headerLen+epochOff : headerLen+epochOff+8]), true, ""
}

// unfinishedSnapshotArtifacts returns, per path, why its snapshot directory lacks a complete snapshot.meta.
func unfinishedSnapshotArtifacts(paths []string) []string {
	var pending []string
	for _, p := range paths {
		if _, ok, why := snapshotArtifactEpoch(filepath.Join(filepath.Dir(p), snapshotMetaFileName)); !ok {
			pending = append(pending, fmt.Sprintf("%s (%s)", p, why))
		}
	}
	return pending
}

// waitForFinishedSnapshotArtifacts waits until dir holds at least one artifact and all are complete, failing with the reasons on timeout.
func waitForFinishedSnapshotArtifacts(t *testing.T, label, dir string, timeout time.Duration) []string {
	t.Helper()
	var paths, pending []string
	waitForConditionDiag(t, timeout,
		fmt.Sprintf("%s finishes every snapshot save under %s", label, dir),
		func() bool {
			paths = snapshotArtifacts(t, dir)
			pending = unfinishedSnapshotArtifacts(paths)
			return len(paths) > 0 && len(pending) == 0
		},
		func() string {
			if len(paths) == 0 {
				return fmt.Sprintf("%s: no snapshot artifact was written under %s", label, dir)
			}
			return fmt.Sprintf("%s: %d of %d snapshot artifact(s) under %s never became "+
				"self-describing (a save killed between its plaintext fsync and its "+
				"encrypt leaves exactly this): %v",
				label, len(pending), len(paths), dir, pending)
		},
	)
	return paths
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

// assertSnapshotsSealed fails unless dir holds complete artifacts that each record a non-zero epoch and contain none of needles.
func assertSnapshotsSealed(t *testing.T, label, dir string, needles []string) {
	t.Helper()
	if len(needles) == 0 {
		t.Fatalf("%s: assertSnapshotsSealed called with no plaintext needles", label)
	}
	paths := waitForFinishedSnapshotArtifacts(t, label, dir, membershipTimeout)
	checked := 0
	for _, p := range paths {
		data, err := os.ReadFile(p)
		if errors.Is(err, fs.ErrNotExist) {
			// Snapshot GC removed the directory; nothing to check.
			continue
		}
		if err != nil {
			t.Fatalf("%s: reading snapshot artifact %s: %v", label, p, err)
		}
		if len(data) == 0 {
			t.Fatalf("%s: snapshot artifact %s is empty", label, p)
		}
		metaPath := filepath.Join(filepath.Dir(p), snapshotMetaFileName)
		epoch, ok, why := snapshotArtifactEpoch(metaPath)
		if !ok {
			if _, statErr := os.Stat(p); errors.Is(statErr, fs.ErrNotExist) {
				continue // collected alongside its metadata; see above
			}
			// The wait above returned only when every record parsed, and the
			// artifact is still here, so this is a record that decayed after
			// that point rather than a save in flight.
			t.Fatalf("%s: snapshot artifact %s has no readable at-rest epoch (%s), so it "+
				"cannot be shown to be sealed", label, p, why)
		}
		if epoch == 0 {
			t.Fatalf("%s: snapshot artifact %s (%d bytes) records at-rest epoch 0 in %s; "+
				"the artifact was stored in the clear on a barrier-encrypted node",
				label, p, len(data), metaPath)
		}
		for _, needle := range needles {
			if bytes.Contains(data, []byte(needle)) {
				t.Fatalf("%s: snapshot artifact %s (%d bytes) contains the plaintext "+
					"bytes %q; the at-rest barrier did not encrypt it",
					label, p, len(data), needle)
			}
		}
		checked++
	}
	if checked == 0 {
		t.Fatalf("%s: %d snapshot artifact(s) were found under %s but every one was "+
			"garbage-collected before it could be read; the at-rest check asserted nothing",
			label, len(paths), dir)
	}
	t.Logf("%s: %d finished snapshot artifact(s) under %s, each sealed at a non-zero "+
		"epoch and none containing plaintext", label, checked, dir)
}

// requestSnapshotSync requests a snapshot of shardID on host and returns its index, retrying only while another snapshot runs.
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

// TestBudgetsDescribeTheClusterTheyBudgetFor checks the election budget constants match the config builders.
func TestBudgetsDescribeTheClusterTheyBudgetFor(t *testing.T) {
	for _, walSync := range []bool{false, true} {
		rtt := harnessRTTMillisecond(walSync)
		if got := harnessHostConfig(t, "127.0.0.1:1", nil, walSync).RTTMillisecond; got != rtt {
			t.Fatalf("walSync=%v: harnessHostConfig sets RTTMillisecond=%d but the budgets "+
				"are derived from harnessRTTMillisecond=%d", walSync, got, rtt)
		}
		if walSync {
			shortest := time.Duration(testElectionRTT) * time.Duration(rtt) * time.Millisecond
			if shortest <= walSyncStallCeiling {
				t.Fatalf("WAL-sync mode's shortest election timeout %v does not outlast "+
					"the %v fdatasync stall", shortest, walSyncStallCeiling)
			}
		}
	}
	if got := testHostConfig(t, "127.0.0.1:1", nil).RTTMillisecond; got != testRTTMillisecond {
		t.Fatalf("testHostConfig sets RTTMillisecond=%d but the budgets are derived "+
			"from testRTTMillisecond=%d; one Raft tick is not what electionRound (%v) "+
			"thinks it is", got, testRTTMillisecond, electionRound)
	}
	if testRTTMillisecond != harnessRTTMillisecond(walSyncMode) {
		t.Fatalf("testRTTMillisecond=%d is not the tick of the run's mode (walSync=%v)",
			testRTTMillisecond, walSyncMode)
	}
	if leaderElectionTimeout != electionbudget.LeaderElection(testElectionRTT, harnessRTTMillisecond(walSyncMode)) ||
		failoverTimeout != electionbudget.Failover(testElectionRTT, harnessRTTMillisecond(walSyncMode)) {
		t.Fatalf("leaderElectionTimeout=%v failoverTimeout=%v do not follow the mode's tick",
			leaderElectionTimeout, failoverTimeout)
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
