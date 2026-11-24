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
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	quicraft "github.com/jeremyhahn/go-quicraft/pkg"
	"github.com/jeremyhahn/go-quicraft/pkg/config"
	"github.com/jeremyhahn/go-quicraft/pkg/sm"
	"github.com/jeremyhahn/go-quicraft/test/electionbudget"
	"github.com/jeremyhahn/go-quicraft/test/membership"
	"github.com/jeremyhahn/go-quicraft/test/proposer"
	"github.com/jeremyhahn/go-quicraft/test/testpki"
)

// Raft timing and election budgets: every budget derives from the constants the config builders use, pinned by TestBudgetsDescribeTheClusterTheyBudgetFor.

// defaultTimeout is the standard budget for a single Raft operation that is
// NOT an election: a proposal round trip, a lookup, a membership query. It is
// deliberately not expressed in election rounds, because none of its callers
// is waiting for an election.
const defaultTimeout = 15 * time.Second

// testRTTMillisecond is this suite's tick in milliseconds (harnessRTTMillisecond: 50ms without WAL fsync, 500ms in WAL-sync mode).
var testRTTMillisecond = harnessRTTMillisecond(walSyncMode)

// walSyncEnv names the switch that selects the harness's WAL-sync mode. With
// QUICRAFT_IT_WAL_SYNC=1 every host fdatasyncs its WAL before an ack, as
// production does (make integration-test-walsync); unset, the harness skips
// the fsync (NoSyncWAL) to keep elections inside a contended disk's stalls.
const walSyncEnv = "QUICRAFT_IT_WAL_SYNC"

// noSyncRTTMillisecond is the tick of the default, NoSyncWAL mode.
const noSyncRTTMillisecond = 50

// walSyncRTTMillisecond is the tick of WAL-sync mode. A real fdatasync on a
// shared, contended disk was measured to stall 2.5-4.6s (see testHostConfig),
// and Raft persists term and vote before any vote leaves the node, so the
// shortest election timeout, testElectionRTT ticks, must outlast that stall:
// 10 ticks of 500ms is 5s. TestBudgetsDescribeTheClusterTheyBudgetFor pins it.
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

// testElectionRTT is the election timeout in ticks, read from Config.SetDefaults since testShardConfig leaves it unset.
const testElectionRTT = config.DefaultElectionRTT

// raftTick is one Raft tick in wall-clock time.
var raftTick = time.Duration(testRTTMillisecond) * time.Millisecond

// The election budgets come from test/electionbudget, derived from testElectionRTT and testRTTMillisecond.
var (
	electionRound         = electionbudget.Round(testElectionRTT, testRTTMillisecond)
	leaderElectionTimeout = electionbudget.LeaderElection(testElectionRTT, testRTTMillisecond)
	failoverTimeout       = electionbudget.Failover(testElectionRTT, testRTTMillisecond)
)

// coldStartElectionRounds is re-exposed under its long-standing local name so
// the suites' log lines keep naming the round count.
const coldStartElectionRounds = electionbudget.ColdStartRounds

// membershipTimeout is the budget for a membership change to land and be confirmed by a linearizable read (test/membership): a failover plus the change.
var membershipTimeout = failoverTimeout + defaultTimeout

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

// encodeIndex is the 8-byte big-endian form a put result carries its entry
// index in; decodeIndex reads it back, 0 for anything else.
func encodeIndex(index uint64) []byte {
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, index)
	return buf
}

func decodeIndex(data []byte) uint64 {
	if len(data) != 8 {
		return 0
	}
	return binary.BigEndian.Uint64(data)
}

// appendResult is the full post-append value kvStateMachine returns for opAppend.
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
	// shardID and replicaID name the replica this machine belongs to, so
	// an apply gate parks only the replica it was armed for.
	shardID   uint64
	replicaID uint64
	mu        sync.RWMutex
	data      map[string]string
	// prevIndex is the last applied index; every Update entry must be strictly greater (gaps are expected and recorded in applyGaps). Only RecoverFromSnapshot resets it.
	prevIndex uint64
	// applyGaps counts skipped index ranges, for diagnostics only.
	applyGaps atomic.Uint64
	applied   atomic.Uint64
	// lastWrite is the index that last wrote each key; after RecoverFromSnapshot it is the snapshot index, a lower bound.
	lastWrite map[string]uint64
}

// applyGates parks kvStateMachine.Update on a put to a chosen key for one replica, so a test can act during a replay; process-wide, keyed by replica and key.
var applyGates sync.Map // applyGateKey -> *applyGate

type applyGateKey struct {
	shardID   uint64
	replicaID uint64
	key       string
}

type applyGate struct {
	parked      chan struct{} // closed when an Update reaches the gate
	release     chan struct{} // closed to let it through
	parkOnce    sync.Once
	releaseOnce sync.Once
}

// gateApply parks every put to key applied by (shardID, replicaID) until release; parked closes on the first. release is idempotent and registered with t.Cleanup.
func gateApply(t *testing.T, key string, shardID, replicaID uint64) (parked <-chan struct{}, release func()) {
	t.Helper()
	gk := applyGateKey{shardID: shardID, replicaID: replicaID, key: key}
	g := &applyGate{parked: make(chan struct{}), release: make(chan struct{})}
	if _, loaded := applyGates.LoadOrStore(gk, g); loaded {
		t.Fatalf("an apply gate on %q for shard %d replica %d is already armed", key, shardID, replicaID)
	}
	release = func() {
		g.releaseOnce.Do(func() {
			applyGates.Delete(gk)
			close(g.release)
		})
	}
	t.Cleanup(release)
	return g.parked, release
}

// parkAtGate is Update's side of gateApply. It runs with kv.mu held, so a
// parked replica answers no Lookup until released; the tests that park read
// from another replica.
func (kv *kvStateMachine) parkAtGate(key string) {
	v, ok := applyGates.Load(applyGateKey{shardID: kv.shardID, replicaID: kv.replicaID, key: key})
	if !ok {
		return
	}
	g := v.(*applyGate)
	g.parkOnce.Do(func() { close(g.parked) })
	<-g.release
}

// indexedGet is a Lookup query that returns the value AND the index of the
// write that produced it, as an indexedValue.
type indexedGet string

// indexedValue is what an indexedGet returns.
type indexedValue struct {
	Value string
	Index uint64 // 0 when the key has never been written
}

func newKVStateMachine(shardID, replicaID uint64) *kvStateMachine {
	return &kvStateMachine{
		shardID:   shardID,
		replicaID: replicaID,
		data:      make(map[string]string),
		lastWrite: make(map[string]uint64),
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
			kv.lastWrite[key] = entry.Index
			kv.parkAtGate(key)
			// Data carries the entry index; Value stays the length callers check.
			results[i] = sm.Result{Value: uint64(len(value)), Data: encodeIndex(entry.Index)}
		case opAppend:
			token := string(entry.Cmd[3+keyLen:])
			value := kv.data[key]
			if value == "" {
				value = token
			} else {
				value = value + appendSep + token
			}
			kv.data[key] = value
			kv.lastWrite[key] = entry.Index
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
	if ig, isIndexed := query.(indexedGet); isIndexed {
		kv.mu.RLock()
		defer kv.mu.RUnlock()
		return indexedValue{Value: kv.data[string(ig)], Index: kv.lastWrite[string(ig)]}, nil
	}
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

// RecoverFromSnapshot restores the store and resets the applied-index expectation; a snapshot older than what was applied is recorded as a violation.
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
	// The snapshot does not carry write indices; the snapshot index bounds
	// every one of them from above, and that bound is what a read-your-writes
	// check compares against (see lastWrite).
	kv.lastWrite = make(map[string]uint64, len(newData))
	for k := range newData {
		kv.lastWrite[k] = snapshotIndex
	}
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
		machine := newKVStateMachine(shardID, replicaID)
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

// testHostConfig creates a valid HostConfig for linearizability testing.
func testHostConfig(t *testing.T, listenAddr string, mtls *config.MTLSConfig) config.HostConfig {
	t.Helper()
	return harnessHostConfig(t, listenAddr, mtls, walSyncMode)
}

// harnessHostConfig is testHostConfig for an explicit harness mode, so the
// pin tests can check both modes in one run.
func harnessHostConfig(t *testing.T, listenAddr string, mtls *config.MTLSConfig, walSync bool) config.HostConfig {
	t.Helper()
	assertApplyOrder(t)
	return config.HostConfig{
		WALDir:                t.TempDir(),
		NodeHostDir:           t.TempDir(),
		RaftAddress:           listenAddr,
		ListenAddress:         listenAddr,
		RTTMillisecond:        harnessRTTMillisecond(walSync),
		DeploymentID:          1,
		AllowZeroDeploymentID: false,
		NumWorkers:            1,
		MaxApplyWorkers:       1,
		// By default the WAL skips only fdatasync, since a contended disk can stall it for seconds and block elections; QUICRAFT_IT_WAL_SYNC=1 runs the production fsync path with walSyncRTTMillisecond.
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

// testShardConfig creates a per-shard config for linearizability testing.
func testShardConfig(shardID, replicaID uint64) config.Config {
	c := config.Config{
		ShardID:   shardID,
		ReplicaID: replicaID,
	}
	c.SetDefaults()
	return c
}

// kernelAddr binds a host on a kernel-assigned loopback port; read it back from Host.RaftAddress.
const kernelAddr = "127.0.0.1:0"

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
	// nodeLocks holds one *sync.Mutex per node index, made on first use: it
	// serializes a node's stop and restart without hostsMu, which is never
	// held across Host.Close (see stopNode).
	nodeLocks sync.Map
	// ca issues every host's leaf — its own keypair, its own node identity
	// (see test/testpki); configs keeps each host's leaf for its restart. ids
	// holds each host's identity, recorded when the host was first built: a
	// property of the leaf, so it survives a stop, and a founder's
	// Config.MemberIdentities has to name every member — stopped ones too.
	ca  *testpki.CA
	ids []string
	t   *testing.T
	// timeline records leader and term transitions for a failing test to
	// print; every host's config carries its event listener.
	timeline *leaderTimeline
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

// newTestCluster creates an N-node cluster with real QUIC transport.
func newTestCluster(t *testing.T, numNodes int) *testCluster {
	t.Helper()

	timeline := newLeaderTimeline()
	ca := testpki.NewCA(t)
	leaves := ca.Leaves(t, numNodes)
	addrs := make([]string, numNodes)
	hosts := make([]*quicraft.Host, numNodes)
	configs := make([]config.HostConfig, numNodes)

	// Each host binds a kernel-assigned port and keeps it: the recorded
	// config names the bound address, so restartNode reopens the host there.
	for i := range numNodes {
		cfg := testHostConfig(t, kernelAddr, leaves[i])
		cfg.EventListener = timeline.listener()
		h, err := quicraft.NewHost(cfg, quicraft.WithoutEncryption())
		if err != nil {
			for j := 0; j < i; j++ {
				hosts[j].Close()
			}
			t.Fatalf("NewHost[%d] failed: %v", i, err)
		}
		cfg.RaftAddress = h.RaftAddress()
		cfg.ListenAddress = cfg.RaftAddress
		addrs[i] = cfg.RaftAddress
		hosts[i] = h
		configs[i] = cfg
	}

	ids := make([]string, numNodes)
	for i, h := range hosts {
		ids[i] = h.NodeIdentity()
	}
	return &testCluster{
		hosts:    hosts,
		addrs:    addrs,
		configs:  configs,
		stopped:  make([]atomic.Bool, numNodes),
		ca:       ca,
		ids:      ids,
		t:        t,
		timeline: timeline,
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

// findLeader returns the index of the current leader for the shard, or -1.
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

// proposeToLeader proposes cmd to whichever node leads shardID at the call; see test/proposer.
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
	start := time.Now()
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
			c.logShardState(shardID)
			c.t.Fatalf("timed out after %v (budget %v) waiting for cluster leader on shard %d",
				time.Since(start), timeout, shardID)
			return -1
		case <-ticker.C:
		}
	}
}

// logShardState logs what every running host reports for shardID: the view
// a failed leader wait leaves behind, so a stuck election shows its terms.
func (c *testCluster) logShardState(shardID uint64) {
	c.t.Helper()
	c.hostsMu.RLock()
	defer c.hostsMu.RUnlock()
	for i, h := range c.hosts {
		if c.stopped[i].Load() {
			continue
		}
		info, err := h.ShardInfo(shardID)
		if err != nil {
			c.t.Logf("node[%d] shard %d: %v", i, shardID, err)
			continue
		}
		c.t.Logf("node[%d] shard %d: term=%d leader=%t leaderID=%d lastApplied=%d health=%v",
			i, shardID, info.Term, info.IsLeader, info.LeaderID, info.LastApplied, info.Health)
	}
}

// stopNode stops the host at the given index.
func (c *testCluster) stopNode(idx int) {
	c.t.Helper()
	lk := c.nodeLock(idx)
	lk.Lock()
	defer lk.Unlock()
	h := c.takeHost(idx)
	if h == nil {
		return
	}
	// Host.Close drains client operations, so a stop is its own phase; it runs outside hostsMu.
	start := time.Now()
	h.Close()
	c.t.Logf("node[%d] stopped in %v", idx, time.Since(start))
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

// restartNode recreates a stopped host with the same WAL/NodeHost directories,
// allowing it to recover state from its write-ahead log. The node is then
// re-joined to the existing shard.
func (c *testCluster) restartNode(idx int, shardID uint64) {
	c.t.Helper()
	lk := c.nodeLock(idx)
	lk.Lock()
	defer lk.Unlock()

	if !c.stopped[idx].Load() {
		c.t.Fatalf("%v", &NodeNotStoppedError{Index: idx})
	}

	cfg := c.configs[idx]
	h, err := quicraft.NewHost(cfg, quicraft.WithoutEncryption())
	if err != nil {
		c.t.Fatalf("restartNode[%d]: NewHost failed: %v", idx, err)
	}

	createFn, tracker := kvCreateFunc()
	replicaID := uint64(idx + 1)
	scfg := testShardConfig(shardID, replicaID)
	scfg.MemberIdentities = c.identities()

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
		scfg.MemberIdentities = c.identities()
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

	cfg := testHostConfig(c.t, kernelAddr, c.ca.Leaf(c.t))
	cfg.EventListener = c.timeline.listener()
	h, err := quicraft.NewHost(cfg, quicraft.WithoutEncryption())
	if err != nil {
		c.t.Fatalf("addNode: NewHost failed: %v", err)
	}
	addr := h.RaftAddress()
	cfg.RaftAddress = addr
	cfg.ListenAddress = addr

	newReplicaID := uint64(len(c.addrs) + 1)

	// Run the change through the leader at each attempt until a linearizable membership read confirms it, within membershipTimeout; see test/membership.
	if err := membership.TryAddNode(c.leaderHost, shardID, newReplicaID, addr, h.NodeIdentity(), membershipTimeout); err != nil {
		h.Close()
		c.t.Fatalf("addNode: %v", err)
	}

	// Start the shard in join mode on the new host, anchored to the
	// existing members: the bootstrap snapshot may come from whichever leads.
	createFn, tracker := kvCreateFunc()
	scfg := testShardConfig(shardID, newReplicaID)
	scfg.MemberIdentities = c.identities()
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
	c.ids = append(c.ids, h.NodeIdentity())
	c.hostsMu.Unlock()

	return idx
}

// removeNode removes a node from the shard's membership via the leader,
// then stops the removed node. The leader must not be the node being removed.
func (c *testCluster) removeNode(idx int, shardID uint64) {
	c.t.Helper()

	// Wait for a leader rather than sampling once; a leader that moved onto the target is fatal.
	leaderIdx := c.waitForClusterLeader(shardID, failoverTimeout)
	if leaderIdx == idx {
		c.t.Fatalf("removeNode: cannot remove the leader (node[%d])", idx)
	}

	replicaID := uint64(idx + 1)

	// The change itself goes through whichever node leads at each attempt
	// (test/membership), not the leader observed above.
	if err := membership.TryDeleteNode(c.leaderHost, shardID, replicaID, membershipTimeout); err != nil {
		c.t.Fatalf("removeNode: %v", err)
	}

	c.stopNode(idx)
}

// ---------------------------------------------------------------------------
// Leader/Term Safety Guard
// ---------------------------------------------------------------------------

// leaderObservation is one poll sample of one host's view of a shard. A host
// that is down, or does not run the shard, is observed with up false.
type leaderObservation struct {
	index     int
	up        bool
	replicaID uint64
	leaderID  uint64
	term      uint64
	isLeader  bool
}

// observeShard samples (LeaderID, Term, IsLeader) from every host, ignoring c.stopped; a down host is observed as down.
func (c *testCluster) observeShard(shardID uint64) []leaderObservation {
	c.hostsMu.RLock()
	hosts := make([]*quicraft.Host, len(c.hosts))
	copy(hosts, c.hosts)
	c.hostsMu.RUnlock()

	obs := make([]leaderObservation, len(hosts))
	for i, h := range hosts {
		obs[i].index = i
		if h == nil {
			continue
		}
		info, err := h.ShardInfo(shardID)
		if err != nil {
			continue
		}
		obs[i] = leaderObservation{
			index:     i,
			up:        true,
			replicaID: info.ReplicaID,
			leaderID:  info.LeaderID,
			term:      info.Term,
			isLeader:  info.IsLeader,
		}
	}
	return obs
}

// leaderTermGuard fails the test the moment two replicas report leading the same term, and feeds leaderTimeline on every poll.
type leaderTermGuard struct {
	t       *testing.T
	cluster *testCluster
	shardID uint64
	stopC   chan struct{}
	doneC   chan struct{}
	polls   atomic.Int64
	maxTerm atomic.Uint64

	// last is the previous poll's observation per node index. Only the guard
	// goroutine, and stop once that has exited, touch it.
	last []leaderObservation

	// mu guards the no-leader-known intervals, which checkLinearizability
	// reads from the test goroutine.
	mu sync.Mutex
	// clusterGap follows "no up node knows a leader"; nodeGaps, per node
	// index, "this node is up and knows no leader".
	clusterGap gapTracker
	nodeGaps   []gapTracker
}

// guards maps a test to its leader/term guard, so that checkLinearizability,
// which is handed only the test and its history, can name the no-leader gap
// behind a throughput collapse.
var guards sync.Map // *testing.T -> *leaderTermGuard

// gapTracker follows one no-leader-known condition across the guard's polls
// and keeps its longest interval. Times are offsets from the timeline start.
type gapTracker struct {
	open    bool
	since   time.Duration
	longest time.Duration
	from    time.Duration
}

// observe records one poll at now. It reports whether the condition began at
// this poll, or ended at it and after how long.
func (g *gapTracker) observe(now time.Duration, inGap bool) (began, ended bool, length time.Duration) {
	switch {
	case inGap && !g.open:
		g.open, g.since = true, now
		return true, false, 0
	case !inGap && g.open:
		g.open = false
		length = now - g.since
		if length > g.longest {
			g.longest, g.from = length, g.since
		}
		return false, true, length
	}
	return false, false, 0
}

// longestAt returns the longest interval so far, counting one still open at
// now.
func (g *gapTracker) longestAt(now time.Duration) (from, length time.Duration) {
	if g.open && now-g.since > g.longest {
		return g.since, now - g.since
	}
	return g.from, g.longest
}

// leaderTimelineCap bounds the leader timeline a failing test prints: the
// transitions nearest the failure are the ones that explain it.
const leaderTimelineCap = 200

// leaderTimeline records leader and term changes, from leaderTermGuard polls and the engine's leader events, with offsets from cluster creation; printed only on failure.
type leaderTimeline struct {
	start   time.Time
	mu      sync.Mutex
	entries []string
	total   int
}

func newLeaderTimeline() *leaderTimeline {
	return &leaderTimeline{start: time.Now()}
}

// since is the monotonic offset of now from the timeline's start.
func (tl *leaderTimeline) since() time.Duration {
	return time.Since(tl.start)
}

// add appends one transition, keeping the last leaderTimelineCap. The offset
// is taken under the lock so that entries stay in time order.
func (tl *leaderTimeline) add(format string, args ...any) {
	tl.mu.Lock()
	defer tl.mu.Unlock()
	line := fmt.Sprintf("+%-9v ", tl.since().Round(time.Millisecond)) + fmt.Sprintf(format, args...)
	tl.total++
	if len(tl.entries) == leaderTimelineCap {
		copy(tl.entries, tl.entries[1:])
		tl.entries = tl.entries[:leaderTimelineCap-1]
	}
	tl.entries = append(tl.entries, line)
}

// dump renders the retained transitions, oldest first.
func (tl *leaderTimeline) dump() string {
	tl.mu.Lock()
	defer tl.mu.Unlock()
	var sb strings.Builder
	fmt.Fprintf(&sb, "leader timeline (last %d of %d transitions, offsets from cluster start):",
		len(tl.entries), tl.total)
	for _, e := range tl.entries {
		sb.WriteString("\n  ")
		sb.WriteString(e)
	}
	return sb.String()
}

// listener returns the engine event callbacks that feed the timeline. They
// run on the host's event dispatcher, never on an engine path.
func (tl *leaderTimeline) listener() *config.EventListener {
	return &config.EventListener{
		OnLeaderUpdated: func(i config.LeaderInfo) {
			tl.add("shard %d event replica=%d LeaderUpdated leader=%d term=%d",
				i.ShardID, i.ReplicaID, i.LeaderID, i.Term)
		},
		OnLeaderReady: func(i config.LeaderInfo) {
			tl.add("shard %d event replica=%d LeaderReady leader=%d term=%d",
				i.ShardID, i.ReplicaID, i.LeaderID, i.Term)
		},
		OnLeaderLost: func(i config.EventShardInfo) {
			tl.add("shard %d event replica=%d LeaderLost leader=%d term=%d",
				i.ShardID, i.ReplicaID, i.LeaderID, i.Term)
		},
		OnCampaignLaunched: func(i config.EventShardInfo) {
			tl.add("shard %d event replica=%d CampaignLaunched leader=%d term=%d",
				i.ShardID, i.ReplicaID, i.LeaderID, i.Term)
		},
	}
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
	guards.Store(t, g)
	t.Cleanup(func() { guards.Delete(t) })
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
	obs := g.cluster.observeShard(g.shardID)
	g.polls.Add(1)
	g.record(obs)

	byTerm := make(map[uint64][]leaderObservation, len(obs))
	for _, o := range obs {
		if !o.isLeader {
			continue
		}
		if o.term > g.maxTerm.Load() {
			g.maxTerm.Store(o.term)
		}
		byTerm[o.term] = append(byTerm[o.term], o)
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

// record writes every change in a node's view since the previous poll to the
// cluster's leader timeline, and advances the no-leader-known intervals. It
// reuses the poll check already made, so the timeline adds no sampling.
func (g *leaderTermGuard) record(obs []leaderObservation) {
	tl := g.cluster.timeline
	for _, o := range obs {
		if o.index < len(g.last) && g.last[o.index] == o {
			continue
		}
		switch {
		case !o.up:
			tl.add("shard %d poll node[%d] down or not running the shard", g.shardID, o.index)
		case o.isLeader:
			tl.add("shard %d poll node[%d](replica=%d) LEADER term=%d", g.shardID, o.index, o.replicaID, o.term)
		case o.leaderID == 0:
			tl.add("shard %d poll node[%d](replica=%d) leader unknown term=%d", g.shardID, o.index, o.replicaID, o.term)
		default:
			tl.add("shard %d poll node[%d](replica=%d) follows leader=%d term=%d",
				g.shardID, o.index, o.replicaID, o.leaderID, o.term)
		}
	}
	g.last = obs

	now := tl.since()
	known := false
	g.mu.Lock()
	for len(g.nodeGaps) < len(obs) {
		g.nodeGaps = append(g.nodeGaps, gapTracker{})
	}
	for _, o := range obs {
		known = known || (o.up && o.leaderID != 0)
		g.nodeGaps[o.index].observe(now, o.up && o.leaderID == 0)
	}
	began, ended, length := g.clusterGap.observe(now, !known)
	g.mu.Unlock()
	if began {
		tl.add("shard %d poll: no node knows a leader", g.shardID)
	}
	if ended {
		tl.add("shard %d poll: a leader is known again after %v", g.shardID, length.Round(time.Millisecond))
	}
}

// gapSummary names the longest interval in which no node knew a leader of
// the shard, and the longest in which one up node alone knew none, each with
// its offset from the timeline start.
func (g *leaderTermGuard) gapSummary() string {
	now := g.cluster.timeline.since()
	g.mu.Lock()
	defer g.mu.Unlock()
	from, length := g.clusterGap.longestAt(now)
	s := fmt.Sprintf("longest no-leader-known interval on shard %d: %v with no node knowing a leader (from +%v)",
		g.shardID, length.Round(time.Millisecond), from.Round(time.Millisecond))
	node := -1
	var nFrom, nLength time.Duration
	for i := range g.nodeGaps {
		if f, l := g.nodeGaps[i].longestAt(now); l > nLength {
			node, nFrom, nLength = i, f, l
		}
	}
	if node >= 0 {
		s += fmt.Sprintf(", %v on node[%d] (from +%v)",
			nLength.Round(time.Millisecond), node, nFrom.Round(time.Millisecond))
	}
	return s
}

// leaderGapSummary is gapSummary for the guard t started, for failure
// messages that have only the test.
func leaderGapSummary(t *testing.T) string {
	if g, ok := guards.Load(t); ok {
		return g.(*leaderTermGuard).gapSummary()
	}
	return "no leader/term guard ran in this test, so there is no leader timeline"
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
	// A failing test explains its leadership from the log alone; a passing
	// one prints nothing extra.
	if g.t.Failed() {
		g.t.Logf("%s\n%s", g.cluster.timeline.dump(), g.gapSummary())
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

// TestBudgetsDescribeTheClusterTheyBudgetFor pins the budget derivation to the configuration the suite starts.
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
	if got := testShardConfig(1, 1).ElectionRTT; got != testElectionRTT {
		t.Fatalf("testShardConfig sets ElectionRTT=%d but the budgets are derived from "+
			"testElectionRTT=%d; electionRound (%v) and every budget built on it "+
			"are wrong by a factor of %.2f", got, testElectionRTT,
			electionRound, float64(got)/float64(testElectionRTT))
	}
	if electionRound != 2*time.Duration(testElectionRTT)*raftTick {
		t.Fatalf("electionRound=%v is not 2*ElectionRTT*tick", electionRound)
	}
	if leaderElectionTimeout != electionbudget.LeaderElection(testElectionRTT, harnessRTTMillisecond(walSyncMode)) ||
		failoverTimeout != electionbudget.Failover(testElectionRTT, harnessRTTMillisecond(walSyncMode)) {
		t.Fatalf("leaderElectionTimeout=%v failoverTimeout=%v do not follow the mode's tick",
			leaderElectionTimeout, failoverTimeout)
	}
	t.Logf("walSync=%v tick=%v electionRound=%v leaderElectionTimeout=%v (%d rounds)%s",
		walSyncMode, raftTick, electionRound, leaderElectionTimeout, coldStartElectionRounds, fmt.Sprintf(" failoverTimeout=%v", failoverTimeout))
}
