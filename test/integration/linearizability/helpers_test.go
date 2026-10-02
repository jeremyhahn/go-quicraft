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
	"github.com/jeremyhahn/go-quicraft/test/netport"
	"github.com/jeremyhahn/go-quicraft/test/proposer"
	"github.com/jeremyhahn/go-quicraft/test/testpki"
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
// though it were — a recurring flake class. Elsewhere the unpinned value is a
// leader index or a replica ID; here it is the shard configuration the
// budget is supposed to describe. A literal cannot follow a config change and
// gives no signal when it stops matching; a derivation cannot drift at all.
//
// The derivation is the one contrib/dragonboat and test/integration/e2e use,
// mirrored on purpose: the suites
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

// The election budgets are test/electionbudget's, derived from this suite's
// testElectionRTT and testRTTMillisecond. The derivation and the reasoning
// behind it — why twice ElectionRTT, why thirty rounds, what was measured —
// live in that package; the pin test below is the half that cannot move,
// because it reads THIS suite's config builders.
var (
	electionRound         = electionbudget.Round(testElectionRTT, testRTTMillisecond)
	leaderElectionTimeout = electionbudget.LeaderElection(testElectionRTT, testRTTMillisecond)
	failoverTimeout       = electionbudget.Failover(testElectionRTT, testRTTMillisecond)
)

// coldStartElectionRounds is re-exposed under its long-standing local name so
// the suites' log lines keep naming the round count.
const coldStartElectionRounds = electionbudget.ColdStartRounds

// membershipTimeout is the budget for a membership change to LAND: to be
// accepted by a leader and confirmed by a linearizable membership read
// (test/membership). It is the sum of the two waits the old direct call sites
// made in sequence — a failover that may be in progress when the change is
// requested, then the change itself — because the retry loop absorbs both
// inside one budget rather than sampling the leader once and then calling.
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
	// lastWrite is the raft index of the entry that last wrote each key: the
	// provenance a read-your-writes check needs, because a value alone
	// cannot say whether it is older or newer than the write a client was
	// just acknowledged for. After RecoverFromSnapshot it is a LOWER BOUND
	// (the snapshot index) for every key the snapshot carries -- the value
	// was written at or before that point, which is all the check needs.
	lastWrite map[string]uint64
}

// applyGates parks kvStateMachine.Update on a put to a chosen key, for a test
// that needs a replay to stand still: a restarted replica replays its whole
// log, and the only way to be certain something happens DURING that replay
// -- rather than to race it against the machine's speed, which fails on a
// slow CI runner -- is to stop the replay at a known entry until
// the something is done. Armed by gateApply before the entry is proposed,
// released by the function it returns. Keyed by the KV key, process-wide,
// because the state machine that parks is created inside StartShard by a
// factory the test does not hold.
var applyGates sync.Map

type applyGate struct {
	parked      chan struct{} // closed when an Update reaches the gate
	release     chan struct{} // closed to let it through
	parkOnce    sync.Once
	releaseOnce sync.Once
}

// gateApply arms a gate on key. The first put to key that any kvStateMachine
// in this process applies parks until release is called; parked is closed
// when it does. release is idempotent and also registered with t.Cleanup,
// so a failed assertion while parked fails the test instead of hanging the
// host's Close behind an apply that never returns.
func gateApply(t *testing.T, key string) (parked <-chan struct{}, release func()) {
	t.Helper()
	g := &applyGate{parked: make(chan struct{}), release: make(chan struct{})}
	if _, loaded := applyGates.LoadOrStore(key, g); loaded {
		t.Fatalf("an apply gate on %q is already armed", key)
	}
	release = func() {
		g.releaseOnce.Do(func() {
			applyGates.Delete(key)
			close(g.release)
		})
	}
	t.Cleanup(release)
	return g.parked, release
}

// parkAtGate is Update's side of gateApply. It runs with kv.mu held, so a
// parked replica answers no Lookup until released; the tests that park read
// from another replica.
func parkAtGate(key string) {
	v, ok := applyGates.Load(key)
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

func newKVStateMachine() *kvStateMachine {
	return &kvStateMachine{
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
			parkAtGate(key)
			// Data carries the entry's index so the proposer learns WHERE
			// its write landed; Value stays the length every existing
			// caller checks.
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

// freePort returns a loopback address for a host that does not exist yet,
// reserved so that nothing else can be handed the same port before the host
// binds it. Binding 127.0.0.1:0 and closing the socket would not; test/netport
// carries the reasoning and the measurements.
func freePort(t *testing.T) string {
	t.Helper()
	return netport.MustReserveUDP(t.Fatalf)
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
	// ca issues every host's leaf — its own keypair, its own node identity
	// (see test/testpki); configs keeps each host's leaf for its restart. ids
	// holds each host's identity, recorded when the host was first built: a
	// property of the leaf, so it survives a stop, and a founder's
	// Config.MemberIdentities has to name every member — stopped ones too.
	ca  *testpki.CA
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

// newTestCluster creates an N-node cluster with real QUIC transport.
func newTestCluster(t *testing.T, numNodes int) *testCluster {
	t.Helper()

	const maxRetries = 3
	ca := testpki.NewCA(t)
	leaves := ca.Leaves(t, numNodes)
	addrs := make([]string, numNodes)
	hosts := make([]*quicraft.Host, numNodes)
	configs := make([]config.HostConfig, numNodes)

	for i := range numNodes {
		var h *quicraft.Host
		var addr string
		var cfg config.HostConfig
		for attempt := range maxRetries {
			addr = freePort(t)
			cfg = testHostConfig(t, addr, leaves[i])
			var err error
			h, err = quicraft.NewHost(cfg, quicraft.WithoutEncryption())
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

	ids := make([]string, numNodes)
	for i, h := range hosts {
		ids[i] = h.NodeIdentity()
	}
	return &testCluster{
		hosts:   hosts,
		addrs:   addrs,
		configs: configs,
		stopped: make([]atomic.Bool, numNodes),
		ca:      ca,
		ids:     ids,
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

	addr := freePort(c.t)
	cfg := testHostConfig(c.t, addr, c.ca.Leaf(c.t))
	h, err := quicraft.NewHost(cfg, quicraft.WithoutEncryption())
	if err != nil {
		c.t.Fatalf("addNode: NewHost failed: %v", err)
	}

	newReplicaID := uint64(len(c.addrs) + 1)

	// The change goes through whichever node leads the shard at each attempt,
	// and is accepted only once a linearizable membership read confirms it.
	// addNode is called mid-test under sustained client load, where a transient
	// election leaves leaderHost returning nil for a few ticks; the loop asks
	// again rather than sampling once, and still fails — loudly — if the
	// change has not landed within membershipTimeout. See test/membership.
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

	// The precondition is on the TARGET: the caller chose a follower, and this
	// checks that it still is one. A leader is waited for rather than sampled
	// once — removeNode runs mid-test under sustained client load, where a
	// transient election leaves findLeader at -1 for a few ticks, and a single
	// sample turned that ordinary window into "no leader found". A leader that
	// has moved ONTO the target is still fatal: the test asked to remove a
	// follower, and the caller (not this helper) owns choosing one that is
	// current.
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
