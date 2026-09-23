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
// though it were — the class this repository has now been bitten by nine times
// (#84, #88, #90, #91, #103, #105, #107, #112, #121). In those the unpinned
// value was a leader index or a replica ID; here it is the shard configuration
// the budget is supposed to describe. A literal cannot follow a config change
// and gives no signal when it stops matching; a derivation cannot drift at all.
//
// This is the root module's twin of what #115 fixed in contrib/dragonboat
// (PR #124), and it mirrors that derivation on purpose. The two suites keep
// separate copies because contrib/dragonboat is a separate module that owns its
// own helpers; the shape is shared, the constants are not.
// ---------------------------------------------------------------------------

// defaultTimeout is the standard budget for a single Raft operation that is
// NOT an election: a proposal round trip, a lookup, a membership query. 15s
// gives headroom for QUIC connection establishment under Docker/CI resource
// contention. It is deliberately not expressed in election rounds, because
// none of its callers is waiting for an election.
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
// testElectionRTT and testRTTMillisecond. The derivation and the ~60 lines of
// reasoning behind it — why twice ElectionRTT, why thirty rounds, what was
// measured on #111 — used to be copied into this file and four others. They
// live in that package now (#133); the pin test below is the half that cannot
// move, because it reads THIS suite's config builders.
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

// testShardConfig creates a per-shard config for E2E testing.
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
// binds it. It used to bind 127.0.0.1:0 and close the socket, which is #131.
// test/netport carries the reasoning and the measurements.
func freePort(t *testing.T) string {
	t.Helper()
	return netport.MustReserveUDP(t.Fatalf)
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
	// ca issues every host's leaf; leaves holds each host's own leaf — its
	// own KEYPAIR, and so its own node identity — so a host brought back
	// for a replica presents the identity the shard committed for it. See
	// test/testpki.
	ca     *testpki.CA
	leaves []*config.MTLSConfig
	// ids holds each host's node identity, recorded when the host was first
	// built. It is a property of the leaf, so it survives a stop and a
	// restart, and a founder's Config.MemberIdentities has to name EVERY
	// member of the shard — stopped ones included.
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
// Host creation retries with a fresh port on bind failures (TOCTOU
// between freePort and NewHost).
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
		ca:      ca,
		leaves:  leaves,
		ids:     nodeIdentities(hosts),
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

// replicaIDAt returns the Raft replica ID that the node at idx runs for
// shardID, as the host itself reports it, and whether there is one.
//
// It is the suite's ONE index-to-replica-ID mapping, and it exists because
// idx+1 is not that mapping. idx+1 holds for the FOUNDING members only:
// startShard numbers them 1..N in slice order. Every replica added later by
// addNodeToCluster takes an explicit ID its caller chose, and this suite
// deliberately chooses IDs that do not follow the index —
// TestE2E_Partition_HealAndReconverge uses 10 and 11 for the hosts it appends
// at indices 5 and 6, "to avoid collision with original replica IDs".
//
// A helper that computes idx+1 therefore asks for a replica that never existed
// the moment a replacement node is the subject. proposeAndWait did exactly
// that and reported "leader state machine not found" for a proposal that had
// committed perfectly (#121).
//
// This is the repository's recurring flake class — a value that is not pinned
// treated as if it were (#84, #88, #90, #91, #103, #107, #112, #115) — with the
// slice index treated as the replica ID. The remedy is the one those took: read
// the value from whatever owns it instead of recomputing it. A host's replica
// ID for a shard is fixed when StartShard is called and never changes while the
// host runs, so GetShardInfo is authoritative here and — unlike IsLeader — is
// not a value that can move between the look and the use.
//
// ok is false when idx is out of range, the node is stopped, or the host is not
// running that shard. Each of those means "no replica here", which is a
// different answer from "replica idx+1", and callers that conflated the two are
// what #121 is about.
func (c *testCluster) replicaIDAt(shardID uint64, idx int) (uint64, bool) {
	c.hostsMu.RLock()
	defer c.hostsMu.RUnlock()
	if idx < 0 || idx >= len(c.hosts) {
		return 0, false
	}
	if c.stopped[idx].Load() {
		return 0, false
	}
	info, err := c.hosts[idx].GetShardInfo(shardID)
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

// awaitLeaderHostInterval is how often awaitLeaderHost re-asks. An election
// completes in roughly one election timeout, so polling faster only burns CPU
// that the shard needs to hold the election.
const awaitLeaderHostInterval = 5 * time.Millisecond

// awaitLeaderHost is leaderHost's blocking form: it returns the Host that leads
// shardID, waiting up to timeout for one to emerge, and nil if none does.
//
// It never touches *testing.T, so it is usable OFF the test goroutine -- which
// is the point, since waitForClusterLeader's t.Fatalf is illegal there.
// Returning nil rather than failing leaves the caller to decide what a
// leaderless shard means to it.
//
// It exists so a writer that finds a shard mid-election does not have to spend
// one of its writes on a proposal it never attempted. Counting those as losses
// is what made TestE2E_MultiShardFailover_UnderLoad score the runner's clock
// instead of the cluster's recovery (#107).
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
//
// The replica ID comes from replicaIDAt — the host's own answer — and falls
// back to i+1 only when the host cannot be asked at all (it was stopped, or it
// never started the shard). Deriving it arithmetically was #121.
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

		// Proposal committed. Wait for the leader's SM to apply.
		//
		// The lookup goes through resolveStateMachine, which asks the host
		// which replica it runs (replicaIDAt) instead of computing leaderIdx+1.
		// The arithmetic held only for the founding members, so when a node
		// added by addNodeToCluster won the election this asked the tracker for
		// a replica that never existed, got nil, and failed the test with
		// "leader state machine not found" for a proposal that had just
		// committed (#121).
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

// addNodeToCluster creates a new host, requests a membership addition from
// the leader, and starts the shard in join mode. The new host is appended
// to the cluster's hosts/addrs/trackers/stopped slices.
//
// opts state the new host's at-rest encryption. With none it declares
// WithoutEncryption, like the cluster's own hosts; a caller passing options
// must include WithBarrier (or WithoutEncryption) itself, and NewHost refuses
// one that does not.
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
	addr := freePort(t)
	leaf := cluster.ca.Leaf(t)
	hostCfg := testHostConfig(t, addr, leaf)
	if len(opts) == 0 {
		opts = []quicraft.HostOption{quicraft.WithoutEncryption()}
	}
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

// syncAddNode adds a voting replica through whichever node is CURRENTLY the
// leader and does not mistake a retry of an already-committed add for a
// failure.
//
// The decision is test/membership's, not this suite's: the leader is resolved
// at the point of each attempt, and a duplicate refusal ("already exists in
// membership") is accepted ONLY after an attempt in the same loop was
// genuinely submitted to raft AND a linearizable membership read shows the
// replica as a full voter at exactly addr. #112 found the second half missing
// from the loop that lived here; #122 wrote the decision for this suite, #135
// moved it to test/membership so the host and linearizability suites could
// share it, and #145 retired this suite's copy. This is the binding of that
// decision to testCluster.leaderHost, nothing more.
func syncAddNode(t *testing.T, cluster *testCluster, shardID, replicaID uint64, addr, identity string, timeout time.Duration) {
	t.Helper()
	membership.AddNode(t, cluster.leaderHost, shardID, replicaID, addr, identity, timeout)
}

// syncDeleteNode removes a replica through whichever node is CURRENTLY the
// leader, with the same idempotence as syncAddNode: "target node not found"
// is accepted only after a genuine submission and a linearizable read showing
// the replica holding no role at all.
func syncDeleteNode(t *testing.T, cluster *testCluster, shardID, replicaID uint64, timeout time.Duration) {
	t.Helper()
	membership.DeleteNode(t, cluster.leaderHost, shardID, replicaID, timeout)
}

// proposeUntilCommitted proposes one key/value through whichever node is
// CURRENTLY the leader and returns nil once the entry commits, or an error the
// caller should treat as fatal.
//
// It exists for the concurrent-writer goroutines, which used to pin the leader
// index captured at the start of the test and treat every error — including
// ErrNotLeader from a perfectly ordinary re-election — as a hard failure. That
// is not a weaker assertion than "no errors": every entry must still commit,
// and the caller still verifies all of them replicated afterwards. It only
// stops a legal leadership change from being reported as a lost write.
//
// Safe to call from a non-test goroutine: it never touches t.
//
// # Which retry policy this needs, and why it is not a third one
//
// This was the FOURTH copy of the propose-retry loop, and the fourth place a
// change to the retry policy had to be remembered (#110). It now runs on the
// same proposer.TryWith as the rest, and the predicate is chosen against the
// principle #97/#109 settled on: retry an error when, and only when, what the
// loop does between attempts can change that error's answer.
//
// Between attempts this loop does BOTH of the things the package's two
// remedies name. It re-resolves the leader, which is proposer.Try's remedy and
// fixes a not-leader rejection. And it waits, which is the pinned callers'
// remedy and fixes the conditions that clear on their own — a shard still
// coming up, a momentarily full pipeline, an election in flight. Its retryable
// set is therefore the union of the two, and because ErrNotLeader is itself
// inside quicraft.Temporary, that union IS quicraft.Temporary.
//
// So this needs no third predicate. It needs the SECOND one, paired with a
// re-resolving resolver — the combination neither of TryWith's original two
// callers uses, and the reason TryWith takes the resolver and the predicate as
// separate parameters rather than bundling them. Try's narrowness is right for
// Try because a caller asserting on one proposal wants a busy pipeline to
// reach it; this helper's callers assert that every entry commits WITHIN the
// budget, across partitions and heals, so a condition that clears on its own
// is something to wait through rather than to report.
//
// quicraft.Temporary is also narrower than the isTransientLeadershipError it
// replaces here, and deliberately so: that predicate retried ErrCanceled and a
// bare context cancellation, which no amount of waiting or re-resolving fixes.
// It is no longer narrower on the leadership-transfer case — #146 re-exported
// ErrTransferInProgress and put it in temporaryErrors, so Temporary now covers
// by sentinel what that predicate could only reach by substring.
//
// Re-proposal is safe for what this writes. A not-leader error can arrive after
// the entry already reached the log (quicraft.MaybeApplied — the form observed
// in practice here), so a retry may apply the command twice; every command this
// helper carries is a last-write-wins KV put.
func proposeUntilCommitted(cluster *testCluster, shardID uint64, key, value string, timeout time.Duration) error {
	_, err := proposer.TryWith(cluster.leaderHost, shardID, encodeKVPut(key, value),
		timeout, quicraft.Temporary)
	if err != nil {
		// Not "never committed within <timeout>": that phrasing was true only
		// for the deadline case and reported a permanent rejection as a slow
		// shard, which is the misdiagnosis #97 named. TryWith returns a
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

// waitForMembershipConverged waits until EVERY live replica in the cluster
// reports the same configuration: exactly wantNodes as voting members, and at
// least wantRemoved in the removed set.
//
// # Why this exists
//
// Every membership assertion in this package used to be made against ONE
// replica — cluster.hosts[leaderIdx], or whichever node happened to be live —
// through waitForMembershipSize. That asks the leader whether the leader
// applied the change, which it necessarily did: the leader is where the config
// change was proposed, and it applies it on commit. The interesting failure is
// the OTHER side of that: a config change that commits, applies on the leader,
// and never lands on a follower. That follower then sizes every quorum over a
// membership the rest of the shard has left — the exact denominator error that
// lets a minority elect, or lets an entry be reported committed on a count the
// real configuration does not accept. A single-replica assertion cannot see it,
// and neither could any test in this suite.
//
// Convergence, not instantaneous agreement, is the property being asserted:
// config changes apply on each replica when the entry commits there, so
// followers legitimately trail the leader by a replication round trip. The
// timeout is what separates "trailing" from "diverged".
//
// Replicas the harness has stopped are excluded — a dead node has no
// configuration to report. Everything still running is included, including
// nodes added mid-test by addNodeToCluster, because a newly joined replica
// holding a configuration that omits its own peers is precisely the divergence
// this is looking for.
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
		// A replica in wantRemoved is excluded: convergence is a property of
		// the CURRENT configuration, and once the removal commits the leader
		// stops replicating to the evicted replica, so whether it ever learns
		// of its own eviction depends on where it was in the replication stream
		// when the entry committed. Requiring it to agree asserts on a race.
		// Every replica still IN the configuration must converge — that is the
		// divergence being hunted.
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
			// Clean up previously created hosts.
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

		var h *quicraft.Host
		var addr string
		var cfg config.HostConfig
		for attempt := range maxRetries {
			addr = freePort(t)
			cfg = testHostConfig(t, addr, leaves[i])
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

// snapshotMetaFileName is the record the engine writes BESIDE each snapshot
// payload once the save that produced it has finished. Like the payload name it
// is duplicated here rather than read out of the engine, for the same reason.
const snapshotMetaFileName = "snapshot.meta"

// snapshotArtifacts returns every snapshot payload file beneath dir. The whole
// tree is walked rather than the documented path being reconstructed so the
// search still finds the artifacts if the layout changes.
//
// An entry that vanishes mid-walk is skipped rather than failed. Snapshot
// garbage collection removes whole snapshot directories concurrently with the
// walk, and a directory that no longer exists holds no bytes to assert over;
// failing on it would report a deleted artifact as an unreadable one. Every
// other walk error still fails.
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

// snapshotArtifactEpoch reads the at-rest encryption epoch out of the
// snapshot.meta record at metaPath. ok is false when the record is not (yet) a
// complete one, which is how a save that has not finished is recognised.
//
// The record is parsed positionally against its documented layout — an 8-byte
// magic, a 2-byte body version, 2 reserved bytes, a 4-byte body length, then
// little-endian uint64 body fields with Epoch sixth. The layout is restated
// here rather than decoded with the engine's own reader for the reason the file
// names are: a check that borrows the writer's decoder cannot notice the writer
// recording something other than what it stored.
//
// An epoch of 0 is a VALID read and means the artifact was stored in the CLEAR.
// It is never conflated with an unreadable record: the two are opposite in
// consequence, and only one of them is a finding.
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

// unfinishedSnapshotArtifacts returns, for each path whose snapshot directory
// does not yet describe itself, why it does not — a missing, empty, or
// half-written snapshot.meta beside snapshot.dat.
//
// A complete metadata record is the completion marker of every path that writes
// an artifact, and it is what separates "the save is still running" from "the
// save is done". A local save writes snapshot.dat, fsyncs it, encrypts it IN
// PLACE and only then creates snapshot.meta and writes the record into it; the
// receive and import paths assemble both files in a staging directory and
// publish them with one rename. So a payload with no complete record beside it
// is a save in flight, and its bytes are whatever the encrypt step has not
// replaced yet.
//
// The record must be COMPLETE, not merely present: writeSnapshotMetadata
// creates the file with O_TRUNC and writes into it afterwards, so an empty
// snapshot.meta is observable and means the same thing as no snapshot.meta.
func unfinishedSnapshotArtifacts(paths []string) []string {
	var pending []string
	for _, p := range paths {
		if _, ok, why := snapshotArtifactEpoch(filepath.Join(filepath.Dir(p), snapshotMetaFileName)); !ok {
			pending = append(pending, fmt.Sprintf("%s (%s)", p, why))
		}
	}
	return pending
}

// waitForFinishedSnapshotArtifacts blocks until dir holds at least one snapshot
// artifact AND every artifact beneath it has finished being written, then
// returns them all.
//
// Waiting for completion is NOT the same as ignoring what is incomplete. A
// directory that never becomes self-describing is a leaked plaintext artifact —
// the state a save killed between its plaintext fsync and its encrypt leaves
// behind — so the timeout FAILS and names the directories with the reason each
// was rejected, rather than dropping them from the search. The only thing the
// wait removes is the window in which a save this node is running right now has
// not finished.
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

// assertSnapshotsSealed fails unless dir holds at least one snapshot artifact,
// every artifact beneath it finished being written, each records a non-zero
// at-rest epoch, and none contains any of the plaintext needles.
//
// This is deliberately a check of the BYTES ON DISK rather than a round trip.
// A round trip through the engine reads back whatever it wrote, so it passes
// identically whether the artifact was sealed or written in the clear — which
// is exactly the plaintext downgrade at-rest encryption exists to prevent. The
// non-empty artifact count is part of the assertion: a search that finds no
// files to search is vacuous, not a pass.
//
// EVERY artifact under the node's whole tree is searched, not just the newest.
// A stale artifact left in the clear by an earlier save is exactly the leak an
// at-rest check exists to catch, and narrowing the search to the most recent
// index would step over it.
//
// The two checks per artifact are independent and neither implies the other.
// The recorded epoch says the encrypt path RAN and under which DEK; a zero
// epoch on a barrier-encrypted node is a plaintext downgrade whether or not
// this particular snapshot happens to hold a needle. The byte search says what
// reached the disk is not readable, which a recorded epoch alone would not
// prove.
//
// What the completion wait buys, and what it costs: the engine's local save
// path writes the state machine to snapshot.dat, fsyncs it, and encrypts it IN
// PLACE afterwards, so between those steps the canonical name holds a COMPLETE
// plaintext copy. Scanning during that window read a save in progress and
// reported it as a downgrade — 4 of 41 runs of this suite on a 24-core box at
// load average ~120 (issue #108). Waiting for every artifact to become
// self-describing removes that window WITHOUT dropping anything from the
// search: an artifact that never finishes fails the wait rather than being
// skipped, which is what keeps a save killed inside the window a failure here.
//
// The window ITSELF is a property of the product, not of this test, and is
// tracked as #116. Nothing about it is being asserted away: what this function
// no longer does is report a save that is running right now as a downgrade.
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
			// Snapshot garbage collection reclaimed the whole directory between
			// the walk and this read. A path that no longer exists holds no
			// bytes on disk, so there is nothing here to leak and nothing to
			// assert over; every other read error still fails.
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

// TestBudgetsDescribeTheClusterTheyBudgetFor pins the derivation at the top of
// this file to the configuration the suite actually starts.
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
