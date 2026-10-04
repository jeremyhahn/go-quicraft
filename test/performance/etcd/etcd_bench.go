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

package etcd_perf

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	crand "crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/binary"
	"encoding/pem"
	"errors"
	"fmt"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jeremyhahn/go-quicraft/test/performance/benchutil"
	"github.com/jeremyhahn/go-quicraft/test/performance/perfresult"
	"go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"
)

// tlsTransportState holds TLS transport resources created for the etcd/raft
// election benchmark. This simulates the production TLS transport overhead
// that etcd has in real deployments (gRPC with TLS), making the comparison
// with QuicRaft's mandatory QUIC/mTLS transport apples-to-apples.
//
// The TLS setup matches QuicRaft's self-signed certificate generation:
// ECDSA P-256 key, self-signed X.509 cert, TLS 1.3, listener + accept loop.
type tlsTransportState struct {
	listener net.Listener
	stopCh   chan struct{}
	done     sync.WaitGroup
}

// setupTLSTransport generates a self-signed ECDSA P-256 certificate and
// starts a TLS listener with an accept loop. This matches the transport
// initialization overhead in QuicRaft's NewQUICTransport:
//   - ECDSA P-256 key generation
//   - Self-signed X.509 certificate creation
//   - TLS listener binding (TCP, matching QUIC's UDP bind)
//   - Background accept loop goroutine
func setupTLSTransport() (*tlsTransportState, error) {
	key, err := ecdsa.GenerateKey(elliptic.P256(), crand.Reader)
	if err != nil {
		return nil, err
	}

	serialNumber, err := crand.Int(crand.Reader, new(big.Int).Lsh(big.NewInt(1), 128))
	if err != nil {
		return nil, err
	}

	template := &x509.Certificate{
		SerialNumber: serialNumber,
		Subject:      pkix.Name{CommonName: "etcd-bench-node"},
		NotBefore:    time.Now(),
		NotAfter:     time.Now().Add(24 * time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		DNSNames:     []string{"localhost"},
	}

	certDER, err := x509.CreateCertificate(crand.Reader, template, template, &key.PublicKey, key)
	if err != nil {
		return nil, err
	}

	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: certDER})
	keyDER, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		return nil, err
	}
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER})

	cert, err := tls.X509KeyPair(certPEM, keyPEM)
	if err != nil {
		return nil, err
	}

	tlsCfg := &tls.Config{
		Certificates: []tls.Certificate{cert},
		MinVersion:   tls.VersionTLS13,
	}

	//quicraft:direct-port the listener is KEPT for the transport's lifetime, so
	// the reserve-then-rebind class cannot occur here and netport is not
	// applicable. Nothing is closed and handed on: the socket opened by this
	// call is the socket the accept loop below serves, and the address peers
	// are given is read back from it while it is still open. The harnesses that
	// must name a port BEFORE the server exists -- Dragonboat and etcd's raft
	// peers -- go through benchutil.MustReserveTCPPort, which is netport.
	listener, err := tls.Listen("tcp", "127.0.0.1:0", tlsCfg)
	if err != nil {
		return nil, err
	}

	state := &tlsTransportState{
		listener: listener,
		stopCh:   make(chan struct{}),
	}

	state.done.Add(1)
	go func() {
		defer state.done.Done()
		for {
			conn, acceptErr := listener.Accept()
			if acceptErr != nil {
				select {
				case <-state.stopCh:
					return
				default:
					continue
				}
			}
			conn.Close()
		}
	}()

	return state, nil
}

// close shuts down the TLS transport: closes the listener, signals the
// accept loop to exit, and waits for goroutine cleanup.
func (s *tlsTransportState) close() {
	close(s.stopCh)
	s.listener.Close()
	s.done.Wait()
}

// walWriter provides a minimal file-based write-ahead log that performs
// sequential writes with fsync. This adds equivalent I/O overhead to
// what QuicRaft incurs through its sharded WAL.
type walWriter struct {
	f *os.File
	// lengthBuf is reused for encoding the 4-byte entry length prefix.
	lengthBuf [4]byte
}

// newWALWriter opens a new WAL file in the given directory.
func newWALWriter(dir string) (*walWriter, error) {
	path := filepath.Join(dir, "wal.bin")
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0600)
	if err != nil {
		return nil, fmt.Errorf("walWriter: open %s: %w", path, err)
	}
	return &walWriter{f: f}, nil
}

// persistReady writes both entries and hard state to the WAL in a single
// write+fsync cycle: entries first, then the hard state, then one
// (*os.File).Sync, which is fsync(2) on Linux. Every event loop calls it
// before it sends, applies or acknowledges anything from the same Ready, so
// no write is acknowledged before it is durable.
func (w *walWriter) persistReady(entries []*raftpb.Entry, hs *raftpb.HardState) error {
	// Write entries without fsyncing.
	for i := range entries {
		data, err := proto.Marshal(entries[i])
		if err != nil {
			return fmt.Errorf("walWriter: marshal entry index=%d: %w", entries[i].GetIndex(), err)
		}
		binary.LittleEndian.PutUint32(w.lengthBuf[:], uint32(len(data)))
		if _, err := w.f.Write(w.lengthBuf[:]); err != nil {
			return fmt.Errorf("walWriter: write length: %w", err)
		}
		if _, err := w.f.Write(data); err != nil {
			return fmt.Errorf("walWriter: write entry: %w", err)
		}
	}

	// Write hard state if non-empty.
	if !raft.IsEmptyHardState(hs) {
		data, err := proto.Marshal(hs)
		if err != nil {
			return fmt.Errorf("walWriter: marshal hard state: %w", err)
		}
		binary.LittleEndian.PutUint32(w.lengthBuf[:], uint32(len(data)))
		if _, err := w.f.Write(w.lengthBuf[:]); err != nil {
			return fmt.Errorf("walWriter: write hs length: %w", err)
		}
		if _, err := w.f.Write(data); err != nil {
			return fmt.Errorf("walWriter: write hs data: %w", err)
		}
	}

	// Single fsync for both entries and hard state.
	if err := w.f.Sync(); err != nil {
		return fmt.Errorf("walWriter: fsync ready: %w", err)
	}
	return nil
}

// close flushes and closes the underlying file.
func (w *walWriter) close() error {
	return w.f.Close()
}

// benchStateMachineEtcd is a key-value state machine that performs the same
// per-entry work as the QuicRaft and Dragonboat benchmark state machines:
// it parses the KV command, copies the value, and inserts it into a
// map[string][]byte. This keeps the apply hot path apples-to-apples across
// all benchmarked engines.
type benchStateMachineEtcd struct {
	mu      sync.Mutex
	data    map[string][]byte
	applied uint64
}

// newBenchStateMachineEtcd creates a state machine with a pre-allocated
// map for storing applied key-value pairs.
func newBenchStateMachineEtcd() *benchStateMachineEtcd {
	return &benchStateMachineEtcd{
		data: make(map[string][]byte, 1024),
	}
}

// apply processes committed entries through the state machine. For each
// normal entry it parses the KV command, copies the value, and stores it
// keyed by the command key, matching the work done by the other engines'
// state machines.
func (sm *benchStateMachineEtcd) apply(entries []*raftpb.Entry) {
	sm.mu.Lock()
	for _, entry := range entries {
		if entry.GetType() == raftpb.EntryNormal && len(entry.Data) > 0 {
			_, key, value := parseKVCmd(entry.Data)
			if key != nil {
				owned := make([]byte, len(value))
				copy(owned, value)
				sm.data[string(key)] = owned
			}
			sm.applied = entry.GetIndex()
		}
	}
	sm.mu.Unlock()
}

// get returns the value stored under key, or nil if none.
func (sm *benchStateMachineEtcd) get(key []byte) []byte {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	return sm.data[string(key)]
}

// errApplyTimeout reports that a proposal was accepted by raft but its own
// entry was not applied on the proposing node within the wait budget.
var errApplyTimeout = errors.New("etcd bench: proposal not applied within timeout")

// proposalApplyTimeout bounds how long a proposer waits for its own entry to
// be applied before the attempt is counted as failed.
const proposalApplyTimeout = 10 * time.Second

// nextRequestID issues the request ids embedded in proposals and read
// contexts. Ids are unique across every node in the process; zero is never
// issued.
var nextRequestID atomic.Uint64

// waiters routes completion of one request, identified by its id, to the one
// goroutine waiting for it. The apply loop completes proposal waiters with the
// applied entry's index; the Ready loop completes read waiters with a
// ReadState's index. This is the per-proposal acknowledgement raw etcd/raft
// does not provide, so a proposer's latency ends when ITS entry applies, not
// when the node's applied count reaches some number another proposer's entry
// could satisfy.
type waiters struct {
	mu sync.Mutex
	m  map[uint64]chan uint64
}

// newWaiters returns an empty waiter registry.
func newWaiters() *waiters {
	return &waiters{m: make(map[uint64]chan uint64)}
}

// register returns the channel on which id's completion value is delivered.
// It must be called before the request is submitted so completion cannot
// overtake it.
func (w *waiters) register(id uint64) <-chan uint64 {
	ch := make(chan uint64, 1)
	w.mu.Lock()
	w.m[id] = ch
	w.mu.Unlock()
	return ch
}

// cancel forgets id after its waiter has given up.
func (w *waiters) cancel(id uint64) {
	w.mu.Lock()
	delete(w.m, id)
	w.mu.Unlock()
}

// complete delivers value to id's waiter, if one is registered.
func (w *waiters) complete(id, value uint64) {
	w.mu.Lock()
	ch, ok := w.m[id]
	if ok {
		delete(w.m, id)
	}
	w.mu.Unlock()
	if ok {
		ch <- value
	}
}

// completeEntries completes the waiter of every applied normal entry that
// carries a request id, delivering the entry's index.
func (w *waiters) completeEntries(entries []*raftpb.Entry) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if len(w.m) == 0 {
		return
	}
	for _, entry := range entries {
		if entry.GetType() != raftpb.EntryNormal {
			continue
		}
		id, ok := requestID(entry.Data)
		if !ok {
			continue
		}
		if ch, found := w.m[id]; found {
			delete(w.m, id)
			ch <- entry.GetIndex()
		}
	}
}

// completeReadStates completes the waiter of each ReadState whose request
// context is a request id, delivering the state's read index.
func (w *waiters) completeReadStates(states []raft.ReadState) {
	for _, rs := range states {
		if len(rs.RequestCtx) == 8 {
			w.complete(binary.BigEndian.Uint64(rs.RequestCtx), rs.Index)
		}
	}
}

// newRequest builds the 128-byte command for key carrying a fresh request
// id. Callers build it before starting their latency clock.
func newRequest(key []byte) (uint64, []byte) {
	id := nextRequestID.Add(1)
	return id, makeRequestCmd(key, id)
}

// proposeAndWait proposes cmd, built by newRequest with id, on node and blocks
// until that entry has been persisted, committed and applied on node. A
// rejected proposal returns raft's error; an accepted proposal whose entry
// does not apply within timeout returns errApplyTimeout.
func proposeAndWait(node raft.Node, w *waiters, id uint64, cmd []byte, timeout time.Duration) error {
	done := w.register(id)
	if err := node.Propose(context.Background(), cmd); err != nil {
		w.cancel(id)
		return err
	}
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case <-done:
		return nil
	case <-timer.C:
		w.cancel(id)
		return errApplyTimeout
	}
}

// etcdRaftTestNode bundles an etcd/raft Node, its MemoryStorage, WAL
// writer, state machine, and a stop channel for the event loop and ticker
// goroutines.
type etcdRaftTestNode struct {
	node        raft.Node
	storage     *raft.MemoryStorage
	wal         *walWriter
	sm          *benchStateMachineEtcd
	confTracker *confChangeTracker
	stopCh      chan struct{}
	loopDone    sync.WaitGroup
	tickDone    chan struct{}
	stopped     atomic.Bool

	// proposals is signalled by the event loop as each proposal's own entry
	// is applied.
	proposals *waiters

	// reads is signalled by the event loop with the index of each ReadState
	// returned for a ReadIndex request.
	reads *waiters

	// appliedIndex is the index of the last entry the event loop applied.
	appliedIndex atomic.Uint64
}

// benchElectionTick, benchHeartbeatTick and benchTickInterval are the raft
// timing every node in this harness runs with, at every cluster size: a 1 ms
// tick, a 10-tick election timeout (etcd/raft randomizes it in [10, 20)), and
// a 1-tick heartbeat. They match QuicRaft's and Dragonboat's RTTMillisecond 1,
// ElectionRTT 10, HeartbeatRTT 1.
const (
	benchElectionTick  = 10
	benchHeartbeatTick = 1
	benchTickInterval  = 1 * time.Millisecond
)

// readIndexTimeout bounds one ReadIndex attempt. A request dropped by an
// election is never answered, so a retry, not a long wait, recovers it.
const readIndexTimeout = 2 * time.Second

// setupWarmupProposals is the number of proposals every harness's setup
// warmup runs, before its one linearizable read, ahead of any timer.
const setupWarmupProposals = 10

// benchRaftConfig returns the raft.Config of node id. Every node in this
// harness, single-node or clustered, WAL or memory, uses it, so the timing,
// CheckQuorum and PreVote are pinned in one place.
func benchRaftConfig(id uint64, storage *raft.MemoryStorage) *raft.Config {
	return &raft.Config{
		ID:              id,
		ElectionTick:    benchElectionTick,
		HeartbeatTick:   benchHeartbeatTick,
		Storage:         storage,
		MaxSizePerMsg:   1024 * 1024,
		MaxInflightMsgs: 256,
		CheckQuorum:     true,
		PreVote:         true,
	}
}

// runTicker calls node.Tick every benchTickInterval until stopCh closes, then
// closes done.
func runTicker(node raft.Node, stopCh <-chan struct{}, done chan<- struct{}) {
	defer close(done)
	ticker := time.NewTicker(benchTickInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			node.Tick()
		case <-stopCh:
			return
		}
	}
}

// linearizableRead performs an etcd/raft ReadIndex read of key: it requests a
// read index, waits for the matching ReadState, waits until the node has
// applied through that index, and then reads the state machine.
func linearizableRead(node raft.Node, reads *waiters, applied *atomic.Uint64, sm *benchStateMachineEtcd, key []byte, timeout time.Duration) ([]byte, error) {
	id := nextRequestID.Add(1)
	var rctx [8]byte
	binary.BigEndian.PutUint64(rctx[:], id)

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	done := reads.register(id)
	if err := node.ReadIndex(ctx, rctx[:]); err != nil {
		reads.cancel(id)
		return nil, err
	}
	var index uint64
	select {
	case index = <-done:
	case <-ctx.Done():
		reads.cancel(id)
		return nil, ctx.Err()
	}

	ticker := time.NewTicker(50 * time.Microsecond)
	defer ticker.Stop()
	for applied.Load() < index {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-ticker.C:
		}
	}
	return sm.get(key), nil
}

// setupWarmup is the setup warmup every harness runs before any timer:
// exactly setupWarmupProposals proposals, each waiting for its own entry to
// apply on the shared warmup retry budget, then one linearizable read.
func setupWarmup(node raft.Node, proposals, reads *waiters, applied *atomic.Uint64, sm *benchStateMachineEtcd) error {
	for i := 0; i < setupWarmupProposals; i++ {
		key := []byte(fmt.Sprintf("warmup-%d", i))
		err := benchutil.Retry(benchutil.WarmupAttempts, benchutil.WarmupBackoff, func() error {
			id, cmd := newRequest(key)
			return proposeAndWait(node, proposals, id, cmd, proposalApplyTimeout)
		})
		if err != nil {
			return fmt.Errorf("warmup proposal %d: %w", i, err)
		}
	}
	// A ReadIndex request lost to a leadership change is dropped without an
	// error, so the read gets the same retry budget as the proposals.
	err := benchutil.Retry(benchutil.WarmupAttempts, benchutil.WarmupBackoff, func() error {
		_, rerr := linearizableRead(node, reads, applied, sm, []byte("warmup-0"), readIndexTimeout)
		return rerr
	})
	if err != nil {
		return fmt.Errorf("warmup linearizable read: %w", err)
	}
	return nil
}

// setupEtcdRaftNode creates a single-node etcd/raft cluster with both
// in-memory storage (for raft internals) and a file-based WAL (for I/O
// parity with QuicRaft). See newEtcdRaftTestNode.
func setupEtcdRaftNode(b *testing.B) *etcdRaftTestNode {
	b.Helper()
	wal, err := newWALWriter(b.TempDir())
	if err != nil {
		b.Fatalf("newWALWriter failed: %v", err)
	}
	return newEtcdRaftTestNode(b, wal)
}

// setupEtcdRaftNodeMemory creates a single-node etcd/raft cluster with
// in-memory storage only (no WAL file). This benchmarks pure algorithm
// performance without disk I/O overhead. See newEtcdRaftTestNode.
func setupEtcdRaftNodeMemory(b *testing.B) *etcdRaftTestNode {
	b.Helper()
	return newEtcdRaftTestNode(b, nil)
}

// newEtcdRaftTestNode starts a single-node etcd/raft cluster persisting to
// wal (nil for memory mode). It starts the event-loop goroutine that persists
// entries, applies committed entries through a state machine and advances the
// raft node, and the 1 ms ticker. Leadership comes from the randomized
// election timeout, as in every harness (no Campaign shortcut). It then runs
// the setup warmup. The node is stopped by b.Cleanup.
func newEtcdRaftTestNode(b *testing.B, wal *walWriter) *etcdRaftTestNode {
	b.Helper()

	storage := raft.NewMemoryStorage()
	node := raft.StartNode(benchRaftConfig(1, storage), []raft.Peer{{ID: 1}})

	tn := &etcdRaftTestNode{
		node:        node,
		storage:     storage,
		wal:         wal,
		sm:          newBenchStateMachineEtcd(),
		confTracker: newConfChangeTracker(),
		stopCh:      make(chan struct{}),
		tickDone:    make(chan struct{}),
		proposals:   newWaiters(),
		reads:       newWaiters(),
	}

	tn.loopDone.Add(1)
	go func() {
		defer tn.loopDone.Done()
		tn.eventLoop()
	}()
	go runTicker(node, tn.stopCh, tn.tickDone)

	b.Cleanup(tn.stop)

	if !tn.waitForConfChange(5 * time.Second) {
		b.Fatalf("etcd/raft: initial ConfChange not processed within timeout")
	}

	leaderTicker := time.NewTicker(1 * time.Millisecond)
	defer leaderTicker.Stop()
	leaderDeadline := time.NewTimer(10 * time.Second)
	defer leaderDeadline.Stop()
	for node.Status().Lead != 1 {
		select {
		case <-leaderDeadline.C:
			b.Fatalf("etcd/raft single-node failed to elect leader: lead=%d", node.Status().Lead)
		case <-leaderTicker.C:
		}
	}

	if err := setupWarmup(node, tn.proposals, tn.reads, &tn.appliedIndex, tn.sm); err != nil {
		b.Fatalf("etcd/raft single-node setup warmup failed: %v", err)
	}
	return tn
}

// confChangeTracker counts applied ConfChange entries and signals when
// the expected number of ConfChanges has been processed. In a single-node
// cluster, expected is 1. In an N-node cluster, expected is N because
// StartNode generates one ConfChange per bootstrap peer.
type confChangeTracker struct {
	mu       sync.Mutex
	cond     *sync.Cond
	count    int
	expected int
}

// newConfChangeTracker creates a tracker that signals after 1 ConfChange
// (used by single-node benchmarks).
func newConfChangeTracker() *confChangeTracker {
	return newConfChangeTrackerWithCount(1)
}

// newConfChangeTrackerWithCount creates a tracker that signals after
// the specified number of ConfChange entries have been applied.
func newConfChangeTrackerWithCount(expected int) *confChangeTracker {
	t := &confChangeTracker{expected: expected}
	t.cond = sync.NewCond(&t.mu)
	return t
}

func (t *confChangeTracker) signal() {
	t.mu.Lock()
	t.count++
	if t.count >= t.expected {
		t.cond.Broadcast()
	}
	t.mu.Unlock()
}

func (t *confChangeTracker) wait(timeout time.Duration) bool {
	ticker := time.NewTicker(500 * time.Microsecond)
	defer ticker.Stop()
	deadline := time.NewTimer(timeout)
	defer deadline.Stop()
	for {
		t.mu.Lock()
		done := t.count >= t.expected
		t.mu.Unlock()
		if done {
			return true
		}
		select {
		case <-deadline.C:
			return false
		case <-ticker.C:
		}
	}
}

// waitForConfChange blocks until the event loop has processed the
// initial ConfChange entry or the timeout expires.
func (tn *etcdRaftTestNode) waitForConfChange(timeout time.Duration) bool {
	return tn.confTracker.wait(timeout)
}

// eventLoop processes Ready structs from the etcd/raft node. For each
// Ready batch it:
//  1. Persists entries and hard state to WAL with single fsync -- skipped when wal is nil
//  2. Appends entries to MemoryStorage (raft internal requirement)
//  3. Applies committed entries through the state machine
//  4. Signals the waiter of each proposal whose entry was applied
//  5. Advances the raft node
//
// When wal is nil (memory mode), step 1 is skipped to benchmark
// pure in-memory raft performance.
func (tn *etcdRaftTestNode) eventLoop() {
	for {
		select {
		case rd, ok := <-tn.node.Ready():
			if !ok {
				// Node was stopped; Ready channel closed.
				return
			}

			// Step 1: Persist entries and hard state with single fsync.
			hasEntries := len(rd.Entries) > 0
			hasHardState := !raft.IsEmptyHardState(rd.HardState)
			if tn.wal != nil && (hasEntries || hasHardState) {
				if err := tn.wal.persistReady(rd.Entries, rd.HardState); err != nil {
					// WAL closed during shutdown -- exit gracefully.
					return
				}
			}

			// Persist hard state to MemoryStorage regardless of WAL mode.
			if hasHardState {
				tn.storage.SetHardState(rd.HardState)
			}

			// Step 2: Append entries to MemoryStorage for raft internals.
			if hasEntries {
				tn.storage.Append(rd.Entries)
			}

			// Step 3: Apply committed entries through the state machine.
			if len(rd.CommittedEntries) > 0 {
				for _, entry := range rd.CommittedEntries {
					if entry.GetType() == raftpb.EntryConfChange {
						var cc raftpb.ConfChange
						if err := proto.Unmarshal(entry.Data, &cc); err == nil {
							tn.node.ApplyConfChange(&cc)
						}
						tn.confTracker.signal()
					}
				}

				// Batch-apply all normal entries through state machine.
				tn.sm.apply(rd.CommittedEntries)

				tn.appliedIndex.Store(rd.CommittedEntries[len(rd.CommittedEntries)-1].GetIndex())

				// Step 4: Wake each proposer whose own entry just applied.
				tn.proposals.completeEntries(rd.CommittedEntries)
			}

			// Hand each ReadIndex result to its reader.
			tn.reads.completeReadStates(rd.ReadStates)

			// Step 5: Advance the raft node.
			tn.node.Advance()

		case <-tn.stopCh:
			return
		}
	}
}

// stop shuts down the raft node, waits for the event loop goroutine
// to exit, then closes the WAL file. This ordering prevents the race
// where the event loop writes to a closed WAL file.
func (tn *etcdRaftTestNode) stop() {
	if !tn.stopped.CompareAndSwap(false, true) {
		return
	}
	tn.node.Stop()
	close(tn.stopCh)
	tn.loopDone.Wait()
	<-tn.tickDone
	if tn.wal != nil {
		tn.wal.close()
	}
}

// benchEtcdRaftPropose runs sequential proposals through etcd/raft,
// recording per-operation latency. Each proposal is timed from the
// Propose call until its own entry has been persisted, committed and
// applied through the state machine.
func benchEtcdRaftPropose(b *testing.B, tn *etcdRaftTestNode, payloadSize int, rec *perfresult.LatencyRecorder) {
	b.Helper()

	// Every command is proposalBytes (128B), identical in size and layout to
	// the commands proposed by the other engines. payloadSize is retained in
	// the signature for call-site symmetry; the shared proposalBytes constant
	// governs the actual entry size.
	_ = payloadSize
	key := []byte("bench-key")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		id, cmd := newRequest(key)
		start := time.Now()
		if err := proposeAndWait(tn.node, tn.proposals, id, cmd, proposalApplyTimeout); err != nil {
			b.Fatalf("etcd propose failed at iteration %d: %v", i, err)
		}
		rec.Record(time.Since(start))
	}
	b.StopTimer()
}

// benchEtcdRaftConcurrentPropose runs concurrent proposals from multiple
// goroutines, recording aggregate latency. Each goroutine proposes under its
// own key (key-%d, by writer index) and waits for its own entry to be fully
// applied (WAL persist + commit + state machine) before recording the
// latency sample.
func benchEtcdRaftConcurrentPropose(b *testing.B, tn *etcdRaftTestNode, payloadSize int, numGoroutines int, rec *perfresult.LatencyRecorder) {
	b.Helper()

	// See benchEtcdRaftPropose: proposalBytes governs the entry size.
	_ = payloadSize

	b.ResetTimer()

	var wg sync.WaitGroup
	opsPerGoroutine := b.N / numGoroutines
	remainder := b.N % numGoroutines
	var errCount atomic.Int64

	for g := 0; g < numGoroutines; g++ {
		ops := opsPerGoroutine
		if g < remainder {
			ops++
		}
		if ops == 0 {
			continue
		}

		wg.Add(1)
		go func(writer, n int) {
			defer wg.Done()
			key := []byte(fmt.Sprintf("key-%d", writer))
			for i := 0; i < n; i++ {
				id, cmd := newRequest(key)
				start := time.Now()
				if err := proposeAndWait(tn.node, tn.proposals, id, cmd, proposalApplyTimeout); err != nil {
					errCount.Add(1)
					continue
				}
				rec.Record(time.Since(start))
			}
		}(g, ops)
	}
	wg.Wait()
	b.StopTimer()

	if count := errCount.Load(); count > 0 {
		b.Logf("warning: %d/%d proposals failed during concurrent benchmark", count, b.N)
	}
}

// benchEtcdRaftElection measures the time from node creation to leader
// election via natural randomized election timeout (no Campaign shortcut).
// Each iteration opens a fresh WAL file before the timer starts; the timer
// covers StartNode, the durable writes of the election itself, and the wait
// for a leader. A ticker goroutine drives node.Tick() at 1ms intervals
// matching QuicRaft's RTTMillisecond.
func benchEtcdRaftElection(b *testing.B, rec *perfresult.LatencyRecorder) {
	b.Helper()

	for i := 0; i < b.N; i++ {
		storage := raft.NewMemoryStorage()

		cfg := benchRaftConfig(1, storage)

		peers := []raft.Peer{{ID: 1}}

		// Initialize TLS transport and open the WAL file outside the timed
		// region. QuicRaft and Dragonboat create their transport
		// infrastructure (cert generation, listener binding) and storage
		// directories outside their election timers and time only the replica
		// start plus the wait for a leader, so etcd must too.
		ts, tlsErr := setupTLSTransport()
		if tlsErr != nil {
			b.Fatalf("setupTLSTransport failed: %v", tlsErr)
		}
		wal, err := newWALWriter(b.TempDir())
		if err != nil {
			ts.close()
			b.Fatalf("newWALWriter failed in election bench: %v", err)
		}

		start := time.Now()

		node := raft.StartNode(cfg, peers)

		// Start event loop that processes ConfChange entries and persists
		// to WAL.
		stopCh := make(chan struct{})
		var loopDone sync.WaitGroup
		loopDone.Add(1)
		go func() {
			defer loopDone.Done()
			for {
				select {
				case rd := <-node.Ready():
					// Persist entries and hard state with one fsync, the
					// same durable path as the steady-state event loops.
					if len(rd.Entries) > 0 || !raft.IsEmptyHardState(rd.HardState) {
						if walErr := wal.persistReady(rd.Entries, rd.HardState); walErr != nil {
							return
						}
					}
					if !raft.IsEmptyHardState(rd.HardState) {
						storage.SetHardState(rd.HardState)
					}
					if len(rd.Entries) > 0 {
						storage.Append(rd.Entries)
					}
					for _, entry := range rd.CommittedEntries {
						if entry.GetType() == raftpb.EntryConfChange {
							var cc raftpb.ConfChange
							if unmarshalErr := proto.Unmarshal(entry.Data, &cc); unmarshalErr == nil {
								node.ApplyConfChange(&cc)
							}
						}
					}
					node.Advance()
				case <-stopCh:
					return
				}
			}
		}()

		// Start a ticker goroutine that drives raft ticks at the same
		// rate as QuicRaft's RTTMillisecond (1ms). This allows etcd/raft
		// to trigger its natural randomized election timeout instead of
		// using the explicit Campaign() shortcut, giving a fair
		// apples-to-apples election time comparison.
		tickDone := make(chan struct{})
		go runTicker(node, stopCh, tickDone)

		// Wait for leader via natural election timeout (no Campaign).
		electionTicker := time.NewTicker(1 * time.Millisecond)
		electionDeadline := time.NewTimer(10 * time.Second)
		elected := false
	electionPoll:
		for {
			status := node.Status()
			if status.Lead == 1 {
				elected = true
				electionTicker.Stop()
				electionDeadline.Stop()
				break electionPoll
			}
			select {
			case <-electionDeadline.C:
				electionTicker.Stop()
				break electionPoll
			case <-electionTicker.C:
			}
		}

		elapsed := time.Since(start)
		rec.Record(elapsed)

		// Stop the node first (drains Ready channel), then wait for
		// all goroutines to exit before closing the WAL file.
		node.Stop()
		close(stopCh)
		loopDone.Wait()
		<-tickDone
		wal.close()
		ts.close()

		if !elected {
			b.Fatalf("etcd/raft election did not complete within timeout (iteration %d)", i)
		}
	}
}

// benchEtcdRaftCommitLatency measures per-proposal commit latency
// (propose to its own entry applied through WAL + state machine). This is
// equivalent to QuicRaft's SyncPropose latency which includes WAL
// persistence, raft commit, and state machine application.
func benchEtcdRaftCommitLatency(b *testing.B, tn *etcdRaftTestNode, payloadSize int, rec *perfresult.LatencyRecorder) {
	b.Helper()

	// See benchEtcdRaftPropose: proposalBytes governs the entry size.
	_ = payloadSize
	key := []byte("commit-key")

	// Warm up: prime the pipeline and WAL file. The same retry budget as
	// all engines tolerates transient startup errors; each attempt waits for
	// its own entry to apply, so a failed attempt is retried.
	for w := 0; w < commitLatencyWarmup; w++ {
		err := benchutil.Retry(benchutil.WarmupAttempts, benchutil.WarmupBackoff, func() error {
			id, cmd := newRequest(key)
			return proposeAndWait(tn.node, tn.proposals, id, cmd, proposalApplyTimeout)
		})
		if err != nil {
			b.Fatalf("warmup propose failed: %v", err)
		}
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		id, cmd := newRequest(key)
		start := time.Now()
		if err := proposeAndWait(tn.node, tn.proposals, id, cmd, proposalApplyTimeout); err != nil {
			b.Fatalf("etcd commit latency propose failed at iteration %d: %v", i, err)
		}
		rec.Record(time.Since(start))
	}
	b.StopTimer()

	if rec.Count() > 0 {
		b.Logf("etcd/raft commit latency: P50=%s P90=%s P99=%s mean=%s (n=%d)",
			perfresult.FormatDuration(rec.P50()), perfresult.FormatDuration(rec.P90()),
			perfresult.FormatDuration(rec.P99()), perfresult.FormatDuration(rec.Mean()), rec.Count())
	}
}

// ---------------------------------------------------------------------------
// Multi-Node Cluster Benchmark Functions
// ---------------------------------------------------------------------------

// benchEtcdRaftElectionCluster measures multi-node election time. The
// cluster infrastructure (TLS material, reserved addresses, transport
// listeners) is created once outside the timed loop, as QuicRaft and
// Dragonboat create their hosts once. Each iteration prepares a fresh set
// of nodes (storage, WAL files) outside the timer, then times only starting
// every node and waiting until a leader is known, then stops the nodes.
// Each round uses new raft ids so traffic left over from the previous round
// is dropped by the transport instead of reaching the new nodes, the way
// Dragonboat gives each round its own shard ID.
func benchEtcdRaftElectionCluster(b *testing.B, numNodes int, rec *perfresult.LatencyRecorder) {
	b.Helper()

	infra := newEtcdClusterInfra(b, numNodes)
	defer infra.close()

	for i := 0; i < b.N; i++ {
		cluster := newEtcdCluster(b, infra, uint64(i*numNodes))

		start := time.Now()
		cluster.start()
		_, err := cluster.waitForLeader(clusterLeaderTimeout)
		elapsed := time.Since(start)

		cluster.stopNodes()
		if err != nil {
			b.Fatalf("cluster leader election failed at iteration %d: %v", i, err)
		}
		rec.Record(elapsed)
	}
}

// benchEtcdRaftProposeCluster measures sequential proposal throughput
// through the cluster leader with TCP+TLS transport and WAL persistence.
// Each proposal is timed from Propose through quorum replication, commit,
// and application of its own entry on the leader node. The leader is
// re-discovered when raft rejects a proposal.
func benchEtcdRaftProposeCluster(b *testing.B, cluster *EtcdCluster, payloadSize int, rec *perfresult.LatencyRecorder) {
	b.Helper()

	// See benchEtcdRaftPropose: proposalBytes governs the entry size.
	_ = payloadSize
	key := []byte("bench-key")

	leader, findErr := cluster.CurrentLeaderNode()
	if findErr != nil {
		b.Fatalf("no leader at benchmark start: %v", findErr)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		id, cmd := newRequest(key)
		start := time.Now()
		var err error
		leader, err = cluster.proposeAndWait(leader, id, cmd)
		if err != nil {
			b.Fatalf("cluster propose failed at iteration %d: %v", i, err)
		}
		rec.Record(time.Since(start))
	}
	b.StopTimer()
}

// benchEtcdRaftConcurrentProposeCluster runs concurrent proposals from
// multiple goroutines through the cluster leader. Each goroutine proposes
// under its own key (key-%d, by writer index) and waits for its own entry
// to be committed and applied before recording the latency sample. The
// leader is re-discovered when raft rejects a proposal.
func benchEtcdRaftConcurrentProposeCluster(b *testing.B, cluster *EtcdCluster, payloadSize int, numGoroutines int, rec *perfresult.LatencyRecorder) {
	b.Helper()

	// See benchEtcdRaftPropose: proposalBytes governs the entry size.
	_ = payloadSize

	// leaderRef is an atomically-swapped pointer to the current leader.
	// All goroutines read from this and update it on leadership change.
	var leaderRef atomic.Pointer[etcdClusterNode]
	initialLeader, findErr := cluster.CurrentLeaderNode()
	if findErr != nil {
		b.Fatalf("no leader at benchmark start: %v", findErr)
	}
	leaderRef.Store(initialLeader)

	b.ResetTimer()

	var wg sync.WaitGroup
	opsPerGoroutine := b.N / numGoroutines
	remainder := b.N % numGoroutines
	var errCount atomic.Int64

	for g := 0; g < numGoroutines; g++ {
		ops := opsPerGoroutine
		if g < remainder {
			ops++
		}
		if ops == 0 {
			continue
		}

		wg.Add(1)
		go func(writer, n int) {
			defer wg.Done()
			key := []byte(fmt.Sprintf("key-%d", writer))
			for i := 0; i < n; i++ {
				id, cmd := newRequest(key)
				start := time.Now()
				current := leaderRef.Load()
				used, err := cluster.proposeAndWait(current, id, cmd)
				if used != current {
					leaderRef.CompareAndSwap(current, used)
				}
				if err != nil {
					errCount.Add(1)
					continue
				}
				rec.Record(time.Since(start))
			}
		}(g, ops)
	}
	wg.Wait()
	b.StopTimer()

	if count := errCount.Load(); count > 0 {
		b.Logf("warning: %d/%d proposals failed during cluster concurrent benchmark", count, b.N)
	}
}

// benchEtcdRaftCommitLatencyCluster measures per-proposal commit latency
// through the cluster leader. A 100-proposal warmup primes the TLS
// connections and WAL before measurement begins. The leader is
// re-discovered when raft rejects a proposal.
func benchEtcdRaftCommitLatencyCluster(b *testing.B, cluster *EtcdCluster, payloadSize int, rec *perfresult.LatencyRecorder) {
	b.Helper()

	// See benchEtcdRaftPropose: proposalBytes governs the entry size.
	_ = payloadSize
	key := []byte("commit-key")

	leader, findErr := cluster.CurrentLeaderNode()
	if findErr != nil {
		b.Fatalf("no leader at benchmark start: %v", findErr)
	}

	// Warm up on the same retry budget as all engines; each attempt waits
	// for its own entry to apply, so a failed attempt is retried.
	for w := 0; w < commitLatencyWarmup; w++ {
		err := benchutil.Retry(benchutil.WarmupAttempts, benchutil.WarmupBackoff, func() error {
			id, cmd := newRequest(key)
			var perr error
			leader, perr = cluster.proposeAndWait(leader, id, cmd)
			return perr
		})
		if err != nil {
			b.Fatalf("cluster warmup propose failed: %v", err)
		}
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		id, cmd := newRequest(key)
		start := time.Now()
		var err error
		leader, err = cluster.proposeAndWait(leader, id, cmd)
		if err != nil {
			b.Fatalf("cluster commit latency propose failed at iteration %d: %v", i, err)
		}
		rec.Record(time.Since(start))
	}
	b.StopTimer()

	if rec.Count() > 0 {
		b.Logf("etcd/raft cluster commit latency: P50=%s P90=%s P99=%s mean=%s (n=%d)",
			perfresult.FormatDuration(rec.P50()), perfresult.FormatDuration(rec.P90()),
			perfresult.FormatDuration(rec.P99()), perfresult.FormatDuration(rec.Mean()), rec.Count())
	}
}

// benchEtcdRaftElectionMemory measures the time from node creation to
// leader election via natural randomized election timeout (no Campaign
// shortcut) without any WAL I/O. A ticker goroutine drives node.Tick()
// at 1ms intervals matching QuicRaft's RTTMillisecond.
func benchEtcdRaftElectionMemory(b *testing.B, rec *perfresult.LatencyRecorder) {
	b.Helper()

	for i := 0; i < b.N; i++ {
		storage := raft.NewMemoryStorage()

		cfg := benchRaftConfig(1, storage)

		peers := []raft.Peer{{ID: 1}}

		// Initialize TLS transport outside the timed region (see WAL election
		// benchmark) so the measurement excludes one-time cert generation and
		// listener binding, matching QuicRaft and Dragonboat.
		ts, tlsErr := setupTLSTransport()
		if tlsErr != nil {
			b.Fatalf("setupTLSTransport (memory) failed: %v", tlsErr)
		}

		start := time.Now()

		node := raft.StartNode(cfg, peers)

		// Start event loop without WAL persistence (memory mode).
		stopCh := make(chan struct{})
		var loopDone sync.WaitGroup
		loopDone.Add(1)
		go func() {
			defer loopDone.Done()
			for {
				select {
				case rd := <-node.Ready():
					// No WAL persistence in memory mode.
					if !raft.IsEmptyHardState(rd.HardState) {
						storage.SetHardState(rd.HardState)
					}
					if len(rd.Entries) > 0 {
						storage.Append(rd.Entries)
					}
					for _, entry := range rd.CommittedEntries {
						if entry.GetType() == raftpb.EntryConfChange {
							var cc raftpb.ConfChange
							if unmarshalErr := proto.Unmarshal(entry.Data, &cc); unmarshalErr == nil {
								node.ApplyConfChange(&cc)
							}
						}
					}
					node.Advance()
				case <-stopCh:
					return
				}
			}
		}()

		// Start a ticker goroutine that drives raft ticks at 1ms
		// intervals, matching QuicRaft's RTTMillisecond. This allows
		// etcd/raft to trigger its natural randomized election timeout.
		tickDone := make(chan struct{})
		go runTicker(node, stopCh, tickDone)

		// Wait for leader via natural election timeout (no Campaign).
		memElectionTicker := time.NewTicker(1 * time.Millisecond)
		memElectionDeadline := time.NewTimer(10 * time.Second)
		elected := false
	memElectionPoll:
		for {
			status := node.Status()
			if status.Lead == 1 {
				elected = true
				memElectionTicker.Stop()
				memElectionDeadline.Stop()
				break memElectionPoll
			}
			select {
			case <-memElectionDeadline.C:
				memElectionTicker.Stop()
				break memElectionPoll
			case <-memElectionTicker.C:
			}
		}

		elapsed := time.Since(start)
		rec.Record(elapsed)

		// Stop the node first (drains Ready channel), then wait for
		// all goroutines to exit.
		node.Stop()
		close(stopCh)
		loopDone.Wait()
		<-tickDone
		ts.close()

		if !elected {
			b.Fatalf("etcd/raft (memory) election did not complete within timeout (iteration %d)", i)
		}
	}
}
