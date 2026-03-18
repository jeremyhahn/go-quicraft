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
	"fmt"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jeremyhahn/go-quicraft/test/performance/perfresult"
	"go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"
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

// persistEntries serializes raft entries to disk with a length-prefix
// framing protocol and calls fsync to ensure durability. This matches
// the I/O pattern of QuicRaft's WAL: encode entries, write, fsync.
func (w *walWriter) persistEntries(entries []raftpb.Entry) error {
	for i := range entries {
		data, err := entries[i].Marshal()
		if err != nil {
			return fmt.Errorf("walWriter: marshal entry index=%d: %w", entries[i].Index, err)
		}
		binary.LittleEndian.PutUint32(w.lengthBuf[:], uint32(len(data)))
		if _, err := w.f.Write(w.lengthBuf[:]); err != nil {
			return fmt.Errorf("walWriter: write length: %w", err)
		}
		if _, err := w.f.Write(data); err != nil {
			return fmt.Errorf("walWriter: write entry: %w", err)
		}
	}
	if err := w.f.Sync(); err != nil {
		return fmt.Errorf("walWriter: fsync: %w", err)
	}
	return nil
}

// persistHardState serializes the hard state to disk with fsync.
func (w *walWriter) persistHardState(hs raftpb.HardState) error {
	data, err := hs.Marshal()
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
	if err := w.f.Sync(); err != nil {
		return fmt.Errorf("walWriter: fsync hs: %w", err)
	}
	return nil
}

// persistReady writes both entries and hard state to the WAL in a single
// write+fsync cycle. This eliminates the dual-fsync overhead that occurs
// when persistEntries and persistHardState are called separately within
// the same Ready cycle. Entries are written first, then the hard state,
// followed by a single fsync to ensure durability of both.
func (w *walWriter) persistReady(entries []raftpb.Entry, hs raftpb.HardState) error {
	// Write entries without fsyncing.
	for i := range entries {
		data, err := entries[i].Marshal()
		if err != nil {
			return fmt.Errorf("walWriter: marshal entry index=%d: %w", entries[i].Index, err)
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
		data, err := hs.Marshal()
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

// benchStateMachineEtcd is a minimal state machine that tracks the last
// applied index and processes committed entries to ensure equivalent CPU
// work on the benchmark hot path.
type benchStateMachineEtcd struct {
	mu      sync.Mutex
	state   map[uint64]uint64
	applied uint64
}

// newBenchStateMachineEtcd creates a state machine with a pre-allocated
// map for storing applied entry results.
func newBenchStateMachineEtcd() *benchStateMachineEtcd {
	return &benchStateMachineEtcd{
		state: make(map[uint64]uint64, 4096),
	}
}

// apply processes committed entries through the state machine, storing
// the payload length keyed by entry index.
func (sm *benchStateMachineEtcd) apply(entries []raftpb.Entry) {
	sm.mu.Lock()
	for _, entry := range entries {
		if entry.Type == raftpb.EntryNormal && len(entry.Data) > 0 {
			sm.state[entry.Index] = uint64(len(entry.Data))
			sm.applied = entry.Index
		}
	}
	sm.mu.Unlock()
}

// etcdRaftTestNode bundles an etcd/raft Node, its MemoryStorage, WAL
// writer, state machine, and a stop channel for the event loop goroutine.
type etcdRaftTestNode struct {
	node        raft.Node
	storage     *raft.MemoryStorage
	wal         *walWriter
	sm          *benchStateMachineEtcd
	confTracker *confChangeTracker
	stopCh      chan struct{}
	loopDone    sync.WaitGroup
	stopped     atomic.Bool

	// totalApplied tracks how many normal entries have been fully
	// persisted to WAL, committed, and applied through the state machine.
	// Waiters poll this atomic counter directly rather than using channel
	// signals, which eliminates dropped signals under sustained load.
	totalApplied atomic.Int64
}

// setupEtcdRaftNode creates a single-node etcd/raft cluster with both
// in-memory storage (for raft internals) and a file-based WAL (for I/O
// parity with QuicRaft). It starts the event-loop goroutine that
// persists entries to the WAL file, applies committed entries through
// a state machine, and advances the raft node. The caller must call
// stop() or rely on b.Cleanup to shut down.
func setupEtcdRaftNode(b *testing.B) *etcdRaftTestNode {
	b.Helper()

	storage := raft.NewMemoryStorage()

	cfg := &raft.Config{
		ID:              1,
		ElectionTick:    10,
		HeartbeatTick:   1,
		Storage:         storage,
		MaxSizePerMsg:   1024 * 1024,
		MaxInflightMsgs: 256,
		CheckQuorum:     true,
		PreVote:         true,
	}

	peers := []raft.Peer{{ID: 1}}
	node := raft.StartNode(cfg, peers)

	walDir := b.TempDir()
	wal, err := newWALWriter(walDir)
	if err != nil {
		node.Stop()
		b.Fatalf("newWALWriter failed: %v", err)
	}

	tn := &etcdRaftTestNode{
		node:        node,
		storage:     storage,
		wal:         wal,
		sm:          newBenchStateMachineEtcd(),
		confTracker: newConfChangeTracker(),
		stopCh:      make(chan struct{}),
	}

	// Start the event loop that persists, commits, and applies.
	tn.loopDone.Add(1)
	go func() {
		defer tn.loopDone.Done()
		tn.eventLoop()
	}()

	// Wait for the initial ConfChange to be applied before calling
	// Campaign. The ConfChange is generated by StartNode for the
	// bootstrap peer. Without waiting for it, Campaign will fail with
	// "cannot campaign: pending configuration changes".
	if !tn.waitForConfChange(5 * time.Second) {
		tn.stop()
		b.Fatalf("etcd/raft: initial ConfChange not processed within timeout")
	}

	// Trigger election. In a single-node cluster the node immediately
	// becomes leader once the bootstrap config change is applied.
	node.Campaign(context.Background())

	// Wait for the node to become leader.
	leaderTicker := time.NewTicker(1 * time.Millisecond)
	leaderDeadline := time.NewTimer(10 * time.Second)
	for {
		status := node.Status()
		if status.Lead == 1 {
			leaderTicker.Stop()
			leaderDeadline.Stop()
			break
		}
		select {
		case <-leaderDeadline.C:
			leaderTicker.Stop()
			leaderDeadline.Stop()
			goto checkLeaderStatus
		case <-leaderTicker.C:
		}
	}
checkLeaderStatus:

	status := node.Status()
	if status.Lead != 1 {
		tn.stop()
		b.Fatalf("etcd/raft single-node failed to elect leader: lead=%d", status.Lead)
	}

	b.Cleanup(func() {
		tn.stop()
	})

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
//  4. Increments totalApplied atomic counter for waiters
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
				applied := int64(0)
				for _, entry := range rd.CommittedEntries {
					switch entry.Type {
					case raftpb.EntryConfChange:
						var cc raftpb.ConfChange
						if err := cc.Unmarshal(entry.Data); err == nil {
							tn.node.ApplyConfChange(cc)
						}
						tn.confTracker.signal()
					case raftpb.EntryNormal:
						if len(entry.Data) > 0 {
							applied++
						}
					}
				}

				// Batch-apply all normal entries through state machine.
				tn.sm.apply(rd.CommittedEntries)

				// Step 4: Atomic counter increment (no channel signals).
				if applied > 0 {
					tn.totalApplied.Add(applied)
				}
			}

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
	if tn.wal != nil {
		tn.wal.close()
	}
}

// waitForApplied polls the atomic totalApplied counter until it reaches
// or exceeds the target count, or the timeout expires. Returns true if
// the target was reached. Uses tight polling to avoid channel signal
// drops under sustained load.
func (tn *etcdRaftTestNode) waitForApplied(target int64, timeout time.Duration) bool {
	ticker := time.NewTicker(50 * time.Microsecond)
	defer ticker.Stop()
	deadline := time.NewTimer(timeout)
	defer deadline.Stop()
	for {
		if tn.totalApplied.Load() >= target {
			return true
		}
		select {
		case <-deadline.C:
			return false
		case <-ticker.C:
		}
	}
}

// benchEtcdRaftPropose runs sequential proposals through etcd/raft,
// recording per-operation latency. Each proposal is timed from the
// Propose call through WAL persistence, commit, and state machine
// application.
func benchEtcdRaftPropose(b *testing.B, tn *etcdRaftTestNode, payloadSize int, rec *perfresult.LatencyRecorder) {
	b.Helper()

	payload := make([]byte, payloadSize)
	for i := range payload {
		payload[i] = byte(i % 256)
	}

	ctx := context.Background()

	// Reset applied counter for measurement.
	tn.totalApplied.Store(0)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		start := time.Now()
		targetApplied := int64(i + 1)

		if err := tn.node.Propose(ctx, payload); err != nil {
			b.Fatalf("etcd Propose failed at iteration %d: %v", i, err)
		}

		// Wait for this entry to be fully applied (WAL + commit + SM).
		if !tn.waitForApplied(targetApplied, 10*time.Second) {
			b.Fatalf("etcd apply did not complete within timeout at iteration %d (applied=%d, target=%d)",
				i, tn.totalApplied.Load(), targetApplied)
		}

		elapsed := time.Since(start)
		rec.Record(elapsed)
	}
	b.StopTimer()
}

// benchEtcdRaftConcurrentPropose runs concurrent proposals from multiple
// goroutines, recording aggregate latency. Each goroutine waits for its
// proposal to be fully applied (WAL persist + commit + state machine)
// before recording the latency sample.
func benchEtcdRaftConcurrentPropose(b *testing.B, tn *etcdRaftTestNode, payloadSize int, numGoroutines int, rec *perfresult.LatencyRecorder) {
	b.Helper()

	payload := make([]byte, payloadSize)
	for i := range payload {
		payload[i] = byte(i % 256)
	}

	ctx := context.Background()

	// Reset applied counter.
	tn.totalApplied.Store(0)

	b.ResetTimer()

	var wg sync.WaitGroup
	opsPerGoroutine := b.N / numGoroutines
	remainder := b.N % numGoroutines
	var errCount atomic.Int64

	// proposalCounter assigns a monotonically increasing target to each
	// proposal so each goroutine can wait for its specific entry to be
	// applied through the full pipeline.
	var proposalCounter atomic.Int64

	for g := 0; g < numGoroutines; g++ {
		ops := opsPerGoroutine
		if g < remainder {
			ops++
		}
		if ops == 0 {
			continue
		}

		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			for i := 0; i < n; i++ {
				start := time.Now()

				// Reserve a target applied index before proposing.
				target := proposalCounter.Add(1)

				if err := tn.node.Propose(ctx, payload); err != nil {
					errCount.Add(1)
					continue
				}

				// Wait for this specific proposal to be applied through
				// the full pipeline (WAL -> commit -> state machine).
				if !tn.waitForApplied(target, 10*time.Second) {
					errCount.Add(1)
					continue
				}

				elapsed := time.Since(start)
				rec.Record(elapsed)
			}
		}(ops)
	}
	wg.Wait()
	b.StopTimer()

	if count := errCount.Load(); count > 0 {
		b.Logf("warning: %d/%d proposals failed during concurrent benchmark", count, b.N)
	}
}

// benchEtcdRaftElection measures the time from node creation to leader
// election via natural randomized election timeout (no Campaign shortcut).
// Each iteration creates a WAL file in a temp directory to match the I/O
// overhead that QuicRaft incurs during startup and election (WAL directory
// creation, initial segment allocation). A ticker goroutine drives
// node.Tick() at 1ms intervals matching QuicRaft's RTTMillisecond.
func benchEtcdRaftElection(b *testing.B, rec *perfresult.LatencyRecorder) {
	b.Helper()

	for i := 0; i < b.N; i++ {
		storage := raft.NewMemoryStorage()

		cfg := &raft.Config{
			ID:              1,
			ElectionTick:    10,
			HeartbeatTick:   1,
			Storage:         storage,
			MaxSizePerMsg:   1024 * 1024,
			MaxInflightMsgs: 256,
			CheckQuorum:     true,
			PreVote:         true,
		}

		peers := []raft.Peer{{ID: 1}}

		walDir := b.TempDir()

		start := time.Now()

		// Initialize TLS transport to match QuicRaft's QUIC/mTLS
		// initialization overhead (cert generation + listener binding).
		ts, tlsErr := setupTLSTransport()
		if tlsErr != nil {
			b.Fatalf("setupTLSTransport failed: %v", tlsErr)
		}

		node := raft.StartNode(cfg, peers)

		wal, err := newWALWriter(walDir)
		if err != nil {
			ts.close()
			node.Stop()
			b.Fatalf("newWALWriter failed in election bench: %v", err)
		}

		// Start event loop that processes ConfChange entries and persists
		// to WAL.
		confTracker := newConfChangeTracker()
		stopCh := make(chan struct{})
		var loopDone sync.WaitGroup
		loopDone.Add(1)
		go func() {
			defer loopDone.Done()
			for {
				select {
				case rd := <-node.Ready():
					// Persist entries to WAL with fsync.
					if len(rd.Entries) > 0 {
						if walErr := wal.persistEntries(rd.Entries); walErr != nil {
							return
						}
					}
					if !raft.IsEmptyHardState(rd.HardState) {
						if walErr := wal.persistHardState(rd.HardState); walErr != nil {
							return
						}
						storage.SetHardState(rd.HardState)
					}
					if len(rd.Entries) > 0 {
						storage.Append(rd.Entries)
					}
					for _, entry := range rd.CommittedEntries {
						if entry.Type == raftpb.EntryConfChange {
							var cc raftpb.ConfChange
							if unmarshalErr := cc.Unmarshal(entry.Data); unmarshalErr == nil {
								node.ApplyConfChange(cc)
							}
							confTracker.signal()
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
		go func() {
			defer close(tickDone)
			ticker := time.NewTicker(1 * time.Millisecond)
			defer ticker.Stop()
			for {
				select {
				case <-ticker.C:
					node.Tick()
				case <-stopCh:
					return
				}
			}
		}()

		// Wait for leader via natural election timeout (no Campaign).
		electionTicker := time.NewTicker(100 * time.Microsecond)
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
// (propose to fully applied through WAL + state machine). This is
// equivalent to QuicRaft's SyncPropose latency which includes WAL
// persistence, raft commit, and state machine application.
func benchEtcdRaftCommitLatency(b *testing.B, tn *etcdRaftTestNode, payloadSize int, rec *perfresult.LatencyRecorder) {
	b.Helper()

	payload := make([]byte, payloadSize)
	for i := range payload {
		payload[i] = byte(i % 256)
	}

	ctx := context.Background()

	// Warm up: prime the pipeline and WAL file.
	tn.totalApplied.Store(0)
	for w := 0; w < 10; w++ {
		if err := tn.node.Propose(ctx, payload); err != nil {
			b.Fatalf("warmup propose failed: %v", err)
		}
	}
	if !tn.waitForApplied(10, 10*time.Second) {
		b.Fatalf("warmup applies did not complete")
	}

	// Reset for measurement.
	tn.totalApplied.Store(0)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		start := time.Now()
		targetApplied := int64(i + 1)

		if err := tn.node.Propose(ctx, payload); err != nil {
			b.Fatalf("etcd Propose commit latency failed at iteration %d: %v", i, err)
		}

		if !tn.waitForApplied(targetApplied, 10*time.Second) {
			b.Fatalf("etcd commit latency: apply did not complete at iteration %d", i)
		}

		elapsed := time.Since(start)
		rec.Record(elapsed)
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

// benchEtcdRaftElectionCluster measures multi-node election time including
// TLS transport initialization, WAL setup, and natural leader election
// across all cluster nodes. Each iteration creates and tears down a
// complete cluster.
func benchEtcdRaftElectionCluster(b *testing.B, numNodes int, rec *perfresult.LatencyRecorder) {
	b.Helper()
	for i := 0; i < b.N; i++ {
		start := time.Now()
		cluster := SetupEtcdCluster(b, numNodes)
		elapsed := time.Since(start)
		rec.Record(elapsed)
		cluster.Close()
	}
}

// benchEtcdRaftProposeCluster measures sequential proposal throughput
// through the cluster leader with TCP+TLS transport and WAL persistence.
// Each proposal is timed from Propose through quorum replication,
// commit, and state machine application on the leader node. The leader
// is re-discovered dynamically on each proposal failure to handle
// leadership changes during the benchmark.
func benchEtcdRaftProposeCluster(b *testing.B, cluster *EtcdCluster, payloadSize int, rec *perfresult.LatencyRecorder) {
	b.Helper()

	payload := make([]byte, payloadSize)
	for i := range payload {
		payload[i] = byte(i % 256)
	}

	ctx := context.Background()
	leader, findErr := cluster.CurrentLeaderNode()
	if findErr != nil {
		b.Fatalf("no leader at benchmark start: %v", findErr)
	}

	// Reset applied counter for measurement.
	leader.totalApplied.Store(0)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		start := time.Now()
		targetApplied := int64(i + 1)

		if err := leader.node.Propose(ctx, payload); err != nil {
			// Re-discover leader on proposal failure.
			newLeader, reErr := cluster.waitForLeader(5 * time.Second)
			if reErr != nil {
				b.Fatalf("cluster propose failed at iteration %d and no leader available: %v", i, err)
			}
			// Transfer applied count to new leader so target tracking
			// stays consistent.
			newLeader.totalApplied.Store(leader.totalApplied.Load())
			leader = newLeader
			// Retry on new leader.
			if retryErr := leader.node.Propose(ctx, payload); retryErr != nil {
				b.Fatalf("cluster propose retry failed at iteration %d: %v", i, retryErr)
			}
		}

		if !leader.waitForApplied(targetApplied, 10*time.Second) {
			b.Fatalf("cluster apply did not complete at iteration %d (applied=%d, target=%d)",
				i, leader.totalApplied.Load(), targetApplied)
		}

		rec.Record(time.Since(start))
	}
	b.StopTimer()
}

// benchEtcdRaftConcurrentProposeCluster runs concurrent proposals from
// multiple goroutines through the cluster leader. Each goroutine waits
// for its proposal to be fully committed and applied before recording
// the latency sample. The leader is re-discovered dynamically when a
// proposal fails due to leadership change.
func benchEtcdRaftConcurrentProposeCluster(b *testing.B, cluster *EtcdCluster, payloadSize int, numGoroutines int, rec *perfresult.LatencyRecorder) {
	b.Helper()

	payload := make([]byte, payloadSize)
	for i := range payload {
		payload[i] = byte(i % 256)
	}

	ctx := context.Background()

	// leaderRef is an atomically-swapped pointer to the current leader.
	// All goroutines read from this and update it on leadership change.
	var leaderRef atomic.Pointer[etcdClusterNode]
	initialLeader, findErr := cluster.CurrentLeaderNode()
	if findErr != nil {
		b.Fatalf("no leader at benchmark start: %v", findErr)
	}
	leaderRef.Store(initialLeader)

	// Reset applied counter.
	initialLeader.totalApplied.Store(0)

	b.ResetTimer()

	var wg sync.WaitGroup
	opsPerGoroutine := b.N / numGoroutines
	remainder := b.N % numGoroutines
	var errCount atomic.Int64
	var proposalCounter atomic.Int64

	for g := 0; g < numGoroutines; g++ {
		ops := opsPerGoroutine
		if g < remainder {
			ops++
		}
		if ops == 0 {
			continue
		}

		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			for i := 0; i < n; i++ {
				start := time.Now()

				target := proposalCounter.Add(1)
				currentLeader := leaderRef.Load()

				if err := currentLeader.node.Propose(ctx, payload); err != nil {
					// Re-discover leader on proposal failure.
					newLeader, reErr := cluster.waitForLeader(5 * time.Second)
					if reErr != nil {
						errCount.Add(1)
						continue
					}
					leaderRef.CompareAndSwap(currentLeader, newLeader)
					currentLeader = leaderRef.Load()
					// Retry on new leader.
					if retryErr := currentLeader.node.Propose(ctx, payload); retryErr != nil {
						errCount.Add(1)
						continue
					}
				}

				if !currentLeader.waitForApplied(target, 10*time.Second) {
					errCount.Add(1)
					continue
				}

				rec.Record(time.Since(start))
			}
		}(ops)
	}
	wg.Wait()
	b.StopTimer()

	if count := errCount.Load(); count > 0 {
		b.Logf("warning: %d/%d proposals failed during cluster concurrent benchmark", count, b.N)
	}
}

// benchEtcdRaftCommitLatencyCluster measures per-proposal commit latency
// through the cluster leader. A warmup phase primes the TLS connections
// and WAL before measurement begins. The leader is re-discovered
// dynamically on proposal failure.
func benchEtcdRaftCommitLatencyCluster(b *testing.B, cluster *EtcdCluster, payloadSize int, rec *perfresult.LatencyRecorder) {
	b.Helper()

	payload := make([]byte, payloadSize)
	for i := range payload {
		payload[i] = byte(i % 256)
	}

	ctx := context.Background()
	leader, findErr := cluster.CurrentLeaderNode()
	if findErr != nil {
		b.Fatalf("no leader at benchmark start: %v", findErr)
	}

	// Warm up: prime the pipeline and TLS connections.
	leader.totalApplied.Store(0)
	for w := 0; w < 10; w++ {
		if err := leader.node.Propose(ctx, payload); err != nil {
			b.Fatalf("cluster warmup propose failed: %v", err)
		}
	}
	if !leader.waitForApplied(10, 10*time.Second) {
		b.Fatalf("cluster warmup applies did not complete")
	}

	// Reset for measurement.
	leader.totalApplied.Store(0)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		start := time.Now()
		targetApplied := int64(i + 1)

		if err := leader.node.Propose(ctx, payload); err != nil {
			// Re-discover leader on proposal failure.
			newLeader, reErr := cluster.waitForLeader(5 * time.Second)
			if reErr != nil {
				b.Fatalf("cluster commit propose failed at iteration %d and no leader: %v", i, err)
			}
			newLeader.totalApplied.Store(leader.totalApplied.Load())
			leader = newLeader
			if retryErr := leader.node.Propose(ctx, payload); retryErr != nil {
				b.Fatalf("cluster commit propose retry failed at iteration %d: %v", i, retryErr)
			}
		}

		if !leader.waitForApplied(targetApplied, 10*time.Second) {
			b.Fatalf("cluster commit latency: apply did not complete at iteration %d", i)
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

// setupEtcdRaftNodeMemory creates a single-node etcd/raft cluster with
// in-memory storage only (no WAL file). This benchmarks pure algorithm
// performance without disk I/O overhead. The event loop skips WAL
// persistence steps when tn.wal is nil.
func setupEtcdRaftNodeMemory(b *testing.B) *etcdRaftTestNode {
	b.Helper()

	storage := raft.NewMemoryStorage()

	cfg := &raft.Config{
		ID:              1,
		ElectionTick:    10,
		HeartbeatTick:   1,
		Storage:         storage,
		MaxSizePerMsg:   1024 * 1024,
		MaxInflightMsgs: 256,
		CheckQuorum:     true,
		PreVote:         true,
	}

	peers := []raft.Peer{{ID: 1}}
	node := raft.StartNode(cfg, peers)

	tn := &etcdRaftTestNode{
		node:        node,
		storage:     storage,
		wal:         nil, // Memory mode: no WAL writer.
		sm:          newBenchStateMachineEtcd(),
		confTracker: newConfChangeTracker(),
		stopCh:      make(chan struct{}),
	}

	// Start the event loop that commits and applies (no WAL persistence).
	tn.loopDone.Add(1)
	go func() {
		defer tn.loopDone.Done()
		tn.eventLoop()
	}()

	// Wait for the initial ConfChange to be applied before calling
	// Campaign. The ConfChange is generated by StartNode for the
	// bootstrap peer.
	if !tn.waitForConfChange(5 * time.Second) {
		tn.stop()
		b.Fatalf("etcd/raft (memory): initial ConfChange not processed within timeout")
	}

	// Trigger election. In a single-node cluster the node immediately
	// becomes leader once the bootstrap config change is applied.
	node.Campaign(context.Background())

	// Wait for the node to become leader.
	memLeaderTicker := time.NewTicker(1 * time.Millisecond)
	memLeaderDeadline := time.NewTimer(10 * time.Second)
	for {
		status := node.Status()
		if status.Lead == 1 {
			memLeaderTicker.Stop()
			memLeaderDeadline.Stop()
			break
		}
		select {
		case <-memLeaderDeadline.C:
			memLeaderTicker.Stop()
			memLeaderDeadline.Stop()
			goto checkMemLeaderStatus
		case <-memLeaderTicker.C:
		}
	}
checkMemLeaderStatus:

	status := node.Status()
	if status.Lead != 1 {
		tn.stop()
		b.Fatalf("etcd/raft (memory) single-node failed to elect leader: lead=%d", status.Lead)
	}

	b.Cleanup(func() {
		tn.stop()
	})

	return tn
}

// benchEtcdRaftElectionMemory measures the time from node creation to
// leader election via natural randomized election timeout (no Campaign
// shortcut) without any WAL I/O. A ticker goroutine drives node.Tick()
// at 1ms intervals matching QuicRaft's RTTMillisecond.
func benchEtcdRaftElectionMemory(b *testing.B, rec *perfresult.LatencyRecorder) {
	b.Helper()

	for i := 0; i < b.N; i++ {
		storage := raft.NewMemoryStorage()

		cfg := &raft.Config{
			ID:              1,
			ElectionTick:    10,
			HeartbeatTick:   1,
			Storage:         storage,
			MaxSizePerMsg:   1024 * 1024,
			MaxInflightMsgs: 256,
			CheckQuorum:     true,
			PreVote:         true,
		}

		peers := []raft.Peer{{ID: 1}}

		start := time.Now()

		// Initialize TLS transport to match QuicRaft's QUIC/mTLS
		// initialization overhead (cert generation + listener binding).
		ts, tlsErr := setupTLSTransport()
		if tlsErr != nil {
			b.Fatalf("setupTLSTransport (memory) failed: %v", tlsErr)
		}

		node := raft.StartNode(cfg, peers)

		// Start event loop without WAL persistence (memory mode).
		confTracker := newConfChangeTracker()
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
						if entry.Type == raftpb.EntryConfChange {
							var cc raftpb.ConfChange
							if unmarshalErr := cc.Unmarshal(entry.Data); unmarshalErr == nil {
								node.ApplyConfChange(cc)
							}
							confTracker.signal()
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
		go func() {
			defer close(tickDone)
			ticker := time.NewTicker(1 * time.Millisecond)
			defer ticker.Stop()
			for {
				select {
				case <-ticker.C:
					node.Tick()
				case <-stopCh:
					return
				}
			}
		}()

		// Wait for leader via natural election timeout (no Campaign).
		memElectionTicker := time.NewTicker(100 * time.Microsecond)
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
