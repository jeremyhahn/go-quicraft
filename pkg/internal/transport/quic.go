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

package transport

import (
	"context"
	"crypto/rand"
	"crypto/x509"
	"encoding/binary"
	"encoding/pem"
	"fmt"
	"log/slog"
	"net"
	"runtime/debug"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/golang/snappy"
	"github.com/jeremyhahn/go-quicraft/pkg/config"
	"github.com/jeremyhahn/go-quicraft/pkg/internal/registry"
	"github.com/jeremyhahn/go-quicraft/pkg/internal/transport/revocation"
	"github.com/jeremyhahn/go-quicraft/pkg/proto"
	"github.com/quic-go/quic-go"
	"golang.org/x/sync/singleflight"
)

// streamShardCount is the number of stream pool shards. Must be a power
// of two for fast modulo via bitmask.
const streamShardCount = 16

// sendWriteDeadline is the maximum time for a single stream.Write on
// the send path. Kept short (2s) to quickly detect dead connections
// from peer restarts, avoiding prolonged stalls that block replication.
const sendWriteDeadline = 2 * time.Second

// sendQueueIdleTimeout is how long a per-target send queue worker waits
// with no messages before cleaning itself up. Set to 2x the QUIC
// MaxIdleTimeout (30s) so that by the time the worker exits, the QUIC
// connection has already been idle-closed. The next Send to this target
// will lazily re-create the queue and goroutine.
const sendQueueIdleTimeout = 60 * time.Second

// snapshotChunkWriteDeadline is the maximum time for a single chunk write
// on the snapshot send path. Snapshot chunks can be up to 64MB each, so
// this deadline is much larger than sendWriteDeadline. If the receiver is
// slow or the network stalls, the deadline prevents indefinite blocking
// that would hold a snapshotTracker slot and make Stop() hang forever.
const snapshotChunkWriteDeadline = 30 * time.Second

// defaultSendQueueLen is the per-target send queue capacity. Each target
// (remote host address) gets its own buffered channel and sender goroutine.
// Messages are enqueued non-blocking (dropped if full -- Raft retransmits).
// 2048 matches dragonboat's SendQueueLength default, sized to absorb burst
// traffic from multiple shards targeting the same host without dropping
// Replicate/ReplicateResp messages that are critical for commit progress.
const defaultSendQueueLen = 2048

// workerPanicRestartDelay is the delay before restarting a panicked
// transport background worker. This prevents tight panic-restart loops
// from consuming CPU and flooding logs when a worker hits a persistent
// panic trigger.
const workerPanicRestartDelay = 100 * time.Millisecond

// streamShard is a bucket in the sharded stream pool. Each shard has its
// own mutex to reduce contention across concurrent senders.
type streamShard struct {
	mu      sync.RWMutex
	streams map[streamKey]*quic.Stream
}

// streamKey identifies a pooled stream by its target address and pool index.
type streamKey struct {
	target string
	idx    uint64
}

// sendQueue is a per-target async send queue. Each remote host address gets
// its own channel and sender goroutine. Messages are enqueued non-blocking
// by Send() and delivered by the background worker. This decouples the
// step worker from transport latency (QUIC dial, stream open, write with
// sendWriteDeadline), preventing heartbeat starvation and multi-node stalls.
//
// Heartbeat messages are routed to a separate high-priority channel (hbCh)
// so they are never blocked behind a backlog of data messages. The worker
// always drains hbCh before processing the regular data channel, ensuring
// leader liveness even under heavy replication load.
//
// The pattern matches dragonboat's per-target send queue architecture.
//
// closed is set true by the worker (under sendQueuesMu) immediately before
// it removes itself from the sendQueues map and exits on idle timeout. The
// enqueue path in Send checks this flag after a successful channel send: a
// producer may have obtained this *sendQueue from getOrCreateSendQueue and
// then enqueued into a channel that no longer has a drainer (the worker
// already pruned the map entry and returned). On observing closed, the
// producer re-acquires a fresh queue via getOrCreateSendQueue and re-enqueues
// once, so the message is delivered by the newly started worker instead of
// being orphaned.
type sendQueue struct {
	ch     chan proto.Message
	hbCh   chan proto.Message // high-priority heartbeat channel
	closed atomic.Bool        // set by worker before idle-prune delete+return
}

// QUICTransport provides QUIC-based message delivery for the Raft engine.
// It manages connection pooling, stream multiplexing, and frame-level I/O
// for both regular Raft messages and snapshot transfers.
//
// Send() is non-blocking: messages are enqueued into per-target send queues
// (one per remote host address). Each queue has a dedicated sender goroutine
// that batches messages and performs the actual QUIC I/O. This ensures the
// step worker never blocks on transport operations.
//
// QUICTransport is safe for concurrent use by multiple goroutines.
type QUICTransport struct {
	cfg      Config
	handler  MessageHandler
	reg      *registry.Registry
	tlsPair  *tlsPair
	quicCfg  *quic.Config
	udpConn  *net.UDPConn
	quicTr   *quic.Transport
	listener *quic.Listener

	connMu    sync.RWMutex
	conns     map[string]*quic.Conn
	dialGroup singleflight.Group

	// bootEpoch is this process's per-start identity, stamped into every
	// outgoing MessageBatch (SourceEpoch). It changes only when this node
	// restarts.
	bootEpoch uint64

	// peerEpoch tracks the last-seen bootEpoch per peer address. A change in
	// a peer's epoch means that peer's process restarted: its view of our
	// previously-dialed outbound connection is gone (that outbound is now
	// black-holed), so we evict it immediately rather than waiting for
	// MaxIdleTimeout. Keying on the epoch (not the inbound *quic.Conn
	// identity) avoids the eviction cascade that benign re-dials caused,
	// because a re-dial from the same process carries the SAME epoch.
	peerEpochMu sync.Mutex
	peerEpoch   map[string]uint64

	streamShards [streamShardCount]streamShard

	// sendQueuesMu protects the sendQueues map. Reads (fast path in Send)
	// use RLock; writes (queue creation for new targets) use Lock.
	sendQueuesMu sync.RWMutex
	sendQueues   map[string]*sendQueue

	snapshotRecv *snapshotReceiver

	stopC           chan struct{}
	stopped         atomic.Bool
	shutdownCtx     context.Context
	shutdownCancel  context.CancelFunc
	wg              sync.WaitGroup
	snapshotTracker shutdownTracker // tracks in-flight snapshot sends and receives

	// bufPool recycles byte buffers for frame marshaling on the send path.
	bufPool sync.Pool

	// snappyEncPool recycles Snappy encode destination buffers to avoid
	// per-send allocations on the compression path. Buffers are returned
	// after the compressed data is copied into the frame buffer.
	snappyEncPool sync.Pool

	// localAddrStr caches the string form of the local UDP listen address.
	// Computed once in Start() to avoid repeated net.UDPAddr.String() calls
	// on every sendBatch invocation.
	localAddrStr string

	// hbPrefix is the pre-serialized MessageBatch header for heartbeat
	// frames: [BinVer:8][DeploymentID:8][SourceAddrLen:4][SourceAddr:N].
	// Computed once in Start() after the local address is known. Used by
	// sendHeartbeatBatch to avoid repeated serialization of invariant
	// fields on the ReadIndex critical path.
	hbPrefix []byte

	// eventListener holds optional event callbacks for connection and snapshot
	// lifecycle events. May be nil.
	eventListener *config.EventListener

	// metrics collects transport-level metrics. Nil when disabled.
	metrics Metrics

	// revocationChecker performs CRL/OCSP certificate revocation checking.
	// Nil when revocation checking is disabled. Created in NewQUICTransport
	// when Config.RevocationConfig is non-nil.
	revocationChecker *revocation.Checker

	// srcAddrAuth caches, per inbound *quic.Conn, the result of validating a
	// batch's claimed SourceAddress against that connection's verified peer
	// certificate. inboundAuthorizedFor calls ConnectionState()+VerifyHostname,
	// which is too costly to run on every batch (heartbeats are frequent); a
	// peer's certificate and advertised address are stable for a connection's
	// lifetime, so the decision is computed once per (conn, claimed-address)
	// and reused. Entries are removed when the inbound connection's handler
	// returns (handleConnection). Keyed by *quic.Conn, value *srcAddrAuthEntry.
	srcAddrAuth sync.Map
}

// srcAddrAuthEntry caches the SourceAddress authorization decision for a single
// inbound connection. addr is the last claimed SourceAddress that was verified
// against the connection's peer certificate; authorized records whether that
// address is covered by the certificate. A connection advertises a stable
// address, so a single-entry cache suffices for the common case while still
// re-verifying if the claimed address ever changes.
type srcAddrAuthEntry struct {
	mu         sync.Mutex
	addr       string
	authorized bool
	valid      bool
}

// newBootEpoch returns a per-process boot epoch used to detect peer restarts
// (evictStaleOutboundOnRecv compares the SourceEpoch carried on inbound
// batches and evicts the cached outbound connection when it changes). It mixes
// the wall clock with a random salt so two processes that start within the
// same nanosecond — or after a backward clock step — still receive distinct
// epochs. Restart detection keys on any *change* in this value, not ordering,
// so the salt cannot weaken correctness; it only closes the same-nanosecond
// collision window that would otherwise fall back to the 30s QUIC idle timeout.
func newBootEpoch() uint64 {
	var b [8]byte
	if _, err := rand.Read(b[:]); err == nil {
		return uint64(time.Now().UnixNano()) ^ binary.LittleEndian.Uint64(b[:])
	}
	return uint64(time.Now().UnixNano())
}

// NewQUICTransport creates a new QUIC transport with the given configuration.
// It applies defaults, validates the config, and builds the TLS configuration.
// The transport is not started until Start is called.
func NewQUICTransport(
	cfg Config,
	handler MessageHandler,
	reg *registry.Registry,
) (*QUICTransport, error) {
	cfg.SetDefaults()
	if err := cfg.Validate(); err != nil {
		return nil, err
	}

	if cfg.MTLSConfig == nil {
		return nil, ErrMTLSConfigRequired
	}

	// Create the revocation checker when configured. The checker is
	// passed to buildMTLSTLS to wire VerifyPeerCertificate on both
	// server and client TLS configs. The issuer certificate is extracted
	// from the CA PEM for OCSP request construction. Parse errors are
	// fatal: a nil issuer would silently disable OCSP checks at runtime.
	var checker *revocation.Checker
	if cfg.RevocationConfig != nil {
		block, _ := pem.Decode(cfg.MTLSConfig.CACert)
		if block == nil {
			return nil, &CACertificateParseError{
				Reason: "PEM decode returned nil block",
			}
		}
		issuerCert, parseErr := x509.ParseCertificate(block.Bytes)
		if parseErr != nil {
			return nil, &CACertificateParseError{
				Reason: "x509.ParseCertificate failed",
				Err:    parseErr,
			}
		}
		checker = revocation.NewChecker(*cfg.RevocationConfig, issuerCert)
	}

	pair, err := buildMTLSTLS(cfg.MTLSConfig, checker)
	if err != nil {
		return nil, err
	}

	// Align the QUIC-level incoming-stream limit with the number of streams
	// we will actually service concurrently. handleConnection sizes its
	// data-stream semaphore to MaxStreamsPerConnection and a SEPARATE snapshot
	// semaphore to MaxConcurrentSnapshotRecv; snapshot streams are accounted
	// against the latter for the duration of their (slow) transfer so they
	// never displace data streams. The incoming-stream budget therefore
	// reserves MaxConcurrentSnapshotRecv slots on top of the data-stream pool,
	// so QUIC flow-control backpressure kicks in exactly when our combined
	// processing capacity (data + snapshot) is reached. Advertising a much
	// larger limit (the old hardcoded 4096) let a peer open streams that then
	// sat accepted-but-unserviced.
	maxIncomingStreams := cfg.MaxStreamsPerConnection
	if maxIncomingStreams < 1 {
		maxIncomingStreams = defaultMaxStreamsPerConnection
	}
	snapStreams := cfg.MaxConcurrentSnapshotRecv
	if snapStreams < 1 {
		snapStreams = defaultMaxConcurrentSnapRecv
	}
	maxIncomingStreams += snapStreams
	// Guarantee the incoming-stream budget always leaves room for a peer's
	// full outbound ordered-stream pool (StreamPoolSize) plus its dedicated
	// snapshot/ephemeral streams. Without this floor an operator who raises
	// StreamPoolSize toward MaxStreamsPerConnection could starve snapshot
	// delivery: the peer's snapshot OpenStreamSync would block on QUIC
	// flow control until its dial deadline expired.
	if minStreams := cfg.StreamPoolSize + snapshotStreamHeadroom; maxIncomingStreams < minStreams {
		maxIncomingStreams = minStreams
	}

	qcfg := &quic.Config{
		MaxIdleTimeout:             30 * time.Second,
		HandshakeIdleTimeout:       5 * time.Second,
		MaxIncomingStreams:         int64(maxIncomingStreams),
		MaxIncomingUniStreams:      -1,
		KeepAlivePeriod:            10 * time.Second,
		InitialStreamReceiveWindow: 2 << 20,  // 2 MB
		MaxStreamReceiveWindow:     8 << 20,  // 8 MB
		MaxConnectionReceiveWindow: 32 << 20, // 32 MB
		// 0-RTT is replayable per RFC 9001 Section 8. Raft's internal
		// idempotency (duplicate message detection, term checks, log
		// index checks) causes replayed RPCs to be harmlessly discarded.
		// Session deduplication further protects client proposals.
		// Config.Validate() ensures MTLSConfig is set when Enable0RTT
		// is true (returns ErrInsecure0RTT otherwise).
		Allow0RTT: cfg.Enable0RTT,
	}

	shutdownCtx, shutdownCancel := context.WithCancel(context.Background())

	t := &QUICTransport{
		cfg:            cfg,
		handler:        handler,
		reg:            reg,
		tlsPair:        pair,
		quicCfg:        qcfg,
		conns:          make(map[string]*quic.Conn),
		bootEpoch:      newBootEpoch(),
		peerEpoch:      make(map[string]uint64),
		sendQueues:     make(map[string]*sendQueue),
		stopC:          make(chan struct{}),
		shutdownCtx:    shutdownCtx,
		shutdownCancel: shutdownCancel,
		bufPool: sync.Pool{
			New: func() any {
				buf := make([]byte, 0, cfg.SendBatchMaxSize+FrameHeaderSize)
				return &buf
			},
		},
	}

	for i := range t.streamShards {
		t.streamShards[i].streams = make(map[streamKey]*quic.Stream)
	}

	t.snapshotRecv = newSnapshotReceiver(
		cfg.MaxConcurrentSnapshotRecv,
		cfg.MaxSnapshotReceiveMemory,
		cfg.MaxSnapshotReceiveRate,
		cfg.DeploymentID,
		handler,
	)
	t.snapshotRecv.SetSnapshotTracker(&t.snapshotTracker)

	t.eventListener = cfg.EventListener
	t.metrics = cfg.Metrics
	t.snapshotRecv.listener = cfg.EventListener
	t.snapshotRecv.metrics = cfg.Metrics
	t.revocationChecker = checker

	return t, nil
}

// Start binds the UDP listener, creates the QUIC transport and listener,
// and starts the accept loop. It returns after the listener is ready.
func (t *QUICTransport) Start() error {
	recvBuf := t.cfg.UDPRecvBufSize
	sendBuf := t.cfg.UDPSendBufSize
	lc := net.ListenConfig{
		Control: func(_, _ string, c syscall.RawConn) error {
			return c.Control(func(fd uintptr) {
				if err := syscall.SetsockoptInt(int(fd), syscall.SOL_SOCKET, syscall.SO_REUSEADDR, 1); err != nil {
					slog.Debug("SO_REUSEADDR setsockopt failed", "fd", fd, "error", err)
				}
				if recvBuf > 0 {
					if err := syscall.SetsockoptInt(int(fd), syscall.SOL_SOCKET, syscall.SO_RCVBUF, recvBuf); err != nil {
						slog.Warn("SO_RCVBUF setsockopt failed; increase net.core.rmem_max",
							"requested", recvBuf, "error", err)
					}
				}
				if sendBuf > 0 {
					if err := syscall.SetsockoptInt(int(fd), syscall.SOL_SOCKET, syscall.SO_SNDBUF, sendBuf); err != nil {
						slog.Warn("SO_SNDBUF setsockopt failed; increase net.core.wmem_max",
							"requested", sendBuf, "error", err)
					}
				}
			})
		},
	}
	conn, err := lc.ListenPacket(context.Background(), "udp", t.cfg.ListenAddress)
	if err != nil {
		return err
	}
	udpConn, ok := conn.(*net.UDPConn)
	if !ok {
		if closeErr := conn.Close(); closeErr != nil {
			slog.Debug("connection close failed after type assertion error", "error", closeErr)
		}
		return &TypeAssertionError{
			Context:      "Start UDP listener",
			ExpectedType: "*net.UDPConn",
			ActualValue:  conn,
		}
	}
	t.udpConn = udpConn

	t.quicTr = &quic.Transport{
		Conn: udpConn,
	}

	serverTLS := t.tlsPair.server.Clone()
	listener, err := t.quicTr.Listen(serverTLS, t.quicCfg)
	if err != nil {
		if closeErr := udpConn.Close(); closeErr != nil {
			slog.Debug("UDP connection close failed after listener error", "error", closeErr)
		}
		return err
	}
	t.listener = listener

	// Cache the local address string to avoid repeated net.UDPAddr.String()
	// calls on every sendBatch invocation.
	t.localAddrStr = t.udpConn.LocalAddr().String()

	// Pre-serialize the heartbeat MessageBatch header now that the local
	// address is resolved. This enables the zero-alloc heartbeat fast path.
	t.initHeartbeatPrefix()

	// Start the revocation checker's CRL reload goroutine. This must
	// happen after the listener is created but before accepting
	// connections so that the initial CRL load is complete.
	if t.revocationChecker != nil {
		t.revocationChecker.Start()
	}

	t.wg.Add(1)
	go t.acceptLoop()

	t.wg.Add(1)
	go t.connectionCleanup()

	return nil
}

// SetEpochTracker configures the epoch tracker on the snapshot receiver.
// When set, the receiver will register in-flight epochs during snapshot
// transfers to prevent premature key purge. Must be called before Start.
func (t *QUICTransport) SetEpochTracker(tracker EpochTracker) {
	t.snapshotRecv.SetEpochTracker(tracker)
}

// Stop gracefully shuts down the transport. It signals all per-target
// sender goroutines to exit via stopC, waits for them to drain, stops
// the accept loop, waits for in-flight snapshot transfers, closes all
// connections, and waits for all goroutines to finish.
func (t *QUICTransport) Stop() error {
	if !t.stopped.CompareAndSwap(false, true) {
		return nil
	}
	close(t.stopC)
	t.shutdownCancel()

	// Stop the revocation checker's CRL reload goroutine before
	// tearing down connections.
	if t.revocationChecker != nil {
		t.revocationChecker.Stop()
	}

	// Acquire and release sendQueuesMu to synchronize with any
	// in-progress getOrCreateSendQueue call. This creates a happens-
	// before edge: any wg.Add(1) that was in progress (inside the
	// write lock with stopped=false) must complete before we proceed.
	// After this point, all future getOrCreateSendQueue calls see
	// stopped=true and return nil without calling wg.Add(1).
	t.sendQueuesMu.Lock()
	t.sendQueuesMu.Unlock() //nolint:staticcheck // intentional lock barrier for happens-before edge

	// Set the snapshot shutdown flag and wait for all in-flight
	// snapshot sends and receives to complete before tearing down
	// connections. After this, TryAdd returns false so no new
	// snapshot operations can begin.
	t.snapshotTracker.ShutdownAndWait()

	if t.listener != nil {
		if err := t.listener.Close(); err != nil {
			slog.Debug("QUIC listener close failed", "error", err)
		}
	}

	t.connMu.Lock()
	for addr, conn := range t.conns {
		if err := conn.CloseWithError(QUICErrShutdown, "transport shutdown"); err != nil {
			slog.Debug("QUIC connection close failed", "addr", addr, "error", err)
		}
		delete(t.conns, addr)
	}
	t.connMu.Unlock()

	t.peerEpochMu.Lock()
	clear(t.peerEpoch)
	t.peerEpochMu.Unlock()

	// Close all pooled streams.
	for i := range t.streamShards {
		shard := &t.streamShards[i]
		shard.mu.Lock()
		for k, s := range shard.streams {
			if err := s.Close(); err != nil {
				slog.Debug("QUIC stream close failed", "target", k.target, "idx", k.idx, "error", err)
			}
			delete(shard.streams, k)
		}
		shard.mu.Unlock()
	}

	if t.quicTr != nil {
		if err := t.quicTr.Close(); err != nil {
			slog.Debug("QUIC transport close failed", "error", err)
		}
	}

	t.wg.Wait()
	return nil
}

// Addr returns the local address the transport is listening on.
// Returns nil if the transport has not been started.
func (t *QUICTransport) Addr() net.Addr {
	if t.udpConn == nil {
		return nil
	}
	return t.udpConn.LocalAddr()
}

// Send delivers Raft messages to their target replicas via non-blocking
// per-target send queues. Each unique target address gets a dedicated
// buffered channel (capacity 2048) and a background sender goroutine.
// Messages are resolved to target addresses and enqueued without blocking.
//
// If a target's queue is full (sender goroutine backed up by slow QUIC),
// the message is silently dropped. This is safe because Raft handles
// retransmission via heartbeat and replicate cycles. The large queue
// capacity (2048) absorbs normal burst traffic without drops.
//
// This non-blocking design ensures the step worker's tick/processReady
// loop is never stalled by transport operations (QUIC dial, stream open,
// stream write with 5s deadline), preventing heartbeat starvation and
// cascading multi-node stalls.
//
// Each message is deep-copied (including entries and their Cmd payloads)
// before enqueueing into the per-target channel. The caller may reuse or
// modify the msgs slice and its contents after Send returns.
func (t *QUICTransport) Send(msgs []proto.Message) {
	if t.stopped.Load() {
		return
	}
	if len(msgs) == 0 {
		return
	}

	for i := range msgs {
		msg := msgs[i] // copy the message value (shallow)
		// Deep-copy entries to break the alias with the step worker's
		// reusable proposeBuf. The EntryQueue.Swap double-buffering
		// reuses backing arrays across drain cycles, so without this
		// copy, the sendQueueWorker may read entry fields while a
		// concurrent Add overwrites the same backing array.
		if len(msg.Entries) > 0 {
			entries := make([]proto.Entry, len(msg.Entries))
			for j := range msg.Entries {
				entries[j] = msg.Entries[j]
				if len(msg.Entries[j].Cmd) > 0 {
					cmd := make([]byte, len(msg.Entries[j].Cmd))
					copy(cmd, msg.Entries[j].Cmd)
					entries[j].Cmd = cmd
				}
			}
			msg.Entries = entries
		}
		addr, err := t.reg.Resolve(msg.ShardID, msg.To)
		if err != nil {
			continue
		}
		if !t.enqueueToTarget(addr, msg) {
			return // transport stopped
		}
	}
}

// enqueueToTarget resolves the per-target send queue and enqueues msg on the
// appropriate channel (heartbeat-priority or data). It returns false only when
// the transport is stopped (the queue could not be created).
//
// After a successful channel send it checks sq.closed: if the worker pruned
// this queue on idle timeout between getOrCreateSendQueue and the send, the
// message landed in an orphaned channel with no drainer. In that case it
// re-acquires a fresh queue (which starts a new worker) and re-enqueues once.
// The retry is bounded to a single attempt because the freshly created queue
// returned by getOrCreateSendQueue cannot have been pruned yet (its worker has
// not run its idle timer), so a second orphaning is impossible.
func (t *QUICTransport) enqueueToTarget(addr string, msg proto.Message) bool {
	for attempt := 0; attempt < 2; attempt++ {
		sq := t.getOrCreateSendQueue(addr)
		if sq == nil {
			return false // transport stopped
		}
		// Route heartbeats to the priority channel so they are never
		// blocked behind a backlog of data messages. This prevents
		// leader election instability under heavy replication load.
		if isHeartbeatMessage(msg.Type) {
			select {
			case sq.hbCh <- msg:
			default:
				return true // queue full; Raft retransmits
			}
		} else {
			// Non-blocking enqueue. If the queue is full, the message is
			// dropped. Raft retransmission will resend it on the next
			// heartbeat/replicate cycle.
			select {
			case sq.ch <- msg:
			default:
				// Log backpressure event when send queue is full.
				slog.Debug("send queue backpressure",
					"target", addr,
					"shard", msg.ShardID,
					"msg_type", proto.MessageTypeName(msg.Type),
					"queue_cap", defaultSendQueueLen,
				)
				return true
			}
		}
		// The send succeeded. If the worker pruned this queue on idle
		// timeout concurrently, the message is orphaned: re-acquire a fresh
		// queue and re-enqueue. A non-closed queue means the message will be
		// drained, so we are done.
		if !sq.closed.Load() {
			return true
		}
	}
	return true
}

// getOrCreateSendQueue returns the send queue for the given target address,
// creating one (with a background sender goroutine) if it does not exist.
// Returns nil if the transport is stopped. The fast path uses RLock for
// minimal contention on repeated sends to the same set of targets.
func (t *QUICTransport) getOrCreateSendQueue(target string) *sendQueue {
	if t.stopped.Load() {
		return nil
	}

	// Fast path: queue already exists and is not pruned.
	t.sendQueuesMu.RLock()
	sq, ok := t.sendQueues[target]
	t.sendQueuesMu.RUnlock()
	if ok && !sq.closed.Load() {
		return sq
	}

	// Slow path: create a new queue and start its sender goroutine.
	t.sendQueuesMu.Lock()
	if t.stopped.Load() {
		t.sendQueuesMu.Unlock()
		return nil
	}
	// Double-check after acquiring write lock. A queue that has been marked
	// closed by an idle-pruning worker is treated as absent: the worker sets
	// closed=true and deletes the map entry under this same lock, so any
	// entry we observe here as closed is mid-prune and must be replaced.
	sq, ok = t.sendQueues[target]
	if ok && !sq.closed.Load() {
		t.sendQueuesMu.Unlock()
		return sq
	}
	sq = &sendQueue{
		ch:   make(chan proto.Message, defaultSendQueueLen),
		hbCh: make(chan proto.Message, 64),
	}
	t.sendQueues[target] = sq
	t.wg.Add(1)
	go t.sendQueueWorker(target, sq)
	t.sendQueuesMu.Unlock()
	return sq
}

// sendQueueWorker is the per-target sender goroutine. It drains the send
// queue channels, batches messages, and delivers them via sendToTarget.
// The goroutine exits when stopC is closed or after sendQueueIdleTimeout
// of inactivity. Idle pruning prevents goroutine and memory leaks when
// cluster membership changes over time: removed nodes' send queues are
// cleaned up automatically instead of persisting until transport shutdown.
//
// Priority: heartbeat messages (in hbCh) are always drained before data
// messages (in ch). This prevents leader election instability under heavy
// replication load, where data messages could otherwise fill the queue and
// delay heartbeats past the election timeout.
//
// Batching strategy: after reading the first message, drain all remaining
// buffered messages (up to 256 for data, 16 for heartbeats) into a batch
// before sending. This amortizes QUIC frame overhead and reduces the number
// of stream writes per batch.
func (t *QUICTransport) sendQueueWorker(target string, sq *sendQueue) {
	defer t.wg.Done()

	for {
		panicked := t.sendQueueWorkerOnce(target, sq)
		if !panicked {
			return
		}
		// Check for shutdown before restarting.
		select {
		case <-t.stopC:
			return
		default:
		}
		timer := time.NewTimer(workerPanicRestartDelay)
		select {
		case <-t.stopC:
			timer.Stop()
			return
		case <-timer.C:
		}
	}
}

// sendQueueWorkerOnce runs one iteration of the send queue worker loop
// and recovers from panics. Returns true if the worker panicked, false
// if it returned normally (via stop signal or idle timeout).
func (t *QUICTransport) sendQueueWorkerOnce(target string, sq *sendQueue) (panicked bool) {
	defer func() {
		if r := recover(); r != nil {
			panicked = true
			stack := debug.Stack()
			panicErr := &WorkerPanicError{
				WorkerName: "sendQueueWorker",
				Target:     target,
				Value:      r,
				Stack:      stack,
			}
			slog.Error("sendQueueWorker panic recovered, restarting",
				"target", target,
				"panic", fmt.Sprint(r),
				"stack", string(stack),
				"error", panicErr,
			)
		}
	}()

	// Pre-size to the drain cap (1 initial + up to 255 more = 256) so a full
	// burst does not force a slice re-grow on the send path.
	batch := make([]proto.Message, 0, 256)
	idleTimer := time.NewTimer(sendQueueIdleTimeout)
	defer idleTimer.Stop()

	resetIdleTimer := func() {
		if !idleTimer.Stop() {
			select {
			case <-idleTimer.C:
			default:
			}
		}
		idleTimer.Reset(sendQueueIdleTimeout)
	}

	for {
		// Priority check: if heartbeats are pending, handle them first.
		// This non-blocking select ensures heartbeats are never starved
		// by a backlog of data messages in the main channel.
		select {
		case msg := <-sq.hbCh:
			batch = append(batch[:0], msg)
		drainHB:
			for range 15 {
				select {
				case m := <-sq.hbCh:
					batch = append(batch, m)
				default:
					break drainHB
				}
			}
			t.sendToTarget(target, batch)
			resetIdleTimer()
			continue
		default:
		}

		// Blocking select: wait for any message type, stop signal, or idle timeout.
		select {
		case <-t.stopC:
			return
		case msg := <-sq.hbCh:
			batch = append(batch[:0], msg)
		drainHB2:
			for range 15 {
				select {
				case m := <-sq.hbCh:
					batch = append(batch, m)
				default:
					break drainHB2
				}
			}
			t.sendToTarget(target, batch)
		case <-idleTimer.C:
			// No messages sent for sendQueueIdleTimeout. Remove
			// ourselves from the map and exit. The next Send to
			// this target will re-create the queue.
			//
			// Between the select picking the idle timer case and
			// acquiring the lock, a concurrent Send() may have
			// enqueued a message. Check under the lock whether the
			// channels are non-empty; if so, keep the entry alive.
			t.sendQueuesMu.Lock()
			if len(sq.ch) > 0 || len(sq.hbCh) > 0 {
				t.sendQueuesMu.Unlock()
				resetIdleTimer()
				continue
			}
			// Mark the queue closed before removing it from the map so a
			// producer that already holds this *sendQueue and enqueues after
			// the delete observes closed=true and re-enqueues into a fresh
			// queue. Both the flag set and the map delete happen under
			// sendQueuesMu, and the producer's closed-check runs after its
			// channel send, establishing the happens-before edge.
			sq.closed.Store(true)
			delete(t.sendQueues, target)
			t.sendQueuesMu.Unlock()
			slog.Debug("send queue idle pruned", "target", target)
			return
		case msg := <-sq.ch:
			batch = append(batch[:0], msg)

			// Drain additional buffered messages for batching.
		drainLoop:
			for range 255 {
				select {
				case m := <-sq.ch:
					batch = append(batch, m)
				default:
					break drainLoop
				}
			}

			t.sendToTarget(target, batch)
		}

		// Reset the idle timer after a successful send.
		resetIdleTimer()
	}
}

// sendToTarget dispatches a batch of messages to a single target, routing
// heartbeat-only batches through the fast path. Extracted from Send() to
// share logic between the single-target and parallel multi-target paths.
func (t *QUICTransport) sendToTarget(target string, batch []proto.Message) {
	if t.hbPrefix != nil && isHeartbeatBatch(batch) {
		t.sendHeartbeatBatch(target, batch)
	} else {
		t.sendBatch(target, batch)
	}
}

// sendBatch marshals a message batch and writes it as a framed payload to
// the appropriate stream for the target. Errors are logged and dropped.
func (t *QUICTransport) sendBatch(target string, msgs []proto.Message) { //nolint:gocognit // metrics instrumentation adds branches
	var sendStart time.Time
	if t.metrics != nil || slog.Default().Enabled(context.Background(), slog.LevelDebug) {
		sendStart = time.Now()
	}
	mb := proto.MessageBatch{
		BinVer:        proto.WireVersion,
		DeploymentID:  t.cfg.DeploymentID,
		SourceEpoch:   t.bootEpoch,
		SourceAddress: t.localAddr(),
		Requests:      msgs,
	}

	sz := mb.Size()
	bufPtr := t.getBuf(sz + FrameHeaderSize)
	buf := (*bufPtr)[:sz+FrameHeaderSize]

	payloadBuf := buf[FrameHeaderSize:]
	n, err := mb.MarshalTo(payloadBuf)
	if err != nil {
		t.putBuf(bufPtr)
		return
	}

	// Determine the stream index: heartbeats use stream 0.
	var shardID uint64
	isHeartbeat := false
	if len(msgs) > 0 {
		shardID = msgs[0].ShardID
		isHeartbeat = isHeartbeatMessage(msgs[0].Type)
	}

	// Apply Snappy compression to the serialized payload. Only use
	// compression if it reduces size (incompressible data is sent raw).
	// Heartbeat messages are tiny (<200 bytes) where Snappy overhead
	// exceeds savings; skip compression to reduce latency on the
	// ReadIndex critical path. DisableCompression bypasses Snappy
	// entirely for all message types.
	payload := payloadBuf[:n]
	flags := FlagNone
	var snappyBuf []byte     // tracks the pooled encode buffer for deferred return
	var snappyBufPtr *[]byte // original pool pointer; returned in Put to avoid local-variable aliasing
	if !isHeartbeat && !t.cfg.DisableCompression {
		maxEnc := snappy.MaxEncodedLen(len(payload))
		if v := t.snappyEncPool.Get(); v != nil {
			snappyBufPtr = v.(*[]byte)
			if cap(*snappyBufPtr) >= maxEnc {
				snappyBuf = (*snappyBufPtr)[:cap(*snappyBufPtr)]
			} else {
				snappyBuf = make([]byte, maxEnc)
				snappyBufPtr = nil // capacity too small; discard the pooled pointer
			}
		} else {
			snappyBuf = make([]byte, maxEnc)
		}
		compressed := snappy.Encode(snappyBuf, payload)
		if len(compressed) < len(payload) {
			payload = compressed
			flags = FlagCompressed
		}
	}

	// Ensure the buffer is large enough for the compressed frame.
	totalSize := FrameHeaderSize + len(payload)
	if cap(buf) < totalSize {
		t.putBuf(bufPtr)
		newBuf := make([]byte, totalSize)
		bufPtr = &newBuf
		buf = newBuf
	} else {
		buf = buf[:totalSize]
	}

	_, err = MarshalFrame(buf, payload, flags)

	// Return the Snappy encode buffer now that the payload has been
	// copied into the frame buffer. The compressed slice aliases
	// snappyBuf, so it must not be used after this point.
	//
	// We return the original *[]byte pointer from Get rather than
	// creating a pointer to a local slice header. This avoids a
	// subtle escape-analysis dependency where &localSlice would
	// only be heap-allocated due to the Pool.Put interface{} box.
	if snappyBufPtr != nil {
		*snappyBufPtr = snappyBuf[:0] // reset length but keep capacity
		t.snappyEncPool.Put(snappyBufPtr)
		snappyBufPtr = nil //nolint:ineffassign,wastedassign // defensive nil to prevent use-after-pool-return
	}
	snappyBuf = nil //nolint:wastedassign // defensive nil to prevent use-after-pool-return

	if err != nil {
		// A MarshalFrame failure here is almost always ErrPayloadTooLarge: the
		// serialized batch exceeds MaxFrameSize (e.g. a Replicate batch bounded
		// by maxReplicationPayload that is larger than the 16MB frame cap).
		// Surface it loudly — a silent drop would stall replication to a
		// lagging follower with no diagnostic. (NOTE: the underlying limit
		// mismatch — maxReplicationPayload/MaxBatchSize vs MaxFrameSize/
		// MaxDecompressedSize — should be reconciled in config.)
		slog.Error("transport: dropping batch that failed to marshal into a frame",
			"target", target,
			"payload_bytes", len(payload),
			"max_frame_size", MaxFrameSize,
			"error", err,
		)
		t.putBuf(bufPtr)
		return
	}

	frameSize := totalSize

	stream, err := t.getStream(target, shardID, isHeartbeat)
	if err != nil {
		t.putBuf(bufPtr)
		return
	}

	err = stream.SetWriteDeadline(time.Now().Add(sendWriteDeadline))
	if err != nil {
		t.putBuf(bufPtr)
		t.removeAndCloseConnection(target, "write deadline failure")
		t.evictStreamsForTarget(target)
		return
	}

	_, err = stream.Write(buf[:frameSize])
	t.putBuf(bufPtr)
	if err != nil {
		// Stream write failed — likely a dead connection. Close and evict
		// the connection and all its streams so the next send attempt dials
		// fresh. Without this, other cached streams on the same dead
		// connection would each hang for the full write deadline before
		// failing, compounding latency on every message type.
		t.removeAndCloseConnection(target, "stream write failure")
		t.evictStreamsForTarget(target)
		return
	}
	sendElapsed := time.Since(sendStart)
	if t.metrics != nil {
		t.metrics.ObserveSendLatency(sendElapsed)
		for i := range msgs {
			t.metrics.IncMessagesSent(msgs[i].ShardID, msgs[i].To, msgs[i].Type)
		}
	}
	if !sendStart.IsZero() && len(msgs) > 0 {
		slog.Debug("message batch sent",
			"target", target,
			"batch_size", len(msgs),
			"frame_bytes", frameSize,
			"send_ns", sendElapsed.Nanoseconds(),
		)
	}
}

// SendSnapshot sends snapshot chunks over a dedicated QUIC stream.
// Each snapshot transfer uses a fresh bidirectional stream to avoid
// interference with the regular message stream pool. The snapshotTracker
// is used to track in-flight sends so Stop() can wait for completion.
//
// Security note: Snapshot confidentiality and integrity in transit are provided
// by QUIC's mandatory TLS 1.3 encryption layer. All snapshot chunk data flows
// over a TLS-protected QUIC connection with mutual TLS (mTLS) authentication.
// Application-layer encryption of snapshot payloads (double encryption) is not
// currently implemented. This is a potential future defense-in-depth enhancement
// for deployments where the TLS termination point is untrusted, but for
// standard deployments the QUIC TLS layer provides sufficient protection.
// At-rest snapshot encryption is handled by the crypto.Barrier when configured.
func (t *QUICTransport) SendSnapshot(chunks []proto.SnapshotChunk) error {
	if len(chunks) == 0 {
		return nil
	}

	// Atomically register this send operation. TryAdd returns false if
	// Stop() has already initiated shutdown, eliminating the TOCTOU race
	// between the old stopped.Load() check and WaitGroup.Add(1).
	if !t.snapshotTracker.TryAdd() {
		return ErrStreamClosed
	}
	defer t.snapshotTracker.Done()

	snapStart := time.Now()

	first := &chunks[0]
	target, err := t.reg.Resolve(first.ShardID, first.ReplicaID)
	if err != nil {
		return err
	}

	conn, err := t.getConnection(target)
	if err != nil {
		return err
	}

	// Derive the stream-open context from shutdownCtx so that when
	// Stop() cancels shutdownCtx, the OpenStreamSync call is unblocked
	// and the stream is automatically torn down. The 60s timeout
	// covers the handshake; per-chunk deadlines handle data transfer.
	openCtx, openCancel := context.WithTimeout(t.shutdownCtx, 60*time.Second)
	defer openCancel()

	stream, err := conn.OpenStreamSync(openCtx)
	if err != nil {
		return err
	}
	defer func() {
		if closeErr := stream.Close(); closeErr != nil {
			slog.Debug("snapshot stream close failed",
				"shard", first.ShardID,
				"replica", first.ReplicaID,
				"error", closeErr)
		}
	}()

	// Write snapshot header first. Set a write deadline so the header
	// write does not block indefinitely on a stalled receiver.
	if err := stream.SetWriteDeadline(time.Now().Add(snapshotChunkWriteDeadline)); err != nil {
		return err
	}
	hdr := SnapshotHeader{
		ShardID:    first.ShardID,
		ReplicaID:  first.ReplicaID,
		Index:      first.Index,
		Term:       first.Term,
		ChunkCount: first.ChunkCount,
		Epoch:      first.Epoch,
	}
	var hdrBuf [SnapshotHeaderSize]byte
	if _, err := hdr.MarshalTo(hdrBuf[:]); err != nil {
		return err
	}

	// Write a frame containing the snapshot header to signal this is a
	// snapshot stream (FlagSnapshot).
	if err := WriteFrame(stream, hdrBuf[:], FlagSnapshot); err != nil {
		return err
	}

	// Fire OnSnapshotSendStarted after the header is written successfully.
	// If the send fails after this point, OnSnapshotSendAborted is fired
	// via the deferred closure below.
	snapInfo := config.SnapshotInfo{
		ShardID:   first.ShardID,
		ReplicaID: first.ReplicaID,
		Index:     first.Index,
	}
	if t.eventListener != nil && t.eventListener.OnSnapshotSendStarted != nil {
		t.eventListener.OnSnapshotSendStarted(snapInfo)
	}

	// Track whether the send completed successfully so the deferred
	// closure can fire OnSnapshotSendAborted on failure.
	sendOK := false
	defer func() {
		if !sendOK && t.eventListener != nil && t.eventListener.OnSnapshotSendAborted != nil {
			t.eventListener.OnSnapshotSendAborted(snapInfo)
		}
	}()

	// Write each chunk as a framed payload. Stamp the deployment ID on
	// each chunk so the receiver can validate it matches (analogous to
	// how sendBatch stamps DeploymentID on the MessageBatch header).
	//
	// Each chunk gets a fresh write deadline to prevent indefinite blocking
	// if the receiver is slow or the network stalls. Without this, a
	// stalled WriteFrame holds the snapshotTracker slot forever, making
	// Stop() -> ShutdownAndWait() block indefinitely.
	for i := range chunks {
		// Copy the chunk by value so stamping DeploymentID does not mutate
		// the caller's backing array. SendSnapshotStreaming already operates
		// on a by-value copy via its emit callback.
		chunk := chunks[i]
		chunk.DeploymentID = t.cfg.DeploymentID
		chunkSz := chunk.Size()
		chunkBuf := make([]byte, chunkSz)
		chunkN, err := chunk.MarshalTo(chunkBuf)
		if err != nil {
			return err
		}
		if err := stream.SetWriteDeadline(time.Now().Add(snapshotChunkWriteDeadline)); err != nil {
			return err
		}
		if err := WriteFrame(stream, chunkBuf[:chunkN], FlagNone); err != nil {
			return &SnapshotChunkWriteTimeoutError{
				ShardID:   first.ShardID,
				ReplicaID: first.ReplicaID,
				ChunkID:   uint64(i),
				Err:       err,
			}
		}
	}

	// Calculate total snapshot bytes sent.
	var snapBytes int64
	for i := range chunks {
		snapBytes += int64(len(chunks[i].Data))
	}

	// Mark send as successful before firing completion events.
	sendOK = true

	slog.Info("snapshot sent",
		"shard", first.ShardID,
		"replica", first.ReplicaID,
		"index", first.Index,
		"chunks", len(chunks))
	if t.eventListener != nil && t.eventListener.OnSnapshotSent != nil {
		t.eventListener.OnSnapshotSent(snapInfo)
	}
	if t.metrics != nil {
		t.metrics.IncSnapshotsSent(first.ShardID, snapBytes, time.Since(snapStart))
	}

	return nil
}

// SnapshotChunkProducer is a function that streams snapshot chunks to an
// emit callback one at a time. The producer reads chunk data from disk and
// calls emit for each chunk; emit writes the chunk to the wire. If emit
// returns an error (e.g., network write failure), the producer stops
// immediately and returns that error.
//
// This design keeps at most one chunk's worth of data (typically 2MB) in
// memory at any time, preventing OOM on large snapshots.
type SnapshotChunkProducer func(emit func(proto.SnapshotChunk) error) error

// SendSnapshotStreaming sends snapshot chunks over a dedicated QUIC stream
// using a streaming producer that emits chunks one at a time. Unlike
// SendSnapshot which requires all chunks in memory at once, this method
// holds at most one chunk in memory at any point, making it safe for large
// snapshots (up to 4GB).
//
// The meta parameter provides snapshot metadata for the stream header.
// The produce function is called with an emit callback; it must call emit
// for each chunk in order. Each chunk is serialized and written to the
// wire immediately upon emission.
func (t *QUICTransport) SendSnapshotStreaming(meta SnapshotHeader, produce SnapshotChunkProducer) error {
	// Atomically register this send operation. TryAdd returns false if
	// Stop() has already initiated shutdown, eliminating the TOCTOU race
	// between the old stopped.Load() check and WaitGroup.Add(1).
	if !t.snapshotTracker.TryAdd() {
		return ErrStreamClosed
	}
	defer t.snapshotTracker.Done()

	snapStart := time.Now()

	target, err := t.reg.Resolve(meta.ShardID, meta.ReplicaID)
	if err != nil {
		return err
	}

	conn, err := t.getConnection(target)
	if err != nil {
		return err
	}

	// Derive the stream-open context from shutdownCtx so that when
	// Stop() cancels shutdownCtx, the OpenStreamSync call is unblocked
	// and the stream is automatically torn down. The 60s timeout
	// covers the handshake; per-chunk deadlines handle data transfer.
	openCtx, openCancel := context.WithTimeout(t.shutdownCtx, 60*time.Second)
	defer openCancel()

	stream, err := conn.OpenStreamSync(openCtx)
	if err != nil {
		return err
	}
	defer func() {
		if closeErr := stream.Close(); closeErr != nil {
			slog.Debug("snapshot stream close failed",
				"shard", meta.ShardID,
				"replica", meta.ReplicaID,
				"error", closeErr)
		}
	}()

	// Write snapshot header first.
	if err := stream.SetWriteDeadline(time.Now().Add(snapshotChunkWriteDeadline)); err != nil {
		return err
	}
	var hdrBuf [SnapshotHeaderSize]byte
	if _, err := meta.MarshalTo(hdrBuf[:]); err != nil {
		return err
	}
	if err := WriteFrame(stream, hdrBuf[:], FlagSnapshot); err != nil {
		return err
	}

	// Fire OnSnapshotSendStarted after the header is written successfully.
	snapInfo := config.SnapshotInfo{
		ShardID:   meta.ShardID,
		ReplicaID: meta.ReplicaID,
		Index:     meta.Index,
	}
	if t.eventListener != nil && t.eventListener.OnSnapshotSendStarted != nil {
		t.eventListener.OnSnapshotSendStarted(snapInfo)
	}

	// Track whether the send completed successfully so the deferred
	// closure can fire OnSnapshotSendAborted on failure.
	sendOK := false
	defer func() {
		if !sendOK && t.eventListener != nil && t.eventListener.OnSnapshotSendAborted != nil {
			t.eventListener.OnSnapshotSendAborted(snapInfo)
		}
	}()

	// Stream chunks from the producer. Each chunk is serialized and
	// written to the wire immediately, then its data buffer can be GC'd
	// before the next chunk is read from disk.
	var chunkIdx uint64
	var snapBytes int64
	emitErr := produce(func(chunk proto.SnapshotChunk) error {
		chunk.DeploymentID = t.cfg.DeploymentID
		chunkSz := chunk.Size()
		chunkBuf := make([]byte, chunkSz)
		chunkN, marshalErr := chunk.MarshalTo(chunkBuf)
		if marshalErr != nil {
			return marshalErr
		}
		if deadlineErr := stream.SetWriteDeadline(time.Now().Add(snapshotChunkWriteDeadline)); deadlineErr != nil {
			return deadlineErr
		}
		if writeErr := WriteFrame(stream, chunkBuf[:chunkN], FlagNone); writeErr != nil {
			return &SnapshotChunkWriteTimeoutError{
				ShardID:   meta.ShardID,
				ReplicaID: meta.ReplicaID,
				ChunkID:   chunkIdx,
				Err:       writeErr,
			}
		}
		snapBytes += int64(len(chunk.Data))
		chunkIdx++
		return nil
	})
	if emitErr != nil {
		return emitErr
	}

	// Mark send as successful before firing completion events.
	sendOK = true

	slog.Info("snapshot sent",
		"shard", meta.ShardID,
		"replica", meta.ReplicaID,
		"index", meta.Index,
		"chunks", chunkIdx)
	if t.eventListener != nil && t.eventListener.OnSnapshotSent != nil {
		t.eventListener.OnSnapshotSent(snapInfo)
	}
	if t.metrics != nil {
		t.metrics.IncSnapshotsSent(meta.ShardID, snapBytes, time.Since(snapStart))
	}

	return nil
}

// localAddr returns the address this node advertises to peers as its
// reachable Raft transport address. When AdvertiseAddress is set in the
// config it takes priority over the OS-reported local socket address so
// that nodes behind NAT or in Docker containers advertise their external
// hostname/IP rather than 0.0.0.0 or [::].
func (t *QUICTransport) localAddr() string {
	if t.cfg.AdvertiseAddress != "" {
		return t.cfg.AdvertiseAddress
	}
	if t.localAddrStr != "" {
		return t.localAddrStr
	}
	return t.cfg.ListenAddress
}

// getBuf retrieves a buffer from the pool, ensuring it has at least minCap capacity.
func (t *QUICTransport) getBuf(minCap int) *[]byte {
	bufPtr := t.bufPool.Get().(*[]byte)
	if cap(*bufPtr) < minCap {
		buf := make([]byte, minCap)
		return &buf
	}
	return bufPtr
}

// putBuf returns a buffer to the pool if it is not oversized.
func (t *QUICTransport) putBuf(bufPtr *[]byte) {
	if cap(*bufPtr) <= maxPoolBufSize {
		*bufPtr = (*bufPtr)[:0]
		t.bufPool.Put(bufPtr)
	}
}

// SendQueueCount returns the number of active per-target send queues.
// Used for testing and metrics.
func (t *QUICTransport) SendQueueCount() int {
	t.sendQueuesMu.RLock()
	n := len(t.sendQueues)
	t.sendQueuesMu.RUnlock()
	return n
}

// isHeartbeatMessage returns true if the message type is a heartbeat or
// heartbeat response.
func isHeartbeatMessage(msgType uint64) bool {
	return msgType == proto.Heartbeat || msgType == proto.HeartbeatResp
}
