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
	"encoding/binary"
	"errors"
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
	"github.com/jeremyhahn/go-quicraft/pkg/internal/supervise"
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

// sendQueueIdleTimeout retires an idle per-target send worker; the QUIC connection stays open (keep-alive) until torn down explicitly.
const sendQueueIdleTimeout = 60 * time.Second

// snapshotChunkWriteDeadline bounds one snapshot chunk write, so a stalled receiver cannot hold a snapshotTracker slot and hang Stop.
const snapshotChunkWriteDeadline = 30 * time.Second

// defaultSendQueueLen is the per-target send queue capacity; a full queue drops and Raft retransmits.
const defaultSendQueueLen = 2048

// streamShard is a mutex-guarded bucket of the stream pool; owners records each stream's own connection for liveness checks, kept in lockstep with streams.
type streamShard struct {
	mu      sync.RWMutex
	streams map[streamKey]*quic.Stream
	owners  map[streamKey]*quic.Conn
}

// put records stream as the pooled entry for key, owned by conn. The caller
// must hold sh.mu. owners is nil only for a manually-constructed test transport
// that never reaches an insert site, so the guard keeps that path panic-free.
func (sh *streamShard) put(key streamKey, stream *quic.Stream, conn *quic.Conn) {
	sh.streams[key] = stream
	if sh.owners != nil {
		sh.owners[key] = conn
	}
}

// drop removes key from the stream pool and the owner map; the caller holds sh.mu.
func (sh *streamShard) drop(key streamKey) {
	delete(sh.streams, key)
	delete(sh.owners, key)
}

// streamKey identifies a pooled stream by its target address and pool index.
type streamKey struct {
	target string
	idx    uint64
}

// sendQueue is a per-target async send queue with a priority heartbeat channel (hbCh); closed tells a producer the worker exited, so it re-enqueues once on a fresh queue.
type sendQueue struct {
	ch     chan proto.Message
	hbCh   chan proto.Message // high-priority heartbeat channel
	closed atomic.Bool        // set by worker before idle-prune delete+return
}

// QUICTransport delivers Raft messages and snapshots over QUIC; Send is non-blocking via per-target send queues. Safe for concurrent use.
type QUICTransport struct {
	cfg     Config
	handler MessageHandler
	reg     *registry.Registry
	// tlsState is the current certificate, CA pool and verification state, swapped whole by ReloadMTLS; read it through currentTLS.
	tlsState atomic.Pointer[tlsPair]
	// reloadMu serializes ReloadMTLS, so the checker's issuer set and the
	// stored pair are always replaced together.
	reloadMu sync.Mutex
	quicCfg  *quic.Config
	udpConn  *net.UDPConn
	quicTr   *quic.Transport
	listener *quic.Listener

	connMu sync.RWMutex
	conns  map[string]*quic.Conn
	// connPeer caches the node identity each pooled outbound's peer presented; maintained with conns under connMu.
	connPeer map[*quic.Conn]string

	// connEpoch records the peer boot epoch each cached outbound was dialed under (0 if unknown), written with conns under connMu; comparing it to a received epoch detects restarts.
	connEpoch map[string]uint64

	// draining holds evicted outbounds awaiting close after staleOutboundDrainGrace, keyed to their target; whoever deletes an entry closes it. Guarded by connMu.
	draining map[*quic.Conn]string

	dialGroup singleflight.Group

	// dialReports orders the connection events of consecutive dials to one
	// destination (see getConnection).
	dialReports dialReportSequencer

	// bootEpoch is this process's per-start identity, stamped into every
	// outgoing MessageBatch (SourceEpoch). It changes only when this node
	// restarts.
	bootEpoch uint64

	// peerEpoch tracks each authenticated peer address's incarnations; reaped by reapPeerIncarnations once the address is idle and unconnected.
	peerEpochMu sync.Mutex
	peerEpoch   map[string]*peerIncarnation

	streamShards [streamShardCount]streamShard

	// beforeStreamInsert is a test seam, nil outside tests: when set,
	// getStream calls it after opening a stream on the slow path and before
	// taking the shard lock, so a test can stage the races that path resolves.
	beforeStreamInsert func()

	// sendQueuesMu protects the sendQueues map. Reads (fast path in Send)
	// use RLock; writes (queue creation for new targets) use Lock.
	sendQueuesMu sync.RWMutex
	sendQueues   map[string]*sendQueue

	snapshotRecv *snapshotReceiver

	// msgRecvBudget is the remaining aggregate budget for inbound frame buffers, reserved before allocation and released after dispatch.
	msgRecvBudget atomic.Int64

	stopC   chan struct{}
	stopped atomic.Bool

	// cleanupInterval is the cleanup sweep period (connectionCleanupInterval); a field so tests can shorten it.
	cleanupInterval time.Duration

	// stopOnce runs the teardown once and stopDone, closed by a defer, releases every other caller when it finishes.
	stopOnce sync.Once
	stopDone chan struct{}

	// acceptWG tracks only the accept loop, so stop can join it before t.wg and then sweep every admitted inbound connection.
	acceptWG sync.WaitGroup

	// acceptSem and acceptIPs are the accept loop's admission counts, created once so a supervised restart keeps them.
	acceptSem chan struct{}
	acceptIPs *ipConnTracker

	// inboundRefusalForTest, when set, replaces refuseInboundPromiscuousCert in admission.
	inboundRefusalForTest func(conn *quic.Conn) error

	// queuedCloseForTest, when set, runs before closeQueuedOnShutdown closes
	// each connection it drained from the accept queue, so tests can make
	// the drain outlast a deadline.
	queuedCloseForTest func()

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

	// hbPrefix is the pre-serialized MessageBatch header for heartbeat frames, computed in Start.
	hbPrefix []byte

	// wireLookup returns a shard's committed WireVersion; outbound batches
	// and snapshot streams are stamped from it. Nil until the host sets it,
	// which means every shard sends the window's previous version.
	wireLookup atomic.Pointer[WireVersionLookup]

	// eventListener holds optional event callbacks for connection and snapshot
	// lifecycle events. May be nil.
	eventListener *config.EventListener

	// metrics collects transport-level metrics. Nil when disabled.
	metrics Metrics

	// revocationChecker performs CRL/OCSP certificate revocation checking.
	// Nil when revocation checking is disabled. Created in NewQUICTransport
	// when Config.RevocationConfig is non-nil.
	revocationChecker *revocation.Checker

	// srcAddrAuth caches, per inbound connection, the result of checking a claimed SourceAddress against the peer certificate; removed when the handler returns.
	srcAddrAuth sync.Map

	// inboundConns tracks live inbound connections so revocation, idle reaping and CloseTarget can close them; entries are removed by forgetInboundConnState.
	inboundConns sync.Map

	// connMsgBudget holds per-peer message-receive reservations keyed by authenticated identity, capping one peer at maxConnMsgRecv; refcounted by live connection.
	connMsgBudget sync.Map

	// maxConnMsgRecv is the per-peer ceiling on in-flight message-frame
	// memory, in proto.DecodeCharge units like msgRecvBudget, derived from
	// MaxMessageReceiveMemory.
	maxConnMsgRecv int64

	// lastProtocolViolationLog is the per-transport rate-limit stamp of the last protocol-violation warning.
	lastProtocolViolationLog atomic.Int64

	// Rate-limit stamps for peer-driven diagnostics on the inbound hot path; the counters beside them count every event.
	lastRecvBudgetLog     atomic.Int64
	lastZeroShardLog      atomic.Int64
	lastSpoofedSourceLog  atomic.Int64
	lastSnapshotRejectLog atomic.Int64
	lastZeroRTTLog        atomic.Int64

	recvBudgetDrops     atomic.Uint64
	zeroShardDrops      atomic.Uint64
	spoofedSourceClears atomic.Uint64
	snapshotStreamDrops atomic.Uint64
	zeroRTTRefusals     atomic.Uint64

	// peerFrameRejections counts every frame a peer's decoder refused (QUICStreamErrFrameRejected).
	peerFrameRejections atomic.Uint64

	// lastPeerRejectionLog is the Unix-nanosecond timestamp of the most recent
	// peer-rejection warning, per transport instance (see
	// lastProtocolViolationLog for why it is not package-global).
	lastPeerRejectionLog atomic.Int64

	// lastIdentityMismatchLog rate-limits dropIdentityMismatch's warning.
	lastIdentityMismatchLog atomic.Int64

	// dialRefusals holds the last identity-pin dial refusal per key, suppressing re-dials for dialRefusalCooldown.
	dialRefusals sync.Map
}

// srcAddrAuthSlots is how many distinct claimed SourceAddresses are cached per inbound connection.
const srcAddrAuthSlots = 4

// srcAddrAuthMaxDistinct bounds the distinct claimed SourceAddresses verified per connection; beyond it the result is negative.
const srcAddrAuthMaxDistinct = 16

// srcAddrAuthSlot is one cached (claimed address, decision) pair.
type srcAddrAuthSlot struct {
	addr       string
	authorized bool
	valid      bool
}

// srcAddrAuthEntry caches SourceAddress authorization decisions for a single
// inbound connection. Slots are filled round-robin and scanned linearly;
// distinct counts how many addresses have been verified for this connection.
type srcAddrAuthEntry struct {
	mu       sync.Mutex
	slots    [srcAddrAuthSlots]srcAddrAuthSlot
	next     int
	distinct int
}

// newBootEpoch returns a per-process boot epoch (wall clock mixed with a random salt); restart detection keys on any change in it.
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

	// Build the revocation checker from the whole CA bundle and wire it into VerifyConnection; parse errors are fatal.
	var checker *revocation.Checker
	if cfg.RevocationConfig != nil {
		revCfg := *cfg.RevocationConfig
		revCfg.SetDefaults()
		if err := revCfg.Validate(); err != nil {
			return nil, err
		}
		issuers, parseErr := parseCACertificates(cfg.MTLSConfig.CACert)
		if parseErr != nil {
			return nil, parseErr
		}
		checker = revocation.NewChecker(revCfg, issuers...)
	}

	pair, err := buildMTLSTLS(cfg.MTLSConfig, checker)
	if err != nil {
		return nil, err
	}

	// QUIC incoming-stream limit: MaxStreamsPerConnection data streams plus MaxConcurrentSnapshotRecv snapshot streams.
	maxIncomingStreams := cfg.MaxStreamsPerConnection
	if maxIncomingStreams < 1 {
		maxIncomingStreams = defaultMaxStreamsPerConnection
	}
	snapStreams := cfg.MaxConcurrentSnapshotRecv
	if snapStreams < 1 {
		snapStreams = defaultMaxConcurrentSnapRecv
	}
	maxIncomingStreams += snapStreams
	// Floor the incoming-stream limit at a peer's StreamPoolSize plus its snapshot and ephemeral streams.
	if minStreams := cfg.StreamPoolSize + snapshotStreamHeadroom; maxIncomingStreams < minStreams {
		maxIncomingStreams = minStreams
	}

	qcfg := &quic.Config{
		MaxIdleTimeout: 30 * time.Second,
		// The handshake idle limit follows DialTimeout.
		HandshakeIdleTimeout:       cfg.DialTimeout,
		MaxIncomingStreams:         int64(maxIncomingStreams),
		MaxIncomingUniStreams:      -1,
		KeepAlivePeriod:            10 * time.Second,
		InitialStreamReceiveWindow: initialStreamReceiveWindow,
		MaxStreamReceiveWindow:     maxStreamReceiveWindow,
		MaxConnectionReceiveWindow: connectionReceiveWindow(cfg.MaxMessageReceiveMemory, cfg.MaxIncomingConnections),
		// 0-RTT is replayable (RFC 9001 section 8); Raft discards replayed RPCs and Config.Validate requires MTLSConfig.
		Allow0RTT: cfg.Enable0RTT,
	}

	shutdownCtx, shutdownCancel := context.WithCancel(context.Background())

	t := &QUICTransport{
		cfg:             cfg,
		handler:         handler,
		reg:             reg,
		quicCfg:         qcfg,
		conns:           make(map[string]*quic.Conn),
		connPeer:        make(map[*quic.Conn]string),
		connEpoch:       make(map[string]uint64),
		draining:        make(map[*quic.Conn]string),
		bootEpoch:       newBootEpoch(),
		peerEpoch:       make(map[string]*peerIncarnation),
		sendQueues:      make(map[string]*sendQueue),
		stopC:           make(chan struct{}),
		cleanupInterval: connectionCleanupInterval,
		stopDone:        make(chan struct{}),
		shutdownCtx:     shutdownCtx,
		shutdownCancel:  shutdownCancel,
		bufPool: sync.Pool{
			New: func() any {
				buf := make([]byte, 0, cfg.SendBatchMaxSize+FrameHeaderSize)
				return &buf
			},
		},
		// Without a New function the encode pool could never bootstrap:
		// Put was gated on a successful Get, so every compressed send
		// heap-allocated its destination buffer on the hot path.
		snappyEncPool: sync.Pool{
			New: func() any {
				buf := make([]byte, 0, maxPoolBufSize)
				return &buf
			},
		},
	}

	t.tlsState.Store(pair)

	for i := range t.streamShards {
		t.streamShards[i].streams = make(map[streamKey]*quic.Stream)
		t.streamShards[i].owners = make(map[streamKey]*quic.Conn)
	}

	t.snapshotRecv = newSnapshotReceiver(
		cfg.MaxConcurrentSnapshotRecv,
		cfg.MaxSnapshotReceiveMemory,
		cfg.MaxSnapshotReceiveRate,
		cfg.DeploymentID,
		handler,
	)
	t.snapshotRecv.wire = cfg.Wire
	t.snapshotRecv.SetSnapshotTracker(&t.snapshotTracker)
	t.msgRecvBudget.Store(cfg.MaxMessageReceiveMemory)
	// Cap one peer at a quarter of the aggregate budget, in proto.DecodeCharge units, but never below one maximum-frame charge.
	t.maxConnMsgRecv = max(cfg.MaxMessageReceiveMemory/4, maxFrameCharge(cfg.MaxDecompressedSize))

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
				setListenerSockopts(fd, recvBuf, sendBuf)
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

	serverTLS := t.listenerTLS()
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

	// A kernel-assigned port is resolved before anything serializes the
	// advertised address, so no peer is ever told port 0.
	t.resolveAdvertisedPort()

	// Pre-serialize the heartbeat MessageBatch header now that the local
	// address is resolved. This enables the zero-alloc heartbeat fast path.
	t.initHeartbeatPrefix()

	// Now that the advertised address is resolved, say so if it leaves the
	// promiscuous-certificate refusal unable to ask its question.
	t.reportPromiscuousCertCoverage()

	// Start the revocation checker's CRL reload goroutine. This must
	// happen after the listener is created but before accepting
	// connections so that the initial CRL load is complete.
	if t.revocationChecker != nil {
		t.revocationChecker.Start()
	}

	t.acceptWG.Add(1)
	t.startAcceptLoop()

	supervise.Supervise(&t.wg, t.stopC, t.connectionCleanup, nil)

	return nil
}

// SetEpochTracker configures the epoch tracker on the snapshot receiver.
// When set, the receiver will register in-flight epochs during snapshot
// transfers to prevent premature key purge. Must be called before Start.
func (t *QUICTransport) SetEpochTracker(tracker EpochTracker) {
	t.snapshotRecv.SetEpochTracker(tracker)
}

// SetShardEpochTrackers sets a per-shard epoch tracker resolver for per-shard barriers, taking precedence over SetEpochTracker. Call before Start.
func (t *QUICTransport) SetShardEpochTrackers(fn func(shardID uint64) EpochTracker) {
	t.snapshotRecv.SetShardEpochTrackers(fn)
}

// SetListenerDispatcher installs the host's listener dispatcher, which delivers OnSnapshotRejected off the receive goroutine; other callbacks stay inline. Call before Start.
func (t *QUICTransport) SetListenerDispatcher(dispatch func(fn func()) bool) {
	t.snapshotRecv.setListenerDispatcher(dispatch)
}

// Stop shuts the transport down: drains send workers, stops accepting, waits for snapshot transfers, announces the close on outbound and inbound connections and joins every goroutine. Idempotent; concurrent callers block until it completes.
func (t *QUICTransport) Stop() error {
	t.stopOnce.Do(t.stop)
	<-t.stopDone
	return nil
}

// stop performs the one-shot teardown behind Stop's sync.Once.
func (t *QUICTransport) stop() {
	defer close(t.stopDone)

	t.stopped.Store(true)
	close(t.stopC)
	t.shutdownCancel()

	// Stop the revocation checker's CRL reload goroutine before
	// tearing down connections.
	if t.revocationChecker != nil {
		t.revocationChecker.Stop()
	}

	// Barrier with getOrCreateSendQueue: after this, no new wg.Add(1) happens.
	t.sendQueuesMu.Lock()
	_ = t.stopped.Load()
	t.sendQueuesMu.Unlock()

	// Stop new snapshot operations and wait for in-flight ones before tearing connections down.
	t.snapshotTracker.ShutdownAndWait()

	// Every diagnostic below goes through supervise.LogContained so a log handler cannot abort the teardown.
	if t.listener != nil {
		if err := t.listener.Close(); err != nil {
			supervise.LogContained(func() { slog.Debug("QUIC listener close failed", "error", err) })
		}
	}

	// Join the accept loop (not t.wg, which would deadlock) so every admitted connection is in t.inboundConns before the sweep.
	t.acceptWG.Wait()

	// Drain connections still queued in the listener and announce the shutdown on each.
	if n := t.closeQueuedOnShutdown(); n > 0 {
		supervise.LogContained(func() { slog.Debug("announced shutdown on queued inbound connections", "connections", n) })
	}

	t.connMu.Lock()
	for addr, conn := range t.conns {
		if err := conn.CloseWithError(QUICErrShutdown, "transport shutdown"); err != nil {
			supervise.LogContained(func() { slog.Debug("QUIC connection close failed", "addr", addr, "error", err) })
		}
		delete(t.connPeer, conn)
		delete(t.conns, addr)
		delete(t.connEpoch, addr)
	}
	// Close outbounds still inside their drain grace.
	for conn := range t.draining {
		if err := conn.CloseWithError(QUICErrShutdown, "transport shutdown"); err != nil {
			supervise.LogContained(func() { slog.Debug("QUIC draining connection close failed", "error", err) })
		}
		delete(t.draining, conn)
	}
	t.connMu.Unlock()

	// Announce the shutdown on inbound connections too, after the listener close and before quicTr.Close, which would destroy them silently.
	if n := t.closeInboundOnShutdown(); n > 0 {
		supervise.LogContained(func() { slog.Debug("announced shutdown on inbound connections", "connections", n) })
	}

	t.peerEpochMu.Lock()
	clear(t.peerEpoch)
	t.peerEpochMu.Unlock()

	// Close all pooled streams.
	for i := range t.streamShards {
		shard := &t.streamShards[i]
		shard.mu.Lock()
		for k, s := range shard.streams {
			if err := s.Close(); err != nil {
				target, idx := k.target, k.idx
				supervise.LogContained(func() {
					slog.Debug("QUIC stream close failed", "target", target, "idx", idx, "error", err)
				})
			}
			shard.drop(k)
		}
		shard.mu.Unlock()
	}

	if t.quicTr != nil {
		if err := t.quicTr.Close(); err != nil {
			supervise.LogContained(func() { slog.Debug("QUIC transport close failed", "error", err) })
		}
	}

	t.wg.Wait()

	// quic.Transport does not close a socket it was handed; close the listener socket so the address is released.
	if t.udpConn != nil {
		if err := t.udpConn.Close(); err != nil {
			supervise.LogContained(func() { slog.Debug("UDP listener close failed", "error", err) })
		}
	}
}

// closeQueuedOnShutdown closes every connection left in the closed listener's accept queue with QUICErrShutdown; it runs after the accept loop returns and before quicTr.Close, without a deadline.
func (t *QUICTransport) closeQueuedOnShutdown() int {
	if t.listener == nil {
		return 0
	}
	ctx := context.Background()
	closed := 0
	for {
		conn, err := t.listener.Accept(ctx)
		if err != nil {
			return closed
		}
		if t.queuedCloseForTest != nil {
			t.queuedCloseForTest()
		}
		if err := conn.CloseWithError(QUICErrShutdown, "transport shutdown"); err != nil {
			remote := conn.RemoteAddr().String()
			supervise.LogContained(func() {
				slog.Debug("QUIC queued inbound connection close failed", "remote", remote, "error", err)
			})
		}
		closed++
	}
}

// Addr returns the local address the transport is listening on.
// Returns nil if the transport has not been started.
func (t *QUICTransport) Addr() net.Addr {
	if t.udpConn == nil {
		return nil
	}
	return t.udpConn.LocalAddr()
}

// AdvertisedAddr returns the address this transport tells peers to reach it
// at. After Start it carries the bound port when the configured advertise
// address asked for a kernel-assigned one (port 0).
func (t *QUICTransport) AdvertisedAddr() string {
	return t.localAddr()
}

// resolveAdvertisedPort replaces a port-0 AdvertiseAddress with the port the
// listener actually bound, keeping the advertised host. Start calls it once,
// after binding and before any goroutine reads the config.
func (t *QUICTransport) resolveAdvertisedPort() {
	host, port, err := net.SplitHostPort(t.cfg.AdvertiseAddress)
	if err != nil || port != "0" {
		return
	}
	_, bound, err := net.SplitHostPort(t.localAddrStr)
	if err != nil {
		return
	}
	t.cfg.AdvertiseAddress = net.JoinHostPort(host, bound)
}

// Send enqueues messages on per-target send queues without blocking, dropping when a queue is full (Raft retransmits); each message is deep-copied, so the caller may reuse msgs.
func (t *QUICTransport) Send(msgs []proto.Message) {
	if t.stopped.Load() {
		return
	}
	if len(msgs) == 0 {
		return
	}

	// Evaluated once per Send so drop paths do not build slog arguments.
	debugOn := debugEnabled()

	for i := range msgs {
		msg := msgs[i] // copy the message value (shallow)
		// Injected network fault (integration builds only): a silent drop, compiled away otherwise.
		if netFaultEnabled && netFaultDrop(msg.ShardID, msg.From, msg.To) {
			continue
		}
		// Deep-copy entries: they alias the step worker's reusable proposeBuf.
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
		// Deep-copy the snapshot's membership maps and checksum, which the async marshal ranges over.
		if !msg.Snapshot.IsEmpty() {
			copySnapshotRefs(&msg.Snapshot)
		}
		addr, err := t.reg.Resolve(msg.ShardID, msg.To)
		if err != nil {
			// Debug level: unresolved targets recur for every message to a
			// removed/not-yet-registered replica, so this can be high-frequency.
			if debugOn {
				slog.Debug("transport: dropping message: target address unresolved",
					"shard", msg.ShardID,
					"to", msg.To,
					"msg_type", proto.MessageTypeName(msg.Type),
					"error", err,
				)
			}
			if t.metrics != nil {
				t.metrics.IncMessagesDropped(msg.ShardID, msg.To, dropReasonUnresolvedTarget)
			}
			continue
		}
		if !t.enqueueToTarget(addr, msg, debugOn) {
			return // transport stopped
		}
	}
}

// debugEnabled reports whether the default logger emits Debug; hot drop paths gate on it.
func debugEnabled() bool {
	return slog.Default().Enabled(context.Background(), slog.LevelDebug)
}

// copySnapshotRefs replaces every reference field of a snapshot's metadata, every membership map included, with an owned copy.
func copySnapshotRefs(s *proto.Snapshot) {
	s.Membership.Addresses = copyAddrMap(s.Membership.Addresses)
	s.Membership.Observers = copyAddrMap(s.Membership.Observers)
	s.Membership.Witnesses = copyAddrMap(s.Membership.Witnesses)
	s.Membership.Identities = copyAddrMap(s.Membership.Identities)
	s.Membership.RemovedIdentities = copyAddrMap(s.Membership.RemovedIdentities)
	if s.Membership.Removed != nil {
		removed := make(map[uint64]bool, len(s.Membership.Removed))
		for k, v := range s.Membership.Removed {
			removed[k] = v
		}
		s.Membership.Removed = removed
	}
}

// copyAddrMap returns an owned copy of a replica-ID-to-address map, or nil
// when the source is nil (preserving the distinction the wire format keeps).
func copyAddrMap(src map[uint64]string) map[uint64]string {
	if src == nil {
		return nil
	}
	dst := make(map[uint64]string, len(src))
	for k, v := range src {
		dst[k] = v
	}
	return dst
}

// enqueueToTarget enqueues msg on its target's heartbeat or data channel, re-enqueueing once if the queue was pruned meanwhile; false only when stopped.
func (t *QUICTransport) enqueueToTarget(addr string, msg proto.Message, debugOn bool) bool {
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
				// Debug level: heartbeat backpressure can be high-frequency
				// under load; Raft retransmits on the next tick.
				if debugOn {
					slog.Debug("heartbeat queue backpressure",
						"target", addr,
						"shard", msg.ShardID,
						"msg_type", proto.MessageTypeName(msg.Type),
					)
				}
				if t.metrics != nil {
					t.metrics.IncMessagesDropped(msg.ShardID, msg.To, dropReasonQueueFull)
				}
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
				if debugOn {
					slog.Debug("send queue backpressure",
						"target", addr,
						"shard", msg.ShardID,
						"msg_type", proto.MessageTypeName(msg.Type),
						"queue_cap", defaultSendQueueLen,
					)
				}
				if t.metrics != nil {
					t.metrics.IncMessagesDropped(msg.ShardID, msg.To, dropReasonQueueFull)
				}
				return true
			}
		}
		// If the queue was pruned concurrently, re-enqueue on a fresh one.
		if !sq.closed.Load() {
			return true
		}
	}
	return true
}

// getOrCreateSendQueue returns target's send queue, creating it and its worker if needed; nil when stopped.
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
	// Re-check under the write lock; a closed queue is mid-prune and is replaced.
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
	supervise.Supervise(&t.wg, t.stopC, func() { t.sendQueueWorker(target, sq) }, nil)
	t.sendQueuesMu.Unlock()
	return sq
}

// retireSendQueue marks target's queue closed and removes it under sendQueuesMu; the worker drains what is buffered and retires itself.
func (t *QUICTransport) retireSendQueue(target string) {
	t.sendQueuesMu.Lock()
	if sq, ok := t.sendQueues[target]; ok {
		sq.closed.Store(true)
		delete(t.sendQueues, target)
	}
	t.sendQueuesMu.Unlock()
}

// sendQueueWorker drains a target's queue in batches (heartbeats first) and sends them; it exits on stopC or after sendQueueIdleTimeout, supervised by supervise.Supervise.
func (t *QUICTransport) sendQueueWorker(target string, sq *sendQueue) {
	for t.sendQueueWorkerOnce(target, sq) {
		if !supervise.AwaitRestart(t.stopC) {
			return
		}
	}
}

// sendQueueWorkerOnce runs one iteration of the send queue worker loop
// and recovers from panics. Returns true if the worker panicked, false
// if it returned normally (via stop signal or idle timeout).
func (t *QUICTransport) sendQueueWorkerOnce(target string, sq *sendQueue) (panicked bool) {
	normalReturn := false
	defer func() {
		if r, abnormal := supervise.ClassifyExit(recover(), normalReturn); abnormal {
			panicked = true
			stack := debug.Stack()
			logRecovered("sendQueueWorker panic recovered, restarting", r, stack,
				"target", target,
				"error", &WorkerPanicError{WorkerName: "sendQueueWorker", Target: target, Value: r, Stack: stack})
		}
	}()
	t.drainSendQueue(target, sq)
	normalReturn = true
	return false
}

// drainSendQueue is the send queue worker's body: it sends what sq holds to
// target until the transport stops or the queue is pruned idle.
func (t *QUICTransport) drainSendQueue(target string, sq *sendQueue) {
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
			// Idle: remove this queue and exit, unless a message arrived meanwhile.
			t.sendQueuesMu.Lock()
			if len(sq.ch) > 0 || len(sq.hbCh) > 0 {
				t.sendQueuesMu.Unlock()
				resetIdleTimer()
				continue
			}
			// Mark closed, then delete the map entry only if it is still this queue.
			sq.closed.Store(true)
			if cur, ok := t.sendQueues[target]; ok && cur == sq {
				delete(t.sendQueues, target)
			}
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

// sendToTarget sends a batch to one target, split by destination identity so each group is written only over a connection presenting it (Config.ReplicaIdentity).
func (t *QUICTransport) sendToTarget(target string, batch []proto.Message) {
	if t.cfg.ReplicaIdentity == nil || len(batch) == 0 {
		t.sendGroup(target, "", batch)
		return
	}
	want := t.cfg.ReplicaIdentity(batch[0].ShardID, batch[0].To)
	for i := 1; i < len(batch); i++ {
		if t.cfg.ReplicaIdentity(batch[i].ShardID, batch[i].To) != want {
			t.sendMixedGroups(target, batch)
			return
		}
	}
	if want == "" {
		t.dropUnbound(target, batch)
		return
	}
	t.sendGroup(target, want, batch)
}

// sendMixedGroups sends a batch whose messages are for replicas bound to
// different identities, one group per identity, in the order each identity
// first appears; order within a group is preserved.
func (t *QUICTransport) sendMixedGroups(target string, batch []proto.Message) {
	var order []string
	groups := make(map[string][]proto.Message)
	for i := range batch {
		want := t.cfg.ReplicaIdentity(batch[i].ShardID, batch[i].To)
		if _, seen := groups[want]; !seen {
			order = append(order, want)
		}
		groups[want] = append(groups[want], batch[i])
	}
	for _, want := range order {
		if want == "" {
			t.dropUnbound(target, groups[want])
			continue
		}
		t.sendGroup(target, want, groups[want])
	}
}

// dropUnbound drops messages for replicas no identity is bound to: with
// pinning enabled there is no peer they may be written to.
func (t *QUICTransport) dropUnbound(target string, msgs []proto.Message) {
	for i := range msgs {
		t.dropIdentityMismatch(target, &msgs[i])
	}
}

// sendGroup sends messages that must all reach the peer presenting want.
func (t *QUICTransport) sendGroup(target, want string, batch []proto.Message) {
	if t.hbPrefix != nil && isHeartbeatBatch(batch) {
		t.sendHeartbeatBatch(target, want, batch)
	} else {
		t.sendBatch(target, want, batch)
	}
}

// streamDropReason is the drop label for a batch whose stream could not be
// had: identity_mismatch when the peer at target is not its destination.
func streamDropReason(err error) string {
	if _, mismatch := errors.AsType[*DialIdentityError](err); mismatch {
		return dropReasonIdentityMismatch
	}
	return dropReasonStreamUnavailable
}

// incBatchDropped records one IncMessagesDropped sample per message in a
// dropped batch, so the counter measures messages rather than batches. No-op
// when metrics are disabled.
func (t *QUICTransport) incBatchDropped(msgs []proto.Message, reason string) {
	if t.metrics == nil {
		return
	}
	for i := range msgs {
		t.metrics.IncMessagesDropped(msgs[i].ShardID, msgs[i].To, reason)
	}
}

// sendBatch writes a batch as as many frames as needed and drops, before writing, any message no peer can accept; want is the destination identity.
func (t *QUICTransport) sendBatch(target, want string, msgs []proto.Message) {
	headerSize := batchHeaderSize(t.localAddr())
	for len(msgs) > 0 {
		n, payload, err := sendableGroup(headerSize, msgs)
		if err != nil {
			t.dropUnsendable(target, &msgs[0], headerSize, err)
			msgs = msgs[1:]
			continue
		}
		t.sendFrame(target, want, msgs[:n], payload)
		msgs = msgs[n:]
	}
}

// dropUnsendable records and logs at Error a message the transport refuses to write, a producer defect.
func (t *QUICTransport) dropUnsendable(target string, msg *proto.Message, headerSize int, reason error) {
	slog.Error("transport: dropping message that cannot be framed; the peer will not receive it until the producer bounds this message",
		"target", target,
		"shard", msg.ShardID,
		"to", msg.To,
		"msg_type", proto.MessageTypeName(msg.Type),
		"entries", len(msg.Entries),
		"message_bytes", msg.Size(),
		"batch_header_bytes", headerSize,
		"max_frame_size", MaxFrameSize,
		"max_entries_per_message", proto.MaxEntriesPerMessage,
		"error", reason,
	)
	if t.metrics != nil {
		t.metrics.IncMessagesDropped(msg.ShardID, msg.To, dropReasonUnsendable)
	}
}

// peerRejectionLogInterval rate-limits the peer-rejection warning. It matches
// protocolViolationLogInterval so the two ends of the same event are sampled at
// the same cadence.
const peerRejectionLogInterval = 10 * time.Second

// recordPeerFrameRejection counts a frame the peer's decoder refused and emits
// a rate-limited warning carrying the running total, so a storm is visible as a
// number even though the log lines are sampled.
func (t *QUICTransport) recordPeerFrameRejection(target string, shardID uint64, batchSize int, err error) {
	total := t.peerFrameRejections.Add(1)

	now := time.Now().UnixNano()
	last := t.lastPeerRejectionLog.Load()
	if now-last < int64(peerRejectionLogInterval) {
		return
	}
	if !t.lastPeerRejectionLog.CompareAndSwap(last, now) {
		return
	}
	slog.Warn("transport: peer rejected a frame; batch dropped, connection kept",
		"target", target,
		"shard", shardID,
		"batch_size", batchSize,
		"total_rejections", total,
		"rate_limit", peerRejectionLogInterval,
		"error", err,
	)
}

// PeerFrameRejections returns the number of frames peers have rejected since
// this transport started.
func (t *QUICTransport) PeerFrameRejections() uint64 {
	return t.peerFrameRejections.Load()
}

// sendFrame marshals msgs as one MessageBatch frame of payloadSize and writes it to the target's stream; errors are logged and dropped.
func (t *QUICTransport) sendFrame(target, want string, msgs []proto.Message, payloadSize int) { //nolint:gocognit // metrics instrumentation adds branches
	debugOn := debugEnabled()
	var sendStart time.Time
	if t.metrics != nil || debugOn {
		sendStart = time.Now()
	}
	binVer, frameVer := t.batchSendVersions(msgs)
	mb := proto.MessageBatch{
		BinVer:        binVer,
		MaxBinVer:     t.cfg.Wire.Proto.Current,
		DeploymentID:  t.cfg.DeploymentID,
		SourceEpoch:   t.bootEpoch,
		SourceAddress: t.localAddr(),
		Requests:      msgs,
	}

	sz := payloadSize
	bufPtr := t.getBuf(sz + FrameHeaderSize)
	buf := (*bufPtr)[:sz+FrameHeaderSize]

	payloadBuf := buf[FrameHeaderSize:]
	n, err := mb.MarshalTo(payloadBuf)
	if err != nil {
		// A marshal failure indicates an internal sizing bug; surface it
		// loudly rather than dropping the batch silently.
		t.putBuf(bufPtr)
		slog.Error("transport: dropping batch that failed to marshal",
			"target", target,
			"batch_size", len(msgs),
			"error", err,
		)
		t.incBatchDropped(msgs, dropReasonMarshalFailed)
		return
	}

	// Determine the stream index: heartbeats use stream 0.
	var shardID uint64
	isHeartbeat := false
	if len(msgs) > 0 {
		shardID = msgs[0].ShardID
		isHeartbeat = isHeartbeatMessage(msgs[0].Type)
	}

	// Snappy-compress unless it does not shrink the payload, for heartbeats, or when DisableCompression is set.
	payload := payloadBuf[:n]
	flags := FlagNone
	var snappyBuf []byte     // tracks the pooled encode buffer for deferred return
	var snappyBufPtr *[]byte // original pool pointer; returned in Put to avoid local-variable aliasing
	if !isHeartbeat && !t.cfg.DisableCompression {
		maxEnc := snappy.MaxEncodedLen(len(payload))
		snappyBufPtr = t.snappyEncPool.Get().(*[]byte)
		if cap(*snappyBufPtr) < maxEnc {
			// Grow through the pooled pointer so the buffer is still returned
			// (subject to the maxPoolBufSize cap) instead of being discarded.
			*snappyBufPtr = make([]byte, 0, maxEnc)
		}
		snappyBuf = (*snappyBufPtr)[:cap(*snappyBufPtr)]
		compressed := snappy.Encode(snappyBuf, payload)
		if len(compressed) < len(payload) {
			payload = compressed
			flags = FlagCompressed
		}
	}

	// Grow the buffer for the frame; keep the pooled buffer until MarshalFrame runs, since payload may alias it.
	totalSize := FrameHeaderSize + len(payload)
	grewBuf := cap(buf) < totalSize
	oldBufPtr := bufPtr
	if grewBuf {
		newBuf := make([]byte, totalSize)
		bufPtr = &newBuf
		buf = newBuf
	} else {
		buf = buf[:totalSize]
	}

	_, err = marshalFrameAt(buf, payload, flags, frameVer)
	if grewBuf {
		t.putBuf(oldBufPtr)
	}

	// Return the Snappy buffer (the original pointer from Get); compressed is invalid after this.
	if snappyBufPtr != nil {
		*snappyBufPtr = snappyBuf[:0] // reset length but keep capacity
		// Apply the standard pool cap: an encode buffer sized for a large
		// payload must not be retained per P as unaccounted memory.
		if cap(*snappyBufPtr) <= maxPoolBufSize {
			t.snappyEncPool.Put(snappyBufPtr)
		}
		snappyBufPtr = nil //nolint:ineffassign,wastedassign // defensive nil to prevent use-after-pool-return
	}
	snappyBuf = nil //nolint:wastedassign // defensive nil to prevent use-after-pool-return

	if err != nil {
		// Unreachable: sendBatch sized the group. Drop rather than write an unacceptable frame.
		t.putBuf(bufPtr)
		slog.Error("transport: dropping frame that failed to marshal despite passing the pre-write size check",
			"target", target,
			"shard", shardID,
			"batch_size", len(msgs),
			"payload_bytes", len(payload),
			"max_frame_size", MaxFrameSize,
			"max_replication_payload_bytes", MaxReplicationPayloadBytes,
			"error", err,
		)
		t.incBatchDropped(msgs, dropReasonMarshalFailed)
		return
	}

	frameSize := totalSize

	// Each failure branch settles state (buffer, eviction) before metrics and diagnostics.
	stream, err := t.getStream(target, shardID, isHeartbeat, want)
	if err != nil {
		t.putBuf(bufPtr)
		// Debug level: repeats for every batch while a peer is unreachable;
		// Raft retransmits once the peer returns.
		if debugOn {
			slog.Debug("transport: dropping batch: stream unavailable",
				"target", target,
				"batch_size", len(msgs),
				"error", err,
			)
		}
		t.incBatchDropped(msgs, streamDropReason(err))
		return
	}

	err = stream.SetWriteDeadline(time.Now().Add(sendWriteDeadline))
	if err != nil {
		t.putBuf(bufPtr)
		t.removeAndCloseConnection(target, "write deadline failure")
		t.evictStreamsForTarget(target)
		t.incBatchDropped(msgs, dropReasonWriteFailed)
		return
	}

	_, err = stream.Write(buf[:frameSize])
	t.putBuf(bufPtr)
	if err != nil {
		// The peer rejected the frame: drop the batch and replace only the stream; the connection is alive.
		if isPeerFrameRejection(err) {
			t.evictStream(target, shardID, isHeartbeat, stream)
			t.incBatchDropped(msgs, dropReasonPeerRejected)
			t.recordPeerFrameRejection(target, shardID, len(msgs), err)
			return
		}
		// Write failed: evict the connection and all its streams so the next send dials fresh.
		t.removeAndCloseConnection(target, "stream write failure")
		t.evictStreamsForTarget(target)
		// Debug level: a dead peer fails every in-flight batch until eviction
		// completes; Raft retransmits the dropped messages.
		if debugOn {
			slog.Debug("transport: dropping batch: stream write failed",
				"target", target,
				"batch_size", len(msgs),
				"error", err,
			)
		}
		t.incBatchDropped(msgs, dropReasonWriteFailed)
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

// writeSnapshotHeader writes the framed SnapshotHeader (FlagSnapshot) that opens a snapshot stream, under a write deadline.
func writeSnapshotHeader(stream *quic.Stream, first *proto.SnapshotChunk, ver byte) error {
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
		From:       first.From,
	}
	var hdrBuf [SnapshotHeaderSize]byte
	if _, err := hdr.MarshalTo(hdrBuf[:]); err != nil {
		return err
	}
	return writeFrameAt(stream, hdrBuf[:], FlagSnapshot, ver)
}

// SendSnapshot sends snapshot chunks on a dedicated QUIC stream tracked by snapshotTracker; transit protection is QUIC TLS 1.3 with mTLS, at rest the seal.Barrier.
func (t *QUICTransport) SendSnapshot(chunks []proto.SnapshotChunk) error {
	if len(chunks) == 0 {
		return nil
	}

	// Register the send; TryAdd returns false once Stop has begun.
	if !t.snapshotTracker.TryAdd() {
		return ErrStreamClosed
	}
	defer t.snapshotTracker.Done()

	snapStart := time.Now()

	first := &chunks[0]
	// Injected partition: fail the transfer as a stream error so the sender retries. Compiled away otherwise.
	if netFaultEnabled && netFaultDrop(first.ShardID, first.From, first.ReplicaID) {
		return ErrStreamClosed
	}
	target, err := t.reg.Resolve(first.ShardID, first.ReplicaID)
	if err != nil {
		return err
	}

	// A snapshot is the whole state machine: it goes only over a connection
	// to the replica it is for (see sendToTarget).
	want, bound := t.wantFor(first.ShardID, first.ReplicaID)
	if !bound {
		return &DialError{Target: target, Err: &DialIdentityError{}}
	}
	conn, err := t.getConnection(target, want)
	if err != nil {
		return err
	}

	// Open the stream on a context derived from shutdownCtx, so Stop unblocks it.
	openCtx, openCancel := context.WithTimeout(t.shutdownCtx, 60*time.Second)
	defer openCancel()

	stream, err := conn.OpenStreamSync(openCtx)
	if err != nil {
		return err
	}
	defer t.closeSnapshotStream(stream, first.ShardID, first.ReplicaID)

	// The whole stream is stamped with the shard's committed version as it
	// stands now, so a switch committed mid-transfer cannot split it.
	binVer, frameVer := t.shardSendVersions(first.ShardID)
	if err := writeSnapshotHeader(stream, first, frameVer); err != nil {
		return err
	}

	// OnSnapshotSendStarted fires after the header; the deferred closure fires OnSnapshotSendAborted on failure. Listener calls are isolated (deliverEvent).
	snapInfo := config.SnapshotInfo{
		ShardID:   first.ShardID,
		ReplicaID: first.ReplicaID,
		Index:     first.Index,
		From:      first.From,
	}
	sendOK := false
	defer func() {
		if !sendOK {
			deliverEvent(t.snapshotEvent(onSnapshotSendAborted, snapInfo))
		}
	}()
	deliverEvent(t.snapshotEvent(onSnapshotSendStarted, snapInfo))

	// Write each chunk with the deployment ID and a fresh write deadline; the shutdown watcher is released explicitly after the last chunk, with the defer as backstop.
	releaseWriteWatch := t.cancelSnapshotWriteOnStop(stream)
	defer releaseWriteWatch()

	for i := range chunks {
		// Abort between chunks as soon as shutdown begins; the watcher above
		// handles a write already in flight.
		select {
		case <-t.stopC:
			return ErrStreamClosed
		default:
		}
		// Copy the chunk by value so stamping DeploymentID does not mutate
		// the caller's backing array. SendSnapshotStreaming already operates
		// on a by-value copy via its emit callback.
		chunk := chunks[i]
		chunk.DeploymentID = t.cfg.DeploymentID
		chunk.BinVer = binVer
		chunkSz := chunk.Size()
		chunkBuf := make([]byte, chunkSz)
		chunkN, err := chunk.MarshalTo(chunkBuf)
		if err != nil {
			return err
		}
		if err := stream.SetWriteDeadline(time.Now().Add(snapshotChunkWriteDeadline)); err != nil {
			return err
		}
		if err := writeFrameAt(stream, chunkBuf[:chunkN], FlagNone, frameVer); err != nil {
			return &SnapshotChunkWriteTimeoutError{
				ShardID:   first.ShardID,
				ReplicaID: first.ReplicaID,
				ChunkID:   uint64(i),
				Err:       err,
			}
		}
	}

	// Every chunk is on the wire: disarm the shutdown watcher before any
	// success bookkeeping so a concurrent Stop cannot reset a completed
	// transfer while this call reports it as sent.
	releaseWriteWatch()

	// Calculate total snapshot bytes sent.
	var snapBytes int64
	for i := range chunks {
		snapBytes += int64(len(chunks[i].Data))
	}

	// Mark send as successful before firing completion events.
	sendOK = true
	t.reportSnapshotSent(snapInfo, uint64(len(chunks)), snapBytes, time.Since(snapStart))
	return nil
}

// closeSnapshotStream closes an outbound snapshot stream, logging a failure through LogContained.
func (t *QUICTransport) closeSnapshotStream(stream *quic.Stream, shardID, replicaID uint64) {
	if closeErr := stream.Close(); closeErr != nil {
		supervise.LogContained(func() {
			slog.Debug("snapshot stream close failed",
				"shard", shardID, "replica", replicaID, "error", closeErr)
		})
	}
}

// cancelSnapshotWriteOnStop resets the stream's write side if the transport stops mid-transfer; release it explicitly after the last chunk, and defer it as backstop.
func (t *QUICTransport) cancelSnapshotWriteOnStop(stream *quic.Stream) func() {
	done := make(chan struct{})
	var mu sync.Mutex
	released := false
	go func() {
		select {
		case <-t.stopC:
			mu.Lock()
			defer mu.Unlock()
			if !released {
				stream.CancelWrite(QUICStreamErrShutdown)
			}
		case <-done:
		}
	}()
	return func() {
		mu.Lock()
		alreadyReleased := released
		released = true
		mu.Unlock()
		if !alreadyReleased {
			close(done)
		}
	}
}

// SnapshotChunkProducer streams chunks to emit one at a time and stops on emit's first error, so at most one chunk is in memory.
type SnapshotChunkProducer func(emit func(proto.SnapshotChunk) error) error

// SendSnapshotStreaming sends snapshot chunks from produce on a dedicated QUIC stream, holding one chunk in memory at a time.
func (t *QUICTransport) SendSnapshotStreaming(meta SnapshotHeader, produce SnapshotChunkProducer) error {
	// Register the send; TryAdd returns false once Stop has begun.
	if !t.snapshotTracker.TryAdd() {
		return ErrStreamClosed
	}
	defer t.snapshotTracker.Done()

	snapStart := time.Now()

	target, err := t.reg.Resolve(meta.ShardID, meta.ReplicaID)
	if err != nil {
		return err
	}

	// A snapshot is the whole state machine: it goes only over a connection
	// to the replica it is for (see sendToTarget).
	want, bound := t.wantFor(meta.ShardID, meta.ReplicaID)
	if !bound {
		return &DialError{Target: target, Err: &DialIdentityError{}}
	}
	conn, err := t.getConnection(target, want)
	if err != nil {
		return err
	}

	// Open the stream on a context derived from shutdownCtx, so Stop unblocks it.
	openCtx, openCancel := context.WithTimeout(t.shutdownCtx, 60*time.Second)
	defer openCancel()

	stream, err := conn.OpenStreamSync(openCtx)
	if err != nil {
		return err
	}
	defer t.closeSnapshotStream(stream, meta.ShardID, meta.ReplicaID)

	// Write snapshot header first.
	if err := stream.SetWriteDeadline(time.Now().Add(snapshotChunkWriteDeadline)); err != nil {
		return err
	}
	var hdrBuf [SnapshotHeaderSize]byte
	if _, err := meta.MarshalTo(hdrBuf[:]); err != nil {
		return err
	}
	// One version for the whole stream; see SendSnapshot.
	binVer, frameVer := t.shardSendVersions(meta.ShardID)
	if err := writeFrameAt(stream, hdrBuf[:], FlagSnapshot, frameVer); err != nil {
		return err
	}

	// Fire OnSnapshotSendStarted after the header is written successfully.
	// The Aborted report is in place before Started runs, and every listener
	// call is isolated (see SendSnapshot).
	snapInfo := config.SnapshotInfo{
		ShardID:   meta.ShardID,
		ReplicaID: meta.ReplicaID,
		Index:     meta.Index,
		From:      meta.From,
	}
	sendOK := false
	defer func() {
		if !sendOK {
			deliverEvent(t.snapshotEvent(onSnapshotSendAborted, snapInfo))
		}
	}()
	deliverEvent(t.snapshotEvent(onSnapshotSendStarted, snapInfo))

	// Write chunks as the producer emits them; release the shutdown watcher explicitly once the producer finishes.
	releaseWriteWatch := t.cancelSnapshotWriteOnStop(stream)
	defer releaseWriteWatch()

	var chunkIdx uint64
	var snapBytes int64
	emitErr := produce(func(chunk proto.SnapshotChunk) error {
		// Abort between chunks as soon as shutdown begins; the watcher above
		// handles a write already in flight.
		select {
		case <-t.stopC:
			return ErrStreamClosed
		default:
		}
		chunk.DeploymentID = t.cfg.DeploymentID
		chunk.BinVer = binVer
		chunkSz := chunk.Size()
		chunkBuf := make([]byte, chunkSz)
		chunkN, marshalErr := chunk.MarshalTo(chunkBuf)
		if marshalErr != nil {
			return marshalErr
		}
		if deadlineErr := stream.SetWriteDeadline(time.Now().Add(snapshotChunkWriteDeadline)); deadlineErr != nil {
			return deadlineErr
		}
		if writeErr := writeFrameAt(stream, chunkBuf[:chunkN], FlagNone, frameVer); writeErr != nil {
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

	// Every chunk is on the wire: disarm the shutdown watcher before any
	// success bookkeeping (see SendSnapshot).
	releaseWriteWatch()

	// Mark send as successful before firing completion events.
	sendOK = true
	t.reportSnapshotSent(snapInfo, chunkIdx, snapBytes, time.Since(snapStart))
	return nil
}

// localAddr returns the address advertised to peers: AdvertiseAddress if set, else the socket's local address.
func (t *QUICTransport) localAddr() string {
	if t.cfg.AdvertiseAddress != "" {
		return t.cfg.AdvertiseAddress
	}
	if t.localAddrStr != "" {
		return t.localAddrStr
	}
	return t.cfg.ListenAddress
}

// getBuf returns a pooled buffer of at least minCap, putting a too-small one back before allocating.
func (t *QUICTransport) getBuf(minCap int) *[]byte {
	bufPtr := t.bufPool.Get().(*[]byte)
	if cap(*bufPtr) < minCap {
		t.putBuf(bufPtr)
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
