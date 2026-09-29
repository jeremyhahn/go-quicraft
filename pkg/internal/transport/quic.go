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
// with no messages before cleaning itself up. It only retires the idle
// GOROUTINE and channel; the next Send to this target lazily re-creates them.
//
// It does NOT idle-close the underlying QUIC connection. KeepAlivePeriod (10s)
// is below MaxIdleTimeout (30s), so quic-go PINGs the connection indefinitely
// and it never idle-closes on its own — a cached outbound to a peer that has
// gone quiet (including one removed from membership but still running) is kept
// alive by keep-alive traffic until it is torn down explicitly (a peer restart
// via evictStaleOutboundOnRecv, a dead-context sweep in cleanStaleConnections,
// a committed membership removal via CloseTarget, or transport Stop). The 60s
// value simply frees the idle worker promptly.
const sendQueueIdleTimeout = 60 * time.Second

// snapshotChunkWriteDeadline is the maximum time for a single chunk write
// on the snapshot send path. A chunk carries far more bytes than a Raft
// message -- the sender emits 2MiB chunks and the receiver accepts up to
// proto.MaxSnapshotChunkDataLen (8MB) -- so this deadline is much larger
// than sendWriteDeadline. If the receiver is slow or the network stalls,
// the deadline prevents indefinite blocking
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
//
// owners records, for each pooled stream, the *quic.Conn it was opened on.
// A stream's liveness is judged against its OWN connection, not whatever
// connection is currently cached for the target: in the peer-restart window a
// delayed pre-open can store a stream opened on a now-closed conn while a fresh
// conn is cached for the same target. Comparing only against the cached conn
// would return that dead stream, the write would fail, and the failure path
// would then tear down the HEALTHY cached conn. owners is kept in exact
// lockstep with streams under mu (put/drop mutate both). A key present in
// streams but absent from owners is treated as "owner unknown" and falls back
// to the currently-cached-conn liveness check (only manually-constructed test
// transports inject a raw stream without an owner).
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

// drop removes key from both the stream pool and the owner map. The caller must
// hold sh.mu. delete on a nil map is a no-op, so no owners guard is needed.
func (sh *streamShard) drop(key streamKey) {
	delete(sh.streams, key)
	delete(sh.owners, key)
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

	connMu sync.RWMutex
	conns  map[string]*quic.Conn
	// connPeer holds, for each pooled outbound connection, the node identity
	// its peer presented, so the identity pin (getConnection, connServes) does
	// not re-derive it — a TLS state copy and a hash — on every send. Maintained
	// under connMu with conns: set where a connection is pooled, deleted
	// wherever one is removed.
	connPeer map[*quic.Conn]string

	// connEpoch records, for each cached outbound in conns, the peer boot
	// epoch this node believed the target to be running when that outbound
	// was dialed (0 when the peer had never been heard from). It is written
	// and deleted under connMu at exactly the sites that write and delete
	// conns, so the two maps never diverge.
	//
	// It is what makes restart detection COMPLETE. peerEpoch alone only
	// records incarnations we have HEARD FROM, and an outbound can easily
	// exist for a target whose epoch we have never recorded (we dial before
	// the peer's first batch reaches us, and every teardown used to drop the
	// record while a later re-dial repopulated conns). In that state a
	// restart is indistinguishable from first contact, so the black-holed
	// outbound survived until MaxIdleTimeout -- 30s of one-way silence.
	// Comparing the RECEIVED epoch against the epoch the outbound was dialed
	// under decides the question without needing to remember history.
	connEpoch map[string]uint64

	dialGroup singleflight.Group

	// bootEpoch is this process's per-start identity, stamped into every
	// outgoing MessageBatch (SourceEpoch). It changes only when this node
	// restarts.
	bootEpoch uint64

	// peerEpoch tracks what is known about each peer address's process
	// incarnations. Its epoch field is the value a new outbound is stamped
	// with in connEpoch, so that a later batch can tell whether the cached
	// outbound belongs to the incarnation that sent it. Keying on the epoch
	// (not the inbound *quic.Conn identity) avoids the eviction cascade that
	// benign re-dials caused, because a re-dial from the same process carries
	// the SAME epoch.
	//
	// Entries are created only for addresses an authenticated peer certificate
	// covers, and reaped by reapPeerIncarnations once the address has neither a
	// cached outbound nor recent traffic, so the map cannot grow without bound
	// across membership churn.
	peerEpochMu sync.Mutex
	peerEpoch   map[string]*peerIncarnation

	streamShards [streamShardCount]streamShard

	// sendQueuesMu protects the sendQueues map. Reads (fast path in Send)
	// use RLock; writes (queue creation for new targets) use Lock.
	sendQueuesMu sync.RWMutex
	sendQueues   map[string]*sendQueue

	snapshotRecv *snapshotReceiver

	// msgRecvBudget is the remaining aggregate memory budget (bytes) for inbound
	// message-frame payload buffers across all connections/streams. A frame
	// reserves its declared length here BEFORE allocating its receive buffer and
	// releases it once dispatched, bounding total in-flight message memory to
	// MaxMessageReceiveMemory. Mirrors snapshotReceiver.memoryBudget for the
	// regular message path, which otherwise has no aggregate cap (OOM DoS).
	msgRecvBudget atomic.Int64

	stopC   chan struct{}
	stopped atomic.Bool

	// cleanupInterval is how often connectionCleanupOnce sweeps stale, revoked
	// and idle connections. It defaults to connectionCleanupInterval and is a
	// FIELD rather than a bare use of that constant so a test can drive the
	// sweep without waiting for a real tick.
	//
	// It is per-instance for the reason this codebase gives elsewhere (see
	// seal.Barrier.randReader): a mutable package global is reachable from
	// production code and from every other test in the binary, and a field is
	// reachable from neither.
	//
	// The unit test for the panic path used to wait a full 30-SECOND tick, and
	// its own comment said "we can't wait 30s" in a branch that is only reached
	// after doing exactly that. Thirty seconds of an otherwise fast package's
	// runtime bought nothing, and a slower suite is a wider window for the
	// load-induced timeouts tracked in #71.
	cleanupInterval time.Duration

	// stopOnce guards the teardown so exactly one caller runs it, and
	// stopDone is closed when that teardown has finished. Stop documents that
	// every goroutine has finished when it returns, so a second or concurrent
	// caller must block on stopDone rather than returning into a transport
	// that is still mid-teardown. stopDone is closed from a defer inside the
	// once: sync.Once treats a PANICKING f as done, and without the defer a
	// panicking teardown would strand every later caller forever.
	stopOnce sync.Once
	stopDone chan struct{}

	// acceptWG tracks the accept loop ALONE, separately from t.wg, so stop()
	// can join it at a point where t.wg cannot yet be joined.
	//
	// t.wg covers the accept loop AND every connection handler, and those
	// handlers only return once their connections are closed — so waiting on
	// t.wg before closing anything would deadlock. The accept loop has no such
	// dependency: the listener close and shutdownCancel are enough to end it.
	//
	// Joining it is what makes the inbound shutdown sweep COMPLETE rather than
	// best-effort. Listener.Close does not discard the connections already
	// queued inside quic-go (baseServer.accept drains connQueue after
	// stopAccepting is closed), so the loop can still admit one after the
	// shutdown has begun. Because acceptLoopIteration publishes into
	// inboundConns synchronously, "the accept loop has returned" means "every
	// connection it admitted is in the map", and a sweep after this join can no
	// longer miss one and let quicTr.Close() destroy it unannounced.
	acceptWG sync.WaitGroup

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

	// inboundConns tracks live INBOUND connections. Inbound connections were
	// held only as locals
	// inside handleConnection, so nothing could reach them after admission —
	// and revocation runs only in VerifyConnection, i.e. only at handshake.
	// A peer whose certificate was revoked, or whose replica the shard
	// committed a removal for, therefore kept its session for the lifetime of
	// the process: quic-go's keep-alive (10s) sits below MaxIdleTimeout (30s),
	// so the connection never idle-closes, and CloseTarget only reaches the
	// OUTBOUND half. Revocation stopped a compromised node reconnecting and
	// nothing else.
	//
	// Tracking is only half of it: the RECLAIM POLICIES are what act on this
	// map, and there are three, none of which owns a goroutine.
	// revokeCheckConnections and reapIdleInboundConns run on the existing 30s
	// cleanup tick — revocation re-checks, and an idle bound that is the only
	// thing able to reclaim a connection whose peer completes a handshake and
	// then says nothing. closeInboundFromPeer is event-driven from CloseTarget,
	// so a committed membership removal reaches the half the removed member
	// controls.
	//
	// Entries are added once the handshake has completed, keyed by *quic.Conn
	// with an *inboundConnState value, and removed by forgetInboundConnState,
	// which runs after every stream goroutine on the connection has finished.
	inboundConns sync.Map

	// connMsgBudget holds the per-PEER message-receive reservation, keyed by
	// the peer's authenticated identity (msgBudgetKey) with value
	// *peerMsgBudget. The global msgRecvBudget alone lets one peer occupy the
	// entire budget and force every other peer's frames into the exhaustion
	// path; this sub-budget caps any single peer at maxConnMsgRecv.
	//
	// It was keyed by *quic.Conn, which did not achieve that: MaxConnectionsPerIP
	// is 16, so one peer opened four connections, saturated four separate
	// sub-budgets of MaxMessageReceiveMemory/4 each, and drove the global
	// budget to zero — after which every other peer's frames were silently
	// dropped. Reproduced with 16 streams and 128 bytes of traffic: the frame
	// header alone reserves its declared length before the body is read, so
	// the attacker never has to send one.
	//
	// Keying on identity rather than the connection is what makes the cap mean
	// what its name says. The identity is already derived on this path for
	// authorization, so this costs one map lookup, not a new mechanism.
	// Entries are refcounted by live connection and removed when the last one
	// closes.
	connMsgBudget sync.Map

	// maxConnMsgRecv is the per-inbound-connection ceiling on in-flight
	// message-frame memory, derived from MaxMessageReceiveMemory.
	maxConnMsgRecv int64

	// lastProtocolViolationLog is the Unix-nanosecond timestamp of the most
	// recent protocol-violation warning this transport emitted. Per-instance,
	// not package-global: a shared limiter lets one transport's log flood
	// suppress every other transport's first diagnostic, and in-process
	// multi-node deployments (and the test suite) run many transports at once.
	lastProtocolViolationLog atomic.Int64

	// Rate-limit stamps for the remaining peer-driven diagnostics on the
	// inbound hot path. Every one of these lines is triggerable at the frame
	// rate by one authenticated peer, and slog's default handler writes
	// synchronously to stderr under a single mutex — so an unsampled line here
	// puts a write syscall on the raft receive path AND serialises every other
	// log call in the process behind it. The counters beside them are
	// unconditional, so the sampled line can carry the true total.
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

	// peerFrameRejections counts every frame a peer's decoder refused
	// (QUICStreamErrFrameRejected). The warning below it is rate-limited, and
	// the receiver's own diagnostic is rate-limited to 10s, so a storm of
	// rejections — the signature of a producer emitting a batch no peer can
	// accept — is otherwise nearly invisible in logs. The counter is
	// incremented on every rejection, never sampled.
	peerFrameRejections atomic.Uint64

	// lastPeerRejectionLog is the Unix-nanosecond timestamp of the most recent
	// peer-rejection warning, per transport instance (see
	// lastProtocolViolationLog for why it is not package-global).
	lastPeerRejectionLog atomic.Int64

	// lastIdentityMismatchLog rate-limits dropIdentityMismatch's warning.
	lastIdentityMismatchLog atomic.Int64

	// dialRefusals holds, per target, the last dial refused by the identity
	// pin (dialRefusal), so it is not re-dialed within dialRefusalCooldown.
	dialRefusals sync.Map
}

// srcAddrAuthSlots is the number of distinct claimed SourceAddresses cached
// per inbound connection. A legitimate peer advertises exactly one, but a
// single slot is defeated by a peer alternating two claimed addresses, which
// forces a full ConnectionState()+VerifyHostname on every batch while holding
// the entry mutex and so serializes that connection's receive path.
const srcAddrAuthSlots = 4

// srcAddrAuthMaxDistinct bounds how many distinct claimed SourceAddresses one
// connection may have verified against its certificate. Past this bound the
// negative result is assumed without re-verifying: a peer cycling addresses is
// abusive, and failing closed only clears the (unauthenticated) SourceAddress.
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
	// passed to buildMTLSTLS to wire VerifyConnection (not
	// VerifyPeerCertificate, which resumed sessions skip) on both
	// server and client TLS configs. The WHOLE CA bundle is parsed and
	// handed to the checker, which selects the actual issuer per leaf and
	// per CRL: TLS verifies against every block in the bundle
	// (AppendCertsFromPEM), so wiring revocation to only the first block
	// would build OCSP requests naming the wrong issuer and reject every
	// intermediate-signed CRL. Parse errors are fatal: a missing issuer
	// would silently disable OCSP checks at runtime.
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
		MaxIdleTimeout: 30 * time.Second,
		// The handshake idle limit follows the dial budget: quic-go abandons
		// a handshake that idles this long and caps the whole handshake at
		// twice it, so a dial context wider than this could never be
		// reached. Production's DialTimeout is the 5s this always was; the
		// test fixture's wider budget widens both together (#196).
		HandshakeIdleTimeout:       cfg.DialTimeout,
		MaxIncomingStreams:         int64(maxIncomingStreams),
		MaxIncomingUniStreams:      -1,
		KeepAlivePeriod:            10 * time.Second,
		InitialStreamReceiveWindow: initialStreamReceiveWindow,
		MaxStreamReceiveWindow:     maxStreamReceiveWindow,
		MaxConnectionReceiveWindow: connectionReceiveWindow(cfg.MaxMessageReceiveMemory, cfg.MaxIncomingConnections),
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
		cfg:             cfg,
		handler:         handler,
		reg:             reg,
		tlsPair:         pair,
		quicCfg:         qcfg,
		conns:           make(map[string]*quic.Conn),
		connPeer:        make(map[*quic.Conn]string),
		connEpoch:       make(map[string]uint64),
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
	t.snapshotRecv.SetSnapshotTracker(&t.snapshotTracker)
	t.msgRecvBudget.Store(cfg.MaxMessageReceiveMemory)
	// Cap any single connection at a quarter of the aggregate budget, but
	// never below one maximum-size frame plus its decompression delta, so a
	// legitimate peer's largest legal frame always fits.
	t.maxConnMsgRecv = cfg.MaxMessageReceiveMemory / 4
	if minPerConn := int64(cfg.MaxDecompressedSize) + int64(MaxFrameSize); t.maxConnMsgRecv < minPerConn {
		t.maxConnMsgRecv = minPerConn
	}

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

	// Now that the advertised address is resolved, say so if it leaves the
	// promiscuous-certificate refusal unable to ask its question.
	t.reportPromiscuousCertCoverage()

	// Start the revocation checker's CRL reload goroutine. This must
	// happen after the listener is created but before accepting
	// connections so that the initial CRL load is complete.
	if t.revocationChecker != nil {
		t.revocationChecker.Start()
	}

	t.wg.Add(1)
	t.acceptWG.Add(1)
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

// SetShardEpochTrackers configures a per-shard epoch tracker resolver on the
// snapshot receiver, used when each Raft shard is sealed under its OWN barrier
// (per-tenant WAL/snapshot encryption). When set, a received snapshot pins its
// in-flight epoch on the barrier mapped to the snapshot's shardID instead of a
// single host-wide tracker. This is additive: SetEpochTracker continues to work
// for the single-barrier case, and a non-nil per-shard resolver takes
// precedence. Must be called before Start.
func (t *QUICTransport) SetShardEpochTrackers(fn func(shardID uint64) EpochTracker) {
	t.snapshotRecv.SetShardEpochTrackers(fn)
}

// Stop gracefully shuts down the transport. It signals all per-target
// sender goroutines to exit via stopC, waits for them to drain, stops
// the accept loop, waits for in-flight snapshot transfers, closes all
// connections — BOTH the ones this transport dialed and the ones it accepted,
// so the shutdown is announced to every peer rather than only to half of
// them — and waits for all goroutines to finish.
//
// Stop is idempotent and safe to call concurrently: exactly one caller runs
// the teardown and every other caller BLOCKS until it has completed. Returning
// early on the second call would contradict the guarantee above — the caller
// would be told every goroutine had finished while the first Stop was still
// closing connections and draining the WaitGroup.
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

	// Acquire sendQueuesMu to synchronize with any in-progress
	// getOrCreateSendQueue call. This creates a happens-before edge: any
	// wg.Add(1) that was in progress (inside the write lock with
	// stopped=false) must complete before we proceed. After this point,
	// all future getOrCreateSendQueue calls see stopped=true and return
	// nil without calling wg.Add(1). The read of t.stopped under the lock
	// is the observable work that makes the barrier explicit (and not an
	// empty critical section).
	t.sendQueuesMu.Lock()
	_ = t.stopped.Load()
	t.sendQueuesMu.Unlock()

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

	// Wait for the accept loop to return before touching any connection map.
	// The listener close alone does not stop admissions: quic-go's
	// baseServer.accept drains its connection queue after stopAccepting is
	// closed, so a connection can still be handed to the loop here. Joining the
	// loop is what makes the inbound sweep below COMPLETE — after this point no
	// further connection can be admitted, and every one that was is already
	// published in t.inboundConns (acceptLoopIteration publishes synchronously,
	// before launching the handler). Without this join the sweep could miss a
	// just-admitted connection, quicTr.Close() would destroy it locally, and
	// the peer would be left holding it unannounced.
	//
	// This is not t.wg: that also covers the connection handlers, which cannot
	// return until their connections are closed, so joining it here would
	// deadlock. The accept loop has no such dependency — the listener close and
	// shutdownCancel above are enough to end it.
	t.acceptWG.Wait()

	t.connMu.Lock()
	for addr, conn := range t.conns {
		if err := conn.CloseWithError(QUICErrShutdown, "transport shutdown"); err != nil {
			slog.Debug("QUIC connection close failed", "addr", addr, "error", err)
		}
		delete(t.connPeer, conn)
		delete(t.conns, addr)
		delete(t.connEpoch, addr)
	}
	t.connMu.Unlock()

	// Announce the shutdown on the INBOUND half too. Closing only t.conns
	// tells the peers this node DIALED that it is going away and leaves every
	// peer that dialed US holding a connection it still believes is live:
	// quic-go's Transport.Close below destroys those connections locally
	// without emitting a CONNECTION_CLOSE, so the peer learns nothing until
	// MaxIdleTimeout expires or one of its own writes fails. A leader keeps
	// enqueuing to a dead follower for that whole window, and its
	// cleanStaleConnections cannot evict a connection it never sees as stale.
	//
	// A crash cannot be announced; a planned shutdown can, and this is the one
	// frame that says so. Ordering is load-bearing in BOTH directions — it must
	// run after the listener close (so the set of connections to chase has
	// stopped growing from the listener's side) and before t.quicTr.Close(),
	// which destroys the connections locally and tears the socket down, making
	// any close issued after it a silent no-op.
	//
	// It is complete rather than best-effort because of the acceptWG join
	// above: no connection can be admitted after it, and every one that was is
	// already in the map.
	if n := t.closeInboundOnShutdown(); n > 0 {
		slog.Debug("announced shutdown on inbound connections", "connections", n)
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
				slog.Debug("QUIC stream close failed", "target", k.target, "idx", k.idx, "error", err)
			}
			shard.drop(k)
		}
		shard.mu.Unlock()
	}

	if t.quicTr != nil {
		if err := t.quicTr.Close(); err != nil {
			slog.Debug("QUIC transport close failed", "error", err)
		}
	}

	t.wg.Wait()
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
// stream write with the 2s sendWriteDeadline), preventing heartbeat
// starvation and cascading multi-node stalls.
//
// Each message is deep-copied (entries and their Cmd payloads, and the
// snapshot metadata's membership maps and checksum) before enqueueing into
// the per-target channel. The caller may reuse or modify the msgs slice and
// its contents after Send returns.
func (t *QUICTransport) Send(msgs []proto.Message) {
	if t.stopped.Load() {
		return
	}
	if len(msgs) == 0 {
		return
	}

	// Hoisted once per Send: the drop sites below fire on every message to an
	// unresolved target during membership churn, and the variadic slog form
	// builds and boxes its []any at the call site regardless of whether Debug
	// is enabled.
	debugOn := debugEnabled()

	for i := range msgs {
		msg := msgs[i] // copy the message value (shallow)
		// Injected network fault (integration builds only). Placed before the
		// deep copies below so a dropped message costs nothing, and modelled as
		// a silent drop: the connection stays up and no error is reported,
		// exactly as a partitioned network behaves. In a normal build
		// netFaultEnabled is a false constant and this whole branch is
		// compiled away.
		if netFaultEnabled && netFaultDrop(msg.ShardID, msg.From, msg.To) {
			continue
		}
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
		// Snapshot is copied by VALUE with the message, but proto.Snapshot
		// embeds Membership's four maps and a Checksum slice, all still
		// aliasing the caller's structures. Membership.MarshalTo ranges over
		// those maps on the async send path, so an in-place mutation by the
		// caller would be an unrecoverable "concurrent map iteration and map
		// write" fatal error that the worker's recover cannot catch. Deep-copy
		// them so the documented reuse contract actually holds.
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

// debugEnabled reports whether the default logger would emit a Debug record.
// The variadic slog.Debug form builds its []any and boxes every value at the
// call site before the handler's Enabled check runs, so high-frequency drop
// paths must gate on this instead of calling slog.Debug unconditionally.
func debugEnabled() bool {
	return slog.Default().Enabled(context.Background(), slog.LevelDebug)
}

// copySnapshotRefs replaces every reference-typed field reachable from a
// snapshot's metadata with an owned copy, so the transport's asynchronous
// marshal never races a caller mutating the original.
// Every map on the membership is covered, including the two authorization maps:
// Identities was missed when it was added, and a snapshot whose identity map is
// mutated between the enqueue and the marshal would ship a configuration that
// authorizes a different set of replicas than the one the caller submitted.
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

// retireSendQueue removes the per-target send queue for target, following the
// worker's own prune protocol exactly: mark the queue closed and delete the map
// entry under sendQueuesMu, so a producer that already holds this *sendQueue
// observes closed=true after its channel send and re-enqueues into a fresh
// queue instead of orphaning the message.
//
// The worker goroutine is NOT stopped here — its channels may still hold
// messages, and closing a channel a producer may be sending on would panic. It
// drains what is buffered, finds the queue idle, and retires itself; the
// identity check on its own prune keeps it from deleting a queue created after
// this one was retired.
func (t *QUICTransport) retireSendQueue(target string) {
	t.sendQueuesMu.Lock()
	if sq, ok := t.sendQueues[target]; ok {
		sq.closed.Store(true)
		delete(t.sendQueues, target)
	}
	t.sendQueuesMu.Unlock()
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
			//
			// Delete only while the map still points at OUR queue: an explicit
			// retirement (retireSendQueue, on a committed member removal) may
			// already have removed this entry and a later Send created a fresh
			// queue and worker at the same key. Deleting unconditionally would
			// evict that live queue from the map without marking it closed,
			// leaving two workers draining two channels for one target.
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

// sendToTarget dispatches a batch of messages to a single target, routing
// heartbeat-only batches through the fast path. Extracted from Send() to
// share logic between the single-target and parallel multi-target paths.
//
// Every message is pinned to its destination: it is written only over a
// connection whose peer presents the identity membership binds to the replica
// it is for (Config.ReplicaIdentity). Connections are keyed by address and
// shared by every replica routed there, and address learning lets a member of
// any shard move its own route onto another replica's address, so the batch
// is split by destination identity — nearly always a single group, since every
// replica a node hosts presents that node's identity — and each group carries
// its identity down to the connection it takes (getStream, getConnection),
// through any re-dial or retry on the way.
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

// sendBatch delivers a batch of messages to target, splitting it into as many
// frames as it takes. A batch is a coalescing artifact of the send queue, not a
// unit the peer sees, so it must never be all-or-nothing:
//
//   - Messages whose combined marshaled size exceeds MaxFrameSize are split
//     across consecutive frames on the same (ordered) stream, instead of being
//     dropped together on a MarshalFrame failure.
//   - A message no peer can accept — more entries than
//     proto.MaxEntriesPerMessage, or bigger on its own than a frame — is
//     dropped here, BEFORE it is written. Writing it makes the peer's decoder
//     reject the frame and reset the stream, which the sender can only read as
//     an I/O failure; the batch is then retried unchanged and the loop is
//     unbounded. Dropping it also stops one unsendable message from taking the
//     rest of its batch (heartbeats included) down with it.
//
// want is the identity every message's destination is bound to (see
// sendToTarget); each frame is written only over a connection serving it.
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

// dropUnsendable records and reports a message the transport refuses to write.
// It is logged at Error because it is a producer defect — raft must bound both
// the byte size (Config.MaxApplyEntrySize) and the entry count
// (proto.MaxEntriesPerMessage) of every message it builds — and because the
// data in it is never delivered: the peer waiting on those entries stalls
// until the producer stops emitting the same message.
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

// sendFrame marshals msgs as one MessageBatch and writes it as a single framed
// payload to the appropriate stream for the target. payloadSize is the size
// sendBatch already measured the group at, which it has verified fits a frame;
// a stale value can only under-size the buffer, which MarshalTo reports as
// ErrBufferTooSmall on the drop path below. Errors are logged and dropped.
func (t *QUICTransport) sendFrame(target, want string, msgs []proto.Message, payloadSize int) { //nolint:gocognit // metrics instrumentation adds branches
	debugOn := debugEnabled()
	var sendStart time.Time
	if t.metrics != nil || debugOn {
		sendStart = time.Now()
	}
	mb := proto.MessageBatch{
		BinVer:        proto.WireVersion,
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
		slog.Error("transport: dropping batch that failed to marshal",
			"target", target,
			"batch_size", len(msgs),
			"error", err,
		)
		t.incBatchDropped(msgs, dropReasonMarshalFailed)
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

	// Ensure the buffer is large enough for the compressed frame. The pooled
	// buffer must NOT be returned before MarshalFrame runs: payload may alias
	// it (uncompressed sends marshal straight into it), so pooling it first
	// would let a concurrent Get hand that backing array to another sender
	// while it is still being copied from.
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

	_, err = MarshalFrame(buf, payload, flags)
	if grewBuf {
		t.putBuf(oldBufPtr)
	}

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
		// Apply the standard pool cap: an encode buffer sized for a large
		// payload must not be retained per P as unaccounted memory.
		if cap(*snappyBufPtr) <= maxPoolBufSize {
			t.snappyEncPool.Put(snappyBufPtr)
		}
		snappyBufPtr = nil //nolint:ineffassign,wastedassign // defensive nil to prevent use-after-pool-return
	}
	snappyBuf = nil //nolint:wastedassign // defensive nil to prevent use-after-pool-return

	if err != nil {
		// Unreachable in normal operation: sendBatch sizes every group against
		// MaxFrameSize before this point, so a failure here means the marshaled
		// size disagreed with MessageBatch.Size() — an internal sizing defect.
		// Dropping (rather than writing an unacceptable frame) is still the
		// correct response: retrying it unchanged can never succeed, and
		// emitting it would cost the connection.
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
		t.putBuf(bufPtr)
		return
	}

	frameSize := totalSize

	stream, err := t.getStream(target, shardID, isHeartbeat, want)
	if err != nil {
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
		t.putBuf(bufPtr)
		return
	}

	err = stream.SetWriteDeadline(time.Now().Add(sendWriteDeadline))
	if err != nil {
		t.incBatchDropped(msgs, dropReasonWriteFailed)
		t.putBuf(bufPtr)
		t.removeAndCloseConnection(target, "write deadline failure")
		t.evictStreamsForTarget(target)
		return
	}

	_, err = stream.Write(buf[:frameSize])
	t.putBuf(bufPtr)
	if err != nil {
		// The peer's decoder rejected a frame on this stream and reset it.
		// That is a statement about the FRAME, not about the connection — the
		// connection is provably alive, since the peer had to receive and parse
		// a frame on it to reject one. Treating it as a dead connection is what
		// turns a single unacceptable batch into a dial storm: close, re-dial,
		// re-send the same batch, repeat. Drop the batch and replace only the
		// stream; the connection, and every other stream on it, survives.
		if isPeerFrameRejection(err) {
			t.incBatchDropped(msgs, dropReasonPeerRejected)
			t.recordPeerFrameRejection(target, shardID, len(msgs), err)
			t.evictStream(target, shardID, isHeartbeat, stream)
			return
		}
		// Stream write failed — likely a dead connection. Close and evict
		// the connection and all its streams so the next send attempt dials
		// fresh. Without this, other cached streams on the same dead
		// connection would each hang for the full write deadline before
		// failing, compounding latency on every message type.
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

// writeSnapshotHeader writes the framed SnapshotHeader that opens a snapshot
// stream, which is what tells the receiver this stream carries a snapshot
// rather than a message batch (FlagSnapshot).
//
// Extracted from SendSnapshot so that function stays under the cyclomatic
// limit: it sits right at the threshold, and the three error branches here
// were enough to push it over when the network-fault guard was added.
//
// The write deadline is set here rather than by the caller because a header
// write to a stalled receiver would otherwise block indefinitely, holding a
// snapshotTracker slot and with it Stop().
func writeSnapshotHeader(stream *quic.Stream, first *proto.SnapshotChunk) error {
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
	return WriteFrame(stream, hdrBuf[:], FlagSnapshot)
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
	// A partition that let snapshots through would not be a partition: a
	// follower cut off from its leader must fall behind, which is the whole
	// point of the catch-up tests. Reported as a stream error rather than a
	// silent success so the sender retries, which is what an interrupted
	// transfer looks like. Compiled away in a normal build.
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

	if err := writeSnapshotHeader(stream, first); err != nil {
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
	// Arm the shutdown watcher so a chunk write blocked on a stalled receiver
	// cannot hold Stop() for the full 30s per-chunk deadline. It is disarmed
	// explicitly once the last chunk is written (see below) so a concurrent
	// Stop cannot reset a stream that already carried the whole snapshot; the
	// defer is the backstop for the error returns inside the loop.
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

// cancelSnapshotWriteOnStop arms a watcher that resets the snapshot stream's
// write side when the transport begins shutting down mid-transfer. Each chunk
// write may block for up to snapshotChunkWriteDeadline (30s); without the
// watcher, Stop()'s ShutdownAndWait stalls behind every in-flight chunk write.
// CancelWrite unblocks a blocked Write immediately, failing the transfer with
// a stream error that the per-chunk error handling already surfaces. The
// returned release func must be called (deferred) when the transfer finishes.
//
// The release func is idempotent and disarms the watcher under a mutex, which
// closes the success race: without it, a close(stopC) landing between the last
// successful chunk write and the release call lets the watcher RESET a stream
// that already carried the whole snapshot, discarding the peer's
// buffered-but-unread data while the sender reports success (OnSnapshotSent,
// IncSnapshotsSent). Callers therefore release explicitly once the final chunk
// is written, before any success bookkeeping, and defer the same func as a
// backstop for the error paths.
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
	// Arm the shutdown watcher so a chunk write blocked on a stalled receiver
	// cannot hold Stop() for the full 30s per-chunk deadline. It is disarmed
	// explicitly once the producer finishes so a concurrent Stop cannot reset
	// a stream that already carried the whole snapshot.
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

	// Every chunk is on the wire: disarm the shutdown watcher before any
	// success bookkeeping (see SendSnapshot).
	releaseWriteWatch()

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

// getBuf retrieves a buffer from the pool, ensuring it has at least minCap
// capacity. When the pooled buffer is too small it is returned to the pool
// before the replacement is allocated (as getRecvBuf does); dropping it would
// discard a live pooled buffer on every oversized send and steadily drain the
// pool.
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
