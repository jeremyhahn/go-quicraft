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

// Package transport provides QUIC-based transport for Raft message passing.
package transport

import (
	"crypto/tls"
	"errors"
	"time"

	"github.com/jeremyhahn/go-quicraft/pkg/config"
	"github.com/jeremyhahn/go-quicraft/pkg/internal/transport/revocation"
	"github.com/jeremyhahn/go-quicraft/pkg/proto"
)

// config.DefaultMaxDecompressedSize is the public name for MaxFrameSize: the
// public configuration validates MaxDecompressedSize against it and sizes the
// default message-receive budget from it. config cannot import this package,
// so these assertions fail the build if the two ever drift, in either
// direction.
const (
	_ = uint64(MaxFrameSize) - uint64(config.DefaultMaxDecompressedSize)
	_ = uint64(config.DefaultMaxDecompressedSize) - uint64(MaxFrameSize)
)

// Metrics is the metrics interface consumed by the transport for
// message and snapshot measurements. When nil, all metric collection is
// skipped (zero overhead). The concrete implementation lives in the public
// quicraft package to avoid an import cycle.
type Metrics interface {
	ObserveSendLatency(d time.Duration)
	IncMessagesSent(shardID, to uint64, msgType uint64)
	IncMessagesReceived(shardID, from uint64, msgType uint64)
	IncSnapshotsSent(shardID uint64, bytes int64, d time.Duration)
	IncSnapshotsReceived(shardID uint64, bytes int64, d time.Duration)
	IncSnapshotsRejected(shardID uint64, reason string)
	IncMessagesDropped(shardID, to uint64, reason string)
}

// snapshotRejectReason is the bucket recorded as the reason label of
// IncSnapshotsRejected. The set is closed: every value is a package constant
// below, because the label feeds Prometheus and a per-transfer string would
// create unbounded child series. The free-form detail that accompanies a
// rejection is logged only.
type snapshotRejectReason string

// Snapshot rejection reasons.
const (
	// snapshotRejectShutdown covers transfers refused because the transport
	// is stopping.
	snapshotRejectShutdown snapshotRejectReason = "shutdown"

	// snapshotRejectConcurrency covers the host-global and per-peer
	// concurrent-receive limits.
	snapshotRejectConcurrency snapshotRejectReason = "concurrency_limit"

	// snapshotRejectMemory covers the aggregate and per-receive snapshot
	// memory budgets.
	snapshotRejectMemory snapshotRejectReason = "memory_budget"

	// snapshotRejectLiveness covers the whole-transfer duration ceiling, the
	// throughput floor, and the zero-length chunk flood — a peer occupying a
	// concurrency slot without making progress.
	snapshotRejectLiveness snapshotRejectReason = "liveness"

	// snapshotRejectMalformed covers header and chunk records that fail
	// bounds, unmarshal, or validation checks.
	snapshotRejectMalformed snapshotRejectReason = "malformed"

	// snapshotRejectIO covers stream read failures during the header or
	// chunk stages.
	snapshotRejectIO snapshotRejectReason = "io_error"

	// snapshotRejectPolicy covers transfers refused by a security policy
	// rather than by a defect, currently only arrival over 0-RTT.
	snapshotRejectPolicy snapshotRejectReason = "policy"

	// snapshotRejectUnauthorized covers transfers the handler refused from
	// the stream header (MessageHandler.AuthorizeSnapshot), before any receive
	// slot or memory was taken.
	snapshotRejectUnauthorized snapshotRejectReason = "unauthorized"

	// snapshotRejectNotAccepting covers transfers AuthorizeSnapshot refused
	// because the shard is failed or stopping (ErrSnapshotNotAccepting): an
	// ordinary state, not an authorization failure.
	snapshotRejectNotAccepting snapshotRejectReason = "not_accepting"
)

// Message-drop reasons recorded as the reason label of IncMessagesDropped.
// Closed set, for the same cardinality reason as snapshotRejectReason.
const (
	// dropReasonUnresolvedTarget is a message whose target replica has no
	// registered address (membership churn).
	dropReasonUnresolvedTarget = "unresolved_target"

	// dropReasonQueueFull is a message shed by a full per-target send or
	// heartbeat queue.
	dropReasonQueueFull = "queue_full"

	// dropReasonStreamUnavailable is a batch dropped because no stream to
	// the target could be obtained.
	dropReasonStreamUnavailable = "stream_unavailable"

	// dropReasonWriteFailed is a batch dropped on a stream write or write
	// deadline failure.
	dropReasonWriteFailed = "write_failed"

	// dropReasonMarshalFailed is a batch dropped because it could not be
	// serialized (an internal sizing defect or an oversized replication
	// payload).
	dropReasonMarshalFailed = "marshal_failed"

	// dropReasonProtocolViolation is an inbound frame discarded before its
	// messages could be parsed, so neither shard nor target is known.
	dropReasonProtocolViolation = "protocol_violation"

	// dropReasonUnsendable is a message no peer can accept — more entries
	// than proto.MaxEntriesPerMessage, or larger on its own than a frame —
	// refused by the send path before it is written. Retrying it unchanged
	// can never succeed, so it is dropped rather than emitted.
	dropReasonUnsendable = "unsendable"

	// dropReasonPeerRejected is a batch the peer's decoder refused, signalled
	// by a stream reset carrying QUICStreamErrFrameRejected. The connection is
	// healthy; only the frame was unacceptable.
	dropReasonPeerRejected = "peer_rejected"

	// dropReasonIdentityMismatch is a message whose destination replica is
	// bound to a node identity other than the one the peer connected at its
	// routed address presents (Config.ReplicaIdentity).
	dropReasonIdentityMismatch = "identity_mismatch"
)

// Default configuration values for QUICTransport.
const (
	defaultStreamPoolSize          = 16
	defaultMaxStreamPoolSize       = 256
	defaultMaxSnapshotReceiveRate  = 256 * 1024 * 1024 // 256 MB/s
	defaultMaxConcurrentSnapRecv   = 4
	defaultMaxSnapshotRecvMemory   = 1024 * 1024 * 1024 // 1 GB
	defaultSendBatchMaxSize        = 64 * 1024          // 64 KB
	defaultMaxIncomingConnections  = 256
	defaultMaxConnectionsPerIP     = 16
	defaultMaxSnapshotChunkCount   = 1 << 20 // 1M chunks
	defaultMaxStreamsPerConnection = 256
	defaultUDPRecvBufSize          = 7 * 1024 * 1024 // 7 MB
	defaultUDPSendBufSize          = 7 * 1024 * 1024 // 7 MB

	// defaultStreamReadIdleTimeout bounds how long an inbound stream read
	// waits at a clean frame boundary before re-checking the connection's
	// liveness. It is NOT a stream lifetime: an idle stream on a live
	// connection is kept open across re-checks (replication streams sit idle
	// between bursts while heartbeats keep the connection alive). Only a dead
	// connection or a half-delivered frame stall ends a stream.
	defaultStreamReadIdleTimeout = 30 * time.Second

	// frameReadActiveTimeout bounds completing a frame once it has begun
	// arriving. A frame that starts but does not finish within this window is
	// a genuine stall (dead/slow peer), so the stream is dropped.
	frameReadActiveTimeout = 30 * time.Second

	// QUIC flow-control window sizes. These bound memory quic-go buffers
	// BEFORE the application reads a frame, which is separate from (and
	// additional to) MaxMessageReceiveMemory.
	//
	// initialStreamReceiveWindow is the starting per-stream window;
	// maxStreamReceiveWindow is the ceiling autotuning may raise it to.
	initialStreamReceiveWindow = 2 << 20 // 2 MB
	maxStreamReceiveWindow     = 8 << 20 // 8 MB

	// maxConnectionReceiveWindowCeiling caps the per-connection window that
	// connectionReceiveWindow derives, so a large MaxMessageReceiveMemory does
	// not translate into an unbounded per-connection buffer.
	maxConnectionReceiveWindowCeiling = 32 << 20 // 32 MB

	// snapshotStreamHeadroom is the number of streams reserved on top of a
	// peer's outbound ordered-stream pool (StreamPoolSize) when sizing the
	// QUIC incoming-stream limit, so a peer's dedicated snapshot/ephemeral
	// streams never block on flow control behind a saturated pool.
	snapshotStreamHeadroom = 4
)

// Configuration validation errors. Each is a distinct typed error to allow
// precise matching with errors.Is().
var (
	// ErrEmptyListenAddress is returned when ListenAddress is empty.
	ErrEmptyListenAddress = errors.New("transport: listen address is required")

	// ErrInvalidStreamPoolSize is returned when StreamPoolSize is not
	// between 1 and MaxStreamPoolSize.
	ErrInvalidStreamPoolSize = errors.New("transport: stream pool size out of range")

	// ErrInvalidMaxStreamPoolSize is returned when MaxStreamPoolSize < 1.
	ErrInvalidMaxStreamPoolSize = errors.New("transport: max stream pool size must be >= 1")

	// ErrInvalidMaxSnapshotReceiveRate is returned when MaxSnapshotReceiveRate <= 0.
	ErrInvalidMaxSnapshotReceiveRate = errors.New("transport: max snapshot receive rate must be > 0")

	// ErrInvalidDialTimeout is returned when DialTimeout is negative after
	// defaulting: a transport whose every dial context is born expired.
	ErrInvalidDialTimeout = errors.New("transport: dial timeout must be > 0")

	// ErrInvalidMaxConcurrentSnapshotRecv is returned when MaxConcurrentSnapshotRecv <= 0.
	ErrInvalidMaxConcurrentSnapshotRecv = errors.New("transport: max concurrent snapshot recv must be > 0")

	// ErrInvalidMaxSnapshotReceiveMemory is returned when MaxSnapshotReceiveMemory <= 0.
	ErrInvalidMaxSnapshotReceiveMemory = errors.New("transport: max snapshot receive memory must be > 0")

	// ErrInvalidMaxMessageReceiveMemory is returned when MaxMessageReceiveMemory <= 0.
	ErrInvalidMaxMessageReceiveMemory = errors.New("transport: max message receive memory must be > 0")

	// ErrInvalidSendBatchMaxSize is returned when SendBatchMaxSize <= 0.
	ErrInvalidSendBatchMaxSize = errors.New("transport: send batch max size must be > 0")

	// ErrInvalidMaxDecompressedSize is returned when MaxDecompressedSize is
	// anything other than MaxFrameSize. It is the receiving half of the wire
	// contract, not a local tunable — see the field's documentation.
	ErrInvalidMaxDecompressedSize = errors.New("transport: max decompressed size must equal MaxFrameSize")

	// ErrIncompleteMTLSConfig is returned when MTLSConfig is partially set.
	ErrIncompleteMTLSConfig = errors.New("transport: mTLS config requires CACert, Cert, and Key")

	// ErrInvalidMaxIncomingConnections is returned when MaxIncomingConnections < 1.
	ErrInvalidMaxIncomingConnections = errors.New("transport: max incoming connections must be >= 1")

	// ErrInvalidMaxConnectionsPerIP is returned when MaxConnectionsPerIP < 1.
	ErrInvalidMaxConnectionsPerIP = errors.New("transport: max connections per IP must be >= 1")
)

// MTLSConfig holds the certificate material for mutual TLS authentication.
// CACert is always required. The node leaf comes from either the PEM Cert/Key
// pair or a pre-built Certificate (whichever is provided).
type MTLSConfig struct {
	// CACert is the PEM-encoded CA certificate used to verify peer certificates.
	CACert []byte
	// Cert is the PEM-encoded certificate for this node.
	Cert []byte
	// Key is the PEM-encoded private key for this node.
	Key []byte
	// Certificate, when non-nil, supplies a pre-built TLS certificate in place
	// of the PEM Cert/Key pair. Its PrivateKey may be a hardware-backed
	// crypto.Signer (TPM/HSM-resident key that never leaves the device). When
	// set, Cert/Key are ignored.
	Certificate *tls.Certificate
}

// Config holds all configuration for a QUICTransport instance.
type Config struct {
	// ListenAddress is the local address to listen on (e.g. ":4001" or "0.0.0.0:4001").
	ListenAddress string

	// AdvertiseAddress is the address this node advertises to peers as its
	// reachable Raft transport address. When non-empty it is used as the
	// SourceAddress in outbound message batches so that peers can dial back
	// to this node using the correct external address rather than the local
	// bind address (which may be "0.0.0.0" or "[::]:port" on multi-homed hosts
	// and Docker containers). When empty, the OS-reported local address from
	// the bound UDP socket is used (backward-compatible behaviour).
	AdvertiseAddress string

	// DeploymentID identifies this deployment. Connections from peers with
	// a different DeploymentID are rejected.
	DeploymentID uint64

	// MTLSConfig holds mTLS certificate material. Required for all
	// transport connections. Must provide CACert, Cert, and Key.
	MTLSConfig *MTLSConfig

	// Enable0RTT enables QUIC 0-RTT for reduced latency reconnects.
	// Default: false.
	//
	// WARNING: 0-RTT data is replayable (RFC 9001 Section 8). Requires
	// MTLSConfig to be set; returns ErrInsecure0RTT otherwise.
	// Raft's internal idempotency (duplicate detection, term/index
	// checks) provides defense-in-depth against replayed internal
	// messages. See TransportConfig.Enable0RTT for full details.
	Enable0RTT bool

	// StreamPoolSize is the number of streams in the pool per connection.
	// Stream 0 is reserved for heartbeats; streams 1..N-1 carry data.
	// Default: 16.
	StreamPoolSize int

	// MaxStreamPoolSize is the upper bound for StreamPoolSize.
	// Default: 256.
	MaxStreamPoolSize int

	// MaxSnapshotReceiveRate is the maximum bytes per second for inbound
	// snapshot data. Default: 256 MB/s.
	MaxSnapshotReceiveRate int64

	// MaxConcurrentSnapshotRecv is the maximum number of concurrent
	// inbound snapshot transfers. Default: 4.
	MaxConcurrentSnapshotRecv int

	// MaxSnapshotReceiveMemory is the total bytes budget for concurrent
	// snapshot receive buffers. Default: 1 GB.
	MaxSnapshotReceiveMemory int64

	// MaxMessageReceiveMemory is the total bytes budget for concurrent inbound
	// message-frame payload buffers, across all connections and streams. Each
	// frame is bounded to MaxFrameSize, but the buffer is allocated before the
	// body is read, so without an aggregate budget an authenticated peer can
	// open many streams that each declare the max length and withhold the body,
	// pinning MaxFrameSize per stream and exhausting host memory (OOM DoS). A
	// reservation against this budget is taken before allocation and released
	// once the frame is dispatched.
	//
	// Frames are charged by proto.FrameDecodeCharge, so one maximum-size frame
	// costs maxFrameCharge (128 MB). Default:
	// config.DefaultMessageReceiveBudgetFrames such charges (512 MB). Validate
	// rejects a budget below one maximum-frame charge with
	// *MaxMessageReceiveMemoryTooSmallError. One peer may hold at most a
	// quarter of the budget, floored at one maximum-frame charge.
	//
	// It bounds APPLICATION frame buffers only. QUIC also buffers received
	// bytes before the application reads them, up to MaxIncomingConnections *
	// the per-connection receive window; that window is derived from this
	// value (see connectionReceiveWindow), so the total inbound receive
	// footprint is this budget plus MaxIncomingConnections *
	// connectionReceiveWindow(...), not this budget alone.
	//
	// The derived window also sets the ceiling on replication throughput over
	// a high-latency link, since a receive window caps a connection at
	// window/RTT. At the defaults (512 MB over 256 connections) the derivation
	// floors at 8 MB, which sustains roughly 134 Gbps at a 0.5 ms same-AZ RTT
	// but caps a 10 Gbps path beyond ~6.7 ms RTT and a 1 Gbps path beyond
	// ~67 ms. Same-datacenter and same-AZ replication has ample headroom;
	// deployments replicating across regions on 10 GbE or faster should raise
	// this budget (or lower MaxIncomingConnections) to widen the window.
	// Measured on loopback by BenchmarkThroughputQUIC, which finds no
	// throughput cost to the 8 MB floor there — loopback RTT is ~0.05 ms, so
	// that benchmark cannot observe the bandwidth-delay-product effect this
	// paragraph describes and does not contradict it.
	MaxMessageReceiveMemory int64

	// SendBatchMaxSize is the initial capacity in bytes (plus the frame
	// header) of each pooled send buffer. It is a sizing hint only: it does
	// not trigger a flush or cap a batch, and a batch that encodes larger
	// gets a buffer of the size it needs. Default: 64 KB.
	SendBatchMaxSize int

	// MaxDecompressedSize is the maximum allowed decompressed payload size.
	// Prevents decompression bombs. It must equal MaxFrameSize; zero selects
	// that value and anything else is rejected by Validate.
	//
	// It is NOT a per-node tunable. MaxFrameSize is a compile-time constant
	// and is what the SEND path admits against (messageFrameError,
	// sendableGroup), so a receiver configured below it rejects frames every
	// sender in the cluster considers valid. The sender treats that rejection
	// as a frame-level failure — it drops the batch and KEEPS the connection
	// (deliberately, so a wire-version mismatch cannot become a dial storm) —
	// so the misconfigured replica simply stops making replication progress,
	// with a 10s-rate-limited warning and a "peer_rejected" drop counter as
	// the only signal. Pinning the value turns that silent, asymmetric wire
	// contract into a startup error.
	MaxDecompressedSize uint32

	// MaxIncomingConnections is the total number of concurrent inbound
	// QUIC connections allowed. Default: 256.
	MaxIncomingConnections int

	// MaxConnectionsPerIP is the maximum number of concurrent inbound
	// QUIC connections allowed from a single source IP. Default: 16.
	MaxConnectionsPerIP int

	// MaxStreamsPerConnection is the maximum number of concurrent
	// stream-processing goroutines per inbound QUIC connection.
	// Prevents resource exhaustion from peers opening excessive streams.
	//
	// Note: an idle stream on a live connection is deliberately kept open
	// (see StreamReadIdleTimeout), so each accepted stream pins one
	// goroutine and one slot of this budget until its connection closes.
	// The worst-case goroutine count from authenticated peers is therefore
	// MaxIncomingConnections * MaxStreamsPerConnection (256*256 = 65536 at
	// the defaults). Reaping idle streams instead would reintroduce the
	// post-idle replication stall: senders cache and reuse streams across
	// idle gaps, so an abandoned-but-cached stream black-holes traffic.
	// Default: 256.
	MaxStreamsPerConnection int

	// UDPRecvBufSize is the SO_RCVBUF size for the UDP socket in bytes.
	// Larger buffers prevent packet drops under burst traffic, especially
	// in multi-node clusters. Default: 7 MB.
	UDPRecvBufSize int

	// UDPSendBufSize is the SO_SNDBUF size for the UDP socket in bytes.
	// Larger buffers allow more outbound packets to be queued before
	// blocking. Default: 7 MB.
	UDPSendBufSize int

	// DisableCompression disables Snappy compression on the wire.
	// When true, all messages are sent uncompressed regardless of size.
	DisableCompression bool

	// StreamReadIdleTimeout bounds how long an inbound stream read waits at a
	// clean frame boundary before re-checking connection liveness. An idle
	// stream on a LIVE connection is kept open (not abandoned), so a
	// replication stream that sits idle between bursts is not torn down while
	// the leader still holds and reuses it. Default: 30s.
	StreamReadIdleTimeout time.Duration

	// DialTimeout bounds one dial attempt: the QUIC handshake with mTLS to a
	// peer that has no live connection. A dial that expires drops the batch
	// it was for, and Raft retransmits, so the production figure is a
	// liveness budget, not a correctness one. Default: dialTimeout (5s).
	//
	// It is configurable for one reason. A test that calls the dial path
	// directly has no retransmission behind it: a handshake that exceeds the
	// budget fails the test outright, and under CI saturation (-race,
	// coverage, every package in parallel) a cold loopback handshake has
	// been traced at 4.3s and past 5s. The test fixture
	// therefore runs with a wider budget; nothing in production sets this.
	DialTimeout time.Duration

	// RevocationConfig holds optional certificate revocation checking
	// configuration. When non-nil, a revocation.Checker is created and
	// wired into the TLS VerifyConnection callback to reject revoked peer
	// certificates. VerifyConnection, not VerifyPeerCertificate: the latter
	// is skipped on resumed sessions, so resumption would bypass revocation.
	// CRL files are periodically reloaded from disk for hot-reload support.
	RevocationConfig *revocation.Config

	// ReplicaIdentity returns the node identity membership binds to replica
	// replicaID of shard shardID, or "" when none is bound. It pins outbound
	// traffic to the replica it is for: a message, or a snapshot, is written
	// only to a connection whose peer presents its destination's identity,
	// and a dial is refused unless the peer presents the identity of some
	// replica routed to the dialed address. The TLS handshake alone proves
	// only a certificate from the cluster CA whose SAN covers the host, which
	// a wildcard or shared-SAN certificate held by anyone on the path to that
	// address also satisfies. Nil disables pinning; the host always sets it.
	ReplicaIdentity func(shardID, replicaID uint64) string

	// EventListener holds optional event callbacks for connection and
	// snapshot lifecycle events. May be nil.
	EventListener *config.EventListener

	// Metrics collects transport-level metrics (send latency, message
	// counts, snapshot transfers). Nil when metrics are disabled.
	Metrics Metrics

	// Wire is the closed set of wire versions this transport accepts and
	// the version it advertises. The zero value means this build's
	// DefaultWireWindow; only a test emulating another build sets it.
	Wire WireWindow
}

// WireWindow pairs the payload accept set (MessageBatch and SnapshotChunk
// BinVer) with the frame header's. Both are closed sets of {current,
// previous} and move together: a frame at FrameCurrent carries a payload at
// Proto.Current, and a frame at FramePrevious one at Proto.Previous.
type WireWindow struct {
	Proto         proto.WireWindow
	FrameCurrent  byte
	FramePrevious byte
}

// DefaultWireWindow returns this build's window. In v1.0 each set has one
// member.
func DefaultWireWindow() WireWindow {
	return WireWindow{
		Proto:         proto.DefaultWireWindow(),
		FrameCurrent:  WireVersion,
		FramePrevious: PreviousWireVersion,
	}
}

// isZero reports whether w is unset.
func (w WireWindow) isZero() bool {
	return w == (WireWindow{})
}

// AcceptsFrame reports whether a frame header version byte is in the set.
func (w WireWindow) AcceptsFrame(v byte) bool {
	return v == w.FrameCurrent || v == w.FramePrevious
}

// sendVersions returns the payload and frame versions to stamp on traffic
// for a shard whose committed WireVersion is v. A shard sends the current
// version only once it has committed the switch to it; any other value,
// including the zero of a shard that never switched, sends the previous
// version, which every member of the window reads.
func (w WireWindow) sendVersions(v uint64) (uint64, byte) {
	if v == w.Proto.Current {
		return w.Proto.Current, w.FrameCurrent
	}
	return w.Proto.Previous, w.FramePrevious
}

// WireVersionLookup returns a shard's committed WireVersion (see
// proto.Membership.WireVersion). It is called on the send path, so it must
// be cheap and must not block; an unknown shard returns the previous
// version or zero.
type WireVersionLookup func(shardID uint64) uint64

// connectionReceiveWindow derives the QUIC per-connection receive window from
// the application's message-receive budget. quic-go buffers received bytes
// before the application ever sees them, so a window fixed independently of
// the budget multiplies by MaxIncomingConnections into memory the budget does
// not cover (32 MB * 256 connections = 8 GB against a 512 MB budget).
//
// The result is floored at maxStreamReceiveWindow — a connection window below
// the per-stream window makes that stream window unreachable and throttles
// replication — and capped at maxConnectionReceiveWindowCeiling. The floor
// means the QUIC-level footprint is not strictly inside the budget for small
// budgets; it is bounded and stated on MaxMessageReceiveMemory rather than
// silently unbounded.
func connectionReceiveWindow(maxMessageRecvMemory int64, maxIncomingConns int) uint64 {
	if maxIncomingConns < 1 || maxMessageRecvMemory <= 0 {
		return maxStreamReceiveWindow
	}
	window := uint64(maxMessageRecvMemory) / uint64(maxIncomingConns)
	if window < maxStreamReceiveWindow {
		return maxStreamReceiveWindow
	}
	if window > maxConnectionReceiveWindowCeiling {
		return maxConnectionReceiveWindowCeiling
	}
	return window
}

// maxFrameCharge returns the most one inbound message frame can reserve
// against the message-receive budgets: a MaxFrameSize frame whose payload
// decodes to maxDecompressed bytes, charged by proto.FrameDecodeCharge. It is
// the composition handleMessageFrame charges (proto.DecodeCharge of the wire
// length, then of the decompression delta), so the budget it sizes cannot
// drift from the reservations it bounds.
func maxFrameCharge(maxDecompressed uint32) int64 {
	return proto.FrameDecodeCharge(int64(MaxFrameSize), int64(maxDecompressed))
}

// SetDefaults fills zero-valued fields with production defaults.
func (c *Config) SetDefaults() {
	if c.Wire.isZero() {
		c.Wire = DefaultWireWindow()
	}
	if c.StreamPoolSize == 0 {
		c.StreamPoolSize = defaultStreamPoolSize
	}
	if c.DialTimeout == 0 {
		c.DialTimeout = dialTimeout
	}
	if c.MaxStreamPoolSize == 0 {
		c.MaxStreamPoolSize = defaultMaxStreamPoolSize
	}
	if c.MaxSnapshotReceiveRate == 0 {
		c.MaxSnapshotReceiveRate = defaultMaxSnapshotReceiveRate
	}
	if c.MaxConcurrentSnapshotRecv == 0 {
		c.MaxConcurrentSnapshotRecv = defaultMaxConcurrentSnapRecv
	}
	if c.MaxSnapshotReceiveMemory == 0 {
		c.MaxSnapshotReceiveMemory = defaultMaxSnapshotRecvMemory
	}
	if c.SendBatchMaxSize == 0 {
		c.SendBatchMaxSize = defaultSendBatchMaxSize
	}
	if c.MaxDecompressedSize == 0 {
		c.MaxDecompressedSize = MaxFrameSize
	}
	// Derived after MaxDecompressedSize, which the frame charge depends on.
	if c.MaxMessageReceiveMemory == 0 {
		c.MaxMessageReceiveMemory = int64(config.DefaultMessageReceiveBudgetFrames) * maxFrameCharge(c.MaxDecompressedSize)
	}
	if c.MaxIncomingConnections == 0 {
		c.MaxIncomingConnections = defaultMaxIncomingConnections
	}
	if c.MaxConnectionsPerIP == 0 {
		c.MaxConnectionsPerIP = defaultMaxConnectionsPerIP
	}
	if c.MaxStreamsPerConnection == 0 {
		c.MaxStreamsPerConnection = defaultMaxStreamsPerConnection
	}
	if c.UDPRecvBufSize == 0 {
		c.UDPRecvBufSize = defaultUDPRecvBufSize
	}
	if c.UDPSendBufSize == 0 {
		c.UDPSendBufSize = defaultUDPSendBufSize
	}
	if c.StreamReadIdleTimeout == 0 {
		c.StreamReadIdleTimeout = defaultStreamReadIdleTimeout
	}
}

// Validate checks that all configuration values are within valid bounds.
// Call SetDefaults before Validate to ensure defaults are applied.
func (c *Config) Validate() error {
	if c.ListenAddress == "" {
		return ErrEmptyListenAddress
	}
	if c.MaxStreamPoolSize < 1 {
		return ErrInvalidMaxStreamPoolSize
	}
	if c.StreamPoolSize < 1 || c.StreamPoolSize > c.MaxStreamPoolSize {
		return ErrInvalidStreamPoolSize
	}
	if c.MaxSnapshotReceiveRate <= 0 {
		return ErrInvalidMaxSnapshotReceiveRate
	}
	if c.DialTimeout <= 0 {
		return ErrInvalidDialTimeout
	}
	if c.MaxConcurrentSnapshotRecv <= 0 {
		return ErrInvalidMaxConcurrentSnapshotRecv
	}
	if c.MaxSnapshotReceiveMemory <= 0 {
		return ErrInvalidMaxSnapshotReceiveMemory
	}
	if c.MaxMessageReceiveMemory <= 0 {
		return ErrInvalidMaxMessageReceiveMemory
	}
	if c.SendBatchMaxSize <= 0 {
		return ErrInvalidSendBatchMaxSize
	}
	if c.MaxDecompressedSize != MaxFrameSize {
		return ErrInvalidMaxDecompressedSize
	}
	if minimum := maxFrameCharge(c.MaxDecompressedSize); c.MaxMessageReceiveMemory < minimum {
		return &MaxMessageReceiveMemoryTooSmallError{Configured: c.MaxMessageReceiveMemory, Minimum: minimum}
	}
	if c.MaxIncomingConnections < 1 {
		return ErrInvalidMaxIncomingConnections
	}
	if c.MaxConnectionsPerIP < 1 {
		return ErrInvalidMaxConnectionsPerIP
	}
	if c.MaxStreamsPerConnection < 1 {
		return ErrInvalidMaxStreamsPerConnection
	}
	// 0-RTT requires mTLS because 0-RTT data is replayable. Without
	// mutual authentication, an attacker could replay captured 0-RTT
	// packets to execute unauthorized operations.
	if c.Enable0RTT && c.MTLSConfig == nil {
		return ErrInsecure0RTT
	}
	return validateMTLSConfig(c.MTLSConfig)
}

// validateMTLSConfig checks that mTLS material is present and complete. It is
// the presence half of the checks NewQUICTransport and ReloadMTLS both run;
// buildMTLSTLS does the parsing half.
func validateMTLSConfig(m *MTLSConfig) error {
	if m == nil {
		return ErrMTLSConfigRequired
	}
	// CACert is always required; the node leaf comes from either a pre-built
	// Certificate or the PEM Cert/Key pair.
	if len(m.CACert) == 0 {
		return ErrIncompleteMTLSConfig
	}
	if m.Certificate == nil &&
		(len(m.Cert) == 0 || len(m.Key) == 0) {
		return ErrIncompleteMTLSConfig
	}
	return nil
}
