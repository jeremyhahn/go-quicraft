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

// config.DefaultMaxDecompressedSize must equal MaxFrameSize; these assertions fail the build if they differ.
const (
	_ = uint64(MaxFrameSize) - uint64(config.DefaultMaxDecompressedSize)
	_ = uint64(config.DefaultMaxDecompressedSize) - uint64(MaxFrameSize)
)

// Metrics is the transport's metrics interface; nil skips collection. The implementation is in package quicraft.
type Metrics interface {
	ObserveSendLatency(d time.Duration)
	IncMessagesSent(shardID, to uint64, msgType uint64)
	IncMessagesReceived(shardID, from uint64, msgType uint64)
	IncSnapshotsSent(shardID uint64, bytes int64, d time.Duration)
	IncSnapshotsReceived(shardID uint64, bytes int64, d time.Duration)
	IncSnapshotsRejected(shardID uint64, reason string)
	IncMessagesDropped(shardID, to uint64, reason string)
}

// snapshotRejectReason is the closed set of reason labels for IncSnapshotsRejected.
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

	// dropReasonUnsendable is a message no peer can accept (too many entries or larger than a frame), dropped before it is written.
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

	// defaultStreamReadIdleTimeout bounds an idle read at a frame boundary before re-checking connection liveness; it is not a stream lifetime.
	defaultStreamReadIdleTimeout = 30 * time.Second

	// frameReadActiveTimeout bounds completing a frame once it has begun
	// arriving. A frame that starts but does not finish within this window is
	// a genuine stall (dead/slow peer), so the stream is dropped.
	frameReadActiveTimeout = 30 * time.Second

	// QUIC flow-control windows bound quic-go buffering before the application reads a frame, in addition to MaxMessageReceiveMemory.
	initialStreamReceiveWindow = 2 << 20 // 2 MB
	maxStreamReceiveWindow     = 8 << 20 // 8 MB

	// maxConnectionReceiveWindowCeiling caps the per-connection window that
	// connectionReceiveWindow derives, so a large MaxMessageReceiveMemory does
	// not translate into an unbounded per-connection buffer.
	maxConnectionReceiveWindowCeiling = 32 << 20 // 32 MB

	// snapshotStreamHeadroom is extra incoming streams reserved per peer beyond StreamPoolSize for snapshot and ephemeral streams.
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
	// Certificate, when non-nil, replaces the PEM Cert/Key pair; its PrivateKey may be a hardware-backed crypto.Signer.
	Certificate *tls.Certificate
}

// Config holds all configuration for a QUICTransport instance.
type Config struct {
	// ListenAddress is the local address to listen on (e.g. ":4001" or "0.0.0.0:4001").
	ListenAddress string

	// AdvertiseAddress is the transport address advertised to peers as SourceAddress; empty uses the bound socket's local address.
	AdvertiseAddress string

	// DeploymentID identifies this deployment. Connections from peers with
	// a different DeploymentID are rejected.
	DeploymentID uint64

	// MTLSConfig holds mTLS certificate material. Required for all
	// transport connections. Must provide CACert, Cert, and Key.
	MTLSConfig *MTLSConfig

	// Enable0RTT enables QUIC 0-RTT (default false); 0-RTT data is replayable (RFC 9001 section 8) and requires MTLSConfig, else ErrInsecure0RTT.
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

	// MaxMessageReceiveMemory is the aggregate inbound frame-buffer budget, charged by proto.FrameDecodeCharge (default 512 MB, at most a quarter per peer); it also derives the per-connection QUIC receive window, which caps throughput at window/RTT.
	MaxMessageReceiveMemory int64

	// SendBatchMaxSize is the initial capacity of each pooled send buffer, a sizing hint only. Default: 64 KB.
	SendBatchMaxSize int

	// MaxDecompressedSize must equal MaxFrameSize; zero selects it and anything else fails Validate, since every sender admits against MaxFrameSize.
	MaxDecompressedSize uint32

	// MaxIncomingConnections is the total number of concurrent inbound
	// QUIC connections allowed. Default: 256.
	MaxIncomingConnections int

	// MaxConnectionsPerIP is the maximum number of concurrent inbound
	// QUIC connections allowed from a single source IP. Default: 16.
	MaxConnectionsPerIP int

	// MaxStreamsPerConnection caps stream goroutines per inbound connection; idle streams stay open, so the worst case is MaxIncomingConnections * this. Default: 256.
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

	// StreamReadIdleTimeout bounds an idle read at a frame boundary before re-checking liveness; idle streams on live connections stay open. Default: 30s.
	StreamReadIdleTimeout time.Duration

	// DialTimeout bounds one QUIC/mTLS dial attempt; an expired dial drops its batch for Raft to retransmit. Only tests change it. Default: dialTimeout (5s).
	DialTimeout time.Duration

	// RevocationConfig, when non-nil, enables certificate revocation checks in VerifyConnection, which also runs on resumed sessions; CRL files are reloaded periodically.
	RevocationConfig *revocation.Config

	// ReplicaIdentity returns the node identity bound to a replica, or ""; outbound messages and dials are pinned to that identity. Nil disables pinning.
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

// WireWindow pairs the payload and frame-header accept sets, each {current, previous}, which move together.
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

// sendVersions returns the payload and frame versions to send for committed WireVersion v: current only once the switch is committed, otherwise previous.
func (w WireWindow) sendVersions(v uint64) (uint64, byte) {
	if v == w.Proto.Current {
		return w.Proto.Current, w.FrameCurrent
	}
	return w.Proto.Previous, w.FramePrevious
}

// WireVersionLookup returns a shard's committed WireVersion; called on the send path, so it must be cheap and non-blocking.
type WireVersionLookup func(shardID uint64) uint64

// connectionReceiveWindow derives the QUIC per-connection receive window from the message budget, floored at maxStreamReceiveWindow and capped at maxConnectionReceiveWindowCeiling.
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

// maxFrameCharge returns the most one inbound frame can reserve: a MaxFrameSize frame decoding to maxDecompressed bytes, charged as handleMessageFrame charges it.
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
