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
	"errors"
	"time"

	"github.com/jeremyhahn/go-quicraft/pkg/config"
	"github.com/jeremyhahn/go-quicraft/pkg/internal/transport/revocation"
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
}

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

	// ErrInvalidMaxConcurrentSnapshotRecv is returned when MaxConcurrentSnapshotRecv <= 0.
	ErrInvalidMaxConcurrentSnapshotRecv = errors.New("transport: max concurrent snapshot recv must be > 0")

	// ErrInvalidMaxSnapshotReceiveMemory is returned when MaxSnapshotReceiveMemory <= 0.
	ErrInvalidMaxSnapshotReceiveMemory = errors.New("transport: max snapshot receive memory must be > 0")

	// ErrInvalidSendBatchMaxSize is returned when SendBatchMaxSize <= 0.
	ErrInvalidSendBatchMaxSize = errors.New("transport: send batch max size must be > 0")

	// ErrInvalidMaxDecompressedSize is returned when MaxDecompressedSize exceeds
	// MaxFrameSize or is zero.
	ErrInvalidMaxDecompressedSize = errors.New("transport: max decompressed size out of range")

	// ErrIncompleteMTLSConfig is returned when MTLSConfig is partially set.
	ErrIncompleteMTLSConfig = errors.New("transport: mTLS config requires CACert, Cert, and Key")

	// ErrInvalidMaxIncomingConnections is returned when MaxIncomingConnections < 1.
	ErrInvalidMaxIncomingConnections = errors.New("transport: max incoming connections must be >= 1")

	// ErrInvalidMaxConnectionsPerIP is returned when MaxConnectionsPerIP < 1.
	ErrInvalidMaxConnectionsPerIP = errors.New("transport: max connections per IP must be >= 1")
)

// MTLSConfig holds the certificate material for mutual TLS authentication.
// All three fields must be provided together.
type MTLSConfig struct {
	// CACert is the PEM-encoded CA certificate used to verify peer certificates.
	CACert []byte
	// Cert is the PEM-encoded certificate for this node.
	Cert []byte
	// Key is the PEM-encoded private key for this node.
	Key []byte
}

// Config holds all configuration for a QUICTransport instance.
type Config struct {
	// ListenAddress is the local address to listen on (e.g. ":4001" or "0.0.0.0:4001").
	ListenAddress string

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

	// SendBatchMaxSize is the maximum byte size of a single send batch.
	// Default: 64 KB.
	SendBatchMaxSize int

	// MaxDecompressedSize is the maximum allowed decompressed payload size.
	// Prevents decompression bombs. Default: MaxFrameSize.
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

	// RevocationConfig holds optional certificate revocation checking
	// configuration. When non-nil, a revocation.Checker is created and
	// wired into the TLS VerifyPeerCertificate callback to reject
	// revoked peer certificates during handshake. CRL files are
	// periodically reloaded from disk for hot-reload support.
	RevocationConfig *revocation.Config

	// EventListener holds optional event callbacks for connection and
	// snapshot lifecycle events. May be nil.
	EventListener *config.EventListener

	// Metrics collects transport-level metrics (send latency, message
	// counts, snapshot transfers). Nil when metrics are disabled.
	Metrics Metrics
}

// SetDefaults fills zero-valued fields with production defaults.
func (c *Config) SetDefaults() {
	if c.StreamPoolSize == 0 {
		c.StreamPoolSize = defaultStreamPoolSize
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
	if c.MaxConcurrentSnapshotRecv <= 0 {
		return ErrInvalidMaxConcurrentSnapshotRecv
	}
	if c.MaxSnapshotReceiveMemory <= 0 {
		return ErrInvalidMaxSnapshotReceiveMemory
	}
	if c.SendBatchMaxSize <= 0 {
		return ErrInvalidSendBatchMaxSize
	}
	if c.MaxDecompressedSize == 0 || c.MaxDecompressedSize > MaxFrameSize {
		return ErrInvalidMaxDecompressedSize
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
	if c.MTLSConfig == nil {
		return ErrMTLSConfigRequired
	}
	if len(c.MTLSConfig.CACert) == 0 || len(c.MTLSConfig.Cert) == 0 || len(c.MTLSConfig.Key) == 0 {
		return ErrIncompleteMTLSConfig
	}
	return nil
}
