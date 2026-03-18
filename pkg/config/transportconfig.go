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

package config

import "time"

// Default values for TransportConfig fields.
const (
	DefaultStreamPoolSize            int    = 16
	DefaultMaxStreamPoolSize         int    = 256
	DefaultMaxSnapshotReceiveRate    int64  = 256 * 1024 * 1024 // 256 MB/s
	DefaultMaxConcurrentSnapshotRecv int    = 4
	DefaultMaxSnapshotReceiveMemory  int64  = 1024 * 1024 * 1024 // 1 GB
	DefaultMaxDecompressedSize       uint32 = 16 * 1024 * 1024   // 16 MB (transport.MaxFrameSize)
	DefaultMaxConnectionsPerIP       int    = 16
	DefaultMaxIncomingConnections    int    = 256
	DefaultSendBatchMaxSize          int    = 64 * 1024       // 64 KB
	DefaultUDPRecvBufSize            int    = 7 * 1024 * 1024 // 7 MB (within typical rmem_max)
	DefaultUDPSendBufSize            int    = 7 * 1024 * 1024 // 7 MB (within typical wmem_max)
)

// MTLSConfig holds the PEM-encoded certificate material for mutual TLS
// authentication. All QUIC transport connections require mTLS; there is
// no insecure fallback.
type MTLSConfig struct {
	// CACert is the PEM-encoded CA certificate used to verify peer certificates.
	CACert []byte

	// Cert is the PEM-encoded certificate for this node.
	Cert []byte

	// Key is the PEM-encoded private key for this node.
	Key []byte
}

// RevocationConfig holds certificate revocation checking configuration.
// When set on TransportConfig, the transport installs a
// VerifyPeerCertificate callback that rejects revoked peer certificates
// at TLS handshake time. CRL files are periodically reloaded from disk
// for hot-reload support without requiring a restart.
type RevocationConfig struct {
	// CRLPaths are file paths to PEM-encoded CRL files. These files are
	// watched for content changes on each CheckInterval tick using
	// SHA-256 hash comparison (no filesystem watcher dependency).
	CRLPaths []string

	// OCSPResponderURL is the URL of an OCSP responder for online
	// certificate status checks. When empty, OCSP checking is disabled
	// and only CRL checking is used.
	OCSPResponderURL string

	// OCSPCacheSeconds controls how long OCSP responses are cached to
	// avoid per-handshake HTTP requests. Default: 300 (5 minutes).
	OCSPCacheSeconds int

	// OCSPTimeoutSeconds is the HTTP timeout for OCSP responder
	// requests. Default: 5.
	OCSPTimeoutSeconds int

	// CheckInterval controls how often CRL files are reloaded from
	// disk. Default: 60s.
	CheckInterval time.Duration

	// Mode determines the revocation checking strategy:
	//   "crl"  - CRL files only
	//   "ocsp" - OCSP responder only
	//   "both" - requires both CRL and OCSP to confirm revocation
	//   "any"  - either CRL or OCSP confirms revocation (default)
	Mode string

	// EnforceRevocation rejects peers when revocation status is unknown
	// (OCSP unreachable, no CRL configured). Default: false (soft-fail
	// allows connections when revocation status cannot be determined).
	EnforceRevocation bool

	// OnCertRevoked is called when a certificate is found to be revoked
	// during a TLS handshake. The serial number is passed as a lowercase
	// hex string. May be nil.
	OnCertRevoked func(serial string)
}

// TransportConfig exposes transport-layer tuning parameters. All fields
// have documented defaults and a zero-value TransportConfig is valid
// after calling SetDefaults.
type TransportConfig struct {
	// MTLSConfig holds the mTLS certificate material. Required when
	// transport is enabled (the default). Must contain valid PEM-encoded
	// CACert, Cert, and Key.
	MTLSConfig *MTLSConfig

	// RevocationConfig holds optional certificate revocation checking
	// configuration. When non-nil, CRL and/or OCSP checking is enabled
	// with hot-reload support. See RevocationConfig for details.
	RevocationConfig *RevocationConfig
	// StreamPoolSize is the number of QUIC streams per connection for
	// multiplexing shard traffic. Default: 16.
	StreamPoolSize int

	// MaxStreamPoolSize is the upper bound for stream pool auto-scaling
	// under load. Default: 256.
	MaxStreamPoolSize int

	// MaxSnapshotReceiveRate is the maximum inbound snapshot bandwidth in
	// bytes per second per connection. Default: 256 MB/s.
	MaxSnapshotReceiveRate int64

	// MaxConcurrentSnapshotReceives is the maximum number of concurrent
	// inbound snapshot streams. Default: 4.
	MaxConcurrentSnapshotReceives int

	// MaxSnapshotReceiveMemory is the total bytes allowed for in-flight
	// snapshot receives across all connections. Default: 1 GB.
	MaxSnapshotReceiveMemory int64

	// MaxDecompressedSize is the maximum allowed size after Snappy
	// decompression. Default: 16 MB (MaxFrameSize).
	MaxDecompressedSize uint32

	// MaxConnectionsPerIP is the maximum number of QUIC connections
	// allowed from a single source IP. Default: 16.
	MaxConnectionsPerIP int

	// MaxIncomingConnections is the total number of concurrent inbound
	// QUIC connections. Default: 256.
	MaxIncomingConnections int

	// SendBatchMaxSize is the maximum send buffer size in bytes before a
	// flush is triggered. Default: 64 KB.
	SendBatchMaxSize int

	// UDPRecvBufSize is the SO_RCVBUF size for the UDP socket. Larger
	// buffers prevent packet drops under burst traffic, especially in
	// multi-node clusters where many QUIC connections share the same
	// socket. Default: 7 MB. Set to 0 to use the OS default.
	UDPRecvBufSize int

	// UDPSendBufSize is the SO_SNDBUF size for the UDP socket. Larger
	// buffers allow more outbound packets to be queued before blocking.
	// Default: 7 MB. Set to 0 to use the OS default.
	UDPSendBufSize int

	// Enable0RTT enables QUIC 0-RTT reconnection for reduced latency on
	// resumed connections. Disabled by default because 0-RTT data is
	// inherently replayable per RFC 9001 Section 8: an attacker who
	// captures a 0-RTT flight can replay it to the server.
	//
	// Security considerations:
	//   - Requires MTLSConfig to be set (validated at startup). Without
	//     mutual TLS, replayed packets could execute unauthenticated
	//     operations.
	//   - Raft's internal idempotency provides defense-in-depth against
	//     replay of internal messages: duplicate message detection, term
	//     checks, and log index checks cause replayed Raft RPCs to be
	//     harmlessly discarded. Session deduplication further protects
	//     client proposals.
	//   - Enable only in trusted networks where the mTLS + Raft
	//     idempotency combination provides acceptable replay resistance.
	Enable0RTT bool

	// DisableCompression disables Snappy compression on the transport
	// wire. By default, non-heartbeat messages are Snappy-compressed
	// when the compressed payload is smaller than the original. Set
	// this to true for localhost benchmarks where compression adds CPU
	// overhead without bandwidth savings, or when comparing against
	// systems that do not compress transport messages.
	DisableCompression bool

	// TransportDisabled indicates that the transport layer is not used.
	// When true, MTLSConfig validation is skipped because no QUIC
	// connections will be established. This is set internally by
	// WithoutTransport() and should not be configured directly.
	TransportDisabled bool
}

// SetDefaults fills zero-valued fields with their documented defaults.
func (tc *TransportConfig) SetDefaults() {
	if tc.StreamPoolSize == 0 {
		tc.StreamPoolSize = DefaultStreamPoolSize
	}
	if tc.MaxStreamPoolSize == 0 {
		tc.MaxStreamPoolSize = DefaultMaxStreamPoolSize
	}
	if tc.MaxSnapshotReceiveRate == 0 {
		tc.MaxSnapshotReceiveRate = DefaultMaxSnapshotReceiveRate
	}
	if tc.MaxConcurrentSnapshotReceives == 0 {
		tc.MaxConcurrentSnapshotReceives = DefaultMaxConcurrentSnapshotRecv
	}
	if tc.MaxSnapshotReceiveMemory == 0 {
		tc.MaxSnapshotReceiveMemory = DefaultMaxSnapshotReceiveMemory
	}
	if tc.MaxDecompressedSize == 0 {
		tc.MaxDecompressedSize = DefaultMaxDecompressedSize
	}
	if tc.MaxConnectionsPerIP == 0 {
		tc.MaxConnectionsPerIP = DefaultMaxConnectionsPerIP
	}
	if tc.MaxIncomingConnections == 0 {
		tc.MaxIncomingConnections = DefaultMaxIncomingConnections
	}
	if tc.SendBatchMaxSize == 0 {
		tc.SendBatchMaxSize = DefaultSendBatchMaxSize
	}
	if tc.UDPRecvBufSize == 0 {
		tc.UDPRecvBufSize = DefaultUDPRecvBufSize
	}
	if tc.UDPSendBufSize == 0 {
		tc.UDPSendBufSize = DefaultUDPSendBufSize
	}
}

// Validate checks all transport configuration constraints. Returns a
// *ValidationError describing the first violated rule, or nil
// if the configuration is valid.
func (tc *TransportConfig) Validate() error {
	if tc.StreamPoolSize < 1 {
		return newValidationError("TransportConfig.StreamPoolSize", "must be >= 1")
	}
	if tc.MaxStreamPoolSize < tc.StreamPoolSize {
		return newValidationError("TransportConfig.MaxStreamPoolSize",
			"must be >= StreamPoolSize")
	}
	if tc.MaxSnapshotReceiveRate < 0 {
		return newValidationError("TransportConfig.MaxSnapshotReceiveRate",
			"must be >= 0")
	}
	if tc.MaxConcurrentSnapshotReceives < 1 {
		return newValidationError("TransportConfig.MaxConcurrentSnapshotReceives",
			"must be >= 1")
	}
	if tc.MaxSnapshotReceiveMemory < 0 {
		return newValidationError("TransportConfig.MaxSnapshotReceiveMemory",
			"must be >= 0")
	}
	if tc.MaxDecompressedSize == 0 {
		return newValidationError("TransportConfig.MaxDecompressedSize",
			"must be > 0")
	}
	if tc.MaxConnectionsPerIP < 1 {
		return newValidationError("TransportConfig.MaxConnectionsPerIP",
			"must be >= 1")
	}
	if tc.MaxIncomingConnections < 1 {
		return newValidationError("TransportConfig.MaxIncomingConnections",
			"must be >= 1")
	}
	if tc.SendBatchMaxSize < 1 {
		return newValidationError("TransportConfig.SendBatchMaxSize",
			"must be >= 1")
	}
	// When transport is enabled, MTLSConfig must be provided. All QUIC
	// connections require mTLS; there is no insecure fallback. When
	// TransportDisabled is true (set by WithoutTransport()), this
	// validation is skipped.
	if !tc.TransportDisabled && tc.MTLSConfig == nil {
		return &MTLSConfigRequiredError{}
	}
	return nil
}
