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

import (
	"crypto"
	"crypto/tls"
	"crypto/x509"
	"time"
)

// Default values for TransportConfig fields.
const (
	DefaultStreamPoolSize            int    = 16
	DefaultMaxStreamPoolSize         int    = 256
	DefaultMaxSnapshotReceiveRate    int64  = 256 * 1024 * 1024 // 256 MB/s
	DefaultMaxConcurrentSnapshotRecv int    = 4
	DefaultMaxSnapshotReceiveMemory  int64  = 1024 * 1024 * 1024 // 1 GB
	DefaultMaxMessageReceiveMemory   int64  = 256 * 1024 * 1024  // 256 MB
	DefaultMaxDecompressedSize       uint32 = 16 * 1024 * 1024   // 16 MB (transport.MaxFrameSize)
	DefaultMaxConnectionsPerIP       int    = 16
	DefaultMaxIncomingConnections    int    = 256
	DefaultSendBatchMaxSize          int    = 64 * 1024       // 64 KB
	DefaultUDPRecvBufSize            int    = 7 * 1024 * 1024 // 7 MB (within typical rmem_max)
	DefaultUDPSendBufSize            int    = 7 * 1024 * 1024 // 7 MB (within typical wmem_max)
	DefaultMaxStreamsPerConnection   int    = 256
	DefaultStreamReadIdleTimeout            = 30 * time.Second

	// MinMaxSnapshotReceiveRate is the smallest accepted non-zero value for
	// MaxSnapshotReceiveRate. Rates below this floor are rejected by Validate
	// because the receiver applies rate limiting by sleeping proportional to
	// chunk size: an absurdly low rate (e.g. a few bytes per second) would
	// compute a multi-day per-chunk sleep that holds the snapshot concurrency
	// semaphore and memory budget for the entire duration, stalling
	// availability. 64 KB/s is the slowest rate that keeps per-chunk sleeps
	// bounded to a sane range for typical multi-megabyte chunks.
	MinMaxSnapshotReceiveRate int64 = 64 * 1024 // 64 KB/s
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

	// Certificate, when non-nil, supplies a pre-built TLS certificate in place
	// of the PEM Cert/Key pair. Its PrivateKey may be a hardware-backed
	// crypto.Signer for a TPM/HSM-resident key that never leaves the device,
	// which cannot be expressed as PEM Key bytes. When set, Cert/Key may be
	// empty; CACert is still required for peer verification.
	Certificate *tls.Certificate
}

// RevocationConfig holds certificate revocation checking configuration.
// When set on TransportConfig, the transport installs a VerifyConnection
// callback that rejects revoked peer certificates at TLS handshake time.
// VerifyConnection rather than VerifyPeerCertificate is deliberate: the
// latter is skipped on resumed TLS sessions, which would let a peer whose
// certificate was revoked after its first handshake keep reconnecting. CRL files are periodically reloaded from disk
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
	//   "both" - consults both sources; revoked by EITHER is revoked
	//   "any"  - either CRL or OCSP confirms revocation (default)
	// Any other value is rejected by Validate; a typo must not silently
	// select the default strategy.
	Mode string

	// AllowSoftFail, when true, admits a peer whose revocation status cannot be
	// determined (OCSP responder unreachable, CRL stale/unreadable, or no
	// revocation source configured). The default (false) FAILS CLOSED: for an
	// intra-cluster mTLS control where the operator owns the CA and responder,
	// revocation is the only mechanism to evict a compromised-but-unexpired
	// certificate, so an unknown status must reject rather than admit — an
	// attacker who got a node revoked could otherwise keep connecting by making
	// the responder appear unreachable. Set this only when the availability
	// tradeoff is understood. A definitive "revoked" verdict always rejects
	// regardless of this flag.
	AllowSoftFail bool

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

	// MaxStreamPoolSize is the upper bound for StreamPoolSize. Default: 256.
	//
	// There is no auto-scaling. The pool is fixed at StreamPoolSize for the
	// lifetime of the connection -- streamIndexFor reads it as a constant --
	// and this field exists only so Validate can reject a StreamPoolSize
	// above it.
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

	// MaxMessageReceiveMemory is the total bytes allowed for in-flight inbound
	// message-frame payload buffers across all connections and streams. It
	// bounds an OOM DoS where a peer opens many streams that each declare a
	// large frame length and withhold the body. Default: 256 MB.
	//
	// It bounds APPLICATION frame buffers only. QUIC additionally buffers
	// received bytes before the application reads them, up to
	// MaxIncomingConnections * the per-connection QUIC receive window; that
	// window is derived from this value, so the total inbound receive
	// footprint is this budget PLUS the QUIC-level windows.
	MaxMessageReceiveMemory int64

	// MaxDecompressedSize is the maximum allowed size after Snappy
	// decompression. It must equal DefaultMaxDecompressedSize, which is the
	// transport's MaxFrameSize; zero selects that value.
	//
	// It is the receiving half of the wire contract rather than a local
	// tunable. The send path admits messages against MaxFrameSize, a
	// compile-time constant, so a replica configured below it rejects frames
	// every other node considers valid. The sender treats such a rejection as
	// a frame-level failure and keeps the connection (so a wire-version
	// mismatch cannot become a dial storm), which leaves the misconfigured
	// replica silently unable to replicate. Validate rejects the value instead.
	MaxDecompressedSize uint32

	// MaxConnectionsPerIP is the maximum number of QUIC connections
	// allowed from a single source IP. Default: 16.
	MaxConnectionsPerIP int

	// MaxIncomingConnections is the total number of concurrent inbound
	// QUIC connections. Default: 256.
	MaxIncomingConnections int

	// MaxStreamsPerConnection is the maximum number of concurrent
	// stream-processing goroutines per inbound QUIC connection. Since an
	// idle stream on a live connection is kept open (see
	// StreamReadIdleTimeout), each accepted stream holds one goroutine and
	// one slot of this budget until its connection closes; the worst-case
	// goroutine count is MaxIncomingConnections * MaxStreamsPerConnection.
	// Default: 256.
	MaxStreamsPerConnection int

	// StreamReadIdleTimeout bounds how long an inbound stream read waits
	// at a clean frame boundary before re-checking its connection's
	// liveness. It is NOT a stream lifetime: an idle stream on a live
	// connection is kept open across re-checks (replication streams sit
	// idle between bursts while heartbeats keep the connection alive).
	// Only a dead connection or a half-delivered frame stall ends a
	// stream. Default: 30s.
	StreamReadIdleTimeout time.Duration

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
	// connections will be established. WithoutTransport() sets this
	// internally, but the field is exported and honoured when set directly.
	//
	// NewHost gates transport creation on
	// (WithoutTransport() || TransportConfig.TransportDisabled), so setting
	// this field directly both skips mTLS validation AND prevents the QUIC
	// transport from being constructed — the same effect as the
	// WithoutTransport() host option. The two are kept in lockstep on
	// purpose: skipping the mTLS check without also disabling the transport
	// would be exactly backwards.
	TransportDisabled bool

	// RequireMemberIdentity makes the receive-side sender gate refuse every
	// quorum-bearing message from a replica whose COMMITTED MEMBERSHIP records
	// no node identity, instead of falling back to the committed-ADDRESS check.
	//
	// The two postures, stated plainly so the choice is an informed one:
	//
	//   - false (the default). A member with a committed identity is still
	//     authorized on that identity and nothing else — the fallback is
	//     per-replica, not a global downgrade. A member WITHOUT one is
	//     authorized on its committed address, which means a peer holding a
	//     wildcard or shared-SAN certificate can still assert that member's
	//     msg.From and have forged ReplicateResp / RequestVoteResp counted
	//     toward quorum. What the operator gives up is exactly the attack
	//     identities exist to close, and only for members that have not been
	//     given one yet.
	//   - true. That residual is closed: an identity-less member's
	//     quorum-bearing traffic is dropped outright (drop reason
	//     "missing_identity"). What the operator gives up is availability
	//     during migration — a shard whose members have not all had identities
	//     committed will lose quorum, because the responses of the members
	//     without one stop being counted. It is safe to enable ONLY after every
	//     member of every shard on this host has a committed identity.
	//
	// The default is false, and that is a deliberate compatibility choice for
	// a pre-1.0 library rather than a security preference. An identity cannot
	// be inferred, discovered, or learned from a connection (learning it from
	// the peer is unsound in precisely the threat model it addresses — see
	// Host.RequestAddNodeWithIdentity); it must be supplied by an operator and
	// committed through a config change. Defaulting to true would therefore
	// brick every existing cluster on upgrade and every new cluster whose
	// operator has not yet learned the field exists — a fail-closed default
	// that is guaranteed to be switched off rather than satisfied. The
	// migration is documented in docs/operations/compatibility.md.
	RequireMemberIdentity bool
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
	if tc.MaxMessageReceiveMemory == 0 {
		tc.MaxMessageReceiveMemory = DefaultMaxMessageReceiveMemory
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
	if tc.MaxStreamsPerConnection == 0 {
		tc.MaxStreamsPerConnection = DefaultMaxStreamsPerConnection
	}
	if tc.StreamReadIdleTimeout == 0 {
		tc.StreamReadIdleTimeout = DefaultStreamReadIdleTimeout
	}
}

// Validate checks all transport configuration constraints. Returns a
// *ValidationError describing the first violated rule, or nil
// if the configuration is valid.
//
// The three groups are checked in a FIXED order — limits, then mTLS material,
// then revocation — because callers and tests observe the FIRST violated rule.
func (tc *TransportConfig) Validate() error {
	if err := tc.validateLimits(); err != nil {
		return err
	}
	if err := tc.validateMTLS(); err != nil {
		return err
	}
	return tc.validateRevocation()
}

// validateLimits checks the numeric transport limits.
func (tc *TransportConfig) validateLimits() error {
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
	// A non-zero rate below the floor would make the receiver's
	// proportional-sleep rate limiter compute multi-day per-chunk sleeps,
	// holding the snapshot semaphore and memory budget and stalling
	// availability. Zero is accepted (SetDefaults supplies the default).
	if tc.MaxSnapshotReceiveRate > 0 && tc.MaxSnapshotReceiveRate < MinMaxSnapshotReceiveRate {
		return newValidationError("TransportConfig.MaxSnapshotReceiveRate",
			"must be 0 (use default) or >= 64 KB/s")
	}
	if tc.MaxConcurrentSnapshotReceives < 1 {
		return newValidationError("TransportConfig.MaxConcurrentSnapshotReceives",
			"must be >= 1")
	}
	if tc.MaxSnapshotReceiveMemory < 0 {
		return newValidationError("TransportConfig.MaxSnapshotReceiveMemory",
			"must be >= 0")
	}
	if tc.MaxMessageReceiveMemory < 0 {
		return newValidationError("TransportConfig.MaxMessageReceiveMemory",
			"must be >= 0")
	}
	if tc.MaxDecompressedSize != DefaultMaxDecompressedSize {
		return newValidationError("TransportConfig.MaxDecompressedSize",
			"must equal DefaultMaxDecompressedSize (the transport frame size): "+
				"the send path admits frames against that constant, so a lower "+
				"value silently rejects frames every peer considers valid and "+
				"stops replication to this node")
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
	if tc.MaxStreamsPerConnection < 1 {
		return newValidationError("TransportConfig.MaxStreamsPerConnection",
			"must be >= 1")
	}
	if tc.StreamReadIdleTimeout < 0 {
		return newValidationError("TransportConfig.StreamReadIdleTimeout",
			"must be >= 0 (0 uses the default)")
	}
	return nil
}

// validateMTLS checks the mandatory mTLS material.
//
// When transport is enabled, MTLSConfig must be provided. All QUIC connections
// require mTLS; there is no insecure fallback. When TransportDisabled is true
// (set by WithoutTransport()), this validation is skipped.
func (tc *TransportConfig) validateMTLS() error {
	if !tc.TransportDisabled {
		if tc.MTLSConfig == nil {
			return &MTLSConfigRequiredError{}
		}
		// mTLS is mandatory (no insecure fallback), so empty/invalid cert
		// material must be rejected at the config boundary rather than
		// surfacing as an opaque handshake error at runtime.
		mtls := tc.MTLSConfig
		if len(mtls.CACert) == 0 {
			return newValidationError("TransportConfig.MTLSConfig.CACert", "must be a non-empty PEM CA certificate")
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(mtls.CACert) {
			return newValidationError("TransportConfig.MTLSConfig.CACert", "no valid PEM certificate found")
		}
		// The node leaf comes from EITHER a pre-built Certificate (e.g. a
		// hardware-signer-backed key) OR the PEM Cert/Key pair. Reject only when
		// neither is usable.
		if mtls.Certificate != nil {
			if len(mtls.Certificate.Certificate) == 0 {
				return newValidationError("TransportConfig.MTLSConfig.Certificate", "must contain at least one certificate")
			}
			// A pre-built leaf with no private key produces a Certificate that
			// passes the length check yet fails every QUIC handshake at runtime.
			// Require a usable signer here. Accept any crypto.Signer so a
			// hardware-signer-backed key (TPM/PKCS#11) — whose PrivateKey is an
			// opaque signer, not raw key bytes — is still allowed.
			if _, ok := mtls.Certificate.PrivateKey.(crypto.Signer); !ok {
				return newValidationError("TransportConfig.MTLSConfig.Certificate",
					"PrivateKey must be a non-nil crypto.Signer")
			}
		} else {
			if len(mtls.Cert) == 0 {
				return newValidationError("TransportConfig.MTLSConfig.Cert", "must be a non-empty PEM certificate (or set Certificate)")
			}
			if len(mtls.Key) == 0 {
				return newValidationError("TransportConfig.MTLSConfig.Key", "must be a non-empty PEM private key (or set Certificate)")
			}
			// Trial-parse so a malformed keypair is caught here.
			if _, err := tls.X509KeyPair(mtls.Cert, mtls.Key); err != nil {
				return newValidationError("TransportConfig.MTLSConfig", "Cert/Key are not a valid PEM keypair: "+err.Error())
			}
		}
	}
	return nil
}

// validateRevocation checks the optional certificate-revocation configuration.
func (tc *TransportConfig) validateRevocation() error {
	if rc := tc.RevocationConfig; rc != nil {
		// Reject unknown mode strings: a typo ("CRL", "strict") would
		// otherwise silently select the default strategy.
		switch rc.Mode {
		case "", "crl", "ocsp", "both", "any":
		default:
			return newValidationError("TransportConfig.RevocationConfig.Mode",
				`must be one of "crl", "ocsp", "both", "any" (or empty for the default)`)
		}
		// A mode that consults a source requires that source to be
		// configured; otherwise the checker can never produce a verdict.
		usesCRL := rc.Mode == "crl" || rc.Mode == "both"
		usesOCSP := rc.Mode == "ocsp" || rc.Mode == "both"
		if usesCRL && len(rc.CRLPaths) == 0 {
			return newValidationError("TransportConfig.RevocationConfig.CRLPaths",
				`must be non-empty when Mode is "crl" or "both"`)
		}
		if usesOCSP && rc.OCSPResponderURL == "" {
			return newValidationError("TransportConfig.RevocationConfig.OCSPResponderURL",
				`must be non-empty when Mode is "ocsp" or "both"`)
		}
		if rc.OCSPCacheSeconds < 0 {
			return newValidationError("TransportConfig.RevocationConfig.OCSPCacheSeconds",
				"must be >= 0 (0 uses the default)")
		}
		if rc.OCSPTimeoutSeconds < 0 {
			return newValidationError("TransportConfig.RevocationConfig.OCSPTimeoutSeconds",
				"must be >= 0 (0 uses the default)")
		}
		if rc.CheckInterval < 0 {
			return newValidationError("TransportConfig.RevocationConfig.CheckInterval",
				"must be >= 0 (0 uses the default)")
		}
		// The default mode ("any") names no specific source, so the two checks
		// above cannot catch it. Enabling revocation with the fail-closed
		// default and nothing to consult admits EVERY revoked certificate for
		// the process lifetime with no error, no log and no metric — the
		// failure mode revocation exists to prevent. Soft-fail is the
		// documented opt-out and explicitly covers "no revocation source
		// configured", so it is left alone.
		if !usesCRL && !usesOCSP && !rc.AllowSoftFail &&
			len(rc.CRLPaths) == 0 && rc.OCSPResponderURL == "" {
			return newValidationError("TransportConfig.RevocationConfig",
				`must configure CRLPaths or OCSPResponderURL when revocation is enforced (set AllowSoftFail, or leave RevocationConfig nil, to disable revocation checking)`)
		}
	}
	return nil
}
