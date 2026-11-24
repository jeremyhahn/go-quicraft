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
	"math"
	"time"

	"github.com/jeremyhahn/go-quicraft/pkg/proto"
)

// Default values for TransportConfig fields.
const (
	DefaultStreamPoolSize             int    = 16
	DefaultMaxStreamPoolSize          int    = 256
	DefaultMaxSnapshotReceiveRate     int64  = 256 * 1024 * 1024 // 256 MB/s
	DefaultMaxConcurrentSnapshotRecv  int    = 4
	DefaultMaxSnapshotReceiveMemory   int64  = 1024 * 1024 * 1024 // 1 GB
	DefaultMaxDecompressedSize        uint32 = 16 * 1024 * 1024   // 16 MB (transport.MaxFrameSize)
	DefaultMessageReceiveBudgetFrames int    = 4
	DefaultMaxConnectionsPerIP        int    = 16
	DefaultMaxIncomingConnections     int    = 256
	DefaultSendBatchMaxSize           int    = 64 * 1024       // 64 KB
	DefaultUDPRecvBufSize             int    = 7 * 1024 * 1024 // 7 MB (within typical rmem_max)
	DefaultUDPSendBufSize             int    = 7 * 1024 * 1024 // 7 MB (within typical wmem_max)
	DefaultMaxStreamsPerConnection    int    = 256
	DefaultStreamReadIdleTimeout             = 30 * time.Second

	// MinMaxSnapshotReceiveRate is the smallest non-zero MaxSnapshotReceiveRate, keeping per-chunk rate-limit sleeps bounded.
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

	// Certificate, when set, replaces the PEM Cert/Key pair and may hold a hardware crypto.Signer; CACert is still required.
	Certificate *tls.Certificate
}

// RevocationConfig enables revoked-certificate rejection in VerifyConnection (which also runs on resumed sessions); CRL files reload from disk.
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

	// Mode is crl, ocsp, both or any (default); any other value is rejected.
	Mode string

	// AllowSoftFail admits a peer whose revocation status is unknown; the default fails closed. Revoked always rejects.
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

	// MaxStreamPoolSize is the upper bound Validate allows for StreamPoolSize. Default: 256.
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

	// MaxMessageReceiveMemory bounds decoded inbound frame buffers (charged via proto.FrameDecodeCharge); zero derives it from MessageReceiveBudgetFrames.
	MaxMessageReceiveMemory int64

	// MessageReceiveBudgetFrames sizes the default MaxMessageReceiveMemory in maximum-size frames. Default: 4.
	MessageReceiveBudgetFrames int

	// MaxDecompressedSize must equal DefaultMaxDecompressedSize (the transport MaxFrameSize); zero selects it.
	MaxDecompressedSize uint32

	// MaxConnectionsPerIP is the maximum number of QUIC connections
	// allowed from a single source IP. Default: 16.
	MaxConnectionsPerIP int

	// MaxIncomingConnections is the total number of concurrent inbound
	// QUIC connections. Default: 256.
	MaxIncomingConnections int

	// MaxStreamsPerConnection caps stream goroutines per inbound connection. Default: 256.
	MaxStreamsPerConnection int

	// StreamReadIdleTimeout is how long an idle stream read waits before re-checking connection liveness, not a stream lifetime. Default: 30s.
	StreamReadIdleTimeout time.Duration

	// SendBatchMaxSize is the initial pooled send-buffer capacity, a sizing hint only. Default: 64 KB.
	SendBatchMaxSize int

	// UDPRecvBufSize is SO_RCVBUF for the UDP socket. Default: 7 MB; negative keeps the OS default.
	UDPRecvBufSize int

	// UDPSendBufSize is SO_SNDBUF for the UDP socket. Default: 7 MB; negative keeps the OS default.
	UDPSendBufSize int

	// Enable0RTT enables replayable QUIC 0-RTT (RFC 9001 section 8); requires mTLS. Default: off.
	Enable0RTT bool

	// DisableCompression turns off Snappy compression of non-heartbeat messages.
	DisableCompression bool

	// TransportDisabled skips mTLS validation and transport construction, like WithoutTransport().
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
	if tc.MessageReceiveBudgetFrames == 0 {
		tc.MessageReceiveBudgetFrames = DefaultMessageReceiveBudgetFrames
	}
	// Derived after MaxDecompressedSize, which the frame charge depends on. A
	// frame count the budget cannot represent is left for Validate to report.
	if tc.MaxMessageReceiveMemory == 0 && tc.messageReceiveBudgetFramesFit() {
		tc.MaxMessageReceiveMemory = int64(tc.MessageReceiveBudgetFrames) * tc.maxFrameReceiveCharge()
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

// Validate returns a *ValidationError for the first violated rule, checking limits, then mTLS, then revocation.
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
	// A non-zero rate below the floor is rejected; zero takes the default.
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
	if tc.MaxDecompressedSize != DefaultMaxDecompressedSize {
		return newValidationError("TransportConfig.MaxDecompressedSize",
			"must equal DefaultMaxDecompressedSize (the transport frame size): "+
				"the send path admits frames against that constant, so a lower "+
				"value silently rejects frames every peer considers valid and "+
				"stops replication to this node")
	}
	if err := tc.validateMessageReceiveBudget(); err != nil {
		return err
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

// maxFrameReceiveCharge returns the receive-budget charge of one maximum-size frame.
func (tc *TransportConfig) maxFrameReceiveCharge() int64 {
	return proto.FrameDecodeCharge(int64(DefaultMaxDecompressedSize), int64(tc.MaxDecompressedSize))
}

// messageReceiveBudgetFramesFit reports whether MessageReceiveBudgetFrames
// maximum-frame charges are representable as a budget.
func (tc *TransportConfig) messageReceiveBudgetFramesFit() bool {
	return tc.MessageReceiveBudgetFrames >= 0 &&
		int64(tc.MessageReceiveBudgetFrames) <= math.MaxInt64/tc.maxFrameReceiveCharge()
}

// validateMessageReceiveBudget checks the message-receive budget and the
// frame count its default is derived from. Zero for either is accepted:
// SetDefaults supplies it.
func (tc *TransportConfig) validateMessageReceiveBudget() error {
	if !tc.messageReceiveBudgetFramesFit() {
		return newValidationError("TransportConfig.MessageReceiveBudgetFrames",
			"must be >= 0 (0 uses the default) and small enough that that many "+
				"maximum-size frame charges fit in an int64 budget")
	}
	if tc.MaxMessageReceiveMemory < 0 {
		return newValidationError("TransportConfig.MaxMessageReceiveMemory",
			"must be >= 0")
	}
	if minimum := tc.maxFrameReceiveCharge(); tc.MaxMessageReceiveMemory > 0 && tc.MaxMessageReceiveMemory < minimum {
		return &MaxMessageReceiveMemoryTooSmallError{
			Configured: tc.MaxMessageReceiveMemory,
			Minimum:    minimum,
		}
	}
	return nil
}

// validateMTLS requires mTLS material unless TransportDisabled is set.
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
		// The leaf comes from Certificate or the PEM Cert/Key pair; reject only when neither is usable.
		if mtls.Certificate != nil {
			if len(mtls.Certificate.Certificate) == 0 {
				return newValidationError("TransportConfig.MTLSConfig.Certificate", "must contain at least one certificate")
			}
			// A pre-built leaf needs a crypto.Signer private key, or every handshake fails.
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
		// Revocation enabled fail-closed with no source would admit every revoked certificate; soft-fail is the opt-out.
		if !usesCRL && !usesOCSP && !rc.AllowSoftFail &&
			len(rc.CRLPaths) == 0 && rc.OCSPResponderURL == "" {
			return newValidationError("TransportConfig.RevocationConfig",
				`must configure CRLPaths or OCSPResponderURL when revocation is enforced (set AllowSoftFail, or leave RevocationConfig nil, to disable revocation checking)`)
		}
	}
	return nil
}
