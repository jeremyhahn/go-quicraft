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
	"crypto/tls"
	"crypto/x509"
	"errors"

	"github.com/jeremyhahn/go-quicraft/pkg/internal/transport/revocation"
)

// TLS configuration errors.
var (
	// ErrInvalidCACert is returned when the CA certificate PEM cannot be parsed.
	ErrInvalidCACert = errors.New("transport: invalid CA certificate")

	// ErrInvalidCertKeyPair is returned when the certificate and key PEM pair
	// cannot be parsed.
	ErrInvalidCertKeyPair = errors.New("transport: invalid certificate/key pair")
)

// tlsALPN is the QUIC ALPN identifier for go-quicraft transport.
const tlsALPN = "quicraft"

// tlsPair holds separate TLS configs for server and client roles. The
// server config is used when accepting connections, and the client config
// when dialing peers. Both use proper CA verification via mTLS.
type tlsPair struct {
	server *tls.Config
	client *tls.Config
}

// buildMTLSTLS builds a TLS pair from provided mTLS certificate material.
// Both server and client verify peers using the configured CA. When a
// revocation checker is provided, a VerifyPeerCertificate callback is
// installed on both server and client configs to reject revoked peer
// certificates during the TLS handshake.
func buildMTLSTLS(mtls *MTLSConfig, checker *revocation.Checker) (*tlsPair, error) {
	cert, err := tls.X509KeyPair(mtls.Cert, mtls.Key)
	if err != nil {
		return nil, ErrInvalidCertKeyPair
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(mtls.CACert) {
		return nil, ErrInvalidCACert
	}

	// Build the VerifyConnection callback when revocation checking is enabled.
	// VerifyConnection runs on ALL connections including resumed sessions,
	// unlike VerifyPeerCertificate which is skipped on resumed sessions.
	// This ensures revocation checks happen even for TLS session resumption.
	var verifyConn func(tls.ConnectionState) error
	if checker != nil {
		cfg := checker.CfgRef()
		verifyConn = func(cs tls.ConnectionState) error {
			// Check all peer certificates for revocation. On fresh handshakes,
			// PeerCertificates contains the verified chain. On resumed sessions,
			// it still contains the peer's certificates from the original handshake.
			for _, cert := range cs.PeerCertificates {
				revoked, checkErr := checker.IsRevoked(cert)
				if checkErr != nil {
					return checkErr
				}
				if revoked {
					serial := cert.SerialNumber.Text(16)
					if cfg.OnCertRevoked != nil {
						cfg.OnCertRevoked(serial)
					}
					return &revocation.CertificateRevokedError{Serial: serial}
				}
			}
			return nil
		}
	}

	// Use X25519-only key exchange to avoid the ~5% overhead of ML-KEM
	// (post-quantum hybrid) negotiation. Intra-cluster traffic where we
	// control both endpoints does not need post-quantum protection.
	// X25519 provides 128-bit security and is the fastest ECDH curve.
	curves := []tls.CurveID{tls.X25519}

	serverCfg := &tls.Config{
		Certificates:     []tls.Certificate{cert},
		RootCAs:          pool,
		ClientCAs:        pool,
		ClientAuth:       tls.RequireAndVerifyClientCert,
		NextProtos:       []string{tlsALPN},
		MinVersion:       tls.VersionTLS13,
		CurvePreferences: curves,
		VerifyConnection: verifyConn,
	}

	clientCfg := &tls.Config{
		Certificates:     []tls.Certificate{cert},
		RootCAs:          pool,
		NextProtos:       []string{tlsALPN},
		MinVersion:       tls.VersionTLS13,
		CurvePreferences: curves,
		VerifyConnection: verifyConn,
		// Session cache enables TLS session ticket resumption, reducing
		// reconnect handshakes from 2 RTTs to 1 RTT. The LRU cache size
		// of 256 covers typical cluster sizes with headroom for churn.
		ClientSessionCache: tls.NewLRUClientSessionCache(256),
	}

	return &tlsPair{server: serverCfg, client: clientCfg}, nil
}
