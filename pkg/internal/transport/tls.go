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
	"encoding/pem"
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

	// ErrNoPeerCertificate is returned by the revocation VerifyConnection
	// callback when a connection presents no peer certificate. mTLS makes
	// this unreachable, but the callback fails closed rather than
	// accepting an unauthenticated connection.
	ErrNoPeerCertificate = errors.New("transport: connection presented no peer certificate")
)

// tlsALPN is the QUIC ALPN identifier for go-quicraft transport.
const tlsALPN = "quicraft"

// tlsPair holds separate TLS configs for server and client roles. The
// server config is used when accepting connections, and the client config
// when dialing peers. Both use proper CA verification via mTLS.
type tlsPair struct {
	server *tls.Config
	client *tls.Config

	// leaf is this node's OWN parsed end-entity certificate — the same
	// certificate peers see in both the server and client roles. It is kept so
	// the startup exclusivity check (VerifyPeerAddressExclusivity) can ask
	// whether this node's SANs also cover other members' hosts, which would
	// collapse every in-band authorization decision the transport makes.
	leaf *x509.Certificate
}

// parseCACertificates parses every CERTIFICATE block in a PEM CA bundle and
// returns the candidates that can act as an issuing CA. The full bundle is
// required because TLS verification consumes all of it
// (pool.AppendCertsFromPEM), so revocation checking must be able to select
// the same issuer the peer's leaf was actually signed by — in a root +
// issuing-intermediate PKI that is never the bundle's first block.
//
// It fails fast when the bundle contains no block that could issue a
// certificate: with no usable issuer, OCSP requests would name the wrong CA
// (a permanent silent no-op under soft-fail, a total handshake outage under
// enforcement) and every CRL signature check would fail.
func parseCACertificates(caPEM []byte) ([]*x509.Certificate, error) {
	var issuers []*x509.Certificate
	rest := caPEM
	blocks := 0
	for {
		var block *pem.Block
		block, rest = pem.Decode(rest)
		if block == nil {
			break
		}
		if block.Type != "CERTIFICATE" {
			continue
		}
		blocks++
		cert, err := x509.ParseCertificate(block.Bytes)
		if err != nil {
			return nil, &CACertificateParseError{
				Reason: "x509.ParseCertificate failed",
				Err:    err,
			}
		}
		if cert.IsCA || cert.KeyUsage&x509.KeyUsageCertSign != 0 {
			issuers = append(issuers, cert)
		}
	}
	if blocks == 0 {
		return nil, &CACertificateParseError{
			Reason: "PEM decode yielded no CERTIFICATE block",
		}
	}
	if len(issuers) == 0 {
		return nil, &CACertificateParseError{
			Reason: "CA bundle contains no certificate able to act as an issuer",
		}
	}
	return issuers, nil
}

// buildMTLSTLS builds a TLS pair from provided mTLS certificate material.
// Both server and client verify peers using the configured CA. When a
// revocation checker is provided, a VerifyConnection callback is installed on
// both server and client configs to reject revoked peer certificates. The
// callback is deliberately VerifyConnection and not VerifyPeerCertificate:
// the latter is skipped on resumed TLS sessions, which would let a revoked
// peer keep reconnecting via session resumption.
func buildMTLSTLS(mtls *MTLSConfig, checker *revocation.Checker) (*tlsPair, error) {
	// The node leaf comes from a pre-built Certificate when supplied (e.g. a
	// hardware-signer-backed key that cannot be exported to PEM), otherwise
	// from the PEM Cert/Key pair.
	var cert tls.Certificate
	if mtls.Certificate != nil {
		cert = *mtls.Certificate
	} else {
		var err error
		cert, err = tls.X509KeyPair(mtls.Cert, mtls.Key)
		if err != nil {
			return nil, ErrInvalidCertKeyPair
		}
	}
	// Parse this node's own leaf once. tls.X509KeyPair populates Leaf, but a
	// pre-built Certificate (hardware signer) may not, so fall back to parsing
	// the DER. A certificate whose leaf cannot be parsed is rejected outright:
	// the startup SAN-exclusivity precondition cannot be evaluated without it.
	leaf := cert.Leaf
	if leaf == nil {
		if len(cert.Certificate) == 0 {
			return nil, ErrInvalidCertKeyPair
		}
		parsed, parseErr := x509.ParseCertificate(cert.Certificate[0])
		if parseErr != nil {
			return nil, ErrInvalidCertKeyPair
		}
		leaf = parsed
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
		onRevoked := checker.OnCertRevoked()
		verifyConn = func(cs tls.ConnectionState) error {
			// Only revocation-check the LEAF certificate (PeerCertificates[0]).
			// The leaf is the end-entity cert that authenticates the peer; it is
			// the certificate that can be individually revoked when a node is
			// decommissioned or compromised. Checking the entire presented chain
			// (intermediates, root) on every handshake added per-handshake OCSP
			// latency for certificates that change rarely and are governed by the
			// CA's own lifecycle rather than per-node revocation. The CA trust
			// pool already validated the chain before VerifyConnection runs.
			//
			// On fresh handshakes PeerCertificates contains the verified chain;
			// on resumed sessions it still contains the peer's certificates from
			// the original handshake, so leaf checking works on both paths.
			//
			// Fail CLOSED on an empty peer-certificate set. With mTLS
			// (RequireAndVerifyClientCert + RootCAs on both ends) this is
			// unreachable, but returning nil here would mean "no cert ⇒
			// accept", a latent fail-open if the TLS config ever changed.
			// A connection with no presented certificate cannot be
			// revocation-checked and must be rejected.
			if len(cs.PeerCertificates) == 0 {
				return ErrNoPeerCertificate
			}
			leaf := cs.PeerCertificates[0]
			revoked, checkErr := checker.IsRevoked(leaf)
			if checkErr != nil {
				return checkErr
			}
			if revoked {
				serial := leaf.SerialNumber.Text(16)
				if onRevoked != nil {
					onRevoked(serial)
				}
				return &revocation.CertificateRevokedError{Serial: serial}
			}
			return nil
		}
	}

	// Naming the key-exchange groups explicitly is what suppresses ML-KEM
	// (post-quantum hybrid) negotiation and its ~5% handshake cost, which
	// intra-cluster traffic between endpoints we control does not need.
	//
	// WHICH groups is build-dependent, because X25519 is not FIPS 140-3
	// approved and this list used to hold nothing else: every intra-cluster
	// handshake — which carries all replicated user data — failed with
	// `tls: no supported elliptic curves for ECDHE` under GODEBUG=fips140=on
	// as well as =only, so the library could not run inside a FIPS process at
	// all. A `-tags fips` build compiles the non-approved group out
	// (tls_curves_fips.go); a standard build keeps it and adds the NIST curves
	// so the two builds can still handshake with each other
	// (tls_curves_nofips.go).
	curves := handshakeCurves()

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

	return &tlsPair{server: serverCfg, client: clientCfg, leaf: leaf}, nil
}
