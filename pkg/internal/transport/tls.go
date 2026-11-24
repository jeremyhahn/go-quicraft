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

	// ErrNoPeerCertificate is returned by the revocation VerifyConnection callback for a connection with no peer certificate.
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

	// leaf is this node's own parsed end-entity certificate, kept for VerifyPeerAddressExclusivity.
	leaf *x509.Certificate
}

// parseCACertificates returns every certificate in a PEM CA bundle that can act as an issuer, failing when none can.
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

// buildMTLSTLS builds the server and client mTLS configs against the CA, installing a revocation VerifyConnection callback (also run on resumed sessions) when a checker is given.
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
	// Parse this node's leaf, from DER when Leaf is unset; an unparsable leaf is rejected.
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

	// Revocation runs in VerifyConnection, which also covers resumed sessions.
	var verifyConn func(tls.ConnectionState) error
	if checker != nil {
		// Resolve issuers against this pair's own bundle snapshot, not the checker's latest reload.
		issuers, err := parseCACertificates(mtls.CACert)
		if err != nil {
			return nil, err
		}
		onRevoked := checker.OnCertRevoked()
		verifyConn = func(cs tls.ConnectionState) error {
			// Revocation-check only the leaf (PeerCertificates[0]); fail closed when no certificate is presented.
			if len(cs.PeerCertificates) == 0 {
				return ErrNoPeerCertificate
			}
			leaf := cs.PeerCertificates[0]
			revoked, checkErr := checker.IsRevokedUnder(leaf, issuers)
			if checkErr != nil {
				return checkErr
			}
			if revoked {
				serial := leaf.SerialNumber.Text(16)
				if onRevoked != nil {
					// The notification is isolated (deliverEvent) and cannot affect the handshake or the rejection.
					deliverEvent(func() { onRevoked(serial) })
				}
				return &revocation.CertificateRevokedError{Serial: serial}
			}
			return nil
		}
	}

	// Key-exchange groups are explicit, post-quantum hybrids first (RFC 10024, FIPS 203 ML-KEM); the list is build-dependent (tls_curves_fips.go, tls_curves_nofips.go).
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
	// Seal session tickets with this config's own rotating keys, so tickets from before ReloadMTLS do not resume.
	serverCfg.WrapSession = func(cs tls.ConnectionState, ss *tls.SessionState) ([]byte, error) {
		return serverCfg.EncryptTicket(cs, ss)
	}
	serverCfg.UnwrapSession = func(identity []byte, cs tls.ConnectionState) (*tls.SessionState, error) {
		return serverCfg.DecryptTicket(identity, cs)
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
