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
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"errors"
	"math/big"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jeremyhahn/go-quicraft/pkg/internal/registry"
	"github.com/jeremyhahn/go-quicraft/pkg/internal/transport/revocation"
)

// TestBuildMTLSTLSPairValid verifies that buildMTLSTLS produces configs
// with proper CA verification.
func TestBuildMTLSTLSPairValid(t *testing.T) {
	caCertPEM, _, certPEM, keyPEM := generateTestCertPair(t)

	pair, err := buildMTLSTLS(&MTLSConfig{
		CACert: caCertPEM,
		Cert:   certPEM,
		Key:    keyPEM,
	}, nil)
	if err != nil {
		t.Fatalf("buildMTLSTLS failed: %v", err)
	}

	if pair.server.ClientAuth != tls.RequireAndVerifyClientCert {
		t.Fatalf("server should require client certs, got %v", pair.server.ClientAuth)
	}
	if pair.server.ClientCAs == nil {
		t.Fatal("server should have ClientCAs")
	}
	if pair.client.RootCAs == nil {
		t.Fatal("client should have RootCAs")
	}
	if pair.client.InsecureSkipVerify {
		t.Fatal("client should NOT have InsecureSkipVerify in mTLS mode")
	}
}

// TestBuildMTLSTLSServerConfig verifies mTLS config with real PEM certificates.
func TestBuildMTLSTLSServerConfig(t *testing.T) {
	caCertPEM, _, certPEM, keyPEM := generateTestCertPair(t)

	mtls := &MTLSConfig{
		CACert: caCertPEM,
		Cert:   certPEM,
		Key:    keyPEM,
	}

	pair, err := buildMTLSTLS(mtls, nil)
	if err != nil {
		t.Fatalf("buildMTLSTLS failed: %v", err)
	}
	cfg := pair.server

	if len(cfg.Certificates) != 1 {
		t.Fatalf("expected 1 certificate, got %d", len(cfg.Certificates))
	}
	if cfg.ClientAuth != tls.RequireAndVerifyClientCert {
		t.Fatalf("expected RequireAndVerifyClientCert, got %v", cfg.ClientAuth)
	}
	if cfg.MinVersion != tls.VersionTLS13 {
		t.Fatalf("expected TLS 1.3, got %d", cfg.MinVersion)
	}
	if cfg.RootCAs == nil {
		t.Fatal("RootCAs should not be nil")
	}
}

// TestBuildMTLSTLSClientConfig verifies mTLS client config has proper
// CA verification and ALPN protocol.
func TestBuildMTLSTLSClientConfig(t *testing.T) {
	caCertPEM, _, certPEM, keyPEM := generateTestCertPair(t)

	pair, err := buildMTLSTLS(&MTLSConfig{
		CACert: caCertPEM,
		Cert:   certPEM,
		Key:    keyPEM,
	}, nil)
	if err != nil {
		t.Fatalf("buildMTLSTLS failed: %v", err)
	}
	cfg := pair.client

	if len(cfg.Certificates) != 1 {
		t.Fatalf("expected 1 certificate, got %d", len(cfg.Certificates))
	}
	if cfg.RootCAs == nil {
		t.Fatal("client RootCAs should not be nil")
	}
	if cfg.MinVersion != tls.VersionTLS13 {
		t.Fatalf("expected TLS 1.3, got %d", cfg.MinVersion)
	}
	foundALPN := false
	for _, proto := range cfg.NextProtos {
		if proto == tlsALPN {
			foundALPN = true
			break
		}
	}
	if !foundALPN {
		t.Fatalf("NextProtos should contain %q", tlsALPN)
	}
}

// TestBuildMTLSTLSInvalidCACert verifies rejection of invalid CA cert.
func TestBuildMTLSTLSInvalidCACert(t *testing.T) {
	_, _, certPEM, keyPEM := generateTestCertPair(t)

	mtls := &MTLSConfig{
		CACert: []byte("not a valid PEM"),
		Cert:   certPEM,
		Key:    keyPEM,
	}

	_, err := buildMTLSTLS(mtls, nil)
	if !errors.Is(err, ErrInvalidCACert) {
		t.Fatalf("expected ErrInvalidCACert, got %v", err)
	}
}

// TestBuildMTLSTLSInvalidCertKeyPair verifies rejection of invalid cert/key.
func TestBuildMTLSTLSInvalidCertKeyPair(t *testing.T) {
	mtls := &MTLSConfig{
		CACert: []byte("-----BEGIN CERTIFICATE-----\nfoo\n-----END CERTIFICATE-----"),
		Cert:   []byte("not-pem"),
		Key:    []byte("not-pem"),
	}

	_, err := buildMTLSTLS(mtls, nil)
	if !errors.Is(err, ErrInvalidCertKeyPair) {
		t.Fatalf("expected ErrInvalidCertKeyPair, got %v", err)
	}
}

// TestTLSALPNConstant verifies the ALPN protocol identifier.
func TestTLSALPNConstant(t *testing.T) {
	if tlsALPN != "quicraft" {
		t.Fatalf("tlsALPN: want quicraft, got %s", tlsALPN)
	}
}

// TestNilMTLSConfigReturnsError verifies that NewQUICTransport returns
// ErrMTLSConfigRequired when MTLSConfig is nil.
func TestNilMTLSConfigReturnsError(t *testing.T) {
	handler := newTestHandler()
	reg := registry.NewRegistry()
	cfg := Config{
		ListenAddress: ":0",
		DeploymentID:  42,
		MTLSConfig:    nil,
	}
	cfg.SetDefaults()

	err := cfg.Validate()
	if !errors.Is(err, ErrMTLSConfigRequired) {
		t.Fatalf("expected ErrMTLSConfigRequired from Validate, got %v", err)
	}

	_, err = NewQUICTransport(cfg, handler, reg)
	if !errors.Is(err, ErrMTLSConfigRequired) {
		t.Fatalf("expected ErrMTLSConfigRequired from NewQUICTransport, got %v", err)
	}
}

// TestBuildMTLSTLSServerALPN verifies the server config has the ALPN protocol.
func TestBuildMTLSTLSServerALPN(t *testing.T) {
	caCertPEM, _, certPEM, keyPEM := generateTestCertPair(t)

	pair, err := buildMTLSTLS(&MTLSConfig{
		CACert: caCertPEM,
		Cert:   certPEM,
		Key:    keyPEM,
	}, nil)
	if err != nil {
		t.Fatalf("buildMTLSTLS failed: %v", err)
	}

	foundALPN := false
	for _, proto := range pair.server.NextProtos {
		if proto == tlsALPN {
			foundALPN = true
			break
		}
	}
	if !foundALPN {
		t.Fatalf("server NextProtos should contain %q", tlsALPN)
	}
}

// generateTestCertPair creates a CA and a leaf certificate signed by it.
// Returns (caCertPEM, caKeyPEM, leafCertPEM, leafKeyPEM).
func generateTestCertPair(t *testing.T) ([]byte, []byte, []byte, []byte) {
	t.Helper()

	// Generate CA key and cert.
	caKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("failed to generate CA key: %v", err)
	}

	caTemplate := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject: pkix.Name{
			Organization: []string{"Test CA"},
		},
		NotBefore:             time.Now().Add(-1 * time.Minute),
		NotAfter:              time.Now().Add(1 * time.Hour),
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageCRLSign,
		BasicConstraintsValid: true,
		IsCA:                  true,
	}

	caCertDER, err := x509.CreateCertificate(rand.Reader, caTemplate, caTemplate, &caKey.PublicKey, caKey)
	if err != nil {
		t.Fatalf("failed to create CA cert: %v", err)
	}

	caCert, err := x509.ParseCertificate(caCertDER)
	if err != nil {
		t.Fatalf("failed to parse CA cert: %v", err)
	}

	caCertPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: caCertDER})
	caKeyDER, _ := x509.MarshalECPrivateKey(caKey)
	caKeyPEM := pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: caKeyDER})

	// Generate leaf key and cert signed by CA.
	leafKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("failed to generate leaf key: %v", err)
	}

	leafTemplate := &x509.Certificate{
		SerialNumber: big.NewInt(2),
		Subject: pkix.Name{
			Organization: []string{"Test Node"},
			CommonName:   "localhost",
		},
		DNSNames:              []string{"localhost"},
		IPAddresses:           []net.IP{net.IPv4(127, 0, 0, 1), net.IPv6loopback, net.IPv6zero},
		NotBefore:             time.Now().Add(-1 * time.Minute),
		NotAfter:              time.Now().Add(1 * time.Hour),
		KeyUsage:              x509.KeyUsageDigitalSignature,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
		BasicConstraintsValid: true,
	}

	leafCertDER, err := x509.CreateCertificate(rand.Reader, leafTemplate, caCert, &leafKey.PublicKey, caKey)
	if err != nil {
		t.Fatalf("failed to create leaf cert: %v", err)
	}

	leafCertPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: leafCertDER})
	leafKeyDER, _ := x509.MarshalECPrivateKey(leafKey)
	leafKeyPEM := pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: leafKeyDER})

	return caCertPEM, caKeyPEM, leafCertPEM, leafKeyPEM
}

// ---------------------------------------------------------------------------
// S2: VerifyConnection revocation callback tests
// ---------------------------------------------------------------------------

// TestBuildMTLSTLSNilCheckerNoCallback verifies that when no revocation
// checker is provided, VerifyConnection is nil on both server and
// client configs.
func TestBuildMTLSTLSNilCheckerNoCallback(t *testing.T) {
	caCertPEM, _, certPEM, keyPEM := generateTestCertPair(t)

	pair, err := buildMTLSTLS(&MTLSConfig{
		CACert: caCertPEM,
		Cert:   certPEM,
		Key:    keyPEM,
	}, nil)
	if err != nil {
		t.Fatalf("buildMTLSTLS failed: %v", err)
	}
	if pair.server.VerifyConnection != nil {
		t.Fatal("server VerifyConnection should be nil without checker")
	}
	if pair.client.VerifyConnection != nil {
		t.Fatal("client VerifyConnection should be nil without checker")
	}
}

// TestBuildMTLSTLSWithCheckerInstallsCallback verifies that a revocation
// checker causes VerifyConnection to be set on both configs.
func TestBuildMTLSTLSWithCheckerInstallsCallback(t *testing.T) {
	caCertPEM, _, certPEM, keyPEM := generateTestCertPair(t)

	// Parse the CA cert for the checker.
	block, _ := pem.Decode(caCertPEM)
	caCert, err := x509.ParseCertificate(block.Bytes)
	if err != nil {
		t.Fatalf("parse CA cert: %v", err)
	}

	checker := revocation.NewChecker(revocation.Config{
		Mode: revocation.ModeCRL,
	}, caCert)

	pair, err := buildMTLSTLS(&MTLSConfig{
		CACert: caCertPEM,
		Cert:   certPEM,
		Key:    keyPEM,
	}, checker)
	if err != nil {
		t.Fatalf("buildMTLSTLS failed: %v", err)
	}
	if pair.server.VerifyConnection == nil {
		t.Fatal("server VerifyConnection should be set with checker")
	}
	if pair.client.VerifyConnection == nil {
		t.Fatal("client VerifyConnection should be set with checker")
	}
}

// TestVerifyConnectionRejectsRevoked verifies that the callback
// rejects a revoked certificate with the correct error type.
func TestVerifyConnectionRejectsRevoked(t *testing.T) {
	caCertPEM, _, certPEM, keyPEM := generateTestCertPair(t)

	block, _ := pem.Decode(caCertPEM)
	caCert, _ := x509.ParseCertificate(block.Bytes)

	checker := revocation.NewChecker(revocation.Config{
		Mode: revocation.ModeCRL,
	}, caCert)

	// Parse the leaf cert and add its serial to the revoked set.
	leafBlock, _ := pem.Decode(certPEM)
	leafCert, _ := x509.ParseCertificate(leafBlock.Bytes)
	serial := leafCert.SerialNumber.Text(16)
	checker.AddRevokedSerial(serial)

	pair, err := buildMTLSTLS(&MTLSConfig{
		CACert: caCertPEM,
		Cert:   certPEM,
		Key:    keyPEM,
	}, checker)
	if err != nil {
		t.Fatalf("buildMTLSTLS failed: %v", err)
	}

	// Simulate the TLS connection state with the leaf cert.
	cs := tls.ConnectionState{
		PeerCertificates: []*x509.Certificate{leafCert},
	}
	cbErr := pair.server.VerifyConnection(cs)
	if cbErr == nil {
		t.Fatal("VerifyConnection should reject revoked cert")
	}
	if !errors.Is(cbErr, revocation.ErrCertificateRevoked) {
		t.Fatalf("expected ErrCertificateRevoked, got %v", cbErr)
	}
}

// TestVerifyConnectionAcceptsGood verifies that the callback
// accepts a non-revoked certificate.
func TestVerifyConnectionAcceptsGood(t *testing.T) {
	caCertPEM, _, certPEM, keyPEM := generateTestCertPair(t)

	block, _ := pem.Decode(caCertPEM)
	caCert, _ := x509.ParseCertificate(block.Bytes)

	checker := revocation.NewChecker(revocation.Config{
		Mode: revocation.ModeCRL,
	}, caCert)
	// No serials added -- all certs are good.

	pair, err := buildMTLSTLS(&MTLSConfig{
		CACert: caCertPEM,
		Cert:   certPEM,
		Key:    keyPEM,
	}, checker)
	if err != nil {
		t.Fatalf("buildMTLSTLS failed: %v", err)
	}

	leafBlock, _ := pem.Decode(certPEM)
	leafCert, _ := x509.ParseCertificate(leafBlock.Bytes)

	cs := tls.ConnectionState{
		PeerCertificates: []*x509.Certificate{leafCert},
	}
	if cbErr := pair.server.VerifyConnection(cs); cbErr != nil {
		t.Fatalf("VerifyConnection should accept good cert, got: %v", cbErr)
	}
}

// TestVerifyConnectionEmptyPeerCerts verifies that the callback
// handles empty peer certificates gracefully.
func TestVerifyConnectionEmptyPeerCerts(t *testing.T) {
	caCertPEM, _, certPEM, keyPEM := generateTestCertPair(t)

	block, _ := pem.Decode(caCertPEM)
	caCert, _ := x509.ParseCertificate(block.Bytes)

	checker := revocation.NewChecker(revocation.Config{
		Mode: revocation.ModeCRL,
	}, caCert)

	pair, err := buildMTLSTLS(&MTLSConfig{
		CACert: caCertPEM,
		Cert:   certPEM,
		Key:    keyPEM,
	}, checker)
	if err != nil {
		t.Fatalf("buildMTLSTLS failed: %v", err)
	}

	// Empty PeerCertificates should not cause a panic.
	cs := tls.ConnectionState{PeerCertificates: nil}
	if cbErr := pair.server.VerifyConnection(cs); cbErr != nil {
		t.Fatalf("nil PeerCertificates should pass: %v", cbErr)
	}

	cs = tls.ConnectionState{PeerCertificates: []*x509.Certificate{}}
	if cbErr := pair.server.VerifyConnection(cs); cbErr != nil {
		t.Fatalf("empty PeerCertificates should pass: %v", cbErr)
	}
}

// TestVerifyConnectionCallbackInvokesOnCertRevoked verifies that
// the OnCertRevoked callback is called when a revoked cert is detected.
func TestVerifyConnectionCallbackInvokesOnCertRevoked(t *testing.T) {
	caCertPEM, _, certPEM, keyPEM := generateTestCertPair(t)

	block, _ := pem.Decode(caCertPEM)
	caCert, _ := x509.ParseCertificate(block.Bytes)

	var callbackCalled atomic.Bool
	var callbackSerial atomic.Value

	checker := revocation.NewChecker(revocation.Config{
		Mode: revocation.ModeCRL,
		OnCertRevoked: func(serial string) {
			callbackCalled.Store(true)
			callbackSerial.Store(serial)
		},
	}, caCert)

	leafBlock, _ := pem.Decode(certPEM)
	leafCert, _ := x509.ParseCertificate(leafBlock.Bytes)
	serial := leafCert.SerialNumber.Text(16)
	checker.AddRevokedSerial(serial)

	pair, err := buildMTLSTLS(&MTLSConfig{
		CACert: caCertPEM,
		Cert:   certPEM,
		Key:    keyPEM,
	}, checker)
	if err != nil {
		t.Fatalf("buildMTLSTLS failed: %v", err)
	}

	cs := tls.ConnectionState{
		PeerCertificates: []*x509.Certificate{leafCert},
	}
	_ = pair.server.VerifyConnection(cs)

	if !callbackCalled.Load() {
		t.Fatal("OnCertRevoked callback should have been called")
	}
	if callbackSerial.Load().(string) != serial {
		t.Fatalf("OnCertRevoked serial: want %q, got %q", serial, callbackSerial.Load())
	}
}

// TestBuildMTLSTLS_CurvePreferences verifies that buildMTLSTLS sets
// CurvePreferences to X25519-only on both server and client configs.
// This avoids the ~5% overhead of ML-KEM post-quantum hybrid negotiation.
func TestBuildMTLSTLS_CurvePreferences(t *testing.T) {
	caCertPEM, _, certPEM, keyPEM := generateTestCertPair(t)

	pair, err := buildMTLSTLS(&MTLSConfig{
		CACert: caCertPEM,
		Cert:   certPEM,
		Key:    keyPEM,
	}, nil)
	if err != nil {
		t.Fatalf("buildMTLSTLS failed: %v", err)
	}

	for _, tc := range []struct {
		name   string
		curves []tls.CurveID
	}{
		{"server", pair.server.CurvePreferences},
		{"client", pair.client.CurvePreferences},
	} {
		if len(tc.curves) != 1 {
			t.Fatalf("%s: expected 1 curve preference, got %d", tc.name, len(tc.curves))
		}
		if tc.curves[0] != tls.X25519 {
			t.Fatalf("%s: expected X25519, got %v", tc.name, tc.curves[0])
		}
	}
}

// TestBuildMTLSTLS_ClientSessionCache verifies that the client TLS config
// has a non-nil ClientSessionCache for session ticket resumption (1-RTT
// reconnects instead of 2-RTT full handshakes).
func TestBuildMTLSTLS_ClientSessionCache(t *testing.T) {
	caCertPEM, _, certPEM, keyPEM := generateTestCertPair(t)

	pair, err := buildMTLSTLS(&MTLSConfig{
		CACert: caCertPEM,
		Cert:   certPEM,
		Key:    keyPEM,
	}, nil)
	if err != nil {
		t.Fatalf("buildMTLSTLS failed: %v", err)
	}

	if pair.client.ClientSessionCache == nil {
		t.Fatal("client ClientSessionCache should be non-nil for session resumption")
	}

	// Server should NOT have a ClientSessionCache (only clients resume).
	if pair.server.ClientSessionCache != nil {
		t.Fatal("server ClientSessionCache should be nil")
	}
}
