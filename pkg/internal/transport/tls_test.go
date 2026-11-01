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
	if pair.server.RootCAs == nil {
		t.Fatal("server RootCAs should not be nil")
	}
	if pair.client.RootCAs == nil {
		t.Fatal("client should have RootCAs")
	}
	if pair.client.InsecureSkipVerify {
		t.Fatal("client should NOT have InsecureSkipVerify in mTLS mode")
	}
	for _, tc := range []struct {
		name string
		cfg  *tls.Config
	}{
		{"server", pair.server},
		{"client", pair.client},
	} {
		if len(tc.cfg.Certificates) != 1 {
			t.Fatalf("%s: expected 1 certificate, got %d", tc.name, len(tc.cfg.Certificates))
		}
		if tc.cfg.MinVersion != tls.VersionTLS13 {
			t.Fatalf("%s: expected TLS 1.3, got %d", tc.name, tc.cfg.MinVersion)
		}
		foundALPN := false
		for _, proto := range tc.cfg.NextProtos {
			if proto == tlsALPN {
				foundALPN = true
				break
			}
		}
		if !foundALPN {
			t.Fatalf("%s: NextProtos should contain %q", tc.name, tlsALPN)
		}
	}
	if pair.client.ClientSessionCache == nil {
		t.Fatal("client ClientSessionCache should be non-nil for session resumption")
	}
	// Only clients resume sessions.
	if pair.server.ClientSessionCache != nil {
		t.Fatal("server ClientSessionCache should be nil")
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

	_, err := NewQUICTransport(cfg, handler, reg)
	if !errors.Is(err, ErrMTLSConfigRequired) {
		t.Fatalf("expected ErrMTLSConfigRequired from NewQUICTransport, got %v", err)
	}
}

// generateTestCertPair creates a CA and a leaf certificate signed by it.
// Returns (caCertPEM, caKeyPEM, leafCertPEM, leafKeyPEM). It takes testing.TB
// so benchmarks can build the same mTLS material as tests.
func generateTestCertPair(t testing.TB) ([]byte, []byte, []byte, []byte) {
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
// VerifyConnection revocation callback tests
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

// TestBuildMTLSTLSWithPrebuiltCertificate verifies the signer seam: when an
// MTLSConfig supplies a pre-built tls.Certificate (e.g. one whose PrivateKey is
// a hardware-backed crypto.Signer for a TPM/HSM-resident key) and no PEM
// Cert/Key, buildMTLSTLS uses it directly on both server and client configs.
func TestBuildMTLSTLSWithPrebuiltCertificate(t *testing.T) {
	caCertPEM, _, certPEM, keyPEM := generateTestCertPair(t)

	prebuilt, err := tls.X509KeyPair(certPEM, keyPEM)
	if err != nil {
		t.Fatalf("X509KeyPair: %v", err)
	}

	// CACert is supplied; Cert/Key are intentionally omitted — the leaf comes
	// from the pre-built Certificate.
	pair, err := buildMTLSTLS(&MTLSConfig{
		CACert:      caCertPEM,
		Certificate: &prebuilt,
	}, nil)
	if err != nil {
		t.Fatalf("buildMTLSTLS with prebuilt certificate: %v", err)
	}
	if len(pair.server.Certificates) != 1 || len(pair.client.Certificates) != 1 {
		t.Fatal("expected the prebuilt certificate on both server and client configs")
	}
	if len(pair.server.Certificates[0].Certificate) == 0 ||
		string(pair.server.Certificates[0].Certificate[0]) != string(prebuilt.Certificate[0]) {
		t.Error("server config did not use the prebuilt certificate")
	}
}

// TestConfigValidateAcceptsPrebuiltCertificate verifies the internal Config
// validation accepts an MTLSConfig that carries a pre-built Certificate (and a
// CACert) with no PEM Cert/Key, and still rejects one missing both.
func TestConfigValidateAcceptsPrebuiltCertificate(t *testing.T) {
	caCertPEM, _, certPEM, keyPEM := generateTestCertPair(t)
	prebuilt, err := tls.X509KeyPair(certPEM, keyPEM)
	if err != nil {
		t.Fatalf("X509KeyPair: %v", err)
	}

	cfg := Config{ListenAddress: ":0", MTLSConfig: &MTLSConfig{CACert: caCertPEM, Certificate: &prebuilt}}
	cfg.SetDefaults()
	if err := cfg.Validate(); err != nil {
		t.Fatalf("Validate with prebuilt certificate: %v", err)
	}

	// Neither a Certificate nor a PEM keypair: must fail.
	bad := Config{ListenAddress: ":0", MTLSConfig: &MTLSConfig{CACert: caCertPEM}}
	bad.SetDefaults()
	if err := bad.Validate(); !errors.Is(err, ErrIncompleteMTLSConfig) {
		t.Fatalf("Validate without leaf material = %v, want ErrIncompleteMTLSConfig", err)
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

	// No CRL file is configured, so soft-fail is what makes "no serial added"
	// an admission: an enforcing checker with nothing to consult rejects.
	checker := revocation.NewChecker(revocation.Config{
		Mode:          revocation.ModeCRL,
		AllowSoftFail: true,
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

	// Empty PeerCertificates must FAIL CLOSED: a connection presenting no
	// certificate cannot be revocation-checked and must be rejected
	// rather than silently accepted (a latent fail-open).
	cs := tls.ConnectionState{PeerCertificates: nil}
	if cbErr := pair.server.VerifyConnection(cs); !errors.Is(cbErr, ErrNoPeerCertificate) {
		t.Fatalf("nil PeerCertificates: got %v, want ErrNoPeerCertificate", cbErr)
	}

	cs = tls.ConnectionState{PeerCertificates: []*x509.Certificate{}}
	if cbErr := pair.server.VerifyConnection(cs); !errors.Is(cbErr, ErrNoPeerCertificate) {
		t.Fatalf("empty PeerCertificates: got %v, want ErrNoPeerCertificate", cbErr)
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

// TestBuildMTLSTLS_CurvePreferences verifies that buildMTLSTLS pins
// CurvePreferences on BOTH roles — an unpinned list negotiates ML-KEM hybrid
// key exchange and pays ~5% per handshake — and that what it pins is the list
// this build is allowed to offer.
//
// WHICH list that is depends on the `fips` build tag, so the judgement lives in
// assertHandshakeCurves (tls_curves_fips_test.go / tls_curves_nofips_test.go).
// Asserting it here against tls.X25519 is what the test did before the tag
// existed, and it would have to be deleted rather than re-pointed to build a
// FIPS variant at all.
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
		if len(tc.curves) == 0 {
			t.Fatalf("%s: CurvePreferences is empty; an unpinned list re-enables "+
				"ML-KEM hybrid negotiation", tc.name)
		}
		assertHandshakeCurves(t, tc.name, tc.curves)
	}
}

// ---------------------------------------------------------------------------
// CA bundle parsing for revocation issuer selection
// ---------------------------------------------------------------------------

// TestParseCACertificatesReturnsWholeBundle is the regression test on the
// transport side: TLS verification consumes every block of the CA bundle
// (pool.AppendCertsFromPEM), so revocation must receive every issuer candidate
// too. Taking only the first PEM block wires revocation to the root in a
// standard root-first bundle, which can never match an intermediate-issued
// leaf.
func TestParseCACertificatesReturnsWholeBundle(t *testing.T) {
	rootPEM, _, _, _ := generateTestCertPair(t)
	secondPEM, _, _, _ := generateTestCertPair(t)

	bundle := append(append([]byte{}, rootPEM...), secondPEM...)
	issuers, err := parseCACertificates(bundle)
	if err != nil {
		t.Fatalf("parseCACertificates failed: %v", err)
	}
	if len(issuers) != 2 {
		t.Fatalf("parsed %d issuers from a two-certificate bundle, want 2", len(issuers))
	}
}

// TestParseCACertificatesRejectsBundleWithoutIssuer verifies the fail-fast
// path: a bundle whose blocks cannot act as an issuing CA leaves revocation
// with no way to build an OCSP request or verify a CRL, which is a silent
// no-op under soft-fail and a total handshake outage under enforcement.
func TestParseCACertificatesRejectsBundleWithoutIssuer(t *testing.T) {
	_, _, leafPEM, _ := generateTestCertPair(t)

	_, err := parseCACertificates(leafPEM)
	if err == nil {
		t.Fatal("expected an error for a bundle containing only a leaf certificate")
	}
	if !errors.Is(err, ErrCACertificateParse) {
		t.Fatalf("expected ErrCACertificateParse, got %v", err)
	}
}

// TestParseCACertificatesRejectsEmptyPEM verifies that input with no
// CERTIFICATE block is rejected rather than yielding an empty issuer list.
func TestParseCACertificatesRejectsEmptyPEM(t *testing.T) {
	if _, err := parseCACertificates([]byte("not-pem")); !errors.Is(err, ErrCACertificateParse) {
		t.Fatalf("expected ErrCACertificateParse for non-PEM input, got %v", err)
	}
}
