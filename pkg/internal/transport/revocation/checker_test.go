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

package revocation

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"errors"
	"fmt"
	"math/big"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"golang.org/x/crypto/ocsp"
)

// ---------------------------------------------------------------------------
// Test helpers
// ---------------------------------------------------------------------------

// testCA holds a CA key pair and can issue leaf certificates and CRLs.
type testCA struct {
	key  *ecdsa.PrivateKey
	cert *x509.Certificate
	pem  []byte
}

// newTestCA creates an ephemeral ECDSA P-256 CA.
func newTestCA(t *testing.T) *testCA {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generate CA key: %v", err)
	}
	tmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{Organization: []string{"Test CA"}},
		NotBefore:             time.Now().Add(-1 * time.Minute),
		NotAfter:              time.Now().Add(1 * time.Hour),
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageCRLSign,
		BasicConstraintsValid: true,
		IsCA:                  true,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatalf("create CA cert: %v", err)
	}
	cert, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatalf("parse CA cert: %v", err)
	}
	pemBytes := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	return &testCA{key: key, cert: cert, pem: pemBytes}
}

// issueLeaf creates a leaf certificate signed by the CA with the given serial.
func (ca *testCA) issueLeaf(t *testing.T, serial int64) *x509.Certificate {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generate leaf key: %v", err)
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(serial),
		Subject:      pkix.Name{CommonName: fmt.Sprintf("leaf-%d", serial)},
		NotBefore:    time.Now().Add(-1 * time.Minute),
		NotAfter:     time.Now().Add(1 * time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, ca.cert, &key.PublicKey, ca.key)
	if err != nil {
		t.Fatalf("create leaf cert: %v", err)
	}
	leaf, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatalf("parse leaf cert: %v", err)
	}
	return leaf
}

// createCRL creates a PEM-encoded CRL revoking the given serial numbers.
func (ca *testCA) createCRL(t *testing.T, serials ...int64) []byte {
	t.Helper()
	var entries []x509.RevocationListEntry
	for _, s := range serials {
		entries = append(entries, x509.RevocationListEntry{
			SerialNumber:   big.NewInt(s),
			RevocationTime: time.Now().Add(-1 * time.Minute),
		})
	}
	tmpl := &x509.RevocationList{
		RevokedCertificateEntries: entries,
		Number:                    big.NewInt(1),
		ThisUpdate:                time.Now().Add(-1 * time.Minute),
		NextUpdate:                time.Now().Add(1 * time.Hour),
	}
	crlDER, err := x509.CreateRevocationList(rand.Reader, tmpl, ca.cert, ca.key)
	if err != nil {
		t.Fatalf("create CRL: %v", err)
	}
	return pem.EncodeToMemory(&pem.Block{Type: "X509 CRL", Bytes: crlDER})
}

// writeCRLFile writes PEM-encoded CRL data to a temporary file.
func writeCRLFile(t *testing.T, dir string, name string, data []byte) string {
	t.Helper()
	path := filepath.Join(dir, name)
	if err := os.WriteFile(path, data, 0600); err != nil {
		t.Fatalf("write CRL file: %v", err)
	}
	return path
}

// ---------------------------------------------------------------------------
// Config.SetDefaults tests
// ---------------------------------------------------------------------------

// TestSetDefaultsAppliesAllDefaults verifies that SetDefaults fills
// all zero-valued fields with production defaults.
func TestSetDefaultsAppliesAllDefaults(t *testing.T) {
	cfg := Config{}
	cfg.SetDefaults()

	if cfg.OCSPCacheSeconds != defaultOCSPCacheSeconds {
		t.Fatalf("OCSPCacheSeconds: want %d, got %d", defaultOCSPCacheSeconds, cfg.OCSPCacheSeconds)
	}
	if cfg.OCSPTimeoutSeconds != defaultOCSPTimeoutSeconds {
		t.Fatalf("OCSPTimeoutSeconds: want %d, got %d", defaultOCSPTimeoutSeconds, cfg.OCSPTimeoutSeconds)
	}
	if cfg.CheckInterval != defaultCheckInterval {
		t.Fatalf("CheckInterval: want %v, got %v", defaultCheckInterval, cfg.CheckInterval)
	}
	if cfg.Mode != defaultMode {
		t.Fatalf("Mode: want %q, got %q", defaultMode, cfg.Mode)
	}
}

// TestSetDefaultsPreservesExplicit verifies that explicitly set values
// are not overwritten by SetDefaults.
func TestSetDefaultsPreservesExplicit(t *testing.T) {
	cfg := Config{
		OCSPCacheSeconds:   60,
		OCSPTimeoutSeconds: 2,
		CheckInterval:      10 * time.Second,
		Mode:               ModeCRL,
	}
	cfg.SetDefaults()

	if cfg.OCSPCacheSeconds != 60 {
		t.Fatalf("OCSPCacheSeconds should be preserved: want 60, got %d", cfg.OCSPCacheSeconds)
	}
	if cfg.OCSPTimeoutSeconds != 2 {
		t.Fatalf("OCSPTimeoutSeconds should be preserved: want 2, got %d", cfg.OCSPTimeoutSeconds)
	}
	if cfg.CheckInterval != 10*time.Second {
		t.Fatalf("CheckInterval should be preserved: want 10s, got %v", cfg.CheckInterval)
	}
	if cfg.Mode != ModeCRL {
		t.Fatalf("Mode should be preserved: want %q, got %q", ModeCRL, cfg.Mode)
	}
}

// ---------------------------------------------------------------------------
// CRL loading tests
// ---------------------------------------------------------------------------

// TestCRLReload verifies that ReloadCRLs parses a CRL file and populates
// the revoked serial set.
func TestCRLReload(t *testing.T) {
	ca := newTestCA(t)
	dir := t.TempDir()

	crlData := ca.createCRL(t, 100, 200)
	path := writeCRLFile(t, dir, "test.crl.pem", crlData)

	checker := NewChecker(Config{CRLPaths: []string{path}}, ca.cert)

	if err := checker.ReloadCRLs(); err != nil {
		t.Fatalf("ReloadCRLs failed: %v", err)
	}

	if checker.RevokedCount() != 2 {
		t.Fatalf("revoked count: want 2, got %d", checker.RevokedCount())
	}

	// Verify specific serials are in the set.
	leaf100 := ca.issueLeaf(t, 100)
	leaf200 := ca.issueLeaf(t, 200)
	leaf300 := ca.issueLeaf(t, 300)

	if !checker.checkCRL(leaf100.SerialNumber.Text(16)) {
		t.Fatal("serial 100 should be revoked")
	}
	if !checker.checkCRL(leaf200.SerialNumber.Text(16)) {
		t.Fatal("serial 200 should be revoked")
	}
	if checker.checkCRL(leaf300.SerialNumber.Text(16)) {
		t.Fatal("serial 300 should NOT be revoked")
	}
}

// TestCRLReloadMissingFile verifies that a missing CRL file is logged
// but does not return an error (allows partial loads).
func TestCRLReloadMissingFile(t *testing.T) {
	checker := NewChecker(Config{
		CRLPaths: []string{"/nonexistent/crl.pem"},
	}, nil)

	// Should not return error for missing files (just logged).
	if err := checker.ReloadCRLs(); err != nil {
		t.Fatalf("ReloadCRLs should not fail for missing file: %v", err)
	}

	if checker.RevokedCount() != 0 {
		t.Fatalf("revoked count should be 0 for missing file, got %d", checker.RevokedCount())
	}
}

// TestCRLReloadInvalidPEM verifies that an unparseable CRL returns a
// CRLParseError.
func TestCRLReloadInvalidPEM(t *testing.T) {
	dir := t.TempDir()
	// Use valid base64 that decodes to garbage DER.
	// "AAAA" decodes to 3 zero bytes, which is not valid DER for a CRL.
	path := writeCRLFile(t, dir, "bad.crl.pem",
		[]byte("-----BEGIN X509 CRL-----\nAAAA\n-----END X509 CRL-----\n"))

	checker := NewChecker(Config{CRLPaths: []string{path}}, nil)

	err := checker.ReloadCRLs()
	if err == nil {
		t.Fatal("expected error for invalid CRL")
	}
	if !errors.Is(err, ErrInvalidCRL) {
		t.Fatalf("expected ErrInvalidCRL, got %v", err)
	}

	var crlErr *CRLParseError
	if !errors.As(err, &crlErr) {
		t.Fatalf("expected *CRLParseError, got %T", err)
	}
	if crlErr.Path != path {
		t.Fatalf("CRLParseError.Path: want %q, got %q", path, crlErr.Path)
	}
}

// TestCRLHotReload verifies that modifying a CRL file on disk causes the
// checker to pick up the changes on the next ReloadCRLs call.
func TestCRLHotReload(t *testing.T) {
	ca := newTestCA(t)
	dir := t.TempDir()

	// Initial CRL: revoke serial 100.
	crlData := ca.createCRL(t, 100)
	path := writeCRLFile(t, dir, "hot.crl.pem", crlData)

	checker := NewChecker(Config{CRLPaths: []string{path}}, ca.cert)
	if err := checker.ReloadCRLs(); err != nil {
		t.Fatalf("initial ReloadCRLs: %v", err)
	}
	if checker.RevokedCount() != 1 {
		t.Fatalf("initial revoked count: want 1, got %d", checker.RevokedCount())
	}

	// Update CRL: revoke serials 100 and 200.
	updatedCRL := ca.createCRL(t, 100, 200)
	writeCRLFile(t, dir, "hot.crl.pem", updatedCRL)

	if err := checker.ReloadCRLs(); err != nil {
		t.Fatalf("updated ReloadCRLs: %v", err)
	}
	if checker.RevokedCount() != 2 {
		t.Fatalf("updated revoked count: want 2, got %d", checker.RevokedCount())
	}

	leaf200 := ca.issueLeaf(t, 200)
	if !checker.checkCRL(leaf200.SerialNumber.Text(16)) {
		t.Fatal("serial 200 should be revoked after hot-reload")
	}
}

// TestCRLHashSkip verifies that unchanged CRL files skip re-parsing
// by checking that the revoked set remains consistent.
func TestCRLHashSkip(t *testing.T) {
	ca := newTestCA(t)
	dir := t.TempDir()

	crlData := ca.createCRL(t, 42)
	path := writeCRLFile(t, dir, "static.crl.pem", crlData)

	checker := NewChecker(Config{CRLPaths: []string{path}}, ca.cert)
	if err := checker.ReloadCRLs(); err != nil {
		t.Fatalf("first ReloadCRLs: %v", err)
	}
	if checker.RevokedCount() != 1 {
		t.Fatalf("first revoked count: want 1, got %d", checker.RevokedCount())
	}

	// Reload again without changing the file.
	if err := checker.ReloadCRLs(); err != nil {
		t.Fatalf("second ReloadCRLs: %v", err)
	}
	if checker.RevokedCount() != 1 {
		t.Fatalf("second revoked count: want 1, got %d", checker.RevokedCount())
	}
}

// TestCRLNoPaths verifies that ReloadCRLs is a no-op when no paths
// are configured.
func TestCRLNoPaths(t *testing.T) {
	checker := NewChecker(Config{}, nil)
	if err := checker.ReloadCRLs(); err != nil {
		t.Fatalf("ReloadCRLs with no paths should succeed: %v", err)
	}
	if checker.RevokedCount() != 0 {
		t.Fatalf("revoked count should be 0, got %d", checker.RevokedCount())
	}
}

// TestCRLMultipleFiles verifies that serials from multiple CRL files
// are merged into a single revoked set.
func TestCRLMultipleFiles(t *testing.T) {
	ca := newTestCA(t)
	dir := t.TempDir()

	crl1 := ca.createCRL(t, 10)
	crl2 := ca.createCRL(t, 20, 30)

	path1 := writeCRLFile(t, dir, "one.crl.pem", crl1)
	path2 := writeCRLFile(t, dir, "two.crl.pem", crl2)

	checker := NewChecker(Config{CRLPaths: []string{path1, path2}}, ca.cert)
	if err := checker.ReloadCRLs(); err != nil {
		t.Fatalf("ReloadCRLs: %v", err)
	}
	if checker.RevokedCount() != 3 {
		t.Fatalf("revoked count: want 3, got %d", checker.RevokedCount())
	}
}

// TestCRLMultiplePEMBlocks verifies that a file containing multiple
// CRL PEM blocks is parsed correctly.
func TestCRLMultiplePEMBlocks(t *testing.T) {
	ca := newTestCA(t)
	dir := t.TempDir()

	crl1 := ca.createCRL(t, 1)
	crl2 := ca.createCRL(t, 2)
	combined := append(crl1, crl2...)

	path := writeCRLFile(t, dir, "multi.crl.pem", combined)

	checker := NewChecker(Config{CRLPaths: []string{path}}, ca.cert)
	if err := checker.ReloadCRLs(); err != nil {
		t.Fatalf("ReloadCRLs: %v", err)
	}
	if checker.RevokedCount() != 2 {
		t.Fatalf("revoked count: want 2, got %d", checker.RevokedCount())
	}
}

// TestCRLNonCRLPEMBlocksIgnored verifies that PEM blocks with types
// other than "X509 CRL" are silently ignored.
func TestCRLNonCRLPEMBlocksIgnored(t *testing.T) {
	ca := newTestCA(t)
	dir := t.TempDir()

	certBlock := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: ca.cert.Raw})
	crlData := ca.createCRL(t, 50)
	combined := append(certBlock, crlData...)

	path := writeCRLFile(t, dir, "mixed.pem", combined)

	checker := NewChecker(Config{CRLPaths: []string{path}}, ca.cert)
	if err := checker.ReloadCRLs(); err != nil {
		t.Fatalf("ReloadCRLs: %v", err)
	}
	if checker.RevokedCount() != 1 {
		t.Fatalf("revoked count: want 1, got %d", checker.RevokedCount())
	}
}

// ---------------------------------------------------------------------------
// Programmatic serial management tests
// ---------------------------------------------------------------------------

// TestAddRevokedSerial verifies that programmatically added serials
// are detected by IsRevoked.
func TestAddRevokedSerial(t *testing.T) {
	ca := newTestCA(t)
	checker := NewChecker(Config{Mode: ModeCRL}, ca.cert)

	leaf := ca.issueLeaf(t, 999)
	serial := leaf.SerialNumber.Text(16)

	// Not revoked yet.
	revoked, err := checker.IsRevoked(leaf)
	if err != nil {
		t.Fatalf("IsRevoked error: %v", err)
	}
	if revoked {
		t.Fatal("should not be revoked before AddRevokedSerial")
	}

	// Add programmatically.
	checker.AddRevokedSerial(serial)

	revoked, err = checker.IsRevoked(leaf)
	if err != nil {
		t.Fatalf("IsRevoked error: %v", err)
	}
	if !revoked {
		t.Fatal("should be revoked after AddRevokedSerial")
	}
}

// TestRemoveRevokedSerial verifies that programmatically removed serials
// are no longer detected as revoked.
func TestRemoveRevokedSerial(t *testing.T) {
	ca := newTestCA(t)
	checker := NewChecker(Config{Mode: ModeCRL}, ca.cert)

	leaf := ca.issueLeaf(t, 888)
	serial := leaf.SerialNumber.Text(16)

	checker.AddRevokedSerial(serial)
	if !checker.checkCRL(serial) {
		t.Fatal("serial should be revoked after add")
	}

	checker.RemoveRevokedSerial(serial)
	if checker.checkCRL(serial) {
		t.Fatal("serial should not be revoked after remove")
	}
}

// ---------------------------------------------------------------------------
// IsRevoked mode tests
// ---------------------------------------------------------------------------

// TestIsRevokedCRLMode verifies that ModeCRL checks only CRL data.
func TestIsRevokedCRLMode(t *testing.T) {
	ca := newTestCA(t)
	dir := t.TempDir()

	crlData := ca.createCRL(t, 5)
	path := writeCRLFile(t, dir, "crl.pem", crlData)

	checker := NewChecker(Config{
		CRLPaths: []string{path},
		Mode:     ModeCRL,
	}, ca.cert)

	if err := checker.ReloadCRLs(); err != nil {
		t.Fatalf("ReloadCRLs: %v", err)
	}

	revokedLeaf := ca.issueLeaf(t, 5)
	goodLeaf := ca.issueLeaf(t, 6)

	revoked, err := checker.IsRevoked(revokedLeaf)
	if err != nil {
		t.Fatalf("IsRevoked error: %v", err)
	}
	if !revoked {
		t.Fatal("serial 5 should be revoked")
	}

	revoked, err = checker.IsRevoked(goodLeaf)
	if err != nil {
		t.Fatalf("IsRevoked error: %v", err)
	}
	if revoked {
		t.Fatal("serial 6 should NOT be revoked")
	}
}

// TestIsRevokedUnknownCertPasses verifies that an unknown certificate
// (not in CRL) passes in default mode.
func TestIsRevokedUnknownCertPasses(t *testing.T) {
	ca := newTestCA(t)
	checker := NewChecker(Config{Mode: ModeAny}, ca.cert)

	leaf := ca.issueLeaf(t, 12345)
	revoked, err := checker.IsRevoked(leaf)
	if err != nil {
		t.Fatalf("IsRevoked error: %v", err)
	}
	if revoked {
		t.Fatal("unknown cert should not be revoked in soft-fail mode")
	}
}

// TestIsRevokedModeAnyFallback verifies that ModeAny checks CRL first,
// then falls back to OCSP when configured.
func TestIsRevokedModeAnyFallback(t *testing.T) {
	ca := newTestCA(t)
	dir := t.TempDir()

	// CRL revokes serial 7.
	crlData := ca.createCRL(t, 7)
	path := writeCRLFile(t, dir, "crl.pem", crlData)

	checker := NewChecker(Config{
		CRLPaths: []string{path},
		Mode:     ModeAny,
		// No OCSP URL configured -- CRL only effective.
	}, ca.cert)
	if err := checker.ReloadCRLs(); err != nil {
		t.Fatalf("ReloadCRLs: %v", err)
	}

	leaf := ca.issueLeaf(t, 7)
	revoked, err := checker.IsRevoked(leaf)
	if err != nil {
		t.Fatalf("IsRevoked error: %v", err)
	}
	if !revoked {
		t.Fatal("serial 7 should be revoked via CRL in ModeAny")
	}
}

// ---------------------------------------------------------------------------
// Enforcement tests
// ---------------------------------------------------------------------------

// TestEnforceRevocationStrictMode verifies that EnforceRevocation rejects
// connections when OCSP is unreachable.
func TestEnforceRevocationStrictMode(t *testing.T) {
	ca := newTestCA(t)
	checker := NewChecker(Config{
		Mode:               ModeOCSP,
		OCSPResponderURL:   "http://127.0.0.1:1/unreachable",
		OCSPTimeoutSeconds: 1,
		EnforceRevocation:  true,
	}, ca.cert)

	leaf := ca.issueLeaf(t, 42)
	_, err := checker.IsRevoked(leaf)
	if err == nil {
		t.Fatal("expected error with EnforceRevocation and unreachable OCSP")
	}
	if !errors.Is(err, ErrOCSPUnavailable) {
		t.Fatalf("expected ErrOCSPUnavailable, got %v", err)
	}
}

// TestSoftFailAllowsUnknown verifies that soft-fail mode (default)
// allows connections when OCSP is unreachable.
func TestSoftFailAllowsUnknown(t *testing.T) {
	ca := newTestCA(t)
	checker := NewChecker(Config{
		Mode:               ModeOCSP,
		OCSPResponderURL:   "http://127.0.0.1:1/unreachable",
		OCSPTimeoutSeconds: 1,
		EnforceRevocation:  false, // soft-fail
	}, ca.cert)

	leaf := ca.issueLeaf(t, 42)
	revoked, err := checker.IsRevoked(leaf)
	if err != nil {
		t.Fatalf("soft-fail should not return error: %v", err)
	}
	if revoked {
		t.Fatal("soft-fail should not report revoked when OCSP is unreachable")
	}
}

// TestEnforceRevocationOCSPNoURL verifies that EnforceRevocation with
// no OCSP URL configured returns an error in ModeOCSP.
func TestEnforceRevocationOCSPNoURL(t *testing.T) {
	ca := newTestCA(t)
	checker := NewChecker(Config{
		Mode:              ModeOCSP,
		OCSPResponderURL:  "", // no URL
		EnforceRevocation: true,
	}, ca.cert)

	leaf := ca.issueLeaf(t, 42)
	_, err := checker.IsRevoked(leaf)
	if err == nil {
		t.Fatal("expected error with EnforceRevocation and no OCSP URL")
	}
	if !errors.Is(err, ErrOCSPUnavailable) {
		t.Fatalf("expected ErrOCSPUnavailable, got %v", err)
	}
}

// ---------------------------------------------------------------------------
// OCSP tests using mock HTTP server
// ---------------------------------------------------------------------------

// newMockOCSPServer creates a test HTTP server that responds to OCSP
// requests. revokedSerials is a set of serial numbers (hex) to report
// as revoked.
func newMockOCSPServer(t *testing.T, ca *testCA, revokedSerials map[string]bool) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}

		body := make([]byte, r.ContentLength)
		if _, err := r.Body.Read(body); err != nil && err.Error() != "EOF" {
			http.Error(w, "read error", http.StatusBadRequest)
			return
		}

		req, err := ocsp.ParseRequest(body)
		if err != nil {
			http.Error(w, "bad request", http.StatusBadRequest)
			return
		}

		serialHex := req.SerialNumber.Text(16)
		status := ocsp.Good
		if revokedSerials[serialHex] {
			status = ocsp.Revoked
		}

		tmpl := ocsp.Response{
			Status:       status,
			SerialNumber: req.SerialNumber,
			ThisUpdate:   time.Now().Add(-1 * time.Minute),
			NextUpdate:   time.Now().Add(1 * time.Hour),
		}

		respBytes, err := ocsp.CreateResponse(ca.cert, ca.cert, tmpl, ca.key)
		if err != nil {
			http.Error(w, "create response error", http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "application/ocsp-response")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(respBytes)
	}))
}

// TestOCSPRevokedCert verifies that a revoked cert is detected via OCSP.
func TestOCSPRevokedCert(t *testing.T) {
	ca := newTestCA(t)
	leaf := ca.issueLeaf(t, 77)
	serialHex := leaf.SerialNumber.Text(16)

	server := newMockOCSPServer(t, ca, map[string]bool{serialHex: true})
	defer server.Close()

	checker := NewChecker(Config{
		Mode:               ModeOCSP,
		OCSPResponderURL:   server.URL,
		OCSPTimeoutSeconds: 5,
	}, ca.cert)

	revoked, err := checker.IsRevoked(leaf)
	if err != nil {
		t.Fatalf("IsRevoked error: %v", err)
	}
	if !revoked {
		t.Fatal("cert should be revoked via OCSP")
	}
}

// TestOCSPGoodCert verifies that a non-revoked cert passes OCSP check.
func TestOCSPGoodCert(t *testing.T) {
	ca := newTestCA(t)
	leaf := ca.issueLeaf(t, 88)

	server := newMockOCSPServer(t, ca, map[string]bool{}) // none revoked
	defer server.Close()

	checker := NewChecker(Config{
		Mode:               ModeOCSP,
		OCSPResponderURL:   server.URL,
		OCSPTimeoutSeconds: 5,
	}, ca.cert)

	revoked, err := checker.IsRevoked(leaf)
	if err != nil {
		t.Fatalf("IsRevoked error: %v", err)
	}
	if revoked {
		t.Fatal("cert should NOT be revoked via OCSP")
	}
}

// TestOCSPCacheHit verifies that cached OCSP responses are reused
// without re-querying the server.
func TestOCSPCacheHit(t *testing.T) {
	ca := newTestCA(t)
	leaf := ca.issueLeaf(t, 99)

	var queryCount atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		queryCount.Add(1)

		body := make([]byte, r.ContentLength)
		_, _ = r.Body.Read(body)
		req, _ := ocsp.ParseRequest(body)

		tmpl := ocsp.Response{
			Status:       ocsp.Good,
			SerialNumber: req.SerialNumber,
			ThisUpdate:   time.Now().Add(-1 * time.Minute),
			NextUpdate:   time.Now().Add(1 * time.Hour),
		}
		respBytes, _ := ocsp.CreateResponse(ca.cert, ca.cert, tmpl, ca.key)
		w.Header().Set("Content-Type", "application/ocsp-response")
		_, _ = w.Write(respBytes)
	}))
	defer server.Close()

	checker := NewChecker(Config{
		Mode:               ModeOCSP,
		OCSPResponderURL:   server.URL,
		OCSPCacheSeconds:   60,
		OCSPTimeoutSeconds: 5,
	}, ca.cert)

	// First call: cache miss, queries server.
	_, _ = checker.IsRevoked(leaf)
	if queryCount.Load() != 1 {
		t.Fatalf("first call should query server once, got %d", queryCount.Load())
	}

	// Second call: cache hit, no server query.
	_, _ = checker.IsRevoked(leaf)
	if queryCount.Load() != 1 {
		t.Fatalf("second call should use cache, got %d queries", queryCount.Load())
	}
}

// TestOCSPNoIssuerCert verifies that OCSP returns an error when no
// issuer certificate is configured.
func TestOCSPNoIssuerCert(t *testing.T) {
	ca := newTestCA(t)
	leaf := ca.issueLeaf(t, 55)

	checker := NewChecker(Config{
		Mode:              ModeOCSP,
		OCSPResponderURL:  "http://localhost:1/unused",
		EnforceRevocation: true,
	}, nil) // nil issuer cert

	_, err := checker.IsRevoked(leaf)
	if err == nil {
		t.Fatal("expected error with nil issuer cert")
	}
	if !errors.Is(err, ErrOCSPUnavailable) {
		t.Fatalf("expected ErrOCSPUnavailable, got %v", err)
	}
}

// ---------------------------------------------------------------------------
// ModeBoth tests
// ---------------------------------------------------------------------------

// TestModeBothRequiresBothSources verifies that ModeBoth only reports
// revoked when both CRL and OCSP agree.
func TestModeBothRequiresBothSources(t *testing.T) {
	ca := newTestCA(t)
	dir := t.TempDir()
	leaf := ca.issueLeaf(t, 33)
	serialHex := leaf.SerialNumber.Text(16)

	// CRL: revoke serial 33.
	crlData := ca.createCRL(t, 33)
	crlPath := writeCRLFile(t, dir, "crl.pem", crlData)

	// OCSP: also reports serial 33 as revoked.
	server := newMockOCSPServer(t, ca, map[string]bool{serialHex: true})
	defer server.Close()

	checker := NewChecker(Config{
		CRLPaths:           []string{crlPath},
		OCSPResponderURL:   server.URL,
		OCSPTimeoutSeconds: 5,
		Mode:               ModeBoth,
	}, ca.cert)
	if err := checker.ReloadCRLs(); err != nil {
		t.Fatalf("ReloadCRLs: %v", err)
	}

	revoked, err := checker.IsRevoked(leaf)
	if err != nil {
		t.Fatalf("IsRevoked error: %v", err)
	}
	if !revoked {
		t.Fatal("both CRL and OCSP confirm revoked, should be revoked")
	}
}

// TestModeBothCRLOnlyNotRevoked verifies that when only CRL reports
// revoked but OCSP says good, ModeBoth returns not revoked.
func TestModeBothCRLOnlyNotRevoked(t *testing.T) {
	ca := newTestCA(t)
	dir := t.TempDir()
	leaf := ca.issueLeaf(t, 44)

	// CRL: revoke serial 44.
	crlData := ca.createCRL(t, 44)
	crlPath := writeCRLFile(t, dir, "crl.pem", crlData)

	// OCSP: reports good (not in revoked set).
	server := newMockOCSPServer(t, ca, map[string]bool{})
	defer server.Close()

	checker := NewChecker(Config{
		CRLPaths:           []string{crlPath},
		OCSPResponderURL:   server.URL,
		OCSPTimeoutSeconds: 5,
		Mode:               ModeBoth,
	}, ca.cert)
	if err := checker.ReloadCRLs(); err != nil {
		t.Fatalf("ReloadCRLs: %v", err)
	}

	revoked, err := checker.IsRevoked(leaf)
	if err != nil {
		t.Fatalf("IsRevoked error: %v", err)
	}
	if revoked {
		t.Fatal("only CRL confirms revoked, ModeBoth should return not revoked")
	}
}

// TestModeBothOCSPUnavailableSoftFall verifies that when OCSP is
// unreachable in ModeBoth with soft-fail, the CRL result is used.
func TestModeBothOCSPUnavailableSoftFail(t *testing.T) {
	ca := newTestCA(t)
	dir := t.TempDir()
	leaf := ca.issueLeaf(t, 55)

	crlData := ca.createCRL(t, 55)
	crlPath := writeCRLFile(t, dir, "crl.pem", crlData)

	checker := NewChecker(Config{
		CRLPaths:           []string{crlPath},
		OCSPResponderURL:   "http://127.0.0.1:1/unreachable",
		OCSPTimeoutSeconds: 1,
		Mode:               ModeBoth,
		EnforceRevocation:  false,
	}, ca.cert)
	if err := checker.ReloadCRLs(); err != nil {
		t.Fatalf("ReloadCRLs: %v", err)
	}

	revoked, err := checker.IsRevoked(leaf)
	if err != nil {
		t.Fatalf("soft-fail should not error: %v", err)
	}
	if !revoked {
		t.Fatal("CRL says revoked, OCSP unavailable in soft-fail = use CRL result")
	}
}

// TestModeBothOCSPUnavailableEnforced verifies that when OCSP is
// unreachable in ModeBoth with enforcement, an error is returned.
func TestModeBothOCSPUnavailableEnforced(t *testing.T) {
	ca := newTestCA(t)
	dir := t.TempDir()
	leaf := ca.issueLeaf(t, 66)

	crlData := ca.createCRL(t, 66)
	crlPath := writeCRLFile(t, dir, "crl.pem", crlData)

	checker := NewChecker(Config{
		CRLPaths:           []string{crlPath},
		OCSPResponderURL:   "http://127.0.0.1:1/unreachable",
		OCSPTimeoutSeconds: 1,
		Mode:               ModeBoth,
		EnforceRevocation:  true,
	}, ca.cert)
	if err := checker.ReloadCRLs(); err != nil {
		t.Fatalf("ReloadCRLs: %v", err)
	}

	_, err := checker.IsRevoked(leaf)
	if err == nil {
		t.Fatal("expected error with enforcement and unreachable OCSP")
	}
	if !errors.Is(err, ErrOCSPUnavailable) {
		t.Fatalf("expected ErrOCSPUnavailable, got %v", err)
	}
}

// ---------------------------------------------------------------------------
// Lifecycle tests
// ---------------------------------------------------------------------------

// TestCheckerStartStop verifies that Start and Stop manage the
// background goroutine lifecycle without panics or leaks.
func TestCheckerStartStop(t *testing.T) {
	ca := newTestCA(t)
	dir := t.TempDir()

	crlData := ca.createCRL(t, 1)
	path := writeCRLFile(t, dir, "lifecycle.crl.pem", crlData)

	checker := NewChecker(Config{
		CRLPaths:      []string{path},
		CheckInterval: 10 * time.Millisecond, // fast for test
	}, ca.cert)

	checker.Start()

	// Poll until the background reload picks up the CRL.
	require.Eventually(t, func() bool {
		return checker.RevokedCount() == 1
	}, 5*time.Second, 5*time.Millisecond, "revoked count after start: want 1")

	checker.Stop()

	// Double-stop should not panic.
	checker.Stop()
}

// TestCheckerStartStopWithHotReload verifies that a CRL file change
// is picked up by the background reload goroutine.
func TestCheckerStartStopWithHotReload(t *testing.T) {
	ca := newTestCA(t)
	dir := t.TempDir()

	crlData := ca.createCRL(t, 10)
	path := writeCRLFile(t, dir, "bg.crl.pem", crlData)

	checker := NewChecker(Config{
		CRLPaths:      []string{path},
		CheckInterval: 20 * time.Millisecond,
		Mode:          ModeCRL,
	}, ca.cert)
	checker.Start()
	defer checker.Stop()

	// Poll for initial load.
	require.Eventually(t, func() bool {
		return checker.RevokedCount() == 1
	}, 5*time.Second, 5*time.Millisecond, "initial count: want 1")

	// Update CRL file.
	updatedCRL := ca.createCRL(t, 10, 20)
	writeCRLFile(t, dir, "bg.crl.pem", updatedCRL)

	// Poll for background reload to pick up.
	require.Eventually(t, func() bool {
		return checker.RevokedCount() == 2
	}, 5*time.Second, 5*time.Millisecond, "after hot-reload: want 2")
}

// ---------------------------------------------------------------------------
// Concurrent access tests
// ---------------------------------------------------------------------------

// TestConcurrentIsRevoked verifies that IsRevoked is safe for concurrent
// access from multiple goroutines (simulating concurrent TLS handshakes).
func TestConcurrentIsRevoked(t *testing.T) {
	ca := newTestCA(t)
	dir := t.TempDir()

	crlData := ca.createCRL(t, 1, 2, 3)
	path := writeCRLFile(t, dir, "conc.crl.pem", crlData)

	checker := NewChecker(Config{
		CRLPaths: []string{path},
		Mode:     ModeCRL,
	}, ca.cert)
	if err := checker.ReloadCRLs(); err != nil {
		t.Fatalf("ReloadCRLs: %v", err)
	}

	revokedLeaf := ca.issueLeaf(t, 2)
	goodLeaf := ca.issueLeaf(t, 99)

	var wg sync.WaitGroup
	const goroutines = 100

	for range goroutines {
		wg.Add(2)
		go func() {
			defer wg.Done()
			revoked, err := checker.IsRevoked(revokedLeaf)
			if err != nil {
				t.Errorf("concurrent IsRevoked error: %v", err)
			}
			if !revoked {
				t.Error("concurrent: revoked leaf should be revoked")
			}
		}()
		go func() {
			defer wg.Done()
			revoked, err := checker.IsRevoked(goodLeaf)
			if err != nil {
				t.Errorf("concurrent IsRevoked error: %v", err)
			}
			if revoked {
				t.Error("concurrent: good leaf should not be revoked")
			}
		}()
	}
	wg.Wait()
}

// ---------------------------------------------------------------------------
// Callback tests
// ---------------------------------------------------------------------------

// TestOnCertRevokedCallback verifies that the OnCertRevoked callback
// is invoked when accessed via CfgRef.
func TestOnCertRevokedCallback(t *testing.T) {
	var called atomic.Bool
	var gotSerial atomic.Value

	ca := newTestCA(t)
	checker := NewChecker(Config{
		Mode: ModeCRL,
		OnCertRevoked: func(serial string) {
			called.Store(true)
			gotSerial.Store(serial)
		},
	}, ca.cert)

	leaf := ca.issueLeaf(t, 42)
	serial := leaf.SerialNumber.Text(16)
	checker.AddRevokedSerial(serial)

	// The callback is used via CfgRef in the TLS wiring, test it directly.
	cfg := checker.CfgRef()
	if cfg.OnCertRevoked == nil {
		t.Fatal("OnCertRevoked should be set")
	}
	cfg.OnCertRevoked(serial)

	if !called.Load() {
		t.Fatal("OnCertRevoked callback was not called")
	}
	if gotSerial.Load().(string) != serial {
		t.Fatalf("OnCertRevoked serial: want %q, got %q", serial, gotSerial.Load())
	}
}

// ---------------------------------------------------------------------------
// Error type tests
// ---------------------------------------------------------------------------

// TestCertificateRevokedErrorIs verifies errors.Is matching.
func TestCertificateRevokedErrorIs(t *testing.T) {
	err := &CertificateRevokedError{Serial: "abc"}
	if !errors.Is(err, ErrCertificateRevoked) {
		t.Fatal("CertificateRevokedError should match ErrCertificateRevoked")
	}
	if err.Error() != "revocation: certificate serial abc is revoked" {
		t.Fatalf("unexpected error message: %s", err.Error())
	}
}

// TestCertificateRevokedErrorNotIs verifies non-matching.
func TestCertificateRevokedErrorNotIs(t *testing.T) {
	err := &CertificateRevokedError{Serial: "abc"}
	if errors.Is(err, ErrOCSPUnavailable) {
		t.Fatal("CertificateRevokedError should not match ErrOCSPUnavailable")
	}
}

// TestOCSPUnavailableErrorIs verifies errors.Is matching and Unwrap.
func TestOCSPUnavailableErrorIs(t *testing.T) {
	inner := errors.New("connection refused")
	err := &OCSPUnavailableError{URL: "http://example.com", Err: inner}
	if !errors.Is(err, ErrOCSPUnavailable) {
		t.Fatal("OCSPUnavailableError should match ErrOCSPUnavailable")
	}
	if !errors.Is(err, inner) {
		t.Fatal("OCSPUnavailableError should unwrap to inner error")
	}
	expected := "revocation: OCSP responder http://example.com unavailable: connection refused"
	if err.Error() != expected {
		t.Fatalf("unexpected error message: %s", err.Error())
	}
}

// TestOCSPUnavailableErrorNotIs verifies non-matching.
func TestOCSPUnavailableErrorNotIs(t *testing.T) {
	err := &OCSPUnavailableError{URL: "http://x", Err: errors.New("fail")}
	if errors.Is(err, ErrCertificateRevoked) {
		t.Fatal("OCSPUnavailableError should not match ErrCertificateRevoked")
	}
}

// TestCRLParseErrorIs verifies errors.Is matching and Unwrap.
func TestCRLParseErrorIs(t *testing.T) {
	inner := errors.New("asn1 decode")
	err := &CRLParseError{Path: "/tmp/crl.pem", Err: inner}
	if !errors.Is(err, ErrInvalidCRL) {
		t.Fatal("CRLParseError should match ErrInvalidCRL")
	}
	if !errors.Is(err, inner) {
		t.Fatal("CRLParseError should unwrap to inner error")
	}
	expected := "revocation: failed to parse CRL /tmp/crl.pem: asn1 decode"
	if err.Error() != expected {
		t.Fatalf("unexpected error message: %s", err.Error())
	}
}

// TestCRLParseErrorNotIs verifies non-matching.
func TestCRLParseErrorNotIs(t *testing.T) {
	err := &CRLParseError{Path: "/tmp/x", Err: errors.New("fail")}
	if errors.Is(err, ErrOCSPUnavailable) {
		t.Fatal("CRLParseError should not match ErrOCSPUnavailable")
	}
}

// ---------------------------------------------------------------------------
// Mode constants tests
// ---------------------------------------------------------------------------

// TestModeConstants verifies the mode constant string values.
func TestModeConstants(t *testing.T) {
	if ModeCRL != "crl" {
		t.Fatalf("ModeCRL: want %q, got %q", "crl", ModeCRL)
	}
	if ModeOCSP != "ocsp" {
		t.Fatalf("ModeOCSP: want %q, got %q", "ocsp", ModeOCSP)
	}
	if ModeBoth != "both" {
		t.Fatalf("ModeBoth: want %q, got %q", "both", ModeBoth)
	}
	if ModeAny != "any" {
		t.Fatalf("ModeAny: want %q, got %q", "any", ModeAny)
	}
}

// TestDefaultConstants verifies the default constant values.
func TestDefaultConstants(t *testing.T) {
	if defaultOCSPCacheSeconds != 300 {
		t.Fatalf("defaultOCSPCacheSeconds: want 300, got %d", defaultOCSPCacheSeconds)
	}
	if defaultOCSPTimeoutSeconds != 5 {
		t.Fatalf("defaultOCSPTimeoutSeconds: want 5, got %d", defaultOCSPTimeoutSeconds)
	}
	if defaultCheckInterval != 60*time.Second {
		t.Fatalf("defaultCheckInterval: want 60s, got %v", defaultCheckInterval)
	}
	if defaultMode != ModeAny {
		t.Fatalf("defaultMode: want %q, got %q", ModeAny, defaultMode)
	}
}

// TestRevokedCount verifies that RevokedCount reflects the current state.
func TestRevokedCount(t *testing.T) {
	checker := NewChecker(Config{Mode: ModeCRL}, nil)

	if checker.RevokedCount() != 0 {
		t.Fatalf("initial count: want 0, got %d", checker.RevokedCount())
	}

	checker.AddRevokedSerial("a")
	checker.AddRevokedSerial("b")
	if checker.RevokedCount() != 2 {
		t.Fatalf("after 2 adds: want 2, got %d", checker.RevokedCount())
	}

	checker.RemoveRevokedSerial("a")
	if checker.RevokedCount() != 1 {
		t.Fatalf("after remove: want 1, got %d", checker.RevokedCount())
	}
}
