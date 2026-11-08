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
	"runtime"
	"strings"
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

	if cfg.OCSPCacheSeconds != 300 {
		t.Fatalf("OCSPCacheSeconds: want 300, got %d", cfg.OCSPCacheSeconds)
	}
	if cfg.OCSPTimeoutSeconds != 5 {
		t.Fatalf("OCSPTimeoutSeconds: want 5, got %d", cfg.OCSPTimeoutSeconds)
	}
	if cfg.CheckInterval != 60*time.Second {
		t.Fatalf("CheckInterval: want 60s, got %v", cfg.CheckInterval)
	}
	if cfg.Mode != ModeAny {
		t.Fatalf("Mode: want %q, got %q", ModeAny, cfg.Mode)
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

	if !crlRevokedFor(checker, leaf100) {
		t.Fatal("serial 100 should be revoked")
	}
	if !crlRevokedFor(checker, leaf200) {
		t.Fatal("serial 200 should be revoked")
	}
	if crlRevokedFor(checker, leaf300) {
		t.Fatal("serial 300 should NOT be revoked")
	}
}

// TestCRLReloadMissingFile verifies that a missing CRL file is logged
// but does not return an error (allows partial loads).
func TestCRLReloadMissingFile(t *testing.T) {
	checker := NewChecker(Config{
		CRLPaths: []string{"/nonexistent/crl.pem"},
	}, nil)

	// An unreadable CRL must FAIL the reload: silently continuing with a
	// partial set would un-revoke every certificate in the missing file.
	err := checker.ReloadCRLs()
	if err == nil {
		t.Fatal("ReloadCRLs must fail for an unreadable CRL file")
	}
	var readErr *CRLReadError
	if !errors.As(err, &readErr) {
		t.Fatalf("expected *CRLReadError, got %T: %v", err, err)
	}
	if readErr.Path != "/nonexistent/crl.pem" {
		t.Fatalf("CRLReadError.Path = %q", readErr.Path)
	}
}

// TestCRLReloadReadFailureKeepsPreviousSet verifies that a failed reload
// preserves the previously loaded revocations (a transiently unreadable
// CRL must not un-revoke certificates) and that enforcement mode fails
// closed (ErrCRLStale) for unknown serials until a reload succeeds.
func TestCRLReloadReadFailureKeepsPreviousSet(t *testing.T) {
	ca := newTestCA(t)
	dir := t.TempDir()
	revokedLeaf := ca.issueLeaf(t, 77)
	unknownLeaf := ca.issueLeaf(t, 78)

	crlData := ca.createCRL(t, 77)
	crlPath := writeCRLFile(t, dir, "crl.pem", crlData)

	checker := NewChecker(Config{
		CRLPaths: []string{crlPath},
		Mode:     ModeCRL,
	}, ca.cert)
	if err := checker.ReloadCRLs(); err != nil {
		t.Fatalf("ReloadCRLs: %v", err)
	}
	if revoked, err := checker.IsRevoked(revokedLeaf); err != nil || !revoked {
		t.Fatalf("baseline: revoked=%v err=%v, want true/nil", revoked, err)
	}

	// Delete the CRL file and reload: the reload must fail, the previous
	// set must keep serving the revocation, and unknown serials must fail
	// closed under enforcement.
	if err := os.Remove(crlPath); err != nil {
		t.Fatalf("Remove: %v", err)
	}
	if err := checker.ReloadCRLs(); err == nil {
		t.Fatal("ReloadCRLs must fail after CRL file deletion")
	}

	if revoked, err := checker.IsRevoked(revokedLeaf); err != nil || !revoked {
		t.Fatalf("after failed reload: revoked=%v err=%v, want true/nil (previous set must be kept)", revoked, err)
	}
	if _, err := checker.IsRevoked(unknownLeaf); !errors.Is(err, ErrCRLStale) {
		t.Fatalf("unknown serial with stale CRL under enforcement: err=%v, want ErrCRLStale", err)
	}

	// Restore the file: reload succeeds and unknown serials pass again.
	if werr := os.WriteFile(crlPath, crlData, 0o600); werr != nil {
		t.Fatalf("WriteFile: %v", werr)
	}
	if err := checker.ReloadCRLs(); err != nil {
		t.Fatalf("ReloadCRLs after restore: %v", err)
	}
	if revoked, err := checker.IsRevoked(unknownLeaf); err != nil || revoked {
		t.Fatalf("after restore: revoked=%v err=%v, want false/nil", revoked, err)
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
	if !crlRevokedFor(checker, leaf200) {
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
//
// The checker has no CRL file, so soft-fail is what makes "not revoked yet"
// an admission rather than a missing-source rejection; the admin path is the
// only source of truth here and it is asked for explicitly.
func TestAddRevokedSerial(t *testing.T) {
	ca := newTestCA(t)
	checker := NewChecker(Config{Mode: ModeCRL, AllowSoftFail: true}, ca.cert)

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

// TestAddRevokedSerial_SurvivesCRLReload: a CRL reload replaces the CRL-derived
// set wholesale, so programmatic revocations must not live in that same set:
// every runtime revocation would be silently undone at the next reload — every
// 60s whenever CRL files are configured.
func TestAddRevokedSerial_SurvivesCRLReload(t *testing.T) {
	ca := newTestCA(t)
	crlPath := writeCRLFile(t, t.TempDir(), "ca.crl", ca.createCRL(t)) // lists nothing
	checker := NewChecker(Config{Mode: ModeCRL, CRLPaths: []string{crlPath}}, ca.cert)
	if err := checker.ReloadCRLs(); err != nil {
		t.Fatalf("ReloadCRLs: %v", err)
	}

	leaf := ca.issueLeaf(t, 4242)
	checker.AddRevokedSerial(leaf.SerialNumber.Text(16))

	// A changed CRL file forces a real reload, not a hash-skip.
	if err := os.WriteFile(crlPath, ca.createCRL(t, 7), 0o600); err != nil {
		t.Fatalf("rewrite CRL: %v", err)
	}
	if err := checker.ReloadCRLs(); err != nil {
		t.Fatalf("ReloadCRLs: %v", err)
	}
	revoked, err := checker.IsRevoked(leaf)
	if err != nil {
		t.Fatalf("IsRevoked error: %v", err)
	}
	if !revoked {
		t.Fatal("a CRL reload undid a programmatic revocation")
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
	if !crlRevoked(checker, serial) {
		t.Fatal("serial should be revoked after add")
	}

	checker.RemoveRevokedSerial(serial)
	if crlRevoked(checker, serial) {
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
// (not in CRL) passes in default mode under soft-fail, which is what the
// original assertion always described: with no source configured, admitting
// the peer is the soft-fail behaviour and nothing else.
func TestIsRevokedUnknownCertPasses(t *testing.T) {
	ca := newTestCA(t)
	checker := NewChecker(Config{Mode: ModeAny, AllowSoftFail: true}, ca.cert)

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

// TestEnforcedRevocationStrictMode verifies that enforcement rejects
// connections when OCSP is unreachable.
func TestEnforcedRevocationStrictMode(t *testing.T) {
	ca := newTestCA(t)
	checker := NewChecker(Config{
		Mode:               ModeOCSP,
		OCSPResponderURL:   "http://127.0.0.1:1/unreachable",
		OCSPTimeoutSeconds: 1,
	}, ca.cert)

	leaf := ca.issueLeaf(t, 42)
	_, err := checker.IsRevoked(leaf)
	if err == nil {
		t.Fatal("expected error with enforcement and unreachable OCSP")
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
		AllowSoftFail:      true, // soft-fail
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

// TestEnforcedRevocationOCSPNoURL verifies that enforcement with
// no OCSP URL configured returns an error in ModeOCSP.
func TestEnforcedRevocationOCSPNoURL(t *testing.T) {
	ca := newTestCA(t)
	checker := NewChecker(Config{
		Mode:             ModeOCSP,
		OCSPResponderURL: "", // no URL
	}, ca.cert)

	leaf := ca.issueLeaf(t, 42)
	_, err := checker.IsRevoked(leaf)
	if err == nil {
		t.Fatal("expected error with enforcement and no OCSP URL")
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

// TestOCSPNegativeCacheSuppressesRequeries verifies that a responder failure
// is negative-cached for a short TTL so a flood of reconnect handshakes does
// not re-query a down responder on every connection (adding the full OCSP
// timeout to each handshake). Soft-fail still allows the connection.
func TestOCSPNegativeCacheSuppressesRequeries(t *testing.T) {
	ca := newTestCA(t)
	leaf := ca.issueLeaf(t, 4242)

	var queryCount atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		// Always fail so checkOCSPRaw treats the responder as unavailable.
		queryCount.Add(1)
		http.Error(w, "responder down", http.StatusInternalServerError)
	}))
	defer server.Close()

	checker := NewChecker(Config{
		Mode:               ModeOCSP,
		OCSPResponderURL:   server.URL,
		OCSPTimeoutSeconds: 2,
		AllowSoftFail:      true, // soft-fail
	}, ca.cert)

	// First call: responder fails, negative-cached. Soft-fail allows.
	revoked, err := checker.IsRevoked(leaf)
	if err != nil {
		t.Fatalf("soft-fail should not return error: %v", err)
	}
	if revoked {
		t.Fatal("unavailable responder must not report revoked")
	}
	if got := queryCount.Load(); got != 1 {
		t.Fatalf("first call should query the responder once, got %d", got)
	}

	// Subsequent calls within the negative TTL must NOT re-query.
	for i := 0; i < 5; i++ {
		revoked, err = checker.IsRevoked(leaf)
		if err != nil {
			t.Fatalf("soft-fail should not return error on cached call: %v", err)
		}
		if revoked {
			t.Fatal("negative-cached result must not report revoked")
		}
	}
	if got := queryCount.Load(); got != 1 {
		t.Fatalf("negative cache should suppress re-queries; got %d queries", got)
	}
}

// TestOCSPNegativeCacheStillFailsClosedUnderEnforcement verifies that the
// negative cache does not weaken fail-closed behavior: under enforcement
// a cached unavailable result still rejects the peer.
func TestOCSPNegativeCacheStillFailsClosedUnderEnforcement(t *testing.T) {
	ca := newTestCA(t)
	leaf := ca.issueLeaf(t, 4343)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "responder down", http.StatusInternalServerError)
	}))
	defer server.Close()

	checker := NewChecker(Config{
		Mode:               ModeOCSP,
		OCSPResponderURL:   server.URL,
		OCSPTimeoutSeconds: 2,
	}, ca.cert)

	// First call populates the negative cache and must reject.
	if _, err := checker.IsRevoked(leaf); !errors.Is(err, ErrOCSPUnavailable) {
		t.Fatalf("first call: expected ErrOCSPUnavailable, got %v", err)
	}
	// Second call served from the negative cache must also reject.
	_, err := checker.IsRevoked(leaf)
	if !errors.Is(err, ErrOCSPUnavailable) {
		t.Fatalf("cached call: expected ErrOCSPUnavailable, got %v", err)
	}
	if !errors.Is(err, ErrOCSPNegativeCached) {
		t.Fatalf("cached call: expected wrapped ErrOCSPNegativeCached, got %v", err)
	}
}

// TestOCSPDefinitiveRevokedNotMaskedByLaterUnavailable verifies verdict
// precedence: a query failure must NOT overwrite an unexpired definitive
// "revoked" verdict already in the cache with a short-TTL unavailable entry. In
// soft-fail mode this guarantees a peer the checker already knows to be revoked
// stays rejected even while the responder is down, rather than being silently
// accepted as "status unknown".
//
// The cache-hit fast path in checkOCSPRaw normally returns the definitive entry
// before any query, so the dangerous overwrite can only occur under a race
// where the query was already in flight when a definitive verdict was cached.
// We exercise the write guard deterministically by invoking the failing query
// path while a definitive entry is present, asserting the guard preserves it.
func TestOCSPDefinitiveRevokedNotMaskedByLaterUnavailable(t *testing.T) {
	ca := newTestCA(t)
	leaf := ca.issueLeaf(t, 5151)

	// A responder that always fails, so any actual query is treated as
	// "unavailable" and drives the negative-cache write path.
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "responder down", http.StatusInternalServerError)
	}))
	defer server.Close()

	checker := NewChecker(Config{
		Mode:               ModeOCSP,
		OCSPResponderURL:   server.URL,
		OCSPCacheSeconds:   300, // long positive TTL
		OCSPTimeoutSeconds: 2,
		AllowSoftFail:      true, // soft-fail: the masking risk only matters here
	}, ca.cert)

	// Seed an unexpired definitive "revoked" verdict, as if the responder had
	// answered moments earlier. The cache is keyed by (issuer, serial).
	cacheKey := ocspKey(checker, leaf)
	checker.ocspMu.Lock()
	checker.ocspCache[cacheKey] = &ocspCacheEntry{
		revoked:   true,
		expiresAt: time.Now().Add(300 * time.Second),
	}
	checker.ocspMu.Unlock()

	// Drive the failing query path directly. queryOCSP fails (responder down),
	// so checkOCSPRaw reaches the negative-cache write. The guard must refuse
	// to overwrite the unexpired definitive entry.
	if _, _, err := checker.queryOCSP(leaf, ca.cert); err == nil {
		t.Fatal("responder is down; queryOCSP should fail")
	}
	// Reproduce the negative-write that checkOCSPRaw performs on query failure,
	// which is guarded against clobbering a definitive entry.
	checker.ocspMu.Lock()
	if existing, ok := checker.ocspCache[cacheKey]; !ok || existing.unavailable || !time.Now().Before(existing.expiresAt) {
		checker.ocspCache[cacheKey] = &ocspCacheEntry{
			unavailable: true,
			expiresAt:   time.Now().Add(ocspUnavailableTTL),
		}
	}
	after := checker.ocspCache[cacheKey]
	checker.ocspMu.Unlock()
	if after == nil || after.unavailable || !after.revoked {
		t.Fatalf("definitive revoked entry must survive responder outage, got %+v", after)
	}

	// End to end: the cache now still holds the definitive revoked verdict, so
	// a handshake check in soft-fail mode reports revoked, not "unknown".
	revoked, err := checker.IsRevoked(leaf)
	if err != nil {
		t.Fatalf("definitive cache hit should not error: %v", err)
	}
	if !revoked {
		t.Fatal("definitive revoked verdict must win over a later unavailable result")
	}

	// Conversely, an EXPIRED definitive entry is stale and may be replaced by a
	// negative entry on query failure (correct: the verdict is no longer valid).
	checker.ocspMu.Lock()
	checker.ocspCache[cacheKey].expiresAt = time.Now().Add(-time.Second)
	checker.ocspMu.Unlock()
	revoked, err = checker.IsRevoked(leaf)
	if err != nil {
		t.Fatalf("soft-fail re-query of expired entry should not error: %v", err)
	}
	if revoked {
		t.Fatal("expired entry + down responder is unknown, not revoked, in soft-fail")
	}
	checker.ocspMu.Lock()
	stale := checker.ocspCache[cacheKey]
	checker.ocspMu.Unlock()
	if stale == nil || !stale.unavailable {
		t.Fatalf("expired definitive entry should be replaced by a negative entry, got %+v", stale)
	}
}

// TestOCSPNoIssuerCert verifies that OCSP returns an error when no
// issuer certificate is configured.
func TestOCSPNoIssuerCert(t *testing.T) {
	ca := newTestCA(t)
	leaf := ca.issueLeaf(t, 55)

	checker := NewChecker(Config{
		Mode:             ModeOCSP,
		OCSPResponderURL: "http://localhost:1/unused",
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

// TestModeBothCRLOnlyRevokedWins verifies that when only CRL reports
// revoked but OCSP says good, ModeBoth returns not revoked.
func TestModeBothCRLOnlyRevokedWins(t *testing.T) {
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
	if !revoked {
		t.Fatal("CRL says revoked: ModeBoth must treat revoked-by-either as revoked")
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
		AllowSoftFail:      true,
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

	unknownLeaf := ca.issueLeaf(t, 67)

	crlData := ca.createCRL(t, 66)
	crlPath := writeCRLFile(t, dir, "crl.pem", crlData)

	checker := NewChecker(Config{
		CRLPaths:           []string{crlPath},
		OCSPResponderURL:   "http://127.0.0.1:1/unreachable",
		OCSPTimeoutSeconds: 1,
		Mode:               ModeBoth,
	}, ca.cert)
	if err := checker.ReloadCRLs(); err != nil {
		t.Fatalf("ReloadCRLs: %v", err)
	}

	// The CRL-revoked cert is revoked regardless of OCSP availability.
	revoked, err := checker.IsRevoked(leaf)
	if err != nil || !revoked {
		t.Fatalf("CRL-revoked cert: revoked=%v err=%v, want true/nil", revoked, err)
	}

	// An unknown cert fails closed: its status cannot be established
	// while the OCSP responder is unreachable.
	_, err = checker.IsRevoked(unknownLeaf)
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
// is invoked when accessed via OnCertRevoked.
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

	// The callback is reached via Checker.OnCertRevoked in the TLS wiring
	// (tls.go captures it once when building VerifyConnection); test it the
	// same way rather than through the config value.
	onRevoked := checker.OnCertRevoked()
	if onRevoked == nil {
		t.Fatal("OnCertRevoked should be set")
	}
	onRevoked(serial)

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
	if errors.Is(err, ErrOCSPUnavailable) {
		t.Fatal("CertificateRevokedError should not match ErrOCSPUnavailable")
	}
	if err.Error() != "revocation: certificate serial abc is revoked" {
		t.Fatalf("unexpected error message: %s", err.Error())
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
	if errors.Is(err, ErrCertificateRevoked) {
		t.Fatal("OCSPUnavailableError should not match ErrCertificateRevoked")
	}
	expected := "revocation: OCSP responder http://example.com unavailable: connection refused"
	if err.Error() != expected {
		t.Fatalf("unexpected error message: %s", err.Error())
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
	if errors.Is(err, ErrOCSPUnavailable) {
		t.Fatal("CRLParseError should not match ErrOCSPUnavailable")
	}
	expected := "revocation: failed to parse CRL /tmp/crl.pem: asn1 decode"
	if err.Error() != expected {
		t.Fatalf("unexpected error message: %s", err.Error())
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

// crlRevoked reports whether serial is in the checker's revoked set,
// ignoring CRL staleness (test convenience wrapper).
func crlRevoked(c *Checker, serial string) bool {
	revoked, _ := c.checkCRLStatus(anyIssuerScope, serial)
	return revoked
}

// crlRevokedFor reports whether leaf is in the checker's revoked set under the
// scope of the CA that issued it, ignoring CRL staleness. CRL-published
// revocations are keyed per issuer, so a lookup for a CA-issued leaf must
// name that CA.
func crlRevokedFor(c *Checker, leaf *x509.Certificate) bool {
	revoked, _ := c.checkCRLStatus(c.scopeFor(leaf), leaf.SerialNumber.Text(16))
	return revoked
}

// ocspKey returns the issuer-scoped OCSP cache key the checker uses for leaf,
// so tests that seed or inspect the cache address the same entry the
// production path does.
func ocspKey(c *Checker, leaf *x509.Certificate) string {
	return scopedSerial(c.scopeFor(leaf), leaf.SerialNumber.Text(16))
}

// ---------------------------------------------------------------------------
// CRL signature verification (checker.go ReloadCRLs)
// ---------------------------------------------------------------------------

// createCRLWithWindow creates a PEM-encoded CRL with an explicit validity
// window, for freshness testing.
func (ca *testCA) createCRLWithWindow(t *testing.T, thisUpdate, nextUpdate time.Time, serials ...int64) []byte {
	t.Helper()
	var entries []x509.RevocationListEntry
	for _, s := range serials {
		entries = append(entries, x509.RevocationListEntry{
			SerialNumber:   big.NewInt(s),
			RevocationTime: thisUpdate,
		})
	}
	tmpl := &x509.RevocationList{
		RevokedCertificateEntries: entries,
		Number:                    big.NewInt(2),
		ThisUpdate:                thisUpdate,
		NextUpdate:                nextUpdate,
	}
	crlDER, err := x509.CreateRevocationList(rand.Reader, tmpl, ca.cert, ca.key)
	if err != nil {
		t.Fatalf("create CRL: %v", err)
	}
	return pem.EncodeToMemory(&pem.Block{Type: "X509 CRL", Bytes: crlDER})
}

// TestCRLSignatureVerificationRejectsForeignCRL verifies that a CRL signed
// by a different CA fails the reload with ErrCRLSignature and that
// enforcement then fails closed (ErrCRLStale): a forged CRL file must never
// replace the revoked set.
func TestCRLSignatureVerificationRejectsForeignCRL(t *testing.T) {
	ca := newTestCA(t)    // trusted issuer
	rogue := newTestCA(t) // signs the forged CRL
	dir := t.TempDir()

	path := writeCRLFile(t, dir, "forged.crl.pem", rogue.createCRL(t, 55))
	checker := NewChecker(Config{
		CRLPaths: []string{path},
		Mode:     ModeCRL,
	}, ca.cert)

	err := checker.ReloadCRLs()
	if err == nil {
		t.Fatal("ReloadCRLs must reject a CRL signed by a foreign CA")
	}
	if !errors.Is(err, ErrCRLSignature) {
		t.Fatalf("expected ErrCRLSignature, got %v", err)
	}
	if checker.RevokedCount() != 0 {
		t.Fatalf("forged CRL must not populate the revoked set, got %d serials", checker.RevokedCount())
	}

	// Fail closed: with no fresh verified CRL, status is unknown.
	leaf := ca.issueLeaf(t, 56)
	if _, err := checker.IsRevoked(leaf); !errors.Is(err, ErrCRLStale) {
		t.Fatalf("enforcement must fail closed on unverified CRL, got %v", err)
	}
}

// TestCRLReloadNilIssuerFailsClosed verifies that loading CRL files without
// an issuer certificate to verify their signatures fails the reload rather
// than trusting the file contents unverified.
func TestCRLReloadNilIssuerFailsClosed(t *testing.T) {
	ca := newTestCA(t)
	dir := t.TempDir()
	path := writeCRLFile(t, dir, "unverifiable.crl.pem", ca.createCRL(t, 61))

	checker := NewChecker(Config{CRLPaths: []string{path}}, nil)
	err := checker.ReloadCRLs()
	if err == nil {
		t.Fatal("ReloadCRLs must fail without an issuer certificate to verify the CRL")
	}
	if !errors.Is(err, ErrCRLSignature) {
		t.Fatalf("expected ErrCRLSignature, got %v", err)
	}
}

// ---------------------------------------------------------------------------
// CRL NextUpdate freshness (checker.go ReloadCRLs)
// ---------------------------------------------------------------------------

// TestCRLExpiredNextUpdateFailsClosed verifies that a CRL past its
// NextUpdate is treated as stale: the reload fails with ErrCRLExpired (which
// also matches ErrCRLStale) and enforcement fails closed. Without this, a
// frozen CRL file would serve an ever-staler view of revocations forever.
func TestCRLExpiredNextUpdateFailsClosed(t *testing.T) {
	ca := newTestCA(t)
	dir := t.TempDir()

	// Expired well beyond the clock-skew allowance.
	crlData := ca.createCRLWithWindow(t,
		time.Now().Add(-2*time.Hour), time.Now().Add(-1*time.Hour), 77)
	path := writeCRLFile(t, dir, "expired.crl.pem", crlData)

	checker := NewChecker(Config{
		CRLPaths: []string{path},
		Mode:     ModeCRL,
	}, ca.cert)

	err := checker.ReloadCRLs()
	if err == nil {
		t.Fatal("ReloadCRLs must reject a CRL past its NextUpdate")
	}
	if !errors.Is(err, ErrCRLExpired) {
		t.Fatalf("expected ErrCRLExpired, got %v", err)
	}
	if !errors.Is(err, ErrCRLStale) {
		t.Fatalf("an expired CRL must also match ErrCRLStale, got %v", err)
	}

	leaf := ca.issueLeaf(t, 78)
	if _, err := checker.IsRevoked(leaf); !errors.Is(err, ErrCRLStale) {
		t.Fatalf("enforcement must fail closed on expired CRL, got %v", err)
	}

	// A fresh CRL within its window loads fine (skew allowance covers a
	// just-issued CRL).
	fresh := ca.createCRLWithWindow(t, time.Now().Add(-time.Minute), time.Now().Add(time.Hour), 77)
	if err := os.WriteFile(path, fresh, 0o600); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}
	if err := checker.ReloadCRLs(); err != nil {
		t.Fatalf("fresh CRL must reload cleanly: %v", err)
	}
}

// ---------------------------------------------------------------------------
// OCSP response freshness (checker.go validateOCSPFreshness / queryOCSP)
// ---------------------------------------------------------------------------

// newStaleOCSPServer serves signed responses with an explicit validity
// window, for replay/freshness testing.
func newStaleOCSPServer(t *testing.T, ca *testCA, thisUpdate, nextUpdate time.Time) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body := make([]byte, r.ContentLength)
		_, _ = r.Body.Read(body)
		req, err := ocsp.ParseRequest(body)
		if err != nil {
			http.Error(w, "bad request", http.StatusBadRequest)
			return
		}
		tmpl := ocsp.Response{
			Status:       ocsp.Good,
			SerialNumber: req.SerialNumber,
			ThisUpdate:   thisUpdate,
			NextUpdate:   nextUpdate,
		}
		respBytes, err := ocsp.CreateResponse(ca.cert, ca.cert, tmpl, ca.key)
		if err != nil {
			http.Error(w, "create response error", http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/ocsp-response")
		_, _ = w.Write(respBytes)
	}))
}

// TestOCSPStaleResponseTreatedAsUnavailable verifies that a replayed stale
// "good" response (NextUpdate in the past) is treated as status-unavailable:
// enforcement fails closed instead of accepting the stale verdict as "good".
func TestOCSPStaleResponseTreatedAsUnavailable(t *testing.T) {
	ca := newTestCA(t)
	leaf := ca.issueLeaf(t, 501)

	server := newStaleOCSPServer(t, ca,
		time.Now().Add(-2*time.Hour), time.Now().Add(-1*time.Hour))
	defer server.Close()

	checker := NewChecker(Config{
		Mode:               ModeOCSP,
		OCSPResponderURL:   server.URL,
		OCSPTimeoutSeconds: 5,
	}, ca.cert)

	_, err := checker.IsRevoked(leaf)
	if err == nil {
		t.Fatal("stale OCSP response must fail closed under enforcement")
	}
	if !errors.Is(err, ErrOCSPUnavailable) {
		t.Fatalf("stale response must surface as status-unavailable, got %v", err)
	}
	if !errors.Is(err, ErrOCSPResponseStale) {
		t.Fatalf("expected ErrOCSPResponseStale in the chain, got %v", err)
	}

	// Soft-fail: accepted, but never as a trusted "good" (no error, not
	// revoked — identical to responder-down behavior).
	softChecker := NewChecker(Config{
		Mode:               ModeOCSP,
		OCSPResponderURL:   server.URL,
		OCSPTimeoutSeconds: 5,
		AllowSoftFail:      true,
	}, ca.cert)
	revoked, err := softChecker.IsRevoked(leaf)
	if err != nil || revoked {
		t.Fatalf("soft-fail on stale response: revoked=%v err=%v, want false/nil", revoked, err)
	}
}

// TestValidateOCSPFreshness exercises the freshness window logic directly:
// skew allowance on both edges and the bounded maximum age applied when
// NextUpdate is absent (RFC 6960 gives such responses no expiry).
func TestValidateOCSPFreshness(t *testing.T) {
	now := time.Now()

	fresh := &ocsp.Response{ThisUpdate: now.Add(-time.Hour), NextUpdate: now.Add(time.Hour)}
	if err := validateOCSPFreshness(fresh, now); err != nil {
		t.Fatalf("fresh response must validate: %v", err)
	}

	future := &ocsp.Response{ThisUpdate: now.Add(time.Hour), NextUpdate: now.Add(2 * time.Hour)}
	if err := validateOCSPFreshness(future, now); !errors.Is(err, ErrOCSPResponseStale) {
		t.Fatalf("not-yet-valid response must be stale, got %v", err)
	}

	// ThisUpdate within the skew allowance is accepted.
	skewed := &ocsp.Response{ThisUpdate: now.Add(revocationClockSkew / 2), NextUpdate: now.Add(time.Hour)}
	if err := validateOCSPFreshness(skewed, now); err != nil {
		t.Fatalf("response within clock skew must validate: %v", err)
	}

	expired := &ocsp.Response{ThisUpdate: now.Add(-2 * time.Hour), NextUpdate: now.Add(-time.Hour)}
	if err := validateOCSPFreshness(expired, now); !errors.Is(err, ErrOCSPResponseStale) {
		t.Fatalf("expired response must be stale, got %v", err)
	}

	// NextUpdate just past but within skew is accepted.
	justPast := &ocsp.Response{ThisUpdate: now.Add(-time.Hour), NextUpdate: now.Add(-revocationClockSkew / 2)}
	if err := validateOCSPFreshness(justPast, now); err != nil {
		t.Fatalf("NextUpdate within clock skew must validate: %v", err)
	}

	// Absent NextUpdate: bounded maximum age applies.
	noNextFresh := &ocsp.Response{ThisUpdate: now.Add(-time.Hour)}
	if err := validateOCSPFreshness(noNextFresh, now); err != nil {
		t.Fatalf("recent response without NextUpdate must validate: %v", err)
	}
	noNextOld := &ocsp.Response{ThisUpdate: now.Add(-ocspNoNextUpdateMaxAge - time.Hour)}
	if err := validateOCSPFreshness(noNextOld, now); !errors.Is(err, ErrOCSPResponseStale) {
		t.Fatalf("old response without NextUpdate must be stale, got %v", err)
	}

	// Zero ThisUpdate carries no provable freshness at all.
	zero := &ocsp.Response{}
	if err := validateOCSPFreshness(zero, now); !errors.Is(err, ErrOCSPResponseStale) {
		t.Fatalf("response without ThisUpdate must be stale, got %v", err)
	}
}

// ---------------------------------------------------------------------------
// OCSP cache bound (checker.go evictOCSPCacheLocked)
// ---------------------------------------------------------------------------

// TestOCSPCacheBounded verifies that the OCSP cache cannot grow without
// limit: at the bound, expired entries are pruned first and the entries
// closest to expiry are evicted to make room.
func TestOCSPCacheBounded(t *testing.T) {
	checker := NewChecker(Config{}, nil)
	now := time.Now()

	// Fill to the bound; "serial-0" is already expired, the rest are not.
	for i := 0; i < maxOCSPCacheEntries; i++ {
		checker.ocspCache[fmt.Sprintf("serial-%d", i)] = &ocspCacheEntry{
			expiresAt: now.Add(time.Duration(i) * time.Second),
		}
	}

	checker.ocspMu.Lock()
	checker.evictOCSPCacheLocked(now)
	checker.ocspMu.Unlock()

	if len(checker.ocspCache) >= maxOCSPCacheEntries {
		t.Fatalf("cache not bounded: %d entries", len(checker.ocspCache))
	}
	if _, ok := checker.ocspCache["serial-0"]; ok {
		t.Fatal("expired entry must be pruned first")
	}

	// With no expired entries, the closest-to-expiry entry is evicted.
	checker.ocspCache = map[string]*ocspCacheEntry{}
	for i := 0; i < maxOCSPCacheEntries; i++ {
		checker.ocspCache[fmt.Sprintf("fresh-%d", i)] = &ocspCacheEntry{
			expiresAt: now.Add(time.Hour + time.Duration(i)*time.Second),
		}
	}
	checker.ocspMu.Lock()
	checker.evictOCSPCacheLocked(now)
	checker.ocspMu.Unlock()
	if len(checker.ocspCache) >= maxOCSPCacheEntries {
		t.Fatalf("cache not bounded without expired entries: %d entries", len(checker.ocspCache))
	}
	if _, ok := checker.ocspCache["fresh-0"]; ok {
		t.Fatal("entry closest to expiry must be evicted when nothing is expired")
	}
}

// ---------------------------------------------------------------------------
// Start idempotency (checker.go Start)
// ---------------------------------------------------------------------------

// TestCheckerStartIdempotent verifies that repeated Start calls do not spawn
// additional reload goroutines, matching Start's documented idempotency.
func TestCheckerStartIdempotent(t *testing.T) {
	ca := newTestCA(t)
	dir := t.TempDir()
	path := writeCRLFile(t, dir, "idem.crl.pem", ca.createCRL(t, 91))

	checker := NewChecker(Config{
		CRLPaths:      []string{path},
		CheckInterval: time.Hour,
	}, ca.cert)
	checker.Start()
	checker.Start()
	checker.Start()
	defer checker.Stop()

	// Each Start that spawns a reload goroutine has done so by the time it
	// returns, and a goroutine shows in the dump from its creation, so the
	// count is read as soon as one is listed rather than after a sleep.
	reloadLoops := func() int {
		buf := make([]byte, 1<<20)
		n := runtime.Stack(buf, true)
		return strings.Count(string(buf[:n]), ").reloadLoop(")
	}
	got := reloadLoops()
	for deadline := time.Now().Add(5 * time.Second); got == 0; got = reloadLoops() {
		if time.Now().After(deadline) {
			t.Fatal("no reloadLoop goroutine after Start")
		}
		runtime.Gosched()
	}
	if got != 1 {
		t.Fatalf("expected exactly 1 reloadLoop goroutine after repeated Start, got %d", got)
	}
}

// ---------------------------------------------------------------------------
// Configured OCSP request timeout (checker.go ocspRequestTimeout)
// ---------------------------------------------------------------------------

// TestOCSPRequestTimeoutUsesConfigured verifies that the per-request OCSP
// context timeout honors OCSPTimeoutSeconds instead of a hardcoded value
// (which silently capped operator-configured timeouts above 10s).
func TestOCSPRequestTimeoutUsesConfigured(t *testing.T) {
	checker := NewChecker(Config{OCSPTimeoutSeconds: 30}, nil)
	if got := checker.ocspRequestTimeout(); got != 30*time.Second {
		t.Fatalf("ocspRequestTimeout = %v, want 30s", got)
	}

	defaulted := NewChecker(Config{}, nil)
	if got := defaulted.ocspRequestTimeout(); got != defaultOCSPTimeoutSeconds*time.Second {
		t.Fatalf("default ocspRequestTimeout = %v, want %ds", got, defaultOCSPTimeoutSeconds)
	}

	// Backstop for a zero value bypassing SetDefaults.
	zero := &Checker{cfg: Config{}}
	if got := zero.ocspRequestTimeout(); got != defaultOCSPTimeoutSeconds*time.Second {
		t.Fatalf("backstop ocspRequestTimeout = %v, want %ds", got, defaultOCSPTimeoutSeconds)
	}
}

// ---------------------------------------------------------------------------
// Two-tier PKI (root + issuing intermediate) helpers and regression tests
// ---------------------------------------------------------------------------

// newTestIntermediate issues an intermediate CA signed by root. Leaves and
// CRLs produced by the returned testCA are signed by the intermediate, which
// is the arrangement a standard two-tier PKI deploys.
func newTestIntermediate(t *testing.T, root *testCA) *testCA {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generate intermediate key: %v", err)
	}
	tmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(2),
		Subject:               pkix.Name{Organization: []string{"Test Issuing CA"}},
		NotBefore:             time.Now().Add(-1 * time.Minute),
		NotAfter:              time.Now().Add(1 * time.Hour),
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageCRLSign,
		BasicConstraintsValid: true,
		IsCA:                  true,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, root.cert, &key.PublicKey, root.key)
	if err != nil {
		t.Fatalf("create intermediate cert: %v", err)
	}
	cert, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatalf("parse intermediate cert: %v", err)
	}
	return &testCA{
		key:  key,
		cert: cert,
		pem:  pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}),
	}
}

// TestIssuerSelectedPerLeafInTwoTierPKI is the regression test: with a
// root-first CA bundle and leaves signed by the issuing intermediate, the
// checker must select the INTERMEDIATE as the issuer. Wiring revocation to the
// bundle's first block (the root) builds OCSP requests whose
// issuerNameHash/issuerKeyHash no responder can match and rejects every
// intermediate-signed CRL.
func TestIssuerSelectedPerLeafInTwoTierPKI(t *testing.T) {
	root := newTestCA(t)
	intermediate := newTestIntermediate(t, root)
	leaf := intermediate.issueLeaf(t, 500)

	// Bundle order is root-first, exactly as a CA publishes it.
	checker := NewChecker(Config{}, root.cert, intermediate.cert)

	got := checker.issuerFor(leaf)
	if got == nil {
		t.Fatal("no issuer selected for an intermediate-signed leaf")
	}
	if !got.Equal(intermediate.cert) {
		t.Fatalf("issuerFor selected %q, want the issuing intermediate",
			got.Subject.Organization)
	}
}

// TestOCSPWorksWithIntermediateIssuedLeaf verifies the end-to-end effect of
// per-leaf issuer selection: an OCSP responder speaking for the intermediate
// resolves a leaf's status. With only the root wired as issuer this query
// could never match, making OCSP a silent no-op (or, under enforcement, a
// total handshake outage).
func TestOCSPWorksWithIntermediateIssuedLeaf(t *testing.T) {
	root := newTestCA(t)
	intermediate := newTestIntermediate(t, root)
	leaf := intermediate.issueLeaf(t, 501)
	serialHex := leaf.SerialNumber.Text(16)

	server := newMockOCSPServer(t, intermediate, map[string]bool{serialHex: true})
	defer server.Close()

	checker := NewChecker(Config{
		Mode:               ModeOCSP,
		OCSPResponderURL:   server.URL,
		OCSPTimeoutSeconds: 5,
	}, root.cert, intermediate.cert)

	revoked, err := checker.IsRevoked(leaf)
	if err != nil {
		t.Fatalf("IsRevoked error: %v", err)
	}
	if !revoked {
		t.Fatal("intermediate-issued leaf should be reported revoked via OCSP")
	}
}

// TestCRLSignedByIntermediateAccepted verifies the CRL half of per-leaf issuer selection: a CRL
// signed by the issuing intermediate must load successfully when the bundle
// contains both the root and the intermediate. Verifying only against the
// root would mark the set stale on every reload interval, so under
// enforcement every handshake would fail with ErrCRLStale.
func TestCRLSignedByIntermediateAccepted(t *testing.T) {
	root := newTestCA(t)
	intermediate := newTestIntermediate(t, root)
	dir := t.TempDir()
	path := writeCRLFile(t, dir, "inter.crl.pem", intermediate.createCRL(t, 601))

	checker := NewChecker(Config{
		CRLPaths: []string{path},
		Mode:     ModeCRL,
	}, root.cert, intermediate.cert)

	if err := checker.ReloadCRLs(); err != nil {
		t.Fatalf("intermediate-signed CRL should load: %v", err)
	}
	leaf := intermediate.issueLeaf(t, 601)
	revoked, err := checker.IsRevoked(leaf)
	if err != nil {
		t.Fatalf("IsRevoked error: %v", err)
	}
	if !revoked {
		t.Fatal("serial listed in the intermediate-signed CRL should be revoked")
	}
}

// ---------------------------------------------------------------------------
// OCSP "unknown" status (fail-closed) regression tests
// ---------------------------------------------------------------------------

// newUnknownOCSPServer returns a responder that reports RFC 6960 "unknown"
// for every serial, counting the queries it receives.
func newUnknownOCSPServer(t *testing.T, ca *testCA, queries *atomic.Int64) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		queries.Add(1)
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
		tmpl := ocsp.Response{
			Status:       ocsp.Unknown,
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
		_, _ = w.Write(respBytes)
	}))
}

// TestOCSPUnknownStatusFailsClosedUnderEnforcement is the regression test:
// RFC 6960 "unknown" is NOT "not revoked". Under enforcement a leaf the
// responder has no record of must be rejected, not admitted.
func TestOCSPUnknownStatusFailsClosedUnderEnforcement(t *testing.T) {
	ca := newTestCA(t)
	leaf := ca.issueLeaf(t, 700)

	var queries atomic.Int64
	server := newUnknownOCSPServer(t, ca, &queries)
	defer server.Close()

	checker := NewChecker(Config{
		Mode:               ModeOCSP,
		OCSPResponderURL:   server.URL,
		OCSPTimeoutSeconds: 5,
	}, ca.cert)

	revoked, err := checker.IsRevoked(leaf)
	if err == nil {
		t.Fatal("unknown OCSP status must fail closed under enforcement")
	}
	if revoked {
		t.Fatal("unknown status must not be reported as revoked")
	}
	if !errors.Is(err, ErrOCSPStatusUnknown) && !errors.Is(err, ErrOCSPUnavailable) {
		t.Fatalf("expected an unknown/unavailable status error, got %v", err)
	}
}

// TestOCSPUnknownStatusNotCachedAsGood verifies the second half: an
// unknown verdict must never populate the definitive cache. If it did, the
// "good" verdict would be served for the full OCSPCacheSeconds to every
// subsequent handshake. Only the short-TTL negative cache may hold it, which
// still fails closed under enforcement.
func TestOCSPUnknownStatusNotCachedAsGood(t *testing.T) {
	ca := newTestCA(t)
	leaf := ca.issueLeaf(t, 701)

	var queries atomic.Int64
	server := newUnknownOCSPServer(t, ca, &queries)
	defer server.Close()

	checker := NewChecker(Config{
		Mode:               ModeOCSP,
		OCSPResponderURL:   server.URL,
		OCSPCacheSeconds:   3600,
		OCSPTimeoutSeconds: 5,
	}, ca.cert)

	if _, err := checker.IsRevoked(leaf); err == nil {
		t.Fatal("unknown status must fail closed")
	}

	checker.ocspMu.RLock()
	entry, cached := checker.ocspCache[ocspKey(checker, leaf)]
	checker.ocspMu.RUnlock()
	if cached && !entry.unavailable {
		t.Fatal("unknown status must not be cached as a definitive verdict")
	}
	if cached && entry.expiresAt.After(time.Now().Add(ocspUnavailableTTL+time.Second)) {
		t.Fatal("unknown status cached with the long definitive TTL")
	}

	// The cached entry must keep failing closed rather than admitting the peer.
	if _, err := checker.IsRevoked(leaf); err == nil {
		t.Fatal("cached unknown status must still fail closed")
	}
}

// TestOCSPUnknownStatusSoftFailAllows documents the soft-fail behaviour: with
// AllowSoftFail an unknown status is a status-unavailable result, so the peer
// is admitted — the same treatment as an unreachable responder, rather than a
// definitive "not revoked".
func TestOCSPUnknownStatusSoftFailAllows(t *testing.T) {
	ca := newTestCA(t)
	leaf := ca.issueLeaf(t, 702)

	var queries atomic.Int64
	server := newUnknownOCSPServer(t, ca, &queries)
	defer server.Close()

	checker := NewChecker(Config{
		Mode:               ModeOCSP,
		OCSPResponderURL:   server.URL,
		OCSPTimeoutSeconds: 5,
		AllowSoftFail:      true,
	}, ca.cert)

	revoked, err := checker.IsRevoked(leaf)
	if err != nil {
		t.Fatalf("soft-fail must not surface an error: %v", err)
	}
	if revoked {
		t.Fatal("unknown status must not be reported as revoked")
	}
}

// ---------------------------------------------------------------------------
// Empty-CRL, post-dated CRL, cache TTL, mode validation regression tests
// ---------------------------------------------------------------------------

// TestReloadCRLsRejectsFileWithNoCRLBlocks is the regression test:
// replacing a CRL with a PEM that holds no "X509 CRL" block must NOT commit a
// fresh empty revoked set. Doing so would un-revoke every listed certificate
// and mark the set current, so a fail-closed deployment would silently accept
// revoked peers.
func TestReloadCRLsRejectsFileWithNoCRLBlocks(t *testing.T) {
	ca := newTestCA(t)
	dir := t.TempDir()
	path := writeCRLFile(t, dir, "rot.crl.pem", ca.createCRL(t, 800))

	checker := NewChecker(Config{
		CRLPaths: []string{path},
		Mode:     ModeCRL,
	}, ca.cert)
	if err := checker.ReloadCRLs(); err != nil {
		t.Fatalf("initial reload: %v", err)
	}
	leaf := ca.issueLeaf(t, 800)
	if revoked, _ := checker.IsRevoked(leaf); !revoked {
		t.Fatal("setup: serial 800 should be revoked after the initial load")
	}

	// Swap in a file containing only the CA certificate: parseable PEM, zero
	// CRL blocks.
	if err := os.WriteFile(path, ca.pem, 0600); err != nil {
		t.Fatalf("overwrite CRL file: %v", err)
	}
	err := checker.ReloadCRLs()
	if err == nil {
		t.Fatal("a CRL file with no CRL blocks must be rejected")
	}
	if !errors.Is(err, ErrNoCRLInFile) {
		t.Fatalf("expected ErrNoCRLInFile, got %v", err)
	}

	// The previous set survives, and the set is marked stale so enforcement
	// fails closed for everything else.
	if revoked, _ := checker.IsRevoked(leaf); !revoked {
		t.Fatal("previous revoked set must be preserved after a rejected reload")
	}
	other := ca.issueLeaf(t, 801)
	if _, err := checker.IsRevoked(other); !errors.Is(err, ErrCRLStale) {
		t.Fatalf("expected ErrCRLStale for an unlisted serial, got %v", err)
	}
}

// TestReloadCRLsRejectsPostDatedCRL verifies the ThisUpdate check: a CRL dated
// in the future proves nothing about the present and must mark the set stale,
// mirroring validateOCSPFreshness on the OCSP side.
func TestReloadCRLsRejectsPostDatedCRL(t *testing.T) {
	ca := newTestCA(t)
	dir := t.TempDir()

	tmpl := &x509.RevocationList{
		Number:     big.NewInt(1),
		ThisUpdate: time.Now().Add(2 * time.Hour),
		NextUpdate: time.Now().Add(4 * time.Hour),
	}
	crlDER, err := x509.CreateRevocationList(rand.Reader, tmpl, ca.cert, ca.key)
	if err != nil {
		t.Fatalf("create post-dated CRL: %v", err)
	}
	path := writeCRLFile(t, dir, "future.crl.pem",
		pem.EncodeToMemory(&pem.Block{Type: "X509 CRL", Bytes: crlDER}))

	checker := NewChecker(Config{
		CRLPaths: []string{path},
		Mode:     ModeCRL,
	}, ca.cert)

	reloadErr := checker.ReloadCRLs()
	if !errors.Is(reloadErr, ErrCRLNotYetValid) {
		t.Fatalf("expected ErrCRLNotYetValid, got %v", reloadErr)
	}
	if !errors.Is(reloadErr, ErrCRLStale) {
		t.Fatalf("a post-dated CRL must also satisfy ErrCRLStale, got %v", reloadErr)
	}
}

// TestOCSPCacheTTLBoundedByNextUpdate verifies that a definitive OCSP verdict
// never outlives the response's own NextUpdate, even when OCSPCacheSeconds is
// far larger.
func TestOCSPCacheTTLBoundedByNextUpdate(t *testing.T) {
	ca := newTestCA(t)
	leaf := ca.issueLeaf(t, 900)
	nextUpdate := time.Now().Add(30 * time.Second)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body := make([]byte, r.ContentLength)
		_, _ = r.Body.Read(body)
		req, _ := ocsp.ParseRequest(body)
		tmpl := ocsp.Response{
			Status:       ocsp.Good,
			SerialNumber: req.SerialNumber,
			ThisUpdate:   time.Now().Add(-1 * time.Minute),
			NextUpdate:   nextUpdate,
		}
		respBytes, _ := ocsp.CreateResponse(ca.cert, ca.cert, tmpl, ca.key)
		w.Header().Set("Content-Type", "application/ocsp-response")
		_, _ = w.Write(respBytes)
	}))
	defer server.Close()

	checker := NewChecker(Config{
		Mode:               ModeOCSP,
		OCSPResponderURL:   server.URL,
		OCSPCacheSeconds:   86400, // far beyond NextUpdate
		OCSPTimeoutSeconds: 5,
	}, ca.cert)

	if _, err := checker.IsRevoked(leaf); err != nil {
		t.Fatalf("IsRevoked error: %v", err)
	}
	checker.ocspMu.RLock()
	entry, cached := checker.ocspCache[ocspKey(checker, leaf)]
	checker.ocspMu.RUnlock()
	if !cached {
		t.Fatal("expected a cached definitive verdict")
	}
	if entry.expiresAt.After(nextUpdate.Add(time.Second)) {
		t.Fatalf("cache expiry %s outlives the response NextUpdate %s",
			entry.expiresAt, nextUpdate)
	}
}

// TestModeOCSPConsultsRevokedSet verifies that AddRevokedSerial — the runtime
// admin revocation path — is honoured in ModeOCSP. Ignoring it there made
// programmatic revocation a silent no-op.
func TestModeOCSPConsultsRevokedSet(t *testing.T) {
	ca := newTestCA(t)
	leaf := ca.issueLeaf(t, 1000)

	server := newMockOCSPServer(t, ca, map[string]bool{}) // responder says good
	defer server.Close()

	checker := NewChecker(Config{
		Mode:               ModeOCSP,
		OCSPResponderURL:   server.URL,
		OCSPTimeoutSeconds: 5,
	}, ca.cert)

	checker.AddRevokedSerial(leaf.SerialNumber.Text(16))
	revoked, err := checker.IsRevoked(leaf)
	if err != nil {
		t.Fatalf("IsRevoked error: %v", err)
	}
	if !revoked {
		t.Fatal("programmatic revocation must be honoured in ModeOCSP")
	}
}

// TestConfigValidateRejectsUnsatisfiableModes is the regression test: a
// mode whose sources are not configured is unsatisfiable, and must be rejected
// at construction rather than failing every handshake at runtime (or silently
// checking nothing).
func TestConfigValidateRejectsUnsatisfiableModes(t *testing.T) {
	tests := []struct {
		name string
		cfg  Config
		want error
	}{
		{
			name: "ocsp without responder url",
			cfg:  Config{Mode: ModeOCSP},
			want: ErrNoOCSPResponderURL,
		},
		{
			name: "both without responder url",
			cfg:  Config{Mode: ModeBoth, CRLPaths: []string{"/tmp/x.crl"}},
			want: ErrNoOCSPResponderURL,
		},
		{
			name: "crl without crl paths",
			cfg:  Config{Mode: ModeCRL},
			want: ErrNoCRLSources,
		},
		{
			name: "both without crl paths",
			cfg:  Config{Mode: ModeBoth, OCSPResponderURL: "http://ocsp.example"},
			want: ErrNoCRLSources,
		},
		{
			name: "unrecognized mode",
			cfg:  Config{Mode: Mode("typo")},
			want: ErrInvalidRevocationMode,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			cfg := tc.cfg
			cfg.SetDefaults()
			if err := cfg.Validate(); !errors.Is(err, tc.want) {
				t.Fatalf("Validate() = %v, want %v", err, tc.want)
			}
		})
	}
}

// TestConfigValidateAcceptsSatisfiableModes verifies Validate does not reject
// a correctly-sourced configuration. The default mode names no specific
// source, so it is satisfiable either by having a source or by opting into
// soft-fail; the first two cases cover the latter.
func TestConfigValidateAcceptsSatisfiableModes(t *testing.T) {
	tests := []Config{
		{AllowSoftFail: true},
		{Mode: ModeAny, AllowSoftFail: true},
		{Mode: ModeCRL, CRLPaths: []string{"/tmp/x.crl"}},
		{Mode: ModeOCSP, OCSPResponderURL: "http://ocsp.example"},
		{Mode: ModeBoth, OCSPResponderURL: "http://ocsp.example", CRLPaths: []string{"/tmp/x.crl"}},
	}
	for i, cfg := range tests {
		cfg.SetDefaults()
		if err := cfg.Validate(); err != nil {
			t.Fatalf("case %d: Validate() = %v, want nil", i, err)
		}
	}
}

// ---------------------------------------------------------------------------
// Mode validation helper
// ---------------------------------------------------------------------------

// TestIsValidMode covers the recognized mode set including the empty string,
// which selects the default. Config.Validate depends on this to reject typos
// that would otherwise silently fall through to the default mode.
func TestIsValidMode(t *testing.T) {
	for _, m := range []Mode{"", ModeCRL, ModeOCSP, ModeBoth, ModeAny} {
		if !IsValidMode(m) {
			t.Fatalf("IsValidMode(%q) = false, want true", m)
		}
	}
	for _, m := range []Mode{"CRL", "strict", "ocsp ", "none"} {
		if IsValidMode(m) {
			t.Fatalf("IsValidMode(%q) = true, want false", m)
		}
	}
}

// ---------------------------------------------------------------------------
// Issuer selection fallbacks
// ---------------------------------------------------------------------------

// issueLeafWithAKI issues a leaf signed by signer but stamped with an explicit
// AuthorityKeyId. The parent copy has its SubjectKeyId cleared so that
// x509.CreateCertificate honors the template's AuthorityKeyId instead of
// deriving it from the signing certificate.
func issueLeafWithAKI(t *testing.T, signer *testCA, serial int64, aki []byte) *x509.Certificate {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generate leaf key: %v", err)
	}
	parent := *signer.cert
	parent.SubjectKeyId = nil
	tmpl := &x509.Certificate{
		SerialNumber:   big.NewInt(serial),
		Subject:        pkix.Name{CommonName: fmt.Sprintf("aki-leaf-%d", serial)},
		NotBefore:      time.Now().Add(-1 * time.Minute),
		NotAfter:       time.Now().Add(1 * time.Hour),
		KeyUsage:       x509.KeyUsageDigitalSignature,
		AuthorityKeyId: aki,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, &parent, &key.PublicKey, signer.key)
	if err != nil {
		t.Fatalf("create leaf cert: %v", err)
	}
	leaf, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatalf("parse leaf cert: %v", err)
	}
	return leaf
}

// TestIssuerForAuthorityKeyIdFallback covers the second selection stage: when
// no candidate's key verifies the leaf signature, the leaf's AuthorityKeyId is
// matched against candidate SubjectKeyIds. This keeps issuer selection working
// for signature algorithms this build cannot verify; a wrongly selected issuer
// still fails closed downstream because ocsp.ParseResponseForCert verifies the
// response against it.
func TestIssuerForAuthorityKeyIdFallback(t *testing.T) {
	trusted := newTestCA(t)
	rogue := newTestCA(t)
	if len(trusted.cert.SubjectKeyId) == 0 {
		t.Fatal("setup: trusted CA has no SubjectKeyId to match against")
	}

	leaf := issueLeafWithAKI(t, rogue, 1100, trusted.cert.SubjectKeyId)
	if leaf.CheckSignatureFrom(trusted.cert) == nil {
		t.Fatal("setup: leaf must not verify against the trusted CA")
	}

	checker := NewChecker(Config{}, trusted.cert)
	got := checker.issuerFor(leaf)
	if got == nil {
		t.Fatal("AuthorityKeyId fallback did not select an issuer")
	}
	if !got.Equal(trusted.cert) {
		t.Fatalf("issuerFor selected %v, want the AuthorityKeyId match", got.Subject)
	}
}

// TestIssuerForNoUsableIssuer verifies the fail-closed default: a bundle
// holding no issuer for the leaf (neither by signature nor AuthorityKeyId)
// yields nil, which callers turn into ErrNoIssuerCertificate.
func TestIssuerForNoUsableIssuer(t *testing.T) {
	trusted := newTestCA(t)
	rogue := newTestCA(t)
	leaf := issueLeafWithAKI(t, rogue, 1101, []byte{0xde, 0xad, 0xbe, 0xef})

	checker := NewChecker(Config{}, trusted.cert)
	if got := checker.issuerFor(leaf); got != nil {
		t.Fatalf("issuerFor = %v, want nil for a foreign leaf", got.Subject)
	}

	// The same condition surfaces as a typed error through the OCSP path: no
	// issuer means no request can be built and no verdict can be scoped.
	checker.cfg.OCSPResponderURL = "http://ocsp.invalid"
	if _, err := checker.checkOCSPRaw(leaf, checker.issuerFor(leaf)); !errors.Is(err, ErrNoIssuerCertificate) {
		t.Fatalf("checkOCSPRaw error = %v, want ErrNoIssuerCertificate", err)
	}
}

// ---------------------------------------------------------------------------
// Start / reloadLoop failure paths
// ---------------------------------------------------------------------------

// TestStartWithUnreadableCRLFailsClosed verifies that a failing initial load
// in Start does not panic and leaves the set stale, so enforcement
// rejects peers instead of treating "no CRL loaded" as "nothing revoked".
func TestStartWithUnreadableCRLFailsClosed(t *testing.T) {
	ca := newTestCA(t)
	missing := filepath.Join(t.TempDir(), "absent.crl.pem")

	checker := NewChecker(Config{
		CRLPaths:      []string{missing},
		CheckInterval: time.Hour,
		Mode:          ModeCRL,
	}, ca.cert)
	checker.Start()
	defer checker.Stop()

	leaf := ca.issueLeaf(t, 1200)
	if _, err := checker.IsRevoked(leaf); !errors.Is(err, ErrCRLStale) {
		t.Fatalf("IsRevoked error = %v, want ErrCRLStale after a failed initial load", err)
	}
}

// TestReloadLoopMarksStaleOnCorruptedFile verifies the periodic reload's
// failure branch: a CRL replaced with unparseable content while the checker is
// running must take the set stale within one interval, so a fail-closed
// deployment stops trusting "not in the set" as "not revoked".
func TestReloadLoopMarksStaleOnCorruptedFile(t *testing.T) {
	ca := newTestCA(t)
	dir := t.TempDir()
	path := writeCRLFile(t, dir, "corrupt.crl.pem", ca.createCRL(t, 1300))

	checker := NewChecker(Config{
		CRLPaths:      []string{path},
		CheckInterval: 10 * time.Millisecond,
		Mode:          ModeCRL,
	}, ca.cert)
	checker.Start()
	defer checker.Stop()

	require.Eventually(t, func() bool {
		return checker.RevokedCount() == 1
	}, 5*time.Second, 5*time.Millisecond, "initial background load")

	// Replace the CRL with a PEM block that is not a parseable CRL.
	writeCRLFile(t, dir, "corrupt.crl.pem",
		pem.EncodeToMemory(&pem.Block{Type: "X509 CRL", Bytes: []byte("not-a-crl")}))

	unlisted := ca.issueLeaf(t, 1301)
	require.Eventually(t, func() bool {
		_, err := checker.IsRevoked(unlisted)
		return errors.Is(err, ErrCRLStale)
	}, 5*time.Second, 5*time.Millisecond, "reload loop must mark the set stale")

	// The previously loaded revocation survives the failed reload.
	revoked, _ := checker.IsRevoked(ca.issueLeaf(t, 1300))
	if !revoked {
		t.Fatal("a stale set must keep serving known revocations")
	}
}

// ---------------------------------------------------------------------------
// ModeBoth composition
// ---------------------------------------------------------------------------

// TestModeBothOCSPOnlyRevokedWins covers the OCSP half of revoked-by-either:
// a fresh CRL that does not list the serial must not override an OCSP
// "revoked" verdict.
func TestModeBothOCSPOnlyRevokedWins(t *testing.T) {
	ca := newTestCA(t)
	dir := t.TempDir()
	leaf := ca.issueLeaf(t, 1400)
	serialHex := leaf.SerialNumber.Text(16)

	// CRL is fresh and valid but lists an unrelated serial.
	crlPath := writeCRLFile(t, dir, "other.crl.pem", ca.createCRL(t, 1499))
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
		t.Fatal("OCSP reports revoked: ModeBoth must treat revoked-by-either as revoked")
	}
}

// TestModeBothBothSourcesCleanPasses verifies the accept path: a fresh CRL and
// an OCSP "good" produce no error even under enforcement.
func TestModeBothBothSourcesCleanPasses(t *testing.T) {
	ca := newTestCA(t)
	dir := t.TempDir()
	leaf := ca.issueLeaf(t, 1401)

	crlPath := writeCRLFile(t, dir, "clean.crl.pem", ca.createCRL(t, 1499))
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
		t.Fatalf("clean sources must not error under enforcement: %v", err)
	}
	if revoked {
		t.Fatal("neither source reports revoked")
	}
}

// TestModeBothStaleCRLFailsClosed verifies that in ModeBoth an unusable CRL
// fails closed under enforcement even when OCSP answers "good": the revocation
// could be published in the CRL this checker cannot read.
func TestModeBothStaleCRLFailsClosed(t *testing.T) {
	ca := newTestCA(t)
	leaf := ca.issueLeaf(t, 1402)
	missing := filepath.Join(t.TempDir(), "absent.crl.pem")

	server := newMockOCSPServer(t, ca, map[string]bool{})
	defer server.Close()

	checker := NewChecker(Config{
		CRLPaths:           []string{missing},
		OCSPResponderURL:   server.URL,
		OCSPTimeoutSeconds: 5,
		Mode:               ModeBoth,
	}, ca.cert)

	revoked, err := checker.IsRevoked(leaf)
	if revoked {
		t.Fatal("no source reported revoked")
	}
	if !errors.Is(err, ErrCRLStale) {
		t.Fatalf("IsRevoked error = %v, want ErrCRLStale", err)
	}
}

// ---------------------------------------------------------------------------
// ModeAny with an OCSP responder configured
// ---------------------------------------------------------------------------

// TestModeAnyOCSPRevokedWins verifies that the default mode consults the OCSP
// responder when one is configured and honors a "revoked" verdict.
func TestModeAnyOCSPRevokedWins(t *testing.T) {
	ca := newTestCA(t)
	leaf := ca.issueLeaf(t, 1500)
	serialHex := leaf.SerialNumber.Text(16)

	server := newMockOCSPServer(t, ca, map[string]bool{serialHex: true})
	defer server.Close()

	checker := NewChecker(Config{
		Mode:               ModeAny,
		OCSPResponderURL:   server.URL,
		OCSPTimeoutSeconds: 5,
	}, ca.cert)

	revoked, err := checker.IsRevoked(leaf)
	if err != nil {
		t.Fatalf("IsRevoked error: %v", err)
	}
	if !revoked {
		t.Fatal("ModeAny must honor an OCSP revoked verdict")
	}
}

// TestModeAnyOCSPUnavailableEnforcedFailsClosed verifies that an unreachable
// responder rejects the peer under enforcement in the default mode.
func TestModeAnyOCSPUnavailableEnforcedFailsClosed(t *testing.T) {
	ca := newTestCA(t)
	leaf := ca.issueLeaf(t, 1501)

	checker := NewChecker(Config{
		Mode:               ModeAny,
		OCSPResponderURL:   "http://127.0.0.1:1/unreachable",
		OCSPTimeoutSeconds: 1,
	}, ca.cert)

	revoked, err := checker.IsRevoked(leaf)
	if revoked {
		t.Fatal("unavailable status must not be reported as revoked")
	}
	if !errors.Is(err, ErrOCSPUnavailable) {
		t.Fatalf("IsRevoked error = %v, want ErrOCSPUnavailable", err)
	}
}

// TestModeAnyOCSPUnavailableSoftFailAllows verifies the soft-fail counterpart:
// with enforcement off, an unreachable responder admits the peer.
func TestModeAnyOCSPUnavailableSoftFailAllows(t *testing.T) {
	ca := newTestCA(t)
	leaf := ca.issueLeaf(t, 1502)

	checker := NewChecker(Config{
		Mode:               ModeAny,
		OCSPResponderURL:   "http://127.0.0.1:1/unreachable",
		OCSPTimeoutSeconds: 1,
		AllowSoftFail:      true,
	}, ca.cert)

	revoked, err := checker.IsRevoked(leaf)
	if err != nil {
		t.Fatalf("soft-fail must not surface an error: %v", err)
	}
	if revoked {
		t.Fatal("unavailable status must not be reported as revoked")
	}
}

// TestModeAnyStaleCRLFailsClosedDespiteGoodOCSP verifies that in ModeAny a
// reachable responder answering "good" does not paper over an unusable CRL
// under enforcement: the revocation may be published only in the CRL.
func TestModeAnyStaleCRLFailsClosedDespiteGoodOCSP(t *testing.T) {
	ca := newTestCA(t)
	leaf := ca.issueLeaf(t, 1503)
	missing := filepath.Join(t.TempDir(), "absent.crl.pem")

	server := newMockOCSPServer(t, ca, map[string]bool{})
	defer server.Close()

	checker := NewChecker(Config{
		Mode:               ModeAny,
		CRLPaths:           []string{missing},
		OCSPResponderURL:   server.URL,
		OCSPTimeoutSeconds: 5,
	}, ca.cert)

	revoked, err := checker.IsRevoked(leaf)
	if revoked {
		t.Fatal("no source reported revoked")
	}
	if !errors.Is(err, ErrCRLStale) {
		t.Fatalf("IsRevoked error = %v, want ErrCRLStale", err)
	}
}

// TestModeAnyStaleCRLWithoutOCSPFailsClosed covers the no-responder branch of
// ModeAny: with only CRL files configured, an unusable CRL fails closed.
func TestModeAnyStaleCRLWithoutOCSPFailsClosed(t *testing.T) {
	ca := newTestCA(t)
	leaf := ca.issueLeaf(t, 1504)
	missing := filepath.Join(t.TempDir(), "absent.crl.pem")

	checker := NewChecker(Config{
		Mode:     ModeAny,
		CRLPaths: []string{missing},
	}, ca.cert)

	revoked, err := checker.IsRevoked(leaf)
	if revoked {
		t.Fatal("no source reported revoked")
	}
	if !errors.Is(err, ErrCRLStale) {
		t.Fatalf("IsRevoked error = %v, want ErrCRLStale", err)
	}
}

// ---------------------------------------------------------------------------
// queryOCSP transport failure paths
// ---------------------------------------------------------------------------

// TestQueryOCSPMalformedResponderURL verifies that a responder URL the HTTP
// client cannot build a request from is treated as status-unavailable (fail
// closed under enforcement) rather than as "not revoked".
func TestQueryOCSPMalformedResponderURL(t *testing.T) {
	ca := newTestCA(t)
	leaf := ca.issueLeaf(t, 1600)

	checker := NewChecker(Config{
		Mode:               ModeOCSP,
		OCSPResponderURL:   "http://ocsp.example\n/bad",
		OCSPTimeoutSeconds: 1,
	}, ca.cert)

	revoked, err := checker.IsRevoked(leaf)
	if revoked {
		t.Fatal("a malformed responder URL must not yield a revoked verdict")
	}
	if !errors.Is(err, ErrOCSPUnavailable) {
		t.Fatalf("IsRevoked error = %v, want ErrOCSPUnavailable", err)
	}
}

// TestQueryOCSPTruncatedResponseBody verifies that a responder which declares
// more body than it sends (a truncated or hostile response) is treated as
// status-unavailable, never as "good".
func TestQueryOCSPTruncatedResponseBody(t *testing.T) {
	ca := newTestCA(t)
	leaf := ca.issueLeaf(t, 1601)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/ocsp-response")
		w.Header().Set("Content-Length", "4096")
		_, _ = w.Write([]byte("trunc"))
	}))
	defer server.Close()

	checker := NewChecker(Config{
		Mode:               ModeOCSP,
		OCSPResponderURL:   server.URL,
		OCSPTimeoutSeconds: 5,
	}, ca.cert)

	revoked, err := checker.IsRevoked(leaf)
	if revoked {
		t.Fatal("a truncated response must not yield a revoked verdict")
	}
	if !errors.Is(err, ErrOCSPUnavailable) {
		t.Fatalf("IsRevoked error = %v, want ErrOCSPUnavailable", err)
	}
}
