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
	"crypto/x509"
	"encoding/hex"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"golang.org/x/crypto/ocsp"
)

// ---------------------------------------------------------------------------
// Enabled + enforcing + no source (fail-closed default with nothing wired)
// ---------------------------------------------------------------------------

// TestValidateRejectsEnforcedRevocationWithoutSource pins the fail-closed
// default. RevocationConfig{} selects ModeAny and enforcement, and ModeAny
// names no specific source, so the per-mode source checks never fire: the
// checker consults nothing and reports EVERY certificate as not revoked for
// the process lifetime, with no error, log or metric.
func TestValidateRejectsEnforcedRevocationWithoutSource(t *testing.T) {
	for _, mode := range []Mode{"", ModeAny} {
		cfg := Config{Mode: mode, EnforceRevocation: true}
		cfg.SetDefaults()
		if err := cfg.Validate(); !errors.Is(err, ErrNoRevocationSources) {
			t.Fatalf("Mode %q enforcing with no CRL and no responder: Validate() = %v, want ErrNoRevocationSources", mode, err)
		}
	}
}

// TestValidateKeepsNoRevocationOptOutUsable pins that the fix does not break
// the documented opt-out: soft-fail explicitly admits peers whose status
// cannot be determined, which includes having no source at all, and every
// mode that DOES name a source stays valid.
func TestValidateKeepsNoRevocationOptOutUsable(t *testing.T) {
	cases := []Config{
		{},              // soft-fail, no source
		{Mode: ModeAny}, // soft-fail, no source
		{Mode: ModeAny, EnforceRevocation: true, CRLPaths: []string{"/tmp/x.crl"}},
		{Mode: ModeAny, EnforceRevocation: true, OCSPResponderURL: "http://ocsp.example"},
	}
	for i, cfg := range cases {
		cfg.SetDefaults()
		if err := cfg.Validate(); err != nil {
			t.Fatalf("case %d: Validate() = %v, want nil", i, err)
		}
	}
}

// TestIsRevokedFailsClosedWithoutSource is the runtime backstop for a checker
// built without Validate (or whose CRLPaths were dropped in a refactor): the
// enforcing ModeAny path must surface the missing source instead of returning
// "not revoked" for every certificate.
func TestIsRevokedFailsClosedWithoutSource(t *testing.T) {
	ca := newTestCA(t)
	leaf := ca.issueLeaf(t, 4242)

	checker := NewChecker(Config{EnforceRevocation: true}, ca.cert)
	revoked, err := checker.IsRevoked(leaf)
	if revoked {
		t.Fatal("no source configured: nothing can report a certificate revoked")
	}
	if !errors.Is(err, ErrNoRevocationSources) {
		t.Fatalf("IsRevoked error = %v, want ErrNoRevocationSources", err)
	}
}

// TestIsRevokedSoftFailWithoutSourceAdmits pins the other half of the opt-out:
// under soft-fail the same checker admits the peer with no error.
func TestIsRevokedSoftFailWithoutSourceAdmits(t *testing.T) {
	ca := newTestCA(t)
	leaf := ca.issueLeaf(t, 4243)

	checker := NewChecker(Config{EnforceRevocation: false}, ca.cert)
	revoked, err := checker.IsRevoked(leaf)
	if err != nil {
		t.Fatalf("soft-fail with no source must admit: %v", err)
	}
	if revoked {
		t.Fatal("nothing revoked this certificate")
	}
}

// TestIsRevokedWithoutSourceStillHonoursProgrammaticRevocation pins that the
// new fail-closed error does not shadow a verdict the checker actually holds:
// an operator revocation added through the admin path still reports revoked.
func TestIsRevokedWithoutSourceStillHonoursProgrammaticRevocation(t *testing.T) {
	ca := newTestCA(t)
	leaf := ca.issueLeaf(t, 4244)

	checker := NewChecker(Config{EnforceRevocation: true}, ca.cert)
	checker.AddRevokedSerial(leaf.SerialNumber.Text(16))

	revoked, err := checker.IsRevoked(leaf)
	if err != nil {
		t.Fatalf("a held verdict must win over the missing-source error: %v", err)
	}
	if !revoked {
		t.Fatal("programmatically revoked serial reported as not revoked")
	}
}

// ---------------------------------------------------------------------------
// Multi-issuer serial collision (revokedSet and ocspCache scoping)
// ---------------------------------------------------------------------------

// TestCRLRevocationScopedToIssuingCA is the multi-issuer collision regression
// for the CRL path. X.509 serials are unique only per ISSUER, so two
// intermediates in the same bundle can both issue serial 0x2a. A CRL published
// by one of them must not revoke the other's certificate.
func TestCRLRevocationScopedToIssuingCA(t *testing.T) {
	root := newTestCA(t)
	interA := newTestIntermediate(t, root)
	interB := newTestIntermediate(t, root)

	const collidingSerial = 42
	leafA := interA.issueLeaf(t, collidingSerial)
	leafB := interB.issueLeaf(t, collidingSerial)

	dir := t.TempDir()
	path := writeCRLFile(t, dir, "a.crl.pem", interA.createCRL(t, collidingSerial))

	checker := NewChecker(Config{Mode: ModeCRL, CRLPaths: []string{path}},
		root.cert, interA.cert, interB.cert)
	if err := checker.ReloadCRLs(); err != nil {
		t.Fatalf("ReloadCRLs: %v", err)
	}

	revokedA, err := checker.IsRevoked(leafA)
	if err != nil {
		t.Fatalf("IsRevoked(leafA): %v", err)
	}
	if !revokedA {
		t.Fatal("leaf issued by interA and listed on interA's CRL must be revoked")
	}

	revokedB, err := checker.IsRevoked(leafB)
	if err != nil {
		t.Fatalf("IsRevoked(leafB): %v", err)
	}
	if revokedB {
		t.Fatalf("interA's CRL falsely revoked interB's certificate with the same serial %#x", collidingSerial)
	}
}

// issuerKeyHashOf returns the hex-encoded issuer key hash an OCSP request for
// leaf under issuer carries, so a mock responder can route by issuer instead
// of by serial.
func issuerKeyHashOf(t *testing.T, leaf *x509.Certificate, issuer *x509.Certificate) string {
	t.Helper()
	der, err := ocsp.CreateRequest(leaf, issuer, nil)
	if err != nil {
		t.Fatalf("create OCSP request: %v", err)
	}
	req, err := ocsp.ParseRequest(der)
	if err != nil {
		t.Fatalf("parse OCSP request: %v", err)
	}
	return hex.EncodeToString(req.IssuerKeyHash)
}

// ocspIssuerRoute describes one intermediate's OCSP behaviour for the shared
// responder: which key hash identifies it, which CA signs its responses, and
// what verdict it reports.
type ocspIssuerRoute struct {
	keyHash string
	ca      *testCA
	status  int
}

// newRoutingOCSPServer answers per ISSUER rather than per serial, which is
// what a real responder does: the same serial issued by two intermediates has
// two independent statuses.
func newRoutingOCSPServer(t *testing.T, routes []ocspIssuerRoute) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
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
		hash := hex.EncodeToString(req.IssuerKeyHash)
		for _, route := range routes {
			if route.keyHash != hash {
				continue
			}
			tmpl := ocsp.Response{
				Status:       route.status,
				SerialNumber: req.SerialNumber,
				ThisUpdate:   time.Now().Add(-1 * time.Minute),
				NextUpdate:   time.Now().Add(1 * time.Hour),
			}
			if route.status == ocsp.Revoked {
				tmpl.RevokedAt = time.Now().Add(-1 * time.Minute)
				tmpl.RevocationReason = ocsp.KeyCompromise
			}
			respBytes, respErr := ocsp.CreateResponse(route.ca.cert, route.ca.cert, tmpl, route.ca.key)
			if respErr != nil {
				http.Error(w, "create response error", http.StatusInternalServerError)
				return
			}
			w.Header().Set("Content-Type", "application/ocsp-response")
			_, _ = w.Write(respBytes)
			return
		}
		http.Error(w, "unknown issuer", http.StatusNotFound)
	}))
}

// TestOCSPCacheScopedToIssuingCA is the multi-issuer collision regression for
// the OCSP cache. A "good" verdict cached for interA's serial 0x2a must not
// satisfy a lookup for interB's serial 0x2a, whose responder reports it
// REVOKED — the cache would otherwise admit a compromised certificate for the
// full cache TTL.
func TestOCSPCacheScopedToIssuingCA(t *testing.T) {
	root := newTestCA(t)
	interA := newTestIntermediate(t, root)
	interB := newTestIntermediate(t, root)

	const collidingSerial = 42
	leafA := interA.issueLeaf(t, collidingSerial)
	leafB := interB.issueLeaf(t, collidingSerial)

	server := newRoutingOCSPServer(t, []ocspIssuerRoute{
		{keyHash: issuerKeyHashOf(t, leafA, interA.cert), ca: interA, status: ocsp.Good},
		{keyHash: issuerKeyHashOf(t, leafB, interB.cert), ca: interB, status: ocsp.Revoked},
	})
	defer server.Close()

	checker := NewChecker(Config{
		Mode:               ModeOCSP,
		OCSPResponderURL:   server.URL,
		OCSPCacheSeconds:   3600,
		OCSPTimeoutSeconds: 5,
		EnforceRevocation:  true,
	}, root.cert, interA.cert, interB.cert)

	// Populate the cache with interA's "good" verdict first.
	revokedA, err := checker.IsRevoked(leafA)
	if err != nil {
		t.Fatalf("IsRevoked(leafA): %v", err)
	}
	if revokedA {
		t.Fatal("interA's responder reported good")
	}

	revokedB, err := checker.IsRevoked(leafB)
	if err != nil {
		t.Fatalf("IsRevoked(leafB): %v", err)
	}
	if !revokedB {
		t.Fatalf("cached verdict for interA's serial %#x satisfied the lookup for interB's revoked certificate",
			collidingSerial)
	}
}
