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
	"encoding/hex"
	"errors"
	"math/big"
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
// default. Config{} selects ModeAny, and the zero value of AllowSoftFail
// ENFORCES; ModeAny names no specific source, so the per-mode source checks
// never fire and, without this rejection, the checker would consult nothing
// and report EVERY certificate as not revoked for the process lifetime, with
// no error, log or metric.
//
// Neither case mentions AllowSoftFail. Enforcement is what the zero value
// means, and spelling it out here would hide a regression that quietly
// restored the old soft-fail default.
func TestValidateRejectsEnforcedRevocationWithoutSource(t *testing.T) {
	for _, mode := range []Mode{"", ModeAny} {
		cfg := Config{Mode: mode}
		cfg.SetDefaults()
		if err := cfg.Validate(); !errors.Is(err, ErrNoRevocationSources) {
			t.Fatalf("Mode %q with no CRL and no responder: Validate() = %v, want ErrNoRevocationSources", mode, err)
		}
	}
}

// TestValidateKeepsNoRevocationOptOutUsable pins that the fix does not break
// the documented opt-out: soft-fail explicitly admits peers whose status
// cannot be determined, which includes having no source at all, and every
// mode that DOES name a source stays valid.
func TestValidateKeepsNoRevocationOptOutUsable(t *testing.T) {
	cases := []Config{
		{AllowSoftFail: true},                // opted into soft-fail, no source
		{Mode: ModeAny, AllowSoftFail: true}, // opted into soft-fail, no source
		{Mode: ModeAny, CRLPaths: []string{"/tmp/x.crl"}},
		{Mode: ModeAny, OCSPResponderURL: "http://ocsp.example"},
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
//
// The Config is left entirely zero-valued, so this is also the assertion that
// the ZERO VALUE ENFORCES, made on observable behaviour: were the default to
// revert to soft-fail, IsRevoked would return (false, nil) here.
func TestIsRevokedFailsClosedWithoutSource(t *testing.T) {
	ca := newTestCA(t)
	leaf := ca.issueLeaf(t, 4242)

	checker := NewChecker(Config{}, ca.cert)
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

	checker := NewChecker(Config{AllowSoftFail: true}, ca.cert)
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

	checker := NewChecker(Config{}, ca.cert)
	checker.AddRevokedSerial(leaf.SerialNumber.Text(16))

	revoked, err := checker.IsRevoked(leaf)
	if err != nil {
		t.Fatalf("a held verdict must win over the missing-source error: %v", err)
	}
	if !revoked {
		t.Fatal("programmatically revoked serial reported as not revoked")
	}
}

// sourcelessModes lists every mode together with the typed error an enforcing
// checker owes when that mode has nothing to consult. It is one table rather
// than one test per mode because the property under test is the CONSISTENCY:
// a mode that quietly stops failing closed is a hole exactly the size of the
// mode, and ModeCRL was such a hole — checkCRLStatus reports staleness only
// when CRLPaths is non-empty, so ModeCRL with no path consulted nothing and
// answered "not revoked" for every certificate.
var sourcelessModes = []struct {
	mode Mode
	want error
}{
	{ModeCRL, ErrNoRevocationSources},
	{ModeOCSP, ErrOCSPUnavailable},
	{ModeBoth, ErrOCSPUnavailable},
	{ModeAny, ErrNoRevocationSources},
	{"", ErrNoRevocationSources}, // the empty string selects ModeAny
}

// TestIsRevokedFailsClosedInEveryModeWithoutSource pins that no mode admits a
// peer it could not check. None of these configs sets AllowSoftFail, so the
// test also fails if the zero value ever means soft-fail again.
func TestIsRevokedFailsClosedInEveryModeWithoutSource(t *testing.T) {
	ca := newTestCA(t)
	leaf := ca.issueLeaf(t, 4246)

	for _, tc := range sourcelessModes {
		checker := NewChecker(Config{Mode: tc.mode}, ca.cert)
		revoked, err := checker.IsRevoked(leaf)
		if revoked {
			t.Fatalf("mode %q: nothing was consulted, so nothing can report a certificate revoked", tc.mode)
		}
		if !errors.Is(err, tc.want) {
			t.Fatalf("mode %q with no source: IsRevoked error = %v, want %v", tc.mode, err, tc.want)
		}
	}
}

// TestIsRevokedSoftFailAdmitsInEveryModeWithoutSource is the positive control
// for the table above: the same sourceless configs admit the peer once
// soft-fail is asked for. Without it, a checker that simply refused every
// certificate would satisfy the fail-closed assertions.
func TestIsRevokedSoftFailAdmitsInEveryModeWithoutSource(t *testing.T) {
	ca := newTestCA(t)
	leaf := ca.issueLeaf(t, 4247)

	for _, tc := range sourcelessModes {
		checker := NewChecker(Config{Mode: tc.mode, AllowSoftFail: true}, ca.cert)
		revoked, err := checker.IsRevoked(leaf)
		if err != nil {
			t.Fatalf("mode %q under soft-fail must admit an unknown-status peer: %v", tc.mode, err)
		}
		if revoked {
			t.Fatalf("mode %q: nothing revoked this certificate", tc.mode)
		}
	}
}

// TestIsRevokedModeCRLWithSourceStillAdmits keeps the ModeCRL backstop from
// degenerating into a blanket rejection: an enforcing checker with a CRL that
// loaded successfully must still admit a certificate the CRL does not list.
func TestIsRevokedModeCRLWithSourceStillAdmits(t *testing.T) {
	ca := newTestCA(t)
	revokedLeaf := ca.issueLeaf(t, 4248)
	cleanLeaf := ca.issueLeaf(t, 4249)

	path := writeCRLFile(t, t.TempDir(), "published.crl.pem", ca.createCRL(t, 4248))
	checker := NewChecker(Config{Mode: ModeCRL, CRLPaths: []string{path}}, ca.cert)
	if err := checker.ReloadCRLs(); err != nil {
		t.Fatalf("ReloadCRLs: %v", err)
	}

	revoked, err := checker.IsRevoked(cleanLeaf)
	if err != nil {
		t.Fatalf("a loaded CRL is a source; an unlisted certificate must be admitted: %v", err)
	}
	if revoked {
		t.Fatal("the CRL does not list this certificate")
	}

	revoked, err = checker.IsRevoked(revokedLeaf)
	if err != nil {
		t.Fatalf("IsRevoked(revokedLeaf): %v", err)
	}
	if !revoked {
		t.Fatal("a certificate listed on the loaded CRL must be reported revoked")
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

// TestOCSPDelegatedResponderMustBeAuthorized: ParseResponseForCert accepts a
// response signed by ANY certificate the issuer signed, embedded as the
// responder. A revoked node holds exactly such a certificate — its own — so on
// the path to the responder it could answer its own OCSP query "good" and pass
// revocation. A delegated responder must carry id-kp-OCSPSigning and be within
// its validity period (RFC 6960 §4.2.2.2); a responder that does still works.
func TestOCSPDelegatedResponderMustBeAuthorized(t *testing.T) {
	ca := newTestCA(t)
	issue := func(serial int64, eku []x509.ExtKeyUsage, notAfter time.Time) (*x509.Certificate, *ecdsa.PrivateKey) {
		key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
		if err != nil {
			t.Fatalf("generate key: %v", err)
		}
		der, err := x509.CreateCertificate(rand.Reader, &x509.Certificate{
			SerialNumber: big.NewInt(serial),
			Subject:      pkix.Name{CommonName: "delegate"},
			NotBefore:    time.Now().Add(-time.Hour),
			NotAfter:     notAfter,
			KeyUsage:     x509.KeyUsageDigitalSignature,
			ExtKeyUsage:  eku,
		}, ca.cert, &key.PublicKey, ca.key)
		if err != nil {
			t.Fatalf("issue: %v", err)
		}
		cert, err := x509.ParseCertificate(der)
		if err != nil {
			t.Fatalf("parse: %v", err)
		}
		return cert, key
	}
	// serve answers every query "good", signed by the given delegate.
	serve := func(responder *x509.Certificate, key *ecdsa.PrivateKey) *httptest.Server {
		return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			body := make([]byte, r.ContentLength)
			_, _ = r.Body.Read(body)
			req, err := ocsp.ParseRequest(body)
			if err != nil {
				http.Error(w, "bad request", http.StatusBadRequest)
				return
			}
			resp, err := ocsp.CreateResponse(ca.cert, responder, ocsp.Response{
				Status:       ocsp.Good,
				SerialNumber: req.SerialNumber,
				ThisUpdate:   time.Now().Add(-time.Minute),
				NextUpdate:   time.Now().Add(time.Hour),
				Certificate:  responder,
			}, key)
			if err != nil {
				http.Error(w, "create response", http.StatusInternalServerError)
				return
			}
			w.Header().Set("Content-Type", "application/ocsp-response")
			_, _ = w.Write(resp)
		}))
	}
	node, nodeKey := issue(501, []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth}, time.Now().Add(time.Hour))
	delegate, delegateKey := issue(502, []x509.ExtKeyUsage{x509.ExtKeyUsageOCSPSigning}, time.Now().Add(time.Hour))
	expired, expiredKey := issue(503, []x509.ExtKeyUsage{x509.ExtKeyUsageOCSPSigning}, time.Now().Add(-10*time.Minute))

	for _, tc := range []struct {
		name      string
		responder *x509.Certificate
		key       *ecdsa.PrivateKey
		wantOK    bool
	}{
		{"a node vouching for itself", node, nodeKey, false},
		{"an expired OCSP-signing delegate", expired, expiredKey, false},
		{"an authorized OCSP-signing delegate", delegate, delegateKey, true},
		// The CA answering OCSP itself commonly embeds its own certificate;
		// that is the issuer signing, not a delegate, and must stay accepted.
		{"the issuer itself, embedding its certificate", ca.cert, ca.key, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			server := serve(tc.responder, tc.key)
			defer server.Close()
			checker := NewChecker(Config{Mode: ModeOCSP, OCSPResponderURL: server.URL, OCSPTimeoutSeconds: 5}, ca.cert)

			revoked, err := checker.IsRevoked(node)
			if tc.wantOK {
				if err != nil || revoked {
					t.Fatalf("IsRevoked = (%v, %v), want a good verdict from an authorized delegate", revoked, err)
				}
				return
			}
			if err == nil {
				t.Fatalf("IsRevoked = (%v, nil): a response from an unauthorized responder was accepted", revoked)
			}
			if !errors.Is(err, ErrOCSPUnavailable) {
				t.Fatalf("IsRevoked error = %v, want it treated as status unavailable (fail closed)", err)
			}
		})
	}
}
