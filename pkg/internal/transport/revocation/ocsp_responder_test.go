package revocation

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"errors"
	"math/big"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"golang.org/x/crypto/ocsp"
)

// issueResponderCert issues a leaf from ca with the given extended key usages
// and validity window, for use as an embedded OCSP responder certificate.
func issueResponderCert(t *testing.T, ca *testCA, serial int64, eku []x509.ExtKeyUsage, notBefore, notAfter time.Time) (*x509.Certificate, *ecdsa.PrivateKey) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generate key: %v", err)
	}
	der, err := x509.CreateCertificate(rand.Reader, &x509.Certificate{
		SerialNumber: big.NewInt(serial),
		Subject:      pkix.Name{CommonName: "responder"},
		NotBefore:    notBefore,
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

// TestValidateOCSPResponder pins RFC 6960 §4.2.2.2: a response signed by the
// issuer (no embedded certificate, or the issuer's own) is accepted; any other
// responder must carry id-kp-OCSPSigning and be within its validity period,
// widened on each side by revocationClockSkew.
func TestValidateOCSPResponder(t *testing.T) {
	ca := newTestCA(t)
	now := time.Now().Truncate(time.Second)
	ocspSigning := []x509.ExtKeyUsage{x509.ExtKeyUsageOCSPSigning}
	cert := func(serial int64, eku []x509.ExtKeyUsage, notBefore, notAfter time.Time) *x509.Certificate {
		c, _ := issueResponderCert(t, ca, serial, eku, notBefore, notAfter)
		return c
	}
	far := now.Add(time.Hour)
	past := now.Add(-time.Hour)

	for _, tc := range []struct {
		name    string
		cert    *x509.Certificate
		issuer  *x509.Certificate
		wantErr error
	}{
		{"no embedded certificate", nil, ca.cert, nil},
		{"the issuer's own certificate", ca.cert, ca.cert, nil},
		// With no issuer to compare against, the CA certificate is treated as
		// a delegate and lacks id-kp-OCSPSigning.
		{"the issuer's certificate with a nil issuer", ca.cert, nil, ErrOCSPResponderUnauthorized},
		{"a delegate with ServerAuth only", cert(10, []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}, past, far), ca.cert, ErrOCSPResponderUnauthorized},
		{"a valid OCSP-signing delegate", cert(11, ocspSigning, past, far), ca.cert, nil},
		{"NotBefore one second past the skew", cert(12, ocspSigning, now.Add(revocationClockSkew+time.Second), far.Add(time.Hour)), ca.cert, ErrOCSPResponderUnauthorized},
		{"NotBefore one second inside the skew", cert(13, ocspSigning, now.Add(revocationClockSkew-time.Second), far.Add(time.Hour)), ca.cert, nil},
		{"NotAfter one second past the skew", cert(14, ocspSigning, past.Add(-time.Hour), now.Add(-revocationClockSkew-time.Second)), ca.cert, ErrOCSPResponderUnauthorized},
		{"NotAfter one second inside the skew", cert(15, ocspSigning, past.Add(-time.Hour), now.Add(-revocationClockSkew+time.Second)), ca.cert, nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := validateOCSPResponder(&ocsp.Response{Certificate: tc.cert}, tc.issuer, now)
			if tc.wantErr == nil {
				if err != nil {
					t.Fatalf("validateOCSPResponder = %v, want nil", err)
				}
				return
			}
			if !errors.Is(err, tc.wantErr) {
				t.Fatalf("validateOCSPResponder = %v, want %v", err, tc.wantErr)
			}
		})
	}
}

// TestIsRevoked_UnauthorizedResponderCause checks that a response signed by a
// not-yet-valid OCSP-signing delegate fails closed as ErrOCSPUnavailable and
// keeps ErrOCSPResponderUnauthorized as its cause.
func TestIsRevoked_UnauthorizedResponderCause(t *testing.T) {
	ca := newTestCA(t)
	now := time.Now()
	node, _ := issueResponderCert(t, ca, 601, []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth}, now.Add(-time.Hour), now.Add(time.Hour))
	delegate, delegateKey := issueResponderCert(t, ca, 602, []x509.ExtKeyUsage{x509.ExtKeyUsageOCSPSigning}, now.Add(time.Hour), now.Add(2*time.Hour))

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body := make([]byte, r.ContentLength)
		_, _ = r.Body.Read(body)
		req, err := ocsp.ParseRequest(body)
		if err != nil {
			http.Error(w, "bad request", http.StatusBadRequest)
			return
		}
		resp, err := ocsp.CreateResponse(ca.cert, delegate, ocsp.Response{
			Status:       ocsp.Good,
			SerialNumber: req.SerialNumber,
			ThisUpdate:   time.Now().Add(-time.Minute),
			NextUpdate:   time.Now().Add(time.Hour),
			Certificate:  delegate,
		}, delegateKey)
		if err != nil {
			http.Error(w, "create response", http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/ocsp-response")
		_, _ = w.Write(resp)
	}))
	defer server.Close()

	checker := NewChecker(Config{Mode: ModeOCSP, OCSPResponderURL: server.URL, OCSPTimeoutSeconds: 5}, ca.cert)
	revoked, err := checker.IsRevoked(node)
	if err == nil {
		t.Fatalf("IsRevoked = (%v, nil): a response from a not-yet-valid delegate was accepted", revoked)
	}
	if !errors.Is(err, ErrOCSPUnavailable) {
		t.Fatalf("IsRevoked error = %v, want ErrOCSPUnavailable (fail closed)", err)
	}
	if !errors.Is(err, ErrOCSPResponderUnauthorized) {
		t.Fatalf("IsRevoked error = %v, want cause ErrOCSPResponderUnauthorized", err)
	}
}
