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
	"errors"
	"io/fs"
	"os"
	"strings"
	"testing"
	"time"
)

// TestTypedErrorMessagesCarryContext verifies that each typed revocation error
// names the artifact it rejected. These messages are the only diagnostic an
// operator gets when a fail-closed handshake rejection happens, so a message
// missing the CRL path or the serial is a real operability defect.
func TestTypedErrorMessagesCarryContext(t *testing.T) {
	stamp := time.Date(2026, 3, 4, 5, 6, 7, 0, time.UTC)
	rfc := stamp.Format(time.RFC3339)

	tests := []struct {
		name string
		err  error
		want []string
	}{
		{
			name: "OCSPStatusUnknownError",
			err:  &OCSPStatusUnknownError{Serial: "beef"},
			want: []string{"unknown", "beef"},
		},
		{
			name: "CRLEmptyError",
			err:  &CRLEmptyError{Path: "/etc/pki/empty.crl"},
			want: []string{"/etc/pki/empty.crl", "no CRL blocks"},
		},
		{
			name: "CRLNotYetValidError",
			err:  &CRLNotYetValidError{Path: "/etc/pki/future.crl", ThisUpdate: stamp},
			want: []string{"/etc/pki/future.crl", "not yet valid", rfc},
		},
		{
			name: "CertificateRevokedError",
			err:  &CertificateRevokedError{Serial: "cafe"},
			want: []string{"cafe", "revoked"},
		},
		{
			name: "OCSPUnavailableError",
			err:  &OCSPUnavailableError{URL: "http://ocsp.example", Err: ErrOCSPNegativeCached},
			want: []string{"http://ocsp.example", "negative-cached"},
		},
		{
			name: "CRLParseError",
			err:  &CRLParseError{Path: "/etc/pki/bad.crl", Err: errors.New("asn1 boom")},
			want: []string{"/etc/pki/bad.crl", "asn1 boom"},
		},
		{
			name: "CRLReadError",
			err:  &CRLReadError{Path: "/etc/pki/gone.crl", Err: fs.ErrNotExist},
			want: []string{"/etc/pki/gone.crl", "file does not exist"},
		},
		{
			name: "CRLSignatureError with cause",
			err:  &CRLSignatureError{Path: "/etc/pki/forged.crl", Err: ErrCRLSignature},
			want: []string{"/etc/pki/forged.crl", "signature verification failed"},
		},
		{
			name: "CRLSignatureError without cause",
			err:  &CRLSignatureError{Path: "/etc/pki/forged.crl"},
			want: []string{"/etc/pki/forged.crl", "signature verification failed"},
		},
		{
			name: "CRLExpiredError",
			err:  &CRLExpiredError{Path: "/etc/pki/old.crl", NextUpdate: stamp},
			want: []string{"/etc/pki/old.crl", "expired", rfc},
		},
		{
			name: "OCSPResponseStaleError",
			err:  &OCSPResponseStaleError{Reason: "NextUpdate in the past", ThisUpdate: stamp, NextUpdate: stamp},
			want: []string{"stale OCSP response", "NextUpdate in the past", rfc},
		},
		{
			name: "ConfigError",
			err:  ErrNoIssuerCertificate,
			want: []string{"IssuerCertificate", "no issuer certificate"},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			msg := tc.err.Error()
			for _, want := range tc.want {
				if !strings.Contains(msg, want) {
					t.Fatalf("Error() = %q, missing %q", msg, want)
				}
			}
		})
	}
}

// TestTypedErrorSentinelMatching verifies the errors.Is contracts the checker
// and its callers rely on to distinguish revoked from status-unknown.
func TestTypedErrorSentinelMatching(t *testing.T) {
	tests := []struct {
		name string
		err  error
		is   []error
		not  []error
	}{
		{
			name: "unknown status",
			err:  &OCSPStatusUnknownError{Serial: "1"},
			is:   []error{ErrOCSPStatusUnknown},
			not:  []error{ErrCertificateRevoked, ErrOCSPUnavailable},
		},
		{
			name: "empty CRL file",
			err:  &CRLEmptyError{Path: "/x"},
			is:   []error{ErrNoCRLInFile, ErrInvalidCRL},
			not:  []error{ErrCRLExpired},
		},
		{
			name: "post-dated CRL",
			err:  &CRLNotYetValidError{Path: "/x"},
			is:   []error{ErrCRLNotYetValid, ErrCRLStale},
			not:  []error{ErrCRLExpired},
		},
		{
			name: "expired CRL",
			err:  &CRLExpiredError{Path: "/x"},
			is:   []error{ErrCRLExpired, ErrCRLStale},
			not:  []error{ErrCRLNotYetValid},
		},
		{
			name: "stale OCSP response",
			err:  &OCSPResponseStaleError{Reason: "r"},
			is:   []error{ErrOCSPResponseStale},
			not:  []error{ErrOCSPStatusUnknown},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			for _, target := range tc.is {
				if !errors.Is(tc.err, target) {
					t.Fatalf("errors.Is(%v, %v) = false, want true", tc.err, target)
				}
			}
			for _, target := range tc.not {
				if errors.Is(tc.err, target) {
					t.Fatalf("errors.Is(%v, %v) = true, want false", tc.err, target)
				}
			}
		})
	}
}

// TestWrappedErrorUnwrapChains verifies that the wrapping errors expose their
// cause. Callers match the underlying condition (a missing CRL file, a missing
// issuer certificate) through these wrappers, so a broken Unwrap would silently
// turn a specific diagnosis into an opaque failure.
func TestWrappedErrorUnwrapChains(t *testing.T) {
	readErr := &CRLReadError{Path: "/etc/pki/gone.crl", Err: os.ErrNotExist}
	if !errors.Is(readErr, os.ErrNotExist) {
		t.Fatal("CRLReadError must unwrap to its I/O cause")
	}

	sigErr := &CRLSignatureError{Path: "/etc/pki/x.crl", Err: ErrNoIssuerCertificate}
	if !errors.Is(sigErr, ErrCRLSignature) {
		t.Fatal("CRLSignatureError must match ErrCRLSignature")
	}
	var cfgErr *ConfigError
	if !errors.As(sigErr, &cfgErr) || cfgErr.Field != "IssuerCertificate" {
		t.Fatalf("CRLSignatureError must unwrap to the missing-issuer ConfigError, got %v", cfgErr)
	}

	parseErr := &CRLParseError{Path: "/etc/pki/x.crl", Err: os.ErrInvalid}
	if !errors.Is(parseErr, os.ErrInvalid) {
		t.Fatal("CRLParseError must unwrap to its parse cause")
	}

	ocspErr := &OCSPUnavailableError{URL: "http://r", Err: ErrOCSPStatusUnknown}
	if !errors.Is(ocspErr, ErrOCSPUnavailable) {
		t.Fatal("OCSPUnavailableError must match ErrOCSPUnavailable")
	}
	if !errors.Is(ocspErr, ErrOCSPStatusUnknown) {
		t.Fatal("OCSPUnavailableError must unwrap to the underlying unknown-status cause")
	}
}
