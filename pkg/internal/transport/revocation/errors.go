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

// Package revocation provides certificate revocation checking via CRL files
// and OCSP responders with hot-reload support. CRL files are periodically
// reloaded from disk using content-hash change detection, and OCSP responses
// are cached with configurable TTL. The checker integrates with the TLS
// handshake via VerifyPeerCertificate to reject revoked peer certificates
// at connection time without requiring a restart.
package revocation

import (
	"errors"
	"fmt"
)

// Sentinel errors for revocation checking. These are pre-allocated for
// zero-alloc hot path returns and matching with errors.Is().
var (
	// ErrCertificateRevoked is returned when a peer certificate has been revoked.
	ErrCertificateRevoked = errors.New("revocation: certificate is revoked")

	// ErrOCSPUnavailable is returned when the OCSP responder is unreachable
	// and EnforceRevocation is true.
	ErrOCSPUnavailable = errors.New("revocation: OCSP responder unavailable")

	// ErrInvalidCRL is returned when a CRL file cannot be parsed.
	ErrInvalidCRL = errors.New("revocation: invalid CRL file")
)

// CertificateRevokedError is the typed error for revoked certificates.
// It includes the hex-encoded serial number of the revoked certificate.
type CertificateRevokedError struct {
	Serial string
}

// Error returns a human-readable description of the revocation.
func (e *CertificateRevokedError) Error() string {
	return fmt.Sprintf("revocation: certificate serial %s is revoked", e.Serial)
}

// Is reports whether target matches the sentinel ErrCertificateRevoked.
func (e *CertificateRevokedError) Is(target error) bool {
	return target == ErrCertificateRevoked
}

// OCSPUnavailableError is the typed error for unreachable OCSP responders.
// It includes the responder URL and the underlying transport error.
type OCSPUnavailableError struct {
	URL string
	Err error
}

// Error returns a human-readable description of the OCSP failure.
func (e *OCSPUnavailableError) Error() string {
	return fmt.Sprintf("revocation: OCSP responder %s unavailable: %v", e.URL, e.Err)
}

// Is reports whether target matches the sentinel ErrOCSPUnavailable.
func (e *OCSPUnavailableError) Is(target error) bool {
	return target == ErrOCSPUnavailable
}

// Unwrap returns the underlying error for errors.Is/As chain traversal.
func (e *OCSPUnavailableError) Unwrap() error {
	return e.Err
}

// CRLParseError is the typed error for unparseable CRL files. It includes
// the file path and the underlying parse error.
type CRLParseError struct {
	Path string
	Err  error
}

// Error returns a human-readable description of the CRL parse failure.
func (e *CRLParseError) Error() string {
	return fmt.Sprintf("revocation: failed to parse CRL %s: %v", e.Path, e.Err)
}

// Is reports whether target matches the sentinel ErrInvalidCRL.
func (e *CRLParseError) Is(target error) bool {
	return target == ErrInvalidCRL
}

// Unwrap returns the underlying error for errors.Is/As chain traversal.
func (e *CRLParseError) Unwrap() error {
	return e.Err
}

// ConfigError represents a missing or invalid configuration field.
type ConfigError struct {
	Field   string
	Message string
}

// Error implements the error interface.
func (e *ConfigError) Error() string {
	return "revocation: " + e.Field + ": " + e.Message
}

// ErrNoOCSPResponderURL is returned when no OCSP responder URL is configured.
var ErrNoOCSPResponderURL = &ConfigError{Field: "OCSPResponderURL", Message: "no OCSP responder URL configured"}

// ErrNoIssuerCertificate is returned when no issuer certificate is configured for OCSP.
var ErrNoIssuerCertificate = &ConfigError{Field: "IssuerCertificate", Message: "no issuer certificate configured for OCSP"}
