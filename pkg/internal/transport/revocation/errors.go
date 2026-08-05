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
	"time"
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

	// ErrOCSPNegativeCached is the underlying cause wrapped in an
	// OCSPUnavailableError when a recent responder failure is served from the
	// short-TTL negative cache instead of being re-queried. It distinguishes a
	// cached failure from a fresh transport error for diagnostics.
	ErrOCSPNegativeCached = errors.New("revocation: OCSP responder recently unavailable (negative-cached)")

	// ErrCRLStale is returned (under EnforceRevocation) when a
	// certificate's revocation status cannot be established because no
	// fresh CRL is loaded: either the initial load has not succeeded yet,
	// or the most recent reload failed (unreadable, unparseable, forged,
	// or expired file). Treating a stale CRL as "nothing revoked" would
	// silently un-revoke every certificate, so enforcement mode fails
	// closed instead.
	ErrCRLStale = errors.New("revocation: no fresh CRL loaded; revocation status unknown")

	// ErrCRLSignature is returned when a CRL's signature does not verify
	// against the issuing CA certificate (or no issuer certificate is
	// available to verify with). A forged or foreign CRL must never
	// replace the revoked set: it could silently un-revoke certificates.
	ErrCRLSignature = errors.New("revocation: CRL signature verification failed")

	// ErrCRLExpired is returned when a CRL is past its NextUpdate time.
	// A frozen CRL file would otherwise serve an ever-staler view of
	// revocations indefinitely; expiry marks the set stale so
	// EnforceRevocation fails closed until a fresh CRL is published.
	ErrCRLExpired = errors.New("revocation: CRL past its NextUpdate; revocation status unknown")

	// ErrOCSPResponseStale is returned when an OCSP response fails
	// freshness validation (ThisUpdate/NextUpdate window, with clock-skew
	// allowance). A replayed stale "good" response must be treated as
	// status-unavailable, never as "good".
	ErrOCSPResponseStale = errors.New("revocation: OCSP response outside freshness window")

	// ErrOCSPStatusUnknown is returned when a responder reports RFC 6960
	// "unknown" for a certificate. Unknown is NOT "not revoked": the
	// responder has no record of the certificate, so its status cannot be
	// established. It must route through the status-unavailable path so
	// EnforceRevocation fails closed and the verdict is never cached as good.
	ErrOCSPStatusUnknown = errors.New("revocation: OCSP responder reported status unknown")

	// ErrNoCRLInFile is returned when a configured CRL file contains no
	// parseable "X509 CRL" PEM block. Committing an empty revoked set from
	// such a file would silently un-revoke every listed certificate.
	ErrNoCRLInFile = errors.New("revocation: CRL file contains no CRL blocks")

	// ErrCRLNotYetValid is returned when a CRL's ThisUpdate lies in the
	// future beyond the clock-skew allowance. A post-dated CRL proves
	// nothing about the present, so the set is marked stale.
	ErrCRLNotYetValid = errors.New("revocation: CRL ThisUpdate in the future; revocation status unknown")
)

// OCSPStatusUnknownError is the typed error for an RFC 6960 "unknown"
// certificate status. It carries the hex-encoded serial the responder had
// no record of.
type OCSPStatusUnknownError struct {
	Serial string
}

// Error returns a human-readable description of the unknown status.
func (e *OCSPStatusUnknownError) Error() string {
	return fmt.Sprintf("revocation: OCSP responder reported status unknown for serial %s", e.Serial)
}

// Is reports whether target matches the sentinel ErrOCSPStatusUnknown.
func (e *OCSPStatusUnknownError) Is(target error) bool {
	return target == ErrOCSPStatusUnknown
}

// CRLEmptyError is the typed error for a configured CRL file that parsed
// without yielding a single CRL block.
type CRLEmptyError struct {
	Path string
}

// Error returns a human-readable description of the empty CRL file.
func (e *CRLEmptyError) Error() string {
	return fmt.Sprintf("revocation: CRL %s contains no CRL blocks", e.Path)
}

// Is reports whether target matches the sentinels ErrNoCRLInFile and
// ErrInvalidCRL (an empty file is an unusable CRL file).
func (e *CRLEmptyError) Is(target error) bool {
	return target == ErrNoCRLInFile || target == ErrInvalidCRL
}

// CRLNotYetValidError is the typed error for a CRL whose ThisUpdate is in
// the future beyond the clock-skew allowance.
type CRLNotYetValidError struct {
	Path       string
	ThisUpdate time.Time
}

// Error returns a human-readable description of the post-dated CRL.
func (e *CRLNotYetValidError) Error() string {
	return fmt.Sprintf("revocation: CRL %s not yet valid (ThisUpdate %s)",
		e.Path, e.ThisUpdate.Format(time.RFC3339))
}

// Is reports whether target matches the sentinels ErrCRLNotYetValid and
// ErrCRLStale (a post-dated CRL is a stale CRL for enforcement purposes).
func (e *CRLNotYetValidError) Is(target error) bool {
	return target == ErrCRLNotYetValid || target == ErrCRLStale
}

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

// CRLReadError is the typed error for CRL files that exist in the
// configuration but could not be read from disk. It includes the file
// path and the underlying I/O error.
type CRLReadError struct {
	Path string
	Err  error
}

// Error returns a human-readable description of the CRL read failure.
func (e *CRLReadError) Error() string {
	return fmt.Sprintf("revocation: failed to read CRL %s: %v", e.Path, e.Err)
}

// Unwrap returns the underlying error for errors.Is/As chain traversal.
func (e *CRLReadError) Unwrap() error {
	return e.Err
}

// CRLSignatureError is the typed error for CRLs whose signature could not
// be verified against the issuing CA. It includes the file path and the
// underlying verification error (nil when no issuer certificate was
// available to verify with).
type CRLSignatureError struct {
	Path string
	Err  error
}

// Error returns a human-readable description of the signature failure.
func (e *CRLSignatureError) Error() string {
	if e.Err != nil {
		return fmt.Sprintf("revocation: CRL %s signature verification failed: %v", e.Path, e.Err)
	}
	return fmt.Sprintf("revocation: CRL %s signature verification failed", e.Path)
}

// Is reports whether target matches the sentinel ErrCRLSignature.
func (e *CRLSignatureError) Is(target error) bool {
	return target == ErrCRLSignature
}

// Unwrap returns the underlying error for errors.Is/As chain traversal.
func (e *CRLSignatureError) Unwrap() error {
	return e.Err
}

// CRLExpiredError is the typed error for CRLs past their NextUpdate time.
// It includes the file path and the expired NextUpdate timestamp.
type CRLExpiredError struct {
	Path       string
	NextUpdate time.Time
}

// Error returns a human-readable description of the expiry.
func (e *CRLExpiredError) Error() string {
	return fmt.Sprintf("revocation: CRL %s expired (NextUpdate %s)",
		e.Path, e.NextUpdate.Format(time.RFC3339))
}

// Is reports whether target matches the sentinels ErrCRLExpired and
// ErrCRLStale (an expired CRL is a stale CRL for enforcement purposes).
func (e *CRLExpiredError) Is(target error) bool {
	return target == ErrCRLExpired || target == ErrCRLStale
}

// OCSPResponseStaleError is the typed error for OCSP responses that fail
// freshness validation. It carries the response's validity window and the
// reason the window was rejected.
type OCSPResponseStaleError struct {
	Reason     string
	ThisUpdate time.Time
	NextUpdate time.Time
}

// Error returns a human-readable description of the freshness failure.
func (e *OCSPResponseStaleError) Error() string {
	return fmt.Sprintf("revocation: stale OCSP response (%s): ThisUpdate %s NextUpdate %s",
		e.Reason, e.ThisUpdate.Format(time.RFC3339), e.NextUpdate.Format(time.RFC3339))
}

// Is reports whether target matches the sentinel ErrOCSPResponseStale.
func (e *OCSPResponseStaleError) Is(target error) bool {
	return target == ErrOCSPResponseStale
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

// ErrNoCRLSources is returned by Config.Validate when the selected mode
// consults CRLs but no CRL file is configured. Under EnforceRevocation such
// a mode fails every handshake; under soft-fail it is a silent no-op.
var ErrNoCRLSources = &ConfigError{Field: "CRLPaths", Message: "selected mode requires at least one CRL file"}

// ErrInvalidRevocationMode is returned by Config.Validate for an
// unrecognized Mode string, which would otherwise silently select the
// default mode.
var ErrInvalidRevocationMode = &ConfigError{Field: "Mode", Message: "unrecognized revocation mode"}

// ErrNoRevocationSources is returned when revocation is enforced (fail
// closed) under the source-agnostic default mode but neither a CRL file nor
// an OCSP responder is configured. That combination consults nothing and
// returns "not revoked" for every certificate — no error, no log, no metric —
// which silently disables the only mechanism that evicts an unexpired
// compromised node certificate. A deployment that genuinely wants no
// revocation checking leaves RevocationConfig unset or opts into soft-fail.
var ErrNoRevocationSources = &ConfigError{
	Field:   "CRLPaths/OCSPResponderURL",
	Message: "enforced revocation requires at least one CRL file or an OCSP responder",
}
