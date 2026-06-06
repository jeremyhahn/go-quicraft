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
	"bytes"
	"context"
	"crypto/sha256"
	"crypto/x509"
	"encoding/pem"
	"io"
	"log/slog"
	"net/http"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/crypto/ocsp"
)

// Mode determines which revocation checking methods to use.
type Mode string

const (
	// ModeCRL checks only CRL files.
	ModeCRL Mode = "crl"

	// ModeOCSP checks only OCSP responders.
	ModeOCSP Mode = "ocsp"

	// ModeBoth requires both CRL and OCSP to confirm revocation.
	ModeBoth Mode = "both"

	// ModeAny passes if either CRL or OCSP confirms revocation (default).
	ModeAny Mode = "any"
)

// Default configuration values.
const (
	defaultOCSPCacheSeconds   = 300
	defaultOCSPTimeoutSeconds = 5
	defaultCheckInterval      = 60 * time.Second
	defaultMode               = ModeAny

	// maxOCSPResponseSize limits the OCSP response body to prevent
	// memory exhaustion from a malicious or misconfigured responder.
	maxOCSPResponseSize = 1 << 20 // 1 MB

	// ocspUnavailableTTL is the negative-cache TTL for "responder
	// unavailable" results. When an OCSP query fails (responder down,
	// timeout, network error), the failure is cached for this short window
	// so a flood of reconnect handshakes does not re-query a dead responder
	// on every connection, adding the full OCSP timeout to each handshake.
	// It is intentionally short so the responder is retried promptly once it
	// recovers, and far shorter than the positive-result cache TTL so a
	// genuine revocation that the responder reports after recovery is picked
	// up quickly. This cache only suppresses re-queries; it never turns an
	// unavailable result into "revoked" or "good".
	//
	// Soft-fail tradeoff: under EnforceRevocation=false, a cached unavailable
	// entry causes the peer to be accepted (status unknown). After the
	// responder recovers and would report a cert as revoked, that revocation
	// can go undetected for at most ocspUnavailableTTL (until the negative
	// entry expires and the next handshake re-queries). This bounded delay is
	// inherent to soft-fail: with no reachable responder there is by
	// definition no fresh verdict to act on. It is NOT a masking of a verdict
	// already known to this checker — a definitive cached "revoked" (or
	// "good") is never overwritten by a later unavailable result (see
	// checkOCSPRaw). Under EnforceRevocation=true the cached unavailable entry
	// fails closed regardless.
	ocspUnavailableTTL = 10 * time.Second
)

// Config holds revocation checking configuration.
type Config struct {
	// CRLPaths are file paths to PEM-encoded CRL files. Watched for
	// content changes on each CheckInterval tick.
	CRLPaths []string

	// OCSPResponderURL is the URL of an OCSP responder. When empty,
	// OCSP checking is disabled.
	OCSPResponderURL string

	// OCSPCacheSeconds controls how long to cache OCSP responses.
	// Default: 300 (5 minutes).
	OCSPCacheSeconds int

	// OCSPTimeoutSeconds is the timeout for OCSP HTTP requests.
	// Default: 5.
	OCSPTimeoutSeconds int

	// CheckInterval controls how often CRL files are reloaded from disk.
	// Default: 60s.
	CheckInterval time.Duration

	// Mode determines the revocation checking strategy. Default: "any".
	Mode Mode

	// EnforceRevocation rejects peers when revocation status is unknown
	// (OCSP unreachable, no CRL). Default: false (soft-fail allows
	// connections when revocation status cannot be determined).
	EnforceRevocation bool

	// OnCertRevoked is called when a certificate is found to be revoked
	// during a TLS handshake. The serial number is passed as a lowercase
	// hex string. May be nil.
	OnCertRevoked func(serial string)
}

// SetDefaults applies default values to zero-valued fields.
func (c *Config) SetDefaults() {
	if c.OCSPCacheSeconds == 0 {
		c.OCSPCacheSeconds = defaultOCSPCacheSeconds
	}
	if c.OCSPTimeoutSeconds == 0 {
		c.OCSPTimeoutSeconds = defaultOCSPTimeoutSeconds
	}
	if c.CheckInterval == 0 {
		c.CheckInterval = defaultCheckInterval
	}
	if c.Mode == "" {
		c.Mode = defaultMode
	}
}

// ocspCacheEntry holds a cached OCSP result with expiry. It represents either
// a definitive status (revoked true/false) or, when unavailable is true, a
// short-lived negative cache of a responder failure so repeated handshakes do
// not re-query a down responder. The two cases use different TTLs: definitive
// results use OCSPCacheSeconds; unavailable results use the much shorter
// ocspUnavailableTTL.
type ocspCacheEntry struct {
	revoked     bool
	unavailable bool
	expiresAt   time.Time
}

// Checker performs certificate revocation checking using CRL files and/or
// OCSP responders. CRL files are periodically reloaded from disk using
// content-hash change detection for hot-reload support. OCSP responses
// are cached with configurable TTL to avoid per-handshake HTTP calls.
//
// Checker is safe for concurrent use by multiple goroutines. The TLS
// handshake path (IsRevoked) uses read locks and atomic cache lookups
// to minimize contention with the background CRL reload goroutine.
type Checker struct {
	// mu protects revokedSet and crlHashes.
	mu         sync.RWMutex
	revokedSet map[string]struct{} // serial hex -> revoked
	crlHashes  map[string][32]byte // path -> SHA-256 content hash

	// ocspMu protects ocspCache.
	ocspMu    sync.RWMutex
	ocspCache map[string]*ocspCacheEntry // serial hex -> cached result

	issuerCert     *x509.Certificate
	cfg            Config
	httpClient     *http.Client
	stopped        atomic.Bool
	stopC          chan struct{}
	shutdownCtx    context.Context
	shutdownCancel context.CancelFunc
}

// NewChecker creates a new revocation checker. The issuerCert is the CA
// certificate used to verify OCSP responses. It may be nil if only CRL
// checking is needed.
func NewChecker(cfg Config, issuerCert *x509.Certificate) *Checker {
	cfg.SetDefaults()
	ctx, cancel := context.WithCancel(context.Background())
	return &Checker{
		revokedSet: make(map[string]struct{}),
		crlHashes:  make(map[string][32]byte),
		ocspCache:  make(map[string]*ocspCacheEntry),
		issuerCert: issuerCert,
		cfg:        cfg,
		httpClient: &http.Client{
			Timeout: time.Duration(cfg.OCSPTimeoutSeconds) * time.Second,
		},
		stopC:          make(chan struct{}),
		shutdownCtx:    ctx,
		shutdownCancel: cancel,
	}
}

// Start performs the initial CRL load and begins the periodic CRL reload
// goroutine. Safe to call once; subsequent calls are no-ops.
func (c *Checker) Start() {
	if err := c.ReloadCRLs(); err != nil {
		slog.Warn("initial CRL reload failed", "error", err)
	}
	go c.reloadLoop()
}

// Stop terminates the CRL reload goroutine and cancels any in-flight OCSP
// requests. Safe to call multiple times.
func (c *Checker) Stop() {
	if c.stopped.CompareAndSwap(false, true) {
		close(c.stopC)
		if c.shutdownCancel != nil {
			c.shutdownCancel()
		}
	}
}

// reloadLoop periodically reloads CRL files from disk. Exits when stopC
// is closed.
func (c *Checker) reloadLoop() {
	ticker := time.NewTicker(c.cfg.CheckInterval)
	defer ticker.Stop()
	for {
		select {
		case <-c.stopC:
			return
		case <-ticker.C:
			if err := c.ReloadCRLs(); err != nil {
				slog.Warn("CRL reload failed", "error", err)
			}
		}
	}
}

// ReloadCRLs reads all configured CRL files, parses them, and rebuilds
// the revoked serial set. Only re-parses files whose SHA-256 content hash
// has changed since the last reload. This method is exported to support
// manual refresh (e.g., from an admin API or signal handler).
func (c *Checker) ReloadCRLs() error {
	if len(c.cfg.CRLPaths) == 0 {
		return nil
	}

	newSet := make(map[string]struct{})
	newHashes := make(map[string][32]byte)

	for _, path := range c.cfg.CRLPaths {
		data, err := os.ReadFile(path)
		if err != nil {
			slog.Warn("CRL file read failed", "path", path, "error", err)
			continue
		}

		hash := sha256.Sum256(data)
		newHashes[path] = hash

		// Parse PEM-encoded CRL and extract revoked serial numbers.
		serials, parseErr := parseCRLSerials(data)
		if parseErr != nil {
			return &CRLParseError{Path: path, Err: parseErr}
		}
		for _, serial := range serials {
			newSet[serial] = struct{}{}
		}

		// Log only when content changed.
		c.mu.RLock()
		oldHash, existed := c.crlHashes[path]
		c.mu.RUnlock()
		if !existed || oldHash != hash {
			slog.Info("CRL reloaded", "path", path, "revoked_serials", len(serials))
		}
	}

	c.mu.Lock()
	c.revokedSet = newSet
	c.crlHashes = newHashes
	c.mu.Unlock()

	return nil
}

// parseCRLSerials extracts revoked serial number hex strings from PEM-
// encoded CRL data. Supports multiple CRL blocks in a single file.
func parseCRLSerials(data []byte) ([]string, error) {
	var serials []string
	rest := data
	for {
		var block *pem.Block
		block, rest = pem.Decode(rest)
		if block == nil {
			break
		}
		if block.Type != "X509 CRL" {
			continue
		}
		crl, err := x509.ParseRevocationList(block.Bytes)
		if err != nil {
			return nil, err
		}
		for _, entry := range crl.RevokedCertificateEntries {
			serials = append(serials, entry.SerialNumber.Text(16))
		}
	}
	return serials, nil
}

// AddRevokedSerial programmatically adds a serial number to the revoked
// set. The serial should be a lowercase hex string. This is useful for
// runtime revocation via admin API without modifying CRL files on disk.
func (c *Checker) AddRevokedSerial(serial string) {
	c.mu.Lock()
	c.revokedSet[serial] = struct{}{}
	c.mu.Unlock()
}

// RemoveRevokedSerial programmatically removes a serial number from the
// revoked set. This is useful for undoing a programmatic revocation.
// Note: the serial will be re-added on the next CRL reload if it still
// appears in a CRL file.
func (c *Checker) RemoveRevokedSerial(serial string) {
	c.mu.Lock()
	delete(c.revokedSet, serial)
	c.mu.Unlock()
}

// RevokedCount returns the number of serials currently in the revoked set.
func (c *Checker) RevokedCount() int {
	c.mu.RLock()
	n := len(c.revokedSet)
	c.mu.RUnlock()
	return n
}

// IsRevoked checks whether the given certificate has been revoked.
// Returns (true, nil) if revoked, (false, nil) if not revoked or status
// unknown in soft-fail mode, or (false, error) if the check failed and
// EnforceRevocation is true.
func (c *Checker) IsRevoked(cert *x509.Certificate) (bool, error) {
	serial := cert.SerialNumber.Text(16)

	switch c.cfg.Mode {
	case ModeCRL:
		return c.checkCRL(serial), nil

	case ModeOCSP:
		return c.checkOCSP(cert)

	case ModeBoth:
		crlRevoked := c.checkCRL(serial)
		// Use checkOCSPRaw to distinguish "OCSP says good" from
		// "OCSP unavailable". When OCSP is unavailable, fall back
		// to the CRL result (soft-fail) or reject (enforcement).
		ocspRevoked, err := c.checkOCSPRaw(cert)
		if err != nil {
			if c.cfg.EnforceRevocation {
				return false, err
			}
			// Soft-fail: OCSP unavailable, use CRL result only.
			return crlRevoked, nil
		}
		return crlRevoked && ocspRevoked, nil

	default: // ModeAny
		if c.checkCRL(serial) {
			return true, nil
		}
		if c.cfg.OCSPResponderURL != "" {
			return c.checkOCSP(cert)
		}
		return false, nil
	}
}

// checkCRL tests whether the serial is in the revoked set.
func (c *Checker) checkCRL(serial string) bool {
	c.mu.RLock()
	_, revoked := c.revokedSet[serial]
	c.mu.RUnlock()
	return revoked
}

// checkOCSP queries the OCSP responder for the certificate's revocation
// status, using the cache when available. When the OCSP check fails
// (unreachable, no URL, no issuer cert), behavior depends on
// EnforceRevocation: if true, returns the error (fail closed, including on a
// negative-cache hit); if false (soft-fail), returns (false, nil).
//
// In soft-fail mode an unavailable result accepts the peer, so a revocation
// the responder would report only after it recovers can go undetected for at
// most ocspUnavailableTTL. This bounded latency is inherent to soft-fail and
// is distinct from masking a known verdict: a definitive cached "revoked" is
// never overwritten by a later unavailable result, so checkOCSPRaw still
// returns the revoked verdict from cache even while the responder is down.
func (c *Checker) checkOCSP(cert *x509.Certificate) (bool, error) {
	revoked, err := c.checkOCSPRaw(cert)
	if err != nil {
		if c.cfg.EnforceRevocation {
			return false, err
		}
		return false, nil
	}
	return revoked, nil
}

// checkOCSPRaw queries the OCSP responder without applying enforcement
// policy. Returns the raw error on failure so callers (e.g., ModeBoth)
// can distinguish "OCSP says good" from "OCSP is unavailable".
func (c *Checker) checkOCSPRaw(cert *x509.Certificate) (bool, error) {
	if c.cfg.OCSPResponderURL == "" {
		return false, &OCSPUnavailableError{
			URL: "(none configured)",
			Err: ErrNoOCSPResponderURL,
		}
	}

	serial := cert.SerialNumber.Text(16)

	// Check cache first (read lock for concurrent handshakes). A cached
	// unavailable entry short-circuits the query so a down responder does not
	// add its full timeout to every reconnect handshake during its negative
	// TTL window.
	c.ocspMu.RLock()
	entry, cached := c.ocspCache[serial]
	c.ocspMu.RUnlock()
	if cached && time.Now().Before(entry.expiresAt) {
		if entry.unavailable {
			return false, &OCSPUnavailableError{URL: c.cfg.OCSPResponderURL, Err: ErrOCSPNegativeCached}
		}
		return entry.revoked, nil
	}

	// Query the OCSP responder.
	revoked, err := c.queryOCSP(cert)
	if err != nil {
		// Negative-cache the failure for a short window so a flood of
		// reconnect handshakes does not re-query a dead responder. This never
		// upgrades to "revoked" or "good"; it only suppresses re-queries.
		//
		// Precedence: a definitive cached verdict (revoked or good) always
		// wins over an unavailable result, so a query failure never clobbers a
		// revocation status this checker already determined. Without this
		// guard, a transient responder outage occurring after a definitive
		// "revoked" verdict would replace that entry with an unavailable one
		// and, in soft-fail mode, cause the known-revoked peer to be accepted
		// — masking a verdict a cheap cache lookup would otherwise produce.
		// We therefore only write the negative entry when no unexpired
		// definitive entry exists for this serial.
		c.ocspMu.Lock()
		if existing, ok := c.ocspCache[serial]; !ok || existing.unavailable || !time.Now().Before(existing.expiresAt) {
			c.ocspCache[serial] = &ocspCacheEntry{
				unavailable: true,
				expiresAt:   time.Now().Add(ocspUnavailableTTL),
			}
		}
		c.ocspMu.Unlock()
		return false, &OCSPUnavailableError{URL: c.cfg.OCSPResponderURL, Err: err}
	}

	// Cache the definitive result.
	c.ocspMu.Lock()
	c.ocspCache[serial] = &ocspCacheEntry{
		revoked:   revoked,
		expiresAt: time.Now().Add(time.Duration(c.cfg.OCSPCacheSeconds) * time.Second),
	}
	c.ocspMu.Unlock()

	return revoked, nil
}

// queryOCSP sends an OCSP request to the configured responder and parses
// the response. Returns true if the certificate is revoked.
func (c *Checker) queryOCSP(cert *x509.Certificate) (bool, error) {
	if c.issuerCert == nil {
		return false, ErrNoIssuerCertificate
	}

	// Build the OCSP request using golang.org/x/crypto/ocsp.
	reqBytes, err := ocsp.CreateRequest(cert, c.issuerCert, nil)
	if err != nil {
		return false, err
	}

	// Derive timeout from the checker's shutdown context so in-flight OCSP
	// requests are cancelled promptly during transport shutdown instead of
	// blocking for the full timeout duration.
	ctx, cancel := context.WithTimeout(c.shutdownCtx, 10*time.Second)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodPost,
		c.cfg.OCSPResponderURL, bytes.NewReader(reqBytes))
	if err != nil {
		return false, err
	}
	req.Header.Set("Content-Type", "application/ocsp-request")

	resp, err := c.httpClient.Do(req) // #nosec G704 -- OCSPResponderURL is from validated config, not user input
	if err != nil {
		return false, err
	}
	defer func() {
		if closeErr := resp.Body.Close(); closeErr != nil {
			slog.Debug("failed to close OCSP response body", "error", closeErr)
		}
	}()

	respBody, err := io.ReadAll(io.LimitReader(resp.Body, maxOCSPResponseSize))
	if err != nil {
		return false, err
	}

	ocspResp, err := ocsp.ParseResponseForCert(respBody, cert, c.issuerCert)
	if err != nil {
		return false, err
	}

	return ocspResp.Status == ocsp.Revoked, nil
}

// CfgRef returns a reference to the checker's config for wiring into the
// TLS callback. This avoids copying the OnCertRevoked callback.
func (c *Checker) CfgRef() *Config {
	return &c.cfg
}
