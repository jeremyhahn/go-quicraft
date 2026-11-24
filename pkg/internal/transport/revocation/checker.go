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
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/crypto/ocsp"

	"github.com/jeremyhahn/go-quicraft/pkg/internal/supervise"
)

// Mode determines which revocation checking methods to use.
type Mode string

const (
	// ModeCRL checks only CRL files.
	ModeCRL Mode = "crl"

	// ModeOCSP checks only OCSP responders.
	ModeOCSP Mode = "ocsp"

	// ModeBoth consults CRL and OCSP; revoked by either is revoked.
	ModeBoth Mode = "both"

	// ModeAny passes if either CRL or OCSP confirms revocation (default).
	ModeAny Mode = "any"
)

// ValidModes lists the recognized revocation mode strings (plus the empty
// string, which selects the default). Configuration validation uses this
// to reject typos that would otherwise silently select the default mode.
var ValidModes = []Mode{ModeCRL, ModeOCSP, ModeBoth, ModeAny}

// IsValidMode reports whether m is a recognized revocation mode. The
// empty string is valid and selects the default (ModeAny).
func IsValidMode(m Mode) bool {
	if m == "" {
		return true
	}
	for _, v := range ValidModes {
		if m == v {
			return true
		}
	}
	return false
}

// Default configuration values.
const (
	defaultOCSPCacheSeconds   = 300
	defaultOCSPTimeoutSeconds = 5
	defaultCheckInterval      = 60 * time.Second
	defaultMode               = ModeAny

	// maxOCSPResponseSize limits the OCSP response body to prevent
	// memory exhaustion from a malicious or misconfigured responder.
	maxOCSPResponseSize = 1 << 20 // 1 MB

	// ocspUnavailableTTL is the short negative-cache TTL for responder failures; it only suppresses re-queries and never overwrites a definitive verdict.
	ocspUnavailableTTL = 10 * time.Second

	// revocationClockSkew is the allowance applied when comparing CRL/OCSP
	// validity timestamps against the local clock, absorbing small clock
	// differences between this node and the CA/responder.
	revocationClockSkew = 5 * time.Minute

	// ocspNoNextUpdateMaxAge bounds the age of an OCSP response without NextUpdate (RFC 6960 2.4).
	ocspNoNextUpdateMaxAge = 24 * time.Hour

	// maxOCSPCacheEntries bounds ocspCache; when full, expired entries go first, then the one closest to expiry.
	maxOCSPCacheEntries = 4096
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

	// AllowSoftFail admits a peer whose revocation status is unknown; revoked still rejects. The zero value enforces.
	AllowSoftFail bool

	// OnCertRevoked is called when a certificate is found to be revoked
	// during a TLS handshake. The serial number is passed as a lowercase
	// hex string. May be nil.
	OnCertRevoked func(serial string)
}

// enforcing reports whether a revocation status that cannot be determined
// must reject the peer. It is the inverse of AllowSoftFail, named for the
// policy the call sites apply so none of them has to spell the negation.
func (c *Config) enforcing() bool {
	return !c.AllowSoftFail
}

// Validate reports whether Mode has the sources it requires; call SetDefaults first.
func (c *Config) Validate() error {
	if !IsValidMode(c.Mode) {
		return ErrInvalidRevocationMode
	}
	switch c.Mode {
	case ModeOCSP, ModeBoth:
		if c.OCSPResponderURL == "" {
			return ErrNoOCSPResponderURL
		}
	}
	switch c.Mode {
	case ModeCRL, ModeBoth:
		if len(c.CRLPaths) == 0 {
			return ErrNoCRLSources
		}
	}
	// ModeAny with no source consults nothing; reject it when enforcing.
	if (c.Mode == ModeAny || c.Mode == "") && c.enforcing() &&
		len(c.CRLPaths) == 0 && c.OCSPResponderURL == "" {
		return ErrNoRevocationSources
	}
	return nil
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

// ocspCacheEntry is a cached OCSP verdict, or a short-lived responder-unavailable entry when unavailable is true.
type ocspCacheEntry struct {
	revoked     bool
	unavailable bool
	expiresAt   time.Time
}

// Checker checks certificate revocation against reloaded CRL files and cached OCSP responses; safe for concurrent use.
type Checker struct {
	// mu protects revokedSet, adminRevoked, crlHashes, and crlCurrent.
	mu         sync.RWMutex
	revokedSet map[string]struct{} // scopedSerial(issuer, serial) -> revoked
	// adminRevoked holds the serials AddRevokedSerial revoked. It is kept
	// apart from revokedSet, which every CRL reload replaces wholesale:
	// sharing it silently undid each runtime revocation at the next reload.
	adminRevoked map[string]struct{}
	crlHashes    map[string][32]byte // path -> SHA-256 content hash
	// crlCurrent is true while revokedSet reflects a fully successful load; when false, not-found is ErrCRLStale under enforcement.
	crlCurrent bool
	// crlSigners holds the issuers of the last successful reload's CRLs, kept by PruneRetainedIssuers.
	crlSigners []*x509.Certificate

	// ocspMu protects ocspCache.
	ocspMu    sync.RWMutex
	ocspCache map[string]*ocspCacheEntry // scopedSerial(issuer, serial) -> result

	// issuers holds every candidate issuing CA from the bundle, selected per leaf and per CRL; SetIssuers swaps it whole.
	issuers atomic.Pointer[[]*x509.Certificate]
	// retained holds issuers a CA reload dropped, backing established sessions' verdicts only, never a new handshake.
	retained atomic.Pointer[[]*x509.Certificate]
	// issuerMu serializes writers of issuers and retained, and ReloadCRLs; lock order issuerMu before mu.
	issuerMu       sync.Mutex
	cfg            Config
	httpClient     *http.Client
	started        atomic.Bool
	stopped        atomic.Bool
	stopC          chan struct{}
	shutdownCtx    context.Context
	shutdownCancel context.CancelFunc
}

// NewChecker creates a revocation checker with the CA bundle as candidate issuers; an empty list suits only programmatic use.
func NewChecker(cfg Config, issuers ...*x509.Certificate) *Checker {
	cfg.SetDefaults()
	ctx, cancel := context.WithCancel(context.Background())
	c := &Checker{
		revokedSet:   make(map[string]struct{}),
		adminRevoked: make(map[string]struct{}),
		crlHashes:    make(map[string][32]byte),
		ocspCache:    make(map[string]*ocspCacheEntry),
		cfg:          cfg,
		httpClient: &http.Client{
			Timeout: time.Duration(cfg.OCSPTimeoutSeconds) * time.Second,
		},
		stopC:          make(chan struct{}),
		shutdownCtx:    ctx,
		shutdownCancel: cancel,
	}
	c.SetIssuers(issuers...)
	return c
}

// SetIssuers replaces the candidate issuing CAs; dropped issuers move to the retained set until PruneRetainedIssuers releases them.
func (c *Checker) SetIssuers(issuers ...*x509.Certificate) {
	kept := make([]*x509.Certificate, 0, len(issuers))
	for _, issuer := range issuers {
		if issuer != nil {
			kept = append(kept, issuer)
		}
	}

	c.issuerMu.Lock()
	defer c.issuerMu.Unlock()
	var retained []*x509.Certificate
	for _, prev := range [][]*x509.Certificate{c.issuerSet(), c.retainedSet()} {
		for _, issuer := range prev {
			if !containsCert(kept, issuer) && !containsCert(retained, issuer) {
				retained = append(retained, issuer)
			}
		}
	}
	c.issuers.Store(&kept)
	c.retained.Store(&retained)
}

// PruneRetainedIssuers releases retained issuers that issued no leaf in live and signed no configured CRL.
func (c *Checker) PruneRetainedIssuers(live []*x509.Certificate) {
	c.issuerMu.Lock()
	defer c.issuerMu.Unlock()
	retained := c.retainedSet()
	if len(retained) == 0 {
		return
	}
	trusted := c.issuerSet()
	var needed []*x509.Certificate
	for _, leaf := range live {
		if leaf == nil || matchIssuer(leaf, trusted) != nil {
			continue
		}
		if issuer := matchIssuer(leaf, retained); issuer != nil && !containsCert(needed, issuer) {
			needed = append(needed, issuer)
		}
	}
	c.mu.RLock()
	for _, signer := range c.crlSigners {
		if containsCert(retained, signer) && !containsCert(needed, signer) {
			needed = append(needed, signer)
		}
	}
	c.mu.RUnlock()
	c.retained.Store(&needed)
}

// containsCert reports whether set holds a certificate equal to cert.
func containsCert(set []*x509.Certificate, cert *x509.Certificate) bool {
	return slices.ContainsFunc(set, cert.Equal)
}

// issuerSet returns the current candidate issuers.
func (c *Checker) issuerSet() []*x509.Certificate {
	if p := c.issuers.Load(); p != nil {
		return *p
	}
	return nil
}

// retainedSet returns the current retained-issuer snapshot (see retained).
func (c *Checker) retainedSet() []*x509.Certificate {
	if p := c.retained.Load(); p != nil {
		return *p
	}
	return nil
}

// anyIssuerScope keys issuer-less AddRevokedSerial records, consulted for every leaf.
const anyIssuerScope = "*"

// issuerScope returns SHA-256 of an issuing CA's RawSubject and RawSubjectPublicKeyInfo, since serials are unique only per issuer.
func issuerScope(issuer *x509.Certificate) string {
	h := sha256.New()
	h.Write(issuer.RawSubject)
	h.Write(issuer.RawSubjectPublicKeyInfo)
	return string(h.Sum(nil))
}

// scopedSerial builds the (issuer scope, hex serial) key used by revokedSet
// and ocspCache.
func scopedSerial(scope, serial string) string {
	return scope + "|" + serial
}

// scopeFor returns the issuer scope to look leaf up under, falling back to
// anyIssuerScope when no configured CA verifies it — nothing issuer-specific
// can match such a leaf, so only issuer-agnostic records apply.
func (c *Checker) scopeFor(leaf *x509.Certificate) string {
	if issuer := c.issuerFor(leaf); issuer != nil {
		return issuerScope(issuer)
	}
	return anyIssuerScope
}

// issuerFor returns the configured CA that issued leaf, by signature then AuthorityKeyId, or nil.
func (c *Checker) issuerFor(leaf *x509.Certificate) *x509.Certificate {
	return matchIssuer(leaf, c.issuerSet())
}

// matchIssuer returns the candidate in set that signed leaf, falling back to
// an AuthorityKeyId match, or nil when none did.
func matchIssuer(leaf *x509.Certificate, set []*x509.Certificate) *x509.Certificate {
	for _, candidate := range set {
		if leaf.CheckSignatureFrom(candidate) == nil {
			return candidate
		}
	}
	if len(leaf.AuthorityKeyId) > 0 {
		for _, candidate := range set {
			if bytes.Equal(candidate.SubjectKeyId, leaf.AuthorityKeyId) {
				return candidate
			}
		}
	}
	return nil
}

// crlIssuerFor selects the configured CA that signed crl, or nil when none
// of the candidates verify it.
func (c *Checker) crlIssuerFor(crl *x509.RevocationList) *x509.Certificate {
	for _, set := range [][]*x509.Certificate{c.issuerSet(), c.retainedSet()} {
		for _, candidate := range set {
			if crl.CheckSignatureFrom(candidate) == nil {
				return candidate
			}
		}
	}
	return nil
}

// Start performs the initial CRL load and begins the periodic CRL reload
// goroutine. Idempotent: only the first call has any effect; subsequent
// calls return without starting a second reload goroutine.
func (c *Checker) Start() {
	if !c.started.CompareAndSwap(false, true) {
		return
	}
	err := c.ReloadCRLs()
	// Start the reload loop before the contained diagnostic.
	go c.reloadLoop()
	if err != nil {
		supervise.LogContained(func() { slog.Warn("initial CRL reload failed", "error", err) })
	}
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
				// Contained: the loop must outlive any log handler, or the
				// revocation set is never refreshed — nor marked stale —
				// again.
				supervise.LogContained(func() { slog.Warn("CRL reload failed", "error", err) })
			}
		}
	}
}

// ReloadCRLs reloads, verifies and checks freshness of every configured CRL, all or nothing; any failure keeps the old set and marks it stale.
func (c *Checker) ReloadCRLs() error {
	if len(c.cfg.CRLPaths) == 0 {
		return nil
	}

	c.issuerMu.Lock()
	defer c.issuerMu.Unlock()

	newSet := make(map[string]struct{})
	newHashes := make(map[string][32]byte)
	now := time.Now()
	var reloaded []reloadedCRL
	var signers []*x509.Certificate

	for _, path := range c.cfg.CRLPaths {
		data, err := os.ReadFile(path)
		if err != nil {
			// Stale first, then the diagnostic, contained: a log handler
			// that panicked or called runtime.Goexit before the mark left
			// the set current — revocation fail-open.
			c.markCRLStale()
			supervise.LogContained(func() {
				slog.Warn("CRL file read failed; keeping previous revocation set",
					"path", path, "error", err)
			})
			return &CRLReadError{Path: path, Err: err}
		}

		hash := sha256.Sum256(data)
		newHashes[path] = hash

		// Parse PEM-encoded CRL blocks.
		crls, parseErr := parseCRLs(data)
		if parseErr != nil {
			c.markCRLStale()
			return &CRLParseError{Path: path, Err: parseErr}
		}
		// A CRL file with no CRL blocks must not commit an empty set.
		if len(crls) == 0 {
			c.markCRLStale()
			return &CRLEmptyError{Path: path}
		}

		serialCount := 0
		for _, crl := range crls {
			// A CRL must verify against a configured issuing CA, trying every bundle candidate.
			if len(c.issuerSet()) == 0 && len(c.retainedSet()) == 0 {
				c.markCRLStale()
				return &CRLSignatureError{Path: path, Err: ErrNoIssuerCertificate}
			}
			crlIssuer := c.crlIssuerFor(crl)
			if crlIssuer == nil {
				c.markCRLStale()
				return &CRLSignatureError{Path: path, Err: ErrCRLSignature}
			}
			// A post-dated CRL (CA clock skew, or a deliberately future-dated
			// file) proves nothing about the present, so it is treated the same
			// as an expired one.
			if crl.ThisUpdate.IsZero() || now.Add(revocationClockSkew).Before(crl.ThisUpdate) {
				c.markCRLStale()
				return &CRLNotYetValidError{Path: path, ThisUpdate: crl.ThisUpdate}
			}
			// A CRL past NextUpdate marks the set stale; the old revoked set is kept.
			if !crl.NextUpdate.IsZero() && now.After(crl.NextUpdate.Add(revocationClockSkew)) {
				c.markCRLStale()
				return &CRLExpiredError{Path: path, NextUpdate: crl.NextUpdate}
			}
			// Scope each entry to the CRL's signing CA.
			scope := issuerScope(crlIssuer)
			if !containsCert(signers, crlIssuer) {
				signers = append(signers, crlIssuer)
			}
			for _, entry := range crl.RevokedCertificateEntries {
				newSet[scopedSerial(scope, entry.SerialNumber.Text(16))] = struct{}{}
				serialCount++
			}
		}

		// Log only on change, after the new set is committed.
		c.mu.RLock()
		oldHash, existed := c.crlHashes[path]
		c.mu.RUnlock()
		if !existed || oldHash != hash {
			reloaded = append(reloaded, reloadedCRL{path: path, serials: serialCount})
		}
	}

	c.mu.Lock()
	c.revokedSet = newSet
	c.crlHashes = newHashes
	c.crlSigners = signers
	c.crlCurrent = true
	c.mu.Unlock()

	if len(reloaded) > 0 {
		supervise.LogContained(func() {
			for _, r := range reloaded {
				slog.Info("CRL reloaded", "path", r.path, "revoked_serials", r.serials)
			}
		})
	}
	return nil
}

// reloadedCRL names a CRL file whose content a reload found changed, for the
// diagnostic emitted once the reload has committed.
type reloadedCRL struct {
	path    string
	serials int
}

// markCRLStale records that the revoked set no longer reflects a fully successful load.
func (c *Checker) markCRLStale() {
	c.mu.Lock()
	c.crlCurrent = false
	c.mu.Unlock()
}

// parseCRLs extracts the CRLs from PEM data, skipping other blocks; validation is the caller's.
func parseCRLs(data []byte) ([]*x509.RevocationList, error) {
	var crls []*x509.RevocationList
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
		crls = append(crls, crl)
	}
	return crls, nil
}

// AddRevokedSerial adds a lowercase-hex serial to the revoked set for every issuer; it survives CRL reloads.
func (c *Checker) AddRevokedSerial(serial string) {
	c.mu.Lock()
	c.adminRevoked[serial] = struct{}{}
	c.mu.Unlock()
}

// RemoveRevokedSerial removes a serial AddRevokedSerial added; CRL-published revocations are unaffected.
func (c *Checker) RemoveRevokedSerial(serial string) {
	c.mu.Lock()
	delete(c.adminRevoked, serial)
	c.mu.Unlock()
}

// RevokedCount returns the number of revocation records currently held: CRL
// entries plus programmatic (AddRevokedSerial) revocations.
func (c *Checker) RevokedCount() int {
	c.mu.RLock()
	n := len(c.revokedSet) + len(c.adminRevoked)
	c.mu.RUnlock()
	return n
}

// IsRevoked reports whether cert is revoked; revoked from any source wins, and unknown status errors unless AllowSoftFail.
func (c *Checker) IsRevoked(cert *x509.Certificate) (bool, error) {
	return c.isRevoked(cert, c.issuerFor(cert))
}

// IsRevokedEstablished is IsRevoked for an established session's leaf, also resolving retained issuers; never for a new handshake.
func (c *Checker) IsRevokedEstablished(cert *x509.Certificate) (bool, error) {
	issuer := c.issuerFor(cert)
	if issuer == nil {
		issuer = matchIssuer(cert, c.retainedSet())
	}
	return c.isRevoked(cert, issuer)
}

// IsRevokedUnder reports whether cert is revoked, resolving its issuer in the bundle the handshake verified against.
func (c *Checker) IsRevokedUnder(cert *x509.Certificate, issuers []*x509.Certificate) (bool, error) {
	return c.isRevoked(cert, matchIssuer(cert, issuers))
}

// isRevoked dispatches on the mode with the issuer the caller resolved for
// cert; nil means none is known, which scopes the CRL lookup to the
// issuer-agnostic entries and leaves OCSP without a request to build.
func (c *Checker) isRevoked(cert, issuer *x509.Certificate) (bool, error) {
	serial := cert.SerialNumber.Text(16)
	scope := anyIssuerScope
	if issuer != nil {
		scope = issuerScope(issuer)
	}

	// Dispatch to the per-mode policy.
	switch c.cfg.Mode {
	case ModeCRL:
		return c.isRevokedCRL(scope, serial)
	case ModeOCSP:
		return c.isRevokedOCSP(cert, issuer, scope, serial)
	case ModeBoth:
		return c.isRevokedBoth(cert, issuer, scope, serial)
	default: // ModeAny
		return c.isRevokedAny(cert, issuer, scope, serial)
	}
}

// noSourceBackstop fails an enforcing check when no CRL path is configured, since not-found then proves nothing.
func (c *Checker) noSourceBackstop() error {
	if len(c.cfg.CRLPaths) == 0 {
		return ErrNoRevocationSources
	}
	return nil
}

// isRevokedCRL consults CRL state only.
func (c *Checker) isRevokedCRL(scope, serial string) (bool, error) {
	revoked, crlErr := c.checkCRLStatus(scope, serial)
	if revoked {
		return true, nil
	}
	if c.cfg.enforcing() {
		if crlErr != nil {
			return false, crlErr
		}
		if err := c.noSourceBackstop(); err != nil {
			return false, err
		}
	}
	return false, nil
}

// revokedLocked reports whether serial is revoked by a CRL entry under the
// leaf's issuer scope or the issuer-agnostic scope, or by AddRevokedSerial.
// The caller holds c.mu.
func (c *Checker) revokedLocked(scope, serial string) bool {
	if _, ok := c.revokedSet[scopedSerial(scope, serial)]; ok {
		return true
	}
	if _, ok := c.revokedSet[scopedSerial(anyIssuerScope, serial)]; ok {
		return true
	}
	_, ok := c.adminRevoked[serial]
	return ok
}

// isRevokedOCSP consults OCSP and the programmatic revoked set, not CRL staleness.
func (c *Checker) isRevokedOCSP(cert, issuer *x509.Certificate, scope, serial string) (bool, error) {
	c.mu.RLock()
	revoked := c.revokedLocked(scope, serial)
	c.mu.RUnlock()
	if revoked {
		return true, nil
	}
	return c.checkOCSP(cert, issuer)
}

// isRevokedBoth consults CRL and OCSP, revoked by either; unknown follows AllowSoftFail.
func (c *Checker) isRevokedBoth(cert, issuer *x509.Certificate, scope, serial string) (bool, error) {
	crlRevoked, crlErr := c.checkCRLStatus(scope, serial)
	if crlRevoked {
		return true, nil
	}
	ocspRevoked, ocspErr := c.checkOCSPRaw(cert, issuer)
	if ocspErr == nil && ocspRevoked {
		return true, nil
	}
	if c.cfg.enforcing() {
		if crlErr != nil {
			return false, crlErr
		}
		if ocspErr != nil {
			return false, ocspErr
		}
	}
	return false, nil
}

// isRevokedAny consults the configured sources; unknown from either fails closed when enforcing.
func (c *Checker) isRevokedAny(cert, issuer *x509.Certificate, scope, serial string) (bool, error) {
	crlRevoked, crlErr := c.checkCRLStatus(scope, serial)
	if crlRevoked {
		return true, nil
	}
	if c.cfg.OCSPResponderURL == "" {
		return c.isRevokedCRLTail(crlErr)
	}

	ocspRevoked, ocspErr := c.checkOCSPRaw(cert, issuer)
	if ocspErr == nil && ocspRevoked {
		return true, nil
	}
	if c.cfg.enforcing() {
		if ocspErr != nil {
			return false, ocspErr
		}
		if crlErr != nil {
			return false, crlErr
		}
	}
	return false, nil
}

// isRevokedCRLTail applies the enforcement tail shared by the CRL-only paths:
// a CRL error wins, then the missing-source backstop.
func (c *Checker) isRevokedCRLTail(crlErr error) (bool, error) {
	if !c.cfg.enforcing() {
		return false, nil
	}
	if crlErr != nil {
		return false, crlErr
	}
	if err := c.noSourceBackstop(); err != nil {
		return false, err
	}
	return false, nil
}

// checkCRLStatus looks up the serial in the leaf's issuer scope and the issuer-agnostic scope; the error is non-nil when not-found cannot be trusted.
func (c *Checker) checkCRLStatus(scope, serial string) (bool, error) {
	c.mu.RLock()
	revoked := c.revokedLocked(scope, serial)
	current := c.crlCurrent
	c.mu.RUnlock()
	if revoked {
		// Found in the set: revoked, whether the set came from CRL files
		// or a programmatic AddRevokedSerial, and even when stale.
		return true, nil
	}
	// Staleness only applies when CRL files are configured; a checker
	// used purely programmatically (no paths) has nothing to go stale.
	if len(c.cfg.CRLPaths) > 0 && !current {
		return false, ErrCRLStale
	}
	return false, nil
}

// checkOCSP queries OCSP through the cache; an unavailable result errors when enforcing and accepts under soft-fail.
func (c *Checker) checkOCSP(cert, issuer *x509.Certificate) (bool, error) {
	revoked, err := c.checkOCSPRaw(cert, issuer)
	if err != nil {
		if c.cfg.enforcing() {
			return false, err
		}
		return false, nil
	}
	return revoked, nil
}

// checkOCSPRaw queries OCSP without enforcement policy, returning the raw error; issuer is nil when unknown.
func (c *Checker) checkOCSPRaw(cert, issuer *x509.Certificate) (bool, error) {
	if c.cfg.OCSPResponderURL == "" {
		return false, &OCSPUnavailableError{
			URL: "(none configured)",
			Err: ErrNoOCSPResponderURL,
		}
	}

	// The issuer is required first: the cache key is scoped to it.
	if issuer == nil {
		return false, &OCSPUnavailableError{URL: c.cfg.OCSPResponderURL, Err: ErrNoIssuerCertificate}
	}
	key := scopedSerial(issuerScope(issuer), cert.SerialNumber.Text(16))

	// Check the cache first; a cached unavailable entry skips the query.
	c.ocspMu.RLock()
	entry, cached := c.ocspCache[key]
	c.ocspMu.RUnlock()
	if cached && time.Now().Before(entry.expiresAt) {
		if entry.unavailable {
			return false, &OCSPUnavailableError{URL: c.cfg.OCSPResponderURL, Err: ErrOCSPNegativeCached}
		}
		return entry.revoked, nil
	}

	// Query the OCSP responder.
	revoked, nextUpdate, err := c.queryOCSP(cert, issuer)
	if err != nil {
		// Negative-cache the failure only when no unexpired definitive verdict exists for the serial.
		c.ocspMu.Lock()
		if existing, ok := c.ocspCache[key]; !ok || existing.unavailable || !time.Now().Before(existing.expiresAt) {
			c.evictOCSPCacheLocked(time.Now())
			c.ocspCache[key] = &ocspCacheEntry{
				unavailable: true,
				expiresAt:   time.Now().Add(ocspUnavailableTTL),
			}
		}
		c.ocspMu.Unlock()
		return false, &OCSPUnavailableError{URL: c.cfg.OCSPResponderURL, Err: err}
	}

	// Cache the verdict no longer than the response's NextUpdate.
	now := time.Now()
	expiresAt := now.Add(time.Duration(c.cfg.OCSPCacheSeconds) * time.Second)
	if !nextUpdate.IsZero() && nextUpdate.Before(expiresAt) {
		expiresAt = nextUpdate
	}
	c.ocspMu.Lock()
	c.evictOCSPCacheLocked(now)
	c.ocspCache[key] = &ocspCacheEntry{
		revoked:   revoked,
		expiresAt: expiresAt,
	}
	c.ocspMu.Unlock()

	return revoked, nil
}

// evictOCSPCacheLocked bounds ocspCache to maxOCSPCacheEntries ahead of an
// insert: expired entries are pruned first, and if the cache is still full
// the entries closest to expiry are evicted. The caller must hold ocspMu.
func (c *Checker) evictOCSPCacheLocked(now time.Time) {
	if len(c.ocspCache) < maxOCSPCacheEntries {
		return
	}
	for serial, entry := range c.ocspCache {
		if !now.Before(entry.expiresAt) {
			delete(c.ocspCache, serial)
		}
	}
	for len(c.ocspCache) >= maxOCSPCacheEntries {
		var oldestSerial string
		var oldestExpiry time.Time
		for serial, entry := range c.ocspCache {
			if oldestSerial == "" || entry.expiresAt.Before(oldestExpiry) {
				oldestSerial = serial
				oldestExpiry = entry.expiresAt
			}
		}
		delete(c.ocspCache, oldestSerial)
	}
}

// queryOCSP queries the responder for cert under issuerCert, returning revoked and NextUpdate; RFC 6960 unknown is an error.
func (c *Checker) queryOCSP(cert, issuerCert *x509.Certificate) (bool, time.Time, error) {
	// x/crypto/ocsp cannot attach a nonce (RFC 8954), so replay protection relies on validateOCSPFreshness.
	reqBytes, err := ocsp.CreateRequest(cert, issuerCert, nil)
	if err != nil {
		return false, time.Time{}, err
	}

	// Bound the request by the configured timeout and the shutdown context.
	ctx, cancel := context.WithTimeout(c.shutdownCtx, c.ocspRequestTimeout())
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodPost,
		c.cfg.OCSPResponderURL, bytes.NewReader(reqBytes))
	if err != nil {
		return false, time.Time{}, err
	}
	req.Header.Set("Content-Type", "application/ocsp-request")

	resp, err := c.httpClient.Do(req) // #nosec G704 -- OCSPResponderURL is from validated config, not user input
	if err != nil {
		return false, time.Time{}, err
	}
	defer func() {
		if closeErr := resp.Body.Close(); closeErr != nil {
			// Contained: this runs inside a TLS handshake's revocation check
			// and inside the transport's cleanup sweep.
			supervise.LogContained(func() { slog.Debug("failed to close OCSP response body", "error", closeErr) })
		}
	}()

	respBody, err := io.ReadAll(io.LimitReader(resp.Body, maxOCSPResponseSize))
	if err != nil {
		return false, time.Time{}, err
	}

	// ParseResponseForCert verifies the responder signature chains to the
	// issuer certificate.
	ocspResp, err := ocsp.ParseResponseForCert(respBody, cert, issuerCert)
	if err != nil {
		return false, time.Time{}, err
	}
	if err := validateOCSPResponder(ocspResp, issuerCert, time.Now()); err != nil {
		return false, time.Time{}, err
	}

	// Reject responses outside their validity window as unavailable.
	if err := validateOCSPFreshness(ocspResp, time.Now()); err != nil {
		return false, time.Time{}, err
	}

	// RFC 6960 unknown is not a not-revoked verdict; return an error.
	switch ocspResp.Status {
	case ocsp.Revoked:
		return true, ocspResp.NextUpdate, nil
	case ocsp.Good:
		return false, ocspResp.NextUpdate, nil
	default:
		return false, time.Time{}, &OCSPStatusUnknownError{Serial: cert.SerialNumber.Text(16)}
	}
}

// ocspRequestTimeout returns the per-request OCSP timeout from the
// configured OCSPTimeoutSeconds (SetDefaults guarantees a positive value;
// the default is used as a backstop for a zero value).
func (c *Checker) ocspRequestTimeout() time.Duration {
	if c.cfg.OCSPTimeoutSeconds > 0 {
		return time.Duration(c.cfg.OCSPTimeoutSeconds) * time.Second
	}
	return defaultOCSPTimeoutSeconds * time.Second
}

// validateOCSPFreshness checks the response window covers now with revocationClockSkew, bounding a missing NextUpdate by ocspNoNextUpdateMaxAge.
func validateOCSPFreshness(resp *ocsp.Response, now time.Time) error {
	if resp.ThisUpdate.IsZero() || now.Add(revocationClockSkew).Before(resp.ThisUpdate) {
		return &OCSPResponseStaleError{
			Reason:     "ThisUpdate in the future",
			ThisUpdate: resp.ThisUpdate,
			NextUpdate: resp.NextUpdate,
		}
	}
	if resp.NextUpdate.IsZero() {
		if now.Sub(resp.ThisUpdate) > ocspNoNextUpdateMaxAge {
			return &OCSPResponseStaleError{
				Reason:     "no NextUpdate and response older than bounded maximum age",
				ThisUpdate: resp.ThisUpdate,
				NextUpdate: resp.NextUpdate,
			}
		}
		return nil
	}
	if now.After(resp.NextUpdate.Add(revocationClockSkew)) {
		return &OCSPResponseStaleError{
			Reason:     "NextUpdate in the past",
			ThisUpdate: resp.ThisUpdate,
			NextUpdate: resp.NextUpdate,
		}
	}
	return nil
}

// validateOCSPResponder rejects a delegated responder without id-kp-OCSPSigning or outside its validity (RFC 6960 4.2.2.2).
func validateOCSPResponder(resp *ocsp.Response, issuer *x509.Certificate, now time.Time) error {
	responder := resp.Certificate
	if responder == nil || (issuer != nil && responder.Equal(issuer)) {
		return nil
	}
	if !slices.Contains(responder.ExtKeyUsage, x509.ExtKeyUsageOCSPSigning) {
		return ErrOCSPResponderUnauthorized
	}
	if now.Add(revocationClockSkew).Before(responder.NotBefore) || now.After(responder.NotAfter.Add(revocationClockSkew)) {
		return ErrOCSPResponderUnauthorized
	}
	return nil
}

// OnCertRevoked returns the configured revocation callback, or nil.
func (c *Checker) OnCertRevoked() func(string) {
	return c.cfg.OnCertRevoked
}
