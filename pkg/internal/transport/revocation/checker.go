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

	// ModeBoth consults both CRL and OCSP; a certificate reported revoked
	// by EITHER source is treated as revoked. (Requiring both sources to
	// agree would let a CRL-revoked certificate with a stale OCSP "good"
	// pass — the weakest possible composition.)
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
	// Soft-fail tradeoff: under AllowSoftFail, a cached unavailable
	// entry causes the peer to be accepted (status unknown). After the
	// responder recovers and would report a cert as revoked, that revocation
	// can go undetected for at most ocspUnavailableTTL (until the negative
	// entry expires and the next handshake re-queries). This bounded delay is
	// inherent to soft-fail: with no reachable responder there is by
	// definition no fresh verdict to act on. It is NOT a masking of a verdict
	// already known to this checker — a definitive cached "revoked" (or
	// "good") is never overwritten by a later unavailable result (see
	// checkOCSPRaw). Under enforcement the cached unavailable entry fails
	// closed regardless.
	ocspUnavailableTTL = 10 * time.Second

	// revocationClockSkew is the allowance applied when comparing CRL/OCSP
	// validity timestamps against the local clock, absorbing small clock
	// differences between this node and the CA/responder.
	revocationClockSkew = 5 * time.Minute

	// ocspNoNextUpdateMaxAge bounds acceptance of OCSP responses that omit
	// NextUpdate. RFC 6960 §2.4 defines an absent NextUpdate as "newer
	// revocation information is available all the time", which gives a
	// replayed response no expiry at all; clients must impose their own
	// bound, so responses older than this are treated as stale.
	ocspNoNextUpdateMaxAge = 24 * time.Hour

	// maxOCSPCacheEntries bounds ocspCache. Each handshake with a distinct
	// peer serial inserts one entry, and a churning (or hostile) peer set
	// would otherwise grow the map without limit. When full, expired
	// entries are pruned first, then the entry closest to expiry.
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

	// AllowSoftFail, when true, admits a peer whose revocation status cannot
	// be determined (OCSP responder unreachable, CRL stale or unreadable, no
	// revocation source configured at all). A definitive "revoked" verdict
	// still rejects regardless of this flag.
	//
	// THE ZERO VALUE ENFORCES. Fail-closed is intrinsic to this package: a
	// Config nobody configured rejects unknown status, and soft-fail has to
	// be asked for. Every construction path — Host, an embedding of the
	// transport, a tool, a test — therefore inherits the safe posture, rather
	// than the posture depending on one caller remembering to set a field.
	//
	// The name and sense match config.RevocationConfig.AllowSoftFail, which
	// is the operator-facing knob, so the two layers cannot drift apart
	// through a mistaken negation.
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

// Validate reports whether the selected Mode has the sources it requires.
// Call SetDefaults first. Without this check a mode can be silently
// unsatisfiable: ModeBoth/ModeOCSP with no responder URL fails every
// handshake under enforcement, and ModeCRL/ModeBoth with no CRL file
// can never observe a file-published revocation.
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
	// ModeAny (and the empty string that selects it) names no specific source,
	// so it is the one mode the per-mode checks above cannot catch: with
	// nothing configured it consults nothing and reports every certificate as
	// not revoked. Reject that only when enforcing — soft-fail is the
	// documented opt-out for "revocation status may be unknown", which
	// includes having no source at all.
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
	// mu protects revokedSet, adminRevoked, crlHashes, and crlCurrent.
	mu         sync.RWMutex
	revokedSet map[string]struct{} // scopedSerial(issuer, serial) -> revoked
	// adminRevoked holds the serials AddRevokedSerial revoked. It is kept
	// apart from revokedSet, which every CRL reload replaces wholesale:
	// sharing it silently undid each runtime revocation at the next reload.
	adminRevoked map[string]struct{}
	crlHashes    map[string][32]byte // path -> SHA-256 content hash
	// crlCurrent is true while revokedSet reflects a fully-successful
	// load of every configured CRL file. It is false before the first
	// successful load and after any failed reload (unreadable or
	// unparseable file). While false, the previous set keeps serving
	// revocations (a stale "revoked" is still trustworthy) but a
	// not-found result cannot be trusted as "not revoked"; under
	// enforcement that surfaces as ErrCRLStale.
	crlCurrent bool

	// ocspMu protects ocspCache.
	ocspMu    sync.RWMutex
	ocspCache map[string]*ocspCacheEntry // scopedSerial(issuer, serial) -> result

	// issuers holds every candidate issuing CA parsed from the configured CA
	// bundle. A deployment with a root plus one or more issuing
	// intermediates presents leaves signed by an intermediate, so a single
	// hardcoded issuer (the bundle's first block) would build OCSP requests
	// carrying the wrong issuerNameHash/issuerKeyHash and would reject every
	// intermediate-signed CRL. The issuer is therefore selected per leaf and
	// per CRL at check time.
	issuers        []*x509.Certificate
	cfg            Config
	httpClient     *http.Client
	started        atomic.Bool
	stopped        atomic.Bool
	stopC          chan struct{}
	shutdownCtx    context.Context
	shutdownCancel context.CancelFunc
}

// NewChecker creates a new revocation checker. The issuers are the CA
// certificates (the full trust bundle, root and intermediates) used to
// verify OCSP responses and CRL signatures; the one that actually issued a
// given leaf or CRL is selected at check time. Nil entries are ignored.
//
// The issuer list may be empty only for purely programmatic use
// (AddRevokedSerial with no CRL files and no OCSP responder): loading a CRL
// file requires an issuer to verify the CRL signature, and reloads fail
// closed (stale set) without one.
func NewChecker(cfg Config, issuers ...*x509.Certificate) *Checker {
	cfg.SetDefaults()
	ctx, cancel := context.WithCancel(context.Background())
	kept := make([]*x509.Certificate, 0, len(issuers))
	for _, issuer := range issuers {
		if issuer != nil {
			kept = append(kept, issuer)
		}
	}
	return &Checker{
		revokedSet:   make(map[string]struct{}),
		adminRevoked: make(map[string]struct{}),
		crlHashes:    make(map[string][32]byte),
		ocspCache:    make(map[string]*ocspCacheEntry),
		issuers:      kept,
		cfg:          cfg,
		httpClient: &http.Client{
			Timeout: time.Duration(cfg.OCSPTimeoutSeconds) * time.Second,
		},
		stopC:          make(chan struct{}),
		shutdownCtx:    ctx,
		shutdownCancel: cancel,
	}
}

// anyIssuerScope keys revocation records that carry no issuer: the
// programmatic AddRevokedSerial admin path. Lookups consult the leaf's own
// issuer scope AND this one, so a hand-revoked serial stays revoked whichever
// configured CA issued the certificate. Over-revoking fails closed; binding
// the admin path to a single issuer it cannot name would fail open.
const anyIssuerScope = "*"

// issuerScope returns a stable identifier for an issuing CA: the SHA-256 of
// its RawSubject followed by its RawSubjectPublicKeyInfo. X.509 serial numbers
// are unique only per ISSUER, so a bundle holding several intermediates can
// legitimately contain two certificates with the same serial; keying
// revocation records or cached OCSP verdicts on the serial alone lets one
// intermediate's verdict decide the other's leaf. SubjectKeyId is not used as
// the identity: it is an optional extension chosen by whoever issued the CA.
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

// issuerFor selects the configured CA that issued leaf: the candidate whose
// key verifies the leaf's signature, falling back to an AuthorityKeyId match
// when the signature check is inconclusive (e.g. an unsupported algorithm).
// Returns nil when no candidate qualifies; callers fail closed.
func (c *Checker) issuerFor(leaf *x509.Certificate) *x509.Certificate {
	for _, candidate := range c.issuers {
		if leaf.CheckSignatureFrom(candidate) == nil {
			return candidate
		}
	}
	if len(leaf.AuthorityKeyId) > 0 {
		for _, candidate := range c.issuers {
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
	for _, candidate := range c.issuers {
		if crl.CheckSignatureFrom(candidate) == nil {
			return candidate
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
	// The reload loop is started before the diagnostic, which is contained:
	// a log handler that panicked or called runtime.Goexit here left the
	// loop unstarted, so the CRL was never loaded and a set loaded earlier
	// never went stale.
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

// ReloadCRLs reads all configured CRL files, parses them, verifies each
// CRL's signature against the issuing CA, checks freshness (NextUpdate),
// and rebuilds the revoked serial set. This method is exported to support
// manual refresh (e.g., from an admin API or signal handler).
//
// The reload is all-or-nothing: on ANY per-file failure (read, parse,
// signature, or expiry) the previous revoked set and hashes are kept and
// an error is returned. Replacing the full set with a partial one would
// silently un-revoke every certificate listed in the unreadable file — a
// transiently missing file, permission change, or NFS blip must never
// widen access. A failed reload also marks the set as stale (see
// crlCurrent), which enforcement surfaces as a handshake failure
// until a reload succeeds. Because the loop re-runs every CheckInterval,
// a CRL that passes its NextUpdate while loaded is detected within one
// interval and the set goes stale — a frozen CRL file cannot serve an
// ever-staler view of revocations indefinitely.
func (c *Checker) ReloadCRLs() error {
	if len(c.cfg.CRLPaths) == 0 {
		return nil
	}

	newSet := make(map[string]struct{})
	newHashes := make(map[string][32]byte)
	now := time.Now()
	var reloaded []reloadedCRL

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
		// A configured CRL file that yields zero CRL blocks must NOT commit a
		// fresh empty revoked set: swapping a real CRL for an unrelated PEM
		// (or a truncated file) would otherwise un-revoke everything and mark
		// the set current, defeating fail-closed enforcement.
		if len(crls) == 0 {
			c.markCRLStale()
			return &CRLEmptyError{Path: path}
		}

		serialCount := 0
		for _, crl := range crls {
			// A CRL whose signature does not verify against one of the
			// configured issuing CAs must never replace the revoked set: a
			// forged (or foreign) file could silently un-revoke certificates.
			// No issuer certificate means no way to verify, which fails closed
			// the same way. Every bundle candidate is tried because an
			// intermediate — not the root — signs the CRL in a two-tier PKI.
			if len(c.issuers) == 0 {
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
			// A CRL past NextUpdate no longer proves anything about serials
			// it does not list; mark stale so enforcement fails closed
			// until the CA publishes a fresh CRL. (The previous revoked set
			// is kept: a stale "revoked" remains trustworthy.)
			if !crl.NextUpdate.IsZero() && now.After(crl.NextUpdate.Add(revocationClockSkew)) {
				c.markCRLStale()
				return &CRLExpiredError{Path: path, NextUpdate: crl.NextUpdate}
			}
			// Scope every entry to the CA that actually signed this CRL: a
			// serial is unique only per issuer, so an unscoped entry would
			// revoke a same-serial certificate issued by a different
			// intermediate in the same bundle.
			scope := issuerScope(crlIssuer)
			for _, entry := range crl.RevokedCertificateEntries {
				newSet[scopedSerial(scope, entry.SerialNumber.Text(16))] = struct{}{}
				serialCount++
			}
		}

		// Log only when content changed, and only once the new set is
		// committed below: a log handler that panicked or called
		// runtime.Goexit here used to keep the reloaded CRL from ever
		// taking effect.
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

// markCRLStale records that the revoked set no longer reflects a
// fully-successful load of every configured CRL file.
func (c *Checker) markCRLStale() {
	c.mu.Lock()
	c.crlCurrent = false
	c.mu.Unlock()
}

// parseCRLs extracts parsed revocation lists from PEM-encoded CRL data.
// Supports multiple CRL blocks in a single file. Non-CRL PEM blocks are
// skipped, so a nil error with an empty result means the file held no CRL
// at all; ReloadCRLs rejects that case rather than committing an empty
// revoked set. Signature and freshness validation are also the caller's
// responsibility (ReloadCRLs).
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

// AddRevokedSerial programmatically adds a serial number to the revoked
// set. The serial should be a lowercase hex string. This is useful for
// runtime revocation via admin API without modifying CRL files on disk.
//
// The entry is issuer-agnostic, so it revokes the serial no matter which
// configured CA issued the certificate: the admin API names no issuer, and
// over-revoking is the fail-closed direction. It survives CRL reloads.
func (c *Checker) AddRevokedSerial(serial string) {
	c.mu.Lock()
	c.adminRevoked[serial] = struct{}{}
	c.mu.Unlock()
}

// RemoveRevokedSerial programmatically removes a serial number from the
// revoked set. This is useful for undoing a programmatic revocation. It
// removes only the entry AddRevokedSerial created: a CRL-published revocation
// is owned by the CA and is unaffected.
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

// IsRevoked checks whether the given certificate has been revoked.
// Returns (true, nil) if revoked, (false, nil) if not revoked or status
// unknown in soft-fail mode, or (false, error) if the status could not
// be established and the config enforces (AllowSoftFail is false).
//
// A "revoked" verdict from ANY consulted source always wins, in every
// mode. "Unknown" (stale CRL, unreachable OCSP responder) is governed by
// AllowSoftFail: fail closed when enforcing, accept when soft-fail.
func (c *Checker) IsRevoked(cert *x509.Certificate) (bool, error) {
	serial := cert.SerialNumber.Text(16)
	scope := c.scopeFor(cert)

	// Dispatch only. Each mode's policy lives in its own method: the bodies
	// differ in which sources they consult and in what a "not found" means
	// when nothing was consulted, and inlining all four put this function over
	// the cyclomatic limit without making any one policy easier to read.
	switch c.cfg.Mode {
	case ModeCRL:
		return c.isRevokedCRL(scope, serial)
	case ModeOCSP:
		return c.isRevokedOCSP(cert, scope, serial)
	case ModeBoth:
		return c.isRevokedBoth(cert, scope, serial)
	default: // ModeAny
		return c.isRevokedAny(cert, scope, serial)
	}
}

// noSourceBackstop reports the missing-source failure when enforcement is on
// and this checker holds no CRL path to consult.
//
// It exists because "serial not found" is only evidence of "not revoked" when
// something was actually consulted. checkCRLStatus cannot even report
// staleness without a path to go stale, so with no paths configured it returns
// a clean not-found that is indistinguishable from a real answer.
// Config.Validate rejects the combination up front; this is the runtime
// backstop for a checker constructed without it, or whose paths were lost in a
// refactor, so the gap surfaces as a handshake failure rather than admitting
// every revoked certificate in silence.
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

// isRevokedOCSP consults OCSP, plus the programmatic revoked set.
//
// The programmatic set (AddRevokedSerial, the runtime admin revocation path) is
// consulted in every mode; ignoring it here would make an operator-issued
// revocation a silent no-op whenever OCSP is the selected mode. CRL staleness
// is NOT consulted: this mode does not derive its verdict from CRL files.
func (c *Checker) isRevokedOCSP(cert *x509.Certificate, scope, serial string) (bool, error) {
	c.mu.RLock()
	revoked := c.revokedLocked(scope, serial)
	c.mu.RUnlock()
	if revoked {
		return true, nil
	}
	return c.checkOCSP(cert)
}

// isRevokedBoth consults CRL and OCSP, revoked-by-either.
//
// Requiring agreement of both sources would let a CRL-revoked certificate
// slide through on a stale OCSP "good". Unknown status from either source is
// per AllowSoftFail.
func (c *Checker) isRevokedBoth(cert *x509.Certificate, scope, serial string) (bool, error) {
	crlRevoked, crlErr := c.checkCRLStatus(scope, serial)
	if crlRevoked {
		return true, nil
	}
	ocspRevoked, ocspErr := c.checkOCSPRaw(cert)
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

// isRevokedAny consults whichever sources are configured.
//
// With a responder configured, unknown from either source fails closed under
// enforcement: a revocation could be hiding behind the stale CRL or the
// unreachable responder. With no responder it degenerates to the CRL policy,
// including the missing-source backstop.
func (c *Checker) isRevokedAny(cert *x509.Certificate, scope, serial string) (bool, error) {
	crlRevoked, crlErr := c.checkCRLStatus(scope, serial)
	if crlRevoked {
		return true, nil
	}
	if c.cfg.OCSPResponderURL == "" {
		return c.isRevokedCRLTail(crlErr)
	}

	ocspRevoked, ocspErr := c.checkOCSPRaw(cert)
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

// checkCRLStatus tests whether the serial is in the revoked set. The
// second return value is non-nil when a not-found result cannot be
// trusted as "not revoked": CRL paths are configured but no fresh,
// fully-successful load is in effect (initial load pending, or the most
// recent reload failed). A found (revoked) result is always trustworthy,
// even from a stale set. Enforcement policy is applied by the caller.
// The lookup consults the leaf's own issuer scope and the issuer-agnostic
// scope used by AddRevokedSerial.
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

// checkOCSP queries the OCSP responder for the certificate's revocation
// status, using the cache when available. When the OCSP check fails
// (unreachable, no URL, no issuer cert), behavior depends on AllowSoftFail:
// when unset (enforcing) it returns the error (fail closed, including on a
// negative-cache hit); under soft-fail it returns (false, nil).
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
		if c.cfg.enforcing() {
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

	// Resolve the issuer before touching the cache: the cache key is scoped to
	// it, because a serial identifies a certificate only together with its
	// issuer. Without an issuer no request can be built and no verdict can be
	// scoped, so this fails immediately rather than caching under a key that
	// would collide across intermediates.
	issuer := c.issuerFor(cert)
	if issuer == nil {
		return false, &OCSPUnavailableError{URL: c.cfg.OCSPResponderURL, Err: ErrNoIssuerCertificate}
	}
	key := scopedSerial(issuerScope(issuer), cert.SerialNumber.Text(16))

	// Check cache first (read lock for concurrent handshakes). A cached
	// unavailable entry short-circuits the query so a down responder does not
	// add its full timeout to every reconnect handshake during its negative
	// TTL window.
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

	// Cache the definitive result. The entry must never outlive the response
	// that produced it: a large OCSPCacheSeconds would otherwise serve a
	// "good" verdict past the responder's own NextUpdate, exactly the window
	// in which a revocation is published.
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

// queryOCSP sends an OCSP request to the configured responder and parses
// the response. issuerCert is the CA the caller selected for cert (also the
// cache scope), so request construction and response verification cannot
// diverge from the key the verdict is stored under. It returns whether the
// certificate is revoked plus the response's NextUpdate (zero when absent),
// which bounds the cache TTL. An RFC 6960 "unknown" status is returned as an
// error, never as "not revoked".
func (c *Checker) queryOCSP(cert, issuerCert *x509.Certificate) (bool, time.Time, error) {
	// Build the OCSP request using golang.org/x/crypto/ocsp.
	//
	// Nonce note: this x/crypto/ocsp version cannot attach a nonce
	// extension (RFC 8954) — CreateRequest's RequestOptions carries only a
	// Hash field and no extension support — so request/response binding via
	// nonce is not possible here. Replay protection instead relies on the
	// freshness validation below (validateOCSPFreshness): a captured "good"
	// response is only replayable within its ThisUpdate/NextUpdate window,
	// and anything outside that window is treated as status-unavailable.
	reqBytes, err := ocsp.CreateRequest(cert, issuerCert, nil)
	if err != nil {
		return false, time.Time{}, err
	}

	// Derive the timeout from the checker's shutdown context so in-flight
	// OCSP requests are cancelled promptly during transport shutdown instead
	// of blocking for the full timeout duration. Use the CONFIGURED timeout:
	// a hardcoded value here would silently cap (or extend past) the
	// operator's OCSPTimeoutSeconds.
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

	// Reject responses outside their validity window. Without this, a
	// replayed stale "good" response (captured before a revocation) would
	// defeat fail-closed revocation checking indefinitely. The error return
	// makes checkOCSPRaw treat the result as status-unavailable, never as
	// "good".
	if err := validateOCSPFreshness(ocspResp, time.Now()); err != nil {
		return false, time.Time{}, err
	}

	// RFC 6960 defines three statuses. "Unknown" means the responder has no
	// record of this certificate — it is NOT a "not revoked" verdict, and
	// must never be cached as one: doing so admits a leaf the responder does
	// not know and pins that admission for the full cache TTL. Returning an
	// error routes it through the short-TTL negative cache and fails closed
	// under enforcement.
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

// validateOCSPFreshness checks that an OCSP response's validity window
// covers now, with revocationClockSkew allowance on both edges. Per RFC
// 6960, an absent NextUpdate means newer information is available all the
// time; since that gives a replayed response no expiry, a bounded maximum
// age (ocspNoNextUpdateMaxAge) is applied instead. A stale or not-yet-valid
// response returns a typed error so callers treat the status as
// unavailable (fail-closed under enforcement), never as "good".
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

// validateOCSPResponder rejects a response signed by a delegated responder
// that is not authorized to sign OCSP responses (RFC 6960 §4.2.2.2). A
// response signed by the issuer itself is authorized whether or not it embeds
// the issuer's certificate — a CA answering OCSP directly commonly includes
// it, and ParseResponseForCert has verified the signature. Any other embedded
// certificate is a delegate, accepted only if it carries id-kp-OCSPSigning and
// is within its validity period: ParseResponseForCert checks nothing but the
// issuer's signature on it, so a node's own CA-issued certificate would
// otherwise let a revoked node vouch "good" for itself.
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

// OnCertRevoked returns the configured revocation-notification callback, or nil
// when none was set. It is the single field the TLS wiring needs from the
// config.
//
// This deliberately returns the callback rather than a *Config. The previous
// CfgRef handed out a mutable pointer to the checker's own config, so any
// holder could flip AllowSoftFail on a live checker while IsRevoked was reading
// it concurrently -- an unsynchronised write whose losing side is a silent
// downgrade from fail-closed to fail-open. Nothing needed that reach: the only
// caller wanted OnCertRevoked. Handing out the callback removes the write path
// instead of documenting it.
func (c *Checker) OnCertRevoked() func(string) {
	return c.cfg.OnCertRevoked
}
