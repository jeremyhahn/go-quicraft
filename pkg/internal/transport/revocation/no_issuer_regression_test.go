package revocation

import (
	"crypto/x509"
	"errors"
	"path/filepath"
	"testing"
)

// noIssuerFixture holds a root, an intermediate under it, a leaf issued by
// the intermediate and listed in the intermediate's CRL, and a second leaf
// from the same intermediate that no CRL lists.
type noIssuerFixture struct {
	root, intermediate *testCA
	revokedLeaf        *x509.Certificate
	cleanLeaf          *x509.Certificate
	crlPath            string
}

const (
	noIssuerRevokedSerial = 0x1258
	noIssuerCleanSerial   = 0x1259
)

func newNoIssuerFixture(t *testing.T) *noIssuerFixture {
	t.Helper()
	f := &noIssuerFixture{root: newTestCA(t)}
	f.intermediate = newTestIntermediate(t, f.root)
	f.revokedLeaf = f.intermediate.issueLeaf(t, noIssuerRevokedSerial)
	f.cleanLeaf = f.intermediate.issueLeaf(t, noIssuerCleanSerial)
	f.crlPath = writeCRLFile(t, t.TempDir(), "inter.crl.pem", f.intermediate.createCRL(t, noIssuerRevokedSerial))
	return f
}

// checker builds a checker that trusts both CAs, so the intermediate's CRL
// loads and its entries are scoped to the intermediate.
func (f *noIssuerFixture) checker(t *testing.T, mode Mode, softFail bool) *Checker {
	t.Helper()
	c := NewChecker(Config{
		Mode:          mode,
		CRLPaths:      []string{f.crlPath},
		AllowSoftFail: softFail,
	}, f.root.cert, f.intermediate.cert)
	if err := c.ReloadCRLs(); err != nil {
		t.Fatalf("ReloadCRLs: %v", err)
	}
	return c
}

var noIssuerCRLModes = []Mode{ModeCRL, ModeBoth, ModeAny}

// An enforcing check whose bundle cannot resolve the leaf's issuer must not
// report a clean "not revoked": it reports that the issuer is unknown.
func TestIsRevokedUnder_UnresolvedIssuerEnforcingFailsClosed(t *testing.T) {
	f := newNoIssuerFixture(t)
	rootOnly := []*x509.Certificate{f.root.cert}
	for _, mode := range noIssuerCRLModes {
		t.Run(string(mode), func(t *testing.T) {
			c := f.checker(t, mode, false)
			for _, leaf := range []*x509.Certificate{f.revokedLeaf, f.cleanLeaf} {
				revoked, err := c.IsRevokedUnder(leaf, rootOnly)
				if revoked {
					t.Fatalf("serial %s: revoked = true, want false with an error", leaf.SerialNumber.Text(16))
				}
				if err == nil {
					t.Fatalf("serial %s: err = nil, want an error", leaf.SerialNumber.Text(16))
				}
				// ModeBoth without a responder already fails on its OCSP
				// leg; that error stands. The other modes reach a clean
				// "not found" and report the missing issuer.
				if mode != ModeBoth && !errors.Is(err, ErrNoIssuerCertificate) {
					t.Fatalf("serial %s: err = %v, want one wrapping ErrNoIssuerCertificate", leaf.SerialNumber.Text(16), err)
				}
			}
		})
	}
}

// A stale or never-loaded CRL source keeps its own error when the issuer is
// also unresolved: the source failure is reported, not the missing issuer.
func TestIsRevokedUnder_UnresolvedIssuerStaleSourceReportsStale(t *testing.T) {
	f := newNoIssuerFixture(t)
	rootOnly := []*x509.Certificate{f.root.cert}
	missing := filepath.Join(t.TempDir(), "absent.crl.pem")
	for _, mode := range noIssuerCRLModes {
		t.Run(string(mode), func(t *testing.T) {
			c := NewChecker(Config{
				Mode:     mode,
				CRLPaths: []string{missing},
			}, f.root.cert, f.intermediate.cert)
			revoked, err := c.IsRevokedUnder(f.cleanLeaf, rootOnly)
			if revoked {
				t.Fatal("revoked = true, want false with an error")
			}
			if !errors.Is(err, ErrCRLStale) {
				t.Fatalf("err = %v, want one wrapping ErrCRLStale", err)
			}
			if errors.Is(err, ErrNoIssuerCertificate) {
				t.Fatalf("err = %v, must not report ErrNoIssuerCertificate for a stale source", err)
			}
		})
	}
}

// Soft-fail keeps the issuer-agnostic lookup: an unresolved issuer is
// accepted without an error.
func TestIsRevokedUnder_UnresolvedIssuerSoftFailUnchanged(t *testing.T) {
	f := newNoIssuerFixture(t)
	rootOnly := []*x509.Certificate{f.root.cert}
	for _, mode := range noIssuerCRLModes {
		t.Run(string(mode), func(t *testing.T) {
			c := f.checker(t, mode, true)
			revoked, err := c.IsRevokedUnder(f.revokedLeaf, rootOnly)
			if err != nil || revoked {
				t.Fatalf("IsRevokedUnder = (%v, %v), want (false, nil)", revoked, err)
			}
		})
	}
}

// An issuer-agnostic or admin entry for the serial is a definite answer even
// when the issuer is unresolved.
func TestIsRevokedUnder_UnresolvedIssuerAgnosticEntryRevokes(t *testing.T) {
	f := newNoIssuerFixture(t)
	rootOnly := []*x509.Certificate{f.root.cert}
	for _, mode := range noIssuerCRLModes {
		t.Run(string(mode), func(t *testing.T) {
			c := f.checker(t, mode, false)
			c.AddRevokedSerial(f.cleanLeaf.SerialNumber.Text(16))
			revoked, err := c.IsRevokedUnder(f.cleanLeaf, rootOnly)
			if err != nil || !revoked {
				t.Fatalf("IsRevokedUnder = (%v, %v), want (true, nil)", revoked, err)
			}
		})
	}
}

// With the issuer in the bundle, the issuer-scoped verdicts are unchanged.
func TestIsRevokedUnder_ResolvedIssuerUnchanged(t *testing.T) {
	f := newNoIssuerFixture(t)
	full := []*x509.Certificate{f.root.cert, f.intermediate.cert}
	c := f.checker(t, ModeCRL, false)
	assertRevokedUnder(t, c, f.revokedLeaf, full, true, "listed leaf under the full bundle")
	assertRevokedUnder(t, c, f.cleanLeaf, full, false, "unlisted leaf under the full bundle")
}
