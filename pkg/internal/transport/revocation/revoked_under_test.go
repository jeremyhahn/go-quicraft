package revocation

import (
	"crypto/x509"
	"errors"
	"testing"
)

// revokedUnderSerial is the serial CA A's CRL revokes; CA B issues a leaf
// with the same serial that its own (empty) CRL does not revoke.
const revokedUnderSerial = 0x30

// revokedUnderFixture is a CRL-mode checker trusting CA A and CA B, with A's
// CRL revoking revokedUnderSerial and B's CRL revoking nothing.
type revokedUnderFixture struct {
	caA, caB     *testCA
	leafA, leafB *x509.Certificate
	crlA         string
	checker      *Checker
}

func newRevokedUnderFixture(t *testing.T) *revokedUnderFixture {
	t.Helper()
	f := &revokedUnderFixture{caA: newTestCA(t), caB: newTestCA(t)}
	f.leafA = issueLeafWithAKI(t, f.caA, revokedUnderSerial, f.caA.cert.SubjectKeyId)
	f.leafB = issueLeafWithAKI(t, f.caB, revokedUnderSerial, f.caB.cert.SubjectKeyId)
	dir := t.TempDir()
	f.crlA = writeCRLFile(t, dir, "a.crl", f.caA.createCRL(t, revokedUnderSerial))
	crlB := writeCRLFile(t, dir, "b.crl", f.caB.createCRL(t))
	f.checker = NewChecker(Config{Mode: ModeCRL, CRLPaths: []string{f.crlA, crlB}}, f.caA.cert, f.caB.cert)
	if err := f.checker.ReloadCRLs(); err != nil {
		t.Fatalf("ReloadCRLs: %v", err)
	}
	return f
}

func assertRevokedUnder(t *testing.T, c *Checker, leaf *x509.Certificate, issuers []*x509.Certificate, want bool, what string) {
	t.Helper()
	revoked, err := c.IsRevokedUnder(leaf, issuers)
	if err != nil {
		t.Fatalf("%s: IsRevokedUnder error: %v", what, err)
	}
	if revoked != want {
		t.Fatalf("%s: IsRevokedUnder = %v, want %v", what, revoked, want)
	}
}

func TestIsRevokedUnder_ResolvesIssuerInGivenBundle(t *testing.T) {
	f := newRevokedUnderFixture(t)
	assertRevokedUnder(t, f.checker, f.leafA, []*x509.Certificate{f.caA.cert}, true, "CA A leaf under [A]")
	assertRevokedUnder(t, f.checker, f.leafA, []*x509.Certificate{f.caB.cert, f.caA.cert}, true, "CA A leaf under [B, A]")
}

func TestIsRevokedUnder_ScopeFollowsIssuerNotSerial(t *testing.T) {
	f := newRevokedUnderFixture(t)
	assertRevokedUnder(t, f.checker, f.leafB, []*x509.Certificate{f.caA.cert, f.caB.cert}, false, "CA B leaf with A's revoked serial under [A, B]")
}

func TestIsRevokedUnder_NoIssuerInBundleAppliesIssuerAgnosticOnly(t *testing.T) {
	f := newRevokedUnderFixture(t)
	adminLeaf := issueLeafWithAKI(t, f.caA, 0x31, f.caA.cert.SubjectKeyId)
	f.checker.AddRevokedSerial(adminLeaf.SerialNumber.Text(16))
	for _, tc := range []struct {
		name    string
		issuers []*x509.Certificate
	}{
		{"[B]", []*x509.Certificate{f.caB.cert}},
		{"nil", nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			assertRevokedUnder(t, f.checker, adminLeaf, tc.issuers, true, "AddRevokedSerial leaf under "+tc.name)
			// Under enforcement an A-scoped entry cannot be consulted without
			// A, so the verdict is unknown rather than a clean "not revoked".
			revoked, err := f.checker.IsRevokedUnder(f.leafA, tc.issuers)
			if revoked || !errors.Is(err, ErrNoIssuerCertificate) {
				t.Fatalf("A-scoped leaf under %s: IsRevokedUnder = (%v, %v), want (false, ErrNoIssuerCertificate)", tc.name, revoked, err)
			}
		})
	}
}

func TestIsRevokedUnder_SurvivesCADrop(t *testing.T) {
	f := newRevokedUnderFixture(t)
	f.checker.SetIssuers(f.caB.cert)
	assertRevokedUnder(t, f.checker, f.leafA, []*x509.Certificate{f.caA.cert}, true, "CA A leaf under [A] after SetIssuers(B)")
	revoked, err := f.checker.IsRevoked(f.leafA)
	if revoked {
		t.Fatal("IsRevoked resolved the dropped CA A: the current set holds only B")
	}
	if !errors.Is(err, ErrNoIssuerCertificate) {
		t.Fatalf("IsRevoked after SetIssuers(B): err = %v, want one wrapping ErrNoIssuerCertificate", err)
	}
}

func TestIsRevokedUnder_FollowsCRLReload(t *testing.T) {
	f := newRevokedUnderFixture(t)
	writeCRLFile(t, "", f.crlA, f.caA.createCRL(t))
	if err := f.checker.ReloadCRLs(); err != nil {
		t.Fatalf("ReloadCRLs with the serial delisted: %v", err)
	}
	assertRevokedUnder(t, f.checker, f.leafA, []*x509.Certificate{f.caA.cert}, false, "CA A leaf under [A] after the CRL delisted it")
}
