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
	"crypto/x509"
	"sync"
	"testing"
)

// TestSetIssuers_ReplacesIssuerSet verifies that SetIssuers swaps the whole
// candidate set a CA-bundle reload hands the checker: a leaf of the newly
// trusted CA finds its issuer, and a leaf of the dropped CA no longer does.
func TestSetIssuers_ReplacesIssuerSet(t *testing.T) {
	oldCA := newTestCA(t)
	newCA := newTestCA(t)
	oldLeaf := issueLeafWithAKI(t, oldCA, 10, oldCA.cert.SubjectKeyId)
	newLeaf := issueLeafWithAKI(t, newCA, 11, newCA.cert.SubjectKeyId)

	c := NewChecker(Config{}, oldCA.cert)
	if got := c.issuerFor(oldLeaf); got != oldCA.cert {
		t.Fatalf("issuerFor(old leaf) before reload = %v, want the old CA", got)
	}
	if got := c.issuerFor(newLeaf); got != nil {
		t.Fatalf("issuerFor(new leaf) before reload = %v, want nil", got)
	}

	c.SetIssuers(nil, newCA.cert)
	if got := c.issuerFor(newLeaf); got != newCA.cert {
		t.Fatalf("issuerFor(new leaf) after reload = %v, want the new CA", got)
	}
	if got := c.issuerFor(oldLeaf); got != nil {
		t.Fatalf("issuerFor(old leaf) after reload = %v, want nil", got)
	}
	if n := len(c.issuerSet()); n != 1 {
		t.Fatalf("issuerSet has %d entries, want 1 (nil entries dropped)", n)
	}
}

// TestSetIssuers_RetainsDroppedIssuerForEstablishedOnly pins the split a CA
// reload relies on: the dropped CA stays resolvable for an established leaf,
// never for a handshake, and is released once no live leaf needs it.
func TestSetIssuers_RetainsDroppedIssuerForEstablishedOnly(t *testing.T) {
	oldCA := newTestCA(t)
	newCA := newTestCA(t)
	oldLeaf := issueLeafWithAKI(t, oldCA, 20, oldCA.cert.SubjectKeyId)
	newLeaf := issueLeafWithAKI(t, newCA, 21, newCA.cert.SubjectKeyId)

	c := NewChecker(Config{Mode: ModeCRL}, oldCA.cert)
	c.AddRevokedSerial(oldLeaf.SerialNumber.Text(16))
	c.mu.Lock()
	c.revokedSet[scopedSerial(issuerScope(oldCA.cert), "ff")] = struct{}{}
	c.mu.Unlock()
	scopedLeaf := issueLeafWithAKI(t, oldCA, 0xff, oldCA.cert.SubjectKeyId)

	c.SetIssuers(newCA.cert)
	if got := c.retainedSet(); len(got) != 1 || !got[0].Equal(oldCA.cert) {
		t.Fatalf("retained after the drop = %v, want the old CA alone", got)
	}
	if revoked, err := c.IsRevokedEstablished(scopedLeaf); !revoked || err != nil {
		t.Fatalf("IsRevokedEstablished(issuer-scoped old leaf) = %v, %v; want revoked", revoked, err)
	}
	if revoked, _ := c.IsRevoked(scopedLeaf); revoked {
		t.Fatal("IsRevoked resolved the retained issuer: the handshake path must see the trusted set only")
	}

	// Re-adding the CA moves it back to the trusted set, not into both.
	c.SetIssuers(newCA.cert, oldCA.cert)
	if n := len(c.retainedSet()); n != 0 {
		t.Fatalf("retained after the CA came back = %d entries, want 0", n)
	}
	c.SetIssuers(newCA.cert)

	c.PruneRetainedIssuers([]*x509.Certificate{newLeaf, nil, oldLeaf})
	if n := len(c.retainedSet()); n != 1 {
		t.Fatalf("retained with an old-CA session live = %d entries, want 1", n)
	}
	c.PruneRetainedIssuers([]*x509.Certificate{newLeaf})
	if n := len(c.retainedSet()); n != 0 {
		t.Fatalf("retained after the last old-CA session closed = %d entries, want 0", n)
	}
	if revoked, _ := c.IsRevokedEstablished(scopedLeaf); revoked {
		t.Fatal("a released issuer still scoped a lookup")
	}
	c.PruneRetainedIssuers(nil) // nothing retained: a no-op
}

// TestSetIssuers_ConcurrentWithLookups runs reloads against issuer lookups;
// under -race it proves the swap is a single atomic store.
func TestSetIssuers_ConcurrentWithLookups(t *testing.T) {
	a := newTestCA(t)
	b := newTestCA(t)
	leaf := issueLeafWithAKI(t, a, 12, a.cert.SubjectKeyId)
	c := NewChecker(Config{}, a.cert)

	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		for i := 0; i < 200; i++ {
			if i%2 == 0 {
				c.SetIssuers(a.cert, b.cert)
			} else {
				c.SetIssuers(a.cert)
			}
		}
	}()
	go func() {
		defer wg.Done()
		for i := 0; i < 200; i++ {
			if c.issuerFor(leaf) != a.cert {
				t.Error("issuerFor lost the issuer present in every set")
				return
			}
		}
	}()
	wg.Wait()
}
