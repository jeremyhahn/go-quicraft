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

package transport

import (
	"errors"
	"strings"
	"testing"
)

// TestPeerAuthAuthorizesFailsClosed pins the helper every handler routes the
// address binding through: a missing callback authorizes nothing, and a
// present one is consulted with exactly the address asked about.
func TestPeerAuthAuthorizesFailsClosed(t *testing.T) {
	if (PeerAuth{}).Authorizes("10.0.0.1:5000") {
		t.Fatal("a PeerAuth with no callback authorized an address; it must fail closed")
	}
	var asked string
	p := PeerAuth{AuthorizesAddress: func(a string) bool {
		asked = a
		return a == "10.0.0.1:5000"
	}}
	if !p.Authorizes("10.0.0.1:5000") || asked != "10.0.0.1:5000" {
		t.Fatalf("Authorizes did not pass the address through (asked %q)", asked)
	}
	if p.Authorizes("10.0.0.2:5000") {
		t.Fatal("Authorizes returned true for an address the callback refused")
	}
}

// TestSharedCertificateErrorsNameTheOverlap pins both halves of the per-node
// SAN precondition's errors: each matches its own sentinel and only that one,
// and each message names what overlapped so an operator can fix it.
func TestSharedCertificateErrorsNameTheOverlap(t *testing.T) {
	shared := &SharedPeerCertificateError{SelfAddress: "node-a:5000", CoveredPeers: []string{"node-b", "node-c"}}
	if !errors.Is(shared, ErrSharedPeerCertificate) || errors.Is(shared, ErrPromiscuousPeerCertificate) {
		t.Fatalf("SharedPeerCertificateError sentinel matching is wrong: %v", shared)
	}
	for _, want := range []string{"node-a:5000", "node-b", "node-c"} {
		if !strings.Contains(shared.Error(), want) {
			t.Errorf("SharedPeerCertificateError message %q does not name %q", shared.Error(), want)
		}
	}

	promiscuous := &PromiscuousPeerCertificateError{PeerAddress: "10.0.0.9:4000", SelfHost: "node-a"}
	if !errors.Is(promiscuous, ErrPromiscuousPeerCertificate) || errors.Is(promiscuous, ErrSharedPeerCertificate) {
		t.Fatalf("PromiscuousPeerCertificateError sentinel matching is wrong: %v", promiscuous)
	}
	for _, want := range []string{"10.0.0.9:4000", "node-a"} {
		if !strings.Contains(promiscuous.Error(), want) {
			t.Errorf("PromiscuousPeerCertificateError message %q does not name %q", promiscuous.Error(), want)
		}
	}
}

// TestNoConnectionKeysAndIdentity covers the documented no-connection
// answers: the fairness key is empty (and so unaccounted) and a transport
// without a leaf reports no identity rather than inventing one.
func TestNoConnectionKeysAndIdentity(t *testing.T) {
	if k := snapshotFairnessKey(nil); k != "" {
		t.Fatalf("snapshotFairnessKey(nil) = %q, want \"\"", k)
	}
	if id := (&QUICTransport{}).LocalNodeIdentity(); id != "" {
		t.Fatalf("LocalNodeIdentity with no leaf = %q, want \"\"", id)
	}
}
