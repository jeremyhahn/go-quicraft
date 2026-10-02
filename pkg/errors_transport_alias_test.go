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

package quicraft

import (
	"errors"
	"fmt"
	"testing"

	"github.com/jeremyhahn/go-quicraft/pkg/internal/transport"
)

// TestSharedPeerCertificateAliasIsTheTransportValue pins the re-export of the
// certificate-exclusivity rejection StartShard and the membership-add API
// return. pkg/internal/transport is unimportable from outside this module, so
// the alias is a caller's only way to match it; it is worth something only
// while it is the SAME value, which a look-alike with identical text must not
// satisfy.
func TestSharedPeerCertificateAliasIsTheTransportValue(t *testing.T) {
	if ErrSharedPeerCertificate != transport.ErrSharedPeerCertificate {
		t.Fatal("quicraft.ErrSharedPeerCertificate is not the same error VALUE as " +
			"transport.ErrSharedPeerCertificate; a caller matching the re-export " +
			"no longer matches the rejection the API returns")
	}
	lookAlike := errors.New(ErrSharedPeerCertificate.Error())
	if errors.Is(lookAlike, ErrSharedPeerCertificate) {
		t.Fatal("errors.Is matched a look-alike with identical text; identity is " +
			"not being tested, message equality is")
	}
}

// TestSharedPeerCertificateErrorAliasMatchesThroughWrapping proves the public
// type is the transport type, not a redeclaration: errors.As into the public
// type recovers the fields from the error the transport builds, through a
// wrapping layer, and the typed error still matches the public sentinel.
func TestSharedPeerCertificateErrorAliasMatchesThroughWrapping(t *testing.T) {
	var built error = &transport.SharedPeerCertificateError{
		SelfAddress:  "node-a.raft.test:5000",
		CoveredPeers: []string{"node-b.raft.test"},
	}
	wrapped := fmt.Errorf("start shard 1: %w", built)

	var typed *SharedPeerCertificateError
	if !errors.As(wrapped, &typed) {
		t.Fatalf("errors.As(%v, *SharedPeerCertificateError) = false", wrapped)
	}
	if typed.SelfAddress != "node-a.raft.test:5000" ||
		len(typed.CoveredPeers) != 1 || typed.CoveredPeers[0] != "node-b.raft.test" {
		t.Fatalf("fields lost through the alias: %+v", typed)
	}
	if !errors.Is(wrapped, ErrSharedPeerCertificate) {
		t.Fatalf("errors.Is(%v, ErrSharedPeerCertificate) = false", wrapped)
	}
}
