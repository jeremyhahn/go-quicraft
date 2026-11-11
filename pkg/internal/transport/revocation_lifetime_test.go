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
	"testing"

	"github.com/jeremyhahn/go-quicraft/pkg/internal/transport/revocation"
)

// transportPairWithRevocation builds a connected pair and attaches a checker
// to the DIALING transport.
//
// The checker is attached directly rather than through Config.RevocationConfig
// because that path requires a CRL file or an OCSP responder, and neither is
// needed here: the unit under test is revokeCheckConnections, and
// AddRevokedSerial is the checker's own admin entry point. Going through the
// config would test config validation, not connection eviction.
//
// The checker opts into soft-fail for the same reason. revokeCheckConnections
// treats a checker ERROR as grounds to close, so an enforcing checker with no
// source would tear the connection down for the missing source and the test
// would pass without the revocation having done anything.
func transportPairWithRevocation(t testing.TB) (*QUICTransport, *QUICTransport, func()) {
	t.Helper()
	t1, t2, cleanup := transportPairWith(t, 42, newTestHandler(), newTestHandler())
	t1.revocationChecker = revocation.NewChecker(revocation.Config{AllowSoftFail: true})
	return t1, t2, cleanup
}
