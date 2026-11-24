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

// transportPairWithRevocation builds a connected pair with a soft-fail checker on the dialing transport.
func transportPairWithRevocation(t testing.TB) (*QUICTransport, *QUICTransport, func()) {
	t.Helper()
	t1, t2, cleanup := transportPairWith(t, 42, newTestHandler(), newTestHandler())
	t1.revocationChecker = revocation.NewChecker(revocation.Config{AllowSoftFail: true})
	return t1, t2, cleanup
}
