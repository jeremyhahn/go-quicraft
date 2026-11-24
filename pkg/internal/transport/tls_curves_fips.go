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

//go:build fips

package transport

import "crypto/tls"

// handshakeCurves for a fips build: SecP256r1MLKEM768 (RFC 10024), then P-256 and P-384; X25519 is compiled out because it is not FIPS 140-3 approved.
func handshakeCurves() []tls.CurveID {
	return []tls.CurveID{tls.SecP256r1MLKEM768, tls.CurveP256, tls.CurveP384}
}
