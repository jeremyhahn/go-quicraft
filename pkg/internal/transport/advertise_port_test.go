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

import "testing"

// TestResolveAdvertisedPort covers the port-0 rewrite Start applies to the
// advertise address: only port 0 is replaced, by the bound port, and the
// advertised host is kept (#386).
func TestResolveAdvertisedPort(t *testing.T) {
	tests := []struct {
		name      string
		advertise string
		bound     string
		want      string
	}{
		{"loopback port 0", "127.0.0.1:0", "127.0.0.1:41234", "127.0.0.1:41234"},
		{"named host keeps its host", "node-a.example:0", "0.0.0.0:41234", "node-a.example:41234"},
		{"ipv6 port 0", "[::1]:0", "[::1]:41234", "[::1]:41234"},
		{"fixed port unchanged", "127.0.0.1:4001", "127.0.0.1:41234", "127.0.0.1:4001"},
		{"empty unchanged", "", "127.0.0.1:41234", ""},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			tr := &QUICTransport{cfg: Config{AdvertiseAddress: tc.advertise}, localAddrStr: tc.bound}
			tr.resolveAdvertisedPort()
			if got := tr.cfg.AdvertiseAddress; got != tc.want {
				t.Fatalf("AdvertiseAddress = %q, want %q", got, tc.want)
			}
			wantAdv := tc.want
			if wantAdv == "" {
				wantAdv = tc.bound
			}
			if got := tr.AdvertisedAddr(); got != wantAdv {
				t.Fatalf("AdvertisedAddr() = %q, want %q", got, wantAdv)
			}
		})
	}
}
