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

//go:build integration

package integration

import (
	"os"
	"regexp"
	"strconv"
	"strings"
	"testing"
)

// clusterBindHost is the address test cluster nodes actually listen on. kernelAddr
// is "<clusterBindHost>:0", so the kernel may hand a node ANY port in the
// host's ephemeral range.
const clusterBindHost = "127.0.0.1"

// literalAddr matches a hard-coded IPv4 loopback address in this package's
// source.
var literalAddr = regexp.MustCompile(`"(127(?:\.[0-9]{1,3}){3}):([0-9]+)"`)

// TestDummyAddressesCannotCollideWithClusterNodes pins the invariant that a
// "dummy" address — one handed to RequestAddReplica/AddWitness/AddNonVoting for
// a member that never comes up — can never equal the address of a real node.
//
// It failed to hold: every dummy address was 127.0.0.1:599xx, and the ephemeral
// range on the runners is 32768-60999, so the kernel could hand a live node the
// very port a later assertion used as its stand-in. The engine then correctly
// refused with "address already held by another member of this shard" and the
// test reported a defect that did not exist (CI run 10783, job 44138).
//
// The fix is a different loopback HOST, not a higher port: ip_local_port_range
// is tunable per host, so "high enough" is a guess that a differently-configured
// box invalidates. A distinct host can never collide at any port.
//
// This asserts the property over the package's own source rather than over the
// five addresses that were wrong, so adding a sixth reintroduces the failure
// here instead of in a soak run months later.
func TestDummyAddressesCannotCollideWithClusterNodes(t *testing.T) {
	entries, err := os.ReadDir(".")
	if err != nil {
		t.Fatalf("reading package dir: %v", err)
	}

	checked := 0
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".go") {
			continue
		}
		src, err := os.ReadFile(e.Name())
		if err != nil {
			t.Fatalf("reading %s: %v", e.Name(), err)
		}
		for _, m := range literalAddr.FindAllStringSubmatch(string(src), -1) {
			host, rawPort := m[1], m[2]
			port, err := strconv.Atoi(rawPort)
			if err != nil {
				continue
			}
			// ":0" is kernelAddr asking the kernel to choose; that is the
			// mechanism under test, not a violation of it.
			if port == 0 {
				continue
			}
			checked++
			if host == clusterBindHost {
				t.Errorf("%s: hard-coded %s:%d shares a host with the cluster's "+
					"bind address, so the kernel may assign a live node that exact "+
					"port and the address collides; use a different loopback host",
					e.Name(), host, port)
			}
		}
	}

	if checked == 0 {
		t.Fatal("found no hard-coded addresses to check — the scan is not looking at the right files")
	}
	t.Logf("checked %d hard-coded addresses across the package", checked)
}
