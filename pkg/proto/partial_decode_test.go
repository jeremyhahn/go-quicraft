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

package proto

import "testing"

// TestConfigChangeUnmarshalLeavesNoPartialState pins that a failed decode does
// not mutate the receiver.
//
// A partial decode that left the NEW ConfigChangeID, Type, ReplicaID and
// Address on the receiver beside the OLD NodeIdentity would produce a value
// describing no configuration any writer produced. NodeIdentity is what the
// receive-side gate authorizes the member by, so a stale one beside a new
// target is the same shape Membership.UnmarshalFrom was
// hardened against.
//
// Every truncation point is swept, so the guarantee holds wherever the payload
// stops rather than at the one boundary a single fixture would probe.
func TestConfigChangeUnmarshalLeavesNoPartialState(t *testing.T) {
	full := ConfigChange{
		ConfigChangeID: 1,
		Type:           AddNode,
		ReplicaID:      5,
		Address:        "h:1",
		Initialize:     true,
		NodeIdentity:   "spki:VICTIM",
	}
	buf := make([]byte, full.Size())
	n, err := full.MarshalTo(buf)
	if err != nil {
		t.Fatalf("MarshalTo: %v", err)
	}

	// The prior value the receiver holds. If any field of it survives beside a
	// field of `full`, the decode was not all-or-nothing.
	prior := ConfigChange{
		ConfigChangeID: 99,
		Type:           RemoveNode,
		ReplicaID:      7,
		Address:        "old:9",
		NodeIdentity:   "spki:STALE",
	}

	for cut := 0; cut < n; cut++ {
		got := prior
		if _, err := got.UnmarshalFrom(buf[:cut]); err == nil {
			// A prefix that legitimately decodes is not a truncation case.
			continue
		}
		if got != prior {
			t.Fatalf("a FAILED decode at cut=%d mutated the receiver:\n"+
				" got %+v\nwant %+v (unchanged).\nA stale NodeIdentity beside a "+
				"new ReplicaID authorizes a replica the decoded configuration "+
				"never named", cut, got, prior)
		}
	}

	// The control: a complete buffer still decodes, so the sweep above is not
	// passing because nothing ever succeeds.
	var ok ConfigChange
	if _, err := ok.UnmarshalFrom(buf[:n]); err != nil {
		t.Fatalf("the complete encoding failed to decode: %v", err)
	}
	if ok != full {
		t.Fatalf("round-trip mismatch:\n got %+v\nwant %+v", ok, full)
	}
}
