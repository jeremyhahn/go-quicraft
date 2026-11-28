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

package writemode

import "testing"

// optInModes is every Mode that acknowledges a write before a quorum has
// persisted it. Each one gives up durability on purpose and must be chosen by
// name; docs/durable-by-default/README.md says what each gives up (#1303).
var optInModes = map[Mode]bool{
	WALDurable: true,
}

// optInAliases is every configuration string that parses to an optInModes
// member. The empty string is deliberately absent: an unset mode is the
// default and must stay quorum-durable.
var optInAliases = map[string]bool{
	"wal_durable": true,
	"wal":         true,
	"async":       true,
}

// TestDefaultMode_IsQuorumDurable pins durable-by-default (#1303): the zero
// Mode, and the Mode an unset configuration string parses to, acknowledge a
// write only after a quorum has committed it.
func TestDefaultMode_IsQuorumDurable(t *testing.T) {
	var zero Mode
	if !zero.RequiresQuorum() || !zero.IsSynchronous() || zero.IsAsync() {
		t.Fatalf("zero Mode %v is not quorum-durable: the default write mode "+
			"must acknowledge only after quorum commit", zero)
	}
	parsed, err := FromString("")
	if err != nil {
		t.Fatalf(`FromString("") error: %v`, err)
	}
	if !parsed.RequiresQuorum() {
		t.Fatalf(`FromString("") = %v, which does not require a quorum: an `+
			"unset write mode must default to a quorum-durable mode", parsed)
	}
}

// TestNonQuorumModes_AreOptIn fails when a Mode that skips the quorum is added
// without being listed in optInModes, or when a configuration string reaches
// such a Mode without being listed in optInAliases, so weakening durability
// is always an explicit, documented choice.
func TestNonQuorumModes_AreOptIn(t *testing.T) {
	for m := range modeSpecs {
		if got := !m.RequiresQuorum(); got != optInModes[m] {
			t.Errorf("Mode %v: skips quorum = %v, listed opt-in = %v; list every "+
				"non-quorum mode in optInModes and document it in "+
				"docs/durable-by-default/README.md", m, got, optInModes[m])
		}
	}
	for m := range optInModes {
		if _, ok := modeSpecs[m]; !ok {
			t.Errorf("optInModes lists Mode %d, which modeSpecs does not describe", m)
		}
	}
	for alias, m := range parseModes {
		if got := !m.RequiresQuorum(); got != optInAliases[alias] {
			t.Errorf("alias %q -> %v: skips quorum = %v, listed opt-in = %v",
				alias, m, got, optInAliases[alias])
		}
	}
	for alias := range optInAliases {
		if _, ok := parseModes[alias]; !ok {
			t.Errorf("optInAliases lists %q, which FromString does not accept", alias)
		}
	}
}
