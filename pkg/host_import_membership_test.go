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
	"testing"

	"github.com/jeremyhahn/go-quicraft/pkg/proto"
)

// artifactMembership is a five-voter configuration standing in for what a
// snapshot artifact carries, with policy fields set so the tests can prove
// policy survives a membership rewrite.
func artifactMembership() proto.Membership {
	m := proto.Membership{
		ConfigChangeID: 77,
		MaxWitnesses:   2,
		Addresses: map[uint64]string{
			1: "10.0.0.1:9000",
			2: "10.0.0.2:9000",
			3: "10.0.0.3:9000",
			4: "10.0.0.4:9000",
			5: "10.0.0.5:9000",
		},
		Observers: map[uint64]string{9: "10.0.0.9:9000"},
	}
	m.SetSessionPolicy(proto.SessionPolicy{
		MaxTotalSessions:       4096,
		MaxResponsesPerSession: 32,
	})
	return m
}

// applyImportOptions materialises options the way ImportSnapshot does.
func applyImportOptions(opts ...ImportOption) importOptions {
	var iopts importOptions
	for _, opt := range opts {
		opt(&iopts)
	}
	return iopts
}

// TestResolveImportMembership_DefaultsToTheArtifact pins that an import without
// the override is unchanged: the overwhelmingly common case must not be able to
// drift into rewriting membership.
func TestResolveImportMembership_DefaultsToTheArtifact(t *testing.T) {
	artifact := artifactMembership()
	got, err := resolveImportMembership(5, 3, artifact, applyImportOptions())
	if err != nil {
		t.Fatalf("resolveImportMembership: %v", err)
	}
	if len(got.Addresses) != 5 {
		t.Fatalf("membership was rewritten without the option: %d voters, want 5",
			len(got.Addresses))
	}
	if len(got.Observers) != 1 {
		t.Errorf("observers were dropped without the option: %+v", got.Observers)
	}
}

// TestResolveImportMembership_ReconstitutesAfterQuorumLoss is the disaster the
// option exists for: a five-voter shard has lost three nodes permanently, so no
// configuration change can ever commit and the shard cannot repair itself. The
// two survivors import with a membership naming only themselves.
func TestResolveImportMembership_ReconstitutesAfterQuorumLoss(t *testing.T) {
	artifact := artifactMembership()
	survivors := map[uint64]string{
		3: "10.0.0.3:9000",
		4: "10.0.0.4:9000",
	}

	got, err := resolveImportMembership(5, 3, artifact,
		applyImportOptions(WithImportMembership(survivors)))
	if err != nil {
		t.Fatalf("resolveImportMembership: %v", err)
	}

	if len(got.Addresses) != 2 {
		t.Fatalf("recovered membership has %d voters, want the 2 survivors: %+v",
			len(got.Addresses), got.Addresses)
	}
	for _, id := range []uint64{3, 4} {
		if got.Addresses[id] != survivors[id] {
			t.Errorf("survivor %d: address %q, want %q",
				id, got.Addresses[id], survivors[id])
		}
	}
	for _, id := range []uint64{1, 2, 5} {
		if _, ok := got.Addresses[id]; ok {
			t.Errorf("dead replica %d survived the rewrite; the shard restarts "+
				"into a configuration whose quorum it still cannot form", id)
		}
	}

	// Observers named by the artifact are dropped: a recovery configuration
	// should name exactly the replicas the operator verified.
	if len(got.Observers) != 0 {
		t.Errorf("observers survived the rewrite: %+v", got.Observers)
	}

	// Policy is not membership and must survive.
	sp := got.SessionPolicy()
	if sp.MaxTotalSessions != 4096 || sp.MaxResponsesPerSession != 32 {
		t.Errorf("session policy lost in the rewrite: %+v", sp)
	}
	if got.MaxWitnesses != 2 {
		t.Errorf("witness policy lost in the rewrite: MaxWitnesses=%d, want 2",
			got.MaxWitnesses)
	}
	if got.ConfigChangeID != artifact.ConfigChangeID {
		t.Errorf("ConfigChangeID = %d, want the artifact's %d; a lower value "+
			"orders the recovered configuration BEFORE the one it replaces",
			got.ConfigChangeID, artifact.ConfigChangeID)
	}
}

// TestResolveImportMembership_RefusesUnusableOverrides covers every way an
// operator can get the map wrong. Each of these is a plausible typo in a
// disaster-recovery runbook, and none is detectable after the fact: the shard
// simply comes up missing writes it once acknowledged, or unable to participate
// in itself.
func TestResolveImportMembership_RefusesUnusableOverrides(t *testing.T) {
	tests := []struct {
		name    string
		members map[uint64]string
		why     string
	}{
		{
			name:    "empty",
			members: map[uint64]string{},
			why:     "a shard with no voting members can never elect",
		},
		{
			name:    "omits the importing replica",
			members: map[uint64]string{4: "10.0.0.4:9000"},
			why: "the replica restores the data and is then not a member of " +
				"the shard it just restored",
		},
		{
			name:    "replica ID zero",
			members: map[uint64]string{3: "10.0.0.3:9000", 0: "10.0.0.9:9000"},
			why:     "0 is not a valid replica ID",
		},
		{
			name:    "empty address",
			members: map[uint64]string{3: "10.0.0.3:9000", 4: ""},
			why:     "the survivors cannot reach each other",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := resolveImportMembership(5, 3, artifactMembership(),
				applyImportOptions(WithImportMembership(tt.members)))
			if err == nil {
				t.Fatalf("accepted an unusable membership (%s): %s",
					tt.name, tt.why)
			}
			if !errors.Is(err, ErrImportMembershipInvalid) {
				t.Fatalf("error %v does not match ErrImportMembershipInvalid, "+
					"so a caller cannot distinguish it from an I/O failure", err)
			}
		})
	}
}

// TestResolveImportMembership_RefusesOversizedMembership pins the voter ceiling.
// Exceeding it panics the shard on every election, so an import must not be a
// way to construct a configuration StartShard would have refused.
func TestResolveImportMembership_RefusesOversizedMembership(t *testing.T) {
	members := make(map[uint64]string, 65)
	for i := uint64(1); i <= 65; i++ {
		members[i] = "10.0.0.1:9000"
	}
	members[3] = "10.0.0.3:9000" // include the importing replica

	_, err := resolveImportMembership(5, 3, artifactMembership(),
		applyImportOptions(WithImportMembership(members)))
	if err == nil {
		t.Fatal("accepted a membership above the voting-member ceiling; the " +
			"shard will panic on its first election")
	}
	if !errors.Is(err, ErrImportMembershipInvalid) {
		t.Fatalf("error %v does not match ErrImportMembershipInvalid", err)
	}
}

// TestWithImportMembership_CopiesTheCallersMap pins that the option does not
// retain the caller's map. The value reaches a persisted snapshot record, so a
// caller mutating its map afterwards would otherwise change what the shard
// restarts into — and the mutation would happen long after the call that
// appeared to define the configuration.
func TestWithImportMembership_CopiesTheCallersMap(t *testing.T) {
	caller := map[uint64]string{3: "10.0.0.3:9000", 4: "10.0.0.4:9000"}
	iopts := applyImportOptions(WithImportMembership(caller))

	caller[5] = "10.0.0.5:9000"
	delete(caller, 4)

	if len(iopts.membership) != 2 {
		t.Fatalf("the option aliased the caller's map: %+v", iopts.membership)
	}
	if _, ok := iopts.membership[5]; ok {
		t.Error("a replica added to the caller's map after the call reached " +
			"the import")
	}
	if _, ok := iopts.membership[4]; !ok {
		t.Error("a replica deleted from the caller's map after the call " +
			"vanished from the import")
	}
}
