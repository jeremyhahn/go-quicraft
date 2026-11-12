package quicraft

import (
	"encoding/json"
	"reflect"
	"testing"

	"github.com/jeremyhahn/go-quicraft/pkg/internal/raft"
)

// Host.replicaProgress fills ReplicaProgress.Role with a direct conversion
// from the raft Role string, so each ReplicaRole constant must equal its raft
// counterpart exactly or the conversion reports a value outside the set.
func TestReplicaRoleEqualsRaftRole(t *testing.T) {
	cases := []struct {
		public ReplicaRole
		raft   raft.Role
	}{
		{ReplicaRoleVoter, raft.RoleVoter},
		{ReplicaRoleObserver, raft.RoleObserver},
		{ReplicaRoleWitness, raft.RoleWitness},
	}
	for _, c := range cases {
		if string(c.public) != string(c.raft) {
			t.Errorf("ReplicaRole %q != raft.Role %q", c.public, c.raft)
		}
		if got := ReplicaRole(c.raft); got != c.public {
			t.Errorf("ReplicaRole(raft %q) = %q, want %q", c.raft, got, c.public)
		}
	}
}

// The documented set is exactly voter, observer and witness, all distinct.
func TestReplicaRoleValuesDistinct(t *testing.T) {
	want := map[ReplicaRole]string{
		ReplicaRoleVoter:    "voter",
		ReplicaRoleObserver: "observer",
		ReplicaRoleWitness:  "witness",
	}
	if len(want) != 3 {
		t.Fatalf("ReplicaRole constants are not distinct: %d unique of 3", len(want))
	}
	for role, s := range want {
		if string(role) != s {
			t.Errorf("ReplicaRole = %q, want %q", role, s)
		}
	}
}

// ReplicaProgress marshals its role as the plain string and unmarshals it
// back to an equal value.
func TestReplicaRoleProgressJSONRoundTrip(t *testing.T) {
	for _, role := range []ReplicaRole{ReplicaRoleVoter, ReplicaRoleObserver, ReplicaRoleWitness} {
		in := ReplicaProgress{ReplicaID: 7, Role: role, Match: 42}
		b, err := json.Marshal(in)
		if err != nil {
			t.Fatalf("marshal %q: %v", role, err)
		}
		var obj map[string]any
		if err := json.Unmarshal(b, &obj); err != nil {
			t.Fatalf("unmarshal %q into map: %v", role, err)
		}
		if got, ok := obj["role"].(string); !ok || got != string(role) {
			t.Errorf("role field = %#v, want %q (json %s)", obj["role"], role, b)
		}
		var out ReplicaProgress
		if err := json.Unmarshal(b, &out); err != nil {
			t.Fatalf("unmarshal %q: %v", role, err)
		}
		if !reflect.DeepEqual(in, out) {
			t.Errorf("round trip: got %+v, want %+v", out, in)
		}
	}
}
