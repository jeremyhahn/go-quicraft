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
	"context"
	"errors"
	"testing"
	"time"

	"github.com/jeremyhahn/go-quicraft/pkg/internal/raft"
)

// ---------------------------------------------------------------------------
// The membership-rejection sentinels pkg/errors.go re-exports
//
// pkg/internal/raft is unimportable from outside this module, so a downstream
// consumer can only match these rejections through the aliases in
// pkg/errors.go. The alias is only worth anything while it is the SAME error
// VALUE the raft package returns: a re-export that decays into
// errors.New("<the same text>") keeps compiling, keeps reading correctly, and
// silently stops matching every rejection the public API actually produces.
//
// Every assertion below is therefore paired with a NEGATIVE CONTROL — a
// locally declared error carrying byte-identical message text — which must NOT
// match. Without it these tests would pass against exactly the look-alike they
// exist to refuse.
// ---------------------------------------------------------------------------

// membershipAliases is the complete set of raft sentinels pkg/errors.go
// re-exports for the membership-change API. Adding a re-export without adding
// it here leaves it unguarded, which is the state all seven originals were in.
var membershipAliases = []struct {
	name     string
	exported error
	internal error
}{
	{"ErrNodeNotFound", ErrNodeNotFound, raft.ErrNodeNotFound},
	{"ErrNodeAlreadyExists", ErrNodeAlreadyExists, raft.ErrNodeAlreadyExists},
	{"ErrNodeRemoved", ErrNodeRemoved, raft.ErrNodeRemoved},
	{"ErrRemoveLastVoter", ErrRemoveLastVoter, raft.ErrRemoveLastVoter},
	{"ErrTooManyVotingMembers", ErrTooManyVotingMembers, raft.ErrTooManyVotingMembers},
	{"ErrZeroReplicaID", ErrZeroReplicaID, raft.ErrZeroReplicaID},
	{"ErrEmptyAddress", ErrEmptyAddress, raft.ErrEmptyAddress},
	{"ErrMemberIdentityRequired", ErrMemberIdentityRequired, raft.ErrMemberIdentityRequired},
	{"ErrRemovedMemberIdentity", ErrRemovedMemberIdentity, raft.ErrRemovedMemberIdentity},
	{"ErrDuplicateMemberAddress", ErrDuplicateMemberAddress, raft.ErrDuplicateMemberAddress},
	{"ErrDuplicateMemberIdentity", ErrDuplicateMemberIdentity, raft.ErrDuplicateMemberIdentity},
}

// TestMembershipAliasesAreTheRaftValues pins the IDENTITY of the
// re-exports against the raft sentinels they alias.
//
// This is the assertion that cannot be made from outside the module, and it is
// the one that fails the instant an alias is rewritten as an independent
// errors.New with the same wording. The look-alike built in each iteration is
// constructed FROM the sentinel's own text, so it tracks any future rewording
// and can never stop being a faithful impostor.
func TestMembershipAliasesAreTheRaftValues(t *testing.T) {
	for _, a := range membershipAliases {
		t.Run(a.name, func(t *testing.T) {
			if a.exported == nil || a.internal == nil {
				t.Fatalf("%s: nil sentinel (exported=%v internal=%v)",
					a.name, a.exported, a.internal)
			}
			if a.exported != a.internal {
				t.Fatalf("quicraft.%s is not the same error VALUE as raft.%s. "+
					"A downstream consumer matching the re-export with errors.Is "+
					"no longer matches the rejection the API returns.", a.name, a.name)
			}
		})
	}
}

// TestMembershipAliasesAreDistinct stops the table above from passing on
// names bound to one value. errors.Is(x, x) is true for any x, so a copy-paste
// that pointed two aliases at the same sentinel would satisfy every identity
// assertion while making the two rejections indistinguishable to a caller.
func TestMembershipAliasesAreDistinct(t *testing.T) {
	seen := make(map[error]string, len(membershipAliases))
	for _, a := range membershipAliases {
		if other, dup := seen[a.exported]; dup {
			t.Fatalf("%s and %s are the same error value; a caller cannot tell the "+
				"two rejections apart", a.name, other)
		}
		seen[a.exported] = a.name
	}
}

// ---------------------------------------------------------------------------
// End-to-end: the real public API, through the real wrapping chain
// ---------------------------------------------------------------------------

// aliasRejectionCase drives one membership rejection through the exported Host
// API and names the sentinel a caller must be able to match it with.
type aliasRejectionCase struct {
	name string
	// setup runs before call and may commit config changes through the public
	// API. It returns nothing: everything it needs is the host it is given.
	setup func(t *testing.T, ctx context.Context, h *Host)
	call  func(ctx context.Context, h *Host) error
	want  error
}

// TestMembershipRejectionsMatchTheExportedSentinels drives each rejection it
// can reach from a single-voter host through NewHost -> StartShard ->
// RequestAddNode / RequestRemoveNode and matches the result with the PUBLIC
// alias, never the internal sentinel.
//
// Three things make this non-vacuous:
//
//   - every case must produce a non-nil error, so a validator that stopped
//     refusing fails here rather than being reported as a match;
//   - the error must match the expected alias and NOT match any of the
//     others, so an alias pointed at the wrong sentinel fails;
//   - the error must not match a look-alike built from the expected
//     sentinel's own text, so the match cannot be coming from message equality.
func TestMembershipRejectionsMatchTheExportedSentinels(t *testing.T) {
	const localAddr = "127.0.0.1:5000"

	cases := []aliasRejectionCase{
		{
			name: "add with replica ID 0",
			call: func(ctx context.Context, h *Host) error {
				_, err := h.RequestAddNode(ctx, 1, 0, "127.0.0.1:6000", 0)
				return err
			},
			want: ErrZeroReplicaID,
		},
		{
			name: "remove with replica ID 0",
			call: func(ctx context.Context, h *Host) error {
				_, err := h.RequestRemoveNode(ctx, 1, 0, 0)
				return err
			},
			want: ErrZeroReplicaID,
		},
		{
			name: "add with no address",
			call: func(ctx context.Context, h *Host) error {
				_, err := h.RequestAddNode(ctx, 1, 10, "", 0)
				return err
			},
			want: ErrEmptyAddress,
		},
		{
			name: "add a replica that is already a voter",
			call: func(ctx context.Context, h *Host) error {
				_, err := h.RequestAddNode(ctx, 1, 1, localAddr, 0)
				return err
			},
			want: ErrNodeAlreadyExists,
		},
		{
			name: "remove a replica that is not a member",
			call: func(ctx context.Context, h *Host) error {
				_, err := h.RequestRemoveNode(ctx, 1, 99, 0)
				return err
			},
			want: ErrNodeNotFound,
		},
		{
			// The tombstone can only be laid by a COMMITTED removal, and a
			// single voter can commit one only for a replica that carries no
			// vote. An observer is exactly that: it is added and removed
			// through consensus without ever raising the quorum, so the whole
			// add/remove/re-add sequence runs on one host with no peer
			// reachable.
			name: "add a replica this shard removed",
			setup: func(t *testing.T, ctx context.Context, h *Host) {
				t.Helper()
				if err := h.SyncRequestAddObserver(ctx, 1, 10, "127.0.0.1:6000", 0); err != nil {
					t.Fatalf("SyncRequestAddObserver: %v", err)
				}
				added, err := h.SyncGetShardMembership(ctx, 1)
				if err != nil {
					t.Fatalf("SyncGetShardMembership: %v", err)
				}
				if err := h.SyncRequestRemoveNode(ctx, 1, 10, added.ConfigChangeID); err != nil {
					t.Fatalf("SyncRequestRemoveNode: %v", err)
				}
			},
			call: func(ctx context.Context, h *Host) error {
				_, err := h.RequestAddNode(ctx, 1, 10, "127.0.0.1:6000", 0)
				return err
			},
			want: ErrNodeRemoved,
		},
		{
			// The identity tombstone outlives the replica ID it was bound to:
			// a NEW replica ID presenting a removed member's keypair is the
			// case a host has to classify, and it is refused before it
			// can commit a member every replica would black-hole.
			name: "add a removed member's identity under a new replica ID",
			setup: func(t *testing.T, ctx context.Context, h *Host) {
				t.Helper()
				if err := h.SyncRequestAddObserverWithIdentity(ctx, 1, 10, "127.0.0.1:6000", testIdentity(10), 0); err != nil {
					t.Fatalf("SyncRequestAddObserverWithIdentity: %v", err)
				}
				added, err := h.SyncGetShardMembership(ctx, 1)
				if err != nil {
					t.Fatalf("SyncGetShardMembership: %v", err)
				}
				if err := h.SyncRequestRemoveNode(ctx, 1, 10, added.ConfigChangeID); err != nil {
					t.Fatalf("SyncRequestRemoveNode: %v", err)
				}
			},
			call: func(ctx context.Context, h *Host) error {
				_, err := h.RequestAddNodeWithIdentity(ctx, 1, 11, "127.0.0.1:6001", testIdentity(10), 0)
				return err
			},
			want: ErrRemovedMemberIdentity,
		},
		{
			// The local voter holds localAddr. A second replica ID at the same
			// address would give the one host behind it two votes.
			name: "add a new replica at an address an active member holds",
			call: func(ctx context.Context, h *Host) error {
				_, err := h.RequestAddNode(ctx, 1, 10, localAddr, 0)
				return err
			},
			want: ErrDuplicateMemberAddress,
		},
		{
			// One keypair bound to two active replica IDs authenticates as
			// both, so the second binding is refused while the first holder is
			// still a member.
			name: "add a new replica with an identity an active member holds",
			setup: func(t *testing.T, ctx context.Context, h *Host) {
				t.Helper()
				if err := h.SyncRequestAddObserverWithIdentity(ctx, 1, 10, "127.0.0.1:6000", testIdentity(10), 0); err != nil {
					t.Fatalf("SyncRequestAddObserverWithIdentity: %v", err)
				}
			},
			call: func(ctx context.Context, h *Host) error {
				_, err := h.RequestAddObserverWithIdentity(ctx, 1, 11, "127.0.0.1:6001", testIdentity(10), 0)
				return err
			},
			want: ErrDuplicateMemberIdentity,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			h := startSingleVoterShard(t)
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()

			if tc.setup != nil {
				tc.setup(t, ctx, h)
			}

			err := tc.call(ctx, h)
			if err == nil {
				t.Fatalf("the call succeeded; it must be refused with %v", tc.want)
			}
			if !errors.Is(err, tc.want) {
				t.Fatalf("errors.Is(err, want) = false: err = %v (%T), want %v. "+
					"An external consumer cannot match this rejection through the "+
					"re-export.", err, err, tc.want)
			}

			lookAlike := errors.New(tc.want.Error())
			if errors.Is(err, lookAlike) {
				t.Fatalf("err matched a locally declared error with the identical "+
					"text of %v; the match above is by message, not by identity", tc.want)
			}

			for _, a := range membershipAliases {
				if a.exported == tc.want {
					continue
				}
				if errors.Is(err, a.exported) {
					t.Fatalf("err also matched %s; the rejection is not "+
						"distinguishable from a different one", a.name)
				}
			}
		})
	}
}

// TestMembershipAliasesUnreachableFromASingleVoterHost records, as an
// executable statement rather than a comment, WHICH of the re-exports
// this package cannot drive through the public API, and why.
//
// ErrRemoveLastVoter fires only for a witness-only membership (no full
// replicas at all), and ErrTooManyVotingMembers only past 64 voting members.
// Both need a committed multi-member configuration, and a single voter can
// commit at most one voter-adding change before its own quorum grows beyond
// what it can satisfy alone. They are covered by identity above and by
// pkg/internal/raft's own membership tests; this test asserts the PRECONDITION
// that keeps them out of reach, so that if the validators are ever reworked to
// fire on a reachable membership, this fails and the end-to-end cases can be
// written.
func TestMembershipAliasesUnreachableFromASingleVoterHost(t *testing.T) {
	h := startSingleVoterShard(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	m, err := h.SyncGetShardMembership(ctx, 1)
	if err != nil {
		t.Fatalf("SyncGetShardMembership: %v", err)
	}
	if len(m.Nodes) != 1 {
		t.Fatalf("voters = %d, want 1", len(m.Nodes))
	}
	if len(m.Witnesses) != 0 {
		t.Fatalf("witnesses = %d, want 0", len(m.Witnesses))
	}

	// Removing the single voter is refused as the last FULL replica, not as
	// the last voting member: that ordering is what puts ErrRemoveLastVoter
	// out of reach here.
	if _, err := h.RequestRemoveNode(ctx, 1, 1, 0); !errors.Is(err, ErrRemoveLastFullReplica) {
		t.Fatalf("RequestRemoveNode(self) = %v, want ErrRemoveLastFullReplica; if "+
			"this now returns ErrRemoveLastVoter, add it to the end-to-end table", err)
	}
}
