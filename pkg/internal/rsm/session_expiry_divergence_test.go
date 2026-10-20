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

package rsm

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/jeremyhahn/go-quicraft/pkg/proto"
	"github.com/jeremyhahn/go-quicraft/pkg/sm"
)

// TestSessionExpiry_ReplicatedThresholdGovernsBothReplicas is the regression
// test for a silent state-machine divergence.
//
// It replaced a CHARACTERIZATION test that asserted the opposite: that two
// replicas fed the identical log, differing only in the expiry threshold each
// was configured with, ended up holding DIFFERENT user data at the same applied
// index — with the log identical, the commit index identical, and nothing in
// the protocol able to detect it. That was the cost of
// Config.SessionExpiryEntries being node-local configuration that the apply
// path treated as replicated.
//
// The threshold is now part of the shard's replicated membership
// (proto.SessionPolicy), so the two replicas below still differ in their LOCAL
// configuration — the disagreement a mixed-version cluster or an edited config
// file produces — and must nevertheless apply identically, because both read
// the value the shard committed.
//
// The two subtests cover both directions. It is not enough that the replicas
// agree: they must agree on the SHARD's behaviour, so each subtest also pins
// which behaviour that is. A fix that simply ignored expiry everywhere would
// pass the first subtest and fail the second.
func TestSessionExpiry_ReplicatedThresholdGovernsBothReplicas(t *testing.T) {
	const clientID = 42

	// newReplica builds a state machine whose LOCAL configuration carries
	// localExpiry and whose shard has committed replicated, and returns it
	// alongside the user KV so the test can read the applied state.
	newReplica := func(t *testing.T, localExpiry uint64, replicated proto.SessionPolicy) (*StateMachine, *testKV) {
		t.Helper()
		kv := newTestKV()
		createFn := sm.NewCreateFunc(func(shardID, replicaID uint64) sm.StateMachine {
			return kv
		})
		local := proto.SessionPolicy{
			ExpiryEntries:          localExpiry,
			MaxTotalSessions:       100,
			MaxResponsesPerSession: 100,
		}
		rsm, err := NewStateMachine(1, 1, createFn, t.TempDir(), local,
			func() proto.SessionPolicy { return replicated }, false)
		if err != nil {
			t.Fatalf("NewStateMachine(local expiry=%d) = %v", localExpiry, err)
		}
		t.Cleanup(func() { _ = rsm.Close() })
		return rsm, kv
	}

	// refused asserts that a replica whose LOCAL expiry disagrees with the
	// shard's committed policy is not built at all: it would expire sessions
	// at log indexes its peers do not.
	refused := func(t *testing.T, localExpiry uint64, replicated proto.SessionPolicy) {
		t.Helper()
		createFn := sm.NewCreateFunc(func(shardID, replicaID uint64) sm.StateMachine {
			return newTestKV()
		})
		local := proto.SessionPolicy{
			ExpiryEntries:          localExpiry,
			MaxTotalSessions:       100,
			MaxResponsesPerSession: 100,
		}
		rsm, err := NewStateMachine(1, 1, createFn, t.TempDir(), local,
			func() proto.SessionPolicy { return replicated }, false)
		if err == nil {
			_ = rsm.Close()
			t.Fatalf("NewStateMachine(local expiry=%d, shard expiry=%d) = nil error, "+
				"want ErrSessionPolicyMismatch", localExpiry, replicated.ExpiryEntries)
		}
		if !errors.Is(err, ErrSessionPolicyMismatch) {
			t.Fatalf("NewStateMachine(local expiry=%d) = %v, want ErrSessionPolicyMismatch",
				localExpiry, err)
		}
		if !strings.Contains(err.Error(), "SessionExpiryEntries") {
			t.Fatalf("error %q does not name the disagreeing setting", err)
		}
	}

	// The log both replicas are fed, in the identical order:
	//
	//	index 1   register client 42
	//	index 200 an unrelated command; 199 > 100, so a replica evaluating a
	//	          threshold of 100 reaches its checkpoint at index 200 and
	//	          expires client 42
	//	index 300 a session-managed write from client 42
	applyLog := func(t *testing.T, replica *StateMachine) {
		t.Helper()
		steps := [][]proto.Entry{
			{{
				Index:    1,
				ClientID: clientID,
				SeriesID: proto.SessionSeriesIDForRegister,
				Type:     proto.EntryNormal,
			}},
			{{Index: 200, Cmd: makeCmd("unrelated", "v")}},
			{{
				Index:    300,
				ClientID: clientID,
				SeriesID: 1,
				Cmd:      makeCmd("divergent", "written-by-client-42"),
			}},
		}
		for i, entries := range steps {
			results := make([]sm.Result, len(entries))
			if err := applyContiguous(t, replica, entries, results); err != nil {
				t.Fatalf("Apply step %d = %v", i, err)
			}
		}
	}

	// applied reads back the key the session-managed write targets.
	applied := func(t *testing.T, kv *testKV) string {
		t.Helper()
		got, err := kv.Lookup(context.Background(), "divergent")
		if err != nil {
			t.Fatalf("Lookup: %v", err)
		}
		val, _ := got.(string)
		return val
	}

	t.Run("shard disabled expiry, one replica configured to expire", func(t *testing.T) {
		// The shard committed expiry=0. A replica configured with 100 would
		// expire client 42 at index 200 and answer the write at index 300 with
		// ResultSessionExpired while its peer applied it, so it is refused;
		// the replicas that do run agree.
		replicated := proto.SessionPolicy{
			ExpiryEntries:          0,
			MaxTotalSessions:       100,
			MaxResponsesPerSession: 100,
		}
		refused(t, 100, replicated)
		a, akv := newReplica(t, 0, replicated)
		b, bkv := newReplica(t, 0, replicated)

		applyLog(t, a)
		applyLog(t, b)

		if got, want := a.lastApplied.Load(), uint64(300); got != want {
			t.Fatalf("replica A lastApplied = %d, want %d", got, want)
		}
		if got, want := b.lastApplied.Load(), uint64(300); got != want {
			t.Fatalf("replica B lastApplied = %d, want %d", got, want)
		}

		// The session table is the state the divergence starts in.
		if !a.Sessions().IsRegistered(clientID) || !b.Sessions().IsRegistered(clientID) {
			t.Fatalf("session held: A=%v B=%v; the shard committed expiry=0, so "+
				"NEITHER replica may expire client %d, whatever either is "+
				"configured with",
				a.Sessions().IsRegistered(clientID),
				b.Sessions().IsRegistered(clientID), clientID)
		}

		// The distinguishing observable: user state at the same applied index.
		const want = "written-by-client-42"
		gotA, gotB := applied(t, akv), applied(t, bkv)
		if gotA != want || gotB != want {
			t.Fatalf("applied value: A=%q B=%q, want both %q. A replica that reads "+
				"the threshold from its LOCAL config expires the session at "+
				"index 200 and rejects the write, which is the divergence "+
				"replicating the threshold closes", gotA, gotB, want)
		}
	})

	t.Run("shard enabled expiry, one replica configured not to", func(t *testing.T) {
		// The mirror image: the shard committed expiry=100, so BOTH replicas
		// must expire client 42 at the index-200 checkpoint and reject the
		// write, and one whose local configuration disables expiry is refused.
		replicated := proto.SessionPolicy{
			ExpiryEntries:          100,
			MaxTotalSessions:       100,
			MaxResponsesPerSession: 100,
		}
		refused(t, 0, replicated)
		a, akv := newReplica(t, 100, replicated)
		b, bkv := newReplica(t, 100, replicated)

		applyLog(t, a)
		applyLog(t, b)

		if a.Sessions().IsRegistered(clientID) || b.Sessions().IsRegistered(clientID) {
			t.Fatalf("session held: A=%v B=%v; the shard committed expiry=100, so "+
				"BOTH replicas must have expired client %d",
				a.Sessions().IsRegistered(clientID),
				b.Sessions().IsRegistered(clientID), clientID)
		}

		gotA, gotB := applied(t, akv), applied(t, bkv)
		if gotA != "" || gotB != "" {
			t.Fatalf("applied value: A=%q B=%q, want both empty. The shard's "+
				"policy expires the session before index 300, so the write must "+
				"be rejected on every replica", gotA, gotB)
		}
	})

	t.Run("local threshold differs in magnitude from the replicated one", func(t *testing.T) {
		// The decisive case, and the one the two subtests above cannot see.
		//
		// The first disables expiry on the shard (replicated 0), which takes
		// Apply's expiry-disabled fast path so the segmented checkpoint walk
		// is never entered at all. The second sets replica B's LOCAL threshold
		// to 100 — the same value the shard committed — so substituting local
		// for replicated inside that walk is a no-op.
		//
		// Injecting `if s.localPolicy.ExpiryEntries != 0 { interval =
		// s.localPolicy.ExpiryEntries }` into the walk therefore survived the
		// whole suite, and that substitution is the exact divergence this file
		// exists to prevent: two replicas evaluating different intervals expire
		// different sessions at different indexes, and one then answers
		// ResultSessionExpired for entries the other applies normally.
		//
		// Here the shard commits 100 and a replica configured with 1000 is
		// refused: under its local value no checkpoint falls before index 300,
		// so it would hold the session and APPLY the write its peers reject.
		// The replicas that run expire client 42 at the index-200 checkpoint.
		replicated := proto.SessionPolicy{
			ExpiryEntries:          100,
			MaxTotalSessions:       100,
			MaxResponsesPerSession: 100,
		}
		refused(t, 1000, replicated)
		a, akv := newReplica(t, 100, replicated)
		b, bkv := newReplica(t, 100, replicated)

		applyLog(t, a)
		applyLog(t, b)

		if a.Sessions().IsRegistered(clientID) || b.Sessions().IsRegistered(clientID) {
			t.Fatalf("session held: A=%v B=%v; the shard committed expiry=100, "+
				"so BOTH replicas must expire client %d",
				a.Sessions().IsRegistered(clientID),
				b.Sessions().IsRegistered(clientID), clientID)
		}

		gotA, gotB := applied(t, akv), applied(t, bkv)
		if gotA != "" || gotB != "" {
			t.Fatalf("applied value: A=%q B=%q, want both empty. A replica that "+
				"reads the checkpoint interval from its LOCAL config finds no "+
				"checkpoint before index 300, holds the session and applies a "+
				"write its peers reject", gotA, gotB)
		}
	})
}

// TestSessionPolicy_LocalUsedOnlyWhileShardHasNone pins the fallback that keeps
// a shard bootstrapped before the policy was replicated working.
//
// A v3 membership carries no session policy, and a join=true replica has no
// membership at all until it replicates one. Both yield a policy whose IsSet
// reports false, and only then may local configuration drive apply. The second
// half is the part that matters: once a policy arrives, the state machine must
// check it WITHOUT a restart, because a joiner learns its shard's
// configuration after its state machine already exists. A policy that agrees
// is applied on; one that disagrees in any setting fails the batch with
// ErrSessionPolicyMismatch, which fails the shard.
func TestSessionPolicy_LocalUsedOnlyWhileShardHasNone(t *testing.T) {
	local := proto.SessionPolicy{
		ExpiryEntries:          700,
		MaxTotalSessions:       11,
		MaxResponsesPerSession: 7,
		MaxSessionsPerOwner:    3,
	}
	// newJoiner builds a replica whose shard policy is published later — as a
	// joiner's peer publishes it when the shard's initial membership
	// replicates — and returns a setter for it.
	newJoiner := func(t *testing.T) (*StateMachine, *proto.SessionPolicy) {
		t.Helper()
		kv := newTestKV()
		createFn := sm.NewCreateFunc(func(shardID, replicaID uint64) sm.StateMachine {
			return kv
		})
		replicated := new(proto.SessionPolicy)
		rsm, err := NewStateMachine(1, 1, createFn, t.TempDir(), local,
			func() proto.SessionPolicy { return *replicated }, false)
		if err != nil {
			t.Fatalf("NewStateMachine = %v", err)
		}
		t.Cleanup(func() { _ = rsm.Close() })
		if rsm.sessionPolicy != local {
			t.Fatalf("policy with no shard value = %+v, want the local %+v",
				rsm.sessionPolicy, local)
		}
		return rsm, replicated
	}
	// An ordinary apply is the check point: nothing else runs on a joiner
	// between learning its configuration and applying the entries that
	// carried it.
	apply := func(rsm *StateMachine, index uint64) error {
		results := make([]sm.Result, 1)
		return rsm.Apply([]proto.Entry{{Index: index, Cmd: makeCmd("k", "v")}}, results)
	}

	t.Run("shard policy matches the local one", func(t *testing.T) {
		rsm, replicated := newJoiner(t)
		*replicated = local
		if err := apply(rsm, 1); err != nil {
			t.Fatalf("Apply = %v", err)
		}
		if rsm.sessionPolicy != local {
			t.Fatalf("policy = %+v, want %+v", rsm.sessionPolicy, local)
		}
		if got := rsm.Sessions().MaxResponses(); got != local.MaxResponsesPerSession {
			t.Fatalf("session manager MaxResponses = %d, want %d",
				got, local.MaxResponsesPerSession)
		}
	})

	// Each setting on its own: a disagreement in any one of the four is
	// refused, and the error names that setting and both values.
	cases := []struct {
		setting string
		mutate  func(*proto.SessionPolicy)
		want    string
	}{
		{"SessionExpiryEntries", func(p *proto.SessionPolicy) { p.ExpiryEntries = 900 },
			"SessionExpiryEntries is 700 in this node's Config, 900 in the shard's committed policy"},
		{"MaxTotalSessions", func(p *proto.SessionPolicy) { p.MaxTotalSessions = 22 },
			"MaxTotalSessions is 11 in this node's Config, 22 in the shard's committed policy"},
		{"MaxResponsesPerSession", func(p *proto.SessionPolicy) { p.MaxResponsesPerSession = 9 },
			"MaxResponsesPerSession is 7 in this node's Config, 9 in the shard's committed policy"},
		{"MaxSessionsPerOwner", func(p *proto.SessionPolicy) { p.MaxSessionsPerOwner = 0 },
			"MaxSessionsPerOwner is 3 in this node's Config, 0 in the shard's committed policy"},
	}
	for _, tc := range cases {
		t.Run("shard policy disagrees on "+tc.setting, func(t *testing.T) {
			rsm, replicated := newJoiner(t)
			committed := local
			tc.mutate(&committed)
			*replicated = committed
			err := apply(rsm, 1)
			if !errors.Is(err, ErrSessionPolicyMismatch) {
				t.Fatalf("Apply = %v, want ErrSessionPolicyMismatch; a replica that "+
					"applies once the shard's policy is known on limits no peer "+
					"shares diverges silently", err)
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("error %q does not contain %q", err, tc.want)
			}
			if got := rsm.LastApplied(); got != 0 {
				t.Fatalf("LastApplied = %d after a refused batch, want 0", got)
			}
			// The limits inside the manager stay the local ones: nothing
			// from the refused policy is adopted.
			if got := rsm.Sessions().MaxResponses(); got != local.MaxResponsesPerSession {
				t.Fatalf("session manager MaxResponses = %d, want the local %d",
					got, local.MaxResponsesPerSession)
			}
		})
	}
}

// TestSessionPolicy_SnapshotInstallChecksThePolicy pins the other point at
// which a joiner first learns its shard's session policy: an InstallSnapshot,
// whose membership publishes the policy before the session table is read. A
// joiner configured with the shard's values recovers; one that disagrees is
// refused before its state machine is touched.
func TestSessionPolicy_SnapshotInstallChecksThePolicy(t *testing.T) {
	committed := proto.SessionPolicy{
		ExpiryEntries:          500,
		MaxTotalSessions:       10,
		MaxResponsesPerSession: 4,
	}
	createFn := sm.NewCreateFunc(func(shardID, replicaID uint64) sm.StateMachine {
		return newTestKV()
	})
	src, err := NewStateMachine(1, 1, createFn, t.TempDir(), committed,
		func() proto.SessionPolicy { return committed }, false)
	if err != nil {
		t.Fatalf("NewStateMachine(source) = %v", err)
	}
	defer src.Close()
	results := make([]sm.Result, 1)
	if err := src.Apply([]proto.Entry{{Index: 1, Cmd: makeCmd("k", "v")}}, results); err != nil {
		t.Fatalf("Apply(source) = %v", err)
	}
	var snap bytes.Buffer
	if err := src.SaveSnapshot(context.Background(), &snap, nil); err != nil {
		t.Fatalf("SaveSnapshot = %v", err)
	}

	recoverInto := func(t *testing.T, local proto.SessionPolicy) error {
		t.Helper()
		var replicated proto.SessionPolicy
		joiner, err := NewStateMachine(1, 2, createFn, t.TempDir(), local,
			func() proto.SessionPolicy { return replicated }, false)
		if err != nil {
			t.Fatalf("NewStateMachine(joiner) = %v", err)
		}
		t.Cleanup(func() { _ = joiner.Close() })
		// The installed snapshot's membership is published before recovery.
		replicated = committed
		return joiner.RecoverFromSnapshot(context.Background(),
			bytes.NewReader(snap.Bytes()), nil)
	}

	if err := recoverInto(t, committed); err != nil {
		t.Fatalf("RecoverFromSnapshot with matching settings = %v", err)
	}
	disagreeing := committed
	disagreeing.MaxResponsesPerSession = 8
	err = recoverInto(t, disagreeing)
	if !errors.Is(err, ErrSessionPolicyMismatch) {
		t.Fatalf("RecoverFromSnapshot with MaxResponsesPerSession=8 against a shard "+
			"that committed 4 = %v, want ErrSessionPolicyMismatch", err)
	}
	if !strings.Contains(err.Error(), "MaxResponsesPerSession is 8 in this node's Config, 4") {
		t.Fatalf("error %q does not name the setting and both values", err)
	}
}

// TestSessionExpiry_DisabledSentinelAppliesEverything pins the claim
// config.SessionExpiryDisabled rests on: that a threshold no log can reach
// needs no special case in the apply path.
//
// Zero can no longer mean "disabled" — it is the default's absence marker — so
// disabling is expressed as math.MaxUint64. That value takes Apply's SEGMENTED
// path rather than its expiry-disabled fast path, and the segmented path is the
// one with a loop that must find no checkpoint and a trailing flush that must
// still apply every entry. A regression here does not expire anything early; it
// silently drops entries.
func TestSessionExpiry_DisabledSentinelAppliesEverything(t *testing.T) {
	const clientID = 7
	kv := newTestKV()
	createFn := sm.NewCreateFunc(func(shardID, replicaID uint64) sm.StateMachine {
		return kv
	})
	// math.MaxUint64 is config.SessionExpiryDisabled; rsm does not import
	// config, so the value is spelled out.
	policy := proto.SessionPolicy{
		ExpiryEntries:          ^uint64(0),
		MaxTotalSessions:       100,
		MaxResponsesPerSession: 100,
	}
	rsm, err := NewStateMachine(1, 1, createFn, t.TempDir(), policy, nil, false)
	if err != nil {
		t.Fatalf("NewStateMachine = %v", err)
	}
	defer rsm.Close()

	entries := []proto.Entry{
		{Index: 1, ClientID: clientID, SeriesID: proto.SessionSeriesIDForRegister,
			Type: proto.EntryNormal},
		{Index: 2, Cmd: makeCmd("a", "1")},
		{Index: 3, Cmd: makeCmd("b", "2")},
	}
	results := make([]sm.Result, len(entries))
	if err := rsm.Apply(entries, results); err != nil {
		t.Fatalf("Apply = %v", err)
	}
	if got, want := rsm.lastApplied.Load(), uint64(3); got != want {
		t.Fatalf("lastApplied = %d, want %d", got, want)
	}
	for _, key := range []string{"a", "b"} {
		got, err := kv.Lookup(context.Background(), key)
		if err != nil {
			t.Fatalf("Lookup(%q): %v", key, err)
		}
		if val, _ := got.(string); val == "" {
			t.Fatalf("entry for key %q was not applied; the segmented path must "+
				"still flush every entry when no checkpoint falls in the batch", key)
		}
	}

	// A far-future index must not expire the session either: the threshold is
	// unreachable by construction, not merely large.
	far := []proto.Entry{{Index: 1 << 40, Cmd: makeCmd("c", "3")}}
	if err := applyContiguous(t, rsm, far, make([]sm.Result, 1)); err != nil {
		t.Fatalf("Apply(far) = %v", err)
	}
	if !rsm.Sessions().IsRegistered(clientID) {
		t.Fatal("session expired under config.SessionExpiryDisabled")
	}
}
