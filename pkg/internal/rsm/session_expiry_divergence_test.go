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
	"context"
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
			ExpiryEntries:        localExpiry,
			MaxTotalSessions:     100,
			MaxSessionsPerClient: 100,
		}
		rsm, err := NewStateMachine(1, 1, createFn, t.TempDir(), local,
			func() proto.SessionPolicy { return replicated }, false)
		if err != nil {
			t.Fatalf("NewStateMachine(local expiry=%d) = %v", localExpiry, err)
		}
		t.Cleanup(func() { _ = rsm.Close() })
		return rsm, kv
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
			if err := replica.Apply(entries, results); err != nil {
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
		// The shard committed expiry=0. The second replica is configured with
		// 100 — under the old node-local semantics it would have expired
		// client 42 at index 200 and answered the write at index 300 with
		// ResultSessionExpired while its peer applied it.
		replicated := proto.SessionPolicy{
			ExpiryEntries:        0,
			MaxTotalSessions:     100,
			MaxSessionsPerClient: 100,
		}
		a, akv := newReplica(t, 0, replicated)
		b, bkv := newReplica(t, 100, replicated)

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
		// write — including the one whose local configuration disables expiry.
		replicated := proto.SessionPolicy{
			ExpiryEntries:        100,
			MaxTotalSessions:     100,
			MaxSessionsPerClient: 100,
		}
		a, akv := newReplica(t, 0, replicated)
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
}

// TestSessionPolicy_LocalUsedOnlyWhileShardHasNone pins the fallback that keeps
// a shard bootstrapped before the policy was replicated working.
//
// A v3 membership carries no session policy, and a join=true replica has no
// membership at all until it replicates one. Both yield a policy whose IsSet
// reports false, and only then may local configuration drive apply. The second
// half is the part that matters: once a policy arrives, the state machine must
// switch to it WITHOUT a restart, because a joiner learns its shard's
// configuration after its state machine already exists.
func TestSessionPolicy_LocalUsedOnlyWhileShardHasNone(t *testing.T) {
	kv := newTestKV()
	createFn := sm.NewCreateFunc(func(shardID, replicaID uint64) sm.StateMachine {
		return kv
	})
	local := proto.SessionPolicy{
		ExpiryEntries:        700,
		MaxTotalSessions:     11,
		MaxSessionsPerClient: 7,
	}
	// The shard's policy, published later — as a joiner's peer publishes it
	// when the shard's initial membership replicates.
	var replicated proto.SessionPolicy
	rsm, err := NewStateMachine(1, 1, createFn, t.TempDir(), local,
		func() proto.SessionPolicy { return replicated }, false)
	if err != nil {
		t.Fatalf("NewStateMachine = %v", err)
	}
	defer rsm.Close()

	if rsm.sessionPolicy != local {
		t.Fatalf("policy with no shard value = %+v, want the local %+v",
			rsm.sessionPolicy, local)
	}
	if got := rsm.Sessions().MaxResponses(); got != local.MaxSessionsPerClient {
		t.Fatalf("session manager MaxResponses = %d, want the local %d",
			got, local.MaxSessionsPerClient)
	}

	replicated = proto.SessionPolicy{
		ExpiryEntries:        900,
		MaxTotalSessions:     22,
		MaxSessionsPerClient: 9,
	}
	// An ordinary apply is the adoption point: nothing else runs on a joiner
	// between learning its configuration and applying the entries that carried
	// it.
	results := make([]sm.Result, 1)
	if err := rsm.Apply([]proto.Entry{{Index: 1, Cmd: makeCmd("k", "v")}}, results); err != nil {
		t.Fatalf("Apply = %v", err)
	}

	if rsm.sessionPolicy != replicated {
		t.Fatalf("policy after the shard published one = %+v, want %+v; a replica "+
			"that keeps its local policy once the shard's is known applies on "+
			"limits no peer shares", rsm.sessionPolicy, replicated)
	}
	if got := rsm.Sessions().MaxResponses(); got != replicated.MaxSessionsPerClient {
		t.Fatalf("session manager MaxResponses = %d, want the replicated %d; the "+
			"limits inside the manager have to move with the policy or the "+
			"response cache stays bounded by local configuration",
			got, replicated.MaxSessionsPerClient)
	}
	// The registration bound moved too: with the replicated MaxTotalSessions of
	// 22 the twelfth session is admitted, where the local 11 would refuse it.
	for i := uint64(1); i <= 12; i++ {
		if err := rsm.Sessions().Register(i, 1); err != nil {
			t.Fatalf("Register #%d = %v; the replicated MaxTotalSessions of %d "+
				"must admit it, the local %d would not",
				i, err, replicated.MaxTotalSessions, local.MaxTotalSessions)
		}
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
		ExpiryEntries:        ^uint64(0),
		MaxTotalSessions:     100,
		MaxSessionsPerClient: 100,
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
	if err := rsm.Apply(far, make([]sm.Result, 1)); err != nil {
		t.Fatalf("Apply(far) = %v", err)
	}
	if !rsm.Sessions().IsRegistered(clientID) {
		t.Fatal("session expired under config.SessionExpiryDisabled")
	}
}
