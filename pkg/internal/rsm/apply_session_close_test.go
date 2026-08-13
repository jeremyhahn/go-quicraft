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
	"testing"

	"github.com/jeremyhahn/go-quicraft/pkg/proto"
	"github.com/jeremyhahn/go-quicraft/pkg/sm"
)

// Apply must never fail on legal committed log content: a failed Apply marks
// the shard permanently failed on EVERY replica (the log is identical), which
// turns ordinary client API usage into a cluster-wide outage. The tests below
// cover batches whose in-log session lifecycle removes a session that entries
// in the same batch were admitted against.

// TestApply_SessionEntryAndCloseInSameBatch is the reproduction of the
// client-triggerable shard kill: ProposeWithSession immediately followed by
// CloseSession drains both proposals into one Propose, one CommittedEntries
// batch and one Apply. The close removes the session before the post-Update
// RecordResult pass reaches the earlier entry.
func TestApply_SessionEntryAndCloseInSameBatch(t *testing.T) {
	kv := newTestKV()
	rsm, err := NewStateMachine(1, 1, func(_, _ uint64) interface{} {
		return kv
	}, t.TempDir(), testPolicy(0, 0, 0), nil, false)
	if err != nil {
		t.Fatalf("NewStateMachine = %v", err)
	}
	defer rsm.Close()

	if err := rsm.Sessions().Register(10, 0); err != nil {
		t.Fatalf("Register = %v", err)
	}

	entries := []proto.Entry{
		{Index: 1, ClientID: 10, SeriesID: 1, Type: proto.EntryNormal, Cmd: makeCmd("k", "v")},
		{Index: 2, ClientID: 10, SeriesID: proto.SessionSeriesIDForUnregister, Type: proto.EntryNormal},
	}
	results := make([]sm.Result, len(entries))

	if err := rsm.Apply(entries, results); err != nil {
		t.Fatalf("Apply = %v, want nil (shard must survive legal log content)", err)
	}

	// The entry that preceded the close in the log must still have been applied.
	if results[0].Value != 1 {
		t.Errorf("results[0].Value = %d, want 1 (entry applied to user SM)", results[0].Value)
	}
	if results[0].Value == sm.ResultSessionExpired {
		t.Errorf("results[0] wrongly rejected as SESSION_EXPIRED")
	}
	got, err := rsm.Lookup(t.Context(), "k")
	if err != nil {
		t.Fatalf("Lookup = %v", err)
	}
	if got != "v" {
		t.Errorf("kv[k] = %q, want %q", got, "v")
	}

	// The close result and the session removal are unchanged.
	if results[1].Value != 10 {
		t.Errorf("results[1].Value = %d, want 10 (clientID)", results[1].Value)
	}
	if rsm.Sessions().IsRegistered(10) {
		t.Error("session 10 must be unregistered after the close entry")
	}
}

// TestApply_CloseBeforeSessionEntryInSameBatch pins the in-log ordering
// semantics of the opposite arrangement: an entry that follows the close in
// the log operates on a session that no longer exists and must be reported as
// SESSION_EXPIRED without ever reaching the user SM.
func TestApply_CloseBeforeSessionEntryInSameBatch(t *testing.T) {
	cs := newCountingSM()
	rsm, err := NewStateMachine(1, 1, func(_, _ uint64) interface{} {
		return cs
	}, t.TempDir(), testPolicy(0, 0, 0), nil, false)
	if err != nil {
		t.Fatalf("NewStateMachine = %v", err)
	}
	defer rsm.Close()

	if err := rsm.Sessions().Register(10, 0); err != nil {
		t.Fatalf("Register = %v", err)
	}

	entries := []proto.Entry{
		{Index: 1, ClientID: 10, SeriesID: proto.SessionSeriesIDForUnregister, Type: proto.EntryNormal},
		{Index: 2, ClientID: 10, SeriesID: 1, Type: proto.EntryNormal, Cmd: makeCmd("k", "v")},
	}
	results := make([]sm.Result, len(entries))

	if err := rsm.Apply(entries, results); err != nil {
		t.Fatalf("Apply = %v, want nil", err)
	}
	if results[0].Value != 10 {
		t.Errorf("results[0].Value = %d, want 10 (clientID)", results[0].Value)
	}
	if results[1].Value != sm.ResultSessionExpired {
		t.Errorf("results[1].Value = %d, want SESSION_EXPIRED (%d)",
			results[1].Value, sm.ResultSessionExpired)
	}
	if cs.total != 0 {
		t.Errorf("user SM applied %d entries, want 0 (session already closed in log order)", cs.total)
	}
}

// TestApply_CloseOfDifferentClientInSameBatch verifies the close of one
// client's session leaves another client's entry in the same batch fully
// intact, including its cached response.
func TestApply_CloseOfDifferentClientInSameBatch(t *testing.T) {
	cs := newCountingSM()
	rsm, err := NewStateMachine(1, 1, func(_, _ uint64) interface{} {
		return cs
	}, t.TempDir(), testPolicy(0, 0, 0), nil, false)
	if err != nil {
		t.Fatalf("NewStateMachine = %v", err)
	}
	defer rsm.Close()

	for _, id := range []uint64{10, 20} {
		if err := rsm.Sessions().Register(id, 0); err != nil {
			t.Fatalf("Register(%d) = %v", id, err)
		}
	}

	entries := []proto.Entry{
		{Index: 1, ClientID: 20, SeriesID: 1, Type: proto.EntryNormal, Cmd: makeCmd("a", "1")},
		{Index: 2, ClientID: 10, SeriesID: proto.SessionSeriesIDForUnregister, Type: proto.EntryNormal},
	}
	results := make([]sm.Result, len(entries))
	if err := rsm.Apply(entries, results); err != nil {
		t.Fatalf("Apply = %v, want nil", err)
	}
	if cs.total != 1 {
		t.Errorf("user SM applied %d entries, want 1", cs.total)
	}
	if !rsm.Sessions().IsRegistered(20) {
		t.Fatal("session 20 must survive the close of session 10")
	}

	// Client 20's response must be cached: a replay of the same series
	// deduplicates instead of re-applying.
	replay := []proto.Entry{
		{Index: 3, ClientID: 20, SeriesID: 1, Type: proto.EntryNormal, Cmd: makeCmd("a", "1")},
	}
	replayResults := make([]sm.Result, 1)
	if err := rsm.Apply(replay, replayResults); err != nil {
		t.Fatalf("Apply replay = %v", err)
	}
	if cs.total != 1 {
		t.Errorf("user SM applied %d entries after replay, want 1 (cached response reused)", cs.total)
	}
	if replayResults[0].Value != results[0].Value {
		t.Errorf("replay result = %d, want cached %d", replayResults[0].Value, results[0].Value)
	}
	if !bytes.Equal(replayResults[0].Data, results[0].Data) {
		t.Errorf("replay data = %q, want cached %q", replayResults[0].Data, results[0].Data)
	}
}

// TestApply_OneSessionClosedOneOpenInSameBatch mixes a closed and a surviving
// session in one batch: both entries apply exactly once, only the surviving
// session retains a cached response.
func TestApply_OneSessionClosedOneOpenInSameBatch(t *testing.T) {
	cs := newCountingSM()
	rsm, err := NewStateMachine(1, 1, func(_, _ uint64) interface{} {
		return cs
	}, t.TempDir(), testPolicy(0, 0, 0), nil, false)
	if err != nil {
		t.Fatalf("NewStateMachine = %v", err)
	}
	defer rsm.Close()

	for _, id := range []uint64{10, 20} {
		if err := rsm.Sessions().Register(id, 0); err != nil {
			t.Fatalf("Register(%d) = %v", id, err)
		}
	}

	entries := []proto.Entry{
		{Index: 1, ClientID: 10, SeriesID: 1, Type: proto.EntryNormal, Cmd: makeCmd("a", "1")},
		{Index: 2, ClientID: 20, SeriesID: 1, Type: proto.EntryNormal, Cmd: makeCmd("b", "2")},
		{Index: 3, ClientID: 10, SeriesID: proto.SessionSeriesIDForUnregister, Type: proto.EntryNormal},
	}
	results := make([]sm.Result, len(entries))
	if err := rsm.Apply(entries, results); err != nil {
		t.Fatalf("Apply = %v, want nil", err)
	}
	if cs.total != 2 {
		t.Errorf("user SM applied %d entries, want 2", cs.total)
	}
	if results[0].Value == sm.ResultSessionExpired || results[1].Value == sm.ResultSessionExpired {
		t.Errorf("results wrongly rejected: [0]=%d [1]=%d", results[0].Value, results[1].Value)
	}
	if rsm.Sessions().IsRegistered(10) {
		t.Error("session 10 must be closed")
	}
	if !rsm.Sessions().IsRegistered(20) {
		t.Error("session 20 must remain open")
	}

	// The surviving session keeps its cached response; a replay dedups.
	replay := []proto.Entry{
		{Index: 4, ClientID: 20, SeriesID: 1, Type: proto.EntryNormal, Cmd: makeCmd("b", "2")},
	}
	replayResults := make([]sm.Result, 1)
	if err := rsm.Apply(replay, replayResults); err != nil {
		t.Fatalf("Apply replay = %v", err)
	}
	if cs.total != 2 {
		t.Errorf("user SM applied %d entries after replay, want 2 (cached response reused)", cs.total)
	}
	if replayResults[0].Value != results[1].Value {
		t.Errorf("replay result = %d, want cached %d", replayResults[0].Value, results[1].Value)
	}
}

// TestApply_CloseThenReRegisterSameClientInSameBatch covers a batch that
// closes a client's session and re-registers the SAME clientID after an entry
// was already admitted against the original session. The earlier entry's
// response must NOT be cached into the replacement session: doing so would
// deduplicate a future proposal of that series which was never applied to the
// replacement session's state.
func TestApply_CloseThenReRegisterSameClientInSameBatch(t *testing.T) {
	cs := newCountingSM()
	rsm, err := NewStateMachine(1, 1, func(_, _ uint64) interface{} {
		return cs
	}, t.TempDir(), testPolicy(0, 0, 0), nil, false)
	if err != nil {
		t.Fatalf("NewStateMachine = %v", err)
	}
	defer rsm.Close()

	if err := rsm.Sessions().Register(10, 0); err != nil {
		t.Fatalf("Register = %v", err)
	}

	entries := []proto.Entry{
		{Index: 1, ClientID: 10, SeriesID: 1, Type: proto.EntryNormal, Cmd: makeCmd("a", "1")},
		{Index: 2, ClientID: 10, SeriesID: proto.SessionSeriesIDForUnregister, Type: proto.EntryNormal},
		{Index: 3, ClientID: 10, SeriesID: proto.SessionSeriesIDForRegister, Type: proto.EntryNormal},
	}
	results := make([]sm.Result, len(entries))
	if err := rsm.Apply(entries, results); err != nil {
		t.Fatalf("Apply = %v, want nil", err)
	}
	if cs.total != 1 {
		t.Errorf("user SM applied %d entries, want 1", cs.total)
	}
	if !rsm.Sessions().IsRegistered(10) {
		t.Fatal("session 10 must be registered by the trailing register entry")
	}

	// Series 1 must be treated as new against the replacement session.
	next := []proto.Entry{
		{Index: 4, ClientID: 10, SeriesID: 1, Type: proto.EntryNormal, Cmd: makeCmd("a", "2")},
	}
	nextResults := make([]sm.Result, 1)
	if err := rsm.Apply(next, nextResults); err != nil {
		t.Fatalf("Apply next = %v", err)
	}
	if cs.total != 2 {
		t.Errorf("user SM applied %d entries, want 2 (series 1 is new for the replacement session)", cs.total)
	}
	if nextResults[0].Value == sm.ResultSessionExpired {
		t.Errorf("entry against replacement session wrongly rejected as SESSION_EXPIRED")
	}
}

// TestApply_InBatchDuplicateWithCloseInSameBatch verifies at-most-once is
// preserved when the batch also closes the session: the duplicate is mirrored
// from the first occurrence, never re-applied.
func TestApply_InBatchDuplicateWithCloseInSameBatch(t *testing.T) {
	cs := newCountingSM()
	rsm, err := NewStateMachine(1, 1, func(_, _ uint64) interface{} {
		return cs
	}, t.TempDir(), testPolicy(0, 0, 0), nil, false)
	if err != nil {
		t.Fatalf("NewStateMachine = %v", err)
	}
	defer rsm.Close()

	if err := rsm.Sessions().Register(10, 0); err != nil {
		t.Fatalf("Register = %v", err)
	}

	entries := []proto.Entry{
		{Index: 1, ClientID: 10, SeriesID: 1, Type: proto.EntryNormal, Cmd: makeCmd("k", "v")},
		{Index: 2, ClientID: 10, SeriesID: 1, Type: proto.EntryNormal, Cmd: makeCmd("k", "v")},
		{Index: 3, ClientID: 10, SeriesID: proto.SessionSeriesIDForUnregister, Type: proto.EntryNormal},
	}
	results := make([]sm.Result, len(entries))
	if err := rsm.Apply(entries, results); err != nil {
		t.Fatalf("Apply = %v, want nil", err)
	}
	if cs.total != 1 {
		t.Errorf("user SM applied %d entries, want 1 (at-most-once)", cs.total)
	}
	if results[0].Value != results[1].Value || !bytes.Equal(results[0].Data, results[1].Data) {
		t.Errorf("mirror result differs from first occurrence: [0]=%+v [1]=%+v", results[0], results[1])
	}
}

// TestApply_SessionExpiryCannotDropSessionMidBatch pins the ordering that
// keeps deterministic session expiry out of the mid-batch removal class:
// expiry runs after the RecordResult pass, so an entry in the batch always
// records against a live session. The entry's response must be cached even
// when the batch's last index expires the session immediately afterwards.
func TestApply_SessionExpiryCannotDropSessionMidBatch(t *testing.T) {
	cs := newCountingSM()
	rsm, err := NewStateMachine(1, 1, func(_, _ uint64) interface{} {
		return cs
	}, t.TempDir(), testPolicy(0, 0, 100), nil, false)
	if err != nil {
		t.Fatalf("NewStateMachine = %v", err)
	}
	defer rsm.Close()

	if err := rsm.Sessions().Register(10, 450); err != nil {
		t.Fatalf("Register(10) = %v", err)
	}
	// Client 20 is idle far behind the batch's last index and is expired by
	// the end-of-batch scan; client 10 is active within the threshold.
	if err := rsm.Sessions().Register(20, 1); err != nil {
		t.Fatalf("Register(20) = %v", err)
	}

	entries := []proto.Entry{
		{Index: 450, ClientID: 10, SeriesID: 1, Type: proto.EntryNormal, Cmd: makeCmd("k", "v")},
		{Index: 500, Type: proto.EntryNormal, Cmd: makeCmd("z", "z")},
	}
	results := make([]sm.Result, len(entries))
	if err := rsm.Apply(entries, results); err != nil {
		t.Fatalf("Apply = %v, want nil", err)
	}
	if rsm.Sessions().IsRegistered(20) {
		t.Error("idle session 20 should have been expired at the end of the batch")
	}
	if !rsm.Sessions().IsRegistered(10) {
		t.Fatal("active session 10 must survive the end-of-batch expiry scan")
	}
	if cs.total != 2 {
		t.Errorf("user SM applied %d entries, want 2", cs.total)
	}

	// RecordResult ran against a live session: the cached response dedups.
	replay := []proto.Entry{
		{Index: 501, ClientID: 10, SeriesID: 1, Type: proto.EntryNormal, Cmd: makeCmd("k", "v")},
	}
	replayResults := make([]sm.Result, 1)
	if err := rsm.Apply(replay, replayResults); err != nil {
		t.Fatalf("Apply replay = %v", err)
	}
	if cs.total != 2 {
		t.Errorf("user SM applied %d entries after replay, want 2 (cached response reused)", cs.total)
	}
	if replayResults[0].Value != results[0].Value {
		t.Errorf("replay result = %d, want cached %d", replayResults[0].Value, results[0].Value)
	}
}
