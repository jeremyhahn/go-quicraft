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
	"log/slog"
	"testing"
	"time"

	"github.com/jeremyhahn/go-quicraft/pkg/config"
	"github.com/jeremyhahn/go-quicraft/pkg/internal/raft"
	"github.com/jeremyhahn/go-quicraft/pkg/logdb"
	"github.com/jeremyhahn/go-quicraft/pkg/logdb/memdb"
	"github.com/jeremyhahn/go-quicraft/pkg/proto"
)

// recordingLogHandler is a slog handler that forwards every record to records,
// dropping it when the channel is full rather than blocking the logger.
type recordingLogHandler struct {
	records chan<- slog.Record
}

func (h recordingLogHandler) Enabled(context.Context, slog.Level) bool { return true }
func (h recordingLogHandler) Handle(_ context.Context, r slog.Record) error {
	select {
	case h.records <- r.Clone():
	default:
	}
	return nil
}
func (h recordingLogHandler) WithAttrs([]slog.Attr) slog.Handler { return h }
func (h recordingLogHandler) WithGroup(string) slog.Handler      { return h }

// awaitRecord waits for a record whose message is msg and returns it.
func awaitRecord(t *testing.T, records <-chan slog.Record, msg string) slog.Record {
	t.Helper()
	deadline := time.After(10 * time.Second)
	for {
		select {
		case r := <-records:
			if r.Message == msg {
				return r
			}
		case <-deadline:
			t.Fatalf("no %q record reached the Host logger", msg)
		}
	}
}

// recordAttr returns the uint64 value of the record's attribute key.
func recordAttr(t *testing.T, r slog.Record, key string) uint64 {
	t.Helper()
	var (
		got   uint64
		found bool
	)
	r.Attrs(func(a slog.Attr) bool {
		if a.Key == key {
			got, found = a.Value.Uint64(), true
			return false
		}
		return true
	})
	if !found {
		t.Fatalf("record %q has no %q attribute", r.Message, key)
	}
	return got
}

const (
	clampWarnMsg  = "recovered commit index exceeds last log entry, clamping"
	clampErrorMsg = "recovered commit index over an EMPTY log: entry records were lost while the state record survived; clamping to 0 and re-learning the log from the leader"
)

// TestStartShard_RecoveryClampLogsOnHostLogger: a recovered commit index ahead
// of the recovered log is clamped, and the clamp is reported on the logger the
// Host was given through WithLogger, not on the process's slog default.
func TestStartShard_RecoveryClampLogsOnHostLogger(t *testing.T) {
	records := make(chan slog.Record, 1024)
	db := memdb.New()
	h, err := NewHost(testHostConfig(t), WithLogDB(db), WithoutTransport(), WithoutEncryption(),
		WithLogger(slog.New(recordingLogHandler{records: records})))
	if err != nil {
		t.Fatalf("NewHost failed: %v", err)
	}
	defer h.Close()

	// The log holds 1..5 while the state record claims commit 10: a torn
	// batch that kept the state record but lost the entries behind it.
	entries := []logdb.Entry{bootstrapLogEntry(t, 3, map[uint64]string{1: "127.0.0.1:5000"})}
	for i := uint64(2); i <= 5; i++ {
		entries = append(entries, logdb.Entry{Index: i, Term: 3})
	}
	if err := db.SaveState([]logdb.Update{{
		ShardID:   1,
		ReplicaID: 1,
		State:     logdb.State{Term: 3, Vote: 1, Commit: 10},
		Entries:   entries,
	}}); err != nil {
		t.Fatalf("SaveState failed: %v", err)
	}

	if err := h.StartShard(map[uint64]string{1: "127.0.0.1:5000"}, false,
		testCreateFunc, testShardConfig(1, 1)); err != nil {
		t.Fatalf("StartShard failed: %v", err)
	}

	r := awaitRecord(t, records, clampWarnMsg)
	if r.Level != slog.LevelWarn {
		t.Errorf("clamp record level = %v, want WARN", r.Level)
	}
	if got := recordAttr(t, r, "recovered_commit"); got != 10 {
		t.Errorf("recovered_commit = %d, want 10", got)
	}
	if got := recordAttr(t, r, "last_index"); got != 5 {
		t.Errorf("last_index = %d, want 5", got)
	}
	if got := recordAttr(t, r, "shard"); got != 1 {
		t.Errorf("shard = %d, want 1", got)
	}
}

// TestLogRecoveryClamp_Levels: a clamp to 0 — the state record survived over
// an empty log — is reported at ERROR, any other clamp at WARN, and no clamp
// reports nothing.
func TestLogRecoveryClamp_Levels(t *testing.T) {
	records := make(chan slog.Record, 16)
	h := &Host{logger: slog.New(recordingLogHandler{records: records})}

	h.logRecoveryClamp(1, 1, raft.RecoveryClamp{})
	h.logRecoveryClamp(1, 1, raft.RecoveryClamp{RecoveredCommit: 7, RecoveredTerm: 2})
	r := awaitRecord(t, records, clampErrorMsg)
	if r.Level != slog.LevelError {
		t.Errorf("empty-log clamp level = %v, want ERROR", r.Level)
	}
	if got := recordAttr(t, r, "recovered_term"); got != 2 {
		t.Errorf("recovered_term = %d, want 2", got)
	}

	h.logRecoveryClamp(1, 1, raft.RecoveryClamp{RecoveredCommit: 9, LastIndex: 4})
	if r := awaitRecord(t, records, clampWarnMsg); r.Level != slog.LevelWarn {
		t.Errorf("partial clamp level = %v, want WARN", r.Level)
	}
	select {
	case r := <-records:
		t.Errorf("unexpected record %q: a zero RecoveryClamp must report nothing", r.Message)
	default:
	}
}

const appliedClampWarnMsg = "state machine applied index exceeds recovered commit index, clamping"

// TestStartShard_AppliedClampLogsOnHostLogger: an on-disk state machine whose
// applied index outruns the recovered commit index is clamped, and the clamp
// is reported at WARN on the logger the Host was given through WithLogger, not
// on the process's slog default.
func TestStartShard_AppliedClampLogsOnHostLogger(t *testing.T) {
	records := make(chan slog.Record, 1024)
	db := memdb.New()
	h, err := NewHost(testHostConfig(t), WithLogDB(db), WithoutTransport(), WithoutEncryption(),
		WithLogger(slog.New(recordingLogHandler{records: records})))
	if err != nil {
		t.Fatalf("NewHost failed: %v", err)
	}
	defer h.Close()

	// The log holds and commits 1..50; the state machine claims 100.
	entries := []logdb.Entry{bootstrapLogEntry(t, 4, map[uint64]string{1: "127.0.0.1:5000"})}
	for i := uint64(2); i <= 50; i++ {
		entries = append(entries, logdb.Entry{Index: i, Term: 4})
	}
	if err := db.SaveState([]logdb.Update{{
		ShardID:   1,
		ReplicaID: 1,
		State:     logdb.State{Term: 4, Vote: 1, Commit: 50},
		Entries:   entries,
	}}); err != nil {
		t.Fatalf("SaveState failed: %v", err)
	}

	diskSM := &recordingOnDiskSM{openLastApplied: 100}
	diskSM.kv = map[string]uint64{"k": 100}
	create := func(uint64, uint64) interface{} { return diskSM }
	if err := h.StartShard(map[uint64]string{1: "127.0.0.1:5000"}, false,
		create, testShardConfig(1, 1)); err != nil {
		t.Fatalf("StartShard failed: %v", err)
	}

	r := awaitRecord(t, records, appliedClampWarnMsg)
	if r.Level != slog.LevelWarn {
		t.Errorf("applied clamp record level = %v, want WARN", r.Level)
	}
	if got := recordAttr(t, r, "sm_applied"); got != 100 {
		t.Errorf("sm_applied = %d, want 100", got)
	}
	if got := recordAttr(t, r, "committed"); got != 50 {
		t.Errorf("committed = %d, want 50", got)
	}
	if got := recordAttr(t, r, "shard"); got != 1 {
		t.Errorf("shard = %d, want 1", got)
	}
}

// TestLogAppliedClamp_ZeroReportsNothing: a zero AppliedClamp reports nothing.
func TestLogAppliedClamp_ZeroReportsNothing(t *testing.T) {
	records := make(chan slog.Record, 16)
	h := &Host{logger: slog.New(recordingLogHandler{records: records})}

	h.logAppliedClamp(1, 1, raft.AppliedClamp{})
	h.logAppliedClamp(1, 1, raft.AppliedClamp{SMApplied: 5, Committed: 5})
	select {
	case r := <-records:
		t.Errorf("unexpected record %q: no clamp must report nothing", r.Message)
	default:
	}
}

const unfoldedConfigErrorMsg = "committed config changes below the state machine's applied index " +
	"were not folded: this replica will not campaign until they are"

// TestStartShard_UnfoldedConfigLogsOnHostLogger: an on-disk state machine
// applied above a committed config change that does not decode cannot have
// that window folded, and the failure is reported at ERROR on the logger the
// Host was given through WithLogger — before StartShard aborts on it.
func TestStartShard_UnfoldedConfigLogsOnHostLogger(t *testing.T) {
	records := make(chan slog.Record, 1024)
	db := memdb.New()
	h, err := NewHost(testHostConfig(t), WithLogDB(db), WithoutTransport(), WithoutEncryption(),
		WithLogger(slog.New(recordingLogHandler{records: records})))
	if err != nil {
		t.Fatalf("NewHost failed: %v", err)
	}
	defer h.Close()

	// The log commits 1..31; index 31 is a config change whose payload is not
	// a marshaled ConfigChange, and the state machine claims 31.
	entries := []logdb.Entry{bootstrapLogEntry(t, 4, map[uint64]string{1: "127.0.0.1:5000"})}
	for i := uint64(2); i <= 30; i++ {
		entries = append(entries, logdb.Entry{Index: i, Term: 4})
	}
	entries = append(entries, logdb.Entry{Index: 31, Term: 4,
		Type: proto.EntryConfigChange, Cmd: []byte{0xFF, 0xFF, 0xFF}})
	if err := db.SaveState([]logdb.Update{{
		ShardID:   1,
		ReplicaID: 1,
		State:     logdb.State{Term: 4, Vote: 1, Commit: 31},
		Entries:   entries,
	}}); err != nil {
		t.Fatalf("SaveState failed: %v", err)
	}

	diskSM := &recordingOnDiskSM{openLastApplied: 31}
	diskSM.kv = map[string]uint64{"k": 31}
	create := func(uint64, uint64) interface{} { return diskSM }
	if err := h.StartShard(map[uint64]string{1: "127.0.0.1:5000"}, false,
		create, testShardConfig(1, 1)); !errors.Is(err, raft.ErrCorruptConfigEntry) {
		t.Fatalf("StartShard = %v, want ErrCorruptConfigEntry", err)
	}

	r := awaitRecord(t, records, unfoldedConfigErrorMsg)
	if r.Level != slog.LevelError {
		t.Errorf("unfolded config record level = %v, want ERROR", r.Level)
	}
	if got := recordAttr(t, r, "index"); got != 31 {
		t.Errorf("index = %d, want 31", got)
	}
	if got := recordAttr(t, r, "stale_through"); got != 31 {
		t.Errorf("stale_through = %d, want 31", got)
	}
	if got := recordAttr(t, r, "shard"); got != 1 {
		t.Errorf("shard = %d, want 1", got)
	}
}

// TestLogUnfoldedConfig_ZeroReportsNothing: a complete fold reports nothing.
func TestLogUnfoldedConfig_ZeroReportsNothing(t *testing.T) {
	records := make(chan slog.Record, 16)
	h := &Host{logger: slog.New(recordingLogHandler{records: records})}

	h.logUnfoldedConfig(1, 1, raft.UnfoldedConfig{})
	select {
	case r := <-records:
		t.Errorf("unexpected record %q: a complete fold must report nothing", r.Message)
	default:
	}
}

// TestCheckRecoveredIdentities_RemovedIdentityWarnsOnHostLogger: a
// Config.MemberIdentities entry naming an identity the shard has removed is
// reported at WARN on the Host's logger, not on the process's slog default.
func TestCheckRecoveredIdentities_RemovedIdentityWarnsOnHostLogger(t *testing.T) {
	records := make(chan slog.Record, 16)
	h := &Host{logger: slog.New(recordingLogHandler{records: records})}

	cfg := config.Config{ShardID: 1, ReplicaID: 1, MemberIdentities: map[uint64]string{3: "gone"}}
	m := proto.Membership{RemovedIdentities: map[uint64]string{2: "gone"}}
	if err := h.checkRecoveredIdentities(cfg, m); err != nil {
		t.Fatalf("checkRecoveredIdentities: %v", err)
	}
	r := awaitRecord(t, records, "quicraft: Config.MemberIdentities names an identity this shard has removed; "+
		"the entry is inert (the removal deny-list is consulted first) and can be dropped")
	if r.Level != slog.LevelWarn {
		t.Errorf("record level = %v, want WARN", r.Level)
	}
	if got := recordAttr(t, r, "removed_replica"); got != 2 {
		t.Errorf("removed_replica = %d, want 2", got)
	}
}
