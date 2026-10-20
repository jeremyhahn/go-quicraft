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
	"errors"
	"log/slog"
	"strings"
	"sync"
	"testing"

	"github.com/jeremyhahn/go-quicraft/pkg/proto"
	"github.com/jeremyhahn/go-quicraft/pkg/sm"
)

// policyLogs is a slog.Handler that keeps every record it is handed, so a test
// can assert that a diagnostic reached the logger given through WithLogger.
type policyLogs struct {
	mu      sync.Mutex
	records []slog.Record
}

func (c *policyLogs) Enabled(context.Context, slog.Level) bool { return true }

func (c *policyLogs) Handle(_ context.Context, r slog.Record) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.records = append(c.records, r.Clone())
	return nil
}

func (c *policyLogs) WithAttrs([]slog.Attr) slog.Handler { return c }

func (c *policyLogs) WithGroup(string) slog.Handler { return c }

// matching returns every record with message msg.
func (c *policyLogs) matching(msg string) []slog.Record {
	c.mu.Lock()
	defer c.mu.Unlock()
	var out []slog.Record
	for _, r := range c.records {
		if r.Message == msg {
			out = append(out, r)
		}
	}
	return out
}

const policyDisagreementMsg = "rsm: refusing the shard: this node's session settings disagree " +
	"with the shard's committed session policy"

// wantPolicyAttrs asserts the refusal record's level, its shard and replica,
// and that its error names the disagreeing setting.
func wantPolicyAttrs(t *testing.T, r slog.Record, setting string) {
	t.Helper()
	if r.Level != slog.LevelError {
		t.Errorf("record level = %v, want ERROR", r.Level)
	}
	got := map[string]slog.Value{}
	r.Attrs(func(a slog.Attr) bool {
		got[a.Key] = a.Value
		return true
	})
	if v := got["shardID"]; v.Uint64() != 1 {
		t.Errorf("shardID = %v, want 1", v)
	}
	if v := got["replicaID"]; v.Uint64() != 2 {
		t.Errorf("replicaID = %v, want 2", v)
	}
	if v, ok := got["error"]; !ok || !strings.Contains(v.String(), setting) {
		t.Errorf("error attribute = %v, want one naming %s", v, setting)
	}
}

// TestLogPolicyDisagreement_ReachesWithLogger pins that the ERROR refusing a
// shard whose replicated session policy differs from this node's configuration
// reaches the logger given through WithLogger: at construction, for a policy
// the shard already knows, and on the apply path, when a joiner learns it.
func TestLogPolicyDisagreement_ReachesWithLogger(t *testing.T) {
	createFn := sm.NewCreateFunc(func(shardID, replicaID uint64) sm.StateMachine {
		return newTestKV()
	})
	local := proto.SessionPolicy{ExpiryEntries: 700, MaxTotalSessions: 11, MaxResponsesPerSession: 7}

	// Construction: a restart, where the shard's policy is already known.
	logs := &policyLogs{}
	replicated := proto.SessionPolicy{ExpiryEntries: 800, MaxTotalSessions: 11, MaxResponsesPerSession: 7}
	if _, err := NewStateMachine(1, 2, createFn, t.TempDir(), local,
		func() proto.SessionPolicy { return replicated }, false,
		WithLogger(slog.New(logs))); !errors.Is(err, ErrSessionPolicyMismatch) {
		t.Fatalf("NewStateMachine = %v, want ErrSessionPolicyMismatch", err)
	}
	got := logs.matching(policyDisagreementMsg)
	if len(got) != 1 {
		t.Fatalf("%d refusal records at construction, want 1", len(got))
	}
	wantPolicyAttrs(t, got[0], "SessionExpiryEntries")

	// Apply: a joiner, which learns the shard's policy after construction.
	logs = &policyLogs{}
	replicated = proto.SessionPolicy{}
	s, err := NewStateMachine(1, 2, createFn, t.TempDir(), local,
		func() proto.SessionPolicy { return replicated }, false, WithLogger(slog.New(logs)))
	if err != nil {
		t.Fatalf("NewStateMachine = %v", err)
	}
	defer s.Close()
	replicated = proto.SessionPolicy{ExpiryEntries: 700, MaxTotalSessions: 22, MaxResponsesPerSession: 7}
	results := make([]sm.Result, 1)
	if err := s.Apply([]proto.Entry{{Index: 1, Cmd: makeCmd("k", "v")}}, results); !errors.Is(err, ErrSessionPolicyMismatch) {
		t.Fatalf("Apply = %v, want ErrSessionPolicyMismatch", err)
	}
	got = logs.matching(policyDisagreementMsg)
	if len(got) != 1 {
		t.Fatalf("%d refusal records after the shard published a disagreeing policy, want 1", len(got))
	}
	wantPolicyAttrs(t, got[0], "MaxTotalSessions")
}

// TestLogPolicyDisagreement_AgreementIsSilent pins that a replicated policy
// equal to the local one reports nothing.
func TestLogPolicyDisagreement_AgreementIsSilent(t *testing.T) {
	createFn := sm.NewCreateFunc(func(shardID, replicaID uint64) sm.StateMachine {
		return newTestKV()
	})
	local := proto.SessionPolicy{ExpiryEntries: 700, MaxTotalSessions: 11, MaxResponsesPerSession: 7}
	logs := &policyLogs{}
	s, err := NewStateMachine(1, 2, createFn, t.TempDir(), local,
		func() proto.SessionPolicy { return local }, false, WithLogger(slog.New(logs)))
	if err != nil {
		t.Fatalf("NewStateMachine = %v", err)
	}
	defer s.Close()
	if got := logs.matching(policyDisagreementMsg); len(got) != 0 {
		t.Fatalf("%d disagreement records for an agreeing policy, want 0", len(got))
	}
}
