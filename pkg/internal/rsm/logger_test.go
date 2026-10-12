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
	"log/slog"
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

const policyDisagreementMsg = "rsm: applying the shard's replicated session policy, which differs " +
	"from this node's configuration; the replicated value governs and cannot " +
	"be overridden locally"

// wantPolicyAttrs asserts the disagreement record's level and the attributes
// that name the previous, effective and local policies.
func wantPolicyAttrs(t *testing.T, r slog.Record, previous, effective, local proto.SessionPolicy) {
	t.Helper()
	if r.Level != slog.LevelWarn {
		t.Errorf("record level = %v, want WARN", r.Level)
	}
	want := map[string]uint64{
		"shardID":                       1,
		"replicaID":                     2,
		"previous_expiry_entries":       previous.ExpiryEntries,
		"expiry_entries":                effective.ExpiryEntries,
		"local_expiry_entries":          local.ExpiryEntries,
		"max_total_sessions":            effective.MaxTotalSessions,
		"local_max_total_sessions":      local.MaxTotalSessions,
		"max_sessions_per_client":       effective.MaxSessionsPerClient,
		"local_max_sessions_per_client": local.MaxSessionsPerClient,
	}
	got := map[string]uint64{}
	r.Attrs(func(a slog.Attr) bool {
		got[a.Key] = a.Value.Uint64()
		return true
	})
	for k, v := range want {
		g, ok := got[k]
		if !ok {
			t.Errorf("record has no %q attribute", k)
			continue
		}
		if g != v {
			t.Errorf("%s = %d, want %d", k, g, v)
		}
	}
}

// TestLogPolicyDisagreement_ReachesWithLogger pins that the WARN for a shard
// whose replicated session policy differs from this node's configuration
// reaches the logger given through WithLogger: at construction, for a policy
// the shard already knows, and on the apply path, when the shard publishes a
// new one.
func TestLogPolicyDisagreement_ReachesWithLogger(t *testing.T) {
	createFn := sm.NewCreateFunc(func(shardID, replicaID uint64) sm.StateMachine {
		return newTestKV()
	})
	local := proto.SessionPolicy{ExpiryEntries: 700, MaxTotalSessions: 11, MaxSessionsPerClient: 7}
	first := proto.SessionPolicy{ExpiryEntries: 800, MaxTotalSessions: 16, MaxSessionsPerClient: 8}
	replicated := first
	logs := &policyLogs{}

	s, err := NewStateMachine(1, 2, createFn, t.TempDir(), local,
		func() proto.SessionPolicy { return replicated }, false, WithLogger(slog.New(logs)))
	if err != nil {
		t.Fatalf("NewStateMachine = %v", err)
	}
	defer s.Close()

	got := logs.matching(policyDisagreementMsg)
	if len(got) != 1 {
		t.Fatalf("%d disagreement records at construction, want 1", len(got))
	}
	wantPolicyAttrs(t, got[0], local, first, local)

	replicated = proto.SessionPolicy{ExpiryEntries: 900, MaxTotalSessions: 22, MaxSessionsPerClient: 9}
	results := make([]sm.Result, 1)
	if err := s.Apply([]proto.Entry{{Index: 1, Cmd: makeCmd("k", "v")}}, results); err != nil {
		t.Fatalf("Apply = %v", err)
	}
	got = logs.matching(policyDisagreementMsg)
	if len(got) != 2 {
		t.Fatalf("%d disagreement records after the shard published a new policy, want 2", len(got))
	}
	wantPolicyAttrs(t, got[1], first, replicated, local)
}

// TestLogPolicyDisagreement_AgreementIsSilent pins that a replicated policy
// equal to the local one reports nothing.
func TestLogPolicyDisagreement_AgreementIsSilent(t *testing.T) {
	createFn := sm.NewCreateFunc(func(shardID, replicaID uint64) sm.StateMachine {
		return newTestKV()
	})
	local := proto.SessionPolicy{ExpiryEntries: 700, MaxTotalSessions: 11, MaxSessionsPerClient: 7}
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
