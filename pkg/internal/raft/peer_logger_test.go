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

package raft

import (
	"context"
	"log/slog"
	"sync"
	"testing"

	"github.com/jeremyhahn/go-quicraft/pkg/proto"
)

// capturedLogs is a slog.Handler that keeps every record it is handed, so a
// test can assert that a diagnostic reached the logger set through
// Peer.SetLogger with its level and attributes.
type capturedLogs struct {
	mu      sync.Mutex
	records []slog.Record
}

func (c *capturedLogs) Enabled(context.Context, slog.Level) bool { return true }

func (c *capturedLogs) Handle(_ context.Context, r slog.Record) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.records = append(c.records, r.Clone())
	return nil
}

func (c *capturedLogs) WithAttrs([]slog.Attr) slog.Handler { return c }

func (c *capturedLogs) WithGroup(string) slog.Handler { return c }

// find returns the first record with message msg, failing the test when there
// is none.
func (c *capturedLogs) find(t *testing.T, msg string) slog.Record {
	t.Helper()
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, r := range c.records {
		if r.Message == msg {
			return r
		}
	}
	t.Fatalf("no %q record reached the Peer's logger (got %d records)", msg, len(c.records))
	return slog.Record{}
}

// attr returns the value of the record's attribute key, failing the test when
// it has none.
func attr(t *testing.T, r slog.Record, key string) slog.Value {
	t.Helper()
	var (
		got   slog.Value
		found bool
	)
	r.Attrs(func(a slog.Attr) bool {
		if a.Key == key {
			got, found = a.Value, true
			return false
		}
		return true
	})
	if !found {
		t.Fatalf("record %q has no %q attribute", r.Message, key)
	}
	return got
}

// wantUint asserts the record's attribute key is the unsigned integer want.
func wantUint(t *testing.T, r slog.Record, key string, want uint64) {
	t.Helper()
	if got := attr(t, r, key).Uint64(); got != want {
		t.Errorf("record %q: %s = %d, want %d", r.Message, key, got, want)
	}
}

// staleTailPeer returns a Peer recovered from a bootstrap entry at index 1 and
// the committed config-change entry tail at index 2, reading through a
// failingReadLogReader so a test can make the tail unreadable.
func staleTailPeer(t *testing.T, tail proto.Entry) (*Peer, *failingReadLogReader) {
	t.Helper()
	base := map[uint64]string{1: "a1", 2: "a2", 3: "a3"}
	bootstrap, err := BuildInitialMembershipEntry(proto.Membership{Addresses: base})
	if err != nil {
		t.Fatalf("BuildInitialMembershipEntry: %v", err)
	}
	inner := newTestLogReader([]proto.Entry{bootstrap, tail}, proto.State{Term: 1, Commit: 2})
	inner.membership = proto.Membership{Addresses: maps(base)}
	lr := &failingReadLogReader{testLogReader: inner}
	p, err := NewPeer(testConfigWithID(1, 1), lr, nil)
	if err != nil {
		t.Fatalf("NewPeer: %v", err)
	}
	return p, lr
}

// TestResolveStaleStartupConfig_UnresolvedWarnsOnPeerLogger pins that the WARN
// a replica emits when it cannot read its committed configuration tail before
// campaigning reaches the logger set through SetLogger, not the slog default.
func TestResolveStaleStartupConfig_UnresolvedWarnsOnPeerLogger(t *testing.T) {
	p, lr := staleTailPeer(t, proto.Entry{
		Index: 2, Term: 1, Type: proto.EntryConfigChange,
		Cmd: marshalConfigChange(t, proto.ConfigChange{Type: proto.AddNode, ReplicaID: 4, Address: "a4"}),
	})
	logs := &capturedLogs{}
	p.SetLogger(slog.New(logs))
	lr.failing = true

	if p.resolveStaleStartupConfig() {
		t.Fatal("resolveStaleStartupConfig resolved an unreadable configuration tail")
	}
	r := logs.find(t, "configuration tail unresolved: refusing to campaign against a "+
		"possibly superseded voting set")
	if r.Level != slog.LevelWarn {
		t.Errorf("record level = %v, want WARN", r.Level)
	}
	wantUint(t, r, "shard", 1)
	wantUint(t, r, "replica", 1)
	wantUint(t, r, "folded_through", 0)
	wantUint(t, r, "stale_through", 2)
	attr(t, r, "config_change_id")
}

// TestResolveStaleStartupConfig_UndecodableErrorsOnPeerLogger pins that the
// ERROR a replica emits when its committed configuration tail holds a config
// change that will never decode reaches the logger set through SetLogger.
func TestResolveStaleStartupConfig_UndecodableErrorsOnPeerLogger(t *testing.T) {
	p, _ := staleTailPeer(t, proto.Entry{
		Index: 2, Term: 1, Type: proto.EntryConfigChange, Cmd: []byte{0xff},
	})
	logs := &capturedLogs{}
	p.SetLogger(slog.New(logs))

	if p.resolveStaleStartupConfig() {
		t.Fatal("resolveStaleStartupConfig resolved an undecodable configuration tail")
	}
	r := logs.find(t, "configuration tail holds an undecodable committed config entry: "+
		"refusing to campaign on a configuration this replica cannot reconstruct")
	if r.Level != slog.LevelError {
		t.Errorf("record level = %v, want ERROR", r.Level)
	}
	wantUint(t, r, "shard", 1)
	wantUint(t, r, "replica", 1)
	wantUint(t, r, "stale_through", 2)
	attr(t, r, "folded_through")
	if attr(t, r, "error").Any() == nil {
		t.Error("record carries a nil error")
	}
}

// TestPublishPendingConfig_SeedBudgetWarnsOnPeerLogger pins that the WARN for a
// recovered committed tail past the pending-config seed budget reaches the
// logger set through SetLogger.
func TestPublishPendingConfig_SeedBudgetWarnsOnPeerLogger(t *testing.T) {
	saved := pendingSeedBudget
	pendingSeedBudget = 4096
	defer func() { pendingSeedBudget = saved }()

	var entries []proto.Entry
	for i := uint64(1); i <= 200; i++ {
		entries = append(entries, proto.Entry{Index: i, Term: 1, Type: proto.EntryNormal, Cmd: make([]byte, 100)})
	}
	entries = append(entries, addNodeEntry(t, 201, 1, 4, "spki:4"))
	p := newTestPeerWithLog(t, testConfig(), entries, proto.State{Term: 1, Commit: 201}, threeNodeMembership())
	logs := &capturedLogs{}
	p.SetLogger(slog.New(logs))

	p.PublishPendingConfig()
	r := logs.find(t, "the recovered committed tail exceeds the pending-config seed budget; "+
		"a config change above the budget admits its member only once the replay folds it")
	if r.Level != slog.LevelWarn {
		t.Errorf("record level = %v, want WARN", r.Level)
	}
	wantUint(t, r, "committed", 201)
	wantUint(t, r, "budget_bytes", 4096)
	attr(t, r, "from")
}

// TestPublishPendingConfig_UnreadableTailWarnsOnPeerLogger pins that the two
// WARNs for a recovered tail the first publish cannot read — its committed
// stretch and its uncommitted one — reach the logger set through SetLogger.
func TestPublishPendingConfig_UnreadableTailWarnsOnPeerLogger(t *testing.T) {
	entries := []proto.Entry{
		{Index: 1, Term: 1, Type: proto.EntryNormal},
		{Index: 2, Term: 1, Type: proto.EntryNormal},
		{Index: 3, Term: 1, Type: proto.EntryNormal},
	}
	inner := newTestLogReader(entries, proto.State{Term: 1, Commit: 2})
	inner.membership = threeNodeMembership()
	lr := &failingReadLogReader{testLogReader: inner}
	p, err := NewPeer(testConfig(), lr, nil)
	if err != nil {
		t.Fatalf("NewPeer: %v", err)
	}
	logs := &capturedLogs{}
	p.SetLogger(slog.New(logs))
	lr.failing = true

	p.PublishPendingConfig()
	committed := logs.find(t, "could not classify the recovered committed tail for pending config changes")
	if committed.Level != slog.LevelWarn {
		t.Errorf("committed-tail record level = %v, want WARN", committed.Level)
	}
	wantUint(t, committed, "to", 2)
	attr(t, committed, "from")
	if attr(t, committed, "error").Any() == nil {
		t.Error("committed-tail record carries a nil error")
	}

	uncommitted := logs.find(t, "could not classify the recovered uncommitted tail for pending config changes")
	if uncommitted.Level != slog.LevelWarn {
		t.Errorf("uncommitted-tail record level = %v, want WARN", uncommitted.Level)
	}
	wantUint(t, uncommitted, "from", 3)
	wantUint(t, uncommitted, "to", 3)
	if attr(t, uncommitted, "error").Any() == nil {
		t.Error("uncommitted-tail record carries a nil error")
	}
}

// TestPeerSetLogger_NilRestoresDefault pins that SetLogger(nil) falls back to
// the process slog default rather than leaving the Peer with no logger.
func TestPeerSetLogger_NilRestoresDefault(t *testing.T) {
	p := newTestPeer(t, testConfig(), proto.State{Term: 1}, threeNodeMembership())
	p.SetLogger(slog.New(&capturedLogs{}))
	p.SetLogger(nil)
	if got := p.log(); got != slog.Default() {
		t.Fatalf("log() = %p after SetLogger(nil), want the slog default %p", got, slog.Default())
	}
}
