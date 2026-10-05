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
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jeremyhahn/go-quicraft/pkg/internal/supervise"
	"github.com/jeremyhahn/go-quicraft/pkg/proto"
)

// ---------------------------------------------------------------------------
// Runtime and recovery must agree on failing CLOSED
//
// A committed config-carrying entry this binary cannot DECODE is replicated
// state every peer can read and this replica cannot. Recovery has always
// treated it as fatal (ReconstructMembership -> StartShard aborts) and the
// commit pipeline now does too (processConfigChanges -> failCommit). The Host's
// ROUTING MIRROR of the same fold — applyMembershipRouting and
// registerBootstrapAddresses, which decode the identical entries a second time
// to update ns.nodes and the transport registry — logged an ERROR and carried
// on.
//
// Carrying on is wrong even though the commit path means nothing undecodable
// should reach here: if one does, the invariant the two paths share has been
// broken, and the result is a replica whose routing table silently disagrees
// with the membership raft is computing quorum over, while it keeps acking the
// leader and serving reads.
// ---------------------------------------------------------------------------

// corruptConfigEntries returns the two committed shapes the Host mirror
// decodes, each with a payload that cannot be parsed. Both are non-empty: an
// empty EntryConfigChange Cmd is the documented silent no-op, not corruption.
func corruptConfigEntries() map[string]proto.Entry {
	return map[string]proto.Entry{
		"config change": {
			Index: 7, Term: 1, Type: proto.EntryConfigChange,
			Cmd: []byte{0xFF, 0xFF, 0xFF},
		},
		"bootstrap membership": {
			Index: 1, Term: 1, Type: proto.EntryInitialMembership,
			Cmd: []byte("this is not a marshaled membership"),
		},
	}
}

// TestApplyMembershipRouting_FaultsTheShardOnACorruptCommittedEntry is the
// regression for both mirrors.
func TestApplyMembershipRouting_FaultsTheShardOnACorruptCommittedEntry(t *testing.T) {
	for name, entry := range corruptConfigEntries() {
		t.Run(name, func(t *testing.T) {
			h := newTestHost(t)
			if err := h.StartShard(map[uint64]string{1: "127.0.0.1:5000"}, false,
				testCreateFunc, testShardConfig(1, 1)); err != nil {
				t.Fatalf("StartShard: %v", err)
			}
			engNode := h.engine.GetNode(1)
			if engNode == nil {
				t.Fatal("shard 1 has no engine node")
			}
			if engNode.IsFailed() {
				t.Fatal("precondition: the shard must start healthy")
			}

			cb := &hostEngineCallback{h: h}
			cb.applyMembershipRouting(testLoadNode(h, 1), 1, []proto.Entry{entry})

			if !engNode.IsFailed() {
				t.Fatalf("the Host routing mirror decoded a committed %s entry, "+
					"failed, and left the shard RUNNING. Recovery aborts StartShard "+
					"for these bytes and the commit pipeline faults the shard for "+
					"them; a replica that keeps serving here has a routing table "+
					"that disagrees with the membership raft holds", name)
			}
		})
	}
}

// TestApplyMembershipRouting_HealthyEntriesDoNotFaultTheShard is the POSITIVE
// CONTROL. Without it a mutation that faulted the shard unconditionally would
// satisfy the assertion above — and would take down every shard on the first
// committed config change.
func TestApplyMembershipRouting_HealthyEntriesDoNotFaultTheShard(t *testing.T) {
	h := newTestHost(t)
	if err := h.StartShard(map[uint64]string{1: "127.0.0.1:5000"}, false,
		testCreateFunc, testShardConfig(1, 1)); err != nil {
		t.Fatalf("StartShard: %v", err)
	}
	engNode := h.engine.GetNode(1)
	if engNode == nil {
		t.Fatal("shard 1 has no engine node")
	}

	cc := proto.ConfigChange{Type: proto.AddNode, ReplicaID: 2, Address: "127.0.0.1:5001"}
	ccBuf := make([]byte, cc.Size())
	n, err := cc.MarshalTo(ccBuf)
	if err != nil {
		t.Fatalf("ConfigChange.MarshalTo: %v", err)
	}
	ms := proto.Membership{Addresses: map[uint64]string{1: "127.0.0.1:5000"}}
	msBuf := make([]byte, ms.Size())
	mn, err := ms.MarshalTo(msBuf)
	if err != nil {
		t.Fatalf("Membership.MarshalTo: %v", err)
	}

	cb := &hostEngineCallback{h: h}
	cb.applyMembershipRouting(testLoadNode(h, 1), 1, []proto.Entry{
		{Index: 1, Term: 1, Type: proto.EntryInitialMembership, Cmd: msBuf[:mn]},
		{Index: 7, Term: 1, Type: proto.EntryConfigChange, Cmd: ccBuf[:n]},
	})

	if engNode.IsFailed() {
		t.Fatal("a batch of well-formed committed membership entries faulted the " +
			"shard: every config change would now take the shard down")
	}
	// The routing effect the mirror exists for must still have happened.
	ns := testLoadNode(h, 1)
	ns.nodesMu.Lock()
	addr := ns.nodes[2]
	ns.nodesMu.Unlock()
	if addr != "127.0.0.1:5001" {
		t.Fatalf("ns.nodes[2] = %q, want 127.0.0.1:5001: the mirror stopped doing "+
			"its job", addr)
	}
}

// TestFailShardOnCorruptConfig_ReportsATypedError pins what an operator
// receives: a typed error naming the shard, the index and which payload, not an
// opaque log line. It is delivered through the engine's terminal-error path, so
// EventListener.OnShardFailed sees it like any other shard failure.
func TestFailShardOnCorruptConfig_ReportsATypedError(t *testing.T) {
	err := error(&CorruptConfigEntryError{
		ShardID: 4, Index: 9, Kind: "config change", Err: proto.ErrBufferTooSmall,
	})
	if !errors.Is(err, proto.ErrBufferTooSmall) {
		t.Fatalf("CorruptConfigEntryError does not unwrap to the decode failure: %v", err)
	}
	msg := err.Error()
	for _, want := range []string{"config change", "index 9", "shard 4"} {
		if !strings.Contains(msg, want) {
			t.Fatalf("error %q does not name %q; an operator cannot locate the "+
				"entry without it", msg, want)
		}
	}
}

// awaitContainedDiagnostics waits until every library diagnostic submitted so
// far has been emitted. Diagnostics are emitted in submission order on one
// contained worker (supervise.LogContained), so a test that counts log
// records can otherwise receive one an earlier test submitted; once a
// sentinel submitted now has run, nothing submitted before it is pending.
func awaitContainedDiagnostics(t *testing.T) {
	t.Helper()
	drained := make(chan struct{})
	supervise.LogContained(func() { close(drained) })
	select {
	case <-drained:
	case <-time.After(5 * time.Second):
		t.Fatal("the contained-diagnostics queue did not drain")
	}
}

// haltCountingHandler counts the host's "shard halted" log line — written once
// per OnShardFailed fan-out — for a corrupt configuration entry, and discards
// everything else.
type haltCountingHandler struct{ halts *atomic.Int64 }

func (h haltCountingHandler) Enabled(context.Context, slog.Level) bool { return true }
func (h haltCountingHandler) Handle(_ context.Context, r slog.Record) error {
	if !strings.HasPrefix(r.Message, "shard halted") {
		return nil
	}
	r.Attrs(func(a slog.Attr) bool {
		if err, ok := a.Value.Any().(error); ok && a.Key == "error" {
			if _, corrupt := errors.AsType[*CorruptConfigEntryError](err); corrupt {
				h.halts.Add(1)
			}
			return false
		}
		return true
	})
	return nil
}
func (h haltCountingHandler) WithAttrs([]slog.Attr) slog.Handler { return h }
func (h haltCountingHandler) WithGroup(string) slog.Handler      { return h }

// TestFailShardOnCorruptConfig_AlreadyFailedShardIsNotReportedAgain: the
// routing mirror reports the halt only when its own Fail tripped the latch. A
// shard another path failed first was reported, and its reads drained, by that
// path; a second OnShardFailed fan-out would report one halt twice.
func TestFailShardOnCorruptConfig_AlreadyFailedShardIsNotReportedAgain(t *testing.T) {
	h := newTestHost(t)
	if err := h.StartShard(map[uint64]string{1: "127.0.0.1:5000"}, false,
		testCreateFunc, testShardConfig(1, 1)); err != nil {
		t.Fatalf("StartShard: %v", err)
	}
	engNode := h.engine.GetNode(1)
	if engNode == nil {
		t.Fatal("shard 1 has no engine node")
	}
	awaitContainedDiagnostics(t)
	var halts atomic.Int64
	prev := slog.Default()
	slog.SetDefault(slog.New(haltCountingHandler{halts: &halts}))
	t.Cleanup(func() { slog.SetDefault(prev) })

	// The halt line is emitted contained, on the diagnostics worker, after
	// the pending requests are failed; it is awaited rather than read back
	// synchronously.
	// haltsWithin returns the halt count once it reaches want or d elapses.
	haltsWithin := func(want int64, d time.Duration) int64 {
		deadline := time.After(d)
		for halts.Load() < want {
			select {
			case <-deadline:
				return halts.Load()
			case <-time.After(time.Millisecond):
			}
		}
		return halts.Load()
	}
	cb := &hostEngineCallback{h: h}
	cb.failShardOnCorruptConfig(1, 9, "config change", proto.ErrBufferTooSmall)
	if got := haltsWithin(1, 5*time.Second); got != 1 {
		t.Fatalf("first corrupt-config failure reported %d halts, want 1", got)
	}
	cb.failShardOnCorruptConfig(1, 10, "config change", proto.ErrBufferTooSmall)
	// A second report would be emitted the same way; it is given far longer
	// than the first one took before concluding there is none.
	if got := haltsWithin(2, 200*time.Millisecond); got != 1 {
		t.Fatalf("a second failure of an already-failed shard reported the halt again: %d halts, want 1", got)
	}
}
