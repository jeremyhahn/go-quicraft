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
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jeremyhahn/go-quicraft/pkg/internal/engine"
	"github.com/jeremyhahn/go-quicraft/pkg/logdb"
	"github.com/jeremyhahn/go-quicraft/pkg/proto"
	"github.com/jeremyhahn/go-quicraft/pkg/sm"
)

// ---------------------------------------------------------------------------
// Close reports the real shutdown outcome to every caller
// ---------------------------------------------------------------------------

// TestClose_SecondCallerGetsTheSameError pins that a repeated Close reports
// the shutdown that actually happened.
//
// Close's own doc says a nil return means every step succeeded, and the
// closed-flag CAS made the SECOND caller return nil unconditionally — before
// the first caller's shutdown had even started. The caller most likely to hit
// that is a supervisor whose deferred Close races the explicit one in its
// shutdown path, so the one report that reached the operator was the fabricated
// one.
func TestClose_SecondCallerGetsTheSameError(t *testing.T) {
	cfg := testHostConfig(t)
	h, err := NewHost(cfg, WithMemoryLogDB(), WithoutTransport(), WithoutEncryption())
	if err != nil {
		t.Fatalf("NewHost failed: %v", err)
	}

	members := map[uint64]string{1: "127.0.0.1:5000"}
	scfg := testShardConfig(1, 1)
	if startErr := h.StartShard(members, false, closeFailingCreateFunc, scfg); startErr != nil {
		t.Fatalf("StartShard failed: %v", startErr)
	}
	waitForLeader(t, h, 1)

	first := h.Close()
	if !errors.Is(first, errCloseSM) {
		t.Fatalf("first Close = %v, want the state machine's Close error; the "+
			"test cannot say anything about the second caller if the first "+
			"never reported a failure", first)
	}

	second := h.Close()
	if !errors.Is(second, errCloseSM) {
		t.Fatalf("second Close = %v, want the same error the first reported. A "+
			"nil here tells the caller the host shut down cleanly when its "+
			"state machine's final flush failed", second)
	}
}

// TestClose_ConcurrentCallersAllWaitAndReport covers the racing form: the
// second Close arrives while the first is still tearing down. It must BLOCK
// until the shutdown finishes and then report the same outcome, because a
// caller that returns early goes on to release resources the shutdown is
// still using.
func TestClose_ConcurrentCallersAllWaitAndReport(t *testing.T) {
	cfg := testHostConfig(t)
	h, err := NewHost(cfg, WithMemoryLogDB(), WithoutTransport(), WithoutEncryption())
	if err != nil {
		t.Fatalf("NewHost failed: %v", err)
	}

	members := map[uint64]string{1: "127.0.0.1:5000"}
	scfg := testShardConfig(1, 1)
	if startErr := h.StartShard(members, false, closeFailingCreateFunc, scfg); startErr != nil {
		t.Fatalf("StartShard failed: %v", startErr)
	}
	waitForLeader(t, h, 1)

	const callers = 4
	results := make([]error, callers)
	var wg sync.WaitGroup
	start := make(chan struct{})
	wg.Add(callers)
	for i := 0; i < callers; i++ {
		go func(idx int) {
			defer wg.Done()
			<-start
			results[idx] = h.Close()
		}(i)
	}
	close(start)

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(60 * time.Second):
		t.Fatal("concurrent Close callers did not all return")
	}

	for i, got := range results {
		if !errors.Is(got, errCloseSM) {
			t.Errorf("Close caller %d = %v, want the state machine's Close error", i, got)
		}
	}
}

// ---------------------------------------------------------------------------
// Snapshot error translation
// ---------------------------------------------------------------------------

// TestTranslateSnapshotError pins that the engine's internal snapshot errors
// reach a SyncRequestSnapshot caller with an identity that caller can match.
//
// Before this translation the snapshot RequestState carried the engine error
// verbatim. engine is an internal package, so no consumer of the library could
// write an errors.Is against any of these — and the public sentinels that
// describe these exact conditions (ErrStopped, ErrSnapshotRecoveryFailed) had
// no producer anywhere in the library as a result.
// errApplyRowInner is the state machine error carried by the *engine.ApplyError
// row of TestTranslateSnapshotError.
var errApplyRowInner = errors.New("quicraft_test: state machine refused the entry")

func TestTranslateSnapshotError(t *testing.T) {
	tests := []struct {
		name string
		in   error
		// want is the sentinel the result must match.
		want error
		// check runs additional assertions on the translated error.
		check func(t *testing.T, got error)
	}{
		{
			name: "nil stays nil",
			in:   nil,
			want: nil,
		},
		{
			name: "engine stop becomes the public ErrStopped",
			in:   engine.ErrStopped,
			want: ErrStopped,
		},
		{
			// *engine.SMPanicError is the ONLY type the engine constructs that
			// matches engine.ErrShardFailed (apply.go builds one in the apply
			// worker's recover block). *engine.ApplyError, the other terminal
			// apply error, carries no Is method and so reaches the caller as
			// itself — see the ApplyError row below.
			name: "a latched shard becomes the public ErrShardFailed",
			in:   &engine.SMPanicError{ShardID: 1, ReplicaID: 1, Value: "apply"},
			want: ErrShardFailed,
		},
		{
			// The counterpart of the row above: a state machine that RETURNS an
			// error is not remapped. Its *engine.ApplyError wraps the state
			// machine's own error, which is strictly more informative than the
			// sentinel, so translateSnapshotError passes it through.
			name: "an apply failure keeps the state machine's own error",
			in:   &engine.ApplyError{ShardID: 1, ReplicaID: 1, Err: errApplyRowInner},
			want: errApplyRowInner,
			check: func(t *testing.T, got error) {
				if errors.Is(got, ErrShardFailed) {
					t.Errorf("got %v, which now matches ErrShardFailed; "+
						"*engine.ApplyError has no Is method and must not be "+
						"remapped onto the panic sentinel", got)
				}
			},
		},
		{
			name: "corrupt frames become the public ErrSnapshotCorrupt",
			in:   engine.ErrSnapshotCorrupt,
			want: ErrSnapshotCorrupt,
		},
		{
			name: "an oversized artifact keeps its limit",
			in:   &engine.SnapshotTooLargeError{ShardID: 4, ReplicaID: 2, Size: 99, Limit: 64},
			want: ErrSnapshotTooLarge,
			check: func(t *testing.T, got error) {
				var tooLarge *SnapshotTooLargeError
				if !errors.As(got, &tooLarge) {
					t.Fatalf("got %T, want *quicraft.SnapshotTooLargeError", got)
				}
				if tooLarge.ShardID != 4 || tooLarge.MaxSize != 64 {
					t.Errorf("translated = %+v, want ShardID 4 and MaxSize 64; the "+
						"limit is the only actionable field and must survive", tooLarge)
				}
			},
		},
		{
			name: "a failed recovery becomes ErrSnapshotRecoveryFailed",
			in: &engine.SnapshotError{
				ShardID: 9, ReplicaID: 3, Save: false, Err: errors.New("truncated artifact"),
			},
			want: ErrSnapshotRecoveryFailed,
			check: func(t *testing.T, got error) {
				var failed *SnapshotFailedError
				if !errors.As(got, &failed) {
					t.Fatalf("got %T, want *SnapshotFailedError", got)
				}
				if !failed.Recovery || failed.ShardID != 9 || failed.ReplicaID != 3 {
					t.Errorf("translated = %+v, want a recovery failure for shard 9 replica 3", failed)
				}
				if failed.Err == nil || !strings.Contains(failed.Error(), "truncated artifact") {
					t.Errorf("translated error %q dropped the underlying cause", failed.Error())
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := translateSnapshotError(tt.in)
			if tt.want == nil {
				if got != nil {
					t.Fatalf("translateSnapshotError(nil) = %v, want nil", got)
				}
				return
			}
			if !errors.Is(got, tt.want) {
				t.Fatalf("translateSnapshotError(%v) = %v (%T), want it to match %v",
					tt.in, got, got, tt.want)
			}
			if tt.check != nil {
				tt.check(t, got)
			}
		})
	}
}

// TestTranslateSnapshotError_SaveFailureClaimsNoRecoverySentinel pins the
// deliberate asymmetry: a SAVE failure must NOT match
// ErrSnapshotRecoveryFailed. The two are opposite directions — one writes an
// artifact, the other installs one over the live state machine — and a caller
// that retries a failed save on being told "recovery failed" would be acting on
// the wrong half of the API.
func TestTranslateSnapshotError_SaveFailureClaimsNoRecoverySentinel(t *testing.T) {
	got := translateSnapshotError(&engine.SnapshotError{
		ShardID: 2, ReplicaID: 1, Save: true, Err: errors.New("disk full"),
	})
	if errors.Is(got, ErrSnapshotRecoveryFailed) {
		t.Fatalf("a save failure matched ErrSnapshotRecoveryFailed: %v", got)
	}
	var failed *SnapshotFailedError
	if !errors.As(got, &failed) {
		t.Fatalf("got %T, want *SnapshotFailedError so the caller has a public type", got)
	}
	if failed.Recovery {
		t.Error("Recovery is true for a save failure")
	}
}

// TestTranslateSnapshotError_BarrierSentinelsPassThrough pins that the
// translation does NOT re-map the failures the engine already fails with by
// PUBLIC identity. Those sentinels are declared in the engine precisely so the
// engine paths and the host paths produce one identity for one condition;
// re-mapping them here would undo that.
func TestTranslateSnapshotError_BarrierSentinelsPassThrough(t *testing.T) {
	for _, sentinel := range []error{
		ErrShardBarrierUnresolved,
		ErrSnapshotBarrierSealed,
		ErrSnapshotEncryptedBarrierUnavailable,
	} {
		if got := translateSnapshotError(sentinel); !errors.Is(got, sentinel) {
			t.Errorf("translateSnapshotError(%v) = %v, which no longer matches the "+
				"sentinel the engine failed with", sentinel, got)
		}
		// And through the engine's own wrapper, which is how they really arrive.
		wrapped := &engine.SnapshotError{ShardID: 1, ReplicaID: 1, Save: true, Err: sentinel}
		if got := translateSnapshotError(wrapped); !errors.Is(got, sentinel) {
			t.Errorf("translateSnapshotError(wrapped %v) = %v, which no longer "+
				"matches the sentinel", sentinel, got)
		}
	}
}

// TestTranslateSnapshotSubmitError pins that the pool's two refusals stay
// tellable apart. Collapsing a "no worker was free" into
// ErrSnapshotInProgress sends an operator looking for a concurrent snapshot on
// a shard that has none.
func TestTranslateSnapshotSubmitError(t *testing.T) {
	busy := translateSnapshotSubmitError(&engine.SnapshotPoolBusyError{ShardID: 3})
	if !errors.Is(busy, ErrBusy) {
		t.Errorf("a full snapshot pool = %v, want ErrBusy", busy)
	}
	if errors.Is(busy, ErrSnapshotInProgress) {
		t.Errorf("a full snapshot pool reported as ErrSnapshotInProgress (%v); "+
			"nothing is snapshotting this shard", busy)
	}

	dup := translateSnapshotSubmitError(&engine.SnapshotDuplicateError{ShardID: 3})
	if !errors.Is(dup, ErrSnapshotInProgress) {
		t.Errorf("a duplicate request = %v, want ErrSnapshotInProgress", dup)
	}
	if errors.Is(dup, ErrBusy) {
		t.Errorf("a duplicate request reported as ErrBusy (%v)", dup)
	}
}

// ---------------------------------------------------------------------------
// Config changes are refused while draining
// ---------------------------------------------------------------------------

// TestRequestConfigChange_DrainingRejected pins that a QUORUM CHANGE is
// refused once the host has entered drain.
//
// Propose, ReadIndex and the session path all refuse here; the config-change
// funnel did not, so a membership change submitted during drain was either
// accepted and abandoned when the drain deadline expired, or completed with
// ErrClosed by the Close sweep. Either way the caller could not tell whether
// the voting set had changed — the one question a config change exists to
// answer.
func TestRequestConfigChange_DrainingRejected(t *testing.T) {
	h := newTestHost(t)

	members := map[uint64]string{1: "127.0.0.1:5000"}
	scfg := testShardConfig(1, 1)
	if err := h.StartShard(members, false, testCreateFunc, scfg); err != nil {
		t.Fatalf("StartShard failed: %v", err)
	}
	waitForLeader(t, h, 1)

	h.draining.Store(true)

	entryPoints := []struct {
		name string
		call func() error
	}{
		{"RequestAddNode", func() error {
			_, err := h.RequestAddNode(context.Background(), 1, 2, "127.0.0.1:5001", 0)
			return err
		}},
		{"RequestRemoveNode", func() error {
			_, err := h.RequestRemoveNode(context.Background(), 1, 2, 0)
			return err
		}},
		{"RequestAddObserver", func() error {
			_, err := h.RequestAddObserver(context.Background(), 1, 3, "127.0.0.1:5002", 0)
			return err
		}},
		{"RequestAddWitness", func() error {
			_, err := h.RequestAddWitness(context.Background(), 1, 4, "127.0.0.1:5003", 0)
			return err
		}},
	}
	for _, ep := range entryPoints {
		t.Run(ep.name, func(t *testing.T) {
			if err := ep.call(); !errors.Is(err, ErrDraining) {
				t.Fatalf("%s during drain = %v, want ErrDraining: a draining host "+
					"accepts no new work, and a membership change it cannot finish "+
					"leaves the caller unable to tell whether the quorum moved",
					ep.name, err)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// OnApplied survives a short results slice
// ---------------------------------------------------------------------------

// TestOnApplied_ShortResultsSliceDoesNotPanic pins that a results slice
// shorter than entries is contained.
//
// OnApplied indexed results[i] while ranging entries. The apply worker slices
// results to len(entries) before calling, so a short slice is an engine
// invariant violation — but the consequence of indexing past it is a panic
// INSIDE the apply worker's callback, which the worker recovers as a worker
// panic and which therefore costs every shard that worker serves, not the one
// shard with the bad batch. lastApplied must still advance, because the
// entries were applied whatever the results slice says.
func TestOnApplied_ShortResultsSliceDoesNotPanic(t *testing.T) {
	h := newTestHost(t)
	addTestNodeState(h, 1, 1)
	ns := testLoadNode(h, 1)
	ns.requestPool = NewRequestStatePool()

	cb := &hostEngineCallback{h: h}
	entries := []proto.Entry{
		{Index: 10, Key: 1},
		{Index: 11, Key: 2},
		{Index: 12, Key: 3},
	}

	// A pending proposal for EVERY entry. Without one the results index is
	// never evaluated, so a test that skips this registration passes against
	// the unguarded code and proves nothing.
	states := make([]*RequestState, len(entries))
	for i, entry := range entries {
		rs := newRequestState(ns.requestPool, entry.Key, time.Time{}, false)
		rs.acquireRef()
		ns.pendingProposals.Store(entry.Key, &pendingProposal{rs: rs})
		h.addPendingOp()
		states[i] = rs
	}

	func() {
		defer func() {
			if r := recover(); r != nil {
				t.Fatalf("OnApplied panicked on a short results slice: %v. The panic "+
					"happens inside the apply worker, so it costs every shard in "+
					"that worker's partition, not just the one with the bad batch", r)
			}
		}()
		cb.OnApplied(1, entries, []sm.Result{{Value: 100}})
	}()

	// The one entry that HAD a result is completed with it.
	select {
	case got := <-states[0].ResultC():
		if got.Value != 100 {
			t.Errorf("first proposal completed with Value %d, want 100", got.Value)
		}
	default:
		t.Error("the proposal that had a result was not completed")
	}

	// lastApplied still advances: the entries were applied whatever the
	// results slice carried, and stalling it would wedge every pending read.
	if got := ns.lastApplied.Load(); got != 12 {
		t.Errorf("lastApplied = %d, want 12", got)
	}
}

// ---------------------------------------------------------------------------
// Cancellation of the bulk snapshot copies
// ---------------------------------------------------------------------------

// cancelOnFirstWrite cancels a context the first time it is written to, so a
// producer's SECOND chunk observes the cancellation.
type cancelOnFirstWrite struct {
	cancel  context.CancelFunc
	written int
	buf     bytes.Buffer
}

func (c *cancelOnFirstWrite) Write(p []byte) (int, error) {
	c.written++
	if c.written == 1 {
		c.cancel()
	}
	return c.buf.Write(p)
}

// TestExportSnapshot_EncryptedPathHonoursContext pins that an ENCRYPTED export
// stops when the caller gives up.
//
// ExportSnapshot has two bodies. The plaintext one copied through
// copyWithContext and stopped promptly; the encrypted one ran
// decryptSnapshotFrames, which has no context parameter, and so read,
// authenticated and re-emitted every frame of a multi-gigabyte artifact with
// nothing looking at ctx — the LONGER of the two paths was the one that could
// not be interrupted.
func TestExportSnapshot_EncryptedPathHonoursContext(t *testing.T) {
	cfg := testHostConfig(t)
	db := newMockLogDB()
	barrier := createTestBarrier(t)
	h, err := NewHost(cfg, WithLogDB(db), WithoutTransport(), WithBarrier(barrier))
	if err != nil {
		t.Fatalf("NewHost failed: %v", err)
	}
	defer h.Close()

	members := map[uint64]string{1: "127.0.0.1:5000"}
	scfg := testShardConfig(1, 1)
	if startErr := h.StartShard(members, false, testCreateFunc, scfg); startErr != nil {
		t.Fatalf("StartShard failed: %v", startErr)
	}

	snapDir := filepath.Join(t.TempDir(), "snap-encrypted")
	if mkErr := os.MkdirAll(snapDir, 0o750); mkErr != nil {
		t.Fatalf("MkdirAll failed: %v", mkErr)
	}
	plaintext := []byte("a state machine image that the caller abandons mid-export")
	writeEncryptedSnapshotFileFor(t, barrier, filepath.Join(snapDir, "snapshot.dat"),
		plaintext, 1, 1, 700)
	db.SaveSnapshot(1, 1, logdb.Snapshot{Index: 700, Filepath: snapDir, Epoch: 1})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	w := &cancelOnFirstWrite{cancel: cancel}

	exportErr := h.ExportSnapshot(ctx, 1, w)
	if !errors.Is(exportErr, ErrCanceled) {
		t.Fatalf("ExportSnapshot on a cancelled context = %v, want ErrCanceled. "+
			"The encrypted path ignored ctx entirely, so an abandoned export of a "+
			"multi-gigabyte artifact ran to completion", exportErr)
	}
	if bytes.Contains(w.buf.Bytes(), plaintext) {
		t.Error("the whole plaintext reached the writer despite the cancellation")
	}
}

// cancelAfterFirstRead hands out data once and cancels the context, so a copy
// loop that checks its context between chunks stops on the next iteration.
type cancelAfterFirstRead struct {
	cancel context.CancelFunc
	reads  int
	chunk  []byte
}

func (r *cancelAfterFirstRead) Read(p []byte) (int, error) {
	r.reads++
	if r.reads > 1 {
		// A copy that reached here ignored the cancellation the first read
		// installed; keep feeding it so the assertion is about the error, not
		// about running out of data.
		n := copy(p, r.chunk)
		return n, nil
	}
	r.cancel()
	n := copy(p, r.chunk)
	return n, nil
}

// TestWriteImportedSnapshotData_HonoursContext pins that the import's bulk
// write stops when the caller gives up.
//
// This is the single longest step of an import — it writes the whole artifact,
// which for a disaster-recovery restore is measured in gigabytes — and it used
// io.Copy, which cannot be interrupted at all. An operator who spotted a wrong
// artifact had to wait for it to finish being written before the staging
// directory was discarded.
func TestWriteImportedSnapshotData_HonoursContext(t *testing.T) {
	h := &Host{}
	dataPath := filepath.Join(t.TempDir(), "snapshot.dat")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	r := &cancelAfterFirstRead{cancel: cancel, chunk: bytes.Repeat([]byte("x"), 4096)}

	_, _, err := h.writeImportedSnapshotData(ctx, osSnapshotFS{}, 1, 1, 1, dataPath, nil, r, nil)
	if !errors.Is(err, ErrCanceled) {
		t.Fatalf("writeImportedSnapshotData on a cancelled context = %v, want "+
			"ErrCanceled; the copy ran to completion instead", err)
	}
	if r.reads > 4 {
		t.Errorf("the copy performed %d reads after the cancellation; it is not "+
			"checking the context between chunks", r.reads)
	}
}

var _ io.Reader = (*cancelAfterFirstRead)(nil)

// ---------------------------------------------------------------------------
// Session client IDs are never the reserved "no session" value
// ---------------------------------------------------------------------------

// fixedEntropy hands out a scripted sequence of 8-byte draws, then repeats the
// last one forever.
type fixedEntropy struct {
	draws [][]byte
	n     int
}

func (e *fixedEntropy) Read(p []byte) (int, error) {
	d := e.draws[min(e.n, len(e.draws)-1)]
	e.n++
	return copy(p, d), nil
}

// TestNewClientID_RejectsTheReservedZero pins that a minted session never
// carries proto.NotSessionManagedClientID.
//
// Zero is the value that marks an entry as carrying NO session, and the failure
// it produced was silent in both directions: the registration entry fails
// Entry.IsNewSessionRequest, so the state machine skips it and returns an empty
// result, and NewSession's own check is `result.Value != clientID` — which
// 0 != 0 satisfies. The call therefore REPORTED SUCCESS and handed back a
// session whose every later proposal is unsessioned, silently turning off
// at-most-once delivery for that client. RegisterSession already rejects a zero
// client ID; the minting side did not.
func TestNewClientID_RejectsTheReservedZero(t *testing.T) {
	zero := make([]byte, 8)
	good := []byte{1, 0, 0, 0, 0, 0, 0, 0}

	// Two degenerate draws, then a usable one.
	src := &fixedEntropy{draws: [][]byte{zero, zero, good}}
	id, err := newClientID(src)
	if err != nil {
		t.Fatalf("newClientID: %v", err)
	}
	if id == proto.NotSessionManagedClientID {
		t.Fatal("newClientID returned the reserved no-session client ID; the " +
			"session it mints is a no-op session reported as a registered one")
	}
	if id != 1 {
		t.Fatalf("newClientID = %d, want 1 (the first usable draw)", id)
	}
	if src.n != 3 {
		t.Fatalf("newClientID took %d draws, want 3: it is not re-rolling", src.n)
	}
}

// TestNewClientID_FailsOnADegenerateEntropySource pins the bound on the
// re-roll. An entropy source that returns the same rejected value forever is
// broken, and looping on it would hang a caller inside NewSession; minting
// from it anyway would hand out predictable identifiers.
func TestNewClientID_FailsOnADegenerateEntropySource(t *testing.T) {
	src := &fixedEntropy{draws: [][]byte{make([]byte, 8)}}
	id, err := newClientID(src)
	if !errors.Is(err, ErrEntropyDegenerate) {
		t.Fatalf("newClientID on a stuck source = (%d, %v), want ErrEntropyDegenerate", id, err)
	}
	if id != 0 {
		t.Errorf("newClientID returned %d alongside an error", id)
	}
	if src.n != clientIDRerollLimit {
		t.Errorf("newClientID took %d draws, want the %d-draw bound",
			src.n, clientIDRerollLimit)
	}
}

// TestGetNewSession_MintsAUsableSession is the end-to-end companion: the
// session NewSession hands back must actually be session-managed, so a
// proposal made with it is deduplicated rather than silently unsessioned.
func TestGetNewSession_MintsAUsableSession(t *testing.T) {
	h := newTestHost(t)
	members := map[uint64]string{1: "127.0.0.1:5000"}
	if err := h.StartShard(members, false, testCreateFunc, testShardConfig(1, 1)); err != nil {
		t.Fatalf("StartShard failed: %v", err)
	}
	waitForLeader(t, h, 1)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	session, err := h.NewSession(ctx, 1)
	if err != nil {
		t.Fatalf("NewSession: %v", err)
	}
	if session.ClientID() == proto.NotSessionManagedClientID {
		t.Fatal("NewSession reported SUCCESS for a session carrying the " +
			"reserved no-session client ID: every proposal made with it is " +
			"unsessioned, so at-most-once delivery is off for this client")
	}
	if session.IsNoOPSession() {
		t.Fatal("NewSession returned a no-op session")
	}
}
