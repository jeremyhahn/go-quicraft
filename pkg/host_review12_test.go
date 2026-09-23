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
	"io/fs"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jeremyhahn/go-quicraft/pkg/internal/engine"
	"github.com/jeremyhahn/go-quicraft/pkg/logdb"
	"github.com/jeremyhahn/go-quicraft/pkg/proto"
)

// ---------------------------------------------------------------------------
// Session limits: MaxTotalSessions and MaxSessionsPerClient are not transposed
// ---------------------------------------------------------------------------

// TestStartShard_SessionLimitsAreNotTransposed pins the argument ORDER of
// rsm.NewStateMachine, which is invisible at the call site because both limits
// are uint64.
//
// The transposed wiring capped a shard at MaxSessionsPerClient concurrent
// sessions (default 128 instead of 16384) while letting a single client pin
// MaxTotalSessions cached results. The limits are chosen so each one fails a
// different assertion: with the arguments swapped the THIRD GetNewSession is
// rejected and MaxResponses reports 3.
func TestStartShard_SessionLimitsAreNotTransposed(t *testing.T) {
	h := newTestHost(t)

	scfg := testShardConfig(1, 1)
	scfg.MaxTotalSessions = 3
	scfg.MaxSessionsPerClient = 2
	if err := h.StartShard(map[uint64]string{1: "127.0.0.1:5000"}, false,
		testCreateFunc, scfg); err != nil {
		t.Fatalf("StartShard failed: %v", err)
	}
	waitForLeader(t, h, 1)

	// The per-session response cache is the maxResponses argument.
	if got := testLoadNode(h, 1).rsm.Sessions().MaxResponses(); got != 2 {
		t.Errorf("session manager MaxResponses = %d, want 2 (MaxSessionsPerClient)", got)
	}

	// The shard-wide session count is the maxTotalSessions argument: three
	// registrations fit, the fourth does not.
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	for i := 1; i <= 3; i++ {
		if _, err := h.GetNewSession(ctx, 1); err != nil {
			t.Fatalf("GetNewSession #%d failed: %v (MaxTotalSessions=3 must admit it)", i, err)
		}
	}
	// The over-limit registration applies to an empty result, which
	// GetNewSession reports as a registration failure.
	_, err := h.GetNewSession(ctx, 1)
	var sessErr *SessionError
	if !errors.As(err, &sessErr) || sessErr.Op != "register" {
		t.Fatalf("GetNewSession #4 error = %v, want a *SessionError from register "+
			"(MaxTotalSessions=3 must reject it)", err)
	}
}

// ---------------------------------------------------------------------------
// Recovered term: an on-disk state machine must never seed term 0
// ---------------------------------------------------------------------------

// TestResolveRecoveredTerm covers the three outcomes of resolving the term for
// the applied index an on-disk state machine reports from Open(). Seeding 0
// stamps term 0 on the next snapshot record, and a follower installing it
// treats every candidate as up to date (isUpToDate against lastTerm() == 0) —
// a Leader Completeness violation — so the unresolvable case must fail closed.
func TestResolveRecoveredTerm(t *testing.T) {
	const shardID, replicaID uint64 = 1, 1

	t.Run("from the log", func(t *testing.T) {
		h := newTestHost(t)
		lr := logdb.NewLogReader(shardID, replicaID, h.logdb)
		if err := h.logdb.SaveState([]logdb.Update{{
			ShardID:   shardID,
			ReplicaID: replicaID,
			Entries: []logdb.Entry{
				{Index: 1, Term: 4},
				{Index: 2, Term: 7},
			},
		}}); err != nil {
			t.Fatalf("SaveState failed: %v", err)
		}
		if _, err := h.recoverLogReader(lr, shardID, replicaID); err != nil {
			t.Fatalf("recoverLogReader failed: %v", err)
		}

		term, err := h.resolveRecoveredTerm(lr, shardID, replicaID, 2)
		if err != nil {
			t.Fatalf("resolveRecoveredTerm failed: %v", err)
		}
		if term != 7 {
			t.Errorf("term = %d, want 7", term)
		}
	})

	t.Run("from the snapshot record when the entry is compacted", func(t *testing.T) {
		h := newTestHost(t)
		lr := logdb.NewLogReader(shardID, replicaID, h.logdb)
		if err := h.logdb.SaveSnapshot(shardID, replicaID, logdb.Snapshot{
			Index: 40,
			Term:  9,
		}); err != nil {
			t.Fatalf("SaveSnapshot failed: %v", err)
		}

		term, err := h.resolveRecoveredTerm(lr, shardID, replicaID, 40)
		if err != nil {
			t.Fatalf("resolveRecoveredTerm failed: %v", err)
		}
		if term != 9 {
			t.Errorf("term = %d, want 9", term)
		}
	})

	t.Run("fails closed when neither source answers", func(t *testing.T) {
		h := newTestHost(t)
		lr := logdb.NewLogReader(shardID, replicaID, h.logdb)
		// A snapshot at a DIFFERENT index carries a term that does not belong
		// to the recovered state, so it must not be adopted.
		if err := h.logdb.SaveSnapshot(shardID, replicaID, logdb.Snapshot{
			Index: 11,
			Term:  9,
		}); err != nil {
			t.Fatalf("SaveSnapshot failed: %v", err)
		}

		term, err := h.resolveRecoveredTerm(lr, shardID, replicaID, 40)
		if !errors.Is(err, ErrRecoveredTermUnknown) {
			t.Fatalf("resolveRecoveredTerm error = %v, want ErrRecoveredTermUnknown", err)
		}
		if term != 0 {
			t.Errorf("term = %d, want 0 alongside the error", term)
		}
	})
}

// ---------------------------------------------------------------------------
// Stat must fail closed on the recovery and import paths
// ---------------------------------------------------------------------------

// errStatInjected is the injected Stat failure.
var errStatInjected = errors.New("review12 test: stat failed")

// statFailFS fails Stat for snapshot.dat and behaves like the real filesystem
// for everything else, modelling the transient stat error (EIO, a racing
// unlink, a stale NFS handle) that both fail-open sites swallowed.
type statFailFS struct {
	osSnapshotFS
}

func (f statFailFS) Stat(name string) (fs.FileInfo, error) {
	if strings.HasSuffix(name, "snapshot.dat") {
		return nil, errStatInjected
	}
	return f.osSnapshotFS.Stat(name)
}

var _ snapshotFS = statFailFS{}

// TestRecoverLogReader_StatFailureFailsClosed asserts that a snapshot record
// whose artifact cannot be measured stops recovery instead of producing a
// LogReader snapshot with an EMPTY Filepath. An empty Filepath is how the send
// path recognises the one legitimate metadata-only snapshot, so the fail-open
// turned an unreadable artifact into a follower-state eraser.
func TestRecoverLogReader_StatFailureFailsClosed(t *testing.T) {
	const shardID, replicaID uint64 = 4, 2

	cfg := testHostConfig(t)
	h, err := NewHost(cfg, WithMemoryLogDB(), WithoutTransport(),
		withSnapshotFS(statFailFS{}), WithoutEncryption())
	if err != nil {
		t.Fatalf("NewHost failed: %v", err)
	}
	defer h.Close()

	snapDir := t.TempDir()
	if writeErr := os.WriteFile(filepath.Join(snapDir, "snapshot.dat"),
		[]byte("artifact"), 0o600); writeErr != nil {
		t.Fatalf("WriteFile failed: %v", writeErr)
	}
	if ssErr := h.logdb.SaveSnapshot(shardID, replicaID, logdb.Snapshot{
		Index:    12,
		Term:     3,
		Filepath: snapDir,
	}); ssErr != nil {
		t.Fatalf("SaveSnapshot failed: %v", ssErr)
	}

	lr := logdb.NewLogReader(shardID, replicaID, h.logdb)
	if _, recErr := h.recoverLogReader(lr, shardID, replicaID); !errors.Is(recErr, errStatInjected) {
		t.Fatalf("recoverLogReader error = %v, want the injected stat failure", recErr)
	}
	if got := lr.Snapshot(); got.Index != 0 {
		t.Errorf("LogReader adopted snapshot %+v; recovery must not publish an "+
			"unmeasured artifact", got)
	}
}

// TestImportSnapshot_StatFailureFailsClosed asserts the same fail-closed rule
// on the import path: a FileSize of 0 recorded for a directory that does hold a
// real snapshot.dat makes this replica stream a metadata-only snapshot once it
// becomes leader. The staging directory must be discarded so the operator can
// retry the restore.
func TestImportSnapshot_StatFailureFailsClosed(t *testing.T) {
	const shardID, replicaID uint64 = 7, 3
	const index uint64 = 900

	cfg := testHostConfig(t)
	h, err := NewHost(cfg, WithMemoryLogDB(), WithoutTransport(),
		withSnapshotFS(statFailFS{}), WithoutEncryption())
	if err != nil {
		t.Fatalf("NewHost failed: %v", err)
	}
	defer h.Close()

	if bsErr := h.logdb.SaveBootstrap(shardID, replicaID, logdb.Bootstrap{
		Addresses: map[uint64]string{replicaID: "127.0.0.1:5000"},
	}); bsErr != nil {
		t.Fatalf("SaveBootstrap failed: %v", bsErr)
	}

	artifact := buildSnapshotArtifact(t, index, 5, 0, proto.Membership{},
		snapshotEnvelope([]byte("RESTORE")))
	importErr := h.ImportSnapshot(context.Background(), shardID, bytes.NewReader(artifact))
	if !errors.Is(importErr, errStatInjected) {
		t.Fatalf("ImportSnapshot error = %v, want the injected stat failure", importErr)
	}

	if ss, ssErr := h.logdb.GetSnapshot(shardID, replicaID); ssErr == nil && ss.Index != 0 {
		t.Fatalf("a failed import recorded snapshot %+v in LogDB", ss)
	}
	// Neither the staging directory nor a published one may survive.
	base := shardSnapshotBaseDir(cfg.NodeHostDir, "", shardID)
	replicaDir := filepath.Dir(engine.SnapshotSaveDir(base, shardID, replicaID, index))
	entries, readErr := os.ReadDir(replicaDir)
	if readErr != nil && !os.IsNotExist(readErr) {
		t.Fatalf("ReadDir(%s): %v", replicaDir, readErr)
	}
	if len(entries) != 0 {
		t.Fatalf("failed import left %d entries under %s; the staging directory "+
			"must be discarded", len(entries), replicaDir)
	}
}

// TestRecoverLogReader_MissingArtifactStillRecordsThePath is the regression
// test for the follower-wiping path.
//
// A snapshot record whose snapshot.dat is ABSENT must still recover as "a
// snapshot with a data file". Clearing Filepath instead produced
// {Index: N, Filepath: "", FileSize: 0}, and an EMPTY Filepath is how the send
// path recognises the one legitimate metadata-only transfer: raft.sendSnapshot
// took it straight from raftLog.snapshot() and forEachSnapshotChunk emitted a
// single empty chunk, erasing the state of every follower this replica streamed
// to. StartShard's deliberate tolerance of a compacted-away artifact for an
// on-disk state machine must not leak into the send path.
func TestRecoverLogReader_MissingArtifactStillRecordsThePath(t *testing.T) {
	const shardID, replicaID uint64 = 9, 4

	h := newTestHost(t)

	// The directory exists; the artifact inside it does not — exactly the state
	// TestStartShard_OnDiskSM_SeedsAppliedMarker builds deliberately.
	snapDir := t.TempDir()
	if ssErr := h.logdb.SaveSnapshot(shardID, replicaID, logdb.Snapshot{
		Index:    25,
		Term:     3,
		Filepath: snapDir,
	}); ssErr != nil {
		t.Fatalf("SaveSnapshot failed: %v", ssErr)
	}

	lr := logdb.NewLogReader(shardID, replicaID, h.logdb)
	if _, recErr := h.recoverLogReader(lr, shardID, replicaID); recErr != nil {
		t.Fatalf("recoverLogReader must tolerate a missing artifact: %v", recErr)
	}

	snap := lr.Snapshot()
	if snap.Index != 25 {
		t.Fatalf("LogReader snapshot index = %d, want 25", snap.Index)
	}
	want := filepath.Join(snapDir, "snapshot.dat")
	if snap.Filepath != want {
		t.Fatalf("LogReader snapshot Filepath = %q, want %q; an EMPTY Filepath is "+
			"the metadata-only marker the send path acts on", snap.Filepath, want)
	}

	// The send path must now FAIL rather than ship an empty chunk.
	msg := proto.Message{
		Type:      proto.InstallSnapshot,
		ShardID:   shardID,
		From:      replicaID,
		To:        7,
		Snapshot:  snap,
		ReplicaID: replicaID,
	}
	var chunks []proto.SnapshotChunk
	err := forEachSnapshotChunk(osSnapshotFS{}, msg, func(c proto.SnapshotChunk) error {
		chunks = append(chunks, c)
		return nil
	})
	var sendErr *SnapshotSendError
	if !errors.As(err, &sendErr) {
		t.Fatalf("forEachSnapshotChunk err = %v (%T), want *SnapshotSendError", err, err)
	}
	if !errors.Is(err, os.ErrNotExist) {
		t.Errorf("send error does not carry the missing-file cause: %v", err)
	}
	if len(chunks) != 0 {
		t.Fatalf("FOLLOWER STATE ERASURE: the send path emitted %d chunk(s) for a "+
			"snapshot whose artifact is missing; the receiver recovers from it and "+
			"marks every index through %d applied", len(chunks), snap.Index)
	}
}

// TestStartShard_OnDiskSM_AppliedIndexOutrunsCommit asserts that an on-disk
// state machine whose Open() reports an applied index PAST the recovered
// HardState.Commit still starts.
//
// That is a lost state fsync, and SetProcessedIndex exists precisely to rescue
// it by clamping to the commit index. Resolving the recovered TERM against the
// raw (unclamped) index instead bricked the node — the index is by definition
// absent from the log, so the fail-closed term lookup rejected the start — and
// paired the clamped index with the unclamped index's term besides.
func TestStartShard_OnDiskSM_AppliedIndexOutrunsCommit(t *testing.T) {
	cfg := testHostConfig(t)
	db := newMockLogDB()
	h, err := NewHost(cfg, WithLogDB(db), WithoutTransport(), WithoutEncryption())
	if err != nil {
		t.Fatalf("NewHost failed: %v", err)
	}
	defer h.Close()

	// The log holds 1..50 and commits 50; the state machine claims 100. Index 1
	// carries the shard's founding configuration, as config-in-log commits it
	// before any other entry; a restart fixture without it models a damaged log.
	entries := make([]logdb.Entry, 0, 50)
	entries = append(entries, bootstrapLogEntry(t, 4, map[uint64]string{1: "127.0.0.1:5000"}))
	for i := uint64(2); i <= 50; i++ {
		entries = append(entries, logdb.Entry{Index: i, Term: 4})
	}
	if saveErr := db.SaveState([]logdb.Update{{
		ShardID:   1,
		ReplicaID: 1,
		State:     logdb.State{Term: 4, Vote: 1, Commit: 50},
		Entries:   entries,
	}}); saveErr != nil {
		t.Fatalf("SaveState failed: %v", saveErr)
	}

	diskSM := &recordingOnDiskSM{openLastApplied: 100}
	diskSM.kv = map[string]uint64{"k": 100}
	create := func(uint64, uint64) interface{} { return diskSM }

	if startErr := h.StartShard(map[uint64]string{1: "127.0.0.1:5000"}, false,
		create, testShardConfig(1, 1)); startErr != nil {
		t.Fatalf("StartShard failed for an SM whose applied index outruns the "+
			"recovered commit index; the clamp exists to rescue exactly this: %v", startErr)
	}

	// The markers adopt the CLAMPED index, and the term recorded beside it is
	// that index's term.
	ns := testLoadNode(h, 1)
	if got := ns.lastApplied.Load(); got != 50 {
		t.Errorf("ns.lastApplied = %d, want 50 (clamped to the recovered commit index)", got)
	}
	engNode := h.engine.GetNode(1)
	if engNode == nil {
		t.Fatal("shard is not loaded in the engine")
	}
	if got := engNode.LastAppliedTerm(); got != 4 {
		t.Errorf("engine lastAppliedTerm = %d, want 4 (the CLAMPED index's term)", got)
	}
}

// ---------------------------------------------------------------------------
// Deadline-less pending requests must still be sweepable
// ---------------------------------------------------------------------------

// TestPendingRequests_DeadlinelessContextIsClamped asserts that a request
// registered under a context with NO deadline still carries one.
//
// The sweeper skips zero deadlines by design, so a zero-deadline entry was
// never reclaimed: its RequestState never returned to the pool and pendingOps
// never fell back to zero, which made EVERY later Drain() burn the full
// ShutdownTimeout and return ErrShutdownTimeout.
func TestPendingRequests_DeadlinelessContextIsClamped(t *testing.T) {
	h := newTestHost(t)
	if err := h.StartShard(map[uint64]string{1: "127.0.0.1:5000"}, false,
		testCreateFunc, testShardConfig(1, 1)); err != nil {
		t.Fatalf("StartShard failed: %v", err)
	}
	waitForLeader(t, h, 1)
	ns := testLoadNode(h, 1)

	before := time.Now()
	rs, err := h.Propose(context.Background(), 1, []byte("cmd"))
	if err != nil {
		t.Fatalf("Propose failed: %v", err)
	}
	defer rs.Release()
	after := time.Now()

	deadline := rs.Deadline()
	if deadline.IsZero() {
		t.Fatal("a proposal made with context.Background() carries no deadline, " +
			"so the sweeper can never reclaim it")
	}
	// The bound must be TIGHT. [before, after+timeout] admits any deadline at
	// all, including one a nanosecond out, so only the IsZero check above would
	// be doing work: the clamp is applied at registration, which happened
	// between before and after, so the deadline lies in
	// [before+timeout, after+timeout].
	if deadline.Before(before.Add(defaultPendingRequestTimeout)) ||
		deadline.After(after.Add(defaultPendingRequestTimeout)) {
		t.Errorf("deadline %v is not %v from the registration instant (call spanned %v..%v)",
			deadline, defaultPendingRequestTimeout, before, after)
	}

	// The map entry the sweeper actually reads carries it too.
	ns.pendingProposals.Range(func(_, value any) bool {
		pp, ok := value.(*pendingProposal)
		if ok && pp.deadline.IsZero() {
			t.Error("pendingProposal registered with a zero deadline")
		}
		return true
	})

	// Reads and snapshots register through their own sites.
	readRS, err := h.ReadIndex(context.Background(), 1)
	if err != nil {
		t.Fatalf("ReadIndex failed: %v", err)
	}
	defer readRS.Release()
	if readRS.Deadline().IsZero() {
		t.Error("ReadIndex registered a zero deadline for a deadline-less context")
	}

	// A snapshot gets the SNAPSHOT backstop, not the commit-path one. Reusing
	// the 5-minute bound expires a caller whose multi-gigabyte state machine is
	// still serializing, and the retry then hits ErrSnapshotInProgress.
	snapBefore := time.Now()
	snapRS, err := h.RequestSnapshot(context.Background(), 1, SnapshotOption{})
	if err != nil {
		t.Fatalf("RequestSnapshot failed: %v", err)
	}
	defer snapRS.Release()
	snapAfter := time.Now()
	snapDeadline := snapRS.Deadline()
	if snapDeadline.IsZero() {
		t.Error("RequestSnapshot registered a zero deadline for a deadline-less context")
	}
	if snapDeadline.Before(snapBefore.Add(defaultPendingSnapshotTimeout)) ||
		snapDeadline.After(snapAfter.Add(defaultPendingSnapshotTimeout)) {
		t.Errorf("snapshot deadline %v is not %v from the registration instant; a "+
			"snapshot must not inherit the %v commit-path bound",
			snapDeadline, defaultPendingSnapshotTimeout, defaultPendingRequestTimeout)
	}

	// A caller-supplied deadline still wins, in both directions.
	tightCtx, tightCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer tightCancel()
	tightRS, err := h.RequestSnapshot(tightCtx, 1, SnapshotOption{})
	if err != nil && !errors.Is(err, ErrSnapshotInProgress) {
		t.Fatalf("RequestSnapshot with a deadline failed: %v", err)
	}
	if tightRS != nil {
		defer tightRS.Release()
		want, _ := tightCtx.Deadline()
		if !tightRS.Deadline().Equal(want) {
			t.Errorf("snapshot deadline = %v, want the caller's %v", tightRS.Deadline(), want)
		}
	}
}

// ---------------------------------------------------------------------------
// Leadership changes must not fail local snapshot work
// ---------------------------------------------------------------------------

// TestOnLeaderUpdated_KeepsPendingSnapshots asserts that a routine election
// leaves an in-flight snapshot request alone. Saving a snapshot is purely
// local: it reads this replica's own state machine and writes its own disk, so
// failing the caller with NotLeaderError while the save keeps running only
// produced a retry that hit ErrSnapshotInProgress.
func TestOnLeaderUpdated_KeepsPendingSnapshots(t *testing.T) {
	h := newTestHost(t)
	addTestNodeState(h, 1, 1)
	ns := testLoadNode(h, 1)
	pool := NewRequestStatePool()

	snapRS := newRequestState(pool, 70, time.Time{}, true)
	ns.pendingSnapshotsMu.Lock()
	ns.pendingSnapshots[70] = snapRS
	ns.pendingSnapshotsMu.Unlock()

	propRS := newRequestState(pool, 71, time.Time{}, true)
	ns.pendingProposals.Store(uint64(71), &pendingProposal{rs: propRS})
	h.addPendingOp()

	// This replica was the leader and has just lost leadership to replica 2.
	ns.isLeader.Store(true)
	cb := &hostEngineCallback{h: h}
	cb.OnLeaderUpdated(1, 2, 10)

	select {
	case result := <-propRS.ResultC():
		if !errors.Is(result.Err, ErrNotLeader) {
			t.Errorf("proposal result.Err = %v, want NotLeaderError", result.Err)
		}
	default:
		t.Error("a pending proposal must be failed on leadership loss")
	}

	select {
	case result := <-snapRS.ResultC():
		t.Fatalf("pending snapshot completed with %v on a leadership change; a "+
			"snapshot save has no leadership dependency", result.Err)
	default:
	}

	// Teardown, where the save really is abandoned, does fail it.
	h.failPendingRequests(ns, ErrClosed)
	select {
	case result := <-snapRS.ResultC():
		if !errors.Is(result.Err, ErrClosed) {
			t.Errorf("snapshot result.Err = %v, want ErrClosed", result.Err)
		}
	default:
		t.Error("teardown must fail pending snapshots")
	}

	propRS.Release()
	snapRS.Release()
}

// ---------------------------------------------------------------------------
// Leader and term must be readable as a consistent pair
// ---------------------------------------------------------------------------

// TestNodeStateLeaderAndTerm_NeverTorn hammers the leader triple with a writer
// while readers sample it. Only the pairs the writer actually published may be
// observed: reading leaderID, term and isLeader as three independent atomics
// let a caller see a (leader, term) combination that never existed and report
// that a replica led in a term it never led.
func TestNodeStateLeaderAndTerm_NeverTorn(t *testing.T) {
	h := newTestHost(t)
	addTestNodeState(h, 1, 1)

	// Replica 1 is us: (1, 10) is the leading pair, (2, 20) the follower pair.
	published := map[uint64]struct {
		term     uint64
		isLeader bool
	}{
		1: {term: 10, isLeader: true},
		2: {term: 20, isLeader: false},
	}

	// Publish one pair up front so a reader can never sample the zero value.
	h.updateNodeLeader(1, 1, 10)

	stop := make(chan struct{})
	var writer, readers sync.WaitGroup

	writer.Add(1)
	go func() {
		defer writer.Done()
		for i := 0; ; i++ {
			select {
			case <-stop:
				return
			default:
			}
			if i%2 == 0 {
				h.updateNodeLeader(1, 1, 10)
			} else {
				h.updateNodeLeader(1, 2, 20)
			}
		}
	}()

	for r := 0; r < 4; r++ {
		readers.Add(1)
		go func() {
			defer readers.Done()
			for i := 0; i < 5000; i++ {
				info, err := h.GetShardInfo(1)
				if err != nil {
					t.Errorf("GetShardInfo: %v", err)
					return
				}
				want, known := published[info.LeaderID]
				if !known {
					t.Errorf("observed unpublished leader %d", info.LeaderID)
					return
				}
				if info.Term != want.term || info.IsLeader != want.isLeader {
					t.Errorf("torn read: leader=%d term=%d isLeader=%v, want term=%d isLeader=%v",
						info.LeaderID, info.Term, info.IsLeader, want.term, want.isLeader)
					return
				}

				leaderID, term, _, err := h.GetLeaderIDAndTerm(1)
				if err != nil {
					t.Errorf("GetLeaderIDAndTerm: %v", err)
					return
				}
				if pair, ok := published[leaderID]; !ok || pair.term != term {
					t.Errorf("torn pair: leader=%d term=%d", leaderID, term)
					return
				}
			}
		}()
	}

	readers.Wait()
	close(stop)
	writer.Wait()
}

// ---------------------------------------------------------------------------
// A pooled read buffer must not hold the state machine read guard
// ---------------------------------------------------------------------------

// TestStaleReadBuf_UnreleasedBufferDoesNotBlockClose asserts that an
// application that never calls release() cannot wedge the shard. The buffer is
// a COPY (the sm.NALookupInto contract), so holding the rsm read guard for its
// lifetime bought nothing while letting APPLICATION code block Close — and,
// once Close queued as a writer, every subsequent Lookup behind it.
func TestStaleReadBuf_UnreleasedBufferDoesNotBlockClose(t *testing.T) {
	want := []byte("pooled-value")
	backing := &bufLookupSM{values: map[string][]byte{"k": want}}
	h := startReadBufShard(t, func(uint64, uint64) interface{} { return backing })

	buf, release, err := h.StaleReadBuf(context.Background(), 1, []byte("k"))
	if err != nil {
		t.Fatalf("StaleReadBuf: %v", err)
	}
	if !bytes.Equal(buf, want) {
		t.Fatalf("StaleReadBuf = %q, want %q", buf, want)
	}

	// Deliberately do NOT release before stopping the shard.
	stopped := make(chan error, 1)
	go func() { stopped <- h.StopShard(1) }()
	select {
	case stopErr := <-stopped:
		if stopErr != nil {
			t.Fatalf("StopShard: %v", stopErr)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("StopShard blocked on an unreleased read buffer: the rsm read " +
			"guard is being held by caller-owned state")
	}

	// The copy survives the close, which is why holding the guard was
	// unnecessary in the first place.
	if !bytes.Equal(buf, want) {
		t.Fatalf("buffer contents changed after Close: %q", buf)
	}
	release()
}

// TestStaleReadBuf_DoubleReleaseIsIdempotent asserts that releasing twice does
// not put the same pooled buffer back twice. Two goroutines would then be
// handed the same backing array and silently overwrite each other's results.
func TestStaleReadBuf_DoubleReleaseIsIdempotent(t *testing.T) {
	backing := &bufLookupSM{values: map[string][]byte{
		"a": []byte("value-a"),
		"b": []byte("value-b"),
	}}
	h := startReadBufShard(t, func(uint64, uint64) interface{} { return backing })

	buf, release, err := h.StaleReadBuf(context.Background(), 1, []byte("a"))
	if err != nil {
		t.Fatalf("StaleReadBuf: %v", err)
	}
	if !bytes.Equal(buf, []byte("value-a")) {
		t.Fatalf("StaleReadBuf = %q, want %q", buf, "value-a")
	}
	release()
	release()

	// Two independent checkouts must not alias. If the double release put the
	// same pointer in the pool twice, the second read overwrites the first.
	first, releaseFirst, err := h.StaleReadBuf(context.Background(), 1, []byte("a"))
	if err != nil {
		t.Fatalf("first StaleReadBuf after double release: %v", err)
	}
	second, releaseSecond, err := h.StaleReadBuf(context.Background(), 1, []byte("b"))
	if err != nil {
		t.Fatalf("second StaleReadBuf after double release: %v", err)
	}
	if !bytes.Equal(first, []byte("value-a")) {
		t.Errorf("first buffer = %q, want %q: the pool handed out one buffer twice",
			first, "value-a")
	}
	if !bytes.Equal(second, []byte("value-b")) {
		t.Errorf("second buffer = %q, want %q", second, "value-b")
	}
	releaseFirst()
	releaseSecond()
}

// ---------------------------------------------------------------------------
// StartShard racing StopShard must not leave a zombie shard
// ---------------------------------------------------------------------------

// TestStartShard_RacingStopShardLeavesNoZombie drives the window between
// StartShard's publication into h.nodes and its LoadNode into the engine.
//
// A StopShard landing there finds the engine holding nothing, so its
// UnloadNode is a no-op: StartShard then published a node into the LIVE engine
// backed by an already-closed state machine, absent from h.nodes and therefore
// unremovable by any later StopShard or Close. The invariant asserted after
// every iteration is that a shard the Host does not know about is not loaded in
// the engine either.
func TestStartShard_RacingStopShardLeavesNoZombie(t *testing.T) {
	h := newTestHost(t)
	members := map[uint64]string{1: "127.0.0.1:5000"}

	for i := 0; i < 50; i++ {
		var wg sync.WaitGroup
		wg.Add(2)
		started := make(chan struct{})
		go func() {
			defer wg.Done()
			defer close(started)
			_ = h.StartShard(members, false, testCreateFunc, testShardConfig(1, 1))
		}()
		go func() {
			// Launched together, StopShard always LOSES this race: StartShard's
			// pre-publication work (NewStateMachine, Open, recoverLogReader,
			// NewPeer) runs first, so the stop returns ErrShardNotFound before
			// h.nodes is ever populated and the test passes against the unfixed
			// code every run. A fixed sleep cannot find the window either — it
			// is the handful of microseconds between the h.nodes publication and
			// LoadNode. Watch for the publication instead and stop the instant
			// it lands, which targets that window directly.
			defer wg.Done()
			for {
				if _, ok := h.nodes.Load(uint64(1)); ok {
					break
				}
				select {
				case <-started:
					// StartShard returned without ever publishing (it failed
					// early, or a previous iteration's stop won): nothing to race.
					return
				default:
				}
				runtime.Gosched()
			}
			_ = h.StopShard(1)
		}()
		wg.Wait()

		// Drain whatever survived so the next iteration starts clean.
		_ = h.StopShard(1)

		if _, loaded := h.nodes.Load(uint64(1)); loaded {
			t.Fatalf("iteration %d: shard still in h.nodes after StopShard", i)
		}
		if engNode := h.engine.GetNode(1); engNode != nil {
			t.Fatalf("iteration %d: shard left loaded in the engine while absent "+
				"from h.nodes — an unremovable zombie backed by a closed rsm", i)
		}
	}
}
