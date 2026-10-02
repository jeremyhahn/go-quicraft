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
	"os"
	"path/filepath"
	"slices"
	"testing"

	"github.com/jeremyhahn/go-quicraft/pkg/config"
	"github.com/jeremyhahn/go-quicraft/pkg/internal/engine"
	"github.com/jeremyhahn/go-quicraft/pkg/logdb"
	"github.com/jeremyhahn/go-quicraft/pkg/logdb/memdb"
	"github.com/jeremyhahn/go-quicraft/pkg/proto"
)

// errInjected is the sentinel a fault-injecting filesystem returns.
var errInjected = errors.New("quicraft_test: injected filesystem failure")

// noRecoveryDeliverer suppresses state-machine recovery so a receive test
// exercises only the persistence path. Recovery reads the snapshot through the
// engine's own os-backed pool, which cannot see the in-memory crashFS.
type noRecoveryDeliverer struct {
	engineDeliverer
}

func (noRecoveryDeliverer) RequestSnapshotRecovery(_, _ uint64, node *engine.Node) error {
	// Release the snapshotting slot the caller claimed, as a completed
	// recovery would.
	node.ClearSnapshotting()
	return nil
}

// recvFixture is a snapshot receive path wired onto a crashFS.
type recvFixture struct {
	fs        *crashFS
	ldb       logdb.LogDB
	host      *Host
	handler   *hostMessageHandler
	node      *engine.Node
	snapDir   func(index uint64) string
	shardID   uint64
	replicaID uint64
	from      uint64
}

const (
	fixtureShardID   = uint64(77)
	fixtureReplicaID = uint64(1)
	fixtureFromID    = uint64(2)
)

func newRecvFixture(t *testing.T, barrier Barrier) *recvFixture {
	t.Helper()
	cfg := config.HostConfig{
		WALDir:                t.TempDir(),
		NodeHostDir:           t.TempDir(),
		RaftAddress:           "127.0.0.1:5000",
		ListenAddress:         "127.0.0.1:5000",
		DeploymentID:          1,
		AllowZeroDeploymentID: false,
	}
	cfg.SetDefaults()

	ldb := memdb.New()
	eng := engine.NewEngine(cfg, ldb, &noopSender{}, nil, nil, nil)
	eng.Start()
	t.Cleanup(eng.Stop)

	h := &Host{cfg: cfg, logdb: ldb, barrier: barrier}
	handler := &hostMessageHandler{engine: noRecoveryDeliverer{eng}, host: h}
	authorizeSnapshotPath(t, eng, h, fixtureShardID, fixtureReplicaID, fixtureFromID)

	node := eng.GetNode(fixtureShardID)
	if node == nil {
		t.Fatal("engine node not loaded")
	}
	cfs := newCrashFS(node.SnapshotDir(), cfg.NodeHostDir)
	h.fsys = cfs

	return &recvFixture{
		fs:        cfs,
		ldb:       ldb,
		host:      h,
		handler:   handler,
		node:      node,
		snapDir:   node.SnapshotSaveDir,
		shardID:   fixtureShardID,
		replicaID: fixtureReplicaID,
		from:      fixtureFromID,
	}
}

func (f *recvFixture) chunks(index, term uint64, payload []byte) []proto.SnapshotChunk {
	return []proto.SnapshotChunk{{
		ShardID:    f.shardID,
		ReplicaID:  f.replicaID,
		From:       f.from,
		Index:      index,
		Term:       term,
		ChunkID:    0,
		ChunkCount: 1,
		ChunkSize:  uint64(len(payload)),
		FileSize:   uint64(len(payload)),
		Data:       payload,
		// Carry the shard membership the fixture authorized against: the
		// delivered InstallSnapshot replaces the raft membership, and an empty
		// one would unauthorize the sender for any follow-up delivery. It comes
		// from the same helper authorizeSnapshotPath builds its configuration
		// with, so the two cannot drift into disagreeing.
		Membership: snapshotPathMembership(fixtureReplicaID, fixtureFromID),
	}}
}

func (f *recvFixture) dataPath(index uint64) string {
	return filepath.Join(f.snapDir(index), "snapshot.dat")
}

// plaintextOf returns the snapshot payload a stored data file represents,
// decrypting when the receiver holds a barrier.
// index identifies the artifact: the receive path binds every frame to
// (shardID, replicaID, index) plus its ordinal, so the reader must reproduce
// the same identity.
func plaintextOf(t *testing.T, barrier Barrier, index uint64, stored []byte) []byte {
	t.Helper()
	if barrier == nil {
		return stored
	}
	var out bytes.Buffer
	if _, err := decryptSnapshotFrames(barrier, fixtureShardID, fixtureReplicaID, index,
		bytes.NewReader(stored), &out); err != nil {
		t.Fatalf("stored snapshot is not a complete, decryptable image: %v", err)
	}
	return out.Bytes()
}

// writeDurableFile stores data at path through fsys and makes both the bytes
// and every directory entry leading to them durable.
func writeDurableFile(t *testing.T, fsys snapshotFS, path string, data []byte) {
	t.Helper()
	dir := filepath.Dir(path)
	if err := fsys.MkdirAll(dir, 0o750); err != nil {
		t.Fatalf("MkdirAll(%s): %v", dir, err)
	}
	f, err := fsys.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0o600)
	if err != nil {
		t.Fatalf("OpenFile(%s): %v", path, err)
	}
	if _, err := f.Write(data); err != nil {
		t.Fatalf("Write(%s): %v", path, err)
	}
	if err := f.Sync(); err != nil {
		t.Fatalf("Sync(%s): %v", path, err)
	}
	if err := f.Close(); err != nil {
		t.Fatalf("Close(%s): %v", path, err)
	}
	for d := dir; ; d = filepath.Dir(d) {
		if err := syncDir(fsys, d); err != nil {
			t.Fatalf("syncDir(%s): %v", d, err)
		}
		if filepath.Dir(d) == d {
			break
		}
	}
}

// encryptToBytes produces the on-disk frame format for plaintext.
func encryptToBytes(t *testing.T, barrier Barrier, plaintext []byte) []byte {
	t.Helper()
	var buf bytes.Buffer
	if _, _, err := encryptSnapshotFrames(barrier, testSnapShardID, testSnapReplicaID, testSnapIndex, bytes.NewReader(plaintext), &buf); err != nil {
		t.Fatalf("encryptSnapshotFrames: %v", err)
	}
	return buf.Bytes()
}

// TestWithSnapshotFS_RoutesHostSnapshotIO asserts the injection wiring: a Host
// built without it runs on the real filesystem, and one built with it routes
// snapshot I/O to the supplied implementation. The option is unexported, so
// the public API is unchanged.
func TestWithSnapshotFS_RoutesHostSnapshotIO(t *testing.T) {
	cfg := testHostConfig(t)
	cfs := newCrashFS(cfg.NodeHostDir)

	def, err := NewHost(cfg, WithoutTransport(), WithLogDB(memdb.New()), WithoutEncryption())
	if err != nil {
		t.Fatalf("NewHost: %v", err)
	}
	defer func() { _ = def.Close() }()
	if _, ok := def.snapshotFsys().(osSnapshotFS); !ok {
		t.Fatalf("default host snapshot filesystem = %T, want osSnapshotFS", def.snapshotFsys())
	}

	cfg2 := testHostConfig(t)
	injected, err := NewHost(cfg2, WithoutTransport(), WithLogDB(memdb.New()), withSnapshotFS(cfs), WithoutEncryption())
	if err != nil {
		t.Fatalf("NewHost with injected filesystem: %v", err)
	}
	defer func() { _ = injected.Close() }()
	if injected.snapshotFsys() != snapshotFS(cfs) {
		t.Fatal("withSnapshotFS did not route the host's snapshot filesystem")
	}
}

// TestSnapshotReceive_AckImpliesDurable asserts the core crash-safety claim of
// the receive path: once HandleSnapshot reports success — which is what stops
// the leader from re-sending — the snapshot survives an immediate power loss
// in full. A snapshot the receiver acked must never come back truncated,
// missing, or with its directory entry lost while LogDB still points at it.
func TestSnapshotReceive_AckImpliesDurable(t *testing.T) {
	payload := bytes.Repeat([]byte("durable-snapshot-payload!"), 400)

	for _, tc := range []struct {
		name      string
		encrypted bool
	}{
		{name: "plaintext"},
		{name: "encrypted", encrypted: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var barrier Barrier
			if tc.encrypted {
				barrier = createTestBarrier(t)
			}
			f := newRecvFixture(t, barrier)

			const index, term = uint64(900), uint64(4)
			if err := f.handler.HandleSnapshot(f.chunks(index, term, payload), authorizeAllSenders); err != nil {
				t.Fatalf("HandleSnapshot: %v", err)
			}

			// The receive reported success, so LogDB durably references it.
			ss, err := f.ldb.GetSnapshot(f.shardID, f.replicaID)
			if err != nil {
				t.Fatalf("GetSnapshot after successful receive: %v", err)
			}
			if ss.Index != index {
				t.Fatalf("LogDB snapshot index = %d, want %d", ss.Index, index)
			}

			f.fs.Crash()

			dataPath := f.dataPath(index)
			if !f.fs.exists(ss.Filepath) {
				t.Fatalf("DURABILITY VIOLATION: snapshot directory %s vanished after crash "+
					"while LogDB still records a snapshot at index %d "+
					"(the directory chain was never fsynced)", ss.Filepath, index)
			}
			stored, ok := f.fs.liveBytes(dataPath)
			if !ok {
				t.Fatalf("DURABILITY VIOLATION: %s is gone after crash although the receive "+
					"was acked and LogDB records index %d", dataPath, index)
			}
			got := plaintextOf(t, barrier, index, stored)
			if !bytes.Equal(got, payload) {
				t.Fatalf("DURABILITY VIOLATION: acked snapshot came back with %d of %d bytes "+
					"after crash; an fsync is missing on the receive path", len(got), len(payload))
			}

			metaPath := filepath.Join(ss.Filepath, "snapshot.meta")
			meta, ok := f.fs.liveBytes(metaPath)
			if !ok {
				t.Fatalf("DURABILITY VIOLATION: %s is gone after crash", metaPath)
			}
			parsed, parseErr := engine.ParseSnapshotMeta(meta)
			if parseErr != nil {
				t.Fatalf("DURABILITY VIOLATION: snapshot metadata (%d bytes) does not decode "+
					"after crash: %v", len(meta), parseErr)
			}
			if !parsed.EpochRecorded || !parsed.CompressionRecorded {
				t.Fatalf("DURABILITY VIOLATION: snapshot metadata survived the crash without "+
					"its epoch/compression fields (epoch_recorded=%v compression_recorded=%v); "+
					"the artifact's DEK floor and reader chain are then unknown",
					parsed.EpochRecorded, parsed.CompressionRecorded)
			}
		})
	}
}

// TestSnapshotReceive_CrashDuringReceiveLeavesNoPartialSnapshot asserts the
// other half of the all-or-nothing claim: a power loss at ANY step of the
// receive must never leave a snapshot LogDB references, and must never leave a
// half-written image under the final name that a later read would accept.
func TestSnapshotReceive_CrashDuringReceiveLeavesNoPartialSnapshot(t *testing.T) {
	payload := bytes.Repeat([]byte("half-written-must-never-be-visible"), 300)

	for _, stage := range []struct {
		name  string
		op    crashOp
		match string
	}{
		{name: "during_data_write", op: opWrite, match: "snapshot.dat"},
		{name: "before_data_fsync", op: opSync, match: "snapshot.dat"},
		{name: "before_metadata_fsync", op: opSync, match: "snapshot.meta"},
		{name: "before_directory_fsync", op: opDirSync, match: "snapshot-"},
	} {
		t.Run(stage.name, func(t *testing.T) {
			barrier := createTestBarrier(t)
			f := newRecvFixture(t, barrier)
			const index, term = uint64(1200), uint64(9)
			f.fs.crashOn(stage.op, stage.match)

			err := f.handler.HandleSnapshot(f.chunks(index, term, payload), authorizeAllSenders)
			if err == nil {
				t.Fatal("HandleSnapshot reported success although power was lost mid-receive; " +
					"the leader would stop re-sending a snapshot that is not durable")
			}

			if ss, getErr := f.ldb.GetSnapshot(f.shardID, f.replicaID); getErr == nil && ss.Index != 0 {
				t.Fatalf("LogDB records snapshot index %d although the receive crashed", ss.Index)
			}

			// Whatever survives under the final name must be a complete image,
			// never a partial one a later read would accept as valid.
			if stored, ok := f.fs.liveBytes(f.dataPath(index)); ok && len(stored) > 0 {
				var out bytes.Buffer
				if _, decErr := decryptSnapshotFrames(barrier, testSnapShardID, testSnapReplicaID, testSnapIndex, bytes.NewReader(stored), &out); decErr != nil {
					t.Fatalf("a partially written snapshot.dat survived the crash under the final name "+
						"(%d bytes, not a decryptable image): %v", len(stored), decErr)
				}
				if !bytes.Equal(out.Bytes(), payload) {
					t.Fatalf("a truncated snapshot.dat survived the crash under the final name: "+
						"%d of %d payload bytes", out.Len(), len(payload))
				}
			}
		})
	}
}

// TestSnapshotReceive_NeverWritesPlaintextAtRest asserts the at-rest claim of
// the receive path in the only terms that matter: for a shard with a barrier,
// the reassembled state machine must never exist in cleartext in any file, at
// any point of the receive, including every point a SIGKILL can land on.
//
// A receive path that wrote the plaintext chunks into the canonical
// snapshot-<index>/snapshot.dat, fsynced them, and re-encrypted in place
// afterwards would let a kill between the fsync and the re-encryption leave a
// COMPLETE PLAINTEXT STATE MACHINE durably under the canonical name. The deferred
// RemoveAll covers error returns, not a kill, and snapshot GC only reclaims a
// directory once maxSnapshotsRetained NEWER snapshots exist — on a follower
// that catches up once and never snapshots again, never.
//
// Two halves, both required. The success path proves the steady state; the
// crash sweep proves there is no window, and it is the crash sweep that fails
// against the two-step write.
func TestSnapshotReceive_NeverWritesPlaintextAtRest(t *testing.T) {
	// A payload no encryption of it can contain verbatim.
	payload := bytes.Repeat([]byte("SECRET-USER-COMMAND-PLAINTEXT!!"), 300)

	t.Run("success", func(t *testing.T) {
		barrier := createTestBarrier(t)
		f := newRecvFixture(t, barrier)
		const index, term = uint64(1300), uint64(2)

		if err := f.handler.HandleSnapshot(f.chunks(index, term, payload), authorizeAllSenders); err != nil {
			t.Fatalf("HandleSnapshot: %v", err)
		}
		if hit := f.fs.findLiveBytes(payload); hit != "" {
			t.Fatalf("the received state machine is on disk in PLAINTEXT at %s", hit)
		}
		// And the artifact really is the snapshot, encrypted — not merely absent.
		stored, ok := f.fs.liveBytes(f.dataPath(index))
		if !ok {
			t.Fatalf("no artifact at %s after a successful receive", f.dataPath(index))
		}
		if got := plaintextOf(t, barrier, index, stored); !bytes.Equal(got, payload) {
			t.Fatalf("artifact decrypts to %d bytes, want the %d-byte payload", len(got), len(payload))
		}
	})

	// Every filesystem operation the receive performs is a point a SIGKILL can
	// land on. Walk them all: at each one, the process stops dead with whatever
	// is in the live image, and none of it may be the plaintext.
	for _, stage := range []struct {
		name  string
		op    crashOp
		match string
	}{
		{name: "kill_at_mkdir", op: opMkdirAll, match: "snapshot-"},
		{name: "kill_at_create", op: opOpenFile, match: "snapshot.dat"},
		{name: "kill_at_data_write", op: opWrite, match: "snapshot.dat"},
		{name: "kill_at_data_fsync", op: opSync, match: "snapshot.dat"},
		{name: "kill_at_metadata_write", op: opWrite, match: "snapshot.meta"},
		{name: "kill_at_metadata_fsync", op: opSync, match: "snapshot.meta"},
		{name: "kill_at_directory_fsync", op: opDirSync, match: "snapshot-"},
	} {
		t.Run(stage.name, func(t *testing.T) {
			f := newRecvFixture(t, createTestBarrier(t))
			const index, term = uint64(1301), uint64(2)
			f.fs.crashOn(stage.op, stage.match)

			// The receive fails; what it leaves behind is the point.
			_ = f.handler.HandleSnapshot(f.chunks(index, term, payload), authorizeAllSenders)

			if hit := f.fs.findLiveBytes(payload); hit != "" {
				t.Fatalf("a kill at %s left the received state machine on disk in PLAINTEXT at %s",
					stage.name, hit)
			}
		})
	}
}

// TestSnapshotReceive_PinsInFlightEpoch is the M-3 regression test.
//
// The receive path chooses the at-rest epoch it seals under RIGHT HERE: the
// sender decrypts before streaming (doSendSnapshot sets Epoch=0), so hdr.Epoch
// is always 0 on this path and the transport's pin protects nothing. Without a
// pin of its own, a Rotate plus a concurrent purgeOldEpochs landing between the
// first frame and the LogDB record can retire the very epoch this artifact was
// sealed under, leaving a record that names ciphertext nothing can ever open —
// a follower permanently unable to recover, from a snapshot it acknowledged.
//
// Two properties, and the second is the one a naive pin misses: the epoch
// actually RECORDED must be pinned, not merely the epoch read before the write.
func TestSnapshotReceive_PinsInFlightEpoch(t *testing.T) {
	inner := createTestBarrier(t)
	pin := &pinRecordingBarrier{Barrier: inner}
	f := newRecvFixture(t, pin)

	const index, term = uint64(1400), uint64(3)
	payload := bytes.Repeat([]byte("epoch-pinned-receive-payload"), 64)

	if err := f.handler.HandleSnapshot(f.chunks(index, term, payload), authorizeAllSenders); err != nil {
		t.Fatalf("HandleSnapshot: %v", err)
	}

	snap, err := f.ldb.GetSnapshot(f.shardID, f.replicaID)
	if err != nil {
		t.Fatalf("GetSnapshot: %v", err)
	}
	if snap.Epoch == 0 {
		t.Fatal("the receive recorded Epoch 0 for a shard WITH a barrier: " +
			"the artifact is plaintext at rest, so the pinning contract is not " +
			"even reachable and this test would pass vacuously")
	}

	pin.mu.Lock()
	registered := append([]uint64(nil), pin.registered...)
	deregistered := append([]uint64(nil), pin.deregistered...)
	pin.mu.Unlock()

	if len(registered) == 0 {
		t.Fatal("the receive path registered NO in-flight epoch: a Rotate plus a " +
			"concurrent purgeOldEpochs during the receive can retire the epoch " +
			"this snapshot was sealed under, and the LogDB record then names " +
			"ciphertext that can never be decrypted")
	}
	if !slices.Contains(registered, snap.Epoch) {
		t.Fatalf("recorded Epoch %d was never pinned (pinned: %v): the pin covers "+
			"the epoch read BEFORE the write, but a rotation landing mid-write "+
			"seals later frames at a higher epoch, and it is the RECORDED epoch "+
			"that must survive the purge", snap.Epoch, registered)
	}

	// The pin must be released, or a long-lived follower leaks a floor that
	// blocks every future DEK purge for this barrier.
	for _, e := range registered {
		if !slices.Contains(deregistered, e) {
			t.Fatalf("epoch %d was pinned and never released (released: %v): the "+
				"DEK purge floor stays held for the life of the process", e, deregistered)
		}
	}
}

// TestSnapshotReceive_FailurePropagation asserts that every persistence
// failure on the receive path reaches the caller. A swallowed failure reported
// as success is the worst outcome available here: the leader stops re-sending
// and the follower is left permanently behind.
func TestSnapshotReceive_FailurePropagation(t *testing.T) {
	payload := []byte("failure-propagation-payload")

	for _, tc := range []struct {
		name  string
		op    crashOp
		match string
		wants string
	}{
		// The receive stages in a sibling of the canonical directory, so the
		// directory it creates up front is the shard/replica base, not
		// "snapshot-<index>".
		{name: "mkdir_fails", op: opMkdirAll, match: "replica-", wants: "mkdir"},
		{name: "stage_fails", op: opMkdirTemp, match: "replica-", wants: "stage"},
		{name: "create_fails", op: opOpenFile, match: "snapshot.dat", wants: "create"},
		{name: "write_fails", op: opWrite, match: "snapshot.dat", wants: "write"},
		{name: "data_fsync_fails", op: opSync, match: "snapshot.dat", wants: "sync"},
		// The receive encrypts INLINE, so the artifact is written exactly once:
		// there is no plaintext file to re-encrypt, no scratch sibling, and no
		// rename. The failure modes the two-step write had (a failed re-encrypt
		// rename, a failed scratch fsync) are covered by write_fails and
		// data_fsync_fails above, which are now the encrypting write itself.
		{name: "metadata_write_fails", op: opWrite, match: "snapshot.meta", wants: "metadata"},
		{name: "metadata_fsync_fails", op: opSync, match: "snapshot.meta", wants: "metadata"},
		// The publishing rename is the step that makes the canonical name
		// appear. A failure there must reach the caller: the artifact is
		// written and fsynced but nothing references it, so a receive that
		// reported success would leave the follower permanently behind with a
		// staging directory nothing enumerates.
		{name: "publish_fails", op: opRename, match: "snapshot-", wants: "publish"},
		{name: "directory_fsync_fails", op: opDirSync, match: "snapshot-", wants: "sync_dir"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newRecvFixture(t, createTestBarrier(t))
			const index, term = uint64(1500), uint64(3)
			f.fs.failOnce(tc.op, tc.match, errInjected)

			err := f.handler.HandleSnapshot(f.chunks(index, term, payload), authorizeAllSenders)
			if err == nil {
				t.Fatalf("HandleSnapshot reported SUCCESS although %s failed; "+
					"the leader stops re-sending and the follower never recovers", tc.op)
			}
			var recvErr *SnapshotReceiveError
			if !errors.As(err, &recvErr) {
				t.Fatalf("error = %v (%T), want *SnapshotReceiveError", err, err)
			}
			if recvErr.Op != tc.wants {
				t.Fatalf("SnapshotReceiveError.Op = %q, want %q", recvErr.Op, tc.wants)
			}
			if !errors.Is(err, errInjected) {
				t.Fatalf("injected cause not propagated: %v", err)
			}
			if ss, getErr := f.ldb.GetSnapshot(f.shardID, f.replicaID); getErr == nil && ss.Index != 0 {
				t.Fatalf("LogDB records snapshot index %d although the receive failed", ss.Index)
			}
		})
	}
}

// TestReencryptSnapshotFile_CrashAtEachStage exercises the temp -> fsync ->
// rename -> directory-fsync sequence under power loss at every stage. The
// destination must always come back as one COMPLETE image — the original or
// the fully re-encrypted replacement — never a partial file under the final
// name, and never missing.
func TestReencryptSnapshotFile_CrashAtEachStage(t *testing.T) {
	original := []byte("the-original-destination-snapshot-content")
	updated := bytes.Repeat([]byte("re-encrypted-replacement-content"), 200)

	for _, stage := range []struct {
		name  string
		op    crashOp
		match string
	}{
		{name: "after_temp_write_before_fsync", op: opSync, match: ".reencrypt-snapshot-"},
		{name: "after_fsync_before_rename", op: opRename, match: "snapshot.dat"},
		{name: "after_rename_before_dir_fsync", op: opDirSync, match: "dst"},
	} {
		t.Run(stage.name, func(t *testing.T) {
			srcBarrier := createTestBarrier(t)
			dstBarrier := createTestBarrier(t)
			root := "/vol"
			cfs := newCrashFS(root)

			srcPath := filepath.Join(root, "src", "snapshot.dat")
			dstPath := filepath.Join(root, "dst", "snapshot.dat")
			writeDurableFile(t, cfs, srcPath, encryptToBytes(t, srcBarrier, updated))
			writeDurableFile(t, cfs, dstPath, encryptToBytes(t, dstBarrier, original))

			cfs.crashOn(stage.op, stage.match)
			if _, err := reencryptSnapshotFile(cfs, srcPath, dstPath, testSnapShardID, testSnapReplicaID, testSnapIndex, srcBarrier, dstBarrier); err == nil {
				t.Fatal("reencryptSnapshotFile reported success although power was lost mid-write")
			}

			stored, ok := cfs.liveBytes(dstPath)
			if !ok {
				t.Fatalf("DURABILITY VIOLATION: the destination snapshot %s is GONE after a crash "+
					"at stage %s; a crash must never lose the file being replaced", dstPath, stage.name)
			}
			var out bytes.Buffer
			if _, err := decryptSnapshotFrames(dstBarrier, testSnapShardID, testSnapReplicaID, testSnapIndex, bytes.NewReader(stored), &out); err != nil {
				t.Fatalf("DURABILITY VIOLATION: %s is not a complete image after a crash at stage %s "+
					"(%d bytes): %v", dstPath, stage.name, len(stored), err)
			}
			got := out.Bytes()
			if !bytes.Equal(got, original) && !bytes.Equal(got, updated) {
				t.Fatalf("DURABILITY VIOLATION: %s holds neither the original nor the complete "+
					"replacement after a crash at stage %s (%d bytes)", dstPath, stage.name, len(got))
			}

			// No temporary file may remain visible in the destination directory.
			for _, name := range cfs.entries(filepath.Dir(dstPath)) {
				if name != "snapshot.dat" {
					t.Fatalf("leftover file %q in %s after a crash at stage %s",
						name, filepath.Dir(dstPath), stage.name)
				}
			}
		})
	}
}

// TestReencryptSnapshotFile_SuccessSurvivesCrash asserts the success half of
// the atomic-replace claim: once the migration reports success its result is
// durable, so a caller that records the new epoch cannot come back to the old
// content, a truncated file, or a missing directory entry.
func TestReencryptSnapshotFile_SuccessSurvivesCrash(t *testing.T) {
	srcBarrier := createTestBarrier(t)
	dstBarrier := createTestBarrier(t)
	updated := bytes.Repeat([]byte("migrated-snapshot-content"), 500)

	root := "/vol"
	cfs := newCrashFS(root)
	srcPath := filepath.Join(root, "src", "snapshot.dat")
	dstPath := filepath.Join(root, "dst", "snapshot.dat")
	writeDurableFile(t, cfs, srcPath, encryptToBytes(t, srcBarrier, updated))
	writeDurableFile(t, cfs, dstPath, encryptToBytes(t, dstBarrier, []byte("stale")))

	if _, err := reencryptSnapshotFile(cfs, srcPath, dstPath, testSnapShardID, testSnapReplicaID, testSnapIndex, srcBarrier, dstBarrier); err != nil {
		t.Fatalf("reencryptSnapshotFile: %v", err)
	}
	cfs.Crash()

	stored, ok := cfs.liveBytes(dstPath)
	if !ok {
		t.Fatalf("DURABILITY VIOLATION: %s is gone after a crash following a SUCCESSFUL migration", dstPath)
	}
	var out bytes.Buffer
	if _, err := decryptSnapshotFrames(dstBarrier, testSnapShardID, testSnapReplicaID, testSnapIndex, bytes.NewReader(stored), &out); err != nil {
		t.Fatalf("DURABILITY VIOLATION: %s is not a complete image after a crash following a "+
			"successful migration (%d bytes): %v", dstPath, len(stored), err)
	}
	if !bytes.Equal(out.Bytes(), updated) {
		t.Fatalf("DURABILITY VIOLATION: a successful migration lost its result across a crash: "+
			"%d of %d bytes survived; an fsync is missing on the reencrypt path",
			out.Len(), len(updated))
	}
}

// TestReencryptSnapshotFile_FailurePropagation asserts an fsync, rename, or
// write failure fails the migration instead of reporting success, and leaves
// the destination untouched.
func TestReencryptSnapshotFile_FailurePropagation(t *testing.T) {
	original := []byte("original-destination-content")
	updated := []byte("replacement-content")

	for _, tc := range []struct {
		name  string
		op    crashOp
		match string
	}{
		{name: "temp_write_fails", op: opWrite, match: ".reencrypt-snapshot-"},
		{name: "temp_fsync_fails", op: opSync, match: ".reencrypt-snapshot-"},
		{name: "rename_fails", op: opRename, match: "snapshot.dat"},
		{name: "dir_fsync_fails", op: opDirSync, match: "dst"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			srcBarrier := createTestBarrier(t)
			dstBarrier := createTestBarrier(t)
			root := "/vol"
			cfs := newCrashFS(root)
			srcPath := filepath.Join(root, "src", "snapshot.dat")
			dstPath := filepath.Join(root, "dst", "snapshot.dat")
			writeDurableFile(t, cfs, srcPath, encryptToBytes(t, srcBarrier, updated))
			writeDurableFile(t, cfs, dstPath, encryptToBytes(t, dstBarrier, original))

			cfs.failOnce(tc.op, tc.match, errInjected)
			_, err := reencryptSnapshotFile(cfs, srcPath, dstPath, testSnapShardID, testSnapReplicaID, testSnapIndex, srcBarrier, dstBarrier)
			if err == nil {
				t.Fatalf("reencryptSnapshotFile reported SUCCESS although %s failed", tc.op)
			}
			if !errors.Is(err, errInjected) {
				t.Fatalf("injected cause not propagated: %v", err)
			}

			// Everything except the directory fsync fails before the rename,
			// so the destination must still hold the original.
			if tc.op == opDirSync {
				return
			}
			stored, ok := cfs.liveBytes(dstPath)
			if !ok {
				t.Fatalf("%s was removed by a failed migration", dstPath)
			}
			var out bytes.Buffer
			if _, decErr := decryptSnapshotFrames(dstBarrier, testSnapShardID, testSnapReplicaID, testSnapIndex, bytes.NewReader(stored), &out); decErr != nil {
				t.Fatalf("%s was corrupted by a failed migration: %v", dstPath, decErr)
			}
			if !bytes.Equal(out.Bytes(), original) {
				t.Fatalf("%s content changed although the migration failed", dstPath)
			}
			for _, name := range cfs.entries(filepath.Dir(dstPath)) {
				if name != "snapshot.dat" {
					t.Fatalf("failed migration left %q behind in %s", name, filepath.Dir(dstPath))
				}
			}
		})
	}
}

// TestImportSnapshot_SurvivesCrashAfterSuccess asserts that a completed import
// is durable. ImportSnapshot records the snapshot in LogDB, and StartShard
// treats a LogDB record whose data file cannot be opened as fatal, so losing
// the imported directory to a crash leaves the shard unstartable.
func TestImportSnapshot_SurvivesCrashAfterSuccess(t *testing.T) {
	cfg := testHostConfig(t)
	db := newMockLogDB()
	db.mu.Lock()
	db.nodeInfos = []logdb.NodeInfo{{ShardID: 5, ReplicaID: 3}}
	db.mu.Unlock()

	cfs := newCrashFS(cfg.NodeHostDir)
	h, err := NewHost(cfg, WithLogDB(db), WithoutTransport(), withSnapshotFS(cfs), WithoutEncryption())
	if err != nil {
		t.Fatalf("NewHost: %v", err)
	}
	defer func() { _ = h.Close() }()

	payload := bytes.Repeat([]byte("imported-snapshot-bytes"), 100)
	artifact := buildSnapshotArtifact(t, 42, 1, 0, proto.Membership{}, payload)
	if err := h.ImportSnapshot(context.Background(), 5, bytes.NewReader(artifact), WithImportDataDir("")); err != nil {
		t.Fatalf("ImportSnapshot: %v", err)
	}

	db.mu.Lock()
	snap := db.snapshots[mockNodeKey{5, 3}]
	db.mu.Unlock()
	if snap.Filepath == "" {
		t.Fatal("import recorded no snapshot directory")
	}

	cfs.Crash()

	dataPath := filepath.Join(snap.Filepath, "snapshot.dat")
	stored, ok := cfs.liveBytes(dataPath)
	if !ok {
		t.Fatalf("DURABILITY VIOLATION: %s is gone after a crash following a SUCCESSFUL import, "+
			"while LogDB durably records the snapshot at %s; the import never fsynced the "+
			"directory chain, so StartShard fails with snapshot_open", dataPath, snap.Filepath)
	}
	if !bytes.Equal(stored, payload) {
		t.Fatalf("DURABILITY VIOLATION: imported snapshot came back with %d of %d bytes after crash",
			len(stored), len(payload))
	}
}

// flakyGetSnapshotDB fails the first GetSnapshot call, modelling a transient
// LogDB read error, and then behaves normally.
type flakyGetSnapshotDB struct {
	logdb.LogDB
	failed bool
}

func (d *flakyGetSnapshotDB) GetSnapshot(shardID, replicaID uint64) (logdb.Snapshot, error) {
	if !d.failed {
		d.failed = true
		return logdb.Snapshot{}, errInjected
	}
	return d.LogDB.GetSnapshot(shardID, replicaID)
}

// TestSnapshotReceive_CleanupDoesNotDestroyCommittedSnapshot asserts that a
// receive which does NOT commit never deletes a snapshot directory LogDB
// already durably references. The cleanup path removes the whole snapshot
// directory, and the directory name is a pure function of the snapshot index,
// so any second receive of an index already recorded in LogDB is aimed at
// committed state.
func TestSnapshotReceive_CleanupDoesNotDestroyCommittedSnapshot(t *testing.T) {
	payload := []byte("committed-snapshot-content-that-must-survive")
	const index, term = uint64(2100), uint64(6)

	t.Run("duplicate_delivery", func(t *testing.T) {
		f := newRecvFixture(t, createTestBarrier(t))
		if err := f.handler.HandleSnapshot(f.chunks(index, term, payload), authorizeAllSenders); err != nil {
			t.Fatalf("first HandleSnapshot: %v", err)
		}
		before, ok := f.fs.liveBytes(f.dataPath(index))
		if !ok {
			t.Fatal("committed snapshot missing after a successful receive")
		}
		if err := f.handler.HandleSnapshot(f.chunks(index, term, payload), authorizeAllSenders); err != nil {
			t.Fatalf("duplicate HandleSnapshot: %v", err)
		}
		after, ok := f.fs.liveBytes(f.dataPath(index))
		if !ok {
			t.Fatal("DATA LOSS: the duplicate delivery destroyed the committed snapshot directory")
		}
		if !bytes.Equal(before, after) {
			t.Fatal("DATA LOSS: the duplicate delivery rewrote the committed snapshot")
		}
	})

	t.Run("failed_rereceive_after_logdb_read_error", func(t *testing.T) {
		f := newRecvFixture(t, createTestBarrier(t))
		if err := f.handler.HandleSnapshot(f.chunks(index, term, payload), authorizeAllSenders); err != nil {
			t.Fatalf("first HandleSnapshot: %v", err)
		}
		committed, ok := f.fs.liveBytes(f.dataPath(index))
		if !ok {
			t.Fatal("committed snapshot missing after a successful receive")
		}
		ss, err := f.ldb.GetSnapshot(f.shardID, f.replicaID)
		if err != nil {
			t.Fatalf("GetSnapshot: %v", err)
		}

		// The dedup guard is the only thing keeping a re-delivery away from
		// the committed directory, and it is skipped when the LogDB read
		// fails. Make the ensuing re-receive fail so the cleanup path runs.
		f.host.logdb = &flakyGetSnapshotDB{LogDB: f.ldb}
		f.fs.failOnce(opSync, "snapshot.meta", errInjected)
		if err := f.handler.HandleSnapshot(f.chunks(index, term, payload), authorizeAllSenders); err == nil {
			t.Fatal("re-receive reported success although the metadata fsync failed")
		}

		stored, ok := f.fs.liveBytes(f.dataPath(index))
		if !ok {
			t.Fatalf("DATA LOSS: the failed re-receive deleted %s, which LogDB durably "+
				"references (snapshot index %d, dir %s)", f.dataPath(index), ss.Index, ss.Filepath)
		}
		if !bytes.Equal(stored, committed) {
			t.Fatalf("DATA LOSS: the failed re-receive left the LogDB-referenced snapshot "+
				"at %d bytes instead of the committed %d", len(stored), len(committed))
		}
	})
}
