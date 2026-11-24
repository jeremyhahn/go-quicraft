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
	"errors"
	"io/fs"
	"strings"
	"testing"
)

// A dropped Close on the snapshot receive path
//
// Sync flushes what this process wrote; Close is where a file system reports a
// write error it deferred — a full or quota-exceeded volume, a network file
// system's delayed error — and on some platforms it is the only place that error
// is ever delivered. The receive path logged both of its Closes at Debug and
// carried on. For snapshot.dat that meant SaveSnapshot recorded a possibly-short
// artifact as this replica's durable snapshot, and RemoveEntriesTo then DELETED
// the log entries the snapshot subsumes: the entries are gone and the snapshot
// cannot replace them. For snapshot.meta it meant a truncated record, which the
// DEK purge floor scans for the epoch the artifact needs.

var errTestSnapshotCloseFailed = errors.New("test fs: close refused")

// closeFailingFS is the real filesystem with Close failing for files whose path
// contains `match`, and only after the write and the Sync have really happened —
// which is exactly the deferred-write-error shape: everything looked fine right
// up to the Close.
type closeFailingFS struct {
	snapshotFS
	match string
}

func (f closeFailingFS) OpenFile(name string, flag int, perm fs.FileMode) (snapshotFile, error) {
	inner, err := f.snapshotFS.OpenFile(name, flag, perm)
	if err != nil {
		return nil, err
	}
	if !strings.Contains(name, f.match) {
		return inner, nil
	}
	return closeFailingFile{snapshotFile: inner}, nil
}

type closeFailingFile struct {
	snapshotFile
}

func (f closeFailingFile) Close() error {
	_ = f.snapshotFile.Close()
	return errTestSnapshotCloseFailed
}

// TestSnapshotReceive_DataFileCloseFailureIsReported is the regression. The
// snapshot data file's Close fails; HandleSnapshot must report it and leave no
// LogDB record, so the leader re-sends after its ack timeout.
func TestSnapshotReceive_DataFileCloseFailureIsReported(t *testing.T) {
	payload := bytes.Repeat([]byte("close-failure-payload"), 200)
	f := newRecvFixture(t, nil)
	f.host.fsys = closeFailingFS{snapshotFS: osSnapshotFS{}, match: "snapshot.dat"}

	const index, term = uint64(700), uint64(3)
	err := f.handler.HandleSnapshot(f.chunks(index, term, payload), authorizeAllSenders)
	if err == nil {
		t.Fatal("HandleSnapshot reported SUCCESS although the snapshot data " +
			"file's Close failed. The receive then went on to record the " +
			"artifact in LogDB and to remove the log entries it subsumes, so a " +
			"short snapshot has replaced entries that no longer exist anywhere")
	}
	var recvErr *SnapshotReceiveError
	if !errors.As(err, &recvErr) || recvErr.Op != "close" {
		t.Fatalf("HandleSnapshot returned %v, want a SnapshotReceiveError with "+
			"Op \"close\"", err)
	}
	if !errors.Is(err, errTestSnapshotCloseFailed) {
		t.Fatalf("the returned error does not wrap the underlying close failure: %v", err)
	}
	if ss, getErr := f.ldb.GetSnapshot(f.shardID, f.replicaID); getErr == nil && ss.Index != 0 {
		t.Fatalf("LogDB records snapshot index %d although the receive failed: "+
			"the leader will stop re-sending", ss.Index)
	}
}

// TestSnapshotReceive_MetadataCloseFailureIsReported covers the second dropped
// Close, in writeReceivedSnapshotMetadata. It sat in a `defer` that could only
// ever log, so the error was discarded by construction.
func TestSnapshotReceive_MetadataCloseFailureIsReported(t *testing.T) {
	payload := bytes.Repeat([]byte("metadata-close-failure"), 200)
	f := newRecvFixture(t, nil)
	f.host.fsys = closeFailingFS{snapshotFS: osSnapshotFS{}, match: "snapshot.meta"}

	const index, term = uint64(701), uint64(3)
	err := f.handler.HandleSnapshot(f.chunks(index, term, payload), authorizeAllSenders)
	if err == nil {
		t.Fatal("HandleSnapshot reported SUCCESS although the snapshot metadata " +
			"file's Close failed. A truncated snapshot.meta reads as corrupt or " +
			"epoch-less, and an epoch-less directory suppresses DEK purging for " +
			"the whole barrier")
	}
	if !errors.Is(err, errTestSnapshotCloseFailed) {
		t.Fatalf("HandleSnapshot returned %v, want the underlying close failure", err)
	}
	if ss, getErr := f.ldb.GetSnapshot(f.shardID, f.replicaID); getErr == nil && ss.Index != 0 {
		t.Fatalf("LogDB records snapshot index %d although the receive failed", ss.Index)
	}
}
