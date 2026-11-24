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
	"encoding/binary"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/jeremyhahn/go-quicraft/pkg/logdb"
	"github.com/jeremyhahn/go-quicraft/pkg/proto"
)

// importFixture is a host that has LogDB bootstrap info for shard 5 but does
// not run it, which is exactly the state ImportSnapshot requires.
type importFixture struct {
	host      *Host
	logdb     *mockLogDB
	shardID   uint64
	replicaID uint64
	snapshots string // root of the on-disk snapshot tree
}

func newImportFixture(t *testing.T) *importFixture {
	t.Helper()
	cfg := testHostConfig(t)
	db := newMockLogDB()
	db.mu.Lock()
	db.nodeInfos = []logdb.NodeInfo{{ShardID: 5, ReplicaID: 3}}
	db.mu.Unlock()

	h, err := NewHost(cfg, WithLogDB(db), WithoutTransport(), WithoutEncryption())
	if err != nil {
		t.Fatalf("NewHost: %v", err)
	}
	t.Cleanup(func() { _ = h.Close() })

	return &importFixture{
		host:      h,
		logdb:     db,
		shardID:   5,
		replicaID: 3,
		snapshots: filepath.Join(cfg.NodeHostDir, "snapshots"),
	}
}

// assertNoImportedSnapshot fails when an import left anything on disk or
// recorded a snapshot in LogDB. A rejected artifact must leave no trace.
func (f *importFixture) assertNoImportedSnapshot(t *testing.T) {
	t.Helper()
	if entries, err := os.ReadDir(f.snapshots); err == nil && len(entries) > 0 {
		t.Fatalf("rejected import left %d entries under %s", len(entries), f.snapshots)
	}
	f.logdb.mu.Lock()
	defer f.logdb.mu.Unlock()
	if _, ok := f.logdb.snapshots[mockNodeKey{f.shardID, f.replicaID}]; ok {
		t.Fatal("rejected import recorded a snapshot in LogDB")
	}
}

// artifactHeaderBytes builds a syntactically well-formed fixed header with a
// caller-chosen membership length, so header validation can be driven
// independently of a real membership encoding.
func artifactHeaderBytes(membLen uint32) []byte {
	buf := make([]byte, snapshotArtifactFixedHeaderSize)
	copy(buf[0:8], snapshotArtifactMagic[:])
	binary.LittleEndian.PutUint32(buf[8:12], snapshotArtifactVersion)
	binary.LittleEndian.PutUint64(buf[12:20], 42)
	binary.LittleEndian.PutUint64(buf[20:28], 1)
	binary.LittleEndian.PutUint64(buf[28:36], 0)
	binary.LittleEndian.PutUint16(buf[36:38], 0)
	binary.LittleEndian.PutUint32(buf[38:42], membLen)
	return buf
}

// TestImportSnapshot_RejectsMalformedArtifactHeader verifies that a foreign or
// corrupt artifact is rejected by the header parser BEFORE any directory,
// data file, or LogDB record is created, and that the rejection identifies
// which check failed. The artifact stream is untrusted input, so a length
// prefix must be bounded before it is used to size an allocation.
func TestImportSnapshot_RejectsMalformedArtifactHeader(t *testing.T) {
	tests := []struct {
		name       string
		artifact   []byte
		wantReason string
	}{
		{
			name: "bad magic",
			artifact: func() []byte {
				b := artifactHeaderBytes(0)
				b[0] = 'X'
				return b
			}(),
			wantReason: "bad magic",
		},
		{
			name:       "membership length exceeds maximum",
			artifact:   artifactHeaderBytes(maxSnapshotArtifactMembLen + 1),
			wantReason: "exceeds maximum",
		},
		{
			name:       "membership truncated",
			artifact:   append(artifactHeaderBytes(512), make([]byte, 8)...),
			wantReason: "read membership",
		},
		{
			name:       "membership undecodable",
			artifact:   append(artifactHeaderBytes(8), make([]byte, 8)...),
			wantReason: "decode membership",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := newImportFixture(t)

			err := f.host.ImportSnapshot(context.Background(), f.shardID, bytes.NewReader(tt.artifact))
			var artErr *SnapshotArtifactError
			if !errors.As(err, &artErr) {
				t.Fatalf("ImportSnapshot error = %v (%T), want *SnapshotArtifactError", err, err)
			}
			if !strings.Contains(artErr.Reason, tt.wantReason) {
				t.Fatalf("reason = %q, want it to mention %q", artErr.Reason, tt.wantReason)
			}
			f.assertNoImportedSnapshot(t)
		})
	}
}

// cancelAfterHeaderReader hands out the artifact bytes and cancels the import
// context the moment the fixed header has been consumed, so the cancellation
// lands deterministically between header parsing and the first disk write.
type cancelAfterHeaderReader struct {
	data   []byte
	offset int
	cancel context.CancelFunc
}

func (r *cancelAfterHeaderReader) Read(p []byte) (int, error) {
	if r.offset >= len(r.data) {
		return 0, io.EOF
	}
	n := copy(p, r.data[r.offset:])
	r.offset += n
	if r.offset >= snapshotArtifactFixedHeaderSize && r.cancel != nil {
		r.cancel()
		r.cancel = nil
	}
	return n, nil
}

// TestImportSnapshot_CanceledAfterHeaderLeavesNoState verifies the
// cancellation checkpoint between header parsing and disk I/O: a context that
// ends there aborts the import with ErrCanceled and creates nothing on disk.
func TestImportSnapshot_CanceledAfterHeaderLeavesNoState(t *testing.T) {
	f := newImportFixture(t)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	artifact := buildSnapshotArtifact(t, 42, 1, 0, proto.Membership{}, []byte("payload"))
	r := &cancelAfterHeaderReader{data: artifact, cancel: cancel}

	err := f.host.ImportSnapshot(ctx, f.shardID, r)
	if !errors.Is(err, ErrCanceled) {
		t.Fatalf("ImportSnapshot error = %v, want ErrCanceled", err)
	}
	f.assertNoImportedSnapshot(t)
}

// TestImportSnapshot_DirectoryCreationFailure verifies that a snapshot tree
// that cannot be created is reported rather than silently skipped: a plain
// file sitting where the snapshots directory belongs must fail the import.
func TestImportSnapshot_DirectoryCreationFailure(t *testing.T) {
	f := newImportFixture(t)

	// Occupy the snapshots root with a regular file so MkdirAll cannot
	// create the shard/replica/snapshot path beneath it.
	if err := os.WriteFile(f.snapshots, []byte("not a directory"), 0o600); err != nil {
		t.Fatalf("write blocking file: %v", err)
	}

	artifact := buildSnapshotArtifact(t, 42, 1, 0, proto.Membership{}, []byte("payload"))
	err := f.host.ImportSnapshot(context.Background(), f.shardID, bytes.NewReader(artifact))
	if err == nil {
		t.Fatal("ImportSnapshot succeeded although the snapshot directory could not be created")
	}

	f.logdb.mu.Lock()
	defer f.logdb.mu.Unlock()
	if _, ok := f.logdb.snapshots[mockNodeKey{f.shardID, f.replicaID}]; ok {
		t.Fatal("failed import recorded a snapshot in LogDB")
	}
}

// failAfterHeaderReader delivers the header and then fails, modelling a
// truncated or aborted artifact stream (a broken pipe from the peer supplying
// the export).
type failAfterHeaderReader struct {
	data   []byte
	offset int
}

var errTestArtifactStream = errors.New("test: artifact stream failed")

func (r *failAfterHeaderReader) Read(p []byte) (int, error) {
	if r.offset >= snapshotArtifactFixedHeaderSize {
		return 0, errTestArtifactStream
	}
	end := snapshotArtifactFixedHeaderSize
	if end > len(r.data) {
		end = len(r.data)
	}
	n := copy(p, r.data[r.offset:end])
	r.offset += n
	return n, nil
}

// TestImportSnapshot_StreamFailureRemovesPartialArtifact verifies that a
// stream that dies mid-copy propagates the read error AND removes the partial
// snapshot directory. Leaving a partial artifact behind would strand disk
// space for an import that is never recorded in LogDB, and a later recovery
// could find a truncated snapshot.dat.
func TestImportSnapshot_StreamFailureRemovesPartialArtifact(t *testing.T) {
	f := newImportFixture(t)

	artifact := buildSnapshotArtifact(t, 42, 1, 0, proto.Membership{}, []byte("payload"))
	err := f.host.ImportSnapshot(context.Background(), f.shardID,
		&failAfterHeaderReader{data: artifact})
	if !errors.Is(err, errTestArtifactStream) {
		t.Fatalf("ImportSnapshot error = %v, want the stream error", err)
	}

	// The whole snapshot-<seq> directory must be gone, not just its contents.
	snapDir := filepath.Join(f.snapshots, "shard-5", "replica-3")
	if entries, readErr := os.ReadDir(snapDir); readErr == nil && len(entries) > 0 {
		t.Fatalf("partial import left %d entries under %s", len(entries), snapDir)
	}
	f.logdb.mu.Lock()
	defer f.logdb.mu.Unlock()
	if _, ok := f.logdb.snapshots[mockNodeKey{f.shardID, f.replicaID}]; ok {
		t.Fatal("failed import recorded a snapshot in LogDB")
	}
}

// ---------------------------------------------------------------------------
// SnapshotArtifactError
// ---------------------------------------------------------------------------

// TestSnapshotArtifactError verifies the message format and unwrapping of the
// artifact parse error, both of which are part of the public API: callers
// classify it with errors.As and log its message.
func TestSnapshotArtifactError(t *testing.T) {
	inner := errors.New("unexpected EOF")

	wrapped := &SnapshotArtifactError{Reason: "read membership", Err: inner}
	const wantWrapped = "quicraft: invalid snapshot artifact: read membership: unexpected EOF"
	if got := wrapped.Error(); got != wantWrapped {
		t.Fatalf("Error() = %q, want %q", got, wantWrapped)
	}
	if !errors.Is(wrapped, inner) {
		t.Fatal("errors.Is did not find the wrapped cause")
	}

	bare := &SnapshotArtifactError{Reason: "bad magic"}
	const wantBare = "quicraft: invalid snapshot artifact: bad magic"
	if got := bare.Error(); got != wantBare {
		t.Fatalf("Error() = %q, want %q", got, wantBare)
	}
	if bare.Unwrap() != nil {
		t.Fatalf("Unwrap() = %v, want nil", bare.Unwrap())
	}
}
