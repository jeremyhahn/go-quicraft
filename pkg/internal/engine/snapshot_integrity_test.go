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

package engine

import (
	"encoding/binary"
	"errors"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/jeremyhahn/go-quicraft/pkg/config"
)

// writeTestSnapshotMetadata is writeSnapshotMetadata for fixtures that build a
// snapshot directory by hand. The writer stamps the integrity fields over
// snapshot.dat and fails when it cannot, so a fixture that only needs the
// record gets an empty artifact beside it first.
func writeTestSnapshotMetadata(dir string, index, term, shardID, replicaID, epoch uint64,
	compression config.CompressionType) error {
	dataPath := filepath.Join(dir, snapshotDataFile)
	if _, err := os.Stat(dataPath); os.IsNotExist(err) {
		if err := os.WriteFile(dataPath, nil, 0o600); err != nil {
			return err
		}
	}
	return writeSnapshotMetadata(dir, index, term, shardID, replicaID, epoch, compression)
}

// writeSnapshotDir builds a snapshot directory with data and a meta record
// carrying integrity fields computed over that data.
func writeSnapshotDir(t *testing.T, data []byte) string {
	t.Helper()
	dir := t.TempDir()
	dataPath := filepath.Join(dir, snapshotDataFile)
	if err := os.WriteFile(dataPath, data, 0o600); err != nil {
		t.Fatalf("write data: %v", err)
	}
	sum, size, err := hashSnapshotData(dataPath)
	if err != nil {
		t.Fatalf("hash: %v", err)
	}
	meta := MarshalSnapshotMeta(SnapshotMeta{
		Index: 10, Term: 2, ShardID: 1, ReplicaID: 1,
		DataLen: size, DataHash: sum,
	})
	if err := os.WriteFile(filepath.Join(dir, snapshotMetadataFile), meta, 0o600); err != nil {
		t.Fatalf("write meta: %v", err)
	}
	return dir
}

// TestVerifySnapshotArtifact_DetectsSilentCorruption is the check a
// no-barrier deployment has instead of AEAD.
//
// A single flipped byte inside a serialized value leaves the file LENGTH
// unchanged, so no structural check trips: the corrupt value parses, recovers
// into the state machine, and is served to clients. The replica then holds a
// different value for that key than its peers at the same log index, with
// every Raft invariant intact and no error raised anywhere.
func TestVerifySnapshotArtifact_DetectsSilentCorruption(t *testing.T) {
	data := []byte("the quick brown fox jumps over the lazy dog, twice over")
	dir := writeSnapshotDir(t, data)
	dataPath := filepath.Join(dir, snapshotDataFile)

	if err := VerifySnapshotArtifact(dir, dataPath); err != nil {
		t.Fatalf("an intact artifact was rejected: %v", err)
	}

	// Flip ONE bit, preserving the length.
	corrupt := append([]byte(nil), data...)
	corrupt[len(corrupt)/2] ^= 0x01
	if err := os.WriteFile(dataPath, corrupt, 0o600); err != nil {
		t.Fatalf("rewrite: %v", err)
	}

	err := VerifySnapshotArtifact(dir, dataPath)
	if !errors.Is(err, ErrSnapshotCorrupt) {
		t.Fatalf("VerifySnapshotArtifact on a single flipped bit returned %v, "+
			"want ErrSnapshotCorrupt. Without this the corrupt value is "+
			"recovered into the state machine and served, diverging this "+
			"replica from its peers permanently", err)
	}
}

// TestVerifySnapshotArtifact_DetectsTruncation covers the other half: a
// trailing frame lost to a torn write reads as a clean EOF, because no frame
// count is recorded anywhere.
func TestVerifySnapshotArtifact_DetectsTruncation(t *testing.T) {
	data := []byte("frame-one|frame-two|frame-three")
	dir := writeSnapshotDir(t, data)
	dataPath := filepath.Join(dir, snapshotDataFile)

	if err := os.WriteFile(dataPath, data[:len(data)-10], 0o600); err != nil {
		t.Fatalf("truncate: %v", err)
	}
	if err := VerifySnapshotArtifact(dir, dataPath); !errors.Is(err, ErrSnapshotCorrupt) {
		t.Fatalf("a truncated artifact returned %v, want ErrSnapshotCorrupt", err)
	}
}

// TestVerifySnapshotArtifact_RefusesUnverifiableMetadata is the regression
// guard for the integrity check that switched itself off.
//
// VerifySnapshotArtifact used to return nil whenever snapshot.meta could not be
// read or parsed, or carried no integrity fields, calling the artifact
// "legacy". Every writer stamps the fields before a directory is published, so
// no legitimately written artifact lacks them; what does is a partial restore
// that dropped or damaged snapshot.meta. Paired with a same-length alteration
// of snapshot.dat — here, one flipped bit — that recovered divergent state into
// the state machine with no error anywhere. Each case must now fail closed.
func TestVerifySnapshotArtifact_RefusesUnverifiableMetadata(t *testing.T) {
	data := []byte("the quick brown fox jumps over the lazy dog, twice over")
	cases := []struct {
		name   string
		mangle func(t *testing.T, metaPath string)
	}{
		{"missing", func(t *testing.T, metaPath string) {
			if err := os.Remove(metaPath); err != nil {
				t.Fatalf("remove meta: %v", err)
			}
		}},
		{"empty", func(t *testing.T, metaPath string) {
			if err := os.WriteFile(metaPath, nil, 0o600); err != nil {
				t.Fatalf("empty meta: %v", err)
			}
		}},
		{"corrupt", func(t *testing.T, metaPath string) {
			// Foreign bytes: neither the magic nor any recognised pre-magic
			// record length.
			if err := os.WriteFile(metaPath, []byte("not a snapshot metadata record"), 0o600); err != nil {
				t.Fatalf("corrupt meta: %v", err)
			}
		}},
		{"truncated_body", func(t *testing.T, metaPath string) {
			meta, err := os.ReadFile(metaPath)
			if err != nil {
				t.Fatalf("read meta: %v", err)
			}
			if err := os.WriteFile(metaPath, meta[:len(meta)-4], 0o600); err != nil {
				t.Fatalf("truncate meta: %v", err)
			}
		}},
		{"no_integrity_fields", func(t *testing.T, metaPath string) {
			// A well-formed record whose body stops before DataLen/DataHash.
			meta := MarshalSnapshotMeta(SnapshotMeta{Index: 10, Term: 2, ShardID: 1, ReplicaID: 1})
			meta = meta[:snapshotMetaHeaderSize+metaBodyCompressionOff+8]
			binary.LittleEndian.PutUint32(meta[snapshotMetaBodyLenOff:], uint32(metaBodyCompressionOff+8))
			if err := os.WriteFile(metaPath, meta, 0o600); err != nil {
				t.Fatalf("write meta: %v", err)
			}
		}},
		{"pre_magic_v0_record", func(t *testing.T, metaPath string) {
			// No current writer produces a v0 record, and none carries the
			// integrity fields.
			if err := os.WriteFile(metaPath,
				legacyMetaRecord(legacyMetaSizeCompression, 10, 2, 1, 1, 1, 0, 0), 0o600); err != nil {
				t.Fatalf("write meta: %v", err)
			}
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			dir := writeSnapshotDir(t, data)
			dataPath := filepath.Join(dir, snapshotDataFile)
			tc.mangle(t, filepath.Join(dir, snapshotMetadataFile))

			// The same-length alteration the record would have caught.
			corrupt := append([]byte(nil), data...)
			corrupt[len(corrupt)/2] ^= 0x01
			if err := os.WriteFile(dataPath, corrupt, 0o600); err != nil {
				t.Fatalf("rewrite: %v", err)
			}

			err := VerifySnapshotArtifact(dir, dataPath)
			if !errors.Is(err, ErrSnapshotCorrupt) {
				t.Fatalf("VerifySnapshotArtifact with %s metadata = %v, want "+
					"ErrSnapshotCorrupt: an artifact nothing can vouch for must "+
					"not be recovered", tc.name, err)
			}
			var invalid *SnapshotMetaInvalidError
			if !errors.As(err, &invalid) {
				t.Fatalf("error = %T, want *SnapshotMetaInvalidError", err)
			}
		})
	}
}

// TestRecoverSnapshot_RefusesMissingMetadata drives the same refusal through
// the runtime recovery path: a recorded snapshot whose snapshot.meta is gone
// fails, and the state machine is not touched.
func TestRecoverSnapshot_RefusesMissingMetadata(t *testing.T) {
	const full = 4096
	f := newRecoverFixture(t, snapshotPayload(100, full), full)
	if err := os.Remove(filepath.Join(filepath.Dir(f.dataPath), snapshotMetadataFile)); err != nil {
		t.Fatalf("remove metadata: %v", err)
	}

	err := f.pool.recoverSnapshot(f.req)
	if !errors.Is(err, ErrSnapshotCorrupt) {
		t.Fatalf("recoverSnapshot without snapshot.meta = %v, want ErrSnapshotCorrupt", err)
	}
	if f.ss.LastApplied() != 0 {
		t.Fatalf("the state machine was recovered to %d despite the refusal", f.ss.LastApplied())
	}
}

// TestSnapshotDataVerifier_AgreesWithVerifySnapshotArtifact pins that the
// streaming verifier the send and export paths use reaches the SAME verdict
// VerifySnapshotArtifact reaches on the same artifact, over every record
// state — intact, flipped, truncated, pre-integrity and missing. The two share
// one policy (checkSnapshotArtifact) rather than copies of it, so a change to
// what a missing record means lands on egress and recovery together; this
// test is what fails if either side grows its own.
func TestSnapshotDataVerifier_AgreesWithVerifySnapshotArtifact(t *testing.T) {
	data := []byte("streamed and verified in the same pass, never read twice")
	rewrite := func(t *testing.T, dir, name string, b []byte) {
		t.Helper()
		if err := os.WriteFile(filepath.Join(dir, name), b, 0o600); err != nil {
			t.Fatalf("write %s: %v", name, err)
		}
	}
	cases := []struct {
		name    string
		mutate  func(t *testing.T, dir string)
		corrupt bool
	}{
		{name: "intact", mutate: func(*testing.T, string) {}},
		{name: "flipped", corrupt: true, mutate: func(t *testing.T, dir string) {
			b := append([]byte(nil), data...)
			b[3] ^= 0x10
			rewrite(t, dir, snapshotDataFile, b)
		}},
		{name: "truncated", corrupt: true, mutate: func(t *testing.T, dir string) {
			rewrite(t, dir, snapshotDataFile, data[:7])
		}},
		{name: "pre_integrity_record", mutate: func(t *testing.T, dir string) {
			meta := MarshalSnapshotMeta(SnapshotMeta{Index: 10, Term: 2, ShardID: 1, ReplicaID: 1})
			meta = meta[:snapshotMetaHeaderSize+metaBodyCompressionOff+8]
			binary.LittleEndian.PutUint32(meta[snapshotMetaBodyLenOff:], uint32(metaBodyCompressionOff+8))
			rewrite(t, dir, snapshotMetadataFile, meta)
		}},
		{name: "missing_record", mutate: func(t *testing.T, dir string) {
			if err := os.Remove(filepath.Join(dir, snapshotMetadataFile)); err != nil {
				t.Fatalf("remove meta: %v", err)
			}
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			dir := writeSnapshotDir(t, data)
			tc.mutate(t, dir)
			dataPath := filepath.Join(dir, snapshotDataFile)
			want := VerifySnapshotArtifact(dir, dataPath)
			if tc.corrupt && !errors.Is(want, ErrSnapshotCorrupt) {
				t.Fatalf("fixture: VerifySnapshotArtifact = %v, want ErrSnapshotCorrupt", want)
			}

			v := NewSnapshotDataVerifier(dir)
			f, err := os.Open(dataPath)
			if err != nil {
				t.Fatalf("open: %v", err)
			}
			defer func() { _ = f.Close() }()
			// Fed in uneven pieces, as a chunked reader would.
			if _, err := io.CopyBuffer(v, struct{ io.Reader }{f}, make([]byte, 5)); err != nil {
				t.Fatalf("feed verifier: %v", err)
			}
			got := v.Verify()

			// The same verdict: both accept, or both refuse with the same
			// error (the two are separately constructed values).
			if (got == nil) != (want == nil) || (got != nil && got.Error() != want.Error()) {
				t.Fatalf("streaming verifier = %v, VerifySnapshotArtifact = %v; "+
					"the egress and recovery verdicts diverge", got, want)
			}
		})
	}
}
