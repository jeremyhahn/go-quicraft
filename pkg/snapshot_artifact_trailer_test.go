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
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"testing/iotest"

	"github.com/jeremyhahn/go-quicraft/pkg/internal/engine"
	"github.com/jeremyhahn/go-quicraft/pkg/logdb"
	"github.com/jeremyhahn/go-quicraft/pkg/proto"
	"github.com/jeremyhahn/go-quicraft/pkg/seal"
)

// readArtifactPayload reads a whole artifact from r and returns its header and
// its payload, with the trailer checked and stripped. It fails the test when
// the header is malformed or the trailer does not match.
func readArtifactPayload(t *testing.T, r io.Reader) (snapshotArtifactHeader, []byte) {
	t.Helper()
	hdr, raw, err := readSnapshotArtifactHeader(r)
	if err != nil {
		t.Fatalf("readSnapshotArtifactHeader: %v", err)
	}
	data, err := io.ReadAll(newArtifactPayloadReader(raw, r))
	if err != nil {
		t.Fatalf("artifact does not match its trailer: %v", err)
	}
	return hdr, data
}

// trailedPayload returns data followed by the trailer ExportSnapshot writes
// for it after an empty header: the post-header part of an artifact, as
// writeImportedSnapshotData reads it when passed a nil header.
func trailedPayload(t *testing.T, data []byte) io.Reader {
	t.Helper()
	var buf bytes.Buffer
	pw := newArtifactPayloadWriter(&buf, nil)
	if _, err := pw.Write(data); err != nil {
		t.Fatalf("write payload: %v", err)
	}
	buf.Write(pw.trailer())
	return &buf
}

// stripArtifactTrailer replaces the contents of buf — a whole artifact — with
// its payload alone and returns its header, failing the test when the header
// is malformed or the trailer does not match.
func stripArtifactTrailer(t *testing.T, buf *bytes.Buffer) snapshotArtifactHeader {
	t.Helper()
	hdr, data := readArtifactPayload(t, bytes.NewReader(buf.Bytes()))
	buf.Reset()
	buf.Write(data)
	return hdr
}

// artifactVariant is one at-rest form a trailer test is run against: the
// exporting shard's snapshot is plaintext or sealed under a barrier, and the
// importing host is correspondingly plaintext or encrypting.
type artifactVariant struct {
	name      string
	encrypted bool
}

var artifactVariants = []artifactVariant{
	{name: "plaintext"},
	{name: "encrypted", encrypted: true},
}

// exportTestArtifact exports a snapshot of payload from a freshly started
// shard and returns what the writer received together with the export's
// error. corrupt makes the source fail its recorded integrity check: a
// same-size bit flip in a plaintext snapshot.dat, and a snapshot.meta that
// disagrees with intact frames for an encrypted one (whose frames would
// otherwise refuse the flip by AEAD before any plaintext was written).
func exportTestArtifact(t *testing.T, v artifactVariant, payload []byte, corrupt bool) ([]byte, error) {
	t.Helper()
	db := newMockLogDB()
	opts := []HostOption{WithLogDB(db), WithoutTransport()}
	var barrier *seal.Barrier
	if v.encrypted {
		barrier = createTestBarrier(t)
		opts = append(opts, WithBarrier(barrier))
	} else {
		opts = append(opts, WithoutEncryption())
	}
	h, err := NewHost(testHostConfig(t), opts...)
	if err != nil {
		t.Fatalf("NewHost: %v", err)
	}
	defer func() { _ = h.Close() }()
	if startErr := h.StartShard(map[uint64]string{1: "127.0.0.1:5000"}, false,
		testCreateFunc, testShardConfig(1, 1)); startErr != nil {
		t.Fatalf("StartShard: %v", startErr)
	}

	dir := t.TempDir()
	rec := logdb.Snapshot{Index: 700, Term: 3, Filepath: dir}
	if v.encrypted {
		writeEncryptedSnapshotFileFor(t, barrier, filepath.Join(dir, "snapshot.dat"), payload, 1, 1, rec.Index)
		skew := uint64(0)
		if corrupt {
			skew = 1
		}
		stampSnapshotMeta(t, dir, skew)
		rec.Epoch = barrier.CurrentEpoch()
	} else {
		path := writeRecordedSnapshot(t, dir, payload)
		if corrupt {
			flipSnapshotByte(t, path, 3)
		}
	}
	if saveErr := db.SaveSnapshot(1, 1, rec); saveErr != nil {
		t.Fatalf("SaveSnapshot: %v", saveErr)
	}

	var out bytes.Buffer
	expErr := h.ExportSnapshot(context.Background(), 1, &out)
	return out.Bytes(), expErr
}

// newArtifactImportFixture is newImportFixture with the importing host's
// at-rest form chosen by v: an encrypted variant imports under a barrier, so
// the payload streams through the frame sealer.
func newArtifactImportFixture(t *testing.T, v artifactVariant) (*importFixture, Barrier) {
	t.Helper()
	if !v.encrypted {
		return newImportFixture(t), nil
	}
	cfg := testHostConfig(t)
	db := newMockLogDB()
	db.mu.Lock()
	db.nodeInfos = []logdb.NodeInfo{{ShardID: 5, ReplicaID: 3}}
	db.mu.Unlock()
	barrier := createTestBarrier(t)
	h, err := NewHost(cfg, WithLogDB(db), WithoutTransport(), WithBarrier(barrier))
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
	}, barrier
}

// importedPayload reads back the data the fixture's recorded snapshot holds,
// decrypting it when the import sealed it.
func (f *importFixture) importedPayload(t *testing.T, barrier Barrier) []byte {
	t.Helper()
	f.logdb.mu.Lock()
	rec, ok := f.logdb.snapshots[mockNodeKey{f.shardID, f.replicaID}]
	f.logdb.mu.Unlock()
	if !ok {
		t.Fatal("the import recorded no snapshot")
	}
	raw, err := os.ReadFile(filepath.Join(rec.Filepath, "snapshot.dat"))
	if err != nil {
		t.Fatalf("read imported snapshot.dat: %v", err)
	}
	if barrier == nil {
		return raw
	}
	var plain bytes.Buffer
	if _, err := decryptSnapshotFrames(barrier, f.shardID, f.replicaID, rec.Index,
		bytes.NewReader(raw), &plain); err != nil {
		t.Fatalf("decrypt imported snapshot.dat: %v", err)
	}
	return plain.Bytes()
}

// assertNothingAdopted fails when a refused import left any trace a later
// StartShard could act on: a LogDB snapshot record, a published snapshot
// directory, a staging directory, or any file at all under the snapshot tree.
// The shard/replica directories the import's MkdirAll created may remain;
// they are empty and name no snapshot.
func (f *importFixture) assertNothingAdopted(t *testing.T) {
	t.Helper()
	f.logdb.mu.Lock()
	_, recorded := f.logdb.snapshots[mockNodeKey{f.shardID, f.replicaID}]
	f.logdb.mu.Unlock()
	if recorded {
		t.Fatal("a refused import recorded a snapshot in LogDB")
	}
	walkErr := filepath.WalkDir(f.snapshots, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			if errors.Is(err, fs.ErrNotExist) {
				return nil
			}
			return err
		}
		name := d.Name()
		if !d.IsDir() || strings.HasPrefix(name, "snapshot-") || strings.HasPrefix(name, engine.ImportStageDirPrefix) {
			t.Errorf("a refused import left %s behind", path)
		}
		return nil
	})
	if walkErr != nil {
		t.Fatalf("walk %s: %v", f.snapshots, walkErr)
	}
}

// trailerTestPayloads are one payload that fits in a single copy buffer and
// one that spans several, so both the whole-artifact and the streamed path
// of each side are exercised.
var trailerTestPayloads = []struct {
	name string
	data []byte
}{
	{name: "small", data: []byte("trailer-bound snapshot payload")},
	{name: "multi_buffer", data: bytes.Repeat([]byte("exported-state"), (3*copyWithContextChunkSize+7)/14+1)},
}

// TestExportImportSnapshot_TrailerRoundTrip verifies an exported artifact
// ends in a trailer binding its payload's length and hash, and that such an
// artifact imports — onto a plaintext host and onto an encrypting one — to
// exactly the payload that was exported.
func TestExportImportSnapshot_TrailerRoundTrip(t *testing.T) {
	for _, v := range artifactVariants {
		for _, p := range trailerTestPayloads {
			t.Run(v.name+"/"+p.name, func(t *testing.T) {
				artifact, err := exportTestArtifact(t, v, p.data, false)
				if err != nil {
					t.Fatalf("ExportSnapshot: %v", err)
				}
				r := bytes.NewReader(artifact)
				if _, got := readArtifactPayload(t, r); !bytes.Equal(got, p.data) {
					t.Fatalf("artifact payload is %d bytes, want the %d exported", len(got), len(p.data))
				}
				tr, trErr := parseSnapshotArtifactTrailer(artifact[len(artifact)-snapshotArtifactTrailerSize:])
				if trErr != nil {
					t.Fatalf("artifact does not end in a trailer: %v", trErr)
				}
				if tr.PayloadLen != uint64(len(p.data)) {
					t.Fatalf("trailer length = %d, want %d", tr.PayloadLen, len(p.data))
				}

				f, barrier := newArtifactImportFixture(t, v)
				if importErr := f.host.ImportSnapshot(context.Background(), f.shardID,
					bytes.NewReader(artifact), WithImportDataDir("")); importErr != nil {
					t.Fatalf("ImportSnapshot: %v", importErr)
				}
				if got := f.importedPayload(t, barrier); !bytes.Equal(got, p.data) {
					t.Fatalf("imported snapshot holds %d bytes, want the %d exported", len(got), len(p.data))
				}
			})
		}
	}
}

// TestImportSnapshot_RefusesADamagedArtifact verifies ImportSnapshot checks
// the header and payload against the trailer before anything is adopted: an
// artifact truncated anywhere after its header, or with a same-length bit flip
// in its payload or in a header field that still parses — the index or term
// the snapshot would be recorded under — is refused, and the refusal leaves no staged or published
// directory and no LogDB record. The intact artifact then imports on the same
// host, so a refusal also leaves nothing behind that blocks a retry.
func TestImportSnapshot_RefusesADamagedArtifact(t *testing.T) {
	payload := bytes.Repeat([]byte("exported-state"), (3*copyWithContextChunkSize+7)/14+1)
	hdrLen := len(buildSnapshotArtifact(t, 42, 1, 0, proto.Membership{}, nil)) - snapshotArtifactTrailerSize
	artifact := buildSnapshotArtifact(t, 42, 1, 0, proto.Membership{}, payload)
	end := len(artifact)

	flip := func(off int) []byte {
		b := bytes.Clone(artifact)
		b[off] ^= 0x40
		return b
	}
	cases := []struct {
		name     string
		artifact []byte
		corrupt  bool // also matches ErrSnapshotCorrupt, not only the format error
	}{
		{name: "header_only", artifact: artifact[:hdrLen]},
		{name: "mid_payload", artifact: artifact[:hdrLen+len(payload)/2]},
		{name: "one_payload_byte_short", artifact: append(bytes.Clone(artifact[:end-snapshotArtifactTrailerSize-1]),
			artifact[end-snapshotArtifactTrailerSize:]...), corrupt: true},
		{name: "trailer_missing", artifact: artifact[:end-snapshotArtifactTrailerSize]},
		{name: "in_trailer", artifact: artifact[:end-snapshotArtifactTrailerSize/2]},
		{name: "last_byte_missing", artifact: artifact[:end-1]},
		{name: "trailing_garbage", artifact: append(bytes.Clone(artifact), 0)},
		{name: "header_index_flip", artifact: flip(12), corrupt: true},
		{name: "header_term_flip", artifact: flip(20), corrupt: true},
		{name: "payload_bit_flip_first", artifact: flip(hdrLen), corrupt: true},
		{name: "payload_bit_flip_middle", artifact: flip(hdrLen + len(payload)/2), corrupt: true},
		{name: "payload_bit_flip_last", artifact: flip(end - snapshotArtifactTrailerSize - 1), corrupt: true},
		{name: "trailer_hash_flip", artifact: flip(end - 1), corrupt: true},
		{name: "trailer_length_flip", artifact: flip(end - 16), corrupt: true},
	}
	for _, v := range artifactVariants {
		for _, tc := range cases {
			t.Run(v.name+"/"+tc.name, func(t *testing.T) {
				f, barrier := newArtifactImportFixture(t, v)
				err := f.host.ImportSnapshot(context.Background(), f.shardID, bytes.NewReader(tc.artifact), WithImportDataDir(""))
				var artErr *SnapshotArtifactError
				if !errors.As(err, &artErr) {
					t.Fatalf("ImportSnapshot of a damaged artifact = %v (%T), want *SnapshotArtifactError", err, err)
				}
				if tc.corrupt && !errors.Is(err, ErrSnapshotCorrupt) {
					t.Fatalf("ImportSnapshot = %v, want it to match ErrSnapshotCorrupt", err)
				}
				f.assertNothingAdopted(t)

				if importErr := f.host.ImportSnapshot(context.Background(), f.shardID,
					bytes.NewReader(artifact), WithImportDataDir("")); importErr != nil {
					t.Fatalf("ImportSnapshot of the intact artifact after a refusal: %v", importErr)
				}
				if got := f.importedPayload(t, barrier); !bytes.Equal(got, payload) {
					t.Fatalf("imported snapshot holds %d bytes, want %d", len(got), len(payload))
				}
			})
		}
	}
}

// TestImportSnapshot_RefusesAnArtifactWithoutATrailer verifies an artifact in
// the layout earlier builds exported — header then payload, nothing after it
// — is refused with the standard invalid-artifact error rather than imported
// unverified, whatever its payload.
func TestImportSnapshot_RefusesAnArtifactWithoutATrailer(t *testing.T) {
	hdr, err := marshalSnapshotArtifactHeader(snapshotArtifactHeader{Index: 42, Term: 1})
	if err != nil {
		t.Fatalf("marshalSnapshotArtifactHeader: %v", err)
	}
	for _, p := range []struct {
		name string
		data []byte
	}{
		{name: "empty", data: nil},
		{name: "shorter_than_a_trailer", data: []byte("short")},
		{name: "large", data: bytes.Repeat([]byte("legacy artifact payload"), 4096)},
	} {
		payload := p.data
		for _, v := range artifactVariants {
			t.Run(v.name+"/"+p.name, func(t *testing.T) {
				f, _ := newArtifactImportFixture(t, v)
				legacy := append(bytes.Clone(hdr), payload...)
				importErr := f.host.ImportSnapshot(context.Background(), f.shardID, bytes.NewReader(legacy), WithImportDataDir(""))
				var artErr *SnapshotArtifactError
				if !errors.As(importErr, &artErr) {
					t.Fatalf("ImportSnapshot of a trailerless artifact = %v (%T), want *SnapshotArtifactError",
						importErr, importErr)
				}
				// It is refused as the wrong format, by the trailer magic, not
				// by an accident of its last bytes failing a hash comparison.
				if artErr.Reason != "missing or malformed trailer" || errors.Is(importErr, ErrSnapshotCorrupt) {
					t.Fatalf("ImportSnapshot of a trailerless artifact = %v, want the malformed-trailer refusal",
						importErr)
				}
				f.assertNothingAdopted(t)
			})
		}
	}
}

// TestImportSnapshot_RefusesAFailedExport verifies what a failed export
// handed its writer can never be imported, even by a caller that ignored the
// export's error. The encrypted case is the sharp one: its frames
// authenticate, so every plaintext byte reaches the writer before the
// recorded-hash check fails, and only the trailer the export withheld keeps
// that output from being a complete artifact.
func TestImportSnapshot_RefusesAFailedExport(t *testing.T) {
	for _, v := range artifactVariants {
		for _, p := range trailerTestPayloads {
			t.Run(v.name+"/"+p.name, func(t *testing.T) {
				partial, expErr := exportTestArtifact(t, v, p.data, true)
				if !errors.Is(expErr, ErrSnapshotCorrupt) {
					t.Fatalf("ExportSnapshot of a corrupt source = %v, want ErrSnapshotCorrupt", expErr)
				}
				for _, iv := range artifactVariants {
					f, _ := newArtifactImportFixture(t, iv)
					importErr := f.host.ImportSnapshot(context.Background(), f.shardID,
						bytes.NewReader(partial), WithImportDataDir(""))
					if importErr == nil {
						t.Fatalf("ImportSnapshot (%s host) accepted the output of a failed export", iv.name)
					}
					f.assertNothingAdopted(t)
				}
				if n := len(partial); n >= snapshotArtifactTrailerSize {
					if _, trErr := parseSnapshotArtifactTrailer(partial[n-snapshotArtifactTrailerSize:]); trErr == nil {
						t.Fatal("a failed export ended its output in a trailer")
					}
				}
			})
		}
	}
}

// testArtifactHeader stands in for the raw artifact header the trailer's hash
// is seeded with, in the tests that exercise the trailer in isolation.
var testArtifactHeader = []byte("artifact header bytes")

// TestArtifactPayloadReader verifies the trailer reader in isolation: it
// yields exactly the payload however its source is chunked, reports io.EOF
// only for a matching trailer under the header it was written for, and never
// hands out payload bytes together with an error.
func TestArtifactPayloadReader(t *testing.T) {
	encode := func(payload []byte) []byte {
		var buf bytes.Buffer
		pw := newArtifactPayloadWriter(&buf, testArtifactHeader)
		if _, err := pw.Write(payload); err != nil {
			t.Fatalf("write payload: %v", err)
		}
		buf.Write(pw.trailer())
		return buf.Bytes()
	}
	payloads := map[string][]byte{
		"empty":        nil,
		"one_byte":     {7},
		"trailer_size": bytes.Repeat([]byte{1}, snapshotArtifactTrailerSize),
		"large":        bytes.Repeat([]byte("0123456789abcdef"), 5000),
	}
	sources := map[string]func(io.Reader) io.Reader{
		"whole":    func(r io.Reader) io.Reader { return r },
		"one_byte": iotest.OneByteReader,
		"half":     iotest.HalfReader,
		"data_eof": iotest.DataErrReader,
	}
	for pname, payload := range payloads {
		for sname, wrap := range sources {
			t.Run(pname+"/"+sname, func(t *testing.T) {
				r := newArtifactPayloadReader(testArtifactHeader, wrap(bytes.NewReader(encode(payload))))
				got, err := io.ReadAll(r)
				if err != nil {
					t.Fatalf("ReadAll: %v", err)
				}
				if !bytes.Equal(got, payload) {
					t.Fatalf("read %d bytes, want %d", len(got), len(payload))
				}
				if n, again := r.Read(make([]byte, 8)); n != 0 || !errors.Is(again, io.EOF) {
					t.Fatalf("Read after the end = (%d, %v), want (0, io.EOF)", n, again)
				}
			})
		}
	}

	t.Run("source_error", func(t *testing.T) {
		errSource := errors.New("test: source failed")
		r := newArtifactPayloadReader(testArtifactHeader, io.MultiReader(bytes.NewReader([]byte("abc")),
			iotest.ErrReader(errSource)))
		if _, err := io.ReadAll(r); !errors.Is(err, errSource) {
			t.Fatalf("ReadAll = %v, want the source error", err)
		}
	})

	t.Run("different_header", func(t *testing.T) {
		other := bytes.Clone(testArtifactHeader)
		other[len(other)-1] ^= 1
		r := newArtifactPayloadReader(other, bytes.NewReader(encode([]byte("payload"))))
		if _, err := io.ReadAll(r); !errors.Is(err, ErrSnapshotCorrupt) {
			t.Fatalf("ReadAll under a header the trailer was not written for = %v, want ErrSnapshotCorrupt", err)
		}
	})

	t.Run("mismatch_yields_no_bytes_with_the_error", func(t *testing.T) {
		b := encode([]byte("payload"))
		b[0] ^= 1
		r := newArtifactPayloadReader(testArtifactHeader, bytes.NewReader(b))
		buf := make([]byte, 64)
		for {
			n, err := r.Read(buf)
			if err != nil {
				if n != 0 {
					t.Fatalf("Read returned %d bytes with the error %v", n, err)
				}
				if !errors.Is(err, ErrSnapshotCorrupt) {
					t.Fatalf("Read = %v, want ErrSnapshotCorrupt", err)
				}
				return
			}
		}
	})
}
