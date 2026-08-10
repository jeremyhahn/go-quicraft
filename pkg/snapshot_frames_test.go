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
	"encoding/binary"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/jeremyhahn/go-quicraft/pkg/config"
	"github.com/jeremyhahn/go-quicraft/pkg/proto"
)

// errTestWriteRefused is returned by a sink that cannot accept more data
// (a full volume, a closed file).
var errTestWriteRefused = errors.New("test: write refused")

// failingWriter accepts nothing.
type failingWriter struct{}

func (failingWriter) Write([]byte) (int, error) { return 0, errTestWriteRefused }

// errTestReadFailed models an unreadable snapshot source.
var errTestReadFailed = errors.New("test: read failed")

// failingReader fails after delivering prefix, so the encoder has already
// produced output when the source dies.
type failingReader struct {
	prefix []byte
	done   bool
}

func (r *failingReader) Read(p []byte) (int, error) {
	if r.done {
		return 0, errTestReadFailed
	}
	r.done = true
	n := copy(p, r.prefix)
	return n, nil
}

// ---------------------------------------------------------------------------
// decryptSnapshotFrames
// ---------------------------------------------------------------------------

// TestDecryptSnapshotFrames_RejectsCorruptFrameLength verifies that an
// oversized frame-length prefix is rejected BEFORE the allocation it
// describes. A snapshot file is attacker- or corruption-influenced input, so
// trusting the prefix would let a single bogus 4-byte field drive an arbitrary
// allocation.
func TestDecryptSnapshotFrames_RejectsCorruptFrameLength(t *testing.T) {
	b := createTestBarrier(t)

	var frameLen [4]byte
	binary.LittleEndian.PutUint32(frameLen[:], maxSnapshotFrameLen+1)

	var out bytes.Buffer
	// Only the 4-byte prefix is supplied: if the length were trusted, the
	// decoder would allocate before discovering the body is missing.
	n, err := decryptSnapshotFrames(b, testSnapShardID, testSnapReplicaID, testSnapIndex, bytes.NewReader(frameLen[:]), &out)
	if !errors.Is(err, ErrSnapshotCorrupt) {
		t.Fatalf("decryptSnapshotFrames error = %v, want ErrSnapshotCorrupt", err)
	}
	if n != 0 || out.Len() != 0 {
		t.Fatalf("decryptSnapshotFrames wrote %d bytes (n=%d) for a corrupt frame", out.Len(), n)
	}
}

// TestDecryptSnapshotFrames_RejectsTruncatedFrame verifies that a frame whose
// body is shorter than its declared length fails instead of decrypting
// whatever bytes happen to be present.
func TestDecryptSnapshotFrames_RejectsTruncatedFrame(t *testing.T) {
	b := createTestBarrier(t)

	var frameLen [4]byte
	binary.LittleEndian.PutUint32(frameLen[:], 128)
	truncated := append(frameLen[:], make([]byte, 16)...)

	var out bytes.Buffer
	if _, err := decryptSnapshotFrames(b, testSnapShardID, testSnapReplicaID, testSnapIndex, bytes.NewReader(truncated), &out); err == nil {
		t.Fatal("decryptSnapshotFrames accepted a truncated frame")
	}
	if out.Len() != 0 {
		t.Fatalf("decryptSnapshotFrames emitted %d bytes from a truncated frame", out.Len())
	}
}

// TestDecryptSnapshotFrames_PropagatesSinkFailure verifies that a destination
// that cannot accept the plaintext aborts the decrypt rather than reporting a
// short but successful result.
func TestDecryptSnapshotFrames_PropagatesSinkFailure(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "snapshot.dat")
	b := createTestBarrier(t)
	writeEncryptedSnapshotFile(t, b, path, []byte("plaintext payload"))

	src, err := os.Open(path)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = src.Close() }()

	n, err := decryptSnapshotFrames(b, testSnapShardID, testSnapReplicaID, testSnapIndex, src, failingWriter{})
	if !errors.Is(err, errTestWriteRefused) {
		t.Fatalf("decryptSnapshotFrames error = %v, want the sink error", err)
	}
	if n != 0 {
		t.Fatalf("decryptSnapshotFrames returned size %d with an error, want 0", n)
	}
}

// ---------------------------------------------------------------------------
// encryptSnapshotFrames
// ---------------------------------------------------------------------------

// TestEncryptSnapshotFrames_PropagatesSinkFailure verifies that a write
// failure aborts the encrypt with the underlying error and a zero epoch, so no
// caller records an at-rest epoch for a file that was never fully written.
func TestEncryptSnapshotFrames_PropagatesSinkFailure(t *testing.T) {
	b := createTestBarrier(t)

	epoch, _, err := encryptSnapshotFrames(b, testSnapShardID, testSnapReplicaID, testSnapIndex, bytes.NewReader([]byte("payload")), failingWriter{})
	if !errors.Is(err, errTestWriteRefused) {
		t.Fatalf("encryptSnapshotFrames error = %v, want the sink error", err)
	}
	if epoch != 0 {
		t.Fatalf("encryptSnapshotFrames returned epoch %d on failure, want 0", epoch)
	}
}

// TestEncryptSnapshotFrames_PropagatesSourceFailure verifies that a source
// that dies mid-stream fails the encrypt rather than silently truncating the
// snapshot at the point of failure.
func TestEncryptSnapshotFrames_PropagatesSourceFailure(t *testing.T) {
	b := createTestBarrier(t)

	var out bytes.Buffer
	epoch, _, err := encryptSnapshotFrames(b, testSnapShardID, testSnapReplicaID, testSnapIndex, &failingReader{prefix: []byte("partial")}, &out)
	if !errors.Is(err, errTestReadFailed) {
		t.Fatalf("encryptSnapshotFrames error = %v, want the source error", err)
	}
	if epoch != 0 {
		t.Fatalf("encryptSnapshotFrames returned epoch %d on failure, want 0", epoch)
	}
}

// TestReencryptReceivedSnapshot_FailsClosedWhenSealed verifies the fail-closed
// rule on the receive path: with a CONFIGURED but sealed barrier the received
// plaintext must not be committed to disk, because doing so would durably
// downgrade a shard that is required to be encrypted at rest.
func TestReencryptReceivedSnapshot_FailsClosedWhenSealed(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "snapshot.dat")
	plaintext := []byte("received plaintext snapshot")
	if err := os.WriteFile(path, plaintext, 0o600); err != nil {
		t.Fatalf("write snapshot: %v", err)
	}

	sealed := &mockBarrier{}
	if err := sealed.Seal(); err != nil {
		t.Fatalf("seal: %v", err)
	}

	epoch, _, err := reencryptReceivedSnapshot(osSnapshotFS{}, path, testSnapShardID, testSnapReplicaID, testSnapIndex, sealed)
	if !errors.Is(err, ErrSnapshotBarrierSealed) {
		t.Fatalf("reencryptReceivedSnapshot error = %v, want ErrSnapshotBarrierSealed", err)
	}
	if epoch != 0 {
		t.Fatalf("epoch = %d, want 0 when the barrier is sealed", epoch)
	}

	// The file must be untouched: no partial re-encryption, no truncation.
	got, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read back: %v", err)
	}
	if !bytes.Equal(got, plaintext) {
		t.Fatalf("snapshot.dat was modified by a refused re-encryption: %q", got)
	}
}

// ---------------------------------------------------------------------------
// classifySnapshotCompression
// ---------------------------------------------------------------------------

// TestClassifySnapshotCompression verifies that a received artifact is
// classified by its own bytes: the wire protocol carries no compression field,
// so misclassifying it would hand the state machine data in an encoding it
// cannot read.
//
// It classifies the reassembled chunks in MEMORY rather than reading the file
// back, because the artifact the receive path writes is encrypted from its
// first byte — there is no plaintext file on disk to read the identifier out
// of. Classifying AFTER the write is what forced the old two-step
// plaintext-then-encrypt receive.
func TestClassifySnapshotCompression(t *testing.T) {
	for _, tc := range []struct {
		name string
		head []byte
		want config.CompressionType
	}{
		{
			name: "snappy stream identifier",
			head: append(append([]byte{}, snappyStreamIdentifier[:]...), []byte("body")...),
			want: config.Snappy,
		},
		{
			name: "raw artifact",
			head: []byte("plain snapshot bytes"),
			want: config.NoCompression,
		},
		{
			// Shorter than the identifier: cannot be a Snappy stream, and both
			// reader chains yield EOF, so raw is the only safe answer.
			name: "short prefix",
			head: snappyStreamIdentifier[:3],
			want: config.NoCompression,
		},
		{
			name: "empty artifact",
			head: nil,
			want: config.NoCompression,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := classifySnapshotCompression(tc.head); got != tc.want {
				t.Fatalf("classifySnapshotCompression(%q) = %v, want %v; the "+
					"receiver would build the wrong reader chain and hand the "+
					"state machine unreadable bytes", tc.head, got, tc.want)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// forEachSnapshotChunk
// ---------------------------------------------------------------------------

// TestForEachSnapshotChunk_MissingFile verifies that a snapshot whose data
// file has vanished is reported as a send failure naming the shard and target
// replica, instead of streaming a metadata-only chunk that would make the
// follower mark the snapshot index applied with no data behind it.
func TestForEachSnapshotChunk_MissingFile(t *testing.T) {
	msg := proto.Message{
		Type:    proto.InstallSnapshot,
		ShardID: 1,
		From:    1,
		To:      2,
		Snapshot: proto.Snapshot{
			Index:    100,
			Term:     5,
			Filepath: filepath.Join(t.TempDir(), "does-not-exist.dat"),
			FileSize: 64,
		},
	}

	var emitted int
	err := forEachSnapshotChunk(osSnapshotFS{}, msg, func(proto.SnapshotChunk) error {
		emitted++
		return nil
	})
	if err == nil {
		t.Fatal("forEachSnapshotChunk accepted a snapshot whose data file is missing")
	}
	var sendErr *SnapshotSendError
	if !errors.As(err, &sendErr) {
		t.Fatalf("error = %v (%T), want *SnapshotSendError", err, err)
	}
	if sendErr.ShardID != 1 || sendErr.ReplicaID != 2 {
		t.Fatalf("error names shard %d replica %d, want shard 1 replica 2", sendErr.ShardID, sendErr.ReplicaID)
	}
	if emitted != 0 {
		t.Fatalf("forEachSnapshotChunk emitted %d chunks for a missing file", emitted)
	}
}

// ---------------------------------------------------------------------------
// snapshotFrameEpochs
// ---------------------------------------------------------------------------

// errTestSeekFailed models an unseekable snapshot source.
var errTestSeekFailed = errors.New("test: seek failed")

// faultyReadSeeker serves data but can fail the read of the length prefix or
// the skip to the next frame.
type faultyReadSeeker struct {
	inner    *bytes.Reader
	failRead bool
	failSeek bool
}

func (f *faultyReadSeeker) Read(p []byte) (int, error) {
	if f.failRead {
		return 0, errTestReadFailed
	}
	return f.inner.Read(p)
}

func (f *faultyReadSeeker) Seek(offset int64, whence int) (int64, error) {
	if f.failSeek {
		return 0, errTestSeekFailed
	}
	return f.inner.Seek(offset, whence)
}

// synthFrames builds frames carrying only the 8-byte big-endian epoch header
// plus filler, which is all snapshotFrameEpochs reads.
func synthFrames(t *testing.T, epochs ...uint64) []byte {
	t.Helper()
	var out bytes.Buffer
	for _, epoch := range epochs {
		frame := make([]byte, 8+16)
		binary.BigEndian.PutUint64(frame[:8], epoch)
		var lenBuf [4]byte
		binary.LittleEndian.PutUint32(lenBuf[:], uint32(len(frame)))
		out.Write(lenBuf[:])
		out.Write(frame)
	}
	return out.Bytes()
}

// TestSnapshotFrameEpochs_RejectsCorruptFrameLength verifies that the
// epoch prescan refuses a frame length that cannot describe a real frame —
// larger than the framing ceiling, or too small to even contain the epoch
// header — and still reports the epochs it had already collected so the caller
// can pin them.
func TestSnapshotFrameEpochs_RejectsCorruptFrameLength(t *testing.T) {
	var lenBuf [4]byte

	t.Run("oversized", func(t *testing.T) {
		data := synthFrames(t, 7)
		binary.LittleEndian.PutUint32(lenBuf[:], maxSnapshotFrameLen+1)
		data = append(data, lenBuf[:]...)

		epochs, err := snapshotFrameEpochs(bytes.NewReader(data))
		if !errors.Is(err, ErrSnapshotCorrupt) {
			t.Fatalf("snapshotFrameEpochs error = %v, want ErrSnapshotCorrupt", err)
		}
		if len(epochs) != 1 || epochs[0] != 7 {
			t.Fatalf("epochs = %v, want the epochs of the complete frames [7]", epochs)
		}
	})

	t.Run("shorter than the epoch header", func(t *testing.T) {
		data := synthFrames(t, 7)
		binary.LittleEndian.PutUint32(lenBuf[:], 4)
		data = append(data, lenBuf[:]...)

		epochs, err := snapshotFrameEpochs(bytes.NewReader(data))
		if !errors.Is(err, ErrSnapshotCorrupt) {
			t.Fatalf("snapshotFrameEpochs error = %v, want ErrSnapshotCorrupt", err)
		}
		if len(epochs) != 1 || epochs[0] != 7 {
			t.Fatalf("epochs = %v, want [7]", epochs)
		}
	})
}

// TestSnapshotFrameEpochs_PropagatesSourceFailures verifies that an I/O error
// while scanning is surfaced rather than being mistaken for end-of-file, which
// would under-report the epochs to pin and let a concurrent purge drop a DEK a
// later frame still needs.
func TestSnapshotFrameEpochs_PropagatesSourceFailures(t *testing.T) {
	t.Run("length prefix unreadable", func(t *testing.T) {
		src := &faultyReadSeeker{inner: bytes.NewReader(synthFrames(t, 7)), failRead: true}
		epochs, err := snapshotFrameEpochs(src)
		if !errors.Is(err, errTestReadFailed) {
			t.Fatalf("snapshotFrameEpochs error = %v, want the read failure", err)
		}
		if len(epochs) != 0 {
			t.Fatalf("epochs = %v, want none", epochs)
		}
	})

	t.Run("frame body unskippable", func(t *testing.T) {
		src := &faultyReadSeeker{inner: bytes.NewReader(synthFrames(t, 7, 8)), failSeek: true}
		epochs, err := snapshotFrameEpochs(src)
		if !errors.Is(err, errTestSeekFailed) {
			t.Fatalf("snapshotFrameEpochs error = %v, want the seek failure", err)
		}
		if len(epochs) != 1 || epochs[0] != 7 {
			t.Fatalf("epochs = %v, want the first frame's epoch [7]", epochs)
		}
	})
}

// ---------------------------------------------------------------------------
// reencryptSnapshotFile
// ---------------------------------------------------------------------------

// TestReencryptSnapshotFile_FilesystemFailures verifies that a rekey which
// cannot create its working files fails without touching the source snapshot
// or leaving a scratch file behind. The source is the only copy of the data at
// that moment, so a partial rekey that damaged it would be unrecoverable.
func TestReencryptSnapshotFile_FilesystemFailures(t *testing.T) {
	tests := []struct {
		name    string
		fsys    faultySnapshotFS
		wantErr error
	}{
		{
			name:    "source cannot be opened",
			fsys:    faultySnapshotFS{failOpen: true},
			wantErr: errTestOpenFailed,
		},
		{
			name:    "temp file cannot be created",
			fsys:    faultySnapshotFS{failCreateTemp: true},
			wantErr: errTestCreateTempFailed,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dir := t.TempDir()
			srcPath := filepath.Join(dir, "snapshot.dat")
			srcBarrier := createTestBarrier(t)
			dstBarrier := createTestBarrier(t)
			writeEncryptedSnapshotFile(t, srcBarrier, srcPath, []byte("rekey payload"))

			original, err := os.ReadFile(srcPath)
			if err != nil {
				t.Fatalf("read source: %v", err)
			}

			dstPath := filepath.Join(dir, "dst", "snapshot.dat")
			epoch, err := reencryptSnapshotFile(tt.fsys, srcPath, dstPath, testSnapShardID, testSnapReplicaID, testSnapIndex, srcBarrier, dstBarrier)
			if !errors.Is(err, tt.wantErr) {
				t.Fatalf("reencryptSnapshotFile error = %v, want %v", err, tt.wantErr)
			}
			if epoch != 0 {
				t.Fatalf("epoch = %d, want 0 on failure", epoch)
			}
			if _, statErr := os.Stat(dstPath); statErr == nil {
				t.Fatal("failed rekey produced a destination file")
			}
			after, err := os.ReadFile(srcPath)
			if err != nil {
				t.Fatalf("re-read source: %v", err)
			}
			if !bytes.Equal(original, after) {
				t.Fatal("failed rekey modified the source snapshot")
			}
		})
	}
}

// ---------------------------------------------------------------------------
// M3: positional context-AAD binding of snapshot frames
//
// Every frame is sealed against magic || ShardID || ReplicaID || SnapshotIndex
// || ChunkSeq. These tests run over the REAL seal.Barrier, so they assert the
// AEAD binding itself rather than a stand-in: a frame that is moved to another
// ordinal, or read under another artifact identity, must fail to authenticate.
// There is no weaker retry anywhere in the reader, by design — an epoch-only
// fallback would readmit exactly the splicing this binding prevents.
// ---------------------------------------------------------------------------

// splitSnapshotFrames decomposes the on-disk framing
// ([ChunkLen:4 LE][AEAD frame])* into its frames.
func splitSnapshotFrames(t *testing.T, blob []byte) [][]byte {
	t.Helper()
	var frames [][]byte
	for off := 0; off < len(blob); {
		if off+4 > len(blob) {
			t.Fatalf("truncated frame header at offset %d", off)
		}
		n := int(binary.LittleEndian.Uint32(blob[off : off+4]))
		off += 4
		if off+n > len(blob) {
			t.Fatalf("truncated frame body at offset %d", off)
		}
		frames = append(frames, blob[off:off+n])
		off += n
	}
	return frames
}

// joinSnapshotFrames is splitSnapshotFrames' inverse.
func joinSnapshotFrames(frames [][]byte) []byte {
	var out []byte
	var lb [4]byte
	for _, f := range frames {
		binary.LittleEndian.PutUint32(lb[:], uint32(len(f)))
		out = append(out, lb[:]...)
		out = append(out, f...)
	}
	return out
}

// multiFramePlaintext is large enough to span several encrypt chunks, so
// reordering is expressible.
func multiFramePlaintext() []byte {
	p := make([]byte, snapshotEncryptChunkSize*3+17)
	for i := range p {
		p[i] = byte(i*31 + 7)
	}
	return p
}

// TestSnapshotFrameAAD_RoundTripUnderMatchingContext is the positive case: the
// encrypt and decrypt sides derive the same context from the same identity, so
// the artifact round-trips byte for byte.
func TestSnapshotFrameAAD_RoundTripUnderMatchingContext(t *testing.T) {
	b := createTestBarrier(t)
	plain := multiFramePlaintext()

	var enc bytes.Buffer
	if _, _, err := encryptSnapshotFrames(b, 4, 2, 900, bytes.NewReader(plain), &enc); err != nil {
		t.Fatalf("encryptSnapshotFrames: %v", err)
	}
	if got := len(splitSnapshotFrames(t, enc.Bytes())); got < 3 {
		t.Fatalf("fixture produced %d frames, want the multi-frame path", got)
	}

	var dec bytes.Buffer
	if _, err := decryptSnapshotFrames(b, 4, 2, 900, bytes.NewReader(enc.Bytes()), &dec); err != nil {
		t.Fatalf("decryptSnapshotFrames under the matching context: %v", err)
	}
	if !bytes.Equal(dec.Bytes(), plain) {
		t.Fatalf("round-trip mismatch: %d bytes back, want %d", dec.Len(), len(plain))
	}
}

// TestSnapshotFrameAAD_ReorderedFrameFailsToDecrypt proves the ORDINAL half of
// the binding. Two frames are swapped in place; the framing is still
// structurally valid and every frame is still authentic under its own epoch, so
// an epoch-only AAD would decrypt the file happily and hand the state machine a
// silently reordered image.
func TestSnapshotFrameAAD_ReorderedFrameFailsToDecrypt(t *testing.T) {
	b := createTestBarrier(t)
	plain := multiFramePlaintext()

	var enc bytes.Buffer
	if _, _, err := encryptSnapshotFrames(b, 4, 2, 900, bytes.NewReader(plain), &enc); err != nil {
		t.Fatalf("encryptSnapshotFrames: %v", err)
	}
	frames := splitSnapshotFrames(t, enc.Bytes())
	if len(frames) < 3 {
		t.Fatalf("fixture produced %d frames, want at least 3", len(frames))
	}
	// Frames 0 and 1 are the same size (both full chunks), so the swap changes
	// nothing but the ORDER.
	frames[0], frames[1] = frames[1], frames[0]

	var dec bytes.Buffer
	if _, err := decryptSnapshotFrames(b, 4, 2, 900,
		bytes.NewReader(joinSnapshotFrames(frames)), &dec); err == nil {
		t.Fatal("a reordered snapshot decrypted successfully: frames are not bound to " +
			"their ordinal, so an at-rest attacker can permute the state machine image")
	}
}

// TestSnapshotFrameAAD_TruncatedTailFailsWhenSpliced proves a frame lifted out
// of one artifact cannot be pasted into another at a different ordinal: the
// last frame of a 4-frame file is replayed as frame 0 of a 1-frame file.
func TestSnapshotFrameAAD_ReplayedFrameAtAnotherOrdinalFails(t *testing.T) {
	b := createTestBarrier(t)

	var enc bytes.Buffer
	if _, _, err := encryptSnapshotFrames(b, 4, 2, 900,
		bytes.NewReader(multiFramePlaintext()), &enc); err != nil {
		t.Fatalf("encryptSnapshotFrames: %v", err)
	}
	frames := splitSnapshotFrames(t, enc.Bytes())
	last := frames[len(frames)-1]

	var dec bytes.Buffer
	if _, err := decryptSnapshotFrames(b, 4, 2, 900,
		bytes.NewReader(joinSnapshotFrames([][]byte{last})), &dec); err == nil {
		t.Fatal("a trailing frame authenticated as frame 0: the ordinal is not bound")
	}
}

// TestSnapshotFrameAAD_ForeignArtifactIdentityFailsToDecrypt proves the
// ARTIFACT half of the binding. The default barrier is host-wide and shared by
// every shard, so shard/replica/index must be in the context or a frame could
// be spliced between two shards holding a snapshot at the same index.
func TestSnapshotFrameAAD_ForeignArtifactIdentityFailsToDecrypt(t *testing.T) {
	b := createTestBarrier(t)
	plain := multiFramePlaintext()

	var enc bytes.Buffer
	if _, _, err := encryptSnapshotFrames(b, 4, 2, 900, bytes.NewReader(plain), &enc); err != nil {
		t.Fatalf("encryptSnapshotFrames: %v", err)
	}

	for _, tc := range []struct {
		name                      string
		shardID, replicaID, index uint64
	}{
		{"other_shard", 5, 2, 900},
		{"other_replica", 4, 3, 900},
		{"other_snapshot_index", 4, 2, 901},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var dec bytes.Buffer
			if _, err := decryptSnapshotFrames(b, tc.shardID, tc.replicaID, tc.index,
				bytes.NewReader(enc.Bytes()), &dec); err == nil {
				t.Fatalf("frames sealed for (4,2,900) decrypted under (%d,%d,%d)",
					tc.shardID, tc.replicaID, tc.index)
			}
		})
	}
}

// TestSnapshotFrameAAD_EpochOnlyCiphertextIsRejected pins the ABSENCE of a
// fallback. A frame sealed with NO context (the pre-M3 shape, and what a
// downgrade attacker would supply) must not decrypt through the reader, which
// never retries with a weaker AAD.
func TestSnapshotFrameAAD_EpochOnlyCiphertextIsRejected(t *testing.T) {
	b := createTestBarrier(t)

	ct, err := b.Encrypt(nil, []byte("epoch-only frame with no positional binding"))
	if err != nil {
		t.Fatalf("Encrypt: %v", err)
	}

	var dec bytes.Buffer
	if _, err := decryptSnapshotFrames(b, 4, 2, 900,
		bytes.NewReader(joinSnapshotFrames([][]byte{ct})), &dec); err == nil {
		t.Fatal("an epoch-only frame decrypted: the reader fell back to a weaker AAD")
	}
}
