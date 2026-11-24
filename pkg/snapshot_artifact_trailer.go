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
	"encoding/binary"
	"errors"
	"fmt"
	"io"

	"github.com/cespare/xxhash/v2"
)

// snapshotArtifactTrailerMagic opens the trailer at the end of every exported artifact, which binds the payload length and the xxhash64 of header plus payload; an artifact without it is refused.
var snapshotArtifactTrailerMagic = [8]byte{'Q', 'R', 'S', 'N', 'T', 'R', 'L', 'R'}

// snapshotArtifactTrailerSize is the encoded size of the artifact trailer:
//
//	magic[8] | payloadLen:u64 | hash:u64
const snapshotArtifactTrailerSize = 8 + 8 + 8

// snapshotArtifactTrailer ends an exported artifact: the payload length and the xxhash64 of the header and plaintext payload.
type snapshotArtifactTrailer struct {
	PayloadLen uint64
	Hash       uint64
}

// marshal encodes the trailer into its fixed little-endian layout.
func (t snapshotArtifactTrailer) marshal() []byte {
	buf := make([]byte, snapshotArtifactTrailerSize)
	copy(buf[0:8], snapshotArtifactTrailerMagic[:])
	binary.LittleEndian.PutUint64(buf[8:16], t.PayloadLen)
	binary.LittleEndian.PutUint64(buf[16:24], t.Hash)
	return buf
}

// parseSnapshotArtifactTrailer decodes snapshotArtifactTrailerSize bytes; without the magic it returns *SnapshotArtifactError.
func parseSnapshotArtifactTrailer(b []byte) (snapshotArtifactTrailer, error) {
	if len(b) != snapshotArtifactTrailerSize || !bytesEqual(b[0:8], snapshotArtifactTrailerMagic[:]) {
		return snapshotArtifactTrailer{}, &SnapshotArtifactError{Reason: "missing or malformed trailer"}
	}
	return snapshotArtifactTrailer{
		PayloadLen: binary.LittleEndian.Uint64(b[8:16]),
		Hash:       binary.LittleEndian.Uint64(b[16:24]),
	}, nil
}

// check compares the trailer with n payload bytes read and sum; a mismatch is *SnapshotArtifactError wrapping ErrSnapshotCorrupt.
func (t snapshotArtifactTrailer) check(n, sum uint64) error {
	if t.PayloadLen != n {
		return &SnapshotArtifactError{
			Reason: fmt.Sprintf("payload length %d does not match the trailer's %d", n, t.PayloadLen),
			Err:    ErrSnapshotCorrupt,
		}
	}
	if t.Hash != sum {
		return &SnapshotArtifactError{Reason: "artifact hash does not match the trailer", Err: ErrSnapshotCorrupt}
	}
	return nil
}

// artifactPayloadWriter records the length and hash (seeded with the header) of the payload bytes the writer accepted.
type artifactPayloadWriter struct {
	w      io.Writer
	digest *xxhash.Digest
	n      uint64
}

// newArtifactPayloadWriter returns a payload writer over w for an artifact
// whose encoded header is header. The header is hashed, not written: the
// caller emits it.
func newArtifactPayloadWriter(w io.Writer, header []byte) *artifactPayloadWriter {
	return &artifactPayloadWriter{w: w, digest: seededArtifactDigest(header)}
}

// seededArtifactDigest returns an xxhash64 digest that has already consumed
// the artifact header, which is where the trailer's hash begins.
func seededArtifactDigest(header []byte) *xxhash.Digest {
	d := xxhash.New()
	_, _ = d.Write(header) // xxhash.Digest.Write never returns an error
	return d
}

// Write writes p to the artifact and adds what was written to the running
// length and hash.
func (a *artifactPayloadWriter) Write(p []byte) (int, error) {
	n, err := a.w.Write(p)
	if n > 0 {
		_, _ = a.digest.Write(p[:n]) // xxhash.Digest.Write never returns an error
		a.n += uint64(n)
	}
	return n, err
}

// trailer returns the encoded trailer; write it only after the export passed every check.
func (a *artifactPayloadWriter) trailer() []byte {
	return snapshotArtifactTrailer{PayloadLen: a.n, Hash: a.digest.Sum64()}.marshal()
}

// artifactPayloadReader yields the payload while hashing it and checks the withheld trailer at end of stream; it never returns bytes together with an error.
type artifactPayloadReader struct {
	r      io.Reader
	buf    []byte // read from r, not yet yielded; its last trailer-size bytes are withheld
	eof    bool
	done   error // the verdict, once the stream has ended
	digest *xxhash.Digest
	n      uint64
}

// artifactPayloadReadSize is how much the reader requests from its source at
// a time, beyond the bytes it must withhold.
const artifactPayloadReadSize = 32 * 1024

// newArtifactPayloadReader returns a payload reader over the post-header
// bytes of an artifact whose header was read as header, the raw bytes
// readSnapshotArtifactHeader returns.
func newArtifactPayloadReader(header []byte, r io.Reader) *artifactPayloadReader {
	return &artifactPayloadReader{
		r:      r,
		buf:    make([]byte, 0, snapshotArtifactTrailerSize+artifactPayloadReadSize),
		digest: seededArtifactDigest(header),
	}
}

// Read yields payload bytes. Once the source is exhausted it returns io.EOF
// if the withheld trailer matches the payload yielded, and the refusal
// otherwise.
func (a *artifactPayloadReader) Read(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	for !a.eof && len(a.buf) <= snapshotArtifactTrailerSize {
		if err := a.fill(); err != nil {
			return 0, err
		}
	}
	if avail := len(a.buf) - snapshotArtifactTrailerSize; avail > 0 {
		n := copy(p, a.buf[:avail])
		_, _ = a.digest.Write(p[:n]) // xxhash.Digest.Write never returns an error
		a.n += uint64(n)
		a.buf = a.buf[:copy(a.buf, a.buf[n:])]
		return n, nil
	}
	if a.done == nil {
		a.done = a.verdict()
	}
	return 0, a.done
}

// fill reads more of the source into buf. It is called only while buf holds
// no more than the withheld trailer, so buf always has room.
func (a *artifactPayloadReader) fill() error {
	n, err := a.r.Read(a.buf[len(a.buf):cap(a.buf)])
	a.buf = a.buf[:len(a.buf)+n]
	if errors.Is(err, io.EOF) {
		a.eof = true
		return nil
	}
	return err
}

// verdict checks the withheld trailer against the payload yielded. It is
// called once the source is exhausted, when buf holds at most the trailer.
func (a *artifactPayloadReader) verdict() error {
	t, err := parseSnapshotArtifactTrailer(a.buf)
	if err != nil {
		return err
	}
	if err := t.check(a.n, a.digest.Sum64()); err != nil {
		return err
	}
	return io.EOF
}
