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

// snapshotArtifactTrailerMagic opens the trailer that ends every exported
// snapshot artifact. It is distinct from snapshotArtifactMagic so a reader
// can tell the trailer from the payload it follows.
//
// The trailer binds the whole artifact: the payload's length, and the
// xxhash64 of the header followed by the payload, as the exporter wrote them.
// The header is inside the hash because its fields are as consequential as
// the payload's bytes — a flipped Index, Term or Membership byte would import
// a sound payload at the wrong log position or into the wrong configuration. The artifact header carries no length, so the trailer
// is found by position — it is the last snapshotArtifactTrailerSize bytes of
// the stream — and the magic is what makes that position self-checking. An
// artifact written by a build that predates the trailer ends in payload
// bytes, so its "trailer" fails the magic and the artifact is refused as
// invalid rather than accepted unverified.
//
// No version constant changed for this: the header layout is untouched, and
// the trailer's own magic is what distinguishes the two layouts (see the
// pre-v1 format rule in CLAUDE.md).
var snapshotArtifactTrailerMagic = [8]byte{'Q', 'R', 'S', 'N', 'T', 'R', 'L', 'R'}

// snapshotArtifactTrailerSize is the encoded size of the artifact trailer:
//
//	magic[8] | payloadLen:u64 | hash:u64
const snapshotArtifactTrailerSize = 8 + 8 + 8

// snapshotArtifactTrailer is the integrity record that ends an exported
// artifact: the length of the payload between the header and the trailer,
// and the xxhash64 of every byte before the trailer — header, then payload.
// The payload is the artifact's own bytes — the PLAINTEXT ExportSnapshot
// wrote — not the source snapshot's at-rest form, so an importer checks
// exactly what it is about to adopt.
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

// parseSnapshotArtifactTrailer decodes the trailer from exactly
// snapshotArtifactTrailerSize bytes. A buffer without the trailer magic is
// refused with *SnapshotArtifactError: it is a truncated artifact, trailing
// garbage, or an artifact from a build that wrote no trailer.
func parseSnapshotArtifactTrailer(b []byte) (snapshotArtifactTrailer, error) {
	if len(b) != snapshotArtifactTrailerSize || !bytesEqual(b[0:8], snapshotArtifactTrailerMagic[:]) {
		return snapshotArtifactTrailer{}, &SnapshotArtifactError{Reason: "missing or malformed trailer"}
	}
	return snapshotArtifactTrailer{
		PayloadLen: binary.LittleEndian.Uint64(b[8:16]),
		Hash:       binary.LittleEndian.Uint64(b[16:24]),
	}, nil
}

// check compares the trailer against what an importer actually read: n
// payload bytes, and sum over the header and those bytes.
// A mismatch is *SnapshotArtifactError wrapping ErrSnapshotCorrupt, so a
// caller can match either the artifact failure or the corruption.
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

// artifactPayloadWriter is the export side of the trailer. Every payload byte
// ExportSnapshot emits passes through it, and it records the length and hash
// of what the underlying writer ACCEPTED, so the trailer describes the bytes
// that are in the artifact rather than the bytes that were offered. The hash
// is seeded with the artifact header.
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

// trailer returns the encoded trailer for everything written so far. It
// must be written only once the export has passed every check: without it
// the artifact is refused on import, which is what makes an export that
// failed part-way — having already handed w some of its payload — harmless.
func (a *artifactPayloadWriter) trailer() []byte {
	return snapshotArtifactTrailer{PayloadLen: a.n, Hash: a.digest.Sum64()}.marshal()
}

// artifactPayloadReader is the import side of the trailer. It yields the
// artifact's payload — every byte after the header EXCEPT the trailing
// snapshotArtifactTrailerSize — hashing it as it goes, and at the end of the
// stream checks what it yielded against the trailer it withheld. The check
// runs inside the stream, so a mismatch fails the copy that writes the
// staged snapshot.dat, before anything is published or recorded.
//
// It never returns payload bytes together with an error: a Read either
// yields bytes or reports the outcome, so a consumer that stops at the first
// error has written nothing the verdict applies to.
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
