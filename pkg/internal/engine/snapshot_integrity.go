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
	"io"
	"os"
	"path/filepath"

	"github.com/cespare/xxhash/v2"

	"github.com/jeremyhahn/go-quicraft/pkg/logdb"
)

// hashSnapshotData returns the xxhash64 and byte length of the snapshot data
// file at path, as written — after compression and after encryption.
//
// Hashing the FINAL bytes is what lets one recorded value be verified by all
// three consumers (recovery, the streamed receive path, ImportSnapshot)
// without any of them holding a key. It is deliberately NOT a cryptographic
// hash: the threat it addresses is bit rot, a remapped sector or a truncated
// restore, not an adversary who can rewrite the artifact — an attacker with
// write access to the data file can rewrite snapshot.meta beside it. Integrity
// against a hostile writer is the barrier's job (per-frame AEAD with
// positional AAD binding), and this closes the gap for deployments that run
// without one.
func hashSnapshotData(path string) (sum uint64, size uint64, err error) {
	f, err := os.Open(path)
	if err != nil {
		return 0, 0, err
	}
	defer func() {
		// A read-only handle's Close error cannot indicate lost data, so it
		// does not override a successful hash.
		_ = f.Close()
	}()
	return HashSnapshotReader(f)
}

// HashSnapshotReader returns the xxhash64 and byte count of everything r
// yields: the DataHash/DataLen pair a snapshot.meta record stamps over its
// snapshot.dat. It is the single implementation of that value, exported for
// the receive and import paths in package quicraft, which read the artifact
// through their own filesystem abstraction and must stamp exactly what this
// package verifies. A read error MUST fail the write it was stamping for: a
// record written without a real hash is refused by every verifier.
func HashSnapshotReader(r io.Reader) (sum uint64, size uint64, err error) {
	h := xxhash.New()
	n, err := io.Copy(h, r)
	if err != nil {
		return 0, 0, err
	}
	return h.Sum64(), uint64(n), nil
}

// checkSnapshotData checks a data file's hash and length, supplied by the
// caller, against the integrity fields recorded in its snapshot.meta. Taking
// the hash from the caller lets a path that hashed the bytes while it read
// them — the send and export paths, which must not read the artifact twice —
// be held to exactly the comparison recovery makes. hash is called only when
// the record carries integrity fields.
//
// A record without those fields fails CLOSED, like every other disagreement:
// every writer stamps them before a directory is published, so their absence
// means the record is not one this format produced (see
// SnapshotMetaInvalidError). A mismatch is ErrSnapshotCorrupt: a corrupt
// snapshot must never be recovered into the state machine, and falling back to
// an older directory is not an option either — that would silently roll the
// replica backwards.
func checkSnapshotData(dir string, meta SnapshotMeta, hash func() (sum uint64, size uint64, err error)) error {
	if !meta.DataHashRecorded {
		return &SnapshotMetaInvalidError{Dir: dir}
	}
	sum, size, err := hash()
	if err != nil {
		return err
	}
	if size != meta.DataLen || sum != meta.DataHash {
		return ErrSnapshotCorrupt
	}
	return nil
}

// readSnapshotMetaFile reads and parses the snapshot.meta record in dir.
//
// It uses the same bounded read readSnapshotEpoch uses, so a corrupt or
// substituted multi-gigabyte file is refused rather than slurped. A missing,
// unreadable or unparseable record is reported as *SnapshotMetaInvalidError,
// which matches ErrSnapshotCorrupt: every consumer that verifies integrity
// fails closed on it. The record is written into the staging directory before
// any snapshot is published, so a published directory without a usable one
// has lost it after the fact.
func readSnapshotMetaFile(dir string) (SnapshotMeta, error) {
	data, err := readBoundedSnapshotMeta(filepath.Join(dir, snapshotMetadataFile))
	if err != nil {
		return SnapshotMeta{}, &SnapshotMetaInvalidError{Dir: dir, Err: err}
	}
	meta, err := ParseSnapshotMeta(data)
	if err != nil {
		return SnapshotMeta{}, &SnapshotMetaInvalidError{Dir: dir, Err: err}
	}
	return meta, nil
}

// VerifySnapshotArtifact checks the snapshot data file at dataPath against the
// integrity fields recorded in the snapshot.meta inside dir.
//
// Exported because the check has three consumers and only one of them lives in
// this package: the runtime recovery path (snapshotPool.recoverSnapshot), the
// startup recovery path (Host.StartShard) and the operator import path
// (Host.ImportSnapshot). Wiring some and not others reproduces the state the
// removed Checksum field was in — an integrity control that reads as present
// and verifies nothing.
//
// It fails CLOSED when there is nothing to check against: a snapshot.meta that
// is missing, unreadable, malformed, or lacks the integrity fields — a pre-magic
// (v0) record among them, since no current writer produces one — is refused
// with *SnapshotMetaInvalidError, which matches ErrSnapshotCorrupt. Every
// writer (save, receive, import) stamps the fields through MarshalSnapshotMeta
// before the directory is published, and pre-v1 layouts are wiped rather than
// migrated, so no legitimately written artifact lacks them. Accepting one that
// does would let a partial restore that omitted or damaged snapshot.meta,
// paired with a same-length alteration of snapshot.dat, recover as divergent
// state.
func VerifySnapshotArtifact(dir, dataPath string) error {
	meta, err := readSnapshotMetaFile(dir)
	return checkSnapshotArtifact(dir, meta, err, func() (uint64, uint64, error) {
		return hashSnapshotData(dataPath)
	})
}

// checkSnapshotArtifact is the policy VerifySnapshotArtifact applies to the
// outcome of reading an artifact's snapshot.meta (meta, metaErr), with the
// data hash supplied by the caller. It is split out so the streaming verifier
// (SnapshotDataVerifier) is governed by the SAME policy rather than a copy of
// it: whatever this function decides about a missing or pre-integrity record
// is what the send and export paths decide too — and it fails closed, as
// VerifySnapshotArtifact documents.
func checkSnapshotArtifact(dir string, meta SnapshotMeta, metaErr error, hash func() (sum uint64, size uint64, err error)) error {
	if metaErr != nil {
		return metaErr
	}
	return checkSnapshotData(dir, meta, hash)
}

// SnapshotDataVerifier checks a snapshot data file against the integrity
// fields recorded in its snapshot.meta WHILE another reader consumes the file.
//
// Recovery verifies an artifact before it reads it, at the cost of a second
// pass over the file. The egress paths — the transport send and
// Host.ExportSnapshot — cannot afford that on a multi-gigabyte artifact, and
// used to not check at all: a same-size bit flip in a retained plaintext
// snapshot was streamed to a lagging follower, which hashed the corrupt bytes
// on arrival, stamped THAT hash as authoritative and discarded the log the
// snapshot covers. Egress instead tees every byte it reads into a verifier and
// calls Verify before the LAST byte leaves the node, so a mismatch fails the
// transfer while it is still incomplete — and an incomplete transfer is one
// the receiver discards.
//
// The record is read when the verifier is created, not when Verify runs: the
// snapshot GC may remove the directory while a long send streams from an
// already-open handle, and the record that describes that handle's bytes is
// the one that was there when it was opened. What a missing or pre-integrity
// record means is checkSnapshotArtifact's decision, the one recovery applies.
type SnapshotDataVerifier struct {
	dir     string
	meta    SnapshotMeta
	metaErr error
	digest  *xxhash.Digest
	n       uint64
}

// NewSnapshotDataVerifier reads the snapshot.meta record in dir and returns a
// verifier for the data file beside it.
func NewSnapshotDataVerifier(dir string) *SnapshotDataVerifier {
	meta, err := readSnapshotMetaFile(dir)
	return &SnapshotDataVerifier{dir: dir, meta: meta, metaErr: err, digest: xxhash.New()}
}

// Write adds p to the running hash. It never fails, so it is safe as the sink
// of an io.TeeReader.
func (v *SnapshotDataVerifier) Write(p []byte) (int, error) {
	n, _ := v.digest.Write(p) // xxhash.Digest.Write never returns an error
	v.n += uint64(n)
	return n, nil
}

// Verify compares every byte written so far against the recorded length and
// hash. It returns ErrSnapshotCorrupt on a mismatch, and nil when the policy
// finds nothing recorded to check against.
func (v *SnapshotDataVerifier) Verify() error {
	return checkSnapshotArtifact(v.dir, v.meta, v.metaErr, func() (uint64, uint64, error) {
		return v.digest.Sum64(), v.n, nil
	})
}

// VerifySnapshotFileSize checks the snapshot data file at dataPath against the
// FileSize the LogDB record claims for it.
//
// FileSize was written by every path that records a snapshot and read by NONE
// of the paths that load one: the record travelled through convert.go into the
// raft layer and out onto the wire, and nothing ever compared it to the file it
// describes. A snapshot.dat truncated after it was recorded — a volume that
// filled between the write and a later flush, a partial restore from a backup,
// a torn copy — was therefore loaded as if complete. What comes out of that is
// not an error but a SHORTER state machine: recovery reads the frames it finds
// and stops, so the replica comes up at the snapshot's index holding a prefix
// of its state, silently diverged from every peer at the same log position.
//
// This is a cheap check that the artifact is the one that was recorded, and it
// is complementary to VerifySnapshotArtifact: the meta-based hash lives beside
// the artifact, whereas FileSize lives in LogDB, so the size check still holds
// when the file and its snapshot.meta are replaced together.
//
// A recorded size of zero claims nothing and is not checked: a record written
// before FileSize was populated carries zero, and refusing those would make an
// upgrade unable to recover the snapshots it inherited.
func VerifySnapshotFileSize(dataPath string, recorded uint64) error {
	if recorded == 0 {
		return nil
	}
	info, err := os.Stat(dataPath)
	if err != nil {
		return err
	}
	if uint64(info.Size()) != recorded {
		return &SnapshotSizeMismatchError{
			Path:     dataPath,
			Recorded: recorded,
			OnDisk:   uint64(info.Size()),
		}
	}
	return nil
}

// verifyRecoveryArtifact runs BOTH at-rest checks a recovery makes on the
// artifact it is about to read: the snapshot.meta integrity fields, and the
// FileSize the LogDB record claims.
//
// The two are complementary and neither subsumes the other. The meta hash
// catches a flipped byte, which no size can; FileSize lives in LogDB, away from
// the directory, so it still holds when the file and its record are swapped
// together. A missing or unusable snapshot.meta fails the recovery (see
// VerifySnapshotArtifact) rather than skipping the hash.
//
// The size is checked only when the record NAMES a directory. The recovery's
// discovery fallback picks the newest directory on disk, which may be a
// different index, and rec.FileSize describes the file in the RECORDED
// directory — not that one. The meta check applies to that fallback all the
// same: a published "snapshot-<index>" directory is only ever produced by a
// rename of a stage whose snapshot.meta is already written and fsynced.
func verifyRecoveryArtifact(snapshotDir, dataPath string, rec logdb.Snapshot) error {
	if err := VerifySnapshotArtifact(snapshotDir, dataPath); err != nil {
		return err
	}
	if rec.Filepath == "" {
		return nil
	}
	return VerifySnapshotFileSize(dataPath, rec.FileSize)
}

// stampSnapshotHash is the hash writeSnapshotMetadata and the decrypted
// export stamp the integrity fields with. The verifiers call hashSnapshotData
// directly, so a test can fail the stamp without also failing the check that
// later reads it.
//
// It is a variable ONLY so the fail-before-publish branch can be exercised: a
// read of a file this process just wrote and fsynced cannot be made to fail
// honestly — chmod does not bind a root test runner, and removing the file
// fails the save earlier. Production code must never assign to it.
var stampSnapshotHash = hashSnapshotData
