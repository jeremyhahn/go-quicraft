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

	h := xxhash.New()
	n, err := io.Copy(h, f)
	if err != nil {
		return 0, 0, err
	}
	return h.Sum64(), uint64(n), nil
}

// verifySnapshotData checks the data file at path against the integrity fields
// recorded in its snapshot.meta.
//
// A record written before those fields existed leaves DataHashRecorded false
// and is accepted without a check, so an upgrade does not refuse artifacts
// that predate it. Everything else fails CLOSED with ErrSnapshotCorrupt: a
// corrupt snapshot must never be recovered into the state machine, and falling
// back to an older directory is not an option either — that would silently
// roll the replica backwards.
func verifySnapshotData(path string, meta SnapshotMeta) error {
	if !meta.DataHashRecorded {
		return nil
	}
	sum, size, err := hashSnapshotData(path)
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
// substituted multi-gigabyte file is refused rather than slurped. A missing or
// unparseable record returns an error; callers that verify integrity treat
// that as "no check recorded" rather than as corruption, because an artifact
// written before the integrity fields existed has a valid record without them
// and a half-written directory is already handled by the recorded-snapshot
// lookup.
func readSnapshotMetaFile(dir string) (SnapshotMeta, error) {
	data, err := readBoundedSnapshotMeta(filepath.Join(dir, snapshotMetadataFile))
	if err != nil {
		return SnapshotMeta{}, err
	}
	return ParseSnapshotMeta(data)
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
// A directory with no readable snapshot.meta, or a record written before the
// integrity fields existed, returns nil: there is nothing recorded to check
// against, and refusing such artifacts would make an upgrade unable to recover
// snapshots taken by the binary it replaced.
func VerifySnapshotArtifact(dir, dataPath string) error {
	meta, err := readSnapshotMetaFile(dir)
	if err != nil {
		// The ABSENCE of a record is not a verification failure. An artifact
		// written before the integrity fields existed carries a valid record
		// without them, and an unreadable one is already handled by the
		// recorded-snapshot lookup that selected this directory. Returning the
		// read error here would make an upgrade unable to recover the
		// snapshots it inherited, which is a worse failure than not checking.
		//nolint:nilerr // absence of a record is deliberately not a failure; see above
		return nil
	}
	return verifySnapshotData(dataPath, meta)
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
// is complementary to VerifySnapshotArtifact: the meta-based hash only exists
// for artifacts written by a binary that stamps it, whereas FileSize is on
// every record ever written and is the only at-rest check an inherited
// artifact has.
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
// The two are complementary and neither subsumes the other. The meta hash only
// exists for artifacts a stamping binary wrote, so an inherited one has no hash
// to check at all; FileSize is on every record ever written. Conversely the
// hash catches a flipped byte, which no size can.
//
// The size is checked only when the record NAMES a directory. The recovery's
// discovery fallback picks the newest directory on disk, which may be a
// different index, and rec.FileSize describes the file in the RECORDED
// directory — not that one.
func verifyRecoveryArtifact(snapshotDir, dataPath string, rec logdb.Snapshot) error {
	if meta, metaErr := readSnapshotMetaFile(snapshotDir); metaErr == nil {
		if err := verifySnapshotData(dataPath, meta); err != nil {
			return err
		}
	}
	if rec.Filepath == "" {
		return nil
	}
	return VerifySnapshotFileSize(dataPath, rec.FileSize)
}

// HashSnapshotFile is hashSnapshotData exported for the receive and import
// paths, which write snapshot artifacts from package quicraft and must stamp
// the same value this package verifies.
func HashSnapshotFile(path string) (sum uint64, size uint64, err error) {
	return hashSnapshotData(path)
}
