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

// hashSnapshotData returns the xxhash64 and length of the final snapshot data bytes; it detects corruption, not a hostile writer.
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

// HashSnapshotReader returns the xxhash64 and byte count of r, the DataHash/DataLen a snapshot.meta stamps; a read error must fail the write.
func HashSnapshotReader(r io.Reader) (sum uint64, size uint64, err error) {
	h := xxhash.New()
	n, err := io.Copy(h, r)
	if err != nil {
		return 0, 0, err
	}
	return h.Sum64(), uint64(n), nil
}

// checkSnapshotData compares a caller-supplied hash and length with snapshot.meta, failing closed without integrity fields; mismatch is ErrSnapshotCorrupt.
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

// readSnapshotMetaFile reads snapshot.meta in dir with a bounded read; any failure is *SnapshotMetaInvalidError.
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

// VerifySnapshotArtifact checks the data file at dataPath against snapshot.meta in dir, failing closed with *SnapshotMetaInvalidError when the record is unusable.
func VerifySnapshotArtifact(dir, dataPath string) error {
	meta, err := readSnapshotMetaFile(dir)
	return checkSnapshotArtifact(dir, meta, err, func() (uint64, uint64, error) {
		return hashSnapshotData(dataPath)
	})
}

// checkSnapshotArtifact is the shared fail-closed policy of VerifySnapshotArtifact and SnapshotDataVerifier.
func checkSnapshotArtifact(dir string, meta SnapshotMeta, metaErr error, hash func() (sum uint64, size uint64, err error)) error {
	if metaErr != nil {
		return metaErr
	}
	return checkSnapshotData(dir, meta, hash)
}

// SnapshotDataVerifier checks a data file against snapshot.meta while another reader streams it; call Verify before the last byte leaves.
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

// NewSnapshotDataVerifierForMeta returns a verifier against an already-read meta; a non-nil metaErr is reported as *SnapshotMetaInvalidError.
func NewSnapshotDataVerifierForMeta(dir string, meta SnapshotMeta, metaErr error) *SnapshotDataVerifier {
	if metaErr != nil {
		meta = SnapshotMeta{}
		metaErr = &SnapshotMetaInvalidError{Dir: dir, Err: metaErr}
	}
	return &SnapshotDataVerifier{dir: dir, meta: meta, metaErr: metaErr, digest: xxhash.New()}
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

// VerifySnapshotFileSize checks the data file against the LogDB record's FileSize; zero is not checked.
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

// verifyRecoveryArtifact runs the snapshot.meta check and, when the record names the directory, the FileSize check.
func verifyRecoveryArtifact(snapshotDir, dataPath string, rec logdb.Snapshot) error {
	if err := VerifySnapshotArtifact(snapshotDir, dataPath); err != nil {
		return err
	}
	if rec.Filepath == "" {
		return nil
	}
	return VerifySnapshotFileSize(dataPath, rec.FileSize)
}

// stampSnapshotHash is the hash used to stamp integrity fields; a variable only for tests, never assigned in production.
var stampSnapshotHash = hashSnapshotData
