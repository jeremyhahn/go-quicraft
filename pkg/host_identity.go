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
	"hash/crc32"
	"io/fs"
	"log/slog"
	"math"
	"os"
	"path/filepath"

	"github.com/jeremyhahn/go-quicraft/pkg/fsutil"
)

// NodeHostDir ownership: one live Host per directory, and one deployment per
// directory for its whole life.
//
// The lock (hostLockFileName) is held from NewHost to Close; see
// fsutil.LockDir. The identity file records the DeploymentID, and the
// RaftAddress, the directory was first started with. A later start under a
// different DeploymentID is refused with ErrHostIdentityMismatch before the
// log is opened: the directory's Raft state belongs to the other deployment.
//
// The RaftAddress is recorded but not enforced. A member may restart at a new
// address (membership binds replicas by certificate identity, not address),
// and a RaftAddress with port 0 takes a different kernel-assigned port on
// every start, so a changed address is logged and the record kept.
//
// The file is published as the WAL's small metadata files are: a fsynced
// temporary file renamed over the name, then the directory fsynced, so the
// name only ever refers to a complete record.
//
// Layout (little-endian):
//
//	magic "QRHOSTID"[8] | deploymentID:u64 | addrLen:u16 | addr[addrLen] | crc32c:u32
//
// The CRC covers every byte before it.
const (
	hostLockFileName         = "host.lock"
	hostIdentityFileName     = "host.identity"
	hostIdentityTempFileName = "host.identity.tmp"
	hostIdentityMagic        = "QRHOSTID"
	hostIdentityHeaderSize   = 8 + 8 + 2
	hostIdentityTrailerSize  = 4
)

var hostIdentityCRCTable = crc32.MakeTable(crc32.Castagnoli)

// errHostIdentityMalformed is wrapped in the HostInitError NewHost returns
// when the identity file exists but does not parse. It fails the start: a
// record that cannot be read is a deployment that cannot be checked.
var errHostIdentityMalformed = errors.New("quicraft: host identity file is malformed")

// hostIdentity is the record the identity file holds.
type hostIdentity struct {
	deploymentID uint64
	raftAddress  string
}

// marshalHostIdentity encodes id as an identity file.
func marshalHostIdentity(id hostIdentity) ([]byte, error) {
	if len(id.raftAddress) > math.MaxUint16 {
		return nil, fmt.Errorf("quicraft: raft address of %d bytes does not fit the host identity file", len(id.raftAddress))
	}
	buf := make([]byte, hostIdentityHeaderSize+len(id.raftAddress)+hostIdentityTrailerSize)
	copy(buf[:8], hostIdentityMagic)
	binary.LittleEndian.PutUint64(buf[8:16], id.deploymentID)
	binary.LittleEndian.PutUint16(buf[16:18], uint16(len(id.raftAddress)))
	off := hostIdentityHeaderSize + copy(buf[hostIdentityHeaderSize:], id.raftAddress)
	binary.LittleEndian.PutUint32(buf[off:], crc32.Checksum(buf[:off], hostIdentityCRCTable))
	return buf, nil
}

// parseHostIdentity decodes an identity file's bytes.
func parseHostIdentity(buf []byte) (hostIdentity, error) {
	if len(buf) < hostIdentityHeaderSize+hostIdentityTrailerSize || string(buf[:8]) != hostIdentityMagic {
		return hostIdentity{}, errHostIdentityMalformed
	}
	addrLen := int(binary.LittleEndian.Uint16(buf[16:18]))
	if len(buf) != hostIdentityHeaderSize+addrLen+hostIdentityTrailerSize {
		return hostIdentity{}, errHostIdentityMalformed
	}
	end := len(buf) - hostIdentityTrailerSize
	if crc32.Checksum(buf[:end], hostIdentityCRCTable) != binary.LittleEndian.Uint32(buf[end:]) {
		return hostIdentity{}, errHostIdentityMalformed
	}
	return hostIdentity{
		deploymentID: binary.LittleEndian.Uint64(buf[8:16]),
		raftAddress:  string(buf[hostIdentityHeaderSize:end]),
	}, nil
}

// checkHostIdentity records want in dir on the directory's first start, and
// on every later start refuses a recorded DeploymentID other than want's
// with an error wrapping ErrHostIdentityMismatch that names both. A changed
// RaftAddress is logged, not refused (see the file comment).
func checkHostIdentity(logger *slog.Logger, dir string, want hostIdentity) error {
	path := filepath.Join(dir, hostIdentityFileName)
	data, err := os.ReadFile(path)
	if errors.Is(err, fs.ErrNotExist) {
		if err := writeHostIdentity(dir, want); err != nil {
			return &HostInitError{Field: "host_identity", Err: err}
		}
		return nil
	}
	if err != nil {
		return &HostInitError{Field: "host_identity", Err: err}
	}
	got, err := parseHostIdentity(data)
	if err != nil {
		return &HostInitError{Field: "host_identity", Err: fmt.Errorf("%w: %s", err, path)}
	}
	if got.deploymentID != want.deploymentID {
		return fmt.Errorf("%w: %s records deployment ID %d, configured deployment ID is %d",
			ErrHostIdentityMismatch, path, got.deploymentID, want.deploymentID)
	}
	if got.raftAddress != want.raftAddress {
		logger.Warn("raft address differs from the one this NodeHostDir was first started with",
			"recorded", got.raftAddress, "configured", want.raftAddress, "path", path)
	}
	return nil
}

// writeHostIdentity publishes id as dir's identity file: temporary file,
// fsync, rename, directory fsync.
func writeHostIdentity(dir string, id hostIdentity) (err error) {
	buf, err := marshalHostIdentity(id)
	if err != nil {
		return err
	}
	tmp := filepath.Join(dir, hostIdentityTempFileName)
	defer func() {
		if err != nil {
			if rmErr := os.Remove(tmp); rmErr != nil && !errors.Is(rmErr, fs.ErrNotExist) {
				err = errors.Join(err, rmErr)
			}
		}
	}()
	f, err := os.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, hostIDFilePerms)
	if err != nil {
		return err
	}
	if _, err := f.Write(buf); err != nil {
		return errors.Join(err, f.Close())
	}
	if err := f.Sync(); err != nil {
		return errors.Join(err, f.Close())
	}
	if err := f.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmp, filepath.Join(dir, hostIdentityFileName)); err != nil {
		return err
	}
	return fsutil.SyncDir(dir)
}
