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

package proto

// SnapshotFile describes an external file in a snapshot, encoded as [Filepath:len+data][FileSize:8][FileID:8][Metadata:len+data].
type SnapshotFile struct {
	// Filepath is the path to the external file.
	Filepath string
	// FileSize is the size of the file in bytes.
	FileSize uint64
	// FileID is the unique identifier for this file within the snapshot.
	FileID uint64
	// Metadata is application-defined and aliases the source buffer after UnmarshalFrom; copy it to keep it.
	Metadata []byte
}

// Size returns the total marshaled size of the snapshot file in bytes.
func (sf *SnapshotFile) Size() int {
	return (4 + len(sf.Filepath)) + 8 + 8 + (4 + len(sf.Metadata))
}

// MarshalTo writes the file into buf and returns bytes written, or ErrBufferTooSmall, ErrFilepathTooLong or ErrMetadataTooLong.
func (sf *SnapshotFile) MarshalTo(buf []byte) (int, error) {
	if len(sf.Filepath) > MaxFilepathLen {
		return 0, ErrFilepathTooLong
	}
	if uint64(len(sf.Metadata)) > uint64(MaxMetadataLen) {
		return 0, ErrMetadataTooLong
	}
	required := sf.Size()
	if len(buf) < required {
		return 0, ErrBufferTooSmall
	}
	offset := putString(buf, sf.Filepath)
	putUint64(buf[offset:], sf.FileSize)
	offset += 8
	putUint64(buf[offset:], sf.FileID)
	offset += 8
	offset += putBytes(buf[offset:], sf.Metadata)
	return offset, nil
}

// UnmarshalFrom reads the snapshot file from buf and returns bytes consumed.
func (sf *SnapshotFile) UnmarshalFrom(buf []byte) (int, error) {
	fp, n, err := getString(buf, MaxFilepathLen, ErrFilepathTooLong)
	if err != nil {
		return 0, err
	}
	sf.Filepath = fp
	offset := n
	if len(buf) < offset+16 {
		return 0, ErrBufferTooSmall
	}
	sf.FileSize = getUint64(buf[offset:])
	offset += 8
	sf.FileID = getUint64(buf[offset:])
	offset += 8
	md, n, err := getBytes(buf[offset:], MaxMetadataLen, ErrMetadataTooLong)
	if err != nil {
		return 0, err
	}
	sf.Metadata = md
	offset += n
	return offset, nil
}

// Reset clears all fields for reuse.
func (sf *SnapshotFile) Reset() {
	sf.Filepath = ""
	sf.FileSize = 0
	sf.FileID = 0
	sf.Metadata = nil
}
