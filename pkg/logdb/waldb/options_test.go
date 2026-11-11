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

package waldb

import "testing"

// TestSetDefaultsFillsValues verifies that SetDefaults populates zero-valued
// fields with the correct production defaults.
func TestSetDefaultsFillsValues(t *testing.T) {
	o := &Options{WALDir: "/tmp/wal"}
	o.SetDefaults()

	if o.BlockSize != 32*1024 {
		t.Fatalf("BlockSize = %d, want %d", o.BlockSize, 32*1024)
	}
	if o.MaxFileSize != 64*1024*1024 {
		t.Fatalf("MaxFileSize = %d, want %d", o.MaxFileSize, int64(64*1024*1024))
	}
}

// TestSetDefaultsPreservesExisting verifies that SetDefaults does not overwrite
// fields that already have non-zero values.
func TestSetDefaultsPreservesExisting(t *testing.T) {
	o := &Options{
		WALDir:      "/tmp/wal",
		BlockSize:   4096,
		MaxFileSize: 128 * 1024 * 1024,
	}
	o.SetDefaults()

	if o.BlockSize != 4096 {
		t.Fatalf("BlockSize = %d, want 4096", o.BlockSize)
	}
	if o.MaxFileSize != 128*1024*1024 {
		t.Fatalf("MaxFileSize = %d, want %d", o.MaxFileSize, int64(128*1024*1024))
	}
}

// TestValidateVariousBlockSizes verifies that valid power-of-two block sizes
// pass validation while invalid ones fail.
func TestValidateVariousBlockSizes(t *testing.T) {
	// The smallest legal block is recordHeaderSize+1 = 12 bytes, so the first
	// power of two that can hold a chunk header plus one payload byte is 16.
	validSizes := []int{16, 32, 64, 128, 256, 512, 1024, 4096, 32768}
	for _, bs := range validSizes {
		o := &Options{
			WALDir:      "/tmp/wal",
			BlockSize:   bs,
			MaxFileSize: 64 * 1024 * 1024,
		}
		if err := o.Validate(); err != nil {
			t.Errorf("Validate(BlockSize=%d): unexpected error: %v", bs, err)
		}
	}
}

// TestValidateEmptyDir verifies that Validate fails when WALDir is empty.
func TestValidateEmptyDir(t *testing.T) {
	o := &Options{
		WALDir:      "",
		BlockSize:   32 * 1024,
		MaxFileSize: 64 * 1024 * 1024,
	}

	err := o.Validate()
	if err != ErrEmptyWALDir {
		t.Fatalf("Validate: error = %v, want %v", err, ErrEmptyWALDir)
	}
}

// TestValidateInvalidBlockSize verifies that Validate rejects every class of
// unusable BlockSize.
func TestValidateInvalidBlockSize(t *testing.T) {
	// Not a power of two, zero, negative, and a power of two too small for a
	// chunk header (7 bytes) plus one payload byte.
	for _, bs := range []int{100, 0, -1024, 4} {
		o := &Options{
			WALDir:      "/tmp/wal",
			BlockSize:   bs,
			MaxFileSize: 64 * 1024 * 1024,
		}
		if err := o.Validate(); err != ErrInvalidBlockSize {
			t.Errorf("Validate(BlockSize=%d): error = %v, want %v", bs, err, ErrInvalidBlockSize)
		}
	}
}

// TestValidateInvalidMaxFileSize verifies that Validate fails when MaxFileSize
// is zero or negative.
func TestValidateInvalidMaxFileSize(t *testing.T) {
	// Non-positive, and above what a chunk's 32-bit generation-start offset
	// can name.
	tests := []int64{0, -1, -1024 * 1024, maxSegmentFileSize + 1, 1 << 33}
	for _, mfs := range tests {
		o := &Options{
			WALDir:      "/tmp/wal",
			BlockSize:   32 * 1024,
			MaxFileSize: mfs,
		}
		err := o.Validate()
		if err != ErrInvalidMaxFileSize {
			t.Errorf("Validate(MaxFileSize=%d): error = %v, want %v", mfs, err, ErrInvalidMaxFileSize)
		}
	}
}
