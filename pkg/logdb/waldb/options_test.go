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
	if o.MaxBatchSize != 64*1024*1024 {
		t.Fatalf("MaxBatchSize = %d, want %d", o.MaxBatchSize, 64*1024*1024)
	}
}

// TestSetDefaultsPreservesExisting verifies that SetDefaults does not overwrite
// fields that already have non-zero values.
func TestSetDefaultsPreservesExisting(t *testing.T) {
	o := &Options{
		WALDir:       "/tmp/wal",
		BlockSize:    4096,
		MaxFileSize:  128 * 1024 * 1024,
		MaxBatchSize: 32 * 1024 * 1024,
	}
	o.SetDefaults()

	if o.BlockSize != 4096 {
		t.Fatalf("BlockSize = %d, want 4096", o.BlockSize)
	}
	if o.MaxFileSize != 128*1024*1024 {
		t.Fatalf("MaxFileSize = %d, want %d", o.MaxFileSize, int64(128*1024*1024))
	}
	if o.MaxBatchSize != 32*1024*1024 {
		t.Fatalf("MaxBatchSize = %d, want %d", o.MaxBatchSize, 32*1024*1024)
	}
}

// TestValidateValid verifies that Validate passes with valid options.
func TestValidateValid(t *testing.T) {
	o := &Options{
		WALDir:       "/tmp/wal",
		BlockSize:    32 * 1024,
		MaxFileSize:  64 * 1024 * 1024,
		MaxBatchSize: 64 * 1024 * 1024,
	}

	if err := o.Validate(); err != nil {
		t.Fatalf("Validate: unexpected error: %v", err)
	}
}

// TestValidateVariousBlockSizes verifies that valid power-of-two block sizes
// pass validation while invalid ones fail.
func TestValidateVariousBlockSizes(t *testing.T) {
	validSizes := []int{8, 16, 32, 64, 128, 256, 512, 1024, 4096, 32768}
	for _, bs := range validSizes {
		o := &Options{
			WALDir:       "/tmp/wal",
			BlockSize:    bs,
			MaxFileSize:  64 * 1024 * 1024,
			MaxBatchSize: 64 * 1024 * 1024,
		}
		if err := o.Validate(); err != nil {
			t.Errorf("Validate(BlockSize=%d): unexpected error: %v", bs, err)
		}
	}
}

// TestValidateEmptyDir verifies that Validate fails when WALDir is empty.
func TestValidateEmptyDir(t *testing.T) {
	o := &Options{
		WALDir:       "",
		BlockSize:    32 * 1024,
		MaxFileSize:  64 * 1024 * 1024,
		MaxBatchSize: 64 * 1024 * 1024,
	}

	err := o.Validate()
	if err != ErrEmptyWALDir {
		t.Fatalf("Validate: error = %v, want %v", err, ErrEmptyWALDir)
	}
}

// TestValidateInvalidBlockSize verifies that Validate fails when BlockSize is
// not a power of two.
func TestValidateInvalidBlockSize(t *testing.T) {
	o := &Options{
		WALDir:       "/tmp/wal",
		BlockSize:    100, // not power of two
		MaxFileSize:  64 * 1024 * 1024,
		MaxBatchSize: 64 * 1024 * 1024,
	}

	err := o.Validate()
	if err != ErrInvalidBlockSize {
		t.Fatalf("Validate: error = %v, want %v", err, ErrInvalidBlockSize)
	}
}

// TestValidateZeroBlockSize verifies that Validate fails when BlockSize is zero.
func TestValidateZeroBlockSize(t *testing.T) {
	o := &Options{
		WALDir:       "/tmp/wal",
		BlockSize:    0,
		MaxFileSize:  64 * 1024 * 1024,
		MaxBatchSize: 64 * 1024 * 1024,
	}

	err := o.Validate()
	if err != ErrInvalidBlockSize {
		t.Fatalf("Validate: error = %v, want %v", err, ErrInvalidBlockSize)
	}
}

// TestValidateNegativeBlockSize verifies that Validate fails when BlockSize is negative.
func TestValidateNegativeBlockSize(t *testing.T) {
	o := &Options{
		WALDir:       "/tmp/wal",
		BlockSize:    -1024,
		MaxFileSize:  64 * 1024 * 1024,
		MaxBatchSize: 64 * 1024 * 1024,
	}

	err := o.Validate()
	if err != ErrInvalidBlockSize {
		t.Fatalf("Validate: error = %v, want %v", err, ErrInvalidBlockSize)
	}
}

// TestValidateInvalidMaxFileSize verifies that Validate fails when MaxFileSize
// is zero or negative.
func TestValidateInvalidMaxFileSize(t *testing.T) {
	tests := []int64{0, -1, -1024 * 1024}
	for _, mfs := range tests {
		o := &Options{
			WALDir:       "/tmp/wal",
			BlockSize:    32 * 1024,
			MaxFileSize:  mfs,
			MaxBatchSize: 64 * 1024 * 1024,
		}
		err := o.Validate()
		if err != ErrInvalidMaxFileSize {
			t.Errorf("Validate(MaxFileSize=%d): error = %v, want %v", mfs, err, ErrInvalidMaxFileSize)
		}
	}
}

// TestValidateInvalidMaxBatchSize verifies that Validate fails when MaxBatchSize
// is zero or negative.
func TestValidateInvalidMaxBatchSize(t *testing.T) {
	tests := []int{0, -1, -64 * 1024 * 1024}
	for _, mbs := range tests {
		o := &Options{
			WALDir:       "/tmp/wal",
			BlockSize:    32 * 1024,
			MaxFileSize:  64 * 1024 * 1024,
			MaxBatchSize: mbs,
		}
		err := o.Validate()
		if err != ErrInvalidMaxBatchSize {
			t.Errorf("Validate(MaxBatchSize=%d): error = %v, want %v", mbs, err, ErrInvalidMaxBatchSize)
		}
	}
}

// TestValidateBlockSizeTooSmallForHeader verifies that a block size smaller
// than recordHeaderSize + 1 byte is rejected.
func TestValidateBlockSizeTooSmallForHeader(t *testing.T) {
	// BlockSize of 4 is a power of two but too small for header (7 bytes) + 1.
	o := &Options{
		WALDir:       "/tmp/wal",
		BlockSize:    4,
		MaxFileSize:  64 * 1024 * 1024,
		MaxBatchSize: 64 * 1024 * 1024,
	}

	err := o.Validate()
	if err != ErrInvalidBlockSize {
		t.Fatalf("Validate: error = %v, want %v", err, ErrInvalidBlockSize)
	}
}

// TestDefaultOptionsValues verifies that DefaultOptions has the correct values.
func TestDefaultOptionsValues(t *testing.T) {
	if DefaultOptions.BlockSize != 32*1024 {
		t.Fatalf("DefaultOptions.BlockSize = %d, want %d", DefaultOptions.BlockSize, 32*1024)
	}
	if DefaultOptions.MaxFileSize != 64*1024*1024 {
		t.Fatalf("DefaultOptions.MaxFileSize = %d, want %d", DefaultOptions.MaxFileSize, int64(64*1024*1024))
	}
	if DefaultOptions.MaxBatchSize != 64*1024*1024 {
		t.Fatalf("DefaultOptions.MaxBatchSize = %d, want %d", DefaultOptions.MaxBatchSize, 64*1024*1024)
	}
}

// TestDefaultOptionsNoSync verifies that NoSync defaults to false.
func TestDefaultOptionsNoSync(t *testing.T) {
	if DefaultOptions.NoSync {
		t.Fatal("DefaultOptions.NoSync = true, want false")
	}
}

// TestIsPowerOfTwo verifies the isPowerOfTwo helper function.
func TestIsPowerOfTwo(t *testing.T) {
	tests := []struct {
		n    int
		want bool
	}{
		{0, false},
		{1, true},
		{2, true},
		{3, false},
		{4, true},
		{5, false},
		{8, true},
		{16, true},
		{100, false},
		{128, true},
		{1024, true},
		{32768, true},
		{-1, false},
		{-2, false},
	}

	for _, tc := range tests {
		got := isPowerOfTwo(tc.n)
		if got != tc.want {
			t.Errorf("isPowerOfTwo(%d) = %v, want %v", tc.n, got, tc.want)
		}
	}
}
