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

// Options mirrors the functional Options Open accepts; each field maps to a With* option, and DefaultOptions supplies defaultDBOptions' defaults.
type Options struct {
	// WALDir is the directory where WAL segment files are stored.
	WALDir string

	// BlockSize is the WAL block size in bytes; larger records are split into block-sized chunks. Default: 32KB.
	BlockSize int

	// MaxFileSize is the maximum segment file size in bytes before rotation.
	// Default: 64MB.
	MaxFileSize int64

	// NoSync disables fsync after writes. Only use for testing.
	NoSync bool
}

// DefaultOptions returns Options populated with production defaults.
var DefaultOptions = Options{
	BlockSize:   32 * 1024,
	MaxFileSize: 64 * 1024 * 1024,
}

// SetDefaults fills zero-valued fields with production defaults.
func (o *Options) SetDefaults() {
	if o.BlockSize == 0 {
		o.BlockSize = DefaultOptions.BlockSize
	}
	if o.MaxFileSize == 0 {
		o.MaxFileSize = DefaultOptions.MaxFileSize
	}
}

// Validate checks that all options are within valid ranges; validateOpenOptions applies the same checks to Open's options.
func (o *Options) Validate() error {
	if o.WALDir == "" {
		return ErrEmptyWALDir
	}
	if o.BlockSize < recordHeaderSize+1 || !isPowerOfTwo(o.BlockSize) {
		return ErrInvalidBlockSize
	}
	if o.MaxFileSize <= 0 || o.MaxFileSize > maxSegmentFileSize {
		return ErrInvalidMaxFileSize
	}
	return nil
}

// isPowerOfTwo returns true if n is a positive power of two.
func isPowerOfTwo(n int) bool {
	return n > 0 && (n&(n-1)) == 0
}
