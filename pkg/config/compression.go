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

// Package config provides configuration types for the QuicRaft Raft library.
// It includes per-shard configuration (Config), per-host configuration
// (HostConfig), and transport-layer tuning (TransportConfig). All
// configuration types support SetDefaults for filling zero-valued fields
// and Validate for cross-field validation.
package config

// CompressionType identifies the compression algorithm for Raft entries
// and snapshot data.
type CompressionType uint16

const (
	// NoCompression disables compression.
	NoCompression CompressionType = 0

	// Snappy enables Snappy compression for reduced bandwidth at minimal CPU cost.
	Snappy CompressionType = 1
)

// String returns a human-readable representation of the compression type.
func (c CompressionType) String() string {
	name, ok := compressionNames[c]
	if !ok {
		return "unknown"
	}
	return name
}

// compressionNames maps compression types to display names.
var compressionNames = map[CompressionType]string{
	NoCompression: "none",
	Snappy:        "snappy",
}

// validCompressionTypes is the set of recognized compression types.
var validCompressionTypes = map[CompressionType]struct{}{
	NoCompression: {},
	Snappy:        {},
}

// IsValid reports whether the compression type is a recognized value.
func (c CompressionType) IsValid() bool {
	_, ok := validCompressionTypes[c]
	return ok
}
