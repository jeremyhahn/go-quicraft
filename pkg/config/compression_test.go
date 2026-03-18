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

package config

import "testing"

func TestCompressionType_String(t *testing.T) {
	tests := []struct {
		name string
		ct   CompressionType
		want string
	}{
		{name: "NoCompression", ct: NoCompression, want: "none"},
		{name: "Snappy", ct: Snappy, want: "snappy"},
		{name: "unknown value", ct: CompressionType(99), want: "unknown"},
		{name: "max uint16", ct: CompressionType(65535), want: "unknown"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.ct.String()
			if got != tt.want {
				t.Errorf("CompressionType(%d).String() = %q, want %q", tt.ct, got, tt.want)
			}
		})
	}
}

func TestCompressionType_IsValid(t *testing.T) {
	tests := []struct {
		name string
		ct   CompressionType
		want bool
	}{
		{name: "NoCompression", ct: NoCompression, want: true},
		{name: "Snappy", ct: Snappy, want: true},
		{name: "unknown 2", ct: CompressionType(2), want: false},
		{name: "unknown 99", ct: CompressionType(99), want: false},
		{name: "max uint16", ct: CompressionType(65535), want: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.ct.IsValid()
			if got != tt.want {
				t.Errorf("CompressionType(%d).IsValid() = %v, want %v", tt.ct, got, tt.want)
			}
		})
	}
}
