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

package kv

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"testing"
)

// TestRecoverFromSnapshot_RejectsOversizedLengths is the regression test for
// H4: a corrupt/malicious snapshot length prefix must be rejected before the
// oversized allocation, for both KV backends.
func TestRecoverFromSnapshot_RejectsOversizedLengths(t *testing.T) {
	// count=1, keyLen=huge.
	var buf bytes.Buffer
	_ = binary.Write(&buf, binary.LittleEndian, uint32(1))
	_ = binary.Write(&buf, binary.LittleEndian, uint32(MaxKeyLength+1))
	oversizedKey := buf.Bytes()

	// count=1, keyLen=3, key, valLen=huge.
	var buf2 bytes.Buffer
	_ = binary.Write(&buf2, binary.LittleEndian, uint32(1))
	_ = binary.Write(&buf2, binary.LittleEndian, uint32(3))
	buf2.WriteString("abc")
	_ = binary.Write(&buf2, binary.LittleEndian, uint32(MaxValueLength+1))
	oversizedVal := buf2.Bytes()

	cases := map[string][]byte{
		"oversized-key":   oversizedKey,
		"oversized-value": oversizedVal,
	}
	for name, data := range cases {
		t.Run("memory/"+name, func(t *testing.T) {
			m := NewMemoryStore()
			err := m.RecoverFromSnapshot(context.Background(), bytes.NewReader(data), nil)
			if !errors.Is(err, errCorruptSnapshotLength) {
				t.Fatalf("got %v, want errCorruptSnapshotLength", err)
			}
		})
		t.Run("concurrent/"+name, func(t *testing.T) {
			c := NewConcurrentStore()
			err := c.RecoverFromSnapshot(context.Background(), bytes.NewReader(data), nil)
			if !errors.Is(err, errCorruptSnapshotLength) {
				t.Fatalf("got %v, want errCorruptSnapshotLength", err)
			}
		})
	}
}
