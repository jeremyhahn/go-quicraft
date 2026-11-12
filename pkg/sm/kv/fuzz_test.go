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
	"testing"
)

// FuzzDecodeCommand feeds arbitrary bytes to DecodeCommand. It asserts
// only that the decoder never panics and that the canonical encoding of
// any command it accepts round-trips: decode(encode(c)) == c. It does not
// assert an acceptance policy: whatever the decoder accepts today (for
// example bytes after a Get or Delete key, which replay of an existing log
// relies on) stays accepted.
func FuzzDecodeCommand(f *testing.F) {
	type pair struct{ key, value []byte }
	seeds := []pair{
		{[]byte("key"), []byte("value")},
		{[]byte("k"), nil},
		{nil, []byte("v")},
		{nil, nil},
		{[]byte("bin\x00\xff"), []byte("\x00\x01\x02")},
	}
	for _, s := range seeds {
		for _, enc := range []func() ([]byte, error){
			func() ([]byte, error) { return EncodePut(s.key, s.value) },
			func() ([]byte, error) { return EncodePutIfAbsent(s.key, s.value) },
			func() ([]byte, error) { return EncodeGet(s.key) },
			func() ([]byte, error) { return EncodeDelete(s.key) },
		} {
			b, err := enc()
			if err != nil {
				f.Fatalf("encode seed: %v", err)
			}
			f.Add(b)
		}
	}
	f.Add([]byte{})                          // empty
	f.Add([]byte{OpPut, 0x01})               // shorter than the header
	f.Add([]byte{0xFF, 0x00, 0x00})          // unknown op
	f.Add([]byte{OpPut - 1, 0x00, 0x00})     // op below the range
	f.Add([]byte{OpPutIfAbsent + 1, 0, 0})   // op above the range
	f.Add([]byte{OpGet, 0x05, 0x00, 'a'})    // key length exceeds data
	f.Add([]byte{OpGet, 0xFF, 0xFF, 'a'})    // maximum key length, short data
	f.Add([]byte{OpDelete, 0x01, 0x00, 'k'}) // exact delete

	f.Fuzz(func(t *testing.T, data []byte) {
		cmd, err := DecodeCommand(data)
		if err != nil {
			return
		}
		// The canonical command: Get and Delete carry no value.
		c := Command{Op: cmd.Op, Key: cmd.Key}
		var enc []byte
		switch cmd.Op {
		case OpPut:
			c.Value = cmd.Value
			enc, err = EncodePut(c.Key, c.Value)
		case OpPutIfAbsent:
			c.Value = cmd.Value
			enc, err = EncodePutIfAbsent(c.Key, c.Value)
		case OpGet:
			enc, err = EncodeGet(c.Key)
		case OpDelete:
			enc, err = EncodeDelete(c.Key)
		default:
			return
		}
		if err != nil {
			// The encoder refuses a shape the decoder accepts (an
			// oversized value): there is no canonical encoding to check.
			return
		}
		got, err := DecodeCommand(enc)
		if err != nil {
			t.Fatalf("decode of canonical op %d encoding %x: %v", c.Op, enc, err)
		}
		if got.Op != c.Op || !bytes.Equal(got.Key, c.Key) || !bytes.Equal(got.Value, c.Value) {
			t.Fatalf("round trip mismatch: encoded %+v as %x, decoded %+v", c, enc, got)
		}
	})
}
