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
	"errors"
	"testing"
)

// TestDecodeCommandRejectsMalformed covers the shapes the DecodeCommand fuzz
// target found accepted: a Get or Delete carrying bytes after its key (no
// encoder ever wrote them, and they used to come back as Value) and a Put or
// PutIfAbsent whose value exceeds MaxValueLength.
func TestDecodeCommandRejectsMalformed(t *testing.T) {
	get, err := EncodeGet([]byte("k"))
	if err != nil {
		t.Fatal(err)
	}
	del, err := EncodeDelete([]byte("k"))
	if err != nil {
		t.Fatal(err)
	}
	cases := []struct {
		name string
		data []byte
	}{
		{"get with trailing byte", append(append([]byte{}, get...), 'x')},
		{"get with trailing bytes", append(append([]byte{}, get...), 0x00, 0xFF, 0x01)},
		{"empty-key get with trailing byte", []byte{OpGet, 0x00, 0x00, 'x'}},
		{"delete with trailing byte", append(append([]byte{}, del...), 'x')},
		{"empty-key delete with trailing bytes", []byte{OpDelete, 0x00, 0x00, 'v', 'v'}},
	}
	if !testing.Short() {
		// The value length is implicit (the rest of the entry), so the bound
		// can only be reached by an entry of that size.
		big := make([]byte, MaxValueLength+1)
		put, err := EncodePut([]byte("k"), big)
		if err != nil {
			t.Fatal(err)
		}
		pia, err := EncodePutIfAbsent([]byte("k"), big)
		if err != nil {
			t.Fatal(err)
		}
		cases = append(cases,
			struct {
				name string
				data []byte
			}{"put value above MaxValueLength", put},
			struct {
				name string
				data []byte
			}{"put-if-absent value above MaxValueLength", pia},
		)
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cmd, err := DecodeCommand(tc.data)
			if err == nil {
				t.Fatalf("DecodeCommand accepted op %d with key %q and %d value bytes", cmd.Op, cmd.Key, len(cmd.Value))
			}
			var ice *InvalidCommandError
			if !errors.As(err, &ice) {
				t.Fatalf("error %T (%v) is not an *InvalidCommandError", err, err)
			}
		})
	}
}

// TestDecodeCommandAcceptsEncoderOutput pins that every encoder's output,
// including a value of exactly MaxValueLength, still decodes.
func TestDecodeCommandAcceptsEncoderOutput(t *testing.T) {
	value := []byte("value")
	if !testing.Short() {
		value = make([]byte, MaxValueLength)
	}
	for _, enc := range []func() ([]byte, error){
		func() ([]byte, error) { return EncodePut([]byte("k"), value) },
		func() ([]byte, error) { return EncodePutIfAbsent([]byte("k"), value) },
		func() ([]byte, error) { return EncodeGet([]byte("k")) },
		func() ([]byte, error) { return EncodeDelete([]byte("k")) },
		func() ([]byte, error) { return EncodeGet(nil) },
	} {
		b, err := enc()
		if err != nil {
			t.Fatal(err)
		}
		if _, err := DecodeCommand(b); err != nil {
			t.Fatalf("DecodeCommand(op %d) = %v", b[0], err)
		}
	}
}
