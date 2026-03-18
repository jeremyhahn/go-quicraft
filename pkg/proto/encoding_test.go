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

import (
	"errors"
	"testing"
)

func TestPutGetUint64(t *testing.T) {
	buf := make([]byte, 8)
	want := uint64(0xDEADBEEFCAFEBABE)
	putUint64(buf, want)
	got := getUint64(buf)
	if got != want {
		t.Fatalf("getUint64: got %d, want %d", got, want)
	}
}

func TestPutGetUint64Zero(t *testing.T) {
	buf := make([]byte, 8)
	putUint64(buf, 0)
	got := getUint64(buf)
	if got != 0 {
		t.Fatalf("getUint64: got %d, want 0", got)
	}
}

func TestPutGetUint64Max(t *testing.T) {
	buf := make([]byte, 8)
	want := ^uint64(0) // max uint64
	putUint64(buf, want)
	got := getUint64(buf)
	if got != want {
		t.Fatalf("getUint64: got %d, want %d", got, want)
	}
}

func TestPutGetUint32(t *testing.T) {
	buf := make([]byte, 4)
	want := uint32(0xDEADBEEF)
	putUint32(buf, want)
	got := getUint32(buf)
	if got != want {
		t.Fatalf("getUint32: got %d, want %d", got, want)
	}
}

func TestPutGetUint32Zero(t *testing.T) {
	buf := make([]byte, 4)
	putUint32(buf, 0)
	got := getUint32(buf)
	if got != 0 {
		t.Fatalf("getUint32: got %d, want 0", got)
	}
}

func TestPutGetBoolTrue(t *testing.T) {
	buf := make([]byte, 8)
	putBool(buf, true)
	if !getBool(buf) {
		t.Fatal("getBool: got false, want true")
	}
}

func TestPutGetBoolFalse(t *testing.T) {
	buf := make([]byte, 8)
	putBool(buf, false)
	if getBool(buf) {
		t.Fatal("getBool: got true, want false")
	}
}

func TestPutGetBoolNonOneIsTrue(t *testing.T) {
	buf := make([]byte, 8)
	putUint64(buf, 42) // any non-zero value should be true
	if !getBool(buf) {
		t.Fatal("getBool: non-zero uint64 should be true")
	}
}

func TestPutGetBytes(t *testing.T) {
	data := []byte("hello, world")
	buf := make([]byte, 4+len(data))
	n := putBytes(buf, data)
	if n != 4+len(data) {
		t.Fatalf("putBytes: wrote %d bytes, want %d", n, 4+len(data))
	}
	got, consumed, err := getBytes(buf, 0)
	if err != nil {
		t.Fatalf("getBytes: unexpected error: %v", err)
	}
	if consumed != n {
		t.Fatalf("getBytes: consumed %d bytes, want %d", consumed, n)
	}
	if string(got) != string(data) {
		t.Fatalf("getBytes: got %q, want %q", got, data)
	}
}

func TestPutGetBytesEmpty(t *testing.T) {
	buf := make([]byte, 4)
	n := putBytes(buf, nil)
	if n != 4 {
		t.Fatalf("putBytes(nil): wrote %d bytes, want 4", n)
	}
	got, consumed, err := getBytes(buf, 0)
	if err != nil {
		t.Fatalf("getBytes: unexpected error: %v", err)
	}
	if consumed != 4 {
		t.Fatalf("getBytes: consumed %d, want 4", consumed)
	}
	if got != nil {
		t.Fatalf("getBytes: got %v, want nil", got)
	}
}

func TestGetBytesBufferTooSmall(t *testing.T) {
	// Buffer too short for length prefix
	_, _, err := getBytes([]byte{0, 0}, 0)
	if !errors.Is(err, ErrBufferTooSmall) {
		t.Fatalf("getBytes: got %v, want ErrBufferTooSmall", err)
	}
}

func TestGetBytesDataTruncated(t *testing.T) {
	buf := make([]byte, 8)
	putUint32(buf, 100) // claims 100 bytes but only 4 bytes of data follow
	_, _, err := getBytes(buf, 0)
	if !errors.Is(err, ErrBufferTooSmall) {
		t.Fatalf("getBytes: got %v, want ErrBufferTooSmall", err)
	}
}

func TestGetBytesExceedsMaxLen(t *testing.T) {
	buf := make([]byte, 8)
	putUint32(buf, 100)
	_, _, err := getBytes(buf, 10)
	if !errors.Is(err, ErrEntrySizeLimitExceeded) {
		t.Fatalf("getBytes: got %v, want ErrEntrySizeLimitExceeded", err)
	}
}

func TestPutGetString(t *testing.T) {
	s := "192.168.1.100:5000"
	buf := make([]byte, 4+len(s))
	n := putString(buf, s)
	if n != 4+len(s) {
		t.Fatalf("putString: wrote %d, want %d", n, 4+len(s))
	}
	got, consumed, err := getString(buf, MaxAddressLength)
	if err != nil {
		t.Fatalf("getString: unexpected error: %v", err)
	}
	if consumed != n {
		t.Fatalf("getString: consumed %d, want %d", consumed, n)
	}
	if got != s {
		t.Fatalf("getString: got %q, want %q", got, s)
	}
}

func TestPutGetStringEmpty(t *testing.T) {
	buf := make([]byte, 4)
	n := putString(buf, "")
	if n != 4 {
		t.Fatalf("putString: wrote %d, want 4", n)
	}
	got, consumed, err := getString(buf, 0)
	if err != nil {
		t.Fatalf("getString: unexpected error: %v", err)
	}
	if consumed != 4 {
		t.Fatalf("getString: consumed %d, want 4", consumed)
	}
	if got != "" {
		t.Fatalf("getString: got %q, want empty", got)
	}
}

func TestGetStringBufferTooSmall(t *testing.T) {
	_, _, err := getString([]byte{0, 0}, 0)
	if !errors.Is(err, ErrBufferTooSmall) {
		t.Fatalf("getString: got %v, want ErrBufferTooSmall", err)
	}
}

func TestGetStringTooLong(t *testing.T) {
	buf := make([]byte, 8)
	putUint32(buf, 300) // exceeds MaxAddressLength
	_, _, err := getString(buf, MaxAddressLength)
	if !errors.Is(err, ErrAddressTooLong) {
		t.Fatalf("getString: got %v, want ErrAddressTooLong", err)
	}
}

func TestGetStringDataTruncated(t *testing.T) {
	buf := make([]byte, 8)
	putUint32(buf, 100) // claims 100 bytes but only 4 bytes follow
	_, _, err := getString(buf, 0)
	if !errors.Is(err, ErrBufferTooSmall) {
		t.Fatalf("getString: got %v, want ErrBufferTooSmall", err)
	}
}

func TestPutGetMapRoundTrip(t *testing.T) {
	m := map[uint64]string{
		3: "node-3:5000",
		1: "node-1:5000",
		2: "node-2:5000",
	}
	sz := mapSize(m)
	buf := make([]byte, sz)
	n := putMap(buf, m)
	if n != sz {
		t.Fatalf("putMap: wrote %d, want %d", n, sz)
	}
	got, consumed, err := getMap(buf)
	if err != nil {
		t.Fatalf("getMap: unexpected error: %v", err)
	}
	if consumed != n {
		t.Fatalf("getMap: consumed %d, want %d", consumed, n)
	}
	if len(got) != len(m) {
		t.Fatalf("getMap: got %d entries, want %d", len(got), len(m))
	}
	for k, v := range m {
		if got[k] != v {
			t.Fatalf("getMap[%d]: got %q, want %q", k, got[k], v)
		}
	}
}

func TestPutGetMapEmpty(t *testing.T) {
	m := map[uint64]string{}
	sz := mapSize(m)
	buf := make([]byte, sz)
	n := putMap(buf, m)
	if n != 4 {
		t.Fatalf("putMap(empty): wrote %d, want 4", n)
	}
	got, consumed, err := getMap(buf)
	if err != nil {
		t.Fatalf("getMap: unexpected error: %v", err)
	}
	if consumed != 4 {
		t.Fatalf("getMap: consumed %d, want 4", consumed)
	}
	if len(got) != 0 {
		t.Fatalf("getMap: got %d entries, want 0", len(got))
	}
}

func TestPutGetMapNil(t *testing.T) {
	var m map[uint64]string
	sz := mapSize(m)
	buf := make([]byte, sz)
	putMap(buf, m)
	got, _, err := getMap(buf)
	if err != nil {
		t.Fatalf("getMap: unexpected error: %v", err)
	}
	if len(got) != 0 {
		t.Fatalf("getMap: got %d entries, want 0", len(got))
	}
}

func TestGetMapBufferTooSmall(t *testing.T) {
	_, _, err := getMap([]byte{0, 0})
	if !errors.Is(err, ErrBufferTooSmall) {
		t.Fatalf("getMap: got %v, want ErrBufferTooSmall", err)
	}
}

func TestGetMapTooManyEntries(t *testing.T) {
	buf := make([]byte, 4)
	putUint32(buf, maxMembershipEntries+1)
	_, _, err := getMap(buf)
	if !errors.Is(err, ErrTooManyMembershipEntries) {
		t.Fatalf("getMap: got %v, want ErrTooManyMembershipEntries", err)
	}
}

func TestGetMapAddressTooLong(t *testing.T) {
	// Encode a map with one entry whose value exceeds MaxAddressLength
	buf := make([]byte, 4+8+4+MaxAddressLength+100)
	putUint32(buf, 1) // count = 1
	putUint64(buf[4:], 1)
	putUint32(buf[12:], uint32(MaxAddressLength+1)) // value too long
	_, _, err := getMap(buf)
	if !errors.Is(err, ErrAddressTooLong) {
		t.Fatalf("getMap: got %v, want ErrAddressTooLong", err)
	}
}

func TestGetMapEntryTruncated(t *testing.T) {
	// count=1 but buffer too short for key+value
	buf := make([]byte, 8)
	putUint32(buf, 1)
	_, _, err := getMap(buf)
	if !errors.Is(err, ErrBufferTooSmall) {
		t.Fatalf("getMap: got %v, want ErrBufferTooSmall", err)
	}
}

func TestGetMapValueTruncated(t *testing.T) {
	// count=1, key present, value length present but value data truncated
	buf := make([]byte, 4+8+4) // count + key + valueLen, no value data
	putUint32(buf, 1)
	putUint64(buf[4:], 1)
	putUint32(buf[12:], 10) // claims 10 bytes but none available
	_, _, err := getMap(buf)
	if !errors.Is(err, ErrBufferTooSmall) {
		t.Fatalf("getMap: got %v, want ErrBufferTooSmall", err)
	}
}

func TestPutGetBoolMapRoundTrip(t *testing.T) {
	m := map[uint64]bool{
		5: true,
		2: false,
		9: true,
	}
	sz := boolMapSize(m)
	buf := make([]byte, sz)
	n := putBoolMap(buf, m)
	if n != sz {
		t.Fatalf("putBoolMap: wrote %d, want %d", n, sz)
	}
	got, consumed, err := getBoolMap(buf)
	if err != nil {
		t.Fatalf("getBoolMap: unexpected error: %v", err)
	}
	if consumed != n {
		t.Fatalf("getBoolMap: consumed %d, want %d", consumed, n)
	}
	if len(got) != len(m) {
		t.Fatalf("getBoolMap: got %d entries, want %d", len(got), len(m))
	}
	for k, v := range m {
		if got[k] != v {
			t.Fatalf("getBoolMap[%d]: got %v, want %v", k, got[k], v)
		}
	}
}

func TestPutGetBoolMapEmpty(t *testing.T) {
	m := map[uint64]bool{}
	buf := make([]byte, boolMapSize(m))
	putBoolMap(buf, m)
	got, _, err := getBoolMap(buf)
	if err != nil {
		t.Fatalf("getBoolMap: unexpected error: %v", err)
	}
	if len(got) != 0 {
		t.Fatalf("getBoolMap: got %d entries, want 0", len(got))
	}
}

func TestGetBoolMapBufferTooSmall(t *testing.T) {
	_, _, err := getBoolMap([]byte{0, 0})
	if !errors.Is(err, ErrBufferTooSmall) {
		t.Fatalf("getBoolMap: got %v, want ErrBufferTooSmall", err)
	}
}

func TestGetBoolMapTooManyEntries(t *testing.T) {
	buf := make([]byte, 4)
	putUint32(buf, maxMembershipEntries+1)
	_, _, err := getBoolMap(buf)
	if !errors.Is(err, ErrTooManyMembershipEntries) {
		t.Fatalf("getBoolMap: got %v, want ErrTooManyMembershipEntries", err)
	}
}

func TestGetBoolMapEntryTruncated(t *testing.T) {
	buf := make([]byte, 8)
	putUint32(buf, 1) // count=1 but only 4 bytes of space remain
	_, _, err := getBoolMap(buf)
	if !errors.Is(err, ErrBufferTooSmall) {
		t.Fatalf("getBoolMap: got %v, want ErrBufferTooSmall", err)
	}
}

func TestMapSortedKeys(t *testing.T) {
	m := map[uint64]string{
		100: "c",
		1:   "a",
		50:  "b",
	}
	keys := sortedKeys(m)
	if len(keys) != 3 || keys[0] != 1 || keys[1] != 50 || keys[2] != 100 {
		t.Fatalf("sortedKeys: got %v, want [1, 50, 100]", keys)
	}
}

func TestBoolMapSortedKeys(t *testing.T) {
	m := map[uint64]bool{
		100: true,
		1:   false,
		50:  true,
	}
	keys := sortedBoolKeys(m)
	if len(keys) != 3 || keys[0] != 1 || keys[1] != 50 || keys[2] != 100 {
		t.Fatalf("sortedBoolKeys: got %v, want [1, 50, 100]", keys)
	}
}

func TestMapDeterministicEncoding(t *testing.T) {
	m := map[uint64]string{
		3: "c",
		1: "a",
		2: "b",
	}
	buf1 := make([]byte, mapSize(m))
	buf2 := make([]byte, mapSize(m))
	putMap(buf1, m)
	putMap(buf2, m)
	for i := range buf1 {
		if buf1[i] != buf2[i] {
			t.Fatalf("putMap not deterministic: buffers differ at byte %d", i)
		}
	}
}

func TestBoolMapDeterministicEncoding(t *testing.T) {
	m := map[uint64]bool{
		3: true,
		1: false,
		2: true,
	}
	buf1 := make([]byte, boolMapSize(m))
	buf2 := make([]byte, boolMapSize(m))
	putBoolMap(buf1, m)
	putBoolMap(buf2, m)
	for i := range buf1 {
		if buf1[i] != buf2[i] {
			t.Fatalf("putBoolMap not deterministic: buffers differ at byte %d", i)
		}
	}
}

func TestMapSize(t *testing.T) {
	tests := []struct {
		name string
		m    map[uint64]string
		want int
	}{
		{"nil", nil, 4},
		{"empty", map[uint64]string{}, 4},
		{"one entry", map[uint64]string{1: "abc"}, 4 + 8 + 4 + 3},
		{"two entries", map[uint64]string{1: "a", 2: "bb"}, 4 + (8 + 4 + 1) + (8 + 4 + 2)},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := mapSize(tt.m)
			if got != tt.want {
				t.Fatalf("mapSize: got %d, want %d", got, tt.want)
			}
		})
	}
}

func TestBoolMapSize(t *testing.T) {
	tests := []struct {
		name string
		m    map[uint64]bool
		want int
	}{
		{"nil", nil, 4},
		{"empty", map[uint64]bool{}, 4},
		{"one entry", map[uint64]bool{1: true}, 4 + 16},
		{"three entries", map[uint64]bool{1: true, 2: false, 3: true}, 4 + 3*16},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := boolMapSize(tt.m)
			if got != tt.want {
				t.Fatalf("boolMapSize: got %d, want %d", got, tt.want)
			}
		})
	}
}
