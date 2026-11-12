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

package shamir

import (
	"bytes"
	"encoding/json"
	"maps"
	"math/rand/v2"
	"testing"

	"github.com/jeremyhahn/go-enclave"
)

// fuzzSecret is the secret the share-set tests split.
var fuzzSecret = []byte("root key material, 3-of-5")

// shareJSONFuzzSeeds returns one share and one share set produced by
// SplitProtected, plus the malformed documents the unmarshal tests refuse.
func shareJSONFuzzSeeds(f *testing.F) (single, sets [][]byte) {
	f.Helper()
	shares, err := SplitProtected(fuzzSecret, 3, 5)
	if err != nil {
		f.Fatalf("SplitProtected: %v", err)
	}
	defer destroyAll(shares)
	shares[0].Metadata = map[string]string{"holder": "a"}
	one, err := shares[0].MarshalJSON()
	if err != nil {
		f.Fatalf("MarshalJSON: %v", err)
	}
	set, err := json.Marshal(shares)
	if err != nil {
		f.Fatalf("marshal share set: %v", err)
	}
	single = [][]byte{
		one,
		[]byte(`{"index":1,"threshold":2,"total":3,"value":"AQEC"}`),
		[]byte(`["nope"]`),
		[]byte(`{"index":1,"threshold":2,"total":3,"value":123}`),
		[]byte(`{"index":1,"threshold":2,"total":3,"value":""}`),
		[]byte(`{"index":1,"threshold":2,"total":3,"value":"AQEC"}`),
		[]byte(`{"index":9,"threshold":2,"total":3,"value":"AQEC"}`),
	}
	sets = [][]byte{
		set,
		[]byte(`[]`),
		[]byte(`[` + string(one) + `,{"index":9}]`),
		[]byte(`null`),
	}
	return single, sets
}

// sameShare reports whether two shares carry the same header, metadata and
// value.
func sameShare(a, b *ProtectedShare) bool {
	return a.Index == b.Index && a.Threshold == b.Threshold && a.Total == b.Total &&
		maps.Equal(a.Metadata, b.Metadata) && bytes.Equal(valueCopy(a), valueCopy(b))
}

// FuzzProtectedShareUnmarshalJSON feeds arbitrary bytes to
// ProtectedShare.UnmarshalJSON. It must never panic, and a share it accepts
// must marshal and decode back to the same share.
func FuzzProtectedShareUnmarshalJSON(f *testing.F) {
	single, _ := shareJSONFuzzSeeds(f)
	for _, seed := range single {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, data []byte) {
		var s ProtectedShare
		if err := s.UnmarshalJSON(data); err != nil {
			return
		}
		defer s.Destroy()
		enc, err := s.MarshalJSON()
		if err != nil {
			t.Fatalf("accepted share does not marshal: %v", err)
		}
		var again ProtectedShare
		if err := again.UnmarshalJSON(enc); err != nil {
			t.Fatalf("re-encoded share refused: %v\n%s", err, enc)
		}
		defer again.Destroy()
		if !sameShare(&s, &again) {
			t.Fatalf("share changed on round trip: %s", enc)
		}
	})
}

// FuzzUnmarshalShareSet feeds arbitrary bytes to UnmarshalShareSet. It must
// never panic, and a set it accepts must marshal and decode back to the same
// shares.
func FuzzUnmarshalShareSet(f *testing.F) {
	_, sets := shareJSONFuzzSeeds(f)
	for _, seed := range sets {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, data []byte) {
		shares, err := UnmarshalShareSet(data)
		if err != nil {
			if shares != nil {
				t.Fatalf("refused set returned %d shares", len(shares))
			}
			return
		}
		defer destroyAll(shares)
		enc, err := json.Marshal(shares)
		if err != nil {
			t.Fatalf("accepted set does not marshal: %v", err)
		}
		again, err := UnmarshalShareSet(enc)
		if err != nil {
			t.Fatalf("re-encoded set refused: %v", err)
		}
		defer destroyAll(again)
		if len(again) != len(shares) {
			t.Fatalf("re-encoded set has %d shares, want %d", len(again), len(shares))
		}
		for i := range shares {
			if !sameShare(shares[i], again[i]) {
				t.Fatalf("share %d changed on round trip", i)
			}
		}
	})
}

// FuzzUnmarshalShareSetWith decodes through a real enclave allocator whose
// budget, zero to eight pages, comes from the fuzz input, so budgets too small
// for the set are fuzzed alongside ones that fit. On every input: no panic; a
// refusal returns no shares and leaves no reservation behind; an accepted set
// stays within the budget and round-trips; destroying it releases every
// reservation.
func FuzzUnmarshalShareSetWith(f *testing.F) {
	_, sets := shareJSONFuzzSeeds(f)
	for _, seed := range sets {
		for _, pages := range []uint8{0, 1, 2, 7} {
			f.Add(seed, pages)
		}
	}
	f.Fuzz(func(t *testing.T, data []byte, pages uint8) {
		a, err := enclave.NewAllocator(8 * pageBytes())
		if err != nil {
			t.Fatalf("NewAllocator(%d): %v", 8*pageBytes(), err)
		}
		limit := int64(pages%9) * pageBytes()
		if err := a.SetLimit(limit); err != nil {
			t.Fatalf("SetLimit(%d): %v", limit, err)
		}
		shares, err := UnmarshalShareSetWith(a, data)
		if limit == 0 && err == nil {
			destroyAll(shares)
			t.Fatalf("decode with no admission headroom accepted %d shares", len(shares))
		}
		if err != nil {
			if shares != nil {
				destroyAll(shares)
				t.Fatalf("refused set returned %d shares", len(shares))
			}
			if got := a.InUse(); got != 0 {
				t.Fatalf("InUse after a refusal = %d, want 0 (%v)", got, err)
			}
			return
		}
		if got := a.InUse(); got <= 0 || got > limit {
			destroyAll(shares)
			t.Fatalf("InUse after decoding %d shares = %d, want in (0, %d]", len(shares), got, limit)
		}
		enc, err := json.Marshal(shares)
		if err != nil {
			destroyAll(shares)
			t.Fatalf("accepted set does not marshal: %v", err)
		}
		again, err := UnmarshalShareSet(enc)
		if err != nil {
			destroyAll(shares)
			t.Fatalf("re-encoded set refused: %v", err)
		}
		defer destroyAll(again)
		if len(again) != len(shares) {
			destroyAll(shares)
			t.Fatalf("re-encoded set has %d shares, want %d", len(again), len(shares))
		}
		for i := range shares {
			if !sameShare(shares[i], again[i]) {
				destroyAll(shares)
				t.Fatalf("share %d changed on round trip", i)
			}
		}
		destroyAll(shares)
		if got := a.InUse(); got != 0 {
			t.Fatalf("InUse after Destroy = %d, want 0", got)
		}
	})
}

// FuzzCombineBytes feeds arbitrary share values to CombineBytes, which
// decodes each through parseRawShare. It must never panic, and a secret it
// returns is as long as the shares' payload.
func FuzzCombineBytes(f *testing.F) {
	values, err := SplitBytes(fuzzSecret, 2, 3)
	if err != nil {
		f.Fatalf("SplitBytes: %v", err)
	}
	f.Add(values[0], values[1], values[2], uint8(2))
	f.Add(values[0], values[0], values[1], uint8(2))
	f.Add([]byte("AQEC"), []byte("AQID"), []byte("AQME"), uint8(3))
	f.Add([]byte("AAEC"), []byte("!!"), []byte(""), uint8(2))
	f.Fuzz(func(t *testing.T, a, b, c []byte, threshold uint8) {
		secret, err := CombineBytes([][]byte{a, b, c}, int(threshold))
		if err != nil {
			if secret != nil {
				t.Fatal("refused set returned a secret")
			}
			return
		}
		if want := len(a)/4*3 - rawShareOverhead; len(secret) > want {
			t.Fatalf("secret of %d bytes from a %d-byte share", len(secret), len(a))
		}
	})
}

// FuzzSplitCombine splits an arbitrary secret and requires every subset of at
// least threshold shares the fuzzer picks to reconstruct it, through both the
// plain and the protected paths.
func FuzzSplitCombine(f *testing.F) {
	f.Add(fuzzSecret, uint8(3), uint8(5), uint64(0))
	f.Add([]byte{0}, uint8(2), uint8(2), uint64(1))
	f.Add([]byte{0xff, 0x00, 0x80}, uint8(255), uint8(255), uint64(7))
	f.Add(bytes.Repeat([]byte{0xa5}, 64), uint8(2), uint8(10), uint64(42))
	f.Fuzz(func(t *testing.T, secret []byte, threshold, total uint8, seed uint64) {
		values, err := SplitBytes(secret, int(threshold), int(total))
		if err != nil {
			return
		}
		rng := rand.New(rand.NewPCG(seed, seed^0x9e3779b97f4a7c15))
		perm := rng.Perm(int(total))
		k := int(threshold) + rng.IntN(int(total)-int(threshold)+1)
		subset := make([][]byte, k)
		for i := range subset {
			subset[i] = values[perm[i]]
		}
		got, err := CombineBytes(subset, int(threshold))
		if err != nil {
			t.Fatalf("CombineBytes of %d of %d shares (threshold %d): %v", k, total, threshold, err)
		}
		if !bytes.Equal(got, secret) {
			t.Fatalf("CombineBytes of %d of %d shares (threshold %d) did not return the secret", k, total, threshold)
		}

		// The protected path locks a page per share; keep it to small sets
		// so the fuzzer stays within the process's locked-memory limit.
		if total > 16 || len(secret) > 1024 {
			return
		}
		shares, err := SplitProtected(secret, int(threshold), int(total))
		if err != nil {
			t.Fatalf("SplitProtected refused what SplitBytes accepted: %v", err)
		}
		defer destroyAll(shares)
		picked := make([]*ProtectedShare, k)
		for i := range picked {
			picked[i] = shares[perm[i]]
		}
		out, err := CombineProtected(picked)
		if err != nil {
			t.Fatalf("CombineProtected of %d of %d shares (threshold %d): %v", k, total, threshold, err)
		}
		defer out.Destroy()
		if !bytes.Equal(encCopy(out), secret) {
			t.Fatalf("CombineProtected of %d of %d shares (threshold %d) did not return the secret", k, total, threshold)
		}
	})
}
