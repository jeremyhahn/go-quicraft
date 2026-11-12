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
	"io"
	"math/rand"
	"reflect"
	"testing"

	"github.com/jeremyhahn/go-quicraft/pkg/sm"
)

// parityKeys is a small key space so commands collide on the same keys.
// It includes the empty key, a NUL key, non-UTF-8 bytes and shared
// prefixes for ListQuery.
var parityKeys = [][]byte{
	{}, []byte("a"), []byte("ab"), []byte("a/b"), []byte("b"), {0x00}, {'k', 0xff},
}

// parityPrefixes are the ListQuery prefixes compared after every batch.
var parityPrefixes = []string{"", "a", "a/", "b", "zz"}

// parityStore is the surface both KV state machines share.
type parityStore interface {
	Update(ctx context.Context, entries []sm.Entry, results []sm.Result) error
	Lookup(ctx context.Context, query interface{}) (interface{}, error)
	BytesLookup(key []byte) ([]byte, error)
	BytesLookupInto(key, dst []byte) ([]byte, error)
	GetHash() (uint64, error)
	RecoverFromSnapshot(ctx context.Context, r io.Reader, stopper <-chan struct{}) error
}

// parityStream turns arbitrary bytes into a command stream. Once the input
// is exhausted every read returns zero.
type parityStream struct {
	data []byte
	pos  int
}

func (s *parityStream) done() bool { return s.pos >= len(s.data) }

func (s *parityStream) next() byte {
	if s.pos >= len(s.data) {
		return 0
	}
	b := s.data[s.pos]
	s.pos++
	return b
}

func (s *parityStream) key() []byte {
	return parityKeys[int(s.next())%len(parityKeys)]
}

func (s *parityStream) value() []byte {
	v := make([]byte, int(s.next())%5)
	for i := range v {
		v[i] = s.next()
	}
	return v
}

// command returns one encoded command, valid or malformed.
func (s *parityStream) command(t testing.TB) []byte {
	var (
		cmd []byte
		err error
	)
	switch s.next() % 9 {
	case 0, 1:
		cmd, err = EncodePut(s.key(), s.value())
	case 2:
		cmd, err = EncodePutIfAbsent(s.key(), s.value())
	case 3:
		cmd, err = EncodeDelete(s.key())
	case 4:
		cmd, err = EncodeGet(s.key())
	case 5:
		// Shorter than the header.
		cmd = make([]byte, int(s.next())%headerSize)
		for i := range cmd {
			cmd[i] = s.next()
		}
	case 6:
		// Unknown opcode: 0 or past OpPutIfAbsent.
		cmd, err = EncodePut(s.key(), s.value())
		if op := s.next(); op <= OpPutIfAbsent {
			cmd[0] = 0
		} else {
			cmd[0] = op
		}
	case 7:
		// Key length running past the data.
		cmd, err = EncodeDelete(s.key())
		binary.LittleEndian.PutUint16(cmd[1:3], uint16(len(cmd)-headerSize+1+int(s.next())))
	case 8:
		// Raw bytes with a valid opcode, so the key/value split is arbitrary.
		cmd = make([]byte, headerSize+int(s.next())%6)
		for i := range cmd {
			cmd[i] = s.next()
		}
		cmd[0] = OpPut + cmd[0]%4
		cmd[2] = 0
		cmd[1] %= 4
	}
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	return cmd
}

// runStoreParity applies the stream to both stores and fails on the first
// difference.
func runStoreParity(t testing.TB, data []byte) {
	t.Helper()
	mem := NewMemoryStore()
	con := NewConcurrentStore()
	s := &parityStream{data: data}
	var index uint64
	for batch := 0; !s.done(); batch++ {
		n := 1 + int(s.next())%8
		cmds := make([][]byte, n)
		for i := range cmds {
			cmds[i] = s.command(t)
		}
		memRes := applyParity(t, mem, cmds, &index)
		index -= uint64(n)
		conRes := applyParity(t, con, cmds, &index)
		if !reflect.DeepEqual(memRes, conRes) {
			t.Fatalf("batch %d %x: Update results differ: memory %v, concurrent %v", batch, cmds, memRes, conRes)
		}
		compareStores(t, "after batch", mem, con)
	}
	compareSnapshots(t, mem, con)
}

// applyParity applies cmds through a private copy of each buffer and
// scribbles over the copies afterwards, so a store that keeps an alias into
// Entry.Cmd shows up as a later mismatch.
func applyParity(t testing.TB, st parityStore, cmds [][]byte, index *uint64) []sm.Result {
	t.Helper()
	entries := make([]sm.Entry, len(cmds))
	for i, c := range cmds {
		*index++
		entries[i] = sm.Entry{Index: *index, Cmd: append([]byte(nil), c...)}
	}
	results := make([]sm.Result, len(cmds))
	if err := st.Update(context.Background(), entries, results); err != nil {
		t.Fatalf("Update: %v", err)
	}
	for _, e := range entries {
		for i := range e.Cmd {
			e.Cmd[i] = 0xEE
		}
	}
	return results
}

// lookupAnswer is one store's answer to a query, normalised for comparison.
type lookupAnswer struct {
	Val interface{}
	Err string
}

func answer(val interface{}, err error) lookupAnswer {
	if err != nil {
		return lookupAnswer{Err: err.Error()}
	}
	return lookupAnswer{Val: val}
}

// storeAnswers collects every read answer a store gives.
func storeAnswers(t testing.TB, st parityStore) []lookupAnswer {
	t.Helper()
	ctx := context.Background()
	var out []lookupAnswer
	for _, k := range parityKeys {
		out = append(out,
			answer(st.Lookup(ctx, k)),
			answer(st.Lookup(ctx, string(k))),
			answer(st.BytesLookup(k)),
			answer(st.BytesLookupInto(k, make([]byte, 0, 2))))
	}
	for _, p := range parityPrefixes {
		out = append(out, answer(st.Lookup(ctx, ListQuery{Prefix: p})))
	}
	out = append(out, answer(st.Lookup(ctx, 42)), answer(st.GetHash()))
	return out
}

func compareStores(t testing.TB, stage string, a, b parityStore) {
	t.Helper()
	ga, gb := storeAnswers(t, a), storeAnswers(t, b)
	if !reflect.DeepEqual(ga, gb) {
		t.Fatalf("%s: read answers differ:\n first %v\nsecond %v", stage, ga, gb)
	}
}

// dumpStore returns every key and value a store holds.
func dumpStore(t testing.TB, st parityStore) map[string][]byte {
	t.Helper()
	keys, err := st.Lookup(context.Background(), ListQuery{})
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	out := make(map[string][]byte)
	for _, k := range keys.([]string) {
		v, err := st.BytesLookup([]byte(k))
		if err != nil {
			t.Fatalf("lookup %q: %v", k, err)
		}
		out[k] = v
	}
	return out
}

func saveMemory(t testing.TB, m *MemoryStore) []byte {
	t.Helper()
	var buf bytes.Buffer
	if err := m.SaveSnapshot(context.Background(), &buf, make(chan struct{})); err != nil {
		t.Fatalf("memory SaveSnapshot: %v", err)
	}
	return buf.Bytes()
}

func saveConcurrent(t testing.TB, c *ConcurrentStore) []byte {
	t.Helper()
	snap, err := c.PrepareSnapshot()
	if err != nil {
		t.Fatalf("concurrent PrepareSnapshot: %v", err)
	}
	var buf bytes.Buffer
	if err := c.SaveSnapshot(context.Background(), snap, &buf, make(chan struct{})); err != nil {
		t.Fatalf("concurrent SaveSnapshot: %v", err)
	}
	return buf.Bytes()
}

func recoverInto(t testing.TB, st parityStore, snap []byte) parityStore {
	t.Helper()
	if err := st.RecoverFromSnapshot(context.Background(), bytes.NewReader(snap), make(chan struct{})); err != nil {
		t.Fatalf("RecoverFromSnapshot: %v", err)
	}
	return st
}

// compareSnapshots saves a snapshot from each store, requires identical
// bytes for identical state, and recovers each into a fresh store of the
// other kind.
func compareSnapshots(t testing.TB, mem *MemoryStore, con *ConcurrentStore) {
	t.Helper()
	memSnap, conSnap := saveMemory(t, mem), saveConcurrent(t, con)
	if !bytes.Equal(memSnap, conSnap) {
		t.Fatalf("snapshot bytes differ for identical state:\n    memory %x\nconcurrent %x", memSnap, conSnap)
	}
	want := dumpStore(t, mem)
	conFromMem := recoverInto(t, NewConcurrentStore(), memSnap)
	memFromCon := recoverInto(t, NewMemoryStore(), conSnap)
	for name, st := range map[string]parityStore{
		"concurrent from memory snapshot": conFromMem,
		"memory from concurrent snapshot": memFromCon,
	} {
		if got := dumpStore(t, st); !reflect.DeepEqual(got, want) {
			t.Fatalf("%s: key set differs:\n got %q\nwant %q", name, got, want)
		}
		compareStores(t, name, mem, st)
	}
}

// TestStoreParity applies seeded random command sequences to both stores.
func TestStoreParity(t *testing.T) {
	rng := rand.New(rand.NewSource(769))
	iterations := 300
	if testing.Short() {
		iterations = 50
	}
	for i := 0; i < iterations; i++ {
		data := make([]byte, 64+rng.Intn(2048))
		rng.Read(data)
		runStoreParity(t, data)
	}
}

// TestStoreParityEmpty covers the empty sequence: two empty stores.
func TestStoreParityEmpty(t *testing.T) {
	runStoreParity(t, nil)
}

// FuzzStoreParity fuzzes the command stream applied to both stores.
func FuzzStoreParity(f *testing.F) {
	// One seed per command selector, then a longer mixed stream.
	for sel := byte(0); sel < 9; sel++ {
		f.Add([]byte{0, sel, 1, 2, 3, 4, 5, 6, 7})
	}
	mixed := make([]byte, 512)
	for i := range mixed {
		mixed[i] = byte(i * 7)
	}
	f.Add(mixed)
	f.Fuzz(func(t *testing.T, data []byte) {
		if len(data) > 1<<14 {
			t.Skip("stream too long")
		}
		runStoreParity(t, data)
	})
}
