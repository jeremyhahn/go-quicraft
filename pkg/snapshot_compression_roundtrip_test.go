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

package quicraft

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jeremyhahn/go-quicraft/pkg/config"
	"github.com/jeremyhahn/go-quicraft/pkg/sm"
)

// snapshotPadding is written after the digest so a compressing codec has
// real, compressible bytes to work on: a snapshot of eight bytes would pass
// through Snappy without exercising a multi-block frame.
const snapshotPadding = 256 * 1024

// persistingHashSM is hashingStateMachine whose digest survives a snapshot:
// SaveSnapshot writes it, RecoverFromSnapshot reads it back and marks the
// instance as recovered so the test can tell a snapshot restore from a full
// log replay.
type persistingHashSM struct {
	hashingStateMachine
	recovered atomic.Bool
}

func (p *persistingHashSM) SaveSnapshot(_ context.Context, w io.Writer, _ <-chan struct{}) error {
	var hdr [8]byte
	binary.LittleEndian.PutUint64(hdr[:], p.sum)
	if _, err := w.Write(hdr[:]); err != nil {
		return err
	}
	_, err := w.Write(bytes.Repeat([]byte("quicraft-snapshot-"), snapshotPadding/18))
	return err
}

func (p *persistingHashSM) RecoverFromSnapshot(_ context.Context, r io.Reader, _ <-chan struct{}) error {
	var hdr [8]byte
	if _, err := io.ReadFull(r, hdr[:]); err != nil {
		return fmt.Errorf("reading the digest: %w", err)
	}
	rest, err := io.ReadAll(r)
	if err != nil {
		return fmt.Errorf("reading the padding: %w", err)
	}
	want := bytes.Repeat([]byte("quicraft-snapshot-"), snapshotPadding/18)
	if !bytes.Equal(rest, want) {
		return fmt.Errorf("padding: got %d bytes, want %d identical bytes", len(rest), len(want))
	}
	p.sum = binary.LittleEndian.Uint64(hdr[:])
	p.recovered.Store(true)
	return nil
}

var _ sm.StateMachine = (*persistingHashSM)(nil)

// snapshotCompressionTypes lists every type config.CompressionType.IsValid
// accepts. validCompressionTypes is unexported in package config, so the
// test probes the whole uint16 space instead of copying the list: a type
// added there is covered here without editing this file.
func snapshotCompressionTypes() []config.CompressionType {
	var out []config.CompressionType
	for v := 0; v <= 0xFFFF; v++ {
		if c := config.CompressionType(v); c.IsValid() {
			out = append(out, c)
		}
	}
	return out
}

// TestSnapshotCompression_RoundTripKeepsState saves a snapshot under each
// valid SnapshotCompressionType, restarts the replica on the same on-disk
// WAL so it recovers from that snapshot, and asserts the recovered state
// machine hashes to the value recorded at save time.
func TestSnapshotCompression_RoundTripKeepsState(t *testing.T) {
	types := snapshotCompressionTypes()
	if len(types) < 2 {
		t.Fatalf("found %d valid compression types, want at least NoCompression and Snappy", len(types))
	}
	for _, ct := range types {
		t.Run(ct.String(), func(t *testing.T) {
			hostCfg := testHostConfig(t)
			scfg := testShardConfig(1, 1)
			scfg.SnapshotCompressionType = ct
			members := map[uint64]string{1: "127.0.0.1:5000"}
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()

			h, err := NewHost(hostCfg, WithoutTransport(), WithoutEncryption())
			if err != nil {
				t.Fatalf("NewHost: %v", err)
			}
			first := &persistingHashSM{}
			if err := h.StartShard(members, false,
				func(_, _ uint64) interface{} { return first }, scfg); err != nil {
				h.Close()
				t.Fatalf("StartShard: %v", err)
			}
			waitForLeader(t, h, 1)
			for i := 0; i < 16; i++ {
				if _, err := h.SyncPropose(ctx, 1, []byte(fmt.Sprintf("set:k%d=%d", i, i*i))); err != nil {
					h.Close()
					t.Fatalf("SyncPropose %d: %v", i, err)
				}
			}
			snapIndex, err := h.SyncRequestSnapshot(ctx, 1, SnapshotOption{})
			if err != nil {
				h.Close()
				t.Fatalf("SyncRequestSnapshot: %v", err)
			}
			savedHash, savedIdx, ok, err := h.StateMachineHash(1)
			if err != nil || !ok {
				h.Close()
				t.Fatalf("StateMachineHash at save: ok=%v err=%v", ok, err)
			}
			if savedIdx != snapIndex {
				h.Close()
				t.Fatalf("applied index %d != snapshot index %d: the hash would not "+
					"describe the snapshot", savedIdx, snapIndex)
			}
			rec, err := h.logdb.GetSnapshot(1, 1)
			if err != nil {
				h.Close()
				t.Fatalf("GetSnapshot: %v", err)
			}
			if rec.Index != snapIndex || rec.Compression != ct {
				h.Close()
				t.Fatalf("snapshot record = (index %d, %v), want (index %d, %v)",
					rec.Index, rec.Compression, snapIndex, ct)
			}
			if err := h.Close(); err != nil {
				t.Fatalf("Close: %v", err)
			}

			restarted, err := NewHost(hostCfg, WithoutTransport(), WithoutEncryption())
			if err != nil {
				t.Fatalf("NewHost (restart): %v", err)
			}
			defer restarted.Close()
			second := &persistingHashSM{}
			if err := restarted.StartShard(members, false,
				func(_, _ uint64) interface{} { return second }, scfg); err != nil {
				t.Fatalf("StartShard (restart): %v", err)
			}
			waitForLeader(t, restarted, 1)
			if !second.recovered.Load() {
				t.Fatal("the restarted replica did not recover from the snapshot: " +
					"a full log replay would hide a broken codec")
			}
			gotHash, _, ok, err := restarted.StateMachineHash(1)
			if err != nil || !ok {
				t.Fatalf("StateMachineHash after restart: ok=%v err=%v", ok, err)
			}
			if gotHash != savedHash {
				t.Fatalf("recovered hash %d != hash at save %d under %v compression",
					gotHash, savedHash, ct)
			}
		})
	}
}

// TestSnapshotCompression_InvalidTypeRejected asserts StartShard refuses an
// unrecognized SnapshotCompressionType with the documented validation error
// instead of starting a shard whose snapshots no binary could decode.
func TestSnapshotCompression_InvalidTypeRejected(t *testing.T) {
	invalid := config.CompressionType(0xBEEF)
	if invalid.IsValid() {
		t.Fatalf("fixture: %d is a valid compression type", invalid)
	}
	h := newTestHost(t)
	scfg := testShardConfig(1, 1)
	scfg.SnapshotCompressionType = invalid
	err := h.StartShard(map[uint64]string{1: "127.0.0.1:5000"}, false, hashingCreateFunc, scfg)
	if err == nil {
		t.Fatal("StartShard accepted an unrecognized SnapshotCompressionType")
	}
	var ve *config.ValidationError
	if !errors.As(err, &ve) {
		t.Fatalf("StartShard error = %v (%T), want a *config.ValidationError", err, err)
	}
	if ve.Field != "SnapshotCompressionType" || ve.Message != "unrecognized compression type" {
		t.Fatalf("ValidationError = {Field: %q, Message: %q}, want "+
			"{SnapshotCompressionType, unrecognized compression type}", ve.Field, ve.Message)
	}
}
