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

package engine

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"testing"

	"github.com/jeremyhahn/go-quicraft/pkg/config"
	"github.com/jeremyhahn/go-quicraft/pkg/internal/raft"
)

var errFrameSourceIO = errors.New("injected source I/O failure")

// failingSource yields n bytes of zeros and then err.
type failingSource struct {
	n   int
	err error
}

func (s *failingSource) Read(p []byte) (int, error) {
	if s.n == 0 {
		return 0, s.err
	}
	if len(p) > s.n {
		p = p[:s.n]
	}
	clear(p)
	s.n -= len(p)
	return len(p), nil
}

// TestSealSnapshotFrames_SourceErrorReleasesPins covers the one failure
// SealSnapshotFrames handles itself rather than the frame writer: the source
// fails after a frame has been sealed and pinned. The error must be the
// source's, and no pin may survive or be handed back.
func TestSealSnapshotFrames_SourceErrorReleasesPins(t *testing.T) {
	barrier := newPinCountBarrier(newTestBarrier(3))
	// One byte past a chunk, so the held first chunk is sealed (and pinned)
	// before the source fails.
	src := &failingSource{n: snapshotEncryptChunkSize + 1, err: errFrameSourceIO}
	epoch, pins, err := SealSnapshotFrames(barrier, 2, 1, 42, src, io.Discard)
	if !errors.Is(err, errFrameSourceIO) {
		t.Fatalf("SealSnapshotFrames = %v, want the source error", err)
	}
	if epoch != 0 || pins != nil {
		t.Fatalf("failure returned epoch %d and pins %v, want 0 and nil", epoch, pins)
	}
	if barrier.regs == 0 {
		t.Fatalf("fixture: nothing was pinned, so the release is untested: %s", barrier.snapshotCounts())
	}
	barrier.assertBalanced(t, "SealSnapshotFrames source error")
}

// TestSealSnapshotFrames_SealErrorReleasesPins is the same contract for a
// failure inside the frame writer, surfacing through Close.
func TestSealSnapshotFrames_SealErrorReleasesPins(t *testing.T) {
	barrier := newPinCountBarrier(&failAfterNBarrier{testBarrier: newTestBarrier(7), failAfter: 1})
	epoch, pins, err := SealSnapshotFrames(barrier, 2, 1, 42,
		bytes.NewReader(make([]byte, 3*snapshotEncryptChunkSize)), io.Discard)
	if err == nil {
		t.Fatal("expected the injected sealing failure")
	}
	if epoch != 0 || pins != nil {
		t.Fatalf("failure returned epoch %d and pins %v, want 0 and nil", epoch, pins)
	}
	barrier.assertBalanced(t, "SealSnapshotFrames seal error")
}

// TestSnapshotFrameWriter_CloseReturnsTheLatchedFailure pins that a Close
// after a failed Write reports the FIRST failure rather than sealing anything.
func TestSnapshotFrameWriter_CloseReturnsTheLatchedFailure(t *testing.T) {
	barrier := newPinCountBarrier(&failAfterNBarrier{testBarrier: newTestBarrier(7), failAfter: 0})
	w := newSnapshotFrameWriter(io.Discard, barrier, 2, 1, 42)
	_, werr := w.Write(make([]byte, 2*snapshotEncryptChunkSize))
	if werr == nil {
		t.Fatal("expected the injected sealing failure from Write")
	}
	if cerr := w.Close(); cerr != werr {
		t.Fatalf("Close = %v, want the latched Write failure %v", cerr, werr)
	}
	barrier.assertBalanced(t, "frame writer latched failure")
}

// TestOpenSnapshotFrames_SourceFailureIsNotCorruption keeps an I/O error on
// the source distinct from damage to the artifact: an operator acting on
// ErrSnapshotCorrupt discards the artifact, which is the wrong response to a
// disk that hiccuped.
func TestOpenSnapshotFrames_SourceFailureIsNotCorruption(t *testing.T) {
	barrier := newTestBarrier(4)
	var sealed bytes.Buffer
	if _, pins, err := SealSnapshotFrames(barrier, 2, 1, 42,
		bytes.NewReader(make([]byte, 2*snapshotEncryptChunkSize)), &sealed); err != nil {
		t.Fatalf("SealSnapshotFrames: %v", err)
	} else {
		for _, e := range pins {
			barrier.DeregisterInFlightEpoch(e)
		}
	}
	src := io.MultiReader(bytes.NewReader(sealed.Bytes()[:sealed.Len()/2]),
		&failingSource{err: errFrameSourceIO})
	_, err := OpenSnapshotFrames(barrier, 2, 1, 42, src, io.Discard)
	if !errors.Is(err, errFrameSourceIO) {
		t.Fatalf("OpenSnapshotFrames = %v, want the source error", err)
	}
	if errors.Is(err, ErrSnapshotCorrupt) {
		t.Fatalf("a source I/O failure was reported as a corrupt artifact: %v", err)
	}
}

// TestOpenSnapshotFrames_OversizedLengthIsCorrupt covers the allocation guard:
// a length prefix beyond one chunk plus framing is damage, refused before
// anything is allocated for it.
func TestOpenSnapshotFrames_OversizedLengthIsCorrupt(t *testing.T) {
	var lenBuf [4]byte
	binary.LittleEndian.PutUint32(lenBuf[:], snapshotEncryptChunkSize+maxSnapshotFrameOverhead+1)
	_, err := OpenSnapshotFrames(newTestBarrier(4), 2, 1, 42, bytes.NewReader(lenBuf[:]), io.Discard)
	if !errors.Is(err, ErrSnapshotCorrupt) {
		t.Fatalf("OpenSnapshotFrames = %v, want ErrSnapshotCorrupt", err)
	}
}

// TestNode_NilPeerConventions pins the documented answer every identity and
// removal accessor gives before the node has a peer. They are NOT uniform on
// purpose: MemberBinding is a GATE and fails closed — nothing committed can
// authorize a sender — while the deny-list and view accessors report nothing
// known, leaving the raft layer's own rules to apply.
func TestNode_NilPeerConventions(t *testing.T) {
	cfg := config.Config{ShardID: 1, ReplicaID: 1}
	cfg.SetDefaults()
	n := NewNode(nil, nil, nil, cfg, nil, "", nil)

	if b, ok := n.MemberBinding(2); ok || b != (raft.MemberBinding{}) {
		t.Errorf("MemberBinding with no peer = (%+v, %v), want a closed gate (zero, false)", b, ok)
	}
	if n.IsRemoved(2) {
		t.Error("IsRemoved with no peer = true, want false")
	}
	if n.IsRemovedIdentity("spki:abc") {
		t.Error("IsRemovedIdentity with no peer = true, want false")
	}
	if n.PendingRemoval(2, "spki:abc") {
		t.Error("PendingRemoval with no peer = true, want false")
	}
	if id, ok := n.IdentityHolder("spki:abc"); ok || id != 0 {
		t.Errorf("IdentityHolder with no peer = (%d, %v), want (0, false)", id, ok)
	}
	if id, ok := n.PendingMemberIdentity(2); ok || id != "" {
		t.Errorf("PendingMemberIdentity with no peer = (%q, %v), want (\"\", false)", id, ok)
	}
	if n.IsJoining() {
		t.Error("IsJoining with no peer = true, want false")
	}
	if leader, term := n.LeaderAndTerm(); leader != 0 || term != 0 {
		t.Errorf("LeaderAndTerm with no peer = (%d, %d), want (0, 0)", leader, term)
	}
}
