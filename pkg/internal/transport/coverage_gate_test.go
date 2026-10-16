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

package transport

import (
	"bytes"
	"crypto/tls"
	"errors"
	"testing"
	"time"

	"github.com/quic-go/quic-go"

	"github.com/jeremyhahn/go-quicraft/pkg/internal/registry"
	"github.com/jeremyhahn/go-quicraft/pkg/proto"
)

// TestBuildMTLSTLS_PrebuiltCertificateWithoutLeaf covers the pre-built
// Certificate path (a hardware signer) whose Leaf is not populated: the leaf
// must be parsed from the DER chain, and a missing or unparsable chain is
// rejected because the SAN-exclusivity precondition needs the leaf.
func TestBuildMTLSTLS_PrebuiltCertificateWithoutLeaf(t *testing.T) {
	base := testMTLSConfig(t)
	pair, err := tls.X509KeyPair(base.Cert, base.Key)
	if err != nil {
		t.Fatalf("X509KeyPair: %v", err)
	}
	withChain := func(chain [][]byte) *MTLSConfig {
		cert := pair
		cert.Leaf = nil
		cert.Certificate = chain
		return &MTLSConfig{CACert: base.CACert, Certificate: &cert}
	}

	if _, err := buildMTLSTLS(withChain(nil), nil); !errors.Is(err, ErrInvalidCertKeyPair) {
		t.Fatalf("empty chain: got %v, want ErrInvalidCertKeyPair", err)
	}
	if _, err := buildMTLSTLS(withChain([][]byte{[]byte("not a certificate")}), nil); !errors.Is(err, ErrInvalidCertKeyPair) {
		t.Fatalf("garbage DER: got %v, want ErrInvalidCertKeyPair", err)
	}

	got, err := buildMTLSTLS(withChain(pair.Certificate), nil)
	if err != nil {
		t.Fatalf("valid DER chain: %v", err)
	}
	if got.leaf == nil {
		t.Fatal("valid DER chain: leaf not parsed")
	}
	if !bytes.Equal(got.leaf.Raw, pair.Certificate[0]) {
		t.Fatal("valid DER chain: parsed leaf is not the chain's first certificate")
	}
}

// unownStream removes the owner record of target's pooled stream for shardID,
// leaving the raw-stream shape a manually constructed transport produces.
func unownStream(tr *QUICTransport, target string, shardID uint64) {
	idx := tr.streamIndexFor(shardID, false)
	shard := &tr.streamShards[idx&(streamShardCount-1)]
	shard.mu.Lock()
	delete(shard.owners, streamKey{target: target, idx: idx})
	shard.mu.Unlock()
}

// hasPooledStream reports whether target has a pooled stream for shardID.
func hasPooledStream(tr *QUICTransport, target string, shardID uint64) bool {
	idx := tr.streamIndexFor(shardID, false)
	shard := &tr.streamShards[idx&(streamShardCount-1)]
	shard.mu.RLock()
	defer shard.mu.RUnlock()
	_, ok := shard.streams[streamKey{target: target, idx: idx}]
	return ok
}

// TestCachedStream_UnownedStreamOnLiveConnection: with no owner recorded, the
// cached connection's liveness decides, and a live one returns the stream.
func TestCachedStream_UnownedStreamOnLiveConnection(t *testing.T) {
	t1, t2, _, _, cleanup := transportPair(t, 42)
	defer cleanup()
	target := t2.Addr().String()

	s, err := t1.getStream(target, 1, false, "")
	if err != nil {
		t.Fatalf("getStream: %v", err)
	}
	unownStream(t1, target, 1)

	got, ok := t1.cachedStream(target, 1, false, "")
	if !ok || got != s {
		t.Fatalf("cachedStream = (%p, %v), want (%p, true)", got, ok, s)
	}
}

// TestCachedStream_UnownedStreamOnClosedConnection: an unowned stream whose
// cached connection is dead removes that connection and evicts its streams.
func TestCachedStream_UnownedStreamOnClosedConnection(t *testing.T) {
	t1, t2, _, _, cleanup := transportPair(t, 42)
	defer cleanup()
	target := t2.Addr().String()

	if _, err := t1.getStream(target, 1, false, ""); err != nil {
		t.Fatalf("getStream: %v", err)
	}
	unownStream(t1, target, 1)

	t1.connMu.RLock()
	conn, ok := t1.conns[target]
	t1.connMu.RUnlock()
	if !ok {
		t.Fatal("expected a cached connection")
	}
	conn.CloseWithError(0, "test: force dead connection")
	select {
	case <-conn.Context().Done():
	case <-time.After(5 * time.Second):
		t.Fatal("connection context not done after close")
	}

	if got, ok := t1.cachedStream(target, 1, false, ""); ok || got != nil {
		t.Fatalf("cachedStream on dead conn = (%p, %v), want (nil, false)", got, ok)
	}
	t1.connMu.RLock()
	_, stillCached := t1.conns[target]
	t1.connMu.RUnlock()
	if stillCached {
		t.Fatal("dead connection still cached")
	}
	if hasPooledStream(t1, target, 1) {
		t.Fatal("stream on dead connection not evicted")
	}
}

// TestGetStream_UnownedEntryAfterRemoveConnectionReturnsExisting: an unowned
// entry left after removeConnection is not served by the fast path, so
// getStream dials again and, the fresh connection being live, keeps the
// existing entry rather than its own duplicate.
func TestGetStream_UnownedEntryAfterRemoveConnectionReturnsExisting(t *testing.T) {
	t1, t2, _, _, cleanup := transportPair(t, 42)
	defer cleanup()
	target := t2.Addr().String()

	s, err := t1.getStream(target, 1, false, "")
	if err != nil {
		t.Fatalf("getStream: %v", err)
	}
	unownStream(t1, target, 1)
	t1.connMu.RLock()
	oldConn := t1.conns[target]
	t1.connMu.RUnlock()
	t1.removeConnection(target)

	got, err := t1.getStream(target, 1, false, "")
	if err != nil {
		t.Fatalf("getStream after removeConnection: %v", err)
	}
	if got != s {
		t.Fatal("getStream did not return the existing pooled entry")
	}
	t1.connMu.RLock()
	newConn, ok := t1.conns[target]
	t1.connMu.RUnlock()
	if !ok || newConn == oldConn {
		t.Fatal("getStream did not dial a fresh connection")
	}
}

// TestHandleMessageFrame_CompressedDeltaRefusedByAggregateBudget: with no
// connection (so no per-peer sub-budget), a compressed frame whose
// compressed-length charge fits the aggregate budget but whose decompression
// delta does not is refused, leaving the budget exactly where it was.
func TestHandleMessageFrame_CompressedDeltaRefusedByAggregateBudget(t *testing.T) {
	tr, err := NewQUICTransport(Config{
		ListenAddress: ":0",
		DeploymentID:  42,
		MTLSConfig:    testMTLSConfig(t),
	}, newTestHandler(), registry.NewRegistry())
	if err != nil {
		t.Fatalf("NewQUICTransport: %v", err)
	}
	compressed := snappyEncode(nil, make([]byte, 64*1024))
	budget := proto.DecodeCharge(int64(len(compressed))) + 1
	tr.msgRecvBudget.Store(budget)

	err = tr.handleMessageFrame(nil, bytes.NewReader(compressed), uint32(len(compressed)), FlagCompressed)
	if !errors.Is(err, ErrMessageRecvMemoryBudgetExceeded) {
		t.Fatalf("got %v, want ErrMessageRecvMemoryBudgetExceeded", err)
	}
	if got := tr.msgRecvBudget.Load(); got != budget {
		t.Fatalf("budget after refusal = %d, want %d", got, budget)
	}
	if got := tr.recvBudgetDrops.Load(); got != 1 {
		t.Fatalf("recvBudgetDrops = %d, want 1", got)
	}
}

// TestSnapshotReceiver_PeerAccountingEdges covers the per-peer accounting
// boundaries: the per-peer slot floor, a disabled memory budget, releases for
// peers holding nothing, and refusals that must not leave a map entry behind.
func TestSnapshotReceiver_PeerAccountingEdges(t *testing.T) {
	if sr := newSnapshotReceiver(0, 0, 0, 42, nil); sr.maxPerPeer != 1 {
		t.Fatalf("maxPerPeer with maxConcurrent=0 = %d, want 1", sr.maxPerPeer)
	}
	if got := perPeerSnapshotMemory(0, 4, 2); got != 0 {
		t.Fatalf("perPeerSnapshotMemory(0, ...) = %d, want 0", got)
	}

	sr := newSnapshotReceiver(4, 1<<30, 0, 42, nil)
	if sr.maxPerPeerMemory <= 0 {
		t.Fatalf("test setup: maxPerPeerMemory = %d, want > 0", sr.maxPerPeerMemory)
	}
	sr.releasePeerMemory("", 10)
	sr.releasePeerMemory("unknown", 10)
	sr.releasePeerSlot("")
	sr.releasePeerSlot("unknown")
	if n := len(sr.peerUse); n != 0 {
		t.Fatalf("releases for absent peers created %d entries", n)
	}

	if sr.reservePeerMemory("p", sr.maxPerPeerMemory+1) {
		t.Fatal("reservation over the per-peer cap accepted")
	}
	if _, ok := sr.peerUse["p"]; ok {
		t.Fatal("refused reservation left a peerUse entry")
	}

	sr.maxPerPeer = 0
	if sr.acquirePeerSlot("p") {
		t.Fatal("slot granted with maxPerPeer = 0")
	}
	if _, ok := sr.peerUse["p"]; ok {
		t.Fatal("refused slot left a peerUse entry")
	}
}

// TestPeerMsgBudgetRef_NilConnIsNoOp: a nil connection holds no per-peer
// message budget, so acquiring or releasing a reference for it records nothing.
func TestPeerMsgBudgetRef_NilConnIsNoOp(t *testing.T) {
	tr, err := NewQUICTransport(Config{
		ListenAddress: ":0",
		DeploymentID:  42,
		MTLSConfig:    testMTLSConfig(t),
	}, newTestHandler(), registry.NewRegistry())
	if err != nil {
		t.Fatalf("NewQUICTransport: %v", err)
	}
	if tr.maxConnMsgRecv <= 0 {
		t.Fatalf("test setup: maxConnMsgRecv = %d, want > 0", tr.maxConnMsgRecv)
	}
	tr.acquirePeerMsgBudgetRef(nil)
	tr.releasePeerMsgBudgetRef(nil)
	entries := 0
	tr.connMsgBudget.Range(func(any, any) bool { entries++; return true })
	if entries != 0 {
		t.Fatalf("nil-conn budget refs created %d entries", entries)
	}
}

// stageStaleEntry opens a pooled stream for shardID, then drops target's
// cached connection so the next getStream takes the slow path. It returns the
// stream and its now-superseded owning connection.
func stageStaleEntry(t *testing.T, tr *QUICTransport, target string, shardID uint64) (*quic.Stream, *quic.Conn) {
	t.Helper()
	s, err := tr.getStream(target, shardID, false, "")
	if err != nil {
		t.Fatalf("getStream: %v", err)
	}
	tr.connMu.RLock()
	owner := tr.conns[target]
	tr.connMu.RUnlock()
	tr.removeConnection(target)
	return s, owner
}

// reinsert puts stream back as target's pooled entry for shardID, owned by
// owner, the shape a concurrent pre-open on a superseded connection leaves.
func reinsert(tr *QUICTransport, target string, shardID uint64, stream *quic.Stream, owner *quic.Conn) {
	idx := tr.streamIndexFor(shardID, false)
	shard := &tr.streamShards[idx&(streamShardCount-1)]
	shard.mu.Lock()
	shard.put(streamKey{target: target, idx: idx}, stream, owner)
	shard.mu.Unlock()
}

// pooledEntry returns target's pooled stream for shardID and its owner.
func pooledEntry(tr *QUICTransport, target string, shardID uint64) (*quic.Stream, *quic.Conn) {
	idx := tr.streamIndexFor(shardID, false)
	shard := &tr.streamShards[idx&(streamShardCount-1)]
	key := streamKey{target: target, idx: idx}
	shard.mu.RLock()
	defer shard.mu.RUnlock()
	return shard.streams[key], shard.owners[key]
}

// TestGetStream_StaleEntryReplacedOnLiveConnection: a stale entry owned by a
// superseded connection appears while getStream opens on the live cached
// connection. The fresh stream replaces it in place and the connection stays.
func TestGetStream_StaleEntryReplacedOnLiveConnection(t *testing.T) {
	t1, t2, _, _, cleanup := transportPair(t, 42)
	defer cleanup()
	target := t2.Addr().String()

	stale, oldConn := stageStaleEntry(t, t1, target, 1)
	t1.beforeStreamInsert = func() { reinsert(t1, target, 1, stale, oldConn) }

	got, err := t1.getStream(target, 1, false, "")
	t1.beforeStreamInsert = nil
	if err != nil {
		t.Fatalf("getStream: %v", err)
	}
	if got == stale {
		t.Fatal("getStream returned the stale entry")
	}
	t1.connMu.RLock()
	cached, ok := t1.conns[target]
	t1.connMu.RUnlock()
	if !ok || cached == oldConn {
		t.Fatal("expected a fresh cached connection")
	}
	stream, owner := pooledEntry(t1, target, 1)
	if stream != got || owner != cached {
		t.Fatal("pooled entry is not the fresh stream on the cached connection")
	}
}

// TestGetStream_StaleEntryAndDroppedConnectionRetries: the stale entry
// appears and the connection getStream opened on is dropped too. Both streams
// are discarded and one retry dials afresh and pools its stream.
func TestGetStream_StaleEntryAndDroppedConnectionRetries(t *testing.T) {
	t1, t2, _, _, cleanup := transportPair(t, 42)
	defer cleanup()
	target := t2.Addr().String()

	stale, oldConn := stageStaleEntry(t, t1, target, 1)
	var openedOn *quic.Conn
	t1.beforeStreamInsert = func() {
		t1.connMu.RLock()
		openedOn = t1.conns[target]
		t1.connMu.RUnlock()
		reinsert(t1, target, 1, stale, oldConn)
		t1.removeConnection(target)
	}

	got, err := t1.getStream(target, 1, false, "")
	t1.beforeStreamInsert = nil
	if err != nil {
		t.Fatalf("getStream: %v", err)
	}
	if got == stale {
		t.Fatal("getStream returned the stale entry")
	}
	t1.connMu.RLock()
	cached, ok := t1.conns[target]
	t1.connMu.RUnlock()
	if !ok || cached == oldConn || cached == openedOn {
		t.Fatal("retry did not dial a fresh connection")
	}
	stream, owner := pooledEntry(t1, target, 1)
	if stream != got || owner != cached {
		t.Fatal("pooled entry is not the retry stream on the retry connection")
	}
}
