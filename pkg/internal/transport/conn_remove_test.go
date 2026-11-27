package transport

import (
	"testing"
	"time"

	"github.com/quic-go/quic-go"
)

// TestRemoveConnectionKeepsReplacement pins the compare-and-delete contract of
// removeConnection and evictStreamsOwnedBy: a caller that observed a dead
// connection C1 and acts on it after a concurrent sender already re-dialed
// must neither unpool the live replacement C2 nor close C2's streams.
func TestRemoveConnectionKeepsReplacement(t *testing.T) {
	t1, t2, _, _, cleanup := transportPair(t, 42)
	defer cleanup()

	target := t2.Addr().String()

	// C1: dial, take it out of the pool as a concurrent sender would, and
	// close it.
	c1, err := t1.getConnection(target, "")
	if err != nil {
		t.Fatalf("getConnection (C1) failed: %v", err)
	}
	t1.connMu.Lock()
	if t1.conns[target] == c1 {
		delete(t1.connPeer, c1)
		delete(t1.conns, target)
		delete(t1.connEpoch, target)
	}
	t1.connMu.Unlock()
	t1.evictStreamsForTarget(target)
	c1.CloseWithError(0, "test: dead connection C1")
	select {
	case <-c1.Context().Done():
	case <-time.After(5 * time.Second):
		t.Fatal("C1 context not done after close")
	}

	// C2: the live replacement, with a pooled data stream on it.
	if _, err := t1.getStream(target, 1, false, ""); err != nil {
		t.Fatalf("getStream on C2 failed: %v", err)
	}
	t1.connMu.RLock()
	c2, ok := t1.conns[target]
	peerBefore, peerOK := t1.connPeer[c2]
	epochBefore, epochOK := t1.connEpoch[target]
	t1.connMu.RUnlock()
	if !ok || c2 == c1 {
		t.Fatalf("expected a pooled replacement distinct from C1 (ok=%v)", ok)
	}

	type pooled struct {
		shard  int
		key    streamKey
		stream *quic.Stream
	}
	var onC2 []pooled
	for i := range t1.streamShards {
		shard := &t1.streamShards[i]
		shard.mu.Lock()
		for key, s := range shard.streams {
			if key.target == target && shard.owners[key] == c2 {
				onC2 = append(onC2, pooled{shard: i, key: key, stream: s})
			}
		}
		shard.mu.Unlock()
	}
	if len(onC2) == 0 {
		t.Fatal("expected at least one stream pooled on C2")
	}

	// The late caller acts on the C1 it observed.
	t1.removeConnection(target, c1)
	t1.evictStreamsOwnedBy(target, c1)

	t1.connMu.RLock()
	got, gotOK := t1.conns[target]
	peerAfter, peerAfterOK := t1.connPeer[c2]
	epochAfter, epochAfterOK := t1.connEpoch[target]
	t1.connMu.RUnlock()
	if !gotOK || got != c2 {
		t.Fatalf("conns[target] = %p (ok=%v), want C2 %p", got, gotOK, c2)
	}
	if peerAfterOK != peerOK || peerAfter != peerBefore {
		t.Fatalf("connPeer[C2] = %q (ok=%v), want %q (ok=%v)", peerAfter, peerAfterOK, peerBefore, peerOK)
	}
	if epochAfterOK != epochOK || epochAfter != epochBefore {
		t.Fatalf("connEpoch[target] = %d (ok=%v), want %d (ok=%v)", epochAfter, epochAfterOK, epochBefore, epochOK)
	}
	if c2.Context().Err() != nil {
		t.Fatal("C2 closed by removal of C1")
	}
	for _, p := range onC2 {
		shard := &t1.streamShards[p.shard]
		shard.mu.Lock()
		s, still := shard.streams[p.key]
		owner := shard.owners[p.key]
		shard.mu.Unlock()
		if !still || s != p.stream || owner != c2 {
			t.Fatalf("stream %+v on C2 evicted by removal of C1", p.key)
		}
		if p.stream.Context().Err() != nil {
			t.Fatalf("stream %+v on C2 closed by removal of C1", p.key)
		}
	}
}
