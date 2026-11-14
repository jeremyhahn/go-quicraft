package transport

import (
	"bytes"
	"context"
	"errors"
	"net"
	"testing"
	"time"

	"github.com/jeremyhahn/go-quicraft/pkg/config"
	"github.com/jeremyhahn/go-quicraft/pkg/internal/registry"
	"github.com/jeremyhahn/go-quicraft/pkg/proto"
)

const (
	chunkWriteErrorShardID    = 7
	chunkWriteErrorReplicaID  = 3
	chunkWriteErrorChunkCount = 128
	chunkWriteErrorAttempts   = 5
	chunkWriteErrorBound      = 15 * time.Second
)

// chunkWriteErrorSender is a sender transport whose OnSnapshotSendStarted
// stops the current receiver, so every send loses its receiver after the
// header is written and before the first chunk goes out.
type chunkWriteErrorSender struct {
	tr       *QUICTransport
	reg      *registry.Registry
	mtls     *MTLSConfig
	receiver chan *QUICTransport
	aborted  chan config.SnapshotInfo
}

// newChunkWriteErrorSender starts the sending transport.
func newChunkWriteErrorSender(ctx context.Context, t *testing.T) *chunkWriteErrorSender {
	t.Helper()
	s := &chunkWriteErrorSender{
		reg:      registry.NewRegistry(),
		mtls:     testMTLSConfig(t),
		receiver: make(chan *QUICTransport, 1),
		aborted:  make(chan config.SnapshotInfo, chunkWriteErrorAttempts),
	}
	tr, err := NewQUICTransport(Config{
		ListenAddress: ":0",
		DeploymentID:  42,
		MTLSConfig:    s.mtls,
		EventListener: &config.EventListener{
			OnSnapshotSendStarted: func(config.SnapshotInfo) {
				select {
				case rcv := <-s.receiver:
					stopWithin(ctx, t, rcv, "receiver")
				default:
				}
			},
			OnSnapshotSendAborted: func(info config.SnapshotInfo) {
				select {
				case s.aborted <- info:
				default:
				}
			},
		},
	}, newTestHandler(), s.reg)
	if err != nil {
		t.Fatalf("NewQUICTransport(sender) failed: %v", err)
	}
	if err := tr.Start(); err != nil {
		t.Fatalf("sender Start failed: %v", err)
	}
	s.tr = tr
	return s
}

// armReceiver starts a fresh receiver, registers it as the target replica
// and hands it to OnSnapshotSendStarted to stop mid-transfer.
func (s *chunkWriteErrorSender) armReceiver(_ context.Context, t *testing.T) {
	t.Helper()
	rcv, err := NewQUICTransport(Config{
		ListenAddress: ":0",
		DeploymentID:  42,
		MTLSConfig:    s.mtls,
	}, newTestHandler(), registry.NewRegistry())
	if err != nil {
		t.Fatalf("NewQUICTransport(receiver) failed: %v", err)
	}
	if err := rcv.Start(); err != nil {
		t.Fatalf("receiver Start failed: %v", err)
	}
	t.Cleanup(func() { _ = rcv.Stop() })
	s.reg.Register(chunkWriteErrorShardID, chunkWriteErrorReplicaID, rcv.Addr().String())
	select {
	case <-s.receiver:
	default:
	}
	s.receiver <- rcv
}

// stopWithin stops tr and fails the test if Stop does not return within
// the bound or before ctx ends.
func stopWithin(ctx context.Context, t *testing.T, tr *QUICTransport, name string) {
	t.Helper()
	done := make(chan struct{})
	go func() {
		_ = tr.Stop()
		close(done)
	}()
	bound, cancel := context.WithTimeout(ctx, chunkWriteErrorBound)
	defer cancel()
	select {
	case <-done:
	case <-bound.Done():
		t.Errorf("%s Stop did not return within %v", name, chunkWriteErrorBound)
	}
}

// sendUntilChunkWriteErrorCause retries send, each time against a fresh
// receiver that is stopped mid-transfer, until a send fails. It fails the
// test when no attempt fails within the bounded number of attempts.
func sendUntilChunkWriteErrorCause(ctx context.Context, t *testing.T, s *chunkWriteErrorSender, send func() error) error {
	t.Helper()
	for attempt := 0; attempt < chunkWriteErrorAttempts; attempt++ {
		if ctx.Err() != nil {
			t.Fatalf("context ended before a send failed: %v", ctx.Err())
		}
		s.armReceiver(ctx, t)
		if err := send(); err != nil {
			return err
		}
	}
	t.Fatalf("no send failed in %d attempts with the receiver stopped mid-transfer", chunkWriteErrorAttempts)
	return nil
}

// checkChunkWriteErrorCause asserts the error a failed send returned, the
// abort event and the sender's Stop.
func checkChunkWriteErrorCause(ctx context.Context, t *testing.T, s *chunkWriteErrorSender, sendErr error) {
	t.Helper()
	var writeErr *SnapshotChunkWriteTimeoutError
	if !errors.As(sendErr, &writeErr) {
		t.Fatalf("send error = %v (%T), want *SnapshotChunkWriteTimeoutError", sendErr, sendErr)
	}
	if !errors.Is(sendErr, ErrSnapshotChunkWriteTimeout) {
		t.Errorf("errors.Is(%v, ErrSnapshotChunkWriteTimeout) = false, want true", sendErr)
	}
	if writeErr.ShardID != chunkWriteErrorShardID || writeErr.ReplicaID != chunkWriteErrorReplicaID {
		t.Errorf("chunk write error names shard %d replica %d, want %d and %d",
			writeErr.ShardID, writeErr.ReplicaID, chunkWriteErrorShardID, chunkWriteErrorReplicaID)
	}
	if writeErr.ChunkID >= chunkWriteErrorChunkCount {
		t.Errorf("chunk write error names chunk %d, want below the chunk count %d",
			writeErr.ChunkID, chunkWriteErrorChunkCount)
	}
	cause := errors.Unwrap(writeErr)
	if cause == nil {
		t.Fatal("errors.Unwrap(chunk write error) = nil, want the stream error")
	}
	if cause != writeErr.Err {
		t.Errorf("errors.Unwrap = %v, want the Err field %v", cause, writeErr.Err)
	}
	// The receiver went away; no deadline was reached. The error type is
	// named for the deadline but carries any chunk write failure.
	var netErr net.Error
	if errors.As(cause, &netErr) && netErr.Timeout() {
		t.Errorf("stream error %v is a timeout, want the closed-connection failure", cause)
	}

	bound, cancel := context.WithTimeout(ctx, chunkWriteErrorBound)
	defer cancel()
	select {
	case info := <-s.aborted:
		if info.ShardID != chunkWriteErrorShardID || info.ReplicaID != chunkWriteErrorReplicaID {
			t.Errorf("OnSnapshotSendAborted shard %d replica %d, want %d and %d",
				info.ShardID, info.ReplicaID, chunkWriteErrorShardID, chunkWriteErrorReplicaID)
		}
	case <-bound.Done():
		t.Errorf("OnSnapshotSendAborted did not fire within %v", chunkWriteErrorBound)
	}

	stopWithin(ctx, t, s.tr, "sender")
}

// chunkWriteErrorChunk is chunk i of the test snapshot.
func chunkWriteErrorChunk(i uint64, data []byte) proto.SnapshotChunk {
	return proto.SnapshotChunk{
		BinVer:     proto.WireVersion,
		ShardID:    chunkWriteErrorShardID,
		ReplicaID:  chunkWriteErrorReplicaID,
		Index:      10,
		Term:       5,
		ChunkID:    i,
		ChunkCount: chunkWriteErrorChunkCount,
		Data:       data,
	}
}

// chunkWriteErrorData is large enough that the chunks exceed the stream's
// flow-control window, so a write blocks until the receiver reads and
// fails once the receiver has gone.
func chunkWriteErrorData() []byte {
	return bytes.Repeat([]byte("chunk-write-error-cause-"), 2048)
}

func TestSendSnapshot_ChunkWriteErrorCause(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	s := newChunkWriteErrorSender(ctx, t)
	t.Cleanup(func() { _ = s.tr.Stop() })

	data := chunkWriteErrorData()
	chunks := make([]proto.SnapshotChunk, chunkWriteErrorChunkCount)
	for i := range chunks {
		chunks[i] = chunkWriteErrorChunk(uint64(i), data)
	}
	sendErr := sendUntilChunkWriteErrorCause(ctx, t, s, func() error {
		return s.tr.SendSnapshot(chunks)
	})
	checkChunkWriteErrorCause(ctx, t, s, sendErr)
}

func TestSendSnapshotStreaming_ChunkWriteErrorCause(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	s := newChunkWriteErrorSender(ctx, t)
	t.Cleanup(func() { _ = s.tr.Stop() })

	data := chunkWriteErrorData()
	meta := SnapshotHeader{
		ShardID:    chunkWriteErrorShardID,
		ReplicaID:  chunkWriteErrorReplicaID,
		Index:      10,
		Term:       5,
		ChunkCount: chunkWriteErrorChunkCount,
	}
	sendErr := sendUntilChunkWriteErrorCause(ctx, t, s, func() error {
		return s.tr.SendSnapshotStreaming(meta, func(emit func(proto.SnapshotChunk) error) error {
			for i := uint64(0); i < chunkWriteErrorChunkCount; i++ {
				if err := emit(chunkWriteErrorChunk(i, data)); err != nil {
					return err
				}
			}
			return nil
		})
	})
	checkChunkWriteErrorCause(ctx, t, s, sendErr)
}
