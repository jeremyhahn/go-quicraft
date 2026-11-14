package transport

import (
	"context"
	"errors"
	"testing"

	"github.com/jeremyhahn/go-quicraft/pkg/proto"
)

const frameAdmissionAddr = "127.0.0.1:5000"

// frameAdmissionHeartbeats returns n Heartbeat messages for shards 1..n.
func frameAdmissionHeartbeats(ctx context.Context, t *testing.T, n int) []proto.Message {
	t.Helper()
	if err := ctx.Err(); err != nil {
		t.Fatalf("context: %v", err)
	}
	msgs := make([]proto.Message, n)
	for i := range msgs {
		msgs[i] = proto.Message{Type: proto.Heartbeat, ShardID: uint64(i + 1), From: 1, To: 2, Term: 1}
	}
	return msgs
}

// frameAdmissionReplicate returns a Replicate message carrying entries
// entries, each with a cmdLen-byte Cmd.
func frameAdmissionReplicate(ctx context.Context, t *testing.T, entries, cmdLen int) proto.Message {
	t.Helper()
	if err := ctx.Err(); err != nil {
		t.Fatalf("context: %v", err)
	}
	es := make([]proto.Entry, entries)
	var cmd []byte
	if cmdLen > 0 {
		cmd = make([]byte, cmdLen)
	}
	for i := range es {
		es[i] = proto.Entry{Index: uint64(i + 1), Term: 1, Cmd: cmd}
	}
	return proto.Message{Type: proto.Replicate, ShardID: 1, From: 1, To: 2, Term: 1, Entries: es}
}

func TestFrameAdmissionEntryCapNamesTooManyMessages(t *testing.T) {
	ctx := context.Background()
	h := batchHeaderSize(frameAdmissionAddr)

	m := frameAdmissionReplicate(ctx, t, proto.MaxEntriesPerMessage, 0)
	if err := messageFrameError(&m, h, 10); err != nil {
		t.Fatalf("messageFrameError at MaxEntriesPerMessage = %v, want nil", err)
	}
	m = frameAdmissionReplicate(ctx, t, proto.MaxEntriesPerMessage+1, 0)
	if err := messageFrameError(&m, h, 10); !errors.Is(err, proto.ErrTooManyMessages) {
		t.Fatalf("messageFrameError past MaxEntriesPerMessage = %v, want ErrTooManyMessages", err)
	}
}

func TestFrameAdmissionFrameSizeNamesPayloadTooLarge(t *testing.T) {
	ctx := context.Background()
	h := batchHeaderSize(frameAdmissionAddr)
	m := frameAdmissionHeartbeats(ctx, t, 1)[0]
	fit := int(MaxFrameSize) - h

	if err := messageFrameError(&m, h, fit); err != nil {
		t.Fatalf("messageFrameError at MaxFrameSize = %v, want nil", err)
	}
	if !messageFrameable(&m, h, fit) {
		t.Fatal("messageFrameable at MaxFrameSize = false, want true")
	}
	if err := messageFrameError(&m, h, fit+1); !errors.Is(err, ErrPayloadTooLarge) {
		t.Fatalf("messageFrameError past MaxFrameSize = %v, want ErrPayloadTooLarge", err)
	}
	if messageFrameable(&m, h, fit+1) {
		t.Fatal("messageFrameable past MaxFrameSize = true, want false")
	}
}

func TestFrameAdmissionUnframeableFirstMessageFails(t *testing.T) {
	ctx := context.Background()
	h := batchHeaderSize(frameAdmissionAddr)
	msgs := []proto.Message{frameAdmissionReplicate(ctx, t, proto.MaxEntriesPerMessage+1, 0)}

	count, payload, err := sendableGroup(h, msgs)
	if count != 0 || payload != 0 || !errors.Is(err, proto.ErrTooManyMessages) {
		t.Fatalf("sendableGroup = (%d, %d, %v), want (0, 0, ErrTooManyMessages)", count, payload, err)
	}
}

func TestFrameAdmissionGroupsWholeBatchWithBatchSize(t *testing.T) {
	ctx := context.Background()
	h := batchHeaderSize(frameAdmissionAddr)
	msgs := frameAdmissionHeartbeats(ctx, t, 3)

	count, payload, err := sendableGroup(h, msgs)
	if err != nil {
		t.Fatalf("sendableGroup error = %v, want nil", err)
	}
	if count != 3 {
		t.Fatalf("count = %d, want 3", count)
	}
	want := (&proto.MessageBatch{SourceAddress: frameAdmissionAddr, Requests: msgs}).Size()
	if payload != want {
		t.Fatalf("payload = %d, want MessageBatch.Size() %d", payload, want)
	}
}

func TestFrameAdmissionStopsBeforeUnframeableMessage(t *testing.T) {
	ctx := context.Background()
	h := batchHeaderSize(frameAdmissionAddr)
	hb := frameAdmissionHeartbeats(ctx, t, 2)
	msgs := []proto.Message{hb[0], frameAdmissionReplicate(ctx, t, proto.MaxEntriesPerMessage+1, 0), hb[1]}

	count, _, err := sendableGroup(h, msgs)
	if err != nil || count != 1 {
		t.Fatalf("sendableGroup = (%d, %v), want (1, nil)", count, err)
	}
}

func TestFrameAdmissionStopsAtBatchCapAndFrameSize(t *testing.T) {
	ctx := context.Background()
	h := batchHeaderSize(frameAdmissionAddr)

	msgs := frameAdmissionHeartbeats(ctx, t, proto.MaxMessagesPerBatch+1)
	count, _, err := sendableGroup(h, msgs)
	if err != nil || count != proto.MaxMessagesPerBatch {
		t.Fatalf("sendableGroup over batch cap = (%d, %v), want (%d, nil)", count, err, proto.MaxMessagesPerBatch)
	}

	const nineMiB = 9 << 20
	big := []proto.Message{
		frameAdmissionReplicate(ctx, t, 1, nineMiB),
		frameAdmissionReplicate(ctx, t, 1, nineMiB),
	}
	count, _, err = sendableGroup(h, big)
	if err != nil || count != 1 {
		t.Fatalf("sendableGroup over frame size = (%d, %v), want (1, nil)", count, err)
	}
	count, _, err = sendableGroup(h, big[1:])
	if err != nil || count != 1 {
		t.Fatalf("sendableGroup on the remainder = (%d, %v), want (1, nil)", count, err)
	}
}
