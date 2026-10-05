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
	"encoding/binary"
	"log/slog"
	"time"

	"github.com/jeremyhahn/go-quicraft/pkg/proto"
)

// heartbeatBatchPrefixSize is the size of the pre-serialized MessageBatch
// header for heartbeat frames: BinVer(8) + DeploymentID(8) + SourceAddrLen(4)
// + SourceAddr(N). The RequestsCount(4) is appended per-send since the
// number of messages varies.
//
// The full prefix size is computed at runtime from the local address length.
const heartbeatBatchFixedPrefix = 8 + 8 + 8 + 4 // 28 bytes: BinVer+DeploymentID+SourceEpoch+AddrLen

// heartbeatWriteDeadline is the write deadline for heartbeat stream writes.
// Shorter than the 5s general deadline since heartbeats are tiny.
const heartbeatWriteDeadline = 2 * time.Second

// initHeartbeatPrefix pre-serializes the MessageBatch header fields that
// are identical for every heartbeat frame: BinVer, DeploymentID, and
// SourceAddress. Called once from Start() after the local address is known.
//
// Wire layout of the prefix (must match proto.MessageBatch's fixed region):
//
//	[BinVer:8][DeploymentID:8][SourceEpoch:8][SourceAddrLen:4][SourceAddr:N]
func (t *QUICTransport) initHeartbeatPrefix() {
	addr := t.localAddr()
	prefixLen := heartbeatBatchFixedPrefix + len(addr)
	t.hbPrefix = make([]byte, prefixLen)

	binary.LittleEndian.PutUint64(t.hbPrefix[0:], proto.WireVersion)
	binary.LittleEndian.PutUint64(t.hbPrefix[8:], t.cfg.DeploymentID)
	binary.LittleEndian.PutUint64(t.hbPrefix[16:], t.bootEpoch)
	binary.LittleEndian.PutUint32(t.hbPrefix[24:], uint32(len(addr)))
	copy(t.hbPrefix[28:], addr)
}

// sendHeartbeatBatch is the fast path for heartbeat-only message batches.
// It avoids the general sendBatch overhead by:
//
//  1. Using the pre-serialized MessageBatch header prefix (hbPrefix)
//  2. Computing the frame size from the known constant HeartbeatMessageSize
//  3. Marshaling messages directly into the frame buffer
//  4. Skipping compression (heartbeats are too small to benefit)
//
// The wire format is fully compatible with the standard receive path:
// the receiver sees a normal FlagNone frame containing a MessageBatch.
func (t *QUICTransport) sendHeartbeatBatch(target, want string, msgs []proto.Message) {
	msgCount := len(msgs)

	// Payload: prefix + requestsCount(4) + N * HeartbeatMessageSize
	payloadSize := len(t.hbPrefix) + 4 + msgCount*proto.HeartbeatMessageSize
	frameSize := FrameHeaderSize + payloadSize

	// Get a buffer for the entire frame.
	bufPtr := t.getBuf(frameSize)
	buf := (*bufPtr)[:frameSize]

	// Write frame header inline.
	buf[0] = MagicByte0
	buf[1] = MagicByte1
	buf[2] = WireVersion
	buf[3] = byte(FlagNone)
	binary.BigEndian.PutUint32(buf[4:8], uint32(payloadSize))

	// Copy the pre-serialized batch header prefix.
	offset := FrameHeaderSize
	copy(buf[offset:], t.hbPrefix)
	offset += len(t.hbPrefix)

	// Write requests count.
	binary.LittleEndian.PutUint32(buf[offset:], uint32(msgCount))
	offset += 4

	// Marshal each heartbeat message directly into the buffer.
	for i := range msgs {
		n, err := msgs[i].MarshalTo(buf[offset:])
		if err != nil {
			// The buffer is sized for fixed HeartbeatMessageSize records, so a
			// marshal failure means a non-fixed-size message slipped past
			// isHeartbeatBatch — an internal routing bug. Surface it loudly:
			// silently dropping the whole batch would stall liveness.
			t.putBuf(bufPtr)
			slog.Warn("transport: dropping heartbeat batch: message failed fixed-size marshal",
				"target", target,
				"batch_size", msgCount,
				"msg_type", proto.MessageTypeName(msgs[i].Type),
				"error", err,
			)
			t.incBatchDropped(msgs, dropReasonMarshalFailed)
			return
		}
		offset += n
	}

	// Use shardID from the first message for stream selection.
	// All heartbeats share stream index 0 regardless of shardID.
	var shardID uint64
	if msgCount > 0 {
		shardID = msgs[0].ShardID
	}

	// Every failure branch below settles first and runs user code (drop
	// metrics, diagnostics) last; see sendFrame.
	stream, err := t.getStream(target, shardID, true, want)
	if err != nil {
		t.putBuf(bufPtr)
		// Debug level: repeats every heartbeat tick while a peer is
		// unreachable; the next tick retries. Gated so the variadic slog form
		// does not build and box its []any on every tick when Debug is off.
		if debugEnabled() {
			slog.Debug("transport: dropping heartbeat batch: stream unavailable",
				"target", target,
				"batch_size", msgCount,
				"error", err,
			)
		}
		t.incBatchDropped(msgs, streamDropReason(err))
		return
	}

	if err := stream.SetWriteDeadline(time.Now().Add(heartbeatWriteDeadline)); err != nil {
		t.putBuf(bufPtr)
		t.removeAndCloseConnection(target, "heartbeat write deadline failure")
		t.evictStreamsForTarget(target)
		t.incBatchDropped(msgs, dropReasonWriteFailed)
		return
	}

	_, err = stream.Write(buf[:frameSize])
	t.putBuf(bufPtr)
	if err != nil {
		// The peer refused a frame on the heartbeat stream (see sendFrame for
		// why this is not a connection failure). Replace the stream only: a
		// peer that rejects every frame — a deployment or wire-version
		// mismatch — would otherwise turn each heartbeat tick into a
		// connection teardown and re-dial.
		if isPeerFrameRejection(err) {
			t.evictStream(target, shardID, true, stream)
			t.incBatchDropped(msgs, dropReasonPeerRejected)
			t.recordPeerFrameRejection(target, shardID, msgCount, err)
			return
		}
		t.removeAndCloseConnection(target, "heartbeat stream write failure")
		t.evictStreamsForTarget(target)
		// Debug level: a dead peer fails every heartbeat until eviction
		// completes; the next tick retries on a fresh connection.
		if debugEnabled() {
			slog.Debug("transport: dropping heartbeat batch: stream write failed",
				"target", target,
				"batch_size", msgCount,
				"error", err,
			)
		}
		t.incBatchDropped(msgs, dropReasonWriteFailed)
	}
}

// isHeartbeatBatch returns true if all messages in the batch are heartbeat
// or heartbeat response types carrying neither entries nor a snapshot. This is
// used by Send() to route batches to the heartbeat fast path. The fast path
// marshals each message into a fixed HeartbeatMessageSize slot, so a heartbeat
// carrying ANY variable-length payload must fall through to the general
// sendBatch path — the fixed-size buffer would otherwise overflow and the
// whole batch would be dropped, which is a liveness event.
func isHeartbeatBatch(msgs []proto.Message) bool {
	for i := range msgs {
		if !isHeartbeatMessage(msgs[i].Type) ||
			len(msgs[i].Entries) > 0 ||
			!msgs[i].Snapshot.IsEmpty() {
			return false
		}
	}
	return true
}
