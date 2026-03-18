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
	"time"

	"github.com/jeremyhahn/go-quicraft/pkg/proto"
)

// heartbeatBatchPrefixSize is the size of the pre-serialized MessageBatch
// header for heartbeat frames: BinVer(8) + DeploymentID(8) + SourceAddrLen(4)
// + SourceAddr(N). The RequestsCount(4) is appended per-send since the
// number of messages varies.
//
// The full prefix size is computed at runtime from the local address length.
const heartbeatBatchFixedPrefix = 8 + 8 + 4 // 20 bytes (before address data)

// heartbeatWriteDeadline is the write deadline for heartbeat stream writes.
// Shorter than the 5s general deadline since heartbeats are tiny.
const heartbeatWriteDeadline = 2 * time.Second

// initHeartbeatPrefix pre-serializes the MessageBatch header fields that
// are identical for every heartbeat frame: BinVer, DeploymentID, and
// SourceAddress. Called once from Start() after the local address is known.
//
// Wire layout of the prefix:
//
//	[BinVer:8][DeploymentID:8][SourceAddrLen:4][SourceAddr:N]
func (t *QUICTransport) initHeartbeatPrefix() {
	addr := t.localAddr()
	prefixLen := heartbeatBatchFixedPrefix + len(addr)
	t.hbPrefix = make([]byte, prefixLen)

	binary.LittleEndian.PutUint64(t.hbPrefix[0:], proto.WireVersion)
	binary.LittleEndian.PutUint64(t.hbPrefix[8:], t.cfg.DeploymentID)
	binary.LittleEndian.PutUint32(t.hbPrefix[16:], uint32(len(addr)))
	copy(t.hbPrefix[20:], addr)
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
func (t *QUICTransport) sendHeartbeatBatch(target string, msgs []proto.Message) {
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
			t.putBuf(bufPtr)
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

	stream, err := t.getStream(target, shardID, true)
	if err != nil {
		t.putBuf(bufPtr)
		return
	}

	if err := stream.SetWriteDeadline(time.Now().Add(heartbeatWriteDeadline)); err != nil {
		t.putBuf(bufPtr)
		t.removeAndCloseConnection(target, "heartbeat write deadline failure")
		t.evictStreamsForTarget(target)
		return
	}

	_, err = stream.Write(buf[:frameSize])
	t.putBuf(bufPtr)
	if err != nil {
		t.removeAndCloseConnection(target, "heartbeat stream write failure")
		t.evictStreamsForTarget(target)
	}
}

// isHeartbeatBatch returns true if all messages in the batch are heartbeat
// or heartbeat response types. This is used by Send() to route batches to
// the heartbeat fast path.
func isHeartbeatBatch(msgs []proto.Message) bool {
	for i := range msgs {
		if !isHeartbeatMessage(msgs[i].Type) {
			return false
		}
	}
	return true
}
