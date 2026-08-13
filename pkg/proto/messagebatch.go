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

package proto

// MessageBatch is a batch of Raft messages for transport. One batch per
// network frame. The wire format is:
//
//	[BinVer:8][DeploymentID:8][SourceEpoch:8][SourceAddress:len+data][RequestsCount:4][requests...]
//
// BinVer is the FIRST field so the receiver can validate the wire version
// before parsing the rest of the batch. UnmarshalFrom validates the
// requests count before allocating the slice and tracks cumulative bytes
// to enforce MaxBatchSize.
type MessageBatch struct {
	// BinVer is the wire format version. Receiver validates
	// BinVer == WireVersion before unmarshaling.
	BinVer uint64
	// DeploymentID identifies the deployment for cross-deployment
	// message rejection.
	DeploymentID uint64
	// SourceEpoch is the sender's per-process boot epoch. It changes only
	// when the sending node restarts, letting a receiver detect a genuine
	// peer restart (vs a benign re-dial) and evict the now-stale cached
	// outbound connection promptly instead of waiting for MaxIdleTimeout.
	SourceEpoch uint64
	// SourceAddress is the sender's network address.
	SourceAddress string
	// Requests contains the batched Raft messages.
	Requests []Message
}

// messageBatchFixedSize is the wire size of the three fixed uint64 fields
// (BinVer, DeploymentID, SourceEpoch).
const messageBatchFixedSize = 3 * 8 // 24 bytes

// minMessageWireSize is the smallest possible encoding of a single Message:
// the fixed fields plus the mandatory entries-count and has-snapshot
// prefixes. Identical to HeartbeatMessageSize by construction — a heartbeat
// (no entries, no snapshot) IS the minimal message.
const minMessageWireSize = HeartbeatMessageSize // 100 bytes

// maxRetainedRequests bounds the Requests capacity a batch keeps across
// Reset. The transport pools batches, so without this a single crafted
// oversized batch would pin cap * unsafe.Sizeof(Message) bytes in the pool
// for the process lifetime.
const maxRetainedRequests = 256

// Size returns the total marshaled size of the batch in bytes.
func (mb *MessageBatch) Size() int {
	sz := messageBatchFixedSize + 4 + len(mb.SourceAddress) + 4
	for i := range mb.Requests {
		sz += mb.Requests[i].Size()
	}
	return sz
}

// MarshalTo writes the message batch into buf and returns bytes written.
// buf must be at least Size() bytes. Returns ErrBufferTooSmall if buf
// is too short.
func (mb *MessageBatch) MarshalTo(buf []byte) (int, error) {
	required := mb.Size()
	if len(buf) < required {
		return 0, ErrBufferTooSmall
	}
	putUint64(buf[0:], mb.BinVer)
	putUint64(buf[8:], mb.DeploymentID)
	putUint64(buf[16:], mb.SourceEpoch)
	offset := messageBatchFixedSize
	offset += putString(buf[offset:], mb.SourceAddress)
	putUint32(buf[offset:], uint32(len(mb.Requests)))
	offset += 4
	for i := range mb.Requests {
		n, err := mb.Requests[i].MarshalTo(buf[offset:])
		if err != nil {
			return 0, err
		}
		offset += n
	}
	return offset, nil
}

// UnmarshalFrom reads the message batch from buf and returns bytes
// consumed. Validates BinVer == WireVersion, request count against
// MaxMessagesPerBatch, and cumulative size against MaxBatchSize.
// Returns ErrUnsupportedVersion, ErrTooManyMessages,
// ErrBatchSizeLimitExceeded, or ErrBufferTooSmall as appropriate.
func (mb *MessageBatch) UnmarshalFrom(buf []byte) (int, error) {
	if len(buf) < messageBatchFixedSize {
		return 0, ErrBufferTooSmall
	}
	mb.BinVer = getUint64(buf[0:])
	// Exact-version gate: there is no version negotiation and no legacy
	// decode path — the v1 layout (pre-witness-policy Membership inside
	// snapshot messages) cannot be parsed by this decoder, so batches from
	// both older AND newer binaries are rejected with a clear signal
	// instead of being misparsed. Correct here because MessageBatch is
	// transport-only: a batch lives for one frame and is never persisted,
	// so a rejection costs one connection rather than stranding data at
	// rest (contrast the persisted Membership section, which accepts a
	// range — see the package comment's compatibility contract).
	if mb.BinVer != WireVersion {
		return 0, ErrUnsupportedVersion
	}
	mb.DeploymentID = getUint64(buf[8:])
	mb.SourceEpoch = getUint64(buf[16:])
	offset := messageBatchFixedSize
	addr, n, err := getString(buf[offset:], MaxAddressLength, ErrAddressTooLong)
	if err != nil {
		return 0, err
	}
	mb.SourceAddress = addr
	offset += n
	if len(buf) < offset+4 {
		return 0, ErrBufferTooSmall
	}
	reqCount := getUint32(buf[offset:])
	offset += 4
	if reqCount > MaxMessagesPerBatch {
		return 0, ErrTooManyMessages
	}
	// The count alone drives the allocation below, so it must also be
	// consistent with what is left in the buffer: a 32-byte frame claiming
	// 10000 requests would otherwise allocate ~2.8 MB before failing. The
	// transport's receive-memory budget charges only the frame length, so
	// this check is the only bound on that amplification.
	if uint64(reqCount)*minMessageWireSize > uint64(len(buf)-offset) {
		return 0, ErrBufferTooSmall
	}
	if reqCount > 0 {
		if cap(mb.Requests) >= int(reqCount) {
			mb.Requests = mb.Requests[:reqCount]
		} else {
			mb.Requests = make([]Message, reqCount)
		}
		for i := uint32(0); i < reqCount; i++ {
			n, err := mb.Requests[i].UnmarshalFrom(buf[offset:])
			if err != nil {
				return 0, err
			}
			offset += n
			// Track cumulative bytes to enforce MaxBatchSize.
			// Compare without truncation: offset is int (64-bit on amd64),
			// MaxBatchSize is uint64. A uint32 cast here would silently
			// wrap offsets > 4GB, bypassing the size limit (H9 DoS fix).
			if uint64(offset) > MaxBatchSize {
				return 0, ErrBatchSizeLimitExceeded
			}
		}
	} else {
		mb.Requests = mb.Requests[:0]
	}
	return offset, nil
}

// Reset clears all fields for reuse. A Requests slice grown past
// maxRetainedRequests is dropped rather than truncated so a pooled batch
// cannot retain attacker-influenced capacity.
//
// Each retained message is reset in turn: truncating Requests alone leaves the
// backing array holding Message values whose Entries slices are still live, so
// Message.maxRetainedEntries would never be applied and a pooled batch could
// pin len(Requests) * cap(Entries) * sizeof(Entry) bytes for the process
// lifetime.
func (mb *MessageBatch) Reset() {
	mb.BinVer = 0
	mb.DeploymentID = 0
	mb.SourceEpoch = 0
	mb.SourceAddress = ""
	for i := range mb.Requests {
		mb.Requests[i].Reset()
	}
	if cap(mb.Requests) > maxRetainedRequests {
		mb.Requests = nil
		return
	}
	mb.Requests = mb.Requests[:0]
}
