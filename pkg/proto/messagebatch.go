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

// MessageBatch is one network frame of Raft messages. Wire format [BinVer:8][DeploymentID:8][SourceEpoch:8][SourceAddress:len+data][RequestsCount:4][requests...]; BinVer is first so it is validated before the rest.
type MessageBatch struct {
	// BinVer is the wire format version the batch is encoded at. The
	// receiver requires it to be in its WireWindow before unmarshaling.
	BinVer uint64
	// DeploymentID identifies the deployment for cross-deployment
	// message rejection.
	DeploymentID uint64
	// SourceEpoch is the sender's per-process boot epoch, which changes only on restart.
	SourceEpoch uint64
	// MaxBinVer is the highest wire version the sender accepts (WireWindow.Current); advertised, never negotiated (docs/compatibility/engineering.md, "Rolling upgrade").
	MaxBinVer uint64
	// SourceAddress is the sender's network address.
	SourceAddress string
	// Requests contains the batched Raft messages.
	Requests []Message
}

// messageBatchFixedSize is the wire size of the three fixed uint64 fields
// (BinVer, DeploymentID, SourceEpoch, MaxBinVer).
const messageBatchFixedSize = 4 * 8 // 32 bytes

// minMessageWireSize is the smallest encoding of a Message, equal to HeartbeatMessageSize.
const minMessageWireSize = HeartbeatMessageSize // 100 bytes

// maxRetainedRequests bounds the Requests capacity a pooled batch keeps across Reset.
const maxRetainedRequests = 256

// Size returns the total marshaled size of the batch in bytes.
func (mb *MessageBatch) Size() int {
	sz := messageBatchFixedSize + 4 + len(mb.SourceAddress) + 4
	for i := range mb.Requests {
		sz += mb.Requests[i].Size()
	}
	return sz
}

// MarshalTo writes the batch into buf (at least Size() bytes) and returns bytes written; ErrBufferTooSmall, ErrTooManyMessages or ErrAddressTooLong when the read side's bounds would be violated.
func (mb *MessageBatch) MarshalTo(buf []byte) (int, error) {
	if len(mb.Requests) > MaxMessagesPerBatch {
		return 0, ErrTooManyMessages
	}
	if len(mb.SourceAddress) > MaxAddressLength {
		return 0, ErrAddressTooLong
	}
	required := mb.Size()
	if len(buf) < required {
		return 0, ErrBufferTooSmall
	}
	putUint64(buf[0:], mb.BinVer)
	putUint64(buf[8:], mb.DeploymentID)
	putUint64(buf[16:], mb.SourceEpoch)
	putUint64(buf[24:], mb.MaxBinVer)
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
	return mb.UnmarshalFromWindow(buf, DefaultWireWindow())
}

// UnmarshalFromWindow is UnmarshalFrom against an explicit accept set. The
// transport passes its host's window; UnmarshalFrom passes this build's.
func (mb *MessageBatch) UnmarshalFromWindow(buf []byte, w WireWindow) (int, error) {
	if len(buf) < messageBatchFixedSize {
		return 0, ErrBufferTooSmall
	}
	mb.BinVer = getUint64(buf[0:])
	// Closed-set gate with no negotiation: a BinVer outside {current, previous} is rejected.
	if !w.Accepts(mb.BinVer) {
		return 0, ErrUnsupportedVersion
	}
	mb.DeploymentID = getUint64(buf[8:])
	mb.SourceEpoch = getUint64(buf[16:])
	mb.MaxBinVer = getUint64(buf[24:])
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
	// The count must also fit the remaining buffer before it drives the allocation.
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
			if exceedsMaxBatchSize(offset) {
				return 0, ErrBatchSizeLimitExceeded
			}
		}
	} else {
		mb.Requests = mb.Requests[:0]
	}
	return offset, nil
}

// exceedsMaxBatchSize reports whether offset has passed MaxBatchSize, compared as uint64 so it never wraps; named so TestExceedsMaxBatchSizeDoesNotTruncate can test it.
func exceedsMaxBatchSize(offset int) bool {
	return uint64(offset) > MaxBatchSize
}

// Reset clears all fields for reuse, resetting each retained message and dropping a Requests slice grown past maxRetainedRequests.
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
