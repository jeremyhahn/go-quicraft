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

package raft

// inflights is a sliding window tracking in-flight append messages sent to a
// single remote peer. It limits the number of outstanding (unacknowledged)
// messages to prevent fast leaders from overwhelming slow followers.
//
// Internally it uses a circular buffer of entry indices. The start field
// tracks the position of the oldest in-flight entry, and count tracks
// how many entries are currently in-flight.
type inflights struct {
	start  int      // position of the oldest in-flight entry in buffer
	count  int      // number of in-flight entries
	size   int      // maximum capacity
	buffer []uint64 // circular buffer of in-flight entry indices
}

// newInflights creates an inflights tracker with the given maximum capacity.
// The buffer is lazily allocated on the first add to avoid memory overhead
// for peers that have not yet received any messages.
func newInflights(size int) *inflights {
	return &inflights{
		size: size,
	}
}

// add records a new in-flight message for the given log index. The buffer
// grows lazily up to the configured size. Returns ErrInflightsFull if the
// buffer is already at capacity. Callers should check full() before calling
// add as a fast-path guard; this error return is a defensive backstop.
func (in *inflights) add(index uint64) error {
	if in.full() {
		return ErrInflightsFull
	}
	// Lazy allocation: allocate on first use to avoid memory for idle peers.
	if len(in.buffer) == 0 {
		in.buffer = make([]uint64, in.size)
	}
	next := in.start + in.count
	if next >= in.size {
		next -= in.size
	}
	in.buffer[next] = index
	in.count++
	return nil
}

// freeTo acknowledges all in-flight messages up to and including the given
// index. Entries are freed from the oldest (start) position forward. If the
// index is not found in the buffer, no entries are freed.
func (in *inflights) freeTo(index uint64) {
	if in.count == 0 {
		return
	}
	// Scan from oldest to newest to find how many entries to free.
	freed := 0
	for i := 0; i < in.count; i++ {
		pos := in.start + i
		if pos >= in.size {
			pos -= in.size
		}
		if in.buffer[pos] <= index {
			freed = i + 1
		}
	}
	// Advance the start pointer past all freed entries.
	in.count -= freed
	in.start += freed
	if in.start >= in.size {
		in.start -= in.size
	}
}

// freeFirstOne frees the oldest in-flight slot WITHOUT the acknowledgement
// that would normally retire it. It exists for one situation: a peer whose
// in-flight messages were delivered to a PREVIOUS incarnation of that peer.
// Those acknowledgements can never arrive, and freeTo only frees records at
// or below an acknowledged index, so every later ack from the restarted peer
// (which reports a position BELOW the window) frees nothing. The window would
// stay full forever, and a full window is exactly what makes isPaused report
// true, so the leader would never send that peer another entry.
//
// Releasing one slot per heartbeat response bounds that: the peer is known to
// be alive, so let exactly one message through. Either it is acknowledged
// (the window was merely busy) or it is rejected (the peer restarted), and
// the rejection drives the normal backtracking that repairs the peer's
// position. This mirrors etcd/raft's MsgHeartbeatResp handling.
func (in *inflights) freeFirstOne() {
	if in.count == 0 {
		return
	}
	in.count--
	in.start++
	if in.start >= in.size {
		in.start -= in.size
	}
}

// full returns true if the in-flight window is at capacity.
func (in *inflights) full() bool {
	return in.count >= in.size
}

// Count returns the number of in-flight messages.
func (in *inflights) Count() int {
	return in.count
}

// reset clears all in-flight tracking, resetting start and count to zero.
// The underlying buffer is retained to avoid reallocation.
func (in *inflights) reset() {
	in.start = 0
	in.count = 0
}
