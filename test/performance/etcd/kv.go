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

package etcd_perf

import "encoding/binary"

// Wire format for KV operations, identical to the QuicRaft and Dragonboat
// benchmark encodings so every engine proposes byte-for-byte equivalent
// commands:
//
//	[op:1][keyLen:2 BE][key:keyLen][value:remaining]
const opPut byte = 0x01

// proposalBytes is the total wire size of every proposed KV command. It is
// fixed at 128 bytes and shared by all benchmarked engines so each Raft log
// entry carries an identical payload, keeping the comparison apples-to-apples.
const proposalBytes = 128

// commitLatencyWarmup is the number of unmeasured proposals every engine runs
// before its commit-latency measurement loop, so the first timed proposal does
// not include one-time pipeline initialization.
const commitLatencyWarmup = 100

// encodeKVCmd builds the wire-format command for a Put operation.
func encodeKVCmd(key, value []byte) []byte {
	keyLen := len(key)
	cmd := make([]byte, 1+2+keyLen+len(value))
	cmd[0] = opPut
	binary.BigEndian.PutUint16(cmd[1:3], uint16(keyLen))
	copy(cmd[3:3+keyLen], key)
	copy(cmd[3+keyLen:], value)
	return cmd
}

// parseKVCmd decodes the wire-format command.
func parseKVCmd(cmd []byte) (op byte, key, value []byte) {
	if len(cmd) < 3 {
		return 0, nil, nil
	}
	op = cmd[0]
	keyLen := binary.BigEndian.Uint16(cmd[1:3])
	if len(cmd) < int(3+keyLen) {
		return op, nil, nil
	}
	key = cmd[3 : 3+keyLen]
	value = cmd[3+keyLen:]
	return op, key, value
}

// makeKVCmd builds a Put command whose total wire size is exactly
// proposalBytes by padding the value to fill the space remaining after the
// 3-byte header and the key.
func makeKVCmd(key []byte) []byte {
	valLen := proposalBytes - 3 - len(key)
	if valLen < 0 {
		valLen = 0
	}
	return encodeKVCmd(key, make([]byte, valLen))
}

// requestIDBytes is the width of the request id that makeRequestCmd embeds at
// the start of the value padding.
const requestIDBytes = 8

// makeRequestCmd builds a proposalBytes-sized Put command, like makeKVCmd, and
// writes id into the first requestIDBytes of the value padding. Raw etcd/raft
// has no per-proposal acknowledgement, so the id is how the apply loop tells
// a proposer that THIS entry, not merely some entry, has been applied. The
// wire size and layout are unchanged: the id occupies bytes the other engines
// fill with zero padding.
func makeRequestCmd(key []byte, id uint64) []byte {
	cmd := makeKVCmd(key)
	binary.BigEndian.PutUint64(cmd[3+len(key):], id)
	return cmd
}

// requestID extracts the id makeRequestCmd embedded in cmd. It reports false
// when cmd is not a Put command or its value is too short to carry an id.
func requestID(cmd []byte) (uint64, bool) {
	op, key, value := parseKVCmd(cmd)
	if op != opPut || key == nil || len(value) < requestIDBytes {
		return 0, false
	}
	return binary.BigEndian.Uint64(value[:requestIDBytes]), true
}
