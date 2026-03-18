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

package kv

import "encoding/binary"

const (
	// OpPut stores a key-value pair.
	OpPut byte = 1

	// OpGet is reserved for read queries (no-op in Update).
	OpGet byte = 2

	// OpDelete removes a key.
	OpDelete byte = 3

	// headerSize is 1 byte op + 2 bytes keyLen (little-endian).
	headerSize = 3

	// MaxKeyLength is the maximum key length (uint16 max).
	MaxKeyLength = 65535
)

// EncodePut creates a binary Put command: [op:1][keyLen:2 LE][key][value].
func EncodePut(key, value []byte) ([]byte, error) {
	if len(key) > MaxKeyLength {
		return nil, &KeyTooLongError{Length: len(key)}
	}
	buf := make([]byte, headerSize+len(key)+len(value))
	buf[0] = OpPut
	binary.LittleEndian.PutUint16(buf[1:3], uint16(len(key)))
	copy(buf[headerSize:], key)
	copy(buf[headerSize+len(key):], value)
	return buf, nil
}

// EncodeGet creates a binary Get command: [op:1][keyLen:2 LE][key].
func EncodeGet(key []byte) ([]byte, error) {
	if len(key) > MaxKeyLength {
		return nil, &KeyTooLongError{Length: len(key)}
	}
	buf := make([]byte, headerSize+len(key))
	buf[0] = OpGet
	binary.LittleEndian.PutUint16(buf[1:3], uint16(len(key)))
	copy(buf[headerSize:], key)
	return buf, nil
}

// EncodeDelete creates a binary Delete command: [op:1][keyLen:2 LE][key].
func EncodeDelete(key []byte) ([]byte, error) {
	if len(key) > MaxKeyLength {
		return nil, &KeyTooLongError{Length: len(key)}
	}
	buf := make([]byte, headerSize+len(key))
	buf[0] = OpDelete
	binary.LittleEndian.PutUint16(buf[1:3], uint16(len(key)))
	copy(buf[headerSize:], key)
	return buf, nil
}

// ListQuery is a typed query for listing keys by prefix. It is passed
// through the Lookup path (not through Raft proposals) so no binary
// encoding is needed.
type ListQuery struct {
	Prefix string
}

// Command represents a decoded KV command.
type Command struct {
	Op    byte
	Key   []byte
	Value []byte
}

// DecodeCommand parses a binary command from data.
func DecodeCommand(data []byte) (Command, error) {
	if len(data) < headerSize {
		return Command{}, &InvalidCommandError{Reason: "data too short"}
	}
	op := data[0]
	if op < OpPut || op > OpDelete {
		return Command{}, &InvalidCommandError{Reason: "unknown op"}
	}
	keyLen := binary.LittleEndian.Uint16(data[1:3])
	if int(headerSize)+int(keyLen) > len(data) {
		return Command{}, &InvalidCommandError{Reason: "key length exceeds data"}
	}
	key := data[headerSize : headerSize+int(keyLen)]
	var value []byte
	if int(headerSize)+int(keyLen) < len(data) {
		value = data[headerSize+int(keyLen):]
	}
	return Command{Op: op, Key: key, Value: value}, nil
}
