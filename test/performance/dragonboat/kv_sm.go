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

package dragonboat_perf

import (
	"encoding/binary"
	"io"
	"sync"

	dsm "github.com/lni/dragonboat/v4/statemachine"
)

// Wire format for KV operations:
//
//	[op:1][keyLen:2 BE][key:keyLen][value:remaining]
//
// Operations:
//
//	0x01 = Put
//	0x02 = Get (value field ignored)
const (
	opPut byte = 0x01
	opGet byte = 0x02
)

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

// encodeKVGet builds the wire-format command for a Get query.
func encodeKVGet(key []byte) []byte {
	keyLen := len(key)
	cmd := make([]byte, 1+2+keyLen)
	cmd[0] = opGet
	binary.BigEndian.PutUint16(cmd[1:3], uint16(keyLen))
	copy(cmd[3:3+keyLen], key)
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

// ---------------------------------------------------------------------------
// Dragonboat KV State Machine
// ---------------------------------------------------------------------------

// dragonboatKVSM implements dsm.IStateMachine for Dragonboat benchmarks.
type dragonboatKVSM struct {
	mu   sync.RWMutex
	data map[string][]byte
}

// Compile-time interface assertion.
var _ dsm.IStateMachine = (*dragonboatKVSM)(nil)

// newDragonboatKVSM creates a new Dragonboat KV state machine.
func newDragonboatKVSM() *dragonboatKVSM {
	return &dragonboatKVSM{
		data: make(map[string][]byte, 1024),
	}
}

// Update applies a single committed entry.
func (s *dragonboatKVSM) Update(entry dsm.Entry) (dsm.Result, error) {
	op, key, value := parseKVCmd(entry.Cmd)
	switch op {
	case opPut:
		owned := make([]byte, len(value))
		copy(owned, value)
		s.mu.Lock()
		s.data[string(key)] = owned
		s.mu.Unlock()
		return dsm.Result{Value: uint64(len(value))}, nil
	case opGet:
		s.mu.RLock()
		v, ok := s.data[string(key)]
		s.mu.RUnlock()
		if ok {
			return dsm.Result{Value: 1, Data: v}, nil
		}
		return dsm.Result{Value: 0}, nil
	default:
		return dsm.Result{Value: 0}, nil
	}
}

// Lookup reads a key from the KV state machine.
func (s *dragonboatKVSM) Lookup(query interface{}) (interface{}, error) {
	key, ok := query.([]byte)
	if !ok {
		return nil, nil
	}
	s.mu.RLock()
	v := s.data[string(key)]
	s.mu.RUnlock()
	return v, nil
}

// SaveSnapshot serializes the full KV state to the writer.
func (s *dragonboatKVSM) SaveSnapshot(w io.Writer, _ dsm.ISnapshotFileCollection, _ <-chan struct{}) error {
	s.mu.RLock()
	defer s.mu.RUnlock()

	countBuf := make([]byte, 4)
	binary.BigEndian.PutUint32(countBuf, uint32(len(s.data)))
	if _, err := w.Write(countBuf); err != nil {
		return err
	}

	lenBuf := make([]byte, 4)
	for k, v := range s.data {
		kb := []byte(k)
		binary.BigEndian.PutUint32(lenBuf, uint32(len(kb)))
		if _, err := w.Write(lenBuf); err != nil {
			return err
		}
		if _, err := w.Write(kb); err != nil {
			return err
		}
		binary.BigEndian.PutUint32(lenBuf, uint32(len(v)))
		if _, err := w.Write(lenBuf); err != nil {
			return err
		}
		if _, err := w.Write(v); err != nil {
			return err
		}
	}
	return nil
}

// RecoverFromSnapshot restores state from the reader.
func (s *dragonboatKVSM) RecoverFromSnapshot(r io.Reader, _ []dsm.SnapshotFile, _ <-chan struct{}) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	countBuf := make([]byte, 4)
	if _, err := io.ReadFull(r, countBuf); err != nil {
		return err
	}
	count := binary.BigEndian.Uint32(countBuf)

	s.data = make(map[string][]byte, count)
	lenBuf := make([]byte, 4)
	for i := uint32(0); i < count; i++ {
		if _, err := io.ReadFull(r, lenBuf); err != nil {
			return err
		}
		keyLen := binary.BigEndian.Uint32(lenBuf)
		key := make([]byte, keyLen)
		if _, err := io.ReadFull(r, key); err != nil {
			return err
		}
		if _, err := io.ReadFull(r, lenBuf); err != nil {
			return err
		}
		valLen := binary.BigEndian.Uint32(lenBuf)
		val := make([]byte, valLen)
		if _, err := io.ReadFull(r, val); err != nil {
			return err
		}
		s.data[string(key)] = val
	}
	return nil
}

// Close releases resources.
func (s *dragonboatKVSM) Close() error {
	return nil
}
