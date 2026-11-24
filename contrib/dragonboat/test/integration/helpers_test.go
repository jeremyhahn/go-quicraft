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

//go:build integration

package integration_test

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	dragonboat "github.com/jeremyhahn/go-quicraft/contrib/dragonboat"
	dbClient "github.com/jeremyhahn/go-quicraft/contrib/dragonboat/client"
	dbConfig "github.com/jeremyhahn/go-quicraft/contrib/dragonboat/config"
	dbSM "github.com/jeremyhahn/go-quicraft/contrib/dragonboat/statemachine"
	quicraft "github.com/jeremyhahn/go-quicraft/pkg"
	"github.com/jeremyhahn/go-quicraft/test/electionbudget"
	"github.com/jeremyhahn/go-quicraft/test/testpki"
)

// Raft timing: the wait budgets below derive from the shard configuration these tests start.

// testRTTMillisecond is the Raft tick interval, in milliseconds, that
// dbTestNodeHostConfig gives every host in this suite. One tick is one
// RTTMillisecond: engine.New computes its tick interval directly from this
// field, so every timeout expressed in ticks is this many milliseconds each.
const testRTTMillisecond = 50

// testElectionRTT is the ElectionRTT dbTestShardConfig gives every shard: the
// number of ticks a follower waits without hearing from a leader before it
// stands for election.
const testElectionRTT = 10

// raftTick is one Raft tick in wall-clock time.
const raftTick = testRTTMillisecond * time.Millisecond

// Election budgets come from test/electionbudget, derived from testElectionRTT and testRTTMillisecond.
var (
	electionRound         = electionbudget.Round(testElectionRTT, testRTTMillisecond)
	leaderElectionTimeout = electionbudget.LeaderElection(testElectionRTT, testRTTMillisecond)
	failoverTimeout       = electionbudget.Failover(testElectionRTT, testRTTMillisecond)
)

// defaultTimeout is the standard budget for a single Raft operation that is
// not an election: a proposal round trip, a linearizable read, a membership
// query. It is deliberately NOT expressed in election rounds, because none of
// those wait for an election.
const defaultTimeout = 15 * time.Second

// coldStartElectionRounds keeps the local name this suite's log lines use.
const coldStartElectionRounds = electionbudget.ColdStartRounds

// membershipTimeout is failoverTimeout plus defaultTimeout: a leadership change, then the commit.
var membershipTimeout = failoverTimeout + defaultTimeout

// leaderTransferRounds is the election rounds one transfer request gets before it is made again; a transfer that completes does so within two.
const leaderTransferRounds = 2

// leaderTransferRound is the wall-clock form of leaderTransferRounds.
var leaderTransferRound = leaderTransferRounds * electionRound

// leaderTransferTimeout bounds the whole transfer, re-requests included: one failover plus one clean attempt.
var leaderTransferTimeout = failoverTimeout + leaderTransferRound

// catchUpTimeout is one failover plus one ordinary operation, for a rejoining replica to hold every committed entry.
var catchUpTimeout = failoverTimeout + defaultTimeout

// ---------------------------------------------------------------------------
// KV Command Encoding
// ---------------------------------------------------------------------------

const (
	opPut byte = 1
)

// encodeKVPut encodes a put command for the dbKVStateMachine.
func encodeKVPut(key, value string) []byte {
	keyBytes := []byte(key)
	valBytes := []byte(value)
	buf := make([]byte, 1+2+len(keyBytes)+len(valBytes))
	buf[0] = opPut
	binary.BigEndian.PutUint16(buf[1:3], uint16(len(keyBytes)))
	copy(buf[3:3+len(keyBytes)], keyBytes)
	copy(buf[3+len(keyBytes):], valBytes)
	return buf
}

// encodeKVGet encodes a get query key for dbKVStateMachine.Lookup.
func encodeKVGet(key string) []byte {
	return []byte(key)
}

// ---------------------------------------------------------------------------
// KV State Machine (dragonboat IStateMachine interface)
// ---------------------------------------------------------------------------

// dbKVStateMachine implements dbSM.IStateMachine for integration testing.
// It is a simple in-memory key-value store that mirrors the e2e KV store
// but uses the dragonboat single-entry Update convention.
type dbKVStateMachine struct {
	mu   sync.RWMutex
	data map[string]string
}

func newDBKVStateMachine() *dbKVStateMachine {
	return &dbKVStateMachine{
		data: make(map[string]string),
	}
}

// Update applies a single committed entry to the KV store.
func (kv *dbKVStateMachine) Update(entry dbSM.Entry) (dbSM.Result, error) {
	if len(entry.Cmd) < 3 {
		return dbSM.Result{Value: 0}, nil
	}
	op := entry.Cmd[0]
	keyLen := binary.BigEndian.Uint16(entry.Cmd[1:3])
	if int(keyLen)+3 > len(entry.Cmd) {
		return dbSM.Result{Value: 0}, nil
	}
	key := string(entry.Cmd[3 : 3+keyLen])
	switch op {
	case opPut:
		value := string(entry.Cmd[3+keyLen:])
		kv.mu.Lock()
		kv.data[key] = value
		kv.mu.Unlock()
		return dbSM.Result{Value: uint64(len(value))}, nil
	default:
		return dbSM.Result{Value: 0}, nil
	}
}

// Lookup reads a value by key from the KV store.
func (kv *dbKVStateMachine) Lookup(query interface{}) (interface{}, error) {
	key, ok := query.(string)
	if !ok {
		keyBytes, isByteSlice := query.([]byte)
		if !isByteSlice {
			return nil, fmt.Errorf("dbKVStateMachine: unsupported query type %T", query)
		}
		key = string(keyBytes)
	}
	kv.mu.RLock()
	defer kv.mu.RUnlock()
	val, exists := kv.data[key]
	if !exists {
		return "", nil
	}
	return val, nil
}

// SaveSnapshot serializes the entire KV store to the writer.
func (kv *dbKVStateMachine) SaveSnapshot(w io.Writer, _ dbSM.ISnapshotFileCollection, stopper <-chan struct{}) error {
	kv.mu.RLock()
	defer kv.mu.RUnlock()

	countBuf := make([]byte, 4)
	binary.BigEndian.PutUint32(countBuf, uint32(len(kv.data)))
	if _, err := w.Write(countBuf); err != nil {
		return err
	}

	for k, v := range kv.data {
		select {
		case <-stopper:
			return fmt.Errorf("dbKVStateMachine: snapshot cancelled")
		default:
		}
		keyBytes := []byte(k)
		valBytes := []byte(v)

		header := make([]byte, 6)
		binary.BigEndian.PutUint16(header[0:2], uint16(len(keyBytes)))
		binary.BigEndian.PutUint32(header[2:6], uint32(len(valBytes)))
		if _, err := w.Write(header); err != nil {
			return err
		}
		if _, err := w.Write(keyBytes); err != nil {
			return err
		}
		if _, err := w.Write(valBytes); err != nil {
			return err
		}
	}
	return nil
}

// RecoverFromSnapshot restores the KV store from a snapshot reader.
func (kv *dbKVStateMachine) RecoverFromSnapshot(r io.Reader, _ []dbSM.SnapshotFile, stopper <-chan struct{}) error {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	countBuf := make([]byte, 4)
	if _, err := io.ReadFull(r, countBuf); err != nil {
		return err
	}
	count := binary.BigEndian.Uint32(countBuf)

	newData := make(map[string]string, count)
	for i := uint32(0); i < count; i++ {
		select {
		case <-stopper:
			return fmt.Errorf("dbKVStateMachine: recovery cancelled")
		default:
		}
		header := make([]byte, 6)
		if _, err := io.ReadFull(r, header); err != nil {
			return err
		}
		keyLen := binary.BigEndian.Uint16(header[0:2])
		valLen := binary.BigEndian.Uint32(header[2:6])

		keyBuf := make([]byte, keyLen)
		if _, err := io.ReadFull(r, keyBuf); err != nil {
			return err
		}
		valBuf := make([]byte, valLen)
		if _, err := io.ReadFull(r, valBuf); err != nil {
			return err
		}
		newData[string(keyBuf)] = string(valBuf)
	}
	kv.data = newData
	return nil
}

// Close is a no-op for the in-memory KV store.
func (kv *dbKVStateMachine) Close() error {
	return nil
}

// Get returns the value for a key directly (for test assertions).
func (kv *dbKVStateMachine) Get(key string) (string, bool) {
	kv.mu.RLock()
	defer kv.mu.RUnlock()
	v, ok := kv.data[key]
	return v, ok
}

// Snapshot returns a copy of the KV data for test assertions.
func (kv *dbKVStateMachine) Snapshot() map[string]string {
	kv.mu.RLock()
	defer kv.mu.RUnlock()
	cp := make(map[string]string, len(kv.data))
	for k, v := range kv.data {
		cp[k] = v
	}
	return cp
}

// Compile-time assertion.
var _ dbSM.IStateMachine = (*dbKVStateMachine)(nil)

// ---------------------------------------------------------------------------
// Concurrent KV State Machine (dragonboat IConcurrentStateMachine)
// ---------------------------------------------------------------------------

// dbConcurrentKVStateMachine implements dbSM.IConcurrentStateMachine for
// integration testing. It supports concurrent reads during writes and
// receives batch updates.
type dbConcurrentKVStateMachine struct {
	mu   sync.RWMutex
	data map[string]string
}

func newDBConcurrentKVStateMachine() *dbConcurrentKVStateMachine {
	return &dbConcurrentKVStateMachine{
		data: make(map[string]string),
	}
}

// Update applies a batch of committed entries to the KV store. Each entry's
// Result field is populated in-place per dragonboat convention.
func (kv *dbConcurrentKVStateMachine) Update(entries []dbSM.Entry) ([]dbSM.Entry, error) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	for i := range entries {
		e := &entries[i]
		if len(e.Cmd) < 3 {
			e.Result = dbSM.Result{Value: 0}
			continue
		}
		op := e.Cmd[0]
		keyLen := binary.BigEndian.Uint16(e.Cmd[1:3])
		if int(keyLen)+3 > len(e.Cmd) {
			e.Result = dbSM.Result{Value: 0}
			continue
		}
		key := string(e.Cmd[3 : 3+keyLen])
		switch op {
		case opPut:
			value := string(e.Cmd[3+keyLen:])
			kv.data[key] = value
			e.Result = dbSM.Result{Value: uint64(len(value))}
		default:
			e.Result = dbSM.Result{Value: 0}
		}
	}
	return entries, nil
}

// Lookup reads a value by key from the KV store. May be called concurrently
// with Update.
func (kv *dbConcurrentKVStateMachine) Lookup(query interface{}) (interface{}, error) {
	key, ok := query.(string)
	if !ok {
		keyBytes, isByteSlice := query.([]byte)
		if !isByteSlice {
			return nil, fmt.Errorf("dbConcurrentKVStateMachine: unsupported query type %T", query)
		}
		key = string(keyBytes)
	}
	kv.mu.RLock()
	defer kv.mu.RUnlock()
	val, exists := kv.data[key]
	if !exists {
		return "", nil
	}
	return val, nil
}

// PrepareSnapshot captures a point-in-time snapshot of the keys for the
// subsequent SaveSnapshot call.
func (kv *dbConcurrentKVStateMachine) PrepareSnapshot() (interface{}, error) {
	kv.mu.RLock()
	defer kv.mu.RUnlock()
	cp := make(map[string]string, len(kv.data))
	for k, v := range kv.data {
		cp[k] = v
	}
	return cp, nil
}

// SaveSnapshot writes the captured snapshot context to the writer.
func (kv *dbConcurrentKVStateMachine) SaveSnapshot(ctx interface{}, w io.Writer, _ dbSM.ISnapshotFileCollection, stopper <-chan struct{}) error {
	data := ctx.(map[string]string)
	countBuf := make([]byte, 4)
	binary.BigEndian.PutUint32(countBuf, uint32(len(data)))
	if _, err := w.Write(countBuf); err != nil {
		return err
	}
	for k, v := range data {
		select {
		case <-stopper:
			return fmt.Errorf("dbConcurrentKVStateMachine: snapshot cancelled")
		default:
		}
		keyBytes := []byte(k)
		valBytes := []byte(v)
		header := make([]byte, 6)
		binary.BigEndian.PutUint16(header[0:2], uint16(len(keyBytes)))
		binary.BigEndian.PutUint32(header[2:6], uint32(len(valBytes)))
		if _, err := w.Write(header); err != nil {
			return err
		}
		if _, err := w.Write(keyBytes); err != nil {
			return err
		}
		if _, err := w.Write(valBytes); err != nil {
			return err
		}
	}
	return nil
}

// RecoverFromSnapshot restores the KV store from a snapshot reader.
func (kv *dbConcurrentKVStateMachine) RecoverFromSnapshot(r io.Reader, _ []dbSM.SnapshotFile, stopper <-chan struct{}) error {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	countBuf := make([]byte, 4)
	if _, err := io.ReadFull(r, countBuf); err != nil {
		return err
	}
	count := binary.BigEndian.Uint32(countBuf)
	newData := make(map[string]string, count)
	for i := uint32(0); i < count; i++ {
		select {
		case <-stopper:
			return fmt.Errorf("dbConcurrentKVStateMachine: recovery cancelled")
		default:
		}
		header := make([]byte, 6)
		if _, err := io.ReadFull(r, header); err != nil {
			return err
		}
		keyLen := binary.BigEndian.Uint16(header[0:2])
		valLen := binary.BigEndian.Uint32(header[2:6])
		keyBuf := make([]byte, keyLen)
		if _, err := io.ReadFull(r, keyBuf); err != nil {
			return err
		}
		valBuf := make([]byte, valLen)
		if _, err := io.ReadFull(r, valBuf); err != nil {
			return err
		}
		newData[string(keyBuf)] = string(valBuf)
	}
	kv.data = newData
	return nil
}

// Close is a no-op for the in-memory concurrent KV store.
func (kv *dbConcurrentKVStateMachine) Close() error {
	return nil
}

// Get returns the value for a key directly (for test assertions).
func (kv *dbConcurrentKVStateMachine) Get(key string) (string, bool) {
	kv.mu.RLock()
	defer kv.mu.RUnlock()
	v, ok := kv.data[key]
	return v, ok
}

// Snapshot returns a copy of the KV data for test assertions.
func (kv *dbConcurrentKVStateMachine) Snapshot() map[string]string {
	kv.mu.RLock()
	defer kv.mu.RUnlock()
	cp := make(map[string]string, len(kv.data))
	for k, v := range kv.data {
		cp[k] = v
	}
	return cp
}

// Compile-time assertion.
var _ dbSM.IConcurrentStateMachine = (*dbConcurrentKVStateMachine)(nil)

// ---------------------------------------------------------------------------
// Concurrent State Machine Tracking
// ---------------------------------------------------------------------------

// dbConcurrentSMTracker tracks dbConcurrentKVStateMachine instances created
// by the factory so tests can assert on internal state.
type dbConcurrentSMTracker struct {
	mu       sync.Mutex
	machines map[dbSMKey]*dbConcurrentKVStateMachine
}

func (t *dbConcurrentSMTracker) Get(shardID, replicaID uint64) *dbConcurrentKVStateMachine {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.machines[dbSMKey{shardID, replicaID}]
}

// dbConcurrentKVCreateFunc returns a dbSM.CreateConcurrentStateMachineFunc
// that produces dbConcurrentKVStateMachine instances with a tracker for
// test assertions.
func dbConcurrentKVCreateFunc() (dbSM.CreateConcurrentStateMachineFunc, *dbConcurrentSMTracker) {
	tracker := &dbConcurrentSMTracker{
		machines: make(map[dbSMKey]*dbConcurrentKVStateMachine),
	}
	createFn := func(shardID, replicaID uint64) dbSM.IConcurrentStateMachine {
		machine := newDBConcurrentKVStateMachine()
		tracker.mu.Lock()
		tracker.machines[dbSMKey{shardID, replicaID}] = machine
		tracker.mu.Unlock()
		return machine
	}
	return createFn, tracker
}

// ---------------------------------------------------------------------------
// On-Disk KV State Machine (dragonboat IOnDiskStateMachine)
// ---------------------------------------------------------------------------

// dbOnDiskKVStateMachine implements dbSM.IOnDiskStateMachine for
// integration testing. It simulates an on-disk state machine backed by
// in-memory storage with Open/Sync lifecycle methods.
type dbOnDiskKVStateMachine struct {
	mu      sync.RWMutex
	data    map[string]string
	applied uint64
}

func newDBOnDiskKVStateMachine() *dbOnDiskKVStateMachine {
	return &dbOnDiskKVStateMachine{
		data: make(map[string]string),
	}
}

// Open initializes the on-disk state machine and returns the last
// applied Raft log index.
func (kv *dbOnDiskKVStateMachine) Open(_ <-chan struct{}) (uint64, error) {
	kv.mu.RLock()
	defer kv.mu.RUnlock()
	return kv.applied, nil
}

// Update applies a batch of committed entries. Each entry's Result field
// is populated in-place.
func (kv *dbOnDiskKVStateMachine) Update(entries []dbSM.Entry) ([]dbSM.Entry, error) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	for i := range entries {
		e := &entries[i]
		if len(e.Cmd) < 3 {
			e.Result = dbSM.Result{Value: 0}
			if e.Index > kv.applied {
				kv.applied = e.Index
			}
			continue
		}
		op := e.Cmd[0]
		keyLen := binary.BigEndian.Uint16(e.Cmd[1:3])
		if int(keyLen)+3 > len(e.Cmd) {
			e.Result = dbSM.Result{Value: 0}
			if e.Index > kv.applied {
				kv.applied = e.Index
			}
			continue
		}
		key := string(e.Cmd[3 : 3+keyLen])
		switch op {
		case opPut:
			value := string(e.Cmd[3+keyLen:])
			kv.data[key] = value
			e.Result = dbSM.Result{Value: uint64(len(value))}
		default:
			e.Result = dbSM.Result{Value: 0}
		}
		if e.Index > kv.applied {
			kv.applied = e.Index
		}
	}
	return entries, nil
}

// Lookup reads a value by key. May be called concurrently with Update.
func (kv *dbOnDiskKVStateMachine) Lookup(query interface{}) (interface{}, error) {
	key, ok := query.(string)
	if !ok {
		keyBytes, isByteSlice := query.([]byte)
		if !isByteSlice {
			return nil, fmt.Errorf("dbOnDiskKVStateMachine: unsupported query type %T", query)
		}
		key = string(keyBytes)
	}
	kv.mu.RLock()
	defer kv.mu.RUnlock()
	val, exists := kv.data[key]
	if !exists {
		return "", nil
	}
	return val, nil
}

// Sync flushes pending writes to durable storage. No-op for the in-memory
// test implementation.
func (kv *dbOnDiskKVStateMachine) Sync() error {
	return nil
}

// PrepareSnapshot captures a point-in-time snapshot context.
func (kv *dbOnDiskKVStateMachine) PrepareSnapshot() (interface{}, error) {
	kv.mu.RLock()
	defer kv.mu.RUnlock()
	cp := make(map[string]string, len(kv.data))
	for k, v := range kv.data {
		cp[k] = v
	}
	return cp, nil
}

// SaveSnapshot writes the on-disk state to the writer. Note: on-disk
// state machines have NO ISnapshotFileCollection parameter.
func (kv *dbOnDiskKVStateMachine) SaveSnapshot(ctx interface{}, w io.Writer, stopper <-chan struct{}) error {
	data := ctx.(map[string]string)
	countBuf := make([]byte, 4)
	binary.BigEndian.PutUint32(countBuf, uint32(len(data)))
	if _, err := w.Write(countBuf); err != nil {
		return err
	}
	for k, v := range data {
		select {
		case <-stopper:
			return fmt.Errorf("dbOnDiskKVStateMachine: snapshot cancelled")
		default:
		}
		keyBytes := []byte(k)
		valBytes := []byte(v)
		header := make([]byte, 6)
		binary.BigEndian.PutUint16(header[0:2], uint16(len(keyBytes)))
		binary.BigEndian.PutUint32(header[2:6], uint32(len(valBytes)))
		if _, err := w.Write(header); err != nil {
			return err
		}
		if _, err := w.Write(keyBytes); err != nil {
			return err
		}
		if _, err := w.Write(valBytes); err != nil {
			return err
		}
	}
	return nil
}

// RecoverFromSnapshot restores the on-disk state from a reader. Note:
// on-disk state machines have NO []SnapshotFile parameter.
func (kv *dbOnDiskKVStateMachine) RecoverFromSnapshot(r io.Reader, stopper <-chan struct{}) error {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	countBuf := make([]byte, 4)
	if _, err := io.ReadFull(r, countBuf); err != nil {
		return err
	}
	count := binary.BigEndian.Uint32(countBuf)
	newData := make(map[string]string, count)
	for i := uint32(0); i < count; i++ {
		select {
		case <-stopper:
			return fmt.Errorf("dbOnDiskKVStateMachine: recovery cancelled")
		default:
		}
		header := make([]byte, 6)
		if _, err := io.ReadFull(r, header); err != nil {
			return err
		}
		keyLen := binary.BigEndian.Uint16(header[0:2])
		valLen := binary.BigEndian.Uint32(header[2:6])
		keyBuf := make([]byte, keyLen)
		if _, err := io.ReadFull(r, keyBuf); err != nil {
			return err
		}
		valBuf := make([]byte, valLen)
		if _, err := io.ReadFull(r, valBuf); err != nil {
			return err
		}
		newData[string(keyBuf)] = string(valBuf)
	}
	kv.data = newData
	return nil
}

// Close is a no-op for the in-memory on-disk KV store simulation.
func (kv *dbOnDiskKVStateMachine) Close() error {
	return nil
}

// Get returns the value for a key directly (for test assertions).
func (kv *dbOnDiskKVStateMachine) Get(key string) (string, bool) {
	kv.mu.RLock()
	defer kv.mu.RUnlock()
	v, ok := kv.data[key]
	return v, ok
}

// Compile-time assertion.
var _ dbSM.IOnDiskStateMachine = (*dbOnDiskKVStateMachine)(nil)

// ---------------------------------------------------------------------------
// On-Disk State Machine Tracking
// ---------------------------------------------------------------------------

// dbOnDiskSMTracker tracks dbOnDiskKVStateMachine instances created by
// the factory so tests can assert on internal state.
type dbOnDiskSMTracker struct {
	mu       sync.Mutex
	machines map[dbSMKey]*dbOnDiskKVStateMachine
}

func (t *dbOnDiskSMTracker) Get(shardID, replicaID uint64) *dbOnDiskKVStateMachine {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.machines[dbSMKey{shardID, replicaID}]
}

// dbOnDiskKVCreateFunc returns a dbSM.CreateOnDiskStateMachineFunc that
// produces dbOnDiskKVStateMachine instances with a tracker for test
// assertions.
func dbOnDiskKVCreateFunc() (dbSM.CreateOnDiskStateMachineFunc, *dbOnDiskSMTracker) {
	tracker := &dbOnDiskSMTracker{
		machines: make(map[dbSMKey]*dbOnDiskKVStateMachine),
	}
	createFn := func(shardID, replicaID uint64) dbSM.IOnDiskStateMachine {
		machine := newDBOnDiskKVStateMachine()
		tracker.mu.Lock()
		tracker.machines[dbSMKey{shardID, replicaID}] = machine
		tracker.mu.Unlock()
		return machine
	}
	return createFn, tracker
}

// ---------------------------------------------------------------------------
// State Machine Tracking
// ---------------------------------------------------------------------------

// dbSMKey identifies a state machine by shard and replica.
type dbSMKey struct {
	shardID   uint64
	replicaID uint64
}

// dbSMTracker tracks dbKVStateMachine instances created by the factory
// so tests can assert on internal state.
type dbSMTracker struct {
	mu       sync.Mutex
	machines map[dbSMKey]*dbKVStateMachine
}

func (t *dbSMTracker) Get(shardID, replicaID uint64) *dbKVStateMachine {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.machines[dbSMKey{shardID, replicaID}]
}

// dbKVCreateFunc returns a dbSM.CreateStateMachineFunc that produces
// dbKVStateMachine instances with a tracker for test assertions.
func dbKVCreateFunc() (dbSM.CreateStateMachineFunc, *dbSMTracker) {
	tracker := &dbSMTracker{
		machines: make(map[dbSMKey]*dbKVStateMachine),
	}
	createFn := func(shardID, replicaID uint64) dbSM.IStateMachine {
		machine := newDBKVStateMachine()
		tracker.mu.Lock()
		tracker.machines[dbSMKey{shardID, replicaID}] = machine
		tracker.mu.Unlock()
		return machine
	}
	return createFn, tracker
}

// ---------------------------------------------------------------------------
// Gated KV State Machine
// ---------------------------------------------------------------------------

// gateKeyPrefix marks a proposal that dbGatedKVStateMachine.Update must block
// on until the test releases it.
const gateKeyPrefix = "gate-"

// decodeKVPutKey extracts the key from a command produced by encodeKVPut.
func decodeKVPutKey(cmd []byte) (string, bool) {
	if len(cmd) < 3 || cmd[0] != opPut {
		return "", false
	}
	keyLen := binary.BigEndian.Uint16(cmd[1:3])
	if int(keyLen)+3 > len(cmd) {
		return "", false
	}
	return string(cmd[3 : 3+keyLen]), true
}

// dbGatedKVStateMachine parks Update on gateKeyPrefix keys until release closes, holding the applied index below a config change.
type dbGatedKVStateMachine struct {
	*dbKVStateMachine
	entered   chan struct{}
	enterOnce sync.Once
	release   <-chan struct{}
}

// Update parks on a gate entry, then delegates to the embedded KV store.
func (kv *dbGatedKVStateMachine) Update(entry dbSM.Entry) (dbSM.Result, error) {
	if key, ok := decodeKVPutKey(entry.Cmd); ok && strings.HasPrefix(key, gateKeyPrefix) {
		kv.enterOnce.Do(func() { close(kv.entered) })
		<-kv.release
	}
	return kv.dbKVStateMachine.Update(entry)
}

// Compile-time assertion.
var _ dbSM.IStateMachine = (*dbGatedKVStateMachine)(nil)

// dbGatedSMTracker tracks dbGatedKVStateMachine instances so a test can wait
// for a replica's apply loop to actually reach the gate.
type dbGatedSMTracker struct {
	mu       sync.Mutex
	machines map[dbSMKey]*dbGatedKVStateMachine
}

func (t *dbGatedSMTracker) Get(shardID, replicaID uint64) *dbGatedKVStateMachine {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.machines[dbSMKey{shardID, replicaID}]
}

// dbGatedKVCreateFunc returns a gated KV factory plus a plain dbSMTracker and a gated tracker.
func dbGatedKVCreateFunc(release <-chan struct{}) (dbSM.CreateStateMachineFunc, *dbSMTracker, *dbGatedSMTracker) {
	plain := &dbSMTracker{machines: make(map[dbSMKey]*dbKVStateMachine)}
	gated := &dbGatedSMTracker{machines: make(map[dbSMKey]*dbGatedKVStateMachine)}
	createFn := func(shardID, replicaID uint64) dbSM.IStateMachine {
		inner := newDBKVStateMachine()
		machine := &dbGatedKVStateMachine{
			dbKVStateMachine: inner,
			entered:          make(chan struct{}),
			release:          release,
		}
		key := dbSMKey{shardID, replicaID}
		plain.mu.Lock()
		plain.machines[key] = inner
		plain.mu.Unlock()
		gated.mu.Lock()
		gated.machines[key] = machine
		gated.mu.Unlock()
		return machine
	}
	return createFn, plain, gated
}

// ---------------------------------------------------------------------------
// mTLS Helpers
// ---------------------------------------------------------------------------

// dbMTLSFiles is one node's mTLS material on disk, for the adapter's
// file-based NodeHostConfig. Every leaf is a distinct node identity
// (NodeHost.NodeIdentity), which is what a host with a transport is admitted
// by; a shared leaf is a shared identity and is refused at StartReplica.
type dbMTLSFiles = testpki.Files

// dbTestPKI is the CA of one test cluster; every host in it takes its own
// leaf from here (leaf). A host that restarts must be given the same files
// again to keep the identity the shard committed for it.
type dbTestPKI struct {
	ca  *testpki.CA
	dir string
}

func dbTestPKIFor(t *testing.T) *dbTestPKI {
	t.Helper()
	return &dbTestPKI{ca: testpki.NewCA(t), dir: t.TempDir()}
}

// leaf issues a fresh leaf for one host.
func (p *dbTestPKI) leaf(t *testing.T) dbMTLSFiles {
	t.Helper()
	return p.ca.LeafFiles(t, p.dir)
}

// kernelAddr binds a kernel-assigned loopback port; read it from NodeHost.RaftAddress.
const kernelAddr = "127.0.0.1:0"

// dbTestNodeHostConfig creates a dragonboat NodeHostConfig for
// integration testing with mTLS.
func dbTestNodeHostConfig(t *testing.T, listenAddr string, mtls dbMTLSFiles) dbConfig.NodeHostConfig {
	t.Helper()
	return dbConfig.NodeHostConfig{
		WALDir:         t.TempDir(),
		NodeHostDir:    t.TempDir(),
		RaftAddress:    listenAddr,
		ListenAddress:  listenAddr,
		RTTMillisecond: testRTTMillisecond,
		DeploymentID:   1,
		MutualTLS:      true,
		CAFile:         mtls.CAFile,
		CertFile:       mtls.CertFile,
		KeyFile:        mtls.KeyFile,
	}
}

// dbTestShardConfig creates a dragonboat per-shard config for testing.
func dbTestShardConfig(shardID, replicaID uint64) dbConfig.Config {
	return dbConfig.Config{
		ShardID:             shardID,
		ReplicaID:           replicaID,
		ElectionRTT:         testElectionRTT,
		HeartbeatRTT:        1,
		CheckQuorum:         true,
		PreVote:             true,
		OrderedConfigChange: true,
	}
}

// dbTestShardConfigWithSnapshot creates a per-shard config with
// aggressive snapshot settings.
func dbTestShardConfigWithSnapshot(shardID, replicaID, snapshotEntries, compactionOverhead uint64) dbConfig.Config {
	return dbConfig.Config{
		ShardID:             shardID,
		ReplicaID:           replicaID,
		ElectionRTT:         testElectionRTT,
		HeartbeatRTT:        1,
		CheckQuorum:         true,
		PreVote:             true,
		OrderedConfigChange: true,
		SnapshotEntries:     snapshotEntries,
		CompactionOverhead:  compactionOverhead,
	}
}

// ---------------------------------------------------------------------------
// Test Cluster (Dragonboat Adapter)
// ---------------------------------------------------------------------------

// dbTestCluster manages a group of dragonboat NodeHosts for multi-node
// integration tests. Each NodeHost runs on a separate listen address
// with real QUIC transport via mTLS.
type dbTestCluster struct {
	hosts    []*dragonboat.NodeHost
	hostsMu  sync.RWMutex
	addrs    []string
	trackers []*dbSMTracker
	stopped  []atomic.Bool
	pki      *dbTestPKI
	t        *testing.T
}

// identities is Config.MemberIdentities for a shard whose replica i+1 is
// served by host i, for every host.
func (c *dbTestCluster) identities() map[uint64]string {
	ids := make(map[uint64]string, len(c.hosts))
	for i, nh := range c.hosts {
		ids[uint64(i+1)] = nh.NodeIdentity()
	}
	return ids
}

// shardConfig is dbTestShardConfig with the cluster's identities filled in.
func (c *dbTestCluster) shardConfig(shardID, replicaID uint64) dbConfig.Config {
	cfg := dbTestShardConfig(shardID, replicaID)
	cfg.MemberIdentities = c.identities()
	return cfg
}

// newDBTestCluster creates an N-node cluster using the dragonboat
// adapter API with real QUIC transport and mTLS.
func newDBTestCluster(t *testing.T, numNodes int) *dbTestCluster {
	t.Helper()

	pki := dbTestPKIFor(t)
	addrs := make([]string, numNodes)
	hosts := make([]*dragonboat.NodeHost, numNodes)

	// Each host binds a kernel-assigned port; the member map is built from
	// the bound addresses before any shard starts.
	for i := range numNodes {
		nhConfig := dbTestNodeHostConfig(t, kernelAddr, pki.leaf(t))
		nh, err := dragonboat.NewNodeHostWithOptions(nhConfig,
			quicraft.WithMemoryLogDB(), quicraft.WithoutEncryption(),
		)
		if err != nil {
			for j := 0; j < i; j++ {
				hosts[j].Close()
			}
			t.Fatalf("NewNodeHostWithOptions[%d] failed: %v", i, err)
		}
		addrs[i] = nh.RaftAddress()
		hosts[i] = nh
	}

	return &dbTestCluster{
		hosts:   hosts,
		addrs:   addrs,
		stopped: make([]atomic.Bool, numNodes),
		pki:     pki,
		t:       t,
	}
}

// startShard starts a shard on all nodes in the cluster using the
// dragonboat adapter API. Returns smTrackers for each node.
func (c *dbTestCluster) startShard(shardID uint64) []*dbSMTracker {
	c.t.Helper()

	members := make(map[uint64]dragonboat.Target, len(c.hosts))
	for i, addr := range c.addrs {
		replicaID := uint64(i + 1)
		members[replicaID] = addr
	}

	trackers := make([]*dbSMTracker, len(c.hosts))
	for i, nh := range c.hosts {
		if c.stopped[i].Load() {
			continue
		}
		replicaID := uint64(i + 1)
		createFn, tracker := dbKVCreateFunc()
		trackers[i] = tracker

		cfg := c.shardConfig(shardID, replicaID)
		if err := nh.StartReplica(members, false, createFn, cfg); err != nil {
			c.t.Fatalf("StartReplica[%d] shard=%d failed: %v", i, shardID, err)
		}
	}

	c.hostsMu.Lock()
	c.trackers = trackers
	c.hostsMu.Unlock()
	return trackers
}

// startShardWithConfig starts a shard on all nodes using a custom
// config factory.
func (c *dbTestCluster) startShardWithConfig(shardID uint64, cfgFn func(shardID, replicaID uint64) dbConfig.Config) []*dbSMTracker {
	c.t.Helper()

	members := make(map[uint64]dragonboat.Target, len(c.hosts))
	for i, addr := range c.addrs {
		replicaID := uint64(i + 1)
		members[replicaID] = addr
	}

	trackers := make([]*dbSMTracker, len(c.hosts))
	for i, nh := range c.hosts {
		if c.stopped[i].Load() {
			continue
		}
		replicaID := uint64(i + 1)
		createFn, tracker := dbKVCreateFunc()
		trackers[i] = tracker

		cfg := cfgFn(shardID, replicaID)
		cfg.MemberIdentities = c.identities()
		if err := nh.StartReplica(members, false, createFn, cfg); err != nil {
			c.t.Fatalf("StartReplica[%d] shard=%d failed: %v", i, shardID, err)
		}
	}

	c.hostsMu.Lock()
	c.trackers = trackers
	c.hostsMu.Unlock()
	return trackers
}

// startGatedShard starts a shard on all nodes using gated KV state machines
// that park on release. It returns the plain trackers (usable with every
// existing helper) alongside the gated trackers.
func (c *dbTestCluster) startGatedShard(
	shardID uint64,
	release <-chan struct{},
	cfgFn func(shardID, replicaID uint64) dbConfig.Config,
) ([]*dbSMTracker, []*dbGatedSMTracker) {
	c.t.Helper()

	members := make(map[uint64]dragonboat.Target, len(c.hosts))
	for i, addr := range c.addrs {
		replicaID := uint64(i + 1)
		members[replicaID] = addr
	}

	trackers := make([]*dbSMTracker, len(c.hosts))
	gated := make([]*dbGatedSMTracker, len(c.hosts))
	for i, nh := range c.hosts {
		if c.stopped[i].Load() {
			continue
		}
		replicaID := uint64(i + 1)
		createFn, plainTracker, gatedTracker := dbGatedKVCreateFunc(release)
		trackers[i] = plainTracker
		gated[i] = gatedTracker

		cfg := cfgFn(shardID, replicaID)
		cfg.MemberIdentities = c.identities()
		if err := nh.StartReplica(members, false, createFn, cfg); err != nil {
			c.t.Fatalf("StartReplica[%d] shard=%d failed: %v", i, shardID, err)
		}
	}

	c.hostsMu.Lock()
	c.trackers = trackers
	c.hostsMu.Unlock()
	return trackers, gated
}

// startConcurrentShard starts a shard on all nodes using the concurrent
// state machine variant. Returns the concurrent SM trackers.
func (c *dbTestCluster) startConcurrentShard(shardID uint64) []*dbConcurrentSMTracker {
	c.t.Helper()

	members := make(map[uint64]dragonboat.Target, len(c.hosts))
	for i, addr := range c.addrs {
		replicaID := uint64(i + 1)
		members[replicaID] = addr
	}

	trackers := make([]*dbConcurrentSMTracker, len(c.hosts))
	for i, nh := range c.hosts {
		if c.stopped[i].Load() {
			continue
		}
		replicaID := uint64(i + 1)
		createFn, tracker := dbConcurrentKVCreateFunc()
		trackers[i] = tracker

		cfg := c.shardConfig(shardID, replicaID)
		if err := nh.StartConcurrentReplica(members, false, createFn, cfg); err != nil {
			c.t.Fatalf("StartConcurrentReplica[%d] shard=%d failed: %v", i, shardID, err)
		}
	}
	return trackers
}

// startOnDiskShard starts a shard on all nodes using the on-disk state
// machine variant. Returns the on-disk SM trackers.
func (c *dbTestCluster) startOnDiskShard(shardID uint64) []*dbOnDiskSMTracker {
	c.t.Helper()

	members := make(map[uint64]dragonboat.Target, len(c.hosts))
	for i, addr := range c.addrs {
		replicaID := uint64(i + 1)
		members[replicaID] = addr
	}

	trackers := make([]*dbOnDiskSMTracker, len(c.hosts))
	for i, nh := range c.hosts {
		if c.stopped[i].Load() {
			continue
		}
		replicaID := uint64(i + 1)
		createFn, tracker := dbOnDiskKVCreateFunc()
		trackers[i] = tracker

		cfg := c.shardConfig(shardID, replicaID)
		if err := nh.StartOnDiskReplica(members, false, createFn, cfg); err != nil {
			c.t.Fatalf("StartOnDiskReplica[%d] shard=%d failed: %v", i, shardID, err)
		}
	}
	return trackers
}

// close shuts down all hosts; NodeHost.Close returns no error.
func (c *dbTestCluster) close() {
	c.hostsMu.Lock()
	defer c.hostsMu.Unlock()
	for i, nh := range c.hosts {
		if c.stopped[i].Load() {
			continue
		}
		nh.Close()
	}
}

// findLeader returns the index of the node that is the current leader
// for the given shard, or -1 if no leader is known.
func (c *dbTestCluster) findLeader(shardID uint64) int {
	c.hostsMu.RLock()
	defer c.hostsMu.RUnlock()
	for i, nh := range c.hosts {
		if c.stopped[i].Load() {
			continue
		}
		leaderID, _, valid, err := nh.GetLeaderID(shardID)
		if err != nil || !valid || leaderID == 0 {
			continue
		}
		// Check if this node IS the leader.
		replicaID := uint64(i + 1)
		if leaderID == replicaID {
			return i
		}
	}
	return -1
}

// waitForClusterLeader waits until one of the nodes in the cluster
// becomes leader for the given shard.
func (c *dbTestCluster) waitForClusterLeader(shardID uint64, timeout time.Duration) int {
	c.t.Helper()
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	deadline := time.NewTimer(timeout)
	defer deadline.Stop()
	for {
		idx := c.findLeader(shardID)
		if idx >= 0 {
			return idx
		}
		select {
		case <-deadline.C:
			c.t.Fatalf("timed out after %v waiting for cluster leader on shard %d", timeout, shardID)
			return -1
		case <-ticker.C:
		}
	}
}

// Leader-resolving proposals: resolve the leader on every attempt and retry only the not-leader rejection (ErrShardNotReady here).

// dbProposal is a proposal's result and the replica that accepted it.
type dbProposal struct {
	// Result is the value the state machine's Update returned.
	Result dbSM.Result
	// Host is the NodeHost that accepted the entry.
	Host *dragonboat.NodeHost
	// Index is Host's position in the cluster's hosts slice, for callers
	// that need the matching replica ID or state-machine tracker.
	Index int
}

// dbAsyncProposal is dbProposal for the asynchronous Propose path, which
// returns a RequestState the caller polls instead of a Result.
type dbAsyncProposal struct {
	// RS is the in-flight request. The caller owns it and must Release it.
	RS *dragonboat.RequestState
	// Host is the NodeHost that accepted the entry.
	Host *dragonboat.NodeHost
	// Index is Host's position in the cluster's hosts slice.
	Index int
}

// leaderRetryInterval is how long a leader-resolving helper waits before
// re-resolving after a not-leader rejection. An election completes in roughly
// one election timeout, so polling faster than this only burns CPU.
const leaderRetryInterval = 20 * time.Millisecond

// minLeaderAttempt is the smallest budget an attempt will be started with.
// Below it the attempt would time out on arrival and overwrite the real cause
// with ErrTimeout, reporting leadership churn as a slow shard.
const minLeaderAttempt = 200 * time.Millisecond

// leaderHost returns the NodeHost leading shardID now and its index, or (nil, -1); do not hold on to it.
func (c *dbTestCluster) leaderHost(shardID uint64) (*dragonboat.NodeHost, int) {
	idx := c.findLeader(shardID)
	if idx < 0 {
		return nil, -1
	}
	c.hostsMu.RLock()
	defer c.hostsMu.RUnlock()
	return c.hosts[idx], idx
}

// waitForLeaderHost waits for a leader on shardID and returns that NodeHost, for sites that address one replica on purpose.
func (c *dbTestCluster) waitForLeaderHost(shardID uint64, timeout time.Duration) *dragonboat.NodeHost {
	c.t.Helper()
	idx := c.waitForClusterLeader(shardID, timeout)
	c.hostsMu.RLock()
	defer c.hostsMu.RUnlock()
	return c.hosts[idx]
}

// isNotLeaderRejection reports whether err is the adapter's not-leader rejection (ErrShardNotReady), the only error the helpers retry.
func isNotLeaderRejection(err error) bool {
	return errors.Is(err, dragonboat.ErrShardNotReady) ||
		// A raw quicraft error can still reach a test through a path that
		// does not run translateError.
		errors.Is(err, quicraft.ErrNotLeader)
}

// driveToLeader is the one retry loop behind every leader-resolving helper; it resolves before each attempt and gives each at least minLeaderAttempt.
func driveToLeader[T any](
	c *dbTestCluster,
	shardID uint64,
	timeout time.Duration,
	attempt func(nh *dragonboat.NodeHost, idx int, budget time.Duration) (T, error),
) (T, error) {
	var zero T
	deadline := time.Now().Add(timeout)
	retry := time.NewTicker(leaderRetryInterval)
	defer retry.Stop()
	expiry := time.NewTimer(timeout)
	defer expiry.Stop()

	// Reported if the deadline passes without a leader ever being observed,
	// which is itself a not-leader condition.
	lastErr := dragonboat.ErrShardNotReady

	for {
		if nh, idx := c.leaderHost(shardID); nh != nil {
			remaining := time.Until(deadline)
			if remaining < minLeaderAttempt {
				return zero, lastErr
			}
			result, err := attempt(nh, idx, remaining)
			if err == nil {
				return result, nil
			}
			if !isNotLeaderRejection(err) {
				return zero, err
			}
			lastErr = err
		}
		select {
		case <-expiry.C:
			return zero, lastErr
		case <-retry.C:
		}
	}
}

// proposeOnce submits cmd to nh and waits for the entry to commit. It does NOT
// resolve the leader and does NOT retry: it is the single attempt the retrying
// helpers are built from, and the way a test addresses one specific replica on
// purpose.
func proposeOnce(
	nh *dragonboat.NodeHost,
	session *dbClient.Session,
	cmd []byte,
	timeout time.Duration,
) (dbSM.Result, error) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	//quicraft:direct-propose the single attempt tryProposeToLeader is built from
	return nh.SyncPropose(ctx, session, cmd)
}

// proposeToLeader proposes cmd to the session shard's current leader, waits for the commit and fails the test on error; a site that means one replica keeps a direct call with a quicraft:direct-propose marker (see test/buildgates in the parent module).
func (c *dbTestCluster) proposeToLeader(
	t *testing.T,
	session *dbClient.Session,
	cmd []byte,
	timeout time.Duration,
) dbProposal {
	t.Helper()
	proposal, err := c.tryProposeToLeader(session, cmd, timeout)
	if err != nil {
		t.Fatalf("propose to the leader of shard %d failed within %v: %v",
			session.ShardID, timeout, err)
	}
	return proposal
}

// tryProposeToLeader is proposeToLeader returning the error; only the not-leader rejection is retried, by re-resolving.
func (c *dbTestCluster) tryProposeToLeader(
	session *dbClient.Session,
	cmd []byte,
	timeout time.Duration,
) (dbProposal, error) {
	return driveToLeader(c, session.ShardID, timeout,
		func(nh *dragonboat.NodeHost, idx int, budget time.Duration) (dbProposal, error) {
			result, err := proposeOnce(nh, session, cmd, budget)
			if err != nil {
				return dbProposal{}, err
			}
			return dbProposal{Result: result, Host: nh, Index: idx}, nil
		})
}

// proposeAsyncToLeader retries only the submission and returns the RequestState, which the caller must Release.
func (c *dbTestCluster) proposeAsyncToLeader(
	t *testing.T,
	session *dbClient.Session,
	cmd []byte,
	timeout time.Duration,
) dbAsyncProposal {
	t.Helper()
	proposal, err := c.tryProposeAsyncToLeader(session, cmd, timeout)
	if err != nil {
		t.Fatalf("async propose to the leader of shard %d failed within %v: %v",
			session.ShardID, timeout, err)
	}
	return proposal
}

// tryProposeAsyncToLeader is proposeAsyncToLeader's error-returning form.
func (c *dbTestCluster) tryProposeAsyncToLeader(
	session *dbClient.Session,
	cmd []byte,
	timeout time.Duration,
) (dbAsyncProposal, error) {
	return driveToLeader(c, session.ShardID, timeout,
		func(nh *dragonboat.NodeHost, idx int, budget time.Duration) (dbAsyncProposal, error) {
			//quicraft:direct-propose the single attempt this loop is built from
			rs, err := nh.Propose(session, cmd, budget)
			if err != nil {
				return dbAsyncProposal{}, err
			}
			return dbAsyncProposal{RS: rs, Host: nh, Index: idx}, nil
		})
}

// Leader-resolving linearizable reads share the proposal helpers' retry rule and loop.

// readOnce issues one linearizable read of query against nh with its own
// deadline. It does NOT resolve the leader and does NOT retry: it is the single
// attempt tryReadFromLeader is built from, and the way a test addresses one
// specific replica on purpose.
func readOnce(
	nh *dragonboat.NodeHost,
	shardID uint64,
	query interface{},
	timeout time.Duration,
) (interface{}, error) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	//quicraft:direct-read the single attempt tryReadFromLeader is built from
	return nh.SyncRead(ctx, shardID, query)
}

// readFromLeader reads query from shardID's current leader and fails the test on error; a site that means one replica keeps a direct call with a quicraft:direct-read marker.
func (c *dbTestCluster) readFromLeader(
	t *testing.T,
	shardID uint64,
	query interface{},
	timeout time.Duration,
) interface{} {
	t.Helper()
	result, err := c.tryReadFromLeader(shardID, query, timeout)
	if err != nil {
		t.Fatalf("linearizable read on shard %d failed within %v: %v",
			shardID, timeout, err)
	}
	return result
}

// tryReadFromLeader is readFromLeader returning the error; only the not-leader rejection is retried, by re-resolving.
func (c *dbTestCluster) tryReadFromLeader(
	shardID uint64,
	query interface{},
	timeout time.Duration,
) (interface{}, error) {
	return driveToLeader(c, shardID, timeout,
		func(nh *dragonboat.NodeHost, _ int, budget time.Duration) (interface{}, error) {
			return readOnce(nh, shardID, query, budget)
		})
}

// Idempotent config changes: "already exists"/"not found" or a stale ConfigChangeID is success only after a submitted attempt and a linearizable read showing the exact change.

// configChangeKind selects one of the adapter's four synchronous membership
// calls.
type configChangeKind int

const (
	addReplica configChangeKind = iota
	addNonVoting
	addWitness
	deleteReplica
)

// membershipRole is where a replica ID sits in a Membership.
type membershipRole int

const (
	roleNone membershipRole = iota
	roleVoter
	roleNonVoting
	roleWitness
)

// membershipRoleNames renders a membershipRole for a failure message.
var membershipRoleNames = map[membershipRole]string{
	roleNone:      "no role",
	roleVoter:     "voting member",
	roleNonVoting: "non-voting member",
	roleWitness:   "witness",
}

// configChangeRoles is the role each kind of change must leave the replica ID
// in for the change to count as landed.
var configChangeRoles = map[configChangeKind]membershipRole{
	addReplica:    roleVoter,
	addNonVoting:  roleNonVoting,
	addWitness:    roleWitness,
	deleteReplica: roleNone,
}

// configChangeNames are the adapter methods each kind submits through, for
// failure messages that read like the call the test made.
var configChangeNames = map[configChangeKind]string{
	addReplica:    "SyncRequestAddReplica",
	addNonVoting:  "SyncRequestAddNonVoting",
	addWitness:    "SyncRequestAddWitness",
	deleteReplica: "SyncRequestDeleteReplica",
}

// configChange is one requested membership change: what to do to which
// replica, at which address, against which configuration.
type configChange struct {
	kind           configChangeKind
	shardID        uint64
	replicaID      uint64
	addr           dragonboat.Target
	identity       string // the node identity an add commits; see dbIdentityFor
	configChangeID uint64
}

// dbIdentityFor is the node identity a test commits for a replica that no
// host serves — an add whose target never starts. It has the shape of a real
// one (NodeIdentity is "spki:" plus 64 hex digits) and is distinct per
// replica, since one keypair cannot be two members.
func dbIdentityFor(replicaID uint64) string {
	return fmt.Sprintf("spki:%064x", replicaID)
}

// String renders the change for log and failure messages.
func (cc configChange) String() string {
	if cc.kind == deleteReplica {
		return fmt.Sprintf("%s(shard=%d, replica=%d, configChangeID=%d)",
			configChangeNames[cc.kind], cc.shardID, cc.replicaID, cc.configChangeID)
	}
	return fmt.Sprintf("%s(shard=%d, replica=%d, addr=%s, configChangeID=%d)",
		configChangeNames[cc.kind], cc.shardID, cc.replicaID, cc.addr, cc.configChangeID)
}

// configChangeSubmitters dispatches a configChange to the adapter method that
// submits it. Each is the single attempt driveConfigChange is built from; the
// gate in test/buildgates knows them by the marker.
var configChangeSubmitters = map[configChangeKind]func(context.Context, *dragonboat.NodeHost, configChange) error{
	addReplica: func(ctx context.Context, nh *dragonboat.NodeHost, cc configChange) error {
		return nh.SyncRequestAddReplicaWithIdentity(ctx, cc.shardID, cc.replicaID, cc.addr, cc.identity, cc.configChangeID) //quicraft:direct-config-change the single attempt driveConfigChange is built from
	},
	addNonVoting: func(ctx context.Context, nh *dragonboat.NodeHost, cc configChange) error {
		return nh.SyncRequestAddNonVotingWithIdentity(ctx, cc.shardID, cc.replicaID, cc.addr, cc.identity, cc.configChangeID) //quicraft:direct-config-change the single attempt driveConfigChange is built from
	},
	addWitness: func(ctx context.Context, nh *dragonboat.NodeHost, cc configChange) error {
		return nh.SyncRequestAddWitnessWithIdentity(ctx, cc.shardID, cc.replicaID, cc.addr, cc.identity, cc.configChangeID) //quicraft:direct-config-change the single attempt driveConfigChange is built from
	},
	deleteReplica: func(ctx context.Context, nh *dragonboat.NodeHost, cc configChange) error {
		return nh.SyncRequestDeleteReplica(ctx, cc.shardID, cc.replicaID, cc.configChangeID) //quicraft:direct-config-change the single attempt driveConfigChange is built from
	},
}

// roleOf reports the role replicaID holds in m and the address recorded for
// it. A nil membership holds nobody.
func roleOf(m *dragonboat.Membership, replicaID uint64) (membershipRole, dragonboat.Target) {
	if m == nil {
		return roleNone, ""
	}
	if addr, ok := m.Nodes[replicaID]; ok {
		return roleVoter, addr
	}
	if addr, ok := m.NonVotings[replicaID]; ok {
		return roleNonVoting, addr
	}
	if addr, ok := m.Witnesses[replicaID]; ok {
		return roleWitness, addr
	}
	return roleNone, ""
}

// membershipConflictError reports a replica ID held in a role or address incompatible with the requested change.
type membershipConflictError struct {
	change configChange
	role   membershipRole
	addr   dragonboat.Target
}

func (e *membershipConflictError) Error() string {
	return fmt.Sprintf("replica %d is a %s at %q, not the %s at %q that %s requested: "+
		"the replica ID is held in a state this change did not produce, which is a "+
		"membership conflict and not a landed retry",
		e.change.replicaID, membershipRoleNames[e.role], e.addr,
		membershipRoleNames[configChangeRoles[e.change.kind]], e.change.addr, e.change)
}

// landed reports whether m shows cc in effect: an add in the exact role and address, a delete with no role.
func (cc configChange) landed(m *dragonboat.Membership) (bool, error) {
	role, addr := roleOf(m, cc.replicaID)
	want := configChangeRoles[cc.kind]
	if want == roleNone {
		return role == roleNone, nil
	}
	if role == roleNone {
		return false, nil
	}
	if role != want || addr != cc.addr {
		return false, &membershipConflictError{change: cc, role: role, addr: addr}
	}
	return true, nil
}

// configChangeFailedError is a membership change driveConfigChange gave up
// on: a non-retryable refusal from an attempt, or a conflict in the
// membership read.
type configChangeFailedError struct {
	change configChange
	err    error
}

func (e *configChangeFailedError) Error() string {
	return fmt.Sprintf("%s failed: %v", e.change, e.err)
}

func (e *configChangeFailedError) Unwrap() error { return e.err }

// configChangeExhaustedError is a membership change that never landed within
// its budget. last is what the final attempt produced.
type configChangeExhaustedError struct {
	change configChange
	budget time.Duration
	last   error
}

func (e *configChangeExhaustedError) Error() string {
	return fmt.Sprintf("%s never landed within %v: %v", e.change, e.budget, e.last)
}

func (e *configChangeExhaustedError) Unwrap() error { return e.last }

// errNoLeaderObserved is what an attempt reports when no live replica claims
// leadership at the moment of the call. It is this helper's own, NOT the
// adapter's ErrShardNotReady, because nothing was sent anywhere: it must be
// retried, and it must never arm the submission bit.
var errNoLeaderObserved = errors.New("integration: no live replica reports leadership")

// configChangeMaybeSubmitted reports whether err can only follow delivery to Raft: ErrTimeout or quicraft.MaybeApplied, not a bare ErrShardNotReady or ErrCanceled.
func configChangeMaybeSubmitted(err error) bool {
	return errors.Is(err, dragonboat.ErrTimeout) || quicraft.MaybeApplied(err)
}

// configChangeRetryableSentinels are errors that clear on their own; ErrRejected is retried only here, where it means a pending config change.
var configChangeRetryableSentinels = [...]error{
	dragonboat.ErrShardNotReady,
	dragonboat.ErrSystemBusy,
	dragonboat.ErrTimeout,
	dragonboat.ErrRejected,
	errNoLeaderObserved,
	// A transfer in flight, refused before the log; the quicraft sentinel arrives wrapped in *AdapterError and matches through errors.Is.
	quicraft.ErrTransferInProgress,
}

// configChangeRetryable reports whether one more attempt at a membership
// change is worth making after err.
func configChangeRetryable(err error) bool {
	if err == nil {
		return false
	}
	for _, s := range configChangeRetryableSentinels {
		if errors.Is(err, s) {
			return true
		}
	}
	return false
}

// driveConfigChange runs attempt until cc is in effect, the budget runs out, or a non-retryable error returns; config_change_helper_test.go tests it without a cluster.
func driveConfigChange(
	cc configChange,
	budget time.Duration,
	attempt func(context.Context) error,
	readMembership func(context.Context) (*dragonboat.Membership, error),
) error {
	deadline := time.Now().Add(budget)
	retry := time.NewTicker(leaderRetryInterval)
	defer retry.Stop()
	expiry := time.NewTimer(budget)
	defer expiry.Stop()

	submitted := false
	lastErr := errNoLeaderObserved

	for {
		remaining := time.Until(deadline)
		if remaining < minLeaderAttempt {
			return &configChangeExhaustedError{change: cc, budget: budget, last: lastErr}
		}
		ctx, cancel := context.WithTimeout(context.Background(), min(remaining, defaultTimeout))
		err := attempt(ctx)
		cancel()
		if err == nil {
			return nil
		}

		// Latch the bit for the whole loop, not just this attempt: the entry
		// an earlier attempt submitted is what a later "already exists" is
		// reporting.
		if configChangeMaybeSubmitted(err) {
			submitted = true
		}

		if submitted {
			landed, conflict, readErr := observeConfigChange(cc, deadline, readMembership)
			if conflict != nil {
				return &configChangeFailedError{change: cc, err: conflict}
			}
			if landed {
				return nil
			}
			// A read that failed transiently is not evidence either way, so
			// a refusal it was meant to check is not final yet: ask again.
			if readErr != nil {
				err = readErr
			}
		}

		if !configChangeRetryable(err) {
			return &configChangeFailedError{change: cc, err: err}
		}
		lastErr = err
		select {
		case <-expiry.C:
			return &configChangeExhaustedError{change: cc, budget: budget, last: lastErr}
		case <-retry.C:
		}
	}
}

// observeConfigChange takes one linearizable membership read and asks whether
// cc has landed in it. A read that fails is reported as readErr rather than
// as an answer.
func observeConfigChange(
	cc configChange,
	deadline time.Time,
	readMembership func(context.Context) (*dragonboat.Membership, error),
) (landed bool, conflict, readErr error) {
	ctx, cancel := context.WithTimeout(context.Background(), max(min(time.Until(deadline), defaultTimeout), minLeaderAttempt))
	defer cancel()

	m, err := readMembership(ctx)
	if err != nil {
		return false, nil, err
	}
	landed, conflict = cc.landed(m)
	return landed, conflict, nil
}

// leaderResolver names the replica leading shardID now, with its index, or (nil, -1).
type leaderResolver func(shardID uint64) (*dragonboat.NodeHost, int)

// configChangeVia applies cc through whichever replica resolve names at each
// attempt, and returns nil once a linearizable read of the membership shows
// it in effect. Both the attempt and the confirming read resolve at the
// point of use; a moment with no leader is errNoLeaderObserved, which the
// loop treats as "ask again" and never as a submission.
func configChangeVia(resolve leaderResolver, cc configChange, budget time.Duration) error {
	attempt := func(ctx context.Context) error {
		nh, _ := resolve(cc.shardID)
		if nh == nil {
			return errNoLeaderObserved
		}
		return configChangeSubmitters[cc.kind](ctx, nh, cc)
	}
	return driveConfigChange(cc, budget, attempt, membershipReaderVia(resolve, cc.shardID))
}

// tryConfigChangeToLeader is configChangeVia over the cluster's own leader
// resolution. It is the error-returning form, for callers with resources to
// release before failing the test.
func (c *dbTestCluster) tryConfigChangeToLeader(cc configChange, budget time.Duration) error {
	return configChangeVia(c.leaderHost, cc, budget)
}

// membershipReaderVia returns a linearizable membership read that resolves
// the leader through resolve at the point of use.
func membershipReaderVia(resolve leaderResolver, shardID uint64) func(context.Context) (*dragonboat.Membership, error) {
	return func(ctx context.Context) (*dragonboat.Membership, error) {
		nh, _ := resolve(shardID)
		if nh == nil {
			return nil, errNoLeaderObserved
		}
		return nh.SyncGetShardMembership(ctx, shardID)
	}
}

// configChangeToLeader is tryConfigChangeToLeader for the test goroutine: it
// fails the test on any error.
func (c *dbTestCluster) configChangeToLeader(t *testing.T, cc configChange, budget time.Duration) {
	t.Helper()
	if err := c.tryConfigChangeToLeader(cc, budget); err != nil {
		t.Fatal(err)
	}
}

// addReplicaToLeader adds replicaID as a voting member at addr, against
// configChangeID, and fails the test unless the change lands.
func (c *dbTestCluster) addReplicaToLeader(t *testing.T, shardID, replicaID uint64, addr dragonboat.Target, identity string, configChangeID uint64, budget time.Duration) {
	t.Helper()
	c.configChangeToLeader(t, configChange{kind: addReplica, shardID: shardID, replicaID: replicaID, addr: addr, identity: identity, configChangeID: configChangeID}, budget)
}

// addNonVotingToLeader adds replicaID as a non-voting member at addr, against
// configChangeID, and fails the test unless the change lands.
func (c *dbTestCluster) addNonVotingToLeader(t *testing.T, shardID, replicaID uint64, addr dragonboat.Target, identity string, configChangeID uint64, budget time.Duration) {
	t.Helper()
	c.configChangeToLeader(t, configChange{kind: addNonVoting, shardID: shardID, replicaID: replicaID, addr: addr, identity: identity, configChangeID: configChangeID}, budget)
}

// addWitnessToLeader adds replicaID as a witness at addr, against
// configChangeID, and fails the test unless the change lands.
func (c *dbTestCluster) addWitnessToLeader(t *testing.T, shardID, replicaID uint64, addr dragonboat.Target, identity string, configChangeID uint64, budget time.Duration) {
	t.Helper()
	c.configChangeToLeader(t, configChange{kind: addWitness, shardID: shardID, replicaID: replicaID, addr: addr, identity: identity, configChangeID: configChangeID}, budget)
}

// deleteReplicaToLeader removes replicaID, against configChangeID, and fails
// the test unless the change lands.
func (c *dbTestCluster) deleteReplicaToLeader(t *testing.T, shardID, replicaID, configChangeID uint64, budget time.Duration) {
	t.Helper()
	c.configChangeToLeader(t, configChange{kind: deleteReplica, shardID: shardID, replicaID: replicaID, configChangeID: configChangeID}, budget)
}

// membershipFromLeader reads membership linearizably from the current leader, retrying the not-leader refusal, and fails the test on anything else.
func (c *dbTestCluster) membershipFromLeader(t *testing.T, shardID uint64, budget time.Duration) *dragonboat.Membership {
	t.Helper()
	deadline := time.Now().Add(budget)
	retry := time.NewTicker(leaderRetryInterval)
	defer retry.Stop()
	expiry := time.NewTimer(budget)
	defer expiry.Stop()
	read := membershipReaderVia(c.leaderHost, shardID)

	lastErr := errNoLeaderObserved
	for {
		remaining := time.Until(deadline)
		if remaining < minLeaderAttempt {
			break
		}
		ctx, cancel := context.WithTimeout(context.Background(), min(remaining, defaultTimeout))
		m, err := read(ctx)
		cancel()
		if err == nil {
			return m
		}
		if !isNotLeaderRejection(err) && !errors.Is(err, errNoLeaderObserved) {
			t.Fatalf("SyncGetShardMembership(shard=%d) failed: %v", shardID, err)
		}
		lastErr = err
		select {
		case <-expiry.C:
			t.Fatalf("SyncGetShardMembership(shard=%d) never succeeded within %v: %v", shardID, budget, lastErr)
		case <-retry.C:
		}
	}
	t.Fatalf("SyncGetShardMembership(shard=%d) never succeeded within %v: %v", shardID, budget, lastErr)
	return nil
}

// transferLeadershipTo requests the transfer of the current leader each round until targetReplicaID reports leading; each request is logged.
func (c *dbTestCluster) transferLeadershipTo(t *testing.T, shardID, targetReplicaID uint64, budget time.Duration) {
	t.Helper()
	targetIdx := int(targetReplicaID) - 1
	started := time.Now()
	expiry := time.NewTimer(budget)
	defer expiry.Stop()
	poll := time.NewTicker(10 * time.Millisecond)
	defer poll.Stop()

	var nextRequest time.Time
	requests := 0
	var lastRefusal error
	for {
		if c.findLeader(shardID) == targetIdx {
			t.Logf("replica %d leads shard %d after %v and %d transfer request(s)",
				targetReplicaID, shardID, time.Since(started).Round(time.Millisecond), requests)
			return
		}
		if !time.Now().Before(nextRequest) {
			if nh, idx := c.leaderHost(shardID); nh != nil {
				requests++
				err := nh.RequestLeaderTransfer(shardID, targetReplicaID)
				if err == nil {
					t.Logf("transfer request %d to replica %d accepted by node[%d] at %v",
						requests, targetReplicaID, idx, time.Since(started).Round(time.Millisecond))
				} else if isNotLeaderRejection(err) || errors.Is(err, quicraft.ErrTransferInProgress) {
					lastRefusal = err
				} else {
					t.Fatalf("RequestLeaderTransfer(shard=%d, target=%d) on node[%d] failed: %v",
						shardID, targetReplicaID, idx, err)
				}
				nextRequest = time.Now().Add(leaderTransferRound)
			}
		}
		select {
		case <-expiry.C:
			t.Fatalf("replica %d never led shard %d within %v: %d transfer request(s), "+
				"leader now node[%d], last refusal: %v",
				targetReplicaID, shardID, budget, requests, c.findLeader(shardID), lastRefusal)
		case <-poll.C:
		}
	}
}

// stopNode stops the host at the given index.
func (c *dbTestCluster) stopNode(idx int) {
	c.t.Helper()
	if c.stopped[idx].Load() {
		return
	}
	c.hostsMu.Lock()
	nh := c.hosts[idx]
	c.hostsMu.Unlock()
	nh.Close()
	c.stopped[idx].Store(true)
}

// ---------------------------------------------------------------------------
// Wait Helpers
// ---------------------------------------------------------------------------

// waitForCondition polls until the predicate returns true or the timeout
// expires.
func waitForCondition(t *testing.T, timeout time.Duration, desc string, predicate func() bool) {
	t.Helper()
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	deadline := time.NewTimer(timeout)
	defer deadline.Stop()
	for {
		if predicate() {
			return
		}
		select {
		case <-deadline.C:
			t.Fatalf("timed out after %v waiting for condition: %s", timeout, desc)
			return
		case <-ticker.C:
		}
	}
}

// liveNodes returns the indices of every node in the cluster that has not
// been stopped.
func (c *dbTestCluster) liveNodes() []int {
	c.hostsMu.RLock()
	numNodes := len(c.hosts)
	c.hostsMu.RUnlock()

	live := make([]int, 0, numNodes)
	for i := range numNodes {
		if !c.stopped[i].Load() {
			live = append(live, i)
		}
	}
	return live
}

// resolveStateMachine returns node i's state machine, from the cluster's live trackers first and the caller's slice second.
func resolveStateMachine(cluster *dbTestCluster, trackers []*dbSMTracker, shardID uint64, i int) *dbKVStateMachine {
	replicaID := uint64(i + 1)

	cluster.hostsMu.RLock()
	live := make([]*dbSMTracker, len(cluster.trackers))
	copy(live, cluster.trackers)
	cluster.hostsMu.RUnlock()

	if i < len(live) && live[i] != nil {
		if machine := live[i].Get(shardID, replicaID); machine != nil {
			return machine
		}
	}
	if i < len(trackers) && trackers[i] != nil {
		if machine := trackers[i].Get(shardID, replicaID); machine != nil {
			return machine
		}
	}
	return nil
}

// waitForReplication waits for key to hold value on every live node and fails if any node was not checked.
func waitForReplication(t *testing.T, cluster *dbTestCluster, trackers []*dbSMTracker, shardID uint64, key, expectedVal string, timeout time.Duration) {
	t.Helper()

	live := cluster.liveNodes()
	if len(live) == 0 {
		t.Fatalf("waitForReplication(key=%q): no live nodes to verify", key)
	}

	verified := 0
	for _, i := range live {
		machine := resolveStateMachine(cluster, trackers, shardID, i)
		if machine == nil {
			// The shard may not have registered its state machine yet; that
			// is a condition to wait for, not one to skip.
			waitForCondition(t, timeout,
				fmt.Sprintf("node[%d] registers a state machine for shard %d", i, shardID),
				func() bool {
					machine = resolveStateMachine(cluster, trackers, shardID, i)
					return machine != nil
				},
			)
		}
		waitForCondition(t, timeout,
			fmt.Sprintf("node[%d] replicates key=%q", i, key),
			func() bool {
				val, ok := machine.Get(key)
				return ok && val == expectedVal
			},
		)
		verified++
	}

	if verified < len(live) {
		t.Fatalf("waitForReplication(key=%q): verified %d nodes but %d are live",
			key, verified, len(live))
	}
}

// proposeAndWait proposes through the current leader and waits on the state machine of the replica that accepted it.
func proposeAndWait(t *testing.T, cluster *dbTestCluster, trackers []*dbSMTracker, shardID uint64, key, value string) {
	t.Helper()

	session := dbClient.NewNoOPSession(shardID, nil)
	cmd := encodeKVPut(key, value)
	proposal := cluster.proposeToLeader(t, session, cmd, defaultTimeout)

	// Wait for the SM of the replica that accepted the entry to apply it.
	leaderIdx := proposal.Index
	leaderSM := resolveStateMachine(cluster, trackers, shardID, leaderIdx)
	if leaderSM == nil {
		t.Fatalf("no state machine for leader node[%d] on shard %d", leaderIdx, shardID)
	}
	waitForCondition(t, defaultTimeout,
		fmt.Sprintf("leader applies key=%q", key),
		func() bool {
			val, ok := leaderSM.Get(key)
			return ok && val == value
		},
	)
}

// proposeNEntries proposes N entries through the leader and returns the
// expected key-value map.
func proposeNEntries(
	t *testing.T,
	cluster *dbTestCluster,
	trackers []*dbSMTracker,
	shardID uint64,
	n int,
	keyPrefix string,
) map[string]string {
	t.Helper()

	expected := make(map[string]string, n)
	for i := range n {
		key := fmt.Sprintf("%s-%03d", keyPrefix, i)
		value := fmt.Sprintf("val-%s-%03d", keyPrefix, i)
		proposeAndWait(t, cluster, trackers, shardID, key, value)
		expected[key] = value
	}
	return expected
}

// verifyAllData checks every expected pair on every live node and fails if any node was not checked.
func verifyAllData(
	t *testing.T,
	cluster *dbTestCluster,
	trackers []*dbSMTracker,
	shardID uint64,
	expected map[string]string,
) {
	t.Helper()

	live := cluster.liveNodes()
	if len(live) == 0 {
		t.Fatal("verifyAllData: no live nodes to verify")
	}

	verified := 0
	for _, i := range live {
		nodeSM := resolveStateMachine(cluster, trackers, shardID, i)
		if nodeSM == nil {
			waitForCondition(t, defaultTimeout,
				fmt.Sprintf("node[%d] registers a state machine for shard %d", i, shardID),
				func() bool {
					nodeSM = resolveStateMachine(cluster, trackers, shardID, i)
					return nodeSM != nil
				},
			)
		}
		for key, val := range expected {
			waitForCondition(t, defaultTimeout,
				fmt.Sprintf("node[%d] has key=%q", i, key),
				func() bool {
					got, ok := nodeSM.Get(key)
					return ok && got == val
				},
			)
		}
		verified++
	}

	if verified < len(live) {
		t.Fatalf("verifyAllData: verified %d nodes but %d are live", verified, len(live))
	}
}
