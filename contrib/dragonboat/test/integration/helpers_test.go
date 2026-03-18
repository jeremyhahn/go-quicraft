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
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/binary"
	"encoding/pem"
	"fmt"
	"io"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	dragonboat "github.com/jeremyhahn/go-quicraft/contrib/dragonboat"
	dbClient "github.com/jeremyhahn/go-quicraft/contrib/dragonboat/client"
	dbConfig "github.com/jeremyhahn/go-quicraft/contrib/dragonboat/config"
	dbSM "github.com/jeremyhahn/go-quicraft/contrib/dragonboat/statemachine"
	quicraft "github.com/jeremyhahn/go-quicraft/pkg"
)

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

// defaultTimeout is the standard timeout for integration test operations.
const defaultTimeout = 15 * time.Second

// leaderElectionTimeout is the time to wait for a leader election.
const leaderElectionTimeout = 5 * time.Second

// failoverTimeout is the extended timeout for failover scenarios.
const failoverTimeout = 20 * time.Second

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
// mTLS Helpers
// ---------------------------------------------------------------------------

// dbTestMTLSConfig generates an ephemeral ECDSA P-256 CA and leaf
// certificate pair, writes them to temporary files, and returns the
// file paths and PEM data for the dragonboat adapter's file-based
// mTLS configuration.
type dbMTLSFiles struct {
	CAFile   string
	CertFile string
	KeyFile  string
}

func dbTestMTLSFiles(t *testing.T) dbMTLSFiles {
	t.Helper()

	caKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generate CA key: %v", err)
	}

	caTemplate := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject: pkix.Name{
			Organization: []string{"QuicRaft DB Adapter Test"},
		},
		NotBefore:             time.Now().Add(-1 * time.Minute),
		NotAfter:              time.Now().Add(1 * time.Hour),
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageCRLSign,
		BasicConstraintsValid: true,
		IsCA:                  true,
	}

	caCertDER, err := x509.CreateCertificate(rand.Reader, caTemplate, caTemplate, &caKey.PublicKey, caKey)
	if err != nil {
		t.Fatalf("create CA cert: %v", err)
	}

	caCert, err := x509.ParseCertificate(caCertDER)
	if err != nil {
		t.Fatalf("parse CA cert: %v", err)
	}

	caCertPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: caCertDER})

	leafKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generate leaf key: %v", err)
	}

	leafTemplate := &x509.Certificate{
		SerialNumber: big.NewInt(2),
		Subject: pkix.Name{
			Organization: []string{"QuicRaft DB Adapter Test"},
			CommonName:   "localhost",
		},
		DNSNames:              []string{"localhost"},
		IPAddresses:           []net.IP{net.IPv4(127, 0, 0, 1), net.IPv6loopback, net.IPv6zero},
		NotBefore:             time.Now().Add(-1 * time.Minute),
		NotAfter:              time.Now().Add(1 * time.Hour),
		KeyUsage:              x509.KeyUsageDigitalSignature,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
		BasicConstraintsValid: true,
	}

	leafCertDER, err := x509.CreateCertificate(rand.Reader, leafTemplate, caCert, &leafKey.PublicKey, caKey)
	if err != nil {
		t.Fatalf("create leaf cert: %v", err)
	}

	leafCertPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: leafCertDER})
	leafKeyDER, err := x509.MarshalECPrivateKey(leafKey)
	if err != nil {
		t.Fatalf("marshal leaf key: %v", err)
	}
	leafKeyPEM := pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: leafKeyDER})

	// Write PEM data to temporary files for the adapter's file-based mTLS.
	dir := t.TempDir()
	caFile := filepath.Join(dir, "ca.pem")
	certFile := filepath.Join(dir, "cert.pem")
	keyFile := filepath.Join(dir, "key.pem")

	if err := os.WriteFile(caFile, caCertPEM, 0600); err != nil {
		t.Fatalf("write CA file: %v", err)
	}
	if err := os.WriteFile(certFile, leafCertPEM, 0600); err != nil {
		t.Fatalf("write cert file: %v", err)
	}
	if err := os.WriteFile(keyFile, leafKeyPEM, 0600); err != nil {
		t.Fatalf("write key file: %v", err)
	}

	return dbMTLSFiles{
		CAFile:   caFile,
		CertFile: certFile,
		KeyFile:  keyFile,
	}
}

// ---------------------------------------------------------------------------
// Host Construction Helpers
// ---------------------------------------------------------------------------

// freePort returns a free UDP port by binding and immediately closing a
// temporary listener.
func freePort(t *testing.T) string {
	t.Helper()
	pc, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to find free port: %v", err)
	}
	addr := pc.LocalAddr().String()
	pc.Close()
	return addr
}

// dbTestNodeHostConfig creates a dragonboat NodeHostConfig for
// integration testing with mTLS.
func dbTestNodeHostConfig(t *testing.T, listenAddr string, mtls dbMTLSFiles) dbConfig.NodeHostConfig {
	t.Helper()
	return dbConfig.NodeHostConfig{
		WALDir:         t.TempDir(),
		NodeHostDir:    t.TempDir(),
		RaftAddress:    listenAddr,
		ListenAddress:  listenAddr,
		RTTMillisecond: 50,
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
		ElectionRTT:         10,
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
		ElectionRTT:         10,
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
	mtls     dbMTLSFiles
	t        *testing.T
}

// newDBTestCluster creates an N-node cluster using the dragonboat
// adapter API with real QUIC transport and mTLS.
func newDBTestCluster(t *testing.T, numNodes int) *dbTestCluster {
	t.Helper()

	const maxRetries = 3
	mtls := dbTestMTLSFiles(t)
	addrs := make([]string, numNodes)
	hosts := make([]*dragonboat.NodeHost, numNodes)

	for i := range numNodes {
		var nh *dragonboat.NodeHost
		var addr string
		for attempt := range maxRetries {
			addr = freePort(t)
			nhConfig := dbTestNodeHostConfig(t, addr, mtls)
			var err error
			nh, err = dragonboat.NewNodeHostWithOptions(nhConfig,
				quicraft.WithMemoryLogDB(),
			)
			if err == nil {
				break
			}
			if attempt == maxRetries-1 {
				for j := 0; j < i; j++ {
					hosts[j].Close()
				}
				t.Fatalf("NewNodeHostWithOptions[%d] failed after %d attempts: %v", i, maxRetries, err)
			}
			t.Logf("NewNodeHostWithOptions[%d] at %s failed (attempt %d), retrying: %v", i, addr, attempt+1, err)
		}
		addrs[i] = addr
		hosts[i] = nh
	}

	return &dbTestCluster{
		hosts:   hosts,
		addrs:   addrs,
		stopped: make([]atomic.Bool, numNodes),
		mtls:    mtls,
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

		cfg := dbTestShardConfig(shardID, replicaID)
		if err := nh.StartReplica(members, false, createFn, cfg); err != nil {
			c.t.Fatalf("StartReplica[%d] shard=%d failed: %v", i, shardID, err)
		}
	}

	c.trackers = trackers
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
		if err := nh.StartReplica(members, false, createFn, cfg); err != nil {
			c.t.Fatalf("StartReplica[%d] shard=%d failed: %v", i, shardID, err)
		}
	}

	c.trackers = trackers
	return trackers
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

		cfg := dbTestShardConfig(shardID, replicaID)
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

		cfg := dbTestShardConfig(shardID, replicaID)
		if err := nh.StartOnDiskReplica(members, false, createFn, cfg); err != nil {
			c.t.Fatalf("StartOnDiskReplica[%d] shard=%d failed: %v", i, shardID, err)
		}
	}
	return trackers
}

// close shuts down all hosts in the cluster.
func (c *dbTestCluster) close() {
	c.hostsMu.Lock()
	defer c.hostsMu.Unlock()
	for i, nh := range c.hosts {
		if !c.stopped[i].Load() {
			nh.Close()
		}
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

// waitForReplication waits for a key to appear in all live nodes' state
// machines with the expected value.
func waitForReplication(t *testing.T, cluster *dbTestCluster, trackers []*dbSMTracker, shardID uint64, key, expectedVal string, timeout time.Duration) {
	t.Helper()
	for i := range cluster.hosts {
		if cluster.stopped[i].Load() {
			continue
		}
		if i >= len(trackers) || trackers[i] == nil {
			continue
		}
		replicaID := uint64(i + 1)
		sm := trackers[i].Get(shardID, replicaID)
		if sm == nil {
			continue
		}
		waitForCondition(t, timeout,
			fmt.Sprintf("node[%d] replicates key=%q", i, key),
			func() bool {
				val, ok := sm.Get(key)
				return ok && val == expectedVal
			},
		)
	}
}

// proposeAndWait proposes a key-value pair through the dragonboat adapter
// leader using SyncPropose and waits for it to appear on the leader SM.
func proposeAndWait(t *testing.T, cluster *dbTestCluster, trackers []*dbSMTracker, shardID uint64, key, value string) {
	t.Helper()
	leaderIdx := cluster.findLeader(shardID)
	if leaderIdx < 0 {
		t.Fatal("no leader found for proposal")
	}
	leaderHost := cluster.hosts[leaderIdx]

	ctx, cancel := context.WithTimeout(context.Background(), defaultTimeout)
	defer cancel()

	session := dbClient.NewNoOPSession(shardID, nil)
	cmd := encodeKVPut(key, value)
	_, err := leaderHost.SyncPropose(ctx, session, cmd)
	if err != nil {
		t.Fatalf("SyncPropose(%q=%q) failed: %v", key, value, err)
	}

	// Wait for the leader's SM to apply.
	replicaID := uint64(leaderIdx + 1)
	leaderTracker := trackers[leaderIdx]
	if leaderTracker == nil {
		t.Fatalf("no tracker for leader node[%d]", leaderIdx)
	}
	leaderSM := leaderTracker.Get(shardID, replicaID)
	if leaderSM == nil {
		t.Fatal("leader state machine not found")
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

// verifyAllData verifies all KV pairs from expected are present on all
// live nodes' state machines.
func verifyAllData(
	t *testing.T,
	cluster *dbTestCluster,
	trackers []*dbSMTracker,
	shardID uint64,
	expected map[string]string,
) {
	t.Helper()

	for i := range cluster.hosts {
		if cluster.stopped[i].Load() || i >= len(trackers) || trackers[i] == nil {
			continue
		}
		replicaID := uint64(i + 1)
		nodeSM := trackers[i].Get(shardID, replicaID)
		if nodeSM == nil {
			continue
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
	}
}
