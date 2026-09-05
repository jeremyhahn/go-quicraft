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
	"errors"
	"fmt"
	"io"
	"math/big"
	"net"
	"os"
	"path/filepath"
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
	"github.com/jeremyhahn/go-quicraft/test/netport"
)

// ---------------------------------------------------------------------------
// Raft timing, and the wait budgets derived from it
//
// Every election budget below is DERIVED from the shard configuration these
// tests actually start, and dbTestNodeHostConfig / dbTestShardConfig are
// written in terms of the same constants. Neither side can be changed without
// the other following, which is the entire point of the arrangement.
//
// A timeout literal is a value that is not pinned treated as though it were.
// That is the class this repository has now been bitten by eight times (#84,
// #88, #90, #91, #103, #105, #107, #112) — there the unpinned value is the
// leader index; here it is the shard configuration the budget is supposed to
// describe. #115 is what it looks like when it bites: ElectionRTT is 10 and
// RTTMillisecond is 50, so a single election attempt can run a full second,
// and the budget for a THREE-node cold start was five of them.
// ---------------------------------------------------------------------------

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

// electionRound is the longest a SINGLE election attempt can run before the
// next candidate's timer fires and the attempt is superseded.
//
// It is TWICE ElectionRTT, not once: raft randomises each replica's election
// timer over [electionTimeout, 2*electionTimeout) — see
// raft.randomizedElectionTimeout, r.electionTimeout + r.rng.Intn(...) — so the
// upper bound on one attempt is two election timeouts' worth of ticks. A
// budget written against the un-randomised value is half the size it looks.
const electionRound = 2 * testElectionRTT * raftTick

// defaultTimeout is the standard budget for a single Raft operation that is
// not an election: a proposal round trip, a linearizable read, a membership
// query. It is deliberately NOT expressed in election rounds, because none of
// those wait for an election.
const defaultTimeout = 15 * time.Second

// coldStartElectionRounds is how many worst-case election rounds
// leaderElectionTimeout allows a freshly started shard.
//
// FOUR of them are the protocol's own worst case for a cold start, and each is
// bounded by electionRound:
//
//   - the QUIC handshake and mTLS exchange between all three hosts, which must
//     complete before the first RequestVote can even be answered;
//   - the PreVote round (dbTestShardConfig sets PreVote: true), which precedes
//     the real one;
//   - the vote round itself;
//   - one split-vote retry. On a cold start a split is likely rather than
//     exceptional: all three replicas arm their election timers within
//     microseconds of each other, which is the one situation the randomised
//     timer has the least room to separate.
//
// The remaining factor is host saturation, and it is a multiplier rather than
// an addend. A Raft tick is a goroutine wakeup; on a host oversubscribed k
// times, a 50ms tick lands roughly k times late, and the whole cold start
// stretches with it.
//
// Measured, with this budget lifted to 120s so the tail was not truncated:
// eight full runs of this suite under `-race -p 1`, on 24 cores at load
// average 150-300 (6-12x oversubscribed), gave n=616 cold starts at p50 0.66s,
// p90 1.09s, p95 1.55s, p99 5.2s and max 10.8s. The distribution is not merely
// slow, it is heavy-tailed — the common case sits near the four rounds above
// and a small minority runs an order of magnitude past it. 1.1% exceeded five
// seconds, which across the ~77 cold starts in one run of this suite is about
// one failure per run: the rate #115 reports.
//
// Thirty rounds is thirty seconds, nearly three times that observed maximum.
// It is bounded on purpose: a shard that CANNOT elect — a lost quorum, a
// broken transport, a misconfigured membership — is reported in half a minute
// rather than at the 1800s suite timeout, and the derivation is verified
// against exactly that case (a three-member shard with only one replica
// running fails this wait in 30.0s).
//
// The same measurement is why this budget is LARGER than failoverTimeout even
// though the names suggest otherwise: on the same runs a re-election after a
// leader was stopped never exceeded 4.8s (n=40), because by then the transport
// is established and the survivors already know one another. The cold start is
// the harder of the two and had been given the smaller budget.
const coldStartElectionRounds = 30

// leaderElectionTimeout is the budget for a shard that has just been started
// to produce its FIRST leader. See coldStartElectionRounds for the derivation.
//
// A cold start is not a re-election and must not be budgeted like one. Every
// caller of this constant is waiting on a shard that was started moments
// earlier, so this is the cold-start budget and nothing else; a wait for a
// SUBSEQUENT leader takes failoverTimeout.
const leaderElectionTimeout = coldStartElectionRounds * electionRound

// failoverTimeout is the budget for a shard that has already been led to elect
// a REPLACEMENT leader after the incumbent is stopped or partitioned.
//
// Its value is unchanged; it is written in election rounds so that it tracks
// ElectionRTT and RTTMillisecond alongside leaderElectionTimeout instead of
// drifting away from them. A failover needs one election timeout for the
// survivors to notice the silence and one or two rounds to elect, over warm
// connections — measured max 4.8s under the saturation described above, so
// twenty rounds is roughly four times the observed worst case.
const failoverTimeout = 20 * electionRound

// membershipTimeout is the budget for one membership change to LAND: to be
// accepted by whichever replica leads at the time, committed, and confirmed by
// a linearizable read of the membership.
//
// It is failoverTimeout plus defaultTimeout, in sequence, because that is what
// the loop behind it has to absorb: a leadership change in progress when the
// change is submitted (a failover is what a mid-test leader gap is, and under
// saturation CheckQuorum produces them with nothing stopped), and then the
// commit itself, which is an ordinary operation. The linearizability suite
// derives its membershipTimeout the same way. Each single attempt inside the
// loop still gets at most defaultTimeout, the budget the converted sites gave
// their one direct call.
const membershipTimeout = failoverTimeout + defaultTimeout

// leaderTransferRounds is how many worst-case election rounds ONE requested
// leadership transfer is given to produce the target as leader before the
// request is made again.
//
// A transfer that is going to complete does so within TWO rounds, and one
// that is not never completes on its own:
//
//   - the leader keeps a transfer open for ONE election timeout of its own
//     ticks and then drops it without retrying (raft.tickLeader:
//     transferElapsed >= electionTimeout clears transferTarget). That is
//     ElectionRTT ticks -- HALF an electionRound, because the round is the
//     randomised upper bound and this timer is not randomised. Within it the
//     target must be caught up (it is, by waitForReplication, so TimeoutNow
//     is sent on the request itself) and TimeoutNow delivered;
//   - TimeoutNow makes the target campaign at once, bypassing PreVote and
//     carrying the transfer hint that lets peers holding a live leader lease
//     grant the vote (raft.handleTimeoutNow, broadcastTransferVote). That is
//     one vote round, bounded by electionRound; the incumbent steps down when
//     it sees the higher term;
//   - the survivors learn the new leader from its first heartbeat, one
//     HeartbeatRTT tick, well inside the same round.
//
// Half a round, a round, and a tick: two rounds. RequestLeaderTransfer
// promises nothing past the request ("There is no guarantee the transfer
// completes", nodehost.go), and under saturation a transfer IS lost -- the
// incumbent steps down on CheckQuorum before TimeoutNow is answered, or the
// target's vote round is contested and a third replica wins -- so a request
// that has not produced the target within two rounds is made again, of
// whichever replica leads by then. Measured: at load average 120-335 on 24
// cores the target led 100-140ms after the request in every run, so the two
// rounds are ~15x the observed time and the re-request is for the lost case,
// not the slow one.
const leaderTransferRounds = 2

// leaderTransferRound is the wall-clock form of leaderTransferRounds.
const leaderTransferRound = leaderTransferRounds * electionRound

// leaderTransferTimeout bounds the WHOLE transfer, re-requests included: one
// failover's worth of rounds for the disturbance that lost a request (a
// CheckQuorum step-down and the re-election behind it are exactly a
// failover), and one clean attempt on top of it. A transfer that can never
// complete -- a target that will not campaign -- fails here, in this many
// seconds, rather than at the suite timeout; that case is what the bound is
// verified against.
const leaderTransferTimeout = failoverTimeout + leaderTransferRound

// catchUpTimeout is the budget for a replica that rejoins a led shard -- a
// restarted follower, in TestIntegration_DB_SnapshotRecoveryAfterRestart --
// to hold every committed entry again.
//
// What it bounds. The rejoining replica replays its own snapshot and log
// first, which is local and not Raft-timed, and then holds nothing it can
// vouch for (GetShardInfo reports it Pending) until the leader reaches it:
// the leader's next heartbeat, one HeartbeatRTT tick, draws the response
// that tells the leader where the replica's log ends, after which the leader
// ships the tail as entries or, if the tail was compacted, as a snapshot. Each
// of those is a message exchange -- an ordinary operation, the unit
// defaultTimeout is for -- and none is an election. But the leader that has to
// do the shipping is itself under the same saturation: a CheckQuorum
// step-down and re-election while the replica rejoins is exactly a failover,
// and until it completes there is no leader to ship anything. So: one
// failover, then one ordinary operation, in sequence, which is the same sum
// membershipTimeout is for the same reason. defaultTimeout alone was the
// second half without the first.
const catchUpTimeout = failoverTimeout + defaultTimeout

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

// dbGatedKVStateMachine is a dbKVStateMachine whose Update parks on any entry
// whose key carries gateKeyPrefix until the shared release channel is closed.
//
// It exists to hold a replica's APPLIED index below an appended config-change
// entry. raft.hasConfigChangeToApply is `log.confChangeIndex > log.processed`,
// and processed only advances as the state machine drains committed entries,
// so parking the state machine keeps the leader's pendingConfigChange latch
// armed for as long as the test needs rather than for the few milliseconds a
// config change normally takes to apply. That turns "submit two config changes
// and hope they overlap" into a deterministic overlap.
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

// dbGatedKVCreateFunc returns a factory producing gated KV state machines that
// all park on the supplied release channel. Two trackers come back: a plain
// dbSMTracker holding the embedded KV stores, so every existing helper
// (proposeAndWait, waitForReplication, verifyAllData) works unchanged, and a
// gated tracker exposing the per-replica gate.
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

// freePort returns a loopback address for a host that does not exist yet,
// reserved so that nothing else can be handed the same port before the host
// binds it. It used to bind 127.0.0.1:0 and close the socket, which is #131.
// test/netport carries the reasoning and the measurements.
func freePort(t *testing.T) string {
	t.Helper()
	return netport.MustReserveUDP(t.Fatalf)
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
//
// NodeHost.Close returns nothing: the dragonboat API it mirrors has a void
// Stop(), so the adapter demotes the error quicraft's Host.Close now returns
// to an slog.Warn (contrib/dragonboat/nodehost.go). There is therefore no
// shutdown failure for this helper to assert on.
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

// ---------------------------------------------------------------------------
// Leader-resolving proposals
// ---------------------------------------------------------------------------
//
// Observing a leader does not pin it. A test that writes
//
//	leaderIdx := cluster.waitForClusterLeader(shardID, leaderElectionTimeout)
//	leaderHost := cluster.hosts[leaderIdx]
//	...
//	_, err := leaderHost.SyncPropose(ctx, session, cmd)
//
// has read a value that was true when it looked and treated it as fixed. Raft
// is free to hold an election in the gap, and the proposal is then refused
// CORRECTLY while the test reports a failure the cluster never had. This is the
// sixth confirmed instance of that class in this repository (#84, #88, #90,
// #91, #103, #105).
//
// The refusal is narrow, which is why the pattern survives most runs and fails
// a loaded one. quicraft's Host.Propose FORWARDS to the leader whenever the
// local replica knows who that is; it fails fast only when leadership has moved
// and the addressed replica does not yet know the successor.
//
// It is harder to recognise through this adapter than through quicraft
// directly, because translateError folds quicraft's
// "not leader for shard N, leader unknown" into the bare ErrShardNotReady
// sentinel (see contrib/dragonboat/errors.go). The failure that reaches a test
// here says only "shard not ready", with no mention of leadership at all.
//
// These helpers are this module's own rather than the parent module's
// test/proposer package. That package is typed on *quicraft.Host, a raw
// shardID and quicraft.RequestResult, and its retry predicate is
// quicraft.NotLeader; every one of those is wrong here. This module
// deliberately mirrors the dragonboat API, whose proposal takes a
// *dbClient.Session carrying its own ShardID and whose not-leader rejection has
// already been translated away. The shared part -- resolve at the point of use,
// retry only the not-leader rejection -- is the rule, and the rule is enforced
// for both trees by the one gate in test/buildgates.

// dbProposal is what a leader-resolving proposal produces: the state machine's
// result, and the replica the entry was actually accepted by.
//
// The replica is returned because these tests routinely address it again --
// a linearizable read on the node that just proposed, a tracker lookup for the
// leader's state machine. Handing it back is what lets a caller keep that
// pairing without re-introducing a cached leader index to do it.
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

// proposeRetryInterval is how long a proposal helper waits before re-resolving
// after a not-leader rejection. An election completes in roughly one election
// timeout, so polling faster than this only burns CPU.
const proposeRetryInterval = 20 * time.Millisecond

// minProposeAttempt is the smallest budget an attempt will be started with.
// Below it the attempt would time out on arrival and overwrite the real cause
// with ErrTimeout, reporting leadership churn as a slow shard.
const minProposeAttempt = 200 * time.Millisecond

// leaderHost returns the NodeHost that leads shardID at the moment of the call
// together with its index in the cluster, or (nil, -1) when no live replica
// reports leadership.
//
// This is the resolver the proposal helpers call on every attempt. It is
// deliberately NOT a value a test should hold on to.
func (c *dbTestCluster) leaderHost(shardID uint64) (*dragonboat.NodeHost, int) {
	idx := c.findLeader(shardID)
	if idx < 0 {
		return nil, -1
	}
	c.hostsMu.RLock()
	defer c.hostsMu.RUnlock()
	return c.hosts[idx], idx
}

// waitForLeaderHost waits for a leader on shardID and returns that NodeHost.
//
// It exists for the sites that must address ONE specific replica on purpose --
// a deposed incumbent that must not commit, a leader whose apply loop is about
// to be parked -- and that therefore cannot route through proposeToLeader. It
// replaces cluster.hosts[cluster.findLeader(id)], which panics with index -1
// when the read lands between two leaders.
func (c *dbTestCluster) waitForLeaderHost(shardID uint64, timeout time.Duration) *dragonboat.NodeHost {
	c.t.Helper()
	idx := c.waitForClusterLeader(shardID, timeout)
	c.hostsMu.RLock()
	defer c.hostsMu.RUnlock()
	return c.hosts[idx]
}

// isNotLeaderRejection reports whether err is this adapter's form of the
// not-leader rejection, and nothing else. It is the ONLY error the proposal
// helpers retry.
//
// The adapter has no not-leader error on its public surface. translateError
// folds quicraft.ErrNotLeader into ErrShardNotReady, which is dragonboat's
// documented "unknown leader is the most common cause" error and, unlike
// ErrNotLeader, satisfies IsTempError in a migrated retry loop
// (contrib/dragonboat/errors.go). ErrShardNotReady is therefore the only shape
// a not-leader rejection can have here.
//
// Two other quicraft errors fold into the same sentinel: ErrDropped (the leader
// is unavailable or not ready) and ErrReadIndexNotReady (the leader has not yet
// committed an entry in its term). From the caller's side all three are one
// condition -- the addressed replica cannot get this entry committed because it
// is not, or not yet, a working leader -- and re-resolving is the correct
// response to every one of them.
//
// Everything else is returned unchanged on its FIRST occurrence: ErrTimeout,
// ErrSystemBusy, ErrRejected, ErrAborted, ErrPayloadTooBig, ErrInvalidSession,
// ErrCanceled, a state machine failure. A blanket retry-on-any-error would mask
// exactly the defects these suites exist to catch.
func isNotLeaderRejection(err error) bool {
	return errors.Is(err, dragonboat.ErrShardNotReady) ||
		// A raw quicraft error can still reach a test through a path that
		// does not run translateError.
		errors.Is(err, quicraft.ErrNotLeader)
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

// proposeToLeader proposes cmd to whichever replica leads the session's shard
// at the moment of the call, waits for the entry to commit, and fails the test
// on any error.
//
// Callers that mean "the current leader" use this. Callers that mean one
// SPECIFIC replica -- the incumbent that must NOT commit once quorum is gone,
// the leader whose apply loop a gate entry has to park, a tracked session whose
// series ID is itself under test -- must NOT: for them the addressed node is
// the subject of the assertion, and re-resolving would test something else.
// Those sites keep their own direct call and say so with a
// quicraft:direct-propose marker (see test/buildgates in the parent module).
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

// tryProposeToLeader is proposeToLeader's error-returning form, for callers
// that layer their own assertion on the error and for goroutines, which must
// not touch a *testing.T.
//
// ONLY the not-leader rejection is retried, and it is retried by RE-RESOLVING
// the leader rather than by re-sending to the same replica.
//
// Re-proposal is safe for the commands these suites write. A no-op session
// carries no at-most-once state and every command here is a last-write-wins KV
// put, which is idempotent under re-proposal. A TRACKED session is re-proposed
// with its series ID unchanged, because ProposalCompleted is the caller's to
// call and a failed attempt must not advance the series; the RSM's duplicate
// detection then either rejects the retry or returns the first attempt's cached
// result. That is correct for a caller asserting on the VALUE, and wrong for
// one asserting on the DEDUPLICATION -- which is why the session-dedup tests
// keep their direct calls.
func (c *dbTestCluster) tryProposeToLeader(
	session *dbClient.Session,
	cmd []byte,
	timeout time.Duration,
) (dbProposal, error) {
	shardID := session.ShardID
	deadline := time.Now().Add(timeout)
	retry := time.NewTicker(proposeRetryInterval)
	defer retry.Stop()
	expiry := time.NewTimer(timeout)
	defer expiry.Stop()

	// Reported if the deadline passes without a leader ever being observed,
	// which is itself a not-leader condition.
	var lastErr error = dragonboat.ErrShardNotReady

	for {
		if nh, idx := c.leaderHost(shardID); nh != nil {
			remaining := time.Until(deadline)
			if remaining < minProposeAttempt {
				return dbProposal{}, lastErr
			}
			result, err := proposeOnce(nh, session, cmd, remaining)
			if err == nil {
				return dbProposal{Result: result, Host: nh, Index: idx}, nil
			}
			if !isNotLeaderRejection(err) {
				return dbProposal{}, err
			}
			lastErr = err
		}
		select {
		case <-expiry.C:
			return dbProposal{}, lastErr
		case <-retry.C:
		}
	}
}

// proposeAsyncToLeader is proposeToLeader for the asynchronous Propose path.
// It returns the RequestState, which the caller owns and must Release.
//
// Only the SUBMISSION is retried. Once Propose has accepted the entry the
// outcome belongs to the returned RequestState, and whatever the caller reads
// from ResultC -- Completed, Dropped, Rejected, a timeout -- is the test's own
// assertion to make. Re-submitting on the strength of a result code would
// silently paper over a dropped proposal.
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
	shardID := session.ShardID
	deadline := time.Now().Add(timeout)
	retry := time.NewTicker(proposeRetryInterval)
	defer retry.Stop()
	expiry := time.NewTimer(timeout)
	defer expiry.Stop()

	var lastErr error = dragonboat.ErrShardNotReady

	for {
		if nh, idx := c.leaderHost(shardID); nh != nil {
			remaining := time.Until(deadline)
			if remaining < minProposeAttempt {
				return dbAsyncProposal{}, lastErr
			}
			//quicraft:direct-propose the single attempt this loop is built from
			rs, err := nh.Propose(session, cmd, remaining)
			if err == nil {
				return dbAsyncProposal{RS: rs, Host: nh, Index: idx}, nil
			}
			if !isNotLeaderRejection(err) {
				return dbAsyncProposal{}, err
			}
			lastErr = err
		}
		select {
		case <-expiry.C:
			return dbAsyncProposal{}, lastErr
		case <-retry.C:
		}
	}
}

// ---------------------------------------------------------------------------
// Idempotent, leader-resolving config changes
// ---------------------------------------------------------------------------
//
// A config change has TWO values a test is tempted to pin and must not, and
// this adapter hides the second.
//
// The first is leadership -- the class the proposal helpers above exist for.
// #123 is its config-change instance: a SyncRequestAddReplica on a host
// observed leading seconds earlier, refused CORRECTLY after an election and
// reported as "shard not ready". The second is whether the change COMMITTED:
// a change is submitted, leadership moves before the response, the entry is in
// the log and can still commit under the new leader (Leader Completeness
// guarantees it if it reached a quorum), and a retry is then refused CORRECTLY
// with "already exists" or "not found" while the test reports a failure the
// cluster never had (#112, #132).
//
// The parent module's test/membership package (#149) is the decision core for
// this and is not reusable here, for the reason test/proposer was not (#114):
// its predicates read signals this adapter erases. Its submission bit is
// quicraft.MaybeApplied, and translateError returns the bare mapped sentinel
// for anything that errors.Is a mapped error, so the *quicraft.MaybeAppliedError
// wrapping the post-delivery NotLeaderError arrives here as ErrShardNotReady
// with the wrapper gone, indistinguishable from the pre-flight refusal. Its
// resolver is typed on *quicraft.Host, which NodeHost does not expose; its
// Confirm on *quicraft.Membership rather than this adapter's; and it carries
// no ConfigChangeID, which every converted site here passes. What IS the same
// is the rule, and the rule is what this section mirrors:
//
//	"already exists" / "not found" is success ONLY when (a) an earlier attempt
//	in the SAME loop was submitted to Raft, AND (b) a LINEARIZABLE membership
//	read shows the exact change asked for: the replica present in exactly the
//	requested role at exactly the requested address, or holding no role at all.
//
// A stale-ConfigChangeID refusal after a submitted attempt is treated the same
// way. The ID the caller supplied describes the configuration BEFORE its own
// change, so once that change commits the ID is stale by construction, and
// only the linearizable read can say whether it committed. A stale ID with no
// submitted attempt behind it, or one whose change the read cannot find, is
// the caller's own error and is reported as such.
//
// Half (a) is where this module's rule is WIDER than the parent's, by exactly
// one error, and it has to be. In the parent, submission is quicraft's own bit
// or an ErrTimeout, which Sync{Add,Delete}Node can only return after delivery.
// Here the only shape the post-delivery leadership change has is
// ErrShardNotReady, so ErrShardNotReady arms the bit as well. The cost is
// precise: an engine that applied a change it had reported as refused
// pre-flight would be caught by the parent's helper and not by this one. That
// is the adapter's erasure rather than this helper's choice, and it is filed
// against contrib/dragonboat/errors.go. Half (b) is unchanged and carries the
// weight: a replica ID found in a different role, at a different address, or a
// delete target still present fails regardless of (a).

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
	configChangeID uint64
}

// String renders the change the way the converted sites used to log it.
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
		return nh.SyncRequestAddReplica(ctx, cc.shardID, cc.replicaID, cc.addr, cc.configChangeID) //quicraft:direct-config-change the single attempt driveConfigChange is built from
	},
	addNonVoting: func(ctx context.Context, nh *dragonboat.NodeHost, cc configChange) error {
		return nh.SyncRequestAddNonVoting(ctx, cc.shardID, cc.replicaID, cc.addr, cc.configChangeID) //quicraft:direct-config-change the single attempt driveConfigChange is built from
	},
	addWitness: func(ctx context.Context, nh *dragonboat.NodeHost, cc configChange) error {
		return nh.SyncRequestAddWitness(ctx, cc.shardID, cc.replicaID, cc.addr, cc.configChangeID) //quicraft:direct-config-change the single attempt driveConfigChange is built from
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

// membershipConflictError reports a replica ID present in the membership in a
// state INCOMPATIBLE with the change that requested it: a different role, or
// the requested role at a different address. It names what actually holds
// the ID so a replica-ID collision is diagnosable from the failure alone.
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

// landed reports whether m shows cc in effect.
//
// For an add, the replica ID must be present in exactly the requested role at
// exactly the requested address; found in any other role, or at any other
// address, it is a conflict. Peer.validateAddNode answers "already exists"
// from the replica ID alone, so "already exists" is also what a genuine
// replica-ID collision looks like, and confirming the ID without the address
// would turn that membership bug into a green test. For a delete, the replica
// ID must hold no role at all -- absence alone is weak evidence, since a
// replica that was never a member is also absent, which is exactly why
// driveConfigChange consults this only after an attempt was submitted.
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

// leaderTransferInProgress is the text of the engine's raft.ErrTransferInProgress,
// which handleLeaderPropose returns for EVERY proposal, config changes
// included, while a leadership transfer is in flight. translateError has no
// mapping for it, so it reaches a test wrapped in AdapterError and can only be
// matched by text (#146 asks for the mapping). It is produced before the entry
// enters the log, so it is retryable and never a submission.
const leaderTransferInProgress = "transfer in progress"

// configChangeMaybeSubmitted reports whether err could have been produced
// AFTER the change was delivered to Raft, so the entry may still commit.
//
// Two errors carry that meaning through this adapter:
//
//   - ErrTimeout. The adapter's Sync* methods refuse a context that has no
//     deadline or an expired one BEFORE calling quicraft (timeoutFromContext),
//     and every attempt here is given a fresh context of at least
//     minProposeAttempt, so an ErrTimeout can only be quicraft's own: the
//     select on ctx.Done() after RequestX returned a live RequestState, or the
//     deadline sweep completing a registered pending proposal. Both are after
//     delivery.
//   - ErrShardNotReady. This is the fold: quicraft's post-delivery
//     *MaybeAppliedError{NotLeaderError} and its pre-flight NotLeaderError both
//     translate to this bare sentinel, and so does ErrDropped. The submission
//     bit therefore has to be armed for all of them; see the section comment
//     for what that costs and why it is the adapter's to fix.
//
// ErrCanceled is deliberately NOT here: an attempt's context is cancelled
// only after the attempt has returned, so a cancellation seen here submitted
// nothing.
func configChangeMaybeSubmitted(err error) bool {
	return errors.Is(err, dragonboat.ErrTimeout) || errors.Is(err, dragonboat.ErrShardNotReady)
}

// configChangeRetryableSentinels are the errors one more attempt can change
// the answer to. Every one of them clears on its own: an election completes,
// a pipeline drains, a deadline that was too short gets a fresh one, a pending
// config change commits, a transfer finishes.
//
// ErrRejected is here for membership calls specifically: on this path its
// producer is raft.ErrPendingConfigChange, which Host.translateProposalError
// maps and documents as retryable -- nothing entered the log, so re-proposing
// once the pending change commits succeeds. It is NOT retried by the proposal
// helpers, where ErrRejected is also what an expired session maps to.
//
// ErrStaleConfigChange, "already exists" and "not found" are deliberately
// absent: they are real answers, and driveConfigChange's submission-and-
// confirmation gate is the only thing that may turn one into a success.
var configChangeRetryableSentinels = [...]error{
	dragonboat.ErrShardNotReady,
	dragonboat.ErrSystemBusy,
	dragonboat.ErrTimeout,
	dragonboat.ErrRejected,
	errNoLeaderObserved,
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
	return strings.Contains(err.Error(), leaderTransferInProgress)
}

// driveConfigChange runs attempt until cc is in effect, the budget runs out,
// or something non-retryable comes back.
//
// It is separated from the cluster it usually drives so the decision logic
// can be tested without one -- see config_change_helper_test.go, which is
// where "already exists at a DIFFERENT address must still fail" and "already
// exists WITHOUT a submission must still fail" are pinned.
//
// attempt performs one membership RPC. readMembership performs one
// LINEARIZABLE membership read. Both resolve the leader themselves at the
// point of use, and both may report a transient error, which is treated as
// "ask again".
func driveConfigChange(
	cc configChange,
	budget time.Duration,
	attempt func(context.Context) error,
	readMembership func(context.Context) (*dragonboat.Membership, error),
) error {
	deadline := time.Now().Add(budget)
	retry := time.NewTicker(proposeRetryInterval)
	defer retry.Stop()
	expiry := time.NewTimer(budget)
	defer expiry.Stop()

	submitted := false
	lastErr := errNoLeaderObserved

	for {
		remaining := time.Until(deadline)
		if remaining < minProposeAttempt {
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
	ctx, cancel := context.WithTimeout(context.Background(), max(min(time.Until(deadline), defaultTimeout), minProposeAttempt))
	defer cancel()

	m, err := readMembership(ctx)
	if err != nil {
		return false, nil, err
	}
	landed, conflict = cc.landed(m)
	return landed, conflict, nil
}

// leaderResolver names the replica leading shardID at the moment of the
// call, with its index, or (nil, -1). dbTestCluster.leaderHost is the one the
// suite uses; the type exists so the decision core's "resolved on EVERY
// attempt" property can be pinned with a counting resolver and no cluster.
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
func (c *dbTestCluster) addReplicaToLeader(t *testing.T, shardID, replicaID uint64, addr dragonboat.Target, configChangeID uint64, budget time.Duration) {
	t.Helper()
	c.configChangeToLeader(t, configChange{kind: addReplica, shardID: shardID, replicaID: replicaID, addr: addr, configChangeID: configChangeID}, budget)
}

// addNonVotingToLeader adds replicaID as a non-voting member at addr, against
// configChangeID, and fails the test unless the change lands.
func (c *dbTestCluster) addNonVotingToLeader(t *testing.T, shardID, replicaID uint64, addr dragonboat.Target, configChangeID uint64, budget time.Duration) {
	t.Helper()
	c.configChangeToLeader(t, configChange{kind: addNonVoting, shardID: shardID, replicaID: replicaID, addr: addr, configChangeID: configChangeID}, budget)
}

// addWitnessToLeader adds replicaID as a witness at addr, against
// configChangeID, and fails the test unless the change lands.
func (c *dbTestCluster) addWitnessToLeader(t *testing.T, shardID, replicaID uint64, addr dragonboat.Target, configChangeID uint64, budget time.Duration) {
	t.Helper()
	c.configChangeToLeader(t, configChange{kind: addWitness, shardID: shardID, replicaID: replicaID, addr: addr, configChangeID: configChangeID}, budget)
}

// deleteReplicaToLeader removes replicaID, against configChangeID, and fails
// the test unless the change lands.
func (c *dbTestCluster) deleteReplicaToLeader(t *testing.T, shardID, replicaID, configChangeID uint64, budget time.Duration) {
	t.Helper()
	c.configChangeToLeader(t, configChange{kind: deleteReplica, shardID: shardID, replicaID: replicaID, configChangeID: configChangeID}, budget)
}

// membershipFromLeader takes one linearizable membership read through
// whichever replica leads shardID at the moment of each attempt, retrying the
// not-leader refusal by re-resolving, and fails the test on anything else.
//
// It is the read half of the same class: a SyncGetShardMembership on a host
// bound seconds earlier is refused outright when that host's r.leaderID is
// noLeader, which under saturation is a real and recurring window. Every
// converted site reads the ConfigChangeID it passes, and verifies the result,
// through this.
func (c *dbTestCluster) membershipFromLeader(t *testing.T, shardID uint64, budget time.Duration) *dragonboat.Membership {
	t.Helper()
	deadline := time.Now().Add(budget)
	retry := time.NewTicker(proposeRetryInterval)
	defer retry.Stop()
	expiry := time.NewTimer(budget)
	defer expiry.Stop()
	read := membershipReaderVia(c.leaderHost, shardID)

	lastErr := errNoLeaderObserved
	for {
		remaining := time.Until(deadline)
		if remaining < minProposeAttempt {
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

// transferLeadershipTo moves shardID's leadership to targetReplicaID and
// returns once the target itself reports leading.
//
// RequestLeaderTransfer is a request, not a guarantee, and the engine does
// not retry one (see leaderTransferRounds), so this does what a caller that
// means "the target must lead" has to do: request the transfer of whichever
// replica leads at the moment, give the request leaderTransferRound to
// produce the target, and request again of whoever leads by then. Only the
// not-leader refusal and a transfer already in flight are absorbed; any other
// error is the test's to see. Every request is logged with the replica it was
// made of, so a transfer that needed several is visible in the output.
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
				} else if isNotLeaderRejection(err) || strings.Contains(err.Error(), leaderTransferInProgress) {
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

// resolveStateMachine returns the state machine for node i on the given shard.
//
// The cluster's own tracker slice is consulted FIRST because it is the live
// one: startShard/startShardWithConfig overwrite it whenever a shard is
// (re)started, while a trackers slice a test captured earlier is a snapshot
// that may point at state machines which no longer run. The caller's slice is
// the fallback, which covers a shard whose trackers were never stored on the
// cluster (startConcurrentShard, per-shard trackers in multi-shard tests).
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

// waitForReplication waits for a key to appear with the expected value in the
// state machine of EVERY live node, and fails if it did not actually check
// every one of them.
//
// The count is the point. The previous version skipped any node whose index
// was past the end of the caller's trackers slice and any node whose state
// machine had not registered yet, silently and without a word in the log. A
// nil tracker entry means the engine never invoked the state-machine factory
// for that replica, which is a startup or recovery failure — exactly the thing
// a replication assertion exists to catch — so it is a condition to wait for
// and then fail on, never one to skip.
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

// proposeAndWait proposes a key-value pair through the shard's CURRENT leader
// and waits for it to appear on that leader's state machine.
//
// The leader is resolved inside tryProposeToLeader, on every attempt, so this
// helper cannot go stale between the resolve and the call the way its previous
// version could. The state machine it then waits on is the one belonging to the
// replica that actually accepted the entry, not to an index read earlier.
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

// verifyAllData verifies all KV pairs from expected are present on EVERY
// live node's state machine, and fails if it did not actually check every
// one of them.
//
// Same defect as waitForReplication had: a nil tracker entry means the engine
// never created a state machine for that replica, which is a startup or
// recovery failure. Skipping it removed the node from every data assertion
// while the caller still reported the data verified.
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
