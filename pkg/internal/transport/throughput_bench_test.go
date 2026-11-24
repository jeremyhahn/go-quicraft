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
	"crypto/x509"
	"encoding/pem"
	"fmt"
	"math/rand/v2"
	"sync"
	"testing"
	"time"

	"github.com/jeremyhahn/go-quicraft/pkg/proto"
)

// End-to-end transport throughput over a real QUIC connection.
const (
	// benchEntryBytes is one replication entry payload, in the size range
	// raft's replication chunking actually produces.
	benchEntryBytes = 256 << 10

	// benchEntriesPerMsg makes each frame ~1 MB, well under MaxFrameSize.
	benchEntriesPerMsg = 4

	// benchFrameBytes is the payload bytes carried by one sendBatch call.
	benchFrameBytes = benchEntryBytes * benchEntriesPerMsg

	// benchTotalBytes is the payload volume transferred per benchmark
	// iteration, split evenly across the concurrent streams.
	benchTotalBytes = 256 << 20

	// benchWarmupFrames is the frames sent down each stream before the timer starts, excluding connection and congestion ramp-up.
	benchWarmupFrames = 16

	// benchStreamPoolSize must exceed the highest concurrency level by more
	// than one: getStream maps shardID to (shardID % (StreamPoolSize-1)) + 1,
	// so two shards sharing a stream index would interleave frames from
	// concurrent writers on one stream and corrupt the wire.
	benchStreamPoolSize = 32

	// benchDeploymentID is arbitrary; both endpoints must agree.
	benchDeploymentID = 0xB1

	// benchDrainTimeout bounds the wait for a burst to be fully delivered.
	// A 256 MB loopback burst completes in seconds; this only bounds a hang.
	benchDrainTimeout = 60 * time.Second
)

// benchRecvBudget is the MaxMessageReceiveMemory every variant runs with; see
// the file comment for why it is not the default.
const benchRecvBudget int64 = 1 << 30

// throughputWindows enumerates the per-connection receive windows under test
// and the MaxIncomingConnections value that produces each one from
// benchRecvBudget.
var throughputWindows = []struct {
	name           string
	incomingConns  int
	wantWindowByte uint64
}{
	{"win8MB", 128, 8 << 20},
	{"win16MB", 64, 16 << 20},
	{"win32MB", 32, 32 << 20},
}

// throughputStreams enumerates the concurrent-stream counts under test.
var throughputStreams = []int{1, 4, 16}

// benchSink is a MessageHandler that counts delivered entry payload bytes and
// closes a channel once an armed target is reached. It deliberately retains
// nothing: the benchmark measures transfer, not handler bookkeeping.
type benchSink struct {
	mu       sync.Mutex
	received int64
	target   int64
	done     chan struct{}
}

func newBenchSink() *benchSink {
	return &benchSink{done: make(chan struct{})}
}

// arm resets the counter, sets the expected byte total for the next burst, and
// returns the channel closed once that many payload bytes have arrived.
func (s *benchSink) arm(total int64) <-chan struct{} {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.received = 0
	s.target = total
	s.done = make(chan struct{})
	return s.done
}

// delivered reports the payload bytes received since the last arm.
func (s *benchSink) delivered() int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.received
}

func (s *benchSink) HandleMessage(batch proto.MessageBatch, _ PeerAuth) error {
	var n int64
	for i := range batch.Requests {
		for j := range batch.Requests[i].Entries {
			n += int64(len(batch.Requests[i].Entries[j].Cmd))
		}
	}
	s.mu.Lock()
	s.received += n
	// target is cleared on completion so a late frame cannot double-close.
	if s.target > 0 && s.received >= s.target {
		s.target = 0
		close(s.done)
	}
	s.mu.Unlock()
	return nil
}

// AuthorizeSnapshot admits every transfer: this fake exercises the paths after admission.
func (*benchSink) AuthorizeSnapshot(uint64, uint64, uint64, uint64, PeerAuth) error { return nil }

func (s *benchSink) HandleSnapshot([]proto.SnapshotChunk, PeerAuth) error {
	return nil
}

// BenchmarkThroughputQUIC measures sustained end-to-end payload throughput
// through a live QUIC transport pair, across connection receive windows and
// concurrent stream counts.
func BenchmarkThroughputQUIC(b *testing.B) {
	for _, win := range throughputWindows {
		for _, streams := range throughputStreams {
			b.Run(fmt.Sprintf("%s/streams=%d", win.name, streams), func(b *testing.B) {
				benchmarkThroughput(b, win.incomingConns, win.wantWindowByte, streams)
			})
		}
	}
}

// benchmarkThroughput runs one (window, concurrency) point.
func benchmarkThroughput(b *testing.B, incomingConns int, wantWindow uint64, streams int) {
	b.Helper()

	if streams > benchStreamPoolSize-1 {
		b.Fatalf("streams=%d exceeds distinct stream indices for pool size %d",
			streams, benchStreamPoolSize)
	}
	// Fail loudly if the config-driven derivation stops producing the window
	// this sub-benchmark claims to measure.
	if got := connectionReceiveWindow(benchRecvBudget, incomingConns); got != wantWindow {
		b.Fatalf("connectionReceiveWindow(%d, %d) = %d, want %d",
			benchRecvBudget, incomingConns, got, wantWindow)
	}

	sink := newBenchSink()
	sender, receiver, cleanup := transportPairWith(b, benchDeploymentID, newBenchSink(), sink,
		func(c *Config) {
			c.MaxMessageReceiveMemory = benchRecvBudget
			c.MaxIncomingConnections = incomingConns
			c.StreamPoolSize = benchStreamPoolSize
			// Snappy is disabled so the reported MB/s is wire bytes rather
			// than pre-compression application bytes, and so per-byte
			// compression CPU does not mask flow-control effects.
			c.DisableCompression = true
		})
	defer cleanup()

	target := receiver.Addr().String()

	// One shared, immutable payload: sendBatch only reads Cmd, so reusing it
	// keeps sender-side allocation out of the measurement.
	payload := make([]byte, benchEntryBytes)
	for i := range payload {
		payload[i] = byte(rand.UintN(256))
	}
	entries := make([]proto.Entry, benchEntriesPerMsg)
	for i := range entries {
		entries[i] = proto.Entry{Term: 1, Index: uint64(i + 1), Cmd: payload}
	}

	// burst pushes framesPerStream frames down each concurrent stream, one shard ID per goroutine.
	burst := func(framesPerStream int) {
		var wg sync.WaitGroup
		wg.Add(streams)
		for s := range streams {
			go func(shardID uint64) {
				defer wg.Done()
				msgs := []proto.Message{{
					Type:    proto.Replicate,
					ShardID: shardID,
					From:    1,
					To:      2,
					Term:    1,
					Entries: entries,
				}}
				for range framesPerStream {
					sender.sendBatch(target, "", msgs)
				}
			}(uint64(s) + 1) // shard 0 is reserved and rejected on receive
		}
		wg.Wait()
	}

	// transfer runs one burst and waits for every payload byte to reach the
	// receiving handler.
	transfer := func(framesPerStream int) {
		total := int64(framesPerStream) * int64(streams) * benchFrameBytes
		done := sink.arm(total)
		burst(framesPerStream)
		timer := time.NewTimer(benchDrainTimeout)
		defer timer.Stop()
		select {
		case <-done:
		case <-timer.C:
			b.Fatalf("timed out draining %d bytes: only %d delivered", total, sink.delivered())
		}
	}

	transfer(benchWarmupFrames)

	framesPerStream := benchTotalBytes / benchFrameBytes / streams
	perIteration := int64(framesPerStream) * int64(streams) * benchFrameBytes
	b.SetBytes(perIteration)

	b.ResetTimer()
	for range b.N {
		transfer(framesPerStream)
	}
	b.StopTimer()
}

// BenchmarkPinnedSendOverhead measures what identity pinning adds to sending a
// batch: sendToTarget's destination-identity lookup per message, and the
// identity check on the pooled stream the frame takes (getStream fast path),
// against the same stream fetch with pinning disabled. The resolver models the
// host's (a sync.Map node lookup and a map read).
func BenchmarkPinnedSendOverhead(b *testing.B) {
	for _, n := range []int{1, 64} {
		for _, pinned := range []bool{false, true} {
			b.Run(fmt.Sprintf("batch=%d/pinned=%t", n, pinned), func(b *testing.B) {
				var nodes sync.Map
				bound := map[uint64]string{}
				ident := func(shardID, replicaID uint64) string {
					if _, ok := nodes.Load(shardID); !ok {
						return ""
					}
					return bound[replicaID]
				}
				sender, receiver, cleanup := transportPairWith(b, benchDeploymentID, newBenchSink(), newBenchSink(),
					func(c *Config) {
						if pinned { // the baseline transport has pinning off
							c.ReplicaIdentity = ident
						}
					})
				defer cleanup()
				target := receiver.Addr().String()

				block, _ := pem.Decode(receiver.cfg.MTLSConfig.Cert)
				leaf, err := x509.ParseCertificate(block.Bytes)
				if err != nil {
					b.Fatalf("parse leaf: %v", err)
				}
				bound[2] = NodeIdentityFromCertificate(leaf)
				batch := make([]proto.Message, n)
				for i := range batch {
					shardID := uint64(i%16) + 1
					nodes.Store(shardID, struct{}{})
					batch[i] = proto.Message{Type: proto.Replicate, ShardID: shardID, From: 1, To: 2, Term: 1}
				}
				want := ""
				if pinned {
					want = bound[2]
				}
				if _, err := sender.getStream(target, 1, false, want); err != nil {
					b.Fatalf("warm stream: %v", err)
				}

				b.ReportAllocs()
				b.ResetTimer()
				for range b.N {
					if pinned {
						w := ident(batch[0].ShardID, batch[0].To)
						for i := 1; i < len(batch); i++ {
							if ident(batch[i].ShardID, batch[i].To) != w {
								b.Fatal("mixed destinations")
							}
						}
					}
					if _, err := sender.getStream(target, 1, false, want); err != nil {
						b.Fatalf("getStream: %v", err)
					}
				}
			})
		}
	}
}
