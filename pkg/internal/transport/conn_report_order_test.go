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
	"context"
	"net"
	"runtime"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jeremyhahn/go-quicraft/pkg/config"
	"github.com/jeremyhahn/go-quicraft/pkg/internal/registry"
)

// TestGetConnection_WaitersHoldForLeaderReport: senders coalesced onto one
// dial do not return before the dialing sender has delivered that dial's
// connection event. A waiter released earlier could dial again at once and
// have its own event delivered first, so a listener would see a later
// OnConnectionFailed overtaken by an earlier OnConnectionEstablished, or the
// reverse.
//
// The target is a UDP socket that never answers, so the coalesced dial runs
// for the whole DialTimeout — every sender joins it — and then fails; the
// listener's OnConnectionFailed blocks until the test releases it.
func TestGetConnection_WaitersHoldForLeaderReport(t *testing.T) {
	silent, err := (&net.ListenConfig{}).ListenPacket(context.Background(), "udp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("ListenPacket: %v", err)
	}
	defer silent.Close()

	entered := make(chan struct{}, 1)
	release := make(chan struct{})
	tr, err := NewQUICTransport(Config{
		ListenAddress: "127.0.0.1:0",
		DeploymentID:  42,
		MTLSConfig:    testMTLSConfig(t),
		DialTimeout:   time.Second,
		EventListener: &config.EventListener{
			OnConnectionFailed: func(config.ConnectionInfo) {
				select {
				case entered <- struct{}{}:
				default:
				}
				<-release
			},
		},
	}, newTestHandler(), registry.NewRegistry())
	if err != nil {
		t.Fatalf("NewQUICTransport: %v", err)
	}
	if err := tr.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer tr.Stop()
	var released atomic.Bool
	releaseOnce := func() {
		if released.CompareAndSwap(false, true) {
			close(release)
		}
	}
	defer releaseOnce()

	const senders = 8
	returned := make(chan error, senders)
	target := silent.LocalAddr().String()
	for range senders {
		go func() {
			_, err := tr.getConnection(target, "")
			returned <- err
		}()
	}

	select {
	case <-entered:
	case <-time.After(10 * time.Second):
		t.Fatal("OnConnectionFailed was never delivered")
	}
	select {
	case <-returned:
		t.Fatal("a sender returned while the dialing sender was still delivering the dial's connection event")
	case <-time.After(500 * time.Millisecond):
	}

	releaseOnce()
	for range senders {
		select {
		case err := <-returned:
			if err == nil {
				t.Fatal("getConnection to a silent target succeeded")
			}
		case <-time.After(10 * time.Second):
			t.Fatal("a sender never returned after the connection event was delivered")
		}
	}
}

// TestGetConnection_ConsecutiveDialsReportInOrder: the connection events of
// two consecutive dials to one destination are delivered in the order the
// dials started, even though the second dial starts after the first one's
// singleflight entry is gone. The first dial fails against a silent socket
// and its OnConnectionFailed is held; the second, started while the first
// dial's report has not finished, reaches a live peer on the same address and
// must not deliver OnConnectionEstablished — nor return — until the first
// report has. Unordered, a listener would see the peer connected and then,
// stale, failed.
func TestGetConnection_ConsecutiveDialsReportInOrder(t *testing.T) {
	silent, err := (&net.ListenConfig{}).ListenPacket(context.Background(), "udp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("ListenPacket: %v", err)
	}
	target := silent.LocalAddr().String()

	mtls := testMTLSConfig(t)
	failedEntered := make(chan struct{}, 1)
	release := make(chan struct{})
	established := make(chan struct{}, 1)
	tr, err := NewQUICTransport(Config{
		ListenAddress: "127.0.0.1:0",
		DeploymentID:  42,
		MTLSConfig:    mtls,
		DialTimeout:   300 * time.Millisecond,
		EventListener: &config.EventListener{
			OnConnectionFailed: func(config.ConnectionInfo) {
				select {
				case failedEntered <- struct{}{}:
				default:
				}
				<-release
			},
			OnConnectionEstablished: func(config.ConnectionInfo) {
				select {
				case established <- struct{}{}:
				default:
				}
			},
		},
	}, newTestHandler(), registry.NewRegistry())
	if err != nil {
		t.Fatalf("NewQUICTransport: %v", err)
	}
	if err := tr.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer tr.Stop()
	var released atomic.Bool
	releaseOnce := func() {
		if released.CompareAndSwap(false, true) {
			close(release)
		}
	}
	defer releaseOnce()

	first := make(chan error, 1)
	go func() {
		_, err := tr.getConnection(target, "")
		first <- err
	}()
	select {
	case <-failedEntered:
	case <-time.After(10 * time.Second):
		t.Fatal("the first dial's OnConnectionFailed was never delivered")
	}

	// The silent socket gives its address to a live peer.
	if err := silent.Close(); err != nil {
		t.Fatalf("close silent socket: %v", err)
	}
	peer, err := NewQUICTransport(Config{
		ListenAddress: target,
		DeploymentID:  42,
		MTLSConfig:    mtls,
	}, newTestHandler(), registry.NewRegistry())
	if err != nil {
		t.Fatalf("NewQUICTransport(peer): %v", err)
	}
	if err := peer.Start(); err != nil {
		t.Fatalf("peer.Start: %v", err)
	}
	defer peer.Stop()

	second := make(chan error, 1)
	go func() {
		_, err := tr.getConnection(target, "")
		second <- err
	}()
	select {
	case <-established:
		t.Fatal("the second dial delivered OnConnectionEstablished before the first dial's OnConnectionFailed returned")
	case err := <-second:
		t.Fatalf("the second dial returned (%v) before the first dial's report finished", err)
	case <-time.After(500 * time.Millisecond):
	}

	releaseOnce()
	if err := <-first; err == nil {
		t.Fatal("the first dial, to a silent socket, succeeded")
	}
	select {
	case err := <-second:
		if err != nil {
			t.Fatalf("the second dial failed: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("the second dial never returned")
	}
	select {
	case <-established:
	case <-time.After(5 * time.Second):
		t.Fatal("the second dial's OnConnectionEstablished was never delivered")
	}
}

// TestDialReportSequencer_ReleasesInOrderAndForgets: a turn released without
// reporting — its dial ended in a panic or runtime.Goexit — still lets its
// successor go, but only after its own predecessor has reported; and once no
// dial to a destination is pending, the sequencer holds nothing for it.
func TestDialReportSequencer_ReleasesInOrderAndForgets(t *testing.T) {
	var s dialReportSequencer
	first := s.take("peer")
	second := s.take("peer")
	third := s.take("peer")

	secondReleased := make(chan struct{})
	go func() {
		defer close(secondReleased)
		// A dial that ends abnormally: released by its defer, never reported.
		defer s.release("peer", &second)
		runtime.Goexit()
	}()
	select {
	case <-secondReleased:
		t.Fatal("an abnormally ended dial released its successors before its predecessor reported")
	case <-time.After(100 * time.Millisecond):
	}

	s.release("peer", &first)
	select {
	case <-secondReleased:
	case <-time.After(5 * time.Second):
		t.Fatal("an abnormally ended dial never released its successors")
	}
	thirdReported := make(chan struct{})
	go func() {
		defer close(thirdReported)
		third.await()
	}()
	select {
	case <-thirdReported:
	case <-time.After(5 * time.Second):
		t.Fatal("the dial after an abnormally ended one never got its turn")
	}

	s.release("peer", &third)
	s.release("peer", &third) // a second release is a no-op
	s.mu.Lock()
	n := len(s.tails)
	s.mu.Unlock()
	if n != 0 {
		t.Fatalf("sequencer holds %d destinations with no dial pending, want 0", n)
	}
}
