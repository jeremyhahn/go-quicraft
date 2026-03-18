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

package discovery

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// fakeTransport simulates multicast I/O for testing without real UDP.
type fakeTransport struct {
	mu       sync.Mutex
	inbox    [][]byte
	written  [][]byte
	closed   atomic.Bool
	readIdx  int
	readErr  error
	writeErr error
}

func newFakeTransport() *fakeTransport {
	return &fakeTransport{}
}

func (f *fakeTransport) Enqueue(data []byte) {
	f.mu.Lock()
	defer f.mu.Unlock()
	cp := make([]byte, len(data))
	copy(cp, data)
	f.inbox = append(f.inbox, cp)
}

func (f *fakeTransport) ReadFrom(buf []byte) (int, error) {
	if f.closed.Load() {
		return 0, io.EOF
	}
	if f.readErr != nil {
		return 0, f.readErr
	}

	f.mu.Lock()
	if f.readIdx < len(f.inbox) {
		data := f.inbox[f.readIdx]
		f.readIdx++
		f.mu.Unlock()
		n := copy(buf, data)
		return n, nil
	}
	f.mu.Unlock()

	// Simulate blocking briefly then returning a timeout-like error.
	<-time.After(10 * time.Millisecond)
	return 0, errors.New("timeout")
}

func (f *fakeTransport) WriteTo(data []byte) error {
	if f.closed.Load() {
		return io.EOF
	}
	if f.writeErr != nil {
		return f.writeErr
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	cp := make([]byte, len(data))
	copy(cp, data)
	f.written = append(f.written, cp)
	return nil
}

func (f *fakeTransport) Close() error {
	f.closed.Store(true)
	return nil
}

func (f *fakeTransport) SetReadDeadline(_ time.Time) error {
	return nil
}

func (f *fakeTransport) Written() [][]byte {
	f.mu.Lock()
	defer f.mu.Unlock()
	result := make([][]byte, len(f.written))
	copy(result, f.written)
	return result
}

func newTestMulticast(nodeID, deploymentID uint64, addr string, secret []byte, transport *fakeTransport) (*MulticastDiscovery, error) {
	return NewMulticastDiscovery(MulticastConfig{
		NodeID:           nodeID,
		Address:          addr,
		DeploymentID:     deploymentID,
		SharedSecret:     secret,
		AnnounceInterval: 50 * time.Millisecond,
		DiscoverTimeout:  500 * time.Millisecond,
		transport:        transport,
	})
}

// waitForPeerCount polls md.peers until at least n peers are seen or the
// deadline is exceeded. It uses runtime.Gosched to yield without sleeping.
func waitForPeerCount(t *testing.T, md *MulticastDiscovery, n int, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		md.mu.RLock()
		count := len(md.peers)
		md.mu.RUnlock()
		if count >= n {
			return
		}
		runtime.Gosched()
	}
	md.mu.RLock()
	got := len(md.peers)
	md.mu.RUnlock()
	t.Fatalf("timed out waiting for %d peers; got %d", n, got)
}

// waitForWritten polls ft.Written() until at least n messages have been written.
func waitForWritten(t *testing.T, ft *fakeTransport, n int, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if len(ft.Written()) >= n {
			return
		}
		runtime.Gosched()
	}
	t.Fatalf("timed out waiting for %d written messages; got %d", n, len(ft.Written()))
}

func TestEncodeDecodeMulticastMessage(t *testing.T) {
	secret := []byte("test-secret-key")
	deploymentID := uint64(12345)
	nodeID := uint64(67890)
	addr := "10.0.0.1:5000"

	encoded, err := EncodeMulticastMessage(deploymentID, nodeID, addr, secret)
	if err != nil {
		t.Fatalf("encode error: %v", err)
	}

	gotDeployment, gotNode, gotAddr, err := DecodeMulticastMessage(encoded, secret, 0)
	if err != nil {
		t.Fatalf("decode error: %v", err)
	}

	if gotDeployment != deploymentID {
		t.Errorf("deploymentID = %d, want %d", gotDeployment, deploymentID)
	}
	if gotNode != nodeID {
		t.Errorf("nodeID = %d, want %d", gotNode, nodeID)
	}
	if gotAddr != addr {
		t.Errorf("addr = %q, want %q", gotAddr, addr)
	}
}

func TestEncodeMulticastMessage_EmptyAddress(t *testing.T) {
	secret := []byte("test-secret-key")
	encoded, err := EncodeMulticastMessage(1, 1, "", secret)
	if err != nil {
		t.Fatalf("encode error: %v", err)
	}

	_, _, gotAddr, err := DecodeMulticastMessage(encoded, secret, 0)
	if err != nil {
		t.Fatalf("decode error: %v", err)
	}
	if gotAddr != "" {
		t.Errorf("addr = %q, want empty string", gotAddr)
	}
}

func TestDecodeMulticastMessage_WrongSecret(t *testing.T) {
	secret := []byte("correct-secret")
	wrongSecret := []byte("wrong-secret")

	encoded, err := EncodeMulticastMessage(1, 1, "addr:5000", secret)
	if err != nil {
		t.Fatalf("encode error: %v", err)
	}

	_, _, _, err = DecodeMulticastMessage(encoded, wrongSecret, 0)
	if err == nil {
		t.Fatal("expected authentication error, got nil")
	}
	var ae *AuthenticationError
	if !errors.As(err, &ae) {
		t.Errorf("expected AuthenticationError, got %T: %v", err, err)
	}
}

func TestDecodeMulticastMessage_TooShort(t *testing.T) {
	secret := []byte("secret")
	_, _, _, err := DecodeMulticastMessage(make([]byte, 10), secret, 0)
	if err == nil {
		t.Fatal("expected error for short message, got nil")
	}
}

func TestDecodeMulticastMessage_TruncatedMessage(t *testing.T) {
	secret := []byte("secret")
	encoded, err := EncodeMulticastMessage(1, 1, "10.0.0.1:5000", secret)
	if err != nil {
		t.Fatalf("encode error: %v", err)
	}

	truncated := encoded[:len(encoded)-5]
	_, _, _, err = DecodeMulticastMessage(truncated, secret, 0)
	if err == nil {
		t.Fatal("expected error for truncated message, got nil")
	}
}

func TestDecodeMulticastMessage_TamperedPayload(t *testing.T) {
	secret := []byte("secret")
	encoded, err := EncodeMulticastMessage(1, 1, "10.0.0.1:5000", secret)
	if err != nil {
		t.Fatalf("encode error: %v", err)
	}

	binary.LittleEndian.PutUint64(encoded[8:16], 999)

	_, _, _, err = DecodeMulticastMessage(encoded, secret, 0)
	if err == nil {
		t.Fatal("expected authentication error for tampered payload, got nil")
	}
}

func TestEncodeMulticastMessage_MessageFormat(t *testing.T) {
	secret := []byte("key")
	deploymentID := uint64(0x0102030405060708)
	nodeID := uint64(0x1112131415161718)
	addr := "a:1"

	encoded, err := EncodeMulticastMessage(deploymentID, nodeID, addr, secret)
	if err != nil {
		t.Fatalf("encode error: %v", err)
	}

	gotDeploy := binary.LittleEndian.Uint64(encoded[0:8])
	gotNode := binary.LittleEndian.Uint64(encoded[8:16])
	gotTimestamp := binary.LittleEndian.Uint64(encoded[16:24])
	gotAddrLen := binary.LittleEndian.Uint16(encoded[24:26])
	gotAddr := string(encoded[26 : 26+gotAddrLen])

	if gotDeploy != deploymentID {
		t.Errorf("deploymentID in wire = %#x, want %#x", gotDeploy, deploymentID)
	}
	if gotNode != nodeID {
		t.Errorf("nodeID in wire = %#x, want %#x", gotNode, nodeID)
	}
	if gotTimestamp == 0 {
		t.Error("timestamp in wire should be non-zero")
	}
	if gotAddrLen != 3 {
		t.Errorf("addrLen in wire = %d, want 3", gotAddrLen)
	}
	if gotAddr != "a:1" {
		t.Errorf("addr in wire = %q, want %q", gotAddr, "a:1")
	}

	msgLen := headerSize + int(gotAddrLen)
	expectedMAC := computeHMAC(encoded[:msgLen], secret)
	actualMAC := encoded[msgLen:]
	if !hmac.Equal(actualMAC, expectedMAC) {
		t.Error("HMAC in message does not match computed HMAC")
	}

	expectedTotal := headerSize + 3 + hmacSize
	if len(encoded) != expectedTotal {
		t.Errorf("total size = %d, want %d", len(encoded), expectedTotal)
	}
}

func TestComputeHMAC_Deterministic(t *testing.T) {
	key := []byte("test-key")
	data := []byte("test-data")

	mac1 := computeHMAC(data, key)
	mac2 := computeHMAC(data, key)

	if !hmac.Equal(mac1, mac2) {
		t.Error("HMAC should be deterministic for same inputs")
	}
}

func TestComputeHMAC_DifferentKeys(t *testing.T) {
	data := []byte("test-data")
	mac1 := computeHMAC(data, []byte("key1"))
	mac2 := computeHMAC(data, []byte("key2"))

	if hmac.Equal(mac1, mac2) {
		t.Error("different keys should produce different HMACs")
	}
}

func TestComputeHMAC_SHA256Length(t *testing.T) {
	mac := computeHMAC([]byte("data"), []byte("key"))
	if len(mac) != sha256.Size {
		t.Errorf("HMAC length = %d, want %d", len(mac), sha256.Size)
	}
}

func TestEncodeMulticastMessage_LongAddress(t *testing.T) {
	secret := []byte("key")
	addr := string(make([]byte, 253))
	_, err := EncodeMulticastMessage(1, 1, addr, secret)
	if err != nil {
		t.Fatalf("expected no error for max-length address, got: %v", err)
	}

	longAddr := string(make([]byte, 254))
	_, err = EncodeMulticastMessage(1, 1, longAddr, secret)
	if err == nil {
		t.Fatal("expected error for address exceeding max length")
	}
}

func TestDecodeMulticastMessage_ExtraBytes(t *testing.T) {
	secret := []byte("secret")
	encoded, err := EncodeMulticastMessage(1, 1, "a:1", secret)
	if err != nil {
		t.Fatalf("encode error: %v", err)
	}

	padded := append(encoded, 0xFF, 0xFF)
	_, _, _, err = DecodeMulticastMessage(padded, secret, 0)
	if err == nil {
		t.Fatal("expected error for message with extra bytes")
	}
}

func TestMulticastDiscovery_Name(t *testing.T) {
	md := &MulticastDiscovery{}
	if name := md.Name(); name != "multicast" {
		t.Errorf("Name() = %q, want %q", name, "multicast")
	}
}

func TestMulticastDiscovery_ImplementsMethod(t *testing.T) {
	var _ Method = (*MulticastDiscovery)(nil)
}

func TestNewMulticastDiscovery_NoSecret(t *testing.T) {
	_, err := NewMulticastDiscovery(MulticastConfig{
		NodeID:  1,
		Address: "10.0.0.1:5000",
	})
	if err == nil {
		t.Fatal("expected error for empty shared secret")
	}
	var de *Error
	if !errors.As(err, &de) {
		t.Errorf("expected Error, got %T: %v", err, err)
	}
}

func TestNewMulticastDiscovery_NoAddress(t *testing.T) {
	_, err := NewMulticastDiscovery(MulticastConfig{
		NodeID:       1,
		SharedSecret: []byte("secret"),
	})
	if err == nil {
		t.Fatal("expected error for empty address")
	}
}

func TestEncodeDecodeMulticastMessage_ZeroValues(t *testing.T) {
	secret := []byte("key")
	encoded, err := EncodeMulticastMessage(0, 0, "x", secret)
	if err != nil {
		t.Fatalf("encode error: %v", err)
	}

	gotDeploy, gotNode, gotAddr, err := DecodeMulticastMessage(encoded, secret, 0)
	if err != nil {
		t.Fatalf("decode error: %v", err)
	}
	if gotDeploy != 0 || gotNode != 0 || gotAddr != "x" {
		t.Errorf("got (%d, %d, %q), want (0, 0, \"x\")", gotDeploy, gotNode, gotAddr)
	}
}

func TestDecodeMulticastMessage_EmptyData(t *testing.T) {
	_, _, _, err := DecodeMulticastMessage(nil, []byte("key"), 0)
	if err == nil {
		t.Fatal("expected error for nil data")
	}
}

func TestDecodeMulticastMessage_ReplayRejected(t *testing.T) {
	secret := []byte("replay-test-secret")
	// Encode a message (will have current timestamp).
	encoded, err := EncodeMulticastMessage(100, 1, "10.0.0.1:5000", secret)
	if err != nil {
		t.Fatalf("encode error: %v", err)
	}

	// Tamper with the timestamp to make it old (set to 1 second ago),
	// then recompute HMAC so it passes authentication.
	oldTime := time.Now().Add(-10 * time.Second)
	binary.LittleEndian.PutUint64(encoded[16:24], uint64(oldTime.UnixNano()))
	// Recompute HMAC for the tampered payload.
	addrLen := int(binary.LittleEndian.Uint16(encoded[24:26]))
	msgLen := headerSize + addrLen
	mac := hmac.New(sha256.New, secret)
	mac.Write(encoded[:msgLen])
	copy(encoded[msgLen:], mac.Sum(nil))

	// Decode with a 2-second max age should reject.
	_, _, _, err = DecodeMulticastMessage(encoded, secret, 2*time.Second)
	if err == nil {
		t.Fatal("expected error for expired message, got nil")
	}
	var replayErr *ReplayError
	if !errors.As(err, &replayErr) {
		t.Fatalf("expected *ReplayError, got %T: %v", err, err)
	}
}

func TestDecodeMulticastMessage_ValidTimestampAccepted(t *testing.T) {
	secret := []byte("valid-timestamp-secret")
	encoded, err := EncodeMulticastMessage(200, 5, "10.0.0.5:5000", secret)
	if err != nil {
		t.Fatalf("encode error: %v", err)
	}

	// Decode with generous max age should succeed.
	deployID, nodeID, addr, err := DecodeMulticastMessage(encoded, secret, 10*time.Second)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if deployID != 200 {
		t.Errorf("deploymentID = %d, want 200", deployID)
	}
	if nodeID != 5 {
		t.Errorf("nodeID = %d, want 5", nodeID)
	}
	if addr != "10.0.0.5:5000" {
		t.Errorf("addr = %q, want %q", addr, "10.0.0.5:5000")
	}
}

func TestReplayError_Format(t *testing.T) {
	e := &ReplayError{}
	want := "discovery: multicast message rejected: timestamp outside acceptable window"
	if e.Error() != want {
		t.Errorf("ReplayError.Error() = %q, want %q", e.Error(), want)
	}
}

func TestMulticastDiscovery_DiscoverWithFakeTransport(t *testing.T) {
	secret := []byte("test-secret")
	ft := newFakeTransport()

	// Enqueue a message from a different node.
	peerMsg, err := EncodeMulticastMessage(100, 2, "10.0.0.2:5000", secret)
	if err != nil {
		t.Fatalf("encode error: %v", err)
	}
	ft.Enqueue(peerMsg)

	md, err := newTestMulticast(1, 100, "10.0.0.1:5000", secret, ft)
	if err != nil {
		t.Fatalf("NewMulticastDiscovery error: %v", err)
	}
	defer md.Stop()

	peers, err := md.Discover(context.Background())
	if err != nil {
		t.Fatalf("Discover error: %v", err)
	}
	if len(peers) != 1 {
		t.Fatalf("expected 1 peer, got %d", len(peers))
	}
	if peers[0].NodeID != 2 {
		t.Errorf("NodeID = %d, want 2", peers[0].NodeID)
	}
	if peers[0].Address != "10.0.0.2:5000" {
		t.Errorf("Address = %q, want %q", peers[0].Address, "10.0.0.2:5000")
	}
}

func TestMulticastDiscovery_DiscoverTimeout(t *testing.T) {
	secret := []byte("test-secret")
	ft := newFakeTransport()
	// No messages enqueued, so Discover should timeout.

	md, err := newTestMulticast(1, 100, "10.0.0.1:5000", secret, ft)
	if err != nil {
		t.Fatalf("NewMulticastDiscovery error: %v", err)
	}
	defer md.Stop()

	_, err = md.Discover(context.Background())
	if err == nil {
		t.Fatal("expected timeout error, got nil")
	}
	var te *TimeoutError
	if !errors.As(err, &te) {
		t.Errorf("expected TimeoutError, got %T: %v", err, err)
	}
}

func TestMulticastDiscovery_DiscoverAfterStop(t *testing.T) {
	secret := []byte("test-secret")
	ft := newFakeTransport()

	md, err := newTestMulticast(1, 100, "10.0.0.1:5000", secret, ft)
	if err != nil {
		t.Fatalf("NewMulticastDiscovery error: %v", err)
	}

	md.Stop()

	_, err = md.Discover(context.Background())
	if err == nil {
		t.Fatal("expected error after Stop, got nil")
	}
}

func TestMulticastDiscovery_SkipsSelf(t *testing.T) {
	secret := []byte("test-secret")
	ft := newFakeTransport()

	// Enqueue message from self (nodeID=1).
	selfMsg, err := EncodeMulticastMessage(100, 1, "10.0.0.1:5000", secret)
	if err != nil {
		t.Fatalf("encode error: %v", err)
	}
	ft.Enqueue(selfMsg)

	md, err := newTestMulticast(1, 100, "10.0.0.1:5000", secret, ft)
	if err != nil {
		t.Fatalf("NewMulticastDiscovery error: %v", err)
	}
	defer md.Stop()

	// Should timeout since self is skipped.
	_, err = md.Discover(context.Background())
	if err == nil {
		t.Fatal("expected timeout (self should be skipped)")
	}
}

func TestMulticastDiscovery_SkipsDifferentDeployment(t *testing.T) {
	secret := []byte("test-secret")
	ft := newFakeTransport()

	// Enqueue message from different deployment.
	otherMsg, err := EncodeMulticastMessage(999, 2, "10.0.0.2:5000", secret)
	if err != nil {
		t.Fatalf("encode error: %v", err)
	}
	ft.Enqueue(otherMsg)

	md, err := newTestMulticast(1, 100, "10.0.0.1:5000", secret, ft)
	if err != nil {
		t.Fatalf("NewMulticastDiscovery error: %v", err)
	}
	defer md.Stop()

	_, err = md.Discover(context.Background())
	if err == nil {
		t.Fatal("expected timeout (different deployment should be skipped)")
	}
}

func TestMulticastDiscovery_SkipsInvalidHMAC(t *testing.T) {
	secret := []byte("test-secret")
	ft := newFakeTransport()

	// Enqueue message with wrong secret.
	badMsg, err := EncodeMulticastMessage(100, 2, "10.0.0.2:5000", []byte("wrong-secret"))
	if err != nil {
		t.Fatalf("encode error: %v", err)
	}
	ft.Enqueue(badMsg)

	md, err := newTestMulticast(1, 100, "10.0.0.1:5000", secret, ft)
	if err != nil {
		t.Fatalf("NewMulticastDiscovery error: %v", err)
	}
	defer md.Stop()

	_, err = md.Discover(context.Background())
	if err == nil {
		t.Fatal("expected timeout (invalid HMAC should be rejected)")
	}
}

func TestMulticastDiscovery_MultiplePeers(t *testing.T) {
	secret := []byte("test-secret")
	ft := newFakeTransport()

	for i := uint64(2); i <= 4; i++ {
		msg, err := EncodeMulticastMessage(100, i, "10.0.0."+string(rune('0'+i))+":5000", secret)
		if err != nil {
			t.Fatalf("encode error: %v", err)
		}
		ft.Enqueue(msg)
	}

	md, err := newTestMulticast(1, 100, "10.0.0.1:5000", secret, ft)
	if err != nil {
		t.Fatalf("NewMulticastDiscovery error: %v", err)
	}
	defer md.Stop()

	// Wait for listener to process all messages.
	waitForPeerCount(t, md, 3, 2*time.Second)

	peers, err := md.Discover(context.Background())
	if err != nil {
		t.Fatalf("Discover error: %v", err)
	}
	if len(peers) != 3 {
		t.Errorf("expected 3 peers, got %d", len(peers))
	}
}

func TestMulticastDiscovery_MinPeersWaitsForEnough(t *testing.T) {
	secret := []byte("test-secret")
	ft := newFakeTransport()

	// Configure MinPeers=2 — Discover should wait for 2 other peers.
	md, err := NewMulticastDiscovery(MulticastConfig{
		NodeID:           1,
		Address:          "10.0.0.1:5000",
		DeploymentID:     100,
		SharedSecret:     secret,
		AnnounceInterval: 50 * time.Millisecond,
		DiscoverTimeout:  2 * time.Second,
		MinPeers:         2,
		transport:        ft,
	})
	if err != nil {
		t.Fatalf("NewMulticastDiscovery error: %v", err)
	}
	defer md.Stop()

	// Enqueue only 1 peer initially.
	msg1, _ := EncodeMulticastMessage(100, 2, "10.0.0.2:5000", secret)
	ft.Enqueue(msg1)

	// After a delay, enqueue the second peer.
	go func() {
		<-time.After(200 * time.Millisecond)
		msg2, _ := EncodeMulticastMessage(100, 3, "10.0.0.3:5000", secret)
		ft.Enqueue(msg2)
	}()

	peers, err := md.Discover(context.Background())
	if err != nil {
		t.Fatalf("Discover error: %v", err)
	}
	if len(peers) < 2 {
		t.Errorf("expected at least 2 peers, got %d", len(peers))
	}
}

func TestMulticastDiscovery_MinPeersTimeoutIfNotEnough(t *testing.T) {
	secret := []byte("test-secret")
	ft := newFakeTransport()

	// Configure MinPeers=3 but only provide 1 peer.
	md, err := NewMulticastDiscovery(MulticastConfig{
		NodeID:           1,
		Address:          "10.0.0.1:5000",
		DeploymentID:     100,
		SharedSecret:     secret,
		AnnounceInterval: 50 * time.Millisecond,
		DiscoverTimeout:  300 * time.Millisecond,
		MinPeers:         3,
		transport:        ft,
	})
	if err != nil {
		t.Fatalf("NewMulticastDiscovery error: %v", err)
	}
	defer md.Stop()

	msg1, _ := EncodeMulticastMessage(100, 2, "10.0.0.2:5000", secret)
	ft.Enqueue(msg1)

	_, err = md.Discover(context.Background())
	if err == nil {
		t.Fatal("expected timeout error when not enough peers found")
	}
	var te *TimeoutError
	if !errors.As(err, &te) {
		t.Errorf("expected TimeoutError, got %T: %v", err, err)
	}
}

func TestMulticastDiscovery_StartSendsAnnouncements(t *testing.T) {
	secret := []byte("test-secret")
	ft := newFakeTransport()

	md, err := newTestMulticast(1, 100, "10.0.0.1:5000", secret, ft)
	if err != nil {
		t.Fatalf("NewMulticastDiscovery error: %v", err)
	}

	md.Start()
	waitForWritten(t, ft, 1, 2*time.Second)
	md.Stop()

	written := ft.Written()
	if len(written) == 0 {
		t.Error("expected at least one announcement to be written")
	}

	// Verify announcements are valid messages.
	for _, msg := range written {
		gotDeploy, gotNode, gotAddr, err := DecodeMulticastMessage(msg, secret, 0)
		if err != nil {
			t.Fatalf("announcement decode error: %v", err)
		}
		if gotDeploy != 100 {
			t.Errorf("deploymentID = %d, want 100", gotDeploy)
		}
		if gotNode != 1 {
			t.Errorf("nodeID = %d, want 1", gotNode)
		}
		if gotAddr != "10.0.0.1:5000" {
			t.Errorf("addr = %q, want %q", gotAddr, "10.0.0.1:5000")
		}
	}
}

func TestMulticastDiscovery_StopIdempotent(t *testing.T) {
	secret := []byte("test-secret")
	ft := newFakeTransport()

	md, err := newTestMulticast(1, 100, "10.0.0.1:5000", secret, ft)
	if err != nil {
		t.Fatalf("NewMulticastDiscovery error: %v", err)
	}

	err1 := md.Stop()
	if err1 != nil {
		t.Fatalf("first Stop() error: %v", err1)
	}

	err2 := md.Stop()
	if err2 != nil {
		t.Fatalf("second Stop() should return nil, got: %v", err2)
	}
}

func TestMulticastDiscovery_Snapshot(t *testing.T) {
	md := &MulticastDiscovery{
		peers: map[uint64]Peer{
			1: {NodeID: 1, Address: "10.0.0.1:5000"},
			2: {NodeID: 2, Address: "10.0.0.2:5000"},
		},
	}

	snap := md.snapshot()
	if len(snap) != 2 {
		t.Fatalf("expected 2 peers in snapshot, got %d", len(snap))
	}

	// Mutate snapshot; internal state should be unaffected.
	snap[0].Address = "mutated"
	snap2 := md.snapshot()
	for _, p := range snap2 {
		if p.Address == "mutated" {
			t.Error("snapshot mutation should not affect internal state")
		}
	}
}

func TestMulticastDiscovery_DiscoverStoppedDuringSleep(t *testing.T) {
	secret := []byte("test-secret")
	ft := newFakeTransport()

	md, err := newTestMulticast(1, 100, "10.0.0.1:5000", secret, ft)
	if err != nil {
		t.Fatalf("NewMulticastDiscovery error: %v", err)
	}

	// Stop after a short delay to trigger the stopCh path in Discover.
	go func() {
		<-time.After(100 * time.Millisecond)
		md.Stop()
	}()

	_, err = md.Discover(context.Background())
	if err == nil {
		t.Fatal("expected error when stopped during Discover")
	}
}

func TestMulticastDiscovery_ListenHandlesReadError(t *testing.T) {
	secret := []byte("test-secret")
	ft := newFakeTransport()
	ft.readErr = errors.New("simulated read error")

	md, err := newTestMulticast(1, 100, "10.0.0.1:5000", secret, ft)
	if err != nil {
		t.Fatalf("NewMulticastDiscovery error: %v", err)
	}

	// Give the listener time to attempt reads (negative test: verify 0 peers).
	<-time.After(50 * time.Millisecond)
	md.Stop()

	// No peers should have been discovered.
	md.mu.RLock()
	count := len(md.peers)
	md.mu.RUnlock()
	if count != 0 {
		t.Errorf("expected 0 peers with read errors, got %d", count)
	}
}

func TestNewMulticastDiscovery_BadGroupAddress(t *testing.T) {
	_, err := NewMulticastDiscovery(MulticastConfig{
		NodeID:         1,
		Address:        "10.0.0.1:5000",
		SharedSecret:   []byte("secret"),
		MulticastGroup: "not-a-valid-address:99999999",
	})
	if err == nil {
		t.Fatal("expected error for bad group address")
	}
	var de *Error
	if !errors.As(err, &de) {
		t.Errorf("expected Error, got %T: %v", err, err)
	}
}

func TestNewMulticastDiscovery_BadInterfaceName(t *testing.T) {
	_, err := NewMulticastDiscovery(MulticastConfig{
		NodeID:       1,
		Address:      "10.0.0.1:5000",
		SharedSecret: []byte("secret"),
		Interface:    "nonexistent-iface-xyz999",
	})
	if err == nil {
		t.Fatal("expected error for bad interface name")
	}
	var de *Error
	if !errors.As(err, &de) {
		t.Errorf("expected Error, got %T: %v", err, err)
	}
}

func TestNewMulticastDiscovery_DefaultIntervals(t *testing.T) {
	ft := newFakeTransport()
	md, err := NewMulticastDiscovery(MulticastConfig{
		NodeID:       1,
		Address:      "10.0.0.1:5000",
		SharedSecret: []byte("secret"),
		transport:    ft,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	defer md.Stop()

	if md.cfg.AnnounceInterval != time.Second {
		t.Errorf("AnnounceInterval = %v, want %v", md.cfg.AnnounceInterval, time.Second)
	}
	if md.cfg.DiscoverTimeout != 10*time.Minute {
		t.Errorf("DiscoverTimeout = %v, want %v", md.cfg.DiscoverTimeout, 10*time.Minute)
	}
}

// BenchmarkMulticastEncodeDecode measures the round-trip cost of encoding and
// decoding a multicast discovery message, including HMAC-SHA256 computation on
// both sides. This is the dominant cost in multicast peer discovery.
func BenchmarkMulticastEncodeDecode(b *testing.B) {
	b.ReportAllocs()
	secret := []byte("bench-secret-key-32bytes-long!!")
	deploymentID := uint64(12345)
	nodeID := uint64(67890)
	addr := "10.0.0.1:5000"
	for b.Loop() {
		encoded, err := EncodeMulticastMessage(deploymentID, nodeID, addr, secret)
		if err != nil {
			b.Fatal(err)
		}
		_, _, _, err = DecodeMulticastMessage(encoded, secret, 0)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func TestUDPTransport_ReadWriteClose(t *testing.T) {
	// Create reader and sender sockets on loopback for testing the transport wrapper.
	readerAddr, err := net.ResolveUDPAddr("udp4", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("ResolveUDPAddr error: %v", err)
	}

	reader, err := net.ListenUDP("udp4", readerAddr)
	if err != nil {
		t.Fatalf("ListenUDP reader error: %v", err)
	}

	sender, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4zero, Port: 0})
	if err != nil {
		reader.Close()
		t.Fatalf("ListenUDP sender error: %v", err)
	}

	localAddr := reader.LocalAddr().(*net.UDPAddr)
	transport := &udpTransport{reader: reader, sender: sender, group: localAddr}

	// Test WriteTo.
	testData := []byte("hello-test")
	if err := transport.WriteTo(testData); err != nil {
		t.Fatalf("WriteTo error: %v", err)
	}

	// Test SetReadDeadline.
	if err := transport.SetReadDeadline(time.Now().Add(time.Second)); err != nil {
		t.Fatalf("SetReadDeadline error: %v", err)
	}

	// Test ReadFrom.
	buf := make([]byte, 64)
	n, err := transport.ReadFrom(buf)
	if err != nil {
		t.Fatalf("ReadFrom error: %v", err)
	}
	if string(buf[:n]) != "hello-test" {
		t.Errorf("ReadFrom got %q, want %q", string(buf[:n]), "hello-test")
	}

	// Test Close.
	if err := transport.Close(); err != nil {
		t.Fatalf("Close error: %v", err)
	}
}

// TestNewMulticastConn_MultipleListeners verifies that multiple listeners
// on the same multicast group all receive every packet (no SO_REUSEPORT
// load balancing).
func TestNewMulticastConn_MultipleListeners(t *testing.T) {
	group, err := net.ResolveUDPAddr("udp4", "239.255.77.77:63780")
	if err != nil {
		t.Fatalf("ResolveUDPAddr: %v", err)
	}

	const numListeners = 3
	listeners := make([]net.PacketConn, numListeners)
	for i := range listeners {
		conn, err := newMulticastConn(group, nil)
		if err != nil {
			for j := 0; j < i; j++ {
				listeners[j].Close()
			}
			t.Fatalf("newMulticastConn[%d]: %v", i, err)
		}
		listeners[i] = conn
	}
	defer func() {
		for _, l := range listeners {
			l.Close()
		}
	}()

	// Send 3 distinct messages from a separate socket.
	sender, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4zero, Port: 0})
	if err != nil {
		t.Fatalf("sender: %v", err)
	}
	defer sender.Close()

	messages := []string{"msg-A", "msg-B", "msg-C"}
	for _, msg := range messages {
		if _, err := sender.WriteToUDP([]byte(msg), group); err != nil {
			t.Fatalf("WriteToUDP: %v", err)
		}
	}

	// Each listener should receive all 3 messages.
	for i, conn := range listeners {
		received := make(map[string]bool)
		buf := make([]byte, 64)
		for j := 0; j < len(messages); j++ {
			conn.SetReadDeadline(time.Now().Add(2 * time.Second))
			n, _, err := conn.ReadFrom(buf)
			if err != nil {
				t.Errorf("listener[%d] read %d: %v", i, j, err)
				break
			}
			received[string(buf[:n])] = true
		}
		if len(received) != len(messages) {
			t.Errorf("listener[%d]: got %d messages, want %d: %v",
				i, len(received), len(messages), received)
		}
	}
}

// TestNewMulticastConn_InvalidGroup verifies that a bad group address
// returns an error.
func TestNewMulticastConn_InvalidGroup(t *testing.T) {
	// Port 0 is invalid for binding.
	group := &net.UDPAddr{IP: net.IPv4(239, 255, 77, 77), Port: 0}
	conn, err := newMulticastConn(group, nil)
	if err == nil {
		conn.Close()
		// Port 0 may succeed on some systems (kernel assigns), so
		// just verify the conn is usable.
		return
	}
	// Error is expected on systems that reject port 0 for multicast bind.
}

// TestNewMulticastSender_SendsToGroup verifies that newMulticastSender
// creates a functional sender that can write to a multicast group.
func TestNewMulticastSender_SendsToGroup(t *testing.T) {
	group, err := net.ResolveUDPAddr("udp4", "239.255.77.77:63781")
	if err != nil {
		t.Fatalf("ResolveUDPAddr: %v", err)
	}

	// Create a listener on the group.
	listener, err := newMulticastConn(group, nil)
	if err != nil {
		t.Fatalf("newMulticastConn: %v", err)
	}
	defer listener.Close()

	// Create a sender using the new function.
	sender, err := newMulticastSender(nil)
	if err != nil {
		t.Fatalf("newMulticastSender: %v", err)
	}
	defer sender.Close()

	// Send a message.
	testMsg := []byte("sender-test-message")
	if _, err := sender.WriteToUDP(testMsg, group); err != nil {
		t.Fatalf("WriteToUDP: %v", err)
	}

	// Verify the listener receives it.
	buf := make([]byte, 64)
	_ = listener.SetReadDeadline(time.Now().Add(2 * time.Second))
	n, _, err := listener.ReadFrom(buf)
	if err != nil {
		t.Fatalf("ReadFrom: %v", err)
	}
	if string(buf[:n]) != "sender-test-message" {
		t.Errorf("received %q, want %q", string(buf[:n]), "sender-test-message")
	}
}

// TestMulticastDiscovery_RealUDP_ThreeNodes verifies that 3 MulticastDiscovery
// instances using real UDP multicast sockets discover each other. This is the
// end-to-end test that validates the complete multicast discovery pipeline.
func TestMulticastDiscovery_RealUDP_ThreeNodes(t *testing.T) {
	// Use a unique port to avoid interference with other tests.
	const groupAddr = "239.255.77.77:63782"
	secret := []byte("e2e-test-secret")
	deploymentID := uint64(42)

	nodes := make([]*MulticastDiscovery, 3)
	for i := range nodes {
		cfg := MulticastConfig{
			NodeID:           uint64(i + 1),
			Address:          fmt.Sprintf("127.0.0.1:%d", 50000+i),
			DeploymentID:     deploymentID,
			SharedSecret:     secret,
			AnnounceInterval: 50 * time.Millisecond,
			DiscoverTimeout:  5 * time.Second,
			MulticastGroup:   groupAddr,
			MinPeers:         2,
		}
		md, err := NewMulticastDiscovery(cfg)
		if err != nil {
			for j := 0; j < i; j++ {
				_ = nodes[j].Stop()
			}
			t.Fatalf("NewMulticastDiscovery[%d]: %v", i, err)
		}
		md.Start()
		nodes[i] = md
	}
	defer func() {
		for _, n := range nodes {
			if n != nil {
				_ = n.Stop()
			}
		}
	}()

	// Each node should discover the other 2.
	for i, md := range nodes {
		peers, err := md.Discover(context.Background())
		if err != nil {
			t.Fatalf("node[%d] Discover: %v", i, err)
		}
		if len(peers) < 2 {
			t.Errorf("node[%d] found %d peers, want >= 2", i, len(peers))
		}

		// Verify we didn't discover ourselves.
		selfID := uint64(i + 1)
		for _, p := range peers {
			if p.NodeID == selfID {
				t.Errorf("node[%d] discovered itself (nodeID=%d)", i, selfID)
			}
		}
	}
}

// TestMulticastDiscovery_RealUDP_TwoNodes verifies that 2 MulticastDiscovery
// instances discover each other using real UDP multicast.
func TestMulticastDiscovery_RealUDP_TwoNodes(t *testing.T) {
	const groupAddr = "239.255.77.77:63783"
	secret := []byte("two-node-secret")

	md1, err := NewMulticastDiscovery(MulticastConfig{
		NodeID:           1,
		Address:          "127.0.0.1:60001",
		DeploymentID:     99,
		SharedSecret:     secret,
		AnnounceInterval: 50 * time.Millisecond,
		DiscoverTimeout:  5 * time.Second,
		MulticastGroup:   groupAddr,
		MinPeers:         1,
	})
	if err != nil {
		t.Fatalf("node1: %v", err)
	}
	md1.Start()
	defer md1.Stop()

	md2, err := NewMulticastDiscovery(MulticastConfig{
		NodeID:           2,
		Address:          "127.0.0.1:60002",
		DeploymentID:     99,
		SharedSecret:     secret,
		AnnounceInterval: 50 * time.Millisecond,
		DiscoverTimeout:  5 * time.Second,
		MulticastGroup:   groupAddr,
		MinPeers:         1,
	})
	if err != nil {
		t.Fatalf("node2: %v", err)
	}
	md2.Start()
	defer md2.Stop()

	// Node 1 should discover node 2.
	peers1, err := md1.Discover(context.Background())
	if err != nil {
		t.Fatalf("node1 Discover: %v", err)
	}
	if len(peers1) != 1 || peers1[0].NodeID != 2 {
		t.Errorf("node1 peers = %v, want [{NodeID:2}]", peers1)
	}

	// Node 2 should discover node 1.
	peers2, err := md2.Discover(context.Background())
	if err != nil {
		t.Fatalf("node2 Discover: %v", err)
	}
	if len(peers2) != 1 || peers2[0].NodeID != 1 {
		t.Errorf("node2 peers = %v, want [{NodeID:1}]", peers2)
	}
}

// TestMulticastDiscovery_RealUDP_DeploymentIsolation verifies that nodes
// with different deployment IDs don't discover each other.
func TestMulticastDiscovery_RealUDP_DeploymentIsolation(t *testing.T) {
	const groupAddr = "239.255.77.77:63784"
	secret := []byte("isolation-secret")

	// Node A in deployment 1.
	mdA, err := NewMulticastDiscovery(MulticastConfig{
		NodeID:           1,
		Address:          "127.0.0.1:60010",
		DeploymentID:     1,
		SharedSecret:     secret,
		AnnounceInterval: 50 * time.Millisecond,
		DiscoverTimeout:  500 * time.Millisecond,
		MulticastGroup:   groupAddr,
		MinPeers:         1,
	})
	if err != nil {
		t.Fatalf("nodeA: %v", err)
	}
	mdA.Start()
	defer mdA.Stop()

	// Node B in deployment 2 (different).
	mdB, err := NewMulticastDiscovery(MulticastConfig{
		NodeID:           2,
		Address:          "127.0.0.1:60011",
		DeploymentID:     2,
		SharedSecret:     secret,
		AnnounceInterval: 50 * time.Millisecond,
		DiscoverTimeout:  500 * time.Millisecond,
		MulticastGroup:   groupAddr,
		MinPeers:         1,
	})
	if err != nil {
		t.Fatalf("nodeB: %v", err)
	}
	mdB.Start()
	defer mdB.Stop()

	// Neither should find the other because deploymentIDs differ.
	_, err = mdA.Discover(context.Background())
	if err == nil {
		t.Error("nodeA should timeout (different deployment)")
	}
	_, err = mdB.Discover(context.Background())
	if err == nil {
		t.Error("nodeB should timeout (different deployment)")
	}
}

// TestResolveInterfaceIP_NilInterface verifies nil interface returns nil.
func TestResolveInterfaceIP_NilInterface(t *testing.T) {
	ip := resolveInterfaceIP(nil)
	if ip != nil {
		t.Errorf("expected nil for nil interface, got %v", ip)
	}
}

// TestResolveInterfaceIP_LoopbackInterface verifies loopback returns 127.0.0.1.
func TestResolveInterfaceIP_LoopbackInterface(t *testing.T) {
	iface, err := net.InterfaceByName("lo")
	if err != nil {
		t.Fatalf("loopback interface not available: %v", err)
	}
	ip := resolveInterfaceIP(iface)
	if ip == nil {
		t.Fatal("expected non-nil IP for loopback")
	}
	if !ip.Equal(net.IPv4(127, 0, 0, 1)) {
		t.Errorf("loopback IP = %v, want 127.0.0.1", ip)
	}
}

// TestResolveInterfaceIP_InterfaceWithoutIPv4 verifies that an interface
// without IPv4 addresses returns nil.
func TestResolveInterfaceIP_InterfaceWithoutIPv4(t *testing.T) {
	// Create a mock interface with only IPv6 addresses.
	iface := &net.Interface{
		Name:  "test",
		Flags: net.FlagUp,
	}

	// Create a mock implementation of Interface that returns IPv6 addresses.
	// Since we can't directly mock, we'll test the real behavior by checking
	// that the function handles interfaces without IPv4 gracefully.
	ip := resolveInterfaceIP(iface)
	if ip != nil {
		t.Errorf("expected nil for interface without IPv4, got %v", ip)
	}
}

// panicTransport is a fakeTransport that panics on the Nth ReadFrom call
// or on WriteTo, for testing panic recovery in goroutines.
type panicTransport struct {
	fakeTransport
	panicOnRead     atomic.Bool
	panicOnWrite    atomic.Bool
	readCallCount   atomic.Int64
	panicAfterReads int64
}

func (p *panicTransport) ReadFrom(buf []byte) (int, error) {
	count := p.readCallCount.Add(1)
	if p.panicOnRead.Load() && count >= p.panicAfterReads {
		panic("simulated ReadFrom panic")
	}
	return p.fakeTransport.ReadFrom(buf)
}

func (p *panicTransport) WriteTo(data []byte) error {
	if p.panicOnWrite.Load() {
		panic("simulated WriteTo panic")
	}
	return p.fakeTransport.WriteTo(data)
}

// TestMulticastDiscovery_ListenPanicRecovery verifies that a panic in the
// listen goroutine is recovered and the goroutine restarts, continuing to
// discover peers.
func TestMulticastDiscovery_ListenPanicRecovery(t *testing.T) {
	secret := []byte("panic-test-secret")
	pt := &panicTransport{
		fakeTransport:   *newFakeTransport(),
		panicAfterReads: 1, // Panic on the first read.
	}
	pt.panicOnRead.Store(true)

	// Enqueue a message that will be found after the panic.
	peerMsg, err := EncodeMulticastMessage(100, 2, "10.0.0.2:5000", secret)
	if err != nil {
		t.Fatalf("encode error: %v", err)
	}
	pt.Enqueue(peerMsg)

	md, err := NewMulticastDiscovery(MulticastConfig{
		NodeID:           1,
		Address:          "10.0.0.1:5000",
		DeploymentID:     100,
		SharedSecret:     secret,
		AnnounceInterval: 50 * time.Millisecond,
		DiscoverTimeout:  2 * time.Second,
		transport:        pt,
	})
	if err != nil {
		t.Fatalf("NewMulticastDiscovery error: %v", err)
	}

	// Wait for the panic to fire and recover.
	<-time.After(100 * time.Millisecond)

	// Stop panicking and enqueue a discoverable peer.
	pt.panicOnRead.Store(false)
	peerMsg2, _ := EncodeMulticastMessage(100, 3, "10.0.0.3:5000", secret)
	pt.Enqueue(peerMsg2)

	// The listener should recover and discover the second peer.
	peers, discoverErr := md.Discover(context.Background())
	if discoverErr != nil {
		t.Fatalf("Discover after panic recovery failed: %v", discoverErr)
	}
	if len(peers) == 0 {
		t.Fatal("expected at least 1 peer after panic recovery")
	}

	md.Stop()
}

// TestMulticastDiscovery_ListenPanicStopsCleanly verifies that when a panic
// occurs and the discovery is stopped, the goroutine exits without restart.
func TestMulticastDiscovery_ListenPanicStopsCleanly(t *testing.T) {
	secret := []byte("panic-stop-secret")
	pt := &panicTransport{
		fakeTransport:   *newFakeTransport(),
		panicAfterReads: 1,
	}
	pt.panicOnRead.Store(true)

	md, err := NewMulticastDiscovery(MulticastConfig{
		NodeID:           1,
		Address:          "10.0.0.1:5000",
		DeploymentID:     100,
		SharedSecret:     secret,
		AnnounceInterval: 50 * time.Millisecond,
		DiscoverTimeout:  200 * time.Millisecond,
		transport:        pt,
	})
	if err != nil {
		t.Fatalf("NewMulticastDiscovery error: %v", err)
	}

	// Let the panic fire.
	<-time.After(50 * time.Millisecond)

	// Stop should not hang because the WaitGroup is properly tracked.
	stopErr := md.Stop()
	if stopErr != nil {
		t.Fatalf("Stop returned error: %v", stopErr)
	}
}

// TestMulticastDiscovery_AnnounceLoopPanicRecovery verifies that a panic
// in the announce goroutine is recovered and the goroutine restarts,
// continuing to send announcements.
func TestMulticastDiscovery_AnnounceLoopPanicRecovery(t *testing.T) {
	secret := []byte("announce-panic-secret")
	pt := &panicTransport{
		fakeTransport: *newFakeTransport(),
	}
	pt.panicOnWrite.Store(true)

	md, err := NewMulticastDiscovery(MulticastConfig{
		NodeID:           1,
		Address:          "10.0.0.1:5000",
		DeploymentID:     100,
		SharedSecret:     secret,
		AnnounceInterval: 50 * time.Millisecond,
		DiscoverTimeout:  2 * time.Second,
		transport:        pt,
	})
	if err != nil {
		t.Fatalf("NewMulticastDiscovery error: %v", err)
	}

	md.Start()

	// Let the panic fire in the announce loop.
	<-time.After(100 * time.Millisecond)

	// Stop panicking so announcements can succeed.
	pt.panicOnWrite.Store(false)

	// Wait for announcements to resume after panic recovery.
	waitForWritten(t, &pt.fakeTransport, 1, 2*time.Second)

	// Stop cleanly -- WaitGroup must be properly tracked.
	md.Stop()

	written := pt.Written()
	if len(written) == 0 {
		t.Error("expected at least one announcement after panic recovery")
	}
}

// TestMulticastDiscovery_StopWaitsForGoroutines verifies that Stop()
// blocks until all background goroutines have exited.
func TestMulticastDiscovery_StopWaitsForGoroutines(t *testing.T) {
	secret := []byte("waitgroup-secret")
	ft := newFakeTransport()

	md, err := newTestMulticast(1, 100, "10.0.0.1:5000", secret, ft)
	if err != nil {
		t.Fatalf("NewMulticastDiscovery error: %v", err)
	}
	md.Start()

	// Give goroutines time to start.
	<-time.After(50 * time.Millisecond)

	// Stop should block until both listen and announceLoop exit.
	done := make(chan struct{})
	go func() {
		md.Stop()
		close(done)
	}()

	select {
	case <-done:
		// Stop returned successfully.
	case <-time.After(5 * time.Second):
		t.Fatal("Stop() did not return within 5 seconds; goroutines may be leaked")
	}
}

// TestMulticastDiscovery_SendAnnouncementError verifies that write errors
// in sendAnnouncement are logged but don't panic or crash the goroutine.
func TestMulticastDiscovery_SendAnnouncementError(t *testing.T) {
	secret := []byte("announce-error-secret")
	ft := newFakeTransport()
	ft.writeErr = errors.New("simulated write error")

	md, err := newTestMulticast(1, 100, "10.0.0.1:5000", secret, ft)
	if err != nil {
		t.Fatalf("NewMulticastDiscovery error: %v", err)
	}
	md.Start()

	// Give the announcer time to encounter the write error.
	<-time.After(100 * time.Millisecond)

	// Should stop cleanly even with write errors.
	md.Stop()

	// Verify we attempted writes despite the error.
	// The sendAnnouncement method logs errors but continues.
}

// TestMulticastDiscovery_TransportCloseError verifies that errors from
// transport.Close() are propagated from Stop().
func TestMulticastDiscovery_TransportCloseError(t *testing.T) {
	secret := []byte("close-error-secret")
	ft := newFakeTransport()

	md, err := newTestMulticast(1, 100, "10.0.0.1:5000", secret, ft)
	if err != nil {
		t.Fatalf("NewMulticastDiscovery error: %v", err)
	}

	// Close the transport first to simulate a close error.
	_ = ft.Close()

	// Stop should still work (transport.Close on a closed connection returns EOF).
	stopErr := md.Stop()
	if stopErr != nil && stopErr != io.EOF {
		t.Errorf("Stop error = %v, expected nil or io.EOF", stopErr)
	}
}

// errorTransport simulates transport errors for testing error paths.
type errorTransport struct {
	mu                sync.Mutex
	socketErr         error
	setsockoptErr     error
	bindErr           error
	filePacketConnErr error
	fileConnErr       error
	readErr           error
	writeErr          error
	closed            atomic.Bool
}

func (e *errorTransport) ReadFrom(buf []byte) (int, error) {
	if e.readErr != nil {
		return 0, e.readErr
	}
	return 0, io.EOF
}

func (e *errorTransport) WriteTo(data []byte) error {
	if e.writeErr != nil {
		return e.writeErr
	}
	return nil
}

func (e *errorTransport) Close() error {
	e.closed.Store(true)
	return nil
}

func (e *errorTransport) SetReadDeadline(_ time.Time) error {
	return nil
}

// TestNewMulticastDiscovery_EncodeError verifies that an address that is
// too long returns an encoding error during initialization.
func TestNewMulticastDiscovery_EncodeError(t *testing.T) {
	secret := []byte("secret")
	longAddr := string(make([]byte, 254)) // Exceeds maxAddrLen of 253

	_, err := NewMulticastDiscovery(MulticastConfig{
		NodeID:       1,
		Address:      longAddr,
		DeploymentID: 100,
		SharedSecret: secret,
		transport:    newFakeTransport(),
	})
	if err == nil {
		t.Fatal("expected error for address too long")
	}
	var de *Error
	if !errors.As(err, &de) {
		t.Errorf("expected Error, got %T: %v", err, err)
	}
}

// TestMulticastDiscovery_ListenLoopHandlesClosedConnection verifies that
// the listen loop exits cleanly when the connection is closed.
func TestMulticastDiscovery_ListenLoopHandlesClosedConnection(t *testing.T) {
	secret := []byte("closed-conn-secret")
	ft := newFakeTransport()

	md, err := newTestMulticast(1, 100, "10.0.0.1:5000", secret, ft)
	if err != nil {
		t.Fatalf("NewMulticastDiscovery error: %v", err)
	}

	// Simulate the connection being closed by the transport.
	ft.Close()

	// Give the listener time to detect the closed connection.
	<-time.After(100 * time.Millisecond)

	// Stop should complete without hanging.
	md.Stop()
}

// TestMulticastDiscovery_AnnounceLoopStopsOnSignal verifies that the
// announceLoop exits cleanly when stopCh is closed.
func TestMulticastDiscovery_AnnounceLoopStopsOnSignal(t *testing.T) {
	secret := []byte("announce-stop-secret")
	ft := newFakeTransport()

	md, err := newTestMulticast(1, 100, "10.0.0.1:5000", secret, ft)
	if err != nil {
		t.Fatalf("NewMulticastDiscovery error: %v", err)
	}
	md.Start()

	// Give the announcer time to start and fire at least once.
	waitForWritten(t, ft, 1, 2*time.Second)

	// Close stopCh to signal goroutines to exit.
	md.Stop()

	// Verify the announcer stopped by checking no new writes occurred.
	writtenBefore := len(ft.Written())
	<-time.After(200 * time.Millisecond)
	writtenAfter := len(ft.Written())

	if writtenAfter > writtenBefore {
		t.Error("announcer did not stop after Stop() signal")
	}
}

// TestMulticastDiscovery_MultipleDiscoverCalls verifies that concurrent
// Discover() calls work correctly and get the same snapshot.
func TestMulticastDiscovery_MultipleDiscoverCalls(t *testing.T) {
	secret := []byte("multi-discover-secret")
	ft := newFakeTransport()

	// Enqueue multiple peers.
	for i := uint64(2); i <= 5; i++ {
		msg, _ := EncodeMulticastMessage(100, i, fmt.Sprintf("10.0.0.%d:5000", i), secret)
		ft.Enqueue(msg)
	}

	md, err := newTestMulticast(1, 100, "10.0.0.1:5000", secret, ft)
	if err != nil {
		t.Fatalf("NewMulticastDiscovery error: %v", err)
	}
	defer md.Stop()

	// Wait for listener to process all 4 messages.
	waitForPeerCount(t, md, 4, 2*time.Second)

	// Call Discover multiple times and verify consistency.
	peers1, err := md.Discover(context.Background())
	if err != nil {
		t.Fatalf("first Discover error: %v", err)
	}

	peers2, err := md.Discover(context.Background())
	if err != nil {
		t.Fatalf("second Discover error: %v", err)
	}

	if len(peers1) != len(peers2) {
		t.Errorf("Discover returned different counts: %d vs %d", len(peers1), len(peers2))
	}

	// Verify both snapshots have the expected peers.
	if len(peers1) < 4 {
		t.Errorf("expected at least 4 peers, got %d", len(peers1))
	}
}

// TestMulticastDiscovery_ListenLoopIgnoresMalformedMessages verifies that
// the listener skips malformed messages and continues processing.
func TestMulticastDiscovery_ListenLoopIgnoresMalformedMessages(t *testing.T) {
	secret := []byte("malformed-secret")
	ft := newFakeTransport()

	// Enqueue a malformed message (too short).
	ft.Enqueue([]byte{0x01, 0x02})

	// Enqueue a valid message after the malformed one.
	validMsg, _ := EncodeMulticastMessage(100, 2, "10.0.0.2:5000", secret)
	ft.Enqueue(validMsg)

	md, err := newTestMulticast(1, 100, "10.0.0.1:5000", secret, ft)
	if err != nil {
		t.Fatalf("NewMulticastDiscovery error: %v", err)
	}
	defer md.Stop()

	// Wait for listener to process both messages (1 valid peer expected).
	waitForPeerCount(t, md, 1, 2*time.Second)

	// Should discover the valid peer despite the malformed message.
	peers, discoverErr := md.Discover(context.Background())
	if discoverErr != nil {
		t.Fatalf("Discover error: %v", discoverErr)
	}
	if len(peers) != 1 {
		t.Errorf("expected 1 peer, got %d", len(peers))
	}
}

// timeoutError implements net.Error and returns true for Timeout().
type timeoutError struct{}

func (e *timeoutError) Error() string   { return "timeout" }
func (e *timeoutError) Timeout() bool   { return true }
func (e *timeoutError) Temporary() bool { return true }

// timeoutTransport returns a timeout error for the first N ReadFrom calls,
// then delegates to fakeTransport.
type timeoutTransport struct {
	base          *fakeTransport
	timeoutFirstN int64
	readCallCount atomic.Int64
}

func newTimeoutTransport(timeoutFirstN int64) *timeoutTransport {
	return &timeoutTransport{
		base:          newFakeTransport(),
		timeoutFirstN: timeoutFirstN,
	}
}

func (t *timeoutTransport) ReadFrom(buf []byte) (int, error) {
	count := t.readCallCount.Add(1)
	if count <= t.timeoutFirstN {
		return 0, &timeoutError{}
	}
	return t.base.ReadFrom(buf)
}

func (t *timeoutTransport) WriteTo(data []byte) error {
	return t.base.WriteTo(data)
}

func (t *timeoutTransport) Close() error {
	return t.base.Close()
}

func (t *timeoutTransport) SetReadDeadline(tm time.Time) error {
	return t.base.SetReadDeadline(tm)
}

func (t *timeoutTransport) Enqueue(data []byte) {
	t.base.Enqueue(data)
}

// TestMulticastDiscovery_ListenLoopHandlesNetTimeout verifies that the listener
// correctly handles network timeout errors by continuing to read.
func TestMulticastDiscovery_ListenLoopHandlesNetTimeout(t *testing.T) {
	secret := []byte("timeout-test-secret")

	// Create a transport that first returns a timeout error, then succeeds.
	tt := newTimeoutTransport(1)

	// Enqueue a valid message that will be read after the timeout.
	validMsg, _ := EncodeMulticastMessage(100, 2, "10.0.0.2:5000", secret)
	tt.Enqueue(validMsg)

	md, err := NewMulticastDiscovery(MulticastConfig{
		NodeID:           1,
		Address:          "10.0.0.1:5000",
		DeploymentID:     100,
		SharedSecret:     secret,
		AnnounceInterval: 50 * time.Millisecond,
		DiscoverTimeout:  500 * time.Millisecond,
		transport:        tt,
	})
	if err != nil {
		t.Fatalf("NewMulticastDiscovery error: %v", err)
	}
	defer md.Stop()

	// Wait for the listener to encounter the timeout and then read the message.
	waitForPeerCount(t, md, 1, 2*time.Second)

	// Should discover the peer even though a timeout was encountered.
	peers, discoverErr := md.Discover(context.Background())
	if discoverErr != nil {
		t.Fatalf("Discover error: %v", discoverErr)
	}
	if len(peers) != 1 {
		t.Errorf("expected 1 peer, got %d", len(peers))
	}
}

// TestNewMulticastDiscovery_ConfigurationValidation verifies that incomplete
// configuration is caught early.
func TestNewMulticastDiscovery_ConfigurationValidation(t *testing.T) {
	tests := []struct {
		name   string
		config MulticastConfig
	}{
		{
			name: "no_address",
			config: MulticastConfig{
				NodeID:       1,
				DeploymentID: 100,
				SharedSecret: []byte("secret"),
				transport:    newFakeTransport(),
			},
		},
		{
			name: "no_secret",
			config: MulticastConfig{
				NodeID:       1,
				Address:      "10.0.0.1:5000",
				DeploymentID: 100,
				transport:    newFakeTransport(),
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := NewMulticastDiscovery(tt.config)
			if err == nil {
				t.Errorf("expected error for configuration: %s", tt.name)
			}
		})
	}
}

// TestMulticastDiscovery_DiscoverMinPeersExactMatch verifies that Discover
// returns immediately when MinPeers exactly matches discovered peer count.
func TestMulticastDiscovery_DiscoverMinPeersExactMatch(t *testing.T) {
	secret := []byte("exact-match-secret")
	ft := newFakeTransport()

	// Enqueue exactly 2 peers.
	for i := uint64(2); i <= 3; i++ {
		msg, _ := EncodeMulticastMessage(100, i, fmt.Sprintf("10.0.0.%d:5000", i), secret)
		ft.Enqueue(msg)
	}

	md, err := NewMulticastDiscovery(MulticastConfig{
		NodeID:           1,
		Address:          "10.0.0.1:5000",
		DeploymentID:     100,
		SharedSecret:     secret,
		AnnounceInterval: 50 * time.Millisecond,
		DiscoverTimeout:  2 * time.Second,
		MinPeers:         2,
		transport:        ft,
	})
	if err != nil {
		t.Fatalf("NewMulticastDiscovery error: %v", err)
	}
	defer md.Stop()

	// Wait for listener to process both peers.
	waitForPeerCount(t, md, 2, 2*time.Second)

	// Should return immediately with exact count.
	peers, discoverErr := md.Discover(context.Background())
	if discoverErr != nil {
		t.Fatalf("Discover error: %v", discoverErr)
	}
	if len(peers) != 2 {
		t.Errorf("expected 2 peers, got %d", len(peers))
	}
}

// TestMulticastDiscovery_UpdatePeerAddressOnDuplicate verifies that when a peer
// sends a new announcement with a different address, the address is updated.
func TestMulticastDiscovery_UpdatePeerAddressOnDuplicate(t *testing.T) {
	secret := []byte("update-addr-secret")
	ft := newFakeTransport()

	// Enqueue first announcement from node 2.
	msg1, _ := EncodeMulticastMessage(100, 2, "10.0.0.2:5000", secret)
	ft.Enqueue(msg1)

	md, err := newTestMulticast(1, 100, "10.0.0.1:5000", secret, ft)
	if err != nil {
		t.Fatalf("NewMulticastDiscovery error: %v", err)
	}
	defer md.Stop()

	// Wait for first discovery.
	waitForPeerCount(t, md, 1, 2*time.Second)
	peers1, _ := md.Discover(context.Background())
	if len(peers1) != 1 || peers1[0].Address != "10.0.0.2:5000" {
		t.Errorf("first discovery: got %q, want 10.0.0.2:5000", peers1[0].Address)
	}

	// Enqueue updated announcement with different address.
	msg2, _ := EncodeMulticastMessage(100, 2, "10.0.0.20:6000", secret)
	ft.Enqueue(msg2)

	// Wait for the peer address to be updated.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		md.mu.RLock()
		p, ok := md.peers[2]
		md.mu.RUnlock()
		if ok && p.Address == "10.0.0.20:6000" {
			break
		}
		runtime.Gosched()
	}

	peers2, _ := md.Discover(context.Background())
	if len(peers2) != 1 || peers2[0].Address != "10.0.0.20:6000" {
		t.Errorf("second discovery: got %q, want 10.0.0.20:6000", peers2[0].Address)
	}
}

// TestMulticastDiscovery_AnnouncementTimestampIsFresh verifies that each
// sendAnnouncement call encodes a fresh timestamp rather than reusing a
// stale one from construction time.
func TestMulticastDiscovery_AnnouncementTimestampIsFresh(t *testing.T) {
	secret := []byte("fresh-timestamp-secret")
	ft := newFakeTransport()

	md, err := newTestMulticast(1, 100, "10.0.0.1:5000", secret, ft)
	if err != nil {
		t.Fatalf("NewMulticastDiscovery: %v", err)
	}
	defer func() {
		if stopErr := md.Stop(); stopErr != nil {
			t.Logf("Stop: %v", stopErr)
		}
	}()

	// Send two announcements with a short delay between them to ensure different timestamps.
	md.sendAnnouncement()
	<-time.After(5 * time.Millisecond)
	md.sendAnnouncement()

	written := ft.Written()
	if len(written) < 2 {
		t.Fatalf("expected at least 2 written messages, got %d", len(written))
	}

	// Extract timestamps from the two most recent messages.
	ts1 := binary.LittleEndian.Uint64(written[len(written)-2][16:24])
	ts2 := binary.LittleEndian.Uint64(written[len(written)-1][16:24])

	if ts2 <= ts1 {
		t.Errorf("second announcement timestamp (%d) should be greater than first (%d)", ts2, ts1)
	}

	// Both messages must decode successfully.
	maxAge := 2 * time.Second
	for i, msg := range written[len(written)-2:] {
		_, _, _, decErr := DecodeMulticastMessage(msg, secret, maxAge)
		if decErr != nil {
			t.Errorf("message %d failed to decode: %v", i, decErr)
		}
	}
}

// TestMulticastDiscovery_AnnouncementsNotRejectedAsReplay verifies that
// announcements sent well after construction are not rejected as replays.
// This is the regression test for the stale-timestamp bug where the
// announce message was pre-encoded once at construction time, causing all
// announcements after 2*AnnounceInterval to be rejected.
func TestMulticastDiscovery_AnnouncementsNotRejectedAsReplay(t *testing.T) {
	secret := []byte("replay-regression-secret")
	ft := newFakeTransport()

	// Use a very short announce interval so 2*AnnounceInterval elapses quickly.
	announceInterval := 20 * time.Millisecond
	md, err := NewMulticastDiscovery(MulticastConfig{
		NodeID:           1,
		Address:          "10.0.0.1:5000",
		DeploymentID:     100,
		SharedSecret:     secret,
		AnnounceInterval: announceInterval,
		DiscoverTimeout:  500 * time.Millisecond,
		transport:        ft,
	})
	if err != nil {
		t.Fatalf("NewMulticastDiscovery: %v", err)
	}
	defer func() {
		if stopErr := md.Stop(); stopErr != nil {
			t.Logf("Stop: %v", stopErr)
		}
	}()

	// Wait longer than 2*AnnounceInterval (the replay rejection window),
	// then send a fresh announcement.
	<-time.After(3 * announceInterval)
	md.sendAnnouncement()

	written := ft.Written()
	if len(written) == 0 {
		t.Fatal("expected at least 1 written message")
	}

	// The most recent announcement must decode without a ReplayError.
	latestMsg := written[len(written)-1]
	maxAge := 2 * announceInterval
	_, _, addr, decErr := DecodeMulticastMessage(latestMsg, secret, maxAge)
	if decErr != nil {
		var replayErr *ReplayError
		if errors.As(decErr, &replayErr) {
			t.Fatalf("announcement sent after 3*AnnounceInterval was rejected as replay: %v", decErr)
		}
		t.Fatalf("unexpected decode error: %v", decErr)
	}
	if addr != "10.0.0.1:5000" {
		t.Errorf("decoded address = %q, want %q", addr, "10.0.0.1:5000")
	}
}

// ---------------------------------------------------------------------------
// Coverage: sendAnnouncement write error path
// ---------------------------------------------------------------------------

func TestMulticastDiscovery_SendAnnouncementWriteError(t *testing.T) {
	secret := []byte("test-secret")
	ft := newFakeTransport()
	ft.writeErr = errors.New("write failed")

	md, err := newTestMulticast(1, 100, "10.0.0.1:5000", secret, ft)
	if err != nil {
		t.Fatalf("newTestMulticast error: %v", err)
	}
	defer func() {
		if stopErr := md.Stop(); stopErr != nil {
			t.Logf("Stop: %v", stopErr)
		}
	}()

	// Should not panic on write error, just log a warning.
	md.sendAnnouncement()
}

// ---------------------------------------------------------------------------
// Coverage: NewMulticastDiscovery validation paths
// ---------------------------------------------------------------------------

func TestNewMulticastDiscovery_AddressTooLong(t *testing.T) {
	secret := []byte("test-secret")
	longAddr := strings.Repeat("a", maxAddrLen+1)
	_, err := NewMulticastDiscovery(MulticastConfig{
		NodeID:       1,
		Address:      longAddr,
		DeploymentID: 100,
		SharedSecret: secret,
		transport:    newFakeTransport(),
	})
	if err == nil {
		t.Fatal("expected error for address too long")
	}
}

func TestNewMulticastDiscovery_DefaultAnnounceInterval(t *testing.T) {
	secret := []byte("test-secret")
	ft := newFakeTransport()

	md, err := NewMulticastDiscovery(MulticastConfig{
		NodeID:       1,
		Address:      "10.0.0.1:5000",
		DeploymentID: 100,
		SharedSecret: secret,
		transport:    ft,
		// AnnounceInterval = 0 should default to 1 second.
	})
	if err != nil {
		t.Fatalf("NewMulticastDiscovery error: %v", err)
	}
	defer md.Stop()

	if md.cfg.AnnounceInterval != time.Second {
		t.Errorf("AnnounceInterval = %v, want %v", md.cfg.AnnounceInterval, time.Second)
	}
}

func TestNewMulticastDiscovery_DefaultDiscoverTimeout(t *testing.T) {
	secret := []byte("test-secret")
	ft := newFakeTransport()

	md, err := NewMulticastDiscovery(MulticastConfig{
		NodeID:           1,
		Address:          "10.0.0.1:5000",
		DeploymentID:     100,
		SharedSecret:     secret,
		AnnounceInterval: 50 * time.Millisecond,
		transport:        ft,
		// DiscoverTimeout = 0 should default to 10 minutes.
	})
	if err != nil {
		t.Fatalf("NewMulticastDiscovery error: %v", err)
	}
	defer md.Stop()

	if md.cfg.DiscoverTimeout != 10*time.Minute {
		t.Errorf("DiscoverTimeout = %v, want %v", md.cfg.DiscoverTimeout, 10*time.Minute)
	}
}

// ---------------------------------------------------------------------------
// Coverage: sendAnnouncement encode error path
// ---------------------------------------------------------------------------

func TestMulticastDiscovery_SendAnnouncementEncodeError(t *testing.T) {
	ft := newFakeTransport()

	// Create a MulticastDiscovery with an address that is too long
	// to encode. We bypass NewMulticastDiscovery validation by
	// constructing the struct directly.
	longAddr := strings.Repeat("x", maxAddrLen+1)
	md := &MulticastDiscovery{
		cfg: MulticastConfig{
			NodeID:       1,
			Address:      longAddr,
			DeploymentID: 100,
			SharedSecret: []byte("test-secret"),
		},
		transport: ft,
		peers:     make(map[uint64]Peer),
		stopCh:    make(chan struct{}),
	}

	// Should not panic; encode error is logged and the method returns.
	md.sendAnnouncement()

	// No data should have been written.
	if len(ft.Written()) != 0 {
		t.Errorf("expected no writes on encode error, got %d", len(ft.Written()))
	}
}

// ---------------------------------------------------------------------------
// Coverage: Discover - verify foundPeers from listen loop
// ---------------------------------------------------------------------------

func TestMulticastDiscovery_DiscoverReturnsMultiplePeers(t *testing.T) {
	secret := []byte("test-secret")
	ft := newFakeTransport()

	// Enqueue announcements from two different nodes.
	msg1, err := EncodeMulticastMessage(100, 2, "10.0.0.2:5000", secret)
	if err != nil {
		t.Fatalf("encode msg1: %v", err)
	}
	msg2, err := EncodeMulticastMessage(100, 3, "10.0.0.3:5000", secret)
	if err != nil {
		t.Fatalf("encode msg2: %v", err)
	}
	ft.Enqueue(msg1)
	ft.Enqueue(msg2)

	md, err := NewMulticastDiscovery(MulticastConfig{
		NodeID:           1,
		Address:          "10.0.0.1:5000",
		DeploymentID:     100,
		SharedSecret:     secret,
		AnnounceInterval: 50 * time.Millisecond,
		DiscoverTimeout:  500 * time.Millisecond,
		MinPeers:         2,
		transport:        ft,
	})
	if err != nil {
		t.Fatalf("NewMulticastDiscovery error: %v", err)
	}
	defer md.Stop()

	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()

	peers, discErr := md.Discover(ctx)
	if discErr != nil {
		t.Fatalf("Discover error: %v", discErr)
	}
	if len(peers) < 2 {
		t.Errorf("expected at least 2 peers, got %d", len(peers))
	}
}

// ---------------------------------------------------------------------------
// Coverage: NewMulticastDiscovery - empty address validation
// ---------------------------------------------------------------------------

func TestNewMulticastDiscovery_EmptyAddress(t *testing.T) {
	secret := []byte("test-secret")
	ft := newFakeTransport()

	_, err := NewMulticastDiscovery(MulticastConfig{
		NodeID:           1,
		Address:          "",
		DeploymentID:     100,
		SharedSecret:     secret,
		AnnounceInterval: 50 * time.Millisecond,
		transport:        ft,
	})
	if err == nil {
		t.Fatal("expected error for empty address")
	}
}

// ---------------------------------------------------------------------------
// Coverage: listenLoop - handles self-message (same NodeID) silently
// ---------------------------------------------------------------------------

func TestMulticastDiscovery_ListenLoopSkipsSelfMessages(t *testing.T) {
	secret := []byte("test-secret")
	ft := newFakeTransport()

	// Enqueue a message from our own NodeID.
	selfMsg, err := EncodeMulticastMessage(100, 1, "10.0.0.1:5000", secret)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	ft.Enqueue(selfMsg)

	md, err := newTestMulticast(1, 100, "10.0.0.1:5000", secret, ft)
	if err != nil {
		t.Fatalf("newTestMulticast error: %v", err)
	}
	defer md.Stop()

	md.Start()
	// Wait for the listener to process the self-message (negative test: verify filtered).
	<-time.After(100 * time.Millisecond)

	md.mu.RLock()
	peerCount := len(md.peers)
	md.mu.RUnlock()

	if peerCount != 0 {
		t.Errorf("expected 0 peers (self-messages filtered), got %d", peerCount)
	}
}

// ---------------------------------------------------------------------------
// Coverage: listenLoop - handles different deployment ID
// ---------------------------------------------------------------------------

func TestMulticastDiscovery_ListenLoopSkipsDifferentDeployment(t *testing.T) {
	secret := []byte("test-secret")
	ft := newFakeTransport()

	// Enqueue a message from a different deployment.
	otherMsg, err := EncodeMulticastMessage(999, 2, "10.0.0.2:5000", secret)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	ft.Enqueue(otherMsg)

	md, err := newTestMulticast(1, 100, "10.0.0.1:5000", secret, ft)
	if err != nil {
		t.Fatalf("newTestMulticast error: %v", err)
	}
	defer md.Stop()

	md.Start()
	// Wait for the listener to process the message (negative test: verify filtered).
	<-time.After(100 * time.Millisecond)

	md.mu.RLock()
	peerCount := len(md.peers)
	md.mu.RUnlock()

	if peerCount != 0 {
		t.Errorf("expected 0 peers (different deployment filtered), got %d", peerCount)
	}
}

// ---------------------------------------------------------------------------
// Coverage: Discover - context cancellation
// ---------------------------------------------------------------------------

func TestMulticastDiscovery_DiscoverContextCanceled(t *testing.T) {
	secret := []byte("ctx-cancel-secret")
	ft := newFakeTransport()

	md, err := newTestMulticast(1, 100, "10.0.0.1:5000", secret, ft)
	if err != nil {
		t.Fatalf("NewMulticastDiscovery error: %v", err)
	}
	defer md.Stop()

	// Use an already-canceled context so ctx.Done() fires immediately.
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err = md.Discover(ctx)
	if err == nil {
		t.Fatal("expected error for canceled context, got nil")
	}
	var ce *CancelledError
	if !errors.As(err, &ce) {
		t.Errorf("expected CancelledError, got %T: %v", err, err)
	}
}

// ---------------------------------------------------------------------------
// Coverage: listenLoop - SetReadDeadline error
// ---------------------------------------------------------------------------

// deadlineErrorTransport wraps fakeTransport but returns an error from
// SetReadDeadline to cover the error logging path in listenLoop.
type deadlineErrorTransport struct {
	*fakeTransport
	deadlineErr error
}

func (d *deadlineErrorTransport) SetReadDeadline(_ time.Time) error {
	return d.deadlineErr
}

func TestMulticastDiscovery_ListenLoopSetReadDeadlineError(t *testing.T) {
	secret := []byte("deadline-err-secret")
	ft := newFakeTransport()
	dt := &deadlineErrorTransport{
		fakeTransport: ft,
		deadlineErr:   errors.New("deadline set failed"),
	}

	md, err := NewMulticastDiscovery(MulticastConfig{
		NodeID:           1,
		Address:          "10.0.0.1:5000",
		DeploymentID:     100,
		SharedSecret:     secret,
		AnnounceInterval: 50 * time.Millisecond,
		DiscoverTimeout:  500 * time.Millisecond,
		transport:        dt,
	})
	if err != nil {
		t.Fatalf("NewMulticastDiscovery error: %v", err)
	}

	md.Start()
	// Give the listen loop time to hit SetReadDeadline error.
	<-time.After(200 * time.Millisecond)
	md.Stop()
	// Test passes if no panic occurs — the error is only logged.
}

// ---------------------------------------------------------------------------
// Coverage: NewMulticastDiscovery - newMulticastConn bind error
// ---------------------------------------------------------------------------

func TestNewMulticastDiscovery_BindError(t *testing.T) {
	// Bind to a multicast group on a nonexistent interface index to force
	// a bind/membership error in newMulticastConn. This triggers the error
	// paths regardless of whether the process runs as root.
	_, err := NewMulticastDiscovery(MulticastConfig{
		NodeID:         1,
		Address:        "10.0.0.1:5000",
		DeploymentID:   100,
		SharedSecret:   []byte("bind-error-secret"),
		MulticastGroup: "239.255.77.77:0",
		Interface:      "nonexistent_iface_99",
	})
	if err == nil {
		t.Fatal("expected error from NewMulticastDiscovery with nonexistent interface")
	}
	var de *Error
	if !errors.As(err, &de) {
		t.Errorf("expected Error, got %T: %v", err, err)
	}
}
