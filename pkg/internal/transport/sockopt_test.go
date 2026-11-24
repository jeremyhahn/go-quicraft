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
	"errors"
	"net"
	"testing"

	"github.com/jeremyhahn/go-quicraft/pkg/internal/registry"
)

// listenerSockopts starts a transport with both UDP buffer sizes set to
// bufSize and returns the SOL_SOCKET options named in opts, read from the
// bound listener socket with getsockopt. The platform test files supply the
// option numbers and the getsockopt call, which differ by OS.
func listenerSockopts(t *testing.T, bufSize int, opts map[string]int, getsockopt func(fd uintptr, opt int) (int, error)) map[string]int {
	t.Helper()
	tr, err := NewQUICTransport(Config{
		ListenAddress:  "127.0.0.1:0",
		DeploymentID:   42,
		MTLSConfig:     testMTLSConfig(t),
		UDPRecvBufSize: bufSize,
		UDPSendBufSize: bufSize,
	}, newTestHandler(), registry.NewRegistry())
	if err != nil {
		t.Fatalf("NewQUICTransport failed: %v", err)
	}
	if err := tr.Start(); err != nil {
		t.Fatalf("Start failed: %v", err)
	}
	defer func() {
		if err := tr.Stop(); err != nil {
			t.Errorf("Stop failed: %v", err)
		}
	}()

	raw, err := tr.udpConn.SyscallConn()
	if err != nil {
		t.Fatalf("SyscallConn failed: %v", err)
	}
	got := make(map[string]int, len(opts))
	var getErr error
	if err := raw.Control(func(fd uintptr) {
		for name, opt := range opts {
			v, err := getsockopt(fd, opt)
			if err != nil {
				getErr = err
				return
			}
			got[name] = v
		}
	}); err != nil {
		t.Fatalf("RawConn.Control failed: %v", err)
	}
	if getErr != nil {
		t.Fatalf("getsockopt failed: %v", getErr)
	}
	return got
}

// TestStart_DuplicateBindFailsWithAddrInUse verifies that the listener holds
// its address exclusively: a second transport, or any plain UDP socket, that
// binds the address of a started transport fails with EADDRINUSE rather than
// sharing the port. Two sockets sharing a UDP address silently split its
// datagrams between them, so a duplicate bind must fail loudly.
func TestStart_DuplicateBindFailsWithAddrInUse(t *testing.T) {
	first, err := NewQUICTransport(Config{
		ListenAddress: "127.0.0.1:0",
		DeploymentID:  42,
		MTLSConfig:    testMTLSConfig(t),
	}, newTestHandler(), registry.NewRegistry())
	if err != nil {
		t.Fatalf("NewQUICTransport failed: %v", err)
	}
	if err := first.Start(); err != nil {
		t.Fatalf("Start failed: %v", err)
	}
	t.Cleanup(func() {
		if err := first.Stop(); err != nil {
			t.Errorf("Stop failed: %v", err)
		}
	})
	addr := first.Addr().String()

	t.Run("second transport", func(t *testing.T) {
		second, err := NewQUICTransport(Config{
			ListenAddress: addr,
			DeploymentID:  42,
			MTLSConfig:    testMTLSConfig(t),
		}, newTestHandler(), registry.NewRegistry())
		if err != nil {
			t.Fatalf("NewQUICTransport failed: %v", err)
		}
		err = second.Start()
		if err == nil {
			if stopErr := second.Stop(); stopErr != nil {
				t.Errorf("Stop failed: %v", stopErr)
			}
			t.Fatalf("second transport bound %s while the first holds it", addr)
		}
		if !errors.Is(err, errAddrInUse) {
			t.Fatalf("second transport Start error = %v, want %v", err, errAddrInUse)
		}
	})

	t.Run("plain socket", func(t *testing.T) {
		var lc net.ListenConfig
		pc, err := lc.ListenPacket(context.Background(), "udp", addr)
		if err == nil {
			if closeErr := pc.Close(); closeErr != nil {
				t.Errorf("Close failed: %v", closeErr)
			}
			t.Fatalf("plain UDP socket bound %s while the transport holds it", addr)
		}
		if !errors.Is(err, errAddrInUse) {
			t.Fatalf("plain UDP bind error = %v, want %v", err, errAddrInUse)
		}
	})
}

// TestStop_ReleasesListenerAddress verifies that Stop closes the listener
// socket, so the address is free for a restart the moment Stop returns.
// quic.Transport does not close a Conn it was handed, so the transport must.
func TestStop_ReleasesListenerAddress(t *testing.T) {
	tr, err := NewQUICTransport(Config{
		ListenAddress: "127.0.0.1:0",
		DeploymentID:  42,
		MTLSConfig:    testMTLSConfig(t),
	}, newTestHandler(), registry.NewRegistry())
	if err != nil {
		t.Fatalf("NewQUICTransport failed: %v", err)
	}
	if err := tr.Start(); err != nil {
		t.Fatalf("Start failed: %v", err)
	}
	addr := tr.Addr().String()
	if err := tr.Stop(); err != nil {
		t.Fatalf("Stop failed: %v", err)
	}

	var lc net.ListenConfig
	pc, err := lc.ListenPacket(context.Background(), "udp", addr)
	if err != nil {
		t.Fatalf("bind %s after Stop: %v; Stop left the listener socket open", addr, err)
	}
	if err := pc.Close(); err != nil {
		t.Errorf("Close failed: %v", err)
	}
}
