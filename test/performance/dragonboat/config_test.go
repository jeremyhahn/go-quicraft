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
	"crypto/tls"
	"crypto/x509"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/jeremyhahn/dragonboat/v4"
)

// TestRaftConfigMatchesCommonConfiguration pins the Raft timing every
// benchmark replica runs with to the cross-engine common configuration.
func TestRaftConfigMatchesCommonConfiguration(t *testing.T) {
	cfg := raftConfig(7, 3)
	if cfg.ShardID != 7 || cfg.ReplicaID != 3 {
		t.Fatalf("raftConfig(7, 3) = shard %d replica %d", cfg.ShardID, cfg.ReplicaID)
	}
	if cfg.ElectionRTT != 10 {
		t.Errorf("ElectionRTT = %d, want 10", cfg.ElectionRTT)
	}
	if cfg.HeartbeatRTT != 1 {
		t.Errorf("HeartbeatRTT = %d, want 1", cfg.HeartbeatRTT)
	}
	if !cfg.CheckQuorum {
		t.Error("CheckQuorum is off")
	}
	if !cfg.PreVote {
		t.Error("PreVote is off: Dragonboat defaults it off, so it must be set explicitly")
	}
	if err := cfg.Validate(); err != nil {
		t.Fatalf("raftConfig is rejected by Dragonboat: %v", err)
	}
	if dbRTT != 1 {
		t.Errorf("RTTMillisecond = %d, want 1", dbRTT)
	}
}

// TestTransportNegotiatesTLS13 checks the protocol version a NodeHost's
// mutual-TLS transport negotiates with a peer built from the same material.
//
// Dragonboat's TCP transport sets no MinVersion on either side
// (lni/goutils netutil.GetServerTLSConfig and NodeHostConfig.GetClientTLSConfig),
// so Go's defaults apply: a TLS 1.2 floor and a TLS 1.3 ceiling. Between two
// Dragonboat peers TLS 1.3 is therefore negotiated but not required, and the
// transport lives in an internal package, so the harness cannot raise the
// floor without replacing it. This test proves the half that is true.
func TestTransportNegotiatesTLS13(t *testing.T) {
	dir := t.TempDir()
	tlsDir := filepath.Join(dir, "tls")
	if err := os.MkdirAll(tlsDir, 0o755); err != nil {
		t.Fatal(err)
	}
	certs := generateTLSCerts(t, tlsDir, 2)
	addr := freeTCPPort(t)

	nh, err := dragonboat.NewNodeHost(nodeHostConfig(dir, addr, certs[0]))
	if err != nil {
		t.Fatalf("NewNodeHost: %v", err)
	}
	defer nh.Close()

	cert, err := tls.LoadX509KeyPair(certs[1].CertFile, certs[1].KeyFile)
	if err != nil {
		t.Fatal(err)
	}
	caPEM, err := os.ReadFile(certs[1].CAFile)
	if err != nil {
		t.Fatal(err)
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(caPEM) {
		t.Fatal("append CA certificate to pool")
	}
	host, _, err := net.SplitHostPort(addr)
	if err != nil {
		t.Fatal(err)
	}

	// The same client configuration Dragonboat's dialer builds.
	dialer := &tls.Dialer{
		NetDialer: &net.Dialer{Timeout: 5 * time.Second},
		Config: &tls.Config{
			ServerName:   host,
			Certificates: []tls.Certificate{cert},
			RootCAs:      roots,
		},
	}
	nc, err := dialer.DialContext(t.Context(), "tcp", addr)
	if err != nil {
		t.Fatalf("mutual-TLS handshake with the NodeHost transport: %v", err)
	}
	defer nc.Close()
	conn := nc.(*tls.Conn)
	if v := conn.ConnectionState().Version; v != tls.VersionTLS13 {
		t.Fatalf("negotiated %s, want TLS 1.3", tls.VersionName(v))
	}
}
