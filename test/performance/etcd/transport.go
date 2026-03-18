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

package etcd_perf

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	crand "crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/binary"
	"encoding/pem"
	"io"
	"math/big"
	"net"
	"sync"
	"time"

	"go.etcd.io/raft/v3/raftpb"
)

// maxMessageSize is the maximum allowed protobuf message size (64 MB).
// Messages exceeding this limit cause the connection to be dropped as
// a safeguard against corrupted length prefixes.
const maxMessageSize = 64 * 1024 * 1024

// connReadDeadline is the read deadline applied to each length-prefix
// read in the receive loop. Short deadlines allow the loop to check
// the stop channel frequently without blocking indefinitely.
const connReadDeadline = 100 * time.Millisecond

// connDialTimeout is the timeout for outbound TLS connection establishment.
const connDialTimeout = 2 * time.Second

// connWriteDeadline is the write deadline for sending a single message
// frame (length prefix + protobuf payload).
const connWriteDeadline = 1 * time.Second

// raftTransport provides TCP+TLS message delivery between etcd/raft nodes.
// Messages are length-prefixed protobuf frames over persistent TLS connections.
// Outbound connections are lazily dialed and cached. Inbound connections are
// accepted and each served by a dedicated goroutine that reads frames and
// delivers them to the node via the recvFn callback.
type raftTransport struct {
	mu        sync.Mutex
	nodeID    uint64
	addr      string
	peers     map[uint64]string   // nodeID -> addr
	conns     map[uint64]net.Conn // nodeID -> cached outbound TLS conn
	listener  net.Listener
	clientCfg *tls.Config
	recvFn    func(raftpb.Message) // callback when a message is received
	stopCh    chan struct{}
	done      sync.WaitGroup
}

// sharedTLSConfig holds a matched server/client TLS configuration pair
// generated from a single ephemeral CA. All nodes in a benchmark cluster
// share this configuration to enable mutual TLS authentication.
type sharedTLSConfig struct {
	server *tls.Config
	client *tls.Config
}

// generateSharedTLS creates a self-signed ECDSA P-256 CA and issues a
// single node certificate signed by that CA. Both server and client TLS
// configs use the same certificate with proper CA verification (no
// InsecureSkipVerify). The CA and cert are ephemeral, valid for 24 hours.
func generateSharedTLS() (*sharedTLSConfig, error) {
	// Generate ephemeral CA key.
	caKey, err := ecdsa.GenerateKey(elliptic.P256(), crand.Reader)
	if err != nil {
		return nil, err
	}

	caSerial, err := crand.Int(crand.Reader, new(big.Int).Lsh(big.NewInt(1), 128))
	if err != nil {
		return nil, err
	}

	caTemplate := &x509.Certificate{
		SerialNumber:          caSerial,
		Subject:               pkix.Name{CommonName: "bench-ca"},
		NotBefore:             time.Now(),
		NotAfter:              time.Now().Add(24 * time.Hour),
		IsCA:                  true,
		BasicConstraintsValid: true,
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageCRLSign,
	}

	caCertDER, err := x509.CreateCertificate(crand.Reader, caTemplate, caTemplate, &caKey.PublicKey, caKey)
	if err != nil {
		return nil, err
	}

	caCert, err := x509.ParseCertificate(caCertDER)
	if err != nil {
		return nil, err
	}

	// Generate node key and certificate signed by the CA.
	nodeKey, err := ecdsa.GenerateKey(elliptic.P256(), crand.Reader)
	if err != nil {
		return nil, err
	}

	nodeSerial, err := crand.Int(crand.Reader, new(big.Int).Lsh(big.NewInt(1), 128))
	if err != nil {
		return nil, err
	}

	nodeTemplate := &x509.Certificate{
		SerialNumber: nodeSerial,
		Subject:      pkix.Name{CommonName: "bench-node"},
		NotBefore:    time.Now(),
		NotAfter:     time.Now().Add(24 * time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage: []x509.ExtKeyUsage{
			x509.ExtKeyUsageServerAuth,
			x509.ExtKeyUsageClientAuth,
		},
		IPAddresses: []net.IP{net.ParseIP("127.0.0.1")},
		DNSNames:    []string{"localhost"},
	}

	nodeCertDER, err := x509.CreateCertificate(crand.Reader, nodeTemplate, caCert, &nodeKey.PublicKey, caKey)
	if err != nil {
		return nil, err
	}

	// Encode to PEM for tls.X509KeyPair.
	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: nodeCertDER})
	keyDER, err := x509.MarshalECPrivateKey(nodeKey)
	if err != nil {
		return nil, err
	}
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER})

	cert, err := tls.X509KeyPair(certPEM, keyPEM)
	if err != nil {
		return nil, err
	}

	// Build CA certificate pool for verification.
	caPool := x509.NewCertPool()
	caPool.AddCert(caCert)

	serverCfg := &tls.Config{
		Certificates: []tls.Certificate{cert},
		ClientAuth:   tls.RequireAndVerifyClientCert,
		ClientCAs:    caPool,
		MinVersion:   tls.VersionTLS13,
	}

	clientCfg := &tls.Config{
		Certificates: []tls.Certificate{cert},
		RootCAs:      caPool,
		MinVersion:   tls.VersionTLS13,
		ServerName:   "localhost",
	}

	return &sharedTLSConfig{server: serverCfg, client: clientCfg}, nil
}

// newRaftTransport creates a TCP+TLS transport for one raft node. It binds
// a TLS listener on the given address, starts the accept loop for inbound
// connections, and prepares outbound connection caching for peer addresses.
// The recvFn callback is invoked for each successfully decoded inbound message.
func newRaftTransport(
	nodeID uint64,
	addr string,
	peers map[uint64]string,
	tlsCfg *sharedTLSConfig,
	recvFn func(raftpb.Message),
) (*raftTransport, error) {

	listener, err := tls.Listen("tcp", addr, tlsCfg.server)
	if err != nil {
		return nil, err
	}

	t := &raftTransport{
		nodeID:    nodeID,
		addr:      listener.Addr().String(),
		peers:     peers,
		conns:     make(map[uint64]net.Conn),
		listener:  listener,
		clientCfg: tlsCfg.client,
		recvFn:    recvFn,
		stopCh:    make(chan struct{}),
	}

	// Start accept loop for inbound peer connections.
	t.done.Add(1)
	go t.acceptLoop()

	return t, nil
}

// acceptLoop accepts inbound TLS connections and spawns a goroutine
// for each to read length-prefixed protobuf message frames.
func (t *raftTransport) acceptLoop() {
	defer t.done.Done()
	for {
		conn, err := t.listener.Accept()
		if err != nil {
			select {
			case <-t.stopCh:
				return
			default:
				continue
			}
		}
		t.done.Add(1)
		go t.handleConn(conn)
	}
}

// handleConn reads length-prefixed protobuf frames from an inbound
// connection and delivers each decoded message via the recvFn callback.
// The loop exits when the connection is closed, the stop channel is
// signaled, or a non-timeout read error occurs.
func (t *raftTransport) handleConn(conn net.Conn) {
	defer t.done.Done()
	defer conn.Close()

	lengthBuf := make([]byte, 4)
	for {
		select {
		case <-t.stopCh:
			return
		default:
		}

		conn.SetReadDeadline(time.Now().Add(connReadDeadline))
		if _, err := io.ReadFull(conn, lengthBuf); err != nil {
			if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
				continue
			}
			return
		}

		msgLen := binary.BigEndian.Uint32(lengthBuf)
		if msgLen > maxMessageSize {
			return
		}

		data := make([]byte, msgLen)
		conn.SetReadDeadline(time.Now().Add(connWriteDeadline))
		if _, err := io.ReadFull(conn, data); err != nil {
			return
		}

		var msg raftpb.Message
		if err := msg.Unmarshal(data); err != nil {
			return
		}

		t.recvFn(msg)
	}
}

// send delivers a batch of raft messages to their target peer nodes.
// Messages are grouped by target peer and sent in parallel: one
// goroutine per distinct target. Within each target, messages are
// sent sequentially over the cached or newly-dialed TLS connection.
// This prevents a slow peer from blocking sends to all other peers.
func (t *raftTransport) send(msgs []raftpb.Message) {
	// Fast path: single target (common case: 1 heartbeat response).
	if len(msgs) <= 1 {
		for i := range msgs {
			t.sendOne(&msgs[i])
		}
		return
	}

	// Group messages by target node.
	byTarget := make(map[uint64][]*raftpb.Message, len(t.peers))
	for i := range msgs {
		to := msgs[i].To
		byTarget[to] = append(byTarget[to], &msgs[i])
	}

	// Single target: no parallelism needed.
	if len(byTarget) == 1 {
		for _, batch := range byTarget {
			for _, msg := range batch {
				t.sendOne(msg)
			}
		}
		return
	}

	// Multiple targets: send to each peer concurrently.
	var wg sync.WaitGroup
	for _, batch := range byTarget {
		wg.Add(1)
		go func(peerMsgs []*raftpb.Message) {
			defer wg.Done()
			for _, msg := range peerMsgs {
				t.sendOne(msg)
			}
		}(batch)
	}
	wg.Wait()
}

// sendOne delivers a single raft message to its target peer. It uses
// a cached outbound TLS connection if available, otherwise lazily dials
// one. Failed sends cause the connection to be evicted from the cache
// so the next attempt will re-dial.
func (t *raftTransport) sendOne(msg *raftpb.Message) {
	t.mu.Lock()
	conn, ok := t.conns[msg.To]
	t.mu.Unlock()

	if !ok {
		peerAddr, exists := t.peers[msg.To]
		if !exists {
			return
		}
		var err error
		conn, err = tls.DialWithDialer(
			&net.Dialer{Timeout: connDialTimeout},
			"tcp",
			peerAddr,
			t.clientCfg,
		)
		if err != nil {
			return
		}
		t.mu.Lock()
		t.conns[msg.To] = conn
		t.mu.Unlock()
	}

	data, err := msg.Marshal()
	if err != nil {
		return
	}

	var lengthBuf [4]byte
	binary.BigEndian.PutUint32(lengthBuf[:], uint32(len(data)))

	conn.SetWriteDeadline(time.Now().Add(connWriteDeadline))
	if _, err := conn.Write(lengthBuf[:]); err != nil {
		t.removeConn(msg.To)
		return
	}
	if _, err := conn.Write(data); err != nil {
		t.removeConn(msg.To)
		return
	}
}

// removeConn evicts and closes a cached outbound connection for the
// given peer. This is called after a send failure to force re-dial
// on the next message.
func (t *raftTransport) removeConn(nodeID uint64) {
	t.mu.Lock()
	if conn, ok := t.conns[nodeID]; ok {
		conn.Close()
		delete(t.conns, nodeID)
	}
	t.mu.Unlock()
}

// close shuts down the transport: closes the listener, closes all
// cached outbound connections, and waits for all goroutines (accept
// loop and connection handlers) to exit.
func (t *raftTransport) close() {
	close(t.stopCh)
	t.listener.Close()
	t.mu.Lock()
	for id, conn := range t.conns {
		conn.Close()
		delete(t.conns, id)
	}
	t.mu.Unlock()
	t.done.Wait()
}
