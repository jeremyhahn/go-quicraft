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
	"log/slog"
	"net"
	"os"
	"runtime/debug"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

const (
	// DefaultMulticastGroup is the default multicast address for peer discovery.
	DefaultMulticastGroup = "239.255.77.77:63777"

	// hmacSize is the length of an HMAC-SHA256 digest.
	hmacSize = 32

	// headerSize is deploymentID(8) + nodeID(8) + timestamp(8) + addrLen(2).
	headerSize = 26

	// maxAddrLen is the maximum address length.
	maxAddrLen = 253

	// readBufferSize is the UDP read buffer size.
	readBufferSize = 512
)

// multicastTransport abstracts the network I/O for multicast discovery,
// enabling unit testing without real UDP multicast.
type multicastTransport interface {
	// ReadFrom reads a datagram, returning bytes read and any error.
	ReadFrom(buf []byte) (int, error)

	// WriteTo sends a datagram to the multicast group.
	WriteTo(data []byte) error

	// Close closes the transport.
	Close() error

	// SetReadDeadline sets the read deadline.
	SetReadDeadline(t time.Time) error
}

// udpTransport implements multicastTransport over real UDP multicast.
// It uses separate connections for reading and writing because
// outbound packets on the multicast listener are not delivered to other
// listeners on the same host.
type udpTransport struct {
	reader net.PacketConn
	sender *net.UDPConn
	group  *net.UDPAddr
}

func (u *udpTransport) ReadFrom(buf []byte) (int, error) {
	n, _, err := u.reader.ReadFrom(buf)
	return n, err
}

func (u *udpTransport) WriteTo(data []byte) error {
	_, err := u.sender.WriteToUDP(data, u.group)
	return err
}

func (u *udpTransport) Close() error {
	if err := u.sender.Close(); err != nil {
		slog.Debug("udpTransport sender close failed", "error", err)
	}
	return u.reader.Close()
}

func (u *udpTransport) SetReadDeadline(t time.Time) error {
	return u.reader.SetReadDeadline(t)
}

// resolveInterfaceIP returns the first IPv4 address of the given interface,
// or nil if iface is nil (use kernel default).
func resolveInterfaceIP(iface *net.Interface) net.IP {
	if iface == nil {
		return nil
	}
	addrs, err := iface.Addrs()
	if err != nil {
		slog.Debug("resolveInterfaceIP failed to get addresses",
			"interface", iface.Name,
			"error", err)
		return nil
	}
	for _, addr := range addrs {
		if ipnet, ok := addr.(*net.IPNet); ok && ipnet.IP.To4() != nil {
			return ipnet.IP.To4()
		}
	}
	return nil
}

// newMulticastConn creates a UDP socket joined to the given multicast group
// using SO_REUSEADDR without SO_REUSEPORT. Go's net.ListenMulticastUDP
// sets SO_REUSEPORT on Linux, which causes the kernel to load-balance
// incoming datagrams across sockets instead of delivering to all. Binding
// to the group address with SO_REUSEADDR alone allows multiple processes
// to receive every multicast packet.
func newMulticastConn(group *net.UDPAddr, iface *net.Interface) (net.PacketConn, error) {
	fd, err := syscall.Socket(syscall.AF_INET, syscall.SOCK_DGRAM|syscall.SOCK_CLOEXEC|syscall.SOCK_NONBLOCK, 0)
	if err != nil {
		return nil, err
	}

	if err := syscall.SetsockoptInt(fd, syscall.SOL_SOCKET, syscall.SO_REUSEADDR, 1); err != nil {
		if closeErr := syscall.Close(fd); closeErr != nil {
			slog.Debug("multicast socket close failed after SO_REUSEADDR error",
				"fd", fd,
				"error", closeErr)
		}
		return nil, err
	}

	// Bind to the multicast group address, not the wildcard.
	sa := &syscall.SockaddrInet4{Port: group.Port}
	copy(sa.Addr[:], group.IP.To4())
	if err := syscall.Bind(fd, sa); err != nil {
		if closeErr := syscall.Close(fd); closeErr != nil {
			slog.Debug("multicast socket close failed after bind error",
				"fd", fd,
				"error", closeErr)
		}
		return nil, err
	}

	// Join the multicast group on the specified interface.
	mreq := &syscall.IPMreq{}
	copy(mreq.Multiaddr[:], group.IP.To4())
	if ip := resolveInterfaceIP(iface); ip != nil {
		copy(mreq.Interface[:], ip)
	}
	if err := syscall.SetsockoptIPMreq(fd, syscall.IPPROTO_IP, syscall.IP_ADD_MEMBERSHIP, mreq); err != nil {
		if closeErr := syscall.Close(fd); closeErr != nil {
			slog.Debug("multicast socket close failed after IP_ADD_MEMBERSHIP error",
				"fd", fd,
				"error", closeErr)
		}
		return nil, err
	}

	file := os.NewFile(uintptr(fd), "multicast")
	conn, err := net.FilePacketConn(file)
	if closeErr := file.Close(); closeErr != nil {
		slog.Debug("multicast file close failed", "error", closeErr)
	}
	if err != nil {
		return nil, err
	}
	return conn, nil
}

// newMulticastSender creates a UDP socket configured for sending multicast
// packets on the specified interface. Setting IP_MULTICAST_IF ensures packets
// are sent on the same interface the listener joined, which is critical on
// hosts with multiple network interfaces (Docker bridges, VPNs, etc.).
func newMulticastSender(iface *net.Interface) (*net.UDPConn, error) {
	fd, err := syscall.Socket(syscall.AF_INET, syscall.SOCK_DGRAM|syscall.SOCK_CLOEXEC, 0)
	if err != nil {
		return nil, err
	}

	// Bind to an ephemeral port.
	sa := &syscall.SockaddrInet4{Port: 0}
	if err := syscall.Bind(fd, sa); err != nil {
		if closeErr := syscall.Close(fd); closeErr != nil {
			slog.Debug("multicast sender socket close failed after bind error",
				"fd", fd,
				"error", closeErr)
		}
		return nil, err
	}

	// Set IP_MULTICAST_IF so packets go out the correct interface.
	if ip := resolveInterfaceIP(iface); ip != nil {
		mreq := &syscall.IPMreq{}
		copy(mreq.Interface[:], ip)
		if err := syscall.SetsockoptIPMreq(fd, syscall.IPPROTO_IP, syscall.IP_MULTICAST_IF, mreq); err != nil {
			if closeErr := syscall.Close(fd); closeErr != nil {
				slog.Debug("multicast sender socket close failed after IP_MULTICAST_IF error",
					"fd", fd,
					"error", closeErr)
			}
			return nil, err
		}
	}

	// Enable multicast loopback so nodes on the same host discover each other.
	if err := syscall.SetsockoptInt(fd, syscall.IPPROTO_IP, syscall.IP_MULTICAST_LOOP, 1); err != nil {
		if closeErr := syscall.Close(fd); closeErr != nil {
			slog.Debug("multicast sender socket close failed after IP_MULTICAST_LOOP error",
				"fd", fd,
				"error", closeErr)
		}
		return nil, err
	}

	file := os.NewFile(uintptr(fd), "multicast-sender")
	fc, err := net.FileConn(file)
	if closeErr := file.Close(); closeErr != nil {
		slog.Debug("multicast sender file close failed", "error", closeErr)
	}
	if err != nil {
		return nil, err
	}
	conn, ok := fc.(*net.UDPConn)
	if !ok {
		if closeErr := fc.Close(); closeErr != nil {
			slog.Debug("multicast sender conn close failed after type assertion", "error", closeErr)
		}
		return nil, &Error{Method: "multicast", Err: &NoPeersError{Method: "sender socket type assertion failed"}}
	}
	return conn, nil
}

// MulticastConfig configures UDP multicast peer discovery.
type MulticastConfig struct {
	// NodeID is this node's unique identifier.
	NodeID uint64

	// Address is this node's Raft address to announce.
	Address string

	// DeploymentID isolates multicast to a specific deployment.
	DeploymentID uint64

	// SharedSecret is the HMAC key for authentication.
	SharedSecret []byte

	// AnnounceInterval is how often to send announcements.
	AnnounceInterval time.Duration

	// DiscoverTimeout is how long to listen for peers.
	DiscoverTimeout time.Duration

	// MulticastGroup is the multicast address (default "239.255.77.77:63777").
	MulticastGroup string

	// Interface is the network interface name to use for multicast (optional).
	Interface string

	// MinPeers is the minimum number of OTHER peers (excluding self) that
	// Discover() must find before returning. When 0, Discover() returns
	// after finding at least 1 peer.
	MinPeers int

	// transport overrides the default UDP transport (for testing).
	transport multicastTransport
}

// MulticastDiscovery discovers peers via UDP multicast with HMAC-SHA256 authentication.
// It periodically announces itself and collects announcements from other peers.
type MulticastDiscovery struct {
	cfg       MulticastConfig
	transport multicastTransport
	mu        sync.RWMutex
	peers     map[uint64]Peer
	stopped   atomic.Bool
	stopCh    chan struct{}
	wg        sync.WaitGroup
}

// NewMulticastDiscovery creates a multicast discovery instance and starts
// listening for peer announcements. Call Start() to begin periodic
// announcements.
func NewMulticastDiscovery(cfg MulticastConfig) (*MulticastDiscovery, error) {
	if len(cfg.SharedSecret) == 0 {
		return nil, &Error{
			Method: "multicast",
			Err:    &AuthenticationError{},
		}
	}
	if cfg.Address == "" {
		return nil, &Error{
			Method: "multicast",
			Err:    &NoPeersError{Method: "multicast"},
		}
	}

	if cfg.AnnounceInterval == 0 {
		cfg.AnnounceInterval = time.Second
	}
	if cfg.DiscoverTimeout == 0 {
		cfg.DiscoverTimeout = 10 * time.Minute
	}

	// Trial encode to validate configuration before starting goroutines.
	if _, encErr := EncodeMulticastMessage(cfg.DeploymentID, cfg.NodeID, cfg.Address, cfg.SharedSecret); encErr != nil {
		return nil, encErr
	}

	var transport multicastTransport
	if cfg.transport != nil {
		transport = cfg.transport
	} else {
		groupAddr := cfg.MulticastGroup
		if groupAddr == "" {
			groupAddr = DefaultMulticastGroup
		}

		group, err := net.ResolveUDPAddr("udp4", groupAddr)
		if err != nil {
			return nil, &Error{Method: "multicast", Err: err}
		}

		var iface *net.Interface
		if cfg.Interface != "" {
			iface, err = net.InterfaceByName(cfg.Interface)
			if err != nil {
				return nil, &Error{Method: "multicast", Err: err}
			}
		}

		reader, err := newMulticastConn(group, iface)
		if err != nil {
			return nil, &Error{Method: "multicast", Err: err}
		}

		// Create a separate socket for sending with IP_MULTICAST_IF set
		// to the same interface the listener joined. This ensures packets
		// are sent on the correct interface on multi-homed hosts.
		sender, err := newMulticastSender(iface)
		if err != nil {
			if closeErr := reader.Close(); closeErr != nil {
				slog.Debug("multicast reader close failed after sender creation error",
					"error", closeErr)
			}
			return nil, &Error{Method: "multicast", Err: err}
		}

		transport = &udpTransport{reader: reader, sender: sender, group: group}
	}

	md := &MulticastDiscovery{
		cfg:       cfg,
		transport: transport,
		peers:     make(map[uint64]Peer),
		stopCh:    make(chan struct{}),
	}

	md.wg.Add(1)
	go md.listen()

	return md, nil
}

// Start begins periodic announcements in a background goroutine.
func (md *MulticastDiscovery) Start() {
	md.wg.Add(1)
	go md.announceLoop()
}

// Discover returns the currently known peers. It waits up to DiscoverTimeout
// for enough peers to appear. When MinPeers is set, it waits for at least
// MinPeers other peers (excluding self). Otherwise it returns after finding
// at least 1 peer. The provided context allows the caller to cancel the
// wait early; a CancelledError is returned if the context is cancelled
// before enough peers are found.
func (md *MulticastDiscovery) Discover(ctx context.Context) ([]Peer, error) {
	if md.stopped.Load() {
		return nil, &Error{
			Method: md.Name(),
			Err:    &TimeoutError{Method: md.Name()},
		}
	}

	needed := md.cfg.MinPeers
	if needed < 1 {
		needed = 1
	}

	deadline := time.NewTimer(md.cfg.DiscoverTimeout)
	defer deadline.Stop()

	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()

	for {
		md.mu.RLock()
		count := len(md.peers)
		md.mu.RUnlock()

		if count >= needed {
			return md.snapshot(), nil
		}

		select {
		case <-ctx.Done():
			return nil, &CancelledError{
				Method: md.Name(),
				Err:    ctx.Err(),
			}
		case <-deadline.C:
			return nil, &TimeoutError{Method: md.Name()}
		case <-md.stopCh:
			return nil, &Error{
				Method: md.Name(),
				Err:    &TimeoutError{Method: md.Name()},
			}
		case <-ticker.C:
			// Continue polling.
		}
	}
}

// Name returns "multicast".
func (md *MulticastDiscovery) Name() string {
	return "multicast"
}

// Stop halts announcements and closes the multicast listener. It waits
// for all background goroutines to exit before returning.
func (md *MulticastDiscovery) Stop() error {
	if md.stopped.CompareAndSwap(false, true) {
		close(md.stopCh)
		err := md.transport.Close()
		md.wg.Wait()
		return err
	}
	return nil
}

// snapshot returns a copy of the current peer map as a slice.
func (md *MulticastDiscovery) snapshot() []Peer {
	md.mu.RLock()
	defer md.mu.RUnlock()
	result := make([]Peer, 0, len(md.peers))
	for _, p := range md.peers {
		result = append(result, p)
	}
	return result
}

// listen reads multicast messages and adds valid peers. It recovers
// from panics caused by malformed packets or encoding bugs and
// restarts the read loop unless the discovery has been stopped.
func (md *MulticastDiscovery) listen() {
	defer md.wg.Done()

	for !md.stopped.Load() {
		md.listenLoop()
	}
}

// listenLoop is the inner read loop for listen. It is separated so
// that panic recovery can restart the loop cleanly.
func (md *MulticastDiscovery) listenLoop() {
	defer func() {
		if r := recover(); r != nil {
			slog.Error("panic in multicast listener",
				"panic", r,
				"stack", string(debug.Stack()),
			)
		}
	}()

	buf := make([]byte, readBufferSize)
	for !md.stopped.Load() {
		if err := md.transport.SetReadDeadline(time.Now().Add(500 * time.Millisecond)); err != nil {
			slog.Debug("multicast transport set read deadline failed", "error", err)
		}
		n, err := md.transport.ReadFrom(buf)
		if err != nil {
			// Timeout errors are normal (read deadline expired).
			if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
				continue
			}
			// "use of closed network connection" is expected during shutdown.
			if md.stopped.Load() || errors.Is(err, net.ErrClosed) {
				return
			}
			slog.Warn("multicast listen read error", "error", err)
			continue
		}

		// Reject messages older than 2x the announce interval to prevent
		// replay attacks while tolerating network jitter.
		maxAge := 2 * md.cfg.AnnounceInterval
		deploymentID, nodeID, addr, decErr := DecodeMulticastMessage(buf[:n], md.cfg.SharedSecret, maxAge)
		if decErr != nil {
			slog.Debug("multicast message decode failed", "bytes", n, "error", decErr)
			continue
		}

		if deploymentID != md.cfg.DeploymentID {
			continue
		}

		// Skip self.
		if nodeID == md.cfg.NodeID {
			continue
		}

		md.mu.Lock()
		_, exists := md.peers[nodeID]
		md.peers[nodeID] = Peer{NodeID: nodeID, Address: addr}
		md.mu.Unlock()

		if !exists {
			slog.Info("multicast discovered peer", "node_id", nodeID, "address", addr)
		}
	}
}

// announceLoop sends periodic multicast announcements. It recovers
// from panics and restarts the loop unless the discovery has been
// stopped.
func (md *MulticastDiscovery) announceLoop() {
	defer md.wg.Done()

	for !md.stopped.Load() {
		md.announceIteration()
	}
}

// announceIteration is the inner announce loop. It is separated so
// that panic recovery can restart the loop cleanly.
func (md *MulticastDiscovery) announceIteration() {
	defer func() {
		if r := recover(); r != nil {
			slog.Error("panic in multicast announcer",
				"panic", r,
				"stack", string(debug.Stack()),
			)
		}
	}()

	ticker := time.NewTicker(md.cfg.AnnounceInterval)
	defer ticker.Stop()

	// Send an immediate announcement.
	md.sendAnnouncement()

	for {
		select {
		case <-md.stopCh:
			return
		case <-ticker.C:
			md.sendAnnouncement()
		}
	}
}

// sendAnnouncement encodes a fresh announcement with the current timestamp
// and writes it to the multicast group.
func (md *MulticastDiscovery) sendAnnouncement() {
	msg, err := EncodeMulticastMessage(md.cfg.DeploymentID, md.cfg.NodeID, md.cfg.Address, md.cfg.SharedSecret)
	if err != nil {
		slog.Warn("multicast announcement encode failed", "error", err)
		return
	}
	if err := md.transport.WriteTo(msg); err != nil {
		slog.Warn("multicast announcement send failed", "error", err)
	}
}

// EncodeMulticastMessage encodes a discovery message with HMAC authentication
// and a monotonic timestamp for replay protection.
// Format: [deploymentID:8 LE][nodeID:8 LE][timestamp:8 LE][addrLen:2 LE][addr:addrLen][hmac:32]
func EncodeMulticastMessage(deploymentID, nodeID uint64, addr string, secret []byte) ([]byte, error) {
	addrBytes := []byte(addr)
	if len(addrBytes) > maxAddrLen {
		return nil, &Error{
			Method: "multicast",
			Err:    &NoPeersError{Method: "address too long"},
		}
	}

	msgLen := headerSize + len(addrBytes)
	buf := make([]byte, msgLen+hmacSize)

	binary.LittleEndian.PutUint64(buf[0:8], deploymentID)
	binary.LittleEndian.PutUint64(buf[8:16], nodeID)
	binary.LittleEndian.PutUint64(buf[16:24], uint64(time.Now().UnixNano()))
	binary.LittleEndian.PutUint16(buf[24:26], uint16(len(addrBytes)))
	copy(buf[26:], addrBytes)

	mac := computeHMAC(buf[:msgLen], secret)
	copy(buf[msgLen:], mac)

	return buf, nil
}

// DecodeMulticastMessage decodes and authenticates a discovery message.
// Returns the deploymentID, nodeID, and address on success.
// The maxAge parameter specifies the maximum age of the message in
// nanoseconds. Messages older than maxAge are rejected as replays.
// If maxAge is 0, no timestamp validation is performed.
func DecodeMulticastMessage(data, secret []byte, maxAge time.Duration) (deploymentID, nodeID uint64, addr string, err error) {
	if len(data) < headerSize+hmacSize {
		return 0, 0, "", &Error{
			Method: "multicast",
			Err:    &AuthenticationError{},
		}
	}

	addrLen := int(binary.LittleEndian.Uint16(data[24:26]))
	expectedLen := headerSize + addrLen + hmacSize

	if len(data) != expectedLen {
		return 0, 0, "", &Error{
			Method: "multicast",
			Err:    &AuthenticationError{},
		}
	}

	msgLen := headerSize + addrLen
	msgMAC := data[msgLen : msgLen+hmacSize]
	expectedMAC := computeHMAC(data[:msgLen], secret)

	if !hmac.Equal(msgMAC, expectedMAC) {
		return 0, 0, "", &AuthenticationError{}
	}

	deploymentID = binary.LittleEndian.Uint64(data[0:8])
	nodeID = binary.LittleEndian.Uint64(data[8:16])
	timestampNano := binary.LittleEndian.Uint64(data[16:24])
	addr = string(data[26 : 26+addrLen])

	// Replay protection: reject messages with timestamps too far in
	// the past. The threshold is 2x the announce interval to allow
	// for network jitter and processing delays.
	if maxAge > 0 {
		msgTime := time.Unix(0, int64(timestampNano))
		age := time.Since(msgTime)
		if age > maxAge || age < -maxAge {
			return 0, 0, "", &ReplayError{}
		}
	}

	return deploymentID, nodeID, addr, nil
}

// computeHMAC computes HMAC-SHA256 over data with the given key.
func computeHMAC(data, key []byte) []byte {
	mac := hmac.New(sha256.New, key)
	mac.Write(data)
	return mac.Sum(nil)
}
