# Discovery and Bootstrap

## Overview

The discovery and bootstrap subsystems handle the two-phase problem of cluster formation: finding peers (discovery) and then initializing a Raft shard across those peers (bootstrap). These are implemented in two separate packages with a clean dependency boundary -- bootstrap depends on discovery, but discovery has no knowledge of Raft.

```
pkg/discovery/    Peer discovery: static, multicast, DNS SRV
pkg/bootstrap/    Cluster formation orchestration
```

## Discovery

Source: `pkg/discovery/`

### Purpose

Discovery answers one question: "Who are my peers?" It runs before any Raft protocol activity. Each discovery method returns a `[]Peer` slice containing node identifiers and network addresses. The caller (typically the Bootstrapper) uses this list to build the initial Raft membership.

### Core Types

```go
// Peer represents a discovered Raft peer.
type Peer struct {
    NodeID  uint64  // unique identifier for this peer
    Address string  // network address (host:port)
}

// Config holds common discovery configuration.
type Config struct {
    DeploymentID uint64  // isolates discovery to a specific cluster deployment
}
```

### Method Interface

Every discovery strategy implements the `Method` interface:

```go
type Method interface {
    Discover() ([]Peer, error)  // returns discovered peers; may block
    Name() string               // human-readable name ("static", "multicast", "dns")
    Stop() error                // releases resources held by the method
}
```

The interface is intentionally minimal. `Discover()` is synchronous and may block up to the method's configured timeout. `Stop()` is idempotent.

---

## Static Discovery

Source: `pkg/discovery/static.go`

The simplest method. All peer addresses are known at deployment time and provided in configuration. No network I/O occurs during `Discover()`.

### Configuration

```go
type StaticConfig struct {
    Peers []Peer  // fixed list of peers
}
```

### Behavior

- `NewStaticDiscovery(cfg)` deep-copies the peer list to prevent external mutation.
- `Discover()` returns a fresh copy on each call. Returns `*NoPeersError` if the configured list is empty.
- `Stop()` is a no-op.
- `Name()` returns `"static"`.

### When to Use

- Development and testing environments.
- Infrastructure where all node addresses are assigned by an orchestrator (Kubernetes StatefulSets, Terraform, static inventory).
- Clusters with stable, well-known membership.

```go
sd := discovery.NewStaticDiscovery(discovery.StaticConfig{
    Peers: []discovery.Peer{
        {NodeID: 1, Address: "10.0.0.1:4001"},
        {NodeID: 2, Address: "10.0.0.2:4001"},
        {NodeID: 3, Address: "10.0.0.3:4001"},
    },
})
peers, err := sd.Discover()
```

---

## Multicast Discovery

Source: `pkg/discovery/multicast.go`

LAN peer discovery via UDP multicast with HMAC-SHA256 authentication. Nodes announce themselves periodically on a multicast group and collect announcements from other nodes.

### Configuration

```go
type MulticastConfig struct {
    NodeID           uint64        // this node's unique identifier
    Address          string        // this node's Raft address to announce
    DeploymentID     uint64        // isolates multicast to a specific deployment
    SharedSecret     []byte        // HMAC key for authentication (required)
    AnnounceInterval time.Duration // how often to send announcements (default: 1s)
    DiscoverTimeout  time.Duration // how long to listen for peers (default: 10m)
    MulticastGroup   string        // multicast address (default: "239.255.77.77:63777")
    Interface        string        // network interface name (optional)
    MinPeers         int           // min OTHER peers to find before returning (default: 1)
}
```

### Wire Format

Multicast messages use a fixed binary layout with HMAC-SHA256 authentication:

```
Offset  Size    Field
------  ------  -----
0       8       DeploymentID  (uint64 LE)
8       8       NodeID        (uint64 LE)
16      2       AddrLen       (uint16 LE)
18      var     Address       (AddrLen bytes, max 253)
18+N    32      HMAC-SHA256   (over bytes 0..18+N-1)
```

Total: 50 bytes minimum (empty address), up to 303 bytes maximum.

Constants:

| Name             | Value | Description                        |
|------------------|-------|------------------------------------|
| `headerSize`     | 18    | deploymentID(8) + nodeID(8) + addrLen(2) |
| `hmacSize`       | 32    | HMAC-SHA256 digest length          |
| `maxAddrLen`     | 253   | maximum address string length      |
| `maxMessageSize` | 303   | headerSize + maxAddrLen + hmacSize |
| `readBufferSize` | 512   | UDP read buffer size               |

### Authentication

Every message is authenticated with HMAC-SHA256 computed over the header and address bytes (everything before the HMAC field). The shared secret must be configured on all nodes. Messages with invalid HMACs are silently discarded. This prevents:

- Spoofed peer announcements from unauthorized nodes.
- Cross-cluster pollution when multiple deployments share a network.

The `DeploymentID` field provides additional isolation. Messages from a different deployment are discarded even if the HMAC is valid.

### Multi-Homed Hosts

On systems with multiple network interfaces, the sender socket requires `IP_MULTICAST_IF` and `IP_MULTICAST_LOOP` socket options for correct operation. The `resolveInterfaceIP()` helper maps an interface name to its IP address. When `Interface` is unspecified, UDP multicast uses the system default interface.

### Lifecycle

```
NewMulticastDiscovery(cfg)
    |
    |-- validates SharedSecret (required) and Address (required)
    |-- defaults: AnnounceInterval=1s, DiscoverTimeout=10m
    |-- encodes the announce message once (EncodeMulticastMessage)
    |-- creates UDP transport (separate reader + sender sockets)
    |-- starts listen() goroutine (background)
    v
md.Start()
    |
    |-- starts announceLoop() goroutine
    |-- sends immediate announcement, then periodic at AnnounceInterval
    v
md.Discover()
    |
    |-- polls peer map every 50ms
    |-- returns when len(peers) >= MinPeers (default 1)
    |-- returns *TimeoutError if DiscoverTimeout expires
    v
md.Stop()
    |
    |-- atomic CAS on stopped flag (idempotent)
    |-- closes stopCh channel (unblocks listen + announceLoop)
    |-- closes transport (reader + sender sockets)
```

### Transport Abstraction

The `multicastTransport` interface abstracts UDP I/O for testability:

```go
type multicastTransport interface {
    ReadFrom(buf []byte) (int, error)
    WriteTo(data []byte) error
    Close() error
    SetReadDeadline(t time.Time) error
}
```

The production implementation (`udpTransport`) uses separate reader and sender UDP connections. The reader is created via `net.ListenMulticastUDP` and the sender via `net.ListenUDP` on an ephemeral port. This separation is required because packets sent via a multicast listener socket are not delivered to other listeners on the same host.

### Self-Filtering

The listen loop skips messages where `nodeID == cfg.NodeID`, preventing a node from discovering itself via its own announcements.

```go
md, err := discovery.NewMulticastDiscovery(discovery.MulticastConfig{
    NodeID:       1,
    Address:      "10.0.0.1:4001",
    DeploymentID: 42,
    SharedSecret: []byte("cluster-secret-key"),
    MinPeers:     2,
})
md.Start()
defer md.Stop()
peers, err := md.Discover()
```

---

## DNS Discovery

Source: `pkg/discovery/dns.go`

Discovers peers by querying DNS SRV records. Suitable for environments where a service registry populates DNS (Consul, CoreDNS, AWS Route53, Kubernetes headless services).

### Configuration

```go
type DNSConfig struct {
    Service       string        // SRV service name (e.g., "_raft")
    Proto         string        // SRV protocol (e.g., "_udp")
    Domain        string        // DNS domain to query
    LookupTimeout time.Duration // DNS query timeout (default: 5s)
    LookupFn      LookupSRVFunc // override net.LookupSRV for testing (optional)
}
```

The `LookupSRVFunc` type signature matches `net.LookupSRV`:

```go
type LookupSRVFunc func(service, proto, name string) (string, []*net.SRV, error)
```

### Behavior

- `NewDNSDiscovery(cfg)` stores the config and resolver function. Defaults `LookupFn` to `net.LookupSRV` and `LookupTimeout` to 5s.
- `Discover()` calls `lookupFn(Service, Proto, Domain)`.
- Each SRV record is mapped to a `Peer`:
  - `Address` = `net.JoinHostPort(target, port)` (trailing dot stripped from target).
  - `NodeID` = FNV-1a 64-bit hash of the address string, since SRV records do not carry node identifiers.
- Returns `*NoPeersError` when zero SRV records are found.
- Returns `*Error` wrapping the DNS lookup error on failure.
- `Stop()` is a no-op.
- `Name()` returns `"dns"`.

### NodeID Derivation

SRV records have no concept of a node identifier. DNS discovery derives `NodeID` from an FNV-1a hash of the `host:port` string:

```go
func fnvHash(s string) uint64 {
    h := fnv.New64a()
    h.Write([]byte(s))
    return h.Sum64()
}
```

This means the same address always produces the same NodeID. Callers that need stable, operator-assigned NodeIDs should use static discovery or set NodeIDs after discovery.

### SRV Record Format

The query targets `_<service>._<proto>.<domain>`. For example, with `Service="_raft"`, `Proto="_udp"`, `Domain="cluster.local"`:

```
_raft._udp.cluster.local.  IN  SRV  0 0 4001  node1.cluster.local.
_raft._udp.cluster.local.  IN  SRV  0 0 4001  node2.cluster.local.
_raft._udp.cluster.local.  IN  SRV  0 0 4001  node3.cluster.local.
```

```go
dd := discovery.NewDNSDiscovery(discovery.DNSConfig{
    Service: "_raft",
    Proto:   "_udp",
    Domain:  "cluster.local",
})
peers, err := dd.Discover()
```

---

## Discovery Manager

Source: `pkg/discovery/manager.go`

The Manager provides fallback chain behavior: it tries multiple discovery methods in order and returns the first successful result.

### Configuration

```go
type ManagerConfig struct {
    Methods       []Method      // ordered list of methods to try
    RetryInterval time.Duration // time between retries (default: 1s)
    MaxRetries    int           // max retry attempts; 0 means no retry
}
```

### Algorithm

```
for attempt in 0..(1 + MaxRetries):
    if attempt > 0:
        sleep(RetryInterval)
    for method in Methods:
        peers, err = method.Discover()
        if err == nil and len(peers) > 0:
            return peers
        lastErr = err
return lastErr or NoPeersError{Method: "manager"}
```

Total attempts = `1 + MaxRetries`. Methods are tried in declaration order within each attempt. The first method that returns a non-empty peer list wins. If all methods fail across all attempts, the last error is returned.

### Additional Methods

- `Peers()` returns the most recently discovered peer list (deep copy). Returns nil if `Discover()` has not yet succeeded.
- `Stop()` calls `Stop()` on every configured method. Returns the first error encountered.

### Fallback Pattern

A typical production setup might try static first (fast, no I/O), then fall back to DNS:

```go
mgr := discovery.NewManager(discovery.ManagerConfig{
    Methods: []discovery.Method{
        discovery.NewStaticDiscovery(staticCfg),
        discovery.NewDNSDiscovery(dnsCfg),
    },
    RetryInterval: 2 * time.Second,
    MaxRetries:    5,
})
defer mgr.Stop()
peers, err := mgr.Discover()
```

---

## Bootstrap

Source: `pkg/bootstrap/`

### Purpose

The Bootstrapper bridges discovery and Raft. It takes a discovered peer list, validates quorum, builds the initial membership map, and calls `RaftNode.StartShard()` to initialize the Raft group.

### RaftNode Interface

The Bootstrapper depends on a minimal interface that `Host` satisfies:

```go
type RaftNode interface {
    StartShard(members map[uint64]string, join bool, create sm.CreateFunc, cfg config.Config) error
    GetLeaderID(shardID uint64) (uint64, bool, error)
}
```

### Configuration

```go
type Config struct {
    NodeID      uint64           // this node's unique identifier
    Address     string           // this node's Raft address
    ShardID     uint64           // shard to bootstrap
    ReplicaID   uint64           // this node's replica ID
    Discovery   discovery.Method // discovery method to use
    CreateFn    sm.CreateFunc    // state machine factory
    ShardConfig config.Config    // per-shard Raft configuration
    MinPeers    int              // minimum peers for quorum (default: 3)
}
```

### Bootstrap Flow

```
Bootstrapper.Bootstrap()
    |
    |  1. DISCOVER
    |  peers, err = cfg.Discovery.Discover()
    |  (wrapped in Error{Phase: "discovery"} on failure)
    |
    |  2. ADD SELF
    |  self = Peer{NodeID: cfg.NodeID, Address: cfg.Address}
    |  peers = addSelfToPeers(peers, self)
    |  (deduplicated: skips if NodeID already present)
    |
    |  3. VALIDATE QUORUM
    |  needed = QuorumSize(cfg.MinPeers)     // n/2 + 1
    |  if len(peers) < needed:
    |      return QuorumError{Have: len(peers), Need: needed}
    |
    |  4. BUILD MEMBER MAP
    |  members = BuildMemberMap(peers)        // map[NodeID]Address
    |
    |  5. START SHARD
    |  node.StartShard(members, false, cfg.CreateFn, cfg.ShardConfig)
    |  (wrapped in Error{Phase: "start-shard"} on failure)
    v
    return nil (success)
```

The `join` parameter to `StartShard` is always `false` because bootstrap creates a new cluster -- it does not join an existing one.

### Cluster Formation Helpers

Source: `pkg/bootstrap/cluster_formation.go`

These are pure functions used by the Bootstrapper and available for direct use by callers that need finer-grained control.

**QuorumSize** computes majority quorum:

```go
func QuorumSize(n int) int {
    return n/2 + 1
}
```

| n (MinPeers) | QuorumSize |
|--------------|------------|
| 1            | 1          |
| 2            | 2          |
| 3            | 2          |
| 5            | 3          |
| 7            | 4          |
| 9            | 5          |

**HasQuorum** checks if discovered peers meet quorum:

```go
func HasQuorum(peers []discovery.Peer, minPeers int) bool {
    return len(peers) >= QuorumSize(minPeers)
}
```

**DeterministicLeader** selects the peer with the lowest NodeID. All nodes running the same algorithm will agree on the initial leader without communication. The input slice is copied before sorting (no mutation).

```go
func DeterministicLeader(peers []discovery.Peer) discovery.Peer
```

**BuildMemberMap** converts a peer slice to the `map[uint64]string` format required by `StartShard`. Duplicate NodeIDs resolve with last-write-wins semantics.

```go
func BuildMemberMap(peers []discovery.Peer) map[uint64]string
```

---

## Error Types

### Discovery Errors

Source: `pkg/discovery/errors.go`

| Type                  | Fields           | Description                                      |
|-----------------------|------------------|--------------------------------------------------|
| `*Error`              | Method, Err      | General discovery failure; wraps underlying error |
| `*NoPeersError`       | Method           | No peers found by the named method               |
| `*TimeoutError`       | Method           | Discovery operation timed out                    |
| `*AuthenticationError`| (none)           | HMAC verification failed                         |

`Error` implements `Unwrap() error` for `errors.Is`/`errors.As` compatibility. All error types produce structured messages:

```
discovery: multicast: connection refused     (Error)
discovery: static: no peers found            (NoPeersError)
discovery: multicast: timed out              (TimeoutError)
discovery: authentication failed             (AuthenticationError)
```

### Bootstrap Errors

Source: `pkg/bootstrap/errors.go`

| Type              | Fields       | Description                                  |
|-------------------|--------------|----------------------------------------------|
| `*QuorumError`    | Have, Need   | Insufficient peers to form quorum            |
| `*Error`          | Phase, Err   | Failure during a bootstrap phase             |

`Error` implements `Unwrap() error`. The `Phase` field identifies where the failure occurred:

```
bootstrap: discovery: <underlying error>     (Phase: "discovery")
bootstrap: start-shard: <underlying error>   (Phase: "start-shard")
bootstrap: insufficient peers for quorum (have 1, need 2)  (QuorumError)
```

---

## Usage Examples

### Static Bootstrap (3-node cluster)

```go
import (
    "github.com/jeremyhahn/go-quicraft/pkg/bootstrap"
    "github.com/jeremyhahn/go-quicraft/pkg/config"
    "github.com/jeremyhahn/go-quicraft/pkg/discovery"
    "github.com/jeremyhahn/go-quicraft/pkg/sm"
)

// Each node runs this with its own NodeID, Address, and ReplicaID.
sd := discovery.NewStaticDiscovery(discovery.StaticConfig{
    Peers: []discovery.Peer{
        {NodeID: 1, Address: "10.0.0.1:4001"},
        {NodeID: 2, Address: "10.0.0.2:4001"},
        {NodeID: 3, Address: "10.0.0.3:4001"},
    },
})

bs := bootstrap.NewBootstrapper(bootstrap.Config{
    NodeID:    1,
    Address:   "10.0.0.1:4001",
    ShardID:   100,
    ReplicaID: 1,
    Discovery: sd,
    CreateFn:  sm.NewCreateFunc(newKVStore),
    ShardConfig: config.Config{
        ShardID:    100,
        ReplicaID:  1,
        ElectionRTT:  10,
        HeartbeatRTT: 1,
        CheckQuorum:  true,
        PreVote:      true,
    },
    MinPeers: 3,
}, host)

if err := bs.Bootstrap(); err != nil {
    log.Fatalf("bootstrap failed: %v", err)
}
```

### Multicast Bootstrap (LAN auto-discovery)

```go
md, err := discovery.NewMulticastDiscovery(discovery.MulticastConfig{
    NodeID:           nodeID,
    Address:          "10.0.0.1:4001",
    DeploymentID:     42,
    SharedSecret:     []byte("my-cluster-secret"),
    AnnounceInterval: time.Second,
    DiscoverTimeout:  30 * time.Second,
    MinPeers:         2,
})
if err != nil {
    log.Fatalf("multicast init: %v", err)
}
md.Start()
defer md.Stop()

bs := bootstrap.NewBootstrapper(bootstrap.Config{
    NodeID:    nodeID,
    Address:   "10.0.0.1:4001",
    ShardID:   100,
    ReplicaID: replicaID,
    Discovery: md,
    CreateFn:  sm.NewCreateFunc(newKVStore),
    ShardConfig: shardCfg,
    MinPeers:  3,
}, host)

if err := bs.Bootstrap(); err != nil {
    log.Fatalf("bootstrap failed: %v", err)
}
```

### DNS Bootstrap (service discovery)

```go
dd := discovery.NewDNSDiscovery(discovery.DNSConfig{
    Service: "_raft",
    Proto:   "_udp",
    Domain:  "cluster.local",
})

// Wrap in a manager with retries for DNS propagation delays.
mgr := discovery.NewManager(discovery.ManagerConfig{
    Methods:       []discovery.Method{dd},
    RetryInterval: 2 * time.Second,
    MaxRetries:    15,
})
defer mgr.Stop()

bs := bootstrap.NewBootstrapper(bootstrap.Config{
    NodeID:    nodeID,
    Address:   "node1.cluster.local:4001",
    ShardID:   100,
    ReplicaID: replicaID,
    Discovery: mgr,
    CreateFn:  sm.NewCreateFunc(newKVStore),
    ShardConfig: shardCfg,
    MinPeers:  3,
}, host)

if err := bs.Bootstrap(); err != nil {
    log.Fatalf("bootstrap failed: %v", err)
}
```

### Fallback Chain (static then DNS)

```go
mgr := discovery.NewManager(discovery.ManagerConfig{
    Methods: []discovery.Method{
        discovery.NewStaticDiscovery(staticCfg),  // try first (fast, no I/O)
        discovery.NewDNSDiscovery(dnsCfg),        // fallback (DNS lookup)
    },
    RetryInterval: time.Second,
    MaxRetries:    10,
})
defer mgr.Stop()

bs := bootstrap.NewBootstrapper(bootstrap.Config{
    NodeID:    nodeID,
    Address:   addr,
    ShardID:   100,
    ReplicaID: replicaID,
    Discovery: mgr,
    CreateFn:  sm.NewCreateFunc(newKVStore),
    ShardConfig: shardCfg,
    MinPeers:  3,
}, host)

if err := bs.Bootstrap(); err != nil {
    log.Fatalf("bootstrap failed: %v", err)
}
```

---

## Cross-References

- [Architecture](../architecture/overview.md) -- System overview and package structure
- [Transport](../transport/quic.md) -- QUIC transport that peers use after bootstrap
