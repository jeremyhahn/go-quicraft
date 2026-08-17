<img src="assets/logo.svg" width="600">


[![Go](https://img.shields.io/badge/Go-1.26.6-blue.svg)](https://go.dev/)
[![License](https://img.shields.io/badge/License-Apache%202.0-green.svg)](LICENSE)
[![Version](https://img.shields.io/badge/Version-0.6.1--alpha-orange.svg)](VERSION)

A multi-group Raft consensus library for Go with QUIC transport and at-rest encryption.

QuicRaft implements the [Raft consensus algorithm](https://raft.github.io/), audited against the [extended Raft paper](docs/references/raft.pdf) and the [Ongaro PhD thesis](docs/references/OngaroPhD.pdf). It carries 6,500+ test functions and 37 fuzz targets across 27 packages, including a Porcupine-based linearizability suite, at 91.2% aggregate statement coverage. Coverage is uneven -- see [Test Coverage](#test-coverage) for the packages below the project's own floor.

## Installation

```sh
go get github.com/jeremyhahn/go-quicraft
```

Requires Go 1.26.6 or later. The public API lives under
`github.com/jeremyhahn/go-quicraft/pkg`.

> **Alpha software.** This is a `0.x` release. The on-disk and on-wire formats
> are not yet stable and have broken across releases.
>
> **0.5.x cannot read a 0.4.x data directory.** The `proto.Entry` header went
> from 60 to 68 bytes, the WAL entry record from 76 to 84, and the sealed-state
> MAC from v1 to v2. There is no in-place upgrade and no dual-read path: wipe
> the data directory, or export snapshots with 0.4.x and re-import. Sealed
> barrier state is the one exception: a pre-fence blob is recognised once and
> restamped on the next persist. Every wire and persisted format stays pinned
> at version 1 until the first production release, so a layout change is made in
> place and the version number does not record it. Build every node and every
> consuming service from one commit.
>
> See [CHANGELOG.md](CHANGELOG.md) and
> [docs/operations/compatibility.md](docs/operations/compatibility.md) before
> upgrading.

## Features

- **Multi-group Raft** -- Thousands of independent Raft groups per process with a double-buffered ReadyMap for O(1) shard dispatch
- **Minimal dependencies** -- No external or embedded database dependencies for Raft log storage
- **QUIC transport** -- Built on [quic-go](https://github.com/quic-go/quic-go) with mandatory mTLS and multiplexed streams. There is no TCP fallback. 0-RTT is deliberately not used: the dial path never sends early data, and message frames that arrive over 0-RTT are refused because early data is replayable
- **Custom binary wire format** -- Fixed-layout little-endian encoding with zero-copy semantics; nothing on the wire is protobuf
- **At-rest encryption** -- Authenticated encryption (AES-GCM or ChaCha20-Poly1305) with automatic hardware detection, barrier/seal/unseal pattern, epoch-based key rotation, and FIPS 140 build support
- **Sharded WAL** -- 16-shard write-ahead log for parallel persistence with fdatasync durability
- **Raft protocol features** -- Leader election, log replication, membership changes, snapshots, ReadIndex, LeaseRead, PreVote, leadership transfer, CheckQuorum
- **Client sessions** -- At-most-once delivery with deduplication across replicas (PhD Figure 6.1). Expiry is index-based, so every replica expires the same session at the same log index
- **Witness replicas** -- Metadata-only voting members that carry Index/Term without the command payload. They count toward both the election and the commit quorum, and the policy bounding them is replicated state in `proto.Membership`
- **Three state machine types** -- `StateMachine`, `ConcurrentStateMachine`, and `DiskStateMachine` interfaces for different workload profiles
- **Cluster primitives** -- explicit per-shard member maps via StartShard; discovery and bootstrap orchestration live in go-qrdb
- **Observability** -- Prometheus metrics for proposals, commits, elections, snapshots, transport, and WAL operations
- **Atomics-first hot paths** -- Atomics and `sync.Map` over mutexes where the access pattern allows it (shard lookup, leader/membership snapshots, the rate limiter's CAS loop); the proposal and read-index queues use a briefly-held mutex amortized over a batched drain
- **Batch proposal/read coalescing** -- ReadIndex requests are batched to amortize heartbeat quorum rounds
- **Encrypted snapshot transfer** -- Snapshots decrypted before transport (per-node barriers), QUIC provides in-transit encryption

## Quick Start

### Starting a single-node cluster with a KV state machine

```go
package main

import (
    "context"
    "fmt"
    "log"
    "time"

    quicraft "github.com/jeremyhahn/go-quicraft/pkg"
    "github.com/jeremyhahn/go-quicraft/pkg/config"
    "github.com/jeremyhahn/go-quicraft/pkg/sm/kv"
)

func main() {
    // Channel notified when a leader is elected
    leaderCh := make(chan struct{}, 1)

    hostCfg := config.HostConfig{
        WALDir:        "/var/data/quicraft/wal",
        NodeHostDir:   "/var/data/quicraft",
        RaftAddress:   "localhost:63100",
        ListenAddress: "0.0.0.0:63100",
        DeploymentID:  1,
        EventListener: &config.EventListener{
            OnLeaderUpdated: func(info config.LeaderInfo) {
                select {
                case leaderCh <- struct{}{}:
                default:
                }
            },
        },
    }

    host, err := quicraft.NewHost(hostCfg, quicraft.WithoutTransport())
    if err != nil {
        log.Fatal(err)
    }
    defer host.Close()

    // Single-node cluster
    members := map[uint64]string{
        1: "localhost:63100",
    }

    shardCfg := config.Config{
        ShardID:   1,
        ReplicaID: 1,
    }
    if err := host.StartShard(members, false, kv.NewMemoryCreateFunc(), shardCfg); err != nil {
        log.Fatal(err)
    }

    // Wait for leader election via EventListener callback
    <-leaderCh

    // Write a key
    ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
    defer cancel()

    cmd, _ := kv.EncodePut([]byte("hello"), []byte("world"))
    if _, err := host.SyncPropose(ctx, 1, cmd); err != nil {
        log.Fatal(err)
    }

    // Linearizable read
    val, err := host.SyncRead(ctx, 1, []byte("hello"))
    if err != nil {
        log.Fatal(err)
    }
    fmt.Printf("hello = %s\n", val.([]byte)) // hello = world
}
```

### Write modes

QuicRaft supports three write durability modes. Pick the one that matches your latency and durability requirements.

**Sync** -- strongest durability. Blocks until the entry is replicated to a quorum and applied to the state machine. This is the default and the right choice when you need read-after-write consistency.

```go
// Sync write -- blocks until quorum commit + SM apply (~10-30ms)
result, err := host.SyncPropose(ctx, shardID, cmd)
if err != nil {
    return err
}
fmt.Println("state machine returned:", result.Value)

// Async variant -- returns immediately, result arrives on channel
rs, err := host.Propose(ctx, shardID, cmd)
if err != nil {
    return err
}
defer rs.Release()

select {
case result := <-rs.ResultC():
    if result.Err != nil {
        return result.Err
    }
    fmt.Println("applied at index:", result.Value)
case <-ctx.Done():
    return ctx.Err()
}
```

**WAL-durable** -- lower latency. Returns after the entry is persisted to the local WAL (~1-2ms) but before quorum replication. If the leader crashes before replicating, the entry can be lost. Enable `NotifyCommit` on the host to use this mode.

```go
hostCfg := config.HostConfig{
    // ...
    NotifyCommit: true, // Enable early WAL-durable notifications
}

rs, err := host.Propose(ctx, shardID, cmd)
if err != nil {
    return err
}
defer rs.Release()

select {
case <-rs.CommittedC():
    // Entry is durable in local WAL (~1-2ms). Quorum replication
    // is still in progress -- safe to acknowledge the client if
    // you can tolerate loss on leader crash.
case result := <-rs.ApplyResultC():
    // Full quorum commit + SM apply (~10-30ms). Use this channel
    // when you need the applied result or read-after-write safety.
    return result.Err
case <-ctx.Done():
    return ctx.Err()
}
```

**Batched** -- amortized cost. Groups many writes into a single Raft entry, giving the same quorum durability as Sync at a fraction of the per-operation cost (~0.1-0.3ms per op). Good for high-throughput ingest where individual write latency matters less than aggregate throughput.

> **Delivery is at-least-once, not at-most-once.** The aggregator proposes a raw encoded batch with no client session, so nothing deduplicates a retry. A flush that fails with `FlushError.Indeterminate` may nonetheless have committed, and retrying re-applies every command in that batch. A batched proposal also cannot report a per-item apply failure: the aggregate result is shared by every co-batched caller. Use `SyncProposeWithSession` for operations that are not idempotent.

```go
import (
    "errors"
    "time"

    "github.com/jeremyhahn/go-quicraft/pkg/batch"
)

// *Host.SyncPropose returns (sm.Result, error); Proposer wants error alone,
// so the Host is wrapped. The discarded Result is the limitation above: a
// batched proposal cannot attribute a result to an individual command.
agg, err := batch.NewAggregator(
    batch.HostProposer{Host: host},
    batch.WithConfig(batch.Config{
        MaxBatchSize:   256,                  // Flush after 256 ops
        MaxBatchBytes:  4 << 20,              // ...or 4 MiB, whichever first
        FlushInterval:  5 * time.Millisecond, // ...or after 5ms
        ProposeTimeout: 10 * time.Second,     // Bounds a stalled flush
        QueueSize:      4096,                 // Backpressure at 4096 pending
    }),
)
if err != nil {
    log.Fatal(err)
}
defer agg.Close()

// Submit blocks until the batch containing this op is committed.
// Many concurrent Submit calls are grouped into one Raft proposal.
if err := agg.Submit(ctx, shardID, cmd); err != nil {
    var fe *batch.FlushError
    if errors.As(err, &fe) && fe.Indeterminate {
        // The command MAY have committed. Do not blindly retry a
        // non-idempotent operation here.
    }
}
```

### Read consistency levels

Four read modes, from strongest to fastest:

| Mode | Consistency | Latency | Method |
|------|-------------|---------|--------|
| ReadIndex | Linearizable | ~5-15ms (heartbeat RTT) | `SyncRead` |
| LeaseRead | Linearizable | ~0.5-2ms (no RTT when lease valid) | `SyncRead` with `LeaseRead: true` |
| Stale | Eventual | ~0.01ms (local SM query) | `StaleRead` |
| Local | None (bypass Raft) | ~0.01ms | `QueryLocalNode` |

The latency figures above and in the durability-mode section are order-of-magnitude
guidance for a 3-node cluster on a low-latency LAN, not measurements you should plan
capacity against. They move with network RTT, disk fsync latency, state-machine cost
and shard count. Measure your own deployment; `test/performance/` holds the
comparative suite and the hardware each published result was produced on.

```go
// ReadIndex -- linearizable. Confirms leadership via heartbeat quorum,
// then queries the state machine. Default mode.
val, err := host.SyncRead(ctx, shardID, []byte("mykey"))

// LeaseRead -- linearizable, lower latency. When the leader has received
// heartbeat acks from a quorum within the lease window (ElectionRTT - 2
// ticks), it skips the heartbeat round-trip entirely. Falls back to
// ReadIndex when the lease has expired. Based on Ongaro PhD thesis 6.4.
// Enable per-shard:
shardCfg := config.Config{
    ShardID:   1,
    ReplicaID: 1,
    LeaseRead: true, // Requires CheckQuorum, which is on by default (CheckQuorum is a *bool; nil = enabled)
}
if err := host.StartShard(members, false, kv.NewMemoryCreateFunc(), shardCfg); err != nil {
    return err
}

// SyncRead is the same call -- LeaseRead is transparent to the caller.
val, err = host.SyncRead(ctx, shardID, []byte("mykey"))

// Stale read -- reads from the local state machine without any consensus
// round-trip. Fast but may return stale data if this node is behind.
val, err = host.StaleRead(ctx, shardID, []byte("mykey"))

// QueryLocalNode -- direct SM.Lookup bypass. No Raft involvement at all.
// Only use when you know exactly what you're doing.
val, err = host.QueryLocalNode(ctx, shardID, []byte("mykey"))
```

**Zero-copy reads** -- for hot paths where interface boxing allocation matters, use the `Buf` variants. These return a `[]byte` directly (via the `NALookup` interface) and a release function for the underlying buffer:

```go
val, release, err := host.SyncReadBuf(ctx, shardID, []byte("mykey"))
if err != nil {
    return err
}
defer release()
// Use val before release() returns the buffer to the pool.
process(val)
```

### Session-managed writes

Sessions provide at-most-once delivery for non-idempotent operations. The session manager deduplicates retransmissions using a (clientID, seriesID) pair tracked through Raft consensus.

```go
// Register a session through Raft (all replicas agree on the session state)
session, err := host.GetNewSession(ctx, shardID)
if err != nil {
    return err
}
defer host.CloseSession(ctx, shardID, session)

// Propose with the session -- retries are automatically deduplicated
rs, err := host.ProposeWithSession(ctx, session, cmd)
if err != nil {
    return err
}
defer rs.Release()

result := <-rs.ResultC()
if result.Err != nil {
    return result.Err
}

// Advance the session's series ID after each successful proposal
session.ProposalCompleted()

// For idempotent operations, skip sessions entirely:
noopSession := host.GetNoOPSession(shardID)
host.SyncPropose(ctx, shardID, cmd) // No deduplication
```

### Three-node cluster with mTLS

```go
members := map[uint64]string{
    1: "node1:63100",
    2: "node2:63100",
    3: "node3:63100",
}

// Load mTLS certificates (required for multi-node clusters)
caCert, _ := os.ReadFile("/etc/quicraft/ca.pem")
cert, _ := os.ReadFile("/etc/quicraft/node1.pem")
key, _ := os.ReadFile("/etc/quicraft/node1-key.pem")

// Each node creates a Host with its own address and starts the shard
// with the same members map but its own ReplicaID.
hostCfg := config.HostConfig{
    WALDir:        "/var/data/quicraft/wal",
    NodeHostDir:   "/var/data/quicraft",
    RaftAddress:   "node1:63100",
    ListenAddress: "0.0.0.0:63100",
    DeploymentID:  1,
    TransportConfig: config.TransportConfig{
        MTLSConfig: &config.MTLSConfig{
            CACert: caCert,
            Cert:   cert,
            Key:    key,
        },
    },
}

shardCfg := config.Config{
    ShardID:   1,
    ReplicaID: 1, // 2 on node2, 3 on node3
}

host, _ := quicraft.NewHost(hostCfg)
host.StartShard(members, false, kv.NewMemoryCreateFunc(), shardCfg)
```

### Cluster discovery and bootstrap

go-quicraft provides only the primitive `Host.StartShard(members, join, createFn, cfg)`, which takes an explicit `map[uint64]string` member map. The caller supplies that map.

Peer discovery (static / multicast / DNS SRV / token), quorum validation, and bootstrap orchestration (discover → quorum → `StartShard`), plus opt-in dynamic membership, live one layer up in **go-qrdb** (`pkg/discovery` + `pkg/cluster`) — the layer that owns the cluster. To form a multi-node cluster automatically, use go-qrdb's `cluster.BootstrapShard` (or run the `qrdbd` daemon, which wraps it). See go-qrdb's `docs/discovery.md`.

### At-rest encryption

QuicRaft encrypts WAL entries and snapshots at rest using a barrier/seal/unseal pattern inspired by HashiCorp Vault. The barrier generates a random root key, derives per-epoch data encryption keys (DEKs) via HKDF, and supports automatic key rotation with epoch-based purging.

The barrier owns no storage of its own. `BarrierConfig.StatePersister` is how the sealed root key reaches durable storage, and a real deployment must set it: without one, `Rotate` and `PurgeEpochsBefore` fail closed with `ErrNoStatePersister`, the load-generation fence that catches a second process loading the same sealed state is inert, and the first `Unseal` after a restart leaves the barrier able to read but not write.

**AES-256-GCM** -- hardware-accelerated on CPUs with AES-NI (most modern x86_64):

```go
import (
    "context"
    "encoding/json"
    "log"
    "os"
    "path/filepath"

    quicraft "github.com/jeremyhahn/go-quicraft/pkg"
    "github.com/jeremyhahn/go-quicraft/pkg/config"
    "github.com/jeremyhahn/go-quicraft/pkg/seal"
)

const sealedPath = "/var/data/quicraft/sealed-root-key.json"

// persistSealedState is the BarrierConfig.StatePersister for this deployment.
// Returning nil has to mean the blob is durable: the barrier activates a new
// epoch on that nil, and data written under an epoch the persisted blob does
// not name is unreadable after a restart. It is called with the barrier's
// write lock held, so it must not call back into the barrier.
func persistSealedState(sealed *seal.SealedRootKey) error {
    blob, err := json.Marshal(sealed)
    if err != nil {
        return err
    }
    dir := filepath.Dir(sealedPath)
    f, err := os.CreateTemp(dir, ".sealed-root-key-*")
    if err != nil {
        return err
    }
    defer os.Remove(f.Name())
    if _, err := f.Write(blob); err != nil {
        f.Close()
        return err
    }
    if err := f.Chmod(0o600); err != nil {
        f.Close()
        return err
    }
    if err := f.Sync(); err != nil {
        f.Close()
        return err
    }
    if err := f.Close(); err != nil {
        return err
    }
    if err := os.Rename(f.Name(), sealedPath); err != nil {
        return err
    }
    d, err := os.Open(dir)
    if err != nil {
        return err
    }
    defer d.Close()
    return d.Sync()
}

func main() {
    // Create barrier with AES-256-GCM. NewBarrier returns an error for an
    // invalid config (unknown algorithm, non-FIPS cipher under FIPS mode);
    // seal.MustNewBarrier is the panicking form for compile-time constants.
    barrier, err := seal.NewBarrier(seal.BarrierConfig{
        Algorithm:      seal.AlgAES256GCM,
        DeploymentID:   1,
        NonceTracking:  seal.BoolPtr(true), // *bool; nil also means enabled
        StatePersister: persistSealedState,
    })
    if err != nil {
        log.Fatal(err)
    }

    // Software sealing strategy -- derives wrapping key from passphrase via Argon2id.
    // The passphrase is a []byte, not a string, so it can be wiped after use.
    passphrase := []byte("operator-passphrase")
    strategy, err := seal.NewSoftwareStrategy(passphrase)
    if err != nil {
        log.Fatal(err)
    }

    // Initialize generates the root key, derives epoch 1 DEK, and seals
    // the root key for persistent storage. Call once per cluster.
    if err := barrier.Initialize(context.Background(), strategy, seal.Credentials{
        Secret: passphrase,
    }); err != nil {
        log.Fatal(err)
    }

    // Initialize is the one path that does not call the persister, so the
    // bootstrap write is explicit. Every later write -- Unseal, Rotate,
    // PurgeEpochsBefore -- goes through StatePersister. Lose this blob and the
    // data directory can never be unsealed again.
    if err := persistSealedState(barrier.SealedRootKeyData()); err != nil {
        log.Fatal(err)
    }

    // Create host with barrier -- WAL and snapshots are now encrypted
    host, err := quicraft.NewHost(config.HostConfig{
        WALDir:        "/var/data/quicraft/wal",
        NodeHostDir:   "/var/data/quicraft",
        RaftAddress:   "localhost:63100",
        ListenAddress: "0.0.0.0:63100",
        DeploymentID:  1,
    }, quicraft.WithBarrier(barrier))
    if err != nil {
        log.Fatal(err)
    }
    defer host.Close()

    // ... start shards, propose, read as usual -- encryption is transparent
}
```

**ChaCha20-Poly1305** -- constant-time on all platforms, no hardware dependency. Preferred on ARM or when AES-NI is unavailable:

```go
barrier, err := seal.NewBarrier(seal.BarrierConfig{
    Algorithm:      seal.AlgChaCha20Poly1305,
    DeploymentID:   1,
    NonceTracking:  seal.BoolPtr(true),
    StatePersister: persistSealedState,
})
if err != nil {
    log.Fatal(err)
}
```

**Auto-select** -- let QuicRaft choose the algorithm from CPU feature detection. Pin the algorithm explicitly for any data directory that may outlive the host that wrote it: the choice is part of the key derivation, so a directory restored onto a machine without AES-NI otherwise reads back as `ErrDecryptionFailed`.

```go
barrier, err := seal.NewBarrier(seal.BarrierConfig{
    Algorithm:      seal.SelectOptimalAlgorithm(false), // AES-256-GCM if AES-NI, else ChaCha20
    DeploymentID:   1,
    NonceTracking:  seal.BoolPtr(true),
    StatePersister: persistSealedState,
})
if err != nil {
    log.Fatal(err)
}
```

`DefaultBarrierConfig` is the same thing with fewer keystrokes. It fills in the algorithm and nonce tracking but leaves `StatePersister` nil, so set that before constructing. `MustNewBarrier` panics rather than returning an error, which suits a compile-time config only.

```go
cfg := seal.DefaultBarrierConfig(1)
cfg.StatePersister = persistSealedState
barrier := seal.MustNewBarrier(cfg)
```

**Unseal on restart** -- load the persisted sealed root key and unseal the barrier before starting the host:

```go
// Load the sealed root key written at Initialize time.
blob, err := os.ReadFile(sealedPath)
if err != nil {
    log.Fatal(err)
}
sealed := new(seal.SealedRootKey)
if err := json.Unmarshal(blob, sealed); err != nil {
    log.Fatal(err)
}

// The same StatePersister as at Initialize time. Unseal writes through it on
// every load to claim the load generation, and again for the rotation it
// performs because the loaded epoch's spent AEAD invocation budget cannot be
// known across a restart. Without a persister the barrier comes up read-only:
// it will decrypt, and every Encrypt fails closed.
cfg := seal.DefaultBarrierConfig(1)
cfg.StatePersister = persistSealedState
barrier := seal.MustNewBarrier(cfg)
if err := barrier.SetSealedRootKey(sealed); err != nil {
    log.Fatal(err)
}

passphrase := []byte("operator-passphrase")
strategy, err := seal.NewSoftwareStrategy(passphrase)
if err != nil {
    log.Fatal(err)
}
if err := barrier.Unseal(context.Background(), strategy, seal.Credentials{
    Secret: passphrase,
}); err != nil {
    log.Fatal(err) // Wrong passphrase, corrupted key, or brute-force backoff
}

host, _ := quicraft.NewHost(hostCfg, quicraft.WithBarrier(barrier))
```

**Key rotation** -- rotate the DEK without re-encrypting existing data. Old epochs remain readable until purged:

```go
newEpoch, err := barrier.Rotate() // Derive new DEK for epoch N+1
if err != nil {
    log.Fatal(err)
}
purged, err := barrier.PurgeEpochsBefore(newEpoch) // Drop DEKs older than newEpoch
if err != nil {
    log.Fatal(err)
}
log.Printf("rotated to epoch %d, purged %d old DEKs", newEpoch, purged)
```

Purge only up to an epoch no live WAL segment or snapshot still needs — a purged DEK is not recoverable. See [Encryption](docs/security/encryption.md) for how the host computes that floor.

This layer ships exactly two sealing strategies: `SoftwareStrategy` (`StrategyPassphrase`, Argon2id / PBKDF2-FIPS) and `StrategyNone`. `SealingStrategy` is a pluggable interface, and the hardware and cloud backends that implement it (TPM2, PKCS#11, AWS KMS, GCP KMS, Azure Key Vault, HashiCorp Vault) live in **go-xkms**; Shamir secret sharing and share custody live in **go-qrdb**. The consensus engine never persists Shamir shares. Strategy selection is always explicit; there is no preference order and no auto-ranking.

**The filesystem is one option, not the design.** The examples above write a JSON file because that is the shortest thing that works, but neither half of root-key custody is fixed. Where the sealed blob lives is `BarrierConfig.StatePersister`, an arbitrary function, and `seal.StorageBackend` covers the case where a key-value store rather than a file holds it. What protects the root key is the `SealingStrategy`: a passphrase and Argon2id here, or a wrapping key held in a TPM, an HSM or a cloud KMS through go-xkms. A strategy may also return a `BarrierEncryptor`, which hands the data-path encryption to that backend instead of the barrier's own epoch DEKs. See [Key custody](docs/security/key-custody.md) for the combinations and what each one buys.

## Build

```bash
make build      # Compile all packages
make test       # Run unit tests with race detector
make lint       # Run golangci-lint
make coverage   # Generate coverage report and enforce the floor
make ci         # fmt, no-mutants, vet, lint, security scans, build, test
make ci-full    # ci plus the build-tagged suites: modules, errfs, fuzz, integration, netfault
make build-fips # Build with GOFIPS140=v1.0.0
```

`make ci` runs `go test` with no build tags, which leaves out the integration,
fault-injection and fuzz suites. `make ci-full` is the one that matches CI.
The security scans are gosec, govulncheck, `go mod verify` and trivy; nancy is
deliberately not among them, because it needs OSS Index credentials and fails
the same way whether it found something or could not reach the service. Run
`make nancy` separately where those credentials exist.

## Testing

QuicRaft has 6,500+ test functions and 37 fuzz targets across 27 packages.

### Test Coverage

The Makefile enforces one floor, `COVER_MIN = 91`, and applies it to every
coverage target rather than tuning a threshold per package. The module total
clears it. Several packages measured on their own do not:

| Package | Coverage |
|---------|----------|
| `pkg/internal/registry` | 78.4% |
| `pkg/internal/transport` | 88.8% |
| `pkg/internal/engine` | 89.1% |
| `pkg/logdb/waldb` | 89.2% |
| `pkg/logdb` | 90.0% |
| `pkg/batch` | 90.5% |
| `pkg` | 90.8% |
| **Module total** | **91.2%** |

Measured at 0.5.1-alpha with `go test -coverprofile ./...`, which is the scope
`make coverage` uses: no build tags, so the integration and fault-injection
suites are excluded and only the code they exercise indirectly shows up.

`pkg/internal/registry` is 37 statements, so a single uncovered line moves it
several points. The gaps that matter are the WAL, the QUIC transport and the
engine pipeline, and they are worth stating rather than rounding away: those
are the layers whose failure modes are hardest to recover from. Raising them is
tracked work, not a settled state.

```bash
# Unit tests (run on host)
make test                              # All packages
make test-raft                         # Core Raft protocol
make test-waldb                        # WAL persistence
make test-engine                       # Engine pipeline
make test-transport                    # QUIC transport
make test-<pkg>                        # Any package: proto, config, seal, rsm, etc.

# Integration tests (run in devcontainer)
make integration-test                  # All integration tests
make integration-test-e2e              # Multi-node E2E cluster tests
make integration-test-linearizability  # Porcupine linearizability verification

# Fuzz testing
make fuzz-all                          # All fuzz targets (proto, transport, WAL, seal, session, raft)
make errfs-waldb                       # WAL I/O error injection tests

# Benchmarks
make bench-raft                        # Raft protocol benchmarks
make bench-waldb                       # WAL benchmarks
make bench-quicraft                    # Full-stack benchmarks
make bench-<pkg>                       # Any package

# Coverage
make coverage                          # Total coverage report
make coverage-<pkg>                    # Per-package coverage

# Performance comparison (QuicRaft vs etcd vs Dragonboat vs OpenRaft)
make perf-compare-all                          # Run all systems, print a report
make perf-all PERF_PROFILE=omega               # Run + store + generate, for one machine profile
make perf-remote PERF_HOST=user@host PERF_PROFILE=omega  # Same, on another machine
```

## Documentation

Full documentation lives in [docs/](docs/README.md), organized by component:

**Core**
- [Architecture Overview](docs/architecture/overview.md) -- Package structure, data flow, configuration
- [Raft Protocol](docs/raft/protocol.md) -- Elections, replication, snapshots, membership changes
- [Engine Pipeline](docs/engine/pipeline.md) -- Step/commit/apply workers, snapshot pool

**Infrastructure**
- [QUIC Transport](docs/transport/quic.md) -- Connection management, frame format, mTLS
- [Sharded WAL](docs/storage/wal.md) -- Segment format, durability, compaction, recovery
- [Wire Format](docs/protocol/wire-format.md) -- Binary encoding for Entry, Message, Snapshot

**State Machines**
- [SM Interfaces](docs/statemachine/interfaces.md) -- StateMachine, ConcurrentStateMachine, DiskStateMachine
- [RSM Adapter](docs/statemachine/rsm-adapter.md) -- Type detection, apply pipeline, snapshot format
- [KV Backends](docs/statemachine/kv-backends.md) -- MemoryStore, ConcurrentStore, command protocol

**Security & Sessions**
- [At-rest Encryption](docs/security/encryption.md) -- Barrier/seal/unseal, key rotation, FIPS 140
- [Key Custody](docs/security/key-custody.md) -- Where the sealed root key lives and what protects it
- [Session Deduplication](docs/sessions/deduplication.md) -- At-most-once delivery, client sessions

**Operations**
- [Public API](docs/api/reference.md) -- Host methods, RequestState, Session API, error types
- [Deployment](docs/operations/deployment.md) -- Configuration, monitoring, tuning
- [Lifecycle](docs/operations/lifecycle.md) -- Startup, shutdown, crash recovery
- [Discovery & Bootstrap](docs/discovery/bootstrap.md) -- moved to go-qrdb; quicraft exposes Raft primitives only

**Quality**
- [Performance](docs/performance/architecture.md) -- Zero-alloc paths, compression, rate limiting
- [Benchmarks](test/performance/README.md) -- QuicRaft vs etcd vs Dragonboat vs OpenRaft comparison
- [Testing](docs/testing/overview.md) -- Unit/integration/E2E tests, coverage targets
- [Test Infrastructure](docs/testing/infrastructure.md) -- Fuzzing, linearizability, chaos testing

## Architecture

```
+------------------------------------------------------------------+
|                             Host                                 |
|                                                                  |
|   +------------+    +------------+    +------------+             |
|   |  Shard 1   |    |  Shard 2   |    |  Shard N   |    ...      |
|   +------+-----+    +------+-----+    +------+-----+             |
|          |                 |                 |                   |
|          +-----------------+-----------------+                   |
|                            |                                     |
|                     +------+------+                              |
|                     |   Engine    |                              |
|                     | Step Commit |                              |
|                     |    Apply    |                              |
|                     +------+------+                              |
|                            |                                     |
|          +-----------------+-----------------+                   |
|          |                 |                 |                   |
|   +------+-----+   +------+------+   +------+------+             |
|   |    WAL     |   |  Transport  |   |  Snapshot   |             |
|   | (16-shard) |   |   (QUIC)    |   |    Pool     |             |
|   +------------+   +-------------+   +-------------+             |
+------------------------------------------------------------------+
```

The Host manages multiple Raft shards. Each shard runs through the engine pipeline: the step worker drives Raft ticks and message processing, the commit worker persists entries to the sharded WAL, and the apply worker feeds committed entries to the state machine. Snapshots are produced asynchronously by a bounded pool. The QUIC transport handles all inter-node communication with mTLS encryption.

## License

[Apache License 2.0](LICENSE)
