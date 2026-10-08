<img src="assets/logo.svg" width="600">

[![Go](https://img.shields.io/badge/Go-1.27.1-blue.svg)](https://go.dev/)
[![License](https://img.shields.io/badge/License-Apache%202.0-green.svg)](LICENSE)
[![Version](https://img.shields.io/badge/Version-0.16.0-orange.svg)](VERSION)

QuicRaft is a multi-group Raft consensus library for Go. One process hosts
many independent Raft groups (shards), replicas talk to each other over QUIC
with mandatory mutual TLS, and the log and snapshots can be encrypted at rest.

It started as a rewrite of [Dragonboat](https://github.com/lni/dragonboat), and
the host-and-shards model will look familiar if you know it. The main
differences:

- **QUIC instead of TCP.** All inter-node traffic, including snapshot
  streaming, runs over [quic-go](https://github.com/quic-go/quic-go), with
  multiplexed streams and TLS 1.3 built into the protocol. There is no TCP
  fallback. 0-RTT is not used: the dialer never sends early data, and frames
  that arrive as early data are refused because they are replayable.
- **Identity-bound membership.** Each member is authorized by its node
  identity, a hash of its certificate's public key, not by its address.
- **At-rest encryption.** WAL entries and snapshots are sealed by a barrier
  with per-epoch keys and key rotation. Running without it is an explicit
  choice (`WithoutEncryption`), never a default.
- **Its own storage and wire format.** A 16-shard write-ahead log with no
  embedded database, and a fixed-layout binary wire format (no protobuf).

The protocol follows the [extended Raft paper](docs/references/raft.pdf) and
[Diego Ongaro's PhD thesis](docs/references/OngaroPhD.pdf): leader election
with PreVote and CheckQuorum, log replication, membership changes, snapshots,
ReadIndex and lease-based reads, leadership transfer, observers, witnesses, and
client sessions for at-most-once proposals.

## Status

The current version is **0.16.0**, a release candidate. QuicRaft has not had a
1.0 release, so:

- The public API may still change between minor versions.
- On-disk and wire formats may change in place before 1.0. Every format
  version is pinned at its current value until the first stable release, so a layout change does
  not show up as a version bump. There is no migration path between
  pre-release layouts: data directories are wiped and re-synced, not
  migrated.
- Build every node of a cluster from the same release.

See [CHANGELOG.md](CHANGELOG.md) and [docs/compatibility.md](docs/compatibility.md)
before upgrading.

## Install

```sh
go get github.com/jeremyhahn/go-quicraft
```

Requires Go 1.27.1 or later. The public API is under
`github.com/jeremyhahn/go-quicraft/pkg` (imported as `quicraft` below).

QuicRaft depends on [go-enclave](https://github.com/jeremyhahn/go-enclave) for
locked memory. Pre-release versions of both modules are served from the
project's Git forge, not from the Go module proxy. To build against the forge,
keep the `github.com/jeremyhahn/...` import paths and redirect where Go fetches
them:

```sh
go env -w GOPRIVATE='github.com/jeremyhahn/*'
git config --global url."ssh://git@forge.example:2222/jeremyhahn/".insteadOf "https://github.com/jeremyhahn/"
```

`GOPRIVATE` makes Go fetch these modules directly and skip the public checksum
database; the rewrite sends that fetch to the forge. Don't set `GOFLAGS`,
`GONOSUMDB` or `GOINSECURE`. No `go.mod` ever names the forge, so the same
commit builds from either source.

## Quick start

A single-node shard running the bundled in-memory key-value state machine:

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
	leaderCh := make(chan struct{}, 1)

	hostCfg := config.HostConfig{
		WALDir:        "/var/lib/quicraft/wal",
		NodeHostDir:   "/var/lib/quicraft",
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

	// A single node needs no transport. WithoutEncryption declares that the
	// log and snapshots are stored in plaintext; NewHost refuses a host that
	// neither configures a barrier nor says this.
	host, err := quicraft.NewHost(hostCfg, quicraft.WithoutTransport(), quicraft.WithoutEncryption())
	if err != nil {
		log.Fatal(err)
	}
	defer host.Close()

	members := map[uint64]string{1: "localhost:63100"}
	shardCfg := config.Config{ShardID: 1, ReplicaID: 1}
	if err := host.StartShard(members, false, kv.NewMemoryCreateFunc(), shardCfg); err != nil {
		log.Fatal(err)
	}
	<-leaderCh

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	cmd, err := kv.EncodePut([]byte("hello"), []byte("world"))
	if err != nil {
		log.Fatal(err)
	}
	if _, err := host.SyncPropose(ctx, 1, cmd); err != nil {
		log.Fatal(err)
	}

	val, err := host.SyncRead(ctx, 1, []byte("hello")) // linearizable read
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("hello = %s\n", val.([]byte))
}
```

The first election takes a few seconds because `HostConfig.RTTMillisecond`
defaults to 200 ms, a WAN-sized value. Set it to 1–5 on a LAN.

### Going multi-node

A cluster needs the QUIC transport, so `TransportConfig.MTLSConfig` is
required, and every member's node identity must be known when the shard
starts. Each host reports its own identity from `host.NodeIdentity()`. Collect
them out of band and pass the same map to every founding member:

```go
hostCfg.TransportConfig = config.TransportConfig{
	MTLSConfig: &config.MTLSConfig{CACert: caPEM, Cert: certPEM, Key: keyPEM},
}

shardCfg := config.Config{
	ShardID:   1,
	ReplicaID: 1, // 2 on node2, 3 on node3
	MemberIdentities: map[uint64]string{
		1: "spki:…", // node1's host.NodeIdentity()
		2: "spki:…",
		3: "spki:…",
	},
}
members := map[uint64]string{1: "node1:63100", 2: "node2:63100", 3: "node3:63100"}
```

A host with a transport refuses to start a shard that lacks an identity for any
member. Members added later go through `RequestAddNodeWithIdentity`. Issue
one keypair per host. The full walkthrough, including directory layout and
configuration reference, is in
[docs/multi-group-host/user.md](docs/multi-group-host/user.md).

QuicRaft starts shards from an explicit member map and does no peer discovery
of its own. Discovery and bootstrap orchestration belong to the application
that embeds it; in this project's own stack that is
[go-qrdb](https://github.com/jeremyhahn/go-qrdb).

## The API at a glance

| Task | Methods | Docs |
|---|---|---|
| Write, wait for apply | `SyncPropose` | [multi-group-host](docs/multi-group-host/user.md) |
| Write, asynchronous | `Propose`, then `RequestState.ResultC()`; with `HostConfig.NotifyCommit`, also `CommittedC()` / `ApplyResultC()` | [write-batching](docs/write-batching/user.md) |
| Batch many writes into one entry | `batch.NewAggregator` (at-least-once; no deduplication) | [write-batching](docs/write-batching/user.md) |
| At-most-once writes | `GetNewSession`, `SyncProposeWithSession`, `CloseSession` | [client-sessions](docs/client-sessions/user.md) |
| Linearizable read | `SyncRead` (ReadIndex; lease-based when `Config.LeaseRead` is set) | [multi-group-host](docs/multi-group-host/user.md) |
| Local reads | `StaleRead`, `QueryLocalNode`; `SyncReadBuf` / `StaleReadBuf` for pooled buffers | [multi-group-host](docs/multi-group-host/user.md) |
| Membership | `RequestAddNodeWithIdentity`, `RequestAddObserverWithIdentity`, `RequestAddWitnessWithIdentity`, `RequestRemoveNode` | [membership-changes](docs/membership-changes/user.md) |
| Snapshots | `RequestSnapshot`, `ExportSnapshot`, `ImportSnapshot` | [snapshots](docs/snapshots/user.md) |
| Your state machine | `sm.StateMachine`, `sm.ConcurrentStateMachine`, `sm.DiskStateMachine` | [state-machines](docs/state-machines/user.md) |
| Encryption at rest | `seal.NewBarrier`, `quicraft.WithBarrier` | [seal-barrier](docs/seal-barrier/user.md) |

## Encryption at rest

Pass `quicraft.WithBarrier(barrier)` to `NewHost` and the WAL and snapshots are
encrypted with AES-256-GCM or ChaCha20-Poly1305. The barrier holds a random
root key, derives a data key per epoch, and supports rotation and purging of
old epochs. The root key is sealed by a `SealingStrategy`. This module ships
the passphrase strategy (`seal.NewSoftwareStrategy`, Argon2id, or PBKDF2 under
FIPS) and `StrategyNone`. Hardware and cloud KMS strategies plug into the same
interface from outside this module.

Two settings matter in every real deployment:

- Set `BarrierConfig.StatePersister`. The barrier stores nothing itself.
  Without a persister it cannot rotate (`ErrNoStatePersister`), and after a
  restart it can decrypt but refuses to encrypt.
- Pin `BarrierConfig.Algorithm` for any data that may move between machines.
  The automatic choice depends on the local CPU, and the algorithm is part of
  the key derivation.

[docs/seal-barrier/user.md](docs/seal-barrier/user.md) has complete
initialize, unseal, rotate and purge examples. FIPS 140-3 builds are covered in
[docs/quic-transport/user.md](docs/quic-transport/user.md#task-choose-a-fips-mode).

## Documentation

[docs/README.md](docs/README.md) is the index. Each feature has its own folder
with an architecture document, an engineering contract, an API reference and a
usage guide. Good places to start:

- [docs/multi-group-host/architecture.md](docs/multi-group-host/architecture.md):
  the system overview and the `Host`
- [docs/state-machines/user.md](docs/state-machines/user.md): writing a state
  machine
- [docs/multi-group-host/user.md](docs/multi-group-host/user.md): deploying and
  configuring
- [docs/compatibility.md](docs/compatibility.md): format pinning and upgrades

## Performance

[test/performance/](test/performance/README.md) compares QuicRaft with
etcd-io/raft, Dragonboat and OpenRaft on single-node, 3-node and 5-node
configurations. Each result set is tied to the machine that measured it. The
most recent full run is the
[`ci-node-2` profile](test/performance/profiles/ci-node-2/), a dual-socket <!-- lab-string-ok: benchmark provenance: the profile directory names the machine that measured the numbers -->
Xeon Gold 6258R server, measured 2026-10-04 with every engine on one common
configuration (same timing, keys, warmup, durability and TLS; Dragonboat on its
Tan LogDB). QuicRaft led Dragonboat in 12 of 18 head-to-head scenarios,
etcd-io/raft in 10 of 16 and OpenRaft in 10 of 14. Dragonboat was ahead on
concurrent writes and multi-node linearizable reads, etcd-io/raft on elections
(all four within 4%, effectively even) and single-node in-memory writes, and OpenRaft on single-node
in-memory writes and 5-node sequential proposals.
Treat these as one machine's numbers, and read the
[fairness rules](test/performance/README.md#fairness),
which list where an engine could not match the common configuration, before
drawing a conclusion from any row.

## Development

See [CONTRIBUTING.md](CONTRIBUTING.md). The short version:

```sh
make test      # unit tests, race detector on
make ci        # format, vet, lint, security scans, build, unit tests
make ci-full   # ci plus the integration, fault-injection, fuzz and FIPS suites
```

Report security issues privately as described in [SECURITY.md](SECURITY.md).

## License

[Apache License 2.0](LICENSE)
