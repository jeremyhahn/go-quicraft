# QuicRaft Documentation

Technical documentation for the QuicRaft multi-group Raft consensus library.

## Getting Started

New to QuicRaft? Start here:

1. [Architecture Overview](architecture/overview.md) — how the pieces fit together
2. [Public API Reference](api/reference.md) — Host methods, configuration, error types
3. [Deployment Guide](operations/deployment.md) — configuration, monitoring, tuning

## Core

| Document | Description |
|----------|-------------|
| [Architecture Overview](architecture/overview.md) | Package structure, data flow, proposal and read paths |
| [Raft Protocol](raft/protocol.md) | Elections, replication, snapshots, membership changes, spec compliance |
| [Engine Pipeline](engine/pipeline.md) | Step, commit, and apply workers, concurrency model, snapshot pool |

## Transport & Wire Format

| Document | Description |
|----------|-------------|
| [QUIC Transport](transport/quic.md) | Connection pooling, stream management, frame format, mTLS |
| [Binary Wire Format](protocol/wire-format.md) | Entry, Message, MessageBatch, and Snapshot encoding, size limits |

## Storage

| Document | Description |
|----------|-------------|
| [Sharded WAL](storage/wal.md) | Segment format, write and read paths, durability, compaction, crash recovery |

## State Machines

| Document | Description |
|----------|-------------|
| [SM Interfaces](statemachine/interfaces.md) | StateMachine, ConcurrentStateMachine, DiskStateMachine contracts |
| [RSM Adapter](statemachine/rsm-adapter.md) | Type detection, apply pipeline, session routing, snapshot format |
| [KV Backends](statemachine/kv-backends.md) | Reference MemoryStore and ConcurrentStore implementations |

## Security

| Document | Description |
|----------|-------------|
| [At-rest Encryption](security/encryption.md) | Barrier seal/unseal, algorithm-agile AEAD encryption, epoch-based key rotation, FIPS 140 |

## Sessions & Discovery

| Document | Description |
|----------|-------------|
| [Session Deduplication](sessions/deduplication.md) | At-most-once delivery, client sessions, snapshot integration |
| [Bootstrap & Discovery](discovery/bootstrap.md) | Static, multicast, DNS SRV discovery, cluster formation |

## Operations

| Document | Description |
|----------|-------------|
| [Public API Reference](api/reference.md) | Host methods, RequestState, Session API, error types |
| [Deployment Guide](operations/deployment.md) | Configuration, monitoring, tuning, operational practices |
| [Lifecycle Management](operations/lifecycle.md) | Startup, shutdown, crash recovery, WAL replay |

## Performance & Testing

| Document | Description |
|----------|-------------|
| [Performance Architecture](performance/architecture.md) | Zero-alloc paths, compression, rate limiting, memory budgets |
| [Testing Overview](testing/overview.md) | Unit, integration, E2E tests, coverage targets, Makefile targets |
| [Test Infrastructure](testing/infrastructure.md) | Fuzzing, linearizability verification, I/O error injection |

## References

- [Raft Paper (PDF)](references/raft.pdf)
- [Ongaro PhD Thesis (PDF)](references/OngaroPhD.pdf)
