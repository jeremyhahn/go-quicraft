# QuicRaft Documentation

> **Status: REFERENCE** · Scope: the index of this repository's documentation ·
> Last verified: 2026-09-15

Technical documentation for the QuicRaft multi-group Raft consensus library.

Documentation follows the **feature-folder convention**: every implemented
feature has a folder holding five documents — `architecture.md` (design,
boundaries, rejected alternatives), `engineering.md` (the implementation
contract), `cli.md`, `api.md`, and `user.md`. The five files together are the
feature's documentation and its requirements.

**go-quicraft ships no binaries**, so every `cli.md` records that explicitly and
points at the consuming surface rather than being absent — an absent file cannot
be told apart from an unwritten one.

## Start here

| If you are… | Read |
|---|---|
| new to the library | [multi-group-host/architecture.md](multi-group-host/architecture.md) — also the system overview |
| writing a state machine | [state-machines/user.md](state-machines/user.md) |
| deploying it | [multi-group-host/user.md](multi-group-host/user.md) |
| looking for a function signature | [multi-group-host/api.md](multi-group-host/api.md) |
| upgrading | [compatibility.md](compatibility.md) |

## Features

| Feature | What it covers |
|---|---|
| [multi-group-host](multi-group-host/architecture.md) | the `Host`: lifecycle, shard management, proposal and read paths, configuration, the performance contract. The system overview lives here. |
| [raft-consensus](raft-consensus/architecture.md) | the Raft protocol and the step/commit/apply worker pipeline |
| [membership-changes](membership-changes/architecture.md) | adding and removing replicas, observers and witnesses, node identity |
| [quic-transport](quic-transport/architecture.md) | QUIC, mTLS, connection and stream pools, revocation, the node registry |
| [wire-protocol](wire-protocol/architecture.md) | the binary wire format and the two version namespaces |
| [log-storage](log-storage/architecture.md) | the 16-shard WAL, the `LogDB` interface, recovery, compaction, repair |
| [snapshots](snapshots/architecture.md) | creation, streaming, import/export, re-encryption |
| [state-machines](state-machines/architecture.md) | the three interfaces you implement, the RSM adapter, the reference KV backends |
| [client-sessions](client-sessions/architecture.md) | at-most-once proposal delivery |
| [write-batching](write-batching/architecture.md) | the proposal aggregator and the write-durability modes |
| [seal-barrier](seal-barrier/architecture.md) | at-rest encryption: epoch DEKs, sealing strategies, the AEAD safety bounds |
| [shamir-primitive](shamir-primitive/architecture.md) | the GF(2^8) split/combine primitive and the share-custody interfaces |
| [memory-enclave](memory-enclave/architecture.md) | how this library holds key material: go-enclave locked memory and sealed enclaves |
| [observability](observability/architecture.md) | metrics, logging and event notifications |

## Cross-feature documents

These describe the repository rather than one feature, and deliberately sit
outside the feature folders.

| Document | Description |
|---|---|
| [Compatibility and Upgrades](compatibility.md) | the pinned-at-1 format policy, every version discriminator that exists, the per-release upgrade path, and the interface requirements for embedders. It spans `pkg/proto`, the WAL, `snapshot.meta` and the sealed root key, so splitting it across feature folders would destroy the upgrade narrative. |
| [Testing Overview](testing/overview.md) | unit, integration and E2E structure, coverage targets, Makefile targets |
| [Test Infrastructure](testing/infrastructure.md) | fuzzing, linearizability verification, I/O error injection |
| [Benchmark Regression Testing](testing/benchmarking.md) | `make bench-compare`, interleaved measurement, stored baselines |

Per-feature test strategy lives in each feature's `engineering.md`; these three
describe the harnesses that serve all of them.

## References

- [Raft Paper (PDF)](references/raft.pdf)
- [Ongaro PhD Thesis (PDF)](references/OngaroPhD.pdf)

## Migration state

**Complete.** Every implemented feature has a folder with all five files, and
the pre-convention layout (`docs/architecture/`, `docs/raft/`, `docs/engine/`,
`docs/transport/`, `docs/protocol/`, `docs/storage/`, `docs/statemachine/`,
`docs/sessions/`, `docs/security/`, `docs/api/`, `docs/operations/`,
`docs/discovery/`, `docs/performance/`) has been moved into it — not rewritten.
`docs/testing/` and `docs/references/` remain as cross-feature material.

A new feature gets a folder from day one. The portfolio view is derived rather
than maintained: count `docs/*/architecture.md`.
