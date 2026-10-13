# QuicRaft documentation

> **Status: REFERENCE** · Scope: the index of this repository's documentation ·
> Last verified: 2026-10-11

Technical documentation for the QuicRaft multi-group Raft consensus library.

[SPEC.md](SPEC.md) lists every feature. Each implemented feature has a folder
whose documents are drawn from `architecture.md` (design, boundaries, rejected
alternatives), `engineering.md` (the implementation contract), `cli.md`,
`api.md` and `user.md`. A folder holds only files with content: a document with
nothing to say is not written. Together they are the feature's documentation
and its requirements.

go-quicraft is a library and ships no binaries. Where a feature's `cli.md`
exists, it explains why the feature has no operator verb and where the
consuming surface is.

## Start here

| If you are | Read |
|---|---|
| looking for what the project is and every feature | [SPEC.md](SPEC.md) |
| new to the library | [multi-group-host/architecture.md](multi-group-host/architecture.md), which is also the system overview |
| writing a state machine | [state-machines/user.md](state-machines/user.md) |
| deploying it | [multi-group-host/user.md](multi-group-host/user.md) |
| looking for a function signature | [multi-group-host/api.md](multi-group-host/api.md) |
| upgrading | [compatibility/user.md](compatibility/user.md) |

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
| [compatibility](compatibility/architecture.md) | the pinned format-version policy, every version discriminator, the interface requirements for embedders, and upgrade notes for each release |

## Cross-feature documents

These describe the repository rather than one feature, and deliberately sit
outside the feature folders.

| Document | Description |
|---|---|
| [Testing overview](testing/overview.md) | how the suites are organised and run, coverage gates, the Makefile target reference |
| [Testing infrastructure](testing/infrastructure.md) | build tags, fault injectors, fuzzing, linearizability verification, test helper packages, and the gates |
| [Benchmark regression testing](testing/benchmarking.md) | `make bench-compare`, interleaved measurement, stored baselines |
| [Public releasability](release/public-releasability.md) | what may be published from this tree, the development-tooling exclusion list, and the `test/releasability` gate that enforces both |

Per-feature test strategy lives in each feature's `engineering.md`; the three
testing documents describe the harnesses that serve all of them.

## References

- [Raft Paper (PDF)](references/raft.pdf)
- [Ongaro PhD Thesis (PDF)](references/OngaroPhD.pdf)

## Migration state

Complete: every implemented feature is listed in [SPEC.md](SPEC.md) and has a
folder holding only files with content, and a new feature gets one from day one.
