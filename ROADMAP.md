# go-quicraft roadmap

> **Status: REFERENCE** · Owner: Software Department · Last verified: 2026-10-15
> Per handbook `software/conventions/roadmap.md`: the Software Department refreshes it; the project agent proposes changes by PR.

Every line names the [SPEC](docs/SPEC.md) feature it belongs to. The features
across Complete, Now, Next, Later and Future are exactly the SPEC feature table.

## Mission

The Raft library of the one-product stack: multi-group Raft consensus over QUIC. Raft primitives only, plus the `Barrier`/`SealingStrategy` interfaces, `EpochBarrier`, the Shamir GF(256) primitive and the `none` and `passphrase` strategies (handbook `docs/00` L1). It embeds go-enclave as a library and passes its types through unchanged. go-qrdb embeds it as the one consensus engine of a deployment and re-exports it. go-quicraft holds no datastore, tenant model or client of its own ([SPEC](docs/SPEC.md) Purpose, Boundaries).


## Milestones

- [`Dev complete`](../../milestone/11): release-candidate milestone; no due date set; 3 open, 62 closed.

## Complete

- **multi-group-host** — multi-group Raft with per-shard configuration — [0.1.0-alpha](CHANGELOG.md), [#259](../../issues/259) ([#260](../../pulls/260)) — done
- **raft-consensus** — election, replication, PreVote, CheckQuorum, leadership transfer; step/commit/apply workers — [0.1.0-alpha](CHANGELOG.md) — done
- **lease-read** — ReadIndex and LeaseRead — [0.1.0-alpha](CHANGELOG.md), [#299](../../issues/299) — done
- **membership-changes** — membership changes; identity-bound members in 0.3.0-alpha — [0.1.0-alpha](CHANGELOG.md), [0.3.0-alpha](../../releases/tag/v0.3.0-alpha) — done
- **quic-transport** — QUIC-only transport with mTLS — [0.1.0-alpha](CHANGELOG.md), [#252](../../issues/252) ([#253](../../pulls/253)) — done
- **0-rtt-policy** — 0-RTT default off; early-data frames and snapshots refused on receive — [0.13.1-alpha](../../releases/tag/v0.13.1-alpha) — done
- **fips-builds** — `-tags fips` at rest (0.7.0-alpha); QUIC transport inside the approved-only gate (0.11.0-alpha) — [0.7.0-alpha](../../releases/tag/v0.7.0-alpha), [0.11.0-alpha](../../releases/tag/v0.11.0-alpha) — done
- **wire-protocol** — fixed-layout binary wire format — [0.1.0-alpha](CHANGELOG.md) — done
- **log-storage** — 16-shard WAL, compaction, crash recovery — [0.1.0-alpha](CHANGELOG.md), [#268](../../issues/268) ([#271](../../pulls/271)) — done
- **snapshots** — snapshots — [0.1.0-alpha](CHANGELOG.md) — done
- **state-machines** — three state machine interfaces, RSM adapter, in-memory KV; Working fundamentally C5 (#280 fixed) — [0.1.0-alpha](CHANGELOG.md), [#280](../../issues/280) ([#295](../../pulls/295)) — done
- **client-sessions** — client sessions for at-most-once delivery — [0.1.0-alpha](CHANGELOG.md) — done
- **write-batching** — `pkg/batch`, `pkg/writemode` — [#268](../../issues/268) ([#271](../../pulls/271)) — done
- **execute-on-leader** — `Host.ExecuteOnLeader` — [0.21.0-rc.1](../../releases/tag/v0.21.0-rc.1), [#257](../../issues/257) ([#261](../../pulls/261)) — done
- **seal-barrier** — at-rest encryption, epoch rotation — [0.1.0-alpha](CHANGELOG.md), [#284](../../issues/284) ([#297](../../pulls/297)) — done
- **seal-barrier** — Working fundamentally C4: sealed proposals return `ErrBarrierSealed` — [#296](../../issues/296), [0.21.0-rc.2](../../releases/tag/v0.21.0-rc.2) — done
- **shamir-primitive** — Shamir `Combine`; share values in protected memory in 0.7.0-alpha — [0.4.0-alpha](../../releases/tag/v0.4.0-alpha), [0.7.0-alpha](../../releases/tag/v0.7.0-alpha) — done
- **memory-enclave** — locked memory provided by go-enclave; `pkg/enclave` deleted — [0.11.0-alpha](../../releases/tag/v0.11.0-alpha) — done
- **observability** — Prometheus metrics — [0.1.0-alpha](CHANGELOG.md), [#277](../../issues/277) ([#295](../../pulls/295), [#301](../../pulls/301)), [#279](../../issues/279) ([#295](../../pulls/295)) — done
- **compatibility** — re-verified — [#274](../../issues/274) ([#287](../../pulls/287)) — done
- **contrib-dragonboat** — `contrib/dragonboat` builds standalone — [0.2.0-alpha](CHANGELOG.md) — done
- **testing** — Porcupine linearizability suite; Working fundamentally C3 (#275 fixed) — [0.1.0-alpha](CHANGELOG.md), [#275](../../issues/275) ([#295](../../pulls/295)), [#251](../../issues/251) ([#254](../../pulls/254)) — done
- **testing** — Working fundamentally C1: the PR tier is green on main — [milestone](../../milestone/26) — done
- **testing** — Working fundamentally C2: the full tier is green on the same SHA — [milestone](../../milestone/26) — done
- **testing** — Working fundamentally C6: the nightly dependency-freshness check (`make deps-check`) is green — [milestone](../../milestone/26) — done
- **release** — public-releasability gate — [0.16.0-alpha](../../releases/tag/v0.16.0-alpha), [#262](../../issues/262), [#269](../../issues/269) ([#270](../../pulls/270)), [#250](../../issues/250) ([#256](../../pulls/256)), [#282](../../issues/282) ([#287](../../pulls/287)) — done
- **release** — Working fundamentally C7: v0.21.0-rc.2 is cut and go-qrdb adopts it — [0.21.0-rc.2](../../releases/tag/v0.21.0-rc.2) — done
- **testing** — `TestLinearizability_3Node_LeaderFailover` and its soak pass reliably ([#320](../../pulls/320), [#322](../../pulls/322), [#347](../../pulls/347)) — [#309](../../issues/309) — done
- **release** — spec-review scrub of every `docs/<feature>/` for the release candidate — [#283](../../issues/283) — done
- **read-index-readiness** — a ReadIndex read a new leader cannot serve yet returns `ErrReadIndexNotReady` instead of waiting, on the leader and on a follower that forwarded it — [go-qrdb#668](../../../go-qrdb/issues/668) — done
- **shard-pause** — `Host.PauseShard`/`ResumeShard`: a paused shard accepts no proposals, reads or applies, keeps its state and resumes after unseal, so go-qrdb can seal a node at runtime without the shard failing — [0.22.0-alpha](../../releases/tag/v0.22.0-alpha) — done
- **heartbeat-under-fsync** — a follower answers a leader's Heartbeat during an in-flight WAL fsync when the step left its hard state and log unchanged, so CheckQuorum does not depose a leader through a bounded fsync stall; a HeartbeatResp never acknowledges a non-durable index — [Unreleased](CHANGELOG.md), [#390](../../issues/390) — done

## Now

No feature in progress. Open in the milestone: [#363](../../issues/363) (multi-group-host restart after a clean stop), [#369](../../issues/369) (docs clean-up), [#370](../../issues/370) (CI in the devcontainer image).

## Next

Nothing queued. The v1.0.0 bar is in [SPEC](docs/SPEC.md) Release criteria.

## Later

No feature is later.

## Future

No SPEC feature is future.

## Dependencies

- Up: go-enclave v0.9.0-alpha (pinned, the newest tag), which carries the enclave-interface API (go-enclave#138, #201); v1.0.0 needs a go-enclave public release.
- Down: go-qrdb (pins v0.20.0, direct); go-qrdb#695 adopts the newer go-quicraft tags (v0.22.0-alpha is tagged). Indirect: go-xkms (v0.20.0), go-signal (v0.20.0), go-quicbaas (v0.15.0-alpha), go-trusted-platform (v0.13.1-alpha), go-recountly (v0.6.2-alpha).
- Standards watch: draft-irtf-cfrg-xchacha pinned at -03 (`AlgXChaCha20Poly1305`) — [pkg/seal](pkg/seal); copy in [docs/references/](docs/references/README.md)
