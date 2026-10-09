# go-quicraft roadmap

> **Status: REFERENCE** · Owner: Software Department · Last verified: 2026-10-09
> Per handbook `software/conventions/roadmap.md`: the Software Department refreshes it; the project agent proposes changes by PR.

## Mission

Multi-group Raft consensus over QUIC. Raft primitives only, plus the `Barrier`/`SealingStrategy` interfaces, `EpochBarrier`, the Shamir GF(256) primitive and the `none` and `passphrase` strategies (handbook `docs/00` L1).

## Current milestone

`Public release` milestone (M11): 0 open, 3 closed. **Release candidate cut**: v0.16.0 (2026-10-02), v0.20.0 (2026-10-08). Production v1.0.0 follows the joint P1 staging run.

## Now

- go-qrdb#620 — go-qrdb builds on v0.20.0 — in review
- go-xkms#1399 — go-xkms builds against go-quicraft ≥ v0.15 — open
- go-quicbaas#1149 — go-quicbaas drops the removed `pkg/enclave` import — open

## Next (v1.0.0 checklist)

- readiness: go-enclave at a public release — blocked: no candidate yet
- readiness: compatibility policy acknowledged (`release-versioning.md` §1, §8) — open
- readiness: publication mechanism for agent files, deferred to v1.0.0 by the owner — open
- readiness: company CI template neutralised for the public tree — open

## Later

Nothing accepted. New work arrives as issues.

## Dependencies

- Up: go-enclave v0.8.0-alpha (pinned = latest).
- Down: go-qrdb (pins v0.18.0), go-xkms (v0.15.0-alpha), go-quicbaas (v0.15.0-alpha), go-signal (v0.6.2-alpha).

## Non-goals

- Peer discovery, cluster formation, Shamir custody and the ceremony: go-qrdb.
- Hardware and cloud sealing strategies: go-xkms.
