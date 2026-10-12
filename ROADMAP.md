# go-quicraft roadmap

> **Status: REFERENCE** · Owner: Software Department · Last verified: 2026-10-11
> Per handbook `software/conventions/roadmap.md`: the Software Department refreshes it; the project agent proposes changes by PR.

## Mission

Multi-group Raft consensus over QUIC. Raft primitives only, plus the `Barrier`/`SealingStrategy` interfaces, `EpochBarrier`, the Shamir GF(256) primitive and the `none` and `passphrase` strategies (handbook `docs/00` L1).

## Current milestone

- [`Release candidate`](../../milestone/11) — target: release candidate; projected date: not yet set. Candidate cut: **v0.21.0-rc.1** (2026-10-11), the first `-rc.N` tag.
- No other open milestone.

v0.21.0-rc.1 contents, all done:

- [#257](../../issues/257) — `Host.ExecuteOnLeader`: leader-executed requests with no log entry — `docs/multi-group-host/` — done
- [#259](../../issues/259) — `ShardInfo.Replicas` reports observers and witnesses with a `Role` (breaking) — `docs/multi-group-host/` — done
- [#250](../../issues/250) — docs conformance: roadmap line, banner stamp, `.serena/project.yml` tracked — done
- [#251](../../issues/251) — `make lint` runs actionlint; trivy targets fail when trivy is missing — done
- [#252](../../issues/252) — Go 1.27.2 and `golang.org/x/net` v0.60.0 security bumps — done
- [#262](../../issues/262) — `make release-candidate VERSION=vX.Y.Z-rc.N` — done

## Now

The candidate's exit: deploy to staging, then consider production. Each line: issue, outcome, the `docs/<feature>/` folder that specifies it, status.

- [#269](../../issues/269) — exit: v0.21.0-rc.1 deployed to staging with the joint P1 run, every consumer building against it and filtering voters on `Role` (#259) — `docs/release/` — open
- [#280](../../issues/280) — an on-disk state machine whose `Open` is behind its local snapshot recovers instead of hitting `ErrApplyGap` — `docs/state-machines/` — in review
- [#279](../../issues/279) — `DiskInfo.Shards` is filled in — `docs/observability/` — open
- [#275](../../issues/275) — `test/proposer` re-resolving reads retry a new leader's `ErrReadIndexNotReady` — `docs/testing/` — in review
- [#277](../../issues/277) — code comments and a log sink agree with the code — `docs/observability/` — open
- [#272](../../issues/272) — an awaited `Submit` racing `Close` reports an indeterminate outcome — `docs/write-batching/` — blocked: owner ruling
- [#284](../../issues/284) — a failed `Unseal`'s backoff persists through `StatePersister` across restarts — `docs/seal-barrier/` — blocked: owner decision
- [#278](../../issues/278) — exported errors no `Host` method returns, and `OnCampaignSkipped`: removed or wired up — `docs/multi-group-host/` — blocked: owner decision
- [#281](../../issues/281) — the quorum-loss runbook works on the exporting survivor and is tested end to end — `docs/multi-group-host/` — blocked: owner decision
- [#268](../../issues/268) — log-storage, write-batching and observability docs re-verified — `docs/log-storage/`, `docs/write-batching/`, `docs/observability/` — in review
- [#274](../../issues/274) — the rc.1 documentation programme re-verified on Opus 5.5 — every `docs/<feature>/` — open
- [#283](../../issues/283) — spec-review scrub of `docs/` for the release candidate — every `docs/<feature>/` — blocked: department audit slot
- [#282](../../issues/282) — ROADMAP.md lists every milestone and names the folder behind each item — `ROADMAP.md`, no feature folder — open

## Next (v1.0.0 checklist)

- readiness: go-enclave at a public release — `docs/memory-enclave/` — blocked: v0.8.0-alpha is the newest tag, no candidate yet
- readiness: compatibility policy acknowledged (`release-versioning.md` §1, §8) — `docs/compatibility/` — open
- readiness: publication mechanism for agent files, deferred to v1.0.0 by the owner — `docs/release/` — open
- readiness: company CI template neutralised for the public tree — `docs/release/` — open: `.github/workflows` still names internal repositories and domains
- readiness: wire and on-disk format versions stay pinned until v1.0.0 ships (CLAUDE.md rule) — `docs/wire-protocol/`, `docs/log-storage/` — holds

## Later

Nothing accepted. New work arrives as issues.

## Dependencies

- Up: go-enclave v0.8.0-alpha (pinned = latest).
- Down: go-qrdb (pins v0.20.0, direct); indirect: go-xkms (v0.19.0), go-quicbaas (v0.15.0-alpha), go-signal (v0.20.0), go-trusted-platform (v0.13.1-alpha), go-recountly (v0.6.2-alpha); pins read from each `go.mod` on main. None is on v0.21.0-rc.1 yet.

## Non-goals

- Peer discovery, cluster formation, Shamir custody and the ceremony: go-qrdb.
- Hardware and cloud sealing strategies: go-xkms.
