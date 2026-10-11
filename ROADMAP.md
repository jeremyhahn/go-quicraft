# go-quicraft roadmap

> **Status: REFERENCE** · Owner: Software Department · Last verified: 2026-10-11
> Per handbook `software/conventions/roadmap.md`: the Software Department refreshes it; the project agent proposes changes by PR.

## Mission

Multi-group Raft consensus over QUIC. Raft primitives only, plus the `Barrier`/`SealingStrategy` interfaces, `EpochBarrier`, the Shamir GF(256) primitive and the `none` and `passphrase` strategies (handbook `docs/00` L1).

## Current milestone

[`Public release`](../../milestone/11) (M11), target: release candidate, then production v1.0.0 after the joint P1 staging run. Candidate cut: **v0.21.0-rc.1** (2026-10-11); earlier candidates v0.16.0 (2026-10-02) and v0.20.0 (2026-10-08) are superseded.

v0.21.0-rc.1 contents, all done:

- [#257](../../issues/257) — `Host.ExecuteOnLeader`: leader-executed requests with no log entry — done
- [#259](../../issues/259) — `ShardInfo.Replicas` reports observers and witnesses with a `Role` (breaking) — done
- [#250](../../issues/250) — docs conformance: roadmap line, banner stamp, `.serena/project.yml` tracked — done
- [#251](../../issues/251) — `make lint` runs actionlint; trivy targets fail when trivy is missing — done
- [#252](../../issues/252) — Go 1.27.2 and `golang.org/x/net` v0.60.0 security bumps — done
- [#262](../../issues/262) — `make release-candidate VERSION=vX.Y.Z-rc.N` — done

## Now

The candidate's exit: deploy to staging, then consider production.

- [#269](../../issues/269) — exit: v0.21.0-rc.1 deployed to staging with the joint P1 run — open
- [#269](../../issues/269) — exit: each consumer builds against v0.21.0-rc.1 (pins under Dependencies) — open
- [#269](../../issues/269) — exit: consumers adapt to the `ShardInfo.Replicas` role change (#259), filtering voters on `Role` — open

## Next (v1.0.0 checklist)

Re-checked 2026-10-11 against the tree.

- readiness: go-enclave at a public release — blocked: v0.8.0-alpha is the newest tag, no candidate yet
- readiness: compatibility policy acknowledged (`release-versioning.md` §1, §8) — open
- readiness: publication mechanism for agent files, deferred to v1.0.0 by the owner — open
- readiness: company CI template neutralised for the public tree — open: `.github/workflows` still names internal repositories and domains
- readiness: wire and on-disk format versions stay pinned until v1.0.0 ships (CLAUDE.md rule) — holds

## Later

Nothing accepted. New work arrives as issues.

## Dependencies

- Up: go-enclave v0.8.0-alpha (pinned = latest).
- Down: go-qrdb (pins v0.20.0), go-xkms (v0.19.0), go-quicbaas (v0.15.0-alpha), go-signal (v0.20.0), go-trusted-platform (v0.13.1-alpha); pins read from each `go.mod` on main, 2026-10-11. None is on v0.21.0-rc.1 yet.

## Non-goals

- Peer discovery, cluster formation, Shamir custody and the ceremony: go-qrdb.
- Hardware and cloud sealing strategies: go-xkms.
