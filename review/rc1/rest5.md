Reviewed at e616cd74, packages: pkg/internal/raft, pkg/internal/engine

| # | severity | class | file:line | defect and the inputs that trigger it | proposed fix |
|---|---|---|---|---|---|
| 1 | P2 | correctness | pkg/internal/raft/bootstrap.go:172 | `ReconstructMembership` drops the error from `m.apply(cc, index)`. The runtime commit path (`peer.go:1601`) treats `proto.ErrUnsupportedVersion` from the same call as fatal, while recovery silently skips the entry and returns a membership that the runtime would have refused to build. The doc comment says the two paths must reach the same decision. Trigger: a committed `EntryConfigChange` whose wire version is outside this build's window, passed to `ReconstructMembership`. It is latent today because the only caller (`pkg/host.go:12448`) passes just the bootstrap entry. | Return `errors.Join(ErrCorruptConfigEntry, err)` when `errors.Is(err, proto.ErrUnsupportedVersion)`, and keep skipping the other rejections, the same way `peer.applyMembershipEntry` does. |

pkg/internal/raft: reviewed bootstrap.go, inflights.go, inmemory.go, quiesce.go, remote.go (the files in this slice)
