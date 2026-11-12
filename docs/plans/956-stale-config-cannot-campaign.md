# #956: TimeoutNow on a stale configuration returns ErrStaleConfigCannotCampaign

Refs #956 (v1.0.0 bar test gap, tracked under #744).

## Objective

`handleTimeoutNow` (pkg/internal/raft/raft.go) refuses to campaign with
`ErrStaleConfigCannotCampaign` when `configCurrentForCampaign` is false, i.e.
when the `resolveStaleConfig` hook cannot fold the committed configuration
tail. Only the resolving path is tested today. Pin the refusing path, the
election-timeout path that shares the gate, and the resolving/nil-hook paths.

## Slices

1. This plan.
2. pkg/internal/raft/stale_config_timeout_now_test.go, on a three-member
   follower from `newTestRaftWithMembers`:
   - (a) a TimeoutNow from the leader at the current term with a hook that
     reports false returns an error matching `ErrStaleConfigCannotCampaign`;
   - (b) afterwards the state is follower, term, vote and leader ID are
     unchanged, and no RequestVote or RequestPreVote is pending;
   - (c) ticking past the election timeout leaves it a follower at the same
     term with no vote request sent;
   - (d) with the hook reporting true the same TimeoutNow makes it a
     candidate soliciting votes from both peers;
   - (e) a nil hook behaves as (d).
3. Per divergence from the documentation: a failing test commit, then a fix
   commit in pkg/internal/raft/raft.go.

## Out of scope

- The fold itself (`resolveStaleConfig` as installed by NewPeer); covered by
  restart_stale_config_regression_test.go.
- Any wire or on-disk format change.
