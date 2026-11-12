# Plan: #1012 — Peer.RequeueMessages ordering and the free-order drain

Test gap under the v1.0.0 bar (#744). `Peer.RequeueMessages`
(`pkg/internal/raft/peer.go`) calls `raft.requeueMessages`
(`pkg/internal/raft/raft.go`), which puts drained messages back at the front of
`r.msgs` and marks them as held ordered (`orderedHeld`). Nothing tests it.

## Tests (`pkg/internal/raft/requeue_messages_test.go`)

- (a) Drain `[VoteResp, ReplicateResp]`, queue a newer message, requeue the
  drained pair: the next `drainMessages` returns the pair first, in their
  original order, then the newer message.
- (b) Requeuing nil or an empty slice leaves `r.msgs` and `orderedHeld`
  unchanged.
- (c) Requeue `[HeartbeatResp, Heartbeat]`, queue a newer `HeartbeatResp`:
  `drainFreeOrderMessages(true)` returns the Heartbeat and the newer
  HeartbeatResp and keeps the requeued HeartbeatResp.
- (d) The next `drainMessages` returns the kept HeartbeatResp and leaves
  `orderedHeld` at 0.
- (e) `Peer.RequeueMessages` on a peer from `newTestPeer` behaves the same.

Every test name contains `RequeueMessages`; helpers take `context.Context`
first.

## Bugs

If the code differs from its documentation: a failing test commit, then a fix
commit in `pkg/internal/raft/raft.go`. A wrong doc comment where the code is
right is corrected in the same file.

## Acceptance

`go test -race -count=1 -run RequeueMessages ./pkg/internal/raft/`,
`go vet ./pkg/internal/raft/`, `make lint`.
