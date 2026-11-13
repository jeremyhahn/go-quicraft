# 744: Node proposal hand-off contract

Refs #744. `TestNodeProposalHandOff` in
`pkg/internal/engine/node_proposal_handoff_test.go` pins the Node-level
contract of `DrainProposalBatch`, `HandOffProposals` and `ProposalTaken`.
Each subtest builds a fresh Node with `newTestPeer` and `NewNode`.

**Empty queue.** `DrainProposalBatch` on a Node with nothing queued returns
the caller's buffer reset to length 0 (same backing array) and batch id 0.

**Drain then hand-off.** After `DeliverTrackedProposal`, *at is non-zero and
`ProposalTaken` is (false, false). The drain returns the delivered entries and
a non-zero id; the proposal stays (false, false) between drain and hand-off,
because a batch taken but never handed off is not taken. `HandOffProposals(id)`
returns true and the proposal becomes (true, false).

**Second batch.** A second delivery drains under a strictly larger id; it is
(false, false) until its own hand-off, while the first stays (true, false).

**Stop fence.** A batch drained before `Node.Stop` cannot be handed off after
it: `HandOffProposals` returns false and the proposal is (false, true), final
and never taken. A proposal still queued at Stop is also (false, true); a
later drain takes nothing (empty, id 0) and `DeliverTrackedProposal` returns
false. Mutation check: deleting `n.proposalQ.Close()` from `Node.Stop` makes
this subtest fail.

**markFailed fence.** The same assertions with `markFailed` in place of Stop.

**Never delivered.** A proposal whose *at is 0 is (false, false).
