# #1084: Drain returns nil while an admitted operation is pending

## Cause

Every admission path (Propose/SyncPropose via `registerPendingProposal`,
`ReadIndex`, `RequestSnapshot`) checks `h.draining` early and counts the
operation with `h.addPendingOp()` much later. A caller that passes the
draining check and is preempted while `Drain` stores the flag and loads
`pendingOps == 0` is counted, registered and admitted after `Drain` returned
nil. `TestHost_Drain_InFlightSyncProposeThenClose` sees it as
`pendingOps = 1 right after Drain returned nil`.

## Ordering argument

A new `Host.admitMu sync.RWMutex` orders admissions against the drain flag:

- `admitPendingOp()` holds `admitMu.RLock` while it checks `closed` and
  `draining` and increments `pendingOps`, and refuses with
  `ErrClosed`/`ErrDraining` without counting.
- `Drain` stores `draining` under `admitMu.Lock`, before it loads
  `pendingOps`.

So an admission either completes its increment before `Drain`'s store (and
`Drain` sees the count and waits for it), or runs after the store and sees
the flag. A pure count-then-recheck (Dekker) ordering would also exclude the
admission, but its rolled-back increment is briefly visible in `pendingOps`
after `Drain` returned nil, which is exactly what the drain test checks; the
lock leaves no transient count. The early `draining` checks stay as fast
paths. A refused admission never counts, so `completePendingOp`'s drainC
close is untouched.

## Callers changed

- `registerPendingProposal`: admits first; on refusal drops the caller's
  RequestState reference (as its rollback path already does) and returns the
  error, registering nothing.
- `ReadIndex`: admits before registering the pending read; on refusal
  releases the RequestState.
- `RequestSnapshot`: admits before registering the pending snapshot; on
  refusal releases the RequestState and clears the snapshotting flag.

`addPendingOp` stays as the unconditional counter the unit tests use to
seed `pendingOps`.

## Test

`pkg/host_drain_admission_test.go` drains an idle host, then performs the
registration the preempted proposer would, and requires `ErrDraining` with
`pendingOps == 0` and nothing left in the pending map.
