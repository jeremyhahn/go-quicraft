# Plan: #944 MemDB.AtRestProtection reports AtRestNonDurable

## Objective

`MemDB.AtRestProtection` (pkg/logdb/memdb/memdb.go) documents that an
in-memory store reports `logdb.AtRestNonDurable`, a state distinct from
encrypted and unprotected. No test calls it. Close that gap.

## Slices

1. This plan.
2. pkg/logdb/memdb/at_rest_protection_test.go (package memdb):
   - `New()` satisfies `logdb.AtRestProtectedLogDB` (compile-time
     assertion plus an interface check on the value);
   - it reports `AtRestNonDurable`, and none of `AtRestUnprotected`,
     `AtRestBarrierEncrypted`, `AtRestHostRouted`, on a new store, after
     a `SaveState` of a few entries, and after `Close` called twice
     (Close is idempotent);
   - the reported value's `String()` is `non_durable`.
3. Per divergence of code from documentation: a failing test commit,
   then a fix commit in pkg/logdb/memdb/memdb.go (a wrong doc comment
   where the code is right is corrected in the same file instead).

## Out of scope

- Other LogDB implementations' at-rest reporting.
- Any wire or on-disk format change.
