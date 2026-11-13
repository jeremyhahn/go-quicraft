# Plan: MemDB readers return copies (#1071)

## Problem

`MemDB.GetSnapshot`, `MemDB.ReadState` and `MemDB.GetBootstrap` in
`pkg/logdb/memdb/memdb.go` return the stored `Membership` and
`Bootstrap.Addresses` maps as-is. A caller that edits a returned map edits
the stored record, and `SaveSnapshot` stores one membership as both the
snapshot's and the node's, so an edit through `ReadState` also changes what
`GetSnapshot` reports. The write side already deep-copies; waldb copies on
the read side too and pins it in `pkg/logdb/waldb/aliasing_test.go`.

## Change

1. New `pkg/logdb/memdb/read_copy_test.go`, every test named `*ReadSideCopy*`:
   - (a) mutating the first `GetSnapshot` result's Addresses, Removed and
     Identities leaves a second `GetSnapshot` unchanged;
   - (b) mutating the `ReadState` membership's RemovedIdentities and
     Observers leaves a second `ReadState` and a following `GetSnapshot`
     unchanged;
   - (c) deleting from and adding to the first `GetBootstrap` result's
     Addresses leaves a second `GetBootstrap` unchanged;
   - (d) nil Witnesses / RemovedIdentities read back nil from `GetSnapshot`
     and `ReadState` (the copy must not invent empty maps).
2. Expected to fail on the current code: test commit first, then a fix
   commit in `memdb.go` returning `copyMembership(...)` /
   `copyAddrMap(...)` from the three readers under the read lock.

## Acceptance

`go test -race -count=1 -run ReadSideCopy ./pkg/logdb/memdb/`,
`go vet ./pkg/logdb/memdb/`, `make lint` clean.
