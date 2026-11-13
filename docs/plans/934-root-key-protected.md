# Plan: #934 Barrier.RootKeyProtected test gap

## Objective

`Barrier.RootKeyProtected` (pkg/seal/barrier.go) has a test only for the
frozen pages it returns on Linux. Pin its documented contract in a
platform-independent test.

## Slices

1. `pkg/seal/root_key_protected_test.go` (package seal):
   - (a) on an unsealed barrier the returned bytes equal `RootKey`;
   - (b) two calls return independent copies: destroying one leaves the
     other readable, and the barrier still encrypts and decrypts;
   - (c) before `Initialize` and after `Seal` it returns `ErrBarrierSealed`;
   - (d) a barrier initialized with `NoneStrategy` returns
     `ErrNoRootKeyAvailable`;
   - (e) after `Unseal` it returns the same root key as before the seal.
2. Per divergence from the doc comment: a failing test commit, then a fix
   commit in pkg/seal/barrier.go (or a doc fix where the code is right).

## Out of scope

- The frozen-page property (covered by strategy_freeze_linux_test.go).
- Any wire or on-disk format change; any other package.
