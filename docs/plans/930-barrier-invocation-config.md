# Plan: #930 barrier invocation configuration

## Objective

`BarrierConfig.InvocationLimit` and `BarrierConfig.RotationThreshold`
(pkg/seal/barrier_config.go) build the barrier's own invocation counter in
`NewBarrier` when no counter is injected. `RotationThreshold` has no test and
`InvocationLimit` is only tested on an injected counter. Pin the documented
behaviour on an initialized barrier with no injected counter.

## Slices

1. Test: pkg/seal/barrier_invocation_config_test.go (package seal):
   - (a) `InvocationLimit` 8, `RotationThreshold` 50: `ShouldRotateKey` is
     false below 4 encrypts and true from 4.
   - (b) `RotationThreshold` 0 gives the default 95: with limit 20,
     false at 18 encrypts (90%), true at 19 (95%).
   - (c) after 8 encrypts the next `Encrypt` fails with
     `ErrInvocationLimitExceeded` and returns no ciphertext.
   - (d) `Rotate` resets the count: `ShouldRotateKey` false and `Encrypt`
     succeeds again.
   - (e) `InvocationLimit` -1 keeps `ShouldRotateKey` false after many
     encrypts.
2. Per divergence from the documentation: failing test commit, then a fix
   commit in pkg/seal/barrier.go or pkg/seal/aead_tracking.go (or a doc
   correction where the code is right).

## Out of scope

Nonce tracking, persisted invocation seeding, wire or on-disk formats.
