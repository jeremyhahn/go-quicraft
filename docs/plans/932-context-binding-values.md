# #932: ContextBinding values and a barrier's ContextBinding across its lifecycle

## Objective

Close the v1.0.0 bar test gap in `pkg/seal/barrier_binding.go`:
`ContextBinding.Resolved` has no test, and `Barrier.ContextBinding` is not
checked across Initialize, Seal and Unseal on one barrier.

## Slices

1. `docs: plan for #932` — this file.
2. `test:` — `pkg/seal/context_binding_values_test.go` (package seal):
   - `TestContextBindingValues`: a table over every `ContextBinding`
     constant plus one out-of-range value. `Resolved` is false only for
     `ContextBindingUnresolved`; `Binds` is true only for
     `ContextBindingBarrier` and `ContextBindingDelegated`; `String` gives the
     documented name (`unresolved`, `barrier`, `delegated`, `unsupported`,
     `plaintext`) and `unknown` for the out-of-range value.
   - `TestContextBindingValuesBarrierLifecycle`: a software barrier reports
     `ContextBindingUnresolved` before `Initialize`, `ContextBindingBarrier`
     after it, `ContextBindingUnresolved` after `Seal`, and
     `ContextBindingBarrier` again after `Unseal`.
3. Per divergence between code and documentation: a failing `test:` commit,
   then a `fix:` commit in `pkg/seal/barrier_binding.go` (or a doc-comment
   correction there when the code is right).

New helpers take `context.Context` first, before `t *testing.T`.

## Out of scope

- Delegated/hardware encryptor lifecycles (covered by
  `barrier_hardware_aad_test.go` and `barrier_regression_test.go`).
- Any wire or on-disk format change.
