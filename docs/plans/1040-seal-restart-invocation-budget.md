# Plan: #1040 seal restart invocation budget

Test gap in `pkg/seal/barrier.go` `resolveInvocationBudget`, called from
`Unseal`.

## Assertions (pkg/seal/barrier_restart_budget_test.go)

a. A barrier restored from a sealed blob whose `StatePersister` refuses any
   blob with Epoch above 1 unseals with nil, stays at `CurrentEpoch()` 1, has
   `InvocationCounter().Count()` equal to the limit, refuses `Encrypt` with
   `ErrInvocationLimitExceeded`, and decrypts a ciphertext made before the
   restart.
b. In that case and in the no-persister restart, exactly one error-level
   record carries `cause` = `ErrInvocationBudgetUnknown.Error()`.
c. With `InvocationLimit` -1 and no persister, a restart keeps epoch 1,
   `Encrypt` succeeds with header epoch 1, the count is 0, and no record
   carries that cause.

Every test name contains `RestartBudget`. A divergence from the docs is a
failing test commit then a fix commit in `pkg/seal/barrier.go`.

Refs #1040
