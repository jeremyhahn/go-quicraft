# #933: Host.ReloadMTLS refusals and identities

## Objective

Close the v1.0.0 bar test gap on `Host.ReloadMTLS` (pkg/host.go): the
Host-level tests only check a new keypair through `errors.Is`. Pin the
documented behaviour with a Host-level test.

## Slices

1. This plan.
2. pkg/host_reload_mtls_refusals_test.go (package quicraft):
   - a leaf on a new keypair: `errors.As` gives a `*NodeIdentityChangedError`
     whose `Current` is the host's `NodeIdentity()` and whose `Reloaded` is
     the `NodeIdentity()` of a Host built from that material;
   - a certificate paired with a key that does not match it, and material
     with an empty CA pool, are each refused with `ErrMTLSReload`;
   - after every refusal `NodeIdentity()` is unchanged;
   - a renewed leaf on the same key with a new serial is accepted and keeps
     `NodeIdentity()`.
3. Per divergence between code and doc comment: a failing test commit, then
   a fix commit in pkg/host.go (or a doc correction where the code is right).

## Out of scope

Transport-level reload tests, CA rollover ordering, peer-overlap checks,
revocation; no wire or on-disk format change.
