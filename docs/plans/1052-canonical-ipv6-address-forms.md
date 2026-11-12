# Plan: #1052 netaddr.Canonical IPv6 address forms

## Objective

Pin how `netaddr.Canonical` (pkg/internal/netaddr/netaddr.go) writes IPv6
address forms that no existing test reaches through `Canonical`:

- (a) `[::ffff:10.0.0.5]:5001` canonicalises to `10.0.0.5:5001`, the same as
  `10.0.0.5:5001`; `Host` of that input is the host component as written
  (`::ffff:10.0.0.5`, per its doc: "extracts the host component"), and
  `CanonicalHost(Host(...))` is `10.0.0.5`.
- (b) `0:0:0:0:0:0:0:1` (no port) canonicalises to `::1`, unbracketed.
- (c) `[0:0:0:0:0:0:0:1]` (bracketed, no port) canonicalises to `::1`.
- (d) `[::ffff:10.0.0.5]:05001` canonicalises to `10.0.0.5:5001`.

## File map

- docs/plans/1052-canonical-ipv6-address-forms.md (this plan)
- pkg/internal/netaddr/ipv6_form_test.go (new; test names contain `IPv6Form`)
- pkg/internal/netaddr/netaddr.go (only for a bug, or a wrong doc comment)

## Known doc divergence

The `Canonical` doc comment says "the port verbatim", but the code
normalises the port numerically through `canonicalPort` (the documented,
tested behaviour of that helper). The code is right; the doc comment is
corrected in netaddr.go.

## Acceptance

- `go test -race -count=1 -run IPv6Form ./pkg/internal/netaddr/` passes
- `go vet ./pkg/internal/netaddr/` passes
- `make lint` is clean
- `git diff --name-only aff54a08..HEAD` lists only File map files

Refs #1052
