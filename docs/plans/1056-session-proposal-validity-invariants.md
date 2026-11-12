# Plan for #1056: dragonboat client Session proposal validity

Test gap under the v1.0.0 bar (#744).

## Objective

Pin the documented behaviour of `contrib/dragonboat/client.Session`:

- `ValidForProposal` refuses a session that was never prepared
  (series ID below `SeriesIDFirstProposal`).
- `ValidForProposal` accepts `RespondedTo == SeriesID` and refuses
  `RespondedTo > SeriesID`.
- `PrepareForRegister` sets the register series ID and clears `RespondedTo`.
- `ValidForProposal` refuses a prepared session on another shard.

## File map

- `docs/plans/1056-session-proposal-validity-invariants.md` (this file)
- `contrib/dragonboat/client/session_validity_test.go` (new)
- `contrib/dragonboat/client/session.go`, only for a bug found

## Method

One test function per assertion, each named with `Validity`. A divergence
between code and documentation gets a failing test commit, then a fix
commit.

## Acceptance

- `cd contrib/dragonboat && go test -race -count=1 -run Validity ./client/`
- `cd contrib/dragonboat && go vet ./client/`
- `make lint`
