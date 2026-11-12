# Plan: #946 ReplicaRole values

Objective: cover the `ReplicaRole` constants in pkg/info.go, which no test
names. `Host.replicaProgress` fills `ReplicaProgress.Role` with
`ReplicaRole(progress[i].Role)`, a direct conversion from the raft `Role`
string, so the two sets of values must stay identical.

## Slices

1. pkg/replica_role_test.go (package quicraft):
   - (a) each `ReplicaRole` constant equals its `raft.Role` counterpart as a
     string (voter, observer, witness), so the conversion loses nothing;
   - (b) the three values are `voter`, `observer` and `witness`, and are
     distinct;
   - (c) a `ReplicaProgress` with each role marshals with `encoding/json`
     to an object whose `role` field is that string, and unmarshals back to
     an equal value.
2. Per divergence between code and documentation: a failing test commit,
   then a fix commit in pkg/info.go (or a doc correction there when the
   code is right).

## Out of scope

- Any change to raft roles, wire or on-disk formats.
- Other fields of ReplicaProgress beyond the JSON round trip.
