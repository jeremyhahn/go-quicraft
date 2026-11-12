# Plan: #1042 transport event delivery isolation

Test gap under the v1.0.0 bar (#744).

## Objective

Pin the documented behaviour of `pkg/internal/transport/events.go`:

- `deliverEvent` skips nil calls, runs the rest in order, each isolated
  (`supervise.RunEach`), and runs nothing when all calls are nil.
- `snapshotEvent` returns nil with no listener or no callback, and otherwise a
  call that passes the `SnapshotInfo` through.
- `reportSnapshotSent` delivers `OnSnapshotSent` and the `IncSnapshotsSent`
  metric, each isolated: a callback that panics or calls `runtime.Goexit`
  neither escapes nor stops the metric.

## File map

- docs/plans/1042-transport-event-delivery-isolation.md (this file)
- pkg/internal/transport/event_delivery_isolation_test.go (new)
- pkg/internal/transport/events.go only for a bug found

## Acceptance

`go test -race -count=1 -run EventDeliveryIsolation ./pkg/internal/transport/`,
`go vet ./pkg/internal/transport/` and `make lint` pass.
