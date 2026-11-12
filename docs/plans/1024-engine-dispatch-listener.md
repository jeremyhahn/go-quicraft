# Plan: #1024 Engine.DispatchListener test gap

Refs #1024 (v1.0.0 bar test gap, tracked under #744).

## Objective

`Engine.DispatchListener` and `Engine.ListenerNotificationsDropped`
(pkg/internal/engine/engine.go) wrap the listener dispatcher
(pkg/internal/engine/dispatch.go). No test calls the two Engine methods, the
inline path or the panic recovery through the engine.

## Test (pkg/internal/engine/engine_dispatch_listener_test.go)

- (a) Before `Start`, `DispatchListener` runs fn before returning, on the
  calling goroutine, and returns true; the same after `Stop`.
- (b) While running, 100 dispatched functions run on a goroutine other than
  the caller's, in dispatch order.
- (c) With the dispatcher parked (`blockDispatcher`) and `listenerQueueSize`
  functions queued, each further call returns false without blocking and
  `ListenerNotificationsDropped` rises by exactly one per refusal.
- (d) A function that panics does not crash the test, and a function
  dispatched after it still runs.

Every test name contains `DispatchListener`; helpers take
`context.Context` first.

## Bugs

A divergence between code and documentation is a failing `test:` commit then
a `fix:` commit in engine.go or dispatch.go; a wrong doc comment over correct
code is corrected in place.

## Acceptance

`go test -race -count=1 -run DispatchListener ./pkg/internal/engine/`,
`go vet ./pkg/internal/engine/`, `make lint`, and
`git diff --name-only bf2df200..HEAD` listing only these files.
