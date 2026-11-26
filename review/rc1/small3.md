Reviewed at e616cd74, packages: pkg/internal/queue, pkg/internal/supervise, pkg/internal/server, pkg/internal/registry, pkg/internal/netaddr, pkg/internal/invariant

| # | severity | class | file:line | defect and the inputs that trigger it | proposed fix |
|---|---|---|---|---|---|

This third pass covers only the files and areas the first two passes did not reach in the six packages above. It found no defect that meets the bar. The paths checked and why they hold:

- queue: SwapBatch zeroes the recycled array before reuse. Batch ids only rise, and HandOff keeps the highest id. Taken reports closed, so an item dropped by Close is never left waiting. Empty's atomic is written only under the lock, on transitions. SwapBuffer has no production caller.
- supervise: drain's give-up and take-back (Store false, len check, CAS) cannot lose a record queued in the gap. RunResilient publishes the outcome before it logs. RunEach consumes each index before the call, so a call that panics or calls Goexit is never repeated. A Goexit restart counts the new goroutine on wg before it releases the old one.
- server/rate.go: every Tick drops follower entries older than gcTick, so the map keyed by sender id stays bounded. The hysteresis and the atomic snapshot are single-writer, as documented.
- registry: count is changed only on the loaded/stored result of each sync.Map operation. CompareAndRegister on an absent key is a no-op.
- netaddr: CanonicalHost errs toward treating two spellings as the same host. The only effect is a refused configuration, never an accepted mismatch. Host("[v6]") with no port is passed on verbatim, and x509 VerifyHostname strips the brackets itself.
- invariant: Assert panics only. Its documented rule (never assert on a value a peer controls) concerns call sites, which are outside this slice.

pkg/internal/queue: reviewed swapbuffer.go, entryqueue.go (Add, Taken, HandOff, Close, AddOne, Swap, SwapBatch, Len, Empty); no finding
pkg/internal/supervise: reviewed supervise.go fully; no finding
pkg/internal/server: reviewed rate.go fully; no finding
pkg/internal/registry: reviewed registry.go, errors.go fully; no finding
pkg/internal/netaddr: reviewed netaddr.go (CanonicalHost, Host); no finding
pkg/internal/invariant: reviewed assert.go fully; no finding
pkg: not in this pass's scope (earlier passes); nothing reviewed here
pkg/config: not in this pass's scope; not reached hostconfig.go, compression.go, errors.go, logging.go, the rest of config.go and transportconfig.go
pkg/sm: not in this pass's scope; not reached createfunc.go, errors.go, interfaces.go, types.go, the rest of cached.go and lru.go
pkg/batch: not in this pass's scope; not reached errors.go, the rest of aggregator.go
pkg/writemode: not in this pass's scope; not reached errors.go
pkg/internal/rsm: not in this pass's scope; not reached adapter.go, errors.go, the rest of statemachine.go
pkg/internal/session: not in this pass's scope; not reached errors.go, session.go, the rest of manager.go
