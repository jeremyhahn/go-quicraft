# QuicRaft Performance Profiling Analysis

- Profiling Duration: 233.06s
- Total CPU Samples:  467.21s (200.47% utilization, multi-core)
- Profile Files: Generated via `Reproducing This Profile` section below

## Summary

QUIC transport I/O and Go runtime scheduling are the dominant CPU consumers, accounting for ~57% of CPU time. Context and timer allocations on the read path are the primary memory hotspots. LeaseRead delivers a 30-34x throughput improvement over standard ReadIndex by eliminating the heartbeat round-trip.

## CPU Profile

### Top Consumers (Flat %)

| Component | CPU % | Description |
|-----------|-------|-------------|
| `syscall/linux.Syscall6` | 11.91% | UDP sendmsg (QUIC transport) |
| `runtime.futex` | 5.84% | OS futex calls for goroutine scheduling |
| `runtime.stealWork` | 4.30% | Work-stealing scheduler |
| `runtime.(*randomEnum).next` | 3.77% | Scheduler randomization |
| `runtime.selectgo` | 3.62% | Channel select across engine workers |
| `runtime.lock2` | 3.59% | Runtime mutex locking |
| `runtime.nanotime` | 2.44% | Time measurement |
| `runtime.unlock2` | 2.18% | Runtime mutex unlock |
| `internal/sync.(*Mutex).Lock` | 2.17% | Sync primitives |
| `time.runtimeNow` | 1.81% | Clock reads for latency recording |
| `internal/runtime/atomic.(*Uint32).CompareAndSwap` | 1.67% | Atomic CAS operations |
| `sync/atomic.(*Int32).Add` | 0.85% | Atomic counters |
| **Total runtime/scheduler** | **~43%** | |
| **Total QUIC transport** | **~12%** | |

### Cumulative CPU (QuicRaft Paths)

| Function | Cumulative % | Role |
|----------|-------|------|
| `engine.(*stepWorker).run` | 13.46% | Core shard processing loop |
| `quic-go.(*Conn).run` | 11.60% | QUIC connection management |
| `quic-go.sendQueue.Run` | 8.99% | QUIC packet transmission |
| `engine.(*stepWorker).processReady` | 8.38% | Update batching, WAL/apply dispatch |
| `Host.SyncRead` | 5.76% | Linearizable read path |
| `transport.(*QUICTransport).handleStream` | 4.52% | Inbound message deserialization |
| `Host.ReadIndex` | 3.03% | ReadIndex submission and waiting |
| `engine.(*commitWorker).run` | 2.68% | Commit pipeline worker |
| `raft.(*Peer).Handle` | 2.51% | Raft message processing |
| `raft.(*Peer).GetUpdate` | 0.61% | Update extraction from Raft core |

### LeaseRead CPU Path

LeaseRead benchmarks account for 5.96% of total cumulative CPU while executing ~2M+ operations each.

| Function | Cumulative % | Role |
|----------|-------|------|
| `BenchmarkSyncRead_3N_WAL_LeaseRead` | 3.67% | 3N benchmark goroutine |
| `BenchmarkSyncRead_5N_WAL_LeaseRead` | 2.29% | 5N benchmark goroutine |
| `Host.SyncRead` | 2.77% | Top-level read API |
| `engine.drainAndHandleReadIndex` | 1.88% | Step worker ReadIndex drain |
| `context.WithDeadlineCause` | 1.65% | Per-request context allocation |
| `Host.ReadIndex` | 1.32% | Lease validity check + SM lookup |
| `engine.WorkSignal.Notify` | 0.61% | Non-blocking wake signal |

`Peer.Handle` does not appear in the LeaseRead hot path. The lease check (`checkQuorumActive`) short-circuits before enqueuing a ReadIndex message. CPU time is dominated by context allocation and scheduler overhead, not Raft consensus.

## Memory Profile

### Top Allocators

| Component | Allocated | Total % | Description |
|-----------|-----------|---------|-------------|
| `rsm.(*StateMachine).Lookup` | 2,142 MB | 15.53% | Data copy per read |
| `BenchQuicRaftStaleRead` | 1,727 MB | 12.52% | Benchmark test harness allocations |
| `context.WithDeadlineCause` | 1,191 MB | 8.63% | New context per SyncRead/SyncPropose |
| `time.newTimer` | 1,022 MB | 7.41% | Timer per deadline context |
| `logdb/memdb.mergeEntries` | 929 MB | 6.74% | Entry merging on log append |
| `Host.ReadIndex` | 825 MB | 5.98% | RequestState allocation per read |
| `context.(*cancelCtx).Done` | 816 MB | 5.92% | Context channel allocation |
| `perfresult.NewLatencyRecorder` | 728 MB | 5.28% | Benchmark recorder (test infrastructure) |
| `transport.NewQUICTransport.func1` | 665 MB | 4.82% | QUIC recv buffer allocations |
| `raft.(*inMemory).merge` | 424 MB | 3.08% | In-memory log merge |
| `raft.(*raft).addReadState` | 293 MB | 2.12% | ReadState per ReadIndex round |
| `logdb.newTermRingBuffer` | 222 MB | 1.61% | Ring buffer per election cycle |

LeaseRead allocates 554 B/op vs 3,635 B/op for standard 3N ReadIndex (85% reduction). It bypasses `raft.(*raft).addReadState` entirely since no heartbeat round-trip occurs.

## Benchmark Results

### Single-Node

| Benchmark | ns/op | MB/s | B/op | allocs/op |
|-----------|-------|------|------|-----------|
| Propose_SEQ_WAL | 485,749 | 0.08 | 989 | 16 |
| Propose_SEQ_Memory | 11,338 | 3.53 | 609 | 11 |
| CommitLatency_WAL | 473,916 | -- | 1,019 | 16 |
| CommitLatency_Memory | 11,041 | -- | 616 | 11 |
| SyncRead_WAL | 2,604 | -- | 120 | 4 |
| SyncRead_Memory | 2,603 | -- | 120 | 4 |
| StaleRead_WAL | 307.1 | -- | 48 | 2 |
| StaleRead_Memory | 289.7 | -- | 48 | 2 |

### Multi-Node (3N/5N, real QUIC + mTLS)

| Benchmark | ns/op | B/op | allocs/op | ops/sec |
|-----------|-------|------|-----------|---------|
| SyncRead_3N_WAL | 164,312 | 3,635 | 84 | 6,086 |
| SyncRead_5N_WAL | 199,646 | 6,980 | 159 | 5,009 |
| SyncRead_3N_WAL_LeaseRead | 5,517 | 554 | 10 | 181,260 |
| SyncRead_5N_WAL_LeaseRead | 6,700 | 600 | 10 | 149,254 |

### LeaseRead vs ReadIndex

| Scenario | ns/op | Speedup | allocs/op | Alloc Reduction |
|----------|-------|---------|-----------|-----------------|
| 3N ReadIndex | 164,312 | 1x | 84 | -- |
| 3N LeaseRead | 5,517 | 29.8x | 10 | 88% |
| 5N ReadIndex | 199,646 | 1x | 159 | -- |
| 5N LeaseRead | 6,700 | 29.8x | 10 | 94% |

LeaseRead eliminates the heartbeat round-trip required by ReadIndex. The leader's lease validity check is a local clock comparison instead of a network round-trip.

- ReadIndex (3N): ~6,086 reads/sec
- LeaseRead (3N): ~181,260 reads/sec (30x throughput)

## Optimizations Implemented

| Optimization | Target | Status |
|-------------|--------|--------|
| Timer pooling (`sync.Pool`) | `RequestState.Result()` timer allocation | Implemented in `pkg/requeststate.go` |
| SM Lookup zero-copy (`NALookupInto`) | `rsm.(*StateMachine).Lookup` buffer allocation | Implemented in `pkg/internal/rsm/`, `pkg/sm/kv/`, `pkg/host.go` |
| Pooled read APIs | `Host.SyncReadBuf()`, `Host.StaleReadBuf()` | Implemented in `pkg/host.go` |

Timer pooling eliminates heap allocation for the `Result()` deadline timer. SM Lookup zero-copy introduces `NALookupInto` where callers provide a destination buffer, and `SyncReadBuf`/`StaleReadBuf` APIs that use a `sync.Pool` of reusable byte buffers. Callers receive a `release()` function to return the buffer to the pool after use.

## Remaining Optimization Opportunities

| Priority | Target | Current Impact | Approach |
|----------|--------|---------------|----------|
| MEDIUM | Context allocation | 8.63% of allocations (1,191 MB) | Benchmark callers create `context.WithTimeout` — internal paths already pass raw deadlines |
| LOW | QUIC GSO tuning | 11.91% CPU | Tune `MaxDatagramSize` and GSO batch size |
| LOW | ReadState pool | 2.12% of allocations (293 MB) | `sync.Pool` for `ReadState` structs (standard ReadIndex only) |

The remaining context allocation is from callers creating `context.WithTimeout` before calling `SyncRead`/`SyncPropose`. Internal paths already extract the deadline as `time.Time` with zero allocation. This is a caller-side cost that cannot be eliminated in the library.

## Reproducing This Profile

```bash
cd test/performance/quicraft
mkdir -p ../profiles

GOWORK=off go test -tags=benchmark -run=^$ \
  -bench='BenchmarkPropose_SEQ_WAL|BenchmarkPropose_SEQ_Memory|BenchmarkSyncRead_WAL|BenchmarkSyncRead_Memory|BenchmarkStaleRead_WAL|BenchmarkStaleRead_Memory|BenchmarkCommitLatency_WAL|BenchmarkCommitLatency_Memory|BenchmarkSyncRead_3N_WAL_LeaseRead|BenchmarkSyncRead_5N_WAL_LeaseRead|BenchmarkSyncRead_3N_WAL|BenchmarkSyncRead_5N_WAL' \
  -benchtime=10s \
  -cpuprofile=../profiles/cpu.prof \
  -memprofile=../profiles/mem.prof \
  -benchmem -count=1 -timeout=3600s .

go tool pprof -top ../profiles/cpu.prof
go tool pprof -top -cum ../profiles/cpu.prof
go tool pprof -top ../profiles/mem.prof
```
