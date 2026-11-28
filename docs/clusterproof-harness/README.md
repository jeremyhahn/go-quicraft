# clusterproof-harness

Status: in progress ([#1301](../../../issues/1301)).

`test/clusterproof` proves, under process crashes, the guarantee an embedder
relies on: a write the cluster acknowledged is never lost, and a
linearizable read sees it.

## Run it

```sh
make test-clusterproof
make test-clusterproof CLUSTERPROOF_ARGS='-clusterproof.cycles=50 -clusterproof.seed=7'
```

The target runs `go test -race -tags=integration ./test/clusterproof/...`.
The default run (10 cycles) takes about a minute.

| Flag | Default | Meaning |
|---|---|---|
| `-clusterproof.cycles` | 10 | SIGKILL-restart cycles |
| `-clusterproof.seed` | 0 (clock) | seed of the kill schedule and of the clients |
| `-clusterproof.writers` | 4 | concurrent writers |
| `-clusterproof.readers` | 2 | concurrent linearizable readers |
| `-clusterproof.rtt-ms` | 20 | `HostConfig.RTTMillisecond` (election timeout is 50 RTTs) |
| `-clusterproof.ack-before-commit` | false | negative check, see below |

A failure names the lost writes or the stale read, prints the tail of each
replica's log, and prints the `CLUSTERPROOF_ARGS` that rerun the same kill
schedule (the seed fixes which replica dies and for how long; timing still
varies between runs).

## What it does

1. Three replica processes. Each is the test binary re-executed as
   `TestClusterproof_Node`: one `Host` with mutual TLS and the WAL fsync on
   (`NoSyncWAL` false), a three-voter shard of a last-write-wins KV state
   machine (snapshot every 500 entries, so restarts recover from a snapshot
   and a compacted log), and a loopback HTTP server the parent drives:
   `POST /put` commits a write through that replica (a follower forwards to
   the leader), `GET /get` and `GET /all` are linearizable `SyncRead`s.
2. Writers put unique keys to random replicas, the dead one included. A
   write counts as acknowledged only after the replica answered it
   committed and the writer appended it to the ack log and fsynced the log.
3. Readers pick an acknowledged write and read it through a random replica;
   a read that answers must return the acknowledged value.
4. Each cycle SIGKILLs a random replica, keeps it down for 300 ms to 1.5 s
   while the writers continue, restarts it on the same WAL and NodeHost
   directories, and waits until it serves a linearizable read. The checker
   then reads the ack log back from disk and requires every acknowledged
   write on every replica (through `GET /all`) within 60 s, and requires
   that the cluster acknowledged writes during the cycle.
5. At the end the clients stop, every acknowledged write must be on every
   replica, and the three replicas' states must be identical.

## Negative check

`-clusterproof.ack-before-commit` makes the writers record a write as
acknowledged before the cluster has committed (and so fsynced) it. Writes
sent to a killed replica are then recorded but never stored, and the
harness must fail: with readers on, a reader catches an acknowledged key
that is absent; with `-clusterproof.readers=0`, the cycle checker reports
the lost writes. Both were run when the harness landed and both fail.
