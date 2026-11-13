# 744: pin the QUIC close codes a refused or reclaimed peer receives

`TestInboundCloseCodes` (pkg/internal/transport/close_codes_test.go) asserts, on
the dialing side, `errors.As(context.Cause(conn.Context()), &*quic.ApplicationError)`
with `Remote == true`, the exact `ErrorCode` and the exact `ErrorMessage`, the way
`TestAcceptLoop_GlobalConnectionLimitRejects` (failpath_test.go) reads the code.

**per-IP limit.** A receiver with `MaxConnectionsPerIP: 1` (as in
`TestAcceptRejection_SettlesBeforeLogging`, windows_test.go). A first sender from
`newStartedTransport` dials it with `getConnection(addr, "")`; the test polls
`ipCountForTest("127.0.0.1") == 1` so the slot is held. A second sender, also on
loopback, dials; its `*quic.Conn` is the one `getConnection` returned. The receiver
closes it with `QUICErrIPLimitReached` and the reason `"per-IP connection limit reached"`.

**promiscuous certificate.** A receiver whose `inboundRefusalForTest` returns an
error (the same hook windows_test.go uses). One sender dials it; the returned
`*quic.Conn` must close with `QUICErrPromiscuousCert` and the redacted reason `""`.

**idle reclaim.** `transportPair` as in `TestIdleInboundConnectionIsReclaimed`
(conn_test.go): t1 sends one heartbeat, handler2 waits for the batch,
`awaitInboundConn(t, t2)` returns t2's inbound side. t1's cached connection to t2
is `t1.pooledConnForTest(t2.Addr().String())`. The sweep
`t2.reapIdleInboundConns(time.Now().Add(inboundIdleRetention + time.Second))`
must close t1's connection with `QUICErrInboundIdle` and reason `""`.

Every wait is bounded at 10 s and fails with the cause it saw. Non-vacuity is shown
by a temporary, uncommitted swap of `QUICErrIPLimitReached` for
`QUICErrConnLimitReached` in recv.go.
