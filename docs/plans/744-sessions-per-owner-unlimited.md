# Plan for su744a: MaxSessionsPerOwner default and SessionsPerOwnerUnlimited

Refs #744 (v1.0.0 bar test gap).

## Objective

`Config.MaxSessionsPerOwner` has two documented values that no test names:
`DefaultMaxSessionsPerOwner` (128), which `SetDefaults` substitutes for an
unset 0, and `SessionsPerOwnerUnlimited` (`math.MaxUint64`), which turns the
per-owner cap off and needs no special case because `MaxTotalSessions` still
bounds the session table. Pin both.

## Tests

`pkg/config/sessions_per_owner_test.go`,
`TestConfig_SetDefaults_MaxSessionsPerOwner`: a zero field becomes
`DefaultMaxSessionsPerOwner`; the default is non-zero (zero is the unset value
`SetDefaults` replaces) and not `SessionsPerOwnerUnlimited`, and below `DefaultMaxTotalSessions` (otherwise one owner could
fill a default shard and the default cap would mean nothing);
`SessionsPerOwnerUnlimited` and an explicit 7 survive `SetDefaults` unchanged
and pass `Validate`.

`pkg/host_sessions_per_owner_unlimited_test.go`,
`TestHostSessionsPerOwnerUnlimited`: a single-replica shard with
`MaxSessionsPerOwner = config.SessionsPerOwnerUnlimited` and
`MaxTotalSessions = 3`. One owner registers 3 sessions with
`NewSessionWithOwner`; the 4th is refused by the shard-wide total. The
refusal shape comes from the code, not a guess: `session.Manager.Register`
returns `ErrSessionLimitExceeded` for both the total and the owner cap
(manager.go), the state machine answers the register with a result value
other than the client ID, and `Host.NewSessionWithOwner` maps that to
`*SessionError{Op: "register", Err: ErrInvalidOperation}` (host.go). After
`CloseSession` of one session completes, the same owner registers again.

## Non-vacuity

Temporarily (never committed) make `SetDefaults` reset
`MaxSessionsPerOwner == SessionsPerOwnerUnlimited` to the default and
confirm the config test fails.
