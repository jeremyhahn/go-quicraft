# 744: ValidateFrameHeaderWindow two-member window

`ValidateFrameHeaderWindow` (pkg/internal/transport/frame.go) checks a frame
header against an explicit `WireWindow`. No test named it before this change.
The only existing test used `DefaultWireWindow`, and both members of that
window are the same byte today. So a gate that checked only `FrameCurrent`,
or only `FramePrevious`, would have passed every test.
`pkg/internal/transport/frame_window_test.go` pins it.

**Two-member sweep.** All 256 version bytes go through `nextBuildWindow()`.
Exactly `WireVersion` and `WireVersion+1` must be accepted. Every other byte
must return `ErrUnsupportedVersion`, and the test counts the accepted bytes.

**Non-adjacent sweep.** The same sweep runs against a window whose two members
are 0x7d and 0x13. Exactly those two bytes must be accepted. This shows the
gate is a closed set, not a range between the two members.

**Order of checks.** A header with bad magic and an out-of-window version
returns `ErrInvalidMagic`. Every buffer shorter than `FrameHeaderSize` returns
`ErrBufferTooSmall`. So the buffer and magic checks run before the version gate.

**Size check.** For both in-window versions, length `MaxFrameSize` is accepted
and `MaxFrameSize+1` returns `ErrFrameTooLarge`.

**Reader path.** `readFrameHeaderWindow` reads a `WireVersion+1` header from a
`bytes.Reader`. Under `nextBuildWindow()` it returns the header's length and
flags. Under `DefaultWireWindow()` it returns `ErrUnsupportedVersion`.

**Mutation check (not committed).** With `AcceptsFrame` changed to check only
`v == w.FrameCurrent`, the two sweeps and the size subtest fail.
