# Compatibility and upgrades: CLI

> **Status: NORMATIVE** · Scope: go-quicraft, all persisted and wire formats ·
> Last verified: 2026-10-11
> Conflicts with this document are defects. Fix them here first.

No CLI: go-quicraft is a library and ships no binaries (there is no `cmd/`
directory), and nothing in it migrates or inspects a data directory, so there is
no upgrade verb to document. The upgrade procedure is in [user.md](user.md).
