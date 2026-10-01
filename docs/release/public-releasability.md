# Public releasability

> **Status: REFERENCE** · Scope: what may be published from this repository,
> and the gate that enforces it · Last verified: 2026-10-01

This repository ships publicly: as a Go module and as release archives built
from a tagged commit. Everything tracked here is published except the private
development workflow, which `.gitattributes` excludes (below). This page is the
rule that keeps that true, and the gate that enforces it.

## The rule

No published file names the private development fleet or its owner. Concretely,
four classes:

| Class | What it covers | Use instead |
|---|---|---|
| `lab-address` | An address on the fleet subnet | RFC 5737 documentation addresses — `192.0.2.0/24`, `198.51.100.0/24`, `203.0.113.0/24` — or a name under `.example` |
| `lab-host` | A fleet hostname (`vstation-NN`, `dellemc-NN`, the GPU review box, the spare-capacity build and bench host) | The role, not the machine: `build-host.example`, `user@bench-host.example`, `forge.example`, `bench-01` |
| `owner-path` | A path under the owner's home directory or the lab's NFS mount | A repository-relative path, or `/var/lib`, `/srv`, `/home/agent` |
| `owner-account` | The owner's account name on its own | A role: `user`, `operator`, or the module path |

The fleet's own subnet, hostnames and path prefixes are written down in exactly
one place: the `patterns` list in `test/releasability/scanner_test.go`. Read it
there rather than repeating it here.

Site configuration — which machine the remote targets offload to, which forge a
build host fetches modules from — is supplied at invocation time
(`REMOTE_HOST`, `FORGE_SSH_HOST`, `PERF_HOST`) or lives in the private
infrastructure repository. The Makefile carries no default for any of them.

Two things are explicitly *not* violations:

- **Benchmark provenance.** A throughput figure is a property of the machine
  that measured it, so "measured on `<host>`, 2026-09-14" is the measurement's
  meaning, not a leak. The stored profiles under `test/performance/profiles/`
  and the baselines under `test/performance/baselines/` are named after their
  machines; prose that cites them waives the line.
- **RFC 1918 literals that are not the fleet subnet.** Tests across this tree
  use `192.168.1.x` and `10.0.0.x` as opaque addresses that are never dialed.
  The gate does not look at `192.168.0.0/16` at large, only at the fleet's own
  `/24`.

Test files are published and scanned like everything else; they use neutral
fixtures (`bench-01`, `198.51.100.x`, `forge.example`). Only the gate's own
tests plant fleet values, because they are what the rules exist to find, and
each such line carries an inline waiver.

## Development tooling stays private

`CLAUDE.md` and `AGENTS.md` (at any depth), `.claude/` and `.serena/` are the
agent development workflow. They name the private forge and internal
identities, so they stay in the private repository and are never published.

The exclusion list is `.gitattributes`: each entry is `export-ignore`, which
every archive git builds applies — `git archive`, and the source tarball a
forge serves for a tag. `.dockerignore` carries the same four entries, so no
image build context includes them. `test/releasability` enforces both:

- `TestThePublishedTreeCarriesNoDevelopmentTooling` — the published manifest
  (`publishedFiles`: the index minus export-ignored paths) holds no agent file,
  and nothing else is export-ignored.
- `TestGitArchiveHonoursTheExclusionList` — a repository carrying this tree's
  `.gitattributes`, archived as a forge would, contains none of them.
- `TestTheExclusionListsNameTheDevelopmentTooling` — `.gitattributes` and
  `.dockerignore` both carry all four entries.

A new agent tool's files go in `isDevelopmentTooling`
(`test/releasability/manifest_test.go`), `.gitattributes` and `.dockerignore`
together.

**Not covered: the module zip and a history push.** The Go module zip
(`proxy.golang.org`, `go mod download`) is built from the repository at the
tag and does not honour `export-ignore`; neither does pushing this
repository's history to a public remote, where every commit carries the agent
files. For the exclusion to hold there, the public repository must be produced
as an export — `git archive` of the release commit, committed fresh — and not
as a push of this history.

## The gate

`test/releasability` scans every published file and reports each line carrying
a value in one of the four classes. It runs as
`TestTheTrackedTreeIsPubliclyReleasable`, and `make test` (`go test ./...`)
includes the package, so there is no target to remember. Every file in the
package is a `_test.go` file: the gate adds nothing to the module's importable
API. Resolution is textual, so it covers Markdown, YAML, shell, Makefiles and
Go alike; binary files are skipped.

```
go test ./test/releasability/       # the gate, well under a second
```

A failure names the file, the line, the class, the matched text, and what to
use instead.

### Accepting a line

Preferred — an **inline waiver**, in the file's own comment syntax, on the
offending line or the line immediately above it:

```markdown
Two profiles are tracked long-term: `<host>` (single-socket, <!-- lab-string-ok: benchmark provenance -->
```

```go
expr: regexp.MustCompile(`...`), // lab-string-ok: the hostname this rule exists to find
```

The reason is what makes it a waiver; `lab-string-ok:` with nothing after it
(or only a comment closer) is not one. The justification belongs next to the
value it explains.

Second — an entry in `allowList` (`test/releasability/allowlist_test.go`), for
a whole tree, a format with no comment syntax, or a file another repository
owns. Each entry names the classes it accepts and why; an entry with no reason,
no classes or a malformed path fails the gate rather than widening it, no entry
may accept every class, and an entry that matches no published file fails too.
The list is meant to shrink. Today it holds:

| Path | Classes | Why |
|---|---|---|
| `test/performance/profiles/**` | `lab-host` | Benchmark provenance: each profile is named after its machine |
| `test/performance/baselines/**` | `lab-host` | Benchmark provenance: the file name and run-host/build-host header |
| `test/performance/perfprofile/generate_test.go` | `lab-host` | Drives the generator with a real stored profile |
| `CHANGELOG.md` | `lab-host` | Historical release notes recording where measurements ran |
| `.github/workflows/opencode-review.yml` | `lab-address`, `lab-host` | Company CI template copy; neutralised in the template, not here |
| `.github/workflows/claude-review.yml` | `lab-host` | Same |
| `.github/dependabot.yml` | `lab-address` | Same |

### Adding a fleet value to the rule set

One entry in `patterns`: the regexp, a lowercase `needle` literal every match
must contain (the scanner searches for needles first, which is what keeps a
whole-tree scan cheap), and the guidance a tripping engineer reads. Add a sample
to `TestPatternsEveryNeedleIsImpliedByItsExpr`, which is what stops a needle
from silently blinding its own pattern.

## What #213 removed

Lab values in shipping files: the Makefile's remote-offload target defaulted to
a fleet address and the owner's account and seeded a known-hosts entry for the
private forge — it now requires `REMOTE_HOST` (and `FORGE_SSH_HOST` for the
forge) with no default; the benchmark baselines' `run-host` and `command`
headers carried the same address and account; the `perfcompare` tests and
`test/performance/README.md` spelled out the private forge in a module-rewrite
example (now `198.51.100.50` and `forge.example`); and copy-pasteable examples
in the Makefile, README and testing docs named a fleet machine as the profile
(now `PERF_PROFILE=<profile>`).

The same scheme is the company model: go-quicbaas carries the original
(`pkg/labstrings`), and this repository's gate mirrors its classes, patterns,
waiver syntax and allow-list rules. go-quicraft cannot import it — go-quicbaas
depends on this module — so the gate is reimplemented here at this
repository's size.
