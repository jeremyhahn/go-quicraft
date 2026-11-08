# Public releasability

> **Status: REFERENCE** · Scope: what may be published from this repository,
> and the gate that enforces it · Last verified: 2026-10-20

This repository ships publicly: as a Go module and as release archives built
from a tagged commit. Everything tracked here is published, `.serena/` and
the CI template's workflows included, except the private forge's own workflows
and the private release trust anchor, which `.gitattributes` excludes (below). This page states the rule and the gate that
enforces it.

## The rule

No published file names the private development fleet, its owner, its
internal identities or its internal repositories. Concretely, six classes:

| Class | What it covers | Use instead |
|---|---|---|
| `lab-address` | An address on the fleet subnet | An RFC 5737 documentation address from `192.0.2.0/24` or `203.0.113.0/24`, or a name under `.example` |
| `lab-host` | A fleet build or bench host name | The role, not the machine: `build-host.example`, `user@bench-host.example`, `forge.example`, `bench-01` |
| `owner-path` | A path under the lab's NFS mount | A repository-relative path, or `/var/lib`, `/srv`, or the agent account's home directory |
| `owner-account` | The owner's account name, in a home path or on its own | A role: `user`, `operator`, or the module path |
| `internal-identity` | An address in the internal mail domain, or an agent principal of the form `agent-<name>@...` on any domain | A neutral example identity: `release@example.invalid`, `ops@forge.example` |
| `internal-repository` | A private-forge repository that is not this project (the company repository, the agent tooling repository) or an issue number on the handbook's tracker | The decision itself, stated in place, or a public document |

The `patterns` list in `test/releasability/scanner_test.go` names the
placeholder vocabulary the published tree uses for the fleet's subnet,
hostnames, path prefixes and internal mail domain, so a placeholder copied into
new prose or a new fixture is caught like the value it stands for. The values a
private scan of an unpublished tree needs live outside this repository.

Site configuration (which machine the remote targets offload to, which forge a
build host fetches modules from) is supplied at invocation time
(`REMOTE_HOST`, `FORGE_SSH_HOST`, `PERF_HOST`) or lives in the private
infrastructure repository. The Makefile carries no default for any of them.

Two things are explicitly *not* violations:

- **Benchmark provenance.** A throughput figure is a property of the machine
  that measured it, so "measured on `<host>`, 2026-09-14" is the measurement's
  meaning, not a leak. The stored profiles under `test/performance/profiles/`
  and the baselines under `test/performance/baselines/` are named after their
  machines; prose that cites them waives the line.
- **RFC 1918 and documentation-range literals that are not the placeholder
  subnet.** Tests across this tree use `192.168.1.x`, `10.0.0.x` and
  `192.0.2.x` as opaque addresses that are never dialed. The gate does not look
  at those ranges at large, only at the placeholder subnet's own `/24`.

Test files are published and scanned like everything else; they use neutral
fixtures (`bench-01`, `192.0.2.x`, `forge.example`). Only the gate's own
tests plant placeholder values, because they are what the rules exist to find,
and each such line carries an inline waiver.

## What is published

Every tracked file is published except four:

| Path | What it is |
|---|---|
| `.github/workflows/automerge.yml` | The private forge's auto-merge driver: part of its SDLC, not the CI template |
| `.github/workflows/revert-red-main.yml` | The private forge's red-main revert driver: part of its SDLC, not the CI template |
| `.github/workflows/code-review.yml` | The private forge's pull-request review on its own runners (company template, verbatim): part of its SDLC, not the CI template |
| `.release-allowed-signers` | The private repository's release trust anchor: the internal principal and its public key |

Nothing a public reader runs calls or reads the three workflows. `make
release-alpha` reads the trust anchor from the checkout it runs in by default; a
maintainer releasing from an export names their own allowed-signers file with
`RELEASE_ALLOWED_SIGNERS` (Releasing, in `CONTRIBUTING.md`). The four are
`privateWorkflowFiles` in `test/releasability/manifest_test.go`, exact paths.

`CLAUDE.md`, `AGENTS.md` and `.claude/` are no longer tracked. `.serena/` and
every other workflow under `.github/workflows` are published as they are and
scanned like everything else.
So they are neutralised the same way as the rest of the tree: no internal host,
repository number or domain. `.github/dependabot.yml` is published too: it is a
github.com service file, inert on the private forge.

The exclusion list is `.gitattributes`. Each entry is `export-ignore`, which
every archive git builds applies: `git archive`, and the source tarball a forge
serves for a tag. `.dockerignore` is a separate list: it keeps the agent files
and the private forge's workflows out of every image build context as well,
because an image runs the code and carries no development tooling.
`test/releasability` enforces both:

- `TestThePublishedTreeIsTheTrackedTreeLessThePrivateWorkflow`: the published
  manifest (`publishedFiles`, the index minus export-ignored paths) holds no
  `privateWorkflowFiles` entry, holds `.serena/project.yml` and the CI
  template's workflows, and nothing else is export-ignored.
- `TestGitArchiveHonoursTheExclusionList`: a repository carrying this tree's
  `.gitattributes`, archived as a forge would, leaves out the private forge's
  workflows and the trust anchor and keeps the agent files and every other
  workflow.
- `TestTheExclusionListsCarryTheirEntries`: `.gitattributes` and
  `.dockerignore` both carry every entry.

A new private-release file goes in `privateWorkflowFiles` and in
`.gitattributes` and `.dockerignore` together.

The release recipe names no identity of its own. `test/buildgates` drives
`make release-alpha` end to end with plain git, throwaway ed25519 keys and a
fabricated allowed-signers file, so those gates run in an export too. The
remote is a local bare repository, which the recipe reads back with plain git
after the push, so the gates that substitute a misbehaving `RELEASE_GIT` at the
push need no network either. One gate
reads a private-forge file: `TestReleaseAllowedSignersIsWellFormed` checks that
every entry in `.release-allowed-signers` names a principal and a key
`ssh-keygen` parses. It goes through `requirePrivateWorkflowFile`, which skips
when the file is absent *and* `.gitattributes` export-ignores it exactly (a
published export, where there is no private machinery to check), and fails
when it is absent for any other reason.
`TestPrivateWorkflowFilesAreExportIgnored` pins that the file is
export-ignored, so an export skips the gate instead of failing it.

**Not covered: the module zip and a history push.** The Go module zip
(`proxy.golang.org`, `go mod download`) is built from the repository at the
tag and does not honour `export-ignore`. Neither does pushing this repository's
history to a public remote, where every commit carries the internal signer
principal and every earlier commit the agent files and workflows as they were
before they were neutralised. For the exclusion to
hold there, the public repository must be produced as an export (`git archive`
of the release commit, committed fresh), not as a push of this history.

## The gate

`test/releasability` scans every published file and reports each line carrying
a value in one of the six classes. It runs as
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

Preferred: an inline waiver, in the file's own comment syntax, on the
offending line or the line immediately above it:

```markdown
Two profiles are tracked long-term: `<host>` (single-socket, <!-- lab-string-ok: benchmark provenance -->
```

```go
expr: regexp.MustCompile(`...`), // lab-string-ok: the hostname this rule exists to find
```

The reason is what makes it a waiver: `lab-string-ok:` with nothing after it,
or only a comment closer, is not one.

Second, an entry in `allowList` (`test/releasability/allowlist_test.go`), for
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
| `docs/references/**` | `lab-address` | Verbatim IETF text |

### Adding a placeholder to the rule set

One entry in `patterns` (a real fleet value never goes here): the regexp, a lowercase `needle` literal every match
must contain (the scanner searches for needles first, which is what keeps a
whole-tree scan cheap), and the guidance a tripping engineer reads. Add a sample
to `TestPatternsEveryNeedleIsImpliedByItsExpr`, which is what stops a needle
from silently blinding its own pattern.

## Origin

The classes, patterns, waiver syntax and allow-list rules mirror
`pkg/labstrings` in go-quicbaas. go-quicbaas depends on this module, so this
repository cannot import it and reimplements the gate at its own size.
