#!/usr/bin/env bash
# Verify commit signatures against .github/allowed_signers: the Signatures
# job of ci.yml runs it from the checkout, and test/buildgates runs it against
# fixture repositories.
#
# Inputs (environment):
#   EVENT       the event name (pull_request, push, or anything else)
#   BASE_SHA    the pull request's base commit (pull_request only)
#   HEAD_SHA    the commit checked out (the pull request head, or the push)
#   BEFORE      the branch tip before the push (push only)
#   GITHUB_REF  the pushed ref; a push to refs/heads/main also checks a window
#   REPO, API, GH_TOKEN  read GitHub's verification of web-flow commits
#   RUNNER_TEMP where the registry is copied (default /tmp)
#
# Exits 0 when every commit checked is signed by its committer's registered
# key (or is a verified GitHub web-flow commit), 1 otherwise.
# The workflow's default shell ran the inline step with errexit; kept here.
set -euo pipefail
zero() { [ -z "$1" ] || printf '%s' "$1" | grep -qE '^0+$'; }
isrev() { ! zero "$1" && git cat-file -e "$1^{commit}" 2>/dev/null; }
ASF="${RUNNER_TEMP:-/tmp}/allowed_signers"
# The registry is read from the base, so a pull request cannot add the key
# that signs it. Only the first adoption, with no registry at the base,
# reads it from the head.
REG=""
if [ "$EVENT" = pull_request ]; then REG="$BASE_SHA"; elif [ "$EVENT" = push ]; then REG="$BEFORE"; fi
# v1.35.1: a named base that is not in the clone (a push after a force
# push, a base the checkout did not bring) is fetched; if it still cannot
# be read the job fails, never falling back to the head's registry or to
# the head commit alone.
if [ "$EVENT" = pull_request ] && [ -z "$REG" ]; then
  echo "FAIL a pull_request run with no base SHA"; exit 1
fi
if ! zero "$REG" && ! isrev "$REG"; then
  git fetch -q --no-tags origin "$REG" 2>/dev/null || true
  isrev "$REG" || { echo "FAIL the base ${REG:0:12} cannot be read, so neither the registry nor the range can be checked"; exit 1; }
fi
if isrev "$REG" && git cat-file -e "$REG:.github/allowed_signers" 2>/dev/null; then
  git show "$REG:.github/allowed_signers" > "$ASF"
  echo "registry: .github/allowed_signers at ${REG:0:12}"
elif git cat-file -e "$HEAD_SHA:.github/allowed_signers" 2>/dev/null; then
  git show "$HEAD_SHA:.github/allowed_signers" > "$ASF"
  echo "::warning::no registry at the base: .github/allowed_signers read from the head (first adoption only)"
else
  echo "FAIL no .github/allowed_signers in this repository"; exit 1
fi
# The range: base..head on a pull request, before..after on a push to main,
# and the one commit otherwise (a new branch, a schedule or dispatch run).
if [ "$EVENT" = pull_request ] && isrev "$BASE_SHA"; then RANGE="$BASE_SHA..$HEAD_SHA"
elif [ "$EVENT" = push ] && isrev "$BEFORE"; then RANGE="$BEFORE..$HEAD_SHA"
else RANGE="$HEAD_SHA -1"; fi
# shellcheck disable=SC2086 # RANGE is one or two words on purpose
COMMITS=$(git rev-list --reverse $RANGE) || { echo "FAIL cannot list the range $RANGE"; exit 1; }
# LOCAL (security): a push to main also checks the last 50 first-parent
# commits, HEAD~50..HEAD (from the root when main has fewer), so a commit
# whose own run was cancelled or superseded is still checked by a later run.
if [ "$EVENT" = push ] && [ "${GITHUB_REF:-}" = refs/heads/main ]; then
  if isrev "$HEAD_SHA~50"; then WIN="$HEAD_SHA~50..$HEAD_SHA"; else WIN="$HEAD_SHA"; fi
  # shellcheck disable=SC2086 # WIN is a range or one commit
  W=$(git rev-list --reverse $WIN) || { echo "FAIL cannot list the window $WIN"; exit 1; }
  COMMITS=$(printf '%s\n%s\n' "$W" "$COMMITS" | awk 'NF && !seen[$0]++')
  RANGE="$RANGE and $WIN"
fi
N=$(printf '%s\n' "$COMMITS" | grep -c . || true)
echo "checking $N commit(s) in $RANGE"
reg_principals=$(awk '!/^[[:space:]]*(#|$)/ {print $1}' "$ASF")
shown() { # print an email only when it is public: a registry principal or a GitHub noreply
  if printf '%s\n' "$reg_principals" | grep -qxF -- "$1" || printf '%s' "$1" | grep -qE '@users\.noreply\.github\.com$|^noreply@github\.com$'; then
    printf '%s' "$1"; else printf 'an address not in the registry'; fi
}
BAD=0
for c in $COMMITS; do
  ce=$(git log -1 --format=%ce "$c")
  s=$(git log -1 --format=%s "$c" | cut -c1-60)
  if [ "$ce" = noreply@github.com ]; then
    # GitHub made it (a squash merge, an update-branch merge): accept it only
    # when the API says GitHub signed and verified it as web-flow.
    v=$(curl -fsS --retry 3 -H "Authorization: Bearer ${GH_TOKEN}" -H 'Accept: application/vnd.github+json' \
        "${API}/repos/${REPO}/commits/${c}" | jq -r '[(.commit.verification.verified|tostring), .commit.verification.reason, (.committer.login // "none")] | @tsv') \
      || { echo "FAIL ${c:0:12} committed by GitHub, and the API could not be read: $s"; BAD=1; continue; }
    IFS=$'\t' read -r ok reason login <<<"$v"
    if [ "$ok" = true ] && [ "$login" = web-flow ]; then
      echo "OK   ${c:0:12} GitHub web-flow, verified: $s"
    else
      echo "FAIL ${c:0:12} committed as GitHub but not verified (verified=$ok reason=$reason committer=$login): $s"; BAD=1
    fi
    continue
  fi
  # v1.35.1: grep reads the whole object (no -q), so under pipefail a
  # long commit message never ends the pipeline with SIGPIPE.
  if ! git cat-file commit "$c" | grep '^gpgsig' >/dev/null; then
    echo "FAIL ${c:0:12} unsigned, committer $(shown "$ce"): $s"; BAD=1; continue
  fi
  g() { git -c gpg.format=ssh -c gpg.ssh.allowedSignersFile="$ASF" "$@"; }
  if ! g verify-commit "$c" >/dev/null 2>&1; then
    echo "FAIL ${c:0:12} signed by a key not in the registry (or a bad signature), committer $(shown "$ce"): $s"; BAD=1; continue
  fi
  signer=$(g log -1 --format=%GS "$c")
  if [ "$signer" != "$ce" ]; then
    echo "FAIL ${c:0:12} principal mismatch: the key is registered to $signer, the committer is $(shown "$ce"): $s"; BAD=1; continue
  fi
  echo "OK   ${c:0:12} signed by $signer: $s"
done
exit $BAD
