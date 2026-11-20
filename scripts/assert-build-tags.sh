#!/bin/sh
# assert-build-tags.sh: the declared build-tag set must select code (ci-template
# v1.9.2; a script from v1.21, run in the toolchain image). Ships beside ci.yml;
# copy it to scripts/ with the workflow.
#
#   GO_BUILD_TAGS='desktop fido2' sh scripts/assert-build-tags.sh
#
# A misspelled tag is invisible: the analysers all run, report clean, and skip
# the code the tag was meant to reach. This costs a few `go list` calls and
# turns that silent miss into a red.
#
# Compares FILES, not packages: a tag very often adds a file to a package that
# already exists, so the package count does not move. Test files count too: a
# tag applied only to _test.go files is a valid GO_BUILD_TAGS. Each tag is then
# checked by REMOVAL, not in isolation: a tag in a conjunction
# (`//go:build desktop && fido2`) selects nothing on its own, and a total-count
# test lets one real tag mask a misspelled sibling. The property is that each
# tag CHANGES the selected set, not that it adds to it: the standard pattern
# pairs a `//go:build !foo` stub with a `//go:build foo` body.
set -eu

[ -n "${GO_BUILD_TAGS:-}" ] || { echo "GO_BUILD_TAGS is empty: nothing to assert"; exit 0; }

# shellcheck disable=SC2016 # a Go template, not a shell expansion
list_files() {
	go list ${1:+-tags "$1"} -f '{{$p := .ImportPath}}{{range .GoFiles}}{{$p}}/{{.}}
{{end}}{{range .CgoFiles}}{{$p}}/{{.}}
{{end}}{{range .TestGoFiles}}{{$p}}/{{.}}
{{end}}{{range .XTestGoFiles}}{{$p}}/{{.}}
{{end}}' ./... 2>/dev/null | sort -u
}

bare=$(list_files "")
full=$(list_files "$GO_BUILD_TAGS")
nb=$(printf '%s\n' "$bare" | grep -c . || true)
nf=$(printf '%s\n' "$full" | grep -c . || true)
echo "files selected: untagged=$nb tagged=$nf (tags: $GO_BUILD_TAGS)"
rc=0
if [ "$full" = "$bare" ]; then
	echo "::error::-tags '$GO_BUILD_TAGS' changed nothing: the same files are selected with and without it. Either it names tags no file declares, or GO_BUILD_TAGS should be empty."
	rc=1
fi
for t in $GO_BUILD_TAGS; do
	# shellcheck disable=SC2086 # word-split on purpose: one tag per line
	rest=$(printf '%s\n' $GO_BUILD_TAGS | grep -vx -- "$t" | tr '\n' ' ')
	if [ "$(list_files "$rest")" = "$full" ]; then
		echo "::error::build tag '$t' selects nothing: removing it from GO_BUILD_TAGS changes no file. Misspelled, or it should be removed."
		rc=1
	fi
done
[ "$rc" -ne 0 ] || echo "OK: every declared tag selects code"
exit "$rc"
