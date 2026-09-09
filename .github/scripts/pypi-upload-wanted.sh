#!/usr/bin/env bash
#
# Decide whether a tag's wheels belong on PyPI.
#
#   .github/scripts/pypi-upload-wanted.sh v26.7.3
#
# Exit 0 to upload, 1 to skip, and say which and why either way -- the reason a
# release is not on PyPI has to be readable in the log of the job that decided
# it, months later, without anyone reconstructing what the settings were.
#
# Four rules, in this order:
#
#   a candidate never goes        -rc.N is for the bindings to pin against, and
#                                 PyPI keeps every filename forever
#
#   nothing but vX.Y.Z goes       an allow-list of the one release shape. The
#                                 condition this replaced tested the tag for
#                                 the substring "rc", which let v26.7.3-rc1
#                                 through and said nothing about -beta.1,
#                                 -stable or a fourth field. check-release-tag.sh
#                                 rejects all of those but cannot refuse a push:
#                                 it goes red minutes into a build that then
#                                 uploads anyway
#
#   a release whose notes say     the opt-out, because the wheels are about half
#   [no-pypi] does not go         a gigabyte against a 10 GB quota that only a
#                                 deleted release gives back. A release cut for
#                                 one binding to move onto, rather than for
#                                 users to install, can say so
#
#   anything else goes            the default, because a release is what a user
#                                 reaches with `pip install chdb-core`
#
# The opt-out lives in the release notes rather than in a repository variable
# for three reasons. It is decided in the same text box, at the same moment, as
# the release itself, so there is no "update the setting before you tag" order
# to get wrong. It does not accumulate -- there is no list to prune, and no
# entry that outlives its release. And it answers "why is this release not on
# PyPI" on the page someone asking that is already looking at.
#
# Either way the GitHub release is unaffected: wheels, libchdb.so, libchdb.a and
# debug symbols are uploaded as release assets by steps this does not gate, so
# every release is installable from the wheel index built over those assets.

set -euo pipefail

MARKER='[no-pypi]'

TAG="${1:-}"
[ -n "$TAG" ] || {
	echo "usage: $0 <tag>" >&2
	exit 2
}

case "$TAG" in
*-rc.*)
	echo "$TAG is a release candidate; PyPI upload skipped"
	exit 1
	;;
esac

# No leading zeros, three fields, nothing after them: the same shape
# check-release-tag.sh accepts for a release, minus its -rc.N alternative,
# which the case above has already taken.
num='(0|[1-9][0-9]*)'
if [[ ! $TAG =~ ^v$num\.$num\.$num$ ]]; then
	echo "$TAG is not a plain vX.Y.Z release; PyPI upload skipped"
	echo "  only that shape publishes, so a mistyped or pre-release tag cannot spend the quota"
	exit 1
fi

# Two digits each, the bound check-release-tag.sh enforces because chdb-go packs
# the release into major*10000 + minor*100 + patch. A tag past it is one the
# bindings cannot publish, and that check goes red without being able to stop
# the upload -- so a filename would be spent on a release nothing downstream
# could use.
if [ "${BASH_REMATCH[2]}" -gt 99 ] || [ "${BASH_REMATCH[3]}" -gt 99 ]; then
	echo "$TAG has a field above 99, which the bindings cannot encode; PyPI upload skipped"
	echo "  see .github/scripts/check-release-tag.sh for why two digits is the budget"
	exit 1
fi

repo="${GITHUB_REPOSITORY:-chdb-io/chdb-core}"
# Read by tag rather than from the event payload, so this behaves the same
# however the build was triggered.
if ! notes=$(gh api "repos/$repo/releases/tags/$TAG" --jq '.body // ""' 2>&1); then
	# Fail closed. The costs are not symmetric: a quota spent is permanent and
	# takes the filename with it, while a skip is recoverable -- fix the notes
	# and re-run the build, or upload by hand. A tag with no release cannot have
	# had wheels attached either, so this is already a broken state.
	echo "could not read the release notes for $TAG; PyPI upload skipped" >&2
	echo "  $notes" >&2
	echo "  re-run this build once the release exists, or upload by hand" >&2
	exit 1
fi

if [[ $notes == *"$MARKER"* ]]; then
	echo "$TAG's release notes say $MARKER; PyPI upload skipped"
	echo "  the wheels are on the GitHub release and in the wheel index"
	exit 1
fi

echo "$TAG is a release and its notes do not say $MARKER; uploading to PyPI"
