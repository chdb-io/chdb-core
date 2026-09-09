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
# Two rules, in this order:
#
#   a candidate never goes           -rc.N is for the bindings to pin against,
#                                    and PyPI keeps every filename forever
#
#   a release goes unless named      the default, because a release is what a
#   in CORE_PYPI_SKIP_TAGS           user reaches with `pip install chdb-core`
#
# The skip list exists because four abi3 wheels come to roughly half a gigabyte
# against a 10 GB project quota, and deleting a release is the only way to get
# any of it back -- and it burns those filenames permanently. So a release cut
# for one binding to move onto, rather than for users to install, can say so.
#
# It is a list of exact tags rather than a boolean for one reason: an entry that
# outlives its release matches nothing. A boolean left set to "skip" silently
# keeps the next release off PyPI too.
#
# Either way the GitHub release is unaffected: wheels, libchdb.so, libchdb.a and
# debug symbols are uploaded as release assets by steps this does not gate, so
# `pip install <release asset URL>` works for every tag.

set -euo pipefail

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

# Commas around both sides so an entry matches one whole tag: bare substring
# matching would let v26.7.3 in the list also skip v26.7.30.
case ",${CORE_PYPI_SKIP_TAGS:-}," in
*",$TAG,"*)
	echo "$TAG is named in CORE_PYPI_SKIP_TAGS; PyPI upload skipped"
	echo "  the wheels are still on the GitHub release for $TAG"
	exit 1
	;;
esac

echo "$TAG is a release and is not in CORE_PYPI_SKIP_TAGS; uploading to PyPI"
