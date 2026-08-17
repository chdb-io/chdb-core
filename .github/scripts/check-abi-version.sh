#!/usr/bin/env bash
#
# Check that CHDB_VERSION in the public header is the release being tagged.
#
#   .github/scripts/check-abi-version.sh v26.7.0
#
# chdb_version() returns that macro and nothing else, so it is the only thing the C
# ABI says about itself and there is nothing to cross-check it against. It sat at
# 26.5.1-rc.3 through v26.7.0, two ClickHouse lines behind the engine around it.
#
# Separate from check-release-tag.sh because the two fail for unrelated reasons: a
# red check should say which.

set -euo pipefail

TAG="${1:?usage: $0 <tag>}"
HEADER=programs/local/chdb.h

[ -f "$HEADER" ] || {
	echo "::error::$HEADER is missing, so there is nothing to check the tag against" >&2
	exit 1
}

declared=$(sed -n 's/^#define CHDB_VERSION "\(.*\)"$/\1/p' "$HEADER" | head -1)
[ -n "$declared" ] || {
	echo "::error::$HEADER has no CHDB_VERSION define" >&2
	exit 1
}

want=${TAG#v}

if [ "$declared" != "$want" ]; then
	cat >&2 <<EOF
::error::the version compiled into the library is not the version being released.

  $HEADER says   $declared
  the tag says   $want

chdb_version() returns the first of those, so every caller asking the library what
it is would be told $declared. Bump CHDB_VERSION in $HEADER, then move the tag onto
that commit.
EOF
	exit 1
fi

echo "$HEADER declares $declared, matching tag $TAG"
