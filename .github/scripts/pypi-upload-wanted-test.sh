#!/usr/bin/env bash
#
# Table test for pypi-upload-wanted.sh.
#
#   .github/scripts/pypi-upload-wanted-test.sh
#
# The decision it tests is one a mistake in cannot be undone: PyPI keeps a
# filename forever and the project quota only comes back by deleting a release.
# It is also never exercised by a pull request -- the upload step it gates runs
# on tags only -- so without this, an edit to those rules first runs during the
# release it is wrong about.
#
# The release-notes lookup is faked by putting a `gh` earlier on PATH, so the
# real code path runs, including the shape checks that decide whether the
# lookup happens at all.

set -uo pipefail

cd "$(dirname "$0")"
SCRIPT=./pypi-upload-wanted.sh
[ -x "$SCRIPT" ] || {
	echo "not executable: $SCRIPT" >&2
	exit 2
}

FAKE_BIN=$(mktemp -d)
trap 'rm -rf "$FAKE_BIN"' EXIT
cat >"$FAKE_BIN/gh" <<'SHIM'
#!/usr/bin/env bash
# Stands in for `gh api .../releases/tags/<tag>`: prints FAKE_NOTES, or fails
# when FAKE_GH_FAILS is set, so the fail-closed path is reachable offline.
if [ -n "${FAKE_GH_FAILS:-}" ]; then
	echo "release not found" >&2
	exit 1
fi
printf '%s' "${FAKE_NOTES:-}"
SHIM
chmod +x "$FAKE_BIN/gh"
export PATH="$FAKE_BIN:$PATH"

failures=0

# want: "upload" or "skip"; notes: the release body the fake gh returns
check() {
	local want=$1 tag=$2 notes=${3:-} fails=${4:-}
	local got output
	output=$(FAKE_NOTES="$notes" FAKE_GH_FAILS="$fails" "$SCRIPT" "$tag" 2>&1) \
		&& got=upload || got=skip
	if [ "$got" = "$want" ]; then
		printf '  ok    %-22s -> %s\n' "$tag" "$got"
	else
		printf '  FAIL  %-22s -> %s, wanted %s\n     %s\n' "$tag" "$got" "$want" "${output%%$'\n'*}"
		failures=$((failures + 1))
	fi
}

echo "a release publishes unless its notes opt out"
check upload v26.7.3 "## chdb-core v26.7.3"
check upload v26.7.30 "notes"
check skip   v26.7.3 "## chdb-core v26.7.3

[no-pypi] wheels are attached to this release."
# The marker anywhere in the body counts: release notes are prose and nobody
# will keep it on a line of its own.
check skip   v26.7.3 "Not on PyPI this time [no-pypi] -- use the wheel index."

echo "a candidate never publishes"
check skip v26.7.1-rc.1
check skip v26.7.3-rc.10

echo "nothing but vX.Y.Z publishes"
for tag in v26.7.3-rc1 v26.7.3rc1 v26.7.3-beta.1 v26.7.3.59-stable v26.07.3 26.7.3 v26.7 v26.7.3.1; do
	check skip "$tag" "notes"
done

echo "two digits per field, the bound the bindings can encode"
check upload v26.7.99 "notes"
check skip   v26.7.100 "notes"
check skip   v26.100.3 "notes"

echo "an unreadable release does not publish"
check skip v26.7.3 "" fails

echo "no tag is a usage error, not a decision"
if "$SCRIPT" >/dev/null 2>&1; then
	echo "  FAIL  no argument -> exit 0"
	failures=$((failures + 1))
else
	[ $? -eq 2 ] && echo "  ok    no argument -> exit 2" || {
		echo "  FAIL  no argument -> exited 1, which reads as a decision to skip"
		failures=$((failures + 1))
	}
fi

if [ "$failures" -gt 0 ]; then
	echo "::error::$failures case(s) failed"
	exit 1
fi
echo "all cases pass"
