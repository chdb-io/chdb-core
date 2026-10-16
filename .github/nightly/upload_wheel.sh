#!/usr/bin/env bash
# Upload a merged sync's wheel to the rolling `nightly` pre-release, creating
# the release the first time. publish.py moves its tag, prunes it and writes the
# pip index once all four platforms are done.
#
# Usage: upload_wheel.sh <wheel>...   (GH_TOKEN or GITHUB_TOKEN may write releases)

set -euo pipefail

tag=nightly
if ! gh release view "$tag" >/dev/null 2>&1; then
  # Another platform may create it at the same moment; the upload below settles it.
  gh release create "$tag" --prerelease --latest=false --title "Nightly builds" \
    --notes "Rolling nightly builds of chdb-core from \`main\`." --target "$GITHUB_SHA" || true
fi
gh release upload "$tag" "$@" --clobber
