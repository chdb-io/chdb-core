#!/usr/bin/env bash
#
# Initialise and update all submodules on a CI runner.
#
#   GITHUB_TOKEN=... .github/scripts/update-submodules.sh
#
# A bare `git submodule update` clones the ~140 contrib repositories anonymously:
# actions/checkout only configures its token for the superproject, and submodule
# clones do not inherit that config. Every self-hosted runner leaves through the
# same NAT address, and one main-push + PR fan-out (a dozen jobs x 4 parallel
# clones) is enough for GitHub to start answering unauthenticated requests with
# HTTP 401:
#
#   fatal: could not read Username for 'https://github.com': No such device or address
#   fatal: expected flush after ref listing
#
# When GITHUB_TOKEN is set it is sent as the same header actions/checkout uses.
# GIT_CONFIG_* is inherited by the clone subprocesses and never written to disk.

set -euo pipefail

if [ -n "${GITHUB_TOKEN:-}" ]; then
    export GIT_CONFIG_COUNT=1
    export GIT_CONFIG_KEY_0="http.https://github.com/.extraheader"
    GIT_CONFIG_VALUE_0="AUTHORIZATION: basic $(printf 'x-access-token:%s' "$GITHUB_TOKEN" | base64 | tr -d '\n')"
    export GIT_CONFIG_VALUE_0
fi

git submodule update --init --recursive --jobs 4
