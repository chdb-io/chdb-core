#!/usr/bin/env bash
#
# Initialise and update all submodules on a CI runner.
#
#   GITHUB_TOKEN=... .github/scripts/update-submodules.sh
#
# Two things go wrong with a bare `git submodule update` on the self-hosted pool:
#
# 1. The clones are anonymous. actions/checkout only configures its token for the
#    superproject, and submodule clones do not inherit that config. Every runner
#    leaves through the same NAT address, and one main-push + PR fan-out (a dozen
#    jobs x 4 parallel clones of ~140 contrib repositories) is enough for GitHub
#    to start answering unauthenticated requests with HTTP 401:
#
#      fatal: could not read Username for 'https://github.com': No such device or address
#      fatal: expected flush after ref listing
#
#    When GITHUB_TOKEN is set it is sent as the same header actions/checkout uses.
#    GIT_CONFIG_* is inherited by the clone subprocesses and never written to disk.
#    Never combine it with the credential actions/checkout persists in the
#    superproject: git then sends two Authorization headers and GitHub answers 400.
#    Submodule clones are fresh repositories, so they only ever see this one.
#
# 2. Runners are reused. A job cancelled (or killed) in the middle of a checkout
#    leaves lock files and half-cloned repositories under .git/modules, which
#    actions/checkout does not clean up (it only resets the superproject):
#
#      fatal: Unable to create '.../.git/modules/contrib/llvm-project/index.lock': File exists.
#      fatal: Unable to find current revision in submodule path 'contrib/google-cloud-cpp'
#
#    Every update runs with --force. Submodules are cloned with --no-checkout and
#    populated afterwards; when an update is aborted in between (one clone failing
#    twice aborts before any checkout), the cloned submodules are left with HEAD
#    at the remote tip and an empty working tree. 24 of the 139 submodules are
#    pinned at that tip, so a plain update considers them up to date and the
#    build fails much later ("Cannot find source file: contrib/librdkafka/src/
#    cJSON.c"). --force runs the checkout regardless, as actions/checkout does.
#
#    If the update still fails, the workspace is known to be damaged and no other
#    git process is running: stale locks are removed, submodule clones without a
#    usable repository are deleted, and the update is retried once.
#
# SUBMODULE_DEPTH=N makes the clones shallow (the wasm job used actions/checkout's
# default of 1 before it moved to this script).

set -euo pipefail

if [ -n "${GITHUB_TOKEN:-}" ]; then
    export GIT_CONFIG_COUNT=1
    export GIT_CONFIG_KEY_0="http.https://github.com/.extraheader"
    GIT_CONFIG_VALUE_0="AUTHORIZATION: basic $(printf 'x-access-token:%s' "$GITHUB_TOKEN" | base64 | tr -d '\n')"
    export GIT_CONFIG_VALUE_0
fi

remove_stale_locks() {
    [ -d .git/modules ] || return 0
    find .git/modules -type f -name '*.lock' -print -delete | sed 's/^/Removed stale lock: /'
}

# Delete submodule clones whose repository is unusable: a gitdir without a
# resolvable HEAD (interrupted clone) or a working tree whose gitdir is gone.
remove_broken_clones() {
    [ -f .gitmodules ] || return 0
    git config --file .gitmodules --get-regexp '^submodule\..*\.path$' |
    while read -r key path; do
        name=${key#submodule.}
        name=${name%.path}
        gitdir=".git/modules/$name"
        if [ -d "$gitdir" ]; then
            git --git-dir="$gitdir" rev-parse --verify --quiet HEAD >/dev/null 2>&1 && continue
        elif [ ! -e "$path/.git" ]; then
            continue
        fi
        echo "Removing broken submodule clone: $path"
        rm -rf "$path" "$gitdir"
    done
}

depth_arg=${SUBMODULE_DEPTH:+--depth=$SUBMODULE_DEPTH}

update() {
    # shellcheck disable=SC2086  # depth_arg is a single token or empty
    git submodule update --init --recursive --force --jobs 4 $depth_arg
}

if ! update; then
    echo "::warning::git submodule update failed, cleaning up stale submodule state and retrying once"
    remove_stale_locks
    remove_broken_clones
    update
fi
