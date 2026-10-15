#!/usr/bin/env bash
# The Ubuntu 24.04 runner's Git is new enough for checkout and submodules.
# Avoid making every build depend on Launchpad being available just to update it.
set -euo pipefail

MIN_GIT_VERSION=2.43

git_version=$(git --version 2>/dev/null | awk '{print $3}' || true)
if [[ -n "$git_version" ]] && dpkg --compare-versions "$git_version" ge "$MIN_GIT_VERSION"; then
    echo "Using runner Git $git_version (minimum $MIN_GIT_VERSION); skipping PPA"
    exit 0
fi

echo "Runner Git ${git_version:-missing} is below $MIN_GIT_VERSION; installing Git"
retry_infrastructure() {
    local label=$1 attempt
    shift
    for attempt in 1 2 3; do
        if "$@"; then
            return 0
        fi
        echo "::warning title=CI infrastructure::$label failed (attempt $attempt/3)"
        if (( attempt < 3 )); then
            sleep "$((attempt * 10))"
        fi
    done
    echo "::error title=CI infrastructure::$label failed after 3 attempts"
    return 75
}

retry_infrastructure 'Add Git PPA' sudo add-apt-repository ppa:git-core/ppa -y
retry_infrastructure 'Update apt indexes' sudo apt-get -o DPkg::Lock::Timeout=600 update
retry_infrastructure 'Install Git' sudo apt-get -o DPkg::Lock::Timeout=600 install -y git

git_version=$(git --version | awk '{print $3}')
echo "Installed Git $git_version"
if ! dpkg --compare-versions "$git_version" ge "$MIN_GIT_VERSION"; then
    echo "::error title=CI infrastructure::Git $git_version is below required $MIN_GIT_VERSION"
    exit 75
fi
