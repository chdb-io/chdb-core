#!/usr/bin/env bash
# Commit the staged sync, push it to $SYNC_BRANCH and open the pull request.
#
# Runs with the personal access token in SYNC_PAT: a pull request opened with the
# workflow's own GITHUB_TOKEN would not start any CI. AUTO_MERGE=true asks GitHub
# to merge once the required checks pass.
set -euo pipefail

: "${SYNC_PAT:?the SYNC_PAT secret is not set}"
: "${SYNC_BRANCH:?}"
export GH_TOKEN="$SYNC_PAT"

account=$(gh api user --jq '.login + " " + (.id | tostring)')
login=${account% *}
id=${account#* }
export SYNC_GIT_NAME="$login"
export SYNC_GIT_EMAIL="${id}+${login}@users.noreply.github.com"
python3 .github/nightly/daily_sync.py commit

# The workflow only gets here when no sync pull request is open, so nobody's
# review is on the branch and it can be replaced.
git push --force --quiet \
  "https://x-access-token:${SYNC_PAT}@github.com/${GITHUB_REPOSITORY}.git" \
  "HEAD:refs/heads/${SYNC_BRANCH}"

url=$(gh pr create --repo "$GITHUB_REPOSITORY" --base main --head "$SYNC_BRANCH" \
  --title "$(cat .sync/pr_title.txt)" --body-file .sync/pr_body.md)
echo "::notice::Opened $url"

if [ "${AUTO_MERGE:-false}" = true ]; then
  gh pr merge "$url" --repo "$GITHUB_REPOSITORY" --auto --squash ||
    echo "::warning::Auto-merge is not available; the pull request waits for a person."
fi
