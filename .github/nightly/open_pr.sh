#!/usr/bin/env bash
# Commit the sync that daily_sync.py stage left in .sync/, push it to $SYNC_BRANCH
# and open the pull request.
#
# Runs with the personal access token in SYNC_PAT: a pull request opened with the
# workflow's own GITHUB_TOKEN would not start any CI. A person approves and merges
# it. The patches come from the machine where Claude Code ran shell commands, so
# nothing they could change runs here: the workflow runs a copy of this script
# taken before they apply, and the commits are plain git, their messages data.
set -euo pipefail

: "${SYNC_PAT:?the SYNC_PAT secret is not set}"
: "${SYNC_BRANCH:?}"
export GH_TOKEN="$SYNC_PAT"

account=$(gh api user --jq '.login + " " + (.id | tostring)')
login=${account% *}
id=${account#* }
commit() {
  git -c user.name="$login" -c user.email="${id}+${login}@users.noreply.github.com" \
    commit --quiet --no-verify -F "$1"
}

# Two commits: the upstream diff as the script applied it, then whatever was
# changed on top of it (conflicts resolved, build fixed), so a reviewer can tell
# what was imported from what was decided.
git apply --index --binary .sync/pr-upstream.diff
commit .sync/commit-upstream.txt
if [ -s .sync/pr-changes.diff ]; then
  git apply --index --binary .sync/pr-changes.diff
  commit .sync/commit-changes.txt
fi

# The workflow only gets here when no sync pull request is open, so nobody's
# review is on the branch and it can be replaced. It is deleted first: pushed
# over an older sync, the push would also carry main's workflow changes since
# then, which GitHub accepts only from a token with the Workflows permission.
gh api -X DELETE "repos/${GITHUB_REPOSITORY}/git/refs/heads/${SYNC_BRANCH}" >/dev/null 2>&1 || true
git push --force --quiet \
  "https://x-access-token:${SYNC_PAT}@github.com/${GITHUB_REPOSITORY}.git" \
  "HEAD:refs/heads/${SYNC_BRANCH}"

url=$(gh pr create --repo "$GITHUB_REPOSITORY" --base main --head "$SYNC_BRANCH" \
  --title "$(cat .sync/pr_title.txt)" --body-file .sync/pr_body.md)
echo "::notice::Opened $url"
