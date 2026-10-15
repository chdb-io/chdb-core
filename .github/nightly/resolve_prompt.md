You are finishing today's ClickHouse master sync in this chdb-core checkout, on a
build machine. The upstream diff from `UPSTREAM_COMMIT` to the target is already
applied: `.sync/summary.json` has the range and the files that conflicted. If
`.sync/build.json` exists, the build or the tests already failed once; it says
where.

Follow `.agents/skills/resolve-sync-conflicts/SKILL.md`. In short: read the guide
to past sync failures it points to, resolve every conflict, and fix what
`python3 .github/nightly/checks.py all` reports. Then run
`bash .github/nightly/build_and_test.sh`, which builds chDB, installs the wheel and
runs the Python tests on Python 3.14 and ends with PASS or FAIL. Fix what the sync
broke and run it again, until it passes or you can make no further progress. Then
write `.sync/resolution.md` for the pull request.

Do not commit or push: the workflow stages your changes and opens the pull request.
