You are resolving the merge conflicts left by the daily ClickHouse master sync in
this chdb-core checkout.

Follow `.agents/skills/resolve-sync-conflicts/SKILL.md` exactly; it is the whole
procedure. The conflict list, the upstream range and the upstream pull requests
that touched each file are in `.sync/summary.json`. For every conflicted file,
the upstream version before and after this range is in
`.sync/upstream/base/<path>` and `.sync/upstream/target/<path>`, and the upstream
merges that touched it are listed in `.sync/upstream/log/<path>.txt`.

When every conflict is resolved, run
`python3 .github/nightly/checks.py markers` and
`python3 .github/nightly/checks.py delta --work @SYNC_WORK@`,
fix anything they report, and write `.sync/resolution.md` as the skill describes.
Do not stage or commit; the workflow does that.
