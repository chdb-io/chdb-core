---
name: pr-description
description: Draft or update pull request titles and descriptions for chdb-io/chdb-core. Use when preparing, creating, or editing a PR in this repository.
---

# chDB Core PR Description

Inspect the branch diff against its base and the commits included in the pull
request. Describe only behavior and validation supported by that evidence. Do
not create or edit a remote pull request unless the user explicitly asks.

## Title

- Start with a capitalized imperative verb and state the concrete outcome.
- Be specific enough to distinguish the change without reading the body.
- Do not use Conventional Commit prefixes such as `feat:`, `fix:`, or `chore:`.

## Body

For a non-trivial change, begin with concise context covering the problem,
approach, result, and important tradeoffs. Then include the applicable sections
below.

### Relations

Link related issues and pull requests with full GitHub URLs, one relationship
per line. Include an upstream ClickHouse change when it is relevant. Omit the
section when there are no related items.

### Changelog category

Keep exactly one category:

- New Feature
- Experimental Feature
- Improvement
- Performance Improvement
- Backward Incompatible Change
- Build/Testing/Packaging Improvement
- Documentation (changelog entry is not required)
- Critical Bug Fix
- Bug Fix
- CI Fix or Improvement (changelog entry is not required)
- Not for changelog (changelog entry is not required)

### Changelog entry

When the selected category requires one, describe the user-visible change in a
single accurate paragraph. Omit this section for a category that does not
require an entry.

### Validation and reviewer context

List only commands and checks that were actually run. Add the context relevant
to the change:

- Public ABI changes: affected symbols and ownership or lifetime contracts,
  plus updates to `bindings.md`, tests, and examples.
- Upstream syncs: ClickHouse version or commit and intentional deviations.
- Packaging changes: affected wheels, platforms, and build variants.
- Known limitations, performance impact, or areas that deserve close review.

## Avoid

- Vague titles, internal ticket language, and openings such as “This PR”.
- Descriptions that list implementation details without explaining impact.
- Invented tests, results, issue links, or unsupported claims.
- Boilerplate tables or overly regular prose that makes the change harder to
  review.

AI-assisted wording is acceptable, but the author must review the final text
for accuracy. When the user asks to apply the description, use the repository's
GitHub tooling; otherwise return a draft for review.
