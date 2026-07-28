# Release-note renderer fixtures

This data-only corpus defines rendering contract version 1 for both the checked-in
renderer and the later managed publication renderer. Each directory under `cases/`
contains:

- optional `base/` fragment files present at the release boundary;
- optional `latest-tag` text naming that boundary's stable tag;
- `proposed/` fragment files added through the candidate commit;
- `expected.md`, the exact category-only renderer bytes; and
- optional `release-body.md`, the exact GitHub Release body for the fixed fixture
  version and revisions used by `scripts/test-release-notes`.

When `latest-tag` is absent, the candidate is treated as having no prior stable release,
so every candidate fragment is selected. Fixture directories contain no executable
logic and may be consumed independently by a managed renderer.
