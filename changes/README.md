# CLI change fragments

Change fragments record the public release intent for CLI work. Add a fragment in the
category that best describes the primary user impact:

- `added`: users can do something they could not do before;
- `changed`: existing behavior is observably different but broadly compatible;
- `fixed`: a user-visible defect is corrected; describe the symptom, not its internal
  cause;
- `breaking`: users, scripts, or integrations must adapt; include the migration in the
  same sentence; and
- `internal`: the change has no user-visible effect and produces no public release-note
  bullet.

The category is also the release-impact recommendation. `internal`, `fixed`, and
compatible `changed` select a patch; `added` selects the adjacent minor series;
`breaking` selects the adjacent minor before `1.0` and the adjacent major from `1.0`
onward. Planning takes the highest impact across all fragments since the latest stable
tag. Update `release.toml`, the Cargo package fallback, and `Cargo.lock` in the same
candidate whenever that cumulative impact selects a different `MAJOR.MINOR` series; do
not advance an already-selected unreleased minor or major series a second time.

The category directories are optional. Create one only when adding its first fragment;
do not add `.gitkeep` files or other placeholders. `README.md` is the only file allowed
at the root of this directory.

## Create a fragment

From the repository's devenv shell, generate 128 random bits as exactly 32 lowercase
hexadecimal characters and write the fragment with one final newline:

```bash
set -euo pipefail
category=fixed
fragment_id=$(LC_ALL=C od -An -N16 -tx1 /dev/urandom | tr -d '[:space:]')
[[ $fragment_id =~ ^[0-9a-f]{32}$ ]]
mkdir -p "cli/changes/$category"
printf '%s\n' 'Fix run monitoring occasionally stopping before completion.' \
  >"cli/changes/$category/$fragment_id.md"
```

In an exported standalone checkout, omit the leading `cli/` from the last two commands.
Every fragment filename must match
`^[0-9a-f]{32}\.md$` exactly.

Choose one or more public categories for user-visible work. Use one `internal` fragment,
with the exact contents below, only when the work is truly invisible to users:

```text
No user-visible changes.
```

The file has exactly one newline after the period. It cannot contain an explanation.

## Public fragment grammar

A public fragment is one concise sentence of 1 through 200 content bytes followed by
exactly one line-feed byte. The content must:

- use only printable ASCII bytes `0x20` through `0x7e`, with no leading or trailing
  whitespace;
- contain balanced, nonempty inline-code spans delimited by single backticks; adjacent
  backticks and multi-backtick spans are not supported;
- outside code spans, contain none of `#`, `*`, `_`, `[`, `]`, `<`, `>`, or `\`, and
  not begin with `- `, `+ `, `> `, or a decimal number followed by `. ` or `) `;
- contain no `@` byte and no case-insensitive `http://`, `https://`, or `www.` substring,
  including inside code; and
- contain exactly one `.`, `!`, or `?` outside code, as the final content byte.

Run `./scripts/check-change-fragments` to validate the complete tree. The validator is a
standalone, offline check and rejects unknown entries and symbolic links.

## Write for public release

Describe present-tense user behavior, not implementation details. Be truthful, specific,
and concise. For `fixed`, state the symptom. For `breaking`, state both the impact and
the required migration. Inline code is appropriate for commands, options, identifiers,
and literal output.

Everything here is public. Never include credentials, private URLs, customer data,
internal incident details, or vulnerability details that would make disclosure unsafe.
A security correction may use carefully reviewed, non-revealing `fixed` wording, or the
change must wait until disclosure is safe. Do not use `internal` to hide a user-visible
security correction.

A fragment may be edited, replaced, or removed while it remains unreleased, including to
address human review. Once a stable public release tag contains a fragment, never modify
or delete it. Correct released wording with a new human-reviewed fragment.
