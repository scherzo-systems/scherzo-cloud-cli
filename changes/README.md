# Frozen CLI change-fragment archive

The files below this directory are the byte-for-byte legacy release-note archive. New
release intent is no longer added here. Every existing fragment path and its bytes are
frozen; do not modify, move, delete, or add a fragment.

The canonical private repository records new release intent in its append-only
`cli-release/` journal before exporting this standalone public tree. Managed allocation
renders approved notes from that journal. This exported archive remains available for
historical provenance and offline validation only.

Legacy categories retain their historical meanings: `added`, `changed`, `fixed`,
`breaking`, and `internal`. Public fragment bytes remain one printable-ASCII sentence of
1 through 200 content bytes followed by one line feed. Internal fragments contain
exactly `No user-visible changes.` and one line feed. `scripts/check-change-fragments`
validates the complete frozen archive without network access.
