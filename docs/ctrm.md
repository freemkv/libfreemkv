# Warner CTRM label parser — relocated rationale

Referenced from `src/labels/ctrm.rs` unit tests via `// See docs/ctrm.md` pointers.

## Immunity pin: no analog of the `paramount` forced_sub false positive

Immunity pin against the defect measured in the `paramount` parser, where a
vendor `forced_sub` cell hung off a FULL dialogue track's own slot to say
"this track also contains forced signs", and reading that cell as "this
track is forced" flagged 30 MB dialogue tracks forced.

This format cannot express that. The forced signal is not a flag beside a
track's entry — it IS the entry's stream-kind token, drawn from a closed
vocabulary in which `subtitle_production` (the full dialogue track) and
`subtitle_narrative` (the forced-narrative track) are mutually exclusive
alternatives in the same position. A row is one or the other; there is no
cell a full track can carry to acquire the qualifier, so the paramount
failure mode has no encoding here.

Mutation: give `subtitle_production` a `Forced` qualifier, or add a forced
side-flag that both kinds may carry.

## Immunity pin: stream numbers come from the row, not a counter

`language_streams.txt` states each stream's number in field 3, so a row the
parser cannot use is simply dropped — it can never renumber the rows behind
it. This is the property that keeps this parser out of the STN-slot-shifting
failure mode that bites parsers which count positionally: there, a skipped
entry silently pulls every later label one stream forward.

Mutation: replace `parts[2]` with a running per-type counter → the three
unusable rows in the corresponding test collapse the survivors onto 1/2 and 1.
