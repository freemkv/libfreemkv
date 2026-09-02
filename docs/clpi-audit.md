# CLPI vs MPLS cross-validation diagnostic

`src/labels/clpi_audit.rs` walks both per-clip CLPI program info and
per-playlist MPLS STN tables, normalizes their stream lists by `(PID,
language, coding_type)`, and classifies each PID into one of four
buckets:

1. **CLPI only** — a stream present in a `.clpi` ProgramInfo that no
   playlist STN table references (orphan on disc).
2. **MPLS only** — a stream a playlist references that no `.clpi`
   ProgramInfo lists (indicates a parser disagreement).
3. **Match** — both sources agree on coding_type and language.
4. **Divergent** — both sources see the PID but disagree on
   coding_type or language.

`audit` returns a structured `ClpiVsMplsAudit` report. This is a
diagnostic surface only; it does not feed the label-selection
pipeline.
