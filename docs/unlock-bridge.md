# unlock_bridge

Bridges libfreemkv's drive layer to the `freemkv-unlock` crate: one generic
SCSI-transport adapter, identity/host-cert mapping, and the dispatch that
assembles the unlocker list and runs each one's `unlock()` until one claims
the drive. libfreemkv names no individual unlocker — it only calls this bridge.

## `Dispatch`

Result of a dispatch: `(matched_name, result)`. `matched_name` is the
unlocker that handled it (or `""` if none did) — lets the caller record WHICH
unlocker ran (e.g. `LD` vs `Renesas`), distinct from the id-only identity
lookup `unlocker_name`. Iterating stops at the first unlocker whose
`unlock()` returns anything other than `Ok(None)` — i.e. an actual unlock
(`Ok(Some)`) OR a real failure such as a dead bus (`Err`), which the caller
must surface rather than skip.

## `run_features` / `run_bus`

Both build their own fixed `Vec<Box<dyn fu::Unlocker>>` — `run_features` the
firmware unlockers (freemkv, LD, Renesas), `run_bus` the disc-keyed ones
(AACS, DVD) — and hand it to the shared `run()` loop. There is no
`all_unlockers()` registry; adding or removing an unlocker means editing
both this file's two lists and `unlocker_names()` below.

## `unlocker_names`

The unlocker names, in dispatch order, for the user-facing unlocker matrix.
A hardcoded `vec!["freemkv", "LD", "Renesas", "AACS", "DVD"]` — kept in sync
with `run_features`/`run_bus` by hand, not registry-driven. The per-unlocker
"did it run this rip" outcome is computed by the caller, which has the disc +
drive runtime state this crate cannot see.
