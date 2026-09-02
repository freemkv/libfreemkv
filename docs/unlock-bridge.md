# unlock_bridge

Bridges libfreemkv's drive layer to the `freemkv-unlock` crate: one generic
SCSI-transport adapter, identity/host-cert mapping, and the dispatch that
news up `all_unlockers()` and runs the first matching one. libfreemkv names
no individual unlocker — it only calls this bridge.

## `Dispatch`

Result of a capability dispatch: `(matched_name, result)`. `matched_name` is
the unlocker that handled it (or `""` if none did) — lets the caller record
WHICH unlocker ran (e.g. `MT1959` vs `Renesas`), distinct from the id-only
identity lookup `unlocker_name`. Iterating stops at the first unlocker whose
capability method returns anything other than `NotApplicable` — i.e. an actual
unlock (`Ok`) OR a real failure such as a dead bus (`Err(Transport)`), which
the caller must surface rather than skip.

## `unlocker_names`

The names of every REGISTERED unlocker, in dispatch order. Registry-driven —
sourced from `all_unlockers()`, so adding/removing an unlocker updates every
report with no other change (no hardcoded names). The per-unlocker "did it
run this rip" outcome is computed by the caller, which has the disc + drive
runtime state this crate cannot see.
