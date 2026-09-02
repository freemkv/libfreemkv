# src/dvdnav/mod.rs

## Module rationale

Byte layout follows the DVD-Video specification (VMGI/VTSI headers,
PGC/cell tables, PCI/HLI button packets); the VM command decoder is
verified against real discs.

Current contents: `vmcmd` — the VM command decoder; `nav` — the
First-Play navigation executor that resolves which title the disc's own
navigation selects as the feature.

## `resolve_feature_start` — parked status (issue #40)

PARKED (issue #40, `USE_NAV_RESOLVER=false`): this is intentionally a stub,
not an accidental dead one. It unconditionally returns `None` and ignores
`reader`/`udf` on purpose — the IFO/PCI parsing + nav executor (built on
`vmcmd`) land incrementally behind that flag. Until wired in, calling this
is behaviour-neutral (the caller always falls back); improvements to the
resolver take effect here without touching the call site. Do not delete.

`reader`/`udf` are the seam inputs the nav executor will consume to read
VIDEO_TS.IFO + the VTS IFOs/menu VOBs. Reserved until that lands.
